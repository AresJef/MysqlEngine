# cython: language_level=3
# cython: wraparound=False
# cython: boundscheck=False
from __future__ import annotations

# Cython imports
import cython
from cython.cimports.cpython.tuple import PyTuple_Size as tuple_len  # type: ignore
from cython.cimports.cpython.dict import PyDict_SetItem as dict_setitem  # type: ignore
from cython.cimports.cpython.unicode import PyUnicode_GET_LENGTH as str_len  # type: ignore
from cython.cimports.mysqlengine.element import Element, Elements, Logs, Metadata, Query  # type: ignore
from cython.cimports.mysqlengine import utils  # type: ignore

# Python imports
from re import compile as _re_compile, Pattern
from sqlcycli.aio.pool import Pool
from sqlcycli.charset import Charset
from sqlcycli import errors as sqlerrors, DictCursor
from sqlcycli.aio import DictCursor as AioDictCursor
from mysqlengine.element import Element, Elements, Logs, Metadata, Query
from mysqlengine import utils


__all__ = [
    "Index",
    "FullTextIndex",
    "Indexes",
    "IndexMetadata",
]


# Index ------------------------------------------------------------------------------------------------------
@cython.cclass
class Index(Element):
    """Represent an index in a database table."""

    # Common
    _columns: tuple[str]
    _index_type: str
    _comment: str
    _visible: cython.bint
    # FullText Index
    _parser: str

    def __init__(
        self,
        *columns: str,
        index_type: str = None,
        comment: str = None,
        visible: cython.bint = True,
    ):
        """The index in a database table.

        :param columns `<'*str'>`: The column names of the index.
        :param index_type `<'str/None'>`: The algorithm of the index. Defaults to `None`.
            Accepts `"BTREE"`, `"HASH"` or `None` to use the engine default.
        :param comment `<'str/None'>`: The COMMENT of the index. Defaults to `None`.
        :param visible `<'bool'>`: The visibility of the index. Default to `True`.
            An invisible index is not used by the query optimizer, unless explicitly hinted.
        """
        super().__init__("INDEX", "INDEX")
        self._columns = self._validate_columns(columns)
        self._index_type = self._validate_index_type(index_type)
        self._comment = self._validate_comment(comment)
        self._visible = visible

    # Property -------------------------------------------------------------------------------------------
    @property
    def name(self) -> str:
        """The name of the index `<'str'>`."""
        self._assure_ready()
        return self._name

    @property
    def db_name(self) -> str:
        """The database name of the index `<'str'>`."""
        self._assure_ready()
        return self._db_name

    @property
    def tb_name(self) -> str:
        """The table name of the index `<'str'>`."""
        self._assure_ready()
        return self._tb_name

    @property
    def tb_qualified_name(self) -> str:
        """The qualified table name '{db_name}.{tb_name}' `<'str'>`."""
        self._assure_ready()
        return self._tb_qualified_name

    @property
    def columns(self) -> tuple[str]:
        """The column names of the index `<'tuple[str]'>`."""
        return self._columns

    @property
    def index_type(self) -> str | None:
        """The algorithm of the index (`"BTREE"`, `"HASH"` or `"FULLTEXT"`) `<'str/None'>`."""
        return self._index_type

    @property
    def comment(self) -> str | None:
        """The COMMENT of the index `<'str/None'>`."""
        return self._comment

    @property
    def visible(self) -> bool:
        """The visibility of the index `<'bool'>`.

        An invisible index is not used by the query optimizer, unless explicitly hinted.
        """
        return self._visible

    # Sync ---------------------------------------------------------------------------------
    @cython.ccall
    def Initialize(self, force: cython.bint = False) -> Logs:
        """[sync] Initialize the index `<'Logs'>`.

        :param force `<'bool'>`: Whether to force the initialization even
            the index has already been initialized. Defaults to `False`.

        ## Explanation (difference from add)
        - Add the index to the table if not exists.
        """
        logs: Logs = Logs()
        # Skip
        if self._initialized and not force:
            return logs
        # Initialize: index
        if not self.Exists():
            logs.extend(self.Add())
        else:
            logs.extend(self.SyncFromRemote())
        # Finished
        self._set_initialized(True)
        return logs

    @cython.ccall
    def Add(self) -> Logs:
        """[sync] Add the index to the table `<'Logs'>`.

        :raises `<'OperationalError'>`: If an index with the same name already exist.
        """
        # Execute alteration
        sql: str = self._gen_add_sql()
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
        logs = Logs().log_sql(self, sql)
        logs.log_element_creation(self, False)
        # Sync from remote
        return logs.extend(self.SyncFromRemote())

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def Exists(self) -> cython.bint:
        """[sync] Check if the index exists in the table `<'bool'>`."""
        sql: str = self._gen_exists_sql()
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
                res = cur.fetchone()
        if res is None:
            self._set_initialized(False)
            return False
        return True

    @cython.ccall
    def Drop(self) -> Logs:
        """[sync] Drop the index from the table `<'Logs'>`.

        :raises `<'OperationalError'>`: If the index does not exist.
        """
        sql: str = self._gen_drop_sql()
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
        self._set_initialized(False)
        logs = Logs().log_sql(self, sql)
        return logs.log_element_deletion(self, False)

    def Alter(
        self,
        columns: object | None = None,
        index_type: str | None = None,
        comment: str | None = None,
        visible: bool | None = None,
    ) -> Logs:
        """[sync] Alter the index `<'Logs'>`.

        ## Notice
        - Default value `None` means to retain the corresponding settings of the index.
        - Alteration of 'visible' simply toggles the visibility of the index.
        - Alteration of other options leads to `DROP` & `RE-CREATE` of the index.

        :param columns `<'str/Column/list/tuple/None'>`: New columns of the index. Defaults to `None`.
        :param index_type `<'str/None'>`: New index algorithm (`"BTREE"` or `"HASH"`). Defaults to `None`.
        :param comment `<'str/None'>`: New COMMENT of the index. Defaults to `None`.
            To remove existing comment, set comment to `''`.
        :param visible `<'bool/None'>`: The visibility of the index. Defaults to `None`.
        """
        return self._Alter(columns, index_type, None, comment, visible)

    @cython.ccall
    def _Alter(
        self,
        columns: object | None,
        index_type: str | None,
        parser: str | None,
        comment: str | None,
        visible: bool | None,
    ) -> Logs:
        """(internal) [sync] Alter the index `<'Logs'>`.

        ## Notice
        - Value `None` means to retain the corresponding settings of the index.
        - Alteration of 'visible' simply toggles the visibility of the index.
        - Alteration of other options leads to `DROP` & `RE-CREATE` of the index.

        :param columns `<'str/Column/list/tuple/None'>`: New columns of the index.
        :param index_type `<'str/None'>`: New index algorithm (`"BTREE"` or `"HASH"`).
            Only applicable to normal <'Index'>.
        :param parser `<'str/None'>`: New parser of the index.
            Only applicable to <'FullTextIndex'>.
        :param comment `<'str/None'>`: New COMMENT of the index.
            To remove existing comment, set comment to `''`.
        :param visible `<'bool/None'>`: The visibility of the index.
        """
        # Generate alter sql
        meta = self.ShowMetadata()
        query = self._gen_alter_query(
            meta, columns, index_type, parser, comment, visible
        )
        # Execute alteration
        if query.executable():
            with self._pool.acquire() as conn:
                with conn.transaction() as cur:
                    query.execute(cur)
            # . refresh metadata
            meta = self.ShowMetadata()
        # Sync from remote
        return self._sync_from_metadata(meta, query._logs)

    @cython.ccall
    def SetVisible(self, visible: cython.bint) -> Logs:
        """[sync] Toggles the visibility of the index `<'Logs'>`.

        :param visible `<'bool'>`: The visibility of the index.
            An invisible index is not used by the query optimizer, unless explicitly hinted.
        """
        # Execute alteration
        sql: str = self._gen_set_visible_sql(visible)
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
        logs = Logs().log_sql(self, sql)
        # Sync from remote
        return logs.extend(self.SyncFromRemote())

    @cython.ccall
    def ShowMetadata(self) -> IndexMetadata:
        """[sync] Show the index metadata from the remote server `<'IndexMetadata'>`.

        :raises `<'OperationalError'>`: If the index does not exist.
        """
        sql: str = self._gen_show_metadata_sql()
        with self._pool.acquire() as conn:
            conn.set_decode_json(False)
            with conn.transaction(DictCursor) as cur:
                cur.execute(sql)
                res = cur.fetchall()
        if tuple_len(res) == 0:
            self._raise_operational_error(1082, "does not exist")
        return IndexMetadata(res)

    @cython.ccall
    def ShowIndexNames(self) -> tuple[str]:
        """[sync] Show all the index names of the table `<'tuple[str]'>`."""
        sql: str = self._gen_show_index_names_sql()
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
                res = cur.fetchall()
        return self._flatten_rows(res)

    @cython.ccall
    def SyncFromRemote(self) -> Logs:
        """[sync] Synchronize the local index configs with the remote server `<'Logs'>`.

        ## Explanation
        - This method does `NOT` alter the remote server index,
          but only changes the local index configurations to match
          the remote server metadata.
        """
        try:
            meta = self.ShowMetadata()
        except sqlerrors.OperationalError as err:
            if err.args[0] == 1082:
                return Logs().log_sync_failed_not_exist(self)
            raise err
        return self._sync_from_metadata(meta)

    @cython.ccall
    def SyncToRemote(self) -> Logs:
        """[sync] Synchronize the remote server index with local configs `<'Logs'>`.

        ## Explanation
        - This method compares the local index configurations with the
          remote server metadata and issues the necessary ALTER TABLE
          statements so that the remote one matches the local settings.
        """
        # Check existence
        if not self.Exists():
            return self.Add()
        # Sync to remote
        return self._Alter(
            self._columns,
            self._index_type,
            self._parser,
            self._comment,
            self._visible,
        )

    # Async --------------------------------------------------------------------------------
    async def aioInitialize(self, force: cython.bint = False) -> Logs:
        """[async] Initialize the index `<'Logs'>`.

        :param force `<'bool'>`: Whether to force the initialization even
            the index has already been initialized. Defaults to `False`.

        ## Explanation (difference from add)
        - Add the index to the table if not exists.
        """
        logs: Logs = Logs()
        # Skip
        if self._initialized and not force:
            return logs
        # Initialize: index
        if not await self.aioExists():
            logs.extend(await self.aioAdd())
        else:
            logs.extend(await self.aioSyncFromRemote())
        # Finished
        self._set_initialized(True)
        return logs

    async def aioAdd(self) -> Logs:
        """[async] Add the index to the table `<'Logs'>`.

        :raises `<'OperationalError'>`: If an index with the same name already exist.
        """
        # Execute alteration
        sql: str = self._gen_add_sql()
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
        logs = Logs().log_sql(self, sql)
        logs.log_element_creation(self, False)
        # Sync from remote
        return logs.extend(await self.aioSyncFromRemote())

    async def aioExists(self) -> bool:
        """[async] Check if the index exists in the table `<'bool'>`."""
        sql: str = self._gen_exists_sql()
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
                res = await cur.fetchone()
        if res is None:
            self._set_initialized(False)
            return False
        return True

    async def aioDrop(self) -> Logs:
        """[async] Drop the index from the table `<'Logs'>`.

        :raises `<'OperationalError'>`: If the index does not exist.
        """
        sql: str = self._gen_drop_sql()
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
        self._set_initialized(False)
        logs = Logs().log_sql(self, sql)
        return logs.log_element_deletion(self, False)

    async def aioAlter(
        self,
        columns: object | None = None,
        index_type: str | None = None,
        comment: str | None = None,
        visible: bool | None = None,
    ) -> Logs:
        """[async] Alter the index `<'Logs'>`.

        ## Notice
        - Default value `None` means to retain the corresponding settings of the index.
        - Alteration of 'visible' simply toggles the visibility of the index.
        - Alteration of other options leads to `DROP` & `RE-CREATE` of the index.

        :param columns `<'str/Column/list/tuple/None'>`: New columns of the index. Defaults to `None`.
        :param index_type `<'str/None'>`: New index algorithm (`"BTREE"` or `"HASH"`). Defaults to `None`.
        :param comment `<'str/None'>`: New COMMENT of the index. Defaults to `None`.
            To remove existing comment, set comment to `''`.
        :param visible `<'bool/None'>`: The visibility of the index. Defaults to `None`.
        """
        return await self._aioAlter(columns, index_type, None, comment, visible)

    async def _aioAlter(
        self,
        columns: object | None,
        index_type: str | None,
        parser: str | None,
        comment: str | None,
        visible: bool | None,
    ) -> Logs:
        """(internal) [async] Alter the index `<'Logs'>`.

        ## Notice
        - Value `None` means to retain the corresponding settings of the index.
        - Alteration of 'visible' simply toggles the visibility of the index.
        - Alteration of other options leads to `DROP` & `RE-CREATE` of the index.

        :param columns `<'str/Column/list/tuple/None'>`: New columns of the index.
        :param index_type `<'str/None'>`: New index algorithm (`"BTREE"` or `"HASH"`).
            Only applicable to normal <'Index'>.
        :param parser `<'str/None'>`: New parser of the index.
            Only applicable to <'FullTextIndex'>.
        :param comment `<'str/None'>`: New COMMENT of the index.
            To remove existing comment, set comment to `''`.
        :param visible `<'bool/None'>`: The visibility of the index.
        """
        # Generate alter sql
        meta = await self.aioShowMetadata()
        query = self._gen_alter_query(
            meta, columns, index_type, parser, comment, visible
        )
        # Execute alteration
        if query.executable():
            async with self._pool.acquire() as conn:
                async with conn.transaction() as cur:
                    await query.aio_execute(cur)
            # . refresh metadata
            meta = await self.aioShowMetadata()
        # Sync from remote
        return self._sync_from_metadata(meta, query._logs)

    async def aioSetVisible(self, visible: cython.bint) -> Logs:
        """[async] Toggles the visibility of the index `<'Logs'>`.

        :param visible `<'bool'>`: The visibility of the index.
            An invisible index is not used by the query optimizer, unless explicitly hinted.
        """
        # Execute alteration
        sql: str = self._gen_set_visible_sql(visible)
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
        logs = Logs().log_sql(self, sql)
        # Sync from remote
        return logs.extend(await self.aioSyncFromRemote())

    async def aioShowMetadata(self) -> IndexMetadata:
        """[async] Show the index metadata from the remote server `<'IndexMetadata'>`.

        :raises `<'OperationalError'>`: If the index does not exist.
        """
        sql: str = self._gen_show_metadata_sql()
        async with self._pool.acquire() as conn:
            conn.set_decode_json(False)
            async with conn.transaction(AioDictCursor) as cur:
                await cur.execute(sql)
                res = await cur.fetchall()
        if tuple_len(res) == 0:
            self._raise_operational_error(1082, "does not exist")
        return IndexMetadata(res)

    async def aioShowIndexNames(self) -> tuple[str]:
        """[async] Show all the index names of the table `<'tuple[str]'>`."""
        sql: str = self._gen_show_index_names_sql()
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
                res = await cur.fetchall()
        return self._flatten_rows(res)

    async def aioSyncFromRemote(self) -> Logs:
        """[async] Synchronize the local index configs with the remote server `<'Logs'>`.

        ## Explanation
        - This method does `NOT` alter the remote server index,
          but only changes the local index configurations to match
          the remote server metadata.
        """
        try:
            meta = await self.aioShowMetadata()
        except sqlerrors.OperationalError as err:
            if err.args[0] == 1082:
                return Logs().log_sync_failed_not_exist(self)
            raise err
        return self._sync_from_metadata(meta)

    async def aioSyncToRemote(self) -> Logs:
        """[async] Synchronize the remote server index with local configs `<'Logs'>`.

        ## Explanation
        - This method compares the local index configurations with the
          remote server metadata and issues the necessary ALTER TABLE
          statements so that the remote one matches the local settings.
        """
        # Check existence
        if not await self.aioExists():
            return await self.aioAdd()
        # Sync to remote
        return await self._aioAlter(
            self._columns,
            self._index_type,
            self._parser,
            self._comment,
            self._visible,
        )

    # Generate SQL -------------------------------------------------------------------------
    @cython.ccall
    def _gen_definition_sql(self) -> str:
        """(internal) Generate the definition SQL of the index `<'str'>`."""
        self._assure_ready()
        sql: str = "%s %s (%s)" % (
            self._el_type,
            self._name,
            ", ".join(self._columns),
        )
        if self._index_type is not None:
            sql += " USING %s" % self._index_type
        if self._comment is not None:
            sql += self._format_sql(" COMMENT %s", self._comment)
        if not self._visible:
            sql += " INVISIBLE"
        return sql

    @cython.ccall
    def _gen_add_sql(self) -> str:
        """(internal) Generate SQL to add the index to the table `<'str'>`."""
        self._assure_ready()
        return "ALTER TABLE %s ADD %s;" % (
            self._tb_qualified_name,
            self._gen_definition_sql(),
        )

    @cython.ccall
    def _gen_exists_sql(self) -> str:
        """(internal) Generate SQL to check if the index exists in the table `<'str'>`."""
        self._assure_ready()
        return (
            "SELECT 1 "
            "FROM INFORMATION_SCHEMA.STATISTICS "
            "WHERE TABLE_NAME = '%s' "
            "AND INDEX_SCHEMA = '%s' "
            "AND INDEX_NAME = '%s' "
            "AND INDEX_SCHEMA = TABLE_SCHEMA "
            "AND SEQ_IN_INDEX = 1 "
            "LIMIT 1;" % (self._tb_name, self._db_name, self._name)
        )

    @cython.ccall
    def _gen_drop_sql(self) -> str:
        """(internal) Generate SQL to drop the index from the table `<'str'>`."""
        self._assure_ready()
        return "ALTER TABLE %s DROP INDEX %s;" % (self._tb_qualified_name, self._name)

    @cython.ccall
    def _gen_alter_query(
        self,
        meta: IndexMetadata,
        columns: object | None,
        index_type: str | None,
        parser: str | None,
        comment: str | None,
        visible: bool | None,
    ) -> Query:
        """(internal) Generate query to alter the index `<'Query'>`.

        :param meta `<'IndexMetadata'>`: The remote server index metadata.
        :param columns `<'str/Column/list/tuple/None'>`: New columns of the index.
        :param index_type `<'str/None'>`: New index algorithm (`"BTREE"` or `"HASH"`).
            Only applicable to normal <'Index'>.
        :param parser `<'str/None'>`: New parser of the index.
            Only applicable to <'FullTextIndex'>.
        :param comment `<'str/None'>`: New COMMENT of the index.
            To remove existing comment, set comment to `''`.
        :param visible `<'bool/None'>`: The visibility of the index.
        """
        self._assure_ready()
        query = Query()

        # New Index
        idx: Index = self._construct(columns, index_type, parser, comment, visible)
        idx.set_name(self._name)
        idx.setup(self._tb_name, self._db_name, self._charset, None, self._pool)

        # Compare differences
        diff = idx._diff_from_metadata(meta)
        # . no differences
        if diff == 0:
            return query
        # . drop & re-create
        if diff == 1:
            query.set_sql(self, idx._gen_drop_sql())
            query.set_sql(self, idx._gen_add_sql())
        # . toggle visibility
        else:
            query.set_sql(self, self._gen_set_visible_sql(idx._visible))
        return query

    @cython.ccall
    def _gen_set_visible_sql(self, visible: cython.bint) -> str:
        """(internal) Generate SQL to set the index visibility `<'str'>`."""
        self._assure_ready()
        if visible:
            return "ALTER TABLE %s ALTER INDEX %s VISIBLE;" % (
                self._tb_qualified_name,
                self._name,
            )
        else:
            return "ALTER TABLE %s ALTER INDEX %s INVISIBLE;" % (
                self._tb_qualified_name,
                self._name,
            )

    @cython.ccall
    def _gen_show_metadata_sql(self) -> str:
        """(internal) Generate SQL to show index metadata `<'str'>`."""
        self._assure_ready()
        return (
            "SELECT "
            # . columns
            "TABLE_CATALOG AS CATALOG_NAME, "
            "TABLE_SCHEMA AS SCHEMA_NAME, "
            "TABLE_NAME AS TABLE_NAME, "
            "INDEX_NAME AS INDEX_NAME, "
            "UPPER(INDEX_TYPE) AS INDEX_TYPE, "
            "NON_UNIQUE AS NON_UNIQUE, "
            "COMMENT AS COMMENT, "
            "INDEX_COMMENT AS INDEX_COMMENT, "
            "UPPER(IS_VISIBLE) AS IS_VISIBLE, "
            "NULL AS PARSER, "
            "COLUMN_NAME AS COLUMN_NAME, "
            "EXPRESSION AS EXPRESSION, "
            "SEQ_IN_INDEX AS SEQ_IN_INDEX, "
            "UPPER(COLLATION) AS COLLATION, "
            "CARDINALITY AS CARDINALITY, "
            "SUB_PART AS SUB_PART, "
            "PACKED AS PACKED "
            # . information_schema.statistics
            "FROM INFORMATION_SCHEMA.STATISTICS "
            # . conditions
            "WHERE TABLE_NAME = '%s' "
            "AND INDEX_SCHEMA = '%s' "
            "AND INDEX_NAME = '%s' "
            "AND INDEX_SCHEMA = TABLE_SCHEMA "
            "ORDER BY SEQ_IN_INDEX ASC;" % (self._tb_name, self._db_name, self._name)
        )

    @cython.ccall
    def _gen_show_index_names_sql(self) -> str:
        """(internal) Generate SQL to select all index names of the table `<'str'>`."""
        self._assure_ready()
        return (
            "SELECT INDEX_NAME AS i "
            "FROM INFORMATION_SCHEMA.STATISTICS "
            "WHERE TABLE_NAME = '%s' "
            "AND INDEX_SCHEMA = '%s' "
            "AND INDEX_SCHEMA = TABLE_SCHEMA "
            "AND SEQ_IN_INDEX = 1;" % (self._tb_name, self._db_name)
        )

    # Metadata -----------------------------------------------------------------------------
    @cython.ccall
    def _sync_from_metadata(self, meta: IndexMetadata, logs: Logs = None) -> Logs:
        """(internal) Synchronize local configs with the remote index metadata `<'Logs'>`."""
        self._assure_ready()
        if logs is None:
            logs = Logs()

        # Validate
        if self._el_type != meta._el_type:
            logs.log_sync_failed_mismatch(
                self, "index type", self._el_type, meta._el_type
            )
            return logs._skip()  # exit
        if self._name != meta._index_name:
            logs.log_sync_failed_mismatch(
                self, "index name", self._name, meta._index_name
            )
            return logs._skip()  # exit
        if self._tb_name != meta._tb_name:
            logs.log_sync_failed_mismatch(
                self, "table name", self._tb_name, meta._tb_name
            )
            return logs._skip()  # exit
        if self._db_name != meta._db_name:
            logs.log_sync_failed_mismatch(
                self, "database name", self._db_name, meta._db_name
            )
            return logs._skip()  # exit
        if meta._unique:
            logs.log_sync_failed_mismatch(self, "index uniqueness", False, True)
            return logs._skip()  # exit

        # Index Type
        if self._index_type != meta._index_type:
            logs.log_config_obj(self, "index_type", self._index_type, meta._index_type)
            self._index_type = meta._index_type

        # Columns
        if self._columns != meta._columns:
            logs.log_config_obj(self, "columns", self._columns, meta._columns)
            self._columns = meta._columns

        # Comment
        if self._comment != meta._comment:
            logs.log_config_obj(self, "comment", self._comment, meta._comment)
            self._comment = meta._comment

        # Visibility
        if self._visible != meta._visible:
            logs.log_config_bool(self, "visible", self._visible, meta._visible)
            self._visible = meta._visible

        # Return logs
        return logs

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _diff_from_metadata(self, meta: IndexMetadata) -> cython.int:
        """(internal) Check if the index configurations are different
        from the remote server metadata `<'int'>`.

        :returns `<'int'>`:
        - `0`: Index configurations are identical.
        - `1`: Index configurations differ in more than visibility.
        - `2`: Only the visibility differs.
        """
        # Different
        if self._el_type != meta._el_type:
            return 1
        if meta._unique:
            return 1
        if self._index_type != meta._index_type:
            return 1
        if self._columns != meta._columns:
            return 1
        if self._comment != meta._comment:
            return 1
        if self._visible != meta._visible:
            return 2
        # Same
        return 0

    # Setter -------------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def setup(
        self,
        tb_name: str,
        db_name: str,
        charset: str | Charset,
        collate: str | None,
        pool: Pool,
    ) -> cython.bint:
        """Setup the index.

        :param tb_name `<'str'>`: The table name of the index.
        :param db_name `<'str'>`: The database name of the index.
        :param charset `<'str/Charset'>`: The charset of the index.
        :param collate `<'str/None'>`: The collation of the index.
        :param pool `<'Pool'>`: The pool of the index.
        """
        self._set_tb_name(tb_name)
        self._set_db_name(db_name)
        self._set_charset(charset, collate)
        self._set_pool(pool)
        return self._assure_ready()

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def set_name(self, name: object) -> cython.bint:
        """Set the name of the Index."""
        if Element.set_name(self, name):
            self._name = self._validete_index_name(name)
        return True

    # Assure Ready -------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _assure_ready(self) -> cython.bint:
        """(internal) Assure the index is ready."""
        if not self._el_ready:
            self._assure_name_ready()
            self._assure_tb_name_ready()
            Element._assure_ready(self)
        return True

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def copy(self) -> Index:
        """Make a copy of the index `<'Index'>`."""
        idx: Index = self._construct(
            self._columns,
            self._index_type,
            self._parser,
            self._comment,
            self._visible,
        )
        idx.set_name(self._name)
        return idx

    @cython.ccall
    def _construct(
        self,
        columns: object | None,
        index_type: str | None,
        parser: str | None,
        comment: str | None,
        visible: bool | None,
    ) -> Index:
        """(internal) Construct a new index instance `<'Index'>`.

        ## Notice
        - Value `None` means to use the corresponding settings of this index.

        :param columns `<'str/Column/tuple/list/None'>`: The column names of the index.
        :param index_type `<'str/None'>`: The algorithm of the index (`"BTREE"` or `"HASH"`).
            Only applicable to normal <'Index'>.
        :param parser `<'str/None'>`: The parser of the index.
            Only applicable to <'FullTextIndex'>.
        :param comment `<'str/None'>`: The COMMENT of the index.
        :param visible `<'bool'>`: The visibility of the index.
        """
        columns = self._columns if columns is None else self._validate_columns(columns)
        return Index(
            # fmt: off
            *columns,
            index_type=self._index_type if index_type is None else self._validate_index_type(index_type),
            comment=self._comment if comment is None else self._validate_comment(comment),
            visible=self._visible if visible is None else bool(visible),
            # fmt: on
        )

    # Special Methods ----------------------------------------------------------------------
    def __repr__(self) -> str:
        return "<%s (%s)>" % (self.__class__.__name__, self._gen_definition_sql())

    def __str__(self) -> str:
        self._assure_ready()
        return self._name


@cython.cclass
class FullTextIndex(Index):
    """Represent a full text index in a database table."""

    _parser_regex: Pattern

    def __init__(
        self,
        *columns: str,
        parser: object = None,
        comment: str = None,
        visible: bool = True,
    ):
        """The full text index in a database table.

        :param columns `<'*str'>`: The column names of the index.
        :param parser `<'str/None'>`: The parser of the index. Defaults to `None`.
        :param comment `<'str/None'>`: The COMMENT of the index. Defaults to `None`.
        :param visible `<'bool'>`: The visibility of the index. Default to `True`.
            An invisible index is not used by the query optimizer, unless explicitly hinted.
        """
        super().__init__(
            *columns,
            index_type="FULLTEXT",
            comment=comment,
            visible=visible,
        )
        self._set_el_type("FULLTEXT INDEX")
        self._parser = self._validate_parser(parser)

    # Property -----------------------------------------------------------------------------
    @property
    def parser(self) -> str | None:
        """The parser of the index `<'str/None'>`."""
        return self._parser

    # Sync ---------------------------------------------------------------------------------
    def Alter(
        self,
        columns: object | None = None,
        parser: str | None = None,
        comment: str | None = None,
        visible: bool | None = None,
    ) -> Logs:
        """[sync] Alter the index `<'Logs'>`.

        ## Notice
        - Default value `None` means to retain the corresponding settings of the full text index.
        - Alteration of 'visible' simply toggles the visibility of the index.
        - Alteration of other options leads to `DROP` & `RE-CREATE` of the index.

        :param columns `<'str/Column/list/tuple/None'>`: New columns of the index. Defaults to `None`.
        :param parser `<'str/None'>`: New parser of the index. Defaults to `None`.
            To remove existing parser, set parser to `''`.
        :param comment `<'str/None'>`: New COMMENT of the index. Defaults to `None`.
            To remove existing comment, set comment to `''`.
        :param visible `<'bool/None'>`: The visibility of the index. Defaults to `None`.
        """
        return self._Alter(columns, None, parser, comment, visible)

    @cython.ccall
    def ShowMetadata(self) -> IndexMetadata:
        """[sync] Show the index metadata from the remote server `<'IndexMetadata'>`.

        :raises `<'OperationalError'>`: If the index does not exist.
        """
        # Index metadata
        sql: str = self._gen_show_metadata_sql()
        with self._pool.acquire() as conn:
            conn.set_decode_json(False)
            with conn.transaction(DictCursor) as cur:
                cur.execute(sql)
                res: tuple = cur.fetchall()
        if tuple_len(res) == 0:
            self._raise_operational_error(1082, "does not exist")
        # Fulltext parser
        parser = self._parser_regex.search(self.ShowCreateTable())
        if parser is not None:
            parser = parser.group(1)
            for row in res:
                dict_setitem(row, "PARSER", parser)
        # Return metadata
        return IndexMetadata(res)

    # Async --------------------------------------------------------------------------------
    async def aioAlter(
        self,
        columns: object | None = None,
        parser: str | None = None,
        comment: str | None = None,
        visible: bool | None = None,
    ) -> Logs:
        """[async] Alter the index `<'Logs'>`.

        ## Notice
        - Default value `None` means to retain the corresponding settings of the full text index.
        - Alteration of 'visible' simply toggles the visibility of the index.
        - Alteration of other options leads to `DROP` & `RE-CREATE` of the index.

        :param columns `<'str/Column/list/tuple/None'>`: New columns of the index. Defaults to `None`.
        :param parser `<'str/None'>`: New parser of the index. Defaults to `None`.
            To remove existing parser, set parser to `''`.
        :param comment `<'str/None'>`: New COMMENT of the index. Defaults to `None`.
            To remove existing comment, set comment to `''`.
        :param visible `<'bool/None'>`: The visibility of the index. Defaults to `None`.
        """
        return await self._aioAlter(columns, None, parser, comment, visible)

    async def aioShowMetadata(self) -> IndexMetadata:
        """[async] Show the index metadata from the remote server `<'IndexMetadata'>`.

        :raises `<'OperationalError'>`: If the index does not exist.
        """
        # Index metadata
        sql: str = self._gen_show_metadata_sql()
        async with self._pool.acquire() as conn:
            conn.set_decode_json(False)
            async with conn.transaction(AioDictCursor) as cur:
                await cur.execute(sql)
                res: tuple = await cur.fetchall()
        if tuple_len(res) == 0:
            self._raise_operational_error(1082, "does not exist")
        # Fulltext parser
        parser = self._parser_regex.search(await self.aioShowCreateTable())
        if parser is not None:
            parser = parser.group(1)
            for row in res:
                dict_setitem(row, "PARSER", parser)
        # Return metadata
        return IndexMetadata(res)

    # Generate SQL -------------------------------------------------------------------------
    @cython.ccall
    def _gen_definition_sql(self) -> str:
        """(internal) Generate the definition SQL of the index `<'str'>`."""
        self._assure_ready()
        sql: str = "%s %s (%s)" % (
            self._el_type,
            self._name,
            ", ".join(self._columns),
        )
        if self._parser is not None:
            sql += " WITH PARSER %s" % self._parser
        if self._comment is not None:
            sql += self._format_sql(" COMMENT %s", self._comment)
        if not self._visible:
            sql += " INVISIBLE"
        return sql

    # Metadata -----------------------------------------------------------------------------
    @cython.ccall
    def _sync_from_metadata(self, meta: IndexMetadata, logs: Logs = None) -> Logs:
        """(internal) Synchronize local configs with the remote index metadata `<'Logs'>`."""
        # Base configs
        logs = Index._sync_from_metadata(self, meta, logs)
        if logs._skip_flag:
            return logs

        # Parser
        if self._parser != meta._parser:
            logs.log_config_obj(self, "parser", self._parser, meta._parser)
            self._parser = meta._parser

        # Return logs
        return logs

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _diff_from_metadata(self, meta: IndexMetadata) -> cython.int:
        """(internal) Check if the index configurations are different
        from the remote server metadata `<'int'>`.

        :returns `<'int'>`:
        - `0`: Index configurations are identical.
        - `1`: Index configurations differ in more than visibility.
        - `2`: Only the visibility differs.
        """
        # Different
        diff = Index._diff_from_metadata(self, meta)
        if diff == 1:
            return 1
        if self._parser != meta._parser:
            return 1
        # Same or Toggle visibility
        return diff

    # Validator ----------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _validate_parser(self, parser: object) -> str:
        """(internal) Validate the parser of the index `<'str/None'>`."""
        if parser is None:
            return None
        if isinstance(parser, str):
            return None if str_len(parser) == 0 else parser
        self._raise_definition_error(
            "parser must be <'str'> type, instead got %s %r." % (type(parser), parser)
        )

    # Setter -------------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def set_name(self, name: object) -> cython.bint:
        """Set the name of the FullTextIndex."""
        if Element.set_name(self, name):
            self._name = self._validete_index_name(name)
            self._parser_regex = _re_compile(
                f"FULLTEXT (?:KEY|INDEX) `{self._name}`.+ WITH PARSER `([^`]+)`"
            )
        return True

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def _construct(
        self,
        columns: object,
        index_type: str | None,
        parser: str | None,
        comment: str | None,
        visible: bool | None,
    ) -> Index:
        """(internal) Construct a new index instance `<'Index'>`.

        ## Notice
        - Value `None` means to use the corresponding settings of this index.

        :param columns `<'str/Column/tuple/list/None'>`: The column names of the index.
        :param index_type `<'str/None'>`: The algorithm of the index (`"BTREE"` or `"HASH"`).
            Only applicable to normal <'Index'>.
        :param parser `<'str/None'>`: The parser of the index.
            Only applicable to <'FullTextIndex'>.
        :param comment `<'str/None'>`: The COMMENT of the index.
        :param visible `<'bool'>`: The visibility of the index.
        """
        columns = self._columns if columns is None else self._validate_columns(columns)
        return FullTextIndex(
            # fmt: off
            *columns,
            parser=self._parser if parser is None else self._validate_parser(parser),
            comment=self._comment if comment is None else self._validate_comment(comment),
            visible=self._visible if visible is None else bool(visible),
            # fmt: on
        )


# Indexes ----------------------------------------------------------------------------------------------------
@cython.cclass
class Indexes(Elements):
    """Represents a collection of indexes in a table.

    Works as a dictionary where keys are the index names
    and values the index instances.
    """

    def __init__(self, *indexes: Index):
        """The collection of indexes in a table.

        Works as a dictionary where keys are the index names
        and values the index instances.

        :param indexes `<'*Index'>`: The indexes in a table.
        """
        super().__init__("INDEX", "INDEXES", Index, *indexes)

    # Generate SQL -------------------------------------------------------------------------
    @cython.ccall
    def _gen_definition_sql(self, indent: cython.int = 0) -> str:
        """(internal) Generate the definition SQL of the collection `<'str'>`.

        :param indent `<'int'>`: The indentation of the definition SQL. Defaults to `0`.
        """
        # Empty collection
        if self._size == 0:
            return ""

        # Generate SQL
        idx: Index
        sqls = [idx._gen_definition_sql() for idx in self._sorted_elements()]
        # . without indent
        nxt: str = ",\n"
        if indent == 0:
            return nxt.join(sqls)
        # . with indent
        if indent > 0:
            pad: str = "\t" * indent
            nxt += pad
            return pad + nxt.join(sqls)
        # Invalid Indent
        self._raise_argument_error(
            "definition SQL statement 'indent' must be a positive integer, "
            "instead got %d." % indent
        )

    # Setter -------------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def setup(
        self,
        tb_name: str,
        db_name: str,
        charset: str | Charset,
        collate: str | None,
        pool: Pool,
    ) -> cython.bint:
        """Setup the index collection.

        :param tb_name `<'str'>`: The table name of the index collection.
        :param db_name `<'str'>`: The database name of the index collection.
        :param charset `<'str/Charset'>`: The charset of the index collection.
        :param collate `<'str/None'>`: The collation of the index collection.
        :param pool `<'Pool'>`: The pool of the index collection.
        """
        # Collection
        self._set_tb_name(tb_name)
        self._set_db_name(db_name)
        self._set_charset(charset, collate)
        self._set_pool(pool)
        # Elements
        tb_name = self._tb_name
        db_name = self._db_name
        charset = self._charset
        pool = self._pool
        el: Index
        for el in self._el_dict.values():
            if not el._el_ready:
                el.setup(tb_name, db_name, charset, None, pool)
        return self._assure_ready()

    # Assure Ready -------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _assure_ready(self) -> cython.bint:
        """(internal) Assure the index collection is ready."""
        if not self._el_ready:
            self._assure_tb_name_ready()
            Elements._assure_ready(self)
        return True

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def copy(self) -> Indexes:
        """Make a copy of the index collection `<'Indexes'>`."""
        el: Index
        return Indexes(*[el.copy() for el in self._el_dict.values()])


# Metadata ---------------------------------------------------------------------------------------------------
@cython.cclass
class IndexMetadata(Metadata):
    """Represents the metadata from the remote server of an index."""

    # Base data
    _db_name: str
    _tb_name: str
    _index_name: str
    _index_type: str
    _unique: cython.bint
    _comment: str
    _visible: cython.bint
    # Additional data
    _el_type: str
    _columns: tuple[str]
    # FullText Index
    _parser: str

    def __init__(self, meta: tuple[dict]):
        """The metadata from the remote server of an index.

        :param meta `<'tuple[dict]'>`: A tuple of dictionaries, each containing the following index metadata:
        ```python
        - "CATALOG_NAME"
        - "SCHEMA_NAME"
        - "TABLE_NAME"
        - "INDEX_NAME"
        - "INDEX_TYPE"
        - "NON_UNIQUE"
        - "COMMENT"
        - "INDEX_COMMENT"
        - "IS_VISIBLE"
        - "PARSER"
        - "COLUMN_NAME"
        - "EXPRESSION"
        - "SEQ_IN_INDEX"
        - "COLLATION"
        - "CARDINALITY"
        - "SUB_PART"
        - "PACKED"
        ```
        """
        # Re-construct
        self._el_cate = "INDEX"
        _meta: dict = None
        _cols: list = []
        row: dict
        try:
            for row in meta:
                if _meta is None:
                    _meta = {
                        "CATALOG_NAME": row["CATALOG_NAME"],
                        "SCHEMA_NAME": row["SCHEMA_NAME"],
                        "TABLE_NAME": row["TABLE_NAME"],
                        "INDEX_NAME": row["INDEX_NAME"],
                        "INDEX_TYPE": row["INDEX_TYPE"],
                        "NON_UNIQUE": row["NON_UNIQUE"],
                        "COMMENT": row["COMMENT"],
                        "INDEX_COMMENT": row["INDEX_COMMENT"],
                        "IS_VISIBLE": row["IS_VISIBLE"],
                        "PARSER": row["PARSER"],
                    }
                _cols.append(
                    {
                        "COLUMN_NAME": row["COLUMN_NAME"],
                        "EXPRESSION": row["EXPRESSION"],
                        "SEQ_IN_INDEX": row["SEQ_IN_INDEX"],
                        "COLLATION": row["COLLATION"],
                        "CARDINALITY": row["CARDINALITY"],
                        "SUB_PART": row["SUB_PART"],
                        "PACKED": row["PACKED"],
                    }
                )
            if _meta is None:
                raise ValueError("Index metadata is empty.")
        except Exception as err:
            self._raise_invalid_metadata_error(meta, err)
        dict_setitem(_meta, "COLUMNS", tuple(_cols))

        # Initialize
        super().__init__("INDEX", _meta, 11)
        try:
            # Base data
            self._db_name = self._meta["SCHEMA_NAME"]
            self._tb_name = self._meta["TABLE_NAME"]
            self._index_name = self._meta["INDEX_NAME"]
            self._index_type = utils.validate_index_type(self._meta["INDEX_TYPE"])
            self._unique = self._meta["NON_UNIQUE"] == 0
            self._comment = utils.validate_comment(self._meta["INDEX_COMMENT"])
            self._visible = utils.validate_visible(self._meta["IS_VISIBLE"])
            self._parser = self._meta["PARSER"]
            # Additional data
            if self._index_type != "FULLTEXT":
                self._el_type = "INDEX"
            else:
                self._el_type = "FULLTEXT INDEX"
            _columns: list = []
            col: dict
            for col in _cols:
                col_name: str = col["COLUMN_NAME"]
                if col_name is not None:
                    sub_part = col["SUB_PART"]
                    if sub_part is not None:
                        col_name += "(%s)" % sub_part
                else:
                    col_name = col["EXPRESSION"]
                if col["COLLATION"] == "D":
                    col_name += " DESC"
                _columns.append(col_name)
            self._columns = tuple(_columns)
        except Exception as err:
            self._raise_invalid_metadata_error(self._meta, err)

    # Property -----------------------------------------------------------------------------
    @property
    def catelog_name(self) -> str:
        """The catalog name of the index `<'str'>`."""
        return self._meta["CATALOG_NAME"]

    @property
    def db_name(self) -> str:
        """The schema name of the index `<'str'>`."""
        return self._db_name

    @property
    def tb_name(self) -> str:
        """The table name of the index `<'str'>`."""
        return self._tb_name

    @property
    def index_name(self) -> str:
        """The name of the index `<'str'>`."""
        return self._index_name

    @property
    def index_type(self) -> str:
        """The algorithm of the index `<'str'>`.
        
        e.g.: `"BTREE"`, `"HASH"`, `"FULLTEXT"`
        """
        return self._index_type

    @property
    def unique(self) -> bool:
        """Whether the index is unique or not `<'bool'>`."""
        return self._unique

    @property
    def comment(self) -> str | None:
        """The comment of the index `<'str/None'>`."""
        return self._comment

    @property
    def visible(self) -> bool:
        """Whether the index is visible to the optimizer `<'bool'>`."""
        return self._visible

    @property
    def columns(self) -> tuple[str]:
        """All the columns of the index `<'tuple[str]'>`."""
        return self._columns

    @property
    def parser(self) -> str | None:
        """The parser of the FULLTEXT INDEX `<'str/None'>`."""
        return self._parser
