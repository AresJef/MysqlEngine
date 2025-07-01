# cython: language_level=3
# cython: wraparound=False
# cython: boundscheck=False
from __future__ import annotations

# Cython imports
import cython
from cython.cimports.cpython.tuple import PyTuple_Size as tuple_len  # type: ignore
from cython.cimports.cpython.dict import PyDict_SetItem as dict_setitem  # type: ignore
from cython.cimports.cpython.unicode import PyUnicode_GET_LENGTH as str_len  # type: ignore
from cython.cimports.cpython.unicode import PyUnicode_Substring as str_substr  # type: ignore
from cython.cimports.cpython.unicode import PyUnicode_Tailmatch as str_tailmatch  # type: ignore
from cython.cimports.mysqlengine.element import Element, Elements, Logs, Metadata, Query  # type: ignore
from cython.cimports.mysqlengine import utils  # type: ignore

# Python imports
from sqlcycli.aio.pool import Pool
from sqlcycli.charset import Charset
from sqlcycli import errors as sqlerrors, DictCursor
from sqlcycli.aio import DictCursor as AioDictCursor
from mysqlengine.element import Element, Elements, Logs, Metadata, Query
from mysqlengine import utils


__all__ = [
    "Constraint",
    "UniqueKey",
    "PrimaryKey",
    "ForeignKey",
    "Check",
    "Constraints",
    "ConstraintMetadata",
]


# Constraint -------------------------------------------------------------------------------------------------
@cython.cclass
class Constraint(Element):
    """The base class for a constraint in a database table."""

    # Common
    _enforced: cython.bint
    # Primary/Unique Key
    _columns: tuple[str]
    _index_type: str
    _comment: str
    _visible: cython.bint
    # Foreign Key
    _reference_table: str
    _reference_columns: tuple[str]
    _on_delete: str
    _on_update: str
    # Check
    _expression: str

    def __init__(self, el_type: str):
        """The base class for a constraint in a database table.

        :param el_type `<'str'>`: The type of the constraint
            (e.g., 'UNIQUE KEY', 'PRIMARY KEY', 'FOREIGN KEY', 'CHECK').
        """
        super().__init__("CONSTRAINT", el_type)
        self._enforced = True

    # Property -----------------------------------------------------------------------------
    @property
    def symbol(self) -> str:
        """The symbol of the constraint `<'str'>`.

        `Symbol` is the actual identifier used in the
        database to refer to the constraint.
        """
        self._assure_ready()
        return self._symbol

    @property
    def name(self) -> str:
        """The name of the constraint `<'str'>`."""
        self._assure_ready()
        return self._name

    @property
    def db_name(self) -> str:
        """The database name of the constraint `<'str'>`."""
        self._assure_ready()
        return self._db_name

    @property
    def tb_name(self) -> str:
        """The table name of the constraint `<'str'>`."""
        self._assure_ready()
        return self._tb_name

    @property
    def tb_qualified_name(self) -> str:
        """The qualified table name '{db_name}.{tb_name}' `<'str'>`."""
        self._assure_ready()
        return self._tb_qualified_name

    @property
    def enforced(self) -> bool:
        """Whether the constraint is enforced `<'bool'>`."""
        return self._enforced

    # Sync ---------------------------------------------------------------------------------
    @cython.ccall
    def Initialize(self, force: cython.bint = False) -> Logs:
        """[sync] Initialize the constraint `<'Logs'>`.

        :param force `<'bool'>`: Whether to force the initialization even
            the constraint has already been initialized. Defaults to `False`.

        ## Explanation (difference from add)
        - Add the constraint to the table if not exists.
        """
        logs: Logs = Logs()
        # Skip
        if self._initialized and not force:
            return logs
        # Initialize: constraint
        if not self.Exists():
            logs.extend(self.Add())
        else:
            logs.extend(self.SyncFromRemote())
        # Finished
        self._set_initialized(True)
        return logs

    @cython.ccall
    def Add(self) -> Logs:
        """[sync] Add the constraint to the table `<'Logs'>`.

        :raises `<'OperationalError'>`: If a constraint with the symbal name already exist.
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
        """[sync] Check if the constraint exists in the table `<'bool'>`."""
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
        """[sync] Drop the constraint from the table `<'Logs'>`.

        :raises `<'OperationalError'>`: If the constraint does not exist.
        """
        sql: str = self._gen_drop_sql()
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
        self._set_initialized(False)
        logs = Logs().log_sql(self, sql)
        return logs.log_element_deletion(self, False)

    def Alter(self, *args, **kwargs) -> Logs:
        """[sync] Alter the Constraint `<'Logs'>`."""
        self._raise_not_implemented_error("alter")

    @cython.ccall
    def _Alter(
        self,
        columns: object | None,
        index_type: str | None,
        comment: str | None,
        visible: bool | None,
        reference_table: str | None,
        reference_columns: object | None,
        on_delete: str | None,
        on_update: str | None,
        expression: object | None,
        enforced: bool | None,
    ) -> Logs:
        """(internal) [sync] Alter the constraint `<'Logs'>`.

        ## Notice
        - Value `None` means to retain the corresponding settings of the constraint.

        :param columns `<'str/Column/tuple/list/None'>`: New columns of the constraint.
            Only applicable to <'UniqueKey'>, <'PrimaryKey'> and <'ForeignKey'>.
        :param index_type `<'str/None'>`: New index algorithm (`"BTREE"` or `"HASH"`).
            Only applicable to <'UniqueKey'> and <'PrimaryKey'>.
        :param comment `<'str/None'>`: New COMMENT of the constraint.
            Only applicable to <'UniqueKey'> and <'PrimaryKey'>.
        :param visible `<'bool'>`: The visibility of the index.
            Only applicable to <'UniqueKey'>.
        :param reference_table `<'str/None'>`: New parent table for FOREIGN KEY.
            Only applicable to <'ForeignKey'>.
        :param reference_columns `<'str/Column/tuple/list/None'>`: New columns in the referenced table.
            Only applicable to <'ForeignKey'>.
        :param on_delete `<'str/None'>`: The action to take when the referenced row is deleted.
            Accepts: `"CASCADE"`, `"SET NULL"`, `"RESTRICT"`, `"NO ACTION"`, `"SET DEFAULT"`.
            Only applicable to <'ForeignKey'>.
        :param on_update `<'str/None'>`: The action to take when the referenced row is updated.
            Accepts: `"CASCADE"`, `"SET NULL"`, `"RESTRICT"`, `"NO ACTION"`, `"SET DEFAULT"`.
            Only applicable to <'ForeignKey'>.
        :param expression `<'str/None>`: New expression for CHECK constraint.
            Only applicable to <'Check'>.
        :param enforce `<'bool/None'>`: The enforcement state for the CHECK constraint.
            Only applicable to <'Check'>.
        """
        # Generate alter sql
        meta = self.ShowMetadata()
        # fmt: off
        query = self._gen_alter_query(
            meta, 
            columns, index_type, comment, visible,  # Unique/Primary Key
            reference_table, reference_columns, on_delete, on_update,  # Foreign Key
            expression, enforced,  # Check
        )
        # fmt: on
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
    def ShowMetadata(self) -> ConstraintMetadata:
        """[sync] Show the constraint metadata from the remote server `<'ConstraintMetadata'>`.

        :raises `<'OperationalError'>`: If the constraint does not exist.
        """
        self._raise_not_implemented_error("show_metadata")

    @cython.ccall
    def ShowConstraintSymbols(self) -> tuple[str]:
        """[sync] Show all the constraint symbols of the table `<'tuple[str]'>`.

        `Symbol` is the actual identifier used in the
        database to refer to the constraint.
        """
        sql: str = self._gen_show_constraint_symbols_sql()
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
                res = cur.fetchall()
        return self._flatten_rows(res)

    @cython.ccall
    def SyncFromRemote(self) -> Logs:
        """[sync] Synchronize the local constraint configs with the remote server `<'Logs'>`.

        ## Explanation
        - This method does `NOT` alter the remote server constraint,
          but only changes the local constraint configurations to match
          the remote server metadata.
        """
        try:
            meta = self.ShowMetadata()
        except sqlerrors.OperationalError as err:
            errno = err.args[0]
            if errno == 1082 or errno == 3821:
                return Logs().log_sync_failed_not_exist(self)
            raise err
        return self._sync_from_metadata(meta)

    @cython.ccall
    def SyncToRemote(self) -> Logs:
        """[sync] Synchronize the remote server constraint with local configs `<'Logs'>`.

        ## Explanation
        - This method compares the local constraint configurations with the
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
            self._comment,
            self._visible,
            self._reference_table,
            self._reference_columns,
            self._on_delete,
            self._on_update,
            self._expression,
            self._enforced,
        )

    # Async ---------------------------------------------------------------------------------
    async def aioInitialize(self, force: cython.bint = False) -> Logs:
        """[async] Initialize the constraint `<'Logs'>`.

        :param force `<'bool'>`: Whether to force the initialization even
            the constraint has already been initialized. Defaults to `False`.

        ## Explanation (difference from add)
        - Add the constraint to the table if not exists.
        """
        logs: Logs = Logs()
        # Skip
        if self._initialized and not force:
            return logs
        # Initialize: constraint
        if not await self.aioExists():
            logs.extend(await self.aioAdd())
        else:
            logs.extend(await self.aioSyncFromRemote())
        # Finished
        self._set_initialized(True)
        return logs

    async def aioAdd(self) -> Logs:
        """[async] Add the constraint to the table `<'Logs'>`.

        :raises `<'OperationalError'>`: If a constraint with the symbal name already exist.
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
        """[async] Check if the constraint exists in the table `<'bool'>`."""
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
        """[async] Drop the constraint from the table `<'Logs'>`.

        :raises `<'OperationalError'>`: If the constraint does not exist.
        """
        sql: str = self._gen_drop_sql()
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
        self._set_initialized(False)
        logs = Logs().log_sql(self, sql)
        return logs.log_element_deletion(self, False)

    async def aioAlter(self, *args, **kwargs) -> Logs:
        """[async] Alter the Constraint `<'Logs'>`."""
        self._raise_not_implemented_error("aio_alter")

    async def _aioAlter(
        self,
        columns: object | None,
        index_type: str | None,
        comment: str | None,
        visible: bool | None,
        reference_table: str | None,
        reference_columns: object | None,
        on_delete: str | None,
        on_update: str | None,
        expression: object | None,
        enforced: bool | None,
    ) -> Logs:
        """(internal) [async] Alter the constraint `<'Logs'>`.

        ## Notice
        - Value `None` means to retain the corresponding settings of the constraint.

        :param columns `<'str/Column/tuple/list/None'>`: New columns of the constraint.
            Only applicable to <'UniqueKey'>, <'PrimaryKey'> and <'ForeignKey'>.
        :param index_type `<'str/None'>`: New index algorithm (`"BTREE"` or `"HASH"`).
            Only applicable to <'UniqueKey'> and <'PrimaryKey'>.
        :param comment `<'str/None'>`: New COMMENT of the constraint.
            Only applicable to <'UniqueKey'> and <'PrimaryKey'>.
        :param visible `<'bool'>`: The visibility of the index.
            Only applicable to <'UniqueKey'>.
        :param reference_table `<'str/None'>`: New parent table for FOREIGN KEY.
            Only applicable to <'ForeignKey'>.
        :param reference_columns `<'str/Column/tuple/list/None'>`: New columns in the referenced table.
            Only applicable to <'ForeignKey'>.
        :param on_delete `<'str/None'>`: The action to take when the referenced row is deleted.
            Accepts: `"CASCADE"`, `"SET NULL"`, `"RESTRICT"`, `"NO ACTION"`, `"SET DEFAULT"`.
            Only applicable to <'ForeignKey'>.
        :param on_update `<'str/None'>`: The action to take when the referenced row is updated.
            Accepts: `"CASCADE"`, `"SET NULL"`, `"RESTRICT"`, `"NO ACTION"`, `"SET DEFAULT"`.
            Only applicable to <'ForeignKey'>.
        :param expression `<'str/None>`: New expression for CHECK constraint.
            Only applicable to <'Check'>.
        :param enforce `<'bool/None'>`: The enforcement state for the CHECK constraint.
            Only applicable to <'Check'>.
        """
        # Generate alter sql
        meta = await self.aioShowMetadata()
        # fmt: off
        query = self._gen_alter_query(
            meta, 
            columns, index_type, comment, visible,  # Unique/Primary Key
            reference_table, reference_columns, on_delete, on_update,  # Foreign Key
            expression, enforced,  # Check
        )
        # fmt: on
        # Execute alteration
        if query.executable():
            async with self._pool.acquire() as conn:
                async with conn.transaction() as cur:
                    await query.aio_execute(cur)
            # . refresh metadata
            meta = await self.aioShowMetadata()
        # Sync from remote
        return self._sync_from_metadata(meta, query._logs)

    async def aioShowMetadata(self) -> ConstraintMetadata:
        """[sync] Show the constraint metadata from the remote server `<'ConstraintMetadata'>`.

        :raises `<'OperationalError'>`: If the constraint does not exist.
        """
        self._raise_not_implemented_error("aio_show_metadata")

    async def aioShowConstraintSymbols(self) -> tuple[str]:
        """[sync] Show all the constraint symbols of the table `<'tuple[str]'>`.

        `Symbol` is the actual identifier used in the
        database to refer to the constraint.
        """
        sql: str = self._gen_show_constraint_symbols_sql()
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
                res = await cur.fetchall()
        return self._flatten_rows(res)

    async def aioSyncFromRemote(self) -> Logs:
        """[async] Synchronize the local constraint configs with the remote server `<'Logs'>`.

        ## Explanation
        - This method does `NOT` alter the remote server constraint,
          but only changes the local constraint configurations to match
          the remote server metadata.
        """
        try:
            meta = await self.aioShowMetadata()
        except sqlerrors.OperationalError as err:
            errno = err.args[0]
            if errno == 1082 or errno == 3821:
                return Logs().log_sync_failed_not_exist(self)
            raise err
        return self._sync_from_metadata(meta)

    async def aioSyncToRemote(self) -> Logs:
        """[async] Synchronize the remote server constraint with local configs `<'Logs'>`.

        ## Explanation
        - This method compares the local constraint configurations with the
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
            self._comment,
            self._visible,
            self._reference_table,
            self._reference_columns,
            self._on_delete,
            self._on_update,
            self._expression,
            self._enforced,
        )

    # Generate SQL -------------------------------------------------------------------------
    @cython.ccall
    def _gen_definition_sql(self) -> str:
        """(internal) Generate the definition SQL of the constraint `<'str'>`."""
        self._raise_not_implemented_error("_gen_definition_sql")

    @cython.ccall
    def _gen_add_sql(self) -> str:
        """(internal) Generate SQL to add the constraint to the table `<'str'>`"""
        self._assure_ready()
        return "ALTER TABLE %s ADD %s;" % (
            self._tb_qualified_name,
            self._gen_definition_sql(),
        )

    @cython.ccall
    def _gen_exists_sql(self) -> str:
        """(internal) Generate SQL to check if the constraint exists in the table `<'str'>`."""
        self._assure_ready()
        return (
            "SELECT 1 "
            "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS "
            "WHERE TABLE_NAME = '%s' "
            "AND CONSTRAINT_SCHEMA = '%s' "
            "AND CONSTRAINT_NAME = '%s' "
            "AND CONSTRAINT_SCHEMA = TABLE_SCHEMA "
            "LIMIT 1;" % (self._tb_name, self._db_name, self._symbol)
        )

    @cython.ccall
    def _gen_drop_sql(self) -> str:
        """(internal) Generate SQL to drop the constraint from the table `<'str'>`."""
        self._raise_not_implemented_error("_gen_drop_sql")

    @cython.ccall
    def _gen_alter_query(
        self,
        meta: ConstraintMetadata,
        columns: object | None,
        index_type: str | None,
        comment: str | None,
        visible: bool | None,
        reference_table: str | None,
        reference_columns: object | None,
        on_delete: str | None,
        on_update: str | None,
        expression: object | None,
        enforced: bool | None,
    ) -> Query:
        """(internal) Generate the query to alter the constraint `<'Query'>`.

        :param meta `<'IndexMetadata'>`: The remote server constraint metadata.
        :param columns `<'str/Column/tuple/list/None'>`: New columns of the constraint.
            Only applicable to <'UniqueKey'>, <'PrimaryKey'> and <'ForeignKey'>.
        :param index_type `<'str/None'>`: New index algorithm (`"BTREE"` or `"HASH"`).
            Only applicable to <'UniqueKey'> and <'PrimaryKey'>.
        :param comment `<'str/None'>`: New COMMENT of the constraint.
            Only applicable to <'UniqueKey'> and <'PrimaryKey'>.
        :param visible `<'bool'>`: The visibility of the index.
            Only applicable to <'UniqueKey'>.
        :param reference_table `<'str/None'>`: New parent table for FOREIGN KEY.
            Only applicable to <'ForeignKey'>.
        :param reference_columns `<'str/Column/tuple/list/None'>`: New columns in the referenced table.
            Only applicable to <'ForeignKey'>.
        :param on_delete `<'str/None'>`: The action to take when the referenced row is deleted.
            Accepts: `"CASCADE"`, `"SET NULL"`, `"RESTRICT"`, `"NO ACTION"`, `"SET DEFAULT"`.
            Only applicable to <'ForeignKey'>.
        :param on_update `<'str/None'>`: The action to take when the referenced row is updated.
            Accepts: `"CASCADE"`, `"SET NULL"`, `"RESTRICT"`, `"NO ACTION"`, `"SET DEFAULT"`.
            Only applicable to <'ForeignKey'>.
        :param expression `<'str/None>`: New expression for CHECK constraint.
            Only applicable to <'Check'>.
        :param enforce `<'bool/None'>`: The enforcement state for the CHECK constraint.
            Only applicable to <'Check'>.
        """
        self._raise_not_implemented_error("_gen_alter_query")

    @cython.ccall
    def _gen_show_metadata_sql(self) -> str:
        """(internal) Generate SQL to show constraint metadata `<'str'>`."""
        self._raise_not_implemented_error("_gen_show_metadata_sql")

    @cython.ccall
    def _gen_show_constraint_symbols_sql(self) -> str:
        """(internal) Generate SQL to select all constraint symbols of the table `<'str'>`."""
        self._assure_ready()
        return (
            "SELECT "
            "CONSTRAINT_NAME AS i "
            "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS "
            "WHERE TABLE_NAME = '%s' "
            "AND CONSTRAINT_SCHEMA = '%s' "
            "AND CONSTRAINT_SCHEMA = TABLE_SCHEMA;" % (self._tb_name, self._db_name)
        )

    # Metadata -----------------------------------------------------------------------------
    @cython.ccall
    def _sync_from_metadata(self, meta: ConstraintMetadata, logs: Logs = None) -> Logs:
        """(internal) Synchronize local configs with the remote constraint metadata `<'Logs'>`."""
        self._assure_ready()
        if logs is None:
            logs = Logs()

        # Validate
        if self._el_type != meta._el_type:
            logs.log_sync_failed_mismatch(
                self, "constraint type", self._el_type, meta._el_type
            )
            return logs._skip()  # exit
        if self._symbol != meta._constraint_name:
            logs.log_sync_failed_mismatch(
                self, "constraint symbol", self._symbol, meta._constraint_name
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

        # Enforced
        if self._enforced != meta._enforced:
            logs.log_config_bool(self, "enforced", self._enforced, meta._enforced)
            self._enforced = meta._enforced

        # Return logs
        return logs

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _diff_from_metadata(self, meta: ConstraintMetadata) -> cython.int:
        """(internal) Check if the constraint configurations are different
        from the remote server metadata `<'int'>`.

        :returns `<'int'>`:
        - `0`: Constraint configurations are identical.
        - `1`: Constraint configurations are different.
        """
        # Different
        if self._el_type != meta._el_type:
            return 1
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
        """Setup the constraint.

        :param tb_name `<'str'>`: The table name of the constraint.
        :param db_name `<'str'>`: The database name of the constraint.
        :param charset `<'str/Charset'>`: The charset of the constraint.
        :param collate `<'str/None'>`: The collation of the constraint.
        :param pool `<'Pool'>`: The pool of the constraint.
        """
        self._set_tb_name(tb_name)
        self._set_db_name(db_name)
        self._set_charset(charset, collate)
        self._set_pool(pool)
        return self._assure_ready()

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def set_name(self, name: object) -> cython.bint:
        """Set the name of the Constraint."""
        if Element.set_name(self, name):
            self._name = self._validate_constraint_name(name)
        return self._set_symbol()

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _set_tb_name(self, name: object) -> cython.bint:
        """(internal) Set the table name of the Constraint."""
        Element._set_tb_name(self, name)
        return self._set_symbol()

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _set_symbol(self) -> cython.bint:
        """(internal) Set the symbol of the Constraint.

        `Symbol` is the actual identifier used in the
        database to refer to the constraint.
        """
        if self._symbol is None:
            # . validate names
            name: str = self._name
            if name is None:
                return False  # exit
            tb_name: str = self._tb_name
            if tb_name is None:
                return False  # exit
            # . set symbol
            prefix: str = tb_name + "_"
            name_len: cython.Py_ssize_t = str_len(name)
            # name.startwith(prefix)
            if str_tailmatch(name, prefix, 0, name_len, -1):
                self._symbol = name
            # name.startwith(tb_name)
            elif str_tailmatch(name, tb_name, 0, name_len, -1):
                self._symbol = prefix + str_substr(name, str_len(tb_name), name_len)
            # concat
            else:
                self._symbol = prefix + name
        return True

    # Assure Ready -------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _assure_ready(self) -> cython.bint:
        """(internal) Assure the constraint is ready."""
        if not self._el_ready:
            self._assure_name_ready()
            self._assure_tb_name_ready()
            Element._assure_ready(self)
        return True

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def copy(self) -> Constraint:
        """Make a copy of the constraint `<'Constraint'>`."""
        cnst: Constraint = self._construct(
            self._columns,
            self._index_type,
            self._comment,
            self._visible,
            self._reference_table,
            self._reference_columns,
            self._on_delete,
            self._on_update,
            self._expression,
            self._enforced,
        )
        cnst.set_name(self._name)
        return cnst

    @cython.ccall
    def _construct(
        self,
        columns: object | None,
        index_type: str | None,
        comment: str | None,
        visible: bool | None,
        reference_table: str | None,
        reference_columns: object | None,
        on_delete: str | None,
        on_update: str | None,
        expression: object | None,
        enforced: bool | None,
    ) -> Constraint:
        """(internal) Construct a new constraint instance `<'Constraint'>`.

        ## Notice
        - Value `None` means to use the corresponding settings of this constraint.

        :param columns `<'str/Column/tuple/list/None'>`: The columns of the constraint.
            Only applicable to <'UniqueKey'>, <'PrimaryKey'> and <'ForeignKey'>.
        :param index_type `<'str/None'>`: The index algorithm (`"BTREE"` or `"HASH"`).
            Only applicable to <'UniqueKey'> and <'PrimaryKey'>.
        :param comment `<'str/None'>`: The COMMENT of the constraint.
            Only applicable to <'UniqueKey'> and <'PrimaryKey'>.
        :param visible `<'bool'>`: The visibility of the index.
            Only applicable to <'UniqueKey'>.
        :param reference_table `<'str/None'>`: The parent table for FOREIGN KEY.
            Only applicable to <'ForeignKey'>.
        :param reference_columns `<'str/Column/tuple/list/None'>`: The columns in the referenced table.
            Only applicable to <'ForeignKey'>.
        :param on_delete `<'str/None'>`: The action to take when the referenced row is deleted.
            Accepts: `"CASCADE"`, `"SET NULL"`, `"RESTRICT"`, `"NO ACTION"`, `"SET DEFAULT"`.
            Only applicable to <'ForeignKey'>.
        :param on_update `<'str/None'>`: The action to take when the referenced row is updated.
            Accepts: `"CASCADE"`, `"SET NULL"`, `"RESTRICT"`, `"NO ACTION"`, `"SET DEFAULT"`.
            Only applicable to <'ForeignKey'>.
        :param expression `<'str/None>`: The expression for CHECK constraint.
            Only applicable to <'Check'>.
        :param enforce `<'bool/None'>`: The enforcement state for the CHECK constraint.
            Only applicable to <'Check'>.
        """
        self._raise_not_implemented_error("_construct_constraint")

    # Special Methods ----------------------------------------------------------------------
    def __repr__(self) -> str:
        return "<%s (%s)>" % (self.__class__.__name__, self._gen_definition_sql())

    def __str__(self) -> str:
        self._assure_ready()
        return self._symbol


@cython.cclass
class UniqueKey(Constraint):
    """Represents a UNIQUE KEY constraint in a database table."""

    def __init__(
        self,
        *columns: str,
        index_type: str = None,
        comment: str = None,
        visible: cython.bint = True,
    ):
        """The UNIQUE KEY constraint in a database table.

        :param columns `<'*str'>`: The column names of the unique key (index).
        :param index_type `<'str/None'>`: The algorithm of the index. Defaults to `None`.
            Accepts: `"BTREE"`, `"HASH"` or `None` to use the engine default.
        :param comment `<'str/None'>`: The COMMENT of the index. Defaults to `None`.
        :param visible `<'bool'>`: The visibility of the index. Default to `True`.
            An invisible index is not used by the query optimizer, unless explicitly hinted.
        """
        super().__init__("UNIQUE KEY")
        self._columns = self._validate_columns(columns)
        self._index_type = self._validate_index_type(index_type)
        self._comment = self._validate_comment(comment)
        self._visible = visible

    # Property -----------------------------------------------------------------------------
    @property
    def columns(self) -> tuple[str]:
        """The column names of the unique key `<'tuple[str]'>`."""
        return self._columns

    @property
    def index_type(self) -> str | None:
        """The algorithm of the index (`"BTREE"` or `"HASH"`) `<'str/None'>`."""
        return self._index_type

    @property
    def comment(self) -> str | None:
        """The COMMENT of the index `<'str/None'>`."""
        return self._comment

    @property
    def visible(self) -> bool:
        """The visibility of the the index `<'bool'>`.

        An invisible index is not used by the query optimizer, unless explicitly hinted.
        """
        return self._visible

    # Sync ---------------------------------------------------------------------------------
    def Alter(
        self,
        columns: object | None = None,
        index_type: str | None = None,
        comment: str | None = None,
        visible: bool | None = None,
    ) -> Logs:
        """[sync] Alter the unique key `<'Logs'>`.

        ## Notice
        - Default value `None` means to retain the corresponding settings of the unique key.
        - Alteration of 'visible' simply toggles the visibility of the unique key.
        - Alteration of other options leads to `DROP` & `RE-CREATE` of the unique key.

        :param columns `<'str/Column/list/tuple/None'>`: New columns of the unique key. Defaults to `None`.
        :param index_type `<'str/None'>`: New index algorithm (`"BTREE"` or `"HASH"`). Defaults to `None`.
        :param comment `<'str/None'>`: New COMMENT of the index. Defaults to `None`.
            To remove existing comment, set comment to `''`.
        :param visible `<'bool/None'>`: The visibility of the index. Defaults to `None`.
        """
        # fmt: off
        return self._Alter(
            columns, index_type, comment, visible,
            None, None, None, None, None, None,
        )
        # fmt: on

    @cython.ccall
    def SetVisible(self, visible: cython.bint) -> Logs:
        """[sync] Toggles the visibility of the unique key `<'Logs'>`.

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
    def ShowMetadata(self) -> ConstraintMetadata:
        """[sync] Show the unique key metadata from the remote server `<'ConstraintMetadata'>`.

        :raises `<'OperationalError'>`: If the unique key does not exist.
        """
        sql: str = self._gen_show_metadata_sql()
        with self._pool.acquire() as conn:
            conn.set_decode_json(False)
            with conn.transaction(DictCursor) as cur:
                cur.execute(sql)
                res = cur.fetchall()
        if tuple_len(res) == 0:
            self._raise_operational_error(
                1082, "(symbol '%s') does not exist" % self._symbol
            )
        return UniPriKeyMetadata(res)

    # Async ---------------------------------------------------------------------------------
    async def aioAlter(
        self,
        columns: object | None = None,
        index_type: str | None = None,
        comment: str | None = None,
        visible: bool | None = None,
    ) -> Logs:
        """[async] Alter the unique key `<'Logs'>`.

        ## Notice
        - Default value `None` means to retain the corresponding settings of the unique key.
        - Alteration of 'visible' simply toggles the visibility of the unique key.
        - Alteration of other options leads to `DROP` & `RE-CREATE` of the unique key.

        :param columns `<'str/Column/list/tuple/None'>`: New columns of the unique key. Defaults to `None`.
        :param index_type `<'str/None'>`: New index algorithm (`"BTREE"` or `"HASH"`). Defaults to `None`.
        :param comment `<'str/None'>`: New COMMENT of the index. Defaults to `None`.
            To remove existing comment, set comment to `''`.
        :param visible `<'bool/None'>`: The visibility of the index. Defaults to `None`.
        """
        # fmt: off
        return await self._aioAlter(
            columns, index_type, comment, visible,
            None, None, None, None, None, None,
        )
        # fmt: on

    async def aioSetVisible(self, visible: cython.bint) -> Logs:
        """[async] Toggles the visibility of the unique key `<'Logs'>`.

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

    async def aioShowMetadata(self) -> ConstraintMetadata:
        """[async] Show the unique key metadata from the remote server `<'ConstraintMetadata'>`.

        :raises `<'OperationalError'>`: If the unique key does not exist.
        """
        sql: str = self._gen_show_metadata_sql()
        async with self._pool.acquire() as conn:
            conn.set_decode_json(False)
            async with conn.transaction(AioDictCursor) as cur:
                await cur.execute(sql)
                res = await cur.fetchall()
        if tuple_len(res) == 0:
            self._raise_operational_error(
                1082, "(symbol '%s') does not exist" % self._symbol
            )
        return UniPriKeyMetadata(res)

    # Generate SQL -------------------------------------------------------------------------
    @cython.ccall
    def _gen_definition_sql(self) -> str:
        """(internal) Generate the definition SQL of the constraint `<'str'>`."""
        self._assure_ready()
        sql: str = "CONSTRAINT %s %s (%s)" % (
            self._name,
            self._el_type,
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
    def _gen_drop_sql(self) -> str:
        """(internal) Generate SQL to drop the constraint from the table `<'str'>`."""
        self._assure_ready()
        return "ALTER TABLE %s DROP INDEX %s;" % (self._tb_qualified_name, self._name)

    @cython.ccall
    def _gen_alter_query(
        self,
        meta: ConstraintMetadata,
        columns: object | None,
        index_type: str | None,
        comment: str | None,
        visible: bool | None,
        reference_table: str | None,
        reference_columns: object | None,
        on_delete: str | None,
        on_update: str | None,
        expression: object | None,
        enforced: bool | None,
    ) -> Query:
        """(internal) Generate the query to alter the constraint `<'Query'>`.

        :param meta `<'IndexMetadata'>`: The remote server constraint metadata.
        :param columns `<'str/Column/tuple/list/None'>`: New columns of the constraint.
            Only applicable to <'UniqueKey'>, <'PrimaryKey'> and <'ForeignKey'>.
        :param index_type `<'str/None'>`: New index algorithm (`"BTREE"` or `"HASH"`).
            Only applicable to <'UniqueKey'> and <'PrimaryKey'>.
        :param comment `<'str/None'>`: New COMMENT of the constraint.
            Only applicable to <'UniqueKey'> and <'PrimaryKey'>.
        :param visible `<'bool'>`: The visibility of the index.
            Only applicable to <'UniqueKey'>.
        :param reference_table `<'str/None'>`: New parent table for FOREIGN KEY.
            Only applicable to <'ForeignKey'>.
        :param reference_columns `<'str/Column/tuple/list/None'>`: New columns in the referenced table.
            Only applicable to <'ForeignKey'>.
        :param on_delete `<'str/None'>`: The action to take when the referenced row is deleted.
            Accepts: `"CASCADE"`, `"SET NULL"`, `"RESTRICT"`, `"NO ACTION"`, `"SET DEFAULT"`.
            Only applicable to <'ForeignKey'>.
        :param on_update `<'str/None'>`: The action to take when the referenced row is updated.
            Accepts: `"CASCADE"`, `"SET NULL"`, `"RESTRICT"`, `"NO ACTION"`, `"SET DEFAULT"`.
            Only applicable to <'ForeignKey'>.
        :param expression `<'str/None>`: New expression for CHECK constraint.
            Only applicable to <'Check'>.
        :param enforce `<'bool/None'>`: The enforcement state for the CHECK constraint.
            Only applicable to <'Check'>.
        """
        self._assure_ready()
        query = Query()

        # New Constraint
        # fmt: off
        cnst: Constraint = self._construct(
            columns, index_type, comment, visible,  # Unique/Primary Key
            reference_table, reference_columns, on_delete, on_update,  # Foreign Key
            expression, enforced,  # Check
        )
        # fmt: on
        cnst.set_name(self._name)
        cnst.setup(self._tb_name, self._db_name, self._charset, None, self._pool)

        # Compare differences
        diff = cnst._diff_from_metadata(meta)
        # . no differences
        if diff == 0:
            return query
        # . drop & re-create
        if diff == 1:
            query.set_sql(self, cnst._gen_drop_sql())
            query.set_sql(self, cnst._gen_add_sql())
        # . toggle visibility
        else:
            query.set_sql(self, self._gen_set_visible_sql(cnst._visible))
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
        """(internal) Generate SQL to show constraint metadata `<'str'>`."""
        self._assure_ready()
        return (
            "SELECT "
            # . columns[t1]
            "t1.CONSTRAINT_CATALOG AS CATALOG_NAME, "
            "t1.CONSTRAINT_SCHEMA AS SCHEMA_NAME, "
            "t1.TABLE_NAME AS TABLE_NAME, "
            "t1.CONSTRAINT_NAME AS CONSTRAINT_NAME, "
            "UPPER(t1.CONSTRAINT_TYPE) AS CONSTRAINT_TYPE, "
            "t1.ENFORCED AS ENFORCED, "
            # . columns[t2]
            "t2.ENGINE_ATTRIBUTE AS ENGINE_ATTRIBUTE, "
            "t2.SECONDARY_ENGINE_ATTRIBUTE AS SECONDARY_ENGINE_ATTRIBUTE, "
            # . columns[t3]
            "UPPER(t3.INDEX_TYPE) AS INDEX_TYPE, "
            "t3.NON_UNIQUE AS NON_UNIQUE, "
            "t3.COMMENT AS COMMENT, "
            "t3.INDEX_COMMENT AS INDEX_COMMENT, "
            "UPPER(t3.IS_VISIBLE) AS IS_VISIBLE, "
            "t3.COLUMN_NAME AS COLUMN_NAME, "
            "t3.EXPRESSION AS EXPRESSION, "
            "t3.SEQ_IN_INDEX AS SEQ_IN_INDEX, "
            "UPPER(t3.COLLATION) AS COLLATION, "
            "t3.CARDINALITY AS CARDINALITY, "
            "t3.SUB_PART AS SUB_PART, "
            "t3.PACKED AS PACKED "
            # . information_schema.table_constraints
            "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS t1 "
            # . information_schema.table_constraints_extensions
            "LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS_EXTENSIONS AS t2 "
            "ON t1.TABLE_NAME = t2.TABLE_NAME "
            "AND t1.CONSTRAINT_SCHEMA = t2.CONSTRAINT_SCHEMA "
            "AND t1.CONSTRAINT_NAME = t2.CONSTRAINT_NAME "
            # . information_schema.statistics
            "JOIN INFORMATION_SCHEMA.STATISTICS AS t3 "
            "ON t1.TABLE_NAME = t3.TABLE_NAME "
            "AND t1.CONSTRAINT_SCHEMA = t3.INDEX_SCHEMA "
            "AND t1.CONSTRAINT_NAME = t3.INDEX_NAME "
            "AND t3.INDEX_SCHEMA = t3.TABLE_SCHEMA "
            # . conditions
            "WHERE t1.TABLE_NAME = '%s' "
            "AND t1.CONSTRAINT_SCHEMA = '%s' "
            "AND t1.CONSTRAINT_NAME = '%s' "
            "AND t1.CONSTRAINT_SCHEMA = t1.TABLE_SCHEMA "
            "ORDER BY t3.SEQ_IN_INDEX ASC;"
            % (self._tb_name, self._db_name, self._symbol)
        )

    # Metadata -----------------------------------------------------------------------------
    @cython.ccall
    def _sync_from_metadata(self, meta: ConstraintMetadata, logs: Logs = None) -> Logs:
        """(internal) Synchronize local configs with the remote constraint metadata `<'Logs'>`."""
        # Base configs
        logs = Constraint._sync_from_metadata(self, meta, logs)
        if logs._skip_flag:
            return logs  # exit

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
    def _diff_from_metadata(self, meta: ConstraintMetadata) -> cython.int:
        """(internal) Check if the constraint configurations are different
        from the remote server metadata `<'int'>`.

        :returns `<'int'>`:
        - `0`: Constraint configurations are identical.
        - `1`: Constraint configurations differ in more than visibility.
        - `2`: Only the visibility differs.
        """
        # Different
        diff = Constraint._diff_from_metadata(self, meta)
        if diff == 1:
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
        return diff

    # Setter -------------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _set_symbol(self) -> cython.bint:
        """(internal) Set the symbol of the Constraint.

        `Symbol` is the actual identifier used in the
        database to refer to the constraint.
        """
        if self._symbol is None:
            # . validate name
            name: str = self._name
            if name is None:
                return False  # exit
            # . set symbol
            self._symbol = name
        return True

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def _construct(
        self,
        columns: object | None,
        index_type: str | None,
        comment: str | None,
        visible: bool | None,
        reference_table: str | None,
        reference_columns: object | None,
        on_delete: str | None,
        on_update: str | None,
        expression: object | None,
        enforced: bool | None,
    ) -> Constraint:
        """(internal) Construct a new constraint instance `<'Constraint'>`.

        ## Notice
        - Value `None` means to use the corresponding settings of this constraint.

        :param columns `<'str/Column/tuple/list/None'>`: The columns of the constraint.
            Only applicable to <'UniqueKey'>, <'PrimaryKey'> and <'ForeignKey'>.
        :param index_type `<'str/None'>`: The index algorithm (`"BTREE"` or `"HASH"`).
            Only applicable to <'UniqueKey'> and <'PrimaryKey'>.
        :param comment `<'str/None'>`: The COMMENT of the constraint.
            Only applicable to <'UniqueKey'> and <'PrimaryKey'>.
        :param visible `<'bool'>`: The visibility of the index.
            Only applicable to <'UniqueKey'>.
        :param reference_table `<'str/None'>`: The parent table for FOREIGN KEY.
            Only applicable to <'ForeignKey'>.
        :param reference_columns `<'str/Column/tuple/list/None'>`: The columns in the referenced table.
            Only applicable to <'ForeignKey'>.
        :param on_delete `<'str/None'>`: The action to take when the referenced row is deleted.
            Accepts: `"CASCADE"`, `"SET NULL"`, `"RESTRICT"`, `"NO ACTION"`, `"SET DEFAULT"`.
            Only applicable to <'ForeignKey'>.
        :param on_update `<'str/None'>`: The action to take when the referenced row is updated.
            Accepts: `"CASCADE"`, `"SET NULL"`, `"RESTRICT"`, `"NO ACTION"`, `"SET DEFAULT"`.
            Only applicable to <'ForeignKey'>.
        :param expression `<'str/None>`: The expression for CHECK constraint.
            Only applicable to <'Check'>.
        :param enforce `<'bool/None'>`: The enforcement state for the CHECK constraint.
            Only applicable to <'Check'>.
        """
        columns = self._columns if columns is None else self._validate_columns(columns)
        return UniqueKey(
            # fmt: off
            *columns,
            index_type=self._index_type if index_type is None else self._validate_index_type(index_type),
            comment=self._comment if comment is None else self._validate_comment(comment),
            visible=self._visible if visible is None else bool(visible),
            # fmt: on
        )

    # Special Methods ----------------------------------------------------------------------
    def __str__(self) -> str:
        self._assure_ready()
        return self._name


@cython.cclass
class PrimaryKey(UniqueKey):
    """Represents a PRIMARY KEY constraint in a database table."""

    def __init__(self, *columns: str, index_type: str = None, comment: str = None):
        """The PRIMARY KEY constraint in a database table.

        :param columns `<'*str'>`: The column names of the primary key (index).
        :param index_type `<'str/None'>`: The algorithm of the index. Defaults to `None`.
            Accepts: `"BTREE"`, `"HASH"` or `None` to use the engine default.
        :param comment `<'str/None'>`: The COMMENT of the index. Defaults to `None`.
        """
        super().__init__(*columns, index_type=index_type, comment=comment, visible=True)
        self._set_el_type("PRIMARY KEY")
        self._symbol = "PRIMARY"

    # Property -----------------------------------------------------------------------------
    @property
    def columns(self) -> tuple[str]:
        """The column names of the primary key `<'tuple[str]'>`."""
        return self._columns

    @property
    def visible(self) -> bool:
        """The visibility of the the primary key `<'bool'>`.

        PRIMARY KEY is always visible.
        """
        return self._visible

    # Sync ---------------------------------------------------------------------------------
    def Alter(
        self,
        columns: object | None = None,
        index_type: str | None = None,
        comment: str | None = None,
    ) -> Logs:
        """[sync] Alter the primary key `<'Logs'>`.

        ## Notice
        - Default value `None` means to retain the corresponding settings of the primary key.
        - Alteration leads to `DROP` & `RE-CREATE` of the primary key.

        :param columns `<'str/Column/list/tuple/None'>`: New columns of the primary key. Defaults to `None`.
        :param index_type `<'str/None'>`: New index algorithm (`"BTREE"` or `"HASH"`). Defaults to `None`.
        :param comment `<'str/None'>`: New COMMENT of the index. Defaults to `None`.
        """
        # fmt: off
        return self._Alter(
            columns, index_type, comment, None,
            None, None, None, None, None, None,
        )
        # fmt: on

    @cython.ccall
    def SetVisible(self, visible: cython.bint) -> Logs:
        """[sync] Toggles the visibility of the primary key `<'Logs'>`.

        :param visible `<'bool'>`: The visibility of the index.

        ## Notice
        - PRIMARY KEY cannot be invisible. Calling this method raises `OperationalError`.
        """
        if not visible:
            self._raise_operational_error(3522, "cannot be invisible")
        return Logs()

    # Async ---------------------------------------------------------------------------------
    async def aioAlter(
        self,
        columns: object | None = None,
        index_type: str | None = None,
        comment: str | None = None,
    ) -> Logs:
        """[async] Alter the primary key `<'Logs'>`.

        ## Notice
        - Default value `None` means to retain the corresponding settings of the primary key.
        - Alteration leads to `DROP` & `RE-CREATE` of the primary key.

        :param columns `<'str/Column/list/tuple/None'>`: New columns of the primary key. Defaults to `None`.
        :param index_type `<'str/None'>`: New index algorithm (`"BTREE"` or `"HASH"`). Defaults to `None`.
        :param comment `<'str/None'>`: New COMMENT of the index. Defaults to `None`.
        """
        # fmt: off
        return await self._aioAlter(
            columns, index_type, comment, None,
            None, None, None, None, None, None,
        )
        # fmt: on

    async def aioSetVisible(self, visible: cython.bint) -> Logs:
        """[async] Toggles the visibility of the primary key `<'Logs'>`.

        :param visible `<'bool'>`: The visibility of the index.

        ## Notice
        - PRIMARY KEY cannot be invisible. Calling this method raises `OperationalError`.
        """
        if not visible:
            self._raise_operational_error(3522, "cannot be invisible")
        return Logs()

    # Generate SQL -------------------------------------------------------------------------
    @cython.ccall
    def _gen_drop_sql(self) -> str:
        """(internal) Generate SQL to drop the constraint from the table `<'str'>`."""
        self._assure_ready()
        return "ALTER TABLE %s DROP PRIMARY KEY;" % self._tb_qualified_name

    @cython.ccall
    def _gen_alter_query(
        self,
        meta: ConstraintMetadata,
        columns: object | None,
        index_type: str | None,
        comment: str | None,
        visible: bool | None,
        reference_table: str | None,
        reference_columns: object | None,
        on_delete: str | None,
        on_update: str | None,
        expression: object | None,
        enforced: bool | None,
    ) -> Query:
        """(internal) Generate the query to alter the constraint `<'Query'>`.

        :param meta `<'IndexMetadata'>`: The remote server constraint metadata.
        :param columns `<'str/Column/tuple/list/None'>`: New columns of the constraint.
            Only applicable to <'UniqueKey'>, <'PrimaryKey'> and <'ForeignKey'>.
        :param index_type `<'str/None'>`: New index algorithm (`"BTREE"` or `"HASH"`).
            Only applicable to <'UniqueKey'> and <'PrimaryKey'>.
        :param comment `<'str/None'>`: New COMMENT of the constraint.
            Only applicable to <'UniqueKey'> and <'PrimaryKey'>.
        :param visible `<'bool'>`: The visibility of the index.
            Only applicable to <'UniqueKey'>.
        :param reference_table `<'str/None'>`: New parent table for FOREIGN KEY.
            Only applicable to <'ForeignKey'>.
        :param reference_columns `<'str/Column/tuple/list/None'>`: New columns in the referenced table.
            Only applicable to <'ForeignKey'>.
        :param on_delete `<'str/None'>`: The action to take when the referenced row is deleted.
            Accepts: `"CASCADE"`, `"SET NULL"`, `"RESTRICT"`, `"NO ACTION"`, `"SET DEFAULT"`.
            Only applicable to <'ForeignKey'>.
        :param on_update `<'str/None'>`: The action to take when the referenced row is updated.
            Accepts: `"CASCADE"`, `"SET NULL"`, `"RESTRICT"`, `"NO ACTION"`, `"SET DEFAULT"`.
            Only applicable to <'ForeignKey'>.
        :param expression `<'str/None>`: New expression for CHECK constraint.
            Only applicable to <'Check'>.
        :param enforce `<'bool/None'>`: The enforcement state for the CHECK constraint.
            Only applicable to <'Check'>.
        """
        self._assure_ready()
        query = Query()

        # New Constraint
        # fmt: off
        cnst: Constraint = self._construct(
            columns, index_type, comment, visible,  # Unique/Primary Key
            reference_table, reference_columns, on_delete, on_update,  # Foreign Key
            expression, enforced,  # Check
        )
        # fmt: on
        cnst.set_name(self._name)
        cnst.setup(self._tb_name, self._db_name, self._charset, None, self._pool)

        # Compare differences
        diff = cnst._diff_from_metadata(meta)
        # . no differences
        if diff != 1:
            return query
        # . drop & re-create
        else:
            query.set_sql(self, cnst._gen_drop_sql())
            query.set_sql(self, cnst._gen_add_sql())
        return query

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def _construct(
        self,
        columns: object | None,
        index_type: str | None,
        comment: str | None,
        visible: bool | None,
        reference_table: str | None,
        reference_columns: object | None,
        on_delete: str | None,
        on_update: str | None,
        expression: object | None,
        enforced: bool | None,
    ) -> Constraint:
        """(internal) Construct a new constraint instance `<'Constraint'>`.

        ## Notice
        - Value `None` means to use the corresponding settings of this constraint.

        :param columns `<'str/Column/tuple/list/None'>`: The columns of the constraint.
            Only applicable to <'UniqueKey'>, <'PrimaryKey'> and <'ForeignKey'>.
        :param index_type `<'str/None'>`: The index algorithm (`"BTREE"` or `"HASH"`).
            Only applicable to <'UniqueKey'> and <'PrimaryKey'>.
        :param comment `<'str/None'>`: The COMMENT of the constraint.
            Only applicable to <'UniqueKey'> and <'PrimaryKey'>.
        :param visible `<'bool'>`: The visibility of the index.
            Only applicable to <'UniqueKey'>.
        :param reference_table `<'str/None'>`: The parent table for FOREIGN KEY.
            Only applicable to <'ForeignKey'>.
        :param reference_columns `<'str/Column/tuple/list/None'>`: The columns in the referenced table.
            Only applicable to <'ForeignKey'>.
        :param on_delete `<'str/None'>`: The action to take when the referenced row is deleted.
            Accepts: `"CASCADE"`, `"SET NULL"`, `"RESTRICT"`, `"NO ACTION"`, `"SET DEFAULT"`.
            Only applicable to <'ForeignKey'>.
        :param on_update `<'str/None'>`: The action to take when the referenced row is updated.
            Accepts: `"CASCADE"`, `"SET NULL"`, `"RESTRICT"`, `"NO ACTION"`, `"SET DEFAULT"`.
            Only applicable to <'ForeignKey'>.
        :param expression `<'str/None>`: The expression for CHECK constraint.
            Only applicable to <'Check'>.
        :param enforce `<'bool/None'>`: The enforcement state for the CHECK constraint.
            Only applicable to <'Check'>.
        """
        columns = self._columns if columns is None else self._validate_columns(columns)
        return PrimaryKey(
            # fmt: off
            *columns,
            index_type=self._index_type if index_type is None else self._validate_index_type(index_type),
            comment=self._comment if comment is None else self._validate_comment(comment),
            # fmt: on
        )

    # Special Methods ----------------------------------------------------------------------
    def __str__(self) -> str:
        self._assure_ready()
        return self._symbol


@cython.cclass
class ForeignKey(Constraint):
    """Represents a FOREIGN KEY constraint in a database table."""

    def __init__(
        self,
        columns: str | tuple[str] | list[str],
        reference_table: str,
        reference_columns: str | tuple[str] | list[str],
        on_delete: str | None = None,
        on_update: str | None = None,
    ):
        """The FOREIGN KEY constraint in a database table.

        :param columns `<'str/tuple[str]/list[str]'>`: The column names in the child table for the FOREIGN KEY.
        :param reference_table `<'str'>`: The parent table for FOREIGN KEY.
        :param reference_columns `<'str/tuple[str]/list[str]'>`: The column names in the parent table.
        :param on_delete `<'str/None'>`: The action to take when the referenced row is deleted. Defaults to `None`.
            Accepts: `"CASCADE"`, `"SET NULL"`, `"RESTRICT"`, `"NO ACTION"`, `"SET DEFAULT"`.
        :param on_update `<'str/None'>`: The action to take when the referenced row is updated. Defaults to `None`.
            Accepts: `"CASCADE"`, `"SET NULL"`, `"RESTRICT"`, `"NO ACTION"`, `"SET DEFAULT"`.

        ## Referential Actions:
        - `CASCADE`: Delete or update the row from the parent table and automatically delete or update
            the matching rows in the child table. Both ON DELETE CASCADE and ON UPDATE CASCADE are supported.
            Between two tables, do not define several ON UPDATE CASCADE clauses that act on the same column
            in the parent table or in the child table. If a FOREIGN KEY clause is defined on both tables in
            a foreign key relationship, making both tables a parent and child, an ON UPDATE CASCADE or ON
            DELETE CASCADE subclause defined for one FOREIGN KEY clause must be defined for the other in order
            for cascading operations to succeed. If an ON UPDATE CASCADE or ON DELETE CASCADE subclause is only
            defined for one FOREIGN KEY clause, cascading operations fail with an error.
        - `SET NULL`: Delete or update the row from the parent table and set the foreign key column or columns
            in the child table to NULL. Both ON DELETE SET NULL and ON UPDATE SET NULL clauses are supported.
            If you specify a SET NULL action, make sure that you have not declared the columns in the child
            table as NOT NULL.
        - `RESTRICT`: Rejects the delete or update operation for the parent table. Specifying RESTRICT
            (or NO ACTION) is the same as omitting the ON DELETE or ON UPDATE clause.
        - `NO ACTION`: A keyword from standard SQL. For InnoDB, this is equivalent to RESTRICT; the delete
            or update operation for the parent table is immediately rejected if there is a related foreign
            key value in the referenced table. NDB supports deferred checks, and NO ACTION specifies a deferred
            check; when this is used, constraint checks are not performed until commit time. Note that for
            NDB tables, this causes all foreign key checks made for both parent and child tables to be deferred.
        """
        super().__init__("FOREIGN KEY")
        self._columns = self._validate_columns(columns)
        self._reference_table = self._validate_table_name(reference_table)
        self._reference_columns = self._validate_columns(reference_columns)
        self._on_delete = self._validate_foreign_key_action(on_delete)
        self._on_update = self._validate_foreign_key_action(on_update)

    # Property -----------------------------------------------------------------------------
    @property
    def columns(self) -> tuple[str]:
        """The column names in the child table `<'tuple[str]'>`."""
        return self._columns

    @property
    def reference_table(self) -> str:
        """The name of the parent table `<'str'>`."""
        return self._reference_table

    @property
    def reference_columns(self) -> tuple[str]:
        """The column names in the parent table `<'tuple[str]'>`."""
        return self._reference_columns

    @property
    def on_delete(self) -> str | None:
        """The action to perform when the referenced row is deleted `<'str/None'>`."""
        return self._on_delete

    @property
    def on_update(self) -> str | None:
        """The action to perform when the referenced row is updated `<'str/None'>`."""
        return self._on_update

    # Sync ---------------------------------------------------------------------------------
    def Alter(
        self,
        columns: object | None = None,
        reference_table: object | None = None,
        reference_columns: object | None = None,
        on_delete: str | None = None,
        on_update: str | None = None,
    ) -> Logs:
        """[sync] Alter the FOREIGN KEY `<'Logs'>`.

        ## Notice
        - Default value `None` means to retain the corresponding settings of the foreign key.
        - Alteration leads to `DROP` & `RE-CREATE` of the foreign key.

        :param columns `<'str/Column/list/tuple/None'>`: New columns for the child table of the foreign key. Defaults to `None`.
        :param reference_table `<'str/Table/None'>`: New parent table for the foreign key. Defaults to `None`.
        :param reference_columns `<'str/Column/list/tuple/None'>`: New columns in the parent table for the foreign key. Defaults to `None`.
        :param on_delete `<'str/None'>`: New action to take when the referenced row is deleted. Defaults to `None`.
            Accepts: `"CASCADE"`, `"SET NULL"`, `"RESTRICT"`, `"NO ACTION"`, `"SET DEFAULT"`.
        :param on_update `<'str/None'>`: New action to take when the referenced row is updated. Defaults to `None`.
            Accepts: `"CASCADE"`, `"SET NULL"`, `"RESTRICT"`, `"NO ACTION"`, `"SET DEFAULT"`.
        """
        # fmt: off
        return self._Alter(
            columns, None, None, None,
            reference_table, reference_columns, on_delete, on_update, 
            None, None,
        )
        # fmt: on

    @cython.ccall
    def ShowMetadata(self) -> ConstraintMetadata:
        """[sync] Show the foreign key metadata from the remote server `<'ConstraintMetadata'>`.

        :raises `<'OperationalError'>`: If the constraint does not exist.
        """
        sql: str = self._gen_show_metadata_sql()
        with self._pool.acquire() as conn:
            conn.set_decode_json(False)
            with conn.transaction(DictCursor) as cur:
                cur.execute(sql)
                res = cur.fetchall()
        if tuple_len(res) == 0:
            self._raise_operational_error(
                1082, "(symbol '%s') does not exist" % self._symbol
            )
        return ForeignKeyMetadata(res)

    # Async ---------------------------------------------------------------------------------
    async def aioAlter(
        self,
        columns: object | None = None,
        reference_table: object | None = None,
        reference_columns: object | None = None,
        on_delete: str | None = None,
        on_update: str | None = None,
    ) -> Logs:
        """[async] Alter the FOREIGN KEY `<'Logs'>`.

        ## Notice
        - Default value `None` means to retain the corresponding settings of the foreign key.
        - Alteration leads to `DROP` & `RE-CREATE` of the foreign key.

        :param columns `<'str/Column/list/tuple/None'>`: New columns for the child table of the foreign key. Defaults to `None`.
        :param reference_table `<'str/Table/None'>`: New parent table for the foreign key. Defaults to `None`.
        :param reference_columns `<'str/Column/list/tuple/None'>`: New columns in the parent table for the foreign key. Defaults to `None`.
        :param on_delete `<'str/None'>`: New action to take when the referenced row is deleted. Defaults to `None`.
            Accepts: `"CASCADE"`, `"SET NULL"`, `"RESTRICT"`, `"NO ACTION"`, `"SET DEFAULT"`.
        :param on_update `<'str/None'>`: New action to take when the referenced row is updated. Defaults to `None`.
            Accepts: `"CASCADE"`, `"SET NULL"`, `"RESTRICT"`, `"NO ACTION"`, `"SET DEFAULT"`.
        """
        # fmt: off
        return await self._aioAlter(
            columns, None, None, None,
            reference_table, reference_columns, on_delete, on_update, 
            None, None,
        )
        # fmt: on

    async def aioShowMetadata(self) -> ConstraintMetadata:
        """[async] Show the foreign key metadata from the remote server `<'ConstraintMetadata'>`.

        :raises `<'OperationalError'>`: If the constraint does not exist.
        """
        sql: str = self._gen_show_metadata_sql()
        async with self._pool.acquire() as conn:
            conn.set_decode_json(False)
            async with conn.transaction(AioDictCursor) as cur:
                await cur.execute(sql)
                res = await cur.fetchall()
        if tuple_len(res) == 0:
            self._raise_operational_error(
                1082, "(symbol '%s') does not exist" % self._symbol
            )
        return ForeignKeyMetadata(res)

    # Generate SQL -------------------------------------------------------------------------
    @cython.ccall
    def _gen_definition_sql(self) -> str:
        """(internal) Generate the definition SQL of the constraint `<'str'>`."""
        self._assure_ready()
        sql: str = "CONSTRAINT %s %s (%s) REFERENCES %s (%s)" % (
            self._symbol,
            self._el_type,
            ", ".join(self._columns),
            self._gen_tb_qualified_name(self._reference_table),
            ", ".join(self._reference_columns),
        )
        if self._on_delete is not None:
            sql += " ON DELETE %s" % self._on_delete
        if self._on_update is not None:
            sql += " ON UPDATE %s" % self._on_update
        return sql

    @cython.ccall
    def _gen_drop_sql(self) -> str:
        """(internal) Generate SQL to drop the constraint from the table `<'str'>`."""
        self._assure_ready()
        return "ALTER TABLE %s DROP FOREIGN KEY %s;" % (
            self._tb_qualified_name,
            self._symbol,
        )

    @cython.ccall
    def _gen_alter_query(
        self,
        meta: ConstraintMetadata,
        columns: object | None,
        index_type: str | None,
        comment: str | None,
        visible: bool | None,
        reference_table: str | None,
        reference_columns: object | None,
        on_delete: str | None,
        on_update: str | None,
        expression: object | None,
        enforced: bool | None,
    ) -> Query:
        """(internal) Generate the query to alter the constraint `<'Query'>`.

        :param meta `<'IndexMetadata'>`: The remote server constraint metadata.
        :param columns `<'str/Column/tuple/list/None'>`: New columns of the constraint.
            Only applicable to <'UniqueKey'>, <'PrimaryKey'> and <'ForeignKey'>.
        :param index_type `<'str/None'>`: New index algorithm (`"BTREE"` or `"HASH"`).
            Only applicable to <'UniqueKey'> and <'PrimaryKey'>.
        :param comment `<'str/None'>`: New COMMENT of the constraint.
            Only applicable to <'UniqueKey'> and <'PrimaryKey'>.
        :param visible `<'bool'>`: The visibility of the index.
            Only applicable to <'UniqueKey'>.
        :param reference_table `<'str/None'>`: New parent table for FOREIGN KEY.
            Only applicable to <'ForeignKey'>.
        :param reference_columns `<'str/Column/tuple/list/None'>`: New columns in the referenced table.
            Only applicable to <'ForeignKey'>.
        :param on_delete `<'str/None'>`: The action to take when the referenced row is deleted.
            Accepts: `"CASCADE"`, `"SET NULL"`, `"RESTRICT"`, `"NO ACTION"`, `"SET DEFAULT"`.
            Only applicable to <'ForeignKey'>.
        :param on_update `<'str/None'>`: The action to take when the referenced row is updated.
            Accepts: `"CASCADE"`, `"SET NULL"`, `"RESTRICT"`, `"NO ACTION"`, `"SET DEFAULT"`.
            Only applicable to <'ForeignKey'>.
        :param expression `<'str/None>`: New expression for CHECK constraint.
            Only applicable to <'Check'>.
        :param enforce `<'bool/None'>`: The enforcement state for the CHECK constraint.
            Only applicable to <'Check'>.
        """
        self._assure_ready()
        query = Query()

        # New Constraint
        # fmt: off
        cnst: Constraint = self._construct(
            columns, index_type, comment, visible,  # Unique/Primary Key
            reference_table, reference_columns, on_delete, on_update,  # Foreign Key
            expression, enforced,  # Check
        )
        # fmt: on
        cnst.set_name(self._name)
        cnst.setup(self._tb_name, self._db_name, self._charset, None, self._pool)

        # Compare differences
        diff = cnst._diff_from_metadata(meta)
        # . no differences
        if diff == 0:
            return query
        # . drop & re-create
        else:
            query.set_sql(self, cnst._gen_drop_sql())
            query.set_sql(self, cnst._gen_add_sql())
        return query

    @cython.ccall
    def _gen_show_metadata_sql(self) -> str:
        """(internal) Generate SQL to show constraint metadata `<'str'>`."""
        self._assure_ready()
        return (
            "SELECT "
            # . columns[t1]
            "t1.CONSTRAINT_CATALOG AS CATALOG_NAME, "
            "t1.CONSTRAINT_SCHEMA AS SCHEMA_NAME, "
            "t1.TABLE_NAME AS TABLE_NAME, "
            "t1.CONSTRAINT_NAME AS CONSTRAINT_NAME, "
            "UPPER(t1.CONSTRAINT_TYPE) AS CONSTRAINT_TYPE, "
            "t1.ENFORCED AS ENFORCED, "
            # . columns[t2]
            "t2.ENGINE_ATTRIBUTE AS ENGINE_ATTRIBUTE, "
            "t2.SECONDARY_ENGINE_ATTRIBUTE AS SECONDARY_ENGINE_ATTRIBUTE, "
            # . columns[t3]
            "t3.REFERENCED_TABLE_NAME AS REFERENCED_TABLE_NAME, "
            "t3.UNIQUE_CONSTRAINT_NAME AS UNIQUE_CONSTRAINT_NAME, "
            "UPPER(t3.UPDATE_RULE) AS UPDATE_RULE, "
            "UPPER(t3.DELETE_RULE) AS DELETE_RULE, "
            "t3.MATCH_OPTION AS MATCH_OPTION, "
            # . columns[t4]
            "t4.ORDINAL_POSITION AS ORDINAL_POSITION, "
            "t4.POSITION_IN_UNIQUE_CONSTRAINT AS POSITION_IN_UNIQUE_CONSTRAINT, "
            "t4.COLUMN_NAME AS COLUMN_NAME, "
            "t4.REFERENCED_COLUMN_NAME AS REFERENCED_COLUMN_NAME "
            # . information_schema.table_constraints
            "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS t1 "
            # . information_schema.table_constraints_extensions
            "LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS_EXTENSIONS AS t2 "
            "ON t1.TABLE_NAME = t2.TABLE_NAME "
            "AND t1.CONSTRAINT_SCHEMA = t2.CONSTRAINT_SCHEMA "
            "AND t1.CONSTRAINT_NAME = t2.CONSTRAINT_NAME "
            # . information_schema.referential_constraints
            "JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS t3 "
            "ON t1.CONSTRAINT_NAME = t3.CONSTRAINT_NAME "
            "AND t1.CONSTRAINT_SCHEMA = t3.CONSTRAINT_SCHEMA "
            # . information_schema.key_column_usage
            "JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS t4 "
            "ON t1.CONSTRAINT_NAME = t4.CONSTRAINT_NAME "
            "AND t1.CONSTRAINT_SCHEMA = t4.CONSTRAINT_SCHEMA "
            # . conditions
            "WHERE t1.CONSTRAINT_NAME = '%s' "
            "AND t1.TABLE_NAME = '%s' "
            "AND t1.CONSTRAINT_SCHEMA = '%s' "
            "AND t1.CONSTRAINT_SCHEMA = t1.TABLE_SCHEMA "
            "ORDER BY t4.ORDINAL_POSITION ASC;"
            % (self._symbol, self._tb_name, self._db_name)
        )

    # Metadata -----------------------------------------------------------------------------
    @cython.ccall
    def _sync_from_metadata(self, meta: ConstraintMetadata, logs: Logs = None) -> Logs:
        """(internal) Synchronize local configs with the remote constraint metadata `<'Logs'>`."""
        # Base configs
        logs = Constraint._sync_from_metadata(self, meta, logs)
        if logs._skip_flag:
            return logs  # exit

        # Columns
        if self._columns != meta._columns:
            logs.log_config_obj(self, "columns", self._columns, meta._columns)
            self._columns = meta._columns

        # Reference Table
        if self._reference_table != meta._reference_table:
            logs.log_config_obj(
                self, "reference_table", self._reference_table, meta._reference_table
            )
            self._reference_table = meta._reference_table

        # Reference Columns
        if self._reference_columns != meta._reference_columns:
            logs.log_config_obj(
                self,
                "reference_columns",
                self._reference_columns,
                meta._reference_columns,
            )
            self._reference_columns = meta._reference_columns

        # On Delete
        if self._on_delete != meta._on_delete:
            logs.log_config_obj(self, "on_delete", self._on_delete, meta._on_delete)
            self._on_delete = meta._on_delete

        # On Update
        if self._on_update != meta._on_update:
            logs.log_config_obj(self, "on_update", self._on_update, meta._on_update)
            self._on_update = meta._on_update

        # Return logs
        return logs

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _diff_from_metadata(self, meta: ConstraintMetadata) -> cython.int:
        """(internal) Check if the constraint configurations are different
        from the remote server metadata `<'int'>`.

        :returns `<'int'>`:
        - `0`: Constraint configurations are identical.
        - `1`: Constraint configurations are different.
        """
        # Different
        diff = Constraint._diff_from_metadata(self, meta)
        if diff == 1:
            return 1
        if self._columns != meta._columns:
            return 1
        if self._reference_table != meta._reference_table:
            return 1
        if self._reference_columns != meta._reference_columns:
            return 1
        if self._on_delete != meta._on_delete:
            return 1
        if self._on_update != meta._on_update:
            return 1
        # Same
        return diff

    # Validator -----------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _validate_foreign_key_action(self, action: object) -> str:
        """(internal) Validate foreign key action `<'str/None'>`."""
        try:
            return utils.validate_foreign_key_action(action)
        except Exception as err:
            self._raise_definition_error(str(err), err)

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def _construct(
        self,
        columns: object | None,
        index_type: str | None,
        comment: str | None,
        visible: bool | None,
        reference_table: str | None,
        reference_columns: object | None,
        on_delete: str | None,
        on_update: str | None,
        expression: object | None,
        enforced: bool | None,
    ) -> Constraint:
        """(internal) Construct a new constraint instance `<'Constraint'>`.

        ## Notice
        - Value `None` means to use the corresponding settings of this constraint.

        :param columns `<'str/Column/tuple/list/None'>`: The columns of the constraint.
            Only applicable to <'UniqueKey'>, <'PrimaryKey'> and <'ForeignKey'>.
        :param index_type `<'str/None'>`: The index algorithm (`"BTREE"` or `"HASH"`).
            Only applicable to <'UniqueKey'> and <'PrimaryKey'>.
        :param comment `<'str/None'>`: The COMMENT of the constraint.
            Only applicable to <'UniqueKey'> and <'PrimaryKey'>.
        :param visible `<'bool'>`: The visibility of the index.
            Only applicable to <'UniqueKey'>.
        :param reference_table `<'str/None'>`: The parent table for FOREIGN KEY.
            Only applicable to <'ForeignKey'>.
        :param reference_columns `<'str/Column/tuple/list/None'>`: The columns in the referenced table.
            Only applicable to <'ForeignKey'>.
        :param on_delete `<'str/None'>`: The action to take when the referenced row is deleted.
            Accepts: `"CASCADE"`, `"SET NULL"`, `"RESTRICT"`, `"NO ACTION"`, `"SET DEFAULT"`.
            Only applicable to <'ForeignKey'>.
        :param on_update `<'str/None'>`: The action to take when the referenced row is updated.
            Accepts: `"CASCADE"`, `"SET NULL"`, `"RESTRICT"`, `"NO ACTION"`, `"SET DEFAULT"`.
            Only applicable to <'ForeignKey'>.
        :param expression `<'str/None>`: The expression for CHECK constraint.
            Only applicable to <'Check'>.
        :param enforce `<'bool/None'>`: The enforcement state for the CHECK constraint.
            Only applicable to <'Check'>.
        """
        return ForeignKey(
            # fmt: off
            self._columns if columns is None else self._validate_columns(columns),
            self._reference_table if reference_table is None else self._validate_table_name(reference_table),
            self._reference_columns if reference_columns is None else self._validate_columns(reference_columns),
            self._on_delete if on_delete is None else self._validate_foreign_key_action(on_delete),
            self._on_update if on_update is None else self._validate_foreign_key_action(on_update),
            # fmt: on
        )


@cython.cclass
class Check(Constraint):
    """Represents a CHECK constraint in a database table.

    ### Supported by MySQL 8.0.16+
    """

    def __init__(self, expression: object, enforced: cython.bint = True):
        """The CHECK constraint in a database table.

        ### Supports MySQL 8.0.16+

        :param expression `<'str/SQLFunction'>`: The expression for the CHECK constraint.
        :param enforce `<'bool'>`: The enforcement state for the CHECK constraint. Defaults to `True`.
        """
        super().__init__("CHECK")
        self._expression = self._validate_expression(expression)
        self._enforced = enforced

    # Property -----------------------------------------------------------------------------
    @property
    def expression(self) -> tuple[str]:
        """The expression for the CHECK constraint `<'str'>`."""
        return self._expression

    # Sync ---------------------------------------------------------------------------------
    def Alter(
        self,
        expression: object | None = None,
        enforced: bool | None = None,
    ) -> Logs:
        """[sync] Alter the CHECK constraint `<'Logs'>`.

        ## Notice
        - Default value `None` means to retain the corresponding settings of the check constraint.
        - Alteration of 'expression' leads to `DROP` & `RE-CREATE` of the check constraint.
        - Alteration of 'enforce' simply toggles the constraint enforcement state.

        :param expression `<'str/SQLFunction/None'>`: New expression for the CHECK constraint. Defaults to `None`.
        :param enforce `<'bool/None'>`: New enforcement state of the CHECK constraint. Defaults to `None`.
        """
        # fmt: off
        return self._Alter(
            None, None, None, None,
            None, None, None, None,
            expression, enforced,
        )
        # fmt: on

    @cython.ccall
    def SetEnforced(self, enforced: cython.bint) -> Logs:
        """[sync] Toggles the enforcement state of the CHECK constraint `<'Logs'>`.

        :param enforced `<'bool'>`: The enforcement state.
        """
        sql: str = self._gen_set_enforced_sql(enforced)
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
        logs = Logs().log_sql(self, sql)
        # Sync from remote
        return logs.extend(self.SyncFromRemote())

    @cython.ccall
    def ShowMetadata(self) -> ConstraintMetadata:
        """[sync] Show the CHECK constraint metadata from the remote server `<'ConstraintMetadata'>`.

        :raises `<'OperationalError'>`: If the constraint does not exist.
        """
        sql: str = self._gen_show_metadata_sql()
        with self._pool.acquire() as conn:
            conn.set_decode_json(False)
            with conn.transaction(DictCursor) as cur:
                cur.execute(sql)
                res = cur.fetchone()
        if res is None:
            self._raise_operational_error(
                3821, "(symbol '%s') does not exist" % self._symbol
            )
        return CheckMetadata(res)

    # Async ---------------------------------------------------------------------------------
    async def aioAlter(
        self,
        expression: object | None = None,
        enforced: bool | None = None,
    ) -> Logs:
        """[async] Alter the CHECK constraint `<'Logs'>`.

        ## Notice
        - Default value `None` means to retain the corresponding settings of the check constraint.
        - Alteration of 'expression' leads to `DROP` & `RE-CREATE` of the check constraint.
        - Alteration of 'enforce' simply toggles the constraint enforcement state.

        :param expression `<'str/SQLFunction/None'>`: New expression for the CHECK constraint. Defaults to `None`.
        :param enforce `<'bool/None'>`: New enforcement state of the CHECK constraint. Defaults to `None`.
        """
        # fmt: off
        return await self._aioAlter(
            None, None, None, None,
            None, None, None, None,
            expression, enforced,
        )
        # fmt: on

    async def aioSetEnforced(self, enforced: cython.bint) -> Logs:
        """[async] Toggles the enforcement state of the CHECK constraint `<'Logs'>`.

        :param enforced `<'bool'>`: The enforcement state.
        """
        sql: str = self._gen_set_enforced_sql(enforced)
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
        logs = Logs().log_sql(self, sql)
        # Sync from remote
        return logs.extend(await self.aioSyncFromRemote())

    async def aioShowMetadata(self) -> ConstraintMetadata:
        """[async] Show the CHECK constraint metadata from the remote server `<'ConstraintMetadata'>`.

        :raises `<'OperationalError'>`: If the constraint does not exist.
        """
        sql: str = self._gen_show_metadata_sql()
        async with self._pool.acquire() as conn:
            conn.set_decode_json(False)
            async with conn.transaction(AioDictCursor) as cur:
                await cur.execute(sql)
                res = await cur.fetchone()
        if res is None:
            self._raise_operational_error(
                3821, "(symbol '%s') does not exist" % self._symbol
            )
        return CheckMetadata(res)

    # Generate SQL -------------------------------------------------------------------------
    @cython.ccall
    def _gen_definition_sql(self) -> str:
        """(internal) Generate the definition SQL of the constraint `<'str'>`."""
        self._assure_ready()
        sql: str = "CONSTRAINT %s %s (%s)" % (
            self._symbol,
            self._el_type,
            self._expression,
        )
        if not self._enforced:
            sql += " NOT ENFORCED"
        return sql

    @cython.ccall
    def _gen_drop_sql(self) -> str:
        """(internal) Generate SQL to drop the constraint from the table `<'str'>`."""
        self._assure_ready()
        return "ALTER TABLE %s DROP CHECK %s;" % (
            self._tb_qualified_name,
            self._symbol,
        )

    @cython.ccall
    def _gen_alter_query(
        self,
        meta: ConstraintMetadata,
        columns: object | None,
        index_type: str | None,
        comment: str | None,
        visible: bool | None,
        reference_table: str | None,
        reference_columns: object | None,
        on_delete: str | None,
        on_update: str | None,
        expression: object | None,
        enforced: bool | None,
    ) -> Query:
        """(internal) Generate the query to alter the constraint `<'Query'>`.

        :param meta `<'IndexMetadata'>`: The remote server constraint metadata.
        :param columns `<'str/Column/tuple/list/None'>`: New columns of the constraint.
            Only applicable to <'UniqueKey'>, <'PrimaryKey'> and <'ForeignKey'>.
        :param index_type `<'str/None'>`: New index algorithm (`"BTREE"` or `"HASH"`).
            Only applicable to <'UniqueKey'> and <'PrimaryKey'>.
        :param comment `<'str/None'>`: New COMMENT of the constraint.
            Only applicable to <'UniqueKey'> and <'PrimaryKey'>.
        :param visible `<'bool'>`: The visibility of the index.
            Only applicable to <'UniqueKey'>.
        :param reference_table `<'str/None'>`: New parent table for FOREIGN KEY.
            Only applicable to <'ForeignKey'>.
        :param reference_columns `<'str/Column/tuple/list/None'>`: New columns in the referenced table.
            Only applicable to <'ForeignKey'>.
        :param on_delete `<'str/None'>`: The action to take when the referenced row is deleted.
            Accepts: `"CASCADE"`, `"SET NULL"`, `"RESTRICT"`, `"NO ACTION"`, `"SET DEFAULT"`.
            Only applicable to <'ForeignKey'>.
        :param on_update `<'str/None'>`: The action to take when the referenced row is updated.
            Accepts: `"CASCADE"`, `"SET NULL"`, `"RESTRICT"`, `"NO ACTION"`, `"SET DEFAULT"`.
            Only applicable to <'ForeignKey'>.
        :param expression `<'str/None>`: New expression for CHECK constraint.
            Only applicable to <'Check'>.
        :param enforce `<'bool/None'>`: The enforcement state for the CHECK constraint.
            Only applicable to <'Check'>.
        """
        self._assure_ready()
        query = Query()

        # New Constraint
        # fmt: off
        cnst: Constraint = self._construct(
            columns, index_type, comment, visible,  # Unique/Primary Key
            reference_table, reference_columns, on_delete, on_update,  # Foreign Key
            expression, enforced,  # Check
        )
        # fmt: on
        cnst.set_name(self._name)
        cnst.setup(self._tb_name, self._db_name, self._charset, None, self._pool)

        # Compare differences
        diff = cnst._diff_from_metadata(meta)
        # . no differences
        if diff == 0:
            return query
        # . drop & re-create
        if diff == 1:
            query.set_sql(self, cnst._gen_drop_sql())
            query.set_sql(self, cnst._gen_add_sql())
        # . toggle enforced
        else:
            query.set_sql(self, self._gen_set_enforced_sql(cnst._enforced))
        return query

    @cython.ccall
    def _gen_set_enforced_sql(self, enforced: cython.bint) -> str:
        """(internal) Generate SQL to set the enforcement state of the constraint `<'str'>`."""
        self._assure_ready()
        if enforced:
            return "ALTER TABLE %s ALTER CHECK %s ENFORCED;" % (
                self._tb_qualified_name,
                self._symbol,
            )
        else:
            return "ALTER TABLE %s ALTER CHECK %s NOT ENFORCED;" % (
                self._tb_qualified_name,
                self._symbol,
            )

    @cython.ccall
    def _gen_show_metadata_sql(self) -> str:
        """(internal) Generate SQL to show constraint metadata `<'str'>`."""
        self._assure_ready()
        return (
            "SELECT "
            # . columns [t1]
            "t1.CONSTRAINT_CATALOG AS CATALOG_NAME, "
            "t1.CONSTRAINT_SCHEMA AS SCHEMA_NAME, "
            "t1.TABLE_NAME AS TABLE_NAME, "
            "t1.CONSTRAINT_NAME AS CONSTRAINT_NAME, "
            "UPPER(t1.CONSTRAINT_TYPE) AS CONSTRAINT_TYPE, "
            "t1.ENFORCED AS ENFORCED, "
            # . columns [t2]
            "t2.ENGINE_ATTRIBUTE AS ENGINE_ATTRIBUTE, "
            "t2.SECONDARY_ENGINE_ATTRIBUTE AS SECONDARY_ENGINE_ATTRIBUTE, "
            # . columns [t3]
            "t3.CHECK_CLAUSE AS CHECK_CLAUSE "
            # . information_schema.table_constraints
            "FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS t1 "
            # . information_schema.table_constraints_extensions
            "LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS_EXTENSIONS AS t2 "
            "ON t1.TABLE_NAME = t2.TABLE_NAME "
            "AND t1.CONSTRAINT_SCHEMA = t2.CONSTRAINT_SCHEMA "
            "AND t1.CONSTRAINT_NAME = t2.CONSTRAINT_NAME "
            # . information_schema.check_constraints
            "JOIN INFORMATION_SCHEMA.CHECK_CONSTRAINTS AS t3 "
            "ON t1.CONSTRAINT_NAME = t3.CONSTRAINT_NAME "
            "AND t1.CONSTRAINT_SCHEMA = t3.CONSTRAINT_SCHEMA "
            # . conditions
            "WHERE t1.CONSTRAINT_NAME = '%s' "
            "AND t1.TABLE_NAME = '%s' "
            "AND t1.CONSTRAINT_SCHEMA = '%s' "
            "AND t1.CONSTRAINT_SCHEMA = t1.TABLE_SCHEMA;"
            % (self._symbol, self._tb_name, self._db_name)
        )

    # Metadata -----------------------------------------------------------------------------
    @cython.ccall
    def _sync_from_metadata(self, meta: ConstraintMetadata, logs: Logs = None) -> Logs:
        """(internal) Synchronize local configs with the remote constraint metadata `<'Logs'>`."""
        # Base configs
        logs = Constraint._sync_from_metadata(self, meta, logs)
        if logs._skip_flag:
            return logs  # exit

        # Expression
        if self._expression != meta._expression:
            logs.log_config_obj(self, "expression", self._expression, meta._expression)
            self._expression = meta._expression

        # Return logs
        return logs

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _diff_from_metadata(self, meta: ConstraintMetadata) -> cython.int:
        """(internal) Check if the constraint configurations are different
        from the remote server metadata `<'int'>`.

        :returns `<'int'>`:
        - `0`: Constraint configurations are identical.
        - `1`: Constraint configurations differ in more than enforcement state.
        - `2`: Only the enforcement state differs.
        """
        # Different
        diff = Constraint._diff_from_metadata(self, meta)
        if diff == 1:
            return 1
        if self._expression != meta._expression:
            return 1
        if self._enforced != meta._enforced:
            return 2
        # Same
        return diff

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def _construct(
        self,
        columns: object | None,
        index_type: str | None,
        comment: str | None,
        visible: bool | None,
        reference_table: str | None,
        reference_columns: object | None,
        on_delete: str | None,
        on_update: str | None,
        expression: object | None,
        enforced: bool | None,
    ) -> Constraint:
        """(internal) Construct a new constraint instance `<'Constraint'>`.

        ## Notice
        - Value `None` means to use the corresponding settings of this constraint.

        :param columns `<'str/Column/tuple/list/None'>`: The columns of the constraint.
            Only applicable to <'UniqueKey'>, <'PrimaryKey'> and <'ForeignKey'>.
        :param index_type `<'str/None'>`: The index algorithm (`"BTREE"` or `"HASH"`).
            Only applicable to <'UniqueKey'> and <'PrimaryKey'>.
        :param comment `<'str/None'>`: The COMMENT of the constraint.
            Only applicable to <'UniqueKey'> and <'PrimaryKey'>.
        :param visible `<'bool'>`: The visibility of the index.
            Only applicable to <'UniqueKey'>.
        :param reference_table `<'str/None'>`: The parent table for FOREIGN KEY.
            Only applicable to <'ForeignKey'>.
        :param reference_columns `<'str/Column/tuple/list/None'>`: The columns in the referenced table.
            Only applicable to <'ForeignKey'>.
        :param on_delete `<'str/None'>`: The action to take when the referenced row is deleted.
            Accepts: `"CASCADE"`, `"SET NULL"`, `"RESTRICT"`, `"NO ACTION"`, `"SET DEFAULT"`.
            Only applicable to <'ForeignKey'>.
        :param on_update `<'str/None'>`: The action to take when the referenced row is updated.
            Accepts: `"CASCADE"`, `"SET NULL"`, `"RESTRICT"`, `"NO ACTION"`, `"SET DEFAULT"`.
            Only applicable to <'ForeignKey'>.
        :param expression `<'str/None>`: The expression for CHECK constraint.
            Only applicable to <'Check'>.
        :param enforce `<'bool/None'>`: The enforcement state for the CHECK constraint.
            Only applicable to <'Check'>.
        """
        return Check(
            # fmt: off
            self._expression if expression is None else self._validate_expression(expression),
            enforced=self._enforced if enforced is None else bool(enforced),
            # fmt: on
        )


# Constraints ------------------------------------------------------------------------------------------------
@cython.cclass
class Constraints(Elements):
    """Represents a collection of constraints in a table.

    Works as a dictionary where keys are the constraint names
    and values the constraint instances.
    """

    def __init__(self, *constraints: Constraint):
        """The collection of constraints in a table.

        Works as a dictionary where keys are the constraint names
        and values the constraint instances.

        :param constraints `<'*Constraint'>`: The constraints in a table.
        """
        super().__init__("CONSTRAINT", "CONSTRAINTS", Constraint, *constraints)

    # Property -----------------------------------------------------------------------------
    @property
    def symbols(self) -> tuple[str]:
        """The symbols of the constraints in the collection `<'tuple[str]'>`.

        `Symbol` is the actual identifier used in the
        database to refer to the constraint.
        """
        cnst: Constraint
        return tuple([cnst._symbol for cnst in self._sorted_elements()])

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
        cnst: Constraint
        sqls = [cnst._gen_definition_sql() for cnst in self._sorted_elements()]
        # . without indent
        nxt: str = ",\n"
        if indent == 0:
            return nxt.join(sqls)
        # With indent
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
        """Setup the constraint collection.

        :param tb_name `<'str'>`: The table name of the constraint collection.
        :param db_name `<'str'>`: The database name of the constraint collection.
        :param charset `<'str/Charset'>`: The charset of the constraint collection.
        :param collate `<'str/None'>`: The collation of the constraint collection.
        :param pool `<'Pool'>`: The pool of the constraint collection.
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
        el: Constraint
        for el in self._el_dict.values():
            if not el._el_ready:
                el.setup(tb_name, db_name, charset, None, pool)
        return self._assure_ready()

    # Assure Ready -------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _assure_ready(self) -> cython.bint:
        """(internal) Assure the constraint collection is ready."""
        if not self._el_ready:
            self._assure_tb_name_ready()
            Elements._assure_ready(self)
        return True

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def copy(self) -> Constraints:
        """Make a copy of the constraint collection `<'Constraints'>`."""
        el: Constraint
        return Constraints(*[el.copy() for el in self._el_dict.values()])


# Metadata ---------------------------------------------------------------------------------------------------
@cython.cclass
class ConstraintMetadata(Metadata):
    """Represents the metadata from the remote server of a constraint."""

    # Base data
    _db_name: str
    _tb_name: str
    _constraint_name: str
    _constraint_type: str
    _enforced: cython.bint
    # Additional data
    _el_type: str
    # Primary/Unique Key
    _index_type: str
    _unique: cython.bint
    _comment: str
    _visible: cython.bint
    _columns: tuple[str]
    # Foreign Key
    _reference_table: str
    _unique_constraint: str
    _on_delete: str
    _on_update: str
    _reference_columns: tuple[str]
    # Check
    _expression: str

    def __init__(self, meta: dict, size: int):
        """The metadata from the remote server of a constraint.

        :param meta `<'dict'>`: A dictionary constains as least the following constraint metadata:
        ```python
        - "CATALOG_NAME"
        - "SCHEMA_NAME"
        - "TABLE_NAME"
        - "CONSTRAINT_NAME"
        - "CONSTRAINT_TYPE"
        - "ENFORCED"
        - "ENGINE_ATTRIBUTE"
        - "SECONDARY_ENGINE_ATTRIBUTE"
        ```
        :param size `<'int'>`: The expected size of the metadata dictionary.
        """
        super().__init__("CONSTRAINT", meta, size)
        try:
            # Base data
            self._db_name = meta["SCHEMA_NAME"]
            self._tb_name = meta["TABLE_NAME"]
            self._constraint_name = meta["CONSTRAINT_NAME"]
            self._constraint_type = meta["CONSTRAINT_TYPE"]
            self._enforced = utils.validate_enforced(meta["ENFORCED"])
            # Additional data
            _type: str = self._constraint_type
            if _type == "UNIQUE":
                self._el_type = "UNIQUE KEY"
            elif _type in ("PRIMARY KEY", "FOREIGN KEY", "CHECK"):
                self._el_type = _type
            else:
                raise ValueError("Invalid constraint type: '%s'." % _type)
        except Exception as err:
            self._raise_invalid_metadata_error(self._meta, err)
        # . Primary/Unique Key
        self._index_type = None
        self._unique = False
        self._comment = None
        self._visible = False
        self._columns = None
        # . Foreign Key
        self._reference_table = None
        self._reference_columns = None
        self._unique_constraint = None
        self._on_delete = None
        self._on_update = None
        # . Check
        self._expression = None

    # Property -----------------------------------------------------------------------------
    @property
    def catelog_name(self) -> str:
        """The catalog name of the constraint `<'str'>`."""
        return self._meta["CATALOG_NAME"]

    @property
    def db_name(self) -> str:
        """The schema name of the constraint `<'str'>`."""
        return self._db_name

    @property
    def tb_name(self) -> str:
        """The table name of the constraint `<'str'>`."""
        return self._tb_name

    @property
    def constraint_name(self) -> str:
        """The name (symbol) of the constraint `<'str'>`."""
        return self._constraint_name

    @property
    def constraint_type(self) -> str:
        """The type of the constraint `<'str'>`."""
        return self._constraint_type

    @property
    def enforced(self) -> bool:
        """Whether the constraint is enforced `<'bool'>`."""
        return self._enforced

    @property
    def engine_attribute(self) -> str | None:
        """The JSON field introduced in later MySQL versions that can
        store custom engine-specific attributes `<'str/None'>`.
        """
        return self._meta["ENGINE_ATTRIBUTE"]

    @property
    def secondary_engine_attribute(self) -> str | None:
        """The secondary JSON field introduced in later MySQL versions
        that can store custom engine-specific attributes `<'str/None'>`.
        """
        return self._meta["SECONDARY_ENGINE_ATTRIBUTE"]

    # . Primary/Unique Key
    @property
    def index_type(self) -> str | None:
        """The algorithm of the index `<'str/None'>`.

        e.g.: `"BTREE"`, `"HASH"`

        ## Notice
        - Only applicable to `<'UniqueKey'>` and `<'PrimaryKey'>`.
        - For other constraint types, always will returns `None`.
        """
        return self._index_type

    @property
    def unique(self) -> bool:
        """Whether the constraint index is unique or not `<'bool'>`.

        ## Notice
        - Only applicable to `<'UniqueKey'>` and `<'PrimaryKey'>`.
        - For other constraint types, always will returns `False`.
        """
        return self._unique

    @property
    def comment(self) -> str | None:
        """The comment of the constraint `<'str/None'>`.

        ## Notice
        - Only applicable to `<'UniqueKey'>` and `<'PrimaryKey'>`.
        - For other constraint types, always will returns `None`.
        """
        return self._comment

    @property
    def visible(self) -> bool:
        """Whether the constraint index is visible to the optimizer `<'bool'>`.

        ## Notice
        - Only applicable to `<'UniqueKey'>` and `<'PrimaryKey'>`.
        - For other constraint types, always will returns `False`.
        """
        return self._visible

    @property
    def columns(self) -> tuple[str] | None:
        """The columns of the constraint `<'tuple[str]/None'>`.

        - For `<'UniqueKey'>` and `<'PrimaryKey'>`, it represents the columns of the constraint index.
        - For `<'ForeignKey'>`, it represents the columns for child table of the foreign key.

        ## Notice
        - Only applicable to `<'UniqueKey'>`, `<'PrimaryKey'>` and <'ForeignKey'>.
        - For other constraint types, always will returns `None`.
        """
        return self._columns

    # . Foreign Key
    @property
    def reference_table(self) -> str | None:
        """The name of the referenced table `<'str/None'>`.

        ## Notice
        - Only applicable to `<'ForeignKey'>`.
        - For other constraint types, always will returns `None`.
        """
        return self._reference_table

    @property
    def reference_columns(self) -> tuple[str] | None:
        """The foreign key columns in the referenced table `<'tuple[str]/None'>`.

        ## Notice
        - Only applicable to `<'ForeignKey'>`.
        - For other constraint types, always will returns `None`.
        """
        return self._reference_columns

    @property
    def unique_constraint(self) -> str | None:
        """The name of the unique constraint in the referenced table `<'str/None'>`.

        ## Notice
        - Only applicable to `<'ForeignKey'>`.
        - For other constraint types, always will returns `None`.
        """
        return self._unique_constraint

    @property
    def on_delete(self) -> str | None:
        """The action to perform when the referenced row is deleted `<'str/None'>`.

        ## Notice
        - Only applicable to `<'ForeignKey'>`.
        - For other constraint types, always will returns `None`.
        """
        return self._on_delete

    @property
    def on_update(self) -> str | None:
        """The action to perform when the referenced row is updated `<'str/None'>`.

        ## Notice
        - Only applicable to `<'ForeignKey'>`.
        - For other constraint types, always will returns `None`.
        """
        return self._on_update

    # . Check
    @property
    def expression(self) -> str | None:
        """The expression of the check constraint `<'str/None'>`.

        ## Notice
        - Only applicable to `<'Check'>`.
        - For other constraint types, always will returns `None`.
        """
        return self._expression


@cython.cclass
class UniPriKeyMetadata(ConstraintMetadata):
    """Represents the metadata from the remote server of a UNIQUE KEY or PRIMARY KEY constraint."""

    def __init__(self, meta: tuple[dict]):
        """The metadata from the remote server of a UNIQUE KEY or PRIMARY KEY constraint.

        :param meta `<'tuple[dict]'>`: A tuple of dictionaries, each containing the following constraint metadata:
        ```python
        - "CATALOG_NAME"
        - "SCHEMA_NAME"
        - "TABLE_NAME"
        - "CONSTRAINT_NAME"
        - "CONSTRAINT_TYPE"
        - "ENFORCED"
        - "ENGINE_ATTRIBUTE"
        - "SECONDARY_ENGINE_ATTRIBUTE"
        - "INDEX_TYPE"
        - "NON_UNIQUE"
        - "COMMENT"
        - "INDEX_COMMENT"
        - "IS_VISIBLE"
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
        self._el_cate = "CONSTRAINT"
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
                        "CONSTRAINT_NAME": row["CONSTRAINT_NAME"],
                        "CONSTRAINT_TYPE": row["CONSTRAINT_TYPE"],
                        "ENFORCED": row["ENFORCED"],
                        "ENGINE_ATTRIBUTE": row["ENGINE_ATTRIBUTE"],
                        "SECONDARY_ENGINE_ATTRIBUTE": row["SECONDARY_ENGINE_ATTRIBUTE"],
                        "INDEX_TYPE": row["INDEX_TYPE"],
                        "NON_UNIQUE": row["NON_UNIQUE"],
                        "COMMENT": row["COMMENT"],
                        "INDEX_COMMENT": row["INDEX_COMMENT"],
                        "IS_VISIBLE": row["IS_VISIBLE"],
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
                raise ValueError("Constraint metadata is empty.")
        except Exception as err:
            self._raise_invalid_metadata_error(meta, err)
        dict_setitem(_meta, "COLUMNS", tuple(_cols))

        # Initialize
        super().__init__(_meta, 14)
        try:
            self._index_type = utils.validate_index_type(self._meta["INDEX_TYPE"])
            self._unique = self._meta["NON_UNIQUE"] == 0
            self._comment = utils.validate_comment(self._meta["INDEX_COMMENT"])
            self._visible = utils.validate_visible(self._meta["IS_VISIBLE"])
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


@cython.cclass
class ForeignKeyMetadata(ConstraintMetadata):
    """Represents the metadata from the remote server of a FOREIGN KEY constraint."""

    def __init__(self, meta: tuple[dict]):
        """The metadata from the remote server of a FOREIGN KEY constraint.

        :param meta `<'tuple[dict]'>`: A tuple of dictionaries, each containing the following index metadata:
        ```python
        - "CATALOG_NAME"
        - "SCHEMA_NAME"
        - "TABLE_NAME"
        - "CONSTRAINT_NAME"
        - "CONSTRAINT_TYPE"
        - "ENFORCED"
        - "ENGINE_ATTRIBUTE"
        - "SECONDARY_ENGINE_ATTRIBUTE"
        - "REFERENCED_TABLE_NAME"
        - "UNIQUE_CONSTRAINT_NAME"
        - "UPDATE_RULE"
        - "DELETE_RULE"
        - "MATCH_OPTION"
        - "ORDINAL_POSITION"
        - "POSITION_IN_UNIQUE_CONSTRAINT"
        - "COLUMN_NAME"
        - "REFERENCED_COLUMN_NAME"
        ```
        """
        # Re-construct
        self._el_cate = "CONSTRAINT"
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
                        "CONSTRAINT_NAME": row["CONSTRAINT_NAME"],
                        "CONSTRAINT_TYPE": row["CONSTRAINT_TYPE"],
                        "ENFORCED": row["ENFORCED"],
                        "ENGINE_ATTRIBUTE": row["ENGINE_ATTRIBUTE"],
                        "SECONDARY_ENGINE_ATTRIBUTE": row["SECONDARY_ENGINE_ATTRIBUTE"],
                        "REFERENCED_TABLE_NAME": row["REFERENCED_TABLE_NAME"],
                        "UNIQUE_CONSTRAINT_NAME": row["UNIQUE_CONSTRAINT_NAME"],
                        "UPDATE_RULE": row["UPDATE_RULE"],
                        "DELETE_RULE": row["DELETE_RULE"],
                        "MATCH_OPTION": row["MATCH_OPTION"],
                    }
                _cols.append(
                    {
                        "ORDINAL_POSITION": row["ORDINAL_POSITION"],
                        "POSITION_IN_UNIQUE_CONSTRAINT": row[
                            "POSITION_IN_UNIQUE_CONSTRAINT"
                        ],
                        "COLUMN_NAME": row["COLUMN_NAME"],
                        "REFERENCED_COLUMN_NAME": row["REFERENCED_COLUMN_NAME"],
                    }
                )
            if _meta is None:
                raise ValueError("Constraint metadata is empty.")
        except Exception as err:
            self._raise_invalid_metadata_error(meta, err)
        dict_setitem(_meta, "COLUMNS", tuple(_cols))

        # Initialize
        super().__init__(_meta, 14)
        try:
            self._reference_table = self._meta["REFERENCED_TABLE_NAME"]
            self._unique_constraint = self._meta["UNIQUE_CONSTRAINT_NAME"]
            self._on_delete = self._meta["DELETE_RULE"]
            self._on_update = self._meta["UPDATE_RULE"]
            _columns, _ref_columns = [], []
            col: dict
            for col in _cols:
                _columns.append(col["COLUMN_NAME"])
                _ref_columns.append(col["REFERENCED_COLUMN_NAME"])
            self._columns = tuple(_columns)
            self._reference_columns = tuple(_ref_columns)
        except Exception as err:
            self._raise_invalid_metadata_error(self._meta, err)


@cython.cclass
class CheckMetadata(ConstraintMetadata):
    """Represents the metadata from the remote server of CHECK constraint."""

    def __init__(self, meta: dict):
        """The metadata from the remote server of CHECK constraint

        :param meta `<'dict'>`: A dictionary constains the following constraint metadata:
        ```python
        - "CATALOG_NAME"
        - "SCHEMA_NAME"
        - "TABLE_NAME"
        - "CONSTRAINT_NAME"
        - "CONSTRAINT_TYPE"
        - "ENFORCED"
        - "ENGINE_ATTRIBUTE"
        - "SECONDARY_ENGINE_ATTRIBUTE"
        - "CHECK_CLAUSE"
        ```
        """
        super().__init__(meta, 9)
        try:
            self._expression = utils.validate_expression(meta["CHECK_CLAUSE"])
        except Exception as err:
            self._raise_invalid_metadata_error(meta, err)
