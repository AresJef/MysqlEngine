# cython: language_level=3
# cython: wraparound=False
# cython: boundscheck=False
from __future__ import annotations

# Cython imports
import cython
from cython.cimports import numpy as np  # type: ignore
from cython.cimports.cpython import datetime  # type: ignore
from cython.cimports.cpython.datetime import datetime_new as gen_dt  # type: ignore
from cython.cimports.cpython.set import PySet_Add as set_add  # type: ignore
from cython.cimports.cpython.set import PySet_Discard as set_discard  # type: ignore
from cython.cimports.cpython.set import PySet_Contains as set_contains  # type: ignore
from cython.cimports.cpython.list import PyList_Check as is_list  # type: ignore
from cython.cimports.cpython.list import PyList_Append as list_append  # type: ignore
from cython.cimports.cpython.dict import PyDict_Check as is_dict  # type: ignore
from cython.cimports.cpython.dict import PyDict_GetItem as dict_get  # type: ignore
from cython.cimports.cpython.dict import PyDict_Items as dict_items  # type: ignore
from cython.cimports.cpython.dict import PyDict_SetItem as dict_setitem  # type: ignore
from cython.cimports.cpython.dict import PyDict_Contains as dict_contains  # type: ignore
from cython.cimports.cpython.tuple import PyTuple_Check as is_tuple  # type: ignore
from cython.cimports.cpython.string import PyString_Check as is_str  # type: ignore
from cython.cimports.cpython.unicode import PyUnicode_GET_LENGTH as str_len  # type: ignore
from cython.cimports.cpython.unicode import PyUnicode_Contains as str_contains  # type: ignore
from cython.cimports.cytimes.pydt import pydt  # type: ignore
from cython.cimports.cytimes.pddt import pddt  # type: ignore
from cython.cimports.cytimes import cydatetime as cydt  # type: ignore
from cython.cimports.mysqlengine import errors, settings, transcode, utils  # type: ignore
from cython.cimports.mysqlengine.regex import Regex, TableRegex  # type: ignore
from cython.cimports.mysqlengine.index import TableIndexes, Index  # type: ignore
from cython.cimports.mysqlengine.index import get_index_name, get_indexes_names  # type: ignore
from cython.cimports.mysqlengine.column import TableColumns, Column  # type: ignore
from cython.cimports.mysqlengine.column import get_column_name, get_columns_names  # type: ignore
from cython.cimports.mysqlengine.connection import Server, Connection  # type: ignore
from cython.cimports.mysqlengine.charset import Charset, charset_by_collate, charset_by_name_and_collate  # type: ignore

np.import_array()
datetime.import_datetime()

# Python imports
import os, datetime
from uuid import uuid4
from asyncio import gather, wait_for
from typing import Any, Union, Literal, Iterator, overload
from cytimes.pydt import pydt
from cytimes.pddt import pddt
from cytimes import cydatetime as cydt
from pandas import DataFrame, Series, read_parquet
from mysqlengine.logs import logger
from mysqlengine import errors, settings, transcode, utils
from mysqlengine.regex import Regex, TableRegex
from mysqlengine.index import TableIndexes, Index
from mysqlengine.column import TableColumns, Column
from mysqlengine.connection import Server, Connection
from mysqlengine.connection import Cursor, DictCursor, DfCursor
from mysqlengine.connection import SSCursor, SSDictCursor, SSDfCursor
from mysqlengine.errors import query_exc_handler
from mysqlengine.query import SelectQuery, InsertQuery, ReplaceQuery
from mysqlengine.query import UpdateQuery, DeleteQuery, CreateTempQuery, CompareQuery
from mysqlengine.charset import Charset, charset_by_collate, charset_by_name_and_collate
from mysqlengine.connection import PoolConnectionManager, PoolTransactionManager


__all__ = ["Table", "TimeTable", "Database"]


# Table =========================================================================================
@cython.cclass
class Table:
    """The base class for a Table in the Database.

    Inherit this class to create a custom Table for the Database. All
    arguments besides 'database' of the  `__init__` method should be
    pre-defined in the subclass.

    Configure table's metadata by overwriting the `metadata()` method:
    - Use `self.columns_metadata()` method to add columns to the table.
    - Use `self.indexes_metadata()` method to add indexes to the table.
    """

    # Settings
    _db: Database
    _db_pfix: str
    _server: Server
    _name: str
    _name_pfix: str
    _fname: str
    _fname_pfix: str
    _charset: str
    _collate: str
    _engine: str
    _temp_id: cython.int
    # Type
    _type: str
    _is_timetable: cython.bint
    # Regex
    _regex: TableRegex
    # Columns & Indexes
    _columns: TableColumns
    _indexes: TableIndexes
    # Syntax
    _syntax_val: str
    _syntax: str
    # Status
    _initiated: cython.bint
    _initiated_tables: set[str]

    def __init__(
        self,
        database: Database,
        name: str,
        charset: Union[str, None] = None,
        collate: Union[str, None] = None,
        engine: Literal["InnoDB", "MyISAM"] = "InnoDB",
    ) -> None:
        """The base class for a Table in the Database.

        Inherit this class to create a custom Table for the Database. All
        arguments besides 'database' of the  `__init__` method should be
        pre-defined in the subclass.

        :param database: `<Database>` The Database that hosts the table.
        :param name: `<str>` The name of the table (auto convert to lower case).
        :param charset: `<str>` The charset of the table. Defaults to `None` (Database.charset).
        :param collate: `<str>` The collate of the table. Defaults to `None` (Database.collate).
        :param engine: `<str>` The engine of the table. Defaults to `'InnoDB'`.

        ### Configuration:
        - Overwrite `Table.metadata()` to define table's columns and indexes.
        - Use `self.columns_metadata()` to define columns of the table.
        - Use `self.indexes_metadata()` to define indexes of the table.

        ### Example:
        >>> class MyTable(Table):
                def __init__(self, database: MyDatabase) -> None:
                    # . pre-define table name
                    # . leave charset, collate & engine to default.
                    super().__init__(database, "my_table")

        >>>     def metadata(self) -> None:
                    # . define columns
                    self.columns_metadata(
                        Column("id", MysqlTypes.BIGINT(primary_key=True)),
                        Column("username", MysqlTypes.VARCHAR()),
                        Column("user_level", MysqlTypes.TINYINT()),
                        Column("user_type", MysqlTypes.VARCHAR()),
                        ...
                    )
                    # . define indexes
                    self.indexes_metadata(
                        Index(self.columns["username"], unique=True, primary_unique=True),
                        Index(self.columns["user_level"], self.columns["user_type"]),
                        ...
                    )
        """
        # Settings
        self._db = database
        self._server = database._server
        self._name = name
        self._temp_id = 0
        self._charset = charset if charset else self._db._charset
        self._collate = collate if collate else self._db._collate
        self._engine = engine
        # Columns & Indexes
        self._columns = None
        self._indexes = None
        # Status
        self._initiated = False
        self._initiated_tables = set()
        # Setup
        self.__setup()

    # Properties ----------------------------------------------------------
    @property
    def db(self) -> Database:
        "The database hosting the table `<Database>`."
        return self._db

    @property
    def server(self) -> Server:
        "The MySQL Server of hosting the database `<Server>`."
        return self._server

    @property
    def name(self) -> str:
        "The name of the table `<str>`. e.g. `'users'`"
        return self._name

    @property
    def fname(self) -> str:
        "The full name of the table `<str>`. e.g. `'mydatabase.users'`."
        return self._fname

    @property
    def tname(self) -> str:
        """The `temporary` name for the table `<str>`. e.g. `'mydatabase.users_tmp0'`

        Primarily use for temporary table creation, temporary name is formated as:
        `'<database>.<table_name>_<unique_tmp_id>'`. A new (different) temporary
        name will be generated each time when this property is accessed.
        """
        return self._gen_tname()

    @property
    def pname(self) -> str:
        """The `placeholder` name for the table `<str>`.
        e.g.`'$_user_0e76621a-2eb5-451e-85c0-5b1e1a5c37e9_$'`

        Primarily use by built-in query methods, placeholder name is formated
        as: `'$_<table_name>_<unique_uuid>_$'`. A new (different) placeholder
        name will be generated each time when this property is accessed.
        """
        return self._gen_pname()

    @property
    def charset(self) -> str:
        "The MySQL `CHARACTER SET` of the table `<str>`."
        return self._charset

    @property
    def collate(self) -> str:
        "The MySQL `COLLATION` of the table `<str>`."
        return self._collate

    @property
    def engine(self) -> str:
        """The MySQL `ENGINE` of the table `<str>`.

        Possible values: `InnoDB` / `MyISAM`.
        """
        return self._engine

    @property
    def type(self) -> str:
        """The type of the table in `<str>`.

        Possible values: `'Table'` or `'TimeTable'`.
        """
        return self._type

    @property
    def is_timetable(self) -> bool:
        "Whether the table is TimeTable `<bool>`."
        return self._is_timetable

    @property
    def regex(self) -> TableRegex:
        "Regular expressions for the Table `<TableRegex>`."
        return self._regex

    @property
    def primary_key(self) -> Column:
        "The `PRIMARY KEY` of the table `<Column>`."
        return self._columns._primary_key

    @property
    def tabletime(self) -> Column:
        """The table column that determines TimeTable's
        sub-tables arrangements `<Column>`.

        - Only applicable for `<TimeTable>`.
        - Standard `<Table>` returns `<DummyColumn>`.
        """
        return self._columns._tabletime

    @property
    def columns(self) -> TableColumns:
        """The collection of all the table columns `<TableColumns>`.

        Works as an immutable dictionary with static typings, where
        keys are the column names and values are the `Column` instances.
        """
        return self._columns

    @property
    def indexes(self) -> TableIndexes:
        """The collection of all the table indexes `<TableIndexes>`.

        Works as an immutable dictionary with static typings, where
        keys are the index names and values are the `Index` instances.
        """
        return self._indexes

    @property
    def syntax(self) -> str:
        "The `SQL` syntax of the table `<str>`."
        return self._syntax

    @property
    def initiated(self) -> bool:
        "Whether the table is initiated `<bool>`."
        return self._initiated

    @property
    def initiated_tables(self) -> set[str]:
        """The names of all initiated tables `<str[str]>`.

        - Standard `<Table>` returns the set of it's own name.
        - `<TimeTable>` returns the names of all sub-tables that
          have been initiated.
        """
        return self._initiated_tables

    # Setup ---------------------------------------------------------------
    def __setup(self) -> None:
        self.__validation()
        self.metadata()
        self.__construct_metadata()

    def __validation(self) -> None:
        # Validate table name
        _name: str = utils._str_clean_name(self._name.lower())
        if not _name:
            raise errors.TableMetadataError(
                "<{}.metadata> Table name is invalid: "
                "'{}'".format(self.__class__.__name__, self._name)
            )
        if _name in settings.PROHIBIT_NAMES:
            raise errors.TableMetadataError(
                "<{}.metadata> Table name '{}' is prohibited, "
                "please choose another one.".format(self.__class__.__name__, self._name)
            )
        if len(_name) > settings.MAX_NAME_LENGTH_TABLE:
            raise errors.TableMetadataError(
                "<{}.metadata> Table name '{}' is too long, "
                "must be <= {} characters.".format(
                    self.__class__.__name__, _name, settings.MAX_NAME_LENGTH_TABLE
                )
            )
        self._name = _name
        self._name_pfix = _name + "_"

        # Validate database
        if not isinstance(self._db, Database):
            raise errors.TableMetadataError(
                "<{}.metadata> Table only accept instance of "
                "<'mysql_engine.Database'> as the database, instead of: "
                "'{}' {}".format(self._name, self._db, type(self._db))
            )
        self._fname = "%s.%s" % (self._db._name, _name)
        self._db_pfix = self._db._name_pfix
        self._fname_pfix = self._db_pfix + self._name_pfix

        # Validate charset & collate
        try:
            charSet = self._validate_charset_collate(self._charset, self._collate)
        except ValueError as err:
            raise errors.TableMetadataError(
                "<{}.metadata> {}".format(self._fname, err)
            ) from err
        self._charset = charSet._name
        self._collate = charSet._collate

        # Validate engine
        if self._engine not in settings.SUPPORT_ENGINES:
            raise errors.TableMetadataError(
                "<{}.metadata> Table engine '{}' is not "
                "supported.".format(self._fname, self._engine)
            )

        # Is TimeTable
        if isinstance(self, TimeTable):
            # . type
            self._type = "TimeTable"
            self._is_timetable = True
            # . setup regex in subclass
        # Is normal Table
        else:
            # . type
            self._type = "Table"
            self._is_timetable = False
            # . setup regex
            self._regex = TableRegex(self._db._name, self._name)

    def metadata(self) -> None:
        """Define the table metadata. This method should be overridden
        in subclass to configure the table's columns and indexes.

        ### Configuration:
        - Use `self.columns_metadata()` to define columns of the table.
        - Use `self.indexes_metadata()` to define indexes of the table.

        ### Example:
        >>> def metadata(self) -> None:
                # . define columns
                self.columns_metadata(
                    Column("id", MysqlTypes.BIGINT(primary_key=True)),
                    Column("username", MysqlTypes.VARCHAR()),
                    Column("user_level", MysqlTypes.TINYINT()),
                    Column("user_type", MysqlTypes.VARCHAR()),
                    ...
                )
                # . define indexes
                self.indexes_metadata(
                    Index(self.columns["username"], unique=True, primary_unique=True),
                    Index(self.columns["user_level"], self.columns["user_type"]),
                    ...
                )
        """
        raise NotImplementedError(
            "<{}.metadata> Table metadata is not configured. "
            "Please overwrite `Table.metadata()` method to "
            "define table's columns and indexes.".format(self._fname)
        )

    def columns_metadata(self, *columns: Column) -> None:
        """Define columns of the table. This method should be called
        within the `metadata()` method to set the desired columns.

        :param columns: `<Column>` The columns to add to the table.

        ### Example:
        >>> self.columns_metadata(
                Column("id", MysqlTypes.BIGINT(primary_key=True)),
                Column("username", MysqlTypes.VARCHAR()),
                Column("user_level", MysqlTypes.TINYINT()),
                Column("user_type", MysqlTypes.VARCHAR()),
                ...
            )
        """
        # Setup columns
        try:
            self._columns = TableColumns(*columns)
        except Exception as err:
            err.add_note("-> <%s> Column metadata error." % self._fname)
            raise err

        # Validate tabletime
        if self._is_timetable and not self._columns._tabletime:
            raise errors.TableMetadataError(
                "<{}.metadata> TimeTable '{}' must have one column's 'dtype' "
                "(DATE/DATETIME/TIMESTAMP) configured with `tabletime=True`. "
                "This settings is not an actual MySQL settings, but affects the "
                "built-in query methods withint `TimeTable` class. For more "
                "information, please refer to `TimeTable` documentation.".format(
                    self.__class__.__name__, self._fname
                )
            )

    def indexes_metadata(self, *indexes: Index) -> None:
        """Define indexes of the table. This method should be called
        within the `metadata()` method to set the desired indexes.

        :param indexes: `<Index>` The indexes to add to the table.

        ### Example:
        >>> self.indexes_metadata(
                Index(self.columns["tinyint_type"], unique=True, primary_unique=True),
                Index(self.columns["smallint_type"], self.columns["mediumint_type"]),
                ...
            )
        """
        try:
            self._indexes = TableIndexes(*indexes)
        except Exception as err:
            err.add_note("-> <%s> Index metadata error." % self._fname)
            raise err

    def __construct_metadata(self) -> None:
        # Validate columns
        if not isinstance(self._columns, TableColumns):
            raise errors.TableMetadataError(
                "<{}.metadata> Table columns are not configured. "
                "Please refer to `Table.metadata()` method for more "
                "details.".format(self._fname)
            )

        # Validate indexes
        if not isinstance(self._indexes, TableIndexes):
            self._indexes = TableIndexes()

        # Construct syntax
        self._syntax_val = self.gen_syntax()
        self._syntax = self._fname + " " + self._syntax_val

    # Syntax --------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _gen_syntax(self, columns: tuple, engine: Union[str, None]) -> str:
        """(cfunc) Generate the SQL TABLE syntax for the specified columns and engine.

        This method will create the (partial) SQL TABLE syntax based on the
        provided columns and engine. If no columns are given, all columns and
        indexes of the table will be included in the syntax. The engine parameter
        is optional and defaults to the set engine of the table.

        :param columns: `<tuple[str/Column]>` The columns for the syntax.
            - Only provided columns will be included, along with indexes
              that are satisfied by the given columns.
            - If not provided, all columns and indexes of the table will
              be included.

        :param engine: `<str>` The database engine to use.
        :return: `<str>` The (partial) SQL TABLE syntax.
        """
        # Validate engine
        if engine is None:
            engine = self._engine
        elif not set_contains(settings.TEMPORARY_ENGINES, engine):
            raise errors.QueryValueError(
                "<{}> Engine '{}' is not supported.".format(self._fname, engine)
            )

        # Generate column syntax
        columns_syntax: str = self._columns._gen_syntax(columns)

        # Generate index syntax
        indexes_syntax: str = self._indexes._gen_syntax(columns)

        # Generate syntax
        return "(\n%s\n) CHARACTER SET %s COLLATE %s ENGINE = %s" % (
            ",\n".join([i for i in (columns_syntax, indexes_syntax) if i]),
            self._charset,
            self._collate,
            engine,
        )

    def gen_syntax(
        self,
        *columns: Union[str, Column],
        engine: Union[Literal["MEMORY", "InnoDB", "MyISAM"], None] = None,
    ) -> str:
        """Generate the SQL TABLE syntax for the specified columns and engine.

        This method will create the (partial) SQL TABLE syntax based on the
        provided columns and engine. If no columns are given, all columns and
        indexes of the table will be included in the syntax. The engine parameter
        is optional and defaults to the set engine of the table.

        :param columns: `<str/Column>` The columns for the syntax.
            - Only provided columns will be included, along with indexes
              that are satisfied by the given columns.
            - If not provided, all columns and indexes of the table will
              be included.

        :param engine: `<str>` The database engine to use. Defaults to `None` (Table.engine).
        :return: `<str>` The (partial) SQL TABLE syntax.
        """
        return self._gen_syntax(columns, engine)

    # Naming --------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _gen_tname(self) -> str:
        "(cfunc) Generate an unique `TEMPORARY` table name `<str>`."
        temp: str = self._name_pfix + "tmp" + str(self._temp_id)
        if str_len(temp) > 64:
            self._temp_id = 0
            temp = self._name_pfix + "tmp0"
        self._temp_id += 1
        return self._db_pfix + temp

    def gen_tname(self) -> str:
        "Generate an unique `TEMPORARY` table name `<str>`."
        return self._gen_tname()

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _gen_pname(self) -> str:
        "(cfunc) Generate `placeholder` name for the table `<str>`."
        uuid_: str = str(uuid4())
        return "$_" + self._name_pfix + uuid_ + "_$"

    def gen_pname(self) -> str:
        "Generate `placeholder` name for the table `<str>`."
        return self._gen_pname()

    def get_name(self) -> str:
        """Get the name of the table, e.g. `'users'`.
        Equivalent to `self.name` property `<str>`.
        """
        return self._name

    def get_fname(self) -> str:
        """Get the full name of the table. e.g. `'db.users'`.
        Equivalent to `self.fname` property `<str>`.
        """
        return self._fname

    # Initiate ------------------------------------------------------------
    @query_exc_handler()
    async def initiate(self) -> bool:
        """Initiate the table.

        :return `bool`: `<bool>` Whether the table is initiated.
        """
        # Already initiated
        if self._initiated:
            return True

        # Create if not exists
        if not await self._exists(self._name):
            await self._create(self._name)
        # Synchronize table information
        else:
            await self._sync_information(self._name)
        # Update status
        self._initiated = True

        # Return status
        return True

    async def _sync_information(self, name: str) -> None:
        """(Base method, internal use only). Synchronize Table information."""
        fetch: dict
        async with self._server.acquire() as conn:
            async with conn.cursor(DictCursor, False) as cur:
                await cur.execute(
                    "SELECT table_collation AS c, engine AS e FROM information_schema.tables\n"
                    "WHERE table_schema = '%s' AND table_name = '%s';"
                    % (self._db._name, name)
                )
                fetch = await cur.fetchone()
        try:
            charSet = charset_by_collate(fetch["c"])
            engine: str = fetch["e"]
        except Exception as err:
            raise errors.QueryOperationalError(
                "<{}> Failed to update Table information: {}".format(self._fname, err)
            ) from err
        self._charset = charSet._name
        self._collate = charSet._collate
        self._engine = engine

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _set_init_tables(self, names: list[str]):
        "Set initiated tables by the given table names (internal user only)."
        self._initiated_tables = set(names)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _add_init_table(self, name: str):
        "Add initiated table (internal user only)."
        set_add(self._initiated_tables, name)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _add_init_tables(self, names: list[str]):
        "Add initiated tables (internal user only)."
        for name in names:
            set_add(self._initiated_tables, name)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _rem_init_table(self, name: str):
        "Remove initiated table (internal user only)."
        if set_contains(self._initiated_tables, name):
            set_discard(self._initiated_tables, name)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _rem_init_tables(self, names: list[str]):
        "Remove initiated tables (internal user only)."
        for name in names:
            if set_contains(self._initiated_tables, name):
                set_discard(self._initiated_tables, name)

    # Core SQL ------------------------------------------------------------
    def acquire(self) -> PoolConnectionManager:
        """Acquire a free connection from the `Server` pool.

        By acquiring connection through this method, the following will happen:
        - 1. Acquire a free/new connection from the Server pool.
        - 2. Return `PoolConnectionManager` that wraps the connection.
        - 3. Release the connection back to the pool at exist.

        This method provides a more flexible approach to execute queries compared
        to the `transaction()` method. However, it requires manual handling of
        transaction states like `BEGIN`, `ROLLBACK`, and `COMMIT`.

        :raise: Subclass of `QueryError`.
        :return `PoolConnectionManager`: Server connection.

        ### Example:
        >>> async with db.user.acquire() as conn:
                await conn.begin() # . start transaction
                username = (
                    await db.user.select("username")
                    .where("id = %s", 1)
                    .for_update()
                    # IMPORTANT: must pass conn to `execute()`. Otherwise, the
                    # query will be executed with a temp (different) connection.
                    .execute(conn)
                )
                ... # . sequences of queries
                await conn.commit() # . commit transaction
        """
        return self._server.acquire()

    def transaction(self) -> PoolTransactionManager:
        """Acquire a free connection from the `Server` pool and `START TRANSACTION`.

        By acquiring connection through this method, the following will happen:
        - 1. Acquire a free/new connection from the Server pool.
        - 2. Use the connection to `START TRANSACTION`.
        - 3. Return `PoolTransactionManager` that wraps the connection.
        - 4a. If catches ANY exceptions during the transaction, execute
            `ROLLBACK`, then close and release the connection.
        - 4b. If the transaction executed successfully, execute `COMMIT`
             and then release the connection back to the Server pool.

        This method offers a more convenient way to execute transactions
        compared to the `acquire()` method, as it automatically manages
        transaction states like `BEGIN`, `ROLLBACK`, and `COMMIT`.

        :raise: Subclass of `QueryError`.
        :return `PoolTransactionManager`: Server connection.

        ### Example:
        >>> async with db.user.transaction() as conn:
                # . transaction is already started
                username = (
                    await db.user.select("username")
                    .where("id = %s", 1)
                    .for_update()
                    # IMPORTANT: must pass conn to `execute()`. Otherwise, the
                    # query will be executed with a temp (different) connection.
                    .execute(conn)
                )
                ... # . sequences of queries
                # . commit will be executed at exist.
        """
        return self._server.transaction()

    async def execute_query(
        self,
        stmt: str,
        args: Union[list, tuple, None] = None,
        conn: Union[Connection, None] = None,
        reusable: bool = True,
        cursor: type[Cursor | SSCursor] = Cursor,
        timeout: Union[int, None] = None,
        warnings: bool = True,
        *,
        resolve_absent_table: bool = False,
    ) -> int:
        """Execute a SQL statement.

        :param stmt: `<str>` The plain SQL statement to be executed.
        :param args: `<list/tuple>` Arguments for the `'%s'` placeholders in 'stmt'. Defaults to `None`.
        :param conn: `<Connection>` Specific connection to execute this query. Defaults to `None`.
            - If provided, the conn will be used to execute the SQL 'stmt'.
              This parameter is typically used within the `acquire()` or
              `transaction()` context.
            - If `None`, a temporary conn will be acquired from the Server pool
              to execute the `stmt`. After execution, the temporary conn will
              execute `COMMIT` and release back to the Server pool.

        :param reusable: `<bool>` Whether the 'conn' (if provided) is reusable after query execution. Defaults to `True`.
            - If `True`, the connection will return back to the Server pool,
              waiting for the next query.
            - If `False`, after returned to the Server pool, the connection
              will be closed and released. This is useful for certain types
              of statements, such as `CREATE TEMPORARY TABLE` and `LOCK TABLES`,
              where it's desirable to ensure the connection is closed at the end
              to release (potential) resources.

        :param cursor: `<type[Cursor/SSCursor]>` The `Cursor` class to use for query execution. Defaults to `Cursor`.

        :param timeout: `<int>` Query execution timeout in seconds. Dafaults to `None`.
            - If set to `None` or `0`, `tables.server.query_timeout` will be used
              as the default timeout.
            - `SQLQueryTimeoutError` will be raised when the timeout is reached.

        :param warnings: `<bool>` Whether to issue any SQL related warnings. Defaults to `True`.

        :param resolve_absent_table: `<bool>` Whether to resolve absent table. Defaults to `False`.
            - If `True`, when `stmt` involves a table that does not exist, an attempt
              will be made to create the missing table (if it belongs to the current
              database). If creation failed, an `SQLQueryProgrammingError` will be
              raised; otherwise, an `SQLQueryTableDontExistsError` will be raised.
            - If `False`, when `stmt` involves a table that does not exist, instead of
              raising an error, `0` will be returned as the execution result.

        :raises: Subclass of `QueryError`.
        :return `<int>`: Number of rows affected by the query.

        ### Example:
        >>> await db.user.execute_query(
                "UPDATE db.user SET name = %s WHERE id = %s;",
                args=('john', 1), # muti-rows: arge=[('john', 1), ('jackson', 2)]
                conn=None,
                reusable=True,
                cursor=Cursor,
                resolve_absent_table=False,
                timeout=None,
                warnings=True,
            )
        """
        return await self._db.execute_query(
            stmt,
            args=args,
            conn=conn,
            reusable=reusable,
            cursor=cursor,
            timeout=timeout,
            warnings=warnings,
            resolve_absent_table=resolve_absent_table,
        )

    @overload
    async def fetch_query(
        self,
        stmt: str,
        args: Union[list, tuple, None] = None,
        conn: Union[Connection, None] = None,
        cursor: type[DictCursor | SSDictCursor] = DictCursor,
        timeout: Union[int, None] = None,
        warnings: bool = True,
        *,
        resolve_absent_table: bool = False,
    ) -> tuple[dict[str, Any]]:
        """Execute a SQL statement and fetch the result.

        :param stmt: `<str>` The plain SQL statement to be executed.
        :param args: `<list/tuple>` Arguments for the `'%s'` placeholders in 'stmt'. Defaults to `None`.
        :param conn: `<Connection>` Specific connection to execute this query. Defaults to `None`.
            - If provided, the conn will be used to execute the SQL 'stmt'.
              This parameter is typically used within the `acquire()` or
              `transaction()` context.
            - If `None`, a temporary conn will be acquired from the Server pool
              to execute the `stmt`. After execution, the temporary conn will
              execute `COMMIT` and release back to the Server pool.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :param timeout: `<int>` Query execution timeout in seconds. Dafaults to `None`.
            - If set to `None` or `0`, `tables.server.query_timeout` will be used
              as the default timeout.
            - `SQLQueryTimeoutError` will be raised when the timeout is reached.

        :param warnings: `<bool>` Whether to issue any SQL related warnings. Defaults to `True`.

        :param resolve_absent_table: `<bool>` Whether to resolve absent table. Defaults to `False`.
            - If `True`, when `stmt` involves a table that does not exist, an attempt
              will be made to create the missing table (if it belongs to the current
              database). If creation failed, an `SQLQueryProgrammingError` will be
              raised; otherwise, an `SQLQueryTableDontExistsError` will be raised.
            - If `False`, when `stmt` involves a table that does not exist, instead of
              raising an error, an empty `<tuple>` will be returned as the execution
              result.

        :raises: Subclass of `QueryError`.
        :return `<tuple[dict[str, Any]]>`: The fetched result.

        Example:
        >>> await db.user.fetch_query(
                "SELECT name, price FROM db.user WHERE id = %s",
                args=(1,), # does not support multi-rows arguments.
                conn=None,
                cursor=DictCursor,
                resolve_absent_table=False,
                timeout=10,
                warnings=True,
            )
        """

    @overload
    async def fetch_query(
        self,
        stmt: str,
        args: Union[list, tuple, None] = None,
        conn: Union[Connection, None] = None,
        cursor: type[DfCursor | SSDfCursor] = DictCursor,
        timeout: Union[int, None] = None,
        warnings: bool = True,
        *,
        resolve_absent_table: bool = False,
    ) -> DataFrame:
        """Execute a SQL statement and fetch the result.

        :param stmt: `<str>` The plain SQL statement to be executed.
        :param args: `<list/tuple>` Arguments for the `'%s'` placeholders in 'stmt'. Defaults to `None`.
        :param conn: `<Connection>` Specific connection to execute this query. Defaults to `None`.
            - If provided, the conn will be used to execute the SQL 'stmt'.
              This parameter is typically used within the `acquire()` or
              `transaction()` context.
            - If `None`, a temporary conn will be acquired from the Server pool
              to execute the `stmt`. After execution, the temporary conn will
              execute `COMMIT` and release back to the Server pool.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :param timeout: `<int>` Query execution timeout in seconds. Dafaults to `None`.
            - If set to `None` or `0`, `tables.server.query_timeout` will be used
              as the default timeout.
            - `SQLQueryTimeoutError` will be raised when the timeout is reached.

        :param warnings: `<bool>` Whether to issue any SQL related warnings. Defaults to `True`.

        :param resolve_absent_table: `<bool>` Whether to resolve absent table. Defaults to `False`.
            - If `True`, when `stmt` involves a table that does not exist, an attempt
              will be made to create the missing table (if it belongs to the current
              database). If creation failed, an `SQLQueryProgrammingError` will be
              raised; otherwise, an `SQLQueryTableDontExistsError` will be raised.
            - If `False`, when `stmt` involves a table that does not exist, instead of
              raising an error, an empty `<DataFrame>` will be returned as the execution
              result.

        :raises: Subclass of `QueryError`.
        :return `<DataFrame>`: The fetched result.

        Example:
        >>> await db.user.fetch_query(
                "SELECT name, price FROM db.user WHERE id = %s",
                args=(1,), # does not support multi-rows arguments.
                conn=None,
                cursor=DictCursor,
                resolve_absent_table=False,
                timeout=10,
                warnings=True,
            )
        """

    @overload
    async def fetch_query(
        self,
        stmt: str,
        args: Union[list, tuple, None] = None,
        conn: Union[Connection, None] = None,
        cursor: type[Cursor | SSCursor] = DictCursor,
        timeout: Union[int, None] = None,
        warnings: bool = True,
        *,
        resolve_absent_table: bool = False,
    ) -> tuple[tuple[Any]]:
        """Execute a SQL statement and fetch the result.

        :param stmt: `<str>` The plain SQL statement to be executed.
        :param args: `<list/tuple>` Arguments for the `'%s'` placeholders in 'stmt'. Defaults to `None`.
        :param conn: `<Connection>` Specific connection to execute this query. Defaults to `None`.
            - If provided, the conn will be used to execute the SQL 'stmt'.
              This parameter is typically used within the `acquire()` or
              `transaction()` context.
            - If `None`, a temporary conn will be acquired from the Server pool
              to execute the `stmt`. After execution, the temporary conn will
              execute `COMMIT` and release back to the Server pool.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :param timeout: `<int>` Query execution timeout in seconds. Dafaults to `None`.
            - If set to `None` or `0`, `tables.server.query_timeout` will be used
              as the default timeout.
            - `SQLQueryTimeoutError` will be raised when the timeout is reached.

        :param warnings: `<bool>` Whether to issue any SQL related warnings. Defaults to `True`.

        :param resolve_absent_table: `<bool>` Whether to resolve absent table. Defaults to `False`.
            - If `True`, when `stmt` involves a table that does not exist, an attempt
              will be made to create the missing table (if it belongs to the current
              database). If creation failed, an `SQLQueryProgrammingError` will be
              raised; otherwise, an `SQLQueryTableDontExistsError` will be raised.
            - If `False`, when `stmt` involves a table that does not exist, instead of
              raising an error, an empty `tuple[tuple]` will be returned as the execution
              result.

        :raises: Subclass of `QueryError`.
        :return `<tuple[tuple]>`: The fetched result.

        Example:
        >>> await db.user.fetch_query(
                "SELECT name, price FROM db.user WHERE id = %s",
                args=(1,), # does not support multi-rows arguments.
                conn=None,
                cursor=DictCursor,
                resolve_absent_table=False,
                timeout=10,
                warnings=True,
            )
        """

    async def fetch_query(
        self,
        stmt: str,
        args: Union[list, tuple, None] = None,
        conn: Union[Connection, None] = None,
        cursor: type[
            DictCursor | SSDictCursor | DfCursor | SSDfCursor | Cursor | SSCursor
        ] = DictCursor,
        timeout: Union[int, None] = None,
        warnings: bool = True,
        *,
        resolve_absent_table: bool = False,
    ) -> Union[tuple[dict[str, Any] | tuple[Any]], DataFrame]:
        """Execute a SQL statement and fetch the result.

        :param stmt: `<str>` The plain SQL statement to be executed.
        :param args: `<list/tuple>` Arguments for the `'%s'` placeholders in 'stmt'. Defaults to `None`.
        :param conn: `<Connection>` Specific connection to execute this query. Defaults to `None`.
            - If provided, the conn will be used to execute the SQL 'stmt'.
              This parameter is typically used within the `acquire()` or
              `transaction()` context.
            - If `None`, a temporary conn will be acquired from the Server pool
              to execute the `stmt`. After execution, the temporary conn will
              execute `COMMIT` and release back to the Server pool.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :param timeout: `<int>` Query execution timeout in seconds. Dafaults to `None`.
            - If set to `None` or `0`, `tables.server.query_timeout` will be used
              as the default timeout.
            - `SQLQueryTimeoutError` will be raised when the timeout is reached.

        :param warnings: `<bool>` Whether to issue any SQL related warnings. Defaults to `True`.

        :param resolve_absent_table: `<bool>` Whether to resolve absent table. Defaults to `False`.
            - If `True`, when `stmt` involves a table that does not exist, an attempt
              will be made to create the missing table (if it belongs to the current
              database). If creation failed, an `SQLQueryProgrammingError` will be
              raised; otherwise, an `SQLQueryTableDontExistsError` will be raised.
            - If `False`, when `stmt` involves a table that does not exist, instead of
              raising an error, an empty `tuple` or `DataFrame` (depends on 'cursor' type)
              will be returned as the execution result.

        :raises: Subclass of `QueryError`.
        :return `<tuple/DataFrame>`: The fetched result (depends on 'cursor' type).

        Example:
        >>> await db.user.fetch_query(
                "SELECT name, price FROM db.user WHERE id = %s",
                args=(1,), # does not support multi-rows arguments.
                conn=None,
                cursor=DictCursor,
                resolve_absent_table=False,
                timeout=10,
                warnings=True,
            )
        """
        return await self._db.fetch_query(
            stmt,
            args=args,
            conn=conn,
            cursor=cursor,
            resolve_absent_table=resolve_absent_table,
            timeout=timeout,
            warnings=warnings,
        )

    async def begin(self, conn: Connection) -> Connection:
        """`BEGIN` a transaction.

        This method serves as an alternative to `conn.begin()`,
        with additional error note to the exception when the
        `BEGIN` operation fails.

        :param conn: `<Connection>` The connection to start transaction.
        :raise: Subclass of `QueryError`.
        :return <`Connection`>: The connection that began the transaction.
        """
        return await self._db.begin(conn)

    async def start(self, conn: Connection) -> Connection:
        """`START TRANSACTION`. Alias for `begin()`."""
        return await self._db.begin(conn)

    async def rollback(self, conn: Connection) -> Connection:
        """`ROLLBACK` the transaction.

        This method serves as an alternative to `conn.rollback()`,
        with additional error note to the exception when the
        `ROLLBACK` operation fails.

        :param conn: `<Connection>` The connection to rollback transaction.
        :raise: Subclass of `QueryError`.
        :return <`Connection`>: The connection rolled back the transaction.
        """
        return await self._db.rollback(conn)

    async def commit(self, conn: Connection) -> Connection:
        """`COMMIT` a transaction.

        This method serves as an alternative to `conn.commit()`,
        with additional error note to the exception when the
        `COMMIT` operation fails.

        :param conn: `<Connection>` The connection to commit transaction.
        :raise: Subclass of `QueryError`.
        :return <`Connection`>: The connection committed the transaction.
        """
        return await self._db.commit(conn)

    async def create_savepoint(
        self,
        conn: Connection,
        savepoint: str = "sp",
    ) -> Connection:
        """Create a transaction `SAVEPOINT`.

        :param conn: `<Connection>` The connection of a transaction.
        :param savepoint: `<str>` The name of the transaction `SAVEPOINT`. Defaults to `'sp'`.
        :raise: Subclass of `QueryError`.
        :return <`Connection`>: The connection that created the `SAVEPOINT`.
        """
        return await self._db.create_savepoint(conn, savepoint=savepoint)

    async def rollback_savepoint(
        self,
        conn: Connection,
        savepoint: str = "sp",
    ) -> Connection:
        """`ROLLBACK` to a transaction `SAVEPOINT`.

        :param conn: `<Connection>` The connection of a transaction.
        :param savepoint: `<str>` The name of the transaction `SAVEPOINT`. Defaults to `'sp'`.
        :raise: Subclass of `QueryError`.
        :return <`Connection`>: The connection that rolled back to `SAVEPOINT`.
        """
        return await self._db.rollback_savepoint(conn, savepoint=savepoint)

    async def _lock(
        self,
        name: str,
        conn: Connection,
        write: cython.bint,
    ) -> Connection:
        """(Base method, internal use only). `LOCK` the table.

        :param name: `<str>` Name of the table.
        :param conn: `<Connection>` The connection to issue the lock.
        :param write: `<bool>` Whether to lock for write.
        :raise: Subclass of `QueryError`.
        :return <`Connection`>: The connection that locked the table.
        """
        try:
            conn._reusable = False
            lock_type: str = " WRITE" if write else " READ"
            syntax: str = self._db_pfix + name + lock_type
            async with conn.cursor(Cursor, False) as cur:
                await cur.execute("LOCK TABLES %s;" % syntax)
            return conn
        except Exception as err:
            err.add_note("-> <%s> Failed to LOCK table: %s" % (self._db._name, name))
            raise err

    async def lock(self, conn: Connection, write: bool = True) -> Connection:
        """`LOCK` the table.

        :param conn: `<Connection>` The connection to issue the lock.
        :param write: `<bool>` Whether to lock for write. Defaults to `True`
        :raise: Subclass of `QueryError`.
        :return <`Connection`>: The connection that locked the table.
        """
        return await self._lock(self._name, conn, write)

    async def unlock(self, conn: Connection) -> Connection:
        """`UNLOCK TABLES` that were previously locked by a connection.

        :param conn: `<Connection>` The connection that previously issued the lock.
        :raise: Subclass of `QueryError`.
        :return <`Connection`>: The conn that unlocked the tables.
        """
        return await self._db.unlock(conn)

    async def drop_temp(self, conn: Connection, temp: str) -> bool:
        """DROP TEMPORARY TABLE (IF EXISTS) from the connection.

        :param conn: `<Connection>` The connection that created the TEMPORARY table.
        :param temp: `<str>` Name of the TEMPORARY table. e.g. `'db.user_tmp0'`.
        :raise: Subclass of `QueryError`.
        :return <`bool`>: Whether the TEMPORARY table has been dropped.
        """
        async with conn.cursor(Cursor, False) as cur:
            await cur.execute("DROP TEMPORARY TABLE IF EXISTS %s;" % temp)
        return True

    # Basic SQL -----------------------------------------------------------
    async def create(self) -> bool:
        """`CREATE` the table `IF NOT EXSITS`.

        :raise: Subclass of `QueryError`.
        :return <`bool`>: Whether the table has been created.
        """
        return await self._create(self._name)

    async def _create(self, name: str) -> bool:
        """(Base method, internal use only). `CREATE` the table `IF NOT EXSITS`.

        :param name: `<str>` Name of the table.
        :raise: Subclass of `QueryError`.
        :return <`bool`>: Whether the table has been created.
        """
        # Create table
        async with self._server.acquire() as conn:
            async with conn.cursor(Cursor, False) as cur:
                await cur.execute(
                    "CREATE TABLE IF NOT EXISTS %s.%s %s;"
                    % (self._db._name, name, self._syntax_val)
                )

        # Add to initiated tables
        self._add_init_table(name)
        return True

    async def exists(self) -> bool:
        """Check if the table exists.

        :raise: Subclass of `QueryError`.
        :return <`bool`>: Whether the table exists.
        """
        return await self._exists(self._name)

    async def _exists(self, name: str) -> bool:
        """(Base method, internal use only). Check if the table exists.

        :param name: `<str>` Name of the table.
        :raise: Subclass of `QueryError`.
        :return <`bool`>: Whether the table exists.
        """
        # Check if already initiated
        if set_contains(self._initiated_tables, name):
            return True

        # Select from database schema
        fetch: tuple
        async with self._server.acquire() as conn:
            async with conn.cursor(Cursor, False) as cur:
                await cur.execute(
                    "SELECT COUNT(*) AS i FROM information_schema.tables\n"
                    "WHERE table_schema = '%s' AND table_name = '%s';"
                    % (self._db._name, name)
                )
                fetch = await cur.fetchone()

        # Check existance
        if fetch and fetch[0] > 0:
            self._add_init_table(name)
            return True
        else:
            self._rem_init_table(name)
            return False

    async def drop(self) -> bool:
        """`DROP` the table(s) `IF EXISTS`.

        :raise: Subclass of `QueryError`.
        :return <`bool`>: Whether the table has been dropped.
        """
        # Execute drop query
        async with self._server.acquire() as conn:
            async with conn.cursor(Cursor, False) as cur:
                await cur.execute("DROP TABLE IF EXISTS %s;" % self._fname)

        # Reset initiation status
        self._rem_init_table(self._name)
        self._initiated = False
        return True

    async def empty(self) -> bool:
        """Check if the table is empty.

        :raise: Subclass of `QueryError`.
        :return <`bool`>: Whether the table is empty.
        """
        # Select from database schema
        fetch: tuple
        async with self._server.acquire() as conn:
            async with conn.cursor(Cursor, False) as cur:
                await cur.execute(
                    "SELECT table_rows AS i FROM information_schema.tables\n"
                    "WHERE table_schema = '%s' AND table_name = '%s';"
                    % (self._db._name, self._name)
                )
                fetch = await cur.fetchone()

        # Check emptyness
        return False if fetch and fetch[0] > 0 else True

    async def truncate(self) -> bool:
        """`TRUNCATE` the table.

        :raise: Subclass of `QueryError`.
        :return <`bool`>: Whether the table has been truncated.
        """
        return await self._truncate(self._name)

    async def _truncate(self, name: str) -> bool:
        """(Base method, internal use only). `TRUNCATE` the table.

        :param name: `<str>` Name of the table.
        :raise: Subclass of `QueryError`.
        :return <`bool`>: Whether the table has been truncated.
        """
        async with self._server.acquire() as conn:
            async with conn.cursor(Cursor, False) as cur:
                await cur.execute("TRUNCATE TABLE %s.%s;" % (self._db._name, name))
        return True

    @overload
    async def information(
        self,
        *info: Literal[
            "*",
            "table_name",
            "table_catalog",
            "table_schema",
            "table_type",
            "engine",
            "version",
            "row_format",
            "table_rows",
            "avg_row_length",
            "data_length",
            "max_data_length",
            "index_length",
            "data_free",
            "auto_increment",
            "create_time",
            "update_time",
            "check_time",
            "table_collation",
            "checksum",
            "create_options",
            "table_comment",
        ],
        cursor: type[DictCursor | SSDictCursor] = DictCursor,
    ) -> tuple[dict[str, Any]]:
        """Select table information from `INFORMATION_SCHEMA`.

        Available information options:
        - 'table_name', 'table_catalog', 'table_schema', 'table_type', 'engine', 'version',
        - 'row_format', 'table_rows', 'avg_row_length', 'data_length', 'max_data_length',
        - 'index_length', 'data_free', 'auto_increment', 'create_time', 'update_time',
        - 'check_time', 'table_collation', 'checksum', 'create_options', 'table_comment'

        :param info: `<str>` The information to be selected.
            - If not specified, defaults to `'table_name'`.
            - Use `'*'` to select all information.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :raise: Subclass of `QueryError`.
        :return `<tuple[dict]>`: Table information..
        """

    @overload
    async def information(
        self,
        *info: Literal[
            "*",
            "table_name",
            "table_catalog",
            "table_schema",
            "table_type",
            "engine",
            "version",
            "row_format",
            "table_rows",
            "avg_row_length",
            "data_length",
            "max_data_length",
            "index_length",
            "data_free",
            "auto_increment",
            "create_time",
            "update_time",
            "check_time",
            "table_collation",
            "checksum",
            "create_options",
            "table_comment",
        ],
        cursor: type[DfCursor | SSDfCursor] = DictCursor,
    ) -> DataFrame:
        """Select table information from `INFORMATION_SCHEMA`.

        Available information options:
        - 'table_name', 'table_catalog', 'table_schema', 'table_type', 'engine', 'version',
        - 'row_format', 'table_rows', 'avg_row_length', 'data_length', 'max_data_length',
        - 'index_length', 'data_free', 'auto_increment', 'create_time', 'update_time',
        - 'check_time', 'table_collation', 'checksum', 'create_options', 'table_comment'

        :param info: `<str>` The information to be selected.
            - If not specified, defaults to `'table_name'`.
            - Use `'*'` to select all information.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :raise: Subclass of `QueryError`.
        :return `<DataFrame>`: Table information.
        """

    @overload
    async def information(
        self,
        *info: Literal[
            "*",
            "table_name",
            "table_catalog",
            "table_schema",
            "table_type",
            "engine",
            "version",
            "row_format",
            "table_rows",
            "avg_row_length",
            "data_length",
            "max_data_length",
            "index_length",
            "data_free",
            "auto_increment",
            "create_time",
            "update_time",
            "check_time",
            "table_collation",
            "checksum",
            "create_options",
            "table_comment",
        ],
        cursor: type[Cursor | SSCursor] = DictCursor,
    ) -> tuple[tuple[Any]]:
        """Select table information from `INFORMATION_SCHEMA`.

        Available information options:
        - 'table_name', 'table_catalog', 'table_schema', 'table_type', 'engine', 'version',
        - 'row_format', 'table_rows', 'avg_row_length', 'data_length', 'max_data_length',
        - 'index_length', 'data_free', 'auto_increment', 'create_time', 'update_time',
        - 'check_time', 'table_collation', 'checksum', 'create_options', 'table_comment'

        :param info: `<str>` The information to be selected.
            - If not specified, defaults to `'table_name'`.
            - Use `'*'` to select all information.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :raise: Subclass of `QueryError`.
        :return `<tuple[tuple]>`: Table information.
        """

    async def information(
        self,
        *info: Literal[
            "*",
            "table_name",
            "table_catalog",
            "table_schema",
            "table_type",
            "engine",
            "version",
            "row_format",
            "table_rows",
            "avg_row_length",
            "data_length",
            "max_data_length",
            "index_length",
            "data_free",
            "auto_increment",
            "create_time",
            "update_time",
            "check_time",
            "table_collation",
            "checksum",
            "create_options",
            "table_comment",
        ],
        cursor: type[
            DictCursor | SSDictCursor | DfCursor | SSDfCursor | Cursor | SSCursor
        ] = DictCursor,
    ) -> Union[tuple[dict[str, Any] | tuple[Any]], DataFrame]:
        """Select table information from `INFORMATION_SCHEMA`.

        Available information options:
        - 'table_name', 'table_catalog', 'table_schema', 'table_type', 'engine', 'version',
        - 'row_format', 'table_rows', 'avg_row_length', 'data_length', 'max_data_length',
        - 'index_length', 'data_free', 'auto_increment', 'create_time', 'update_time',
        - 'check_time', 'table_collation', 'checksum', 'create_options', 'table_comment'

        :param info: `<str>` The information to be selected.
            - If not specified, defaults to `'table_name'`.
            - Use `'*'` to select all information.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :raise: Subclass of `QueryError`.
        :return `<tuple/DataFrame>`: Table information (depends on 'cursor' type).
        """
        # Check info
        info_: str
        if not info:
            info_ = "table_name"
        elif "*" in info:
            info_ = "*"
        else:
            info_ = ", ".join(map(str, info))

        # Execute select query
        async with self._server.acquire() as conn:
            async with conn.cursor(cursor, False) as cur:
                await cur.execute(
                    "SELECT %s FROM information_schema.tables\n"
                    "WHERE table_schema = '%s' AND table_name = '%s';"
                    % (info_, self._db._name, self._name)
                )
                return await cur.fetchall()

    @overload
    async def describe(
        self,
        cursor: type[DictCursor | SSDictCursor] = DictCursor,
    ) -> tuple[dict[str, Any]]:
        """`DESCRIBE` the table.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :raise: Subclass of `QueryError`.
        :return `<tuple[dict]>`: Table description.
        """

    @overload
    async def describe(
        self,
        cursor: type[DfCursor | SSDfCursor] = DictCursor,
    ) -> DataFrame:
        """`DESCRIBE` the table.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :raise: Subclass of `QueryError`.
        :return `<DataFrame>`: Table description.
        """

    @overload
    async def describe(
        self,
        cursor: type[Cursor | SSCursor] = DictCursor,
    ) -> tuple[tuple[Any]]:
        """`DESCRIBE` the table.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :raise: Subclass of `QueryError`.
        :return `<tuple[tuple]>`: Table description.
        """

    async def describe(
        self,
        cursor: type[
            DictCursor | SSDictCursor | DfCursor | SSDfCursor | Cursor | SSCursor
        ] = DictCursor,
    ) -> Union[tuple[dict[str, Any] | tuple[Any]], DataFrame]:
        """`DESCRIBE` the table.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :raise: Subclass of `QueryError`.
        :return `<tuple/DataFrame>`: Table description (depends on 'cursor' type).
        """
        return await self._describe(self._name, cursor)

    async def _describe(
        self,
        name: str,
        cursor: type[
            DictCursor | SSDictCursor | DfCursor | SSDfCursor | Cursor | SSCursor
        ],
    ) -> Union[tuple[dict | Any], DataFrame]:
        """(Base method, internal use only). `DESCRIBE` the table.

        :param name: `<str>` Name of the table.
        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :raise: Subclass of `QueryError`.
        :return `<tuple/DataFrame>`: Table description (depends on 'cursor' type).
        """
        # Execute describe query
        async with self._server.acquire() as conn:
            async with conn.cursor(cursor, False) as cur:
                await cur.execute("DESCRIBE %s.%s;" % (self._db._name, name))
                return await cur.fetchall()

    async def optimize(self) -> int:
        """`OPTIMIZE` the table.

        :raise: Subclass of `QueryError`.
        :return `<int>`: The number of table(s) been optimized.
        """
        return await self._optimize(self._name)

    async def _optimize(self, name: str) -> int:
        """(Base method, internal use only). `OPTIMIZE` the table.

        :param name: `<str>` Name of the table.
        :raise: Subclass of `QueryError`.
        :return `<int>`: The number of table(s) been optimized.
        """
        rows: cython.int
        async with self._server.acquire() as conn:
            async with conn.cursor(Cursor, False) as cur:
                rows = await cur.execute(
                    "OPTIMIZE TABLE %s.%s;" % (self._db._name, name)
                )
        return 1 if rows > 0 else 0

    async def alter_charset_collate(
        self,
        charset: str = "utf8mb4",
        collate: str = "utf8mb4_0900_as_cs",
    ) -> bool:
        """`ALTER` the `CHARACTER SET` and `COLLATION` of the table.

        :param charset: `<str>` The charset to apply. Defaults to `'utf8mb4'`.
        :param collate: `<str>` The collate to apply. Defaults to `'utf8mb4_0900_as_cs'`.
        :raise: Subclass of `QueryError`.
        :return `<bool>`: Whether charset & collate have been altered.

        Note: After altering the charset and collate, please manually update the
        corresponding Table's settings to ensure consistency between the program
        and the actual MySQL database. A restart of the program is typically required.
        """
        # Validate charset & collate
        try:
            charSet = self._validate_charset_collate(charset, collate)
        except ValueError as err:
            raise errors.QueryValueError(
                "<{}.alter_charset_collate> {}".format(self._fname, err)
            ) from err
        self._charset = charSet._name
        self._collate = charSet._collate

        # Execute alter query
        return await self._alter_charset_collate(
            self._name, self._charset, self._collate
        )

    async def _alter_charset_collate(
        self,
        name: str,
        charset: str,
        collate: str,
    ) -> bool:
        """(Base method, internal use only).
        `ALTER` the `CHARACTER SET` and `COLLATION` of the table.

        :param name: `<str>` Name of the table.
        :param charset: `<str>` The charset to apply.
        :param collate: `<str>` The collate to apply.
        :raise: Subclass of `QueryError`.
        :return `<bool>`: Whether charset & collate have been altered.
        """
        async with self._server.acquire() as conn:
            async with conn.cursor(Cursor, False) as cur:
                await cur.execute(
                    "ALTER TABLE %s.%s CHARACTER SET '%s' COLLATE '%s';"
                    % (self._db._name, name, charset, collate)
                )
        return True

    async def add_column(self, column: Column, after: Union[str, None] = None) -> bool:
        """`ADD` a `COLUMN` to the table.

        :param column: `<Column>` The new column to add to the table.
        :param after: `<str/None>` Name of the column the new column goes after. Defaults to `None`.
            - If `None`, the new column will be added at the end of the table.
            - If specified, the new column will be added after that column.

        :raise: Subclass of `QueryError`.
        :return `<bool>`: Whether the new column has been added.

        Note: After altering the column of the table, please manually update the
        corresponding Table's metadata to ensure consistency between the program
        and the actual MySQL database. A restart of the program is typically required.
        """
        return await self._add_column(self._name, column, after)

    async def _add_column(
        self,
        name: str,
        column: Column,
        after: Union[str, Column, None],
    ) -> bool:
        """(Base method, internal use only). `ADD` a `COLUMN` to the table.

        :param name: `<str>` Name of the table.
        :param column: `<Column>` The new column to add to the table.
        :param after: `<str/None>` Name of the column the new column goes after.
            - If `None`, the new column will be added at the end of the table.
            - If specified, the new column will be added after that column.

        :raise: Subclass of `QueryError`.
        :return `<bool>`: Whether the new column has been added.
        """
        # Validate column
        column = self._columns.validate(column)

        # Validate after
        after_syntax: str
        if after is not None:
            after = get_column_name(after)
            if not is_str(after):
                raise errors.QueryValueError(
                    "<{}.add_column> Parameter 'after' only accepts "
                    "instance of `<str>` or `<Column>`, instead of: "
                    "{} {}".format(self._fname, type(after), repr(after))
                )
            after_syntax = " AFTER %s" % after
        else:
            after_syntax = ""

        # Execute alter query
        async with self._server.acquire() as conn:
            async with conn.cursor(Cursor, False) as cur:
                await cur.execute(
                    "ALTER TABLE %s.%s ADD %s%s;"
                    % (self._db._name, name, column._syntax, after_syntax)
                )
        return True

    async def drop_column(self, column: Union[str, Column]) -> bool:
        """`DROP` a `Column` from the table.

        :param column: `<str/Column>` The column to drop from the table.
        :raise: Subclass of `QueryError`.
        :return `<bool>`: Whether the column has been dropped.

        Note: After altering the column of the table, please manually update the
        corresponding Table's metadata to ensure consistency between the program
        and the actual MySQL database. A restart of the program is typically required.
        """
        return await self._drop_column(self._name, column)

    async def _drop_column(self, name: str, column: Union[str, Column]) -> bool:
        """(Base method, internal use only). `DROP` a `Column` from the table.

        :param name: `<str>` Name of the table.
        :param column: `<str/Column>` The column to drop from the table.
        :raise: Subclass of `QueryError`.
        :return `<bool>`: Whether the column has been dropped.
        """
        # Validate column
        column_name = get_column_name(column)
        if not is_str(column_name):
            raise errors.QueryValueError(
                "<{}.drop_column> Parameter 'column' only accepts "
                "instance of `<str>` or `<Column>`, instead of: "
                "{} {}".format(self._fname, type(column), repr(column))
            )

        # Execute alter query
        async with self._server.acquire() as conn:
            async with conn.cursor(Cursor, False) as cur:
                await cur.execute(
                    "ALTER TABLE %s.%s DROP COLUMN %s;"
                    % (self._db._name, name, column_name)
                )
        return True

    async def add_index(self, index: Index) -> bool:
        """`ADD` an `Index` to the table.

        :param index `<Index>`: The new index to added to the table.
        :raise: Subclass of `QueryError`.
        :return `<bool>`: Whether the index has been added.

        Note: After altering the index of the table, please manually update the
        corresponding Table's metadata to ensure consistency between the program
        and the actual MySQL database. A restart of the program is typically required.
        """
        return await self._add_index(self._name, index)

    async def _add_index(self, name: str, index: Index) -> bool:
        """(Base method, internal use only). `ADD` an `Index` to the table.

        :param name: `<str>` Name of the table.
        :param index `<Index>`: The new index to added to the table.
        :raise: Subclass of `QueryError`.
        :return `<bool>`: Whether the index has been added.
        """
        # Validate index
        index = self._indexes.validate(index)

        # Execute alter query
        async with self._server.acquire() as conn:
            async with conn.cursor(Cursor, False) as cur:
                await cur.execute(
                    "ALTER TABLE %s.%s ADD %s;" % (self._db._name, name, index._syntax)
                )
        return True

    async def drop_index(self, index: Union[str, Index]) -> bool:
        """`DROP` an `Index` from the table.

        :param index `<str/Index>`: The index to drop from the table.
        :raise: Subclass of `QueryError`.
        :return `<bool>`: Whether the index has been dropped.

        Note: After altering the index of the table, please manually update the
        corresponding Table's metadata to ensure consistency between the program
        and the actual MySQL database. A restart of the program is typically required.
        """
        return await self._drop_index(self._name, index)

    async def _drop_index(self, name: str, index: Union[str, Index]) -> bool:
        """(Base method, internal use only). `DROP` an `Index` from the table.

        :param name: `<str>` Name of the table.
        :param index `<str/Index>`: The index to drop from the table.
        :raise: Subclass of `QueryError`.
        :return `<bool>`: Whether the index has been dropped.
        """
        # Validate index
        index_name = get_index_name(index)
        if not is_str(index_name):
            raise errors.QueryValueError(
                "<{}.drop_index> Parameter 'index' only accepts "
                "instance of `<str>` or `<Index>`, instead of: "
                "{} {}".format(self._fname, type(index), repr(index))
            )

        # Execute alter query
        async with self._server.acquire() as conn:
            async with conn.cursor(Cursor, False) as cur:
                await cur.execute(
                    "ALTER TABLE %s.%s DROP INDEX %s;"
                    % (self._db._name, name, index_name)
                )
        return True

    @overload
    async def show_index(
        self,
        cursor: type[DictCursor | SSDictCursor] = DictCursor,
    ) -> tuple[dict[str, Any]]:
        """`SHOW INDEX` from the table.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :raise: Subclass of `QueryError`.
        :return `<tuple[dict]>`: Index information.
        """

    @overload
    async def show_index(
        self,
        cursor: type[DfCursor | SSDfCursor] = DictCursor,
    ) -> DataFrame:
        """`SHOW INDEX` from the table.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :raise: Subclass of `QueryError`.
        :return `<DataFrame>`: Index information.
        """

    @overload
    async def show_index(
        self,
        cursor: type[Cursor | SSCursor] = DictCursor,
    ) -> tuple[tuple[Any]]:
        """`SHOW INDEX` from the table.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :raise: Subclass of `QueryError`.
        :return `<tuple[tuple]>`: Index information.
        """

    async def show_index(
        self,
        cursor: type[
            DictCursor | SSDictCursor | DfCursor | SSDfCursor | Cursor | SSCursor
        ] = DictCursor,
    ) -> Union[tuple[dict[str, Any] | tuple[Any]], DataFrame]:
        """`SHOW INDEX` from the table.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :raise: Subclass of `QueryError`.
        :return `<tuple/DataFrame>`: Index information (depends on 'cursor' type).
        """
        return await self._show_index(self._name, cursor)

    async def _show_index(
        self,
        name: str,
        cursor: type[
            DictCursor | SSDictCursor | DfCursor | SSDfCursor | Cursor | SSCursor
        ],
    ) -> Union[tuple[dict | Any], DataFrame]:
        """(Base method, internal use only). `SHOW INDEX` from the table.

        :param name: `<str>` Name of the table.
        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :raise: Subclass of `QueryError`.
        :return `<tuple/DataFrame>`: Index information (depends on 'cursor' type).
        """
        # Execute show query
        async with self._server.acquire() as conn:
            async with conn.cursor(cursor, False) as cur:
                await cur.execute("SHOW INDEX FROM %s.%s;" % (self._db._name, name))
                return await cur.fetchall()

    async def reset_indexes(self) -> int:
        """`RESET` all indexes of the table.

        This method is invoked when there's a change in the table index settings,
        ensuring the server's indexes align with the program's `Table.indexes`
        settings. The process involves two steps:
        1. Drop all existing indexes of the table (excludes `PRIMARY` key).
        2. Recreate indexes based on the current `Table.indexes` settings.

        :raise: Subclass of `QueryError`.
        :return `<int>`: The number of tables that indexes been reset.
        """
        return await self._reset_indexes(self._name)

    async def _reset_indexes(self, name: str) -> int:
        """(Base method, internal use only). `RESET` all indexes of the table.

        This method is invoked when there's a change in the table index settings,
        ensuring the server's indexes align with the program's `Table.indexes`
        settings. The process involves two steps:
        1. Drop all existing indexes of the table (excludes `PRIMARY` key).
        2. Recreate indexes based on the current `Table.indexes` settings.

        :param name: `<str>` Name of the table.
        :raise: Subclass of `QueryError`.
        :return `<int>`: The number of tables that indexes been reset.
        """
        # Get existing index names
        indexes: tuple = await self._show_index(name, DictCursor)
        idx_names: set = set()
        idx_name: str
        index: dict
        for index in indexes:
            idx_name = index["Key_name"]
            if idx_name != "PRIMARY":  # exclude primary key
                set_add(idx_names, idx_name)

        # No index to reset
        if not idx_names:
            return 0

        # Drop existing indexes
        async with self._server.acquire() as conn:
            for idx_name in idx_names:
                async with conn.cursor(Cursor, False) as cur:
                    await cur.execute(
                        "ALTER TABLE %s.%s DROP INDEX %s;"
                        % (self._db._name, name, idx_name)
                    )

        # Reset with new indexes
        idx: Index
        async with self._server.acquire() as conn:
            for idx in self._indexes._instances:
                async with conn.cursor(Cursor, False) as cur:
                    await cur.execute(
                        "ALTER TABLE %s.%s ADD %s;"
                        % (self._db._name, name, idx._syntax)
                    )

        # Return reset status
        return 1

    # SQL Query -----------------------------------------------------------
    def select(
        self,
        *columns: Union[str, Column],
        distinct: bool = False,
        buffer_result: bool = False,
        explain: bool = False,
        from_: Union[Table, TimeTable, SelectQuery, None] = None,
        tabletime: Union[str, datetime.date, datetime.datetime, None] = None,
        alias: Union[str, None] = None,
    ) -> SelectQuery:
        """Initiate a SELECT query of the table.

        :param columns: `<str/Column>` The columns to select, accept both column names `<str>` and instances `<Column>`.
            When a `SELECT` query involves `JOIN`, each column must prefixed
            with the correct alias. For more information about aliasing, see
            the 'alias' parameter.

        :param distinct: `<bool>` The `DISTINCT` modifier. Defaults to `False`.
            - Determines whether to select only distinct values.
            - *Notice: When the `SELECT...` query involes TimeTable(s) which
              `tabletime` are specified through `tabletimes()` method, setting
              `distict=True` will force the use of union select mode. For more
              detail, please refer to the `order_by` or `limit` methods.

        :param buffer_result: `<bool>` The `SQL_BUFFER_RESULT` modifier. Defaults to `False`.
            Determines whether to buffer the result set in MySQL Server memory.
            This can help release table locks earlier for large result sets, but
            will result in higher memory usage.

        :param explain: `<bool>` The `EXPLAIN` modifier. Defaults to `False`.
            Determines whether to return the MySQL optimizer execution plan
            instead of the actual result set. This is helpful for debugging
            and query optimization.

        :param from_: Specify the `FROM` clause. Defaults to `None`.
            - `None`: The `FROM` clause will default to the table that initiated the `SelectQuery`.
            - `<Table/TimeTable>`: The `FROM` clause will be generated based on the given table.
            - `<SelectQuery>`: The `FROM` cluase will be constructed as a subquery based on the
              given `SelectQuery` instance.
            - `<str>`: If the string corresponds to the table name of a table in the database,
              the `FROM` clause will be generated based on that table. Otherwise, the string will
              be treated as a raw sql syntax placed right after the `FROM` keyword.

        :param tabletime: `<str/None>` A specific `tabletime` for the `FORM` table. Defaults to `None`.
            - This parameter is only applicable when the argument 'from_' corresponds
              to a TimeTable (regardless type of `<str>` or `<TimeTable>` instance).
            - If `tabletime` is specified, the actual sub-table will derive from this
              parameter. Otherwise, it is required to `tabletimes()` method to specify
              the sub-tables. For more details, please refer to the `tabletimes()` method.

        :param alias: `<str/None>` The alias of the `SELECT... FROM...` clause. Defaults to `None`.
            - The alias of the clause will be added to the corresponding part of the SQL
              statement using the `'AS <alias>'` syntax.
            - For instance, in a `SELECT... FROM ...` query, without specified
              alias (default alias), the statement would be constructed as:
              `'SELECT ... FROM ... AS t1'`, where default alias is derived
              from the order of the tables in the query.
            - However, with a user-defined alias (for example, `alias='tb'`), the
              statement would be constructed as: `'SELECT ... FROM ... AS tb'`.
            - Although this aliasing behavior might not be necessary for certain SQL
              statements, this module enforces it to ensure all built-in methods can
              be used interchangeably.

        ### Example (select query):
        >>> data = (
                await db.user.select("t1.name", "t2.age")
                .join(db.user_info, "t1.id = t2.id", "t2.age > %s", args=20)
                .where(ins={"t1.name": ["John", "Mary"]})
                .order_by("age DESC", "name ASC")
                .limit(10)
                .execute()
            )
        ### -> Equivalent to:
        >>> SELECT t1.name, t2.age FROM db.user AS t1
            INNER JOIN db.user_info AS t2
                ON t1.id = t2.id AND t2.age > 20
            WHERE t1.name IN ('John', 'Mary')
            ORDER BY age DESC, name ASC
            LIMIT 10

        ### Example (user-defined alias):
        >>> db.user.select("name", "address", "order", alias="user")
        ### -> Equivalent to:
        >>> SELECT name, address, order FROM db.user AS user

        ### Example (from subquery):
        >>> db.user.select("name", from_=db.prime_user.select("name"))
        ### -> Equivalent to:
        >>> SELECT name FROM (SELECT name FROM db.prime_user AS t1) AS t1

        ### Example (from a TimeTable):
        >>> db.price.select("price", "change", tabletime="2021-01-01")
        ### -> Equivalent to:
        >>> SELECT price, change FROM db.price_20210101 AS t1

        ### Example (from temporary table <str>):
        >>> db.user.select("name", "address", "order", from_="db.user_tmp0")
        ### -> Equivalent to:
        >>> SELECT name, address, order FROM db.user_tmp0 AS t1
        """
        return SelectQuery(self).select(
            *columns,
            distinct=distinct,
            buffer_result=buffer_result,
            explain=explain,
            from_=from_,
            tabletime=tabletime,
            alias=alias,
        )

    def insert(
        self,
        *columns: Union[str, Column],
        ignore: bool = False,
        tabletime: Union[str, datetime.date, datetime.datetime, None] = None,
    ) -> InsertQuery:
        """Initiate an INSERT query of the table.

        :param columns: `<str/Column>` The columns targeted by the insert operation, accept both column names `<str>` and instances `<Column>`.
        :param ignore: `<bool>` The `IGNORE` modifier. Defaults to `False`.
            Determines whether to ignore the duplicate key errors.

        :param tabletime: `<str/None>` A specific `tabletime` for the `INSERT` table. Defaults to `None`.
            - This parameter is only applicable when the `INSERT` table corresponds
              to a TimeTable.
            - If `tabletime` is specified, the actual sub-table will derive from this
              parameter. Otherwise, it is required to use `tabletimes()` method at to
              specify the sub-tables. For more details, please refer to the `tabletimes()`
              method.

        ### Example (INSERT... VALUES...)
        >>> values = [
                {"id": 1, "name": "John", "age": 20, "status": "active"},
                {"id": 2, "name": "Mary", "age": 25, "status": "inactive"},
                ...
            ]
        >>> (
                await db.user.insert("name", "age")
                .values(values)
                # . the 'columns' arguemnt ('name', "age") of thr `insert()` method
                # . will be used to construct and validate the 'values' data.
                .execute()
            )
        ### -> Equivalent to:
        >>> INSERT INTO db.user (name, age)
            VALUES ('John', 20), ('Mary', 25), ...
            AS val  # (Applicable for MySQL 8.0 and later versions)

        ### Example (INSERT... SELECT...)
        >>> (
                await db.user.insert("name")
                .select(db.user_info, tabletime="2023-01-01")
                .execute()
            )
        ### -> Equivalent to:
        >>> INSERT INTO db.user (name)
            SELECT name FROM db.user_info_202301 AS t1
        """
        return InsertQuery(self).insert(*columns, ignore=ignore, tabletime=tabletime)

    def replace(
        self,
        *columns: Union[str, Column],
        tabletime: Union[str, datetime.date, datetime.datetime, None] = None,
    ) -> ReplaceQuery:
        """Initiate a REPLACE query of the table.

        :param columns: `<str/Column>` The columns targeted by the replace operation, accept both column names `<str>` and instances `<Column>`.
        :param tabletime: `<str/None>` A specific `tabletime` for the `REPLACE` table. Defaults to `None`.
            - This parameter is only applicable when the `REPLACE` table corresponds
              to a TimeTable.
            - If `tabletime` is specified, the actual sub-table will derive from this
              parameter. Otherwise, it is required to use `tabletimes()` method to
              specify the sub-tables. For more details, please refer to the `tabletimes()`
              method.

        ### Example (REPLACE... VALUES...)
        >>> values = [
                {"id": 1, "name": "John", "age": 20, "status": "active"},
                {"id": 2, "name": "Mary", "age": 25, "status": "inactive"},
                ...
            ]
        >>> (
                await db.user.replace("name", "age")
                .values(values)
                # The 'columns' arguemnt ('name', "age") of thr `replace()`
                # method will be used construct and validate the 'values' data.
                .execute()
            )
        ### -> Equivalent to:
        >>> REPLACE INTO db.user (name, age)
            VALUES ('John', 20), ('Mary', 25), ...

        ### Example (REPLACE... SELECT...)
        >>> (
                await db.user.replace("name")
                .select(db.user_info, tabletime="2023-01-01")
                .execute()
            )
        ### -> Equivalent to:
        >>> REPLACE INTO db.user (name)
            SELECT name FROM db.user_info_202301 AS t1
        """
        return ReplaceQuery(self).replace(*columns, tabletime=tabletime)

    def update(
        self,
        ignore: bool = False,
        tabletime: Union[str, datetime.date, datetime.datetime, None] = None,
        alias: Union[str, None] = None,
    ) -> UpdateQuery:
        """Initiate an UPDATE query of the table.

        :param ignore: `<bool>` The `IGNORE` modifier. Defaults to `False`.
            Determines whether to ignore the duplicate key errors.

        :param tabletime: `<str/None>` A specific `tabletime` for the `UPDATE` table. Defaults to `None`.
            - This parameter is only applicable when the `UPDATE` table corresponds
              to a TimeTable.
            - If `tabletime` is specified, the actual sub-table will derive from this
              parameter. Otherwise, it is required to use `tabletimes()` method to specify
              the sub-tables. For more details, please refer to the `tabletimes()` method.

        :param alias: `<str/None>` The alias of the `UPDATE` clause. Defaults to `None`.
            - The alias of the clause will be added to the corresponding part of the SQL
              statement using the `'AS <alias>'` syntax.
            - For instance, in a `UPDATE... JOIN... SET...` query, without specified
              alias (default alias), the statement would be constructed as:
              `'UPDATE... AS t1 JOIN... AS t2 SET ...'`, where default alias is derived
              from the order of the tables in the query.
            - However, with a user-defined alias (for example, `alias='tb'`), the
              statement would be constructed as: `'UPDATE... AS tb JOIN... AS t2 SET ...'`.

        ### Example (UPDATE... SET...)
        >>> (
                await db.user.update()
                .set("name = %s", "age = %s", args=["John", 20])
                .where("id = 1")
                .execute()
            )
        ### -> Equivalent to:
        >>> UPDATE db.user AS t1
            SET name = 'John', age = 20
            WHERE id = 1

        ### Example (UPDATE... JOIN... SET...)
        >>> (
                await db.user.update()
                .join(db.user_info, "t1.id = t2.user_id", tabletime="2023-01-01")
                .set("t1.name = t2.name", "t1.age = t2.age")
                .where("t1.age > 18")
                .execute()
            )
        ### -> Equivalent to:
        >>> UPDATE db.user AS t1
            INNER JOIN db.user_info_202301 AS t2
                ON t1.id = t2.user_id
            SET t1.name = t2.name, t1.age = t2.age
            WHERE t1.age > 18

        ### Example (UPDATE... with `values()` method)
        >>> values = [
                {"id": 1, "name": "John", "age": 20, "status": "active"},
                {"id": 2, "name": "Mary", "age": 25, "status": "inactive"},
            ]
        >>> (
                await db.user.update()
                .values(
                    values,
                    value_columns=["name", "age"],
                    where_columns=["id"])
                .execute()
            )
        ### -> Equivalent to the following TWO queries:
        >>> UPDATE db.user AS t1
            SET t1.name = 'John', t1.age = 20
            WHERE t1.id = 1
        >>> UPDATE db.user AS t1
            SET t1.name = 'Mary', t1.age = 25
            WHERE t1.id = 2
        """
        return UpdateQuery(self).update(ignore=ignore, tabletime=tabletime, alias=alias)

    def delete(
        self,
        *table_aliases: str,
        ignore: cython.bint = False,
        tabletime: Union[str, datetime.date, datetime.datetime, None] = None,
        alias: Union[str, None] = None,
    ) -> DeleteQuery:
        """Initiate a DELETE query of the table.

        :param table_aliases: `<str>` The table aliases of the DELETE operation.
            - Only applicable for multi-table DELETE (when JOIN clause is used).
              Single table DELETE takes no effects.
            - If not specified, the DELETE operation will be performed on all
              tables (main & joined ones).
            - If specified, the DELETE operation will be performed only on the
              given tables.
            - *Notice: this arguments only takes the alias of the tables instead
              of the actual table name. For more information, please refer to
              the 'alias' parameter or the Example section below.

        :param ignore: `<bool>` The `IGNORE` modifier. Defaults to `False`.
            Determines whether to ignore the duplicate key errors.

        :param tabletime: `<str/None>` A specific `tabletime` for the `DELETE` table. Defaults to `None`.
            - This parameter is only applicable when the `DELETE` table corresponds
              to a TimeTable.
            - If `tabletime` is specified, the actual sub-table will derive from this
              parameter. Otherwise, it is required to use `tabletimes()` method to specify
              the sub-tables. For more details, please refer to the `tabletimes()` method.

        :param alias: `<str/None>` The alias of the `DELETE` clause. Defaults to `None`.
            - The alias of the clause will be added to the corresponding part of the SQL
              statement using the `'AS <alias>'` syntax.
            - For instance, in a `DELETE... FROM... WHERE...` query, without specified
              alias (default alias), the statement would be constructed as:
              `'DELETE... FROM... AS t1 WHERE...'`, where default alias is derived
              from the order of the tables in the query.
            - However, with a user-defined alias (for example, `alias='tb'`), the
              statement would be constructed as: `'DELETE... FROM... AS tb WHERE...'`.

        ### Example (DELETE... WHERE... single table):
        >>> await db.user.delete().where("id = 1").execute()
        ### -> Equivalent to:
        >>> DELETE FROM db.user AS t1 WHERE id = 1

        ### Example (DELETE... JOIN... WHERE... multi-table [all tables]):
        >>> (
                await db.user.delete()  # delete from 't1' and 't2'
                .join(db.user_info, "t1.id = t2.user_id", tabletime="2023-01-01")
                .where("t1.age > 18")
                .execute()
            )
        ### -> Equivalent to:
        >>> DELETE t1, t2 FROM db.user AS t1
            INNER JOIN db.user_info_202301 AS t2
                ON t1.id = t2.user_id
            WHERE t1.age > 18

        ### Example (DELETE... JOIN... WHERE... multi-table [specific table(s)]):
        >>> (
                await db.user.delete("t2")  # Only delete from 't2'
                .join(db.user_info, "t1.id = t2.user_id", tabletime="2023-01-01")
                .where("t1.age > 18")
                .execute()
            )
        ### -> Equivalent to:
        >>> DELETE t2 FROM db.user AS t1
            INNER JOIN db.user_info_202301 AS t2
                ON t1.id = t2.user_id
            WHERE t1.age > 18

        ### Example (DELETE... with `values()` method):
        >>> values = [
                {"id": 1, "name": "John", "age": 20, "status": "active"},
                {"id": 2, "name": "Mary", "age": 25, "status": "inactive"},
            ]
        >>> (
                await db.user.delete()
                .values(values, where_columns=["name", "age"])
                .execute()
            )
        ### -> Equivalent to the following TWO queries:
        >>> DELETE FROM db.user AS t1
            WHERE t1.name = 'John' AND t1.age = 20
        >>> DELETE FROM db.user AS t1
            WHERE t1.name = 'Mary' AND t1.age = 25
        """
        return DeleteQuery(self).delete(
            *table_aliases, ignore=ignore, tabletime=tabletime, alias=alias
        )

    def create_temp(
        self,
        *columns: Union[str, Column],
        indexes: Union[list[str | Index], Literal["auto"], None] = None,
        engine: Literal["MEMORY", "InnoDB", "MyISAM"] = "MEMORY",
        charset: Union[str, None] = None,
        collate: Union[str, None] = None,
    ) -> CreateTempQuery:
        """Create a temporary table.

        :param columns: `<str/Column>` The columns of the temporary table, accept both `<str>` and column instances `<Column>`.
            - If columns are not specified, defaults to the 'columns' of the table
              that initiated the 'create_temp()' query.
            - By providing the Column instances or the name of the columns,
              the columns syntax will be generated based on the given columns.
            - However, a custom column syntax `<str>` is also acceptable,
              for example: `"id BIGINT NOT NULL UNSIGNED AUTO_INCREMENT PRIMARY KEY"`.
            - * Notice: Regardless the approach, the column name must belong
              to the table that initiated the query.

        :param indexes: `<list/'auto'/None>` The indexes of the temporary table. Defaults to `None`.
            - `None`: No indexes for the temporary table.
            - `'auto'`: Indexes syntax will be generated automatically based on
              the given columns that belongs to the table.
            - By providing a list/tuple of Index instances or the name of the
              indexes, the index syntax will be generated based on the given indexes.
            - However, a custom index syntax `<str>` is also acceptable,
              for example: `"INDEX idx1 (id, name)"`.

        :param engine: `<str>` The engine of the temporary table. Defaults to `'MEMORY'`.
        :param charset: `<str/None>` The charset of the temporary table. Defaults to `None` (table.charset).
        :param collate: `<str/None>` The collate of the temporary table. Defaults to `None` (table.collate).

        ### Example (CREATE TEMPORARY TABLE... SELECT...):
        >>> async with db.user.transaction() as conn:  # acquire connection
                tmp = ( # . 'tmp' is the name of the created temporary table.
                        await db.user.create_temp("id", "user_name", "age", indexes="auto")
                        # . specify temporary table columns to: "id", "user_name", "age".
                        # . auto generate indexes based on the given columns and table settings.
                        # . use 'MEMORY' engine and 'user' table's charset and collate
                        .select(db.user.select("id", "user_name", "age").where("age > 18")
                        # . select data from 'user' table.
                        .execute(conn)
                        # . provide the connection to `execute()` method.
                    )
                ...  # do something with the temporary table in the transaction
        ### -> Equivalent to:
        >>> CREATE TEMPORARY TABLE db.user_tmp0 (
                id BIGINT NOT NULL UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                user_name VARCHAR(255) NOT NULL,
                age INT NOT NULL,
                UNIQUE INDEX uixUserName (user_name)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_as_cs ENGINE = MEMORY
            SELECT id, user_name, age FROM db.user AS t1 WHERE age > 18;

        ### Example (CREATE TEMPORARY TABLE... VALUES...):
        >>> values = [
                {"id": 1, "name": "John", "age": 20, "status": "active"},
                {"id": 2, "name": "Mary", "age": 25, "status": "inactive"},
                ...
            ]
        >>> async with db.user.transaction() as conn:  # acquire connection
                tmp = ( # . 'tmp' is the name of the created temporary table.
                    await db.user.create_temp(
                        "id", "user_name", "age",
                        indexes=["UNIQUE INDEX idx1 (name)", "INDEX idx2 (age)"],
                        engine="InnoDB",
                        charset="utf8mb4", collate="utf8mb4_slovak_ci")
                        # . specify temporary table columns to: "id", "user_name", "age".
                        # . specify custom indexes for the temporary table.
                        # . use 'InnoDB' engine and 'utf8mb4' & 'utf8mb4_slovak_ci'.
                        .values(values, "name", "age")
                        # . specify the columns of the 'values' to insert. since 'id' is omitted,
                        # . only 'name' and 'age' will be inserted.
                        # . if columns are not specified, will defaults to 'id', 'user_name'
                        # . and 'age', which are the columns of the temporary table.
                        .execute(conn)
                        # . provide the connection to `execute()` method.
                    )
                ...  # do something with the temporary table in the transaction
        ### -> Equivalent to the following TWO queries:
        >>> CREATE TEMPORARY TABLE db.user_tmp1 (
                id BIGINT NOT NULL UNSIGNED AUTO_INCREMENT PRIMARY KEY,
                user_name VARCHAR(255) NOT NULL,
                age INT NOT NULL,
                UNIQUE INDEX idx1 (name),
                INDEX idx2 (age)
            ) CHARACTER SET utf8mb4 COLLATE utf8mb4_slovak_ci ENGINE = InnoDB;
        >>> INSERT INTO db.user_tmp1 (user_name, age)
            VALUES ('John', 20), ('Mary', 25), ...;
        """
        return CreateTempQuery(self).create_temp(
            *columns, indexes=indexes, engine=engine, charset=charset, collate=collate
        )

    def compare(
        self,
        src_data: Union[DataFrame, list[dict], tuple[dict]],
        bsl_data: Union[DataFrame, list[dict], tuple[dict]],
        unique_columns: Union[list[str | Column], tuple[str | Column]] = None,
        compare_columns: Union[list[str | Column], tuple[str | Column]] = None,
    ) -> CompareQuery:
        """Initiate a compare query of the table.

        The `CompareQuery` fulfills two purposes:
        - 1. Find out the differences and similarities between the source and
             the baseline data. By calling `results()` method at the end of the
             chain, an instance of `<COMPARE_RESULTS>` will be returned, which
             contains all the comparison information.
        - 2. Modify the table to match the source data. By providing the correct
             baseline data (what the table currently look like) and calling the
             `execute()` method at the end of the chain, the table will be modified
             to match the source data through a combination of `INSERT`, `UPDATE`
             and `DELETE` queries.
        - *. For more information about the data comparison, and how the arguments
             in this method affects the query, please refer to the `operations`
             method, which should always be called after the `compare()` method.

        :param src_data: The source data to compare with the baseline data.
            - This data should be the representation of the final form of
              the table data. In other words, it should be what the table
              look like after the query execution.
            - Data will be cleaned automatically based on the table's column
              schema before comparison and only columns that belongs to the
              table will be kept.
            - Accepts: `<DataFrame>`, `<list[dict]>`, `<tuple[dict]>`.

        :param bsl_data: The baseline data to compare to.
            - This data should be the present state of the table data. In
              other words, this data should be what the table currently
              look like before the query execution.
            - Data will be cleaned automatically based on the table's column
              schema before comparison and only columns that belongs to the
              table will be kept.
            - Accepts: `<DataFrame>`, `<list[dict]>`, `<tuple[dict]>`.

        :param unique_columns: `<list[str/Column]>` The columns to determine the uniqueness of the data. Defaults to `None`.
            - The 'unique_columns' sets a column combination as an unique
              identifier for each row of the data.
            - If not provided (`None`), defaults to the the columns of the `primary`
              UNIQUE INDEX from the table if applicable, else `QueryValueError` will
              be raised.

        :param compare_columns: `<list[str/Column]>` The columns to determine the differences in data values. Defaults to `None`.
            - The 'compare_columns' sets which columns should be used to
              identify the differences in value between the source and the
              baseline data.
            - If not provided (`None`), defaults to all shared columns between
              the source and the baseline data, columns that belongs to the table
              while excluding the 'unique_columns'.

        ### Notice
        Comparision cannot be based on the PARIMARY KEY of the table, and for data
        containing the PRIMARY KEY, the PRIMARY KEY column will be automatically
        excluded at data cleaning process.

        ### Example (find differences and similarities)
        >>> src = DataFrame(...)
            # . the source data (table final form)
        >>> bsl = await db.user.select().where(...).execute()
            # . select the baseline data (table current state)
            results = (
                await db.user.compare(src, bsl,
                    unique_columns=["user_name"],
                    compare_columns=["user_age", "user_status"])
                .operations(
                    find_insert=True,
                    find_update=True,
                    find_delete=True,
                    find_common=True,
                    find_identical=True)
                .results()
            )
            # . returns the comparison results
            print(results)

        ### Example (modify table)
        >>> src = DataFrame(...)
            # . the source data (table final form)
        >>> async with db.transaction() as conn:
                bsl = await db.user.select().where(...).for_update().execute(conn)
                # . select & lock the baseline data (table current state)
                (
                    await db.user.compare(src, bsl,
                        unique_columns=["user_name"],
                        compare_columns=["user_age", "user_status"])
                    .operations(
                        find_insert=True,
                        find_update=True,
                        find_delete=True)
                    .execute(conn)
                )
                # . modify the table to match the source data
        """
        return CompareQuery(self).compare(
            src_data,
            bsl_data,
            unique_columns=unique_columns,
            compare_columns=compare_columns,
        )

    # Validate ------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _validate_charset_collate(self, charset: str, collate: str) -> Charset:
        """(cfunc) Validate charset & collate combination.
        :raise `ValueError`: Invalid charset or collate.
        """
        # Validate charset
        if not is_str(charset):
            raise ValueError(f"Invalid charset: {repr(charset)} {type(charset)}")
        charset = charset.lower()

        # Validate collate
        if not is_str(collate):
            raise ValueError(f"Invalid collate: {repr(charset)} {type(collate)}")
        collate = collate.lower()

        # Validate combination
        try:
            return charset_by_name_and_collate(charset, collate)
        except KeyError as err:
            raise ValueError(
                "Unsupported MySQL charset {} & collate {} "
                "combination.".format(repr(charset), repr(collate))
            ) from err

    @cython.cfunc
    @cython.inline(True)
    def _validate_cursor(self, cursor: object):
        "(cfunc) Validate cursor `<Cursor>` (internal use only)."
        if not issubclass(cursor, Cursor):
            raise errors.QueryValueError(
                "<{}> Parameter 'cursor' only accepts subclass of `<Cursor>`, "
                "instead of: {} {}".format(self._name, cursor, repr(cursor))
            )

    # Escape data ---------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _escape_item(self, item: object) -> str:
        "(cfunc) Escape item to Literal string `<str>`."
        return transcode.encode_item(item, self._server._backslash_escapes)

    def escape_item(self, item: Any) -> str:
        "Escape item to Literal string `<str>`."
        return self._escape_item(item)

    @cython.cfunc
    @cython.inline(True)
    def _escape_args(self, args: object) -> object:
        """(cfunc) Escape arguments to literal `<tuple/dict>`.

        - If the given 'args' is type of `<dict>`, returns `<dict>`.
        - All other supported data types returns `<tuple>`.
        """
        return transcode.encode_args(args, self._server._backslash_escapes)

    def escape_args(self, args: Union[list, tuple]) -> Union[tuple, dict]:
        """Escape arguments to literal `<tuple/dict>`.

        - If the given 'args' is type of `<dict>`, returns `<dict>`.
        - All other supported data types returns `<tuple>`.
        """
        return self._escape_args(args)

    # Filter data ---------------------------------------------------------
    @overload
    def filter_columns(self, data: dict, *columns: str) -> dict:
        """Filter Table data by columns.

        :param data: The data of the Table to filter. Support data types:
            - `<dict>`: a single row of table data.
            - `<list[dict]>`: multiple rows of table data.
            - `<tuple[dict]>`: multiple rows of table data.
            - `<DataFrame>`: a pandas DataFrame of table data.

        :param columns: `<str>` Columns to keep in the data.
            - If not provided, all columns in the data that
              belongs to the table will be kept.
            - If provided, only the specified columns that
              also belongs to the table will be kept.

        :raise: `QueryDataError`
        :return `<dict>`: Table data filtered by columns.
        """

    @overload
    def filter_columns(self, data: list[dict], *columns: str) -> list[dict]:
        """Filter Table data by columns.

        :param data: The data of the Table to filter. Support data types:
            - `<dict>`: a single row of table data.
            - `<list[dict]>`: multiple rows of table data.
            - `<tuple[dict]>`: multiple rows of table data.
            - `<DataFrame>`: a pandas DataFrame of table data.

        :param columns: `<str>` Columns to keep in the data.
            - If not provided, all columns in the data that
              belongs to the table will be kept.
            - If provided, only the specified columns that
              also belongs to the table will be kept.

        :raise: `QueryDataError`
        :return `<list[dict]>`: Table data filtered by columns.
        """

    @overload
    def filter_columns(self, data: tuple[dict], *columns: str) -> tuple[dict]:
        """Filter Table data by columns.

        :param data: The data of the Table to filter. Support data types:
            - `<dict>`: a single row of table data.
            - `<list[dict]>`: multiple rows of table data.
            - `<tuple[dict]>`: multiple rows of table data.
            - `<DataFrame>`: a pandas DataFrame of table data.

        :param columns: `<str>` Columns to keep in the data.
            - If not provided, all columns in the data that
              belongs to the table will be kept.
            - If provided, only the specified columns that
              also belongs to the table will be kept.

        :raise: `QueryDataError`
        :return `<tuple[dict]>`: Table data filtered by columns.
        """

    @overload
    def filter_columns(self, data: DataFrame, *columns: str) -> DataFrame:
        """Filter Table data by columns.

        :param data: The data of the Table to filter. Support data types:
            - `<dict>`: a single row of table data.
            - `<list[dict]>`: multiple rows of table data.
            - `<tuple[dict]>`: multiple rows of table data.
            - `<DataFrame>`: a pandas DataFrame of table data.

        :param columns: `<str>` Columns to keep in the data.
            - If not provided, all columns in the data that
              belongs to the table will be kept.
            - If provided, only the specified columns that
              also belongs to the table will be kept.

        :raise: `QueryDataError`
        :return `<DataFrame>`: Table data filtered by columns (original data type).
        """

    def filter_columns(
        self,
        data: Union[dict, list[dict], tuple[dict], DataFrame],
        *columns: str,
    ) -> Union[dict, list[dict], tuple[dict], DataFrame]:
        """Filter Table data by columns.

        :param data: The data of the Table to filter. Support data types:
            - `<dict>`: a single row of table data.
            - `<list[dict]>`: multiple rows of table data.
            - `<tuple[dict]>`: multiple rows of table data.
            - `<DataFrame>`: a pandas DataFrame of table data.

        :param columns: `<str>` Columns to keep in the data.
            - If not provided, all columns in the data that
              belongs to the table will be kept.
            - If provided, only the specified columns that
              also belongs to the table will be kept.

        :raise: `QueryDataError`
        :return: Table data filtered by columns (original data type).
        """
        return self._filter_columns(data, columns)

    @cython.cfunc
    @cython.inline(True)
    def _filter_columns(self, data: object, columns: tuple[str]) -> object:
        """(cfunc) Filter Table data by columns.

        :param data: The data of the Table to filter. Support data types:
            - `<dict>`: a single row of table data.
            - `<list[dict]>`: multiple rows of table data.
            - `<tuple[dict]>`: multiple rows of table data.
            - `<DataFrame>`: a pandas DataFrame of table data.

        :param columns: `<tuple[str]>` Columns to keep in the data.
            - If not provided, all columns in the data that
              belongs to the table will be kept.
            - If provided, only the specified columns that
              also belongs to the table will be kept.

        :raise: `QueryDataError`
        :return: Table data filtered by columns (original data type).
        """
        # Filter & sort columns
        if not columns:
            columns = self._columns._names
        else:
            columns = tuple(self._columns._filter(columns))

        # Filter data
        if is_tuple(data):
            return self._filter_columns_tuple(data, columns)
        elif is_list(data):
            return self._filter_columns_list(data, columns)
        elif is_dict(data):
            return self._filter_columns_dict(data, columns)
        elif isinstance(data, DataFrame):
            return self._filter_columns_df(data, columns)
        else:
            raise errors.QueryDataError(
                "<{}.filter_columns> Only supports table data of the following formats:"
                "`dict`, `list[dict]`, `tuple[dict]` and `DataFrame`, instead of: "
                "{}".format(self._fname, type(data))
            )

    @cython.cfunc
    @cython.inline(True)
    def _filter_columns_dict(self, data: dict, columns: tuple[str]) -> dict:
        "(cfunc) Filter data row (dict) by columns `<dict>` (Internal use only)."
        # Empty data row
        if not data:
            return data

        # Filter columns
        try:
            res: dict = {}
            for col in columns:
                if dict_contains(data, col):
                    dict_setitem(res, col, data[col])
            return res
        except Exception as err:
            raise errors.QueryDataError(
                "<{}.filter_columns> Failed to filter data "
                "columns: {}".format(self._fname, err)
            ) from err

    @cython.cfunc
    @cython.inline(True)
    def _filter_columns_tuple(self, data: tuple[dict], columns: tuple[str]) -> tuple:
        "(cfunc) Filter data (tuple[dict]) by columns `<tuple[dict]>` (Internal use only)."
        # Empty data
        if not data:
            return data

        # Filter columns
        try:
            res: list = []
            for row in data:
                list_append(res, self._filter_columns_dict(row, columns))
            return tuple(res)
        except errors.QueryDataError:
            raise
        except Exception as err:
            raise errors.QueryDataError(
                "<{}.filter_columns> Failed to filter data "
                "columns: {}".format(self._fname, err)
            ) from err

    @cython.cfunc
    @cython.inline(True)
    def _filter_columns_list(self, data: list[dict], columns: tuple[str]) -> list:
        "(cfunc) Filter data (list[dict]) by columns `<list[dict]>` (Internal use only)."
        # Empty data
        if not data:
            return data

        # Filter columns
        try:
            res: list = []
            for row in data:
                list_append(res, self._filter_columns_dict(row, columns))
            return res
        except errors.QueryDataError:
            raise
        except Exception as err:
            raise errors.QueryDataError(
                "<{}.filter_columns> Failed to filter data "
                "columns: {}".format(self._fname, err)
            ) from err

    @cython.cfunc
    @cython.inline(True)
    def _filter_columns_df(self, data: DataFrame, columns: tuple[str]) -> object:
        "(cfunc) Filter data (DataFrame) by columns `<DataFrame>` (Internal use only)."
        # Empty data
        if data.columns.empty:
            return data

        # Filter columns
        try:
            cols_set: set = set(data.columns)
            columns_: list = []
            for col in columns:
                if set_contains(cols_set, col):
                    list_append(columns_, col)
            return data[columns_]
        except Exception as err:
            raise errors.QueryDataError(
                "<{}.filter_columns> Failed to filter data "
                "columns: {}".format(self._fname, err)
            ) from err

    # Clean data ----------------------------------------------------------
    def clean_data(
        self,
        data: Union[dict, list[dict], tuple[dict], DataFrame],
        *columns: str,
    ) -> list[dict]:
        """Clean Table data by columns.

        :param data: The data of the Table to clean. Support data types:
            - `<dict>`: a single row of table data.
            - `<list[dict]>`: multiple rows of table data.
            - `<tuple[dict]>`: multiple rows of table data.
            - `<DataFrame>`: a pandas DataFrame of table data.

        :param columns: `<str>` Columns to clean in the data.
            - If not provided, all columns in the data that
              belongs to the table will be kept and cleaned.
            - If provided, only the specified columns that also
              belongs to the table will be kept and cleaned.

        :raise: `QueryDataError`
        :return `<list[dict]>`: Table data cleaned by columns.
        """
        return self._clean_data(data, columns)

    @cython.cfunc
    @cython.inline(True)
    def _clean_data(self, data: object, columns: tuple[str]) -> list:
        """(cfunc) Clean Table data by columns.

        :param data: The data of the Table to clean. Support data types:
            - `<dict>`: a single row of table data.
            - `<list[dict]>`: multiple rows of table data.
            - `<tuple[dict]>`: multiple rows of table data.
            - `<DataFrame>`: a pandas DataFrame of table data.

        :param columns: `<tuple[str]>` Columns to clean in the data.
            - If not provided, all columns in the data that
              belongs to the table will be kept and cleaned.
            - If provided, only the specified columns that also
              belongs to the table will be kept and cleaned.

        :raise: `QueryDataError`
        :return `<list[dict]>`: Table data cleaned by columns.
        """
        # Filter & sort columns
        if not columns:
            columns = self._columns._names
        else:
            columns = tuple(self._columns._filter(columns))

        # Clean data
        if is_tuple(data):
            return self._clean_data_tuple(data, columns)
        elif is_list(data):
            return self._clean_data_list(data, columns)
        elif is_dict(data):
            return [self._clean_data_dict(data, columns)]
        elif isinstance(data, DataFrame):
            return self._clean_data_df(data, columns)
        else:
            raise errors.QueryDataError(
                "<{}.clean_data> Only supports table data of the following formats:"
                "`dict`, `list[dict]`, `tuple[dict]` and `DataFrame`, instead of: "
                "{}".format(self._fname, type(data))
            )

    @cython.cfunc
    @cython.inline(True)
    def _clean_data_dict(self, data: dict, columns: tuple[str]) -> dict:
        "(cfunc) Clean data row (dict) by columns `<dict>` (Internal use only)."
        # Empty data row
        if not data:
            return {}

        # Clean data
        try:
            res: dict = {}
            for col in columns:
                if dict_contains(data, col):
                    dict_setitem(
                        res, col, self._columns._item_validators[col](data[col])
                    )
            return res
        except Exception as err:
            raise errors.QueryDataError(
                "<{}.clean_data> Failed to clean data: {}".format(self._fname, err)
            ) from err

    @cython.cfunc
    @cython.inline(True)
    def _clean_data_tuple(self, data: tuple[dict], columns: tuple[str]) -> list:
        "(cfunc) Clean data (tuple[dict]) by columns `<list[dict]>` (Internal use only)."
        # Empty data
        if not data:
            return []

        # Clean data
        try:
            res: list = []
            for row in data:
                list_append(res, self._clean_data_dict(row, columns))
            return res
        except errors.QueryDataError:
            raise
        except Exception as err:
            raise errors.QueryDataError(
                "<{}.clean_data> Failed to clean data: {}".format(self._fname, err)
            ) from err

    @cython.cfunc
    @cython.inline(True)
    def _clean_data_list(self, data: list[dict], columns: tuple[str]) -> list:
        "(cfunc) Clean data (list[dict]) by columns `<list[dict]>` (Internal use only)."
        # Empty data
        if not data:
            return []

        # Clean data
        try:
            res: list = []
            for row in data:
                list_append(res, self._clean_data_dict(row, columns))
            return res
        except errors.QueryDataError:
            raise
        except Exception as err:
            raise errors.QueryDataError(
                "<{}.clean_data> Failed to clean data: {}".format(self._fname, err)
            ) from err

    @cython.cfunc
    @cython.inline(True)
    def _clean_data_df(self, data: DataFrame, columns: tuple[str]) -> list:
        "(cfunc) Clean data (DataFrame) by columns `<list[dict]>` (Internal use only)."
        # Empty data
        if data.columns.empty:
            return []

        # Clean data
        try:
            cols_set: set = set(data.columns)
            columns_: list = []
            for col in columns:
                if set_contains(cols_set, col):
                    list_append(columns_, col)
            if columns_:
                return (
                    data[columns_]
                    .apply(lambda col: self._columns._series_validators[col.name](col))
                    .to_dict("records")
                )
            else:
                return []
        except Exception as err:
            raise errors.QueryDataError(
                "<{}.clean_data> Failed to clean data: {}".format(self._fname, err)
            ) from err

    # Utils ---------------------------------------------------------------
    def hash_md5(self, obj: Any) -> str:
        """MD5 hash an object.

        :param obj: `<Any>` Object can be stringified.
        :raises ValueError: If failed to md5 hash the object.
        :return <'str'>: The md5 hashed value in string.
        """
        return utils._hash_md5(obj)

    def hash_sha256(self, obj: Any) -> str:
        """SHA256 hash an object.

        :param obj: `<Any>` Object can be stringified.
        :raises ValueError: If failed to sha256 hash the object.
        :return <'str'>: The sha256 hashed value in string.
        """
        return utils._hash_sha256(obj)

    def chunk_list(
        self,
        lst: list,
        size: Union[int, None] = None,
        chunks: Union[int, None] = None,
    ) -> list[list]:
        """Chunk a list into sub-lists.

        :param lst: `<list>` List to be chunked.
        :param size: `<int>` Desired size of each chunk. Defaults to `None`.
            - If 'size' is specified (`size > 0`), 'chunks' is ignored.
            - All chunks will have the desired 'size' except potentially the last one.

        :param chunks: `<int>` Desired number of chunks. Defaults to `None`.
            - Only applicable when `size=None` or `size <= 0`.
            - Chunks will be as evenly sized as possible.

        :raises ValueError: If both 'size' and 'chunks' are not specified.
        :return <list[list]>: List of chunked sub-lists.
        """
        return utils._chunk_list(lst, size or -1, chunks or -1)

    def chunk_df(
        self,
        df: DataFrame,
        size: Union[int, None] = None,
        chunks: Union[int, None] = None,
    ) -> list[DataFrame]:
        """Chunk a DataFrame into sub-DataFrames.

        :param df: `<DataFrame>` DataFrame to be chunked.
        :param size: `<int>` Desired size of each chunk. Defaults to `None`.
            - If 'size' is specified (`size > 0`), 'chunks' is ignored.
            - All chunks will have the desired 'size' except potentially the last one.

        :param chunks: `<int>` Desired number of chunks. Defaults to `None`.
            - Only applicable when `size=None` or `size <= 0`.
            - Chunks will be as evenly sized as possible.

        :raises ValueError: If both 'size' and 'chunks' are not specified.
        :return <list[DataFrame]>: List of chunked sub-DataFrames.
        """
        return utils._chunk_df(df, size or -1, chunks or -1)

    def concat_df_columns(self, df: DataFrame) -> Series:
        """Concatenate DataFrame values to one single columns `<Series[str]>`.

        ### Notice
        The new column `<Series>` should only be used for row comparison,
        since the values will be escaped to `<str>` for concatenation.
        """
        return Series(df.values.tolist(), index=df.index).apply(self._escape_item)

    def gen_dt_now(self) -> datetime.datetime:
        "Generate datetime based on the current local time `<datetime.datetime>`."
        return cydt.gen_dt_now()

    def gen_dt_utcnow(self) -> datetime.datetime:
        "Generate datetime based on the UTC time (timezone-aware) `<datetime.datetime>`."
        return cydt.gen_dt_utcnow()

    def gen_time_now(self) -> datetime.time:
        "Generate time based on the current local time `<datetime.time>."
        return cydt.gen_time_now()

    def gen_time_utcnow(self) -> datetime.time:
        "Generate time based on the UTC time (timezone-aware) `<datetime.time>."
        return cydt.gen_time_utcnow()

    # Export & Import -----------------------------------------------------
    async def export_data(self, dir: str) -> int:
        """Export table data to a local directory.

        ### Notice
        - This method is designed to work alongside `import_data()` method,
          and exports data in the parquet format.
        - It does not serve as a replacement for dedicated backup tools like
          mysqldump, as it omits the MySQL schema information during export.

        :param dir: `<str>` The root directory to store the export data.
            - This should be the root export directory, not a directory
              for specific databases or tables.
            - For example, `dir = 'desktop/mysql_export'`.

        :raises `TableExportError`: If the export operation fails.
        :return `<int>` : The number of tables successfully exported.
        """
        return await self._export_data(self._name, dir)

    async def _export_data(self, name: str, dir: str) -> int:
        """(Base `Table` method, internal use only) Export table data to a local directory.

        ### Notice
        - This method is designed to work alongside `import_data()` method,
          and exports data in the parquet format.
        - It does not serve as a replacement for dedicated backup tools like
          mysqldump, as it omits the MySQL schema information during export.

        :param name: `<str>` Name of the table.
        :param dir: `<str>` The root directory to store the export data.
            - This should be the root export directory, not a directory
              for specific databases or tables.
            - For example, `dir = 'desktop/mysql_export'`.

        :raises `TableExportError`: If the export operation fails.
        :return `<int>` : The number of tables successfully exported.
        """
        # Message
        fname = "%s.%s" % (self._db._name, name)
        msg1 = "Exporting data for table: '%s'..." % fname
        msg2 = "Bypass data export for table: '%s'. <Table is empty>." % fname
        msg3 = "Finish data export for table: '%s'" % fname

        # Validate directory
        dir = os.path.join(dir, self._db._name)
        os.makedirs(dir, exist_ok=True)
        filename = name + ".parquet"
        dst = os.path.join(dir, filename)

        # Start export
        print(msg1.ljust(100), end="\r")
        try:
            data = await query_exc_handler()(self.fetch_query)(
                "SELECT * FROM %s;" % fname,
                cursor=SSDfCursor,
                resolve_absent_table=False,
                timeout=3600,
                warnings=False,
            )
        except Exception as err:
            raise errors.TableExportError(
                "<{}> Failed to export data from table '{}': {}".format(
                    self.__class__.__name__, fname, err
                )
            )

        # Skip empty table
        if data.empty:
            print(msg2.ljust(100))
            return 0

        # Save table data
        try:
            data.to_parquet(dst)
        except Exception as err:
            raise errors.TableExportError(
                "<{}> Failed to save export data from table '{}': {}".format(
                    self.__class__.__name__, fname, err
                )
            ) from err

        # Finish export
        print(msg3.ljust(100))
        return 1

    async def import_data(self, dir: str, truncate: bool = True) -> int:
        """Import table data from a local directory.

        ### Notice
        - This method is designed to work alongside `export_data()` method,
          and imports data from saved parquet file of the table.
        - It does not serve as a replacement for dedicated backup tools like
          mysqldump, as it omits the MySQL schema information.

        :param dir: `<str>` The root directory where the exported data is stored.
            - This should be the root export data directory, not a directory
              for specific databases or tables.
            - For example, `dir = 'desktop/mysql_export'`.

        :param truncate `<bool>`: Specifies how to handle the table before import.
            - If the table does not exists in the Database, this argument has no
              effects and table will be created prior to data import.
            - If `True`, the existing table will be truncated before data import.
            - If `False`, the existing table will remain intact and the data will
              be imported with the 'IGNORE' keyword.

        :raises `TableImportError`: If the import operation fails.
        :return `<int>` : The number of tables successfully imported.
        """
        return await self._import_data(self._name, dir, truncate)

    async def _import_data(self, name: str, dir: str, truncate: cython.bint) -> int:
        """(Base method, internal use only) Import table data from a local directory.

        ### Notice
        - This method is designed to work alongside `export_data()` method,
          and imports data from saved parquet file of the table.
        - It does not serve as a replacement for dedicated backup tools like
          mysqldump, as it omits the MySQL schema information.

        :param name: `<str>` Name of the table.
        :param dir: `<str>` The root directory where the exported data is stored.
            - This should be the root export data directory, not a directory
              for specific databases or tables.
            - For example, `dir = 'desktop/mysql_export'`.

        :param truncate `<bool>`: Specifies how to handle the table before import.
            - If the table does not exists in the Database, this argument has no
              effects and table will be created prior to data import.
            - If `True`, the existing table will be truncated before data import.
            - If `False`, the existing table will remain intact and the data will
              be imported with the 'IGNORE' keyword.

        :raises `TableImportError`: If the import operation fails.
        :return `<int>` : The number of tables successfully imported.
        """
        # Message
        fname = "%s.%s" % (self._db._name, name)
        msg1 = "Importing data for table: '%s'..." % fname
        msg2 = "Bypass data import for table: '%s'. <Table data non-exists>." % fname
        msg3 = "Bypass data import for table: '%s'. <Table data invalid>." % fname
        msg4 = "Finish data import for table: '%s'" % fname

        # Validate source
        filename = name + ".parquet"
        src = os.path.join(dir, self._db._name, filename)
        if not os.path.exists(src):
            print(msg2.ljust(100))
            return 0

        # Load data
        try:
            data = read_parquet(src)
        except Exception as err:
            raise errors.TableImportError(
                "<{}> Failed to load export data for table '{}': {}".format(
                    self.__class__.__name__, fname, err
                )
            ) from err

        # Validate & clean data
        if data.empty:
            print(msg3.ljust(100))
            return 0
        columns: tuple = tuple(data.columns)
        if columns != self._columns._names:
            print(msg3.ljust(100))
            return 0
        try:
            data = self._clean_data_df(data, columns)
        except Exception as err:
            raise errors.TableImportError(
                "<{}> Failed to clean export data for table '{}': {}".format(
                    self.__class__.__name__, fname, err
                )
            ) from err

        # Construct stmt & args
        stmt = "INSERT IGNORE INTO %s (\n\t%s\n) VALUES (%s);" % (
            fname,
            ",\n\t".join(columns),
            ", ".join(["%s"] * len(columns)),
        )
        args = tuple(dic.values() for dic in data)

        # Prepare table
        if not await self._exists(name):
            await self._create(name)
        elif truncate:
            await self._truncate(name)

        # Import data
        print(msg1.ljust(100), end="\r")
        try:
            await query_exc_handler()(self.execute_query)(
                stmt,
                args=args,
                cursor=Cursor,
                resolve_absent_table=False,
                timeout=3600,
                warnings=False,
            )
        except Exception as err:
            raise errors.TableImportError(
                "<{}> Failed to import data for table '{}': {}".format(
                    self.__class__.__name__, fname, err
                )
            ) from err

        # Finish import
        print(msg4.ljust(100))
        return 1

    # Accessors -----------------------------------------------------------
    def keys(self) -> tuple[str]:
        "Access Table's column names `<tuple[str]>`."
        return self._columns._names

    def values(self) -> tuple[Column]:
        "Access Table's column instances `<tuple[Column]>`."
        return self._columns._instances

    def items(self) -> tuple[tuple[str, Column]]:
        "Access Table's column names and instances `<tuple[tuple[str, Column]]>`."
        return self._columns._items

    def get(self, col: Union[str, Column], default: Any = None) -> Union[Column, Any]:
        "Get Table's column `<Column>`. Return `default` if column does not exist."
        return self._columns._get(col, default)

    # Speical Methods -----------------------------------------------------
    def __repr__(self) -> str:
        return "<%s (name='%s', database='%s')>" % (
            self._type,
            self._name,
            self._db._name,
        )

    def __str__(self) -> str:
        return self._fname

    def __hash__(self) -> int:
        return hash((self._server, self.__repr__()))

    def __eq__(self, __o: object) -> bool:
        return hash(self) == hash(__o) if isinstance(__o, type(self)) else False

    def __len__(self) -> int:
        return self._columns._length

    def __iter__(self) -> Iterator[Column]:
        return self._columns.__iter__()

    def __getitem__(self, col: object) -> Column:
        return self._columns._getitem(col)

    def __contains__(self, col: object) -> bool:
        return self._columns._contains(col)


@cython.cclass
class TimeTable(Table):
    """The base class for a TimeTable in the Database.

    Inherit this class to create a custom TimeTable for the Database.
    All arguments besides 'database' of the  `__init__` method should
    be pre-defined in the subclass.

    TimeTable is a special class of Table, designed to manage time-series
    data. It must have at least one column of type `DATE`, `DATETIME`, or
    `TIMESTAMP`, with `tabletime=True`. This column organizes data into
    sub-tables within the Database, allowing TimeTable to act as a proxy
    to these sub-tables while maintaining an similar interface as a standard
    Table.

    Configure table's metadata by overwriting the `metadata()` method:
    - Use `self.columns_metadata()` method to add columns to the table.
    - Use `self.indexes_metadata()` method to add indexes to the table.
    """

    _time_unit: str
    _time_format: str
    _name_format: str

    def __init__(
        self,
        database: Database,
        name: str,
        time_unit: Literal["year", "Y", "month", "M", "week", "W", "day", "D"],
        charset: Union[str, None] = None,
        collate: Union[str, None] = None,
        engine: Literal["InnoDB", "MyISAM"] = "InnoDB",
    ) -> None:
        """The base class for a TimeTable in the Database.

        Inherit this class to create a custom TimeTable for the Database.
        All arguments besides 'database' of `__init__` method should be
        pre-defined in the subclass.

        TimeTable is a special class of Table, designed to manage time-series
        data. It must have at least one column of type `DATE`, `DATETIME`, or
        `TIMESTAMP`, with `tabletime=True`. This column organizes data into
        sub-tables within the Database, allowing TimeTable to act as a proxy
        to these sub-tables while maintaining an similar interface as a standard
        Table.

        :param database: `<Database>` The Database that hosts the table.
        :param name: `<str>` The name of the table (auto convert to lower case).
        :param time_unit: `<str>` The time unit for the table.
            - Data within the same time unit will be organized into one sub-table.
            - Each sub-table will be named as: `'<table_name>_<time_unit_suffix>'`.

        :param charset: `<str>` The charset of the table. Defaults to `None` (Database.charset).
        :param collate: `<str>` The collate of the table. Defaults to `None` (Database.collate).
        :param engine: `<str>` The engine of the table. Defaults to `'InnoDB'`.

        ### Configuration:
        - Overwrite `TimeTable.metadata()` to define table's columns and indexes.
        - Use `self.columns_metadata()` to define columns of the table.
        - Use `self.indexes_metadata()` to define indexes of the table.

        ### Example:
        >>> class MyTimeTable(Table):
                def __init__(self, database: MyDatabase) -> None:
                    # . pre-define table name & time_unit (monthly sub-table)
                    # . leave charset, collate & engine to default.
                    super().__init__(database, "my_timetable", "month")

        >>>     def metadata(self) -> None:
                    # . define columns (same as a standard table)
                    self.columns_metadata(
                        Column("id", MysqlTypes.BIGINT(primary_key=True)),
                        Column("username", MysqlTypes.VARCHAR()),
                        Column("user_level", MysqlTypes.TINYINT()),
                        Column("user_type", MysqlTypes.VARCHAR()),
                        ...
                        Column("create_time", MysqlTypes.DATETIME(tabletime=True)),
                        # . the column with `tabletime=True` will be
                        # . used to organize table data by time
                    )
                    # . define indexes (same as a standard table)
                    self.indexes_metadata(
                        Index(self.columns["username"], unique=True, primary_unique=True),
                        Index(self.columns["user_level"], self.columns["user_type"]),
                        ...
                        Index(self.columns["create_time"]),
                        # . it is recommended to add an index to the
                        # . tabletime column to speed up queries
                    )
        """
        super().__init__(database, name, charset, collate, engine)
        self.__setup(time_unit)

    # Properties ----------------------------------------------------------
    @property
    def time_unit(self) -> str:
        """The time unit of the timetable `<str>`.

        All table data within the same time unit will be organized into
        one sub-table. Possible values: `'year'`, `'month'`, `'week'` and `'day'`.
        """
        return self._time_unit

    @property
    def time_format(self) -> str:
        """The datetime format for the sub-tables `<str>`.

        Possible values: `'%Y'`, `'%Y-%m'` and `'%Y-%m-%d'`.
        """
        return self._time_format

    @property
    def name_format(self) -> str:
        """The datetime format of the naming suffix for the sub-tables `<str>`.

        Possible values: `'%Y'`, `'%Y%m'` and `'%Y%m%d'`.
        """
        return self._name_format

    @property
    def regex(self) -> TableRegex:
        "Regular expressions for the TimeTable `<TableRegex>`."
        return self._regex

    # Setup ---------------------------------------------------------------
    def __setup(self, time_unit: str) -> None:
        # Validate time_unit
        try:
            self._time_unit = settings.SUPPORT_TIME_UNITS.get(time_unit.lower())
            if self._time_unit is None:
                raise ValueError("Invalid time_unit: {}.".format(repr(time_unit)))
        except Exception as err:
            raise errors.TableMetadataError(
                "<{}.tables_metadata> Invalid time_unit {} for "
                "'TimeTable'.".format(self, repr(time_unit))
            ) from err

        # Setup format
        if self._time_unit == "year":
            time_format: str = "%Y"
            name_format: str = "%Y"
        elif self._time_unit == "month":
            time_format: str = "%Y-%m"
            name_format: str = "%Y%m"
        else:
            time_format: str = "%Y-%m-%d"
            name_format: str = "%Y%m%d"
        self._time_format = time_format
        self._name_format = name_format

        # Setup regex
        self._regex = TableRegex(self._db._name, self._name, self._time_unit)

    # Parse ---------------------------------------------------------------
    def parse_time(
        self,
        time: Union[str, datetime.date, datetime.datetime, None],
    ) -> datetime.datetime:
        "Parse sub-table time `<datetime>`."
        return self._parse_time(time)

    @cython.cfunc
    @cython.inline(True)
    def _parse_time(self, time: object) -> datetime.datetime:
        "(cfunc) Parse sub-table time `<datetime>`."
        try:
            if self._time_unit == "week":
                return pydt(time)._monday()._dt
            else:
                return pydt(time)._dt
        except Exception as err:
            raise errors.QueryValueError(
                "<{}> Unable to parse sub-table time from: "
                "{}".format(self._fname, repr(time))
            ) from err

    @cython.cfunc
    @cython.inline(True)
    def _parse_time_from_subname(self, subname: str) -> datetime.datetime:
        "(cfunc) Parse time from sub-table name ('user_202312') `<datetime>`."
        try:
            res = self._regex._time_gp._re_dpin.search(subname)
            if res is None:
                raise ValueError("Invalid sub-table name: {}.".format(repr(subname)))

            gp: tuple = res.groups()
            if self._time_unit == "year":
                return gen_dt(int(gp[0]), 1, 1, 0, 0, 0, 0, None, 0)
            elif self._time_unit == "month":
                return gen_dt(int(gp[0]), int(gp[1]), 1, 0, 0, 0, 0, None, 0)
            else:
                return gen_dt(int(gp[0]), int(gp[1]), int(gp[2]), 0, 0, 0, 0, None, 0)
        except Exception as err:
            raise errors.QueryValueError(
                "<{}> Unable to parse time from sub-table name: "
                "{}".format(self._fname, repr(subname))
            ) from err

    # Naming --------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _gen_name_from_time(self, time: datetime.datetime) -> str:
        "(cfunc) Generate sub-table name from dt `<str>`."
        sfix: str = time.strftime(self._name_format)
        return self._name_pfix + sfix

    @cython.cfunc
    @cython.inline(True)
    def _gen_fname_from_time(self, time: datetime.datetime) -> str:
        "(cfunc) Generate sub-table full name from dt `<str>`."
        sfix: str = time.strftime(self._name_format)
        return self._fname_pfix + sfix

    async def _get_one_name(self) -> str:
        "(Base method, internal use only) Get one sub-table name `<str/None>`."
        fetch: tuple
        async with self._server.acquire() as conn:
            async with conn.cursor(Cursor, False) as cur:
                await cur.execute(
                    "SELECT table_name AS i FROM information_schema.tables\n"
                    "WHERE table_schema = '%s' AND table_name REGEXP '%s' LIMIT 1;"
                    % (self._db._name, self._regex._name._pattern_dpin),
                )
                fetch = await cur.fetchone()

        # No sub-tables
        return fetch[0] if fetch else None

    def get_name(
        self,
        time: Union[str, datetime.date, datetime.datetime, None],
        with_dt: bool = False,
    ) -> Union[str, tuple[str, datetime.datetime]]:
        """(cfunc) Get the name of the sub-table. e.g. `'sales_20120312'`

        :param time: The specific time for the sub-table. If `None`, defaults to local datetime.
        :param with_dt: Whether to return datetime along with sub-table name. Defaults to `False`.
            - If `False`, returns the name of the sub-table.
            - If `True`, returns a tuple of `(name, datetime)`.

        :return `<str/tuple>`: The name (datetime) of the sub-table.
        """
        return self._get_name_with_dt(time) if with_dt else self._get_name(time)

    @cython.cfunc
    @cython.inline(True)
    def _get_name(self, time: object) -> str:
        dt = self._parse_time(time)
        return self._gen_name_from_time(dt)

    @cython.cfunc
    @cython.inline(True)
    def _get_name_with_dt(self, time: object) -> tuple:
        dt = self._parse_time(time)
        name = self._gen_name_from_time(dt)
        return name, dt

    def get_fname(
        self,
        time: Union[str, datetime.date, datetime.datetime, None],
        with_dt: bool = False,
    ) -> Union[str, tuple[str, datetime.datetime]]:
        """Get the full name of the sub-table. e.g. `'mydatabase.sales_20120312'`

        :param time: The specific time for the sub-table. If `None`, defaults to local datetime.
        :param with_dt: Whether to return datetime along with sub-table full name. Defaults to `False`.
            - If `False`, returns the full name of the sub-table.
            - If `True`, returns a tuple of `(full_name, datetime)`.

        :return `<str/tuple>`: The full name (datetime) of the sub-table.
        """
        return self._get_fname_with_dt(time) if with_dt else self._get_fname(time)

    @cython.cfunc
    @cython.inline(True)
    def _get_fname(self, time: object) -> str:
        dt = self._parse_time(time)
        return self._gen_fname_from_time(dt)

    @cython.cfunc
    @cython.inline(True)
    def _get_fname_with_dt(self, time: object) -> tuple:
        dt = self._parse_time(time)
        fname = self._gen_fname_from_time(dt)
        return fname, dt

    async def get_names(
        self,
        *times: Union[str, datetime.date, datetime.datetime, None],
        start: Union[str, datetime.date, datetime.datetime, None] = None,
        end: Union[str, datetime.date, datetime.datetime, None] = None,
        days: Union[int, None] = None,
        with_dt: bool = False,
        invert: bool = False,
        filter: bool = False,
    ) -> Union[list[str], dict[str, datetime.datetime], dict[datetime.datetime, str]]:
        """Generate the names of sub-tables based on the given times or time range.
        e.g. ['sales_20120312', 'sales_20120313', ...]

        :param times: The times of the sub-tables.
            If not provided, will use 'start', 'end', and 'days' arguments to determine
            the time span and generate the corresponding sub-table names.

        :param start, end, days: The time span parameters.
            - If 'start' and 'end' are specified -> sub-tables between [start, ... end].
            - If 'start' and 'days' are specified -> sub-tables between [start, ... start + days - 1]
            - If only 'start' is specified -> sub-tables between [start, ... datetime.now()] or [datetime.now(), ... start]
            - If 'end' and 'days' are specified -> sub-tables between [end - days + 1, ... end]
            - If only 'end' is specified -> sub-tables between [datetime.now(), ... end] or [end, ... datetime.now()]
            - If only 'days' is specified -> sub-tables between [datetime.now() - days + 1, ... datetime.now()]
            - If none of the params are given -> empty `list` or `dict` (depends on 'with_dt')

        :param with_dt: `<bool>` Whether to return datetimes along with sub-table names. Defaults to `False`.
            - If `False`, returns a list of the sub-table names.
            - If `True`, returns a dictionary of `{name: datetime}`.

        :param invert: `<bool>` Whether to invert the key (name) and val (time). Defaults to `False`.
            - Only applicable when `with_dt=True`.
            - If `False`, returns a dictionary of `{name: datetime}`.
            - If `True`, returns a dictionary of `{datetime: name}`.

        :param filter: `<bool>` Whether to filter out non-existent sub-table names. Defaults to `False`.
            - If `True`, a query will be executed to fetch all existing sub-tables
              in the database, and only matching table names will be returned.
            - If `False`, returns all sub-table names generated based on the given
              parameters.

        :raise: Subclass of `QueryError`.
        :return `<list/dict>`: The names (datetimes) of the sub-tables.
        """
        # Whether to filter out non-existent sub-table names
        names: set = await self.sync_sub_tables() if filter else set()

        # Generate names
        if with_dt:
            return self._get_names_with_dt(
                list(times), start, end, days or -1, filter, names, invert
            )
        else:
            return self._get_names(list(times), start, end, days or -1, filter, names)

    @cython.cfunc
    @cython.inline(True)
    def _get_names(
        self,
        times: list[Union[str, datetime.date, datetime.datetime, None]],
        start: Union[str, datetime.date, datetime.datetime, None],
        end: Union[str, datetime.date, datetime.datetime, None],
        days: cython.int,
        filter: cython.bint,
        exist_tables: set[str],
    ) -> list:
        # Times not specified - generate time span
        if not times:
            times = utils._gen_time_span(start, end, days, self._time_unit, False)
            if times is None:  # . invalid arguments - exist
                return []

        # With table name filter
        res: list = []
        seen: set = set()
        if filter:
            for time in times:
                name = self._get_name(time)
                if set_contains(exist_tables, name) and not set_contains(seen, name):
                    list_append(res, name)
                    set_add(seen, name)
        # Without name filter
        else:
            for time in times:
                name = self._get_name(time)
                if not set_contains(seen, name):
                    list_append(res, name)
                    set_add(seen, name)
        # Return sub-table names
        return res

    @cython.cfunc
    @cython.inline(True)
    def _get_names_with_dt(
        self,
        times: list[Union[str, datetime.date, datetime.datetime, None]],
        start: Union[str, datetime.date, datetime.datetime, None],
        end: Union[str, datetime.date, datetime.datetime, None],
        days: cython.int,
        filter: cython.bint,
        exist_tables: set[str],
        invert: cython.bint,
    ) -> dict:
        # Times not specified - generate time span
        if not times:
            times = utils._gen_time_span(start, end, days, self._time_unit, False)
            if times is None:  # . invalid arguments - exist
                return {}

        # With table name filter
        res: dict = {}
        seen: set = set()
        if filter:
            if invert:  # . invert key (name) & val (time)
                for time in times:
                    dt = self._parse_time(time)
                    name = self._gen_name_from_time(dt)
                    if set_contains(exist_tables, name) and not set_contains(
                        seen, name
                    ):
                        dict_setitem(res, dt, name)
                        set_add(seen, name)
            else:  # default key (name) & val (time)
                for time in times:
                    dt = self._parse_time(time)
                    name = self._gen_name_from_time(dt)
                    if set_contains(exist_tables, name) and not set_contains(
                        seen, name
                    ):
                        dict_setitem(res, name, dt)
                        set_add(seen, name)
        # Without name filter
        else:
            if invert:  # . invert key (name) & val (time)
                for time in times:
                    dt = self._parse_time(time)
                    name = self._gen_name_from_time(dt)
                    if not set_contains(seen, name):
                        dict_setitem(res, dt, name)
                        set_add(seen, name)
            else:  # default key (name) & val (time)
                for time in times:
                    dt = self._parse_time(time)
                    name = self._gen_name_from_time(dt)
                    if not set_contains(seen, name):
                        dict_setitem(res, name, dt)
                        set_add(seen, name)
        # Return sub-table names & datetimes
        return res

    async def get_fnames(
        self,
        *times: Union[str, datetime.date, datetime.datetime, None],
        start: Union[str, datetime.date, datetime.datetime, None] = None,
        end: Union[str, datetime.date, datetime.datetime, None] = None,
        days: Union[int, None] = None,
        with_dt: bool = False,
        invert: bool = False,
        filter: bool = False,
    ) -> Union[list[str], dict[str, datetime.datetime], dict[datetime.datetime, str]]:
        """Generate the full names of sub-tables based on the given times or time range.
        e.g. ['mydatabase.sales_20120312', 'mydatabase.sales_20120313', ...]

        :param times: The times of the sub-tables.
            If not provided, will use 'start', 'end', and 'days' arguments to determine
            the time span and generate the corresponding sub-table full names.

        :param start, end, days: The time span parameters.
            - If 'start' and 'end' are specified -> sub-tables between [start, ... end].
            - If 'start' and 'days' are specified -> sub-tables between [start, ... start + days - 1]
            - If only 'start' is specified -> sub-tables between [start, ... datetime.now()] or [datetime.now(), ... start]
            - If 'end' and 'days' are specified -> sub-tables between [end - days + 1, ... end]
            - If only 'end' is specified -> sub-tables between [datetime.now(), ... end] or [end, ... datetime.now()]
            - If only 'days' is specified -> sub-tables between [datetime.now() - days + 1, ... datetime.now()]
            - If none of the params are given -> empty `list` or `dict` (depends on 'with_dt')

        :param with_dt: `<bool>` Whether to return datetimes along with sub-table full names. Defaults to `False`.
            - If `False`, returns a list of the sub-table full names.
            - If `True`, returns a dictionary of `{full_name: datetime}`.

        :param invert: `<bool>` Whether to invert the key (name) and val (time). Defaults to `False`.
            - Only applicable when `with_dt=True`.
            - If `False`, returns a dictionary of `{full_name: datetime}`.
            - If `True`, returns a dictionary of `{datetime: full_name}`.

        :param filter: `<bool>` Whether to filter out non-existent sub-table full names. Defaults to `False`.
            - If `True`, a query will be executed to fetch all existing sub-tables
              in the database, and only matching table full names will be returned.
            - If `False`, returns all sub-table full names generated based on the
              given parameters.

        :raise: Subclass of `QueryError`.
        :return `<list/dict>`: The full names (datetimes) of the sub-tables.
        """
        # Whether to filter out non-existent sub-table names
        names: set = await self.sync_sub_tables() if filter else set()

        # Generate names
        if with_dt:
            return self._get_fnames_with_dt(
                list(times), start, end, days, filter, names, invert
            )
        else:
            return self._get_fnames(list(times), start, end, days, filter, names)

    @cython.cfunc
    @cython.inline(True)
    def _get_fnames(
        self,
        times: list[Union[str, datetime.date, datetime.datetime, None]],
        start: Union[str, datetime.date, datetime.datetime, None],
        end: Union[str, datetime.date, datetime.datetime, None],
        days: cython.int,
        filter: cython.bint,
        exist_tables: set[str],
    ) -> list:
        # Times not specified - generate time span
        if not times:
            times = utils._gen_time_span(start, end, days, self._time_unit, False)
            if times is None:  # . invalid arguments - exist
                return []

        # With table name filter
        res: list = []
        seen: set = set()
        if filter:
            for time in times:
                name = self._get_name(time)
                if set_contains(exist_tables, name) and not set_contains(seen, name):
                    list_append(res, self._db_pfix + name)
                    set_add(seen, name)
        # Without name filter
        else:
            for time in times:
                name = self._get_name(time)
                if not set_contains(seen, name):
                    list_append(res, self._db_pfix + name)
                    set_add(seen, name)
        # Return sub-table names
        return res

    @cython.cfunc
    @cython.inline(True)
    def _get_fnames_with_dt(
        self,
        times: list[Union[str, datetime.date, datetime.datetime, None]],
        start: Union[str, datetime.date, datetime.datetime, None],
        end: Union[str, datetime.date, datetime.datetime, None],
        days: cython.int,
        filter: cython.bint,
        exist_tables: set[str],
        invert: cython.bint,
    ) -> dict:
        # Times not specified - generate time span
        if not times:
            times = utils._gen_time_span(start, end, days, self._time_unit, False)
            if times is None:  # . invalid arguments - exist
                return {}

        # With table name filter
        res: dict = {}
        seen: set = set()
        if filter:
            if invert:  # . invert key (name) & val (time)
                for time in times:
                    dt = self._parse_time(time)
                    name = self._gen_name_from_time(dt)
                    if set_contains(exist_tables, name) and not set_contains(
                        seen, name
                    ):
                        dict_setitem(res, dt, self._db_pfix + name)
                        set_add(seen, name)
            else:  # default key (name) & val (time)
                for time in times:
                    dt = self._parse_time(time)
                    name = self._gen_name_from_time(dt)
                    if set_contains(exist_tables, name) and not set_contains(
                        seen, name
                    ):
                        dict_setitem(res, self._db_pfix + name, dt)
                        set_add(seen, name)
        # Without name filter
        else:
            if invert:  # . invert key (name) & val (time)
                for time in times:
                    dt = self._parse_time(time)
                    name = self._gen_name_from_time(dt)
                    if not set_contains(seen, name):
                        dict_setitem(res, dt, self._db_pfix + name)
                        set_add(seen, name)
            else:  # default key (name) & val (time)
                for time in times:
                    dt = self._parse_time(time)
                    name = self._gen_name_from_time(dt)
                    if not set_contains(seen, name):
                        dict_setitem(res, self._db_pfix + name, dt)
                        set_add(seen, name)
        # Return sub-table names & datetimes
        return res

    async def all_names(
        self,
        with_dt: bool = False,
        invert: bool = False,
    ) -> Union[list[str], dict[str, datetime.datetime], dict[datetime.datetime, str]]:
        """Get all sub-tables names. e.g. ['sales_20120312', 'sales_20120313', ...]

        :param with_dt: `<bool>` Whether to return datetimes along with sub-table names. Defaults to `False`.
            - If `False`, returns a list of the sub-table names.
            - If `True`, returns a dictionary of `{name: datetime}`.

        :param invert: `<bool>` Whether to invert the key (name) and val (time). Defaults to `False`.
            - Only applicable when `with_dt=True`.
            - If `False`, returns a dictionary of `{name: datetime}`.
            - If `True`, returns a dictionary of `{datetime: name}`.

        :raise: Subclass of `QueryError`.
        :return `<list/dict>`: The names (datetimes) of all the sub-tables.
        """
        # Generate names
        names: list = sorted(await self.sync_sub_tables())
        return self._all_names_with_dt(names, invert) if with_dt else names

    @cython.cfunc
    @cython.inline(True)
    def _all_names_with_dt(self, names: list[str], invert: cython.bint) -> dict:
        # Invert key (name) & val (time)
        res: dict = {}
        name: str
        if invert:
            for name in names:
                dt = self._parse_time_from_subname(name)
                dict_setitem(res, dt, name)
        # Default key (name) & val (time)
        else:
            for name in names:
                dt = self._parse_time_from_subname(name)
                dict_setitem(res, name, dt)
        # Return names & times
        return res

    async def all_fnames(
        self,
        with_dt: bool = False,
        invert: bool = False,
    ) -> Union[list[str], dict[str, datetime.datetime], dict[datetime.datetime, str]]:
        """Get all sub-tables full names.
        e.g. ['mydatabase.sales_20120312', 'mydatabase.sales_20120313', ...]

        :param with_dt: `<bool>` Whether to return datetimes along with sub-table full names. Defaults to `False`.
            - If `False`, returns a list of the sub-table full names.
            - If `True`, returns a dictionary of `{full_name: datetime}`.

        :param invert: `<bool>` Whether to invert the key (name) and val (time). Defaults to `False`.
            - Only applicable when `with_dt=True`.
            - If `False`, returns a dictionary of `{full_name: datetime}`.
            - If `True`, returns a dictionary of `{datetime: full_name}`.

        :raise: Subclass of `QueryError`.
        :return `<list/dict>`: The full names (datetimes) of all the sub-tables.
        """
        # Get all sub-table names
        names: list = sorted(await self.sync_sub_tables())

        # Without datetime
        if not with_dt:
            res: list = []
            name: str
            for name in names:
                list_append(res, self._db_pfix + name)
            return res
        # With datetime
        else:
            return self._all_fnames_with_dt(names, invert)

    @cython.cfunc
    @cython.inline(True)
    def _all_fnames_with_dt(self, names: list[str], invert: cython.bint) -> dict:
        # Invert key (name) & val (time)
        res: dict = {}
        name: str
        if invert:
            for name in names:
                dt = self._parse_time_from_subname(name)
                dict_setitem(res, dt, self._db_pfix + name)
        # Default key (name) & val (time)
        else:
            for name in names:
                dt = self._parse_time_from_subname(name)
                dict_setitem(res, self._db_pfix + name, dt)
        # Return names & times
        return res

    @query_exc_handler()
    async def sync_sub_tables(self) -> set[str]:
        """Synchronize initiated sub-tables.

        In order to optimize the performance of the built-in query methods,
        `TimeTable` automatically caches initiated sub-tables. This method
        synchronize the cache with all the sub-tables that currently exist in
        datatbase. *In most cases, manual calling this method is not required.

        :return `set[str]`: All sub-table names that currenlty exist in the database.
        """
        # Select from schema
        fetch: tuple
        async with self._server.acquire() as conn:
            async with conn.cursor(Cursor, False) as cur:
                await cur.execute(
                    "SELECT table_name AS i FROM information_schema.tables\n"
                    "WHERE table_schema = '%s' AND table_name REGEXP '%s';"
                    % (self._db._name, self._regex._name._pattern_dpin),
                )
                fetch = await cur.fetchall()

        # Get names
        res: set = set()
        row: tuple
        for row in fetch:
            set_add(res, row[0])

        # Set cache
        self._initiated_tables = res.copy()
        return res

    def get_names_series(self, times: Series) -> Series[str]:
        """Get the sub-table names from the give pandas.Series `<Series[str]>`"""
        return self._get_names_series(times)

    @cython.cfunc
    @cython.inline(True)
    def _get_names_series(self, times: Series) -> object:
        """Get the sub-table names from the give pandas.Series `<Series[str]>`"""
        try:
            if self._time_unit == "week":
                times = pddt(times).monday.dt
            else:
                times = pddt(times).dt
        except Exception:
            return times.apply(self._get_name)

        return self._name_pfix + times.dt.strftime(self._name_format)

    def get_fnames_series(self, times: Series) -> Series[str]:
        """Get the sub-table full names from the give pandas.Series `<Series[str]>`"""
        return self._get_fnames_series(times)

    @cython.cfunc
    @cython.inline(True)
    def _get_fnames_series(self, times: Series) -> object:
        """Get the sub-table full names from the give pandas.Series `<Series[str]>`"""
        try:
            if self._time_unit == "week":
                times = pddt(times).monday.dt
            else:
                times = pddt(times).dt
        except Exception:
            return times.apply(self._get_fname)

        return self._fname_pfix + times.dt.strftime(self._name_format)

    # Initiate ------------------------------------------------------------
    @query_exc_handler()
    async def initiate(
        self,
        time: Union[str, datetime.date, datetime.datetime, None] = None,
    ) -> bool:
        """Initiate a sub-table of the TimeTable.

        :param time: Time of the sub-table. Defaults to `None` (local datetime).
        :return `bool`: `<bool>` Whether the sub-table is initiated.
        """
        # Synchronize table information
        if not self._initiated:
            names: set = await self.sync_sub_tables()
            if names:
                await self._sync_information(names.pop())
            self._initiated = True

        # Create sub-table if not exists
        name: str = self._get_name(time)
        if not await self._exists(name):
            await self._create(name)

        # Return status
        return True

    # Core SQL ------------------------------------------------------------
    async def lock(
        self,
        conn: Connection,
        time: Union[str, datetime.date, datetime.datetime, None] = None,
        write: bool = True,
    ) -> Connection:
        """`LOCK` the sub-table.

        :param conn: `<Connection>` The connection to issue the lock.
        :param time: Time of the sub-table. Defaults to `None` (local datetime).
        :param write: `<bool>` Whether to lock for write. Defaults to `True`
        :raise: Subclass of `QueryError`.
        :return <`Connection`>: The connection that locked the table.
        """
        return await self._lock(self._get_name(time), conn, write)

    # Basic SQL -----------------------------------------------------------
    async def create(
        self,
        time: Union[str, datetime.date, datetime.datetime, None] = None,
    ) -> bool:
        """`CREATE` a sub-table `IF NOT EXSITS`.

        :param time: Time of the sub-table. Defaults to `None` (local datetime).
        :raise: Subclass of `QueryError`.
        :return <`bool`>: Whether the sub-table has been created.
        """
        return await self._create(self._get_name(time))

    async def exists(
        self,
        time: Union[str, datetime.date, datetime.datetime, None],
    ) -> bool:
        """Check if a sub-table exists.

        :param time: Time of the sub-table. If `None`, defaults to local datetime.
        :raise: Subclass of `QueryError`.
        :return <`bool`>: Whether the sub-table exists.
        """
        return await self._exists(self._get_name(time))

    async def drop(
        self,
        *times: Union[str, datetime.date, datetime.datetime, None],
        start: Union[str, datetime.date, datetime.datetime, None] = None,
        end: Union[str, datetime.date, datetime.datetime, None] = None,
        days: Union[int, None] = None,
    ) -> bool:
        """`DROP` sub-tables `IF EXISTS`.

        ### Notice
        If no time-related arguments are provided, `NO` sub-tables will be dropped.
        Time-related arguments: 'times', 'start', 'end', and 'days'.

        :param times: The times of the sub-tables to drop.
            If not provided, will use 'start', 'end', and 'days' arguments to
            determine the time span and the corresponding sub-tables to drop.

        :param start, end, days: The time span parameters.
            - If 'start' and 'end' are specified -> sub-tables between [start, ... end].
            - If 'start' and 'days' are specified -> sub-tables between [start, ... start + days - 1]
            - If only 'start' is specified -> sub-tables between [start, ... datetime.now()] or [datetime.now(), ... start]
            - If 'end' and 'days' are specified -> sub-tables between [end - days + 1, ... end]
            - If only 'end' is specified -> sub-tables between [datetime.now(), ... end] or [end, ... datetime.now()]
            - If only 'days' is specified -> sub-tables between [datetime.now() - days + 1, ... datetime.now()]
            - If none of the params are given -> no sub-tables will be affected

        :raise: Subclass of `QueryError`.
        :return `<bool>`: Whether sub-tables have been dropped.
        """
        # Get sub-table names
        names: list = await self.get_names(
            *times, start=start, end=end, days=days, with_dt=False, filter=True
        )
        if not names:
            return True

        # Execute drop query
        fnames: list = []
        names_: list = []
        name: str
        for name in names:
            list_append(fnames, self._db_pfix + name)
            list_append(names_, name)
        syntax: str = ", ".join(fnames)
        async with self._server.acquire() as conn:
            async with conn.cursor(Cursor, False) as cur:
                await cur.execute("DROP TABLE IF EXISTS %s;" % syntax)

        # Remove from initiated tables
        self._rem_init_tables(names_)
        return True

    async def empty(
        self,
        *times: Union[str, datetime.date, datetime.datetime, None],
        start: Union[str, datetime.date, datetime.datetime, None] = None,
        end: Union[str, datetime.date, datetime.datetime, None] = None,
        days: Union[int, None] = None,
    ) -> bool:
        """Check if sub-tables are empty.

        ### Notice
        If no time-related arguments are provided, returns `True` directly.
        Time-related arguments: 'times', 'start', 'end', and 'days'.

        :param times: The times of the sub-tables to check.
            If not provided, will use 'start', 'end', and 'days' arguments to
            determine the time span and the corresponding sub-tables to check.

        :param start, end, days: The time span parameters.
            - If 'start' and 'end' are specified -> sub-tables between [start, ... end].
            - If 'start' and 'days' are specified -> sub-tables between [start, ... start + days - 1]
            - If only 'start' is specified -> sub-tables between [start, ... datetime.now()] or [datetime.now(), ... start]
            - If 'end' and 'days' are specified -> sub-tables between [end - days + 1, ... end]
            - If only 'end' is specified -> sub-tables between [datetime.now(), ... end] or [end, ... datetime.now()]
            - If only 'days' is specified -> sub-tables between [datetime.now() - days + 1, ... datetime.now()]
            - If none of the params are given -> no tables will be checked

        :raise: Subclass of `QueryError`.
        :return `<bool>`: `True` if all sub-tables are empty, else `False`.
        """
        # Get sub-table names
        names: list = await self.get_names(
            *times, start=start, end=end, days=days, with_dt=False, filter=True
        )
        if not names:
            return True

        # Select from database schema
        fetch: tuple
        async with self._server.acquire() as conn:
            async with conn.cursor(Cursor, False) as cur:
                await cur.execute(
                    "SELECT SUM(table_rows) AS i FROM information_schema.tables\n"
                    "WHERE table_schema = '%s' AND table_name IN %s;"
                    % (self._db._name, self._escape_item(names))
                )
                fetch = await cur.fetchone()

        # Check emptyness
        return False if fetch and fetch[0] > 0 else True

    async def truncate(
        self,
        *times: Union[str, datetime.date, datetime.datetime, None],
        start: Union[str, datetime.date, datetime.datetime, None] = None,
        end: Union[str, datetime.date, datetime.datetime, None] = None,
        days: Union[int, None] = None,
    ) -> bool:
        """`TRUNCATE` sub-tables asynchronously.

        ### Notice
        If no time-related arguments are provided, `NO` sub-tables will be dropped.
        Time-related arguments: 'times', 'start', 'end', and 'days'. Errors from
        individual sub-tables will `NOT STOP` the operation and are raised after
        all truncation attempts.

        :param times: The times of the sub-tables to truncate.
            If not provided, will use 'start', 'end', and 'days' arguments to
            determine the time span and the corresponding sub-tables to truncate.

        :param start, end, days: The time span parameters.
            - If 'start' and 'end' are specified -> sub-tables between [start, ... end].
            - If 'start' and 'days' are specified -> sub-tables between [start, ... start + days - 1]
            - If only 'start' is specified -> sub-tables between [start, ... datetime.now()] or [datetime.now(), ... start]
            - If 'end' and 'days' are specified -> sub-tables between [end - days + 1, ... end]
            - If only 'end' is specified -> sub-tables between [datetime.now(), ... end] or [end, ... datetime.now()]
            - If only 'days' is specified -> sub-tables between [datetime.now() - days + 1, ... datetime.now()]
            - If none of the params are given -> no tables will be affected

        :raise: Subclass of `QueryError`.
        :return `<bool>`: Whether sub-tables have been truncated.
        """
        # Get sub-table names
        names: list = await self.get_names(
            *times, start=start, end=end, days=days, with_dt=False, filter=True
        )
        if not names:
            return True

        # Execute truncate query
        res: list = await gather(
            *[self._truncate(name) for name in names],
            return_exceptions=True,
        )
        for i in res:
            if isinstance(i, Exception):
                raise i
        return True

    @overload
    async def information(
        self,
        *info: Literal[
            "*",
            "table_name",
            "table_catalog",
            "table_schema",
            "table_type",
            "engine",
            "version",
            "row_format",
            "table_rows",
            "avg_row_length",
            "data_length",
            "max_data_length",
            "index_length",
            "data_free",
            "auto_increment",
            "create_time",
            "update_time",
            "check_time",
            "table_collation",
            "checksum",
            "create_options",
            "table_comment",
        ],
        cursor: type[DictCursor | SSDictCursor] = DictCursor,
    ) -> tuple[dict[str, Any]]:
        """Select all sub-tables information from `INFORMATION_SCHEMA`.

        Available information options:
        - 'table_name', 'table_catalog', 'table_schema', 'table_type', 'engine', 'version',
        - 'row_format', 'table_rows', 'avg_row_length', 'data_length', 'max_data_length',
        - 'index_length', 'data_free', 'auto_increment', 'create_time', 'update_time',
        - 'check_time', 'table_collation', 'checksum', 'create_options', 'table_comment'

        :param info: `<str>` The information to be selected.
            - If not specified, defaults to `'table_name'`.
            - Use `'*'` to select all information.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :raise: Subclass of `QueryError`.
        :return `<tuple[dict]>`: All sub-tables information.
        """

    @overload
    async def information(
        self,
        *info: Literal[
            "*",
            "table_name",
            "table_catalog",
            "table_schema",
            "table_type",
            "engine",
            "version",
            "row_format",
            "table_rows",
            "avg_row_length",
            "data_length",
            "max_data_length",
            "index_length",
            "data_free",
            "auto_increment",
            "create_time",
            "update_time",
            "check_time",
            "table_collation",
            "checksum",
            "create_options",
            "table_comment",
        ],
        cursor: type[DfCursor | SSDfCursor] = DictCursor,
    ) -> DataFrame:
        """Select all sub-tables information from `INFORMATION_SCHEMA`.

        Available information options:
        - 'table_name', 'table_catalog', 'table_schema', 'table_type', 'engine', 'version',
        - 'row_format', 'table_rows', 'avg_row_length', 'data_length', 'max_data_length',
        - 'index_length', 'data_free', 'auto_increment', 'create_time', 'update_time',
        - 'check_time', 'table_collation', 'checksum', 'create_options', 'table_comment'

        :param info: `<str>` The information to be selected.
            - If not specified, defaults to `'table_name'`.
            - Use `'*'` to select all information.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :raise: Subclass of `QueryError`.
        :return `<DataFrame>`: All sub-tables information.
        """

    @overload
    async def information(
        self,
        *info: Literal[
            "*",
            "table_name",
            "table_catalog",
            "table_schema",
            "table_type",
            "engine",
            "version",
            "row_format",
            "table_rows",
            "avg_row_length",
            "data_length",
            "max_data_length",
            "index_length",
            "data_free",
            "auto_increment",
            "create_time",
            "update_time",
            "check_time",
            "table_collation",
            "checksum",
            "create_options",
            "table_comment",
        ],
        cursor: type[Cursor | SSCursor] = DictCursor,
    ) -> tuple[tuple[Any]]:
        """Select all sub-tables information from `INFORMATION_SCHEMA`.

        Available information options:
        - 'table_name', 'table_catalog', 'table_schema', 'table_type', 'engine', 'version',
        - 'row_format', 'table_rows', 'avg_row_length', 'data_length', 'max_data_length',
        - 'index_length', 'data_free', 'auto_increment', 'create_time', 'update_time',
        - 'check_time', 'table_collation', 'checksum', 'create_options', 'table_comment'

        :param info: `<str>` The information to be selected.
            - If not specified, defaults to `'table_name'`.
            - Use `'*'` to select all information.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :raise: Subclass of `QueryError`.
        :return `<tuple[tuple]>`: All sub-tables information.
        """

    async def information(
        self,
        *info: Literal[
            "*",
            "table_name",
            "table_catalog",
            "table_schema",
            "table_type",
            "engine",
            "version",
            "row_format",
            "table_rows",
            "avg_row_length",
            "data_length",
            "max_data_length",
            "index_length",
            "data_free",
            "auto_increment",
            "create_time",
            "update_time",
            "check_time",
            "table_collation",
            "checksum",
            "create_options",
            "table_comment",
        ],
        cursor: type[
            DictCursor | SSDictCursor | DfCursor | SSDfCursor | Cursor | SSCursor
        ] = DictCursor,
    ) -> Union[tuple[dict[str, Any] | tuple[Any]], DataFrame]:
        """Select all sub-tables information from `INFORMATION_SCHEMA`.

        Available information options:
        - 'table_name', 'table_catalog', 'table_schema', 'table_type', 'engine', 'version',
        - 'row_format', 'table_rows', 'avg_row_length', 'data_length', 'max_data_length',
        - 'index_length', 'data_free', 'auto_increment', 'create_time', 'update_time',
        - 'check_time', 'table_collation', 'checksum', 'create_options', 'table_comment'

        :param info: `<str>` The information to be selected.
            - If not specified, defaults to `'table_name'`.
            - Use `'*'` to select all information.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :raise: Subclass of `QueryError`.
        :return `<tuple/DataFrame>`: All sub-tables information (depends on 'cursor' type).
        """
        # Check info
        info_: str
        if not info:
            info_ = "table_name"
        elif "*" in info:
            info_ = "*"
        else:
            info_ = ", ".join(map(str, info))

        # Execute select query
        async with self._server.acquire() as conn:
            async with conn.cursor(cursor, False) as cur:
                await cur.execute(
                    "SELECT %s FROM information_schema.tables\n"
                    "WHERE table_schema = '%s' AND table_name REGEXP '%s';"
                    % (info_, self._db._name, self._regex._name._pattern_dpin)
                )
                return await cur.fetchall()

    @overload
    async def describe(
        self,
        cursor: type[DictCursor | SSDictCursor] = DictCursor,
    ) -> tuple[dict[str, Any]]:
        """`DESCRIBE` a sub-table.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :raise: Subclass of `QueryError`.
        :return `<tuple[dict]>`: Table description.
        """

    @overload
    async def describe(
        self,
        cursor: type[DfCursor | SSDfCursor] = DictCursor,
    ) -> DataFrame:
        """`DESCRIBE` a sub-table.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :raise: Subclass of `QueryError`.
        :return `<DataFrame>`: Table description.
        """

    @overload
    async def describe(
        self,
        cursor: type[Cursor | SSCursor] = DictCursor,
    ) -> tuple[tuple[Any]]:
        """`DESCRIBE` a sub-table.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :raise: Subclass of `QueryError`.
        :return `<tuple[tuple]>`: Table description.
        """

    async def describe(
        self,
        cursor: type[
            DictCursor | SSDictCursor | DfCursor | SSDfCursor | Cursor | SSCursor
        ] = DictCursor,
    ) -> Union[tuple[dict[str, Any] | tuple[Any]], DataFrame]:
        """`DESCRIBE` a sub-table.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :raise: Subclass of `QueryError`.
        :return `<tuple/DataFrame>`: Table description (depends on 'cursor' type).
        """
        # Get one sub-table name
        name = await self._get_one_name()

        # No sub-tables
        if name is None:
            self._validate_cursor(cursor)
            return cursor.empty_result
        # Execute describe query
        else:
            return await self._describe(name, cursor)

    async def optimize(
        self,
        *times: Union[str, datetime.date, datetime.datetime, None],
        start: Union[str, datetime.date, datetime.datetime, None] = None,
        end: Union[str, datetime.date, datetime.datetime, None] = None,
        days: Union[int, None] = None,
    ) -> int:
        """`OPTIMIZE` sub-tables synchronously.

        ### Notice
        If no time-related arguments are provided, `ALL` sub-tables will be optimized.
        Time-related arguments: 'times', 'start', 'end', and 'days'. Errors from
        individual sub-tables will `STOP` the operation and the error will be raised
        immediately.

        :param times: The times of the sub-tables to optimize.
            If not provided, will use 'start', 'end', and 'days' arguments to
            determine the time span and the corresponding sub-tables to optimize.

        :param start, end, days: The time span parameters.
            - If 'start' and 'end' are specified -> sub-tables between [start, ... end].
            - If 'start' and 'days' are specified -> sub-tables between [start, ... start + days - 1]
            - If only 'start' is specified -> sub-tables between [start, ... datetime.now()] or [datetime.now(), ... start]
            - If 'end' and 'days' are specified -> sub-tables between [end - days + 1, ... end]
            - If only 'end' is specified -> sub-tables between [datetime.now(), ... end] or [end, ... datetime.now()]
            - If only 'days' is specified -> sub-tables between [datetime.now() - days + 1, ... datetime.now()]
            - If none of the params are given -> all table will be optimzed

        :raise: Subclass of `QueryError`.
        :return `<int>`: Number of sub-tables that have been optimized.
        """
        # Get sub-table names
        names: list
        if any(i for i in [times, start, end, days]):
            names = await self.get_names(
                *times, start=start, end=end, days=days, with_dt=False, filter=True
            )
        else:
            names = list(await self.sync_sub_tables())

        # Execute optimize query
        count: cython.int = 0
        for name in names:
            count += await self._optimize(name)
        return count

    async def alter_charset_collate(
        self,
        charset: str = "utf8mb4",
        collate: str = "utf8mb4_0900_as_cs",
    ) -> bool:
        """`ALTER` the `CHARACTER SET` and `COLLATION` of
        all sub-tables asynchronously.

        ### Notice
        Errors from individual sub-tables will `NOT STOP` the operation
        and are raised after all alteration attempts.

        :param charset: `<str>` The charset to apply. Defaults to `'utf8mb4'`.
        :param collate: `<str>` The collate to apply. Defaults to `'utf8mb4_0900_as_cs'`.
        :raise: Subclass of `QueryError`.
        :return `<bool>`: Whether charset & collate have been altered.

        Note: After altering the charset and collate, please manually update the
        corresponding Table's settings to ensure consistency between the program
        and the actual MySQL database. A restart of the program is typically required.
        """
        # Get all sub-table names
        names: set = await self.sync_sub_tables()
        if not names:
            return False

        # Validate charset & collate
        try:
            charSet = self._validate_charset_collate(charset, collate)
        except ValueError as err:
            raise errors.QueryValueError(
                "<{}.alter_charset_collate> {}".format(self._fname, err)
            ) from err
        self._charset = charSet._name
        self._collate = charSet._collate

        # Execute alter charset & collate query
        res: list = await gather(
            *[
                self._alter_charset_collate(name, self._charset, self._collate)
                for name in names
            ],
            return_exceptions=True,
        )
        for i in res:
            if isinstance(i, Exception):
                raise i
        return True

    async def add_column(self, column: Column, after: Union[str, None] = None) -> bool:
        """`ADD` a `COLUMN` to all sub-tables asynchronously.

        ### Notice
        Errors from individual sub-tables will `NOT STOP` the operation
        and are raised after all alteration attempts.

        :param column: `<Column>` The new column to add to the sub-tables.
        :param after: `<str/None>` Name of the column the new column goes after. Defaults to `None`.
            - If `None`, the new column will be added at the end of the table.
            - If specified, the new column will be added after that column.

        :raise: Subclass of `QueryError`.
        :return `<bool>`: Whether the new column has been added.

        Note: After altering the column of the table, please manually update the
        corresponding Table's metadata to ensure consistency between the program
        and the actual MySQL database. A restart of the program is typically required.
        """
        # Get all sub-table names
        names: set = await self.sync_sub_tables()
        if not names:
            return False

        # Execute add column query
        res: list = await gather(
            *[self._add_column(name, column, after) for name in names],
            return_exceptions=True,
        )
        for i in res:
            if isinstance(i, Exception):
                raise i
        return True

    async def drop_column(self, column: Union[str, Column]) -> bool:
        """`DROP` a `Column` from all sub-tables asynchronously.

        ### Notice
        Errors from individual sub-tables will `NOT STOP` the operation
        and are raised after all alteration attempts.

        :param column: `<str/Column>` The column to drop from the sub-tables.
        :raise: Subclass of `QueryError`.
        :return `<bool>`: Whether the column has been dropped.

        Note: After altering the column of the table, please manually update the
        corresponding Table's metadata to ensure consistency between the program
        and the actual MySQL database. A restart of the program is typically required.
        """
        # Get all sub-table names
        names: set = await self.sync_sub_tables()
        if not names:
            return False

        # Execute drop column query
        res: list = await gather(
            *[self._drop_column(name, column) for name in names],
            return_exceptions=True,
        )
        for i in res:
            if isinstance(i, Exception):
                raise i
        return True

    async def add_index(self, index: Index) -> bool:
        """`ADD` an `Index` to all sub-tables asynchronously.

        ### Notice
        Errors from individual sub-tables will `NOT STOP` the operation
        and are raised after all alteration attempts.

        :param index `<Index>`: The new index to added to the sub-tables.
        :raise: Subclass of `QueryError`.
        :return `<bool>`: Whether the index has been added.

        Note: After altering the index of the table, please manually update the
        corresponding Table's metadata to ensure consistency between the program
        and the actual MySQL database. A restart of the program is typically required.
        """
        # Get all sub-table names
        names: set = await self.sync_sub_tables()
        if not names:
            return False

        # Execute add index query
        res: list = await gather(
            *[self._add_index(name, index) for name in names],
            return_exceptions=True,
        )
        for i in res:
            if isinstance(i, Exception):
                raise i
        return True

    async def drop_index(self, index: Union[str, Index]) -> bool:
        """`DROP` an `Index` from all sub-tables asynchronously.

        ### Notice
        Errors from individual sub-tables will `NOT STOP` the operation
        and are raised after all alteration attempts.

        :param index `<str/Index>`: The index to drop from the sub-tables.
        :raise: Subclass of `QueryError`.
        :return `<bool>`: Whether the index has been dropped.

        Note: After altering the index of the table, please manually update the
        corresponding Table's metadata to ensure consistency between the program
        and the actual MySQL database. A restart of the program is typically required.
        """
        # Get all sub-table names
        names: set = await self.sync_sub_tables()
        if not names:
            return False

        # Execute drop index query
        res: list = await gather(
            *[self._drop_index(name, index) for name in names],
            return_exceptions=True,
        )
        for i in res:
            if isinstance(i, Exception):
                raise i
        return True

    @overload
    async def show_index(
        self,
        cursor: type[DictCursor | SSDictCursor] = DictCursor,
    ) -> tuple[dict[str, Any]]:
        """`SHOW INDEX` of the TimeTable.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :raise: Subclass of `QueryError`.
        :return `<tuple[dict]>`: Index information.
        """

    @overload
    async def show_index(
        self,
        cursor: type[DfCursor | SSDfCursor] = DictCursor,
    ) -> DataFrame:
        """`SHOW INDEX` of the TimeTable.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :raise: Subclass of `QueryError`.
        :return `<DataFrame>`: Index information.
        """

    @overload
    async def show_index(
        self,
        cursor: type[Cursor | SSCursor] = DictCursor,
    ) -> tuple[tuple[Any]]:
        """`SHOW INDEX` of the TimeTable.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :raise: Subclass of `QueryError`.
        :return `<tuple[tuple]>`: Index information.
        """

    async def show_index(
        self,
        cursor: type[
            DictCursor | SSDictCursor | DfCursor | SSDfCursor | Cursor | SSCursor
        ] = DictCursor,
    ) -> Union[tuple[dict[str, Any] | tuple[Any]], DataFrame]:
        """`SHOW INDEX` of the TimeTable.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :raise: Subclass of `QueryError`.
        :return `<tuple/DataFrame>`: Index information (depends on 'cursor' type).
        """
        # Get one sub-table name
        name = await self._get_one_name()

        # No sub-tables
        if name is None:
            self._validate_cursor(cursor)
            return cursor.empty_result
        # Execute show index query
        else:
            return await self._show_index(name, cursor)

    async def reset_indexes(self) -> int:
        """`RESET` all indexes of the sub-tables asynchronously.

        This method is invoked when there's a change in the table index settings,
        ensuring the server's indexes align with the program's `TimeTable.indexes`
        settings. The process involves two steps:
        1. Drop all existing indexes of the sub-tables (excludes `PRIMARY` key).
        2. Recreate indexes based on the current `TimeTable.indexes` settings.

        :raise: Subclass of `QueryError`.
        :return `<int>`: The number of tables that indexes been reset.

        ### Notice
        Errors from individual sub-tables will `STOP` the operation and the error
        will be raised immediately.
        """
        # Get all sub-table names
        names: set = await self.sync_sub_tables()
        if not names:
            return 0

        # Reset sub-tables indexes
        res: list = await gather(*[self._reset_indexes(name) for name in names])
        return sum(res)

    # Export & Import -----------------------------------------------------
    async def export_data(self, dir: str) -> int:
        """Export sub-tables data to a local directory synchronously.

        ### Notice
        - This method is designed to work alongside `import_data()` method,
          and exports data in the parquet format.
        - It does not serve as a replacement for dedicated backup tools like
          mysqldump, as it omits the MySQL schema information during export.
        - Errors from individual sub-tables will `STOP` the operation and the
          error will be raised immediately.

        :param dir: `<str>` The root directory to store the export data.
            - This should be the root export directory, not a directory
              for specific databases or tables.
            - For example, `dir = 'desktop/mysql_export'`.

        :raises `TableExportError`: If the export operation fails.
        :return `<int>` : The number of sub-tables successfully exported.
        """
        # Get sub-table names
        names: set = await self.sync_sub_tables()
        if not names:
            return 0

        # Export sub-tables data
        count: cython.int = 0
        for name in names:
            count += await self._export_data(name, dir)
        return count

    async def import_data(self, dir: str, truncate: bool = True) -> int:
        """Import sub-tables data from a local directory synchronously.

        ### Notice
        - This method is designed to work alongside `export_data()` method,
          and imports data from saved parquet files of the sub-tables.
        - It does not serve as a replacement for dedicated backup tools like
          mysqldump, as it omits the MySQL schema information.
        - Errors from individual sub-tables will `STOP` the operation and the
          error will be raised immediately.

        :param dir: `<str>` The root directory where the exported data is stored.
            - This should be the root export data directory, not a directory
              for specific databases or tables.
            - For example, `dir = 'desktop/mysql_export'`.

        :param truncate `<bool>`: Specifies how to handle the sub-tables before import.
            - If the sub-table does not exists in the Database, this argument has no
              effects and sub-table will be created prior to data import.
            - If `True`, the existing sub-table will be truncated before data import.
            - If `False`, the existing sub-table will remain intact and the data will
              be imported with the 'IGNORE' keyword.

        :raises `TableImportError`: If the import operation fails.
        :return `<int>` : The number of sub-tables successfully imported.
        """
        count: cython.int = 0
        file: str
        for file in os.listdir(os.path.join(dir, self._db._name)):
            if not file.endswith(".parquet"):
                continue
            name = file.replace(".parquet", "")
            if self._regex._name._re_dpin.match(name):
                count += await self._import_data(name, dir, truncate)
        return count


# Tables =======================================================================================
@cython.cclass
class Tables:
    """Represents a collection of tables.

    Works as an immutable dictionary with static typings, where
    keys are the table names and values are the `Table` instances.
    Meanwhile, it provides search methods to find tables.
    """

    _db: Database
    _dict: dict[str, Table]
    _names: tuple[str]
    _names_set: set[str]
    _instances: tuple[Table]
    _items: tuple[tuple[str, Table]]
    _length: cython.int
    # Regex
    _regex_names: Regex
    _regex_fnames: Regex

    def __init__(self, database: Database, *tables: Table) -> None:
        """A collection of tables.

        Works as an immutable dictionary with static typings, where
        keys are the table names and values are the `Table` instances.
        Meanwhile, it provides search methods to find tables.

        :param database: `<Database>` The database that hosts the tables.
        :param tables: `<Table>` Table instances to add to the collection.
        """
        self._db = database
        self.__setup(tables)

    # Properties -----------------------------------------------------------
    @property
    def names(self) -> tuple[str]:
        "The names of the tables `<tuple[str]>`."
        return self._names

    @property
    def names_set(self) -> set[str]:
        "The names of the tables, access in `<set[str]>`."
        return self._names_set

    @property
    def instances(self) -> tuple[Table]:
        "The table instances `<tuple[Table]>`."
        return self._instances

    @property
    def regex_names(self) -> Regex:
        """The regex expressions for aggregated tables names (grouped) `<Regex>`.

        For exmaple, base pattern formated as: `'(<tb1>|<tb2>...<tbn>)'`
        """
        return self._regex_names

    @property
    def regex_fnames(self) -> Regex:
        """The regex expressions for aggregated tables full names (grouped) `<Regex>`.

        For exmaple, base pattern formated as: `'<db>.(<tb1>|<tb2>...<tbn>)'`
        """
        return self._regex_fnames

    # Setup ---------------------------------------------------------------
    def __setup(self, tables: tuple[Table]) -> None:
        # Validate tables
        _seen: set = set()
        _names: list = []
        _instances: list = []
        _dic: dict = {}
        for tb in tables:
            if not isinstance(tb, Table):
                raise errors.DatabaseMetadataError(
                    "<{}.metadata> Invalid table: {} {}. Database only "
                    "accepts instance of `<mysqlengine.Table>`.".format(
                        self.__class__.__name__, type(tb), repr(tb)
                    )
                )

            # . duplicates
            if tb in _seen:
                raise errors.TableError(
                    "<{}> Duplicated table: {}.".format(
                        self.__class__.__name__, repr(tb)
                    )
                )
            _seen.add(tb)
            _names.append(tb.name)
            _instances.append(tb)
            _dic[tb.name] = tb

        # Setup columns
        self._dict = _dic
        self._names = tuple(_names)
        self._names_set = set(_names)
        self._instances = tuple(_instances)
        self._items = tuple(dict_items(_dic))
        self._length = len(_names)
        del _seen

        # Setup regex
        self._setup_regex()

    def _setup_regex(self) -> None:
        # Aggregate patterns
        names_pattern: list = []
        tb: Table
        for tb in self._instances:
            list_append(names_pattern, tb._regex._name._pattern)
        names: str = "(" + "|".join(names_pattern) + ")"

        # Table names regex
        self._regex_names = Regex(names)

        # Table full names regex
        fnames: str = self._db._name + "." + names
        self._regex_fnames = Regex(fnames)

    # Search --------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _search_by_name(self, name: str, exact: cython.bint) -> list:
        """(cfunc) Search tables by name.

        :param name: `<str>` The name or partial name of the desired table(s).
        :param exact: `<bool>` Whether to perform exact match.
            - If `True`, return only the table with the exact provided 'name'.
            - If `False`, return all tables that contain the '(partial) name'.

        :return `<list[Table]>`: Matched table(s).
        """
        res: list = []
        tb: Table
        if exact:
            for tb in self._instances:
                if tb._name == name:
                    list_append(res, tb)
        else:
            name = name.lower()
            for tb in self._instances:
                if str_contains(tb._name, name):
                    list_append(res, tb)
        return res

    def search_by_name(self, name: str, exact: bool = False) -> list[Table]:
        """Search tables by name.

        :param name: `<str>` The name or partial name of the desired table(s).
        :param exact: `<bool>` Whether to perform exact match. Defaults to `False`.
            - If `True`, return only the table with the exact provided 'name'.
            - If `False`, return all tables that contain the '(partial) name'.

        :return `<list[Table]>`: Matched table(s).
        """
        return self._search_by_name(name, exact)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _search_by_charset(self, charset: str, exact: cython.bint) -> list:
        """(cfunc) Search tables by charset.

        :param charset: `<str>` The charset or partial charset of the desired table(s).
        :param exact: `<bool>` Whether to perform exact match.
            - If `True`, return only the tables with the exact provided 'charset'.
            - If `False`, return all tables that contain the '(partial) charset'.

        :return `<list[Table]>`: Matched table(s).
        """
        charset = charset.lower()
        res: list = []
        tb: Table
        if exact:
            for tb in self._instances:
                if tb._charset == charset:
                    list_append(res, tb)
        else:
            for tb in self._instances:
                if str_contains(tb._charset, charset):
                    list_append(res, tb)
        return res

    def search_by_charset(self, charset: str, exact: bool = False) -> list[Table]:
        """Search tables by charset.

        :param charset: `<str>` The charset or partial charset of the desired table(s).
        :param exact: `<bool>` Whether to perform exact match. Defaults to `False`.
            - If `True`, return only the tables with the exact provided 'charset'.
            - If `False`, return all tables that contain the '(partial) charset'.

        :return `<list[Table]>`: Matched table(s).
        """
        return self._search_by_charset(charset, exact)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _search_by_collate(self, collate: str, exact: cython.bint) -> list:
        """(cfunc) Search tables by collate.

        :param collate: `<str>` The collate or partial collate of the desired table(s).
        :param exact: `<bool>` Whether to perform exact match.
            - If `True`, return only the tables with the exact provided 'collate'.
            - If `False`, return all tables that contain the '(partial) collate'.

        :return `<list[Table]>`: Matched table(s).
        """
        collate = collate.lower()
        res: list = []
        tb: Table
        if exact:
            for tb in self._instances:
                if tb._collate == collate:
                    list_append(res, tb)
        else:
            for tb in self._instances:
                if str_contains(tb._collate, collate):
                    list_append(res, tb)
        return res

    def search_by_collate(self, collate: str, exact: bool = False) -> list[Table]:
        """Search tables by collate.

        :param collate: `<str>` The collate or partial collate of the desired table(s).
        :param exact: `<bool>` Whether to perform exact match. Defaults to `False`.
            - If `True`, return only the tables with the exact provided 'collate'.
            - If `False`, return all tables that contain the '(partial) collate'.

        :return `<list[Table]>`: Matched table(s).
        """
        return self._search_by_collate(collate, exact)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _search_by_columns(self, columns: tuple, match_all: cython.bint) -> list:
        """(cfunc) Search tables by columns.

        :param columns: `<tuple[str, Column]>` Column names or instances.
        :param match_all: `<bool>` Whether to match all columns.
            - If `True`, return tables that contains all of the given 'columns'.
            - If `False`, return tables if any of its columns matches any
              of the provided 'columns'.

        :return `<list[Table]>`: Matched table(s).
        """
        # Empty columns
        if not columns:
            return []

        # Get column names
        names: set = get_columns_names(columns)

        # Search tables
        res: list = []
        tb: Table
        if match_all:
            for tb in self._instances:
                if tb._columns._names_set >= names:
                    list_append(res, tb)
        else:
            for tb in self._instances:
                if tb._columns._names_set & names:
                    list_append(res, tb)
        return res

    def search_by_columns(
        self,
        *columns: Union[str, Column],
        match_all: bool = False,
    ) -> list[Table]:
        """Search tables by columns.

        :param columns: `<tuple[str, Column]>` Column names or instances.
        :param match_all: `<bool>` Whether to match all columns.
            - If `True`, return tables that contains all of the given 'columns'.
            - If `False`, return tables if any of its columns matches any
              of the provided 'columns'.

        :return `<list[Table]>`: Matched table(s).
        """
        return self._search_by_columns(columns, match_all)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _search_by_indexes(self, indexes: tuple, match_all: cython.bint) -> list:
        """(cfunc) Search tables by indexes.

        :param indexes: `<tuple[str, Index]>` Index names or instances.
        :param match_all: `<bool>` Whether to match all indexes.
            - If `True`, return tables that contains all of the given 'indexes'.
            - If `False`, return tables if any of its indexes matches any
              of the provided 'indexes'.

        :return `<list[Table]>`: Matched table(s).
        """
        # Empty indexes:
        if not indexes:
            return []

        # Get indexex names
        names: set = get_indexes_names(indexes)

        # Search tables
        res: list = []
        tb: Table
        if match_all:
            for tb in self._instances:
                if tb._indexes._names_set >= names:
                    list_append(res, tb)
        else:
            for tb in self._instances:
                if tb._indexes._names_set & names:
                    list_append(res, tb)
        return res

    def search_by_indexes(
        self,
        *indexes: Union[str, Index],
        match_all: bool = False,
    ) -> list[Table]:
        """(cfunc) Search tables by indexes.

        :param indexes: `<tuple[str, Index]>` Index names or instances.
        :param match_all: `<bool>` Whether to match all indexes. Defaults to `False`.
            - If `True`, return tables that contains all of the given 'indexes'.
            - If `False`, return tables if any of its indexes matches any
              of the provided 'indexes'.

        :return `<list[Table]>`: Matched table(s).
        """
        return self._search_by_indexes(indexes, match_all)

    # Filter --------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _issubset(self, tables: tuple) -> cython.bint:
        "(cfunc) Whether Table collection is a subset of the given tables `<bool>`."
        # Empty tables
        if self._length == 0 or not tables:
            return False

        # Get table names
        names: set = get_tables_names(tables)  # type: ignore

        # Subset comparison
        return True if self._names_set <= names else False

    def issubset(self, *tables: Union[str, Table]) -> bool:
        "Whether Table collection is a subset of the given tables `<bool>`."
        return self._issubset(tables)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _filter(self, tables: tuple) -> list:
        """(cfunc) Filter & sort the given tables by the Tables collection.
        Returns the table names that exist in the collection `<list[str]>`.
        """
        # Empty tables
        if self._length == 0 or not tables:
            return []

        # Get table names
        names: set = get_tables_names(tables)  # type: ignore

        # Filter & sort
        res: list = []
        for name in self._names:
            if set_contains(names, name):
                list_append(res, name)
        return res

    def filter(self, *tables: Union[str, Table]) -> list[str]:
        """Filter & sort the given tables by the Tables collection.
        Returns the table names that exist in the collection `<list[str]>`.
        """
        return self._filter(tables)

    # Accessors ------------------------------------------------------------
    def keys(self) -> tuple[str]:
        "Access the names of the tables `<tuple[str]>`."
        return self._names

    def values(self) -> tuple[Table]:
        "Access the table instances `<tuple[Table]>`."
        return self._instances

    def items(self) -> tuple[tuple[str, Table]]:
        "Access the table names and instances `<tuple[tuple[str, Table]]>`."
        return self._items

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _get(self, tb: object, default: object) -> object:
        "(cfunc) Get table `<Table/Any>`. Return `default` if table does not exist."
        res = dict_get(self._dict, get_table_name(tb))  # type: ignore
        if res == cython.NULL:
            return default
        else:
            return cython.cast(object, res)

    def get(self, tb: Union[str, Table], default: Any = None) -> Union[Table, Any]:
        "Get table `<Table>`. Return `default` if table does not exist."
        return self._get(tb, default)

    # Special Methods ------------------------------------------------------
    def __repr__(self) -> str:
        return "<%s %s>" % (self.__class__.__name__, self._dict.__repr__())

    def __str__(self) -> str:
        return self._dict.__str__()

    def __hash__(self) -> int:
        return self.__repr__().__hash__()

    def __eq__(self, __o: object) -> bool:
        return str(self) == str(__o) if isinstance(__o, Tables) else False

    def __bool__(self) -> bool:
        return self._length > 0

    def __len__(self) -> int:
        return self._length

    def __iter__(self) -> Iterator[Table]:
        return self._instances.__iter__()

    @cython.cfunc
    @cython.inline(True)
    def _getitem(self, tb: object) -> Table:
        """(cfunc) Get table `<Table>`. Equivalent to `Tables[tb]`.
        Raises `TableNotExistError` if table does not exist.
        """
        res = dict_get(self._dict, get_table_name(tb))  # type: ignore
        if res == cython.NULL:
            raise errors.TableNotExistError(
                "<%s> Does not contain table: %s" % (self.__class__.__name__, repr(tb))
            )
        return cython.cast(object, res)

    def __getitem__(self, tb: object) -> Table:
        return self._getitem(tb)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _contains(self, tb: object) -> cython.bint:
        "(cfunc) Whether contains the table `<bool>`. Equivalent to `tb in Tables`."
        return set_contains(self._names_set, get_table_name(tb))  # type: ignore

    def __contains__(self, tb: object) -> bool:
        return self._contains(tb)

    def __del__(self):
        self._dict = None
        self._names = None
        self._names_set = None
        self._instances = None
        self._items = None


@cython.cclass
class TimeTables(Tables):
    """Represents a collection of `TimeTables`.

    Works as an immutable dictionary with static typings, where
    keys are the table names and values are the `TimeTable` instances.
    Meanwhile, it provides search methods to find tables.
    """

    def __init__(self, database: Database, *tables: Table) -> None:
        """A collection of `TimeTables`.

        Works as an immutable dictionary with static typings, where
        keys are the table names and values are the `TimeTable` instances.
        Meanwhile, it provides search methods to find tables.

        :param database: `<Database>` The database that hosts the tables.
        :param tables: `<Table>` Table instances to add to the collection.
        """
        super().__init__(database, *tables)
        self.__setup()

    # Properties -----------------------------------------------------------
    @property
    def instances(self) -> tuple[TimeTable]:
        "The table instances `<tuple[TimeTable]>`."
        return self._instances

    # Setup ----------------------------------------------------------------
    def __setup(self) -> None:
        # Empty tables
        if self._length == 0:
            return None

        # Validate tables
        _timetbs: list = []
        for tb in self._instances:
            if tb.is_timetable:
                list_append(_timetbs, tb)

        # Setup tables
        timetbs = Tables(self._db, *_timetbs)
        self._dict = timetbs._dict
        self._names = timetbs._names
        self._names_set = timetbs._names_set
        self._instances = timetbs._instances
        self._items = timetbs._items
        self._length = timetbs._length
        del timetbs

    # Search --------------------------------------------------------------
    def search_by_name(self, name: str, exact: bool = False) -> list[TimeTable]:
        """Search tables by name.

        :param name: `<str>` The name or partial name of the desired table(s).
        :param exact: `<bool>` Whether to perform exact match. Defaults to `False`.
            - If `True`, return only the table with the exact provided 'name'.
            - If `False`, return all tables that contain the '(partial) name'.

        :return `<list[TimeTable]>`: Matched table(s).
        """
        return self._search_by_name(name, exact)

    def search_by_charset(self, charset: str, exact: bool = False) -> list[TimeTable]:
        """Search tables by charset.

        :param charset: `<str>` The charset or partial charset of the desired table(s).
        :param exact: `<bool>` Whether to perform exact match. Defaults to `False`.
            - If `True`, return only the tables with the exact provided 'charset'.
            - If `False`, return all tables that contain the '(partial) charset'.

        :return `<list[TimeTable]>`: Matched table(s).
        """
        return self._search_by_charset(charset, exact)

    def search_by_collate(self, collate: str, exact: bool = False) -> list[TimeTable]:
        """Search tables by collate.

        :param collate: `<str>` The collate or partial collate of the desired table(s).
        :param exact: `<bool>` Whether to perform exact match. Defaults to `False`.
            - If `True`, return only the tables with the exact provided 'collate'.
            - If `False`, return all tables that contain the '(partial) collate'.

        :return `<list[TimeTable]>`: Matched table(s).
        """
        return self._search_by_collate(collate, exact)

    def search_by_columns(
        self,
        *columns: Union[str, Column],
        match_all: bool = False,
    ) -> list[TimeTable]:
        """Search tables by columns.

        :param columns: `<tuple[str, Column]>` Column names or instances.
        :param match_all: `<bool>` Whether to match all columns.
            - If `True`, return tables that contains all of the given 'columns'.
            - If `False`, return tables if any of its columns matches any
              of the provided 'columns'.

        :return `<list[TimeTable]>`: Matched table(s).
        """
        return self._search_by_columns(columns, match_all)

    def search_by_indexes(
        self,
        *indexes: Union[str, Index],
        match_all: bool = False,
    ) -> list[TimeTable]:
        """(cfunc) Search tables by indexes.

        :param indexes: `<tuple[str, Index]>` Index names or instances.
        :param match_all: `<bool>` Whether to match all indexes. Defaults to `False`.
            - If `True`, return tables that contains all of the given 'indexes'.
            - If `False`, return tables if any of its indexes matches any
              of the provided 'indexes'.

        :return `<list[TimeTable]>`: Matched table(s).
        """
        return self._search_by_indexes(indexes, match_all)

    # Accessors ------------------------------------------------------------
    def values(self) -> tuple[TimeTable]:
        "Access the table instances `<tuple[TimeTable]>`."
        return self._instances

    def items(self) -> tuple[tuple[str, TimeTable]]:
        "Access the table names and instances `<tuple[tuple[str, TimeTable]]>`."
        return self._items

    def get(
        self,
        tb: Union[str, TimeTable],
        default: Any = None,
    ) -> Union[TimeTable, Any]:
        "Get table `<TimeTable>`. Return `default` if table does not exist."
        return self._get(tb, default)

    # Special Methods ------------------------------------------------------
    def __iter__(self) -> Iterator[TimeTable]:
        return self._instances.__iter__()

    def __getitem__(self, tb: object) -> TimeTable:
        return self._getitem(tb)


@cython.cclass
class DatabaseTables(Tables):
    """Represents a collection of Tables of for `<Database>` class (specific).

    Works as an immutable dictionary with static typings, where
    keys are the table names and values are the `Table` instances.
    Meanwhile provides distinctive features compared to the `Tables` class:
    - 1. Table validation based on MySQL Database rules.
    - 2. Collection of all the standard tables `<Tables>`.
    - 3. Collection of all the time tables `<TimeTables>`.
    """

    _standards: Tables
    _timetables: TimeTables

    def __init__(self, database: Database, *tables: Table) -> None:
        """A collection of Tables of for `<Database>` class (specific).

        Works as an immutable dictionary with static typings, where
        keys are the table names and values are the `Table` instances.
        Meanwhile provides distinctive features compared to the `Tables` class:
        - 1. Table validation based on MySQL Database rules.
        - 2. Collection of all the standard tables `<Tables>`.
        - 3. Collection of all the time tables `<TimeTables>`.

        :param database: `<Database>` The database that hosts the tables.
        :param tables: `<Table>` Table instances to add to the database.
        """
        super().__init__(database, *tables)
        self.__setup()

    # Properties -----------------------------------------------------------
    @property
    def instances(self) -> tuple[Table | TimeTable]:
        "The table instances `<tuple[Table/TimeTable]>`."
        return self._instances

    @property
    def standards(self) -> Tables:
        """The collection of standard Tables `<Tables>`.

        Works as an immutable dictionary with static typings, where
        keys are the table names and values are the `Table` instances.
        """
        return self._standards

    @property
    def timetables(self) -> TimeTables:
        """The collection of TimeTables `<TimeTables>`.

        Works as an immutable dictionary with static typings, where
        keys are the table names and values are the `TimeTable` instances.
        """
        return self._timetables

    # Setup ----------------------------------------------------------------
    def __setup(self) -> None:
        # Check for table limits
        if self._length == 0:
            raise errors.DatabaseMetadataError(
                "<{}.metadata> Database must contain at least one "
                "table.".format(self.__class__.__name__)
            )

        # Validate tables
        _standards: list = []
        _timetables: list = []
        for tb in self._instances:
            if tb.is_timetable:
                list_append(_timetables, tb)
            else:
                list_append(_standards, tb)

        # Setup sub-tables
        self._standards = Tables(self._db, *_standards)
        self._timetables = TimeTables(self._db, *_timetables)

        # Setup regex

    # Search --------------------------------------------------------------
    def search_by_name(self, name: str, exact: bool = False) -> list[Table | TimeTable]:
        """Search tables by name.

        :param name: `<str>` The name or partial name of the desired table(s).
        :param exact: `<bool>` Whether to perform exact match. Defaults to `False`.
            - If `True`, return only the table with the exact provided 'name'.
            - If `False`, return all tables that contain the '(partial) name'.

        :return `<list[Table/TimeTable]>`: Matched table(s).
        """
        return self._search_by_name(name, exact)

    def search_by_charset(
        self, charset: str, exact: bool = False
    ) -> list[Table | TimeTable]:
        """Search tables by charset.

        :param charset: `<str>` The charset or partial charset of the desired table(s).
        :param exact: `<bool>` Whether to perform exact match. Defaults to `False`.
            - If `True`, return only the tables with the exact provided 'charset'.
            - If `False`, return all tables that contain the '(partial) charset'.

        :return `<list[Table/TimeTable]>`: Matched table(s).
        """
        return self._search_by_charset(charset, exact)

    def search_by_collate(
        self, collate: str, exact: bool = False
    ) -> list[Table | TimeTable]:
        """Search tables by collate.

        :param collate: `<str>` The collate or partial collate of the desired table(s).
        :param exact: `<bool>` Whether to perform exact match. Defaults to `False`.
            - If `True`, return only the tables with the exact provided 'collate'.
            - If `False`, return all tables that contain the '(partial) collate'.

        :return `<list[Table/TimeTable]>`: Matched table(s).
        """
        return self._search_by_collate(collate, exact)

    def search_by_columns(
        self,
        *columns: Union[str, Column],
        match_all: bool = False,
    ) -> list[Table | TimeTable]:
        """Search tables by columns.

        :param columns: `<tuple[str, Column]>` Column names or instances.
        :param match_all: `<bool>` Whether to match all columns.
            - If `True`, return tables that contains all of the given 'columns'.
            - If `False`, return tables if any of its columns matches any
              of the provided 'columns'.

        :return `<list[Table/TimeTable]>`: Matched table(s).
        """
        return self._search_by_columns(columns, match_all)

    def search_by_indexes(
        self,
        *indexes: Union[str, Index],
        match_all: bool = False,
    ) -> list[Table | TimeTable]:
        """(cfunc) Search tables by indexes.

        :param indexes: `<tuple[str, Index]>` Index names or instances.
        :param match_all: `<bool>` Whether to match all indexes. Defaults to `False`.
            - If `True`, return tables that contains all of the given 'indexes'.
            - If `False`, return tables if any of its indexes matches any
              of the provided 'indexes'.

        :return `<list[Table/TimeTable]>`: Matched table(s).
        """
        return self._search_by_indexes(indexes, match_all)

    # Exception ------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _match_table_from_absent_exc(self, exc: object) -> tuple:
        """(cfunc) Match table name from an exception.

        :param exc: `<QueryTableAbsentError>` The exception that contains the absent table name.
        :return `<tuple>`: The matched table name.
            - If matched a normal Table, returns `(table_name, None)`.
            - If matched a TimeTable, returns `(table_name, table_time <datetime>)`.
            - If not matched any table, returns `(None, None)`.
        """
        # Extract error message
        msg = str(exc)

        # Match timetable
        res = self._timetables._regex_fnames._re_dsep.search(msg)
        if res is not None:
            name: str = res.group(1)
            tb: TimeTable
            for tb in self._timetables._instances:
                try:
                    # Try to parse time from name
                    dt = tb._parse_time_from_subname(name)
                    # Is a TimeTable
                    return (tb._name, dt)
                except errors.QueryValueError:
                    pass

        # Match normal table
        res = self._standards._regex_fnames._re_dsep.search(msg)
        if res is not None:
            return (res.group(1), None)

        # Not matched
        return (None, None)

    def match_table_from_absent_exc(
        self,
        exc: object,
    ) -> tuple[str | None, datetime.datetime | None]:
        """(cfunc) Match table name from an exception.

        :param exc: `<QueryTableAbsentError>` The exception that contains the absent table name.
        :return `<tuple>`: The matched table name.
            - If matched a normal Table, returns `(table_name, None)`.
            - If matched a TimeTable, returns `(table_name, table_time <datetime>)`.
            - If not matched any table, returns `(None, None)`.
        """
        return self._match_table_from_absent_exc(exc)

    # Basic SQL -----------------------------------------------------------
    async def reset_indexes(self) -> int:
        """`RESET` all indexes of the tables synchronously.

        This method is invoked when there's a change in the table index settings,
        ensuring the server's indexes align with the program's `Table.indexes`
        settings. The process involves two steps:
        1. Drop all existing indexes of the table (excludes `PRIMARY` key).
        2. Recreate indexes based on the current `Table.indexes` settings.

        :raise: Subclass of `QueryError`.
        :return `<int>`: The number of tables that indexes been reset.

        ### Notice
        Errors from individual tables will `STOP` the operation and the error
        will be raised immediately.
        """
        count: cython.int = 0
        for table in self._instances:
            count += await table.reset_indexes()
        return count

    # Accessors ------------------------------------------------------------
    def values(self) -> tuple[Table | TimeTable]:
        "Access the table instances `<tuple[Table/TimeTable]>`."
        return self._instances

    def items(self) -> tuple[tuple[str, Table | TimeTable]]:
        "Access the table names and instances `<tuple[tuple[str, Table/TimeTable]]>`."
        return self._items

    def get(
        self,
        tb: Union[str, Table, TimeTable],
        default: Any = None,
    ) -> Union[Table, TimeTable, Any]:
        "Get table `<Table/TimeTable>`. Return `default` if table does not exist."
        return self._get(tb, default)

    # Special Methods ------------------------------------------------------
    def __iter__(self) -> Iterator[Table | TimeTable]:
        return self._instances.__iter__()

    def __getitem__(self, tb: object) -> Union[Table, TimeTable]:
        return self._getitem(tb)


# Database ======================================================================================
@cython.cclass
class Database:
    """Represents a Database hosted on the Sever.

    Configure database's metadata by overwriting the `metadata()` method:
    - Add tables through Table `instance`: `self.my_table = MyTable(self)`
    - Add tables through Table `subclass`: `self.my_table = MyTable`
    - * Notice, using `subclass` approach, most static typing of the table
      methods will be incorrect (redundant 'self' argument), but the
      functionality of the table will not be affected.
    """

    _server: Server
    _name: str
    _name_pfix: str
    _charset: str
    _collate: str
    _tables: DatabaseTables
    _syntax: str
    _initiated: cython.bint
    __namespace: set[str]

    def __init__(
        self,
        server: Server,
        name: str,
        charset: str = "utf8mb4",
        collate: str = "utf8mb4_0900_as_cs",
    ) -> None:
        """The base class for a Database hosted on the Server.

        :param server: `<Server>` The server that host the database.
        :param name: `<str>` The name of the database (auto convert to lower case).
        :param charset: `<str>` The charset of the database. Defaults to `'utf8mb4'`.
        :param collate: `<str>` The collate of the database. Defaults to `'utf8mb4_0900_as_cs'`.

        ### Configuration:
        - Overwrite `Database.metadata()` to define database's tables.
        - Add tables through Table `instance`: `self.my_table = MyTable(self)`
        - Add tables through Table `subclass`: `self.my_table = MyTable`
        - * Notice, using `subclass` approach, most static typing of the table
        methods will be incorrect (redundant 'self' argument), but the
        functionality of the table will not be affected.

        ### Example (pre-defined database name):
        >>> class MyDatabase1(Database):
                def __init__(self, server: Server) -> None:
                    # . pre-define database name
                    super().__init__(server, "mydb1")

        >>>     def metadata(self) -> None:
                    # . instance approach
                    self.table1 = MyTable1(self)
                    # . subclass approach
                    self.table2 = MyTable2
                    ...

        ### Example (dynamic database name):
        >>> class MyDatabase2(Database):
                def __init__(self, server: Server, county: str) -> None:
                    name = "mydb2_" + county
                    super().__init__(server, name)

        >>>     def metadata(self) -> None:
                    self.table1 = MyTable1(self)
                    self.table2 = MyTable2
                    ...
            # This dynamic database name approach is useful when you want to
            # create multiple databases with the same schema, but different
            # names, e.g. one database per county (mydb2_us, mydb2_ca, ...).
        """
        self._server = server
        self._name = name
        self._charset = charset
        self._collate = collate
        self._initiated = False
        self.__setup()

    # Properties ----------------------------------------------------------
    @property
    def server(self) -> Server:
        "The MySQL Server of hosting the database `<Server>`."
        return self._server

    @property
    def name(self) -> str:
        "The name of the database `<str>`."
        return self._name

    @property
    def charset(self) -> str:
        "The MySQL `CHARACTER SET` of the database `<str>`."
        return self._charset

    @property
    def collate(self) -> str:
        "The MySQL `COLLATION` of the database `<str>`."
        return self._collate

    @property
    def tables(self) -> DatabaseTables:
        """The collection of all the tables `<DatabaseTables>`.

        Works as an immutable dictionary with static typings, where
        keys are the table names and values are the `Table` instances.
        """
        return self._tables

    @property
    def syntax(self) -> str:
        "The `SQL` syntax of the database `<str>`."
        return self._syntax

    @property
    def initiated(self) -> bool:
        "Whether the database is initiated `<bool>`."
        return self._initiated

    # Setup ---------------------------------------------------------------
    def __setup(self) -> None:
        self.__validation()
        self.metadata()
        self.__construct_metadata()

    def __validation(self) -> None:
        # Validate database name
        _name: str = utils._str_clean_name(self._name.lower())
        if not _name:
            raise errors.DatabaseMetadataError(
                "<{}.metadata> Database name is not invalid: "
                "'{}'".format(self.__class__.__name__, self._name)
            )
        if _name in settings.PROHIBIT_NAMES:
            raise errors.DatabaseMetadataError(
                "<{}.metadata> Database name '{}' is prohibited, "
                "please choose another one.".format(self.__class__.__name__, self._name)
            )
        if len(_name) > settings.MAX_NAME_LENGTH:
            raise errors.DatabaseMetadataError(
                "<{}.metadata> Database name '{}' is too long, "
                "must be <= {} characters.".format(
                    self.__class__.__name__, _name, settings.MAX_NAME_LENGTH
                )
            )
        self._name = _name
        self._name_pfix = _name + "."

        # Validate Server
        if not isinstance(self._server, Server):
            raise errors.DatabaseMetadataError(
                "<{}.metadata> Database only accept instance of "
                "<'mysql_engine.Server'> as the server, instead of: "
                "'{}' {}".format(self._name, self._server, type(self._server))
            )

        # Validate charset & collate
        try:
            charSet = self._validate_charset_collate(self._charset, self._collate)
        except ValueError as err:
            raise errors.DatabaseMetadataError(
                "<{}.metadata> {}".format(self._name, err)
            ) from err
        self._charset = charSet._name
        self._collate = charSet._collate

        # Save the name space
        namespace: set = set(dir(self))
        namespace.add("_server")
        namespace.add("_name")
        namespace.add("_name_pfix")
        namespace.add("_charset")
        namespace.add("_collate")
        namespace.add("_tables")
        namespace.add("_syntax")
        namespace.add("_initiated")
        self.__namespace = namespace

    def metadata(self) -> None:
        """Define the database metadata. This method should be overwritten
        in subclass to configure database's tables.

        ### Configuration:
        - Overwrite `Database.metadata()` to define database's tables.
        - Add tables through Table `instance`: `self.my_table = MyTable(self)`
        - Add tables through Table `subclass`: `self.my_table = MyTable`
        - * Notice, using `subclass` approach, most static typing of the table
        methods will be incorrect (redundant 'self' argument), but the
        functionality of the table will not be affected.

        ### Example:
        >>> def metadata(self) -> None:
                # . instance approach
                self.table1 = MyTable1(self)
                # . subclass approach
                self.table2 = MyTable2
                ...
        """
        raise NotImplementedError(
            "<{}.metadata> Database metadata is not configured. "
            "Please overwrite `Database.metadata()` method to "
            "define database's tables.".format(self._name)
        )

    def __construct_metadata(self) -> None:
        # Setup tables
        tables: list = []
        for name, attr in self.__dict__.items():
            # . table instance
            if isinstance(attr, Table):
                table = attr
            # . table class type
            elif type(attr) is type and issubclass(attr, Table):
                try:
                    table = attr(self)
                except Exception as err:
                    err.add_note(
                        "-> <%s> Failed to instanciate table 'self.%s = %s'. "
                        "Please refer to `<Database>` and `<Table>` classes "
                        "for more information." % (self._name, name, attr)
                    )
                    raise err
                self.__setattr__(name, table)
            # . not relevant
            else:
                continue

            # . validate attribute collision
            if set_contains(self.__namespace, name):
                raise errors.DatabaseMetadataError(
                    "<{}.metadata> The attritbue name of the Table 'self.{}' collides "
                    "with `<Database>` built-in attributes, please choose another one.".format(
                        self.__class__.__name__, name
                    )
                )
            list_append(tables, table)

        # Construct tables
        try:
            self._tables = DatabaseTables(self, *tables)
        except Exception as err:
            err.add_note("-> <%s> Table metadata error." % self._name)
            raise err

        # Construct syntax
        self._syntax = "%s CHARACTER SET %s COLLATE %s" % (
            self._name,
            self._charset,
            self._collate,
        )

    # Initiate ------------------------------------------------------------
    @query_exc_handler()
    async def initiate(self) -> bool:
        """Initiate the database `<bool>`.

        - If the database has been already initiated, nothing happens.
        - Else, `CREATE` the database `IF NOT EXSITS`, and then initiate
          all normal `Table` (`TimeTable` will only be initiated at use).
        """
        # Already initiated
        if self._initiated:
            return True

        # Create if not exists
        if not await self.exists():
            await self.create()
        # Synchronize database information
        else:
            await self._sync_information()

        # Initiate tables
        await gather(*[tb.initiate() for tb in self._tables._standards._instances])

        # Update status
        self._initiated = True
        return True

    async def _sync_information(self) -> None:
        """(Base method, internal use only). Synchronize Database information."""
        fetch: dict
        async with self._server.acquire() as conn:
            async with conn.cursor(DictCursor, False) as cur:
                await cur.execute(
                    "SELECT default_collation_name AS c FROM information_schema.schemata"
                    "\nWHERE schema_name = '%s';" % self._name
                )
                fetch = await cur.fetchone()
        try:
            charSet = charset_by_collate(fetch["c"])
        except Exception as err:
            raise errors.QueryOperationalError(
                "<{}> Failed to update Database's charset & "
                "collate: {}".format(self._name, err)
            ) from err
        self._charset = charSet._name
        self._collate = charSet._collate

    # Exception ------------------------------------------------------------
    async def _handle_absent_table(
        self,
        exc: Exception,
        resolve: cython.bint,
        warnings: cython.bint,
    ) -> None:
        """(Internal use only) When query involves a table that does
        not exists, this method will be called to resolve the error.
        """
        # Resolve absent table
        if resolve:
            # Match absent table
            table_name, table_time = self._tables._match_table_from_absent_exc(exc)
            if table_name is not None:
                # . is TimeTable
                if table_time is not None:
                    tb = self._tables._timetables.get(table_name)
                    if tb is not None:
                        await tb.initiate(table_time)
                        raise exc
                # . is normal Table
                else:
                    tb = self._tables._standards.get(table_name)
                    if tb is not None:
                        await tb.initiate()
                        raise exc

            # Failed to resolve absent table & raise `QueryProgrammingError`.
            raise errors.QueryProgrammingError(
                "<{}> Query involves table that does not belongs to the database "
                "'{}': {}".format(self._name, self._name, exc)
            ) from exc

        # Issue warning
        if warnings:
            logger.error(f"<{self._name}> {str(exc)}")

    # Core SQL ------------------------------------------------------------
    def acquire(self) -> PoolConnectionManager:
        """Acquire a free connection from the `Server` pool.

        By acquiring connection through this method, the following will happen:
        - 1. Acquire a free/new connection from the Server pool.
        - 2. Return `PoolConnectionManager` that wraps the connection.
        - 3. Release the connection back to the pool at exist.

        This method provides a more flexible approach to execute queries compared
        to the `transaction()` method. However, it requires manual handling of
        transaction states like `BEGIN`, `ROLLBACK`, and `COMMIT`.

        :raise: Subclass of `QueryError`.
        :return `PoolConnectionManager`: Server connection.

        ### Example:
        >>> async with db.acquire() as conn:
                await conn.begin() # . start transaction
                username = (
                    await db.user.select("username")
                    .where("id = %s", 1)
                    .for_update()
                    # IMPORTANT: must pass conn to `execute()`. Otherwise, the
                    # query will be executed with a temp (different) connection.
                    .execute(conn)
                )
                ... # . sequences of queries
                await conn.commit() # . commit transaction
        """
        return self._server.acquire()

    def transaction(self) -> PoolTransactionManager:
        """Acquire a free connection from the `Server` pool and `START TRANSACTION`.

        By acquiring connection through this method, the following will happen:
        - 1. Acquire a free/new connection from the Server pool.
        - 2. Use the connection to `START TRANSACTION`.
        - 3. Return `PoolTransactionManager` that wraps the connection.
        - 4a. If catches ANY exceptions during the transaction, execute
            `ROLLBACK`, then close and release the connection.
        - 4b. If the transaction executed successfully, execute `COMMIT`
             and then release the connection back to the Server pool.

        This method offers a more convenient way to execute transactions
        compared to the `acquire()` method, as it automatically manages
        transaction states like `BEGIN`, `ROLLBACK`, and `COMMIT`.

        :raise: Subclass of `QueryError`.
        :return `PoolTransactionManager`: Server connection.

        ### Example:
        >>> async with db.transaction() as conn:
                # . transaction is already started
                username = (
                    await db.user.select("username")
                    .where("id = %s", 1)
                    .for_update()
                    # IMPORTANT: must pass conn to `execute()`. Otherwise, the
                    # query will be executed with a temp (different) connection.
                    .execute(conn)
                )
                ... # . sequences of queries
                # . commit will be executed at exist.
        """
        return self._server.transaction()

    async def execute_query(
        self,
        stmt: str,
        args: Union[list, tuple, None] = None,
        conn: Union[Connection, None] = None,
        reusable: bool = True,
        cursor: type[Cursor | SSCursor] = Cursor,
        timeout: Union[int, None] = None,
        warnings: bool = True,
        *,
        resolve_absent_table: bool = False,
    ) -> int:
        """Execute a SQL statement.

        :param stmt: `<str>` The plain SQL statement to be executed.
        :param args: `<list/tuple>` Arguments for the `'%s'` placeholders in 'stmt'. Defaults to `None`.
        :param conn: `<Connection>` Specific connection to execute this query. Defaults to `None`.
            - If provided, the conn will be used to execute the SQL 'stmt'.
              This parameter is typically used within the `acquire()` or
              `transaction()` context.
            - If `None`, a temporary conn will be acquired from the Server pool
              to execute the `stmt`. After execution, the temporary conn will
              execute `COMMIT` and release back to the Server pool.

        :param reusable: `<bool>` Whether the 'conn' (if provided) is reusable after query execution. Defaults to `True`.
            - If `True`, the connection will return back to the Server pool,
              waiting for the next query.
            - If `False`, after returned to the Server pool, the connection
              will be closed and released. This is useful for certain types
              of statements, such as `CREATE TEMPORARY TABLE` and `LOCK TABLES`,
              where it's desirable to ensure the connection is closed at the end
              to release (potential) resources.

        :param cursor: `<type[Cursor/SSCursor]>` The `Cursor` class to use for query execution. Defaults to `Cursor`.

        :param timeout: `<int>` Query execution timeout in seconds. Dafaults to `None`.
            - If set to `None` or `0`, `tables.server.query_timeout` will be used
              as the default timeout.
            - `SQLQueryTimeoutError` will be raised when the timeout is reached.

        :param warnings: `<bool>` Whether to issue any SQL related warnings. Defaults to `True`.

        :param resolve_absent_table: `<bool>` Whether to resolve absent table. Defaults to `False`.
            - If `True`, when `stmt` involves a table that does not exist, an attempt
              will be made to create the missing table (if it belongs to the current
              database). If creation failed, an `SQLQueryProgrammingError` will be
              raised; otherwise, an `SQLQueryTableDontExistsError` will be raised.
            - If `False`, when `stmt` involves a table that does not exist, instead of
              raising an error, `0` will be returned as the execution result.

        :raises: Subclass of `QueryError`.
        :return `<int>`: Number of rows affected by the query.

        ### Example:
        >>> await db.execute_query(
                "UPDATE db.user SET name = %s WHERE id = %s;",
                args=('john', 1), # muti-rows: arge=[('john', 1), ('jackson', 2)]
                conn=None,
                reusable=True,
                cursor=Cursor,
                resolve_absent_table=False,
                timeout=None,
                warnings=True,
            )
        """

        # . execute by temporary connection
        async def execute_by_temp(timeout: object):
            async with self.acquire() as conn:
                try:
                    res = await execute(conn, timeout)
                except Exception as err:
                    err.add_note("-> <%s> Failed to execute:\n%s" % (self._name, stmt))
                    try:
                        await conn.rollback()
                    except Exception as rb_err:
                        err.add_note(
                            "-> <%s> Failed to ROLLBACK: %s" % (self._name, rb_err)
                        )
                    raise err
                await self.commit(conn)
                return res

        # . execute by given connection
        async def execute_by_conn(conn: Connection, timeout: object):
            try:
                return await execute(conn, timeout)
            except Exception as err:
                err.add_note("-> <%s> Failed to execute:\n%s" % (self._name, stmt))
                raise err

        # . execute
        async def execute(conn: Connection, timeout: object):
            async def query() -> int:
                async with conn.cursor(cursor, warnings) as cur:
                    return await cur.execute(stmt, args)

            try:
                conn._reusable = reusable
                return await wait_for(query(), timeout)
            except errors.TimeoutError as err:
                raise errors.QueryTimeoutError(err) from err
            except errors.IncompleteReadError as err:
                raise errors.QueryIncompleteReadError(err) from err
            except errors.QueryTableAbsentError as err:
                await self._handle_absent_table(err, resolve_absent_table, warnings)
                return 0

        # Validate arguments
        if args is not None and not is_tuple(args) and not is_list(args):
            raise errors.QueryValueError(
                "<{}.execute_query> Parameter 'args' only accepts `None`, `<tuple>` or "
                "`<list>`, instead of: {}".format(self._name, type(args))
            )

        # Validate timeout
        if timeout is None and self._server._query_timeout > 0:
            timeout = self._server._query_timeout

        # Execute query
        if isinstance(conn, Connection):
            return await execute_by_conn(conn, timeout)
        else:
            return await execute_by_temp(timeout)

    @overload
    async def fetch_query(
        self,
        stmt: str,
        args: Union[list, tuple, None] = None,
        conn: Union[Connection, None] = None,
        cursor: type[DictCursor | SSDictCursor] = DictCursor,
        timeout: Union[int, None] = None,
        warnings: bool = True,
        *,
        resolve_absent_table: bool = False,
    ) -> tuple[dict[str, Any]]:
        """Execute a SQL statement and fetch the result.

        :param stmt: `<str>` The plain SQL statement to be executed.
        :param args: `<list/tuple>` Arguments for the `'%s'` placeholders in 'stmt'. Defaults to `None`.
        :param conn: `<Connection>` Specific connection to execute this query. Defaults to `None`.
            - If provided, the conn will be used to execute the SQL 'stmt'.
              This parameter is typically used within the `acquire()` or
              `transaction()` context.
            - If `None`, a temporary conn will be acquired from the Server pool
              to execute the `stmt`. After execution, the temporary conn will
              execute `COMMIT` and release back to the Server pool.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :param timeout: `<int>` Query execution timeout in seconds. Dafaults to `None`.
            - If set to `None` or `0`, `tables.server.query_timeout` will be used
              as the default timeout.
            - `SQLQueryTimeoutError` will be raised when the timeout is reached.

        :param warnings: `<bool>` Whether to issue any SQL related warnings. Defaults to `True`.

        :param resolve_absent_table: `<bool>` Whether to resolve absent table. Defaults to `False`.
            - If `True`, when `stmt` involves a table that does not exist, an attempt
              will be made to create the missing table (if it belongs to the current
              database). If creation failed, an `SQLQueryProgrammingError` will be
              raised; otherwise, an `SQLQueryTableDontExistsError` will be raised.
            - If `False`, when `stmt` involves a table that does not exist, instead of
              raising an error, an empty `<tuple>` will be returned as the execution
              result.

        :raises: Subclass of `QueryError`.
        :return `<tuple[dict]>`: The fetched result.

        Example:
        >>> await db.fetch_query(
                "SELECT name, price FROM db.user WHERE id = %s",
                args=(1,), # does not support multi-rows arguments.
                conn=None,
                cursor=DictCursor,
                resolve_absent_table=False,
                timeout=10,
                warnings=True,
            )
        """

    @overload
    async def fetch_query(
        self,
        stmt: str,
        args: Union[list, tuple, None] = None,
        conn: Union[Connection, None] = None,
        cursor: type[DfCursor | SSDfCursor] = DictCursor,
        timeout: Union[int, None] = None,
        warnings: bool = True,
        *,
        resolve_absent_table: bool = False,
    ) -> DataFrame:
        """Execute a SQL statement and fetch the result.

        :param stmt: `<str>` The plain SQL statement to be executed.
        :param args: `<list/tuple>` Arguments for the `'%s'` placeholders in 'stmt'. Defaults to `None`.
        :param conn: `<Connection>` Specific connection to execute this query. Defaults to `None`.
            - If provided, the conn will be used to execute the SQL 'stmt'.
              This parameter is typically used within the `acquire()` or
              `transaction()` context.
            - If `None`, a temporary conn will be acquired from the Server pool
              to execute the `stmt`. After execution, the temporary conn will
              execute `COMMIT` and release back to the Server pool.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :param timeout: `<int>` Query execution timeout in seconds. Dafaults to `None`.
            - If set to `None` or `0`, `tables.server.query_timeout` will be used
              as the default timeout.
            - `SQLQueryTimeoutError` will be raised when the timeout is reached.

        :param warnings: `<bool>` Whether to issue any SQL related warnings. Defaults to `True`.

        :param resolve_absent_table: `<bool>` Whether to resolve absent table. Defaults to `False`.
            - If `True`, when `stmt` involves a table that does not exist, an attempt
              will be made to create the missing table (if it belongs to the current
              database). If creation failed, an `SQLQueryProgrammingError` will be
              raised; otherwise, an `SQLQueryTableDontExistsError` will be raised.
            - If `False`, when `stmt` involves a table that does not exist, instead of
              raising an error, an empty `<DataFrame>` will be returned as the execution
              result.

        :raises: Subclass of `QueryError`.
        :return `<DataFrame>`: The fetched result.

        Example:
        >>> await db.fetch_query(
                "SELECT name, price FROM db.user WHERE id = %s",
                args=(1,), # does not support multi-rows arguments.
                conn=None,
                cursor=DictCursor,
                resolve_absent_table=False,
                timeout=10,
                warnings=True,
            )
        """

    @overload
    async def fetch_query(
        self,
        stmt: str,
        args: Union[list, tuple, None] = None,
        conn: Union[Connection, None] = None,
        cursor: type[Cursor | SSCursor] = DictCursor,
        timeout: Union[int, None] = None,
        warnings: bool = True,
        *,
        resolve_absent_table: bool = False,
    ) -> tuple[tuple[Any]]:
        """Execute a SQL statement and fetch the result.

        :param stmt: `<str>` The plain SQL statement to be executed.
        :param args: `<list/tuple>` Arguments for the `'%s'` placeholders in 'stmt'. Defaults to `None`.
        :param conn: `<Connection>` Specific connection to execute this query. Defaults to `None`.
            - If provided, the conn will be used to execute the SQL 'stmt'.
              This parameter is typically used within the `acquire()` or
              `transaction()` context.
            - If `None`, a temporary conn will be acquired from the Server pool
              to execute the `stmt`. After execution, the temporary conn will
              execute `COMMIT` and release back to the Server pool.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :param timeout: `<int>` Query execution timeout in seconds. Dafaults to `None`.
            - If set to `None` or `0`, `tables.server.query_timeout` will be used
              as the default timeout.
            - `SQLQueryTimeoutError` will be raised when the timeout is reached.

        :param warnings: `<bool>` Whether to issue any SQL related warnings. Defaults to `True`.

        :param resolve_absent_table: `<bool>` Whether to resolve absent table. Defaults to `False`.
            - If `True`, when `stmt` involves a table that does not exist, an attempt
              will be made to create the missing table (if it belongs to the current
              database). If creation failed, an `SQLQueryProgrammingError` will be
              raised; otherwise, an `SQLQueryTableDontExistsError` will be raised.
            - If `False`, when `stmt` involves a table that does not exist, instead of
              raising an error, an empty `tuple[tuple]` will be returned as the execution
              result.

        :raises: Subclass of `QueryError`.
        :return `<tuple[tuple]>`: The fetched result.

        Example:
        >>> await db.fetch_query(
                "SELECT name, price FROM db.user WHERE id = %s",
                args=(1,), # does not support multi-rows arguments.
                conn=None,
                cursor=DictCursor,
                resolve_absent_table=False,
                timeout=10,
                warnings=True,
            )
        """

    async def fetch_query(
        self,
        stmt: str,
        args: Union[list, tuple, None] = None,
        conn: Union[Connection, None] = None,
        cursor: type[
            DictCursor | SSDictCursor | DfCursor | SSDfCursor | Cursor | SSCursor
        ] = DictCursor,
        timeout: Union[int, None] = None,
        warnings: bool = True,
        *,
        resolve_absent_table: bool = False,
    ) -> Union[tuple[dict[str, Any] | tuple[Any]], DataFrame]:
        """Execute a SQL statement and fetch the result.

        :param stmt: `<str>` The plain SQL statement to be executed.
        :param args: `<list/tuple>` Arguments for the `'%s'` placeholders in 'stmt'. Defaults to `None`.
        :param conn: `<Connection>` Specific connection to execute this query. Defaults to `None`.
            - If provided, the conn will be used to execute the SQL 'stmt'.
              This parameter is typically used within the `acquire()` or
              `transaction()` context.
            - If `None`, a temporary conn will be acquired from the Server pool
              to execute the `stmt`. After execution, the temporary conn will
              execute `COMMIT` and release back to the Server pool.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :param timeout: `<int>` Query execution timeout in seconds. Dafaults to `None`.
            - If set to `None` or `0`, `tables.server.query_timeout` will be used
              as the default timeout.
            - `SQLQueryTimeoutError` will be raised when the timeout is reached.

        :param warnings: `<bool>` Whether to issue any SQL related warnings. Defaults to `True`.

        :param resolve_absent_table: `<bool>` Whether to resolve absent table. Defaults to `False`.
            - If `True`, when `stmt` involves a table that does not exist, an attempt
              will be made to create the missing table (if it belongs to the current
              database). If creation failed, an `SQLQueryProgrammingError` will be
              raised; otherwise, an `SQLQueryTableDontExistsError` will be raised.
            - If `False`, when `stmt` involves a table that does not exist, instead of
              raising an error, an empty `tuple` or `DataFrame` (depends on 'cursor' type)
              will be returned as the execution result.

        :raises: Subclass of `QueryError`.
        :return `<tuple/DataFrame>`: The fetched result (depends on 'cursor' type).

        Example:
        >>> await db.fetch_query(
                "SELECT name, price FROM db.user WHERE id = %s",
                args=(1,), # does not support multi-rows arguments.
                conn=None,
                cursor=DictCursor,
                resolve_absent_table=False,
                timeout=10,
                warnings=True,
            )
        """

        # . execute by temporary connection
        async def execute_by_temp(timeout: object) -> object:
            async with self.acquire() as conn:
                try:
                    res = await execute(conn, timeout)
                except Exception as err:
                    err.add_note("-> <%s> Failed to execute:\n%s" % (self._name, stmt))
                    raise err
                await self.commit(conn)
                return res

        # . execute by given connection
        async def execute_by_conn(conn: Connection, timeout: object) -> object:
            try:
                return await execute(conn, timeout)
            except Exception as err:
                err.add_note("-> <%s> Failed to execute:\n%s" % (self._name, stmt))
                raise err

        # . execute
        async def execute(conn: Connection, timeout: object) -> object:
            async def query() -> object:
                async with conn.cursor(cursor, warnings) as cur:
                    await cur.execute(stmt, args)
                    return await cur.fetchall()

            try:
                return await wait_for(query(), timeout)
            except errors.TimeoutError as err:
                raise errors.QueryTimeoutError(err) from err
            except errors.IncompleteReadError as err:
                raise errors.QueryIncompleteReadError(err) from err
            except errors.QueryTableAbsentError as err:
                await self._handle_absent_table(err, resolve_absent_table, warnings)
                return cursor.empty_result

        # Validate arguments
        if args is not None and not is_tuple(args) and not is_list(args):
            raise errors.QueryValueError(
                "<{}.execute_query> Parameter 'args' only accepts `None`, `<tuple>` or "
                "`<list>`, instead of: {}".format(self._name, type(args))
            )

        # Validate timeout
        if timeout is None and self._server._query_timeout > 0:
            timeout = self._server._query_timeout

        # Execute query
        if isinstance(conn, Connection):
            return await execute_by_conn(conn, timeout)
        else:
            return await execute_by_temp(timeout)

    async def begin(self, conn: Connection) -> Connection:
        """`BEGIN` a transaction.

        This method serves as an alternative to `conn.begin()`,
        with additional error note to the exception when the
        `BEGIN` operation fails.

        :param conn: `<Connection>` The connection to start transaction.
        :raise: Subclass of `QueryError`.
        :return <`Connection`>: The connection that began the transaction.
        """
        try:
            await conn.begin()
            return conn
        except Exception as err:
            err.add_note("-> <%s> Failed to BEGIN transaction." % self._name)
            raise err

    async def start(self, conn: Connection) -> Connection:
        """`START TRANSACTION`. Alias for `begin()`."""
        return await self.begin(conn)

    async def rollback(self, conn: Connection) -> Connection:
        """`ROLLBACK` the transaction.

        This method serves as an alternative to `conn.rollback()`,
        with additional error note to the exception when the
        `ROLLBACK` operation fails.

        :param conn: `<Connection>` The connection of rollback transaction.
        :raise: Subclass of `QueryError`.
        :return <`Connection`>: The connection rolled back the transaction.
        """
        try:
            conn._reusable = False
            await conn.rollback()
            return conn
        except Exception as err:
            err.add_note("-> <%s> Failed to ROLLBACK transaction." % self._name)
            raise err

    async def commit(self, conn: Connection) -> Connection:
        """`COMMIT` a transaction.

        This method serves as an alternative to `conn.commit()`,
        with additional error note to the exception when the
        `COMMIT` operation fails.

        :param conn: `<Connection>` The connection to commit transaction.
        :raise: Subclass of `QueryError`.
        :return <`Connection`>: The connection committed the transaction.
        """
        try:
            await conn.commit()
            return conn
        except Exception as err:
            err.add_note("-> <%s> Failed to COMMIT transaction." % self._name)
            raise err

    async def create_savepoint(
        self,
        conn: Connection,
        savepoint: str = "sp",
    ) -> Connection:
        """Create a transaction `SAVEPOINT`.

        :param conn: `<Connection>` The connection of a transaction.
        :param savepoint: `<str>` The name of the transaction `SAVEPOINT`. Defaults to `'sp'`.
        :raise: Subclass of `QueryError`.
        :return <`Connection`>: The connection that created the `SAVEPOINT`.
        """
        try:
            async with conn.cursor(Cursor, False) as cur:
                await cur.execute("SAVEPOINT %s;" % savepoint)
            return conn
        except Exception as err:
            err.add_note(
                "-> <%s> Failed to create SAVEPOINT '%s'." % (self._name, savepoint)
            )
            raise err

    async def rollback_savepoint(
        self,
        conn: Connection,
        savepoint: str = "sp",
    ) -> Connection:
        """`ROLLBACK` to a transaction `SAVEPOINT`.

        :param conn: `<Connection>` The connection of a transaction.
        :param savepoint: `<str>` The name of the transaction `SAVEPOINT`. Defaults to `'sp'`.
        :raise: Subclass of `QueryError`.
        :return <`Connection`>: The connection that rolled back to `SAVEPOINT`.
        """
        try:
            conn._reusable = False
            async with conn.cursor(Cursor, False) as cur:
                await cur.execute("ROLLBACK TO SAVEPOINT %s;" % savepoint)
            return conn
        except Exception as err:
            err.add_note(
                "-> <%s> Failed to ROLLBACK TO SAVEPOINT '%s'."
                % (self._name, savepoint)
            )
            raise err

    async def lock(
        self,
        conn: Connection,
        *tables: Union[str, Table],
        write: bool = True,
    ) -> Connection:
        """`LOCK TABLES` in the database.

        :param conn: `<Connection>` The connection to issue the lock.
        :param tables: `<str/Table>` Tables to be locked (names or instances).
        :param write: `<bool>` Whether to lock for write. Defaults to `True`.
        :raise: Subclass of `QueryError`.
        :return <`Connection`>: The conn that locked the tables.
        """
        # No tables to lock
        if not tables:
            return conn

        # Execute lock query
        try:
            conn._reusable = False
            lock_type: str = " WRITE" if write else " READ"
            syntax: list = []
            table_name: str
            for table in tables:
                if isinstance(table, Table):
                    table_name = access_table_name(table)  # type: ignore
                elif is_str(table):
                    table_name = table
                else:
                    raise errors.QueryValueError(
                        "<{}.lock> Parameter 'tables' only accepts type of `<str>` "
                        "or `<Table>`, instead of: {}".format(self._name, type(table))
                    )
                list_append(syntax, self._name_pfix + table_name + lock_type)
            async with conn.cursor(Cursor, False) as cur:
                await cur.execute("LOCK TABLES %s;" % ", ".join(syntax))
            return conn
        except Exception as err:
            err.add_note(
                "-> <%s> Failed to LOCK TABLES: %s"
                % (self._name, ", ".join(map(str, tables)))
            )
            raise err

    async def unlock(self, conn: Connection) -> Connection:
        """`UNLOCK TABLES` that were previously locked by a connection.

        :param conn: `<Connection>` The connection that previously issued the lock.
        :raise: Subclass of `QueryError`.
        :return <`Connection`>: The conn that unlocked the tables.
        """
        try:
            async with conn.cursor(Cursor, False) as cur:
                await cur.execute("UNLOCK TABLES;")
            return conn
        except Exception as err:
            err.add_note("-> <%s> Failed to UNLOCK TABLES." % self._name)
            raise err

    # Basic SQL -----------------------------------------------------------
    async def create(self) -> bool:
        """`CREATE` the database `IF NOT EXSITS`.

        :raise: Subclass of `QueryError`.
        :return `<bool>`: Whether the database has been created.
        """
        async with self._server.acquire() as conn:
            async with conn.cursor(Cursor, False) as cur:
                await cur.execute("CREATE DATABASE IF NOT EXISTS %s;" % self._syntax)
        return True

    async def drop(self) -> int:
        """`DROP` the database `IF EXISTS`.

        :raise: Subclass of `QueryError`.
        :return `<int>`: Number of tables dropped along with the database.
        """
        async with self._server.acquire() as conn:
            async with conn.cursor(Cursor, False) as cur:
                return await cur.execute("DROP DATABASE IF EXISTS %s;" % self._name)

    async def exists(self) -> bool:
        """Check if the database exists.

        :raise: Subclass of `QueryError`.
        :return `bool`: `True` if database exists, else `False`
        """
        fetch: tuple
        async with self._server.acquire() as conn:
            async with conn.cursor(Cursor, False) as cur:
                await cur.execute(
                    "SELECT COUNT(*) AS i FROM information_schema.schemata\n"
                    "WHERE schema_name = '%s';" % self._name
                )
                fetch = await cur.fetchone()

        # Check existance
        return True if fetch and fetch[0] > 0 else False

    @overload
    async def information(
        self,
        *info: Literal[
            "*",
            "catelog_name",
            "schema_name",
            "default_character_set_name",
            "default_collation_name",
            "sql_path",
            "default_encryption",
        ],
        cursor: type[DictCursor | SSDictCursor] = DictCursor,
    ) -> tuple[dict[str, Any]]:
        """Select database information from `INFORMATION_SCHEMA`.

        Available information options:
        - 'catelog_name', 'schema_name', 'default_character_set_name',
        - 'default_collation_name', 'sql_path', 'default_encryption'

        :param info: `<str>` The information to be selected.
            - If not specified, defaults to `'schema_name'`.
            - Use `'*'` to select all information.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :raise: Subclass of `QueryError`.
        :return `<tuple[dict]>`: Database information.
        """

    @overload
    async def information(
        self,
        *info: Literal[
            "*",
            "catelog_name",
            "schema_name",
            "default_character_set_name",
            "default_collation_name",
            "sql_path",
            "default_encryption",
        ],
        cursor: type[DfCursor | SSDfCursor] = DictCursor,
    ) -> DataFrame:
        """Select database information from `INFORMATION_SCHEMA`.

        Available information options:
        - 'catelog_name', 'schema_name', 'default_character_set_name',
        - 'default_collation_name', 'sql_path', 'default_encryption'

        :param info: `<str>` The information to be selected.
            - If not specified, defaults to `'schema_name'`.
            - Use `'*'` to select all information.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :raise: Subclass of `QueryError`.
        :return `<DataFrame>`: Database information.
        """

    @overload
    async def information(
        self,
        *info: Literal[
            "*",
            "catelog_name",
            "schema_name",
            "default_character_set_name",
            "default_collation_name",
            "sql_path",
            "default_encryption",
        ],
        cursor: type[Cursor | SSCursor] = DictCursor,
    ) -> tuple[tuple[Any]]:
        """Select database information from `INFORMATION_SCHEMA`.

        Available information options:
        - 'catelog_name', 'schema_name', 'default_character_set_name',
        - 'default_collation_name', 'sql_path', 'default_encryption'

        :param info: `<str>` The information to be selected.
            - If not specified, defaults to `'schema_name'`.
            - Use `'*'` to select all information.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :raise: Subclass of `QueryError`.
        :return `<tuple[tuple]>`: Database information.
        """

    async def information(
        self,
        *info: Literal[
            "*",
            "catelog_name",
            "schema_name",
            "default_character_set_name",
            "default_collation_name",
            "sql_path",
            "default_encryption",
        ],
        cursor: type[
            DictCursor | SSDictCursor | DfCursor | SSDfCursor | Cursor | SSCursor
        ] = DictCursor,
    ) -> Union[tuple[dict[str, Any] | tuple[Any]], DataFrame]:
        """Select database information from `INFORMATION_SCHEMA`.

        Available information options:
        - 'catelog_name', 'schema_name', 'default_character_set_name',
        - 'default_collation_name', 'sql_path', 'default_encryption'

        :param info: `<str>` The information to be selected.
            - If not specified, defaults to `'schema_name'`.
            - Use `'*'` to select all information.

        :param cursor: `<type[Cursor]>` The `Cursor` class to use for query execution. Defaults to `DictCursor`.
            - `DictCursor/SSDictCursor`: Fetch result as `<tuple[dict]>`.
            - `DfCursor/SSDfCursor`: Fetch result as `<pandas.DataFrame>`.
            - `Cursor/SSCursor`: Fetch result as `<tuple[tuple]>` (without column names).

        :raise: Subclass of `QueryError`.
        :return `<tuple/DataFrame>`: Database information.
        """
        # Check info
        info_: str
        if not info:
            info_ = "schema_name"
        elif "*" in info:
            info_ = "*"
        else:
            info_ = ", ".join(map(str, info))

        # Execute select query
        async with self._server.acquire() as conn:
            async with conn.cursor(cursor, False) as cur:
                await cur.execute(
                    "SELECT %s FROM information_schema.schemata\n"
                    "WHERE schema_name = '%s';" % (info_, self._name)
                )
                return await cur.fetchall()

    async def optimize(self) -> int:
        """Optimize all tables in the database synchronously.

        ### Notice
        Errors from individual tables will `STOP` the operation
        and the error will be raised immediately.

        :raise: Subclass of `QueryError`.
        :return `<int>`: Number of tables have been optimized.
        """
        # Optimize tables synchronously
        count: cython.int = 0
        for table in self._tables._instances:
            count += await table.optimize()
        return count

    async def alter_charset_collate(
        self,
        charset: str = "utf8mb4",
        collate: str = "utf8mb4_0900_as_cs",
        alter_tables: bool = False,
    ) -> bool:
        """`ALTER` the `CHARACTER SET` and `COLLATION` of the database
        and tables (if applicable) synchronously.

        ### Notice
        Errors from individual tables will `STOP` the operation
        and the error will be raised immediately.

        :param charset: `<str>` The charset to apply. Defaults to `'utf8mb4'`.
        :param collate: `<str>` The collate to apply. Defaults to `'utf8mb4_0900_as_cs'`.
        :param alter_tables: `<bool>` Whether to also alter all tables in the database. Defaults to `False`.
        :raise: Subclass of `QueryError`.
        :return `<bool>`: Whether charset & collate have been altered.

        Note: After altering the charset and collate, please manually update the
        corresponding Database and Table settings to ensure consistency between
        the program and the actual MySQL database. A restart of the program is
        typically required.
        """
        # Validate charset & collate
        try:
            charSet = self._validate_charset_collate(charset, collate)
        except ValueError as err:
            raise errors.QueryValueError(
                "<{}.alter_charset_collate> {}".format(self._name, err)
            ) from err
        self._charset = charSet._name
        self._collate = charSet._collate

        # Alter database
        async with self._server.acquire() as conn:
            async with conn.cursor(Cursor, False) as cur:
                await cur.execute(
                    "ALTER DATABASE %s CHARACTER SET '%s' COLLATE '%s';"
                    % (self._name, self._charset, self._collate)
                )
        if not alter_tables:
            return True

        # Alter tables
        for table in self._tables._instances:
            await table.alter_charset_collate(self._charset, self._collate)
        return True

    # Validate ------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _validate_charset_collate(self, charset: str, collate: str) -> Charset:
        """(cfunc) Validate charset & collate combination.
        :raise `ValueError`: Invalid charset or collate.
        """
        # Validate charset
        if not is_str(charset):
            raise ValueError(f"Invalid charset: {repr(charset)} {type(charset)}")
        charset = charset.lower()

        # Validate collate
        if not is_str(collate):
            raise ValueError(f"Invalid collate: {repr(charset)} {type(collate)}")
        collate = collate.lower()

        # Validate combination
        try:
            return charset_by_name_and_collate(charset, collate)
        except KeyError as err:
            raise ValueError(
                "Unsupported MySQL charset {} & collate {} "
                "combination.".format(repr(charset), repr(collate))
            ) from err

    @cython.cfunc
    @cython.inline(True)
    def _validate_cursor(self, cursor: object):
        "(cfunc) Validate cursor `<Cursor>` (internal use only)."
        if not issubclass(cursor, Cursor):
            raise errors.QueryValueError(
                "<{}> Parameter 'cursor' only accepts subclass of `<Cursor>`, "
                "instead of: {} {}".format(self._name, cursor, repr(cursor))
            )

    # Escape data ---------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _escape_item(self, item: object) -> str:
        "(cfunc) Escape item to Literal string `<str>`."
        return transcode.encode_item(item, self._server._backslash_escapes)

    def escape_item(self, item: Any) -> str:
        "Escape item to Literal string `<str>`."
        return self._escape_item(item)

    @cython.cfunc
    @cython.inline(True)
    def _escape_args(self, args: object) -> object:
        """(cfunc) Escape arguments to literal `<tuple/dict>`.

        - If the given 'args' is type of `<dict>`, returns `<dict>`.
        - All other supported data types returns `<tuple>`.
        """
        return transcode.encode_args(args, self._server._backslash_escapes)

    def escape_args(self, args: Union[list, tuple]) -> Union[tuple, dict]:
        """Escape arguments to literal `<tuple/dict>`.

        - If the given 'args' is type of `<dict>`, returns `<dict>`.
        - All other supported data types returns `<tuple>`.
        """
        return self._escape_args(args)

    # Utils ---------------------------------------------------------------
    def hash_md5(self, obj: Any) -> str:
        """MD5 hash an object.

        :param obj: `<Any>` Object can be stringified.
        :raises ValueError: If failed to md5 hash the object.
        :return <'str'>: The md5 hashed value in string.
        """
        return utils._hash_md5(obj)

    def hash_sha256(self, obj: Any) -> str:
        """SHA256 hash an object.

        :param obj: `<Any>` Object can be stringified.
        :raises ValueError: If failed to sha256 hash the object.
        :return <'str'>: The sha256 hashed value in string.
        """
        return utils._hash_sha256(obj)

    def chunk_list(
        self,
        lst: list,
        size: Union[int, None] = None,
        chunks: Union[int, None] = None,
    ) -> list[list]:
        """Chunk a list into sub-lists.

        :param lst: `<list>` List to be chunked.
        :param size: `<int>` Desired size of each chunk. Defaults to `None`.
            - If 'size' is specified (`size > 0`), 'chunks' is ignored.
            - All chunks will have the desired 'size' except potentially the last one.

        :param chunks: `<int>` Desired number of chunks. Defaults to `None`.
            - Only applicable when `size=None` or `size <= 0`.
            - Chunks will be as evenly sized as possible.

        :raises ValueError: If both 'size' and 'chunks' are not specified.
        :return <list[list]>: List of chunked sub-lists.
        """
        return utils._chunk_list(lst, size or -1, chunks or -1)

    def chunk_df(
        self,
        df: DataFrame,
        size: Union[int, None] = None,
        chunks: Union[int, None] = None,
    ) -> list[DataFrame]:
        """Chunk a DataFrame into sub-DataFrames.

        :param df: `<DataFrame>` DataFrame to be chunked.
        :param size: `<int>` Desired size of each chunk. Defaults to `None`.
            - If 'size' is specified (`size > 0`), 'chunks' is ignored.
            - All chunks will have the desired 'size' except potentially the last one.

        :param chunks: `<int>` Desired number of chunks. Defaults to `None`.
            - Only applicable when `size=None` or `size <= 0`.
            - Chunks will be as evenly sized as possible.

        :raises ValueError: If both 'size' and 'chunks' are not specified.
        :return <list[DataFrame]>: List of chunked sub-DataFrames.
        """
        return utils._chunk_df(df, size or -1, chunks or -1)

    def concat_df_columns(self, df: DataFrame) -> Series:
        """Concatenate DataFrame values to one single columns `<Series[str]>`.

        ### Notice
        The new column `<Series>` should only be used for row comparison,
        since the values will be escaped to `<str>` for concatenation.
        """
        return Series(df.values.tolist(), index=df.index).apply(self._escape_item)

    def gen_dt_now(self) -> datetime.datetime:
        "Generate datetime based on the current local time `<datetime.datetime>`."
        return cydt.gen_dt_now()

    def gen_dt_utcnow(self) -> datetime.datetime:
        "Generate datetime based on the UTC time (timezone-aware) `<datetime.datetime>`."
        return cydt.gen_dt_utcnow()

    def gen_time_now(self) -> datetime.time:
        "Generate time based on the current local time `<datetime.time>."
        return cydt.gen_time_now()

    def gen_time_utcnow(self) -> datetime.time:
        "Generate time based on the UTC time (timezone-aware) `<datetime.time>."
        return cydt.gen_time_utcnow()

    # Export & Import -----------------------------------------------------
    async def export_data(self, dir: str) -> int:
        """Export Database table data to a local directory synchronously.

        ### Notice
        - This method is designed to work alongside `import_data()` method,
          and exports data in the parquet format.
        - It does not serve as a replacement for dedicated backup tools like
          mysqldump, as it omits the MySQL schema information during export.
        - Errors from individual tables will `STOP` the operation and the
          error will be raised immediately.

        :param dir: `<str>` The root directory to store the export data.
            - This should be the root export directory, not a directory
              for specific databases or tables.
            - For example, `dir = 'desktop/mysql_export'`.

        :raises `TableExportError`: If the export operation fails.
        :return `<int>` : The number of tables successfully exported.
        """
        count: cython.int = 0
        for table in self._tables._instances:
            count += await table.export_data(dir)
        return count

    async def import_data(self, dir: str, truncate: bool = True) -> int:
        """Import Database table data from a local directory synchronously.

        ### Notice
        - This method is designed to work alongside `export_data()` method,
          and imports data from saved parquet files of the tables.
        - It does not serve as a replacement for dedicated backup tools like
          mysqldump, as it omits the MySQL schema information.
        - Errors from individual tables will `STOP` the operation and the
          error will be raised immediately.

        :param dir: `<str>` The root directory where the exported data is stored.
            - This should be the root export data directory, not a directory
              for specific databases or tables.
            - For example, `dir = 'desktop/mysql_export'`.

        :param truncate `<bool>`: Specifies how to handle the tables before import.
            - If the table does not exists in the Database, this argument has no
              effects and table will be created prior to data import.
            - If `True`, the existing table will be truncated before data import.
            - If `False`, the existing table will remain intact and the data will
              be imported with the 'IGNORE' keyword.

        :raises `TableImportError`: If the import operation fails.
        :return `<int>` : The number of tables successfully imported.
        """
        count: cython.int = 0
        for table in self._tables._instances:
            count += await table.import_data(dir, truncate)
        return count

    # Accessors -----------------------------------------------------------
    def keys(self) -> tuple[str]:
        "Access the names of the tables `<tuple[str]>`."
        return self._tables._names

    def values(self) -> tuple[Table | TimeTable]:
        "Access the table instances `<tuple[Table/TimeTable]>`."
        return self._tables._instances

    def items(self) -> tuple[tuple[str, Table | TimeTable]]:
        "Access the table names and instances `<tuple[tuple[str, Table/TimeTable]]>`."
        return self._tables._items

    def get(
        self,
        tb: Union[str, Table, TimeTable],
        default: Any = None,
    ) -> Union[Table, TimeTable, Any]:
        "Get table `<Table/TimeTable>`. Return `default` if table does not exist."
        return self._tables._get(tb, default)

    # Special Methods -----------------------------------------------------
    def __repr__(self) -> str:
        return "<Database (name='%s')>" % self._name

    def __str__(self) -> str:
        return self._name

    def __hash__(self) -> int:
        return hash((self._server, self.__repr__()))

    def __eq__(self, __o: object) -> bool:
        return hash(self) == hash(__o) if isinstance(__o, Database) else False

    def __len__(self) -> int:
        return self._tables._length

    def __iter__(self) -> Iterator[Table | TimeTable]:
        return self._tables.__iter__()

    def __getitem__(self, tb: object) -> Union[Table, TimeTable]:
        return self._tables._getitem(tb)

    def __contains__(self, tb: object) -> bool:
        return self._tables._contains(tb)
