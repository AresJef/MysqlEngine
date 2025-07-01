# cython: language_level=3
# cython: wraparound=False
# cython: boundscheck=False
from __future__ import annotations

# Cython imports
import cython
from cython.cimports.cpython import datetime  # type: ignore
from cython.cimports.cpython.time import time as unix_time  # type: ignore
from cython.cimports.cpython.list import PyList_Size as list_len  # type: ignore
from cython.cimports.cpython.list import PyList_Append as list_append  # type: ignore
from cython.cimports.cpython.set import PySet_Add as set_add  # type: ignore
from cython.cimports.cpython.set import PySet_Size as set_len  # type: ignore
from cython.cimports.cpython.set import PySet_Discard as set_discard  # type: ignore
from cython.cimports.cpython.set import PySet_Contains as set_contains  # type: ignore
from cython.cimports.cpython.dict import PyDict_Size as dict_len  # type: ignore
from cython.cimports.cpython.dict import PyDict_DelItem as dict_del  # type: ignore
from cython.cimports.cpython.dict import PyDict_SetItem as dict_setitem  # type: ignore
from cython.cimports.cpython.dict import PyDict_Contains as dict_contains  # type: ignore
from cython.cimports.cpython.unicode import PyUnicode_GET_LENGTH as str_len  # type: ignore
from cython.cimports.cpython.unicode import PyUnicode_Contains as str_contains  # type: ignore
from cython.cimports.sqlcycli.sqlfunc import ObjStr  # type: ignore
from cython.cimports.sqlcycli.charset import Charset  # type: ignore
from cython.cimports.sqlcycli.transcode import escape as _escape  # type: ignore
from cython.cimports.sqlcycli.utils import format_sql as _format_sql  # type: ignore
from cython.cimports.sqlcycli.connection import Cursor  # type: ignore
from cython.cimports.sqlcycli.aio.connection import Cursor as AioCursor  # type: ignore
from cython.cimports.sqlcycli.aio.pool import (  # type: ignore
    Pool,
    PoolConnection,
    PoolSyncConnection,
    PoolConnectionManager,
    PoolTransactionManager,
)
from cython.cimports.mysqlengine import utils  # type: ignore

datetime.import_datetime()

# Python imports
import datetime
from typing import Iterator
from warnings import warn as _warn
from sqlcycli import errors as sqlerrors
from sqlcycli.sqlfunc import ObjStr
from sqlcycli.charset import Charset
from sqlcycli.transcode import escape as _escape
from sqlcycli.connection import Cursor
from sqlcycli.aio.connection import Cursor as AioCursor
from sqlcycli.aio.pool import (
    Pool,
    PoolConnection,
    PoolSyncConnection,
    PoolConnectionManager,
    PoolTransactionManager,
)
from mysqlengine import utils, errors


__all__ = [
    "Element",
    "Elements",
    "Metadata",
    "Query",
    "Logs",
]


# Element -----------------------------------------------------------------------------------------
@cython.cclass
class Element(ObjStr):
    """Represent the base class for an element."""

    # . internal
    _el_cate: str  # The string representation of the element category
    _el_type: str  # The string representation of the element type
    _el_repr: str  # The string representation of category & type combined
    _el_position: cython.int  # The ordinal position of the element
    _el_ready: cython.bint  # Whether the element is ready
    _initialized: cython.bint  # Whether the element has already been initialized
    _hashcode: cython.Py_ssize_t  # The unique hash code of the element
    # . settings
    _name: str  # The name of the element
    _symbol: str  # The symbol of the element (used by constraint only)
    _db_name: str  # The database name of the element
    _tb_name: str  # The table name of the element
    _tb_qualified_name: str  # The qualified table name of the element [db_name.tb_name]
    _charset: Charset  # The character set of the element
    _pool: Pool  # The connection Pool of the element

    def __init__(self, el_cate: str, el_type: str):
        """The base class for an element.

        :param el_cate `<'str'>`: The string representation of the element category.
            Accepts: `"DATABASE"`, `"TABLE"`, `"COLUMN"`, `"CONSTRAINT"`, `"INDEX"`, `"PARTITION"`.
        :param el_type `<'str'>`: The string representation of the element type. Defaults to `None`.
            Example: `"TABLE"`, `"COLUMN"`, `"GENERATED COLUMN"`, `"UNIQUE KEY"`, `"INDEX"`, etc.
        """
        # . internal
        if el_cate is None:
            raise AssertionError(
                "<'%s'> element category cannot be 'None'." % self.__class__.__name__
            )
        self._el_cate = el_cate
        self._set_el_type(el_type)
        self._el_position = -1
        self._el_ready = False
        self._initialized = False
        self._hashcode = -1
        # . settings
        self._name = None
        self._symbol = None
        self._db_name = None
        self._tb_name = None
        self._tb_qualified_name = None
        self._pool = None
        self._charset = None

    # Property -----------------------------------------------------------------------------
    @property
    def charset(self) -> Charset | None:
        """The character set of the element `<'Charset/None'>`."""
        self._assure_ready()
        return self._charset

    @property
    def pool(self) -> Pool:
        """The connection Pool of the element `<'Pool'>`."""
        self._assure_ready()
        return self._pool

    @property
    def initialized(self) -> bool:
        """Whether the element has been initialized `<'bool'>`."""
        return self._initialized

    # Acquire / Transaction / Fill / Release -----------------------------------------------
    @cython.ccall
    def acquire(self) -> PoolConnectionManager:
        """Acquire a free connection from the connection pool
        through context manager `<'PoolConnectionManager'>`.

        ## Notice
        - On acquisition, the following session settings are reset to the pool's defaults:
          'autocommit', 'used_decimal', 'decode_bit', 'decode_json'.
        - If you use any `set_*()` methods to change charset or timeouts
          (read_timeout, write_timeout, wait_timeout, interactive_timeout,
          lock_wait_timeout, execution_timeout), those will be reverted to
          pool defaults on release.
        - Any other session-level changes (e.g. via SQL statements) will break
          the pool connections consistency. Please call `conn.schedule_close()`
          before releasing the connection back to the pool.

        ## Example (sync):
        >>> with element.acquire() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT * FROM table")

        ## Example (async):
        >>> async with element.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT * FROM table")
        """
        return self._pool.acquire()

    @cython.ccall
    def transaction(self) -> PoolTransactionManager:
        """Acquire a free connection from the connection pool in
        `TRANSACTION` mode through context manager `<'PoolTransactionManager'>`.

        ## Explanation
        By acquiring connection through this method, the following happens:
        - 1. Acquire a free connection from the pool.
        - 2. Use the connection to `START` a transaction.
        - 3. Returns the connection to the user.
        - 4a. If the transaction executed successfully, execute `COMMIT`
              and release the connection back to the pool.
        - 4b. If catches ANY exceptions during the transaction, close
              the connection when release back to the pool.

        ## Notice
        - On acquisition, the following session settings are reset to the pool's defaults:
          'autocommit', 'used_decimal', 'decode_bit', 'decode_json'.
        - If you use any `set_*()` methods to change charset or timeouts
          (read_timeout, write_timeout, wait_timeout, interactive_timeout,
          lock_wait_timeout, execution_timeout), those will be reverted to
          pool defaults on release.
        - Any other session-level changes (e.g. via SQL statements) will break
          the pool connections consistency. Please call `conn.schedule_close()`
          before releasing the connection back to the pool.

        ## Example (sync):
        >>> with element.transaction() as conn:
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO tb (id, name) VALUES (1, 'test')")
            # Equivalent to:
            BEGIN;
            INSERT INTO tb (id, name) VALUES (1, 'test');
            COMMIT;

        ## Example (async):
        >>> async with element.transaction() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("INSERT INTO tb (id, name) VALUES (1, 'test')")
            # Equivalent to:
            BEGIN;
            INSERT INTO tb (id, name) VALUES (1, 'test');
            COMMIT;
        """
        return self._pool.transaction()

    async def fill(self, num: int = 1) -> None:
        """Fill the pool with new asynchronous connections.

        :param num `<'int'>`: Number of new [async] connections to create. Defaults to `1`.
            - If 'num' plus the existing [async] connections in the pool exceeds the
              maximum pool size, pool is only filled up to the 'max_size' limit.
            - If 'num=-1', the pool is filled up to the 'min_size' limit.
        """
        return await self._pool.fill(num)

    @cython.ccall
    def release(self, conn: PoolConnection | PoolSyncConnection) -> object:
        """Release a connection back to the pool `<'Future'>`.

        Use this method only when you acquire a connection manually. Connections
        obtained via 'acquire()' or 'transaction()' are released automatically
        by their context managers.

        :param conn `<'PoolConnection/PoolSyncConnection'>`: The pool [sync/async] connection to release.
        :raises `<'PoolReleaseError'>`: If the connection does not belong to the connection pool.

        ## Notice
        - On acquisition, the following session settings are reset to the pool's defaults:
          'autocommit', 'used_decimal', 'decode_bit', 'decode_json'.
        - If you use any `set_*()` methods to change charset or timeouts
          (read_timeout, write_timeout, wait_timeout, interactive_timeout,
          lock_wait_timeout, execution_timeout), those will be reverted to
          pool defaults on release.
        - Any other session-level changes (e.g. via SQL statements) will break
          the pool connections consistency. Please call `conn.schedule_close()`
          before calling the 'release()' method.
        """
        return self._pool.release(conn)

    # Sync ---------------------------------------------------------------------------------
    @cython.ccall
    def ShowDatabases(self) -> tuple[str]:
        """[sync] Show all the database names in the remote server `<'tuple[str]'>`."""
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute("SHOW DATABASES;")
                res = cur.fetchall()
        return self._flatten_rows(res)

    @cython.ccall
    def ShowCreateTable(self) -> str:
        """[sync] 'SHOW CREATE TABLE' statement of the table `<'str'>`.

        :raises `<'OperationalError'>`: If the table does not exist.
        """
        sql: str = self._gen_show_create_table_sql()
        with self._pool.acquire() as conn:
            with conn.transaction() as cur:
                cur.execute(sql)
                res: tuple = cur.fetchone()
        return res[1]

    @cython.ccall
    def Unlock(self, conn: PoolSyncConnection) -> PoolSyncConnection:
        """[sync] Release all table locks held by the [sync] connection `<'PoolSyncConnection'>`.

        :param conn `<'PoolSyncConnection'>`: The [sync] connection holding any table locks.
        :returns `<'PoolSyncConnection'>`: The passed-in [sync] connection that released all the locks.
        """
        with conn.cursor() as cur:
            cur.execute("UNLOCK TABLES;")
        return conn

    # Async --------------------------------------------------------------------------------
    async def aioShowDatabases(self) -> tuple[str]:
        """[async] Show all the database names in the server `<'tuple[str]'>`."""
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute("SHOW DATABASES;")
                res = await cur.fetchall()
        return self._flatten_rows(res)

    async def aioShowCreateTable(self) -> str:
        """[async] 'SHOW CREATE TABLE' statement of the table `<'str'>`.

        :raises `<'OperationalError'>`: If the table does not exist.
        """
        sql: str = self._gen_show_create_table_sql()
        async with self._pool.acquire() as conn:
            async with conn.transaction() as cur:
                await cur.execute(sql)
                res: tuple = await cur.fetchone()
        return res[1]

    async def aioUnlock(self, conn: PoolConnection) -> PoolConnection:
        """[async] Release all table locks held by the [async] connection `<'PoolConnection'>`.

        :param conn `<'PoolConnection'>`: The [async] connection holding any table locks.
        :returns `<'PoolConnection'>`: The passed-in [async] connection that released all the locks.
        """
        async with conn.cursor() as cur:
            await cur.execute("UNLOCK TABLES;")
        return conn

    # Generate SQL -------------------------------------------------------------------------
    @cython.ccall
    def _gen_show_create_table_sql(self) -> str:
        """(internal) Generate the 'SHOW CREATE TABLE' sql `<'str'>`."""
        self._assure_ready()
        if self._tb_qualified_name is None:
            self._raise_operational_error(0, "does not support 'SHOW CREATE TABLE'.")
        return "SHOW CREATE TABLE %s;" % self._tb_qualified_name

    # Setter -------------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _set_el_type(self, el_type: str) -> cython.bint:
        """(internal) Set the element type.

        :param el_type `<'str'>`: The string representation of the element type.
            Example: "TABLE", "COLUMN", "GENERATED COLUMN", "UNIQUE KEY", "INDEX", etc.
        """
        # . set element type
        if el_type is None:
            self._raise_critical_error("element type cannot be 'None'.")
        self._el_type = el_type
        # . set element representation
        el_cate: str = self._el_cate
        if str_contains(el_type, el_cate):
            self._el_repr = el_type
        else:
            self._el_repr = "%s %s" % (el_cate, el_type)
        return True

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def set_name(self, name: object) -> cython.bint:
        """Set the name of the element `<'bool'>`.

        :param name `<'str'>`: The name of the element.

        ## Notice
        - This method does `NOT` actually set the name for the element,
          instead it only performs element name validation.
        - If the method returns `True`, write the preceeding codes to
          set the element name.
        - If returns `False`, ignore the name.
        """
        if name is None:
            return False
        if self._name is not None:
            if self._name == name:
                return False
            self._raise_critical_error(
                "has already setup its name as '%s'." % self._name
            )
        return True

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _set_tb_name(self, name: object) -> cython.bint:
        """(internal) Set the table name of the element.

        :param name `<'str'>`: The table name of the element.
        """
        if self._tb_name is not None:
            if self._tb_name == name:
                return True
            self._raise_critical_error(
                "has already setup its table name as '%s'." % self._tb_name
            )
        self._tb_name = self._validate_table_name(name)
        return self._set_tb_qualified_name()

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _set_db_name(self, name: object) -> cython.bint:
        """(internal) Set the database name of the element.

        :param name `<'str'>`: The database name of the element.
        """
        if self._db_name is not None:
            if self._db_name == name:
                return True
            self._raise_critical_error(
                "has already setup its database name as '%s'." % self._db_name
            )
        self._db_name = self._validate_database_name(name)
        return self._set_tb_qualified_name()

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _set_tb_qualified_name(self) -> cython.bint:
        """(internal) Setup the qualified table name (db_name.tb_name) of the element `<'bool'>`."""
        # Already Set
        if self._tb_qualified_name is not None:
            return True
        # Validate table & database name
        tb_name: str = self._tb_name
        if tb_name is None:
            return False
        db_name: str = self._db_name
        if db_name is None:
            return False
        # Set qualified name
        self._tb_qualified_name = db_name + "." + tb_name
        return True

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _set_charset(
        self,
        charset: object = None,
        collate: object = None,
    ) -> cython.bint:
        """(internal) Set the charset of the element.

        :param charset `<'str/Charset/None'>`: The character set of the element. Defaults to `None`.
        :param collate `<'str/None'>`: The collation of the element. Defaults to `None`.
        """
        if self._charset is None:
            self._charset = self._validate_charset(charset, collate)
        return True

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _set_pool(self, pool: object) -> cython.bint:
        """(internal) Set the connection Pool of the element.

        :param pool `<'Pool'>`: The connection pool of the element.
        """
        if self._pool is not None:
            if self._pool is pool:
                return True
            self._raise_critical_error(
                "has already setup its connection pool as %s.\n" % self._pool
            )
        self._pool = self._validate_pool(pool)
        return True

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _set_position(self, position: cython.int) -> cython.bint:
        """(internal) Set the ordinal position of the element in the elements collection."""
        if position < 1:
            self._raise_argument_error(
                "ordinal position must be greater than 0, instead got %d." % position
            )
        self._el_position = position
        return True

    # Assure Ready -------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _assure_ready(self) -> cython.bint:
        """(internal) Assure the element is ready."""
        if not self._el_ready:
            self._assure_db_name_ready()
            self._assure_encoding_ready()
            self._el_ready = True
        return True

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _assure_name_ready(self) -> cython.bint:
        """(internal) Assure the name of the element is ready."""
        if self._name is None:
            self._raise_critical_error(
                "is required to setup its name.\n"
                "Please call the 'set_name()' method to complete the configuration."
            )
        return True

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _assure_tb_name_ready(self) -> cython.bint:
        """(internal) Assure the table name of the element is ready."""
        if self._tb_name is None:
            self._raise_critical_error(
                "is required to setup its table name.\n"
                "Please call the 'setup()' method to complete the configuration."
            )
        return True

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _assure_db_name_ready(self) -> cython.bint:
        """(internal) Assure the database name of the element is ready."""
        if self._db_name is None:
            self._raise_critical_error(
                "is required to setup its database name.\n"
                "Please call the 'setup()' method to complete the configuration."
            )
        return True

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _assure_charset_ready(self) -> cython.bint:
        """(internal) Assure the character set of the element is ready."""
        if self._charset is None:
            self._raise_critical_error(
                "is required to setup its character set.\n"
                "Please call the 'setup()' method to complete the configuration."
            )
        return True

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _assure_pool_ready(self) -> cython.bint:
        """(internal) Assure the connection pool of the element is ready."""
        if self._pool is None:
            self._raise_critical_error(
                "is required to setup its connection pool.\n"
                "Please call the 'setup()' method to complete the configuration."
            )
        return True

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _assure_encoding_ready(self) -> cython.bint:
        """(internal) Assure the encoding of the element is ready.

        The method also assures both the charset & pool are ready.
        """
        self._assure_charset_ready()
        self._validate_encoding(self._charset)
        return True

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _assure_position_ready(self) -> cython.bint:
        """(internal) Assure the ordinal position of the element is ready."""
        if self._el_position < 1:
            self._raise_critical_error(
                "is required to setup its ordinal position.\n"
                "Please call the '_set_position()' method to complete the configuration."
            )
        return True

    # Validate -----------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _validate_database_name(self, name: object) -> str:
        """(internal) Validate the name of a database `<'str'>`.

        :param name `<'str'>`: The name of a database.
        """
        try:
            return utils.validate_database_name(name)
        except Exception as err:
            self._raise_definition_error(str(err), err)

    @cython.cfunc
    @cython.inline(True)
    def _validate_table_name(self, name: object) -> str:
        """(internal) Validate the name of a table `<'str'>`.

        :param name `<'str'>`: The name of a table.
        """
        if isinstance(name, Element):
            el: Element = name
            if el._tb_qualified_name is not None:
                return el._tb_qualified_name
            if el._tb_name is not None:
                return el._tb_name
        try:
            return utils.validate_table_name(name)
        except Exception as err:
            self._raise_definition_error(str(err), err)

    @cython.cfunc
    @cython.inline(True)
    def _validate_column_name(self, name: object) -> str:
        """(internal) Validate the name of a column `<'str'>`.

        :param name `<'str'>`: The name of a column.
        """
        try:
            return utils.validate_column_name(name)
        except Exception as err:
            self._raise_definition_error(str(err), err)

    @cython.cfunc
    @cython.inline(True)
    def _validete_index_name(self, name: object) -> str:
        """(internal) Validate the name of an index `<'str'>`.

        :param name `<'str'>`: The name of an index.
        """
        try:
            return utils.validete_index_name(name)
        except Exception as err:
            self._raise_definition_error(str(err), err)

    @cython.cfunc
    @cython.inline(True)
    def _validate_constraint_name(self, name: object) -> str:
        """(internal) Validate the name of a constraint `<'str'>`.

        :param name `<'str'>`: The name of a constraint.
        """
        try:
            return utils.validate_constraint_name(name)
        except Exception as err:
            self._raise_definition_error(str(err), err)

    @cython.cfunc
    @cython.inline(True)
    def _validate_partition_name(self, name: object) -> str:
        """(internal) Validate the name of a partition `<'str'>`.

        :param name `<'str'>`: The name of a partition.
        """
        try:
            return utils.validate_partition_name(name)
        except Exception as err:
            self._raise_definition_error(str(err), err)

    @cython.cfunc
    @cython.inline(True)
    def _validate_columns(self, columns: object) -> tuple[str]:
        """(internal) Validate the column(s) `<'tuple[str]'>`.

        :param columns `<'str/Element/Elements/tuple/list'>`: The column(s) to validate.
        :returns `<'tuple[str]'>`: A tuple of validated column names.
        """
        if isinstance(columns, str):
            return (self._validate_column_name(columns),)

        if isinstance(columns, Element):
            if isinstance(columns, Elements):
                els: Elements = columns
                els._assure_ready()
                return els.keys()
            else:
                el: Element = columns
                el._assure_ready()
                return (el._name,)

        if isinstance(columns, (tuple, list)):
            res: list = []
            for i in columns:
                if isinstance(i, str):
                    res.append(self._validate_column_name(i))
                elif isinstance(i, Element):
                    if isinstance(i, Elements):
                        els: Elements = i
                        els._assure_ready()
                        res.extend(els.keys())
                    else:
                        el: Element = i
                        el._assure_ready()
                        res.append(el._name)
                else:
                    self._raise_definition_error(
                        "column (%s %r) is invalid." % (type(i), i)
                    )
            if list_len(res) == 0:
                self._raise_definition_error("must specify at least one column.")
            return tuple(res)

        self._raise_definition_error(
            "column (%s %r) is invalid." % (type(columns), columns)
        )

    @cython.cfunc
    @cython.inline(True)
    def _validate_charset(
        self,
        charset: object = None,
        collate: object = None,
    ) -> Charset:
        """(internal) Validate CHARACTER SET and COLLATION (optional) `<'Charset/None'>`.

        :param charset `<'str/Charset/None'>`: The character set. Defaults to `None`.
        :param collate `<'str/None'>`: The collation. Defaults to `None`.
        """
        try:
            return utils.validate_charset(charset, collate)
        except Exception as err:
            self._raise_definition_error(str(err), err)

    @cython.cfunc
    @cython.inline(True)
    def _validate_encoding(self, charset: Charset) -> Charset:
        """(internal) Validate if the given 'charset' is compatible
        with the connection pool `<'Charset'>`.

        :param charset `<'Charset'>`: The charset to validate.
        """
        if charset is None:
            self._raise_critical_error("charset cannot be None.")
        self._assure_pool_ready()
        if charset._encoding_c != self._pool._charset._encoding_c:
            self._raise_definition_error(
                "charset encoding must be the same as the connection pool.\n"
                "<'%s'> encoding (%s) vs <'Pool'> encoding (%s)\n"
                "Please change 'charset' to a compatible one."
                % (
                    self.__class__.__name__,
                    charset._encoding,
                    self._pool._charset._encoding,
                )
            )
        return charset

    @cython.cfunc
    @cython.inline(True)
    def _validate_pool(self, pool: object) -> Pool:
        """(internal) Validate connection pool `<'Pool'>`.

        :param pool `<'Pool'>`: The connection pool to validate.
        """
        if not isinstance(pool, Pool):
            self._raise_argument_error(
                "'pool' must be an instance of <'Pool'>, "
                "instead got %s %r." % (type(pool), pool)
            )
        pool_: Pool = pool
        pool_._autocommit_mode = 0  # disable autocommit
        return pool_

    @cython.cfunc
    @cython.inline(True)
    def _validate_index_type(self, index_type: object) -> str:
        """(internal) Validate the index type `<'str/None'>`.

        :param index_type `<'str/None'>`: The index type to validate.
        """
        try:
            return utils.validate_index_type(index_type)
        except Exception as err:
            self._raise_definition_error(str(err), err)

    @cython.cfunc
    @cython.inline(True)
    def _validate_comment(self, comment: object) -> str:
        """(internal) Validate the comment `<'str/None'>`.

        :param comment `<'str/None'>`: The comment to validate.
        """
        try:
            return utils.validate_comment(comment)
        except Exception as err:
            self._raise_definition_error(str(err), err)

    @cython.cfunc
    @cython.inline(True)
    def _validate_expression(self, expr: object) -> str:
        """(internal) Validate the expression `<'str'>`.

        :param expr `<'str/None'>`: The expression to validate.
        """
        try:
            expr = utils.validate_expression(expr)
        except Exception as err:
            self._raise_definition_error(str(err), err)
        if expr is None:
            self._raise_definition_error(
                "EXPRESSION cannot be an empty string or 'None'."
            )
        return expr

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-2, check=False)
    def _validate_encryption(self, encryption: object) -> cython.int:
        """(internal) Validate the ENCRYPTION config `<'int'>.

        :param encryption `<'bool/None'>`: The encryption config to validate.

        ## Notice
        - Returns `-1` if 'encryption' is None.
        - Returns `1` if 'encryption' is True, else `0`.
        """
        try:
            return utils.validate_encryption(encryption)
        except Exception as err:
            self._raise_definition_error(str(err), err)

    # Error --------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _raise_element_error(
        self,
        err_type: str,
        msg: str,
        tb_exc: Exception = None,
    ) -> cython.bint:
        """(internal) Raise an element error.

        :param err_type `<'str'>`: The type of the element error.
            Accepts: `"DEFINITION"`, `"METADATA"`, `"ARGUMENT"`, `"NOT_EXISTS"`, `"CRITICAL"`, `"WARNING"`
        :param msg `<'str'>`: The error message.
        :param tb_exc `<'Exception/None'>`: The traceback exception. Defaults to `None`.
        """
        msg = self._prep_error_message(msg)
        exc = errors.map_sql_element_exc(self._el_cate, err_type)
        if tb_exc is None:
            raise exc(msg)
        else:
            raise exc(msg) from tb_exc

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _raise_definition_error(
        self,
        msg: str,
        tb_exc: Exception = None,
    ) -> cython.bint:
        """(internal) Raise element 'DEFINITION' error.

        :param msg `<'str'>`: The error message.
        :param tb_exc `<'Exception/None'>`: The traceback exception. Defaults to `None`.
        """
        return self._raise_element_error("DEFINITION", msg, tb_exc)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _raise_argument_error(
        self,
        msg: str,
        tb_exc: Exception = None,
    ) -> cython.bint:
        """(internal) Raise element 'ARGUMENT' error.

        :param msg `<'str'>`: The error message.
        :param tb_exc `<'Exception/None'>`: The traceback exception. Defaults to `None`.
        """
        return self._raise_element_error("ARGUMENT", msg, tb_exc)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _raise_metadata_error(
        self,
        msg: str,
        tb_exc: Exception = None,
    ) -> cython.bint:
        """(internal) Raise element 'METADATA' error.

        :param msg `<'str'>`: The error message.
        :param tb_exc `<'Exception/None'>`: The traceback exception. Defaults to `None`.
        """
        return self._raise_element_error("METADATA", msg, tb_exc)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _raise_not_exists_error(
        self,
        msg: str,
        tb_exc: Exception = None,
    ) -> cython.bint:
        """(internal) Raise element 'NOT_EXISTS' error.

        :param msg `<'str'>`: The error message.
        :param tb_exc `<'Exception/None'>`: The traceback exception. Defaults to `None`.
        """
        return self._raise_element_error("NOT_EXISTS", msg, tb_exc)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _raise_critical_error(
        self,
        msg: str,
        tb_exc: Exception = None,
    ) -> cython.bint:
        """(internal) Raise element 'CRITICAL' error.

        :param msg `<'str'>`: The error message.
        :param tb_exc `<'Exception/None'>`: The traceback exception. Defaults to `None`.
        """
        return self._raise_element_error("CRITICAL", msg, tb_exc)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _raise_operational_error(
        self,
        errno: object,
        msg: str,
        tb_exc: Exception = None,
    ) -> cython.bint:
        """(internal) Raise element operational error.

        :param errno `<'int'>`: The operational error code.
        :param msg `<'str'>`: The error message.
        :param tb_exc `<'Exception/None'>`: The traceback exception. Defaults to `None`.
        """
        msg = self._prep_error_message(msg)
        if tb_exc is None:
            raise sqlerrors.OperationalError(errno, msg)
        else:
            raise sqlerrors.OperationalError(errno, msg) from tb_exc

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _raise_not_implemented_error(self, method_name: str) -> cython.bint:
        """(internal) Raise element method not implemented error.

        :param method_name `<'str'>`: The name of the method that's not implemented.
        """
        msg = self._prep_error_message(
            "'%s()' method is not implemented." % method_name
        )
        raise NotImplementedError(msg)

    @cython.cfunc
    @cython.inline(True)
    def _prep_error_message(self, msg: str) -> str:
        """(internal) Prepare the error message `<'str'>`."""
        el_name: str = self._name
        qualified_name: str = self._tb_qualified_name
        # fmt: off
        if qualified_name is None:
            if el_name is None:
                return "<'%s'> %s %s" % (
                    self.__class__.__name__, self._el_repr, msg
                )
            else:
                return "<'%s'> %s '%s' %s" % (
                    self.__class__.__name__, self._el_repr, el_name, msg
                )
        elif el_name is None:
            return "<'%s'> %s in '%s' %s" % (
                self.__class__.__name__, self._el_repr, qualified_name, msg
            )
        elif str_contains(qualified_name, el_name):
            return "<'%s'> %s '%s' %s" % (
                self.__class__.__name__, self._el_repr, qualified_name, msg
            )
        else:
            return "<'%s'> %s '%s' in '%s' %s" % (
                self.__class__.__name__, self._el_repr, el_name, qualified_name, msg
            )
        # fmt: on

    # Warning ------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _warn(self, msg: str) -> cython.bint:
        """(internal) Issue element warning."""
        msg = self._prep_error_message(msg)
        _warn(msg, errors.map_sql_element_exc(self._el_cate, "WARNING"))
        return True

    # Internal -----------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _set_initialized(self, flag: cython.bint) -> cython.bint:
        """(internal) Set the initialized flag of the element."""
        self._initialized = flag
        return True

    # Utils --------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _escape_args(self, args: object | None, itemize: cython.bint = True) -> object:
        """(internal) Escape the arguments `<'str/tuple/list'>`.

        :param args `<'list/tuple/DataFrame/Any'>`: The arguments to escape.
        :param itemize `<'bool'>`: Whether to escape each items of the 'args' individually. Defaults to True.
            - **'itemize=True'**: the 'args' type determines how to escape.
                * 1. Sequence or Mapping (e.g. list, tuple, dict, etc) escapes to <'tuple[str]'>.
                * 2. pd.Series and 1-dimensional np.ndarray escapes to <'tuple[str]'>.
                * 3. pd.DataFrame and 2-dimensional np.ndarray escapes to <'list[tuple[str]]'>.
                  Each tuple represents one row of the 'args' .
                * 4. Single object (such as int, float, str, etc) escapes to one literal string <'str'>.
            - **'itemize=False'**: regardless of the 'args' type, all escapes to one single literal string <'str'>.
        """
        return _escape(args, False, itemize)

    @cython.cfunc
    @cython.inline(True)
    def _format_sql(
        self,
        sql: str,
        args: object | None,
        itemize: cython.bint = True,
    ) -> str:
        """(internal) Format the SQL with the given arguments `<'str'>`.

        :param sql `<'str'>`: The SQL statement to format.
        :param args `<'list/tuple/DataFrame/Any'>`: The arguments to escape and incorporate into the SQL statement.
        :param itemize `<'bool'>`: Whether to escape each items of the 'args' individually. Defaults to True.
            - **'itemize=True'**: the 'args' type determines how to escape.
                * 1. Sequence or Mapping (e.g. list, tuple, dict, etc) escapes to <'tuple[str]'>.
                * 2. pd.Series and 1-dimensional np.ndarray escapes to <'tuple[str]'>.
                * 3. pd.DataFrame and 2-dimensional np.ndarray escapes to <'list[tuple[str]]'>.
                  Each tuple represents one row of the 'args' .
                * 4. Single object (such as int, float, str, etc) escapes to one literal string <'str'>.
            - **'itemize=False'**: regardless of the 'args' type, all escapes to one single literal string <'str'>.
        """
        if args is None:
            return sql  # exit
        return _format_sql(sql, _escape(args, False, itemize))

    @cython.cfunc
    @cython.inline(True)
    def _gen_tb_qualified_name(self, tb_name: str) -> str:
        """(internal) Generate the qualified table name (db_name.tb_name) `<'str'>`.

        :param tb_name `<'str'>`: The name of the table.
        """
        self._assure_ready()
        db_name: str = self._db_name
        prefix: str = db_name + "."
        if tb_name.startswith(prefix):
            return tb_name
        elif tb_name.startswith("."):
            return db_name + tb_name
        else:
            return prefix + tb_name

    @cython.cfunc
    @cython.inline(True)
    def _flatten_rows(
        self,
        rows: tuple[tuple],
        skip_none: cython.bint = False,
    ) -> tuple:
        """(internal) Flatten the fetched result set (tuple of rows) `<'tuple'>`.

        :param rows `<'tuple[tuple]'>`: A tuple of rows (tuple) from the 'fetchall()' result set.
        :param skip_none `<'bool'>`: Skip the 'None' value if `True`, else `False`.

        ## Notice
        - This method should be only used for the 'fetchall()'
          result set from the <'Cursor'> and <'SSCursor'>.
        """
        res: list = []
        row: tuple
        if skip_none:
            for row in rows:
                for i in row:
                    if i is not None:
                        res.append(i)
        else:
            for row in rows:
                for i in row:
                    res.append(i)
        return tuple(res)

    @cython.cfunc
    @cython.inline(True)
    def _partitioning_flag_to_method(self, flag: cython.int) -> str:
        """(internal) Convert partitioning flag to the corresponding string method `<'str'>`."""
        try:
            return utils.partitioning_flag_to_method(flag)
        except Exception as err:
            self._raise_argument_error(str(err), err)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _partitioning_method_to_flag(self, method: str) -> cython.int:
        """(internal) Convert partitioning method to the corresponding flag `<'int'>`."""
        try:
            return utils.partitioning_method_to_flag(method)
        except Exception as err:
            self._raise_argument_error(str(err), err)

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def copy(self) -> Element:
        """Make a of the element `<'Element'>`.

        ## Notice
        - This method should be implemented by the subclass.
        - The copy method should create a new instance of the element
          with the same initialization settings.
        - The new instance should left the internal settings untouched
          (e.g. 'db_name', 'tb_name', etc), which should be setup
          by the 'setup()' method of the new instance after the copy.
        """
        self._raise_not_implemented_error("copy")

    # Special Methods ----------------------------------------------------------------------
    @cython.ccall
    def _sp_hashcode(self) -> cython.Py_ssize_t:
        """(special) Get the hashcode of the element `<'int'>`."""
        if self._hashcode == -1:
            self._hashcode = id(self)
        return self._hashcode

    @cython.ccall
    @cython.exceptval(-2, check=False)
    def _sp_equal(self, o: object) -> cython.int:
        """(special) Check if the element equals to the passed-in object `<'int'>.

        :returns `<'int'>`
        - `1` means equal.
        - `0` means not equal.
        - `-1` means NotImplemented.
        """
        return int(self is o)

    @cython.ccall
    @cython.exceptval(-2, check=False)
    def _sp_less_than(self, o: object) -> cython.int:
        """(special) Check if the element is less than the passed-in object `<'int'>`.

        :returns `<'int'>`
        - `1` means less than.
        - `0` means not less than.
        - `-1` means NotImplemented.
        """
        if not isinstance(o, Element):
            return -1  # not implemented
        _o: Element = o
        if self._el_cate != _o._el_cate:
            return int(self._el_cate < _o._el_cate)
        if self._el_position != _o._el_position:
            return int(self._el_position < _o._el_position)
        if (
            self._el_type is not None
            and _o._el_type is not None
            and self._el_type != _o._el_type
        ):
            return int(self._el_type < _o._el_type)
        if self._name is not None and _o._name is not None:
            return int(self._name < _o._name)
        return -1  # not implemented

    def __repr__(self) -> str:
        self._raise_not_implemented_error("__repr__")

    def __str__(self) -> str:
        self._raise_not_implemented_error("__str__")

    def __bool__(self) -> bool:
        if self._el_ready:
            return True
        try:
            self._assure_ready()
            return True
        except Exception:
            return False

    def __hash__(self) -> int:
        return self._sp_hashcode()

    def __eq__(self, o: object) -> bool:
        eq = self._sp_equal(o)
        if eq == 1:
            return True
        if eq == 0:
            return False
        return NotImplemented

    def __lt__(self, o: object) -> bool:
        lt = self._sp_less_than(o)
        if lt == 1:
            return True
        if lt == 0:
            return False
        return NotImplemented


# Elements ----------------------------------------------------------------------------------------
@cython.cclass
class Elements(Element):
    """The base class for element collection.

    Works as a dictionary where keys are the element names
    and values are the element instances.
    """

    _el_class: type
    _el_dict: dict[str, Element]
    _el_set: set[Element]
    _size: cython.Py_ssize_t

    def __init__(
        self,
        el_cate: str,
        el_type: str,
        el_class: type,
        *elements: Element,
    ):
        """The base class for element collection.

        Works as a dictionary where keys are the element names
        and values are the element instances.

        :param el_cate `<'str'>`: The string representation of the element collection category.
            Accepts: `"DATABASE"`, `"TABLE"`, `"COLUMN"`, `"CONSTRAINT"`, `"INDEX"`, `"PARTITION"`.
        :param el_type `<'str'>`: The string representation of the element collection type.
            Example: `"TABLE"`, `"COLUMN"`, `"GENERATED COLUMN"`, `"UNIQUE KEY"`, `"INDEX"`, etc.
        :param el_class `<'type'>`: The specific element class (type) the collection host.
        :param elements `<'*Element'>: The elements of the collection.
        """
        super().__init__(el_cate, el_type)
        if not isinstance(el_class, type) and not issubclass(el_class, Element):
            self._raise_critical_error(
                "'el_class' must be a type of the <'Element'> subclass, "
                "instead got %s %r." % (type(el_class), el_class)
            )
        self._el_class = el_class
        self._el_dict = {}
        self._el_set = set()
        self._size = 0

        # Setup collection
        for el in self._validate_elements(elements):
            self.add(el)

    # Property -----------------------------------------------------------------------------
    @property
    def names(self) -> tuple[str]:
        """The name of the elements `<'tuple[str]'>`."""
        return self.keys()

    @property
    def elements(self) -> tuple[Element]:
        """The instance of the elements `<'tuple[Element]'>`."""
        return self.values()

    # Collection ---------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def add(self, element: object) -> cython.bint:
        """Add new element to the collection `<'bool'>`.

        :param element `<'Element'>`: The new element to add to the collection.
        :returns `<'bool'>`: Whether the element is added.
        """
        # Validate
        if not isinstance(element, self._el_class):
            self._raise_argument_error(
                "expects instance of <'%s'> as its element, instead got %s %r."
                % (self._el_class.__name__, type(element), element)
            )
        el: Element = element
        name: str = el._name
        if name is None:
            self._raise_argument_error("does not accept element without a name.")
        if dict_contains(self._el_dict, name):
            self._raise_argument_error(
                "must be unique, instead got duplicate:\n%s\n%s."
                % (self._el_dict[name], el)
            )

        # Add to collection
        dict_setitem(self._el_dict, name, el)
        set_add(self._el_set, el)
        self._size += 1
        if el._el_position == -1:
            el._set_position(self._size)
        return True

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def remove(self, element: str | Element) -> cython.bint:
        """Remove an element from the collection `<'bool'>`.

        :param element `<'str/Element'>`: The name/instance of the element to remove.
        :returns `<'bool'>`: Whether the element is removed.
        """
        # Validate
        if isinstance(element, self._el_class):
            if not set_contains(self._el_set, element):
                return False  # exit
            el: Element = element
            name: str = el._name
        elif isinstance(element, str):
            if not dict_contains(self._el_dict, element):
                return False  # exit
            name: str = element
            el: Element = self._el_dict[name]
        else:
            return False  # exit
        # Drop from collection
        dict_del(self._el_dict, name)
        set_discard(self._el_set, el)
        if self._size > 0:
            self._size -= 1
        return True

    def search_name(
        self,
        *names: str | Element,
        exact: cython.bint = True,
    ) -> Elements:
        """Find the elements in the collection by name `<'Elements'>`.

        :param names `<'*str/Element'>`: One or more element names or Element instances to search for.
        :param exact `<'bool'`>: Perform exact matches if `True`, else partial matches. Defaults to `True`.
            - **True**: only elements whose names exactly equal the provided names are returned.
            - **False**: any element whose name contains the provided strings is included.
        """
        return self._search_name(names, exact)

    @cython.ccall
    def _search_name(self, names: object, exact: cython.bint) -> Elements:
        """(internal) Find the elements in the collection by name `<'Elements'>`.

        :param names `<'str/Element/tuple/list'>`: One or more element names or Element instances to search for.
        :param exact `<'bool'`>: Perform exact matches if `True`, else partial matches. Defaults to `True`.
            - **True**: only elements whose names exactly equal the provided names are returned.
            - **False**: any element whose name contains the provided strings is included.
        """
        # Validate
        if self._size == 0 or names is None:
            return self.__class__()  # exit
        el_names: set = self._extract_element_names(names, "'search_name'")
        if set_len(el_names) == 0:
            return self.__class__()  # exit

        # Search Name
        el: Element
        res: list = []
        for el in self._el_dict.values():
            el_name: str = el._name
            el_symbol: str = el._symbol
            if exact:
                if set_contains(el_names, el_name):
                    res.append(el)
                elif el_symbol is not None and set_contains(el_names, el_symbol):
                    res.append(el)
            else:
                for name in el_names:
                    if str_contains(el_name, name):
                        res.append(el)
                        break
                    elif el_symbol is not None and str_contains(el_symbol, name):
                        res.append(el)
                        break
        return self.__class__(*res)

    def search_type(
        self,
        *types: str | type | Element,
        exact: cython.bint = True,
    ) -> Elements:
        """Find the elements in the collection by MySQL type `<'str'>` or Python class `<'type'>` `<'Elements'>`.

        :param types `<'*str/type/Element'>`: One or more MySQL types, Python types, or Element instances to search for.
        :param exact `<'bool'`>: Perform exact matches if `True`, else partial matches. Defaults to `True`.
            - **`True`**: only elements whose type exactly matches the provided values are returned.
            - **`False`**: returns elements whose MySQL type name contains any of the provided strings,
              or whose Python type is a subclass of any provided class.
        """
        return self._search_type(types, exact)

    @cython.ccall
    def _search_type(self, types: object, exact: cython.bint) -> Elements:
        """(internal) Find the elements in the collection by MySQL type `<'str'>` or Python class `<'type'>` `<'Elements'>`.

        :param types `<'str/type/Element/list/tuple'>`: One or more MySQL types, Python types, or Element instances to search for.
        :param exact `<'bool'`>: Perform exact matches if `True`, else partial matches. Defaults to `True`.
            - **`True`**: only elements whose type exactly matches the provided values are returned.
            - **`False`**: returns elements whose MySQL type name contains any of the provided strings,
              or whose Python type is a subclass of any provided class.
        """
        # Validate
        if self._size == 0 or types is None:
            return self.__class__()  # exit
        el_types: set = self._extract_element_types(types, "'search_type'")
        if set_len(el_types) == 0:
            return self.__class__()  # exit

        # Search Type
        el: Element
        res: list = []
        for el in self._el_dict.values():
            if exact:
                # fmt: off
                if (
                    set_contains(el_types, el._el_type) 
                    or set_contains(el_types, type(el))
                ):
                    res.append(el)
                # fmt: on
            else:
                for _type in el_types:
                    if isinstance(_type, str):
                        if str_contains(el._el_type, _type):
                            res.append(el)
                            break
                    elif isinstance(el, _type):
                        res.append(el)
                        break
        return self.__class__(*res)

    def filter(self, *elements: str | Element | Elements) -> Elements:
        """Return a subset of the collection containing only the specified elements `<'Elements'>`.

        - This method filters the current Elements collection, retaining only those
        items whose name or instance matches any of the provided identifiers.

        :param elements `<'*str/Element/Elements'>`: One or more element identifiers to filter by.
            - **str**: element name.
            - **Element**: specific Element instance.
            - **Elements**: another Elements collection whose members.
        """
        return self._filter(elements)

    @cython.ccall
    def _filter(self, elements: object) -> Elements:
        """(internal) Return a subset of the collection containing only the specified elements `<'Elements'>`.

        - This method filters the current Elements collection, retaining only those
        items whose name or instance matches any of the provided identifiers.

        :param elements `<'str/Element/Elements/list/tuple'>`: One or more element identifiers to filter by.
            - **str**: element name.
            - **Element**: specific Element instance.
            - **Elements**: another Elements collection whose members.
        """
        # Validate
        if self._size == 0 or elements is None:
            return self.__class__()
        els: set = self._extract_elements(elements, "'filter'")
        if set_len(els) == 0:
            return self.__class__()

        # Filter Elements
        el: Element
        res: list = []
        for el in self._el_dict.values():
            if set_contains(els, el._name) or set_contains(els, el):
                res.append(el)
            elif el._symbol is not None and set_contains(els, el._symbol):
                res.append(el)
        return self.__class__(*res)

    def issubset(self, *elements: str | Element | Elements) -> cython.bint:
        """Determine whether all specified elements are contained in this collection `<'bool'>`.

        :param elements `<'str/Element/Elements'>`: One or more identifiers to check.
            - **str**: element name.
            - **Element**: specific Element instance.
            - **Elements**: another Elements collection.
        """
        return self._issubset(elements)

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _issubset(self, elements: object) -> cython.bint:
        """(internal) Determine whether all specified elements are contained in this collection `<'bool'>`.

        :param elements `<'str/Element/Elements'>`: One or more identifiers to check.
            - **str**: element name.
            - **Element**: specific Element instance.
            - **Elements**: another Elements collection.
        """
        # Validate
        if self._size == 0 or elements is None:
            return False
        els: set = self._extract_elements(elements, "'issubset'")
        if set_len(els) == 0:
            return False

        # Check Elements
        for el in els:
            if isinstance(el, str):
                if not dict_contains(self._el_dict, el):
                    return False
            elif not set_contains(self._el_set, el):
                return False
        return True

    # Generate SQL -------------------------------------------------------------------------
    @cython.ccall
    def _gen_definition_sql(self, indent: cython.int = 0) -> str:
        """(internal) Generate the definition SQL of the collection `<'str'>`.

        :param indent `<'int'>`: The indentation of the definition SQL. Defaults to `0`.
        """
        self._raise_not_implemented_error("_gen_definition_sql")

    # Assure Ready -------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _assure_ready(self) -> cython.bint:
        """(internal) Assure the element is ready."""
        if not self._el_ready:
            el: Element
            for el in self._el_dict.values():
                el._assure_ready()
            Element._assure_ready(self)
        return True

    # Validate -----------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _validate_elements(self, elements: object) -> list[Element]:
        """(internal) Validate the elements `<'list[Element]'>`.

        :returns `<'list[Element]'>`: A list of validated element instances.

        ## Notice
        - The elements restricted to the element class of the collection.
        """
        el_class: type = self._el_class
        if isinstance(elements, el_class):
            return [elements]

        elif isinstance(elements, Elements):
            els: Elements = elements
            if issubclass(els._el_class, el_class):
                return list(els._el_dict.values())

        elif isinstance(elements, (tuple, list)):
            res: list = []
            for i in elements:
                if isinstance(i, el_class):
                    res.append(i)
                else:
                    res.extend(self._validate_elements(i))
            return res

        self._raise_argument_error(
            "expects instance of <'%s'> as its element, instead got %s %r."
            % (el_class.__name__, type(elements), elements)
        )

    # Utils --------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _extract_elements(self, elements: object, msg: str) -> set[str | Element]:
        """(internal) Extract the elements `<'set[str/Element]'>`.

        :returns `<'set[str/Element]'>`: A set of strings or element instances.

        ## Notice
        - The elements restricted to the element class of the collection.
        """
        if isinstance(elements, str):
            return {elements}

        el_class: type = self._el_class
        if isinstance(elements, el_class):
            return {elements}

        elif isinstance(elements, Elements):
            els: Elements = elements
            if issubclass(els._el_class, el_class):
                return els._el_set

        elif isinstance(elements, (tuple, list)):
            res: set = set()
            for i in elements:
                if isinstance(i, str):
                    set_add(res, i)
                elif isinstance(i, el_class):
                    set_add(res, i)
                else:
                    for j in self._extract_elements(i, msg):
                        set_add(res, j)
            return res

        self._raise_argument_error(
            "%s expects instance of <'str/%s'>, instead got %s %r."
            % (msg, el_class.__name__, type(elements), elements)
        )

    @cython.cfunc
    @cython.inline(True)
    def _extract_element_names(self, elements: object, msg: str) -> set[str]:
        """(internal) Extract the element names `<'set[str]'>`.

        :returns `<'set[str]'>`: A set of strings or element names.

        ## Notice
        - The element names restricted to the element class of the collection.
        """
        if isinstance(elements, str):
            return {elements}

        el_class: type = self._el_class
        if isinstance(elements, el_class):
            el: Element = elements
            return set() if el._name is None else {el._name}

        elif isinstance(elements, Elements):
            els: Elements = elements
            if issubclass(els._el_class, el_class):
                return set(els._el_dict.keys())

        elif isinstance(elements, (tuple, list)):
            res: set = set()
            for i in elements:
                if isinstance(i, str):
                    set_add(res, i)
                elif isinstance(i, el_class):
                    el: Element = i
                    if el._name is not None:
                        set_add(res, el._name)
                else:
                    for j in self._extract_element_names(i, msg):
                        set_add(res, j)
            return res

        self._raise_argument_error(
            "%s expects instance of <'str/%s'>, instead got %s %r."
            % (msg, el_class.__name__, type(elements), elements)
        )

    @cython.cfunc
    @cython.inline(True)
    def _extract_element_types(
        self,
        elements: object,
        msg: str,
    ) -> set[str | type[Element]]:
        """(internal) Extract the element types `<'set[str/type[Element]]'>`.

        :returns `<'set[str/type[Element]]'>`: A set of strings or types of element.

        ## Notice
        - The element types are generic, and `NOT` restrict to the element class of the collection.
        """
        if isinstance(elements, str):
            s: str = elements
            return {s.upper()}

        if type(elements) is type:
            return {elements}

        if isinstance(elements, Element):
            if isinstance(elements, Elements):
                els: Elements = elements
                return {type(i) for i in els._el_dict.values()}
            else:
                return {type(elements)}

        if isinstance(elements, (tuple, list)):
            res: set = set()
            for i in elements:
                if isinstance(i, str):
                    s: str = i
                    set_add(res, s.upper())
                elif type(i) is type:
                    set_add(res, i)
                elif isinstance(i, Element):
                    if isinstance(i, Elements):
                        els: Elements = i
                        for j in els._el_dict.values():
                            set_add(res, type(j))
                    else:
                        set_add(res, type(i))
                else:
                    for j in self._extract_element_types(i, msg):
                        set_add(res, j)
            return res

        self._raise_argument_error(
            "%s expects instance of <'str/type/Element'>, instead got %s %r."
            % (msg, type(elements), elements)
        )

    # Accessors ----------------------------------------------------------------------------
    @cython.ccall
    def keys(self) -> tuple[str]:
        """Returns the name of the elements `<'tuple[str]'>`."""
        el: Element
        return tuple([el._name for el in self._sorted_elements()])

    @cython.ccall
    def values(self) -> tuple[Element]:
        """Returns the instance of the elements `<'tuple[Element]'>."""
        return tuple(self._sorted_elements())

    @cython.ccall
    def items(self) -> tuple[tuple[str, Element]]:
        """Returns the items of the elements `<'tuple[tuple[str, Element]]'>`."""
        el: Element
        return tuple([(el._name, el) for el in self._sorted_elements()])

    @cython.ccall
    def get(self, key: str | Element, default: object = None) -> object:
        """Get element from the collection `<'Element/Any'>`.

        :param key `<'str/Element'>`: The name/instance of the element.
        :param default `<'Any'>`: The default value to return if the element does not exist. Defaults to `None`.
        """
        if isinstance(key, str):
            return self._el_dict.get(key, default)
        else:
            return key if set_contains(self._el_set, key) else default

    @cython.cfunc
    @cython.inline(True)
    def _sorted_elements(self) -> list[Element]:
        """(internal) Returns the elements in sorted order `<'list[Element]'>`."""
        return sorted(self._el_dict.values())

    # Internal -----------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def _set_initialized(self, flag: cython.bint) -> cython.bint:
        """(internal) Set the initialized flag of the element collection."""
        Element._set_initialized(self, flag)
        if self._size > 0:
            el: Element
            for el in self._el_dict.values():
                el._set_initialized(flag)
        return True

    # Copy ---------------------------------------------------------------------------------
    @cython.ccall
    def copy(self) -> Elements:
        """Make a copy of the element collection `<'Elements'>`."""
        self._raise_not_implemented_error("copy")

    # Special Methods ----------------------------------------------------------------------
    def __getitem__(self, key: str | Element) -> Element:
        if isinstance(key, str):
            try:
                return self._el_dict[key]
            except KeyError as err:
                self._raise_not_exists_error("does not contain %r." % key, err)
        else:
            if not set_contains(self._el_set, key):
                self._raise_not_exists_error("does not contain %s." % repr(key))
            return key

    def __contains__(self, key: str | Element) -> bool:
        if isinstance(key, str):
            return dict_contains(self._el_dict, key)
        else:
            return set_contains(self._el_set, key)

    def __iter__(self) -> Iterator[Element]:
        return iter(self._sorted_elements())

    def __repr__(self) -> str:
        cls_name: str = self.__class__.__name__
        if self._size == 0:
            return "<%s {}>" % cls_name
        else:
            el: Element
            el_reprs = [
                "'%s': %s" % (el._name, el._el_repr) for el in self._sorted_elements()
            ]
            return "<%s {%s}>" % (cls_name, ", ".join(el_reprs))

    def __str__(self) -> str:
        return repr(self)

    def __bool__(self) -> bool:
        if not self._el_ready:
            try:
                self._assure_ready()
            except Exception:
                return False
        return self._size > 0

    def __len__(self) -> int:
        return self._size


# Metadata ----------------------------------------------------------------------------------------
@cython.cclass
class Metadata:
    """Represents the metadata from the remote server of an element."""

    _el_cate: str
    _meta: dict
    _size: cython.int
    _hashcode: cython.Py_ssize_t

    def __init__(self, el_cate: str, meta: dict, size: cython.int):
        """The metadata from the remote server of an element.

        :param el_cate `<str'>`: The string representation of the element category.
            Allowed values: "DATABASE", "TABLE", "COLUMN", "CONSTRAINT", "INDEX", "PARTITION".
        :param meta `<'dict'>`: The metadata of the element.
        :param size `<'int'>`: The expected size of the metadata dictionary.
        """
        self._el_cate = el_cate
        self._meta = meta
        if dict_len(meta) != size:
            self._raise_invalid_metadata_error(meta, None)
        self._size = size
        self._hashcode = -1

    # Error --------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _raise_invalid_metadata_error(
        self,
        meta: object,
        tb_exc: Exception = None,
    ) -> cython.bint:
        """(internal) Raise METADATA error.

        :param meta `<'object'>`: The metadata information.
        :param tb_exc `<'Exception/None'>`: The traceback exception. Defaults to `None`.
        """
        msg = "<'%s'> %s METADATA is invalid:\n%s." % (
            self.__class__.__name__,
            self._el_cate,
            meta,
        )
        exc = errors.map_sql_element_exc(self._el_cate, "METADATA")
        if tb_exc is None:
            raise exc(msg)
        else:
            raise exc(msg) from tb_exc

    # Accessors ----------------------------------------------------------------------------
    @cython.ccall
    def keys(self) -> tuple[str]:
        """Returns the keys of the metadata `<'tuple[str]'>`."""
        return tuple(self._meta.keys())

    @cython.ccall
    def values(self) -> tuple[object]:
        """Returns the values of the metadata `<'tuple[object]'>`."""
        return tuple(self._meta.values())

    @cython.ccall
    def items(self) -> tuple[tuple[str, object]]:
        """Returns the items of the metadata `<'tuple[str, object]'>`."""
        return tuple(self._meta.items())

    @cython.ccall
    def get(self, key: str, default: object = None) -> object:
        """Get the value of the metadata `<'Any/None'>`.

        :param key `<'str'>`: The key of the metadata.
        :param default `<'Any'>`: The default value to return if the key does not exist. Defaults to `None`.
        """
        return self._meta.get(key, default)

    # Special Methods ----------------------------------------------------------------------
    def __getitem__(self, key: str) -> object:
        return self._meta[key]

    def __contains__(self, key: str) -> bool:
        return key in self._meta

    def __iter__(self) -> Iterator[str]:
        return iter(self._meta)

    def __repr__(self) -> str:
        return repr(self._meta)

    def __str__(self) -> str:
        return str(self._meta)

    def __hash__(self) -> int:
        if self._hashcode == -1:
            self._hashcode = id(self)
        return self._hashcode

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, Metadata):
            return NotImplemented
        if self.__class__ is not o.__class__:
            return False
        _o: Metadata = o
        return _o._meta == self._meta

    def __bool__(self) -> bool:
        return self._size > 0

    def __len__(self) -> int:
        return self._size


# Query -------------------------------------------------------------------------------------------
@cython.cclass
class Query:
    """Represents the query to be executed."""

    _logs: Logs
    _sql1: str
    _sql2: str

    def __init__(self):
        """The query to be executed."""
        self._logs = Logs()
        self._sql1 = None
        self._sql2 = None

    # Property -----------------------------------------------------------------------------
    @property
    def logs(self) -> Logs:
        """The logs of the query `<'Logs'>`."""
        return self._logs

    # Execute ------------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def executable(self) -> cython.bint:
        """The flag to indicate whether the query can be executed `<'bool'>."""
        return self._sql1 is not None

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def execute(self, cur: Cursor) -> cython.bint:
        """[sync] Execute the query `<'bool'>`.

        :param cur `<'Cursor'>`: The [sync] cursor to execute the query.
        """
        if not self.executable():
            return False  # exit
        cur.execute(self._sql1)
        if self._sql2 is not None:
            cur.execute(self._sql2)
        return True  # finished

    async def aio_execute(self, cur: AioCursor) -> cython.bint:
        """[async] Execute the query `<'bool'>`.

        :param cur `<'Cursor'>`: The [async] cursor to execute the query.
        """
        if not self.executable():
            return False  # exit
        await cur.execute(self._sql1)
        if self._sql2 is not None:
            await cur.execute(self._sql2)
        return True  # finished

    # Setter -------------------------------------------------------------------------------
    @cython.ccall
    @cython.exceptval(-1, check=False)
    def set_sql(self, element: Element, sql: str) -> cython.bint:
        """Set the sql of the query to be executed `<'bool'>`.

        :param element `<'Element'>`: The element relates to the sql.
        :param sql `<'str'>`: The sql to be executed by the query.
        """
        # Validate sql
        if sql is None:
            return False
        if str_len(sql) == 0:
            return False

        # Set sql
        if self._sql1 is None:
            self._sql1 = sql
        elif self._sql2 is None:
            self._sql2 = sql
        else:
            raise AssertionError(
                "the maximum number of sqls for the `<'Query'>` class "
                "has been reached, cannot set more than 2 sql statements."
            )

        # Log sql
        self._logs.log_sql(element, sql)
        return True

    # Special Methods ----------------------------------------------------------------------
    def __repr__(self) -> str:
        if self._sql1 is None:
            return "<%s ()>" % self.__class__.__name__  # exit
        if self._sql2 is None:
            sqls = [self._sql1]
        else:
            sqls = [self._sql1, self._sql2]
        return "<%s (\n\t%s\n)>" % (self.__class__.__name__, "\n\t".join(sqls))

    def __bool__(self) -> bool:
        return self.executable()


# Logs --------------------------------------------------------------------------------------------
@cython.cclass
class Logs:
    """Represents the logs of any change messages."""

    _records: list[str]
    _skip_flag: cython.bint
    _size: cython.Py_ssize_t

    def __init__(self):
        """The logs of any change messages."""
        self._records = []
        self._skip_flag = False
        self._size = 0

    # Property -----------------------------------------------------------------------------
    @property
    def records(self) -> tuple[str]:
        """The log records `<'tuple[str]'>`."""
        return tuple(self._records)

    # Logging ------------------------------------------------------------------------------
    @cython.ccall
    def log(self, element: Element, local: cython.bint, msg: str) -> Logs:
        """Record a log message `<'Logs'>`.

        :param element `<'Element'>`: The element relates to the message.
        :param local `<'bool'>`: The change happens on local if `True`, else on remote server.
        :param msg `<'str'>`: The log message.
        """
        dt: datetime.datetime = datetime.datetime_from_timestamp(unix_time(), None)
        el_name: str = element._name
        qualified_name: str = element._tb_qualified_name
        side: str = "local" if local else "server"
        # fmt: off
        if qualified_name is None:
            if el_name is None:
                record = "[%s] %s (%s) %s |" % (
                    dt, element._el_repr, side, msg
                )
            else:
                record = "[%s] %s '%s' (%s) %s |" % (
                    dt, element._el_repr, el_name, side, msg
                )
        elif el_name is None:
            record = "[%s] %s in '%s' (%s) %s |" % (
                dt, element._el_repr, qualified_name, side, msg
            )
        elif str_contains(qualified_name, el_name):
            record = "[%s] %s '%s' (%s) %s |" % (
                dt, element._el_repr, qualified_name, side, msg
            )
        else:
            record = "[%s] %s '%s' in '%s' (%s) %s |" % (
                dt, element._el_repr, el_name, qualified_name, side, msg
            )
        # fmt: on
        list_append(self._records, record)
        self._skip_flag = False
        self._size += 1
        return self

    @cython.ccall
    def log_element_creation(self, element: Element, local: cython.bint) -> Logs:
        """Log the creation of an element `<'Logs'>`.

        :param element `<'Element'>`: The element relates to the change.
        :param local `<'bool'>`: The change happens on local if `True`, else on remote server side.
        """
        return self.log(element, local, "CREATED")

    @cython.ccall
    def log_element_deletion(self, element: Element, local: cython.bint) -> Logs:
        """Log drop of an element `<'Logs'>`.

        :param element `<'Element'>`: The element relates to the change.
        :param local `<'bool'>`: The change happens on local if `True`, else on remote server side.
        """
        return self.log(element, local, "DELETED")

    @cython.ccall
    def log_sql(self, element: Element, sql: str) -> Logs:
        """Log the query sql to be executed on remote server of the element `<'Logs'>`.

        :param element `<'Element'>`: The element relates to the sql.
        :param sql `<'str'>`: The sql to be executed.
        """
        return self.log(element, False, sql)

    @cython.ccall
    def log_charset(self, element: Element, old: Charset, new: Charset) -> Logs:
        """Log change of element [local] 'charset' `<'Logs'>`.

        :param element `<'Element'>`: The element relates to the change.
        :param old `<'Charset'>`: The old charset.
        :param new `<'Charset'>`: The new charset.
        """
        self.log_config_obj(element, "charset", old._name, new._name)
        self.log_config_obj(element, "collate", old._collation, new._collation)
        return self

    @cython.ccall
    def log_config_bool(
        self,
        element: Element,
        config_name: str,
        config_old: cython.int,
        config_new: cython.int,
    ) -> Logs:
        """Log change of element <'bool'> type [local] config `<'Logs'>`.

        :param element `<'Element'>`: The element relates to the change.
        :param config_name `<'str'>`: The name of the config.
        :param config_old `<'int'>`: The old value of the config.
        :param config_new `<'int'>`: The new value of the config.
        """
        msg: str = "%s: %s => %s" % (
            config_name,
            utils.read_bool_config(config_old),
            utils.read_bool_config(config_new),
        )
        return self.log(element, True, msg)

    @cython.ccall
    def log_config_int(
        self,
        element: Element,
        config_name: str,
        config_old: cython.longlong,
        config_new: cython.longlong,
    ) -> Logs:
        """Log change of element <'int'> type [local] config `<'Logs'>`.

        :param element `<'Element'>`: The element relates to the change.
        :param config_name `<'str'>`: The name of the config.
        :param config_old `<'int'>`: The old value of the config.
        :param config_new `<'int'>`: The new value of the config.
        """
        msg: str = "%s: %d => %d" % (config_name, config_old, config_new)
        return self.log(element, True, msg)

    @cython.ccall
    def log_config_obj(
        self,
        element: Element,
        config_name: str,
        config_old: object,
        config_new: object,
    ) -> Logs:
        """Log change of element <'object'> type [local] config `<'Logs'>`.

        :param element `<'Element'>`: The element relates to the change.
        :param config_name `<'str'>`: The name of the config.
        :param config_old `<'object'>`: The old value of the config.
        :param config_new `<'object'>`: The new value of the config.
        """
        msg: str = "%s: %r => %r" % (config_name, config_old, config_new)
        return self.log(element, True, msg)

    @cython.ccall
    def log_sync_failed_not_exist(self, element: Element) -> Logs:
        """Log sync from remote failed, because the element does not
        exist on the remote server `<'Logs'>`.

        :param element `<'Element'>`: The element relates to the sync failure.

        ## Notice
        - This method will also issue a corresponding warning message.
        """
        msg: str = "failed to sync from the remote server: it does NOT EXIST"
        element._warn(msg)
        return self.log(element, True, msg)

    @cython.ccall
    def log_sync_failed_mismatch(
        self,
        element: Element,
        config_name: str,
        config_local: object,
        config_remote: object,
    ) -> Logs:
        """Log sync from remote failed, because the critical metadata from
        the remote server does not match with the local config `<'Logs'>`.

        :param element `<'Element'>`: The element relates to the sync failure.

        ## Notice
        - This method will also issue a corresponding warning message.
        """
        msg: str = (
            "failed to sync from the remote server. Critical local config '%s' "
            "does not match with the remote server: [local] %r <=> [remote] %r"
            % (config_name, config_local, config_remote)
        )
        element._warn(msg)
        return self.log(element, True, msg)

    # Manipulate ---------------------------------------------------------------------------
    @cython.ccall
    def extend(self, logs: Logs) -> Logs:
        """Extend log records by another logs `<'Logs'>`."""
        if logs._size > 0:
            if self._size == 0:
                self._records = [i for i in logs._records]
            else:
                self._records.extend(logs._records)
            self._size += logs._size
        return self

    @cython.ccall
    def _skip(self) -> Logs:
        """(internal) Set the 'skip_flag=True` `<'Logs'>`.

        - Once logs a new record, the 'skip_flag' automatically
          sets back to `False`.
        """
        self._skip_flag = True
        return self

    # Special Methods ----------------------------------------------------------------------
    def __repr__(self) -> str:
        if self._size == 0:
            return "<%s ()>" % self.__class__.__name__
        else:
            return "<%s (\n\t%s\n)>" % (
                self.__class__.__name__,
                "\n\t".join(self._records),
            )

    def __len__(self) -> int:
        return self._size

    def __bool__(self) -> bool:
        return self._size > 0

    def __add__(self, o: object) -> Logs:
        if not isinstance(o, Logs):
            return NotImplemented
        _o: Logs = o
        logs: Logs = Logs()
        logs._records = self._records + _o._records
        return logs
