# cython: language_level=3
from __future__ import annotations

# Cython imports
import cython
from cython.cimports.cpython.time import time as unix_time  # type: ignore
from cython.cimports.cpython.int import PyInt_Check as is_int  # type: ignore
from cython.cimports.cpython.float import PyFloat_Check as is_float  # type: ignore
from cython.cimports.cpython.string import PyString_Check as is_str  # type: ignore
from cython.cimports.cpython.bytes import PyBytes_Check as is_bytes  # type: ignore
from cython.cimports.cpython.bytes import PyBytes_GET_SIZE as bytes_len  # type: ignore
from cython.cimports.cpython.set import PySet_Add as set_add  # type: ignore
from cython.cimports.cpython.set import PySet_Clear as set_clear  # type: ignore
from cython.cimports.cpython.set import PySet_GET_SIZE as set_len  # type: ignore
from cython.cimports.cpython.set import PySet_Discard as set_discard  # type: ignore
from cython.cimports.cpython.set import PySet_Contains as set_contains  # type: ignore
from cython.cimports.cpython.list import PyList_Check as is_list  # type: ignore
from cython.cimports.cpython.list import PyList_GET_SIZE as list_len  # type: ignore
from cython.cimports.cpython.list import PyList_Append as list_append  # type: ignore
from cython.cimports.cpython.tuple import PyTuple_Check as is_tuple  # type: ignore
from cython.cimports.cpython.tuple import PyTuple_GET_SIZE as tuple_len  # type: ignore
from cython.cimports.cpython.tuple import PyTuple_GetSlice as tuple_slice  # type: ignore
from cython.cimports.cpython.bytearray import PyByteArray_Check as is_bytearray  # type: ignore
from cython.cimports.mysqlengine import constant, errors, protocol, transcode  # type: ignore
from cython.cimports.mysqlengine.charset import charset_by_id, charset_by_name  # type: ignore
from cython.cimports.mysqlengine.protocol import EOFPacketWrapper, LoadLocalPacketWrapper  # type: ignore
from cython.cimports.mysqlengine.protocol import MysqlPacket, FieldDescriptorPacket, OKPacketWrapper  # type: ignore

# Python imports
from os import getpid
from math import ceil
from warnings import warn
from ssl import SSLContext
import configparser, functools, os
from typing import Any, Union, Literal, Callable
from re import compile, Match, IGNORECASE, DOTALL
from _collections_abc import dict_values, dict_keys
from struct import pack as struct_pack, unpack as struct_unpack
from socket import SOL_SOCKET, SO_KEEPALIVE, IPPROTO_TCP, TCP_NODELAY
from asyncio import CancelledError, IncompleteReadError
from asyncio import Condition, StreamWriter, WriteTransport, BaseProtocol
from asyncio import AbstractEventLoop, StreamReader, StreamReaderProtocol
from asyncio import gather, wait_for, create_task, sleep, get_running_loop
from numpy import record, ndarray
from pandas import DataFrame, Series, DatetimeIndex, TimedeltaIndex
from mysqlengine.logs import logger
from mysqlengine import constant, errors, protocol, transcode
from mysqlengine.charset import charset_by_id, charset_by_name
from mysqlengine.protocol import EOFPacketWrapper, LoadLocalPacketWrapper
from mysqlengine.protocol import MysqlPacket, FieldDescriptorPacket, OKPacketWrapper

__all__ = [
    "Cursor",
    "DictCursor",
    "DfCursor",
    "SSCursor",
    "SSDictCursor",
    "SSDfCursor",
    "Connection",
    "Pool",
    "Server",
    "CursorManager",
    "ConnectionManager",
    "TransactionManager",
    "PoolManager",
    "PoolConnectionManager",
    "PoolTransactionManager",
    "connect",
    "transaction",
    "acquire_pool",
]

# Constants ===================================================================================================
# The maximum length of data received in a packet
MAX_PACKET_LEN: cython.longlong = 2**24 - 1
#: Maximum affected rows (to represent None)
MAX_AFFECTED_ROWS: cython.longlong = 18446744073709551615
#: Max statement size which :meth:`execute_rows` generates.
#: Max size of allowed statement is max_allowed_packet - packet_header_size.
#: Default value of max_allowed_packet is 1048576.
MAX_STMT_LENGTH: cython.int = 1024000
#: Regular expression for (INSERT/REPLACE) ... (VALUES) (AS ...) (ON DUPLICATE) statements.
INSERT_REPLACE_VALUES_RE = compile(
    r"\s*((?:INSERT|REPLACE)\s.+\sVALUES?\s+)"
    + r"(\(\s*(?:%s|%\(.+\)s)\s*(?:,\s*(?:%s|%\(.+\)s)\s*)*\))"
    + r"(\s*(?:AS\s.*?)?)"
    + r"(\s*(?:ON DUPLICATE.*)?);?\s*\Z",
    IGNORECASE | DOTALL,
)
#: Quit command bytes send to the writer
QUIT_COMMAND_BYTES: bytes = struct_pack("<i", 1) + bytes([constant.COM_QUIT])
#: Client info prefix
_client_info_prefix: list[str] = ["_client_name", "mysqlengine", "_pid"]
_client_info_prefix = [i.encode("utf8") for i in _client_info_prefix]
_client_info_prefix = [struct_pack("B", bytes_len(i)) + i for i in _client_info_prefix]
CLIENT_INFO_PREFIX: bytes = b"".join(_client_info_prefix)
# Multi-rows arguments data types
MULTIROWS_ARGS_DTYPES: set[type] = {
    list,
    tuple,
    dict,
    dict_keys,
    dict_values,
    ndarray,
    record,
    DatetimeIndex,
    TimedeltaIndex,
    Series,
}


# Cursor ======================================================================================================
@cython.cclass
class Cursor:
    """Represents a cursor to interact with the database.

    All fetch*() methods return `tuple` or `tuple[tuple]` as the result.
    """

    _connection: Connection
    _host: str
    _port: cython.int
    _result: MysqlResult
    _row_index: cython.longlong
    _row_count: cython.longlong
    _last_row_id: cython.longlong
    _last_query: object
    _columns: tuple[str]
    _rows: tuple[tuple]
    _echo: cython.bint
    _warnings: cython.bint

    def __init__(
        self,
        connection: Connection,
        echo: cython.bint = False,
        warnings: cython.bint = True,
    ) -> None:
        """The Cursor to interact with the database.

        :param connection: `<Connection>` The connection the Cursor attaches to.
        :param echo: `<bool>` Whether to enable echo (log each executed queries). Defaults to `False`.
        :param warnings: `<bool>` Whether to issue any SQL related warnings. Defaults to `True`.
        """
        self._connection = connection
        self._host = connection._host
        self._port = connection._port
        self._result = None
        self._row_index = 0
        self._row_count = -1
        self._last_query = None
        self._last_row_id = 0
        self._columns = None
        self._rows = None
        self._echo = echo
        self._warnings = warnings

    # Properties ---------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def get_connection(self) -> Connection:
        "(cfunc) Get the connection that created the Cursor `<Connection>`."
        if self._connection is None:
            raise errors.QueryProgrammingError("Cursor has been closed.")
        else:
            return self._connection

    @property
    def connection(self) -> Connection:
        "The connection that created the Cursor `<Connection>`."
        return self.get_connection()

    @property
    def columns(self) -> tuple[str]:
        """The name of columns for the returned result set `<tuple[str]>`.

        This attribute will be `None` for operations that do not return
        rows or if the cursor has not had an operation invoked via the
        execute() method yet.
        """
        return self._columns

    @property
    def row_count(self) -> int:
        """The number of rows that has been affected by the query `<int>`.

        This read-only attribute specifies the number of rows that the
        last :meth:`execute` produced (for Data Query Language
        statements like SELECT) or affected (for Data Manipulation
        Language statements like UPDATE or INSERT).

        The attribute is -1 in case no .execute() has been performed
        on the cursor or the row count of the last operation if it
        can't be determined by the interface.
        """
        return self._row_count

    @property
    def row_index(self) -> int:
        """Row index `<int>`.

        This read-only attribute provides the current 0-based index of
        the cursor in the result.
        """
        return self._row_index

    @property
    def last_row_id(self) -> int:
        """This read-only property returns the value generated for an
        AUTO_INCREMENT column by the previous INSERT or UPDATE statement
        or None when there is no such value available. For example,
        if you perform an INSERT into a table that contains an AUTO_INCREMENT
        column, last_row_id returns the AUTO_INCREMENT value for the new row.
        """
        return self._last_row_id

    @property
    def echo(self) -> bool:
        "Whether to log executed queries `<bool>`."
        return self._echo

    @property
    def warnings(self) -> bool:
        "Whether to issue any SQL related warnings `<bool>`."
        return self._warnings

    @property
    def empty_result(self) -> Union[tuple, Any]:
        "The empty result set based on Cursor class `<tuple/Any>`."
        return ()

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def get_closed(self) -> cython.bint:
        "(cfunc) Get whether the connection is detached from the Cursor `<bool>`."
        if self._connection is None:
            return True
        else:
            return False

    @property
    def closed(self) -> bool:
        "Whether the connection is detached from the Cursor `<bool>`."
        return self.get_closed()

    # Execute ------------------------------------------------------------------------------------
    async def execute(self, query: str, args: Any = None) -> int:
        """Executes the given SQL query.

        `INSERT` or `REPLACE` statements will be optimized by the
        MySQL multiple rows syntax for batch operation. `UPDATE`
        or `DELETE` statements will still be executed row by row.

        :param query: <`str`> The sql statement.
        :param args: `<Any>` The arguments for the placeholders in the sql query.
        :return `<int>`: Number of rows affected by the query.
        """
        # Exectue without args
        if args is None:
            return await self._execute(query)

        # Execute single row args
        if not self.is_args_multi_rows(args):
            return await self._execute_row(query, args)

        # Optimize milti-rows operation (INSERT & REPLACE)
        match = INSERT_REPLACE_VALUES_RE.match(query)
        if match:
            return await self._execute_rows(match, args)

        # Row by row operation
        else:
            rows: cython.longlong = 0
            for arg in args:
                await self._execute_row(query, arg)
                rows += self._row_count
            self._row_count = rows
            return self._row_count

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def is_args_multi_rows(self, args: object) -> cython.bint:
        "(cfunc) Check if query arguments is multi-rows."
        if is_tuple(args) and tuple_len(args) > 0:
            if set_contains(MULTIROWS_ARGS_DTYPES, type(args[0])):
                return True
            else:
                return False
        elif is_list(args) and list_len(args) > 0:
            if set_contains(MULTIROWS_ARGS_DTYPES, type(args[0])):
                return True
            else:
                return False
        else:
            return False

    async def _execute_row(self, query: str, args: object) -> int:
        """Executes the given SQL query (single row arguments).

        :param query: <`str`> The sql statement.
        :param args: `<Any>` Single row of arguments for the placeholders in the sql query.
        :return `<int>`: Number of rows affected by the query.
        """
        # Execute
        if args is not None:
            query = query % self.get_connection()._escape_args(args)
        return await self._execute(query)

    async def _execute_rows(self, match: Match[str], args: object) -> int:
        """Executes the given SQL query (multi-row arguments).

        :param query: <`str`> The sql statement.
        :param args: `<Any>` Multi-rows of arguments for the placeholders in the sql query.
        :return `<int>`: Number of rows affected by the query.
        """
        # Get connection
        conn = self.get_connection()
        encoding: str = conn._encoding

        # Parse multi-rows syntax
        values: str = match.group(2).rstrip()
        if values[0] != "(" or values[-1] != ")":
            raise errors.QueryProgrammingError(
                "Invalid syntax for multi-rows query. Lacking parentheses for the values: %s"
                % values
            )
        prefix: bytes = (match.group(1) % ()).encode(encoding)
        temp: str = match.group(3)
        alias: bytes = temp.encode(encoding) if temp else b""
        temp: str = match.group(4)
        suffix: bytes = temp.encode(encoding) if temp else b""

        # Execute multi-rows query
        base_len: cython.int = bytes_len(prefix) + bytes_len(alias) + bytes_len(suffix)
        args = iter(args)
        sql: bytearray = bytearray(prefix)
        val: bytes = self._escape_row(values, next(args), encoding, conn)
        curr_len: cython.int = base_len + bytes_len(val)
        sql += val
        rows: cython.longlong = 0
        for arg in args:
            val = self._escape_row(values, arg, encoding, conn)
            curr_len += bytes_len(val) + 1
            if curr_len > MAX_STMT_LENGTH:
                await self._execute(sql + alias + suffix)
                rows += self._row_count
                sql = bytearray(prefix)
                curr_len = base_len + bytes_len(val)
            else:
                sql += b","
            sql += val
        await self._execute(sql + alias + suffix)
        self._row_count += rows
        return self._row_count

    async def _execute(self, query: Union[str, bytes, bytearray]) -> int:
        "Executes the Raw SQL query. Accepts: `<str>`, `<bytes>`, `<bytearray>`."
        # Exhaust last query
        while await self.next():
            pass

        # Execute query
        conn = self.get_connection()
        self._clear_result()
        await conn.query(query, False)
        self._get_result(conn)
        if self._warnings:
            await self._show_warnings(conn)

        # Record query
        self._last_query = query
        if self._echo:
            logger.info("CALL %s", query)
        return self._row_count

    @cython.cfunc
    @cython.inline(True)
    def _escape_row(
        self,
        sql: str,
        args: object,
        encoding: str,
        conn: Connection,
    ) -> bytes:
        "(cfun) Escape row of arguments `<bytes>`."
        sql = sql % conn._escape_args(args)
        return sql.encode(encoding, "surrogateescape")

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _clear_result(self):
        self._result = None
        self._row_index = 0
        self._row_count = -1
        self._last_row_id = 0
        self._columns = None
        self._rows = None

    @cython.cfunc
    @cython.inline(True)
    def _get_result(self, conn: Connection):
        if conn is None:
            conn: Connection = self.get_connection()
        self._result = result = conn._result
        self._row_index = 0
        self._row_count = result.affected_rows
        self._last_row_id = result.insert_id
        self._columns = result.columns
        self._rows = result.rows

    async def _show_warnings(self, conn: Connection = None) -> None:
        if self._result is None:
            return None
        if self._result.has_next or self._result.warning_count <= 0:
            return None

        if conn is None:
            conn: Connection = self.get_connection()
        ws = await conn.show_warnings()
        if ws is None:
            return None
        for w in ws:
            msg = w[-1]
            warn(str(msg), errors.QueryWarning, 4)

    @cython.cfunc
    @cython.inline(True)
    def _verify_executed(self):
        "(cfunc) Verify the cursor has execute any query `<None>`."
        if self._last_query is None:
            raise errors.QueryProgrammingError("execute() first")

    def mogrify(self, query: str, args: Any = None) -> str:
        """Returns the exact statement sent to the database.

        :param query: <`str`> The sql statement.
        :param args: `<Any>` The arguments for the placeholders in the sql query.
        :return `<str>`: The exact statement sent to the database.
        """
        if args is not None:
            return query % self.get_connection()._escape_args(args)
        else:
            return query

    # Fetch --------------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.wraparound(False)
    @cython.boundscheck(False)
    def _fetchone(self) -> tuple:
        "(cfunc) Fetch the next set of row."
        self._verify_executed()
        if self._rows is None or self._row_index >= tuple_len(self._rows):
            result = None
        else:
            result = self._rows[self._row_index]
            self._row_index += 1
        return result

    async def fetchone(self) -> tuple:
        """Fetch the next row.

        :return `<tuple>`: The value of the next row.
        - When no more rows are available, returns empty `tuple`.
        """
        res = self._fetchone()
        return tuple() if res is None else res

    @cython.cfunc
    @cython.inline(True)
    @cython.wraparound(False)
    @cython.boundscheck(False)
    def _fetchmany(self, rows: cython.longlong) -> tuple:
        "(cfunc) Fetch the next set of rows (multiple)."
        self._verify_executed()
        if self._rows is None:
            result = None
        else:
            length: cython.longlong = tuple_len(self._rows)
            end: cython.longlong = min(
                self._row_index + (rows if rows > 1 else 1), length
            )
            result = tuple_slice(self._rows, self._row_index, end)
            self._row_index = end
        return result

    async def fetchmany(self, rows: int = 1) -> tuple[tuple]:
        """Fetch next rows (multiple).

        :param rows: `<int>` Number of rows to return.
        :return `<tuple[tuple]>`: The values of next rows.
        - When no more rows are available, returns empty `tuple`.
        """
        res = self._fetchmany(rows)
        return tuple() if res is None else res

    @cython.cfunc
    @cython.inline(True)
    @cython.wraparound(False)
    @cython.boundscheck(False)
    def _fetchall(self) -> tuple:
        "(cfunc) Fetch all of the (remaining) rows."
        self._verify_executed()
        if self._rows is None:
            result = None
        else:
            length: cython.longlong = tuple_len(self._rows)
            if self._row_index > 0:
                result = tuple_slice(self._rows, self._row_index, length)
            else:
                result = self._rows
            self._row_index = length
        return result

    async def fetchall(self) -> tuple[tuple]:
        """Fetch all of the (remaining) rows.
        :return `<tuple[tuple]>`: The values of all the (remaining) rows.
        - When no more rows are available, returns empty `tuple`.
        """
        res = self._fetchall()
        return tuple() if res is None else res

    @cython.cfunc
    @cython.inline(True)
    def _scroll(self, value: cython.longlong, mode: str):
        "(cfunc) Scroll the cursor to a new position of the result set."
        self._verify_executed()
        if self._rows is None:
            return None

        if mode == "relative":
            value += self._row_index
        if not 0 <= value < tuple_len(self._rows):
            raise IndexError("Cursor index [%d] out of range." % value)
        self._row_index = value

    async def scroll(
        self,
        value: int,
        mode: Literal["relative", "absolute"] = "relative",
    ) -> None:
        """Scroll the cursor to a new position of the result set.

        :param value: `<int>` The value of the cursor movement.
        :param mode: `<str>` The mode of the cursor movement. Accepts:
            - `'relative'`: Value is taken as the offset to the current
              position in the result set.
            - `'absolute'`: Value states an absolute target position.
        :raises `IndexError`: If the scroll operation is out of the result set range.
        """
        self._scroll(value, mode)

    async def next(self) -> bool:
        """Move to the next result set.
        :return `<bool>`: Whether next result set exists.
        """
        # No more result set
        if self._result is None or not self._result.has_next:
            return False

        # Different result
        conn = self.get_connection()
        if self._result is not conn._result:
            return False

        # Move to next set
        self._clear_result()
        await conn.next_result()
        self._get_result(conn)
        if self._warnings:
            await self._show_warnings(conn)
        return True

    # Close --------------------------------------------------------------------------------------
    async def close(self) -> None:
        "Exhaust all remaining data and close the cursor."
        # All ready closed
        if self._connection is None:
            return None

        # Exhaust all remaining data
        try:
            while await self.next():
                pass
            self._connection = None
        except Exception:
            self._connection = None
            raise

    # Special Methods ----------------------------------------------------------------------------
    def __aiter__(self):
        return self

    async def __anext__(self):
        res = await self.fetchone()
        if res is not None:
            return res
        else:
            raise StopAsyncIteration  # noqa

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def __repr__(self) -> str:
        return "<Curosr (host='%s', port=%d)>" % (self._host, self._port)

    def __hash__(self) -> int:
        return id(self)

    def __del__(self):
        if not self.get_closed():
            self._connection = None
            self._result = None
            self._columns = None
            self._rows = None

    # Exceptions ---------------------------------------------------------------------------------
    Warning = errors.QueryWarning
    Error = errors.QueryExeError
    InterfaceError = errors.QueryInterfaceError
    DatabaseError = errors.QueryDatabaseError
    DataError = errors.QueryDataError
    OperationalError = errors.QueryOperationalError
    IntegrityError = errors.QueryIntegrityError
    InternalError = errors.QueryInternalError
    ProgrammingError = errors.QueryProgrammingError
    NotSupportedError = errors.QueryNotSupportedError


@cython.cclass
class DictCursor(Cursor):
    """Represents a cursor to interact with the database.

    All fetch*() methods return `dict` or `tuple[dict]` as the result.
    """

    # Fetch --------------------------------------------------------------------------------------
    async def fetchone(self) -> dict:
        """Fetch the next row.

        :return `<dict>`: The value of the next row.
        - When no more rows are available, returns empty `dict`.
        """
        return self._convert_row(self._fetchone())

    async def fetchmany(self, rows: int = 1) -> tuple[dict]:
        """Fetch next rows (multiple).

        :param rows: `<int>` Number of rows to return.
        :return `<tuple[dict]>`: The values of next rows.
        - When no more rows are available, returns empty `tuple`.
        """
        return self._convert_rows(self._fetchmany(rows))

    async def fetchall(self) -> tuple[dict]:
        """Fetch all of the (remaining) rows.
        :return `<tuple[dict]>`: The values of all the (remaining) rows.
        - When no more rows are available, returns empty `tuple`.
        """
        return self._convert_rows(self._fetchall())

    # Utils --------------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _convert_row(self, row: tuple) -> dict:
        if row is None or self._columns is None:
            return {}
        else:
            return dict(zip(self._columns, row))

    @cython.cfunc
    @cython.inline(True)
    def _convert_rows(self, rows: tuple) -> tuple:
        if rows is None or self._columns is None:
            return tuple()
        else:
            columns = self._columns
            lst: list = []
            for row in rows:
                lst.append(dict(zip(columns, row)))
            return tuple(lst)


@cython.cclass
class DfCursor(Cursor):
    """Represents a cursor to interact with the database.

    All fetch*() methods return `pandas.DataFrame` as the result.
    """

    # Properties ---------------------------------------------------------------------------------
    @property
    def empty_result(self) -> Union[DataFrame, Any]:
        "The empty result set based on Cursor class `<DataFrame/Any>`."
        return DataFrame()

    # Fetch --------------------------------------------------------------------------------------
    async def fetchone(self) -> DataFrame:
        """Fetch the next row.

        :return `<DataFrame>`: The value of the next row.
        - When no more rows are available, returns empty `DataFrame`.
        """
        return self._convert_row(self._fetchone())

    async def fetchmany(self, rows: int = 1) -> DataFrame:
        """Fetch next rows (multiple).

        :param rows: `<int>` Number of rows to return.
        :return `<DataFrame>`: The values of next rows.
        - When no more rows are available, returns empty `DataFrame`.
        """
        return self._convert_rows(self._fetchmany(rows))

    async def fetchall(self) -> DataFrame:
        """Fetch all of the (remaining) rows.
        :return `<DataFrame>`: The values of all the (remaining) rows.
        - When no more rows are available, returns empty `DataFrame`.
        """
        return self._convert_rows(self._fetchall())

    # Utils --------------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _convert_row(self, row: tuple) -> object:
        if self._columns is None:
            return DataFrame()
        elif row is None:
            return DataFrame([], columns=self._columns)
        else:
            return DataFrame([row], columns=self._columns)

    @cython.cfunc
    @cython.inline(True)
    def _convert_rows(self, rows: tuple) -> object:
        if self._columns is None:
            return DataFrame()
        elif rows is None:
            return DataFrame([], columns=self._columns)
        else:
            return DataFrame(rows, columns=self._columns)


@cython.cclass
class SSCursor(Cursor):
    """Represents an `UNBUFFERED` cursor to interact with the database.

    All fetch*() methods return `tuple` or `tuple[tuple]` as the result.

    Mainly useful for queries that return a lot of data, or for connections
    to remote servers over a slow network.

    Instead of copying every row of data into a buffer, this will fetch
    rows as needed. The upside of this, is the client uses much less memory,
    and rows are returned much faster when traveling over a slow network,
    or if the result set is very big.

    There are limitations, though. The MySQL protocol doesn't support
    returning the total number of rows, so the only way to tell how many rows
    there are is to iterate over every row returned. Also, it currently isn't
    possible to scroll backwards, as only the current row is held in memory.
    """

    # Execute ------------------------------------------------------------------------------------
    async def _execute(self, query: Union[str, bytes, bytearray]) -> int:
        # Exhaust last query
        while await self.next():
            pass

        # Execute query
        conn = self.get_connection()
        await conn.query(query, True)
        self._get_result(conn)
        if self._warnings:
            await self._show_warnings(conn)

        # Record query
        self._last_query = query
        if self._echo:
            logger.info("CALL %s", query)
        return self._row_count

    # Fetch --------------------------------------------------------------------------------------
    async def fetchone(self) -> tuple:
        """Fetch the next row.

        :return `<tuple>`: The value of the next row.
        - When no more rows are available, returns empty `tuple`.
        """
        self._verify_executed()
        row = await self._read_next()
        if row is None:
            return tuple()
        else:
            self._row_index += 1
            return row

    async def fetchmany(self, rows: int = 1) -> tuple[tuple]:
        """Fetch next rows (multiple).

        :param rows: `<int>` Number of rows to return.
        :return `<tuple[tuple]>`: The values of next rows.
        - When no more rows are available, returns empty `tuple`.
        """
        self._verify_executed()
        lst: list = []
        for _ in range(rows if rows > 1 else 1):
            row = await self._read_next()
            if row is None:
                break
            lst.append(row)
            self._row_index += 1
        return tuple(lst)

    async def fetchall(self) -> tuple[tuple]:
        """Fetch all of the (remaining) rows.
        :return `<tuple[tuple]>`: The values of all the (remaining) rows.
        - When no more rows are available, returns empty `tuple`.
        """
        self._verify_executed()
        lst: list = []
        while True:
            row = await self._read_next()
            if row is None:
                break
            lst.append(row)
            self._row_index += 1
        return tuple(lst)

    async def scroll(
        self,
        value: int,
        mode: Literal["relative", "absolute"] = "relative",
    ) -> None:
        """Scroll the cursor to a new position of the result set.

        Same as `Cursor.scroll`, but move cursor on server side one by one
        row. If you want to move 20 rows forward scroll will make 20 queries
        to move cursor. Currently only forward scrolling is supported.

        :param value: `<int>` The value of the cursor movement.
        :param mode: `<str>` The mode of the cursor movement. Accepts:
            - `'relative'`: Value is taken as the offset to the current
              position in the result set.
            - `'absolute'`: Value states an absolute target position.
        :raises `QueryNotSupportedError`: If try scroll backward.
        """
        self._verify_executed()
        if mode == "relative":
            if value < 0:
                raise errors.QueryNotSupportedError(
                    "Backwards scrolling not supported " "by this cursor"
                )
            for _ in range(value):
                await self._read_next()
            self._row_index += value
        else:
            if value < self._row_index:
                raise errors.QueryNotSupportedError(
                    "Backwards scrolling not supported by this cursor"
                )
            end = value - self._row_index
            for _ in range(end):
                await self._read_next()
            self._row_index = value

    async def _read_next(self) -> tuple:
        "Read the next row (internal use only)."
        return await self._result._read_row_packet_unbuffered()

    # Close --------------------------------------------------------------------------------------
    async def close(self):
        "Exhaust all remaining data and close the cursor."
        # All ready closed
        if self._connection is None:
            return None

        try:
            # Finish unbuffered query
            if self._result is not None and self._result is self._connection._result:
                await self._result.finish_unbuffered_query()
            # Exhaust all remaining data
            while await self.next():
                pass
            self._connection = None
        except Exception:
            self._connection = None
            raise


@cython.cclass
class SSDictCursor(SSCursor):
    """Represents an `UNBUFFERED` cursor to interact with the database.

    All fetch*() methods return `dict` or `tuple[dict]` as the result.

    Mainly useful for queries that return a lot of data, or for connections
    to remote servers over a slow network.

    Instead of copying every row of data into a buffer, this will fetch
    rows as needed. The upside of this, is the client uses much less memory,
    and rows are returned much faster when traveling over a slow network,
    or if the result set is very big.

    There are limitations, though. The MySQL protocol doesn't support
    returning the total number of rows, so the only way to tell how many rows
    there are is to iterate over every row returned. Also, it currently isn't
    possible to scroll backwards, as only the current row is held in memory.
    """

    # Fetch --------------------------------------------------------------------------------------
    async def fetchone(self) -> dict:
        """Fetch the next row.

        :return `<dict>`: The value of the next row.
        - When no more rows are available, returns empty `dict`.
        """
        self._verify_executed()
        if self._columns is None:
            return {}

        row = await self._read_next()
        if row is None:
            return {}

        self._row_index += 1
        return self._convert_row(self._columns, row)

    async def fetchmany(self, rows: int = 1) -> tuple[dict]:
        """Fetch next rows (multiple).

        :param rows: `<int>` Number of rows to return.
        :return `<tuple[dict]>`: The values of next rows.
        - When no more rows are available, returns empty `tuple`.
        """
        self._verify_executed()
        columns = self._columns
        if columns is None:
            return tuple()

        lst: list = []
        for _ in range(rows if rows > 1 else 1):
            row = await self._read_next()
            if row is None:
                break
            lst.append(self._convert_row(columns, row))
            self._row_index += 1
        return tuple(lst)

    async def fetchall(self) -> tuple[dict]:
        """Fetch all of the (remaining) rows.
        :return `<tuple[dict]>`: The values of all the (remaining) rows.
        - When no more rows are available, returns empty `tuple`.
        """
        self._verify_executed()
        columns: tuple = self._columns
        if columns is None:
            return tuple()

        lst: list = []
        while True:
            row = await self._read_next()
            if row is None:
                break
            lst.append(self._convert_row(columns, row))
            self._row_index += 1
        return tuple(lst)

    # Utils --------------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _convert_row(self, columns: object, row: object) -> dict:
        return dict(zip(columns, row))


@cython.cclass
class SSDfCursor(SSCursor):
    """Represents an `UNBUFFERED` cursor to interact with the database.

    All fetch*() methods return `pandas.DataFrame` as the result.

    Mainly useful for queries that return a lot of data, or for connections
    to remote servers over a slow network.

    Instead of copying every row of data into a buffer, this will fetch
    rows as needed. The upside of this, is the client uses much less memory,
    and rows are returned much faster when traveling over a slow network,
    or if the result set is very big.

    There are limitations, though. The MySQL protocol doesn't support
    returning the total number of rows, so the only way to tell how many rows
    there are is to iterate over every row returned. Also, it currently isn't
    possible to scroll backwards, as only the current row is held in memory.
    """

    # Properties ---------------------------------------------------------------------------------
    @property
    def empty_result(self) -> Union[DataFrame, Any]:
        "The empty result set based on Cursor class `<DataFrame/Any>`."
        return DataFrame()

    # Fetch --------------------------------------------------------------------------------------
    async def fetchone(self) -> DataFrame:
        """Fetch the next row.

        :return `<DataFrame>`: The value of the next row.
        - When no more rows are available, returns empty `DataFrame`.
        """
        self._verify_executed()
        if self._columns is None:
            return DataFrame()

        row = await self._read_next()
        if row is None:
            return DataFrame([], columns=self._columns)

        self._row_index += 1
        return DataFrame([row], columns=self._columns)

    async def fetchmany(self, rows: int = 1) -> DataFrame:
        """Fetch next rows (multiple).

        :param rows: `<int>` Number of rows to return.
        :return `<DataFrame>`: The values of next rows.
        - When no more rows are available, returns empty `DataFrame`.
        """
        self._verify_executed()
        if self._columns is None:
            return DataFrame()

        lst: list = []
        for _ in range(rows if rows > 1 else 1):
            row = await self._read_next()
            if row is None:
                break
            lst.append(row)
            self._row_index += 1
        return DataFrame(lst, columns=self._columns)

    async def fetchall(self) -> DataFrame:
        """Fetch all of the (remaining) rows.
        :return `<DataFrame>`: The values of all the (remaining) rows.
        - When no more rows are available, returns empty `DataFrame`.
        """
        self._verify_executed()
        if self._columns is None:
            return DataFrame()

        lst: list = []
        while True:
            row = await self._read_next()
            if row is None:
                break
            lst.append(row)
            self._row_index += 1
        return DataFrame(lst, columns=self._columns)


# Connection ===================================================================================
class ConnectionReader(StreamReader):
    "The StreamReader for connection."

    def __init__(self, loop: AbstractEventLoop = None):
        self._eof_received: cython.bint = False
        super().__init__(loop=loop)

    def feed_eof(self) -> None:
        try:
            self._eof_received = True
            super().feed_eof()
        except Exception as err:
            errors.raise_exc(err)

    @property
    def eof_received(self) -> bool:
        return self._eof_received


class ConnectionWriter(StreamWriter):
    "The StreamWriter for connection."

    def __init__(
        self,
        transport: WriteTransport,
        protocol: BaseProtocol,
        reader: Union[StreamReader, None],
        loop: AbstractEventLoop,
    ) -> None:
        super().__init__(transport, protocol, reader, loop)


@cython.cclass
class Connection:
    """Represents a socket connection to the MySQL Server.

    This class serves as the base connection class. It does not perform
    argument validation during initialization. Such validation is
    delegated to the `_Connection` subclass and the `Pool` class. This
    design helps to minimize redundant validation when the `Pool` spawns
    new connections.

    Do `NOT` create an instance of this class directly. The proper
    way to get an instance of `Connection` is to call `connect()`,
    `Pool.acquire()` or `Server.acquire()` through `async with`.
    """

    # Client
    _last_usage: cython.double
    _host: str
    _port: cython.int
    _user: str
    _password: bytes
    _db: str
    # Auth
    _client_auth_plugin: str
    _server_auth_plugin: str
    _auth_plugin_used: str
    _server_public_key: bytes
    _unix_socket: str
    _ssl: cython.bint
    _ssl_context: SSLContext
    _secure: cython.bint
    # Settings
    _charset: str
    _encoding: str
    _sql_mode: str
    _init_command: str
    _autocommit: cython.bint
    _connect_timeout: cython.int
    _cursorclass: type[Cursor]
    _client_info: bytes
    _client_flag: cython.int
    _echo: cython.bint
    # Reader & Writer
    _reader: ConnectionReader
    _writer: ConnectionWriter
    # Query
    _result: MysqlResult
    _affected_rows: cython.longlong
    _host_info: str
    _salt: bytes
    _close_reason: str
    _reusable: cython.bint
    _next_seq_id: cython.longlong
    _protocol_version: cython.int
    _server_version: str
    _server_version_major: cython.int
    _server_thread_id: cython.int
    _server_capabilities: cython.int
    _server_language: cython.int
    _server_charset: str
    _server_status: cython.int

    def __init__(
        self,
        host: str,
        port: cython.int,
        user: str,
        password: bytes,
        database: str,
        auth_plugin: str,
        server_public_key: bytes,
        unix_socket: str,
        ssl: cython.bint,
        ssl_context: SSLContext,
        charset: str,
        encoding: str,
        sql_mode: str,
        init_command: str,
        autocommit: cython.bint,
        connect_timeout: cython.int,
        cursorclass: type[Cursor],
        program_name: str,
        client_flag: cython.int,
        echo: cython.bint,
    ) -> None:
        """Connection with the MySQL Server.

        :param host: `<str>` Host of the database Server.
        :param port: `<int>` Port of the database Server.
        :param user: `<str>` The username to login as.
        :param password: `<bytes>` The password for user authentication.
        :param database: `<str>` Specific database to use.
        :param auth_plugin: `<str>` The authorization plugin to be used.
        :param server_public_key: `<bytes>`  SHA256 authentication plugin public key value.
        :param unix_socket: `<str>` Use the provide unix socket to establish the connection.
        :param ssl: `<bool>` Whether to establish the connection with ssl.
        :param ssl_context: `<ssl.SSLContext>` Force to establish the connection with ssl.
        :param charset: `<str>` The charset for the connection.
        :param encoding: `<str>` The encoding for the connection.
        :param sql_mode: `<str>` The default SQL mode for the connection.
        :param init_command: `<str>` The initial command to be executed once connection is established.
        :param autocommit: `<bool>` Whether to auto commit after each query execution.
        :param connect_timeout: `<int>` Timeout in seconds for establishing the connection.
        :param cursorclass: `<type[Cursor]>` The Cursor class to be used.
        :param program_name: `<str>` The program name to be used while handshaking.
        :param client_flag: `<int>` Custom flags to send to MySQL.
        :param echo: `<bool>` Whether to enable echo (log each executed queries).
        """
        # Asyncio
        self._last_usage = unix_time()
        # Client
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._db = database
        # Auth
        self._client_auth_plugin = auth_plugin
        self._server_auth_plugin = None
        self._auth_plugin_used = None
        self._server_public_key = server_public_key
        self._unix_socket = unix_socket
        self._ssl = ssl
        self._ssl_context = ssl_context
        self._secure = False
        # Settings
        self._charset = charset
        self._encoding = encoding
        self._sql_mode = sql_mode
        self._init_command = init_command
        self._autocommit = autocommit
        self._connect_timeout = connect_timeout
        self._cursorclass = cursorclass
        self._client_info = self._gen_client_info(program_name)
        self._client_flag = client_flag
        self._echo = echo
        # Reader & Writer
        self._reader = None
        self._writer = None
        # Query
        self._result = None
        self._affected_rows = 0
        self._host_info = "Not connected"
        self._salt = None
        self._close_reason = None
        self._reusable = True
        # Server
        self._protocol_version = 0
        self._server_version = None
        self._server_version_major = 5
        self._server_thread_id = 0

    # Properties ---------------------------------------------------------------------------------
    @property
    def host(self) -> str:
        "Host of the database Server `<str>`."
        return self._host

    @property
    def port(self) -> int:
        "Port of the database Server `<int>`."
        return self._port

    @property
    def user(self) -> str:
        "The username logged in as `<str>`."
        return self._user or None

    @property
    def database(self) -> str:
        "Specific database being used `<str>`."
        return self._db or None

    @property
    def unix_socket(self) -> str:
        "The unix socket been used instead of TCP/IP `<str>`."
        return self._unix_socket or None

    @property
    def ssl_context(self) -> SSLContext:
        "SSL Context been used to force ssl connection `<ssl.SSLContext>`."
        return self._ssl_context

    @property
    def charset(self) -> str:
        "The charset being used by the connection `<str>`."
        return self._charset

    async def set_charset(self, charset: str) -> None:
        """Set the character for the current connection.

        :param charset: `<str>` The charset to be used.
        """
        # Make sure charset is supported.
        encoding: str = charset_by_name(charset).get_encoding()
        await self._execute_command(
            constant.COM_QUERY, "SET NAMES %s" % self._escape_item(charset)
        )
        await self._read_packet()
        self._charset = charset
        self._encoding = encoding

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def get_autocommit(self) -> cython.bint:
        "(cfunc) Whether autocommit is enabled `<bool>`."
        if self._server_status & constant.SERVER_STATUS_AUTOCOMMIT:
            return True
        else:
            return False

    @property
    def autocommit(self) -> bool:
        "Whether autocommit is enabled `<bool>`."
        return self.get_autocommit()

    async def set_autocommit(self, auto: cython.bint) -> None:
        """Set autocommit mode for the connection.

        :param auto: `<bool>` Whether to enable autocommit.
        """
        self._autocommit = auto
        curr = self.get_autocommit()
        if auto != curr:
            await self._execute_command(
                constant.COM_QUERY, "SET AUTOCOMMIT = %d" % self._autocommit
            )
            await self._read_ok_packet()

    @property
    def encoding(self) -> str:
        "Encoding employed for the connection. `<str>`."
        return self._encoding

    @property
    def host_info(self) -> str:
        "The host information of the connection `<str>`."
        return self._host_info

    @property
    def version(self) -> str:
        "The Mysql Server version `<str>`."
        return self._server_version

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def get_version_major(self) -> cython.int:
        "(cfunc) Get the `MAJOR` Mysql Server version `<int>`."
        return self._server_version_major

    @property
    def version_major(self) -> int:
        "The `MAJOR` Mysql Server version `<int>`."
        return self.get_version_major()

    @property
    def protocol(self) -> int:
        "The MySQL Server protocol version `<int>`."
        return self._protocol_version

    @property
    def thread_id(self) -> int:
        "The thread id of the connection `<int>`."
        return self._server_thread_id

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def get_insert_id(self) -> cython.longlong:
        "(cfunc) Get the last insert id `<int>`."
        if self._result is not None:
            return self._result.insert_id
        else:
            return 0

    @property
    def insert_id(self) -> int:
        "The last insert id `<int>`."
        return self.get_insert_id()

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def get_backslash_escapes(self) -> cython.bint:
        "(cfunc) Get whether the connection use `BACKSLASH_ESCAPES` `<bool>`."
        if self._server_status & constant.SERVER_STATUS_NO_BACKSLASH_ESCAPES:
            return False
        else:
            return True

    @property
    def backslash_escapes(self) -> bool:
        "Whether the connection use `BACKSLASH_ESCAPES` `<bool>`."
        return self.get_backslash_escapes()

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def get_reusable(self) -> cython.bint:
        "(cfunc) Get whether the connection is reusable by the Pool `<bool>`."
        return self._reusable

    @property
    def reusable(self) -> bool:
        """Whether the connection is reusable by the Pool `<bool>`.
        (Only relavent if the conneciton is acquired from a `Pool`.)

        - If `True`, after returning to the Pool, the connection will
          stay persistend until next acquisition or closed by the
          recycling mechanism.
        - If `False`, after returning to the Pool, the connection
          will be closed. This is useful for certain types of queries,
          such as `CREATE TEMPORARY TABLE` and `LOCK TABLES`, when you
          want to ensure the connection will be closed and release all
          the resources at the end.
        """
        return self.get_reusable()

    def set_reusable(self, reuse: cython.bint) -> None:
        """Set `reusable` state for the connection.
        (Only relavent if the conneciton is acquired from a `Pool`.)

        :param reuse: Whether the connection is resuable.
        - If `True`, after returning to the Pool, the connection will
          stay persistend until next acquisition or closed by the
          recycling mechanism.
        - If `False`, after returning to the Pool, the connection
          will be closed. This is useful for certain types of queries,
          such as `CREATE TEMPORARY TABLE` and `LOCK TABLES`, when you
          want to ensure the connection will be closed and release all
          the resources at the end.

        ### Notice
        Once `reusable` set to `False`, the state cannot revert back to `True`.
        """
        if reuse or not self._reusable:
            return
        self._reusable = False

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def get_last_usage(self) -> cython.double:
        "(cfunc) Get the last time when the connection was used `<float>`."
        return self._last_usage

    @property
    def last_usage(self) -> float:
        "Last time when the connection was used `<float>`."
        return self.get_last_usage()

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def get_transaction_status(self) -> cython.bint:
        "(cfunc) Get the transaction status `<bool>`."
        if self._server_status & constant.SERVER_STATUS_IN_TRANS:
            return True
        else:
            return False

    @property
    def transaction_status(self) -> bool:
        "The Transaction status `<bool>`."
        return self.get_transaction_status()

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def get_affected_rows(self) -> cython.longlong:
        "(cfunc) Get the number of rows affected by the query `<int>`."
        return self._affected_rows

    @property
    def affected_rows(self) -> int:
        "Number of rows affected by the query `<int>`."
        return self.get_affected_rows()

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def get_closed(self) -> cython.bint:
        "(cfunc) Get whether the connection is closed `<bool>`."
        if self._writer is None:
            return True
        else:
            return False

    @property
    def closed(self) -> bool:
        "Whether the connection is closed `< bool>`."
        return self.get_closed()

    # Query --------------------------------------------------------------------------------------
    async def begin(self) -> None:
        "Begin transaction."
        await self._execute_command(constant.COM_QUERY, "BEGIN")
        await self._read_ok_packet()

    async def start(self) -> None:
        "Start transaction. Alias `for begin()`"
        await self._execute_command(constant.COM_QUERY, "BEGIN")
        await self._read_ok_packet()

    async def commit(self) -> None:
        "Commit changes to stable storage."
        await self._execute_command(constant.COM_QUERY, "COMMIT")
        await self._read_ok_packet()

    async def rollback(self) -> None:
        "Roll back the current transaction."
        await self._execute_command(constant.COM_QUERY, "ROLLBACK")
        await self._read_ok_packet()

    async def select_db(self, database: str) -> None:
        "Set current database to be used."
        await self._execute_command(constant.COM_INIT_DB, database)
        await self._read_ok_packet()

    async def show_warnings(self) -> tuple:
        "Fetch result from `SHOW WARNINGS` statement `<tuple>`."
        await self._execute_command(constant.COM_QUERY, "SHOW WARNINGS")
        result = MysqlResult(self)
        await result.read()
        return result.rows

    @cython.cfunc
    @cython.inline(True)
    def _escape_item(self, obj: object) -> str:
        "(cfunc) Escape item to literal `<str>`."
        return transcode.encode_item(obj, self.get_backslash_escapes())

    def escape_tiem(self, obj: Any) -> str:
        "Escape item to literal `<str>`."
        return self._escape_item(obj)

    @cython.cfunc
    @cython.inline(True)
    def _escape_args(self, args: object) -> object:
        """(cfunc) Escape arguments to literal `<tuple/dict>`.

        - If the given 'args' is type of `<dict>`, returns `<dict>`.
        - All other supported data types returns `<tuple>`.
        """
        return transcode.encode_args(args, self.get_backslash_escapes())

    def escape_args(self, args: Any) -> Union[tuple, dict]:
        """Escape arguments to literal `<tuple/dict>`.

        - If the given 'args' is type of `<dict>`, returns `<dict>`.
        - All other supported data types returns `<tuple>`.
        """
        return self._escape_args(args)

    # Cursor -------------------------------------------------------------------------------------
    def cursor(
        self,
        cursor: type[Cursor] = None,
        warnings: cython.bint = True,
    ) -> CursorManager:
        """Create a `Cursor` to interact with the Database (Context Manager).

        :param cursor: `<type>` custom cursor class. Defaults to `None`.
        :param warnings: `<bool`> Whether to issue any `SQL` related warnings. Defaults to `True`.
        :raises `TypeError`: If the provided 'cursor' is not a subclass of `<Cursor>`.
        :return `CursorManager`: Cursor wrapped by context manager.
        """
        self._ensure_alive()
        self._last_usage = unix_time()
        cur: Cursor
        if cursor is not None:
            if not issubclass(cursor, Cursor):
                raise TypeError("Custom `cursor` must be subclass of `<Cursor>`.")
            cur = cursor(self, self._echo, warnings=warnings)
        else:
            cur = self._cursorclass(self, self._echo, warnings=warnings)
        return CursorManager(cur)

    async def query(
        self,
        sql: Union[str, bytes, bytearray],
        unbuffered: cython.bint = False,
    ) -> int:
        """Execute the SQL query. (interal use by the `Cursor` class)

        :param sql: `<str>/<bytes>` The SQL statement.
        :param unbuffered: `<bool>` Whether to read the query result in unbuffered way.
        :return: `<int>` Number of rows affected by the query.
        """
        await self._execute_command(constant.COM_QUERY, sql)
        await self._read_query_result(unbuffered=unbuffered)
        return self._affected_rows

    async def next_result(self) -> int:
        """Move to the next query result set. (internal use by the `Cursor` class)

        :return: `<int>` Number of rows affected by the query.
        """
        await self._read_query_result()
        return self._affected_rows

    # Connect ------------------------------------------------------------------------------------
    async def connect(self) -> None:
        "Establish a socket connection to the MySQL Server. (Do not call this method directly.)"
        # TODO: Set close callback
        # raise SQLQueryOperationalError(CR.CR_SERVER_GONE_ERROR,
        # "MySQL server has gone away (err)"
        try:
            # Open socket connection
            if self._unix_socket:
                await wait_for(
                    self._open_unix_connection(self._unix_socket),
                    timeout=self._connect_timeout,
                )
                self._host_info = "Localhost via UNIX socket: " + self._unix_socket
                self._secure = True
            else:
                await wait_for(
                    self._open_connection(self._host, self._port),
                    timeout=self._connect_timeout,
                )
                self._set_keep_alive()
                self._set_nodelay()
                self._host_info = "socket %s:%d" % (self._host, self._port)
            self._next_seq_id = 0

            # Handshake
            packet: MysqlPacket = await self._read_packet()
            self._get_server_information(packet)
            await self._request_auth()

            # Initial settings
            if self._sql_mode:
                await self.query("SET sql_mode=%s" % self._sql_mode)
            if self._init_command:
                await self.query(self._init_command)
                await self.commit()
            if self._autocommit is not None:
                await self.set_autocommit(self._autocommit)

        except Exception as err:
            if self._writer is not None:
                self._writer.transport.close()
            self._reader = None
            self._writer = None
            raise errors.QueryOperationalError(
                2003,
                "Can't connect to MySQL server on %s.\nError: %s" % (self._host, err),
            ) from err

    async def _open_connection(
        self,
        host: str = None,
        port: object = None,
        **kwargs,
    ) -> None:
        """Open a socket connection to MySQL server using TCP/IP.

        This is based on asyncio.open_connection, allowing the use
        the custom StreamReader (ConnectionReader).

        :param host: `<str>` Host of the socket. Defaults to `'None'`.
        :param port: `<int>` Port of the socket. Defaults to `'None'`.
        """
        loop = get_running_loop()
        reader = ConnectionReader(loop=loop)
        protocol = StreamReaderProtocol(reader, loop=loop)
        transport, _ = await loop.create_connection(
            lambda: protocol, host, port, **kwargs
        )
        writer = ConnectionWriter(transport, protocol, reader, loop)
        self._reader = reader
        self._writer = writer

    async def _open_unix_connection(
        self,
        path: str = None,
        **kwargs,
    ) -> None:
        """Open a socket connection to MySQL server using UNIX socket.

        This is based on asyncio.open_unix_connection, allowing the use
        the custom StreamReader (ConnectionReader).

        :param path: `<str>` Path of the UNIX socket. Defaults to `'None'`.
        """
        loop = get_running_loop()
        reader = ConnectionReader(loop=loop)
        protocol = StreamReaderProtocol(reader, loop=loop)
        transport, _ = await loop.create_unix_connection(
            lambda: protocol, path, **kwargs
        )
        writer = ConnectionWriter(transport, protocol, reader, loop)
        self._reader = reader
        self._writer = writer

    def _set_keep_alive(self) -> None:
        "Set the connection socket to `keep alive`."
        transport = self._writer.transport
        transport.pause_reading()
        raw_sock = transport.get_extra_info("socket", default=None)
        if raw_sock is None:
            raise RuntimeError("Transport does not expose socket instance")
        raw_sock.setsockopt(SOL_SOCKET, SO_KEEPALIVE, 1)
        transport.resume_reading()

    def _set_nodelay(self) -> None:
        "Set the connection socket to `nodelay`."
        transport = self._writer.transport
        transport.pause_reading()
        raw_sock = transport.get_extra_info("socket", default=None)
        if raw_sock is None:
            raise RuntimeError("Transport does not expose socket instance")
        raw_sock.setsockopt(IPPROTO_TCP, TCP_NODELAY, True)
        transport.resume_reading()

    @cython.cfunc
    @cython.inline(True)
    def _get_server_information(self, packet: MysqlPacket):
        # Get Data
        data: bytes = packet.get_data()

        # Protocol version
        i: cython.int = 0
        self._protocol_version = data[i]
        i += 1

        # Server version
        server_end: cython.int = data.find(b"\0", i)
        self._server_version = data[i:server_end].decode("latin1")
        self._server_version_major = int(self._server_version.split(".", 1)[0])
        i = server_end + 1

        # Thread id
        self._server_thread_id = struct_unpack("<I", data[i : i + 4])[0]
        i += 4

        # Salt
        self._salt = data[i : i + 8]
        i += 9  # 8 + 1(filler)

        # Capabilities
        self._server_capabilities = struct_unpack("<H", data[i : i + 2])[0]
        i += 2

        # Get data length
        length: cython.int = bytes_len(data)

        # Language, Charset, Status & Capabilities
        lang: cython.int
        stat: cython.int
        cap_h: cython.int
        salt_len: cython.int
        if length >= i + 6:
            lang, stat, cap_h, salt_len = struct_unpack("<BHHB", data[i : i + 6])
            i += 6

            self._server_language = lang
            try:
                self._server_charset = charset_by_id(lang)._name
            except KeyError:
                self._server_charset = None
            self._server_status = stat
            self._server_capabilities |= cap_h << 16
            salt_len = max(12, salt_len - 9)
            i += 10

            # Addition Salt
            if length >= i + salt_len:
                # salt_len includes auth_plugin_data_part_1 and filler
                self._salt += data[i : i + salt_len]
                i += salt_len
            i += 1
        else:
            i += 11

        # AUTH PLUGIN NAME may appear here.
        if self._server_capabilities & constant.CLIENT_PLUGIN_AUTH and length >= i:
            # Due to Bug#59453 the auth-plugin-name is missing the terminating
            # NUL-char in versions prior to 5.5.10 and 5.6.2.
            # ref: https://dev.mysql.com/doc/internals/en/
            # connection-phase-packets.html#packet-Protocol::Handshake
            # didn't use version checks as mariadb is corrected and reports
            # earlier than those two.
            server_end: cython.int = data.find(b"\0", i)
            if server_end < 0:  # pragma: no cover - very specific upstream bug
                # not found \0 and last field so take it all
                self._server_auth_plugin = data[i:].decode("latin1")
            else:
                self._server_auth_plugin = data[i:server_end].decode("latin1")

    async def _request_auth(self) -> None:
        # https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::HandshakeResponse
        if self._server_version_major >= 5:
            self._client_flag |= constant.CLIENT_MULTI_RESULTS
        data: bytes = struct_pack(
            "<iIB23s",
            self._client_flag,
            MAX_PACKET_LEN,
            charset_by_name(self._charset)._id,
            b"",
        )

        # Force SSL connection
        if self._ssl and self._server_capabilities & constant.CLIENT_SSL:
            self.write_packet(data)

            # Stop sending events to data_received
            self._writer.transport.pause_reading()

            # Get the raw socket from the transport
            raw_sock = self._writer.transport.get_extra_info("socket", default=None)
            if raw_sock is None:
                raise RuntimeError("Transport does not expose socket instance")

            raw_sock = raw_sock.dup()
            self._writer.transport.close()
            # MySQL expects TLS negotiation to happen in the middle of a
            # TCP connection not at start. Passing in a socket to
            # open_connection will cause it to negotiate TLS on an existing
            # connection not initiate a new one.
            await self._open_connection(
                sock=raw_sock, ssl=self._ssl_context, server_hostname=self._host
            )
            self._secure = True

        # Username
        if not self._user:
            raise ValueError("Did not specify a username")
        _user: bytes = self._user.encode(self._encoding)
        data += _user + b"\0"

        # Password
        auth_plugin: str
        authresp: bytes
        header: bytes
        if self._client_auth_plugin:
            auth_plugin = self._client_auth_plugin
        else:
            auth_plugin = self._server_auth_plugin
        # . native password
        if not auth_plugin or auth_plugin == "mysql_native_password":
            authresp = protocol.scramble_native_password(self._password, self._salt)
        # . old password
        elif auth_plugin == "caching_sha2_password":
            if self._password:
                authresp = protocol.scramble_caching_sha2(self._password, self._salt)
            else:
                authresp = b""  # empty password
        # . SHA256 password
        elif auth_plugin == "sha256_password":
            if self._ssl and self._server_capabilities & constant.CLIENT_SSL:
                authresp = self._password + b"\0"
            elif self._password:
                authresp = b"\1"  # request public key
            else:
                authresp = b"\0"  # empty password
        # . clear password
        elif auth_plugin == "mysql_clear_password":
            authresp = self._password + b"\0"
        # . empty password
        else:
            authresp = b""

        if self._server_capabilities & constant.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA:
            data += self._lenenc_int(bytes_len(authresp)) + authresp
        elif self._server_capabilities & constant.CLIENT_SECURE_CONNECTION:
            header = struct_pack("B", bytes_len(authresp))
            data += header + authresp
        else:  # pragma: no cover
            # not testing against servers without secure auth (>=5.0)
            data += authresp + b"\0"

        # Database
        if self._db and self._server_capabilities & constant.CLIENT_CONNECT_WITH_DB:
            db: bytes = self._db.encode(self._encoding)
            data += db + b"\0"

        # Auth plugin
        if self._server_capabilities & constant.CLIENT_PLUGIN_AUTH:
            name: bytes = auth_plugin.encode("ascii")
            data += name + b"\0"
        self._auth_plugin_used = auth_plugin

        # Client info
        if self._server_capabilities & constant.CLIENT_CONNECT_ATTRS:
            header = struct_pack("B", bytes_len(self._client_info))
            data += header + self._client_info

        # Send handshake response packet
        self.write_packet(data)
        auth_packet: MysqlPacket = await self._read_packet()

        # if authentication method isn't accepted the first byte
        # will have the octet 254
        if auth_packet.is_auth_switch_request():
            # https://dev.mysql.com/doc/internals/en/
            # connection-phase-packets.html#packet-Protocol::AuthSwitchRequest
            auth_packet.read_uint8()  # 0xfe packet identifier
            plugin_name: bytes = auth_packet.read_string()
            if (
                self._server_capabilities & constant.CLIENT_PLUGIN_AUTH
                and plugin_name is not None
            ):
                await self._process_auth(plugin_name.decode(), auth_packet)
            else:
                raise errors.QueryOperationalError(
                    2059,
                    "Authentication plugin '{0}'" " not supported.".format(plugin_name),
                )
        elif auth_packet.is_extra_auth_data():
            if auth_plugin == "caching_sha2_password":
                await self._auth_caching_sha2_password(auth_packet)
            elif auth_plugin == "sha256_password":
                await self._auth_sha256_password(auth_packet)
            else:
                raise errors.QueryOperationalError(
                    "Received extra packet " "for auth method %r", auth_plugin
                )

    async def _process_auth(self, plugin_name: str, auth_packet: MysqlPacket) -> None:
        # These auth plugins do their own packet handling
        if plugin_name == "caching_sha2_password":
            await self._auth_caching_sha2_password(auth_packet)
            self._auth_plugin_used = plugin_name
        elif plugin_name == "sha256_password":
            await self._auth_sha256_password(auth_packet)
            self._auth_plugin_used = plugin_name
        else:
            if plugin_name == "mysql_native_password":
                # https://dev.mysql.com/doc/internals/en/
                # secure-password-authentication.html#packet-Authentication::
                # Native41
                data = protocol.scramble_native_password(
                    self._password, auth_packet.read_all()
                )
            elif plugin_name == "mysql_clear_password":
                # https://dev.mysql.com/doc/internals/en/
                # clear-text-authentication.html
                data = self._password + b"\0"
            else:
                raise errors.QueryOperationalError(
                    2059,
                    "Authentication plugin '{0}'" " not supported.".format(plugin_name),
                )
            self.write_packet(data)
            pkt: MysqlPacket = await self._read_packet()
            pkt.check_error()
            self._auth_plugin_used = plugin_name

    async def _auth_caching_sha2_password(self, pkt: MysqlPacket) -> None:
        # Fast path - no password
        if not self._password:
            self.write_packet(b"")
            pkt = await self._read_packet()
            pkt.check_error()
            return None

        if pkt.is_auth_switch_request():
            # Try from fast auth
            logger.debug("caching sha2: Trying fast path")
            self._salt = pkt.read_all()
            scrambled = protocol.scramble_caching_sha2(self._password, self._salt)
            self.write_packet(scrambled)
            pkt = await self._read_packet()
            pkt.check_error()

        # else: fast auth is tried in initial handshake
        if not pkt.is_extra_auth_data():
            raise errors.QueryOperationalError(
                "caching sha2: Unknown packet "
                "for fast auth: {0}".format(pkt.get_data()[:1])
            )

        # magic numbers:
        # 2 - request public key
        # 3 - fast auth succeeded
        # 4 - need full auth

        pkt.advance(1)
        n: cython.int = pkt.read_uint8()
        if n == 3:
            logger.debug("caching sha2: succeeded by fast path.")
            pkt = await self._read_packet()
            pkt.check_error()  # pkt must be OK packet
            return None
        if n != 4:
            raise errors.QueryOperationalError(
                "caching sha2: Unknown " "result for fast auth: {0}".format(n)
            )
        logger.debug("caching sha2: Trying full auth...")

        if self._secure:
            logger.debug(
                "caching sha2: Sending plain " "password via secure connection"
            )
            self.write_packet(self._password + b"\0")
            pkt = await self._read_packet()
            pkt.check_error()
            return None

        if not self._server_public_key:
            self.write_packet(b"\x02")
            pkt = await self._read_packet()  # Request public key
            pkt.check_error()
            if not pkt.is_extra_auth_data():
                raise errors.QueryOperationalError(
                    "caching sha2: Unknown packet "
                    "for public key: {0}".format(pkt.get_data()[:1])
                )
            self._server_public_key = pkt.get_data()[1:]
            logger.debug(self._server_public_key.decode("ascii"))

        data = protocol.sha2_rsa_encrypt(
            self._password, self._salt, self._server_public_key
        )
        self.write_packet(data)
        pkt = await self._read_packet()
        pkt.check_error()

    async def _auth_sha256_password(self, pkt: MysqlPacket) -> None:
        if self._secure:
            logger.debug("sha256: Sending plain password")
            data = self._password + b"\0"
            self.write_packet(data)
            pkt = await self._read_packet()
            pkt.check_error()
            return None

        if pkt.is_auth_switch_request():
            self._salt = pkt.read_all()
            if not self._server_public_key and self._password:
                # Request server public key
                logger.debug("sha256: Requesting server public key")
                self.write_packet(b"\1")
                pkt = await self._read_packet()
                pkt.check_error()

        if pkt.is_extra_auth_data():
            self._server_public_key = pkt.get_data()[1:]
            logger.debug(
                "Received public key:\n", self._server_public_key.decode("ascii")
            )

        if self._password:
            if not self._server_public_key:
                raise errors.QueryOperationalError(
                    "Couldn't receive server's public key"
                )
            data = protocol.sha2_rsa_encrypt(
                self._password, self._salt, self._server_public_key
            )
        else:
            data = b""
        self.write_packet(data)
        pkt = await self._read_packet()
        pkt.check_error()

    # Execute ------------------------------------------------------------------------------------
    async def _execute_command(
        self,
        command: cython.int,
        sql: Union[str, bytes, bytearray],
    ) -> None:
        self._ensure_alive()
        # If the last query was unbuffered, make sure it finishes before
        # sending new commands
        if self._result is not None:
            if self._result.unbuffered_active:
                await self._result.finish_unbuffered_query()
            while self._result.has_next:
                await self.next_result()
            self._result = None

        # Validate command
        sql_: bytes
        if is_str(sql):
            sql_ = sql.encode(self._encoding, "surrogateescape")
        elif is_bytearray(sql):
            sql_ = bytes(sql)
        elif is_bytes(sql):
            sql_ = sql
        else:
            raise TypeError(
                "Query sql must be type of `<str>/<bytes>/<bytearray>`, "
                "instead of {}".format(type(sql))
            )

        # Send query command
        chunk_size: cython.longlong = min(
            MAX_PACKET_LEN, bytes_len(sql_) + 1
        )  # +1 is for command
        prelude: bytes = struct_pack("<iB", chunk_size, command)
        self._write_bytes(prelude + sql_[: chunk_size - 1])
        self._next_seq_id = 1
        if chunk_size < MAX_PACKET_LEN:
            return None

        # Keep sending query command
        sql_ = sql_[chunk_size - 1 :]
        while True:
            chunk_size = min(MAX_PACKET_LEN, bytes_len(sql_))
            self.write_packet(sql_[:chunk_size])
            sql_ = sql_[chunk_size:]
            if not sql_ and chunk_size < MAX_PACKET_LEN:
                break

    async def _read_query_result(self, unbuffered: cython.bint = False) -> None:
        self._result = None
        result: MysqlResult
        if unbuffered:
            try:
                result = MysqlResult(self)
                await result.init_unbuffered_query()
            except BaseException:
                result.unbuffered_active = False
                result.connection = None
                raise
        else:
            result = MysqlResult(self)
            await result.read()

        self._result = result
        self._affected_rows = result.affected_rows
        if self._result.server_status >= 0:
            self._server_status = result.server_status

    @cython.cfunc
    @cython.inline(True)
    def _ensure_alive(self):
        "(cfunc) Ensure connection is alive (internal use only)."
        if self._writer is None:
            if self._close_reason is None:
                raise errors.QueryInterfaceError(0, "Not connected")
            else:
                raise errors.QueryInterfaceError(0, self._close_reason)

    # Read packet --------------------------------------------------------------------------------
    async def _read_packet(self, packet_type: type = MysqlPacket) -> MysqlPacket:
        """Read an entire "mysql packet" in its entirety from the network
        and return a MysqlPacket type that represents the results.
        """
        buff: bytes = b""
        btrl: cython.longlong
        btrh: cython.longlong
        packet_header: bytes
        packet_number: cython.longlong
        bytes_to_read: cython.longlong
        recv_data: bytes
        while True:
            try:
                packet_header = await self._read_bytes(4)
            except CancelledError:
                self._close_on_cancel()
                raise

            btrl, btrh, packet_number = struct_unpack("<HBB", packet_header)
            bytes_to_read = btrl + (btrh << 16)

            # Outbound and inbound packets are numbered sequentialy, so
            # we increment in both write_packet and read_packet. The count
            # is reset at new constant PHASE.
            if packet_number != self._next_seq_id:
                await self.close()
                if packet_number == 0:
                    # MySQL 8.0 sends error packet with seqno==0 when shutdown
                    raise errors.QueryOperationalError(
                        constant.CR_SERVER_LOST,
                        "Lost connection to MySQL server during query",
                    )

                raise errors.QueryInternalError(
                    "Packet sequence number wrong - got %d expected %d"
                    % (packet_number, self._next_seq_id)
                )
            self._next_seq_id = (self._next_seq_id + 1) % 256

            try:
                recv_data = await self._read_bytes(bytes_to_read)
            except CancelledError:
                self._close_on_cancel()
                raise

            buff += recv_data
            # https://dev.mysql.com/doc/internals/en/sending-more-than-16mbyte.html
            if bytes_to_read == 0xFFFFFF:
                continue
            if bytes_to_read < MAX_PACKET_LEN:
                break

        packet: MysqlPacket = packet_type(buff, self._encoding)
        if packet.is_error_packet():
            if self._result is not None and self._result.unbuffered_active is True:
                self._result.unbuffered_active = False
            packet.raise_for_error()
        return packet

    async def _read_bytes(self, nth: int) -> bytes:
        """Read the exactly nth bytes from ConnectionReader (internal use only)."""
        try:
            return await self._reader.readexactly(nth)
        except IncompleteReadError as err:
            await self.close()
            raise errors.QueryOperationalError(
                constant.CR_SERVER_LOST,
                "Lost connection to MySQL server during query (%s)" % err,
            ) from err
        except (IOError, OSError) as err:
            await self.close()
            raise errors.QueryOperationalError(
                constant.CR_SERVER_LOST,
                "Lost connection to MySQL server during query (%s)" % err,
            ) from err
        except Exception as err:
            errors.raise_exc(err)

    async def _read_ok_packet(self) -> None:
        pkt: MysqlPacket = await self._read_packet()
        if not pkt.is_ok_packet():
            raise errors.QueryOperationalError(2014, "Command Out of Sync")
        ok: OKPacketWrapper = OKPacketWrapper(pkt)
        self._server_status = ok.server_status

    # Write packet -------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def write_packet(self, payload: bytes):
        """Writes an entire "mysql packet" in its entirety to the network
        addings its length and sequence number.
        """
        # Internal note: when you build packet manually and calls
        # _write_bytes() directly, you should set self._next_seq_id properly.
        # data = struct.pack("<I", len(payload))[:3] + bytes([self._next_seq_id]) + payload
        header: bytes = struct_pack("<I", bytes_len(payload))
        header = header[:3] + bytes([self._next_seq_id])
        self._write_bytes(header + payload)
        self._next_seq_id = (self._next_seq_id + 1) % 256

    @cython.cfunc
    @cython.inline(True)
    def _write_bytes(self, data: bytes):
        try:
            self._writer.write(data)
        except Exception as err:
            errors.raise_exc(err)

    # Close --------------------------------------------------------------------------------------
    async def ping(self, reconnect: cython.bint = True) -> None:
        """Check if the server is alive"""
        if self._writer is None and self._reader is None:
            if reconnect:
                await self.connect()
                reconnect = False
            else:
                raise errors.QueryExeError("Connection already closed.")
        try:
            await self._execute_command(constant.COM_PING, "")
            await self._read_ok_packet()
        except Exception:
            if reconnect:
                await self.connect()
                await self.ping(False)
            else:
                raise

    async def kill(self, thread_id: cython.int) -> None:
        await self._execute_command(
            constant.COM_PROCESS_KILL, struct_pack("<I", thread_id)
        )
        await self._read_ok_packet()

    async def close(self) -> None:
        "Close the socket connection (exception free)."
        # Connection already closed
        if self._writer is None:
            return None

        # Try to close connection
        try:
            # Quit command in bytes:
            # struct.pack("<i", 1) + bytes([constant.COM_QUIT])
            self._writer.write(QUIT_COMMAND_BYTES)
            await self._writer.drain()
            self._close()
        except CancelledError:
            self._close_on_cancel()
        except Exception:
            self._close()
        except BaseException:
            self._close()
            raise

    def force_close(self) -> None:
        "Force close the socket connection (exception free)."
        self._close()

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _close(self):
        "(cfunc) Force close the socket connection (internal use only)."
        if self._writer is not None:
            try:
                self._writer.transport.close()
            except Exception:
                pass
            except BaseException:
                self._writer = None
                self._reader = None
                raise

        self._writer = None
        self._reader = None

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _close_on_cancel(self):
        "(cfunc) Connection closed on cancel (internal use only)."
        self._close_reason = "Cancelled during execution"
        self._close()

    # Utils --------------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def _gen_client_info(self, program_name: str) -> bytes:
        """Generate client info string.

        :param program_name: Program name string to provide when handshaking with MySQL.
        :return: Client info string.
        """
        pid_str: str = str(getpid())
        pid: bytes = pid_str.encode("utf8")
        header: bytes = struct_pack("B", bytes_len(pid))
        info: bytes = CLIENT_INFO_PREFIX + header + pid
        if program_name is not None and program_name:
            name: bytes = program_name.encode("utf8")
            header: bytes = struct_pack("B", bytes_len(name))
            pfix: bytes = b"\x0cprogram_name"
            info = info + pfix + header + name
        return info

    @cython.cfunc
    @cython.inline(True)
    def _lenenc_int(self, i: cython.int) -> bytes:
        packet: bytes
        if i < 0:
            raise ValueError(
                "Encoding %d is less than 0 - no representation in LengthEncodedInteger"
                % i
            )
        elif i < 0xFB:
            return bytes([i])
        elif i < (1 << 16):
            packet = struct_pack("<H", i)
            return b"\xfc" + packet
        elif i < (1 << 24):
            packet = struct_pack("<I", i)
            return b"\xfd" + packet[:3]
        elif i < (1 << 64):
            packet = struct_pack("<Q", i)
            return b"\xfe" + packet
        else:
            raise ValueError(
                "Encoding %x is larger than %x - no representation in LengthEncodedInteger"
                % (i, (1 << 64))
            )

    # Special methods ----------------------------------------------------------------------------
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    def __repr__(self) -> str:
        return "<Connection (host='%s', port=%d)>" % (self._host, self._port)

    def __hash__(self) -> int:
        return id(self)

    def __del__(self):
        if not self.get_closed():
            self._close()
            self._ssl_context = None
            self._cursorclass = None
            self._result = None

    # Exceptions ---------------------------------------------------------------------------------
    Warning = errors.QueryWarning
    Error = errors.QueryExeError
    InterfaceError = errors.QueryInterfaceError
    DatabaseError = errors.QueryDatabaseError
    DataError = errors.QueryDataError
    OperationalError = errors.QueryOperationalError
    IntegrityError = errors.QueryIntegrityError
    InternalError = errors.QueryInternalError
    ProgrammingError = errors.QueryProgrammingError
    NotSupportedError = errors.QueryNotSupportedError


@cython.cclass
class _Connection(Connection):
    """Represents a socket connection with the MySQL Server.

    This subclass of `Connection` adds argument validation during
    initialization.

    Do `NOT` create an instance of this class directly. The proper
    way to get an instance of `Connection` is to call `connect()`,
    `Pool.acquire()` or `Server.acquire()` through `async with`.
    """

    def __init__(
        self,
        host: str = "localhost",
        port: cython.int = 3306,
        user: Union[str, None] = None,
        password: Union[str, None] = None,
        database: Union[str, None] = None,
        auth_plugin: Union[
            Literal[
                "mysql_native_password",
                "caching_sha2_password",
                "sha256_password",
                "mysql_clear_password",
            ],
            None,
        ] = None,
        server_public_key: Union[str, None] = None,
        unix_socket: Union[str, None] = None,
        ssl_context: Union[SSLContext, None] = None,
        charset: Union[str, None] = None,
        sql_mode: Union[str, None] = None,
        init_command: Union[str, None] = None,
        autocommit: cython.bint = False,
        connect_timeout: cython.int = 10,
        cursorclass: type[Cursor] = Cursor,
        program_name: Union[str, None] = None,
        local_infile: cython.bint = False,
        client_flag: cython.int = 0,
        echo: cython.bint = False,
        read_default_file: Union[str, None] = None,
        read_default_group: Union[str, None] = None,
    ) -> None:
        """Connection with the MySQL Server.

        :param host: `<str>` Host of the database Server. Defaults to `'localhost'`.
        :param port: `<int>` Port of the database Server. Defaults to `3306`.
        :param user: `<str>` The username to login as. Defaults to `None`.
        :param password: `<str>` The password for user authentication. Defaults to `None`.
        :param database: `<str>` Specific database to use. Defaults to `None`.
        :param auth_plugin: `<str>` The authorization plugin to be used. Defaults to `None` (Server settings).
        :param server_public_key: `<str>`  SHA256 authentication plugin public key value. Defaults to `None`.
        :param unix_socket: `<str>` Use the provide unix socket to establish the connection. Defaults to `None`.
        :param ssl_context: `<ssl.SSLContext>` Force to establish the connection with ssl. Defaults to `None`.
        :param charset: `<str>` The charset for the connection. Defaults to `None`.
        :param sql_mode: `<str>` The default SQL mode for the connection. Defaults to `None`.
        :param init_command: `<str>` The initial command to be executed once connection is established. Defaults to `None`.
        :param autocommit: `<bool>` Whether to auto commit after each query execution. Defaults to `False`.
        :param connect_timeout: `<int>` Timeout in seconds for establishing the connection. Defaults to `10`.
        :param cursorclass: `<type[Cursor]>` The Cursor class to be used. Defaults to `Cursor`.
        :param program_name: `<str>` The program name to be used while handshaking. Defaults to `None`.
        :param local_infile: `<bool>` Whether to enable the use of LOAD DATA LOCAL command. Defaults to `False`.
        :param client_flag: `<int>` Custom flags to send to MySQL. Defaults to `0`.
            For more information, please refer tp `pymysql.constants.CLIENT`.
        :param echo: `<bool>` Whether to enable echo (log each executed queries). Defaults to `False`.
        :param read_default_file: `<str>` The path to the `my.cnf` file to load configuration. Defaults to `None`.
        :param read_default_group: `<str>` The group to read from the configuration file. Defaults to `None`.
        """
        # Load from default
        if read_default_file:
            if not read_default_group:
                read_default_group = "client"
            cfg = configparser.RawConfigParser()
            cfg.read(os.path.expanduser(read_default_file))
            _config = functools.partial(cfg.get, read_default_group)
            host: str = _config("host", fallback=host)
            port: int = int(_config("port", fallback=port))
            user: str = _config("user", fallback=user)
            password: str = _config("password", fallback=password)
            database: str = _config("database", fallback=database)
            unix_socket: str = _config("socket", fallback=unix_socket)
            charset: str = _config("default-character-set", fallback=charset)
        # Password
        if is_str(password):
            password = password.encode("latin1")
        elif not password:
            password = b""
        # Client flag
        if client_flag < 0:
            client_flag = 0
        # Server public key
        if not server_public_key:
            server_public_key = b""
        elif is_str(server_public_key):
            server_public_key = server_public_key.encode("ascii")
        # SSL
        if ssl_context is not None:
            if not isinstance(ssl_context, SSLContext):
                raise TypeError(
                    "'ssl_context' must be an instance of `ssl.SSLContext`."
                )
            client_flag |= constant.CLIENT_SSL
            ssl = True
        else:
            ssl = False
        # Charset
        if not charset:
            charset = "utf8mb4"
        encoding = charset_by_name(charset).get_encoding()
        # Client flag
        if local_infile:
            client_flag |= constant.CLIENT_LOCAL_FILES
        client_flag |= constant.CLIENT_CAPABILITIES
        client_flag |= constant.CLIENT_MULTI_STATEMENTS
        if database:
            client_flag |= constant.CLIENT_CONNECT_WITH_DB
        # Initialize
        super().__init__(
            host if host else "localhost",
            port if port > 0 else 3306,
            user if user else "",
            password,
            database if database else "",
            auth_plugin if auth_plugin else "",
            server_public_key,
            unix_socket if unix_socket else "",
            ssl,
            ssl_context,
            charset,
            encoding,
            sql_mode if sql_mode else "",
            init_command if init_command else "",
            autocommit,
            connect_timeout if 0 < connect_timeout <= 31536000 else 10,
            cursorclass if issubclass(cursorclass, Cursor) else Cursor,
            program_name if program_name else "",
            client_flag,
            echo,
        )


@cython.cclass
class MysqlResult:
    """Represents the MySQL query result."""

    connection: Connection
    affected_rows: cython.longlong
    insert_id: cython.longlong
    server_status: cython.int
    warning_count: cython.int
    field_count: cython.longlong
    unbuffered_active: cython.bint
    message: bytes
    has_next: cython.bint
    fields: list[FieldDescriptorPacket]
    columns: tuple[str]
    rows: tuple[tuple]
    _encodings: list[str]
    _converters: list[Callable]

    def __init__(self, connection):
        self.connection = connection
        self.affected_rows = 0
        self.insert_id = 0
        self.server_status = -1
        self.warning_count = -1
        self.field_count = 0
        self.unbuffered_active = False
        self.message = None
        self.columns = None
        self.rows = None
        self.has_next = False

    async def read(self) -> None:
        try:
            first_packet: MysqlPacket = await self.connection._read_packet()
            if first_packet.is_ok_packet():
                self._read_ok_packet(first_packet)
            elif first_packet.is_load_local_packet():
                await self._read_local_packet(first_packet)
            else:
                await self._read_result_packet(first_packet)
            self.connection = None
        except Exception as err:
            self.connection = None
            errors.raise_exc(err)

    async def init_unbuffered_query(self) -> None:
        self.unbuffered_active = True
        first_packet: MysqlPacket = await self.connection._read_packet()
        if first_packet.is_ok_packet():
            self._read_ok_packet(first_packet)
            self.unbuffered_active = False
            self.connection = None
        elif first_packet.is_load_local_packet():
            await self._read_local_packet(first_packet)
            self.unbuffered_active = False
            self.connection = None
        else:
            self.field_count = first_packet.read_length_encoded_integer()
            await self._get_descriptions()
            # Apparently, MySQLdb picks this number because it's the maximum
            # value of a 64bit unsigned integer. Since we're emulating MySQLdb,
            # we set it to this instead of None, which would be preferred.
            self.affected_rows = MAX_AFFECTED_ROWS

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _is_eof_packet(self, packet: MysqlPacket) -> cython.bint:
        if packet.is_eof_packet():
            eof_packet = EOFPacketWrapper(packet)
            self.warning_count = eof_packet.warning_count
            self.has_next = eof_packet.has_next
            return True
        else:
            return False

    @cython.cfunc
    @cython.inline(True)
    def _read_ok_packet(self, first_packet: MysqlPacket):
        ok_packet = OKPacketWrapper(first_packet)
        self.affected_rows = ok_packet.affected_rows
        self.insert_id = ok_packet.insert_id
        self.server_status = ok_packet.server_status
        self.warning_count = ok_packet.warning_count
        self.message = ok_packet.message
        self.has_next = ok_packet.has_next

    async def _read_local_packet(self, first_packet: MysqlPacket) -> None:
        try:
            load_packet = LoadLocalPacketWrapper(first_packet)
            sender = LoadLocalFile(load_packet.filename, self.connection)
            await sender.send_data()
        except Exception as err:
            # Skip ok packet
            await self.connection._read_packet()
            errors.raise_exc(err)

        ok_packet: MysqlPacket = await self.connection._read_packet()
        if not ok_packet.is_ok_packet():
            raise errors.QueryOperationalError(2014, "Commands Out of Sync")
        self._read_ok_packet(ok_packet)

    async def _read_result_packet(self, first_packet: MysqlPacket) -> None:
        self.field_count = first_packet.read_length_encoded_integer()
        await self._get_descriptions()
        await self._read_row_packet_buffered()

    async def _read_row_packet_buffered(self) -> None:
        """Read a rowdata packet for each data row in the result set."""
        rows: list = []
        while True:
            packet: MysqlPacket = await self.connection._read_packet()
            if self._is_eof_packet(packet):
                # release reference to kill cyclic reference.
                self.connection = None
                break
            rows.append(self._read_row_from_packet(packet))
        self.affected_rows = list_len(rows)
        self.rows = tuple(rows)

    async def _read_row_packet_unbuffered(self) -> tuple:
        # Check if in an active query
        if not self.unbuffered_active:
            return None

        packet = await self.connection._read_packet()
        if self._is_eof_packet(packet):
            self.unbuffered_active = False
            self.connection = None
            self.rows = None
            return None

        row = self._read_row_from_packet(packet)
        self.affected_rows = 1
        # rows should tuple of row for MySQL-python compatibility.
        self.rows = (row,)
        return row

    @cython.cfunc
    @cython.inline(True)
    def _read_row_from_packet(self, packet: MysqlPacket) -> tuple:
        row = []
        idx: cython.longlong = 0
        for encoding in self._encodings:
            try:
                data = packet.read_length_coded_string()
            except IndexError:
                # No more columns in this row
                # See https://github.com/PyMySQL/PyMySQL/pull/434
                break
            row.append(self._convert_data(idx, data, encoding))
            idx += 1
        return tuple(row)

    @cython.cfunc
    @cython.inline(True)
    @cython.wraparound(False)
    @cython.boundscheck(False)
    def _convert_data(self, idx: cython.longlong, data: bytes, encoding: str) -> object:
        if data is None:
            return None

        converter = self._converters[idx]
        if encoding is not None:
            if converter is not None:
                return converter(data.decode(encoding))
            else:
                return data.decode(encoding)
        elif converter is not None:
            return converter(data)
        else:
            return data

    async def _get_descriptions(self) -> None:
        "Read a column descriptor packet for each column in the result."
        self.fields = []
        columns = []
        self._encodings = []
        self._converters = []
        encoding: str
        conn_encoding = self.connection._encoding
        field: FieldDescriptorPacket
        for _ in range(self.field_count):
            field = await self.connection._read_packet(FieldDescriptorPacket)
            self.fields.append(field)
            column = field.name
            if column in columns:
                column = field.table_name + "." + column
            columns.append(column)
            field_type = field.type_code
            if field_type == constant.FIELD_TYPE_JSON:
                # When SELECT from JSON column: charset = binary
                # When SELECT CAST(... AS JSON): charset = connection
                # encoding
                # This behavior is different from TEXT / BLOB.
                # We should decode result by connection encoding
                # regardless charsetnr.
                # See https://github.com/PyMySQL/PyMySQL/issues/488
                encoding = conn_encoding  # SELECT CAST(... AS JSON)
            elif field_type in constant.FIELD_TEXT_TYPES:
                if field.charsetnr == 63:  # binary
                    # TEXTs with charset=binary means BINARY types.
                    encoding = None
                else:
                    encoding = conn_encoding
            else:
                # Integers, Dates and Times, and other basic data
                # is encoded in ascii
                encoding = "ascii"

            self._encodings.append(encoding)
            self._converters.append(transcode.DECODERS.get(field_type))

        eof_packet: MysqlPacket = await self.connection._read_packet()
        assert eof_packet.is_eof_packet(), "Protocol error, expecting EOF"
        self.columns = tuple(columns)

    async def finish_unbuffered_query(self) -> None:
        # After much reading on the MySQL protocol, it appears that there is,
        # in fact, no way to stop MySQL from sending all the data after
        # executing a query, so we just spin, and wait for an EOF packet.
        packet: MysqlPacket
        while self.unbuffered_active:
            try:
                packet = await self.connection._read_packet()
            except errors.QueryOperationalError as err:
                # TODO: replace these numbers with constants when available
                # TODO: in a new PyMySQL release
                if err.args[0] in (
                    3024,  # ER.QUERY_TIMEOUT
                    1969,  # ER.STATEMENT_TIMEOUT
                ):
                    # if the query timed out we can simply ignore this error
                    self.unbuffered_active = False
                    self.connection = None
                    return None
                else:
                    raise

            if self._is_eof_packet(packet):
                self.unbuffered_active = False
                # release reference to kill cyclic reference.
                self.connection = None


@cython.cclass
class LoadLocalFile:
    """Represents the local file to be loaded into MySQL."""

    connection: Connection
    filename: bytes
    _file_object: object
    _executor: object

    def __init__(self, filename: bytes, connection: Connection):
        self.connection = connection
        self.filename = filename
        self._file_object = None
        self._executor = None  # means use default executor

    def _open_file(self):
        def opener(filename):
            try:
                self._file_object = open(filename, "rb")
            except IOError as err:
                raise errors.QueryOperationalError(
                    1017, "Can't find file '{0}'".format(filename)
                ) from err

        loop = get_running_loop()
        return loop.run_in_executor(self._executor, opener, self.filename)

    def _file_read(self, chunk_size):
        def freader(chunk_size):
            try:
                chunk = self._file_object.read(chunk_size)

                if not chunk:
                    self._file_object.close()
                    self._file_object = None

            except Exception as err:
                self._file_object.close()
                self._file_object = None
                raise errors.QueryOperationalError(
                    1024, "Error reading file {}".format(self.filename)
                ) from err
            return chunk

        loop = get_running_loop()
        return loop.run_in_executor(self._executor, freader, chunk_size)

    async def send_data(self):
        """Send data packets from the local file to the server"""

        self.connection._ensure_alive()
        conn = self.connection

        try:
            await self._open_file()
            with self._file_object:
                chunk_size = MAX_PACKET_LEN
                while True:
                    chunk = await self._file_read(chunk_size)
                    if not chunk:
                        break
                    # TODO: consider drain data
                    conn.write_packet(chunk)
        except CancelledError:
            self.connection._close_on_cancel()
            raise
        finally:
            # send the empty packet to signify we are done sending data
            conn.write_packet(b"")


# Pool =========================================================================================
@cython.cclass
class Pool:
    """Represents a pool that manages and maintains connections
    of the MySQL Server. Increases reusability of established
    connections and reduces the overhead for creating new ones.
    """

    # Conneciton [client]
    _host: str
    _port: cython.int
    _user: str
    _password: bytes
    _db: str
    # Connection [auth]
    _auth_plugin: str
    _server_public_key: bytes
    _unix_socket: str
    _ssl: cython.bint
    _ssl_context: SSLContext
    # Conneciton [settings]
    _charset: str
    _encoding: str
    _sql_mode: str
    _init_command: str
    _autocommit: cython.bint
    _connect_timeout: cython.int
    _cursorclass: type[Cursor]
    _program_name: str
    _client_flag: cython.int
    _echo: cython.bint
    # Pool
    _max_size: cython.int
    _acquiring: cython.int
    _free_conn: list[Connection]
    _used_conn: set[Connection]
    _invlid_conn: set[Connection]
    _condition: Condition
    _recycle: cython.int
    _closing: cython.bint
    _closed: cython.bint
    _server_version: cython.int
    _backslash_escapes: cython.bint

    def __init__(
        self,
        host: str = "localhost",
        port: cython.int = 3306,
        user: Union[str, None] = None,
        password: Union[str, None] = None,
        max_size: cython.int = 10,
        recycle: cython.int = 0,
        database: Union[str, None] = None,
        auth_plugin: Union[
            Literal[
                "mysql_native_password",
                "caching_sha2_password",
                "sha256_password",
                "mysql_clear_password",
            ],
            None,
        ] = None,
        server_public_key: Union[str, None] = None,
        unix_socket: Union[str, None] = None,
        ssl_context: Union[SSLContext, None] = None,
        charset: Union[str, None] = None,
        sql_mode: Union[str, None] = None,
        init_command: Union[str, None] = None,
        autocommit: cython.bint = False,
        connect_timeout: cython.int = 10,
        cursorclass: type[Cursor] = Cursor,
        program_name: Union[str, None] = None,
        local_infile: cython.bint = False,
        client_flag: cython.int = 0,
        echo: cython.bint = False,
        read_default_file: Union[str, None] = None,
        read_default_group: Union[str, None] = None,
    ) -> None:
        """The Pool that manages and maintains connections of the MySQL Server.

        :param host: `<str>` Host of the database Server. Defaults to `'localhost'`.
        :param port: `<int>` Port of the database Server. Defaults to `3306`.
        :param user: `<str>` The username to login as. Defaults to `None`.
        :param password: `<str>` The password for user authentication. Defaults to `None`.
        :param max_size: `<int>` The maximum number of connections maintain by the Pool. Defaults to `10`.
        :param recycle: `<int>` Total seconds a connection can be idle in the Pool before closed and removed. Defaults to `0`.
            For `recycle <= 0`, it means all connections in the pool stays permanent.
        :param database: `<str>` Specific database to use. Defaults to `None`.
        :param auth_plugin: `<str>` The authorization plugin to be used. Defaults to `None` (Server settings).
        :param server_public_key: `<str>`  SHA256 authentication plugin public key value. Defaults to `None`.
        :param unix_socket: `<str>` Use the provide unix socket to establish connections. Defaults to `None`.
        :param ssl_context: `<ssl.SSLContext>` Force to establish connections with ssl. Defaults to `None`.
        :param charset: `<str>` The charset for the connections. Defaults to `None`.
        :param sql_mode: `<str>` The default SQL mode for the connections. Defaults to `None`.
        :param init_command: `<str>` The initial command to be executed once connection is established. Defaults to `None`.
        :param autocommit: `<bool>` Whether to auto commit after each query execution. Defaults to `False`.
        :param connect_timeout: `<int>` Timeout in seconds for establishing new connections. Defaults to `10`.
        :param cursorclass: `<type[Cursor]>` The Cursor class to be used. Defaults to `Cursor`.
        :param program_name: `<str>` The program name to be used while handshaking. Defaults to `None`.
        :param local_infile: `<bool>` Whether to enable the use of LOAD DATA LOCAL command. Defaults to `False`.
        :param client_flag: `<int>` Custom flags to send to MySQL. Defaults to `0`.
            For more information, please refer tp `pymysql.constants.CLIENT`.
        :param echo: `<bool>` Whether to enable echo (log each executed queries). Defaults to `False`.
        :param read_default_file: `<str>` The path to the `my.cnf` file to load configuration. Defaults to `None`.
        :param read_default_group: `<str>` The group to read from the configuration file. Defaults to `None`.
        """
        # Connection ------------------------------------------
        # . load from default
        if read_default_file:
            if not read_default_group:
                read_default_group = "client"
            cfg = configparser.RawConfigParser()
            cfg.read(os.path.expanduser(read_default_file))
            _config = functools.partial(cfg.get, read_default_group)
            host: str = _config("host", fallback=host)
            port: int = int(_config("port", fallback=port))
            user: str = _config("user", fallback=user)
            password: str = _config("password", fallback=password)
            database: str = _config("database", fallback=database)
            unix_socket: str = _config("socket", fallback=unix_socket)
            charset: str = _config("default-character-set", fallback=charset)
        # . client
        self._host = host if host else "localhost"
        self._port = port if port > 0 else 3306
        self._user = user if user else ""
        if is_str(password):
            self._password = password.encode("latin1")
        elif password:
            self._password = password
        else:
            self._password = b""
        self._db = database if database else ""
        # . auth
        if client_flag < 0:
            client_flag = 0
        self._auth_plugin = auth_plugin if auth_plugin else ""
        if not server_public_key:
            self._server_public_key = b""
        elif is_str(server_public_key):
            self._server_public_key = server_public_key.encode("ascii")
        else:
            self._server_public_key = server_public_key
        self._unix_socket = unix_socket if unix_socket else ""
        if ssl_context is not None:
            if not isinstance(ssl_context, SSLContext):
                raise TypeError(
                    "'ssl_context' must be an instance of `ssl.SSLContext`."
                )
            client_flag |= constant.CLIENT_SSL
            self._ssl = True
        else:
            self._ssl = False
        self._ssl_context = ssl_context
        # . settings
        self._charset = charset if charset else "utf8mb4"
        self._encoding = charset_by_name(self._charset).get_encoding()
        self._sql_mode = sql_mode if sql_mode else ""
        self._init_command = init_command if init_command else ""
        self._autocommit = autocommit
        if 0 < connect_timeout <= 31536000:
            self._connect_timeout = connect_timeout
        else:
            self._connect_timeout = 10
        if issubclass(cursorclass, Cursor):
            self._cursorclass = cursorclass
        else:
            self._cursorclass = Cursor
        self._program_name = program_name if program_name else ""
        if local_infile:
            client_flag |= constant.CLIENT_LOCAL_FILES
        client_flag |= constant.CLIENT_CAPABILITIES
        client_flag |= constant.CLIENT_MULTI_STATEMENTS
        if database:
            client_flag |= constant.CLIENT_CONNECT_WITH_DB
        self._client_flag = client_flag
        self._echo = echo

        # Pool ------------------------------------------------
        self._max_size = max_size if max_size > 0 else 10
        self._acquiring = 0
        self._free_conn = []
        self._used_conn = set()
        self._invlid_conn = set()
        self._condition = Condition()
        self._recycle = recycle if recycle > 0 else 0
        self._closing = False
        self._closed = False
        self._backslash_escapes = True
        self._server_version = 5

    # Properties ---------------------------------------------------------------------------------
    @property
    def host(self) -> str:
        "Host of the database Server `<str>`."
        return self._host

    @property
    def port(self) -> int:
        "Port of the database Server `<int>`."
        return self._port

    @property
    def user(self) -> str:
        "The username logged in as `<str>`."
        return self._user or None

    @property
    def database(self) -> str:
        "Specific database being used `<str>`."
        return self._db or None

    @property
    def unix_socket(self) -> str:
        "The unix socket been used instead of TCP/IP `<str>`."
        return self._unix_socket or None

    @property
    def ssl_context(self) -> SSLContext:
        "SSL Context been used to force ssl connection `<ssl.SSLContext>`."
        return self._ssl_context

    @property
    def charset(self) -> str:
        "The charset being used `<str>`."
        return self._charset

    @property
    def autocommit(self) -> bool:
        "Whether autocommit is enabled `<bool>`."
        return self._autocommit

    async def set_autocommit(self, auto: cython.bint) -> None:
        """Set autocommit mode for the connection.

        Change of autocommint mode will only affects new spawn
        connections, connections in use will not be affected.
        All idling free connections will be closed and removed.

        :param auto: `<bool>` Whether to enable autocommit.
        """
        if auto != self._autocommit:
            self._autocommit = auto
            await self.clear()

    @property
    def encoding(self) -> str:
        "Encoding employed for the connection. `<str>`."
        return self._encoding

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def get_version(self) -> cython.int:
        "(cfunc) Get the `MAJOR` Mysql Server version `<int>`."
        return self._server_version

    @property
    def version(self) -> int:
        "The `MAJOR` Mysql Server version `<int>`."
        return self.get_version()

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def get_backslash_escapes(self) -> cython.bint:
        "(cfunc) Get whether the server use `BACKSLASH_ESCAPES` `<bool>`."
        return self._backslash_escapes

    @property
    def backslash_escapes(self) -> bool:
        "Whether the connection use `BACKSLASH_ESCAPES` `<bool>`."
        return self.get_backslash_escapes()

    @property
    def max_size(self) -> int:
        "Maximum number of connections the Pool will maintain `<int>`."
        return self._max_size

    def set_max_size(self, size: cython.int) -> None:
        """Set the maximum number of connections the Pool will maintain.

        :param size: `<int>` The maximum number of connections.
        """
        if size < 0:
            raise ValueError("The maximum size of the Pool must be greater than 0.")
        self._max_size = size

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def get_size(self) -> cython.int:
        """(cfunc) Get the number of connections
        currently maintained by the Pool `<int>`."""
        return self._acquiring + self.get_free_size() + self.get_used_size()

    @property
    def size(self) -> int:
        "Number of connections currently maintained by the Pool `<int>`."
        return self.get_size()

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def get_free_size(self) -> cython.int:
        """(cfunc) Get the number of free connections
        currently idling in the Pool `<int>`."""
        return list_len(self._free_conn)

    @property
    def free_size(self) -> int:
        "Number of free connections currently idling in the Pool `<int>`."
        return self.get_free_size()

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def get_used_size(self) -> cython.int:
        """(cfunc) Get the number of connections that
        acquired from the Pool and in use `<int>`."""
        return set_len(self._used_conn)

    @property
    def used_size(self) -> int:
        "Number of connections that acquired from the Pool and in use `<int>`."
        return self.get_used_size()

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def get_invalid_size(self) -> cython.int:
        """(cfunc) Get the number of invalid connections
        that should be closed and removed `<int>`."""
        return set_len(self._invlid_conn)

    @property
    def invalid_size(self) -> int:
        "Number of invalid connections that should be closed and removed `<int>`."
        return self.get_invalid_size()

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def get_size_info(self) -> str:
        "(cfunc) Get the size information snapshot of the Pool `<str>`."

        return "max.: %d, ttl.: %d, acqr: %d, free: %d, used: %d" % (
            self._max_size,
            self.get_size(),
            self._acquiring,
            self.get_free_size(),
            self.get_used_size(),
        )

    @property
    def size_info(self) -> str:
        "Size information snapshot of the Pool `<str>`."
        return self.get_size_info()

    @property
    def recycle(self) -> int:
        """The number of seconds a connection can be idle in the Pool
        before closed and removed. Value of `0` means permanent `<int>`.
        """
        return self._recycle

    def set_recycle(self, seconds: cython.int) -> None:
        """Set the number of seconds a connection can be idle in the Pool
        before closed and removed. Value of `0` means permanent.

        :param seconds: `<int>` Recycle time in seconds.
        """
        if seconds > 0:
            self._recycle = seconds
        else:
            self._recycle = 0

    @property
    def closed(self) -> bool:
        "Whether the Pool is closed `<bool>`."
        return self._closed

    # Acquire ------------------------------------------------------------------------------------
    def acquire(self) -> PoolConnectionManager:
        """Acquire a connection from the Pool (Context Manager).

        By acquiring a connection through this method with `async with`
        statement, the following will happen:
        - 1. Acquire a free/new connection from the Pool.
        - 2. Return `PoolConnectionManager` that wraps the connection.
        - 3. Release the connection back to the Pool at exist.
        """
        return PoolConnectionManager(self)

    def transaction(self) -> PoolTransactionManager:
        """Acquire a connection from the Pool in transaction mode (Context Manager).

        By acquiring a connection through this method with `async with`
        statement, the following will happen:
        - 1. Acquire a free/new connection from the Pool.
        - 2. Use the connection to `START` a transaction.
        - 3. Return `PoolTransactionManager` that wraps the connection.
        - 4a. If catches ANY exceptions during the transaction, execute
            `ROLLBACK`, then close and release the connection.
        - 4b. If the transaction executed successfully, execute `COMMIT`
            and then release the connection back to the pool.
        """
        return PoolTransactionManager(self)

    async def _acquire_connection(self) -> Connection:
        """Acquire a connection from the Pool.

        :return `<PoolConnection>`: A connection from the Pool.
        """
        async with self._condition:
            while not self._closing:
                # Acquire free connection
                conn = self._acquire_free_connection()
                if self.get_invalid_size() > 0:
                    await self._close_invalid_connections()
                if conn is not None:
                    return conn

                # Acquire new connection
                conn = await self._acquire_new_connection(False)
                if conn is not None:
                    return conn

                # Wait for condition
                await self._condition.wait()

        # Pool is closed
        await self._wait_for_close()

    @cython.cfunc
    @cython.inline(True)
    def _acquire_free_connection(self) -> Connection:
        """Acquire a valid free connection from the Pool.

        :return `<PoolConnection>`: A valid free connection, or `None` if no free connection.
        """
        # Get the 1st valid free connection
        conn: Connection
        while self.get_free_size() > 0:
            # Get free connection
            conn = self._free_conn.pop()

            # Already closed
            if conn.get_closed():
                continue

            # Already in use
            if set_contains(self._used_conn, conn):
                continue

            # Should be recycled
            if (
                self._recycle > 0
                and unix_time() - conn.get_last_usage() > self._recycle
            ):
                set_add(self._invlid_conn, conn)
                continue

            # Inavlid connection
            reader: ConnectionReader = conn._reader
            if reader.eof_received or reader.at_eof() or reader.exception():
                set_add(self._invlid_conn, conn)
                continue

            # Return free connection
            set_add(self._used_conn, conn)
            return conn

        # No Free Connection
        return None

    async def _acquire_new_connection(self, free: cython.bint) -> Connection:
        """Create a new connection with the Server.

        :params free: `<bool>` The status of the new connection.
            - If `True`, treated as a free connection and append to `free pool`.
            - If `False`, treated as a connection in use and added to `used pool`.

        :return `<PoolConnection>`: A new connection, or `None` if max pool size is reached.
        """
        # Pool is closed or limit by pool size
        if self.get_size() >= self._max_size or self._closing:
            return None

        # Create new connection
        self._acquiring += 1
        conn: Connection = None
        try:
            conn = Connection(
                self._host,
                self._port,
                self._user,
                self._password,
                self._db,
                self._auth_plugin,
                self._server_public_key,
                self._unix_socket,
                self._ssl,
                self._ssl_context,
                self._charset,
                self._encoding,
                self._sql_mode,
                self._init_command,
                self._autocommit,
                self._connect_timeout,
                self._cursorclass,
                self._program_name,
                self._client_flag,
                self._echo,
            )
            await conn.connect()
            self._backslash_escapes = conn.get_backslash_escapes()
            self._server_version = conn.get_version_major()
            if free:
                list_append(self._free_conn, conn)
            else:
                set_add(self._used_conn, conn)
            self._acquiring -= 1
            return conn
        except BaseException:
            if conn is not None:
                await conn.close()
            self._acquiring -= 1
            raise

    async def _close_invalid_connections(self) -> None:
        """Close all invalid connections in the Pool."""
        await gather(*[c.close() for c in list(self._invlid_conn)])
        set_clear(self._invlid_conn)

    # Fill ---------------------------------------------------------------------------------------
    async def fill(self, size: cython.int = 1) -> None:
        """Fill the Pool with new connections.

        :param size: `<int>` The number of connections to be filled. Defaults to `1`.
            Total number of connections actually filled will be limited by the
            `max_size` of the Pool.
        """
        # Fill new connections
        if size > 0:
            async with self._condition:
                await gather(
                    *[
                        self._acquire_new_connection(True)
                        for _ in range(min(size, self._max_size))
                    ]
                )
                self._condition.notify()

    # Release ------------------------------------------------------------------------------------
    async def release(self, conn: Connection) -> None:
        """Release connection back to the `Pool` (exception free).

        ### Notice
        Connection acquired directly from `Pool` should always be released
        by this method. For connections acquired through Context Manager,
        release will be handled automatically at exist.
        """
        # Remove from `used_conn` pool
        set_discard(self._used_conn, conn)

        # Already closed
        if conn.get_closed():
            pass
        # Not reusable
        elif not conn.get_reusable() or self._closing:
            await conn.close()
        # Transaction in progress
        elif conn.get_transaction_status():
            await conn.close()
        # Back to `free_conn` pool
        else:
            list_append(self._free_conn, conn)

        # Notify
        async with self._condition:
            self._condition.notify()

    # Close --------------------------------------------------------------------------------------
    async def clear(self) -> None:
        """Clear all `free` & `invalid` connections in the Pool (exception free).

        This method closes and removes all of the free & invalid
        connections in the Pool. Connection in use will not be
        affected. Consider as a way to reset the Pool.
        """
        async with self._condition:
            while self.get_free_size() > 0:
                conn = self._free_conn.pop()
                await conn.close()
            while self.get_invalid_size() > 0:
                conn = self._invlid_conn.pop()
                await conn.close()
            self._condition.notify()

    async def close(self, countdown: Union[int, None] = None) -> None:
        """Close the Pool gracefully (exception free).

        Calling this method will close all free & invalid connections maintained
        by the Pool, then wait for all in use connections to be released before
        shutdown (gentle close). However, a countdown can be set to force the Pool
        to terminate all connections after certain seconds (force close).

        :param countdown: `<int>` The number of seconds to wait before termination. Defaults to `None`.
            - If `countdown <= 0 or = None`, the Pool will wait util all
              connections are released.
            - If `countdown > 0`, the Pool will terminate all connections
              (regardless state) after the countdown.
        """
        # Already closed
        if self._closed:
            return None

        # Set closing flag
        self._closing = True

        # Close all free connections
        while self.get_free_size() > 0:
            conn = self._free_conn.pop()
            await conn.close()

        # Close all Invalid connections
        while self.get_invalid_size() > 0:
            conn = self._invlid_conn.pop()
            await conn.close()

        # Set up termination countdown (if specified)
        if countdown is None:
            task = None
        elif is_int(countdown) and countdown > 0:
            task = create_task(self._countdown(countdown))
        elif is_float(countdown) and ceil(countdown) > 0:
            task = create_task(self._countdown(ceil(countdown)))
        else:
            task = None

        # Wait for all connections to be released or closed
        await self._wait_for_close()

        # Wait for termination (if applicable)
        if task is not None:
            await task

    async def _wait_for_close(self) -> None:
        """Wait until all connections have been released
        or closed by the Pool (internal use only).
        """
        # Already closed
        if self._closed:
            return None

        # Wait for all connections to be released or closed
        async with self._condition:
            # Wait for connections
            while self.get_size() > self.get_free_size():
                await self._condition.wait()
            # Set closed flag
            self._closed = True
            # Notify
            self._condition.notify()

    async def terminate(self) -> None:
        """Terminate the Pool (exception free).

        Calling this method will enforce the Pool to close all connections
        in use regardless their status. This method is not designed to be
        called in normal use case. However, if the Pool cannot be closed
        properly (mostly due to connection leak), this method can be used
        to force shutdown the Pool.
        """
        # Already closed
        if self._closed:
            return None

        # Set closing flag
        self._closing = True

        # Close all used connections
        while self.get_used_size() > 0:
            conn = self._used_conn.pop()
            await conn.close()

        # Notify
        async with self._condition:
            self._condition.notify()

        # Close all free connections
        await self.close(None)

    async def _countdown(self, seconds: cython.int) -> None:
        """Setup a termination countdown (internal use only).

        After countdown in seconds, the Pool will execute `terminate()`
        if there are still remaining connections in use.

        :param seconds: `<int>` The number of seconds to wait before termination.
        """
        # Invalid countdown
        if seconds <= 0:
            return None

        # Wait for countdown
        countdown: cython.int = 0
        while not self._closed:
            if countdown >= seconds:
                # . terminate
                await self.terminate()
                return None
            await sleep(1)
            countdown += 1

    def quit(self) -> None:
        """Forcefully terminates all connections in the Pool (exception free).

        This method immediately terminates all active connections
        in the pool. For a graceful shutdown, use the `close()`
        method instead.

        Side Effects:
            - All managed server processes will be interrupted.
            - All network connections in the pool will be closed.
            - This operation is irreversible and should be used with caution.
        """
        # Already closed
        if self._closed:
            return None  # exit

        # Set closing flag
        self._closing = True

        # Force quit
        if self.get_free_size() > 0:
            for conn in self._free_conn:
                conn.force_close()
        if self.get_used_size() > 0:
            for conn in self._used_conn:
                conn.force_close()
        if self.get_invalid_size() > 0:
            for conn in self._invlid_conn:
                conn.force_close()
        self._ssl_context = None
        self._cursorclass = None
        self._free_conn = None
        self._used_conn = None
        self._invlid_conn = None
        self._condition = None
        self._closed = True

    # Special Methods ----------------------------------------------------------------------------
    async def __aenter__(self) -> Pool:
        return self

    async def __aexit__(self, exc_type, exc, exc_tb) -> None:
        await self.close(None)

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _repr_val(self) -> str:
        return "host='%s', port=%d, size=[%s]" % (
            self._host,
            self._port,
            self.get_size_info(),
        )

    def __repr__(self) -> str:
        return "<%s (%s)>" % (self.__class__.__name__, self._repr_val())

    def __hash__(self) -> int:
        return id(self)

    def __del__(self):
        if not self._closed:
            logger.error(
                "%s is not closed properly. Please call `close()` "
                "to gracefully shutdown the Pool/Server." % self
            )
            self.quit()


# Server =======================================================================================
@cython.cclass
class Server(Pool):
    """Represents a Server, extending the functionalities of the `Pool` class to
    cater to `Database` and `Table` classes in this package.

    Key Differences with `Pool`:
    - 1.Autocommit Mode: Modification of the autocommit mode is prohibited. This
        ensures all query methods within `Database` and `Table` classes maintain
        consistent results, relying on `autocommit=False`.
    - 2.Query Timeout: Provides the `query_timeout` argument, defining the
        default timeout for all query methods in `Database` and `Table` classes
        associated with this Server. Each query method can still set its own
        timeout value to override this default per call.
    """

    _query_timeout: cython.int

    def __init__(
        self,
        host: str = "localhost",
        port: int = 3306,
        user: Union[str, None] = None,
        password: Union[str, None] = None,
        max_size: int = 10,
        recycle: int = 0,
        auth_plugin: Union[
            Literal[
                "mysql_native_password",
                "caching_sha2_password",
                "sha256_password",
                "mysql_clear_password",
            ],
            None,
        ] = None,
        server_public_key: Union[str, None] = None,
        unix_socket: Union[str, None] = None,
        ssl_context: Union[SSLContext, None] = None,
        charset: Union[str, None] = None,
        sql_mode: Union[str, None] = None,
        init_command: Union[str, None] = None,
        connect_timeout: int = 10,
        query_timeout: int = -1,
        cursorclass: type[Cursor] = Cursor,
        program_name: Union[str, None] = None,
        local_infile: bool = False,
        client_flag: int = 0,
        echo: bool = False,
        read_default_file: Union[str, None] = None,
        read_default_group: Union[str, None] = None,
    ) -> None:
        """The Server that manages and maintains connections of the MySQL Server.

        :param host: `<str>` Host of the database Server. Defaults to `'localhost'`.
        :param port: `<int>` Port of the database Server. Defaults to `3306`.
        :param user: `<str>` The username to login as. Defaults to `None`.
        :param password: `<str>` The password for user authentication. Defaults to `None`.
        :param max_size: `<int>` The maximum number of connections maintain by the Server pool. Defaults to `10`.
        :param recycle: `<int>` Total seconds a connection can be idle in the Pool before closed and removed. Defaults to `0`.
            For `recycle <= 0`, it means all connections in the pool stays permanent.
        :param auth_plugin: `<str>` The authorization plugin to be used. Defaults to `None` (Server settings).
        :param server_public_key: `<str>`  SHA256 authentication plugin public key value. Defaults to `None`.
        :param unix_socket: `<str>` Use the provide unix socket to establish connections. Defaults to `None`.
        :param ssl_context: `<ssl.SSLContext>` Force to establish connections with ssl. Defaults to `None`.
        :param charset: `<str>` The charset for the connections. Defaults to `None`.
        :param sql_mode: `<str>` The default SQL mode for the connections. Defaults to `None`.
        :param init_command: `<str>` The initial command to be executed once connection is established. Defaults to `None`.
        :param autocommit: `<bool>` Whether to auto commit after each query execution. Defaults to `False`.
        :param connect_timeout: `<int>` Timeout in seconds for establishing new connections. Defaults to `10`.
        :param query_timeout: `<int>` The default timeout in seconds for query methods. Defaults to `-1` (No timeout).
            This argument defines the default timeout for all query methods within `Database` and `Table`
            classes associated with this Server. Each query method can still set its own timeout value
            to override this default per call. Setting `query_timeout <= 0` means no default timeout.
        :param cursorclass: `<type[Cursor]>` The Cursor class to be used. Defaults to `Cursor`.
        :param program_name: `<str>` The program name to be used while handshaking. Defaults to `None`.
        :param local_infile: `<bool>` Whether to enable the use of LOAD DATA LOCAL command. Defaults to `False`.
        :param client_flag: `<int>` Custom flags to send to MySQL. Defaults to `0`.
            For more information, please refer tp `pymysql.constants.CLIENT`.
        :param echo: `<bool>` Whether to enable echo (log each executed queries). Defaults to `False`.
        :param read_default_file: `<str>` The path to the `my.cnf` file to load configuration. Defaults to `None`.
        :param read_default_group: `<str>` The group to read from the configuration file. Defaults to `None`.
        """
        super().__init__(
            host=host,
            port=port,
            user=user,
            password=password,
            max_size=max_size,
            recycle=recycle,
            database="mysql",
            auth_plugin=auth_plugin,
            server_public_key=server_public_key,
            unix_socket=unix_socket,
            ssl_context=ssl_context,
            charset=charset,
            sql_mode=sql_mode,
            init_command=init_command,
            autocommit=False,
            connect_timeout=connect_timeout,
            cursorclass=cursorclass,
            program_name=program_name,
            local_infile=local_infile,
            client_flag=client_flag,
            echo=echo,
            read_default_file=read_default_file,
            read_default_group=read_default_group,
        )
        self.set_query_timeout(query_timeout)

    # Properties ---------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def get_query_timeout(self) -> cython.int:
        """(cfunc) Get the default timeout for all query methods within
        `Database` and `Table` classes associated with this Server `<int>`.
        """
        return self._query_timeout

    @property
    def query_timeout(self) -> int:
        """The default timeout for all query methods withint `Database`
        and `Table` classes associated with this Server`<int>`.
        """
        return self.get_query_timeout()

    def set_query_timeout(self, seconds: int) -> None:
        """Set the default timeout for all query methods within `Database`
        and `Table` classes associated with this Server.

        :param seconds: `<int>` Timeout in seconds. `seconds<=0` means no default timeout.
        """
        self._query_timeout = seconds
        if self._query_timeout < 0:
            self._query_timeout = 0

    async def set_autocommit(self) -> None:
        """Modification of the autocommit mode is prohibited for Server. This
        ensures all query methods within `Database` and `Table` classes maintain
        consistent results, relying on `autocommit=False`.
        """
        raise errors.ServerError(
            "<Server> Modification of autocommit mode is prohibited."
        )


# Context Manager ==============================================================================
@cython.cclass
class CursorManager:
    "The Context Manager for Cursor."

    _cur: Cursor

    def __init__(self, cur: Cursor) -> None:
        """The Context Manager for Cursor.

        :param cur: An instance of a `Cursor`.
        """
        self._cur = cur

    async def close(self) -> None:
        try:
            await self._cur.close()
            self._cur = None
        except BaseException:
            self._cur = None
            raise

    async def __aenter__(self) -> Cursor:
        return self._cur

    async def __aexit__(self, exc_type, exc, exc_tb) -> None:
        try:
            await self.close()
        except BaseException as err:
            if exc is not None:
                exc.add_note("-> <Curosr> Failed to close: %s." % err)
            else:
                err.add_note("-> <Curosr> Failed to close.")
                raise err


@cython.cclass
class ConnectionManager:
    """The Context Manager for Connection.

    By acquiring connection through this manager, the following will happen:
    - 1. Establish the socket connection with the MySQL Sever.
    - 2. Return the `ConnectionManager` that wraps the connection.
    - 3. Safely close the connection at exist.
    """

    _conn: Connection

    def __init__(self, conn: Connection) -> None:
        """The Context Manager for Connection.

        :param conn: An instance of a `Connection`.
        """
        self._conn = conn

    async def connect(self) -> None:
        try:
            await self._conn.connect()
        except BaseException as err:
            await self.close()
            err.add_note("-> <Connection> Failed to establish connection.")
            raise err

    async def close(self) -> None:
        await self._conn.close()
        self._conn = None

    async def __aenter__(self) -> Connection:
        await self.connect()
        return self._conn

    async def __aexit__(self, exc_type, exc, exc_tb):
        await self.close()


@cython.cclass
class TransactionManager(ConnectionManager):
    """The Context Manager for Connection in transaction mode.

    By acquiring connection through this manager, the following will happen:
    - 1. Establish the socket connection with the MySQL Sever.
    - 2. Use the connection to `START` a transaction.
    - 3. Return the `TransactionManager` that wraps the connection.
    - 4a. If catches ANY exceptions during the transaction, execute
         `ROLLBACK` and then close the connection.
    - 4b. If the transaction executed successfully, execute `COMMIT`
         and then close the connection.
    """

    def __init__(self, conn: Connection) -> None:
        """The Context Manager for Connection in transaction mode.

        :param conn: An instance of a `Connection`."""
        super().__init__(conn)

    async def __aenter__(self) -> Connection:
        await self.connect()
        try:
            await self._conn.begin()
            return self._conn
        except BaseException as err:
            await self.close()
            err.add_note("-> <Connection> Failed to `BEGIN`.")
            raise err

    async def __aexit__(self, exc_type, exc, exc_tb):
        if exc is not None:
            try:
                await self._conn.rollback()
                await self.close()
            except BaseException as err:
                await self.close()
                exc.add_note("-> <Connection> Failed to `ROLLBACK`: %s." % err)
        else:
            try:
                await self._conn.commit()
                await self.close()
            except BaseException as err:
                await self.close()
                err.add_note("-> <Connection> Failed to `COMMIT`.")
                raise err


@cython.cclass
class PoolManager:
    _pool: Pool

    """The Context Manager for Pool.

    By acquiring Pool through this manager, the following will happen:
    - 1. Fill the Pool with 1 new connection (validation).
    - 2. Return the `PoolManager` that wraps the Pool.
    - 3. Safely close the Pool at exist.
    """

    def __init__(self, pool: Pool) -> None:
        """The Context Manager Pool.

        :param pool: An instance of a `Pool`.
        """
        self._pool = pool

    async def __aenter__(self) -> Pool:
        try:
            await self._pool.fill(1)
            return self._pool
        except BaseException as err:
            await self._pool.close(None)
            err.add_note("-> <Pool> Failed to establish connection.")
            raise err

    async def __aexit__(self, exc_type, exc, exc_tb) -> None:
        await self._pool.close(None)


@cython.cclass
class PoolConnectionManager(PoolManager):
    """The Context Manager for acquiring Connection from a Pool.

    By acquiring connection through this manager, the following will happen:
    - 1. Acquire a free/new connection from the Pool.
    - 2. Return `PoolConnectionManager` that wraps the connection.
    - 3. Release the connection back to the Pool at exist.
    """

    _conn: Connection

    def __init__(self, pool: Pool) -> None:
        """The Context Manager for acquiring Connection from a Pool.

        :param pool: An instance of a `Pool`.
        """
        super().__init__(pool)
        self._conn = None

    async def acquire(self) -> None:
        try:
            self._conn = await self._pool._acquire_connection()
        except BaseException as err:
            err.add_note("-> <Pool> Failed to acquire connection.")
            raise err

    async def release(self) -> None:
        await self._pool.release(self._conn)
        self._conn = None
        self._pool = None

    async def __aenter__(self) -> Connection:
        await self.acquire()
        return self._conn

    async def __aexit__(self, exc_type, exc, exc_tb) -> None:
        if exc is not None:
            await self._conn.close()
        await self.release()


@cython.cclass
class PoolTransactionManager(PoolConnectionManager):
    """The Context Manager for acquiring Connection from a Pool in transaction mode.

    By acquiring connection through this manager, the following will happen:
    - 1. Acquire a free/new connection from the Pool.
    - 2. Use the connection to `START` a transaction.
    - 3. Return `PoolTransactionManager` that wraps the connection.
    - 4a. If catches ANY exceptions during the transaction, execute
         `ROLLBACK`, then close and release the connection.
    - 4b. If the transaction executed successfully, execute `COMMIT`
          and then release the connection back to the pool.
    """

    def __init__(self, pool: Pool) -> None:
        """The Context Manager for acquiring Connection from a Pool in transaction mode.

        :param pool: An instance of a `Pool`.
        """
        super().__init__(pool)

    async def close_release(self) -> None:
        await self._conn.close()
        await self.release()

    async def __aenter__(self) -> Connection:
        await self.acquire()
        try:
            await self._conn.begin()
            return self._conn
        except BaseException as err:
            await self.close_release()
            err.add_note("-> <Pool> Connection failed to `BEGIN`.")
            raise err

    async def __aexit__(self, exc_type, exc, exc_tb) -> None:
        if exc is not None:
            try:
                await self._conn.rollback()
                await self.close_release()
            except BaseException as err:
                await self.close_release()
                exc.add_note("-> <Pool> Connection failed to `ROLLBACK`: %s." % err)
        else:
            try:
                await self._conn.commit()
                await self.release()
            except BaseException as err:
                await self.close_release()
                err.add_note("-> <Pool> Connection failed to `COMMIT`.")
                raise err


# Acquire Manager ==============================================================================
def connect(
    host: str = "localhost",
    port: cython.int = 3306,
    user: Union[str, None] = None,
    password: Union[str, None] = None,
    database: Union[str, None] = None,
    auth_plugin: Union[
        Literal[
            "mysql_native_password",
            "caching_sha2_password",
            "sha256_password",
            "mysql_clear_password",
        ],
        None,
    ] = None,
    server_public_key: Union[str, None] = None,
    unix_socket: Union[str, None] = None,
    ssl_context: Union[SSLContext, None] = None,
    charset: Union[str, None] = None,
    sql_mode: Union[str, None] = None,
    init_command: Union[str, None] = None,
    autocommit: cython.bint = False,
    connect_timeout: cython.int = 10,
    cursorclass: type[Cursor] = Cursor,
    program_name: Union[str, None] = None,
    local_infile: cython.bint = False,
    client_flag: int = 0,
    echo: cython.bint = False,
    read_default_file: Union[str, None] = None,
    read_default_group: Union[str, None] = None,
) -> ConnectionManager:
    """Establish a socket connection with the MySQL Server (Context Manager).

    By acquiring connection through this function with `async with` statement,
    the following will happen:
    - 1. Establish a socket connection with the MySQL Sever.
    - 2. Return the `ConnectionManager` that wraps the connection.
    - 3. Safely close the connection at exist.

    :param host: `<str>` Host of the database Server. Defaults to `'localhost'`.
    :param port: `<int>` Port of the database Server. Defaults to `3306`.
    :param user: `<str>` The username to login as. Defaults to `None`.
    :param password: `<str>` The password for user authentication. Defaults to `None`.
    :param database: `<str>` Specific database to use. Defaults to `None`.
    :param auth_plugin: `<str>` The authorization plugin to be used. Defaults to `None` (Server settings).
    :param server_public_key: `<str>`  SHA256 authentication plugin public key value. Defaults to `None`.
    :param unix_socket: `<str>` Use the provide unix socket to establish the connection. Defaults to `None`.
    :param ssl_context: `<ssl.SSLContext>` Force to establish the connection with ssl. Defaults to `None`.
    :param charset: `<str>` The charset for the connection. Defaults to `None`.
    :param sql_mode: `<str>` The default SQL mode for the connection. Defaults to `None`.
    :param init_command: `<str>` The initial command to be executed once connection is established. Defaults to `None`.
    :param autocommit: `<bool>` Whether to auto commit after each query execution. Defaults to `False`.
    :param connect_timeout: `<int>` Timeout in seconds for establishing the connection. Defaults to `10`.
    :param cursorclass: `<type[Cursor]>` The Cursor class to be used. Defaults to `Cursor`.
    :param program_name: `<str>` The program name to be used while handshaking. Defaults to `None`.
    :param local_infile: `<bool>` Whether to enable the use of LOAD DATA LOCAL command. Defaults to `False`.
    :param client_flag: `<int>` Custom flags to send to MySQL. Defaults to `0`.
        For more information, please refer tp `pymysql.constants.CLIENT`.
    :param echo: `<bool>` Whether to enable echo (log each executed queries). Defaults to `False`.
    :param read_default_file: `<str>` The path to the `my.cnf` file to load configuration. Defaults to `None`.
    :param read_default_group: `<str>` The group to read from the configuration file. Defaults to `None`.
    :return: `ConnectionManager`: Which wraps the established connection.
    """
    return ConnectionManager(
        _Connection(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            auth_plugin=auth_plugin,
            server_public_key=server_public_key,
            unix_socket=unix_socket,
            ssl_context=ssl_context,
            charset=charset,
            sql_mode=sql_mode,
            init_command=init_command,
            autocommit=autocommit,
            connect_timeout=connect_timeout,
            cursorclass=cursorclass,
            program_name=program_name,
            local_infile=local_infile,
            client_flag=client_flag,
            echo=echo,
            read_default_file=read_default_file,
            read_default_group=read_default_group,
        )
    )


def transaction(
    host: str = "localhost",
    user: str = None,
    password: str = "",
    database: str = None,
    port: int = 3306,
    unix_socket: str = None,
    charset: str = "",
    sql_mode: str = None,
    read_default_file: str = None,
    client_flag: int = 0,
    cursorclass: type[Cursor] = Cursor,
    init_command: str = None,
    connect_timeout: int = 10,
    read_default_group: str = None,
    autocommit: bool = False,
    echo: bool = False,
    local_infile: bool = False,
    ssl_context: bool | Any = None,
    auth_plugin: str = "",
    program_name: str = "",
    server_public_key: str = None,
) -> TransactionManager:
    """Establish a socket connection with the MySQL Server in transaction mode (Context Manager).

    By acquiring connection through this function with `async with` statement,
    the following will happen:
    - 1. Establish a socket connection with the MySQL Sever.
    - 2. Use the connection to `START` a transaction.
    - 3. Return the `TransactionManager` that wraps the connection.
    - 4a. If catches ANY exceptions during the transaction, execute
         `ROLLBACK` and then close the connection.
    - 4b. If the transaction executed successfully, execute `COMMIT`
         and then close the connection.

    :param host: `<str>` Host of the database Server. Defaults to `'localhost'`.
    :param port: `<int>` Port of the database Server. Defaults to `3306`.
    :param user: `<str>` The username to login as. Defaults to `None`.
    :param password: `<str>` The password for user authentication. Defaults to `None`.
    :param database: `<str>` Specific database to use. Defaults to `None`.
    :param auth_plugin: `<str>` The authorization plugin to be used. Defaults to `None` (Server settings).
    :param server_public_key: `<str>`  SHA256 authentication plugin public key value. Defaults to `None`.
    :param unix_socket: `<str>` Use the provide unix socket to establish the connection. Defaults to `None`.
    :param ssl_context: `<ssl.SSLContext>` Force to establish the connection with ssl. Defaults to `None`.
    :param charset: `<str>` The charset for the connection. Defaults to `None`.
    :param sql_mode: `<str>` The default SQL mode for the connection. Defaults to `None`.
    :param init_command: `<str>` The initial command to be executed once connection is established. Defaults to `None`.
    :param autocommit: `<bool>` Whether to auto commit after each query execution. Defaults to `False`.
    :param connect_timeout: `<int>` Timeout in seconds for establishing the connection. Defaults to `10`.
    :param cursorclass: `<type[Cursor]>` The Cursor class to be used. Defaults to `Cursor`.
    :param program_name: `<str>` The program name to be used while handshaking. Defaults to `None`.
    :param local_infile: `<bool>` Whether to enable the use of LOAD DATA LOCAL command. Defaults to `False`.
    :param client_flag: `<int>` Custom flags to send to MySQL. Defaults to `0`.
        For more information, please refer tp `pymysql.constants.CLIENT`.
    :param echo: `<bool>` Whether to enable echo (log each executed queries). Defaults to `False`.
    :param read_default_file: `<str>` The path to the `my.cnf` file to load configuration. Defaults to `None`.
    :param read_default_group: `<str>` The group to read from the configuration file. Defaults to `None`.
    :return: `TransactionManager`: Which wraps the established connection.
    """
    return TransactionManager(
        _Connection(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            unix_socket=unix_socket,
            charset=charset,
            sql_mode=sql_mode,
            read_default_file=read_default_file,
            client_flag=client_flag,
            cursorclass=cursorclass,
            init_command=init_command,
            connect_timeout=connect_timeout,
            read_default_group=read_default_group,
            autocommit=autocommit,
            echo=echo,
            local_infile=local_infile,
            ssl_context=ssl_context,
            auth_plugin=auth_plugin,
            program_name=program_name,
            server_public_key=server_public_key,
        )
    )


def acquire_pool(
    host: str = "localhost",
    port: cython.int = 3306,
    user: Union[str, None] = None,
    password: Union[str, None] = None,
    max_size: cython.int = 10,
    recycle: cython.int = 0,
    database: Union[str, None] = None,
    auth_plugin: Union[
        Literal[
            "mysql_native_password",
            "caching_sha2_password",
            "sha256_password",
            "mysql_clear_password",
        ],
        None,
    ] = None,
    server_public_key: Union[str, None] = None,
    unix_socket: Union[str, None] = None,
    ssl_context: Union[SSLContext, None] = None,
    charset: Union[str, None] = None,
    sql_mode: Union[str, None] = None,
    init_command: Union[str, None] = None,
    autocommit: cython.bint = False,
    connect_timeout: cython.int = 10,
    cursorclass: type[Cursor] = Cursor,
    program_name: Union[str, None] = None,
    local_infile: cython.bint = False,
    client_flag: cython.int = 0,
    echo: cython.bint = False,
    read_default_file: Union[str, None] = None,
    read_default_group: Union[str, None] = None,
) -> PoolManager:
    """Create a Pool that manages and maintains connections of the MySQL Server (Context Manager).

    By creating Pool through this function with `async with` statement,
    the following will happen:
    - 1. Create a new connection Pool.
    - 2. Fill the Pool with 1 new connection (validation).
    - 3. Return the `PoolManager` that wraps the Pool.
    - 4. Safely close the Pool at exist.

    :param host: `<str>` Host of the database Server. Defaults to `'localhost'`.
    :param port: `<int>` Port of the database Server. Defaults to `3306`.
    :param user: `<str>` The username to login as. Defaults to `None`.
    :param password: `<str>` The password for user authentication. Defaults to `None`.
    :param max_size: `<int>` The maximum number of connections maintain by the Pool. Defaults to `10`.
    :param recycle: `<int>` Total seconds a connection can be idle in the Pool before closed and removed. Defaults to `0`.
        For `recycle <= 0`, it means all connections in the pool stays permanent.
    :param database: `<str>` Specific database to use. Defaults to `None`.
    :param auth_plugin: `<str>` The authorization plugin to be used. Defaults to `None` (Server settings).
    :param server_public_key: `<str>`  SHA256 authentication plugin public key value. Defaults to `None`.
    :param unix_socket: `<str>` Use the provide unix socket to establish connections. Defaults to `None`.
    :param ssl_context: `<ssl.SSLContext>` Force to establish connections with ssl. Defaults to `None`.
    :param charset: `<str>` The charset for the connections. Defaults to `None`.
    :param sql_mode: `<str>` The default SQL mode for the connections. Defaults to `None`.
    :param init_command: `<str>` The initial command to be executed once connection is established. Defaults to `None`.
    :param autocommit: `<bool>` Whether to auto commit after each query execution. Defaults to `False`.
    :param connect_timeout: `<int>` Timeout in seconds for establishing new connections. Defaults to `10`.
    :param cursorclass: `<type[Cursor]>` The Cursor class to be used. Defaults to `Cursor`.
    :param program_name: `<str>` The program name to be used while handshaking. Defaults to `None`.
    :param local_infile: `<bool>` Whether to enable the use of LOAD DATA LOCAL command. Defaults to `False`.
    :param client_flag: `<int>` Custom flags to send to MySQL. Defaults to `0`.
        For more information, please refer tp `pymysql.constants.CLIENT`.
    :param echo: `<bool>` Whether to enable echo (log each executed queries). Defaults to `False`.
    :param read_default_file: `<str>` The path to the `my.cnf` file to load configuration. Defaults to `None`.
    :param read_default_group: `<str>` The group to read from the configuration file. Defaults to `None`.
    :return: `PoolManager`: Which wraps the created Pool.
    """
    return PoolManager(
        Pool(
            host=host,
            port=port,
            user=user,
            password=password,
            max_size=max_size,
            recycle=recycle,
            database=database,
            auth_plugin=auth_plugin,
            server_public_key=server_public_key,
            unix_socket=unix_socket,
            ssl_context=ssl_context,
            charset=charset,
            sql_mode=sql_mode,
            init_command=init_command,
            autocommit=autocommit,
            connect_timeout=connect_timeout,
            cursorclass=cursorclass,
            program_name=program_name,
            local_infile=local_infile,
            client_flag=client_flag,
            echo=echo,
            read_default_file=read_default_file,
            read_default_group=read_default_group,
        )
    )
