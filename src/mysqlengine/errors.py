# cython: language_level=3

# Cython imports
import cython
from cython.cimports.cpython.int import PyInt_Check as is_int  # type: ignore
from cython.cimports.cpython.unicode import PyUnicode_GET_LENGTH as str_len  # type: ignore
from cython.cimports.mysqlengine import constant, utils  # type: ignore

# Python imports
from random import uniform
from struct import unpack as struct_unpack
from asyncio import sleep, TimeoutError, IncompleteReadError
from mysqlengine.logs import logger
from mysqlengine import constant, utils


# Exceptions =================================================================================
# . Base Exceptions
class MysqlEngineError(Exception):
    """Mysql Engine Error."""


# . Engine Exceptions
class EngineError(MysqlEngineError):
    """Engine Error."""


class EngineDatabaseError(EngineError, ValueError):
    """Engine Database Error."""


class EngineDatabaseAccessKeyError(EngineDatabaseError, KeyError):
    """Engine Database Access Key Error."""


class EngineDatabaseInstanciateError(EngineDatabaseError):
    """Engine Database Instanciate Error."""


# . Server Exceptions
class ServerError(MysqlEngineError):
    """Server Error."""


# . Database Exceptions
class DatabaseError(MysqlEngineError):
    """Database Error."""


class DatabaseMetadataError(DatabaseError, ValueError):
    """Mysql Engine Database Attribute Error."""


# . Table Exceptions
class TableError(MysqlEngineError):
    """Table Error."""


class TableMetadataError(TableError, ValueError):
    """Mysql Engine Table Attribute Error."""


class TableNotExistError(TableError, KeyError):
    """Table Not Exists Error."""


class TableExportError(TableError):
    """Exception raised when exporting the table data encouters any error."""


class TableImportError(TableError):
    """Exception raised when importing the table data encouters any error."""


# . Column Exceptions
class ColumnError(TableError):
    """Column Error."""


class ColumnMetadataError(ColumnError, ValueError):
    """Mysql Engine Column Metadata Error."""


class ColumnNotExistError(ColumnError, KeyError):
    """Column Not Exists Error."""


# . Index Exceptions
class IndexError(TableError):
    """Index Error."""


class IndexMetadataError(IndexError, ValueError):
    """Mysql Engine Index Metadata Error."""


class IndexNotExistError(IndexError, KeyError):
    """Index Not Exists Error."""


# . Data Type Exceptions
class DataTypeError(MysqlEngineError):
    """Data Type Error."""


class DataTypeMetadataError(DataTypeError, ValueError):
    """Data Type Metadata Error."""


# Query Exceptions ===========================================================================
class QueryError(MysqlEngineError):
    """SQL Query Error."""


class QueryValueError(QueryError, ValueError):
    """Exception raised when sql query related values are wrong.
    Not related to Query execution itself."""


class QueryDataError(QueryValueError):
    """Exception raised when sql query fetch data values are wrong.
    Not related to Query execution itself."""


class QueryDataValidationError(QueryValueError):
    """Exception raised when sql query column data values are wrong.
    Values can't be parsed into column's data type or don't satisfied
    the column design."""


class QueryWarning(QueryError, Warning):
    """Exception raised for important warnings like data truncations
    while inserting, etc."""


class QueryExeError(QueryError):
    """Exception raised when the sql query execution failed.
    This exception is raised when the query execution failed."""


class QueryErrorHandler(QueryExeError):
    """This error is use to be catched by the error_handler decorator,
    so the error_handler will catch this error and re-execute the
    function until success."""


class QueryTimeoutError(QueryErrorHandler, TimeoutError):
    """Exception raised when the the query execution timeout is reached."""


class QueryInterfaceError(QueryErrorHandler):
    """Exception raised for errors that are related to the database
    interface rather than the database itself."""


class QueryOperationalError(QueryErrorHandler):
    """Exception raised for errors that are related to the database's
    operation and not necessarily under the control of the programmer,
    e.g. an unexpected disconnect occurs, the data source name is not
    found, a transaction could not be processed, a memory allocation
    error occurred during processing, etc."""


class QueryIncompleteReadError(QueryOperationalError, IncompleteReadError):
    """Exception raised when the server unexpectedly disconnects while
    program is still reading data from it."""


class QueryDataError(QueryExeError):
    """Exception raised for errors that are due to problems with the
    processed data like division by zero, numeric value out of range,
    etc."""


class QueryDatabaseError(QueryExeError, DatabaseError):
    """Exception raised for errors that are related to the database."""


class QueryIntegrityError(QueryExeError):
    """Exception raised when the relational integrity of the database
    is affected, e.g. a foreign key check fails, duplicate key,
    etc."""


class QueryInternalError(QueryExeError):
    """Exception raised when the database encounters an internal
    error, e.g. the cursor is not valid anymore, the transaction is
    out of sync, etc."""


class QueryProgrammingError(QueryExeError):
    """Exception raised for programming errors, e.g. table not found
    or already exists, syntax error in the SQL statement, wrong number
    of parameters specified, etc."""


class QueryTableAbsentError(QueryProgrammingError):
    """Exception raised when the table does not exists."""


class QueryNotSupportedError(QueryExeError):
    """Exception raised in case a method or database API was used
    which is not supported by the database, e.g. requesting a
    .rollback() on a connection that does not support transaction or
    has transactions turned off."""


class QueryUnknownError(QueryExeError):
    """Exception raised when the error is not in the list of known errors."""


QUERY_ERROR_MAP: dict[int, QueryError] = {
    # Operational Error
    constant.ER_DBACCESS_DENIED_ERROR: QueryOperationalError,
    constant.ER_ACCESS_DENIED_ERROR: QueryOperationalError,
    constant.ER_CON_COUNT_ERROR: QueryOperationalError,
    constant.ER_TABLEACCESS_DENIED_ERROR: QueryOperationalError,
    constant.ER_COLUMNACCESS_DENIED_ERROR: QueryOperationalError,
    constant.ER_CONSTRAINT_FAILED: QueryOperationalError,
    constant.ER_LOCK_DEADLOCK: QueryOperationalError,
    # Data Error
    constant.ER_WARN_DATA_TRUNCATED: QueryDataError,
    constant.ER_WARN_NULL_TO_NOTNULL: QueryDataError,
    constant.ER_WARN_DATA_OUT_OF_RANGE: QueryDataError,
    constant.ER_NO_DEFAULT: QueryDataError,
    constant.ER_PRIMARY_CANT_HAVE_NULL: QueryDataError,
    constant.ER_DATA_TOO_LONG: QueryDataError,
    constant.ER_DATETIME_FUNCTION_OVERFLOW: QueryDataError,
    constant.ER_TRUNCATED_WRONG_VALUE_FOR_FIELD: QueryDataError,
    constant.ER_ILLEGAL_VALUE_FOR_TYPE: QueryDataError,
    # Integrity Error
    constant.ER_DUP_ENTRY: QueryIntegrityError,
    constant.ER_NO_REFERENCED_ROW: QueryIntegrityError,
    constant.ER_NO_REFERENCED_ROW_2: QueryIntegrityError,
    constant.ER_ROW_IS_REFERENCED: QueryIntegrityError,
    constant.ER_ROW_IS_REFERENCED_2: QueryIntegrityError,
    constant.ER_CANNOT_ADD_FOREIGN: QueryIntegrityError,
    constant.ER_BAD_NULL_ERROR: QueryIntegrityError,
    # Programming Error
    constant.ER_DB_CREATE_EXISTS: QueryProgrammingError,
    constant.ER_NO_DB_ERROR: QueryProgrammingError,
    constant.ER_SYNTAX_ERROR: QueryProgrammingError,
    constant.ER_PARSE_ERROR: QueryProgrammingError,
    constant.ER_NO_SUCH_TABLE: QueryTableAbsentError,
    constant.ER_WRONG_DB_NAME: QueryProgrammingError,
    constant.ER_WRONG_TABLE_NAME: QueryProgrammingError,
    constant.ER_FIELD_SPECIFIED_TWICE: QueryProgrammingError,
    constant.ER_INVALID_GROUP_FUNC_USE: QueryProgrammingError,
    constant.ER_UNSUPPORTED_EXTENSION: QueryProgrammingError,
    constant.ER_TABLE_MUST_HAVE_COLUMNS: QueryProgrammingError,
    constant.ER_CANT_DO_THIS_DURING_AN_TRANSACTION: QueryProgrammingError,
    constant.ER_WRONG_DB_NAME: QueryProgrammingError,
    constant.ER_WRONG_COLUMN_NAME: QueryProgrammingError,
    constant.ER_INVALID_DEFAULT: QueryProgrammingError,
    constant.ER_FILE_NOT_FOUND: QueryProgrammingError,
    constant.ER_WRONG_FIELD_SPEC: QueryProgrammingError,
    constant.ER_SP_DOES_NOT_EXIST: QueryProgrammingError,
    constant.ER_BAD_FIELD_ERROR: QueryProgrammingError,
    constant.ER_WRONG_FIELD_WITH_GROUP: QueryProgrammingError,
    constant.ER_WRONG_GROUP_FIELD: QueryProgrammingError,
    # Database Error
    constant.ER_SERVER_SHUTDOWN: QueryDatabaseError,
    # Not Supported Error
    constant.ER_WARNING_NOT_COMPLETE_ROLLBACK: QueryNotSupportedError,
    constant.ER_NOT_SUPPORTED_YET: QueryNotSupportedError,
    constant.ER_FEATURE_DISABLED: QueryNotSupportedError,
    constant.ER_UNKNOWN_STORAGE_ENGINE: QueryNotSupportedError,
}


@cython.cfunc
@cython.inline(True)
def raise_query_exc(data: bytes):
    """Raise a query exception from the error packet."""
    err_code: cython.int = struct_unpack("<h", data[1:3])[0]
    err_msg: str = data[9:].decode("utf-8", "replace")
    err = QUERY_ERROR_MAP.get(err_code)
    if err is None:
        if err_code < 1000:
            err = QueryInternalError
        else:
            err = QueryOperationalError
    raise err(err_code, err_msg)


ASYNCIO_ERRORS_MAP: dict[Exception, Exception] = {
    TimeoutError: QueryTimeoutError,
    IncompleteReadError: QueryIncompleteReadError,
}


def raise_exc(exc: Exception):
    """Raise a query exception.

    This handle will try to convert asyncio exceptions to query exceptions.
    If the exception is not a asyncio exception, it will be raised directly.
    """
    err = ASYNCIO_ERRORS_MAP.get(exc.__class__)
    if err is None:
        raise exc
    else:
        raise err(*exc.args) from exc


@cython.cfunc
@cython.inline(True)
@cython.exceptval(check=False)
def parse_query_exc_code(exc: Exception) -> cython.int:
    """Parse the error code from a query exception."""
    if exc.args:
        code = exc.args[0]
        if is_int(code):
            return code

    err: str = str(exc)
    length: cython.int = str_len(err)
    if length == 0:
        return -1
    elif err[0] != "(":
        return -1
    try:
        return utils._str_parse_int(err)
    except Exception:
        return -1


# Handler ====================================================================================
QUERY_ERROR_FATAL: set[int] = {
    constant.ER_NON_UNIQ_ERROR,
    constant.ER_DUP_ENTRY,
    constant.ER_TABLE_DEF_CHANGED,
}


def query_exc_handler(
    retry_times: cython.int = -1,
    min_wait_time: float = 0.2,
    max_wait_time: float = 2.0,
):
    """Query exception handler for query execution. (Decorator).

    If the decorated function raise exception that is a subclass of
    `QueryErrorHandler`, this decorator will catch the error and
    re-execute the query in a loop until success or when the preset
    `retry_times` is reached.

    QueryErrorHandler subclass exceptions:
    - QueryTimeoutError
    - QueryInterfaceError
    - QueryOperationalError
    - QueryIncompleteReadError

    Notice: For certain fatil query exceptions, the error will be raised
    directly. Please refer to the `QUERY_ERROR_FATAL` constant for
    more information.

    :param retry_times: `<int>` The maximum retry times. Default is `-1`.
        If `retry_times <= 0`, retry forever until success.
    :param min_wait_time: `<float>` The minimum wait time between each retry. Default is `0.2`.
    :param max_wait_time: `<float>` The maximum wait time between each retry. Default is `2.0`.
    """

    def decorator(func):
        async def wrapper(*args, **kwargs):
            retry_count: cython.longlong = 0
            while True:
                # Execute the function
                try:
                    return await func(*args, **kwargs)
                # Catch handler exception
                except QueryErrorHandler as err:
                    # Check for fatal operational errors and raise
                    if parse_query_exc_code(err) in QUERY_ERROR_FATAL:
                        raise err

                    # Reach the retry limit and raise the error
                    if retry_times > 0 and retry_count >= retry_times:
                        raise err

                    # Log the error
                    if retry_count >= 10:
                        logger.warning(err)

                    # Randomize wait time
                    await sleep(uniform(min_wait_time, max_wait_time))

                    # Increase retry count
                    retry_count += 1

        return wrapper

    return decorator
