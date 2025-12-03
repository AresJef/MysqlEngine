# cython: language_level=3
from cpython cimport datetime
from cytimes.pydt cimport _Pydt
from sqlcycli.charset cimport Charset
from sqlcycli.aio.pool cimport PoolConnection, PoolSyncConnection
from mysqlengine.index cimport Indexes
from mysqlengine.column cimport Columns
from mysqlengine.constraint cimport Constraints
from mysqlengine.partition cimport Partitioning
from mysqlengine.dml cimport InsertDML, ReplaceDML, UpdateDML, DeleteDML
from mysqlengine.element cimport Element, Elements, Logs, Metadata, Query

# BaseTable
cdef class BaseTable(Element):
    cdef:
        # . options
        str _engine
        str _comment
        int _encryption
        str _row_format
        int _partitioned
        # . internal
        Columns _columns
        Indexes _indexes
        Constraints _constraints
        Partitioning _partitioning
        bint _temporary
        PoolSyncConnection _sync_conn
        PoolConnection _async_conn
        bint _setup_finished
    # DML
    cpdef InsertDML Insert(self, object partition=?, bint ignore=?, object priority=?)
    cpdef ReplaceDML Replace(self, object partition=?, bint low_priority=?)
    cpdef UpdateDML Update(self, object partition=?, bint ignore=?, bint low_priority=?, object alias=?)
    cpdef DeleteDML Delete(self, object partition=?, bint ignore=?, bint low_priority=?, bint quick=?, object alias=?, object multi_tables=?)
    # Generate SQL
    cpdef str _gen_create_sql(self, bint if_not_exists)
    cpdef str _gen_exists_sql(self)
    cpdef str _gen_truncate_sql(self)
    cpdef str _gen_drop_sql(self, bint if_exists)
    cpdef str _gen_empty_sql(self)
    cpdef Query _gen_alter_query(self, TableMetadata meta, str engine, object charset, str collate, str comment, object encryption, str row_format)
    cpdef str _gen_show_metadata_sql(self)
    cpdef str _gen_show_column_names_sql(self)
    cpdef str _gen_show_index_names_sql(self)
    cpdef str _gen_show_constraint_symbols_sql(self)
    cpdef str _gen_lock_sql(self, bint lock_for_read)
    cpdef str _gen_analyze_sql(self, bint write_to_binlog)
    cpdef str _gen_check_sql(self, tuple options)
    cpdef str _gen_optimize_sql(self, bint write_to_binlog)
    cpdef str _gen_repair_sql(self, bint write_to_binlog, object option)
    # Metadata
    cpdef Logs _sync_from_metadata(self, TableMetadata meta, Logs logs=?)
    # Setter
    cpdef bint setup(self, str db_name, object charset, str collate, object pool) except -1
    # Assure Ready
    cdef inline bint _assure_setup_ready(self) except -1
    # Validate
    cdef inline str _validate_engine(self, object engine)
    cdef inline str _validate_row_format(self, object row_format)
    cdef inline str  _validate_check_option(self, object option)
    cdef inline str _validate_repair_option(self, object option)

# Table
cdef class Table(BaseTable):
    # Sync
    cpdef Logs Initialize(self, bint force=?)
    cpdef Logs Create(self, bint if_not_exists=?)
    cpdef bint Exists(self) except -1
    cpdef Logs Truncate(self)
    cpdef Logs Drop(self, bint if_exists=?)
    cpdef bint Empty(self) except -1
    cpdef Logs Alter(self, str engine=?, object charset=?, str collate=?, str comment=?, object encryption=?, str row_format=?)
    cpdef TableMetadata ShowMetadata(self)
    cpdef tuple ShowColumnNames(self)
    cpdef tuple ShowIndexNames(self)
    cpdef tuple ShowConstraintSymbols(self)
    cpdef PoolSyncConnection Lock(self, PoolSyncConnection conn, bint lock_for_read=?)
    cpdef tuple Analyze(self, bint write_to_binlog=?)
    cpdef tuple Optimize(self, bint write_to_binlog=?)
    cpdef tuple Repair(self, bint write_to_binlog=?, str option=?)
    cpdef Logs SyncFromRemote(self, bint thorough=?)
    cpdef Logs SyncToRemote(self)
    # Copy
    cpdef Table copy(self)

# Time Table
cdef class TimeTable(Table):
    cdef:
        str _time_column
        bint _timestamp_based
        int _time_unit
        str _pydt_unit
        _Pydt _start_from
        _Pydt _end_with
    # Sync
    cpdef Logs _ExtendToTime(self, bint future, object to_time)
    cpdef Logs _CoalesceToTime(self, bint future, object to_time)
    cpdef Logs _DropToTime(self, bint future, object to_time)
    cpdef Logs ReorganizeOverflow(self, str catcher=?)
    cpdef tuple ShowPartitionNames(self)
    cpdef dict ShowPartitionRows(self)
    cpdef _Pydt GetBoundaryPartitionTime(self, bint future=?)
    cpdef int _GetInRangePartitionCount(self) except -1
    cpdef object _GetOverflowBoundaryValue(self, bint future)
    # Generate SQL
    cpdef str _gen_get_overflow_boundary_value_sql(self, bint future)
    # Time Tools
    cdef inline str _gen_partition_name(self, _Pydt time)
    cdef inline _Pydt _gen_partition_time(self, _Pydt time)
    cdef inline _Pydt _parse_partition_time(self, str name)
    cdef inline _Pydt _shift_partition_time(self, _Pydt time, int offset)
    cdef object _create_partition(self, str name, _Pydt value, str comment)
    cdef object _create_past_partition(self, _Pydt value)
    cdef object _create_future_partition(self)
    cdef object _create_in_range_partition(self, _Pydt time)
    cdef long long _cal_partition_time_diff(self, _Pydt time1, _Pydt time2)
    # Validate
    cdef inline int _validate_time_unit(self, object time_unit) except -1
    cdef inline str _validate_pydt_unit(self, int time_unit)
    cdef inline _Pydt _validate_partition_time(self, object dtobj, str arg_name)
    # Error
    cdef inline bint _raise_partitioning_broken_error(self, Exception tb_exc=?) except -1
    # Copy
    cpdef TimeTable copy(self)

# Temporary Table
cdef class TempTable(BaseTable):
    # Sync
    cpdef Logs Create(self, bint if_not_exists=?)
    cpdef Logs Drop(self, bint if_exists=?)
    cpdef bint Empty(self) except -1
    # Connection
    cpdef bint _set_connection(self, object conn) except -1
    cpdef bint _del_connection(self) except -1
    # Assure Ready
    cdef inline bint _assure_sync_connection_ready(self) except -1
    cdef inline bint _assure_async_connection_ready(self) except -1
    # Copy
    cpdef TempTable copy(self)

cdef class TempTableManager:
    cdef:
        TempTable _temp_table
        bint _sync_mode
    # Special Method
    cdef inline bint _cleanup(self) except -1

# Tables
cdef class Tables(Elements):
    # Setter
    cpdef bint setup(self, str db_name, object charset, str collate, object pool) except -1
    # Copy
    cpdef Tables copy(self)

# Metadata
cdef class TableMetadata(Metadata):
    cdef:
        # Base data
        str _db_name
        str _tb_name
        str _engine
        str _row_format
        Charset _charset
        str _options
        str _comment
        # Addtional data
        bint _encryption
        bint _partitioned
