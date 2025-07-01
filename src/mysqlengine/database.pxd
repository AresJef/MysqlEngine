# cython: language_level=3
from sqlcycli.charset cimport Charset
from mysqlengine.table cimport Tables, TempTableManager
from mysqlengine.element cimport Element, Logs, Metadata, Query
from mysqlengine.dml cimport InsertDML, ReplaceDML, UpdateDML, DeleteDML

# Database
cdef class Database(Element):
    cdef:
        # . configs
        int _encryption
        bint _read_only
        # . internal
        Tables _tables
        bint _setup_finished
    # DML
    cpdef InsertDML Insert(self, object table, object partition=?, bint ignore=?, object priority=?)
    cpdef ReplaceDML Replace(self, object table, object partition=?, bint low_priority=?)
    cpdef UpdateDML Update(self, object table, object partition=?, bint ignore=?, bint low_priority=?, object alias=?)
    cpdef DeleteDML Delete(self, object table, object partition=?, bint ignore=?, bint low_priority=?, bint quick=?, object alias=?, object multi_tables=?)
    cpdef TempTableManager CreateTempTable(self, object conn, object name, object temp_table)
    # Sync
    cpdef Logs Initialize(self, bint force=?)
    cpdef Logs Create(self, bint if_not_exists=?)
    cpdef bint Exists(self) except -1
    cpdef Logs Drop(self, bint if_exists=?)
    cpdef Logs Alter(self, object charset=?, object collate=?, object encryption=?, object read_only=?)
    cpdef DatabaseMetadata ShowMetadata(self)
    cpdef Logs SyncFromRemote(self, bint thorough=?)
    cpdef Logs SyncToRemote(self)
    # Generate SQL
    cpdef str _gen_create_sql(self, bint if_not_exists)
    cpdef str _gen_exists_sql(self)
    cpdef str _gen_drop_sql(self, bint if_exists)
    cpdef Query _gen_alter_query(self, DatabaseMetadata meta, object charset, object collate, object encryption, object read_only)
    cpdef str _gen_show_metadata_sql(self)
    cpdef str _gen_lock_sql(self, tuple tables, bint lock_for_read)
    # Metadata
    cpdef Logs _sync_from_metadata(self, DatabaseMetadata meta, Logs logs=?)
    # Setter
    cpdef bint setup(self) except -1
    # Assure Ready
    cdef inline bint _assure_setup_ready(self) except -1
    # Validate
    cdef inline int _validate_read_only(self, object read_only) except -2
    # Copy
    cpdef Database copy(self)

# Metadata
cdef class DatabaseMetadata(Metadata):
    cdef:
        # Base data
        str _db_name
        Charset _charset
        bint _encryption
        str _options
        # Additional data
        bint _read_only
