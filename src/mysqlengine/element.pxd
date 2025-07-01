# cython: language_level=3
from sqlcycli.sqlfunc cimport ObjStr
from sqlcycli.charset cimport Charset
from sqlcycli.connection cimport Cursor
from sqlcycli.aio.pool cimport (
    Pool,
    PoolSyncConnection,
    PoolConnectionManager,
    PoolTransactionManager,
)

# Element
cdef class Element(ObjStr):
    cdef:
        # . internal
        str _el_cate
        str _el_type
        str _el_repr
        int _el_position
        bint _el_ready
        bint _initialized
        Py_ssize_t _hashcode
        # . settings
        str _name
        str _symbol
        str _tb_name
        str _db_name
        str _tb_qualified_name
        Charset _charset
        Pool _pool
    # Acquire / Fill / Release
    cpdef PoolConnectionManager acquire(self)
    cpdef PoolTransactionManager transaction(self)
    cpdef object release(self, object conn)
    # Sync
    cpdef tuple ShowDatabases(self)
    cpdef str ShowCreateTable(self)
    cpdef PoolSyncConnection Unlock(self, PoolSyncConnection conn)
    # SQL
    cpdef str _gen_show_create_table_sql(self)
    # Setter
    cpdef bint _set_el_type(self, str el_type) except -1
    cpdef bint set_name(self, object name) except -1
    cpdef bint _set_tb_name(self, object name) except -1
    cpdef bint _set_db_name(self, object name) except -1
    cpdef bint _set_tb_qualified_name(self) except -1
    cpdef bint _set_charset(self, object charset=?, object collate=?) except -1
    cpdef bint _set_pool(self, object pool) except -1
    cpdef bint _set_position(self, int position) except -1
    # Assure Ready
    cpdef bint _assure_ready(self) except -1
    cdef inline bint _assure_name_ready(self) except -1
    cdef inline bint _assure_tb_name_ready(self) except -1
    cdef inline bint _assure_db_name_ready(self) except -1
    cdef inline bint _assure_charset_ready(self) except -1
    cdef inline bint _assure_pool_ready(self) except -1
    cdef inline bint _assure_encoding_ready(self) except -1
    cdef inline bint _assure_position_ready(self) except -1
    # Validate
    cdef inline str _validate_database_name(self, object name)
    cdef inline str _validate_table_name(self, object name)
    cdef inline str _validate_column_name(self, object name)
    cdef inline str _validete_index_name(self, object name)
    cdef inline str _validate_constraint_name(self, object name)
    cdef inline str _validate_partition_name(self, object name)
    cdef inline tuple _validate_columns(self, object columns)
    cdef inline Pool _validate_pool(self, object pool)
    cdef inline Charset _validate_charset(self, object charset=?, object collate=?)
    cdef inline Charset _validate_encoding(self, Charset charset)
    cdef inline str _validate_index_type(self, object row_format)
    cdef inline str _validate_comment(self, object comment)
    cdef inline str _validate_expression(self, object expr)
    cdef inline int _validate_encryption(self, object encryption) except -2
    # Error
    cdef inline bint _raise_element_error(self, str err_type, str msg, Exception tb_exc=?) except -1
    cdef inline bint _raise_definition_error(self, str msg, Exception tb_exc=?) except -1
    cdef inline bint _raise_argument_error(self, str msg, Exception tb_exc=?) except -1
    cdef inline bint _raise_metadata_error(self, str msg, Exception tb_exc=?) except -1
    cdef inline bint _raise_not_exists_error(self, str msg, Exception tb_exc=?) except -1
    cdef inline bint _raise_critical_error(self, str msg, Exception tb_exc=?) except -1
    cdef inline bint _raise_operational_error(self, object errno, str msg, Exception tb_exc=?) except -1
    cdef inline bint _raise_not_implemented_error(self, str method_name) except -1
    cdef inline str _prep_error_message(self, str msg)
    # Warning
    cdef inline bint _warn(self, str msg) except -1
    # Internal
    cpdef bint _set_initialized(self, bint flag) except -1
    # Utils
    cdef inline object _escape_args(self, object args, bint itemize=?)
    cdef inline str _format_sql(self, str sql, object args, bint itemize=?)
    cdef inline str _gen_tb_qualified_name(self, str name)
    cdef inline tuple _flatten_rows(self, tuple rows, bint skip_none=?)
    cdef inline str _partitioning_flag_to_method(self, int flag)
    cdef inline int _partitioning_method_to_flag(self, str method) except -1
    # Copy
    cpdef Element copy(self)
    # Special Methods
    cpdef Py_ssize_t _sp_hashcode(self)
    cpdef int _sp_equal(self, object o) except -2
    cpdef int _sp_less_than(self, object o) except -2

# Elements
cdef class Elements(Element):
    cdef:
        object _el_class
        dict _el_dict
        set _el_set
        Py_ssize_t _size
    # Collection
    cpdef bint add(self, object element) except -1
    cpdef bint remove(self, object name) except -1
    cpdef Elements _search_name(self, object names, bint exact)
    cpdef Elements _search_type(self, object types, bint exact)
    cpdef Elements _filter(self, object elements)
    cpdef bint _issubset(self, object elements) except -1
    # Generate SQL
    cpdef str _gen_definition_sql(self, int indent=?)
    # Assure Ready
    cpdef bint _assure_ready(self) except -1
    # Validate
    cdef inline list _validate_elements(self, object elements)
    # Utils
    cdef inline set _extract_elements(self, object elements, str msg)
    cdef inline set _extract_element_names(self, object elements, str msg)
    cdef inline set _extract_element_types(self, object elements, str msg)
    # Accessors
    cpdef tuple keys(self)
    cpdef tuple values(self)
    cpdef tuple items(self)
    cpdef object get(self, object key, object default=?)
    cdef inline list _sorted_elements(self)

# Metadata
cdef class Metadata:
    cdef:
        str _el_cate
        dict _meta
        int _size
        Py_ssize_t _hashcode
    # Error
    cdef inline bint _raise_invalid_metadata_error(self, object meta, Exception tb_exc=?) except -1
    # Accessors
    cpdef tuple keys(self)
    cpdef tuple values(self)
    cpdef tuple items(self)
    cpdef object get(self, str key, object default=?)

# Query
cdef class Query:
    cdef:
        Logs _logs
        str _sql1
        str _sql2
    # Execute
    cpdef bint executable(self) except -1
    cpdef bint execute(self, Cursor cur) except -1
    # Setter
    cpdef bint set_sql(self, Element element, str sql) except -1

# Logs
cdef class Logs:
    cdef:
        list _records
        bint _skip_flag
        Py_ssize_t _size
    # Logging
    cpdef Logs log(self, Element element, bint local, str msg)
    cpdef Logs log_element_creation(self, Element element, bint local)
    cpdef Logs log_element_deletion(self, Element element, bint local)
    cpdef Logs log_sql(self, Element element, str sql)
    cpdef Logs log_charset(self, Element element, Charset config_old, Charset config_new)
    cpdef Logs log_config_bool(self, Element element, str config_name, int config_old, int config_new)
    cpdef Logs log_config_int(self, Element element, str config_name, long long config_old, long long config_new)
    cpdef Logs log_config_obj(self, Element element, str config_name, object config_old, object config_new)
    cpdef Logs log_sync_failed_not_exist(self, Element element)
    cpdef Logs log_sync_failed_mismatch(self, Element element, str config_name, object config_local, object config_remote)
    # Manipulate
    cpdef Logs extend(self, Logs logs)
    cpdef Logs _skip(self)
