# cython: language_level=3
from sqlcycli.charset cimport Charset
from mysqlengine.element cimport Element, Elements, Logs, Metadata, Query

# Definition
cdef class Definition(Element):
    cdef:
        # . definition
        str _data_type
        object _python_type
        bint _null
        object _default
        bint _primary_key
        bint _unique_key
        bint _indexed
        str _comment
        bint _visible
        # . integer column
        bint _unsigned
        bint _auto_increment
        # . floating/fixed point column
        int _default_precision
        int _precision
        int _default_scale
        int _scale
        # . temporal column
        int _default_fsp
        int _fsp
        bint _auto_init
        bint _auto_update
        # . string column
        long long _default_length
        long long _length
        # . enum column
        tuple _elements
    # Generate SQL
    cpdef str _gen_definition_sql(self)
    cpdef str _gen_data_type_sql(self)
    # Metadata
    cpdef Logs _sync_from_metadata(self, ColumnMetadata meta, Logs logs=?)
    cpdef int _diff_from_metadata(self, ColumnMetadata meta) except -1
    cpdef long long _read_metadata_precision(self, object value) except -2
    # Setter
    cpdef bint setup(self, Column col) except -1
    # Validate
    cpdef object _validate_default(self, object default)
    # Internal
    cdef inline int _get_fsp(self) except -2
    cdef inline long long _get_length(self) except -2
    # Copy
    cpdef Definition copy(self)
    # Special method
    cdef inline str _gen_repr(self, list reprs)

# Column
cdef class Column(Element):
    cdef:
        # Common
        Definition _definition
        bint _primary_key
        bint _unique_key
        bint _indexed
        # Generated column
        str _expression
        int _virtual
    # Sync
    cpdef Logs Initialize(self, bint force=?)
    cpdef Logs Add(self, int position=?)
    cpdef bint Exists(self) except -1
    cpdef Logs Drop(self)
    cpdef Logs _Modify(self, object definition, object expression, int position)
    cpdef Logs SetVisible(self, bint visible)
    cpdef Logs SetDefault(self, object default)
    cpdef ColumnMetadata ShowMetadata(self)
    cpdef tuple ShowColumnNames(self)
    cpdef Logs SyncFromRemote(self)
    cpdef Logs SyncToRemote(self)
    # Generate SQL
    cpdef str _gen_definition_sql(self)
    cpdef str _gen_add_sql(self, int position, tuple columns)
    cpdef str _gen_exists_sql(self)
    cpdef str _gen_drop_sql(self)
    cpdef ColumnQuery _gen_modify_query(self, ColumnMetadata meta, Definition definition, object expression, int position, tuple columns)
    cpdef str _gen_set_visible_sql(self, bint visible)
    cpdef str _gen_set_default_sql(self, object default)
    cpdef str _gen_show_metadata_sql(self)
    cpdef str _gen_show_column_names_sql(self)
    # Metadata
    cpdef Logs _sync_from_metadata(self, ColumnMetadata meta, Logs logs=?)
    cpdef int _diff_from_metadata(self, ColumnMetadata meta) except -1
    # Setter
    cpdef bint setup(self, str tb_name, str db_name, object charset, object collate, object pool) except -1
    cpdef bint _set_definition(self, Definition definition) except -1
    # Copy
    cpdef Column copy(self)
    cpdef Column _construct(self, Definition definition, object expression, object virtual)

cdef class GeneratedColumn(Column):
    pass

# Columns
cdef class Columns(Elements):
    # Setter
    cpdef bint setup(self, str tb_name, str db_name, object charset, object collate, object pool) except -1
    # Copy
    cpdef Columns copy(self)

# Metadata
cdef class ColumnMetadata(Metadata):
    cdef:
        # Base data
        str _db_name
        str _tb_name
        str _column_name
        str _column_type
        int _position
        str _default
        bint _null
        str _data_type
        object _character_maximum_length
        object _numeric_precision
        object _numeric_scale
        object _datetime_precision
        Charset _charset
        str _extra
        str _comment
        str _expression
        int _virtual
        # Additional data
        str _el_type
        bint _visible
        # . integer
        bint _auto_increment
        bint _unsigned
        # . datetime
        bint _auto_init
        bint _auto_update
        # . index
        bint _primary_key
        bint _unique_key
        bint _indexed
        str _column_index_name
        int _column_index_seq
        int _column_index_length

# Query
cdef class ColumnQuery(Query):
    cdef:
        Definition _definition
