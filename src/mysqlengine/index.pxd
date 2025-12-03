# cython: language_level=3
from mysqlengine.element cimport Element, Elements, Logs, Metadata, Query

# Index
cdef class Index(Element):
    cdef:
        # Common
        tuple _columns
        str _index_type
        str _comment
        bint _visible
        # FullText Index
        str _parser
    # Sync
    cpdef Logs Initialize(self, bint force=?)
    cpdef Logs Add(self)
    cpdef bint Exists(self) except -1
    cpdef Logs Drop(self)
    cpdef Logs _Alter(self, object columns, str index_type, str parser, str comment, bool visible)
    cpdef Logs SetVisible(self, bint visible)
    cpdef IndexMetadata ShowMetadata(self)
    cpdef tuple ShowIndexNames(self)
    cpdef Logs SyncFromRemote(self)
    cpdef Logs SyncToRemote(self)
    # Generate SQL
    cpdef str _gen_definition_sql(self)
    cpdef str _gen_add_sql(self)
    cpdef str _gen_exists_sql(self)
    cpdef str _gen_drop_sql(self)
    cpdef Query _gen_alter_query(self, IndexMetadata meta, object columns, str index_type, str parser, str comment, bool visible)
    cpdef str _gen_set_visible_sql(self, bint visible)
    cpdef str _gen_show_metadata_sql(self)
    cpdef str _gen_show_index_names_sql(self)
    # Metadata
    cpdef Logs _sync_from_metadata(self, IndexMetadata meta, Logs logs=?)
    cpdef int _diff_from_metadata(self, IndexMetadata meta) except -1
    # Setter
    cpdef bint setup(self, str tb_name, str db_name, object charset, str collate, object pool) except -1
    # Copy
    cpdef Index copy(self)
    cpdef Index _construct(self, object columns, str index_type, str parser, str comment, bool visible)

cdef class FullTextIndex(Index):
    cdef:
        object _parser_regex
    # Validator
    cdef inline str _validate_parser(self, object parser)

# Indexex
cdef class Indexes(Elements):
    # Setter
    cpdef bint setup(self, str tb_name, str db_name, object charset, str collate, object pool) except -1
    # Copy
    cpdef Indexes copy(self)

# Metadata
cdef class IndexMetadata(Metadata):
    cdef:
        # Base data
        str _db_name
        str _tb_name
        str _index_name
        str _index_type
        bint _unique
        str _comment
        bint _visible
        # Additional data
        str _el_type
        tuple _columns
        # FullText Index
        str _parser
