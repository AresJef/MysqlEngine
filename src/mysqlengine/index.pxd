# cython: language_level=3

from cpython.set cimport PySet_Add as set_add
from mysqlengine.column cimport Columns

# Utils
cdef inline str access_index_name(Index idx) noexcept:
    return idx._name

cdef inline object get_index_name(object idx) noexcept:
    if isinstance(idx, Index):
        return access_index_name(idx)
    else:
        return idx

cdef inline set get_indexes_names(tuple indexes) noexcept:
    names: set = set()
    for idx in indexes:
        set_add(names, get_index_name(idx))
    return names

cdef inline str access_index_syntax(Index idx) noexcept:
    return idx._syntax

# Index
cdef class Index:
    # Attributes
    cdef:
        str _name, _name_lower, _syntax
        bint _unique, _primary_unique
        Columns _columns
    # Search
    cdef list _search_by_name(self, str name, bint exact) noexcept
    cdef list _search_by_mysql_dtype(self, str dtype, bint exact) noexcept
    cdef list _search_by_python_dtype(self, tuple dtypes) noexcept
    # Filter
    cdef bint _issubset(self, tuple columns) noexcept

# Indexes
cdef class Indexes:
    # Attributes
    cdef:
        dict _dict
        tuple _names, _instances, _items
        set _names_set
        int _length
    # Search
    cdef list _search_by_name(self, str name, bint exact) noexcept
    cdef list _search_by_columns(self, tuple columns, bint match_all) noexcept
    # Filter
    cdef bint _issubset(self, tuple indexes) noexcept
    cdef list _filter(self, tuple indexes) noexcept
    # Accessors
    cdef object _get(self, object idx, object default) noexcept
    # Special Methods
    cdef Index _getitem(self, object idx) except *
    cdef bint _contains(self, object idx) noexcept

cdef class UniqueIndexes(Indexes):
    # Attributes
    cdef:
        Index _primary

cdef class TableIndexes(Indexes):
    # Attribtues
    cdef:
        UniqueIndexes _uniques
        str _syntax
    # Syntax
    cdef str _gen_syntax(self, tuple columns) noexcept
