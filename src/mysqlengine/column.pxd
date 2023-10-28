# cython: language_level=3

from cpython.set cimport PySet_Add as set_add
from mysqlengine.dtype cimport DataType

# Utils
cdef inline str access_column_name(Column col) noexcept:
    return col._name

cdef inline object get_column_name(object col) noexcept:
    if isinstance(col, Column):
        return access_column_name(col)
    else:
        return col

cdef inline set get_columns_names(tuple columns) noexcept:
    names: set = set()
    for col in columns:
        set_add(names, get_column_name(col))
    return names

cdef inline str access_column_syntax(Column col) noexcept:
    return col._syntax

cdef inline str access_column_mysql(Column col) noexcept:
    return col._mysql

cdef inline DataType access_column_dtype(Column col) noexcept:
    return col._dtype

# Column
cdef class Column:
    # Attributes
    cdef:
        DataType _dtype
        str _name, _name_lower, _mysql, _syntax
        object _python, _default
        bint _primary_key, _auto_increment, _null, _tabletime

# Columns
cdef class Columns:
    # Attributes
    cdef:
        dict _dict
        tuple _names, _instances, _items
        set _names_set
        int _length
    # Search
    cdef list _search_by_name(self, str name, bint exact) noexcept
    cdef list _search_by_mysql_dtype(self, str dtype, bint exact) noexcept
    cdef list _search_by_python_dtype(self, tuple dtypes) noexcept
    # Filter
    cdef bint _issubset(self, tuple columns) noexcept
    cdef list _filter(self, tuple columns) noexcept
    # Accessors
    cdef object _get(self, object col, object default) noexcept
    # Special Methods
    cdef Column _getitem(self, object col) except *
    cdef bint _contains(self, object col) noexcept

cdef class TableColumns(Columns):
    # Attributes
    cdef:
        Column _primary_key, _tabletime
        Columns _auto_increments, _non_auto_increments
        dict _item_validators, _series_validators
        str _syntax
    # Syntax
    cdef str _gen_syntax(self, tuple columns) noexcept
