# cython: language_level=3

from cpython cimport datetime
from cpython.set cimport PySet_Add as set_add
from mysqlengine.connection cimport Server
from mysqlengine.charset cimport Charset
from mysqlengine.index cimport TableIndexes
from mysqlengine.column cimport TableColumns
from mysqlengine.regex cimport Regex, TableRegex

# Utils
cdef inline str access_table_name(Table tb) noexcept:
    return tb._name

cdef inline object get_table_name(object tb) noexcept:
    if isinstance(tb, Table):
        return access_table_name(tb)
    else:
        return tb

cdef inline set get_tables_names(tuple tables) noexcept:
    names: set = set()
    for tb in tables:
        set_add(names, get_table_name(tb))
    return names

# Table
cdef class Table:
    # Attributes
    cdef:
        # . settings
        Database _db
        Server _server
        str _db_pfix
        str _name, _name_pfix
        str _fname, _fname_pfix
        str _charset, _collate, _engine
        int _temp_id
        # . type
        str _type
        bint _is_timetable
        # . regex
        TableRegex _regex
        # . columns & indexes
        TableColumns _columns
        TableIndexes _indexes
        # . syntax
        str _syntax_val, _syntax
        # . status
        bint _initiated
        set _initiated_tables
    # Syntax
    cdef str _gen_syntax(self, tuple columns, object engine) noexcept
    # Naming
    cdef str _gen_tname(self) noexcept
    cdef str _gen_pname(self) noexcept
    # Initiate
    cdef _set_init_tables(self, list names) noexcept
    cdef _add_init_table(self, str name) noexcept
    cdef _add_init_tables(self, list names) noexcept
    cdef _rem_init_table(self, str name) noexcept
    cdef _rem_init_tables(self, list names) noexcept
    # Validate
    cdef _validate_cursor(self, object cursor) except *
    cdef Charset _validate_charset_collate(self, str charset, str collate) except *
    # Escape data
    cdef str _escape_item(self, object item) except *
    cdef object _escape_args(self, object args) except *
    # Filter data
    cdef object _filter_columns(self, object data, tuple columns) except *
    cdef dict _filter_columns_dict(self, dict data, tuple columns) except *
    cdef tuple _filter_columns_tuple(self, tuple data, tuple columns) except *
    cdef list _filter_columns_list(self, list data, tuple columns) except *
    cdef object _filter_columns_df(self, object data, tuple columns) except *
    # Clean data
    cdef list _clean_data(self, object data, tuple columns) except *
    cdef dict _clean_data_dict(self, dict data, tuple columns) except *
    cdef list _clean_data_tuple(self, tuple data, tuple columns) except *
    cdef list _clean_data_list(self, list data, tuple columns) except *
    cdef list _clean_data_df(self, object data, tuple columns) except *

# TimeTable
cdef class TimeTable(Table):
    # Attributes
    cdef:
        str _time_unit
        str _time_format
        str _name_format
    # Parse
    cdef datetime.datetime _parse_time(self, object time) except *
    cdef datetime.datetime _parse_time_from_subname(self, str subname) except *
    # Naming
    cdef str _gen_name_from_time(self, datetime.datetime time) except *
    cdef str _gen_fname_from_time(self, datetime.datetime time) except *
    cdef str _get_name(self, object time) except *
    cdef tuple _get_name_with_dt(self, object time) except *
    cdef str _get_fname(self, object time) except *
    cdef tuple _get_fname_with_dt(self, object time) except *
    cdef list _get_names(self, list times, object start, object end, int days, bint filter, set exist_tables) except *
    cdef dict _get_names_with_dt(self, list times, object start, object end, int days, bint filter, set exist_tables, bint invert) except *
    cdef list _get_fnames(self, list times, object start, object end, int days, bint filter, set exist_tables) except *
    cdef dict _get_fnames_with_dt(self, list times, object start, object end, int days, bint filter, set exist_tables, bint invert) except *
    cdef dict _all_names_with_dt(self, list names, bint invert) except *
    cdef dict _all_fnames_with_dt(self, list names, bint invert) except *
    cdef object _get_names_series(self, object times) except *
    cdef object _get_fnames_series(self, object times) except *

# Tables
cdef class Tables:
    # Attributes
    cdef:
        Database _db
        dict _dict
        tuple _names, _instances, _items
        set _names_set
        int _length
        Regex _regex_names, _regex_fnames
    # Search
    cdef list _search_by_name(self, str name, bint exact) noexcept
    cdef list _search_by_charset(self, str charset, bint exact) noexcept
    cdef list _search_by_collate(self, str collate, bint exact) noexcept
    cdef list _search_by_columns(self, tuple columns, bint match_all) noexcept
    cdef list _search_by_indexes(self, tuple indexes, bint match_all) noexcept
    # Filter
    cdef bint _issubset(self, tuple tables) noexcept
    cdef list _filter(self, tuple tables) noexcept
    # Accessors
    cdef object _get(self, object tb, object default) noexcept
    # Special Methods
    cdef Table _getitem(self, object tb) except *
    cdef bint _contains(self, object tb) noexcept

# Database Tables
cdef class DatabaseTables(Tables):
    # Attributes
    cdef:
        Tables _standards, _timetables
    # Exception
    cdef tuple _match_table_from_absent_exc(self, object exc) except *

# Database
cdef class Database:
    # Attributes
    cdef:
        Server _server
        str _name, _name_pfix
        str _charset, _collate
        DatabaseTables _tables
        str _syntax
        bint _initiated
        set __namespace
    # Validate
    cdef _validate_cursor(self, object cursor) except *
    cdef Charset _validate_charset_collate(self, str charset, str collate) except *
    # Escape data
    cdef str _escape_item(self, object item) except *
    cdef object _escape_args(self, object args) except *
