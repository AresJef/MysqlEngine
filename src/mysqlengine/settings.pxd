# cython: language_level=3

# Prohibit names for Database/Table/Column/Index
cdef set PROHIBIT_NAMES
# Maximum length of names for Database/Column/Index
cdef int MAX_NAME_LENGTH
# Maximum length of names for Table
cdef int MAX_NAME_LENGTH_TABLE
# Maximum number of columns per index
cdef int MAX_INDEX_COLUMNS
# Maximum number of indexes per table
cdef int MAX_INDEXES
# Maximum number of columns per table
cdef int MAX_COLUMNS
# Supported MySQL engines for Database/Table
cdef set SUPPORT_ENGINES
# Supported MySQL engines for Temporary Table
cdef set TEMPORARY_ENGINES
# Supported TimeTable units
cdef dict SUPPORT_TIME_UNITS
