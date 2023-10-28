# cython: language_level=3

# Cython imports
import cython

# Prohibit names for Database/Table/Column/Index
PROHIBIT_NAMES: set[str] = {
    "table_time",
    "table",
    "norm_table",
    "time_table",
    "information_schema",
    "mysql",
    "performance_schema",
    "sys",
    "__recycle_bin__",
}

# Maximum length of names for Database/Column/Index
MAX_NAME_LENGTH: cython.int = 64

# Maximum length of names for Table
MAX_NAME_LENGTH_TABLE: cython.int = 54

# Maximum number of columns per index
MAX_INDEX_COLUMNS: cython.int = 16

# Maximum number of indexes per table
MAX_INDEXES: cython.int = 64

# Maximum number of columns per table
MAX_COLUMNS: cython.int = 1024

# Supported MySQL engines for Database/Table
SUPPORT_ENGINES: set[str] = {"InnoDB", "MyISAM"}

# Supported MySQL engines for Temporary Table
TEMPORARY_ENGINES: set[str] = {"MEMORY", "InnoDB", "MyISAM"}

# Supported TimeTable units
SUPPORT_TIME_UNITS: dict[str, str] = {
    "day": "day",
    "d": "day",
    "week": "week",
    "w": "week",
    "month": "month",
    "m": "month",
    "year": "year",
    "y": "year",
}
