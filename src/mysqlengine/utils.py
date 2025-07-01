# cython: language_level=3
# cython: wraparound=False
# cython: boundscheck=False

# Cython imports
import cython
from cython.cimports.cpython import datetime  # type: ignore
from cython.cimports.cpython.set import PySet_Add as set_add  # type: ignore
from cython.cimports.cpython.unicode import PyUnicode_Check as str_len  # type: ignore
from cython.cimports.sqlcycli.sqlfunc import RawText  # type: ignore

datetime.import_datetime()

import datetime
from sqlcycli.sqlfunc import RawText

# Constant ---------------------------------------------------------------------------------
#: Base Date (1970-01-01)
BASE_DATE: datetime.date = datetime.date_new(1970, 1, 1)
#: The MAXVALUE for MySQL partitioning
MAXVALUE: RawText = RawText("MAXVALUE")
#: The MINVALUE (-MAXVALUE) for MySQL partitioning
MINVALUE: RawText = RawText("-MAXVALUE")
#: The maximum name length for a MySQL element (Database, Table, Column, Index, etc.)
SCHEMA_ELEMENT_MAX_NAME_LENGTH: cython.int = 64
# Options - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#: Acceptable storage engines
STORAGE_ENGINES: dict[str, str] = {
    "INNODB": "InnoDB",
    "MYISAM": "MyISAM",
    "MEMORY": "MEMORY",
    "CSV": "CSV",
    "ARCHIVE": "ARCHIVE",
    "EXAMPLE": "EXAMPLE",
    "FEDERATED": "FEDERATED",
    "HEAP": "HEAP",
    "MERGE": "MERGE",
    "NDB": "NDB",
}
#: Acceptable compression methods
COMPRESSION_METHODS: dict[str, str] = {"ZLIB": "Zlib", "LZ4": "LZ4"}
#: Acceptable join methods
JOIN_METHODS: dict[str, str] = {
    "JOIN": "INNER JOIN",
    "INNER": "INNER JOIN",
    "INNER JOIN": "INNER JOIN",
    "LEFT": "LEFT JOIN",
    "LEFT JOIN": "LEFT JOIN",
    "LEFT OUTER JOIN": "LEFT JOIN",
    "RIGHT": "RIGHT JOIN",
    "RIGHT JOIN": "RIGHT JOIN",
    "RIGHT OUTER JOIN": "RIGHT JOIN",
}
#: Acceptable index hints scopes
INDEX_HINTS_SCOPES: dict[str, str] = {
    "JOIN": "JOIN",
    "FOR JOIN": "JOIN",
    "ORDER BY": "ORDER BY",
    "FOR ORDER BY": "ORDER BY",
    "GROUP BY": "GROUP BY",
    "FOR GROUP BY": "GROUP BY",
}
#: Acceptable insert priorities
INSERT_PRIORITIES: dict[str, str] = {
    "LOW": "LOW_PRIORITY",
    "LOW_PRIORITY": "LOW_PRIORITY",
    "HIGH": "HIGH_PRIORITY",
    "HIGH_PRIORITY": "HIGH_PRIORITY",
}
#: Acceptable time table units
TIMETABLE_UNITS: dict[str] = {
    "YEAR": "YEAR",
    "Y": "YEAR",
    "QUARTER": "QUARTER",
    "Q": "QUARTER",
    "MONTH": "MONTH",
    "M": "MONTH",
    "WEEK": "WEEK",
    "W": "WEEK",
    "DAY": "DAY",
    "D": "DAY",
    "HOUR": "HOUR",
    "H": "HOUR",
    "MINUTE": "MINUTE",
    "MIN": "MINUTE",
    "I": "MINUTE",
    "SECOND": "SECOND",
    "S": "SECOND",
}
#: Acceptable check table options
CHECK_TABLE_OPTIONS: set[str] = {
    "FOR UPGRADE",
    "QUICK",
    "FAST",
    "MEDIUM",
    "EXTENDED",
    "CHANGED",
}
#: Acceptable repair table options
REPAIR_TABLE_OPTIONS: set[str] = {"QUICK", "EXTENDED", "USE_FRM"}
#: Acceptable row formats
ROW_FORMATS: set[str] = {
    "COMPACT",
    "COMPRESSED",
    "DYNAMIC",
    "FIXED",
    "REDUNDANT",
    "PAGED",
}
#: Acceptable index types
INDEX_TYPES: set[str] = {"BTREE", "HASH", "FULLTEXT"}
#: Acceptable foreign key actions
FOREIGN_KEY_ACTIONS: set[str] = {
    "CASCADE",
    "SET NULL",
    "RESTRICT",
    "NO ACTION",
    "SET DEFAULT",
}
#: Acceptable locking reads options
LOCKING_READS_OPTIONS: set[str] = {"NOWAIT", "SKIP LOCKED"}


# Schema Element Settings ------------------------------------------------------------------
@cython.cclass
class SchemaElementSettings:
    #: Prohibit names for a MySQL element (Database, Table, Column, Index, etc.)
    SCHEMA_ELEMENT_PROHIBITED_NAMES: set[str]
    #: The maximum name length for a MySQL element (Database, Table, Column, Index, etc.)
    SCHEMA_ELEMENT_MAX_NAME_LENGTH: cython.int

    def __init__(self):
        self.SCHEMA_ELEMENT_PROHIBITED_NAMES = set()
        self.SCHEMA_ELEMENT_MAX_NAME_LENGTH = SCHEMA_ELEMENT_MAX_NAME_LENGTH

    @cython.ccall
    @cython.exceptval(-1, check=False)
    def add_prohibited_names(self, names: list[str]) -> cython.bint:
        """Add prohibited names for schema element (ignores duplicates)."""
        for name in names:
            if not isinstance(name, str):
                raise AssertionError(
                    "prohibited names must be <'str'> type, "
                    "instead got %s %r." % (type(name), name)
                )
            _name: str = name
            _name = _name.strip()
            if str_len(name) == 0:
                raise AssertionError("prohibited name cannot be an empty string.")
            set_add(self.SCHEMA_ELEMENT_PROHIBITED_NAMES, _name.lower())
        return True


SCHEMA_ELEMENT_SETTINGS: SchemaElementSettings = SchemaElementSettings()
# MySQL prohibited names
SCHEMA_ELEMENT_SETTINGS.add_prohibited_names(
    [
        # mysql build-in
        "information_schema",
        "mysql",
        "performance_schema",
        "sys",
        # keywords
        "all",
        "maxvalue",
        "-maxvalue",
        "minvalue",
    ]
)
