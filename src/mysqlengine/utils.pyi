import datetime
from sqlcycli import Charset
from sqlcycli.sqlfunc import RawText

# Constant ---------------------------------------------------------------------------------
BASE_DATE: datetime.date
MAXVALUE: RawText
MINVALUE: RawText
SCHEMA_ELEMENT_MAX_NAME_LENGTH: int
# options
STORAGE_ENGINES: dict[str, str]
COMPRESSION_METHODS: dict[str, str]
JOIN_METHODS: dict[str, str]
INDEX_HINTS_SCOPES: dict[str, str]
INSERT_PRIORITIES: dict[str, str]
TIMETABLE_UNITS: dict[str]
CHECK_TABLE_OPTIONS: set[str]
REPAIR_TABLE_OPTIONS: set[str]
ROW_FORMATS: set[str]
INDEX_TYPES: set[str]
FOREIGN_KEY_ACTIONS: set[str]
LOCKING_READS_OPTIONS: set[str]

# Schema Element Settings ------------------------------------------------------------------
class SchemaElementSettings:
    SCHEMA_ELEMENT_PROHIBITED_NAMES: set[str]
    SCHEMA_ELEMENT_MAX_NAME_LENGTH: int
    def add_prohibited_names(self, names: list[str]) -> bool: ...

SCHEMA_ELEMENT_SETTINGS: SchemaElementSettings

# Validator --------------------------------------------------------------------------------
def validate_database_name(name: str) -> str:
    """(cfunc) Validate MySQL database name `<'str'>`.

    #### Database name must be a lowercased string with length between 1 and 64.
    """

def validate_table_name(name: str) -> str:
    """(cfunc) Validate MySQL table name `<'str'>`.

    #### Table name must be a lowercased string with length between 1 and 64.
    """

def validate_column_name(name: str) -> str:
    """(cfunc) Validate MySQL column name `<'str'>`.

    #### Column name must be a string with length between 1 and 64.
    """

def validete_index_name(name: str) -> str:
    """(cfunc) Validate MySQL index name `<'str'>`.

    #### Index name must be a string with length between 1 and 64.
    """

def validate_constraint_name(name: str) -> str:
    """(cfunc) Validate MySQL constraint name `<'str'>`.

    #### Constraint name must be a string with length between 1 and 64.
    """

def validate_partition_name(name: str) -> str:
    """(cfunc) Validate MySQL partition name `<'str'>`.

    #### Partition name must be a lowercased string with length between 1 and 64.
    """

def validate_charset(
    charset: str | Charset | None = None,
    collate: str | None = None,
) -> Charset | None:
    """(cfunc) Validate the CHARACTER SET and COLLATION `<'Charset/None'>`

    :param charset `<'str/Charset/None'>`: The character set. Defaults to `None`.
    :param collate `<'str/None'>`: The collation. Defaults to `None`.

    #### If both 'charset' and 'collate' are None, returns None.
    """

def validate_comment(comment: object) -> str | None:
    """(cfunc) Validate MySQL comment `<'str/None'>`"""

def validate_expression(expr: object) -> str | None:
    """(cfunc) Validate MySQL comment `<'str/None'>`"""

def validate_str_contains(string: str, substr: str) -> bool:
    """(cfunc) Validate if the 'str' value contains the 'substr' `<'bool'>`."""

# . option [bool]
def validate_encryption(encryption: object) -> int:
    """(cfunc) Validate MySQL `ENCRYPTION` config `<'int'>.

    ### Note:
    - Returns -1 if 'encryption' is None.
    - Returns 1 if 'encryption' represents True, else 0.
    """

def validate_read_only(read_only: object) -> int:
    """(cfunc) Validate MySQL `READ ONLY` config `<'int'>.

    ### Note:
    - Returns -1 if 'read_only' is None.
    - Returns 1 if 'read_only' represents True, else 0.
    """

def validate_null(null: object) -> int:
    """(cfunc) Validate MySQL `NULL` config `<'int'>.

    ### Note:
    - Returns -1 if 'null' is None.
    - Returns 1 if 'null' represents True, else 0.
    """

def validate_visible(visible: object) -> int:
    """(cfunc) Validate MySQL element `VISIBLE` config `<'int'>.

    ### Note:
    - Returns -1 if 'visible' is None.
    - Returns 1 if 'visible' represents True, else 0.
    """

def validate_enforced(enforced: object) -> int:
    """(cfunc) Validate MySQL element `ENFORCED` config `<'int'>.

    ### Note:
    - Returns -1 if 'enforced' is None.
    - Returns 1 if 'enforced' represents True, else 0.
    """

# . option [dict]
def validate_engine(engine: str | None) -> str | None:
    """(cfunc) Validate MySQL storage engine `<'str/None'>`."""

def validate_compression(compression: str | None) -> str | None:
    """(cfunc) Validate MySQL compression method `<'str/None'>`"""

def validate_join_method(join_method: str | None) -> str | None:
    """(cfunc) Validate MySQL join method `<'str/None'>`"""

def validate_index_hints_scope(scope: str | None) -> str | None:
    """(cfunc) Validate MySQL index hints scope `<'str/None'>`"""

def validate_insert_priority(priority: str | None) -> str | None:
    """(cfunc) Validate MySQL insert priority `<'str/None'>`"""

# . option [set]
def validate_row_format(row_format: str | None) -> str | None:
    """(cfunc) Validate MySQL row format `<'str/None'>`"""

def validate_index_type(index_type: str | None) -> str | None:
    """(cfunc) Validate MySQL index type `<'str/None'>`"""

def validate_foreign_key_action(action: str | None) -> str | None:
    """(cfunc) Validate MySQL foreign key action `<'str/None'>`"""

def validate_check_table_option(option: str | None) -> str | None:
    """(cfunc) Validate MySQL check table option `<'str/None'>`"""

def validate_repair_table_option(option: str | None) -> str | None:
    """(cfunc) Validate MySQL repair table option `<'str/None'>`"""

def validate_locking_reads_option(option: str | None) -> str | None:
    """(cfunc) Validate MySQL locking reads option `<'str/None'>`"""

# Reader -----------------------------------------------------------------------------------
def read_bool_config(value: int) -> bool | None:
    """(cfunc) Read MySQL boolean value `<'bool/None'>`

    - Returns `None` if value == -1.
    - Returns `False` if value == 0.
    - Returns `True` if value == 1.
    """

# Cleaner ----------------------------------------------------------------------------------
def cleanup_expression(expr: str) -> str | None:
    """(cfunc) Clean MySQL expression `<'str/None'>`

    - If the expression starts with "(" and ends with ")",
      the parentheses will first be removed.
    - Remove all leading and trailing whitespaces.
    """

# Partition --------------------------------------------------------------------------------
class PARTITIONING_METHOD:
    NONSET: int
    # Method
    HASH: int
    LINEAR_HASH: int
    KEY: int
    LINEAR_KEY: int
    RANGE: int
    RANGE_COLUMNS: int
    LIST: int
    LIST_COLUMNS: int

def partitioning_flag_to_method(flag: int) -> str:
    """(cfunc) Convert partitioning integer flag to its corresponding string method `<'str'>`."""

def partitioning_method_to_flag(method: str) -> int:
    """(cfunc) Convert partitioning method to its corresponding integer flag `<'int'>`."""

def is_partition_by_hash(flag: int) -> bool:
    """(cfunc) Check if the partitioning flag represents PARTITIION BY (LINEAR) HASH `<'bool'>`."""

def is_partition_by_key(flag: int) -> bool:
    """(cfunc) Check if the partitioning flag represents PARTITIION BY (LINEAR) KEY `<'bool'>`."""

def is_partition_by_range(flag: int) -> bool:
    """(cfunc) Check if the partitioning flag represents PARTITIION BY RANGE (COLUMNS) `<'bool'>`."""

def is_partition_by_list(flag: int) -> bool:
    """(cfunc) Check if the partitioning flag represents PARTITIION BY LIST (COLUMNS) `<'bool'>`."""

def is_partition_by_hashkey(flag: int) -> bool:
    """(cfunc) Check if the partitioning flag represents PARTITIION BY (LINEAR) HASH/KEY `<'bool'>`."""

def is_partition_by_rangelist(flag: int) -> bool:
    """(cfunc) Check if the partitioning flag represents PARTITION BY RANGE/LIST (COLUMNS) `<'bool'>`."""

def is_partitioning_flag_valid(flag: int) -> bool:
    """(cfunc) Check if the partitioning flag is valid (1-8) `<'bool'>`."""

# DML Clause -------------------------------------------------------------------------------
class DML_CLAUSE:
    NONE: int
    # WITH
    WITH: int
    # Delete
    DELETE: int
    # Update
    UPDATE: int
    # Insert
    INSERT: int
    INSERT_COLUMNS: int
    INSERT_VALUES: int
    SET: int
    ON_DUPLICATE: int
    # Select
    SELECT: int
    FROM: int
    JOIN: int
    WHERE: int
    GROUP_BY: int
    HAVING: int
    WINDOW: int
    SET_OPERATION: int
    ORDER_BY: int
    LIMIT: int
    LOCKING_READS: int
    INTO: int

class INSERT_MODE:
    INCOMPLETE: int
    # Mode
    VALUES_MODE: int
    SET_MODE: int
    SELECT_MODE: int

# TimeTable --------------------------------------------------------------------------------
class TIMETABLE_UNIT:
    UNKNOWN: int
    # Unit
    YEAR: int
    QUARTER: int
    MONTH: int
    WEEK: int
    DAY: int
    HOUR: int
    MINUTE: int
    SECOND: int

def validate_timetable_unit(unit: object) -> str | None:
    """(cfunc) Validate MySQL storage engine `<'str/None'>`."""

def timetable_unit_flag_to_str(flag: int) -> str:
    """(cfunc) Convert timetable unit integer flag to its corresponding string representation `<'str'>`."""

def timetable_unit_str_to_flag(unit: str) -> int:
    """(cfunc) Convert timetable unit string representation to its corresponding integer flag `<'int'>`."""
