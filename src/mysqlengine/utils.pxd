# cython: language_level=3
from cpython cimport datetime
from cpython.unicode cimport (
    PyUnicode_Check as is_str,
    PyUnicode_GET_LENGTH as str_len,
    PyUnicode_Substring as str_substr,
    PyUnicode_Contains as str_contains,
    PyUnicode_Tailmatch as str_tailmatch,
)
from cpython.set cimport PySet_Contains as set_contains
from cpython.dict cimport PyDict_GetItem as dict_getitem
from sqlcycli.charset cimport Charset, _charsets
from sqlcycli.sqlfunc cimport SQLFunction, RawText

# Constant ---------------------------------------------------------------------------------
cdef: 
    #: Base Date (1970-01-01)
    datetime.date BASE_DATE
    #: The MAXVALUE for MySQL partitioning
    RawText MAXVALUE
    #: The MINVALUE (-MAXVALUE) for MySQL partitioning
    RawText MINVALUE
    #: The maximum name length for a MySQL element (Database, Table, Column, Index, etc.)
    int SCHEMA_ELEMENT_MAX_NAME_LENGTH
    # . Options
    #: Acceptable storage engines
    dict STORAGE_ENGINES
    #: Acceptable compression methods
    dict COMPRESSION_METHODS
    #: Acceptable join methods
    dict JOIN_METHODS
    #: Acceptable index hints scopes
    dict INDEX_HINTS_SCOPES
    #: Acceptable insert priorities
    dict INSERT_PRIORITIES
    #: Acceptable time table units
    dict TIMETABLE_UNITS
    #: Acceptable check table options
    set CHECK_TABLE_OPTIONS
    #: Acceptable repair table options
    set REPAIR_TABLE_OPTIONS
    #: Acceptable row formats
    set ROW_FORMATS
    #: Acceptable index types
    set INDEX_TYPES
    #: Acceptable foreign key actions
    set FOREIGN_KEY_ACTIONS
    #: Acceptable locking reads options
    set LOCKING_READS_OPTIONS


# Schema Element Settings ------------------------------------------------------------------
cdef class SchemaElementSettings:
    cdef:
        #: Prohibit names for a MySQL element (Database, Table, Column, Index, etc.)
        set SCHEMA_ELEMENT_PROHIBITED_NAMES
        #: The maximum name length for a MySQL element (Database, Table, Column, Index, etc.)
        int SCHEMA_ELEMENT_MAX_NAME_LENGTH
    cpdef bint add_prohibited_names(self, list names) except -1

cdef SchemaElementSettings SCHEMA_ELEMENT_SETTINGS

# Validator --------------------------------------------------------------------------------
cdef inline str _validate_element_name(str element, object name, bint lowercase):
    """Validate MySQL element name (e.g., 'Database', 'Table', 'Column', 'Index') `<'str'>`

    :param element `<str'>`: The string representation of the MySQL element.
    :param name `<'object'>`: The name of MySQL element.
    :lowercase `<'bool'>`: Whether to ensure the name is in lowercase.
    """
    if not is_str(name):
        raise TypeError(
            "%s name must be <'str'> type, instead got %s %r."
            % (element, type(name), name)
        )

    cdef str _name = name
    _name = _name.strip()
    name_lower = _name.lower()
    if set_contains(SCHEMA_ELEMENT_SETTINGS.SCHEMA_ELEMENT_PROHIBITED_NAMES, name_lower):
        raise ValueError(
            "%s name '%s' is prohibited, please choose another one."
            % (element, _name)
        )
        
    cdef Py_ssize_t length = str_len(_name)
    if length == 0:
        raise ValueError("%s name cannot be an empty string." % element)
    if length > SCHEMA_ELEMENT_MAX_NAME_LENGTH:
        raise ValueError(
            "%s name cannot exceed %d characters.\nInvalid name: '%s'." 
            % (element, SCHEMA_ELEMENT_MAX_NAME_LENGTH, _name)
        )
    return name_lower if lowercase else _name

cdef inline str validate_database_name(object name):
    """Validate MySQL database name `<'str'>`.
    
    #### Database name must be a lowercased string with length between 1 and 64.
    """
    return _validate_element_name("DATABASE", name, True)

cdef inline str validate_table_name(object name):
    """Validate MySQL table name `<'str'>`.
    
    #### Table name must be a lowercased string with length between 1 and 64.
    """
    return _validate_element_name("TABLE", name, True)

cdef inline str validate_column_name(object name):
    """Validate MySQL column name `<'str'>`.
    
    #### Column name must be a string with length between 1 and 64.
    """
    return _validate_element_name("COLUMN", name, False)

cdef inline str validete_index_name(object name):
    """Validate MySQL index name `<'str'>`.

    #### Index name must be a string with length between 1 and 64.
    """
    return _validate_element_name("INDEX", name, False)

cdef inline str validate_constraint_name(object name):
    """Validate MySQL constraint name `<'str'>`.

    #### Constraint name must be a string with length between 1 and 64.
    """
    return _validate_element_name("CONSTRAINT", name, False)

cdef inline str validate_partition_name(object name):
    """Validate MySQL partition name `<'str'>`.

    #### Partition name must be a string with length between 1 and 64.
    """
    return _validate_element_name("PARTITION", name, True)

cdef inline Charset validate_charset(object charset=None, str collate=None):
    """Validate the CHARACTER SET and COLLATE `<'Charset/None'>`
    
    :param charset `<'str/Charset/None'>`: The character set. Defaults to `None`.
    :param collate `<'str/None'>`: The collation. Defaults to `None`.
    
    #### If both 'charset' and 'collate' are None, returns None.
    """
    if isinstance(charset, Charset):
        return charset

    if collate is None:
        if charset is None:
            return None
        return _charsets.by_name(charset)

    if charset is None:
        return _charsets.by_collation(collate)

    return _charsets.by_name_n_collation(charset, collate)

cdef inline str validate_comment(object comment):
    """Validate MySQL comment `<'str/None'>`"""
    if comment is None:
        return None
    if not is_str(comment):
        raise TypeError(
            "COMMENT must be <'str/None'> type, instead got %s %r."
            % (type(comment), comment)
        )
    if str_len(comment) == 0:
        return None
    return comment

cdef inline str validate_expression(object expr):
    """Validate MySQL expression `<'str/None'>`"""
    if expr is None:
        return None
    if is_str(expr):
        return cleanup_expression(expr)
    if isinstance(expr, SQLFunction):
        return cleanup_expression(str(expr))
    raise TypeError(
        "EXPRESSION must be <'str/SQLFunction/None'> type, instead got %s %r."
        % (type(expr), expr)
    )

cdef inline bint validate_str_contains(str string, str substr) except -1:
    """Validate if the 'str' value contains the 'substr' `<'bool'>`."""
    return str_contains(string, substr)

# . option [bool]
cdef inline int _validate_bool_config(object config, str config_name) except -2:
    """Validate MySQL boolean option `<'int'>`
    
    ### Note:
    - Returns -1 if 'option' is None.
    - Returns 1 if 'option' represents True, else 0.
    """
    if config is None:
        return -1
    elif isinstance(config, str):
        _opt: str = config
        _opt = _opt.upper()
        if _opt in ("Y", "YES", "TRUE", "ON", "1"):
            return 1
        if _opt in ("N", "NO", "FALSE", "OFF", "0", ""):
            return 0
    else:
        try:
            return bool(config)
        except Exception:
            pass

    raise ValueError("invalid '%s' option value %r." % (config_name, config))

cdef inline int validate_encryption(object encryption) except -2:
    """Validate MySQL `ENCRYPTION` config `<'int'>.

    ### Note:
    - Returns -1 if 'encryption' is None.
    - Returns 1 if 'encryption' represents True, else 0.
    """
    return _validate_bool_config(encryption, "encryption")

cdef inline int validate_read_only(object read_only) except -2:
    """Validate MySQL `READ ONLY` config `<'int'>.

    ### Note:
    - Returns -1 if 'read_only' is None.
    - Returns 1 if 'read_only' represents True, else 0.
    """
    return _validate_bool_config(read_only, "read_only")

cdef inline int validate_null(object null) except -2:
    """Validate MySQL column `NULL` config `<'int'>.

    ### Note:
    - Returns -1 if 'null' is None.
    - Returns 1 if 'null' represents True, else 0.
    """
    return _validate_bool_config(null, "null")

cdef inline int validate_visible(object visible) except -2:
    """Validate MySQL element `VISIBLE` config `<'int'>.

    ### Note:
    - Returns -1 if 'visible' is None.
    - Returns 1 if 'visible' represents True, else 0.
    """
    return _validate_bool_config(visible, "visible")

cdef inline int validate_enforced(object enforced) except -2:
    """Validate MySQL element `ENFORCED` config `<'int'>.

    ### Note:
    - Returns -1 if 'enforced' is None.
    - Returns 1 if 'enforced' represents True, else 0.
    """
    return _validate_bool_config(enforced, "enforced")

# . option [dict]
cdef inline str _validate_dict_option(object option, dict available_options, str option_name):
    """Validate MySQL option [dict based available options] `<'str/None'>`"""
    if option is None:
        return None
    if not is_str(option):
        raise TypeError(
            "%s must be <'str/None'> type, instead got %s %r."
            % (option_name, type(option), option)
        )
    if str_len(option) == 0:
        return None
    cdef str key = option
    key = key.strip()
    key = key.upper()
    if key in ("DEFAULT", "NONE"):
        return None

    res = dict_getitem(available_options, key)
    if res is NULL:
        raise ValueError(
            "%s '%s' is invalid, must be one of: %s."
            % (option_name, option, list(available_options.keys()))
        )
    return <str> res

cdef inline str validate_engine(object engine):
    """Validate MySQL storage engine `<'str/None'>`."""
    return _validate_dict_option(
        engine, STORAGE_ENGINES, "STORAGE ENGINE"
    )

cdef inline str validate_compression(object compression):
    """Validate MySQL compression method `<'str/None'>`"""
    return _validate_dict_option(
        compression, COMPRESSION_METHODS, "COMPRESSION"
    )

cdef inline str validate_join_method(object join_method):
    """Validate MySQL join method `<'str/None'>`"""
    return _validate_dict_option(
        join_method, JOIN_METHODS, "JOIN method"
    )

cdef inline str validate_index_hints_scope(object scope):
    """Validate MySQL index hints scope `<'str/None'>`"""
    return _validate_dict_option(
        scope, INDEX_HINTS_SCOPES, "HINTS scope"
    )

cdef inline str validate_insert_priority(object priority):
    """Validate MySQL insert priority `<'str/None'>`"""
    return _validate_dict_option(
        priority, INSERT_PRIORITIES, "INSERT priority"
    )

# . option [set]
cdef inline str _validate_set_option(object option, set available_options, str option_name):
    """Validate MySQL option [set based available options] `<'str/None'>`"""
    if option is None:
        return None
    if not is_str(option):
        raise TypeError(
            "%s must be <'str/None'> type, instead got %s %r."
            % (option_name, type(option), option)
        )
    if str_len(option) == 0:
        return None
    cdef str opt = option
    opt = opt.strip()
    opt = opt.upper()
    if opt in ("DEFAULT", "NONE"):
        return None

    if not set_contains(available_options, opt):
        raise ValueError(
            "%s '%s' is invalid, must be one of: %s."
            % (option_name, option, sorted(available_options))
        )
    return opt

cdef inline str validate_row_format(object row_format):
    """Validate MySQL row format `<'str/None'>`"""
    return _validate_set_option(
        row_format, ROW_FORMATS, "ROW FORMAT"
    )

cdef inline str validate_index_type(object index_type):
    """Validate MySQL index type `<'str/None'>`"""
    return _validate_set_option(
        index_type, INDEX_TYPES, "INDEX TYPE"
    )

cdef inline str validate_foreign_key_action(object action):
    """Validate MySQL foreign key action `<'str/None'>`"""
    return _validate_set_option(
        action, FOREIGN_KEY_ACTIONS, "FOREIGN KEY ACTION"
    )

cdef inline str validate_check_table_option(object check_option):
    """Validate MySQL check table option `<'str/None'>`"""
    return _validate_set_option(
        check_option, CHECK_TABLE_OPTIONS, "TABLE CHECK OPTION"
    )

cdef inline str validate_repair_table_option(object repair_option):
    """Validate MySQL repair table option `<'str/None'>`"""
    return _validate_set_option(
        repair_option, REPAIR_TABLE_OPTIONS, "TABLE REPAIR OPTION"
    )

cdef inline str validate_locking_reads_option(object read_option):
    """Validate MySQL locking reads option `<'str/None'>`"""
    return _validate_set_option(
        read_option, LOCKING_READS_OPTIONS, "LOCK option"
    )

# Reader -----------------------------------------------------------------------------------
cdef inline object read_bool_config(int value):
    """Read MySQL boolean value `<'int/None'>`
    
    - Returns `None` if value == -1.
    - Returns `False` if value == 0.
    - Returns `True` if value == 1.
    """
    if value < 0:
        return None
    return True if value == 1 else False

# Cleaner ----------------------------------------------------------------------------------
cdef inline str cleanup_expression(str expr):
    """Clean MySQL expression `<'str/None'>`
    
    - If the expression starts with "(" and ends with ")",
      the parentheses will first be removed.
    - Remove all leading and trailing whitespaces.
    """
    if expr is None:
        return None
    expr = expr.strip()
    cdef Py_ssize_t size = str_len(expr)
    if size == 0:
        return None
    if (
        str_tailmatch(expr, "(", 0, size, -1)  # expr.startwith("(")
        and str_tailmatch(expr, ")", 0, size, 1)  # expr.endswith(")")
    ):
        expr = str_substr(expr, 1, size - 1)
        expr = expr.strip()
    return expr

# Partitioning -----------------------------------------------------------------------------
ctypedef enum PARTITIONING_METHOD:
    NONSET
    # Method
    HASH
    LINEAR_HASH
    KEY
    LINEAR_KEY
    RANGE
    RANGE_COLUMNS
    LIST
    LIST_COLUMNS

cdef inline str partitioning_flag_to_method(int flag):
    """Convert partitioning integer flag to its corresponding string method `<'str'>`."""
    # Range
    if flag == PARTITIONING_METHOD.RANGE:
        return "RANGE"
    if flag == PARTITIONING_METHOD.RANGE_COLUMNS:
        return "RANGE COLUMNS"
    # Hash
    if flag == PARTITIONING_METHOD.HASH:
        return "HASH"
    if flag == PARTITIONING_METHOD.LINEAR_HASH:
        return "LINEAR HASH"
    # List
    if flag == PARTITIONING_METHOD.LIST:
        return "LIST"
    if flag == PARTITIONING_METHOD.LIST_COLUMNS:
        return "LIST COLUMNS"
    # Key
    if flag == PARTITIONING_METHOD.KEY:
        return "KEY"
    if flag == PARTITIONING_METHOD.LINEAR_KEY:
        return "LINEAR KEY"
    # Invalid
    raise ValueError("partitioning flag %d is invalid, must be between 1 and 8." % flag)

cdef inline int partitioning_method_to_flag(str method) except -1:
    """Convert partitioning method to its corresponding integer flag `<'int'>`."""
    if method is not None:
        if method == "RANGE":
            return PARTITIONING_METHOD.RANGE
        if method == "RANGE COLUMNS":
            return PARTITIONING_METHOD.RANGE_COLUMNS
        # Hash
        if method == "HASH":
            return PARTITIONING_METHOD.HASH
        if method == "LINEAR HASH":
            return PARTITIONING_METHOD.LINEAR_HASH
        # List
        if method == "LIST":
            return PARTITIONING_METHOD.LIST
        if method == "LIST COLUMNS":
            return PARTITIONING_METHOD.LIST_COLUMNS
        # Key
        if method == "KEY":
            return PARTITIONING_METHOD.KEY
        if method == "LINEAR KEY":
            return PARTITIONING_METHOD.LINEAR_KEY
    raise ValueError(
        "partitioning method '%s' is invalid, must be one of:\n"
        "('HASH', 'LINEAR HASH', 'KEY', 'LINEAR KEY', 'RANGE', "
        "'RANGE COLUMNS', 'LIST', 'LIST COLUMNS')." % method
    )

cdef inline bint is_partition_by_hash(int flag) except -1:
    """Check if the partitioning flag represents PARTITIION BY (LINEAR) HASH `<'bool'>`."""
    return flag in (PARTITIONING_METHOD.HASH, PARTITIONING_METHOD.LINEAR_HASH)

cdef inline bint is_partition_by_key(int flag) except -1:
    """Check if the partitioning flag represents PARTITIION BY (LINEAR) KEY `<'bool'>`."""
    return flag in (PARTITIONING_METHOD.KEY, PARTITIONING_METHOD.LINEAR_KEY)

cdef inline bint is_partition_by_range(int flag) except -1:
    """Check if the partitioning flag represents PARTITIION BY RANGE (COLUMNS) `<'bool'>`."""
    return flag in (PARTITIONING_METHOD.RANGE, PARTITIONING_METHOD.RANGE_COLUMNS)

cdef inline bint is_partition_by_list(int flag) except -1:
    """Check if the partitioning flag represents PARTITIION BY LIST (COLUMNS) `<'bool'>`."""
    return flag in (PARTITIONING_METHOD.LIST, PARTITIONING_METHOD.LIST_COLUMNS)

cdef inline bint is_partition_by_hashkey(int flag) except -1:
    """Check if the partitioning flag represents PARTITIION BY (LINEAR) HASH/KEY `<'bool'>`."""
    return is_partition_by_hash(flag) or is_partition_by_key(flag)

cdef inline bint is_partition_by_rangelist(int flag) except -1:
    """Check if the partitioning flag represents PARTITION BY RANGE/LIST (COLUMNS) `<'bool'>`."""
    return is_partition_by_range(flag) or is_partition_by_list(flag)

cdef inline bint is_partitioning_flag_valid(int flag) except -1:
    """Check if the partitioning flag is valid (1-8) `<'bool'>`."""
    return flag in (
        PARTITIONING_METHOD.RANGE,
        PARTITIONING_METHOD.RANGE_COLUMNS,
        PARTITIONING_METHOD.HASH,
        PARTITIONING_METHOD.LINEAR_HASH,
        PARTITIONING_METHOD.KEY,
        PARTITIONING_METHOD.LINEAR_KEY,
        PARTITIONING_METHOD.LIST,
        PARTITIONING_METHOD.LIST_COLUMNS,
    )

# DML Clause -------------------------------------------------------------------------------
ctypedef enum DML_CLAUSE:
    NONE
    # WITH
    WITH
    # Delete
    DELETE
    # Update
    UPDATE
    # Insert
    INSERT
    INSERT_COLUMNS
    INSERT_VALUES
    SET
    ON_DUPLICATE
    # Select
    SELECT
    FROM
    JOIN
    WHERE
    GROUP_BY
    HAVING
    WINDOW
    SET_OPERATION
    ORDER_BY
    LIMIT
    LOCKING_READS
    INTO

ctypedef enum INSERT_MODE:
    INCOMPLETE
    # Mode
    VALUES_MODE
    SET_MODE
    SELECT_MODE

# TimeTable --------------------------------------------------------------------------------
ctypedef enum TIMETABLE_UNIT:
    UNKNOWN
    # Unit
    YEAR
    QUARTER
    MONTH
    WEEK
    DAY
    HOUR
    MINUTE
    SECOND

cdef inline str validate_timetable_unit(object unit):
    """Validate MySQL storage engine `<'str/None'>`."""
    return _validate_dict_option(unit, TIMETABLE_UNITS, "time unit")

cdef inline str timetable_unit_flag_to_str(int flag):
    """Convert time table unit integer flag to its corresponding string representation `<'str'>`."""
    if flag == TIMETABLE_UNIT.YEAR:
        return "YEAR"
    if flag == TIMETABLE_UNIT.QUARTER:
        return "QUARTER"
    if flag == TIMETABLE_UNIT.MONTH:
        return "MONTH"
    if flag == TIMETABLE_UNIT.WEEK:
        return "WEEK"
    if flag == TIMETABLE_UNIT.DAY:
        return "DAY"
    if flag == TIMETABLE_UNIT.HOUR:
        return "HOUR"
    if flag == TIMETABLE_UNIT.MINUTE:
        return "MINUTE"
    if flag == TIMETABLE_UNIT.SECOND:
        return "SECOND"
    raise ValueError(
        "time unit flag %d is invalid, must be between 1 and 8." % flag
    )

cdef inline int timetable_unit_str_to_flag(str unit) except -1:
    """Convert time table unit string representation to its corresponding integer flag `<'int'>`."""
    if unit is not None:
        if unit == "YEAR":
            return TIMETABLE_UNIT.YEAR
        if unit == "QUARTER":
            return TIMETABLE_UNIT.QUARTER
        if unit == "MONTH":
            return TIMETABLE_UNIT.MONTH
        if unit == "WEEK":
            return TIMETABLE_UNIT.WEEK
        if unit == "DAY":
            return TIMETABLE_UNIT.DAY
        if unit == "HOUR":
            return TIMETABLE_UNIT.HOUR
        if unit == "MINUTE":
            return TIMETABLE_UNIT.MINUTE
        if unit == "SECOND":
            return TIMETABLE_UNIT.SECOND
    raise ValueError(
        "time unit '%s' is invalid, must be one of:\n"
        "('YEAR', 'QUARTER', 'MONTH', 'WEEK', 'DAY', "
        "'HOUR', 'MINUTE', 'SECOND')" % unit
    )
