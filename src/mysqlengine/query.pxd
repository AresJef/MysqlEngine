# cython: language_level=3

# Constants
cdef:
    # Order of the query clauses
    dict SELECT_CLAUSES, INSERT_CLAUSES, UPDATE_CLAUSES, DELETE_CLAUSES
    # Clause constrains
    set TIME_CLAUSE_COLUMN_TYPES, JOIN_CLAUSE_METHODS
    # Value clause data types
    set VALUES_CLAUSE_ITEM, VALUES_CLAUSE_NEST_SEQU, VALUES_CLAUSE_FLAT_SEQU
    # Regular expressions
    object ON_DUPLICATE_VALUES_FUNC_RE
    # Compare query special columns
    str UNIQUE_COLUMN, COMPARE_COLUMN, SUBTABLE_COLUMN
    # Join symbol
    str JOIN_SYM
