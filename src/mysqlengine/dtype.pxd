# cython: language_level=3

from cpython cimport datetime

# Constants
cdef:
    datetime.datetime DEFAULT_DATETIME
    long long INTEGER_UNSIGNED_MIN
    long long TINYINT_SIGNED_MIN, TINYINT_SIGNED_MAX, TINYINT_UNSIGNED_MAX
    long long SMALLINT_SIGNED_MIN, SMALLINT_SIGNED_MAX, SMALLINT_UNSIGNED_MAX
    long long MEDIUMINT_SIGNED_MIN, MEDIUMINT_SIGNED_MAX, MEDIUMINT_UNSIGNED_MAX
    long long INT_SIGNED_MIN, INT_SIGNED_MAX, INT_UNSIGNED_MAX
    long long BIGINT_SIGNED_MIN, BIGINT_SIGNED_MAX, BIGINT_UNSIGNED_MAX
    long long TINYTEXT_MAX, TEXT_MAX, MEDIUMTEXT_MAX, LONGTEXT_MAX

# Data type
cdef class DataType:
    # Attributes
    cdef:
        str _mysql, _syntax
        object _python, _default
        bint _primary_key, _tabletime
        bint _auto_increment, _null
    # Utils
    cdef str convert_str(self, object val) except *
    cdef long long convert_int(self, object val) except *
    cdef double convert_float(self, object val) except *
    cdef bytes convert_bytes(self, object val) except *
