# cython: language_level=3

# Constants
cdef:
    object ONLY_WHITESPACE_RE, LEAD_WHITESPACE_RE
# Math
cdef double _round_half_away(double num, int ndigits) except *
cdef double _round_half_away_factor(double num, long long f) except *
# String
cdef str _str_clean_name(str s) except *
cdef str _str_dedent(str text) except *
cdef str _str_replace_iter(str s, str new, str old, int iterate) except *
cdef str _str_replace(str s, str new, str old, int iterate) except *
cdef str _str_replaces(str s, str new, tuple olds, int iterate) except *
cdef str _str_replace_pairs(str s, tuple paris, int iterate) except *
cdef str _str_squeeze_spaces(str s, bint strip) except *
cdef int _str_parse_int(str s) except *
cdef double _str_parse_float(str s) except *
# Tuple
cdef set _parse_dtypes(tuple seq) noexcept
# List
cdef bint _list_duplicated(list lst) except *
cdef list _list_duplicates(list lst) except *
cdef list _list_drop_duplicates(list lst) except *
cdef list _chunk_list(list lst, int size, int chunks) except *
# DataFrame
cdef list _chunk_df(object df, int size, int chunks) except *
# Datetime
cdef tuple _cal_time_span(object start, object end, int days, str unit, bint raise_error) except *
cdef list _gen_time_span(object start, object end, int days, str unit, bint raise_error) except *
# Hash
cdef str _hash_md5(object obj) except *
cdef str _hash_sha256(object obj) except *
