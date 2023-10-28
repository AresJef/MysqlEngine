# cython: language_level=3

# Regex
cdef class Regex:
    # Attributes
    cdef:
        str _pattern
        str _pattern_lpin, _pattern_rpin, _pattern_dpin
        str _pattern_lsep, _pattern_rsep, _pattern_dsep
        object _re
        object _re_lpin, _re_rpin, _re_dpin
        object _re_lsep, _re_rsep, _re_dsep

cdef Regex TIMETABLE_SFIX

# Table Regex
cdef class TableRegex:
    # Attributes
    cdef:
        Regex _name, _name_gp, _time_gp
        Regex _fname, _fname_gp, _ftime_gp
