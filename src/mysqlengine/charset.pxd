# cython: language_level=3

# Constant
cdef:
    dict MBLENGTH

# Charset
cdef class Charset:
    # Attributes
    cdef:
        int _id
        str _name, _collate
        bint _is_default
    # Properties
    cdef str get_encoding(self) noexcept
    cdef bint get_is_binary(self) noexcept

cdef class Charsets:
    # Attributes
    cdef:
        dict _by_id, _by_name
        dict _by_collate, _by_name_and_collate
    # Methods
    cdef Charset _get_by_id(self, int id) noexcept
    cdef Charset _get_by_name(self, str name) noexcept
    cdef Charset _get_by_collate(self, str collate) noexcept
    cdef Charset _get_by_name_and_collate(self, str name, str collate) noexcept

cdef Charsets charsets

# Functions
cpdef Charset charset_by_id(int id) noexcept
cpdef Charset charset_by_name(str name) noexcept
cpdef Charset charset_by_collate(str collate) noexcept
cpdef Charset charset_by_name_and_collate(str name, str collate) noexcept
