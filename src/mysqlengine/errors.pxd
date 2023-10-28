# cython: language_level=3

# Constant
cdef:
    dict QUERY_ERROR_MAP, ASYNCIO_ERRORS_MAP
    set QUERY_ERROR_FATAL

# Handler
cdef raise_query_exc(bytes data) except *
cdef int parse_query_exc_code(Exception exc) except *