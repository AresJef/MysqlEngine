# cython: language_level=3

# Constant
cdef:
    list ESCAPE_TABLE
    object DECODE_DT_RE, DECODE_TIME_RE, DECODE_DELTA_RE
    dict ITEM_ENCODERS_BACKSLASH, ITEM_ENCODERS_NO_BACKSLASH
    dict ARGS_ENCODERS_BACKSLASH, ARGS_ENCODERS_NO_BACKSLASH

# Encoder
cdef str encode_item(object val, bint backslash) except *
cdef object encode_args(object val, bint backslash) except *

# Decoder
cdef dict DECODERS
