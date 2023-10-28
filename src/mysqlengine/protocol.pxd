# cython: language_level=3

# Constant =============================================================================================
cdef:
    int NULL_COLUMN, UNSIGNED_CHAR_COLUMN, UNSIGNED_SHORT_COLUMN
    int UNSIGNED_INT24_COLUMN, UNSIGNED_INT64_COLUMN
    int SCRAMBLE_LENGTH
    bint DEBUG

# Packet ===============================================================================================
cdef class MysqlPacket:
    # Attributes
    cdef:
        bytes _data
        str _encoding
        long long _dlen
        long long _position

    # Methods
    cdef bytes get_data(self) noexcept
    cdef bytes read(self, long long size) except *
    cdef bytes read_all(self) except *
    cdef advance(self, long long length) except *
    cdef rewind(self, long long position=?) except *
    cdef bint _varify_position(self) except -1
    cdef bytes get_bytes(self, long long position, long long length=?) except *
    cdef int read_uint8(self) except *
    cdef long long read_uint16(self) except *
    cdef long long read_uint24(self) except *
    cdef long long read_uint32(self) except *
    cdef long long read_uint64(self) except *
    cdef bytes read_string(self) except *
    cdef long long read_length_encoded_integer(self) except *
    cdef bytes read_length_coded_string(self) except *
    cdef tuple read_struct(self, str fmt) except *
    cdef bint is_ok_packet(self) except -1
    cdef bint is_eof_packet(self) except -1
    cdef bint is_auth_switch_request(self) except -1
    cdef bint is_extra_auth_data(self) except -1
    cdef bint is_resultset_packet(self) except -1
    cdef bint is_load_local_packet(self) except -1
    cdef bint is_error_packet(self) except -1
    cdef check_error(self) except *
    cdef raise_for_error(self) except *
    cdef str _printable(self, bytes data) noexcept
    cdef dump(self) except *

cdef class FieldDescriptorPacket(MysqlPacket):
    # Attributes
    cdef:
        bytes catalog, db
        str table_name, org_table, name, org_name
        long long charsetnr, length
        int type_code, flags, scale
    # Methods
    cdef _parse_field_descriptor(self) except *
    cdef tuple description(self) except *
    cdef long long get_column_length(self) except -1

# Packet wrapper
cdef class OKPacketWrapper:
    cdef:
        MysqlPacket packet
        long long affected_rows, insert_id
        int server_status
        long long warning_count
        bytes message
        bint has_next

cdef class EOFPacketWrapper:
    cdef:
        MysqlPacket packet
        int server_status
        long long warning_count
        bint has_next

cdef class LoadLocalPacketWrapper:
    cdef:
        MysqlPacket packet
        bytes filename

# Auth ================================================================================================
cdef bytes scramble_native_password(bytes password, bytes salt) except *
cdef bytes scramble_caching_sha2(bytes password, bytes salt) except *
cdef bytes sha2_rsa_encrypt(bytes password, bytes salt, bytes public_key) except *
