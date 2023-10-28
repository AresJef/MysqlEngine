# cython: language_level=3

# Cython imports
import cython
from cython.cimports.cpython.bytes import PyBytes_GET_SIZE as bytes_len  # type: ignore
from cython.cimports.cpython.bytearray import PyByteArray_GET_SIZE as bytearray_len  # type: ignore
from cython.cimports.mysqlengine.charset import MBLENGTH  # type: ignore
from cython.cimports.mysqlengine import constant, errors  # type: ignore

# Python imports
import sys
from hashlib import sha1, sha256
from struct import unpack_from as struct_unpack_from, Struct
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding
from mysqlengine import constant, errors

__all__ = [
    "MysqlPacket",
    "FieldDescriptorPacket",
    "OKPacketWrapper",
    "EOFPacketWrapper",
    "LoadLocalPacketWrapper",
    "scramble_native_password",
    "scramble_caching_sha2",
    "sha2_rsa_encrypt",
]

# Debug ================================================================================================
DEBUG: cython.bint = False

# Constant =============================================================================================
NULL_COLUMN: cython.int = 251
UNSIGNED_CHAR_COLUMN: cython.int = 251
UNSIGNED_SHORT_COLUMN: cython.int = 252
UNSIGNED_INT24_COLUMN: cython.int = 253
UNSIGNED_INT64_COLUMN: cython.int = 254
SCRAMBLE_LENGTH: cython.int = 20


# Packet ==============================================================================================
@cython.cclass
class MysqlPacket:
    """Represents a MySQL response packet.

    Provides an interface for reading/parsing the packet results.
    """

    _data: bytes
    _encoding: str
    _dlen: cython.longlong
    _position: cython.longlong

    def __init__(self, data: bytes, encoding: str = None):
        """MySQL response packet.

        :param data: `<bytes>` Buffer result from Mysql response.
        :param encoding: `<str>` The encoding of the response. Defaults to `None`.
        """
        self._data = data
        self._encoding = encoding
        self._dlen = bytes_len(self._data)
        self._position = 0

    # Read ---------------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def get_data(self) -> bytes:
        return self._data

    @cython.cfunc
    @cython.inline(True)
    def get_bytes(
        self,
        position: cython.longlong,
        length: cython.longlong = 1,
    ) -> bytes:
        """Get 'length' bytes starting at 'position'.

        Position is start of payload (first four packet header bytes are not
        included) starting at index '0'.

        No error checking is done.  If requesting outside end of buffer
        an empty string (or string shorter than 'length') may be returned!
        """
        return self._data[position : position + length]

    @cython.cfunc
    @cython.inline(True)
    def read(self, size: cython.longlong) -> bytes:
        """Read the first 'size' bytes in packet and advance cursor past them."""
        self._varify_position()
        result: bytes = self._data[self._position : self._position + size]
        if bytes_len(result) != size:
            error = (
                "Result length not equal to requested length:\n"
                "Expected=%s | Actual=%s | Position: %s | Data Length: %s"
                % (size, bytes_len(result), self._position, self._dlen)
            )
            if DEBUG:
                print(error)
                self.dump()
            raise AssertionError(error)
        self._position += size
        return result

    @cython.cfunc
    @cython.inline(True)
    def read_all(self) -> bytes:
        """Read all remaining data in the packet.

        (Subsequent read() will return errors.)
        """
        self._varify_position()
        result: bytes = self._data[self._position : self._dlen]
        self._position = -1  # ensure no subsequent read()
        return result

    @cython.cfunc
    @cython.inline(True)
    def read_uint8(self) -> cython.int:
        self._varify_position()
        result = self._data[self._position]
        self._position += 1
        return result

    @cython.cfunc
    @cython.inline(True)
    def read_uint16(self) -> cython.longlong:
        self._varify_position()
        result = struct_unpack_from("<H", self._data, self._position)[0]
        self._position += 2
        return result

    @cython.cfunc
    @cython.inline(True)
    def read_uint24(self) -> cython.longlong:
        self._varify_position()
        low: cython.longlong
        high: cython.longlong
        low, high = struct_unpack_from("<HB", self._data, self._position)
        self._position += 3
        return low + (high << 16)

    @cython.cfunc
    @cython.inline(True)
    def read_uint32(self) -> cython.longlong:
        self._varify_position()
        result = struct_unpack_from("<I", self._data, self._position)[0]
        self._position += 4
        return result

    @cython.cfunc
    @cython.inline(True)
    def read_uint64(self) -> cython.longlong:
        self._varify_position()
        result = struct_unpack_from("<Q", self._data, self._position)[0]
        self._position += 8
        return result

    @cython.cfunc
    @cython.inline(True)
    def read_string(self) -> bytes:
        self._varify_position()
        end_pos: cython.longlong = self._data.find(b"\0", self._position)
        if end_pos < 0:
            return None
        result = self._data[self._position : end_pos]
        self._position = end_pos + 1
        return result

    @cython.cfunc
    @cython.inline(True)
    def read_length_encoded_integer(self) -> cython.longlong:
        """Read a 'Length Coded Binary' number from the data buffer.

        Length coded numbers can be anywhere from 1 to 9 bytes depending
        on the value of the first byte.
        """
        c = self.read_uint8()
        if c == NULL_COLUMN:
            return -1
        elif c < UNSIGNED_CHAR_COLUMN:
            return c
        elif c == UNSIGNED_SHORT_COLUMN:
            return self.read_uint16()
        elif c == UNSIGNED_INT24_COLUMN:
            return self.read_uint24()
        elif c == UNSIGNED_INT64_COLUMN:
            return self.read_uint64()

    @cython.cfunc
    @cython.inline(True)
    def read_length_coded_string(self) -> bytes:
        """Read a 'Length Coded String' from the data buffer.

        A 'Length Coded String' consists first of a length coded
        (unsigned, positive) integer represented in 1-9 bytes followed by
        that many bytes of binary data.  (For example "cat" would be "3cat".)
        """
        length = self.read_length_encoded_integer()
        if length < 0:
            return None
        else:
            return self.read(length)

    @cython.cfunc
    @cython.inline(True)
    def read_struct(self, fmt: str) -> tuple:
        self._varify_position()
        s = Struct(fmt)
        result: tuple = s.unpack_from(self._data, self._position)
        size: cython.longlong = s.size
        self._position += size
        return result

    # Control ------------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    def advance(self, length: cython.longlong):
        """Advance the cursor in data buffer 'length' bytes."""
        self._varify_position()
        new_position = self._position + length
        if not 0 <= new_position <= self._dlen:
            raise Exception(
                "Invalid advance amount (%s) for cursor.  "
                "Position=%s" % (length, new_position)
            )
        self._position = new_position

    @cython.cfunc
    @cython.inline(True)
    def rewind(self, position: cython.longlong = 0):
        """Set the position of the data buffer cursor to 'position'."""
        if not 0 <= position <= self._dlen:
            raise Exception("Invalid position to rewind cursor to: %s." % position)
        self._position = position

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def _varify_position(self) -> cython.bint:
        if self._position < 0:
            raise IndexError("Packet already read in its entirety.")
        return True

    # Check --------------------------------------------------------------------------------------
    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def is_ok_packet(self) -> cython.bint:
        # https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
        if self._data[0] == 0 and self._dlen >= 7:
            return True
        else:
            return False

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def is_eof_packet(self) -> cython.bint:
        # http://dev.mysql.com/doc/internals/en/generic-response-packets.html#packet-EOF_Packet
        # Caution: \xFE may be LengthEncodedInteger.
        # If \xFE is LengthEncodedInteger header, 8bytes followed.
        if self._data[0] == 0xFE and self._dlen < 9:
            return True
        else:
            return False

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def is_auth_switch_request(self) -> cython.bint:
        # http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchRequest
        if self._data[0] == 0xFE:
            return True
        else:
            return False

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def is_extra_auth_data(self) -> cython.bint:
        # https://dev.mysql.com/doc/internals/en/successful-authentication.html
        if self._data[0] == 1:
            return True
        else:
            return False

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def is_resultset_packet(self) -> cython.bint:
        if 1 <= self._data[0] <= 250:
            return True
        else:
            return False

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def is_load_local_packet(self) -> cython.bint:
        if self._data[0] == 0xFB:
            return True
        else:
            return False

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(-1, check=False)
    def is_error_packet(self) -> cython.bint:
        if self._data[0] == 0xFF:
            return True
        else:
            return False

    @cython.cfunc
    @cython.inline(True)
    def check_error(self):
        if self.is_error_packet():
            self.raise_for_error()

    @cython.cfunc
    @cython.inline(True)
    def raise_for_error(self):
        self.rewind()
        self.advance(1)  # field_count == error (we already know that)
        errno = self.read_uint16()
        if DEBUG:
            print("errno =", errno)
        errors.raise_query_exc(self._data)

    @cython.cfunc
    @cython.inline(True)
    def dump(self):
        i: cython.longlong
        try:
            print("packet length:", self._dlen)
            for i in range(1, 7):
                f = sys._getframe(i)
                print("call[%d]: %s (line %d)" % (i, f.f_code.co_name, f.f_lineno))
            print("-" * 66)
        except ValueError:
            pass
        d: bytes
        for i in range(0, min(self._dlen, 256), 16):
            d = self._data[i : i + 16]
            print(
                " ".join([f"{x:02X}" for x in d])
                + "   " * (16 - bytes_len(d))
                + " " * 2
                + "".join([self._printable(x) for x in d])
            )

    @cython.cfunc
    @cython.inline(True)
    @cython.exceptval(check=False)
    def _printable(self, data: bytes) -> str:
        if 32 <= data < 127:
            return chr(data)
        else:
            return "."


@cython.cclass
class FieldDescriptorPacket(MysqlPacket):
    """MysqlPacket that represents a specific column's metadata in the result.

    Parsing is automatically done and the results are exported via attributes
    on the class such as: db, table_name, name, length, type_code.
    """

    catalog: bytes
    db: bytes
    table_name: str
    org_table: str
    name: str
    org_name: str
    charsetnr: cython.longlong
    length: cython.longlong
    type_code: cython.int
    flags: cython.int
    scale: cython.int

    def __init__(self, data: bytes, encoding: str = None):
        """MySQL column descriptor packet.

        :param data: `<bytes>` Buffer result from Mysql response.
        :param encoding: `<str>` The encoding of the response. Defaults to `None`.
        """
        super().__init__(data, encoding)
        self._parse_field_descriptor()

    @cython.cfunc
    @cython.inline(True)
    def _parse_field_descriptor(self):
        """Parse the 'Field Descriptor' (Metadata) packet.

        This is compatible with MySQL 4.1+ (not compatible with MySQL 4.0).
        """
        self.catalog = self.read_length_coded_string()
        self.db = self.read_length_coded_string()
        self.table_name = self.read_length_coded_string().decode(self._encoding)
        self.org_table = self.read_length_coded_string().decode(self._encoding)
        self.name = self.read_length_coded_string().decode(self._encoding)
        self.org_name = self.read_length_coded_string().decode(self._encoding)
        (
            self.charsetnr,
            self.length,
            self.type_code,
            self.flags,
            self.scale,
        ) = self.read_struct("<xHIBHBxx")
        # 'default' is a length coded binary and is still in the buffer?
        # not used for normal result sets...

    @cython.cfunc
    @cython.inline(True)
    def description(self) -> tuple:
        """Provides a 7-item tuple compatible with the Python PEP249 DB Spec."""
        return (
            # The name of the column (name).
            self.name,
            # The type of the column (type_code).
            self.type_code,
            # The actual length of the column in bytes (display_size).
            self.length,
            # The size in bytes of the column on the server (internal_size).
            self.get_column_length(),
            # Total number of digits in columns of type NUMERIC. 0 for other types (precision).
            self.get_column_length(),
            # The decimal places in columns of type NUMERIC. 0 for other types (scale).
            self.scale,
            # Whether the column is NULLABLE (null_ok).
            self.flags % 2 == 0,
        )

    @cython.cfunc
    @cython.inline(True)
    @cython.cdivision(True)
    @cython.exceptval(-1, check=False)
    def get_column_length(self) -> cython.longlong:
        if self.type_code == constant.FIELD_TYPE_VAR_STRING:
            mblen: cython.longlong = MBLENGTH.get(self.charsetnr, 1)
            return self.length // mblen
        else:
            return self.length

    def __str__(self):
        return "{} {!r}.{!r}.{!r}, type={}, flags={:x}".format(
            self.__class__,
            self.db,
            self.table_name,
            self.name,
            self.type_code,
            self.flags,
        )


# Packet wrapper
@cython.cclass
class OKPacketWrapper:
    """OK Packet Wrapper.

    It takes an existing `MysqlPacket` object and wraps around it,
    exposing useful variables while still providing access to the
    original packet objects variables and methods.
    """

    packet: MysqlPacket
    affected_rows: cython.longlong
    insert_id: cython.longlong
    server_status: cython.int
    warning_count: cython.longlong
    message: bytes
    has_next: cython.bint

    def __init__(self, from_packet: MysqlPacket):
        """OK Packet Wrapper.

        :param from_packet: The MysqlPacket to be wrapped.
        """
        if not from_packet.is_ok_packet():
            raise ValueError(
                "Cannot create "
                + str(self.__class__.__name__)
                + " object from invalid packet type"
            )
        self.packet = from_packet
        self.packet.advance(1)

        self.affected_rows = self.packet.read_length_encoded_integer()
        self.insert_id = self.packet.read_length_encoded_integer()
        self.server_status, self.warning_count = self.packet.read_struct("<HH")
        self.message = self.packet.read_all()
        self.has_next = self.server_status & constant.SERVER_MORE_RESULTS_EXISTS

    def __getattr__(self, key):
        return getattr(self.packet, key)


@cython.cclass
class EOFPacketWrapper:
    """EOF Packet Wrapper.

    It takes an existing `MysqlPacket` object and wraps around it,
    exposing useful variables while still providing access to the
    original packet objects variables and methods.
    """

    packet: MysqlPacket
    server_status: cython.int
    warning_count: cython.longlong
    has_next: cython.bint

    def __init__(self, from_packet: MysqlPacket):
        """EOF Packet Wrapper.

        :param from_packet: The MysqlPacket to be wrapped.
        """
        if not from_packet.is_eof_packet():
            raise ValueError(
                f"Cannot create '{self.__class__}' object from invalid packet type"
            )

        self.packet = from_packet
        self.warning_count, self.server_status = self.packet.read_struct("<xhh")
        if DEBUG:
            print("server_status=", self.server_status)
        self.has_next = self.server_status & constant.SERVER_MORE_RESULTS_EXISTS

    def __getattr__(self, key):
        return getattr(self.packet, key)


@cython.cclass
class LoadLocalPacketWrapper:
    """Load Local Packet Wrapper.

    It takes an existing `MysqlPacket` object and wraps around it,
    exposing useful variables while still providing access to the
    original packet objects variables and methods.
    """

    packet: MysqlPacket
    filename: bytes

    def __init__(self, from_packet: MysqlPacket):
        """Load Local Packet Wrapper.

        :param from_packet: The MysqlPacket to be wrapped.
        """
        if not from_packet.is_load_local_packet():
            raise ValueError(
                f"Cannot create '{self.__class__}' object from invalid packet type"
            )

        self.packet = from_packet
        self.filename = self.packet.get_data[1:]
        if DEBUG:
            print("filename=", self.filename)

    def __getattr__(self, key):
        return getattr(self.packet, key)


# Auth ================================================================================================
@cython.cfunc
@cython.inline(True)
def scramble_native_password(password: bytes, salt: bytes) -> bytes:
    """Scramble used for mysql_native_password"""
    if not password:
        return b""

    stage1 = sha1(password).digest()
    stage2 = sha1(stage1).digest()
    s = sha1()
    s.update(salt[:SCRAMBLE_LENGTH])
    s.update(stage2)
    result = s.digest()

    msg1 = bytearray(result)
    msg2 = bytearray(stage1)
    i: cython.int
    for i in range(bytearray_len(msg1)):
        msg1[i] ^= msg2[i]
    return bytes(msg1)


@cython.cfunc
@cython.inline(True)
def scramble_caching_sha2(password: bytes, salt: bytes) -> bytes:
    """Scramble algorithm used in cached_sha2_password fast path.

    XOR(SHA256(password), SHA256(SHA256(SHA256(password)), nonce))
    """
    if not password:
        return b""

    p1 = sha256(password).digest()
    p2 = sha256(p1).digest()
    p3 = sha256(p2 + salt).digest()

    msg1 = bytearray(p1)
    msg2 = bytearray(p3)
    i: cython.int
    for i in range(bytearray_len(msg2)):
        msg1[i] ^= msg2[i]
    return bytes(msg1)


@cython.cfunc
@cython.inline(True)
def sha2_rsa_encrypt(password: bytes, salt: bytes, public_key: bytes) -> bytes:
    """Encrypt password with salt and public_key.

    Used for sha256_password and caching_sha2_password.
    """
    rsa_key = serialization.load_pem_public_key(public_key, default_backend())
    return rsa_key.encrypt(
        _xor_password(password + b"\0", salt),
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA1()),
            algorithm=hashes.SHA1(),
            label=None,
        ),
    )


@cython.cfunc
@cython.inline(True)
def _xor_password(password: bytes, salt: bytes) -> bytes:
    # Trailing NUL character will be added in Auth Switch Request.
    # See https://github.com/mysql/mysql-server/blob/7d10c82196c8e45554f27c00681474a9fb86d137/sql/auth/sha2_password.cc#L939-L945
    msg1 = bytearray(password)
    msg2 = bytearray(salt[:SCRAMBLE_LENGTH])
    salt_len: cython.int = bytearray_len(msg2)
    i: cython.int
    for i in range(bytearray_len(msg1)):
        msg1[i] ^= msg2[i % salt_len]
    return bytes(msg1)
