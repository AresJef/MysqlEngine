# cython: language_level=3

from mysqlengine.protocol cimport MysqlPacket

# Constant
cdef:
    long long MAX_PACKET_LEN, MAX_AFFECTED_ROWS
    int MAX_STMT_LENGTH
    object INSERT_REPLACE_VALUES_RE
    bytes QUIT_COMMAND_BYTES, CLIENT_INFO_PREFIX
    set MULTIROWS_ARGS_DTYPES

# Mysql Result
cdef class MysqlResult:
    # Attributes
    cdef:
        Connection connection
        long long affected_rows, insert_id, field_count
        int server_status, warning_count
        bint unbuffered_active, has_next
        bytes message
        list fields, _encodings, _converters
        tuple columns, rows
    # Methods
    cdef bint _is_eof_packet(self, MysqlPacket packet) except -1
    cdef _read_ok_packet(self, MysqlPacket first_packet) except *
    cdef tuple _read_row_from_packet(self, MysqlPacket packet) except *
    cdef object _convert_data(self, long long idx, bytes data, str encoding) except *

# Cursor
cdef class Cursor:
    # Attributes
    cdef:
        Connection _connection
        MysqlResult _result
        str _host
        int _port
        long long _row_index, _row_count, _last_row_id
        object _last_query
        tuple _columns, _rows
        bint _echo, _warnings
    # Properties
    cdef Connection get_connection(self) except *
    cdef bint get_closed(self) except -1
    # Execute
    cdef bint is_args_multi_rows(self, object args) except -1
    cdef bytes _escape_row(self, str sql, object args, str encoding, Connection conn) except *
    cdef _clear_result(self) noexcept
    cdef _get_result(self, Connection conn) except *
    cdef _verify_executed(self) except *
    cdef tuple _fetchone(self) except *
    cdef tuple _fetchmany(self, long long rows) except *
    cdef tuple _fetchall(self) except *
    cdef _scroll(self, long long value, str mode) except *

# Connection
cdef class Connection:
    # Attributes
    cdef:
        # . client
        double _last_usage
        str _host, _user, _db
        int _port
        bytes _password
        # . auth
        str _client_auth_plugin, _server_auth_plugin
        str _auth_plugin_used, _unix_socket
        bytes _server_public_key
        bint _ssl, _secure
        object _ssl_context
        # . settings
        str _charset, _encoding
        str _sql_mode, _init_command
        bint _autocommit, _echo
        int _connect_timeout, _client_flag
        object _cursorclass
        bytes _client_info
        # . reader & writer
        object _reader, _writer
        # . query
        MysqlResult _result
        long long _affected_rows, _next_seq_id
        str _host_info, _close_reason, _server_version, _server_charset
        bytes _salt
        bint _reusable
        int _protocol_version, _server_version_major, _server_thread_id
        int _server_capabilities, _server_language, _server_status
    # Properties
    cdef bint get_autocommit(self) except -1
    cdef int get_version_major(self) noexcept
    cdef long long get_insert_id(self) noexcept
    cdef bint get_backslash_escapes(self) except -1
    cdef bint get_reusable(self) except -1
    cdef double get_last_usage(self) noexcept
    cdef bint get_transaction_status(self) except -1
    cdef long long get_affected_rows(self) noexcept
    cdef bint get_closed(self) except -1
    # Query
    cdef str _escape_item(self, object obj) except *
    cdef object _escape_args(self, object args) except *
    # Connect
    cdef _get_server_information(self, MysqlPacket packet) except *
    # Execute
    cdef _ensure_alive(self) except *
    # Write packet
    cdef write_packet(self, bytes payload) except *
    cdef _write_bytes(self, bytes data) except *
    # Close
    cdef _close(self) noexcept
    cdef _close_on_cancel(self) noexcept
    # Utils
    cdef bytes _gen_client_info(self, str program_name) except *
    cdef bytes _lenenc_int(self, int i) except *

# Pool
cdef class Pool:
    # Attributes
    cdef:
        # Conneciton [client]
        str _host, _user, _db
        int _port
        bytes _password
        # Connection [auth]
        str _auth_plugin, _unix_socket
        bytes _server_public_key
        bint _ssl
        object _ssl_context
        # Connection [settings]
        str _charset, _encoding
        str _sql_mode, _init_command, _program_name
        bint _autocommit, _echo
        int _connect_timeout, _client_flag
        object _cursorclass
        # Pool
        int _max_size, _acquiring
        int _recycle, _server_version
        list _free_conn
        set _used_conn, _invlid_conn
        object _condition
        bint _closing, _closed, _backslash_escapes
    # Properties
    cdef int get_version(self) noexcept
    cdef bint get_backslash_escapes(self) except -1
    cdef int get_size(self) noexcept
    cdef int get_free_size(self) noexcept
    cdef int get_used_size(self) noexcept
    cdef int get_invalid_size(self) noexcept
    cdef str get_size_info(self) noexcept
    # Acquire
    cdef Connection _acquire_free_connection(self) except *
    # Special methods
    cdef str _repr_val(self) noexcept

# Server
cdef class Server(Pool):
    # Attributes
    cdef:
        int _query_timeout
    # Properties
    cdef int get_query_timeout(self) noexcept
