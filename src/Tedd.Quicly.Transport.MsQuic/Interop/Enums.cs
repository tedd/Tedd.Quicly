// Enum names follow msquic.h; member names follow the runtime's own bindings (Microsoft.Quic), i.e. the
// msquic.h member with the enum's common prefix stripped (QUIC_SEND_FLAG_FIN -> QUIC_SEND_FLAGS.FIN).
// Values are validated against docs/reference/msquic/msquic_layout_dotnet11_preview7.txt by LayoutTests.

namespace Tedd.Quicly.Transport.MsQuic.Interop;

/// <summary>TLS provider MsQuic was built with (<c>QUIC_PARAM_GLOBAL_TLS_PROVIDER</c>).</summary>
public enum QUIC_TLS_PROVIDER
{
    SCHANNEL = 0,
    OPENSSL = 1,
}

/// <summary>Execution profile of a registration (worker thread behaviour).</summary>
public enum QUIC_EXECUTION_PROFILE
{
    LOW_LATENCY = 0,
    MAX_THROUGHPUT = 1,
    SCAVENGER = 2,
    REAL_TIME = 3,
}

/// <summary>Server-ID load balancing mode.</summary>
public enum QUIC_LOAD_BALANCING_MODE
{
    DISABLED = 0,
    SERVER_ID_IP = 1,
    SERVER_ID_FIXED = 2,
    COUNT = 3,
}

/// <summary>TLS alert codes used with <c>ConnectionCertificateValidationComplete</c>.</summary>
public enum QUIC_TLS_ALERT_CODES
{
    SUCCESS = 0xFFFF,
    UNEXPECTED_MESSAGE = 10,
    BAD_CERTIFICATE = 42,
    UNSUPPORTED_CERTIFICATE = 43,
    CERTIFICATE_REVOKED = 44,
    CERTIFICATE_EXPIRED = 45,
    CERTIFICATE_UNKNOWN = 46,
    ILLEGAL_PARAMETER = 47,
    UNKNOWN_CA = 48,
    ACCESS_DENIED = 49,
    INSUFFICIENT_SECURITY = 71,
    INTERNAL_ERROR = 80,
    USER_CANCELED = 90,
    CERTIFICATE_REQUIRED = 116,
    MAX = 255,
}

/// <summary>Selects which member of the <see cref="QUIC_CREDENTIAL_CONFIG"/> union is used.</summary>
public enum QUIC_CREDENTIAL_TYPE
{
    NONE = 0,
    CERTIFICATE_HASH = 1,
    CERTIFICATE_HASH_STORE = 2,
    CERTIFICATE_CONTEXT = 3,
    CERTIFICATE_FILE = 4,
    CERTIFICATE_FILE_PROTECTED = 5,
    CERTIFICATE_PKCS12 = 6,
}

/// <summary>Credential flags (<c>QUIC_CREDENTIAL_FLAG_*</c>).</summary>
[Flags]
public enum QUIC_CREDENTIAL_FLAGS
{
    NONE = 0x00000000,
    CLIENT = 0x00000001,
    LOAD_ASYNCHRONOUS = 0x00000002,
    NO_CERTIFICATE_VALIDATION = 0x00000004,
    ENABLE_OCSP = 0x00000008,
    INDICATE_CERTIFICATE_RECEIVED = 0x00000010,
    DEFER_CERTIFICATE_VALIDATION = 0x00000020,
    REQUIRE_CLIENT_AUTHENTICATION = 0x00000040,
    USE_TLS_BUILTIN_CERTIFICATE_VALIDATION = 0x00000080,
    REVOCATION_CHECK_END_CERT = 0x00000100,
    REVOCATION_CHECK_CHAIN = 0x00000200,
    REVOCATION_CHECK_CHAIN_EXCLUDE_ROOT = 0x00000400,
    IGNORE_NO_REVOCATION_CHECK = 0x00000800,
    IGNORE_REVOCATION_OFFLINE = 0x00001000,
    SET_ALLOWED_CIPHER_SUITES = 0x00002000,
    USE_PORTABLE_CERTIFICATES = 0x00004000,
    USE_SUPPLIED_CREDENTIALS = 0x00008000,
    USE_SYSTEM_MAPPER = 0x00010000,
    CACHE_ONLY_URL_RETRIEVAL = 0x00020000,
    REVOCATION_CHECK_CACHE_ONLY = 0x00040000,
    INPROC_PEER_CERTIFICATE = 0x00080000,
    SET_CA_CERTIFICATE_FILE = 0x00100000,
    DISABLE_AIA = 0x00200000,
}

/// <summary>Allowed TLS 1.3 cipher suites (<c>QUIC_ALLOWED_CIPHER_SUITE_*</c>).</summary>
[Flags]
public enum QUIC_ALLOWED_CIPHER_SUITE_FLAGS
{
    NONE = 0x0,
    AES_128_GCM_SHA256 = 0x1,
    AES_256_GCM_SHA384 = 0x2,
    CHACHA20_POLY1305_SHA256 = 0x4,
}

/// <summary>Flags for <see cref="QUIC_CERTIFICATE_HASH_STORE"/>.</summary>
[Flags]
public enum QUIC_CERTIFICATE_HASH_STORE_FLAGS
{
    NONE = 0x0000,
    MACHINE_STORE = 0x0001,
}

/// <summary>Flags for <c>ConnectionShutdown</c> / <c>RegistrationShutdown</c>.</summary>
[Flags]
public enum QUIC_CONNECTION_SHUTDOWN_FLAGS
{
    NONE = 0x0000,
    SILENT = 0x0001,
}

/// <summary>Server resumption level (<see cref="QUIC_SETTINGS.ServerResumptionLevel"/>).</summary>
public enum QUIC_SERVER_RESUMPTION_LEVEL
{
    NO_RESUME = 0,
    RESUME_ONLY = 1,
    RESUME_AND_ZERORTT = 2,
}

/// <summary>Flags for <c>ConnectionSendResumptionTicket</c>.</summary>
[Flags]
public enum QUIC_SEND_RESUMPTION_FLAGS
{
    NONE = 0x0000,
    FINAL = 0x0001,
}

/// <summary>Stream scheduling scheme (<c>QUIC_PARAM_CONN_STREAM_SCHEDULING_SCHEME</c>).</summary>
public enum QUIC_STREAM_SCHEDULING_SCHEME
{
    FIFO = 0x0000,
    ROUND_ROBIN = 0x0001,
    COUNT = 2,
}

/// <summary>Flags for <c>StreamOpen</c>.</summary>
[Flags]
public enum QUIC_STREAM_OPEN_FLAGS
{
    NONE = 0x0000,
    UNIDIRECTIONAL = 0x0001,
    ZERO_RTT = 0x0002,
    DELAY_ID_FC_UPDATES = 0x0004,
    APP_OWNED_BUFFERS = 0x0008,
}

/// <summary>Flags for <c>StreamStart</c>.</summary>
[Flags]
public enum QUIC_STREAM_START_FLAGS
{
    NONE = 0x0000,
    IMMEDIATE = 0x0001,
    FAIL_BLOCKED = 0x0002,
    SHUTDOWN_ON_FAIL = 0x0004,
    INDICATE_PEER_ACCEPT = 0x0008,
    PRIORITY_WORK = 0x0010,
}

/// <summary>Flags for <c>StreamShutdown</c>.</summary>
[Flags]
public enum QUIC_STREAM_SHUTDOWN_FLAGS
{
    NONE = 0x0000,
    GRACEFUL = 0x0001,
    ABORT_SEND = 0x0002,
    ABORT_RECEIVE = 0x0004,
    ABORT = 0x0006,
    IMMEDIATE = 0x0008,
    INLINE = 0x0010,
}

/// <summary>Flags on received stream data / datagrams.</summary>
[Flags]
public enum QUIC_RECEIVE_FLAGS
{
    NONE = 0x0000,
    ZERO_RTT = 0x0001,
    FIN = 0x0002,
}

/// <summary>Flags for <c>StreamSend</c> / <c>DatagramSend</c>.</summary>
[Flags]
public enum QUIC_SEND_FLAGS
{
    NONE = 0x0000,
    ALLOW_0_RTT = 0x0001,
    START = 0x0002,
    FIN = 0x0004,
    DGRAM_PRIORITY = 0x0008,
    DELAY_SEND = 0x0010,
    CANCEL_ON_LOSS = 0x0020,
    PRIORITY_WORK = 0x0040,
    CANCEL_ON_BLOCKED = 0x0080,
}

/// <summary>Lifecycle state of a sent datagram (<c>DATAGRAM_SEND_STATE_CHANGED</c>).</summary>
public enum QUIC_DATAGRAM_SEND_STATE
{
    UNKNOWN = 0,
    SENT = 1,
    LOST_SUSPECT = 2,
    LOST_DISCARDED = 3,
    ACKNOWLEDGED = 4,
    ACKNOWLEDGED_SPURIOUS = 5,
    CANCELED = 6,
}

/// <summary>Helpers for <see cref="QUIC_DATAGRAM_SEND_STATE"/>.</summary>
public static class QuicDatagramSendState
{
    /// <summary>True when MsQuic no longer tracks the datagram (<c>QUIC_DATAGRAM_SEND_STATE_IS_FINAL</c>).</summary>
    public static bool IsFinal(QUIC_DATAGRAM_SEND_STATE state) => state >= QUIC_DATAGRAM_SEND_STATE.LOST_DISCARDED;
}

/// <summary>TLS protocol version negotiated.</summary>
public enum QUIC_TLS_PROTOCOL_VERSION
{
    UNKNOWN = 0,
    TLS_1_3 = 0x3000,
}

/// <summary>Cipher algorithm negotiated.</summary>
public enum QUIC_CIPHER_ALGORITHM
{
    NONE = 0,
    AES_128 = 0x660E,
    AES_256 = 0x6610,
    CHACHA20 = 0x6612,
}

/// <summary>Hash algorithm negotiated.</summary>
public enum QUIC_HASH_ALGORITHM
{
    NONE = 0,
    SHA_256 = 0x800C,
    SHA_384 = 0x800D,
}

/// <summary>Key exchange algorithm negotiated.</summary>
public enum QUIC_KEY_EXCHANGE_ALGORITHM
{
    NONE = 0,
}

/// <summary>TLS 1.3 cipher suite negotiated.</summary>
public enum QUIC_CIPHER_SUITE
{
    TLS_AES_128_GCM_SHA256 = 0x1301,
    TLS_AES_256_GCM_SHA384 = 0x1302,
    TLS_CHACHA20_POLY1305_SHA256 = 0x1303,
}

/// <summary>Congestion control algorithm (<see cref="QUIC_SETTINGS.CongestionControlAlgorithm"/>).</summary>
public enum QUIC_CONGESTION_CONTROL_ALGORITHM
{
    CUBIC = 0,
    BBR = 1,
    MAX = 2,
}

/// <summary>Indices into the <c>QUIC_PARAM_GLOBAL_PERF_COUNTERS</c> array.</summary>
public enum QUIC_PERFORMANCE_COUNTERS
{
    CONN_CREATED = 0,
    CONN_HANDSHAKE_FAIL = 1,
    CONN_APP_REJECT = 2,
    CONN_RESUMED = 3,
    CONN_ACTIVE = 4,
    CONN_CONNECTED = 5,
    CONN_PROTOCOL_ERRORS = 6,
    CONN_NO_ALPN = 7,
    STRM_ACTIVE = 8,
    PKTS_SUSPECTED_LOST = 9,
    PKTS_DROPPED = 10,
    PKTS_DECRYPTION_FAIL = 11,
    UDP_RECV = 12,
    UDP_SEND = 13,
    UDP_RECV_BYTES = 14,
    UDP_SEND_BYTES = 15,
    UDP_RECV_EVENTS = 16,
    UDP_SEND_CALLS = 17,
    APP_SEND_BYTES = 18,
    APP_RECV_BYTES = 19,
    CONN_QUEUE_DEPTH = 20,
    CONN_OPER_QUEUE_DEPTH = 21,
    CONN_OPER_QUEUED = 22,
    CONN_OPER_COMPLETED = 23,
    WORK_OPER_QUEUE_DEPTH = 24,
    WORK_OPER_QUEUED = 25,
    WORK_OPER_COMPLETED = 26,
    PATH_VALIDATED = 27,
    PATH_FAILURE = 28,
    SEND_STATELESS_RESET = 29,
    SEND_STATELESS_RETRY = 30,
    CONN_LOAD_REJECT = 31,
    MAX = 32,
}

/// <summary>Listener event types.</summary>
public enum QUIC_LISTENER_EVENT_TYPE
{
    NEW_CONNECTION = 0,
    STOP_COMPLETE = 1,
    DOS_MODE_CHANGED = 2,
}

/// <summary>Connection event types.</summary>
public enum QUIC_CONNECTION_EVENT_TYPE
{
    CONNECTED = 0,
    SHUTDOWN_INITIATED_BY_TRANSPORT = 1,
    SHUTDOWN_INITIATED_BY_PEER = 2,
    SHUTDOWN_COMPLETE = 3,
    LOCAL_ADDRESS_CHANGED = 4,
    PEER_ADDRESS_CHANGED = 5,
    PEER_STREAM_STARTED = 6,
    STREAMS_AVAILABLE = 7,
    PEER_NEEDS_STREAMS = 8,
    IDEAL_PROCESSOR_CHANGED = 9,
    DATAGRAM_STATE_CHANGED = 10,
    DATAGRAM_RECEIVED = 11,
    DATAGRAM_SEND_STATE_CHANGED = 12,
    RESUMED = 13,
    RESUMPTION_TICKET_RECEIVED = 14,
    PEER_CERTIFICATE_RECEIVED = 15,
    RELIABLE_RESET_NEGOTIATED = 16,
    ONE_WAY_DELAY_NEGOTIATED = 17,
    NETWORK_STATISTICS = 18,
}

/// <summary>Stream event types.</summary>
public enum QUIC_STREAM_EVENT_TYPE
{
    START_COMPLETE = 0,
    RECEIVE = 1,
    SEND_COMPLETE = 2,
    PEER_SEND_SHUTDOWN = 3,
    PEER_SEND_ABORTED = 4,
    PEER_RECEIVE_ABORTED = 5,
    SEND_SHUTDOWN_COMPLETE = 6,
    SHUTDOWN_COMPLETE = 7,
    IDEAL_SEND_BUFFER_SIZE = 8,
    PEER_ACCEPTED = 9,
    CANCEL_ON_LOSS = 10,
}
