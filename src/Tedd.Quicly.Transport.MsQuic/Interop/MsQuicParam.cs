namespace Tedd.Quicly.Transport.MsQuic.Interop;

/// <summary><c>QUIC_PARAM_*</c> identifiers for <c>GetParam</c> / <c>SetParam</c>, from msquic.h (MsQuic 2.6.1).</summary>
public static class MsQuicParam
{
    public const uint QUIC_PARAM_PREFIX_GLOBAL = 0x01000000;
    public const uint QUIC_PARAM_PREFIX_REGISTRATION = 0x02000000;
    public const uint QUIC_PARAM_PREFIX_CONFIGURATION = 0x03000000;
    public const uint QUIC_PARAM_PREFIX_LISTENER = 0x04000000;
    public const uint QUIC_PARAM_PREFIX_CONNECTION = 0x05000000;
    public const uint QUIC_PARAM_PREFIX_TLS = 0x06000000;
    public const uint QUIC_PARAM_PREFIX_TLS_SCHANNEL = 0x07000000;
    public const uint QUIC_PARAM_PREFIX_STREAM = 0x08000000;

    /// <summary>Combine with any param to make it high priority.</summary>
    public const uint QUIC_PARAM_HIGH_PRIORITY = 0x40000000;

    /// <summary><c>QUIC_PARAM_IS_GLOBAL</c>.</summary>
    public static bool IsGlobal(uint param) => (param & 0x3F000000) == QUIC_PARAM_PREFIX_GLOBAL;

    // Global
    public const uint QUIC_PARAM_GLOBAL_RETRY_MEMORY_PERCENT = 0x01000000;   // uint16_t
    public const uint QUIC_PARAM_GLOBAL_SUPPORTED_VERSIONS = 0x01000001;     // uint32_t[] - network byte order
    public const uint QUIC_PARAM_GLOBAL_LOAD_BALACING_MODE = 0x01000002;     // uint16_t - QUIC_LOAD_BALANCING_MODE
    public const uint QUIC_PARAM_GLOBAL_PERF_COUNTERS = 0x01000003;          // uint64_t[QUIC_PERF_COUNTER_MAX]
    public const uint QUIC_PARAM_GLOBAL_LIBRARY_VERSION = 0x01000004;        // uint32_t[4]
    public const uint QUIC_PARAM_GLOBAL_SETTINGS = 0x01000005;               // QUIC_SETTINGS
    public const uint QUIC_PARAM_GLOBAL_GLOBAL_SETTINGS = 0x01000006;        // QUIC_GLOBAL_SETTINGS
    public const uint QUIC_PARAM_GLOBAL_VERSION_SETTINGS = 0x01000007;       // QUIC_VERSION_SETTINGS (preview)
    public const uint QUIC_PARAM_GLOBAL_LIBRARY_GIT_HASH = 0x01000008;       // char[64]
    public const uint QUIC_PARAM_GLOBAL_EXECUTION_CONFIG = 0x01000009;       // QUIC_GLOBAL_EXECUTION_CONFIG (preview)
    public const uint QUIC_PARAM_GLOBAL_TLS_PROVIDER = 0x0100000A;           // QUIC_TLS_PROVIDER
    public const uint QUIC_PARAM_GLOBAL_STATELESS_RESET_KEY = 0x0100000B;    // uint8_t[QUIC_STATELESS_RESET_KEY_LENGTH]
    public const uint QUIC_PARAM_GLOBAL_STATISTICS_V2_SIZES = 0x0100000C;    // uint32_t[]
    public const uint QUIC_PARAM_GLOBAL_STATELESS_RETRY_CONFIG = 0x0100000D; // QUIC_STATELESS_RETRY_CONFIG
    public const uint QUIC_PARAM_GLOBAL_XDP_MAP_CONFIG = 0x0100000E;         // QUIC_XDP_MAP_CONFIG[] (preview)

    // Configuration
    public const uint QUIC_PARAM_CONFIGURATION_SETTINGS = 0x03000000;                       // QUIC_SETTINGS
    public const uint QUIC_PARAM_CONFIGURATION_TICKET_KEYS = 0x03000001;                    // QUIC_TICKET_KEY_CONFIG[]
    public const uint QUIC_PARAM_CONFIGURATION_VERSION_SETTINGS = 0x03000002;               // QUIC_VERSION_SETTINGS (preview)
    public const uint QUIC_PARAM_CONFIGURATION_SCHANNEL_CREDENTIAL_ATTRIBUTE_W = 0x03000003; // QUIC_SCHANNEL_CREDENTIAL_ATTRIBUTE_W

    // Listener
    public const uint QUIC_PARAM_LISTENER_LOCAL_ADDRESS = 0x04000000;   // QUIC_ADDR
    public const uint QUIC_PARAM_LISTENER_STATS = 0x04000001;           // QUIC_LISTENER_STATISTICS
    public const uint QUIC_PARAM_LISTENER_CIBIR_ID = 0x04000002;        // uint8_t[] {offset, id[]} (preview)
    public const uint QUIC_PARAM_LISTENER_PARTITION_INDEX = 0x04000005; // uint16_t (preview)
    public const uint QUIC_PARAM_DOS_MODE_EVENTS = 0x04000004;          // BOOLEAN

    // Connection
    public const uint QUIC_PARAM_CONN_QUIC_VERSION = 0x05000000;             // uint32_t
    public const uint QUIC_PARAM_CONN_LOCAL_ADDRESS = 0x05000001;            // QUIC_ADDR
    public const uint QUIC_PARAM_CONN_REMOTE_ADDRESS = 0x05000002;           // QUIC_ADDR
    public const uint QUIC_PARAM_CONN_IDEAL_PROCESSOR = 0x05000003;          // uint16_t
    public const uint QUIC_PARAM_CONN_SETTINGS = 0x05000004;                 // QUIC_SETTINGS
    public const uint QUIC_PARAM_CONN_STATISTICS = 0x05000005;               // QUIC_STATISTICS
    public const uint QUIC_PARAM_CONN_STATISTICS_PLAT = 0x05000006;          // QUIC_STATISTICS
    public const uint QUIC_PARAM_CONN_SHARE_UDP_BINDING = 0x05000007;        // uint8_t (BOOLEAN)
    public const uint QUIC_PARAM_CONN_LOCAL_BIDI_STREAM_COUNT = 0x05000008;  // uint16_t
    public const uint QUIC_PARAM_CONN_LOCAL_UNIDI_STREAM_COUNT = 0x05000009; // uint16_t
    public const uint QUIC_PARAM_CONN_MAX_STREAM_IDS = 0x0500000A;           // uint64_t[4]
    public const uint QUIC_PARAM_CONN_CLOSE_REASON_PHRASE = 0x0500000B;      // char[]
    public const uint QUIC_PARAM_CONN_STREAM_SCHEDULING_SCHEME = 0x0500000C; // QUIC_STREAM_SCHEDULING_SCHEME
    public const uint QUIC_PARAM_CONN_DATAGRAM_RECEIVE_ENABLED = 0x0500000D; // uint8_t (BOOLEAN)
    public const uint QUIC_PARAM_CONN_DATAGRAM_SEND_ENABLED = 0x0500000E;    // uint8_t (BOOLEAN)
    public const uint QUIC_PARAM_CONN_DISABLE_1RTT_ENCRYPTION = 0x0500000F;  // uint8_t (BOOLEAN) (insecure features)
    public const uint QUIC_PARAM_CONN_RESUMPTION_TICKET = 0x05000010;        // uint8_t[]
    public const uint QUIC_PARAM_CONN_PEER_CERTIFICATE_VALID = 0x05000011;   // uint8_t (BOOLEAN)
    public const uint QUIC_PARAM_CONN_LOCAL_INTERFACE = 0x05000012;          // uint32_t
    public const uint QUIC_PARAM_CONN_TLS_SECRETS = 0x05000013;              // QUIC_TLS_SECRETS
    public const uint QUIC_PARAM_CONN_VERSION_SETTINGS = 0x05000014;         // QUIC_VERSION_SETTINGS (preview)
    public const uint QUIC_PARAM_CONN_CIBIR_ID = 0x05000015;                 // uint8_t[] {offset, id[]} (preview)
    public const uint QUIC_PARAM_CONN_STATISTICS_V2 = 0x05000016;            // QUIC_STATISTICS_V2
    public const uint QUIC_PARAM_CONN_STATISTICS_V2_PLAT = 0x05000017;       // QUIC_STATISTICS_V2
    public const uint QUIC_PARAM_CONN_ORIG_DEST_CID = 0x05000018;            // uint8_t[]
    public const uint QUIC_PARAM_CONN_SEND_DSCP = 0x05000019;                // uint8_t
    public const uint QUIC_PARAM_CONN_NETWORK_STATISTICS = 0x05000020;       // QUIC_NETWORK_STATISTICS (preview)
    public const uint QUIC_PARAM_CONN_CLOSE_ASYNC = 0x0500001A;              // uint8_t (preview)

    // TLS
    public const uint QUIC_PARAM_TLS_HANDSHAKE_INFO = 0x06000000;  // QUIC_HANDSHAKE_INFO
    public const uint QUIC_PARAM_TLS_NEGOTIATED_ALPN = 0x06000001; // uint8_t[] (max 255 bytes)

    // TLS (Schannel only)
    public const uint QUIC_PARAM_TLS_SCHANNEL_CONTEXT_ATTRIBUTE_W = 0x07000000;    // QUIC_SCHANNEL_CONTEXT_ATTRIBUTE_W
    public const uint QUIC_PARAM_TLS_SCHANNEL_CONTEXT_ATTRIBUTE_EX_W = 0x07000001; // QUIC_SCHANNEL_CONTEXT_ATTRIBUTE_EX_W
    public const uint QUIC_PARAM_TLS_SCHANNEL_SECURITY_CONTEXT_TOKEN = 0x07000002; // HANDLE

    // Stream
    public const uint QUIC_PARAM_STREAM_ID = 0x08000000;                     // QUIC_UINT62
    public const uint QUIC_PARAM_STREAM_0RTT_LENGTH = 0x08000001;            // uint64_t
    public const uint QUIC_PARAM_STREAM_IDEAL_SEND_BUFFER_SIZE = 0x08000002; // uint64_t - bytes
    public const uint QUIC_PARAM_STREAM_PRIORITY = 0x08000003;               // uint16_t - 0 (low) to 0xFFFF (high) - 0x7FFF (default)
    public const uint QUIC_PARAM_STREAM_STATISTICS = 0x08000004;             // QUIC_STREAM_STATISTICS
    public const uint QUIC_PARAM_STREAM_RELIABLE_OFFSET = 0x08000005;        // uint64_t (preview)
}
