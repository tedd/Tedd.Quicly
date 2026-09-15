namespace Tedd.Quicly.Http3;

/// <summary>SETTINGS identifiers (RFC 9114 §7.2.4.1, RFC 9204 §5, RFC 9220, RFC 9297, draft-ietf-webtrans-http3).</summary>
public enum Http3SettingId : ulong
{
    /// <summary>SETTINGS_QPACK_MAX_TABLE_CAPACITY (0x01).</summary>
    QpackMaxTableCapacity = 0x01,
    /// <summary>SETTINGS_MAX_FIELD_SECTION_SIZE (0x06).</summary>
    MaxFieldSectionSize = 0x06,
    /// <summary>SETTINGS_QPACK_BLOCKED_STREAMS (0x07).</summary>
    QpackBlockedStreams = 0x07,
    /// <summary>SETTINGS_ENABLE_CONNECT_PROTOCOL (0x08).</summary>
    EnableConnectProtocol = 0x08,
    /// <summary>SETTINGS_H3_DATAGRAM (0x33).</summary>
    H3Datagram = 0x33,
    /// <summary>SETTINGS_ENABLE_WEBTRANSPORT (0x2b603742).</summary>
    EnableWebTransport = 0x2b603742,
    /// <summary>SETTINGS_WEBTRANSPORT_MAX_SESSIONS (0xc671706a).</summary>
    WebTransportMaxSessions = 0xc671706a,
}
