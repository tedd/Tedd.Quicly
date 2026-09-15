namespace Tedd.Quicly.Http3;

/// <summary>
/// SETTINGS identifiers (RFC 9114 §7.2.4.1, RFC 9204 §5, RFC 9220, RFC 9297, draft-ietf-webtrans-http3).
/// </summary>
/// <remarks>
/// The WebTransport identifiers are the <c>0xc671706a</c> family of draft-ietf-webtrans-http3 up to -12, which is
/// what docs/PROTOCOL.md §5 pins and what deployed browsers negotiate. draft-13 renumbered them
/// (SETTINGS_WT_MAX_SESSIONS 0x14e9cd29, WT_INITIAL_MAX_DATA 0x2b61, WT_INITIAL_MAX_STREAMS_UNI 0x2b64,
/// WT_INITIAL_MAX_STREAMS_BIDI 0x2b65); those codepoints are deliberately not recognised and are ignored like any
/// other unknown setting.
/// </remarks>
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
    /// <summary>SETTINGS_WT_INITIAL_MAX_DATA (0xc671706b): initial session-level flow-control credit in bytes.</summary>
    WebTransportInitialMaxData = 0xc671706b,
    /// <summary>SETTINGS_WT_INITIAL_MAX_STREAMS_UNI (0xc671706c): initial unidirectional stream credit per session.</summary>
    WebTransportInitialMaxStreamsUni = 0xc671706c,
    /// <summary>SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI (0xc671706d): initial bidirectional stream credit per session.</summary>
    WebTransportInitialMaxStreamsBidi = 0xc671706d,
}
