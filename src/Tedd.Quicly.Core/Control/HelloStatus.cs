namespace Tedd.Quicly.Core.Control;

/// <summary>
/// <see cref="HelloAck.Status"/> values (PROTOCOL.md §3.4). Values not listed here (5 and 8–254) are
/// rejected by the parser with <see cref="ControlParseStatus.InvalidValue"/>.
/// </summary>
public enum HelloStatus : byte
{
    /// <summary>The session is admitted.</summary>
    Accepted = 0,

    /// <summary>The server does not speak the client's protocol version.</summary>
    VersionMismatch = 1,

    /// <summary>The client's table hash differs from the server's channel table.</summary>
    ChannelTableMismatch = 2,

    /// <summary>
    /// Rejected: bad or expired auth token, bad or expired session token, or admission policy. The cause is
    /// never distinguished to the peer.
    /// </summary>
    Rejected = 3,

    /// <summary>The server has no capacity for another session.</summary>
    ServerFull = 4,

    /// <summary>The server failed internally.</summary>
    InternalError = 6,

    /// <summary>One side lacks QUIC datagram support but the channel table needs datagrams.</summary>
    DatagramsRequired = 7,

    /// <summary>Not a handshake answer: a HelloAck-shaped reply to <see cref="ControlType.ChannelTableRequest"/>.</summary>
    Informational = 0xFF,
}
