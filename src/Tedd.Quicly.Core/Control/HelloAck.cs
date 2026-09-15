namespace Tedd.Quicly.Core.Control;

/// <summary>
/// HelloAck (type 0x11, PROTOCOL.md §3.4): the server's answer to <see cref="Hello"/>, and (with
/// <see cref="HelloStatus.Informational"/>) the answer to <see cref="ControlType.ChannelTableRequest"/>.
/// Wire body: <c>status u8</c>, <c>sessionId u64</c>, <c>epoch u32</c>, <c>maxReceiveDatagram u16</c>, <c>caps u16</c>,
/// <c>maxMessageSize</c>, <c>heartbeatMicros</c>, <c>graceMicros</c> (varints), <c>sessionToken</c> (<c>len varint</c> + bytes,
/// ≤ 4 096), <c>tableIncluded u8</c> [+ table section], <c>reason</c> (<c>len varint</c> + UTF-8, ≤ 512).
/// </summary>
/// <remarks>
/// <para>
/// The table section is opaque to this codec: the session layer supplies the already-encoded canonical table
/// followed by the channel names (PROTOCOL.md §3.4) and interprets it on receipt. The codec only checks that
/// the section is structurally well-formed (so the reason that follows can be located): minimal varints, channel
/// ids in [2, 16383], names ≤ 64 bytes of valid UTF-8. Semantic validation (ascending ids, modes, flags) belongs to
/// the channel-table parser.
/// </para>
/// <para>A parsed instance's spans are slices of the received frame and are valid only as long as that buffer is.</para>
/// </remarks>
public readonly ref struct HelloAck
{
    /// <summary>Admission result; must be a defined <see cref="HelloStatus"/> value.</summary>
    public HelloStatus Status { get; init; }

    /// <summary>Server-chosen random session identifier (not a secret).</summary>
    public ulong SessionId { get; init; }

    /// <summary>Epoch of this connection within the session (starts at 1, strictly increasing).</summary>
    public uint Epoch { get; init; }

    /// <summary>Largest datagram payload the server will process (0 = no cap beyond the transport's).</summary>
    public ushort MaxReceiveDatagram { get; init; }

    /// <summary>Server capabilities; unknown bits are carried unchanged.</summary>
    public PeerCaps Caps { get; init; }

    /// <summary>Server-wide cap on message size; the effective limit is <c>min(channel.MaxMessageSize, MaxMessageSize)</c>.</summary>
    public ulong MaxMessageSize { get; init; }

    /// <summary>Heartbeat interval in microseconds.</summary>
    public ulong HeartbeatMicros { get; init; }

    /// <summary>How long the session (and its token) survives a lost connection, in microseconds.</summary>
    public ulong GraceMicros { get; init; }

    /// <summary>The fresh session token for the next resume (empty when not accepted); ≤ 4 096 bytes.</summary>
    public ReadOnlySpan<byte> SessionToken { get; init; }

    /// <summary>
    /// The table section (canonical table followed by the names), or empty when no table is included. A table
    /// section is never empty (its channel count alone takes one byte), so <c>tableIncluded</c> = <see cref="HasTable"/>.
    /// </summary>
    public ReadOnlySpan<byte> Table { get; init; }

    /// <summary>UTF-8 reason text, ≤ 512 bytes (peer supplied when parsed: untrusted; sanitise before logging).</summary>
    public ReadOnlySpan<byte> Reason { get; init; }

    /// <summary>Whether a table section is present.</summary>
    public bool HasTable => !Table.IsEmpty;
}
