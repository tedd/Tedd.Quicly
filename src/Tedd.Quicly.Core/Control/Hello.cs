namespace Tedd.Quicly.Core.Control;

/// <summary>
/// Hello (type 0x10, PROTOCOL.md §3.4): the client's first control-stream frame.
/// Wire body: <c>magic "QLCY"</c>, <c>version u16</c>, <c>flags u16</c>, <c>tableHash u64</c>, <c>lastEpoch u32</c>,
/// <c>sessionToken</c> and <c>authToken</c> (each <c>len varint</c> + bytes, ≤ 4 096), <c>maxReceiveDatagram u16</c>,
/// <c>caps u16</c>; fixed-width fields little-endian.
/// </summary>
/// <remarks>
/// A parsed instance's token spans are slices of the received frame and are valid only as long as that buffer is.
/// </remarks>
public readonly ref struct Hello
{
    /// <summary>
    /// The protocol version announced by the peer. Set by <see cref="ControlCodec.TryParse(ReadOnlySpan{byte}, out Hello)"/>;
    /// the writer always emits <see cref="ControlCodec.ProtocolVersion"/>.
    /// </summary>
    public ushort Version { get; internal init; }

    /// <summary>Hello flags; unknown bits are carried unchanged.</summary>
    public HelloFlags Flags { get; init; }

    /// <summary>xxHash64 of the client's canonical channel table (PROTOCOL.md §1).</summary>
    public ulong TableHash { get; init; }

    /// <summary>The last epoch the client saw for this session (informational; 0 = fresh session).</summary>
    public uint LastEpoch { get; init; }

    /// <summary>Server-minted session token to resume a session (empty = fresh session); ≤ 4 096 bytes.</summary>
    public ReadOnlySpan<byte> SessionToken { get; init; }

    /// <summary>Application authentication token checked by the admission policy; ≤ 4 096 bytes.</summary>
    public ReadOnlySpan<byte> AuthToken { get; init; }

    /// <summary>Largest datagram payload the client will process (0 = no cap beyond the transport's).</summary>
    public ushort MaxReceiveDatagram { get; init; }

    /// <summary>Client capabilities; unknown bits are carried unchanged.</summary>
    public PeerCaps Caps { get; init; }
}
