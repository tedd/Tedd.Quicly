namespace Tedd.Quicly.Core.Control;

/// <summary>
/// Capability bits announced in <see cref="Hello.Caps"/> and <see cref="HelloAck.Caps"/> (PROTOCOL.md §3.4).
/// Unknown bits are preserved by the codec and ignored by version-1 endpoints.
/// </summary>
[Flags]
public enum PeerCaps : ushort
{
    /// <summary>No capabilities.</summary>
    None = 0,

    /// <summary>QUIC datagrams are supported.</summary>
    Datagrams = 1,

    /// <summary>The transport reports datagram send state (loss/acknowledgement).</summary>
    DatagramSendState = 2,

    /// <summary>LZ4 block compression is supported.</summary>
    Lz4 = 4,
}
