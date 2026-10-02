using System.Runtime.InteropServices;
using Tedd.Quicly.Core.Memory;

namespace Tedd.Quicly.Core.State;

/// <summary>Flags carried by a <see cref="ReceiveEntry"/>.</summary>
[Flags]
public enum ReceiveFlags : ushort
{
    /// <summary>No flags.</summary>
    None = 0,
    /// <summary>The message arrived in fragments and was reassembled.</summary>
    Fragmented = 1,
    /// <summary>The payload is compressed; <see cref="ReceiveEntry.RawLength"/> is the decoded size.</summary>
    Compressed = 2,
    /// <summary>A newer version of the same key arrived before this one was polled (statistics only).</summary>
    Superseded = 4,
    /// <summary>The message is a request; <see cref="ReceiveEntry.RequestId"/> is set.</summary>
    IsRequest = 8,
    /// <summary>The message is a response; <see cref="ReceiveEntry.RequestId"/> names the request.</summary>
    IsResponse = 16,
    /// <summary>The peer retired the key after this value (KeyRetired follows).</summary>
    KeyRetired = 32,
    /// <summary>Control-plane message (channel 0).</summary>
    Control = 64,
}

/// <summary>
/// Descriptor of one complete received message, handed from the transport thread to the game thread through the
/// receive ring (or reconstructed from a key mailbox). Exactly 64 bytes, reference-free.
/// </summary>
/// <remarks>
/// Layout (sequential, 8-byte packed): <see cref="Channel"/> (2), <see cref="Flags"/> (2), <see cref="Sequence"/> (4),
/// <see cref="Key"/> (8), <see cref="Lease"/> (16), <see cref="Length"/> (4), <see cref="RawLength"/> (4),
/// <see cref="ReceivedMicrosDelta"/> (4), <see cref="SenderTick"/> (4), <see cref="RequestId"/> (4), the internal
/// credit tag (1, at 52), padding to 64.
/// Written by the transport thread before the ring publish; read by the game thread after the ring's acquire.
/// </remarks>
[StructLayout(LayoutKind.Sequential, Size = Size)]
public struct ReceiveEntry
{
    /// <summary>Size of the struct in bytes.</summary>
    public const int Size = 64;

    /// <summary>Channel id.</summary>
    public ushort Channel;
    /// <summary>Flags.</summary>
    public ReceiveFlags Flags;
    /// <summary>Sequence number as received (0 for channels without sequence).</summary>
    public uint Sequence;
    /// <summary>Message key (keyed channels).</summary>
    public ulong Key;
    /// <summary>Lease holding the payload; released by the game thread after dispatch unless retained.</summary>
    public BufferLease Lease;
    /// <summary>Number of payload bytes in the lease (compressed size when <see cref="ReceiveFlags.Compressed"/>).</summary>
    public int Length;
    /// <summary>Decoded size when compressed, else 0.</summary>
    public int RawLength;
    /// <summary>Microseconds between the peer's reference tick and the receive time (clock-sync input).</summary>
    public uint ReceivedMicrosDelta;
    /// <summary>The sender's tick from the container header, when present.</summary>
    public uint SenderTick;
    /// <summary>Request id for request/response channels (odd for requests), else 0.</summary>
    public uint RequestId;

    /// <summary>
    /// 1 when the message was taken while its channel was limited (<c>ReceiveCredit</c>, CreditTake.Limited): it counts in
    /// the half of the receive budget the reliable channels no handler reads share, and gives that back with its credit.
    /// Written by the transport thread before the ring publish (padding of the 64 bytes; never seen by the application).
    /// </summary>
    internal byte CreditShared;
}
