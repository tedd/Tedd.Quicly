using System.Runtime.InteropServices;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.State;

/// <summary>Flags in <see cref="KeySendSlot.Flags"/>. Game thread.</summary>
[Flags]
public enum KeySendFlags : byte
{
    /// <summary>No flags.</summary>
    None = 0,
    /// <summary>A value for the key is queued but not yet handed to the transport.</summary>
    Pending = 1,
    /// <summary>The current value travels on a dedicated large-value stream (<see cref="KeySendSlot.LargeValueStream"/>).</summary>
    LargeValue = 2,
    /// <summary>The application retired the key; send KeyRetired after the current value is acked.</summary>
    RetireRequested = 4,
    /// <summary>A retry timer is armed (<see cref="KeySendSlot.RetryDeadline"/> is valid).</summary>
    RetryArmed = 8,
}

/// <summary>Flags in <see cref="KeyRecvSlot.Flags"/>. Transport thread.</summary>
[Flags]
public enum KeyRecvFlags : uint
{
    /// <summary>No flags.</summary>
    None = 0,
    /// <summary>A latest-ack for <see cref="KeyRecvSlot.LastAccepted"/> is queued.</summary>
    AckPending = 1,
    /// <summary>A fragmented value is being reassembled (<see cref="KeyRecvSlot.Reassembly"/> is valid).</summary>
    Reassembling = 2,
    /// <summary>The peer retired the key; the slot is released once the game thread has drained it.</summary>
    Retired = 4,
    /// <summary>At least one value has been accepted (<see cref="KeyRecvSlot.LastAccepted"/> is valid).</summary>
    HasAccepted = 8,
}

/// <summary>
/// Per-key send state (ReliableLatest and keyed sequenced channels): one cache line, native memory, indexed by
/// the key's slot from the channel's <see cref="KeyTable"/>/<see cref="DenseKeyTable"/>. <b>Game thread only.</b>
/// </summary>
/// <remarks>
/// Layout: <see cref="CurrentVersion"/> (0), <see cref="AckedVersion"/> (4), <see cref="CurrentLease"/> (8),
/// <see cref="RetryDeadline"/> (24), <see cref="InFlightEntry"/> (32), <see cref="LargeValueStream"/> (36),
/// <see cref="Attempts"/> (44), <see cref="Flags"/> (45), reserved (46), <see cref="Key"/> (48),
/// <see cref="LastSentMicros"/> (56).
/// </remarks>
[StructLayout(LayoutKind.Explicit, Size = Size)]
public struct KeySendSlot
{
    /// <summary>Size in bytes.</summary>
    public const int Size = 64;

    /// <summary>Version of the most recent value sent (or queued) for the key.</summary>
    [FieldOffset(0)] public uint CurrentVersion;
    /// <summary>Highest version the peer acknowledged.</summary>
    [FieldOffset(4)] public uint AckedVersion;
    /// <summary>Lease holding the current value (kept for retries until acked).</summary>
    [FieldOffset(8)] public BufferLease CurrentLease;
    /// <summary>Clock micros at which the current value is retransmitted unless acked (valid with <see cref="KeySendFlags.RetryArmed"/>).</summary>
    [FieldOffset(24)] public long RetryDeadline;
    /// <summary>The <see cref="SendEntry"/> slot carrying the current value, or -1.</summary>
    [FieldOffset(32)] public int InFlightEntry;
    /// <summary>Stream used for a large value, when <see cref="KeySendFlags.LargeValue"/>.</summary>
    [FieldOffset(36)] public TransportStreamId LargeValueStream;
    /// <summary>Transmissions of the current version so far (retry budget).</summary>
    [FieldOffset(44)] public byte Attempts;
    /// <summary>Flags.</summary>
    [FieldOffset(45)] public KeySendFlags Flags;
    /// <summary>The key (reverse lookup for hashed key spaces).</summary>
    [FieldOffset(48)] public ulong Key;
    /// <summary>Clock micros of the last transmission.</summary>
    [FieldOffset(56)] public long LastSentMicros;
}

/// <summary>
/// Per-key receive state: one cache line, native memory, indexed by key slot. <b>Transport thread only</b>, except
/// <see cref="Mailbox"/> which mirrors the index of the key's entry in the channel's <see cref="Mailboxes"/>
/// (the mailbox itself is the cross-thread cell).
/// </summary>
/// <remarks>
/// Layout: <see cref="LastAccepted"/> (0), <see cref="Mailbox"/> (4), <see cref="Reassembly"/> (8),
/// <see cref="Flags"/> (12), <see cref="LastUpdateMicros"/> (16), <see cref="Key"/> (24),
/// <see cref="PendingAckVersion"/> (32), <see cref="Updates"/> (36), reserved (40 … 63).
/// </remarks>
[StructLayout(LayoutKind.Explicit, Size = Size)]
public struct KeyRecvSlot
{
    /// <summary>Size in bytes.</summary>
    public const int Size = 64;

    /// <summary>Last accepted sequence/version for the key.</summary>
    [FieldOffset(0)] public uint LastAccepted;
    /// <summary>Index of the key's mailbox in the channel's <see cref="Mailboxes"/> (normally the slot itself), or -1.</summary>
    [FieldOffset(4)] public int Mailbox;
    /// <summary>Index of the reassembly in progress, or -1.</summary>
    [FieldOffset(8)] public int Reassembly;
    /// <summary>Flags.</summary>
    [FieldOffset(12)] public KeyRecvFlags Flags;
    /// <summary>Clock micros of the last accepted value.</summary>
    [FieldOffset(16)] public long LastUpdateMicros;
    /// <summary>The key (reverse lookup).</summary>
    [FieldOffset(24)] public ulong Key;
    /// <summary>Version whose ack is queued (valid with <see cref="KeyRecvFlags.AckPending"/>).</summary>
    [FieldOffset(32)] public uint PendingAckVersion;
    /// <summary>Values accepted for the key (statistics).</summary>
    [FieldOffset(36)] public uint Updates;
}
