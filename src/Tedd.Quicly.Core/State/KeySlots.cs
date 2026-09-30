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
    /// <summary>
    /// Reserved; no engine sets it. Fragments are not reassembled per key: the datagram engine keeps its own reassembly
    /// table, scoped by <c>(channel, key, sequence)</c> (PROTOCOL.md §2.1; docs/design/session-layer.md §7.8).
    /// </summary>
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

    /// <summary>Version of the most recent value sent (or queued) for the key. Owner: game thread.</summary>
    [FieldOffset(0)] public uint CurrentVersion;
    /// <summary>
    /// The last version the peer acknowledged that was, when the ack arrived, the version most recently transmitted for the
    /// key. Diagnostic: it decides nothing. Owner: game thread.
    /// </summary>
    [FieldOffset(4)] public uint AckedVersion;
    /// <summary>Lease holding the current value (kept for retries until acked). Owner: game thread.</summary>
    [FieldOffset(8)] public BufferLease CurrentLease;
    /// <summary>Clock micros at which the current value is retransmitted unless acked (valid with <see cref="KeySendFlags.RetryArmed"/>). Owner: game thread.</summary>
    [FieldOffset(24)] public long RetryDeadline;
    /// <summary>The <see cref="SendEntry"/> slot carrying the current value, or -1. Owner: game thread.</summary>
    [FieldOffset(32)] public int InFlightEntry;
    /// <summary>Stream used for a large value, when <see cref="KeySendFlags.LargeValue"/>. Owner: game thread.</summary>
    [FieldOffset(36)] public TransportStreamId LargeValueStream;
    /// <summary>Transmissions of the current version so far (retry budget). Owner: game thread.</summary>
    [FieldOffset(44)] public byte Attempts;
    /// <summary>Flags. Owner: game thread.</summary>
    [FieldOffset(45)] public KeySendFlags Flags;
    /// <summary>The key (reverse lookup for hashed key spaces). Owner: game thread.</summary>
    [FieldOffset(48)] public ulong Key;
    /// <summary>Clock micros of the last transmission. Owner: game thread.</summary>
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
/// <see cref="PendingAckVersion"/> (32), <see cref="Updates"/> (36), <see cref="LastAcceptedExtended"/> (40),
/// reserved (48 … 63).
/// </remarks>
[StructLayout(LayoutKind.Explicit, Size = Size)]
public struct KeyRecvSlot
{
    /// <summary>Size in bytes.</summary>
    public const int Size = 64;

    /// <summary>
    /// Last accepted sequence/version for the key as it appeared on the wire: what a latest-ack and a KeyRetired notice
    /// name. Staleness is decided on <see cref="LastAcceptedExtended"/>. Owner: transport thread.
    /// </summary>
    [FieldOffset(0)] public uint LastAccepted;
    /// <summary>Index of the key's mailbox in the channel's <see cref="Mailboxes"/> (normally the slot itself), or -1. Owner: transport thread.</summary>
    [FieldOffset(4)] public int Mailbox;
    /// <summary>
    /// Reserved; no engine uses it. Fragment reassembly is not per key: the datagram engine keeps its own table, scoped by
    /// <c>(channel, key, sequence)</c> (PROTOCOL.md §2.1; docs/design/session-layer.md §7.8). Owner: transport thread.
    /// </summary>
    [FieldOffset(8)] public int Reassembly;
    /// <summary>Flags. Owner: transport thread.</summary>
    [FieldOffset(12)] public KeyRecvFlags Flags;
    /// <summary>Clock micros of the last accepted value. Owner: transport thread.</summary>
    [FieldOffset(16)] public long LastUpdateMicros;
    /// <summary>The key (reverse lookup). Owner: transport thread.</summary>
    [FieldOffset(24)] public ulong Key;
    /// <summary>Version whose ack is queued (valid with <see cref="KeyRecvFlags.AckPending"/>). Owner: transport thread.</summary>
    [FieldOffset(32)] public uint PendingAckVersion;
    /// <summary>Values accepted for the key (statistics). Owner: transport thread.</summary>
    [FieldOffset(36)] public uint Updates;
    /// <summary>
    /// <see cref="LastAccepted"/> extended to 64 bits on the channel's version clock (valid with
    /// <see cref="KeyRecvFlags.HasAccepted"/>; PROTOCOL.md §8): a value is newer than the key's last one exactly when its
    /// extended version is greater, however long the key idled. Owner: transport thread.
    /// </summary>
    [FieldOffset(40)] public ulong LastAcceptedExtended;
}
