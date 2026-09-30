using Tedd.Quicly.Core.Threading;

namespace Tedd.Quicly.Core.Session;

/// <summary>Addresses a send: the channel and, for keyed channels, the key.</summary>
/// <param name="Channel">Application channel id (2 … 16383) from the peer's <see cref="Channels.ChannelTable"/>.</param>
/// <param name="Key">Key of the message on keyed channels (at most 2^62 − 1); ignored otherwise.</param>
public readonly record struct SendHeader(ushort Channel, ulong Key = 0);

/// <summary>When an admitted message becomes eligible for transmission (PROTOCOL.md §4.5).</summary>
public enum SendMode : byte
{
    /// <summary>Transmitted by the next <see cref="QuiclyPeer.Flush"/> (or when a container or group fills).</summary>
    Buffered = 0,

    /// <summary>Eligible now, together with whatever is already buffered for the peer; bypasses only the batching delay.</summary>
    Immediate = 1,
}

/// <summary>Per-send options. The default value is a buffered, untracked send with the channel's own expiry.</summary>
public readonly struct SendOptions
{
    /// <summary>Buffered (default) or immediate.</summary>
    public SendMode Mode { get; init; }

    /// <summary>Allocate a <see cref="SendToken"/> so completion stages can be awaited (<see cref="QuiclyPeer.WaitAsync"/>).</summary>
    public bool Track { get; init; }

    /// <summary>Opaque application value carried with the send (reported with its completion).</summary>
    public ulong Context { get; init; }

    /// <summary>
    /// Expiry in microseconds; 0 = the channel's default (PROTOCOL.md §4.5). It is the longest the scheduler may hold the
    /// message back, <em>not</em> its age since this send call: the clock starts at the first scheduler pass after the
    /// send (the next <see cref="QuiclyPeer.Flush"/>, or the pass of an <see cref="SendMode.Immediate"/> send), that pass
    /// never expires the message, and a later pass drops it (<see cref="DeliveryStatus.Expired"/>) when more than this has
    /// elapsed since the first.
    /// </summary>
    /// <remarks>
    /// A message that goes out in its first pass is therefore never expired, however long the host took between the send
    /// and the Flush. Time before that first pass is not counted; to bound a message's age from the send call, track it and
    /// <see cref="QuiclyPeer.TryCancel"/> it. (Earlier builds counted from the send, against the clock of the peer's last
    /// Poll or Flush, and dropped messages that had never waited.)
    /// </remarks>
    public long ExpiryMicros { get; init; }

    /// <summary>Options for an immediate, untracked send.</summary>
    public static SendOptions Immediate => new() { Mode = SendMode.Immediate };

    /// <summary>Options for a buffered, tracked send.</summary>
    public static SendOptions Tracked => new() { Track = true };
}

/// <summary>Outcome of a send call.</summary>
/// <param name="Status">Whether the message was admitted, and why not otherwise.</param>
/// <param name="Token">The completion token of a tracked, admitted send; <see langword="default"/> otherwise.</param>
public readonly record struct SendResult(SendStatus Status, SendToken Token)
{
    /// <summary>True when <see cref="Status"/> is <see cref="SendStatus.Admitted"/>.</summary>
    public bool IsAdmitted => Status == SendStatus.Admitted;

    internal static SendResult Rejected(SendStatus status) => new(status, default);
}

/// <summary>Why a send was or was not admitted. Rejections are counters in the channel's statistics, never exceptions.</summary>
public enum SendStatus : byte
{
    /// <summary>The message was accepted; the library owns the payload (or its copy) until the completion.</summary>
    Admitted = 0,

    /// <summary>The channel queue, the send table or the completion table is full; retry after completions were polled.</summary>
    QueueFull = 1,

    /// <summary>The payload exceeds the channel's effective maximum message size.</summary>
    TooLarge = 2,

    /// <summary>No payload buffer is available (the peer's send budget or the pool is exhausted).</summary>
    OutOfBuffers = 3,

    /// <summary>The channel's stream failed or was closed for the rest of the epoch.</summary>
    ChannelClosed = 4,

    /// <summary>The peer is not <see cref="PeerState.Connected"/>.</summary>
    NotConnected = 5,

    /// <summary>The channel's key table is full.</summary>
    KeyTableFull = 6,

    /// <summary>The channel id is not in the channel table.</summary>
    InvalidChannel = 7,

    /// <summary>The operation is not supported for this channel's mode (or not implemented yet).</summary>
    NotSupported = 8,
}
