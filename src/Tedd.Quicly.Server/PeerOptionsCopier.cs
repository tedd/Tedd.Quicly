using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Server;

/// <summary>
/// Copies a <see cref="PeerOptions"/> template (Core offers no clone). The server derives its peers' options from the
/// application's template without modifying it.
/// </summary>
internal static class PeerOptionsCopier
{
    /// <summary>The public properties <see cref="Copy"/> copies; a test compares this list with the public settable properties of PeerOptions.</summary>
    internal static readonly string[] CopiedProperties =
    [
        nameof(PeerOptions.Clock), nameof(PeerOptions.Allocator), nameof(PeerOptions.AllocatorOptions), nameof(PeerOptions.SendBudgetBytes),
        nameof(PeerOptions.ReceiveBudgetBytes), nameof(PeerOptions.SendTableCapacity), nameof(PeerOptions.ReceiveRingCapacity),
        nameof(PeerOptions.SegmentArenaCapacity), nameof(PeerOptions.CompletionMode), nameof(PeerOptions.ThreadSafeSend), nameof(PeerOptions.AckDelay),
        nameof(PeerOptions.PingInterval), nameof(PeerOptions.FastPingInterval), nameof(PeerOptions.FastLockDuration), nameof(PeerOptions.AdmissionTimeout),
        nameof(PeerOptions.HeartbeatTimeout), nameof(PeerOptions.CloseLinger), nameof(PeerOptions.SessionGrace), nameof(PeerOptions.FlushInterval),
        nameof(PeerOptions.MaxSendBytesPerSecond), nameof(PeerOptions.BulkShareOfEstimatedBandwidth), nameof(PeerOptions.BulkMaxBytesPerSecond),
        nameof(PeerOptions.AutoFlushInterval), nameof(PeerOptions.MaxMessageSize), nameof(PeerOptions.MaxReceiveDatagram),
        nameof(PeerOptions.ControlMessagesPerSecond), nameof(PeerOptions.PongsPerSecond), nameof(PeerOptions.PongBurst),
        nameof(PeerOptions.DecodedBytesPerSecond), nameof(PeerOptions.RequestChannelTable), nameof(PeerOptions.SessionToken), nameof(PeerOptions.LastEpoch),
        nameof(PeerOptions.FailFastOnCallbackException),
    ];

    /// <summary>Returns a new instance with every public value of <paramref name="source"/>.</summary>
    public static PeerOptions Copy(PeerOptions source) => new()
    {
        Clock = source.Clock,
        Allocator = source.Allocator,
        AllocatorOptions = source.AllocatorOptions,
        SendBudgetBytes = source.SendBudgetBytes,
        ReceiveBudgetBytes = source.ReceiveBudgetBytes,
        SendTableCapacity = source.SendTableCapacity,
        ReceiveRingCapacity = source.ReceiveRingCapacity,
        SegmentArenaCapacity = source.SegmentArenaCapacity,
        CompletionMode = source.CompletionMode,
        ThreadSafeSend = source.ThreadSafeSend,
        AckDelay = source.AckDelay,
        PingInterval = source.PingInterval,
        FastPingInterval = source.FastPingInterval,
        FastLockDuration = source.FastLockDuration,
        AdmissionTimeout = source.AdmissionTimeout,
        HeartbeatTimeout = source.HeartbeatTimeout,
        CloseLinger = source.CloseLinger,
        SessionGrace = source.SessionGrace,
        FlushInterval = source.FlushInterval,
        MaxSendBytesPerSecond = source.MaxSendBytesPerSecond,
        BulkShareOfEstimatedBandwidth = source.BulkShareOfEstimatedBandwidth,
        BulkMaxBytesPerSecond = source.BulkMaxBytesPerSecond,
        AutoFlushInterval = source.AutoFlushInterval,
        MaxMessageSize = source.MaxMessageSize,
        MaxReceiveDatagram = source.MaxReceiveDatagram,
        ControlMessagesPerSecond = source.ControlMessagesPerSecond,
        PongsPerSecond = source.PongsPerSecond,
        PongBurst = source.PongBurst,
        DecodedBytesPerSecond = source.DecodedBytesPerSecond,
        RequestChannelTable = source.RequestChannelTable,
        SessionToken = source.SessionToken,
        LastEpoch = source.LastEpoch,
        FailFastOnCallbackException = source.FailFastOnCallbackException,
    };
}
