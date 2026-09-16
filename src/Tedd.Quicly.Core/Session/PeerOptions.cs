using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.Time;

namespace Tedd.Quicly.Core.Session;

/// <summary>
/// Configuration of a <see cref="QuiclyPeer"/>. Read once when the peer is created (later changes to the same
/// instance do not affect existing peers), so one instance may serve many peers. Defaults follow ARCHITECTURE.md §9
/// and PROTOCOL.md §4 and §7.
/// </summary>
public sealed class PeerOptions
{
    /// <summary>Clock for every timestamp, deadline and time-driven task. Share it with the transport in tests (<see cref="VirtualClock"/>).</summary>
    public IClock Clock { get; set; } = MonotonicClock.Instance;

    /// <summary>
    /// Shared buffer pool for payload leases (not owned: the peer never disposes it). When <see langword="null"/> the peer
    /// creates a private allocator from <see cref="AllocatorOptions"/> and disposes it with the peer.
    /// </summary>
    public SlabAllocator? Allocator { get; set; }

    /// <summary>
    /// Sizing of the private allocator used when <see cref="Allocator"/> is <see langword="null"/>; <see langword="null"/>
    /// selects a compact per-peer set of size classes (about 1.5 MiB).
    /// </summary>
    public SlabAllocatorOptions? AllocatorOptions { get; set; }

    /// <summary>Payload bytes this peer may hold in send leases at once (blocks in flight). Default 256 KiB.</summary>
    public int SendBudgetBytes { get; set; } = 256 * 1024;

    /// <summary>Payload bytes this peer may hold in receive leases at once (ring, mailboxes, staging, reassembly). Default 256 KiB.</summary>
    public int ReceiveBudgetBytes { get; set; } = 256 * 1024;

    /// <summary>Send entries (queued and in-flight sends, tracked or not). Rounded up to a power of two. Default 1 024.</summary>
    public int SendTableCapacity { get; set; } = 1024;

    /// <summary>Complete received messages that may wait for <see cref="QuiclyPeer.Poll"/>. Default 4 096 (PROTOCOL.md §7).</summary>
    public int ReceiveRingCapacity { get; set; } = 4096;

    /// <summary>Segments of the arena stream gathers take their per-submission segment arrays from. Default 1 024.</summary>
    public int SegmentArenaCapacity { get; set; } = 1024;

    /// <summary>Where tracked-send completions are delivered. Default <see cref="Session.CompletionMode.PollOnly"/>.</summary>
    public CompletionMode CompletionMode { get; set; } = CompletionMode.PollOnly;

    /// <summary>
    /// Allow <c>SendCopy</c>/<c>SendOwned</c>/<c>SendPinned</c>/<c>SendBorrowed</c>/<c>SendGather</c>/<c>SendAsync</c> and
    /// <c>RentBuffer</c>/<c>ReturnBuffer</c> from threads other than the game thread (ARCHITECTURE.md §3). The game thread is
    /// the thread that last entered <c>Poll</c> or <c>Flush</c> (before the first call: the thread that created the peer);
    /// its sends are admitted directly. A send from any other thread copies the payload into a send lease (an owned lease
    /// moves as it is), queues a 64-byte request in a lock-free multi-producer ring of <see cref="SendTableCapacity"/> slots
    /// and answers <c>Admitted</c> without a token (<c>QueueFull</c> when the ring is full; tracked sends answer
    /// <c>NotSupported</c> because tokens belong to the game thread). The game thread admits the requests, oldest first, at
    /// the start of its next <c>Poll</c> or <c>Flush</c>; <c>Immediate</c> from another thread means "at the next Poll or
    /// Flush". Requests of one thread keep their order; there is no order between threads. The send budget is then kept with
    /// atomic operations. Off by default: all other members stay game-thread only.
    /// </summary>
    public bool ThreadSafeSend { get; set; }

    /// <summary>Longest delay before a coalesced LatestAck/LatestReject datagram is sent. Default 5 ms.</summary>
    public TimeSpan AckDelay { get; set; } = TimeSpan.FromMilliseconds(5);

    /// <summary>Ping period after the initial fast lock (PROTOCOL.md §4.6). Default 1 s.</summary>
    public TimeSpan PingInterval { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Ping period during the fast lock at the start of a session. Default 100 ms.</summary>
    public TimeSpan FastPingInterval { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>Length of the fast lock after the session is admitted. Default 3 s.</summary>
    public TimeSpan FastLockDuration { get; set; } = TimeSpan.FromSeconds(3);

    /// <summary>
    /// Server: the Hello must arrive and admission must complete within this time of the transport connecting, or the
    /// connection is closed with <see cref="QuiclyErrorCode.Timeout"/>. Client: the HelloAck must arrive within it. Default 5 s.
    /// </summary>
    public TimeSpan AdmissionTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// A connected session with no data received for this long is closed with <see cref="QuiclyErrorCode.Timeout"/>
    /// (pings keep a live session busy). <see cref="TimeSpan.Zero"/> disables the check. Default 10 s.
    /// </summary>
    public TimeSpan HeartbeatTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// A stream of the peer that stops making progress in the middle of a message for this long is reset with
    /// <see cref="QuiclyErrorCode.Timeout"/> (PROTOCOL.md §7 "stream idle mid-message"), which releases the staging
    /// lease and the receive-ring reservation the half-received message holds; the connection survives.
    /// <see cref="TimeSpan.Zero"/> disables the check. Default 30 s.
    /// </summary>
    public TimeSpan StreamIdleTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// After Close is sent, how long the peer waits for the control stream to deliver it before closing the transport
    /// anyway. Default 1 s.
    /// </summary>
    public TimeSpan CloseLinger { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Server: how long a session (and its token) survives a lost connection; sent as <c>HelloAck.graceMicros</c>. Default 30 s.</summary>
    public TimeSpan SessionGrace { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>The host's expected flush period, used to resolve <see cref="ChannelDefinition.ExpiryTwiceFlushInterval"/>. Default 1/60 s.</summary>
    public TimeSpan FlushInterval { get; set; } = TimeSpan.FromTicks(166_667);

    /// <summary>
    /// Send cap in bytes per second (a token bucket, PROTOCOL.md §4.5): the scheduler hands application datagrams (and, with the
    /// ordered-stream engine, stream data) to the transport only while the bucket is positive; its burst is two flush intervals'
    /// worth, and a pass held back by it lowers <see cref="QuiclyPeer.NextDeadline"/> to the refill time. Control traffic (pings,
    /// the handshake, close) is not capped. 0, or 2 000 000 000 and more, means no cap.
    /// </summary>
    public long MaxSendBytesPerSecond { get; set; }

    /// <summary>Bulk traffic's share of the estimated bandwidth (PROTOCOL.md §4.5). Reserved for the bulk engine. Default 0.5.</summary>
    public double BulkShareOfEstimatedBandwidth { get; set; } = 0.5;

    /// <summary>Absolute cap on bulk bytes per second; 0 = only the share applies. Reserved for the bulk engine.</summary>
    public long BulkMaxBytesPerSecond { get; set; }

    /// <summary>
    /// ReliableLatest retransmissions' share of the estimated bandwidth (PROTOCOL.md §4.4: "an aggregate per-peer retry
    /// budget, default 10 % of the estimated bandwidth"). The estimate is the transport's congestion window divided by its
    /// RTT, or <see cref="MaxSendBytesPerSecond"/> when the transport reports none. Default 0.1.
    /// </summary>
    public double RetryShareOfEstimatedBandwidth { get; set; } = 0.1;

    /// <summary>
    /// Absolute cap on ReliableLatest retransmission bytes per second; 0 (the default) derives the cap from
    /// <see cref="RetryShareOfEstimatedBandwidth"/>. Retries held back by it wait for the next pass.
    /// </summary>
    public long MaxRetryBytesPerSecond { get; set; }

    /// <summary>
    /// Timer-driven flush period for hosts without a tick (PROTOCOL.md §4.5). Only <see cref="TimeSpan.Zero"/> (off, the
    /// default) is supported by the peer itself; client and server hosts implement the timer.
    /// </summary>
    public TimeSpan AutoFlushInterval { get; set; } = TimeSpan.Zero;

    /// <summary>
    /// Server: session-wide message size cap sent as <c>HelloAck.maxMessageSize</c>; the effective limit of a channel is
    /// <c>min(channel.MaxMessageSize, cap)</c> (Bulk transfers excepted). Default 1 MiB.
    /// </summary>
    public int MaxMessageSize { get; set; } = ChannelDefinition.ReliableMaxMessageSize;

    /// <summary>Largest datagram payload this end will process, announced in Hello/HelloAck (0 = no cap beyond the transport's).</summary>
    public ushort MaxReceiveDatagram { get; set; }

    /// <summary>Control messages (datagram and stream) accepted per second before the connection is closed with <see cref="QuiclyErrorCode.LimitExceeded"/>. Default 200.</summary>
    public int ControlMessagesPerSecond { get; set; } = 200;

    /// <summary>Sustained Pong rate (PROTOCOL.md §2.3); excess Pings are ignored and counted. Default 4 per second.</summary>
    public int PongsPerSecond { get; set; } = 4;

    /// <summary>Pong burst allowance on top of <see cref="PongsPerSecond"/> (covers the peer's 10 Hz fast lock). Default 32.</summary>
    public int PongBurst { get; set; } = 32;

    /// <summary>Decompressed bytes per second this peer will produce (PROTOCOL.md §7); further compressed messages are dropped and counted. Default 8 MiB.</summary>
    public int DecodedBytesPerSecond { get; set; } = 8 * 1024 * 1024;

    /// <summary>Client: ask the server to include its channel table (with names) in the HelloAck.</summary>
    public bool RequestChannelTable { get; set; }

    /// <summary>Client: the session token of a previous connection, to resume that session (empty = fresh session).</summary>
    public ReadOnlyMemory<byte> SessionToken { get; set; }

    /// <summary>Client: the epoch of the previous connection (informational, sent in Hello).</summary>
    public uint LastEpoch { get; set; }

    /// <summary>
    /// When a transport callback throws (a library bug), terminate the process instead of recording the fault and closing
    /// the connection with <see cref="QuiclyErrorCode.InternalError"/>. Default off.
    /// </summary>
    public bool FailFastOnCallbackException { get; set; }

    /// <summary>Test hook: overrides the engine created for a delivery mode (return <see langword="null"/> to keep the default).</summary>
    internal Func<ChannelMode, ChannelEngine?>? EngineFactory { get; set; }

    /// <summary>Checks every value.</summary>
    /// <exception cref="ArgumentException">A value is out of range.</exception>
    /// <exception cref="NotSupportedException"><see cref="AutoFlushInterval"/> is not zero.</exception>
    internal void Validate()
    {
        if (Clock is null)
        {
            throw new ArgumentException("A clock is required.", nameof(Clock));
        }

        CheckRange(SendBudgetBytes, 1, int.MaxValue, nameof(SendBudgetBytes));
        CheckRange(ReceiveBudgetBytes, 1, int.MaxValue, nameof(ReceiveBudgetBytes));
        CheckRange(SendTableCapacity, 16, 1 << 20, nameof(SendTableCapacity));
        CheckRange(ReceiveRingCapacity, 2, 1 << 20, nameof(ReceiveRingCapacity));
        CheckRange(SegmentArenaCapacity, 8, 1 << 20, nameof(SegmentArenaCapacity));
        CheckRange(ControlMessagesPerSecond, 1, 1_000_000, nameof(ControlMessagesPerSecond));
        CheckRange(PongsPerSecond, 1, 1_000_000, nameof(PongsPerSecond));
        CheckRange(PongBurst, 1, 1_000_000, nameof(PongBurst));
        CheckRange(DecodedBytesPerSecond, 1, int.MaxValue, nameof(DecodedBytesPerSecond));
        CheckRange(MaxMessageSize, 1, ChannelDefinition.BulkMaxMessageSize, nameof(MaxMessageSize));
        CheckPositive(PingInterval, nameof(PingInterval));
        CheckPositive(FastPingInterval, nameof(FastPingInterval));
        CheckPositive(AdmissionTimeout, nameof(AdmissionTimeout));
        CheckPositive(FlushInterval, nameof(FlushInterval));
        CheckNonNegative(AckDelay, nameof(AckDelay));
        CheckNonNegative(FastLockDuration, nameof(FastLockDuration));
        CheckNonNegative(HeartbeatTimeout, nameof(HeartbeatTimeout));
        CheckNonNegative(StreamIdleTimeout, nameof(StreamIdleTimeout));
        CheckNonNegative(CloseLinger, nameof(CloseLinger));
        CheckNonNegative(SessionGrace, nameof(SessionGrace));
        if (MaxSendBytesPerSecond < 0 || BulkMaxBytesPerSecond < 0 || MaxRetryBytesPerSecond < 0)
        {
            throw new ArgumentException("Bandwidth caps must not be negative.", nameof(MaxSendBytesPerSecond));
        }

        if (!(BulkShareOfEstimatedBandwidth > 0 && BulkShareOfEstimatedBandwidth <= 1))
        {
            throw new ArgumentException("BulkShareOfEstimatedBandwidth must be in (0, 1].", nameof(BulkShareOfEstimatedBandwidth));
        }

        if (!(RetryShareOfEstimatedBandwidth > 0 && RetryShareOfEstimatedBandwidth <= 1))
        {
            throw new ArgumentException("RetryShareOfEstimatedBandwidth must be in (0, 1].", nameof(RetryShareOfEstimatedBandwidth));
        }

        if (SessionToken.Length > ControlCodec.MaxTokenLength)
        {
            throw new ArgumentException($"A session token is at most {ControlCodec.MaxTokenLength} bytes.", nameof(SessionToken));
        }

        if (AutoFlushInterval != TimeSpan.Zero)
        {
            throw new NotSupportedException("AutoFlushInterval is implemented by the client and server hosts; a bare peer requires 0.");
        }
    }

    internal static long ToMicros(TimeSpan value) => value.Ticks / (TimeSpan.TicksPerMillisecond / 1000);

    private static void CheckRange(int value, int min, int max, string name)
    {
        if (value < min || value > max)
        {
            throw new ArgumentException($"{name} must be in [{min}, {max}] (got {value}).", name);
        }
    }

    private static void CheckPositive(TimeSpan value, string name)
    {
        if (value <= TimeSpan.Zero)
        {
            throw new ArgumentException($"{name} must be positive.", name);
        }
    }

    private static void CheckNonNegative(TimeSpan value, string name)
    {
        if (value < TimeSpan.Zero)
        {
            throw new ArgumentException($"{name} must not be negative.", name);
        }
    }
}
