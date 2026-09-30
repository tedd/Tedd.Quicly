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

    // ---- region added by wave C2b (ReliableUnordered group streams)

    /// <summary>
    /// Shortest time between two group streams of one <see cref="ChannelMode.ReliableUnordered"/> channel
    /// (PROTOCOL.md §3.2 <c>GroupMinIntervalMicros</c>): while the interval has not passed the channel's open group keeps
    /// collecting messages instead of opening another stream, which bounds stream churn. A message sent with
    /// <see cref="SendMode.Immediate"/> opens its group's stream at once. <see cref="TimeSpan.Zero"/> disables the bound.
    /// Default 1 ms.
    /// </summary>
    public TimeSpan GroupMinInterval { get; set; } = TimeSpan.FromMilliseconds(1);

    // ---- end of the wave C2b region

    // ---- region added by wave C2c (Bulk transfers)

    /// <summary>
    /// Where the bytes of a bulk transfer the peer opened are written (PROTOCOL.md §3.3, ARCHITECTURE.md §4.2 "Direct
    /// mode"). <see langword="null"/> (the default) refuses every peer-initiated transfer, which is what PROTOCOL.md §3.3
    /// requires of the receive router, and it also refuses the answer to a range this end asked for — an application that
    /// calls <see cref="QuiclyPeer.RequestBulk"/> supplies a router that recognises what it requested.
    /// </summary>
    public IBulkRouter? BulkRouter { get; set; }

    /// <summary>
    /// Decides whether the peer may have a range it asked for with <c>BulkRequest</c> (PROTOCOL.md §3.4).
    /// <see langword="null"/> (the default) refuses every request with <c>BulkReject</c>: serving bulk objects is opt-in.
    /// </summary>
    public IBulkAuthorizer? BulkAuthorizer { get; set; }

    /// <summary>Supplies the object an authorised <c>BulkRequest</c> asked for; <see langword="null"/> refuses every request.</summary>
    public IBulkProvider? BulkProvider { get; set; }

    /// <summary>
    /// Concurrent bulk transfers per direction per peer (PROTOCOL.md §7); further peer streams are reset
    /// <see cref="QuiclyErrorCode.LimitExceeded"/> and further requests answered <c>BulkReject</c>. Default 2.
    /// </summary>
    public int BulkTransfersPerDirection { get; set; } = 2;

    /// <summary>
    /// Bulk bytes one transfer keeps outstanding (submitted and not yet completed) when the transport reports no
    /// <c>IdealSendBufferSize</c> for its stream (<see cref="Transport.TransportCapabilities.IdealSendBufferSize"/>);
    /// when it does, that value is the window instead. Default 256 KiB.
    /// </summary>
    public int BulkSendWindowBytes { get; set; } = 256 * 1024;

    /// <summary>
    /// Bulk's share of the transport's congestion window (ARCHITECTURE.md §7): the send window is additionally capped to
    /// this fraction of the window, because datagrams and streams share one congestion window and stream priority alone
    /// cannot protect real-time latency. Default 0.5.
    /// </summary>
    public double BulkShareOfCongestionWindow { get; set; } = 0.5;

    /// <summary>
    /// Bytes of object payload one bulk stream send carries (one pooled block; also the size of a compressed chunk's
    /// input). Default 64 KiB, which fits the default pool's largest size classes on both ends.
    /// </summary>
    public int BulkChunkBytes { get; set; } = 64 * 1024;

    /// <summary>
    /// Largest chunk this end sends and stages on receive (PROTOCOL.md §3.3 <c>BulkMaxChunk</c>, at most 1 MiB). The
    /// receive side stages a <em>compressed</em> chunk in a pooled lease and decodes it into a second one, so the
    /// effective receive limit is <c>min(this, the pool's largest block)</c> and a larger chunk resets its stream with
    /// <see cref="QuiclyErrorCode.LimitExceeded"/>. Default 1 MiB (the protocol maximum); the sender is bounded by
    /// <see cref="BulkChunkBytes"/>.
    /// </summary>
    public int BulkMaxChunk { get; set; } = Framing.StreamFraming.DefaultBulkMaxChunk;

    // ---- end of the wave C2c region

    // ---- region added by the bulk object driver (objects larger than one transfer)

    /// <summary>
    /// Where a bulk <em>object</em> the peer sends is assembled — one call per object rather than one per transfer, so an
    /// application receiving a 10 GB file sees one sink and one completion (<see cref="IBulkObjectRouter"/>).
    /// <see langword="null"/> (the default) leaves <see cref="BulkRouter"/> in charge, which is the raw per-transfer view.
    /// Setting both is an error: the object driver <em>is</em> the router when it is present.
    /// </summary>
    public IBulkObjectRouter? BulkObjectRouter { get; set; }

    /// <summary>Bulk objects this end may send, and assemble on receive, at once. Default 4.</summary>
    public int BulkObjectsPerDirection { get; set; } = 4;

    /// <summary>
    /// Ranges of one object in flight at once. 0 (the default) follows <see cref="BulkTransfersPerDirection"/>, which is
    /// also the ceiling: the engine's own slots are what a range has to fit into, and they are shared with the transfers
    /// the peer asked for.
    /// </summary>
    public int BulkObjectRangesInFlight { get; set; }

    /// <summary>
    /// Largest range one transfer of an object carries, capped by the Bulk channel's own <c>MaxMessageSize</c>; 0 uses
    /// that cap, which is the protocol's ceiling for one transfer (PROTOCOL.md §8). Default 1 MiB.
    /// </summary>
    /// <remarks>
    /// The ceiling is not the good default it looks like. The range is the unit of <em>detection and recovery</em>: a
    /// range that fails its checksum is re-read and sent again on its own, so the range size is how much work one bad
    /// megabyte costs. A range also costs only one stream and about forty header bytes, so at 1 MiB the wire overhead is
    /// four thousandths of a percent — there is nothing to buy back by making it 16 times larger, and a great deal to
    /// lose when something has to be re-sent.
    /// </remarks>
    public int BulkObjectRangeBytes { get; set; } = 1024 * 1024;

    /// <summary>
    /// How many of an object's ranges may be sent again after failing their checksum before the object is given up on.
    /// Default 3; 0 disables retrying.
    /// </summary>
    /// <remarks>
    /// The bound is per object rather than per range, and deliberately small. A checksum failure is not packet loss —
    /// QUIC has already retransmitted and its AEAD has already thrown out anything the wire corrupted — so it means a bug,
    /// a bad memory module, or a source that is changing underneath the transfer. None of those get better with retries,
    /// and a 10 GB object that hits three of them should fail loudly rather than grind.
    /// </remarks>
    public int BulkObjectRangeRetries { get; set; } = 3;

    /// <summary>
    /// Whether bulk transfers append a checksum trailer by default (PROTOCOL.md §3.3). Default <see langword="true"/>;
    /// <see cref="BulkDescriptor.Checksum"/> and <see cref="BulkObjectDescriptor.Checksum"/> override it per transfer.
    /// </summary>
    /// <remarks>
    /// <para><b>This is not a wire integrity check and it is not optional in QUIC.</b> TLS 1.3 is mandatory there
    /// (RFC 9001) and its AEAD tag already discards anything the wire corrupts, retransmitting it. What this catches is
    /// what the AEAD structurally cannot: the path <em>inside</em> each endpoint, between the application handing bytes to
    /// <see cref="IBulkSource"/> and getting them back from <see cref="IBulkSink"/>. Framing and reassembly bugs, a wrong
    /// offset, memory corruption before encrypt or after decrypt, a partial object that rotted on disk across a resume.
    /// </para>
    /// <para>So it is defence in depth against bugs and hardware, and worth turning off if you would rather not pay for
    /// it: about 8 wire bytes per transfer and one xxHash64 pass over the data, which runs at better than 10 GB/s — under
    /// 1 % of a core at 1 Gbps, but real at 100. A peer that sends without checksums and one that requires them still
    /// interoperate: the flag is per transfer, and a receiver checks whatever arrives.</para>
    /// </remarks>
    public bool BulkChecksum { get; set; } = true;

    /// <summary>
    /// Bytes between <see cref="BulkObjectProgressCallback"/> reports on both sides; the final state is always reported
    /// through the object's completion or <see cref="IBulkObjectSink.Finish"/>. Default 1 MiB.
    /// </summary>
    public long BulkObjectProgressBytes { get; set; } = 1024 * 1024;

    /// <summary>
    /// How long an incomplete object being assembled may go with no range of its own running before it is given up on
    /// (<see cref="BulkStatus.Failed"/>, <see cref="QuiclyErrorCode.Timeout"/>). Default 30 s, as
    /// <see cref="StreamIdleTimeout"/>; <see cref="TimeSpan.Zero"/> disables it.
    /// </summary>
    /// <remarks>
    /// This is what guarantees a receiving object ends. An object is several transfers, and the protocol has no frame
    /// that says "the object is over" (PROTOCOL.md §3.4: a sender abandoning a transfer signals by resetting <em>its
    /// stream</em>). A sender that stops between two ranges therefore leaves nothing to reset, and its peer would
    /// otherwise wait for bytes that are never coming: a cancel that lands exactly on a range boundary, a sender whose
    /// application abandoned the object, a sender that died. A cancel that lands anywhere else resets the range that was
    /// running and reaches the receiver at once; this is the backstop for the rest.
    /// </remarks>
    public TimeSpan BulkObjectIdleTimeout { get; set; } = TimeSpan.FromSeconds(30);

    // ---- end of the bulk object driver region

    /// <summary>
    /// After Close is sent, how long the peer waits for the control stream to deliver it before closing the transport
    /// anyway. Default 1 s.
    /// </summary>
    public TimeSpan CloseLinger { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Server: how long a session (and its token) survives a lost connection; sent as <c>HelloAck.graceMicros</c>. Default 30 s.</summary>
    public TimeSpan SessionGrace { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The host's expected flush period, used to resolve <see cref="ChannelDefinition.ExpiryTwiceFlushInterval"/>. Default
    /// 1/60 s. A host that flushes more slowly does not lose messages to that default: expiry counts the time a message is
    /// held back from its first scheduler pass on (PROTOCOL.md §4.5), so a message sent in the pass that first sees it
    /// never expires.
    /// </summary>
    public TimeSpan FlushInterval { get; set; } = TimeSpan.FromTicks(166_667);

    /// <summary>
    /// Send cap in bytes per second (a token bucket, PROTOCOL.md §4.5): the scheduler hands application datagrams (and, with the
    /// ordered-stream engine, stream data) to the transport only while the bucket is positive; its burst is two flush intervals'
    /// worth, and a pass held back by it lowers <see cref="QuiclyPeer.NextDeadline"/> to the refill time. Control traffic (pings,
    /// the handshake, close) is not capped. 0, or 2 000 000 000 and more, means no cap.
    /// </summary>
    public long MaxSendBytesPerSecond { get; set; }

    /// <summary>
    /// Whether unreliable datagrams are sent with the transport's cancel-on-blocked flag (PROTOCOL.md §4.5), so a datagram
    /// the transport cannot send at once is dropped instead of queueing behind congestion, where it would go out late.
    /// Default <see langword="true"/>. It takes effect only on a transport that honours the flag
    /// (<see cref="Transport.TransportCapabilities.CancelOnBlocked"/>); ReliableLatest datagrams and control datagrams never
    /// carry it, and a packed container carries it only when every member is unreliable.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A dropped datagram's messages complete <see cref="Threading.DeliveryStatus.Expired"/> and are counted in
    /// <see cref="ChannelStatistics.TransportCanceled"/> and <see cref="PeerStatistics.DatagramsCanceled"/>. When those
    /// counters explain messages that go missing — typically bursts after a stall, on any link including loopback — set this
    /// to <see langword="false"/>: blocked datagrams then wait in the transport's queue and are sent when it can send again.
    /// </para>
    /// <para>
    /// The price of <see langword="false"/> is staleness: a channel's expiry is evaluated only while the scheduler holds a
    /// message, so a datagram that waits inside the transport is sent however old it has become, and a sustained overload
    /// grows the transport's queue instead of shedding load. Read once, when the peer is created.
    /// </para>
    /// </remarks>
    public bool DropWhenBlocked { get; set; } = true;

    /// <summary>
    /// Bulk traffic's share of the estimated bandwidth (PROTOCOL.md §4.5): the rate at which the bulk engine hands
    /// stream bytes to the transport, so real-time traffic keeps flowing. The estimate is the transport's congestion
    /// window divided by its RTT, or <see cref="MaxSendBytesPerSecond"/> when the transport reports no window. A derived
    /// rate is floored at 16 KiB/s, and with neither a window nor a send cap the floor is the rate: an unmeasured link is
    /// not assumed to be a fast one. A window reported with an RTT under a microsecond (a loopback or in-memory link) has no
    /// bound, and bulk is then limited by its send window alone. Default 0.5.
    /// </summary>
    public double BulkShareOfEstimatedBandwidth { get; set; } = 0.5;

    /// <summary>Absolute cap on bulk bytes per second; 0 (the default) derives the cap from <see cref="BulkShareOfEstimatedBandwidth"/>.</summary>
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

    /// <summary>
    /// Control messages (datagram and stream) accepted per second before the connection is closed with
    /// <see cref="QuiclyErrorCode.LimitExceeded"/>. Default 2 000 (PROTOCOL.md §7). Size it from the channel table: the
    /// coalesced LatestAck traffic of keyed <see cref="Channels.ChannelMode.ReliableLatest"/> channels dominates it, and one
    /// ack datagram carries about 170 keys, so a channel of <i>N</i> keys updated at <i>F</i> Hz makes the peer receive
    /// roughly <c>N·F / 170</c> control messages per second (about 360/s for 1 000 keys at 60 Hz).
    /// </summary>
    public int ControlMessagesPerSecond { get; set; } = 2_000;

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

    /// <summary>
    /// Told once, without blocking, whenever the peer publishes game-thread work — received messages, completions, control
    /// frames and the handshake from the transport thread, and work an application call created
    /// (<see cref="QuiclyPeer.Close(CloseReason)"/>, <see cref="QuiclyPeer.CompleteAdmission"/>, a send from another
    /// thread) — so a host can wake a sleeping game thread instead of polling idle peers. Set-once until the next
    /// <see cref="QuiclyPeer.Poll"/>; <see cref="QuiclyPeer.HasPendingWork"/> says whether anything is really waiting.
    /// <see langword="null"/> (the default) means the host polls on its own schedule.
    /// </summary>
    public IPeerWorkSignal? WorkSignal { get; set; }

    /// <summary>Test hook: overrides the engine created for a delivery mode (return <see langword="null"/> to keep the default).</summary>
    internal Func<ChannelMode, ChannelEngine?>? EngineFactory { get; set; }

    /// <summary>
    /// An independent copy of these options, so a host can hand every peer its own instance instead of sharing one (and
    /// without copying property by property). Shallow: the copy shares the <see cref="Clock"/>, <see cref="Allocator"/>,
    /// <see cref="AllocatorOptions"/> and <see cref="WorkSignal"/> instances and the memory behind
    /// <see cref="SessionToken"/>, which is what a host wants — one pool and one clock serve many peers.
    /// </summary>
    /// <returns>The copy.</returns>
    public PeerOptions Clone() => (PeerOptions)MemberwiseClone();

    /// <summary>
    /// Checks every value, exactly as <see cref="QuiclyPeer.Connect"/> and <see cref="QuiclyPeer.CreateServerPeer"/> do, so
    /// a host can reject a bad configuration at start-up instead of when its first connection arrives.
    /// </summary>
    /// <exception cref="ArgumentException">A value is out of range.</exception>
    /// <exception cref="NotSupportedException"><see cref="AutoFlushInterval"/> is not zero.</exception>
    public void Validate()
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
        CheckNonNegative(GroupMinInterval, nameof(GroupMinInterval));
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

        if (!(BulkShareOfCongestionWindow > 0 && BulkShareOfCongestionWindow <= 1))
        {
            throw new ArgumentException("BulkShareOfCongestionWindow must be in (0, 1].", nameof(BulkShareOfCongestionWindow));
        }

        CheckRange(BulkTransfersPerDirection, 1, 1024, nameof(BulkTransfersPerDirection));
        CheckRange(BulkSendWindowBytes, 1, int.MaxValue, nameof(BulkSendWindowBytes));
        CheckRange(BulkChunkBytes, 1, Framing.StreamFraming.DefaultBulkMaxChunk, nameof(BulkChunkBytes));
        CheckRange(BulkMaxChunk, 1, Framing.StreamFraming.DefaultBulkMaxChunk, nameof(BulkMaxChunk));
        CheckNonNegative(BulkObjectIdleTimeout, nameof(BulkObjectIdleTimeout));
        CheckRange(BulkObjectsPerDirection, 1, 1024, nameof(BulkObjectsPerDirection));
        CheckRange(BulkObjectRangesInFlight, 0, 1024, nameof(BulkObjectRangesInFlight));
        CheckRange(BulkObjectRangeBytes, 0, ChannelDefinition.BulkMaxMessageSize, nameof(BulkObjectRangeBytes));
        CheckRange(BulkObjectRangeRetries, 0, 1024, nameof(BulkObjectRangeRetries));
        if (BulkObjectProgressBytes < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(BulkObjectProgressBytes), BulkObjectProgressBytes, "BulkObjectProgressBytes must be at least 1.");
        }

        if (BulkObjectRouter is not null && BulkRouter is not null)
        {
            // The object driver is installed *as* the engine's router, so there is no place for a second one; an
            // application that wants the raw per-transfer view routes objects itself.
            throw new ArgumentException("BulkObjectRouter and BulkRouter cannot both be set.", nameof(BulkObjectRouter));
        }

        if (BulkChunkBytes > BulkMaxChunk)
        {
            throw new ArgumentException($"BulkChunkBytes ({BulkChunkBytes}) must not exceed BulkMaxChunk ({BulkMaxChunk}).", nameof(BulkChunkBytes));
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

    /// <summary>
    /// A duration in clock microseconds, truncated towards zero — except that a <em>positive</em> duration always becomes at
    /// least one micro.
    /// </summary>
    /// <remarks>
    /// Every caller reads 0 as "off" or "none" and nothing else: the request timeout of
    /// <see cref="QuiclyPeer.SendRequestAsync"/> ("wait until the response arrives"), <see cref="HeartbeatTimeout"/>,
    /// <see cref="StreamIdleTimeout"/>, <see cref="GroupMinInterval"/> and <see cref="CloseLinger"/> ("disabled"),
    /// <see cref="AckDelay"/> ("acknowledge at once"), and the options <see cref="Validate"/> requires to be positive
    /// (<see cref="PingInterval"/>, <see cref="FastPingInterval"/>, <see cref="AdmissionTimeout"/>,
    /// <see cref="FlushInterval"/>), for which 0 is not a value at all. Plain truncation turned a positive sub-microsecond
    /// duration into that sentinel — a 500 ns request timeout waited forever, a 500 ns heartbeat timeout switched the check
    /// off — so the rounding is here, once, for every caller.
    /// </remarks>
    /// <param name="value">The duration.</param>
    /// <returns>Whole microseconds; 0 only for a duration that is not positive.</returns>
    internal static long ToMicros(TimeSpan value)
    {
        long micros = value.Ticks / (TimeSpan.TicksPerMillisecond / 1000);
        return micros == 0 && value.Ticks > 0 ? 1 : micros;
    }

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
