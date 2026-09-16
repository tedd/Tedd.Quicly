using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Session;

/// <summary>
/// The engine-facing façade of one peer (docs/design/session-layer.md §7): every table and service the delivery engines
/// share — send entries with their header blocks and cold side tables, the segment arena, send and receive leases with
/// their budgets, the completion ring and completion table, the receive ring (with reservations) and mailboxes, the stream
/// table, the transport, per-peer and per-channel counters, the clock, and close requests. The peer owns it; engines get
/// it in <see cref="ChannelEngine.Initialize"/>.
/// </summary>
/// <remarks>
/// <para><b>Threads.</b> Each member documents its thread. Game thread: entry allocation/fill/submit/complete, send leases,
/// counters of <see cref="SendCounters"/>, the segment arena, mailbox creation (construction time only). Transport thread:
/// receive leases (rent), the receive ring producer side and its reservations, <see cref="RecvCounters"/>, the stream table,
/// <see cref="CurrentSenderTick"/>, <see cref="OnTransportDatagramState"/>/<see cref="OnTransportStreamCompleted"/>.
/// Any thread: <see cref="RequestClose"/>, <see cref="ReturnReceive"/>, read-only accessors.</para>
/// <para><b>Send entry protocol</b> (ADR 0008 invariants 1-3): <see cref="TryAllocateEntry"/> → fill (header block via
/// <see cref="SendEntryTable.GetHeaderBlock"/>/<see cref="SendEntryTable.SetHeaderLength"/>, payload via
/// <see cref="AttachLease"/>/<see cref="SetPayload"/>, optional <see cref="TryTrack"/>) → <see cref="SubmitDatagram"/> or
/// <see cref="SubmitStream"/> (publish, then call the transport; on failure the entry is back in <c>Filling</c> and still
/// owned by the caller) → the completion arrives through the completion ring in <see cref="ChannelEngine.OnSendCompleted"/>
/// → <see cref="CompleteEntry"/> (token stages, lease/pin release, slot free). An entry that is never submitted is
/// unwound with <see cref="DiscardEntry"/>.</para>
/// </remarks>
internal sealed unsafe class PeerCore : IDisposable
{
    /// <summary>Channel id carried by send entries of the peer's own control traffic (pings, handshake, close).</summary>
    public const ushort ControlChannelId = 0;

    /// <summary>Channel id carried by packed-container send entries (owned by the packer; see <see cref="OnContainerCompleted"/>).</summary>
    public const ushort ContainerChannelId = 1;

    /// <summary>Upper bound of the unidirectional stream allowance given to the peer after admission (ARCHITECTURE.md §7).</summary>
    public const int MaxPeerUnidirectionalStreams = 4096;

    /// <summary><see cref="ITransport.OpenStream"/> context of the control stream; engines must use other values.</summary>
    public const ulong ControlStreamContext = 1UL << 63;

    /// <summary>
    /// Tag bit of the <see cref="ITransport.OpenStream"/> context of a stream an engine opened
    /// (<see cref="MakeEngineStreamContext"/>): the peer routes its <see cref="ITransportSink.OnStreamStarted"/> to the
    /// engine of the encoded mode (<see cref="ChannelEngine.OnStreamStarted"/>).
    /// </summary>
    public const ulong EngineStreamContextTag = 1UL << 62;

    /// <summary>Mask of the stream serial carried in an engine stream context (24 bits; it wraps).</summary>
    public const uint EngineStreamSerialMask = 0xFF_FFFF;

    private readonly SlabAllocator _allocator;
    private readonly bool _ownsAllocator;
    private readonly int[] _indexById;
    private readonly ChannelDefinition[] _channels;
    private readonly ChannelEngine?[] _engineByMode = new ChannelEngine?[ChannelEngines.ModeCount];
    private readonly ChannelEngine[] _engineByIndex;
    private ChannelEngine[] _activeEngines = [];
    private readonly NativeArray<ChannelSendCounters> _sendCounters;
    private readonly NativeArray<ChannelRecvCounters> _recvCounters;
    private readonly NativeArray<SendToken> _tokens;
    private readonly NativeArray<ulong> _userContexts;
    private readonly int[] _entryOfToken;
    private readonly ReceiveMailbox?[] _mailboxByIndex;
    private ReceiveMailbox[] _mailboxes = [];
    // Cold, by slot: the shared payload an entry holds one reference on (SendShared), released in ReleasePayload.
    private readonly SharedLeaseTable?[] _sharedTables;
    private readonly SharedLease[] _sharedLeases;
    private readonly long _sendBudget;
    private readonly long _receiveBudget;
    private readonly bool _signalFromTransport;
    private ITransport? _transport;
    private long _sendBytes;
    private long _receiveBytes;
    private int _receiveReserved;
    private long _closeRequest;
    private int _streamCredit;
    private volatile int _maxDatagramPayload;
    private volatile bool _datagramsEnabled;
    private volatile bool _datagramStatesReported;
    private volatile bool _cancelOnBlocked;
    private volatile bool _admitted;
    private volatile bool _transportClosing;
    private volatile bool _transportClosed;
    private bool _disposed;
    private readonly int[] _scheduleOrder;
    private readonly CompletionEntry[] _localCompletions;
    private readonly NativeArray<long> _stamps;
    private readonly bool _atomicSendBudget;
    private long _passMicros;
    private long _stamp;
    private int _localHead;
    private int _localTail;
    private int _localCount;

    /// <summary>Creates the shared state of <paramref name="peer"/>. Engines are created by <see cref="InitializeEngines"/>.</summary>
    /// <param name="peer">The owning peer.</param>
    /// <param name="role">Client or server.</param>
    /// <param name="table">The channel table.</param>
    /// <param name="options">Validated options.</param>
    public PeerCore(QuiclyPeer peer, PeerRole role, ChannelTable table, PeerOptions options)
    {
        Peer = peer;
        Role = role;
        Table = table;
        Clock = options.Clock;
        CompletionMode = options.CompletionMode;
        _signalFromTransport = options.CompletionMode == CompletionMode.ThreadPool;
        _channels = table.All.ToArray();
        _indexById = new int[table.MaxId + 1];
        Array.Fill(_indexById, -1);
        for (int i = 0; i < _channels.Length; i++)
        {
            _indexById[_channels[i].Id] = i;
        }

        _engineByIndex = new ChannelEngine[_channels.Length];
        _mailboxByIndex = new ReceiveMailbox?[_channels.Length];
        if (options.Allocator is { } shared)
        {
            _allocator = shared;
        }
        else
        {
            _allocator = new SlabAllocator(options.AllocatorOptions ?? CreateCompactAllocatorOptions());
            _ownsAllocator = true;
        }

        _sendBudget = options.SendBudgetBytes;
        _receiveBudget = options.ReceiveBudgetBytes;
        Entries = new SendEntryTable(options.SendTableCapacity);
        int capacity = Entries.Capacity;
        Segments = new SegmentArena(options.SegmentArenaCapacity);
        _tokens = new NativeArray<SendToken>(capacity);
        _stamps = new NativeArray<long>(capacity);
        _atomicSendBudget = options.ThreadSafeSend;
        _userContexts = new NativeArray<ulong>(capacity);
        _entryOfToken = new int[capacity];
        Array.Fill(_entryOfToken, -1);
        _sharedTables = new SharedLeaseTable?[capacity];
        _sharedLeases = new SharedLease[capacity];
        Completions = new CompletionTable(capacity, _signalFromTransport);
        // Two completions per entry at most (one early Sent notice plus one final completion), and capacity is already
        // a power of two, so this is exactly the bound: asking for one more would double the ring (ADR 0008 §5).
        CompletionRing = new SpscRing<CompletionEntry>(2 * capacity);
        ReceiveRing = new SpscRing<ReceiveEntry>(options.ReceiveRingCapacity);
        _sendCounters = new NativeArray<ChannelSendCounters>(Math.Max(1, _channels.Length));
        _recvCounters = new NativeArray<ChannelRecvCounters>(Math.Max(1, _channels.Length));
        PeerUnidirectionalStreamLimit = ComputeUnidirectionalLimit(_channels);
        PendedStreams = new SpscRing<TransportStreamId>(PeerUnidirectionalStreamLimit + 2);
        Streams = new StreamTable();
        SessionMaxMessageSize = role == PeerRole.Server ? options.MaxMessageSize : 0;
        FlushIntervalMicros = Math.Max(1, PeerOptions.ToMicros(options.FlushInterval));
        _maxDatagramPayload = 0;
        _scheduleOrder = ComputeScheduleOrder(_channels);
        _localCompletions = new CompletionEntry[capacity];
        _passMicros = Clock.NowMicros;
        Packer = new DatagramPacker(this);
    }

    // ------------------------------------------------------------------ scheduler support (game thread)

    /// <summary>The per-peer datagram packer (engines hand it entries from <see cref="ChannelEngine.FlushChannel"/>).</summary>
    public DatagramPacker Packer { get; }

    /// <summary>
    /// Dense channel indices in scheduling order (PROTOCOL.md §4.5): highest <see cref="ChannelDefinition.Priority"/> first,
    /// ascending id among channels of equal priority. Precomputed at construction.
    /// </summary>
    public ReadOnlySpan<int> ScheduleOrder => _scheduleOrder;

    /// <summary>The admission stamp of the most recently admitted message (0 before the first; game thread).</summary>
    public long LastAdmissionStamp => _stamp;

    /// <summary>
    /// Clock micros of the current game-thread pass: read once at the start of every <see cref="QuiclyPeer.Poll"/>,
    /// <see cref="QuiclyPeer.Flush"/> and <see cref="SendMode.Immediate"/> pass (ADR 0008 invariant 9), and at
    /// construction. Engines stamp expiry deadlines from it instead of reading the clock per admitted message.
    /// </summary>
    public long CurrentPassMicros => _passMicros;

    /// <summary>Records the clock stamp of a game-thread pass (game thread; <paramref name="nowMicros"/> read once by the caller).</summary>
    /// <param name="nowMicros">Clock micros.</param>
    public void NotePass(long nowMicros) => _passMicros = nowMicros;

    /// <summary>
    /// Gives an admitted entry the next peer-wide admission number (game thread, at commit). Engines keep every channel
    /// queue in admission order, so the stamp of a queue's head is the oldest of the queue; <see cref="QuiclyPeer.FlushAsync"/>
    /// waits until no queue holds a stamp at or below its mark (<see cref="ChannelEngine.OldestQueuedStamp"/>).
    /// </summary>
    /// <param name="slot">The entry.</param>
    public void StampAdmission(int slot) => _stamps[slot] = ++_stamp;

    /// <summary>The admission stamp of an entry (<see cref="StampAdmission"/>; game thread).</summary>
    /// <param name="slot">The entry.</param>
    /// <returns>The stamp.</returns>
    public long GetAdmissionStamp(int slot) => _stamps[slot];

    /// <summary>
    /// The <see cref="ITransport.OpenStream"/> context of a stream an engine opens: <see cref="EngineStreamContextTag"/>, the
    /// mode (bits 56-61), the stream serial (bits 32-55) and the dense channel index (bits 0-31). Never equal to
    /// <see cref="ControlStreamContext"/>.
    /// </summary>
    /// <param name="mode">The engine's mode.</param>
    /// <param name="channelIndex">Dense channel index.</param>
    /// <param name="serial">The engine's serial of the stream (masked to <see cref="EngineStreamSerialMask"/>).</param>
    /// <returns>The context.</returns>
    public static ulong MakeEngineStreamContext(ChannelMode mode, int channelIndex, uint serial) =>
        EngineStreamContextTag | ((ulong)((byte)mode & 0x3F) << 56) | ((ulong)(serial & EngineStreamSerialMask) << 32) | (uint)channelIndex;

    /// <summary>Decodes a context made by <see cref="MakeEngineStreamContext"/> (any thread).</summary>
    /// <param name="context">A stream context.</param>
    /// <param name="mode">The engine's mode.</param>
    /// <param name="channelIndex">Dense channel index.</param>
    /// <param name="serial">The stream serial.</param>
    /// <returns><see langword="false"/> for any other context (the control stream, a test engine's own values).</returns>
    public static bool TryDecodeEngineStreamContext(ulong context, out ChannelMode mode, out int channelIndex, out uint serial)
    {
        if ((context & (ControlStreamContext | EngineStreamContextTag)) != EngineStreamContextTag)
        {
            mode = default;
            channelIndex = -1;
            serial = 0;
            return false;
        }

        mode = (ChannelMode)(byte)((context >> 56) & 0x3F);
        channelIndex = (int)(uint)context;
        serial = (uint)(context >> 32) & EngineStreamSerialMask;
        return true;
    }

    /// <summary>Completions queued with <see cref="QueueLocalCompletion"/> and not yet routed.</summary>
    public int LocalCompletionsQueued => _localCount;

    /// <summary>
    /// Queues a completion the game thread decided itself: the entry was never handed to the transport, or the transport
    /// refused it (expiry at scheduling time, a cancellation, a refused submission). It is routed like a transport
    /// completion — to the channel's engine, the packer's fan-out or the peer's control traffic — with
    /// <see cref="CompletionKind.Local"/> and <paramref name="status"/>, at the end of the current scheduler pass or at the
    /// start of the next Poll or Flush; never inline, so no continuation runs inside a pass. Queue an entry at most once,
    /// after it left every queue. Game thread.
    /// </summary>
    /// <param name="slot">The entry (normally <c>Filling</c>).</param>
    /// <param name="status">How the send ends.</param>
    public void QueueLocalCompletion(int slot, DeliveryStatus status)
    {
        CompletionEntry[] queue = _localCompletions;
        Debug.Assert(_localCount < queue.Length, "an entry is queued for local completion at most once");
        queue[_localTail] = new CompletionEntry
        {
            Slot = slot,
            Generation = Entries[slot].Generation,
            Kind = CompletionKind.Local,
            Canceled = true,
            Final = true,
            Status = status,
        };
        _localTail = _localTail + 1 == queue.Length ? 0 : _localTail + 1;
        _localCount++;
        NoteWork();
    }

    /// <summary>Takes the oldest completion queued with <see cref="QueueLocalCompletion"/> (game thread).</summary>
    /// <param name="completion">The completion.</param>
    /// <returns><see langword="false"/> when none is queued.</returns>
    public bool TryDequeueLocalCompletion(out CompletionEntry completion)
    {
        if (_localCount == 0)
        {
            completion = default;
            return false;
        }

        CompletionEntry[] queue = _localCompletions;
        completion = queue[_localHead];
        _localHead = _localHead + 1 == queue.Length ? 0 : _localHead + 1;
        _localCount--;
        return true;
    }

    /// <summary>
    /// The default delivery status of a final completion: the queued status of a <see cref="CompletionKind.Local"/> one,
    /// otherwise <see cref="MapDatagramState"/> or <see cref="MapStreamCompletion"/>.
    /// </summary>
    /// <param name="completion">A final completion.</param>
    /// <returns>The status.</returns>
    public DeliveryStatus MapCompletion(in CompletionEntry completion) => completion.Kind switch
    {
        CompletionKind.Local => completion.Status,
        CompletionKind.Datagram => MapDatagramState(completion.DatagramState),
        _ => MapStreamCompletion(completion.Canceled),
    };

    /// <summary>The delivery status of a send the transport refused synchronously (no completion follows).</summary>
    /// <param name="status">The refusal.</param>
    /// <returns><see cref="DeliveryStatus.Disconnected"/> while the connection is closing, otherwise <see cref="DeliveryStatus.Failed"/>.</returns>
    public DeliveryStatus MapSubmitFailure(TransportStatus status) =>
        _transportClosing || (status == TransportStatus.InvalidState && Transport is null) ? DeliveryStatus.Disconnected : DeliveryStatus.Failed;

    private static int[] ComputeScheduleOrder(ChannelDefinition[] channels)
    {
        int[] order = new int[channels.Length];
        for (int i = 0; i < order.Length; i++)
        {
            order[i] = i;
        }

        // Highest priority first; channels of equal priority keep the table's (ascending id) order.
        Array.Sort(order, (a, b) => channels[a].Priority != channels[b].Priority ? channels[b].Priority.CompareTo(channels[a].Priority) : a.CompareTo(b));
        return order;
    }

    /// <summary>The owning peer.</summary>
    public QuiclyPeer Peer { get; }

    /// <summary>Client or server.</summary>
    public PeerRole Role { get; }

    /// <summary>The channel table.</summary>
    public ChannelTable Table { get; }

    /// <summary>The clock of every timestamp and deadline.</summary>
    public IClock Clock { get; }

    /// <summary>Where tracked completions are signalled.</summary>
    public CompletionMode CompletionMode { get; }

    /// <summary>The flush interval in micros (resolves <see cref="ChannelDefinition.ExpiryTwiceFlushInterval"/>).</summary>
    public long FlushIntervalMicros { get; }

    /// <summary>The buffer pool (shared or private).</summary>
    public SlabAllocator Allocator => _allocator;

    /// <summary>Send entries (game thread, except the state word; see <see cref="SendEntryTable"/>).</summary>
    public SendEntryTable Entries { get; }

    /// <summary>Per-submission segment arrays for stream gathers (game thread).</summary>
    public SegmentArena Segments { get; }

    /// <summary>Tracked-send completion slots.</summary>
    public CompletionTable Completions { get; }

    /// <summary>Transport completions, transport thread → game thread (capacity 2 × the send table: two per entry).</summary>
    public SpscRing<CompletionEntry> CompletionRing { get; }

    /// <summary>Complete received messages, transport thread → game thread.</summary>
    public SpscRing<ReceiveEntry> ReceiveRing { get; }

    /// <summary>Streams whose receive returned Pending, transport thread → game thread (resumed in Poll).</summary>
    public SpscRing<TransportStreamId> PendedStreams { get; }

    /// <summary>Receive-side stream records (transport thread).</summary>
    public StreamTable Streams { get; }

    /// <summary>Peer-level counters.</summary>
    public PeerCounters Counters { get; } = new();

    /// <summary>The transport; <see langword="null"/> until a client's connector returned it (callbacks may arrive earlier).</summary>
    public ITransport? Transport
    {
        get => Volatile.Read(ref _transport);
        set => Volatile.Write(ref _transport, value);
    }

    /// <summary>Number of application channels.</summary>
    public int ChannelCount => _channels.Length;

    /// <summary>Every application channel, ascending id (the dense index is the position).</summary>
    public ReadOnlySpan<ChannelDefinition> Channels => _channels;

    /// <summary>One engine per mode present in the table.</summary>
    public ReadOnlySpan<ChannelEngine> ActiveEngines => _activeEngines;

    /// <summary>Mailboxes of coalescing channels, in creation order.</summary>
    public ReadOnlySpan<ReceiveMailbox> Mailboxes => _mailboxes;

    /// <summary>Unidirectional streams the peer may open after admission: Σ max(MaxGroups, 1) over stream-capable channels, capped at 4 096.</summary>
    public int PeerUnidirectionalStreamLimit { get; }

    /// <summary>Clock micros when the transport connected (connection-relative wire timestamps count from here).</summary>
    public long ConnectionStartMicros { get; set; }

    /// <summary>The session epoch (0 until admitted, then ≥ 1).</summary>
    public uint Epoch { get; set; }

    /// <summary>
    /// Session message size cap (<c>HelloAck.maxMessageSize</c>; 0 = only each channel's limit). A server knows it from its
    /// options; a client learns it on the transport thread from an accepted HelloAck before admission is published.
    /// </summary>
    public int SessionMaxMessageSize { get; set; }

    /// <summary>Tick of the packed container being dispatched (transport thread; 0 outside containers).</summary>
    public uint CurrentSenderTick { get; set; }

    /// <summary>True once the session is admitted: application datagrams and streams are accepted (any thread).</summary>
    public bool IsAdmitted
    {
        get => _admitted;
        set => _admitted = value;
    }

    /// <summary>Current maximum datagram payload (updated by the transport; read it at pack time).</summary>
    public int MaxDatagramPayload => _maxDatagramPayload;

    /// <summary>Datagrams are negotiated.</summary>
    public bool DatagramsEnabled => _datagramsEnabled;

    /// <summary>The transport reports per-datagram acknowledgement and loss.</summary>
    public bool DatagramStatesReported => _datagramStatesReported;

    /// <summary>Incremented whenever the peer raised our stream limits (engines retry opens that hit the limit).</summary>
    public int StreamCreditGeneration => Volatile.Read(ref _streamCredit);

    /// <summary>A close of the transport was started (completions that are canceled from now on map to Disconnected).</summary>
    public bool IsTransportClosing => _transportClosing;

    /// <summary>The transport reported <c>OnClosed</c>; no further callback arrives.</summary>
    public bool IsTransportClosed => _transportClosed;

    /// <summary>Send lease bytes held now (game thread; any thread with <see cref="PeerOptions.ThreadSafeSend"/>).</summary>
    public long SendBytesOutstanding => Volatile.Read(ref _sendBytes);

    /// <summary>Receive lease bytes held now (any thread).</summary>
    public long ReceiveBytesOutstanding => Volatile.Read(ref _receiveBytes);

    /// <summary>The receive budget (<see cref="PeerOptions.ReceiveBudgetBytes"/>).</summary>
    public long ReceiveBudgetBytes => _receiveBudget;

    // ------------------------------------------------------------------ construction

    /// <summary>Creates one engine per mode present in the table and initialises it (constructor time, game thread).</summary>
    /// <param name="factory">Test hook: overrides the engine of a mode (null result = default).</param>
    /// <exception cref="InvalidOperationException">The factory returned an engine of another mode.</exception>
    public void InitializeEngines(Func<ChannelMode, ChannelEngine?>? factory)
    {
        List<ChannelEngine> active = [];
        for (int m = 0; m < ChannelEngines.ModeCount; m++)
        {
            ChannelMode mode = (ChannelMode)m;
            int count = 0;
            foreach (ChannelDefinition channel in _channels)
            {
                if (channel.Mode == mode)
                {
                    count++;
                }
            }

            if (count == 0)
            {
                continue;
            }

            ChannelDefinition[] ofMode = new ChannelDefinition[count];
            ChannelEngine engine = factory?.Invoke(mode) ?? ChannelEngines.Create(mode);
            if (engine.Mode != mode)
            {
                throw new InvalidOperationException($"The engine created for {mode} implements {engine.Mode}.");
            }

            int n = 0;
            for (int i = 0; i < _channels.Length; i++)
            {
                if (_channels[i].Mode == mode)
                {
                    ofMode[n++] = _channels[i];
                    _engineByIndex[i] = engine;
                }
            }

            _engineByMode[m] = engine;
            active.Add(engine);
            engine.Initialize(this, ofMode);
        }

        _activeEngines = active.ToArray();
    }

    /// <summary>
    /// Allocator options of a peer without a shared allocator: a compact set of size classes (about 1.8 MiB) that covers
    /// the default 256 KiB send and receive budgets. Servers with many peers should share one allocator
    /// (<see cref="PeerOptions.Allocator"/>).
    /// </summary>
    internal static SlabAllocatorOptions CreateCompactAllocatorOptions() => new()
    {
        FreeListShards = 2,
        SizeClasses =
        [
            new(64, 4096),
            new(256, 1024),
            new(1536, 192),
            new(4096, 64),
            new(16384, 16),
            new(65536, 4),
            new(262144, 1),
        ],
    };

    private static int ComputeUnidirectionalLimit(ChannelDefinition[] channels)
    {
        long total = 0;
        foreach (ChannelDefinition channel in channels)
        {
            if (channel.IsStreamMode)
            {
                total += Math.Max(channel.MaxGroups, 1);
            }
        }

        return (int)Math.Min(total, MaxPeerUnidirectionalStreams);
    }

    // ------------------------------------------------------------------ channels, engines, counters (any thread, read-only)

    /// <summary>Dense index of channel <paramref name="channelId"/>, or -1 when the table has no such channel.</summary>
    /// <param name="channelId">Channel id (any int).</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int ChannelIndexOf(int channelId)
    {
        int[] map = _indexById;
        return (uint)channelId < (uint)map.Length ? map[channelId] : -1;
    }

    /// <summary>The channel at dense index <paramref name="channelIndex"/>.</summary>
    /// <param name="channelIndex">Dense index.</param>
    public ChannelDefinition GetChannel(int channelIndex) => _channels[channelIndex];

    /// <summary>The engine of the channel at dense index <paramref name="channelIndex"/>.</summary>
    /// <param name="channelIndex">Dense index.</param>
    public ChannelEngine GetEngine(int channelIndex) => _engineByIndex[channelIndex];

    /// <summary>The engine of <paramref name="mode"/>, or <see langword="null"/> when the table has no channel of that mode.</summary>
    /// <param name="mode">The mode.</param>
    public ChannelEngine? GetEngine(ChannelMode mode) => _engineByMode[(int)mode];

    /// <summary>
    /// Effective maximum raw message size of <paramref name="channel"/>: <c>min(channel.MaxMessageSize, session cap)</c>,
    /// except for Bulk channels whose limit is their own (PROTOCOL.md §8).
    /// </summary>
    /// <param name="channel">The channel.</param>
    public int EffectiveMaxMessageSize(ChannelDefinition channel)
    {
        int cap = SessionMaxMessageSize;
        return cap <= 0 || channel.Mode == ChannelMode.Bulk || cap >= channel.MaxMessageSize ? channel.MaxMessageSize : cap;
    }

    /// <summary>Send-side counters of a channel (game thread).</summary>
    /// <param name="channelIndex">Dense index.</param>
    public ref ChannelSendCounters SendCounters(int channelIndex) => ref _sendCounters[channelIndex];

    /// <summary>Receive-side counters of a channel (transport thread; the game thread may add decode drops).</summary>
    /// <param name="channelIndex">Dense index.</param>
    public ref ChannelRecvCounters RecvCounters(int channelIndex) => ref _recvCounters[channelIndex];

    /// <summary>Counts a dropped datagram message of <paramref name="channel"/> (transport thread).</summary>
    /// <param name="channel">Channel id.</param>
    public void CountDatagramDropped(ushort channel)
    {
        int index = ChannelIndexOf(channel);
        if (index >= 0)
        {
            _recvCounters[index].Dropped++;
        }
    }

    /// <summary>Creates a key table for <paramref name="channel"/> (dense or hashed; the engine owns and disposes it).</summary>
    /// <param name="channel">A keyed channel.</param>
    /// <returns>The key table.</returns>
    public static IKeyTable CreateKeyTable(ChannelDefinition channel) =>
        channel.KeySpace.IsDense ? new DenseKeyTable(channel.MaxKeys) : new KeyTable(channel.MaxKeys);

    // ------------------------------------------------------------------ transport state (transport thread writes)

    /// <summary>Records the datagram capability (transport thread).</summary>
    /// <param name="enabled">Datagrams negotiated.</param>
    /// <param name="maxPayload">Current maximum datagram payload.</param>
    public void SetDatagramCapability(bool enabled, int maxPayload)
    {
        _maxDatagramPayload = maxPayload;
        _datagramsEnabled = enabled;
    }

    /// <summary>Records whether the transport reports datagram send states (transport thread, at connect).</summary>
    /// <param name="reported">The capability.</param>
    public void SetDatagramStatesReported(bool reported) => _datagramStatesReported = reported;

    /// <summary>
    /// Records whether the transport honours <see cref="TransportSendFlags.CancelOnBlocked"/> (transport thread: at connect and
    /// whenever the datagram capability changes).
    /// </summary>
    /// <param name="honoured">The capability (<see cref="TransportCapabilities.CancelOnBlocked"/>).</param>
    public void SetCancelOnBlocked(bool honoured) => _cancelOnBlocked = honoured;

    /// <summary>The transport honours <see cref="TransportSendFlags.CancelOnBlocked"/> (any thread).</summary>
    public bool CancelOnBlockedHonoured => _cancelOnBlocked;

    /// <summary>The peer raised our stream limits (transport thread).</summary>
    public void NoteStreamCredit() => Interlocked.Increment(ref _streamCredit);

    /// <summary>A transport close was started (game thread).</summary>
    public void MarkTransportClosing() => _transportClosing = true;

    /// <summary>The transport reported <c>OnClosed</c> (transport thread).</summary>
    public void MarkTransportClosed()
    {
        _transportClosing = true;
        _transportClosed = true;
    }

    /// <summary>Connection-relative 32-bit wire micros (PROTOCOL.md §2.3 Ping/Pong timestamps).</summary>
    /// <param name="nowMicros">Clock micros.</param>
    public uint ToWireMicros(long nowMicros) => (uint)(nowMicros - ConnectionStartMicros);

    /// <summary>Low 32 bits of a receive time (<see cref="ReceiveEntry.ReceivedMicrosDelta"/>).</summary>
    /// <param name="nowMicros">Clock micros of the receive callback.</param>
    public static uint StampReceive(long nowMicros) => (uint)nowMicros;

    /// <summary>Restores a receive time stamped with <see cref="StampReceive"/> (exact while the message waited less than 71 minutes).</summary>
    /// <param name="stamp">The stamp.</param>
    /// <param name="nowMicros">Clock micros, not earlier than the receive.</param>
    public static long RestoreReceive(uint stamp, long nowMicros) => nowMicros - (uint)((uint)nowMicros - stamp);

    // ------------------------------------------------------------------ close requests (any thread)

    /// <summary>
    /// Asks for the connection to be closed with <paramref name="code"/>; the game thread executes it in the next
    /// <see cref="QuiclyPeer.Poll"/> or <see cref="QuiclyPeer.Flush"/> (never <see cref="ITransport.Close"/> from a
    /// callback). The first request wins.
    /// </summary>
    /// <param name="code">The error code (PROTOCOL.md §6).</param>
    public void RequestClose(QuiclyErrorCode code)
    {
        if (Interlocked.CompareExchange(ref _closeRequest, (long)code + 1, 0) == 0)
        {
            Peer.Signal(QuiclyPeer.SignalCloseRequest);
        }
    }

    /// <summary>The pending close request, if any (game thread).</summary>
    /// <param name="code">The requested code.</param>
    /// <returns><see langword="true"/> when a close was requested.</returns>
    public bool TryGetCloseRequest(out QuiclyErrorCode code)
    {
        long value = Volatile.Read(ref _closeRequest);
        code = value > 0 ? (QuiclyErrorCode)(value - 1) : QuiclyErrorCode.NoError;
        return value > 0;
    }

    // ------------------------------------------------------------------ leases

    /// <summary>
    /// Rents a send lease of at least <paramref name="length"/> bytes within the send budget (game thread; any thread when
    /// <see cref="PeerOptions.ThreadSafeSend"/> is on, which makes the budget accounting atomic).
    /// </summary>
    /// <param name="length">Bytes needed.</param>
    /// <param name="lease">The lease, or empty.</param>
    /// <returns><see langword="false"/> when the budget or the pool is exhausted.</returns>
    public bool TryRentSend(int length, out BufferLease lease)
    {
        if (!_allocator.TryRent(length, out lease))
        {
            return false;
        }

        if (_atomicSendBudget)
        {
            if (Interlocked.Add(ref _sendBytes, lease.Length) > _sendBudget)
            {
                Interlocked.Add(ref _sendBytes, -lease.Length);
                _allocator.Return(in lease);
                lease = BufferLease.Empty;
                return false;
            }

            return true;
        }

        if (_sendBytes + lease.Length > _sendBudget)
        {
            _allocator.Return(in lease);
            lease = BufferLease.Empty;
            return false;
        }

        _sendBytes += lease.Length;
        return true;
    }

    /// <summary>Returns a send lease (the threads of <see cref="TryRentSend"/>). Empty leases are ignored.</summary>
    /// <param name="lease">The lease.</param>
    public void ReturnSend(in BufferLease lease)
    {
        if (lease.IsEmpty)
        {
            return;
        }

        if (_atomicSendBudget)
        {
            Interlocked.Add(ref _sendBytes, -lease.Length);
        }
        else
        {
            _sendBytes -= lease.Length;
        }

        _allocator.Return(in lease);
    }

    /// <summary>
    /// Rents a receive lease of at least <paramref name="length"/> bytes within the receive budget (transport thread; the
    /// game thread when decoding). The caller counts a failure (<see cref="PeerCounters.OutOfReceiveBuffers"/>).
    /// </summary>
    /// <param name="length">Bytes needed.</param>
    /// <param name="lease">The lease, or empty.</param>
    /// <returns><see langword="false"/> when the budget or the pool is exhausted.</returns>
    public bool TryRentReceive(int length, out BufferLease lease)
    {
        if (!_allocator.TryRent(length, out lease))
        {
            return false;
        }

        if (Interlocked.Add(ref _receiveBytes, lease.Length) > _receiveBudget)
        {
            Interlocked.Add(ref _receiveBytes, -lease.Length);
            _allocator.Return(in lease);
            lease = BufferLease.Empty;
            return false;
        }

        return true;
    }

    /// <summary>Returns a receive lease (any thread; normally the game thread). Empty leases are ignored.</summary>
    /// <param name="lease">The lease.</param>
    public void ReturnReceive(in BufferLease lease)
    {
        if (lease.IsEmpty)
        {
            return;
        }

        Interlocked.Add(ref _receiveBytes, -lease.Length);
        _allocator.Return(in lease);
    }

    /// <summary>Native address of a lease's first byte.</summary>
    /// <param name="lease">A non-empty lease.</param>
    public byte* GetPointer(in BufferLease lease) => _allocator.GetPointer(in lease);

    /// <summary>The whole block of a lease as a span.</summary>
    /// <param name="lease">A non-empty lease.</param>
    public Span<byte> GetSpan(in BufferLease lease) => _allocator.GetSpan(in lease);

    // ------------------------------------------------------------------ receive ring (transport thread = producer)

    /// <summary>
    /// Publishes a complete message to the game thread (transport thread). The ring's lease ownership moves on success;
    /// on failure (ring full, counting reservations) the caller still owns the lease, drops the message and counts
    /// its channel's <c>RingDrops</c> (the peer-level drop counter is incremented here).
    /// </summary>
    /// <param name="entry">The message descriptor (<see cref="ReceiveEntry.ReceivedMicrosDelta"/> = <see cref="StampReceive"/>).</param>
    /// <returns><see langword="false"/> when the ring is full.</returns>
    public bool TryEnqueueReceive(in ReceiveEntry entry)
    {
        SpscRing<ReceiveEntry> ring = ReceiveRing;
        if (!ring.TryEnqueueReserving(in entry, _receiveReserved))
        {
            Counters.ReceiveRingDrops++;
            return false;
        }

        NoteRingUse(ring.CachedCount + _receiveReserved);
        NoteWork();
        return true;
    }

    /// <summary>
    /// Tells the host that game-thread work was published (<see cref="PeerOptions.WorkSignal"/>): any thread,
    /// non-blocking, and at most once between two <see cref="QuiclyPeer.Poll"/> calls, so a burst costs one call.
    /// </summary>
    public void NoteWork() => Peer.NoteWork();

    /// <summary>
    /// Reserves one receive-ring slot for a message that is still arriving (a stream message), so that publishing it at its
    /// end with <see cref="PublishReserved"/> cannot fail (transport thread).
    /// </summary>
    /// <returns><see langword="false"/> when the ring (counting reservations) is full: apply back-pressure.</returns>
    public bool TryReserveReceive()
    {
        SpscRing<ReceiveEntry> ring = ReceiveRing;
        if (!ring.HasRoomFor(_receiveReserved))
        {
            return false;
        }

        _receiveReserved++;
        NoteRingUse(ring.CachedCount + _receiveReserved);
        return true;
    }

    /// <summary>Publishes a message into a slot reserved with <see cref="TryReserveReceive"/> (transport thread).</summary>
    /// <param name="entry">The message descriptor.</param>
    public void PublishReserved(in ReceiveEntry entry)
    {
        Debug.Assert(_receiveReserved > 0, "no receive reservation");
        _receiveReserved--;
        bool enqueued = ReceiveRing.TryEnqueue(in entry);
        Debug.Assert(enqueued, "a reserved receive slot must be free");
        NoteWork();
    }

    /// <summary>Gives back a reservation that will not be published (transport thread).</summary>
    public void CancelReservation()
    {
        Debug.Assert(_receiveReserved > 0, "no receive reservation");
        _receiveReserved--;
    }

    /// <summary>
    /// Updates the ring's occupancy high-water mark from the producer's own view (transport thread). The consumer's
    /// index is read, and the shared counter written, only when this call suspects a new maximum — which can happen at
    /// most <c>Capacity</c> times in the peer's life — so a received message costs no coherence traffic
    /// (ADR 0008 invariant 5).
    /// </summary>
    /// <param name="used">Queued entries plus reservations as the producer's cached view sees them (an upper bound).</param>
    private void NoteRingUse(int used)
    {
        PeerCounters counters = Counters;
        if (used <= counters.ReceiveRingHighWater)
        {
            return;
        }

        int exact = ReceiveRing.RefreshedCount() + _receiveReserved;
        if (exact > counters.ReceiveRingHighWater)
        {
            counters.ReceiveRingHighWater = exact;
        }
    }

    /// <summary>
    /// Creates the mailboxes of a coalescing keyed channel (engine <see cref="ChannelEngine.Initialize"/>, construction
    /// time). <see cref="QuiclyPeer.Poll"/> dispatches it to the channel's handler; <see cref="QuiclyPeer.Drain"/> takes from it.
    /// </summary>
    /// <param name="channelIndex">Dense channel index.</param>
    /// <param name="keySlots">Key slots (the channel's key table capacity).</param>
    /// <returns>The mailboxes (owned by the core; disposed with it).</returns>
    public ReceiveMailbox CreateMailbox(int channelIndex, int keySlots)
    {
        ReceiveMailbox box = new(_channels[channelIndex].Id, channelIndex, keySlots);
        _mailboxByIndex[channelIndex] = box;
        Array.Resize(ref _mailboxes, _mailboxes.Length + 1);
        _mailboxes[^1] = box;
        return box;
    }

    /// <summary>The mailboxes of a channel, or <see langword="null"/>.</summary>
    /// <param name="channelIndex">Dense channel index.</param>
    public ReceiveMailbox? GetMailbox(int channelIndex) => _mailboxByIndex[channelIndex];

    /// <summary>
    /// Remembers a stream whose receive returned <see cref="ReceiveResult.PendingAfter"/> (transport thread);
    /// <see cref="QuiclyPeer.Poll"/> calls <see cref="ITransport.ResumeStreamReceive"/> for it after draining the ring.
    /// </summary>
    /// <param name="id">The stream.</param>
    public void NotePendedStream(TransportStreamId id)
    {
        Counters.StreamReceivePends++;
        if (!PendedStreams.TryEnqueue(in id))
        {
            // Sized to the peer's stream allowance + 2: a stream can pend only once until resumed.
            Counters.CallbackFaults++;
        }

        NoteWork();
    }

    // ------------------------------------------------------------------ streams (game thread)

    /// <summary>Opens a local stream (game thread).</summary>
    /// <param name="kind">Stream kind.</param>
    /// <param name="context">Context reported by <see cref="ITransportSink.OnStreamStarted"/>.</param>
    /// <param name="priority">Stream priority (<c>channel priority × 257</c>, PROTOCOL.md §4.5).</param>
    /// <param name="id">The stream.</param>
    /// <returns>The transport status.</returns>
    public TransportStatus OpenStream(StreamKind kind, ulong context, ushort priority, out TransportStreamId id)
    {
        ITransport? transport = Transport;
        if (transport is null || _transportClosing)
        {
            id = TransportStreamId.None;
            return TransportStatus.InvalidState;
        }

        return transport.OpenStream(kind, context, priority, out id);
    }

    // ------------------------------------------------------------------ send entries (game thread)

    /// <summary>Allocates a send entry for <paramref name="channel"/> in state <c>Filling</c> (game thread).</summary>
    /// <param name="channel">Channel id (0 = control, 1 = container).</param>
    /// <param name="flags">Initial flags.</param>
    /// <param name="slot">The slot, or -1.</param>
    /// <returns><see langword="false"/> when the table is full (answer <see cref="SendStatus.QueueFull"/>).</returns>
    public bool TryAllocateEntry(ushort channel, SendEntryFlags flags, out int slot)
    {
        if (!Entries.TryAllocate(out slot))
        {
            return false;
        }

        ref SendEntry entry = ref Entries[slot];
        entry.Channel = channel;
        entry.Flags = flags;
        _tokens[slot] = default;
        _userContexts[slot] = 0;
        return true;
    }

    /// <summary>
    /// Makes an entry tracked: allocates its <see cref="SendToken"/> and marks it <see cref="SendEntryFlags.Tracked"/>
    /// (game thread, while <c>Filling</c>). If admission then fails, call <see cref="DiscardEntry"/>, which releases the token.
    /// </summary>
    /// <param name="slot">The entry.</param>
    /// <param name="userContext">The application's <see cref="SendOptions.Context"/>.</param>
    /// <param name="token">The token.</param>
    /// <returns><see langword="false"/> when the completion table is full (answer <see cref="SendStatus.QueueFull"/>).</returns>
    public bool TryTrack(int slot, ulong userContext, out SendToken token)
    {
        if (!Completions.TryAllocate(out token))
        {
            return false;
        }

        _tokens[slot] = token;
        _userContexts[slot] = userContext;
        _entryOfToken[token.Slot] = slot;
        Entries[slot].Flags |= SendEntryFlags.Tracked;
        return true;
    }

    /// <summary>The token of a tracked entry.</summary>
    /// <param name="slot">The entry.</param>
    public SendToken GetToken(int slot) => _tokens[slot];

    /// <summary>The application context of a tracked entry.</summary>
    /// <param name="slot">The entry.</param>
    public ulong GetUserContext(int slot) => _userContexts[slot];

    /// <summary>The live entry of a token, or -1 (game thread).</summary>
    /// <param name="token">A token.</param>
    public int EntryOfToken(SendToken token)
    {
        if (!token.IsValid || (uint)token.Slot >= (uint)_entryOfToken.Length)
        {
            return -1;
        }

        int slot = _entryOfToken[token.Slot];
        return slot >= 0 && Entries.GetState(slot) != SendEntryState.Free && _tokens[slot] == token ? slot : -1;
    }

    /// <summary>Points the entry's payload segment at a send lease and gives the lease to the entry (released at completion).</summary>
    /// <param name="slot">The entry.</param>
    /// <param name="lease">A send lease (<see cref="TryRentSend"/>).</param>
    /// <param name="length">Payload bytes at the start of the lease.</param>
    public void AttachLease(int slot, in BufferLease lease, int length)
    {
        Entries.Leases[slot] = lease;
        Entries[slot].Payload = new TransportSegment(_allocator.GetPointer(in lease), length);
    }

    /// <summary>Points the entry's payload segment at caller memory (pinned or native; valid until the completion).</summary>
    /// <param name="slot">The entry.</param>
    /// <param name="data">The payload.</param>
    /// <param name="length">Payload bytes.</param>
    public void SetPayload(int slot, byte* data, int length) => Entries[slot].Payload = new TransportSegment(data, length);

    /// <summary>
    /// Takes one reference on a shared payload for the entry (<see cref="QuiclyPeer.SendShared"/>; game thread, while
    /// <c>Filling</c>): <see cref="SharedLeaseTable.Retain"/> now, and exactly one
    /// <see cref="SharedLeaseTable.Release"/> in <see cref="ReleasePayload"/> — when the transport released the payload,
    /// when the entry is discarded, when the session closes, or when the peer is disposed with the send in flight. The
    /// payload segment itself is set by <see cref="SetPayload"/>.
    /// </summary>
    /// <param name="slot">The entry.</param>
    /// <param name="table">The table that counts the lease's references.</param>
    /// <param name="lease">The shared payload.</param>
    public void AttachShared(int slot, SharedLeaseTable table, in SharedLease lease)
    {
        table.Retain(in lease);
        _sharedTables[slot] = table;
        _sharedLeases[slot] = lease;
    }

    /// <summary>
    /// Publishes the entry and hands it to the transport as one datagram: header segment (when <c>HeaderLength</c> &gt; 0)
    /// plus payload segment (when not empty). Sets <see cref="SendEntryFlags.Datagram"/>. Game thread.
    /// </summary>
    /// <param name="slot">A <c>Filling</c> entry.</param>
    /// <param name="flags">Datagram flags: <c>Priority</c>; <c>CancelOnBlocked</c> only when the transport reports it, and no <c>DelaySend</c> for tick bursts (§7.1).</param>
    /// <returns>
    /// <see cref="TransportStatus.Success"/> (a completion will follow; do not touch the entry until it is drained), or the
    /// failure (no completion follows; the entry is <c>Filling</c> again and still owned by the caller).
    /// </returns>
    public TransportStatus SubmitDatagram(int slot, TransportSendFlags flags)
    {
        ITransport? transport = Transport;
        if (transport is null || _transportClosing)
        {
            return TransportStatus.InvalidState;
        }

        ref SendEntry entry = ref Entries[slot];
        TransportSegment* segments = Entries.GetSegments(slot);
        int count = 2;
        if (entry.HeaderLength == 0)
        {
            segments++;
            count = 1;
        }
        else if (entry.Payload.Length == 0)
        {
            count = 1;
        }

        entry.Flags |= SendEntryFlags.Datagram;
        ulong context = Entries.Contexts[slot];
        Entries.Publish(slot);
        TransportStatus status = transport.SendDatagram(segments, count, context, flags);
        if (status != TransportStatus.Success)
        {
            Unpublish(slot);
        }

        return status;
    }

    /// <summary>
    /// Publishes the entry and hands <paramref name="segments"/> to the transport as one stream send whose context is the
    /// entry's (game thread). The segments must stay valid until the completion (the entry's own pair, or a run from
    /// <see cref="Segments"/>).
    /// </summary>
    /// <param name="stream">The stream.</param>
    /// <param name="segments">Contiguous segment array.</param>
    /// <param name="count">Number of segments.</param>
    /// <param name="slot">The <c>Filling</c> entry whose context the completion carries.</param>
    /// <param name="flags">Stream flags (<c>Start</c>, <c>Fin</c>, <c>Priority</c>; no <c>DelaySend</c> for tick bursts).</param>
    /// <returns>As <see cref="SubmitDatagram"/>.</returns>
    public TransportStatus SubmitStream(TransportStreamId stream, TransportSegment* segments, int count, int slot, TransportSendFlags flags)
    {
        ITransport? transport = Transport;
        if (transport is null || _transportClosing)
        {
            return TransportStatus.InvalidState;
        }

        ulong context = Entries.Contexts[slot];
        Entries.Publish(slot);
        TransportStatus status = transport.SendStream(stream, segments, count, context, flags);
        if (status != TransportStatus.Success)
        {
            Unpublish(slot);
        }

        return status;
    }

    private void Unpublish(int slot)
    {
        // No completion follows a failed send call, so the entry is still ours.
        bool reverted = Entries.TryTransition(slot, SendEntryState.InFlight, SendEntryState.Filling);
        Debug.Assert(reverted, "a rejected send must leave the entry in flight");
    }

    /// <summary>Completes one stage of a tracked entry's token (game thread). Ignored for untracked entries.</summary>
    /// <param name="slot">The entry.</param>
    /// <param name="stage">The stage.</param>
    /// <param name="status">The status known now (<see cref="DeliveryStatus.Pending"/> keeps the current one).</param>
    public void CompleteStage(int slot, CompletionStage stage, DeliveryStatus status)
    {
        if ((Entries[slot].Flags & SendEntryFlags.Tracked) != 0)
        {
            Completions.Complete(_tokens[slot], stage, status);
        }
    }

    /// <summary>
    /// Finishes an entry (game thread): completes both stages of its token with <paramref name="status"/> (stages already
    /// completed are unaffected), releases its payload lease or pin, and frees the slot. Accepts an entry in any state:
    /// <c>Completed</c> (the normal case), <c>InFlight</c> (a gather member covered by another entry's completion) or
    /// <c>Filling</c> (never submitted).
    /// </summary>
    /// <param name="slot">The entry.</param>
    /// <param name="status">The final delivery status.</param>
    public void CompleteEntry(int slot, DeliveryStatus status)
    {
        if ((Entries[slot].Flags & SendEntryFlags.Tracked) != 0)
        {
            SendToken token = _tokens[slot];
            _tokens[slot] = default;
            Completions.Complete(token, CompletionStage.BufferReleased, status);
            Completions.Complete(token, CompletionStage.RemoteAccepted, status);
        }

        ReleasePayload(slot);
        RetireEntry(slot);
    }

    /// <summary>Unwinds an entry that was never submitted (game thread): releases its token (Canceled), payload and slot.</summary>
    /// <param name="slot">A <c>Filling</c> entry.</param>
    public void DiscardEntry(int slot)
    {
        if ((Entries[slot].Flags & SendEntryFlags.Tracked) != 0)
        {
            SendToken token = _tokens[slot];
            _tokens[slot] = default;
            Completions.Release(token);
        }

        ReleasePayload(slot);
        RetireEntry(slot);
    }

    /// <summary>
    /// Returns the entry's payload lease, frees its pin handle and drops its shared reference, if any (game thread).
    /// Idempotent: each of the three is cleared as it is released, so a <c>Sent</c> notice followed by the final completion
    /// releases the shared payload exactly once.
    /// </summary>
    /// <param name="slot">The entry.</param>
    public void ReleasePayload(int slot)
    {
        BufferLease lease = Entries.Leases[slot];
        if (!lease.IsEmpty)
        {
            Entries.Leases[slot] = BufferLease.Empty;
            ReturnSend(in lease);
        }

        nint pin = Entries.PinHandles[slot];
        if (pin != 0)
        {
            Entries.PinHandles[slot] = 0;
            GCHandle.FromIntPtr(pin).Free();
        }

        SharedLeaseTable? shared = _sharedTables[slot];
        if (shared is not null)
        {
            SharedLease sharedLease = _sharedLeases[slot];
            _sharedTables[slot] = null;
            _sharedLeases[slot] = default;
            shared.Release(in sharedLease);
        }
    }

    private void RetireEntry(int slot)
    {
        switch (Entries.GetState(slot))
        {
            case SendEntryState.Filling:
                Entries.Discard(slot);
                break;
            case SendEntryState.InFlight:
            case SendEntryState.Cancelling:
                if (!Entries.TryTransition(slot, SendEntryState.InFlight, SendEntryState.Completed))
                {
                    Entries.TryTransition(slot, SendEntryState.Cancelling, SendEntryState.Completed);
                }

                Entries.Free(slot);
                break;
            case SendEntryState.Completed:
                Entries.Free(slot);
                break;
        }
    }

    /// <summary>
    /// Completion routing for packed-container entries (channel 1, game thread): the packer's fan-out to the member entries
    /// (<see cref="DatagramPacker.OnContainerCompleted"/>).
    /// </summary>
    /// <param name="slot">The container entry.</param>
    /// <param name="completion">The completion.</param>
    public void OnContainerCompleted(int slot, in CompletionEntry completion) => Packer.OnContainerCompleted(slot, in completion);

    /// <summary>
    /// Default delivery status of a datagram's final send state (PROTOCOL.md §4.3). An acknowledgement is
    /// <see cref="DeliveryStatus.Delivered"/>; <see cref="DatagramSendState.Sent"/> is final only on a carrier that
    /// reports no per-datagram states (<see cref="DatagramStatesReported"/> false: WebTransport/browser, MsQuic without
    /// the capability), and such a send completes <see cref="DeliveryStatus.Sent"/> — the datagram reached the network
    /// and nothing more will ever be known about it, so it is never reported as delivered.
    /// </summary>
    /// <param name="state">A final state (or Sent when the transport does not report states).</param>
    /// <returns>The status (<see cref="DeliveryStatus.Disconnected"/> for a cancellation caused by the connection closing).</returns>
    public DeliveryStatus MapDatagramState(DatagramSendState state) => state switch
    {
        DatagramSendState.Acknowledged or DatagramSendState.AcknowledgedSpurious => DeliveryStatus.Delivered,
        DatagramSendState.Sent => DeliveryStatus.Sent,
        DatagramSendState.LostDiscarded => DeliveryStatus.Lost,
        _ => _transportClosing ? DeliveryStatus.Disconnected : DeliveryStatus.Expired,
    };

    /// <summary>Default delivery status of a stream send completion.</summary>
    /// <param name="canceled">The send was canceled.</param>
    /// <returns>The status.</returns>
    public DeliveryStatus MapStreamCompletion(bool canceled) =>
        !canceled ? DeliveryStatus.Delivered : _transportClosing ? DeliveryStatus.Disconnected : DeliveryStatus.Failed;

    // ------------------------------------------------------------------ transport completions (transport thread)

    /// <summary>
    /// A datagram send state (transport thread): validates the generation-tagged context, moves the entry to
    /// <c>Completed</c> on a final state and pushes the completion (and, for tracked/container entries, the early Sent
    /// notice) into the completion ring. In <see cref="Session.CompletionMode.ThreadPool"/> mode a tracked entry's token is
    /// also signalled here (unless <see cref="SendEntryFlags.EngineCompletes"/>).
    /// </summary>
    /// <param name="context">The entry context.</param>
    /// <param name="state">The reported state.</param>
    public void OnTransportDatagramState(ulong context, DatagramSendState state)
    {
        bool final = state.IsFinal() || (state == DatagramSendState.Sent && !_datagramStatesReported);
        if (!final)
        {
            if (state != DatagramSendState.Sent)
            {
                return; // LostSuspect: engines act on the final state
            }

            if (!Entries.TryResolveContext(context, out int sentSlot))
            {
                Counters.StaleCompletions++;
                return;
            }

            // Every datagram entry gets the notice: the payload block is released at Sent (ADR 0008 invariant 1,
            // ARCHITECTURE.md §2.1), tracked or not, and the ring holds two items per entry.
            SendEntryFlags flags = Entries[sentSlot].Flags;
            if (_signalFromTransport && (flags & (SendEntryFlags.Tracked | SendEntryFlags.Container | SendEntryFlags.EngineCompletes)) == SendEntryFlags.Tracked)
            {
                Completions.Complete(_tokens[sentSlot], CompletionStage.BufferReleased, DeliveryStatus.Pending);
            }

            PushCompletion(new CompletionEntry
            {
                Slot = sentSlot,
                Generation = (uint)(context >> 32),
                Kind = CompletionKind.Datagram,
                DatagramState = state,
            });
            return;
        }

        if (!TryComplete(context, out int slot))
        {
            return;
        }

        if (_signalFromTransport)
        {
            SignalFinal(slot, MapDatagramState(state));
        }

        PushCompletion(new CompletionEntry
        {
            Slot = slot,
            Generation = (uint)(context >> 32),
            Kind = CompletionKind.Datagram,
            DatagramState = state,
            Canceled = state == DatagramSendState.Canceled,
            Final = true,
        });
    }

    /// <summary>A stream send completion (transport thread); see <see cref="OnTransportDatagramState"/>.</summary>
    /// <param name="context">The entry context.</param>
    /// <param name="canceled">The data was not delivered.</param>
    public void OnTransportStreamCompleted(ulong context, bool canceled)
    {
        if (!TryComplete(context, out int slot))
        {
            return;
        }

        if (_signalFromTransport)
        {
            SignalFinal(slot, MapStreamCompletion(canceled));
        }

        PushCompletion(new CompletionEntry
        {
            Slot = slot,
            Generation = (uint)(context >> 32),
            Kind = CompletionKind.Stream,
            Canceled = canceled,
            Final = true,
        });
    }

    private bool TryComplete(ulong context, out int slot)
    {
        if (Entries.TryTransitionContext(context, SendEntryState.InFlight, SendEntryState.Completed, out slot)
            || Entries.TryTransitionContext(context, SendEntryState.Cancelling, SendEntryState.Completed, out slot))
        {
            return true;
        }

        Counters.StaleCompletions++;
        return false;
    }

    private void SignalFinal(int slot, DeliveryStatus status)
    {
        if ((Entries[slot].Flags & (SendEntryFlags.Tracked | SendEntryFlags.Container | SendEntryFlags.EngineCompletes)) == SendEntryFlags.Tracked)
        {
            SendToken token = _tokens[slot];
            Completions.Complete(token, CompletionStage.BufferReleased, status);
            Completions.Complete(token, CompletionStage.RemoteAccepted, status);
        }
    }

    private void PushCompletion(in CompletionEntry completion)
    {
        if (!CompletionRing.TryEnqueue(in completion))
        {
            // Cannot happen: at most two items per entry (one Sent notice, one final completion) and the ring holds
            // two per entry.
            Counters.CallbackFaults++;
        }

        NoteWork();
    }

    // ------------------------------------------------------------------ reconnect (game thread)

    /// <summary>
    /// Clears everything bound to the transport that was lost, before <see cref="QuiclyPeer.Reconnect"/> attaches a new one
    /// (game thread, while no transport callback can arrive: the old transport reported its close and the new one is not
    /// attached yet). The engines forget their streams (<see cref="ChannelEngine.OnReconnecting"/>), any send entry still
    /// allocated completes <see cref="DeliveryStatus.Disconnected"/> and frees its slot, the hand-off rings, the container
    /// packer and the segment arena are emptied, and admission, the datagram capabilities and a client's session message cap
    /// go back to their pre-handshake values. Kept: the channel table, the engines themselves, every counter, the send and
    /// receive budgets and <see cref="Epoch"/> — the epoch is what the resumed Hello presents as <c>LastEpoch</c>
    /// (PROTOCOL.md §4.1).
    /// </summary>
    public void ResetForReconnect()
    {
        foreach (ChannelEngine engine in _activeEngines)
        {
            engine.OnReconnecting();
        }

        AbandonEntries();
        while (CompletionRing.TryDequeue(out _))
        {
        }

        while (PendedStreams.TryDequeue(out _))
        {
        }

        _localHead = 0;
        _localTail = 0;
        _localCount = 0;
        Transport = null;
        _transportClosing = false;
        _transportClosed = false;
        _admitted = false;
        _datagramsEnabled = false;
        _datagramStatesReported = false;
        _cancelOnBlocked = false;
        _maxDatagramPayload = 0;
        _closeRequest = 0;
        _receiveReserved = 0;
        CurrentSenderTick = 0;
        if (Role == PeerRole.Client)
        {
            // Learned again from the resumed HelloAck; a server keeps the cap from its own options.
            SessionMaxMessageSize = 0;
        }

        Streams.Clear();
        Packer.Reset();
        Segments.Reset();
    }

    /// <summary>
    /// Completes every send entry still allocated <see cref="DeliveryStatus.Disconnected"/> and frees its slot: what the
    /// lost connection left behind after its engines finished their own queues (a Close frame whose completion never came,
    /// a container of a refused pass). Game thread.
    /// </summary>
    private void AbandonEntries()
    {
        for (int slot = 0; slot < Entries.Capacity; slot++)
        {
            if (Entries.GetState(slot) != SendEntryState.Free)
            {
                CompleteEntry(slot, DeliveryStatus.Disconnected);
            }
        }
    }

    // ------------------------------------------------------------------ teardown

    /// <summary>
    /// Frees everything (after the transport can no longer call back): engines, mailboxes and queued receive leases,
    /// payloads of entries still allocated, native tables, and the private allocator. Idempotent.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        foreach (ChannelEngine engine in _activeEngines)
        {
            engine.Dispose();
        }

        foreach (ReceiveMailbox box in _mailboxes)
        {
            box.ReleaseAll(this);
            box.Dispose();
        }

        while (ReceiveRing.TryDequeue(out ReceiveEntry entry))
        {
            ReturnReceive(in entry.Lease);
        }

        for (int slot = 0; slot < Entries.Capacity; slot++)
        {
            if (Entries.GetState(slot) != SendEntryState.Free)
            {
                ReleasePayload(slot);
            }
        }

        Entries.Dispose();
        Segments.Dispose();
        ReceiveRing.Dispose();
        CompletionRing.Dispose();
        PendedStreams.Dispose();
        Completions.Dispose();
        _tokens.Dispose();
        _stamps.Dispose();
        _userContexts.Dispose();
        _sendCounters.Dispose();
        _recvCounters.Dispose();
        if (_ownsAllocator)
        {
            _allocator.Dispose();
        }
    }
}
