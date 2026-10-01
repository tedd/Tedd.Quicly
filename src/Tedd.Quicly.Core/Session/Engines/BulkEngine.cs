using System.Runtime.InteropServices;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Primitives;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Session.Engines;

/// <summary>
/// The <see cref="ChannelMode.Bulk"/> engine (PROTOCOL.md §3.3; docs/design/session-layer.md §7.7): large objects on one
/// unidirectional stream per transfer, at the lowest stream priority, windowed so real-time traffic keeps flowing, with
/// progress, cancel, resume, request authorisation and a whole-object SHA-256.
/// </summary>
/// <remarks>
/// <para><b>Send (game thread).</b> <see cref="BeginBulkSendAsync"/> registers a transfer and answers a
/// <see cref="BulkTransfer"/>; nothing is read from the <see cref="IBulkSource"/> until a scheduler pass reaches it. All
/// send work happens in <see cref="Flush"/>, after every channel's <see cref="ChannelEngine.FlushChannel"/>, because
/// PROTOCOL.md §4.5 schedules bulk <em>after</em> fresh real-time traffic. Each pass opens the transfer's stream if it has
/// none (priority band 0), then submits body pieces while three limits allow: the pass's send cap
/// (<see cref="FlushContext.BudgetBytes"/>), a per-peer rate bucket
/// (<see cref="PeerOptions.BulkMaxBytesPerSecond"/> / <see cref="PeerOptions.BulkShareOfEstimatedBandwidth"/>) and a
/// per-transfer send window of <c>min(IdealSendBufferSize or PeerOptions.BulkSendWindowBytes,
/// BulkShareOfCongestionWindow × cwnd)</c> outstanding wire bytes — stream priority alone cannot protect datagram
/// latency, because datagrams and streams share one congestion window (ARCHITECTURE.md §7).</para>
/// <para><b>Completion.</b> A submission's completion releases its pooled block (PROTOCOL.md §4.3: BufferReleased per
/// chunk) and frees window; the transfer completes <see cref="BulkStatus.Completed"/> when the peer's
/// <c>BulkProgress</c> reaches the range's length, which is the mode's <c>Delivered</c>. A refused stream start
/// (<see cref="TransportStatus.StreamLimitReached"/>) never reached the peer, so the transfer rewinds to the last
/// completed byte and goes out on a new stream once <see cref="PeerCore.StreamCreditGeneration"/> changes; a
/// <c>SubmitStream</c> that fails for any <em>other</em> reason after the open succeeded does not park the transfer on the
/// current credit generation, because nothing took credit and no credit event is coming.</para>
/// <para><b>The channel's stream cap binds the sender too.</b> Both ends hold the same table, so more than
/// <c>max(MaxGroups, 1)</c> streams on a Bulk channel would have the receiver reset a live transfer of ours
/// (PROTOCOL.md §7). The slot is held by the transfer whose stream it is and released on that stream's <em>one</em> close
/// notice (<see cref="OnStreamClosed"/> is called exactly once per stream), which is also when the peer frees its own
/// slot and returns credit.</para>
/// <para><b>Receive</b> is in BulkEngine.Receive.cs: peer-initiated transfers go through the direct-mode receive router
/// (<see cref="PeerOptions.BulkRouter"/>, default reject), their bytes are written progressively into the application's
/// <see cref="IBulkSink"/> — never buffered at the declared size — and the whole-object hash is verified when the
/// transfer carried the whole object.</para>
/// </remarks>
internal sealed unsafe partial class BulkEngine : ChannelEngine, IBulkCancelSink
{
    /// <summary>Bytes reserved in a submission's block for the preamble, the §3.3 header and a chunk header.</summary>
    private const int MaxPrefixBytes = 128;

    /// <summary>
    /// Floor of the bulk rate whenever it is not an explicit <see cref="PeerOptions.BulkMaxBytesPerSecond"/>: a tiny
    /// bandwidth estimate still moves a transfer, and a transport that reports no congestion window at all (with no
    /// <see cref="PeerOptions.MaxSendBytesPerSecond"/> to fall back on) moves bulk at exactly this rate rather than unmetered
    /// — an unmeasured link is not assumed to be a fast one (docs/design/session-layer.md §7.7, see <see cref="RefillBudget"/>).
    /// </summary>
    private const long MinBulkBytesPerSecond = 16 * 1024;

    // Flags of BulkSend.Flags.
    private const byte SendHeaderWritten = 1;
    private const byte SendFinSent = 2;
    private const byte SendCounted = 4;
    private const byte SendFreed = 8;
    private const byte SendCompress = 16;
    private const byte SendChecksum = 32;

    /// <summary>The peer's <c>BulkProgress</c> claimed the whole range at least once (PROTOCOL.md §4.3's <c>Delivered</c>).</summary>
    private const byte SendPeerClaimedAll = 64;

    /// <summary>
    /// The transfer answers the peer's <c>BulkRequest</c> (its id is in <see cref="_servedRequests"/>): abandoned before any
    /// stream of it exists, it is answered <c>BulkReject</c>, because no stream reset can tell the peer (PROTOCOL.md §3.4).
    /// </summary>
    private const byte SendRequested = 128;

    /// <summary>A transfer whose stream was never refused: it may open one as soon as a pass reaches it.</summary>
    private const int CreditUnrefused = int.MinValue;

    private PeerCore _core = null!;
    private ChannelDefinition[] _channels = [];
    private int[] _localOf = [];
    private int[] _denseOf = [];
    private int[] _maxStreams = [];
    private NativeArray<BulkSendState> _send = null!;
    private NativeArray<BulkSend> _records = null!;

    /// <summary>Raw bytes of the chunk being compressed; rented lazily, because only a compressing transfer needs it.</summary>
    private NativeArray<byte>? _scratch;
    private IBulkSource?[] _sources = [];
    private BulkTransfer?[] _transfers = [];
    private XxHash64Builder[] _sendChecksums = [];

    /// <summary>The peer's request id of a transfer flagged <see cref="SendRequested"/> (game thread).</summary>
    private ulong[] _servedRequests = [];
    private TransportStreamId[] _txStreams = [];
    private uint[] _txSerials = [];
    private SpscRing<StreamNotice> _notices = null!;
    private SpscRing<ControlNotice> _control = null!;
    private PendingRequest[] _requests = [];
    private int _freeSend = -1;
    private int _liveSends;
    private ulong _nextTransferId = 1;
    private ulong _nextRequestId = 1;
    private int _chunkBytes;
    private int _bodyBytes;
    private long _windowBytes;
    private double _cwndShare;
    private double _rateShare;
    private long _rateCap;
    private long _rate;
    private TokenBucket _bucket;
    private long _cwnd;
    private long _rttMicros;
    private bool _bucketStarted;

    /// <summary>
    /// Since the last pass began serving the send lists, a transfer was registered or a stream notice was applied (game
    /// thread): the next pass may have something to send that no other level shows.
    /// </summary>
    private bool _sendsChanged;

    /// <summary>
    /// 1 once <see cref="BulkTransfer.Cancel"/> was called for a transfer the next pass has not looked at (any thread sets it,
    /// the pass exchanges it back to 0 before it reads the transfers' flags).
    /// </summary>
    private int _cancelRequested;

    /// <summary>
    /// 1 once the transport thread queued a <c>BulkRequest</c>, <c>BulkCancel</c> or <c>BulkReject</c> the next pass has not
    /// drained: control that brings the flush deadline forward (<see cref="LowerPassDeadline"/>), unlike progress.
    /// </summary>
    private int _urgentControl;

    /// <summary>Where a send transfer is in its lifecycle (game thread).</summary>
    internal enum BulkPhase : byte
    {
        /// <summary>Registered, no stream: the next pass opens one unless it is waiting for stream credit.</summary>
        Waiting = 0,

        /// <summary>The first send (with Start, the preamble and the §3.3 header) is out; nothing more goes out until the start is confirmed.</summary>
        Starting = 1,

        /// <summary>Started: body pieces flow, and the piece carrying the last byte carries FIN.</summary>
        Open = 2,

        /// <summary>The peer's stream limit refused the start; the submission is being cancelled back.</summary>
        Refused = 3,

        /// <summary>Terminal: the transfer completed, failed or was cancelled.</summary>
        Finished = 4,
    }

    private enum NoticeKind : byte
    {
        Started,
        Refused,
        StartFailed,
        Stopped,
        ShutDown,
    }

    private enum ControlKind : byte
    {
        Progress,
        Cancel,
        Reject,
        Request,
    }

    /// <inheritdoc/>
    public override ChannelMode Mode => ChannelMode.Bulk;

    /// <summary>Stream starts the peer's stream limit refused (the transfer goes out again on a new stream; tests).</summary>
    internal long StreamsRefused { get; private set; }

    /// <summary>Control notices dropped because the hand-off ring was full (tests; progress is cumulative, so nothing is lost).</summary>
    internal long ControlNoticesDropped { get; private set; }

    /// <summary>
    /// <c>BulkProgress</c> frames claiming more bytes than this end ever handed to the transport (tests, and
    /// <see cref="PeerStatistics.BulkProgressOverClaims"/>): a hostile peer under ADR 0009, counted, and dropped or closed on
    /// exactly as a sibling control frame with an impossible field is (PROTOCOL.md §3.4).
    /// </summary>
    internal long ProgressOverClaims { get; private set; }

    /// <summary>
    /// The rate the bulk bucket runs at in bytes per second as of the last pass, or 0 when the gate is off because the
    /// estimate has no bound (a window with no measurable RTT; tests; see <see cref="RefillBudget"/>).
    /// </summary>
    internal long RatePerSecond => _rate;

    /// <summary>The largest ideal send buffer the transport has reported for any send stream of this engine, or 0 (tests).</summary>
    internal long LargestIdealSendBuffer
    {
        get
        {
            long largest = 0;
            for (int record = 0; record < _idealSendBuffer.Length; record++)
            {
                largest = Math.Max(largest, Volatile.Read(ref _idealSendBuffer[record]));
            }

            return largest;
        }
    }

    /// <summary>
    /// Checks the sizes of the explicit-layout bulk structs (game thread, at construction). A <c>Size</c> that cuts off the
    /// last field, or an 8-byte field at a misaligned offset, makes the type fail to load at all — and the first touch would
    /// otherwise be inside a transport callback, where a <see cref="TypeLoadException"/> is hardest to diagnose. The expected
    /// sizes are parameters so a test can drive the failure without a deliberately broken struct.
    /// </summary>
    /// <param name="stateSize">Expected size of the per-channel send state.</param>
    /// <param name="sendSize">Expected size of a send transfer's record.</param>
    /// <param name="receiveSize">Expected size of a receive transfer's record.</param>
    /// <exception cref="InvalidOperationException">A struct does not have the size it declares.</exception>
    internal static void AssertLayout(int stateSize = 64, int sendSize = 128, int receiveSize = 192)
    {
        if (sizeof(BulkSendState) != stateSize || sizeof(BulkSend) != sendSize || sizeof(BulkRecv) != receiveSize)
        {
            throw new InvalidOperationException(
                $"The bulk structs do not have their declared layout: BulkSendState {sizeof(BulkSendState)}/{stateSize}, "
                + $"BulkSend {sizeof(BulkSend)}/{sendSize}, BulkRecv {sizeof(BulkRecv)}/{receiveSize} bytes.");
        }
    }

    /// <inheritdoc/>
    public override void Initialize(PeerCore core, ReadOnlySpan<ChannelDefinition> channelsOfMode)
    {
        AssertLayout();
        _core = core;
        int count = channelsOfMode.Length;
        _channels = channelsOfMode.ToArray();
        _localOf = new int[core.ChannelCount];
        Array.Fill(_localOf, -1);
        _denseOf = new int[count];
        _maxStreams = new int[count];
        _send = new NativeArray<BulkSendState>(Math.Max(count, 1));
        for (int local = 0; local < count; local++)
        {
            ChannelDefinition channel = _channels[local];
            int dense = core.ChannelIndexOf(channel.Id);
            _localOf[dense] = local;
            _denseOf[local] = dense;

            // Both ends read the same table, so MaxGroups bounds the streams this end may open on the channel as well as
            // the peer streams it accepts (PROTOCOL.md §7): a sender that exceeded it would have its own transfers reset.
            _maxStreams[local] = Math.Max(channel.MaxGroups, 1);
            ref BulkSendState send = ref _send[local];
            send.ListHead = -1;
            send.ListTail = -1;
        }

        int transfers = Math.Max(core.BulkTransfersPerDirection, 1);
        _records = new NativeArray<BulkSend>(transfers);
        _sources = new IBulkSource?[transfers];
        _transfers = new BulkTransfer?[transfers];
        _sendChecksums = new XxHash64Builder[transfers];
        _servedRequests = new ulong[transfers];
        for (int record = transfers - 1; record >= 0; record--)
        {
            ref BulkSend send = ref _records[record];
            send = default;
            send.Next = _freeSend;
            send.Prev = -1;
            send.Flags = SendFreed;
            send.Phase = BulkPhase.Finished;
            _freeSend = record;
        }

        _txStreams = new TransportStreamId[transfers];
        _txSerials = new uint[transfers];
        // At most three notices per live stream between two drains.
        _notices = new SpscRing<StreamNotice>((4 * transfers) + 8);
        _control = new SpscRing<ControlNotice>(64);
        // PROTOCOL.md §7: BulkTransfersPerDirection transfers, plus one more slot so a range can be asked for while the
        // transfers are still running (three by default; the limit row spells the arithmetic out).
        _requests = new PendingRequest[transfers + 1];
        _chunkBytes = Math.Max(MaxPrefixBytes + 1, Math.Min(core.BulkChunkBytes, core.BulkMaxChunk));
        _bodyBytes = _chunkBytes - MaxPrefixBytes;
        _windowBytes = Math.Max(1, core.BulkSendWindowBytes);
        _cwndShare = core.BulkShareOfCongestionWindow;
        _rateShare = core.BulkShareOfEstimatedBandwidth;
        _rateCap = core.BulkMaxBytesPerSecond;
        InitializeReceive(core, count, transfers);
    }

    /// <summary>Live send transfers of a channel (tests).</summary>
    /// <param name="channelIndex">Dense index of a channel of this engine.</param>
    /// <returns>The number of live transfers.</returns>
    internal int SendTransfersOf(int channelIndex) => _send[_localOf[channelIndex]].Count;

    /// <summary>Send transfers of a channel that hold a stream (bounded by <see cref="ChannelDefinition.MaxGroups"/>; tests).</summary>
    /// <param name="channelIndex">Dense index of a channel of this engine.</param>
    /// <returns>The number of streams open.</returns>
    internal int SendStreamsOf(int channelIndex) => _send[_localOf[channelIndex]].StreamedCount;

    /// <summary>Phases of a channel's live send transfers, oldest first (tests).</summary>
    /// <param name="channelIndex">Dense index of a channel of this engine.</param>
    /// <returns>One phase per live transfer.</returns>
    internal List<BulkPhase> SendPhasesOf(int channelIndex)
    {
        List<BulkPhase> phases = [];
        for (int record = _send[_localOf[channelIndex]].ListHead; record >= 0; record = _records[record].Next)
        {
            phases.Add(_records[record].Phase);
        }

        return phases;
    }

    /// <summary>Outbound range requests waiting for an answer (tests).</summary>
    internal int PendingRequests
    {
        get
        {
            int count = 0;
            foreach (PendingRequest request in _requests)
            {
                if (request.InUse)
                {
                    count++;
                }
            }

            return count;
        }
    }

    // ------------------------------------------------------------------ send admission (game thread)

    /// <inheritdoc/>
    /// <remarks>Bulk channels carry no messages: an object is sent with <see cref="BeginBulkSendAsync"/>.</remarks>
    public override SendStatus Admit(ref SendRequest request) => SendStatus.NotSupported;

    /// <inheritdoc/>
    public override ValueTask<BulkTransfer> BeginBulkSendAsync(
        ChannelDefinition channel, in BulkDescriptor descriptor, IBulkSource source, CancellationToken cancellationToken)
    {
        int dense = _core.ChannelIndexOf(channel.Id);
        int local = dense >= 0 ? _localOf[dense] : -1;
        if (local < 0)
        {
            throw new ArgumentException($"Channel {channel.Id} is not a Bulk channel of this peer.", nameof(channel));
        }

        if (cancellationToken.IsCancellationRequested)
        {
            return ValueTask.FromCanceled<BulkTransfer>(cancellationToken);
        }

        long total = descriptor.TotalLength;
        long offset = descriptor.Offset;
        long length = descriptor.EffectiveLength;

        // Exactly 0 is the "to the end of the object" sentinel, so a negative Length is a caller's arithmetic error and is
        // refused rather than quietly turned into a whole-object transfer.
        if (total < 0 || total > (long)VarInt.MaxValue || offset < 0 || offset > total
            || descriptor.Length < 0 || length <= 0 || length > total - offset)
        {
            throw new ArgumentOutOfRangeException(nameof(descriptor), "A bulk range needs Length >= 0 (0 = to the end) and Offset + Length <= TotalLength <= 2^62-1.");
        }

        int limit = _core.EffectiveMaxMessageSize(channel);
        if (length > limit)
        {
            _core.SendCounters(dense).TooLarge++;
            throw new ArgumentOutOfRangeException(nameof(descriptor), $"A transfer carries at most {limit} bytes on channel {channel.Id} (asked for {length}).");
        }

        BulkTransfer transfer = StartSend(local, dense, in descriptor, length, source, requestId: null, out bool started);
        if (!started)
        {
            // Not connected, or the peer-wide / per-channel limit is reached: the transfer is finished before it began and
            // the application may retry (PROTOCOL.md §7 answers a peer's request the same way, with BulkReject).
            _core.SendCounters(dense).QueueFull++;
        }
        else
        {
            // The next pass opens its stream. (A transfer answering the peer's request starts inside a pass, which serves it
            // at once: no signal there.)
            _core.NoteWork();
        }

        return ValueTask.FromResult(transfer);
    }

    /// <summary>Registers a send transfer, or answers a finished, rejected one when it cannot start now.</summary>
    /// <param name="local">Engine-local index of the channel.</param>
    /// <param name="dense">Dense index of the channel.</param>
    /// <param name="descriptor">The validated range.</param>
    /// <param name="length">Bytes the transfer carries.</param>
    /// <param name="source">Where its bytes come from.</param>
    /// <param name="requestId">The peer's request id when the transfer answers a <c>BulkRequest</c>, else null.</param>
    /// <param name="started">Whether the transfer was registered.</param>
    private BulkTransfer StartSend(
        int local, int dense, in BulkDescriptor descriptor, long length, IBulkSource source, ulong? requestId,
        out bool started)
    {
        DrainNotices();
        started = false;
        if (_core.Peer.State != PeerState.Connected || _freeSend < 0 || _liveSends >= _core.BulkTransfersPerDirection)
        {
            BulkTransfer rejected = new(in descriptor, 0, length);
            rejected.Finish(new BulkResult(BulkStatus.Rejected, 0, QuiclyErrorCode.BulkRejected));
            return rejected;
        }

        int record = _freeSend;
        ref BulkSend send = ref _records[record];
        _freeSend = send.Next;
        uint serial = send.Serial;
        send = default;
        send.Serial = serial;
        send.Local = local;
        send.Next = -1;
        send.Prev = -1;
        send.Phase = BulkPhase.Waiting;
        send.CreditGeneration = CreditUnrefused;
        send.TransferId = _nextTransferId++;
        send.ObjectId = descriptor.ObjectId;
        send.ObjectVersion = descriptor.ObjectVersion;
        send.TotalLength = descriptor.TotalLength;
        send.Offset = descriptor.Offset;
        send.Length = length;
        if (descriptor.Compress)
        {
            send.Flags |= SendCompress;
        }

        if (descriptor.Checksum ?? _core.BulkChecksum)
        {
            send.Flags |= SendChecksum;
            _sendChecksums[record].Reset();
        }

        if (requestId is { } id)
        {
            send.Flags |= SendRequested;
            _servedRequests[record] = id;
        }

        BulkTransfer transfer = new(in descriptor, send.TransferId, length, this, record, serial);
        _sources[record] = source;
        _transfers[record] = transfer;
        Link(local, record);
        _liveSends++;
        started = true;
        _sendsChanged = true;
        return transfer;
    }

    private void Link(int local, int record)
    {
        ref BulkSendState state = ref _send[local];
        ref BulkSend send = ref _records[record];
        send.Prev = state.ListTail;
        send.Next = -1;
        if (state.ListTail < 0)
        {
            state.ListHead = record;
        }
        else
        {
            _records[state.ListTail].Next = record;
        }

        state.ListTail = record;
        state.Count++;
    }

    /// <inheritdoc/>
    void IBulkCancelSink.RequestCancel(int slot, uint serial)
    {
        // Any thread: the flag itself lives in the BulkTransfer the application holds (set before this call), so the next pass
        // sees it. The engine-wide flag is the level behind the signal (HasPassWork) until that pass.
        Volatile.Write(ref _cancelRequested, 1);
        _core.NoteWork();
    }

    // ------------------------------------------------------------------ work for the next pass (level and deadline)

    /// <summary>
    /// Whether this engine has work only its next scheduler pass does, every piece of which raised the work signal when it
    /// was published: the peer's control frames (<c>BulkProgress</c>, <c>BulkRequest</c>, <c>BulkCancel</c>,
    /// <c>BulkReject</c>) and the notices of this end's own streams, handed over by the transport thread; a transfer the
    /// application started, or asked to cancel, since the pass last served the send lists; and the progress this end owes
    /// for bytes it accepted (which may wait out the 100 ms progress window, as ReliableLatest acks wait out AckDelay).
    /// <see cref="QuiclyPeer.HasPendingWork"/> reports it. Game thread; from another thread the answer is advisory (the
    /// started flag and the progress bookkeeping are the game thread's own).
    /// </summary>
    internal bool HasPassWork =>
        !_control.IsEmpty || !_notices.IsEmpty || _sendsChanged || Volatile.Read(ref _cancelRequested) != 0 || OwesProgress();

    /// <summary>
    /// Lowers <paramref name="deadline"/> to when the next pass has the work <see cref="HasPassWork"/> reports (game thread,
    /// the end of a Poll): now for a transfer's discrete events — a range request, cancel or reject from the peer, a notice
    /// of one of this end's streams, a transfer started or cancelled by the application, a retired receive's final progress;
    /// the progress window of a live receive (<see cref="TickProgress"/>, never at or before now) for the bytes it accepted.
    /// The peer's <c>BulkProgress</c> frames (every 64 KiB) do not move it: the send pump does not wait on them, and a flush
    /// per frame would cost a pass per 64 KiB; they are applied by the host's next flush.
    /// </summary>
    /// <param name="now">Clock micros.</param>
    /// <param name="deadline">The flush deadline to lower.</param>
    internal void LowerPassDeadline(long now, ref long deadline)
    {
        if (Volatile.Read(ref _urgentControl) != 0 || !_notices.IsEmpty || _sendsChanged || Volatile.Read(ref _cancelRequested) != 0
            || (_retiredPending < 0 && !_retired.IsEmpty))
        {
            if (now < deadline)
            {
                deadline = now;
            }

            return;
        }

        if (_retiredPending >= 0 || Volatile.Read(ref _recvLive) != 0)
        {
            TickProgress(now, ref deadline);
        }
    }

    // ------------------------------------------------------------------ scheduling (game thread)

    /// <inheritdoc/>
    /// <remarks>Bulk does nothing per channel: PROTOCOL.md §4.5 schedules it after every channel's fresh traffic, in <see cref="Flush"/>.</remarks>
    public override void FlushChannel(int channelIndex, ref FlushContext flush)
    {
    }

    /// <inheritdoc/>
    public override void Flush(ref FlushContext flush)
    {
        DrainNotices();
        if (Volatile.Read(ref _urgentControl) != 0)
        {
            Interlocked.Exchange(ref _urgentControl, 0); // before the drain: a frame queued after it keeps the flag
        }

        DrainControl(ref flush);
        ReadTransport();
        RefillBudget(flush.NowMicros);

        // Every live transfer is looked at below (a transfer a request just started included), and so is every cancel asked
        // for before this point: the exchange is a full fence, so the transfers' own flags are read after it.
        _sendsChanged = false;
        if (Volatile.Read(ref _cancelRequested) != 0)
        {
            Interlocked.Exchange(ref _cancelRequested, 0);
        }

        for (int local = 0; local < _channels.Length; local++)
        {
            FlushTransfers(local, ref flush);
        }

        FlushProgress(ref flush);
    }

    /// <inheritdoc/>
    public override void Tick(long nowMicros, ref long nextDeadline)
    {
        TickProgress(nowMicros, ref nextDeadline);
        if (_liveSends > 0 && _rate > 0 && _bucketStarted)
        {
            // A transfer held back by the rate budget must come back without new application work.
            long wait = _bucket.MicrosUntil(1);
            if (wait > 0 && wait < long.MaxValue)
            {
                LowerDeadline(nowMicros, nowMicros + wait, ref nextDeadline);
            }
        }
    }

    /// <summary>Lowers <paramref name="nextDeadline"/> to <paramref name="deadline"/>, never to a time at or before now.</summary>
    private static void LowerDeadline(long nowMicros, long deadline, ref long nextDeadline)
    {
        if (deadline <= nowMicros)
        {
            deadline = nowMicros + 1;
        }

        if (deadline < nextDeadline)
        {
            nextDeadline = deadline;
        }
    }

    private void FlushTransfers(int local, ref FlushContext flush)
    {
        ref BulkSendState state = ref _send[local];
        if (state.ListHead < 0)
        {
            return;
        }

        bool noMoreStreams = false;
        int record = state.ListHead;
        while (record >= 0)
        {
            // Read the link first: a finished transfer leaves the list inside the loop.
            int next = _records[record].Next;
            if (!TryCancelRequested(local, record))
            {
                BulkPhase phase = _records[record].Phase;
                if (phase == BulkPhase.Waiting)
                {
                    if (noMoreStreams || state.StreamedCount >= _maxStreams[local]
                        || (_records[record].CreditGeneration != CreditUnrefused && _records[record].CreditGeneration == _core.StreamCreditGeneration))
                    {
                        // Every later transfer of the channel would wait on the same credit, so the pass stops opening here.
                        noMoreStreams = true;
                        record = next;
                        continue;
                    }

                    if (!SubmitPiece(local, record, ref state, ref flush))
                    {
                        noMoreStreams |= _records[record].Phase == BulkPhase.Waiting && _records[record].CreditGeneration != CreditUnrefused;
                    }
                }
                else if (phase == BulkPhase.Open)
                {
                    while (SubmitPiece(local, record, ref state, ref flush))
                    {
                    }
                }
            }

            // No ReleaseIfDone here: a record becomes releasable only when it finishes (TerminateSend), when its last piece
            // completes (OnSendCompleted) or when its stream's close notice returns the slot (DrainNotices), and each of
            // those releases it itself — one call per event, not a second one per pass.
            record = next;
        }
    }

    /// <summary>Acts on a <see cref="BulkTransfer.Cancel"/> the application asked for; true when the transfer is finished.</summary>
    private bool TryCancelRequested(int local, int record)
    {
        if (_transfers[record] is not { CancelRequested: true } transfer || transfer.IsFinished)
        {
            return _records[record].Phase == BulkPhase.Finished;
        }

        // The stream reset *is* the signal: the peer's receive side ends the transfer on it, with the code saying it was
        // cancelled (docs/design/session-layer.md §7.7). No BulkCancel goes out from here — that frame is receiver-to-sender
        // only (PROTOCOL.md §3.4), because transfer ids are scoped per direction and one sent the other way would name a
        // transfer of the peer's own.
        TerminateSend(local, record, BulkStatus.Canceled, QuiclyErrorCode.BulkCanceled);
        return true;
    }

    /// <summary>
    /// Hands one body piece of a transfer to its stream, opening the stream first when it has none and setting FIN on the
    /// piece that carries the last byte. Returns <see langword="false"/> when this transfer can send no more in this pass.
    /// </summary>
    private bool SubmitPiece(int local, int record, ref BulkSendState state, ref FlushContext flush)
    {
        ref BulkSend send = ref _records[record];
        long remaining = send.Length - send.BytesRead;
        if (remaining <= 0)
        {
            return false;
        }

        if (flush.BudgetBytes <= 0)
        {
            flush.BudgetExhausted = true;
            return false;
        }

        // The transport may have published a window for this stream since the last pass (advisory; see _idealSendBuffer).
        send.IdealSendBuffer = Volatile.Read(ref _idealSendBuffer[record]);
        long window = WindowOf(in send) - send.WireOutstanding;
        if (window <= 0)
        {
            return false;
        }

        long allowance = _rate > 0 ? _bucket.Available(flush.NowMicros) : long.MaxValue;
        if (allowance <= 0)
        {
            return false;
        }

        bool opening = send.Phase == BulkPhase.Waiting;

        // The three gates above decide *whether* another piece goes out; the piece itself is sized from the object alone.
        // Sizing it from them would be wrong for a chunked body, whose wire cost is a fraction of the bytes it carries — a
        // 64 KiB chunk of compressible data costs a few hundred bytes — so a window in wire bytes would cut the piece to a
        // few hundred object bytes and turn one transfer into thousands of chunks. Each gate is charged the wire bytes the
        // piece really cost, and the last piece of a pass may overdraw, as §7.1's budget rule allows. Both operands are
        // positive (remaining was checked above, and _bodyBytes is at least one byte by construction), so the piece is too.
        int body = (int)Math.Min(remaining, _bodyBytes);

        ChannelDefinition channel = _channels[local];
        if (!_core.TryAllocateEntry(channel.Id, SendEntryFlags.None, out int slot))
        {
            return false;
        }

        if (!_core.TryRentSend(_chunkBytes, out BufferLease lease))
        {
            _core.DiscardEntry(slot);
            return false;
        }

        Span<byte> block = _core.GetSpan(in lease);
        int written = 0;
        if (opening)
        {
            written = WriteHeader(block, in send, channel);
        }

        // The piece may still be refused below, and its bytes are then read — and hashed — again next pass, so the
        // checksum is snapshotted here and restored on every path that does not commit.
        bool checksummed = (send.Flags & SendChecksum) != 0;
        XxHash64Builder savedChecksum = checksummed ? _sendChecksums[record] : default;

        int encoded = EncodeBody(record, block.Slice(written), in send, body, out int decoded);
        if (encoded < 0)
        {
            // The source ran dry: the range it promised does not exist any more (PROTOCOL.md §3.3 has no way to shorten a
            // transfer once its header is out), so the transfer fails and its stream is reset.
            _core.ReturnSend(in lease);
            _core.DiscardEntry(slot);
            TerminateSend(local, record, BulkStatus.Failed, QuiclyErrorCode.InternalError);
            return false;
        }

        written += encoded;
        bool fin = send.BytesRead + decoded >= send.Length;
        if (fin && checksummed)
        {
            // PROTOCOL.md §3.3: the range's xxHash64 follows its last body byte. MaxPrefixBytes leaves far more headroom
            // than this now that the header carries no hash, so the trailer always fits the piece that closes the range.
            written += StreamFraming.WriteBulkChecksum(block.Slice(written), _sendChecksums[record].Digest());
        }

        if (opening && !TryOpenStream(record, ref send, channel))
        {
            _core.ReturnSend(in lease);
            _core.DiscardEntry(slot);
            if (checksummed)
            {
                _sendChecksums[record] = savedChecksum;
            }

            return false;
        }

        _core.AttachLease(slot, in lease, written);
        SendEntryTable entries = _core.Entries;
        ref SendEntry entry = ref entries[slot];
        entry.Aux0 = MakeTag(record, send.Serial);
        entry.Aux1 = ((long)written << 32) | (uint)decoded;
        _core.StampAdmission(slot);
        TransportSendFlags flags = opening ? TransportSendFlags.Start : TransportSendFlags.None;
        if (fin)
        {
            flags |= TransportSendFlags.Fin;
            entry.Flags |= SendEntryFlags.Fin;
        }

        // Datagrams the scheduler handed to the packer earlier in this pass (every real-time channel) leave first.
        _core.Packer.SubmitPending(ref flush);
        TransportStatus status = _core.SubmitStream(send.Stream, entries.GetSegments(slot) + 1, 1, slot, flags);
        if (status != TransportStatus.Success)
        {
            // No completion follows a refused call: the bytes were never read, so nothing has to be rewound — except the
            // checksum, which did take them and will take them again when the piece is retried.
            _core.DiscardEntry(slot);
            if (checksummed)
            {
                _sendChecksums[record] = savedChecksum;
            }

            if (opening)
            {
                AbandonStream(ref send);
                if (status == TransportStatus.StreamLimitReached)
                {
                    // Retain the generation captured before this start: a concurrent grant must enable its retry.
                    StreamsRefused++;
                }
                else
                {
                    // The open succeeded and only the send failed: no stream limit refused this transfer, so it must not
                    // wait for credit it already has — nothing took credit, so no credit event is coming.
                    send.CreditGeneration = CreditUnrefused;
                }
            }

            return false;
        }

        send.BytesRead += decoded;
        send.WireOutstanding += written;
        send.CarriersOutstanding++;
        if (fin)
        {
            send.Flags |= SendFinSent;
        }

        if (opening)
        {
            send.Phase = BulkPhase.Starting;
            send.Flags |= SendCounted | SendHeaderWritten;
            state.StreamedCount++;
        }

        if (_rate > 0)
        {
            _bucket.Consume(written);
        }

        flush.BudgetBytes -= written;
        flush.BytesSubmitted += written;
        ref ChannelSendCounters counters = ref _core.SendCounters(_denseOf[local]);
        counters.Sent++;
        counters.Bytes += decoded;
        PeerCounters peerCounters = _core.Counters;
        peerCounters.StreamSends++;
        peerCounters.StreamBytesSent += written;

        // Nothing more goes out until the start is confirmed (a refused start comes back and is sent again).
        return !opening && send.BytesRead < send.Length;
    }

    /// <summary>Writes the preamble and the PROTOCOL.md §3.3 header of a transfer's first piece.</summary>
    private static int WriteHeader(Span<byte> destination, in BulkSend send, ChannelDefinition channel)
    {
        int written = StreamFraming.WritePreamble(destination, channel.Id);
        BulkHeader header = new()
        {
            TransferId = send.TransferId,
            ObjectId = send.ObjectId,
            ObjectVersion = send.ObjectVersion,
            TotalLength = (ulong)send.TotalLength,
            Offset = (ulong)send.Offset,
            Length = (ulong)send.Length,
            Flags = ((send.Flags & SendChecksum) != 0 ? BulkFlags.ChecksumPresent : BulkFlags.None)
                | ((send.Flags & SendCompress) != 0 ? BulkFlags.Chunked : BulkFlags.None),
        };

        return written + StreamFraming.WriteBulkHeader(destination.Slice(written), in header);
    }

    /// <summary>
    /// Encodes up to <paramref name="body"/> object bytes into <paramref name="destination"/>: raw for an unchunked body,
    /// otherwise one chunk (<c>ChunkLength, RawLength, bytes</c>, PROTOCOL.md §3.3). Returns the bytes written, or −1 when
    /// the source ran dry; <paramref name="decoded"/> is the object bytes the piece carries.
    /// </summary>
    private int EncodeBody(int record, Span<byte> destination, in BulkSend send, int body, out int decoded)
    {
        decoded = 0;
        long offset = send.Offset + send.BytesRead;
        IBulkSource source = _sources[record]!;
        if ((send.Flags & SendCompress) == 0)
        {
            int read = source.Read(offset, destination.Slice(0, body));
            if (read <= 0)
            {
                return -1;
            }

            decoded = read;
            if ((send.Flags & SendChecksum) != 0)
            {
                _sendChecksums[record].Append(destination.Slice(0, read));
            }

            return read;
        }

        // Only a compressing transfer needs a scratch block (the source writes raw bytes that LZ4 then reads), so a peer
        // that never compresses a bulk object never holds one.
        _scratch ??= new NativeArray<byte>(_bodyBytes);
        Span<byte> raw = _scratch.AsSpan(0, body);
        int taken = source.Read(offset, raw);
        if (taken <= 0)
        {
            return -1;
        }

        decoded = taken;
        raw = raw.Slice(0, taken);

        // The checksum covers the range's *decoded* bytes, so it takes the raw chunk before LZ4 sees it — which is also
        // what lets the receiver check it against the bytes it hands the application rather than the bytes on the wire.
        if ((send.Flags & SendChecksum) != 0)
        {
            _sendChecksums[record].Append(raw);
        }

        int headerLength = StreamFraming.GetBulkChunkHeaderLength(taken, taken);
        int compressed = 0;
        if (taken >= 2)
        {
            // PROTOCOL.md §8: a compressed chunk is strictly shorter than its RawLength, so the destination is one byte short.
            compressed = Lz4Block.Compress(raw, destination.Slice(headerLength, taken - 1));
        }

        if (compressed > 0)
        {
            int prefix = StreamFraming.WriteBulkChunkHeader(destination, compressed, taken);
            if (prefix != headerLength)
            {
                // The two varints are sized from (taken, taken), which is never shorter than (compressed, taken).
                destination.Slice(headerLength, compressed).CopyTo(destination.Slice(prefix));
            }

            return prefix + compressed;
        }

        int written = StreamFraming.WriteBulkChunkHeader(destination, taken, 0);
        raw.CopyTo(destination.Slice(written));
        return written + taken;
    }

    /// <summary>The outstanding wire bytes one transfer may keep on its stream (ARCHITECTURE.md §7).</summary>
    private long WindowOf(in BulkSend send)
    {
        long window = send.IdealSendBuffer > 0 ? send.IdealSendBuffer : _windowBytes;
        if (_cwnd > 0)
        {
            long share = (long)(_cwnd * _cwndShare);
            window = Math.Min(window, Math.Max(1, share));
        }

        return window;
    }

    /// <summary>
    /// Reads the transport's congestion window and RTT <b>once</b> per pass: the send window and the rate budget both need
    /// them, and one statistics call per flush is enough (ADR 0008 invariant 9 reads the clock once per pass for the same
    /// reason).
    /// </summary>
    private void ReadTransport()
    {
        // A transport that is gone reports nothing, which leaves both at zero: no window to share and no rate to derive.
        ITransport? transport = _core.Transport;
        TransportStatistics statistics = default;
        if (transport is not null && !_core.IsTransportClosed)
        {
            transport.GetStatistics(out statistics);
        }

        _cwnd = statistics.CongestionWindowBytes;
        _rttMicros = statistics.RttMicros;
    }

    /// <summary>
    /// Bulk's share of the estimated bandwidth (PROTOCOL.md §4.5, docs/design/session-layer.md §7.7). An explicit
    /// <see cref="PeerOptions.BulkMaxBytesPerSecond"/> is taken as it is — an explicit cap is not floored. Otherwise:
    /// <list type="bullet">
    /// <item>With a congestion window and an RTT, the estimate is the window divided by the RTT; bulk gets
    /// <see cref="PeerOptions.BulkShareOfEstimatedBandwidth"/> of it, floored at <see cref="MinBulkBytesPerSecond"/> so a
    /// tiny estimate still moves a transfer.</item>
    /// <item>With <b>no</b> congestion window, the estimate falls back to <see cref="PeerOptions.MaxSendBytesPerSecond"/>;
    /// with no send cap either there is no estimate at all, and the floor is the rate: a transport that measures nothing is
    /// not assumed to be fast, and a transfer still always progresses.</item>
    /// <item>A window with an RTT under a microsecond (a loopback or in-memory link) is an estimate without a bound — the
    /// window crosses in no time — so no rate is derived and the gate is off; the pass's send cap and the transfer's send
    /// window still bound the traffic.</item>
    /// </list>
    /// Every rate is clamped to <see cref="TokenBucket.MaxRatePerSecond"/>, far above any real link, so the bucket's integer
    /// arithmetic stays in range.
    /// </summary>
    private void RefillBudget(long now)
    {
        long rate = _rateCap;
        if (rate == 0 && !(_cwnd > 0 && _rttMicros <= 0))
        {
            long estimate = _cwnd > 0 ? _cwnd * 1_000_000 / _rttMicros : _core.MaxSendBytesPerSecond;
            rate = Math.Max(MinBulkBytesPerSecond, (long)(estimate * _rateShare));
        }

        rate = Math.Min(rate, TokenBucket.MaxRatePerSecond);
        if (rate == _rate)
        {
            return;
        }

        _rate = rate;
        if (rate > 0)
        {
            // The level is carried across the change, not refilled: a rate derived from the congestion window moves on
            // every pass, and re-initialising the bucket would hand out a full burst each time, so the cap would never bind.
            _bucket.SetRate(rate, Math.Max(1, rate / 10), now);
            _bucketStarted = true;
        }
    }

    private static long MakeTag(int record, uint serial) => ((long)record << 24) | (serial & PeerCore.EngineStreamSerialMask);

    private bool TryOpenStream(int record, ref BulkSend send, ChannelDefinition channel)
    {
        int credit = _core.StreamCreditGeneration;
        uint serial = (send.Serial + 1) & PeerCore.EngineStreamSerialMask;
        ulong context = PeerCore.MakeEngineStreamContext(ChannelMode.Bulk, record, serial);

        // PROTOCOL.md §4.5: Bulk streams get priority band 0, below every real-time channel.
        TransportStatus status = _core.OpenStream(StreamKind.Unidirectional, context, 0, out TransportStreamId id);
        if (status != TransportStatus.Success)
        {
            if (status == TransportStatus.StreamLimitReached)
            {
                send.CreditGeneration = credit;
                StreamsRefused++;
            }

            return false;
        }

        send.Stream = id;
        send.Serial = serial;
        send.CreditGeneration = credit;
        send.CarriersOutstanding = 0;
        send.IdealSendBuffer = 0;
        Volatile.Write(ref _idealSendBuffer[record], 0);
        return true;
    }

    /// <summary>
    /// The peer's stream limit refused the transfer's start and every piece sent with it has come back canceled (game
    /// thread). Nothing reached the peer, so the transfer rewinds to its last completed byte, forgets that its header went
    /// out and waits for a <em>new</em> stream once the peer grants credit (docs/design/session-layer.md §7.7). The refusal
    /// notice and the canceled completions arrive in either order, and whichever is last lands here.
    /// </summary>
    private void RewindRefused(int record, ref BulkSend send, ref BulkSendState state)
    {
        // Nothing of this transfer reached the peer, so its checksum starts over with the bytes that are about to be read
        // again. The rewind target is BytesCompleted, which is 0 here by construction: a transfer has one stream, and a
        // refused start means none of its sends completed.
        if ((send.Flags & SendChecksum) != 0)
        {
            _sendChecksums[record].Reset();
        }

        send.BytesRead = send.BytesCompleted;
        send.WireOutstanding = 0;
        send.Phase = BulkPhase.Waiting;
        send.Stream = default;
        send.Flags = (byte)(send.Flags & ~(SendHeaderWritten | SendFinSent));
        ReleaseStreamSlot(ref send, ref state);
    }

    /// <summary>Releases a stream that never started (no accepted send carried Start): no callback follows for it.</summary>
    private void AbandonStream(ref BulkSend send)
    {
        _core.Transport?.CloseStream(send.Stream);
        send.Stream = default;
        send.Phase = BulkPhase.Waiting;

        // The header must be written again on the new stream.
        send.Flags = (byte)(send.Flags & ~(SendHeaderWritten | SendFinSent));
    }

    private void AbortSendStream(ref BulkSend send, QuiclyErrorCode code)
    {
        if (send.Stream.IsValid)
        {
            _core.Transport?.AbortStream(send.Stream, (ulong)code, StreamAbortDirection.Send);
        }
    }

    // ------------------------------------------------------------------ completions (game thread)

    /// <inheritdoc/>
    public override void OnSendCompleted(int entrySlot, in CompletionEntry completion)
    {
        if (!completion.Final)
        {
            return;
        }

        DrainNotices();
        SendEntryTable entries = _core.Entries;
        ref SendEntry entry = ref entries[entrySlot];
        long tag = entry.Aux0;
        int record = (int)(tag >> 24);
        uint serial = (uint)(tag & PeerCore.EngineStreamSerialMask);
        int wire = (int)(entry.Aux1 >> 32);
        int decoded = (int)(uint)entry.Aux1;
        if ((uint)record >= (uint)_records.Length)
        {
            _core.CompleteEntry(entrySlot, _core.MapCompletion(in completion));
            return;
        }

        ref BulkSend send = ref _records[record];
        int local = send.Local;
        ref BulkSendState state = ref _send[local];
        bool current = serial == send.Serial && (send.Flags & SendFreed) == 0;
        if (current)
        {
            send.CarriersOutstanding--;
            send.WireOutstanding -= wire;
        }

        // The piece's block goes back here: PROTOCOL.md §4.3 releases a bulk buffer per chunk.
        _core.CompleteEntry(entrySlot, completion.Canceled ? _core.MapCompletion(in completion) : DeliveryStatus.Delivered);
        if (!current)
        {
            return;
        }

        if (!completion.Canceled)
        {
            send.BytesCompleted += decoded;

            // The peer may have confirmed the whole range before this last piece's completion came back (both are triggered
            // by the same round trip, so the order is racy); the transfer completes at whichever of the two arrives last.
            if (TryCompleteSend(local, record))
            {
                return;
            }
        }
        else if (_core.IsTransportClosing)
        {
            TerminateSend(local, record, BulkStatus.Disconnected, QuiclyErrorCode.NoError);
            return;
        }
        else if (send.Phase == BulkPhase.Refused && send.CarriersOutstanding == 0)
        {
            // The refusal notice came first and this was the last piece to come back canceled (the simulator's order).
            RewindRefused(record, ref send, ref state);
        }

        // A piece canceled for any other reason means the stream is going away, but not yet *why*: a peer that stopped it
        // with BulkCanceled ends the transfer Canceled, anything else Failed. The transport reports a canceled send before
        // the stop that caused it just as often as after, so the verdict is left to the stream's close notice, which the
        // peer delivers exactly once for every stream (docs/design/session-layer.md §4.3).
        ReleaseIfDone(record, ref state);
    }

    /// <inheritdoc/>
    /// <remarks>A bulk transfer is cancelled through <see cref="BulkTransfer.Cancel"/>, not through a send token.</remarks>
    public override bool TryCancel(int entrySlot) => false;

    /// <inheritdoc/>
    /// <remarks>
    /// A bulk transfer is never a <see cref="QuiclyPeer.FlushAsync"/> watermark: it is minutes of traffic under a rate cap,
    /// so waiting for it would turn <c>FlushAsync</c> into "wait for the object".
    /// </remarks>
    public override long OldestQueuedStamp() => long.MaxValue;

    /// <inheritdoc/>
    public override void AddStatistics(int channelIndex, ref ChannelStatistics statistics)
    {
        ref BulkSendState state = ref _send[_localOf[channelIndex]];
        for (int record = state.ListHead; record >= 0; record = _records[record].Next)
        {
            ref BulkSend send = ref _records[record];
            statistics.QueuedMessages++;
            statistics.QueuedBytes += Math.Max(0, send.Length - send.BytesRead);
            statistics.InFlightBytes += send.WireOutstanding;
            if (send.CarriersOutstanding > 0)
            {
                statistics.InFlightMessages += send.CarriersOutstanding;
            }
        }
    }

    /// <summary>
    /// The single exit of a send transfer (docs/design/session-layer.md §7.7). It resets the transfer's stream when this end
    /// never finished sending on it, finishes the application's transfer exactly once, and gives back the record and the
    /// channel's stream slot exactly once. Every terminal path goes through it — the peer's <c>BulkCancel</c>, the
    /// application's <see cref="BulkTransfer.Cancel"/>, a source that ran dry, a completed transfer, a stream notice and a
    /// closed connection — because a path that released the slot without ending the stream would leak that stream for the
    /// life of the connection while the engine's own accounting said the channel was free, and the next pass would then open
    /// more streams than the table allows (PROTOCOL.md §7).
    /// </summary>
    /// <param name="local">Engine-local index of the transfer's channel.</param>
    /// <param name="record">The transfer's record.</param>
    /// <param name="status">How the transfer ended.</param>
    /// <param name="code">The error code involved.</param>
    private void TerminateSend(int local, int record, BulkStatus status, QuiclyErrorCode code)
    {
        ref BulkSend send = ref _records[record];
        if ((send.Flags & SendFreed) == 0 && send.Phase != BulkPhase.Finished && (send.Flags & SendFinSent) == 0)
        {
            // Nothing carried FIN, so the peer would wait for bytes that never come and the stream would stay open: reset
            // it. A transfer that did send FIN leaves its stream to shut down by itself.
            AbortSendStream(ref send, code);
        }

        if ((send.Flags & (SendRequested | SendHeaderWritten)) == SendRequested && send.Phase != BulkPhase.Finished
            && status is BulkStatus.Canceled or BulkStatus.Failed)
        {
            // PROTOCOL.md §3.4: the peer asked for this range and no stream of it exists — the header never went out, or
            // its start was refused and rewound — so no reset can tell the peer. Its request is answered BulkReject
            // instead; otherwise it would wait for an answer for the life of the connection. A disconnect sends nothing.
            send.Flags = (byte)(send.Flags & ~SendRequested);
            SendBulkReject(_servedRequests[record], code);
        }

        FinishSend(local, record, status, code);
        ReleaseIfDone(record, ref _send[local]);
    }

    /// <summary>
    /// Completes a transfer once <b>both</b> ends are done with it (PROTOCOL.md §4.3): the peer confirmed the whole range
    /// with <c>BulkProgress</c> <em>and</em> this end read the range, put every byte on the wire with FIN and saw every
    /// piece complete. Confirmation alone is not enough — it is a claim by the peer, and a hostile one would otherwise
    /// report an object as delivered that this end never read (ADR 0009).
    /// </summary>
    /// <param name="local">Engine-local index of the transfer's channel.</param>
    /// <param name="record">The transfer's record.</param>
    /// <returns><see langword="true"/> when the transfer is now finished.</returns>
    private bool TryCompleteSend(int local, int record)
    {
        ref BulkSend send = ref _records[record];
        if (send.Phase == BulkPhase.Finished || (send.Flags & (SendPeerClaimedAll | SendFinSent)) != (SendPeerClaimedAll | SendFinSent)
            || send.BytesCompleted < send.Length)
        {
            return false;
        }

        send.BytesAcked = send.Length;
        _transfers[record]?.Advance(send.Length);
        TerminateSend(local, record, BulkStatus.Completed, QuiclyErrorCode.NoError);
        return true;
    }

    /// <summary>Finishes a send transfer once: its application transfer completes and its stream slot goes back.</summary>
    private void FinishSend(int local, int record, BulkStatus status, QuiclyErrorCode code)
    {
        ref BulkSend send = ref _records[record];
        if (send.Phase == BulkPhase.Finished)
        {
            return;
        }

        send.Phase = BulkPhase.Finished;

        // The slot belongs to the transfer whose stream it is and comes back on that stream's *one* close notice (§4.3),
        // which is also when the peer frees its own slot and returns credit; releasing it while the stream is still alive
        // would let the next pass open one stream too many. A transfer that holds no stream releases it here.
        if (!send.Stream.IsValid)
        {
            ReleaseStreamSlot(ref send, ref _send[local]);
        }

        _transfers[record]?.Finish(new BulkResult(status, send.BytesAcked, code));
    }

    /// <summary>Gives back the channel's bulk-stream slot when a transfer stops holding a stream (at most once per open).</summary>
    private static void ReleaseStreamSlot(ref BulkSend send, ref BulkSendState state)
    {
        if ((send.Flags & SendCounted) != 0)
        {
            send.Flags = (byte)(send.Flags & ~SendCounted);
            state.StreamedCount--;
        }
    }

    /// <summary>
    /// Returns a finished transfer whose pieces have all completed to the free list. Idempotent: a record already on the
    /// free list is left alone, so a second close notice for one stream could never release it twice.
    /// </summary>
    private void ReleaseIfDone(int record, ref BulkSendState state)
    {
        ref BulkSend send = ref _records[record];
        if ((send.Flags & SendFreed) != 0 || send.Phase != BulkPhase.Finished || send.CarriersOutstanding > 0)
        {
            return;
        }

        // A transfer that still holds a stream keeps its record: the slot (and the peer's credit) return when the stream
        // shuts down, which is also when the receiving end frees its own slot.
        if ((send.Flags & SendCounted) != 0)
        {
            return;
        }

        int previous = send.Prev;
        int after = send.Next;
        if (previous < 0)
        {
            state.ListHead = after;
        }
        else
        {
            _records[previous].Next = after;
        }

        if (after < 0)
        {
            state.ListTail = previous;
        }
        else
        {
            _records[after].Prev = previous;
        }

        state.Count--;
        _liveSends--;
        _sources[record] = null;
        _transfers[record] = null;
        send.Stream = default;
        send.Prev = -1;

        // Serials only ever rise for a record, so every notice and every piece tag of the stream it just had is stale.
        send.Serial = (send.Serial + 1) & PeerCore.EngineStreamSerialMask;
        send.Flags = SendFreed;
        send.Next = _freeSend;
        _freeSend = record;
    }

    /// <inheritdoc/>
    public override void OnPeerClosed()
    {
        DrainNotices();
        for (int local = 0; local < _channels.Length; local++)
        {
            ref BulkSendState state = ref _send[local];
            int record = state.ListHead;
            while (record >= 0)
            {
                int next = _records[record].Next;

                // Nothing will shut a stream down after the connection is gone, and no reset can go out on it either, so
                // the stream is forgotten first and the slots go back with the record here.
                _records[record].Stream = default;
                TerminateSend(local, record, BulkStatus.Disconnected, QuiclyErrorCode.NoError);
                record = next;
            }
        }

        // Receive first: a request whose answer was arriving is settled by that transfer (a resumable one keeps the part still
        // missing), so only the requests nothing answered are reported to the router as closed.
        OnPeerClosedReceive();
        ClosePendingRequests(BulkStatus.Disconnected);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Every <see cref="BulkTransfer"/> this end is sending ends <see cref="BulkStatus.Disconnected"/>, so an
    /// <c>await transfer.Completion</c> is released (ADR 0008; PROTOCOL.md §4.3 gives a transfer terminal states so that its
    /// caller always is). Only game-thread state is touched: the streams are forgotten rather than reset (the transport is
    /// closed right after this), the pieces still in flight keep their blocks until the transport is done with them, and the
    /// receive records — the transport thread's — are finished by <see cref="Dispose"/>, once the transport has reported its
    /// close. Pending range requests are dropped without calling the router back: nothing awaits them, and a disposed peer
    /// makes no more callbacks than it must.
    /// </remarks>
    public override void OnDisposing()
    {
        DrainNotices();
        for (int local = 0; local < _channels.Length; local++)
        {
            int record = _send[local].ListHead;
            while (record >= 0)
            {
                int next = _records[record].Next;
                _records[record].Stream = default;
                TerminateSend(local, record, BulkStatus.Disconnected, QuiclyErrorCode.NoError);
                record = next;
            }
        }

        Array.Clear(_requests);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Everything bound to the lost transport goes: the transfers' streams (their pieces already completed
    /// <see cref="DeliveryStatus.Disconnected"/>, and <see cref="OnPeerClosed"/> finished the transfers themselves), the
    /// per-channel stream counts, the notices the transport thread handed over and the half-received transfers. The
    /// session's own rule — resumable ranges re-requested — belongs to <see cref="OnEpochReset"/>, which runs when the
    /// resume is accepted.
    /// </remarks>
    public override void OnReconnecting()
    {
        while (_notices.TryDequeue(out _))
        {
        }

        while (_control.TryDequeue(out _))
        {
        }

        _freeSend = -1;
        for (int record = _records.Length - 1; record >= 0; record--)
        {
            ref BulkSend send = ref _records[record];

            // A backstop, not an exit: the peer settles the lost connection (OnPeerClosed, so TerminateSend for every live
            // transfer) before it resets for the reconnect, so this Finish finds each transfer already finished and is a
            // no-op; the table itself is wiped wholesale below because nothing bound to the old transport survives.
            _transfers[record]?.Finish(new BulkResult(BulkStatus.Disconnected, send.BytesAcked, QuiclyErrorCode.NoError));
            uint serial = (send.Serial + 1) & PeerCore.EngineStreamSerialMask;
            send = default;
            send.Serial = serial;
            send.Phase = BulkPhase.Finished;
            send.Flags = SendFreed;
            send.Prev = -1;
            send.Next = _freeSend;
            _freeSend = record;
            _sources[record] = null;
            _transfers[record] = null;
        }

        for (int local = 0; local < _channels.Length; local++)
        {
            ref BulkSendState state = ref _send[local];
            state.ListHead = -1;
            state.ListTail = -1;
            state.Count = 0;
            state.StreamedCount = 0;
        }

        for (int record = 0; record < _txStreams.Length; record++)
        {
            _txStreams[record] = default;
            _txSerials[record] = 0;
        }

        _liveSends = 0;
        OnReconnectingReceive();
    }

    /// <inheritdoc/>
    /// <remarks>
    /// PROTOCOL.md §4.1: "Bulk transfers marked <c>Resumable</c> are re-requested by the library for the remaining range."
    /// It is the end that <em>asked</em> for a range that re-asks, so every resumable outbound request whose transfer did
    /// not complete goes out again for the bytes still missing, under a fresh request id. Transfer ids are scoped to the
    /// epoch, so the receive side forgets the ids it has seen before the next transfer arrives.
    /// </remarks>
    public override void OnEpochReset(bool resumed)
    {
        if (!resumed)
        {
            return;
        }

        for (int i = 0; i < _requests.Length; i++)
        {
            ref PendingRequest request = ref _requests[i];
            if (!request.InUse || !request.Range.Resumable || request.Range.Length <= 0)
            {
                request = default;
                continue;
            }

            request.RequestId = _nextRequestId++;
            SendBulkRequest(in request);
        }

        ResetReceiveState();
    }

    // ------------------------------------------------------------------ range requests (game thread)

    /// <inheritdoc/>
    public override void RequestBulk(ChannelDefinition channel, in BulkRangeRequest request)
    {
        int dense = _core.ChannelIndexOf(channel.Id);
        int local = dense >= 0 ? _localOf[dense] : -1;
        if (local < 0)
        {
            throw new ArgumentException($"Channel {channel.Id} is not a Bulk channel of this peer.", nameof(channel));
        }

        if (request.Offset < 0 || request.Length <= 0 || request.Offset > (long)VarInt.MaxValue - request.Length)
        {
            throw new ArgumentOutOfRangeException(nameof(request), "A bulk request needs Length > 0 and Offset + Length <= 2^62-1.");
        }

        int slot = -1;
        for (int i = 0; i < _requests.Length; i++)
        {
            if (!_requests[i].InUse)
            {
                slot = i;
                break;
            }
        }

        if (slot < 0)
        {
            // PROTOCOL.md §7 bounds the requests in flight; the application retries.
            _core.SendCounters(dense).QueueFull++;
            return;
        }

        _requests[slot] = new PendingRequest { InUse = true, RequestId = _nextRequestId++, Range = request };
        SendBulkRequest(in _requests[slot]);
    }

    private void SendBulkRequest(in PendingRequest pending)
    {
        BulkRangeRequest range = pending.Range;
        Span<byte> frame = stackalloc byte[64];
        BulkRequest message = new(pending.RequestId, range.Channel, range.ObjectId, range.ObjectVersion, (ulong)range.Offset, (ulong)range.Length);
        if (ControlCodec.TryWrite(frame, in message, out int written))
        {
            _core.SendControlFrame(frame.Slice(0, written), ControlCarrier.Stream);
        }
    }

    private void SendBulkCancel(ulong transferId, QuiclyErrorCode code)
    {
        Span<byte> frame = stackalloc byte[32];
        if (ControlCodec.TryWrite(frame, new BulkCancel(transferId, code), out int written))
        {
            _core.SendControlFrame(frame.Slice(0, written), ControlCarrier.Stream);
        }
    }

    private void SendBulkReject(ulong requestId, QuiclyErrorCode code)
    {
        Span<byte> frame = stackalloc byte[32];
        if (ControlCodec.TryWrite(frame, new BulkReject(requestId, code), out int written))
        {
            _core.SendControlFrame(frame.Slice(0, written), ControlCarrier.Stream);
        }
    }

    /// <summary>
    /// Drops the outbound requests a lost connection cannot answer. A <em>resumable</em> request is kept, because
    /// <see cref="OnEpochReset"/> asks for its remaining range again on the resumed session (PROTOCOL.md §4.1).
    /// </summary>
    private void ClosePendingRequests(BulkStatus status)
    {
        for (int i = 0; i < _requests.Length; i++)
        {
            ref PendingRequest request = ref _requests[i];
            if (request.InUse && (!request.Range.Resumable || status != BulkStatus.Disconnected))
            {
                BulkRangeRequest range = request.Range;
                request = default;
                _core.BulkRouter?.OnRequestRejected(in range, QuiclyErrorCode.NoError);
            }
        }
    }

    // ------------------------------------------------------------------ control messages

    /// <inheritdoc/>
    /// <remarks>
    /// The peer's <c>BulkProgress</c>, <c>BulkCancel</c>, <c>BulkReject</c> and <c>BulkRequest</c> (the frames are already
    /// validated by <see cref="ControlCodec"/>). They change game-thread state — a transfer's acknowledged bytes, an
    /// authorisation decision — so they are handed over through an SPSC ring and applied in the next scheduler pass.
    /// </remarks>
    public override bool OnControl(ControlType type, ReadOnlySpan<byte> body, bool onStream, long nowMicros)
    {
        ConsumeEpochReset();
        ControlNotice notice = default;
        switch (type)
        {
            case ControlType.BulkProgress:
            {
                if (ControlCodec.TryParse(body, out BulkProgress progress) != ControlParseStatus.Ok)
                {
                    return false;
                }

                notice.Kind = ControlKind.Progress;
                notice.Id = progress.TransferId;
                notice.Offset = (long)progress.BytesAccepted;
                notice.OnStream = onStream;
                break;
            }

            case ControlType.BulkCancel:
            {
                // PROTOCOL.md §3.4: types 0x10-0x17 are control-stream only.
                if (!onStream || ControlCodec.TryParse(body, out BulkCancel cancel) != ControlParseStatus.Ok)
                {
                    return false;
                }

                notice.Kind = ControlKind.Cancel;
                notice.Id = cancel.TransferId;
                notice.Code = (uint)cancel.Code;
                break;
            }

            case ControlType.BulkReject:
            {
                if (!onStream || ControlCodec.TryParse(body, out BulkReject reject) != ControlParseStatus.Ok)
                {
                    return false;
                }

                notice.Kind = ControlKind.Reject;
                notice.Id = reject.RequestId;
                notice.Code = (uint)reject.Code;
                break;
            }

            case ControlType.BulkRequest:
            {
                if (!onStream || ControlCodec.TryParse(body, out BulkRequest request) != ControlParseStatus.Ok)
                {
                    return false;
                }

                int dense = _core.ChannelIndexOf(request.Channel);
                if (dense < 0 || _localOf[dense] < 0)
                {
                    // The request names a channel that is not a Bulk channel of this session.
                    return false;
                }

                notice.Kind = ControlKind.Request;
                notice.Channel = request.Channel;
                notice.Id = request.RequestId;
                notice.ObjectId = request.ObjectId;
                notice.ObjectVersion = request.ObjectVersion;
                notice.Offset = (long)request.Offset;
                notice.Length = (long)request.Length;
                break;
            }

            default:
                return false;
        }

        if (!_control.TryEnqueue(in notice))
        {
            // Progress is cumulative, so a dropped one is superseded by the next; a dropped cancel is still carried by the
            // stream reset that accompanies it, and a dropped request or reject is the peer's to retry.
            ControlNoticesDropped++;
            return true;
        }

        if (notice.Kind != ControlKind.Progress)
        {
            Volatile.Write(ref _urgentControl, 1); // after the enqueue, so the pass that clears it drains the frame
        }

        // Transport thread: one signal for every frame a control-stream read or datagram callback carried.
        _core.NoteTransportWork();
        return true;
    }

    /// <summary>Applies the peer's control messages to the send side (game thread, start of every pass).</summary>
    private void DrainControl(ref FlushContext flush)
    {
        while (_control.TryDequeue(out ControlNotice notice))
        {
            switch (notice.Kind)
            {
                case ControlKind.Progress:
                    ApplyProgress(notice.Id, notice.Offset, notice.OnStream);
                    break;
                case ControlKind.Cancel:
                    ApplyCancel(notice.Id, (QuiclyErrorCode)notice.Code);
                    break;
                case ControlKind.Reject:
                    ApplyReject(notice.Id, (QuiclyErrorCode)notice.Code);
                    break;
                default:
                    ApplyRequest(in notice);
                    break;
            }
        }
    }

    /// <summary>
    /// The peer says it accepted bytes of a transfer this end is sending (PROTOCOL.md §4.3: the range's length is
    /// <c>Delivered</c>). The claim is the peer's, so it is bounded by what this end really did, with two different bounds
    /// for two different questions (ADR 0009: a client parses hostile servers too).
    /// <para>
    /// <b>What is a violation</b> is measured against <c>BytesRead</c>, the bytes handed to the transport: a peer cannot
    /// possibly have accepted bytes that were never submitted, so such a frame carries an impossible field and is treated
    /// exactly as its sibling control frames with impossible fields are (PROTOCOL.md §3.4, control-message bounds): on the
    /// control stream it closes the connection <see cref="QuiclyErrorCode.ProtocolViolation"/>, as a control datagram it
    /// is dropped whole and counted (<see cref="PeerStatistics.BulkProgressOverClaims"/>). Dropped means nothing of it
    /// applies — neither the bytes nor the "whole range confirmed" it would imply. A claim that merely runs ahead of a
    /// completion still in flight is ordinary (both are triggered by the same round trip) and is no violation.
    /// </para>
    /// <para>
    /// <b>What is reported</b> is clamped to <c>BytesCompleted</c> — the bytes whose sends completed are the bytes the peer
    /// can have seen — so <see cref="BulkTransfer.BytesTransferred"/> never counts a byte this end did not see complete.
    /// That the peer confirmed the whole range is remembered, so the transfer completes once this end's own send side is
    /// finished too — which is <see cref="TryCompleteSend"/>, and the reason a confirmation alone completes nothing.
    /// </para>
    /// </summary>
    /// <param name="transferId">The transfer the peer named.</param>
    /// <param name="bytesAccepted">Bytes it claims to have accepted.</param>
    /// <param name="onStream">The frame came on the control stream (a violation closes) rather than as a datagram (dropped).</param>
    private void ApplyProgress(ulong transferId, long bytesAccepted, bool onStream)
    {
        int record = FindSend(transferId);
        if (record < 0)
        {
            // A transfer that already ended, or one this end never sent: a late or stray frame, ignored.
            return;
        }

        ref BulkSend send = ref _records[record];
        if (bytesAccepted > send.BytesRead)
        {
            ProgressOverClaims++;
            _core.Counters.BulkProgressOverClaims++;
            if (onStream)
            {
                _core.RequestClose(QuiclyErrorCode.ProtocolViolation);
            }

            return;
        }

        if (send.Phase == BulkPhase.Finished)
        {
            // A late frame for a transfer that already ended (its record waits for pieces or its stream): its outcome and its
            // byte count were published when it finished and must not move afterwards.
            return;
        }

        // BytesRead never exceeds Length, so an honest claim of the whole range is exactly Length.
        if (bytesAccepted == send.Length)
        {
            send.Flags |= SendPeerClaimedAll;
        }

        long credited = Math.Min(bytesAccepted, send.BytesCompleted);

        // A progress frame is cumulative; an older, torn or over-stated one never moves the count backwards.
        if (credited > send.BytesAcked)
        {
            send.BytesAcked = credited;
            _transfers[record]?.Advance(credited);
        }

        TryCompleteSend(send.Local, record);
    }

    /// <summary>
    /// The peer asks this end to stop sending a transfer (PROTOCOL.md §3.4: <c>BulkCancel</c> is receiver-to-sender, so it
    /// always names a transfer of <em>this</em> end's send side, which is what makes resolving it against the send records
    /// correct).
    /// </summary>
    /// <param name="transferId">The transfer this end is sending.</param>
    /// <param name="code">The peer's code.</param>
    private void ApplyCancel(ulong transferId, QuiclyErrorCode code)
    {
        int record = FindSend(transferId);
        if (record < 0 || _records[record].Phase == BulkPhase.Finished)
        {
            // PROTOCOL.md §3.4: a BulkCancel naming no transfer this end is sending is ignored and counted. One that crossed
            // its transfer's completion on the wire lands here as well.
            _core.Counters.BulkCancelsIgnored++;
            return;
        }

        // The peer asked for the stop, so it needs no BulkReject for a range it requested: the reset below is the answer.
        _records[record].Flags = (byte)(_records[record].Flags & ~SendRequested);
        TerminateSend(_records[record].Local, record, BulkStatus.Canceled, code);
    }

    private void ApplyReject(ulong requestId, QuiclyErrorCode code)
    {
        for (int i = 0; i < _requests.Length; i++)
        {
            ref PendingRequest request = ref _requests[i];
            if (request.InUse && request.RequestId == requestId)
            {
                BulkRangeRequest range = request.Range;
                request = default;
                _core.BulkRouter?.OnRequestRejected(in range, code);
                return;
            }
        }
    }

    /// <summary>Authorises a range the peer asked for and starts serving it (game thread; default deny).</summary>
    private void ApplyRequest(in ControlNotice notice)
    {
        // OnControl queued only a request naming a Bulk channel of this table, and the table never changes.
        int dense = _core.ChannelIndexOf(notice.Channel);
        int local = _localOf[dense];

        BulkRequestInfo info = new()
        {
            Channel = notice.Channel,
            RequestId = notice.Id,
            ObjectId = notice.ObjectId,
            ObjectVersion = notice.ObjectVersion,
            Offset = notice.Offset,
            Length = notice.Length,
            PeerIndex = _core.Peer.Index,
        };

        // ADR 0009: serving bulk objects is opt-in, so no authorizer (or no provider) means every request is refused.
        if (_core.BulkAuthorizer is not { } authorizer || !authorizer.Authorize(in info)
            || _core.BulkProvider is not { } provider || !provider.TryGetObject(in info, out BulkDescriptor descriptor, out IBulkSource? source)
            || source is null)
        {
            SendBulkReject(notice.Id, QuiclyErrorCode.BulkRejected);
            return;
        }

        ChannelDefinition channel = _channels[local];
        long length = descriptor.EffectiveLength;
        if (descriptor.Channel != channel.Id || descriptor.TotalLength < 0 || descriptor.TotalLength > (long)VarInt.MaxValue
            || descriptor.Offset < 0 || descriptor.Offset > descriptor.TotalLength || descriptor.Length < 0 || length <= 0
            || length > descriptor.TotalLength - descriptor.Offset || length > _core.EffectiveMaxMessageSize(channel))
        {
            SendBulkReject(notice.Id, QuiclyErrorCode.BulkRejected);
            return;
        }

        StartSend(local, dense, in descriptor, length, source, notice.Id, out bool started);
        if (!started)
        {
            SendBulkReject(notice.Id, QuiclyErrorCode.BulkRejected);
        }
    }

    private int FindSend(ulong transferId)
    {
        for (int record = 0; record < _records.Length; record++)
        {
            ref BulkSend send = ref _records[record];
            if ((send.Flags & SendFreed) == 0 && send.TransferId == transferId)
            {
                return record;
            }
        }

        return -1;
    }

    // ------------------------------------------------------------------ local stream events

    /// <inheritdoc/>
    public override void OnStreamStarted(TransportStreamId id, ulong context, TransportStatus status)
    {
        if (!PeerCore.TryDecodeEngineStreamContext(context, out ChannelMode mode, out int record, out uint serial)
            || mode != ChannelMode.Bulk || (uint)record >= (uint)_txStreams.Length)
        {
            return;
        }

        _txStreams[record] = id;
        _txSerials[record] = serial;
        NoticeKind kind = status switch
        {
            TransportStatus.Success => NoticeKind.Started,
            TransportStatus.StreamLimitReached => NoticeKind.Refused,
            _ => NoticeKind.StartFailed,
        };
        Post(record, serial, kind, 0);
    }

    /// <inheritdoc/>
    public override void OnIdealSendBufferSize(TransportStreamId id, ulong bytes)
    {
        if (!id.IsValid)
        {
            // A cleared slot of _txStreams holds the invalid id, so an invalid one would "match" record zero and write a
            // window onto a transfer it has nothing to do with; the close path checks the same thing.
            return;
        }

        for (int record = 0; record < _txStreams.Length; record++)
        {
            if (_txStreams[record] == id)
            {
                // A window the transport asked for: the game thread reads it when it next fills the stream.
                Volatile.Write(ref _idealSendBuffer[record], (long)Math.Min(bytes, long.MaxValue));
                return;
            }
        }
    }

    private void Post(int record, uint serial, NoticeKind kind, ulong errorCode)
    {
        StreamNotice notice = new() { Record = record, Serial = serial, Kind = kind, ErrorCode = errorCode };
        if (!_notices.TryEnqueue(in notice))
        {
            // Sized for every live stream's notices between two drains; cannot happen.
            _core.Counters.CallbackFaults++;
        }

        _core.NoteTransportWork(); // transport thread (stream events only)
    }

    /// <summary>
    /// Applies the transport-thread notices of this engine's own streams (game thread). A notice applied outside a pass (a
    /// completion routed by Poll drains them first) can leave a transfer something to send — a confirmed start lets its body
    /// go — so it counts as work for the next pass (<see cref="_sendsChanged"/>).
    /// </summary>
    private void DrainNotices()
    {
        while (_notices.TryDequeue(out StreamNotice notice))
        {
            _sendsChanged = true;
            int record = notice.Record;
            ref BulkSend send = ref _records[record];
            if (notice.Serial != send.Serial || (send.Flags & SendFreed) != 0)
            {
                continue; // an earlier stream of this record, or of an earlier occupant
            }

            int local = send.Local;
            ref BulkSendState state = ref _send[local];
            switch (notice.Kind)
            {
                case NoticeKind.Started:
                    if (send.Phase == BulkPhase.Starting)
                    {
                        send.Phase = BulkPhase.Open;
                    }

                    break;
                case NoticeKind.Refused:
                    if (send.Phase == BulkPhase.Starting)
                    {
                        StreamsRefused++;
                        if (send.CarriersOutstanding == 0)
                        {
                            // Every piece already came back canceled before the notice did (the order is the transport's).
                            RewindRefused(record, ref send, ref state);
                        }
                        else
                        {
                            send.Phase = BulkPhase.Refused;
                        }
                    }

                    break;
                case NoticeKind.ShutDown:
                    // The stream is gone either way, so it is forgotten before the verdict: its slot (and the peer's
                    // credit) are free again, and no termination can try to reset it. A live transfer whose stream shut down
                    // before it sent FIN, with no stop from the peer (that is a Stopped notice) and no reset of ours (that
                    // finished it first), lost it to the connection: the transport shuts every stream down before it reports
                    // its own close, so this can arrive while IsTransportClosing is still false.
                    send.Stream = default;
                    if (send.Phase is BulkPhase.Starting or BulkPhase.Open && (send.Flags & SendFinSent) == 0)
                    {
                        // The one exit, like every other termination; with the stream forgotten it resets nothing.
                        TerminateSend(local, record, BulkStatus.Disconnected, QuiclyErrorCode.NoError);
                    }

                    // A transfer that did send FIN keeps its record until the peer's final progress; only the slot is free.
                    ReleaseStreamSlot(ref send, ref state);
                    break;
                default:
                    // Stopped by the peer (STOP_SENDING) or failed to start.
                    send.Stream = default;
                    if (send.Phase is BulkPhase.Starting or BulkPhase.Open)
                    {
                        QuiclyErrorCode code = (QuiclyErrorCode)notice.ErrorCode;
                        BulkStatus status = _core.IsTransportClosing ? BulkStatus.Disconnected
                            : code == QuiclyErrorCode.BulkCanceled ? BulkStatus.Canceled
                            : BulkStatus.Failed;
                        TerminateSend(local, record, status, code);
                    }

                    ReleaseStreamSlot(ref send, ref state);
                    break;
            }

            ReleaseIfDone(record, ref state);
        }
    }

    /// <inheritdoc/>
    /// <remarks>Bulk channels carry no datagrams (the peer rejects them before they get here); counted as dropped.</remarks>
    public override void OnDatagram(in MessageHeader header, ReadOnlySpan<byte> payload, long nowMicros) =>
        _core.CountDatagramDropped(header.Channel);

    /// <inheritdoc/>
    public override void Dispose()
    {
        DisposeReceive();
        _send?.Dispose();
        _records?.Dispose();
        _scratch?.Dispose();
        _notices?.Dispose();
        _control?.Dispose();
    }

    /// <summary>A transport-thread event of one of this engine's own bulk streams, handed to the game thread.</summary>
    private struct StreamNotice
    {
        /// <summary>The transfer record the stream belongs to.</summary>
        public int Record;

        /// <summary>Serial of the stream (notices of earlier streams are ignored).</summary>
        public uint Serial;

        /// <summary>What happened.</summary>
        public NoticeKind Kind;

        /// <summary>The reset code of a stop.</summary>
        public ulong ErrorCode;
    }

    /// <summary>A control message of the peer, handed from the transport thread to the game thread.</summary>
    private struct ControlNotice
    {
        /// <summary>Which message.</summary>
        public ControlKind Kind;

        /// <summary>Bulk channel (requests only).</summary>
        public ushort Channel;

        /// <summary>The frame came on the control stream rather than as a control datagram (progress only).</summary>
        public bool OnStream;

        /// <summary>Error code (cancel and reject).</summary>
        public uint Code;

        /// <summary>Transfer id (progress, cancel) or request id (reject, request).</summary>
        public ulong Id;

        /// <summary>Requested object (requests only).</summary>
        public ulong ObjectId;

        /// <summary>Requested object version (requests only).</summary>
        public ulong ObjectVersion;

        /// <summary>Bytes accepted (progress) or the range's offset (request).</summary>
        public long Offset;

        /// <summary>The range's length (request).</summary>
        public long Length;
    }

    /// <summary>A range this end asked for and has not been answered yet (game thread).</summary>
    private struct PendingRequest
    {
        /// <summary>Whether the slot holds a request.</summary>
        public bool InUse;

        /// <summary>The request id on the wire.</summary>
        public ulong RequestId;

        /// <summary>
        /// What was asked for. A resumable request whose answer a disconnect cut short advances past the bytes that arrived
        /// (<see cref="SettleRequest"/>), so the resume asks for the rest.
        /// </summary>
        public BulkRangeRequest Range;
    }

    /// <summary>Send-side state of one Bulk channel: one cache line, native memory, game thread only.</summary>
    [StructLayout(LayoutKind.Explicit, Size = 64)]
    private struct BulkSendState
    {
        /// <summary>Oldest live transfer of the channel (-1 = none).</summary>
        [FieldOffset(0)] public int ListHead;

        /// <summary>Newest live transfer of the channel (-1 = none).</summary>
        [FieldOffset(4)] public int ListTail;

        /// <summary>Live transfers of the channel.</summary>
        [FieldOffset(8)] public int Count;

        /// <summary>Transfers of the channel holding a stream (bounded by <see cref="ChannelDefinition.MaxGroups"/>).</summary>
        [FieldOffset(12)] public int StreamedCount;
    }

    /// <summary>One send transfer: its range, its stream and its phase. Two cache lines, native memory, game thread only.</summary>
    [StructLayout(LayoutKind.Explicit, Size = 128)]
    private struct BulkSend
    {
        /// <summary>The transfer id on the wire.</summary>
        [FieldOffset(0)] public ulong TransferId;

        /// <summary>Object identity.</summary>
        [FieldOffset(8)] public ulong ObjectId;

        /// <summary>Object version.</summary>
        [FieldOffset(16)] public ulong ObjectVersion;

        /// <summary>Size of the whole object.</summary>
        [FieldOffset(24)] public long TotalLength;

        /// <summary>First object byte of the range.</summary>
        [FieldOffset(32)] public long Offset;

        /// <summary>Object bytes the range carries.</summary>
        [FieldOffset(40)] public long Length;

        /// <summary>Object bytes read from the source and handed to the transport.</summary>
        [FieldOffset(48)] public long BytesRead;

        /// <summary>Object bytes whose stream sends completed.</summary>
        [FieldOffset(56)] public long BytesCompleted;

        /// <summary>Object bytes the peer reported accepted (<c>BulkProgress</c>); the range's length is <c>Delivered</c>.</summary>
        [FieldOffset(64)] public long BytesAcked;

        /// <summary>Wire bytes submitted and not yet completed (what the send window bounds).</summary>
        [FieldOffset(72)] public long WireOutstanding;

        /// <summary>Bytes the transport asked to keep outstanding on this stream, or 0.</summary>
        [FieldOffset(80)] public long IdealSendBuffer;

        /// <summary>The transfer's stream (valid from the open until it ends or is abandoned).</summary>
        [FieldOffset(88)] public TransportStreamId Stream;

        /// <summary>Serial of the current stream (in its context; notices of earlier streams are ignored).</summary>
        [FieldOffset(96)] public uint Serial;

        /// <summary><see cref="PeerCore.StreamCreditGeneration"/> read before the last open, or <see cref="CreditUnrefused"/>.</summary>
        [FieldOffset(100)] public int CreditGeneration;

        /// <summary>Pieces of the current stream not yet completed.</summary>
        [FieldOffset(104)] public int CarriersOutstanding;

        /// <summary>Next live transfer of the channel, or the next free record (-1 = none).</summary>
        [FieldOffset(108)] public int Next;

        /// <summary>Previous live transfer of the channel (-1 = none).</summary>
        [FieldOffset(112)] public int Prev;

        /// <summary>Engine-local index of the transfer's channel.</summary>
        [FieldOffset(116)] public int Local;

        /// <summary>Lifecycle of the transfer.</summary>
        [FieldOffset(120)] public BulkPhase Phase;

        /// <summary><see cref="SendHeaderWritten"/>, <see cref="SendFinSent"/>, <see cref="SendCounted"/>, <see cref="SendFreed"/>, <see cref="SendCompress"/>, <see cref="SendChecksum"/>.</summary>
        [FieldOffset(121)] public byte Flags;
    }
}
