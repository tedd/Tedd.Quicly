using System.Runtime.InteropServices;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Session.Engines;

/// <summary>
/// The <see cref="ChannelMode.ReliableOrdered"/> engine (PROTOCOL.md §3.1; docs/design/session-layer.md §7.2): every
/// message, in order, per channel and direction, over one persistent unidirectional stream per channel per direction. One
/// instance per peer owns the structure-of-arrays state of all ordered channels.
/// </summary>
/// <remarks>
/// <para><b>Send (game thread).</b> <see cref="Admit"/> checks size, the channel's <see cref="ChannelDefinition.QueueLimitBytes"/>
/// (bytes admitted and not yet acknowledged: queued plus in flight) and the entry table (a reserve of one entry per ordered
/// channel is kept for the submissions), prepares the payload by every send path (<see cref="EnginePayload"/>; a single
/// gathered page is taken as it is), writes the frame header (Length, Key, RequestId, RawLength) into the entry's header
/// block and appends the entry to the channel's FIFO. The scheduler's <see cref="FlushChannel"/> drops expired messages
/// (PROTOCOL.md §4.5), then gathers the FIFO into stream sends: each send is a <em>carrier</em> entry (untracked, never a
/// message itself) whose header block holds the preamble when it opens the stream, whose <c>Aux0</c> is a run of the
/// peer's <see cref="SegmentArena"/> (at most <see cref="MaxSegmentsPerSend"/> segments: the preamble plus each message's
/// header/payload pair, ADR 0008 invariant 1) and whose batch list (<see cref="SendEntryTable.BatchHead"/> and the members'
/// <c>Next</c> links, in admission order) names the messages it carries. The members stay <c>Filling</c> and are never
/// submitted themselves. Stream bytes follow the send cap's budget rule (§7.1). One carrier per stream per pass holds up to
/// 32 messages, so the interop cost is O(flushes), not O(messages) (ADR 0008 invariant 14).</para>
/// <para><b>Stream lifecycle.</b> The stream is opened lazily by the first pass with queued messages
/// (<see cref="PeerCore.OpenStream"/> with a <see cref="PeerCore.MakeEngineStreamContext"/> context and priority
/// <c>channel priority × 257</c>); the first carrier carries <see cref="TransportSendFlags.Start"/> and no second carrier
/// goes out until <see cref="OnStreamStarted"/> confirmed the start. A start the peer's stream limit refuses — synchronously
/// (the send returns <see cref="TransportStatus.StreamLimitReached"/>: the stream is released with
/// <see cref="ITransport.CloseStream"/>) or asynchronously (<see cref="OnStreamStarted"/> reports the refusal, then the carrier
/// completes canceled, then the stream shuts down) — never reached the peer, so its messages return to the head of the
/// queue in order and go out on a new stream once <see cref="PeerCore.StreamCreditGeneration"/> changes. A stream the peer
/// stops (STOP_SENDING) or that fails to start closes the channel for the rest of the connection: queued and in-flight
/// messages complete <see cref="DeliveryStatus.Failed"/> and sends answer <see cref="SendStatus.ChannelClosed"/>.
/// Transport-thread events of local streams reach the game thread through an SPSC ring of notices, drained before every
/// decision that depends on them.</para>
/// <para><b>Completions (game thread).</b> A carrier's completion (stream bytes acknowledged, PROTOCOL.md §4.3) completes
/// both stages of every member <see cref="DeliveryStatus.Delivered"/>, frees its segment run and the entries; a canceled one
/// completes them <see cref="DeliveryStatus.Disconnected"/> while the connection closes, re-queues them after a refusal, and
/// fails them otherwise. <see cref="TryCancel"/> works while a message is queued.</para>
/// <para><b>Receive (transport thread).</b> <see cref="OnStreamOpened"/> accepts one stream per channel per connection (a
/// second is a connection-level <see cref="QuiclyErrorCode.ProtocolViolation"/>). <see cref="StreamMessagePhase.Start"/>
/// reserves a receive-ring slot and rents a lease of the frame's <c>Length</c> within the receive budget — either failing
/// answers <see cref="StreamConsume.Pend"/> (the peer un-reads the event and resumes the stream from Poll); a message larger
/// than the receive budget or the largest pool block could never be buffered and closes the connection with
/// <see cref="QuiclyErrorCode.LimitExceeded"/>. Chunks are copied progressively; the end publishes the message into the
/// reserved slot. <c>MaxMessageSize</c> (session cap included) and frame errors are enforced by the peer's
/// <see cref="StreamFrameParser"/> (connection <see cref="QuiclyErrorCode.ProtocolViolation"/>).</para>
/// </remarks>
internal sealed unsafe class ReliableOrderedEngine : ChannelEngine
{
    /// <summary>Most segments one stream send carries (docs/design/session-layer.md §7.2).</summary>
    public const int MaxSegmentsPerSend = 64;

    // Aux1 of a message entry: where it is.
    private const long InQueue = 0;
    private const long InCarrier = 1;
    private const long Finished = 2;

    // Aux1 of a carrier entry: CarrierBase + the serial of the stream it went out on.
    private const long CarrierBase = 1L << 40;

    private const byte RecvSeen = 1;
    private const byte RecvOpen = 2;
    private const byte RecvReserved = 4;

    private PeerCore _core = null!;
    private ChannelDefinition[] _channels = [];
    private int[] _localOf = [];
    private int[] _denseOf = [];
    private long[] _expiryMicros = [];
    private NativeArray<OrderedSendState> _send = null!;
    private NativeArray<OrderedRecvState> _recv = null!;
    private TransportStreamId[] _txStreams = [];
    private uint[] _txSerials = [];
    private SpscRing<StreamNotice> _notices = null!;
    private int _carrierReserve;
    private long _maxReceiveMessage;
    private int _maxSegments;

    /// <summary>Where the channel's send stream is in its lifecycle (game thread).</summary>
    internal enum StreamPhase : byte
    {
        /// <summary>No stream: the next pass with queued messages opens one.</summary>
        NoStream = 0,

        /// <summary>The first send (with Start) is out; nothing more is sent until the start is confirmed.</summary>
        Starting = 1,

        /// <summary>Started: sends flow.</summary>
        Open = 2,

        /// <summary>The peer's stream limit refused the start; its carrier is being canceled back.</summary>
        Refused = 3,

        /// <summary>No stream; waiting until the peer grants stream credit (<see cref="PeerCore.StreamCreditGeneration"/> changes).</summary>
        Blocked = 4,

        /// <summary>The stream was stopped or failed: the channel is closed for the rest of the connection.</summary>
        Closed = 5,
    }

    private enum NoticeKind : byte
    {
        Started,
        Refused,
        StartFailed,
        Stopped,
        ShutDown,
    }

    /// <inheritdoc/>
    public override ChannelMode Mode => ChannelMode.ReliableOrdered;

    /// <inheritdoc/>
    public override void Initialize(PeerCore core, ReadOnlySpan<ChannelDefinition> channelsOfMode)
    {
        _core = core;
        int count = channelsOfMode.Length;
        _channels = channelsOfMode.ToArray();
        _localOf = new int[core.ChannelCount];
        Array.Fill(_localOf, -1);
        _denseOf = new int[count];
        _expiryMicros = new long[count];
        _send = new NativeArray<OrderedSendState>(Math.Max(count, 1));
        _recv = new NativeArray<OrderedRecvState>(Math.Max(count, 1));
        _txStreams = new TransportStreamId[count];
        _txSerials = new uint[count];
        // At most three notices per stream and two live streams per channel between drains.
        _notices = new SpscRing<StreamNotice>((16 * count) + 16);
        for (int local = 0; local < count; local++)
        {
            ChannelDefinition channel = _channels[local];
            int dense = core.ChannelIndexOf(channel.Id);
            _localOf[dense] = local;
            _denseOf[local] = dense;
            _expiryMicros[local] = channel.ResolveExpiryMicros(core.FlushIntervalMicros);
            ref OrderedSendState send = ref _send[local];
            send.QueueHead = -1;
            send.QueueTail = -1;
            send.StartCarrier = -1;
        }

        _carrierReserve = count + 1;
        _maxReceiveMessage = Math.Min(core.ReceiveBudgetBytes, core.Allocator.MaxBlockSize);
        _maxSegments = Math.Min(MaxSegmentsPerSend, core.Segments.Capacity);
    }

    /// <summary>The lifecycle phase of a channel's send stream (game thread; tests).</summary>
    /// <param name="channelIndex">Dense index of a channel of this engine.</param>
    /// <returns>The phase.</returns>
    internal StreamPhase PhaseOf(int channelIndex) => _send[_localOf[channelIndex]].Phase;

    /// <summary>The serial of a channel's current send stream (0 before the first; game thread; tests).</summary>
    /// <param name="channelIndex">Dense index of a channel of this engine.</param>
    /// <returns>The serial.</returns>
    internal uint StreamSerialOf(int channelIndex) => _send[_localOf[channelIndex]].StreamSerial;

    // ------------------------------------------------------------------ send (game thread)

    /// <inheritdoc/>
    public override SendStatus Admit(ref SendRequest request)
    {
        DrainNotices();
        ChannelDefinition channel = request.Channel;
        int dense = request.ChannelIndex;
        int local = _localOf[dense];
        ref ChannelSendCounters counters = ref _core.SendCounters(dense);
        ref OrderedSendState send = ref _send[local];
        if (send.Phase == StreamPhase.Closed)
        {
            return SendStatus.ChannelClosed;
        }

        int length = request.Kind == SendPayloadKind.Gather ? EnginePayload.GatherLength(request.Gather) : request.Length;
        if (length > _core.EffectiveMaxMessageSize(channel))
        {
            counters.TooLarge++;
            return SendStatus.TooLarge;
        }

        // The limit covers bytes admitted and not yet acknowledged; a message larger than the limit still goes alone.
        long pending = send.QueueBytes + send.InFlightBytes;
        if (channel.QueueLimitBytes > 0 && pending > 0 && pending + length > channel.QueueLimitBytes)
        {
            counters.QueueFull++;
            return SendStatus.QueueFull;
        }

        SendEntryTable entries = _core.Entries;
        if (entries.Available <= _carrierReserve || !_core.TryAllocateEntry(channel.Id, SendEntryFlags.None, out int slot))
        {
            // The reserve keeps entries for the carriers that drain the queues (no deadlock on a full table).
            counters.QueueFull++;
            return SendStatus.QueueFull;
        }

        PreparedPayload payload = default;
        if (!EnginePayload.TryPrepare(_core, ref request, channel, length, takeSinglePage: true, ref payload))
        {
            _core.DiscardEntry(slot);
            return SendStatus.OutOfBuffers;
        }

        if (request.Options.Track && !_core.TryTrack(slot, request.Options.Context, out request.Token))
        {
            EnginePayload.Release(_core, in payload);
            _core.DiscardEntry(slot);
            counters.QueueFull++;
            return SendStatus.QueueFull;
        }

        // Commit: nothing below fails.
        StreamMessageHeader header = default;
        header.Length = payload.Length;
        header.Key = channel.Keyed ? request.Key : 0;
        header.RequestId = channel.RequestResponse ? request.RequestId : 0;
        header.RawLength = payload.RawLength;
        entries.SetHeaderLength(slot, StreamFraming.WriteFrameHeader(entries.GetHeaderBlock(slot), channel, in header));
        EnginePayload.Commit(_core, slot, ref request, in payload);
        entries.Keys[slot] = header.Key;
        long expiry = request.Options.ExpiryMicros > 0 ? request.Options.ExpiryMicros : _expiryMicros[local];
        if (expiry > 0)
        {
            long now = _core.Clock.NowMicros;
            entries.Deadlines[slot] = expiry >= long.MaxValue - now ? long.MaxValue : now + expiry;
        }

        ref SendEntry entry = ref entries[slot];
        entry.Aux0 = length;
        entry.Aux1 = InQueue;
        if (request.Options.Mode == SendMode.Immediate)
        {
            entry.Flags |= SendEntryFlags.Immediate;
        }

        _core.StampAdmission(slot);
        NativeArray<int> links = entries.Next;
        links[slot] = -1;
        if (send.QueueTail < 0)
        {
            send.QueueHead = slot;
        }
        else
        {
            links[send.QueueTail] = slot;
        }

        send.QueueTail = slot;
        send.QueueCount++;
        send.QueueBytes += length;
        return SendStatus.Admitted;
    }

    /// <inheritdoc/>
    public override void FlushChannel(int channelIndex, ref FlushContext flush)
    {
        DrainNotices();
        int local = _localOf[channelIndex];
        ref OrderedSendState send = ref _send[local];
        if (send.QueueHead < 0)
        {
            return;
        }

        switch (send.Phase)
        {
            case StreamPhase.Closed:
                FailQueued(ref send, DeliveryStatus.Failed);
                return;
            case StreamPhase.Starting:
            case StreamPhase.Refused:
                return;
            case StreamPhase.Blocked:
                if (_core.StreamCreditGeneration == send.CreditGeneration)
                {
                    return;
                }

                send.Phase = StreamPhase.NoStream;
                break;
        }

        ref ChannelSendCounters counters = ref _core.SendCounters(channelIndex);
        DropExpiredHead(ref send, ref counters, flush.NowMicros);
        while (send.QueueHead >= 0 && send.Phase is StreamPhase.Open or StreamPhase.NoStream)
        {
            if (flush.BudgetBytes <= 0)
            {
                flush.BudgetExhausted = true;
                return;
            }

            if (!SubmitCarrier(local, ref send, ref counters, ref flush))
            {
                return;
            }
        }
    }

    /// <inheritdoc/>
    public override void Flush(ref FlushContext flush)
    {
    }

    /// <inheritdoc/>
    /// <remarks>Nothing is time-driven: expiry is evaluated when the scheduler reaches a message.</remarks>
    public override void Tick(long nowMicros, ref long nextDeadline)
    {
    }

    /// <inheritdoc/>
    public override void OnSendCompleted(int entrySlot, in CompletionEntry completion)
    {
        if (!completion.Final)
        {
            return;
        }

        if (_core.Entries[entrySlot].Aux1 >= CarrierBase)
        {
            OnCarrierCompleted(entrySlot, in completion);
            return;
        }

        // A message finished without being sent (expired, canceled, failed): a local completion this engine queued.
        _core.CompleteEntry(entrySlot, _core.MapCompletion(in completion));
    }

    /// <inheritdoc/>
    /// <remarks>Only a message still queued can be canceled; it completes <see cref="DeliveryStatus.Canceled"/> at the next Poll or Flush.</remarks>
    public override bool TryCancel(int entrySlot)
    {
        SendEntryTable entries = _core.Entries;
        ref SendEntry entry = ref entries[entrySlot];
        if (entries.GetState(entrySlot) != SendEntryState.Filling || entry.Aux1 != InQueue)
        {
            return false;
        }

        int dense = _core.ChannelIndexOf(entry.Channel);
        if (dense < 0 || _localOf[dense] < 0)
        {
            return false;
        }

        ref OrderedSendState send = ref _send[_localOf[dense]];
        NativeArray<int> links = entries.Next;
        int previous = -1;
        int slot = send.QueueHead;
        while (slot >= 0 && slot != entrySlot)
        {
            previous = slot;
            slot = links[slot];
        }

        if (slot < 0)
        {
            return false;
        }

        int after = links[entrySlot];
        if (previous < 0)
        {
            send.QueueHead = after;
        }
        else
        {
            links[previous] = after;
        }

        if (send.QueueTail == entrySlot)
        {
            send.QueueTail = previous;
        }

        send.QueueCount--;
        send.QueueBytes -= entry.Aux0;
        entry.Aux1 = Finished;
        _core.QueueLocalCompletion(entrySlot, DeliveryStatus.Canceled);
        return true;
    }

    /// <inheritdoc/>
    public override long OldestQueuedStamp()
    {
        long oldest = long.MaxValue;
        for (int local = 0; local < _channels.Length; local++)
        {
            ref OrderedSendState send = ref _send[local];

            // A carrier whose start is unconfirmed may still come back (a refused start): its messages count as queued.
            int head = send.StartCarrier >= 0 && send.Phase is StreamPhase.Starting or StreamPhase.Refused
                ? _core.Entries.BatchHead[send.StartCarrier]
                : send.QueueHead;
            if (head >= 0)
            {
                long stamp = _core.GetAdmissionStamp(head);
                if (stamp < oldest)
                {
                    oldest = stamp;
                }
            }
        }

        return oldest;
    }

    /// <inheritdoc/>
    public override void AddStatistics(int channelIndex, ref ChannelStatistics statistics)
    {
        ref OrderedSendState send = ref _send[_localOf[channelIndex]];
        statistics.QueuedMessages = send.QueueCount;
        statistics.QueuedBytes = send.QueueBytes;
        statistics.InFlightMessages = send.InFlightCount;
        statistics.InFlightBytes = send.InFlightBytes;
    }

    /// <inheritdoc/>
    public override void OnPeerClosed()
    {
        DrainNotices();
        SendEntryTable entries = _core.Entries;
        NativeArray<int> links = entries.Next;
        for (int local = 0; local < _channels.Length; local++)
        {
            ref OrderedSendState send = ref _send[local];
            int slot = send.QueueHead;
            send.QueueHead = -1;
            send.QueueTail = -1;
            send.QueueCount = 0;
            send.QueueBytes = 0;
            send.Phase = StreamPhase.Closed;
            send.Stream = default;
            while (slot >= 0)
            {
                int next = links[slot];
                entries[slot].Aux1 = Finished;
                _core.CompleteEntry(slot, DeliveryStatus.Disconnected);
                slot = next;
            }
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// A peer is one connection and one epoch: its streams start with the session, so nothing needs resetting here. A new
    /// connection gets a new peer with new streams (PROTOCOL.md §4.1).
    /// </remarks>
    public override void OnEpochReset(bool resumed)
    {
    }

    private void DropExpiredHead(ref OrderedSendState send, ref ChannelSendCounters counters, long now)
    {
        SendEntryTable entries = _core.Entries;
        NativeArray<long> deadlines = entries.Deadlines;
        NativeArray<int> links = entries.Next;
        int slot = send.QueueHead;
        while (slot >= 0)
        {
            long deadline = deadlines[slot];
            if (deadline == 0 || now <= deadline)
            {
                break;
            }

            // PROTOCOL.md §4.5: expiry is evaluated at scheduling time; a message never written to the stream leaves no gap.
            int next = links[slot];
            ref SendEntry entry = ref entries[slot];
            send.QueueHead = next;
            if (next < 0)
            {
                send.QueueTail = -1;
            }

            send.QueueCount--;
            send.QueueBytes -= entry.Aux0;
            entry.Aux1 = Finished;
            counters.Expired++;
            _core.QueueLocalCompletion(slot, DeliveryStatus.Expired);
            slot = next;
        }
    }

    /// <summary>
    /// Gathers the head of the queue into one stream send (a carrier entry and a segment run), opening the stream first when
    /// there is none. Returns <see langword="false"/> when the pass must stop for this channel (resources, refusal, stream
    /// start pending).
    /// </summary>
    private bool SubmitCarrier(int local, ref OrderedSendState send, ref ChannelSendCounters counters, ref FlushContext flush)
    {
        ChannelDefinition channel = _channels[local];
        SendEntryTable entries = _core.Entries;
        SegmentArena arena = _core.Segments;
        bool opening = send.Phase == StreamPhase.NoStream;
        int preamble = opening ? 1 : 0;
        int capacity = Math.Min(_maxSegments, preamble + (2 * Math.Min(send.QueueCount, MaxSegmentsPerSend)));
        if (!arena.TryAllocate(capacity, out int run))
        {
            // The arena reclaims oldest first; a single message may still fit.
            int minimal = preamble + 2;
            if (minimal >= capacity || !arena.TryAllocate(minimal, out run))
            {
                return false;
            }

            capacity = minimal;
        }

        if (!_core.TryAllocateEntry(channel.Id, SendEntryFlags.None, out int carrier))
        {
            arena.Free(run);
            return false;
        }

        int credit = _core.StreamCreditGeneration;
        if (opening && !TryOpenStream(local, ref send, channel, credit))
        {
            _core.DiscardEntry(carrier);
            arena.Free(run);
            return false;
        }

        TransportSegment* segments = arena.GetPointer(run);
        int used = 0;
        long bytes = 0;
        if (opening)
        {
            int written = StreamFraming.WritePreamble(entries.GetHeaderBlock(carrier), channel.Id);
            entries.SetHeaderLength(carrier, written);
            segments[used++] = entries[carrier].Header;
            bytes = written;
        }

        NativeArray<int> links = entries.Next;
        NativeArray<long> deadlines = entries.Deadlines;
        long now = flush.NowMicros;
        long budget = flush.BudgetBytes - bytes;
        int first = -1;
        int last = -1;
        int members = 0;
        long payloadBytes = 0;
        long admitted = 0;
        bool priority = false;
        int slot = send.QueueHead;
        while (slot >= 0 && used + 2 <= capacity)
        {
            if (members > 0 && budget <= 0)
            {
                flush.BudgetExhausted = true;
                break;
            }

            int next = links[slot];
            ref SendEntry entry = ref entries[slot];
            send.QueueHead = next;
            if (next < 0)
            {
                send.QueueTail = -1;
            }

            send.QueueCount--;
            send.QueueBytes -= entry.Aux0;
            long deadline = deadlines[slot];
            if (deadline != 0 && now > deadline)
            {
                counters.Expired++;
                entry.Aux1 = Finished;
                _core.QueueLocalCompletion(slot, DeliveryStatus.Expired);
                slot = next;
                continue;
            }

            TransportSegment* pair = entries.GetSegments(slot);
            segments[used++] = pair[0];
            uint payloadLength = entry.Payload.Length;
            long size = entry.HeaderLength + payloadLength;
            if (payloadLength != 0)
            {
                segments[used++] = pair[1];
            }

            budget -= size;
            bytes += size;
            payloadBytes += payloadLength;
            admitted += entry.Aux0;
            priority |= (entry.Flags & SendEntryFlags.Immediate) != 0;
            entry.Aux1 = InCarrier;
            links[slot] = -1;
            if (last < 0)
            {
                first = slot;
            }
            else
            {
                links[last] = slot;
            }

            last = slot;
            members++;
            slot = next;
        }

        if (members == 0)
        {
            // Everything taken had expired (the queue is empty now).
            _core.DiscardEntry(carrier);
            arena.Free(run);
            if (opening)
            {
                AbandonStream(ref send);
            }

            return false;
        }

        entries.BatchHead[carrier] = first;
        entries.BatchCount[carrier] = members;
        ref SendEntry carrierEntry = ref entries[carrier];
        carrierEntry.Aux0 = run;
        carrierEntry.Aux1 = CarrierBase + send.StreamSerial;
        TransportSendFlags flags = opening ? TransportSendFlags.Start : TransportSendFlags.None;
        if (priority)
        {
            flags |= TransportSendFlags.Priority;
        }

        // Datagrams the scheduler handed to the packer earlier in this pass (channels of higher priority) leave first.
        _core.Packer.SubmitPending(ref flush);
        TransportStatus status = _core.SubmitStream(send.Stream, segments, used, carrier, flags);
        if (status != TransportStatus.Success)
        {
            // No completion follows a refused call: the messages go back to the head of the queue in order.
            RequeueFront(ref send, first, wasInFlight: false, ref counters);
            entries.ClearBatch(carrier);
            _core.DiscardEntry(carrier);
            arena.Free(run);
            if (opening)
            {
                AbandonStream(ref send);
                if (status == TransportStatus.StreamLimitReached)
                {
                    send.Phase = StreamPhase.Blocked;
                    send.CreditGeneration = credit;
                }
            }

            return false;
        }

        send.InFlightCount += members;
        send.InFlightBytes += admitted;
        send.CarriersOutstanding++;
        if (opening)
        {
            send.Phase = StreamPhase.Starting;
            send.StartCarrier = carrier;
        }

        counters.Sent += members;
        counters.Bytes += payloadBytes;
        flush.BudgetBytes -= bytes;
        flush.BytesSubmitted += bytes;
        PeerCounters peerCounters = _core.Counters;
        peerCounters.StreamSends++;
        peerCounters.StreamBytesSent += bytes;
        return true;
    }

    private bool TryOpenStream(int local, ref OrderedSendState send, ChannelDefinition channel, int credit)
    {
        uint serial = (send.StreamSerial + 1) & PeerCore.EngineStreamSerialMask;
        ulong context = PeerCore.MakeEngineStreamContext(ChannelMode.ReliableOrdered, _denseOf[local], serial);
        TransportStatus status = _core.OpenStream(StreamKind.Unidirectional, context, (ushort)(channel.Priority * 257), out TransportStreamId id);
        if (status != TransportStatus.Success)
        {
            if (status == TransportStatus.StreamLimitReached)
            {
                send.Phase = StreamPhase.Blocked;
                send.CreditGeneration = credit;
            }

            return false;
        }

        send.Stream = id;
        send.StreamSerial = serial;
        send.CreditGeneration = credit;
        send.CarriersOutstanding = 0;
        return true;
    }

    /// <summary>Releases a stream that never started (no accepted send carried Start): no callback follows for it (§7.2).</summary>
    private void AbandonStream(ref OrderedSendState send)
    {
        _core.Transport?.CloseStream(send.Stream);
        send.Stream = default;
        send.Phase = StreamPhase.NoStream;
    }

    private void OnCarrierCompleted(int carrier, in CompletionEntry completion)
    {
        // A refusal or stop reported before this completion decides what happens to the carried messages.
        DrainNotices();
        SendEntryTable entries = _core.Entries;
        ref SendEntry carrierEntry = ref entries[carrier];
        int dense = _core.ChannelIndexOf(carrierEntry.Channel);
        int local = _localOf[dense];
        ref OrderedSendState send = ref _send[local];
        ref ChannelSendCounters counters = ref _core.SendCounters(dense);
        uint serial = (uint)(carrierEntry.Aux1 - CarrierBase);
        bool current = serial == send.StreamSerial;
        if (current)
        {
            send.CarriersOutstanding--;
        }

        if (send.StartCarrier == carrier)
        {
            send.StartCarrier = -1;
        }

        _core.Segments.Free((int)carrierEntry.Aux0);
        int first = entries.BatchHead[carrier];
        entries.ClearBatch(carrier);
        if (!completion.Canceled)
        {
            FinishMembers(ref send, first, DeliveryStatus.Delivered);
        }
        else if (_core.IsTransportClosing)
        {
            FinishMembers(ref send, first, DeliveryStatus.Disconnected);
        }
        else if (current && send.Phase == StreamPhase.Refused)
        {
            // The peer's stream limit refused the start: nothing reached the peer. The messages go out again, first, on a
            // new stream once the peer grants credit.
            RequeueFront(ref send, first, wasInFlight: true, ref counters);
            if (send.CarriersOutstanding == 0)
            {
                send.Phase = StreamPhase.Blocked;
                send.Stream = default;
            }
        }
        else
        {
            FinishMembers(ref send, first, DeliveryStatus.Failed);
            if (current && send.Phase is StreamPhase.Open or StreamPhase.Starting)
            {
                CloseChannel(ref send, DeliveryStatus.Failed);
            }
        }

        // The carrier is untracked: this only frees its slot.
        _core.CompleteEntry(carrier, DeliveryStatus.Delivered);
    }

    private void FinishMembers(ref OrderedSendState send, int first, DeliveryStatus status)
    {
        SendEntryTable entries = _core.Entries;
        NativeArray<int> links = entries.Next;
        int slot = first;
        while (slot >= 0)
        {
            // Read the link first: completing the entry frees its slot (a continuation may reuse it at once).
            int next = links[slot];
            ref SendEntry entry = ref entries[slot];
            send.InFlightCount--;
            send.InFlightBytes -= entry.Aux0;
            entry.Aux1 = Finished;
            _core.CompleteEntry(slot, status);
            slot = next;
        }
    }

    private void RequeueFront(ref OrderedSendState send, int first, bool wasInFlight, ref ChannelSendCounters counters)
    {
        SendEntryTable entries = _core.Entries;
        NativeArray<int> links = entries.Next;
        int last = -1;
        for (int slot = first; slot >= 0; slot = links[slot])
        {
            ref SendEntry entry = ref entries[slot];
            entry.Aux1 = InQueue;
            send.QueueCount++;
            send.QueueBytes += entry.Aux0;
            if (wasInFlight)
            {
                send.InFlightCount--;
                send.InFlightBytes -= entry.Aux0;
                counters.Sent--;
                counters.Bytes -= entry.Payload.Length;
            }

            last = slot;
        }

        if (last < 0)
        {
            return;
        }

        links[last] = send.QueueHead;
        if (send.QueueHead < 0)
        {
            send.QueueTail = last;
        }

        send.QueueHead = first;
    }

    private void CloseChannel(ref OrderedSendState send, DeliveryStatus queuedStatus)
    {
        send.Phase = StreamPhase.Closed;
        send.Stream = default;
        FailQueued(ref send, queuedStatus);
    }

    private void FailQueued(ref OrderedSendState send, DeliveryStatus status)
    {
        SendEntryTable entries = _core.Entries;
        NativeArray<int> links = entries.Next;
        int slot = send.QueueHead;
        send.QueueHead = -1;
        send.QueueTail = -1;
        send.QueueCount = 0;
        send.QueueBytes = 0;
        while (slot >= 0)
        {
            int next = links[slot];
            entries[slot].Aux1 = Finished;
            _core.QueueLocalCompletion(slot, status);
            slot = next;
        }
    }

    /// <summary>Applies the transport-thread notices of this engine's streams to the send state (game thread).</summary>
    private void DrainNotices()
    {
        SpscRing<StreamNotice> ring = _notices;
        while (ring.TryDequeue(out StreamNotice notice))
        {
            ref OrderedSendState send = ref _send[notice.Local];
            if (notice.Serial != send.StreamSerial)
            {
                continue; // an earlier stream of the channel
            }

            switch (notice.Kind)
            {
                case NoticeKind.Started:
                    if (send.Phase == StreamPhase.Starting)
                    {
                        send.Phase = StreamPhase.Open;
                        send.StartCarrier = -1;
                    }

                    break;
                case NoticeKind.Refused:
                    if (send.Phase == StreamPhase.Starting)
                    {
                        if (send.CarriersOutstanding == 0)
                        {
                            send.Phase = StreamPhase.Blocked;
                            send.Stream = default;
                        }
                        else
                        {
                            send.Phase = StreamPhase.Refused;
                        }
                    }

                    break;
                case NoticeKind.ShutDown:
                    // A persistent stream only ends with the connection; anything queued will not go out.
                    if (send.Phase is StreamPhase.Starting or StreamPhase.Open)
                    {
                        CloseChannel(ref send, DeliveryStatus.Disconnected);
                    }

                    break;
                default:
                    // Stopped by the peer (STOP_SENDING) or failed to start: closed for the rest of the connection.
                    if (send.Phase is StreamPhase.Starting or StreamPhase.Open)
                    {
                        CloseChannel(ref send, _core.IsTransportClosing ? DeliveryStatus.Disconnected : DeliveryStatus.Failed);
                    }

                    break;
            }
        }
    }

    // ------------------------------------------------------------------ transport thread

    /// <inheritdoc/>
    /// <remarks>Ordered channels carry no datagrams (the peer rejects them before they get here); counted as dropped.</remarks>
    public override void OnDatagram(in MessageHeader header, ReadOnlySpan<byte> payload, long nowMicros) =>
        _core.CountDatagramDropped(header.Channel);

    /// <inheritdoc/>
    public override StreamAccept OnStreamOpened(TransportStreamId id, ushort channel, ulong groupId)
    {
        int dense = _core.ChannelIndexOf(channel);
        int local = dense >= 0 ? _localOf[dense] : -1;
        if (local < 0)
        {
            return StreamAccept.Reject(QuiclyErrorCode.UnsupportedChannel);
        }

        ref OrderedRecvState recv = ref _recv[local];
        if ((recv.Flags & RecvSeen) != 0)
        {
            // PROTOCOL.md §3: at most one persistent ordered stream per channel per direction; a second is a connection-level violation.
            return StreamAccept.CloseConnection(QuiclyErrorCode.ProtocolViolation);
        }

        recv.Flags = RecvSeen | RecvOpen;
        recv.Stream = id;
        return StreamAccept.Accept(local);
    }

    /// <inheritdoc/>
    public override StreamConsume OnStreamMessage(ref StreamMessageContext message)
    {
        int local = (int)message.Cookie;
        ref OrderedRecvState recv = ref _recv[local];
        switch (message.Phase)
        {
            case StreamMessagePhase.Start:
            {
                int length = message.Header.Length;
                if (length > _maxReceiveMessage)
                {
                    // It could never be buffered with this peer's receive budget or pool: pending would stall the stream for good.
                    _core.RecvCounters(message.ChannelIndex).TooLarge++;
                    return StreamConsume.CloseConnection(QuiclyErrorCode.LimitExceeded);
                }

                if (!_core.TryReserveReceive())
                {
                    return StreamConsume.Pend;
                }

                BufferLease lease = BufferLease.Empty;
                if (length > 0 && !_core.TryRentReceive(length, out lease))
                {
                    _core.CancelReservation();
                    return StreamConsume.Pend;
                }

                recv.Lease = lease;
                recv.Length = length;
                recv.Filled = 0;
                recv.Key = message.Header.Key;
                recv.RequestId = message.Header.RequestId;
                recv.RawLength = message.Header.RawLength;
                recv.Flags |= RecvReserved;
                return StreamConsume.Continue;
            }

            case StreamMessagePhase.Chunk:
            {
                ReadOnlySpan<byte> chunk = message.Chunk;
                chunk.CopyTo(new Span<byte>(_core.GetPointer(in recv.Lease) + recv.Filled, recv.Length - recv.Filled));
                recv.Filled += chunk.Length;
                return StreamConsume.Continue;
            }

            case StreamMessagePhase.End:
            {
                ReceiveEntry entry = default;
                entry.Channel = message.Channel;
                entry.Flags = recv.RawLength > 0 ? ReceiveFlags.Compressed : ReceiveFlags.None;
                if (recv.RequestId != 0)
                {
                    entry.Flags |= (recv.RequestId & 1) != 0 ? ReceiveFlags.IsRequest : ReceiveFlags.IsResponse;
                }

                entry.Key = recv.Key;
                entry.Lease = recv.Lease;
                entry.Length = recv.Length;
                entry.RawLength = recv.RawLength;
                entry.RequestId = recv.RequestId;
                entry.ReceivedMicrosDelta = PeerCore.StampReceive(message.NowMicros);
                _core.PublishReserved(in entry);
                recv.Lease = BufferLease.Empty;
                recv.Flags = (byte)(recv.Flags & ~RecvReserved);
                ref ChannelRecvCounters counters = ref _core.RecvCounters(message.ChannelIndex);
                counters.Received++;
                counters.Bytes += entry.Length;
                return StreamConsume.Continue;
            }

            default:
                return StreamConsume.CloseConnection(QuiclyErrorCode.ProtocolViolation);
        }
    }

    /// <inheritdoc/>
    public override void OnStreamClosed(TransportStreamId id, bool aborted, ulong errorCode)
    {
        for (int local = 0; local < _channels.Length; local++)
        {
            ref OrderedRecvState recv = ref _recv[local];
            if ((recv.Flags & RecvOpen) != 0 && recv.Stream == id)
            {
                ReleaseReceive(ref recv);
                recv.Flags = (byte)(recv.Flags & ~RecvOpen);
                return;
            }
        }

        if (!id.IsValid)
        {
            return;
        }

        for (int local = 0; local < _txStreams.Length; local++)
        {
            if (_txStreams[local] == id)
            {
                Post(local, _txSerials[local], aborted ? NoticeKind.Stopped : NoticeKind.ShutDown);
                if (!aborted)
                {
                    _txStreams[local] = default;
                }

                return;
            }
        }
    }

    /// <inheritdoc/>
    public override void OnStreamStarted(TransportStreamId id, ulong context, TransportStatus status)
    {
        if (!PeerCore.TryDecodeEngineStreamContext(context, out ChannelMode mode, out int dense, out uint serial)
            || mode != ChannelMode.ReliableOrdered || (uint)dense >= (uint)_localOf.Length)
        {
            return;
        }

        int local = _localOf[dense];
        if (local < 0)
        {
            return;
        }

        _txStreams[local] = id;
        _txSerials[local] = serial;
        NoticeKind kind = status switch
        {
            TransportStatus.Success => NoticeKind.Started,
            TransportStatus.StreamLimitReached => NoticeKind.Refused,
            _ => NoticeKind.StartFailed,
        };
        Post(local, serial, kind);
    }

    private void Post(int local, uint serial, NoticeKind kind)
    {
        StreamNotice notice = new() { Local = local, Serial = serial, Kind = kind };
        if (!_notices.TryEnqueue(in notice))
        {
            // Sized for every live stream's notices between two drains; cannot happen.
            _core.Counters.CallbackFaults++;
        }
    }

    private void ReleaseReceive(ref OrderedRecvState recv)
    {
        if ((recv.Flags & RecvReserved) != 0)
        {
            _core.CancelReservation();
            recv.Flags = (byte)(recv.Flags & ~RecvReserved);
        }

        if (!recv.Lease.IsEmpty)
        {
            _core.ReturnReceive(in recv.Lease);
            recv.Lease = BufferLease.Empty;
        }
    }

    /// <inheritdoc/>
    public override void Dispose()
    {
        if (_recv is not null && !_recv.IsDisposed)
        {
            for (int local = 0; local < _channels.Length; local++)
            {
                ref OrderedRecvState recv = ref _recv[local];
                if (!recv.Lease.IsEmpty)
                {
                    _core.ReturnReceive(in recv.Lease);
                    recv.Lease = BufferLease.Empty;
                }
            }

            _recv.Dispose();
        }

        _send?.Dispose();
    }

    /// <summary>A transport-thread event of one of this engine's send streams, handed to the game thread.</summary>
    private struct StreamNotice
    {
        public int Local;
        public uint Serial;
        public NoticeKind Kind;
    }

    /// <summary>Send-side state of one ordered channel: one cache line, native memory, game thread only.</summary>
    [StructLayout(LayoutKind.Explicit, Size = 64)]
    private struct OrderedSendState
    {
        /// <summary>First queued entry (-1 = empty); entries chain through <see cref="SendEntryTable.Next"/>.</summary>
        [FieldOffset(0)] public int QueueHead;

        /// <summary>Last queued entry (-1 = empty).</summary>
        [FieldOffset(4)] public int QueueTail;

        /// <summary>Queued entries.</summary>
        [FieldOffset(8)] public int QueueCount;

        /// <summary>Messages handed to the transport and not yet completed.</summary>
        [FieldOffset(12)] public int InFlightCount;

        /// <summary>Admitted payload bytes queued.</summary>
        [FieldOffset(16)] public long QueueBytes;

        /// <summary>Admitted payload bytes in flight.</summary>
        [FieldOffset(24)] public long InFlightBytes;

        /// <summary>The current send stream (valid from the open until it ends or is abandoned).</summary>
        [FieldOffset(32)] public TransportStreamId Stream;

        /// <summary>Serial of the current stream (in its context; notices of earlier streams are ignored).</summary>
        [FieldOffset(40)] public uint StreamSerial;

        /// <summary><see cref="PeerCore.StreamCreditGeneration"/> read before the last open.</summary>
        [FieldOffset(44)] public int CreditGeneration;

        /// <summary>Carriers on the current stream not yet completed.</summary>
        [FieldOffset(48)] public int CarriersOutstanding;

        /// <summary>Lifecycle of the send stream.</summary>
        [FieldOffset(52)] public StreamPhase Phase;

        /// <summary>The carrier that carried <see cref="TransportSendFlags.Start"/> while the start is unconfirmed, else -1.</summary>
        [FieldOffset(56)] public int StartCarrier;
    }

    /// <summary>Receive-side state of one ordered channel: one cache line, native memory, transport thread only.</summary>
    [StructLayout(LayoutKind.Explicit, Size = 64)]
    private struct OrderedRecvState
    {
        /// <summary>The peer's stream of the channel.</summary>
        [FieldOffset(0)] public TransportStreamId Stream;

        /// <summary>Lease of the message being received.</summary>
        [FieldOffset(8)] public BufferLease Lease;

        /// <summary>Key of the message being received.</summary>
        [FieldOffset(24)] public ulong Key;

        /// <summary>Payload length on the wire.</summary>
        [FieldOffset(32)] public int Length;

        /// <summary>Payload bytes copied so far.</summary>
        [FieldOffset(36)] public int Filled;

        /// <summary>Decoded length of a compressed payload, else 0.</summary>
        [FieldOffset(40)] public int RawLength;

        /// <summary>Request id (request/response channels).</summary>
        [FieldOffset(44)] public uint RequestId;

        /// <summary><c>RecvSeen</c>, <c>RecvOpen</c>, <c>RecvReserved</c>.</summary>
        [FieldOffset(48)] public byte Flags;
    }
}
