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
/// The <see cref="ChannelMode.ReliableUnordered"/> engine (PROTOCOL.md §3.2; docs/design/session-layer.md §7.5): every
/// message arrives, but no message waits for another's retransmission, because each <em>group</em> of messages travels on
/// its own unidirectional stream. One instance per peer owns the structure-of-arrays state of all group channels.
/// </summary>
/// <remarks>
/// <para><b>Groups (game thread).</b> The messages a channel admits between two scheduler passes form one group, bounded by
/// <see cref="ChannelDefinition.GroupMaxBytes"/> (default 64 KiB): reaching the bound seals the open group at once and starts
/// a new one, and <see cref="FlushChannel"/> seals whatever is open. A sealed group opens one unidirectional stream whose
/// preamble is <c>ChannelId, GroupId</c> (a per-channel counter), carries its messages in PROTOCOL.md §3.1 framing without a
/// request id, and ends with FIN on the send that carries its last message. Groups are independent: each has its own stream,
/// so loss in one never delays another.</para>
/// <para><b>Waiting, never failing.</b> When the peer's stream credit is exhausted the group waits
/// (PROTOCOL.md §3.2). A start the limit refuses — synchronously (the call returns
/// <see cref="TransportStatus.StreamLimitReached"/>; the stream is released with <see cref="ITransport.CloseStream"/>) or
/// asynchronously as MsQuic and the simulator do (<see cref="OnStreamStarted"/> reports the refusal, the carrier completes
/// canceled, the stream shuts down and the peer's shutdown handling closes it) — never reached the peer, so the group's
/// messages go out again, in admission order, on a <em>new</em> stream once
/// <see cref="PeerCore.StreamCreditGeneration"/> changes. <see cref="PeerCore.GroupMinIntervalMicros"/> bounds stream churn:
/// a channel opens at most one group stream per interval (an <see cref="SendMode.Immediate"/> message in the open group
/// bypasses the wait).</para>
/// <para><b>Carriers and completions.</b> Like the ordered engine, a stream send is a <em>carrier</em>: an untracked entry of
/// the channel whose <c>Aux0</c> is a run of <see cref="PeerCore.Segments"/> (at most <see cref="MaxSegmentsPerSend"/>
/// segments: the preamble plus each message's header/payload pair, ADR 0008 invariant 1) and whose batch list names the
/// messages it carries in admission order. A carrier's completion completes both stages of every member
/// (<see cref="DeliveryStatus.Delivered"/>; BufferReleased and Delivered are the same event for reliable streams,
/// PROTOCOL.md §4.3), <see cref="DeliveryStatus.Disconnected"/> while the connection closes, and re-queues them after a
/// refusal. A stream the peer stops, or one that fails to start, fails only <em>its</em> group
/// (<see cref="DeliveryStatus.Failed"/>): the channel stays open and later groups keep flowing. The peer reports a stream's
/// close exactly once (<see cref="ChannelEngine.OnStreamClosed"/>), and a record's serial rises both when it opens a stream and
/// when it is released, so a notice or a carrier tag of any earlier stream of that record is recognised as stale.</para>
/// <para><b>Receive (transport thread).</b> At most <see cref="ChannelDefinition.MaxGroups"/> (default 8) concurrent peer
/// group streams per channel; further streams are reset <see cref="QuiclyErrorCode.LimitExceeded"/> (PROTOCOL.md §7). Each
/// accepted stream stages one message at a time into a pooled lease behind a receive-ring reservation and publishes it at its
/// end, so messages are delivered as they complete. A malformed group is reset
/// <see cref="QuiclyErrorCode.ProtocolViolation"/> by the peer's parser and the connection survives (PROTOCOL.md §6); a group
/// that stalls in the middle of a message is reset <see cref="QuiclyErrorCode.Timeout"/> by the peer's mid-message idle sweep
/// (PROTOCOL.md §7). Both paths reach <see cref="OnStreamClosed"/>, which releases the staging lease and cancels the
/// reservation.</para>
/// </remarks>
internal sealed unsafe class GroupStreamEngine : ChannelEngine
{
    /// <summary>Most segments one stream send carries (docs/design/session-layer.md §7.2, §7.5).</summary>
    public const int MaxSegmentsPerSend = 64;

    // Aux1 of a message entry: where it is.
    private const long InQueue = 0;
    private const long InCarrier = 1;
    private const long Finished = 2;

    // Aux1 of a carrier entry: CarrierBase + (group << SerialBits) + the serial of the stream it went out on.
    private const long CarrierBase = 1L << 48;
    private const int SerialBits = 24;

    private const byte RecvInUse = 1;
    private const byte RecvReserved = 2;

    private const byte GroupImmediate = 1;
    private const byte GroupFinSent = 2;
    private const byte GroupCounted = 4;

    /// <summary>The record is on the free list: <see cref="ReleaseIfDone"/> already returned it and must not do so again.</summary>
    private const byte GroupFreed = 8;

    /// <summary>A group whose stream was never refused: it may open one as soon as the pass reaches it.</summary>
    private const int CreditUnrefused = int.MinValue;

    private PeerCore _core = null!;
    private ChannelDefinition[] _channels = [];
    private int[] _localOf = [];
    private int[] _denseOf = [];
    private int[] _maxGroups = [];
    private long[] _expiryMicros = [];
    private NativeArray<GroupSendState> _send = null!;
    private NativeArray<GroupState> _groups = null!;
    private NativeArray<GroupRecv> _recv = null!;
    private NativeArray<int> _openGroups = null!;
    private TransportStreamId[] _txStreams = [];
    private uint[] _txSerials = [];
    private SpscRing<StreamNotice> _notices = null!;
    private int _freeGroup = -1;
    private int _freeRecv = -1;
    private int _carrierReserve;
    private long _maxReceiveMessage;
    private int _maxSegments;
    private long _groupIntervalMicros;

    /// <summary>Where a group is in its lifecycle (game thread).</summary>
    internal enum GroupPhase : byte
    {
        /// <summary>The channel's open group: admitted messages join it until it is sealed.</summary>
        Filling = 0,

        /// <summary>Sealed, with no stream: the next pass opens one, unless it is waiting for stream credit.</summary>
        Waiting = 1,

        /// <summary>The first send (with Start) is out; nothing more goes out on this group until the start is confirmed.</summary>
        Starting = 2,

        /// <summary>Started: the group's remaining messages flow, the last send carries FIN.</summary>
        Open = 3,

        /// <summary>The peer's stream limit refused the start; the carrier is being canceled back.</summary>
        Refused = 4,

        /// <summary>The stream was stopped or failed: this group is finished, the channel is not.</summary>
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
    public override ChannelMode Mode => ChannelMode.ReliableUnordered;

    /// <summary>Group starts the peer's stream limit refused (they are sent again on a new stream; game thread, tests).</summary>
    internal long StreamsRefused { get; private set; }

    /// <inheritdoc/>
    public override void Initialize(PeerCore core, ReadOnlySpan<ChannelDefinition> channelsOfMode)
    {
        _core = core;
        int count = channelsOfMode.Length;
        _channels = channelsOfMode.ToArray();
        _localOf = new int[core.ChannelCount];
        Array.Fill(_localOf, -1);
        _denseOf = new int[count];
        _maxGroups = new int[count];
        _expiryMicros = new long[count];
        _send = new NativeArray<GroupSendState>(Math.Max(count, 1));
        _openGroups = new NativeArray<int>(Math.Max(count, 1));
        int groupSlots = 0;
        int recvSlots = 0;
        for (int local = 0; local < count; local++)
        {
            ChannelDefinition channel = _channels[local];
            int dense = core.ChannelIndexOf(channel.Id);
            _localOf[dense] = local;
            _denseOf[local] = dense;
            _expiryMicros[local] = channel.ResolveExpiryMicros(core.FlushIntervalMicros);
            ref GroupSendState send = ref _send[local];
            send.ListHead = -1;
            send.ListTail = -1;
            send.FillingGroup = -1;
            _openGroups[local] = 0;

            // Both ends hold the same table, so MaxGroups bounds the streams this end may have open on the channel as well
            // as the peer streams it accepts (PROTOCOL.md §7): a sender that exceeded it would have its own groups reset.
            int maxGroups = Math.Max(channel.MaxGroups, 1);
            _maxGroups[local] = maxGroups;

            // Send side: room for groups that are still filling or waiting on top of those holding a stream (a group keeps
            // its record until its stream shuts down). Receive side: exactly the PROTOCOL.md §7 limit.
            groupSlots += (3 * maxGroups) + 4;
            recvSlots += maxGroups;
        }

        _groups = new NativeArray<GroupState>(Math.Max(groupSlots, 1));
        _recv = new NativeArray<GroupRecv>(Math.Max(recvSlots, 1));
        for (int i = _groups.Length - 1; i >= 0; i--)
        {
            ref GroupState group = ref _groups[i];
            group = default;
            group.Next = _freeGroup;
            _freeGroup = i;
        }

        for (int i = _recv.Length - 1; i >= 0; i--)
        {
            ref GroupRecv recv = ref _recv[i];
            recv = default;
            recv.Next = _freeRecv;
            _freeRecv = i;
        }

        _txStreams = new TransportStreamId[_groups.Length];
        _txSerials = new uint[_groups.Length];
        // At most three notices per live stream between two drains.
        _notices = new SpscRing<StreamNotice>((4 * _groups.Length) + 8);
        _carrierReserve = count + 1;
        _maxReceiveMessage = Math.Min(core.ReceiveBudgetBytes, core.Allocator.MaxBlockSize);
        _maxSegments = Math.Min(MaxSegmentsPerSend, core.Segments.Capacity);
        _groupIntervalMicros = core.GroupMinIntervalMicros;
    }

    /// <summary>Groups the channel holds: filling, waiting for credit, or with a stream (game thread; tests).</summary>
    /// <param name="channelIndex">Dense index of a channel of this engine.</param>
    /// <returns>The number of live groups.</returns>
    internal int GroupsOf(int channelIndex) => _send[_localOf[channelIndex]].GroupCount;

    /// <summary>The group id the channel's next group will carry (game thread; tests).</summary>
    /// <param name="channelIndex">Dense index of a channel of this engine.</param>
    /// <returns>The next group id.</returns>
    internal ulong NextGroupIdOf(int channelIndex) => _send[_localOf[channelIndex]].NextGroupId;

    /// <summary>The phases of the channel's live groups, oldest first (game thread; tests).</summary>
    /// <param name="channelIndex">Dense index of a channel of this engine.</param>
    /// <returns>One phase per live group.</returns>
    internal List<GroupPhase> PhasesOf(int channelIndex)
    {
        List<GroupPhase> phases = [];
        for (int group = _send[_localOf[channelIndex]].ListHead; group >= 0; group = _groups[group].Next)
        {
            phases.Add(_groups[group].Phase);
        }

        return phases;
    }

    /// <summary>Peer group streams open on a channel right now (transport thread's count; tests).</summary>
    /// <param name="channelIndex">Dense index of a channel of this engine.</param>
    /// <returns>The number of accepted peer streams.</returns>
    internal int OpenPeerGroupsOf(int channelIndex) => _openGroups[_localOf[channelIndex]];

    // ------------------------------------------------------------------ send (game thread)

    /// <inheritdoc/>
    public override SendStatus Admit(ref SendRequest request)
    {
        DrainNotices();
        ChannelDefinition channel = request.Channel;
        int dense = request.ChannelIndex;
        int local = _localOf[dense];
        ref ChannelSendCounters counters = ref _core.SendCounters(dense);
        ref GroupSendState send = ref _send[local];
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

        int group = ResolveGroup(local, ref send, channel, length);
        if (group < 0)
        {
            counters.QueueFull++;
            return SendStatus.QueueFull;
        }

        SendEntryTable entries = _core.Entries;
        if (entries.Available <= _carrierReserve || !_core.TryAllocateEntry(channel.Id, SendEntryFlags.None, out int slot))
        {
            // The reserve keeps entries for the carriers that drain the groups (no deadlock on a full table).
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
        header.RawLength = payload.RawLength;
        entries.SetHeaderLength(slot, StreamFraming.WriteFrameHeader(entries.GetHeaderBlock(slot), channel, in header));
        EnginePayload.Commit(_core, slot, ref request, in payload);
        entries.Keys[slot] = header.Key;
        long expiry = request.Options.ExpiryMicros > 0 ? request.Options.ExpiryMicros : _expiryMicros[local];
        if (expiry > 0)
        {
            // The pass's clock stamp, not a QPC per message (ADR 0008 invariant 9).
            long now = _core.CurrentPassMicros;
            entries.Deadlines[slot] = expiry >= long.MaxValue - now ? long.MaxValue : now + expiry;
        }

        ref SendEntry entry = ref entries[slot];
        entry.Aux0 = length;
        entry.Aux1 = InQueue;
        ref GroupState state = ref _groups[group];
        if (request.Options.Mode == SendMode.Immediate)
        {
            entry.Flags |= SendEntryFlags.Immediate;
            state.Flags |= GroupImmediate;
        }

        _core.StampAdmission(slot);
        NativeArray<int> links = entries.Next;
        links[slot] = -1;
        if (state.Tail < 0)
        {
            state.Head = slot;
        }
        else
        {
            links[state.Tail] = slot;
        }

        state.Tail = slot;
        state.Count++;
        state.Bytes += length;
        send.QueueCount++;
        send.QueueBytes += length;
        return SendStatus.Admitted;
    }

    /// <summary>
    /// The group an admitted message joins: the channel's open group, or a new one when there is none or the open one would
    /// exceed <see cref="ChannelDefinition.GroupMaxBytes"/> (PROTOCOL.md §3.2). With no free group slot the open group keeps
    /// growing instead of failing the send; only a channel with no open group at all answers <see cref="SendStatus.QueueFull"/>.
    /// </summary>
    private int ResolveGroup(int local, ref GroupSendState send, ChannelDefinition channel, int length)
    {
        int group = send.FillingGroup;
        if (group >= 0)
        {
            ref GroupState state = ref _groups[group];
            if (state.Count == 0 || state.Bytes + length <= channel.GroupMaxBytes || _freeGroup < 0)
            {
                return group;
            }

            // The group is full: seal it now (PROTOCOL.md §4.5, "or when a container or group fills") and open the next.
            SealFilling(ref send);
        }

        return TakeGroup(local, ref send);
    }

    /// <inheritdoc/>
    public override void FlushChannel(int channelIndex, ref FlushContext flush)
    {
        DrainNotices();
        int local = _localOf[channelIndex];
        ref GroupSendState send = ref _send[local];
        if (send.ListHead < 0)
        {
            return;
        }

        ref ChannelSendCounters counters = ref _core.SendCounters(channelIndex);
        SealForPass(local, ref send, ref flush);
        bool noMoreStreams = false;
        int group = send.ListHead;
        while (group >= 0)
        {
            // Read the link first: a finished group leaves the list inside the loop.
            int next = _groups[group].Next;
            GroupPhase phase = _groups[group].Phase;
            if (phase is GroupPhase.Waiting or GroupPhase.Open)
            {
                bool opening = phase == GroupPhase.Waiting;
                if (opening)
                {
                    DropExpired(group, ref send, ref counters, flush.NowMicros);
                    if (_groups[group].Count == 0)
                    {
                        ReleaseIfDone(group, ref send);
                        group = next;
                        continue;
                    }

                    // A group waits for stream credit the peer took away, and for a free group slot of its channel; either
                    // way every later group of the channel would wait too, so the pass stops opening streams here.
                    if (noMoreStreams || send.StreamedGroups >= _maxGroups[local]
                        || (_groups[group].CreditGeneration != CreditUnrefused
                            && _groups[group].CreditGeneration == _core.StreamCreditGeneration))
                    {
                        noMoreStreams = true;
                        group = next;
                        continue;
                    }
                }

                while (_groups[group].Count > 0 && _groups[group].Phase is GroupPhase.Waiting or GroupPhase.Open)
                {
                    if (flush.BudgetBytes <= 0)
                    {
                        flush.BudgetExhausted = true;
                        return;
                    }

                    if (!SubmitCarrier(local, group, ref send, ref counters, ref flush))
                    {
                        // Only a start the peer's stream limit refused stops the pass from trying the channel's other
                        // groups: a resource shortage (arena, entry table) just defers this group.
                        noMoreStreams |= _groups[group].Phase == GroupPhase.Waiting
                            && _groups[group].CreditGeneration != CreditUnrefused;
                        break;
                    }
                }

                ReleaseIfDone(group, ref send);
            }

            group = next;
        }
    }

    /// <summary>
    /// Seals the channel's open group so this pass sends it (PROTOCOL.md §3.2: the messages admitted between two flushes form
    /// one group), unless <see cref="PeerCore.GroupMinIntervalMicros"/> still bounds stream churn — then the group keeps
    /// filling and the deadline moves to the moment the next stream may open. An <see cref="SendMode.Immediate"/> message in
    /// the group bypasses the wait (PROTOCOL.md §4.5: eligible now).
    /// </summary>
    private void SealForPass(int local, ref GroupSendState send, ref FlushContext flush)
    {
        int filling = send.FillingGroup;
        if (filling < 0 || _groups[filling].Count == 0)
        {
            return;
        }

        if (flush.NowMicros >= send.NextGroupAllowedMicros || (_groups[filling].Flags & GroupImmediate) != 0)
        {
            SealFilling(ref send);
            return;
        }

        if (send.NextGroupAllowedMicros < flush.NextDeadline)
        {
            flush.NextDeadline = send.NextGroupAllowedMicros;
        }
    }

    private void SealFilling(ref GroupSendState send)
    {
        int filling = send.FillingGroup;
        _groups[filling].Phase = GroupPhase.Waiting;
        _groups[filling].CreditGeneration = CreditUnrefused;
        send.FillingGroup = -1;
    }

    /// <summary>Takes a free group record, gives it the channel's next group id and appends it to the channel's list.</summary>
    private int TakeGroup(int local, ref GroupSendState send)
    {
        int group = _freeGroup;
        if (group < 0)
        {
            return -1;
        }

        ref GroupState state = ref _groups[group];
        _freeGroup = state.Next;
        uint serial = state.Serial;
        state = default;
        state.Local = (ushort)local;
        state.Head = -1;
        state.Tail = -1;
        state.Next = -1;
        state.Prev = -1;
        state.StartCarrier = -1;
        state.Phase = GroupPhase.Filling;
        state.CreditGeneration = CreditUnrefused;
        // Serials only ever rise for a record, so notices of an earlier occupant are recognised as stale.
        state.Serial = serial;
        state.GroupId = send.NextGroupId++;
        state.Prev = send.ListTail;
        if (send.ListTail < 0)
        {
            send.ListHead = group;
        }
        else
        {
            _groups[send.ListTail].Next = group;
        }

        send.ListTail = group;
        send.FillingGroup = group;
        send.GroupCount++;
        return group;
    }

    /// <summary>
    /// Returns a group whose messages are all finished and whose carriers have all completed to the free list. Idempotent: a
    /// record already on the free list is left alone, so the second close notice of one stream — the shutdown that always
    /// follows a stop — can never release it twice (ADR 0008: every resource released exactly once on every path).
    /// </summary>
    private void ReleaseIfDone(int group, ref GroupSendState send)
    {
        ref GroupState state = ref _groups[group];
        if ((state.Flags & GroupFreed) != 0)
        {
            return;
        }

        if (state.Phase == GroupPhase.Filling || state.Count > 0 || state.CarriersOutstanding > 0)
        {
            return;
        }

        if (state.Phase is GroupPhase.Starting or GroupPhase.Refused)
        {
            return;
        }

        // A group that still holds a stream keeps its record: the channel's stream slot (and the peer's stream credit)
        // return when the stream shuts down, which is also when the receiving end frees the group's slot, so this end never
        // opens the stream that would push the peer past its MaxGroups.
        if ((state.Flags & GroupCounted) != 0)
        {
            return;
        }

        // One step, not a walk of the channel's list: a pass that finishes many groups of a channel with a large MaxGroups
        // would otherwise cost a link traversal per live group per release.
        int previous = state.Prev;
        int after = state.Next;
        if (previous < 0)
        {
            send.ListHead = after;
        }
        else
        {
            _groups[previous].Next = after;
        }

        if (after < 0)
        {
            send.ListTail = previous;
        }
        else
        {
            _groups[after].Prev = previous;
        }

        ReleaseStreamSlot(ref state, ref send);
        state.Phase = GroupPhase.Closed;
        state.Stream = default;
        state.Head = -1;
        state.Tail = -1;
        state.Prev = -1;

        // Serials only ever rise for a record, so every notice and every carrier tag of the stream it just had is stale from
        // here on: the shutdown that follows a stop, and any event of a stream this end abandoned, no longer match it.
        state.Serial = (state.Serial + 1) & PeerCore.EngineStreamSerialMask;
        state.Flags |= GroupFreed;
        state.Next = _freeGroup;
        _freeGroup = group;
        send.GroupCount--;
    }

    /// <inheritdoc/>
    public override void Flush(ref FlushContext flush)
    {
    }

    /// <inheritdoc/>
    /// <remarks>Nothing is time-driven: the group interval's deadline is lowered by the scheduler pass itself.</remarks>
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
    /// <remarks>Only a message still waiting in its group can be canceled; it completes <see cref="DeliveryStatus.Canceled"/> at the next Poll or Flush.</remarks>
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

        int local = _localOf[dense];
        ref GroupSendState send = ref _send[local];
        NativeArray<int> links = entries.Next;
        for (int group = send.ListHead; group >= 0; group = _groups[group].Next)
        {
            ref GroupState state = ref _groups[group];
            int previous = -1;
            int slot = state.Head;
            while (slot >= 0 && slot != entrySlot)
            {
                previous = slot;
                slot = links[slot];
            }

            if (slot < 0)
            {
                continue;
            }

            int after = links[entrySlot];
            if (previous < 0)
            {
                state.Head = after;
            }
            else
            {
                links[previous] = after;
            }

            if (state.Tail == entrySlot)
            {
                state.Tail = previous;
            }

            state.Count--;
            state.Bytes -= entry.Aux0;
            send.QueueCount--;
            send.QueueBytes -= entry.Aux0;
            entry.Aux1 = Finished;
            _core.QueueLocalCompletion(entrySlot, DeliveryStatus.Canceled);
            ReleaseIfDone(group, ref send);
            return true;
        }

        return false;
    }

    /// <inheritdoc/>
    public override long OldestQueuedStamp()
    {
        long oldest = long.MaxValue;
        for (int local = 0; local < _channels.Length; local++)
        {
            for (int group = _send[local].ListHead; group >= 0; group = _groups[group].Next)
            {
                ref GroupState state = ref _groups[group];

                // A carrier whose start is unconfirmed may still come back (a refused start): its messages count as queued.
                int head = state.Head;
                if (head < 0 && state.StartCarrier >= 0 && state.Phase is GroupPhase.Starting or GroupPhase.Refused)
                {
                    head = _core.Entries.BatchHead[state.StartCarrier];
                }

                if (head >= 0)
                {
                    long stamp = _core.GetAdmissionStamp(head);
                    if (stamp < oldest)
                    {
                        oldest = stamp;
                    }
                }
            }
        }

        return oldest;
    }

    /// <inheritdoc/>
    public override void AddStatistics(int channelIndex, ref ChannelStatistics statistics)
    {
        ref GroupSendState send = ref _send[_localOf[channelIndex]];
        statistics.QueuedMessages = send.QueueCount;
        statistics.QueuedBytes = send.QueueBytes;
        statistics.InFlightMessages = send.InFlightCount;
        statistics.InFlightBytes = send.InFlightBytes;
    }

    /// <inheritdoc/>
    public override void OnPeerClosed()
    {
        DrainNotices();
        for (int local = 0; local < _channels.Length; local++)
        {
            ref GroupSendState send = ref _send[local];
            int group = send.ListHead;
            while (group >= 0)
            {
                int next = _groups[group].Next;
                FinishQueued(group, ref send, DeliveryStatus.Disconnected, inline: true);
                ref GroupState state = ref _groups[group];
                state.Phase = GroupPhase.Closed;
                state.Stream = default;

                // Nothing will shut a stream down after the connection is gone, so the slots go back here.
                ReleaseStreamSlot(ref state, ref send);
                if (state.CarriersOutstanding == 0)
                {
                    ReleaseIfDone(group, ref send);
                }

                group = next;
            }

            send.FillingGroup = -1;
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Group ids are per-channel counters, and every counter restarts in a new epoch (PROTOCOL.md §1, §4.1), so a resumed
    /// session hands out group ids from 0 again. Nothing else is epoch-scoped: the lost connection's groups were dropped by
    /// <see cref="OnReconnecting"/>, and a fresh session starts with empty state.
    /// </remarks>
    public override void OnEpochReset(bool resumed)
    {
        if (!resumed)
        {
            return;
        }

        for (int local = 0; local < _channels.Length; local++)
        {
            _send[local].NextGroupId = 0;
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// The lost connection's streams are gone, so every group record goes back to the free list (its messages have already
    /// completed <see cref="DeliveryStatus.Disconnected"/>), the per-channel bookkeeping — including the streams this end held
    /// open and the churn deadline — is zeroed, each record's serial is bumped so a notice of a lost stream is ignored if one
    /// still arrives, and a half-received group's staging lease and ring reservation are given back.
    /// </remarks>
    public override void OnReconnecting()
    {
        while (_notices.TryDequeue(out _))
        {
        }

        for (int local = 0; local < _channels.Length; local++)
        {
            ref GroupSendState send = ref _send[local];
            int group = send.ListHead;
            while (group >= 0)
            {
                ref GroupState state = ref _groups[group];
                int next = state.Next;
                uint serial = (state.Serial + 1) & PeerCore.EngineStreamSerialMask;
                state = default;
                state.Serial = serial;
                state.Phase = GroupPhase.Closed;
                state.Flags = GroupFreed;
                state.Prev = -1;
                state.Next = _freeGroup;
                _freeGroup = group;
                group = next;
            }

            send.ListHead = -1;
            send.ListTail = -1;
            send.FillingGroup = -1;
            send.QueueCount = 0;
            send.QueueBytes = 0;
            send.InFlightCount = 0;
            send.InFlightBytes = 0;
            send.GroupCount = 0;
            send.StreamedGroups = 0;
            send.NextGroupAllowedMicros = 0;
            _openGroups[local] = 0;
        }

        for (int group = 0; group < _txStreams.Length; group++)
        {
            _txStreams[group] = default;
            _txSerials[group] = 0;
        }

        _freeRecv = -1;
        for (int record = _recv.Length - 1; record >= 0; record--)
        {
            ref GroupRecv recv = ref _recv[record];
            ReleaseReceive(ref recv);
            recv = default;
            recv.Next = _freeRecv;
            _freeRecv = record;
        }
    }

    /// <summary>Drops messages at the head of a group whose expiry has passed (only while the group has no stream yet).</summary>
    private void DropExpired(int group, ref GroupSendState send, ref ChannelSendCounters counters, long now)
    {
        SendEntryTable entries = _core.Entries;
        NativeArray<long> deadlines = entries.Deadlines;
        NativeArray<int> links = entries.Next;
        ref GroupState state = ref _groups[group];
        int slot = state.Head;
        while (slot >= 0)
        {
            long deadline = deadlines[slot];
            if (deadline == 0 || now <= deadline)
            {
                break;
            }

            // PROTOCOL.md §4.5: expiry is evaluated at scheduling time. Once the group's stream is open its messages are
            // committed to that stream, so only a group still waiting drops anything.
            int next = links[slot];
            ref SendEntry entry = ref entries[slot];
            state.Head = next;
            if (next < 0)
            {
                state.Tail = -1;
            }

            state.Count--;
            state.Bytes -= entry.Aux0;
            send.QueueCount--;
            send.QueueBytes -= entry.Aux0;
            entry.Aux1 = Finished;
            counters.Expired++;
            _core.QueueLocalCompletion(slot, DeliveryStatus.Expired);
            slot = next;
        }
    }

    /// <summary>
    /// Gathers the head of one group into a stream send (a carrier entry and a segment run), opening the group's stream first
    /// when it has none and setting FIN on the send that carries its last message. Returns <see langword="false"/> when this
    /// group can send no more in this pass (resources, refusal, a start still unconfirmed).
    /// </summary>
    private bool SubmitCarrier(int local, int group, ref GroupSendState send, ref ChannelSendCounters counters, ref FlushContext flush)
    {
        ChannelDefinition channel = _channels[local];
        SendEntryTable entries = _core.Entries;
        SegmentArena arena = _core.Segments;
        ref GroupState state = ref _groups[group];
        bool opening = state.Phase == GroupPhase.Waiting;
        int preamble = opening ? 1 : 0;
        int capacity = Math.Min(_maxSegments, preamble + (2 * Math.Min(state.Count, MaxSegmentsPerSend)));
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
        if (opening && !TryOpenStream(group, ref state, channel, credit))
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
            int written = StreamFraming.WriteGroupPreamble(entries.GetHeaderBlock(carrier), channel.Id, state.GroupId);
            entries.SetHeaderLength(carrier, written);
            segments[used++] = entries[carrier].Header;
            bytes = written;
        }

        NativeArray<int> links = entries.Next;
        long budget = flush.BudgetBytes - bytes;
        int first = -1;
        int last = -1;
        int members = 0;
        long payloadBytes = 0;
        long admitted = 0;
        bool priority = false;
        int slot = state.Head;
        while (slot >= 0 && used + 2 <= capacity)
        {
            if (members > 0 && budget <= 0)
            {
                flush.BudgetExhausted = true;
                break;
            }

            int next = links[slot];
            ref SendEntry entry = ref entries[slot];
            state.Head = next;
            if (next < 0)
            {
                state.Tail = -1;
            }

            state.Count--;
            state.Bytes -= entry.Aux0;
            send.QueueCount--;
            send.QueueBytes -= entry.Aux0;
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
            _core.DiscardEntry(carrier);
            arena.Free(run);
            if (opening)
            {
                AbandonStream(ref state);
            }

            return false;
        }

        entries.BatchHead[carrier] = first;
        entries.BatchCount[carrier] = members;
        ref SendEntry carrierEntry = ref entries[carrier];
        carrierEntry.Aux0 = run;
        carrierEntry.Aux1 = MakeCarrierTag(group, state.Serial);
        TransportSendFlags flags = opening ? TransportSendFlags.Start : TransportSendFlags.None;
        if (priority)
        {
            flags |= TransportSendFlags.Priority;
        }

        // PROTOCOL.md §3.2: the group's stream is closed with FIN after its last message.
        bool fin = state.Head < 0;
        if (fin)
        {
            flags |= TransportSendFlags.Fin;
            carrierEntry.Flags |= SendEntryFlags.Fin;
        }

        // Datagrams the scheduler handed to the packer earlier in this pass (channels of higher priority) leave first.
        _core.Packer.SubmitPending(ref flush);
        TransportStatus status = _core.SubmitStream(state.Stream, segments, used, carrier, flags);
        if (status != TransportStatus.Success)
        {
            // No completion follows a refused call: the messages go back to the head of the group in order.
            RequeueFront(group, ref send, first, wasInFlight: false, ref counters);
            entries.ClearBatch(carrier);
            _core.DiscardEntry(carrier);
            arena.Free(run);
            if (opening)
            {
                AbandonStream(ref state);
                if (status == TransportStatus.StreamLimitReached)
                {
                    state.CreditGeneration = credit;
                    StreamsRefused++;
                }
                else
                {
                    // The open succeeded and only the send failed (a transport going away, a resource shortage): no stream
                    // limit refused this group, so it must not wait for credit it already has — the next pass tries again.
                    state.CreditGeneration = CreditUnrefused;
                }
            }

            return false;
        }

        send.InFlightCount += members;
        send.InFlightBytes += admitted;
        state.CarriersOutstanding++;
        if (fin)
        {
            state.Flags |= GroupFinSent;
        }

        if (opening)
        {
            state.Phase = GroupPhase.Starting;
            state.StartCarrier = carrier;
            state.Flags |= GroupCounted;
            send.StreamedGroups++;

            // PROTOCOL.md §3.2: GroupMinIntervalMicros bounds stream churn — the channel's next group waits for it.
            long now = flush.NowMicros;
            send.NextGroupAllowedMicros = _groupIntervalMicros >= long.MaxValue - now ? long.MaxValue : now + _groupIntervalMicros;
        }

        counters.Sent += members;
        counters.Bytes += payloadBytes;
        flush.BudgetBytes -= bytes;
        flush.BytesSubmitted += bytes;
        PeerCounters peerCounters = _core.Counters;
        peerCounters.StreamSends++;
        peerCounters.StreamBytesSent += bytes;
        return opening ? false : state.Count > 0;
    }

    private static long MakeCarrierTag(int group, uint serial) => CarrierBase + ((long)group << SerialBits) + (serial & PeerCore.EngineStreamSerialMask);

    private bool TryOpenStream(int group, ref GroupState state, ChannelDefinition channel, int credit)
    {
        uint serial = (state.Serial + 1) & PeerCore.EngineStreamSerialMask;
        // The context carries this engine's group slot where the ordered engine carries its channel index: the peer only
        // routes OnStreamStarted by the context's mode (docs/design/session-layer.md §7.5).
        ulong context = PeerCore.MakeEngineStreamContext(ChannelMode.ReliableUnordered, group, serial);
        TransportStatus status = _core.OpenStream(StreamKind.Unidirectional, context, (ushort)(channel.Priority * 257), out TransportStreamId id);
        if (status != TransportStatus.Success)
        {
            if (status == TransportStatus.StreamLimitReached)
            {
                state.CreditGeneration = credit;
            }

            return false;
        }

        state.Stream = id;
        state.Serial = serial;
        state.CreditGeneration = credit;
        state.CarriersOutstanding = 0;
        state.StartCarrier = -1;
        return true;
    }

    /// <summary>Releases a stream that never started (no accepted send carried Start): no callback follows for it.</summary>
    private void AbandonStream(ref GroupState state)
    {
        _core.Transport?.CloseStream(state.Stream);
        state.Stream = default;
        state.Phase = GroupPhase.Waiting;
    }

    private void OnCarrierCompleted(int carrier, in CompletionEntry completion)
    {
        // A refusal or stop reported before this completion decides what happens to the carried messages.
        DrainNotices();
        SendEntryTable entries = _core.Entries;
        ref SendEntry carrierEntry = ref entries[carrier];
        long tag = carrierEntry.Aux1 - CarrierBase;
        int group = (int)(tag >> SerialBits);
        uint serial = (uint)(tag & PeerCore.EngineStreamSerialMask);
        ref GroupState state = ref _groups[group];

        // The carrier's own channel, never the record's: a record that was released and taken again may belong to another
        // channel by now, and it is this carrier's members whose counters must be decremented.
        int local = _localOf[_core.ChannelIndexOf(carrierEntry.Channel)];
        ref GroupSendState send = ref _send[local];
        ref ChannelSendCounters counters = ref _core.SendCounters(_denseOf[local]);
        bool current = serial == state.Serial;
        if (current)
        {
            state.CarriersOutstanding--;
            if (state.StartCarrier == carrier)
            {
                state.StartCarrier = -1;
            }
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
        else if (current && state.Phase == GroupPhase.Refused)
        {
            // The peer's stream limit refused the start: nothing reached the peer. The group goes out again, in admission
            // order, on a new stream once the peer grants credit (PROTOCOL.md §3.2: the group waits, it never fails).
            RequeueFront(group, ref send, first, wasInFlight: true, ref counters);
            if (state.CarriersOutstanding == 0)
            {
                state.Phase = GroupPhase.Waiting;
                state.Stream = default;
                state.Flags = (byte)(state.Flags & ~GroupFinSent);
                ReleaseStreamSlot(ref state, ref send);
            }
        }
        else
        {
            FinishMembers(ref send, first, DeliveryStatus.Failed);
            if (current && state.Phase is GroupPhase.Open or GroupPhase.Starting)
            {
                FailGroup(group, ref send, DeliveryStatus.Failed);
            }
        }

        // The carrier is untracked: this only frees its slot.
        _core.CompleteEntry(carrier, DeliveryStatus.Delivered);
        ReleaseIfDone(group, ref send);
    }

    private void FinishMembers(ref GroupSendState send, int first, DeliveryStatus status)
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

    private void RequeueFront(int group, ref GroupSendState send, int first, bool wasInFlight, ref ChannelSendCounters counters)
    {
        SendEntryTable entries = _core.Entries;
        NativeArray<int> links = entries.Next;
        ref GroupState state = ref _groups[group];
        int last = -1;
        for (int slot = first; slot >= 0; slot = links[slot])
        {
            ref SendEntry entry = ref entries[slot];
            entry.Aux1 = InQueue;
            state.Count++;
            state.Bytes += entry.Aux0;
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

        links[last] = state.Head;
        if (state.Head < 0)
        {
            state.Tail = last;
        }

        state.Head = first;
    }

    /// <summary>
    /// The group's stream failed or was stopped: its remaining messages end with <paramref name="status"/> and the group is
    /// finished. The channel and its other groups are unaffected (PROTOCOL.md §3.2, §6).
    /// </summary>
    private void FailGroup(int group, ref GroupSendState send, DeliveryStatus status)
    {
        FinishQueued(group, ref send, status, inline: false);
        ref GroupState state = ref _groups[group];
        state.Phase = GroupPhase.Closed;
        state.Stream = default;
        ReleaseStreamSlot(ref state, ref send);
        if (send.FillingGroup == group)
        {
            send.FillingGroup = -1;
        }
    }

    /// <summary>Gives back the channel's group-stream slot when a group stops holding a stream (at most once per open).</summary>
    private static void ReleaseStreamSlot(ref GroupState state, ref GroupSendState send)
    {
        if ((state.Flags & GroupCounted) != 0)
        {
            state.Flags = (byte)(state.Flags & ~GroupCounted);
            send.StreamedGroups--;
        }
    }

    /// <summary>Finishes the messages a group still holds: queued completions inside a pass, inline once the peer is closed.</summary>
    private void FinishQueued(int group, ref GroupSendState send, DeliveryStatus status, bool inline)
    {
        SendEntryTable entries = _core.Entries;
        NativeArray<int> links = entries.Next;
        ref GroupState state = ref _groups[group];
        int slot = state.Head;
        state.Head = -1;
        state.Tail = -1;
        send.QueueCount -= state.Count;
        send.QueueBytes -= state.Bytes;
        state.Count = 0;
        state.Bytes = 0;
        while (slot >= 0)
        {
            int next = links[slot];
            entries[slot].Aux1 = Finished;
            if (inline)
            {
                _core.CompleteEntry(slot, status);
            }
            else
            {
                _core.QueueLocalCompletion(slot, status);
            }

            slot = next;
        }
    }

    /// <summary>Applies the transport-thread notices of this engine's streams to the group states (game thread).</summary>
    private void DrainNotices()
    {
        SpscRing<StreamNotice> ring = _notices;
        while (ring.TryDequeue(out StreamNotice notice))
        {
            int group = notice.Group;
            ref GroupState state = ref _groups[group];
            if (notice.Serial != state.Serial)
            {
                continue; // an earlier stream of this group, or of an earlier occupant of the record
            }

            ref GroupSendState send = ref _send[state.Local];
            switch (notice.Kind)
            {
                case NoticeKind.Started:
                    if (state.Phase == GroupPhase.Starting)
                    {
                        state.Phase = GroupPhase.Open;
                        state.StartCarrier = -1;
                    }

                    break;
                case NoticeKind.Refused:
                    if (state.Phase == GroupPhase.Starting)
                    {
                        StreamsRefused++;
                        if (state.CarriersOutstanding == 0)
                        {
                            state.Phase = GroupPhase.Waiting;
                            state.Stream = default;
                            state.Flags = (byte)(state.Flags & ~GroupFinSent);
                            ReleaseStreamSlot(ref state, ref send);
                        }
                        else
                        {
                            state.Phase = GroupPhase.Refused;
                        }
                    }

                    break;
                case NoticeKind.ShutDown:
                    // After FIN the shutdown is the group's ordinary end; before it, the group will not finish.
                    if (state.Phase is GroupPhase.Starting or GroupPhase.Open && (state.Flags & GroupFinSent) == 0)
                    {
                        FailGroup(group, ref send, DeliveryStatus.Disconnected);
                    }

                    // The stream is gone: its slot (and the peer's credit) are free again.
                    ReleaseStreamSlot(ref state, ref send);
                    break;
                default:
                    // Stopped by the peer (STOP_SENDING) or failed to start: this group is finished, the channel is not.
                    if (state.Phase is GroupPhase.Starting or GroupPhase.Open)
                    {
                        FailGroup(group, ref send, _core.IsTransportClosing ? DeliveryStatus.Disconnected : DeliveryStatus.Failed);
                    }

                    ReleaseStreamSlot(ref state, ref send);
                    break;
            }

            ReleaseIfDone(group, ref send);
        }
    }

    // ------------------------------------------------------------------ transport thread

    /// <inheritdoc/>
    /// <remarks>Group channels carry no datagrams (the peer rejects them before they get here); counted as dropped.</remarks>
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

        // PROTOCOL.md §7: at most MaxGroups concurrent peer group streams per channel; the rest are reset LimitExceeded.
        int record = _freeRecv;
        if (_openGroups[local] >= Math.Max(_channels[local].MaxGroups, 1) || record < 0)
        {
            return StreamAccept.Reject(QuiclyErrorCode.LimitExceeded);
        }

        ref GroupRecv recv = ref _recv[record];
        _freeRecv = recv.Next;
        recv = default;
        recv.Next = -1;
        recv.Local = local;
        recv.Stream = id;
        recv.Lease = BufferLease.Empty;
        recv.Flags = RecvInUse;
        _openGroups[local]++;
        return StreamAccept.Accept(record);
    }

    /// <inheritdoc/>
    public override StreamConsume OnStreamMessage(ref StreamMessageContext message)
    {
        int record = (int)message.Cookie;
        ref GroupRecv recv = ref _recv[record];
        switch (message.Phase)
        {
            case StreamMessagePhase.Start:
            {
                int length = message.Header.Length;
                if (length > _maxReceiveMessage)
                {
                    // Within the channel's MaxMessageSize but above this peer's receive budget or pool: pending would stall
                    // the group for good, so the group is reset and the connection survives (PROTOCOL.md §6).
                    _core.RecvCounters(message.ChannelIndex).TooLarge++;
                    return StreamConsume.ResetStream(QuiclyErrorCode.LimitExceeded);
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
                recv.RawLength = message.Header.RawLength;
                recv.Flags |= RecvReserved;
                return StreamConsume.Continue;
            }

            case StreamMessagePhase.Chunk:
            {
                ReadOnlySpan<byte> chunk = message.Chunk;
                if ((recv.Flags & RecvReserved) == 0 || chunk.Length > recv.Length - recv.Filled)
                {
                    // Payload bytes outside a staged message: no Start reserved this record's lease, or the chunk is longer
                    // than the frame header promised. The peer's parser produces neither, so this is defence in depth — the
                    // engine never writes through a lease it does not hold.
                    return StreamConsume.ResetStream(QuiclyErrorCode.ProtocolViolation);
                }

                chunk.CopyTo(new Span<byte>(_core.GetPointer(in recv.Lease) + recv.Filled, recv.Length - recv.Filled));
                recv.Filled += chunk.Length;
                return StreamConsume.Continue;
            }

            case StreamMessagePhase.End:
            {
                ReceiveEntry entry = default;
                entry.Channel = message.Channel;
                entry.Flags = recv.RawLength > 0 ? ReceiveFlags.Compressed : ReceiveFlags.None;
                entry.Key = recv.Key;
                entry.Lease = recv.Lease;
                entry.Length = recv.Length;
                entry.RawLength = recv.RawLength;
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
                return StreamConsume.ResetStream(QuiclyErrorCode.ProtocolViolation);
        }
    }

    /// <inheritdoc/>
    public override void OnStreamClosed(TransportStreamId id, bool aborted, ulong errorCode)
    {
        for (int record = 0; record < _recv.Length; record++)
        {
            ref GroupRecv recv = ref _recv[record];
            if ((recv.Flags & RecvInUse) != 0 && recv.Stream == id)
            {
                ReleaseReceive(ref recv);
                _openGroups[recv.Local]--;
                recv.Flags = 0;
                recv.Stream = default;
                recv.Next = _freeRecv;
                _freeRecv = record;
                return;
            }
        }

        if (!id.IsValid)
        {
            return;
        }

        for (int group = 0; group < _txStreams.Length; group++)
        {
            if (_txStreams[group] == id)
            {
                Post(group, _txSerials[group], aborted ? NoticeKind.Stopped : NoticeKind.ShutDown);

                // The stream is gone either way — a stop ends this end's sending and the peer's shutdown handling closes it —
                // so the transport-side entries go with it and no later event of that id resolves to this group again.
                _txStreams[group] = default;
                _txSerials[group] = 0;
                return;
            }
        }
    }

    /// <inheritdoc/>
    public override void OnStreamStarted(TransportStreamId id, ulong context, TransportStatus status)
    {
        if (!PeerCore.TryDecodeEngineStreamContext(context, out ChannelMode mode, out int group, out uint serial)
            || mode != ChannelMode.ReliableUnordered || (uint)group >= (uint)_txStreams.Length)
        {
            return;
        }

        _txStreams[group] = id;
        _txSerials[group] = serial;
        NoticeKind kind = status switch
        {
            TransportStatus.Success => NoticeKind.Started,
            TransportStatus.StreamLimitReached => NoticeKind.Refused,
            _ => NoticeKind.StartFailed,
        };
        Post(group, serial, kind);
    }

    private void Post(int group, uint serial, NoticeKind kind)
    {
        StreamNotice notice = new() { Group = group, Serial = serial, Kind = kind };
        if (!_notices.TryEnqueue(in notice))
        {
            // Sized for every live stream's notices between two drains; cannot happen.
            _core.Counters.CallbackFaults++;
        }
    }

    private void ReleaseReceive(ref GroupRecv recv)
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
            for (int record = 0; record < _recv.Length; record++)
            {
                ref GroupRecv recv = ref _recv[record];
                if (!recv.Lease.IsEmpty)
                {
                    _core.ReturnReceive(in recv.Lease);
                    recv.Lease = BufferLease.Empty;
                }
            }

            _recv.Dispose();
        }

        _send?.Dispose();
        _groups?.Dispose();
        _openGroups?.Dispose();
        _notices?.Dispose();
    }

    /// <summary>A transport-thread event of one of this engine's group streams, handed to the game thread.</summary>
    private struct StreamNotice
    {
        /// <summary>The group record the stream belongs to.</summary>
        public int Group;

        /// <summary>Serial of the stream (notices of earlier streams are ignored).</summary>
        public uint Serial;

        /// <summary>What happened.</summary>
        public NoticeKind Kind;
    }

    /// <summary>Send-side state of one group channel: one cache line, native memory, game thread only.</summary>
    [StructLayout(LayoutKind.Explicit, Size = 64)]
    private struct GroupSendState
    {
        /// <summary>Oldest live group of the channel (-1 = none); groups chain through <see cref="GroupState.Next"/>.</summary>
        [FieldOffset(0)] public int ListHead;

        /// <summary>Newest live group of the channel (-1 = none).</summary>
        [FieldOffset(4)] public int ListTail;

        /// <summary>The group admitted messages join (-1 = none open).</summary>
        [FieldOffset(8)] public int FillingGroup;

        /// <summary>Messages in the channel's groups that have not been handed to the transport.</summary>
        [FieldOffset(12)] public int QueueCount;

        /// <summary>Admitted payload bytes of <see cref="QueueCount"/>.</summary>
        [FieldOffset(16)] public long QueueBytes;

        /// <summary>Admitted payload bytes handed to the transport and not yet completed.</summary>
        [FieldOffset(24)] public long InFlightBytes;

        /// <summary>Messages handed to the transport and not yet completed.</summary>
        [FieldOffset(32)] public int InFlightCount;

        /// <summary>Live groups of the channel.</summary>
        [FieldOffset(36)] public int GroupCount;

        /// <summary>Group id of the channel's next group (a per-channel counter, PROTOCOL.md §3.2).</summary>
        [FieldOffset(40)] public ulong NextGroupId;

        /// <summary>Clock micros before which no new group stream opens (<see cref="PeerCore.GroupMinIntervalMicros"/>).</summary>
        [FieldOffset(48)] public long NextGroupAllowedMicros;

        /// <summary>Groups of the channel that hold a stream (bounded by <see cref="ChannelDefinition.MaxGroups"/>).</summary>
        [FieldOffset(56)] public int StreamedGroups;
    }

    /// <summary>One group: its messages, its stream and its phase. One cache line, native memory, game thread only.</summary>
    [StructLayout(LayoutKind.Explicit, Size = 64)]
    private struct GroupState
    {
        /// <summary>Previous live group of the channel (-1 = none): the list is doubly linked, so a finished group leaves it in O(1).</summary>
        [FieldOffset(0)] public int Prev;

        /// <summary>First message entry not yet handed to the transport (-1 = none).</summary>
        [FieldOffset(4)] public int Head;

        /// <summary>Last message entry not yet handed to the transport (-1 = none).</summary>
        [FieldOffset(8)] public int Tail;

        /// <summary>Messages between <see cref="Head"/> and <see cref="Tail"/>.</summary>
        [FieldOffset(12)] public int Count;

        /// <summary>Admitted payload bytes of <see cref="Count"/> (bounded by <see cref="ChannelDefinition.GroupMaxBytes"/>).</summary>
        [FieldOffset(16)] public long Bytes;

        /// <summary>The group id in the stream's preamble.</summary>
        [FieldOffset(24)] public ulong GroupId;

        /// <summary>The group's stream (valid from the open until it ends or is abandoned).</summary>
        [FieldOffset(32)] public TransportStreamId Stream;

        /// <summary>Serial of the current stream (in its context; notices of earlier streams are ignored).</summary>
        [FieldOffset(40)] public uint Serial;

        /// <summary><see cref="PeerCore.StreamCreditGeneration"/> read before the last open, or <see cref="CreditUnrefused"/>.</summary>
        [FieldOffset(44)] public int CreditGeneration;

        /// <summary>Carriers of the current stream not yet completed.</summary>
        [FieldOffset(48)] public int CarriersOutstanding;

        /// <summary>Next live group of the channel, or the next free record (-1 = none).</summary>
        [FieldOffset(52)] public int Next;

        /// <summary>The carrier that carried Start while the start is unconfirmed, else -1.</summary>
        [FieldOffset(56)] public int StartCarrier;

        /// <summary>Lifecycle of the group.</summary>
        [FieldOffset(60)] public GroupPhase Phase;

        /// <summary><see cref="GroupImmediate"/>, <see cref="GroupFinSent"/>, <see cref="GroupCounted"/>, <see cref="GroupFreed"/>.</summary>
        [FieldOffset(61)] public byte Flags;

        /// <summary>Engine-local index of the group's channel (a channel count never needs more).</summary>
        [FieldOffset(62)] public ushort Local;
    }

    /// <summary>Receive-side state of one peer group stream: one cache line, native memory, transport thread only.</summary>
    [StructLayout(LayoutKind.Explicit, Size = 64)]
    private struct GroupRecv
    {
        /// <summary>The peer's stream.</summary>
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

        /// <summary>Engine-local index of the stream's channel.</summary>
        [FieldOffset(44)] public int Local;

        /// <summary>Next free record (-1 = none).</summary>
        [FieldOffset(48)] public int Next;

        /// <summary><see cref="RecvInUse"/>, <see cref="RecvReserved"/>.</summary>
        [FieldOffset(52)] public byte Flags;
    }
}
