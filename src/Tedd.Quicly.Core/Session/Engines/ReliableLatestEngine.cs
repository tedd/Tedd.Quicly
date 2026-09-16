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
/// The <see cref="ChannelMode.ReliableLatest"/> engine (PROTOCOL.md §2.1, §2.3, §3.2, §4.4; docs/design/session-layer.md
/// §7.5): intermediate versions of a key may be discarded, the latest one is eventually delivered while the epoch lives.
/// One instance per peer owns the structure-of-arrays state of every ReliableLatest channel.
/// </summary>
/// <remarks>
/// <para><b>Send (game thread).</b> Every key has a <see cref="KeySendSlot"/> and at most one live <em>value</em>: an
/// unsubmitted send entry that owns the value's payload lease, its 32-byte datagram header, its tracking token and the
/// version taken from the channel's 32-bit counter. Each transmission is a separate, untracked entry that points at the
/// value's payload segment (so the bytes are sent again without a copy, ADR 0008 invariant 1) and goes out through
/// <see cref="PeerCore.Packer"/> — small values as datagrams, packed with other messages when they fit — or, for a value
/// that does not fit one datagram, on a one-message group stream (§3.2 and §8: preamble <c>ChannelId, GroupId = version</c>,
/// frame <c>Length, Sequence = version, Key, RawLength</c>, FIN). <see cref="Admit"/> replaces the pending value of a key:
/// a value with no transmission outstanding completes <see cref="DeliveryStatus.Superseded"/> at once, one still in flight
/// is marked and completed when its last transmission completes (its lease must stay valid until then), and a still-open
/// large-value stream is aborted.</para>
/// <para><b>Retransmission</b> (PROTOCOL.md §4.4). <see cref="DeliveryStatus.Sent"/> and a transport acknowledgement both
/// mean only "it left this host" — the <c>LatestAck</c> is the sole source of <see cref="DeliveryStatus.Delivered"/>. A
/// transmission that the transport reports lost, canceled or failed schedules an immediate retry; the timer backstop is
/// <c>clamp(1.5 × RTT, MinRetry 20 ms, MaxRetry 1 s)</c>, doubled per attempt. A version that reaches
/// <see cref="MaxTransmissions"/> transmissions or <see cref="VersionBudgetMicros"/> completes
/// <see cref="DeliveryStatus.Failed"/>. Retries run in <see cref="Flush"/>, after every channel's fresh traffic, and are
/// bounded by a per-peer byte bucket (<see cref="PeerOptions.MaxRetryBytesPerSecond"/> /
/// <see cref="PeerOptions.RetryShareOfEstimatedBandwidth"/>).</para>
/// <para><b>Receive (transport thread).</b> Only a version newer than the key's last accepted one is accepted, into the
/// channel's coalescing mailbox (ReliableLatest always coalesces, so no receive-ring entry and no reservation); an older or
/// duplicate version re-acks the current one, so a lost ack cannot stall completion. Local drops answer
/// <c>LatestReject</c> with the reason. Acks and rejects are coalesced per key (highest version wins) and sent from the
/// game thread at most once per <see cref="PeerOptions.AckDelay"/> as high-priority control datagrams, with the control
/// stream as the fallback.</para>
/// <para><b>Key retirement.</b> <see cref="RetireKey"/> completes the key's live value, sends <c>KeyRetired</c> (0x17) on
/// the control stream and frees the send slot; a received <c>KeyRetired</c> frees the receive slot and delivers a message
/// with <see cref="ReceiveFlags.KeyRetired"/>.</para>
/// </remarks>
internal sealed unsafe partial class ReliableLatestEngine : ChannelEngine
{
    /// <summary>Transmissions of one version before it completes <see cref="DeliveryStatus.Failed"/> (PROTOCOL.md §4.4).</summary>
    public const int MaxTransmissions = 16;

    /// <summary>Lifetime of one version before it completes <see cref="DeliveryStatus.Failed"/> (PROTOCOL.md §4.4).</summary>
    public const long VersionBudgetMicros = 30_000_000;

    /// <summary>Shortest retry interval (PROTOCOL.md §4.4 <c>MinRetry</c>).</summary>
    public const long MinRetryMicros = 20_000;

    /// <summary>Longest retry interval (PROTOCOL.md §4.4 <c>MaxRetry</c>).</summary>
    public const long MaxRetryMicros = 1_000_000;

    /// <summary>Retry bytes per second when no estimate is available (a floor, so a stalled key always makes progress).</summary>
    public const long MinRetryBytesPerSecond = 16 * 1024;

    /// <summary>Datagrams one coalesced ack pass may send (a batch that does not fit continues in the next datagram).</summary>
    public const int MaxAckDatagramsPerPass = 8;

    private const int AckFrameBytes = 1200;
    private const int InitialKeySlots = 64;

    /// <summary>Keys the ack hand-off ring holds; beyond that the per-channel sweep finds the versions still waiting.</summary>
    private const int MaxAckRingSlots = 1024;

    // Aux1 of a value entry: the marker, its key slot, its outstanding transmissions, its flags and a pending status.
    private const long ValueMarker = 1L << 48;
    private const int OutstandingShift = 32;
    private const long OutstandingMask = 0xFFL << OutstandingShift;
    private const int FlagsShift = 40;
    private const long FlagsMask = 0xFFL << FlagsShift;
    private const int StatusShift = 52;
    private const long StatusMask = 0xFL << StatusShift;

    private PeerCore _core = null!;
    private ChannelDefinition[] _channels = [];
    private int[] _localOf = [];
    private int[] _denseOf = [];
    private bool[] _streamBlocked = [];
    private LatestSendKeys[] _sendKeys = [];
    private NativeArray<LatestSendState> _send = null!;
    private TokenBucket _retryBucket;
    private long _retryRate;
    private long _ackDelayMicros;
    private long _nextAckMicros;
    private double _retryShare;
    private long _retryCap;

    /// <summary>Where a value entry is and what it still owes.</summary>
    [Flags]
    private enum ValueFlags : byte
    {
        None = 0,

        /// <summary>The value waits in the channel's fresh queue.</summary>
        Queued = 1,

        /// <summary>The value waits in the channel's retry queue.</summary>
        Retrying = 2,

        /// <summary>A newer value of the key replaced it; it completes <see cref="DeliveryStatus.Superseded"/>.</summary>
        Superseded = 4,

        /// <summary>A terminal status is known and waits for the last transmission to complete.</summary>
        Finish = 8,

        /// <summary>The value was handed to the transport at least once (it counts as in flight).</summary>
        Transmitted = 16,
    }

    /// <inheritdoc/>
    public override ChannelMode Mode => ChannelMode.ReliableLatest;

    /// <inheritdoc/>
    public override void Initialize(PeerCore core, ReadOnlySpan<ChannelDefinition> channelsOfMode)
    {
        _core = core;
        int count = channelsOfMode.Length;
        _channels = channelsOfMode.ToArray();
        _localOf = new int[core.ChannelCount];
        Array.Fill(_localOf, -1);
        _denseOf = new int[count];
        _streamBlocked = new bool[count];
        _sendKeys = new LatestSendKeys[count];
        _send = new NativeArray<LatestSendState>(Math.Max(count, 1));
        InitializeReceive(core, count);
        for (int local = 0; local < count; local++)
        {
            ChannelDefinition channel = _channels[local];
            int dense = core.ChannelIndexOf(channel.Id);
            _localOf[dense] = local;
            _denseOf[local] = dense;
            _sendKeys[local] = new LatestSendKeys(channel);
            ref LatestSendState send = ref _send[local];
            send.NextVersion = 1;
            send.PendingHead = -1;
            send.PendingTail = -1;
            send.RetryHead = -1;
            send.RetryTail = -1;
            send.NextRetryMicros = long.MaxValue;
        }

        _ackDelayMicros = core.AckDelayMicros;
        _retryShare = core.RetryShareOfEstimatedBandwidth;
        _retryCap = core.MaxRetryBytesPerSecond;
        _retryRate = 0;
    }

    /// <summary>The version the next value of a channel will carry (game thread; tests).</summary>
    /// <param name="channelIndex">Dense index of a channel of this engine.</param>
    /// <returns>The next version.</returns>
    internal uint NextVersionOf(int channelIndex) => _send[_localOf[channelIndex]].NextVersion;

    /// <summary>
    /// Test hook: starts the channel's version counter at <paramref name="version"/> so the 32-bit roll-over can be
    /// exercised without sending 2^32 messages (game thread).
    /// </summary>
    /// <param name="channelIndex">Dense index of a channel of this engine.</param>
    /// <param name="version">The next version to hand out (0 is skipped, as on the wrap).</param>
    internal void SetNextVersion(int channelIndex, uint version) => _send[_localOf[channelIndex]].NextVersion = version == 0 ? 1u : version;

    /// <summary>Keys with a live (unacknowledged) value on a channel (game thread; tests and statistics).</summary>
    /// <param name="channelIndex">Dense index of a channel of this engine.</param>
    /// <returns>The number of live keys.</returns>
    internal int LiveKeys(int channelIndex) => _send[_localOf[channelIndex]].LiveKeys;

    /// <summary>The highest version the peer acknowledged for a key, or 0 (game thread; tests).</summary>
    /// <param name="channelIndex">Dense index of a channel of this engine.</param>
    /// <param name="key">The key.</param>
    /// <returns>The acknowledged version.</returns>
    internal uint AckedVersionOf(int channelIndex, ulong key)
    {
        LatestSendKeys keys = _sendKeys[_localOf[channelIndex]];
        return keys.TryGet(key, out int slot) ? keys[slot].AckedVersion : 0;
    }

    /// <inheritdoc/>
    public override long OldestQueuedStamp()
    {
        long oldest = long.MaxValue;
        for (int local = 0; local < _channels.Length; local++)
        {
            ref LatestSendState send = ref _send[local];
            int head = send.PendingHead >= 0 ? send.PendingHead : send.RetryHead;
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
        ref LatestSendState send = ref _send[_localOf[channelIndex]];
        statistics.QueuedMessages = send.PendingCount;
        statistics.QueuedBytes = send.PendingBytes;
        statistics.InFlightMessages = send.InFlightCount;
        statistics.InFlightBytes = send.InFlightBytes;
    }

    // ------------------------------------------------------------------ admission (game thread)

    /// <inheritdoc/>
    public override SendStatus Admit(ref SendRequest request)
    {
        DrainNotices();
        ChannelDefinition channel = request.Channel;
        int dense = request.ChannelIndex;
        int local = _localOf[dense];
        ref ChannelSendCounters counters = ref _core.SendCounters(dense);
        int length = request.Kind == SendPayloadKind.Gather ? EnginePayload.GatherLength(request.Gather) : request.Length;
        if (length > _core.EffectiveMaxMessageSize(channel))
        {
            counters.TooLarge++;
            return SendStatus.TooLarge;
        }

        LatestSendKeys keys = _sendKeys[local];
        if (!keys.TryGetOrAdd(request.Key, out int keySlot))
        {
            // PROTOCOL.md §7: a full ReliableLatest key table rejects, it never evicts.
            counters.KeyTableFull++;
            return SendStatus.KeyTableFull;
        }

        ref LatestSendState send = ref _send[local];
        if (channel.QueueLimitBytes > 0)
        {
            long pending = send.PendingBytes + send.InFlightBytes;
            if (pending > 0 && pending + length > channel.QueueLimitBytes)
            {
                counters.QueueFull++;
                return SendStatus.QueueFull;
            }
        }

        // One entry is kept per channel for the transmissions that drain the queues, so a full table cannot deadlock.
        if (_core.Entries.Available <= _channels.Length || !_core.TryAllocateEntry(channel.Id, SendEntryFlags.EngineCompletes, out int value))
        {
            counters.QueueFull++;
            return SendStatus.QueueFull;
        }

        if (!TryTakeValue(_core, ref request, channel, length, out BufferLease lease, out byte* payload, out int wireLength, out int rawLength))
        {
            _core.DiscardEntry(value);
            return SendStatus.OutOfBuffers;
        }

        if (request.Options.Track && !_core.TryTrack(value, request.Options.Context, out request.Token))
        {
            _core.ReturnSend(in lease);
            _core.DiscardEntry(value);
            counters.QueueFull++;
            return SendStatus.QueueFull;
        }

        // Commit: nothing below fails.
        uint version = send.NextVersion;
        send.NextVersion = version + 1 == 0 ? 1u : version + 1;
        MessageHeader header = default;
        header.Channel = channel.Id;
        header.Sequence = version;
        header.Key = request.Key;
        header.FragCount = 1;
        header.RawLength = rawLength;
        SendEntryTable entries = _core.Entries;
        entries.SetHeaderLength(value, DatagramFraming.WriteHeader(entries.GetHeaderBlock(value), channel, in header));
        entries.Leases[value] = lease;
        _core.SetPayload(value, payload, wireLength);
        entries.Keys[value] = request.Key;
        entries.Sequences[value] = version;
        entries.BatchCount[value] = rawLength;
        long now = _core.CurrentPassMicros;
        entries.Deadlines[value] = now + VersionBudgetMicros;
        _core.StampAdmission(value);
        ref SendEntry entry = ref entries[value];
        entry.Aux0 = length;
        entry.Aux1 = ValueMarker | (uint)keySlot;
        if (request.Options.Mode == SendMode.Immediate)
        {
            entry.Flags |= SendEntryFlags.Immediate;
        }

        Supersede(local, keySlot, ref counters);
        ref KeySendSlot key = ref keys[keySlot];
        key.Key = request.Key;
        key.CurrentVersion = version;
        key.InFlightEntry = value;
        key.Attempts = 0;
        key.LastSentMicros = 0;
        key.RetryDeadline = 0;
        key.Flags = KeySendFlags.Pending;
        keys.Arm(keySlot);
        send.LiveKeys++;
        EnqueueFresh(ref send, value, length);
        return SendStatus.Admitted;
    }

    /// <summary>
    /// Takes the value's bytes into memory this engine owns for as long as the version may be retransmitted: a copy (or its
    /// LZ4 block) in a send lease, or the caller's owned lease. Pinned, borrowed and gathered payloads are copied, because
    /// caller memory is only promised until the BufferReleased completion while a version lives for up to 30 s.
    /// </summary>
    private static bool TryTakeValue(PeerCore core, ref SendRequest request, ChannelDefinition channel, int length,
        out BufferLease lease, out byte* payload, out int wireLength, out int rawLength)
    {
        SendPayloadKind kind = request.Kind;
        SendRequest copy = request;
        if (kind == SendPayloadKind.Pinned)
        {
            copy.Kind = SendPayloadKind.Copy;
            copy.Source = new ReadOnlySpan<byte>(request.Pointer, length);
        }
        else if (kind == SendPayloadKind.Borrowed)
        {
            copy.Kind = SendPayloadKind.Copy;
            copy.Source = request.Borrowed.Span;
        }

        PreparedPayload prepared = default;
        if (!EnginePayload.TryPrepare(core, ref copy, channel, length, takeSinglePage: false, ref prepared))
        {
            lease = BufferLease.Empty;
            payload = null;
            wireLength = 0;
            rawLength = 0;
            return false;
        }

        // Either the engine rented the lease (a copy, a compressed block or a gather) or the caller handed one over.
        lease = prepared.Lease;
        payload = prepared.Pointer;
        wireLength = prepared.Length;
        rawLength = prepared.RawLength;
        if (kind == SendPayloadKind.Owned && !prepared.Rented)
        {
            lease = request.Lease;
        }
        else if (kind == SendPayloadKind.Owned)
        {
            // The caller's lease was compressed into a new one and goes back to the pool now.
            core.ReturnSend(in request.Lease);
        }
        else if (kind == SendPayloadKind.Gather)
        {
            foreach (BufferLease page in request.Gather)
            {
                core.ReturnSend(in page);
            }
        }

        return true;
    }

    /// <summary>Replaces the key's live value (PROTOCOL.md §4.3 <see cref="DeliveryStatus.Superseded"/>).</summary>
    private void Supersede(int local, int keySlot, ref ChannelSendCounters counters)
    {
        LatestSendKeys keys = _sendKeys[local];
        ref KeySendSlot key = ref keys[keySlot];
        int previous = key.InFlightEntry;
        if (previous < 0)
        {
            return;
        }

        key.InFlightEntry = -1;
        keys.Disarm(keySlot);
        _send[local].LiveKeys--;
        AbortLargeStream(ref key);
        counters.Superseded++;
        MarkValueFinished(local, previous, DeliveryStatus.Superseded, superseded: true);
    }

    /// <summary>
    /// Records the terminal status of a value and takes it out of every queue. A value with no transmission outstanding is
    /// finished now (through a local completion, so no continuation runs inside a pass); one still in flight keeps its
    /// payload lease until its last transmission completes, because the transport may still be reading those bytes
    /// (ADR 0008 invariant 1).
    /// </summary>
    private void MarkValueFinished(int local, int value, DeliveryStatus status, bool superseded)
    {
        SendEntryTable entries = _core.Entries;
        ref LatestSendState send = ref _send[local];
        ValueFlags flags = FlagsOf(entries[value].Aux1);
        if ((flags & ValueFlags.Queued) != 0)
        {
            Unlink(ref send, value, retries: false);
        }
        else if ((flags & ValueFlags.Retrying) != 0)
        {
            Unlink(ref send, value, retries: true);
        }

        ref SendEntry entry = ref entries[value];
        long aux = entry.Aux1;
        flags = (FlagsOf(aux) | ValueFlags.Finish) & ~(ValueFlags.Queued | ValueFlags.Retrying);
        if (superseded)
        {
            flags |= ValueFlags.Superseded;
        }

        entry.Aux1 = WithStatus(WithFlags(aux, flags), status);
        if (OutstandingOf(aux) == 0)
        {
            FinishValue(local, value, status);
        }
    }

    /// <summary>Completes a value: no queue holds it and no transmission references its payload any more.</summary>
    private void FinishValue(int local, int value, DeliveryStatus status)
    {
        ref SendEntry entry = ref _core.Entries[value];
        if ((FlagsOf(entry.Aux1) & ValueFlags.Transmitted) != 0)
        {
            ref LatestSendState send = ref _send[local];
            send.InFlightCount--;
            send.InFlightBytes -= entry.Aux0;
            entry.Aux1 = WithFlags(entry.Aux1, FlagsOf(entry.Aux1) & ~ValueFlags.Transmitted);
        }

        _core.QueueLocalCompletion(value, status);
    }

    /// <summary>Appends a freshly admitted value to the channel's queue (the links live in <see cref="SendEntryTable.Next"/>).</summary>
    private void EnqueueFresh(ref LatestSendState send, int value, int length)
    {
        SendEntryTable entries = _core.Entries;
        ref SendEntry entry = ref entries[value];
        entry.Aux1 = WithFlags(entry.Aux1, FlagsOf(entry.Aux1) | ValueFlags.Queued);
        entries.Next[value] = -1;
        if (send.PendingTail < 0)
        {
            send.PendingHead = value;
        }
        else
        {
            entries.Next[send.PendingTail] = value;
        }

        send.PendingTail = value;
        send.PendingCount++;
        send.PendingBytes += length;
    }

    /// <summary>Takes the head of one queue off it, clearing the flag and the queue accounting.</summary>
    private void Dequeue(ref LatestSendState send, bool retries, int value)
    {
        SendEntryTable entries = _core.Entries;
        int next = entries.Next[value];
        ref SendEntry entry = ref entries[value];
        ValueFlags flags = FlagsOf(entry.Aux1);
        if (retries)
        {
            send.RetryHead = next;
            if (next < 0)
            {
                send.RetryTail = -1;
            }

            entry.Aux1 = WithFlags(entry.Aux1, flags & ~ValueFlags.Retrying);
            return;
        }

        send.PendingHead = next;
        if (next < 0)
        {
            send.PendingTail = -1;
        }

        if ((flags & ValueFlags.Queued) != 0)
        {
            send.PendingCount--;
            send.PendingBytes -= entry.Aux0;
        }

        entry.Aux1 = WithFlags(entry.Aux1, flags & ~ValueFlags.Queued);
    }

    /// <summary>
    /// Removes a value from the middle of one queue (a supersede or a budget failure). The queues hold the values admitted
    /// since the last pass, so the walk is short; a finished value must never stay linked, because its slot is freed as soon
    /// as its local completion is routed.
    /// </summary>
    private void Unlink(ref LatestSendState send, int value, bool retries)
    {
        SendEntryTable entries = _core.Entries;
        NativeArray<int> links = entries.Next;
        int head = retries ? send.RetryHead : send.PendingHead;
        if (head == value)
        {
            Dequeue(ref send, retries, value);
            return;
        }

        int previous = head;
        while (previous >= 0 && links[previous] != value)
        {
            previous = links[previous];
        }

        if (previous < 0)
        {
            return;
        }

        int next = links[value];
        links[previous] = next;
        if (retries)
        {
            if (send.RetryTail == value)
            {
                send.RetryTail = previous;
            }

            return;
        }

        if (send.PendingTail == value)
        {
            send.PendingTail = previous;
        }

        ref SendEntry entry = ref entries[value];
        if ((FlagsOf(entry.Aux1) & ValueFlags.Queued) != 0)
        {
            send.PendingCount--;
            send.PendingBytes -= entry.Aux0;
        }
    }

    // ------------------------------------------------------------------ the scheduler's passes (game thread)

    /// <inheritdoc/>
    public override void FlushChannel(int channelIndex, ref FlushContext flush)
    {
        DrainNotices();
        int local = _localOf[channelIndex];
        ref LatestSendState send = ref _send[local];
        if (send.PendingHead < 0)
        {
            return;
        }

        DrainQueue(local, ref send, ref flush, retries: false);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// PROTOCOL.md §4.5: retries are scheduled after fresh real-time traffic, so they go out here — bounded by the per-peer
    /// retry byte budget — followed by the coalesced acks and rejects this end owes (§2.3).
    /// </remarks>
    public override void Flush(ref FlushContext flush)
    {
        DrainNotices();
        RefillRetryBudget(flush.NowMicros);
        for (int local = 0; local < _channels.Length; local++)
        {
            ref LatestSendState send = ref _send[local];
            if (send.RetryHead >= 0)
            {
                DrainQueue(local, ref send, ref flush, retries: true);
            }
        }

        SendPendingAcks(ref flush);
        LowerDeadline(ref flush.NextDeadline);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// The timer backstop of PROTOCOL.md §4.4 and the per-version budget: every key with a live value carries its retry
    /// deadline, and a channel keeps the earliest of them, so a pass that is not due costs one comparison per channel.
    /// </remarks>
    public override void Tick(long nowMicros, ref long nextDeadline)
    {
        DrainNotices();
        for (int local = 0; local < _channels.Length; local++)
        {
            ref LatestSendState send = ref _send[local];
            if (nowMicros < send.NextRetryMicros)
            {
                continue;
            }

            LatestSendKeys keys = _sendKeys[local];
            long earliest = long.MaxValue;
            int keySlot = keys.ArmedHead;
            while (keySlot >= 0)
            {
                int next = keys.ArmedNext(keySlot);
                ref KeySendSlot key = ref keys[keySlot];
                int value = key.InFlightEntry;
                if (value >= 0 && (key.Flags & KeySendFlags.RetryArmed) != 0 && nowMicros >= key.RetryDeadline)
                {
                    if (!TryFailExpiredVersion(local, keys, keySlot, ref key, value, nowMicros))
                    {
                        // The value goes back into the retry queue and its timer is re-armed rather than cleared: a retry the
                        // per-peer byte budget holds back must stay visible here, or its 30-second version budget would never
                        // be checked again (PROTOCOL.md §4.4).
                        EnqueueRetry(local, ref send, value);
                        ArmRetry(local, ref send, ref key, nowMicros);
                        if (key.RetryDeadline < earliest)
                        {
                            earliest = key.RetryDeadline;
                        }
                    }
                }
                else if (value >= 0 && (key.Flags & KeySendFlags.RetryArmed) != 0 && key.RetryDeadline < earliest)
                {
                    earliest = key.RetryDeadline;
                }

                keySlot = next;
            }

            send.NextRetryMicros = earliest;
        }

        LowerDeadline(ref nextDeadline);
    }

    /// <summary>Completes a version that used up its 30-second budget (PROTOCOL.md §4.4).</summary>
    private bool TryFailExpiredVersion(int local, LatestSendKeys keys, int keySlot, ref KeySendSlot key, int value, long nowMicros)
    {
        if (nowMicros <= _core.Entries.Deadlines[value])
        {
            return false;
        }

        key.InFlightEntry = -1;
        key.Flags &= ~(KeySendFlags.RetryArmed | KeySendFlags.Pending);
        keys.Disarm(keySlot);
        _send[local].LiveKeys--;
        AbortLargeStream(ref key);
        MarkValueFinished(local, value, DeliveryStatus.Failed, superseded: false);
        return true;
    }

    private void LowerDeadline(ref long nextDeadline)
    {
        for (int local = 0; local < _channels.Length; local++)
        {
            long retry = _send[local].NextRetryMicros;
            if (retry < nextDeadline)
            {
                nextDeadline = retry;
            }
        }

        if (HasPendingAcks() && _nextAckMicros < nextDeadline)
        {
            nextDeadline = _nextAckMicros;
        }
    }

    /// <summary>
    /// Hands the channel's fresh values (or its due retries) to the transport in admission order, stopping when the pass's
    /// budget, the retry budget, the datagram limit or the peer's stream credit runs out.
    /// </summary>
    private void DrainQueue(int local, ref LatestSendState send, ref FlushContext flush, bool retries)
    {
        SendEntryTable entries = _core.Entries;
        ref ChannelSendCounters counters = ref _core.SendCounters(_denseOf[local]);
        while (true)
        {
            int value = retries ? send.RetryHead : send.PendingHead;
            if (value < 0)
            {
                return;
            }

            // A finished value is unlinked where it is finished (MarkValueFinished), so a queue only holds live values.
            ref SendEntry entry = ref entries[value];
            if (flush.BudgetBytes <= 0)
            {
                flush.BudgetExhausted = true;
                return;
            }

            long bytes = entry.HeaderLength + entry.Payload.Length;
            if (retries && _retryRate > 0 && _retryBucket.Available(flush.NowMicros) <= 0)
            {
                // The aggregate per-peer retry budget is used up; the armed timer tries again later (PROTOCOL.md §4.4).
                return;
            }

            if (!TryTransmit(local, value, ref send, ref counters, ref flush, retries))
            {
                return;
            }

            if (retries && _retryRate > 0)
            {
                _retryBucket.Consume(bytes);
            }

            if ((retries ? send.RetryHead : send.PendingHead) == value)
            {
                // Still the head unless the transmission itself finished the value (its budget ran out).
                Dequeue(ref send, retries, value);
            }
        }
    }

    /// <summary>
    /// Sends one transmission of a value: a datagram through the packer, or a one-message group stream when the value does
    /// not fit the current datagram limit (PROTOCOL.md §3.2). Returns <see langword="false"/> when the pass must stop for
    /// this channel (no entry, no datagrams, no stream credit, budget exhausted).
    /// </summary>
    private bool TryTransmit(int local, int value, ref LatestSendState send, ref ChannelSendCounters counters, ref FlushContext flush, bool retry)
    {
        SendEntryTable entries = _core.Entries;
        LatestSendKeys keys = _sendKeys[local];
        int keySlot = KeySlotOf(entries[value].Aux1);
        ref KeySendSlot key = ref keys[keySlot];
        if (key.Attempts >= MaxTransmissions)
        {
            key.InFlightEntry = -1;
            key.Flags &= ~(KeySendFlags.RetryArmed | KeySendFlags.Pending);
            keys.Disarm(keySlot);
            send.LiveKeys--;
            AbortLargeStream(ref key);
            MarkValueFinished(local, value, DeliveryStatus.Failed, superseded: false);
            return true;
        }

        ChannelDefinition channel = _channels[local];
        long size = entries[value].HeaderLength + entries[value].Payload.Length;
        // The pass's own snapshot of the datagram limit — the one the packer was started with (§7.1) — decides datagram or
        // group stream, so the packer can never answer TooLarge for a value this pass already measured.
        bool large = !flush.DatagramsEnabled || size > flush.MaxDatagramPayload;
        if (!_core.TryAllocateEntry(channel.Id, SendEntryFlags.None, out int transmission))
        {
            return false;
        }

        bool sent = large
            ? TrySendLarge(local, value, transmission, keySlot, ref key, ref flush)
            : TrySendDatagram(local, value, transmission, ref flush);
        if (!sent)
        {
            return false;
        }

        long now = flush.NowMicros;
        ref SendEntry valueEntry = ref entries[value];
        long aux = valueEntry.Aux1;
        ValueFlags valueFlags = FlagsOf(aux);
        if ((valueFlags & ValueFlags.Transmitted) == 0)
        {
            aux = WithFlags(aux, valueFlags | ValueFlags.Transmitted);
            send.InFlightCount++;
            send.InFlightBytes += valueEntry.Aux0;
        }

        valueEntry.Aux1 = WithOutstanding(aux, OutstandingOf(aux) + 1);
        key.Attempts++;
        key.LastSentMicros = now;
        ArmRetry(local, ref send, ref key, now);
        counters.Sent++;
        counters.Bytes += valueEntry.Payload.Length;
        if (retry)
        {
            counters.Retries++;
        }

        return true;
    }

    private bool TrySendDatagram(int local, int value, int transmission, ref FlushContext flush)
    {
        SendEntryTable entries = _core.Entries;
        ref SendEntry valueEntry = ref entries[value];
        int headerLength = valueEntry.HeaderLength;
        entries.GetHeaderBlock(value).Slice(0, headerLength).CopyTo(entries.GetHeaderBlock(transmission));
        entries.SetHeaderLength(transmission, headerLength);
        _core.SetPayload(transmission, valueEntry.Payload.Buffer, (int)valueEntry.Payload.Length);
        ref SendEntry entry = ref entries[transmission];
        entry.Aux0 = value;
        entry.Aux1 = entries.Sequences[value];
        ChannelDefinition channel = _channels[local];
        // Never CancelOnBlocked: a ReliableLatest datagram is retransmitted, not dropped (PROTOCOL.md §4.4, §4.5).
        DatagramHints hints = channel.Priority >= DatagramPacker.PriorityThreshold || (valueEntry.Flags & SendEntryFlags.Immediate) != 0
            ? DatagramHints.Priority
            : DatagramHints.None;
        PackResult result = _core.Packer.Add(transmission, hints, ref flush);
        if (result == PackResult.Accepted)
        {
            return true;
        }

        // Blocked by the send cap, or datagrams are not available in this pass: the value keeps its place in the queue.
        _core.DiscardEntry(transmission);
        return false;
    }

    /// <summary>
    /// Sends a large value as one group stream (PROTOCOL.md §3.2, §8 item 8): preamble <c>ChannelId, GroupId = version</c>,
    /// one frame <c>Length, Sequence = version, Key, RawLength</c>, FIN. The header and the value's payload are the entry's
    /// own adjacent segment pair, so nothing is copied.
    /// </summary>
    private bool TrySendLarge(int local, int value, int transmission, int keySlot, ref KeySendSlot key, ref FlushContext flush)
    {
        ChannelDefinition channel = _channels[local];
        SendEntryTable entries = _core.Entries;
        ref LatestSendState send = ref _send[local];
        int credit = _core.StreamCreditGeneration;
        if (_streamBlocked[local] && credit == send.CreditGeneration)
        {
            _core.DiscardEntry(transmission);
            return false;
        }

        uint version = entries.Sequences[value];
        uint serial = (send.StreamSerial + 1) & PeerCore.EngineStreamSerialMask;
        ulong context = PeerCore.MakeEngineStreamContext(ChannelMode.ReliableLatest, _denseOf[local], serial);
        if (_core.OpenStream(StreamKind.Unidirectional, context, (ushort)(channel.Priority * 257), out TransportStreamId stream) != TransportStatus.Success)
        {
            _core.DiscardEntry(transmission);
            _streamBlocked[local] = true;
            send.CreditGeneration = credit;
            return false;
        }

        send.StreamSerial = serial;
        StreamMessageHeader header = default;
        header.Length = (int)entries[value].Payload.Length;
        header.Sequence = version;
        header.Key = entries.Keys[value];
        header.RawLength = entries.BatchCount[value];
        Span<byte> block = entries.GetHeaderBlock(transmission);
        int written = StreamFraming.WriteGroupPreamble(block, channel.Id, version);
        written += StreamFraming.WriteFrameHeader(block.Slice(written), channel, in header);
        entries.SetHeaderLength(transmission, written);
        _core.SetPayload(transmission, entries[value].Payload.Buffer, header.Length);
        ref SendEntry entry = ref entries[transmission];
        entry.Aux0 = value;
        entry.Aux1 = version;
        // A newer version aborts the older stream of the same key, so at most one is open per key.
        AbortLargeStream(ref key);
        key.LargeValueStream = stream;
        key.Flags |= KeySendFlags.LargeValue;
        _core.Packer.SubmitPending(ref flush);
        TransportStatus status = _core.SubmitStream(stream, entries.GetSegments(transmission), header.Length == 0 ? 1 : 2, transmission,
            TransportSendFlags.Start | TransportSendFlags.Fin);
        if (status != TransportStatus.Success)
        {
            // A refused start never reached the peer: release the stream and wait for credit (PROTOCOL.md §3.2).
            _core.DiscardEntry(transmission);
            _core.Transport?.CloseStream(stream);
            key.LargeValueStream = default;
            key.Flags &= ~KeySendFlags.LargeValue;
            if (status == TransportStatus.StreamLimitReached)
            {
                _streamBlocked[local] = true;
                send.CreditGeneration = credit;
            }

            return false;
        }

        _streamBlocked[local] = false;
        long bytes = written + header.Length;
        flush.BudgetBytes -= bytes;
        flush.BytesSubmitted += bytes;
        PeerCounters counters = _core.Counters;
        counters.StreamSends++;
        counters.StreamBytesSent += bytes;
        return true;
    }

    private void AbortLargeStream(ref KeySendSlot key)
    {
        if ((key.Flags & KeySendFlags.LargeValue) == 0)
        {
            return;
        }

        TransportStreamId stream = key.LargeValueStream;
        key.LargeValueStream = default;
        key.Flags &= ~KeySendFlags.LargeValue;
        if (stream.IsValid)
        {
            _core.Transport?.AbortStream(stream, (ulong)QuiclyErrorCode.NoError, StreamAbortDirection.Send);
        }
    }

    /// <summary>Arms the timer backstop of PROTOCOL.md §4.4: <c>clamp(1.5 × RTT, MinRetry, MaxRetry)</c>, doubled per attempt.</summary>
    private void ArmRetry(int local, ref LatestSendState send, ref KeySendSlot key, long now)
    {
        long rtt = _core.ApplicationRttMicros;
        long interval = rtt > 0 ? rtt + (rtt >> 1) : MinRetryMicros;
        interval = Math.Clamp(interval, MinRetryMicros, MaxRetryMicros);
        int doublings = Math.Min((int)key.Attempts, 8);
        for (int i = 0; i < doublings && interval < MaxRetryMicros; i++)
        {
            interval = Math.Min(interval * 2, MaxRetryMicros);
        }

        key.RetryDeadline = now + interval;
        key.Flags |= KeySendFlags.RetryArmed;
        if (key.RetryDeadline < send.NextRetryMicros)
        {
            send.NextRetryMicros = key.RetryDeadline;
        }
    }

    private void EnqueueRetry(int local, ref LatestSendState send, int value)
    {
        SendEntryTable entries = _core.Entries;
        ref SendEntry entry = ref entries[value];
        ValueFlags flags = FlagsOf(entry.Aux1);
        if ((flags & (ValueFlags.Queued | ValueFlags.Retrying | ValueFlags.Finish)) != 0)
        {
            return;
        }

        entry.Aux1 = WithFlags(entry.Aux1, flags | ValueFlags.Retrying);
        entries.Next[value] = -1;
        if (send.RetryTail < 0)
        {
            send.RetryHead = value;
        }
        else
        {
            entries.Next[send.RetryTail] = value;
        }

        send.RetryTail = value;
    }

    private void RefillRetryBudget(long now)
    {
        long rate = _retryCap;
        if (rate == 0)
        {
            rate = EstimateRetryRate();
        }

        if (rate != _retryRate)
        {
            _retryRate = rate;
            if (rate > 0)
            {
                _retryBucket.Initialize(rate, Math.Max(1, rate / 10), now);
            }
        }
    }

    /// <summary>The share of the estimated bandwidth retries may use (PROTOCOL.md §4.4), floored so a key always progresses.</summary>
    private long EstimateRetryRate()
    {
        long estimate = 0;
        ITransport? transport = _core.Transport;
        if (transport is not null && !_core.IsTransportClosed)
        {
            transport.GetStatistics(out TransportStatistics statistics);
            if (statistics.CongestionWindowBytes > 0 && statistics.RttMicros > 0)
            {
                estimate = (long)statistics.CongestionWindowBytes * 1_000_000 / statistics.RttMicros;
            }
        }

        if (estimate <= 0)
        {
            return 0; // no estimate and no configured cap: retries are bounded by the per-version budget alone
        }

        return Math.Max(MinRetryBytesPerSecond, (long)(estimate * _retryShare));
    }

    // ------------------------------------------------------------------ completions (game thread)

    /// <inheritdoc/>
    public override void OnSendCompleted(int entrySlot, in CompletionEntry completion)
    {
        SendEntryTable entries = _core.Entries;
        long aux = entries[entrySlot].Aux1;
        if (IsValueEntry(aux))
        {
            if (completion.Final)
            {
                // A value is never submitted: only the local completion this engine queued for it arrives here.
                _core.CompleteEntry(entrySlot, _core.MapCompletion(in completion));
            }

            return;
        }

        if (!completion.Final)
        {
            // DatagramSendState.Sent: the transmission's bytes left the host. The value keeps its payload for retries, so
            // there is nothing to release; the BufferReleased stage of the value's token completes with its first Sent.
            int owner = (int)entries[entrySlot].Aux0;
            _core.CompleteStage(owner, CompletionStage.BufferReleased, DeliveryStatus.Pending);
            return;
        }

        OnTransmissionCompleted(entrySlot, in completion);
    }

    private void OnTransmissionCompleted(int transmission, in CompletionEntry completion)
    {
        SendEntryTable entries = _core.Entries;
        int value = (int)entries[transmission].Aux0;
        uint version = (uint)entries[transmission].Aux1;
        int dense = _core.ChannelIndexOf(entries[transmission].Channel);
        // The transmission owns no payload lease (the value does), so this only frees its slot.
        _core.CompleteEntry(transmission, DeliveryStatus.Delivered);
        int local = dense >= 0 ? _localOf[dense] : -1;
        if (local < 0 || entries.GetState(value) == SendEntryState.Free)
        {
            return;
        }

        ref SendEntry valueEntry = ref entries[value];
        long aux = valueEntry.Aux1;
        if (!IsValueEntry(aux) || entries.Sequences[value] != version)
        {
            return; // the value's slot was reused: nothing of this transmission is left
        }

        int outstanding = OutstandingOf(aux) - 1;
        aux = WithOutstanding(aux, outstanding);
        valueEntry.Aux1 = aux;
        ValueFlags flags = FlagsOf(aux);
        if (outstanding == 0 && (flags & ValueFlags.Finish) != 0)
        {
            FinishValue(local, value, StatusOf(aux));
            return;
        }

        DeliveryStatus status = _core.MapCompletion(in completion);
        if (status is DeliveryStatus.Delivered or DeliveryStatus.Sent)
        {
            // PROTOCOL.md §4.3: an acknowledgement (or Sent on a carrier without datagram state) only means the datagram
            // left this host. The LatestAck decides Delivered; until then the armed timer is the backstop.
            return;
        }

        // Lost, canceled, expired or refused: retransmit at once, within the budgets (PROTOCOL.md §4.4).
        int keySlot = KeySlotOf(aux);
        LatestSendKeys keys = _sendKeys[local];
        if (keys[keySlot].InFlightEntry != value)
        {
            return;
        }

        ref LatestSendState send = ref _send[local];
        EnqueueRetry(local, ref send, value);

        // The timer stays armed while the retry waits for its turn, so the version budget keeps being checked.
        ArmRetry(local, ref send, ref keys[keySlot], _core.CurrentPassMicros);
    }

    /// <inheritdoc/>
    /// <remarks>Only a value that has not been transmitted yet can be canceled; it completes at the next Poll or Flush.</remarks>
    public override bool TryCancel(int entrySlot)
    {
        SendEntryTable entries = _core.Entries;
        if (entries.GetState(entrySlot) != SendEntryState.Filling)
        {
            return false;
        }

        long aux = entries[entrySlot].Aux1;
        if (!IsValueEntry(aux) || OutstandingOf(aux) != 0 || (FlagsOf(aux) & ValueFlags.Finish) != 0)
        {
            return false;
        }

        int dense = _core.ChannelIndexOf(entries[entrySlot].Channel);
        int local = dense >= 0 ? _localOf[dense] : -1;
        if (local < 0)
        {
            return false;
        }

        int keySlot = KeySlotOf(aux);
        LatestSendKeys keys = _sendKeys[local];
        if (keys[keySlot].InFlightEntry == entrySlot)
        {
            keys[keySlot].InFlightEntry = -1;
            keys[keySlot].Flags &= ~(KeySendFlags.RetryArmed | KeySendFlags.Pending);
            keys.Disarm(keySlot);
            _send[local].LiveKeys--;
        }

        MarkValueFinished(local, entrySlot, DeliveryStatus.Canceled, superseded: false);
        return true;
    }

    /// <inheritdoc/>
    public override void OnPeerClosed()
    {
        for (int local = 0; local < _channels.Length; local++)
        {
            LatestSendKeys keys = _sendKeys[local];
            ref LatestSendState send = ref _send[local];
            send.PendingHead = -1;
            send.PendingTail = -1;
            send.RetryHead = -1;
            send.RetryTail = -1;
            send.PendingCount = 0;
            send.PendingBytes = 0;
            send.NextRetryMicros = long.MaxValue;
            int keySlot = keys.ArmedHead;
            while (keySlot >= 0)
            {
                int next = keys.ArmedNext(keySlot);
                ref KeySendSlot key = ref keys[keySlot];
                int value = key.InFlightEntry;
                key.InFlightEntry = -1;
                key.Flags = KeySendFlags.None;
                key.LargeValueStream = default;
                keys.Disarm(keySlot);
                send.LiveKeys--;
                if (value >= 0)
                {
                    // The transport is gone, so nothing reads the payload any more.
                    _core.Entries[value].Aux1 = WithOutstanding(_core.Entries[value].Aux1, 0);
                    _core.CompleteEntry(value, DeliveryStatus.Disconnected);
                }

                keySlot = next;
            }
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// PROTOCOL.md §4.1: a resumed session re-queues every live key at its current value — a free full-state resync. The
    /// version counter restarts with the epoch (§1), so each live key's value is re-queued under a fresh version, and the
    /// receive side forgets its keys before the next value arrives.
    /// </remarks>
    public override void OnEpochReset(bool resumed)
    {
        if (_epochs++ == 0)
        {
            return;
        }

        for (int local = 0; local < _channels.Length; local++)
        {
            LatestSendKeys keys = _sendKeys[local];
            ref LatestSendState send = ref _send[local];
            send.NextVersion = 1;
            send.NextRetryMicros = long.MaxValue;
            int keySlot = keys.ArmedHead;
            while (keySlot >= 0)
            {
                int next = keys.ArmedNext(keySlot);
                ref KeySendSlot key = ref keys[keySlot];
                int value = key.InFlightEntry;
                if (value >= 0)
                {
                    uint version = send.NextVersion;
                    send.NextVersion = version + 1 == 0 ? 1u : version + 1;
                    Requeue(local, value, ref key, version);
                    ref SendEntry entry = ref _core.Entries[value];
                    if ((FlagsOf(entry.Aux1) & (ValueFlags.Queued | ValueFlags.Retrying)) == 0)
                    {
                        EnqueueRetry(local, ref send, value);
                    }
                }

                keySlot = next;
            }
        }

        ResetReceiveState();
    }

    /// <summary>Re-stamps a live value with a new version of the new epoch and re-arms its budget.</summary>
    private void Requeue(int local, int value, ref KeySendSlot key, uint version)
    {
        SendEntryTable entries = _core.Entries;
        ChannelDefinition channel = _channels[local];
        MessageHeader header = default;
        header.Channel = channel.Id;
        header.Sequence = version;
        header.Key = entries.Keys[value];
        header.FragCount = 1;
        header.RawLength = entries.BatchCount[value];
        entries.SetHeaderLength(value, DatagramFraming.WriteHeader(entries.GetHeaderBlock(value), channel, in header));
        entries.Sequences[value] = version;
        entries.Deadlines[value] = _core.CurrentPassMicros + VersionBudgetMicros;
        key.CurrentVersion = version;
        key.AckedVersion = 0;
        key.Attempts = 0;
        key.LastSentMicros = 0;
        key.RetryDeadline = 0;
        key.Flags = (key.Flags & ~(KeySendFlags.RetryArmed | KeySendFlags.LargeValue)) | KeySendFlags.Pending;
        key.LargeValueStream = default;
    }

    /// <inheritdoc/>
    /// <remarks>
    /// PROTOCOL.md §3.4 type 0x17: the value the application sees last for this key is the one that was acknowledged; the
    /// live value (if any) completes <see cref="DeliveryStatus.Canceled"/>, the retries stop and the slot is freed, so the
    /// same key can be used again in this epoch with a fresh slot.
    /// </remarks>
    public override SendStatus RetireKey(ChannelDefinition channel, ulong key)
    {
        int dense = _core.ChannelIndexOf(channel.Id);
        int local = dense >= 0 ? _localOf[dense] : -1;
        if (local < 0)
        {
            return SendStatus.InvalidChannel;
        }

        LatestSendKeys keys = _sendKeys[local];
        if (keys.TryGet(key, out int keySlot))
        {
            ref KeySendSlot slot = ref keys[keySlot];
            int value = slot.InFlightEntry;
            slot.InFlightEntry = -1;
            slot.Flags = KeySendFlags.None;
            AbortLargeStream(ref slot);
            keys.Disarm(keySlot);
            if (value >= 0)
            {
                _send[local].LiveKeys--;
                MarkValueFinished(local, value, DeliveryStatus.Canceled, superseded: false);
            }

            keys.Remove(key);
        }

        Span<byte> frame = stackalloc byte[32];
        if (!ControlCodec.TryWrite(frame, new KeyRetired(channel.Id, key), out int written) || !_core.SendControlFrame(frame.Slice(0, written), ControlCarrier.Stream))
        {
            return SendStatus.NotConnected;
        }

        return SendStatus.Admitted;
    }

    /// <inheritdoc/>
    public override void Dispose()
    {
        foreach (LatestSendKeys keys in _sendKeys)
        {
            keys?.Dispose();
        }

        _send?.Dispose();
        DisposeReceive();
    }

    private static bool IsValueEntry(long aux1) => (aux1 & ValueMarker) != 0;

    private static int KeySlotOf(long aux1) => (int)(uint)aux1;

    private static int OutstandingOf(long aux1) => (int)((aux1 >> OutstandingShift) & 0xFF);

    private static long WithOutstanding(long aux1, int outstanding) => (aux1 & ~OutstandingMask) | ((long)(outstanding & 0xFF) << OutstandingShift);

    private static ValueFlags FlagsOf(long aux1) => (ValueFlags)((aux1 >> FlagsShift) & 0xFF);

    private static long WithFlags(long aux1, ValueFlags flags) => (aux1 & ~FlagsMask) | ((long)(byte)flags << FlagsShift);

    private static DeliveryStatus StatusOf(long aux1) => (DeliveryStatus)((aux1 >> StatusShift) & 0xF);

    private static long WithStatus(long aux1, DeliveryStatus status) => (aux1 & ~StatusMask) | (((long)status & 0xF) << StatusShift);

    /// <summary>Send-side state of one ReliableLatest channel: one cache line, native memory, game thread only.</summary>
    [StructLayout(LayoutKind.Explicit, Size = 64)]
    private struct LatestSendState
    {
        /// <summary>The version the next value takes (a per-channel 32-bit counter; 0 is skipped on the wrap).</summary>
        [FieldOffset(0)] public uint NextVersion;

        /// <summary>First value waiting for its first transmission (-1 = none); values chain through <see cref="SendEntryTable.Next"/>.</summary>
        [FieldOffset(4)] public int PendingHead;

        /// <summary>Last value waiting for its first transmission.</summary>
        [FieldOffset(8)] public int PendingTail;

        /// <summary>First value waiting for a retransmission (-1 = none).</summary>
        [FieldOffset(12)] public int RetryHead;

        /// <summary>Last value waiting for a retransmission.</summary>
        [FieldOffset(16)] public int RetryTail;

        /// <summary>Values admitted and not yet transmitted.</summary>
        [FieldOffset(20)] public int PendingCount;

        /// <summary>Values transmitted at least once and not yet completed.</summary>
        [FieldOffset(24)] public int InFlightCount;

        /// <summary>Keys with a live (unacknowledged) value.</summary>
        [FieldOffset(28)] public int LiveKeys;

        /// <summary>Admitted payload bytes of <see cref="PendingCount"/>.</summary>
        [FieldOffset(32)] public long PendingBytes;

        /// <summary>Admitted payload bytes of <see cref="InFlightCount"/>.</summary>
        [FieldOffset(40)] public long InFlightBytes;

        /// <summary>Earliest armed retry deadline of the channel's keys (<see cref="long.MaxValue"/> = none).</summary>
        [FieldOffset(48)] public long NextRetryMicros;

        /// <summary>Serial of the last large-value stream opened on the channel (its context; it wraps).</summary>
        [FieldOffset(56)] public uint StreamSerial;

        /// <summary><see cref="PeerCore.StreamCreditGeneration"/> read when a large value last found no stream credit.</summary>
        [FieldOffset(60)] public int CreditGeneration;
    }

    /// <summary>
    /// Per-key send state of one channel (game thread): the channel's key table, a <see cref="KeySendSlot"/> per key and an
    /// intrusive list of the keys with a live value, so the retry sweep visits only those. The slot arrays grow with the
    /// slots in use, up to <see cref="ChannelDefinition.MaxKeys"/>.
    /// </summary>
    private sealed class LatestSendKeys : IDisposable
    {
        private readonly IKeyTable _table;
        private readonly int _maxKeys;
        private NativeArray<KeySendSlot> _slots;
        private NativeArray<int> _next;
        private NativeArray<int> _previous;

        public LatestSendKeys(ChannelDefinition channel)
        {
            _maxKeys = channel.MaxKeys;
            _table = PeerCore.CreateKeyTable(channel);
            int initial = Math.Min(_maxKeys, InitialKeySlots);
            _slots = new NativeArray<KeySendSlot>(initial);
            _next = new NativeArray<int>(initial);
            _previous = new NativeArray<int>(initial);
        }

        /// <summary>First key with a live value, or -1.</summary>
        public int ArmedHead { get; private set; } = -1;

        public ref KeySendSlot this[int slot] => ref _slots[slot];

        /// <summary>The next key of the live list after <paramref name="slot"/>, or -1.</summary>
        /// <param name="slot">A key slot.</param>
        public int ArmedNext(int slot) => _next[slot];

        public bool TryGet(ulong key, out int slot) => _table.TryGetSlot(key, out slot);

        /// <summary>Finds or adds the key's slot; a fresh slot starts with no value.</summary>
        /// <param name="key">The key.</param>
        /// <param name="slot">The slot, or -1 when the table is full or the key is outside a dense key space.</param>
        /// <returns><see langword="false"/> when the key got no slot.</returns>
        public bool TryGetOrAdd(ulong key, out int slot)
        {
            if (_table.TryGetSlot(key, out slot))
            {
                return true;
            }

            if (!_table.TryGetOrAdd(key, out slot) || slot < 0)
            {
                return false;
            }

            EnsureSlot(slot);
            _slots[slot] = default;
            _slots[slot].InFlightEntry = -1;
            _next[slot] = -1;
            _previous[slot] = -1;
            return true;
        }

        public void Remove(ulong key) => _table.Remove(key, out _);

        /// <summary>Puts a key on the live list (it has a value that is not acknowledged yet).</summary>
        /// <param name="slot">The key slot.</param>
        public void Arm(int slot)
        {
            if (_previous[slot] >= 0 || ArmedHead == slot)
            {
                return;
            }

            _previous[slot] = -1;
            _next[slot] = ArmedHead;
            if (ArmedHead >= 0)
            {
                _previous[ArmedHead] = slot;
            }

            ArmedHead = slot;
        }

        /// <summary>Takes a key off the live list.</summary>
        /// <param name="slot">The key slot.</param>
        public void Disarm(int slot)
        {
            if (_previous[slot] < 0 && ArmedHead != slot)
            {
                return;
            }

            int previous = _previous[slot];
            int next = _next[slot];
            if (previous >= 0)
            {
                _next[previous] = next;
            }
            else
            {
                ArmedHead = next;
            }

            if (next >= 0)
            {
                _previous[next] = previous;
            }

            _next[slot] = -1;
            _previous[slot] = -1;
        }

        public void Dispose()
        {
            (_table as IDisposable)?.Dispose();
            _slots.Dispose();
            _next.Dispose();
            _previous.Dispose();
        }

        private void EnsureSlot(int slot)
        {
            if (slot < _slots.Length)
            {
                return;
            }

            int length = (int)Math.Min(_maxKeys, Math.Max(slot + 1L, _slots.Length * 2L));
            _slots = Grow(_slots, length);
            _next = Grow(_next, length);
            _previous = Grow(_previous, length);
        }

        private static NativeArray<T> Grow<T>(NativeArray<T> array, int length)
            where T : unmanaged
        {
            NativeArray<T> grown = new(length);
            array.AsSpan().CopyTo(grown.AsSpan());
            array.Dispose();
            return grown;
        }
    }
}
