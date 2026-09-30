using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Session.Engines;

/// <summary>
/// The machinery the unreliable datagram modes share (<see cref="ChannelMode.UnreliableUnordered"/>,
/// <see cref="ChannelMode.UnreliableSequenced"/>; docs/design/session-layer.md §7.1). One instance per mode per peer owns
/// the structure-of-arrays state of all channels of its mode; the mode engines add only their receive acceptance.
/// </summary>
/// <remarks>
/// <para><b>Send (game thread).</b> <see cref="Admit"/> checks size, key and queue limit, then fills a send entry: the
/// header in the slot's header block, the payload by every send path — a copy in a lease, an owned lease, pinned memory,
/// pinned (array) or copied borrowed memory, gathered pages copied into one lease — LZ4-compressed when the channel asks
/// for it and it shrinks the payload (PROTOCOL.md §2.1: RawLength 0 otherwise) — and appends the entry to the channel's
/// FIFO (the entry's <c>Next</c> link). Nothing reaches the transport there: the scheduler's <see cref="FlushChannel"/>
/// hands the queue to the packer in admission order and drops entries whose expiry has passed (counted
/// <c>Expired</c>, completing <see cref="DeliveryStatus.Expired"/>). Sequence numbers come from a per-channel counter of the
/// channel's width, assigned at admission.</para>
/// <para><b>Completions (game thread).</b> The early Sent notice returns the payload and completes the BufferReleased
/// stage; the final completion maps to Delivered (acknowledged), Sent (handed to a carrier that reports no per-datagram
/// states, so delivery can never be known — PROTOCOL.md §4.3), Lost,
/// Expired (canceled by the transport, or expired here), Canceled (<see cref="TryCancel"/>), Failed or Disconnected, and
/// frees the slot. Entry scratch: <c>Aux0</c> holds the admitted length (queue accounting), <c>Aux1</c> where the entry is
/// (queued, handed to the packer, finished).</para>
/// <para><b>Receive (transport thread).</b> <see cref="OnDatagram"/> asks the mode for acceptance (<see cref="Accept"/>),
/// copies the payload into a receive lease within the peer's receive budget and publishes a <see cref="ReceiveEntry"/> to
/// the receive ring — or, on a <see cref="ChannelDefinition.CoalesceOnReceive"/> channel, to the key's mailbox (latest
/// wins, no ring entry). A full ring drops the newest message; Poll decodes compressed payloads, dispatches and
/// releases.</para>
/// <para><b>Fragmentation</b> (<see cref="ChannelDefinition.Fragmentation"/>, PROTOCOL.md §2.1; the
/// <c>DatagramEngine.Fragmentation.cs</c> half of this class): a message that does not fit one datagram goes out as at most
/// 8 fragments of one owner entry, and the receive side reassembles each <c>(channel, key, sequence)</c> in a partial of
/// its own, at most <see cref="ChannelDefinition.MaxReassemblies"/> per channel.</para>
/// </remarks>
internal abstract unsafe partial class DatagramEngine : ChannelEngine
{
    private const long InQueue = 0;
    private const long HandedToPacker = 1;
    private const long Finished = 2;

    private PeerCore _core = null!;
    private ChannelDefinition[] _channels = [];
    private int[] _localOf = [];
    private long[] _expiryMicros = [];
    private DatagramHints[] _hints = [];
    private NativeArray<ChannelSendState> _send = null!;
    private NativeArray<ChannelRecvState> _recv = null!;
    private ReceiveKeyTracker?[] _keys = [];
    private ReceiveMailbox?[] _mailboxes = [];
    private int _epochs;
    private int _resetReceive;

    /// <inheritdoc/>
    public override void Initialize(PeerCore core, ReadOnlySpan<ChannelDefinition> channelsOfMode)
    {
        _core = core;
        int count = channelsOfMode.Length;
        _channels = channelsOfMode.ToArray();
        _localOf = new int[core.ChannelCount];
        Array.Fill(_localOf, -1);
        _expiryMicros = new long[count];
        _hints = new DatagramHints[count];
        _keys = new ReceiveKeyTracker?[count];
        _mailboxes = new ReceiveMailbox?[count];
        _send = new NativeArray<ChannelSendState>(Math.Max(count, 1));
        _recv = new NativeArray<ChannelRecvState>(Math.Max(count, 1));
        for (int local = 0; local < count; local++)
        {
            ChannelDefinition channel = _channels[local];
            int dense = core.ChannelIndexOf(channel.Id);
            _localOf[dense] = local;
            ref ChannelSendState send = ref _send[local];
            send.QueueHead = -1;
            send.QueueTail = -1;
            _expiryMicros[local] = channel.ResolveExpiryMicros(core.FlushIntervalMicros);
            // DirectCompletion: OnSendCompleted finishes an unfragmented message with CompleteEntry(MapCompletion) and nothing
            // else unless the completion is local (FlushChannel clears the hint for fragments, which fold into their owner).
            _hints[local] = (channel.Priority >= DatagramPacker.PriorityThreshold ? DatagramHints.Unreliable | DatagramHints.Priority : DatagramHints.Unreliable)
                | DatagramHints.DirectCompletion;
            if (TracksKeys(channel))
            {
                _keys[local] = new ReceiveKeyTracker(channel);
            }

            if (channel.CoalesceOnReceive)
            {
                _mailboxes[local] = core.CreateMailbox(dense, channel.MaxKeys);
            }
        }

        InitializeFragmentation(core, channelsOfMode);
    }

    /// <summary>Whether received messages of <paramref name="channel"/> need per-key state (a <see cref="ReceiveKeyTracker"/>).</summary>
    /// <param name="channel">A channel of this engine.</param>
    protected abstract bool TracksKeys(ChannelDefinition channel);

    /// <summary>
    /// The mode's receive acceptance (transport thread): whether to deliver the message, counting its own drops
    /// (<c>Dropped</c> for stale values, <c>KeyTableFull</c> when the key gets no slot).
    /// </summary>
    /// <param name="local">The channel's index within this engine.</param>
    /// <param name="header">The parsed header.</param>
    /// <param name="keySlot">The key's slot when the channel tracks keys (the mailbox index of a coalescing channel), else -1.</param>
    /// <param name="nowMicros">The receive callback's clock stamp (for a reassembled message, that of the fragment that completed it).</param>
    /// <param name="counters">The channel's receive counters.</param>
    /// <returns><see langword="true"/> to deliver.</returns>
    protected abstract bool Accept(int local, in MessageHeader header, out int keySlot, long nowMicros, ref ChannelRecvCounters counters);

    /// <summary>The peer-level counters (the transport-thread group is this engine's to increment inside a receive callback).</summary>
    protected PeerCounters PeerCounters => _core.Counters;

    /// <summary>The channel at <paramref name="local"/>.</summary>
    /// <param name="local">Index within this engine.</param>
    protected ChannelDefinition ChannelAt(int local) => _channels[local];

    /// <summary>The key tracker of the channel at <paramref name="local"/>, or <see langword="null"/>.</summary>
    /// <param name="local">Index within this engine.</param>
    protected ReceiveKeyTracker? KeysOf(int local) => _keys[local];

    /// <summary>The receive-side state of the channel at <paramref name="local"/> (transport thread).</summary>
    /// <param name="local">Index within this engine.</param>
    protected ref ChannelRecvState ReceiveState(int local) => ref _recv[local];

    /// <summary>Messages queued on a channel and not yet handed to the transport (game thread; tests and statistics).</summary>
    /// <param name="channelIndex">Dense index of a channel of this engine.</param>
    internal int QueuedMessages(int channelIndex) => _send[_localOf[channelIndex]].QueueCount;

    /// <summary>Keys evicted on a channel's receive side (tests and statistics).</summary>
    /// <param name="channelIndex">Dense index of a channel of this engine.</param>
    internal long KeyEvictions(int channelIndex) => _keys[_localOf[channelIndex]]?.Evictions ?? 0;

    /// <inheritdoc/>
    public override long OldestQueuedStamp()
    {
        long oldest = long.MaxValue;
        for (int local = 0; local < _channels.Length; local++)
        {
            int head = _send[local].QueueHead;
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
        ref ChannelSendState send = ref _send[_localOf[channelIndex]];
        statistics.QueuedMessages = send.QueueCount;
        statistics.QueuedBytes = send.QueueBytes;
    }

    // ------------------------------------------------------------------ send (game thread)

    /// <inheritdoc/>
    public override SendStatus Admit(ref SendRequest request)
    {
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

        if (channel.Keyed && channel.KeySpace.IsDense && request.Key > (ulong)channel.KeySpace.MaxKey)
        {
            counters.KeyTableFull++;
            return SendStatus.KeyTableFull;
        }

        ref ChannelSendState send = ref _send[local];
        if (channel.QueueLimitBytes > 0 && send.QueueBytes + length > channel.QueueLimitBytes)
        {
            counters.QueueFull++;
            return SendStatus.QueueFull;
        }

        if (!_core.TryAllocateEntry(channel.Id, SendEntryFlags.None, out int slot))
        {
            counters.QueueFull++;
            return SendStatus.QueueFull;
        }

        NoteOrdinaryEntry(slot);
        PreparedPayload payload = default;
        if (!EnginePayload.TryPrepare(_core, ref request, channel, length, takeSinglePage: false, ref payload))
        {
            _core.DiscardEntry(slot);
            return SendStatus.OutOfBuffers;
        }

        MessageHeader header = default;
        header.Key = channel.Keyed ? request.Key : 0;
        header.FragCount = 1;
        header.RawLength = payload.RawLength;
        int headerLength = DatagramFraming.GetHeaderLength(channel, in header);
        if (_core.DatagramsEnabled && headerLength + payload.Length > _core.MaxDatagramPayload)
        {
            _core.DiscardEntry(slot);
            if (channel.Fragmentation)
            {
                // The payload is prepared once for both shapes (docs/design/session-layer.md §7.8): the fragments point into
                // these very bytes, so the LZ4 pass, the send lease and the pin of a borrowed array are not repeated.
                return AdmitFragmented(ref request, length, ref payload);
            }

            EnginePayload.Release(_core, in payload);
            counters.TooLarge++;
            return SendStatus.TooLarge;
        }

        if (request.Options.Track && !_core.TryTrack(slot, request.Options.Context, out request.Token))
        {
            EnginePayload.Release(_core, in payload);
            _core.DiscardEntry(slot);
            counters.QueueFull++;
            return SendStatus.QueueFull;
        }

        // Commit: nothing below fails.
        if (channel.HasSequence)
        {
            header.Sequence = send.NextSequence;
            send.NextSequence = channel.SequenceBits == 16 ? (ushort)(header.Sequence + 1) : header.Sequence + 1;
        }

        SendEntryTable entries = _core.Entries;
        entries.SetHeaderLength(slot, DatagramFraming.WriteHeader(entries.GetHeaderBlock(slot), channel, in header));
        EnginePayload.Commit(_core, slot, ref request, in payload);
        _core.StampAdmission(slot);
        entries.Sequences[slot] = header.Sequence;
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
        if (request.Options.Mode == SendMode.Immediate)
        {
            entry.Flags |= SendEntryFlags.Immediate;
        }

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
        int local = _localOf[channelIndex];
        ref ChannelSendState send = ref _send[local];
        int slot = send.QueueHead;
        if (slot < 0)
        {
            return;
        }

        SendEntryTable entries = _core.Entries;
        NativeArray<int> links = entries.Next;
        NativeArray<long> deadlines = entries.Deadlines;
        ref ChannelSendCounters counters = ref _core.SendCounters(channelIndex);
        DatagramPacker packer = _core.Packer;
        DatagramHints channelHints = _hints[local];
        long now = flush.NowMicros;
        while (slot >= 0)
        {
            // Read the link first: the packer reuses it for the container's member list.
            int next = links[slot];
            ref SendEntry entry = ref entries[slot];
            long deadline = deadlines[slot];
            if (deadline != 0 && now > deadline)
            {
                // PROTOCOL.md §4.5: expiry is evaluated at scheduling time, never after hand-off.
                counters.Expired++;
                entry.Aux1 = Finished;
                _core.QueueLocalCompletion(slot, DeliveryStatus.Expired);
            }
            else
            {
                DatagramHints hints = (entry.Flags & SendEntryFlags.Immediate) != 0 ? channelHints | DatagramHints.Priority : channelHints;
                if (IsFragment(slot))
                {
                    hints &= ~DatagramHints.DirectCompletion;
                }

                long payloadLength = entry.Payload.Length;
                PackResult result = packer.Add(slot, hints, ref flush);
                if (result == PackResult.Accepted)
                {
                    entry.Aux1 = HandedToPacker;
                    counters.Sent++;
                    counters.Bytes += payloadLength;
                }
                else if (result == PackResult.TooLarge)
                {
                    // The path's datagram limit shrank below the message after admission.
                    counters.TooLarge++;
                    entry.Aux1 = Finished;
                    _core.QueueLocalCompletion(slot, DeliveryStatus.Failed);
                }
                else
                {
                    // Blocked by the send cap or no datagrams right now: the entry stays at the head.
                    break;
                }
            }

            send.QueueBytes -= entry.Aux0;
            send.QueueCount--;
            send.QueueHead = next;
            if (next < 0)
            {
                send.QueueTail = -1;
            }

            slot = next;
        }
    }

    /// <inheritdoc/>
    public override void Flush(ref FlushContext flush)
    {
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Nothing is time-driven here: expiry is evaluated when the scheduler reaches an entry. The receive side's reassembly
    /// window (2 × RTT + 100 ms, PROTOCOL.md §7) is refreshed for the transport thread to read, which publishes no deadline —
    /// partials are swept when a fragment of their channel arrives.
    /// </remarks>
    public override void Tick(long nowMicros, ref long nextDeadline)
    {
        if (Fragments)
        {
            RefreshReassemblyWindow(nowMicros);
        }
    }

    /// <inheritdoc/>
    public override void OnSendCompleted(int entrySlot, in CompletionEntry completion)
    {
        if (TryCompleteFragment(entrySlot, in completion))
        {
            return;
        }

        if (!completion.Final)
        {
            // DatagramSendState.Sent: the transport (or the container) no longer references the payload.
            _core.ReleasePayload(entrySlot);
            _core.CompleteStage(entrySlot, CompletionStage.BufferReleased, DeliveryStatus.Pending);
            return;
        }

        ref SendEntry entry = ref _core.Entries[entrySlot];
        if (completion.Kind == CompletionKind.Local && entry.Aux1 == HandedToPacker)
        {
            // The packer took the message but the transport refused the datagram: it was never sent.
            ref ChannelSendCounters counters = ref _core.SendCounters(_core.ChannelIndexOf(entry.Channel));
            counters.Sent--;
            counters.Bytes -= entry.Payload.Length;
        }

        _core.CompleteEntry(entrySlot, _core.MapCompletion(in completion));
    }

    /// <inheritdoc/>
    /// <remarks>Only a message still queued (not yet handed to the packer) can be canceled; it completes <see cref="DeliveryStatus.Canceled"/> at the next Poll or Flush.</remarks>
    public override bool TryCancel(int entrySlot)
    {
        SendEntryTable entries = _core.Entries;
        if (Fragments && _fragmentOwner[entrySlot] == entrySlot && entries.GetState(entrySlot) == SendEntryState.Filling)
        {
            // The token of a fragmented message names its owner entry (docs/design/session-layer.md §7.8).
            return TryCancelFragmented(entrySlot);
        }

        ref SendEntry entry = ref entries[entrySlot];
        if (entries.GetState(entrySlot) != SendEntryState.Filling || entry.Aux1 != InQueue)
        {
            return false;
        }

        ref ChannelSendState send = ref _send[_localOf[_core.ChannelIndexOf(entry.Channel)]];
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
    public override void OnPeerClosed()
    {
        SendEntryTable entries = _core.Entries;
        NativeArray<int> links = entries.Next;
        for (int local = 0; local < _channels.Length; local++)
        {
            ref ChannelSendState send = ref _send[local];
            int slot = send.QueueHead;
            send.QueueHead = -1;
            send.QueueTail = -1;
            send.QueueCount = 0;
            send.QueueBytes = 0;
            while (slot >= 0)
            {
                int next = links[slot];
                // A fragment is finished through the fragment path, so its payload reference goes back to its message and its
                // outcome folds into it exactly as a transport completion would (docs/design/session-layer.md §7.8).
                CompletionEntry completion = new()
                {
                    Slot = slot,
                    Generation = entries[slot].Generation,
                    Kind = CompletionKind.Local,
                    Canceled = true,
                    Final = true,
                    Status = DeliveryStatus.Disconnected,
                };
                if (!TryCompleteFragment(slot, in completion))
                {
                    entries[slot].Aux1 = Finished;
                    _core.CompleteEntry(slot, DeliveryStatus.Disconnected);
                }

                slot = next;
            }
        }

        // The fragments were just finished one by one, so what is left of a fragmented message is its owner entry (a message
        // whose fragments the transport still holds; the queued ones completed their message above).
        AbandonFragmentedMessages(DeliveryStatus.Disconnected);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// PROTOCOL.md §4.1: sequences are scoped to the epoch. The first epoch starts from fresh tables; a later one on the
    /// same engine restarts the send counters here and asks the transport thread to forget its receive-side keys before the
    /// next datagram (receive state belongs to that thread).
    /// </remarks>
    public override void OnEpochReset(bool resumed)
    {
        if (_epochs++ == 0)
        {
            return;
        }

        for (int local = 0; local < _channels.Length; local++)
        {
            _send[local].NextSequence = 0;
        }

        Volatile.Write(ref _resetReceive, 1);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// The queues were completed <see cref="DeliveryStatus.Disconnected"/> before this call, so only the FIFO bookkeeping and
    /// the epoch-scoped state are left. No transport callback can arrive here, so the receive tables are cleared directly
    /// instead of through the flag <see cref="OnEpochReset"/> has to use.
    /// </remarks>
    public override void OnReconnecting()
    {
        for (int local = 0; local < _channels.Length; local++)
        {
            ref ChannelSendState send = ref _send[local];
            send.QueueHead = -1;
            send.QueueTail = -1;
            send.QueueCount = 0;
            send.QueueBytes = 0;
            send.NextSequence = 0;
        }

        Volatile.Write(ref _resetReceive, 0);
        ResetFragmentOwners();
        ResetReceiveState();
    }

    // ------------------------------------------------------------------ receive (transport thread)

    /// <inheritdoc/>
    public override void OnDatagram(in MessageHeader header, ReadOnlySpan<byte> payload, long nowMicros)
    {
        if (Volatile.Read(ref _resetReceive) != 0 && Interlocked.Exchange(ref _resetReceive, 0) != 0)
        {
            ResetReceiveState();
        }

        int dense = _core.ChannelIndexOf(header.Channel);
        int local = _localOf[dense];
        ref ChannelRecvCounters counters = ref _core.RecvCounters(dense);
        if (header.IsFragment)
        {
            OnFragment(in header, payload, nowMicros);
            return;
        }

        if (!Accept(local, in header, out int keySlot, nowMicros, ref counters))
        {
            return;
        }

        BufferLease lease = BufferLease.Empty;
        if (!payload.IsEmpty)
        {
            if (!_core.TryRentReceive(payload.Length, out lease))
            {
                _core.Counters.OutOfReceiveBuffers++;
                counters.OutOfBuffers++;
                return;
            }

            payload.CopyTo(_core.GetSpan(in lease));
        }

        ReceiveEntry entry = default;
        entry.Channel = header.Channel;
        entry.Flags = header.Compressed ? ReceiveFlags.Compressed : ReceiveFlags.None;
        entry.Sequence = header.Sequence;
        entry.Key = header.Key;
        entry.Lease = lease;
        entry.Length = payload.Length;
        entry.RawLength = header.RawLength;
        entry.ReceivedMicrosDelta = PeerCore.StampReceive(nowMicros);
        entry.SenderTick = _core.CurrentSenderTick;
        ReceiveMailbox? box = _mailboxes[local];
        if (box is not null)
        {
            if (!box.TryPost(keySlot, in entry, out BufferLease displaced, out bool replaced))
            {
                _core.ReturnReceive(in lease);
                counters.RingDrops++;
                return;
            }

            if (replaced)
            {
                _core.ReturnReceive(in displaced);
                counters.Superseded++;
            }

            // A mailbox bypasses the receive ring, so the peer's work signal is raised here instead (ADR 0008 §6).
            _core.NoteTransportWork();
        }
        else if (!_core.TryEnqueueReceive(in entry))
        {
            // PROTOCOL.md §7: a full ring drops the newest message (the peer-level counter is kept by the core).
            _core.ReturnReceive(in lease);
            counters.RingDrops++;
            return;
        }

        counters.Received++;
        counters.Bytes += payload.Length;
    }

    /// <inheritdoc/>
    /// <remarks>Datagram-only channels never carry streams; the peer resets such streams before they get here.</remarks>
    public override StreamAccept OnStreamOpened(TransportStreamId id, ushort channel, ulong groupId) => StreamAccept.Reject(QuiclyErrorCode.UnsupportedChannel);

    /// <inheritdoc/>
    public override StreamConsume OnStreamMessage(ref StreamMessageContext message) => StreamConsume.ResetStream(QuiclyErrorCode.UnsupportedChannel);

    /// <inheritdoc/>
    public override void OnStreamClosed(TransportStreamId id, bool aborted, ulong errorCode)
    {
    }

    /// <inheritdoc/>
    public override void Dispose()
    {
        DisposeFragmentation();
        _send?.Dispose();
        _recv?.Dispose();
        foreach (ReceiveKeyTracker? keys in _keys)
        {
            keys?.Dispose();
        }
    }

    private void ResetReceiveState()
    {
        // The partials of the epoch that ended are given up first: their count lives in the receive state below.
        ClearReassemblies();
        for (int local = 0; local < _channels.Length; local++)
        {
            _recv[local] = default;
            _keys[local]?.Clear();
        }
    }
}
