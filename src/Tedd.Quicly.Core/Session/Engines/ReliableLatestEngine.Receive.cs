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

// The ReliableLatest engine's receive side (transport thread), the coalesced acks and rejects it owes (game thread), and
// key retirement in both directions. PROTOCOL.md §2.1, §2.3, §3.2, §4.4 and §7; docs/design/session-layer.md §7.6.
internal sealed unsafe partial class ReliableLatestEngine
{
    private int _epochs;
    private int _resetReceive;
    private LatestRecvKeys[] _recvKeys = [];
    private ReceiveMailbox[] _mailboxes = [];
    private int[] _activeGroups = [];
    private int[] _ackSweep = [];
    private NativeArray<LatestRecvStream> _streams = null!;
    private int[] _streamFree = [];
    private int _streamFreeCount;
    private int _streamCapacity;
    private SpscRing<AckRequest> _ackQueue = null!;
    private SpscRing<RejectRequest> _rejectQueue = null!;
    private SpscRing<LatestNotice> _notices = null!;

    // The starts, refusals and shutdowns of the large-value streams this engine opened, in a ring of their own: sized for
    // every stream the engine can have open, so that a burst of acks can never cost one of them. A lost shutdown would
    // leave its per-channel stream slot taken for the rest of the connection.
    private SpscRing<LatestNotice> _streamNotices = null!;
    private AckRequest _heldAck;
    private RejectRequest _heldReject;
    private int _sweepLocal = -1;
    private int _sweepSlot;
    private long _maxStage;

    private bool _ackDatagramsRefused;

    /// <summary>
    /// The last pass that was due to send owed acks sent none (no carrier fits a batch): until one does, a Poll does not
    /// bring the flush deadline forward for them (<see cref="LowerControlDeadline"/>). Game thread.
    /// </summary>
    private bool _acksStuck;

    /// <summary>Clock micros of the first Poll that saw acks owed since the last transmission, or 0 (game thread).</summary>
    private long _ackOwedSince;
    private TransportStreamId[] _txStreams = [];
    private int[] _txLocals = [];

    private enum NoticeKind : byte
    {
        Ack = 0,
        Reject = 1,

        /// <summary>A large-value stream this engine opened has shut down; its per-channel slot is free again.</summary>
        StreamClosed = 2,

        /// <summary>
        /// The peer's stream limit refused the start of a large-value stream this engine opened. The send that carried the
        /// start completes canceled and the stream shuts down afterwards; the value never reached the peer and waits for
        /// stream credit.
        /// </summary>
        StreamRefused = 3,

        /// <summary>
        /// The start of a large-value stream this engine opened succeeded: the transmission it carries is now one that left
        /// this host, and is counted as one (<see cref="ReliableLatestEngine.OnLargeStreamStarted"/>).
        /// </summary>
        StreamStarted = 4,
    }

    /// <summary>Builds the receive-side state (engine construction, game thread).</summary>
    private void InitializeReceive(PeerCore core, int count)
    {
        _recvKeys = new LatestRecvKeys[count];
        _mailboxes = new ReceiveMailbox[count];
        _activeGroups = new int[count];
        _ackSweep = new int[count];
        long ackSlots = 16;
        int streams = 0;
        for (int local = 0; local < count; local++)
        {
            ChannelDefinition channel = _channels[local];
            _recvKeys[local] = new LatestRecvKeys(channel);
            _mailboxes[local] = core.CreateMailbox(core.ChannelIndexOf(channel.Id), channel.MaxKeys);
            ackSlots += Math.Min(channel.MaxKeys, 4096);
            streams += Math.Max(channel.MaxGroups, 1);
        }

        _streamCapacity = Math.Max(streams, 1);
        _streams = new NativeArray<LatestRecvStream>(_streamCapacity);

        // One entry per stream this engine may hold open plus one per channel: a start the peer refused is recorded here too
        // until its shutdown arrives, and it must never crowd out the record of a stream that is really carrying a value.
        _txStreams = new TransportStreamId[_streamCapacity + count];
        _txLocals = new int[_streamCapacity + count];
        _streamFree = new int[_streamCapacity];
        for (int i = 0; i < _streamCapacity; i++)
        {
            _streamFree[i] = _streamCapacity - 1 - i;
        }

        _streamFreeCount = _streamCapacity;
        // One entry per key with an ack outstanding, bounded: beyond this many keys the per-channel sweep finds the rest, so
        // the ring stays small (1 024 × 8 B) whatever MaxKeys is.
        _ackQueue = new SpscRing<AckRequest>((int)Math.Min(ackSlots, MaxAckRingSlots));
        _rejectQueue = new SpscRing<RejectRequest>(256);
        _notices = new SpscRing<LatestNotice>(1024);
        // A stream the engine opened raises its start (or its refusal) and its shutdown: two notices, and its record in
        // _txStreams is free again only after the second.
        _streamNotices = new SpscRing<LatestNotice>((2 * _txStreams.Length) + 8);
        _heldAck.Local = -1;
        // A value is staged whole, so it can never be larger than the receive budget or the pool's largest block (§7.2).
        _maxStage = Math.Min(core.ReceiveBudgetBytes, core.Allocator.MaxBlockSize);
    }

    /// <summary>
    /// Asks the transport thread to forget its per-key receive state before the next value, whichever way that value arrives
    /// (a new epoch, PROTOCOL.md §4.1), and drops the acks this end still owed: their versions belong to the counter that
    /// just ended, so sending one could tell the peer a value of the new epoch had arrived. Game thread.
    /// </summary>
    private void ResetReceiveState()
    {
        for (int local = 0; local < _channels.Length; local++)
        {
            LatestRecvKeys keys = _recvKeys[local];
            int used = keys.SlotsUsed;
            for (int slot = 0; slot < used; slot++)
            {
                Interlocked.Exchange(ref keys.VersionRef(slot), 0);
            }
        }

        while (_ackQueue.TryDequeue(out _))
        {
        }

        while (_rejectQueue.TryDequeue(out _))
        {
        }

        Array.Clear(_ackSweep);
        _heldAck.Local = -1;
        _heldReject = default;
        _sweepLocal = -1;
        _sweepSlot = 0;
        Volatile.Write(ref _resetReceive, 1);
    }

    /// <summary>
    /// Forgets the per-key receive state once, if a new epoch asked for it (<see cref="ResetReceiveState"/>). Every receive
    /// entry point calls this before it looks at a key: the epoch restarts the sender's version counter, so a value that
    /// arrives on a group stream must not be measured against the previous epoch's versions any more than a datagram must
    /// (PROTOCOL.md §4.1, §4.3). Transport thread.
    /// </summary>
    private void ConsumeEpochReset()
    {
        if (Volatile.Read(ref _resetReceive) != 0 && Interlocked.Exchange(ref _resetReceive, 0) != 0)
        {
            ClearReceiveKeys();
        }
    }

    private void DisposeReceive()
    {
        if (_streams is not null && !_streams.IsDisposed)
        {
            for (int i = 0; i < _streamCapacity; i++)
            {
                ref LatestRecvStream stream = ref _streams[i];
                if (!stream.Lease.IsEmpty)
                {
                    _core.ReturnReceive(in stream.Lease);
                    stream.Lease = BufferLease.Empty;
                }
            }

            _streams.Dispose();
        }

        foreach (LatestRecvKeys keys in _recvKeys)
        {
            keys?.Dispose();
        }

        _ackQueue?.Dispose();
        _rejectQueue?.Dispose();
        _notices?.Dispose();
        _streamNotices?.Dispose();
    }

    // ------------------------------------------------------------------ control messages (transport thread)

    /// <inheritdoc/>
    /// <remarks>
    /// The peer has validated the batch's structure (<see cref="ControlCodec"/>); this checks that every entry names a
    /// ReliableLatest channel of this engine and hands the entries to the game thread, which owns the send state.
    /// </remarks>
    public override bool OnControl(ControlType type, ReadOnlySpan<byte> body, bool onStream, long nowMicros)
    {
        switch (type)
        {
            case ControlType.LatestAck:
            {
                if (ControlCodec.TryParse(body, out LatestAckBatchReader reader) != ControlParseStatus.Ok)
                {
                    return false;
                }

                foreach (LatestAckEntry entry in reader)
                {
                    int local = LocalOf(entry.Channel);
                    if (local < 0)
                    {
                        return false;
                    }

                    Post(new LatestNotice { Local = local, Key = entry.Key, Version = entry.Version, Kind = NoticeKind.Ack });
                }

                return true;
            }

            case ControlType.LatestReject:
            {
                if (ControlCodec.TryParse(body, out LatestRejectBatchReader reader) != ControlParseStatus.Ok)
                {
                    return false;
                }

                foreach (LatestRejectEntry entry in reader)
                {
                    int local = LocalOf(entry.Channel);
                    if (local < 0)
                    {
                        return false;
                    }

                    Post(new LatestNotice
                    {
                        Local = local,
                        Key = entry.Key,
                        Version = entry.Version,
                        Kind = NoticeKind.Reject,
                        Reason = entry.Reason,
                    });
                }

                return true;
            }

            case ControlType.KeyRetired:
            {
                if (ControlCodec.TryParse(body, out KeyRetired retired) != ControlParseStatus.Ok)
                {
                    return false;
                }

                int local = LocalOf(retired.Channel);
                if (local < 0)
                {
                    return false;
                }

                DeliverKeyRetired(local, retired.Key, nowMicros);
                return true;
            }

            default:
                return true;
        }
    }

    private int LocalOf(ushort channel)
    {
        int dense = _core.ChannelIndexOf(channel);
        return dense >= 0 ? _localOf[dense] : -1;
    }

    /// <summary>
    /// Hands a LatestAck / LatestReject entry or a stream shutdown to the game thread, whose next scheduler pass applies it,
    /// and raises the work signal for it (transport thread; once per callback, <see cref="PeerCore.NoteTransportWork"/>).
    /// </summary>
    private void Post(in LatestNotice notice)
    {
        if (!_notices.TryEnqueue(in notice))
        {
            // The game thread has not drained them yet; a lost ack is covered by the sender's timer (PROTOCOL.md §4.4).
            _core.Counters.CallbackFaults++;
            return;
        }

        _core.NoteTransportWork();
    }

    /// <summary>
    /// Hands the start, the refusal or the shutdown of a stream this engine opened to the game thread (transport thread).
    /// The ring holds two notices per stream the engine can have recorded, so it does not fill.
    /// </summary>
    private void PostStream(in LatestNotice notice)
    {
        if (!_streamNotices.TryEnqueue(in notice))
        {
            _core.Counters.CallbackFaults++;
            return;
        }

        _core.NoteTransportWork();
    }

    // ------------------------------------------------------------------ datagram values (transport thread)

    /// <inheritdoc/>
    public override void OnDatagram(in MessageHeader header, ReadOnlySpan<byte> payload, long nowMicros)
    {
        ConsumeEpochReset();
        int dense = _core.ChannelIndexOf(header.Channel);
        int local = _localOf[dense];
        ref ChannelRecvCounters counters = ref _core.RecvCounters(dense);
        LatestRecvKeys keys = _recvKeys[local];

        // Every arrival moves the channel's version clock, whatever then becomes of the value: the clock follows the
        // sender's counter, not what this end could keep.
        ulong extended = ExtendVersion(keys, header.Sequence);
        if (!TryGetRecvSlot(local, header.Key, out int keySlot))
        {
            // PROTOCOL.md §7: a full ReliableLatest key table rejects with reason 4 and never evicts.
            counters.KeyTableFull++;
            QueueReject(local, header.Key, header.Sequence, LatestRejectReason.KeyTableFull);
            return;
        }

        ref KeyRecvSlot key = ref keys[keySlot];
        if ((key.Flags & KeyRecvFlags.Retired) != 0 && !TryReuseRetiredSlot(local, keySlot, ref key))
        {
            // The application has not taken the key's retirement yet: a value posted now would displace it (§3.4 type 0x17).
            counters.RingDrops++;
            QueueReject(local, header.Key, header.Sequence, LatestRejectReason.RingFull);
            return;
        }

        if ((key.Flags & KeyRecvFlags.HasAccepted) != 0 && extended <= key.LastAcceptedExtended)
        {
            // An older or duplicate version: re-ack the current one so a lost ack cannot stall the sender (§4.4).
            counters.Dropped++;
            QueueAck(local, keySlot, key.LastAccepted);
            return;
        }

        if (header.RawLength > _maxStage)
        {
            counters.TooLarge++;
            QueueReject(local, header.Key, header.Sequence, LatestRejectReason.DecodeError);
            return;
        }

        BufferLease lease = BufferLease.Empty;
        if (!payload.IsEmpty && !_core.TryRentReceive(payload.Length, out lease))
        {
            _core.Counters.OutOfReceiveBuffers++;
            counters.OutOfBuffers++;
            QueueReject(local, header.Key, header.Sequence, LatestRejectReason.RingFull);
            return;
        }

        if (!payload.IsEmpty)
        {
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
        Accept(local, keySlot, ref key, in entry, extended, nowMicros, ref counters);
    }

    /// <summary>
    /// Extends a version on the channel's version clock and advances the clock when the version is ahead of it (transport
    /// thread; PROTOCOL.md §8). The sender takes every version of a channel from one counter, so the newest version that
    /// arrived for <em>any</em> key says where that counter is, and a version extended against it compares with a key's last
    /// accepted one as a plain 64-bit number — however many versions of other keys lie between the two. The first version of
    /// an epoch seeds the clock one span up, which keeps 0 free as "nothing seen" and leaves room below the seed for the half
    /// span a later arrival may extend behind it.
    /// </summary>
    /// <remarks>
    /// Unlike the sequence clock of an UnreliableSequenced channel, this one never resynchronises on time: a retransmission
    /// legitimately arrives behind the newest version after any length of quiet, and must stay a duplicate.
    /// </remarks>
    /// <param name="keys">The channel's receive keys, which hold the clock.</param>
    /// <param name="version">The arriving version.</param>
    /// <returns>The version on the channel's 64-bit scale.</returns>
    private static ulong ExtendVersion(LatestRecvKeys keys, uint version)
    {
        ulong newest = keys.Newest;
        if (newest == 0)
        {
            newest = 0x1_0000_0000UL + version;
            keys.Newest = newest;
            return newest;
        }

        ulong extended = SerialNumber.Extend(newest, version);
        if (extended > newest)
        {
            keys.Newest = extended;
        }

        return extended;
    }

    /// <summary>
    /// Publishes an accepted value into the key's mailbox (ReliableLatest always coalesces, so no receive-ring entry and no
    /// reservation, ADR 0008 invariant 6), queues the key's ack and raises the work signal: a mailbox bypasses the receive
    /// ring, whose publication would otherwise have raised it.
    /// </summary>
    private void Accept(int local, int keySlot, ref KeyRecvSlot key, in ReceiveEntry entry, ulong extended, long nowMicros, ref ChannelRecvCounters counters)
    {
        ReceiveMailbox box = _mailboxes[local];
        if (!box.TryPost(keySlot, in entry, out BufferLease displaced, out bool replaced))
        {
            _core.ReturnReceive(in entry.Lease);
            counters.RingDrops++;
            QueueReject(local, entry.Key, entry.Sequence, LatestRejectReason.RingFull);
            return;
        }

        if (replaced)
        {
            _core.ReturnReceive(in displaced);
            counters.Superseded++;
        }

        key.LastAccepted = entry.Sequence;
        key.LastAcceptedExtended = extended;
        key.LastUpdateMicros = nowMicros;
        key.Updates++;
        key.Flags |= KeyRecvFlags.HasAccepted;
        counters.Received++;
        counters.Bytes += entry.Length;
        EnqueueAck(local, keySlot, entry.Sequence);

        // One signal covers the value and its ack (and, inside a callback, every other value of the burst).
        _core.NoteTransportWork();
    }

    /// <summary>
    /// Records the version to acknowledge for a key and raises the work signal when the key was not queued yet: the ack goes
    /// out in the next scheduler pass (transport thread).
    /// </summary>
    private void QueueAck(int local, int keySlot, uint version)
    {
        if (EnqueueAck(local, keySlot, version))
        {
            _core.NoteTransportWork();
        }
    }

    /// <summary>
    /// Records the version to acknowledge for a key (transport thread). The version word is a mailbox: a non-zero previous
    /// value means the key is already queued, so the ring holds at most one entry per key. When the ring is full the version
    /// stays in the word and a sweep flag makes the game thread find it.
    /// </summary>
    /// <returns><see langword="true"/> when the key was queued now (ring entry or sweep flag); an already queued key only had its version raised.</returns>
    private bool EnqueueAck(int local, int keySlot, uint version)
    {
        if (version == 0)
        {
            return false;
        }

        LatestRecvKeys keys = _recvKeys[local];
        uint previous = Interlocked.Exchange(ref keys.VersionRef(keySlot), version);
        if (previous != 0)
        {
            return false;
        }

        AckRequest request = new() { Local = local, KeySlot = keySlot };
        if (!_ackQueue.TryEnqueue(in request))
        {
            Interlocked.Exchange(ref _ackSweep[local], 1);
        }

        return true;
    }

    /// <summary>Queues a LatestReject for the next scheduler pass and raises the work signal (transport thread).</summary>
    private void QueueReject(int local, ulong key, uint version, LatestRejectReason reason)
    {
        RejectRequest request = new() { Channel = _channels[local].Id, Key = key, Version = version, Reason = reason };
        if (!_rejectQueue.TryEnqueue(in request))
        {
            // A lost reject only costs the sender one more timed retry (PROTOCOL.md §4.4).
            _core.Counters.CallbackFaults++;
            return;
        }

        _core.NoteTransportWork();
    }

    /// <summary>Frees the per-key receive state of a key the peer retired and reports it to the application (PROTOCOL.md §3.4 type 0x17).</summary>
    private void DeliverKeyRetired(int local, ulong key, long nowMicros)
    {
        LatestRecvKeys keys = _recvKeys[local];
        int dense = _denseOf[local];
        ref ChannelRecvCounters counters = ref _core.RecvCounters(dense);
        if (!TryGetRecvSlot(local, key, out int keySlot))
        {
            counters.KeyTableFull++;
            return;
        }

        ReceiveEntry entry = default;
        entry.Channel = _channels[local].Id;
        entry.Key = key;
        entry.Sequence = keys[keySlot].LastAccepted;
        entry.Flags = ReceiveFlags.KeyRetired;
        entry.ReceivedMicrosDelta = PeerCore.StampReceive(nowMicros);
        if (_mailboxes[local].TryPost(keySlot, in entry, out BufferLease displaced, out bool replaced))
        {
            if (replaced)
            {
                _core.ReturnReceive(in displaced);
                counters.Superseded++;
            }

            // The retirement bypasses the receive ring like a value, so it raises the work signal itself.
            _core.NoteTransportWork();
        }

        // The slot is kept until the application has taken the notice: freeing it here would let a new holder of the same key
        // id post into the mailbox and displace the retirement, and the application would never hear that the key was
        // retired. The next value for the key takes the slot over once the notice is gone (TryReuseRetiredSlot), and a key
        // retired and never reused is reclaimed when the table is full (ReclaimRetiredSlots).
        Interlocked.Exchange(ref keys.VersionRef(keySlot), 0);
        keys[keySlot].Flags |= KeyRecvFlags.Retired;
    }

    /// <summary>
    /// The receive slot of <paramref name="key"/>. A full table first gives up the slots of keys whose retirement the
    /// application has already taken, because those are free in every sense; a live key is never evicted (PROTOCOL.md §7).
    /// Transport thread.
    /// </summary>
    /// <param name="local">The channel.</param>
    /// <param name="key">The key.</param>
    /// <param name="keySlot">The slot, when one was found.</param>
    /// <returns><see langword="false"/> when the key can get no slot.</returns>
    private bool TryGetRecvSlot(int local, ulong key, out int keySlot)
    {
        LatestRecvKeys keys = _recvKeys[local];
        return keys.TryGetOrAdd(key, out keySlot) || (ReclaimRetiredSlots(local) && keys.TryGetOrAdd(key, out keySlot));
    }

    /// <summary>Gives up the slot of every retired key of a channel whose notice has been delivered (transport thread).</summary>
    /// <param name="local">The channel.</param>
    /// <returns>Whether at least one slot was freed.</returns>
    private bool ReclaimRetiredSlots(int local)
    {
        LatestRecvKeys keys = _recvKeys[local];
        ReceiveMailbox box = _mailboxes[local];
        bool freed = false;
        int used = keys.SlotsUsed;
        for (int slot = 0; slot < used; slot++)
        {
            ref KeyRecvSlot candidate = ref keys[slot];
            if ((candidate.Flags & KeyRecvFlags.Retired) == 0 || box.HasPending(slot))
            {
                continue;
            }

            ulong retired = candidate.Key;
            candidate = default;
            keys.Remove(retired);
            freed = true;
        }

        return freed;
    }

    /// <summary>
    /// Lets a new holder of a retired key take the slot over, once the application has taken the retirement out of the
    /// mailbox: the slot forgets its versions, so the new holder's first value is newer than anything remembered.
    /// Transport thread.
    /// </summary>
    /// <param name="local">The channel.</param>
    /// <param name="keySlot">The slot.</param>
    /// <param name="key">The slot's state.</param>
    /// <returns><see langword="false"/> while the retirement is still waiting to be delivered.</returns>
    private bool TryReuseRetiredSlot(int local, int keySlot, ref KeyRecvSlot key)
    {
        if (_mailboxes[local].HasPending(keySlot))
        {
            return false;
        }

        ulong id = key.Key;
        key = default;
        key.Key = id;
        return true;
    }

    private void ClearReceiveKeys()
    {
        for (int local = 0; local < _channels.Length; local++)
        {
            _recvKeys[local].Clear();
        }
    }

    // ------------------------------------------------------------------ large values on group streams (transport thread)

    /// <inheritdoc/>
    /// <remarks>
    /// PROTOCOL.md §3.2: a ReliableLatest group stream carries exactly one value whose <c>GroupId</c> is its version. At most
    /// <see cref="ChannelDefinition.MaxGroups"/> of them are accepted per channel (§7); further streams are reset
    /// <see cref="QuiclyErrorCode.LimitExceeded"/>.
    /// </remarks>
    public override StreamAccept OnStreamOpened(TransportStreamId id, ushort channel, ulong groupId)
    {
        ConsumeEpochReset();
        int local = LocalOf(channel);
        if (local < 0)
        {
            return StreamAccept.Reject(QuiclyErrorCode.UnsupportedChannel);
        }

        if (_activeGroups[local] >= Math.Max(_channels[local].MaxGroups, 1) || _streamFreeCount == 0)
        {
            return StreamAccept.Reject(QuiclyErrorCode.LimitExceeded);
        }

        int index = _streamFree[--_streamFreeCount];
        ref LatestRecvStream stream = ref _streams[index];
        stream = default;
        stream.Stream = id;
        stream.Local = local;
        stream.Version = (uint)groupId;
        stream.InUse = 1;
        _activeGroups[local]++;
        return StreamAccept.Accept(index);
    }

    /// <inheritdoc/>
    public override StreamConsume OnStreamMessage(ref StreamMessageContext message)
    {
        int index = (int)message.Cookie;
        ref LatestRecvStream stream = ref _streams[index];
        if (stream.InUse == 0 || stream.Stream != message.Id)
        {
            // The staging records are pooled, so a record is only ever read for the stream it was handed to: one that has
            // been freed (its stream shut down) may already be serving another stream.
            return StreamConsume.ResetStream(QuiclyErrorCode.ProtocolViolation);
        }

        int local = stream.Local;
        ref ChannelRecvCounters counters = ref _core.RecvCounters(message.ChannelIndex);
        switch (message.Phase)
        {
            case StreamMessagePhase.Start:
            {
                // A stream opened in the epoch that just ended can still deliver its first message here, so the reset is
                // consumed on this path too (PROTOCOL.md §4.1).
                ConsumeEpochReset();

                // The parser has checked that Sequence equals the stream's GroupId (PROTOCOL.md §8 item 8).
                uint version = message.Header.Sequence;
                ulong key = message.Header.Key;
                stream.Key = key;
                stream.Version = version;
                stream.Length = message.Header.Length;
                stream.RawLength = message.Header.RawLength;
                stream.Filled = 0;

                // The version moves the channel's clock like a datagram's does, whatever then becomes of the value. The
                // extended version is not kept with the stream: End derives it again, against a clock that may have moved.
                LatestRecvKeys keys = _recvKeys[local];
                ulong extended = ExtendVersion(keys, version);
                if (message.Header.Length > _maxStage)
                {
                    counters.TooLarge++;
                    QueueReject(local, key, version, LatestRejectReason.TooLarge);
                    return StreamConsume.ResetStream(QuiclyErrorCode.LimitExceeded);
                }

                if (message.Header.RawLength > _maxStage)
                {
                    counters.TooLarge++;
                    QueueReject(local, key, version, LatestRejectReason.DecodeError);
                    return StreamConsume.ResetStream(QuiclyErrorCode.LimitExceeded);
                }

                if (!TryGetRecvSlot(local, key, out int keySlot))
                {
                    counters.KeyTableFull++;
                    QueueReject(local, key, version, LatestRejectReason.KeyTableFull);
                    return StreamConsume.ResetStream(QuiclyErrorCode.LimitExceeded);
                }

                stream.KeySlot = keySlot;
                if ((keys[keySlot].Flags & KeyRecvFlags.Retired) != 0 && !TryReuseRetiredSlot(local, keySlot, ref keys[keySlot]))
                {
                    // The key's retirement is still waiting to be delivered, so this value has nowhere to go yet (§3.4).
                    counters.RingDrops++;
                    QueueReject(local, key, version, LatestRejectReason.RingFull);
                    return StreamConsume.ResetStream(QuiclyErrorCode.LimitExceeded);
                }

                if ((keys[keySlot].Flags & KeyRecvFlags.HasAccepted) != 0 && extended <= keys[keySlot].LastAcceptedExtended)
                {
                    // Stale: consume the stream and re-ack the current version at its end.
                    counters.Dropped++;
                    stream.Drop = 1;
                    return StreamConsume.Continue;
                }

                if (message.Header.Length > 0 && !_core.TryRentReceive(message.Header.Length, out stream.Lease))
                {
                    _core.Counters.OutOfReceiveBuffers++;
                    counters.OutOfBuffers++;
                    QueueReject(local, key, version, LatestRejectReason.RingFull);
                    stream.Drop = 1;
                }

                return StreamConsume.Continue;
            }

            case StreamMessagePhase.Chunk:
            {
                if (stream.Drop != 0)
                {
                    return StreamConsume.Continue;
                }

                ReadOnlySpan<byte> chunk = message.Chunk;
                chunk.CopyTo(new Span<byte>(_core.GetPointer(in stream.Lease) + stream.Filled, stream.Length - stream.Filled));
                stream.Filled += chunk.Length;
                return StreamConsume.Continue;
            }

            case StreamMessagePhase.End:
            {
                LatestRecvKeys keys = _recvKeys[local];
                if (stream.Drop != 0)
                {
                    stream.Drop = 0;
                    QueueAck(local, stream.KeySlot, keys[stream.KeySlot].LastAccepted);
                    return StreamConsume.Continue;
                }

                // The value was newer than the key's when the stream started, but a stream takes time and a datagram of the
                // same key can overtake it: judged again here, or an older value would replace a newer one in the mailbox
                // and the ack owed for the newer version would be overwritten with the older one.
                ref KeyRecvSlot slot = ref keys[stream.KeySlot];
                ulong extended = ExtendVersion(keys, stream.Version);
                if ((slot.Flags & KeyRecvFlags.HasAccepted) != 0 && extended <= slot.LastAcceptedExtended)
                {
                    if (!stream.Lease.IsEmpty)
                    {
                        _core.ReturnReceive(in stream.Lease);

                        // OnStreamClosed returns whatever the record still holds.
                        stream.Lease = BufferLease.Empty;
                    }

                    counters.Dropped++;
                    QueueAck(local, stream.KeySlot, slot.LastAccepted);
                    return StreamConsume.Continue;
                }

                ReceiveEntry entry = default;
                entry.Channel = message.Channel;
                entry.Flags = stream.RawLength > 0 ? ReceiveFlags.Compressed : ReceiveFlags.None;
                entry.Sequence = stream.Version;
                entry.Key = stream.Key;
                entry.Lease = stream.Lease;
                entry.Length = stream.Length;
                entry.RawLength = stream.RawLength;
                entry.ReceivedMicrosDelta = PeerCore.StampReceive(message.NowMicros);
                stream.Lease = BufferLease.Empty;
                Accept(local, stream.KeySlot, ref slot, in entry, extended, message.NowMicros, ref counters);
                return StreamConsume.Continue;
            }

            default:
                return StreamConsume.ResetStream(QuiclyErrorCode.ProtocolViolation);
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// A stream this engine opened has shut down (or never started): its per-channel slot is handed back to the game thread
    /// through a notice, because the receiver frees its own slot at the same point. For a peer stream this releases what a
    /// half-received value held (the peer's mid-message idle sweep resets a stalled stream, PROTOCOL.md §7).
    /// </remarks>
    public override void OnStreamClosed(TransportStreamId id, bool aborted, ulong errorCode)
    {
        for (int index = 0; index < _streamCapacity; index++)
        {
            ref LatestRecvStream stream = ref _streams[index];
            if (stream.InUse == 0 || stream.Stream != id)
            {
                continue;
            }

            if (!stream.Lease.IsEmpty)
            {
                _core.ReturnReceive(in stream.Lease);
                stream.Lease = BufferLease.Empty;
            }

            _activeGroups[stream.Local]--;
            stream.InUse = 0;
            _streamFree[_streamFreeCount++] = index;
            return;
        }

        if (!id.IsValid)
        {
            return;
        }

        for (int index = 0; index < _txStreams.Length; index++)
        {
            if (_txStreams[index] == id)
            {
                _txStreams[index] = default;
                PostStream(new LatestNotice { Local = _txLocals[index], Kind = NoticeKind.StreamClosed, Stream = id });
                return;
            }
        }
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Records a large-value stream this engine opened so that <see cref="OnStreamClosed"/> can recognise it (transport
    /// thread; the peer routes it here by the context's mode), and tells the game thread how the start went: the
    /// transmission the stream carries is counted only once it is known to have started. A start the peer's stream limit
    /// refused is followed by the canceled completion of its send, which puts the value back in its queue; the refusal
    /// makes the channel's large values wait for stream credit (MsQuic and the simulator refuse a start only here, never
    /// in the call that made it).
    /// </remarks>
    public override void OnStreamStarted(TransportStreamId id, ulong context, TransportStatus status)
    {
        if (!PeerCore.TryDecodeEngineStreamContext(context, out ChannelMode mode, out int dense, out _)
            || mode != ChannelMode.ReliableLatest
            || (uint)dense >= (uint)_localOf.Length)
        {
            return;
        }

        int local = _localOf[dense];
        if (local < 0)
        {
            return;
        }

        if (status == TransportStatus.Success)
        {
            PostStream(new LatestNotice { Local = local, Kind = NoticeKind.StreamStarted, Stream = id });
        }
        else if (status == TransportStatus.StreamLimitReached)
        {
            PostStream(new LatestNotice { Local = local, Kind = NoticeKind.StreamRefused, Stream = id });
        }

        for (int index = 0; index < _txStreams.Length; index++)
        {
            if (!_txStreams[index].IsValid)
            {
                _txStreams[index] = id;
                _txLocals[index] = local;
                return;
            }
        }
    }

    /// <summary>Drops everything the lost connection held (game thread, <see cref="ChannelEngine.OnReconnecting"/>).</summary>
    private void ResetReceiveForReconnect()
    {
        for (int index = 0; index < _streamCapacity; index++)
        {
            ref LatestRecvStream stream = ref _streams[index];
            if (stream.InUse != 0 && !stream.Lease.IsEmpty)
            {
                _core.ReturnReceive(in stream.Lease);
            }

            stream = default;
            _streamFree[index] = _streamCapacity - 1 - index;
        }

        Array.Clear(_txStreams);
        _streamFreeCount = _streamCapacity;
        Array.Clear(_activeGroups);
        Array.Clear(_ackSweep);
        while (_ackQueue.TryDequeue(out _))
        {
        }

        while (_rejectQueue.TryDequeue(out _))
        {
        }

        while (_notices.TryDequeue(out _))
        {
        }

        while (_streamNotices.TryDequeue(out _))
        {
        }

        // Pending acks name versions of the epoch that ended.
        for (int local = 0; local < _channels.Length; local++)
        {
            LatestRecvKeys keys = _recvKeys[local];
            int used = keys.SlotsUsed;
            for (int slot = 0; slot < used; slot++)
            {
                Interlocked.Exchange(ref keys.VersionRef(slot), 0);
            }
        }

        _heldAck.Local = -1;
        _heldReject = default;
        _sweepLocal = -1;
        _sweepSlot = 0;
        _nextAckMicros = 0;
        _ackDatagramsRefused = false;
        _acksStuck = false;
        _ackOwedSince = 0;
    }

    // ------------------------------------------------------------------ acks the peer sent us (game thread)

    /// <summary>
    /// Drops the acks and rejects the peer sent in the epoch that just ended — their versions belong to a counter that no
    /// longer exists — while still applying the stream shutdowns among them, which release per-channel stream slots that
    /// nothing else would give back (game thread, <see cref="OnEpochReset"/>).
    /// </summary>
    private void DropNoticesOfClosedEpoch()
    {
        while (_notices.TryDequeue(out _))
        {
        }

        while (_streamNotices.TryDequeue(out LatestNotice notice))
        {
            if (notice.Kind == NoticeKind.StreamClosed)
            {
                ReleaseCountedStream(notice.Stream);
            }
        }
    }

    /// <summary>Applies the LatestAck and LatestReject entries the transport thread handed over (game thread).</summary>
    private void DrainNotices()
    {
        // The streams first: a stream's start is posted before anything the peer can answer it with, so the ack of a
        // large value is applied after its transmission was counted.
        SpscRing<LatestNotice> streams = _streamNotices;
        while (streams.TryDequeue(out LatestNotice stream))
        {
            if (stream.Kind == NoticeKind.StreamClosed)
            {
                // A large-value stream of ours shut down, so its per-channel slot is free again (the receiver freed its own
                // at the same point) and a value waiting for credit may go out. The slot is looked up by the stream's own id,
                // so a start that was reported and then refused — one this engine never counted — releases nothing.
                ReleaseCountedStream(stream.Stream);
            }
            else if (stream.Kind == NoticeKind.StreamRefused)
            {
                OnLargeStreamRefused(stream.Local, stream.Stream);
            }
            else
            {
                OnLargeStreamStarted(stream.Local, stream.Stream);
            }
        }

        SpscRing<LatestNotice> ring = _notices;
        while (ring.TryDequeue(out LatestNotice notice))
        {
            int local = notice.Local;
            LatestSendKeys keys = _sendKeys[local];
            if (!keys.TryGet(notice.Key, out int keySlot))
            {
                continue;
            }

            ref KeySendSlot key = ref keys[keySlot];
            if (notice.Kind == NoticeKind.Reject)
            {
                // PROTOCOL.md §2.3: the receiver dropped this version locally; back off, then retry.
                if (key.InFlightEntry >= 0 && notice.Version == key.CurrentVersion)
                {
                    ArmRetry(local, ref _send[local], ref key, _core.CurrentPassMicros);
                }

                continue;
            }

            // PROTOCOL.md §4.3, §4.4: an ack is evidence only for the version this host last put on the wire for the key. The
            // receiver acks the highest version it accepted for the key, which is never above that one; an ack that names
            // anything else is of a superseded value, of a closed epoch (the counter restarted and nothing of the new epoch
            // has been transmitted: sent is 0), or a late duplicate of an earlier value's ack. None of those says the
            // current value arrived. The test is equality on purpose: no serial arithmetic, so no distance between two
            // versions of one key — a key may idle for any number of the channel's versions — can make an old ack look new.
            uint sent = keys.SentVersion(keySlot);
            if (sent == 0 || notice.Version != sent)
            {
                continue;
            }

            key.AckedVersion = sent;
            int value = key.InFlightEntry;
            if (value < 0 || sent != key.CurrentVersion)
            {
                // Already finished — or a newer value was admitted and has not been transmitted yet, and this is the ack of
                // the value it replaced.
                continue;
            }

            // The ack names the current version, and that version was transmitted: the value is Delivered.
            key.InFlightEntry = -1;
            key.Flags &= ~(KeySendFlags.RetryArmed | KeySendFlags.Pending);
            keys.Disarm(keySlot);
            _send[local].LiveKeys--;
            AbortLargeStream(ref key);
            MarkValueFinished(local, value, DeliveryStatus.Delivered, superseded: false);
        }
    }

    // ------------------------------------------------------------------ acks this end owes (game thread)

    /// <summary>
    /// Whether the transport thread left work only a scheduler pass consumes: acks or rejects this end owes, or LatestAck /
    /// LatestReject / stream notices to apply. Every one of them raised the work signal when it was published, and this is
    /// the level behind that edge: <see cref="QuiclyPeer.HasPendingWork"/> reports it and the flush gate
    /// (<see cref="QuiclyPeer.CanSkipFlush"/>) flushes for it. Game thread; from another thread the answer is advisory (the
    /// held entries and the sweep cursor are the game thread's own).
    /// </summary>
    internal bool HasUnsentControl => !_notices.IsEmpty || !_streamNotices.IsEmpty || HasPendingAcks();

    private bool HasPendingAcks()
    {
        if (_heldAck.Local >= 0 || _heldReject.Channel != 0 || !_ackQueue.IsEmpty || !_rejectQueue.IsEmpty || _sweepLocal >= 0)
        {
            return true;
        }

        for (int local = 0; local < _ackSweep.Length; local++)
        {
            if (Volatile.Read(ref _ackSweep[local]) != 0)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Sends what this end owes as at most one coalesced transmission per <see cref="PeerOptions.AckDelay"/> (PROTOCOL.md
    /// §2.3): LatestAck first, then LatestReject, as high-priority control datagrams, de-duplicated per key by construction
    /// (one pending version per key, the highest). A batch that does not fit one datagram continues in the next one, at most
    /// <see cref="MaxAckDatagramsPerPass"/> per transmission; when no datagram can be sent the batch goes on the control
    /// stream (§3.4).
    /// </summary>
    private void SendPendingAcks(ref FlushContext flush)
    {
        long now = flush.NowMicros;
        if (now < _nextAckMicros || !HasPendingAcks())
        {
            return;
        }

        // The carrier is chosen before the batch is written, because the two carriers frame a control message differently: a
        // datagram the transport refused makes the next transmission take the control stream instead (PROTOCOL.md §2.3).
        bool datagrams = _core.DatagramsEnabled && !_ackDatagramsRefused;
        ControlCarrier carrier = datagrams ? ControlCarrier.Datagram : ControlCarrier.Stream;
        int limit = datagrams ? Math.Min(_core.MaxDatagramPayload, AckFrameBytes) : AckFrameBytes;
        if (limit <= 8)
        {
            _acksStuck = true;
            return;
        }

        Span<byte> buffer = stackalloc byte[AckFrameBytes];
        bool transmitted = false;
        for (int frame = 0; frame < MaxAckDatagramsPerPass; frame++)
        {
            LatestAckBatchWriter writer = new(buffer.Slice(0, limit), carrier);
            int added = 0;
            while (TryTakeAck(out int local, out ulong key, out uint version))
            {
                LatestAckEntry entry = new(_channels[local].Id, key, version);
                if (!writer.TryAdd(in entry))
                {
                    _heldAck = new AckRequest { Local = local, KeySlot = -1, Key = key, Version = version };
                    break;
                }

                added++;
            }

            if (added == 0)
            {
                break;
            }

            Send(buffer, writer.Finish(), carrier);
            transmitted = true;
        }

        for (int frame = 0; frame < MaxAckDatagramsPerPass; frame++)
        {
            LatestRejectBatchWriter writer = new(buffer.Slice(0, limit), carrier);
            int added = 0;
            while (TryTakeReject(out RejectRequest request))
            {
                LatestRejectEntry entry = new(request.Channel, request.Key, request.Version, request.Reason);
                if (!writer.TryAdd(in entry))
                {
                    _heldReject = request;
                    break;
                }

                added++;
            }

            if (added == 0)
            {
                break;
            }

            Send(buffer, writer.Finish(), carrier);
            transmitted = true;
        }

        if (transmitted)
        {
            // The AckDelay window starts when a transmission really went out. Advancing it for a pass that could send
            // nothing (no carrier, a datagram limit too small for any batch) would skip that window's acks silently, and
            // with AckDelay 0 it would also pin the flush deadline at the current time (LowerDeadline).
            _nextAckMicros = now + _ackDelayMicros;
            _ackOwedSince = 0;
        }

        _acksStuck = !transmitted && HasPendingAcks();
    }

    /// <summary>
    /// Lowers <paramref name="deadline"/> to the latest time the acks and rejects this end owes may go out (game thread, the
    /// end of a Poll; a pass computes its deadline from what it saw, and acks a transport callback queued since are not in
    /// it): <see cref="PeerOptions.AckDelay"/> after the first Poll that saw them owed — the longest delay the option allows —
    /// and not before the current coalescing window ends. Any Flush before then (the host's own tick) sends them with its
    /// other traffic, so the deadline only matters to a host whose next flush is further away. Acks the last due pass could
    /// not send at all are left out: that pass published their time, or none (<see cref="LowerDeadline"/>), and bringing the
    /// flush forward for them would make a host that sleeps until the deadline spin on a Flush that cannot send them.
    /// LatestAck / LatestReject notices do not move the deadline: the pass that would retransmit a value applies them first,
    /// so they wait for the host's next flush or the retry timer, whichever is first.
    /// </summary>
    /// <param name="now">Clock micros.</param>
    /// <param name="deadline">The flush deadline to lower.</param>
    internal void LowerControlDeadline(long now, ref long deadline)
    {
        if (_acksStuck || !HasPendingAcks())
        {
            return;
        }

        if (_ackOwedSince == 0)
        {
            _ackOwedSince = now;
        }

        long due = Math.Max(_nextAckMicros, _ackOwedSince + _ackDelayMicros);
        if (due < deadline)
        {
            deadline = due;
        }
    }

    private void Send(Span<byte> buffer, int length, ControlCarrier carrier)
    {
        if (length <= 0)
        {
            return;
        }

        if (_core.SendControlFrame(buffer.Slice(0, length), carrier))
        {
            _ackDatagramsRefused = false;
            return;
        }

        // The batch is lost: the sender's timer retransmits the value and the duplicate is re-acked (PROTOCOL.md §4.4), and
        // the next coalesced transmission goes on the control stream.
        _ackDatagramsRefused = carrier == ControlCarrier.Datagram;
    }

    /// <summary>Takes the next key whose ack is due: the held one, then the ring, then a sweep after a ring overflow.</summary>
    private bool TryTakeAck(out int local, out ulong key, out uint version)
    {
        if (_heldAck.Local >= 0)
        {
            local = _heldAck.Local;
            key = _heldAck.Key;
            version = _heldAck.Version;
            _heldAck.Local = -1;
            return true;
        }

        while (_ackQueue.TryDequeue(out AckRequest request))
        {
            LatestRecvKeys keys = _recvKeys[request.Local];
            uint pending = Interlocked.Exchange(ref keys.VersionRef(request.KeySlot), 0);
            if (pending == 0)
            {
                continue; // already acked through a sweep or an earlier entry for the same key
            }

            local = request.Local;
            key = keys[request.KeySlot].Key;
            version = pending;
            return true;
        }

        return TrySweepAck(out local, out key, out version);
    }

    /// <summary>After the ack ring overflowed, walks the channel's receive slots for versions still waiting.</summary>
    private bool TrySweepAck(out int local, out ulong key, out uint version)
    {
        while (true)
        {
            if (_sweepLocal < 0)
            {
                for (int candidate = 0; candidate < _ackSweep.Length; candidate++)
                {
                    if (Interlocked.Exchange(ref _ackSweep[candidate], 0) != 0)
                    {
                        _sweepLocal = candidate;
                        _sweepSlot = 0;
                        break;
                    }
                }

                if (_sweepLocal < 0)
                {
                    local = -1;
                    key = 0;
                    version = 0;
                    return false;
                }
            }

            LatestRecvKeys keys = _recvKeys[_sweepLocal];
            int used = keys.SlotsUsed;
            while (_sweepSlot < used)
            {
                int slot = _sweepSlot++;
                uint pending = Interlocked.Exchange(ref keys.VersionRef(slot), 0);
                if (pending == 0)
                {
                    continue;
                }

                local = _sweepLocal;
                key = keys[slot].Key;
                version = pending;
                return true;
            }

            _sweepLocal = -1;
        }
    }

    private bool TryTakeReject(out RejectRequest request)
    {
        if (_heldReject.Channel != 0)
        {
            request = _heldReject;
            _heldReject = default;
            return true;
        }

        return _rejectQueue.TryDequeue(out request);
    }

    /// <summary>One key whose ack is due (transport thread → game thread; the version lives in the key's slot).</summary>
    private struct AckRequest
    {
        public int Local;
        public int KeySlot;
        public ulong Key;
        public uint Version;
    }

    /// <summary>One version the receive side dropped locally (transport thread → game thread).</summary>
    private struct RejectRequest
    {
        public ushort Channel;
        public ulong Key;
        public uint Version;
        public LatestRejectReason Reason;
    }

    /// <summary>One LatestAck or LatestReject entry the peer sent (transport thread → game thread).</summary>
    private struct LatestNotice
    {
        public int Local;
        public ulong Key;
        public uint Version;
        public NoticeKind Kind;
        public LatestRejectReason Reason;

        /// <summary><see cref="NoticeKind.StreamClosed"/>: the stream that shut down (the game thread resolves its slot).</summary>
        public TransportStreamId Stream;
    }

    /// <summary>Staging state of one peer group stream carrying a large value (transport thread).</summary>
    [StructLayout(LayoutKind.Sequential, Size = 64)]
    private struct LatestRecvStream
    {
        public TransportStreamId Stream;
        public BufferLease Lease;
        public ulong Key;
        public uint Version;
        public int Length;
        public int Filled;
        public int RawLength;
        public int Local;
        public int KeySlot;
        public byte InUse;
        public byte Drop;
    }

    /// <summary>
    /// Per-key receive state of one channel (transport thread, except the pending-ack version word and the slot high-water
    /// mark, which the game thread reads to send the coalesced acks): the channel's key table and a
    /// <see cref="KeyRecvSlot"/> per key, growing with the slots in use up to <see cref="ChannelDefinition.MaxKeys"/>. It
    /// also holds the channel's version clock (<see cref="Newest"/>), which lives and dies with the keys:
    /// <see cref="Clear"/> is the one place both are forgotten.
    /// </summary>
    private sealed class LatestRecvKeys : IDisposable
    {
        private readonly IKeyTable _table;
        private readonly int _maxKeys;
        private NativeArray<KeyRecvSlot> _slots;
        private int _used;

        /// <summary>
        /// The newest version seen on the channel this epoch, for any key, extended to 64 bits and biased by 2^32 so that
        /// 0 means "nothing seen yet" (<see cref="ExtendVersion"/>). Transport thread.
        /// </summary>
        public ulong Newest;

        public LatestRecvKeys(ChannelDefinition channel)
        {
            _maxKeys = channel.MaxKeys;
            _table = PeerCore.CreateKeyTable(channel);
            _slots = new NativeArray<KeyRecvSlot>(Math.Min(_maxKeys, InitialKeySlots));
        }

        /// <summary>One past the highest slot ever handed out (game thread reads it for the ack sweep).</summary>
        public int SlotsUsed => Volatile.Read(ref _used);

        public ref KeyRecvSlot this[int slot] => ref _slots[slot];

        /// <summary>The key's pending-ack version word, used as a one-slot mailbox with <see cref="Interlocked.Exchange(ref uint, uint)"/>.</summary>
        /// <param name="slot">The key slot.</param>
        public ref uint VersionRef(int slot) => ref _slots[slot].PendingAckVersion;

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
            _slots[slot].Key = key;
            if (slot >= _used)
            {
                Volatile.Write(ref _used, slot + 1);
            }

            return true;
        }

        public void Remove(ulong key) => _table.Remove(key, out _);

        /// <summary>Forgets every key and the version clock they were measured on (a new epoch restarts the sender's counter).</summary>
        public void Clear()
        {
            _table.Clear();
            Newest = 0;
        }

        public void Dispose()
        {
            (_table as IDisposable)?.Dispose();
            _slots.Dispose();
        }

        private void EnsureSlot(int slot)
        {
            if (slot < _slots.Length)
            {
                return;
            }

            int length = (int)Math.Min(_maxKeys, Math.Max(slot + 1L, _slots.Length * 2L));
            NativeArray<KeyRecvSlot> grown = new(length);
            _slots.AsSpan().CopyTo(grown.AsSpan());
            NativeArray<KeyRecvSlot> old = _slots;
            _slots = grown;
            old.Dispose();
        }
    }
}
