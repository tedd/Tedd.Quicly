using System.Diagnostics;
using System.Runtime.InteropServices;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Primitives;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Threading;

namespace Tedd.Quicly.Core.Session.Engines;

// Fragmentation of the unreliable modes (PROTOCOL.md §2.1, §7; docs/design/session-layer.md §7.8): a message that does not
// fit one datagram is split into at most 8 fragments on the send side (game thread) and reassembled per channel on the
// receive side (transport thread).
internal abstract unsafe partial class DatagramEngine
{
    /// <summary>Most fragments one message may be split into (PROTOCOL.md §2.1).</summary>
    private const int MaxFragments = DatagramFraming.MaxFragments;

    /// <summary>
    /// Fragment scratch bit in <c>Aux1</c>, next to the base phases (<c>InQueue</c>, <c>HandedToPacker</c>,
    /// <c>Finished</c>): the fragment's payload reference on the message has been given back.
    /// </summary>
    private const long FragmentPayloadReleased = 4;

    /// <summary>Reassembly expiry when no RTT sample exists yet: 2 × 0 + 100 ms (PROTOCOL.md §7).</summary>
    private const long ReassemblyGraceMicros = 100_000;

    /// <summary><see cref="Reassembly.Flags"/>: the record holds a partial message.</summary>
    private const byte PartialInUse = 1;

    /// <summary><see cref="Reassembly.Flags"/>: the last fragment's bytes wait at offset 0 until fragment 0's size is known.</summary>
    private const byte PartialLastAtFront = 2;

    // By send-entry slot (game thread): -1 = an ordinary message, the slot itself = the owner entry of a fragmented
    // message, anything else = a fragment of the owner it names. Only allocated when a channel of this engine fragments,
    // and written by every allocation this engine makes, so a recycled slot never inherits a stale owner.
    private int[] _fragmentOwner = [];

    // By engine-local channel index: the channel's range in _partials (-1 when the channel does not fragment).
    private int[] _partialBase = [];
    private int[] _partialCap = [];
    private NativeArray<Reassembly>? _partials;
    private int _maxReassembleBytes;

    // Written by the game thread in Tick, read by the transport thread in OnFragment: 2 × RTT + 100 ms (PROTOCOL.md §7).
    private long _reassemblyWindowMicros = ReassemblyGraceMicros;

    /// <summary>Whether any channel of this engine fragments (else every fragmentation path is skipped).</summary>
    private bool Fragments => _partials is not null;

    /// <summary>
    /// Partial reassemblies a channel holds right now (diagnostics only: statistics and tests). The field belongs to the
    /// transport thread (ADR 0008 invariant 4), so this is a single volatile read of one counter — the same shape as the
    /// receive-side counters <see cref="QuiclyPeer.GetStatistics"/> snapshots. It may be one fragment out of date, and nothing
    /// in the engine decides anything from it.
    /// </summary>
    /// <param name="channelIndex">Dense index of a channel of this engine.</param>
    internal int Reassemblies(int channelIndex) => Volatile.Read(ref _recv[_localOf[channelIndex]].Reassemblies);

    /// <summary>The reassembly expiry window in micros as the transport thread sees it (tests).</summary>
    internal long ReassemblyWindowMicros => Volatile.Read(ref _reassemblyWindowMicros);

    /// <summary>Sets up the fragmentation state of the channels that fragment (constructor time, game thread).</summary>
    /// <param name="core">The peer's shared state.</param>
    /// <param name="channelsOfMode">The channels of this engine.</param>
    private void InitializeFragmentation(PeerCore core, ReadOnlySpan<ChannelDefinition> channelsOfMode)
    {
        int count = channelsOfMode.Length;
        _partialBase = new int[count];
        _partialCap = new int[count];
        int records = 0;
        for (int local = 0; local < count; local++)
        {
            ChannelDefinition channel = channelsOfMode[local];
            if (!channel.Fragmentation)
            {
                _partialBase[local] = -1;
                continue;
            }

            _partialBase[local] = records;
            _partialCap[local] = channel.MaxReassemblies;
            records += channel.MaxReassemblies;
        }

        if (records == 0)
        {
            return;
        }

        _partials = new NativeArray<Reassembly>(records);
        // A reassembled message is one pooled block, so the engine can never accept more than the pool's largest block or
        // the peer's whole receive budget (the §7.2 sizing rule, applied to reassembly).
        _maxReassembleBytes = (int)Math.Min(core.ReceiveBudgetBytes, core.Allocator.MaxBlockSize);
        _fragmentOwner = new int[core.Entries.Capacity];
        Array.Fill(_fragmentOwner, -1);
    }

    /// <summary>Marks a send entry as an ordinary message (game thread, at every allocation of a non-fragment entry).</summary>
    /// <param name="slot">The entry.</param>
    private void NoteOrdinaryEntry(int slot)
    {
        if (_partials is not null)
        {
            _fragmentOwner[slot] = -1;
        }
    }

    /// <summary>Refreshes the reassembly window from the application RTT (game thread, once per flush).</summary>
    /// <param name="nowMicros">Clock micros of this flush (unused; the window is time-independent).</param>
    private void RefreshReassemblyWindow(long nowMicros)
    {
        _ = nowMicros;
        long window = (2 * _core.ApplicationRttMicros) + ReassemblyGraceMicros;
        if (window != Volatile.Read(ref _reassemblyWindowMicros))
        {
            Volatile.Write(ref _reassemblyWindowMicros, window);
        }
    }

    // ------------------------------------------------------------------ send (game thread)

    /// <summary>
    /// A message of a fragmenting channel that does not fit one datagram (PROTOCOL.md §2.1): it goes out as
    /// <c>FragCount</c> ≤ 8 fragments of one <c>(channel, key, sequence)</c>, all of the size of fragment 0 except the last.
    /// Called by <see cref="Admit"/> once its single-datagram attempt gave up its entry; the payload it prepared is handed
    /// over here rather than prepared a second time.
    /// </summary>
    /// <remarks>
    /// The message keeps <em>one</em> owner entry: it holds the payload (a copy, the caller's own lease, pinned or shared
    /// memory, or the LZ4 block — whatever <see cref="EnginePayload"/> prepared), the tracking token, the admission stamp and
    /// the message's outstanding counts. Each fragment is an untracked entry of its own whose payload segment points into the
    /// owner's bytes, so no fragment copies anything (ADR 0008 invariant 1). The owner is never submitted; it completes when
    /// every fragment has, with the worst of their outcomes, and its payload is released once every fragment's payload
    /// reference is gone — which is what completes the message's BufferReleased stage.
    /// <para><b>Delivery probability.</b> A fragmented unreliable message arrives only if every fragment does:
    /// (1 − loss)^FragCount, so at 2 % loss an 8-fragment message is lost 15 % of the time against 2 % for a single
    /// datagram. That is why <c>FragCount</c> is capped at 8 (a channel may not carry more than 8 × 1 100 raw bytes) and why
    /// anything larger belongs on a reliable channel.</para>
    /// </remarks>
    /// <param name="request">The request.</param>
    /// <param name="length">Raw payload bytes of the message (a gather's total).</param>
    /// <param name="payload">
    /// The payload <see cref="Admit"/> prepared for its single-datagram attempt — the very bytes the fragments point into.
    /// This method owns it: it releases it when the message is refused, and commits it to the owner entry otherwise.
    /// </param>
    /// <returns>The admission result.</returns>
    private SendStatus AdmitFragmented(ref SendRequest request, int length, ref PreparedPayload payload)
    {
        ChannelDefinition channel = request.Channel;
        int dense = request.ChannelIndex;
        int local = _localOf[dense];
        ref ChannelSendCounters counters = ref _core.SendCounters(dense);
        SendEntryTable entries = _core.Entries;

        // One owner entry plus at most eight fragments; the exact count is only known once the payload is prepared, so the
        // reserve is checked for the worst case (a fragmented message is rare and never urgent).
        if (entries.Available <= MaxFragments + 1)
        {
            EnginePayload.Release(_core, in payload);
            counters.QueueFull++;
            return SendStatus.QueueFull;
        }

        MessageHeader header = default;
        header.Key = channel.Keyed ? request.Key : 0;
        header.FragCount = 2; // > 1, so the header carries the FragIndex byte; its size does not depend on the count
        header.RawLength = payload.RawLength;
        int headerLength = DatagramFraming.GetHeaderLength(channel, in header);
        int capacity = _core.MaxDatagramPayload - headerLength;
        int wire = payload.Length;
        // Fragments of one message are equal except the last (PROTOCOL.md §2.1), so the count follows from the capacity and
        // the size from the count. The caller only gets here when the message does not fit one datagram, so count ≥ 2.
        int count = capacity > 0 ? (wire + capacity - 1) / capacity : int.MaxValue;
        if (count > MaxFragments)
        {
            // PROTOCOL.md §7 "fragmented message size": more than 8 fragments can never be sent on this channel.
            EnginePayload.Release(_core, in payload);
            counters.TooLarge++;
            return SendStatus.TooLarge;
        }

        int size = (wire + count - 1) / count;
        int lastSize = wire - (size * (count - 1));
        Debug.Assert(count >= 2 && size <= capacity && lastSize >= 1 && lastSize <= size, "fragment layout");

        // The reserve above was checked against the worst case and nothing between it and here takes an entry, so every one
        // of these allocations succeeds.
        int* slots = stackalloc int[MaxFragments + 1];
        for (int i = 0; i <= count; i++)
        {
            bool reserved = _core.TryAllocateEntry(channel.Id, SendEntryFlags.None, out slots[i]);
            Debug.Assert(reserved, "the entry reserve of a fragmented message");
            // Every allocation this engine makes writes the owner map at once, so a recycled slot never inherits a stale
            // owner and the unwind below leaves none behind either (docs/design/session-layer.md §7.8).
            _fragmentOwner[slots[i]] = -1;
        }

        int owner = slots[0];
        if (request.Options.Track && !_core.TryTrack(owner, request.Options.Context, out request.Token))
        {
            for (int i = 0; i <= count; i++)
            {
                _core.DiscardEntry(slots[i]);
            }

            EnginePayload.Release(_core, in payload);
            counters.QueueFull++;
            return SendStatus.QueueFull;
        }

        // Commit: nothing below fails.
        ref ChannelSendState send = ref _send[local];
        uint sequence = send.NextSequence;
        send.NextSequence = channel.SequenceBits == 16 ? (ushort)(sequence + 1) : sequence + 1;
        header.FragCount = (byte)count;
        header.Sequence = sequence;
        EnginePayload.Commit(_core, owner, ref request, in payload);
        _core.StampAdmission(owner);
        entries.Keys[owner] = header.Key;
        entries.Sequences[owner] = sequence;
        long expiry = request.Options.ExpiryMicros > 0 ? request.Options.ExpiryMicros : _expiryMicros[local];
        long deadline = 0;
        if (expiry > 0)
        {
            // The pass's clock stamp, not a QPC per message (ADR 0008 invariant 9).
            long now = _core.CurrentPassMicros;
            deadline = expiry >= long.MaxValue - now ? long.MaxValue : now + expiry;
        }

        ref SendEntry ownerEntry = ref entries[owner];
        ownerEntry.Aux0 = length;
        ownerEntry.Aux1 = PackOwner(count, count, DeliveryStatus.Pending);
        _fragmentOwner[owner] = owner;
        byte* bytes = payload.Pointer;
        NativeArray<int> links = entries.Next;
        for (int index = 0; index < count; index++)
        {
            int slot = slots[index + 1];
            int fragmentSize = index == count - 1 ? lastSize : size;
            header.FragIndex = (byte)index;
            entries.SetHeaderLength(slot, DatagramFraming.WriteHeader(entries.GetHeaderBlock(slot), channel, in header));
            _core.SetPayload(slot, bytes + (index * size), fragmentSize);
            entries.Keys[slot] = header.Key;
            entries.Sequences[slot] = sequence;
            entries.Deadlines[slot] = deadline;
            _fragmentOwner[slot] = owner;
            ref SendEntry entry = ref entries[slot];
            entry.Aux0 = fragmentSize;
            entry.Aux1 = InQueue;
            if (request.Options.Mode == SendMode.Immediate)
            {
                entry.Flags |= SendEntryFlags.Immediate;
            }

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
            send.QueueBytes += fragmentSize;
        }

        PeerCounters peer = _core.Counters;
        peer.FragmentedMessagesSent++;
        peer.FragmentsSent += count;
        return SendStatus.Admitted;
    }

    /// <summary>
    /// Routes the completion of a fragment or of a fragmented message's owner entry (game thread). A fragment's payload
    /// notice gives back one reference on the message's payload; its final completion folds its outcome into the owner and
    /// finishes the message when it was the last one.
    /// </summary>
    /// <param name="entrySlot">The entry.</param>
    /// <param name="completion">What the transport (or the game thread) reported.</param>
    /// <returns><see langword="false"/> when the entry is an ordinary message (the caller handles it).</returns>
    private bool TryCompleteFragment(int entrySlot, in CompletionEntry completion)
    {
        if (_partials is null)
        {
            return false;
        }

        int owner = _fragmentOwner[entrySlot];
        if (owner < 0 || owner == entrySlot)
        {
            // An ordinary message, or the owner itself: an owner is never submitted, so no completion names it.
            return false;
        }

        SendEntryTable entries = _core.Entries;
        ref SendEntry fragment = ref entries[entrySlot];
        if (!completion.Final)
        {
            // DatagramSendState.Sent: the transport no longer reads this fragment's bytes.
            if ((fragment.Aux1 & FragmentPayloadReleased) == 0)
            {
                fragment.Aux1 |= FragmentPayloadReleased;
                ReleaseOwnerPayload(owner);
            }

            return true;
        }

        if (completion.Kind == CompletionKind.Local && (fragment.Aux1 & ~FragmentPayloadReleased) == HandedToPacker)
        {
            // The packer took the fragment but the transport refused its datagram: it was never sent.
            ref ChannelSendCounters counters = ref _core.SendCounters(_core.ChannelIndexOf(fragment.Channel));
            counters.Sent--;
            counters.Bytes -= fragment.Payload.Length;
        }

        if ((fragment.Aux1 & FragmentPayloadReleased) == 0)
        {
            // No Sent notice reached this fragment: it travelled in a packed container none of whose members was tracked (a
            // container forwards its notice to every member only when one of them is — a fragment never is itself), or it
            // never reached the transport at all. Its payload reference goes back here; the bit makes it exactly once either way.
            fragment.Aux1 |= FragmentPayloadReleased;
            ReleaseOwnerPayload(owner);
        }

        DeliveryStatus status = _core.MapCompletion(in completion);
        _fragmentOwner[entrySlot] = -1;
        _core.CompleteEntry(entrySlot, status);
        FoldFragmentStatus(owner, status);
        return true;
    }

    /// <summary>Gives back one reference on a message's payload and releases it with the last one (game thread).</summary>
    /// <param name="owner">The owner entry.</param>
    private void ReleaseOwnerPayload(int owner)
    {
        ref SendEntry entry = ref _core.Entries[owner];
        UnpackOwner(entry.Aux1, out int outstanding, out int payloadRefs, out DeliveryStatus status);
        payloadRefs--;
        entry.Aux1 = PackOwner(outstanding, payloadRefs, status);
        if (payloadRefs == 0)
        {
            // Pinned, borrowed, shared and gathered payloads are promised only until BufferReleased (ARCHITECTURE.md §4.1):
            // the promise ends when no fragment can be read by the transport any more.
            _core.ReleasePayload(owner);
            _core.CompleteStage(owner, CompletionStage.BufferReleased, DeliveryStatus.Pending);
        }
    }

    /// <summary>Folds one fragment's outcome into its message and completes the message with the last fragment (game thread).</summary>
    /// <param name="owner">The owner entry.</param>
    /// <param name="status">The fragment's delivery status.</param>
    private void FoldFragmentStatus(int owner, DeliveryStatus status)
    {
        ref SendEntry entry = ref _core.Entries[owner];
        UnpackOwner(entry.Aux1, out int outstanding, out int payloadRefs, out DeliveryStatus folded);
        if (Rank(status) > Rank(folded))
        {
            folded = status;
        }

        outstanding--;
        entry.Aux1 = PackOwner(outstanding, payloadRefs, folded);
        if (outstanding > 0)
        {
            return;
        }

        // A fragmented message is delivered only when every one of its fragments was (PROTOCOL.md §2.1).
        _fragmentOwner[owner] = -1;
        _core.CompleteEntry(owner, folded);
    }

    /// <summary>
    /// Cancels a fragmented message while every one of its fragments is still queued (game thread). The fragments leave the
    /// channel's FIFO and complete <see cref="DeliveryStatus.Canceled"/> at the next Poll or Flush, which completes the
    /// message.
    /// </summary>
    /// <param name="ownerSlot">The owner entry named by the caller's token.</param>
    /// <returns><see langword="true"/> when the message will complete canceled.</returns>
    private bool TryCancelFragmented(int ownerSlot)
    {
        SendEntryTable entries = _core.Entries;
        int dense = _core.ChannelIndexOf(entries[ownerSlot].Channel);
        if (dense < 0 || _localOf[dense] < 0)
        {
            return false;
        }

        ref ChannelSendState send = ref _send[_localOf[dense]];
        NativeArray<int> links = entries.Next;
        // Every fragment must still be queued: a message the packer has already taken in part cannot be recalled.
        int queued = 0;
        for (int slot = send.QueueHead; slot >= 0; slot = links[slot])
        {
            if (_fragmentOwner[slot] == ownerSlot)
            {
                if (entries[slot].Aux1 != InQueue)
                {
                    return false;
                }

                queued++;
            }
        }

        UnpackOwner(entries[ownerSlot].Aux1, out int outstanding, out _, out _);
        if (queued != outstanding)
        {
            return false;
        }

        int previous = -1;
        int next;
        for (int slot = send.QueueHead; slot >= 0; slot = next)
        {
            next = links[slot];
            if (_fragmentOwner[slot] != ownerSlot)
            {
                previous = slot;
                continue;
            }

            if (previous < 0)
            {
                send.QueueHead = next;
            }
            else
            {
                links[previous] = next;
            }

            if (send.QueueTail == slot)
            {
                send.QueueTail = previous;
            }

            ref SendEntry entry = ref entries[slot];
            send.QueueCount--;
            send.QueueBytes -= entry.Aux0;
            entry.Aux1 = Finished;
            _core.QueueLocalCompletion(slot, DeliveryStatus.Canceled);
        }

        return true;
    }

    /// <summary>
    /// Completes every fragmented message still allocated (game thread): the session closed or the connection was lost, and
    /// the fragments of those messages have already been finished by the caller's own sweep.
    /// </summary>
    /// <param name="status">How the messages end.</param>
    private void AbandonFragmentedMessages(DeliveryStatus status)
    {
        if (_partials is null)
        {
            return;
        }

        SendEntryTable entries = _core.Entries;
        for (int slot = 0; slot < _fragmentOwner.Length; slot++)
        {
            if (_fragmentOwner[slot] != slot || entries.GetState(slot) == SendEntryState.Free)
            {
                continue;
            }

            _fragmentOwner[slot] = -1;
            _core.CompleteEntry(slot, status);
        }
    }

    /// <summary>Forgets every fragment and message bookkeeping of a lost connection (game thread, <see cref="OnReconnecting"/>).</summary>
    private void ResetFragmentOwners()
    {
        if (_partials is not null)
        {
            Array.Fill(_fragmentOwner, -1);
        }
    }

    private static long PackOwner(int outstanding, int payloadRefs, DeliveryStatus status) =>
        (uint)outstanding | ((long)(uint)payloadRefs << 8) | ((long)(byte)status << 16);

    private static void UnpackOwner(long aux, out int outstanding, out int payloadRefs, out DeliveryStatus status)
    {
        outstanding = (int)(aux & 0xFF);
        payloadRefs = (int)((aux >> 8) & 0xFF);
        status = (DeliveryStatus)(byte)((aux >> 16) & 0xFF);
    }

    /// <summary>How bad an outcome is: the worst of a message's fragments decides the message (see <see cref="AdmitFragmented"/>).</summary>
    private static int Rank(DeliveryStatus status) => status switch
    {
        DeliveryStatus.Pending => 0,
        DeliveryStatus.Delivered => 1,
        DeliveryStatus.Sent => 2,
        DeliveryStatus.Canceled => 3,
        DeliveryStatus.Expired => 4,
        DeliveryStatus.Lost => 5,
        DeliveryStatus.Failed => 6,
        _ => 7,
    };

    // ------------------------------------------------------------------ receive (transport thread)

    /// <summary>
    /// One fragment of a larger message (PROTOCOL.md §2.1, §7): every limit is checked from this single fragment before a
    /// buffer is chosen, then the fragment is copied into the partial message of its <c>(channel, key, sequence)</c> — the
    /// reassembly scope of PROTOCOL.md §2.1 — and the message is published when the last missing fragment arrives. Transport
    /// thread.
    /// </summary>
    /// <remarks>
    /// The framing layer has already checked the fragment fields (<c>FragCount</c> ∈ [1, 8], <c>FragIndex</c> &lt;
    /// <c>FragCount</c>, a non-empty payload, and the total the fragment implies against the channel's effective
    /// <c>MaxMessageSize</c>). This method adds what only the session knows: the same bound against what this peer could ever
    /// buffer, the channel's <see cref="ChannelDefinition.MaxReassemblies"/> cap (the oldest partial is evicted),
    /// the expiry of 2 × RTT + 100 ms, consistency of <c>FragCount</c>, <c>RawLength</c> and the fragment sizes within one
    /// message, the message's <em>real</em> total once both sizes are known, and duplicates (ignored).
    /// <para>Two messages of one channel reassemble side by side: they differ in their sequence, which is part of the scope.
    /// The §7 rule that "a newer sequence for the same key abandons the older partial" applies only where the sequence carries
    /// ordering — an <see cref="ChannelMode.UnreliableSequenced"/> channel, where the older message is obsolete anyway — and a
    /// fragment of an older sequence is dropped there for the same reason. On <see cref="ChannelMode.UnreliableUnordered"/>
    /// the sequence is nothing but a reassembly id (PROTOCOL.md §2.1), so neither rule applies and the channel's cap alone
    /// bounds how many messages it reassembles at once.</para>
    /// </remarks>
    /// <param name="header">The fragment's header.</param>
    /// <param name="payload">The fragment's payload (valid during the call).</param>
    /// <param name="nowMicros">Clock micros of the callback.</param>
    private void OnFragment(in MessageHeader header, ReadOnlySpan<byte> payload, long nowMicros)
    {
        int dense = _core.ChannelIndexOf(header.Channel);
        int local = _localOf[dense];
        ref ChannelRecvCounters counters = ref _core.RecvCounters(dense);
        PeerCounters peer = _core.Counters;
        peer.FragmentsReceived++;
        if (_partials is null || _partialBase[local] < 0)
        {
            // The channel does not fragment: the framing layer accepts FragCount > 1 only on channels that do.
            peer.FragmentsDropped++;
            counters.Dropped++;
            return;
        }

        ChannelDefinition channel = ChannelAt(local);
        int limit = Math.Min(_core.EffectiveMaxMessageSize(channel), _maxReassembleBytes);
        int count = header.FragCount;
        int index = header.FragIndex;
        int size = payload.Length;
        bool last = index == count - 1;
        long bound = last ? (long)size * count : ((long)size * (count - 1)) + 1;
        if (bound > limit)
        {
            // PROTOCOL.md §8 item "§2.1 fragments": the total the fragment implies is bounded before any buffer is chosen.
            peer.FragmentsDropped++;
            counters.TooLarge++;
            return;
        }

        int record = FindPartial(local, channel, in header, nowMicros, ref counters, out SequenceVerdict verdict, out int related);
        if (verdict == SequenceVerdict.Stale)
        {
            // A fragment of a message this key has already moved past; only a channel whose sequence orders its messages can
            // know that.
            peer.FragmentsDropped++;
            return;
        }

        if (verdict == SequenceVerdict.Superseded)
        {
            // PROTOCOL.md §7: on a channel whose sequence carries ordering, a newer sequence for the same key abandons the
            // older partial.
            ReleasePartial(local, ref _partials![related]);
            peer.ReassembliesAbandoned++;
            counters.Dropped++;
        }

        if (record >= 0)
        {
            ref Reassembly existing = ref _partials[record];
            if (existing.FragCount != count || existing.RawLength != header.RawLength
                || (!last && existing.FragmentSize != 0 && size != existing.FragmentSize)
                || (last && existing.LastLength != 0 && size != existing.LastLength))
            {
                // Fragments of one message must agree on FragCount, RawLength and their sizes (PROTOCOL.md §2.1): a set that
                // does not can never be reassembled, so the partial goes with the fragment.
                ReleasePartial(local, ref existing);
                peer.FragmentsDropped++;
                counters.Dropped++;
                return;
            }

            if ((existing.Mask & (byte)(1 << index)) != 0)
            {
                // A duplicate fragment is ignored (PROTOCOL.md §2.1).
                peer.FragmentsDropped++;
                return;
            }
        }
        else
        {
            record = TakeRecord(local, nowMicros, ref counters);
        }

        ref Reassembly partial = ref _partials[record];
        if ((partial.Flags & PartialInUse) == 0)
        {
            // A fresh partial: the buffer is the exact bound when fragment 0's size is known, and the channel's limit when
            // the last fragment arrived first (its own size says nothing about the others). Never more than the limit: a
            // fragment whose own §8 bound passes can still name a count whose product exceeds what the channel promised, and
            // renting that would charge the receive budget — and count OutOfReceiveBuffers — for bytes no message may have.
            int wanted = last ? limit : (int)Math.Min((long)size * count, limit);
            if (!_core.TryRentReceive(wanted, out BufferLease lease))
            {
                _core.Counters.OutOfReceiveBuffers++;
                counters.OutOfBuffers++;
                peer.FragmentsDropped++;
                return;
            }

            partial = default;
            partial.Key = header.Key;
            partial.Sequence = header.Sequence;
            partial.Lease = lease;
            partial.StartedMicros = nowMicros;
            partial.FragCount = (byte)count;
            partial.RawLength = header.RawLength;
            partial.FragmentSize = last ? 0 : size;
            partial.Flags = PartialInUse;
            _recv[local].Reassemblies++;
        }

        byte* buffer = _core.GetPointer(in partial.Lease);
        int available = partial.Lease.Length;
        if (last)
        {
            if (partial.FragmentSize != 0 && (size > partial.FragmentSize || ((long)partial.FragmentSize * (count - 1)) + size > limit))
            {
                // The last fragment is never larger than fragment 0 (PROTOCOL.md §8): a claim that it is would write past
                // the message inside the buffer that was reserved for it. And with both sizes known the message's real total
                // is known too, so it is checked against the channel's limit here (PROTOCOL.md §7, §8): the two per-fragment
                // bounds are each satisfied by sets whose sum is not, and the non-last branch below makes the same check for
                // the other arrival order.
                ReleasePartial(local, ref partial);
                peer.FragmentsDropped++;
                counters.Dropped++;
                return;
            }

            partial.LastLength = size;
            if (partial.FragmentSize == 0)
            {
                // Fragment 0's size is unknown, so the tail has no offset yet: stage it at the front and move it once the
                // size is known (at most one memmove of a fragment, only when the last fragment arrives first).
                payload.CopyTo(new Span<byte>(buffer, available));
                partial.Flags |= PartialLastAtFront;
            }
            else
            {
                payload.CopyTo(new Span<byte>(buffer + (partial.FragmentSize * (count - 1)), available - (partial.FragmentSize * (count - 1))));
            }
        }
        else
        {
            if (partial.FragmentSize == 0)
            {
                long total = ((long)size * (count - 1)) + Math.Max(partial.LastLength, 1);
                if (partial.LastLength > size || total > limit || total > available)
                {
                    // The tail is larger than fragment 0, or the message no longer fits what was reserved for it.
                    ReleasePartial(local, ref partial);
                    peer.FragmentsDropped++;
                    counters.Dropped++;
                    return;
                }

                partial.FragmentSize = size;
                if ((partial.Flags & PartialLastAtFront) != 0)
                {
                    int tail = size * (count - 1);
                    NativeMemory.Copy(buffer, buffer + tail, (nuint)partial.LastLength);
                    partial.Flags &= unchecked((byte)~PartialLastAtFront);
                }
            }

            payload.CopyTo(new Span<byte>(buffer + (index * partial.FragmentSize), available - (index * partial.FragmentSize)));
        }

        partial.Filled += size;
        partial.Mask |= (byte)(1 << index);
        if (partial.Mask != (byte)((1 << count) - 1))
        {
            return;
        }

        int length = (partial.FragmentSize * (count - 1)) + partial.LastLength;
        Debug.Assert(partial.Filled == length, "a complete reassembly holds exactly its message");
        BufferLease message = partial.Lease;
        partial = default;
        _recv[local].Reassemblies--;
        PublishReassembled(local, dense, in header, in message, length, nowMicros, ref counters);
    }

    /// <summary>What the partials a channel already holds for a fragment's key say about the fragment's sequence.</summary>
    private enum SequenceVerdict : byte
    {
        /// <summary>Nothing contradicts the fragment (always the answer on a channel whose sequence carries no ordering).</summary>
        Independent = 0,

        /// <summary>A sequenced channel holds a partial of a newer sequence for the key: the fragment is obsolete.</summary>
        Stale = 1,

        /// <summary>A sequenced channel holds a partial of an older sequence for the key: that partial is abandoned.</summary>
        Superseded = 2,
    }

    /// <summary>
    /// The channel's partial of this fragment's <c>(key, sequence)</c> — the reassembly scope of PROTOCOL.md §2.1 — or -1;
    /// expires the partials whose window has passed on the way, and reports what the key's <em>other</em> partials mean for
    /// this fragment (PROTOCOL.md §7, only on a channel whose sequence carries ordering).
    /// </summary>
    /// <param name="local">The channel's index within this engine.</param>
    /// <param name="channel">The channel.</param>
    /// <param name="header">The fragment's header.</param>
    /// <param name="nowMicros">Clock micros of the callback.</param>
    /// <param name="counters">The channel's receive counters.</param>
    /// <param name="verdict">What the key's other partials say about this sequence.</param>
    /// <param name="related">The partial <paramref name="verdict"/> is about, or -1.</param>
    /// <returns>The record of this message's partial, or -1 when the channel holds none.</returns>
    private int FindPartial(int local, ChannelDefinition channel, in MessageHeader header, long nowMicros, ref ChannelRecvCounters counters,
        out SequenceVerdict verdict, out int related)
    {
        int start = _partialBase[local];
        int end = start + _partialCap[local];
        long window = Volatile.Read(ref _reassemblyWindowMicros);
        // On an unordered channel the sequence is only a reassembly id (PROTOCOL.md §2.1), so a partial of another sequence is
        // another message, never an obsolete version of this one.
        bool ordered = channel.Mode == ChannelMode.UnreliableSequenced;
        bool sixteenBit = channel.SequenceBits == 16;
        int found = -1;
        verdict = SequenceVerdict.Independent;
        related = -1;
        for (int i = start; i < end; i++)
        {
            ref Reassembly partial = ref _partials![i];
            if ((partial.Flags & PartialInUse) == 0)
            {
                continue;
            }

            if (nowMicros - partial.StartedMicros > window)
            {
                // PROTOCOL.md §7: reassembly expiry 2 × RTT + 100 ms. Swept whenever a fragment of the channel arrives, which
                // is the only time the transport thread owns this table.
                ReleasePartial(local, ref partial);
                _core.Counters.ReassembliesExpired++;
                counters.Dropped++;
                continue;
            }

            if (partial.Key != header.Key)
            {
                continue;
            }

            if (partial.Sequence == header.Sequence)
            {
                found = i;
            }
            else if (ordered && verdict != SequenceVerdict.Stale)
            {
                bool newer = IsNewerSequence(header.Sequence, partial.Sequence, sixteenBit);
                verdict = newer ? SequenceVerdict.Superseded : SequenceVerdict.Stale;
                related = i;
            }
        }

        return found;
    }

    /// <summary>RFC 1982 serial comparison in the channel's sequence width (PROTOCOL.md §1).</summary>
    private static bool IsNewerSequence(uint sequence, uint other, bool sixteenBit) =>
        sixteenBit ? SerialNumber.IsNewer((ushort)sequence, (ushort)other) : SerialNumber.IsNewer(sequence, other);

    /// <summary>A free record of the channel, evicting its oldest partial when the cap is reached (PROTOCOL.md §7).</summary>
    private int TakeRecord(int local, long nowMicros, ref ChannelRecvCounters counters)
    {
        int start = _partialBase[local];
        int end = start + _partialCap[local];
        int oldest = start;
        long oldestStamp = long.MaxValue;
        for (int i = start; i < end; i++)
        {
            ref Reassembly partial = ref _partials![i];
            if ((partial.Flags & PartialInUse) == 0)
            {
                return i;
            }

            if (partial.StartedMicros < oldestStamp)
            {
                oldestStamp = partial.StartedMicros;
                oldest = i;
            }
        }

        ref Reassembly victim = ref _partials![oldest];
        ReleasePartial(local, ref victim);
        _core.Counters.ReassembliesAbandoned++;
        counters.Dropped++;
        _ = nowMicros;
        return oldest;
    }

    /// <summary>Gives back a partial's buffer and frees its record (transport thread).</summary>
    private void ReleasePartial(int local, ref Reassembly partial)
    {
        _core.ReturnReceive(in partial.Lease);
        partial = default;
        _recv[local].Reassemblies--;
    }

    /// <summary>
    /// Delivers a reassembled message: the mode decides acceptance (a stale sequence is dropped here, as for any datagram),
    /// then the partial's own buffer becomes the message's payload — no copy — in the key's mailbox or the receive ring.
    /// </summary>
    private void PublishReassembled(int local, int dense, in MessageHeader header, in BufferLease lease, int length, long nowMicros, ref ChannelRecvCounters counters)
    {
        if (!Accept(local, in header, out int keySlot, ref counters))
        {
            _core.ReturnReceive(in lease);
            return;
        }

        ReceiveEntry entry = default;
        entry.Channel = header.Channel;
        entry.Flags = header.Compressed ? ReceiveFlags.Fragmented | ReceiveFlags.Compressed : ReceiveFlags.Fragmented;
        entry.Sequence = header.Sequence;
        entry.Key = header.Key;
        entry.Lease = lease;
        entry.Length = length;
        entry.RawLength = header.RawLength;
        entry.ReceivedMicrosDelta = PeerCore.StampReceive(nowMicros);
        // The fragments may have travelled in different containers; the tick of the one that completed the message is used.
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

            _core.NoteWork();
        }
        else if (!_core.TryEnqueueReceive(in entry))
        {
            _core.ReturnReceive(in lease);
            counters.RingDrops++;
            return;
        }

        counters.Received++;
        counters.Bytes += length;
        _core.Counters.FragmentedMessagesReceived++;
    }

    /// <summary>Gives back every partial message's buffer (a new epoch, a reconnect, or teardown).</summary>
    private void ClearReassemblies()
    {
        NativeArray<Reassembly>? partials = _partials;
        if (partials is null || partials.IsDisposed || _recv is null || _recv.IsDisposed)
        {
            return;
        }

        for (int local = 0; local < _partialBase.Length; local++)
        {
            int start = _partialBase[local];
            if (start < 0)
            {
                continue;
            }

            int end = start + _partialCap[local];
            for (int i = start; i < end; i++)
            {
                ref Reassembly partial = ref partials[i];
                if ((partial.Flags & PartialInUse) != 0)
                {
                    _core.ReturnReceive(in partial.Lease);
                    partial = default;
                }
            }

            _recv[local].Reassemblies = 0;
        }
    }

    /// <summary>Frees the reassembly table (after the transport can no longer call back).</summary>
    private void DisposeFragmentation()
    {
        ClearReassemblies();
        _partials?.Dispose();
    }

    /// <summary>
    /// One partial message: 64 bytes of native memory, transport thread only (ADR 0008 invariants 4 and 12). The channel's
    /// records are a contiguous range of <see cref="_partials"/>, so its cap is a bound on the range, not on a list.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 64)]
    private struct Reassembly
    {
        /// <summary>The message's key (0 on unkeyed channels, whose messages differ only in their sequence).</summary>
        [FieldOffset(0)] public ulong Key;

        /// <summary>The buffer the fragments are copied into; it becomes the message's payload.</summary>
        [FieldOffset(8)] public BufferLease Lease;

        /// <summary>Clock micros of the first fragment (the expiry and eviction order).</summary>
        [FieldOffset(24)] public long StartedMicros;

        /// <summary>The message's sequence: with the channel and the key, the reassembly scope of PROTOCOL.md §2.1.</summary>
        [FieldOffset(32)] public uint Sequence;

        /// <summary>Size of fragment 0, which every fragment but the last has; 0 until a non-last fragment arrived.</summary>
        [FieldOffset(36)] public int FragmentSize;

        /// <summary>Size of the last fragment; 0 until it arrived.</summary>
        [FieldOffset(40)] public int LastLength;

        /// <summary>Decoded size of a compressed message (the same in every fragment), else 0.</summary>
        [FieldOffset(44)] public int RawLength;

        /// <summary>Payload bytes copied so far.</summary>
        [FieldOffset(48)] public int Filled;

        /// <summary>Bit per fragment received.</summary>
        [FieldOffset(52)] public byte Mask;

        /// <summary>The message's fragment count.</summary>
        [FieldOffset(53)] public byte FragCount;

        /// <summary><see cref="PartialInUse"/>, <see cref="PartialLastAtFront"/>.</summary>
        [FieldOffset(54)] public byte Flags;
    }
}
