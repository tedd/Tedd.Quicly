using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Primitives;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Session;

/// <summary>Transmission hints of a message handed to <see cref="DatagramPacker.Add"/>.</summary>
[Flags]
internal enum DatagramHints : byte
{
    /// <summary>No hints.</summary>
    None = 0,

    /// <summary>
    /// Send with <see cref="TransportSendFlags.Priority"/> (PROTOCOL.md §4.5: <see cref="SendMode.Immediate"/> sends,
    /// channel priority ≥ <see cref="DatagramPacker.PriorityThreshold"/>, control frames). A container carrying at least one
    /// such message is sent with the flag.
    /// </summary>
    Priority = 1,

    /// <summary>
    /// The message is unreliable, so its datagram may be dropped when the transport cannot send it at once
    /// (<c>DropWhenBlocked</c>). A container gets <see cref="TransportSendFlags.CancelOnBlocked"/> only when every member is
    /// unreliable and the pass allows the flag (<see cref="FlushContext.CancelBlockedDatagrams"/>).
    /// </summary>
    Unreliable = 2,

    /// <summary>
    /// The owner finishes a final, non-local completion of the entry with exactly
    /// <see cref="PeerCore.CompleteEntry"/>(slot, <see cref="PeerCore.MapCompletion"/>(completion)) and keeps no per-entry
    /// state of its own (an unfragmented message of a <see cref="DatagramEngine"/>). When every member of a container
    /// carries it, <see cref="DatagramPacker.OnContainerCompleted"/> finishes the members itself instead of routing each one
    /// back through its engine; the outcome is the same.
    /// </summary>
    DirectCompletion = 4,
}

/// <summary>What <see cref="DatagramPacker.Add"/> did with an entry.</summary>
internal enum PackResult : byte
{
    /// <summary>
    /// The packer took the entry: it goes out in this pass, alone (zero copy) or copied into a packed container. Its
    /// completion reaches the owner through the usual routing (<see cref="ChannelEngine.OnSendCompleted"/>), directly or
    /// through the container's fan-out. The engine must not use the entry's queue link any more.
    /// </summary>
    Accepted = 0,

    /// <summary>The send cap of this pass is used up (<see cref="FlushContext.BudgetBytes"/>): keep the entry queued and stop.</summary>
    Blocked = 1,

    /// <summary>Datagrams cannot be sent right now (not negotiated, or disabled by the transport): keep the entry queued.</summary>
    Unavailable = 2,

    /// <summary>Header and payload exceed the current maximum datagram payload: the message cannot be sent as it is.</summary>
    TooLarge = 3,
}

/// <summary>
/// The per-peer datagram packer (PROTOCOL.md §2.2, ADR 0008 invariant 14; docs/design/session-layer.md §7.1). During one
/// scheduler pass the engines hand it filled send entries (header block + payload segment) in schedule order. Buffered
/// messages are packed into one container whenever at least two fit; a message that fits next to nothing else is sent
/// alone, zero copy. A container is its own send entry on channel 1 (<see cref="SendEntryFlags.Container"/>) whose lease is
/// sized from the <em>current</em> maximum datagram payload (read once per pass), written in the
/// <see cref="PackedContainer"/> format (header, then one (Length varint, message) entry per member, written in place) and
/// stamped with the tick of <see cref="QuiclyPeer.Flush"/> when it is not 0.
/// Members join the container with <see cref="SendEntryTable.AddToBatch"/>, stay <c>Filling</c> and are never submitted
/// themselves; their payload is returned as soon as it has been copied. The container's completion fans out to them
/// (<see cref="OnContainerCompleted"/>). Game thread only; no allocation.
/// </summary>
internal sealed unsafe class DatagramPacker
{
    /// <summary>Channel priority from which datagrams are sent with <see cref="TransportSendFlags.Priority"/> (PROTOCOL.md §4.5).</summary>
    public const int PriorityThreshold = 192;

    // A container entry's Aux1: whether every member carried DatagramHints.DirectCompletion (Aux0 counts tracked members).
    private const long RoutedMembers = 0;
    private const long DirectMembers = 1;

    // Headers up to this size are copied into a container with one unaligned vector move (Append).
    private const int HeaderMove = 16;

    private readonly PeerCore _core;
    private int _maxPayload;
    private uint _tick;
    private bool _hasTick;
    private bool _cancelBlocked;
    private int _pending = -1;
    private DatagramHints _pendingHints;
    private int _container = -1;
    private byte* _buffer;
    private int _length;
    private int _count;
    private bool _containerPriority;
    private bool _containerUnreliable;
    private bool _containerDirect;
    private int _trackedMembers;

    /// <summary>Creates the packer of <paramref name="core"/>.</summary>
    /// <param name="core">The peer's shared state.</param>
    public DatagramPacker(PeerCore core) => _core = core;

    /// <summary>True between passes: no message is held and no container is open.</summary>
    public bool IsIdle => _pending < 0 && _container < 0;

    /// <summary>
    /// Forgets what the packer holds without submitting it (<see cref="QuiclyPeer.Reconnect"/>: the entries themselves are
    /// completed by the peer, and the container's lease goes back with its entry). Game thread, between passes.
    /// </summary>
    public void Reset()
    {
        _pending = -1;
        _container = -1;
        _buffer = null;
        _length = 0;
        _count = 0;
        _trackedMembers = 0;
        _containerPriority = false;
        _containerUnreliable = false;
        _containerDirect = false;
    }

    /// <summary>Starts a pass: the current datagram limit, the tick and the send-flag policy (scheduler).</summary>
    /// <param name="flush">The pass.</param>
    public void Begin(in FlushContext flush)
    {
        Debug.Assert(IsIdle, "the previous pass left work in the packer");
        _maxPayload = flush.DatagramsEnabled ? flush.MaxDatagramPayload : 0;
        _tick = flush.Tick;
        _hasTick = flush.Tick != 0;
        _cancelBlocked = flush.CancelBlockedDatagrams;
    }

    /// <summary>
    /// Takes a filled, <c>Filling</c> entry of this pass (engine, from <see cref="ChannelEngine.FlushChannel"/>). The entry
    /// must have left its channel queue when the result is <see cref="PackResult.Accepted"/>.
    /// </summary>
    /// <param name="slot">The entry: header in its header block, payload in its payload segment.</param>
    /// <param name="hints">Priority and reliability of the message.</param>
    /// <param name="flush">The pass (its send budget is checked and charged).</param>
    /// <returns>What happened; on anything but <see cref="PackResult.Accepted"/> the entry is untouched.</returns>
    public PackResult Add(int slot, DatagramHints hints, ref FlushContext flush)
    {
        if (_maxPayload <= 0)
        {
            return PackResult.Unavailable;
        }

        int size = MessageSize(slot);
        if (size > _maxPayload)
        {
            return PackResult.TooLarge;
        }

        if (flush.BudgetBytes <= 0)
        {
            flush.BudgetExhausted = true;
            return PackResult.Blocked;
        }

        flush.BudgetBytes -= size;
        if (_container >= 0)
        {
            if (CanAppend(size))
            {
                Append(slot, hints);
                return PackResult.Accepted;
            }

            SubmitContainer(ref flush);
        }

        if (_pending < 0)
        {
            _pending = slot;
            _pendingHints = hints;
            return PackResult.Accepted;
        }

        long needed = PackedContainer.GetHeaderLength(_tick, _hasTick) + PackedContainer.GetEntryLength(MessageSize(_pending)) + PackedContainer.GetEntryLength(size);
        if (needed <= _maxPayload && TryOpenContainer())
        {
            int pending = _pending;
            _pending = -1;
            Append(pending, _pendingHints);
            Append(slot, hints);
            return PackResult.Accepted;
        }

        // The held message fits next to nothing: it goes out alone and the new one waits for a partner.
        int loose = _pending;
        DatagramHints looseHints = _pendingHints;
        _pending = slot;
        _pendingHints = hints;
        SubmitLoose(loose, looseHints, ref flush);
        return PackResult.Accepted;
    }

    /// <summary>Ends the pass: submits the open container, or the held message alone (scheduler).</summary>
    /// <param name="flush">The pass.</param>
    public void Finish(ref FlushContext flush)
    {
        if (_container >= 0)
        {
            SubmitContainer(ref flush);
        }

        if (_pending >= 0)
        {
            int pending = _pending;
            _pending = -1;
            SubmitLoose(pending, _pendingHints, ref flush);
        }
    }

    /// <summary>
    /// Hands what the packer holds (the open container, the held message) to the transport now, in the middle of a pass
    /// (game thread). A stream engine calls it before its own stream sends, so datagrams of channels the scheduler reached
    /// earlier (higher priority) leave first instead of queueing behind stream data, which matters with
    /// <see cref="TransportSendFlags.CancelOnBlocked"/>. Messages added later in the pass start a new container.
    /// </summary>
    /// <param name="flush">The pass.</param>
    public void SubmitPending(ref FlushContext flush) => Finish(ref flush);

    /// <summary>
    /// The completion of a container entry (game thread, from the completion routing): the early Sent notice returns the
    /// container's lease (the transport no longer needs the bytes; the slot stays reserved until the final state, ADR 0008
    /// invariant 1) and, when a member is tracked, passes the notice on; the final completion goes to every member's owner
    /// with the container's outcome — or, when every member is a plain datagram message
    /// (<see cref="DatagramHints.DirectCompletion"/>) and the completion came from the transport, finishes the members here
    /// exactly as their owner would — then the container entry is finished.
    /// </summary>
    /// <param name="container">The container entry.</param>
    /// <param name="completion">What the transport (or the game thread, for a refused submission) reported.</param>
    public void OnContainerCompleted(int container, in CompletionEntry completion)
    {
        SendEntryTable entries = _core.Entries;
        if (!completion.Final)
        {
            _core.ReleasePayload(container);
            if (entries[container].Aux0 > 0)
            {
                int member = entries.BatchHead[container];
                while (member >= 0)
                {
                    int next = entries.Next[member];
                    RouteMember(member, in completion);
                    member = next;
                }
            }

            return;
        }

        int m = entries.BatchHead[container];
        entries.ClearBatch(container);
        if (entries[container].Aux1 == DirectMembers && completion.Kind != CompletionKind.Local)
        {
            // Every member is a plain datagram message whose owner would do exactly this (DatagramHints.DirectCompletion),
            // so the per-member routing (entry → channel → engine → virtual call → fragment check) is skipped. The status is
            // still mapped per member, as the owner would: a continuation of an earlier member may start closing the
            // transport, which changes the mapping of a canceled datagram. A local completion (refused submission) still
            // goes to the owners, which correct their counters.
            NativeArray<int> links = entries.Next;
            while (m >= 0)
            {
                // Read the link first: finishing a member frees its slot, which a continuation may reuse at once.
                int next = links[m];
                _core.CompleteEntry(m, _core.MapCompletion(in completion));
                m = next;
            }
        }
        else
        {
            while (m >= 0)
            {
                // Read the link first: finishing a member frees its slot, which a continuation may reuse at once.
                int next = entries.Next[m];
                RouteMember(m, in completion);
                m = next;
            }
        }

        _core.CompleteEntry(container, _core.MapCompletion(in completion));
    }

    private void RouteMember(int member, in CompletionEntry container)
    {
        CompletionEntry completion = container;
        completion.Slot = member;
        completion.Generation = _core.Entries[member].Generation;
        _core.Peer.RouteCompletion(in completion);
    }

    private int MessageSize(int slot)
    {
        ref SendEntry entry = ref _core.Entries[slot];
        return entry.HeaderLength + (int)entry.Payload.Length;
    }

    private bool CanAppend(int size) =>
        _count < PackedContainer.MaxMessages && PackedContainer.GetEntryLength(size) <= _maxPayload - _length;

    private bool TryOpenContainer()
    {
        if (!_core.TryAllocateEntry(PeerCore.ContainerChannelId, SendEntryFlags.Container, out int slot))
        {
            return false;
        }

        if (!_core.TryRentSend(_maxPayload, out BufferLease lease))
        {
            _core.DiscardEntry(slot);
            return false;
        }

        _core.AttachLease(slot, in lease, 0);
        _buffer = _core.GetPointer(in lease);
        _length = PackedContainer.WriteHeader(new Span<byte>(_buffer, _maxPayload), _tick, _hasTick);
        _count = 0;
        _container = slot;
        _containerPriority = false;
        _containerUnreliable = true;
        _containerDirect = true;
        _trackedMembers = 0;
        return true;
    }

    private void Append(int member, DatagramHints hints)
    {
        SendEntryTable entries = _core.Entries;
        ref SendEntry entry = ref entries[member];
        int headerLength = entry.HeaderLength;
        int payloadLength = (int)entry.Payload.Length;
        int messageLength = headerLength + payloadLength;
        byte* header = entry.Header.Buffer;

        // The (Length varint, message) entry is written in place: the caller already checked what PackedContainerWriter
        // would check again (CanAppend, or the size check before TryOpenContainer: it fits, and fewer than MaxMessages are
        // in), and a message is never empty and never starts with the container's channel id (it has its own channel id).
        Debug.Assert(messageLength >= 1 && _count < PackedContainer.MaxMessages, "the caller checked the message count");
        Debug.Assert(PackedContainer.GetEntryLength(messageLength) <= _maxPayload - _length, "the caller checked that the message fits");
        Debug.Assert(*header != PackedContainer.ChannelId, "a container member is a channel-0 or channel->=2 frame");
        Debug.Assert(SendEntryTable.HeaderBlockSize >= HeaderMove, "the header block holds the bytes of one vector move");
        byte* target = _buffer + _length;
        target += VarInt.Write(target, (uint)messageLength);
        if (headerLength <= HeaderMove && _maxPayload - (int)(target - _buffer) >= HeaderMove)
        {
            // One 16-byte move instead of a Memmove call for a header of a few bytes. The header block always holds
            // SendEntryTable.HeaderBlockSize readable bytes, and the bytes written past the header stay inside the container's
            // buffer: the payload below overwrites them, or a later entry does, or they lie past the container's length.
            Unsafe.WriteUnaligned(target, Unsafe.ReadUnaligned<Vector128<byte>>(header));
        }
        else
        {
            new ReadOnlySpan<byte>(header, headerLength).CopyTo(new Span<byte>(target, headerLength));
        }

        if (payloadLength > 0)
        {
            new ReadOnlySpan<byte>(entry.Payload.Buffer, payloadLength).CopyTo(new Span<byte>(target + headerLength, payloadLength));
        }

        _length = (int)(target - _buffer) + messageLength;
        _count++;
        entries.AddToBatch(_container, member);
        _containerPriority |= (hints & DatagramHints.Priority) != 0;
        _containerUnreliable &= (hints & DatagramHints.Unreliable) != 0;
        _containerDirect &= (hints & DatagramHints.DirectCompletion) != 0;
        if ((entry.Flags & SendEntryFlags.Tracked) != 0)
        {
            _trackedMembers++;
        }

        // The bytes live in the container now; the member only waits for the container's outcome.
        _core.ReleasePayload(member);
        _core.Counters.MessagesPacked++;
    }

    private void SubmitContainer(ref FlushContext flush)
    {
        int slot = _container;
        _container = -1;
        int size = _length;
        ref SendEntry entry = ref _core.Entries[slot];
        entry.Payload.Length = (uint)size;
        entry.Aux0 = _trackedMembers;
        entry.Aux1 = _containerDirect ? DirectMembers : RoutedMembers;
        TransportStatus status = _core.SubmitDatagram(slot, SendFlags(_containerPriority, _containerUnreliable));
        if (status != TransportStatus.Success)
        {
            // No completion follows a refused call: the container (and through it every member) is finished locally.
            _core.QueueLocalCompletion(slot, _core.MapSubmitFailure(status));
            return;
        }

        PeerCounters counters = _core.Counters;
        counters.DatagramsSent++;
        counters.ContainersSent++;
        counters.DatagramBytesSent += size;
        flush.BytesSubmitted += size;
    }

    private void SubmitLoose(int slot, DatagramHints hints, ref FlushContext flush)
    {
        // Read before submitting: after a successful call the entry belongs to the transport until its completion.
        int size = MessageSize(slot);
        TransportStatus status = _core.SubmitDatagram(slot, SendFlags((hints & DatagramHints.Priority) != 0, (hints & DatagramHints.Unreliable) != 0));
        if (status != TransportStatus.Success)
        {
            _core.QueueLocalCompletion(slot, _core.MapSubmitFailure(status));
            return;
        }

        PeerCounters counters = _core.Counters;
        counters.DatagramsSent++;
        counters.DatagramBytesSent += size;
        flush.BytesSubmitted += size;
    }

    private TransportSendFlags SendFlags(bool priority, bool unreliable)
    {
        // Never DelaySend: it measured 16-19 % slower per datagram for tick bursts on MsQuic loopback (§7.1).
        TransportSendFlags flags = priority ? TransportSendFlags.Priority : TransportSendFlags.None;
        if (unreliable && _cancelBlocked)
        {
            flags |= TransportSendFlags.CancelOnBlocked;
        }

        return flags;
    }
}
