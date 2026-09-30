using System.Diagnostics;
using System.Runtime.CompilerServices;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Primitives;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Session;

// Game thread: Poll (completions, control signals, timers, state events, handler dispatch, stream resume), Drain,
// Release, Retain and handler registration (docs/design/session-layer.md §4.4).
public sealed unsafe partial class QuiclyPeer
{
    private readonly MessageHandler?[] _handlers;

    /// <summary>
    /// Per dense channel: a reliable stream channel that compresses. <see cref="Drain"/> hands out decoded payloads the
    /// caller releases only after the call, so a batch can need more decode buffers than the receive budget has; such a
    /// channel's message then waits for the next Drain instead of being dropped (<see cref="TryRentForDecode"/>).
    /// </summary>
    private readonly bool[] _decodeWaits;
    private readonly ReceiveQueues _queues;
    private ReceiveEntry _held;
    private bool _hasHeld;
    private ReceiveHeader _dispatchHeader;
    private BufferLease _dispatchLease;
    private byte* _dispatchData;
    private bool _dispatching;
    private bool _dispatchRetained;
    private bool _inPoll;
    private TokenBucket _decodeBucket;
    private long _nextDeadlineMicros = long.MaxValue;

    /// <summary>
    /// Runs the game-thread side of the peer: drains transport completions (tracked sends complete here in
    /// <see cref="CompletionMode.PollOnly"/>), processes the control protocol (handshake, admission, pongs, close),
    /// runs time-driven work, raises <see cref="StateChanged"/>, dispatches received messages to their channel's
    /// <see cref="MessageHandler"/> (messages of channels without a handler wait for <see cref="Drain"/>) and resumes
    /// streams held back by the receive ring. Allocation-free in steady state.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Channels without a handler.</b> Their messages are moved to per-channel queues for <see cref="Drain"/>, and what
    /// happens when nobody drains depends on the delivery mode.
    /// </para>
    /// <para>
    /// An <em>unreliable</em> channel (UnreliableUnordered or UnreliableSequenced without <c>CoalesceOnReceive</c>) that is
    /// drained every frame loses nothing the receive ring and the receive budget took, in whatever order the host calls
    /// Poll and Drain and however many channels it reads this way: a burst larger than the queue pool waits in the pool,
    /// one held message and the ring until the Drain. That includes what arrived with the handshake: the Poll in which
    /// the peer became Connected — <c>QuiclyClient.ConnectAsync</c> runs it for a client — counts as drained for every
    /// channel. What such a channel still has queued when the <em>next</em> Poll begins — nobody drained it empty in
    /// between — is its <em>backlog</em>, and the backlog of all these channels together is bounded: at most the part of
    /// the queue pool that is not reserved for reliable channels (the pool is <see cref="PeerOptions.ReceiveRingCapacity"/>
    /// messages, at most 1 024; half of it is reserved when the table has a ReliableOrdered or ReliableUnordered
    /// channel) and at most a quarter of <see cref="PeerOptions.ReceiveBudgetBytes"/>, counted in buffer blocks. Poll cuts
    /// it to that where it starts, and a later message of a backlogged channel that does not fit drops the
    /// <em>oldest</em> queued one, counted in <see cref="ChannelStatistics.DrainQueueDrops"/> and
    /// <see cref="PeerStatistics.DrainQueueDrops"/>. So a host that polls several times between two drains — a server
    /// that polls at network rate and drains at simulation rate — keeps only that much of a burst. A channel nobody
    /// drains keeps the ring closed once, for one Poll interval (the message held for a Drain that did not come), and
    /// afterwards costs only its own oldest messages: it stays backlog, and is never held again, until it has been
    /// drained. A channel that was drained and then no longer is can keep the ring closed for a second interval.
    /// </para>
    /// <para>
    /// A <em>reliable</em> channel (ReliableOrdered, ReliableUnordered) loses nothing, so once the queue pool is full Poll
    /// holds its next message and takes nothing more out of the receive ring: every stream channel is then
    /// back-pressured, datagrams of ring channels are dropped on arrival (<see cref="PeerStatistics.ReceiveRingDrops"/>),
    /// responses wait and <see cref="HasPendingWork"/> stays set, until the application drains that channel or registers a
    /// handler for it. Coalescing channels and ReliableLatest keep one value per key in a mailbox and are dispatched even
    /// then. Register a handler for, or drain, every reliable channel the other end sends on.
    /// </para>
    /// </remarks>
    /// <param name="maxItems">Most messages to dispatch to handlers in this call.</param>
    /// <returns>Messages dispatched to handlers.</returns>
    /// <exception cref="ObjectDisposedException">The peer is disposed.</exception>
    public int Poll(int maxItems = int.MaxValue)
    {
        ThrowIfDisposed();
        ArgumentOutOfRangeException.ThrowIfNegative(maxItems);
        if (_closedRaised || _inPoll)
        {
            return 0;
        }

        _inPoll = true;
        if (_inFlush)
        {
            _gate.Touched = true; // nested in a Flush (a continuation): its pass may have run already (QuiclyPeer.FlushGate.cs)
        }

        EnterCall();
        NoteGameThread();
        // The host is here now: the next work published raises a fresh signal (IPeerWorkSignal is an edge).
        ClearWorkSignal();
        int dispatched = 0;
        try
        {
            long now = _clock.NowMicros;
            _core.NotePass(now);
            DrainCompletionsMarking(); // a completion that reaches an engine here is the next Flush's to follow up
            bool immediate = DrainForeignSends();
            RetrySendWaiters();
            if (immediate && _state == PeerState.Connected)
            {
                // An Immediate send from another thread: its scheduler pass runs here, on the game thread.
                FlushImmediate();
            }

            ProcessSignals(now);

            // A bulk object's next range starts here: BeginBulkSendAsync is the game thread's, while the range that just
            // finished completed on the thread pool and only raised the work signal that brought the host back.
            PumpBulkObjects();
            long next = RunTimers(now);
            RaiseTransitions(holdClosed: true);
            if (_disposed)
            {
                return 0;
            }

            if (_state is PeerState.Connected or PeerState.Closing or PeerState.Closed)
            {
                dispatched = DispatchReceived(maxItems, now);
                if (_disposed)
                {
                    return dispatched;
                }
            }

            ResumePendedStreams();
            if (_state == PeerState.Closed)
            {
                FinishClosed();
            }
            else
            {
                RaiseTransitions(holdClosed: false);
            }

            if (_passEngines && _state == PeerState.Connected)
            {
                LowerFlushDeadlineForPassWork(now);
            }

            UpdateNextDeadline(next);
            return dispatched;
        }
        finally
        {
            _inPoll = false;
            ExitCall();
        }
    }

    /// <summary>
    /// Takes up to <paramref name="into"/>.Length received messages of <paramref name="channel"/> (batch alternative to a
    /// handler, for data-oriented consumers). Each message's payload stays valid until it is passed to
    /// <see cref="Release(ReadOnlySpan{ReceivedMessage})"/>; every drained message must be released exactly once.
    /// Messages of other channels met on the way wait in per-channel queues. Game thread.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Drain every channel you read this way once per frame, and completely (call until it returns 0, or pass a span
    /// that is not filled): a channel that was drained empty since the last <see cref="Poll"/> began loses nothing the
    /// receive ring and the receive budget took, whether the Drain comes before or after the Poll. A channel that was not
    /// becomes backlog at the next Poll, which is bounded and evicts its oldest messages
    /// (<see cref="ChannelStatistics.DrainQueueDrops"/>; see <see cref="Poll"/>), and stays backlog until a Drain finds
    /// or leaves its queue empty.
    /// </para>
    /// <para>
    /// The queue pool is bounded. A message of another channel that finds it full makes room by evicting the oldest
    /// message of such a backlog when there is one; otherwise it is held, and this call then takes nothing more out of the
    /// receive ring. How long a hold lasts depends on the held message's channel: one <em>with a handler</em> is never
    /// evicted and never dropped — the next <see cref="Poll"/> dispatches its queue, the held message and the ring, in
    /// order, so the two styles can be mixed in either order without loss; an <em>unreliable</em> channel without a
    /// handler is held until its own Drain — across the next Poll too while the channel is being drained, so a host that
    /// drains several channels one after the other every frame loses nothing — or, when nobody drains it, until a Poll
    /// makes it backlog; a <em>reliable</em> channel without a handler is held until that channel is drained.
    /// </para>
    /// <para>
    /// A compressed message is decoded here, into a second buffer of its raw size, and the caller can release nothing
    /// before the call returns. On a reliable channel (ReliableOrdered, ReliableUnordered) a message for which there is
    /// no such buffer within the receive budget is not dropped: it stays where it is and the call returns what it has,
    /// possibly fewer messages than the span holds, or none. Release them and drain again. (A message that could never
    /// be decoded is dropped and counted in <see cref="PeerStatistics.DecodeFailures"/>, as are compressed messages of
    /// unreliable channels and those that exceed <see cref="PeerOptions.DecodedBytesPerSecond"/>.)
    /// </para>
    /// </remarks>
    /// <param name="channel">The channel.</param>
    /// <param name="into">Receives the messages, oldest first (coalesced keys last).</param>
    /// <returns>Messages written.</returns>
    /// <exception cref="ArgumentException">The channel is not in the table.</exception>
    /// <exception cref="ObjectDisposedException">The peer is disposed.</exception>
    public int Drain(ushort channel, Span<ReceivedMessage> into)
    {
        ThrowIfDisposed();
        int index = ChannelIndexOrThrow(channel);
        if (_closedRaised || into.IsEmpty || _state is PeerState.Connecting or PeerState.Handshaking)
        {
            return 0;
        }

        long now = _clock.NowMicros;
        int written = 0;
        ReceiveQueues queues = _queues;
        bool waits = _decodeWaits[index];
        while (written < into.Length)
        {
            // A compressed message of a reliable channel is looked at first: without a buffer to decode it into, it stays
            // queued and the call ends here. The caller releases what it got and drains again.
            BufferLease room = BufferLease.Empty;
            if (waits && queues.TryPeek(index, out ReceiveEntry head) && !TryRentForDecode(in head, out room))
            {
                break;
            }

            if (!queues.TryTake(index, out ReceiveEntry entry))
            {
                break;
            }

            // A response is taken by its engine where it leaves the receive ring (below, and in Route), and that is the only
            // way into a per-channel queue or the held slot, so neither can hold one (see IsResponse).
            Debug.Assert(!IsResponse(in entry), "a response never reaches a per-channel queue");
            Emit(ref entry, now, into, ref written, in room);
        }

        if (_hasHeld && written < into.Length)
        {
            Debug.Assert(!IsResponse(in _held), "a response never reaches the held slot");
            if (_held.Channel == channel)
            {
                BufferLease room = BufferLease.Empty;
                if (!waits || TryRentForDecode(in _held, out room))
                {
                    ReceiveEntry entry = _held;
                    _hasHeld = false;
                    Emit(ref entry, now, into, ref written, in room);
                }
            }
            else if (TryQueue(in _held, mayHold: true))
            {
                _hasHeld = false;
            }
        }

        SpscRing<ReceiveEntry> ring = _core.ReceiveRing;
        while (!_hasHeld && written < into.Length && ring.TryDequeue(out ReceiveEntry entry))
        {
            if (IsResponse(in entry))
            {
                // A response never reaches the application: it completes its SendRequestAsync, or is dropped and counted.
                TakeResponse(ref entry, now);
            }
            else if (entry.Channel == channel)
            {
                BufferLease room = BufferLease.Empty;
                if (waits && !TryRentForDecode(in entry, out room))
                {
                    // It left the ring and cannot be decoded yet: held for the next Drain (a Poll moves it to the queue).
                    _held = entry;
                    _hasHeld = true;
                }
                else
                {
                    Emit(ref entry, now, into, ref written, in room);
                }
            }
            else if (!TryQueue(in entry, mayHold: true))
            {
                _held = entry;
                _hasHeld = true;
            }
        }

        ReceiveMailbox? box = _core.GetMailbox(index);
        if (box is not null && written < into.Length)
        {
            Span<int> keys = stackalloc int[64];
            int count;
            while (written < into.Length && (count = box.PopDirty(keys.Slice(0, Math.Min(keys.Length, into.Length - written)))) > 0)
            {
                for (int i = 0; i < count; i++)
                {
                    if (box.TryTake(keys[i], out ReceiveEntry entry))
                    {
                        Emit(ref entry, now, into, ref written, BufferLease.Empty);
                    }
                }
            }
        }

        return written;
    }

    /// <summary>
    /// Returns the payloads of drained messages to the pool (game thread). Each message must be released exactly once.
    /// </summary>
    /// <remarks>
    /// Still valid after <see cref="Dispose"/>, and required then for a peer over a shared allocator
    /// (<see cref="PeerOptions.Allocator"/>; every peer of a server): the blocks go back to the shared pool, which outlives
    /// the peer. For a peer with its own private pool the call is a no-op once the peer is disposed (the pool is freed with
    /// the peer). Release before the shared allocator itself is disposed; a release after that is a no-op.
    /// </remarks>
    /// <param name="messages">Messages from <see cref="Drain"/>.</param>
    public void Release(ReadOnlySpan<ReceivedMessage> messages)
    {
        if (_disposed)
        {
            for (int i = 0; i < messages.Length; i++)
            {
                _core.ReturnReceiveAfterDispose(in messages[i].Lease);
            }

            return;
        }

        for (int i = 0; i < messages.Length; i++)
        {
            _core.ReturnReceive(in messages[i].Lease);
        }
    }

    /// <summary>
    /// Returns a retained payload, or the payload of a response from <see cref="SendRequestAsync"/>, to the pool (game
    /// thread). Must be called exactly once per lease.
    /// </summary>
    /// <remarks>
    /// Still valid after <see cref="Dispose"/>, and required then for a peer over a shared allocator
    /// (<see cref="PeerOptions.Allocator"/>; every peer of a server, which disposes a peer right after its
    /// <c>PeerClosed</c> event): the block goes back to the shared pool, which outlives the peer. For a peer with its own
    /// private pool the call is a no-op once the peer is disposed (the pool is freed with the peer, and the lease's
    /// payload with it). Release before the shared allocator itself is disposed; a release after that is a no-op.
    /// </remarks>
    /// <param name="lease">A lease from <see cref="Retain"/>.</param>
    public void Release(in ReceiveLease lease)
    {
        if (_disposed)
        {
            _core.ReturnReceiveAfterDispose(in lease.Lease);
            return;
        }

        _core.ReturnReceive(in lease.Lease);
    }

    /// <summary>
    /// Keeps the payload of the message being handled beyond its handler. Only valid inside a <see cref="MessageHandler"/>
    /// for the header it was given; release the lease with <see cref="Release(in ReceiveLease)"/> — exactly once, and also
    /// when the peer was disposed meanwhile (a peer over a shared <see cref="PeerOptions.Allocator"/> returns the block to
    /// that pool then; see <see cref="Dispose"/>).
    /// </summary>
    /// <param name="header">The header passed to the handler.</param>
    /// <returns>The lease.</returns>
    /// <exception cref="InvalidOperationException">Not inside a handler, another message's header, or already retained.</exception>
    public ReceiveLease Retain(in ReceiveHeader header)
    {
        if (!_dispatching)
        {
            throw new InvalidOperationException("Retain is only valid inside a message handler.");
        }

        if (!Unsafe.AreSame(ref Unsafe.AsRef(in header), ref _dispatchHeader) && !IsSameMessage(in header, in _dispatchHeader))
        {
            throw new InvalidOperationException("The header does not belong to the message being handled.");
        }

        if (_dispatchRetained)
        {
            throw new InvalidOperationException("The message was already retained.");
        }

        _dispatchRetained = true;
        return new ReceiveLease(in _dispatchHeader, in _dispatchLease, _dispatchData);
    }

    /// <summary>Registers the handler of a channel; <see cref="Poll"/> passes it every message of the channel (game thread).</summary>
    /// <param name="channel">The channel.</param>
    /// <param name="handler">The handler (one per channel).</param>
    /// <exception cref="ArgumentException">The channel is not in the table.</exception>
    /// <exception cref="InvalidOperationException">The channel already has a handler.</exception>
    public void RegisterHandler(ushort channel, MessageHandler handler)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(handler);
        int index = ChannelIndexOrThrow(channel);
        if (_handlers[index] is not null)
        {
            throw new InvalidOperationException($"Channel {channel} already has a handler.");
        }

        _handlers[index] = handler;
        _queues.SetHandled(index, true);
    }

    /// <summary>
    /// Removes the handler of a channel; its messages then wait for <see cref="Drain"/> (game thread), within the limits
    /// described at <see cref="Poll"/>: an unreliable channel that is not drained loses its oldest messages, a reliable one
    /// that is not drained eventually holds up every other channel.
    /// </summary>
    /// <param name="channel">The channel.</param>
    /// <returns><see langword="false"/> when the channel had no handler.</returns>
    /// <exception cref="ArgumentException">The channel is not in the table.</exception>
    public bool UnregisterHandler(ushort channel)
    {
        ThrowIfDisposed();
        int index = ChannelIndexOrThrow(channel);
        if (_handlers[index] is null)
        {
            return false;
        }

        _handlers[index] = null;
        _queues.SetHandled(index, false);
        return true;
    }

    private int ChannelIndexOrThrow(ushort channel)
    {
        int index = _core.ChannelIndexOf(channel);
        if (index < 0)
        {
            throw new ArgumentException($"Channel {channel} is not in the channel table.", nameof(channel));
        }

        return index;
    }

    private int DispatchReceived(int maxItems, long now)
    {
        int dispatched = 0;

        // A new pass: whatever an unreliable channel without a handler still has queued was not drained since the last
        // one, and is backlog from here on (bounded, evicted oldest first).
        _queues.BeginPass(_core);
        if (_queues.QueuedHandled > 0)
        {
            dispatched = DispatchQueued(maxItems, now);
        }

        // A held message whose channel has a handler is a dispatch and waits for room in maxItems; any other is only
        // moved to its queue, and is retried whatever the limit — a Poll(0) must not keep the ring closed.
        if (_hasHeld && !_disposed && (dispatched < maxItems || !HasHandler(_held.Channel)))
        {
            ReceiveEntry held = _held;
            _hasHeld = false;
            if (!Route(ref held, now, ref dispatched, mayHold: false))
            {
                // Still no room for it: the ring stays closed (the loop below is guarded by the held slot), but the
                // mailboxes do not pass through the ring and are dispatched all the same. Returning here would stop
                // every coalescing and ReliableLatest handler for as long as one reliable channel is not drained —
                // while the transport thread keeps acknowledging those values to the sender. A reliable channel (or a
                // replaced engine's) gets here, and an unreliable one the application drained since the last pass
                // began: its Drain is about to take the message. An unreliable message of a channel nobody drained
                // is queued by evicting backlog, or dropped (mayHold is false).
                _held = held;
                _hasHeld = true;
            }
        }

        SpscRing<ReceiveEntry> ring = _core.ReceiveRing;
        while (dispatched < maxItems && !_disposed && !_hasHeld && ring.TryDequeue(out ReceiveEntry entry))
        {
            if (!Route(ref entry, now, ref dispatched, mayHold: true))
            {
                _held = entry;
                _hasHeld = true;
            }
        }

        ReadOnlySpan<ReceiveMailbox> boxes = _core.Mailboxes;
        if (!boxes.IsEmpty && dispatched < maxItems && !_disposed)
        {
            dispatched += DispatchMailboxes(boxes, maxItems - dispatched, now);
        }

        return dispatched;
    }

    private bool HasHandler(ushort channel)
    {
        int index = _core.ChannelIndexOf(channel);
        return index >= 0 && _handlers[index] is not null;
    }

    private bool Route(ref ReceiveEntry entry, long now, ref int dispatched, bool mayHold)
    {
        if (IsResponse(in entry))
        {
            TakeResponse(ref entry, now);
            return true;
        }

        int index = _core.ChannelIndexOf(entry.Channel);
        MessageHandler? handler = index >= 0 ? _handlers[index] : null;
        if (handler is not null)
        {
            if (_queues.QueuedHandled != 0 && _queues.Count(index) != 0)
            {
                // Older messages of this channel wait in its queue: a handler of this very Poll drained another channel
                // and met them, or registered this handler over a backlog, after the queues were dispatched. This one
                // goes behind them, and the next Poll dispatches the queue first — or the channel would be out of order.
                // It cannot starve there: a Poll takes nothing from the ring before the queues of channels with a
                // handler are empty (or maxItems is used up), so the queue is always served first.
                return TryQueue(in entry, mayHold);
            }

            Dispatch(handler, ref entry, now);
            dispatched++;
            return true;
        }

        if (index < 0)
        {
            _core.ReturnReceive(in entry.Lease);
            return true;
        }

        return TryQueue(in entry, mayHold);
    }

    /// <summary>
    /// Moves a message that left the receive ring into its channel's drain queue (game thread, from <see cref="Poll"/> for a
    /// channel without a handler and from <see cref="Drain"/> for a message of another channel).
    /// </summary>
    /// <remarks>
    /// <para>
    /// <see langword="false"/> means the pool is full and the message may not be dropped: the caller holds it and stops
    /// taking from the ring. Who can be held, and for how long (<see cref="ReceiveQueues.TryAppendDatagram"/>):
    /// </para>
    /// <list type="bullet">
    /// <item>a reliable channel (or a replaced engine's): until the application drains it or registers a handler — the
    /// limit that remains (see <see cref="Poll"/>);</item>
    /// <item>an unreliable channel <em>with</em> a handler, met by a <see cref="Drain"/> of another channel: until the next
    /// <see cref="Poll"/>. That Poll dispatches the channel's queue, then the held message, then the ring, all to the
    /// handler and in arrival order, so the hold cannot outlive it (a Poll whose <c>maxItems</c> is used up dispatches
    /// less, and the rest waits like every other message of a handled channel). Nothing of a handled channel is ever
    /// evicted;</item>
    /// <item>an unreliable channel <em>without</em> a handler that is not backlogged (the
    /// application drained it, as a host that polls and then drains every frame does): until its <see cref="Drain"/>,
    /// which takes the queue, the held message and the ring, in that order — nothing is lost that the ring and the
    /// budget took. A Poll that comes first holds the message again while the channel was drained since the Poll before
    /// it (a host that drains several channels in turn reaches this one's Drain after the Poll). If nobody drained it,
    /// that Poll makes everything the channel has queued backlog: the held message is then queued by evicting the
    /// oldest (<paramref name="mayHold"/> is <see langword="false"/> for it), and from then on the channel never holds
    /// again until it has been drained — also when other channels evict its whole queue meanwhile.</item>
    /// </list>
    /// <para>
    /// So an unreliable channel that nobody drains closes the ring once, for one Poll interval, and afterwards costs
    /// only its own oldest messages (<see cref="ChannelStatistics.DrainQueueDrops"/>); several such channels close it
    /// once each, not in turns for ever. One that was drained and is then abandoned can close it for two intervals:
    /// the one in which it was last drained and the one after.
    /// </para>
    /// </remarks>
    /// <param name="entry">The message.</param>
    /// <param name="mayHold">
    /// <see langword="false"/> for a message that was held when this pass began: an unreliable message without a handler
    /// is then held again only if its channel was drained since the pass before began, and otherwise queued or dropped.
    /// </param>
    /// <returns><see langword="false"/> when the caller has to hold the message.</returns>
    private bool TryQueue(in ReceiveEntry entry, bool mayHold)
    {
        int index = _core.ChannelIndexOf(entry.Channel);
        ReceiveQueues queues = _queues;
        if (queues.IsDatagram(index))
        {
            return queues.TryAppendDatagram(index, in entry, _core, mayHold);
        }

        return queues.TryAppend(index, in entry);
    }

    private int DispatchQueued(int maxItems, long now)
    {
        int dispatched = 0;
        ReceiveQueues queues = _queues;
        for (int index = 0; index < _handlers.Length && queues.QueuedHandled > 0 && dispatched < maxItems && !_disposed; index++)
        {
            MessageHandler? handler = _handlers[index];
            while (handler is not null && dispatched < maxItems && !_disposed && queues.TryTake(index, out ReceiveEntry entry))
            {
                Debug.Assert(!IsResponse(in entry), "a response never reaches a per-channel queue");
                Dispatch(handler, ref entry, now);
                dispatched++;
                handler = _handlers[index];
            }
        }

        return dispatched;
    }

    private int DispatchMailboxes(ReadOnlySpan<ReceiveMailbox> boxes, int budget, long now)
    {
        Span<int> keys = stackalloc int[64];
        int dispatched = 0;
        for (int b = 0; b < boxes.Length && dispatched < budget && !_disposed; b++)
        {
            ReceiveMailbox box = boxes[b];
            MessageHandler? handler = _handlers[box.ChannelIndex];
            if (handler is null)
            {
                continue; // left for Drain
            }

            int count;
            while (dispatched < budget && !_disposed && (count = box.PopDirty(keys.Slice(0, Math.Min(keys.Length, budget - dispatched)))) > 0)
            {
                for (int i = 0; i < count; i++)
                {
                    if (box.TryTake(keys[i], out ReceiveEntry entry))
                    {
                        Dispatch(handler, ref entry, now);
                        dispatched++;
                    }
                }
            }
        }

        return dispatched;
    }

    private void Dispatch(MessageHandler handler, ref ReceiveEntry entry, long now)
    {
        if ((entry.Flags & ReceiveFlags.Compressed) != 0 && !TryDecode(ref entry, now, BufferLease.Empty))
        {
            return;
        }

        byte* data = entry.Lease.IsEmpty ? null : _core.GetPointer(in entry.Lease);
        _dispatchHeader = MakeHeader(in entry, now);
        _dispatchLease = entry.Lease;
        _dispatchData = data;
        _dispatchRetained = false;
        _dispatching = true;
        try
        {
            handler(this, in _dispatchHeader, new ReadOnlySpan<byte>(data, entry.Length));
        }
        finally
        {
            _dispatching = false;
            if (!_dispatchRetained && Volatile.Read(ref _freed) == 0)
            {
                _core.ReturnReceive(in entry.Lease);
            }
        }
    }

    /// <summary>
    /// Whether a received message is the response of a request/response channel (PROTOCOL.md §3.1).
    /// </summary>
    /// <remarks>
    /// Every message the game thread takes out of the receive ring is offered to its engine first — in <see cref="Route"/> for
    /// <see cref="Poll"/>, in the ring loop of <see cref="Drain"/> for the batch API — and only a message that is not a
    /// response is dispatched, written to the caller's span, queued for another channel or held. Those two are therefore the
    /// only interception points the peer needs: a per-channel queue and the held slot can only ever receive what already
    /// passed one of them, so no channel handler and no <see cref="Drain"/> caller can see a response. The queue and held
    /// paths assert that invariant instead of checking it again.
    /// </remarks>
    private static bool IsResponse(in ReceiveEntry entry) => (entry.Flags & ReceiveFlags.IsResponse) != 0;

    /// <summary>
    /// Hands a response to the engine of its channel instead of the application (game thread, from <see cref="Poll"/> and
    /// <see cref="Drain"/>): a request waiting for it completes with the payload, and a response no request matches is
    /// dropped and counted (PROTOCOL.md §3.1). A compressed response is decoded first, exactly as a dispatched message is.
    /// </summary>
    private void TakeResponse(ref ReceiveEntry entry, long now)
    {
        if ((entry.Flags & ReceiveFlags.Compressed) != 0 && !TryDecode(ref entry, now, BufferLease.Empty))
        {
            // Dropped and counted (DecodeFailures); the request ends with its timeout.
            return;
        }

        int index = _core.ChannelIndexOf(entry.Channel);
        if (index >= 0)
        {
            byte* data = entry.Lease.IsEmpty ? null : _core.GetPointer(in entry.Lease);
            ReceiveLease response = new(MakeHeader(in entry, now), in entry.Lease, data);
            if (_core.GetEngine(index).TryTakeResponse(in response))
            {
                return;
            }
        }

        _core.Counters.ResponsesUnmatched++;
        _core.ReturnReceive(in entry.Lease);
    }

    private void Emit(ref ReceiveEntry entry, long now, Span<ReceivedMessage> into, ref int written, in BufferLease room)
    {
        if ((entry.Flags & ReceiveFlags.Compressed) != 0 && !TryDecode(ref entry, now, in room))
        {
            return;
        }

        byte* data = entry.Lease.IsEmpty ? null : _core.GetPointer(in entry.Lease);
        into[written++] = new ReceivedMessage(MakeHeader(in entry, now), in entry.Lease, data);
    }

    /// <summary>
    /// Rents the buffer a compressed message of a reliable channel will be decoded into, before <see cref="Drain"/> takes
    /// the message (game thread).
    /// </summary>
    /// <remarks>
    /// Drain hands decoded payloads to a caller that can release nothing before the call returns, so one call can ask for
    /// more decode buffers than the receive budget holds. Dropping the message for that — as a dispatch to a handler does,
    /// which needs one buffer at a time — would lose reliable messages to the size of the caller's own span, and the loop
    /// would go on to drop the rest of the queue. So the message waits: <see langword="false"/> means there is no buffer
    /// now and the caller leaves the message where it is. A message that could never be decoded (no block of its raw
    /// size within the budget next to the block it arrived in, or no raw size at all) gets <see langword="true"/>
    /// without a buffer, and <see cref="TryDecode"/> drops and counts it as before.
    /// </remarks>
    /// <param name="entry">The message (not taken yet).</param>
    /// <param name="room">The buffer for its decoded payload, or empty when it needs none or can never have one.</param>
    /// <returns><see langword="false"/> when the message has to wait for a buffer.</returns>
    private bool TryRentForDecode(in ReceiveEntry entry, out BufferLease room)
    {
        room = BufferLease.Empty;
        int rawLength = entry.RawLength;
        if ((entry.Flags & ReceiveFlags.Compressed) == 0 || rawLength <= 0 || entry.Lease.IsEmpty)
        {
            return true;
        }

        return _core.TryRentReceive(rawLength, out room) || !_core.CanEverRentReceive(rawLength, entry.Lease.Length);
    }

    /// <summary>
    /// Decodes an LZ4-compressed message into a second receive lease of exactly <c>RawLength</c> bytes (PROTOCOL.md §2.1,
    /// §7: game thread, bounded by the decoded-bytes budget). On failure the message is dropped and counted.
    /// </summary>
    /// <param name="entry">The message; on success its lease and length are the decoded payload's.</param>
    /// <param name="now">Clock micros.</param>
    /// <param name="room">A buffer rented for the decoded payload already (<see cref="TryRentForDecode"/>), or empty.</param>
    private bool TryDecode(ref ReceiveEntry entry, long now, in BufferLease room)
    {
        BufferLease compressed = entry.Lease;
        int rawLength = entry.RawLength;
        BufferLease decoded = room;
        if (rawLength <= 0 || compressed.IsEmpty || !_decodeBucket.TryTake(now, rawLength)
            || (decoded.IsEmpty && !_core.TryRentReceive(rawLength, out decoded)))
        {
            _core.ReturnReceive(in compressed);
            _core.ReturnReceive(in room);
            _core.Counters.DecodeFailures++;
            return false;
        }

        bool ok = Lz4Block.DecompressExact(
            new ReadOnlySpan<byte>(_core.GetPointer(in compressed), entry.Length),
            new Span<byte>(_core.GetPointer(in decoded), rawLength));
        _core.ReturnReceive(in compressed);
        if (!ok)
        {
            _core.ReturnReceive(in decoded);
            _core.Counters.DecodeFailures++;
            return false;
        }

        entry.Lease = decoded;
        entry.Length = rawLength;
        return true;
    }

    private ReceiveHeader MakeHeader(in ReceiveEntry entry, long now) => new()
    {
        Channel = entry.Channel,
        Key = entry.Key,
        Sequence = entry.Sequence,
        Length = entry.Length,
        RawLength = entry.RawLength,
        ReceivedMicros = PeerCore.RestoreReceive(entry.ReceivedMicrosDelta, now),
        SenderTick = entry.SenderTick,
        Epoch = _core.Epoch,
        PeerIndex = Index,
        RequestId = entry.RequestId,
        Flags = entry.Flags,
    };

    private static bool IsSameMessage(in ReceiveHeader a, in ReceiveHeader b) =>
        a.Channel == b.Channel && a.Key == b.Key && a.Sequence == b.Sequence && a.Length == b.Length
        && a.ReceivedMicros == b.ReceivedMicros && a.RequestId == b.RequestId;

    /// <summary>
    /// Resumes streams whose receive was held back for the receive ring or the receive budget, oldest first, and no more
    /// of them than the ring has free slots: a resumed stream takes at least one, or is held back again at once, so
    /// resuming more only sends the rest round — with thousands of streams held after a long hitch, every Poll would
    /// resume all of them to let a ring's worth through. What is left stays queued, which keeps
    /// <see cref="HasPendingWork"/> set, and the next Poll goes on.
    /// </summary>
    private void ResumePendedStreams()
    {
        PeerCore core = _core;
        ITransport? transport = _transport;
        SpscRing<TransportStreamId>[]? retired = core.RetiredPendedStreams;
        SpscRing<TransportStreamId> pended = core.PendedStreams;
        if (retired is null && pended.IsEmpty)
        {
            return;
        }

        SpscRing<ReceiveEntry> ring = core.ReceiveRing;
        int budget = transport is null ? int.MaxValue : ring.Capacity - ring.Count;
        if (retired is not null)
        {
            // Rings the peer's stream capacity outgrew (PeerCore.SetTransportPeerStreams): what was held before the
            // replacement is older than anything in the current ring.
            foreach (SpscRing<TransportStreamId> old in retired)
            {
                while (budget > 0 && old.TryDequeue(out TransportStreamId id))
                {
                    budget--;
                    transport?.ResumeStreamReceive(id, 0);
                }
            }
        }

        while (budget > 0 && pended.TryDequeue(out TransportStreamId id))
        {
            budget--;
            transport?.ResumeStreamReceive(id, 0);
        }
    }

    private void FinishClosed()
    {
        DrainCompletions();
        ReadOnlySpan<ChannelEngine> engines = _core.ActiveEngines;
        for (int i = 0; i < engines.Length; i++)
        {
            engines[i].OnPeerClosed();
        }

        // After the engines, so the ranges they just finished are folded into their objects rather than counted as lost.
        AbortBulkObjects(BulkStatus.Disconnected);
        DrainCompletions();
        ReleaseForeignSends();
        ReleaseAllReceived();
        _closedRaised = true;
        CompleteSendWaiters(exception: null);
        CompleteFlushWaiters(all: true);
        RaiseTransitions(holdClosed: false);
    }

    /// <summary>
    /// Releases the promises a <see cref="Dispose"/> would otherwise leave outstanding for good (ADR 0008). Disposing
    /// without closing first is ordinary teardown — a host shutting down, a <c>using</c> block left on an exception — and no
    /// <see cref="Poll"/> will ever run <see cref="FinishClosed"/> afterwards. Every await the session layer hands out is
    /// released on this path, once, in this order: <see cref="SendAsync"/> and <see cref="FlushAsync"/> waiters were failed
    /// by <c>FailWaitersOnDispose</c> just before; then each engine releases its own, in mode order
    /// (<see cref="ChannelEngine.OnDisposing"/>: a <see cref="SendRequestAsync"/> still waiting for its response fails with
    /// <see cref="ObjectDisposedException"/>, and a bulk transfer this end is sending ends
    /// <see cref="BulkStatus.Disconnected"/>); last, every outstanding <see cref="WaitAsync"/> on a tracked send completes
    /// <see cref="DeliveryStatus.Disconnected"/> (nothing drains the completion ring after this). A <c>ForeignSendRetry</c>
    /// wait ends on its next attempt, which finds the peer disposed. A peer already polled to <see cref="PeerState.Closed"/>
    /// skips all of it: <see cref="FinishClosed"/> released the same awaits then, and a closed peer hands out no new ones.
    /// <para>
    /// This is deliberately <em>not</em> <see cref="ChannelEngine.OnPeerClosed"/>: that hook runs once the transport has
    /// reported its close, and it completes in-flight entries and returns their payloads. Here the transport is still live —
    /// it may still be reading a payload it was handed, and its thread may still be writing into a receive record — so
    /// releasing either now would hand a block the transport is using back to the pool. What the transport holds is
    /// released with the peer's memory, once the transport has reported its close (<c>FreeResources</c>).
    /// </para>
    /// </summary>
    private void FinishEnginesOnDispose()
    {
        if (_closedRaised)
        {
            // The host polled the peer to Closed already: FinishClosed did all of this.
            return;
        }

        ReadOnlySpan<ChannelEngine> engines = _core.ActiveEngines;
        for (int i = 0; i < engines.Length; i++)
        {
            engines[i].OnDisposing();
        }

        // Only the objects this end is sending. The receiving ones are assembled from records the transport thread may
        // still be writing into, which is the very reason the engines' OnDisposing leaves those records alone; they are
        // ended in FreeResources, once the transport has reported its close and BulkEngine.Dispose has finished them.
        AbortSendingBulkObjects(BulkStatus.Disconnected);
        _core.Completions.CompleteAll(Tedd.Quicly.Core.Threading.DeliveryStatus.Disconnected);
    }

    private void ReleaseAllReceived()
    {
        while (_core.ReceiveRing.TryDequeue(out ReceiveEntry entry))
        {
            _core.ReturnReceive(in entry.Lease);
        }

        if (_hasHeld)
        {
            _hasHeld = false;
            _core.ReturnReceive(in _held.Lease);
        }

        _queues.ReleaseAll(_core);
        ReadOnlySpan<ReceiveMailbox> boxes = _core.Mailboxes;
        for (int i = 0; i < boxes.Length; i++)
        {
            boxes[i].ReleaseAll(_core);
        }
    }
}
