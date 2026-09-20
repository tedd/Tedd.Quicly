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
    private readonly ReceiveQueues _queues;
    private int _queuedWithHandler;
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
        while (written < into.Length && queues.TryTake(index, out ReceiveEntry entry))
        {
            if (_handlers[index] is not null)
            {
                _queuedWithHandler--;
            }

            // A response is taken by its engine where it leaves the receive ring (below, and in Route), and that is the only
            // way into a per-channel queue or the held slot, so neither can hold one (see IsResponse).
            Debug.Assert(!IsResponse(in entry), "a response never reaches a per-channel queue");
            Emit(ref entry, now, into, ref written);
        }

        if (_hasHeld && written < into.Length)
        {
            Debug.Assert(!IsResponse(in _held), "a response never reaches the held slot");
            if (_held.Channel == channel)
            {
                ReceiveEntry entry = _held;
                _hasHeld = false;
                Emit(ref entry, now, into, ref written);
            }
            else if (TryQueue(in _held))
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
                Emit(ref entry, now, into, ref written);
            }
            else if (!TryQueue(in entry))
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
                        Emit(ref entry, now, into, ref written);
                    }
                }
            }
        }

        return written;
    }

    /// <summary>Returns the payloads of drained messages to the pool (game thread). Each message must be released exactly once.</summary>
    /// <param name="messages">Messages from <see cref="Drain"/>.</param>
    public void Release(ReadOnlySpan<ReceivedMessage> messages)
    {
        if (_disposed)
        {
            return;
        }

        for (int i = 0; i < messages.Length; i++)
        {
            _core.ReturnReceive(in messages[i].Lease);
        }
    }

    /// <summary>Returns a retained payload to the pool (game thread). Must be called exactly once per lease.</summary>
    /// <param name="lease">A lease from <see cref="Retain"/>.</param>
    public void Release(in ReceiveLease lease)
    {
        if (!_disposed)
        {
            _core.ReturnReceive(in lease.Lease);
        }
    }

    /// <summary>
    /// Keeps the payload of the message being handled beyond its handler. Only valid inside a <see cref="MessageHandler"/>
    /// for the header it was given; release the lease with <see cref="Release(in ReceiveLease)"/>.
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
        _queuedWithHandler += _queues.Count(index);
    }

    /// <summary>Removes the handler of a channel; its messages then wait for <see cref="Drain"/> (game thread).</summary>
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
        _queuedWithHandler -= _queues.Count(index);
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
        if (_queuedWithHandler > 0)
        {
            dispatched = DispatchQueued(maxItems, now);
        }

        if (_hasHeld && dispatched < maxItems && !_disposed)
        {
            ReceiveEntry held = _held;
            _hasHeld = false;
            if (!Route(ref held, now, ref dispatched))
            {
                _held = held;
                _hasHeld = true;
                return dispatched;
            }
        }

        SpscRing<ReceiveEntry> ring = _core.ReceiveRing;
        while (dispatched < maxItems && !_disposed && !_hasHeld && ring.TryDequeue(out ReceiveEntry entry))
        {
            if (!Route(ref entry, now, ref dispatched))
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

    private bool Route(ref ReceiveEntry entry, long now, ref int dispatched)
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
            Dispatch(handler, ref entry, now);
            dispatched++;
            return true;
        }

        if (index < 0)
        {
            _core.ReturnReceive(in entry.Lease);
            return true;
        }

        return TryQueue(in entry);
    }

    private bool TryQueue(in ReceiveEntry entry)
    {
        int index = _core.ChannelIndexOf(entry.Channel);
        ReceiveQueues queues = _queues;
        if (!queues.TryAppend(index, in entry))
        {
            return false;
        }

        if (_handlers[index] is not null)
        {
            _queuedWithHandler++;
        }

        return true;
    }

    private int DispatchQueued(int maxItems, long now)
    {
        int dispatched = 0;
        ReceiveQueues queues = _queues;
        for (int index = 0; index < _handlers.Length && _queuedWithHandler > 0 && dispatched < maxItems && !_disposed; index++)
        {
            MessageHandler? handler = _handlers[index];
            while (handler is not null && dispatched < maxItems && !_disposed && queues.TryTake(index, out ReceiveEntry entry))
            {
                _queuedWithHandler--;
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
        if ((entry.Flags & ReceiveFlags.Compressed) != 0 && !TryDecode(ref entry, now))
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
        if ((entry.Flags & ReceiveFlags.Compressed) != 0 && !TryDecode(ref entry, now))
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

    private void Emit(ref ReceiveEntry entry, long now, Span<ReceivedMessage> into, ref int written)
    {
        if ((entry.Flags & ReceiveFlags.Compressed) != 0 && !TryDecode(ref entry, now))
        {
            return;
        }

        byte* data = entry.Lease.IsEmpty ? null : _core.GetPointer(in entry.Lease);
        into[written++] = new ReceivedMessage(MakeHeader(in entry, now), in entry.Lease, data);
    }

    /// <summary>
    /// Decodes an LZ4-compressed message into a second receive lease of exactly <c>RawLength</c> bytes (PROTOCOL.md §2.1,
    /// §7: game thread, bounded by the decoded-bytes budget). On failure the message is dropped and counted.
    /// </summary>
    private bool TryDecode(ref ReceiveEntry entry, long now)
    {
        BufferLease compressed = entry.Lease;
        int rawLength = entry.RawLength;
        if (rawLength <= 0 || compressed.IsEmpty || !_decodeBucket.TryTake(now, rawLength) || !_core.TryRentReceive(rawLength, out BufferLease decoded))
        {
            _core.ReturnReceive(in compressed);
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

    private void ResumePendedStreams()
    {
        SpscRing<TransportStreamId> pended = _core.PendedStreams;
        while (pended.TryDequeue(out TransportStreamId id))
        {
            _transport?.ResumeStreamReceive(id, 0);
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
        _queuedWithHandler = 0;
        ReadOnlySpan<ReceiveMailbox> boxes = _core.Mailboxes;
        for (int i = 0; i < boxes.Length; i++)
        {
            boxes[i].ReleaseAll(_core);
        }
    }
}
