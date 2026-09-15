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
    private ReceiveQueues? _queues;
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
        EnterCall();
        NoteGameThread();
        int dispatched = 0;
        try
        {
            long now = _clock.NowMicros;
            DrainCompletions();
            bool immediate = DrainForeignSends();
            RetrySendWaiters();
            if (immediate && _state == PeerState.Connected)
            {
                // An Immediate send from another thread: its scheduler pass runs here, on the game thread.
                FlushImmediate();
            }

            ProcessSignals(now);
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
        ReceiveQueues? queues = _queues;
        if (queues is not null)
        {
            while (written < into.Length && queues.TryTake(index, out ReceiveEntry entry))
            {
                if (_handlers[index] is not null)
                {
                    _queuedWithHandler--;
                }

                Emit(ref entry, now, into, ref written);
            }
        }

        if (_hasHeld && written < into.Length)
        {
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
            if (entry.Channel == channel)
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
        if (_queues is not null)
        {
            _queuedWithHandler += _queues.Count(index);
        }
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
        if (_queues is not null)
        {
            _queuedWithHandler -= _queues.Count(index);
        }

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
        ReceiveQueues queues = _queues ??= new ReceiveQueues(_core.ReceiveRing.Capacity, _core.ChannelCount);
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
        ReceiveQueues queues = _queues!;
        for (int index = 0; index < _handlers.Length && _queuedWithHandler > 0 && dispatched < maxItems && !_disposed; index++)
        {
            MessageHandler? handler = _handlers[index];
            while (handler is not null && dispatched < maxItems && !_disposed && queues.TryTake(index, out ReceiveEntry entry))
            {
                _queuedWithHandler--;
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

        DrainCompletions();
        ReleaseForeignSends();
        ReleaseAllReceived();
        _closedRaised = true;
        CompleteSendWaiters(exception: null);
        CompleteFlushWaiters(all: true);
        RaiseTransitions(holdClosed: false);
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

        _queues?.ReleaseAll(_core);
        _queuedWithHandler = 0;
        ReadOnlySpan<ReceiveMailbox> boxes = _core.Mailboxes;
        for (int i = 0; i < boxes.Length; i++)
        {
            boxes[i].ReleaseAll(_core);
        }
    }
}
