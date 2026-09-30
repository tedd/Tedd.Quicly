using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Session;

// Game thread: Flush (docs/design/session-layer.md §4.2), the scheduler (§7.1) and NextDeadline bookkeeping.
public sealed unsafe partial class QuiclyPeer
{
    /// <summary>Send caps at or above this many bytes per second count as unlimited (keeps the token arithmetic in range).</summary>
    internal const long MaxSendRateBytesPerSecond = 2_000_000_000;

    private readonly List<FlushWaiter> _flushWaiters = [];
    private readonly List<FlushWaiter> _readyFlushWaiters = [];
    private bool _completingFlushWaiters;
    private bool _inFlush;
    private bool _inScheduler;
    private uint _lastTick;
    private long _engineDeadline = long.MaxValue;
    private TokenBucket _sendBucket;
    private long _sendRate;

    /// <summary>
    /// Transmits everything buffered and runs time-driven work (game thread, clock read once): drains transport
    /// completions, runs the peer timers (pings, heartbeat, admission timeout, close linger), then — while
    /// <see cref="PeerState.Connected"/> — every engine's <c>Tick</c> and the scheduler: every channel in priority order
    /// (admission order within a channel, expiry at scheduling time, the <see cref="PeerOptions.MaxSendBytesPerSecond"/>
    /// cap), with buffered datagram messages packed into containers whenever at least two fit (PROTOCOL.md §2.2, §4.5).
    /// Allocation-free in steady state.
    /// </summary>
    /// <param name="tick">The host's simulation tick, carried in packed containers (PROTOCOL.md §2.2); 0 = none.</param>
    /// <exception cref="ObjectDisposedException">The peer is disposed.</exception>
    public void Flush(uint tick = 0)
    {
        ThrowIfDisposed();
        if (_closedRaised || _inFlush)
        {
            return;
        }

        _inFlush = true;
        _lastTick = tick;
        EnterCall();
        NoteGameThread();
        try
        {
            long now = _clock.NowMicros;
            _core.NotePass(now);
            OpenFlushGate();
            DrainCompletions();
            DrainForeignSends();
            RetrySendWaiters();
            long next = RunTimers(now);
            long engineDeadline = long.MaxValue;
            if (_state == PeerState.Connected)
            {
                FlushContext flush = NewFlushContext(now, tick);
                ReadOnlySpan<ChannelEngine> engines = _core.ActiveEngines;
                for (int i = 0; i < engines.Length; i++)
                {
                    engines[i].Tick(now, ref flush.NextDeadline);
                }

                // Everything admitted or routed up to here is this pass's to serve (no continuation runs before it ends).
                NoteFlushGatePass();
                FlushEngines(ref flush);
                engineDeadline = flush.NextDeadline;
                CloseFlushGate();
            }

            _engineDeadline = engineDeadline;
            UpdateNextDeadline(next);
            CompleteFlushWaiters(all: false);
        }
        finally
        {
            _inFlush = false;
            ExitCall();
        }
    }

    /// <summary>
    /// Flushes, then completes when everything admitted before the call has been handed to the transport (game thread): a
    /// watermark over the admission stamps of the messages the engines still hold queued
    /// (<see cref="ChannelEngine.OldestQueuedStamp"/>). Messages dropped instead (expired, canceled, failed) count as done.
    /// When nothing is held back (no send cap in the way, streams open) the returned task is already complete and nothing is
    /// allocated; otherwise it completes inside the later <see cref="Flush"/> (or Immediate send) whose pass hands the last
    /// of them over, or when the session closes. Continuations run there with <see cref="CompletionMode.PollOnly"/> and on
    /// the thread pool with <see cref="CompletionMode.ThreadPool"/>. Sends queued from other threads
    /// (<see cref="PeerOptions.ThreadSafeSend"/>) count once the game thread admitted them.
    /// </summary>
    /// <param name="cancellationToken">Cancels the wait (not the flush).</param>
    /// <returns>A task that completes when everything admitted before the call was handed to the transport.</returns>
    /// <exception cref="ObjectDisposedException">The peer is disposed (also fails a waiting call).</exception>
    public ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return ValueTask.FromCanceled(cancellationToken);
        }

        Flush();
        long mark = _core.LastAdmissionStamp;
        if (_closedRaised || OldestQueuedStamp() > mark)
        {
            return ValueTask.CompletedTask;
        }

        FlushWaiter waiter = new(mark, _core.CompletionMode == CompletionMode.ThreadPool);
        if (cancellationToken.CanBeCanceled)
        {
            waiter.Registration = cancellationToken.UnsafeRegister(static (state, token) => ((FlushWaiter)state!).Source.TrySetCanceled(token), waiter);
        }

        _flushWaiters.Add(waiter);
        return new ValueTask(waiter.Source.Task);
    }

    /// <summary>The admission stamp of the oldest message any engine still holds queued (<see cref="long.MaxValue"/> when none).</summary>
    private long OldestQueuedStamp()
    {
        long oldest = long.MaxValue;
        ReadOnlySpan<ChannelEngine> engines = _core.ActiveEngines;
        for (int i = 0; i < engines.Length; i++)
        {
            long stamp = engines[i].OldestQueuedStamp();
            if (stamp < oldest)
            {
                oldest = stamp;
            }
        }

        return oldest;
    }

    /// <summary>
    /// Completes the <see cref="FlushAsync"/> calls whose messages have all been handed over (after every scheduler pass), or
    /// every one (<paramref name="all"/>: the session closed and the engines finished their queues). Canceled waits are
    /// dropped. The list is settled before any continuation runs.
    /// </summary>
    private void CompleteFlushWaiters(bool all)
    {
        List<FlushWaiter> waiters = _flushWaiters;
        if (waiters.Count == 0 || _completingFlushWaiters)
        {
            return;
        }

        _completingFlushWaiters = true;
        try
        {
            long oldest = all ? long.MaxValue : OldestQueuedStamp();
            List<FlushWaiter> ready = _readyFlushWaiters;
            int kept = 0;
            for (int i = 0; i < waiters.Count; i++)
            {
                FlushWaiter waiter = waiters[i];
                if (waiter.Mark < oldest || waiter.Source.Task.IsCompleted)
                {
                    ready.Add(waiter);
                }
                else
                {
                    waiters[kept++] = waiter;
                }
            }

            waiters.RemoveRange(kept, waiters.Count - kept);
            for (int i = 0; i < ready.Count; i++)
            {
                FlushWaiter waiter = ready[i];
                waiter.Registration.Dispose();
                waiter.Source.TrySetResult();
            }

            ready.Clear();
        }
        finally
        {
            _completingFlushWaiters = false;
        }
    }

    private void FailFlushWaiters(Exception exception)
    {
        List<FlushWaiter> waiters = _flushWaiters;
        for (int i = 0; i < waiters.Count; i++)
        {
            waiters[i].Registration.Dispose();
            waiters[i].Source.TrySetException(exception);
        }

        waiters.Clear();
    }

    /// <summary>A <see cref="FlushAsync"/> waiting for its watermark (allocated only when the flush could not hand everything over).</summary>
    private sealed class FlushWaiter(long mark, bool asynchronous)
    {
        public readonly long Mark = mark;
        public readonly TaskCompletionSource Source = new(asynchronous ? TaskCreationOptions.RunContinuationsAsynchronously : TaskCreationOptions.None);
        public CancellationTokenRegistration Registration;
    }

    private FlushContext NewFlushContext(long now, uint tick) => new()
    {
        NowMicros = now,
        Tick = tick,
        MaxDatagramPayload = _core.MaxDatagramPayload,
        DatagramsEnabled = _core.DatagramsEnabled,
        NextDeadline = long.MaxValue,
    };

    /// <summary>
    /// Sets up the send cap (constructor): a token bucket of <paramref name="maxSendBytesPerSecond"/> bytes per second whose
    /// burst is two flush intervals' worth (the interval clamped to 10 ms … 500 ms).
    /// </summary>
    private void InitializeScheduler(long maxSendBytesPerSecond, long now)
    {
        if (maxSendBytesPerSecond <= 0 || maxSendBytesPerSecond >= MaxSendRateBytesPerSecond)
        {
            return;
        }

        _sendRate = maxSendBytesPerSecond;
        long window = 2 * Math.Clamp(_core.FlushIntervalMicros, 10_000, 500_000);
        _sendBucket.Initialize(maxSendBytesPerSecond, Math.Max(1, maxSendBytesPerSecond * window / 1_000_000), now);
    }

    /// <summary>
    /// The scheduler (docs/design/session-layer.md §7.1), one pass: the expiry deadlines of everything admitted since the
    /// previous pass are anchored at this pass's clock (<see cref="PeerCore.ResolveExpiry"/>, so this pass never expires
    /// them); the send cap is refilled; every channel is offered to
    /// its engine in priority order (<see cref="PeerCore.ScheduleOrder"/>; within a channel admission order, expiry at
    /// scheduling time, PROTOCOL.md §4.5); every engine's engine-level <see cref="ChannelEngine.Flush"/> follows (retries and
    /// other traffic §4.5 schedules after fresh messages); the packer submits what it still holds. The bytes submitted are
    /// charged to the cap. Completions the pass queued (expired, refused) are routed after it, so no continuation runs
    /// inside a pass; a pass requested while one runs (an Immediate send from such a continuation) does nothing.
    /// </summary>
    /// <param name="flush">The pass.</param>
    private void FlushEngines(ref FlushContext flush)
    {
        if (_inScheduler)
        {
            return;
        }

        _inScheduler = true;
        try
        {
            long now = flush.NowMicros;
            // Expiry runs from a message's first pass (PROTOCOL.md §4.5): what was admitted since the previous pass gets
            // its deadline from this pass's clock, before any engine compares one.
            _core.ResolveExpiry(now);
            flush.BudgetBytes = _sendRate > 0 ? _sendBucket.Available(now) : long.MaxValue;
            flush.CancelBlockedDatagrams = TransportHonoursCancelOnBlocked();
            DatagramPacker packer = _core.Packer;
            packer.Begin(in flush);
            ReadOnlySpan<int> order = _core.ScheduleOrder;
            for (int i = 0; i < order.Length; i++)
            {
                int channelIndex = order[i];
                _core.GetEngine(channelIndex).FlushChannel(channelIndex, ref flush);
            }

            ReadOnlySpan<ChannelEngine> engines = _core.ActiveEngines;
            for (int i = 0; i < engines.Length; i++)
            {
                engines[i].Flush(ref flush);
            }

            packer.Finish(ref flush);
            if (_sendRate > 0)
            {
                _sendBucket.Consume(flush.BytesSubmitted);
                if (flush.BudgetExhausted)
                {
                    flush.NextDeadline = Math.Min(flush.NextDeadline, now + Math.Max(1, _sendBucket.MicrosUntil(1)));
                }
            }
        }
        finally
        {
            _inScheduler = false;
        }

        if (_core.LocalCompletionsQueued != 0)
        {
            // Routed after the pass computed its deadlines (an engine may re-queue or re-arm on them): the next FlushAll
            // must not skip this peer (QuiclyPeer.FlushGate.cs).
            _gate.Touched = true;
        }

        DrainLocalCompletions();
    }

    /// <summary>
    /// One scheduler pass at the end of an <see cref="SendMode.Immediate"/> send (PROTOCOL.md §4.5: eligible now, together
    /// with whatever is already buffered for the peer). The engines' <c>Tick</c> does not run (time-driven work belongs to
    /// Flush and Poll); containers carry the tick last passed to <see cref="Flush"/>.
    /// </summary>
    private void FlushImmediate()
    {
        // Only reached from an admitted send, so the peer is Connected; a pass already running makes FlushEngines a no-op.
        EnterCall();
        try
        {
            long now = _clock.NowMicros;
            _core.NotePass(now);
            FlushContext flush = NewFlushContext(now, _lastTick);
            FlushEngines(ref flush);
            if (flush.NextDeadline < _engineDeadline)
            {
                _engineDeadline = flush.NextDeadline;
                if (flush.NextDeadline < _nextDeadlineMicros)
                {
                    _nextDeadlineMicros = flush.NextDeadline;
                }
            }

            CompleteFlushWaiters(all: false);
        }
        finally
        {
            ExitCall();
        }
    }

    /// <summary>
    /// PROTOCOL.md §4.5, <see cref="PeerOptions.DropWhenBlocked"/>: unreliable datagrams go out with
    /// <see cref="TransportSendFlags.CancelOnBlocked"/> (a datagram the transport cannot send at once is dropped instead of
    /// queueing behind congestion; its messages complete <c>Expired</c> and are counted as transport-cancelled) only when the
    /// option is on and the transport reports that it honours the flag: <see cref="TransportCapabilities.CancelOnBlocked"/>,
    /// recorded in <see cref="PeerCore"/> at <c>OnConnected</c> and whenever the datagram capability changes. Otherwise
    /// blocked datagrams are queued by the transport.
    /// </summary>
    /// <returns>Whether unreliable datagrams may carry <see cref="TransportSendFlags.CancelOnBlocked"/>.</returns>
    private bool TransportHonoursCancelOnBlocked() => _dropWhenBlocked && _core.CancelOnBlockedHonoured;

    private void UpdateNextDeadline(long timerDeadline)
    {
        long engineDeadline = _state == PeerState.Connected ? _engineDeadline : long.MaxValue;
        _nextDeadlineMicros = Math.Min(timerDeadline, engineDeadline);
    }
}
