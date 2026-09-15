using Tedd.Quicly.Core.Session.Engines;

namespace Tedd.Quicly.Core.Session;

// Game thread: Flush (docs/design/session-layer.md §4.2) and NextDeadline bookkeeping.
public sealed unsafe partial class QuiclyPeer
{
    private bool _inFlush;
    private long _engineDeadline = long.MaxValue;

    /// <summary>
    /// Transmits everything buffered and runs time-driven work (game thread, clock read once): drains transport
    /// completions, runs the peer timers (pings, heartbeat, admission timeout, close linger), then — while
    /// <see cref="PeerState.Connected"/> — every engine's <c>Tick</c> and the scheduler. Allocation-free in steady state.
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
        try
        {
            long now = _clock.NowMicros;
            DrainCompletions();
            long next = RunTimers(now);
            long engineDeadline = long.MaxValue;
            if (_state == PeerState.Connected)
            {
                FlushContext flush = new()
                {
                    NowMicros = now,
                    Tick = tick,
                    MaxDatagramPayload = _core.MaxDatagramPayload,
                    DatagramsEnabled = _core.DatagramsEnabled,
                    NextDeadline = long.MaxValue,
                };
                ReadOnlySpan<ChannelEngine> engines = _core.ActiveEngines;
                for (int i = 0; i < engines.Length; i++)
                {
                    engines[i].Tick(now, ref flush.NextDeadline);
                }

                FlushEngines(ref flush);
                engineDeadline = flush.NextDeadline;
            }

            _engineDeadline = engineDeadline;
            UpdateNextDeadline(next);
        }
        finally
        {
            _inFlush = false;
            if (_disposed)
            {
                TryFreeAfterDispose();
            }
        }
    }

    /// <summary>
    /// Flushes and completes (game thread). Everything admitted before the call has been handed to the transport when the
    /// returned task completes; with the engines of this wave that happens synchronously.
    /// </summary>
    /// <param name="cancellationToken">Cancels the wait (not the flush).</param>
    /// <returns>A task that completes when the flush is done.</returns>
    public ValueTask FlushAsync(CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return ValueTask.FromCanceled(cancellationToken);
        }

        Flush();
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// The scheduler seam: submits the engines' queued work. This wave calls every engine once, in mode order; the datagram
    /// wave replaces it with the priority scheduler (channel priority order precomputed, admission order within a channel,
    /// expiry at scheduling time, token bucket) and the per-peer packer.
    /// </summary>
    /// <param name="flush">The flush context.</param>
    private void FlushEngines(ref FlushContext flush)
    {
        ReadOnlySpan<ChannelEngine> engines = _core.ActiveEngines;
        for (int i = 0; i < engines.Length; i++)
        {
            engines[i].Flush(ref flush);
        }
    }

    private void UpdateNextDeadline(long timerDeadline)
    {
        long engineDeadline = _state == PeerState.Connected ? _engineDeadline : long.MaxValue;
        _nextDeadlineMicros = Math.Min(timerDeadline, engineDeadline);
    }
}
