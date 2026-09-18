using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session.Engines;

namespace Tedd.Quicly.Core.Session;

// Game thread: the flush gate of a host that flushes many peers per tick (QuiclyServer.FlushAll). A Flush of an idle peer
// costs about a microsecond of cache misses (docs/benchmarks); the gate proves from a handful of levels that the Flush would
// do nothing and records what it would still have recorded instead.
public sealed unsafe partial class QuiclyPeer
{
    private FlushGate _gate;
    private ReliableLatestEngine? _gateLatest;

    /// <summary>
    /// Turns the flush gate on (host, game thread, once; the server when it activates the peer). From then on every
    /// <see cref="Flush"/> records what its scheduler pass saw, which is what <see cref="CanSkipFlush"/> compares against.
    /// The gate stays off (every Flush runs) for a peer with work no level shows: a Bulk engine (its transfers are driven
    /// by the pass itself: windows, progress, the transport's statistics), or a stream idle sweep shorter than two ping
    /// intervals (a stream the transport thread arms is otherwise only seen by the next timer pass, which the ping schedule
    /// bounds).
    /// </summary>
    internal void EnableFlushGate()
    {
        if (_gate.Enabled)
        {
            return;
        }

        bool allowed = _streamIdleMicros <= 0 || _streamIdleMicros / 2 >= _pingIntervalMicros;
        ReadOnlySpan<ChannelEngine> engines = _core.ActiveEngines;
        for (int i = 0; i < engines.Length; i++)
        {
            if (engines[i].Mode == ChannelMode.Bulk)
            {
                allowed = false;
            }
            else if (engines[i] is ReliableLatestEngine latest)
            {
                _gateLatest = latest;
            }
        }

        _gate.Enabled = allowed;
    }

    /// <summary>
    /// Whether a <see cref="Flush"/> at <paramref name="now"/> would find nothing to do (game thread; <paramref name="now"/>
    /// is the peer's clock, read by the caller for a batch of peers). True only when the last Flush's scheduler pass left
    /// every engine empty and since then: nothing was admitted, no Poll ran (a Poll drains completions, raises transitions
    /// and runs handlers), no completion, send from another thread or waiting <see cref="SendAsync"/>/<see cref="FlushAsync"/>
    /// is pending, no deadline (<see cref="NextDeadlineMicros"/>: timers and engine work) is due, the session is still
    /// Connected with no transition queued, the calling thread is the game thread already (<see cref="PeerOptions.ThreadSafeSend"/>
    /// records it in Flush), and no ack, reject or notice the transport thread left for a ReliableLatest pass is waiting.
    /// </summary>
    /// <param name="now">Clock micros.</param>
    /// <param name="threadId">The calling thread's managed id.</param>
    /// <returns><see langword="true"/> when <see cref="SkipFlush"/> may stand in for the Flush.</returns>
    internal bool CanSkipFlush(long now, int threadId)
    {
        PeerCore core = _core;
        return _gate.Quiescent
            && !_gate.Polled
            && core.LastAdmissionStamp == _gate.Stamp
            && now < _nextDeadlineMicros
            && _state == PeerState.Connected
            && _transitionCount == 0
            && !_inFlush
            && core.LocalCompletionsQueued == 0
            && core.CompletionRing.IsEmpty
            && !_hasHeldForeign
            && _front is null or { IsEmpty: true }
            && (!_threadSafeSend || Volatile.Read(ref _gameThreadId) == threadId)
            && _sendWaiters.Count == 0
            && _flushWaiters.Count == 0
            && (_gateLatest is null || !_gateLatest.HasUnsentControl);
    }

    /// <summary>
    /// Stands in for a <see cref="Flush"/> that <see cref="CanSkipFlush"/> proved empty (game thread): records the tick
    /// (carried by the containers of later Immediate sends), the pass clock (engines stamp expiry deadlines from it) and the
    /// ping clock's slew, which are the only things such a Flush changes.
    /// </summary>
    /// <param name="tick">The tick passed to the flush.</param>
    /// <param name="now">The clock micros <see cref="CanSkipFlush"/> was given.</param>
    internal void SkipFlush(uint tick, long now)
    {
        _lastTick = tick;
        _core.NotePass(now);
        _ping.Advance(now, now >= _fastLockEndMicros);
    }

    /// <summary>Start of a Flush: whatever a Poll changed is this Flush's to serve, and nothing is proved yet.</summary>
    private void OpenFlushGate()
    {
        _gate.Polled = false;
        _gate.Quiescent = false;
    }

    /// <summary>After a scheduler pass (Flush only): whether it left every engine empty.</summary>
    private void CloseFlushGate()
    {
        if (!_gate.Enabled)
        {
            return;
        }

        ReadOnlySpan<ChannelEngine> engines = _core.ActiveEngines;
        for (int i = 0; i < engines.Length; i++)
        {
            if (engines[i].OldestQueuedStamp() != long.MaxValue)
            {
                return;
            }
        }

        _gate.Quiescent = true;
    }

    /// <summary>What the last Flush proved (one line of the peer, next to nothing else the probe reads).</summary>
    private struct FlushGate
    {
        /// <summary><see cref="PeerCore.LastAdmissionStamp"/> when the last Flush's pass began: admissions after it are unseen.</summary>
        public long Stamp;

        /// <summary>The host turned the gate on (<see cref="EnableFlushGate"/>) and nothing rules it out.</summary>
        public bool Enabled;

        /// <summary>The last Flush's pass left every engine empty (reset when a Flush starts).</summary>
        public bool Quiescent;

        /// <summary>A Poll ran, or a pass routed completions after computing its deadlines, since the last Flush started.</summary>
        public bool Polled;
    }
}
