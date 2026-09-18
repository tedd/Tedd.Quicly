using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.Threading;

namespace Tedd.Quicly.Core.Session;

/// <summary>What <see cref="QuiclyPeer.CanSkipFlush"/> found: flush, skip, or drain the completion ring first.</summary>
internal enum FlushGateDecision : byte
{
    /// <summary>A Flush has work: run it.</summary>
    Flush,

    /// <summary>A Flush would do nothing: <see cref="QuiclyPeer.SkipFlush"/> stands in for it.</summary>
    Skip,

    /// <summary>Only transport completions are waiting: <see cref="QuiclyPeer.DrainForFlushGate"/>, then ask again.</summary>
    Drain,
}

// Game thread: the flush gate of a host that flushes many peers per tick (QuiclyServer.FlushAll). A Flush of an idle peer
// costs about a microsecond of cache misses (docs/benchmarks); the gate proves from a handful of levels that the Flush would
// do nothing and records what it would still have recorded instead.
public sealed unsafe partial class QuiclyPeer
{
    private FlushGate _gate;
    private ReliableOrderedEngine? _gateOrdered;

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
            else if (engines[i] is ReliableOrderedEngine ordered)
            {
                // A canceled request keeps its slot until a pass sweeps it (RunPollDeadlines); the cancel itself is invisible
                // to every other level.
                _gateOrdered = ordered;
            }
        }

        foreach (ChannelDefinition channel in _core.Table.All)
        {
            // The reassembly window (2 x RTT + grace) is refreshed by the datagram engine's Tick.
            _gate.RttMatters |= channel.Fragmentation;
        }

        _gate.Enabled = allowed;
    }

    /// <summary>
    /// Whether a <see cref="Flush"/> at <paramref name="now"/> would find nothing to do (game thread; <paramref name="now"/>
    /// is the peer's clock, read by the caller for a batch of peers). <see cref="FlushGateDecision.Skip"/> only when the last
    /// Flush's scheduler pass left every engine empty and since then: nothing was admitted, no completion reached an engine
    /// outside a Flush (a Poll, an Immediate pass), no local completion, send from another thread or waiting
    /// <see cref="SendAsync"/>/<see cref="FlushAsync"/> is pending, no deadline (<see cref="NextDeadlineMicros"/>: timers
    /// and engine work) is due, the session is still Connected with no transition queued, the calling thread is the game
    /// thread already (<see cref="PeerOptions.ThreadSafeSend"/> records it in Flush), no ack, reject or notice the transport
    /// thread left for a ReliableLatest pass is waiting, no canceled request waits for the pass that frees its slot, the RTT a fragmenting channel's reassembly window follows has not
    /// moved, and the transport's completion ring is empty. When the ring is the only thing waiting the answer is
    /// <see cref="FlushGateDecision.Drain"/>: most completions of an idle peer are its own control traffic (pings), which
    /// touch no engine.
    /// </summary>
    /// <param name="now">Clock micros.</param>
    /// <param name="threadId">The calling thread's managed id.</param>
    /// <returns>What the caller does with the peer.</returns>
    internal FlushGateDecision CanSkipFlush(long now, int threadId)
    {
        PeerCore core = _core;
        if (!_gate.Quiescent
            || _gate.Touched
            || core.LastAdmissionStamp != _gate.Stamp
            || now >= _nextDeadlineMicros
            || _state != PeerState.Connected
            || _transitionCount != 0
            || _inFlush
            || core.LocalCompletionsQueued != 0
            || _hasHeldForeign
            || _front is { IsEmpty: false }
            || (_threadSafeSend && Volatile.Read(ref _gameThreadId) != threadId)
            || _sendWaiters.Count != 0
            || _flushWaiters.Count != 0
            || (_latest is not null && _latest.HasUnsentControl)
            || (_gateOrdered is not null && _gateOrdered.HasCanceledRequests)
            || (_gate.RttMatters && _ping.SmoothedRtt != _gate.Rtt))
        {
            return FlushGateDecision.Flush;
        }

        return core.CompletionRing.IsEmpty ? FlushGateDecision.Skip : FlushGateDecision.Drain;
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

    /// <summary>
    /// The first step of a Flush on its own (game thread, <see cref="FlushGateDecision.Drain"/>): the tick and the pass
    /// clock recorded, the transport's completions routed exactly as a Flush routes them first. A completion that reached an
    /// engine (or a container, or a local one) marks the gate, so the caller's next <see cref="CanSkipFlush"/> asks for the
    /// rest of the Flush; the peer's own control traffic does not.
    /// </summary>
    /// <param name="tick">The tick passed to the flush.</param>
    /// <param name="now">Clock micros.</param>
    internal void DrainForFlushGate(uint tick, long now)
    {
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
            _core.NotePass(now);
            DrainCompletionsMarking();
        }
        finally
        {
            _inFlush = false;
            ExitCall();
        }
    }

    /// <summary>
    /// <see cref="DrainCompletions"/> outside a Flush's pass (Poll, <see cref="DrainForFlushGate"/>): marks the gate when a
    /// completion is routed anywhere but the peer's own control traffic, because an engine may re-queue or re-arm on it.
    /// </summary>
    private void DrainCompletionsMarking()
    {
        SpscRing<CompletionEntry> ring = _core.CompletionRing;
        while (ring.TryDequeue(out CompletionEntry completion))
        {
            if (_core.Entries[completion.Slot].Channel != PeerCore.ControlChannelId)
            {
                _gate.Touched = true;
            }

            RouteCompletion(in completion);
        }

        if (_core.LocalCompletionsQueued != 0)
        {
            _gate.Touched = true;
            DrainLocalCompletions();
        }
    }

    /// <summary>Start of a Flush: nothing is proved yet.</summary>
    private void OpenFlushGate() => _gate.Quiescent = false;

    /// <summary>
    /// Just before a Flush's scheduler pass: everything admitted, routed or measured up to here is this pass's to serve (no
    /// continuation runs before the pass ends).
    /// </summary>
    private void NoteFlushGatePass()
    {
        _gate.Stamp = _core.LastAdmissionStamp;
        _gate.Touched = false;
        _gate.Rtt = _ping.SmoothedRtt;
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

    /// <summary>What the last Flush proved.</summary>
    private struct FlushGate
    {
        /// <summary><see cref="PeerCore.LastAdmissionStamp"/> when the last Flush's pass began: admissions after it are unseen.</summary>
        public long Stamp;

        /// <summary><see cref="QuiclyPeer.ApplicationRttMicros"/> when the last Flush's pass began.</summary>
        public long Rtt;

        /// <summary>The host turned the gate on (<see cref="EnableFlushGate"/>) and nothing rules it out.</summary>
        public bool Enabled;

        /// <summary>The last Flush's pass left every engine empty (reset when a Flush starts).</summary>
        public bool Quiescent;

        /// <summary>
        /// Engine state may have changed outside a Flush's pass since the last one began: a completion routed to an engine,
        /// a container or a local completion by a Poll, an Immediate pass or the end of a pass; a Poll nested in a Flush.
        /// </summary>
        public bool Touched;

        /// <summary>A channel fragments, so the RTT feeds the datagram engine's Tick.</summary>
        public bool RttMatters;
    }
}
