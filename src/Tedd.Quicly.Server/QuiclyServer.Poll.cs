using System.Numerics;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Server;

// Game thread: PollAll (work bits, split poll/flush deadlines, session expiry, queued events, auto flush) and FlushAll.
public sealed partial class QuiclyServer
{
    private readonly List<SessionEndInfo> _endedSessions = [];
    private int _endedHead;
    private bool _inPollAll;
    private int _pollCursor;
    private long _nextAutoFlush;
    private long _pollAllCalls;
    private long _peersPolled;
    private long _earliestFlushDeadline = long.MaxValue;

    /// <summary>
    /// Test seam: replaces the peers' <see cref="QuiclyPeer.Poll"/> inside PollAll, and with it the
    /// <see cref="QuiclyPeer.HasPendingWork"/> probe that decides whether a marked peer is polled at all (the override
    /// decides what a poll finds, so the peer's own level says nothing about it).
    /// </summary>
    internal Func<QuiclyPeer, int, int>? PollOverride { get; set; }

    /// <summary>
    /// Clock micros (<see cref="PeerOptions.Clock"/>) of the earliest deadline <see cref="PollAll"/> has to serve: the
    /// peers' own timers (ping, heartbeat, admission timeout, close linger, stream idle sweep) and the sessions waiting for
    /// a resume. <see cref="long.MaxValue"/> when nothing is scheduled. A host that sleeps between passes sleeps its polling
    /// loop until this time (and wakes early on <see cref="PeerOptions.WorkSignal"/>, which the server turns into a marked
    /// slot); <see cref="QuiclyPeer.NextFlushDeadlineMicros"/> work is <em>not</em> in here, because Poll cannot serve it.
    /// </summary>
    public long NextPollDeadlineMicros => Math.Min(_earliestDeadline, _sessions.EarliestExpiry);

    /// <summary>
    /// Clock micros of the earliest deadline only a <see cref="FlushAll"/> can serve: the peers' engine work (retries,
    /// expiry, a send cap's refill time) and the <see cref="PeerOptions.AutoFlushInterval"/> schedule, or
    /// <see cref="long.MaxValue"/>. <see cref="PollAll"/> brings its own flush forward to this time, so a host that only
    /// calls PollAll needs it for nothing; a host that flushes on its own tick brings that flush forward instead of waiting
    /// the tick out.
    /// </summary>
    public long NextFlushDeadlineMicros =>
        _autoFlushMicros > 0 ? Math.Min(_earliestFlushDeadline, _nextAutoFlush) : _earliestFlushDeadline;

    /// <summary>
    /// Runs the game-thread side of every peer that has something to do (game thread): activates newly accepted connections,
    /// applies admission decisions, then polls each peer whose <see cref="PeerOptions.WorkSignal"/> reported work (and whose
    /// <see cref="QuiclyPeer.HasPendingWork"/> confirms it) or whose next poll deadline (ping, timeout, linger) is due.
    /// Raises <see cref="PeerAdmitted"/>, <see cref="PeerClosed"/>, <see cref="SessionEnded"/> and the events other threads
    /// queued (<see cref="AdmissionFailed"/>, <see cref="CertificateConsumerFailed"/>), releases closed peers, and flushes
    /// when the <see cref="PeerOptions.AutoFlushInterval"/> or a peer's engine deadline
    /// (<see cref="QuiclyPeer.NextFlushDeadlineMicros"/>) is due. Idle peers are skipped; allocation-free in steady state.
    /// Exceptions from <see cref="PeerClosed"/> and <see cref="SessionEnded"/> handlers propagate (the remaining peers keep
    /// their work, and the remaining <see cref="SessionEnded"/> events stay queued, for the next call); a
    /// <see cref="PeerAdmitted"/> handler runs inside its peer's state change, so its exception is recorded as that peer's
    /// callback fault instead. A call from inside one of its own events returns 0.
    /// </summary>
    /// <remarks>
    /// With a <paramref name="maxItems"/> budget the peers with work are served round-robin: a call that runs out of budget
    /// makes the next one start after the peer that used the budget up, so a busy peer in a low slot cannot starve the others.
    /// </remarks>
    /// <param name="maxItems">Most messages to dispatch to handlers in this call, across all peers.</param>
    /// <returns>Messages dispatched to handlers.</returns>
    /// <exception cref="ObjectDisposedException">The server is disposed.</exception>
    public int PollAll(int maxItems = int.MaxValue)
    {
        ThrowIfDisposed();
        ArgumentOutOfRangeException.ThrowIfNegative(maxItems);
        if (_inPollAll)
        {
            return 0;
        }

        _inPollAll = true;
        try
        {
            _pollAllCalls++;
            if (Volatile.Read(ref _activationCount) != 0)
            {
                ActivatePending();
            }

            if (Volatile.Read(ref _decisionCount) != 0)
            {
                ApplyDecisions();
            }

            long now = _clock.NowMicros;
            int budget = maxItems;
            int dispatched = PollMarked(now, ref budget);
            if (now >= _earliestDeadline)
            {
                dispatched += PollDue(now, ref budget);
            }

            if (now >= _sessions.EarliestExpiry)
            {
                SweepSessions(now);
            }

            if (_endedHead < _endedSessions.Count)
            {
                RaiseEndedSessions();
            }

            if (_sharedTrackedCount != 0)
            {
                PruneShared();
            }

            // Poll never runs a scheduler pass, so engine work waits for a flush: the earliest engine deadline of the peers
            // brings this flush forward (docs/design/session-layer.md §4.7).
            bool autoFlush = _autoFlushMicros > 0 && now >= _nextAutoFlush;
            if (autoFlush || now >= _earliestFlushDeadline)
            {
                if (autoFlush)
                {
                    _nextAutoFlush = now + _autoFlushMicros;
                }

                FlushAll();
            }

            RaiseQueuedEvents();
            return dispatched;
        }
        finally
        {
            _inPollAll = false;
        }
    }

    /// <summary>
    /// Flushes every peer (game thread): transmits what was sent since the last flush and runs each peer's time-driven work
    /// (<see cref="QuiclyPeer.Flush"/>). Call it once per tick after the game logic sent its updates; <see cref="PollAll"/>
    /// also calls it when a peer's engine deadline (<see cref="NextFlushDeadlineMicros"/>) is due. Peers the application
    /// disposed itself are skipped.
    /// </summary>
    /// <param name="tick">The simulation tick carried in packed containers; 0 = none.</param>
    /// <exception cref="ObjectDisposedException">The server is disposed.</exception>
    public void FlushAll(uint tick = 0)
    {
        ThrowIfDisposed();
        if (Volatile.Read(ref _activationCount) != 0)
        {
            ActivatePending();
        }

        int count = _highWater;
        long earliestFlush = long.MaxValue;
        long previous = _earliestFlushDeadline;
        _earliestFlushDeadline = long.MaxValue;
        bool scanned = false;
        try
        {
            for (int slot = 0; slot < count; slot++)
            {
                QuiclyPeer? peer = _slots[slot].Peer;
                if (peer is null || peer.IsDisposed)
                {
                    continue;
                }

                peer.Flush(tick);
                if (!ReferenceEquals(_slots[slot].Peer, peer))
                {
                    continue; // released by a completion continuation
                }

                UpdateDeadlines(slot, peer);
                long flush = peer.NextFlushDeadlineMicros;
                if (flush < earliestFlush)
                {
                    earliestFlush = flush;
                }
            }

            scanned = true;
        }
        finally
        {
            // A peer activated (or flushed) while the scan ran lowered the field again; keep the lower of the two.
            _earliestFlushDeadline = Math.Min(_earliestFlushDeadline, scanned ? earliestFlush : previous);
        }
    }

    /// <summary>
    /// Polls every live peer regardless of work, for the measurement that compares the naive loop with <see cref="PollAll"/>.
    /// Not a substitute for it: no events, no slot release (a peer that closes here is marked for the next PollAll).
    /// </summary>
    internal int PollEveryPeer()
    {
        int dispatched = 0;
        for (int slot = 0; slot < _highWater; slot++)
        {
            if (_slots[slot].Peer is not { } peer || peer.IsDisposed)
            {
                continue;
            }

            dispatched += peer.Poll();
            if (peer.State == PeerState.Closed)
            {
                MarkWork(slot);
            }
        }

        return dispatched;
    }

    /// <summary>
    /// Polls the peers whose work bit is set, round-robin from <see cref="_pollCursor"/>: the bits of the cursor's word from the
    /// cursor up, the words after it, the words before it, and last the bits of the cursor's word below the cursor.
    /// </summary>
    private int PollMarked(long now, ref int budget)
    {
        long[] words = _workBits;
        int count = (_highWater + 63) >> 6;
        if (count == 0)
        {
            return 0;
        }

        int cursor = _pollCursor < _highWater ? _pollCursor : 0;
        int start = cursor >> 6;
        long below = (1L << (cursor & 63)) - 1;
        int dispatched = 0;
        for (int i = 0; i < count; i++)
        {
            int w = start + i < count ? start + i : start + i - count;
            if (Volatile.Read(ref words[w]) == 0)
            {
                continue;
            }

            // In the cursor's word only the bits from the cursor up are taken now; the ones below stay set for the end.
            long pending = i == 0 && below != 0 ? Interlocked.And(ref words[w], below) & ~below : Interlocked.Exchange(ref words[w], 0);
            dispatched += PollWord(w, pending, now, ref budget);
        }

        if (below != 0 && (Volatile.Read(ref words[start]) & below) != 0)
        {
            dispatched += PollWord(start, Interlocked.And(ref words[start], ~below) & below, now, ref budget);
        }

        return dispatched;
    }

    /// <summary>Polls the slots whose bits are in <paramref name="pending"/> (taken from word <paramref name="w"/>).</summary>
    private int PollWord(int w, long pending, long now, ref int budget)
    {
        int dispatched = 0;
        try
        {
            while (pending != 0)
            {
                int slot = (w << 6) + BitOperations.TrailingZeroCount(pending);
                dispatched += PollSlot(slot, now, ref budget);
                pending &= pending - 1;
            }
        }
        finally
        {
            if (pending != 0)
            {
                Interlocked.Or(ref _workBits[w], pending); // a handler threw: keep the rest (and the thrower) for the next call
            }
        }

        return dispatched;
    }

    private int PollDue(long now, ref int budget)
    {
        int dispatched = 0;
        long earliest = long.MaxValue;
        long[] deadlines = _deadlines;
        int count = _highWater;
        int slot = 0;

        // Deadlines set while the scan runs (the polled peers' next ones, peers a handler activated through FlushAll) lower
        // _earliestDeadline again, and the scan's own minimum is merged in at the end. A handler that throws leaves it no
        // later than before, so the next call scans again.
        long previous = _earliestDeadline;
        _earliestDeadline = long.MaxValue;
        bool scanned = false;
        try
        {
            if (Vector.IsHardwareAccelerated && count >= Vector<long>.Count)
            {
                Vector<long> nowVector = new(now);
                Vector<long> minimum = new(long.MaxValue);
                int last = count - Vector<long>.Count;
                for (; slot <= last; slot += Vector<long>.Count)
                {
                    Vector<long> block = new(deadlines, slot);
                    if (Vector.LessThanOrEqualAny(block, nowVector))
                    {
                        for (int k = slot; k < slot + Vector<long>.Count; k++)
                        {
                            if (deadlines[k] <= now)
                            {
                                dispatched += PollSlot(k, now, ref budget);
                            }
                        }

                        block = new Vector<long>(deadlines, slot);
                    }

                    minimum = Vector.Min(minimum, block);
                }

                for (int k = 0; k < Vector<long>.Count; k++)
                {
                    earliest = Math.Min(earliest, minimum[k]);
                }
            }

            for (; slot < count; slot++)
            {
                if (deadlines[slot] <= now)
                {
                    dispatched += PollSlot(slot, now, ref budget);
                }

                earliest = Math.Min(earliest, deadlines[slot]);
            }

            scanned = true;
        }
        finally
        {
            _earliestDeadline = Math.Min(_earliestDeadline, scanned ? earliest : previous);
        }

        return dispatched;
    }

    /// <summary>
    /// Runs one peer's game-thread work: a pending limit close, its <see cref="QuiclyPeer.Poll"/> when
    /// <see cref="QuiclyPeer.HasPendingWork"/> says there is something to do, and the slot's bookkeeping afterwards
    /// (deadlines, release of a closed peer).
    /// </summary>
    private int PollSlot(int slot, long now, ref int budget)
    {
        QuiclyPeer? peer = _slots[slot].Peer;
        if (peer is null)
        {
            return 0;
        }

        if (peer.IsDisposed)
        {
            // The application disposed the peer (from a handler, say): there is nothing left to poll, and its slot goes back.
            FinalizeSlot(slot, peer);
            return 0;
        }

        SlotInfo info = _info[slot]!;
        if (info.Sink!.TakeLimitClose() && peer.State is not (PeerState.Closing or PeerState.Closed))
        {
            peer.Close(new CloseReason(QuiclyErrorCode.LimitExceeded, "too many connections from this address"));
        }

        if (peer.State == PeerState.Closed)
        {
            FinalizeSlot(slot, peer);
            return 0;
        }

        if (PollOverride is null && !peer.HasPendingWork)
        {
            // Nothing is waiting. The peer's work signal is an edge that only a Poll re-arms, so the slot keeps its bit
            // instead of being cleared silently: the next PollAll probes again, and no publication can be missed.
            MarkWork(slot);
            UpdateDeadlines(slot, peer);
            return 0;
        }

        _peersPolled++;
        int given = budget;
        int dispatched = PollOverride is { } poll ? poll(peer, given) : peer.Poll(given);
        if (given != int.MaxValue)
        {
            budget = Math.Max(0, given - dispatched);
            if (dispatched >= given)
            {
                MarkWork(slot); // the budget ran out: the peer may hold more messages
                if (given > 0)
                {
                    _pollCursor = slot + 1; // the next budget-limited call starts after the peer that used it up
                }
            }
        }

        if (!ReferenceEquals(_slots[slot].Peer, peer))
        {
            return dispatched; // released by a handler (a shutdown from inside an event)
        }

        if (peer.State == PeerState.Closed)
        {
            FinalizeSlot(slot, peer);
            return dispatched;
        }

        UpdateDeadlines(slot, peer);
        if (peer.HasPendingWork && peer.NextDeadlineMicros > now)
        {
            // Work this Poll did not consume (a message of a channel without a handler, a stream held back): the peer raises
            // no second signal for it, so the level is what keeps the slot marked.
            MarkWork(slot);
        }

        return dispatched;
    }

    /// <summary>Records <paramref name="peer"/>'s poll deadline for the scan and merges its flush deadline (game thread).</summary>
    private void UpdateDeadlines(int slot, QuiclyPeer peer)
    {
        long deadline = peer.NextPollDeadlineMicros;
        _deadlines[slot] = deadline;
        if (deadline < _earliestDeadline)
        {
            _earliestDeadline = deadline;
        }

        long flush = peer.NextFlushDeadlineMicros;
        if (flush < _earliestFlushDeadline)
        {
            _earliestFlushDeadline = flush;
        }
    }

    private void SweepSessions(long now)
    {
        int before = _endedSessions.Count;
        _sessions.Sweep(now, _endedSessions);
        _sessionsExpired += _endedSessions.Count - before;
    }

    /// <summary>
    /// Raises <see cref="SessionEnded"/> for the queued ends, oldest first. A handler that throws leaves the rest queued for the
    /// next call.
    /// </summary>
    private void RaiseEndedSessions()
    {
        while (_endedHead < _endedSessions.Count)
        {
            SessionEndInfo ended = _endedSessions[_endedHead++];
            if (_endedHead == _endedSessions.Count)
            {
                _endedSessions.Clear();
                _endedHead = 0;
            }

            SessionEnded?.Invoke(ended);
        }
    }

    /// <summary>Raises every queued <see cref="SessionEnded"/> (the shutdown): all handlers run, and the first exception is returned.</summary>
    private Exception? RaiseAllEndedSessions()
    {
        Exception? first = null;
        while (_endedHead < _endedSessions.Count)
        {
            try
            {
                RaiseEndedSessions();
            }
            catch (Exception exception)
            {
                first ??= exception;
            }
        }

        return first;
    }
}
