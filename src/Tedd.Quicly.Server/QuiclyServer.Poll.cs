using System.Numerics;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Server;

// Game thread: PollAll (work bits, deadlines, session expiry, auto flush) and FlushAll.
public sealed partial class QuiclyServer
{
    private readonly List<SessionEndInfo> _endedSessions = [];
    private bool _inPollAll;
    private long _nextAutoFlush;
    private long _pollAllCalls;
    private long _peersPolled;

    /// <summary>
    /// Runs the game-thread side of every peer that has something to do (game thread): activates newly accepted connections,
    /// applies admission decisions, then polls each peer whose transport raised a callback since its last poll or whose
    /// next deadline (ping, timeout, linger) is due. Raises <see cref="PeerAdmitted"/>, <see cref="PeerClosed"/> and
    /// <see cref="SessionEnded"/>, releases closed peers and runs the <see cref="PeerOptions.AutoFlushInterval"/> flush.
    /// Idle peers are skipped; allocation-free in steady state. Handler exceptions propagate (the remaining peers keep their
    /// work for the next call). A call from inside one of its own events returns 0.
    /// </summary>
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
            int dispatched = PollMarked(ref budget);
            if (now >= _earliestDeadline)
            {
                dispatched += PollDue(now, ref budget);
            }

            if (now >= _sessions.EarliestExpiry)
            {
                SweepSessions(now);
            }

            if (_autoFlushMicros > 0 && now >= _nextAutoFlush)
            {
                _nextAutoFlush = now + _autoFlushMicros;
                FlushAll();
            }

            return dispatched;
        }
        finally
        {
            _inPollAll = false;
        }
    }

    /// <summary>
    /// Flushes every peer (game thread): transmits what was sent since the last flush and runs each peer's time-driven work
    /// (<see cref="QuiclyPeer.Flush"/>). Call it once per tick after the game logic sent its updates.
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
        for (int slot = 0; slot < count; slot++)
        {
            QuiclyPeer? peer = _slots[slot].Peer;
            if (peer is null)
            {
                continue;
            }

            peer.Flush(tick);
            long deadline = peer.NextDeadlineMicros;
            _deadlines[slot] = deadline;
            if (deadline < _earliestDeadline)
            {
                _earliestDeadline = deadline;
            }
        }
    }

    /// <summary>Polls every live peer regardless of work (the naive loop, kept for the measurement that compares it with <see cref="PollAll"/>).</summary>
    internal int PollEveryPeer()
    {
        int budget = int.MaxValue;
        int dispatched = 0;
        for (int slot = 0; slot < _highWater; slot++)
        {
            if (_slots[slot].Peer is not null)
            {
                dispatched += PollSlot(slot, ref budget);
            }
        }

        return dispatched;
    }

    private int PollMarked(ref int budget)
    {
        int dispatched = 0;
        long[] words = _workBits;
        int count = (_highWater + 63) >> 6;
        for (int w = 0; w < count; w++)
        {
            if (Volatile.Read(ref words[w]) == 0)
            {
                continue;
            }

            long pending = Interlocked.Exchange(ref words[w], 0);
            try
            {
                while (pending != 0)
                {
                    int slot = (w << 6) + BitOperations.TrailingZeroCount(pending);
                    dispatched += PollSlot(slot, ref budget);
                    pending &= pending - 1;
                }
            }
            finally
            {
                if (pending != 0)
                {
                    Interlocked.Or(ref words[w], pending); // a handler threw: keep the rest (and the thrower) for the next call
                }
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
                            dispatched += PollSlot(k, ref budget);
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
                dispatched += PollSlot(slot, ref budget);
            }

            earliest = Math.Min(earliest, deadlines[slot]);
        }

        _earliestDeadline = earliest;
        return dispatched;
    }

    private int PollSlot(int slot, ref int budget)
    {
        QuiclyPeer? peer = _slots[slot].Peer;
        if (peer is null)
        {
            return 0;
        }

        _peersPolled++;
        SlotInfo info = _info[slot]!;
        if (info.Sink!.TakeLimitClose() && peer.State is not (PeerState.Closing or PeerState.Closed))
        {
            peer.Close(new CloseReason(QuiclyErrorCode.LimitExceeded, "too many connections from this address"));
        }

        int given = budget;
        int dispatched = peer.Poll(given);
        if (given != int.MaxValue)
        {
            budget = Math.Max(0, given - dispatched);
            if (dispatched >= given)
            {
                MarkWork(slot); // the budget ran out: the peer may hold more messages
            }
        }

        if (!ReferenceEquals(_slots[slot].Peer, peer))
        {
            return dispatched; // released by a handler (a shutdown from inside an event)
        }

        if (_sharedHead[slot] >= 0)
        {
            ProcessShared(slot, peer);
        }

        if (peer.State == PeerState.Closed)
        {
            FinalizeSlot(slot, peer);
        }
        else
        {
            long deadline = peer.NextDeadlineMicros;
            _deadlines[slot] = deadline;
            if (deadline < _earliestDeadline)
            {
                _earliestDeadline = deadline;
            }
        }

        return dispatched;
    }

    private void SweepSessions(long now)
    {
        _endedSessions.Clear();
        _sessions.Sweep(now, _endedSessions);
        _sessionsExpired += _endedSessions.Count;
        Action<SessionEndInfo>? handler = SessionEnded;
        if (handler is null)
        {
            return;
        }

        for (int i = 0; i < _endedSessions.Count; i++)
        {
            handler(_endedSessions[i]);
        }
    }
}
