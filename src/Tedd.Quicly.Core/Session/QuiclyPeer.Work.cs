namespace Tedd.Quicly.Core.Session;

// Any thread: the host-facing work signal (PeerOptions.WorkSignal) and the pending-work probe
// (docs/design/session-layer.md §4.7).
public sealed unsafe partial class QuiclyPeer
{
    private int _workSignalled;

    /// <summary>
    /// Whether the peer has game-thread work waiting right now: a received message (ring, per-channel drain queues, a
    /// coalescing mailbox), a send completion (the transport's completion ring or one the game thread queued itself), a
    /// control signal from the transport thread (connect, Hello, HelloAck, close, table), a pong or stream-ping sample, a
    /// stream held back by back-pressure, a send queued by another thread, a <see cref="StateChanged"/> transition that has
    /// not been raised, or a timer that is due (<see cref="NextDeadlineMicros"/> has passed). <see langword="false"/> once
    /// the session is closed or the peer is disposed.
    /// </summary>
    /// <remarks>
    /// The rings, the mailbox bitsets and the transport signal word are read with acquire semantics, so a call from a
    /// thread other than the game thread never misses work the transport published. The game thread's own bookkeeping
    /// (queued transitions, drain queues, the held entry) is read without synchronisation: for a foreign caller those parts
    /// are advisory, which is why a host wakes on the edge (<see cref="IPeerWorkSignal"/>) and decides on this probe from
    /// the game thread. A <see cref="Flush"/>-only deadline is not work: use <see cref="NextFlushDeadlineMicros"/> for that.
    /// </remarks>
    public bool HasPendingWork
    {
        get
        {
            if (_disposed || IsFreed || _closedRaised)
            {
                return false;
            }

            PeerCore core = _core;
            if (Volatile.Read(ref _signals) != 0
                || !core.CompletionRing.IsEmpty
                || core.LocalCompletionsQueued != 0
                || !core.ReceiveRing.IsEmpty
                || !core.PendedStreams.IsEmpty
                || !_pongs.IsEmpty
                || !_streamPings.IsEmpty
                || _transitionCount != 0
                || _queuedWithHandler != 0
                || _hasHeld
                || _hasHeldForeign
                || _front is { IsEmpty: false })
            {
                return true;
            }

            ReadOnlySpan<ReceiveMailbox> boxes = core.Mailboxes;
            for (int i = 0; i < boxes.Length; i++)
            {
                if (boxes[i].HasDirty)
                {
                    return true;
                }
            }

            return _nextDeadlineMicros <= _clock.NowMicros;
        }
    }

    /// <summary>
    /// Publishes that the peer has game-thread work and tells <see cref="PeerOptions.WorkSignal"/> once (any thread; the
    /// transport thread for received traffic, the game thread for work an application call created). Set-once until the next
    /// <see cref="Poll"/> clears it, so a burst of messages costs one host call; allocation-free, and a host exception is
    /// recorded and turned into a queued close instead of reaching the transport.
    /// </summary>
    internal void NoteWork()
    {
        IPeerWorkSignal? signal = _workSignal;
        if (signal is null || Interlocked.Exchange(ref _workSignalled, 1) != 0)
        {
            return;
        }

        try
        {
            signal.OnWork(this);
        }
        catch (Exception exception)
        {
            OnCallbackFault(exception);
        }
    }

    /// <summary>Re-arms the work signal (start of every <see cref="Poll"/>, where the host consumes what was published).</summary>
    private void ClearWorkSignal()
    {
        if (_workSignal is not null)
        {
            Volatile.Write(ref _workSignalled, 0);
        }
    }
}
