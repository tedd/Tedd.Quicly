using System.Runtime.CompilerServices;
using Tedd.Quicly.Core.Session.Engines;

namespace Tedd.Quicly.Core.Session;

// Any thread: the host-facing work signal (PeerOptions.WorkSignal) and the pending-work probe
// (docs/design/session-layer.md §4.7).
public sealed unsafe partial class QuiclyPeer
{
    private int _workSignalled;

    /// <summary>The ReliableLatest engine, whose transport-thread control work (acks owed, notices) is part of <see cref="HasPendingWork"/>; null without such a channel.</summary>
    private readonly ReliableLatestEngine? _latest;

    /// <summary>The Bulk engine, whose work for its next pass (control frames, stream notices, progress owed, started and cancelled transfers) is part of <see cref="HasPendingWork"/>; null without such a channel.</summary>
    private readonly BulkEngine? _bulk;

    /// <summary>The table has a ReliableLatest or Bulk channel: a Poll brings the flush deadline forward for their pass work.</summary>
    private readonly bool _passEngines;

    /// <summary>
    /// Whether the peer has game-thread work waiting right now: a received message (the ring, or a per-channel drain queue
    /// or a mailbox of a channel with a handler), a send completion (the transport's completion ring or one the game thread
    /// queued itself), a control signal from the transport thread (connect, Hello, HelloAck, close, table), a pong or
    /// stream-ping sample, a stream held back by back-pressure, a send queued by another thread, a <see cref="StateChanged"/> transition that has
    /// not been raised, a deadline that is due (<see cref="NextDeadlineMicros"/> has passed), or engine work left for the
    /// next <see cref="Flush"/>: the ReliableLatest acks and rejects this end owes and the LatestAck and LatestReject entries
    /// the peer sent, which complete or retry values; the Bulk control frames the peer sent (progress, a range request, a
    /// cancel, a reject), the notices of this end's bulk streams, the progress this end owes for bulk bytes it accepted, and
    /// a transfer the application started or asked to cancel. <see langword="false"/> once the session is closed or the
    /// peer is disposed.
    /// </summary>
    /// <remarks>
    /// The rings, the mailbox bitsets and the transport signal word are read with acquire semantics, so a call from a
    /// thread other than the game thread never misses work the transport published. The game thread's own bookkeeping
    /// (queued transitions, drain queues, the held entry) is read without synchronisation: for a foreign caller those parts
    /// are advisory, which is why a host wakes on the edge (<see cref="IPeerWorkSignal"/>) and decides on this probe from
    /// the game thread. It is the level behind the edge, so a host that woke and finds it set polls (re-arming the edge)
    /// and flushes; the part that needs a Flush stays set until one runs, and ack and progress work may wait out
    /// <see cref="PeerOptions.AckDelay"/> or the bulk progress window in it. A Flush-only deadline that has not passed is
    /// not work: use <see cref="NextFlushDeadlineMicros"/> for that, which a <see cref="Poll"/> brings forward to the
    /// engine work above.
    /// <para>
    /// <b>The probe and the edge.</b> The edge (<see cref="IPeerWorkSignal"/>) is raised once and stays consumed until it
    /// is re-armed. A <see cref="Poll"/> re-arms it, and so does a call of this property that returns
    /// <see langword="false"/>: a host that is told there is nothing to do will not poll, so the probe re-arms the edge
    /// (and then reads the level once more, behind a full fence) before it answers. Without that, a publication the probe
    /// does not count — a mailbox value of a channel without a handler — or work a <see cref="Drain"/> took before the
    /// host asked would leave the edge consumed, and every later publication silent. A host can therefore rely on
    /// either order of events: work published before the answer is in the answer, work published after it raises the
    /// signal. A call that returns <see langword="true"/> leaves the edge alone, so a burst still costs one host call.
    /// </para>
    /// <para>
    /// Messages waiting for a channel without a handler are not work — they wait for the application's
    /// <see cref="Drain"/>, which no <see cref="Poll"/> can do for it — whether they wait in a drain queue or in a mailbox
    /// (a coalescing channel, ReliableLatest). So a host that loops on the probe must not expect it to announce them: it
    /// drains on its own tick, or on the edge, which the first such arrival after a Poll or after a probe that answered
    /// <see langword="false"/> raises. An unreliable channel nobody drains leaves the probe clear: its backlog is bounded
    /// and evicts (see <see cref="Poll"/>). The one exception is short: when a burst fills the queue pool, the next
    /// message is held for the Drain that may follow in the same frame and the probe is set; a Poll that finds the
    /// channel undrained queues or drops it and clears the probe (the first Poll after the hold, or the second when the
    /// channel had been drained just before). A
    /// <em>reliable</em> channel without a handler that is not drained is different: once the queue pool is full its next
    /// message is held and the ring behind it is not emptied, and the probe stays set for as long as that lasts, because
    /// there is work that no <see cref="Poll"/> can consume. A host that polls while the probe is set then polls every
    /// pass; the cure is to drain that channel or register a handler for it.
    /// </para>
    /// </remarks>
    public bool HasPendingWork
    {
        get
        {
            if (_disposed || IsFreed || _closedRaised)
            {
                return false;
            }

            if (HasWorkNow())
            {
                return true;
            }

            // Nothing is waiting, so the host will not poll — and only a Poll re-arms the edge. If the edge was consumed
            // (by work another call took meanwhile, or by a publication this probe does not count, such as a mailbox
            // value of a channel without a handler), re-arm it here, or every later publication would stay silent. The
            // exchange is a full fence and the level is read again after it: work published before the fence is seen
            // by that second read, work published after it finds the edge armed and raises the signal.
            if (_workSignal is not null && Volatile.Read(ref _workSignalled) != 0)
            {
                Interlocked.Exchange(ref _workSignalled, 0);
                return HasWorkNow();
            }

            return false;
        }
    }

    // Engine work only a scheduler pass consumes: only a Connected peer runs a pass.
    private bool HasWorkNow() =>
        HasQueuedPollWork()
        || (_state == PeerState.Connected && (_latest is { HasUnsentControl: true } || _bulk is { HasPassWork: true }))
        || _nextDeadlineMicros <= _clock.NowMicros;

    /// <summary>
    /// What a <see cref="Poll"/> still has to serve: <see cref="HasPendingWork"/> without the engine work left for the next
    /// scheduler pass and without a due flush deadline (<see cref="NextPollDeadlineMicros"/> is the only deadline here). A
    /// host that polls a marked peer and flushes on its own tick (QuiclyServer.PollAll) keeps the peer marked after its Poll
    /// only for this, because polling it again cannot consume the rest; the edge was re-armed by that Poll, and the flush
    /// deadline the Poll brought forward is the host's to serve.
    /// </summary>
    internal bool HasPendingPollWork =>
        !_disposed && !IsFreed && !_closedRaised && (HasQueuedPollWork() || _timerDeadline <= _clock.NowMicros);

    /// <summary>
    /// Brings the flush deadline forward to the engine work the transport thread (or the application) left since the last
    /// scheduler pass (game thread, the end of a Connected Poll). A pass computes <see cref="NextFlushDeadlineMicros"/> from
    /// what it saw, so without this the acks a value received after it is owed, or a bulk range request, would wait for the
    /// host's next Flush however far off that is; a host that brings its flush forward to the deadline (QuiclyServer.PollAll)
    /// serves them in time — the acks within <see cref="PeerOptions.AckDelay"/>, leaving a sooner flush of the host's own to
    /// carry them.
    /// </summary>
    /// <param name="now">Clock micros of the Poll.</param>
    [MethodImpl(MethodImplOptions.NoInlining)] // inlined into Poll, stages.Packed measured 1.7 % slower (client poll); out of line, no change
    private void LowerFlushDeadlineForPassWork(long now)
    {
        long deadline = _engineDeadline;
        _latest?.LowerControlDeadline(now, ref deadline);
        _bulk?.LowerPassDeadline(now, ref deadline);
        _engineDeadline = deadline;
    }

    /// <summary>
    /// The queues and signals a Poll consumes (not the deadlines): <see cref="HasPendingWork"/> and
    /// <see cref="HasPendingPollWork"/> without their deadline and engine parts. The caller checked the peer is not disposed.
    /// A mailbox counts only while its channel has a handler: Poll leaves the values of a channel without one for
    /// <see cref="Drain"/>, so counting them would keep a host that polls while there is work polling for good.
    /// </summary>
    private bool HasQueuedPollWork()
    {
        PeerCore core = _core;
        if (Volatile.Read(ref _signals) != 0
            || !core.CompletionRing.IsEmpty
            || core.LocalCompletionsQueued != 0
            || !core.ReceiveRing.IsEmpty
            || !core.PendedStreams.IsEmpty
            || core.HasRetiredPendedStreams
            || !_pongs.IsEmpty
            || !_streamPings.IsEmpty
            || _transitionCount != 0
            || _queues.QueuedHandled != 0
            || _hasHeld
            || _hasHeldForeign
            || _front is { IsEmpty: false }
            || HasBulkObjectWork)
        {
            return true;
        }

        ReadOnlySpan<ReceiveMailbox> boxes = core.Mailboxes;
        for (int i = 0; i < boxes.Length; i++)
        {
            if (boxes[i].HasDirty && _handlers[boxes[i].ChannelIndex] is not null)
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Publishes that the peer has game-thread work and tells <see cref="PeerOptions.WorkSignal"/> once (any thread; the
    /// transport thread for received traffic, the game thread for work an application call created). Set-once until the next
    /// <see cref="Poll"/>, or a <see cref="HasPendingWork"/> probe that finds nothing, clears it, so a burst of messages
    /// costs one host call; allocation-free, and a host exception is recorded and turned into a queued close instead of
    /// reaching the transport.
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

    /// <summary>
    /// Re-arms the work signal (game thread, start of every <see cref="Poll"/>, where the host consumes what was
    /// published).
    /// </summary>
    /// <remarks>
    /// The clear is an <see cref="Interlocked.Exchange(ref int, int)"/>, not a plain store: a store may still sit in this
    /// core's store buffer while the Poll reads the completion ring and the thread-safe send front, and a publisher whose
    /// <see cref="NoteWork"/> exchange then reads the old 1 stays silent about work the Poll has already looked past. The
    /// full fence orders the clear before every read the Poll makes, so a publication is either seen by this Poll or finds
    /// the edge armed. A Poll that was not signalled pays only the read: a publisher that finds 0 raises the signal itself.
    /// </remarks>
    private void ClearWorkSignal()
    {
        if (_workSignal is not null && Volatile.Read(ref _workSignalled) != 0)
        {
            Interlocked.Exchange(ref _workSignalled, 0);
        }
    }
}
