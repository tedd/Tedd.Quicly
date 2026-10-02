using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.Threading;

namespace Tedd.Quicly.Core.Session;

// Game thread: tracked-send completion APIs and the completion drain that routes transport completions (and the ones the
// game thread queued itself) to the entry's owner: control traffic → the peer, containers → the packer's fan-out,
// everything else → the channel's engine.
public sealed unsafe partial class QuiclyPeer
{
    /// <summary>
    /// Waits for a stage of a tracked send (<see cref="SendOptions.Track"/>). With <see cref="CompletionMode.PollOnly"/> the
    /// continuation runs inside <see cref="Poll"/> or <see cref="Flush"/> on the game thread; with
    /// <see cref="CompletionMode.ThreadPool"/> it is queued to the thread pool. Allocation-free unless the caller's own
    /// async method suspends.
    /// </summary>
    /// <param name="token">The token of an admitted, tracked send.</param>
    /// <param name="stage">BufferReleased or RemoteAccepted.</param>
    /// <param name="cancellationToken">Cancels the wait, not the send.</param>
    /// <returns>The delivery status when the stage completed.</returns>
    public ValueTask<DeliveryStatus> WaitAsync(SendToken token, CompletionStage stage, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        return _core.Completions.WaitAsync(token, stage, cancellationToken);
    }

    /// <summary>
    /// Blocks the game thread until a stage of a tracked send completes or <paramref name="timeout"/> elapses (native hosts
    /// only). Spins for up to about 20 µs, then parks.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Call it on the game thread, like every other member (one thread at a time), with at most one wait (blocking or
    /// <see cref="WaitAsync"/>) per stage of a token.
    /// </para>
    /// <para>
    /// While the game thread is blocked here nothing polls, so only a completion that the transport thread signals itself
    /// can end the wait. It does so only with <see cref="CompletionMode.ThreadPool"/>, and only for an unfragmented message on
    /// an <see cref="Channels.ChannelMode.UnreliableUnordered"/> or <see cref="Channels.ChannelMode.UnreliableSequenced"/>
    /// channel that left in a datagram of its own (<see cref="CompletionStage.BufferReleased"/> once the transport has sent
    /// it, both stages at its outcome). Every other stage completes inside <see cref="Poll"/> or <see cref="Flush"/>: every
    /// stage with <see cref="CompletionMode.PollOnly"/>, messages packed together into one datagram, fragmented or expired
    /// messages, <see cref="Channels.ChannelMode.ReliableLatest"/> and every stream channel. A blocking wait for one of those
    /// returns <see cref="DeliveryStatus.Pending"/> when its timeout elapses: use <see cref="WaitAsync"/> instead, or poll and
    /// read <see cref="GetDeliveryStatus"/>.
    /// </para>
    /// </remarks>
    /// <param name="token">The token.</param>
    /// <param name="stage">The stage.</param>
    /// <param name="timeout">Longest wait; <see cref="Timeout.InfiniteTimeSpan"/> waits forever.</param>
    /// <returns>The status, or <see cref="DeliveryStatus.Pending"/> on timeout.</returns>
    public DeliveryStatus Wait(SendToken token, CompletionStage stage, TimeSpan timeout)
    {
        ThrowIfDisposed();
        return _core.Completions.Wait(token, stage, timeout);
    }

    /// <summary>The current delivery status of a tracked send (for a finished token: the status it ended with).</summary>
    /// <param name="token">The token.</param>
    /// <returns>The status.</returns>
    public DeliveryStatus GetDeliveryStatus(SendToken token)
    {
        ThrowIfDisposed();
        return _core.Completions.GetStatus(token);
    }

    /// <summary>
    /// Best-effort cancellation of a tracked send that has not been handed to the transport yet (routed to its engine).
    /// Never releases the payload by itself: the send completes <see cref="DeliveryStatus.Canceled"/> through the usual
    /// path, at the next <see cref="Poll"/> or <see cref="Flush"/>.
    /// </summary>
    /// <param name="token">The token.</param>
    /// <returns><see langword="true"/> when the send will complete canceled.</returns>
    public bool TryCancel(SendToken token)
    {
        ThrowIfDisposed();
        int slot = _core.EntryOfToken(token);
        if (slot < 0)
        {
            return false;
        }

        int index = _core.ChannelIndexOf(_core.Entries[slot].Channel);
        return index >= 0 && _core.GetEngine(index).TryCancel(slot);
    }

    /// <summary>
    /// Routes everything in the completion ring, then the completions the game thread queued itself. The ring is also where
    /// a datagram's outcome is counted (<see cref="PeerCore.CountDatagramOutcome"/>): each final datagram completion passes
    /// here (or through <see cref="DrainCompletionsMarking"/>) exactly once, whereas the routing below sees a container's
    /// completion once more for every member. The entry is still allocated at this point — a final completion leaves it
    /// <c>Completed</c> until the game thread frees it — so its channel can be read.
    /// </summary>
    private void DrainCompletions()
    {
        SpscRing<CompletionEntry> ring = _core.CompletionRing;
        while (ring.TryDequeue(out CompletionEntry completion))
        {
            if (completion.Final && completion.Kind == CompletionKind.Datagram)
            {
                _core.CountDatagramOutcome(_core.Entries[completion.Slot].Channel, completion.DatagramState);
            }

            RouteCompletion(in completion);
        }

        DrainLocalCompletions();
    }

    /// <summary>Routes the completions the game thread queued itself (<see cref="PeerCore.QueueLocalCompletion"/>).</summary>
    private void DrainLocalCompletions()
    {
        while (_core.TryDequeueLocalCompletion(out CompletionEntry completion))
        {
            RouteCompletion(in completion);
        }
    }

    /// <summary>
    /// Routes one completion to the owner of its entry (game thread): channel 0 → the peer's control traffic, channel 1 →
    /// the packer's fan-out (<see cref="PeerCore.OnContainerCompleted"/>, which routes every member back here), any other
    /// channel → its engine's <see cref="ChannelEngine.OnSendCompleted"/>.
    /// </summary>
    /// <param name="completion">The completion.</param>
    internal void RouteCompletion(in CompletionEntry completion)
    {
        int slot = completion.Slot;
        ushort channel = _core.Entries[slot].Channel;
        if (channel == PeerCore.ControlChannelId)
        {
            OnControlEntryCompleted(in completion);
        }
        else if (channel == PeerCore.ContainerChannelId)
        {
            _core.OnContainerCompleted(slot, in completion);
        }
        else
        {
            _core.GetEngine(_core.ChannelIndexOf(channel)).OnSendCompleted(slot, in completion);
        }
    }
}
