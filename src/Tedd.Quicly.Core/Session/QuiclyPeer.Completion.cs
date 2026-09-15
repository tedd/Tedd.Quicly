using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.Threading;

namespace Tedd.Quicly.Core.Session;

// Game thread: tracked-send completion APIs and the completion-ring drain that routes transport completions to the entry's
// owner (control traffic → the peer, containers → the packer seam, everything else → the channel's engine).
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
    /// Blocks until a stage of a tracked send completes (native hosts only; never on the thread that polls in
    /// <see cref="CompletionMode.PollOnly"/> mode, where the completion is only observed inside <see cref="Poll"/>).
    /// </summary>
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
    /// Never releases the payload by itself: the send completes <see cref="DeliveryStatus.Canceled"/> through the usual path.
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

    private void DrainCompletions()
    {
        SpscRing<CompletionEntry> ring = _core.CompletionRing;
        while (ring.TryDequeue(out CompletionEntry completion))
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
}
