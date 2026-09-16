namespace Tedd.Quicly.Core.Session;

/// <summary>
/// Told by a <see cref="QuiclyPeer"/> that it has game-thread work waiting (<see cref="PeerOptions.WorkSignal"/>), so a
/// host that sleeps between passes can wake the thread that owns the peer instead of polling idle peers.
/// </summary>
/// <remarks>
/// <para><b>Contract.</b> <see cref="OnWork"/> runs on whichever thread produced the work: a transport thread for received
/// messages, completions, control frames and the handshake, or the game thread for work an application call created
/// (<see cref="QuiclyPeer.Close(CloseReason)"/>, <see cref="QuiclyPeer.CompleteAdmission"/>, a send from another thread).
/// It must not block, must not allocate and must not call back into the peer; setting a flag, or an
/// <c>Interlocked</c> push onto the host's own "peers with work" queue, is the intended body. An exception is recorded like
/// a transport callback fault (<see cref="QuiclyPeer.LastCallbackFault"/>, <see cref="PeerStatistics.CallbackFaults"/>) and
/// closes the connection with <see cref="Control.QuiclyErrorCode.InternalError"/>; it never reaches the transport.</para>
/// <para><b>Rate.</b> Set-once semantics: the peer calls <see cref="OnWork"/> for the first work published after each
/// <see cref="QuiclyPeer.Poll"/> and stays silent until that Poll runs, so a burst of a thousand messages costs one call.
/// The signal is therefore an edge, not a level: <see cref="QuiclyPeer.HasPendingWork"/> is the authority on whether
/// anything is actually waiting (work that <see cref="QuiclyPeer.Poll"/> does not consume — a message of a channel without
/// a handler, engine work that needs a <see cref="QuiclyPeer.Flush"/> — raises no second signal).</para>
/// </remarks>
public interface IPeerWorkSignal
{
    /// <summary>The peer published game-thread work. Non-blocking, allocation-free, must not throw or re-enter the peer.</summary>
    /// <param name="peer">The peer with work waiting.</param>
    void OnWork(QuiclyPeer peer);
}
