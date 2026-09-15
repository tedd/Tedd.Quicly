using Tedd.Quicly.Core.Control;

namespace Tedd.Quicly.Core.Session;

/// <summary>Which end of the connection a <see cref="QuiclyPeer"/> is.</summary>
public enum PeerRole : byte
{
    /// <summary>The peer connected out (<see cref="QuiclyPeer.Connect"/>); it opens the control stream and sends Hello.</summary>
    Client = 0,

    /// <summary>The peer was accepted by a listener (<see cref="QuiclyPeer.CreateServerPeer"/>); it answers Hello with HelloAck.</summary>
    Server = 1,
}

/// <summary>Lifecycle of a <see cref="QuiclyPeer"/>. Changes are reported by <see cref="QuiclyPeer.StateChanged"/> from <see cref="QuiclyPeer.Poll"/>.</summary>
public enum PeerState : byte
{
    /// <summary>The transport handshake has not completed.</summary>
    Connecting = 0,

    /// <summary>The transport is connected; the QUICLY Hello/HelloAck exchange is in progress (or awaiting admission).</summary>
    Handshaking = 1,

    /// <summary>Admitted: application traffic flows.</summary>
    Connected = 2,

    /// <summary>The connection was lost and a reconnect is in progress (set by a client host; never entered by the peer itself).</summary>
    Reconnecting = 3,

    /// <summary>A close was initiated (locally or by the peer); the transport close is pending.</summary>
    Closing = 4,

    /// <summary>The transport is closed. No handler or event runs any more.</summary>
    Closed = 5,
}

/// <summary>Where send completions (awaiters of <see cref="QuiclyPeer.WaitAsync"/>) are delivered.</summary>
public enum CompletionMode : byte
{
    /// <summary>Completions are observed and continuations run inside <see cref="QuiclyPeer.Poll"/> on the game thread (default).</summary>
    PollOnly = 0,

    /// <summary>
    /// Completions are signalled from the transport thread and continuations are queued to the thread pool, for hosts
    /// that do not poll. Slot bookkeeping (lease return, slot reuse) still happens in <see cref="QuiclyPeer.Poll"/> or
    /// <see cref="QuiclyPeer.Flush"/>.
    /// </summary>
    ThreadPool = 1,
}

/// <summary>Who ended the session.</summary>
public enum CloseSource : byte
{
    /// <summary>Not closed.</summary>
    None = 0,

    /// <summary>This end closed (<see cref="QuiclyPeer.Close(CloseReason)"/>, or a local limit or protocol violation).</summary>
    Local = 1,

    /// <summary>The peer sent Close or closed the QUIC connection.</summary>
    Peer = 2,

    /// <summary>The transport closed the connection (idle timeout, handshake failure, link loss).</summary>
    Transport = 3,
}

/// <summary>Why a session ended: a PROTOCOL.md §6 error code, an optional reason text and who closed it.</summary>
/// <param name="Code">Application error code (<see cref="QuiclyErrorCode.NoError"/> for an orderly close).</param>
/// <param name="Reason">
/// Reason text. For <see cref="QuiclyPeer.Close(CloseReason)"/> at most 512 bytes of UTF-8; when received from the peer it is
/// sanitised (control and format characters replaced with U+FFFD) before it is exposed.
/// </param>
public readonly record struct CloseReason(QuiclyErrorCode Code, string? Reason = null)
{
    /// <summary>An orderly close with no reason text.</summary>
    public static CloseReason Normal => new(QuiclyErrorCode.NoError);

    /// <summary>Who closed the session (<see cref="CloseSource.Local"/> for a reason passed to <see cref="QuiclyPeer.Close(CloseReason)"/>).</summary>
    public CloseSource Source { get; init; }

    /// <summary>For <see cref="CloseSource.Transport"/>: the transport's status code (see the transport's documentation); otherwise 0.</summary>
    public int TransportStatus { get; init; }
}
