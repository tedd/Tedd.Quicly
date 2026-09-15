using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Server;

/// <summary>State of a <see cref="PeerSlot"/>.</summary>
public enum PeerSlotState : byte
{
    /// <summary>The slot holds no peer.</summary>
    Free = 0,

    /// <summary>The connection is in the transport or QUICLY handshake, or waiting for its admission decision.</summary>
    Handshaking = 1,

    /// <summary>The session is admitted (<see cref="QuiclyServer.PeerAdmitted"/> was raised).</summary>
    Admitted = 2,

    /// <summary>A close was initiated; the slot is released once the peer reports <see cref="PeerState.Closed"/>.</summary>
    Closing = 3,
}

/// <summary>
/// One entry of the server's dense peer table (<see cref="QuiclyServer.Peers"/>). <see cref="Generation"/> changes every
/// time the slot is released, so (index, generation) names one connection for its whole life (see
/// <see cref="QuiclyServer.GetPeer(int, uint)"/>).
/// </summary>
public readonly struct PeerSlot
{
    internal PeerSlot(QuiclyPeer? peer, uint generation, PeerSlotState state)
    {
        Peer = peer;
        Generation = generation;
        State = state;
    }

    /// <summary>The peer, or <see langword="null"/> for a free slot.</summary>
    public QuiclyPeer? Peer { get; }

    /// <summary>The slot's generation (starts at 1, incremented on every release).</summary>
    public uint Generation { get; }

    /// <summary>The slot's state.</summary>
    public PeerSlotState State { get; }

    /// <summary>True when the slot holds a peer.</summary>
    public bool IsOccupied => Peer is not null;
}

/// <summary>
/// Outcome of <see cref="QuiclyServer.SendShared"/>: which peers admitted the message. The masks and statuses belong to the
/// server and are overwritten by its next <see cref="QuiclyServer.SendShared"/> call; copy what you keep.
/// </summary>
public readonly struct SharedSendResult
{
    private readonly byte[]? _statuses;

    internal SharedSendResult(int admitted, int rejected, PeerSet admittedPeers, PeerSet rejectedPeers, byte[] statuses)
    {
        AdmittedCount = admitted;
        RejectedCount = rejected;
        Admitted = admittedPeers;
        Rejected = rejectedPeers;
        _statuses = statuses;
    }

    /// <summary>Peers that admitted the message (each holds a reference to the shared lease until its send completed).</summary>
    public int AdmittedCount { get; }

    /// <summary>Peers that refused it (see <see cref="GetStatus"/>) or no longer exist.</summary>
    public int RejectedCount { get; }

    /// <summary>Indices of the peers that admitted the message (read-only).</summary>
    public PeerSet? Admitted { get; }

    /// <summary>Indices of the peers that refused it (read-only).</summary>
    public PeerSet? Rejected { get; }

    /// <summary>True when no peer refused the message.</summary>
    public bool AllAdmitted => RejectedCount == 0;

    /// <summary>The send status of one peer of the set.</summary>
    /// <param name="peerIndex">An index of the set that was sent to.</param>
    /// <returns><see cref="SendStatus.Admitted"/>, or why the peer refused (<see cref="SendStatus.NotConnected"/> for a free slot).</returns>
    /// <exception cref="ArgumentOutOfRangeException">The index was not part of the send.</exception>
    public SendStatus GetStatus(int peerIndex)
    {
        if (Admitted is not null && Admitted.Contains(peerIndex))
        {
            return SendStatus.Admitted;
        }

        if (Rejected is not null && Rejected.Contains(peerIndex))
        {
            return (SendStatus)_statuses![peerIndex];
        }

        throw new ArgumentOutOfRangeException(nameof(peerIndex), peerIndex, "The peer was not part of the send.");
    }
}

/// <summary>A session left the server's registry (<see cref="QuiclyServer.SessionEnded"/>): it can no longer be resumed.</summary>
/// <param name="SessionId">The session.</param>
/// <param name="Tag">The <see cref="QuiclyPeer.Tag"/> its last connection ended with (for example a player id set by the application).</param>
/// <param name="Expired">
/// <see langword="true"/> when the grace period after a lost connection ran out; <see langword="false"/> when the session
/// ended with its connection (a deliberate close by either side, or a zero grace period).
/// </param>
public readonly record struct SessionEndInfo(ulong SessionId, ulong Tag, bool Expired);

/// <summary>Snapshot of a server's counters (<see cref="QuiclyServer.GetStatistics"/>). Totals count since the server was created.</summary>
public struct ServerStatistics
{
    /// <summary>Connections holding a slot (handshaking, admitted or closing).</summary>
    public int Connections;

    /// <summary>Admitted peers (<see cref="QuiclyServer.AdmittedPeers"/>).</summary>
    public int AdmittedPeers;

    /// <summary>Connections that completed no admission yet.</summary>
    public int UnadmittedConnections;

    /// <summary>Sessions in the registry: connected, or within their grace period.</summary>
    public int Sessions;

    /// <summary>Per-peer shared sends whose buffer was not released yet.</summary>
    public int SharedSendsOutstanding;

    /// <summary>Connections accepted by the listener callbacks.</summary>
    public long ConnectionsAccepted;

    /// <summary>Connections refused before the handshake (policy or limits).</summary>
    public long ConnectionsRefused;

    /// <summary>Fresh sessions created.</summary>
    public long SessionsCreated;

    /// <summary>Sessions resumed on a new connection.</summary>
    public long SessionsResumed;

    /// <summary>Live connections closed with <c>SessionReplaced</c> by a resume.</summary>
    public long SessionsReplaced;

    /// <summary>Sessions whose grace period ran out.</summary>
    public long SessionsExpired;

    /// <summary>Hellos refused (any status).</summary>
    public long AdmissionsRejected;

    /// <summary>Hellos whose decision was deferred (<see cref="Core.Session.AdmissionDecision.Pending"/>).</summary>
    public long AdmissionsPending;

    /// <summary>Calls of <see cref="QuiclyServer.PollAll"/>.</summary>
    public long PollAllCalls;

    /// <summary>Peer <see cref="QuiclyPeer.Poll"/> calls made by <see cref="QuiclyServer.PollAll"/> (idle peers are skipped).</summary>
    public long PeersPolled;

    /// <summary>Exceptions thrown by <see cref="QuiclyServer.AdmissionFailed"/> and <see cref="QuiclyServer.CertificateConsumerFailed"/> handlers (swallowed).</summary>
    public long EventHandlerFaults;
}
