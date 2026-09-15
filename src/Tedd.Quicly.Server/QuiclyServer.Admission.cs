using System.Net;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Server;

// Game thread: the Hello admission adapter, capacity reservation, deferred decisions and the peers' state changes.
public sealed partial class QuiclyServer
{
    private List<Decision> _decisions = [];
    private List<Decision> _decisionsSpare = [];
    private int _decisionCount;
    private long _sessionsCreated;
    private long _sessionsResumed;
    private long _sessionsReplaced;
    private long _sessionsExpired;
    private long _admissionsRejected;
    private long _admissionsPending;

    /// <summary>
    /// Completes an admission whose auth validation returned <see cref="AuthTokenDecision.Pending"/> (or whose custom policy
    /// returned <see cref="AdmissionResult.Pending"/>). Thread-safe: the decision is queued and applied on the game thread by
    /// the next <see cref="PollAll"/>, which then runs the remaining checks (capacity, session commit) of the default policy.
    /// Ignored when the connection closed meanwhile (for example after <see cref="PeerOptions.AdmissionTimeout"/>) or no
    /// decision is pending. A rejection reaches the client as <see cref="HelloStatus.Rejected"/>; <paramref name="reason"/>
    /// is reported only server-side (<see cref="AdmissionFailure.Detail"/>).
    /// </summary>
    /// <param name="peer">The connection whose admission is pending.</param>
    /// <param name="accepted">The outcome of the validation.</param>
    /// <param name="reason">Why it was rejected (server-side diagnostics only).</param>
    public void CompleteAdmission(QuiclyPeer peer, bool accepted, string? reason = null)
    {
        ArgumentNullException.ThrowIfNull(peer);
        Enqueue(new Decision(peer, accepted, reason, null));
    }

    /// <summary>
    /// Completes a pending admission with an explicit result (for custom policies; the server still enforces
    /// <see cref="ServerOptions.MaxPeers"/>). For an admission deferred by the default policy only the decision is used and
    /// its session handling applies. Thread-safe; applied by the next <see cref="PollAll"/>.
    /// </summary>
    /// <param name="peer">The connection whose admission is pending.</param>
    /// <param name="result">Accept or Reject.</param>
    /// <exception cref="ArgumentException"><paramref name="result"/> is Pending.</exception>
    public void CompleteAdmission(QuiclyPeer peer, AdmissionResult result)
    {
        ArgumentNullException.ThrowIfNull(peer);
        if (result.Decision == AdmissionDecision.Pending)
        {
            throw new ArgumentException("An admission cannot be completed with Pending.", nameof(result));
        }

        Enqueue(new Decision(peer, result.Decision == AdmissionDecision.Accept, result.Reason, result));
    }

    internal bool TryReserve(QuiclyPeer peer, bool allowOverCapacity) => InfoOf(peer) is { } info && TryReserve(info, allowOverCapacity);

    internal void Unreserve(QuiclyPeer peer)
    {
        if (InfoOf(peer) is not { Reserved: true } info)
        {
            return;
        }

        _reserved--;
        info.Reserved = false;
        info.Unadmitted = true;
        lock (_gate)
        {
            _unadmitted++;
        }
    }

    internal void BindSession(QuiclyPeer peer, SessionRecord record)
    {
        if (InfoOf(peer) is { } info)
        {
            info.Session = record;
        }
    }

    internal void SetPending(QuiclyPeer peer, PendingAdmission state)
    {
        if (InfoOf(peer) is { } info)
        {
            info.Pending = state;
        }
    }

    /// <summary>A resume took the session over: close its previous connection with <see cref="QuiclyErrorCode.SessionReplaced"/>.</summary>
    internal void ReplaceConnection(QuiclyPeer previous)
    {
        if (InfoOf(previous) is { } info)
        {
            info.Session = null;
        }

        _sessionsReplaced++;
        if (previous.State is not (PeerState.Closing or PeerState.Closed))
        {
            previous.Close(new CloseReason(QuiclyErrorCode.SessionReplaced, "the session was resumed on another connection"));
        }

        MarkWork(previous.Index);
    }

    internal void CountSessionCreated() => _sessionsCreated++;

    internal void CountSessionResumed() => _sessionsResumed++;

    private void Enqueue(in Decision decision)
    {
        lock (_gate)
        {
            _decisions.Add(decision);
            Volatile.Write(ref _decisionCount, _decisions.Count);
        }

        MarkWork(decision.Peer.Index);
    }

    private void ApplyDecisions()
    {
        List<Decision> batch;
        lock (_gate)
        {
            batch = _decisions;
            _decisions = _decisionsSpare;
            _decisionsSpare = batch;
            Volatile.Write(ref _decisionCount, 0);
        }

        try
        {
            foreach (Decision decision in batch)
            {
                Apply(in decision);
            }
        }
        finally
        {
            batch.Clear();
        }
    }

    private void Apply(in Decision decision)
    {
        QuiclyPeer peer = decision.Peer;
        SlotInfo? info = InfoOf(peer);
        if (info is null || !info.AwaitingDecision)
        {
            return;
        }

        info.AwaitingDecision = false;
        AdmissionResult result;
        if (info.Pending is { } state)
        {
            info.Pending = null;
            result = _defaultPolicy.CompletePending(peer, state, decision.Accepted, decision.Reason);
        }
        else if (decision.Result is { } explicitResult)
        {
            result = explicitResult;
        }
        else if (decision.Accepted)
        {
            result = AdmissionResult.Accept();
        }
        else
        {
            ReportFailure(new AdmissionFailure(AdmissionStage.Hello, AdmissionFailureReason.AuthTokenRejected, peer.RemoteEndPoint, Detail: decision.Reason));
            result = AdmissionResult.Reject(HelloStatus.Rejected);
        }

        result = AfterDecision(info, result, peer.RemoteEndPoint);
        try
        {
            peer.CompleteAdmission(result);
        }
        catch (InvalidOperationException)
        {
            // The peer no longer waits for a decision; it is closing and its slot is released as usual.
        }

        MarkWork(peer.Index);
    }

    private AdmissionResult OnHello(in HelloInfo hello, QuiclyPeer peer)
    {
        SlotInfo? info = InfoOf(peer);
        if (info is null)
        {
            return AdmissionResult.Reject(HelloStatus.InternalError);
        }

        AdmissionResult result;
        try
        {
            result = Volatile.Read(ref _policy).Admit(in hello, peer);
        }
        catch (Exception exception)
        {
            ReportFailure(new AdmissionFailure(AdmissionStage.Hello, AdmissionFailureReason.PolicyFault, hello.RemoteEndPoint, Exception: exception));
            result = AdmissionResult.Reject(HelloStatus.InternalError);
        }

        return AfterDecision(info, result, hello.RemoteEndPoint);
    }

    /// <summary>Enforces capacity on an accept of any policy and keeps the counters (game thread).</summary>
    private AdmissionResult AfterDecision(SlotInfo info, AdmissionResult result, IPEndPoint? remote)
    {
        switch (result.Decision)
        {
            case AdmissionDecision.Accept:
                if (!TryReserve(info, allowOverCapacity: false))
                {
                    ReportFailure(new AdmissionFailure(AdmissionStage.Hello, AdmissionFailureReason.ServerFull, remote));
                    _admissionsRejected++;
                    return AdmissionResult.Reject(HelloStatus.ServerFull);
                }

                return result;
            case AdmissionDecision.Pending:
                info.AwaitingDecision = true;
                _admissionsPending++;
                return result;
            default:
                _admissionsRejected++;
                return result;
        }
    }

    private bool TryReserve(SlotInfo info, bool allowOverCapacity)
    {
        if (info.Reserved)
        {
            return true;
        }

        if (!allowOverCapacity && _reserved >= _maxPeers)
        {
            return false;
        }

        _reserved++;
        info.Reserved = true;
        if (info.Unadmitted)
        {
            info.Unadmitted = false;
            lock (_gate)
            {
                _unadmitted--;
            }
        }

        return true;
    }

    private void OnPeerStateChanged(QuiclyPeer peer, PeerState from, PeerState to)
    {
        SlotInfo? info = InfoOf(peer);
        if (info is null)
        {
            return;
        }

        int slot = peer.Index;
        if (to == PeerState.Connected)
        {
            if (info.Admitted)
            {
                return; // re-raised after an earlier handler threw
            }

            TryReserve(info, allowOverCapacity: true);
            info.Admitted = true;
            _slots[slot] = new PeerSlot(peer, _slots[slot].Generation, PeerSlotState.Admitted);
            _admittedSet.AddCore(slot);
            PeerAdmitted?.Invoke(peer);
        }
        else if (to == PeerState.Closing && _slots[slot].State != PeerSlotState.Closing)
        {
            _slots[slot] = new PeerSlot(peer, _slots[slot].Generation, PeerSlotState.Closing);
            _admittedSet.RemoveCore(slot);
        }
    }

    private readonly record struct Decision(QuiclyPeer Peer, bool Accepted, string? Reason, AdmissionResult? Result);

    /// <summary>The <see cref="IPeerAdmission"/> every server peer is created with: forwards the Hello to the server.</summary>
    private sealed class HelloAdmission(QuiclyServer server) : IPeerAdmission
    {
        public AdmissionResult Admit(in HelloInfo hello, QuiclyPeer peer) => server.OnHello(in hello, peer);
    }
}
