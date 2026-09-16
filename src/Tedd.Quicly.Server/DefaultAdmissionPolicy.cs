using System.Net;
using System.Text;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Server;

/// <summary>
/// The admission policy a <see cref="QuiclyServer"/> starts with (ADR 0009, PROTOCOL.md §3.4 and §4.1).
/// </summary>
/// <remarks>
/// <para><b>Before TLS</b> (<see cref="PreHandshake"/>, on the listener's thread) it refuses while the server is not
/// accepting, ALPNs and server names outside <see cref="ServerAdmissionOptions.AllowedAlpns"/> and
/// <see cref="ServerAdmissionOptions.AllowedServerNames"/>, addresses the auth-failure rate limiter is blocking, and
/// connections beyond the per-address, unadmitted and slot limits.</para>
/// <para><b>On the Hello</b> (<see cref="Admit"/>, on the game thread inside <see cref="QuiclyServer.PollAll"/>): a presented
/// session token is inspected (<see cref="SessionTokenAuthority.TryInspect"/>, which does not consume it; the token's own
/// expiry is its maximum age, <see cref="ServerSessionOptions.TokenLifetime"/>) and must name a session the server still
/// holds, in its current epoch, whose connection did not end deliberately (a goodbye or a kick, even while that close is
/// still under way), and, once its connection was lost, within <see cref="ServerSessionOptions.Grace"/> of the loss. A
/// session may resume <see cref="ServerAdmissionOptions.ResumeBurst"/> times in quick succession and then once per
/// <see cref="ServerAdmissionOptions.MinResumeInterval"/>. Then the auth token goes to
/// <see cref="ServerAdmissionOptions.AuthTokenValidator"/> (accept, reject, or pending until
/// <see cref="QuiclyServer.CompleteAdmission(QuiclyPeer, bool, string?)"/>), then capacity is reserved
/// (<see cref="ServerOptions.MaxPeers"/>, answered <see cref="HelloStatus.ServerFull"/>; a resume that replaces a live
/// connection may exceed it by that connection). Only then is a resume committed: the token is verified once more and spent
/// in the server's replay cache (<see cref="ServerSessionOptions.ReplayCacheCapacity"/> — bounded, expiring by
/// <see cref="ServerSessionOptions.Grace"/> and evicting its oldest entry rather than ever refusing a resume), the epoch is
/// incremented — which is what makes a token single-use — a new token is minted and a live connection of the session is
/// closed with <see cref="QuiclyErrorCode.SessionReplaced"/>. A refusal before the commit leaves the client's token
/// usable.</para>
/// <para>Every token failure, auth or session, is answered <see cref="HelloStatus.Rejected"/> with no reason text, charged
/// to the per-address <see cref="AuthFailureRateLimiter"/> and reported with its real cause through
/// <see cref="QuiclyServer.AdmissionFailed"/>. A resume refused for its rate (<see cref="AdmissionFailureReason.ResumeTooSoon"/>)
/// is answered the same way but not charged: the token is good, the client only came back too often.</para>
/// <para>To add rules, assign a policy of your own to <see cref="QuiclyServer.AdmissionPolicy"/> that refuses first and
/// delegates the rest to <see cref="QuiclyServer.DefaultAdmissionPolicy"/>, which keeps sessions and resume working.</para>
/// </remarks>
public sealed class DefaultAdmissionPolicy : IAdmissionPolicy
{
    private readonly QuiclyServer _server;
    private readonly byte[][] _alpns;
    private readonly string[] _serverNames;
    private readonly AuthTokenValidator? _validator;
    private readonly long _minResumeMicros;
    private readonly long _resumeToleranceMicros;

    internal DefaultAdmissionPolicy(QuiclyServer server, ServerAdmissionOptions options)
    {
        _server = server;
        _alpns = new byte[options.AllowedAlpns.Count][];
        for (int i = 0; i < _alpns.Length; i++)
        {
            _alpns[i] = Encoding.ASCII.GetBytes(options.AllowedAlpns[i]);
        }

        _serverNames = [.. options.AllowedServerNames];
        _validator = options.AuthTokenValidator;
        _minResumeMicros = QuiclyServer.ToMicros(options.MinResumeInterval);
        _resumeToleranceMicros = _minResumeMicros * options.ResumeBurst;
    }

    /// <inheritdoc/>
    public PreHandshakeDecision PreHandshake(in NewConnectionInfo info)
    {
        IPEndPoint? remote = info.RemoteEndPoint;
        AdmissionFailureReason reason;
        if (!_server.IsAccepting)
        {
            reason = AdmissionFailureReason.ServerStopping;
        }
        else if (_alpns.Length != 0 && !IsAlpnAllowed(in info))
        {
            reason = AdmissionFailureReason.AlpnNotAllowed;
        }
        else if (_serverNames.Length != 0 && !IsServerNameAllowed(info.ServerName))
        {
            reason = AdmissionFailureReason.ServerNameNotAllowed;
        }
        else if (remote is not null && !_server.RateLimiter.IsAllowed(remote.Address))
        {
            reason = AdmissionFailureReason.AddressRateLimited;
        }
        else if (_server.CheckConnectionLimits(remote) is { } limit)
        {
            reason = limit;
        }
        else
        {
            return PreHandshakeDecision.Accept;
        }

        _server.QueueFailure(new AdmissionFailure(AdmissionStage.PreHandshake, reason, remote)); // a transport thread
        return PreHandshakeDecision.Reject;
    }

    /// <inheritdoc/>
    public AdmissionResult Admit(in HelloInfo hello, QuiclyPeer peer)
    {
        ArgumentNullException.ThrowIfNull(peer);
        IPEndPoint? remote = hello.RemoteEndPoint;
        if (!_server.IsAccepting)
        {
            return Refuse(AdmissionFailureReason.ServerStopping, remote, 0, HelloStatus.ServerFull);
        }

        if (remote is not null && !_server.RateLimiter.IsAllowed(remote.Address))
        {
            return Refuse(AdmissionFailureReason.AddressRateLimited, remote, 0, HelloStatus.Rejected);
        }

        PendingAdmission state = new();
        ulong tag = 0;
        if (!hello.SessionToken.IsEmpty)
        {
            long now = _server.Clock.NowMicros;
            SessionTokenStatus status = _server.Tokens.TryInspect(hello.SessionToken, out ulong sessionId, out uint epoch);
            if (status != SessionTokenStatus.Valid)
            {
                return TokenFailure(ReasonOf(status), remote, 0);
            }

            if (_server.Replay.Contains(hello.SessionToken, now))
            {
                // Already spent by a resume that committed (the registry's epoch check would refuse it as well).
                return TokenFailure(AdmissionFailureReason.SessionTokenReplayed, remote, sessionId);
            }

            SessionRecord? record = _server.Sessions.Find(sessionId);
            if (CheckSession(record, epoch, now, checkResumeRate: true) is { } problem)
            {
                return problem == AdmissionFailureReason.ResumeTooSoon
                    ? Refuse(problem, remote, sessionId, HelloStatus.Rejected)
                    : TokenFailure(problem, remote, sessionId);
            }

            state.Resume = true;
            state.SessionId = sessionId;
            state.Epoch = epoch;
            state.Token = hello.SessionToken.ToArray();

            // A live connection's tag is current; the record holds the tag the session's last connection ended with.
            tag = record!.Peer?.Tag ?? record.Tag;
            peer.Tag = tag;
        }

        if (_validator is not null)
        {
            AuthTokenDecision decision;
            try
            {
                AuthTokenContext context = new()
                {
                    AuthToken = hello.AuthToken,
                    Peer = peer,
                    RemoteEndPoint = remote,
                    IsResume = state.Resume,
                    SessionId = state.SessionId,
                    SessionTag = tag,
                };
                decision = _validator(in context);
            }
            catch (Exception exception)
            {
                return Refuse(AdmissionFailureReason.PolicyFault, remote, state.SessionId, HelloStatus.InternalError, exception);
            }

            if (decision == AuthTokenDecision.Pending)
            {
                _server.SetPending(peer, state);
                return AdmissionResult.Pending;
            }

            if (decision != AuthTokenDecision.Accept)
            {
                return TokenFailure(AdmissionFailureReason.AuthTokenRejected, remote, state.SessionId);
            }
        }

        return Commit(peer, state, remote);
    }

    /// <summary>Finishes an admission whose auth validation was pending (game thread).</summary>
    internal AdmissionResult CompletePending(QuiclyPeer peer, PendingAdmission state, bool accepted, string? detail)
    {
        IPEndPoint? remote = peer.RemoteEndPoint;
        if (!accepted)
        {
            return TokenFailure(AdmissionFailureReason.AuthTokenRejected, remote, state.SessionId, detail);
        }

        return Commit(peer, state, remote); // the server refuses before this when it stopped accepting meanwhile
    }

    private AdmissionResult Commit(QuiclyPeer peer, PendingAdmission state, IPEndPoint? remote)
    {
        long now = _server.Clock.NowMicros;
        if (!state.Resume)
        {
            if (!_server.TryReserve(peer, allowOverCapacity: false))
            {
                return Refuse(AdmissionFailureReason.ServerFull, remote, 0, HelloStatus.ServerFull);
            }

            SessionRecord created = _server.Sessions.Create(peer);
            created.ResumeTat = now + _minResumeMicros; // the creation counts as an admission for the resume rate
            _server.BindSession(peer, created);
            _server.CountSessionCreated();
            return AdmissionResult.Accept(_server.MintToken(created.SessionId, 1, now), created.SessionId, 1);
        }

        // The state may have changed while the validation was pending: check the session again.
        SessionRecord? record = _server.Sessions.Find(state.SessionId);
        if (CheckSession(record, state.Epoch, now, checkResumeRate: false) is { } problem)
        {
            return TokenFailure(problem, remote, state.SessionId);
        }

        QuiclyPeer? previous = record!.Peer;
        bool replacing = previous is not null && !ReferenceEquals(previous, peer);
        if (!_server.TryReserve(peer, allowOverCapacity: replacing))
        {
            return Refuse(AdmissionFailureReason.ServerFull, remote, state.SessionId, HelloStatus.ServerFull);
        }

        // The resume commits, so the token is spent now: verify it once more (the signing key may have rotated, or the token
        // may have expired, while a validation was pending) and remember it for one grace period. A full cache evicts its
        // oldest entry instead of refusing (PROTOCOL.md §4.1).
        SessionTokenStatus status = _server.Tokens.TryInspect(state.Token, out _, out _);
        if (status != SessionTokenStatus.Valid || !_server.Replay.TryConsume(state.Token, now))
        {
            _server.Unreserve(peer);
            return TokenFailure(status == SessionTokenStatus.Valid ? AdmissionFailureReason.SessionTokenReplayed : ReasonOf(status), remote, state.SessionId);
        }

        uint epoch = record.Epoch + 1;
        _server.Sessions.Attach(record, peer, epoch);
        record.ResumeTat = Math.Max(record.ResumeTat, now) + _minResumeMicros;
        _server.BindSession(peer, record);
        if (replacing)
        {
            _server.ReplaceConnection(record, previous!);
        }

        _server.CountSessionResumed();
        return AdmissionResult.Accept(_server.MintToken(record.SessionId, epoch, now), record.SessionId, epoch);
    }

    private AdmissionFailureReason? CheckSession(SessionRecord? record, uint epoch, long now, bool checkResumeRate)
    {
        if (record is null)
        {
            return AdmissionFailureReason.SessionUnknown;
        }

        if (record.Epoch != epoch || record.Epoch == uint.MaxValue)
        {
            return AdmissionFailureReason.SessionTokenSuperseded;
        }

        if (record.Peer is { } current && current.State is (PeerState.Closing or PeerState.Closed) && !QuiclyServer.IsResumable(current.CloseReason))
        {
            // Its connection is closing deliberately (a goodbye, or the server kicked the player): the session ends with that
            // close, and a resume must not take it over while the close lingers.
            return AdmissionFailureReason.SessionEnded;
        }

        if (record.Peer is null && now >= record.ExpiresMicros)
        {
            return AdmissionFailureReason.SessionExpired;
        }

        // A token bucket over virtual time: every admission moves ResumeTat one interval past max(ResumeTat, now), and a
        // resume is allowed while ResumeTat is at most ResumeBurst intervals ahead of now.
        if (checkResumeRate && Math.Max(record.ResumeTat, now) - now > _resumeToleranceMicros)
        {
            return AdmissionFailureReason.ResumeTooSoon;
        }

        return null;
    }

    private AdmissionResult TokenFailure(AdmissionFailureReason reason, IPEndPoint? remote, ulong sessionId, string? detail = null)
    {
        if (remote is not null)
        {
            _server.RateLimiter.RecordFailure(remote.Address);
        }

        _server.ReportFailure(new AdmissionFailure(AdmissionStage.Hello, reason, remote, sessionId, detail));
        return AdmissionResult.Reject(HelloStatus.Rejected);
    }

    private AdmissionResult Refuse(AdmissionFailureReason reason, IPEndPoint? remote, ulong sessionId, HelloStatus status, Exception? exception = null)
    {
        _server.ReportFailure(new AdmissionFailure(AdmissionStage.Hello, reason, remote, sessionId, null, exception));
        return AdmissionResult.Reject(status);
    }

    /// <summary>The failure behind a token status; the server's own replay cache reports replays, so only the authority's checks are mapped here.</summary>
    private static AdmissionFailureReason ReasonOf(SessionTokenStatus status) => status switch
    {
        SessionTokenStatus.BadSignature => AdmissionFailureReason.SessionTokenBadSignature,
        SessionTokenStatus.Expired => AdmissionFailureReason.SessionTokenExpired,
        SessionTokenStatus.Replayed => AdmissionFailureReason.SessionTokenReplayed,
        _ => AdmissionFailureReason.SessionTokenMalformed,
    };

    private bool IsAlpnAllowed(in NewConnectionInfo info)
    {
        AlpnBuffer buffer = info.Alpn;
        ReadOnlySpan<byte> alpn = ((ReadOnlySpan<byte>)buffer).Slice(0, info.AlpnLength);
        foreach (byte[] allowed in _alpns)
        {
            if (alpn.SequenceEqual(allowed))
            {
                return true;
            }
        }

        return false;
    }

    private bool IsServerNameAllowed(string? serverName)
    {
        if (serverName is null)
        {
            return false;
        }

        foreach (string allowed in _serverNames)
        {
            if (string.Equals(serverName, allowed, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }
}

/// <summary>What a pending admission needs to finish (game thread).</summary>
internal sealed class PendingAdmission
{
    /// <summary>A valid session token named a held session.</summary>
    public bool Resume;

    /// <summary>The session being resumed.</summary>
    public ulong SessionId;

    /// <summary>The epoch of the presented token.</summary>
    public uint Epoch;

    /// <summary>A copy of the presented token (consumed when the resume commits).</summary>
    public byte[] Token = [];
}
