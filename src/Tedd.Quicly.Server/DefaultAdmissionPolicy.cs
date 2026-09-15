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
/// session token is inspected (<see cref="SessionTokenAuthority.TryInspect"/>, which does not consume it) and must name a
/// session the server still holds, in its current epoch, within its grace period, admitted at least
/// <see cref="ServerAdmissionOptions.MinResumeInterval"/> ago. Then the auth token goes to
/// <see cref="ServerAdmissionOptions.AuthTokenValidator"/> (accept, reject, or pending until
/// <see cref="QuiclyServer.CompleteAdmission(QuiclyPeer, bool, string?)"/>), then capacity is reserved
/// (<see cref="ServerOptions.MaxPeers"/>, answered <see cref="HelloStatus.ServerFull"/>; a resume that replaces a live
/// connection may exceed it by that connection). Only then is a resume committed: <see cref="SessionTokenAuthority.TryValidate"/>
/// consumes the token, the epoch is incremented, a new token is minted and a live connection of the session is closed with
/// <see cref="QuiclyErrorCode.SessionReplaced"/>. A refusal before the commit leaves the client's token usable.</para>
/// <para>Every token failure, auth or session, is answered <see cref="HelloStatus.Rejected"/> with no reason text, charged
/// to the per-address <see cref="AuthFailureRateLimiter"/> and reported with its real cause through
/// <see cref="QuiclyServer.AdmissionFailed"/>.</para>
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

        _server.ReportFailure(new AdmissionFailure(AdmissionStage.PreHandshake, reason, remote));
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
            SessionTokenStatus status = _server.Tokens.TryInspect(hello.SessionToken, out ulong sessionId, out uint epoch);
            if (status != SessionTokenStatus.Valid)
            {
                return TokenFailure(ReasonOf(status), remote, 0);
            }

            SessionRecord? record = _server.Sessions.Find(sessionId);
            if (CheckSession(record, epoch, _server.Clock.NowMicros, checkResumeInterval: true) is { } problem)
            {
                return TokenFailure(problem, remote, sessionId);
            }

            state.Resume = true;
            state.SessionId = sessionId;
            state.Epoch = epoch;
            state.Token = hello.SessionToken.ToArray();
            tag = record!.Tag;
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

        if (!_server.IsAccepting)
        {
            return Refuse(AdmissionFailureReason.ServerStopping, remote, state.SessionId, HelloStatus.ServerFull);
        }

        return Commit(peer, state, remote);
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

            SessionRecord created = _server.Sessions.Create(peer, now);
            _server.BindSession(peer, created);
            _server.CountSessionCreated();
            return AdmissionResult.Accept(_server.MintToken(created.SessionId, 1, now), created.SessionId, 1);
        }

        // The state may have changed while the validation was pending: check the session again.
        SessionRecord? record = _server.Sessions.Find(state.SessionId);
        if (CheckSession(record, state.Epoch, now, checkResumeInterval: false) is { } problem)
        {
            return TokenFailure(problem, remote, state.SessionId);
        }

        QuiclyPeer? previous = record!.Peer;
        bool replacing = previous is not null && !ReferenceEquals(previous, peer);
        if (!_server.TryReserve(peer, allowOverCapacity: replacing))
        {
            return Refuse(AdmissionFailureReason.ServerFull, remote, state.SessionId, HelloStatus.ServerFull);
        }

        SessionTokenStatus status = _server.Tokens.TryValidate(state.Token, out _, out _);
        if (status != SessionTokenStatus.Valid)
        {
            _server.Unreserve(peer);
            return TokenFailure(ReasonOf(status), remote, state.SessionId);
        }

        uint epoch = record.Epoch + 1;
        _server.Sessions.Attach(record, peer, epoch, now);
        _server.BindSession(peer, record);
        if (replacing)
        {
            _server.ReplaceConnection(previous!);
        }

        _server.CountSessionResumed();
        return AdmissionResult.Accept(_server.MintToken(record.SessionId, epoch, now), record.SessionId, epoch);
    }

    private AdmissionFailureReason? CheckSession(SessionRecord? record, uint epoch, long now, bool checkResumeInterval)
    {
        if (record is null)
        {
            return AdmissionFailureReason.SessionUnknown;
        }

        if (record.Epoch != epoch || record.Epoch == uint.MaxValue)
        {
            return AdmissionFailureReason.SessionTokenSuperseded;
        }

        if (record.Peer is null && now >= record.ExpiresMicros)
        {
            return AdmissionFailureReason.SessionExpired;
        }

        if (checkResumeInterval && now - record.LastAdmittedMicros < _minResumeMicros)
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

    private static AdmissionFailureReason ReasonOf(SessionTokenStatus status) => status switch
    {
        SessionTokenStatus.Malformed => AdmissionFailureReason.SessionTokenMalformed,
        SessionTokenStatus.BadSignature => AdmissionFailureReason.SessionTokenBadSignature,
        SessionTokenStatus.Expired => AdmissionFailureReason.SessionTokenExpired,
        SessionTokenStatus.Replayed => AdmissionFailureReason.SessionTokenReplayed,
        _ => AdmissionFailureReason.SessionTokenReplayCacheFull,
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
