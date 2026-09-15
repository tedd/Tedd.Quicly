using System.Net;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Server;

/// <summary>
/// Server admission (ADR 0009): a cheap check before any TLS work, and the decision on the client's Hello. The server's
/// policy is <see cref="QuiclyServer.AdmissionPolicy"/>; it starts as the <see cref="DefaultAdmissionPolicy"/>.
/// </summary>
public interface IAdmissionPolicy
{
    /// <summary>
    /// Runs in the listener's new-connection callback (a transport thread) before any TLS work: remote address, SNI, ALPN.
    /// Rejecting drops the connection with no application state. Must be thread-safe, fast and non-blocking.
    /// </summary>
    /// <param name="info">What the listener knows about the connection.</param>
    /// <returns>The decision.</returns>
    PreHandshakeDecision PreHandshake(in NewConnectionInfo info);

    /// <summary>
    /// Decides the client's Hello on the game thread, inside <see cref="QuiclyServer.PollAll"/>, after the peer checked the
    /// version, the channel table and datagram support. <see cref="AdmissionResult.Pending"/> is completed later with
    /// <see cref="QuiclyServer.CompleteAdmission(QuiclyPeer, AdmissionResult)"/>. The server still enforces
    /// <see cref="ServerOptions.MaxPeers"/> on an accept (answering <see cref="HelloStatus.ServerFull"/>). An exception is
    /// reported through <see cref="QuiclyServer.AdmissionFailed"/> and answered <see cref="HelloStatus.InternalError"/>.
    /// </summary>
    /// <param name="hello">The client's Hello; its spans are valid only during the call.</param>
    /// <param name="peer">The connection.</param>
    /// <returns>The decision.</returns>
    AdmissionResult Admit(in HelloInfo hello, QuiclyPeer peer);
}

/// <summary>Answer of an <see cref="AuthTokenValidator"/>.</summary>
public enum AuthTokenDecision : byte
{
    /// <summary>The token is good.</summary>
    Accept = 0,

    /// <summary>The token is bad; the client sees <see cref="HelloStatus.Rejected"/> like any other token failure.</summary>
    Reject = 1,

    /// <summary>
    /// Validation continues asynchronously; call <see cref="QuiclyServer.CompleteAdmission(QuiclyPeer, bool, string?)"/>
    /// with the outcome (from any thread) within <see cref="PeerOptions.AdmissionTimeout"/>. A decision for a connection that
    /// closed meanwhile is ignored.
    /// </summary>
    Pending = 2,
}

/// <summary>Validates a client's auth token (<see cref="ServerAdmissionOptions.AuthTokenValidator"/>). Runs on the game thread.</summary>
/// <param name="context">The token and what is known about the connection; its span is valid only during the call.</param>
/// <returns>Accept, reject, or pending.</returns>
public delegate AuthTokenDecision AuthTokenValidator(in AuthTokenContext context);

/// <summary>What an <see cref="AuthTokenValidator"/> is given. Valid only during the call.</summary>
public readonly ref struct AuthTokenContext
{
    /// <summary>The presented auth token (untrusted). Copy it to keep it beyond the call (for a pending validation).</summary>
    public ReadOnlySpan<byte> AuthToken { get; init; }

    /// <summary>
    /// The connection being admitted. For a resume its <see cref="QuiclyPeer.Tag"/> already holds the session's tag (the
    /// current tag of the connection being replaced, or the tag the session's last connection ended with), so the validator
    /// can check that the token belongs to the same identity.
    /// </summary>
    public QuiclyPeer Peer { get; init; }

    /// <summary>The client's address as reported by the transport.</summary>
    public IPEndPoint? RemoteEndPoint { get; init; }

    /// <summary>True when the client presented a valid session token for a session the server still holds.</summary>
    public bool IsResume { get; init; }

    /// <summary>The session being resumed; 0 for a fresh session.</summary>
    public ulong SessionId { get; init; }

    /// <summary>The resumed session's tag (see <see cref="Peer"/>); 0 for a fresh session.</summary>
    public ulong SessionTag { get; init; }
}

/// <summary>Where an admission was refused.</summary>
public enum AdmissionStage : byte
{
    /// <summary>In the listener's new-connection callback, before TLS (the connection was dropped).</summary>
    PreHandshake = 0,

    /// <summary>On the client's Hello (the client got a refusing HelloAck).</summary>
    Hello = 1,
}

/// <summary>
/// Why an admission was refused. Reported only server-side (<see cref="QuiclyServer.AdmissionFailed"/>): every token failure
/// reaches the client as the same <see cref="HelloStatus.Rejected"/> (PROTOCOL.md §3.4).
/// </summary>
public enum AdmissionFailureReason : byte
{
    /// <summary>The server is stopping or not started.</summary>
    ServerStopping = 0,

    /// <summary>The ALPN is not in <see cref="ServerAdmissionOptions.AllowedAlpns"/>.</summary>
    AlpnNotAllowed,

    /// <summary>The server name is not in <see cref="ServerAdmissionOptions.AllowedServerNames"/>.</summary>
    ServerNameNotAllowed,

    /// <summary><see cref="ServerAdmissionOptions.MaxConnectionsPerAddress"/> was reached.</summary>
    TooManyConnectionsFromAddress,

    /// <summary><see cref="ServerAdmissionOptions.MaxUnadmittedConnections"/> was reached.</summary>
    TooManyUnadmittedConnections,

    /// <summary>Every peer slot is in use.</summary>
    ServerAtCapacity,

    /// <summary>The address has too many recent token failures (<see cref="AuthFailureRateLimiter"/>).</summary>
    AddressRateLimited,

    /// <summary>The auth-token validator rejected the token (or a pending validation completed with a rejection).</summary>
    AuthTokenRejected,

    /// <summary>The admission policy or the auth-token validator threw (see <see cref="AdmissionFailure.Exception"/>).</summary>
    PolicyFault,

    /// <summary>The session token has the wrong length or format version.</summary>
    SessionTokenMalformed,

    /// <summary>The session token's HMAC does not verify.</summary>
    SessionTokenBadSignature,

    /// <summary>The session token expired.</summary>
    SessionTokenExpired,

    /// <summary>The session token was already used.</summary>
    SessionTokenReplayed,

    /// <summary>The token replay cache is full of unexpired entries (fail closed).</summary>
    SessionTokenReplayCacheFull,

    /// <summary>The session named by the token no longer exists (it ended, or the server restarted with the same key).</summary>
    SessionUnknown,

    /// <summary>A newer token was issued for the session (the token's epoch is not the session's current epoch).</summary>
    SessionTokenSuperseded,

    /// <summary>The session's grace period ran out before the resume.</summary>
    SessionExpired,

    /// <summary>
    /// The session resumed too often (<see cref="ServerAdmissionOptions.MinResumeInterval"/>,
    /// <see cref="ServerAdmissionOptions.ResumeBurst"/>). Answered <see cref="HelloStatus.Rejected"/> but not charged to the
    /// failure rate limiter; the token stays usable.
    /// </summary>
    ResumeTooSoon,

    /// <summary><see cref="ServerOptions.MaxPeers"/> sessions are admitted (answered <see cref="HelloStatus.ServerFull"/>).</summary>
    ServerFull,

    /// <summary>The server peer could not be created (see <see cref="AdmissionFailure.Exception"/>).</summary>
    PeerCreationFailed,

    /// <summary>
    /// The session ended with a deliberate close of its connection (a goodbye, or a kick by the server) that was still under
    /// way when the resume arrived.
    /// </summary>
    SessionEnded,
}

/// <summary>A refused admission, with its real cause (<see cref="QuiclyServer.AdmissionFailed"/>).</summary>
/// <param name="Stage">Before the handshake or on the Hello.</param>
/// <param name="Reason">The cause.</param>
/// <param name="RemoteEndPoint">The client's address, when known.</param>
/// <param name="SessionId">The session named by a (valid-looking) session token, otherwise 0.</param>
/// <param name="Detail">Text supplied by the application (a pending validation's reason), or <see langword="null"/>.</param>
/// <param name="Exception">The exception behind <see cref="AdmissionFailureReason.PolicyFault"/> or <see cref="AdmissionFailureReason.PeerCreationFailed"/>.</param>
public readonly record struct AdmissionFailure(
    AdmissionStage Stage,
    AdmissionFailureReason Reason,
    IPEndPoint? RemoteEndPoint,
    ulong SessionId = 0,
    string? Detail = null,
    Exception? Exception = null)
{
    /// <summary>True for failures charged to the per-address <see cref="AuthFailureRateLimiter"/> (auth and session token failures).</summary>
    public bool IsTokenFailure => Reason is AdmissionFailureReason.AuthTokenRejected
        or AdmissionFailureReason.SessionTokenMalformed or AdmissionFailureReason.SessionTokenBadSignature
        or AdmissionFailureReason.SessionTokenExpired or AdmissionFailureReason.SessionTokenReplayed
        or AdmissionFailureReason.SessionTokenReplayCacheFull or AdmissionFailureReason.SessionUnknown
        or AdmissionFailureReason.SessionTokenSuperseded or AdmissionFailureReason.SessionExpired
        or AdmissionFailureReason.SessionEnded;
}
