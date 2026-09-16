using System.Net;
using System.Text;
using Tedd.Quicly.Core.Control;

namespace Tedd.Quicly.Core.Session;

/// <summary>
/// Server-side admission of a QUICLY session (ADR 0009: the second admission check, on the Hello). Runs on the game
/// thread inside <see cref="QuiclyPeer.Poll"/> once the version, channel table and datagram checks passed.
/// </summary>
public interface IPeerAdmission
{
    /// <summary>
    /// Decides whether <paramref name="peer"/> is admitted. Return <see cref="AdmissionResult.Pending"/> to decide later
    /// with <see cref="QuiclyPeer.CompleteAdmission"/> (within <see cref="PeerOptions.AdmissionTimeout"/>).
    /// </summary>
    /// <param name="hello">The client's Hello; its spans are valid only during this call.</param>
    /// <param name="peer">The connection being admitted.</param>
    /// <returns>The decision.</returns>
    AdmissionResult Admit(in HelloInfo hello, QuiclyPeer peer);
}

/// <summary>Kind of <see cref="AdmissionResult"/>.</summary>
public enum AdmissionDecision : byte
{
    /// <summary>Admit the session.</summary>
    Accept = 0,

    /// <summary>Refuse it with a HelloAck status.</summary>
    Reject = 1,

    /// <summary>Decide later through <see cref="QuiclyPeer.CompleteAdmission"/>.</summary>
    Pending = 2,
}

/// <summary>An admission decision (<see cref="IPeerAdmission.Admit"/>, <see cref="QuiclyPeer.CompleteAdmission"/>).</summary>
public readonly struct AdmissionResult
{
    private AdmissionResult(AdmissionDecision decision, HelloStatus status, string? reason, ReadOnlyMemory<byte> sessionToken, ulong sessionId, uint epoch)
    {
        Decision = decision;
        Status = status;
        Reason = reason;
        SessionToken = sessionToken;
        SessionId = sessionId;
        Epoch = epoch;
    }

    /// <summary>The decision.</summary>
    public AdmissionDecision Decision { get; }

    /// <summary>The HelloAck status: <see cref="HelloStatus.Accepted"/> for an accept, the refusal status for a reject.</summary>
    public HelloStatus Status { get; }

    /// <summary>Reason text sent with a rejection (at most 512 bytes of UTF-8), or <see langword="null"/>.</summary>
    public string? Reason { get; }

    /// <summary>Session token sent in the HelloAck of an accept (at most 4 096 bytes; typically minted by <see cref="SessionTokenAuthority"/>).</summary>
    public ReadOnlyMemory<byte> SessionToken { get; }

    /// <summary>Session id sent in the HelloAck; 0 lets the peer choose a random one.</summary>
    public ulong SessionId { get; }

    /// <summary>Epoch of the admitted connection (1 for a fresh session, strictly increasing per session on resume).</summary>
    public uint Epoch { get; }

    /// <summary>A pending decision; complete it with <see cref="QuiclyPeer.CompleteAdmission"/>.</summary>
    public static AdmissionResult Pending => new(AdmissionDecision.Pending, HelloStatus.Accepted, null, default, 0, 0);

    /// <summary>Admits the session.</summary>
    /// <param name="sessionToken">Token for a later resume (empty = none).</param>
    /// <param name="sessionId">Session id; 0 = random.</param>
    /// <param name="epoch">Epoch, at least 1.</param>
    /// <returns>The decision.</returns>
    /// <exception cref="ArgumentException">The token is longer than 4 096 bytes or <paramref name="epoch"/> is 0.</exception>
    public static AdmissionResult Accept(ReadOnlyMemory<byte> sessionToken = default, ulong sessionId = 0, uint epoch = 1)
    {
        if (sessionToken.Length > ControlCodec.MaxTokenLength)
        {
            throw new ArgumentException($"A session token is at most {ControlCodec.MaxTokenLength} bytes.", nameof(sessionToken));
        }

        if (epoch == 0)
        {
            throw new ArgumentException("Epochs start at 1.", nameof(epoch));
        }

        return new AdmissionResult(AdmissionDecision.Accept, HelloStatus.Accepted, null, sessionToken, sessionId, epoch);
    }

    /// <summary>Refuses the session. The client sees <paramref name="status"/> and the connection closes with <see cref="QuiclyErrorCode.AdmissionRejected"/>.</summary>
    /// <param name="status">
    /// <see cref="HelloStatus.Rejected"/> (every token or policy failure, never distinguished), <see cref="HelloStatus.ServerFull"/>,
    /// <see cref="HelloStatus.InternalError"/>, or one of the statuses the peer itself decides (1, 2, 7).
    /// </param>
    /// <param name="reason">Reason text, at most 512 bytes of UTF-8.</param>
    /// <returns>The decision.</returns>
    /// <exception cref="ArgumentException"><paramref name="status"/> is <see cref="HelloStatus.Accepted"/>, informational or undefined, or the reason is too long.</exception>
    public static AdmissionResult Reject(HelloStatus status, string? reason = null)
    {
        if (status is HelloStatus.Accepted or HelloStatus.Informational || !ControlCodec.IsDefined(status))
        {
            throw new ArgumentException($"{status} is not a refusal status.", nameof(status));
        }

        if (reason is not null && Encoding.UTF8.GetByteCount(reason) > ControlCodec.MaxReasonLength)
        {
            throw new ArgumentException($"A reason is at most {ControlCodec.MaxReasonLength} bytes of UTF-8.", nameof(reason));
        }

        return new AdmissionResult(AdmissionDecision.Reject, status, reason, default, 0, 0);
    }
}

/// <summary>The client's Hello as seen by <see cref="IPeerAdmission.Admit"/>. Spans are valid only during that call.</summary>
public readonly ref struct HelloInfo
{
    /// <summary>The protocol version the client speaks (always 1 here: other versions are answered by the peer).</summary>
    public ushort Version { get; init; }

    /// <summary>Hello flags.</summary>
    public HelloFlags Flags { get; init; }

    /// <summary>The client's table hash (equal to the server's here: a mismatch is answered by the peer).</summary>
    public ulong TableHash { get; init; }

    /// <summary>The last epoch the client saw (informational; 0 = fresh session).</summary>
    public uint LastEpoch { get; init; }

    /// <summary>The presented session token (empty = fresh session). Untrusted: validate with <see cref="SessionTokenAuthority"/>.</summary>
    public ReadOnlySpan<byte> SessionToken { get; init; }

    /// <summary>The presented authentication token. Untrusted.</summary>
    public ReadOnlySpan<byte> AuthToken { get; init; }

    /// <summary>The client's capabilities.</summary>
    public PeerCaps Caps { get; init; }

    /// <summary>The largest datagram payload the client will process (0 = no cap).</summary>
    public ushort MaxReceiveDatagram { get; init; }

    /// <summary>The client's address as reported by the transport.</summary>
    public IPEndPoint? RemoteEndPoint { get; init; }
}
