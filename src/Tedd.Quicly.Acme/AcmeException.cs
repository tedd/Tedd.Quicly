using System.Net;
using Tedd.Quicly.Acme.Models;

namespace Tedd.Quicly.Acme;

/// <summary>Raised when the ACME server returns a problem document or the client detects a protocol-level failure.</summary>
public sealed class AcmeException : Exception
{
    /// <summary>Creates an exception wrapping a problem document.</summary>
    public AcmeException(AcmeProblem problem, HttpStatusCode? statusCode = null, Exception? innerException = null)
        : base(problem?.ToString() ?? AcmeErrorTypes.Unknown, innerException)
    {
        Problem = problem ?? new AcmeProblem { Type = AcmeErrorTypes.Unknown };
        StatusCode = statusCode;
    }

    /// <summary>The structured problem (type, detail, status, subproblems).</summary>
    public AcmeProblem Problem { get; }

    /// <summary>HTTP status code of the failing response, when the error came from the server.</summary>
    public HttpStatusCode? StatusCode { get; }

    /// <summary>Server-suggested wait (from <c>Retry-After</c>) before retrying, e.g. on <c>rateLimited</c>.</summary>
    public TimeSpan? RetryAfter { get; init; }

    /// <summary>Shortcut for <c>Problem.Type</c>.</summary>
    public string? Type => Problem.Type;

    /// <summary>True when the problem is of the given type (see <see cref="AcmeErrorTypes"/>).</summary>
    public bool IsType(string type) => Problem.IsType(type);

    /// <summary>Creates a client-side (not server-originated) exception.</summary>
    public static AcmeException Client(string type, string detail, Exception? innerException = null)
    {
        return new AcmeException(new AcmeProblem { Type = type, Detail = detail }, null, innerException);
    }
}
