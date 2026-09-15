namespace Tedd.Quicly.Http;

/// <summary>
/// A request handler. The server invokes handlers in registration order; the first one that returns
/// <see langword="true"/> owns the response (it must have sent, or started sending, a response). A handler that
/// returns <see langword="false"/> must not have written anything.
/// </summary>
public interface IHttpHandler
{
    /// <summary>Tries to handle <paramref name="context"/>.</summary>
    /// <returns><see langword="true"/> when the request was handled and no further handler should run.</returns>
    ValueTask<bool> TryHandleAsync(HttpRequestContext context, CancellationToken cancellationToken);
}

/// <summary>
/// A handler that opts in to request bodies. Bodies are refused (413) by default; the server accepts
/// <c>Content-Length</c> values up to the largest opt-in across all registered handlers (and
/// <see cref="HttpServerLimits.MaxRequestBodyBytes"/>).
/// </summary>
public interface IHttpBodyHandler : IHttpHandler
{
    /// <summary>Largest request body (in bytes) this handler wants to receive; 0 declines bodies.</summary>
    long MaxRequestBodyBytes { get; }
}
