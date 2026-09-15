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
