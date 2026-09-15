namespace Tedd.Quicly.Http.Handlers;

/// <summary>Answers <c>GET /healthz</c> (configurable path) with <c>200 text/plain "ok"</c>.</summary>
public sealed class HealthHandler : IHttpHandler
{
    private readonly string _path;

    /// <summary>Creates a health handler for <paramref name="path"/>.</summary>
    public HealthHandler(string path = "/healthz")
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        _path = path;
    }

    /// <summary>The path served.</summary>
    public string Path => _path;

    /// <inheritdoc/>
    public async ValueTask<bool> TryHandleAsync(HttpRequestContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (!string.Equals(context.Path, _path, StringComparison.Ordinal))
            return false;
        var response = context.Response;
        if (!string.Equals(context.Method, "GET", StringComparison.Ordinal) && !string.Equals(context.Method, "HEAD", StringComparison.Ordinal))
        {
            response.StatusCode = 405;
            response.Headers.Set("Allow", "GET, HEAD");
            await response.SendTextAsync("405 Method Not Allowed", cancellationToken: cancellationToken).ConfigureAwait(false);
            return true;
        }
        response.Headers.Set("Cache-Control", "no-store");
        await response.SendTextAsync("ok", cancellationToken: cancellationToken).ConfigureAwait(false);
        return true;
    }
}
