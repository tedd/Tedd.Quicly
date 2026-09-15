namespace Tedd.Quicly.Http.Handlers;

/// <summary>Handles a routed request; the handler must send (or start) the response.</summary>
public delegate ValueTask HttpRouteHandler(HttpRequestContext context, CancellationToken cancellationToken);

/// <summary>
/// Exact-path and prefix routes to delegates. Exact routes are tried first, then prefix routes from longest to
/// shortest. A path that matches only routes for other methods yields <c>405</c> with an <c>Allow</c> header.
/// <c>GET</c> routes also serve <c>HEAD</c>. Routes that read request bodies need <see cref="MaxRequestBodyBytes"/>
/// raised (bodies are refused server-wide otherwise, ADR 0009).
/// </summary>
public sealed class RouteTable : IHttpBodyHandler
{
    private struct Route
    {
        public string? Method;
        public string Path;
        public HttpRouteHandler Handler;
    }

    private Route[] _exact = new Route[8];
    private int _exactCount;
    private Route[] _prefix = new Route[4];
    private int _prefixCount;

    /// <summary>Number of registered routes.</summary>
    public int Count => _exactCount + _prefixCount;

    /// <summary>Largest request body (bytes) the routes in this table accept; 0 (default) refuses bodies.</summary>
    public long MaxRequestBodyBytes { get; set; }

    /// <summary>Registers an exact-path route. <paramref name="method"/> <see langword="null"/> matches every method; the path <c>*</c> matches <c>OPTIONS *</c>.</summary>
    public RouteTable Map(string? method, string path, HttpRouteHandler handler)
    {
        ValidatePath(path);
        ArgumentNullException.ThrowIfNull(handler);
        var route = new Route { Method = NormalizeMethod(method), Path = path, Handler = handler };
        if (_exactCount == _exact.Length)
            Array.Resize(ref _exact, _exact.Length * 2);
        _exact[_exactCount++] = route;
        return this;
    }

    /// <summary>Registers a prefix route: matches every path that starts with <paramref name="prefix"/>.</summary>
    public RouteTable MapPrefix(string? method, string prefix, HttpRouteHandler handler)
    {
        ValidatePath(prefix);
        ArgumentNullException.ThrowIfNull(handler);
        var route = new Route { Method = NormalizeMethod(method), Path = prefix, Handler = handler };
        if (_prefixCount == _prefix.Length)
            Array.Resize(ref _prefix, _prefix.Length * 2);
        // keep sorted by descending prefix length so the most specific prefix wins
        int i = _prefixCount;
        while (i > 0 && _prefix[i - 1].Path.Length < prefix.Length)
        {
            _prefix[i] = _prefix[i - 1];
            i--;
        }
        _prefix[i] = route;
        _prefixCount++;
        return this;
    }

    /// <summary>Registers a <c>GET</c> (and <c>HEAD</c>) route.</summary>
    public RouteTable MapGet(string path, HttpRouteHandler handler) => Map("GET", path, handler);

    /// <summary>Registers a <c>POST</c> route.</summary>
    public RouteTable MapPost(string path, HttpRouteHandler handler) => Map("POST", path, handler);

    /// <summary>Registers a <c>PUT</c> route.</summary>
    public RouteTable MapPut(string path, HttpRouteHandler handler) => Map("PUT", path, handler);

    /// <summary>Registers a <c>DELETE</c> route.</summary>
    public RouteTable MapDelete(string path, HttpRouteHandler handler) => Map("DELETE", path, handler);

    /// <inheritdoc/>
    public ValueTask<bool> TryHandleAsync(HttpRequestContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        string path = context.Path;
        string method = context.Method;
        bool pathMatched = false;

        for (int i = 0; i < _exactCount; i++)
        {
            ref var r = ref _exact[i];
            if (!string.Equals(r.Path, path, StringComparison.Ordinal))
                continue;
            if (MethodMatches(r.Method, method))
                return Invoke(r.Handler, context, cancellationToken);
            pathMatched = true;
        }
        for (int i = 0; i < _prefixCount; i++)
        {
            ref var r = ref _prefix[i];
            if (!path.StartsWith(r.Path, StringComparison.Ordinal))
                continue;
            if (MethodMatches(r.Method, method))
                return Invoke(r.Handler, context, cancellationToken);
            pathMatched = true;
        }
        if (!pathMatched)
            return new ValueTask<bool>(false);
        return MethodNotAllowedAsync(context, path, cancellationToken);
    }

    private static async ValueTask<bool> Invoke(HttpRouteHandler handler, HttpRequestContext context, CancellationToken cancellationToken)
    {
        await handler(context, cancellationToken).ConfigureAwait(false);
        return true;
    }

    private async ValueTask<bool> MethodNotAllowedAsync(HttpRequestContext context, string path, CancellationToken cancellationToken)
    {
        var allow = new System.Text.StringBuilder();
        AppendAllowed(allow, _exact, _exactCount, path, exact: true);
        AppendAllowed(allow, _prefix, _prefixCount, path, exact: false);
        context.Response.StatusCode = 405;
        context.Response.Headers.Set("Allow", allow.ToString());
        await context.Response.SendTextAsync("405 Method Not Allowed", cancellationToken: cancellationToken).ConfigureAwait(false);
        return true;
    }

    private static void AppendAllowed(System.Text.StringBuilder allow, Route[] routes, int count, string path, bool exact)
    {
        for (int i = 0; i < count; i++)
        {
            ref var r = ref routes[i];
            bool matches = exact ? string.Equals(r.Path, path, StringComparison.Ordinal) : path.StartsWith(r.Path, StringComparison.Ordinal);
            if (!matches)
                continue;
            string m = r.Method ?? "*";
            if (allow.Length > 0)
                allow.Append(", ");
            allow.Append(m);
            if (string.Equals(m, "GET", StringComparison.Ordinal))
                allow.Append(", HEAD");
        }
    }

    private static bool MethodMatches(string? routeMethod, string requestMethod)
        => routeMethod is null
           || string.Equals(routeMethod, requestMethod, StringComparison.Ordinal)
           || (string.Equals(routeMethod, "GET", StringComparison.Ordinal) && string.Equals(requestMethod, "HEAD", StringComparison.Ordinal));

    private static string? NormalizeMethod(string? method)
    {
        if (method is null)
            return null;
        ArgumentException.ThrowIfNullOrWhiteSpace(method);
        return method.ToUpperInvariant();
    }

    private static void ValidatePath(string path)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        if (path[0] != '/' && path != "*")
            throw new ArgumentException("Route paths must start with '/' (or be '*' for OPTIONS *).", nameof(path));
    }
}
