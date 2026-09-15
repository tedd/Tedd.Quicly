namespace Tedd.Quicly.Http.Handlers;

/// <summary>
/// Redirects every plain-HTTP request to the same target on HTTPS. Requests that already arrived over TLS are
/// left to the next handler. Place it after <see cref="Http01ChallengeHandler"/> so challenges stay on port 80.
/// </summary>
public sealed class RedirectToHttpsHandler : IHttpHandler
{
    private readonly int _httpsPort;
    private readonly bool _permanent;

    /// <summary>Creates a redirect handler targeting <paramref name="httpsPort"/> (omitted from the URL when 443).</summary>
    public RedirectToHttpsHandler(int httpsPort = 443, bool permanent = true)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(httpsPort, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(httpsPort, 65535);
        _httpsPort = httpsPort;
        _permanent = permanent;
    }

    /// <inheritdoc/>
    public async ValueTask<bool> TryHandleAsync(HttpRequestContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        if (context.IsSecure)
            return false;
        var response = context.Response;
        var host = context.Host;
        if (string.IsNullOrEmpty(host))
        {
            response.StatusCode = 400;
            await response.SendTextAsync("400 Bad Request", cancellationToken: cancellationToken).ConfigureAwait(false);
            return true;
        }
        host = StripPort(host);
        string target = context.RawTarget;
        if (target.Length == 0 || target[0] != '/')
            target = "/";
        string location = _httpsPort == 443
            ? "https://" + host + target
            : "https://" + host + ":" + _httpsPort.ToString(System.Globalization.CultureInfo.InvariantCulture) + target;

        response.StatusCode = _permanent ? 301 : 307;
        response.Headers.Set("Location", location);
        await response.SendStatusAsync(response.StatusCode, cancellationToken).ConfigureAwait(false);
        return true;
    }

    private static string StripPort(string host)
    {
        if (host.StartsWith('['))
        {
            int close = host.IndexOf(']', StringComparison.Ordinal);
            return close < 0 ? host : host[..(close + 1)];
        }
        int colon = host.IndexOf(':', StringComparison.Ordinal);
        return colon < 0 ? host : host[..colon];
    }
}
