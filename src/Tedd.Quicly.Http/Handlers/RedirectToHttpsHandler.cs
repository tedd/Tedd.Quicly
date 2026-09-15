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
        var rawHost = context.Host;
        if (string.IsNullOrEmpty(rawHost) || !TrySplitHost(rawHost, out var host))
        {
            response.StatusCode = 400;
            await response.SendTextAsync("400 Bad Request", cancellationToken: cancellationToken).ConfigureAwait(false);
            return true;
        }
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

    private static readonly System.Buffers.SearchValues<char> RegNameChars =
        System.Buffers.SearchValues.Create("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-.");

    private static readonly System.Buffers.SearchValues<char> Ipv6LiteralChars =
        System.Buffers.SearchValues.Create("0123456789ABCDEFabcdef:.");

    /// <summary>
    /// Validates a non-empty <c>Host</c> value and returns it without the port. Accepted: a name of letters, digits,
    /// <c>-</c> and <c>.</c>, or a bracketed IPv6 literal, optionally followed by <c>:</c> and at most five digits.
    /// Userinfo, paths, percent-encoding and anything else are refused, so the <c>Location</c> header can only ever
    /// name a plain host.
    /// </summary>
    internal static bool TrySplitHost(string host, out string hostOnly)
    {
        hostOnly = string.Empty;
        ReadOnlySpan<char> s = host;
        int end;
        if (s[0] == '[')
        {
            end = s.IndexOf(']') + 1;
            if (end < 3 || s[1..(end - 1)].ContainsAnyExcept(Ipv6LiteralChars))
                return false;
        }
        else
        {
            end = s.IndexOf(':');
            if (end < 0)
                end = s.Length;
            if (end == 0 || s[..end].ContainsAnyExcept(RegNameChars))
                return false;
        }
        var port = s[end..];
        if (!port.IsEmpty && (port[0] != ':' || port.Length > 6 || port[1..].ContainsAnyExceptInRange('0', '9')))
            return false;
        hostOnly = end == s.Length ? host : host[..end];
        return true;
    }
}
