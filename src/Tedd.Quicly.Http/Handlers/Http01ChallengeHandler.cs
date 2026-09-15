using System.Buffers;
using System.Collections.Concurrent;
using Tedd.Quicly.Acme;

namespace Tedd.Quicly.Http.Handlers;

/// <summary>
/// Serves ACME <c>http-01</c> challenge responses from memory at <c>/.well-known/acme-challenge/{token}</c>
/// (RFC 8555 §8.3) and implements <see cref="IHttp01Responder"/> so the ACME client can publish into it.
/// </summary>
public sealed class Http01ChallengeHandler : IHttpHandler, IHttp01Responder
{
    /// <summary>The well-known path prefix.</summary>
    public const string PathPrefix = "/.well-known/acme-challenge/";

    private static readonly SearchValues<char> TokenChars = SearchValues.Create("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_");

    private readonly ConcurrentDictionary<string, string> _challenges = new(StringComparer.Ordinal);

    /// <summary>Number of published challenges.</summary>
    public int Count => _challenges.Count;

    /// <inheritdoc/>
    public ValueTask PublishAsync(string token, string keyAuthorization, CancellationToken ct)
    {
        ValidateToken(token);
        ArgumentException.ThrowIfNullOrEmpty(keyAuthorization);
        _challenges[token] = keyAuthorization;
        return default;
    }

    /// <inheritdoc/>
    public ValueTask RemoveAsync(string token, CancellationToken ct)
    {
        ValidateToken(token);
        _challenges.TryRemove(token, out _);
        return default;
    }

    /// <summary>Returns the key authorization published for <paramref name="token"/>.</summary>
    public bool TryGet(string token, out string keyAuthorization)
    {
        ArgumentNullException.ThrowIfNull(token);
        if (_challenges.TryGetValue(token, out var v))
        {
            keyAuthorization = v;
            return true;
        }
        keyAuthorization = string.Empty;
        return false;
    }

    /// <inheritdoc/>
    public ValueTask<bool> TryHandleAsync(HttpRequestContext context, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(context);
        string path = context.Path;
        if (!path.StartsWith(PathPrefix, StringComparison.Ordinal))
            return new ValueTask<bool>(false);

        var response = context.Response;
        if (!string.Equals(context.Method, "GET", StringComparison.Ordinal) && !string.Equals(context.Method, "HEAD", StringComparison.Ordinal))
        {
            response.StatusCode = 405;
            response.Headers.Set("Allow", "GET, HEAD");
            return Complete(response.SendTextAsync("405 Method Not Allowed", cancellationToken: cancellationToken));
        }

        var token = path.AsSpan(PathPrefix.Length);
        if (token.IsEmpty || token.ContainsAnyExcept(TokenChars) || !_challenges.TryGetValue(path[PathPrefix.Length..], out var keyAuthorization))
        {
            response.StatusCode = 404;
            return Complete(response.SendTextAsync("404 Not Found", cancellationToken: cancellationToken));
        }

        response.StatusCode = 200;
        response.Headers.Set("Cache-Control", "no-store");
        return Complete(response.SendTextAsync(keyAuthorization, "application/octet-stream", cancellationToken));
    }

    private static async ValueTask<bool> Complete(ValueTask send)
    {
        await send.ConfigureAwait(false);
        return true;
    }

    private static void ValidateToken(string token)
    {
        ArgumentException.ThrowIfNullOrEmpty(token);
        if (token.AsSpan().ContainsAnyExcept(TokenChars))
            throw new ArgumentException("Token must be base64url (A-Z, a-z, 0-9, '-', '_').", nameof(token));
    }
}
