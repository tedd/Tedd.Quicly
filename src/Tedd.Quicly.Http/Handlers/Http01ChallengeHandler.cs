using System.Buffers;
using System.Collections.Concurrent;
using Tedd.Quicly.Acme;

namespace Tedd.Quicly.Http.Handlers;

/// <summary>
/// Serves ACME <c>http-01</c> challenge responses from memory at <c>/.well-known/acme-challenge/{token}</c>
/// (RFC 8555 §8.3) and implements <see cref="IHttp01Responder"/> so the ACME client can publish into it.
/// Tokens must match <c>[A-Za-z0-9_-]{22,}</c> (ADR 0009) and every entry expires <see cref="Lifetime"/> after
/// it was published (default 10 minutes) even if the ACME client never removes it.
/// </summary>
public sealed class Http01ChallengeHandler : IHttpHandler, IHttp01Responder
{
    /// <summary>The well-known path prefix.</summary>
    public const string PathPrefix = "/.well-known/acme-challenge/";

    /// <summary>Shortest token accepted (RFC 8555 §8.3 requires at least 128 bits of entropy, i.e. 22 base64url characters).</summary>
    public const int MinTokenLength = 22;

    /// <summary>Default entry lifetime.</summary>
    public static readonly TimeSpan DefaultLifetime = TimeSpan.FromMinutes(10);

    private static readonly SearchValues<char> TokenChars = SearchValues.Create("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_");

    private readonly struct Entry(string keyAuthorization, long expiresAt)
    {
        public readonly string KeyAuthorization = keyAuthorization;
        public readonly long ExpiresAt = expiresAt;
    }

    private readonly ConcurrentDictionary<string, Entry> _challenges = new(StringComparer.Ordinal);
    private readonly TimeProvider _time;

    /// <summary>Creates a handler. <paramref name="timeProvider"/> drives expiry (tests inject a fake clock).</summary>
    public Http01ChallengeHandler(TimeProvider? timeProvider = null, TimeSpan? lifetime = null)
    {
        _time = timeProvider ?? TimeProvider.System;
        Lifetime = lifetime ?? DefaultLifetime;
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(Lifetime, TimeSpan.Zero);
    }

    /// <summary>How long a published challenge stays served.</summary>
    public TimeSpan Lifetime { get; }

    /// <summary>Number of published challenges, expired entries included until they are purged.</summary>
    public int Count => _challenges.Count;

    /// <inheritdoc/>
    public ValueTask PublishAsync(string token, string keyAuthorization, CancellationToken ct)
    {
        ValidateToken(token);
        ArgumentException.ThrowIfNullOrEmpty(keyAuthorization);
        long now = _time.GetTimestamp();
        PurgeExpired(now);
        _challenges[token] = new Entry(keyAuthorization, now + TicksFor(Lifetime));
        return default;
    }

    /// <inheritdoc/>
    public ValueTask RemoveAsync(string token, CancellationToken ct)
    {
        ValidateToken(token);
        _challenges.TryRemove(token, out _);
        return default;
    }

    /// <summary>Returns the key authorization published for <paramref name="token"/> when it exists and has not expired.</summary>
    public bool TryGet(string token, out string keyAuthorization)
    {
        ArgumentNullException.ThrowIfNull(token);
        if (_challenges.TryGetValue(token, out var entry))
        {
            if (entry.ExpiresAt - _time.GetTimestamp() > 0)
            {
                keyAuthorization = entry.KeyAuthorization;
                return true;
            }
            _challenges.TryRemove(new KeyValuePair<string, Entry>(token, entry));
        }
        keyAuthorization = string.Empty;
        return false;
    }

    /// <summary>Whether <paramref name="token"/> has the shape of an ACME challenge token.</summary>
    public static bool IsValidToken(ReadOnlySpan<char> token)
        => token.Length >= MinTokenLength && !token.ContainsAnyExcept(TokenChars);

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
        if (!IsValidToken(token) || !TryGet(path[PathPrefix.Length..], out var keyAuthorization))
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

    private void PurgeExpired(long now)
    {
        foreach (var pair in _challenges)
        {
            if (pair.Value.ExpiresAt - now <= 0)
                _challenges.TryRemove(pair);
        }
    }

    private long TicksFor(TimeSpan duration) => (long)(duration.TotalSeconds * _time.TimestampFrequency);

    private static void ValidateToken(string token)
    {
        ArgumentException.ThrowIfNullOrEmpty(token);
        if (!IsValidToken(token))
            throw new ArgumentException("Token must be at least " + MinTokenLength.ToString(System.Globalization.CultureInfo.InvariantCulture) + " base64url characters (A-Z, a-z, 0-9, '-', '_').", nameof(token));
    }
}
