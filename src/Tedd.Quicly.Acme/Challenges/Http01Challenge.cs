namespace Tedd.Quicly.Acme.Challenges;

/// <summary>Constants and helpers for the <c>http-01</c> challenge (RFC 8555 §8.3).</summary>
public static class Http01Challenge
{
    /// <summary>The well-known path prefix the CA fetches over plain HTTP on port 80.</summary>
    public const string WellKnownPathPrefix = "/.well-known/acme-challenge/";

    /// <summary>Content type of the response body (the key authorization).</summary>
    public const string ContentType = "text/plain";

    /// <summary>Returns the request path the CA will fetch for <paramref name="token"/>.</summary>
    /// <exception cref="ArgumentException">The token is not a valid challenge token (see <see cref="ChallengeToken.IsValid"/>).</exception>
    public static string GetPath(string token)
    {
        ChallengeToken.Validate(token);
        return WellKnownPathPrefix + token;
    }

    /// <summary>
    /// Extracts the token from a request path, or <see langword="null"/> when the path is not a challenge path or the
    /// token does not match <c>[A-Za-z0-9_-]{22,}</c> (responders must answer only such tokens, ADR 0009).
    /// </summary>
    public static string? TryGetToken(string path)
    {
        if (path is null || !path.StartsWith(WellKnownPathPrefix, StringComparison.Ordinal))
        {
            return null;
        }

        string token = path[WellKnownPathPrefix.Length..];
        return ChallengeToken.IsValid(token) ? token : null;
    }
}

/// <summary>Validation of ACME challenge tokens: RFC 8555 §8.3 base64url, at least 128 bits of entropy → <c>[A-Za-z0-9_-]{22,}</c>.</summary>
public static class ChallengeToken
{
    /// <summary>Minimum token length (22 base64url characters encode 128 bits).</summary>
    public const int MinimumLength = 22;

    /// <summary>True when <paramref name="token"/> matches <c>[A-Za-z0-9_-]{22,}</c>.</summary>
    public static bool IsValid(ReadOnlySpan<char> token)
    {
        if (token.Length < MinimumLength)
        {
            return false;
        }

        for (int i = 0; i < token.Length; i++)
        {
            char c = token[i];
            bool ok = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '_';
            if (!ok)
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Throws when <paramref name="token"/> is not a valid challenge token.</summary>
    /// <exception cref="ArgumentException">The token is null, empty or does not match <c>[A-Za-z0-9_-]{22,}</c>.</exception>
    public static void Validate(string token)
    {
        ArgumentException.ThrowIfNullOrEmpty(token);
        if (!IsValid(token))
        {
            throw new ArgumentException("Challenge token must match [A-Za-z0-9_-]{22,}.", nameof(token));
        }
    }
}
