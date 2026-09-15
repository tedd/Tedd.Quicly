namespace Tedd.Quicly.Acme.Challenges;

/// <summary>Key authorization strings (RFC 8555 §8.1): <c>token || '.' || base64url(JWK thumbprint)</c>.</summary>
public static class KeyAuthorization
{
    /// <summary>Computes the key authorization for a challenge token and account key.</summary>
    /// <exception cref="ArgumentException">The token does not match <c>[A-Za-z0-9_-]{22,}</c>.</exception>
    public static string Compute(string token, AcmeAccountKey accountKey)
    {
        ChallengeToken.Validate(token);
        ArgumentNullException.ThrowIfNull(accountKey);
        return string.Concat(token, ".", accountKey.Thumbprint);
    }
}
