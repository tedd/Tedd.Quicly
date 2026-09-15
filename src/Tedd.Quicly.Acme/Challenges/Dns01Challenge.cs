using System.Security.Cryptography;
using System.Text;

namespace Tedd.Quicly.Acme.Challenges;

/// <summary>Helpers for the <c>dns-01</c> challenge (RFC 8555 §8.4).</summary>
public static class Dns01Challenge
{
    /// <summary>Label prefixed to the domain to form the TXT record name.</summary>
    public const string RecordLabel = "_acme-challenge";

    /// <summary>Returns <c>_acme-challenge.&lt;domain&gt;</c>; a leading <c>*.</c> (wildcard) is stripped first.</summary>
    public static string GetRecordName(string domain)
    {
        ArgumentException.ThrowIfNullOrEmpty(domain);
        if (domain.StartsWith("*.", StringComparison.Ordinal))
        {
            domain = domain[2..];
        }

        return string.Concat(RecordLabel, ".", domain);
    }

    /// <summary>Returns the TXT record value: <c>base64url(SHA-256(keyAuthorization))</c>.</summary>
    public static string ComputeTxtValue(string keyAuthorization)
    {
        ArgumentException.ThrowIfNullOrEmpty(keyAuthorization);
        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(Encoding.ASCII.GetBytes(keyAuthorization), hash);
        return Base64UrlCodec.Encode(hash);
    }
}
