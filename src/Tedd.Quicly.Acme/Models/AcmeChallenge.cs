namespace Tedd.Quicly.Acme.Models;

/// <summary>ACME challenge object (RFC 8555 §8).</summary>
public sealed record AcmeChallenge
{
    /// <summary>Challenge type: <c>http-01</c>, <c>dns-01</c> or <c>tls-alpn-01</c> (see <see cref="AcmeChallengeTypes"/>).</summary>
    public required string Type { get; init; }

    /// <summary>URL to POST the challenge response to.</summary>
    public required Uri Url { get; init; }

    /// <summary>Status: <c>pending</c>, <c>processing</c>, <c>valid</c> or <c>invalid</c>.</summary>
    public required string Status { get; init; }

    /// <summary>Random token supplied by the server (base64url, at least 128 bits of entropy).</summary>
    public string? Token { get; init; }

    /// <summary>When the challenge was validated.</summary>
    public DateTimeOffset? Validated { get; init; }

    /// <summary>Error describing why validation failed.</summary>
    public AcmeProblem? Error { get; init; }
}

/// <summary>Challenge status values.</summary>
public static class AcmeChallengeStatus
{
    /// <summary>Not yet responded to.</summary>
    public const string Pending = "pending";

    /// <summary>The server is validating the challenge.</summary>
    public const string Processing = "processing";

    /// <summary>Validated successfully.</summary>
    public const string Valid = "valid";

    /// <summary>Validation failed.</summary>
    public const string Invalid = "invalid";
}

/// <summary>Well-known challenge type identifiers.</summary>
public static class AcmeChallengeTypes
{
    /// <summary>HTTP challenge (RFC 8555 §8.3).</summary>
    public const string Http01 = "http-01";

    /// <summary>DNS challenge (RFC 8555 §8.4).</summary>
    public const string Dns01 = "dns-01";

    /// <summary>TLS-ALPN challenge (RFC 8737).</summary>
    public const string TlsAlpn01 = "tls-alpn-01";
}
