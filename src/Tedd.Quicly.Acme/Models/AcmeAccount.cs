using System.Text.Json.Serialization;

namespace Tedd.Quicly.Acme.Models;

/// <summary>ACME account object (RFC 8555 §7.1.2).</summary>
public sealed record AcmeAccount
{
    /// <summary>Account status: <c>valid</c>, <c>deactivated</c> or <c>revoked</c>.</summary>
    public required string Status { get; init; }

    /// <summary>Contact URLs (e.g. <c>mailto:admin@example.com</c>).</summary>
    public IReadOnlyList<string>? Contact { get; init; }

    /// <summary>Whether the account holder agreed to the terms of service.</summary>
    public bool? TermsOfServiceAgreed { get; init; }

    /// <summary>URL of the account's orders list.</summary>
    public Uri? Orders { get; init; }

    /// <summary>Account URL (the JWS <c>kid</c>), taken from the <c>Location</c> response header.</summary>
    [JsonIgnore]
    public Uri? Location { get; init; }
}

/// <summary>Account status values.</summary>
public static class AcmeAccountStatus
{
    /// <summary>The account is usable.</summary>
    public const string Valid = "valid";

    /// <summary>The account was deactivated by the client.</summary>
    public const string Deactivated = "deactivated";

    /// <summary>The account was revoked by the server.</summary>
    public const string Revoked = "revoked";
}
