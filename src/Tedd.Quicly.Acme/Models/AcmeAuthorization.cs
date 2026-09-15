using System.Text.Json.Serialization;

namespace Tedd.Quicly.Acme.Models;

/// <summary>ACME authorization object (RFC 8555 §7.1.4).</summary>
public sealed record AcmeAuthorization
{
    /// <summary>The identifier this authorization is for.</summary>
    public required AcmeIdentifier Identifier { get; init; }

    /// <summary>Status: <c>pending</c>, <c>valid</c>, <c>invalid</c>, <c>deactivated</c>, <c>expired</c> or <c>revoked</c>.</summary>
    public required string Status { get; init; }

    /// <summary>When the authorization expires.</summary>
    public DateTimeOffset? Expires { get; init; }

    /// <summary>Challenges the client may complete; any one suffices.</summary>
    public required IReadOnlyList<AcmeChallenge> Challenges { get; init; }

    /// <summary>True when the identifier is a wildcard (the <c>*.</c> prefix is stripped from <see cref="Identifier"/>).</summary>
    public bool? Wildcard { get; init; }

    /// <summary>Authorization URL (the URL it was fetched from).</summary>
    [JsonIgnore]
    public Uri? Location { get; init; }
}

/// <summary>Authorization / challenge status values.</summary>
public static class AcmeAuthorizationStatus
{
    /// <summary>No challenge has been completed yet.</summary>
    public const string Pending = "pending";

    /// <summary>A challenge was validated.</summary>
    public const string Valid = "valid";

    /// <summary>A challenge failed.</summary>
    public const string Invalid = "invalid";

    /// <summary>Deactivated by the client.</summary>
    public const string Deactivated = "deactivated";

    /// <summary>Expired.</summary>
    public const string Expired = "expired";

    /// <summary>Revoked by the server.</summary>
    public const string Revoked = "revoked";
}
