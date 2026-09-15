using System.Text.Json.Serialization;

namespace Tedd.Quicly.Acme.Models;

/// <summary>ACME order object (RFC 8555 §7.1.3).</summary>
public sealed record AcmeOrder
{
    /// <summary>Order status: <c>pending</c>, <c>ready</c>, <c>processing</c>, <c>valid</c> or <c>invalid</c>.</summary>
    public required string Status { get; init; }

    /// <summary>When the order expires (RFC 3339).</summary>
    public DateTimeOffset? Expires { get; init; }

    /// <summary>Identifiers this order covers.</summary>
    public required IReadOnlyList<AcmeIdentifier> Identifiers { get; init; }

    /// <summary>Requested <c>notBefore</c> for the certificate.</summary>
    public DateTimeOffset? NotBefore { get; init; }

    /// <summary>Requested <c>notAfter</c> for the certificate.</summary>
    public DateTimeOffset? NotAfter { get; init; }

    /// <summary>Error that caused the order to become <c>invalid</c>, if any.</summary>
    public AcmeProblem? Error { get; init; }

    /// <summary>URLs of the authorizations that must be completed before finalization.</summary>
    public required IReadOnlyList<Uri> Authorizations { get; init; }

    /// <summary>URL to POST the CSR to once the order is <c>ready</c>.</summary>
    public required Uri Finalize { get; init; }

    /// <summary>URL of the issued certificate once the order is <c>valid</c>.</summary>
    public Uri? Certificate { get; init; }

    /// <summary>Order URL, taken from the <c>Location</c> response header (or the URL it was fetched from).</summary>
    [JsonIgnore]
    public Uri? Location { get; init; }
}

/// <summary>Order status values.</summary>
public static class AcmeOrderStatus
{
    /// <summary>Waiting for authorizations to be completed.</summary>
    public const string Pending = "pending";

    /// <summary>All authorizations are valid; the order can be finalized.</summary>
    public const string Ready = "ready";

    /// <summary>The CA is issuing the certificate.</summary>
    public const string Processing = "processing";

    /// <summary>The certificate has been issued.</summary>
    public const string Valid = "valid";

    /// <summary>The order failed or expired.</summary>
    public const string Invalid = "invalid";
}
