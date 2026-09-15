using System.Text.Json.Serialization;

namespace Tedd.Quicly.Acme.Models;

/// <summary><c>newAccount</c> request payload (RFC 8555 §7.3).</summary>
internal sealed record NewAccountRequest
{
    public IReadOnlyList<string>? Contact { get; init; }

    public bool? TermsOfServiceAgreed { get; init; }

    public bool? OnlyReturnExisting { get; init; }

    public JwsEnvelope? ExternalAccountBinding { get; init; }
}

/// <summary>Account update payload (RFC 8555 §7.3.2 / §7.3.6).</summary>
internal sealed record AccountUpdateRequest
{
    public string? Status { get; init; }

    public IReadOnlyList<string>? Contact { get; init; }
}

/// <summary><c>newOrder</c> request payload (RFC 8555 §7.4).</summary>
internal sealed record NewOrderRequest
{
    public required IReadOnlyList<AcmeIdentifier> Identifiers { get; init; }

    public DateTimeOffset? NotBefore { get; init; }

    public DateTimeOffset? NotAfter { get; init; }

    /// <summary>ARI certificate identifier of the certificate this order replaces (RFC 9773 §5).</summary>
    public string? Replaces { get; init; }
}

/// <summary>Order finalization payload (RFC 8555 §7.4).</summary>
internal sealed record FinalizeRequest
{
    /// <summary>Base64url DER CSR.</summary>
    public required string Csr { get; init; }
}

/// <summary>Certificate revocation payload (RFC 8555 §7.6).</summary>
internal sealed record RevokeRequest
{
    /// <summary>Base64url DER certificate.</summary>
    public required string Certificate { get; init; }

    public int? Reason { get; init; }
}

/// <summary>Empty JSON object payload (<c>{}</c>) used to respond to a challenge.</summary>
internal sealed record EmptyRequest
{
}

/// <summary>Persisted account state (see <see cref="AcmeAccountStore"/>).</summary>
public sealed record AcmeAccountState
{
    /// <summary>The directory the account was created at.</summary>
    public required Uri DirectoryUrl { get; init; }

    /// <summary>The account URL (JWS <c>kid</c>).</summary>
    public required Uri AccountUrl { get; init; }

    /// <summary>Account key algorithm.</summary>
    [JsonConverter(typeof(JsonStringEnumConverter<AcmeKeyAlgorithm>))]
    public required AcmeKeyAlgorithm Algorithm { get; init; }

    /// <summary>PKCS#8 PEM of the account private key.</summary>
    public required string PrivateKeyPem { get; init; }

    /// <summary>An order that was started but not completed (so a crashed run can resume it), or <see langword="null"/>.</summary>
    public AcmePendingOrder? PendingOrder { get; init; }
}

/// <summary>Persisted state of an in-flight order (see <see cref="AcmeCertificateManager"/>).</summary>
public sealed record AcmePendingOrder
{
    /// <summary>The order URL.</summary>
    public required Uri OrderUrl { get; init; }

    /// <summary>The identifiers the order covers.</summary>
    public required IReadOnlyList<AcmeIdentifier> Identifiers { get; init; }

    /// <summary>PKCS#8 PEM of the certificate private key the CSR was (or will be) created with.</summary>
    public required string CertificateKeyPem { get; init; }

    /// <summary>When the order expires at the CA, if known.</summary>
    public DateTimeOffset? Expires { get; init; }

    /// <summary>When the order was created (client clock).</summary>
    public DateTimeOffset CreatedAt { get; init; }
}

/// <summary>On-disk envelope used by <see cref="AcmeAccountStore"/> when the document is protected with DPAPI.</summary>
internal sealed record AcmeStoreEnvelope
{
    /// <summary>Base64 of the DPAPI-protected UTF-8 JSON <see cref="AcmeAccountState"/>; absent for plain-text files.</summary>
    public string? Dpapi { get; init; }
}
