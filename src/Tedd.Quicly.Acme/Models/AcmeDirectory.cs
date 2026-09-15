namespace Tedd.Quicly.Acme.Models;

/// <summary>ACME directory object (RFC 8555 §7.1.1).</summary>
public sealed record AcmeDirectory
{
    /// <summary>URL for fetching a fresh nonce (<c>newNonce</c>).</summary>
    public required Uri NewNonce { get; init; }

    /// <summary>URL for creating / looking up an account (<c>newAccount</c>).</summary>
    public required Uri NewAccount { get; init; }

    /// <summary>URL for creating a new order (<c>newOrder</c>).</summary>
    public required Uri NewOrder { get; init; }

    /// <summary>Optional pre-authorization URL (<c>newAuthz</c>).</summary>
    public Uri? NewAuthz { get; init; }

    /// <summary>URL for revoking a certificate (<c>revokeCert</c>).</summary>
    public Uri? RevokeCert { get; init; }

    /// <summary>URL for account key roll-over (<c>keyChange</c>).</summary>
    public Uri? KeyChange { get; init; }

    /// <summary>ARI endpoint (RFC 9773 §4.1, <c>renewalInfo</c>); <see langword="null"/> when the CA offers no renewal information.</summary>
    public Uri? RenewalInfo { get; init; }

    /// <summary>Directory metadata.</summary>
    public AcmeDirectoryMeta? Meta { get; init; }
}

/// <summary>ACME directory <c>meta</c> object (RFC 8555 §7.1.1).</summary>
public sealed record AcmeDirectoryMeta
{
    /// <summary>URL of the CA's terms of service; must be agreed to when creating an account.</summary>
    public Uri? TermsOfService { get; init; }

    /// <summary>CA website.</summary>
    public Uri? Website { get; init; }

    /// <summary>CAA identities recognised by this CA.</summary>
    public IReadOnlyList<string>? CaaIdentities { get; init; }

    /// <summary>When <see langword="true"/> the CA requires External Account Binding on <c>newAccount</c>.</summary>
    public bool? ExternalAccountRequired { get; init; }
}
