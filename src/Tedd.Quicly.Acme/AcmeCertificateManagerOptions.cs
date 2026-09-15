using Tedd.Quicly.Acme.Challenges;
using Tedd.Quicly.Acme.Models;

namespace Tedd.Quicly.Acme;

/// <summary>Configuration for <see cref="AcmeCertificateManager"/>.</summary>
public sealed class AcmeCertificateManagerOptions
{
    /// <summary>The CA directory URL (see <see cref="AcmeDirectories"/>).</summary>
    public required Uri DirectoryUrl { get; init; }

    /// <summary>Where the account key and URL are persisted between runs.</summary>
    public required AcmeAccountStore AccountStore { get; init; }

    /// <summary>The identifiers to certify (all go into one order / one certificate).</summary>
    public required IReadOnlyList<AcmeIdentifier> Identifiers { get; init; }

    /// <summary>Account contact URLs (e.g. <c>mailto:admin@example.com</c>). Some CAs (Buypass) require one.</summary>
    public IReadOnlyList<string> Contacts { get; init; } = [];

    /// <summary>Whether the CA's terms of service are accepted when creating the account. Default true.</summary>
    public bool AgreeTermsOfService { get; init; } = true;

    /// <summary>External Account Binding credentials for CAs that require them (ZeroSSL, Google Trust Services, SSL.com).</summary>
    public ExternalAccountBinding? ExternalAccountBinding { get; init; }

    /// <summary>Algorithm for a newly created account key. Default ES256.</summary>
    public AcmeKeyAlgorithm AccountKeyAlgorithm { get; init; } = AcmeKeyAlgorithm.ES256;

    /// <summary>Algorithm for the certificate key (a fresh key is generated for every order unless <see cref="ReuseKey"/>). Default ES256.</summary>
    public AcmeKeyAlgorithm CertificateKeyAlgorithm { get; init; } = AcmeKeyAlgorithm.ES256;

    /// <summary>RSA key size when <see cref="CertificateKeyAlgorithm"/> is RS256. Default 2048.</summary>
    public int RsaKeySizeBits { get; init; } = 2048;

    /// <summary>
    /// When true the private key of the certificate persisted at <see cref="CertificatePath"/> is reused for every renewal
    /// (required when clients pin the SPKI, ADR 0009). A fresh key is generated only when no certificate is persisted yet.
    /// Requires <see cref="CertificatePath"/>. Default false: every order gets a new key.
    /// </summary>
    public bool ReuseKey { get; init; }

    /// <summary>Retry policy for transient failures of <see cref="AcmeCertificateManager.OrderCertificateAsync(CancellationToken)"/>.</summary>
    public AcmeRetryOptions Retry { get; init; } = new();

    /// <summary>
    /// Challenge types in order of preference; the first one offered by the authorization for which a responder is
    /// configured is used. Default: http-01, tls-alpn-01, dns-01.
    /// </summary>
    public IReadOnlyList<string> PreferredChallengeTypes { get; init; } = [AcmeChallengeTypes.Http01, AcmeChallengeTypes.TlsAlpn01, AcmeChallengeTypes.Dns01];

    /// <summary>Responder for <c>http-01</c>; <see langword="null"/> disables the challenge type.</summary>
    public IHttp01Responder? Http01Responder { get; init; }

    /// <summary>Provider for <c>dns-01</c>; <see langword="null"/> disables the challenge type.</summary>
    public IDns01Provider? Dns01Provider { get; init; }

    /// <summary>Responder for <c>tls-alpn-01</c>; <see langword="null"/> disables the challenge type.</summary>
    public ITlsAlpn01Responder? TlsAlpn01Responder { get; init; }

    /// <summary>Delay between publishing a challenge and asking the CA to validate it (DNS propagation). Default zero.</summary>
    public TimeSpan ChallengePropagationDelay { get; init; } = TimeSpan.Zero;

    /// <summary>When set, every issued certificate is written here as PKCS#12 and can be reloaded on restart.</summary>
    public string? CertificatePath { get; init; }

    /// <summary>Optional password for <see cref="CertificatePath"/>.</summary>
    public string? CertificatePassword { get; init; }

    /// <summary>
    /// When set, alternate chains (<c>Link: rel="alternate"</c>) are inspected and the first chain whose top-most
    /// certificate's issuer name contains this text (ordinal, case-insensitive) is used; otherwise the default chain.
    /// </summary>
    public string? PreferredChainIssuer { get; init; }

    /// <summary>Requested <c>notBefore</c> for the order (most public CAs ignore or reject this; leave null).</summary>
    public DateTimeOffset? NotBefore { get; init; }

    /// <summary>Requested <c>notAfter</c> for the order (most public CAs ignore or reject this; leave null).</summary>
    public DateTimeOffset? NotAfter { get; init; }

    /// <summary>Options for the underlying <see cref="AcmeClient"/>.</summary>
    public AcmeClientOptions ClientOptions { get; init; } = new();
}

/// <summary>Stages reported through <see cref="AcmeCertificateManager.Progress"/>.</summary>
public enum AcmeStage
{
    /// <summary>The directory was fetched.</summary>
    DirectoryFetched,

    /// <summary>An account was created or reused.</summary>
    AccountReady,

    /// <summary>The order was created.</summary>
    OrderCreated,

    /// <summary>An order persisted by an earlier (crashed) run was found still usable and is resumed instead of creating a new one.</summary>
    OrderResumed,

    /// <summary>An authorization was already valid and skipped.</summary>
    AuthorizationSkipped,

    /// <summary>A challenge was selected for an authorization.</summary>
    ChallengeSelected,

    /// <summary>The challenge response was published via the responder.</summary>
    ChallengePublished,

    /// <summary>The CA was asked to validate the challenge.</summary>
    ChallengeResponded,

    /// <summary>The authorization became valid.</summary>
    AuthorizationValid,

    /// <summary>The challenge response was removed.</summary>
    ChallengeCleanedUp,

    /// <summary>The order is ready for finalization.</summary>
    OrderReady,

    /// <summary>The CSR was submitted.</summary>
    OrderFinalized,

    /// <summary>The certificate chain was downloaded.</summary>
    CertificateDownloaded,

    /// <summary>The certificate was written to <see cref="AcmeCertificateManagerOptions.CertificatePath"/>.</summary>
    CertificatePersisted,

    /// <summary>A transient failure occurred; the message carries the error and the flow is retried after a back-off.</summary>
    Retrying,

    /// <summary>The flow failed; the message carries the error.</summary>
    Failed,
}

/// <summary>A progress notification from <see cref="AcmeCertificateManager"/>.</summary>
/// <param name="Stage">The stage reached.</param>
/// <param name="Message">Human-readable detail.</param>
/// <param name="Identifier">The identifier concerned, when the stage is per-authorization.</param>
public readonly record struct AcmeProgress(AcmeStage Stage, string Message, AcmeIdentifier? Identifier = null);
