using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Acme;
using Tedd.Quicly.Acme.Challenges;
using Tedd.Quicly.Acme.Models;
using Tedd.Quicly.Http;

namespace Tedd.Quicly.Server.Certificates;

/// <summary>An ACME challenge type the provisioner can answer.</summary>
public enum AcmeChallengeKind
{
    /// <summary><c>http-01</c>: a key authorization served over plain HTTP on TCP port 80 (<see cref="AcmeProvisioningOptions.HttpChallengeEndpoint"/>).</summary>
    Http01,

    /// <summary><c>tls-alpn-01</c>: a validation certificate presented over TLS on TCP port 443 for ALPN <c>acme-tls/1</c> (<see cref="AcmeProvisioningOptions.TlsAlpnEndpoint"/>).</summary>
    TlsAlpn01,

    /// <summary><c>dns-01</c>: a TXT record created through <see cref="AcmeProvisioningOptions.Dns01Provider"/>. The only type that can validate wildcard names.</summary>
    Dns01,
}

/// <summary>
/// Configuration for ACME certificate provisioning (<see cref="ServerCertificateOptions.Acme"/>). The minimum is a
/// directory URL, <see cref="AgreeToTermsOfService"/>, at least one name, and the two file paths:
/// <code>
/// var acme = new AcmeProvisioningOptions
/// {
///     DirectoryUrl = AcmeDirectories.LetsEncrypt,
///     AgreeToTermsOfService = true,
///     Contacts = { "mailto:ops@example.com" },
///     DnsNames = { "play.example.com" },
///     AccountStorePath = "/var/lib/mygame/acme-account.json",
///     CertificatePath = "/var/lib/mygame/server.pfx",
/// };
/// </code>
/// </summary>
/// <remarks>
/// Do not change the options after they have been handed to a <see cref="CertificateProvisioner"/>. Secrets (the EAB
/// HMAC key, <see cref="CertificatePassword"/>) are never logged or included in status messages.
/// </remarks>
public sealed class AcmeProvisioningOptions
{
    /// <summary>Default path of the health endpoint.</summary>
    public const string DefaultHealthPath = "/healthz";

    /// <summary>
    /// The CA's directory URL. Default: Let's Encrypt production (<see cref="AcmeDirectories.LetsEncrypt"/>). Use
    /// <see cref="AcmeDirectories.LetsEncryptStaging"/> while testing a deployment: staging has generous rate limits but
    /// issues certificates no client trusts.
    /// </summary>
    public Uri DirectoryUrl { get; set; } = AcmeDirectories.LetsEncrypt;

    /// <summary>
    /// Account contacts, for example <c>mailto:ops@example.com</c>. A bare address without a scheme gets <c>mailto:</c>
    /// prepended. Optional for most CAs; Buypass requires one.
    /// </summary>
    public IList<string> Contacts { get; } = new List<string>();

    /// <summary>
    /// Whether you agree to the CA's terms of service (the directory's <c>meta.termsOfService</c>). Must be
    /// <see langword="true"/>: every public ACME CA refuses to create an account otherwise. Default <see langword="false"/>
    /// so that agreeing is an explicit decision.
    /// </summary>
    public bool AgreeToTermsOfService { get; set; }

    /// <summary>
    /// External Account Binding key id, for CAs that require EAB (ZeroSSL, Google Trust Services, SSL.com; see
    /// <see cref="AcmeCaInfo.RequiresExternalAccountBinding"/>). Set together with <see cref="ExternalAccountHmacKey"/>.
    /// </summary>
    public string? ExternalAccountKeyId { get; set; }

    /// <summary>
    /// External Account Binding HMAC key as issued by the CA (base64url; standard base64 is accepted). Secret: it is used
    /// only to sign the account creation request and is not persisted.
    /// </summary>
    public string? ExternalAccountHmacKey { get; set; }

    /// <summary>DNS names to certify. A leading <c>*.</c> requests a wildcard, which needs <see cref="AcmeChallengeKind.Dns01"/>.</summary>
    public IList<string> DnsNames { get; } = new List<string>();

    /// <summary>IP addresses to certify (RFC 8738). Only <c>http-01</c> and <c>tls-alpn-01</c> can validate them, and few public CAs issue them.</summary>
    public IList<IPAddress> IpAddresses { get; } = new List<IPAddress>();

    /// <summary>
    /// Challenge types in order of preference; for each name the first one the CA offers is used. Only the listed types
    /// are served: <see cref="AcmeChallengeKind.Http01"/> starts the HTTP endpoint and <see cref="AcmeChallengeKind.TlsAlpn01"/>
    /// the TLS endpoint. Default: http-01, then tls-alpn-01.
    /// </summary>
    public IList<AcmeChallengeKind> ChallengeTypes { get; } = new List<AcmeChallengeKind> { AcmeChallengeKind.Http01, AcmeChallengeKind.TlsAlpn01 };

    /// <summary>Creates and removes <c>_acme-challenge</c> TXT records; required when <see cref="ChallengeTypes"/> contains <see cref="AcmeChallengeKind.Dns01"/>.</summary>
    public IDns01Provider? Dns01Provider { get; set; }

    /// <summary>Wait between publishing a challenge response and asking the CA to check it: DNS propagation for eventually consistent dns-01 providers. Default zero.</summary>
    public TimeSpan ChallengePropagationDelay { get; set; } = TimeSpan.Zero;

    /// <summary>Where the ACME account (key and URL) and any in-flight order are stored. Required.</summary>
    public string? AccountStorePath { get; set; }

    /// <summary>How the account store is protected; <see langword="null"/> (default) uses <see cref="AcmeAccountStore.DefaultProtection"/> (DPAPI on Windows, a 0600 file elsewhere).</summary>
    public AcmeStoreProtection? AccountStoreProtection { get; set; }

    /// <summary>
    /// Where the issued certificate and its private key are kept as PKCS#12. Required: on restart a persisted certificate
    /// that still covers the names and is not due for renewal is used without contacting the CA, so restarts do not
    /// spend the CA's rate limits.
    /// </summary>
    public string? CertificatePath { get; set; }

    /// <summary>Optional password protecting <see cref="CertificatePath"/>.</summary>
    public string? CertificatePassword { get; set; }

    /// <summary>
    /// When to renew: <see cref="RenewalSchedulerOptions.RenewBefore"/> (default: when one third of the lifetime remains),
    /// <see cref="RenewalSchedulerOptions.UseRenewalInfo"/> (ARI, default on), the retry delay after a failed renewal and
    /// the clock. The same rule decides whether a persisted certificate is due at start-up.
    /// </summary>
    public RenewalSchedulerOptions Renewal { get; set; } = new();

    /// <summary>Key algorithm of the certificate key. Default ES256 (ECDSA P-256). SSL.com's RSA directory needs RS256.</summary>
    public AcmeKeyAlgorithm KeyAlgorithm { get; set; } = AcmeKeyAlgorithm.ES256;

    /// <summary>RSA key size when <see cref="KeyAlgorithm"/> is RS256. Default 2048.</summary>
    public int RsaKeySizeBits { get; set; } = 2048;

    /// <summary>Reuse the persisted certificate's key for every renewal (required when clients pin the SPKI, ARCHITECTURE §8). Default false.</summary>
    public bool ReuseKey { get; set; }

    /// <summary>Back-off for transient CA failures within one order attempt (rate limits, 5xx, network errors).</summary>
    public AcmeRetryOptions Retry { get; set; } = new();

    /// <summary>Polling and transport tunables of the ACME client.</summary>
    public AcmeClientOptions ClientOptions { get; set; } = new();

    /// <summary>
    /// HTTP client used to talk to the CA; <see langword="null"/> (default) creates one that the provisioner owns. Inject
    /// one built on <see cref="AcmeClient.CreateHttpHandler"/> to trust a private CA's root (Pebble, in-house CAs). An
    /// injected client is not disposed.
    /// </summary>
    public HttpClient? HttpClient { get; set; }

    /// <summary>Prefer the alternate chain whose top certificate's issuer contains this text (see <see cref="AcmeCertificateManagerOptions.PreferredChainIssuer"/>).</summary>
    public string? PreferredChainIssuer { get; set; }

    /// <summary>Bound on removing challenge material after validation, even after cancellation. Default 30 s.</summary>
    public TimeSpan ChallengeCleanupTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Where the plain-HTTP challenge endpoint listens when <c>http-01</c> is allowed. Default <c>0.0.0.0:80</c>: the CA
    /// always connects to port 80 of the name's address, so a different port only makes sense behind a port forward.
    /// </summary>
    public IPEndPoint HttpChallengeEndpoint { get; set; } = new(IPAddress.Any, 80);

    /// <summary>
    /// Where the TLS endpoint listens when <c>tls-alpn-01</c> is allowed. Default <c>0.0.0.0:443</c> (TCP; a QUIC listener on
    /// UDP 443 does not conflict). Normal TLS clients get the current certificate there.
    /// </summary>
    public IPEndPoint TlsAlpnEndpoint { get; set; } = new(IPAddress.Any, 443);

    /// <summary>Redirect every other plain-HTTP request on the challenge endpoint to HTTPS (ADR 0009 default). Default true.</summary>
    public bool RedirectToHttps { get; set; } = true;

    /// <summary>HTTPS port used in redirects (omitted from the URL when 443). Default 443.</summary>
    public int RedirectHttpsPort { get; set; } = 443;

    /// <summary>Answer <see cref="HealthPath"/> with <c>200 ok</c> on the challenge endpoints (opt-in, ADR 0009). Default false.</summary>
    public bool EnableHealthEndpoint { get; set; }

    /// <summary>Path of the health endpoint. Default <c>/healthz</c>.</summary>
    public string HealthPath { get; set; } = DefaultHealthPath;

    /// <summary>Extra handlers for normal clients of the TLS endpoint (tried before the health endpoint; anything unhandled gets 404).</summary>
    public IList<IHttpHandler> TlsEndpointHandlers { get; } = new List<IHttpHandler>();

    /// <summary>
    /// Key storage for the certificates handed to consumers. Default <see cref="X509KeyStorageFlags.DefaultKeySet"/>:
    /// not exportable, not ephemeral (Schannel cannot sign with an ephemeral key), and on Windows the key container is
    /// removed when the certificate is disposed. Services on Windows that run MsQuic's certificate-context path under a
    /// machine account may need <see cref="X509KeyStorageFlags.MachineKeySet"/> (ADR 0009).
    /// <see cref="X509KeyStorageFlags.EphemeralKeySet"/> is refused on Windows.
    /// </summary>
    public X509KeyStorageFlags KeyStorageFlags { get; set; } = X509KeyStorageFlags.DefaultKeySet;

    /// <summary>Builds the ACME identifiers: DNS names (deduplicated, case-insensitively) then IP addresses.</summary>
    internal IReadOnlyList<AcmeIdentifier> BuildIdentifiers()
    {
        List<AcmeIdentifier> identifiers = [];
        HashSet<string> seen = new(StringComparer.OrdinalIgnoreCase);
        foreach (string name in DnsNames)
        {
            if (seen.Add("dns:" + name))
            {
                identifiers.Add(AcmeIdentifier.Dns(name));
            }
        }

        foreach (IPAddress address in IpAddresses)
        {
            if (seen.Add("ip:" + address))
            {
                identifiers.Add(AcmeIdentifier.Ip(address.ToString()));
            }
        }

        return identifiers;
    }

    /// <summary>The distinct challenge types in preference order, as ACME type strings.</summary>
    internal IReadOnlyList<string> BuildChallengeTypes()
    {
        List<string> types = [];
        foreach (AcmeChallengeKind kind in ChallengeTypes)
        {
            string type = kind switch
            {
                AcmeChallengeKind.Http01 => AcmeChallengeTypes.Http01,
                AcmeChallengeKind.TlsAlpn01 => AcmeChallengeTypes.TlsAlpn01,
                _ => AcmeChallengeTypes.Dns01,
            };
            if (!types.Contains(type))
            {
                types.Add(type);
            }
        }

        return types;
    }

    /// <summary>The contacts with <c>mailto:</c> prepended to bare addresses.</summary>
    internal IReadOnlyList<string> BuildContacts()
    {
        List<string> contacts = [];
        foreach (string contact in Contacts)
        {
            contacts.Add(contact.Contains(':', StringComparison.Ordinal) ? contact : "mailto:" + contact);
        }

        return contacts;
    }

    /// <summary>Checks the options and throws a descriptive <see cref="ArgumentException"/> for the first problem found.</summary>
    internal void Validate()
    {
        if (DirectoryUrl is null || !DirectoryUrl.IsAbsoluteUri)
        {
            throw Invalid("DirectoryUrl must be an absolute URL (see AcmeDirectories).");
        }

        if (!AgreeToTermsOfService)
        {
            throw Invalid("AgreeToTermsOfService must be true: ACME CAs only create accounts for clients that agree to their terms of service (read them at the directory's meta.termsOfService).");
        }

        if (string.IsNullOrWhiteSpace(AccountStorePath))
        {
            throw Invalid("AccountStorePath is required (the ACME account key is stored there).");
        }

        if (string.IsNullOrWhiteSpace(CertificatePath))
        {
            throw Invalid("CertificatePath is required (the issued certificate is persisted there and reused on restart).");
        }

        if ((ExternalAccountKeyId is null) != (ExternalAccountHmacKey is null))
        {
            throw Invalid("ExternalAccountKeyId and ExternalAccountHmacKey must be set together.");
        }

        List<AcmeChallengeKind> kinds = [.. ChallengeTypes];
        if (kinds.Count == 0)
        {
            throw Invalid("ChallengeTypes must contain at least one challenge type.");
        }

        foreach (AcmeChallengeKind kind in kinds)
        {
            if (!Enum.IsDefined(kind))
            {
                throw Invalid("ChallengeTypes contains an unknown value " + ((int)kind).ToString(CultureInfo.InvariantCulture) + ".");
            }
        }

        bool dns01 = kinds.Contains(AcmeChallengeKind.Dns01);
        if (dns01 && Dns01Provider is null)
        {
            throw Invalid("ChallengeTypes contains Dns01 but no Dns01Provider is set.");
        }

        if (DnsNames.Count == 0 && IpAddresses.Count == 0)
        {
            throw Invalid("At least one DNS name or IP address is required.");
        }

        foreach (string name in DnsNames)
        {
            ValidateDnsName(name, dns01);
        }

        foreach (IPAddress? address in IpAddresses)
        {
            if (address is null)
            {
                throw Invalid("IpAddresses contains null.");
            }
        }

        if (IpAddresses.Count > 0 && !kinds.Contains(AcmeChallengeKind.Http01) && !kinds.Contains(AcmeChallengeKind.TlsAlpn01))
        {
            throw Invalid("IP addresses can only be validated with Http01 or TlsAlpn01 (RFC 8738), and neither is in ChallengeTypes.");
        }

        if (kinds.Contains(AcmeChallengeKind.Http01) && HttpChallengeEndpoint is null)
        {
            throw Invalid("HttpChallengeEndpoint is required for Http01.");
        }

        if (kinds.Contains(AcmeChallengeKind.TlsAlpn01) && TlsAlpnEndpoint is null)
        {
            throw Invalid("TlsAlpnEndpoint is required for TlsAlpn01.");
        }

        if (RedirectHttpsPort is < 1 or > 65535)
        {
            throw Invalid("RedirectHttpsPort must be between 1 and 65535.");
        }

        if (EnableHealthEndpoint && (string.IsNullOrEmpty(HealthPath) || HealthPath[0] != '/'))
        {
            throw Invalid("HealthPath must start with '/'.");
        }

        if (ChallengePropagationDelay < TimeSpan.Zero)
        {
            throw Invalid("ChallengePropagationDelay must not be negative.");
        }

        if (Renewal is null || Retry is null || ClientOptions is null)
        {
            throw Invalid("Renewal, Retry and ClientOptions must not be null.");
        }

        if (OperatingSystem.IsWindows() && (KeyStorageFlags & X509KeyStorageFlags.EphemeralKeySet) != 0)
        {
            throw Invalid("KeyStorageFlags must not contain EphemeralKeySet on Windows: Schannel cannot sign with an ephemeral key (ADR 0009).");
        }

        foreach (string contact in Contacts)
        {
            if (string.IsNullOrWhiteSpace(contact))
            {
                throw Invalid("Contacts contains an empty entry.");
            }
        }
    }

    private static void ValidateDnsName(string name, bool dns01)
    {
        if (string.IsNullOrWhiteSpace(name) || name.Length > 253 || name.AsSpan().ContainsAny(" \t\r\n/:"))
        {
            throw Invalid("'" + name + "' is not a valid DNS name.");
        }

        if (IPAddress.TryParse(name, out IPAddress? address) && address.AddressFamily is AddressFamily.InterNetwork or AddressFamily.InterNetworkV6)
        {
            throw Invalid("'" + name + "' is an IP address: add it to IpAddresses instead of DnsNames.");
        }

        if (name.StartsWith("*.", StringComparison.Ordinal) && !dns01)
        {
            throw Invalid("The wildcard name '" + name + "' can only be validated with Dns01, which is not in ChallengeTypes.");
        }
    }

    private static ArgumentException Invalid(string message) => new("Invalid ACME provisioning options: " + message, "options");
}
