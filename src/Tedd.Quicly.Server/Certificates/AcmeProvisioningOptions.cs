using System.Buffers;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
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
/// HMAC key, <see cref="CertificatePassword"/>) are never logged or included in status messages, <see cref="ToString"/>
/// or the debugger view.
/// </remarks>
[DebuggerDisplay("{ToString(),nq}")]
public sealed class AcmeProvisioningOptions
{
    /// <summary>Default path of the health endpoint.</summary>
    public const string DefaultHealthPath = "/healthz";

    private const int MinRsaKeySizeBits = 2048;
    private const int MaxRsaKeySizeBits = 8192;
    private const int MaxDnsNameLength = 253;
    private const int MaxLabelLength = 63;
    private const string Redacted = "***";
    private static readonly TimeSpan MaxTimeout = TimeSpan.FromDays(30);

    /// <summary>Longest accepted <see cref="RenewalSchedulerOptions.RenewBefore"/>: the longest lifetime a publicly trusted certificate may have.</summary>
    private static readonly TimeSpan MaxRenewBefore = TimeSpan.FromDays(398);

    private static readonly SearchValues<char> LdhCharacters = SearchValues.Create("abcdefghijklmnopqrstuvwxyz0123456789-");

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
    /// only to sign the account creation request, is not persisted, and is redacted from <see cref="ToString"/> and the
    /// debugger view.
    /// </summary>
    [DebuggerBrowsable(DebuggerBrowsableState.Never)]
    public string? ExternalAccountHmacKey { get; set; }

    /// <summary>
    /// DNS names to certify: host names of letters, digits and hyphens (internationalised names are converted to their
    /// ASCII form). A leading <c>*.</c> requests a wildcard, which needs <see cref="AcmeChallengeKind.Dns01"/>. Invalid
    /// names are refused up front.
    /// </summary>
    public IList<string> DnsNames { get; } = new List<string>();

    /// <summary>IP addresses to certify (RFC 8738). Only <c>http-01</c> and <c>tls-alpn-01</c> can validate them, and few public CAs issue them.</summary>
    public IList<IPAddress> IpAddresses { get; } = new List<IPAddress>();

    /// <summary>
    /// Challenge types in order of preference; for each name the first one the CA offers is used. Only the listed types
    /// are served: <see cref="AcmeChallengeKind.Http01"/> starts the HTTP endpoint and <see cref="AcmeChallengeKind.TlsAlpn01"/>
    /// the TLS endpoint. Default: http-01 only, so the default exposure is the ACME responder plus the redirect to HTTPS on
    /// TCP 80 (ADR 0009). Add <see cref="AcmeChallengeKind.TlsAlpn01"/> to answer on TCP 443 as well, or use
    /// <see cref="AcmeChallengeKind.Dns01"/> for wildcards and servers without inbound TCP.
    /// </summary>
    public IList<AcmeChallengeKind> ChallengeTypes { get; } = new List<AcmeChallengeKind> { AcmeChallengeKind.Http01 };

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

    /// <summary>
    /// Password protecting <see cref="CertificatePath"/>, which holds the private key. Recommended: the file is written
    /// owner-only on Unix, but on Windows it inherits the directory's ACL, and backups copy it. Secret: redacted from
    /// <see cref="ToString"/> and the debugger view.
    /// </summary>
    [DebuggerBrowsable(DebuggerBrowsableState.Never)]
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
    /// Where the plain-HTTP challenge endpoint listens when <c>http-01</c> is allowed. Default <c>[::]:80</c>, a dual-mode
    /// socket that accepts IPv4 and IPv6 (a CA validates over IPv6 when the name has an AAAA record); on a host without
    /// IPv6 it falls back to <c>0.0.0.0:80</c>. The CA always connects to port 80 of the name's address, so a different
    /// port only makes sense behind a port forward.
    /// </summary>
    public IPEndPoint HttpChallengeEndpoint { get; set; } = new(IPAddress.IPv6Any, 80);

    /// <summary>
    /// Where the TLS endpoint listens when <c>tls-alpn-01</c> is allowed. Default <c>[::]:443</c>, dual-mode like
    /// <see cref="HttpChallengeEndpoint"/> (TCP; a QUIC listener on UDP 443 does not conflict). Normal TLS clients get the
    /// current certificate there; before the first certificate exists their handshakes are refused.
    /// </summary>
    public IPEndPoint TlsAlpnEndpoint { get; set; } = new(IPAddress.IPv6Any, 443);

    /// <summary>Redirect every other plain-HTTP request on the challenge endpoint to HTTPS (ADR 0009 default). Default true.</summary>
    public bool RedirectToHttps { get; set; } = true;

    /// <summary>HTTPS port used in redirects (omitted from the URL when 443). Default 443.</summary>
    public int RedirectHttpsPort { get; set; } = 443;

    /// <summary>
    /// Answer <see cref="HealthPath"/> with <c>200 ok</c> on the HTTP challenge endpoint (opt-in, ADR 0009). Default false.
    /// The TLS endpoint serves only <see cref="TlsEndpointHandlers"/>; add a <see cref="Http.Handlers.HealthHandler"/> there
    /// to answer health checks over TLS.
    /// </summary>
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
            string ascii = NormalizeDnsName(name);
            if (seen.Add("dns:" + ascii))
            {
                identifiers.Add(AcmeIdentifier.Dns(ascii));
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

        ValidateDistinctPaths(AccountStorePath, CertificatePath);

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

        foreach (string? name in DnsNames)
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

        if (ChallengePropagationDelay < TimeSpan.Zero || ChallengePropagationDelay > MaxTimeout)
        {
            throw Invalid("ChallengePropagationDelay must be between zero and 30 days.");
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

        if (!Enum.IsDefined(KeyAlgorithm))
        {
            throw Invalid("KeyAlgorithm " + ((int)KeyAlgorithm).ToString(CultureInfo.InvariantCulture) + " is not supported.");
        }

        if (KeyAlgorithm == AcmeKeyAlgorithm.RS256 && RsaKeySizeBits is < MinRsaKeySizeBits or > MaxRsaKeySizeBits)
        {
            throw Invalid("RsaKeySizeBits must be between 2048 and 8192.");
        }

        if (ExternalAccountKeyId is not null)
        {
            ValidateExternalAccountBinding(ExternalAccountKeyId, ExternalAccountHmacKey!);
        }

        if (ChallengeCleanupTimeout <= TimeSpan.Zero || ChallengeCleanupTimeout > MaxTimeout)
        {
            throw Invalid("ChallengeCleanupTimeout must be positive and at most 30 days.");
        }

        try
        {
            Retry.Validate();
        }
        catch (ArgumentException e)
        {
            throw Invalid("Retry is out of range: " + e.Message);
        }

        ValidateRenewal(Renewal);

        foreach (IHttpHandler? handler in TlsEndpointHandlers)
        {
            if (handler is null)
            {
                throw Invalid("TlsEndpointHandlers contains null.");
            }
        }

        if (kinds.Contains(AcmeChallengeKind.Http01) && kinds.Contains(AcmeChallengeKind.TlsAlpn01) && SharePort(HttpChallengeEndpoint, TlsAlpnEndpoint))
        {
            throw Invalid("HttpChallengeEndpoint and TlsAlpnEndpoint both use TCP port " + HttpChallengeEndpoint.Port.ToString(CultureInfo.InvariantCulture) + "; the HTTP and TLS challenge servers need separate ports.");
        }
    }

    private static void ValidateDnsName(string? name, bool dns01)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw Invalid("DnsNames contains an empty entry.");
        }

        if (IPAddress.TryParse(name, out IPAddress? address) && address.AddressFamily is AddressFamily.InterNetwork or AddressFamily.InterNetworkV6)
        {
            throw Invalid("'" + name + "' is an IP address: add it to IpAddresses instead of DnsNames.");
        }

        if (NormalizeDnsName(name).StartsWith("*.", StringComparison.Ordinal) && !dns01)
        {
            throw Invalid("The wildcard name '" + name + "' can only be validated with Dns01, which is not in ChallengeTypes.");
        }
    }

    /// <summary>
    /// The ASCII form that ACME and certificates use for a DNS name: internationalised labels converted to A-labels
    /// (<see cref="IdnMapping"/>), in lower case. Throws unless the result is a host name: labels of 1 to 63 letters,
    /// digits and hyphens that neither start nor end with a hyphen, at most 253 characters, and an optional leading
    /// <c>*.</c> (a wildcard) as the only asterisk. Invalid names thus fail at start-up rather than in every order.
    /// </summary>
    /// <exception cref="ArgumentException">The name is not a valid DNS name.</exception>
    internal static string NormalizeDnsName(string name)
    {
        bool wildcard = name.StartsWith("*.", StringComparison.Ordinal);
        string host = wildcard ? name[2..] : name;
        string ascii;
        try
        {
            ascii = new IdnMapping().GetAscii(host).ToLowerInvariant();
        }
        catch (ArgumentException)
        {
            throw InvalidName(name, "it is not a valid (internationalised) domain name");
        }

        if (ascii.Length + (wildcard ? 2 : 0) > MaxDnsNameLength)
        {
            throw InvalidName(name, "it is longer than 253 characters");
        }

        foreach (string label in ascii.Split('.'))
        {
            if (label.Length is 0 or > MaxLabelLength || label[0] == '-' || label[^1] == '-' || label.AsSpan().ContainsAnyExcept(LdhCharacters))
            {
                throw InvalidName(name, "each label needs 1 to 63 letters, digits or hyphens and cannot start or end with a hyphen (a trailing dot is not accepted either)");
            }
        }

        return wildcard ? "*." + ascii : ascii;
    }

    private static ArgumentException InvalidName(string name, string reason) => Invalid("'" + name + "' is not a valid DNS name: " + reason + ".");

    /// <summary>The account store and the certificate (and its metadata file) must be different files, or each write would destroy the other.</summary>
    private static void ValidateDistinctPaths(string accountStorePath, string certificatePath)
    {
        string account;
        string certificate;
        try
        {
            account = Path.GetFullPath(accountStorePath);
            certificate = Path.GetFullPath(certificatePath);
        }
        catch (Exception e) when (e is ArgumentException or NotSupportedException or PathTooLongException)
        {
            throw Invalid("AccountStorePath or CertificatePath is not a valid path: " + e.Message);
        }

        StringComparison comparison = OperatingSystem.IsWindows() || OperatingSystem.IsMacOS() ? StringComparison.OrdinalIgnoreCase : StringComparison.Ordinal;
        if (string.Equals(account, certificate, comparison) || string.Equals(account, CertificateMetadata.PathFor(certificate), comparison))
        {
            throw Invalid("AccountStorePath and CertificatePath must name different files: the account key and the certificate would overwrite each other.");
        }
    }

    /// <summary>Checks the EAB credentials without ever quoting the HMAC key (a secret) in the message.</summary>
    private static void ValidateExternalAccountBinding(string keyId, string hmacKey)
    {
        if (string.IsNullOrWhiteSpace(keyId))
        {
            throw Invalid("ExternalAccountKeyId must not be empty.");
        }

        byte[] key;
        try
        {
            key = new ExternalAccountBinding(keyId, hmacKey).DecodeHmacKey();
        }
        catch (Exception e) when (e is FormatException or ArgumentException)
        {
            throw Invalid("ExternalAccountHmacKey is not valid base64url; copy it exactly as the CA shows it.");
        }

        bool empty = key.Length == 0;
        CryptographicOperations.ZeroMemory(key);
        if (empty)
        {
            throw Invalid("ExternalAccountHmacKey is empty.");
        }
    }

    /// <summary>The ranges <see cref="RenewalScheduler"/> enforces, checked before anything is bound; a zero retry delay is refused too.</summary>
    private static void ValidateRenewal(RenewalSchedulerOptions renewal)
    {
        if (renewal.RenewBefore < TimeSpan.Zero
            || renewal.MinimumRenewalInterval < TimeSpan.Zero
            || renewal.StartupDelay < TimeSpan.Zero
            || renewal.ImmediateRenewalThreshold < TimeSpan.Zero)
        {
            throw Invalid("Renewal.RenewBefore, MinimumRenewalInterval, StartupDelay and ImmediateRenewalThreshold must not be negative.");
        }

        if (renewal.CheckInterval <= TimeSpan.Zero || renewal.RetryDelay <= TimeSpan.Zero || renewal.RenewalInfoRefreshInterval <= TimeSpan.Zero)
        {
            throw Invalid("Renewal.CheckInterval, RetryDelay and RenewalInfoRefreshInterval must be positive (a zero RetryDelay would order again back to back after every failure).");
        }

        // Upper bounds keep every clock computation representable (now + RetryDelay, notAfter - ImmediateRenewalThreshold,
        // ...) and every wait within what a timer accepts.
        if (renewal.CheckInterval > MaxTimeout
            || renewal.RetryDelay > MaxTimeout
            || renewal.MinimumRenewalInterval > MaxTimeout
            || renewal.StartupDelay > MaxTimeout
            || renewal.ImmediateRenewalThreshold > MaxTimeout
            || renewal.RenewalInfoRefreshInterval > MaxTimeout)
        {
            throw Invalid("Renewal.CheckInterval, RetryDelay, MinimumRenewalInterval, StartupDelay, ImmediateRenewalThreshold and RenewalInfoRefreshInterval must be at most 30 days.");
        }

        if (renewal.RenewBefore > MaxRenewBefore)
        {
            throw Invalid("Renewal.RenewBefore must be at most 398 days (the longest lifetime of a publicly trusted certificate).");
        }

        if (renewal.TimeProvider is null || renewal.Random is null)
        {
            throw Invalid("Renewal.TimeProvider and Renewal.Random must not be null.");
        }
    }

    /// <summary>True when two listen endpoints would claim the same TCP port (same address, or either is a wildcard address).</summary>
    private static bool SharePort(IPEndPoint a, IPEndPoint b)
    {
        return a.Port != 0 && a.Port == b.Port && (a.Address.Equals(b.Address) || IsWildcard(a.Address) || IsWildcard(b.Address));

        static bool IsWildcard(IPAddress address) => address.Equals(IPAddress.Any) || address.Equals(IPAddress.IPv6Any);
    }

    private static ArgumentException Invalid(string message) => new("Invalid ACME provisioning options: " + message, "options");

    /// <summary>A summary for logs: directory, names, challenge types and paths. The EAB HMAC key and the certificate password are redacted.</summary>
    public override string ToString()
    {
        return "AcmeProvisioningOptions { DirectoryUrl = " + DirectoryUrl
            + ", DnsNames = [" + string.Join(", ", DnsNames)
            + "], IpAddresses = [" + string.Join(", ", IpAddresses)
            + "], ChallengeTypes = [" + string.Join(", ", ChallengeTypes)
            + "], AccountStorePath = " + AccountStorePath
            + ", CertificatePath = " + CertificatePath
            + ", CertificatePassword = " + (CertificatePassword is null ? "(none)" : Redacted)
            + ", ExternalAccountKeyId = " + (ExternalAccountKeyId ?? "(none)")
            + ", ExternalAccountHmacKey = " + (ExternalAccountHmacKey is null ? "(none)" : Redacted)
            + " }";
    }
}
