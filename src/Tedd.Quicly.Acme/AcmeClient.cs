using System.Collections.Concurrent;
using System.Net;
using System.Net.Http.Headers;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Tedd.Quicly.Acme.Models;

namespace Tedd.Quicly.Acme;

/// <summary>
/// RFC 8555 ACME v2 client for any directory URL. Handles directory caching, the nonce pool (harvesting the
/// <c>Replay-Nonce</c> of every response, <c>HEAD newNonce</c> on demand), a single automatic retry on
/// <c>badNonce</c> (with the nonce that response carried), <c>Retry-After</c>-aware polling and structured problem
/// documents (<see cref="AcmeException"/>).
/// </summary>
/// <remarks>
/// Only <c>https</c> URLs are requested (RFC 8555 §6.1), except loopback hosts or with
/// <see cref="AcmeClientOptions.AllowInsecureHttp"/>. Certificate validation is done by the injected
/// <see cref="HttpClient"/>; use <see cref="CreateHttpHandler"/> to trust a private or mock CA root without turning
/// validation off (ADR 0009).
/// </remarks>
public sealed class AcmeClient
{
    private const string JoseContentType = "application/jose+json";
    private const string ProblemContentType = "application/problem+json";
    private const string PemChainContentType = "application/pem-certificate-chain";

    /// <summary>Upper bound of the nonce pool; nonces beyond it are dropped (each request needs only one).</summary>
    internal const int MaxPooledNonces = 32;

    /// <summary>Longest accepted <c>Replay-Nonce</c> value.</summary>
    internal const int MaxNonceLength = 512;

    private static readonly byte[] EmptyObjectPayload = JsonSerializer.SerializeToUtf8Bytes(new EmptyRequest(), AcmeJsonContext.Default.EmptyRequest);

    private readonly HttpClient _http;
    private readonly ConcurrentQueue<string> _nonces = new();
    private AcmeDirectory? _directory;

    /// <summary>Creates a client.</summary>
    /// <param name="httpClient">Injected HTTP client (not disposed by this class).</param>
    /// <param name="directoryUrl">The CA directory URL (see <see cref="AcmeDirectories"/>).</param>
    /// <param name="accountKey">The account key.</param>
    /// <param name="accountUrl">Existing account URL (<c>kid</c>); <see langword="null"/> until an account is created / found.</param>
    /// <param name="options">Polling / retry options.</param>
    /// <exception cref="ArgumentException">
    /// <paramref name="directoryUrl"/> is not absolute, or not HTTPS while its host is not loopback and
    /// <see cref="AcmeClientOptions.AllowInsecureHttp"/> is off (RFC 8555 §6.1).
    /// </exception>
    public AcmeClient(HttpClient httpClient, Uri directoryUrl, AcmeAccountKey accountKey, Uri? accountUrl = null, AcmeClientOptions? options = null)
    {
        _http = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        ArgumentNullException.ThrowIfNull(directoryUrl);
        AccountKey = accountKey ?? throw new ArgumentNullException(nameof(accountKey));
        Options = options ?? new AcmeClientOptions();
        if (!IsAllowedUrl(directoryUrl, Options.AllowInsecureHttp))
        {
            throw new ArgumentException(
                "The directory URL '" + directoryUrl + "' must be an absolute https:// URL (RFC 8555 §6.1); plain http is only accepted for loopback hosts or with AcmeClientOptions.AllowInsecureHttp.",
                nameof(directoryUrl));
        }

        DirectoryUrl = directoryUrl;
        AccountUrl = accountUrl;
    }

    /// <summary>The directory URL.</summary>
    public Uri DirectoryUrl { get; }

    /// <summary>The account key used to sign every request.</summary>
    public AcmeAccountKey AccountKey { get; }

    /// <summary>The account URL used as JWS <c>kid</c>; set automatically by <see cref="CreateAccountAsync"/>.</summary>
    public Uri? AccountUrl { get; set; }

    /// <summary>Client options.</summary>
    public AcmeClientOptions Options { get; }

    /// <summary>Number of unused nonces currently pooled.</summary>
    public int PooledNonceCount => _nonces.Count;

    /// <summary>
    /// Creates an HTTP handler whose TLS server-certificate validation trusts exactly <paramref name="trustAnchors"/>
    /// (custom root trust), for a private or mock CA such as Pebble: <c>new HttpClient(AcmeClient.CreateHttpHandler(roots))</c>.
    /// Host-name validation stays on. Revocation is not checked, since private CA roots rarely publish CRL / OCSP; build your
    /// own <see cref="SocketsHttpHandler"/> when that is needed.
    /// </summary>
    /// <param name="trustAnchors">Root certificate(s) that anchor the CA's TLS chain.</param>
    /// <exception cref="ArgumentException"><paramref name="trustAnchors"/> is empty.</exception>
    public static SocketsHttpHandler CreateHttpHandler(X509Certificate2Collection trustAnchors)
    {
        ArgumentNullException.ThrowIfNull(trustAnchors);
        if (trustAnchors.Count == 0)
        {
            throw new ArgumentException("At least one trust anchor is required.", nameof(trustAnchors));
        }

        X509ChainPolicy policy = new()
        {
            TrustMode = X509ChainTrustMode.CustomRootTrust,
            RevocationMode = X509RevocationMode.NoCheck,
        };
        policy.CustomTrustStore.AddRange(trustAnchors);
        return new SocketsHttpHandler
        {
            SslOptions = new System.Net.Security.SslClientAuthenticationOptions { CertificateChainPolicy = policy },
        };
    }

    /// <summary>Fetches (once) and caches the directory.</summary>
    /// <exception cref="AcmeException">The directory cannot be fetched or parsed, or advertises a non-HTTPS URL (<see cref="AcmeErrorTypes.InsecureUrl"/>).</exception>
    public async ValueTask<AcmeDirectory> GetDirectoryAsync(CancellationToken cancellationToken = default)
    {
        if (_directory is { } cached)
        {
            return cached;
        }

        using HttpRequestMessage request = new(HttpMethod.Get, DirectoryUrl);
        AcmeResponse response = await SendAsync(request, cancellationToken).ConfigureAwait(false);
        if (!response.IsSuccess)
        {
            throw CreateException(response);
        }

        AcmeDirectory directory = Parse(response, AcmeJsonContext.Default.AcmeDirectory);

        // Fail early (before any key material is sent) when the directory points somewhere the client would refuse anyway.
        EnsureAllowedUrl(directory.NewNonce);
        EnsureAllowedUrl(directory.NewAccount);
        EnsureAllowedUrl(directory.NewOrder);
        EnsureAllowedOptionalUrl(directory.NewAuthz);
        EnsureAllowedOptionalUrl(directory.RevokeCert);
        EnsureAllowedOptionalUrl(directory.KeyChange);
        EnsureAllowedOptionalUrl(directory.RenewalInfo);
        _directory = directory;
        return directory;
    }

    /// <summary>Returns a pooled nonce or fetches a fresh one via <c>HEAD newNonce</c>.</summary>
    public async ValueTask<string> GetNonceAsync(CancellationToken cancellationToken = default)
    {
        if (_nonces.TryDequeue(out string? pooled))
        {
            return pooled;
        }

        AcmeDirectory directory = await GetDirectoryAsync(cancellationToken).ConfigureAwait(false);
        using HttpRequestMessage request = new(HttpMethod.Head, directory.NewNonce);
        AcmeResponse response = await SendAsync(request, cancellationToken).ConfigureAwait(false);
        if (!response.IsSuccess)
        {
            throw CreateException(response);
        }

        if (!_nonces.TryDequeue(out string? fresh))
        {
            throw AcmeException.Client(AcmeErrorTypes.InvalidResponse, "newNonce response carried no Replay-Nonce header.");
        }

        return fresh;
    }

    /// <summary>
    /// Creates an account (RFC 8555 §7.3) or, with <paramref name="onlyReturnExisting"/>, looks up the account for
    /// <see cref="AccountKey"/>. On success <see cref="AccountUrl"/> is set from the <c>Location</c> header.
    /// </summary>
    /// <param name="contacts">Contact URLs such as <c>mailto:admin@example.com</c>.</param>
    /// <param name="agreeTermsOfService">Whether the terms of service are accepted.</param>
    /// <param name="externalAccountBinding">EAB credentials for CAs that require them (see <see cref="AcmeDirectories.KnownCas"/>).</param>
    /// <param name="onlyReturnExisting">When true the server must not create a new account (error <c>accountDoesNotExist</c> if none).</param>
    /// <param name="cancellationToken">Cancellation.</param>
    public async ValueTask<AcmeAccount> CreateAccountAsync(
        IReadOnlyList<string>? contacts,
        bool agreeTermsOfService,
        ExternalAccountBinding? externalAccountBinding = null,
        bool onlyReturnExisting = false,
        CancellationToken cancellationToken = default)
    {
        AcmeDirectory directory = await GetDirectoryAsync(cancellationToken).ConfigureAwait(false);
        NewAccountRequest payload = new()
        {
            Contact = contacts is { Count: > 0 } ? contacts : null,
            TermsOfServiceAgreed = agreeTermsOfService ? true : null,
            OnlyReturnExisting = onlyReturnExisting ? true : null,
            ExternalAccountBinding = externalAccountBinding is null
                ? null
                : JwsSigner.CreateExternalAccountBinding(AccountKey, externalAccountBinding, directory.NewAccount),
        };
        byte[] body = JsonSerializer.SerializeToUtf8Bytes(payload, AcmeJsonContext.Default.NewAccountRequest);
        AcmeResponse response = await SendSignedAsync(directory.NewAccount, body, embedJwk: true, accept: null, cancellationToken).ConfigureAwait(false);
        AcmeAccount account = Parse(response, AcmeJsonContext.Default.AcmeAccount);
        Uri location = response.Location ?? throw AcmeException.Client(AcmeErrorTypes.InvalidResponse, "newAccount response carried no Location header.");
        AccountUrl = location;
        return account with { Location = location };
    }

    /// <summary>Looks up the existing account for <see cref="AccountKey"/> without creating one.</summary>
    public ValueTask<AcmeAccount> FindAccountAsync(CancellationToken cancellationToken = default)
    {
        return CreateAccountAsync(null, agreeTermsOfService: false, externalAccountBinding: null, onlyReturnExisting: true, cancellationToken);
    }

    /// <summary>Fetches the account object (POST-as-GET to <see cref="AccountUrl"/>).</summary>
    public async ValueTask<AcmeAccount> GetAccountAsync(CancellationToken cancellationToken = default)
    {
        Uri url = RequireAccountUrl();
        AcmeResponse response = await SendSignedAsync(url, default, embedJwk: false, accept: null, cancellationToken).ConfigureAwait(false);
        return Parse(response, AcmeJsonContext.Default.AcmeAccount) with { Location = url };
    }

    /// <summary>Deactivates the account (RFC 8555 §7.3.6). This cannot be undone.</summary>
    public async ValueTask<AcmeAccount> DeactivateAccountAsync(CancellationToken cancellationToken = default)
    {
        Uri url = RequireAccountUrl();
        byte[] body = JsonSerializer.SerializeToUtf8Bytes(new AccountUpdateRequest { Status = AcmeAccountStatus.Deactivated }, AcmeJsonContext.Default.AccountUpdateRequest);
        AcmeResponse response = await SendSignedAsync(url, body, embedJwk: false, accept: null, cancellationToken).ConfigureAwait(false);
        return Parse(response, AcmeJsonContext.Default.AcmeAccount) with { Location = url };
    }

    /// <summary>Creates a new order (RFC 8555 §7.4).</summary>
    /// <param name="identifiers">Identifiers to certify.</param>
    /// <param name="notBefore">Requested certificate <c>notBefore</c> (most CAs ignore or reject it).</param>
    /// <param name="notAfter">Requested certificate <c>notAfter</c>.</param>
    /// <param name="replaces">ARI certificate identifier (<see cref="GetAriCertificateId"/>) of the certificate being renewed (RFC 9773 §5), or <see langword="null"/>.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    public async ValueTask<AcmeOrder> NewOrderAsync(
        IReadOnlyList<AcmeIdentifier> identifiers,
        DateTimeOffset? notBefore = null,
        DateTimeOffset? notAfter = null,
        string? replaces = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(identifiers);
        if (identifiers.Count == 0)
        {
            throw new ArgumentException("At least one identifier is required.", nameof(identifiers));
        }

        AcmeDirectory directory = await GetDirectoryAsync(cancellationToken).ConfigureAwait(false);
        NewOrderRequest payload = new() { Identifiers = identifiers, NotBefore = notBefore, NotAfter = notAfter, Replaces = replaces };
        byte[] body = JsonSerializer.SerializeToUtf8Bytes(payload, AcmeJsonContext.Default.NewOrderRequest);
        AcmeResponse response = await SendSignedAsync(directory.NewOrder, body, embedJwk: false, accept: null, cancellationToken).ConfigureAwait(false);
        AcmeOrder order = Parse(response, AcmeJsonContext.Default.AcmeOrder);
        Uri location = response.Location ?? throw AcmeException.Client(AcmeErrorTypes.InvalidResponse, "newOrder response carried no Location header.");
        return order with { Location = location };
    }

    /// <summary>Fetches an order.</summary>
    public async ValueTask<AcmeOrder> GetOrderAsync(Uri orderUrl, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(orderUrl);
        AcmeResponse response = await SendSignedAsync(orderUrl, default, embedJwk: false, accept: null, cancellationToken).ConfigureAwait(false);
        return Parse(response, AcmeJsonContext.Default.AcmeOrder) with { Location = orderUrl };
    }

    /// <summary>Fetches an authorization.</summary>
    public async ValueTask<AcmeAuthorization> GetAuthorizationAsync(Uri authorizationUrl, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(authorizationUrl);
        AcmeResponse response = await SendSignedAsync(authorizationUrl, default, embedJwk: false, accept: null, cancellationToken).ConfigureAwait(false);
        return Parse(response, AcmeJsonContext.Default.AcmeAuthorization) with { Location = authorizationUrl };
    }

    /// <summary>Fetches a challenge.</summary>
    public async ValueTask<AcmeChallenge> GetChallengeAsync(Uri challengeUrl, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(challengeUrl);
        AcmeResponse response = await SendSignedAsync(challengeUrl, default, embedJwk: false, accept: null, cancellationToken).ConfigureAwait(false);
        return Parse(response, AcmeJsonContext.Default.AcmeChallenge);
    }

    /// <summary>Tells the server the challenge is ready to be validated (POST <c>{}</c>, RFC 8555 §7.5.1).</summary>
    public async ValueTask<AcmeChallenge> RespondToChallengeAsync(Uri challengeUrl, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(challengeUrl);
        AcmeResponse response = await SendSignedAsync(challengeUrl, EmptyObjectPayload, embedJwk: false, accept: null, cancellationToken).ConfigureAwait(false);
        return Parse(response, AcmeJsonContext.Default.AcmeChallenge);
    }

    /// <summary>
    /// Polls the authorization until it leaves <c>pending</c>. Returns it when <c>valid</c>; throws
    /// <see cref="AcmeException"/> (<see cref="AcmeErrorTypes.ValidationFailed"/>, carrying the failed challenge's error as a
    /// subproblem) for any other terminal status and <see cref="AcmeErrorTypes.PollTimeout"/> when
    /// <see cref="AcmeClientOptions.MaxPollAttempts"/> is exhausted.
    /// </summary>
    public async ValueTask<AcmeAuthorization> WaitForAuthorizationAsync(Uri authorizationUrl, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(authorizationUrl);
        for (int attempt = 0; attempt < Options.MaxPollAttempts; attempt++)
        {
            AcmeResponse response = await SendSignedAsync(authorizationUrl, default, embedJwk: false, accept: null, cancellationToken).ConfigureAwait(false);
            AcmeAuthorization authz = Parse(response, AcmeJsonContext.Default.AcmeAuthorization) with { Location = authorizationUrl };
            if (string.Equals(authz.Status, AcmeAuthorizationStatus.Valid, StringComparison.Ordinal))
            {
                return authz;
            }

            if (!string.Equals(authz.Status, AcmeAuthorizationStatus.Pending, StringComparison.Ordinal))
            {
                throw new AcmeException(new AcmeProblem
                {
                    Type = AcmeErrorTypes.ValidationFailed,
                    Detail = "Authorization for " + authz.Identifier + " ended in status '" + authz.Status + "'.",
                    Identifier = authz.Identifier,
                    Subproblems = CollectChallengeErrors(authz),
                });
            }

            await DelayAsync(response.RetryAfter, cancellationToken).ConfigureAwait(false);
        }

        throw AcmeException.Client(AcmeErrorTypes.PollTimeout, "Authorization " + authorizationUrl + " is still pending after " + Options.MaxPollAttempts + " polls.");
    }

    /// <summary>
    /// Polls the order until it leaves <c>pending</c> / <c>processing</c>. Returns it when <c>ready</c> or <c>valid</c>;
    /// throws <see cref="AcmeException"/> (<see cref="AcmeErrorTypes.ValidationFailed"/>, carrying the order error as a
    /// subproblem) when <c>invalid</c>, and <see cref="AcmeErrorTypes.PollTimeout"/> when polls are exhausted.
    /// </summary>
    public async ValueTask<AcmeOrder> WaitForOrderAsync(Uri orderUrl, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(orderUrl);
        for (int attempt = 0; attempt < Options.MaxPollAttempts; attempt++)
        {
            AcmeResponse response = await SendSignedAsync(orderUrl, default, embedJwk: false, accept: null, cancellationToken).ConfigureAwait(false);
            AcmeOrder order = Parse(response, AcmeJsonContext.Default.AcmeOrder) with { Location = orderUrl };
            switch (order.Status)
            {
                case AcmeOrderStatus.Ready:
                case AcmeOrderStatus.Valid:
                    return order;
                case AcmeOrderStatus.Pending:
                case AcmeOrderStatus.Processing:
                    await DelayAsync(response.RetryAfter, cancellationToken).ConfigureAwait(false);
                    break;
                default:
                    throw new AcmeException(new AcmeProblem
                    {
                        Type = AcmeErrorTypes.ValidationFailed,
                        Detail = "Order " + orderUrl + " ended in status '" + order.Status + "'.",
                        Subproblems = order.Error is null ? null : [order.Error],
                    });
            }
        }

        throw AcmeException.Client(AcmeErrorTypes.PollTimeout, "Order " + orderUrl + " is still pending after " + Options.MaxPollAttempts + " polls.");
    }

    /// <summary>
    /// Submits the CSR (DER) to the order's <c>finalize</c> URL (RFC 8555 §7.4). The returned order's
    /// <see cref="AcmeOrder.Location"/> is the <c>Location</c> header when the CA sends one and <see langword="null"/>
    /// otherwise (the header is optional on finalize), so keep the order URL from <see cref="NewOrderAsync"/>.
    /// </summary>
    public async ValueTask<AcmeOrder> FinalizeOrderAsync(Uri finalizeUrl, byte[] csrDer, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(finalizeUrl);
        ArgumentNullException.ThrowIfNull(csrDer);
        byte[] body = JsonSerializer.SerializeToUtf8Bytes(new FinalizeRequest { Csr = Base64UrlCodec.Encode(csrDer) }, AcmeJsonContext.Default.FinalizeRequest);
        AcmeResponse response = await SendSignedAsync(finalizeUrl, body, embedJwk: false, accept: null, cancellationToken).ConfigureAwait(false);
        return Parse(response, AcmeJsonContext.Default.AcmeOrder) with { Location = response.Location };
    }

    /// <summary>Downloads the certificate chain (RFC 8555 §7.4.2) as <c>application/pem-certificate-chain</c>.</summary>
    public async ValueTask<AcmeCertificateChain> DownloadCertificateAsync(Uri certificateUrl, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(certificateUrl);
        AcmeResponse response = await SendSignedAsync(certificateUrl, default, embedJwk: false, accept: PemChainContentType, cancellationToken).ConfigureAwait(false);
        string pem = System.Text.Encoding.UTF8.GetString(response.Body);
        X509Certificate2Collection certificates = [];
        try
        {
            certificates.ImportFromPem(pem);
        }
        catch (System.Security.Cryptography.CryptographicException e)
        {
            throw AcmeException.Client(AcmeErrorTypes.InvalidResponse, "Certificate response is not a valid PEM chain.", e);
        }

        if (certificates.Count == 0)
        {
            throw AcmeException.Client(AcmeErrorTypes.InvalidResponse, "Certificate response contained no certificates.");
        }

        List<Uri> alternates = [];
        for (int i = 0; i < response.Links.Count; i++)
        {
            if (HasRelation(response.Links[i].Rel, "alternate"))
            {
                alternates.Add(response.Links[i].Url);
            }
        }

        return new AcmeCertificateChain(pem, certificates, alternates);
    }

    /// <summary>Revokes a certificate (DER) using the account key (RFC 8555 §7.6).</summary>
    public async ValueTask RevokeCertificateAsync(byte[] certificateDer, AcmeRevocationReason? reason = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(certificateDer);
        Uri url = await GetRevokeUrlAsync(cancellationToken).ConfigureAwait(false);
        await SendSignedAsync(url, CreateRevokeBody(certificateDer, reason), embedJwk: false, accept: null, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Revokes a certificate using the account key.</summary>
    public ValueTask RevokeCertificateAsync(X509Certificate2 certificate, AcmeRevocationReason? reason = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(certificate);
        return RevokeCertificateAsync(certificate.RawData, reason, cancellationToken);
    }

    /// <summary>
    /// Revokes a certificate (DER) with a JWS signed by the certificate's own private key, which is embedded as <c>jwk</c>
    /// (RFC 8555 §7.6). No account is needed, so this works when the account key is lost, and it is the natural path for
    /// <see cref="AcmeRevocationReason.KeyCompromise"/>.
    /// </summary>
    /// <param name="certificateDer">The certificate to revoke.</param>
    /// <param name="certificateKey">
    /// The certificate's private key, wrapped as an <see cref="AcmeAccountKey"/> (P-256 or RSA 2048+, as created by
    /// <see cref="CsrBuilder.CreateKey"/>), for example <c>new AcmeAccountKey(cert.GetECDsaPrivateKey()!)</c>. Not disposed.
    /// </param>
    /// <param name="reason">Revocation reason.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    public async ValueTask RevokeCertificateWithKeyAsync(byte[] certificateDer, AcmeAccountKey certificateKey, AcmeRevocationReason? reason = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(certificateDer);
        ArgumentNullException.ThrowIfNull(certificateKey);
        Uri url = await GetRevokeUrlAsync(cancellationToken).ConfigureAwait(false);
        await SendSignedAsync(url, CreateRevokeBody(certificateDer, reason), embedJwk: true, accept: null, cancellationToken, certificateKey).ConfigureAwait(false);
    }

    /// <summary>
    /// Fetches ARI renewal information (RFC 9773 §4.2) for <paramref name="certificate"/> with an unsigned GET.
    /// Returns <see langword="null"/> when the directory advertises no <c>renewalInfo</c> endpoint.
    /// </summary>
    /// <exception cref="ArgumentException">The certificate has no Authority Key Identifier extension.</exception>
    public async ValueTask<AcmeRenewalInfo?> GetRenewalInfoAsync(X509Certificate2 certificate, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(certificate);
        AcmeDirectory directory = await GetDirectoryAsync(cancellationToken).ConfigureAwait(false);
        if (directory.RenewalInfo is not { } endpoint)
        {
            return null;
        }

        string id = GetAriCertificateId(certificate);
        string baseUrl = endpoint.AbsoluteUri;
        Uri url = new(baseUrl.EndsWith('/') ? baseUrl + id : baseUrl + "/" + id);
        using HttpRequestMessage request = new(HttpMethod.Get, url);
        AcmeResponse response = await SendAsync(request, cancellationToken).ConfigureAwait(false);
        if (!response.IsSuccess)
        {
            throw CreateException(response);
        }

        AcmeRenewalInfo info = Parse(response, AcmeJsonContext.Default.AcmeRenewalInfo);
        return info with { RetryAfter = response.RetryAfter };
    }

    /// <summary>
    /// Computes the ARI certificate identifier (RFC 9773 §4.1): <c>base64url(AKI keyIdentifier) "." base64url(serial number DER value)</c>.
    /// </summary>
    /// <exception cref="ArgumentException">The certificate has no Authority Key Identifier extension with a key identifier.</exception>
    public static string GetAriCertificateId(X509Certificate2 certificate)
    {
        ArgumentNullException.ThrowIfNull(certificate);
        foreach (X509Extension extension in certificate.Extensions)
        {
            if (extension is X509AuthorityKeyIdentifierExtension { KeyIdentifier: { } keyIdentifier })
            {
                return string.Concat(Base64UrlCodec.Encode(keyIdentifier.Span), ".", Base64UrlCodec.Encode(certificate.SerialNumberBytes.Span));
            }
        }

        throw new ArgumentException("The certificate carries no Authority Key Identifier with a key identifier; ARI cannot identify it.", nameof(certificate));
    }

    /// <summary>
    /// True when <paramref name="url"/> may be requested: absolute <c>https</c>, or <c>http</c> to a loopback host or with
    /// <paramref name="allowInsecureHttp"/> (RFC 8555 §6.1).
    /// </summary>
    internal static bool IsAllowedUrl(Uri url, bool allowInsecureHttp)
    {
        if (!url.IsAbsoluteUri)
        {
            return false;
        }

        if (string.Equals(url.Scheme, Uri.UriSchemeHttps, StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return string.Equals(url.Scheme, Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase) && (allowInsecureHttp || url.IsLoopback);
    }

    /// <summary>True for a non-empty base64url nonce of sane length (RFC 8555 §6.5.1).</summary>
    internal static bool IsValidNonce(string? nonce)
    {
        if (string.IsNullOrEmpty(nonce) || nonce.Length > MaxNonceLength)
        {
            return false;
        }

        for (int i = 0; i < nonce.Length; i++)
        {
            char c = nonce[i];
            if (!(char.IsAsciiLetterOrDigit(c) || c == '-' || c == '_'))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Parses one <c>Link</c> header value (RFC 8288): <c>&lt;URI&gt; *( ";" param )</c> entries separated by <c>,</c>. The
    /// URI is taken verbatim between the angle brackets and parameter values may be quoted strings, so commas or
    /// semicolons inside either do not split an entry. Only absolute URIs are kept; the first <c>rel</c> wins.
    /// </summary>
    internal static void ParseLinkHeader(string value, List<(Uri Url, string Rel)> links)
    {
        int i = 0;
        while (i < value.Length)
        {
            int open = value.IndexOf('<', i);
            if (open < 0)
            {
                return;
            }

            int close = value.IndexOf('>', open + 1);
            if (close < 0)
            {
                return;
            }

            string target = value.Substring(open + 1, close - open - 1);
            string? rel = null;
            i = close + 1;
            while (i < value.Length && value[i] != ',')
            {
                if (value[i] != ';')
                {
                    i++;
                    continue;
                }

                i++;
                (string name, string paramValue) = ReadLinkParam(value, ref i);
                if (rel is null && string.Equals(name, "rel", StringComparison.OrdinalIgnoreCase))
                {
                    rel = paramValue;
                }
            }

            i++; // skip the ',' separating link-values
            if (Uri.TryCreate(target.Trim(), UriKind.Absolute, out Uri? url))
            {
                links.Add((url, rel ?? string.Empty));
            }
        }
    }

    /// <summary>Reads <c>name[=value]</c> at <paramref name="i"/> (after a <c>;</c>), stopping before the next <c>;</c> or <c>,</c> outside quotes.</summary>
    private static (string Name, string Value) ReadLinkParam(string s, ref int i)
    {
        int nameStart = i;
        while (i < s.Length && s[i] is not ('=' or ';' or ','))
        {
            i++;
        }

        string name = s[nameStart..i].Trim();
        if (i >= s.Length || s[i] != '=')
        {
            return (name, string.Empty);
        }

        i++; // '='
        while (i < s.Length && s[i] is ' ' or '\t')
        {
            i++;
        }

        if (i < s.Length && s[i] == '"')
        {
            System.Text.StringBuilder quoted = new();
            i++;
            while (i < s.Length && s[i] != '"')
            {
                if (s[i] == '\\' && i + 1 < s.Length)
                {
                    i++; // quoted-pair
                }

                quoted.Append(s[i]);
                i++;
            }

            i++; // closing quote
            return (name, quoted.ToString());
        }

        int valueStart = i;
        while (i < s.Length && s[i] is not (';' or ','))
        {
            i++;
        }

        return (name, s[valueStart..i].Trim());
    }

    /// <summary>True when the space-separated relation types in <paramref name="rel"/> include <paramref name="relation"/> (case-insensitive).</summary>
    private static bool HasRelation(string rel, string relation)
    {
        foreach (string part in rel.Split(' ', StringSplitOptions.RemoveEmptyEntries))
        {
            if (string.Equals(part, relation, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private async ValueTask<Uri> GetRevokeUrlAsync(CancellationToken cancellationToken)
    {
        AcmeDirectory directory = await GetDirectoryAsync(cancellationToken).ConfigureAwait(false);
        return directory.RevokeCert ?? throw AcmeException.Client(AcmeErrorTypes.InvalidResponse, "The directory advertises no revokeCert URL.");
    }

    private static byte[] CreateRevokeBody(byte[] certificateDer, AcmeRevocationReason? reason)
    {
        RevokeRequest payload = new() { Certificate = Base64UrlCodec.Encode(certificateDer), Reason = reason is null ? null : (int)reason.Value };
        return JsonSerializer.SerializeToUtf8Bytes(payload, AcmeJsonContext.Default.RevokeRequest);
    }

    private Uri RequireAccountUrl()
    {
        return AccountUrl ?? throw new InvalidOperationException("AccountUrl (kid) is not set; call CreateAccountAsync or FindAccountAsync first.");
    }

    private Task DelayAsync(TimeSpan? retryAfter, CancellationToken cancellationToken)
    {
        TimeSpan delay = Options.PollInterval;
        if (retryAfter is TimeSpan ra)
        {
            delay = ra < TimeSpan.Zero ? TimeSpan.Zero : ra > Options.MaxRetryAfter ? Options.MaxRetryAfter : ra;
        }

        return AcmeTimers.Delay(delay, Options.TimeProvider, cancellationToken);
    }

    private static IReadOnlyList<AcmeProblem>? CollectChallengeErrors(AcmeAuthorization authz)
    {
        List<AcmeProblem>? errors = null;
        for (int i = 0; i < authz.Challenges.Count; i++)
        {
            if (authz.Challenges[i].Error is { } error)
            {
                (errors ??= []).Add(error);
            }
        }

        return errors;
    }

    /// <param name="url">Request URL (also the JWS <c>url</c> header).</param>
    /// <param name="payload">JSON payload; empty for POST-as-GET.</param>
    /// <param name="embedJwk">Embed the public key as <c>jwk</c> instead of the account <c>kid</c> (newAccount / revokeCert).</param>
    /// <param name="accept">Optional <c>Accept</c> media type.</param>
    /// <param name="cancellationToken">Cancellation.</param>
    /// <param name="signingKey">Key to sign with; <see langword="null"/> for <see cref="AccountKey"/>.</param>
    private async Task<AcmeResponse> SendSignedAsync(Uri url, ReadOnlyMemory<byte> payload, bool embedJwk, string? accept, CancellationToken cancellationToken, AcmeAccountKey? signingKey = null)
    {
        Uri? kid = embedJwk ? null : RequireAccountUrl();
        string? retryNonce = null;
        for (int attempt = 0; ; attempt++)
        {
            string nonce = retryNonce ?? await GetNonceAsync(cancellationToken).ConfigureAwait(false);
            byte[] body = JwsSigner.SignToUtf8(signingKey ?? AccountKey, url, nonce, payload.Span, kid);
            using HttpRequestMessage request = new(HttpMethod.Post, url) { Content = new ByteArrayContent(body) };
            request.Content.Headers.ContentType = new MediaTypeHeaderValue(JoseContentType);
            if (accept is not null)
            {
                request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue(accept));
            }

            AcmeResponse response = await SendAsync(request, cancellationToken, harvestNonces: false).ConfigureAwait(false);
            if (response.IsSuccess)
            {
                HarvestNonces(response.Nonces);
                return response;
            }

            AcmeException error = CreateException(response);
            if (attempt == 0 && error.IsType(AcmeErrorTypes.BadNonce))
            {
                // RFC 8555 §6.5: retry with the nonce carried by this badNonce response. Nonces pooled before it were
                // harvested at the same time as the rejected one (typically before a long idle), so drop them too.
                _nonces.Clear();
                retryNonce = response.Nonces.Count > 0 ? response.Nonces[^1] : null;
                continue;
            }

            HarvestNonces(response.Nonces);
            throw error;
        }
    }

    /// <summary>Adds nonces to the pool, which is bounded so a hostile server cannot grow it without limit (ADR 0009).</summary>
    private void HarvestNonces(List<string> nonces)
    {
        for (int i = 0; i < nonces.Count && _nonces.Count < MaxPooledNonces; i++)
        {
            _nonces.Enqueue(nonces[i]);
        }
    }

    private void EnsureAllowedOptionalUrl(Uri? url)
    {
        if (url is not null)
        {
            EnsureAllowedUrl(url);
        }
    }

    private void EnsureAllowedUrl(Uri? url)
    {
        if (url is null || !IsAllowedUrl(url, Options.AllowInsecureHttp))
        {
            throw AcmeException.Client(
                AcmeErrorTypes.InsecureUrl,
                "Refusing to request '" + url + "': ACME requires https (RFC 8555 §6.1); plain http is only accepted for loopback hosts or with AcmeClientOptions.AllowInsecureHttp.");
        }
    }

    private async Task<AcmeResponse> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken, bool harvestNonces = true)
    {
        // Every URL that reaches the wire passes here: the directory URL, directory entries, Location / Link targets and
        // the order, authorization, challenge, certificate and renewalInfo URLs the server hands out.
        EnsureAllowedUrl(request.RequestUri);
        request.Headers.UserAgent.ParseAdd(Options.UserAgent);
        using HttpResponseMessage response = await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
        byte[] body = await ReadBoundedAsync(response.Content, Options.MaxResponseBytes, cancellationToken).ConfigureAwait(false);

        List<string> nonces = [];
        if (response.Headers.TryGetValues("Replay-Nonce", out IEnumerable<string>? nonceValues))
        {
            foreach (string nonce in nonceValues)
            {
                // RFC 8555 §6.5.1: the value is base64url; clients MUST ignore invalid values.
                if (IsValidNonce(nonce) && nonces.Count < MaxPooledNonces)
                {
                    nonces.Add(nonce);
                }
            }
        }

        if (harvestNonces)
        {
            HarvestNonces(nonces);
        }

        List<(Uri Url, string Rel)> links = [];
        if (response.Headers.TryGetValues("Link", out IEnumerable<string>? linkValues))
        {
            foreach (string value in linkValues)
            {
                ParseLinkHeader(value, links);
            }
        }

        TimeSpan? retryAfter = null;
        if (response.Headers.RetryAfter is { } ra)
        {
            retryAfter = ra.Delta ?? (ra.Date is { } date ? date - Options.TimeProvider.GetUtcNow() : null);
        }

        // Location may be relative (RFC 9110 §10.2.2); resolve it against the request URL.
        Uri? location = response.Headers.Location;
        if (location is not null && !location.IsAbsoluteUri)
        {
            location = new Uri(request.RequestUri!, location);
        }

        return new AcmeResponse(response.StatusCode, body, response.Content.Headers.ContentType?.MediaType, location, links, retryAfter, nonces);
    }

    /// <summary>Reads the body, refusing (before buffering it) anything larger than <paramref name="maxBytes"/>.</summary>
    private static async Task<byte[]> ReadBoundedAsync(HttpContent content, int maxBytes, CancellationToken cancellationToken)
    {
        if (content.Headers.ContentLength is long declared && declared > maxBytes)
        {
            throw TooLarge(maxBytes);
        }

        Stream stream = await content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        await using (stream.ConfigureAwait(false))
        {
            using MemoryStream buffer = new();
            byte[] chunk = new byte[16 * 1024];
            while (true)
            {
                int read = await stream.ReadAsync(chunk, cancellationToken).ConfigureAwait(false);
                if (read == 0)
                {
                    return buffer.ToArray();
                }

                if (buffer.Length + read > maxBytes)
                {
                    throw TooLarge(maxBytes);
                }

                buffer.Write(chunk, 0, read);
            }
        }

        static AcmeException TooLarge(int max) => AcmeException.Client(
            AcmeErrorTypes.InvalidResponse,
            "The server response exceeds MaxResponseBytes (" + max.ToString(System.Globalization.CultureInfo.InvariantCulture) + ").");
    }

    private static T Parse<T>(AcmeResponse response, JsonTypeInfo<T> typeInfo)
        where T : class
    {
        T? value;
        try
        {
            value = JsonSerializer.Deserialize(response.Body, typeInfo);
        }
        catch (JsonException e)
        {
            throw AcmeException.Client(AcmeErrorTypes.InvalidResponse, "The server response is not valid JSON.", e);
        }

        return value ?? throw AcmeException.Client(AcmeErrorTypes.InvalidResponse, "The server response is empty.");
    }

    private static AcmeException CreateException(AcmeResponse response)
    {
        AcmeProblem? problem = null;
        if (response.Body.Length > 0 && string.Equals(response.ContentType, ProblemContentType, StringComparison.OrdinalIgnoreCase))
        {
            try
            {
                problem = JsonSerializer.Deserialize(response.Body, AcmeJsonContext.Default.AcmeProblem);
            }
            catch (JsonException)
            {
                problem = null;
            }
        }

        problem ??= new AcmeProblem { Type = AcmeErrorTypes.Unknown, Detail = "HTTP " + ((int)response.StatusCode).ToString(System.Globalization.CultureInfo.InvariantCulture) + " without a problem document." };
        if (problem.Status is null)
        {
            problem = problem with { Status = (int)response.StatusCode };
        }

        return new AcmeException(problem, response.StatusCode) { RetryAfter = response.RetryAfter };
    }

    // Nonces: the valid Replay-Nonce values of the response (already pooled unless the caller harvests them itself).
    private sealed record AcmeResponse(
        HttpStatusCode StatusCode,
        byte[] Body,
        string? ContentType,
        Uri? Location,
        List<(Uri Url, string Rel)> Links,
        TimeSpan? RetryAfter,
        List<string> Nonces)
    {
        public bool IsSuccess => (int)StatusCode is >= 200 and < 300;
    }
}
