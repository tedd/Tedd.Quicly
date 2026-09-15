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
/// <c>badNonce</c>, <c>Retry-After</c>-aware polling and structured problem documents (<see cref="AcmeException"/>).
/// </summary>
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
    public AcmeClient(HttpClient httpClient, Uri directoryUrl, AcmeAccountKey accountKey, Uri? accountUrl = null, AcmeClientOptions? options = null)
    {
        _http = httpClient ?? throw new ArgumentNullException(nameof(httpClient));
        DirectoryUrl = directoryUrl ?? throw new ArgumentNullException(nameof(directoryUrl));
        AccountKey = accountKey ?? throw new ArgumentNullException(nameof(accountKey));
        AccountUrl = accountUrl;
        Options = options ?? new AcmeClientOptions();
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

    /// <summary>Fetches (once) and caches the directory.</summary>
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
            if (string.Equals(response.Links[i].Rel, "alternate", StringComparison.OrdinalIgnoreCase))
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
        AcmeDirectory directory = await GetDirectoryAsync(cancellationToken).ConfigureAwait(false);
        Uri url = directory.RevokeCert ?? throw AcmeException.Client(AcmeErrorTypes.InvalidResponse, "The directory advertises no revokeCert URL.");
        RevokeRequest payload = new() { Certificate = Base64UrlCodec.Encode(certificateDer), Reason = reason is null ? null : (int)reason.Value };
        byte[] body = JsonSerializer.SerializeToUtf8Bytes(payload, AcmeJsonContext.Default.RevokeRequest);
        await SendSignedAsync(url, body, embedJwk: false, accept: null, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Revokes a certificate using the account key.</summary>
    public ValueTask RevokeCertificateAsync(X509Certificate2 certificate, AcmeRevocationReason? reason = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(certificate);
        return RevokeCertificateAsync(certificate.RawData, reason, cancellationToken);
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

        return Task.Delay(delay, Options.TimeProvider, cancellationToken);
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

    private async Task<AcmeResponse> SendSignedAsync(Uri url, ReadOnlyMemory<byte> payload, bool embedJwk, string? accept, CancellationToken cancellationToken)
    {
        Uri? kid = embedJwk ? null : RequireAccountUrl();
        for (int attempt = 0; ; attempt++)
        {
            string nonce = await GetNonceAsync(cancellationToken).ConfigureAwait(false);
            byte[] body = JwsSigner.SignToUtf8(AccountKey, url, nonce, payload.Span, kid);
            using HttpRequestMessage request = new(HttpMethod.Post, url) { Content = new ByteArrayContent(body) };
            request.Content.Headers.ContentType = new MediaTypeHeaderValue(JoseContentType);
            if (accept is not null)
            {
                request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue(accept));
            }

            AcmeResponse response = await SendAsync(request, cancellationToken).ConfigureAwait(false);
            if (response.IsSuccess)
            {
                return response;
            }

            AcmeException error = CreateException(response);
            if (attempt == 0 && error.IsType(AcmeErrorTypes.BadNonce))
            {
                continue;
            }

            throw error;
        }
    }

    private async Task<AcmeResponse> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        request.Headers.UserAgent.ParseAdd(Options.UserAgent);
        using HttpResponseMessage response = await _http.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
        byte[] body = await ReadBoundedAsync(response.Content, Options.MaxResponseBytes, cancellationToken).ConfigureAwait(false);

        if (response.Headers.TryGetValues("Replay-Nonce", out IEnumerable<string>? nonces))
        {
            foreach (string nonce in nonces)
            {
                // RFC 8555 §6.5.1: the value is base64url; clients MUST ignore invalid values. The pool is bounded so a
                // hostile server cannot grow it without limit (ADR 0009).
                if (IsValidNonce(nonce) && _nonces.Count < MaxPooledNonces)
                {
                    _nonces.Enqueue(nonce);
                }
            }
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

        return new AcmeResponse(response.StatusCode, body, response.Content.Headers.ContentType?.MediaType, location, links, retryAfter);
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

    private static void ParseLinkHeader(string value, List<(Uri Url, string Rel)> links)
    {
        // Link: <https://a>;rel="alternate", <https://b>;rel="index"
        foreach (string part in value.Split(','))
        {
            string[] segments = part.Split(';');
            string urlPart = segments[0].Trim();
            if (urlPart.Length < 2 || urlPart[0] != '<' || urlPart[^1] != '>')
            {
                continue;
            }

            string rel = string.Empty;
            for (int i = 1; i < segments.Length; i++)
            {
                string param = segments[i].Trim();
                if (param.StartsWith("rel=", StringComparison.OrdinalIgnoreCase))
                {
                    rel = param[4..].Trim('"');
                }
            }

            if (Uri.TryCreate(urlPart[1..^1], UriKind.Absolute, out Uri? url))
            {
                links.Add((url, rel));
            }
        }
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

    private sealed record AcmeResponse(
        HttpStatusCode StatusCode,
        byte[] Body,
        string? ContentType,
        Uri? Location,
        List<(Uri Url, string Rel)> Links,
        TimeSpan? RetryAfter)
    {
        public bool IsSuccess => (int)StatusCode is >= 200 and < 300;
    }
}
