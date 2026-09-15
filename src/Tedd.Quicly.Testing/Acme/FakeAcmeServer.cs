using System.Formats.Asn1;
using System.Globalization;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Tedd.Quicly.Acme;
using Tedd.Quicly.Acme.Models;

namespace Tedd.Quicly.Testing.Acme;

/// <summary>
/// An in-process RFC 8555 server on a loopback HttpListener: directory, nonces, accounts (JWS verification for ES256 and
/// RS256, single-use nonces, EAB), orders, authorizations, challenges, finalization (CSR parsed and signed by
/// <see cref="TestCa"/>), certificate download with an alternate chain, revocation, ARI, and controllable failures.
/// </summary>
/// <remarks>
/// <para>For tests only: nothing here is hardened, and no production project may reference <c>Tedd.Quicly.Testing</c>.</para>
/// <para>Challenge validation has two modes per type. The callback mode (<see cref="Http01Lookup"/>,
/// <see cref="TlsAlpnLookup"/>) asks a delegate what the responder would serve. The real mode validates over the network
/// like a public CA: <c>http-01</c> performs <c>GET http://{<see cref="Http01ValidationHost"/>}:{<see cref="Http01ValidationPort"/>}/.well-known/acme-challenge/{token}</c>
/// with <c>Host</c> set to the identifier, follows no redirects, and compares the body with the key authorization;
/// <c>tls-alpn-01</c> connects to <see cref="TlsAlpnValidationHost"/>:<see cref="TlsAlpnValidationPort"/>, handshakes
/// TLS with SNI set to the identifier (the <c>in-addr.arpa</c> / <c>ip6.arpa</c> name for IP identifiers, RFC 8738)
/// and ALPN <c>acme-tls/1</c>, and checks the presented certificate: a single subjectAltName equal to the identifier
/// and a critical <c>acmeIdentifier</c> extension (<c>1.3.6.1.5.5.7.1.31</c>) holding SHA-256(keyAuthorization).
/// When a callback is set it takes precedence. <c>dns-01</c> always queries the injected <see cref="DnsTxtLookup"/>
/// (for example <see cref="InMemoryDns01Provider.Lookup"/>); real DNS is out of scope. Every validation is recorded in
/// <see cref="ValidationLog"/>.</para>
/// </remarks>
public sealed class FakeAcmeServer : IAsyncDisposable
{
    private readonly HttpListener _listener = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly object _lock = new();
    private readonly HashSet<string> _validNonces = new(StringComparer.Ordinal);
    private readonly Dictionary<string, AccountRecord> _accounts = new(StringComparer.Ordinal);
    private readonly Dictionary<string, OrderRecord> _orders = new(StringComparer.Ordinal);
    private readonly Dictionary<string, AuthzRecord> _authzs = new(StringComparer.Ordinal);
    private readonly Dictionary<string, ChallengeRecord> _challenges = new(StringComparer.Ordinal);
    private readonly Dictionary<string, CertRecord> _certs = new(StringComparer.Ordinal);
    private readonly HashSet<string> _revoked = new(StringComparer.Ordinal);
    // Like a CA's validation client: no proxy, no redirects (a redirect away from the challenge path is a failure here).
    private readonly HttpClient _validationHttp = new(new SocketsHttpHandler { AllowAutoRedirect = false, UseProxy = false });
    private int _nextId;
    private Task? _loop;

    /// <summary>Creates the CA and binds its loopback listener to a free port; call <see cref="Start"/> to begin answering.</summary>
    public FakeAcmeServer()
    {
        // The listener is started here (not in Start) so a port race with a parallel test class is resolved before the URLs are handed out.
        BaseUri = FreePort.StartOnFreePort(_listener);
        DirectoryUrl = new Uri(BaseUri, "directory");
        EabHmacKey = RandomNumberGenerator.GetBytes(32);
        Ca = new TestCa("Fake Root CA");
        AlternateCa = new TestCa("Fake Alternate Root CA");
    }

    /// <summary>Base URL of the CA (<c>http://127.0.0.1:{port}/</c>).</summary>
    public Uri BaseUri { get; }

    /// <summary>The ACME directory URL to hand to clients.</summary>
    public Uri DirectoryUrl { get; }

    /// <summary>The root that signs the default chain.</summary>
    public TestCa Ca { get; }

    /// <summary>The root of the alternate chain (<c>Link: rel="alternate"</c>).</summary>
    public TestCa AlternateCa { get; }

    // ---- configuration -------------------------------------------------------------------------------------------

    /// <summary>Require External Account Binding on newAccount, verified against <see cref="EabKid"/> and <see cref="EabHmacKey"/>.</summary>
    public bool RequireEab { get; set; }

    /// <summary>The EAB key id the CA accepts.</summary>
    public string EabKid { get; set; } = "eab-kid-1";

    /// <summary>The EAB HMAC key the CA verifies with (random per instance).</summary>
    public byte[] EabHmacKey { get; set; }

    /// <summary><see cref="EabHmacKey"/> in base64url, the way a CA dashboard hands it out.</summary>
    public string EabHmacKeyBase64Url => System.Buffers.Text.Base64Url.EncodeToString(EabHmacKey);

    /// <summary>Respond with badNonce to this many POSTs (each still consumes the nonce and issues a fresh one).</summary>
    public int BadNonceFailuresRemaining { get; set; }

    /// <summary>Respond 429 rateLimited (with Retry-After) to this many requests.</summary>
    public int RateLimitRemaining { get; set; }

    /// <summary>Keep authorizations pending for this many polls after a challenge response (each poll carries Retry-After).</summary>
    public int PendingAuthzPolls { get; set; }

    /// <summary>Keep the order in status processing for this many polls after finalization.</summary>
    public int ProcessingOrderPolls { get; set; }

    /// <summary>Retry-After value in seconds for pending / processing responses.</summary>
    public int RetryAfterSeconds { get; set; } = 1;

    /// <summary>Send Retry-After as an HTTP-date instead of delta seconds.</summary>
    public bool RetryAfterAsHttpDate { get; set; }

    /// <summary>Force every challenge validation to fail.</summary>
    public bool FailValidation { get; set; }

    /// <summary>Answer newNonce without a <c>Replay-Nonce</c> header.</summary>
    public bool OmitNonceOnNewNonce { get; set; }

    /// <summary>Answer newAccount without a <c>Location</c> header.</summary>
    public bool OmitLocationOnNewAccount { get; set; }

    /// <summary>Answer newOrder without a <c>Location</c> header.</summary>
    public bool OmitLocationOnNewOrder { get; set; }

    /// <summary>Finalize responses carry no Location header (RFC 8555 does not require one).</summary>
    public bool OmitLocationOnFinalize { get; set; }

    /// <summary>Leave <c>revokeCert</c> out of the directory.</summary>
    public bool OmitRevokeCertFromDirectory { get; set; }

    /// <summary>Leave the <c>certificate</c> URL out of valid orders.</summary>
    public bool OmitCertificateUrlOnValidOrder { get; set; }

    /// <summary>Serve an empty certificate chain.</summary>
    public bool ReturnEmptyPemChain { get; set; }

    /// <summary>Serve a certificate chain whose PEM does not decode to a certificate.</summary>
    public bool ReturnInvalidPem { get; set; }

    /// <summary>Do not advertise the alternate chain.</summary>
    public bool OmitAlternateChainLink { get; set; }

    /// <summary>Additional raw Link header value appended to certificate responses (to exercise the client's parser).</summary>
    public string? ExtraLinkHeader { get; set; }

    /// <summary>Advertise <c>renewalInfo</c> (RFC 9773) in the directory and serve the ARI endpoint.</summary>
    public bool RenewalInfoEnabled { get; set; }

    /// <summary>ARI window relative to the certificate's validity: (fraction of lifetime for start, fraction for end). Default 0.5–0.6.</summary>
    public (double Start, double End) RenewalWindowFractions { get; set; } = (0.5, 0.6);

    /// <summary>Retry-After (seconds) sent with ARI responses; null omits the header.</summary>
    public int? RenewalInfoRetryAfterSeconds { get; set; } = 21600;

    /// <summary>Explanation URL included in ARI responses, if any.</summary>
    public string? RenewalInfoExplanationUrl { get; set; }

    /// <summary>Reject every newOrder that carries a <c>replaces</c> field with alreadyReplaced.</summary>
    public bool RejectReplaces { get; set; }

    /// <summary>The <c>replaces</c> values received on newOrder, in order (null when absent).</summary>
    public List<string?> ReplacesSeen { get; } = [];

    /// <summary>Number of ARI requests served.</summary>
    public int RenewalInfoRequests { get; private set; }

    /// <summary>ARI certificate identifier of the last certificate issued, for assertions.</summary>
    public string? LastIssuedCertificateId { get; private set; }

    /// <summary>Identifiers whose authorizations are created already valid (account has prior authorization).</summary>
    public HashSet<string> PreauthorizedIdentifiers { get; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Identifiers that newOrder rejects with rejectedIdentifier.</summary>
    public HashSet<string> RejectedIdentifiers { get; } = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Challenge types offered for DNS identifiers.</summary>
    public List<string> OfferedChallengeTypes { get; } = [AcmeChallengeTypes.Http01, AcmeChallengeTypes.Dns01, AcmeChallengeTypes.TlsAlpn01];

    /// <summary>Overrides the next response entirely (status, content type, body); consumed once.</summary>
    public (int Status, string? ContentType, string Body)? NextOverride
    {
        get => _overrides.Count > 0 && _overrides[0].PathPrefix.Length == 0 ? _overrides[0].Response : null;
        set
        {
            lock (_lock)
            {
                _overrides.RemoveAll(o => o.PathPrefix.Length == 0);
                if (value is { } v)
                {
                    _overrides.Add((string.Empty, v));
                }
            }
        }
    }

    /// <summary>Overrides the response of the next request whose path starts with <paramref name="pathPrefix"/>; consumed once.</summary>
    public void AddOverride(string pathPrefix, int status, string? contentType, string body)
    {
        lock (_lock)
        {
            _overrides.Add((pathPrefix, (status, contentType, body)));
        }
    }

    /// <summary>Clock used for issued certificate validity and HTTP-date Retry-After values.</summary>
    public Func<DateTimeOffset> Clock { get; set; } = static () => DateTimeOffset.UtcNow;

    private readonly List<(string PathPrefix, (int Status, string? ContentType, string Body) Response)> _overrides = [];

    /// <summary>Base URL of the http-01 responder to fetch key authorizations from (the CA's HTTP validation).</summary>
    public Uri? Http01BaseUri { get; set; }

    /// <summary>Alternative direct http-01 lookup: (domain, token) → key authorization.</summary>
    public Func<string, string, string?>? Http01Lookup { get; set; }

    /// <summary><c>dns-01</c> validation: returns the TXT values of a record name, for example <see cref="InMemoryDns01Provider.Lookup"/>. Real DNS is never queried.</summary>
    public Func<string, IReadOnlyList<string>>? DnsTxtLookup { get; set; }

    /// <summary>Callback <c>tls-alpn-01</c> validation: the certificate the responder would present for a domain. Takes precedence over <see cref="TlsAlpnValidationHost"/>.</summary>
    public Func<string, X509Certificate2?>? TlsAlpnLookup { get; set; }

    /// <summary>
    /// Real <c>http-01</c> validation: the host (name or IP literal) the CA connects to, standing in for the identifier's
    /// address record. <see langword="null"/> (default) disables the real mode. Ignored while <see cref="Http01Lookup"/> is set.
    /// </summary>
    public string? Http01ValidationHost { get; set; }

    /// <summary>Port for real <c>http-01</c> validation. Default 80 (tests pass the port the responder actually bound).</summary>
    public int Http01ValidationPort { get; set; } = 80;

    /// <summary>
    /// Real <c>tls-alpn-01</c> validation: the host (name or IP literal) the CA connects to. <see langword="null"/> (default)
    /// disables the real mode. Ignored while <see cref="TlsAlpnLookup"/> is set.
    /// </summary>
    public string? TlsAlpnValidationHost { get; set; }

    /// <summary>Port for real <c>tls-alpn-01</c> validation. Default 443 (tests pass the port the responder actually bound).</summary>
    public int TlsAlpnValidationPort { get; set; } = 443;

    /// <summary>Bound on one real validation (connect, handshake, request and body). Default 10 s.</summary>
    public TimeSpan ValidationTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>Every challenge validation performed, in order (not those short-circuited by <see cref="FailValidation"/>).</summary>
    public List<FakeAcmeValidation> ValidationLog { get; } = [];

    /// <summary>Validity of issued certificates when the order requests no <c>notAfter</c>. Default 90 days.</summary>
    public TimeSpan CertificateLifetime { get; set; } = TimeSpan.FromDays(90);

    /// <summary>How far <c>notBefore</c> is backdated when the order requests none. Default 5 minutes.</summary>
    public TimeSpan CertificateBackdate { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>Answer this many requests with a bare <c>503 Service Unavailable</c> (no problem document), as during a CA outage.</summary>
    public int UnavailableRequestsRemaining { get; set; }

    /// <summary>Number of certificates issued (finalized orders).</summary>
    public int IssuedCount
    {
        get { lock (_lock) { return _certs.Count; } }
    }

    // ---- observability -------------------------------------------------------------------------------------------

    /// <summary>Every request as <c>METHOD /path</c>, in arrival order.</summary>
    public List<string> RequestLog { get; } = [];

    /// <summary><c>User-Agent</c> of the last request.</summary>
    public string? LastUserAgent { get; private set; }

    /// <summary><c>Accept</c> header of the last request.</summary>
    public string? LastAccept { get; private set; }

    /// <summary><c>Content-Type</c> of the last request.</summary>
    public string? LastContentType { get; private set; }

    /// <summary>Number of newNonce requests.</summary>
    public int NewNonceRequests { get; private set; }

    /// <summary>The key authorization of every successful validation.</summary>
    public List<string> ValidatedKeyAuthorizations { get; } = [];

    /// <summary>Number of accounts created.</summary>
    public int AccountCount
    {
        get { lock (_lock) { return _accounts.Count; } }
    }

    /// <summary>Number of certificates revoked.</summary>
    public int RevokedCount
    {
        get { lock (_lock) { return _revoked.Count; } }
    }

    /// <summary>The status of the account at <paramref name="accountUrl"/>, or <see langword="null"/> when there is none.</summary>
    public string? GetAccountStatus(Uri accountUrl)
    {
        lock (_lock)
        {
            foreach (AccountRecord a in _accounts.Values)
            {
                if (a.Url == accountUrl)
                {
                    return a.Status;
                }
            }

            return null;
        }
    }

    /// <summary>The contacts of the account at <paramref name="accountUrl"/>, or <see langword="null"/> when there is none.</summary>
    public IReadOnlyList<string>? GetAccountContacts(Uri accountUrl)
    {
        lock (_lock)
        {
            foreach (AccountRecord a in _accounts.Values)
            {
                if (a.Url == accountUrl)
                {
                    return a.Contacts;
                }
            }

            return null;
        }
    }

    /// <summary>Starts answering requests.</summary>
    public void Start()
    {
        _loop = Task.Run(LoopAsync);
    }

    /// <summary>Stops the listener and releases the CA keys.</summary>
    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        _listener.Stop();
        _listener.Close();
        if (_loop is not null)
        {
            await _loop;
        }

        _validationHttp.Dispose();
        Ca.Dispose();
        AlternateCa.Dispose();
    }

    // ---- request loop ----------------------------------------------------------------------------------------------

    private async Task LoopAsync()
    {
        while (!_cts.IsCancellationRequested)
        {
            HttpListenerContext ctx;
            try
            {
                ctx = await _listener.GetContextAsync();
            }
            catch (Exception) when (_cts.IsCancellationRequested)
            {
                return;
            }
            catch (HttpListenerException)
            {
                return;
            }
            catch (ObjectDisposedException)
            {
                return;
            }

            _ = Task.Run(() => HandleSafeAsync(ctx));
        }
    }

    private async Task HandleSafeAsync(HttpListenerContext ctx)
    {
        try
        {
            await HandleAsync(ctx);
        }
        catch (Exception e)
        {
            try
            {
                await WriteProblemAsync(ctx.Response, 500, AcmeErrorTypes.ServerInternal, e.ToString());
            }
            catch (Exception)
            {
                // Client went away.
            }
        }
    }

    private async Task HandleAsync(HttpListenerContext ctx)
    {
        HttpListenerRequest req = ctx.Request;
        HttpListenerResponse res = ctx.Response;
        string path = req.Url!.AbsolutePath;
        byte[] body;
        using (MemoryStream ms = new())
        {
            await req.InputStream.CopyToAsync(ms);
            body = ms.ToArray();
        }

        (int Status, string? ContentType, string Body)? overrideResponse = null;
        bool rateLimited = false;
        bool unavailable = false;
        lock (_lock)
        {
            RequestLog.Add(req.HttpMethod + " " + path);
            LastUserAgent = req.UserAgent;
            LastAccept = req.Headers["Accept"];
            LastContentType = req.ContentType;
            int overrideIndex = _overrides.FindIndex(o => path.StartsWith(o.PathPrefix, StringComparison.Ordinal));
            if (overrideIndex >= 0)
            {
                overrideResponse = _overrides[overrideIndex].Response;
                _overrides.RemoveAt(overrideIndex);
            }
            else if (UnavailableRequestsRemaining > 0)
            {
                UnavailableRequestsRemaining--;
                unavailable = true;
            }
            else if (RateLimitRemaining > 0)
            {
                RateLimitRemaining--;
                rateLimited = true;
            }
        }

        if (unavailable)
        {
            await WriteRawAsync(res, 503, "text/html", Encoding.ASCII.GetBytes("<html><body>503 Service Unavailable</body></html>"), addNonce: false);
            return;
        }

        if (overrideResponse is { } ov)
        {
            await WriteRawAsync(res, ov.Status, ov.ContentType, Encoding.UTF8.GetBytes(ov.Body));
            return;
        }

        if (rateLimited)
        {
            res.Headers["Retry-After"] = "3";
            await WriteProblemAsync(res, 429, AcmeErrorTypes.RateLimited, "Too many requests, slow down.");
            return;
        }

        if (path == "/directory" && req.HttpMethod == "GET")
        {
            await WriteJsonAsync(res, 200, BuildDirectory());
            return;
        }

        if (path == "/new-nonce")
        {
            lock (_lock)
            {
                NewNonceRequests++;
            }

            await WriteRawAsync(res, req.HttpMethod == "HEAD" ? 200 : 204, null, [], addNonce: !OmitNonceOnNewNonce);
            return;
        }

        if (path.StartsWith("/renewal-info/", StringComparison.Ordinal) && req.HttpMethod == "GET")
        {
            await RenewalInfoAsync(res, path[14..]);
            return;
        }

        if (req.HttpMethod != "POST")
        {
            await WriteProblemAsync(res, 405, AcmeErrorTypes.Malformed, "Method not allowed.");
            return;
        }

        bool allowJwk = path is "/new-account" or "/revoke-cert";
        Problem? problem = VerifyJws(req, body, allowJwk, out JwsResult jws);
        if (problem is not null)
        {
            await WriteProblemAsync(res, problem.Status, problem.Type, problem.Detail, problem.Subproblems);
            return;
        }

        (int status, JsonNode? node, string? location, string? contentType, byte[]? raw, List<string>? links, string? retryAfter) result = path switch
        {
            "/new-account" => NewAccount(jws),
            "/new-order" => NewOrder(jws),
            "/revoke-cert" => Revoke(jws),
            _ when path.StartsWith("/acct/", StringComparison.Ordinal) => Account(jws, path[6..]),
            _ when path.StartsWith("/order/", StringComparison.Ordinal) && path.EndsWith("/finalize", StringComparison.Ordinal) => Finalize(jws, path[7..^9]),
            _ when path.StartsWith("/order/", StringComparison.Ordinal) => GetOrder(jws, path[7..]),
            _ when path.StartsWith("/authz/", StringComparison.Ordinal) => GetAuthz(jws, path[7..]),
            _ when path.StartsWith("/chall/", StringComparison.Ordinal) => await ChallengeAsync(jws, path[7..]),
            _ when path.StartsWith("/cert/", StringComparison.Ordinal) => Certificate(jws, path[6..]),
            _ => (404, ProblemNode(AcmeErrorTypes.Malformed, "No such resource."), null, "application/problem+json", null, null, null),
        };

        if (result.retryAfter is not null)
        {
            res.Headers["Retry-After"] = result.retryAfter;
        }

        if (result.location is not null)
        {
            res.Headers["Location"] = result.location;
        }

        if (result.links is not null)
        {
            foreach (string link in result.links)
            {
                res.Headers.Add("Link", link);
            }
        }

        if (result.raw is not null)
        {
            await WriteRawAsync(res, result.status, result.contentType, result.raw);
        }
        else
        {
            await WriteJsonAsync(res, result.status, result.node, result.contentType ?? "application/json");
        }
    }

    // ---- JWS verification ------------------------------------------------------------------------------------------

    private sealed record Problem(int Status, string Type, string Detail, JsonArray? Subproblems = null);

    private sealed class JwsResult
    {
        public AccountRecord? Account;
        public AcmeAccountKey Key = null!;
        public string Thumbprint = null!;
        public JsonObject Header = null!;
        public string PayloadText = string.Empty;
        public JsonObject? Payload;
        public bool IsPostAsGet;
        public string RequestUrl = string.Empty;
    }

    private Problem? VerifyJws(HttpListenerRequest req, byte[] body, bool allowJwk, out JwsResult result)
    {
        result = new JwsResult { RequestUrl = req.Url!.ToString() };
        if (req.ContentType is null || !req.ContentType.StartsWith("application/jose+json", StringComparison.OrdinalIgnoreCase))
        {
            return new Problem(415, AcmeErrorTypes.Malformed, "Content-Type must be application/jose+json.");
        }

        JsonObject envelope;
        try
        {
            envelope = JsonNode.Parse(body) as JsonObject ?? throw new JsonException();
        }
        catch (JsonException)
        {
            return new Problem(400, AcmeErrorTypes.Malformed, "Request body is not a JSON object.");
        }

        string? protectedB64 = envelope["protected"]?.GetValue<string>();
        string? payloadB64 = envelope["payload"]?.GetValue<string>();
        string? signatureB64 = envelope["signature"]?.GetValue<string>();
        if (protectedB64 is null || payloadB64 is null || signatureB64 is null)
        {
            return new Problem(400, AcmeErrorTypes.Malformed, "JWS must have protected, payload and signature.");
        }

        JsonObject header;
        byte[] signature;
        try
        {
            header = JsonNode.Parse(System.Buffers.Text.Base64Url.DecodeFromChars(protectedB64)) as JsonObject ?? throw new JsonException();
            signature = System.Buffers.Text.Base64Url.DecodeFromChars(signatureB64);
        }
        catch (Exception e) when (e is JsonException or FormatException)
        {
            return new Problem(400, AcmeErrorTypes.Malformed, "JWS protected header or signature is not decodable.");
        }

        result.Header = header;
        string? alg = header["alg"]?.GetValue<string>();
        string? nonce = header["nonce"]?.GetValue<string>();
        string? url = header["url"]?.GetValue<string>();
        JsonObject? jwk = header["jwk"] as JsonObject;
        string? kid = header["kid"]?.GetValue<string>();

        if (alg is not ("ES256" or "RS256"))
        {
            return new Problem(400, AcmeErrorTypes.BadSignatureAlgorithm, "Unsupported alg '" + alg + "'.");
        }

        if (url != result.RequestUrl)
        {
            return new Problem(400, AcmeErrorTypes.Malformed, "JWS url '" + url + "' does not match request URL '" + result.RequestUrl + "'.");
        }

        if ((jwk is null) == (kid is null))
        {
            return new Problem(400, AcmeErrorTypes.Malformed, "Exactly one of jwk and kid must be present.");
        }

        lock (_lock)
        {
            if (nonce is null || !_validNonces.Remove(nonce))
            {
                return new Problem(400, AcmeErrorTypes.BadNonce, "Nonce is missing, unknown or already used.");
            }

            if (BadNonceFailuresRemaining > 0)
            {
                BadNonceFailuresRemaining--;
                return new Problem(400, AcmeErrorTypes.BadNonce, "Simulated stale nonce.");
            }
        }

        if (jwk is not null)
        {
            if (!allowJwk)
            {
                return new Problem(400, AcmeErrorTypes.Malformed, "jwk is only allowed for newAccount and revokeCert.");
            }

            Jwk? parsed = ParseJwk(jwk);
            try
            {
                result.Key = AcmeAccountKey.FromJwk(parsed ?? throw new ArgumentException("jwk has no kty."));
            }
            catch (ArgumentException e)
            {
                return new Problem(400, AcmeErrorTypes.BadPublicKey, e.Message);
            }

            result.Thumbprint = result.Key.Thumbprint;
            lock (_lock)
            {
                foreach (AccountRecord a in _accounts.Values)
                {
                    if (a.Thumbprint == result.Thumbprint)
                    {
                        result.Account = a;
                    }
                }
            }
        }
        else
        {
            AccountRecord? account = null;
            lock (_lock)
            {
                foreach (AccountRecord a in _accounts.Values)
                {
                    if (a.Url.ToString() == kid)
                    {
                        account = a;
                    }
                }
            }

            if (account is null)
            {
                return new Problem(400, AcmeErrorTypes.AccountDoesNotExist, "No account for kid '" + kid + "'.");
            }

            if (account.Status != AcmeAccountStatus.Valid)
            {
                return new Problem(401, AcmeErrorTypes.Unauthorized, "Account is " + account.Status + ".");
            }

            result.Account = account;
            result.Key = account.Key;
            result.Thumbprint = account.Thumbprint;
        }

        if ((alg == "ES256") != (result.Key.Algorithm == AcmeKeyAlgorithm.ES256))
        {
            return new Problem(400, AcmeErrorTypes.BadSignatureAlgorithm, "alg does not match the key type.");
        }

        byte[] input = Encoding.ASCII.GetBytes(protectedB64 + "." + payloadB64);
        if (!result.Key.Verify(input, signature))
        {
            return new Problem(400, AcmeErrorTypes.Malformed, "JWS signature verification failed.");
        }

        if (payloadB64.Length == 0)
        {
            result.IsPostAsGet = true;
            return null;
        }

        try
        {
            result.PayloadText = Encoding.UTF8.GetString(System.Buffers.Text.Base64Url.DecodeFromChars(payloadB64));
            result.Payload = JsonNode.Parse(result.PayloadText) as JsonObject;
        }
        catch (Exception e) when (e is JsonException or FormatException)
        {
            return new Problem(400, AcmeErrorTypes.Malformed, "Payload is not a JSON object.");
        }

        return result.Payload is null ? new Problem(400, AcmeErrorTypes.Malformed, "Payload is not a JSON object.") : null;
    }

    // ---- handlers --------------------------------------------------------------------------------------------------

    private (int, JsonNode?, string?, string?, byte[]?, List<string>?, string?) NewAccount(JwsResult jws)
    {
        JsonObject payload = jws.Payload ?? [];
        bool onlyExisting = payload["onlyReturnExisting"]?.GetValue<bool>() == true;
        lock (_lock)
        {
            if (jws.Account is { } existing)
            {
                return (200, AccountNode(existing), OmitLocationOnNewAccount ? null : existing.Url.ToString(), null, null, null, null);
            }

            if (onlyExisting)
            {
                return Fail(400, AcmeErrorTypes.AccountDoesNotExist, "No account exists for this key.");
            }

            if (payload["termsOfServiceAgreed"]?.GetValue<bool>() != true)
            {
                return Fail(400, AcmeErrorTypes.Malformed, "Terms of service must be agreed to.");
            }

            if (RequireEab)
            {
                if (payload["externalAccountBinding"] is not JsonObject eab)
                {
                    return Fail(400, AcmeErrorTypes.ExternalAccountRequired, "This CA requires External Account Binding.");
                }

                Problem? eabProblem = VerifyEab(eab, jws);
                if (eabProblem is not null)
                {
                    return Fail(eabProblem.Status, eabProblem.Type, eabProblem.Detail);
                }
            }

            List<string> contacts = [];
            if (payload["contact"] is JsonArray arr)
            {
                foreach (JsonNode? c in arr)
                {
                    string value = c!.GetValue<string>();
                    if (!value.StartsWith("mailto:", StringComparison.Ordinal))
                    {
                        return Fail(400, AcmeErrorTypes.UnsupportedContact, "Only mailto: contacts are supported.");
                    }

                    contacts.Add(value);
                }
            }

            string id = NextId("acct");
            AccountRecord account = new()
            {
                Id = id,
                Url = new Uri(BaseUri, "acct/" + id),
                Key = jws.Key,
                Thumbprint = jws.Thumbprint,
                Contacts = contacts,
            };
            _accounts[id] = account;
            return (201, AccountNode(account), OmitLocationOnNewAccount ? null : account.Url.ToString(), null, null, null, null);
        }
    }

    private Problem? VerifyEab(JsonObject eab, JwsResult outer)
    {
        string? protectedB64 = eab["protected"]?.GetValue<string>();
        string? payloadB64 = eab["payload"]?.GetValue<string>();
        string? signatureB64 = eab["signature"]?.GetValue<string>();
        if (protectedB64 is null || payloadB64 is null || signatureB64 is null)
        {
            return new Problem(400, AcmeErrorTypes.Malformed, "EAB JWS is incomplete.");
        }

        JsonObject header = (JsonObject)JsonNode.Parse(System.Buffers.Text.Base64Url.DecodeFromChars(protectedB64))!;
        if (header["alg"]?.GetValue<string>() != "HS256")
        {
            return new Problem(400, AcmeErrorTypes.Malformed, "EAB alg must be HS256.");
        }

        if (header["kid"]?.GetValue<string>() != EabKid)
        {
            return new Problem(401, AcmeErrorTypes.Unauthorized, "Unknown EAB key id.");
        }

        if (header["url"]?.GetValue<string>() != outer.RequestUrl)
        {
            return new Problem(400, AcmeErrorTypes.Malformed, "EAB url mismatch.");
        }

        if (header["nonce"] is not null)
        {
            return new Problem(400, AcmeErrorTypes.Malformed, "EAB must not carry a nonce.");
        }

        byte[] expected = HMACSHA256.HashData(EabHmacKey, Encoding.ASCII.GetBytes(protectedB64 + "." + payloadB64));
        if (!CryptographicOperations.FixedTimeEquals(expected, System.Buffers.Text.Base64Url.DecodeFromChars(signatureB64)))
        {
            return new Problem(401, AcmeErrorTypes.Unauthorized, "EAB HMAC verification failed.");
        }

        Jwk? innerJwk = ParseJwk(JsonNode.Parse(System.Buffers.Text.Base64Url.DecodeFromChars(payloadB64)) as JsonObject);
        using AcmeAccountKey innerKey = AcmeAccountKey.FromJwk(innerJwk!);
        return innerKey.Thumbprint == outer.Thumbprint ? null : new Problem(400, AcmeErrorTypes.Malformed, "EAB payload JWK does not match the account key.");
    }

    private (int, JsonNode?, string?, string?, byte[]?, List<string>?, string?) Account(JwsResult jws, string id)
    {
        lock (_lock)
        {
            if (!_accounts.TryGetValue(id, out AccountRecord? account) || !ReferenceEquals(account, jws.Account))
            {
                return Fail(403, AcmeErrorTypes.Unauthorized, "Account URL does not belong to the signing account.");
            }

            if (!jws.IsPostAsGet)
            {
                JsonObject payload = jws.Payload!;
                if (payload["status"]?.GetValue<string>() == AcmeAccountStatus.Deactivated)
                {
                    account.Status = AcmeAccountStatus.Deactivated;
                }

                if (payload["contact"] is JsonArray arr)
                {
                    account.Contacts = [.. arr.Select(c => c!.GetValue<string>())];
                }
            }

            return (200, AccountNode(account), null, null, null, null, null);
        }
    }

    private (int, JsonNode?, string?, string?, byte[]?, List<string>?, string?) NewOrder(JwsResult jws)
    {
        if (jws.IsPostAsGet)
        {
            return Fail(400, AcmeErrorTypes.Malformed, "newOrder requires a payload.");
        }

        JsonObject payload = jws.Payload!;
        List<AcmeIdentifier> identifiers = [];
        JsonArray subproblems = [];
        if (payload["identifiers"] is JsonArray arr)
        {
            foreach (JsonNode? n in arr)
            {
                string type = n!["type"]!.GetValue<string>();
                string value = n["value"]!.GetValue<string>();
                if (type is not ("dns" or "ip"))
                {
                    subproblems.Add((JsonNode)ProblemNode(AcmeErrorTypes.UnsupportedIdentifier, "Unsupported type " + type, new AcmeIdentifier(type, value)));
                }
                else if (RejectedIdentifiers.Contains(value))
                {
                    subproblems.Add((JsonNode)ProblemNode(AcmeErrorTypes.RejectedIdentifier, "Policy forbids " + value, new AcmeIdentifier(type, value)));
                }

                identifiers.Add(new AcmeIdentifier(type, value));
            }
        }

        if (identifiers.Count == 0)
        {
            return Fail(400, AcmeErrorTypes.Malformed, "No identifiers.");
        }

        if (subproblems.Count > 0)
        {
            return (400, ProblemNode(AcmeErrorTypes.Compound, "Some identifiers were rejected.", null, subproblems), null, "application/problem+json", null, null, null);
        }

        string? replaces = payload["replaces"]?.GetValue<string>();
        lock (_lock)
        {
            ReplacesSeen.Add(replaces);
            if (replaces is not null)
            {
                CertRecord? replaced = _certs.Values.FirstOrDefault(c => c.AriId == replaces);
                if (replaced is null)
                {
                    return Fail(400, AcmeErrorTypes.Malformed, "replaces names an unknown certificate.");
                }

                if (RejectReplaces || replaced.Replaced)
                {
                    return Fail(409, AcmeErrorTypes.AlreadyReplaced, "Certificate " + replaces + " has already been replaced.");
                }

                replaced.Replaced = true;
            }

            string id = NextId("order");
            OrderRecord order = new()
            {
                Id = id,
                Url = new Uri(BaseUri, "order/" + id),
                Identifiers = identifiers,
                Account = jws.Account!,
                Expires = DateTimeOffset.UtcNow.AddDays(7),
                NotBefore = ParseDate(payload["notBefore"]),
                NotAfter = ParseDate(payload["notAfter"]),
            };
            foreach (AcmeIdentifier identifier in identifiers)
            {
                bool wildcard = identifier.Value.StartsWith("*.", StringComparison.Ordinal);
                string authzId = NextId("authz");
                AuthzRecord authz = new()
                {
                    Id = authzId,
                    Url = new Uri(BaseUri, "authz/" + authzId),
                    Identifier = wildcard ? new AcmeIdentifier(identifier.Type, identifier.Value[2..]) : identifier,
                    Wildcard = wildcard,
                    Order = order,
                    Status = PreauthorizedIdentifiers.Contains(identifier.Value) ? AcmeAuthorizationStatus.Valid : AcmeAuthorizationStatus.Pending,
                };
                IEnumerable<string> types = wildcard ? [AcmeChallengeTypes.Dns01]
                    : identifier.IsIp ? OfferedChallengeTypes.Where(t => t != AcmeChallengeTypes.Dns01)
                    : OfferedChallengeTypes;
                foreach (string type in types)
                {
                    string challId = NextId("chall");
                    ChallengeRecord chall = new()
                    {
                        Id = challId,
                        Url = new Uri(BaseUri, "chall/" + challId),
                        Type = type,
                        Token = System.Buffers.Text.Base64Url.EncodeToString(RandomNumberGenerator.GetBytes(16)),
                        Authz = authz,
                    };
                    authz.Challenges.Add(chall);
                    _challenges[challId] = chall;
                }

                order.Authzs.Add(authz);
                _authzs[authzId] = authz;
            }

            order.Status = order.Authzs.All(a => a.Status == AcmeAuthorizationStatus.Valid) ? AcmeOrderStatus.Ready : AcmeOrderStatus.Pending;
            _orders[id] = order;
            return (201, OrderNode(order), OmitLocationOnNewOrder ? null : order.Url.ToString(), null, null, null, null);
        }
    }

    private (int, JsonNode?, string?, string?, byte[]?, List<string>?, string?) GetOrder(JwsResult jws, string id)
    {
        lock (_lock)
        {
            if (!_orders.TryGetValue(id, out OrderRecord? order))
            {
                return Fail(404, AcmeErrorTypes.Malformed, "No such order.");
            }

            if (!ReferenceEquals(order.Account, jws.Account))
            {
                return Fail(403, AcmeErrorTypes.Unauthorized, "Order belongs to another account.");
            }

            string? retryAfter = null;
            if (order.Status == AcmeOrderStatus.Processing)
            {
                if (order.ProcessingPollsRemaining > 0)
                {
                    order.ProcessingPollsRemaining--;
                    retryAfter = RetryAfterValue();
                }
                else
                {
                    order.Status = AcmeOrderStatus.Valid;
                }
            }

            return (200, OrderNode(order), null, null, null, null, retryAfter);
        }
    }

    private (int, JsonNode?, string?, string?, byte[]?, List<string>?, string?) GetAuthz(JwsResult jws, string id)
    {
        lock (_lock)
        {
            if (!_authzs.TryGetValue(id, out AuthzRecord? authz))
            {
                return Fail(404, AcmeErrorTypes.Malformed, "No such authorization.");
            }

            if (!ReferenceEquals(authz.Order.Account, jws.Account))
            {
                return Fail(403, AcmeErrorTypes.Unauthorized, "Authorization belongs to another account.");
            }

            string? retryAfter = null;
            if (authz.DeferredResult is { } deferred)
            {
                if (authz.PendingPollsRemaining > 0)
                {
                    authz.PendingPollsRemaining--;
                    retryAfter = RetryAfterValue();
                }
                else
                {
                    authz.DeferredResult = null;
                    deferred();
                }
            }

            return (200, AuthzNode(authz), null, null, null, null, retryAfter);
        }
    }

    private async Task<(int, JsonNode?, string?, string?, byte[]?, List<string>?, string?)> ChallengeAsync(JwsResult jws, string id)
    {
        ChallengeRecord? chall;
        lock (_lock)
        {
            if (!_challenges.TryGetValue(id, out chall))
            {
                return Fail(404, AcmeErrorTypes.Malformed, "No such challenge.");
            }

            if (!ReferenceEquals(chall.Authz.Order.Account, jws.Account))
            {
                return Fail(403, AcmeErrorTypes.Unauthorized, "Challenge belongs to another account.");
            }

            if (jws.IsPostAsGet)
            {
                return (200, ChallengeNode(chall), null, null, null, null, null);
            }

            if (jws.PayloadText.Trim() != "{}")
            {
                return Fail(400, AcmeErrorTypes.Malformed, "Challenge response payload must be {}.");
            }

            if (chall.Status != AcmeChallengeStatus.Pending)
            {
                return Fail(400, AcmeErrorTypes.Malformed, "Challenge is " + chall.Status + ".");
            }

            chall.Status = AcmeChallengeStatus.Processing;
        }

        string keyAuth = chall.Token + "." + jws.Thumbprint;
        string? failure = FailValidation ? "Simulated validation failure." : await ValidateAsync(chall, keyAuth);

        lock (_lock)
        {
            AuthzRecord authz = chall.Authz;
            void Apply()
            {
                if (failure is null)
                {
                    chall.Status = AcmeChallengeStatus.Valid;
                    chall.Validated = DateTimeOffset.UtcNow;
                    authz.Status = AcmeAuthorizationStatus.Valid;
                    ValidatedKeyAuthorizations.Add(keyAuth);
                    if (authz.Order.Authzs.All(a => a.Status == AcmeAuthorizationStatus.Valid))
                    {
                        authz.Order.Status = AcmeOrderStatus.Ready;
                    }
                }
                else
                {
                    chall.Status = AcmeChallengeStatus.Invalid;
                    chall.Error = ProblemNode(AcmeErrorTypes.IncorrectResponse, failure);
                    authz.Status = AcmeAuthorizationStatus.Invalid;
                    authz.Order.Status = AcmeOrderStatus.Invalid;
                    authz.Order.Error = ProblemNode(AcmeErrorTypes.IncorrectResponse, failure, authz.Identifier);
                }
            }

            if (PendingAuthzPolls > 0)
            {
                authz.PendingPollsRemaining = PendingAuthzPolls;
                authz.DeferredResult = Apply;
            }
            else
            {
                Apply();
            }

            return (200, ChallengeNode(chall), null, null, null, null, null);
        }
    }

    private const string AcmeTls1 = "acme-tls/1";

    private const string AcmeIdentifierOid = "1.3.6.1.5.5.7.1.31";

    private const int MaxHttp01BodyBytes = 64 * 1024;

    private async Task<string?> ValidateAsync(ChallengeRecord chall, string keyAuth)
    {
        AcmeIdentifier identifier = chall.Authz.Identifier;
        string? failure = chall.Type switch
        {
            AcmeChallengeTypes.Http01 => await ValidateHttp01Async(identifier, chall.Token, keyAuth),
            AcmeChallengeTypes.Dns01 => ValidateDns01(identifier.Value, keyAuth),
            _ => await ValidateTlsAlpn01Async(identifier, keyAuth),
        };

        lock (_lock)
        {
            ValidationLog.Add(new FakeAcmeValidation(chall.Type, identifier, failure));
        }

        return failure;
    }

    private async Task<string?> ValidateHttp01Async(AcmeIdentifier identifier, string token, string keyAuth)
    {
        string domain = identifier.Value;
        string? served = null;
        if (Http01Lookup is not null)
        {
            served = Http01Lookup(domain, token);
        }
        else if (Http01ValidationHost is { } host)
        {
            return await FetchHttp01Async(identifier, host, Http01ValidationPort, token, keyAuth);
        }
        else if (Http01BaseUri is not null)
        {
            using HttpRequestMessage req = new(HttpMethod.Get, new Uri(Http01BaseUri, "/.well-known/acme-challenge/" + token));
            req.Headers.Host = domain;
            using HttpResponseMessage resp = await _validationHttp.SendAsync(req);
            if (resp.IsSuccessStatusCode)
            {
                served = (await resp.Content.ReadAsStringAsync()).Trim();
            }
        }

        return served is null ? "Fetching http://" + domain + "/.well-known/acme-challenge/" + token + " failed."
            : served != keyAuth ? "Key authorization mismatch."
            : null;
    }

    /// <summary>The real http-01 check: GET the challenge path from host:port with the identifier as Host, no redirects.</summary>
    private async Task<string?> FetchHttp01Async(AcmeIdentifier identifier, string host, int port, string token, string keyAuth)
    {
        Uri uri = new UriBuilder(Uri.UriSchemeHttp, host, port, "/.well-known/acme-challenge/" + token).Uri;
        string hostHeader = identifier.IsIp && IPAddress.Parse(identifier.Value).AddressFamily == AddressFamily.InterNetworkV6
            ? "[" + identifier.Value + "]"
            : identifier.Value;
        using CancellationTokenSource timeout = new(ValidationTimeout);
        try
        {
            using HttpRequestMessage request = new(HttpMethod.Get, uri);
            request.Headers.Host = hostHeader;
            using HttpResponseMessage response = await _validationHttp.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, timeout.Token);
            if (response.StatusCode != HttpStatusCode.OK)
            {
                return "GET " + uri + " (Host: " + hostHeader + ") answered HTTP " + ((int)response.StatusCode).ToString(CultureInfo.InvariantCulture) + ".";
            }

            byte[] body = await ReadBoundedAsync(response.Content, MaxHttp01BodyBytes, timeout.Token);

            // RFC 8555 §8.3: the body must be the key authorization; like Boulder, trailing whitespace is tolerated.
            string text = Encoding.ASCII.GetString(body).TrimEnd();
            return text == keyAuth ? null : "Key authorization mismatch at " + uri + ".";
        }
        catch (Exception e)
        {
            return "Fetching " + uri + " (Host: " + hostHeader + ") failed: " + e.Message;
        }
    }

    private static async Task<byte[]> ReadBoundedAsync(HttpContent content, int limit, CancellationToken cancellationToken)
    {
        using Stream stream = await content.ReadAsStreamAsync(cancellationToken);
        using MemoryStream buffer = new();
        byte[] chunk = new byte[4096];
        int read;
        while ((read = await stream.ReadAsync(chunk, cancellationToken)) > 0)
        {
            if (buffer.Length + read > limit)
            {
                throw new InvalidDataException("The response body exceeds " + limit.ToString(CultureInfo.InvariantCulture) + " bytes.");
            }

            buffer.Write(chunk, 0, read);
        }

        return buffer.ToArray();
    }

    private string? ValidateDns01(string domain, string keyAuth)
    {
        string name = "_acme-challenge." + domain;
        string expected = System.Buffers.Text.Base64Url.EncodeToString(SHA256.HashData(Encoding.ASCII.GetBytes(keyAuth)));
        IReadOnlyList<string> records = DnsTxtLookup?.Invoke(name) ?? [];
        return records.Contains(expected) ? null : "No TXT record " + name + " with the expected value.";
    }

    private async Task<string?> ValidateTlsAlpn01Async(AcmeIdentifier identifier, string keyAuth)
    {
        string domain = identifier.Value;
        if (TlsAlpnLookup is null && TlsAlpnValidationHost is { } host)
        {
            return await HandshakeTlsAlpn01Async(identifier, host, TlsAlpnValidationPort, keyAuth);
        }

        X509Certificate2? cert = TlsAlpnLookup?.Invoke(domain);
        if (cert is null)
        {
            return "No acme-tls/1 certificate presented for " + domain + ".";
        }

        if (!cert.MatchesHostname(domain))
        {
            return "Certificate SAN does not cover " + domain + ".";
        }

        X509Extension? ext = cert.Extensions[AcmeIdentifierOid];
        if (ext is null || !ext.Critical)
        {
            return "acmeIdentifier extension missing or not critical.";
        }

        byte[] value = new AsnReader(ext.RawData, AsnEncodingRules.DER).ReadOctetString();
        byte[] expected = SHA256.HashData(Encoding.ASCII.GetBytes(keyAuth));
        return value.AsSpan().SequenceEqual(expected) ? null : "acmeIdentifier hash mismatch.";
    }

    /// <summary>
    /// The real tls-alpn-01 check (RFC 8737 §3): TLS to host:port with SNI = identifier (reverse-DNS name for IPs, RFC 8738
    /// §6) offering only ALPN acme-tls/1, then inspect the presented certificate.
    /// </summary>
    private async Task<string?> HandshakeTlsAlpn01Async(AcmeIdentifier identifier, string host, int port, string keyAuth)
    {
        string serverName = identifier.IsIp ? ReverseDnsName(IPAddress.Parse(identifier.Value)) : identifier.Value;
        string target = host + ":" + port.ToString(CultureInfo.InvariantCulture);
        using CancellationTokenSource timeout = new(ValidationTimeout);
        try
        {
            using Socket socket = new(SocketType.Stream, ProtocolType.Tcp);
            await socket.ConnectAsync(host, port, timeout.Token);
            using SslStream ssl = new(new NetworkStream(socket, ownsSocket: false), leaveInnerStreamOpen: false);
            SslClientAuthenticationOptions options = new()
            {
                TargetHost = serverName,
                ApplicationProtocols = [new SslApplicationProtocol(AcmeTls1)],

                // The validation certificate is self-signed by design: RFC 8737 checks its content, not its chain.
                RemoteCertificateValidationCallback = static (_, _, _, _) => true,
                CertificateRevocationCheckMode = System.Security.Cryptography.X509Certificates.X509RevocationMode.NoCheck,
            };
            await ssl.AuthenticateAsClientAsync(options, timeout.Token);
            if (ssl.NegotiatedApplicationProtocol != new SslApplicationProtocol(AcmeTls1))
            {
                return "TLS server " + target + " did not negotiate acme-tls/1 for SNI " + serverName + ".";
            }

            if (ssl.RemoteCertificate is not { } remote)
            {
                return "TLS server " + target + " presented no certificate for SNI " + serverName + ".";
            }

            using X509Certificate2 presented = X509CertificateLoader.LoadCertificate(remote.GetRawCertData());
            return CheckTlsAlpnCertificate(presented, identifier, keyAuth);
        }
        catch (Exception e)
        {
            return "TLS handshake with " + target + " (SNI " + serverName + ", ALPN acme-tls/1) failed: " + e.Message;
        }
    }

    /// <summary>RFC 8737 §3: exactly one subjectAltName (the identifier) and a critical acmeIdentifier = SHA-256(keyAuthorization).</summary>
    private static string? CheckTlsAlpnCertificate(X509Certificate2 certificate, AcmeIdentifier identifier, string keyAuth)
    {
        X509Extension? sanExtension = certificate.Extensions["2.5.29.17"];
        if (sanExtension is null)
        {
            return "The acme-tls/1 certificate has no subjectAltName.";
        }

        X509SubjectAlternativeNameExtension san = new(sanExtension.RawData, sanExtension.Critical);
        List<string> dnsNames = [.. san.EnumerateDnsNames()];
        List<IPAddress> addresses = [.. san.EnumerateIPAddresses()];
        bool matches = identifier.IsIp
            ? dnsNames.Count == 0 && addresses.Count == 1 && addresses[0].Equals(IPAddress.Parse(identifier.Value))
            : addresses.Count == 0 && dnsNames.Count == 1 && string.Equals(dnsNames[0], identifier.Value, StringComparison.OrdinalIgnoreCase);
        if (!matches)
        {
            return "The acme-tls/1 certificate's subjectAltName [" + string.Join(", ", dnsNames.Concat(addresses.Select(a => a.ToString()))) + "] is not exactly " + identifier.Value + ".";
        }

        X509Extension? ext = certificate.Extensions[AcmeIdentifierOid];
        if (ext is null || !ext.Critical)
        {
            return "acmeIdentifier extension missing or not critical.";
        }

        byte[] value;
        try
        {
            AsnReader reader = new(ext.RawData, AsnEncodingRules.DER);
            value = reader.ReadOctetString();
            reader.ThrowIfNotEmpty();
        }
        catch (AsnContentException)
        {
            return "acmeIdentifier extension is not a DER OCTET STRING.";
        }

        byte[] expected = SHA256.HashData(Encoding.ASCII.GetBytes(keyAuth));
        return value.AsSpan().SequenceEqual(expected) ? null : "acmeIdentifier hash mismatch.";
    }

    /// <summary>The reverse-DNS name of an address (<c>4.3.2.1.in-addr.arpa</c>, nibble-format <c>ip6.arpa</c>), the SNI a CA sends for IP identifiers (RFC 8738 §6).</summary>
    public static string ReverseDnsName(IPAddress address)
    {
        ArgumentNullException.ThrowIfNull(address);
        byte[] bytes = address.GetAddressBytes();
        StringBuilder sb = new();
        if (address.AddressFamily == AddressFamily.InterNetwork)
        {
            for (int i = bytes.Length - 1; i >= 0; i--)
            {
                sb.Append(bytes[i].ToString(CultureInfo.InvariantCulture)).Append('.');
            }

            return sb.Append("in-addr.arpa").ToString();
        }

        const string hex = "0123456789abcdef";
        for (int i = bytes.Length - 1; i >= 0; i--)
        {
            sb.Append(hex[bytes[i] & 0xF]).Append('.').Append(hex[bytes[i] >> 4]).Append('.');
        }

        return sb.Append("ip6.arpa").ToString();
    }

    /// <summary>Reads a public JWK from a JSON object with the public <see cref="Jwk"/> model (the library's JSON context is internal).</summary>
    private static Jwk? ParseJwk(JsonObject? node)
    {
        string? kty = Text(node, "kty");
        return kty is null ? null : new Jwk
        {
            Kty = kty,
            Crv = Text(node, "crv"),
            X = Text(node, "x"),
            Y = Text(node, "y"),
            E = Text(node, "e"),
            N = Text(node, "n"),
        };

        static string? Text(JsonObject? o, string name) => o?[name] is JsonValue v && v.TryGetValue(out string? s) ? s : null;
    }

    private (int, JsonNode?, string?, string?, byte[]?, List<string>?, string?) Finalize(JwsResult jws, string id)
    {
        lock (_lock)
        {
            if (!_orders.TryGetValue(id, out OrderRecord? order))
            {
                return Fail(404, AcmeErrorTypes.Malformed, "No such order.");
            }

            if (!ReferenceEquals(order.Account, jws.Account))
            {
                return Fail(403, AcmeErrorTypes.Unauthorized, "Order belongs to another account.");
            }

            if (order.Status != AcmeOrderStatus.Ready)
            {
                return Fail(403, AcmeErrorTypes.OrderNotReady, "Order is " + order.Status + ".");
            }

            string? csrB64 = jws.Payload?["csr"]?.GetValue<string>();
            if (csrB64 is null)
            {
                return Fail(400, AcmeErrorTypes.Malformed, "Missing csr.");
            }

            CertificateRequest csr;
            try
            {
                csr = CertificateRequest.LoadSigningRequest(System.Buffers.Text.Base64Url.DecodeFromChars(csrB64), HashAlgorithmName.SHA256, CertificateRequestLoadOptions.UnsafeLoadCertificateExtensions);
            }
            catch (Exception e)
            {
                return Fail(400, AcmeErrorTypes.BadCsr, "CSR could not be parsed: " + e.Message);
            }

            HashSet<string> sans = new(StringComparer.OrdinalIgnoreCase);
            foreach (X509Extension ext in csr.CertificateExtensions)
            {
                if (ext is X509SubjectAlternativeNameExtension san)
                {
                    foreach (string dns in san.EnumerateDnsNames())
                    {
                        sans.Add("dns:" + dns);
                    }

                    foreach (IPAddress ip in san.EnumerateIPAddresses())
                    {
                        sans.Add("ip:" + ip);
                    }
                }
            }

            HashSet<string> wanted = new(order.Identifiers.Select(i => i.Type + ":" + i.Value), StringComparer.OrdinalIgnoreCase);
            if (!sans.SetEquals(wanted))
            {
                return Fail(400, AcmeErrorTypes.BadCsr, "CSR SANs [" + string.Join(", ", sans) + "] do not match order identifiers [" + string.Join(", ", wanted) + "].");
            }

            // X.509 validity has second precision: truncate so the stored values match what the certificate carries.
            DateTimeOffset now = Clock();
            DateTimeOffset notBefore = Truncate(order.NotBefore ?? now - CertificateBackdate);
            DateTimeOffset notAfter = Truncate(order.NotAfter ?? now + CertificateLifetime);
            using X509Certificate2 leaf = Ca.IssueFromCsr(csr, order.Identifiers, notBefore, notAfter);
            string certId = NextId("cert");
            string? ariId = Ca.IncludeAuthorityKeyIdentifier ? AcmeClient.GetAriCertificateId(leaf) : null;
            _certs[certId] = new CertRecord
            {
                Id = certId,
                LeafDer = leaf.RawData,
                ChainPem = leaf.ExportCertificatePem() + "\n" + Ca.Root.ExportCertificatePem() + "\n",
                AltChainPem = leaf.ExportCertificatePem() + "\n" + AlternateCa.Root.ExportCertificatePem() + "\n",
                AriId = ariId,
                NotBefore = notBefore,
                NotAfter = notAfter,
            };
            LastIssuedCertificateId = ariId;
            order.CertId = certId;
            if (ProcessingOrderPolls > 0)
            {
                order.Status = AcmeOrderStatus.Processing;
                order.ProcessingPollsRemaining = ProcessingOrderPolls;
            }
            else
            {
                order.Status = AcmeOrderStatus.Valid;
            }

            return (200, OrderNode(order), OmitLocationOnFinalize ? null : order.Url.ToString(), null, null, null, null);
        }
    }

    private (int, JsonNode?, string?, string?, byte[]?, List<string>?, string?) Certificate(JwsResult jws, string idAndSuffix)
    {
        bool alternate = idAndSuffix.EndsWith("/alt", StringComparison.Ordinal);
        string id = alternate ? idAndSuffix[..^4] : idAndSuffix;
        lock (_lock)
        {
            if (!_certs.TryGetValue(id, out CertRecord? cert))
            {
                return Fail(404, AcmeErrorTypes.Malformed, "No such certificate.");
            }

            string pem = ReturnEmptyPemChain ? string.Empty : ReturnInvalidPem ? "-----BEGIN CERTIFICATE-----\nAAAAAAAA\n-----END CERTIFICATE-----\n" : alternate ? cert.AltChainPem : cert.ChainPem;
            List<string> links = [];
            if (!alternate && !OmitAlternateChainLink)
            {
                links.Add("<" + new Uri(BaseUri, "cert/" + id + "/alt") + ">;rel=\"alternate\"");
            }

            if (ExtraLinkHeader is not null)
            {
                links.Add(ExtraLinkHeader);
            }

            return (200, null, null, "application/pem-certificate-chain", Encoding.ASCII.GetBytes(pem), links, null);
        }
    }

    private (int, JsonNode?, string?, string?, byte[]?, List<string>?, string?) Revoke(JwsResult jws)
    {
        string? certB64 = jws.Payload?["certificate"]?.GetValue<string>();
        if (certB64 is null)
        {
            return Fail(400, AcmeErrorTypes.Malformed, "Missing certificate.");
        }

        int? reason = jws.Payload!["reason"]?.GetValue<int>();
        if (reason is 7 or > 10 or < 0)
        {
            return Fail(400, AcmeErrorTypes.BadRevocationReason, "Reason " + reason + " is not allowed.");
        }

        byte[] der = System.Buffers.Text.Base64Url.DecodeFromChars(certB64);
        string key = Convert.ToHexString(SHA256.HashData(der));
        lock (_lock)
        {
            CertRecord? record = _certs.Values.FirstOrDefault(c => c.LeafDer.AsSpan().SequenceEqual(der));
            if (record is null)
            {
                return Fail(404, AcmeErrorTypes.Malformed, "Certificate was not issued by this CA.");
            }

            // RFC 8555 §7.6: a jwk-signed revocation must be signed by the certificate's own key.
            if (jws.Header?["jwk"] is not null && jws.Thumbprint != CertificateKeyThumbprint(der))
            {
                return Fail(403, AcmeErrorTypes.Unauthorized, "The jwk is not the certificate's key.");
            }

            if (!_revoked.Add(key))
            {
                return Fail(400, AcmeErrorTypes.AlreadyRevoked, "Certificate is already revoked.");
            }

            return (200, null, null, null, [], null, null);
        }
    }

    /// <summary>RFC 7638 thumbprint of the certificate's public key (to authorize jwk-signed revocations).</summary>
    private static string CertificateKeyThumbprint(byte[] der)
    {
        using X509Certificate2 cert = X509CertificateLoader.LoadCertificate(der);
        ECDsa? ecdsa = cert.GetECDsaPublicKey();
        using AcmeAccountKey key = ecdsa is not null ? new AcmeAccountKey(ecdsa) : new AcmeAccountKey(cert.GetRSAPublicKey()!);
        return key.Thumbprint;
    }

    private async Task RenewalInfoAsync(HttpListenerResponse res, string certId)
    {
        CertRecord? cert;
        lock (_lock)
        {
            RenewalInfoRequests++;
            cert = RenewalInfoEnabled ? _certs.Values.FirstOrDefault(c => c.AriId == certId) : null;
        }

        if (cert is null)
        {
            await WriteProblemAsync(res, 404, AcmeErrorTypes.Malformed, "Unknown certificate identifier.");
            return;
        }

        TimeSpan lifetime = cert.NotAfter - cert.NotBefore;
        JsonObject node = new()
        {
            ["suggestedWindow"] = new JsonObject
            {
                ["start"] = (cert.NotBefore + lifetime * RenewalWindowFractions.Start).ToString("o", CultureInfo.InvariantCulture),
                ["end"] = (cert.NotBefore + lifetime * RenewalWindowFractions.End).ToString("o", CultureInfo.InvariantCulture),
            },
        };
        if (RenewalInfoExplanationUrl is not null)
        {
            node["explanationURL"] = RenewalInfoExplanationUrl;
        }

        if (RenewalInfoRetryAfterSeconds is int ra)
        {
            res.Headers["Retry-After"] = ra.ToString(CultureInfo.InvariantCulture);
        }

        await WriteJsonAsync(res, 200, node);
    }

    // ---- JSON builders ---------------------------------------------------------------------------------------------

    private JsonObject BuildDirectory()
    {
        JsonObject dir = new()
        {
            ["newNonce"] = new Uri(BaseUri, "new-nonce").ToString(),
            ["newAccount"] = new Uri(BaseUri, "new-account").ToString(),
            ["newOrder"] = new Uri(BaseUri, "new-order").ToString(),
            ["keyChange"] = new Uri(BaseUri, "key-change").ToString(),
            ["meta"] = new JsonObject
            {
                ["termsOfService"] = new Uri(BaseUri, "terms").ToString(),
                ["website"] = "https://example.test/",
                ["caaIdentities"] = new JsonArray("fake-ca.test"),
                ["externalAccountRequired"] = RequireEab,
            },
        };
        if (!OmitRevokeCertFromDirectory)
        {
            dir["revokeCert"] = new Uri(BaseUri, "revoke-cert").ToString();
        }

        if (RenewalInfoEnabled)
        {
            dir["renewalInfo"] = new Uri(BaseUri, "renewal-info").ToString();
        }

        return dir;
    }

    private JsonObject AccountNode(AccountRecord account)
    {
        return new JsonObject
        {
            ["status"] = account.Status,
            ["contact"] = new JsonArray(account.Contacts.Select(c => (JsonNode)c).ToArray()),
            ["termsOfServiceAgreed"] = true,
            ["orders"] = new Uri(BaseUri, "acct/" + account.Id + "/orders").ToString(),
        };
    }

    private JsonObject OrderNode(OrderRecord order)
    {
        JsonObject node = new()
        {
            ["status"] = order.Status,
            ["expires"] = order.Expires.ToString("o", CultureInfo.InvariantCulture),
            ["identifiers"] = new JsonArray(order.Identifiers.Select(i => (JsonNode)new JsonObject { ["type"] = i.Type, ["value"] = i.Value }).ToArray()),
            ["authorizations"] = new JsonArray(order.Authzs.Select(a => (JsonNode)a.Url.ToString()).ToArray()),
            ["finalize"] = new Uri(BaseUri, "order/" + order.Id + "/finalize").ToString(),
        };
        if (order.NotBefore is { } nb)
        {
            node["notBefore"] = nb.ToString("o", CultureInfo.InvariantCulture);
        }

        if (order.NotAfter is { } na)
        {
            node["notAfter"] = na.ToString("o", CultureInfo.InvariantCulture);
        }

        if (order.Error is not null)
        {
            node["error"] = order.Error.DeepClone();
        }

        if (order.Status == AcmeOrderStatus.Valid && order.CertId is not null && !OmitCertificateUrlOnValidOrder)
        {
            node["certificate"] = new Uri(BaseUri, "cert/" + order.CertId).ToString();
        }

        return node;
    }

    private static JsonObject AuthzNode(AuthzRecord authz)
    {
        JsonObject node = new()
        {
            ["identifier"] = new JsonObject { ["type"] = authz.Identifier.Type, ["value"] = authz.Identifier.Value },
            ["status"] = authz.Status,
            ["expires"] = DateTimeOffset.UtcNow.AddDays(1).ToString("o", CultureInfo.InvariantCulture),
            ["challenges"] = new JsonArray(authz.Challenges.Select(c => (JsonNode)ChallengeNode(c)).ToArray()),
        };
        if (authz.Wildcard)
        {
            node["wildcard"] = true;
        }

        return node;
    }

    private static JsonObject ChallengeNode(ChallengeRecord chall)
    {
        JsonObject node = new()
        {
            ["type"] = chall.Type,
            ["url"] = chall.Url.ToString(),
            ["status"] = chall.Status,
            ["token"] = chall.Token,
        };
        if (chall.Validated is { } v)
        {
            node["validated"] = v.ToString("o", CultureInfo.InvariantCulture);
        }

        if (chall.Error is not null)
        {
            node["error"] = chall.Error.DeepClone();
        }

        return node;
    }

    private static JsonObject ProblemNode(string type, string detail, AcmeIdentifier? identifier = null, JsonArray? subproblems = null)
    {
        JsonObject node = new() { ["type"] = type, ["detail"] = detail };
        if (identifier is not null)
        {
            node["identifier"] = new JsonObject { ["type"] = identifier.Type, ["value"] = identifier.Value };
        }

        if (subproblems is not null)
        {
            node["subproblems"] = subproblems;
        }

        return node;
    }

    private static (int, JsonNode?, string?, string?, byte[]?, List<string>?, string?) Fail(int status, string type, string detail)
    {
        return (status, ProblemNode(type, detail), null, "application/problem+json", null, null, null);
    }

    private static DateTimeOffset Truncate(DateTimeOffset value) => new(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond), value.Offset);

    private static DateTimeOffset? ParseDate(JsonNode? node)
    {
        return node is null ? null : DateTimeOffset.Parse(node.GetValue<string>(), CultureInfo.InvariantCulture);
    }

    private string RetryAfterValue()
    {
        return RetryAfterAsHttpDate
            ? Clock().AddSeconds(RetryAfterSeconds).ToString("r", CultureInfo.InvariantCulture)
            : RetryAfterSeconds.ToString(CultureInfo.InvariantCulture);
    }

    private string NextId(string prefix) => prefix + "-" + Interlocked.Increment(ref _nextId).ToString(CultureInfo.InvariantCulture);

    // ---- response writers -------------------------------------------------------------------------------------------

    private string IssueNonce()
    {
        string nonce = System.Buffers.Text.Base64Url.EncodeToString(RandomNumberGenerator.GetBytes(16));
        lock (_lock)
        {
            _validNonces.Add(nonce);
        }

        return nonce;
    }

    private Task WriteJsonAsync(HttpListenerResponse res, int status, JsonNode? node, string contentType = "application/json")
    {
        return WriteRawAsync(res, status, contentType, node is null ? [] : Encoding.UTF8.GetBytes(node.ToJsonString()));
    }

    private Task WriteProblemAsync(HttpListenerResponse res, int status, string type, string detail, JsonArray? subproblems = null)
    {
        return WriteJsonAsync(res, status, ProblemNode(type, detail, null, subproblems), "application/problem+json");
    }

    private async Task WriteRawAsync(HttpListenerResponse res, int status, string? contentType, byte[] body, bool addNonce = true)
    {
        res.StatusCode = status;
        if (addNonce)
        {
            res.Headers["Replay-Nonce"] = IssueNonce();
        }

        res.Headers["Cache-Control"] = "no-store";
        res.Headers.Add("Link", "<" + DirectoryUrl + ">;rel=\"index\"");
        if (contentType is not null)
        {
            res.ContentType = contentType;
        }

        res.ContentLength64 = body.Length;
        if (body.Length > 0)
        {
            await res.OutputStream.WriteAsync(body);
        }

        res.Close();
    }

    // ---- records ----------------------------------------------------------------------------------------------------

    private sealed class AccountRecord
    {
        public required string Id;
        public required Uri Url;
        public required AcmeAccountKey Key;
        public required string Thumbprint;
        public string Status = AcmeAccountStatus.Valid;
        public List<string> Contacts = [];
    }

    private sealed class OrderRecord
    {
        public required string Id;
        public required Uri Url;
        public required List<AcmeIdentifier> Identifiers;
        public required AccountRecord Account;
        public string Status = AcmeOrderStatus.Pending;
        public List<AuthzRecord> Authzs = [];
        public DateTimeOffset Expires;
        public DateTimeOffset? NotBefore;
        public DateTimeOffset? NotAfter;
        public string? CertId;
        public JsonObject? Error;
        public int ProcessingPollsRemaining;
    }

    private sealed class AuthzRecord
    {
        public required string Id;
        public required Uri Url;
        public required AcmeIdentifier Identifier;
        public required OrderRecord Order;
        public bool Wildcard;
        public string Status = AcmeAuthorizationStatus.Pending;
        public List<ChallengeRecord> Challenges = [];
        public int PendingPollsRemaining;
        public Action? DeferredResult;
    }

    private sealed class ChallengeRecord
    {
        public required string Id;
        public required Uri Url;
        public required string Type;
        public required string Token;
        public required AuthzRecord Authz;
        public string Status = AcmeChallengeStatus.Pending;
        public DateTimeOffset? Validated;
        public JsonObject? Error;
    }

    private sealed class CertRecord
    {
        public required string Id;
        public required byte[] LeafDer;
        public required string ChainPem;
        public required string AltChainPem;
        public string? AriId;
        public DateTimeOffset NotBefore;
        public DateTimeOffset NotAfter;
        public bool Replaced;
    }
}

/// <summary>One challenge validation performed by <see cref="FakeAcmeServer"/>.</summary>
/// <param name="ChallengeType">The challenge type (<c>http-01</c>, <c>dns-01</c>, <c>tls-alpn-01</c>).</param>
/// <param name="Identifier">The identifier validated (a wildcard's base domain).</param>
/// <param name="Error">Why validation failed, or <see langword="null"/> on success.</param>
public sealed record FakeAcmeValidation(string ChallengeType, AcmeIdentifier Identifier, string? Error)
{
    /// <summary>True when the validation succeeded.</summary>
    public bool Succeeded => Error is null;
}
