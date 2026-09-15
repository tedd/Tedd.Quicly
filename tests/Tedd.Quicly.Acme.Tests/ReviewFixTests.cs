using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
using Tedd.Quicly.Acme.Challenges;
using Tedd.Quicly.Acme.Models;
using Tedd.Quicly.Acme.Tests.Fake;

namespace Tedd.Quicly.Acme.Tests;

/// <summary>
/// Tests for the second review round: secret redaction, HTTPS-only URLs with an injectable trust anchor, badNonce retry
/// edge cases, Link parsing, certificate-key revocation, stale-account recovery, bounded cleanup, the narrowed
/// <c>replaces</c> fallback, delay clamping, the ARI and minimum-interval renewal guards and the CSR subject rules.
/// </summary>
public sealed class ReviewFixTests : IAsyncDisposable
{
    private static readonly Uri StubDirectory = new("https://ca.test/directory");
    private const string StubDirectoryJson = "{\"newNonce\":\"https://ca.test/nonce\",\"newAccount\":\"https://ca.test/acct\",\"newOrder\":\"https://ca.test/order\"}";

    private readonly FakeAcmeServer _server = new();
    private readonly HttpClient _http = new();
    private readonly FastTimeProvider _clock = new();
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "quicly-acme-tests", Guid.NewGuid().ToString("N"));
    private readonly InMemoryDns01Provider _dns01 = new();

    public ReviewFixTests()
    {
        Directory.CreateDirectory(_dir);
        _server.DnsTxtLookup = _dns01.Lookup;
        _server.Start();
    }

    public async ValueTask DisposeAsync()
    {
        _http.Dispose();
        await _server.DisposeAsync();
        if (Directory.Exists(_dir))
        {
            Directory.Delete(_dir, true);
        }
    }

    private AcmeCertificateManager NewManager(
        string store = "account.json",
        IDns01Provider? dns = null,
        TimeSpan? cleanupTimeout = null,
        TimeSpan? propagationDelay = null,
        string domain = "fix2.example.test")
    {
        return new AcmeCertificateManager(_http, new AcmeCertificateManagerOptions
        {
            DirectoryUrl = _server.DirectoryUrl,
            AccountStore = new AcmeAccountStore(Path.Combine(_dir, store)),
            Identifiers = [AcmeIdentifier.Dns(domain)],
            PreferredChallengeTypes = [AcmeChallengeTypes.Dns01],
            Dns01Provider = dns ?? _dns01,
            ChallengeCleanupTimeout = cleanupTimeout ?? TimeSpan.FromSeconds(30),
            ChallengePropagationDelay = propagationDelay ?? TimeSpan.Zero,
            ClientOptions = new AcmeClientOptions { TimeProvider = _clock, MaxPollAttempts = 5 },
            Retry = new AcmeRetryOptions { MaxAttempts = 1, Jitter = 0 },
        });
    }

    // ---- secret redaction ----------------------------------------------------------------------------------------------

    [Fact]
    public void Records_RedactSecrets_ButKeepTheOtherMembers()
    {
        using AcmeAccountKey key = AcmeAccountKey.Create();
        AcmePendingOrder pending = new()
        {
            OrderUrl = new Uri("https://ca.test/order/7"),
            Identifiers = [AcmeIdentifier.Dns("a.example.test"), AcmeIdentifier.Ip("192.0.2.1")],
            CertificateKeyPem = key.ExportPem(),
            Expires = new DateTimeOffset(2026, 10, 1, 0, 0, 0, TimeSpan.Zero),
        };
        AcmeAccountState state = AcmeAccountStore.CreateState(key, new Uri("https://ca.test/acct/9"), StubDirectory) with { PendingOrder = pending };

        string text = state.ToString();
        Assert.DoesNotContain("PRIVATE KEY", text, StringComparison.Ordinal);
        Assert.Contains("PrivateKeyPem = ***", text, StringComparison.Ordinal);
        Assert.Contains("CertificateKeyPem = ***", text, StringComparison.Ordinal);
        Assert.Contains("https://ca.test/acct/9", text, StringComparison.Ordinal);
        Assert.Contains("https://ca.test/order/7", text, StringComparison.Ordinal);
        Assert.Contains("a.example.test", text, StringComparison.Ordinal);
        Assert.Contains("ES256", text, StringComparison.Ordinal);

        string eab = new ExternalAccountBinding("kid-42", "c2VjcmV0").ToString();
        Assert.Contains("kid-42", eab, StringComparison.Ordinal);
        Assert.Contains("HmacKey = ***", eab, StringComparison.Ordinal);
        Assert.DoesNotContain("c2VjcmV0", eab, StringComparison.Ordinal);
    }

    // ---- HTTPS-only URLs (RFC 8555 §6.1, ADR 0009) ----------------------------------------------------------------------

    [Fact]
    public void IsAllowedUrl_Rules()
    {
        Assert.True(AcmeClient.IsAllowedUrl(new Uri("https://ca.test/x"), false));
        Assert.True(AcmeClient.IsAllowedUrl(new Uri("http://127.0.0.1:8080/x"), false));
        Assert.True(AcmeClient.IsAllowedUrl(new Uri("http://localhost/x"), false));
        Assert.True(AcmeClient.IsAllowedUrl(new Uri("http://[::1]/x"), false));
        Assert.False(AcmeClient.IsAllowedUrl(new Uri("http://ca.test/x"), false));
        Assert.True(AcmeClient.IsAllowedUrl(new Uri("http://ca.test/x"), true));
        Assert.False(AcmeClient.IsAllowedUrl(new Uri("ftp://ca.test/x"), true));
        Assert.False(AcmeClient.IsAllowedUrl(new Uri("/relative", UriKind.Relative), true));
    }

    [Fact]
    public async Task Constructor_RejectsInsecureDirectoryUrls_UnlessLoopbackOrAllowed()
    {
        int requests = 0;
        using HttpClient http = new(new StubHandler(_ =>
        {
            requests++;
            return Json(HttpStatusCode.OK, StubDirectoryJson.Replace("https://", "http://", StringComparison.Ordinal), "n1");
        }));
        using AcmeAccountKey key = AcmeAccountKey.Create();

        Assert.Throws<ArgumentException>(() => new AcmeClient(http, new Uri("/directory", UriKind.Relative), key));
        Assert.Throws<ArgumentException>(() => new AcmeClient(http, new Uri("ftp://ca.test/directory"), key));
        Assert.Throws<ArgumentNullException>(() => new AcmeClient(http, null!, key));
        _ = new AcmeClient(http, new Uri("http://localhost/directory"), key); // loopback is fine
        Assert.Equal(0, requests);

        // A mock CA elsewhere: allowed explicitly, for the directory and for the http:// URLs it hands out.
        AcmeClient allowed = new(http, new Uri("http://ca.test/directory"), key, options: new AcmeClientOptions { AllowInsecureHttp = true });
        Assert.True(allowed.Options.AllowInsecureHttp);
        AcmeDirectory directory = await allowed.GetDirectoryAsync();
        Assert.Equal("http", directory.NewOrder.Scheme);
        Assert.False(new AcmeClientOptions().AllowInsecureHttp);
    }

    [Theory]
    [InlineData("newOrder")]
    [InlineData("revokeCert")]
    [InlineData("renewalInfo")]
    [InlineData("keyChange")]
    [InlineData("newAuthz")]
    public async Task Directory_WithAnInsecureEntry_IsRejected(string entry)
    {
        string json = "{\"newNonce\":\"https://ca.test/nonce\",\"newAccount\":\"https://ca.test/acct\",\"newOrder\":\"https://ca.test/order\","
            + "\"revokeCert\":\"https://ca.test/revoke\",\"renewalInfo\":\"https://ca.test/ari\",\"keyChange\":\"https://ca.test/kc\",\"newAuthz\":\"https://ca.test/authz\"}";
        json = json.Replace("\"" + entry + "\":\"https://", "\"" + entry + "\":\"http://", StringComparison.Ordinal);
        using HttpClient http = new(new StubHandler(_ => Json(HttpStatusCode.OK, json, "n1")));
        using AcmeAccountKey key = AcmeAccountKey.Create();
        AcmeClient client = new(http, StubDirectory, key);

        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await client.GetDirectoryAsync());
        Assert.Equal(AcmeErrorTypes.InsecureUrl, e.Type);
        Assert.Contains("http://ca.test/", e.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ServerSuppliedInsecureUrl_IsRefusedBeforeSending()
    {
        List<string> sent = [];
        using HttpClient http = new(new StubHandler(request =>
        {
            sent.Add(request.Method + " " + request.RequestUri);
            return Json(HttpStatusCode.OK, StubDirectoryJson, "n1", "n2");
        }));
        using AcmeAccountKey key = AcmeAccountKey.Create();
        AcmeClient client = new(http, StubDirectory, key, new Uri("https://ca.test/acct/1"));
        await client.GetDirectoryAsync();

        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await client.GetOrderAsync(new Uri("http://ca.test/order/1")));
        Assert.Equal(AcmeErrorTypes.InsecureUrl, e.Type);
        Assert.Equal(["GET https://ca.test/directory"], sent);
    }

    [Fact]
    public void CreateHttpHandler_ConfiguresCustomRootTrust()
    {
        Assert.Throws<ArgumentNullException>(() => AcmeClient.CreateHttpHandler(null!));
        Assert.Throws<ArgumentException>(() => AcmeClient.CreateHttpHandler([]));

        using TestCa ca = new("Trust Anchor Root");
        using SocketsHttpHandler handler = AcmeClient.CreateHttpHandler([ca.Root]);
        X509ChainPolicy policy = handler.SslOptions.CertificateChainPolicy!;
        Assert.Equal(X509ChainTrustMode.CustomRootTrust, policy.TrustMode);
        Assert.Equal(X509RevocationMode.NoCheck, policy.RevocationMode);
        Assert.Equal(ca.Root.Thumbprint, Assert.Single(policy.CustomTrustStore).Thumbprint);
    }

    [Fact]
    public async Task Https_IsValidated_AndATrustAnchorLetsTheClientReachAPrivateCa()
    {
        using TestCa ca = new("Private ACME Root");
        using ECDsa serverKey = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        CertificateRequest csr = new("CN=127.0.0.1", serverKey, HashAlgorithmName.SHA256);
        DateTimeOffset now = DateTimeOffset.UtcNow;
        using X509Certificate2 issued = ca.IssueFromCsr(csr, [AcmeIdentifier.Ip("127.0.0.1")], now.AddDays(-1), now.AddDays(1));
        using X509Certificate2 withKey = issued.CopyWithPrivateKey(serverKey);
        // Schannel needs a persisted (not ephemeral) key for a server credential: round-trip through PKCS#12.
        using X509Certificate2 serverCertificate = X509CertificateLoader.LoadPkcs12(withKey.Export(X509ContentType.Pkcs12), null);

        TcpListener listener = new(IPAddress.Loopback, 0);
        listener.Start();
        using CancellationTokenSource stop = new();
        string baseUrl = "https://127.0.0.1:" + ((IPEndPoint)listener.LocalEndpoint).Port + "/";
        string directoryJson = "{\"newNonce\":\"" + baseUrl + "nonce\",\"newAccount\":\"" + baseUrl + "acct\",\"newOrder\":\"" + baseUrl + "order\"}";
        Task serve = ServeHttpsAsync(listener, serverCertificate, directoryJson, stop.Token);
        try
        {
            using AcmeAccountKey key = AcmeAccountKey.Create();

            // Default validation: the private root is not trusted, so the directory is never parsed.
            using HttpClient untrusted = new() { Timeout = TimeSpan.FromSeconds(20) };
            await Assert.ThrowsAsync<HttpRequestException>(async () => await new AcmeClient(untrusted, new Uri(baseUrl + "directory"), key).GetDirectoryAsync());

            // With the CA's root as the trust anchor the same server is accepted.
            using HttpClient trusted = new(AcmeClient.CreateHttpHandler([ca.Root])) { Timeout = TimeSpan.FromSeconds(20) };
            AcmeClient client = new(trusted, new Uri(baseUrl + "directory"), key);
            AcmeDirectory directory = await client.GetDirectoryAsync();
            Assert.Equal(new Uri(baseUrl + "order"), directory.NewOrder);
            Assert.Equal(1, client.PooledNonceCount);
        }
        finally
        {
            stop.Cancel();
            listener.Stop();
            await serve;
        }
    }

    private static async Task ServeHttpsAsync(TcpListener listener, X509Certificate2 certificate, string body, CancellationToken cancellationToken)
    {
        List<Task> connections = [];
        while (true)
        {
            TcpClient tcp;
            try
            {
                tcp = await listener.AcceptTcpClientAsync(cancellationToken);
            }
            catch (Exception)
            {
                break;
            }

            connections.Add(Task.Run(async () =>
            {
                using (tcp)
                {
                    try
                    {
                        using SslStream ssl = new(tcp.GetStream());
                        await ssl.AuthenticateAsServerAsync(new SslServerAuthenticationOptions { ServerCertificate = certificate }, cancellationToken);
                        byte[] buffer = new byte[8192];
                        int total = 0;
                        while (!Encoding.ASCII.GetString(buffer, 0, total).Contains("\r\n\r\n", StringComparison.Ordinal))
                        {
                            int read = await ssl.ReadAsync(buffer.AsMemory(total), cancellationToken);
                            if (read == 0)
                            {
                                return;
                            }

                            total += read;
                        }

                        byte[] payload = Encoding.UTF8.GetBytes(body);
                        string head = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nReplay-Nonce: tls-nonce\r\nContent-Length: "
                            + payload.Length.ToString(System.Globalization.CultureInfo.InvariantCulture) + "\r\nConnection: close\r\n\r\n";
                        await ssl.WriteAsync(Encoding.ASCII.GetBytes(head), cancellationToken);
                        await ssl.WriteAsync(payload, cancellationToken);
                        await ssl.FlushAsync(cancellationToken);
                    }
                    catch (Exception)
                    {
                        // Handshakes the untrusting client aborts land here.
                    }
                }
            }, CancellationToken.None));
        }

        await Task.WhenAll(connections);
    }

    // ---- badNonce retry edge cases -------------------------------------------------------------------------------------

    [Fact]
    public async Task BadNonce_WithoutAFreshNonce_FallsBackToNewNonce()
    {
        List<string?> used = [];
        using HttpClient http = new(new StubHandler(request =>
        {
            if (request.Method == HttpMethod.Get)
            {
                return Json(HttpStatusCode.OK, StubDirectoryJson, "stale1");
            }

            if (request.Method == HttpMethod.Head)
            {
                return Json(HttpStatusCode.OK, "{}", "fresh");
            }

            string? nonce = ReadJwsNonce(request);
            used.Add(nonce);
            if (nonce == "fresh")
            {
                HttpResponseMessage created = Json(HttpStatusCode.Created, "{\"status\":\"valid\"}", "next");
                created.Headers.Location = new Uri("https://ca.test/acct/1");
                return created;
            }

            return Problem(AcmeErrorTypes.BadNonce); // no Replay-Nonce on this one
        }));
        using AcmeAccountKey key = AcmeAccountKey.Create();
        AcmeClient client = new(http, StubDirectory, key);

        await client.CreateAccountAsync(null, true);
        Assert.Equal(["stale1", "fresh"], used);
        Assert.Equal(1, client.PooledNonceCount); // "next"
    }

    [Fact]
    public async Task BadNonce_Twice_Fails_AndKeepsTheLastNonce()
    {
        int posts = 0;
        using HttpClient http = new(new StubHandler(request =>
        {
            if (request.Method == HttpMethod.Get)
            {
                return Json(HttpStatusCode.OK, StubDirectoryJson, "n0");
            }

            posts++;
            return Problem(AcmeErrorTypes.BadNonce, "n" + posts.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }));
        using AcmeAccountKey key = AcmeAccountKey.Create();
        AcmeClient client = new(http, StubDirectory, key);

        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await client.CreateAccountAsync(null, true));
        Assert.Equal(AcmeErrorTypes.BadNonce, e.Type);
        Assert.Equal(2, posts); // one retry only
        Assert.Equal(1, client.PooledNonceCount);
        Assert.Equal("n2", await client.GetNonceAsync());
    }

    // ---- Link header parsing -------------------------------------------------------------------------------------------

    [Fact]
    public void ParseLinkHeader_HonoursAngleBracketsAndQuotes()
    {
        List<(Uri Url, string Rel)> links = [];
        AcmeClient.ParseLinkHeader(
            "<https://ca.test/cert/1/alt,1>;rel=\"alternate\", <https://ca.test/dir>; rel=index, "
            + "<https://ca.test/cert/1/2>; title=\"x;y,z\"; rel=\"up alternate\", <relative/path>;rel=alternate",
            links);
        Assert.Equal(3, links.Count);
        Assert.Equal("https://ca.test/cert/1/alt,1", links[0].Url.AbsoluteUri);
        Assert.Equal("alternate", links[0].Rel);
        Assert.Equal("index", links[1].Rel);
        Assert.Equal("up alternate", links[2].Rel);

        links.Clear();
        AcmeClient.ParseLinkHeader("<https://a.test/>;flag;rel=first;rel=second", links);
        AcmeClient.ParseLinkHeader("<https://b.test/>; rel = \"q\\\"uoted\"", links);
        AcmeClient.ParseLinkHeader("<https://c.test/>;rel=", links);
        AcmeClient.ParseLinkHeader("<https://d.test/>;title=\"unterminated", links);
        AcmeClient.ParseLinkHeader("<https://e.test/>", links);
        AcmeClient.ParseLinkHeader("<https://f.test/> ; rel=\"spaced\" , <https://g.test/>;rel=g", links); // whitespace around separators
        Assert.Equal(["first", "q\"uoted", "", "", "", "spaced", "g"], links.Select(l => l.Rel));

        links.Clear();
        AcmeClient.ParseLinkHeader("no link here", links);
        AcmeClient.ParseLinkHeader("<https://unterminated.test/", links);
        Assert.Empty(links);
    }

    [Fact]
    public async Task DownloadCertificate_CollectsAlternates_FromMultiRelationLinks()
    {
        using TestCa ca = new("Link Root");
        using X509Certificate2 leaf = ca.IssueLeaf("link.example.test", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));
        string pem = leaf.ExportCertificatePem() + "\n" + ca.Root.ExportCertificatePem() + "\n";
        using HttpClient http = new(new StubHandler(request =>
        {
            if (request.Method == HttpMethod.Get)
            {
                return Json(HttpStatusCode.OK, StubDirectoryJson, "n1");
            }

            HttpResponseMessage response = new(HttpStatusCode.OK) { Content = new StringContent(pem, Encoding.ASCII, "application/pem-certificate-chain") };
            response.Headers.TryAddWithoutValidation("Replay-Nonce", "n2");
            response.Headers.TryAddWithoutValidation("Link", "<https://ca.test/cert/1/alt,1>;rel=\"up alternate\", <https://ca.test/directory>;rel=\"index\"");
            return response;
        }));
        using AcmeAccountKey key = AcmeAccountKey.Create();
        AcmeClient client = new(http, StubDirectory, key, new Uri("https://ca.test/acct/1"));

        using AcmeCertificateChain chain = await client.DownloadCertificateAsync(new Uri("https://ca.test/cert/1"));
        Assert.Equal("https://ca.test/cert/1/alt,1", Assert.Single(chain.AlternateChainUrls).AbsoluteUri);
    }

    // ---- revocation with the certificate key (RFC 8555 §7.6) -----------------------------------------------------------

    [Fact]
    public async Task RevokeWithCertificateKey_NeedsNoAccount_AndMustBeTheCertificateKey()
    {
        AcmeCertificateManager manager = NewManager();
        using IssuedCertificate first = await manager.OrderCertificateAsync();
        using IssuedCertificate second = await manager.OrderCertificateAsync();

        // A client with a fresh key and no account: the JWS embeds the certificate key as jwk.
        using AcmeAccountKey unrelated = AcmeAccountKey.Create();
        AcmeClient client = new(_http, _server.DirectoryUrl, unrelated);
        Assert.Null(client.AccountUrl);

        using AcmeAccountKey certificateKey = new(first.Certificate.GetECDsaPrivateKey()!);
        await client.RevokeCertificateWithKeyAsync(first.Certificate.RawData, certificateKey, AcmeRevocationReason.KeyCompromise);
        Assert.Equal(1, _server.RevokedCount);

        // Signing with a key that is not the certificate's is refused by the CA.
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await client.RevokeCertificateWithKeyAsync(second.Certificate.RawData, unrelated));
        Assert.Equal(AcmeErrorTypes.Unauthorized, e.Type);
        Assert.Equal(1, _server.RevokedCount);

        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.RevokeCertificateWithKeyAsync(null!, certificateKey));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await client.RevokeCertificateWithKeyAsync(second.Certificate.RawData, null!));
    }

    // ---- stale account recovery and ownership --------------------------------------------------------------------------

    [Fact]
    public async Task StoredAccount_ThatWasDeactivated_IsReplacedByANewAccount()
    {
        AcmeCertificateManager first = NewManager();
        AcmeClient firstClient = await first.GetClientAsync();
        Uri oldAccount = firstClient.AccountUrl!;
        await firstClient.DeactivateAccountAsync();
        Assert.Equal(AcmeAccountStatus.Deactivated, _server.GetAccountStatus(oldAccount));

        using AcmeCertificateManager second = NewManager();
        List<AcmeProgress> events = [];
        second.Progress += events.Add;
        AcmeClient client = await second.GetClientAsync();
        Assert.NotEqual(oldAccount, client.AccountUrl);
        Assert.Equal(client.AccountUrl, second.Options.AccountStore.Load()!.AccountUrl);
        Assert.Contains(events, e => e.Stage == AcmeStage.AccountReady && e.Message.Contains("not usable (" + AcmeErrorTypes.Unauthorized, StringComparison.Ordinal));
        Assert.Contains(events, e => e.Stage == AcmeStage.AccountReady && e.Message.Contains("is no longer usable; created account", StringComparison.Ordinal));

        // The new account can issue.
        using IssuedCertificate issued = await second.OrderCertificateAsync();
        Assert.True(issued.Certificate.HasPrivateKey);
    }

    [Fact]
    public async Task StoredAccount_UnknownToTheCa_OrNotValid_IsReplaced()
    {
        AcmeAccountStore store = new(Path.Combine(_dir, "account.json"));
        using (AcmeAccountKey key = AcmeAccountKey.Create())
        {
            store.Save(AcmeAccountStore.CreateState(key, new Uri(_server.BaseUri, "acct/does-not-exist"), _server.DirectoryUrl));
        }

        using AcmeCertificateManager unknown = NewManager();
        AcmeClient client = await unknown.GetClientAsync();
        Assert.NotEqual(new Uri(_server.BaseUri, "acct/does-not-exist"), client.AccountUrl);
        Assert.Equal(1, _server.AccountCount);

        // The CA reports the account as revoked (status field): replaced too.
        Uri previous = client.AccountUrl!;
        _server.AddOverride("/acct/", 200, "application/json", "{\"status\":\"revoked\"}");
        using AcmeCertificateManager revoked = NewManager();
        List<string> messages = [];
        revoked.Progress += p => messages.Add(p.Message);
        AcmeClient replacement = await revoked.GetClientAsync();
        Assert.NotEqual(previous, replacement.AccountUrl);
        Assert.Contains(messages, m => m.Contains("(status revoked)", StringComparison.Ordinal));
    }

    [Fact]
    public async Task StoredAccount_TransientVerificationFailure_Propagates_ThenReuses()
    {
        using AcmeCertificateManager creator = NewManager();
        Uri account = (await creator.GetClientAsync()).AccountUrl!;

        using AcmeCertificateManager manager = NewManager();
        _server.AddOverride("/acct/", 500, "application/problem+json", "{\"type\":\"" + AcmeErrorTypes.ServerInternal + "\",\"detail\":\"boom\"}");
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await manager.GetClientAsync());
        Assert.Equal(AcmeErrorTypes.ServerInternal, e.Type);

        AcmeClient client = await manager.GetClientAsync();
        Assert.Equal(account, client.AccountUrl);
        Assert.Equal(1, _server.AccountCount);
    }

    [Fact]
    public async Task Dispose_ReleasesTheAccountKey_AndBlocksFurtherUse()
    {
        AcmeCertificateManager manager = NewManager();
        AcmeClient client = await manager.GetClientAsync();
        manager.Dispose();
        manager.Dispose(); // idempotent
        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await manager.GetClientAsync());
        Assert.ThrowsAny<ObjectDisposedException>(() => client.AccountKey.Sign([1, 2, 3]));

        // Disposing a manager that never created a client is fine too.
        NewManager("unused.json").Dispose();
    }

    // ---- cleanup bounds, progress handlers, replaces fallback ----------------------------------------------------------

    [Fact]
    public async Task HangingCleanup_IsBounded_AndDoesNotMaskTheOutcome()
    {
        HangingRemoveDns01Provider dns = new(_dns01);
        AcmeCertificateManager manager = NewManager(dns: dns, cleanupTimeout: TimeSpan.FromMilliseconds(200));
        List<AcmeProgress> events = [];
        manager.Progress += events.Add;

        using IssuedCertificate issued = await manager.OrderCertificateAsync();
        Assert.True(issued.Certificate.HasPrivateKey);
        Assert.Equal(1, dns.RemoveCalls);
        Assert.Contains(events, e => e.Stage == AcmeStage.ChallengeCleanedUp && e.Message.Contains("timed out", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ThrowingProgressHandler_DoesNotChangeTheFlow()
    {
        AcmeCertificateManager manager = NewManager();
        int calls = 0;
        manager.Progress += _ =>
        {
            calls++;
            throw new InvalidOperationException("observer failed");
        };

        using IssuedCertificate issued = await manager.OrderCertificateAsync();
        Assert.True(calls > 5);
        Assert.Empty(_dns01.Lookup(Dns01Challenge.GetRecordName("fix2.example.test"))); // cleanup still ran
    }

    [Fact]
    public void ManagerOptions_CleanupTimeout_IsValidated()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => NewManager(cleanupTimeout: TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => NewManager(cleanupTimeout: TimeSpan.FromDays(31)));
        Assert.Equal(TimeSpan.FromSeconds(30), new AcmeCertificateManagerOptions
        {
            DirectoryUrl = StubDirectory,
            AccountStore = new AcmeAccountStore(Path.Combine(_dir, "x.json")),
            Identifiers = [AcmeIdentifier.Dns("x.test")],
        }.ChallengeCleanupTimeout);
    }

    [Fact]
    public async Task UnrelatedNewOrderErrors_AreNotRetriedWithoutReplaces()
    {
        _server.RejectedIdentifiers.Add("fix2.example.test");
        AcmeCertificateManager manager = NewManager();
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await manager.OrderCertificateAsync("bm9wZQ.AQ"));
        Assert.Equal(AcmeErrorTypes.Compound, e.Type);
        Assert.Equal(1, _server.RequestLog.Count(r => r == "POST /new-order"));
    }

    [Fact]
    public void IsReplacesRejection_OnlyForReplacesErrors()
    {
        Assert.True(AcmeCertificateManager.IsReplacesRejection(Error(AcmeErrorTypes.AlreadyReplaced, null)));
        Assert.True(AcmeCertificateManager.IsReplacesRejection(Error(AcmeErrorTypes.Malformed, "Replaces names an unknown certificate")));
        Assert.False(AcmeCertificateManager.IsReplacesRejection(Error(AcmeErrorTypes.Malformed, "bad JWS")));
        Assert.False(AcmeCertificateManager.IsReplacesRejection(Error(AcmeErrorTypes.Malformed, null)));
        Assert.False(AcmeCertificateManager.IsReplacesRejection(Error(AcmeErrorTypes.RejectedIdentifier, "replaces")));

        static AcmeException Error(string type, string? detail) => new(new AcmeProblem { Type = type, Detail = detail });
    }

    [Fact]
    public void SameIdentifiers_IsASetComparison()
    {
        AcmeIdentifier a = AcmeIdentifier.Dns("a.test");
        AcmeIdentifier b = AcmeIdentifier.Dns("b.test");
        Assert.True(AcmeCertificateManager.SameIdentifiers([a, b], [AcmeIdentifier.Dns("B.TEST"), a]));
        Assert.False(AcmeCertificateManager.SameIdentifiers([a, a], [a, b]));
        Assert.False(AcmeCertificateManager.SameIdentifiers([a, b], [a, a]));
        Assert.False(AcmeCertificateManager.SameIdentifiers([a], [a, b]));
        Assert.False(AcmeCertificateManager.SameIdentifiers([AcmeIdentifier.Ip("192.0.2.1")], [AcmeIdentifier.Dns("192.0.2.1")]));
    }

    // ---- delay clamping ----------------------------------------------------------------------------------------------

    [Fact]
    public void Timers_ClampAndRoundUpToWholeMilliseconds()
    {
        Assert.Equal(TimeSpan.Zero, AcmeTimers.Clamp(TimeSpan.FromTicks(-1)));
        Assert.Equal(TimeSpan.Zero, AcmeTimers.Clamp(TimeSpan.Zero));
        Assert.Equal(TimeSpan.FromMilliseconds(1), AcmeTimers.Clamp(TimeSpan.FromTicks(1)));
        Assert.Equal(TimeSpan.FromMilliseconds(1), AcmeTimers.Clamp(TimeSpan.FromMilliseconds(1)));
        Assert.Equal(TimeSpan.FromMilliseconds(2), AcmeTimers.Clamp(TimeSpan.FromMilliseconds(1) + TimeSpan.FromTicks(1)));
        Assert.Equal(AcmeTimers.MaxDelay, AcmeTimers.Clamp(TimeSpan.FromDays(31)));
        Assert.Equal(AcmeTimers.MaxDelay, AcmeTimers.Clamp(TimeSpan.MaxValue));
    }

    [Fact]
    public void RetryDelay_NearTimeSpanMaxValue_DoesNotOverflow()
    {
        AcmeRetryOptions options = new()
        {
            InitialDelay = TimeSpan.FromDays(1),
            MaxDelay = TimeSpan.MaxValue,
            Multiplier = 1e6,
            Jitter = 1.0,
            Random = new FixedRandom(0.99),
        };
        options.Validate();
        Assert.Equal(TimeSpan.MaxValue, options.GetDelay(10, null));
    }

    [Fact]
    public async Task HugeDelays_AreClamped_ForTimers()
    {
        // ChallengePropagationDelay beyond Task.Delay's ~49.7-day limit no longer throws.
        AcmeCertificateManager manager = NewManager(propagationDelay: TimeSpan.FromDays(100));
        using IssuedCertificate issued = await manager.OrderCertificateAsync();
        Assert.Contains(AcmeTimers.MaxDelay, _clock.Delays);

        // A CheckInterval longer than the wait is fine too: the wait is chunked at 30 days.
        _clock.Delays.Clear();
        RenewalScheduler scheduler = new(manager, new RenewalSchedulerOptions { TimeProvider = _clock, CheckInterval = TimeSpan.FromDays(365), UseRenewalInfo = false });
        using TestCa ca = new("Clamp Root");
        DateTimeOffset start = _clock.Now;
        using X509Certificate2 current = ca.IssueLeaf("fix2.example.test", start.AddDays(-1), start.AddDays(90));
        using ECDsa key = current.GetECDsaPrivateKey()!;
        using X509Certificate2 pub = X509CertificateLoader.LoadCertificate(current.RawData);
        using IssuedCertificate currentIssued = IssuedCertificate.Create([pub], key);
        using CancellationTokenSource cts = new();
        Task run = scheduler.RunAsync(currentIssued, (cert, _) =>
        {
            cert.Dispose();
            cts.Cancel();
            return Task.CompletedTask;
        }, null, cts.Token);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await run);
        Assert.All(_clock.Delays, d => Assert.True(d <= AcmeTimers.MaxDelay));
        Assert.Contains(AcmeTimers.MaxDelay, _clock.Delays);
    }

    // ---- renewal guards --------------------------------------------------------------------------------------------------

    [Fact]
    public void AriRefreshDelay_IsClamped()
    {
        TimeSpan fallback = TimeSpan.FromHours(6);
        Assert.Equal(fallback, RenewalScheduler.GetAriRefreshDelay(null, fallback));
        Assert.Equal(fallback, RenewalScheduler.GetAriRefreshDelay(TimeSpan.Zero, fallback));
        Assert.Equal(TimeSpan.FromMinutes(1), RenewalScheduler.GetAriRefreshDelay(TimeSpan.FromSeconds(5), fallback));
        Assert.Equal(TimeSpan.FromHours(2), RenewalScheduler.GetAriRefreshDelay(TimeSpan.FromHours(2), fallback));
        Assert.Equal(TimeSpan.FromHours(24), RenewalScheduler.GetAriRefreshDelay(TimeSpan.FromDays(365), fallback));
    }

    [Fact]
    public void RenewalTime_FallsBackToOneThird_WhenTheLeadCoversTheLifetime()
    {
        DateTimeOffset notBefore = new(2026, 10, 1, 0, 0, 0, TimeSpan.Zero);
        DateTimeOffset notAfter = notBefore.AddDays(6); // short-lived profile
        Assert.Equal(notBefore.AddDays(4), RenewalScheduler.GetRenewalTime(notBefore, notAfter, TimeSpan.FromDays(30)));
        Assert.Equal(notBefore.AddDays(4), RenewalScheduler.GetRenewalTime(notBefore, notAfter, TimeSpan.FromDays(6)));
        Assert.Equal(notBefore.AddDays(1), RenewalScheduler.GetRenewalTime(notBefore, notAfter, TimeSpan.FromDays(5)));
    }

    [Fact]
    public async Task AriSelection_IsDrawnOncePerWindow()
    {
        _server.Clock = () => _clock.Now;
        _server.RenewalInfoEnabled = true;
        _server.RenewalWindowFractions = (0.5, 0.6);
        _server.RenewalInfoRetryAfterSeconds = 3600;
        AcmeCertificateManager manager = NewManager();
        CountingRandom random = new();
        RenewalScheduler scheduler = new(manager, new RenewalSchedulerOptions { TimeProvider = _clock, CheckInterval = TimeSpan.FromDays(1), StartupDelay = TimeSpan.Zero, Random = random });
        using IssuedCertificate first = await manager.OrderCertificateAsync();
        DateTimeOffset notBefore = new(first.Certificate.NotBefore.ToUniversalTime(), TimeSpan.Zero);
        TimeSpan lifetime = first.NotAfter - notBefore;

        DateTimeOffset? renewedAt = null;
        using CancellationTokenSource cts = new();
        Task run = scheduler.RunAsync(first, (cert, _) =>
        {
            renewedAt = _clock.Now;
            cert.Dispose();
            cts.Cancel();
            return Task.CompletedTask;
        }, null, cts.Token);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await run);

        Assert.True(_server.RenewalInfoRequests > 10);
        Assert.Equal(1, random.Draws); // one draw for the unchanged window, however often ARI was re-fetched
        DateTimeOffset expected = notBefore + lifetime * 0.5 + (lifetime * 0.1) / 2;
        Assert.InRange(renewedAt!.Value, expected - TimeSpan.FromSeconds(1), expected + TimeSpan.FromSeconds(1));
    }

    [Fact]
    public async Task MinimumRenewalInterval_SpacesRenewals_EvenWhenAriSaysRenewNow()
    {
        _server.Clock = () => _clock.Now;
        _server.RenewalInfoEnabled = true;
        _server.RenewalWindowFractions = (-0.5, -0.4); // every certificate is "due" the moment it is issued
        AcmeCertificateManager manager = NewManager();
        RenewalScheduler scheduler = new(manager, new RenewalSchedulerOptions
        {
            TimeProvider = _clock,
            StartupDelay = TimeSpan.Zero,
            CheckInterval = TimeSpan.FromMinutes(30),
            MinimumRenewalInterval = TimeSpan.FromHours(2),
        });
        Assert.Throws<ArgumentOutOfRangeException>(() => new RenewalScheduler(manager, new RenewalSchedulerOptions { MinimumRenewalInterval = TimeSpan.FromTicks(-1) }));
        Assert.Equal(TimeSpan.FromHours(1), new RenewalSchedulerOptions().MinimumRenewalInterval);

        List<DateTimeOffset> renewedAt = [];
        using CancellationTokenSource cts = new();
        Task run = scheduler.RunAsync(null, (cert, _) =>
        {
            renewedAt.Add(_clock.Now);
            cert.Dispose();
            if (renewedAt.Count == 3)
            {
                cts.Cancel();
            }

            return Task.CompletedTask;
        }, null, cts.Token);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await run);

        Assert.Equal(3, renewedAt.Count);
        Assert.True(renewedAt[1] - renewedAt[0] >= TimeSpan.FromHours(2));
        Assert.True(renewedAt[2] - renewedAt[1] >= TimeSpan.FromHours(2));
        Assert.True(renewedAt[2] - renewedAt[0] < TimeSpan.FromHours(5));
    }

    // ---- account store default, key storage flags, CSR subject -----------------------------------------------------------

    [Fact]
    public void AccountStore_DefaultsToDpapiOnWindows()
    {
        Assert.Equal(OperatingSystem.IsWindows() ? AcmeStoreProtection.DpapiCurrentUser : AcmeStoreProtection.None, AcmeAccountStore.DefaultProtection);
        AcmeAccountStore store = new(Path.Combine(_dir, "default.json"));
        Assert.Equal(AcmeAccountStore.DefaultProtection, store.Protection);

        using AcmeAccountKey key = AcmeAccountKey.Create();
        store.Save(AcmeAccountStore.CreateState(key, new Uri("https://ca.test/acct/5"), StubDirectory));
        string raw = File.ReadAllText(store.Path);
        if (OperatingSystem.IsWindows())
        {
            Assert.Contains("\"dpapi\":", raw, StringComparison.Ordinal);
            Assert.DoesNotContain("PRIVATE KEY", raw, StringComparison.Ordinal);
        }
        else
        {
            Assert.Contains("PRIVATE KEY", raw, StringComparison.Ordinal);
        }

        Assert.Equal(key.Thumbprint, store.LoadKey()!.Thumbprint);
    }

    [Fact]
    public void IssuedCertificate_Load_AcceptsKeyStorageFlags()
    {
        using TestCa ca = new("Flags Root");
        using X509Certificate2 leaf = ca.IssueLeaf("flags.example.test", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(10));
        using ECDsa key = leaf.GetECDsaPrivateKey()!;
        using X509Certificate2 pub = X509CertificateLoader.LoadCertificate(leaf.RawData);
        using IssuedCertificate issued = IssuedCertificate.Create([pub], key);

        using IssuedCertificate reloaded = IssuedCertificate.Load(issued.Pfx, null, X509KeyStorageFlags.EphemeralKeySet);
        Assert.True(reloaded.Certificate.HasPrivateKey);
        Assert.Equal(issued.Certificate.Thumbprint, reloaded.Certificate.Thumbprint);
    }

    [Fact]
    public void Csr_Subject_UsesAFittingDnsName_OrStaysEmpty()
    {
        using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        string longName = new string('b', 60) + ".example.test";
        string exactly64 = new string('c', 51) + ".example.test";
        Assert.Equal(64, exactly64.Length);

        Assert.Equal("short.example.test", CsrBuilder.SelectCommonName([AcmeIdentifier.Dns(longName), AcmeIdentifier.Ip("192.0.2.1"), AcmeIdentifier.Dns("short.example.test")]));
        Assert.Equal(exactly64, CsrBuilder.SelectCommonName([AcmeIdentifier.Dns(exactly64)]));
        Assert.Null(CsrBuilder.SelectCommonName([AcmeIdentifier.Dns(longName + "x")]));
        Assert.Null(CsrBuilder.SelectCommonName([AcmeIdentifier.Ip("192.0.2.1")]));

        CertificateRequest withCn = Load(CsrBuilder.CreateCsr([AcmeIdentifier.Dns(longName), AcmeIdentifier.Dns("short.example.test")], key));
        Assert.Equal("CN=short.example.test", withCn.SubjectName.Name);
        Assert.False(San(withCn).Critical);

        // IP-only: empty subject and, as RFC 5280 §4.2.1.6 requires then, a critical SAN.
        CertificateRequest ipOnly = Load(CsrBuilder.CreateCsr([AcmeIdentifier.Ip("192.0.2.7")], key));
        Assert.Equal([0x30, 0x00], ipOnly.SubjectName.RawData);
        X509SubjectAlternativeNameExtension san = San(ipOnly);
        Assert.True(san.Critical);
        Assert.Equal(IPAddress.Parse("192.0.2.7"), Assert.Single(san.EnumerateIPAddresses()));

        static CertificateRequest Load(byte[] der) => CertificateRequest.LoadSigningRequest(der, HashAlgorithmName.SHA256, CertificateRequestLoadOptions.UnsafeLoadCertificateExtensions);

        static X509SubjectAlternativeNameExtension San(CertificateRequest request)
        {
            X509Extension extension = Assert.Single(request.CertificateExtensions, e => e.Oid?.Value == "2.5.29.17");
            return new X509SubjectAlternativeNameExtension(extension.RawData, extension.Critical);
        }
    }

    // ---- helpers -----------------------------------------------------------------------------------------------------

    private static HttpResponseMessage Json(HttpStatusCode status, string json, params string[] nonces)
    {
        HttpResponseMessage response = new(status) { Content = new StringContent(json, Encoding.UTF8, "application/json") };
        foreach (string nonce in nonces)
        {
            response.Headers.TryAddWithoutValidation("Replay-Nonce", nonce);
        }

        return response;
    }

    private static HttpResponseMessage Problem(string type, string? nonce = null)
    {
        HttpResponseMessage response = new(HttpStatusCode.BadRequest)
        {
            Content = new StringContent("{\"type\":\"" + type + "\",\"detail\":\"x\"}", Encoding.UTF8, "application/problem+json"),
        };
        if (nonce is not null)
        {
            response.Headers.TryAddWithoutValidation("Replay-Nonce", nonce);
        }

        return response;
    }

    private static string? ReadJwsNonce(HttpRequestMessage request)
    {
        byte[] body = request.Content!.ReadAsByteArrayAsync().GetAwaiter().GetResult();
        using JsonDocument envelope = JsonDocument.Parse(body);
        byte[] header = System.Buffers.Text.Base64Url.DecodeFromChars(envelope.RootElement.GetProperty("protected").GetString());
        using JsonDocument protectedHeader = JsonDocument.Parse(header);
        return protectedHeader.RootElement.TryGetProperty("nonce", out JsonElement nonce) ? nonce.GetString() : null;
    }

    private sealed class StubHandler(Func<HttpRequestMessage, HttpResponseMessage> respond) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            HttpResponseMessage response = respond(request);
            response.RequestMessage = request;
            return Task.FromResult(response);
        }
    }

    /// <summary>Creates TXT records normally but never completes a removal.</summary>
    private sealed class HangingRemoveDns01Provider(InMemoryDns01Provider inner) : IDns01Provider
    {
        public int RemoveCalls;

        public ValueTask CreateTxtAsync(string name, string value, CancellationToken cancellationToken) => inner.CreateTxtAsync(name, value, cancellationToken);

        public ValueTask RemoveTxtAsync(string name, string value, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref RemoveCalls);
            return new ValueTask(new TaskCompletionSource().Task); // ignores the token and never finishes
        }
    }

    private sealed class CountingRandom : Random
    {
        public int Draws;

        public override long NextInt64(long maxValue)
        {
            Draws++;
            return maxValue / 2;
        }
    }

    private sealed class FixedRandom(double value) : Random
    {
        public override double NextDouble() => value;
    }
}
