using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.Json;
using Tedd.Quicly.Acme.Challenges;
using Tedd.Quicly.Acme.Models;
using Tedd.Quicly.Testing.Acme;

namespace Tedd.Quicly.Acme.Tests;

/// <summary>
/// Failing tests written during review. Each one demonstrates a defect in the production code; they are expected to fail
/// until the corresponding fix lands.
/// </summary>
public sealed class ReviewFindingsTests : IAsyncDisposable
{
    private static readonly Uri StubDirectory = new("https://ca.test/directory");
    private const string StubDirectoryJson = "{\"newNonce\":\"https://ca.test/nonce\",\"newAccount\":\"https://ca.test/acct\",\"newOrder\":\"https://ca.test/order\"}";

    private readonly FakeAcmeServer _server = new();
    private readonly HttpClient _http = new();
    private readonly FastTimeProvider _clock = new();
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "quicly-acme-tests", Guid.NewGuid().ToString("N"));
    private readonly InMemoryDns01Provider _dns01 = new();

    public ReviewFindingsTests()
    {
        Directory.CreateDirectory(_dir);
        _server.DnsTxtLookup = _dns01.Lookup;
        _server.Clock = () => _clock.Now;
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

    // ---- ADR 0009: "account key ... never logged" ----------------------------------------------------------------
    // Public records get a compiler-generated ToString() that prints every property, so logging the state object (or
    // an EAB credential) writes the private key / HMAC secret to the log.

    [Fact]
    public void AccountState_ToString_DoesNotExposeThePrivateKey()
    {
        using AcmeAccountKey key = AcmeAccountKey.Create();
        AcmeAccountState state = AcmeAccountStore.CreateState(key, new Uri("https://ca.test/acct/1"), StubDirectory);
        Assert.DoesNotContain("PRIVATE KEY", state.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void PendingOrder_ToString_DoesNotExposeTheCertificateKey()
    {
        using AcmeAccountKey key = AcmeAccountKey.Create();
        AcmePendingOrder pending = new()
        {
            OrderUrl = new Uri("https://ca.test/order/1"),
            Identifiers = [AcmeIdentifier.Dns("a.example.test")],
            CertificateKeyPem = key.ExportPem(),
        };
        Assert.DoesNotContain("PRIVATE KEY", pending.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public void ExternalAccountBinding_ToString_DoesNotExposeTheHmacKey()
    {
        const string secret = "c2VjcmV0LWhtYWMta2V5LWZyb20tdGhlLWNh";
        ExternalAccountBinding eab = new("kid-1", secret);
        Assert.DoesNotContain(secret, eab.ToString(), StringComparison.Ordinal);
    }

    // ---- RFC 8555 §6.5: "the client SHOULD retry the request using the new nonce" ---------------------------------
    // The single badNonce retry takes the head of a FIFO pool instead of the fresh nonce the badNonce response carried,
    // so with two or more stale pooled nonces (long idle between renewals) the one retry is wasted.

    [Fact]
    public async Task BadNonceRetry_UsesTheFreshNonceFromTheBadNonceResponse()
    {
        List<string?> noncesUsed = [];
        using HttpClient http = new(new StubHandler(request =>
        {
            if (request.Method == HttpMethod.Get)
            {
                // Two nonces the CA has since forgotten (harvested earlier, then a long idle period).
                return Json(HttpStatusCode.OK, StubDirectoryJson, "stale1", "stale2");
            }

            string? nonce = ReadJwsNonce(request);
            noncesUsed.Add(nonce);
            if (nonce == "fresh")
            {
                HttpResponseMessage created = Json(HttpStatusCode.Created, "{\"status\":\"valid\"}", "next");
                created.Headers.Location = new Uri("https://ca.test/acct/1");
                return created;
            }

            HttpResponseMessage bad = new(HttpStatusCode.BadRequest)
            {
                Content = new StringContent("{\"type\":\"" + AcmeErrorTypes.BadNonce + "\",\"detail\":\"stale\"}", Encoding.UTF8, "application/problem+json"),
            };
            bad.Headers.TryAddWithoutValidation("Replay-Nonce", "fresh");
            return bad;
        }));
        using AcmeAccountKey key = AcmeAccountKey.Create();
        AcmeClient client = new(http, StubDirectory, key);

        AcmeAccount account = await client.CreateAccountAsync(null, agreeTermsOfService: true);

        Assert.Equal(new Uri("https://ca.test/acct/1"), account.Location);
        Assert.Equal(["stale1", "fresh"], noncesUsed);
    }

    // ---- ADR 0009: "the CA connection is always TLS-validated" / RFC 8555 §6.1 (HTTPS) ----------------------------
    // Nothing stops a plain-http directory (or http:// URLs inside a directory) for a non-loopback CA.

    [Fact]
    public async Task PlainHttpDirectory_OnNonLoopbackHost_IsRefusedBeforeAnyRequest()
    {
        int requests = 0;
        using HttpClient http = new(new StubHandler(_ =>
        {
            requests++;
            return Json(HttpStatusCode.OK, StubDirectoryJson.Replace("https://", "http://", StringComparison.Ordinal));
        }));
        using AcmeAccountKey key = AcmeAccountKey.Create();

        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            AcmeClient client = new(http, new Uri("http://ca.test/directory"), key);
            await client.GetDirectoryAsync();
        });
        Assert.Equal(0, requests);
    }

    // ---- CSR: subject CN is ub-common-name (64) bounded ------------------------------------------------------------
    // RFC 5280 limits CN to 64 characters and Boulder (Let's Encrypt) rejects longer CNs with badCSR, so a first
    // identifier longer than 64 characters must not be copied into the CN (the SAN alone carries it).

    [Fact]
    public void Csr_ForDnsNameLongerThan64Characters_HasNoOverlongCommonName()
    {
        string longName = new string('a', 60) + ".example.test"; // 73 characters, every label <= 63
        using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);

        byte[] der = CsrBuilder.CreateCsr([AcmeIdentifier.Dns(longName)], key);

        // Requested extensions are only loaded with UnsafeLoadCertificateExtensions (the default skips them).
        CertificateRequest parsed = CertificateRequest.LoadSigningRequest(der, HashAlgorithmName.SHA256, CertificateRequestLoadOptions.UnsafeLoadCertificateExtensions);
        foreach (X500RelativeDistinguishedName rdn in parsed.SubjectName.EnumerateRelativeDistinguishedNames())
        {
            if (rdn.GetSingleElementType().Value == "2.5.4.3")
            {
                Assert.True(rdn.GetSingleElementValue()!.Length <= 64, "CSR subject CN is longer than 64 characters.");
            }
        }

        X509Extension sanExtension = Assert.Single(parsed.CertificateExtensions, e => e.Oid?.Value == "2.5.29.17");
        Assert.Contains(longName, new X509SubjectAlternativeNameExtension(sanExtension.RawData).EnumerateDnsNames());
    }

    // ---- RenewalScheduler: a just-issued certificate is re-ordered back to back ------------------------------------
    // With a fixed RenewBefore at least as long as the certificate lifetime (e.g. 30 days against a short-lived 6-day
    // profile) the renewal time of every new certificate is already in the past, so the loop orders certificate after
    // certificate with no wait until the CA rate-limits the account.

    [Fact]
    public async Task RunAsync_DoesNotReorderAFreshCertificateImmediately_WhenRenewBeforeExceedsLifetime()
    {
        AcmeCertificateManager manager = new(_http, new AcmeCertificateManagerOptions
        {
            DirectoryUrl = _server.DirectoryUrl,
            AccountStore = new AcmeAccountStore(Path.Combine(_dir, "account.json")),
            Identifiers = [AcmeIdentifier.Dns("loop.example.test")],
            PreferredChallengeTypes = [AcmeChallengeTypes.Dns01],
            Dns01Provider = _dns01,
            ClientOptions = new AcmeClientOptions { TimeProvider = _clock, MaxPollAttempts = 5 },
            Retry = new AcmeRetryOptions { MaxAttempts = 1, Jitter = 0 },
        });

        // The fake CA issues 90-day certificates; the lead time is longer than that.
        RenewalScheduler scheduler = new(manager, new RenewalSchedulerOptions
        {
            TimeProvider = _clock,
            RenewBefore = TimeSpan.FromDays(120),
            StartupDelay = TimeSpan.Zero,
        });

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
        Assert.True(
            renewedAt[1] - renewedAt[0] >= TimeSpan.FromHours(1),
            "A certificate issued moments ago was replaced after only " + (renewedAt[1] - renewedAt[0]) + ".");
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
}
