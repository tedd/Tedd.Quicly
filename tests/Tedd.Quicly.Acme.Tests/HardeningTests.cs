using System.Net;
using System.Security.Cryptography;
using System.Text;
using Tedd.Quicly.Acme.Challenges;
using Tedd.Quicly.Acme.Models;
using Tedd.Quicly.Acme.Tests.Fake;

namespace Tedd.Quicly.Acme.Tests;

/// <summary>
/// Regression tests for the review fixes: bounded responses, nonce validation, relative Location headers, owner-only
/// atomic files, finalize without Location, resumed challenges already processing, HttpClient timeouts as transient
/// failures, and alternate-chain download failures.
/// </summary>
public sealed class HardeningTests : IAsyncDisposable
{
    private static readonly Uri StubDirectory = new("https://ca.test/directory");
    private const string StubDirectoryJson = "{\"newNonce\":\"https://ca.test/nonce\",\"newAccount\":\"https://ca.test/acct\",\"newOrder\":\"https://ca.test/order\"}";

    private readonly FakeAcmeServer _server = new();
    private readonly HttpClient _http = new();
    private readonly FastTimeProvider _clock = new();
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "quicly-acme-tests", Guid.NewGuid().ToString("N"));
    private readonly InMemoryDns01Provider _dns01 = new();

    public HardeningTests()
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

    private AcmeCertificateManager NewManager(HttpClient? http = null, int maxAttempts = 1, string? preferredChainIssuer = null, string domain = "fix.example.test")
    {
        return new AcmeCertificateManager(http ?? _http, new AcmeCertificateManagerOptions
        {
            DirectoryUrl = _server.DirectoryUrl,
            AccountStore = new AcmeAccountStore(Path.Combine(_dir, "account.json")),
            Identifiers = [AcmeIdentifier.Dns(domain)],
            PreferredChallengeTypes = [AcmeChallengeTypes.Dns01],
            Dns01Provider = _dns01,
            PreferredChainIssuer = preferredChainIssuer,
            ClientOptions = new AcmeClientOptions { TimeProvider = _clock, MaxPollAttempts = 5 },
            Retry = new AcmeRetryOptions { MaxAttempts = maxAttempts, InitialDelay = TimeSpan.FromSeconds(1), Jitter = 0 },
        });
    }

    private static HttpResponseMessage Json(HttpStatusCode status, string json, params string[] nonces)
    {
        HttpResponseMessage response = new(status) { Content = new StringContent(json, Encoding.UTF8, "application/json") };
        foreach (string nonce in nonces)
        {
            response.Headers.TryAddWithoutValidation("Replay-Nonce", nonce);
        }

        return response;
    }

    // ---- bounded responses ------------------------------------------------------------------------------------------

    [Fact]
    public async Task OversizedResponse_WithContentLength_IsRejectedBeforeBuffering()
    {
        using HttpClient http = new(new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.OK) { Content = new ByteArrayContent(new byte[2048]) }));
        using AcmeAccountKey key = AcmeAccountKey.Create();
        AcmeClient client = new(http, StubDirectory, key, null, new AcmeClientOptions { MaxResponseBytes = 1024 });
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await client.GetDirectoryAsync());
        Assert.Equal(AcmeErrorTypes.InvalidResponse, e.Type);
        Assert.Contains("MaxResponseBytes (1024)", e.Message);
    }

    [Fact]
    public async Task OversizedResponse_WithoutContentLength_IsRejectedWhileStreaming()
    {
        using HttpClient http = new(new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.OK) { Content = new StreamContent(new NonSeekableStream(new byte[40_000])) }));
        using AcmeAccountKey key = AcmeAccountKey.Create();
        AcmeClient client = new(http, StubDirectory, key, null, new AcmeClientOptions { MaxResponseBytes = 20_000 });
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await client.GetDirectoryAsync());
        Assert.Equal(AcmeErrorTypes.InvalidResponse, e.Type);

        // A small streamed (length-less) body within the bound parses normally.
        using HttpClient ok = new(new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.OK) { Content = new StreamContent(new NonSeekableStream(Encoding.UTF8.GetBytes(StubDirectoryJson))) }));
        AcmeClient small = new(ok, StubDirectory, key);
        AcmeDirectory directory = await small.GetDirectoryAsync();
        Assert.Equal(new Uri("https://ca.test/order"), directory.NewOrder);
        Assert.Equal(1024 * 1024, small.Options.MaxResponseBytes);
    }

    // ---- nonces -----------------------------------------------------------------------------------------------------

    [Theory]
    [InlineData(null, false)]
    [InlineData("", false)]
    [InlineData("abc+def", false)]
    [InlineData("abc def", false)]
    [InlineData("abc=", false)]
    [InlineData("AZaz09-_", true)]
    public void IsValidNonce_AcceptsOnlyBase64Url(string? nonce, bool valid)
    {
        Assert.Equal(valid, AcmeClient.IsValidNonce(nonce));
    }

    [Fact]
    public void IsValidNonce_RejectsOverlongValues()
    {
        Assert.True(AcmeClient.IsValidNonce(new string('a', AcmeClient.MaxNonceLength)));
        Assert.False(AcmeClient.IsValidNonce(new string('a', AcmeClient.MaxNonceLength + 1)));
    }

    [Fact]
    public async Task InvalidNonces_AreIgnored_AndThePoolIsBounded()
    {
        string[] nonces = new string[AcmeClient.MaxPooledNonces + 8 + 2];
        for (int i = 0; i < nonces.Length - 2; i++)
        {
            nonces[i] = "nonce" + i.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        nonces[^2] = "not/valid";
        nonces[^1] = "also invalid";
        using HttpClient http = new(new StubHandler(_ => Json(HttpStatusCode.OK, StubDirectoryJson, nonces)));
        using AcmeAccountKey key = AcmeAccountKey.Create();
        AcmeClient client = new(http, StubDirectory, key);
        await client.GetDirectoryAsync();
        Assert.Equal(AcmeClient.MaxPooledNonces, client.PooledNonceCount);
        Assert.Equal("nonce0", await client.GetNonceAsync());
    }

    // ---- Location resolution ----------------------------------------------------------------------------------------

    [Fact]
    public async Task RelativeLocationHeader_IsResolvedAgainstTheRequestUrl()
    {
        using HttpClient http = new(new StubHandler(request =>
        {
            if (request.Method == HttpMethod.Get)
            {
                return Json(HttpStatusCode.OK, StubDirectoryJson, "n1");
            }

            HttpResponseMessage created = Json(HttpStatusCode.Created, "{\"status\":\"valid\"}", "n2");
            created.Headers.Location = new Uri("/acct/7", UriKind.Relative);
            return created;
        }));
        using AcmeAccountKey key = AcmeAccountKey.Create();
        AcmeClient client = new(http, StubDirectory, key);
        AcmeAccount account = await client.CreateAccountAsync(null, agreeTermsOfService: true);
        Assert.Equal(new Uri("https://ca.test/acct/7"), account.Location);
        Assert.Equal(new Uri("https://ca.test/acct/7"), client.AccountUrl);
    }

    // ---- secrets on disk --------------------------------------------------------------------------------------------

    [Fact]
    public void SecureFile_WritesAtomically_OwnerOnly_AndCleansUpOnFailure()
    {
        string target = Path.Combine(_dir, "nested", "secret.bin");
        SecureFile.WriteAllBytesAtomic(target, [1, 2, 3]);
        SecureFile.WriteAllBytesAtomic(target, [4, 5]); // overwrite
        Assert.Equal([4, 5], File.ReadAllBytes(target));
        Assert.Empty(Directory.GetFiles(Path.GetDirectoryName(target)!, "*.tmp"));
        if (!OperatingSystem.IsWindows())
        {
            Assert.Equal(SecureFile.OwnerOnly, File.GetUnixFileMode(target));
        }

        // The target is a directory: the rename fails and the temporary file is removed again.
        string directoryTarget = Path.Combine(_dir, "is-a-directory");
        Directory.CreateDirectory(directoryTarget);
        Assert.ThrowsAny<Exception>(() => SecureFile.WriteAllBytesAtomic(directoryTarget, [1]));
        Assert.Empty(Directory.GetFiles(_dir, "*.tmp"));
    }

    // ---- key import / EAB -------------------------------------------------------------------------------------------

    [Fact]
    public void Import_RejectsWrongCurveAndShortRsa_ForEveryPemLabel()
    {
        using ECDsa p384 = ECDsa.Create(ECCurve.NamedCurves.nistP384);
        Assert.Throws<ArgumentException>(() => AcmeAccountKey.Import(p384.ExportPkcs8PrivateKeyPem()));
        Assert.Throws<ArgumentException>(() => AcmeAccountKey.Import(p384.ExportECPrivateKeyPem()));
        using RSA rsa1024 = RSA.Create(1024);
        Assert.Throws<ArgumentException>(() => AcmeAccountKey.Import(rsa1024.ExportRSAPrivateKeyPem()));
        Assert.Throws<ArgumentException>(() => AcmeAccountKey.Import(rsa1024.ExportPkcs8PrivateKeyPem()));
    }

    [Fact]
    public void Eab_HmacKey_AcceptsBase64UrlAndStandardBase64()
    {
        byte[] raw = [0xFB, 0xEF, 0xFF, 0x10];
        Assert.Equal(raw, new ExternalAccountBinding("k", Base64UrlCodec.Encode(raw)).DecodeHmacKey());
        Assert.Equal(raw, new ExternalAccountBinding("k", Convert.ToBase64String(raw)).DecodeHmacKey()); // "++//EA=="
        Assert.Throws<ArgumentException>(() => new ExternalAccountBinding("k", string.Empty).DecodeHmacKey());
    }

    // ---- manager flow fixes -----------------------------------------------------------------------------------------

    [Fact]
    public async Task FinalizeWithoutLocationHeader_StillCompletes()
    {
        // RFC 8555 does not require Location on finalize; the manager must keep polling the order URL it already knows.
        _server.OmitLocationOnFinalize = true;
        _server.ProcessingOrderPolls = 1;
        using IssuedCertificate issued = await NewManager().OrderCertificateAsync();
        Assert.True(issued.Certificate.MatchesHostname("fix.example.test"));
    }

    [Fact]
    public async Task ResumedChallenge_AlreadyProcessing_IsNotPostedAgain()
    {
        // First run: the challenge is answered but the authorization stays pending past MaxPollAttempts (the run "dies").
        _server.PendingAuthzPolls = 7;
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await NewManager().OrderCertificateAsync());
        Assert.Equal(AcmeErrorTypes.PollTimeout, e.Type);
        _server.PendingAuthzPolls = 0;

        // Second run resumes the order, sees the challenge 'processing' and only waits for it.
        AcmeCertificateManager second = NewManager();
        List<AcmeProgress> events = [];
        second.Progress += events.Add;
        using IssuedCertificate issued = await second.OrderCertificateAsync();
        Assert.Equal(1, _server.RequestLog.Count(r => r.StartsWith("POST /chall/", StringComparison.Ordinal)));
        Assert.Contains(events, ev => ev.Stage == AcmeStage.OrderResumed && ev.Message.StartsWith("Resuming", StringComparison.Ordinal));
        Assert.Contains(events, ev => ev.Stage == AcmeStage.ChallengeResponded && ev.Message.Contains("already processing", StringComparison.Ordinal));
        Assert.Empty(_dns01.Lookup("_acme-challenge.fix.example.test")); // cleaned up again
    }

    [Fact]
    public async Task HttpClientTimeout_IsRetriedAsTransient()
    {
        using HttpClient flaky = new(new FailFirstHandler(failures: 1) { InnerHandler = new HttpClientHandler() });
        AcmeCertificateManager manager = NewManager(flaky, maxAttempts: 3);
        List<AcmeProgress> events = [];
        manager.Progress += events.Add;
        using IssuedCertificate issued = await manager.OrderCertificateAsync();
        Assert.Single(events, ev => ev.Stage == AcmeStage.Retrying);
        Assert.DoesNotContain(events, ev => ev.Stage == AcmeStage.Failed);
    }

    [Fact]
    public async Task AlternateChainDownloadFailure_Propagates()
    {
        _server.OmitAlternateChainLink = true;
        _server.ExtraLinkHeader = "<" + new Uri(_server.BaseUri, "cert/missing") + ">;rel=\"alternate\"";
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await NewManager(preferredChainIssuer: "No Such Root").OrderCertificateAsync());
        Assert.Equal(HttpStatusCode.NotFound, e.StatusCode);
    }

    [Fact]
    public async Task PendingOrder_ForSameCountButDifferentIdentifier_IsNotResumed()
    {
        _dns01.FailCreate = true; // persist a pending order for fix.example.test, then "crash"
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await NewManager().OrderCertificateAsync());
        _dns01.FailCreate = false;

        AcmeCertificateManager other = NewManager(domain: "other.example.test");
        List<AcmeProgress> events = [];
        other.Progress += events.Add;
        using IssuedCertificate issued = await other.OrderCertificateAsync();
        Assert.True(issued.Certificate.MatchesHostname("other.example.test"));
        Assert.Equal(2, _server.RequestLog.Count(r => r == "POST /new-order"));
        Assert.DoesNotContain(events, ev => ev.Stage == AcmeStage.OrderResumed);
    }

    [Fact]
    public async Task RenewalScheduler_CancelledDuringAriFetch_StopsWithoutReportingAnError()
    {
        _server.RenewalInfoEnabled = true;
        using IssuedCertificate current = await NewManager().OrderCertificateAsync();

        using CancellationTokenSource cts = new();
        using HttpClient cancelling = new(new CancelOnPathHandler("/renewal-info/", cts) { InnerHandler = new HttpClientHandler() });
        RenewalScheduler scheduler = new(NewManager(cancelling), new RenewalSchedulerOptions { TimeProvider = _clock });
        List<Exception> errors = [];
        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => scheduler.RunAsync(current, static (c, _) => { c.Dispose(); return Task.CompletedTask; }, errors.Add, cts.Token));
        Assert.Empty(errors);
        Assert.Contains(_server.RequestLog, r => r == "GET /directory"); // the manager reached the CA before ARI was cancelled
    }

    // ---- helpers ----------------------------------------------------------------------------------------------------

    /// <summary>Cancels <paramref name="cts"/> when a request path contains <paramref name="pathFragment"/>, then observes the cancellation.</summary>
    private sealed class CancelOnPathHandler(string pathFragment, CancellationTokenSource cts) : DelegatingHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (request.RequestUri!.AbsolutePath.Contains(pathFragment, StringComparison.Ordinal))
            {
                cts.Cancel();
                cancellationToken.ThrowIfCancellationRequested();
            }

            return base.SendAsync(request, cancellationToken);
        }
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

    /// <summary>Fails the first requests the way HttpClient reports its own timeout.</summary>
    private sealed class FailFirstHandler(int failures) : DelegatingHandler
    {
        private int _remaining = failures;

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (Interlocked.Decrement(ref _remaining) >= 0)
            {
                throw new TaskCanceledException("The request was canceled due to the configured HttpClient.Timeout.", new TimeoutException());
            }

            return base.SendAsync(request, cancellationToken);
        }
    }

    /// <summary>A read-only stream without a length, so StreamContent reports no Content-Length.</summary>
    private sealed class NonSeekableStream(byte[] data) : Stream
    {
        private readonly MemoryStream _inner = new(data);

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => throw new NotSupportedException();

        public override long Position
        {
            get => throw new NotSupportedException();
            set => throw new NotSupportedException();
        }

        public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);

        public override void Flush()
        {
        }

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            if (disposing)
            {
                _inner.Dispose();
            }

            base.Dispose(disposing);
        }
    }
}
