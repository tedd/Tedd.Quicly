using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Acme.Challenges;
using Tedd.Quicly.Acme.Models;
using Tedd.Quicly.Testing.Acme;

namespace Tedd.Quicly.Acme.Tests;

/// <summary>Retry with back-off, crash-resumable orders, key reuse and ARI <c>replaces</c> handling in <see cref="AcmeCertificateManager"/>.</summary>
public sealed class AcmeCertificateManagerResilienceTests : IAsyncDisposable
{
    private readonly FakeAcmeServer _server = new();
    private readonly HttpClient _http = new();
    private readonly FastTimeProvider _clock = new();
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "quicly-acme-tests", Guid.NewGuid().ToString("N"));
    private readonly InMemoryDns01Provider _dns01 = new();

    public AcmeCertificateManagerResilienceTests()
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
        Directory.Delete(_dir, true);
    }

    private AcmeCertificateManagerOptions Options(
        IReadOnlyList<AcmeIdentifier>? identifiers = null,
        string? certificatePath = null,
        bool reuseKey = false,
        AcmeRetryOptions? retry = null,
        AcmeKeyAlgorithm certificateKeyAlgorithm = AcmeKeyAlgorithm.ES256)
    {
        return new AcmeCertificateManagerOptions
        {
            DirectoryUrl = _server.DirectoryUrl,
            AccountStore = new AcmeAccountStore(Path.Combine(_dir, "account.json")),
            Identifiers = identifiers ?? [AcmeIdentifier.Dns("resume.example.test"), AcmeIdentifier.Dns("www.resume.example.test")],
            PreferredChallengeTypes = [AcmeChallengeTypes.Dns01],
            Dns01Provider = _dns01,
            CertificatePath = certificatePath,
            ReuseKey = reuseKey,
            CertificateKeyAlgorithm = certificateKeyAlgorithm,
            ClientOptions = new AcmeClientOptions { TimeProvider = _clock, MaxPollAttempts = 5 },
            Retry = retry ?? new AcmeRetryOptions { MaxAttempts = 1 },
        };
    }

    private static string PublicKeyOf(X509Certificate2 cert) => Convert.ToHexString(cert.PublicKey.ExportSubjectPublicKeyInfo());

    [Fact]
    public void Constructor_ValidatesReuseKeyAndRetry()
    {
        Assert.Throws<ArgumentException>(() => new AcmeCertificateManager(_http, Options(reuseKey: true)));
        Assert.Throws<ArgumentOutOfRangeException>(() => new AcmeCertificateManager(_http, Options(retry: new AcmeRetryOptions { MaxAttempts = 0 })));
        AcmeCertificateManager ok = new(_http, Options(reuseKey: true, certificatePath: Path.Combine(_dir, "c.pfx")));
        Assert.True(ok.Options.ReuseKey);
    }

    [Fact]
    public async Task TransientFailures_AreRetried_WithBackoffHonouringRetryAfter()
    {
        AcmeRetryOptions retry = new() { MaxAttempts = 5, InitialDelay = TimeSpan.FromSeconds(1), Multiplier = 2, Jitter = 0 };
        AcmeCertificateManager manager = new(_http, Options(retry: retry));
        List<AcmeProgress> events = [];
        manager.Progress += events.Add;

        _server.RateLimitRemaining = 2; // the first two HTTP requests get 429 + Retry-After: 3
        using IssuedCertificate issued = await manager.OrderCertificateAsync();

        Assert.True(issued.Certificate.MatchesHostname("resume.example.test"));
        Assert.Equal(2, events.Count(e => e.Stage == AcmeStage.Retrying));
        Assert.DoesNotContain(events, e => e.Stage == AcmeStage.Failed);
        // Back-off 1 s then 2 s, but Retry-After (3 s) is the floor for both.
        Assert.Equal([TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(3)], _clock.Delays.Where(d => d >= TimeSpan.FromSeconds(1)));

        // Exhausted retries surface the last error and report Failed.
        events.Clear();
        _clock.Delays.Clear();
        _server.RateLimitRemaining = int.MaxValue;
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await manager.OrderCertificateAsync());
        Assert.Equal(AcmeErrorTypes.RateLimited, e.Type);
        Assert.Equal(4, events.Count(ev => ev.Stage == AcmeStage.Retrying));
        Assert.Equal(AcmeStage.Failed, events[^1].Stage);
        Assert.Equal(4, _clock.Delays.Count(d => d >= TimeSpan.FromSeconds(1)));
        _server.RateLimitRemaining = 0;

        // Non-transient failures are not retried at all.
        events.Clear();
        _server.FailValidation = true;
        e = await Assert.ThrowsAsync<AcmeException>(async () => await manager.OrderCertificateAsync());
        Assert.Equal(AcmeErrorTypes.ValidationFailed, e.Type);
        Assert.DoesNotContain(events, ev => ev.Stage == AcmeStage.Retrying);
        Assert.Single(events, ev => ev.Stage == AcmeStage.Failed);
    }

    [Fact]
    public async Task Retry_ResumesTheSameOrder_InsteadOfCreatingANewOne()
    {
        AcmeRetryOptions retry = new() { MaxAttempts = 3, InitialDelay = TimeSpan.FromSeconds(1), Jitter = 0 };
        AcmeCertificateManager manager = new(_http, Options(retry: retry));
        List<AcmeProgress> events = [];
        manager.Progress += events.Add;

        // The order is created, then the first authorization fetch is rate-limited; the retry must resume the persisted order.
        _server.AddOverride("/authz/", 429, "application/problem+json", "{\"type\":\"urn:ietf:params:acme:error:rateLimited\",\"detail\":\"slow down\"}");
        using IssuedCertificate issued = await manager.OrderCertificateAsync();

        Assert.Equal(1, _server.RequestLog.Count(r => r == "POST /new-order"));
        Assert.Single(events, e => e.Stage == AcmeStage.OrderCreated);
        Assert.Single(events, e => e.Stage == AcmeStage.OrderResumed && e.Message.StartsWith("Resuming order", StringComparison.Ordinal));
        Assert.Single(events, e => e.Stage == AcmeStage.Retrying);
        Assert.Null(manager.Options.AccountStore.Load()!.PendingOrder);
    }

    [Fact]
    public async Task CrashAfterOrderCreation_IsResumedByANewManager_WithTheSameKey()
    {
        AcmeCertificateManagerOptions options = Options();
        AcmeCertificateManager first = new(_http, options);
        _dns01.FailCreate = true; // "crash" after the order and its key were persisted
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await first.OrderCertificateAsync());

        AcmePendingOrder pending = options.AccountStore.Load()!.PendingOrder!;
        Assert.Equal(2, pending.Identifiers.Count);
        Assert.Contains("PRIVATE KEY", pending.CertificateKeyPem);
        Assert.NotNull(pending.Expires);
        Assert.Equal(_clock.Now, pending.CreatedAt);
        using AsymmetricAlgorithm persistedKey = CsrBuilder.ImportKeyPem(pending.CertificateKeyPem);
        string expectedSpki = Convert.ToHexString(persistedKey.ExportSubjectPublicKeyInfo());

        _dns01.FailCreate = false;
        AcmeCertificateManager second = new(_http, options);
        List<AcmeProgress> events = [];
        second.Progress += events.Add;
        using IssuedCertificate issued = await second.OrderCertificateAsync();

        Assert.Equal(1, _server.RequestLog.Count(r => r == "POST /new-order"));
        Assert.Equal(pending.OrderUrl.ToString(), Assert.Single(events, e => e.Stage == AcmeStage.OrderResumed).Message.Split(' ')[2]);
        Assert.DoesNotContain(events, e => e.Stage == AcmeStage.OrderCreated);
        Assert.Equal(expectedSpki, PublicKeyOf(issued.Certificate));
        Assert.Null(options.AccountStore.Load()!.PendingOrder);
    }

    [Fact]
    public async Task PendingOrder_IsDiscarded_WhenStale()
    {
        AcmeCertificateManagerOptions options = Options();
        AcmeCertificateManager manager = new(_http, options);
        List<AcmeProgress> events = [];
        manager.Progress += events.Add;

        // 1. Different identifiers -> new order without even fetching the old one.
        _dns01.FailCreate = true;
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await manager.OrderCertificateAsync());
        _dns01.FailCreate = false;
        AcmePendingOrder pending = options.AccountStore.Load()!.PendingOrder!;
        AcmeCertificateManager other = new(_http, Options(identifiers: [AcmeIdentifier.Dns("other.example.test")]));
        using (IssuedCertificate o = await other.OrderCertificateAsync())
        {
            Assert.Equal(2, _server.RequestLog.Count(r => r == "POST /new-order"));
            Assert.DoesNotContain("POST /order/" + pending.OrderUrl.Segments[^1], _server.RequestLog);
        }

        // 2. Expired according to the client clock -> new order.
        options.AccountStore.SavePendingOrder(pending with { Expires = _clock.Now.AddSeconds(-1) });
        using (IssuedCertificate o = await manager.OrderCertificateAsync())
        {
            Assert.Equal(3, _server.RequestLog.Count(r => r == "POST /new-order"));
            Assert.Null(options.AccountStore.Load()!.PendingOrder);
        }

        // 3. Unknown at the CA (404) -> new order, with the failure reported.
        events.Clear();
        options.AccountStore.SavePendingOrder(pending with { OrderUrl = new Uri(_server.BaseUri, "order/gone"), Expires = null });
        using (IssuedCertificate o = await manager.OrderCertificateAsync())
        {
            Assert.Equal(4, _server.RequestLog.Count(r => r == "POST /new-order"));
            Assert.Contains(events, e => e.Stage == AcmeStage.OrderResumed && e.Message.Contains("could not be fetched", StringComparison.Ordinal));
            Assert.Contains(events, e => e.Stage == AcmeStage.OrderCreated);
        }

        // 4. Invalid at the CA -> new order.
        events.Clear();
        _server.FailValidation = true;
        AcmeCertificateManager failing = new(_http, options);
        await Assert.ThrowsAsync<AcmeException>(async () => await failing.OrderCertificateAsync());
        Assert.NotNull(options.AccountStore.Load()!.PendingOrder); // the invalid order stays recorded ...
        _server.FailValidation = false;
        using (IssuedCertificate o = await manager.OrderCertificateAsync())
        {
            // ... but is recognised as invalid and replaced.
            Assert.Equal(6, _server.RequestLog.Count(r => r == "POST /new-order"));
            Assert.DoesNotContain(events, e => e.Stage == AcmeStage.OrderResumed && e.Message.StartsWith("Resuming", StringComparison.Ordinal));
        }

        // 5. Identifier comparison is case-insensitive on the value and order-insensitive.
        _dns01.FailCreate = true;
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await manager.OrderCertificateAsync());
        _dns01.FailCreate = false;
        AcmeCertificateManager reordered = new(_http, Options(identifiers: [AcmeIdentifier.Dns("WWW.resume.example.test"), AcmeIdentifier.Dns("resume.example.test")]));
        events.Clear();
        reordered.Progress += events.Add;
        using (IssuedCertificate o = await reordered.OrderCertificateAsync())
        {
            Assert.Contains(events, e => e.Stage == AcmeStage.OrderResumed && e.Message.StartsWith("Resuming", StringComparison.Ordinal));
        }

        // A pending order whose authorization is no longer pending (already invalid) fails fast without touching responders.
        _dns01.FailCreate = true;
        await Assert.ThrowsAsync<InvalidOperationException>(async () => await manager.OrderCertificateAsync());
        _dns01.FailCreate = false;
        _server.AddOverride("/authz/", 200, "application/json", "{\"identifier\":{\"type\":\"dns\",\"value\":\"resume.example.test\"},\"status\":\"deactivated\",\"challenges\":[]}");
        int created = _dns01.Created.Count;
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await manager.OrderCertificateAsync());
        Assert.Equal(AcmeErrorTypes.ValidationFailed, e.Type);
        Assert.Contains("deactivated", e.Message);
        Assert.Equal(created, _dns01.Created.Count);
    }

    [Theory]
    [InlineData(AcmeKeyAlgorithm.ES256)]
    [InlineData(AcmeKeyAlgorithm.RS256)]
    public async Task ReuseKey_KeepsTheCertificateKeyAcrossRenewals(AcmeKeyAlgorithm algorithm)
    {
        string pfx = Path.Combine(_dir, "reuse.pfx");
        AcmeCertificateManager fresh = new(_http, Options(certificatePath: pfx, reuseKey: false, certificateKeyAlgorithm: algorithm));
        string first;
        using (IssuedCertificate a = await fresh.OrderCertificateAsync())
        using (IssuedCertificate b = await fresh.OrderCertificateAsync())
        {
            Assert.NotEqual(PublicKeyOf(a.Certificate), PublicKeyOf(b.Certificate)); // default: new key per order
            first = PublicKeyOf(b.Certificate);
        }

        AcmeCertificateManager reusing = new(_http, Options(certificatePath: pfx, reuseKey: true, certificateKeyAlgorithm: algorithm));
        using (IssuedCertificate c = await reusing.OrderCertificateAsync())
        using (IssuedCertificate d = await reusing.OrderCertificateAsync())
        {
            Assert.Equal(first, PublicKeyOf(c.Certificate));
            Assert.Equal(first, PublicKeyOf(d.Certificate));
            Assert.NotEqual(c.Certificate.Thumbprint, d.Certificate.Thumbprint);
        }

        // With nothing persisted yet, ReuseKey generates a key like the default path.
        string pfx2 = Path.Combine(_dir, "reuse2.pfx");
        AcmeCertificateManager empty = new(_http, Options(certificatePath: pfx2, reuseKey: true, certificateKeyAlgorithm: algorithm));
        using (IssuedCertificate e = await empty.OrderCertificateAsync())
        using (IssuedCertificate f = await empty.OrderCertificateAsync())
        {
            Assert.NotEqual(first, PublicKeyOf(e.Certificate));
            Assert.Equal(PublicKeyOf(e.Certificate), PublicKeyOf(f.Certificate));
        }
    }

    [Fact]
    public async Task Replaces_IsSent_AndRetriedWithoutIt_WhenRejected()
    {
        _server.RenewalInfoEnabled = true;
        AcmeCertificateManager manager = new(_http, Options());
        List<AcmeProgress> events = [];
        manager.Progress += events.Add;

        using IssuedCertificate first = await manager.OrderCertificateAsync();
        string id = AcmeClient.GetAriCertificateId(first.Certificate);
        using IssuedCertificate second = await manager.OrderCertificateAsync(id);
        Assert.Equal([null, id], _server.ReplacesSeen);

        // Replacing the same certificate again is rejected with alreadyReplaced: the manager orders without replaces.
        using IssuedCertificate third = await manager.OrderCertificateAsync(id);
        Assert.Equal([null, id, id, null], _server.ReplacesSeen);
        Assert.Contains(events, e => e.Stage == AcmeStage.OrderCreated && e.Message.Contains("rejected replaces", StringComparison.Ordinal) && e.Message.Contains("alreadyReplaced", StringComparison.Ordinal));

        // Transient failures on the newOrder are not treated as a replaces rejection.
        _server.RateLimitRemaining = 1;
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await manager.OrderCertificateAsync(AcmeClient.GetAriCertificateId(third.Certificate)));
        Assert.Equal(AcmeErrorTypes.RateLimited, e.Type);
    }

    [Fact]
    public async Task InvalidToken_FromTheCa_IsRejectedBeforePublishing()
    {
        AcmeCertificateManager manager = new(_http, Options(identifiers: [AcmeIdentifier.Dns("resume.example.test")]));
        _server.AddOverride("/authz/", 200, "application/json", "{\"identifier\":{\"type\":\"dns\",\"value\":\"resume.example.test\"},\"status\":\"pending\",\"challenges\":[{\"type\":\"dns-01\",\"url\":\"" + _server.BaseUri + "chall/x\",\"status\":\"pending\",\"token\":\"short+token\"}]}");
        AcmeException e = await Assert.ThrowsAsync<AcmeException>(async () => await manager.OrderCertificateAsync());
        Assert.Equal(AcmeErrorTypes.InvalidResponse, e.Type);
        Assert.Contains("[A-Za-z0-9_-]{22,}", e.Message);
        Assert.Empty(_dns01.Created);
    }
}
