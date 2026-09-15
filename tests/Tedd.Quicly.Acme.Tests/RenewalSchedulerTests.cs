using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Acme.Challenges;
using Tedd.Quicly.Acme.Models;
using Tedd.Quicly.Acme.Tests.Fake;

namespace Tedd.Quicly.Acme.Tests;

public sealed class RenewalSchedulerTests : IAsyncDisposable
{
    private readonly FakeAcmeServer _server = new();
    private readonly HttpClient _http = new();
    private readonly FastTimeProvider _clock = new();
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "quicly-acme-tests", Guid.NewGuid().ToString("N"));
    private readonly InMemoryDns01Provider _dns01 = new();

    public RenewalSchedulerTests()
    {
        Directory.CreateDirectory(_dir);
        _server.DnsTxtLookup = _dns01.Lookup;
        _server.Clock = () => _clock.Now; // issued certificates follow the simulated clock
        _server.Start();
    }

    public async ValueTask DisposeAsync()
    {
        _http.Dispose();
        await _server.DisposeAsync();
        Directory.Delete(_dir, true);
    }

    private AcmeCertificateManager NewManager(int maxAttempts = 1, string? certificatePath = null)
    {
        return new AcmeCertificateManager(_http, new AcmeCertificateManagerOptions
        {
            DirectoryUrl = _server.DirectoryUrl,
            AccountStore = new AcmeAccountStore(Path.Combine(_dir, "account.json")),
            Identifiers = [AcmeIdentifier.Dns("renew.example.test")],
            PreferredChallengeTypes = [AcmeChallengeTypes.Dns01],
            Dns01Provider = _dns01,
            CertificatePath = certificatePath,
            ClientOptions = new AcmeClientOptions { TimeProvider = _clock, MaxPollAttempts = 5 },
            Retry = new AcmeRetryOptions { MaxAttempts = maxAttempts, Jitter = 0 },
        });
    }

    /// <summary>Wraps a certificate (with key) issued by <see cref="TestCa"/> as an <see cref="IssuedCertificate"/>.</summary>
    private static IssuedCertificate Wrap(X509Certificate2 withKey)
    {
        using System.Security.Cryptography.ECDsa key = withKey.GetECDsaPrivateKey()!;
        using X509Certificate2 pub = X509CertificateLoader.LoadCertificate(withKey.RawData);
        return IssuedCertificate.Create([pub], key);
    }

    [Fact]
    public void RenewalDecision_Boundaries()
    {
        DateTimeOffset notBefore = new(2026, 10, 2, 0, 0, 0, TimeSpan.Zero);
        DateTimeOffset notAfter = new(2026, 12, 31, 0, 0, 0, TimeSpan.Zero); // 90-day certificate

        // Fixed lead time.
        Assert.Equal(new DateTimeOffset(2026, 12, 1, 0, 0, 0, TimeSpan.Zero), RenewalScheduler.GetRenewalTime(notBefore, notAfter, TimeSpan.FromDays(30)));
        Assert.Equal(notAfter, RenewalScheduler.GetRenewalTime(notBefore, notAfter, TimeSpan.Zero));

        // One third of the lifetime remaining: 90 days -> renew after 60 days.
        Assert.Equal(notBefore.AddDays(60), RenewalScheduler.GetRenewalTime(notBefore, notAfter, null));
        Assert.Equal(notBefore.AddDays(4), RenewalScheduler.GetRenewalTime(notBefore, notBefore.AddDays(6), null));

        // Degenerate lifetimes fall back to notAfter.
        Assert.Equal(notAfter, RenewalScheduler.GetRenewalTime(notAfter, notAfter, null));
        Assert.Equal(notAfter, RenewalScheduler.GetRenewalTime(notAfter.AddDays(1), notAfter, null));
    }

    [Fact]
    public void IsRenewalDue_UsesCertificateAndClock()
    {
        using TestCa ca = new("Renewal Root");
        DateTimeOffset notAfter = new(2026, 12, 31, 0, 0, 0, TimeSpan.Zero);
        using X509Certificate2 cert = ca.IssueLeaf("x.test", notAfter.AddDays(-90), notAfter);
        RenewalScheduler scheduler = new(NewManager(), new RenewalSchedulerOptions { TimeProvider = _clock, RenewBefore = TimeSpan.FromDays(30) });

        Assert.Equal(notAfter.AddDays(-30), scheduler.GetRenewalTime(cert));
        _clock.Now = notAfter.AddDays(-31);
        Assert.False(scheduler.IsRenewalDue(cert));
        _clock.Now = notAfter.AddDays(-30);
        Assert.True(scheduler.IsRenewalDue(cert));
        _clock.Now = notAfter.AddDays(-29);
        Assert.True(scheduler.IsRenewalDue(cert));
        _clock.Now = notAfter.AddDays(5); // already expired
        Assert.True(scheduler.IsRenewalDue(cert));
        Assert.Throws<ArgumentNullException>(() => scheduler.IsRenewalDue(null!));

        // Default rule: one third of the lifetime (30 days of 90).
        RenewalScheduler thirds = new(NewManager(), new RenewalSchedulerOptions { TimeProvider = _clock });
        Assert.Equal(notAfter.AddDays(-30), thirds.GetRenewalTime(cert));
        _clock.Now = notAfter.AddDays(-30).AddTicks(-1);
        Assert.False(thirds.IsRenewalDue(cert));
        _clock.Now = notAfter.AddDays(-30);
        Assert.True(thirds.IsRenewalDue(cert));
    }

    [Fact]
    public void Constructor_ValidatesOptions()
    {
        AcmeCertificateManager manager = NewManager();
        Assert.Throws<ArgumentNullException>(() => new RenewalScheduler(null!));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RenewalScheduler(manager, new RenewalSchedulerOptions { RenewBefore = TimeSpan.FromDays(-1) }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RenewalScheduler(manager, new RenewalSchedulerOptions { CheckInterval = TimeSpan.Zero }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RenewalScheduler(manager, new RenewalSchedulerOptions { RetryDelay = TimeSpan.FromSeconds(-1) }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RenewalScheduler(manager, new RenewalSchedulerOptions { StartupDelay = TimeSpan.FromSeconds(-1) }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RenewalScheduler(manager, new RenewalSchedulerOptions { ImmediateRenewalThreshold = TimeSpan.FromSeconds(-1) }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new RenewalScheduler(manager, new RenewalSchedulerOptions { RenewalInfoRefreshInterval = TimeSpan.Zero }));
        Assert.Throws<ArgumentNullException>(() => new RenewalScheduler(manager, new RenewalSchedulerOptions { TimeProvider = null! }));
        Assert.Throws<ArgumentNullException>(() => new RenewalScheduler(manager, new RenewalSchedulerOptions { Random = null! }));
        RenewalScheduler defaults = new(manager);
        Assert.Null(defaults.Options.RenewBefore);
        Assert.Equal(TimeSpan.FromHours(1), defaults.Options.CheckInterval);
        Assert.Equal(TimeSpan.FromHours(1), defaults.Options.RetryDelay);
        Assert.Equal(TimeSpan.FromHours(1), defaults.Options.StartupDelay);
        Assert.Equal(TimeSpan.FromHours(24), defaults.Options.ImmediateRenewalThreshold);
        Assert.Equal(TimeSpan.FromHours(6), defaults.Options.RenewalInfoRefreshInterval);
        Assert.True(defaults.Options.UseRenewalInfo);
        Assert.Equal(TimeProvider.System, defaults.Options.TimeProvider);
        Assert.Same(Random.Shared, defaults.Options.Random);
    }

    [Fact]
    public async Task RunAsync_WaitsInChunks_ThenRenews_AndInvokesCallback()
    {
        AcmeCertificateManager manager = NewManager();
        RenewalScheduler scheduler = new(manager, new RenewalSchedulerOptions { TimeProvider = _clock, RenewBefore = TimeSpan.FromDays(30), CheckInterval = TimeSpan.FromDays(7) });

        // Current certificate expires in 90 days -> first renewal due after 60 days.
        using TestCa ca = new("Renewal Root");
        DateTimeOffset start = _clock.Now;
        using X509Certificate2 current = ca.IssueLeaf("renew.example.test", start.AddDays(-1), start.AddDays(90));
        using IssuedCertificate currentIssued = Wrap(current);

        List<IssuedCertificate> renewed = [];
        using CancellationTokenSource cts = new();
        Task run = scheduler.RunAsync(currentIssued, (cert, _) =>
        {
            renewed.Add(cert);
            if (renewed.Count == 2)
            {
                cts.Cancel();
            }

            return Task.CompletedTask;
        }, null, cts.Token);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await run);

        Assert.Equal(2, renewed.Count);
        Assert.True(renewed[0].Certificate.MatchesHostname("renew.example.test"));
        // 60 days waited in 7-day chunks (8 x 7d + 1 x 4d), then a fresh 90-day certificate: another 60 days.
        TimeSpan[] chunks = _clock.Delays.Where(d => d >= TimeSpan.FromDays(1)).ToArray();
        Assert.Equal(TimeSpan.FromDays(120), chunks.Aggregate(TimeSpan.Zero, (a, b) => a + b));
        Assert.Equal(TimeSpan.FromDays(7), chunks[0]);
        Assert.Contains(TimeSpan.FromDays(4), chunks);
        Assert.True(_clock.Now >= start.AddDays(120));
        // No ARI advertised: the directory is consulted but no renewalInfo request is made and no replaces is sent.
        Assert.Equal(0, _server.RenewalInfoRequests);
        Assert.All(_server.ReplacesSeen, r => Assert.Null(r));
        foreach (IssuedCertificate c in renewed)
        {
            c.Dispose();
        }
    }

    [Fact]
    public async Task RunAsync_NeverRenewsOnStartup_UnlessExpiringWithin24Hours()
    {
        using TestCa ca = new("Renewal Root");
        DateTimeOffset start = _clock.Now;

        // Overdue (10 days left, 30-day lead) but not expiring within 24 h: deferred until StartupDelay has elapsed.
        using X509Certificate2 overdue = ca.IssueLeaf("renew.example.test", start.AddDays(-80), start.AddDays(10));
        using IssuedCertificate overdueIssued = Wrap(overdue);
        RenewalScheduler scheduler = new(NewManager(), new RenewalSchedulerOptions
        {
            TimeProvider = _clock,
            RenewBefore = TimeSpan.FromDays(30),
            StartupDelay = TimeSpan.FromHours(2),
            CheckInterval = TimeSpan.FromMinutes(45),
        });
        DateTimeOffset? renewedAt = null;
        using CancellationTokenSource cts = new();
        Task run = scheduler.RunAsync(overdueIssued, (cert, _) =>
        {
            renewedAt = _clock.Now;
            cert.Dispose();
            cts.Cancel();
            return Task.CompletedTask;
        }, null, cts.Token);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await run);
        Assert.Equal(start.AddHours(2), renewedAt);
        Assert.Equal([TimeSpan.FromMinutes(45), TimeSpan.FromMinutes(45), TimeSpan.FromMinutes(30)], _clock.Delays.Where(d => d >= TimeSpan.FromMinutes(1)));

        // Expiring within 24 h: renewed immediately on startup.
        _clock.Delays.Clear();
        DateTimeOffset start2 = _clock.Now;
        using X509Certificate2 expiring = ca.IssueLeaf("renew.example.test", start2.AddDays(-89), start2.AddHours(12));
        using IssuedCertificate expiringIssued = Wrap(expiring);
        renewedAt = null;
        using CancellationTokenSource cts2 = new();
        run = scheduler.RunAsync(expiringIssued, (cert, _) =>
        {
            renewedAt = _clock.Now;
            cert.Dispose();
            cts2.Cancel();
            return Task.CompletedTask;
        }, null, cts2.Token);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await run);
        Assert.Equal(start2, renewedAt);
        Assert.DoesNotContain(_clock.Delays, d => d >= TimeSpan.FromMinutes(1));
    }

    [Fact]
    public async Task RunAsync_UsesAriWindow_AndSendsReplaces()
    {
        _server.RenewalInfoEnabled = true;
        _server.RenewalWindowFractions = (0.5, 0.6);
        _server.RenewalInfoRetryAfterSeconds = 3600;
        AcmeCertificateManager manager = NewManager();
        RenewalScheduler scheduler = new(manager, new RenewalSchedulerOptions
        {
            TimeProvider = _clock,
            CheckInterval = TimeSpan.FromDays(1),
            StartupDelay = TimeSpan.Zero,
            Random = new Random(7),
        });

        // Bootstrap a CA-issued certificate (carries an AKI so ARI can identify it).
        using IssuedCertificate first = await manager.OrderCertificateAsync();
        DateTimeOffset notBefore = new(first.Certificate.NotBefore.ToUniversalTime(), TimeSpan.Zero);
        TimeSpan lifetime = first.NotAfter - notBefore;
        string firstId = AcmeClient.GetAriCertificateId(first.Certificate);
        Assert.Equal(firstId, _server.LastIssuedCertificateId);

        List<DateTimeOffset> renewedAt = [];
        using CancellationTokenSource cts = new();
        Task run = scheduler.RunAsync(first, (cert, _) =>
        {
            renewedAt.Add(_clock.Now);
            cert.Dispose();
            if (renewedAt.Count == 2)
            {
                cts.Cancel();
            }

            return Task.CompletedTask;
        }, null, cts.Token);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await run);

        // Renewed inside the suggested window (50–60 % of the lifetime), not at the 1/3-remaining point (66 %).
        Assert.InRange(renewedAt[0], notBefore + lifetime * 0.5, notBefore + lifetime * 0.6);
        Assert.InRange(renewedAt[1] - renewedAt[0], lifetime * 0.5 - TimeSpan.FromDays(1), lifetime * 0.6 + TimeSpan.FromDays(1));
        // The renewal orders carried replaces = the previous certificate's ARI id; the bootstrap order did not.
        Assert.Equal(3, _server.ReplacesSeen.Count);
        Assert.Null(_server.ReplacesSeen[0]);
        Assert.Equal(firstId, _server.ReplacesSeen[1]);
        Assert.NotNull(_server.ReplacesSeen[2]);
        Assert.NotEqual(firstId, _server.ReplacesSeen[2]);
        // ARI was re-fetched at the Retry-After cadence (1 h) while waiting ~45 days, not on every loop.
        Assert.InRange(_server.RenewalInfoRequests, 2, 2 * 60 * 24);
    }

    [Fact]
    public async Task RunAsync_AriFailures_FallBackToLifetimeRule()
    {
        _server.RenewalInfoEnabled = true;
        _server.Ca.IncludeAuthorityKeyIdentifier = false; // ARI cannot identify the certificate -> ArgumentException inside the lookup
        AcmeCertificateManager manager = NewManager();
        RenewalScheduler scheduler = new(manager, new RenewalSchedulerOptions { TimeProvider = _clock, CheckInterval = TimeSpan.FromDays(1), StartupDelay = TimeSpan.Zero });
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
        Assert.InRange(renewedAt!.Value, notBefore + lifetime * 2 / 3 - TimeSpan.FromSeconds(1), notBefore + lifetime * 2 / 3 + TimeSpan.FromDays(1));
        Assert.Equal(0, _server.RenewalInfoRequests);
        Assert.Null(_server.ReplacesSeen[^1]);

        // A server-side ARI failure (404 for an unknown certificate) also falls back, and UseRenewalInfo=false skips ARI entirely.
        _server.Ca.IncludeAuthorityKeyIdentifier = true;
        using IssuedCertificate second = await manager.OrderCertificateAsync();
        _server.AddOverride("/renewal-info/", 500, "application/problem+json", "{\"type\":\"urn:ietf:params:acme:error:serverInternal\",\"detail\":\"boom\"}");
        DateTimeOffset secondNotBefore = new(second.Certificate.NotBefore.ToUniversalTime(), TimeSpan.Zero);
        renewedAt = null;
        using CancellationTokenSource cts2 = new();
        run = scheduler.RunAsync(second, (cert, _) =>
        {
            renewedAt = _clock.Now;
            cert.Dispose();
            cts2.Cancel();
            return Task.CompletedTask;
        }, null, cts2.Token);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await run);
        // The first lookup failed (fallback), later lookups succeed and move the renewal into the ARI window (50–60 %).
        Assert.InRange(renewedAt!.Value, secondNotBefore + lifetime * 0.5, secondNotBefore + lifetime * 0.6 + TimeSpan.FromDays(1));
        Assert.True(_server.RenewalInfoRequests >= 2);

        int requestsBefore = _server.RenewalInfoRequests;
        RenewalScheduler noAri = new(manager, new RenewalSchedulerOptions { TimeProvider = _clock, CheckInterval = TimeSpan.FromDays(1), StartupDelay = TimeSpan.Zero, UseRenewalInfo = false });
        using IssuedCertificate third = await manager.OrderCertificateAsync();
        using CancellationTokenSource cts3 = new();
        run = noAri.RunAsync(third, (cert, _) =>
        {
            cert.Dispose();
            cts3.Cancel();
            return Task.CompletedTask;
        }, null, cts3.Token);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await run);
        Assert.Equal(requestsBefore, _server.RenewalInfoRequests);
        Assert.Null(_server.ReplacesSeen[^1]);
    }

    [Fact]
    public async Task RunAsync_WithoutCurrentCertificate_OrdersImmediately_AndRetriesAfterFailure()
    {
        AcmeCertificateManager manager = NewManager();
        RenewalScheduler scheduler = new(manager, new RenewalSchedulerOptions { TimeProvider = _clock, RetryDelay = TimeSpan.FromMinutes(15) });

        _server.RateLimitRemaining = 3; // the first requests fail
        List<Exception> errors = [];
        List<IssuedCertificate> renewed = [];
        using CancellationTokenSource cts = new();
        Task run = scheduler.RunAsync(null, (cert, _) =>
        {
            renewed.Add(cert);
            cts.Cancel();
            return Task.CompletedTask;
        }, errors.Add, cts.Token);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await run);
        Assert.Single(renewed);
        Assert.NotEmpty(errors);
        Assert.All(errors, e => Assert.IsType<AcmeException>(e));
        Assert.Equal(errors.Count, _clock.Delays.Count(d => d == TimeSpan.FromMinutes(15)));
        renewed[0].Dispose();
    }

    [Fact]
    public async Task RunAsync_CallbackFailure_IsReported_AndDoesNotReorder()
    {
        AcmeCertificateManager manager = NewManager();
        RenewalScheduler scheduler = new(manager, new RenewalSchedulerOptions { TimeProvider = _clock, RetryDelay = TimeSpan.FromMinutes(1), CheckInterval = TimeSpan.FromDays(30) });
        List<Exception> errors = [];
        int calls = 0;
        using CancellationTokenSource cts = new();
        Task run = scheduler.RunAsync(null, (cert, _) =>
        {
            calls++;
            cert.Dispose();
            throw new InvalidOperationException("consumer failed");
        }, e =>
        {
            errors.Add(e);
            cts.Cancel();
        }, cts.Token);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await run);
        Assert.Equal(1, calls);
        Assert.IsType<InvalidOperationException>(Assert.Single(errors));
        Assert.Equal(1, _server.RequestLog.Count(r => r == "POST /new-order"));
    }

    [Fact]
    public async Task RunAsync_AlreadyCancelled_ThrowsImmediately()
    {
        RenewalScheduler scheduler = new(NewManager(), new RenewalSchedulerOptions { TimeProvider = _clock });
        using CancellationTokenSource cts = new();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await scheduler.RunAsync(null, (_, _) => Task.CompletedTask, null, cts.Token));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await scheduler.RunAsync(null, null!, null, CancellationToken.None));
        Assert.Empty(_server.RequestLog);
    }

    [Fact]
    public async Task RunAsync_CancellationDuringOrder_PropagatesWithoutErrorCallback()
    {
        AcmeCertificateManager manager = NewManager();
        RenewalScheduler scheduler = new(manager, new RenewalSchedulerOptions { TimeProvider = _clock });
        using CancellationTokenSource cts = new();
        manager.Progress += p =>
        {
            if (p.Stage == AcmeStage.OrderCreated)
            {
                cts.Cancel(); // the next HTTP request observes the cancelled token
            }
        };

        List<Exception> errors = [];
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await scheduler.RunAsync(null, (_, _) => Task.CompletedTask, errors.Add, cts.Token));
        Assert.Empty(errors);
        Assert.Equal(1, _server.RequestLog.Count(r => r == "POST /new-order"));
    }

    [Fact]
    public async Task RunAsync_CancelFromErrorCallback_StopsLoop()
    {
        AcmeCertificateManager manager = NewManager();
        RenewalScheduler scheduler = new(manager, new RenewalSchedulerOptions { TimeProvider = _clock });
        List<Exception> errors = [];
        using CancellationTokenSource cts = new();
        _server.RateLimitRemaining = int.MaxValue;
        Task run = scheduler.RunAsync(null, (_, _) => Task.CompletedTask, e =>
        {
            errors.Add(e);
            cts.Cancel();
        }, cts.Token);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await run);
        Assert.Single(errors);
    }
}
