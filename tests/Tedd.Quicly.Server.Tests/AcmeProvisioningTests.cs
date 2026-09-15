using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Acme;
using Tedd.Quicly.Acme.Models;
using Tedd.Quicly.Http.Handlers;
using Tedd.Quicly.Server.Certificates;
using Tedd.Quicly.Testing.Acme;

namespace Tedd.Quicly.Server.Tests;

/// <summary>End-to-end ACME provisioning against the fake CA, validating through the provisioner's real endpoints.</summary>
public sealed class AcmeProvisioningTests : IAsyncDisposable
{
    private static readonly TimeSpan LongWait = TimeSpan.FromSeconds(30);
    private readonly AcmeTestEnvironment _env = new();

    public ValueTask DisposeAsync() => _env.DisposeAsync();

    [Fact]
    public async Task Http01_IssuesThroughTheRealChallengeEndpoint()
    {
        AcmeProvisioningOptions options = _env.Options(AcmeChallengeKind.Http01);
        options.EnableHealthEndpoint = true;
        options.RedirectHttpsPort = 8443;
        CertificateProvisioner provisioner = _env.Create(options);
        Recorder recorder = new(provisioner);

        await provisioner.StartAsync();

        Assert.Equal(CertificateState.Valid, provisioner.Status.State);
        Assert.NotNull(provisioner.Current);
        X509Certificate2 current = provisioner.Current;
        Assert.True(current.HasPrivateKey);
        Assert.True(current.MatchesHostname("game.example.test"));
        FakeAcmeValidation validation = Assert.Single(_env.Ca.ValidationLog);
        Assert.Equal(AcmeChallengeTypes.Http01, validation.ChallengeType);
        Assert.True(validation.Succeeded, validation.Error);
        Assert.Equal(0, provisioner.Http01Challenges!.Count);
        Assert.Null(provisioner.TlsEndPoint);
        Assert.True(File.Exists(_env.CertificatePath));
        Assert.Same(current, Assert.Single(recorder.Changed));
        Assert.Same(current, await provisioner.WaitForCertificateAsync());
        Assert.Equal(CertificateState.Starting, recorder.Statuses[0].State);
        Assert.True(recorder.HasStatus(CertificateState.Starting, "http-01 on 127.0.0.1:"));
        Assert.Contains(recorder.Progress, p => p.Stage == AcmeStage.ChallengeCleanedUp);

        // The challenge port also serves health and redirects everything else to HTTPS.
        int port = provisioner.HttpChallengeEndPoint!.Port;
        using HttpClient client = Certs.CreateInsecureClient();
        using HttpResponseMessage health = await client.GetAsync("http://127.0.0.1:" + port + "/healthz");
        Assert.Equal(HttpStatusCode.OK, health.StatusCode);
        Assert.Equal("ok", await health.Content.ReadAsStringAsync());
        using HttpResponseMessage redirect = await client.GetAsync("http://127.0.0.1:" + port + "/lobby?x=1");
        Assert.Equal(HttpStatusCode.MovedPermanently, redirect.StatusCode);
        Assert.Equal("https://127.0.0.1:8443/lobby?x=1", redirect.Headers.Location!.ToString());
        using HttpResponseMessage unknown = await client.GetAsync("http://127.0.0.1:" + port + "/.well-known/acme-challenge/" + new string('A', 43));
        Assert.Equal(HttpStatusCode.NotFound, unknown.StatusCode);

        // Disposal releases every certificate the provisioner created.
        await provisioner.DisposeAsync();
        Assert.True(Certs.IsDisposed(current));
        Assert.Null(provisioner.Current);
        Assert.Equal(CertificateState.Stopped, provisioner.Status.State);
    }

    [Fact]
    public async Task TlsAlpn01_IssuesThroughTheRealTlsEndpoint_AndServesTheCurrentCertificateToOtherClients()
    {
        AcmeProvisioningOptions options = _env.Options(AcmeChallengeKind.TlsAlpn01);
        options.EnableHealthEndpoint = true;
        options.TlsEndpointHandlers.Add(new HealthHandler("/custom"));
        CertificateProvisioner provisioner = _env.Create(options);

        await provisioner.StartAsync();

        Assert.Equal(CertificateState.Valid, provisioner.Status.State);
        FakeAcmeValidation validation = Assert.Single(_env.Ca.ValidationLog);
        Assert.Equal(AcmeChallengeTypes.TlsAlpn01, validation.ChallengeType);
        Assert.True(validation.Succeeded, validation.Error);
        Assert.Equal(0, provisioner.TlsAlpn01Challenges!.Count);
        Assert.Null(provisioner.HttpChallengeEndPoint);

        using X509Certificate2 served = await Certs.GetServedCertificateAsync(provisioner.TlsEndPoint!, "game.example.test");
        Assert.Equal(provisioner.Current!.Thumbprint, served.Thumbprint);

        // The TLS endpoint serves only TlsEndpointHandlers; EnableHealthEndpoint applies to the HTTP challenge endpoint.
        using HttpClient client = Certs.CreateInsecureClient();
        using HttpResponseMessage custom = await client.GetAsync("https://127.0.0.1:" + provisioner.TlsEndPoint!.Port + "/custom");
        Assert.Equal(HttpStatusCode.OK, custom.StatusCode);
        using HttpResponseMessage health = await client.GetAsync("https://127.0.0.1:" + provisioner.TlsEndPoint!.Port + "/healthz");
        Assert.Equal(HttpStatusCode.NotFound, health.StatusCode);
    }

    [Fact]
    public async Task TlsAlpn01_ForIpIdentifiers_AnswersTheReverseDnsServerName()
    {
        AcmeProvisioningOptions options = _env.Options(AcmeChallengeKind.TlsAlpn01);
        options.DnsNames.Clear();
        options.IpAddresses.Add(IPAddress.Loopback);
        options.IpAddresses.Add(IPAddress.IPv6Loopback);
        CertificateProvisioner provisioner = _env.Create(options);

        await provisioner.StartAsync();

        Assert.Equal(CertificateState.Valid, provisioner.Status.State);
        Assert.Equal(2, _env.Ca.ValidationLog.Count);
        Assert.All(_env.Ca.ValidationLog, v =>
        {
            Assert.True(v.Succeeded, v.Error);
            Assert.True(v.Identifier.IsIp);
        });
        Assert.Equal(0, provisioner.TlsAlpn01Challenges!.Count);
        X509SubjectAlternativeNameExtension san = provisioner.Current!.Extensions.OfType<X509SubjectAlternativeNameExtension>().Single();
        Assert.Equal([IPAddress.Loopback, IPAddress.IPv6Loopback], san.EnumerateIPAddresses().ToArray());
    }

    [Fact]
    public async Task Dns01_WithAnInMemoryProvider_CoversWildcards_AndRemovesTheRecords()
    {
        AcmeProvisioningOptions options = _env.Options(AcmeChallengeKind.Dns01);
        options.DnsNames.Add("*.game.example.test");
        options.ChallengePropagationDelay = TimeSpan.FromMilliseconds(10);
        options.HttpClient = null; // the provisioner creates (and disposes) its own client
        CertificateProvisioner provisioner = _env.Create(options);

        await provisioner.StartAsync();

        Assert.Equal(CertificateState.Valid, provisioner.Status.State);
        Assert.Equal(2, _env.Ca.ValidationLog.Count);
        Assert.All(_env.Ca.ValidationLog, v => Assert.Equal(AcmeChallengeTypes.Dns01, v.ChallengeType));
        Assert.Equal(2, _env.Dns.Created.Count);
        Assert.Equal(2, _env.Dns.Removed.Count);
        Assert.All(_env.Dns.Records.Values, values => Assert.Empty(values));
        Assert.Null(provisioner.HttpChallengeEndPoint);
        Assert.Null(provisioner.TlsEndPoint);
        Assert.True(provisioner.Current!.MatchesHostname("lobby.game.example.test"));
    }

    [Fact]
    public async Task PersistedCertificate_IsReusedOnRestart_WithoutContactingTheCa()
    {
        CertificateProvisioner first = _env.Create(_env.Options());
        await first.StartAsync();
        string thumbprint = first.Current!.Thumbprint;
        await first.DisposeAsync();
        int requests = _env.Ca.RequestLog.Count;

        CertificateProvisioner second = _env.Create(_env.Options());
        Recorder recorder = new(second);
        await second.StartAsync();

        Assert.Equal(CertificateState.Valid, second.Status.State);
        Assert.Contains("persisted", second.Status.Reason, StringComparison.Ordinal);
        Assert.Equal(thumbprint, second.Current!.Thumbprint);
        Assert.Single(recorder.Changed);
        await Task.Delay(200); // the renewal loop is running and must stay quiet
        Assert.Equal(requests, _env.Ca.RequestLog.Count);
        Assert.Equal(1, _env.Ca.IssuedCount);
    }

    [Fact]
    public async Task PersistedCertificate_ThatNoLongerCoversTheNames_IsReplaced()
    {
        CertificateProvisioner first = _env.Create(_env.Options());
        await first.StartAsync();
        await first.DisposeAsync();

        AcmeProvisioningOptions options = _env.Options();
        options.DnsNames.Add("lobby.game.example.test");
        CertificateProvisioner second = _env.Create(options);
        Recorder recorder = new(second);
        await second.StartAsync();

        Assert.Equal(CertificateState.Valid, second.Status.State);
        Assert.Equal(2, _env.Ca.IssuedCount);
        Assert.True(second.Current!.MatchesHostname("lobby.game.example.test"));
        Assert.True(recorder.HasStatus(CertificateState.Starting, "does not cover"));
    }

    [Fact]
    public async Task PersistedCertificate_DueForRenewal_IsServedWhileAReplacementIsOrdered()
    {
        CertificateProvisioner first = _env.Create(_env.Options());
        await first.StartAsync();
        string oldThumbprint = first.Current!.Thumbprint;
        await first.DisposeAsync();

        // 70 days later a 90-day certificate is past the one-third-remaining mark.
        OffsetTimeProvider later = new(TimeSpan.FromDays(70));
        _env.Ca.Clock = later.GetUtcNow;
        AcmeProvisioningOptions options = _env.Options();
        options.Renewal = new RenewalSchedulerOptions { UseRenewalInfo = false, TimeProvider = later, RetryDelay = TimeSpan.FromHours(1) };
        CertificateProvisioner second = _env.Create(options);
        Recorder recorder = new(second);
        await second.StartAsync();

        Assert.Equal(CertificateState.Valid, second.Status.State);
        X509Certificate2[] changed = recorder.Changed;
        Assert.Equal(2, changed.Length);
        Assert.Equal(oldThumbprint, changed[0].Thumbprint);
        Assert.NotEqual(oldThumbprint, changed[1].Thumbprint);
        Assert.Same(changed[1], second.Current);
        Assert.True(recorder.HasStatus(CertificateState.Starting, "is due for renewal"));
        Assert.Equal(2, _env.Ca.IssuedCount);
    }

    [Fact]
    public async Task PersistedCertificate_ThatExpired_IsReplaced()
    {
        CertificateProvisioner first = _env.Create(_env.Options());
        await first.StartAsync();
        await first.DisposeAsync();

        OffsetTimeProvider later = new(TimeSpan.FromDays(100));
        _env.Ca.Clock = later.GetUtcNow;
        AcmeProvisioningOptions options = _env.Options();
        options.Renewal = new RenewalSchedulerOptions { UseRenewalInfo = false, TimeProvider = later, RetryDelay = TimeSpan.FromHours(1) };
        CertificateProvisioner second = _env.Create(options);
        Recorder recorder = new(second);
        await second.StartAsync();

        Assert.Equal(CertificateState.Valid, second.Status.State);
        Assert.Single(recorder.Changed);
        Assert.True(recorder.HasStatus(CertificateState.Starting, "has expired"));
    }

    [Theory]
    [InlineData("garbage")]
    [InlineData("no-private-key")]
    [InlineData("no-subject-alternative-name")]
    public async Task UnusablePersistedCertificate_IsReplaced(string kind)
    {
        byte[] bytes = kind switch
        {
            "garbage" => "not a pfx"u8.ToArray(),
            "no-private-key" => Certs.PublicOnlyPfx("game.example.test"),
            _ => Certs.Pfx(null),
        };
        File.WriteAllBytes(_env.CertificatePath, bytes);
        CertificateProvisioner provisioner = _env.Create(_env.Options());
        Recorder recorder = new(provisioner);

        await provisioner.StartAsync();

        Assert.Equal(CertificateState.Valid, provisioner.Status.State);
        Assert.Equal(1, _env.Ca.IssuedCount);
        if (kind == "no-subject-alternative-name")
        {
            Assert.True(recorder.HasStatus(CertificateState.Starting, "does not cover"));
        }
        else
        {
            Assert.Contains(recorder.Errors, e => e is InvalidDataException { InnerException: CryptographicException });
            Assert.True(recorder.HasStatus(CertificateState.Starting, "cannot be loaded"));
        }
    }

    [Fact]
    public async Task ShortLivedCertificate_IsRenewed_AndHotSwappedIntoEveryConsumer()
    {
        _env.Ca.CertificateLifetime = TimeSpan.FromSeconds(3);
        _env.Ca.CertificateBackdate = TimeSpan.Zero;
        AcmeProvisioningOptions options = _env.Options();
        options.Renewal = new RenewalSchedulerOptions { UseRenewalInfo = false, CheckInterval = TimeSpan.FromMilliseconds(100), RetryDelay = TimeSpan.FromHours(1) };
        CertificateProvisioner provisioner = _env.Create(options);
        Recorder recorder = new(provisioner);
        RecordingConsumer quic = new();
        RecordingConsumer https = new();
        await using CertificateBinder binder = new(provisioner, [quic, https], new CertificateBinderOptions { SupersededCertificateGracePeriod = TimeSpan.FromMilliseconds(300) });

        await provisioner.StartAsync();
        X509Certificate2 first = provisioner.Current!;
        string firstThumbprint = first.Thumbprint;
        Assert.Same(first, Assert.Single(quic.Received));
        Assert.Same(first, Assert.Single(https.Received));

        await Wait.ForAsync(() => quic.Received.Length >= 2 && https.Received.Length >= 2, LongWait, "the renewal to reach both consumers");
        X509Certificate2 second = quic.Received[1];
        Assert.Same(second, https.Received[1]);
        Assert.Same(second, provisioner.Current);
        Assert.NotEqual(firstThumbprint, second.Thumbprint);
        Assert.Contains(recorder.Statuses, s => s.State == CertificateState.Renewing);
        await Wait.ForAsync(() => recorder.HasStatus(CertificateState.Valid, "Renewed: "), LongWait, "the Valid status after renewal");

        // The superseded certificate survives its grace period, then is disposed; the current one is not.
        await Wait.ForAsync(() => Certs.IsDisposed(first), LongWait, "the superseded certificate to be disposed");
        Assert.False(Certs.IsDisposed(second));
        Assert.Equal(2, _env.Ca.IssuedCount);
    }

    [Fact]
    public async Task RenewNowAsync_OrdersAReplacement_AndReportsFailures()
    {
        CertificateProvisioner provisioner = _env.Create(_env.Options());
        await provisioner.StartAsync();
        X509Certificate2 first = provisioner.Current!;

        await provisioner.RenewNowAsync();

        Assert.NotSame(first, provisioner.Current);
        Assert.Equal(CertificateState.Valid, provisioner.Status.State);
        Assert.Contains("on request", provisioner.Status.Reason, StringComparison.Ordinal);
        Assert.Equal(2, _env.Ca.IssuedCount);

        _env.Ca.FailValidation = true;
        X509Certificate2 second = provisioner.Current!;
        await Assert.ThrowsAsync<AcmeException>(() => provisioner.RenewNowAsync());
        Assert.Equal(CertificateState.Failed, provisioner.Status.State);
        Assert.Same(second, provisioner.Current);
    }

    [Fact]
    public async Task CaUnavailable_ThenAvailable_RetriesWithBackOffUntilValid()
    {
        _env.Ca.UnavailableRequestsRemaining = 2;
        AcmeProvisioningOptions options = _env.Options();
        options.Retry = new AcmeRetryOptions { MaxAttempts = 5, InitialDelay = TimeSpan.FromMilliseconds(20), MaxDelay = TimeSpan.FromMilliseconds(100), Jitter = 0 };
        CertificateProvisioner provisioner = _env.Create(options);
        Recorder recorder = new(provisioner);

        await provisioner.StartAsync();

        Assert.Equal(CertificateState.Valid, provisioner.Status.State);
        Assert.Contains(recorder.Progress, p => p.Stage == AcmeStage.Retrying);
        Assert.True(recorder.HasStatus(CertificateState.Starting, "Retrying after a transient CA failure"));
    }

    [Fact]
    public async Task CaUnavailableBeyondTheRetryBudget_ReportsFailed_ThenRecoversInTheBackground()
    {
        _env.Ca.UnavailableRequestsRemaining = 3;
        AcmeProvisioningOptions options = _env.Options();
        options.Retry = new AcmeRetryOptions { MaxAttempts = 1 };
        options.Renewal = new RenewalSchedulerOptions { UseRenewalInfo = false, RetryDelay = TimeSpan.FromMilliseconds(250) };
        CertificateProvisioner provisioner = _env.Create(options);
        Recorder recorder = new(provisioner);

        await provisioner.StartAsync();

        Assert.Equal(CertificateState.Failed, provisioner.Status.State);
        Assert.IsType<AcmeException>(provisioner.Status.Error);
        Assert.Null(provisioner.Current);

        using CancellationTokenSource timeout = new(LongWait);
        X509Certificate2 certificate = await provisioner.WaitForCertificateAsync(timeout.Token);
        Assert.Same(provisioner.Current, certificate);
        await Wait.ForAsync(() => provisioner.Status.State == CertificateState.Valid, LongWait, "Valid after recovery");
        Assert.True(recorder.HasStatus(CertificateState.Failed, "Renewal failed"));
        Assert.Contains(recorder.Statuses, s => s.State == CertificateState.Renewing);
    }

    [Theory]
    [InlineData(AcmeChallengeKind.Http01)]
    [InlineData(AcmeChallengeKind.TlsAlpn01)]
    [InlineData(AcmeChallengeKind.Dns01)]
    public async Task ChallengeFailure_ReportsFailed_AndLeavesNoChallengeMaterialBehind(AcmeChallengeKind kind)
    {
        _env.Ca.FailValidation = true;
        CertificateProvisioner provisioner = _env.Create(_env.Options(kind));
        Recorder recorder = new(provisioner);

        await provisioner.StartAsync();

        Assert.Equal(CertificateState.Failed, provisioner.Status.State);
        Assert.StartsWith("Ordering a certificate from", provisioner.Status.Reason, StringComparison.Ordinal);
        Assert.IsType<AcmeException>(provisioner.Status.Error);
        Assert.Null(provisioner.Current);
        Assert.Contains(recorder.Progress, p => p.Stage == AcmeStage.ChallengeCleanedUp);
        switch (kind)
        {
            case AcmeChallengeKind.Http01:
                Assert.Equal(0, provisioner.Http01Challenges!.Count);
                break;
            case AcmeChallengeKind.TlsAlpn01:
                Assert.Equal(0, provisioner.TlsAlpn01Challenges!.Count);
                break;
            default:
                Assert.Equal(_env.Dns.Created.Count, _env.Dns.Removed.Count);
                Assert.All(_env.Dns.Records.Values, values => Assert.Empty(values));
                break;
        }
    }

    [Fact]
    public async Task Http01ValidatedAgainstTheWrongServer_FailsWithTheCasReason()
    {
        CertificateProvisioner provisioner = _env.Create(_env.Options(), wireValidationPorts: false);
        _env.Ca.Http01ValidationPort = _env.Ca.BaseUri.Port; // the CA's own port answers instead of the challenge endpoint

        await provisioner.StartAsync();

        Assert.Equal(CertificateState.Failed, provisioner.Status.State);
        FakeAcmeValidation validation = Assert.Single(_env.Ca.ValidationLog);
        Assert.False(validation.Succeeded);
        Assert.Contains("HTTP", validation.Error, StringComparison.Ordinal);
        Assert.Equal(0, provisioner.Http01Challenges!.Count);
    }

    [Fact]
    public async Task ExternalAccountBinding_AndBareContacts_ReachTheCa()
    {
        _env.Ca.RequireEab = true;
        AcmeProvisioningOptions options = _env.Options();
        options.ExternalAccountKeyId = _env.Ca.EabKid;
        options.ExternalAccountHmacKey = _env.Ca.EabHmacKeyBase64Url;
        options.AccountStoreProtection = null; // platform default
        CertificateProvisioner provisioner = _env.Create(options);

        await provisioner.StartAsync();

        Assert.Equal(CertificateState.Valid, provisioner.Status.State);
        AcmeAccountState state = new AcmeAccountStore(_env.AccountPath).Load()!;
        Assert.Equal(["mailto:ops@example.test"], _env.Ca.GetAccountContacts(state.AccountUrl)!);
    }

    [Fact]
    public async Task RenewedCertificateThatFailsToLoadOnce_IsLoadedAgain_WithoutAnotherOrder()
    {
        _env.Ca.CertificateLifetime = TimeSpan.FromSeconds(3);
        _env.Ca.CertificateBackdate = TimeSpan.Zero;
        AcmeProvisioningOptions options = _env.Options();
        options.Renewal = new RenewalSchedulerOptions { UseRenewalInfo = false, CheckInterval = TimeSpan.FromMilliseconds(100), RetryDelay = TimeSpan.FromHours(1) };
        CertificateProvisioner provisioner = _env.Create(options);
        int loads = 0;
        provisioner.CertificateLoader = (pfx, password, flags) => Interlocked.Increment(ref loads) == 2
            ? throw new CryptographicException("simulated key storage failure")
            : CertificateProvisioner.LoadServedCertificate(pfx, password, flags);
        Recorder recorder = new(provisioner);
        int issuedWhenRenewed = -1;
        provisioner.StatusChanged += s =>
        {
            if (s.State == CertificateState.Valid && s.Reason!.StartsWith("Renewed: ", StringComparison.Ordinal))
            {
                Interlocked.CompareExchange(ref issuedWhenRenewed, _env.Ca.IssuedCount, -1);
            }
        };

        await provisioner.StartAsync();
        X509Certificate2 first = provisioner.Current!;

        await Wait.ForAsync(() => Volatile.Read(ref issuedWhenRenewed) >= 0, LongWait, "the renewal to be served");
        Assert.True(recorder.HasStatus(CertificateState.Renewing, "could not be loaded (simulated key storage failure); loading it again in "));
        Assert.Equal(2, issuedWhenRenewed); // the load was retried; nothing was ordered again
        Assert.NotSame(first, provisioner.Current);
    }

    [Fact]
    public async Task RenewedCertificateThatKeepsFailingToLoad_IsKept_AndLoadedAfterTheRetryDelay_BeforeAnythingIsOrdered()
    {
        _env.Ca.CertificateLifetime = TimeSpan.FromSeconds(3);
        _env.Ca.CertificateBackdate = TimeSpan.Zero;
        AcmeProvisioningOptions options = _env.Options(); // three load attempts per round
        options.Renewal = new RenewalSchedulerOptions { UseRenewalInfo = false, CheckInterval = TimeSpan.FromMilliseconds(100), RetryDelay = TimeSpan.FromMilliseconds(200) };
        CertificateProvisioner provisioner = _env.Create(options);
        int loads = 0;
        provisioner.CertificateLoader = (pfx, password, flags) => Interlocked.Increment(ref loads) is >= 2 and <= 4
            ? throw new CryptographicException("simulated key storage failure")
            : CertificateProvisioner.LoadServedCertificate(pfx, password, flags);
        Recorder recorder = new(provisioner);
        int issuedWhenLoaded = -1;
        provisioner.StatusChanged += s =>
        {
            if (s.State == CertificateState.Valid && s.Reason!.StartsWith("Loaded the certificate issued earlier", StringComparison.Ordinal))
            {
                Interlocked.CompareExchange(ref issuedWhenLoaded, _env.Ca.IssuedCount, -1);
            }
        };

        await provisioner.StartAsync();

        await Wait.ForAsync(() => Volatile.Read(ref issuedWhenLoaded) >= 0, LongWait, "the kept certificate to be served");
        Assert.True(recorder.HasStatus(CertificateState.Failed, "A renewed certificate could not be loaded (simulated key storage failure); loading it again after 00:00:00.2000000, before anything is ordered."));
        Assert.Equal(2, issuedWhenLoaded);
    }

    [Fact]
    public async Task StopDuringStartUp_CancelsTheOrder_AndReleasesTheEndpoints()
    {
        _env.Ca.UnavailableRequestsRemaining = int.MaxValue;
        AcmeProvisioningOptions options = _env.Options();
        options.Retry = new AcmeRetryOptions { MaxAttempts = 100, InitialDelay = TimeSpan.FromMilliseconds(200), MaxDelay = TimeSpan.FromMilliseconds(200), Jitter = 0 };
        CertificateProvisioner provisioner = _env.Create(options);
        Recorder recorder = new(provisioner);

        Task start = provisioner.StartAsync();
        await Wait.ForAsync(() => recorder.Progress.Any(p => p.Stage == AcmeStage.Retrying), LongWait, "a retry");
        Assert.NotNull(provisioner.HttpChallengeEndPoint);

        await provisioner.StopAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => start);
        Assert.Equal(CertificateState.Stopped, provisioner.Status.State);
        Assert.Null(provisioner.HttpChallengeEndPoint);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => provisioner.WaitForCertificateAsync());
        InvalidOperationException restart = await Assert.ThrowsAsync<InvalidOperationException>(() => provisioner.StartAsync());
        Assert.Contains("stopped", restart.Message, StringComparison.Ordinal);
        await Assert.ThrowsAsync<InvalidOperationException>(() => provisioner.RenewNowAsync());
    }

    [Fact]
    public async Task StartAsync_WithACancelledToken_Throws_AndReleasesTheEndpoints()
    {
        CertificateProvisioner provisioner = _env.Create(_env.Options(AcmeChallengeKind.Http01, AcmeChallengeKind.TlsAlpn01));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => provisioner.StartAsync(new CancellationToken(canceled: true)));

        Assert.Equal(CertificateState.Stopped, provisioner.Status.State);
        Assert.Null(provisioner.HttpChallengeEndPoint);
        Assert.Null(provisioner.TlsEndPoint);
    }

    [Fact]
    public async Task EndpointThatCannotBeBound_FailsStartUp_AndReleasesEverything()
    {
        using Socket blocker = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        blocker.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        blocker.Listen();
        AcmeProvisioningOptions options = _env.Options(AcmeChallengeKind.Http01, AcmeChallengeKind.TlsAlpn01);
        options.TlsAlpnEndpoint = (IPEndPoint)blocker.LocalEndPoint!;
        CertificateProvisioner provisioner = _env.Create(options);

        await Assert.ThrowsAsync<SocketException>(() => provisioner.StartAsync());

        Assert.Equal(CertificateState.Failed, provisioner.Status.State);
        Assert.StartsWith("Start-up failed", provisioner.Status.Reason, StringComparison.Ordinal);
        Assert.Null(provisioner.HttpChallengeEndPoint);
        Assert.Null(provisioner.TlsEndPoint);
    }
}
