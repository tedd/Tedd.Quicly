using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Acme;
using Tedd.Quicly.Server.Certificates;
using Tedd.Quicly.Testing.Acme;

namespace Tedd.Quicly.Server.Tests;

/// <summary>The background loop: retries, on-demand renewals, and how they interact with waits, the scheduler and stopping.</summary>
public sealed class RenewalLoopTests : IAsyncDisposable
{
    private static readonly TimeSpan LongWait = TimeSpan.FromSeconds(30);
    private readonly AcmeTestEnvironment _env = new();

    public ValueTask DisposeAsync() => _env.DisposeAsync();

    [Fact]
    public async Task RenewNowAsync_CutsTheRetryWaitShort_AfterAFailedStartUp()
    {
        _env.Ca.FailValidation = true;
        CertificateProvisioner provisioner = _env.Create(_env.Options()); // RetryDelay: one hour
        Recorder recorder = new(provisioner);

        await provisioner.StartAsync();

        Assert.Equal(CertificateState.Failed, provisioner.Status.State);
        Assert.EndsWith("Ordering again after 01:00:00.", provisioner.Status.Reason, StringComparison.Ordinal);
        Assert.Null(provisioner.Current);

        _env.Ca.FailValidation = false;
        await provisioner.RenewNowAsync().WaitAsync(LongWait);

        Assert.Equal(CertificateState.Valid, provisioner.Status.State);
        Assert.StartsWith("Obtained ", provisioner.Status.Reason, StringComparison.Ordinal);
        Assert.Same(provisioner.Current, await provisioner.WaitForCertificateAsync());
        Assert.True(recorder.HasStatus(CertificateState.Renewing, "Ordering again after the start-up order failed."));
    }

    [Fact]
    public async Task ConcurrentRenewNowAsyncCalls_ShareOneOrder()
    {
        CertificateProvisioner provisioner = _env.Create(_env.Options());
        await provisioner.StartAsync();
        X509Certificate2 first = provisioner.Current!;

        Task a = provisioner.RenewNowAsync();
        Task b = provisioner.RenewNowAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => provisioner.RenewNowAsync(new CancellationToken(canceled: true)));
        await Task.WhenAll(a, b).WaitAsync(LongWait);

        Assert.Equal(2, _env.Ca.IssuedCount);
        Assert.NotSame(first, provisioner.Current);
        Assert.Equal(CertificateState.Valid, provisioner.Status.State);
        Assert.StartsWith("Renewed on request: ", provisioner.Status.Reason, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PersistedCertificateDueForRenewal_WhenTheStartUpOrderFails_IsServed_AndRenewedAfterTheRetryDelay()
    {
        CertificateProvisioner first = _env.Create(_env.Options());
        await first.StartAsync();
        string oldThumbprint = first.Current!.Thumbprint;
        await first.DisposeAsync();

        // 70 days later the 90-day certificate is due, and the CA fails the first attempt.
        OffsetTimeProvider later = new(TimeSpan.FromDays(70));
        _env.Ca.Clock = later.GetUtcNow;
        _env.Ca.FailValidation = true;
        AcmeProvisioningOptions options = _env.Options();
        options.Renewal = new RenewalSchedulerOptions { UseRenewalInfo = false, TimeProvider = later, RetryDelay = TimeSpan.FromMilliseconds(300) };
        CertificateProvisioner second = _env.Create(options);
        Recorder recorder = new(second);

        await second.StartAsync();

        Assert.True(recorder.HasStatus(CertificateState.Failed, "The persisted certificate is served meanwhile"));
        Assert.Equal(oldThumbprint, second.Current!.Thumbprint);

        _env.Ca.FailValidation = false;
        await Wait.ForAsync(() => second.Current!.Thumbprint != oldThumbprint, LongWait, "the retried renewal");
        await Wait.ForAsync(() => second.Status.State == CertificateState.Valid, LongWait, "Valid after the renewal");
        Assert.True(recorder.HasStatus(CertificateState.Renewing, "Renewing the persisted certificate"));
        Assert.True(recorder.HasStatus(CertificateState.Valid, "Renewed: "));
    }

    [Fact]
    public async Task StopAsync_CancelsARenewNowAsyncThatIsStillRetrying()
    {
        AcmeProvisioningOptions options = _env.Options();
        options.Retry = new AcmeRetryOptions { MaxAttempts = 100, InitialDelay = TimeSpan.FromMilliseconds(200), MaxDelay = TimeSpan.FromMilliseconds(200), Jitter = 0 };
        CertificateProvisioner provisioner = _env.Create(options);
        Recorder recorder = new(provisioner);
        await provisioner.StartAsync();
        _env.Ca.UnavailableRequestsRemaining = int.MaxValue;

        Task renew = provisioner.RenewNowAsync();
        await Wait.ForAsync(() => recorder.HasStatus(CertificateState.Renewing, "Retrying after a transient CA failure"), LongWait, "a retry in the background");
        await provisioner.StopAsync().WaitAsync(LongWait);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => renew);
        Assert.Equal(CertificateState.Stopped, provisioner.Status.State);
        Assert.NotNull(provisioner.Current); // served until disposal
    }

    [Fact]
    public async Task UnexpectedLoopFailure_IsReported_AndRenewNowAsyncRecovers()
    {
        File.WriteAllBytes(_env.CertificatePath, "not a pfx"u8.ToArray());
        CertificateProvisioner provisioner = _env.Create(_env.Options()); // RetryDelay: one hour

        // The unreadable file is accepted as a valid certificate, so the loop cannot seed the renewal scheduler from it.
        provisioner.CertificateLoader = (pfx, password, flags) => pfx.AsSpan().SequenceEqual("not a pfx"u8)
            ? Certs.Create()
            : CertificateProvisioner.LoadServedCertificate(pfx, password, flags);
        Recorder recorder = new(provisioner);

        await provisioner.StartAsync();
        Assert.Equal(CertificateState.Valid, provisioner.Status.State);
        Assert.Equal(0, _env.Ca.IssuedCount);

        await Wait.ForAsync(() => recorder.HasStatus(CertificateState.Failed, "The renewal loop failed unexpectedly"), LongWait, "the loop failure");
        Assert.NotNull(provisioner.Current);

        await provisioner.RenewNowAsync().WaitAsync(LongWait); // cuts the one-hour restart delay short
        Assert.Equal(CertificateState.Valid, provisioner.Status.State);
        Assert.Equal(1, _env.Ca.IssuedCount);
    }

    [Fact]
    public async Task SupersededCertificates_DisposedByTheBinder_AreNoLongerTracked()
    {
        CertificateProvisioner provisioner = _env.Create(_env.Options());
        await using CertificateBinder binder = new(provisioner, [new RecordingConsumer()], new CertificateBinderOptions { SupersededCertificateGracePeriod = TimeSpan.Zero });
        await provisioner.StartAsync();

        await provisioner.RenewNowAsync().WaitAsync(LongWait);
        await provisioner.RenewNowAsync().WaitAsync(LongWait);

        Assert.Equal(3, _env.Ca.IssuedCount);
        Assert.Equal(2, provisioner.OwnedCertificateCount); // the first was disposed by the binder, and dropped on the next change
    }

    [Fact]
    public async Task TlsAlpn01ValidatedAgainstTheWrongServer_FailsWithTheCasReason()
    {
        CertificateProvisioner provisioner = _env.Create(_env.Options(AcmeChallengeKind.TlsAlpn01), wireValidationPorts: false);
        _env.Ca.TlsAlpnValidationPort = _env.Ca.BaseUri.Port; // a plain-HTTP server answers instead of the TLS endpoint

        await provisioner.StartAsync();

        Assert.Equal(CertificateState.Failed, provisioner.Status.State);
        FakeAcmeValidation validation = Assert.Single(_env.Ca.ValidationLog);
        Assert.False(validation.Succeeded);
        Assert.Contains("TLS handshake", validation.Error, StringComparison.Ordinal);
        Assert.Equal(0, provisioner.TlsAlpn01Challenges!.Count);
    }
}
