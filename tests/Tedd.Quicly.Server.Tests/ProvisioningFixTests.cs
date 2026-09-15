using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Acme;
using Tedd.Quicly.Acme.Challenges;
using Tedd.Quicly.Server.Certificates;
using Tedd.Quicly.Testing.Acme;

namespace Tedd.Quicly.Server.Tests;

/// <summary>
/// Provisioner behaviour required by the review: CA quota protection when an issued certificate cannot be loaded, the ARI
/// <c>replaces</c> identifier for on-demand and start-up renewals, staging-to-production switches, bounded stopping, and
/// renewal requests whose token is already cancelled.
/// </summary>
public sealed class ProvisioningFixTests : IAsyncDisposable
{
    private static readonly TimeSpan LongWait = TimeSpan.FromSeconds(30);
    private readonly AcmeTestEnvironment _env = new();

    public ValueTask DisposeAsync() => _env.DisposeAsync();

    // ---- issued certificates that cannot be loaded ----------------------------------------------------------------

    [Fact]
    public async Task IssuedCertificateThatCannotBeLoadedAtStartUp_IsLoadedAgainAfterTheRetryDelay_WithoutAnotherOrder()
    {
        AcmeProvisioningOptions options = _env.Options(); // three load attempts per round
        options.Renewal = new RenewalSchedulerOptions { UseRenewalInfo = false, RetryDelay = TimeSpan.FromMilliseconds(300) };
        CertificateProvisioner provisioner = _env.Create(options);
        int failures = 3; // the whole start-up round
        provisioner.CertificateLoader = (pfx, password, flags) => Interlocked.Decrement(ref failures) >= 0
            ? throw new CryptographicException("simulated key storage failure")
            : CertificateProvisioner.LoadServedCertificate(pfx, password, flags);
        Recorder recorder = new(provisioner);

        await provisioner.StartAsync();

        Assert.Equal(CertificateState.Failed, provisioner.Status.State);
        Assert.StartsWith("A certificate was issued by " + _env.Ca.DirectoryUrl + " but could not be loaded: simulated key storage failure.", provisioner.Status.Reason, StringComparison.Ordinal);
        Assert.EndsWith(" Loading it again after 00:00:00.3000000, before anything is ordered.", provisioner.Status.Reason, StringComparison.Ordinal);
        Assert.IsType<CryptographicException>(provisioner.Status.Error);
        Assert.True(recorder.HasStatus(CertificateState.Starting, "The issued certificate could not be loaded (simulated key storage failure); loading it again in "));
        Assert.Null(provisioner.Current);

        await Wait.ForAsync(() => provisioner.Status.State == CertificateState.Valid, LongWait, "the kept certificate to be served");
        Assert.StartsWith("Loaded the certificate issued earlier, which could not be loaded then: ", provisioner.Status.Reason, StringComparison.Ordinal);
        Assert.Equal(1, _env.Ca.IssuedCount);
        Assert.Same(provisioner.Current, await provisioner.WaitForCertificateAsync());
    }

    [Fact]
    public async Task KeptCertificateThatStillCannotBeLoaded_IsDiscarded_AndANewOneOrdered()
    {
        AcmeProvisioningOptions options = _env.Options();
        options.Renewal = new RenewalSchedulerOptions { UseRenewalInfo = false, RetryDelay = TimeSpan.FromMilliseconds(300) };
        CertificateProvisioner provisioner = _env.Create(options);
        byte[]? broken = null;
        provisioner.CertificateLoader = (pfx, password, flags) =>
        {
            Interlocked.CompareExchange(ref broken, pfx, null);
            return ReferenceEquals(pfx, broken)
                ? throw new CryptographicException("this certificate never loads")
                : CertificateProvisioner.LoadServedCertificate(pfx, password, flags);
        };
        Recorder recorder = new(provisioner);

        await provisioner.StartAsync();
        Assert.Equal(CertificateState.Failed, provisioner.Status.State);

        await Wait.ForAsync(() => provisioner.Status.State == CertificateState.Valid, LongWait, "a new certificate");
        Assert.StartsWith("Obtained ", provisioner.Status.Reason, StringComparison.Ordinal);
        Assert.Equal(2, _env.Ca.IssuedCount);
        Assert.Contains(recorder.Errors, e => e is CryptographicException && e.Message.Contains("still cannot be loaded", StringComparison.Ordinal));
        Assert.True(broken!.All(static b => b == 0)); // the discarded PFX holds a private key: it was zeroed
    }

    // ---- RenewNowAsync --------------------------------------------------------------------------------------------

    [Fact]
    public async Task RenewNowAsync_WithAnAlreadyCancelledToken_QueuesNoOrder()
    {
        CertificateProvisioner provisioner = _env.Create(_env.Options());
        await provisioner.StartAsync();
        X509Certificate2 first = provisioner.Current!;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => provisioner.RenewNowAsync(new CancellationToken(canceled: true)));
        await Task.Delay(300); // a queued order would have run by now

        Assert.Equal(1, _env.Ca.IssuedCount);
        Assert.Same(first, provisioner.Current);
        Assert.Equal(CertificateState.Valid, provisioner.Status.State);
    }

    // ---- ARI replaces ---------------------------------------------------------------------------------------------

    [Fact]
    public async Task RenewNowAsync_MarksTheOrderAsTheReplacement_WhenTheCaOffersAri()
    {
        _env.Ca.RenewalInfoEnabled = true;
        AcmeProvisioningOptions options = _env.Options();
        options.Renewal = new RenewalSchedulerOptions { UseRenewalInfo = true, RetryDelay = TimeSpan.FromHours(1) };
        CertificateProvisioner provisioner = _env.Create(options);
        await provisioner.StartAsync();
        string replaced = AcmeClient.GetAriCertificateId(provisioner.Current!);

        await provisioner.RenewNowAsync().WaitAsync(LongWait);

        Assert.Equal(new string?[] { null, replaced }, _env.Ca.ReplacesSeen);
    }

    [Fact]
    public async Task StartUpRenewalOfADuePersistedCertificate_MarksTheOrderAsTheReplacement()
    {
        _env.Ca.RenewalInfoEnabled = true;
        CertificateProvisioner first = _env.Create(_env.Options());
        await first.StartAsync();
        string replaced = AcmeClient.GetAriCertificateId(first.Current!);
        await first.DisposeAsync();

        // 70 days later the 90-day certificate is due.
        OffsetTimeProvider later = new(TimeSpan.FromDays(70));
        _env.Ca.Clock = later.GetUtcNow;
        AcmeProvisioningOptions options = _env.Options();
        options.Renewal = new RenewalSchedulerOptions { UseRenewalInfo = true, TimeProvider = later, RetryDelay = TimeSpan.FromHours(1) };
        CertificateProvisioner second = _env.Create(options);
        await second.StartAsync();

        Assert.Equal(CertificateState.Valid, second.Status.State);
        Assert.Equal(new string?[] { null, replaced }, _env.Ca.ReplacesSeen);
    }

    [Fact]
    public async Task RenewNowAsync_ForACertificateWithoutAnAuthorityKeyIdentifier_OrdersWithoutReplaces()
    {
        _env.Ca.RenewalInfoEnabled = true;
        byte[] pfx = Certs.Pfx("game.example.test"); // self-signed: no Authority Key Identifier, so no ARI identifier
        File.WriteAllBytes(_env.CertificatePath, pfx);
        using (X509Certificate2 persisted = X509CertificateLoader.LoadPkcs12(pfx, null))
        {
            CertificateMetadata.Write(CertificateMetadata.PathFor(_env.CertificatePath), _env.Ca.DirectoryUrl, persisted);
        }

        AcmeProvisioningOptions options = _env.Options();
        options.Renewal = new RenewalSchedulerOptions { UseRenewalInfo = true, RetryDelay = TimeSpan.FromHours(1) };
        CertificateProvisioner provisioner = _env.Create(options);
        await provisioner.StartAsync();
        Assert.StartsWith("Loaded the persisted certificate", provisioner.Status.Reason, StringComparison.Ordinal);

        await provisioner.RenewNowAsync().WaitAsync(LongWait);

        Assert.Equal(new string?[] { null }, _env.Ca.ReplacesSeen);
        Assert.Equal(CertificateState.Valid, provisioner.Status.State);
    }

    // ---- which directory issued the persisted certificate ---------------------------------------------------------

    [Fact]
    public async Task PersistedCertificateFromAnotherDirectory_IsServedOnlyUntilItsReplacementArrives()
    {
        CertificateProvisioner first = _env.Create(_env.Options());
        await first.StartAsync();
        string stagingThumbprint = first.Current!.Thumbprint;

        // As if the staging directory had issued it before the switch to production.
        CertificateMetadata.Write(CertificateMetadata.PathFor(_env.CertificatePath), AcmeDirectories.LetsEncryptStaging, first.Current!);
        await first.DisposeAsync();

        _env.Ca.RenewalInfoEnabled = true;
        AcmeProvisioningOptions options = _env.Options();
        options.Renewal = new RenewalSchedulerOptions { UseRenewalInfo = true, RetryDelay = TimeSpan.FromHours(1) };
        CertificateProvisioner second = _env.Create(options);
        Recorder recorder = new(second);
        await second.StartAsync();

        Assert.Equal(CertificateState.Valid, second.Status.State);
        Assert.True(recorder.HasStatus(CertificateState.Starting, "was issued by " + AcmeDirectories.LetsEncryptStaging.AbsoluteUri + ", not " + _env.Ca.DirectoryUrl));
        Assert.Equal(stagingThumbprint, recorder.Changed[0].Thumbprint); // served meanwhile
        Assert.NotEqual(stagingThumbprint, second.Current!.Thumbprint);
        Assert.Equal(new string?[] { null, null }, _env.Ca.ReplacesSeen); // another CA's certificate is not "replaced" here
        await second.DisposeAsync();

        // The replacement is recorded as this directory's, so the next restart keeps it.
        CertificateProvisioner third = _env.Create(_env.Options());
        await third.StartAsync();
        Assert.StartsWith("Loaded the persisted certificate", third.Status.Reason, StringComparison.Ordinal);
        Assert.Equal(2, _env.Ca.IssuedCount);
    }

    [Fact]
    public async Task PersistedCertificateWithoutAReadableRecordOfItsDirectory_IsReplaced()
    {
        CertificateProvisioner first = _env.Create(_env.Options());
        await first.StartAsync();
        await first.DisposeAsync();
        File.WriteAllText(CertificateMetadata.PathFor(_env.CertificatePath), "{ not json");

        CertificateProvisioner second = _env.Create(_env.Options());
        Recorder recorder = new(second);
        await second.StartAsync();

        Assert.True(recorder.HasStatus(CertificateState.Starting, "was issued by an unrecorded directory"));
        Assert.Equal(CertificateState.Valid, second.Status.State);
        Assert.Equal(2, _env.Ca.IssuedCount);
    }

    [Fact]
    public async Task DirectoryRecordThatCannotBeWritten_IsReported_AndTheCertificateIsStillServed()
    {
        Directory.CreateDirectory(CertificateMetadata.PathFor(_env.CertificatePath)); // a directory is in the way
        CertificateProvisioner first = _env.Create(_env.Options());
        Recorder recorder = new(first);

        await first.StartAsync();

        Assert.Equal(CertificateState.Valid, first.Status.State);
        Assert.Contains(recorder.Errors, e => e is IOException && e.Message.StartsWith("Recording which ACME directory issued the certificate failed", StringComparison.Ordinal));
        Assert.Empty(Directory.GetFiles(_env.Folder, "*.tmp")); // the temporary file was removed
        await first.DisposeAsync();

        // Without a readable record the next start replaces the certificate once.
        CertificateProvisioner second = _env.Create(_env.Options());
        await second.StartAsync();
        Assert.Equal(2, _env.Ca.IssuedCount);
    }

    // ---- stopping -------------------------------------------------------------------------------------------------

    [Fact]
    public async Task DisposeAsync_DoesNotHang_OnAChallengeProviderThatIgnoresCancellation()
    {
        HangingDns01Provider dns = new();
        AcmeProvisioningOptions options = _env.Options(AcmeChallengeKind.Dns01);
        options.Dns01Provider = dns;
        options.ChallengeCleanupTimeout = TimeSpan.FromMilliseconds(100);
        CertificateProvisioner provisioner = _env.Create(options);
        provisioner.StopWaitMargin = TimeSpan.FromMilliseconds(200);
        Recorder recorder = new(provisioner);

        Task start = provisioner.StartAsync();
        await dns.Entered.WaitAsync(LongWait); // the start-up order is stuck in CreateTxtAsync

        await provisioner.DisposeAsync().AsTask().WaitAsync(LongWait);

        Assert.Equal(CertificateState.Stopped, provisioner.Status.State);
        Assert.Contains(recorder.Errors, e => e is TimeoutException && e.Message.StartsWith("Start-up did not finish within ", StringComparison.Ordinal));
        dns.Release(); // the abandoned start-up winds down
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => start.WaitAsync(LongWait));
    }

    [Fact]
    public async Task StopAsync_WhenACallbackOnTheStopTokenThrows_StillClosesTheEndpoints_AndStops()
    {
        CancelCallbackThrowingDns01Provider dns = new(_env.Dns);
        AcmeProvisioningOptions options = _env.Options(AcmeChallengeKind.Dns01, AcmeChallengeKind.Http01); // http-01: an endpoint to close
        options.Dns01Provider = dns;
        CertificateProvisioner provisioner = _env.Create(options);
        Recorder recorder = new(provisioner);
        await provisioner.StartAsync();
        Assert.NotNull(provisioner.HttpChallengeEndPoint);

        Task renew = provisioner.RenewNowAsync();
        await dns.Waiting.WaitAsync(LongWait); // the renewal order waits in CreateTxtAsync, with a throwing callback on the stop token
        await provisioner.StopAsync().WaitAsync(LongWait);

        Assert.Equal(CertificateState.Stopped, provisioner.Status.State);
        Assert.Null(provisioner.HttpChallengeEndPoint);
        Assert.Contains(recorder.Errors, e => e is AggregateException);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => renew);
    }

    /// <summary>
    /// Publishes into the test DNS for the first order; for later orders it registers a callback that throws on the order's
    /// cancellation token, and waits (honouring cancellation).
    /// </summary>
    private sealed class CancelCallbackThrowingDns01Provider(InMemoryDns01Provider inner) : IDns01Provider
    {
        private readonly TaskCompletionSource _waiting = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _calls;

        public Task Waiting => _waiting.Task;

        public async ValueTask CreateTxtAsync(string name, string value, CancellationToken cancellationToken)
        {
            if (Interlocked.Increment(ref _calls) == 1)
            {
                await inner.CreateTxtAsync(name, value, cancellationToken);
                return;
            }

            cancellationToken.Register(static () => throw new InvalidOperationException("a responder's cancellation callback failed"));
            _waiting.TrySetResult();
            await Task.Delay(Timeout.Infinite, cancellationToken);
        }

        public ValueTask RemoveTxtAsync(string name, string value, CancellationToken cancellationToken) => inner.RemoveTxtAsync(name, value, cancellationToken);
    }

    /// <summary>A DNS provider whose <see cref="CreateTxtAsync"/> ignores cancellation until it is released.</summary>
    private sealed class HangingDns01Provider : IDns01Provider
    {
        private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _release = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task Entered => _entered.Task;

        public void Release() => _release.TrySetResult();

        public async ValueTask CreateTxtAsync(string name, string value, CancellationToken cancellationToken)
        {
            _entered.TrySetResult();
            await _release.Task;
        }

        public ValueTask RemoveTxtAsync(string name, string value, CancellationToken cancellationToken) => ValueTask.CompletedTask;
    }
}
