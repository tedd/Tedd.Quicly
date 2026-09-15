using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Server.Certificates;

namespace Tedd.Quicly.Server.Tests;

/// <summary>Static and file sources, and the provisioner's lifecycle and event contract.</summary>
public sealed class ProvisionerSourceTests : IDisposable
{
    private static readonly TimeSpan LongWait = TimeSpan.FromSeconds(30);
    private readonly string _folder = Path.Combine(Path.GetTempPath(), "quicly-server-tests", Guid.NewGuid().ToString("N"));

    public ProvisionerSourceTests()
    {
        Directory.CreateDirectory(_folder);
    }

    public void Dispose()
    {
        try
        {
            Directory.Delete(_folder, true);
        }
        catch (IOException)
        {
        }
    }

    [Fact]
    public async Task Static_ServesTheCertificate_AndNeverDisposesIt()
    {
        using X509Certificate2 certificate = Certs.Create();
        CertificateProvisioner provisioner = new(ServerCertificateOptions.Static(certificate));
        Recorder recorder = new(provisioner);
        Assert.Null(provisioner.Current);
        Assert.Equal(CertificateState.Stopped, provisioner.Status.State);

        await provisioner.StartAsync();

        Assert.Same(certificate, provisioner.Current);
        Assert.Equal(CertificateState.Valid, provisioner.Status.State);
        Assert.Same(certificate, Assert.Single(recorder.Changed));
        Assert.Same(certificate, await provisioner.WaitForCertificateAsync());
        Assert.Null(provisioner.HttpChallengeEndPoint);
        Assert.Null(provisioner.TlsEndPoint);
        await Assert.ThrowsAsync<InvalidOperationException>(() => provisioner.RenewNowAsync());

        await provisioner.DisposeAsync();
        Assert.False(Certs.IsDisposed(certificate));
        Assert.Equal(CertificateState.Stopped, provisioner.Status.State);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => provisioner.WaitForCertificateAsync());
        await Assert.ThrowsAsync<ObjectDisposedException>(() => provisioner.StartAsync());
        await provisioner.DisposeAsync(); // idempotent
    }

    [Fact]
    public async Task Lifecycle_StartsOnce_AndStopsIdempotently()
    {
        using X509Certificate2 certificate = Certs.Create();
        await using CertificateProvisioner provisioner = new(ServerCertificateOptions.Static(certificate));
        await provisioner.StartAsync();

        InvalidOperationException again = await Assert.ThrowsAsync<InvalidOperationException>(() => provisioner.StartAsync());
        Assert.Contains("already been started", again.Message, StringComparison.Ordinal);

        await provisioner.StopAsync();
        await provisioner.StopAsync();
        Assert.Equal(CertificateState.Stopped, provisioner.Status.State);
    }

    [Fact]
    public async Task StopBeforeStart_PreventsStarting()
    {
        using X509Certificate2 certificate = Certs.Create();
        await using CertificateProvisioner provisioner = new(ServerCertificateOptions.Static(certificate));

        await provisioner.StopAsync();

        InvalidOperationException error = await Assert.ThrowsAsync<InvalidOperationException>(() => provisioner.StartAsync());
        Assert.Contains("create a new instance", error.Message, StringComparison.Ordinal);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => provisioner.WaitForCertificateAsync());
    }

    [Fact]
    public async Task ThrowingHandlers_AreReportedThroughError_AndTheOtherHandlersStillRun()
    {
        using X509Certificate2 certificate = Certs.Create();
        await using CertificateProvisioner provisioner = new(ServerCertificateOptions.Static(certificate));
        List<Exception> errors = [];
        int changedAfter = 0;
        int statusAfter = 0;
        provisioner.Changed += _ => throw new InvalidOperationException("changed boom");
        provisioner.Changed += _ => changedAfter++;
        provisioner.StatusChanged += _ => throw new InvalidOperationException("status boom");
        provisioner.StatusChanged += _ => statusAfter++;
        provisioner.Error += errors.Add;
        provisioner.Error += _ => throw new InvalidOperationException("error handlers cannot report further");

        await provisioner.StartAsync();

        Assert.Equal(1, changedAfter);
        Assert.True(statusAfter >= 2);
        Assert.Contains(errors, e => e.Message.Contains("Changed handler", StringComparison.Ordinal) && e.InnerException?.Message == "changed boom");
        Assert.Contains(errors, e => e.Message.Contains("StatusChanged handler", StringComparison.Ordinal));
    }

    [Fact]
    public void Constructor_ValidatesArguments()
    {
        Assert.Throws<ArgumentNullException>(() => new CertificateProvisioner(null!));
    }

    [Fact]
    public async Task File_ReplacedOnDisk_RaisesChanged_AndReachesBoundConsumers()
    {
        string path = Path.Combine(_folder, "server.pfx");
        Certs.WriteAtomically(path, Certs.Pfx("a.example.test", "secret"), DateTime.UtcNow.AddMinutes(-10));
        CertificateProvisioner provisioner = new(ServerCertificateOptions.File(path, "secret", reloadOnChange: true, reloadInterval: TimeSpan.FromMilliseconds(50)));
        Recorder recorder = new(provisioner);
        RecordingConsumer consumer = new();
        await using CertificateBinder binder = new(provisioner, [consumer], new CertificateBinderOptions { SupersededCertificateGracePeriod = TimeSpan.Zero });

        await provisioner.StartAsync();

        Assert.Equal(CertificateState.Valid, provisioner.Status.State);
        Assert.StartsWith("Loaded ", provisioner.Status.Reason, StringComparison.Ordinal);
        X509Certificate2 first = provisioner.Current!;
        Assert.True(first.MatchesHostname("a.example.test"));
        Assert.Same(first, Assert.Single(consumer.Received));

        Certs.WriteAtomically(path, Certs.Pfx("b.example.test", "secret"), DateTime.UtcNow.AddMinutes(-5));
        await Wait.ForAsync(() => consumer.Received.Length == 2, LongWait, "the replaced file to reach the consumer");
        X509Certificate2 second = provisioner.Current!;
        Assert.True(second.MatchesHostname("b.example.test"));
        Assert.Same(second, consumer.Received[1]);
        Assert.True(Certs.IsDisposed(first)); // zero grace period
        Assert.True(recorder.HasStatus(CertificateState.Valid, "Reloaded "));

        // A file that cannot be read is reported; the previous certificate stays in use.
        Certs.WriteAtomically(path, "half-written"u8.ToArray(), DateTime.UtcNow.AddMinutes(-4));
        await Wait.ForAsync(() => provisioner.Status.State == CertificateState.Failed, LongWait, "the reload failure");
        Assert.IsType<CryptographicException>(provisioner.Status.Error, exactMatch: false);
        Assert.Same(second, provisioner.Current);

        Certs.WriteAtomically(path, Certs.Pfx("c.example.test", "secret"), DateTime.UtcNow.AddMinutes(-3));
        await Wait.ForAsync(() => provisioner.Status.State == CertificateState.Valid && provisioner.Current!.MatchesHostname("c.example.test"), LongWait, "recovery");

        X509Certificate2 last = provisioner.Current!;
        await provisioner.DisposeAsync();
        Assert.True(Certs.IsDisposed(last));
    }

    [Fact]
    public async Task File_WithoutReload_ServesTheLoadedCertificate()
    {
        string path = Path.Combine(_folder, "server.pfx");
        File.WriteAllBytes(path, Certs.Pfx("a.example.test"));
        ServerCertificateOptions options = ServerCertificateOptions.File(path, reloadOnChange: false);
        await using CertificateProvisioner provisioner = new(options);

        await provisioner.StartAsync();

        Assert.Equal(CertificateState.Valid, provisioner.Status.State);
        Assert.False(options.ReloadOnChange);
        Assert.True(provisioner.Current!.MatchesHostname("a.example.test"));
    }

    [Fact]
    public async Task File_Missing_FailsStartUp()
    {
        await using CertificateProvisioner provisioner = new(ServerCertificateOptions.File(Path.Combine(_folder, "missing.pfx")));

        await Assert.ThrowsAsync<FileNotFoundException>(() => provisioner.StartAsync());

        Assert.Equal(CertificateState.Failed, provisioner.Status.State);
        Assert.StartsWith("Start-up failed", provisioner.Status.Reason, StringComparison.Ordinal);
        Assert.IsType<FileNotFoundException>(provisioner.Status.Error);
    }

    [Fact]
    public async Task StartAsync_WithACancelledToken_Throws_AndStops()
    {
        using X509Certificate2 certificate = Certs.Create();
        await using CertificateProvisioner provisioner = new(ServerCertificateOptions.Static(certificate));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => provisioner.StartAsync(new CancellationToken(canceled: true)));

        Assert.Equal(CertificateState.Stopped, provisioner.Status.State);
        InvalidOperationException again = await Assert.ThrowsAsync<InvalidOperationException>(() => provisioner.StartAsync());
        Assert.Contains("has been stopped", again.Message, StringComparison.Ordinal);
    }
}
