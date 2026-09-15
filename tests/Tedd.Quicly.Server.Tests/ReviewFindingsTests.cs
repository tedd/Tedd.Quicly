using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Acme;
using Tedd.Quicly.Http.Tls;
using Tedd.Quicly.Server.Certificates;

namespace Tedd.Quicly.Server.Tests;

/// <summary>
/// Defects found in the review of the server certificate module. Each test states the expected behaviour and fails
/// until the finding is fixed.
/// </summary>
public sealed class ReviewFindingsTests : IAsyncDisposable
{
    private static readonly TimeSpan LongWait = TimeSpan.FromSeconds(30);
    private readonly List<X509Certificate2> _certificates = [];
    private readonly ManualTimeProvider _time = new();
    private readonly string _folder = Path.Combine(Path.GetTempPath(), "quicly-server-tests", Guid.NewGuid().ToString("N"));
    private AcmeTestEnvironment? _env;

    public async ValueTask DisposeAsync()
    {
        if (_env is not null)
        {
            await _env.DisposeAsync();
        }

        foreach (X509Certificate2 certificate in _certificates)
        {
            certificate.Dispose();
        }

        try
        {
            if (Directory.Exists(_folder))
            {
                Directory.Delete(_folder, true);
            }
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }

    // ---- CertificateBinder ---------------------------------------------------------------------------------------

    [Fact]
    public void Binder_DoesNotDispose_TheCertificateAFailedConsumerStillPresents()
    {
        X509Certificate2 a = NewCertificate();
        X509Certificate2 b = NewCertificate();
        TestSource source = new() { Current = a };
        RecordingConsumer quic = new();
        RecordingConsumer https = new();
        using CertificateBinder binder = new(source, [quic, https], Grace(TimeSpan.FromMinutes(2)));

        // The QUIC listener cannot load the renewed credential; by the binder's contract it keeps presenting a.
        quic.ThrowOnUpdate = new InvalidOperationException("ConfigurationLoadCredential failed");
        source.Raise(b);
        _time.Advance(TimeSpan.FromMinutes(5));

        // a is not superseded for that listener: disposing it (on Windows this deletes its key container) breaks every
        // later handshake there until the next renewal.
        Assert.Same(b, https.Received[^1]);
        Assert.False(Certs.IsDisposed(a));
    }

    [Fact]
    public void Binder_CreatedWhileTheSourcePublishes_EndsOnTheSourcesCurrentCertificate()
    {
        X509Certificate2 stale = NewCertificate();
        X509Certificate2 fresh = NewCertificate();
        PublishesWhileReadSource source = new(stale, fresh);
        RecordingConsumer consumer = new();

        using CertificateBinder binder = new(source, [consumer], Grace(TimeSpan.FromMinutes(2)));
        _time.Advance(TimeSpan.FromMinutes(5));

        // The constructor applies the stale certificate it read after the fresh one was already applied from Changed:
        // the consumers go back to the old certificate and the source's current one is disposed after the grace period.
        Assert.Same(fresh, source.Current);
        Assert.Same(fresh, binder.Certificate);
        Assert.Same(fresh, consumer.Received[^1]);
        Assert.False(Certs.IsDisposed(fresh));
    }

    [Fact]
    public void Binder_CertificateRaisedAgainAsItsGracePeriodEnds_IsNotDisposed()
    {
        X509Certificate2 a = NewCertificate();
        X509Certificate2 b = NewCertificate();
        TestSource source = new() { Current = a };
        InterleavingTimeProvider time = new();
        using CertificateBinder binder = new(source, [new RecordingConsumer()], new CertificateBinderOptions { SupersededCertificateGracePeriod = TimeSpan.FromMinutes(1), TimeProvider = time });
        source.Raise(b);

        // a's grace callback has taken a out of the pending set and released the binder lock when the source raises a
        // again on another thread (reproduced from the point where the callback releases its timer, just before it
        // disposes a).
        time.WhenATimerIsReleased = () => source.Raise(a);
        time.FireAll();

        Assert.Same(a, binder.Certificate);
        Assert.False(Certs.IsDisposed(a)); // "A certificate that the source raises again within its grace period is kept."
    }

    // ---- CertificateProvisioner: file source ----------------------------------------------------------------------

    [Fact]
    public async Task FileSource_ThatRecoversWithTheSameCertificate_IsValidAgain()
    {
        Directory.CreateDirectory(_folder);
        string path = Path.Combine(_folder, "server.pfx");
        byte[] pfx = Certs.Pfx("a.example.test", "secret");
        Replace(path, pfx, DateTime.UtcNow.AddMinutes(-10));
        await using CertificateProvisioner provisioner = new(ServerCertificateOptions.File(path, "secret", reloadOnChange: true, reloadInterval: TimeSpan.FromMilliseconds(50)));
        await provisioner.StartAsync();
        X509Certificate2 served = provisioner.Current!;

        // A poll finds the file unreadable (half-written, or locked by the tool replacing it) ...
        Replace(path, "half-written"u8.ToArray(), DateTime.UtcNow.AddMinutes(-5));
        await Wait.ForAsync(() => provisioner.Status.State == CertificateState.Failed, LongWait, "the reload failure");

        // ... and the next poll reads it fine, holding the certificate that is being served (a periodic re-export of an
        // unchanged certificate, a restored backup). The source is healthy again, but no Changed is raised because the
        // certificate did not change, so the status stays Failed until the certificate is next replaced.
        Replace(path, pfx, DateTime.UtcNow.AddMinutes(-4));
        await Wait.ForAsync(() => provisioner.Status.State == CertificateState.Valid, TimeSpan.FromSeconds(10), "Valid once the file is readable again");
        Assert.Same(served, provisioner.Current);
    }

    // ---- CertificateProvisioner: ACME -----------------------------------------------------------------------------

    [Fact]
    public async Task AcmeStartUp_WithTheLargestRetryDelay_DoesNotThrowForACaFailure()
    {
        AcmeTestEnvironment env = Env();
        env.Ca.FailValidation = true;
        AcmeProvisioningOptions options = env.Options();
        options.Renewal = new RenewalSchedulerOptions { UseRenewalInfo = false, RetryDelay = TimeSpan.MaxValue }; // "never retry by itself"

        CertificateProvisioner provisioner;
        try
        {
            provisioner = env.Create(options);
        }
        catch (ArgumentException)
        {
            return; // refused up front like the other out-of-range options: fine
        }

        // Accepted by validation, then StartAsync throws ArgumentOutOfRangeException ("un-representable DateTime") from
        // QueueRetry's now + RetryDelay, although a CA failure must never make StartAsync throw.
        await provisioner.StartAsync();
        Assert.Equal(CertificateState.Failed, provisioner.Status.State);
    }

    [Fact]
    public async Task TlsEndpoint_BeforeTheFirstCertificate_DoesNotReportEveryClientHandshakeAsAnError()
    {
        AcmeTestEnvironment env = Env();
        env.Ca.FailValidation = true; // no certificate yet: the start-up order failed and is retried in an hour
        CertificateProvisioner provisioner = env.Create(env.Options(AcmeChallengeKind.TlsAlpn01));
        Recorder recorder = new(provisioner);
        await provisioner.StartAsync();
        Assert.Null(provisioner.Current);
        int before = recorder.Errors.Length;

        // TCP 443 is public: players and scanners keep connecting while the CA is unreachable.
        for (int i = 0; i < 5; i++)
        {
            await Assert.ThrowsAnyAsync<Exception>(() => Certs.GetServedCertificateAsync(provisioner.TlsEndPoint!, "game.example.test"));
        }

        await Task.Delay(500); // the server finishes tearing the connections down

        // A handshake that cannot complete is expected on a public port and the HTTP server does not report those; here
        // each one reaches Error ("No server certificate is configured ..."): a log flood any remote client can drive.
        Assert.Equal(before, recorder.Errors.Length);
    }

    // ---- helpers --------------------------------------------------------------------------------------------------

    private AcmeTestEnvironment Env() => _env ??= new AcmeTestEnvironment();

    private CertificateBinderOptions Grace(TimeSpan grace) => new() { SupersededCertificateGracePeriod = grace, TimeProvider = _time };

    private X509Certificate2 NewCertificate()
    {
        X509Certificate2 certificate = Certs.Create();
        _certificates.Add(certificate);
        return certificate;
    }

    /// <summary>Atomic replace, retried: on Windows a rename over a file the poller is reading fails with a sharing violation.</summary>
    private static void Replace(string path, byte[] bytes, DateTime lastWriteUtc)
    {
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                Certs.WriteAtomically(path, bytes, lastWriteUtc);
                return;
            }
            catch (Exception e) when (e is IOException or UnauthorizedAccessException && attempt < 50)
            {
                Thread.Sleep(20);
            }
        }
    }

    /// <summary>
    /// A source whose certificate is replaced while the binder's constructor runs: the constructor has subscribed and
    /// read <see cref="Current"/> (the stale certificate) when the source publishes the fresh one and raises Changed.
    /// </summary>
    private sealed class PublishesWhileReadSource(X509Certificate2 stale, X509Certificate2 fresh) : ICertificateSource
    {
        private X509Certificate2 _current = stale;
        private bool _published;

        public X509Certificate2? Current
        {
            get
            {
                X509Certificate2 read = _current;
                if (!_published && Changed is { } changed)
                {
                    _published = true;
                    _current = fresh;
                    changed(fresh);
                }

                return read;
            }
        }

        public event Action<X509Certificate2>? Changed;
    }

    /// <summary>Timers fire on <see cref="FireAll"/>; releasing one runs <see cref="WhenATimerIsReleased"/> once, to interleave a second thread.</summary>
    private sealed class InterleavingTimeProvider : TimeProvider
    {
        private readonly List<(TimerCallback Callback, object? State)> _timers = [];

        public Action? WhenATimerIsReleased { get; set; }

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            _timers.Add((callback, state));
            return new InterleavingTimer(this);
        }

        public void FireAll()
        {
            foreach ((TimerCallback callback, object? state) in _timers.ToArray())
            {
                callback(state);
            }
        }

        private sealed class InterleavingTimer(InterleavingTimeProvider owner) : ITimer
        {
            public bool Change(TimeSpan dueTime, TimeSpan period) => false;

            public void Dispose()
            {
                Action? interleave = owner.WhenATimerIsReleased;
                owner.WhenATimerIsReleased = null;
                interleave?.Invoke();
            }

            public ValueTask DisposeAsync()
            {
                Dispose();
                return ValueTask.CompletedTask;
            }
        }
    }
}
