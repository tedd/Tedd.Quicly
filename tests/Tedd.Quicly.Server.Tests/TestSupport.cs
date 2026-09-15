using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Acme;
using Tedd.Quicly.Http.Tls;
using Tedd.Quicly.Server.Certificates;
using Tedd.Quicly.Testing.Acme;
using Tedd.Quicly.Testing.Certificates;

namespace Tedd.Quicly.Server.Tests;

/// <summary>
/// A fake ACME CA on loopback that validates for real against the provisioner's challenge endpoints, plus a temp folder
/// for the account store and the certificate. Every endpoint binds 127.0.0.1:0; the bound ports are handed to the CA.
/// </summary>
internal sealed class AcmeTestEnvironment : IAsyncDisposable
{
    private readonly List<CertificateProvisioner> _created = [];

    public AcmeTestEnvironment()
    {
        Folder = Path.Combine(Path.GetTempPath(), "quicly-server-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Folder);
        Ca.Http01ValidationHost = "127.0.0.1";
        Ca.TlsAlpnValidationHost = "127.0.0.1";
        Ca.DnsTxtLookup = Dns.Lookup;
        Ca.Start();
    }

    public FakeAcmeServer Ca { get; } = new();

    public InMemoryDns01Provider Dns { get; } = new();

    public HttpClient Http { get; } = new();

    public string Folder { get; }

    public string AccountPath => Path.Combine(Folder, "account.json");

    public string CertificatePath => Path.Combine(Folder, "server.pfx");

    /// <summary>Options with loopback port-0 endpoints, fast retries and no ARI (so an idle loop never contacts the CA).</summary>
    public AcmeProvisioningOptions Options(params AcmeChallengeKind[] challengeTypes)
    {
        AcmeProvisioningOptions options = new()
        {
            DirectoryUrl = Ca.DirectoryUrl,
            AgreeToTermsOfService = true,
            AccountStorePath = AccountPath,
            AccountStoreProtection = AcmeStoreProtection.None,
            CertificatePath = CertificatePath,
            HttpClient = Http,
            HttpChallengeEndpoint = new IPEndPoint(IPAddress.Loopback, 0),
            TlsAlpnEndpoint = new IPEndPoint(IPAddress.Loopback, 0),
            Dns01Provider = Dns,
            Retry = new AcmeRetryOptions { MaxAttempts = 3, InitialDelay = TimeSpan.FromMilliseconds(20), MaxDelay = TimeSpan.FromMilliseconds(100), Jitter = 0 },
            ClientOptions = new AcmeClientOptions { PollInterval = TimeSpan.FromMilliseconds(20), MaxPollAttempts = 50 },
            Renewal = new RenewalSchedulerOptions { UseRenewalInfo = false, RetryDelay = TimeSpan.FromHours(1) },
        };
        options.DnsNames.Add("game.example.test");
        options.Contacts.Add("ops@example.test");
        options.ChallengeTypes.Clear();
        foreach (AcmeChallengeKind kind in challengeTypes.Length == 0 ? [AcmeChallengeKind.Http01] : challengeTypes)
        {
            options.ChallengeTypes.Add(kind);
        }

        return options;
    }

    public CertificateProvisioner Create(AcmeProvisioningOptions options, bool wireValidationPorts = true)
    {
        CertificateProvisioner provisioner = new(ServerCertificateOptions.Acme(options));
        if (wireValidationPorts)
        {
            // The first ACME progress event comes after the endpoints are bound and before any challenge is answered.
            provisioner.Progress += _ => WireValidationPorts(provisioner);
        }

        lock (_created)
        {
            _created.Add(provisioner);
        }

        return provisioner;
    }

    public void WireValidationPorts(CertificateProvisioner provisioner)
    {
        if (provisioner.HttpChallengeEndPoint is { } http)
        {
            Ca.Http01ValidationPort = http.Port;
        }

        if (provisioner.TlsEndPoint is { } tls)
        {
            Ca.TlsAlpnValidationPort = tls.Port;
        }
    }

    public async ValueTask DisposeAsync()
    {
        CertificateProvisioner[] created;
        lock (_created)
        {
            created = [.. _created];
        }

        foreach (CertificateProvisioner provisioner in created)
        {
            await provisioner.DisposeAsync();
        }

        Http.Dispose();
        await Ca.DisposeAsync();
        try
        {
            Directory.Delete(Folder, true);
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }
}

/// <summary>Records everything a provisioner reports.</summary>
internal sealed class Recorder
{
    private readonly Lock _lock = new();
    private readonly List<CertificateStatus> _statuses = [];
    private readonly List<X509Certificate2> _changed = [];
    private readonly List<Exception> _errors = [];
    private readonly List<AcmeProgress> _progress = [];

    public Recorder(CertificateProvisioner provisioner)
    {
        provisioner.StatusChanged += s => Add(_statuses, s);
        provisioner.Changed += c => Add(_changed, c);
        provisioner.Error += e => Add(_errors, e);
        provisioner.Progress += p => Add(_progress, p);
    }

    public CertificateStatus[] Statuses => Snapshot(_statuses);

    public X509Certificate2[] Changed => Snapshot(_changed);

    public Exception[] Errors => Snapshot(_errors);

    public AcmeProgress[] Progress => Snapshot(_progress);

    public bool HasStatus(CertificateState state, string reasonFragment)
    {
        return Statuses.Any(s => s.State == state && s.Reason?.Contains(reasonFragment, StringComparison.Ordinal) == true);
    }

    private void Add<T>(List<T> list, T item)
    {
        lock (_lock)
        {
            list.Add(item);
        }
    }

    private T[] Snapshot<T>(List<T> list)
    {
        lock (_lock)
        {
            return [.. list];
        }
    }
}

internal sealed class RecordingConsumer : ICertificateConsumer
{
    private readonly Lock _lock = new();
    private readonly List<X509Certificate2> _received = [];

    public Exception? ThrowOnUpdate { get; set; }

    public X509Certificate2[] Received
    {
        get
        {
            lock (_lock)
            {
                return [.. _received];
            }
        }
    }

    public void UpdateCertificate(X509Certificate2 certificate)
    {
        lock (_lock)
        {
            _received.Add(certificate);
        }

        if (ThrowOnUpdate is { } exception)
        {
            throw exception;
        }
    }
}

/// <summary>A certificate source driven by the test.</summary>
internal sealed class TestSource : ICertificateSource
{
    public X509Certificate2? Current { get; set; }

    public event Action<X509Certificate2>? Changed;

    public int SubscriberCount => Changed?.GetInvocationList().Length ?? 0;

    public void Raise(X509Certificate2 certificate)
    {
        Current = certificate;
        Changed?.Invoke(certificate);
    }

    public void RaiseNull() => Changed?.Invoke(null!);
}

/// <summary>A clock whose timers fire only when the test advances it.</summary>
internal sealed class ManualTimeProvider : TimeProvider
{
    private readonly Lock _lock = new();
    private readonly List<ManualTimer> _timers = [];
    private DateTimeOffset _now = new(2026, 9, 15, 12, 0, 0, TimeSpan.Zero);

    /// <summary>Fire timers even after they were disposed (a callback already queued when the timer was stopped).</summary>
    public bool FireDisposedTimers { get; set; }

    public int ActiveTimers
    {
        get
        {
            lock (_lock)
            {
                return _timers.Count(t => !t.Disposed && !t.Fired);
            }
        }
    }

    public override DateTimeOffset GetUtcNow()
    {
        lock (_lock)
        {
            return _now;
        }
    }

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        ManualTimer timer = new(callback, state, GetUtcNow() + dueTime);
        lock (_lock)
        {
            _timers.Add(timer);
        }

        return timer;
    }

    public void Advance(TimeSpan by)
    {
        List<ManualTimer> due;
        lock (_lock)
        {
            _now += by;
            due = [.. _timers.Where(t => t.DueAt <= _now && !t.Fired && (!t.Disposed || FireDisposedTimers))];
            foreach (ManualTimer timer in due)
            {
                timer.Fired = true;
            }
        }

        foreach (ManualTimer timer in due)
        {
            timer.Callback(timer.State);
        }
    }

    private sealed class ManualTimer(TimerCallback callback, object? state, DateTimeOffset dueAt) : ITimer
    {
        public TimerCallback Callback { get; } = callback;

        public object? State { get; } = state;

        public DateTimeOffset DueAt { get; } = dueAt;

        public bool Disposed { get; private set; }

        public bool Fired { get; set; }

        public bool Change(TimeSpan dueTime, TimeSpan period) => false;

        public void Dispose() => Disposed = true;

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }
    }
}

/// <summary>The system clock shifted into the future, with real timers.</summary>
internal sealed class OffsetTimeProvider(TimeSpan offset) : TimeProvider
{
    public override DateTimeOffset GetUtcNow() => base.GetUtcNow() + offset;
}

internal static class Certs
{
    public static X509Certificate2 Create(string dnsName = "game.example.test")
    {
        return TestCertificates.CreateSelfSigned("CN=" + dnsName, TimeSpan.FromDays(30), true, dnsName);
    }

    /// <summary>A PKCS#12 with an ECDSA key; a subjectAltName for <paramref name="dnsName"/> unless it is null.</summary>
    public static byte[] Pfx(string? dnsName, string? password = null)
    {
        using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        CertificateRequest request = new("CN=" + (dnsName ?? "no-san"), key, HashAlgorithmName.SHA256);
        if (dnsName is not null)
        {
            SubjectAlternativeNameBuilder san = new();
            san.AddDnsName(dnsName);
            request.CertificateExtensions.Add(san.Build());
        }

        DateTimeOffset now = DateTimeOffset.UtcNow;
        using X509Certificate2 certificate = request.CreateSelfSigned(now.AddMinutes(-5), now.AddDays(30));
        return certificate.Export(X509ContentType.Pkcs12, password);
    }

    /// <summary>A PKCS#12 holding only a certificate, no private key.</summary>
    public static byte[] PublicOnlyPfx(string dnsName)
    {
        using X509Certificate2 withKey = X509CertificateLoader.LoadPkcs12(Pfx(dnsName), null);
        using X509Certificate2 publicOnly = X509CertificateLoader.LoadCertificate(withKey.RawData);
        return publicOnly.Export(X509ContentType.Pkcs12)!;
    }

    /// <summary>Replaces a file the way deployments should: write a temporary file, then rename it over the old one.</summary>
    public static void WriteAtomically(string path, byte[] bytes, DateTime lastWriteUtc)
    {
        string temp = path + ".tmp";
        File.WriteAllBytes(temp, bytes);
        File.SetLastWriteTimeUtc(temp, lastWriteUtc);
        File.Move(temp, path, overwrite: true);
    }

    public static bool IsDisposed(X509Certificate2 certificate) => certificate.Handle == IntPtr.Zero;

    /// <summary>Handshakes like a normal client (ALPN http/1.1) and returns the certificate the server presented.</summary>
    public static async Task<X509Certificate2> GetServedCertificateAsync(IPEndPoint endPoint, string serverName)
    {
        using TcpClient tcp = new();
        await tcp.ConnectAsync(endPoint.Address, endPoint.Port);
        using SslStream ssl = new(tcp.GetStream(), leaveInnerStreamOpen: false);
        await ssl.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
        {
            TargetHost = serverName,
            ApplicationProtocols = [SslApplicationProtocol.Http11],
            RemoteCertificateValidationCallback = static (_, _, _, _) => true,
        });
        return X509CertificateLoader.LoadCertificate(ssl.RemoteCertificate!.GetRawCertData());
    }

    public static HttpClient CreateInsecureClient()
    {
        return new HttpClient(new SocketsHttpHandler
        {
            AllowAutoRedirect = false,
            UseProxy = false,
            SslOptions = new SslClientAuthenticationOptions { RemoteCertificateValidationCallback = static (_, _, _, _) => true },
        });
    }
}

internal static class Wait
{
    public static async Task ForAsync(Func<bool> condition, TimeSpan timeout, string because)
    {
        DateTime deadline = DateTime.UtcNow + timeout;
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException("Timed out after " + timeout + " waiting for: " + because);
            }

            await Task.Delay(20);
        }
    }
}
