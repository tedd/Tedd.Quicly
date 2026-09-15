using System.Collections.Concurrent;
using System.Net;
using System.Security.Cryptography;
using Tedd.Quicly.Acme;
using Tedd.Quicly.Http.Tls;
using Tedd.Quicly.Server.Certificates;
using Tedd.Quicly.Testing.Acme;
using Tedd.Quicly.Transport.MsQuic;

namespace Tedd.Quicly.EndToEnd.Tests.Infrastructure;

/// <summary>
/// Everything one end-to-end test creates, torn down in the order the library documents (ACME.md, "Hot swap into running
/// listeners"): QUIC clients, then the QUIC listeners (consumers stop presenting certificates), then the certificate binders,
/// then the provisioners, then the fake CAs, and finally the MsQuic registration and the temporary folder. Every endpoint
/// binds loopback port 0, and every file lives in a folder of its own.
/// </summary>
internal sealed class TestBed : IAsyncDisposable
{
    /// <summary>The DNS identifier every test certifies. It does not resolve: clients connect to the loopback address and send it as SNI.</summary>
    public const string Identifier = "game.example.test";

    /// <summary>The raw-QUIC ALPN (ARCHITECTURE §2).</summary>
    public const string Alpn = "quicly/1";

    private readonly Lock _lock = new();
    private readonly List<QuicTestClient> _clients = [];
    private readonly List<MsQuicConfiguration> _clientConfigurations = [];
    private readonly List<QuicTestServer> _servers = [];
    private readonly List<CertificateBinder> _binders = [];
    private readonly List<CertificateProvisioner> _provisioners = [];
    private readonly List<IAsyncDisposable> _helpers = [];
    private readonly List<FakeAcmeServer> _cas = [];
    private readonly ConcurrentQueue<CertificateConsumerFailure> _consumerFailures = new();
    private MsQuicRegistration? _registration;

    public TestBed()
    {
        Folder = Path.Combine(Path.GetTempPath(), "quicly-e2e", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Folder);
        CertificatePassword = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));
    }

    public string Folder { get; }

    public string AccountStorePath => Path.Combine(Folder, "acme-account.json");

    public string CertificatePath => Path.Combine(Folder, "server.pfx");

    /// <summary>The record of the directory that issued the persisted certificate (ACME.md: <c>&lt;CertificatePath&gt;.acme.json</c>).</summary>
    public string CertificateRecordPath => CertificatePath + ".acme.json";

    /// <summary>The PFX password (ACME.md recommends one: the PFX holds the private key).</summary>
    public string CertificatePassword { get; }

    /// <summary>Every consumer failure any binder of this bed reported.</summary>
    public CertificateConsumerFailure[] ConsumerFailures => [.. _consumerFailures];

    /// <summary>The MsQuic registration shared by the bed's listeners and clients, opened on first use.</summary>
    public MsQuicRegistration Registration
    {
        get
        {
            lock (_lock)
            {
                return _registration ??= new MsQuicRegistration("quicly-e2e");
            }
        }
    }

    /// <summary>Starts a fake ACME CA that validates for real over loopback (the ports are wired per provisioner).</summary>
    public FakeAcmeServer StartCa()
    {
        FakeAcmeServer ca = new()
        {
            Http01ValidationHost = "127.0.0.1",
            TlsAlpnValidationHost = "127.0.0.1",
            // Longer than anything a test does while the CA waits on a validation (the tls-alpn-01 test probes the endpoint then).
            ValidationTimeout = E2eTimeouts.Order,
        };
        lock (_lock)
        {
            _cas.Add(ca);
        }

        ca.Start();
        return ca;
    }

    /// <summary>
    /// ACME options for <paramref name="ca"/>: loopback port-0 challenge endpoints, the bed's files, a PFX password, fast ACME
    /// polling and back-off, and no ARI with an hour between retries, so that once a certificate is served the renewal loop
    /// never contacts the CA during a test. Key storage stays at the product default (<c>DefaultKeySet</c>, ADR 0009), and the
    /// account store at its default protection (DPAPI for the current user on Windows).
    /// </summary>
    public AcmeProvisioningOptions AcmeOptions(FakeAcmeServer ca, params AcmeChallengeKind[] challengeTypes)
    {
        AcmeProvisioningOptions options = new()
        {
            DirectoryUrl = ca.DirectoryUrl,
            AgreeToTermsOfService = true,
            AccountStorePath = AccountStorePath,
            CertificatePath = CertificatePath,
            CertificatePassword = CertificatePassword,
            HttpChallengeEndpoint = new IPEndPoint(IPAddress.Loopback, 0),
            TlsAlpnEndpoint = new IPEndPoint(IPAddress.Loopback, 0),
            Retry = new AcmeRetryOptions { MaxAttempts = 3, InitialDelay = TimeSpan.FromMilliseconds(50), MaxDelay = TimeSpan.FromMilliseconds(250), Jitter = 0 },
            ClientOptions = new AcmeClientOptions { PollInterval = TimeSpan.FromMilliseconds(25), MaxPollAttempts = 2_000 },
            Renewal = new RenewalSchedulerOptions { UseRenewalInfo = false, RetryDelay = TimeSpan.FromHours(1) },
        };
        options.Contacts.Add("mailto:ops@example.test");
        options.DnsNames.Add(Identifier);
        options.ChallengeTypes.Clear();
        foreach (AcmeChallengeKind kind in challengeTypes.Length == 0 ? [AcmeChallengeKind.Http01] : challengeTypes)
        {
            options.ChallengeTypes.Add(kind);
        }

        return options;
    }

    /// <summary>
    /// Creates a provisioner for <paramref name="ca"/>. Its challenge endpoints bind loopback port 0, so the CA learns the bound
    /// ports from the provisioner's progress events: the first one comes after the endpoints are bound and before any
    /// challenge is answered. With <paramref name="wireTlsAlpnPort"/> false the test points the CA's tls-alpn-01 check elsewhere.
    /// </summary>
    public CertificateProvisioner CreateProvisioner(FakeAcmeServer ca, AcmeProvisioningOptions options, bool wireTlsAlpnPort = true)
    {
        CertificateProvisioner provisioner = new(ServerCertificateOptions.Acme(options));
        provisioner.Progress += _ =>
        {
            if (provisioner.HttpChallengeEndPoint is { } http)
            {
                ca.Http01ValidationPort = http.Port;
            }

            if (wireTlsAlpnPort && provisioner.TlsEndPoint is { } tls)
            {
                ca.TlsAlpnValidationPort = tls.Port;
            }
        };
        lock (_lock)
        {
            _provisioners.Add(provisioner);
        }

        return provisioner;
    }

    /// <summary>Starts a QUIC listener on 127.0.0.1:0 for ALPN quicly/1; it refuses connections until a certificate is applied.</summary>
    public QuicTestServer StartQuicServer()
    {
        QuicTestServer server = new(Registration, Alpn);
        lock (_lock)
        {
            _servers.Add(server);
        }

        return server;
    }

    /// <summary>
    /// Binds <paramref name="source"/> to <paramref name="consumers"/>. The consumers are added after the binder exists, so a
    /// consumer that refuses the current certificate is recorded in <see cref="ConsumerFailures"/> (the binder's constructor
    /// would report such a failure before anyone could subscribe) and fails the test right here.
    /// </summary>
    public CertificateBinder Bind(ICertificateSource source, CertificateBinderOptions? options, params ICertificateConsumer[] consumers)
    {
        CertificateBinder binder = new(source, options);
        binder.ConsumerFailed += _consumerFailures.Enqueue;
        lock (_lock)
        {
            _binders.Add(binder);
        }

        foreach (ICertificateConsumer consumer in consumers)
        {
            binder.Add(consumer);
        }

        if (_consumerFailures.TryPeek(out CertificateConsumerFailure? failure))
        {
            throw new InvalidOperationException("A consumer refused the certificate " + failure.Certificate.Subject + ": " + failure.Exception.Message, failure.Exception);
        }

        return binder;
    }

    /// <summary>Starts a client towards <paramref name="server"/> that sends <paramref name="serverName"/> as SNI and validates with <paramref name="validation"/>.</summary>
    public QuicTestClient Connect(QuicTestServer server, ClientValidation validation, string serverName = Identifier)
    {
        MsQuicConfiguration configuration = MsQuicConfiguration.CreateClient(Registration, [Alpn], validation.Mode, E2eSettings.Client());
        lock (_lock)
        {
            _clientConfigurations.Add(configuration);
        }

        QuicTestClient client = QuicTestClient.Start(Registration, configuration, server.EndPoint, serverName, validation.Validator);
        lock (_lock)
        {
            _clients.Add(client);
        }

        return client;
    }

    /// <summary>Starts a <see cref="ValidationProxy"/> relaying to <paramref name="target"/>.</summary>
    public ValidationProxy StartProxy(Func<IPEndPoint?> target, Func<CancellationToken, Task> beforeForwarding)
    {
        ValidationProxy proxy = new(target, beforeForwarding);
        lock (_lock)
        {
            _helpers.Add(proxy);
        }

        return proxy;
    }

    /// <summary>
    /// Takes one server down mid-test (a restart), in the documented order: its clients, the QUIC listener, the binder, then
    /// the provisioner (stop the listeners before disposing the binder, and dispose the binder before the provisioner).
    /// </summary>
    public async Task StopAsync(QuicTestServer? server, CertificateBinder? binder, CertificateProvisioner provisioner)
    {
        QuicTestClient[] clients;
        lock (_lock)
        {
            clients = [.. _clients];
            _clients.Clear();
            if (server is not null)
            {
                _servers.Remove(server);
            }

            if (binder is not null)
            {
                _binders.Remove(binder);
            }

            _provisioners.Remove(provisioner);
        }

        foreach (QuicTestClient client in clients)
        {
            await client.DisposeAsync();
        }

        if (server is not null)
        {
            await server.DisposeAsync();
        }

        binder?.Dispose();
        await provisioner.DisposeAsync();
    }

    public async ValueTask DisposeAsync()
    {
        QuicTestClient[] clients;
        MsQuicConfiguration[] clientConfigurations;
        QuicTestServer[] servers;
        CertificateBinder[] binders;
        CertificateProvisioner[] provisioners;
        IAsyncDisposable[] helpers;
        FakeAcmeServer[] cas;
        MsQuicRegistration? registration;
        lock (_lock)
        {
            clients = [.. _clients];
            clientConfigurations = [.. _clientConfigurations];
            servers = [.. _servers];
            binders = [.. _binders];
            provisioners = [.. _provisioners];
            helpers = [.. _helpers];
            cas = [.. _cas];
            registration = _registration;
            _clients.Clear();
            _clientConfigurations.Clear();
            _servers.Clear();
            _binders.Clear();
            _provisioners.Clear();
            _helpers.Clear();
            _cas.Clear();
            _registration = null;
        }

        List<Exception> failures = [];
        foreach (QuicTestClient client in clients)
        {
            await Try(failures, client.DisposeAsync);
        }

        foreach (QuicTestServer server in servers)
        {
            await Try(failures, server.DisposeAsync);
        }

        foreach (MsQuicConfiguration configuration in clientConfigurations)
        {
            await Try(failures, () =>
            {
                configuration.Close();
                return ValueTask.CompletedTask;
            });
        }

        foreach (CertificateBinder binder in binders)
        {
            await Try(failures, binder.DisposeAsync);
        }

        foreach (CertificateProvisioner provisioner in provisioners)
        {
            await Try(failures, provisioner.DisposeAsync);
        }

        foreach (IAsyncDisposable helper in helpers)
        {
            await Try(failures, helper.DisposeAsync);
        }

        foreach (FakeAcmeServer ca in cas)
        {
            await Try(failures, ca.DisposeAsync);
        }

        if (registration is not null)
        {
            await Try(failures, () =>
            {
                CloseWithDeadline(registration);
                return ValueTask.CompletedTask;
            });
        }

        try
        {
            Directory.Delete(Folder, recursive: true);
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException)
        {
            // Best effort: a temporary folder.
        }

        if (failures.Count > 0)
        {
            throw new AggregateException("Tearing the test bed down failed.", failures);
        }
    }

    private static async Task Try(List<Exception> failures, Func<ValueTask> dispose)
    {
        try
        {
            await dispose();
        }
        catch (Exception e)
        {
            failures.Add(e);
        }
    }

    /// <summary>
    /// RegistrationClose blocks until every child handle is closed, so a handle leaked by a failing test would hang the run;
    /// the close runs on its own thread with a deadline, which turns a leak into a failure.
    /// </summary>
    private static void CloseWithDeadline(MsQuicRegistration registration)
    {
        Thread thread = new(registration.Close) { IsBackground = true, Name = "quicly-e2e RegistrationClose" };
        thread.Start();
        if (!thread.Join(E2eTimeouts.Step))
        {
            throw new TimeoutException("RegistrationClose did not return within " + E2eTimeouts.Step + ": a listener, connection, stream or configuration handle was leaked.");
        }
    }
}
