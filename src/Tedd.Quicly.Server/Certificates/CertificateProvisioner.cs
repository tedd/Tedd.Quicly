using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Acme;
using Tedd.Quicly.Acme.Challenges;
using Tedd.Quicly.Acme.Models;
using Tedd.Quicly.Http;
using Tedd.Quicly.Http.Handlers;
using Tedd.Quicly.Http.Tls;

namespace Tedd.Quicly.Server.Certificates;

/// <summary>
/// Provides the server certificate from one of three sources (<see cref="ServerCertificateOptions"/>): a fixed
/// certificate, a PFX file that may be replaced on disk, or an ACME CA. Consumers read <see cref="Current"/> or bind
/// through <see cref="CertificateBinder"/>, and every new certificate is announced through <see cref="Changed"/>, so a
/// renewal reaches running listeners without a restart.
/// </summary>
/// <remarks>
/// <para><b>ACME.</b> <see cref="StartAsync"/> starts the challenge endpoints for the allowed challenge types: a plain
/// HTTP server answering <c>http-01</c> (optionally redirecting everything else to HTTPS and serving a health path),
/// and a TLS server answering <c>tls-alpn-01</c> that presents the current certificate to every other client. It then
/// serves the persisted certificate when one exists, covers every configured name and has not expired, and orders a new
/// one (with the <see cref="AcmeCertificateManager"/>'s retry and back-off) when there is none or it is due for renewal.
/// Afterwards a <see cref="RenewalScheduler"/> runs in the background: renewals follow ARI or the lifetime rule, failed
/// renewals are retried after <see cref="RenewalSchedulerOptions.RetryDelay"/>, and every outcome is reported through
/// <see cref="Status"/> and <see cref="StatusChanged"/>. Nothing thrown in the background escapes.</para>
/// <para><b>Failures.</b> <see cref="StartAsync"/> throws for configuration and environment errors that retrying cannot
/// fix: an endpoint that cannot be bound, a missing or unreadable PFX file, invalid options. A CA that cannot issue a
/// certificate does not make it throw: the status becomes <see cref="CertificateState.Failed"/> with the reason and the
/// provisioner orders again after <see cref="RenewalSchedulerOptions.RetryDelay"/> until it succeeds;
/// <see cref="WaitForCertificateAsync"/> completes once a certificate exists.</para>
/// <para><b>Ownership.</b> Certificates the provisioner loads or obtains are disposed by <see cref="DisposeAsync"/>; a
/// superseded one is disposed earlier when bound through a <see cref="CertificateBinder"/> (after its grace period). A
/// <see cref="ServerCertificateSourceKind.Static"/> certificate stays the application's. Stop the consumers before
/// disposing the provisioner.</para>
/// <para><b>Events</b> are raised synchronously on the thread that caused them (a start-up, timer or background thread);
/// handlers must not block. A handler that throws is reported through <see cref="Error"/> and the other handlers still run.</para>
/// <para>A provisioner starts once; after <see cref="StopAsync"/> create a new one.</para>
/// </remarks>
public sealed class CertificateProvisioner : ICertificateSource, IAsyncDisposable
{
    private const int StateCreated = 0;
    private const int StateStarting = 1;
    private const int StateRunning = 2;
    private const int StateStopped = 3;

    /// <summary>Longest single timer wait used by the background loop.</summary>
    private static readonly TimeSpan MaxDelay = TimeSpan.FromDays(30);

    private readonly ServerCertificateOptions _options;
    private readonly Lock _lock = new();
    private readonly Lock _publishLock = new();
    private readonly Lock _statusLock = new();
    private readonly List<X509Certificate2> _owned = [];
    private readonly CancellationTokenSource _stopCts = new();
    private readonly TaskCompletionSource _firstCertificate = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _startCompleted = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly TaskCompletionSource _stopped = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private X509Certificate2? _current;
    private CertificateStatus _status = CertificateStatus.Stopped;
    private bool _statusSealed;
    private int _state;
    private int _stopRequested;
    private int _disposed;

    // Running resources, guarded by _lock and drained by ReleaseResourcesAsync.
    private HttpServer? _httpServer;
    private HttpServer? _tlsServer;
    private FileCertificateSource? _fileSource;
    private Task? _loop;

    // ACME state.
    private Http01ChallengeHandler? _http01;
    private TlsAlpn01Responder? _tlsAlpn01;
    private AcmeCertificateManager? _manager;
    private RenewalScheduler? _scheduler;
    private HttpClient? _ownedHttp;
    private byte[]? _currentPfx;

    // Renewal loop coordination, guarded by _lock.
    private RenewalRequest? _pendingRenewal;
    private RenewalRequest? _activeRenewal;
    private CancellationTokenSource? _interrupt;

    /// <summary>Creates a provisioner; nothing is bound or loaded until <see cref="StartAsync"/>.</summary>
    /// <exception cref="ArgumentException">The ACME options are incomplete or inconsistent (they are validated again here because they are mutable).</exception>
    public CertificateProvisioner(ServerCertificateOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        options.AcmeOptions?.Validate();
        _options = options;
    }

    /// <summary>The configuration.</summary>
    public ServerCertificateOptions Options => _options;

    /// <inheritdoc/>
    /// <remarks>The certificate with its private key, or <see langword="null"/> before the first one is available and after disposal.</remarks>
    public X509Certificate2? Current => Volatile.Read(ref _current);

    /// <inheritdoc/>
    public event Action<X509Certificate2>? Changed;

    /// <summary>The current status.</summary>
    public CertificateStatus Status => Volatile.Read(ref _status);

    /// <summary>Raised on every status change, in order.</summary>
    public event Action<CertificateStatus>? StatusChanged;

    /// <summary>Every stage of the ACME order flow (account, order, challenge, finalisation, retries), for logging.</summary>
    public event Action<AcmeProgress>? Progress;

    /// <summary>
    /// Non-fatal problems that do not change <see cref="Status"/>: an event handler threw, the challenge endpoints reported
    /// a connection error, a persisted certificate could not be read and is being replaced.
    /// </summary>
    public event Action<Exception>? Error;

    /// <summary>The endpoint the <c>http-01</c> challenge server is bound to (port 0 resolved) while running, else <see langword="null"/>.</summary>
    public IPEndPoint? HttpChallengeEndPoint
    {
        get
        {
            lock (_lock)
            {
                return _httpServer is { IsRunning: true } server ? server.BoundEndpoints[0] : null;
            }
        }
    }

    /// <summary>The endpoint the <c>tls-alpn-01</c> TLS server is bound to (port 0 resolved) while running, else <see langword="null"/>.</summary>
    public IPEndPoint? TlsEndPoint
    {
        get
        {
            lock (_lock)
            {
                return _tlsServer is { IsRunning: true } server ? server.BoundEndpoints[0] : null;
            }
        }
    }

    /// <summary>The <c>http-01</c> responder (tests inspect that no challenge material is left behind).</summary>
    internal Http01ChallengeHandler? Http01Challenges => _http01;

    /// <summary>The <c>tls-alpn-01</c> responder (tests inspect that no challenge material is left behind).</summary>
    internal TlsAlpn01Responder? TlsAlpn01Challenges => _tlsAlpn01;

    /// <summary>Loads the served certificate from PKCS#12 bytes (test seam for load failures).</summary>
    internal Func<byte[], string?, X509KeyStorageFlags, X509Certificate2> CertificateLoader { get; set; } = LoadServedCertificate;

    /// <summary>Number of certificates held for disposal; superseded ones a binder has disposed are dropped (tests).</summary>
    internal int OwnedCertificateCount
    {
        get
        {
            lock (_publishLock)
            {
                return _owned.Count;
            }
        }
    }

    /// <summary>
    /// Key storage for the throw-away copy the renewal scheduler is seeded with: it only reads the public certificate, so
    /// the key stays in memory where the platform supports that (macOS does not).
    /// </summary>
    private static X509KeyStorageFlags SeedKeyStorageFlags => OperatingSystem.IsMacOS() ? X509KeyStorageFlags.DefaultKeySet : X509KeyStorageFlags.EphemeralKeySet;

    /// <summary>
    /// Starts the source: loads the static or file certificate, or for ACME starts the challenge endpoints, serves or
    /// obtains the first certificate and starts the renewal loop. Returns once the first attempt has finished; check
    /// <see cref="Status"/>, or await <see cref="WaitForCertificateAsync"/> when a certificate is required.
    /// </summary>
    /// <exception cref="InvalidOperationException">Already started, or stopped.</exception>
    /// <exception cref="ObjectDisposedException">Disposed.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> fired or <see cref="StopAsync"/> was called during start-up.</exception>
    /// <exception cref="System.Net.Sockets.SocketException">A challenge endpoint could not be bound (everything started so far is released).</exception>
    /// <exception cref="FileNotFoundException">The PFX of a file source does not exist.</exception>
    /// <exception cref="CryptographicException">The PFX of a file source cannot be read (wrong password, corrupt).</exception>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        int previous = Interlocked.CompareExchange(ref _state, StateStarting, StateCreated);
        if (previous != StateCreated)
        {
            throw new InvalidOperationException(previous == StateStopped
                ? "The certificate provisioner has been stopped; create a new instance to start again."
                : "The certificate provisioner has already been started.");
        }

        SetStatus(new CertificateStatus(CertificateState.Starting));
        using CancellationTokenSource linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _stopCts.Token);
        try
        {
            switch (_options.Kind)
            {
                case ServerCertificateSourceKind.Static:
                    Publish(_options.Certificate!, owned: false);
                    SetStatus(new CertificateStatus(CertificateState.Valid, "Serving the configured certificate " + CertificateIdentity.Describe(_options.Certificate!) + "."));
                    break;
                case ServerCertificateSourceKind.File:
                    StartFile();
                    break;
                default:
                    await StartAcmeAsync(linked.Token).ConfigureAwait(false);
                    break;
            }

            // Cancelled, or stopped through StopAsync, while starting: nothing started may keep running.
            if (linked.Token.IsCancellationRequested || Interlocked.CompareExchange(ref _state, StateRunning, StateStarting) != StateStarting)
            {
                throw new OperationCanceledException("The certificate provisioner was cancelled or stopped during start-up.", cancellationToken);
            }

            if (_options.Kind == ServerCertificateSourceKind.Acme)
            {
                StartLoop();
            }
        }
        catch (Exception e)
        {
            // Nothing started may outlive a failed start: stop whatever was bound and never become Running.
            Volatile.Write(ref _state, StateStopped);
            _stopCts.Cancel();
            await ReleaseResourcesAsync().ConfigureAwait(false);
            _firstCertificate.TrySetCanceled(CancellationToken.None);
            SetStatus(e is OperationCanceledException ? CertificateStatus.Stopped : CertificateStatus.Failed("Start-up failed: " + e.Message, e));
            throw;
        }
        finally
        {
            _startCompleted.TrySetResult();
        }
    }

    /// <summary>Completes with <see cref="Current"/> once a certificate is available (at once when one already is).</summary>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> fired, or the provisioner stopped before any certificate was available.</exception>
    /// <exception cref="ObjectDisposedException">The provisioner was disposed.</exception>
    public async Task<X509Certificate2> WaitForCertificateAsync(CancellationToken cancellationToken = default)
    {
        await _firstCertificate.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        return Current ?? throw new ObjectDisposedException(nameof(CertificateProvisioner));
    }

    /// <summary>
    /// Orders a replacement certificate now, whatever the renewal schedule says (key compromise, manual rotation, a
    /// certificate revoked by the CA), and completes when it is being served. A wait for the next retry, or for a
    /// scheduled renewal, is cut short; calls made while an on-demand order is queued or running share its outcome. The
    /// renewal schedule then continues from the new certificate.
    /// </summary>
    /// <returns>
    /// A task that completes when the new certificate is current. It faults with <see cref="InvalidOperationException"/>
    /// when the source is not ACME or the provisioner is not running, with the order's exception (usually an
    /// <see cref="AcmeException"/>; the status is then <see cref="CertificateState.Failed"/>) when the order fails, and is
    /// cancelled when <paramref name="cancellationToken"/> fires (the order still completes in the background) or the
    /// provisioner stops first.
    /// </returns>
    public Task RenewNowAsync(CancellationToken cancellationToken = default)
    {
        if (_options.Kind != ServerCertificateSourceKind.Acme)
        {
            return Task.FromException(new InvalidOperationException("Only ACME certificates can be renewed on demand."));
        }

        TaskCompletionSource completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_lock)
        {
            // Checked under the lock: the loop cancels the queued requests under this lock when it exits.
            if (Volatile.Read(ref _state) != StateRunning)
            {
                return Task.FromException(new InvalidOperationException("The certificate provisioner is not running."));
            }

            RenewalRequest request = _pendingRenewal ?? _activeRenewal ?? (_pendingRenewal = new RenewalRequest("Renewal requested.", DateTimeOffset.MinValue, retryUntilSuccess: false));
            request.NotBefore = DateTimeOffset.MinValue; // a retry waiting for its delay starts at once
            request.Requested = true;
            request.Waiters.Add(completion);
            Interrupt();
        }

        return completion.Task.WaitAsync(cancellationToken);
    }

    /// <summary>
    /// Stops the renewal loop (an order in progress is cancelled and its challenge material removed), closes the challenge
    /// endpoints and stops watching the file. Idempotent; <paramref name="cancellationToken"/> only bounds the wait.
    /// </summary>
    public Task StopAsync(CancellationToken cancellationToken = default)
    {
        if (Interlocked.Exchange(ref _stopRequested, 1) == 0)
        {
            _ = StopCoreAsync();
        }

        return _stopped.Task.WaitAsync(cancellationToken);
    }

    /// <summary>Stops (see <see cref="StopAsync"/>) and disposes every certificate the provisioner created, the ACME account key and an owned HTTP client.</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await StopAsync().ConfigureAwait(false);
        _manager?.Dispose();
        _ownedHttp?.Dispose();
        X509Certificate2[] owned;
        lock (_publishLock)
        {
            owned = [.. _owned];
            _owned.Clear();
            Volatile.Write(ref _current, null);
        }

        foreach (X509Certificate2 certificate in owned)
        {
            certificate.Dispose();
        }
    }

    private async Task StopCoreAsync()
    {
        try
        {
            int previous = Interlocked.Exchange(ref _state, StateStopped);
            _stopCts.Cancel();
            if (previous is StateStarting or StateRunning)
            {
                // StartAsync may still be between becoming Running and starting the loop: let it finish, so that the loop
                // (and everything else it started) is seen and released below.
                await _startCompleted.Task.ConfigureAwait(false);
            }

            await ReleaseResourcesAsync().ConfigureAwait(false);
            _firstCertificate.TrySetCanceled(CancellationToken.None);
            SetStatus(CertificateStatus.Stopped, seal: true);
        }
        catch (Exception e)
        {
            ReportError(e);
        }
        finally
        {
            _stopped.TrySetResult();
        }
    }

    private async Task ReleaseResourcesAsync()
    {
        Task? loop;
        HttpServer? http;
        HttpServer? tls;
        FileCertificateSource? file;
        lock (_lock)
        {
            loop = _loop;
            _loop = null;
            http = _httpServer;
            _httpServer = null;
            tls = _tlsServer;
            _tlsServer = null;
            file = _fileSource;
            _fileSource = null;
        }

        if (loop is not null)
        {
            // Never faults; exits once _stopCts is cancelled (an order in progress still removes its challenge material,
            // bounded by ChallengeCleanupTimeout), so the endpoints serving that material are closed only afterwards.
            await loop.ConfigureAwait(false);
        }

        if (file is not null)
        {
            file.Changed -= OnFileChanged;
            file.ReloadFailed -= OnFileReloadFailed;
            file.Dispose();
        }

        if (http is not null)
        {
            await http.DisposeAsync().ConfigureAwait(false);
        }

        if (tls is not null)
        {
            await tls.DisposeAsync().ConfigureAwait(false);
        }
    }

    // ---- file source ------------------------------------------------------------------------------------------------

    private void StartFile()
    {
        FileCertificateSource source = new(_options.FilePath!, _options.FilePassword, _options.ReloadOnChange ? _options.ReloadInterval : null);
        source.Changed += OnFileChanged;
        source.ReloadFailed += OnFileReloadFailed;
        lock (_lock)
        {
            _fileSource = source;
        }

        PublishFileCertificate(source, "Loaded ");
    }

    private void OnFileChanged(X509Certificate2 certificate)
    {
        FileCertificateSource? source;
        lock (_lock)
        {
            source = _fileSource;
        }

        if (source is not null)
        {
            PublishFileCertificate(source, "Reloaded ");
        }
    }

    private void PublishFileCertificate(FileCertificateSource source, string verb)
    {
        // Always the source's latest certificate, so a reload racing with start-up cannot leave a stale one current.
        X509Certificate2 certificate = source.Current!;
        string description = CertificateIdentity.Describe(certificate);
        if (Publish(certificate, owned: true))
        {
            SetStatus(new CertificateStatus(CertificateState.Valid, verb + source.FilePath + ": " + description + "."));
        }
    }

    private void OnFileReloadFailed(Exception exception)
    {
        SetStatus(CertificateStatus.Failed("Reloading " + _options.FilePath + " failed; the previous certificate stays in use: " + exception.Message, exception));
    }

    // ---- ACME start-up ----------------------------------------------------------------------------------------------

    private async Task StartAcmeAsync(CancellationToken cancellationToken)
    {
        AcmeProvisioningOptions o = _options.AcmeOptions!;
        IReadOnlyList<AcmeIdentifier> identifiers = o.BuildIdentifiers();
        IReadOnlyList<string> types = o.BuildChallengeTypes();

        IHttp01Responder? http01 = null;
        ITlsAlpn01Responder? tlsAlpn01 = null;
        List<string> endpoints = [];
        if (types.Contains(AcmeChallengeTypes.Http01))
        {
            Http01ChallengeHandler handler = new();
            HttpServer server = CreateHttpChallengeServer(o, handler);
            lock (_lock)
            {
                _http01 = handler;
                _httpServer = server;
            }

            server.Start();
            http01 = handler;
            endpoints.Add("http-01 on " + server.BoundEndpoints[0]);
        }

        if (types.Contains(AcmeChallengeTypes.TlsAlpn01))
        {
            TlsAlpn01Responder responder = new();
            HttpServer server = CreateTlsServer(o, responder);
            lock (_lock)
            {
                _tlsAlpn01 = responder;
                _tlsServer = server;
            }

            server.Start();
            tlsAlpn01 = new ReverseDnsTlsAlpn01Responder(responder);
            endpoints.Add("tls-alpn-01 on " + server.BoundEndpoints[0]);
        }

        if (endpoints.Count > 0)
        {
            SetStatus(new CertificateStatus(CertificateState.Starting, "Challenge endpoints listening: " + string.Join(", ", endpoints) + "."));
        }

        HttpClient httpClient = o.HttpClient ?? (_ownedHttp = new HttpClient());
        AcmeCertificateManager manager = new(httpClient, new AcmeCertificateManagerOptions
        {
            DirectoryUrl = o.DirectoryUrl,
            AccountStore = o.AccountStoreProtection is { } protection ? new AcmeAccountStore(o.AccountStorePath!, protection) : new AcmeAccountStore(o.AccountStorePath!),
            Identifiers = identifiers,
            Contacts = o.BuildContacts(),
            AgreeTermsOfService = o.AgreeToTermsOfService,
            ExternalAccountBinding = o.ExternalAccountKeyId is { } keyId ? new ExternalAccountBinding(keyId, o.ExternalAccountHmacKey!) : null,
            CertificateKeyAlgorithm = o.KeyAlgorithm,
            RsaKeySizeBits = o.RsaKeySizeBits,
            ReuseKey = o.ReuseKey,
            Retry = o.Retry,
            PreferredChallengeTypes = types,
            Http01Responder = http01,
            TlsAlpn01Responder = tlsAlpn01,
            Dns01Provider = types.Contains(AcmeChallengeTypes.Dns01) ? o.Dns01Provider : null,
            ChallengePropagationDelay = o.ChallengePropagationDelay,
            ChallengeCleanupTimeout = o.ChallengeCleanupTimeout,
            CertificatePath = o.CertificatePath,
            CertificatePassword = o.CertificatePassword,
            PreferredChainIssuer = o.PreferredChainIssuer,
            ClientOptions = o.ClientOptions,
        });
        _manager = manager;
        manager.Progress += OnManagerProgress;
        _scheduler = new RenewalScheduler(manager, o.Renewal);

        bool persisted = TryUsePersistedCertificate(o, identifiers, out bool due);
        if (persisted && !due)
        {
            return;
        }

        try
        {
            IssuedCertificate issued = await manager.OrderCertificateAsync(cancellationToken).ConfigureAwait(false);
            string served = ApplyIssued(issued);
            SetStatus(new CertificateStatus(CertificateState.Valid, "Obtained " + served + " from " + o.DirectoryUrl + "."));
        }
        catch (Exception e) when (!(e is OperationCanceledException && cancellationToken.IsCancellationRequested))
        {
            // Not fatal for start-up: the background loop orders again after the retry delay until an order succeeds, and
            // a persisted certificate (if any) is served meanwhile.
            string serving = persisted ? " The persisted certificate is served meanwhile; ordering again after " : " Ordering again after ";
            SetStatus(CertificateStatus.Failed("Ordering a certificate from " + o.DirectoryUrl + " failed: " + e.Message + serving + o.Renewal.RetryDelay + ".", e));
            QueueRetry(persisted ? "Renewing the persisted certificate, which is due, after the start-up order failed." : "Ordering again after the start-up order failed.");
        }
    }

    /// <summary>
    /// Serves the persisted certificate when it can be loaded, covers every identifier and has not expired.
    /// <paramref name="due"/> tells whether it should be replaced right away (the renewal rule says it is due).
    /// </summary>
    private bool TryUsePersistedCertificate(AcmeProvisioningOptions o, IReadOnlyList<AcmeIdentifier> identifiers, out bool due)
    {
        due = false;
        string path = o.CertificatePath!;
        if (!File.Exists(path))
        {
            SetStatus(new CertificateStatus(CertificateState.Starting, "No persisted certificate at " + path + "; ordering one from " + o.DirectoryUrl + "."));
            return false;
        }

        byte[] pfx;
        X509Certificate2 certificate;
        try
        {
            pfx = File.ReadAllBytes(path);
            certificate = CertificateLoader(pfx, o.CertificatePassword, o.KeyStorageFlags);
        }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException or CryptographicException)
        {
            ReportError(new InvalidDataException("The persisted certificate " + path + " cannot be loaded; ordering a new one.", e));
            SetStatus(new CertificateStatus(CertificateState.Starting, "The persisted certificate " + path + " cannot be loaded (" + e.Message + "); ordering a new one."));
            return false;
        }

        string? unusable = !CertificateIdentity.Covers(certificate, identifiers)
            ? "does not cover " + string.Join(", ", identifiers.Select(static i => i.Value))
            : certificate.NotAfter.ToUniversalTime() <= o.Renewal.TimeProvider.GetUtcNow().UtcDateTime
                ? "has expired"
                : null;
        if (unusable is not null)
        {
            certificate.Dispose();
            SetStatus(new CertificateStatus(CertificateState.Starting, "The persisted certificate " + path + " " + unusable + "; ordering a new one."));
            return false;
        }

        // Everything is read from the certificate before it is published: a concurrent DisposeAsync disposes it there.
        due = _scheduler!.IsRenewalDue(certificate);
        string description = CertificateIdentity.Describe(certificate);
        Volatile.Write(ref _currentPfx, pfx);
        Publish(certificate, owned: true);
        SetStatus(due
            ? new CertificateStatus(CertificateState.Starting, "The persisted certificate " + description + " is due for renewal; it is served while a replacement is ordered.")
            : new CertificateStatus(CertificateState.Valid, "Loaded the persisted certificate " + description + "."));
        return true;
    }

    private HttpServer CreateHttpChallengeServer(AcmeProvisioningOptions o, Http01ChallengeHandler handler)
    {
        HttpServerOptions options = new() { OnError = ReportError };
        options.Listen(o.HttpChallengeEndpoint);
        options.Use(handler);
        if (o.EnableHealthEndpoint)
        {
            options.Use(new HealthHandler(o.HealthPath));
        }

        if (o.RedirectToHttps)
        {
            // Last: the challenge path (and health) must be answered on port 80, never redirected.
            options.Use(new RedirectToHttpsHandler(o.RedirectHttpsPort));
        }

        return new HttpServer(options);
    }

    private HttpServer CreateTlsServer(AcmeProvisioningOptions o, TlsAlpn01Responder responder)
    {
        HttpServerOptions options = new() { OnError = ReportError };

        // Normal clients get the current certificate (read per handshake, so renewals apply at once); clients offering
        // only acme-tls/1 for a published name get the challenge certificate.
        options.Listen(o.TlsAlpnEndpoint, new HttpTlsOptions { CertificateSource = this, Alpn01Responder = responder });
        foreach (IHttpHandler handler in o.TlsEndpointHandlers)
        {
            options.Use(handler);
        }

        if (o.EnableHealthEndpoint)
        {
            options.Use(new HealthHandler(o.HealthPath));
        }

        return new HttpServer(options);
    }

    // ---- ACME background loop ---------------------------------------------------------------------------------------
    //
    // One loop task does all background work, one step at a time: it runs a queued order (RenewNowAsync, or an internal
    // retry after a failure), waits for a queued retry to become due, or runs the RenewalScheduler until something is
    // queued. Waits and scheduler runs register an interrupt that RenewNowAsync cancels, so a request never waits for
    // a timer. Every exception is caught and reported; the loop only ends when the provisioner stops.

    private void StartLoop()
    {
        CancellationToken stop = _stopCts.Token;
        Task loop;
        using (ExecutionContext.SuppressFlow())
        {
            loop = Task.Run(() => RunLoopAsync(stop));
        }

        lock (_lock)
        {
            _loop = loop;
        }
    }

    private async Task RunLoopAsync(CancellationToken stop)
    {
        RenewalSchedulerOptions renewal = _options.AcmeOptions!.Renewal;
        TimeSpan delay = TimeSpan.Zero;
        try
        {
            while (!stop.IsCancellationRequested)
            {
                try
                {
                    if (delay > TimeSpan.Zero)
                    {
                        TimeSpan wait = delay;
                        delay = TimeSpan.Zero;
                        await WaitAsync(wait, stop).ConfigureAwait(false);
                        continue;
                    }

                    RenewalRequest? request = TakeDueRenewal(out TimeSpan untilDue);
                    if (request is not null)
                    {
                        await RenewOnRequestAsync(request, stop).ConfigureAwait(false);
                    }
                    else if (untilDue > TimeSpan.Zero)
                    {
                        await WaitAsync(untilDue, stop).ConfigureAwait(false);
                    }
                    else
                    {
                        await RunSchedulerAsync(stop).ConfigureAwait(false);
                    }
                }
                catch (OperationCanceledException) when (stop.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception e)
                {
                    // Order failures are reported by the scheduler and by RenewOnRequestAsync, so this is unexpected (the
                    // scheduler's copy of the certificate could not be loaded, say); keep renewing rather than let the
                    // loop die.
                    SetStatus(CertificateStatus.Failed("The renewal loop failed unexpectedly and restarts after " + renewal.RetryDelay + ": " + e.Message, e));
                    delay = renewal.RetryDelay;
                }
            }
        }
        finally
        {
            CancelQueuedRenewals();
        }
    }

    /// <summary>Runs the renewal scheduler from the current certificate until it is interrupted by a queued order, or stopped.</summary>
    private async Task RunSchedulerAsync(CancellationToken stop)
    {
        AcmeProvisioningOptions o = _options.AcmeOptions!;
        using CancellationTokenSource run = CancellationTokenSource.CreateLinkedTokenSource(stop);

        // Not registered when an order was queued since the loop looked: the loop runs that first.
        if (TrySetInterrupt(run, refuseWhenAnyQueued: true))
        {
            IssuedCertificate? seed = null;
            try
            {
                byte[]? pfx = Volatile.Read(ref _currentPfx);
                seed = pfx is null ? null : IssuedCertificate.Load(pfx, o.CertificatePassword, SeedKeyStorageFlags);
                await _scheduler!.RunAsync(seed, OnRenewedAsync, OnRenewalFailed, run.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (run.IsCancellationRequested && !stop.IsCancellationRequested)
            {
                // Interrupted by a queued order; the loop runs it.
            }
            finally
            {
                ClearInterrupt(run);
                seed?.Dispose();
            }
        }
    }

    private Task OnRenewedAsync(IssuedCertificate issued, CancellationToken cancellationToken)
    {
        string served;
        try
        {
            served = ApplyIssued(issued);
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            // The scheduler now tracks a certificate that is not being served: interrupt it and order again after the
            // retry delay, so the served certificate cannot silently run out.
            SetStatus(CertificateStatus.Failed("A renewed certificate could not be loaded (" + e.Message + "); ordering again after " + _options.AcmeOptions!.Renewal.RetryDelay + ".", e));
            QueueRetry("Ordering again after a renewed certificate could not be loaded.");
            return Task.CompletedTask;
        }

        SetStatus(new CertificateStatus(CertificateState.Valid, "Renewed: " + served + "."));
        return Task.CompletedTask;
    }

    private void OnRenewalFailed(Exception exception)
    {
        SetStatus(CertificateStatus.Failed("Renewal failed; retrying after " + _options.AcmeOptions!.Renewal.RetryDelay + ": " + exception.Message, exception));
    }

    /// <summary>Runs a queued order and completes its waiters; a failed internal retry is queued again after the retry delay.</summary>
    private async Task RenewOnRequestAsync(RenewalRequest request, CancellationToken stop)
    {
        AcmeProvisioningOptions o = _options.AcmeOptions!;
        bool hadCertificate = Current is not null;
        try
        {
            SetStatus(new CertificateStatus(CertificateState.Renewing, request.Reason));
            IssuedCertificate issued = await _manager!.OrderCertificateAsync(stop).ConfigureAwait(false);
            string served = ApplyIssued(issued);
            string message = !hadCertificate ? "Obtained " + served + " from " + o.DirectoryUrl + "."
                : (request.Requested ? "Renewed on request: " : "Renewed: ") + served + ".";
            SetStatus(new CertificateStatus(CertificateState.Valid, message));
            Complete(request, error: null, cancelled: false);
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested)
        {
            Complete(request, error: null, cancelled: true);
            throw;
        }
        catch (Exception e)
        {
            bool retry = request.RetryUntilSuccess;
            SetStatus(CertificateStatus.Failed("Renewal failed" + (retry ? "; retrying after " + o.Renewal.RetryDelay : string.Empty) + ": " + e.Message, e));
            Complete(request, e, cancelled: false);
            if (retry)
            {
                QueueRetry(request.Reason);
            }
        }
    }

    /// <summary>Queues an internal order that starts after the retry delay (and is retried until it succeeds), and interrupts the scheduler.</summary>
    private void QueueRetry(string reason)
    {
        RenewalSchedulerOptions renewal = _options.AcmeOptions!.Renewal;
        DateTimeOffset notBefore = renewal.TimeProvider.GetUtcNow() + renewal.RetryDelay;
        lock (_lock)
        {
            // A request queued already keeps its own (earlier) start, and is retried until it succeeds as well.
            RenewalRequest queued = _pendingRenewal ??= new RenewalRequest(reason, notBefore, retryUntilSuccess: true);
            queued.RetryUntilSuccess = true;
            Interrupt();
        }
    }

    /// <summary>Takes the queued order when it is due; otherwise <paramref name="untilDue"/> is the time left (zero when nothing is queued).</summary>
    private RenewalRequest? TakeDueRenewal(out TimeSpan untilDue)
    {
        DateTimeOffset now = _options.AcmeOptions!.Renewal.TimeProvider.GetUtcNow();
        lock (_lock)
        {
            untilDue = TimeSpan.Zero;
            RenewalRequest? request = _pendingRenewal;
            if (request is null)
            {
                return null;
            }

            if (request.NotBefore > now)
            {
                untilDue = request.NotBefore - now;
                return null;
            }

            _pendingRenewal = null;
            _activeRenewal = request;
            return request;
        }
    }

    private void Complete(RenewalRequest request, Exception? error, bool cancelled)
    {
        TaskCompletionSource[] waiters;
        lock (_lock)
        {
            if (ReferenceEquals(_activeRenewal, request))
            {
                _activeRenewal = null;
            }

            waiters = [.. request.Waiters];
            request.Waiters.Clear();
        }

        foreach (TaskCompletionSource waiter in waiters)
        {
            if (cancelled)
            {
                waiter.TrySetCanceled(CancellationToken.None);
            }
            else if (error is not null)
            {
                waiter.TrySetException(error);
            }
            else
            {
                waiter.TrySetResult();
            }
        }
    }

    /// <summary>The loop is ending: callers of <see cref="RenewNowAsync"/> still waiting get a cancelled task.</summary>
    private void CancelQueuedRenewals()
    {
        RenewalRequest? queued;
        lock (_lock)
        {
            queued = _pendingRenewal;
            _pendingRenewal = null;
            _activeRenewal = null;
        }

        if (queued is not null)
        {
            Complete(queued, error: null, cancelled: true);
        }
    }

    /// <summary>Waits <paramref name="delay"/> on the renewal clock; returns early when an order is queued that is due.</summary>
    private async Task WaitAsync(TimeSpan delay, CancellationToken stop)
    {
        using CancellationTokenSource interrupt = CancellationTokenSource.CreateLinkedTokenSource(stop);

        // Not registered when an order became due since the loop looked: then there is nothing to wait for.
        if (TrySetInterrupt(interrupt, refuseWhenAnyQueued: false))
        {
            try
            {
                await DelayAsync(delay, interrupt.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!stop.IsCancellationRequested)
            {
                // Interrupted by RenewNowAsync; the loop runs the order.
            }
            finally
            {
                ClearInterrupt(interrupt);
            }
        }
    }

    /// <summary>
    /// Registers the loop's current wait or scheduler run so that <see cref="RenewNowAsync"/> can cut it short. Returns
    /// false, registering nothing, when a queued order is due (or, with <paramref name="refuseWhenAnyQueued"/>, when any
    /// order is queued), so a request made just before the registration is not missed.
    /// </summary>
    private bool TrySetInterrupt(CancellationTokenSource interrupt, bool refuseWhenAnyQueued)
    {
        DateTimeOffset now = _options.AcmeOptions!.Renewal.TimeProvider.GetUtcNow();
        lock (_lock)
        {
            bool queuedFirst = _pendingRenewal is { } queued && (refuseWhenAnyQueued || queued.NotBefore <= now);
            _interrupt = queuedFirst ? null : interrupt;
            return !queuedFirst;
        }
    }

    private void ClearInterrupt(CancellationTokenSource interrupt)
    {
        lock (_lock)
        {
            if (ReferenceEquals(_interrupt, interrupt))
            {
                _interrupt = null;
            }
        }
    }

    private Task DelayAsync(TimeSpan delay, CancellationToken cancellationToken)
    {
        // Rounded up to a whole millisecond (timers truncate: a wait that ends a fraction of a millisecond early would
        // make the loop spin until the due time) and clamped to the longest wait a timer accepts.
        long milliseconds = (delay.Ticks + TimeSpan.TicksPerMillisecond - 1) / TimeSpan.TicksPerMillisecond;
        TimeSpan rounded = TimeSpan.FromMilliseconds(Math.Clamp(milliseconds, 0, (long)MaxDelay.TotalMilliseconds));
        return Task.Delay(rounded, _options.AcmeOptions!.Renewal.TimeProvider, cancellationToken);
    }

    /// <summary>
    /// Cuts the loop's registered wait or scheduler run short. Called under <c>_lock</c>, which keeps the source from being
    /// disposed meanwhile (the loop unregisters it under the lock before disposing it). The cancellation callbacks run
    /// asynchronously, so no loop code runs under the lock.
    /// </summary>
    private void Interrupt()
    {
        _ = _interrupt?.CancelAsync();
    }

    private void OnManagerProgress(AcmeProgress progress)
    {
        Raise(Progress, progress, "Progress");
        CertificateState state = Status.State;
        if (progress.Stage == AcmeStage.Retrying)
        {
            SetStatus(new CertificateStatus(state == CertificateState.Starting ? CertificateState.Starting : CertificateState.Renewing, "Retrying after a transient CA failure: " + progress.Message));
        }
        else if (progress.Stage is AcmeStage.OrderCreated or AcmeStage.OrderResumed && state is CertificateState.Valid or CertificateState.Failed)
        {
            SetStatus(new CertificateStatus(CertificateState.Renewing, progress.Message));
        }
    }

    /// <summary>
    /// Loads the served certificate from the issued PKCS#12, makes it current and disposes <paramref name="issued"/>.
    /// Returns the certificate's description, for the status.
    /// </summary>
    /// <exception cref="OperationCanceledException">The provisioner was disposed meanwhile; the certificate was discarded.</exception>
    private string ApplyIssued(IssuedCertificate issued)
    {
        AcmeProvisioningOptions o = _options.AcmeOptions!;
        byte[] pfx = issued.Pfx;
        X509Certificate2 served;
        try
        {
            served = CertificateLoader(pfx, o.CertificatePassword, o.KeyStorageFlags);
        }
        finally
        {
            issued.Dispose();
        }

        string description = CertificateIdentity.Describe(served);
        Volatile.Write(ref _currentPfx, pfx);
        if (!Publish(served, owned: true))
        {
            throw new OperationCanceledException("The certificate provisioner was disposed while a new certificate was being applied; the certificate was discarded.");
        }

        return description;
    }

    /// <summary>
    /// Loads the certificate with a private key from PKCS#12 with the given key storage, without <c>Exportable</c>
    /// (ADR 0009), disposing the chain certificates that come with it.
    /// </summary>
    internal static X509Certificate2 LoadServedCertificate(byte[] pfx, string? password, X509KeyStorageFlags keyStorageFlags)
    {
        X509Certificate2Collection all = X509CertificateLoader.LoadPkcs12Collection(pfx, password, keyStorageFlags);
        X509Certificate2? leaf = null;
        foreach (X509Certificate2 certificate in all)
        {
            if (leaf is null && certificate.HasPrivateKey)
            {
                leaf = certificate;
            }
            else
            {
                certificate.Dispose();
            }
        }

        return leaf ?? throw new CryptographicException("The PKCS#12 data contains no certificate with a private key.");
    }

    // ---- publishing and events --------------------------------------------------------------------------------------

    /// <summary>
    /// Makes <paramref name="certificate"/> current and raises <see cref="Changed"/>. Returns false when it already is
    /// current, or when the provisioner is disposed (an owned certificate is then disposed).
    /// </summary>
    private bool Publish(X509Certificate2 certificate, bool owned)
    {
        lock (_publishLock)
        {
            bool disposed = Volatile.Read(ref _disposed) != 0;
            if (disposed || ReferenceEquals(Current, certificate))
            {
                // Arrived during disposal (an order finishing, a file reload): nobody will serve it. Or it is current
                // already (a file reload racing with start-up).
                if (disposed && owned)
                {
                    certificate.Dispose();
                }

                return false;
            }

            if (owned)
            {
                // Superseded certificates a CertificateBinder has disposed after their grace period need no tracking.
                _owned.RemoveAll(static c => c.Handle == IntPtr.Zero);
                _owned.Add(certificate);
            }

            Volatile.Write(ref _current, certificate);
            _firstCertificate.TrySetResult();
            Raise(Changed, certificate, "Changed");
            return true;
        }
    }

    private void SetStatus(CertificateStatus status, bool seal = false)
    {
        lock (_statusLock)
        {
            // After stopping, late reports from winding-down work (a file poll already under way) must not overwrite Stopped.
            if (!_statusSealed)
            {
                _statusSealed = seal;
                Volatile.Write(ref _status, status);
                Raise(StatusChanged, status, "StatusChanged");
            }
        }
    }

    private void Raise<T>(Action<T>? handlers, T value, string eventName)
    {
        if (handlers is null)
        {
            return;
        }

        foreach (Delegate handler in handlers.GetInvocationList())
        {
            try
            {
                ((Action<T>)handler)(value);
            }
            catch (Exception e)
            {
                ReportError(new InvalidOperationException("A " + eventName + " handler threw; the other handlers still ran.", e));
            }
        }
    }

    private void ReportError(Exception exception)
    {
        Action<Exception>? handlers = Error;
        if (handlers is null)
        {
            return;
        }

        foreach (Delegate handler in handlers.GetInvocationList())
        {
            try
            {
                ((Action<Exception>)handler)(exception);
            }
            catch (Exception)
            {
                // Nowhere left to report it.
            }
        }
    }

    /// <summary>An order the loop runs outside the renewal schedule: requested through <see cref="RenewNowAsync"/>, or an internal retry.</summary>
    private sealed class RenewalRequest(string reason, DateTimeOffset notBefore, bool retryUntilSuccess)
    {
        /// <summary>Why the order runs (the <see cref="CertificateState.Renewing"/> status reason).</summary>
        public string Reason { get; } = reason;

        /// <summary>Earliest start on the renewal clock; <see cref="DateTimeOffset.MinValue"/> for at once.</summary>
        public DateTimeOffset NotBefore { get; set; } = notBefore;

        /// <summary>When the order fails, queue another attempt after the retry delay (internal retries).</summary>
        public bool RetryUntilSuccess { get; set; } = retryUntilSuccess;

        /// <summary>Whether a caller of <see cref="RenewNowAsync"/> asked for it.</summary>
        public bool Requested { get; set; }

        /// <summary>Callers of <see cref="RenewNowAsync"/> waiting for the outcome.</summary>
        public List<TaskCompletionSource> Waiters { get; } = [];
    }
}
