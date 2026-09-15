using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Runtime.ExceptionServices;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Http;
using Tedd.Quicly.Http.Handlers;
using Tedd.Quicly.Server.Certificates;

namespace Tedd.Quicly.Server;

// Start, graceful shutdown and disposal; certificate provisioning and the HTTP side endpoint.
public sealed partial class QuiclyServer
{
    /// <summary>How often <see cref="StopAsync"/> polls the peers while it waits for them to close.</summary>
    public static readonly TimeSpan ShutdownPollInterval = TimeSpan.FromMilliseconds(10);

    private CertificateProvisioner? _provisioner;
    private CertificateBinder? _binder;
    private HttpServer? _http;
    private bool _shutdownBegun;
    private CloseReason _shutdownClose;

    /// <summary>Test seam: replaces the real-time delay of <see cref="StopAsync"/>'s wait loop (a simulated network advances here).</summary>
    internal Func<TimeSpan, CancellationToken, Task>? DelayOverride { get; set; }

    /// <summary>True once <see cref="BeginShutdown"/> ran and every connection is gone.</summary>
    public bool IsShutdownComplete => _shutdownBegun && Volatile.Read(ref _connections) == 0;

    /// <summary>
    /// Starts the server: provisions the certificate and binds it to <see cref="ServerOptions.CertificateConsumers"/> (waiting
    /// for the first one when <see cref="ServerOptions.WaitForCertificate"/>), starts the HTTP side endpoint, then starts the
    /// listener. From then on call <see cref="PollAll"/> and <see cref="FlushAll"/> from the game thread. Anything started is
    /// stopped again when a step fails.
    /// </summary>
    /// <param name="cancellationToken">Bounds the certificate wait.</param>
    /// <exception cref="InvalidOperationException">Already started.</exception>
    /// <exception cref="ObjectDisposedException">Disposed.</exception>
    public async Task StartAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (Interlocked.CompareExchange(ref _state, StateStarting, StateCreated) != StateCreated)
        {
            throw new InvalidOperationException("The server has already been started; create a new instance to start again.");
        }

        try
        {
            if (_certificateOptions is not null)
            {
                await StartCertificatesAsync(_certificateOptions, cancellationToken).ConfigureAwait(false);
            }

            cancellationToken.ThrowIfCancellationRequested();
            if (_httpSide is not null)
            {
                StartHttp(_httpSide);
            }

            lock (_gate)
            {
                Volatile.Write(ref _accepting, true);
            }

            _listener.Start(_preHandshake, _accept);
            Volatile.Write(ref _state, StateRunning);
        }
        catch
        {
            lock (_gate)
            {
                Volatile.Write(ref _accepting, false);
            }

            await StopSideServicesAsync().ConfigureAwait(false);
            Volatile.Write(ref _state, StateStopped);
            throw;
        }
    }

    /// <summary>
    /// Begins a graceful shutdown (game thread): stops accepting, stops the listener and closes every peer with
    /// <paramref name="reason"/> (default <see cref="ServerOptions.ShutdownReason"/>). Keep calling <see cref="PollAll"/> and
    /// <see cref="FlushAll"/> until <see cref="IsShutdownComplete"/>, or let <see cref="StopAsync"/> drive it.
    /// </summary>
    /// <param name="reason">The close code and reason; <see langword="null"/> for the configured one.</param>
    /// <exception cref="ArgumentException">The reason is invalid.</exception>
    public void BeginShutdown(CloseReason? reason = null)
    {
        ThrowIfDisposed();
        if (reason is { } given)
        {
            OptionChecks.CheckCloseReason(given, nameof(reason));
        }

        CloseReason close = reason ?? _shutdownReason;
        lock (_gate)
        {
            Volatile.Write(ref _accepting, false);
        }

        _shutdownClose = close; // also for connections the listener accepted before it stopped (ActivatePending closes them)
        if (!_shutdownBegun)
        {
            _shutdownBegun = true;
            Interlocked.CompareExchange(ref _state, StateStopping, StateRunning);
            _listener.Stop();
        }

        if (Volatile.Read(ref _activationCount) != 0)
        {
            ActivatePending();
        }

        int count = _highWater;
        for (int slot = 0; slot < count; slot++)
        {
            QuiclyPeer? peer = _slots[slot].Peer;
            if (peer is null || peer.State is PeerState.Closing or PeerState.Closed)
            {
                continue;
            }

            peer.Close(close);
            MarkWork(slot);
        }
    }

    /// <summary>
    /// Shuts down gracefully: <see cref="BeginShutdown"/>, then polls and flushes the peers until they closed or
    /// <see cref="ServerOptions.ShutdownTimeout"/> passed, disposes the rest (raising <see cref="PeerClosed"/> for admitted
    /// ones), ends the sessions still waiting for a resume (<see cref="SessionEnded"/>), and stops the HTTP endpoint and the
    /// certificate provisioning. Call it from the game thread, and stop calling <see cref="PollAll"/> yourself while it runs.
    /// Every step runs even when an event handler throws in an earlier one; the first exception is rethrown at the end. A
    /// server that never started just becomes stopped.
    /// </summary>
    /// <param name="cancellationToken">Cuts the wait short (the remaining peers are disposed).</param>
    public async Task StopAsync(CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        if (Interlocked.CompareExchange(ref _state, StateStopped, StateCreated) == StateCreated)
        {
            lock (_gate)
            {
                Volatile.Write(ref _accepting, false);
            }

            return;
        }

        if (Volatile.Read(ref _state) == StateStopped)
        {
            return;
        }

        ExceptionDispatchInfo? failure = null;
        try
        {
            BeginShutdown();
            long started = Stopwatch.GetTimestamp();
            while (!IsShutdownComplete && !cancellationToken.IsCancellationRequested && Stopwatch.GetElapsedTime(started) < _shutdownTimeout)
            {
                try
                {
                    PollAll();
                    FlushAll();
                }
                catch (Exception exception)
                {
                    KeepFirst(ref failure, exception); // an event handler threw: the other peers keep closing
                }

                if (IsShutdownComplete)
                {
                    break;
                }

                try
                {
                    await (DelayOverride?.Invoke(ShutdownPollInterval, cancellationToken) ?? Task.Delay(ShutdownPollInterval, cancellationToken)).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
        }
        catch (Exception exception)
        {
            KeepFirst(ref failure, exception); // the listener's Stop threw
        }

        KeepFirst(ref failure, Capture(ForceCloseAll));
        KeepFirst(ref failure, Capture(EndRemainingSessions));
        KeepFirst(ref failure, await CaptureAsync(StopSideServicesAsync).ConfigureAwait(false));
        RaiseQueuedEvents();
        Volatile.Write(ref _state, StateStopped);
        failure?.Throw();
    }

    /// <summary>
    /// Stops the server if it runs (<see cref="StopAsync"/>), then disposes the listener and the session token keys; the HTTP
    /// side endpoint and the certificate provisioning are stopped even when an event handler threw during the stop. The
    /// shared buffer pool is disposed once every peer's transport has reported its close.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposing, 1) != 0)
        {
            return;
        }

        try
        {
            if (Volatile.Read(ref _state) is StateStarting or StateRunning or StateStopping)
            {
                await StopAsync().ConfigureAwait(false); // also stops the HTTP endpoint and the certificate provisioning
            }
            else
            {
                lock (_gate)
                {
                    Volatile.Write(ref _accepting, false);
                }
            }
        }
        finally
        {
            // Dispose the listener, then close whatever it accepted while the shutdown ran: once it is disposed no callback
            // can add another. Every step runs even when another fails, and the side services are stopped here as well in
            // case StopAsync did not get to them.
            Exception? listenerFailure = Capture(_listener.Dispose);
            Exception? closeFailure = Capture(ForceCloseAll);
            Exception? sessionFailure = Capture(EndRemainingSessions);
            Exception? sideFailure = await CaptureAsync(StopSideServicesAsync).ConfigureAwait(false);
            _disposed = true;
            _tokens.Dispose();
            Volatile.Write(ref _allocatorReleaseRequested, 1);
            TryReleaseAllocator();
            if ((listenerFailure ?? closeFailure ?? sessionFailure ?? sideFailure) is { } failure)
            {
                ExceptionDispatchInfo.Throw(failure);
            }
        }
    }

    private static async Task<Exception?> CaptureAsync(Func<Task> action)
    {
        try
        {
            await action().ConfigureAwait(false);
            return null;
        }
        catch (Exception exception)
        {
            return exception;
        }
    }

    private static void KeepFirst(ref ExceptionDispatchInfo? first, Exception? exception)
    {
        if (first is null && exception is not null)
        {
            first = ExceptionDispatchInfo.Capture(exception);
        }
    }

    /// <summary>
    /// The server stops: every session it still holds ends now (one waiting for a resume with
    /// <see cref="SessionEndInfo.Expired"/> set), and the queued <see cref="SessionEnded"/> events are raised. Every handler
    /// runs; the first exception is rethrown.
    /// </summary>
    private void EndRemainingSessions()
    {
        _sessions.EndAll(_endedSessions);
        if (RaiseAllEndedSessions() is { } failure)
        {
            ExceptionDispatchInfo.Throw(failure);
        }
    }

    private static Exception? Capture(Action action)
    {
        try
        {
            action();
            return null;
        }
        catch (Exception exception)
        {
            return exception;
        }
    }

    private void ForceCloseAll()
    {
        if (Volatile.Read(ref _activationCount) != 0)
        {
            ActivatePending();
        }

        Exception? first = null;
        for (int slot = _highWater - 1; slot >= 0; slot--)
        {
            if (_slots[slot].Peer is not { } peer)
            {
                continue;
            }

            try
            {
                FinalizeSlot(slot, peer);
            }
            catch (Exception exception)
            {
                first ??= exception;
            }
        }

        if (first is not null)
        {
            ExceptionDispatchInfo.Throw(first);
        }
    }

    private async Task StartCertificatesAsync(ServerCertificateOptions options, CancellationToken cancellationToken)
    {
        CertificateProvisioner provisioner = new(options);
        _provisioner = provisioner;
        await provisioner.StartAsync(cancellationToken).ConfigureAwait(false);
        if (_waitForCertificate)
        {
            await provisioner.WaitForCertificateAsync(cancellationToken).ConfigureAwait(false);
        }

        CertificateBinder binder = new(provisioner, _binderOptions);
        _binder = binder;
        binder.ConsumerFailed += OnCertificateConsumerFailed;
        foreach (ICertificateConsumer consumer in _certificateConsumers)
        {
            binder.Add(consumer);
        }
    }

    /// <summary>The binder reports this under its lock on whichever thread applied the certificate: queued for <see cref="PollAll"/>.</summary>
    private void OnCertificateConsumerFailed(CertificateConsumerFailure failure)
    {
        if (CertificateConsumerFailed is not null)
        {
            _consumerFailures.Enqueue(in failure);
        }
    }

    private void StartHttp(HttpSideOptions side)
    {
        HttpServerOptions options = new();
        IPEndPoint endPoint = side.EndPoint.Address.Equals(IPAddress.IPv6Any) && !Socket.OSSupportsIPv6
            ? new IPEndPoint(IPAddress.Any, side.EndPoint.Port)
            : side.EndPoint;
        options.Listen(endPoint);
        foreach (IHttpHandler handler in side.Handlers)
        {
            options.Use(handler);
        }

        if (side.EnableHealth)
        {
            options.Use(new HealthHandler(side.HealthPath));
        }

        if (side.RedirectToHttps)
        {
            options.Use(new RedirectToHttpsHandler(side.HttpsPort));
        }

        side.Configure?.Invoke(options);
        HttpServer http = new(options);
        _http = http;
        http.Start();
    }

    private async Task StopSideServicesAsync()
    {
        if (Interlocked.Exchange(ref _http, null) is { } http)
        {
            await http.DisposeAsync().ConfigureAwait(false);
        }

        if (Interlocked.Exchange(ref _binder, null) is { } binder)
        {
            binder.ConsumerFailed -= OnCertificateConsumerFailed;
            await binder.DisposeAsync().ConfigureAwait(false);
        }

        if (Interlocked.Exchange(ref _provisioner, null) is { } provisioner)
        {
            await provisioner.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>The HTTP side endpoint's configuration, captured when the server was created.</summary>
    private sealed class HttpSideOptions
    {
        public HttpSideOptions(ServerHttpOptions options, int listenPort)
        {
            EndPoint = options.EndPoint;
            RedirectToHttps = options.RedirectToHttps;
            HttpsPort = options.HttpsPort ?? (listenPort is > 0 and <= 65535 ? listenPort : 443);
            EnableHealth = options.EnableHealthEndpoint;
            HealthPath = options.HealthPath;
            Handlers = [.. options.Handlers];
            Configure = options.Configure;
        }

        public IPEndPoint EndPoint { get; }

        public bool RedirectToHttps { get; }

        public int HttpsPort { get; }

        public bool EnableHealth { get; }

        public string HealthPath { get; }

        public IHttpHandler[] Handlers { get; }

        public Action<HttpServerOptions>? Configure { get; }
    }
}
