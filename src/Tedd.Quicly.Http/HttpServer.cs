using System.Net;
using System.Net.Sockets;
using System.Text;
using Tedd.Quicly.Http.Internal;

namespace Tedd.Quicly.Http;

/// <summary>
/// A small HTTP/1.1 (and 1.0) server over raw sockets. One accept loop per endpoint, one task per connection,
/// sequential request handling per connection (pipelining-safe), TLS via <see cref="System.Net.Security.SslStream"/>.
/// </summary>
public sealed class HttpServer : IAsyncDisposable
{
    private readonly HttpServerOptions _options;
    private readonly List<Socket> _listeners = [];
    private readonly List<Task> _acceptLoops = [];
    private readonly List<IPEndPoint> _bound = [];
    private readonly Dictionary<long, HttpConnection> _connections = [];
    private readonly Dictionary<IPAddress, int> _perAddress = [];
    private readonly Lock _lock = new();
    private readonly CancellationTokenSource _acceptCts = new();
    private readonly CancellationTokenSource _idleCts = new();
    private readonly CancellationTokenSource _abortCts = new();
    private SemaphoreSlim? _slots;
    private Task? _stopTask;
    private int _state; // 0 created, 1 running, 2 stopping, 3 stopped
    private long _accepted;
    private long _rejected;
    private long _acmeHandshakes;
    private long _handshakeFailures;
    private long _handshakeTimeouts;
    private long _requestTimeouts;
    private long _protocolErrors;

    /// <summary>Creates a server; call <see cref="Start"/> to bind and accept.</summary>
    public HttpServer(HttpServerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
    }

    /// <summary>The options this server was created with. Do not mutate after <see cref="Start"/>.</summary>
    public HttpServerOptions Options => _options;

    /// <summary>Actual bound endpoints (resolves port 0), in the order of <see cref="HttpServerOptions.Endpoints"/>. Empty before <see cref="Start"/>.</summary>
    public IReadOnlyList<IPEndPoint> BoundEndpoints => _bound;

    /// <summary>Whether the server is accepting connections.</summary>
    public bool IsRunning => Volatile.Read(ref _state) == 1;

    /// <summary>Whether shutdown has begun (new responses carry <c>Connection: close</c>).</summary>
    public bool IsStopping => Volatile.Read(ref _state) >= 2;

    /// <summary>Number of open connections.</summary>
    public int ActiveConnections
    {
        get
        {
            lock (_lock)
                return _connections.Count;
        }
    }

    /// <summary>Total connections accepted since start (rejected ones excluded).</summary>
    public long TotalConnectionsAccepted => Volatile.Read(ref _accepted);

    /// <summary>Connections closed immediately because the remote address had <see cref="HttpServerLimits.MaxConnectionsPerAddress"/> open already.</summary>
    public long ConnectionsRejected => Volatile.Read(ref _rejected);

    /// <summary>Number of completed <c>acme-tls/1</c> validation handshakes.</summary>
    public long AcmeTlsAlpnHandshakes => Volatile.Read(ref _acmeHandshakes);

    /// <summary>TLS connections that never completed a handshake (non-TLS bytes, oversized or malformed ClientHello, handshake failure or timeout).</summary>
    public long HandshakeFailures => Volatile.Read(ref _handshakeFailures);

    /// <summary>
    /// Handshakes that ran out of <see cref="HttpServerLimits.TlsHandshakeTimeout"/> (a subset of
    /// <see cref="HandshakeFailures"/>): a client that stalled, or a server whose credential setup was too slow. Timeouts
    /// after a complete ClientHello are also reported through <see cref="HttpServerOptions.OnError"/>.
    /// </summary>
    public long HandshakeTimeouts => Volatile.Read(ref _handshakeTimeouts);

    /// <summary>Requests whose header section did not arrive within <see cref="HttpServerLimits.HeaderReadTimeout"/> (answered with 408).</summary>
    public long RequestTimeouts => Volatile.Read(ref _requestTimeouts);

    /// <summary>Requests rejected by the server itself before any handler ran (400, 405, 411, 413, 414, 417, 431, 501, 505).</summary>
    public long ProtocolErrors => Volatile.Read(ref _protocolErrors);

    internal CancellationToken IdleToken => _idleCts.Token;

    internal CancellationToken AbortToken => _abortCts.Token;

    internal byte[] ServerHeaderLine { get; private set; } = [];

    /// <summary>Largest request body accepted: the server-wide limit or the largest handler opt-in.</summary>
    internal long EffectiveMaxRequestBodyBytes { get; private set; }

    /// <summary>Pre-rendered <c>Allow</c> header value for 405 responses.</summary>
    internal string AllowHeaderValue { get; private set; } = string.Empty;

    /// <summary>The listening sockets, in endpoint order.</summary>
    internal IReadOnlyList<Socket> Listeners => _listeners;

    /// <summary>Binds every endpoint and starts accepting. Throws when any bind fails (nothing stays bound).</summary>
    /// <exception cref="InvalidOperationException">
    /// The server was already started, or the options are inconsistent: no endpoint, no allowed method, a method that
    /// is not an HTTP token, a <c>Server</c> value with control characters, or a TLS endpoint without certificates.
    /// </exception>
    public void Start()
    {
        if (Interlocked.CompareExchange(ref _state, 1, 0) != 0)
            throw new InvalidOperationException("The server has already been started.");
        try
        {
            if (_options.Endpoints.Count == 0)
                throw new InvalidOperationException("At least one endpoint is required.");
            _options.Limits.Validate();
            HttpServerLimits.ValidateTimeout(_options.ShutdownTimeout, allowZero: true, nameof(HttpServerOptions.ShutdownTimeout));
            if (_options.AllowedMethods.Count == 0)
                throw new InvalidOperationException("At least one request method must be allowed.");
            foreach (var method in _options.AllowedMethods)
            {
                if (!Parsing.HeaderTokens.IsToken(method))
                    throw new InvalidOperationException("Allowed method '" + method + "' is not a valid HTTP token.");
            }
            if (_options.AddServerHeader && (string.IsNullOrEmpty(_options.ServerHeaderValue) || !Parsing.HeaderTokens.IsFieldValue(_options.ServerHeaderValue)))
                throw new InvalidOperationException("ServerHeaderValue must be a non-empty header value without control characters.");
            foreach (var ep in _options.Endpoints)
            {
                if (ep.Tls is { } tls && tls.CertificateSelector is null && tls.CertificateProvider is null && tls.CertificateSource is null)
                    throw new InvalidOperationException("TLS endpoint " + ep.EndPoint + " has no certificate source, provider or selector.");
            }

            long maxBody = _options.Limits.MaxRequestBodyBytes;
            foreach (var handler in _options.Handlers)
            {
                if (handler is IHttpBodyHandler body)
                {
                    HttpServerLimits.ValidateBodyLimit(body.MaxRequestBodyBytes);
                    maxBody = Math.Max(maxBody, body.MaxRequestBodyBytes);
                }
            }
            EffectiveMaxRequestBodyBytes = maxBody;
            AllowHeaderValue = string.Join(", ", _options.AllowedMethods);
            ServerHeaderLine = _options.AddServerHeader ? Encoding.Latin1.GetBytes("Server: " + _options.ServerHeaderValue + "\r\n") : [];
            _slots = new SemaphoreSlim(_options.Limits.MaxConnections, _options.Limits.MaxConnections);

            foreach (var ep in _options.Endpoints)
            {
                var socket = new Socket(ep.EndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
                try
                {
                    if (ep.EndPoint.AddressFamily == AddressFamily.InterNetworkV6 && ep.EndPoint.Address.Equals(IPAddress.IPv6Any))
                        socket.DualMode = true;
                    socket.Bind(ep.EndPoint);
                    socket.Listen(_options.ListenBacklog);
                }
                catch
                {
                    socket.Dispose();
                    throw;
                }
                _listeners.Add(socket);
                _bound.Add((IPEndPoint)socket.LocalEndPoint!);
            }
            // The caller's ExecutionContext (AsyncLocals such as Activity.Current) must not leak into every
            // connection the server will ever accept.
            using (ExecutionContext.SuppressFlow())
            {
                for (int i = 0; i < _listeners.Count; i++)
                {
                    var listener = _listeners[i];
                    var endpoint = _options.Endpoints[i];
                    _acceptLoops.Add(Task.Run(() => AcceptLoopAsync(listener, endpoint)));
                }
            }
        }
        catch
        {
            foreach (var l in _listeners)
                l.Dispose();
            _listeners.Clear();
            _bound.Clear();
            Volatile.Write(ref _state, 3);
            throw;
        }
    }

    private async Task AcceptLoopAsync(Socket listener, HttpEndpointOptions endpoint)
    {
        var slots = _slots!;
        var token = _acceptCts.Token;
        while (true)
        {
            try
            {
                await slots.WaitAsync(token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                return;
            }

            Socket socket;
            try
            {
                socket = await listener.AcceptAsync(token).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is OperationCanceledException or ObjectDisposedException or SocketException)
            {
                slots.Release();
                if (ex is not SocketException || token.IsCancellationRequested)
                    return; // stopping, or the listener is gone
                // A transient accept failure (a connection reset before accept completed, descriptor exhaustion):
                // report it and back off briefly instead of spinning on a failing listener.
                ReportError(ex);
                await Task.Delay(20).ConfigureAwait(false);
                continue;
            }

            IPEndPoint? remote = null;
            IPAddress? key = null;
            HttpConnection connection;
            try
            {
                remote = socket.RemoteEndPoint as IPEndPoint;
                key = LimitKey(remote);
                if (!TryReserveAddress(key))
                {
                    Interlocked.Increment(ref _rejected);
                    socket.Dispose();
                    slots.Release();
                    continue;
                }

                // Only admitted sockets get a connection object (pooled buffers, cancellation sources), so a flood of
                // rejected connects costs nothing beyond the accept itself.
                BeforeConnectionCreated?.Invoke(socket);
                connection = new HttpConnection(this, socket, endpoint, remote);
            }
            catch (Exception ex)
            {
                // getsockname on a socket the peer already reset, or resource exhaustion: drop this socket, undo its
                // reservation and keep accepting. The accept loop must never fault.
                if (key is not null)
                    ReleaseAddress(key);
                socket.Dispose();
                slots.Release();
                ReportError(ex);
                continue;
            }
            lock (_lock)
                _connections[connection.Id] = connection;
            Interlocked.Increment(ref _accepted);
            connection.Start(); // hands off to the thread pool; socket options are applied inside the connection
        }
    }

    /// <summary>Test seam: invoked for every admitted socket just before its connection object is created.</summary>
    internal Action<Socket>? BeforeConnectionCreated { get; set; }

    /// <summary>
    /// Reports an unexpected error to <see cref="HttpServerOptions.OnError"/>. A callback that throws is ignored: user
    /// code must not be able to fault an accept loop or a connection's teardown.
    /// </summary>
    internal void ReportError(Exception exception)
    {
        try
        {
            _options.OnError?.Invoke(exception);
        }
        catch (Exception)
        {
            // Nowhere left to report it; swallowing keeps the server alive.
        }
    }

    /// <summary>
    /// The per-address limit key. IPv4-mapped IPv6 addresses (seen on dual-mode listeners) count as their IPv4
    /// address, so one client cannot double its allowance by connecting to both an IPv4 and a dual-mode endpoint.
    /// </summary>
    internal static IPAddress LimitKey(IPEndPoint? remote)
    {
        var address = remote?.Address ?? IPAddress.None;
        return address.IsIPv4MappedToIPv6 ? address.MapToIPv4() : address;
    }

    private bool TryReserveAddress(IPAddress address)
    {
        lock (_lock)
        {
            _perAddress.TryGetValue(address, out int count);
            if (count >= _options.Limits.MaxConnectionsPerAddress)
                return false;
            _perAddress[address] = count + 1;
            return true;
        }
    }

    private void ReleaseAddress(IPAddress address)
    {
        lock (_lock)
        {
            if (_perAddress.TryGetValue(address, out int count) && count > 1)
                _perAddress[address] = count - 1;
            else
                _perAddress.Remove(address);
        }
    }

    internal void OnConnectionClosed(HttpConnection connection)
    {
        lock (_lock)
        {
            _connections.Remove(connection.Id);
            ReleaseAddress(LimitKey(connection.RemoteEndPoint)); // Lock is reentrant
        }
        _slots?.Release();
    }

    /// <summary>Number of remote addresses currently holding at least one connection (tests).</summary>
    internal int TrackedAddressCount
    {
        get
        {
            lock (_lock)
                return _perAddress.Count;
        }
    }

    internal void OnAcmeTlsAlpnHandshake() => Interlocked.Increment(ref _acmeHandshakes);

    internal void OnHandshakeFailure() => Interlocked.Increment(ref _handshakeFailures);

    internal void OnHandshakeTimeout()
    {
        Interlocked.Increment(ref _handshakeTimeouts);
        Interlocked.Increment(ref _handshakeFailures);
    }

    internal void OnRequestTimeout() => Interlocked.Increment(ref _requestTimeouts);

    internal void OnProtocolError() => Interlocked.Increment(ref _protocolErrors);

    /// <summary>
    /// Stops accepting, closes idle connections, lets in-flight requests finish (their responses carry
    /// <c>Connection: close</c>), then aborts whatever is still running once <paramref name="cancellationToken"/>
    /// fires or <see cref="HttpServerOptions.ShutdownTimeout"/> elapses.
    /// </summary>
    public Task StopAsync(CancellationToken cancellationToken = default)
    {
        lock (_lock)
        {
            if (_stopTask is not null)
                return _stopTask;
            int previous = Interlocked.Exchange(ref _state, 2);
            if (previous is 0 or 3)
            {
                Volatile.Write(ref _state, 3);
                _stopTask = Task.CompletedTask;
                return _stopTask;
            }
            _stopTask = StopCoreAsync(cancellationToken);
            return _stopTask;
        }
    }

    /// <summary>A point-in-time copy of the open connections.</summary>
    internal HttpConnection[] SnapshotConnections()
    {
        lock (_lock)
            return [.. _connections.Values];
    }

    private async Task StopCoreAsync(CancellationToken cancellationToken)
    {
        _acceptCts.Cancel();
        foreach (var listener in _listeners)
            listener.Dispose();
        await Task.WhenAll(_acceptLoops).ConfigureAwait(false);

        _idleCts.Cancel();
        var connections = SnapshotConnections();
        var tasks = new Task[connections.Length];
        for (int i = 0; i < connections.Length; i++)
            tasks[i] = connections[i].Completion;

        using var grace = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        grace.CancelAfter(_options.ShutdownTimeout);
        try
        {
            await Task.WhenAll(tasks).WaitAsync(grace.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            _abortCts.Cancel();
            foreach (var c in connections)
                c.Abort();
            await Task.WhenAll(tasks).ConfigureAwait(false);
        }
        Volatile.Write(ref _state, 3);
    }

    /// <summary>Stops the server (see <see cref="StopAsync"/>) and releases its resources.</summary>
    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
        _acceptCts.Dispose();
        _idleCts.Dispose();
        _abortCts.Dispose();
        _slots?.Dispose();
    }
}
