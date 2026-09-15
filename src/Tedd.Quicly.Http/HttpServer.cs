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

    /// <summary>Binds every endpoint and starts accepting. Throws when any bind fails (nothing stays bound).</summary>
    public void Start()
    {
        if (Interlocked.CompareExchange(ref _state, 1, 0) != 0)
            throw new InvalidOperationException("The server has already been started.");
        try
        {
            if (_options.Endpoints.Count == 0)
                throw new InvalidOperationException("At least one endpoint is required.");
            _options.Limits.Validate();
            if (_options.AllowedMethods.Count == 0)
                throw new InvalidOperationException("At least one request method must be allowed.");
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
            for (int i = 0; i < _listeners.Count; i++)
            {
                var listener = _listeners[i];
                var endpoint = _options.Endpoints[i];
                _acceptLoops.Add(Task.Run(() => AcceptLoopAsync(listener, endpoint)));
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
            catch (OperationCanceledException)
            {
                slots.Release();
                return;
            }
            catch (ObjectDisposedException)
            {
                slots.Release();
                return;
            }
            catch (SocketException ex)
            {
                slots.Release();
                if (token.IsCancellationRequested)
                    return;
                _options.OnError?.Invoke(ex);
                continue;
            }

            var remote = socket.RemoteEndPoint as IPEndPoint;
            var connection = new HttpConnection(this, socket, endpoint, remote);
            if (!TryAdmit(connection, remote?.Address))
            {
                Interlocked.Increment(ref _rejected);
                socket.Dispose();
                slots.Release();
                continue;
            }

            Interlocked.Increment(ref _accepted);
            try
            {
                socket.NoDelay = true;
            }
            catch (SocketException)
            {
            }
            connection.Start();
        }
    }

    private bool TryAdmit(HttpConnection connection, IPAddress? address)
    {
        lock (_lock)
        {
            if (address is not null)
            {
                _perAddress.TryGetValue(address, out int count);
                if (count >= _options.Limits.MaxConnectionsPerAddress)
                    return false;
                _perAddress[address] = count + 1;
            }
            _connections[connection.Id] = connection;
            return true;
        }
    }

    internal void OnConnectionClosed(HttpConnection connection)
    {
        lock (_lock)
        {
            _connections.Remove(connection.Id);
            var address = connection.RemoteEndPoint?.Address;
            if (address is not null && _perAddress.TryGetValue(address, out int count))
            {
                if (count <= 1)
                    _perAddress.Remove(address);
                else
                    _perAddress[address] = count - 1;
            }
        }
        _slots?.Release();
    }

    internal void OnAcmeTlsAlpnHandshake() => Interlocked.Increment(ref _acmeHandshakes);

    internal void OnHandshakeFailure() => Interlocked.Increment(ref _handshakeFailures);

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

    private async Task StopCoreAsync(CancellationToken cancellationToken)
    {
        _acceptCts.Cancel();
        foreach (var listener in _listeners)
            listener.Dispose();
        await Task.WhenAll(_acceptLoops).ConfigureAwait(false);

        _idleCts.Cancel();
        HttpConnection[] connections;
        lock (_lock)
            connections = [.. _connections.Values];
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
