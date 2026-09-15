using System.Buffers;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Tedd.Quicly.Http.Parsing;
using Tedd.Quicly.Http.Tls;

namespace Tedd.Quicly.Http.Internal;

/// <summary>One accepted socket: optional TLS handshake, then a sequential request/response loop.</summary>
internal sealed class HttpConnection
{
    private static readonly byte[] ContinueResponse = "HTTP/1.1 100 Continue\r\n\r\n"u8.ToArray();
    private static long s_nextId;

    private readonly HttpServer _server;
    private readonly HttpServerOptions _options;
    private readonly HttpServerLimits _limits;
    private readonly HttpEndpointOptions _endpoint;
    private readonly Socket _socket;
    private readonly CancellationTokenSource _abortCts;
    private readonly CancellationTokenSource _idleCts;
    private readonly HttpRequestContext _context;
    private readonly HttpResponse _response;
    private readonly HttpRequestBodyStream _bodyStream;
    private readonly int _maxHeaderBlock;

    private CancellationTokenSource _ioCts;
    private Stream _stream = Stream.Null;
    private byte[] _buffer;
    private int _start;
    private int _end;
    private ParsedRequest _parsed;

    private long _bodyRemaining;
    private bool _expectContinue;
    private bool _continueSent;
    private bool _protocolViolation;
    private byte[]? _bodyBuffer;
    private ReadOnlyMemory<byte> _bodyMemory;
    private bool _bodyRead;
    private int _closed;

    public HttpConnection(HttpServer server, Socket socket, HttpEndpointOptions endpoint, IPEndPoint? remoteEndPoint)
    {
        _server = server;
        _options = server.Options;
        _limits = _options.Limits;
        _endpoint = endpoint;
        _socket = socket;
        Id = Interlocked.Increment(ref s_nextId);
        _abortCts = CancellationTokenSource.CreateLinkedTokenSource(server.AbortToken);
        _idleCts = CancellationTokenSource.CreateLinkedTokenSource(server.IdleToken, _abortCts.Token);
        _ioCts = CancellationTokenSource.CreateLinkedTokenSource(_abortCts.Token);
        _maxHeaderBlock = _limits.MaxRequestLineBytes + 2 + _limits.MaxHeadersBytes;
        _buffer = ArrayPool<byte>.Shared.Rent(_maxHeaderBlock);
        _parsed = new ParsedRequest(_limits.MaxHeaderCount);
        _response = new HttpResponse(this);
        _context = new HttpRequestContext(this, _response);
        _bodyStream = new HttpRequestBodyStream(this);
        RemoteEndPoint = remoteEndPoint;
        LocalEndPoint = socket.LocalEndPoint as IPEndPoint;
    }

    public long Id { get; }

    public IPEndPoint? RemoteEndPoint { get; }

    public IPEndPoint? LocalEndPoint { get; }

    public bool IsSecure { get; private set; }

    public string? TlsServerName { get; private set; }

    public CancellationToken AbortToken => _abortCts.Token;

    public HttpRequestBodyStream BodyStream => _bodyStream;

    public bool IsStopping => _server.IsStopping;

    /// <summary>
    /// A response must advertise <c>Connection: close</c> when the request body state makes reuse impossible: an
    /// <c>Expect: 100-continue</c> body the client may never send, or a body read that failed or was cancelled.
    /// </summary>
    public bool MustCloseAfterResponse => _bodyRemaining < 0 || (_expectContinue && !_continueSent && _bodyRemaining > 0);

    public ReadOnlySpan<byte> ServerHeaderLine => _server.ServerHeaderLine;

    /// <summary>Completes when the connection has been fully closed and released.</summary>
    public Task Completion { get; private set; } = Task.CompletedTask;

    public void Start() => Completion = RunAsync();

    /// <summary>Closes the socket immediately, interrupting any pending I/O.</summary>
    public void Abort()
    {
        if (!_abortCts.IsCancellationRequested)
        {
            try
            {
                _abortCts.Cancel();
            }
            catch (ObjectDisposedException)
            {
            }
        }
        try
        {
            _socket.Dispose();
        }
        catch (ObjectDisposedException)
        {
        }
    }

    internal void MarkProtocolViolation() => _protocolViolation = true;

    private async Task RunAsync()
    {
        try
        {
            var stream = await CreateStreamAsync().ConfigureAwait(false);
            if (stream is not null)
            {
                _stream = stream;
                await ProcessRequestsAsync().ConfigureAwait(false);
            }
        }
        catch (Exception ex) when (ex is IOException or SocketException or OperationCanceledException or ObjectDisposedException or AuthenticationException)
        {
            // Client went away, timed out, or failed the handshake: expected on a public port, nothing to report.
        }
        catch (Exception ex)
        {
            _options.OnError?.Invoke(ex);
        }
        finally
        {
            Close();
        }
    }

    private void Close()
    {
        if (Interlocked.Exchange(ref _closed, 1) != 0)
            return;
        try
        {
            _abortCts.Cancel();
        }
        catch (ObjectDisposedException)
        {
        }
        try
        {
            _stream.Dispose();
        }
        catch (Exception ex) when (ex is IOException or SocketException or ObjectDisposedException)
        {
        }
        try
        {
            _socket.Dispose();
        }
        catch (ObjectDisposedException)
        {
        }
        ReleaseRequestResources();
        var buffer = _buffer;
        _buffer = [];
        ArrayPool<byte>.Shared.Return(buffer);
        _response.DisposeBuffers();
        _ioCts.Dispose();
        _idleCts.Dispose();
        _abortCts.Dispose();
        _server.OnConnectionClosed(this);
    }

    // ------------------------------------------------------------------ TLS

    private async ValueTask<Stream?> CreateStreamAsync()
    {
        var network = new NetworkStream(_socket, ownsSocket: true);
        _stream = network; // disposed by Close() whatever happens below
        var tls = _endpoint.Tls;
        if (tls is null)
            return network;

        // Peek the ClientHello so that ALPN acme-tls/1 can pick the challenge certificate (SslStream's own
        // selection callback only exposes the SNI host, not the offered protocols). The whole peek plus the
        // handshake must finish within HeaderReadTimeout.
        long deadline = Deadline(_limits.HeaderReadTimeout);
        var rented = ArrayPool<byte>.Shared.Rent(ClientHelloParser.MaxPeekBytes);
        var assembled = ArrayPool<byte>.Shared.Rent(ClientHelloParser.MaxClientHelloLength);
        bool handedOff = false;
        int filled = 0;
        bool parsed = false;
        ClientHelloInfo hello = default;
        try
        {
            while (true)
            {
                var status = ClientHelloParser.TryAssemble(rented.AsSpan(0, filled), assembled.AsSpan(0, ClientHelloParser.MaxClientHelloLength), out int handshakeLength, out _);
                if (status == ClientHelloAssembleStatus.Complete)
                {
                    parsed = ClientHelloParser.TryParse(assembled.AsSpan(0, handshakeLength), out hello);
                    break;
                }
                if (status != ClientHelloAssembleStatus.NeedMore || filled == ClientHelloParser.MaxPeekBytes)
                {
                    // Not TLS, or a ClientHello we refuse to buffer: drop the connection without a handshake attempt.
                    _server.OnHandshakeFailure();
                    return null;
                }
                var remaining = Remaining(deadline);
                if (remaining <= TimeSpan.Zero)
                {
                    _server.OnHandshakeFailure();
                    return null;
                }
                int n;
                try
                {
                    n = await ReadWithTimeoutAsync(network, rented.AsMemory(filled, ClientHelloParser.MaxPeekBytes - filled), remaining, IoCts()).ConfigureAwait(false);
                }
                catch (Exception)
                {
                    _server.OnHandshakeFailure();
                    throw;
                }
                if (n == 0)
                {
                    if (filled > 0)
                        _server.OnHandshakeFailure();
                    return null;
                }
                filled += n;
            }
            handedOff = true;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(assembled);
            if (!handedOff)
                ArrayPool<byte>.Shared.Return(rented);
        }

        // From here on the rented buffer belongs to the PrefixedStream, which returns it once replayed.
        var ssl = new SslStream(new PrefixedStream(network, rented, filled), leaveInnerStreamOpen: false);
        _stream = ssl;
        var sslOptions = new SslServerAuthenticationOptions
        {
            ClientCertificateRequired = false,
            EnabledSslProtocols = tls.EnabledSslProtocols,
            CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
            AllowRenegotiation = false,
        };
        bool acme = false;
        if (parsed && hello.Offers("acme-tls/1") && tls.Alpn01Responder is { } responder && responder.TryGetCertificate(hello.ServerName, out var challengeCertificate))
        {
            sslOptions.ServerCertificate = challengeCertificate;
            sslOptions.ApplicationProtocols = TlsAlpn01Responder.AcmeOnlyProtocols;
            acme = true;
        }
        else
        {
            sslOptions.ServerCertificateSelectionCallback = tls.SelectionCallback;
            if (tls.ProtocolList.Count > 0)
                sslOptions.ApplicationProtocols = tls.ProtocolList;
        }

        var handshakeBudget = Remaining(deadline);
        if (handshakeBudget <= TimeSpan.Zero)
        {
            _server.OnHandshakeFailure();
            return null;
        }
        var cts = IoCts();
        cts.CancelAfter(handshakeBudget);
        try
        {
            await ssl.AuthenticateAsServerAsync(sslOptions, cts.Token).ConfigureAwait(false);
        }
        catch
        {
            _server.OnHandshakeFailure();
            throw;
        }
        finally
        {
            cts.CancelAfter(Timeout.InfiniteTimeSpan);
        }

        if (acme)
        {
            _server.OnAcmeTlsAlpnHandshake();
            return null;
        }
        IsSecure = true;
        TlsServerName = string.IsNullOrEmpty(ssl.TargetHostName) ? (parsed ? hello.ServerName : null) : ssl.TargetHostName;
        return ssl;
    }

    // ------------------------------------------------------------------ request loop

    private enum HeaderBlockResult : byte
    {
        Found,
        Eof,
        TooLarge,
        Timeout,
    }

    private async Task ProcessRequestsAsync()
    {
        var handlers = _options.Handlers;
        int requests = 0;
        while (true)
        {
            var read = await ReadHeaderBlockAsync(requests == 0).ConfigureAwait(false);
            if (read == HeaderBlockResult.Eof)
                return;
            if (read == HeaderBlockResult.Timeout)
            {
                _server.OnRequestTimeout();
                await SendErrorAsync(408).ConfigureAwait(false);
                return;
            }

            var buffered = _buffer.AsSpan(_start, _end - _start);
            var status = HttpParser.TryParse(buffered, _limits.MaxRequestLineBytes, _limits.MaxHeadersBytes, ref _parsed);
            int error = status switch
            {
                HttpParseStatus.Ok => 0,
                HttpParseStatus.Invalid => 400,
                HttpParseStatus.RequestLineTooLong => 414,
                HttpParseStatus.VersionNotSupported => 505,
                _ => 431, // HeadersTooLarge, or NeedMore after the block limit was hit
            };
            if (error != 0)
            {
                _server.OnProtocolError();
                await SendErrorAsync(error).ConfigureAwait(false);
                return;
            }

            requests++;
            error = Materialize(buffered);
            _start += _parsed.Consumed;
            if (error != 0)
            {
                _server.OnProtocolError();
                await SendErrorAsync(error).ConfigureAwait(false);
                return;
            }

            bool keepAlive = WantsKeepAlive() && requests < _limits.MaxRequestsPerConnection && !_server.IsStopping;
            bool isHead = string.Equals(_context.Method, "HEAD", StringComparison.Ordinal);
            _response.Reset(_context.Version, isHead, closeAfter: !keepAlive);

            bool handled = false;
            try
            {
                for (int i = 0; i < handlers.Count; i++)
                {
                    if (await handlers[i].TryHandleAsync(_context, AbortToken).ConfigureAwait(false))
                    {
                        handled = true;
                        break;
                    }
                }
                if (!handled)
                {
                    if (_response.HasStarted)
                        return; // a handler wrote but declined ownership: the framing cannot be trusted
                    _response.StatusCode = 404;
                    await _response.SendTextAsync("404 Not Found", cancellationToken: AbortToken).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (AbortToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex) when (ex is not (IOException or SocketException or ObjectDisposedException or OperationCanceledException))
            {
                _options.OnError?.Invoke(ex);
                if (_response.HasStarted)
                    return;
                await SendErrorAsync(500).ConfigureAwait(false);
                return;
            }

            if (!_response.IsCompleted)
                await _response.CompleteAsync(AbortToken).ConfigureAwait(false);
            if (_response.CloseConnection || _protocolViolation)
                keepAlive = false;

            if (_bodyRemaining > 0)
            {
                if (_expectContinue && !_continueSent)
                    keepAlive = false; // the client may never send the body it announced
                else if (keepAlive)
                    await DrainBodyAsync().ConfigureAwait(false);
            }
            else if (_bodyRemaining < 0)
            {
                keepAlive = false;
            }

            ReleaseRequestResources();
            if (!keepAlive)
                return;
        }
    }

    private async ValueTask<HeaderBlockResult> ReadHeaderBlockAsync(bool firstRequest)
    {
        int scanFrom = _start;
        long deadline = firstRequest ? Deadline(_limits.HeaderReadTimeout) : 0;
        while (true)
        {
            int available = _end - _start;
            if (available > 0)
            {
                int idx = _buffer.AsSpan(scanFrom, _end - scanFrom).IndexOf(HttpParser.HeaderTerminator);
                if (idx >= 0)
                    return HeaderBlockResult.Found;
                if (available >= _maxHeaderBlock)
                    return HeaderBlockResult.TooLarge;
                scanFrom = Math.Max(_start, _end - 2);
            }

            if (_start > 0)
            {
                _buffer.AsSpan(_start, available).CopyTo(_buffer);
                scanFrom -= _start;
                _end = available;
                _start = 0;
            }

            int n;
            try
            {
                if (available == 0 && !firstRequest)
                {
                    // Waiting for the next request on a persistent connection: idle timeout, closable on shutdown.
                    n = await ReadWithTimeoutAsync(_stream, _buffer.AsMemory(_end), _limits.KeepAliveTimeout, _idleCts).ConfigureAwait(false);
                    deadline = Deadline(_limits.HeaderReadTimeout);
                }
                else
                {
                    var remaining = Remaining(deadline);
                    if (remaining <= TimeSpan.Zero)
                        return HeaderBlockResult.Timeout;
                    n = await ReadWithTimeoutAsync(_stream, _buffer.AsMemory(_end), remaining, available == 0 ? _idleCts : IoCts()).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (available > 0 && !AbortToken.IsCancellationRequested)
            {
                // A partial request that stalled: answer 408 rather than dropping silently.
                return HeaderBlockResult.Timeout;
            }
            if (n == 0)
                return HeaderBlockResult.Eof;
            _end += n;
        }
    }

    private static long Deadline(TimeSpan timeout)
        => timeout == Timeout.InfiniteTimeSpan ? long.MaxValue : Environment.TickCount64 + (long)timeout.TotalMilliseconds;

    private static TimeSpan Remaining(long deadline)
        => deadline == long.MaxValue ? Timeout.InfiniteTimeSpan : TimeSpan.FromMilliseconds(deadline - Environment.TickCount64);

    /// <summary>Fills the request context from the parsed ranges. Returns 0 or the status code of the error to send.</summary>
    private int Materialize(ReadOnlySpan<byte> buffer)
    {
        ref var p = ref _parsed;
        var ctx = _context;
        ctx.Reset();
        ctx.Method = KnownStrings.InternMethod(buffer.Slice(p.MethodStart, p.MethodLength));
        ctx.Version = p.Version;

        var target = buffer.Slice(p.TargetStart, p.TargetLength);
        ctx.RawTarget = Encoding.Latin1.GetString(target);
        string? authority = null;
        ReadOnlySpan<byte> pathBytes;
        if (target[0] == (byte)'/')
        {
            pathBytes = target;
        }
        else if (target.Length == 1 && target[0] == (byte)'*' && string.Equals(ctx.Method, "OPTIONS", StringComparison.Ordinal))
        {
            pathBytes = target;
        }
        else if (TrySplitAbsoluteForm(target, out authority, out pathBytes))
        {
            // absolute-form: path extracted, authority remembered for Host
        }
        else
        {
            return 400;
        }

        int q = pathBytes.IndexOf((byte)'?');
        if (q >= 0)
        {
            ctx.QueryString = Encoding.Latin1.GetString(pathBytes[(q + 1)..]);
            pathBytes = pathBytes[..q];
        }
        if (!PercentDecoder.TryDecode(pathBytes, out var path))
            return 400;
        ctx.Path = path;

        var headers = ctx.Headers;
        var ranges = p.Headers;
        for (int i = 0; i < p.HeaderCount; i++)
        {
            ref var r = ref ranges[i];
            headers.AddUnchecked(
                KnownStrings.InternHeaderName(buffer.Slice(r.NameStart, r.NameLength)),
                KnownStrings.InternHeaderValue(buffer.Slice(r.ValueStart, r.ValueLength)));
        }

        long contentLength = 0;
        bool haveContentLength = false;
        string? transferEncoding = null;
        int hostCount = 0;
        for (int i = 0; i < headers.Count; i++)
        {
            var h = headers[i];
            if (string.Equals(h.Name, "Content-Length", StringComparison.OrdinalIgnoreCase))
            {
                if (!HeaderTokens.TryParseContentLength(h.Value, out long v))
                    return 400;
                if (haveContentLength && v != contentLength)
                    return 400;
                contentLength = v;
                haveContentLength = true;
            }
            else if (string.Equals(h.Name, "Transfer-Encoding", StringComparison.OrdinalIgnoreCase))
            {
                transferEncoding = h.Value;
            }
            else if (string.Equals(h.Name, "Host", StringComparison.OrdinalIgnoreCase))
            {
                hostCount++;
            }
        }
        if (hostCount > 1)
            return 400;
        if (hostCount == 0)
        {
            if (authority is not null)
                headers.AddUnchecked("Host", authority);
            else if (ctx.Version == HttpProtocolVersion.Http11)
                return 400;
        }
        if (transferEncoding is not null)
        {
            if (haveContentLength)
                return 400;
            return string.Equals(transferEncoding, "chunked", StringComparison.OrdinalIgnoreCase) ? 411 : 501;
        }
        if (!_options.AllowedMethods.Contains(ctx.Method))
            return 405;
        if (contentLength > _server.EffectiveMaxRequestBodyBytes)
            return 413;

        ctx.ContentLength = contentLength;
        _bodyRemaining = contentLength;
        _expectContinue = false;
        _continueSent = false;
        if (ctx.Version == HttpProtocolVersion.Http11 && headers.TryGetValue("Expect", out var expect))
        {
            if (!string.Equals(expect, "100-continue", StringComparison.OrdinalIgnoreCase))
                return 417;
            _expectContinue = contentLength > 0;
        }
        return 0;
    }

    private static bool TrySplitAbsoluteForm(ReadOnlySpan<byte> target, out string? authority, out ReadOnlySpan<byte> path)
    {
        authority = null;
        path = default;
        int schemeEnd;
        if (target.Length > 7 && Ascii.EqualsIgnoreCase(target[..7], "http://"u8))
            schemeEnd = 7;
        else if (target.Length > 8 && Ascii.EqualsIgnoreCase(target[..8], "https://"u8))
            schemeEnd = 8;
        else
            return false;
        var rest = target[schemeEnd..];
        int authorityEnd = rest.IndexOfAny((byte)'/', (byte)'?');
        if (authorityEnd < 0)
            authorityEnd = rest.Length;
        if (authorityEnd == 0)
            return false;
        authority = Encoding.Latin1.GetString(rest[..authorityEnd]);
        if (authorityEnd == rest.Length)
        {
            path = "/"u8;
            return true;
        }
        if (rest[authorityEnd] != (byte)'/')
            return false; // "http://host?x=1" without a path is not accepted
        path = rest[authorityEnd..];
        return true;
    }

    private bool WantsKeepAlive()
    {
        var headers = _context.Headers;
        bool? explicitChoice = null;
        for (int i = 0; i < headers.Count; i++)
        {
            var h = headers[i];
            if (!string.Equals(h.Name, "Connection", StringComparison.OrdinalIgnoreCase))
                continue;
            if (HeaderTokens.Contains(h.Value, "close"))
                return false;
            if (HeaderTokens.Contains(h.Value, "keep-alive"))
                explicitChoice = true;
        }
        return explicitChoice ?? (_context.Version == HttpProtocolVersion.Http11);
    }

    private async ValueTask SendErrorAsync(int statusCode)
    {
        _response.Reset(HttpProtocolVersion.Http11, isHead: false, closeAfter: true);
        _response.StatusCode = statusCode;
        if (statusCode == 405)
            _response.Headers.Set("Allow", _server.AllowHeaderValue);
        string text = statusCode.ToString(System.Globalization.CultureInfo.InvariantCulture) + " " + HttpReasonPhrases.Get(statusCode);
        await _response.SendTextAsync(text, cancellationToken: AbortToken).ConfigureAwait(false);
    }

    private void ReleaseRequestResources()
    {
        var body = _bodyBuffer;
        _bodyBuffer = null;
        _bodyMemory = default;
        _bodyRead = false;
        if (body is not null)
            ArrayPool<byte>.Shared.Return(body);
        _bodyRemaining = 0;
        _expectContinue = false;
        _continueSent = false;
    }

    // ------------------------------------------------------------------ body

    private async ValueTask DrainBodyAsync()
    {
        int buffered = _end - _start;
        int skip = (int)Math.Min(buffered, _bodyRemaining);
        _start += skip;
        _bodyRemaining -= skip;
        if (_start == _end)
        {
            _start = 0;
            _end = 0;
        }
        while (_bodyRemaining > 0)
        {
            // The header buffer is free once the buffered bytes are consumed, so reuse it as scratch space.
            if (_start != _end)
                throw new InvalidOperationException("Unexpected buffered data while draining.");
            int want = (int)Math.Min(_buffer.Length, _bodyRemaining);
            int n = await ReadWithTimeoutAsync(_stream, _buffer.AsMemory(0, want), _limits.RequestBodyReadTimeout, IoCts()).ConfigureAwait(false);
            if (n == 0)
                throw new IOException("The client closed the connection before the request body was complete.");
            _bodyRemaining -= n;
        }
    }

    public async ValueTask<int> ReadBodyAsync(Memory<byte> destination, CancellationToken cancellationToken)
    {
        if (_bodyRemaining <= 0 || destination.IsEmpty)
            return 0;
        if (_expectContinue && !_continueSent)
        {
            _continueSent = true;
            await WriteAsync(ContinueResponse, cancellationToken).ConfigureAwait(false);
        }
        int toRead = (int)Math.Min(destination.Length, _bodyRemaining);
        int buffered = _end - _start;
        int n;
        if (buffered > 0)
        {
            n = Math.Min(buffered, toRead);
            _buffer.AsSpan(_start, n).CopyTo(destination.Span);
            _start += n;
        }
        else
        {
            var cts = IoCts();
            using var registration = RegisterExternal(cancellationToken, cts);
            try
            {
                n = await ReadWithTimeoutAsync(_stream, destination[..toRead], _limits.RequestBodyReadTimeout, cts).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                _bodyRemaining = -1; // how much of the body is still on the wire is unknown: the connection cannot be reused
                throw;
            }
            if (n == 0)
            {
                _bodyRemaining = -1;
                throw new IOException("The client closed the connection before the request body was complete.");
            }
        }
        _bodyRemaining -= n;
        return n;
    }

    public int ReadBody(Span<byte> destination)
    {
        if (_bodyRemaining <= 0 || destination.IsEmpty)
            return 0;
        if (_expectContinue && !_continueSent)
        {
            _continueSent = true;
            _stream.Write(ContinueResponse);
        }
        int toRead = (int)Math.Min(destination.Length, _bodyRemaining);
        int buffered = _end - _start;
        int n;
        if (buffered > 0)
        {
            n = Math.Min(buffered, toRead);
            _buffer.AsSpan(_start, n).CopyTo(destination);
            _start += n;
        }
        else
        {
            n = _stream.Read(destination[..toRead]);
            if (n == 0)
            {
                _bodyRemaining = -1;
                throw new IOException("The client closed the connection before the request body was complete.");
            }
        }
        _bodyRemaining -= n;
        return n;
    }

    public async ValueTask<ReadOnlyMemory<byte>> ReadWholeBodyAsync(CancellationToken cancellationToken)
    {
        if (_bodyRead)
            return _bodyMemory;
        _bodyRead = true;
        if (_bodyRemaining <= 0)
            return default;
        int length = (int)_bodyRemaining;
        var buffer = ArrayPool<byte>.Shared.Rent(length);
        _bodyBuffer = buffer;
        int filled = 0;
        while (filled < length)
            filled += await ReadBodyAsync(buffer.AsMemory(filled, length - filled), cancellationToken).ConfigureAwait(false);
        _bodyMemory = buffer.AsMemory(0, length);
        return _bodyMemory;
    }

    // ------------------------------------------------------------------ raw I/O with timeouts

    public async ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken)
    {
        var cts = IoCts();
        using var registration = RegisterExternal(cancellationToken, cts);
        cts.CancelAfter(_limits.ResponseWriteTimeout);
        try
        {
            await _stream.WriteAsync(data, cts.Token).ConfigureAwait(false);
        }
        finally
        {
            cts.CancelAfter(Timeout.InfiniteTimeSpan);
        }
    }

    private CancellationTokenRegistration RegisterExternal(CancellationToken cancellationToken, CancellationTokenSource target)
        => cancellationToken.CanBeCanceled && cancellationToken != AbortToken
            ? cancellationToken.UnsafeRegister(static s => ((CancellationTokenSource)s!).Cancel(), target)
            : default;

    /// <summary>
    /// The per-operation timeout source. A source that has fired stays cancelled, so after a timeout (or its timer
    /// racing the completion of the operation it guarded) it is replaced; this only allocates on the error path.
    /// </summary>
    private CancellationTokenSource IoCts()
    {
        var cts = _ioCts;
        if (cts.IsCancellationRequested && !_abortCts.IsCancellationRequested)
        {
            cts.Dispose();
            _ioCts = cts = CancellationTokenSource.CreateLinkedTokenSource(_abortCts.Token);
        }
        return cts;
    }

    private static async ValueTask<int> ReadWithTimeoutAsync(Stream stream, Memory<byte> destination, TimeSpan timeout, CancellationTokenSource cts)
    {
        cts.CancelAfter(timeout);
        try
        {
            return await stream.ReadAsync(destination, cts.Token).ConfigureAwait(false);
        }
        finally
        {
            cts.CancelAfter(Timeout.InfiniteTimeSpan);
        }
    }
}
