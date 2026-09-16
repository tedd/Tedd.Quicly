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
    private readonly HttpRequestContext _context;
    private readonly HttpResponse _response;
    private readonly HttpRequestBodyStream _bodyStream;
    private readonly int _maxHeaderBlock;

    private CancellationTokenSource _ioCts;
    private CancellationTokenSource _idleCts;
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
        // Request line + CRLF + header section, plus the empty lines the parser tolerates before the request line.
        _maxHeaderBlock = (HttpParser.MaxLeadingEmptyLines * 2) + _limits.MaxRequestLineBytes + 2 + _limits.MaxHeadersBytes;
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

    /// <summary>
    /// Starts processing on the thread pool. <see cref="Completion"/> is set before this returns, and no connection
    /// code runs on the caller (the accept loop), even when the first read would complete synchronously.
    /// </summary>
    public void Start() => Completion = RunAsync();

    /// <summary>Closes the socket immediately, interrupting any pending I/O. Safe to call at any time, also after the connection closed.</summary>
    public void Abort()
    {
        try
        {
            _abortCts.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // The connection already closed and released its resources.
        }
        _socket.Dispose();
    }

    internal void MarkProtocolViolation() => _protocolViolation = true;

    private async Task RunAsync()
    {
        // Leave the accept loop at once: without this, a request already buffered at accept time would be parsed,
        // handled (TLS handshake included) and answered inline, and no other connection would be accepted meanwhile.
        await Task.Yield();
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
            _server.ReportError(ex);
        }
        finally
        {
            Close();
        }
    }

    /// <summary>Disposes <paramref name="disposable"/>, swallowing the I/O errors a stream may raise while a broken connection is torn down.</summary>
    internal static void DisposeQuietly(IDisposable disposable)
    {
        try
        {
            disposable.Dispose();
        }
        catch (Exception ex) when (ex is IOException or SocketException or ObjectDisposedException)
        {
            // Nothing useful can be done: the peer is gone or the stream already failed.
        }
    }

    /// <summary>Releases everything the connection owns. Runs exactly once, from <see cref="RunAsync"/>'s <c>finally</c>.</summary>
    private void Close()
    {
        _abortCts.Cancel(); // only Close disposes this source, so it is still alive here
        DisposeQuietly(_stream);
        _socket.Dispose();
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

    /// <summary>Initial size of the ClientHello peek buffer; it doubles (up to <see cref="ClientHelloParser.MaxPeekBytes"/>) only as bytes arrive.</summary>
    internal const int InitialPeekBytes = 4 * 1024;

    private async ValueTask<Stream?> CreateStreamAsync()
    {
        var network = new NetworkStream(_socket, ownsSocket: true);
        _stream = network; // disposed by Close() whatever happens below
        _socket.NoDelay = true;
        // Socket timeouts only govern synchronous I/O: they bound the synchronous body-read path
        // (HttpRequestBodyStream.Read and its 100-continue write) as cancellation bounds the async paths.
        _socket.ReceiveTimeout = ToSocketTimeout(_limits.RequestBodyReadTimeout);
        _socket.SendTimeout = ToSocketTimeout(_limits.ResponseWriteTimeout);
        var tls = _endpoint.Tls;
        if (tls is null)
            return network;

        // Peek the ClientHello so that ALPN acme-tls/1 can pick the challenge certificate (SslStream's own
        // selection callback only exposes the SNI host, not the offered protocols). The whole peek plus the
        // handshake must finish within TlsHandshakeTimeout; a request's HeaderReadTimeout starts after it, so a
        // slow handshake (the server's first credential setup, a chain build) never eats a request's budget. The
        // peek buffer starts small and grows with what the client actually sent, so a connection trickling a
        // hello holds memory proportional to its own bytes.
        long deadline = Deadline(_limits.TlsHandshakeTimeout);
        var rented = ArrayPool<byte>.Shared.Rent(InitialPeekBytes);
        bool handedOff = false;
        int filled = 0;
        bool parsed;
        ClientHelloInfo hello;
        // Reassembles incrementally: each read only walks the records it completed, so a hello trickled as
        // thousands of tiny records costs linear, not quadratic, CPU.
        var reader = default(ClientHelloReader);
        try
        {
            while (true)
            {
                var status = reader.Read(rented.AsSpan(0, filled), out hello, out parsed);
                if (status == ClientHelloAssembleStatus.Complete)
                    break;
                if (status != ClientHelloAssembleStatus.NeedMore || filled == ClientHelloParser.MaxPeekBytes)
                {
                    // Not TLS, or a ClientHello we refuse to buffer: drop the connection without a handshake attempt.
                    _server.OnHandshakeFailure();
                    return null;
                }
                int capacity = Math.Min(rented.Length, ClientHelloParser.MaxPeekBytes);
                if (filled == capacity)
                {
                    var larger = ArrayPool<byte>.Shared.Rent(Math.Min(ClientHelloParser.MaxPeekBytes, rented.Length * 2));
                    rented.AsSpan(0, filled).CopyTo(larger);
                    ArrayPool<byte>.Shared.Return(rented);
                    rented = larger;
                    capacity = Math.Min(rented.Length, ClientHelloParser.MaxPeekBytes);
                }
                var remaining = Remaining(deadline);
                if (remaining <= TimeSpan.Zero && remaining != Timeout.InfiniteTimeSpan)
                {
                    _server.OnHandshakeTimeout();
                    return null;
                }
                int n;
                try
                {
                    n = await ReadWithTimeoutAsync(network, rented.AsMemory(filled, capacity - filled), remaining, IoCts()).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (!AbortToken.IsCancellationRequested)
                {
                    _server.OnHandshakeTimeout(); // the client stopped sending its hello (everyday noise on a public port)
                    throw;
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
            reader.Dispose();
            if (!handedOff)
                ArrayPool<byte>.Shared.Return(rented);
        }

        // From here on the rented buffer belongs to the PrefixedStream, which returns it once replayed.
        var ssl = new SslStream(new PrefixedStream(network, rented, filled), leaveInnerStreamOpen: false);
        _stream = ssl;
        // A validation client that offers only acme-tls/1 for a published name gets the challenge certificate; every other
        // client gets what its SNI host resolves to, through the options callback (SslStream parses the SNI itself). Both
        // serve a credential built once per certificate, so no handshake pays for a chain build twice.
        SslServerAuthenticationOptions? acmeOptions = null;
        if (parsed && hello.Offers("acme-tls/1") && tls.Alpn01Responder is { } responder && responder.TryGetCertificate(hello.ServerName, out var challengeCertificate))
            acmeOptions = tls.CreateAcmeOptions(challengeCertificate);

        var cts = IoCts();
        cts.CancelAfter(Remaining(deadline));
        try
        {
            if (acmeOptions is not null)
                await ssl.AuthenticateAsServerAsync(acmeOptions, cts.Token).ConfigureAwait(false);
            else
                await ssl.AuthenticateAsServerAsync(tls.OptionsCallback, state: null, cts.Token).ConfigureAwait(false);
        }
        catch (Exception ex) when (cts.IsCancellationRequested && !AbortToken.IsCancellationRequested)
        {
            // The handshake budget ran out: the client stalled after a complete ClientHello, or the server's own credential
            // setup was too slow. Reported as well as counted, because it is not the scanner noise the peek phase sees and it
            // can point at the server (the exception may be the cancellation, or an AuthenticationException wrapping it).
            _server.OnHandshakeTimeout();
            _server.ReportError(new TimeoutException("The TLS handshake with " + (RemoteEndPoint?.ToString() ?? "a client")
                + " did not complete within TlsHandshakeTimeout (" + _limits.TlsHandshakeTimeout + ").", ex));
            throw;
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

        if (acmeOptions is not null)
        {
            _server.OnAcmeTlsAlpnHandshake();
            return null;
        }
        IsSecure = true;
        TlsServerName = string.IsNullOrEmpty(ssl.TargetHostName) ? (parsed ? hello.ServerName : null) : ssl.TargetHostName;
        return ssl;
    }

    // ------------------------------------------------------------------ request loop

    private async Task ProcessRequestsAsync()
    {
        var handlers = _options.Handlers;
        int requests = 0;
        while (true)
        {
            int error = await ReadRequestHeadAsync(requests == 0).ConfigureAwait(false);
            if (error < 0)
                return; // clean end of stream between requests
            if (error != 0)
            {
                if (error == 408)
                    _server.OnRequestTimeout();
                else
                    _server.OnProtocolError();
                await SendErrorAsync(error).ConfigureAwait(false);
                return;
            }

            var buffered = _buffer.AsSpan(_start, _end - _start);
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
                _server.ReportError(ex);
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

    /// <summary>
    /// Reads until the request line and header section are complete and parses them into <see cref="_parsed"/>.
    /// Returns 0 when a request is ready, -1 when the client closed the connection, or the status code to answer
    /// with (400, 408, 414, 431, 505).
    /// </summary>
    private async ValueTask<int> ReadRequestHeadAsync(bool firstRequest)
    {
        // The header deadline runs from accept for the first request and from the first byte of every later one;
        // with pipelining those bytes may already be buffered. 0 means "idle, waiting for the next request".
        long deadline = firstRequest || _end > _start ? Deadline(_limits.HeaderReadTimeout) : 0;
        int scanFrom = _start;
        while (true)
        {
            int available = _end - _start;
            if (available > 0)
            {
                // The parser only runs once a header terminator has arrived (or the size limit is reached), so a
                // request trickled in small pieces is scanned once, not re-parsed per packet.
                int idx = _buffer.AsSpan(scanFrom, _end - scanFrom).IndexOf(HttpParser.HeaderTerminator);
                bool full = available >= _maxHeaderBlock;
                if (idx >= 0 || full)
                {
                    switch (HttpParser.TryParse(_buffer.AsSpan(_start, available), _limits.MaxRequestLineBytes, _limits.MaxHeadersBytes, ref _parsed))
                    {
                        case HttpParseStatus.Ok:
                            return 0;
                        case HttpParseStatus.Invalid:
                            return 400;
                        case HttpParseStatus.RequestLineTooLong:
                            return 414;
                        case HttpParseStatus.VersionNotSupported:
                            return 505;
                        case HttpParseStatus.HeadersTooLarge:
                        // The whole block (empty lines + request line + headers) is buffered, so the parser reports a
                        // limit itself; should it still want more, refuse rather than read past the budget.
                        case HttpParseStatus.NeedMore when full:
                            return 431;
                    }
                    // NeedMore although a terminator is buffered: it ended tolerated empty lines before the request
                    // line, not the header section. Keep scanning after it.
                    scanFrom += idx + 1;
                    continue;
                }
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
                if (deadline == 0)
                {
                    // Waiting for the next request on a persistent connection: idle timeout, closable on shutdown.
                    n = await ReadWithTimeoutAsync(_stream, _buffer.AsMemory(_end), _limits.KeepAliveTimeout, IdleCts()).ConfigureAwait(false);
                    deadline = Deadline(_limits.HeaderReadTimeout);
                }
                else
                {
                    var remaining = Remaining(deadline);
                    if (remaining <= TimeSpan.Zero && remaining != Timeout.InfiniteTimeSpan)
                        return 408;
                    n = await ReadWithTimeoutAsync(_stream, _buffer.AsMemory(_end), remaining, available == 0 ? IdleCts() : IoCts()).ConfigureAwait(false);
                }
            }
            catch (OperationCanceledException) when (available > 0 && !AbortToken.IsCancellationRequested)
            {
                // A partial request that stalled: answer 408 rather than dropping silently.
                return 408;
            }
            if (n == 0)
                return -1;
            _end += n;
        }
    }

    private static long Deadline(TimeSpan timeout)
        => timeout == Timeout.InfiniteTimeSpan ? long.MaxValue : Environment.TickCount64 + (long)timeout.TotalMilliseconds;

    private static TimeSpan Remaining(long deadline)
        => deadline == long.MaxValue ? Timeout.InfiniteTimeSpan : TimeSpan.FromMilliseconds(Math.Max(0, deadline - Environment.TickCount64));

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
        if (authority is not null)
        {
            // RFC 9112 §3.2.2: with an absolute-form target the received Host is ignored and replaced by the
            // target's authority.
            if (hostCount != 0)
                headers.Remove("Host");
            headers.AddUnchecked("Host", authority);
        }
        else if (hostCount == 0 && ctx.Version == HttpProtocolVersion.Http11)
        {
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
        if (authorityEnd == 0 || rest[..authorityEnd].Contains((byte)'@'))
            return false; // empty authority, or userinfo (RFC 9110 §4.2.4: a recipient should treat it as an error)
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
        int n;
        try
        {
            if (_expectContinue && !_continueSent)
            {
                _continueSent = true;
                _stream.Write(ContinueResponse);
            }
            int toRead = (int)Math.Min(destination.Length, _bodyRemaining);
            int buffered = _end - _start;
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
                    throw new IOException("The client closed the connection before the request body was complete.");
            }
        }
        catch (IOException)
        {
            // Early close, reset, or the socket's ReceiveTimeout: how much of the body is still on the wire is
            // unknown, so the connection cannot be reused.
            _bodyRemaining = -1;
            throw;
        }
        _bodyRemaining -= n;
        return n;
    }

    /// <summary>Converts a limit to a <see cref="Socket.ReceiveTimeout"/>/<see cref="Socket.SendTimeout"/> value (0 = infinite).</summary>
    internal static int ToSocketTimeout(TimeSpan timeout)
        => timeout == Timeout.InfiniteTimeSpan ? 0 : (int)Math.Min(int.MaxValue, Math.Ceiling(timeout.TotalMilliseconds));

    /// <summary>One-shot <see cref="ClientHelloReader.Read"/> over <paramref name="raw"/> (tests).</summary>
    internal static ClientHelloAssembleStatus TryReadClientHello(ReadOnlySpan<byte> raw, out ClientHelloInfo hello, out bool parsed)
    {
        var reader = default(ClientHelloReader);
        try
        {
            return reader.Read(raw, out hello, out parsed);
        }
        finally
        {
            reader.Dispose();
        }
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
    private CancellationTokenSource IoCts() => Renew(ref _ioCts, idle: false);

    /// <summary>
    /// The idle/first-byte timeout source. Like <see cref="IoCts"/> it is replaced once its own timer has fired (a
    /// timer racing the completion of the read it guarded would otherwise fail the next idle read at once), but it
    /// stays cancelled when the server is closing idle connections or the connection is aborted.
    /// </summary>
    private CancellationTokenSource IdleCts() => Renew(ref _idleCts, idle: true);

    /// <summary>Returns <paramref name="source"/>, replacing it first when only its own timer (not abort or shutdown) cancelled it.</summary>
    private CancellationTokenSource Renew(ref CancellationTokenSource source, bool idle)
    {
        var cts = source;
        if (cts.IsCancellationRequested && !_abortCts.IsCancellationRequested && !(idle && _server.IdleToken.IsCancellationRequested))
        {
            cts.Dispose();
            source = cts = idle ? CancellationTokenSource.CreateLinkedTokenSource(_server.IdleToken, _abortCts.Token) : CancellationTokenSource.CreateLinkedTokenSource(_abortCts.Token);
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
