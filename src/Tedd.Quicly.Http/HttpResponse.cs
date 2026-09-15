using System.Buffers;
using System.Text;
using Tedd.Quicly.Http.Internal;
using Tedd.Quicly.Http.Parsing;

namespace Tedd.Quicly.Http;

/// <summary>
/// Response writer. Either send everything at once (<see cref="SendAsync"/>, <see cref="SendTextAsync"/>,
/// <see cref="SendFileAsync"/>, <see cref="SendStreamAsync"/>) or stream it (<see cref="StartAsync"/>,
/// <see cref="WriteAsync"/>, <see cref="CompleteAsync"/>). Bodies of unknown length use chunked transfer
/// encoding on HTTP/1.1 and close-delimiting on HTTP/1.0. <c>HEAD</c> requests get headers only.
/// </summary>
public sealed class HttpResponse
{
    private const int FileBufferSize = 64 * 1024;
    private const int InlineBodyLimit = 16 * 1024;

    private readonly HttpConnection _connection;
    private ByteBufferWriter _writer;
    private HttpProtocolVersion _version = HttpProtocolVersion.Http11;
    private bool _isHead;
    private bool _bodyAllowed;
    private bool _chunked;
    private long _declaredLength = -1;
    private long _written;

    internal HttpResponse(HttpConnection connection)
    {
        _connection = connection;
        _writer = new ByteBufferWriter(4096);
        Headers = new HttpHeaderCollection(8);
    }

    /// <summary>Status code; default 200.</summary>
    public int StatusCode { get; set; } = 200;

    /// <summary>Reason phrase override; <see langword="null"/> uses <see cref="HttpReasonPhrases.Get"/>.</summary>
    public string? ReasonPhrase { get; set; }

    /// <summary>Response headers. <c>Date</c>, <c>Server</c>, <c>Content-Length</c>, <c>Transfer-Encoding</c> and <c>Connection</c> are managed by the server and ignored here.</summary>
    public HttpHeaderCollection Headers { get; }

    /// <summary>Shortcut for the <c>Content-Type</c> header.</summary>
    public string? ContentType
    {
        get => Headers["Content-Type"];
        set => Headers["Content-Type"] = value;
    }

    /// <summary>Whether the status line and headers have been written.</summary>
    public bool HasStarted { get; private set; }

    /// <summary>Whether the response is complete.</summary>
    public bool IsCompleted { get; private set; }

    /// <summary>Request that the connection closes after this response (sets <c>Connection: close</c>).</summary>
    public bool CloseConnection { get; set; }

    /// <summary>Number of body bytes written so far (0 for HEAD).</summary>
    public long BytesWritten => _written;

    internal void Reset(HttpProtocolVersion version, bool isHead, bool closeAfter)
    {
        _version = version;
        _isHead = isHead;
        CloseConnection = closeAfter;
        StatusCode = 200;
        ReasonPhrase = null;
        Headers.Clear();
        HasStarted = false;
        IsCompleted = false;
        _bodyAllowed = false;
        _chunked = false;
        _declaredLength = -1;
        _written = 0;
        _writer.Clear();
    }

    /// <summary>
    /// Writes the status line and headers. Pass the body length when known; otherwise HTTP/1.1 responses are chunked
    /// and HTTP/1.0 responses are terminated by closing the connection.
    /// </summary>
    public ValueTask StartAsync(long? contentLength = null, CancellationToken cancellationToken = default)
    {
        if (HasStarted)
            throw new InvalidOperationException("The response has already started.");
        if (contentLength is { } declared)
            ArgumentOutOfRangeException.ThrowIfNegative(declared);
        BuildHeaders(contentLength);
        return _connection.WriteAsync(_writer.WrittenMemory, cancellationToken);
    }

    /// <summary>Writes body bytes, starting the response (chunked / close-delimited) if needed.</summary>
    public async ValueTask WriteAsync(ReadOnlyMemory<byte> data, CancellationToken cancellationToken = default)
    {
        if (IsCompleted)
            throw new InvalidOperationException("The response is complete.");
        if (!HasStarted)
            await StartAsync(null, cancellationToken).ConfigureAwait(false);
        if (!_bodyAllowed || data.IsEmpty)
            return;
        if (_declaredLength >= 0 && _written + data.Length > _declaredLength)
            throw new InvalidOperationException("Body exceeds the declared Content-Length.");

        _writer.Clear();
        if (_chunked)
        {
            _writer.AppendHex(data.Length);
            _writer.AppendCrLf();
            if (data.Length <= InlineBodyLimit)
            {
                _writer.Append(data.Span);
                _writer.AppendCrLf();
                await _connection.WriteAsync(_writer.WrittenMemory, cancellationToken).ConfigureAwait(false);
            }
            else
            {
                await _connection.WriteAsync(_writer.WrittenMemory, cancellationToken).ConfigureAwait(false);
                await _connection.WriteAsync(data, cancellationToken).ConfigureAwait(false);
                await _connection.WriteAsync(CrLf, cancellationToken).ConfigureAwait(false);
            }
        }
        else
        {
            await _connection.WriteAsync(data, cancellationToken).ConfigureAwait(false);
        }
        _written += data.Length;
    }

    /// <summary>Finishes the response. Starts it with an empty body when nothing was written yet.</summary>
    public async ValueTask CompleteAsync(CancellationToken cancellationToken = default)
    {
        if (IsCompleted)
            return;
        if (!HasStarted)
            await StartAsync(0, cancellationToken).ConfigureAwait(false);
        IsCompleted = true;
        if (!_bodyAllowed)
            return;
        if (_chunked)
        {
            await _connection.WriteAsync(ChunkedTerminator, cancellationToken).ConfigureAwait(false);
        }
        else if (_declaredLength >= 0 && _written != _declaredLength)
        {
            // Truncated body: the framing is broken, so the connection must not be reused.
            CloseConnection = true;
            _connection.MarkProtocolViolation();
        }
    }

    /// <summary>Sends a complete response with <paramref name="body"/> and an exact <c>Content-Length</c>.</summary>
    public ValueTask SendAsync(ReadOnlyMemory<byte> body, CancellationToken cancellationToken = default)
    {
        if (HasStarted)
            throw new InvalidOperationException("The response has already started.");
        BuildHeaders(body.Length);
        IsCompleted = true;
        if (!_bodyAllowed || body.IsEmpty)
            return _connection.WriteAsync(_writer.WrittenMemory, cancellationToken);
        _written = body.Length;
        if (body.Length <= InlineBodyLimit)
        {
            _writer.Append(body.Span);
            return _connection.WriteAsync(_writer.WrittenMemory, cancellationToken);
        }
        return SendTwoPartAsync(body, cancellationToken);
    }

    private async ValueTask SendTwoPartAsync(ReadOnlyMemory<byte> body, CancellationToken cancellationToken)
    {
        await _connection.WriteAsync(_writer.WrittenMemory, cancellationToken).ConfigureAwait(false);
        await _connection.WriteAsync(body, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Sends <paramref name="text"/> as UTF-8 with the given content type.</summary>
    public ValueTask SendTextAsync(string text, string contentType = "text/plain; charset=utf-8", CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(text);
        if (HasStarted)
            throw new InvalidOperationException("The response has already started.");
        if (contentType is not null)
            Headers.Set("Content-Type", contentType);
        int byteCount = Encoding.UTF8.GetByteCount(text);
        BuildHeaders(byteCount);
        IsCompleted = true;
        if (_bodyAllowed && byteCount > 0)
        {
            _written = byteCount;
            var rented = ArrayPool<byte>.Shared.Rent(byteCount);
            try
            {
                int n = Encoding.UTF8.GetBytes(text, rented);
                _writer.Append(rented.AsSpan(0, n));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
        return _connection.WriteAsync(_writer.WrittenMemory, cancellationToken);
    }

    /// <summary>Sends an empty response with <paramref name="statusCode"/>.</summary>
    public ValueTask SendStatusAsync(int statusCode, CancellationToken cancellationToken = default)
    {
        StatusCode = statusCode;
        return SendAsync(ReadOnlyMemory<byte>.Empty, cancellationToken);
    }

    /// <summary>Streams a file with an exact <c>Content-Length</c>.</summary>
    public async ValueTask SendFileAsync(string path, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        if (HasStarted)
            throw new InvalidOperationException("The response has already started.");
        using var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete, 1, FileOptions.Asynchronous | FileOptions.SequentialScan);
        await SendStreamAsync(file, file.Length, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Streams <paramref name="body"/>. When <paramref name="contentLength"/> is <see langword="null"/> and the stream is
    /// seekable its remaining length is used; otherwise the response is chunked / close-delimited.
    /// </summary>
    public async ValueTask SendStreamAsync(Stream body, long? contentLength = null, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(body);
        if (HasStarted)
            throw new InvalidOperationException("The response has already started.");
        if (contentLength is null && body.CanSeek)
            contentLength = body.Length - body.Position;
        await StartAsync(contentLength, cancellationToken).ConfigureAwait(false);
        if (_bodyAllowed)
        {
            long remaining = contentLength ?? long.MaxValue;
            var buffer = ArrayPool<byte>.Shared.Rent(FileBufferSize);
            try
            {
                while (remaining > 0)
                {
                    int want = (int)Math.Min(buffer.Length, remaining);
                    int n = await body.ReadAsync(buffer.AsMemory(0, want), cancellationToken).ConfigureAwait(false);
                    if (n == 0)
                        break;
                    await WriteAsync(buffer.AsMemory(0, n), cancellationToken).ConfigureAwait(false);
                    remaining -= n;
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
        await CompleteAsync(cancellationToken).ConfigureAwait(false);
    }

    private static readonly byte[] CrLfBytes = "\r\n"u8.ToArray();

    private static readonly byte[] ChunkedTerminatorBytes = "0\r\n\r\n"u8.ToArray();

    private static ReadOnlyMemory<byte> CrLf => CrLfBytes;

    private static ReadOnlyMemory<byte> ChunkedTerminator => ChunkedTerminatorBytes;

    private void BuildHeaders(long? contentLength)
    {
        HasStarted = true;
        int status = StatusCode;
        if (status < 100 || status > 999)
            throw new InvalidOperationException("Invalid status code " + status.ToString(System.Globalization.CultureInfo.InvariantCulture) + ".");
        bool statusAllowsBody = status >= 200 && status != 204 && status != 304;
        _bodyAllowed = statusAllowsBody && !_isHead;
        if (_connection.IsStopping)
            CloseConnection = true;

        // Framing decision.
        bool emitLength;
        long length = 0;
        _chunked = false;
        _declaredLength = -1;
        if (contentLength is { } declared)
        {
            length = declared;
            _declaredLength = declared;
            emitLength = status >= 200 && status != 204;   // 1xx/204 never carry Content-Length; 304 and HEAD may
        }
        else if (!statusAllowsBody || _isHead)
        {
            emitLength = false;
        }
        else if (_version == HttpProtocolVersion.Http11)
        {
            _chunked = true;
            emitLength = false;
        }
        else
        {
            // HTTP/1.0 without a known length: close-delimited.
            CloseConnection = true;
            emitLength = false;
        }

        ref var w = ref _writer;
        w.Clear();
        w.Append("HTTP/1.1 "u8);
        w.AppendInt64(status);
        w.Append((byte)' ');
        w.AppendLatin1(ReasonPhrase ?? HttpReasonPhrases.Get(status));
        w.AppendCrLf();
        w.Append("Date: "u8);
        w.Append(HttpDate.GetCurrentBytes());
        w.AppendCrLf();
        w.Append(_connection.ServerHeaderLine);
        if (emitLength)
        {
            w.Append("Content-Length: "u8);
            w.AppendInt64(length);
            w.AppendCrLf();
        }
        else if (_chunked)
        {
            w.Append("Transfer-Encoding: chunked\r\n"u8);
        }
        if (CloseConnection)
            w.Append("Connection: close\r\n"u8);
        else if (_version == HttpProtocolVersion.Http10)
            w.Append("Connection: keep-alive\r\n"u8);

        var headers = Headers;
        for (int i = 0; i < headers.Count; i++)
        {
            var h = headers[i];
            if (IsManagedHeader(h.Name))
                continue;
            w.AppendLatin1(h.Name);
            w.Append(": "u8);
            w.AppendLatin1(h.Value);
            w.AppendCrLf();
        }
        w.AppendCrLf();
    }

    private static bool IsManagedHeader(string name)
        => name.Length is 4 or 6 or 10 or 14 or 17
           && (string.Equals(name, "Content-Length", StringComparison.OrdinalIgnoreCase)
               || string.Equals(name, "Transfer-Encoding", StringComparison.OrdinalIgnoreCase)
               || string.Equals(name, "Connection", StringComparison.OrdinalIgnoreCase)
               || string.Equals(name, "Date", StringComparison.OrdinalIgnoreCase)
               || string.Equals(name, "Server", StringComparison.OrdinalIgnoreCase));

    internal void DisposeBuffers() => _writer.Dispose();
}
