using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Text;

namespace Tedd.Quicly.Http.Tests;

/// <summary>Starts an <see cref="HttpServer"/> on an ephemeral loopback port.</summary>
internal sealed class TestHost : IAsyncDisposable
{
    public HttpServer Server { get; }

    public HttpServerOptions Options { get; }

    public IPEndPoint EndPoint => Server.BoundEndpoints[0];

    public bool IsSecure { get; }

    public Uri BaseUri => new((IsSecure ? "https" : "http") + "://127.0.0.1:" + EndPoint.Port + "/");

    private TestHost(HttpServer server, HttpServerOptions options, bool secure)
    {
        Server = server;
        Options = options;
        IsSecure = secure;
    }

    /// <summary>
    /// Starts a server with test-friendly defaults: every common method allowed and a 1 MiB body budget. The ADR 0009
    /// defaults (GET/HEAD only, no bodies) are covered by dedicated tests that build <see cref="HttpServerOptions"/> directly.
    /// </summary>
    public static TestHost Start(Action<HttpServerOptions>? configure = null, HttpTlsOptions? tls = null)
    {
        var options = new HttpServerOptions();
        options.Listen(IPAddress.Loopback, 0, tls);
        options.AllowMethods("POST", "PUT", "DELETE", "PATCH", "OPTIONS");
        options.Limits.MaxRequestBodyBytes = 1024 * 1024;
        configure?.Invoke(options);
        var server = new HttpServer(options);
        server.Start();
        return new TestHost(server, options, tls is not null);
    }

    public HttpClient CreateClient(Action<SocketsHttpHandler>? configure = null, Version? version = null)
    {
        var handler = new SocketsHttpHandler
        {
            PooledConnectionLifetime = TimeSpan.FromMinutes(1),
            SslOptions = new SslClientAuthenticationOptions
            {
                RemoteCertificateValidationCallback = static (_, _, _, _) => true,
            },
        };
        configure?.Invoke(handler);
        var client = new HttpClient(handler) { BaseAddress = BaseUri, Timeout = TimeSpan.FromSeconds(30) };
        client.DefaultRequestVersion = version ?? HttpVersion.Version11;
        client.DefaultVersionPolicy = HttpVersionPolicy.RequestVersionExact;
        return client;
    }

    public ValueTask DisposeAsync() => Server.DisposeAsync();
}

/// <summary>A raw TCP client for sending arbitrary bytes and reading HTTP/1.x responses.</summary>
internal sealed class RawClient : IDisposable
{
    private readonly Socket _socket;
    private readonly NetworkStream _stream;
    private readonly MemoryStream _buffered = new();

    private RawClient(Socket socket)
    {
        _socket = socket;
        _stream = new NetworkStream(socket, ownsSocket: true);
    }

    public static async Task<RawClient> ConnectAsync(IPEndPoint endPoint)
    {
        var socket = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        await socket.ConnectAsync(endPoint);
        return new RawClient(socket);
    }

    public Task SendAsync(string ascii) => SendAsync(Encoding.Latin1.GetBytes(ascii));

    public async Task SendAsync(byte[] bytes)
    {
        await _stream.WriteAsync(bytes);
        await _stream.FlushAsync();
    }

    /// <summary>Reads one response (headers, then a Content-Length body if present) and returns it parsed.</summary>
    public async Task<RawResponse> ReadResponseAsync(TimeSpan? timeout = null, bool noBody = false)
    {
        using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(15));
        var buffer = new byte[16 * 1024];
        while (true)
        {
            var data = _buffered.GetBuffer().AsMemory(0, (int)_buffered.Length);
            int end = data.Span.IndexOf("\r\n\r\n"u8);
            if (end >= 0)
            {
                var head = Encoding.Latin1.GetString(data.Span[..end]);
                int consumed = end + 4;
                var response = RawResponse.ParseHead(head);
                if (noBody || response.StatusCode < 200 || response.StatusCode is 204 or 304)
                {
                    // HEAD responses and 1xx/204/304 carry no body whatever the framing headers say
                }
                else if (response.ContentLength > 0)
                {
                    while (_buffered.Length - consumed < response.ContentLength)
                    {
                        int n = await _stream.ReadAsync(buffer, cts.Token);
                        if (n == 0)
                            throw new IOException("Connection closed before the body completed.");
                        _buffered.Write(buffer, 0, n);
                    }
                    data = _buffered.GetBuffer().AsMemory(0, (int)_buffered.Length);
                    response.Body = Encoding.UTF8.GetString(data.Span.Slice(consumed, (int)response.ContentLength));
                    consumed += (int)response.ContentLength;
                }
                else if (response.IsChunked)
                {
                    // read until the terminating chunk
                    while (true)
                    {
                        data = _buffered.GetBuffer().AsMemory(0, (int)_buffered.Length);
                        int term = data.Span[consumed..].IndexOf("0\r\n\r\n"u8);
                        if (term >= 0)
                        {
                            response.Body = DecodeChunked(data.Span.Slice(consumed, term + 5));
                            consumed += term + 5;
                            break;
                        }
                        int n = await _stream.ReadAsync(buffer, cts.Token);
                        if (n == 0)
                            throw new IOException("Connection closed before the chunked body completed.");
                        _buffered.Write(buffer, 0, n);
                    }
                }
                // drop consumed bytes
                var rest = _buffered.GetBuffer().AsSpan(consumed, (int)_buffered.Length - consumed).ToArray();
                _buffered.SetLength(0);
                _buffered.Write(rest);
                return response;
            }
            int read = await _stream.ReadAsync(buffer, cts.Token);
            if (read == 0)
                throw new IOException("Connection closed before the response head completed.");
            _buffered.Write(buffer, 0, read);
        }
    }

    private static string DecodeChunked(ReadOnlySpan<byte> chunked)
    {
        var result = new MemoryStream();
        while (true)
        {
            int lf = chunked.IndexOf((byte)'\n');
            int size = Convert.ToInt32(Encoding.ASCII.GetString(chunked[..(lf - 1)]), 16);
            chunked = chunked[(lf + 1)..];
            if (size == 0)
                break;
            result.Write(chunked[..size]);
            chunked = chunked[(size + 2)..];
        }
        return Encoding.UTF8.GetString(result.ToArray());
    }

    /// <summary>Reads everything until the server closes the connection.</summary>
    public async Task<string> ReadToEndAsync(TimeSpan? timeout = null)
    {
        using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(15));
        var buffer = new byte[16 * 1024];
        while (true)
        {
            int n;
            try
            {
                n = await _stream.ReadAsync(buffer, cts.Token);
            }
            catch (IOException)
            {
                break; // reset by peer counts as closed
            }
            if (n == 0)
                break;
            _buffered.Write(buffer, 0, n);
        }
        var text = Encoding.Latin1.GetString(_buffered.GetBuffer(), 0, (int)_buffered.Length);
        _buffered.SetLength(0);
        return text;
    }

    /// <summary>Waits for the server to close the connection; returns false when it is still open after <paramref name="timeout"/>.</summary>
    public async Task<bool> WaitForCloseAsync(TimeSpan timeout)
    {
        using var cts = new CancellationTokenSource(timeout);
        var buffer = new byte[1024];
        try
        {
            while (true)
            {
                int n = await _stream.ReadAsync(buffer, cts.Token);
                if (n == 0)
                    return true;
                _buffered.Write(buffer, 0, n);
            }
        }
        catch (OperationCanceledException)
        {
            return false;
        }
        catch (IOException)
        {
            return true;
        }
    }

    public Stream Stream => _stream;

    public Socket Socket => _socket;

    public void Dispose()
    {
        _stream.Dispose();
        _buffered.Dispose();
    }
}

internal sealed class RawResponse
{
    public int StatusCode { get; private set; }

    public string ReasonPhrase { get; private set; } = string.Empty;

    public string Version { get; private set; } = string.Empty;

    public List<KeyValuePair<string, string>> Headers { get; } = [];

    public string Body { get; set; } = string.Empty;

    public long ContentLength { get; private set; }

    public bool IsChunked { get; private set; }

    public string? this[string name]
    {
        get
        {
            foreach (var h in Headers)
            {
                if (string.Equals(h.Key, name, StringComparison.OrdinalIgnoreCase))
                    return h.Value;
            }
            return null;
        }
    }

    public static RawResponse ParseHead(string head)
    {
        var lines = head.Split("\r\n");
        var status = lines[0].Split(' ', 3);
        var r = new RawResponse
        {
            Version = status[0],
            StatusCode = int.Parse(status[1], System.Globalization.CultureInfo.InvariantCulture),
            ReasonPhrase = status.Length > 2 ? status[2] : string.Empty,
        };
        for (int i = 1; i < lines.Length; i++)
        {
            int colon = lines[i].IndexOf(':', StringComparison.Ordinal);
            r.Headers.Add(new KeyValuePair<string, string>(lines[i][..colon], lines[i][(colon + 1)..].Trim()));
        }
        if (r["Content-Length"] is { } cl)
            r.ContentLength = long.Parse(cl, System.Globalization.CultureInfo.InvariantCulture);
        r.IsChunked = string.Equals(r["Transfer-Encoding"], "chunked", StringComparison.OrdinalIgnoreCase);
        return r;
    }
}
