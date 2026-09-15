using System.Net;
using System.Net.Http.Headers;
using System.Text;
using Tedd.Quicly.Http.Handlers;

namespace Tedd.Quicly.Http.Tests;

public class HttpServerTests
{
    internal static async Task WaitUntilAsync(Func<bool> condition, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(10));
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("Condition not met in time.");
            await Task.Delay(10);
        }
    }

    [Fact]
    public async Task Get_routes_and_sets_standard_headers()
    {
        var routes = new RouteTable().MapGet("/hello", (ctx, ct) => ctx.Response.SendTextAsync("hi " + ctx.QueryString, cancellationToken: ct));
        await using var host = TestHost.Start(o => o.Use(routes));
        using var client = host.CreateClient();

        using var response = await client.GetAsync("/hello?name=x");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("hi name=x", await response.Content.ReadAsStringAsync());
        Assert.Equal("text/plain", response.Content.Headers.ContentType!.MediaType);
        Assert.Equal(9, response.Content.Headers.ContentLength);
        Assert.NotNull(response.Headers.Date);
        Assert.Equal("Tedd.Quicly", response.Headers.Server.ToString());
        Assert.Equal(1, host.Server.TotalConnectionsAccepted);
    }

    [Fact]
    public async Task Server_header_can_be_disabled_and_custom_headers_pass_through()
    {
        var routes = new RouteTable().MapGet("/", (ctx, ct) =>
        {
            ctx.Response.Headers.Add("X-Custom", "1");
            ctx.Response.Headers.Add("Server", "ignored");     // managed headers are dropped
            ctx.Response.Headers.Add("Content-Length", "999"); // managed headers are dropped
            ctx.Response.Headers.Add("Connection", "close");
            ctx.Response.Headers.Add("Date", "x");
            ctx.Response.Headers.Add("Transfer-Encoding", "gzip");
            return ctx.Response.SendTextAsync("ok", cancellationToken: ct);
        });
        await using var host = TestHost.Start(o =>
        {
            o.AddServerHeader = false;
            o.Use(routes);
        });
        using var client = host.CreateClient();
        using var response = await client.GetAsync("/");
        Assert.Empty(response.Headers.Server);
        Assert.Equal("1", response.Headers.GetValues("X-Custom").Single());
        Assert.Equal(2, response.Content.Headers.ContentLength);
        Assert.Null(response.Headers.TransferEncodingChunked);
        Assert.Equal("ok", await response.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task No_handler_yields_404()
    {
        await using var host = TestHost.Start();
        using var client = host.CreateClient();
        using var response = await client.GetAsync("/nothing");
        Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
        Assert.Equal("404 Not Found", await response.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task Post_body_can_be_read_whole_or_streamed()
    {
        var routes = new RouteTable()
            .MapPost("/whole", async (ctx, ct) =>
            {
                var body = await ctx.ReadBodyAsync(ct);
                var again = await ctx.ReadBodyAsync(ct);
                Assert.True(body.Span.SequenceEqual(again.Span));
                await ctx.Response.SendAsync(body, ct);
            })
            .MapPost("/stream", async (ctx, ct) =>
            {
                Assert.True(ctx.HasBody);
                Assert.True(ctx.Body.CanRead);
                Assert.False(ctx.Body.CanSeek);
                Assert.False(ctx.Body.CanWrite);
                using var ms = new MemoryStream();
                await ctx.Body.CopyToAsync(ms, ct);
                ctx.Body.Flush();
                ctx.Body.Dispose(); // no-op
                await ctx.Response.SendAsync(ms.ToArray(), ct);
            })
            .MapPost("/legacy", async (ctx, ct) =>
            {
                var buffer = new byte[ctx.ContentLength];
                int read = 0;
                while (read < buffer.Length)
                    read += await ctx.Body.ReadAsync(buffer, read, buffer.Length - read, ct);
                await ctx.Response.SendAsync(buffer, ct);
            });
        await using var host = TestHost.Start(o => o.Use(routes));
        using var client = host.CreateClient();

        var payload = new byte[300 * 1024];
        Random.Shared.NextBytes(payload);
        foreach (var path in new[] { "/whole", "/stream", "/legacy" })
        {
            using var response = await client.PostAsync(path, new ByteArrayContent(payload));
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            Assert.Equal(payload, await response.Content.ReadAsByteArrayAsync());
        }

        using var empty = await client.PostAsync("/whole", new ByteArrayContent([]));
        Assert.Equal(HttpStatusCode.OK, empty.StatusCode);
        Assert.Empty(await empty.Content.ReadAsByteArrayAsync());
    }

    [Fact]
    public async Task Head_returns_headers_without_body()
    {
        var routes = new RouteTable()
            .MapGet("/fixed", (ctx, ct) => ctx.Response.SendTextAsync("hello world", cancellationToken: ct))
            .MapGet("/chunked", async (ctx, ct) =>
            {
                await ctx.Response.WriteAsync(Encoding.ASCII.GetBytes("part1"), ct);
                await ctx.Response.WriteAsync(Encoding.ASCII.GetBytes("part2"), ct);
                await ctx.Response.CompleteAsync(ct);
            });
        await using var host = TestHost.Start(o => o.Use(routes));
        using var client = host.CreateClient();

        using var head = await client.SendAsync(new HttpRequestMessage(HttpMethod.Head, "/fixed"));
        Assert.Equal(HttpStatusCode.OK, head.StatusCode);
        Assert.Equal(11, head.Content.Headers.ContentLength);
        Assert.Empty(await head.Content.ReadAsByteArrayAsync());

        using var headChunked = await client.SendAsync(new HttpRequestMessage(HttpMethod.Head, "/chunked"));
        Assert.Equal(HttpStatusCode.OK, headChunked.StatusCode);
        Assert.Null(headChunked.Headers.TransferEncodingChunked);
        Assert.Empty(await headChunked.Content.ReadAsByteArrayAsync());

        // the connection is still usable afterwards
        using var get = await client.GetAsync("/chunked");
        Assert.Equal("part1part2", await get.Content.ReadAsStringAsync());
        Assert.Equal(1, host.Server.TotalConnectionsAccepted);
    }

    [Fact]
    public async Task Keep_alive_reuses_one_connection()
    {
        var routes = new RouteTable().MapGet("/", (ctx, ct) => ctx.Response.SendTextAsync(ctx.ConnectionId.ToString(System.Globalization.CultureInfo.InvariantCulture), cancellationToken: ct));
        await using var host = TestHost.Start(o => o.Use(routes));
        using var client = host.CreateClient();
        var ids = new HashSet<string>();
        for (int i = 0; i < 5; i++)
            ids.Add(await client.GetStringAsync("/"));
        Assert.Single(ids);
        Assert.Equal(1, host.Server.TotalConnectionsAccepted);
        Assert.Equal(1, host.Server.ActiveConnections);
    }

    [Fact]
    public async Task Max_requests_per_connection_closes_after_limit()
    {
        var routes = new RouteTable().MapGet("/", (ctx, ct) => ctx.Response.SendTextAsync("x", cancellationToken: ct));
        await using var host = TestHost.Start(o =>
        {
            o.Limits.MaxRequestsPerConnection = 2;
            o.Use(routes);
        });
        using var client = host.CreateClient();
        for (int i = 0; i < 4; i++)
            await client.GetStringAsync("/");
        Assert.Equal(2, host.Server.TotalConnectionsAccepted);
    }

    [Fact]
    public async Task Streamed_responses_use_chunked_encoding()
    {
        var routes = new RouteTable()
            .MapGet("/chunks", async (ctx, ct) =>
            {
                await ctx.Response.StartAsync(null, ct);
                await ctx.Response.WriteAsync(new byte[20_000], ct); // larger than the inline limit
                await ctx.Response.WriteAsync(Encoding.ASCII.GetBytes("tail"), ct);
                await ctx.Response.WriteAsync(ReadOnlyMemory<byte>.Empty, ct);
                await ctx.Response.CompleteAsync(ct);
                await ctx.Response.CompleteAsync(ct); // idempotent
                Assert.Equal(20_004, ctx.Response.BytesWritten);
            })
            .MapGet("/stream", (ctx, ct) => ctx.Response.SendStreamAsync(new NonSeekableStream(Encoding.ASCII.GetBytes("streamed!")), null, ct))
            .MapGet("/seekable", (ctx, ct) => ctx.Response.SendStreamAsync(new MemoryStream(Encoding.ASCII.GetBytes("seekable")), null, ct))
            .MapGet("/implicit", async (ctx, ct) =>
            {
                await ctx.Response.WriteAsync(Encoding.ASCII.GetBytes("implicit start"), ct);
                // handler returns without completing: the server completes the chunked body
            });
        await using var host = TestHost.Start(o => o.Use(routes));
        using var client = host.CreateClient();

        using var chunks = await client.GetAsync("/chunks");
        Assert.True(chunks.Headers.TransferEncodingChunked);
        var bytes = await chunks.Content.ReadAsByteArrayAsync();
        Assert.Equal(20_004, bytes.Length);
        Assert.Equal("tail", Encoding.ASCII.GetString(bytes[^4..]));

        using var stream = await client.GetAsync("/stream");
        Assert.True(stream.Headers.TransferEncodingChunked);
        Assert.Equal("streamed!", await stream.Content.ReadAsStringAsync());

        using var seekable = await client.GetAsync("/seekable");
        Assert.Equal(8, seekable.Content.Headers.ContentLength);
        Assert.Equal("seekable", await seekable.Content.ReadAsStringAsync());

        Assert.Equal("implicit start", await client.GetStringAsync("/implicit"));
        Assert.Equal(1, host.Server.TotalConnectionsAccepted);
    }

    [Fact]
    public async Task Handler_exception_yields_500_and_reports()
    {
        var errors = new List<Exception>();
        var routes = new RouteTable()
            .MapGet("/boom", (_, _) => throw new InvalidOperationException("boom"))
            .MapGet("/late", async (ctx, ct) =>
            {
                await ctx.Response.StartAsync(100, ct);
                throw new InvalidOperationException("late");
            })
            .MapGet("/ok", (ctx, ct) => ctx.Response.SendTextAsync("ok", cancellationToken: ct));
        await using var host = TestHost.Start(o =>
        {
            o.OnError = e => { lock (errors) errors.Add(e); };
            o.Use(routes);
        });
        using var client = host.CreateClient();

        using var response = await client.GetAsync("/boom");
        Assert.Equal(HttpStatusCode.InternalServerError, response.StatusCode);
        Assert.Equal("500 Internal Server Error", await response.Content.ReadAsStringAsync());
        Assert.Contains(errors, e => e.Message == "boom");

        // after headers were sent the connection is aborted; HttpClient sees a transport error
        await Assert.ThrowsAnyAsync<HttpRequestException>(() => client.GetAsync("/late"));
        Assert.Contains(errors, e => e.Message == "late");

        // and the server keeps serving new connections
        Assert.Equal("ok", await client.GetStringAsync("/ok"));
    }

    [Fact]
    public async Task Expect_100_continue_with_http_client()
    {
        var routes = new RouteTable().MapPost("/echo", async (ctx, ct) =>
        {
            Assert.Equal("100-continue", ctx.Headers["Expect"]);
            var body = await ctx.ReadBodyAsync(ct);
            await ctx.Response.SendAsync(body, ct);
        });
        await using var host = TestHost.Start(o => o.Use(routes));
        using var client = host.CreateClient(h => h.Expect100ContinueTimeout = TimeSpan.FromSeconds(10));
        var request = new HttpRequestMessage(HttpMethod.Post, "/echo") { Content = new StringContent("payload") };
        request.Headers.ExpectContinue = true;
        using var response = await client.SendAsync(request);
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal("payload", await response.Content.ReadAsStringAsync());
    }

    [Fact]
    public async Task Route_table_returns_405_with_allow()
    {
        var routes = new RouteTable()
            .MapGet("/only-get", (ctx, ct) => ctx.Response.SendTextAsync("g", cancellationToken: ct))
            .MapPut("/only-get", (ctx, ct) => ctx.Response.SendTextAsync("p", cancellationToken: ct))
            .MapPrefix("DELETE", "/only-", (ctx, ct) => ctx.Response.SendTextAsync("d", cancellationToken: ct))
            .MapPrefix(null, "/any/", (ctx, ct) => ctx.Response.SendTextAsync(ctx.Method, cancellationToken: ct));
        await using var host = TestHost.Start(o => o.Use(routes));
        using var client = host.CreateClient();

        using var response = await client.PostAsync("/only-get", new StringContent("x"));
        Assert.Equal(HttpStatusCode.MethodNotAllowed, response.StatusCode);
        Assert.Equal("GET, HEAD, PUT, DELETE", string.Join(", ", response.Content.Headers.Allow));

        Assert.Equal("g", await client.GetStringAsync("/only-get"));
        using var put = await client.PutAsync("/only-get", new StringContent("x"));
        Assert.Equal("p", await put.Content.ReadAsStringAsync());
        using var del = await client.DeleteAsync("/only-get");
        Assert.Equal("d", await del.Content.ReadAsStringAsync());
        using var patch = await client.PatchAsync("/any/thing", new StringContent("x"));
        Assert.Equal("PATCH", await patch.Content.ReadAsStringAsync());
        using var head = await client.SendAsync(new HttpRequestMessage(HttpMethod.Head, "/only-get"));
        Assert.Equal(HttpStatusCode.OK, head.StatusCode);
        Assert.Equal(1, head.Content.Headers.ContentLength);
    }

    [Fact]
    public async Task Path_is_decoded_and_query_kept_raw()
    {
        HttpRequestContext? seen = null;
        string? path = null, query = null, raw = null, method = null, hostHeader = null;
        bool secure = true;
        IPEndPoint? remote = null, local = null;
        var routes = new RouteTable().MapPrefix(null, "/", (ctx, ct) =>
        {
            seen = ctx;
            path = ctx.Path;
            query = ctx.QueryString;
            raw = ctx.RawTarget;
            method = ctx.Method;
            hostHeader = ctx.Host;
            secure = ctx.IsSecure;
            remote = ctx.RemoteEndPoint;
            local = ctx.LocalEndPoint;
            Assert.Null(ctx.TlsServerName);
            Assert.False(ctx.RequestAborted.IsCancellationRequested);
            Assert.Equal(HttpProtocolVersion.Http11, ctx.Version);
            return ctx.Response.SendStatusAsync(204, ct);
        });
        await using var host = TestHost.Start(o => o.Use(routes));
        using var client = host.CreateClient();
        using var response = await client.GetAsync("/a%20b/c%C3%A9?q=%20x&y");
        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
        Assert.NotNull(seen);
        Assert.Equal("/a b/cé", path);
        Assert.Equal("q=%20x&y", query);
        Assert.Equal("/a%20b/c%C3%A9?q=%20x&y", raw);
        Assert.Equal("GET", method);
        Assert.Equal("127.0.0.1:" + host.EndPoint.Port, hostHeader);
        Assert.False(secure);
        Assert.Equal(IPAddress.Loopback, remote!.Address);
        Assert.Equal(host.EndPoint.Port, local!.Port);
    }

    [Fact]
    public async Task Response_close_connection_forces_new_connection()
    {
        var routes = new RouteTable().MapGet("/", (ctx, ct) =>
        {
            ctx.Response.CloseConnection = true;
            return ctx.Response.SendTextAsync("bye", cancellationToken: ct);
        });
        await using var host = TestHost.Start(o => o.Use(routes));
        using var client = host.CreateClient();
        await client.GetStringAsync("/");
        await client.GetStringAsync("/");
        Assert.Equal(2, host.Server.TotalConnectionsAccepted);
    }

    [Fact]
    public async Task Response_api_guards()
    {
        Exception? captured = null;
        var routes = new RouteTable()
            .MapGet("/double-start", async (ctx, ct) =>
            {
                await ctx.Response.StartAsync(0, ct);
                try { await ctx.Response.StartAsync(0, ct); } catch (InvalidOperationException e) { captured = e; }
                try { await ctx.Response.SendAsync(new byte[1], ct); } catch (InvalidOperationException) { }
                try { await ctx.Response.SendTextAsync("x", cancellationToken: ct); } catch (InvalidOperationException) { }
                try { await ctx.Response.SendFileAsync("x", ct); } catch (InvalidOperationException) { }
                try { await ctx.Response.SendStreamAsync(Stream.Null, null, ct); } catch (InvalidOperationException) { }
                await ctx.Response.CompleteAsync(ct);
                try { await ctx.Response.WriteAsync(new byte[1], ct); } catch (InvalidOperationException) { }
            })
            .MapGet("/overflow", async (ctx, ct) =>
            {
                await ctx.Response.StartAsync(2, ct);
                await ctx.Response.WriteAsync(new byte[1], ct);
                await Assert.ThrowsAsync<InvalidOperationException>(async () => await ctx.Response.WriteAsync(new byte[2], ct));
                await ctx.Response.WriteAsync(new byte[1], ct);
                await ctx.Response.CompleteAsync(ct);
            })
            .MapGet("/negative", (ctx, ct) => ctx.Response.StartAsync(-1, ct))
            .MapGet("/bad-status", (ctx, ct) =>
            {
                ctx.Response.StatusCode = 42;
                return ctx.Response.SendStatusAsync(42, ct);
            })
            .MapGet("/reason", (ctx, ct) =>
            {
                ctx.Response.StatusCode = 299;
                ctx.Response.ReasonPhrase = "Custom";
                ctx.Response.ContentType = "application/x-test";
                Assert.Equal("application/x-test", ctx.Response.ContentType);
                return ctx.Response.SendTextAsync("r", contentType: null!, cancellationToken: ct);
            });
        await using var host = TestHost.Start(o => o.Use(routes));
        using var client = host.CreateClient();

        using var r1 = await client.GetAsync("/double-start");
        Assert.Equal(HttpStatusCode.OK, r1.StatusCode);
        Assert.IsType<InvalidOperationException>(captured);

        using var r2 = await client.GetAsync("/overflow");
        Assert.Equal(2, (await r2.Content.ReadAsByteArrayAsync()).Length);

        using var r3 = await client.GetAsync("/negative");
        Assert.Equal(HttpStatusCode.InternalServerError, r3.StatusCode);

        using var r4 = await client.GetAsync("/bad-status");
        Assert.Equal(HttpStatusCode.InternalServerError, r4.StatusCode);

        using var r5 = await client.GetAsync("/reason");
        Assert.Equal(299, (int)r5.StatusCode);
        Assert.Equal("Custom", r5.ReasonPhrase);
        Assert.Equal("application/x-test", r5.Content.Headers.ContentType!.MediaType);
    }

    [Fact]
    public async Task Handler_that_claims_without_writing_gets_empty_200()
    {
        await using var host = TestHost.Start(o => o.Use(new ClaimingHandler()));
        using var client = host.CreateClient();
        using var response = await client.GetAsync("/");
        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
        Assert.Equal(0, response.Content.Headers.ContentLength);
        Assert.Empty(await response.Content.ReadAsByteArrayAsync());
    }

    private sealed class ClaimingHandler : IHttpHandler
    {
        public ValueTask<bool> TryHandleAsync(HttpRequestContext context, CancellationToken cancellationToken) => new(true);
    }

    [Fact]
    public async Task Handlers_may_pass_their_own_cancellation_tokens()
    {
        var routes = new RouteTable()
            .MapPost("/own", async (ctx, ct) =>
            {
                using var own = new CancellationTokenSource(TimeSpan.FromSeconds(30));
                var body = await ctx.ReadBodyAsync(own.Token);
                await ctx.Response.SendAsync(body, own.Token);
            })
            .MapPost("/cancelled", async (ctx, ct) =>
            {
                using var own = new CancellationTokenSource();
                own.Cancel();
                var buffer = new byte[16];
                await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await ctx.Body.ReadAsync(buffer, own.Token));
                // the connection's own timeout source was replaced, so the response can still be written
                await ctx.Response.SendTextAsync("recovered", cancellationToken: ct);
            })
            .MapDelete("/gone", (ctx, ct) => ctx.Response.SendStatusAsync(204, ct));
        routes.MaxRequestBodyBytes = 1024;
        await using var host = TestHost.Start(o => o.Use(routes));
        using var client = host.CreateClient();
        using var own = await client.PostAsync("/own", new StringContent("payload"));
        Assert.Equal("payload", await own.Content.ReadAsStringAsync());

        using var raw = await RawClient.ConnectAsync(host.EndPoint);
        await raw.SendAsync("POST /cancelled HTTP/1.1\r\nHost: a\r\nContent-Length: 4\r\n\r\n");
        await Task.Delay(50);
        var response = await raw.ReadResponseAsync();
        Assert.Equal("recovered", response.Body);
        Assert.Equal("close", response["Connection"]); // the unread body cannot be trusted after a cancelled read
        using var del = await client.DeleteAsync("/gone");
        Assert.Equal(HttpStatusCode.NoContent, del.StatusCode);
    }

    [Fact]
    public async Task Truncated_declared_body_closes_connection()
    {
        var routes = new RouteTable().MapGet("/short", async (ctx, ct) =>
        {
            await ctx.Response.StartAsync(10, ct);
            await ctx.Response.WriteAsync(new byte[3], ct);
            await ctx.Response.CompleteAsync(ct);
            Assert.True(ctx.Response.CloseConnection);
        });
        await using var host = TestHost.Start(o => o.Use(routes));
        using var raw = await RawClient.ConnectAsync(host.EndPoint);
        await raw.SendAsync("GET /short HTTP/1.1\r\nHost: x\r\n\r\n");
        var text = await raw.ReadToEndAsync();
        Assert.StartsWith("HTTP/1.1 200 OK", text, StringComparison.Ordinal);
        Assert.Contains("Content-Length: 10", text, StringComparison.Ordinal);
        Assert.EndsWith("\r\n\r\n\0\0\0", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Http10_client_gets_close_semantics()
    {
        var routes = new RouteTable().MapGet("/", (ctx, ct) => ctx.Response.SendTextAsync("v" + (int)ctx.Version, cancellationToken: ct));
        await using var host = TestHost.Start(o => o.Use(routes));
        using var client = host.CreateClient(version: HttpVersion.Version10);
        Assert.Equal("v0", await client.GetStringAsync("/"));
        Assert.Equal("v0", await client.GetStringAsync("/"));
        Assert.Equal(2, host.Server.TotalConnectionsAccepted);
    }

    [Fact]
    public async Task Multiple_endpoints_and_options_helpers()
    {
        var options = new HttpServerOptions()
            .Listen(new IPEndPoint(IPAddress.Loopback, 0))
            .Listen(IPAddress.Loopback, 0)
            .Use(new HealthHandler());
        Assert.Throws<ArgumentNullException>(() => options.Use(null!));
        Assert.Throws<ArgumentNullException>(() => new HttpEndpointOptions(null!));
        Assert.False(options.Endpoints[0].IsSecure);
        await using var server = new HttpServer(options);
        Assert.Empty(server.BoundEndpoints);
        Assert.False(server.IsRunning);
        server.Start();
        Assert.True(server.IsRunning);
        Assert.Equal(2, server.BoundEndpoints.Count);
        Assert.NotEqual(server.BoundEndpoints[0].Port, server.BoundEndpoints[1].Port);
        Assert.Same(options, server.Options);
        foreach (var ep in server.BoundEndpoints)
        {
            using var client = new HttpClient();
            Assert.Equal("ok", await client.GetStringAsync("http://127.0.0.1:" + ep.Port + "/healthz"));
        }
    }

    private sealed class NonSeekableStream(byte[] data) : Stream
    {
        private readonly MemoryStream _inner = new(data);

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => false;

        public override long Length => throw new NotSupportedException();

        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

        public override void Flush() { }

        public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) => _inner.ReadAsync(buffer.Length > 3 ? buffer[..3] : buffer, cancellationToken);

        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
