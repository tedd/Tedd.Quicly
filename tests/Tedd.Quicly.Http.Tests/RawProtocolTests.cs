using System.Text;
using Tedd.Quicly.Http.Handlers;

namespace Tedd.Quicly.Http.Tests;

public class RawProtocolTests
{
    private static RouteTable EchoRoutes() => new RouteTable()
        .MapPrefix("GET", "/", (ctx, ct) => ctx.Response.SendTextAsync("path=" + ctx.Path + ";host=" + ctx.Host + ";v=" + (int)ctx.Version, cancellationToken: ct))
        .MapPrefix("OPTIONS", "/", (ctx, ct) => ctx.Response.SendTextAsync("path=" + ctx.Path, cancellationToken: ct))
        .MapPrefix("POST", "/", async (ctx, ct) =>
        {
            var body = await ctx.ReadBodyAsync(ct);
            await ctx.Response.SendTextAsync("len=" + body.Length + ";" + Encoding.UTF8.GetString(body.Span), cancellationToken: ct);
        });

    [Theory]
    [InlineData("GET / HTTP/1.1\nHost: x\r\n\r\n", 400)]
    [InlineData("GET / HTTP/1.1\r\nHost: x\r\n folded\r\n\r\n", 400)]
    [InlineData("GET / HTTP/1.1\r\n\r\n", 400)]                                  // missing Host on 1.1
    [InlineData("GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n", 400)]            // duplicate Host
    [InlineData("GET / HTTP/1.1\r\nHost: a\r\nContent-Length: x\r\n\r\n", 400)]
    [InlineData("GET / HTTP/1.1\r\nHost: a\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\n", 400)]
    [InlineData("GET / HTTP/1.1\r\nHost: a\r\nContent-Length: 1\r\nTransfer-Encoding: chunked\r\n\r\n", 400)]
    [InlineData("GET nopath HTTP/1.1\r\nHost: a\r\n\r\n", 400)]
    [InlineData("GET /%zz HTTP/1.1\r\nHost: a\r\n\r\n", 400)]
    [InlineData("GET http:// HTTP/1.1\r\n\r\n", 400)]
    [InlineData("GET http://h?x=1 HTTP/1.1\r\n\r\n", 400)]
    [InlineData("GET * HTTP/1.1\r\nHost: a\r\n\r\n", 400)]                       // '*' only for OPTIONS
    [InlineData("POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n", 411)]
    [InlineData("POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: gzip, chunked\r\n\r\n", 501)]
    [InlineData("POST / HTTP/1.1\r\nHost: a\r\nExpect: something-else\r\n\r\n", 417)]
    [InlineData("GET / HTTP/2.0\r\nHost: a\r\n\r\n", 505)]
    public async Task Malformed_requests_get_error_and_close(string request, int expectedStatus)
    {
        await using var host = TestHost.Start(o => o.Use(EchoRoutes()));
        using var raw = await RawClient.ConnectAsync(host.EndPoint);
        await raw.SendAsync(request);
        var response = await raw.ReadResponseAsync();
        Assert.Equal(expectedStatus, response.StatusCode);
        Assert.Equal("close", response["Connection"]);
        Assert.Equal(expectedStatus + " " + HttpReasonPhrases.Get(expectedStatus), response.Body);
        Assert.True(await raw.WaitForCloseAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task Oversize_headers_431_and_request_line_414()
    {
        await using var host = TestHost.Start(o =>
        {
            o.Limits.MaxRequestLineBytes = 64;
            o.Limits.MaxHeadersBytes = 256;
            o.Use(EchoRoutes());
        });

        using (var raw = await RawClient.ConnectAsync(host.EndPoint))
        {
            await raw.SendAsync("GET / HTTP/1.1\r\nHost: a\r\nX-Big: " + new string('v', 300) + "\r\n\r\n");
            var response = await raw.ReadResponseAsync();
            Assert.Equal(431, response.StatusCode);
            Assert.True(await raw.WaitForCloseAsync(TimeSpan.FromSeconds(5)));
        }
        using (var raw = await RawClient.ConnectAsync(host.EndPoint))
        {
            await raw.SendAsync("GET /" + new string('a', 100) + " HTTP/1.1\r\nHost: a\r\n\r\n");
            var response = await raw.ReadResponseAsync();
            Assert.Equal(414, response.StatusCode);
            Assert.True(await raw.WaitForCloseAsync(TimeSpan.FromSeconds(5)));
        }
        using (var raw = await RawClient.ConnectAsync(host.EndPoint))
        {
            // never-terminated header block larger than the whole budget, sent in pieces
            await raw.SendAsync("GET / HTTP/1.1\r\nHost: a\r\nX: ");
            await raw.SendAsync(new string('v', 400));
            var response = await raw.ReadResponseAsync();
            Assert.Equal(431, response.StatusCode);
        }
        using (var raw = await RawClient.ConnectAsync(host.EndPoint))
        {
            // too many header fields
            var sb = new StringBuilder("GET / HTTP/1.1\r\nHost: a\r\n");
            for (int i = 0; i < 150; i++)
                sb.Append("A: 1\r\n");
            sb.Append("\r\n");
            await raw.SendAsync(sb.ToString());
            var response = await raw.ReadResponseAsync();
            Assert.Equal(431, response.StatusCode);
        }
    }

    [Fact]
    public async Task Body_over_limit_413()
    {
        await using var host = TestHost.Start(o =>
        {
            o.Limits.MaxRequestBodyBytes = 10;
            o.Use(EchoRoutes());
        });
        using var raw = await RawClient.ConnectAsync(host.EndPoint);
        await raw.SendAsync("POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 11\r\n\r\n");
        var response = await raw.ReadResponseAsync();
        Assert.Equal(413, response.StatusCode);
        Assert.True(await raw.WaitForCloseAsync(TimeSpan.FromSeconds(5)));

        using var ok = await RawClient.ConnectAsync(host.EndPoint);
        await ok.SendAsync("POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 10\r\n\r\n0123456789");
        var okResponse = await ok.ReadResponseAsync();
        Assert.Equal(200, okResponse.StatusCode);
        Assert.Equal("len=10;0123456789", okResponse.Body);
    }

    [Fact]
    public async Task Pipelined_requests_are_answered_in_order()
    {
        await using var host = TestHost.Start(o => o.Use(EchoRoutes()));
        using var raw = await RawClient.ConnectAsync(host.EndPoint);
        await raw.SendAsync(
            "GET /one HTTP/1.1\r\nHost: a\r\n\r\n" +
            "POST /two HTTP/1.1\r\nHost: a\r\nContent-Length: 3\r\n\r\nabc" +
            "GET /three HTTP/1.1\r\nHost: a\r\n\r\n");
        var r1 = await raw.ReadResponseAsync();
        var r2 = await raw.ReadResponseAsync();
        var r3 = await raw.ReadResponseAsync();
        Assert.Equal("path=/one;host=a;v=1", r1.Body);
        Assert.Equal("len=3;abc", r2.Body);
        Assert.Equal("path=/three;host=a;v=1", r3.Body);
        Assert.Null(r1["Connection"]);
        Assert.Equal(1, host.Server.TotalConnectionsAccepted);
    }

    [Fact]
    public async Task Request_split_across_many_writes()
    {
        await using var host = TestHost.Start(o => o.Use(EchoRoutes()));
        using var raw = await RawClient.ConnectAsync(host.EndPoint);
        const string request = "POST /split HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\nhello";
        foreach (char c in request)
        {
            await raw.SendAsync(c.ToString());
            await Task.Delay(1);
        }
        var response = await raw.ReadResponseAsync();
        Assert.Equal("len=5;hello", response.Body);
    }

    [Fact]
    public async Task Http10_closes_unless_keep_alive_requested()
    {
        await using var host = TestHost.Start(o => o.Use(EchoRoutes()));
        using (var raw = await RawClient.ConnectAsync(host.EndPoint))
        {
            await raw.SendAsync("GET /a HTTP/1.0\r\n\r\n");
            var response = await raw.ReadResponseAsync();
            Assert.Equal(200, response.StatusCode);
            Assert.Equal("HTTP/1.1", response.Version);
            Assert.Equal("close", response["Connection"]);
            Assert.Equal("path=/a;host=;v=0", response.Body);
            Assert.True(await raw.WaitForCloseAsync(TimeSpan.FromSeconds(5)));
        }
        using (var raw = await RawClient.ConnectAsync(host.EndPoint))
        {
            await raw.SendAsync("GET /a HTTP/1.0\r\nConnection: keep-alive\r\n\r\n");
            var response = await raw.ReadResponseAsync();
            Assert.Equal("keep-alive", response["Connection"]);
            await raw.SendAsync("GET /b HTTP/1.0\r\nConnection: Keep-Alive\r\nHost: h\r\n\r\n");
            var second = await raw.ReadResponseAsync();
            Assert.Equal("path=/b;host=h;v=0", second.Body);
            await raw.SendAsync("GET /c HTTP/1.0\r\n\r\n");
            var third = await raw.ReadResponseAsync();
            Assert.Equal("close", third["Connection"]);
            Assert.True(await raw.WaitForCloseAsync(TimeSpan.FromSeconds(5)));
        }
        Assert.Equal(2, host.Server.TotalConnectionsAccepted);
    }

    [Fact]
    public async Task Http10_unknown_length_response_is_close_delimited()
    {
        var routes = new RouteTable().MapGet("/", async (ctx, ct) =>
        {
            await ctx.Response.WriteAsync(Encoding.ASCII.GetBytes("abc"), ct);
            await ctx.Response.WriteAsync(Encoding.ASCII.GetBytes("def"), ct);
            await ctx.Response.CompleteAsync(ct);
        });
        await using var host = TestHost.Start(o => o.Use(routes));
        using var raw = await RawClient.ConnectAsync(host.EndPoint);
        await raw.SendAsync("GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n");
        var text = await raw.ReadToEndAsync();
        Assert.Contains("Connection: close", text, StringComparison.Ordinal);
        Assert.DoesNotContain("Transfer-Encoding", text, StringComparison.Ordinal);
        Assert.DoesNotContain("Content-Length", text, StringComparison.Ordinal);
        Assert.EndsWith("\r\n\r\nabcdef", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Http11_connection_close_is_honoured()
    {
        await using var host = TestHost.Start(o => o.Use(EchoRoutes()));
        using var raw = await RawClient.ConnectAsync(host.EndPoint);
        await raw.SendAsync("GET / HTTP/1.1\r\nHost: a\r\nConnection: keep-alive, close\r\n\r\n");
        var response = await raw.ReadResponseAsync();
        Assert.Equal("close", response["Connection"]);
        Assert.True(await raw.WaitForCloseAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task Slow_headers_time_out_and_close()
    {
        await using var host = TestHost.Start(o =>
        {
            o.Limits.HeaderReadTimeout = TimeSpan.FromMilliseconds(400);
            o.Use(EchoRoutes());
        });
        using var raw = await RawClient.ConnectAsync(host.EndPoint);
        await raw.SendAsync("GET / HTTP/1.1\r\nHost: a\r\nX-Slow: ");
        var sw = System.Diagnostics.Stopwatch.StartNew();
        Assert.True(await raw.WaitForCloseAsync(TimeSpan.FromSeconds(10)));
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(8));
        await HttpServerTests.WaitUntilAsync(() => host.Server.ActiveConnections == 0);
    }

    [Fact]
    public async Task Slow_trickle_headers_hit_total_deadline()
    {
        await using var host = TestHost.Start(o =>
        {
            o.Limits.HeaderReadTimeout = TimeSpan.FromMilliseconds(500);
            o.Use(EchoRoutes());
        });
        using var raw = await RawClient.ConnectAsync(host.EndPoint);
        // second request on a keep-alive connection: first byte starts the header deadline
        await raw.SendAsync("GET / HTTP/1.1\r\nHost: a\r\n\r\n");
        await raw.ReadResponseAsync();
        var closed = false;
        for (int i = 0; i < 40 && !closed; i++)
        {
            try
            {
                await raw.SendAsync("X");
            }
            catch (IOException)
            {
                closed = true;
            }
            await Task.Delay(50);
        }
        Assert.True(closed || await raw.WaitForCloseAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task Idle_keep_alive_connection_times_out()
    {
        await using var host = TestHost.Start(o =>
        {
            o.Limits.KeepAliveTimeout = TimeSpan.FromMilliseconds(300);
            o.Use(EchoRoutes());
        });
        using var raw = await RawClient.ConnectAsync(host.EndPoint);
        await raw.SendAsync("GET / HTTP/1.1\r\nHost: a\r\n\r\n");
        await raw.ReadResponseAsync();
        Assert.True(await raw.WaitForCloseAsync(TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task Absolute_form_options_star_and_leading_crlf()
    {
        await using var host = TestHost.Start(o => o.Use(EchoRoutes()));
        using var raw = await RawClient.ConnectAsync(host.EndPoint);
        await raw.SendAsync("GET http://proxy.example:8080/abs?x HTTP/1.1\r\n\r\n");
        Assert.Equal("path=/abs;host=proxy.example:8080;v=1", (await raw.ReadResponseAsync()).Body);
        await raw.SendAsync("GET HTTPS://other.example HTTP/1.1\r\nHost: real\r\n\r\n");
        Assert.Equal("path=/;host=real;v=1", (await raw.ReadResponseAsync()).Body);
        await raw.SendAsync("\r\n\r\nOPTIONS * HTTP/1.1\r\nHost: a\r\n\r\n");
        Assert.Equal("path=*", (await raw.ReadResponseAsync()).Body);
        await raw.SendAsync("GET /café HTTP/1.1\r\nHost: a\r\n\r\n");
        Assert.Equal(400, (await raw.ReadResponseAsync()).StatusCode);
    }

    [Fact]
    public async Task Expect_100_continue_raw()
    {
        await using var host = TestHost.Start(o => o.Use(EchoRoutes()));
        using var raw = await RawClient.ConnectAsync(host.EndPoint);
        await raw.SendAsync("POST / HTTP/1.1\r\nHost: a\r\nExpect: 100-continue\r\nContent-Length: 4\r\n\r\n");
        var interim = await raw.ReadResponseAsync();
        Assert.Equal(100, interim.StatusCode);
        await raw.SendAsync("data");
        var final = await raw.ReadResponseAsync();
        Assert.Equal(200, final.StatusCode);
        Assert.Equal("len=4;data", final.Body);

        // HTTP/1.0 clients never get 100 Continue and Expect is ignored
        await raw.SendAsync("POST / HTTP/1.0\r\nConnection: keep-alive\r\nExpect: 100-continue\r\nContent-Length: 2\r\n\r\nhi");
        var v10 = await raw.ReadResponseAsync();
        Assert.Equal(200, v10.StatusCode);
        Assert.Equal("len=2;hi", v10.Body);

        // Expect with an empty body: no interim response either
        await raw.SendAsync("POST / HTTP/1.1\r\nHost: a\r\nExpect: 100-continue\r\nContent-Length: 0\r\n\r\n");
        var empty = await raw.ReadResponseAsync();
        Assert.Equal("len=0;", empty.Body);
    }

    [Fact]
    public async Task Unread_body_after_expect_closes_connection()
    {
        var routes = new RouteTable().MapPost("/reject", (ctx, ct) => ctx.Response.SendStatusAsync(403, ct));
        await using var host = TestHost.Start(o => o.Use(routes));
        using var raw = await RawClient.ConnectAsync(host.EndPoint);
        await raw.SendAsync("POST /reject HTTP/1.1\r\nHost: a\r\nExpect: 100-continue\r\nContent-Length: 4\r\n\r\n");
        var response = await raw.ReadResponseAsync();
        Assert.Equal(403, response.StatusCode);
        Assert.Equal("close", response["Connection"]);
        Assert.True(await raw.WaitForCloseAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task Unread_body_is_drained_and_connection_reused()
    {
        var routes = new RouteTable()
            .MapPost("/ignore", (ctx, ct) => ctx.Response.SendTextAsync("ignored", cancellationToken: ct))
            .MapPost("/partial", async (ctx, ct) =>
            {
                var two = new byte[2];
                Assert.Equal(2, await ctx.Body.ReadAsync(two, ct));
                await ctx.Response.SendTextAsync(Encoding.ASCII.GetString(two), cancellationToken: ct);
            })
            .MapGet("/", (ctx, ct) => ctx.Response.SendTextAsync("get", cancellationToken: ct));
        await using var host = TestHost.Start(o => o.Use(routes));
        using var raw = await RawClient.ConnectAsync(host.EndPoint);

        // body fully buffered with the headers
        await raw.SendAsync("POST /ignore HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\nabcdeGET / HTTP/1.1\r\nHost: a\r\n\r\n");
        Assert.Equal("ignored", (await raw.ReadResponseAsync()).Body);
        Assert.Equal("get", (await raw.ReadResponseAsync()).Body);

        // body arriving after the response was sent
        await raw.SendAsync("POST /ignore HTTP/1.1\r\nHost: a\r\nContent-Length: 70000\r\n\r\n");
        Assert.Equal("ignored", (await raw.ReadResponseAsync()).Body);
        await raw.SendAsync(new byte[70000]);
        await raw.SendAsync("GET / HTTP/1.1\r\nHost: a\r\n\r\n");
        Assert.Equal("get", (await raw.ReadResponseAsync()).Body);

        // partially read body
        await raw.SendAsync("POST /partial HTTP/1.1\r\nHost: a\r\nContent-Length: 6\r\n\r\nxyzabc");
        Assert.Equal("xy", (await raw.ReadResponseAsync()).Body);
        await raw.SendAsync("GET / HTTP/1.1\r\nHost: a\r\n\r\n");
        Assert.Equal("get", (await raw.ReadResponseAsync()).Body);
        Assert.Equal(1, host.Server.TotalConnectionsAccepted);
    }

    [Fact]
    public async Task Body_cut_short_closes_connection_without_response()
    {
        var errors = new List<Exception>();
        await using var host = TestHost.Start(o =>
        {
            o.OnError = e => { lock (errors) errors.Add(e); };
            o.Use(EchoRoutes());
        });
        using var raw = await RawClient.ConnectAsync(host.EndPoint);
        await raw.SendAsync("POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 10\r\n\r\nabc");
        raw.Socket.Shutdown(System.Net.Sockets.SocketShutdown.Send);
        var text = await raw.ReadToEndAsync();
        Assert.Equal(string.Empty, text);
        await HttpServerTests.WaitUntilAsync(() => host.Server.ActiveConnections == 0);
        Assert.Empty(errors);
    }

    [Fact]
    public async Task Body_read_timeout_closes_connection()
    {
        await using var host = TestHost.Start(o =>
        {
            o.Limits.RequestBodyReadTimeout = TimeSpan.FromMilliseconds(300);
            o.Use(EchoRoutes());
        });
        using var raw = await RawClient.ConnectAsync(host.EndPoint);
        await raw.SendAsync("POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 10\r\n\r\nabc");
        Assert.True(await raw.WaitForCloseAsync(TimeSpan.FromSeconds(10)));
    }

    [Fact]
    public async Task Synchronous_body_reads_work()
    {
        var routes = new RouteTable().MapPost("/sync", (ctx, ct) =>
        {
            var buffer = new byte[64];
            int total = 0;
            int n;
            while ((n = ctx.Body.Read(buffer, total, buffer.Length - total)) > 0)
                total += n;
            Assert.Equal(0, ctx.Body.Read(Span<byte>.Empty));
            return ctx.Response.SendTextAsync(Encoding.ASCII.GetString(buffer, 0, total), cancellationToken: ct);
        });
        await using var host = TestHost.Start(o => o.Use(routes));
        using var raw = await RawClient.ConnectAsync(host.EndPoint);
        // buffered part + part read from the socket, with 100-continue sent synchronously
        await raw.SendAsync("POST /sync HTTP/1.1\r\nHost: a\r\nExpect: 100-continue\r\nContent-Length: 8\r\n\r\n");
        Assert.Equal(100, (await raw.ReadResponseAsync()).StatusCode);
        await raw.SendAsync("abcd");
        await Task.Delay(50);
        await raw.SendAsync("efgh");
        Assert.Equal("abcdefgh", (await raw.ReadResponseAsync()).Body);

        await raw.SendAsync("POST /sync HTTP/1.1\r\nHost: a\r\nContent-Length: 3\r\n\r\nxyz");
        Assert.Equal("xyz", (await raw.ReadResponseAsync()).Body);

        // synchronous read hitting EOF
        await raw.SendAsync("POST /sync HTTP/1.1\r\nHost: a\r\nContent-Length: 10\r\n\r\nab");
        raw.Socket.Shutdown(System.Net.Sockets.SocketShutdown.Send);
        Assert.Equal(string.Empty, await raw.ReadToEndAsync());
    }

    [Fact]
    public async Task Body_stream_rejects_unsupported_operations()
    {
        var routes = new RouteTable().MapPost("/", (ctx, ct) =>
        {
            var body = ctx.Body;
            Assert.Throws<NotSupportedException>(() => body.Length);
            Assert.Throws<NotSupportedException>(() => body.Position);
            Assert.Throws<NotSupportedException>(() => body.Position = 1);
            Assert.Throws<NotSupportedException>(() => body.Seek(0, SeekOrigin.Begin));
            Assert.Throws<NotSupportedException>(() => body.SetLength(0));
            Assert.Throws<NotSupportedException>(() => body.Write(new byte[1], 0, 1));
            Assert.Throws<NotSupportedException>(() => body.Write(new byte[1].AsSpan()));
            return ctx.Response.SendStatusAsync(204, ct);
        });
        await using var host = TestHost.Start(o => o.Use(routes));
        using var raw = await RawClient.ConnectAsync(host.EndPoint);
        await raw.SendAsync("POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 1\r\n\r\nx");
        Assert.Equal(204, (await raw.ReadResponseAsync()).StatusCode);
    }

    [Fact]
    public async Task Client_disconnect_without_request_is_silent()
    {
        var errors = new List<Exception>();
        await using var host = TestHost.Start(o => o.OnError = e => { lock (errors) errors.Add(e); });
        using (var raw = await RawClient.ConnectAsync(host.EndPoint))
        {
            await HttpServerTests.WaitUntilAsync(() => host.Server.ActiveConnections == 1);
        }
        await HttpServerTests.WaitUntilAsync(() => host.Server.ActiveConnections == 0);
        using (var raw = await RawClient.ConnectAsync(host.EndPoint))
        {
            await raw.SendAsync("GET / HTTP/1.1\r\nHost: a");
        }
        await HttpServerTests.WaitUntilAsync(() => host.Server.ActiveConnections == 0);
        Assert.Empty(errors);
    }

    [Fact]
    public async Task Head_with_unknown_length_and_no_content_statuses()
    {
        var routes = new RouteTable()
            .MapGet("/nc", (ctx, ct) => ctx.Response.SendStatusAsync(204, ct))
            .MapGet("/nc-body", (ctx, ct) =>
            {
                ctx.Response.StatusCode = 204;
                return ctx.Response.SendTextAsync("ignored", cancellationToken: ct);
            })
            .MapGet("/nm", (ctx, ct) =>
            {
                ctx.Response.StatusCode = 304;
                return ctx.Response.SendAsync(new byte[5], ct);
            })
            .MapGet("/stream", async (ctx, ct) =>
            {
                await ctx.Response.StartAsync(null, ct);
                await ctx.Response.WriteAsync(new byte[3], ct);
                await ctx.Response.CompleteAsync(ct);
            });
        await using var host = TestHost.Start(o => o.Use(routes));
        using var raw = await RawClient.ConnectAsync(host.EndPoint);

        await raw.SendAsync("GET /nc HTTP/1.1\r\nHost: a\r\n\r\n");
        var nc = await raw.ReadResponseAsync();
        Assert.Equal(204, nc.StatusCode);
        Assert.Null(nc["Content-Length"]);
        Assert.Null(nc["Transfer-Encoding"]);

        await raw.SendAsync("GET /nc-body HTTP/1.1\r\nHost: a\r\n\r\n");
        var ncBody = await raw.ReadResponseAsync();
        Assert.Equal(204, ncBody.StatusCode);
        Assert.Null(ncBody["Content-Length"]);

        await raw.SendAsync("GET /nm HTTP/1.1\r\nHost: a\r\n\r\n");
        var nm = await raw.ReadResponseAsync();
        Assert.Equal(304, nm.StatusCode);
        Assert.Equal("5", nm["Content-Length"]);
        Assert.Equal(string.Empty, nm.Body);
        Assert.Equal("keep-alive-check", await SendAndRead(raw, "GET /nc HTTP/1.1\r\nHost: a\r\n\r\n") is { StatusCode: 204 } ? "keep-alive-check" : "broken");

        await raw.SendAsync("HEAD /stream HTTP/1.1\r\nHost: a\r\n\r\n");
        var head = await raw.ReadResponseAsync();
        Assert.Equal(200, head.StatusCode);
        Assert.Null(head["Transfer-Encoding"]);
        Assert.Null(head["Content-Length"]);

        await raw.SendAsync("GET /stream HTTP/1.1\r\nHost: a\r\n\r\n");
        var stream = await raw.ReadResponseAsync();
        Assert.Equal("chunked", stream["Transfer-Encoding"]);
        Assert.Equal(3, stream.Body.Length);
        Assert.Equal(1, host.Server.TotalConnectionsAccepted);
    }

    private static async Task<RawResponse> SendAndRead(RawClient raw, string request)
    {
        await raw.SendAsync(request);
        return await raw.ReadResponseAsync();
    }

    [Fact]
    public async Task Handler_that_declines_after_writing_closes_connection()
    {
        await using var host = TestHost.Start(o => o.Use(new DeclineAfterWriteHandler()));
        using var raw = await RawClient.ConnectAsync(host.EndPoint);
        await raw.SendAsync("GET / HTTP/1.1\r\nHost: a\r\n\r\n");
        var text = await raw.ReadToEndAsync();
        Assert.StartsWith("HTTP/1.1 200", text, StringComparison.Ordinal);
    }

    private sealed class DeclineAfterWriteHandler : IHttpHandler
    {
        public async ValueTask<bool> TryHandleAsync(HttpRequestContext context, CancellationToken cancellationToken)
        {
            await context.Response.StartAsync(null, cancellationToken);
            return false;
        }
    }
}
