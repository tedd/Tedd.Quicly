using System.Net;
using System.Net.Sockets;
using System.Text;
using Tedd.Quicly.Http.Handlers;

namespace Tedd.Quicly.Http.Tests;

/// <summary>ADR 0009 defaults and the policies that gate a request before any handler runs.</summary>
public class LimitsAndPolicyTests
{
    [Fact]
    public void Defaults_follow_adr_0009()
    {
        var limits = new HttpServerLimits();
        Assert.Equal(2 * 1024, limits.MaxRequestLineBytes);
        Assert.Equal(8 * 1024, limits.MaxHeadersBytes);
        Assert.Equal(32, limits.MaxHeaderCount);
        Assert.Equal(TimeSpan.FromSeconds(5), limits.HeaderReadTimeout);
        Assert.Equal(TimeSpan.FromSeconds(10), limits.TlsHandshakeTimeout);
        Assert.Equal(TimeSpan.FromSeconds(15), limits.KeepAliveTimeout);
        Assert.Equal(64, limits.MaxConnectionsPerAddress);
        Assert.Equal(2048, limits.MaxConnections);
        Assert.Equal(0, limits.MaxRequestBodyBytes);
        Assert.Equal(1000, limits.MaxRequestsPerConnection);
        var options = new HttpServerOptions();
        Assert.Equal(["GET", "HEAD"], options.AllowedMethods.Order());
        Assert.True(options.AddServerHeader);
    }

    [Fact]
    public async Task Methods_outside_the_allow_list_get_405_before_any_handler()
    {
        int calls = 0;
        var routes = new RouteTable().MapPrefix(null, "/", (ctx, ct) =>
        {
            calls++;
            return ctx.Response.SendTextAsync(ctx.Method, cancellationToken: ct);
        });
        var options = new HttpServerOptions().Listen(IPAddress.Loopback, 0).Use(routes);
        await using var server = new HttpServer(options);
        server.Start();
        var ep = server.BoundEndpoints[0];

        using (var raw = await RawClient.ConnectAsync(ep))
        {
            await raw.SendAsync("POST /x HTTP/1.1\r\nHost: a\r\nContent-Length: 0\r\n\r\n");
            var response = await raw.ReadResponseAsync();
            Assert.Equal(405, response.StatusCode);
            Assert.Equal("GET, HEAD", response["Allow"]);
            Assert.Equal("close", response["Connection"]);
            Assert.True(await raw.WaitForCloseAsync(TimeSpan.FromSeconds(5)));
        }
        using (var raw = await RawClient.ConnectAsync(ep))
        {
            await raw.SendAsync("get /x HTTP/1.1\r\nHost: a\r\n\r\n"); // methods are case-sensitive tokens
            Assert.Equal(405, (await raw.ReadResponseAsync()).StatusCode);
        }
        using (var raw = await RawClient.ConnectAsync(ep))
        {
            await raw.SendAsync("HEAD /x HTTP/1.1\r\nHost: a\r\n\r\n");
            Assert.Equal(200, (await raw.ReadResponseAsync(noBody: true)).StatusCode);
        }
        Assert.Equal(1, calls);
        Assert.Equal(2, server.ProtocolErrors);

        // opting in
        options.AllowMethods("post", "Put");
        Assert.Contains("POST", options.AllowedMethods);
        Assert.Contains("PUT", options.AllowedMethods);
        Assert.Throws<ArgumentException>(() => options.AllowMethods(" "));
        Assert.Throws<ArgumentNullException>(() => options.AllowMethods(null!));

        var empty = new HttpServerOptions().Listen(IPAddress.Loopback, 0);
        empty.AllowedMethods.Clear();
        var noMethods = new HttpServer(empty);
        Assert.Throws<InvalidOperationException>(noMethods.Start);
    }

    [Fact]
    public async Task Bodies_are_refused_unless_a_handler_opts_in()
    {
        var routes = new RouteTable().MapPost("/echo", async (ctx, ct) =>
        {
            var body = await ctx.ReadBodyAsync(ct);
            await ctx.Response.SendAsync(body, ct);
        });
        var options = new HttpServerOptions().Listen(IPAddress.Loopback, 0).AllowMethods("POST").Use(routes);
        await using (var server = new HttpServer(options))
        {
            server.Start();
            using var raw = await RawClient.ConnectAsync(server.BoundEndpoints[0]);
            await raw.SendAsync("POST /echo HTTP/1.1\r\nHost: a\r\nContent-Length: 1\r\n\r\nx");
            Assert.Equal(413, (await raw.ReadResponseAsync()).StatusCode);
            Assert.Equal(1, server.ProtocolErrors);
            using var zero = await RawClient.ConnectAsync(server.BoundEndpoints[0]);
            await zero.SendAsync("POST /echo HTTP/1.1\r\nHost: a\r\nContent-Length: 0\r\n\r\n");
            Assert.Equal(200, (await zero.ReadResponseAsync()).StatusCode);
        }

        // the route table opts in: its budget becomes the server's effective limit
        routes.MaxRequestBodyBytes = 4;
        var optedIn = new HttpServerOptions().Listen(IPAddress.Loopback, 0).AllowMethods("POST").Use(routes);
        await using (var server = new HttpServer(optedIn))
        {
            server.Start();
            using var raw = await RawClient.ConnectAsync(server.BoundEndpoints[0]);
            await raw.SendAsync("POST /echo HTTP/1.1\r\nHost: a\r\nContent-Length: 4\r\n\r\nabcd");
            Assert.Equal("abcd", (await raw.ReadResponseAsync()).Body);
            await raw.SendAsync("POST /echo HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\nabcde");
            Assert.Equal(413, (await raw.ReadResponseAsync()).StatusCode);
        }

        // an invalid opt-in is rejected at start
        routes.MaxRequestBodyBytes = -1;
        var bad = new HttpServer(new HttpServerOptions().Listen(IPAddress.Loopback, 0).Use(routes));
        Assert.Throws<ArgumentOutOfRangeException>(bad.Start);
    }

    [Fact]
    public async Task Per_address_connection_limit_closes_extra_connections()
    {
        await using var host = TestHost.Start(o =>
        {
            o.Limits.MaxConnectionsPerAddress = 2;
            o.Use(new HealthHandler());
        });
        using var c1 = await RawClient.ConnectAsync(host.EndPoint);
        using var c2 = await RawClient.ConnectAsync(host.EndPoint);
        await HttpServerTests.WaitUntilAsync(() => host.Server.ActiveConnections == 2);

        using var c3 = await RawClient.ConnectAsync(host.EndPoint);
        Assert.True(await c3.WaitForCloseAsync(TimeSpan.FromSeconds(5)));
        await HttpServerTests.WaitUntilAsync(() => host.Server.ConnectionsRejected == 1);
        Assert.Equal(2, host.Server.TotalConnectionsAccepted);
        Assert.Equal(2, host.Server.ActiveConnections);

        // the two admitted connections still work
        await c1.SendAsync("GET /healthz HTTP/1.1\r\nHost: a\r\n\r\n");
        Assert.Equal("ok", (await c1.ReadResponseAsync()).Body);

        // closing one frees a slot for the address
        c1.Dispose();
        await HttpServerTests.WaitUntilAsync(() => host.Server.ActiveConnections == 1);
        using var c4 = await RawClient.ConnectAsync(host.EndPoint);
        await c4.SendAsync("GET /healthz HTTP/1.1\r\nHost: a\r\n\r\n");
        Assert.Equal("ok", (await c4.ReadResponseAsync()).Body);
        Assert.Equal(3, host.Server.TotalConnectionsAccepted);
        Assert.Equal(1, host.Server.ConnectionsRejected);
    }

    [Fact]
    public async Task Total_connection_limit_and_per_address_limit_are_independent()
    {
        // per-address rejections must release the global slot, otherwise the accept loop would starve
        await using var host = TestHost.Start(o =>
        {
            o.Limits.MaxConnections = 2;
            o.Limits.MaxConnectionsPerAddress = 1;
            o.Use(new HealthHandler());
        });
        using var c1 = await RawClient.ConnectAsync(host.EndPoint);
        await HttpServerTests.WaitUntilAsync(() => host.Server.ActiveConnections == 1);
        for (int i = 0; i < 5; i++)
        {
            using var extra = await RawClient.ConnectAsync(host.EndPoint);
            Assert.True(await extra.WaitForCloseAsync(TimeSpan.FromSeconds(5)));
        }
        await HttpServerTests.WaitUntilAsync(() => host.Server.ConnectionsRejected == 5);
        await c1.SendAsync("GET /healthz HTTP/1.1\r\nHost: a\r\n\r\n");
        Assert.Equal("ok", (await c1.ReadResponseAsync()).Body);
    }

    [Fact]
    public async Task Default_limits_are_enforced_on_the_wire()
    {
        var options = new HttpServerOptions().Listen(IPAddress.Loopback, 0).Use(new HealthHandler());
        await using var server = new HttpServer(options);
        server.Start();
        var ep = server.BoundEndpoints[0];

        using (var raw = await RawClient.ConnectAsync(ep))
        {
            await raw.SendAsync("GET /" + new string('a', 2100) + " HTTP/1.1\r\nHost: a\r\n\r\n");
            Assert.Equal(414, (await raw.ReadResponseAsync()).StatusCode);
        }
        using (var raw = await RawClient.ConnectAsync(ep))
        {
            var sb = new StringBuilder("GET /healthz HTTP/1.1\r\nHost: a\r\n");
            for (int i = 0; i < 33; i++)
                sb.Append("X-H").Append(i).Append(": v\r\n");
            sb.Append("\r\n");
            await raw.SendAsync(sb.ToString());
            Assert.Equal(431, (await raw.ReadResponseAsync()).StatusCode);
        }
        using (var raw = await RawClient.ConnectAsync(ep))
        {
            await raw.SendAsync("GET /healthz HTTP/1.1\r\nHost: a\r\nX-Big: " + new string('v', 8200) + "\r\n\r\n");
            Assert.Equal(431, (await raw.ReadResponseAsync()).StatusCode);
        }
        using (var raw = await RawClient.ConnectAsync(ep))
        {
            await raw.SendAsync("GET /healthz HTTP/1.1\r\nHost: a\r\n\r\n");
            Assert.Equal("ok", (await raw.ReadResponseAsync()).Body);
        }
        Assert.Equal(3, server.ProtocolErrors);
    }

    [Fact]
    public async Task Fuzzed_requests_never_crash_the_server()
    {
        var errors = new List<Exception>();
        await using var host = TestHost.Start(o =>
        {
            o.OnError = e => { lock (errors) errors.Add(e); };
            o.Limits.HeaderReadTimeout = TimeSpan.FromSeconds(2);
            o.Use(new HealthHandler());
            o.Use(new RouteTable().MapPrefix(null, "/", (ctx, ct) => ctx.Response.SendTextAsync("x", cancellationToken: ct)));
        });
        var random = new Random(12345);
        var seed = Encoding.ASCII.GetBytes("GET /healthz?q=1 HTTP/1.1\r\nHost: example.com\r\nUser-Agent: fuzz\r\nAccept: */*\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        var interesting = new byte[] { 0, (byte)'\r', (byte)'\n', (byte)' ', (byte)'\t', (byte)':', (byte)'%', (byte)'/', 0x7F, 0xFF, (byte)'\\', (byte)'?', (byte)'.' };

        int served = 0, rejected = 0, closed = 0;
        for (int iteration = 0; iteration < 120; iteration++)
        {
            byte[] request;
            if (iteration % 4 == 0)
            {
                request = new byte[random.Next(1, 600)];
                random.NextBytes(request);
            }
            else
            {
                request = (byte[])seed.Clone();
                int mutations = random.Next(1, 6);
                for (int m = 0; m < mutations; m++)
                {
                    int at = random.Next(request.Length);
                    request[at] = random.Next(3) == 0 ? interesting[random.Next(interesting.Length)] : (byte)random.Next(256);
                }
                if (random.Next(5) == 0)
                    request = request[..random.Next(request.Length)];
            }

            using var raw = await RawClient.ConnectAsync(host.EndPoint);
            await raw.SendAsync(request);
            raw.Socket.Shutdown(SocketShutdown.Send);
            var text = await raw.ReadToEndAsync(TimeSpan.FromSeconds(10));
            if (text.Length == 0)
            {
                closed++;
                continue;
            }
            Assert.StartsWith("HTTP/1.1 ", text, StringComparison.Ordinal);
            int status = int.Parse(text.AsSpan(9, 3), System.Globalization.CultureInfo.InvariantCulture);
            Assert.True(status is 200 or (>= 400 and < 600), "unexpected status " + status + " for " + Convert.ToHexString(request));
            if (status == 200)
                served++;
            else
                rejected++;
        }
        await HttpServerTests.WaitUntilAsync(() => host.Server.ActiveConnections == 0);
        Assert.Empty(errors);
        Assert.True(rejected + closed > 0);
        Assert.True(served + rejected + closed == 120);
    }
}
