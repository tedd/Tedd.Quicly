using System.Net;
using System.Net.Sockets;
using Tedd.Quicly.Http.Handlers;

namespace Tedd.Quicly.Http.Tests;

public class ServerLifecycleTests
{
    [Fact]
    public async Task Start_and_stop_state_machine()
    {
        Assert.Throws<ArgumentNullException>(() => new HttpServer(null!));

        var noEndpoints = new HttpServer(new HttpServerOptions());
        Assert.Throws<InvalidOperationException>(noEndpoints.Start);
        await noEndpoints.StopAsync();
        await noEndpoints.DisposeAsync();

        var server = new HttpServer(new HttpServerOptions().Listen(IPAddress.Loopback, 0));
        await server.StopAsync(); // before start: no-op
        Assert.Throws<InvalidOperationException>(server.Start); // already stopped
        await server.DisposeAsync();

        var running = new HttpServer(new HttpServerOptions().Listen(IPAddress.Loopback, 0));
        running.Start();
        Assert.Throws<InvalidOperationException>(running.Start);
        Assert.True(running.IsRunning);
        Assert.False(running.IsStopping);
        var stop1 = running.StopAsync();
        var stop2 = running.StopAsync();
        Assert.Same(stop1, stop2);
        await stop1;
        Assert.True(running.IsStopping);
        Assert.False(running.IsRunning);
        await running.DisposeAsync();
        await running.DisposeAsync();
    }

    [Fact]
    public void Bind_failure_leaves_nothing_bound()
    {
        using var blocker = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        blocker.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        blocker.Listen(1);
        int port = ((IPEndPoint)blocker.LocalEndPoint!).Port;

        var options = new HttpServerOptions().Listen(IPAddress.Loopback, 0).Listen(IPAddress.Loopback, port);
        var server = new HttpServer(options);
        Assert.Throws<SocketException>(server.Start);
        Assert.Empty(server.BoundEndpoints);
        Assert.False(server.IsRunning);
    }

    [Fact]
    public async Task Limits_are_validated()
    {
        static void Check(Action<HttpServerLimits> mutate)
        {
            var options = new HttpServerOptions().Listen(IPAddress.Loopback, 0);
            mutate(options.Limits);
            var server = new HttpServer(options);
            Assert.Throws<ArgumentOutOfRangeException>(server.Start);
        }

        Check(l => l.MaxRequestLineBytes = 1);
        Check(l => l.MaxHeadersBytes = 0);
        Check(l => l.MaxHeaderCount = 0);
        Check(l => l.MaxRequestBodyBytes = -1);
        Check(l => l.MaxRequestBodyBytes = (long)int.MaxValue + 1);
        Check(l => l.MaxConnections = 0);
        Check(l => l.MaxConnectionsPerAddress = 0);
        Check(l => l.MaxRequestsPerConnection = 0);
        Check(l => l.HeaderReadTimeout = TimeSpan.Zero);
        Check(l => l.TlsHandshakeTimeout = TimeSpan.Zero);
        Check(l => l.KeepAliveTimeout = TimeSpan.FromSeconds(-1));
        Check(l => l.RequestBodyReadTimeout = TimeSpan.Zero);
        Check(l => l.ResponseWriteTimeout = TimeSpan.Zero);
        // CancellationTokenSource.CancelAfter refuses anything above uint.MaxValue - 1 ms (~49.7 days)
        Check(l => l.HeaderReadTimeout = TimeSpan.FromDays(60));
        Check(l => l.TlsHandshakeTimeout = TimeSpan.FromDays(60));
        Check(l => l.KeepAliveTimeout = HttpServerLimits.MaxTimeout + TimeSpan.FromMilliseconds(1));
        Check(l => l.RequestBodyReadTimeout = TimeSpan.MaxValue);
        Check(l => l.ResponseWriteTimeout = TimeSpan.FromDays(50));
        // per-connection pre-allocations are bounded
        Check(l => l.MaxRequestLineBytes = HttpServerLimits.MaxHeaderBufferLimit + 1);
        Check(l => l.MaxHeadersBytes = HttpServerLimits.MaxHeaderBufferLimit + 1);
        Check(l => l.MaxHeaderCount = HttpServerLimits.MaxHeaderCountLimit + 1);

        foreach (var shutdown in new[] { TimeSpan.FromSeconds(-2), HttpServerLimits.MaxTimeout + TimeSpan.FromMilliseconds(1) })
        {
            var options = new HttpServerOptions().Listen(IPAddress.Loopback, 0);
            options.ShutdownTimeout = shutdown;
            var ex = Assert.Throws<ArgumentOutOfRangeException>(new HttpServer(options).Start);
            Assert.Equal(nameof(HttpServerOptions.ShutdownTimeout), ex.ParamName);
        }

        var infinite = new HttpServerOptions().Listen(IPAddress.Loopback, 0);
        infinite.Limits.KeepAliveTimeout = Timeout.InfiniteTimeSpan;
        infinite.Limits.HeaderReadTimeout = Timeout.InfiniteTimeSpan;
        infinite.Limits.TlsHandshakeTimeout = Timeout.InfiniteTimeSpan;
        infinite.Limits.RequestBodyReadTimeout = HttpServerLimits.MaxTimeout; // the largest finite value is accepted
        infinite.Limits.MaxRequestLineBytes = HttpServerLimits.MaxHeaderBufferLimit;
        infinite.Limits.MaxHeadersBytes = HttpServerLimits.MaxHeaderBufferLimit;
        infinite.Limits.MaxHeaderCount = HttpServerLimits.MaxHeaderCountLimit;
        infinite.ShutdownTimeout = TimeSpan.Zero;
        var ok = new HttpServer(infinite);
        ok.Start();
        await ok.DisposeAsync();
    }

    [Fact]
    public async Task Graceful_shutdown_waits_for_in_flight_request()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var routes = new RouteTable().MapGet("/slow", async (ctx, ct) =>
        {
            started.SetResult();
            await release.Task;
            await ctx.Response.SendTextAsync("done", cancellationToken: ct);
        });
        var host = TestHost.Start(o => o.Use(routes));
        using var idle = await RawClient.ConnectAsync(host.EndPoint);
        await idle.SendAsync("GET /slow HTTP/1.1\r\nHost: a\r\n\r\n"); // will be in flight
        await started.Task;
        using var idle2 = await RawClient.ConnectAsync(host.EndPoint); // idle: closed immediately on stop
        await HttpServerTests.WaitUntilAsync(() => host.Server.ActiveConnections == 2);

        var stop = host.Server.StopAsync();
        await Task.Delay(200);
        Assert.False(stop.IsCompleted);
        Assert.True(host.Server.IsStopping);
        Assert.True(await idle2.WaitForCloseAsync(TimeSpan.FromSeconds(5)));

        // new connections are refused while stopping
        await Assert.ThrowsAnyAsync<SocketException>(async () =>
        {
            using var late = await RawClient.ConnectAsync(host.EndPoint);
            await late.SendAsync("GET /slow HTTP/1.1\r\nHost: a\r\n\r\n");
            await late.ReadResponseAsync(TimeSpan.FromSeconds(2));
        });

        release.SetResult();
        var response = await idle.ReadResponseAsync();
        Assert.Equal(200, response.StatusCode);
        Assert.Equal("done", response.Body);
        Assert.Equal("close", response["Connection"]);
        await stop;
        Assert.Equal(0, host.Server.ActiveConnections);
        await host.DisposeAsync();
    }

    [Fact]
    public async Task Shutdown_timeout_aborts_stuck_requests()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        bool aborted = false;
        var routes = new RouteTable().MapGet("/stuck", async (ctx, ct) =>
        {
            started.SetResult();
            try
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            }
            catch (OperationCanceledException)
            {
                aborted = ctx.RequestAborted.IsCancellationRequested;
                throw;
            }
        });
        var host = TestHost.Start(o =>
        {
            o.ShutdownTimeout = TimeSpan.FromMilliseconds(300);
            o.Use(routes);
        });
        using var raw = await RawClient.ConnectAsync(host.EndPoint);
        await raw.SendAsync("GET /stuck HTTP/1.1\r\nHost: a\r\n\r\n");
        await started.Task;
        var sw = System.Diagnostics.Stopwatch.StartNew();
        await host.Server.StopAsync();
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(10));
        Assert.True(aborted);
        Assert.True(await raw.WaitForCloseAsync(TimeSpan.FromSeconds(5)));
        await host.DisposeAsync();
    }

    [Fact]
    public async Task Stop_cancellation_token_aborts_immediately()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var routes = new RouteTable().MapGet("/stuck", async (ctx, ct) =>
        {
            started.SetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, ct);
        });
        var host = TestHost.Start(o => o.Use(routes));
        using var raw = await RawClient.ConnectAsync(host.EndPoint);
        await raw.SendAsync("GET /stuck HTTP/1.1\r\nHost: a\r\n\r\n");
        await started.Task;
        using var cts = new CancellationTokenSource(100);
        await host.Server.StopAsync(cts.Token);
        Assert.Equal(0, host.Server.ActiveConnections);
        await host.DisposeAsync();
    }

    [Fact]
    public async Task Connection_limit_pauses_accepting()
    {
        var routes = new RouteTable().MapGet("/", (ctx, ct) => ctx.Response.SendTextAsync("x", cancellationToken: ct));
        await using var host = TestHost.Start(o =>
        {
            o.Limits.MaxConnections = 2;
            o.Use(routes);
        });
        using var c1 = await RawClient.ConnectAsync(host.EndPoint);
        using var c2 = await RawClient.ConnectAsync(host.EndPoint);
        await HttpServerTests.WaitUntilAsync(() => host.Server.ActiveConnections == 2);

        using var c3 = await RawClient.ConnectAsync(host.EndPoint); // sits in the backlog
        await c3.SendAsync("GET / HTTP/1.1\r\nHost: a\r\n\r\n");
        await Assert.ThrowsAsync<OperationCanceledException>(() => c3.ReadResponseAsync(TimeSpan.FromMilliseconds(500)));
        Assert.Equal(2, host.Server.TotalConnectionsAccepted);

        c1.Dispose();
        var response = await c3.ReadResponseAsync();
        Assert.Equal(200, response.StatusCode);
        Assert.Equal(3, host.Server.TotalConnectionsAccepted);
    }

    [Fact]
    public async Task Ipv6_loopback_endpoint()
    {
        if (!Socket.OSSupportsIPv6)
            return;
        var options = new HttpServerOptions().Listen(IPAddress.IPv6Loopback, 0).Use(new HealthHandler());
        await using var server = new HttpServer(options);
        server.Start();
        using var client = new HttpClient();
        Assert.Equal("ok", await client.GetStringAsync("http://[::1]:" + server.BoundEndpoints[0].Port + "/healthz"));
    }

    [Fact]
    public async Task Ipv6_any_is_dual_mode()
    {
        if (!Socket.OSSupportsIPv6)
            return;
        var options = new HttpServerOptions().Listen(IPAddress.IPv6Any, 0).Use(new HealthHandler());
        await using var server = new HttpServer(options);
        server.Start();
        using var client = new HttpClient();
        Assert.Equal("ok", await client.GetStringAsync("http://127.0.0.1:" + server.BoundEndpoints[0].Port + "/healthz"));
    }
}
