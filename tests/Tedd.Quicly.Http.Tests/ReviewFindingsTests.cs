using System.Diagnostics;
using System.Net;
using Tedd.Quicly.Http.Handlers;

namespace Tedd.Quicly.Http.Tests;

/// <summary>
/// Failing tests added by review. Each documents a defect in the current implementation; they are expected to fail
/// until the production code is fixed.
/// </summary>
public class ReviewFindingsTests
{
    /// <summary>Blocks synchronously on "/block" (as a CPU-bound or sync-I/O user handler would); answers "/fast" at once.</summary>
    private sealed class BlockingHandler : IHttpHandler
    {
        public ValueTask<bool> TryHandleAsync(HttpRequestContext context, CancellationToken cancellationToken)
        {
            if (context.Path == "/block")
                Thread.Sleep(3000);
            return Send(context, cancellationToken);
        }

        private static async ValueTask<bool> Send(HttpRequestContext context, CancellationToken ct)
        {
            await context.Response.SendTextAsync(context.Path, cancellationToken: ct);
            return true;
        }
    }

    /// <summary>
    /// The task requires "per-connection processing on the thread pool". <c>HttpConnection.Start()</c> calls
    /// <c>RunAsync()</c> directly from the accept loop, so when the request bytes are already buffered at accept time
    /// the first read completes synchronously and the whole request (TLS handshake / handler included) runs inline on
    /// the accept loop. While it runs, no other connection on that endpoint is accepted.
    /// </summary>
    [Fact]
    public async Task A_handler_running_on_one_connection_does_not_stall_accepting_others()
    {
        await using var host = TestHost.Start(o =>
        {
            o.Limits.MaxConnections = 2;
            o.Use(new BlockingHandler());
        });

        // Fill both slots so the accept loop parks on the slot semaphore.
        var x = await RawClient.ConnectAsync(host.EndPoint);
        using var y = await RawClient.ConnectAsync(host.EndPoint);
        await HttpServerTests.WaitUntilAsync(() => host.Server.ActiveConnections == 2);

        // A's TCP handshake completes in the kernel backlog and its request is buffered before the server accepts it.
        using var a = await RawClient.ConnectAsync(host.EndPoint);
        await a.SendAsync("GET /block HTTP/1.1\r\nHost: t\r\n\r\n");
        await Task.Delay(300);

        // Free a slot: the accept loop accepts A, whose first read now completes synchronously.
        x.Dispose();
        await HttpServerTests.WaitUntilAsync(() => host.Server.TotalConnectionsAccepted == 3);

        // Free another slot and connect B. With per-connection work on the thread pool B is accepted and served at once.
        y.Dispose();
        await HttpServerTests.WaitUntilAsync(() => host.Server.ActiveConnections == 1);
        using var b = await RawClient.ConnectAsync(host.EndPoint);
        await b.SendAsync("GET /fast HTTP/1.1\r\nHost: t\r\n\r\n");
        var sw = Stopwatch.StartNew();
        RawResponse response;
        try
        {
            response = await b.ReadResponseAsync(TimeSpan.FromSeconds(1.5));
        }
        catch (OperationCanceledException)
        {
            Assert.Fail("Connection B was not served within 1.5 s while A's handler blocked: requests run inline on the accept loop instead of on the thread pool.");
            return;
        }
        Assert.Equal("/fast", response.Body);
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(1.5));
    }

    /// <summary>
    /// RFC 9112 §3.2.2: when the request-target is in absolute-form the server MUST ignore the received Host header
    /// and use the target's authority. The implementation keeps the Host header and only falls back to the authority
    /// when Host is absent.
    /// </summary>
    [Fact]
    public async Task Absolute_form_authority_overrides_the_Host_header()
    {
        var routes = new RouteTable().MapGet("/who", (ctx, ct) => ctx.Response.SendTextAsync(ctx.Host ?? "(null)", cancellationToken: ct));
        await using var host = TestHost.Start(o => o.Use(routes));
        using var raw = await RawClient.ConnectAsync(host.EndPoint);
        await raw.SendAsync("GET http://target.example/who HTTP/1.1\r\nHost: header.example\r\n\r\n");
        var r = await raw.ReadResponseAsync();
        Assert.Equal(200, r.StatusCode);
        Assert.Equal("target.example", r.Body);
    }

    /// <summary>
    /// <c>HttpServerLimits.Validate</c> accepts any positive timeout, but every timeout is later passed to
    /// <c>CancellationTokenSource.CancelAfter</c>, which throws for delays above ~49.7 days. A server configured with
    /// such a timeout starts fine and then drops every connection without a response (the exception goes to OnError).
    /// Either <c>Start</c> must reject the value or requests must work.
    /// </summary>
    [Fact]
    public async Task Very_long_timeouts_are_rejected_at_start_or_work()
    {
        var options = new HttpServerOptions().Listen(IPAddress.Loopback, 0).Use(new HealthHandler());
        options.Limits.HeaderReadTimeout = TimeSpan.FromDays(60);
        var errors = new List<Exception>();
        options.OnError = e => { lock (errors) errors.Add(e); };
        await using var server = new HttpServer(options);
        try
        {
            server.Start();
        }
        catch (ArgumentOutOfRangeException)
        {
            return; // rejected up front: acceptable
        }

        using var raw = await RawClient.ConnectAsync(server.BoundEndpoints[0]);
        await raw.SendAsync("GET /healthz HTTP/1.1\r\nHost: a\r\n\r\n");
        RawResponse r;
        try
        {
            r = await raw.ReadResponseAsync(TimeSpan.FromSeconds(5));
        }
        catch (IOException)
        {
            string detail;
            lock (errors)
                detail = errors.Count > 0 ? errors[0].GetType().Name + ": " + errors[0].Message : "no OnError report";
            Assert.Fail("Start accepted HeaderReadTimeout = 60 days, then the connection was dropped without a response (" + detail + ").");
            return;
        }
        Assert.Equal(200, r.StatusCode);
    }
}
