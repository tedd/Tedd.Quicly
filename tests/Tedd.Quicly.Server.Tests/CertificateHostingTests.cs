using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Http;
using Tedd.Quicly.Http.Handlers;
using Tedd.Quicly.Server.Certificates;
using Tedd.Quicly.Testing.Certificates;

namespace Tedd.Quicly.Server.Tests;

public class CertificateHostingTests
{
    private static X509Certificate2 Certificate() => TestCertificates.CreateSelfSigned("CN=game.test", TimeSpan.FromDays(1), true, "game.test");

    [Fact]
    public async Task Static_Certificate_Is_Bound_To_The_Consumers()
    {
        using X509Certificate2 certificate = Certificate();
        List<X509Certificate2> received = [];
        await using ServerFixture f = new(o =>
        {
            o.Certificate = ServerCertificateOptions.Static(certificate);
            o.CertificateConsumers.Add(CertificateConsumers.FromDelegate(received.Add));
            o.CertificateBinding = new CertificateBinderOptions { SupersededCertificateGracePeriod = TimeSpan.Zero };
        });
        Assert.Same(certificate, Assert.Single(received));
        Assert.NotNull(f.Server.Provisioner);
        Assert.Equal(CertificateState.Valid, f.Server.Provisioner!.Status.State);
        Assert.NotNull(f.Server.Binder);
        Assert.Same(certificate, f.Server.Binder!.Certificate);
        Assert.Equal(TimeSpan.Zero, f.Server.Binder.Options.SupersededCertificateGracePeriod);
        await f.Server.StopAsync(TestContext.Current.CancellationToken);
        Assert.Null(f.Server.Provisioner);
        Assert.Null(f.Server.Binder);
    }

    [Fact]
    public async Task A_Failing_Consumer_Is_Reported()
    {
        using X509Certificate2 certificate = Certificate();
        List<CertificateConsumerFailure> failures = [];
        await using ServerFixture f = new(o =>
        {
            o.Certificate = ServerCertificateOptions.Static(certificate);
            o.WaitForCertificate = false;
            o.CertificateConsumers.Add(CertificateConsumers.FromDelegate(_ => throw new InvalidOperationException("consumer")));
        }, start: false);
        f.Server.CertificateConsumerFailed += failures.Add;
        f.Server.CertificateConsumerFailed += _ => throw new InvalidOperationException("handler");
        await f.Server.StartAsync(TestContext.Current.CancellationToken);
        f.Server.PollAll(); // raises the failure the binder found while the server started
        CertificateConsumerFailure failure = Assert.Single(failures);
        Assert.IsType<InvalidOperationException>(failure.Exception);
        f.Server.GetStatistics(out ServerStatistics statistics);
        Assert.Equal(1, statistics.EventHandlerFaults);
    }

    [Fact]
    public async Task A_Failing_Consumer_Without_Handlers_Is_Tolerated()
    {
        using X509Certificate2 certificate = Certificate();
        await using ServerFixture f = new(o =>
        {
            o.Certificate = ServerCertificateOptions.Static(certificate);
            o.CertificateConsumers.Add(CertificateConsumers.FromDelegate(_ => throw new InvalidOperationException("consumer")));
        });
        Assert.True(f.Server.IsRunning);
        Assert.Equal(1, f.Server.Binder!.RetainedCertificateCount + 1);
    }

    [Fact]
    public async Task Http_Side_Endpoint_Serves_Health_And_Redirects_To_Https()
    {
        await using ServerFixture f = new(o =>
        {
            o.ListenEndPoint = new IPEndPoint(IPAddress.IPv6Any, 4433);
            o.Http = new ServerHttpOptions { EndPoint = new IPEndPoint(IPAddress.Loopback, 0), EnableHealthEndpoint = true };
            o.Http.Handlers.Add(new RouteTable().MapGet("/version", async (context, token) => await context.Response.SendTextAsync("1.0", cancellationToken: token)));
            o.Http.Configure = http => http.AddServerHeader = false;
        });
        IPEndPoint endPoint = f.Server.HttpEndPoint!;
        CancellationToken cancel = TestContext.Current.CancellationToken;
        using HttpClient client = new(new SocketsHttpHandler { AllowAutoRedirect = false });
        string root = "http://127.0.0.1:" + endPoint.Port;

        using HttpResponseMessage health = await client.GetAsync(root + "/healthz", cancel);
        Assert.Equal(HttpStatusCode.OK, health.StatusCode);
        Assert.Equal("ok", await health.Content.ReadAsStringAsync(cancel));
        Assert.False(health.Headers.Contains("Server"));

        using HttpResponseMessage version = await client.GetAsync(root + "/version", cancel);
        Assert.Equal("1.0", await version.Content.ReadAsStringAsync(cancel));

        using HttpResponseMessage redirect = await client.GetAsync(root + "/play?x=1", cancel);
        Assert.True((int)redirect.StatusCode is 301 or 308, redirect.StatusCode.ToString());
        Assert.Equal("https://127.0.0.1:4433/play?x=1", redirect.Headers.Location!.OriginalString);

        await f.Server.StopAsync(cancel);
        Assert.Null(f.Server.HttpEndPoint);
    }

    [Fact]
    public async Task Redirect_Port_Can_Be_Set_And_The_Redirect_Turned_Off()
    {
        await using ServerFixture explicitPort = new(o => o.Http = new ServerHttpOptions
        {
            EndPoint = new IPEndPoint(IPAddress.Loopback, 0),
            HttpsPort = 8443,
        });
        await using ServerFixture ephemeral = new(o =>
        {
            o.ListenEndPoint = new IPEndPoint(IPAddress.Loopback, 0);
            o.Http = new ServerHttpOptions { EndPoint = new IPEndPoint(IPAddress.Loopback, 0) };
        });
        await using ServerFixture off = new(o => o.Http = new ServerHttpOptions
        {
            EndPoint = new IPEndPoint(IPAddress.Loopback, 0),
            RedirectToHttps = false,
        });
        CancellationToken cancel = TestContext.Current.CancellationToken;
        using HttpClient client = new(new SocketsHttpHandler { AllowAutoRedirect = false });

        using HttpResponseMessage toExplicit = await client.GetAsync("http://127.0.0.1:" + explicitPort.Server.HttpEndPoint!.Port + "/a", cancel);
        Assert.Equal("https://127.0.0.1:8443/a", toExplicit.Headers.Location!.OriginalString);

        using HttpResponseMessage toDefault = await client.GetAsync("http://127.0.0.1:" + ephemeral.Server.HttpEndPoint!.Port + "/b", cancel);
        Assert.Equal("https://127.0.0.1/b", toDefault.Headers.Location!.OriginalString);

        using HttpResponseMessage notFound = await client.GetAsync("http://127.0.0.1:" + off.Server.HttpEndPoint!.Port + "/c", cancel);
        Assert.Equal(HttpStatusCode.NotFound, notFound.StatusCode);
    }

    [Fact]
    public async Task A_Failed_Start_Stops_What_Was_Started()
    {
        using TcpListener blocker = new(IPAddress.Loopback, 0);
        blocker.Start();
        int port = ((IPEndPoint)blocker.LocalEndpoint).Port;
        using X509Certificate2 certificate = Certificate();
        await using ServerFixture f = new(o =>
        {
            o.Certificate = ServerCertificateOptions.Static(certificate);
            o.Http = new ServerHttpOptions { EndPoint = new IPEndPoint(IPAddress.Loopback, port) };
        }, start: false);
        await Assert.ThrowsAnyAsync<SocketException>(() => f.Server.StartAsync(TestContext.Current.CancellationToken));
        Assert.False(f.Server.IsRunning);
        Assert.Null(f.Server.Provisioner);
        Assert.Null(f.Server.Binder);
        Assert.Null(f.Server.HttpEndPoint);
    }

    [Fact]
    public async Task A_Canceled_Start_Stops_What_Was_Started()
    {
        using X509Certificate2 certificate = Certificate();
        await using ServerFixture f = new(o => o.Certificate = ServerCertificateOptions.Static(certificate), start: false);
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => f.Server.StartAsync(new CancellationToken(true)));
        Assert.False(f.Server.IsRunning);
        Assert.Null(f.Server.Provisioner);
    }
}
