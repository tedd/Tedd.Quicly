using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Acme;
using Tedd.Quicly.Http.Handlers;
using Tedd.Quicly.Http.Tls;

namespace Tedd.Quicly.Http.Tests;

public class TlsTests
{
    private static RouteTable Routes() => new RouteTable().MapGet("/", (ctx, ct) => ctx.Response.SendTextAsync("secure=" + ctx.IsSecure + ";sni=" + ctx.TlsServerName, cancellationToken: ct));

    private static async Task<(SslStream Stream, Socket Socket)> ConnectTlsAsync(IPEndPoint endPoint, string targetHost, params string[] alpn)
    {
        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        await socket.ConnectAsync(endPoint);
        var ssl = new SslStream(new NetworkStream(socket, ownsSocket: true), leaveInnerStreamOpen: false);
        var options = new SslClientAuthenticationOptions
        {
            TargetHost = targetHost,
            RemoteCertificateValidationCallback = static (_, _, _, _) => true,
        };
        if (alpn.Length > 0)
        {
            options.ApplicationProtocols = [];
            foreach (var p in alpn)
                options.ApplicationProtocols.Add(new SslApplicationProtocol(p));
        }
        await ssl.AuthenticateAsClientAsync(options);
        return (ssl, socket);
    }

    [Fact]
    public async Task Https_with_self_signed_certificate_and_custom_validation()
    {
        using var cert = TestCertificates.CreateSelfSigned("localhost", "localhost");
        X509Certificate2? seen = null;
        await using var host = TestHost.Start(o => o.Use(Routes()), HttpTlsOptions.FromCertificate(cert));
        using var client = host.CreateClient(h => h.SslOptions.RemoteCertificateValidationCallback = (_, c, _, _) =>
        {
            seen = c as X509Certificate2 ?? (c is null ? null : new X509Certificate2(c));
            return true;
        });
        Assert.Equal("secure=True;sni=", await client.GetStringAsync("/"));
        Assert.NotNull(seen);
        Assert.Equal(cert.Thumbprint, seen!.Thumbprint);
        Assert.Equal(1, host.Server.TotalConnectionsAccepted);
        // keep-alive over TLS
        Assert.Equal("secure=True;sni=", await client.GetStringAsync("/"));
        Assert.Equal(1, host.Server.TotalConnectionsAccepted);
    }

    [Fact]
    public async Task Sni_selects_certificate()
    {
        using var certA = TestCertificates.CreateSelfSigned("a.example", "a.example");
        using var certWild = TestCertificates.CreateSelfSigned("*.wild.example", "*.wild.example");
        using var certDefault = TestCertificates.CreateSelfSigned("default", "default.example");
        var selector = new SniCertificateSelector(new StaticCertificateSource(certDefault))
            .Add("a.example", certA)
            .Add("*.wild.example", new StaticCertificateSource(certWild));
        Assert.Equal(2, selector.Count);
        var tls = new HttpTlsOptions { CertificateSelector = selector };
        await using var host = TestHost.Start(o => o.Use(Routes()), tls);

        async Task<(string Thumbprint, string Body)> Get(string sniHost)
        {
            X509Certificate2? seen = null;
            using var client = host.CreateClient(h =>
            {
                h.SslOptions.RemoteCertificateValidationCallback = (_, c, _, _) =>
                {
                    seen = new X509Certificate2(c!);
                    return true;
                };
                h.ConnectCallback = async (_, ct) =>
                {
                    var s = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
                    await s.ConnectAsync(host.EndPoint, ct);
                    return new NetworkStream(s, ownsSocket: true);
                };
            });
            var body = await client.GetStringAsync("https://" + sniHost + "/");
            return (seen!.Thumbprint, body);
        }

        var a = await Get("a.example");
        Assert.Equal(certA.Thumbprint, a.Thumbprint);
        Assert.Equal("secure=True;sni=a.example", a.Body);
        Assert.Equal(certWild.Thumbprint, (await Get("sub.wild.example")).Thumbprint);
        Assert.Equal(certDefault.Thumbprint, (await Get("unknown.example")).Thumbprint);
        Assert.Equal(certDefault.Thumbprint, (await Get("wild.example")).Thumbprint);

        // selector unit behaviour
        Assert.Same(certDefault, selector.SelectCertificate(null));
        Assert.Same(certDefault, selector.SelectCertificate(""));
        Assert.Same(certDefault, selector.SelectCertificate("x."));
        Assert.True(selector.Remove("a.example"));
        Assert.False(selector.Remove("a.example"));
        Assert.Same(certDefault, selector.SelectCertificate("a.example"));
        selector.Add("empty.example", new StaticCertificateSource());
        Assert.Same(certDefault, selector.SelectCertificate("empty.example"));
        selector.Add("*.empty.example", new StaticCertificateSource());
        Assert.Same(certDefault, selector.SelectCertificate("x.empty.example"));
        selector.Default = null;
        Assert.Null(selector.SelectCertificate("nothing"));
        Assert.Null(selector.SelectCertificate("empty.example"));
        Assert.Throws<ArgumentException>(() => selector.Add("", certA));
        Assert.Throws<ArgumentNullException>(() => selector.Add("h", (ICertificateSource)null!));
    }

    [Fact]
    public async Task Alpn_acme_tls1_selects_challenge_certificate_only_when_offered()
    {
        using var normal = TestCertificates.CreateSelfSigned("site.example", "site.example");
        using var challenge = TestCertificates.CreateAcmeChallenge("site.example", "token.thumb");
        var responder = new TlsAlpn01Responder();
        var tls = new HttpTlsOptions { CertificateSource = new StaticCertificateSource(normal), Alpn01Responder = responder };
        await using var host = TestHost.Start(o => o.Use(Routes()), tls);

        // Before publishing: a client that only offers acme-tls/1 cannot negotiate.
        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            var (ssl, _) = await ConnectTlsAsync(host.EndPoint, "site.example", "acme-tls/1");
            ssl.Dispose();
        });

        ITlsAlpn01Responder r = responder;
        await r.PublishAsync("site.example", challenge, CancellationToken.None);
        Assert.Equal(1, responder.Count);
        Assert.True(responder.TryGetCertificate("SITE.example", out var found));
        Assert.Same(challenge, found);
        Assert.False(responder.TryGetCertificate("other.example", out _));
        Assert.False(responder.TryGetCertificate(null, out _));

        // Validation client: ALPN acme-tls/1 + SNI -> challenge certificate with the acmeIdentifier extension.
        {
            var (ssl, _) = await ConnectTlsAsync(host.EndPoint, "site.example", "acme-tls/1");
            using (ssl)
            {
                Assert.Equal(TlsAlpn01Responder.AcmeTls1, ssl.NegotiatedApplicationProtocol);
                var remote = new X509Certificate2(ssl.RemoteCertificate!);
                Assert.Equal(challenge.Thumbprint, remote.Thumbprint);
                var ext = remote.Extensions[TlsAlpn01Responder.AcmeIdentifierOid];
                Assert.NotNull(ext);
                Assert.True(ext!.Critical);
                Assert.Equal("site.example", remote.GetNameInfo(X509NameType.DnsName, false));
                // the server closes right after the handshake
                var buffer = new byte[16];
                Assert.Equal(0, await ssl.ReadAsync(buffer));
            }
        }
        await HttpServerTests.WaitUntilAsync(() => host.Server.AcmeTlsAlpnHandshakes == 1);

        // A regular client (http/1.1 ALPN) still gets the normal certificate and a working HTTP session.
        {
            var (ssl, _) = await ConnectTlsAsync(host.EndPoint, "site.example", "http/1.1");
            using (ssl)
            {
                Assert.Equal(SslApplicationProtocol.Http11, ssl.NegotiatedApplicationProtocol);
                Assert.Equal(normal.Thumbprint, new X509Certificate2(ssl.RemoteCertificate!).Thumbprint);
                await ssl.WriteAsync("GET / HTTP/1.1\r\nHost: site.example\r\n\r\n"u8.ToArray());
                var buffer = new byte[1024];
                int n = await ssl.ReadAsync(buffer);
                Assert.Contains("secure=True;sni=site.example", System.Text.Encoding.ASCII.GetString(buffer, 0, n), StringComparison.Ordinal);
            }
        }

        // Client offering both: prefers acme only for validation; here it gets the challenge certificate because acme-tls/1 is offered.
        {
            var (ssl, _) = await ConnectTlsAsync(host.EndPoint, "site.example", "acme-tls/1", "http/1.1");
            using (ssl)
                Assert.Equal(challenge.Thumbprint, new X509Certificate2(ssl.RemoteCertificate!).Thumbprint);
        }

        // Different SNI: no challenge published -> normal certificate even with acme-tls/1 offered alongside http/1.1.
        {
            var (ssl, _) = await ConnectTlsAsync(host.EndPoint, "other.example", "acme-tls/1", "http/1.1");
            using (ssl)
                Assert.Equal(normal.Thumbprint, new X509Certificate2(ssl.RemoteCertificate!).Thumbprint);
        }

        // No ALPN at all: normal certificate.
        {
            var (ssl, _) = await ConnectTlsAsync(host.EndPoint, "site.example");
            using (ssl)
                Assert.Equal(normal.Thumbprint, new X509Certificate2(ssl.RemoteCertificate!).Thumbprint);
        }

        await r.RemoveAsync("site.example", CancellationToken.None);
        Assert.Equal(0, responder.Count);
        await Assert.ThrowsAsync<ArgumentException>(async () => await responder.PublishAsync("", challenge, CancellationToken.None));
        await Assert.ThrowsAsync<ArgumentNullException>(async () => await responder.PublishAsync("x", null!, CancellationToken.None));
        await Assert.ThrowsAsync<ArgumentException>(async () => await responder.RemoveAsync("", CancellationToken.None));
    }

    [Fact]
    public async Task Certificate_hot_swap_without_restart()
    {
        using var first = TestCertificates.CreateSelfSigned("first", "localhost");
        using var second = TestCertificates.CreateSelfSigned("second", "localhost");
        var source = new StaticCertificateSource(first);
        X509Certificate2? changed = null;
        source.Changed += c => changed = c;
        await using var host = TestHost.Start(o => o.Use(Routes()), HttpTlsOptions.FromSource(source));

        async Task<string> Thumbprint()
        {
            var (ssl, _) = await ConnectTlsAsync(host.EndPoint, "localhost");
            using (ssl)
                return new X509Certificate2(ssl.RemoteCertificate!).Thumbprint;
        }

        Assert.Equal(first.Thumbprint, await Thumbprint());
        source.Update(second);
        Assert.Same(second, changed);
        Assert.Same(second, source.Current);
        Assert.Equal(second.Thumbprint, await Thumbprint());
        Assert.Throws<ArgumentNullException>(() => source.Update(null!));
    }

    [Fact]
    public async Task Certificate_provider_delegate_is_used()
    {
        using var cert = TestCertificates.CreateSelfSigned("prov", "localhost");
        int calls = 0;
        var tls = new HttpTlsOptions { CertificateProvider = () => { calls++; return cert; } };
        Assert.Same(cert, tls.ResolveCertificate("anything"));
        await using var host = TestHost.Start(o => o.Use(Routes()), tls);
        using var client = host.CreateClient();
        Assert.Equal("secure=True;sni=", await client.GetStringAsync("/"));
        Assert.True(calls >= 2);
    }

    [Fact]
    public void Resolution_order_and_defaults()
    {
        using var a = TestCertificates.CreateSelfSigned("a", "a");
        using var b = TestCertificates.CreateSelfSigned("b", "b");
        using var c = TestCertificates.CreateSelfSigned("c", "c");
        var tls = new HttpTlsOptions();
        Assert.Null(tls.ResolveCertificate(null));
        Assert.Equal([SslApplicationProtocol.Http11], tls.ApplicationProtocols);
        Assert.Equal(SslProtocols.None, tls.EnabledSslProtocols);
        Assert.Null(tls.Alpn01Responder);
        tls.CertificateSource = new StaticCertificateSource(c);
        Assert.Same(c, tls.ResolveCertificate("x"));
        tls.CertificateProvider = () => b;
        Assert.Same(b, tls.ResolveCertificate("x"));
        tls.CertificateSelector = new SniCertificateSelector().Add("a", a);
        Assert.Same(a, tls.ResolveCertificate("a"));
        Assert.Same(b, tls.ResolveCertificate("other"));
        tls.CertificateProvider = () => null;
        Assert.Same(c, tls.ResolveCertificate("other"));
        Assert.Same(tls.SelectionCallback, tls.SelectionCallback);
        Assert.Same(a, tls.SelectionCallback(this, "a"));
        var empty = new HttpTlsOptions();
        Assert.Throws<InvalidOperationException>(() => empty.SelectionCallback(this, "nope"));
    }

    [Fact]
    public async Task Missing_certificate_at_handshake_is_reported_and_connection_closed()
    {
        var errors = new List<Exception>();
        var tls = new HttpTlsOptions { CertificateProvider = () => null };
        await using var host = TestHost.Start(o =>
        {
            o.OnError = e => { lock (errors) errors.Add(e); };
            o.Use(Routes());
        }, tls);
        await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            var (ssl, _) = await ConnectTlsAsync(host.EndPoint, "localhost");
            ssl.Dispose();
        });
        await HttpServerTests.WaitUntilAsync(() => host.Server.ActiveConnections == 0);
    }

    [Fact]
    public async Task Non_tls_bytes_and_silent_clients_are_closed()
    {
        using var cert = TestCertificates.CreateSelfSigned("localhost", "localhost");
        await using var host = TestHost.Start(o =>
        {
            o.Limits.HeaderReadTimeout = TimeSpan.FromMilliseconds(500);
            o.Use(Routes());
        }, HttpTlsOptions.FromCertificate(cert));

        using (var raw = await RawClient.ConnectAsync(host.EndPoint))
        {
            await raw.SendAsync("GET / HTTP/1.1\r\nHost: a\r\n\r\n");
            Assert.True(await raw.WaitForCloseAsync(TimeSpan.FromSeconds(10)));
        }
        using (var raw = await RawClient.ConnectAsync(host.EndPoint))
        {
            // TLS record header announcing a record that never arrives -> handshake timeout
            await raw.SendAsync([0x16, 0x03, 0x01, 0x10, 0x00, 0x01]);
            Assert.True(await raw.WaitForCloseAsync(TimeSpan.FromSeconds(10)));
        }
        using (var raw = await RawClient.ConnectAsync(host.EndPoint))
        {
            // nothing at all
            Assert.True(await raw.WaitForCloseAsync(TimeSpan.FromSeconds(10)));
        }
        using (var raw = await RawClient.ConnectAsync(host.EndPoint))
        {
            // a full but bogus handshake record (unparseable ClientHello) still goes to SslStream and fails
            var record = new byte[5 + 64];
            record[0] = 0x16; record[1] = 0x03; record[2] = 0x01; record[3] = 0; record[4] = 64;
            record[5] = 0x01;
            await raw.SendAsync(record);
            Assert.True(await raw.WaitForCloseAsync(TimeSpan.FromSeconds(10)));
        }
        await HttpServerTests.WaitUntilAsync(() => host.Server.ActiveConnections == 0);
        Assert.Equal(4, host.Server.HandshakeFailures);
        Assert.Equal(0, host.Server.RequestTimeouts);
    }

    [Fact]
    public void Start_requires_certificate_configuration()
    {
        var options = new HttpServerOptions().Listen(IPAddress.Loopback, 0, new HttpTlsOptions());
        var server = new HttpServer(options);
        Assert.Throws<InvalidOperationException>(server.Start);
        Assert.False(server.IsRunning);
        Assert.Empty(server.BoundEndpoints);
    }

    [Fact]
    public async Task File_certificate_source_loads_and_reloads()
    {
        using var first = TestCertificates.CreateSelfSigned("file1", "localhost");
        using var second = TestCertificates.CreateSelfSigned("file2", "localhost");
        var path = Path.Combine(Path.GetTempPath(), "quicly-cert-" + Guid.NewGuid().ToString("N") + ".pfx");
        try
        {
            File.WriteAllBytes(path, TestCertificates.ExportPfx(first, "pw"));
            using var source = new FileCertificateSource(path, "pw");
            Assert.Equal(Path.GetFullPath(path), source.FilePath);
            Assert.Equal(first.Thumbprint, source.Current!.Thumbprint);
            Assert.True(source.Current.HasPrivateKey);
            Assert.False(source.Reload());

            var changes = new List<X509Certificate2>();
            source.Changed += changes.Add;
            File.WriteAllBytes(path, TestCertificates.ExportPfx(second, "pw"));
            Assert.True(source.Reload());
            Assert.Equal(second.Thumbprint, source.Current!.Thumbprint);
            Assert.Single(changes);

            // serves over TLS
            await using var host = TestHost.Start(o => o.Use(Routes()), HttpTlsOptions.FromSource(source));
            var (ssl, _) = await ConnectTlsAsync(host.EndPoint, "localhost");
            using (ssl)
                Assert.Equal(second.Thumbprint, new X509Certificate2(ssl.RemoteCertificate!).Thumbprint);

            // wrong password
            Assert.ThrowsAny<CryptographicException>(() => new FileCertificateSource(path, "wrong"));
            Assert.Throws<ArgumentException>(() => new FileCertificateSource(""));
            Assert.Throws<ArgumentOutOfRangeException>(() => new FileCertificateSource(path, "pw", TimeSpan.Zero));
            Assert.ThrowsAny<IOException>(() => new FileCertificateSource(path + ".missing"));

            source.Dispose();
            Assert.Throws<ObjectDisposedException>(() => source.Reload());
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task File_certificate_source_polls_for_changes()
    {
        using var first = TestCertificates.CreateSelfSigned("poll1", "localhost");
        using var second = TestCertificates.CreateSelfSigned("poll2", "localhost");
        var path = Path.Combine(Path.GetTempPath(), "quicly-cert-" + Guid.NewGuid().ToString("N") + ".pfx");
        try
        {
            File.WriteAllBytes(path, TestCertificates.ExportPfx(first, null));
            File.SetLastWriteTimeUtc(path, DateTime.UtcNow.AddMinutes(-5));
            using var source = new FileCertificateSource(path, null, TimeSpan.FromMilliseconds(50));
            var failures = new List<Exception>();
            source.ReloadFailed += failures.Add;
            Assert.Equal(first.Thumbprint, source.Current!.Thumbprint);

            // unchanged timestamp: no reload
            await Task.Delay(200);
            Assert.Equal(first.Thumbprint, source.Current!.Thumbprint);

            // corrupt file with a new timestamp: reload fails, old certificate stays
            File.WriteAllBytes(path, [1, 2, 3]);
            File.SetLastWriteTimeUtc(path, DateTime.UtcNow.AddMinutes(-4));
            await HttpServerTests.WaitUntilAsync(() => failures.Count > 0);
            Assert.Equal(first.Thumbprint, source.Current!.Thumbprint);

            // valid new file: picked up
            File.WriteAllBytes(path, TestCertificates.ExportPfx(second, null));
            File.SetLastWriteTimeUtc(path, DateTime.UtcNow.AddMinutes(-3));
            await HttpServerTests.WaitUntilAsync(() => source.Current!.Thumbprint == second.Thumbprint);

            source.Dispose();
            source.Dispose();
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public async Task Alpn_can_be_disabled()
    {
        using var cert = TestCertificates.CreateSelfSigned("noalpn", "localhost");
        var tls = HttpTlsOptions.FromCertificate(cert);
        tls.ApplicationProtocols.Clear();
        await using var host = TestHost.Start(o => o.Use(Routes()), tls);
        var (ssl, _) = await ConnectTlsAsync(host.EndPoint, "localhost", "http/1.1");
        using (ssl)
        {
            Assert.Equal(default, ssl.NegotiatedApplicationProtocol);
            await ssl.WriteAsync("GET / HTTP/1.1\r\nHost: h\r\n\r\n"u8.ToArray());
            var buffer = new byte[512];
            int n = await ssl.ReadAsync(buffer);
            Assert.StartsWith("HTTP/1.1 200", System.Text.Encoding.ASCII.GetString(buffer, 0, n), StringComparison.Ordinal);
        }
    }
}
