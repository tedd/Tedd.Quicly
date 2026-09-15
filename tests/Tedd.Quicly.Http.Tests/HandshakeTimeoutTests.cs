using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Http.Handlers;
using Tedd.Quicly.Http.Tls;

namespace Tedd.Quicly.Http.Tests;

/// <summary>
/// The TLS handshake has its own budget (<see cref="HttpServerLimits.TlsHandshakeTimeout"/>, Kestrel's 10 s by default), the
/// request's <see cref="HttpServerLimits.HeaderReadTimeout"/> starts after it, a certificate's credential is built once per
/// certificate (offline: no handshake waits for a download) and its intermediates travel with it.
/// </summary>
public class HandshakeTimeoutTests
{
    private static RouteTable Routes() => new RouteTable().MapGet("/", (ctx, ct) => ctx.Response.SendTextAsync("ok", cancellationToken: ct));

    [Fact]
    public async Task A_slow_handshake_within_the_handshake_timeout_still_serves_the_request()
    {
        using X509Certificate2 cert = TestCertificates.CreateSelfSigned("localhost", "localhost");
        await using var host = TestHost.Start(o =>
        {
            o.Limits.HeaderReadTimeout = TimeSpan.FromMilliseconds(400); // shorter than the handshake takes
            o.Limits.TlsHandshakeTimeout = TimeSpan.FromSeconds(30);
            o.Use(Routes());
        }, new HttpTlsOptions { CertificateSelector = new SlowSelector(cert, TimeSpan.FromMilliseconds(800)) });

        using var client = host.CreateClient();
        Assert.Equal("ok", await client.GetStringAsync("/"));
        Assert.Equal(0, host.Server.HandshakeFailures);
        Assert.Equal(0, host.Server.HandshakeTimeouts);
        Assert.Equal(0, host.Server.RequestTimeouts);
    }

    [Fact]
    public async Task A_handshake_beyond_the_handshake_timeout_is_counted_and_reported()
    {
        using X509Certificate2 cert = TestCertificates.CreateSelfSigned("localhost", "localhost");
        List<Exception> errors = [];
        await using var host = TestHost.Start(o =>
        {
            o.OnError = e =>
            {
                lock (errors)
                {
                    errors.Add(e);
                }
            };
            o.Limits.TlsHandshakeTimeout = TimeSpan.FromMilliseconds(200);
            o.Use(Routes());
        }, new HttpTlsOptions { CertificateSelector = new SlowSelector(cert, TimeSpan.FromSeconds(1)) });

        using var client = host.CreateClient();
        await Assert.ThrowsAnyAsync<Exception>(() => client.GetStringAsync("/"));
        await HttpServerTests.WaitUntilAsync(() => host.Server.HandshakeTimeouts == 1);
        Assert.Equal(1, host.Server.HandshakeFailures); // a timeout is one of them
        await HttpServerTests.WaitUntilAsync(() =>
        {
            lock (errors)
            {
                return errors.Count == 1;
            }
        });
        lock (errors)
        {
            Assert.IsType<TimeoutException>(errors[0]);
        }
    }

    [Fact]
    public async Task A_certificate_credential_is_built_once_and_again_after_a_swap()
    {
        using X509Certificate2 first = TestCertificates.CreateSelfSigned("first", "localhost");
        using X509Certificate2 second = TestCertificates.CreateSelfSigned("second", "localhost");
        StaticCertificateSource source = new(first);
        HttpTlsOptions tls = HttpTlsOptions.FromSource(source);
        await using var host = TestHost.Start(o => o.Use(Routes()), tls);

        for (int i = 0; i < 2; i++)
        {
            using var client = host.CreateClient();
            Assert.Equal("ok", await client.GetStringAsync("/"));
        }

        Assert.Equal(1, tls.CertificateContextBuilds); // one credential served both handshakes

        source.Update(second);
        using (var client = host.CreateClient())
        {
            Assert.Equal("ok", await client.GetStringAsync("/"));
        }

        Assert.Equal(2, tls.CertificateContextBuilds); // the swap is served by a new one
    }

    [Fact]
    public async Task The_intermediates_of_a_certificate_are_sent_with_it()
    {
        using ECDsa rootKey = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        CertificateRequest rootRequest = new("CN=Quicly Test Root", rootKey, HashAlgorithmName.SHA256);
        rootRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        rootRequest.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(rootRequest.PublicKey, false));
        DateTimeOffset now = DateTimeOffset.UtcNow;
        using X509Certificate2 root = rootRequest.CreateSelfSigned(now.AddDays(-1), now.AddDays(1));
        using ECDsa leafKey = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        CertificateRequest leafRequest = new("CN=localhost", leafKey, HashAlgorithmName.SHA256);
        SubjectAlternativeNameBuilder san = new();
        san.AddDnsName("localhost");
        leafRequest.CertificateExtensions.Add(san.Build());
        leafRequest.CertificateExtensions.Add(X509AuthorityKeyIdentifierExtension.CreateFromCertificate(root, true, false));
        using X509Certificate2 issued = leafRequest.Create(root, now.AddMinutes(-5), now.AddDays(1), RandomNumberGenerator.GetBytes(8));
        using X509Certificate2 ephemeral = issued.CopyWithPrivateKey(leafKey);
        // Through a PKCS#12 round trip: SChannel cannot sign with an ephemeral key.
        using X509Certificate2 leaf = X509CertificateLoader.LoadPkcs12(ephemeral.Export(X509ContentType.Pkcs12), null, X509KeyStorageFlags.DefaultKeySet);

        HttpTlsOptions tls = HttpTlsOptions.FromCertificate(leaf);
        tls.AdditionalCertificates = [root]; // the issuer is in no store, and a handshake never fetches it
        await using var host = TestHost.Start(o => o.Use(Routes()), tls);

        List<string> received = await ChainSubjectsAsync(host.EndPoint);
        Assert.Contains("CN=localhost", received);
        Assert.Contains("CN=Quicly Test Root", received);
    }

    /// <summary>The subjects of the chain a client can build from what the server sent.</summary>
    private static async Task<List<string>> ChainSubjectsAsync(IPEndPoint endPoint)
    {
        List<string> subjects = [];
        using Socket socket = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        await socket.ConnectAsync(endPoint);
        using SslStream ssl = new(new NetworkStream(socket, ownsSocket: true), leaveInnerStreamOpen: false);
        await ssl.AuthenticateAsClientAsync(new SslClientAuthenticationOptions
        {
            TargetHost = "localhost",
            RemoteCertificateValidationCallback = (_, _, chain, _) =>
            {
                if (chain is not null)
                {
                    foreach (X509ChainElement element in chain.ChainElements)
                    {
                        subjects.Add(element.Certificate.Subject);
                    }
                }

                return true;
            },
        });
        return subjects;
    }

    /// <summary>Stands in for a slow credential setup inside the handshake (a chain build, a key read on a loaded machine).</summary>
    private sealed class SlowSelector(X509Certificate2 certificate, TimeSpan delay) : ICertificateSelector
    {
        public X509Certificate2? SelectCertificate(string? serverName)
        {
            Thread.Sleep(delay);
            return certificate;
        }
    }
}
