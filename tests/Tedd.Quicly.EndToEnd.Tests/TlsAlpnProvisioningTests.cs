using System.Collections.Concurrent;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Acme.Models;
using Tedd.Quicly.EndToEnd.Tests.Infrastructure;
using Tedd.Quicly.Server.Certificates;
using Tedd.Quicly.Testing.Acme;

namespace Tedd.Quicly.EndToEnd.Tests;

/// <summary>
/// Scenario 4, the tls-alpn-01 variant: the same provisioning flow with the TLS-ALPN challenge opted in (and preferred),
/// while the HTTP endpoint also serves /healthz. The fake CA validates through a transparent relay, so at the very moment the
/// CA validates, the test handshakes with the TLS endpoint itself: the ClientHello peek must select the challenge
/// certificate only for ALPN acme-tls/1, and the certificate being served (or, before the first one, nothing) otherwise.
/// </summary>
[Collection(EndToEndCollection.Name)]
public sealed class TlsAlpnProvisioningTests
{
    [Fact(Timeout = E2eTimeouts.TestMilliseconds)]
    public async Task TlsAlpn01_PresentsTheChallengeCertificateOnlyForAcmeTls1_WhileHttpServesHealth_AndQuicServesTheIssuedCertificate()
    {
        Requires.MsQuic();
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using TestBed bed = new();
        FakeAcmeServer ca = bed.StartCa();

        // tls-alpn-01 is opt-in; list it first so it is preferred, and keep http-01 so that the HTTP endpoint runs.
        AcmeProvisioningOptions options = bed.AcmeOptions(ca, AcmeChallengeKind.TlsAlpn01, AcmeChallengeKind.Http01);
        options.EnableHealthEndpoint = true;
        CertificateProvisioner provisioner = bed.CreateProvisioner(ca, options, wireTlsAlpnPort: false);
        StatusRecorder status = new(provisioner);

        // The CA's tls-alpn-01 check goes through the relay; each time it connects, the TLS endpoint is probed first.
        ConcurrentQueue<ChallengeWindow> windows = new();
        ValidationProxy proxy = bed.StartProxy(() => provisioner.TlsEndPoint, async _ => windows.Enqueue(await ChallengeWindow.ProbeAsync(provisioner)));
        ca.TlsAlpnValidationPort = proxy.Port;
        try
        {
            await provisioner.StartAsync(ct).Within(E2eTimeouts.Order, "the provisioner to obtain its first certificate over tls-alpn-01");
            Assert.True(provisioner.Status.State == CertificateState.Valid, status.Describe());
            X509Certificate2 first = provisioner.Current!;
            string firstThumbprint = first.Thumbprint;

            // The CA validated with tls-alpn-01, through the relay.
            FakeAcmeValidation validation = Assert.Single(ca.ValidationLog);
            Assert.Equal(AcmeChallengeTypes.TlsAlpn01, validation.ChallengeType);
            Assert.True(validation.Succeeded, validation.Error);
            Assert.Equal(1, proxy.Connections);
            Assert.Empty(proxy.Errors);

            // While the CA validated: acme-tls/1 got the challenge certificate for this very key authorization; a normal
            // client got nothing, because no certificate exists before the first one is issued.
            ChallengeWindow atIssuance = Assert.Single(windows);
            AssertChallengeCertificate(atIssuance.Acme, ca.ValidatedKeyAuthorizations.ToArray()[0]);
            Assert.False(atIssuance.Normal.Succeeded, "Before the first certificate a normal handshake must be refused; it " + atIssuance.Normal + ".");
            Assert.Null(atIssuance.ServedThumbprint);

            // The HTTP challenge endpoint also answers /healthz.
            using HttpClient http = new(new SocketsHttpHandler { UseProxy = false, AllowAutoRedirect = false });
            IPEndPoint httpEndPoint = provisioner.HttpChallengeEndPoint!;
            using HttpResponseMessage health = await http.GetAsync(new Uri("http://127.0.0.1:" + httpEndPoint.Port + "/healthz"), ct);
            Assert.Equal(HttpStatusCode.OK, health.StatusCode);
            Assert.Equal("ok", await health.Content.ReadAsStringAsync(ct));

            // Now the TLS endpoint gives normal clients the issued certificate ...
            using (TlsProbeResult normal = await TlsProbe.HandshakeAsync(provisioner.TlsEndPoint!, TestBed.Identifier, TlsProbe.Http11))
            {
                Assert.True(normal.Succeeded, normal.ToString());
                Assert.Equal(firstThumbprint, normal.Certificate!.Thumbprint);
                Assert.Equal(TlsProbe.Http11, normal.NegotiatedAlpn);
            }

            // ... and so does the QUIC listener: one source, two consumers.
            QuicTestServer server = bed.StartQuicServer();
            bed.Bind(provisioner, options: null, server);
            ClientValidation policy = ClientValidation.PrivateRoot(ca.Ca.Root, TestBed.Identifier);
            QuicTestClient client = bed.Connect(server, policy);
            Assert.Equal(TestBed.Alpn, await client.HandshakeAsync(E2eTimeouts.Step));
            Assert.Equal(first.RawData, client.PresentedCertificate!.LeafDer);

            // A renewal validates over tls-alpn-01 again. During that validation the endpoint serves both at once: the
            // challenge certificate to acme-tls/1, and the certificate still being served to everyone else.
            await provisioner.RenewNowAsync(ct).Within(E2eTimeouts.Order, "the renewal over tls-alpn-01");
            X509Certificate2 second = provisioner.Current!;
            Assert.NotEqual(firstThumbprint, second.Thumbprint);
            Assert.Equal(2, ca.ValidationLog.ToArray().Count(static v => v.ChallengeType == AcmeChallengeTypes.TlsAlpn01 && v.Succeeded));
            Assert.Equal(2, proxy.Connections);
            Assert.Empty(proxy.Errors);
            ChallengeWindow atRenewal = windows.ToArray()[1];
            AssertChallengeCertificate(atRenewal.Acme, ca.ValidatedKeyAuthorizations.ToArray()[1]);
            Assert.True(atRenewal.Normal.Succeeded, "During the renewal a normal handshake must get the served certificate; it " + atRenewal.Normal + ".");
            Assert.Equal(firstThumbprint, atRenewal.ServedThumbprint);
            Assert.Equal(firstThumbprint, atRenewal.Normal.Certificate!.Thumbprint);
            Assert.False(TlsProbe.IsChallengeCertificate(atRenewal.Normal.Certificate));
            Assert.Equal(TlsProbe.Http11, atRenewal.Normal.NegotiatedAlpn);

            // Afterwards both consumers serve the renewed certificate, and the challenge certificate is gone.
            using (TlsProbeResult normal = await TlsProbe.HandshakeAsync(provisioner.TlsEndPoint!, TestBed.Identifier, TlsProbe.Http11))
            {
                Assert.True(normal.Succeeded, normal.ToString());
                Assert.Equal(second.Thumbprint, normal.Certificate!.Thumbprint);
            }

            using (TlsProbeResult acme = await TlsProbe.HandshakeAsync(provisioner.TlsEndPoint!, TestBed.Identifier, TlsProbe.AcmeTls1))
            {
                Assert.False(acme.Certificate is { } presented && TlsProbe.IsChallengeCertificate(presented), "The challenge certificate must be removed after validation; acme-tls/1 " + acme + ".");
            }

            QuicTestClient renewedClient = bed.Connect(server, policy);
            await renewedClient.HandshakeAsync(E2eTimeouts.Step);
            Assert.Equal(second.RawData, renewedClient.PresentedCertificate!.LeafDer);
            Assert.Empty(bed.ConsumerFailures);
        }
        finally
        {
            foreach (ChallengeWindow window in windows)
            {
                window.Dispose();
            }
        }
    }

    /// <summary>A tls-alpn-01 certificate per RFC 8737: acme-tls/1 negotiated, self-signed, the identifier as its only SAN, the key authorization's digest in the critical acmeIdentifier.</summary>
    private static void AssertChallengeCertificate(TlsProbeResult probe, string keyAuthorization)
    {
        Assert.True(probe.Succeeded, "The acme-tls/1 handshake should present the challenge certificate; it " + probe + ".");
        X509Certificate2 certificate = probe.Certificate!;
        Assert.Equal(TlsProbe.AcmeTls1, probe.NegotiatedAlpn);
        Assert.True(TlsProbe.IsChallengeCertificate(certificate), "A tls-alpn-01 certificate carries a critical acmeIdentifier extension.");
        Assert.Equal(TlsProbe.KeyAuthorizationDigest(keyAuthorization), TlsProbe.AcmeIdentifierDigest(certificate));
        X509SubjectAlternativeNameExtension san = certificate.Extensions.OfType<X509SubjectAlternativeNameExtension>().Single();
        Assert.Equal(new[] { TestBed.Identifier }, san.EnumerateDnsNames().ToArray());
        Assert.Equal(certificate.SubjectName.RawData, certificate.IssuerName.RawData);
    }

    /// <summary>What the TLS endpoint presented while the CA was validating: to acme-tls/1, and to a normal (http/1.1) client.</summary>
    private sealed class ChallengeWindow(TlsProbeResult acme, TlsProbeResult normal, string? servedThumbprint) : IDisposable
    {
        public TlsProbeResult Acme { get; } = acme;

        public TlsProbeResult Normal { get; } = normal;

        /// <summary>The provisioner's current certificate at that moment (<see langword="null"/> before the first one).</summary>
        public string? ServedThumbprint { get; } = servedThumbprint;

        public static async Task<ChallengeWindow> ProbeAsync(CertificateProvisioner provisioner)
        {
            IPEndPoint endPoint = provisioner.TlsEndPoint ?? throw new InvalidOperationException("The TLS endpoint is not running.");
            string? served = provisioner.Current?.Thumbprint;
            TlsProbeResult acme = await TlsProbe.HandshakeAsync(endPoint, TestBed.Identifier, TlsProbe.AcmeTls1);
            TlsProbeResult normal = await TlsProbe.HandshakeAsync(endPoint, TestBed.Identifier, TlsProbe.Http11);
            return new ChallengeWindow(acme, normal, served);
        }

        public void Dispose()
        {
            Acme.Dispose();
            Normal.Dispose();
        }
    }
}
