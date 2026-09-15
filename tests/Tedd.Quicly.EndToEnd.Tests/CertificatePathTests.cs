using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Acme.Models;
using Tedd.Quicly.EndToEnd.Tests.Infrastructure;
using Tedd.Quicly.Server.Certificates;
using Tedd.Quicly.Testing.Acme;
using Tedd.Quicly.Transport.MsQuic;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.EndToEnd.Tests;

/// <summary>
/// Scenario 1, the full certificate path: the fake CA issues a certificate through a real http-01 validation, the provisioner
/// hands it to a real MsQuic listener through the consumer adapter, and a real MsQuic client completes a quicly/1 handshake
/// with the test identifier as SNI. On Windows this is ADR 0009's key-storage path end to end, which the unit tests cover
/// only piece by piece: the provisioner loads the PFX with DefaultKeySet (a persisted, non-exportable CNG key), which the
/// Schannel msquic.dll bundled with .NET can use only through a CERTIFICATE_CONTEXT credential.
/// </summary>
[Collection(EndToEndCollection.Name)]
public sealed class CertificatePathTests
{
    private static readonly byte[] Payload = "ping over quicly/1"u8.ToArray();

    [Fact(Timeout = E2eTimeouts.TestMilliseconds)]
    public async Task AcmeCertificate_IsServedByAMsQuicListener_AndAQuiclyClientCompletesTheHandshake()
    {
        Requires.MsQuic();
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using TestBed bed = new();
        FakeAcmeServer ca = bed.StartCa();
        CertificateProvisioner provisioner = bed.CreateProvisioner(ca, bed.AcmeOptions(ca, AcmeChallengeKind.Http01));
        StatusRecorder status = new(provisioner);

        await provisioner.StartAsync(ct).Within(E2eTimeouts.Order, "the provisioner to obtain its first certificate");
        X509Certificate2 certificate = await provisioner.WaitForCertificateAsync(ct).Within(E2eTimeouts.Step, "the first certificate");

        // Issued by the fake CA after a real http-01 validation of the provisioner's challenge endpoint.
        Assert.True(provisioner.Status.State == CertificateState.Valid, status.Describe());
        FakeAcmeValidation validation = Assert.Single(ca.ValidationLog);
        Assert.Equal(AcmeChallengeTypes.Http01, validation.ChallengeType);
        Assert.True(validation.Succeeded, validation.Error);
        Assert.Equal(ca.Ca.Root.SubjectName.Name, certificate.IssuerName.Name);
        Assert.True(certificate.MatchesHostname(TestBed.Identifier));
        Assert.True(certificate.HasPrivateKey);

        // ADR 0009 key storage: a persisted, non-ephemeral key (Schannel cannot sign with an ephemeral one), never exportable.
        Assert.False(MsQuicCertificateHelper.HasEphemeralPrivateKey(certificate));
        if (OperatingSystem.IsWindows())
        {
            KeyContainer key = KeyContainer.Of(certificate);
            Assert.False(key.IsEphemeral);
            Assert.False(key.AllowsExport, "ADR 0009: served certificates are never loaded Exportable.");
            Assert.True(key.Exists());
            Assert.False(MsQuicCertificateHelper.TryExportPkcs12(certificate, out _), "A non-exportable key cannot become MsQuic's PKCS#12 credential.");
        }

        // The consumer adapter: the binder hands the certificate to the listener, which opens a configuration for it.
        QuicTestServer server = bed.StartQuicServer();
        CertificateBinder binder = bed.Bind(provisioner, options: null, server);
        ServedConfiguration served = Assert.Single(server.Configurations);
        Assert.Same(certificate, served.Certificate);
        Assert.Same(certificate, binder.Certificate);
        if (OperatingSystem.IsWindows())
        {
            // The Schannel msquic.dll refuses PKCS#12 and the key is not exportable anyway: the certificate context is handed
            // over as it is, with the provisioner's key container behind it.
            Assert.Equal(QUIC_CREDENTIAL_TYPE.CERTIFICATE_CONTEXT, served.CredentialType);
        }

        QuicTestClient client = bed.Connect(server, ClientValidation.PrivateRoot(ca.Ca.Root, TestBed.Identifier));
        Assert.Equal(TestBed.Alpn, await client.HandshakeAsync(E2eTimeouts.Step));

        // What the client saw: the provisioned leaf, over TLS 1.3.
        PresentedCertificate presented = Assert.IsType<PresentedCertificate>(client.PresentedCertificate);
        Assert.Equal(certificate.RawData, presented.LeafDer);
        Assert.Equal(QUIC_TLS_PROTOCOL_VERSION.TLS_1_3, client.HandshakeInfo().TlsProtocolVersion);

        // What the listener saw: the identifier as SNI, quicly/1 negotiated, and the configuration it handed out.
        ServerConnection accepted = await server.AcceptedFromAsync(client, E2eTimeouts.Step);
        Assert.Equal(TestBed.Identifier, accepted.ServerName);
        Assert.Equal(TestBed.Alpn, accepted.NegotiatedAlpn);
        Assert.Equal(TestBed.Alpn, await accepted.WhenConnected.Within(E2eTimeouts.Step, "the server side of the handshake"));
        Assert.Same(served, accepted.Configuration);

        // The connection carries data.
        Assert.Equal(Payload, await client.EchoAsync(Payload, E2eTimeouts.Step));
        Assert.Equal(1, accepted.Echoes);
        Assert.Empty(bed.ConsumerFailures);
        TestContext.Current.TestOutputHelper?.WriteLine("msquic " + MsQuicApi.Instance.Version + " (" + MsQuicApi.Instance.TlsProvider + "), credential " + served.CredentialType);

        // An orderly shutdown (clients, listener, binder, provisioner) leaves no key container behind (ADR 0009).
        KeyContainer? servedKey = OperatingSystem.IsWindows() ? KeyContainer.Of(certificate) : null;
        await bed.DisposeAsync();
        if (OperatingSystem.IsWindows())
        {
            Assert.False(servedKey!.Value.Exists(), "Disposing the provisioner should delete the served certificate's key container.");
        }
    }

    [Fact(Timeout = E2eTimeouts.TestMilliseconds)]
    public async Task ListenerBoundBeforeTheFirstCertificate_RefusesConnections_UntilTheProvisionerHasOne()
    {
        Requires.MsQuic();
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using TestBed bed = new();
        FakeAcmeServer ca = bed.StartCa();
        CertificateProvisioner provisioner = bed.CreateProvisioner(ca, bed.AcmeOptions(ca));
        ClientValidation validation = ClientValidation.PrivateRoot(ca.Ca.Root, TestBed.Identifier);

        // A game server starts its listener at once and binds it before the provisioner has anything to serve.
        QuicTestServer server = bed.StartQuicServer();
        CertificateBinder binder = bed.Bind(provisioner, options: null, server);
        Assert.Null(binder.Certificate);
        Assert.Empty(server.Configurations);

        QuicTestClient early = bed.Connect(server, validation);
        int refusal = await early.HandshakeFailureAsync(E2eTimeouts.Failure);
        Assert.True(refusal == MsQuicStatus.QUIC_STATUS_CONNECTION_REFUSED, "Expected QUIC_STATUS_CONNECTION_REFUSED, got " + MsQuicStatus.GetName(refusal) + ".");
        Assert.True(server.RefusedWithoutCertificate >= 1);
        Assert.Null(early.PresentedCertificate);

        await provisioner.StartAsync(ct).Within(E2eTimeouts.Order, "the provisioner to obtain its first certificate");
        ServedConfiguration served = Assert.Single(server.Configurations);
        Assert.Same(provisioner.Current, served.Certificate);

        QuicTestClient late = bed.Connect(server, validation);
        Assert.Equal(TestBed.Alpn, await late.HandshakeAsync(E2eTimeouts.Step));
        Assert.Equal(provisioner.Current!.RawData, late.PresentedCertificate!.LeafDer);
        Assert.Equal(Payload, await late.EchoAsync(Payload, E2eTimeouts.Step));
        TestContext.Current.TestOutputHelper?.WriteLine("refused before the first certificate with " + MsQuicStatus.GetName(refusal));
    }
}
