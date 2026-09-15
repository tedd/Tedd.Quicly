using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.EndToEnd.Tests.Infrastructure;
using Tedd.Quicly.Server.Certificates;
using Tedd.Quicly.Testing.Acme;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.EndToEnd.Tests;

/// <summary>
/// Scenario 3, client validation against the provisioned certificate (ARCHITECTURE §8). SystemRoots fails because the test
/// CA is not trusted; PinnedSpki accepts the leaf whose SubjectPublicKeyInfo hash is pinned and rejects a wrong pin, and a
/// pin survives a renewal only when the server reuses its key (the ReuseKey requirement of §8); a callback validator
/// receives the portable DER chain and can accept it on its own terms (here: a chain to the private root it trusts).
/// </summary>
[Collection(EndToEndCollection.Name)]
public sealed class ClientValidationTests
{
    private static readonly byte[] Payload = "validated"u8.ToArray();

    [Fact(Timeout = E2eTimeouts.TestMilliseconds)]
    public async Task SystemRoots_RejectsTheProvisionedCertificate_BecauseTheTestCaIsNotTrusted()
    {
        Requires.MsQuic();
        await using TestBed bed = new();
        Served served = await ServeAsync(bed, TestContext.Current.CancellationToken);

        QuicTestClient client = bed.Connect(served.Server, ClientValidation.SystemRoots);
        int status = await client.HandshakeFailureAsync(E2eTimeouts.Failure);

        Assert.True(MsQuicStatus.Failed(status), MsQuicStatus.GetName(status));
        if (OperatingSystem.IsWindows())
        {
            // Schannel's verdict is "untrusted root" (the callback test sees it deferred), which MsQuic reports as the TLS alert
            // unknown_ca: the chain arrived but ends in no trusted anchor. The name matched, so trust is all that failed.
            Assert.True(status == TlsAlerts.UnknownCa, "Expected QUIC_STATUS_TLS_ALERT(48), unknown_ca; got " + MsQuicStatus.GetName(status) + ".");
        }

        Assert.Null(client.PresentedCertificate); // the platform decided; nothing was handed to the application

        // The listener presented its certificate, but the handshake never completed on its side either.
        ServerConnection accepted = await served.Server.AcceptedFromAsync(client, E2eTimeouts.Step);
        Assert.False(await accepted.WhenShutdownComplete.Within(E2eTimeouts.Failure, "the server side of the rejected handshake to end"));
        Assert.False(accepted.WhenConnected.IsCompleted);
    }

    [Fact(Timeout = E2eTimeouts.TestMilliseconds)]
    public async Task PinnedSpki_MatchingTheLeafKey_Connects_WhateverThePlatformThinksOfTheChain()
    {
        Requires.MsQuic();
        await using TestBed bed = new();
        Served served = await ServeAsync(bed, TestContext.Current.CancellationToken);
        byte[] pin = Pins.SpkiSha256(served.Certificate);

        QuicTestClient client = bed.Connect(served.Server, ClientValidation.PinnedSpki(pin));

        Assert.Equal(TestBed.Alpn, await client.HandshakeAsync(E2eTimeouts.Step));
        PresentedCertificate presented = Assert.IsType<PresentedCertificate>(client.PresentedCertificate);
        Assert.Equal(served.Certificate.RawData, presented.LeafDer);
        Assert.Equal(pin, presented.SpkiSha256());

        // The pin is the trust anchor: the platform did not trust the private root, and that did not matter.
        Assert.True(MsQuicStatus.Failed(presented.DeferredStatus), MsQuicStatus.GetName(presented.DeferredStatus));
        Assert.Equal(Payload, await client.EchoAsync(Payload, E2eTimeouts.Step));
    }

    [Fact(Timeout = E2eTimeouts.TestMilliseconds)]
    public async Task PinnedSpki_WithAWrongPin_IsRejected()
    {
        Requires.MsQuic();
        await using TestBed bed = new();
        Served served = await ServeAsync(bed, TestContext.Current.CancellationToken);
        using ECDsa otherKey = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        byte[] wrongPin = SHA256.HashData(otherKey.ExportSubjectPublicKeyInfo());

        QuicTestClient client = bed.Connect(served.Server, ClientValidation.PinnedSpki(wrongPin));
        int status = await client.HandshakeFailureAsync(E2eTimeouts.Failure);

        AssertRejectedByTheValidator(status);
        PresentedCertificate presented = Assert.IsType<PresentedCertificate>(client.PresentedCertificate);
        Assert.Equal(served.Certificate.RawData, presented.LeafDer); // it saw the certificate and said no
        Assert.Null(client.ValidatorFailure);
        ServerConnection accepted = await served.Server.AcceptedFromAsync(client, E2eTimeouts.Step);
        Assert.False(await accepted.WhenShutdownComplete.Within(E2eTimeouts.Failure, "the server side of the rejected handshake to end"));
    }

    [Fact(Timeout = E2eTimeouts.TestMilliseconds)]
    public async Task Callback_ReceivesThePortableDerChain_AndAcceptsAChainToThePrivateRootItTrusts()
    {
        Requires.MsQuic();
        await using TestBed bed = new();
        Served served = await ServeAsync(bed, TestContext.Current.CancellationToken);

        QuicTestClient client = bed.Connect(served.Server, ClientValidation.PrivateRoot(served.Ca.Ca.Root, TestBed.Identifier));

        Assert.Equal(TestBed.Alpn, await client.HandshakeAsync(E2eTimeouts.Step));
        PresentedCertificate presented = Assert.IsType<PresentedCertificate>(client.PresentedCertificate);
        Assert.Equal(served.Certificate.RawData, presented.LeafDer);

        // The chain arrives as portable DER (PKCS#7) and holds the leaf.
        X509Certificate2Collection chain = presented.LoadChain();
        try
        {
            Assert.NotEmpty(chain);
            Assert.Contains(chain, c => c.Thumbprint == served.Certificate.Thumbprint);
        }
        finally
        {
            foreach (X509Certificate2 certificate in chain)
            {
                certificate.Dispose();
            }
        }

        // Platform validation ran and its verdict was deferred to the validator: the machine does not trust the test CA ...
        Assert.True(MsQuicStatus.Failed(presented.DeferredStatus), MsQuicStatus.GetName(presented.DeferredStatus));
        if (OperatingSystem.IsWindows())
        {
            Assert.True(presented.DeferredStatus == MsQuicStatus.QUIC_STATUS_CERT_UNTRUSTED_ROOT, "Expected the deferred QUIC_STATUS_CERT_UNTRUSTED_ROOT, got " + MsQuicStatus.GetName(presented.DeferredStatus) + ".");
        }

        // ... and the validator decided on its own terms: this exact chain builds to the fake CA's root, for this name only.
        Assert.True(ClientValidation.ChainsTo(presented, served.Ca.Ca.Root, TestBed.Identifier));
        Assert.False(ClientValidation.ChainsTo(presented, served.Ca.AlternateCa.Root, TestBed.Identifier));
        Assert.False(ClientValidation.ChainsTo(presented, served.Ca.Ca.Root, "other.example.test"));
        Assert.Equal(Payload, await client.EchoAsync(Payload, E2eTimeouts.Step));
    }

    [Fact(Timeout = E2eTimeouts.TestMilliseconds)]
    public async Task Callback_TrustingAnotherRoot_RejectsTheCertificate()
    {
        Requires.MsQuic();
        await using TestBed bed = new();
        Served served = await ServeAsync(bed, TestContext.Current.CancellationToken);

        QuicTestClient client = bed.Connect(served.Server, ClientValidation.PrivateRoot(served.Ca.AlternateCa.Root, TestBed.Identifier));
        int status = await client.HandshakeFailureAsync(E2eTimeouts.Failure);

        AssertRejectedByTheValidator(status);
        Assert.Equal(served.Certificate.RawData, client.PresentedCertificate!.LeafDer);
    }

    [Theory(Timeout = E2eTimeouts.TestMilliseconds)]
    [InlineData(true)]
    [InlineData(false)]
    public async Task PinnedSpki_SurvivesARenewal_OnlyWhenTheServerReusesItsKey(bool reuseKey)
    {
        Requires.MsQuic();
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using TestBed bed = new();
        ManualTimeProvider clock = new();
        Served served = await ServeAsync(bed, ct, options => options.ReuseKey = reuseKey, new CertificateBinderOptions { TimeProvider = clock });
        X509Certificate2 first = served.Certificate;
        string firstThumbprint = first.Thumbprint;
        byte[] pin = Pins.SpkiSha256(first);
        KeyContainer? firstKey = OperatingSystem.IsWindows() ? KeyContainer.Of(first) : null;

        QuicTestClient before = bed.Connect(served.Server, ClientValidation.PinnedSpki(pin));
        await before.HandshakeAsync(E2eTimeouts.Step);

        await served.Provisioner.RenewNowAsync(ct).Within(E2eTimeouts.Order, "the forced renewal");
        X509Certificate2 second = served.Provisioner.Current!;
        Assert.NotEqual(firstThumbprint, second.Thumbprint);
        QuicTestClient after = bed.Connect(served.Server, ClientValidation.PinnedSpki(pin));
        if (reuseKey)
        {
            // Same key, new certificate: the pin still matches.
            Assert.Equal(pin, Pins.SpkiSha256(second));
            await after.HandshakeAsync(E2eTimeouts.Step);
            Assert.Equal(second.RawData, after.PresentedCertificate!.LeafDer);

            // Each load has a key container of its own, so disposing the superseded certificate (and its container) leaves the
            // current certificate's key, the same key material, usable.
            clock.Advance(CertificateBinderOptions.DefaultGracePeriod);
            Assert.True(first.Handle == IntPtr.Zero, "The superseded certificate should be disposed after the grace period.");
            if (OperatingSystem.IsWindows())
            {
                KeyContainer secondKey = KeyContainer.Of(second);
                Assert.NotEqual(firstKey!.Value.Name, secondKey.Name);
                Assert.False(firstKey.Value.Exists());
                Assert.True(secondKey.Exists());
            }

            QuicTestClient afterDisposal = bed.Connect(served.Server, ClientValidation.PinnedSpki(pin));
            await afterDisposal.HandshakeAsync(E2eTimeouts.Step);
            Assert.Equal(Payload, await afterDisposal.EchoAsync(Payload, E2eTimeouts.Step));
        }
        else
        {
            // A new key: the old pin no longer matches (why ARCHITECTURE §8 requires ReuseKey for PinnedSpki).
            byte[] newPin = Pins.SpkiSha256(second);
            Assert.NotEqual(pin, newPin);
            AssertRejectedByTheValidator(await after.HandshakeFailureAsync(E2eTimeouts.Failure));
            QuicTestClient repinned = bed.Connect(served.Server, ClientValidation.PinnedSpki(newPin));
            await repinned.HandshakeAsync(E2eTimeouts.Step);
        }

        // The connection made before the renewal was never affected.
        Assert.Equal(Payload, await before.EchoAsync(Payload, E2eTimeouts.Step));
    }

    /// <summary>The application's validator refused the certificate: MsQuic fails the handshake with the TLS alert bad_certificate.</summary>
    private static void AssertRejectedByTheValidator(int status)
    {
        Assert.True(status == MsQuicStatus.QUIC_STATUS_BAD_CERTIFICATE, "Expected QUIC_STATUS_BAD_CERTIFICATE, got " + MsQuicStatus.GetName(status) + ".");
    }

    /// <summary>A provisioned certificate (http-01, fake CA) served by a QUIC listener through a binder.</summary>
    private static async Task<Served> ServeAsync(TestBed bed, CancellationToken ct, Action<AcmeProvisioningOptions>? configure = null, CertificateBinderOptions? binderOptions = null)
    {
        FakeAcmeServer ca = bed.StartCa();
        AcmeProvisioningOptions options = bed.AcmeOptions(ca);
        configure?.Invoke(options);
        CertificateProvisioner provisioner = bed.CreateProvisioner(ca, options);
        await provisioner.StartAsync(ct).Within(E2eTimeouts.Order, "the provisioner to obtain its first certificate");
        QuicTestServer server = bed.StartQuicServer();
        bed.Bind(provisioner, binderOptions, server);
        return new Served(ca, provisioner, server, provisioner.Current!);
    }

    private sealed record Served(FakeAcmeServer Ca, CertificateProvisioner Provisioner, QuicTestServer Server, X509Certificate2 Certificate);
}
