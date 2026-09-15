using Tedd.Quicly.EndToEnd.Tests.Infrastructure;
using Tedd.Quicly.Server.Certificates;
using Tedd.Quicly.Testing.Acme;

namespace Tedd.Quicly.EndToEnd.Tests;

/// <summary>
/// Scenario 5, restart behaviour. A second provisioner started on the same paths serves the persisted certificate without
/// contacting the CA (no request at all, so certainly no new order), and MsQuic serves the reloaded certificate as it served
/// the issued one. A persisted certificate whose record names a different directory (or that has no record) is treated as
/// due: served at once, replaced by an order from the configured directory during start-up, and the replacement's record
/// makes the next restart quiet again.
/// </summary>
[Collection(EndToEndCollection.Name)]
public sealed class RestartTests
{
    private static readonly byte[] Payload = "after the restart"u8.ToArray();

    [Fact(Timeout = E2eTimeouts.TestMilliseconds)]
    public async Task SecondProvisionerOnTheSamePaths_ReusesThePersistedCertificate_WithoutContactingTheCa()
    {
        Requires.MsQuic();
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using TestBed bed = new();
        FakeAcmeServer ca = bed.StartCa();
        ClientValidation validation = ClientValidation.PrivateRoot(ca.Ca.Root, TestBed.Identifier);

        CertificateProvisioner first = bed.CreateProvisioner(ca, bed.AcmeOptions(ca));
        await first.StartAsync(ct).Within(E2eTimeouts.Order, "the first provisioner to obtain a certificate");
        byte[] issued = first.Current!.RawData;
        string thumbprint = first.Current.Thumbprint;
        QuicTestServer firstServer = bed.StartQuicServer();
        CertificateBinder firstBinder = bed.Bind(first, options: null, firstServer);
        QuicTestClient before = bed.Connect(firstServer, validation);
        await before.HandshakeAsync(E2eTimeouts.Step);
        Assert.Equal(issued, before.PresentedCertificate!.LeafDer);
        await bed.StopAsync(firstServer, firstBinder, first);
        Assert.True(File.Exists(bed.CertificatePath));
        Assert.True(File.Exists(bed.CertificateRecordPath));
        int requests = ca.RequestLog.Count;
        Assert.Equal(1, NewOrders(ca));
        Assert.Equal(1, ca.IssuedCount);

        // The restart: a new provisioner on the same paths, bound to a new listener before it starts, as a game server would.
        CertificateProvisioner second = bed.CreateProvisioner(ca, bed.AcmeOptions(ca));
        StatusRecorder status = new(second);
        QuicTestServer server = bed.StartQuicServer();
        bed.Bind(second, options: null, server);
        await second.StartAsync(ct).Within(E2eTimeouts.Order, "the restarted provisioner");

        Assert.True(second.Status.State == CertificateState.Valid, status.Describe());
        Assert.True(status.Has(CertificateState.Valid, "Loaded the persisted certificate"), status.Describe());
        Assert.Equal(thumbprint, second.Current!.Thumbprint);
        Assert.Equal(requests, ca.RequestLog.Count); // not a single request: no directory, no account check, no order
        Assert.Equal(1, NewOrders(ca));
        Assert.Equal(1, ca.IssuedCount);

        // The certificate reloaded from the PFX (DefaultKeySet, a fresh key container) works with MsQuic like the issued one.
        ServedConfiguration served = Assert.Single(server.Configurations);
        Assert.Equal(thumbprint, served.Thumbprint);
        QuicTestClient after = bed.Connect(server, validation);
        Assert.Equal(TestBed.Alpn, await after.HandshakeAsync(E2eTimeouts.Step));
        Assert.Equal(issued, after.PresentedCertificate!.LeafDer);
        Assert.Equal(Payload, await after.EchoAsync(Payload, E2eTimeouts.Step));

        // The renewal loop has been running all along; it stayed quiet.
        Assert.Equal(requests, ca.RequestLog.Count);
        Assert.Empty(status.Errors);
    }

    [Theory(Timeout = E2eTimeouts.TestMilliseconds)]
    [InlineData(true)]
    [InlineData(false)]
    public async Task PersistedCertificateFromAnotherOrAnUnrecordedDirectory_IsTreatedAsDue_ServedWhileAReplacementIsOrdered(bool recordNamesAnotherDirectory)
    {
        Requires.MsQuic();
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using TestBed bed = new();
        FakeAcmeServer original = bed.StartCa();
        CertificateProvisioner first = bed.CreateProvisioner(original, bed.AcmeOptions(original));
        await first.StartAsync(ct).Within(E2eTimeouts.Order, "the first provisioner to obtain a certificate");
        string persisted = first.Current!.Thumbprint;
        await bed.StopAsync(server: null, binder: null, first);
        Assert.True(File.Exists(bed.CertificateRecordPath));

        FakeAcmeServer configured;
        string reason;
        if (recordNamesAnotherDirectory)
        {
            // A switch of directory (staging to production, say): the record names the directory that issued the certificate.
            configured = bed.StartCa();
            reason = "was issued by " + original.DirectoryUrl.AbsoluteUri + ", not " + configured.DirectoryUrl;
        }
        else
        {
            // A PFX without a record (placed there by hand, say).
            File.Delete(bed.CertificateRecordPath);
            configured = original;
            reason = "was issued by an unrecorded directory";
        }

        int originalRequests = original.RequestLog.Count;
        int ordersBefore = NewOrders(configured);
        int issuedBefore = configured.IssuedCount;

        CertificateProvisioner second = bed.CreateProvisioner(configured, bed.AcmeOptions(configured));
        StatusRecorder status = new(second);
        QuicTestServer server = bed.StartQuicServer();
        CertificateBinder binder = bed.Bind(second, new CertificateBinderOptions { TimeProvider = new ManualTimeProvider() }, server);
        await second.StartAsync(ct).Within(E2eTimeouts.Order, "the restarted provisioner to replace the persisted certificate");

        // Treated as due: served at once, and replaced during start-up by an order from the configured directory.
        Assert.True(second.Status.State == CertificateState.Valid, status.Describe());
        Assert.True(status.Has(CertificateState.Starting, reason), status.Describe());
        Assert.True(status.Has(CertificateState.Starting, "it is served while a replacement is ordered"), status.Describe());
        Assert.True(status.Has(CertificateState.Valid, "Obtained "), status.Describe());
        string replacement = second.Current!.Thumbprint;
        byte[] replacementDer = second.Current.RawData;
        Assert.NotEqual(persisted, replacement);
        Assert.Equal(ordersBefore + 1, NewOrders(configured));
        Assert.Equal(issuedBefore + 1, configured.IssuedCount);
        if (recordNamesAnotherDirectory)
        {
            Assert.Equal(originalRequests, original.RequestLog.Count); // the old directory is never contacted again
        }

        // The listener saw both, in order, through the same binder; new connections present the replacement.
        Assert.Equal(new[] { persisted, replacement }, server.Configurations.Select(static c => c.Thumbprint).ToArray());
        QuicTestClient client = bed.Connect(server, ClientValidation.PrivateRoot(configured.Ca.Root, TestBed.Identifier));
        Assert.Equal(TestBed.Alpn, await client.HandshakeAsync(E2eTimeouts.Step));
        Assert.Equal(replacementDer, client.PresentedCertificate!.LeafDer);

        // The replacement was recorded as issued by the configured directory: the next restart is quiet again.
        await bed.StopAsync(server, binder, second);
        int requests = configured.RequestLog.Count;
        CertificateProvisioner third = bed.CreateProvisioner(configured, bed.AcmeOptions(configured));
        StatusRecorder thirdStatus = new(third);
        await third.StartAsync(ct).Within(E2eTimeouts.Order, "the second restart");
        Assert.True(thirdStatus.Has(CertificateState.Valid, "Loaded the persisted certificate"), thirdStatus.Describe());
        Assert.Equal(replacement, third.Current!.Thumbprint);
        Assert.Equal(requests, configured.RequestLog.Count);
    }

    private static int NewOrders(FakeAcmeServer ca) => ca.RequestLog.ToArray().Count(static entry => entry == "POST /new-order");
}
