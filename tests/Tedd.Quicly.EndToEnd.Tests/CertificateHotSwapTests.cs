using System.Security.Cryptography.X509Certificates;
using System.Text;
using Tedd.Quicly.EndToEnd.Tests.Infrastructure;
using Tedd.Quicly.Server.Certificates;
using Tedd.Quicly.Testing.Acme;

namespace Tedd.Quicly.EndToEnd.Tests;

/// <summary>
/// Scenario 2, certificate hot swap: a renewal forced through the provisioner reaches the running MsQuic listener through the
/// binder; new connections present the new certificate while an established connection keeps working; the superseded
/// certificate is disposed only when the binder's grace period ends (on a manual clock, so "not before, but then" is exact);
/// and connections opened after that disposal still succeed.
/// </summary>
[Collection(EndToEndCollection.Name)]
public sealed class CertificateHotSwapTests
{
    [Fact(Timeout = E2eTimeouts.TestMilliseconds)]
    public async Task ForcedRenewal_MovesNewConnectionsToTheNewCertificate_KeepsTheExistingConnection_AndDisposesTheOldCertificateOnlyAfterTheGracePeriod()
    {
        Requires.MsQuic();
        CancellationToken ct = TestContext.Current.CancellationToken;
        await using TestBed bed = new();
        FakeAcmeServer ca = bed.StartCa();
        CertificateProvisioner provisioner = bed.CreateProvisioner(ca, bed.AcmeOptions(ca));
        StatusRecorder status = new(provisioner);
        await provisioner.StartAsync(ct).Within(E2eTimeouts.Order, "the first certificate");
        X509Certificate2 first = provisioner.Current!;
        string firstThumbprint = first.Thumbprint;
        KeyContainer? firstKey = OperatingSystem.IsWindows() ? KeyContainer.Of(first) : null;

        // The grace period runs on a manual clock: its end is a step of the test, not a race with the machine.
        ManualTimeProvider clock = new();
        TimeSpan grace = CertificateBinderOptions.DefaultGracePeriod;
        QuicTestServer server = bed.StartQuicServer();
        CertificateBinder binder = bed.Bind(provisioner, new CertificateBinderOptions { SupersededCertificateGracePeriod = grace, TimeProvider = clock }, server);
        ServedConfiguration firstConfiguration = Assert.Single(server.Configurations);
        ClientValidation validation = ClientValidation.PrivateRoot(ca.Ca.Root, TestBed.Identifier);

        QuicTestClient existing = bed.Connect(server, validation);
        await existing.HandshakeAsync(E2eTimeouts.Step);
        Assert.Equal(first.RawData, existing.PresentedCertificate!.LeafDer);
        await AssertEchoesAsync(existing, "before the renewal");

        // The server has finished its side of the handshake as well, so no connection is handshaking with the first configuration.
        ServerConnection existingOnServer = await server.AcceptedFromAsync(existing, E2eTimeouts.Step);
        Assert.Equal(TestBed.Alpn, await existingOnServer.WhenConnected.Within(E2eTimeouts.Step, "the server side of the first handshake"));
        Assert.Same(firstConfiguration, existingOnServer.Configuration);

        // Force a renewal: a new order at the CA; the binder applies the result to the listener before RenewNowAsync returns.
        await provisioner.RenewNowAsync(ct).Within(E2eTimeouts.Order, "the forced renewal");
        X509Certificate2 second = provisioner.Current!;
        Assert.NotEqual(firstThumbprint, second.Thumbprint);
        Assert.True(status.Has(CertificateState.Valid, "Renewed on request"), status.Describe());
        Assert.Equal(2, ca.IssuedCount);
        Assert.Same(second, binder.Certificate);
        Assert.Equal(2, server.Configurations.Length);
        ServedConfiguration secondConfiguration = Assert.IsType<ServedConfiguration>(server.Current);
        Assert.Same(second, secondConfiguration.Certificate);

        // ADR 0009, open new, swap, close old: no connection is still handshaking with the old configuration, so it is
        // closed now; MsQuic keeps it alive for the established connection.
        await firstConfiguration.WhenClosed.Within(E2eTimeouts.Step, "the replaced configuration to be closed");

        // The superseded certificate is not disposed yet: a handshake that selected it may still need its key.
        Assert.Equal(1, binder.PendingDisposalCount);
        Assert.False(IsDisposed(first));
        if (OperatingSystem.IsWindows())
        {
            Assert.True(firstKey!.Value.Exists());
        }

        // New connections present the new certificate; the existing connection keeps working.
        QuicTestClient renewed = bed.Connect(server, validation);
        await renewed.HandshakeAsync(E2eTimeouts.Step);
        Assert.Equal(second.RawData, renewed.PresentedCertificate!.LeafDer);
        Assert.Same(secondConfiguration, (await server.AcceptedFromAsync(renewed, E2eTimeouts.Step)).Configuration);
        await AssertEchoesAsync(existing, "after the swap");
        await AssertEchoesAsync(renewed, "on the renewed certificate");

        // The grace period ends on time, and not a tick early.
        clock.Advance(grace - TimeSpan.FromTicks(1));
        Assert.False(IsDisposed(first));
        Assert.Equal(1, binder.PendingDisposalCount);
        clock.Advance(TimeSpan.FromTicks(1));
        Assert.True(IsDisposed(first));
        Assert.Equal(0, binder.PendingDisposalCount);
        Assert.False(IsDisposed(second));
        if (OperatingSystem.IsWindows())
        {
            // Disposing a DefaultKeySet certificate deletes its key container: ADR 0009's "old key containers are deleted".
            Assert.False(firstKey!.Value.Exists(), "The superseded certificate's key container should be deleted with it.");
            Assert.True(KeyContainer.Of(second).Exists());
        }

        // An established connection needs no key after the handshake; a new one uses the current certificate's key.
        await AssertEchoesAsync(existing, "after the old certificate was disposed");
        QuicTestClient afterDisposal = bed.Connect(server, validation);
        await afterDisposal.HandshakeAsync(E2eTimeouts.Step);
        Assert.Equal(second.RawData, afterDisposal.PresentedCertificate!.LeafDer);
        await AssertEchoesAsync(afterDisposal, "after the old certificate was disposed");

        Assert.False(existing.IsShuttingDown);
        Assert.Equal(new[] { 1, 2, 2 }, server.Connections.Select(static c => c.Configuration.Generation).ToArray());
        Assert.Equal(0, binder.RetainedCertificateCount);
        Assert.Empty(bed.ConsumerFailures);
    }

    private static async Task AssertEchoesAsync(QuicTestClient client, string when)
    {
        byte[] payload = Encoding.UTF8.GetBytes("echo " + when);
        Assert.Equal(payload, await client.EchoAsync(payload, E2eTimeouts.Step));
    }

    private static bool IsDisposed(X509Certificate2 certificate) => certificate.Handle == IntPtr.Zero;
}
