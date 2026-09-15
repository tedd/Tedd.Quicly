using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Testing.Certificates;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic.Tests;

/// <summary>
/// Tests added by review. Each pins a documented requirement (ADR 0006 as amended, ADR 0009, the task brief) that the
/// implementation does not meet yet; they are expected to fail until the production code is fixed.
/// </summary>
[Collection(MsQuicCollection.Name)]
public class ReviewFindingsTests
{
    /// <summary>
    /// ADR 0006 (amended) / ADR 0009 and the task brief: keys handed to Schannel are imported with
    /// <c>PersistKeySet | UserKeySet</c>; <c>Exportable</c> and <c>EphemeralKeySet</c> are not used.
    /// <see cref="TestCertificates"/> re-imports every certificate with <see cref="X509KeyStorageFlags.Exportable"/>.
    /// </summary>
    [Fact]
    public void TestCertificates_do_not_import_private_keys_as_exportable()
    {
        if (!OperatingSystem.IsWindows())
        {
            Assert.Skip("CNG export policies exist only on Windows");
            return;
        }
        using X509Certificate2 certificate = TestCertificates.CreateSelfSigned("CN=localhost", TimeSpan.FromHours(1), ecdsa: true, "localhost");
        using ECDsa? key = certificate.GetECDsaPrivateKey();
        ECDsaCng cng = Assert.IsType<ECDsaCng>(key);
        CngExportPolicies policy = cng.Key.ExportPolicy;
        Assert.True(
            (policy & (CngExportPolicies.AllowExport | CngExportPolicies.AllowPlaintextExport)) == 0,
            $"TestCertificates imported the key with export policy '{policy}' (X509KeyStorageFlags.Exportable), which ADR 0006/0009 forbid.");
    }

    /// <summary>
    /// ADR 0009 renewal sequence: open the new configuration, swap the pointer the listener hands out, close the old
    /// configuration; "old key containers are deleted after the configuration swap has completed and all connections
    /// created with the old configuration are gone". <see cref="MsQuicConfiguration.Close"/> deletes the key container it
    /// persisted immediately, while connections created with that configuration are still alive.
    /// </summary>
    [Fact]
    public async Task Configuration_close_keeps_its_persisted_key_until_connections_created_with_it_are_gone()
    {
        if (!OperatingSystem.IsWindows())
        {
            Assert.Skip("the persisted key container exists only on the Windows CERTIFICATE_CONTEXT path");
            return;
        }
        using var loopback = new Loopback();
        using ECDsa ephemeralKey = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest("CN=localhost", ephemeralKey, HashAlgorithmName.SHA256);
        using X509Certificate2 ephemeral = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddHours(1));
        Assert.True(MsQuicCertificateHelper.HasEphemeralPrivateKey(ephemeral));

        MsQuicConfiguration renewed = MsQuicConfiguration.CreateServer(loopback.Registration, [Loopback.Alpn], ephemeral, Loopback.TestServerSettings());
        string? keyName = null;
        CngProvider? provider = null;
        try
        {
            Assert.Equal(QUIC_CREDENTIAL_TYPE.CERTIFICATE_CONTEXT, renewed.CredentialType);
            X509Certificate2 owned = Assert.IsType<X509Certificate2>(renewed.OwnedCertificate);
            using (ECDsa? ownedKey = owned.GetECDsaPrivateKey())
            {
                ECDsaCng ownedCng = Assert.IsType<ECDsaCng>(ownedKey);
                keyName = ownedCng.Key.KeyName;
                provider = ownedCng.Key.Provider;
            }
            Assert.NotNull(keyName);
            Assert.True(CngKey.Exists(keyName, provider!));

            loopback.SelectConfiguration = _ => renewed;
            (MsQuicConnection client, ConnectionRecorder clientEvents, MsQuicConnection server, ConnectionRecorder serverEvents) = await loopback.ConnectPairAsync();

            // ADR 0009: close the old configuration after the swap, while its connections live on.
            renewed.Close();
            Assert.False(server.IsClosed);
            Assert.True(
                CngKey.Exists(keyName, provider!),
                "MsQuicConfiguration.Close deleted the persisted key container while a connection created with the configuration is still alive; ADR 0009 defers deletion until those connections are gone.");

            client.Shutdown(QUIC_CONNECTION_SHUTDOWN_FLAGS.NONE, 0);
            await clientEvents.ShutdownCompleteTcs.Within();
            await serverEvents.ShutdownCompleteTcs.Within();
        }
        finally
        {
            renewed.Close();
            if (keyName is not null && provider is not null && CngKey.Exists(keyName, provider))
            {
                using CngKey leftover = CngKey.Open(keyName, provider);
                leftover.Delete();
            }
        }
    }
}
