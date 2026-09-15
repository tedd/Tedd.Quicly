using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Testing.Certificates;

namespace Tedd.Quicly.Transport.MsQuic.Tests;

public class CertificateHelperTests
{
    private static X509Certificate2 CreateEphemeral(bool ecdsa)
    {
        if (ecdsa)
        {
            using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
            return new CertificateRequest("CN=ephemeral", key, HashAlgorithmName.SHA256).CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddDays(1));
        }
        using RSA rsa = RSA.Create(2048);
        return new CertificateRequest("CN=ephemeral", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1).CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddDays(1));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Ephemeral_keys_are_detected_and_re_imported_on_windows(bool ecdsa)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "CNG ephemeral keys are a Windows concept");
        using X509Certificate2 ephemeral = CreateEphemeral(ecdsa);
        Assert.True(MsQuicCertificateHelper.HasEphemeralPrivateKey(ephemeral));

        using X509Certificate2 persisted = MsQuicCertificateHelper.EnsurePersistedPrivateKey(ephemeral);
        Assert.NotSame(ephemeral, persisted);
        Assert.True(persisted.HasPrivateKey);
        Assert.False(MsQuicCertificateHelper.HasEphemeralPrivateKey(persisted));
        Assert.Equal(ephemeral.Thumbprint, persisted.Thumbprint);

        // Already persisted: same instance comes back.
        Assert.Same(persisted, MsQuicCertificateHelper.EnsurePersistedPrivateKey(persisted));
    }

    [Fact]
    public void Test_certificates_are_already_persisted()
    {
        using X509Certificate2 cert = TestCertificates.CreateSelfSigned("CN=t", TimeSpan.FromHours(1));
        Assert.False(MsQuicCertificateHelper.HasEphemeralPrivateKey(cert));
        Assert.Same(cert, MsQuicCertificateHelper.EnsurePersistedPrivateKey(cert));
    }

    [Fact]
    public void Reimport_yields_an_independent_persisted_copy()
    {
        using X509Certificate2 cert = TestCertificates.CreateSelfSigned("CN=t", TimeSpan.FromHours(1), ecdsa: false);
        using X509Certificate2 copy = MsQuicCertificateHelper.ReimportWithPersistedKey(cert);
        Assert.NotSame(cert, copy);
        Assert.True(copy.HasPrivateKey);
        Assert.Equal(cert.Thumbprint, copy.Thumbprint);
        Assert.False(MsQuicCertificateHelper.HasEphemeralPrivateKey(copy));
    }

    [Fact]
    public void Public_only_certificate_is_rejected_or_reported_as_not_ephemeral()
    {
        using X509Certificate2 withKey = TestCertificates.CreateSelfSigned("CN=t", TimeSpan.FromHours(1));
        using X509Certificate2 publicOnly = X509CertificateLoader.LoadCertificate(withKey.Export(X509ContentType.Cert));
        Assert.False(publicOnly.HasPrivateKey);
        Assert.False(MsQuicCertificateHelper.HasEphemeralPrivateKey(publicOnly));
        Assert.Throws<ArgumentException>(() => MsQuicCertificateHelper.EnsurePersistedPrivateKey(publicOnly));
        Assert.Throws<ArgumentNullException>(() => MsQuicCertificateHelper.EnsurePersistedPrivateKey(null!));
        Assert.Throws<ArgumentNullException>(() => MsQuicCertificateHelper.HasEphemeralPrivateKey(null!));
        Assert.Throws<ArgumentNullException>(() => MsQuicCertificateHelper.ReimportWithPersistedKey(null!));
    }

    [Fact]
    public void Server_configuration_accepts_an_ephemeral_certificate_by_persisting_it()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "CERTIFICATE_CONTEXT is Windows only");
        using var registration = new MsQuicRegistration();
        using X509Certificate2 ephemeral = CreateEphemeral(ecdsa: true);
        using MsQuicConfiguration config = MsQuicConfiguration.CreateServer(registration, ["x"], ephemeral, mode: MsQuicServerCredentialMode.CertificateContext);
        Assert.True(config.HasCredential);
        Assert.False(config.IndicatesPortableCertificate);
    }
}
