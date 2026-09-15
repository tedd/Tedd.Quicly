using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Testing.Certificates;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic.Tests;

[Collection(MsQuicCollection.Name)]
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
    public void Ephemeral_keys_are_detected_and_re_imported_into_a_persisted_container(bool ecdsa)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "CNG ephemeral keys are a Windows concept");
        using X509Certificate2 ephemeral = CreateEphemeral(ecdsa);
        Assert.True(MsQuicCertificateHelper.HasEphemeralPrivateKey(ephemeral));
        Assert.False(MsQuicCertificateHelper.DeletePersistedPrivateKey(ephemeral));

        X509Certificate2 persisted = MsQuicCertificateHelper.EnsurePersistedPrivateKey(ephemeral);
        try
        {
            Assert.NotSame(ephemeral, persisted);
            Assert.True(persisted.HasPrivateKey);
            Assert.False(MsQuicCertificateHelper.HasEphemeralPrivateKey(persisted));
            Assert.Equal(ephemeral.Thumbprint, persisted.Thumbprint);
            // PersistKeySet | UserKeySet, never Exportable: the key cannot be exported again.
            Assert.False(MsQuicCertificateHelper.TryExportPkcs12(persisted, out _));
            Assert.Same(persisted, MsQuicCertificateHelper.EnsurePersistedPrivateKey(persisted));
        }
        finally
        {
            Assert.True(MsQuicCertificateHelper.DeletePersistedPrivateKey(persisted));
            persisted.Dispose();
        }
    }

    [Fact]
    public void Test_certificates_are_exportable_and_not_ephemeral()
    {
        using X509Certificate2 cert = TestCertificates.CreateSelfSigned("CN=t", TimeSpan.FromHours(1));
        Assert.False(MsQuicCertificateHelper.HasEphemeralPrivateKey(cert));
        Assert.Same(cert, MsQuicCertificateHelper.EnsurePersistedPrivateKey(cert));
        Assert.True(MsQuicCertificateHelper.TryExportPkcs12(cert, out byte[]? pfx));
        Assert.NotEmpty(pfx);
    }

    [Fact]
    public void Public_only_certificate_is_rejected_or_reported_as_not_ephemeral()
    {
        using X509Certificate2 withKey = TestCertificates.CreateSelfSigned("CN=t", TimeSpan.FromHours(1));
        using X509Certificate2 publicOnly = X509CertificateLoader.LoadCertificate(withKey.Export(X509ContentType.Cert));
        Assert.False(publicOnly.HasPrivateKey);
        Assert.False(MsQuicCertificateHelper.HasEphemeralPrivateKey(publicOnly));
        Assert.False(MsQuicCertificateHelper.TryExportPkcs12(publicOnly, out byte[]? none));
        Assert.Null(none);
        Assert.False(MsQuicCertificateHelper.DeletePersistedPrivateKey(publicOnly));
        Assert.Throws<ArgumentException>(() => MsQuicCertificateHelper.EnsurePersistedPrivateKey(publicOnly));
        Assert.Throws<ArgumentNullException>(() => MsQuicCertificateHelper.EnsurePersistedPrivateKey(null!));
        Assert.Throws<ArgumentNullException>(() => MsQuicCertificateHelper.HasEphemeralPrivateKey(null!));
        Assert.Throws<ArgumentNullException>(() => MsQuicCertificateHelper.ReimportWithPersistedKey(null!));
        Assert.Throws<ArgumentNullException>(() => MsQuicCertificateHelper.TryExportPkcs12(null!, out _));
        Assert.Throws<ArgumentNullException>(() => MsQuicCertificateHelper.DeletePersistedPrivateKey(null!));
    }

    [Fact]
    public void Delete_persisted_key_removes_a_reimported_container()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "key containers are a Windows concept");
        using X509Certificate2 cert = TestCertificates.CreateSelfSigned("CN=t", TimeSpan.FromHours(1), ecdsa: false);
        using X509Certificate2 copy = MsQuicCertificateHelper.ReimportWithPersistedKey(cert);
        Assert.NotSame(cert, copy);
        Assert.Equal(cert.Thumbprint, copy.Thumbprint);
        Assert.False(MsQuicCertificateHelper.HasEphemeralPrivateKey(copy));
        Assert.True(MsQuicCertificateHelper.DeletePersistedPrivateKey(copy));
    }

    [Fact]
    public void Server_configuration_accepts_an_ephemeral_certificate_in_auto_mode()
    {
        using var registrationScope = new TestRegistration();
        using X509Certificate2 ephemeral = CreateEphemeral(ecdsa: true);
        using MsQuicConfiguration config = MsQuicConfiguration.CreateServer(registrationScope.Registration, ["x"], ephemeral);
        Assert.True(config.HasCredential);
        // Schannel rejects PKCS#12, so Auto re-imports the ephemeral key into a persisted container and uses a context.
        bool schannel = MsQuicApi.Instance.TlsProvider == QUIC_TLS_PROVIDER.SCHANNEL;
        Assert.Equal(schannel ? QUIC_CREDENTIAL_TYPE.CERTIFICATE_CONTEXT : QUIC_CREDENTIAL_TYPE.CERTIFICATE_PKCS12, config.CredentialType);
        Assert.Equal(schannel, config.OwnedCertificate is not null);
        Assert.False(config.IndicatesPortableCertificate);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Certificate_context_path_persists_an_ephemeral_key_and_close_deletes_it(bool ecdsa)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "CERTIFICATE_CONTEXT is Windows only");
        using var registrationScope = new TestRegistration();
        using X509Certificate2 ephemeral = CreateEphemeral(ecdsa);
        MsQuicConfiguration config = MsQuicConfiguration.CreateServer(registrationScope.Registration, ["x"], ephemeral, mode: MsQuicServerCredentialMode.CertificateContext);
        Assert.Equal(QUIC_CREDENTIAL_TYPE.CERTIFICATE_CONTEXT, config.CredentialType);
        X509Certificate2? owned = config.OwnedCertificate;
        Assert.NotNull(owned);
        Assert.NotSame(ephemeral, owned);
        Assert.False(MsQuicCertificateHelper.HasEphemeralPrivateKey(owned));
        string container = PersistedKeyName(owned);
        Assert.True(KeyContainerExists(container), container);

        config.Close();
        Assert.Null(config.OwnedCertificate);
        Assert.False(KeyContainerExists(container), container);
    }

    [Fact]
    public void Certificate_context_path_uses_an_already_persisted_key_as_is()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "CERTIFICATE_CONTEXT is Windows only");
        using var registrationScope = new TestRegistration();
        using X509Certificate2 cert = TestCertificates.CreateSelfSigned("CN=persisted", TimeSpan.FromHours(1));
        using MsQuicConfiguration config = MsQuicConfiguration.CreateServer(registrationScope.Registration, ["x"], cert, mode: MsQuicServerCredentialMode.CertificateContext);
        Assert.Equal(QUIC_CREDENTIAL_TYPE.CERTIFICATE_CONTEXT, config.CredentialType);
        Assert.Null(config.OwnedCertificate);
    }

    private static bool KeyContainerExists(string name) => OperatingSystem.IsWindows() && CngKey.Exists(name);

    private static string PersistedKeyName(X509Certificate2 certificate)
    {
        if (!OperatingSystem.IsWindows()) throw new PlatformNotSupportedException();
        using ECDsa? ecdsa = certificate.GetECDsaPrivateKey();
        if (ecdsa is ECDsaCng ecdsaCng) return ecdsaCng.Key.KeyName!;
        using RSA? rsa = certificate.GetRSAPrivateKey();
        return ((RSACng)rsa!).Key.KeyName!;
    }

    [Fact]
    public void Test_certificate_factory_validates_and_builds_usable_certificates()
    {
        Assert.Throws<ArgumentException>(() => TestCertificates.CreateSelfSigned("", TimeSpan.FromHours(1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => TestCertificates.CreateSelfSigned("CN=x", TimeSpan.Zero));
        Assert.Throws<ArgumentNullException>(() => TestCertificates.CreateSelfSigned("CN=x", TimeSpan.FromHours(1), true, null!));
        Assert.Throws<ArgumentException>(() => TestCertificates.CreateSelfSigned("CN=x", TimeSpan.FromHours(1), true, " "));
        Assert.Throws<ArgumentNullException>(() => TestCertificates.CreateWebTransportDevCertificate("CN=x", null!));
        Assert.Throws<ArgumentNullException>(() => TestCertificates.Sha256Fingerprint(null!));

        using X509Certificate2 rsa = TestCertificates.CreateSelfSigned("CN=rsa", TimeSpan.FromHours(1), ecdsa: false, "a.example", "b.example");
        Assert.True(rsa.HasPrivateKey);
        Assert.NotNull(rsa.GetRSAPrivateKey());
        Assert.Contains("a.example", rsa.MatchesHostname("a.example") ? "a.example" : "", StringComparison.Ordinal);
        Assert.True(rsa.MatchesHostname("b.example"));

        using X509Certificate2 wt = TestCertificates.CreateWebTransportDevCertificate();
        Assert.True(wt.MatchesHostname("localhost"));
        Assert.True(wt.NotAfter - wt.NotBefore <= TimeSpan.FromDays(14));
        Assert.NotNull(wt.GetECDsaPrivateKey());
        byte[] fingerprint = TestCertificates.Sha256Fingerprint(wt);
        Assert.Equal(32, fingerprint.Length);
        string hex = TestCertificates.Sha256FingerprintHex(wt);
        Assert.Equal(32 * 3 - 1, hex.Length);
        Assert.Equal(Convert.ToHexString(fingerprint), hex.Replace(":", "", StringComparison.Ordinal));

        using X509Certificate2 custom = TestCertificates.CreateWebTransportDevCertificate("CN=dev", "dev.local");
        Assert.True(custom.MatchesHostname("dev.local"));
        Assert.False(custom.MatchesHostname("localhost"));
    }
}
