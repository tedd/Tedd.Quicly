using System.Runtime.Versioning;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.RegularExpressions;
using Tedd.Quicly.Testing.Certificates;

namespace Tedd.Quicly.Testing.Tests.Certificates;

public class TestCertificatesTests
{
    private const string ServerAuthOid = "1.3.6.1.5.5.7.3.1";
    private const string P256Oid = "1.2.840.10045.3.1.7";

    [Fact]
    public void Ecdsa_certificate_has_a_p256_key_the_requested_validity_and_extensions()
    {
        DateTimeOffset start = DateTimeOffset.UtcNow;
        using X509Certificate2 cert = TestCertificates.CreateSelfSigned("CN=unit", TimeSpan.FromHours(2), ecdsa: true, "a.example", "b.example");

        Assert.Equal("CN=unit", cert.Subject);
        Assert.True(cert.HasPrivateKey);
        using ECDsa? key = cert.GetECDsaPrivateKey();
        Assert.NotNull(key);
        Assert.Equal(256, key.KeySize);
        Assert.Equal(P256Oid, key.ExportParameters(false).Curve.Oid.Value);

        // Not-before is backdated five minutes; the total period is the requested validity.
        Assert.True(cert.NotBefore.ToUniversalTime() <= start.UtcDateTime);
        Assert.InRange((cert.NotAfter - cert.NotBefore).TotalSeconds, TimeSpan.FromHours(2).TotalSeconds - 1, TimeSpan.FromHours(2).TotalSeconds + 1);

        X509SubjectAlternativeNameExtension san = Assert.Single(cert.Extensions.OfType<X509SubjectAlternativeNameExtension>());
        Assert.Equal(["a.example", "b.example"], san.EnumerateDnsNames().ToArray());
        X509EnhancedKeyUsageExtension eku = Assert.Single(cert.Extensions.OfType<X509EnhancedKeyUsageExtension>());
        Assert.Contains(eku.EnhancedKeyUsages.Cast<Oid>(), o => o.Value == ServerAuthOid);
        X509BasicConstraintsExtension basic = Assert.Single(cert.Extensions.OfType<X509BasicConstraintsExtension>());
        Assert.False(basic.CertificateAuthority);
        X509KeyUsageExtension usage = Assert.Single(cert.Extensions.OfType<X509KeyUsageExtension>());
        Assert.Equal(X509KeyUsageFlags.DigitalSignature, usage.KeyUsages);
    }

    [Fact]
    public void Rsa_certificate_and_certificate_without_names()
    {
        using X509Certificate2 rsa = TestCertificates.CreateSelfSigned("CN=rsa", TimeSpan.FromHours(1), ecdsa: false, "rsa.example");
        using RSA? key = rsa.GetRSAPrivateKey();
        Assert.NotNull(key);
        Assert.Equal(2048, key.KeySize);
        Assert.True(rsa.MatchesHostname("rsa.example"));
        Assert.Equal(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, Assert.Single(rsa.Extensions.OfType<X509KeyUsageExtension>()).KeyUsages);

        using X509Certificate2 bare = TestCertificates.CreateSelfSigned("CN=bare", TimeSpan.FromMinutes(10));
        Assert.Empty(bare.Extensions.OfType<X509SubjectAlternativeNameExtension>());
    }

    [Fact]
    public void WebTransport_dev_certificate_is_p256_and_valid_for_at_most_14_days()
    {
        using X509Certificate2 cert = TestCertificates.CreateWebTransportDevCertificate();
        Assert.True(cert.NotAfter - cert.NotBefore <= TimeSpan.FromDays(14));
        using ECDsa? key = cert.GetECDsaPrivateKey();
        Assert.Equal(P256Oid, key!.ExportParameters(false).Curve.Oid.Value);
        Assert.True(cert.MatchesHostname("localhost"));
        Assert.Equal("CN=localhost", cert.Subject);

        using X509Certificate2 custom = TestCertificates.CreateWebTransportDevCertificate("CN=dev", "dev.local", "alt.local");
        Assert.True(custom.MatchesHostname("dev.local"));
        Assert.True(custom.MatchesHostname("alt.local"));
        Assert.False(custom.MatchesHostname("localhost"));
    }

    [Fact]
    public void Fingerprint_is_the_sha256_of_the_der_certificate()
    {
        using X509Certificate2 cert = TestCertificates.CreateWebTransportDevCertificate();
        byte[] fingerprint = TestCertificates.Sha256Fingerprint(cert);
        Assert.Equal(SHA256.HashData(cert.RawData), fingerprint);
        string hex = TestCertificates.Sha256FingerprintHex(cert);
        Assert.Matches(new Regex("^([0-9A-F]{2}:){31}[0-9A-F]{2}$"), hex);
        Assert.Equal(Convert.ToHexString(fingerprint), hex.Replace(":", "", StringComparison.Ordinal));
    }

    [Fact]
    public void Arguments_are_validated()
    {
        Assert.Throws<ArgumentNullException>(() => TestCertificates.CreateSelfSigned(null!, TimeSpan.FromHours(1)));
        Assert.Throws<ArgumentException>(() => TestCertificates.CreateSelfSigned(" ", TimeSpan.FromHours(1)));
        Assert.Throws<ArgumentOutOfRangeException>(() => TestCertificates.CreateSelfSigned("CN=x", TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() => TestCertificates.CreateSelfSigned("CN=x", TimeSpan.FromSeconds(-1)));
        Assert.Throws<ArgumentNullException>(() => TestCertificates.CreateSelfSigned("CN=x", TimeSpan.FromHours(1), true, null!));
        Assert.Throws<ArgumentException>(() => TestCertificates.CreateSelfSigned("CN=x", TimeSpan.FromHours(1), false, "ok", ""));
        Assert.Throws<ArgumentNullException>(() => TestCertificates.CreateWebTransportDevCertificate("CN=x", null!));
        Assert.Throws<ArgumentNullException>(() => TestCertificates.Sha256Fingerprint(null!));
        Assert.Throws<ArgumentNullException>(() => TestCertificates.Sha256FingerprintHex(null!));
    }

    /// <summary>
    /// ADR 0006 (amended) / ADR 0009: no <c>Exportable</c>, no <c>EphemeralKeySet</c>. On Windows the key lives in a
    /// real CNG container that cannot be exported and is deleted again when the certificate is disposed.
    /// </summary>
    [Fact]
    public void Windows_keys_are_non_ephemeral_non_exportable_and_deleted_on_dispose()
    {
        if (!OperatingSystem.IsWindows())
        {
            Assert.Skip("CNG key containers exist only on Windows");
            return;
        }
        AssertWindowsKeyPosture(ecdsa: true);
        AssertWindowsKeyPosture(ecdsa: false);
    }

    [SupportedOSPlatform("windows")]
    private static void AssertWindowsKeyPosture(bool ecdsa)
    {
        X509Certificate2 cert = TestCertificates.CreateSelfSigned("CN=posture", TimeSpan.FromHours(1), ecdsa);
        string name;
        CngProvider provider;
        try
        {
            CngKey key;
            AsymmetricAlgorithm algorithm = ecdsa ? cert.GetECDsaPrivateKey()! : cert.GetRSAPrivateKey()!;
            using (algorithm)
            {
                key = algorithm switch
                {
                    ECDsaCng e => e.Key,
                    RSACng r => r.Key,
                    _ => throw new InvalidOperationException("expected a CNG key, got " + algorithm.GetType()),
                };
                Assert.False(key.IsEphemeral);
                Assert.Equal(CngExportPolicies.None, key.ExportPolicy & (CngExportPolicies.AllowExport | CngExportPolicies.AllowPlaintextExport));
                name = key.KeyName!;
                provider = key.Provider!;
            }
            Assert.True(CngKey.Exists(name, provider));
            Assert.ThrowsAny<CryptographicException>(() => cert.Export(X509ContentType.Pkcs12));
        }
        finally
        {
            cert.Dispose();
        }
        Assert.False(CngKey.Exists(name, provider), "disposing the certificate must delete its key container");
    }
}
