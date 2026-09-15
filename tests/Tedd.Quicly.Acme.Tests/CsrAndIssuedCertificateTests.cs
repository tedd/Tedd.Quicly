using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Acme.Models;
using Tedd.Quicly.Acme.Tests.Fake;

namespace Tedd.Quicly.Acme.Tests;

public class CsrAndIssuedCertificateTests
{
    [Theory]
    [InlineData(AcmeKeyAlgorithm.ES256)]
    [InlineData(AcmeKeyAlgorithm.RS256)]
    public void CreateCsr_ContainsDnsAndIpSans_AndVerifies(AcmeKeyAlgorithm algorithm)
    {
        using AsymmetricAlgorithm key = CsrBuilder.CreateKey(algorithm);
        AcmeIdentifier[] ids = [AcmeIdentifier.Dns("example.com"), AcmeIdentifier.Dns("www.example.com"), AcmeIdentifier.Ip("192.0.2.1"), AcmeIdentifier.Ip("2001:db8::1")];
        byte[] der = CsrBuilder.CreateCsr(ids, key);

        CertificateRequest parsed = CertificateRequest.LoadSigningRequest(der, HashAlgorithmName.SHA256, CertificateRequestLoadOptions.UnsafeLoadCertificateExtensions);
        Assert.Equal("CN=example.com", parsed.SubjectName.Name);
        X509SubjectAlternativeNameExtension san = parsed.CertificateExtensions.OfType<X509SubjectAlternativeNameExtension>().Single();
        Assert.Equal(["example.com", "www.example.com"], san.EnumerateDnsNames().ToArray());
        Assert.Equal([IPAddress.Parse("192.0.2.1"), IPAddress.Parse("2001:db8::1")], san.EnumerateIPAddresses().ToArray());
        Assert.Equal(algorithm == AcmeKeyAlgorithm.ES256 ? "1.2.840.10045.2.1" : "1.2.840.113549.1.1.1", parsed.PublicKey.Oid.Value);
    }

    [Fact]
    public void CreateCsr_ValidatesArguments()
    {
        using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        Assert.Throws<ArgumentNullException>(() => CsrBuilder.CreateCsr(null!, key));
        Assert.Throws<ArgumentNullException>(() => CsrBuilder.CreateCsr([AcmeIdentifier.Dns("a")], null!));
        Assert.Throws<ArgumentException>(() => CsrBuilder.CreateCsr([], key));
        Assert.Throws<ArgumentException>(() => CsrBuilder.CreateCsr([AcmeIdentifier.Ip("not-an-ip")], key));
        Assert.Throws<ArgumentException>(() => CsrBuilder.CreateCsr([new AcmeIdentifier("email", "a@b")], key));
        using DSA dsa = DSA.Create(1024);
        Assert.Throws<ArgumentException>(() => CsrBuilder.CreateCsr([AcmeIdentifier.Dns("a")], dsa));
        Assert.Throws<ArgumentOutOfRangeException>(() => CsrBuilder.CreateKey((AcmeKeyAlgorithm)7));
    }

    [Theory]
    [InlineData(AcmeKeyAlgorithm.ES256)]
    [InlineData(AcmeKeyAlgorithm.RS256)]
    public void KeyPem_RoundTrips(AcmeKeyAlgorithm algorithm)
    {
        using AsymmetricAlgorithm key = CsrBuilder.CreateKey(algorithm);
        string pem = CsrBuilder.ExportKeyPem(key);
        Assert.StartsWith("-----BEGIN PRIVATE KEY-----", pem, StringComparison.Ordinal);
        using AsymmetricAlgorithm imported = CsrBuilder.ImportKeyPem(pem);
        Assert.IsAssignableFrom(algorithm == AcmeKeyAlgorithm.ES256 ? typeof(ECDsa) : typeof(RSA), imported);
        Assert.Equal(key.ExportSubjectPublicKeyInfo(), imported.ExportSubjectPublicKeyInfo());

        using DSA dsa = DSA.Create(1024);
        Assert.Throws<ArgumentException>(() => CsrBuilder.ExportKeyPem(dsa));
        Assert.Throws<ArgumentNullException>(() => CsrBuilder.ExportKeyPem(null!));
        Assert.Throws<ArgumentException>(() => CsrBuilder.ImportKeyPem("not pem"));
        Assert.Throws<ArgumentException>(() => CsrBuilder.ImportKeyPem(""));
    }

    [Fact]
    public void Identifier_Helpers()
    {
        AcmeIdentifier dns = AcmeIdentifier.Dns("a.test");
        Assert.True(dns.IsDns);
        Assert.False(dns.IsIp);
        Assert.Equal("dns:a.test", dns.ToString());
        Assert.True(AcmeIdentifier.Ip("1.2.3.4").IsIp);
    }

    [Theory]
    [InlineData(AcmeKeyAlgorithm.ES256, null)]
    [InlineData(AcmeKeyAlgorithm.RS256, "s3cret")]
    public void IssuedCertificate_CombinesChainAndKey_RoundTripsThroughPfx(AcmeKeyAlgorithm algorithm, string? password)
    {
        using TestCa ca = new("Unit Root");
        using AsymmetricAlgorithm key = CsrBuilder.CreateKey(algorithm);
        AcmeIdentifier[] ids = [AcmeIdentifier.Dns("issued.test")];
        CertificateRequest csr = CertificateRequest.LoadSigningRequest(CsrBuilder.CreateCsr(ids, key), HashAlgorithmName.SHA256, CertificateRequestLoadOptions.UnsafeLoadCertificateExtensions);
        using X509Certificate2 leaf = ca.IssueFromCsr(csr, ids, DateTimeOffset.UtcNow.AddMinutes(-1), DateTimeOffset.UtcNow.AddDays(30));
        X509Certificate2Collection chain = [leaf, ca.Root];

        using IssuedCertificate issued = IssuedCertificate.Create(chain, key, password);
        Assert.True(issued.Certificate.HasPrivateKey);
        Assert.Equal(leaf.Thumbprint, issued.Certificate.Thumbprint);
        Assert.Equal(2, issued.Chain.Count);
        Assert.Equal(leaf.Thumbprint, issued.Chain[0].Thumbprint);
        Assert.Equal(ca.Root.Thumbprint, issued.Chain[1].Thumbprint);
        Assert.False(issued.Chain[0].HasPrivateKey);
        Assert.Equal(leaf.NotAfter.ToUniversalTime(), issued.NotAfter.UtcDateTime);
        Assert.Contains("BEGIN CERTIFICATE", issued.ExportChainPem());

        // Exportable private key (ADR 0006: not ephemeral).
        if (algorithm == AcmeKeyAlgorithm.ES256)
        {
            using ECDsa? priv = issued.Certificate.GetECDsaPrivateKey();
            Assert.NotNull(priv);
            Assert.NotEmpty(priv.ExportPkcs8PrivateKey());
        }
        else
        {
            using RSA? priv = issued.Certificate.GetRSAPrivateKey();
            Assert.NotNull(priv);
            Assert.NotEmpty(priv.ExportPkcs8PrivateKey());
        }

        string path = Path.Combine(Path.GetTempPath(), "quicly-acme-tests", Guid.NewGuid().ToString("N"), "cert.pfx");
        try
        {
            issued.Save(path);
            Assert.False(File.Exists(path + ".tmp"));
            using IssuedCertificate reloaded = IssuedCertificate.LoadFile(path, password);
            Assert.Equal(leaf.Thumbprint, reloaded.Certificate.Thumbprint);
            Assert.True(reloaded.Certificate.HasPrivateKey);
            Assert.Equal(2, reloaded.Chain.Count);
            issued.Save(path); // overwrite
        }
        finally
        {
            Directory.Delete(Path.GetDirectoryName(path)!, true);
        }
    }

    [Fact]
    public void IssuedCertificate_Create_ValidatesInput()
    {
        using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        Assert.Throws<ArgumentNullException>(() => IssuedCertificate.Create(null!, key));
        Assert.Throws<ArgumentNullException>(() => IssuedCertificate.Create([], null!));
        Assert.Throws<ArgumentException>(() => IssuedCertificate.Create([], key));
        Assert.Throws<ArgumentNullException>(() => IssuedCertificate.Load(null!));

        using TestCa ca = new("Unit Root");
        using DSA dsa = DSA.Create(1024);
        Assert.Throws<ArgumentException>(() => IssuedCertificate.Create([ca.Root], dsa));

        // Key does not match the certificate.
        using ECDsa other = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        Assert.ThrowsAny<Exception>(() => IssuedCertificate.Create([ca.Root], other));
    }

    [Fact]
    public void IssuedCertificate_Load_RejectsPfxWithoutPrivateKey()
    {
        using TestCa ca = new("Unit Root");
        using X509Certificate2 publicOnly = X509CertificateLoader.LoadCertificate(ca.Root.RawData);
        byte[] pfx = new X509Certificate2Collection(publicOnly).Export(X509ContentType.Pkcs12)!;
        Assert.Throws<ArgumentException>(() => IssuedCertificate.Load(pfx));
    }

    [Fact]
    public void IssuedCertificate_Save_ValidatesPath()
    {
        using TestCa ca = new("Unit Root");
        using X509Certificate2 leaf = ca.IssueLeaf("x.test", DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        using ECDsa key = leaf.GetECDsaPrivateKey()!;
        using X509Certificate2 pub = X509CertificateLoader.LoadCertificate(leaf.RawData);
        using IssuedCertificate issued = IssuedCertificate.Create([pub], key);
        Assert.Throws<ArgumentException>(() => issued.Save(""));
        Assert.Single(issued.Chain);
    }
}
