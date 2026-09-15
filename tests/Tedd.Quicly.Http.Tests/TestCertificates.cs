using System.Formats.Asn1;
using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace Tedd.Quicly.Http.Tests;

/// <summary>Self-signed ECDSA certificates with a persisted (SChannel-usable) private key.</summary>
internal static class TestCertificates
{
    public const string AcmeIdentifierOid = "1.3.6.1.5.5.7.1.31";

    public static X509Certificate2 CreateSelfSigned(string subject, params string[] dnsNames)
    {
        using var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest("CN=" + subject, key, HashAlgorithmName.SHA256);
        var san = new SubjectAlternativeNameBuilder();
        san.AddIpAddress(IPAddress.Loopback);
        foreach (var name in dnsNames)
            san.AddDnsName(name);
        request.CertificateExtensions.Add(san.Build());
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension([new Oid("1.3.6.1.5.5.7.3.1")], false));
        using var ephemeral = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));
        return Persist(ephemeral);
    }

    /// <summary>RFC 8737 challenge certificate: SAN = domain, critical acmeIdentifier extension with SHA-256(keyAuthorization).</summary>
    public static X509Certificate2 CreateAcmeChallenge(string domain, string keyAuthorization)
    {
        using var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest("CN=" + domain, key, HashAlgorithmName.SHA256);
        var san = new SubjectAlternativeNameBuilder();
        san.AddDnsName(domain);
        request.CertificateExtensions.Add(san.Build());
        var writer = new AsnWriter(AsnEncodingRules.DER);
        writer.WriteOctetString(SHA256.HashData(Encoding.ASCII.GetBytes(keyAuthorization)));
        request.CertificateExtensions.Add(new X509Extension(new Oid(AcmeIdentifierOid), writer.Encode(), critical: true));
        using var ephemeral = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(7));
        return Persist(ephemeral);
    }

    public static byte[] ExportPfx(X509Certificate2 certificate, string? password) => certificate.Export(X509ContentType.Pfx, password);

    private static X509Certificate2 Persist(X509Certificate2 ephemeral)
    {
        var pfx = ephemeral.Export(X509ContentType.Pfx);
        return X509CertificateLoader.LoadPkcs12(pfx, null, X509KeyStorageFlags.Exportable);
    }
}
