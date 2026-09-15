using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Acme.Models;

namespace Tedd.Quicly.Testing.Acme;

/// <summary>A self-signed in-test root CA that issues leaf certificates from CSRs.</summary>
public sealed class TestCa : IDisposable
{
    private readonly ECDsa _key;
    private long _serial = 1;

    public TestCa(string name)
    {
        Name = name;
        _key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        CertificateRequest request = new("CN=" + name, _key, HashAlgorithmName.SHA256);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
        request.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(request.PublicKey, false));
        DateTimeOffset now = DateTimeOffset.UtcNow;
        Root = request.CreateSelfSigned(now.AddDays(-1), now.AddYears(10));
    }

    public string Name { get; }

    public X509Certificate2 Root { get; }

    /// <summary>Issues a certificate for the CSR's public key with the given SANs (no private key attached).</summary>
    public X509Certificate2 IssueFromCsr(CertificateRequest csr, IReadOnlyList<AcmeIdentifier> identifiers, DateTimeOffset notBefore, DateTimeOffset notAfter)
    {
        CertificateRequest request = new(csr.SubjectName, csr.PublicKey, HashAlgorithmName.SHA256);
        SubjectAlternativeNameBuilder san = new();
        foreach (AcmeIdentifier id in identifiers)
        {
            if (id.IsIp)
            {
                san.AddIpAddress(IPAddress.Parse(id.Value));
            }
            else
            {
                san.AddDnsName(id.Value);
            }
        }

        request.CertificateExtensions.Add(san.Build());
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, false));
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension([new Oid("1.3.6.1.5.5.7.3.1")], false));
        return Sign(request, notBefore, notAfter);
    }

    /// <summary>When false, issued certificates carry no Authority Key Identifier (so ARI cannot identify them).</summary>
    public bool IncludeAuthorityKeyIdentifier { get; set; } = true;

    private X509Certificate2 Sign(CertificateRequest request, DateTimeOffset notBefore, DateTimeOffset notAfter)
    {
        if (IncludeAuthorityKeyIdentifier)
        {
            request.CertificateExtensions.Add(X509AuthorityKeyIdentifierExtension.CreateFromCertificate(Root, includeKeyIdentifier: true, includeIssuerAndSerial: false));
        }

        // The signature-generator overload signs any subject key type (RSA or ECDSA) with the ECDSA root.
        byte[] serial = BitConverter.GetBytes(Interlocked.Increment(ref _serial));
        return request.Create(Root.SubjectName, X509SignatureGenerator.CreateForECDsa(_key), notBefore, notAfter, serial);
    }

    /// <summary>Issues a leaf certificate (with private key) directly, for tests that need a certificate with a given expiry.</summary>
    public X509Certificate2 IssueLeaf(string dnsName, DateTimeOffset notBefore, DateTimeOffset notAfter)
    {
        using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        CertificateRequest request = new("CN=" + dnsName, key, HashAlgorithmName.SHA256);
        SubjectAlternativeNameBuilder san = new();
        san.AddDnsName(dnsName);
        request.CertificateExtensions.Add(san.Build());
        using X509Certificate2 issued = Sign(request, notBefore, notAfter);
        using X509Certificate2 withKey = issued.CopyWithPrivateKey(key);
        return X509CertificateLoader.LoadPkcs12(withKey.Export(X509ContentType.Pkcs12), null, X509KeyStorageFlags.Exportable);
    }

    public void Dispose()
    {
        Root.Dispose();
        _key.Dispose();
    }
}
