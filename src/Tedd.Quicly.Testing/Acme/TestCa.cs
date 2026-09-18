using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Acme.Models;

namespace Tedd.Quicly.Testing.Acme;

/// <summary>A self-signed in-test root CA that issues leaf certificates from CSRs.</summary>
/// <remarks>
/// <para>Every instance is a different root (a fresh key), so every instance also gets a different name: the name passed in
/// plus a random suffix. On Windows, a TLS endpoint that serves a certificate this CA issued (SslStream, through
/// <c>SslStreamCertificateContext</c>) finds that the OS cannot build the chain on its own and copies the root into the
/// "Intermediate Certification Authorities" store (CurrentUser, or LocalMachine when elevated), where it stays. With one
/// fixed name those copies piled up across test runs, and once more than 50 certificates share the issuer name of a
/// certificate without having signed it, Windows chain building fails outright ("An unknown chain building error
/// occurred"): the server-side credential cannot be built, Schannel ends QUIC handshakes with internal_error, and a
/// client's <c>X509Chain.Build</c> throws.</para>
/// <para><see cref="Dispose"/> removes this CA's root from those stores again, so a test run leaves nothing behind; a run
/// that dies before disposing leaves a copy whose unique name no later run ever looks up.</para>
/// </remarks>
public sealed class TestCa : IDisposable
{
    private readonly ECDsa _key;
    private long _serial = 1;

    /// <summary>Creates a root CA named <paramref name="name"/> plus a random suffix, with a fresh P-256 key, valid for ten years.</summary>
    public TestCa(string name)
    {
        Name = name + " " + Convert.ToHexString(RandomNumberGenerator.GetBytes(6));
        _key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        CertificateRequest request = new("CN=" + Name, _key, HashAlgorithmName.SHA256);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
        request.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(request.PublicKey, false));
        DateTimeOffset now = DateTimeOffset.UtcNow;
        Root = request.CreateSelfSigned(now.AddDays(-1), now.AddYears(10));
    }

    /// <summary>The common name of the root: the name given to the constructor plus a random suffix.</summary>
    public string Name { get; }

    /// <summary>The self-signed root certificate, with its private key.</summary>
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

    /// <summary>Removes the root from the stores SslStream copied it into (see the remarks), then releases it and its key.</summary>
    public void Dispose()
    {
        RemoveFromIntermediateStores(Root);
        Root.Dispose();
        _key.Dispose();
    }

    /// <summary>
    /// Removes <paramref name="certificate"/> from the CurrentUser and LocalMachine "Intermediate Certification Authorities"
    /// stores wherever it is present (Windows only). Best effort: a store that cannot be opened for writing (LocalMachine
    /// without elevation, where nothing could have been added either) is skipped.
    /// </summary>
    private static void RemoveFromIntermediateStores(X509Certificate2 certificate)
    {
        if (!OperatingSystem.IsWindows())
        {
            return;
        }

        foreach (StoreLocation location in (StoreLocation[])[StoreLocation.CurrentUser, StoreLocation.LocalMachine])
        {
            try
            {
                using X509Store store = new(StoreName.CertificateAuthority, location);
                store.Open(OpenFlags.ReadWrite | OpenFlags.OpenExistingOnly);
                store.Remove(certificate); // a certificate that is not in the store is ignored
            }
            catch (Exception e) when (e is CryptographicException or UnauthorizedAccessException)
            {
            }
        }
    }
}
