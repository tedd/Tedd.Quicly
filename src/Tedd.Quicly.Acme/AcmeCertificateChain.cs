using System.Security.Cryptography.X509Certificates;

namespace Tedd.Quicly.Acme;

/// <summary>A downloaded <c>application/pem-certificate-chain</c> (RFC 8555 §7.4.2) plus any alternate chain links.</summary>
public sealed class AcmeCertificateChain : IDisposable
{
    internal AcmeCertificateChain(string pem, X509Certificate2Collection certificates, IReadOnlyList<Uri> alternateChainUrls)
    {
        Pem = pem;
        Certificates = certificates;
        AlternateChainUrls = alternateChainUrls;
    }

    /// <summary>The PEM exactly as served.</summary>
    public string Pem { get; }

    /// <summary>Parsed certificates: the end-entity certificate first, then the issuer chain.</summary>
    public X509Certificate2Collection Certificates { get; }

    /// <summary>The end-entity certificate.</summary>
    public X509Certificate2 Leaf => Certificates[0];

    /// <summary>URLs of alternate chains advertised through <c>Link: &lt;url&gt;;rel="alternate"</c>.</summary>
    public IReadOnlyList<Uri> AlternateChainUrls { get; }

    /// <summary>Disposes every parsed certificate.</summary>
    public void Dispose()
    {
        foreach (X509Certificate2 certificate in Certificates)
        {
            certificate.Dispose();
        }
    }
}
