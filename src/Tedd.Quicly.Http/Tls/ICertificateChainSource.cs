using System.Security.Cryptography.X509Certificates;

namespace Tedd.Quicly.Http.Tls;

/// <summary>
/// Supplies the intermediate CA certificates that belong with a server certificate. Optional: an
/// <see cref="ICertificateSelector"/> or an <see cref="ICertificateSource"/> may implement it, and
/// <see cref="HttpTlsOptions"/> then sends what it returns with the certificate it resolved. A TLS handshake never
/// downloads a missing intermediate (that would stall it, and the stall lands inside the handshake timeout), so a
/// certificate whose issuers are not in the machine's stores needs this, or <see cref="HttpTlsOptions.AdditionalCertificates"/>.
/// </summary>
public interface ICertificateChainSource
{
    /// <summary>The intermediates of <paramref name="certificate"/> (it excluded), or <see langword="null"/> when unknown.</summary>
    /// <param name="certificate">The certificate that is about to be served.</param>
    /// <returns>The intermediates, or <see langword="null"/>.</returns>
    X509Certificate2Collection? GetIntermediates(X509Certificate2 certificate);
}
