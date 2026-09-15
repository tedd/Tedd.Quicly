using System.Security.Cryptography.X509Certificates;

namespace Tedd.Quicly.Http.Tls;

/// <summary>Chooses a server certificate from the client's SNI server name.</summary>
public interface ICertificateSelector
{
    /// <summary>Returns the certificate for <paramref name="serverName"/> (<see langword="null"/> when the client sent no SNI), or <see langword="null"/> to fall back.</summary>
    X509Certificate2? SelectCertificate(string? serverName);
}
