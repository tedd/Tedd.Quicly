using System.Security.Cryptography.X509Certificates;

namespace Tedd.Quicly.Http.Tls;

/// <summary>
/// Supplies the current server certificate and announces replacements. Consumers read <see cref="Current"/> per
/// handshake, so a swap takes effect without restarting any listener.
/// </summary>
public interface ICertificateSource
{
    /// <summary>The certificate to present now, or <see langword="null"/> when none is available yet.</summary>
    X509Certificate2? Current { get; }

    /// <summary>Raised after <see cref="Current"/> changed, with the new certificate.</summary>
    event Action<X509Certificate2>? Changed;
}
