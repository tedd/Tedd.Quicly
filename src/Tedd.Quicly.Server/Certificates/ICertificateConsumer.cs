using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Http.Tls;

namespace Tedd.Quicly.Server.Certificates;

/// <summary>
/// Something that presents a server certificate and can switch to a new one without restarting: a QUIC listener (a new
/// MsQuic configuration selected for new connections), an HTTPS endpoint, a WebTransport host. Bind consumers to a
/// source with <see cref="CertificateBinder"/>.
/// </summary>
public interface ICertificateConsumer
{
    /// <summary>
    /// Starts presenting <paramref name="certificate"/> to new handshakes. The consumer must not dispose it: the source owns
    /// it, and <see cref="CertificateBinder"/> disposes a superseded certificate after its grace period. Throwing is
    /// allowed; the binder reports the failure and still updates the other consumers.
    /// </summary>
    void UpdateCertificate(X509Certificate2 certificate);
}

/// <summary>Ready-made <see cref="ICertificateConsumer"/> adapters.</summary>
public static class CertificateConsumers
{
    /// <summary>A consumer that calls <paramref name="update"/>, for example a listener's <c>UpdateCertificate</c> method group.</summary>
    public static ICertificateConsumer FromDelegate(Action<X509Certificate2> update)
    {
        ArgumentNullException.ThrowIfNull(update);
        return new DelegateConsumer(update);
    }

    /// <summary>
    /// A consumer that forwards every certificate into <paramref name="target"/>, typically the
    /// <see cref="Http.HttpTlsOptions.CertificateSource"/> of an HTTPS endpoint, which reads it per handshake.
    /// </summary>
    public static ICertificateConsumer FromSource(StaticCertificateSource target)
    {
        ArgumentNullException.ThrowIfNull(target);
        return new DelegateConsumer(target.Update);
    }

    private sealed class DelegateConsumer(Action<X509Certificate2> update) : ICertificateConsumer
    {
        public void UpdateCertificate(X509Certificate2 certificate) => update(certificate);
    }
}
