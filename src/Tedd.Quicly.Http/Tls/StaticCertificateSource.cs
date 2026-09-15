using System.Security.Cryptography.X509Certificates;

namespace Tedd.Quicly.Http.Tls;

/// <summary>A certificate source holding one certificate that can be replaced explicitly.</summary>
public sealed class StaticCertificateSource : ICertificateSource
{
    private X509Certificate2? _current;

    /// <summary>Creates a source with an optional initial certificate.</summary>
    public StaticCertificateSource(X509Certificate2? certificate = null)
    {
        _current = certificate;
    }

    /// <inheritdoc/>
    public X509Certificate2? Current => Volatile.Read(ref _current);

    /// <inheritdoc/>
    public event Action<X509Certificate2>? Changed;

    /// <summary>Replaces the certificate and raises <see cref="Changed"/>.</summary>
    public void Update(X509Certificate2 certificate)
    {
        ArgumentNullException.ThrowIfNull(certificate);
        Volatile.Write(ref _current, certificate);
        Changed?.Invoke(certificate);
    }
}
