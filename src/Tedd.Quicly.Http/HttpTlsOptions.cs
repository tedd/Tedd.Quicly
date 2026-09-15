using System.Net.Security;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Http.Tls;

namespace Tedd.Quicly.Http;

/// <summary>
/// TLS configuration for one endpoint. The certificate for a handshake is resolved in this order:
/// <see cref="CertificateSelector"/> (SNI-aware), then <see cref="CertificateProvider"/>, then <see cref="CertificateSource"/>.
/// Whatever the sources return is read per handshake, so swapping a certificate takes effect immediately.
/// </summary>
public sealed class HttpTlsOptions
{
    private readonly List<SslApplicationProtocol> _protocols = [SslApplicationProtocol.Http11];
    private ServerCertificateSelectionCallback? _callback;

    /// <summary>Default certificate source (hot-swappable).</summary>
    public ICertificateSource? CertificateSource { get; set; }

    /// <summary>Certificate factory consulted when no <see cref="CertificateSelector"/> is set or it returns <see langword="null"/>.</summary>
    public Func<X509Certificate2?>? CertificateProvider { get; set; }

    /// <summary>SNI-aware selector, consulted first with the client's requested server name.</summary>
    public ICertificateSelector? CertificateSelector { get; set; }

    /// <summary>ALPN protocols offered for regular connections (default: <c>http/1.1</c>). Empty disables ALPN.</summary>
    public IList<SslApplicationProtocol> ApplicationProtocols => _protocols;

    /// <summary>When set, connections that offer ALPN <c>acme-tls/1</c> get the challenge certificate published for their SNI host.</summary>
    public TlsAlpn01Responder? Alpn01Responder { get; set; }

    /// <summary>TLS versions to enable; <see cref="SslProtocols.None"/> (default) lets the OS choose.</summary>
    public SslProtocols EnabledSslProtocols { get; set; } = SslProtocols.None;

    /// <summary>Creates options whose default certificate comes from <paramref name="source"/>.</summary>
    public static HttpTlsOptions FromSource(ICertificateSource source) => new() { CertificateSource = source };

    /// <summary>Creates options that always serve <paramref name="certificate"/>.</summary>
    public static HttpTlsOptions FromCertificate(X509Certificate2 certificate) => new() { CertificateSource = new StaticCertificateSource(certificate) };

    /// <summary>Resolves the certificate to present for <paramref name="serverName"/> (SNI; may be <see langword="null"/>).</summary>
    public X509Certificate2? ResolveCertificate(string? serverName)
    {
        var cert = CertificateSelector?.SelectCertificate(serverName);
        cert ??= CertificateProvider?.Invoke();
        cert ??= CertificateSource?.Current;
        return cert;
    }

    internal List<SslApplicationProtocol> ProtocolList => _protocols;

    internal ServerCertificateSelectionCallback SelectionCallback => _callback ??= SelectForSslStream;

    private X509Certificate SelectForSslStream(object sender, string? hostName)
        => ResolveCertificate(hostName) ?? throw new InvalidOperationException("No server certificate is configured for host '" + (hostName ?? string.Empty) + "'.");
}
