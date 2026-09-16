using System.Net.Security;
using System.Runtime.CompilerServices;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Http.Tls;

namespace Tedd.Quicly.Http;

/// <summary>
/// TLS configuration for one endpoint. The certificate for a handshake is resolved in this order:
/// <see cref="CertificateSelector"/> (SNI-aware), then <see cref="CertificateProvider"/>, then <see cref="CertificateSource"/>.
/// Whatever the sources return is read per handshake, so swapping a certificate takes effect immediately; the credential of a
/// certificate (its chain) is built once per certificate instance and dropped with it.
/// </summary>
public sealed class HttpTlsOptions
{
    private readonly List<SslApplicationProtocol> _protocols = [SslApplicationProtocol.Http11];
    private readonly ConditionalWeakTable<X509Certificate2, SslStreamCertificateContext> _contexts = new();
    private readonly ConditionalWeakTable<X509Certificate2, SslStreamCertificateContext>.CreateValueCallback _createContext;
    private ServerCertificateSelectionCallback? _callback;
    private ServerOptionsSelectionCallback? _optionsCallback;
    private long _contextBuilds;

    /// <summary>Creates TLS options; set a certificate selector, provider or source before the endpoint starts.</summary>
    public HttpTlsOptions()
    {
        _createContext = CreateContext;
    }

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

    /// <summary>
    /// Intermediate CA certificates sent with a resolved certificate that no <see cref="ICertificateChainSource"/> describes.
    /// A handshake never downloads a missing intermediate (that would stall it), so a certificate whose issuers are not in the
    /// machine's stores needs them here, or from a selector or source that implements <see cref="ICertificateChainSource"/>.
    /// </summary>
    public X509Certificate2Collection? AdditionalCertificates { get; set; }

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

    internal ServerCertificateSelectionCallback SelectionCallback => _callback ??= SelectForSslStream;

    /// <summary>The per-handshake options of a regular connection: the certificate for the client's SNI host with its built credential.</summary>
    internal ServerOptionsSelectionCallback OptionsCallback => _optionsCallback ??= SelectOptions;

    /// <summary>Credentials built so far, one per certificate instance (tests).</summary>
    internal long CertificateContextBuilds => Interlocked.Read(ref _contextBuilds);

    /// <summary>The options of an <c>acme-tls/1</c> validation handshake: the published challenge certificate and that ALPN only.</summary>
    internal SslServerAuthenticationOptions CreateAcmeOptions(X509Certificate2 challengeCertificate) => new()
    {
        ServerCertificateContext = GetCertificateContext(challengeCertificate),
        ClientCertificateRequired = false,
        EnabledSslProtocols = EnabledSslProtocols,
        CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
        AllowRenegotiation = false,
        ApplicationProtocols = TlsAlpn01Responder.AcmeOnlyProtocols,
    };

    /// <summary>
    /// The credential of <paramref name="certificate"/>, built once per certificate instance: the chain build is the expensive
    /// part of a first handshake, and a swap simply misses this cache (its entry dies with the certificate). Built offline, so
    /// no handshake ever waits for a certificate download.
    /// </summary>
    internal SslStreamCertificateContext GetCertificateContext(X509Certificate2 certificate) => _contexts.GetValue(certificate, _createContext);

    private ValueTask<SslServerAuthenticationOptions> SelectOptions(SslStream stream, SslClientHelloInfo hello, object? state, CancellationToken cancellationToken)
    {
        string? serverName = string.IsNullOrEmpty(hello.ServerName) ? null : hello.ServerName;
        SslServerAuthenticationOptions options = new()
        {
            ServerCertificateContext = GetCertificateContext(ResolveCertificate(serverName) ?? throw NoCertificate(serverName)),
            ClientCertificateRequired = false,
            EnabledSslProtocols = EnabledSslProtocols,
            CertificateRevocationCheckMode = X509RevocationMode.NoCheck,
            AllowRenegotiation = false,
        };
        if (_protocols.Count > 0)
            options.ApplicationProtocols = _protocols;
        return ValueTask.FromResult(options);
    }

    private SslStreamCertificateContext CreateContext(X509Certificate2 certificate)
    {
        Interlocked.Increment(ref _contextBuilds);
        return SslStreamCertificateContext.Create(certificate, ResolveIntermediates(certificate), offline: true);
    }

    private X509Certificate2Collection? ResolveIntermediates(X509Certificate2 certificate)
    {
        if (CertificateSelector is ICertificateChainSource selector && selector.GetIntermediates(certificate) is { Count: > 0 } fromSelector)
            return fromSelector;
        if (CertificateSource is ICertificateChainSource source && source.GetIntermediates(certificate) is { Count: > 0 } fromSource)
            return fromSource;
        return AdditionalCertificates is { Count: > 0 } additional ? additional : null;
    }

    private X509Certificate SelectForSslStream(object sender, string? hostName)
        => ResolveCertificate(hostName) ?? throw NoCertificate(hostName);

    private static InvalidOperationException NoCertificate(string? hostName)
        => new("No server certificate is configured for host '" + (hostName ?? string.Empty) + "'.");
}
