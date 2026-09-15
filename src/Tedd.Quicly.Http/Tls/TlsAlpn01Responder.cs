using System.Collections.Concurrent;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Acme;

namespace Tedd.Quicly.Http.Tls;

/// <summary>
/// Holds ACME <c>tls-alpn-01</c> challenge certificates per domain (RFC 8737). An <see cref="HttpTlsOptions"/> that
/// references this responder presents the challenge certificate only to clients that offer ALPN <c>acme-tls/1</c>
/// for a published SNI host; every other handshake gets the regular certificate.
/// </summary>
public sealed class TlsAlpn01Responder : ITlsAlpn01Responder
{
    /// <summary>The ALPN protocol id used by the challenge: <c>acme-tls/1</c>.</summary>
    public static readonly SslApplicationProtocol AcmeTls1 = new("acme-tls/1");

    /// <summary>OID of the <c>acmeIdentifier</c> certificate extension (<c>1.3.6.1.5.5.7.1.31</c>).</summary>
    public const string AcmeIdentifierOid = "1.3.6.1.5.5.7.1.31";

    internal static readonly List<SslApplicationProtocol> AcmeOnlyProtocols = [AcmeTls1];

    private readonly ConcurrentDictionary<string, X509Certificate2> _certificates = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Number of published challenge certificates.</summary>
    public int Count => _certificates.Count;

    /// <inheritdoc/>
    public ValueTask PublishAsync(string domain, X509Certificate2 certificate, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrEmpty(domain);
        ArgumentNullException.ThrowIfNull(certificate);
        _certificates[domain] = certificate;
        return default;
    }

    /// <inheritdoc/>
    public ValueTask RemoveAsync(string domain, CancellationToken ct)
    {
        ArgumentException.ThrowIfNullOrEmpty(domain);
        _certificates.TryRemove(domain, out _);
        return default;
    }

    /// <summary>Returns the published challenge certificate for <paramref name="domain"/>.</summary>
    public bool TryGetCertificate(string? domain, out X509Certificate2 certificate)
    {
        if (!string.IsNullOrEmpty(domain) && _certificates.TryGetValue(domain, out var c))
        {
            certificate = c;
            return true;
        }
        certificate = null!;
        return false;
    }
}
