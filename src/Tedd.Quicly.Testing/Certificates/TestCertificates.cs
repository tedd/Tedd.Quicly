using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Tedd.Quicly.Testing.Certificates;

/// <summary>
/// Self-signed certificates for tests and development.
/// </summary>
/// <remarks>
/// <para>Every certificate returned here goes through a PKCS#12 round trip and is re-imported with
/// <see cref="X509KeyStorageFlags.Exportable"/> (never <c>EphemeralKeySet</c>): the key ends up in a real,
/// non-ephemeral key container that lives exactly as long as the certificate object (no <c>PersistKeySet</c>, so
/// disposing the certificate deletes it again), which is what MsQuic needs on Windows.</para>
/// <para>On Windows the msquic.dll bundled with .NET (2.5.10, Schannel) rejects
/// <c>QUIC_CREDENTIAL_TYPE_CERTIFICATE_PKCS12</c> with <c>QUIC_STATUS_NOT_SUPPORTED</c>, so the server credential
/// path that works is <c>QUIC_CREDENTIAL_TYPE_CERTIFICATE_CONTEXT</c>, and Schannel can only sign with a key in a
/// non-ephemeral container: these certificates are used through a certificate context as they are, with no
/// further re-import. On OpenSSL builds (Linux, macOS) the PKCS#12 path is taken, which re-exports the key — hence
/// <c>Exportable</c>. Certificates created directly with <c>CertificateRequest.CreateSelfSigned</c> carry an
/// ephemeral CNG key that Schannel cannot use; <c>MsQuicConfiguration</c> re-imports those itself.</para>
/// <para>Keep the certificate alive (undisposed) until every MsQuic configuration that loaded it is closed.</para>
/// </remarks>
public static class TestCertificates
{
    private static readonly Oid s_serverAuth = new("1.3.6.1.5.5.7.3.1");
    private static readonly Oid s_clientAuth = new("1.3.6.1.5.5.7.3.2");

    /// <summary>
    /// Creates a self-signed server certificate. Dispose the returned instance when done.
    /// </summary>
    /// <param name="subject">Distinguished name, e.g. <c>CN=localhost</c>.</param>
    /// <param name="validity">Total validity period (not-before is backdated by five minutes to absorb clock skew).</param>
    /// <param name="ecdsa">ECDSA P-256 when true (default), RSA 2048 otherwise.</param>
    /// <param name="dnsNames">Optional subject alternative DNS names.</param>
    public static X509Certificate2 CreateSelfSigned(string subject, TimeSpan validity, bool ecdsa = true, params string[] dnsNames)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(subject);
        ArgumentNullException.ThrowIfNull(dnsNames);
        if (validity <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(validity), "Validity must be positive.");

        DateTimeOffset notBefore = DateTimeOffset.UtcNow.AddMinutes(-5);
        DateTimeOffset notAfter = notBefore + validity;

        if (ecdsa)
        {
            using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
            var request = new CertificateRequest(subject, key, HashAlgorithmName.SHA256);
            AddExtensions(request, X509KeyUsageFlags.DigitalSignature, dnsNames);
            using X509Certificate2 ephemeral = request.CreateSelfSigned(notBefore, notAfter);
            return RoundTripThroughPfx(ephemeral);
        }
        else
        {
            using RSA key = RSA.Create(2048);
            var request = new CertificateRequest(subject, key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            AddExtensions(request, X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, dnsNames);
            using X509Certificate2 ephemeral = request.CreateSelfSigned(notBefore, notAfter);
            return RoundTripThroughPfx(ephemeral);
        }
    }

    /// <summary>
    /// Creates a certificate usable with WebTransport's <c>serverCertificateHashes</c>: ECDSA P-256 and a validity
    /// period of at most 14 days (13 days here). Pin it with <see cref="Sha256Fingerprint"/>.
    /// </summary>
    public static X509Certificate2 CreateWebTransportDevCertificate(string subject = "CN=localhost", params string[] dnsNames)
    {
        ArgumentNullException.ThrowIfNull(dnsNames);
        return CreateSelfSigned(subject, TimeSpan.FromDays(13), ecdsa: true, dnsNames.Length == 0 ? ["localhost"] : dnsNames);
    }

    /// <summary>SHA-256 fingerprint of the DER certificate (what WebTransport's <c>serverCertificateHashes</c> expects).</summary>
    public static byte[] Sha256Fingerprint(X509Certificate2 certificate)
    {
        ArgumentNullException.ThrowIfNull(certificate);
        return certificate.GetCertHash(HashAlgorithmName.SHA256);
    }

    /// <summary>Upper-case hex form of <see cref="Sha256Fingerprint"/>, colon separated (<c>AB:CD:...</c>).</summary>
    public static string Sha256FingerprintHex(X509Certificate2 certificate)
    {
        byte[] hash = Sha256Fingerprint(certificate);
        return string.Join(':', Array.ConvertAll(hash, static b => b.ToString("X2", System.Globalization.CultureInfo.InvariantCulture)));
    }

    private static void AddExtensions(CertificateRequest request, X509KeyUsageFlags keyUsage, string[] dnsNames)
    {
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(keyUsage, true));
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension([s_serverAuth, s_clientAuth], false));
        request.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(request.PublicKey, false));
        if (dnsNames.Length > 0)
        {
            var san = new SubjectAlternativeNameBuilder();
            foreach (string name in dnsNames)
            {
                ArgumentException.ThrowIfNullOrWhiteSpace(name, nameof(dnsNames));
                san.AddDnsName(name);
            }
            request.CertificateExtensions.Add(san.Build());
        }
    }

    private static X509Certificate2 RoundTripThroughPfx(X509Certificate2 ephemeral)
    {
        byte[] pfx = ephemeral.Export(X509ContentType.Pkcs12);
        try
        {
            return X509CertificateLoader.LoadPkcs12(pfx, null, X509KeyStorageFlags.Exportable);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(pfx);
        }
    }
}
