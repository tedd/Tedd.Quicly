using System.Formats.Asn1;
using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;

namespace Tedd.Quicly.Acme.Challenges;

/// <summary>Helpers for the <c>tls-alpn-01</c> challenge (RFC 8737).</summary>
public static class TlsAlpn01Challenge
{
    /// <summary>ALPN protocol name the CA negotiates during validation.</summary>
    public const string AlpnProtocolName = "acme-tls/1";

    /// <summary>OID of the critical <c>id-pe-acmeIdentifier</c> extension (RFC 8737 §6.1).</summary>
    public const string AcmeIdentifierOid = "1.3.6.1.5.5.7.1.31";

    /// <summary>Returns SHA-256(keyAuthorization), the value carried in the acmeIdentifier extension.</summary>
    public static byte[] ComputeExpectedHash(string keyAuthorization)
    {
        ArgumentException.ThrowIfNullOrEmpty(keyAuthorization);
        return SHA256.HashData(Encoding.ASCII.GetBytes(keyAuthorization));
    }

    /// <summary>
    /// Creates the self-signed validation certificate: subjectAltName = <paramref name="identifier"/> (dNSName or iPAddress),
    /// plus the critical acmeIdentifier extension containing <c>OCTET STRING SHA-256(keyAuthorization)</c>.
    /// The certificate has an exportable private key (PKCS#12 round trip) so it can be used with Schannel / SslStream.
    /// </summary>
    public static X509Certificate2 CreateCertificate(string identifier, string keyAuthorization, TimeSpan? validity = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(identifier);
        byte[] hash = ComputeExpectedHash(keyAuthorization);

        AsnWriter writer = new(AsnEncodingRules.DER);
        writer.WriteOctetString(hash);
        byte[] extensionDer = writer.Encode();

        using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        CertificateRequest request = new(new X500DistinguishedName("CN=" + identifier), key, HashAlgorithmName.SHA256);
        SubjectAlternativeNameBuilder san = new();
        if (IPAddress.TryParse(identifier, out IPAddress? ip))
        {
            san.AddIpAddress(ip);
        }
        else
        {
            san.AddDnsName(identifier);
        }

        request.CertificateExtensions.Add(san.Build(critical: false));
        request.CertificateExtensions.Add(new X509Extension(new Oid(AcmeIdentifierOid), extensionDer, critical: true));
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, critical: false));

        DateTimeOffset now = DateTimeOffset.UtcNow;
        using X509Certificate2 ephemeral = request.CreateSelfSigned(now.AddMinutes(-5), now.Add(validity ?? TimeSpan.FromDays(7)));
        byte[] pfx = ephemeral.Export(X509ContentType.Pkcs12);
        return X509CertificateLoader.LoadPkcs12(pfx, null, X509KeyStorageFlags.Exportable);
    }

    /// <summary>
    /// Reads the acmeIdentifier extension back out of a validation certificate.
    /// Returns <see langword="false"/> when the extension is missing, not critical or not a 32-byte OCTET STRING.
    /// </summary>
    public static bool TryGetKeyAuthorizationHash(X509Certificate2 certificate, out byte[] hash)
    {
        ArgumentNullException.ThrowIfNull(certificate);
        hash = [];
        foreach (X509Extension ext in certificate.Extensions)
        {
            if (!string.Equals(ext.Oid?.Value, AcmeIdentifierOid, StringComparison.Ordinal))
            {
                continue;
            }

            if (!ext.Critical)
            {
                return false;
            }

            try
            {
                AsnReader reader = new(ext.RawData, AsnEncodingRules.DER);
                byte[] value = reader.ReadOctetString();
                reader.ThrowIfNotEmpty();
                if (value.Length != 32)
                {
                    return false;
                }

                hash = value;
                return true;
            }
            catch (AsnContentException)
            {
                return false;
            }
        }

        return false;
    }

    /// <summary>True when <paramref name="certificate"/> carries SHA-256(<paramref name="keyAuthorization"/>) in its acmeIdentifier extension.</summary>
    public static bool Matches(X509Certificate2 certificate, string keyAuthorization)
    {
        return TryGetKeyAuthorizationHash(certificate, out byte[] hash)
            && CryptographicOperations.FixedTimeEquals(hash, ComputeExpectedHash(keyAuthorization));
    }
}
