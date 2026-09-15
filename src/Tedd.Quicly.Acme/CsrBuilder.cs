using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Acme.Models;

namespace Tedd.Quicly.Acme;

/// <summary>Builds PKCS#10 certificate signing requests for a set of ACME identifiers.</summary>
public static class CsrBuilder
{
    /// <summary>Creates a fresh certificate key: ECDSA P-256 or RSA (<paramref name="rsaKeySizeBits"/>, default 2048).</summary>
    public static AsymmetricAlgorithm CreateKey(AcmeKeyAlgorithm algorithm = AcmeKeyAlgorithm.ES256, int rsaKeySizeBits = 2048)
    {
        return algorithm switch
        {
            AcmeKeyAlgorithm.ES256 => ECDsa.Create(ECCurve.NamedCurves.nistP256),
            AcmeKeyAlgorithm.RS256 => RSA.Create(rsaKeySizeBits),
            _ => throw new ArgumentOutOfRangeException(nameof(algorithm)),
        };
    }

    /// <summary>Exports a certificate key created by <see cref="CreateKey"/> as PKCS#8 PEM (for order-state persistence).</summary>
    /// <exception cref="ArgumentException">The key is neither <see cref="ECDsa"/> nor <see cref="RSA"/>.</exception>
    public static string ExportKeyPem(AsymmetricAlgorithm key)
    {
        ArgumentNullException.ThrowIfNull(key);
        return key switch
        {
            ECDsa ecdsa => ecdsa.ExportPkcs8PrivateKeyPem(),
            RSA rsa => rsa.ExportPkcs8PrivateKeyPem(),
            _ => throw new ArgumentException("Only ECDsa and RSA keys are supported.", nameof(key)),
        };
    }

    /// <summary>Imports a PKCS#8 PEM (<c>PRIVATE KEY</c>) produced by <see cref="ExportKeyPem"/> as an <see cref="ECDsa"/> or <see cref="RSA"/> key.</summary>
    /// <exception cref="ArgumentException">The PEM does not contain a supported private key.</exception>
    public static AsymmetricAlgorithm ImportKeyPem(string pem)
    {
        using AcmeAccountKey wrapper = AcmeAccountKey.Import(pem);
        return wrapper.Algorithm == AcmeKeyAlgorithm.ES256 ? ImportEc(pem) : ImportRsa(pem);

        static ECDsa ImportEc(string pem)
        {
            ECDsa key = ECDsa.Create();
            key.ImportFromPem(pem);
            return key;
        }

        static RSA ImportRsa(string pem)
        {
            RSA key = RSA.Create();
            key.ImportFromPem(pem);
            return key;
        }
    }

    /// <summary>Upper bound of the subject common name (RFC 5280 <c>ub-common-name</c>); Boulder rejects longer CNs as <c>badCSR</c>.</summary>
    internal const int MaxCommonNameLength = 64;

    /// <summary>
    /// Creates a DER-encoded CSR whose subjectAltName lists every identifier (DNS names as dNSName, IP identifiers as
    /// iPAddress) and whose subject CN is the first DNS identifier of at most 64 characters (RFC 5280
    /// <c>ub-common-name</c>). When no DNS identifier fits (only long names, or an IP-only order) the subject is empty and
    /// the SAN extension is marked critical (RFC 5280 §4.2.1.6), as certbot and lego do. Signed with SHA-256.
    /// </summary>
    /// <exception cref="ArgumentException">No identifiers, an unsupported identifier type, an invalid IP, or an unsupported key type.</exception>
    public static byte[] CreateCsr(IReadOnlyList<AcmeIdentifier> identifiers, AsymmetricAlgorithm key)
    {
        ArgumentNullException.ThrowIfNull(identifiers);
        ArgumentNullException.ThrowIfNull(key);
        if (identifiers.Count == 0)
        {
            throw new ArgumentException("At least one identifier is required.", nameof(identifiers));
        }

        SubjectAlternativeNameBuilder san = new();
        for (int i = 0; i < identifiers.Count; i++)
        {
            AcmeIdentifier id = identifiers[i];
            if (id.IsDns)
            {
                san.AddDnsName(id.Value);
            }
            else if (id.IsIp)
            {
                if (!IPAddress.TryParse(id.Value, out IPAddress? ip))
                {
                    throw new ArgumentException("Invalid IP identifier '" + id.Value + "'.", nameof(identifiers));
                }

                san.AddIpAddress(ip);
            }
            else
            {
                throw new ArgumentException("Unsupported identifier type '" + id.Type + "'.", nameof(identifiers));
            }
        }

        string? commonName = SelectCommonName(identifiers);
        X500DistinguishedName subject;
        if (commonName is null)
        {
            subject = new X500DistinguishedName([0x30, 0x00]); // empty RDNSequence
        }
        else
        {
            X500DistinguishedNameBuilder dn = new();
            dn.AddCommonName(commonName);
            subject = dn.Build();
        }

        CertificateRequest request = key switch
        {
            ECDsa ecdsa => new CertificateRequest(subject, ecdsa, HashAlgorithmName.SHA256),
            RSA rsa => new CertificateRequest(subject, rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1),
            _ => throw new ArgumentException("Only ECDsa and RSA keys are supported.", nameof(key)),
        };

        // RFC 5280 §4.2.1.6: with an empty subject the subjectAltName extension MUST be critical.
        request.CertificateExtensions.Add(san.Build(critical: commonName is null));
        return request.CreateSigningRequest();
    }

    /// <summary>The first DNS identifier that fits in a CN (≤ 64 characters), or <see langword="null"/> when none does.</summary>
    internal static string? SelectCommonName(IReadOnlyList<AcmeIdentifier> identifiers)
    {
        for (int i = 0; i < identifiers.Count; i++)
        {
            if (identifiers[i].IsDns && identifiers[i].Value.Length <= MaxCommonNameLength)
            {
                return identifiers[i].Value;
            }
        }

        return null;
    }
}
