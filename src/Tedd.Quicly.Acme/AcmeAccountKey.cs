using System.Formats.Asn1;
using System.Security.Cryptography;
using System.Text;
using Tedd.Quicly.Acme.Models;

namespace Tedd.Quicly.Acme;

/// <summary>
/// An ACME account key: ECDSA P-256 (<c>ES256</c>, default) or RSA 2048+ (<c>RS256</c>).
/// Provides JWK export, the RFC 7638 thumbprint, JWS signing and PKCS#8 PEM persistence.
/// </summary>
public sealed class AcmeAccountKey : IDisposable
{
    private const string EcPublicKeyOid = "1.2.840.10045.2.1";
    private const string RsaEncryptionOid = "1.2.840.113549.1.1.1";

    private readonly ECDsa? _ecdsa;
    private readonly RSA? _rsa;
    private string? _thumbprint;

    private AcmeAccountKey(ECDsa ecdsa, bool hasPrivateKey)
    {
        _ecdsa = ecdsa;
        Algorithm = AcmeKeyAlgorithm.ES256;
        HasPrivateKey = hasPrivateKey;
    }

    private AcmeAccountKey(RSA rsa, bool hasPrivateKey)
    {
        _rsa = rsa;
        Algorithm = AcmeKeyAlgorithm.RS256;
        HasPrivateKey = hasPrivateKey;
    }

    /// <summary>Wraps an existing P-256 <see cref="ECDsa"/> key (ownership is transferred).</summary>
    /// <exception cref="ArgumentException">The key is not a 256-bit key.</exception>
    public AcmeAccountKey(ECDsa ecdsa)
        : this(ecdsa ?? throw new ArgumentNullException(nameof(ecdsa)), hasPrivateKey: true)
    {
        if (ecdsa.KeySize != 256)
        {
            throw new ArgumentException("ES256 requires a P-256 (256-bit) ECDsa key.", nameof(ecdsa));
        }
    }

    /// <summary>Wraps an existing <see cref="RSA"/> key of at least 2048 bits (ownership is transferred).</summary>
    /// <exception cref="ArgumentException">The key is shorter than 2048 bits.</exception>
    public AcmeAccountKey(RSA rsa)
        : this(rsa ?? throw new ArgumentNullException(nameof(rsa)), hasPrivateKey: true)
    {
        if (rsa.KeySize < 2048)
        {
            throw new ArgumentException("RS256 requires an RSA key of at least 2048 bits.", nameof(rsa));
        }
    }

    /// <summary>The key / signature algorithm.</summary>
    public AcmeKeyAlgorithm Algorithm { get; }

    /// <summary>True when the key can sign; false for keys created from a public JWK.</summary>
    public bool HasPrivateKey { get; }

    /// <summary>The JWS <c>alg</c> header value (<c>ES256</c> or <c>RS256</c>).</summary>
    public string JwsAlgorithmName => Algorithm == AcmeKeyAlgorithm.ES256 ? "ES256" : "RS256";

    /// <summary>Base64url-encoded RFC 7638 SHA-256 thumbprint of the public key (used in key authorizations).</summary>
    public string Thumbprint => _thumbprint ??= Base64UrlCodec.Encode(ComputeThumbprint());

    /// <summary>Generates a new random key.</summary>
    /// <param name="algorithm">Key algorithm; <see cref="AcmeKeyAlgorithm.ES256"/> by default.</param>
    /// <param name="rsaKeySizeBits">RSA key size (ignored for ES256); must be at least 2048.</param>
    public static AcmeAccountKey Create(AcmeKeyAlgorithm algorithm = AcmeKeyAlgorithm.ES256, int rsaKeySizeBits = 2048)
    {
        return algorithm switch
        {
            AcmeKeyAlgorithm.ES256 => new AcmeAccountKey(ECDsa.Create(ECCurve.NamedCurves.nistP256)),
            AcmeKeyAlgorithm.RS256 => new AcmeAccountKey(RSA.Create(rsaKeySizeBits)),
            _ => throw new ArgumentOutOfRangeException(nameof(algorithm)),
        };
    }

    /// <summary>Imports a key from PEM (<c>PRIVATE KEY</c> PKCS#8, <c>EC PRIVATE KEY</c> or <c>RSA PRIVATE KEY</c>).</summary>
    /// <exception cref="ArgumentException">The PEM does not contain a supported private key.</exception>
    public static AcmeAccountKey Import(string pem)
    {
        ArgumentException.ThrowIfNullOrEmpty(pem);
        if (!PemEncoding.TryFind(pem, out PemFields fields))
        {
            throw new ArgumentException("No PEM block found.", nameof(pem));
        }

        ReadOnlySpan<char> label = pem.AsSpan(fields.Label);
        bool isEc;
        if (label.SequenceEqual("EC PRIVATE KEY"))
        {
            isEc = true;
        }
        else if (label.SequenceEqual("RSA PRIVATE KEY"))
        {
            isEc = false;
        }
        else if (label.SequenceEqual("PRIVATE KEY"))
        {
            byte[] der = Convert.FromBase64String(pem[fields.Base64Data]);
            string oid = ReadPkcs8AlgorithmOid(der);
            isEc = oid switch
            {
                EcPublicKeyOid => true,
                RsaEncryptionOid => false,
                _ => throw new ArgumentException("Unsupported PKCS#8 key algorithm OID " + oid + ".", nameof(pem)),
            };
        }
        else
        {
            throw new ArgumentException("Unsupported PEM label '" + label.ToString() + "'.", nameof(pem));
        }

        if (isEc)
        {
            ECDsa ecdsa = ECDsa.Create();
            ecdsa.ImportFromPem(pem);
            return new AcmeAccountKey(ecdsa);
        }

        RSA rsa = RSA.Create();
        rsa.ImportFromPem(pem);
        return new AcmeAccountKey(rsa);
    }

    /// <summary>Creates a verification-only key from a public JWK (as sent in a JWS <c>jwk</c> header).</summary>
    /// <exception cref="ArgumentException">The JWK is not a P-256 EC key or an RSA key.</exception>
    public static AcmeAccountKey FromJwk(Jwk jwk)
    {
        ArgumentNullException.ThrowIfNull(jwk);
        if (string.Equals(jwk.Kty, "EC", StringComparison.Ordinal))
        {
            if (!string.Equals(jwk.Crv, "P-256", StringComparison.Ordinal) || jwk.X is null || jwk.Y is null)
            {
                throw new ArgumentException("EC JWK must use crv P-256 and carry x and y.", nameof(jwk));
            }

            ECParameters p = new()
            {
                Curve = ECCurve.NamedCurves.nistP256,
                Q = new ECPoint { X = Base64UrlCodec.Decode(jwk.X), Y = Base64UrlCodec.Decode(jwk.Y) },
            };
            return new AcmeAccountKey(ECDsa.Create(p), hasPrivateKey: false);
        }

        if (string.Equals(jwk.Kty, "RSA", StringComparison.Ordinal))
        {
            if (jwk.N is null || jwk.E is null)
            {
                throw new ArgumentException("RSA JWK must carry n and e.", nameof(jwk));
            }

            RSAParameters p = new() { Modulus = Base64UrlCodec.Decode(jwk.N), Exponent = Base64UrlCodec.Decode(jwk.E) };
            return new AcmeAccountKey(RSA.Create(p), hasPrivateKey: false);
        }

        throw new ArgumentException("Unsupported JWK kty '" + jwk.Kty + "'.", nameof(jwk));
    }

    /// <summary>Exports the private key as PKCS#8 PEM.</summary>
    /// <exception cref="InvalidOperationException">The key has no private component.</exception>
    public string ExportPem()
    {
        EnsurePrivateKey();
        return _ecdsa is not null ? _ecdsa.ExportPkcs8PrivateKeyPem() : _rsa!.ExportPkcs8PrivateKeyPem();
    }

    /// <summary>Exports the public key as a JWK.</summary>
    public Jwk ExportJwk()
    {
        if (_ecdsa is not null)
        {
            ECParameters p = _ecdsa.ExportParameters(false);
            return new Jwk
            {
                Kty = "EC",
                Crv = "P-256",
                X = Base64UrlCodec.Encode(FixedLength(p.Q.X!, 32)),
                Y = Base64UrlCodec.Encode(FixedLength(p.Q.Y!, 32)),
            };
        }

        RSAParameters r = _rsa!.ExportParameters(false);
        return new Jwk { Kty = "RSA", E = Base64UrlCodec.Encode(r.Exponent!), N = Base64UrlCodec.Encode(r.Modulus!) };
    }

    /// <summary>Computes the raw RFC 7638 SHA-256 thumbprint over the canonical JWK members.</summary>
    public byte[] ComputeThumbprint()
    {
        Jwk jwk = ExportJwk();
        // RFC 7638 §3: required members only, lexicographically ordered, no whitespace.
        string canonical = _ecdsa is not null
            ? "{\"crv\":\"P-256\",\"kty\":\"EC\",\"x\":\"" + jwk.X + "\",\"y\":\"" + jwk.Y + "\"}"
            : "{\"e\":\"" + jwk.E + "\",\"kty\":\"RSA\",\"n\":\"" + jwk.N + "\"}";
        return SHA256.HashData(Encoding.UTF8.GetBytes(canonical));
    }

    /// <summary>
    /// Signs <paramref name="data"/>: ES256 produces the raw 64-byte <c>R||S</c> form (RFC 7518 §3.4),
    /// RS256 produces a PKCS#1 v1.5 signature over SHA-256.
    /// </summary>
    /// <exception cref="InvalidOperationException">The key has no private component.</exception>
    public byte[] Sign(ReadOnlySpan<byte> data)
    {
        EnsurePrivateKey();
        return _ecdsa is not null
            ? _ecdsa.SignData(data, HashAlgorithmName.SHA256, DSASignatureFormat.IeeeP1363FixedFieldConcatenation)
            : _rsa!.SignData(data, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
    }

    /// <summary>Verifies a signature produced by <see cref="Sign"/> with the matching public key.</summary>
    public bool Verify(ReadOnlySpan<byte> data, ReadOnlySpan<byte> signature)
    {
        return _ecdsa is not null
            ? _ecdsa.VerifyData(data, signature, HashAlgorithmName.SHA256, DSASignatureFormat.IeeeP1363FixedFieldConcatenation)
            : _rsa!.VerifyData(data, signature, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
    }

    /// <inheritdoc/>
    public void Dispose()
    {
        _ecdsa?.Dispose();
        _rsa?.Dispose();
    }

    private void EnsurePrivateKey()
    {
        if (!HasPrivateKey)
        {
            throw new InvalidOperationException("This AcmeAccountKey only holds a public key.");
        }
    }

    /// <summary>Left-pads a big-endian field element to <paramref name="length"/> bytes (JWK requires the full field width).</summary>
    internal static byte[] FixedLength(byte[] value, int length)
    {
        if (value.Length == length)
        {
            return value;
        }

        if (value.Length > length)
        {
            throw new ArgumentException("Field element is longer than the field width.", nameof(value));
        }

        // Some providers strip leading zero bytes from coordinates.
        byte[] padded = new byte[length];
        value.AsSpan().CopyTo(padded.AsSpan(length - value.Length));
        return padded;
    }

    private static string ReadPkcs8AlgorithmOid(byte[] pkcs8Der)
    {
        AsnReader reader = new(pkcs8Der, AsnEncodingRules.DER);
        AsnReader privateKeyInfo = reader.ReadSequence();
        privateKeyInfo.ReadInteger(); // version
        AsnReader algorithmIdentifier = privateKeyInfo.ReadSequence();
        return algorithmIdentifier.ReadObjectIdentifier();
    }
}
