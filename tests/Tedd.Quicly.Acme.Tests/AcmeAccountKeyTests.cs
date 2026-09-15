using System.Security.Cryptography;
using System.Text;
using Tedd.Quicly.Acme.Models;

namespace Tedd.Quicly.Acme.Tests;

public class AcmeAccountKeyTests
{
    [Theory]
    [InlineData(AcmeKeyAlgorithm.ES256, "ES256")]
    [InlineData(AcmeKeyAlgorithm.RS256, "RS256")]
    public void Create_SignAndVerify_RoundTrips(AcmeKeyAlgorithm algorithm, string jwsAlg)
    {
        using AcmeAccountKey key = AcmeAccountKey.Create(algorithm);
        Assert.Equal(algorithm, key.Algorithm);
        Assert.Equal(jwsAlg, key.JwsAlgorithmName);
        Assert.True(key.HasPrivateKey);

        byte[] data = Encoding.ASCII.GetBytes("hello");
        byte[] signature = key.Sign(data);
        Assert.True(key.Verify(data, signature));
        Assert.False(key.Verify(Encoding.ASCII.GetBytes("hellp"), signature));

        if (algorithm == AcmeKeyAlgorithm.ES256)
        {
            Assert.Equal(64, signature.Length); // raw R||S, not DER
        }
        else
        {
            Assert.Equal(256, signature.Length); // 2048-bit PKCS#1 v1.5
        }
    }

    [Fact]
    public void Create_RejectsUnknownAlgorithm()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => AcmeAccountKey.Create((AcmeKeyAlgorithm)42));
    }

    [Fact]
    public void Constructors_ValidateKeySizes()
    {
        Assert.Throws<ArgumentException>(() => new AcmeAccountKey(ECDsa.Create(ECCurve.NamedCurves.nistP384)));
        Assert.Throws<ArgumentException>(() => new AcmeAccountKey(RSA.Create(1024)));
        Assert.Throws<ArgumentNullException>(() => new AcmeAccountKey((ECDsa)null!));
        Assert.Throws<ArgumentNullException>(() => new AcmeAccountKey((RSA)null!));
    }

    [Fact]
    public void Ec_Jwk_HasFixedWidthCoordinates_AndThumbprintIsStable()
    {
        using AcmeAccountKey key = AcmeAccountKey.Create();
        Jwk jwk = key.ExportJwk();
        Assert.Equal("EC", jwk.Kty);
        Assert.Equal("P-256", jwk.Crv);
        Assert.Equal(32, Base64UrlCodec.Decode(jwk.X!).Length);
        Assert.Equal(32, Base64UrlCodec.Decode(jwk.Y!).Length);
        Assert.Null(jwk.N);

        string canonical = "{\"crv\":\"P-256\",\"kty\":\"EC\",\"x\":\"" + jwk.X + "\",\"y\":\"" + jwk.Y + "\"}";
        string expected = Base64UrlCodec.Encode(SHA256.HashData(Encoding.UTF8.GetBytes(canonical)));
        Assert.Equal(expected, key.Thumbprint);
        Assert.Same(key.Thumbprint, key.Thumbprint); // cached
        Assert.Equal(32, key.ComputeThumbprint().Length);
    }

    [Fact]
    public void FixedLength_PadsShortCoordinates()
    {
        byte[] full = new byte[32];
        Assert.Same(full, AcmeAccountKey.FixedLength(full, 32));
        byte[] padded = AcmeAccountKey.FixedLength([0xAB, 0xCD], 4);
        Assert.Equal([0, 0, 0xAB, 0xCD], padded);
        Assert.Throws<ArgumentException>(() => AcmeAccountKey.FixedLength(new byte[5], 4));
    }

    [Fact]
    public void Rsa_Thumbprint_MatchesRfc7638Example()
    {
        // RFC 7638 §3.1 example key and thumbprint.
        Jwk jwk = new()
        {
            Kty = "RSA",
            E = "AQAB",
            N = "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw",
        };
        using AcmeAccountKey key = AcmeAccountKey.FromJwk(jwk);
        Assert.False(key.HasPrivateKey);
        Assert.Equal("NzbLsXh8uDCcd-6MNwXF4W_7noWXFZAfHkxZsRGC9Xs", key.Thumbprint);
        Assert.Throws<InvalidOperationException>(() => key.Sign([1, 2, 3]));
        Assert.Throws<InvalidOperationException>(() => key.ExportPem());
    }

    [Theory]
    [InlineData(AcmeKeyAlgorithm.ES256)]
    [InlineData(AcmeKeyAlgorithm.RS256)]
    public void FromJwk_VerifiesSignaturesOfOriginalKey(AcmeKeyAlgorithm algorithm)
    {
        using AcmeAccountKey key = AcmeAccountKey.Create(algorithm);
        using AcmeAccountKey publicOnly = AcmeAccountKey.FromJwk(key.ExportJwk());
        byte[] data = [1, 2, 3, 4];
        Assert.True(publicOnly.Verify(data, key.Sign(data)));
        Assert.Equal(key.Thumbprint, publicOnly.Thumbprint);
        Assert.Equal(algorithm, publicOnly.Algorithm);
    }

    [Fact]
    public void FromJwk_RejectsInvalidKeys()
    {
        Assert.Throws<ArgumentNullException>(() => AcmeAccountKey.FromJwk(null!));
        Assert.Throws<ArgumentException>(() => AcmeAccountKey.FromJwk(new Jwk { Kty = "oct" }));
        Assert.Throws<ArgumentException>(() => AcmeAccountKey.FromJwk(new Jwk { Kty = "EC", Crv = "P-384", X = "AA", Y = "AA" }));
        Assert.Throws<ArgumentException>(() => AcmeAccountKey.FromJwk(new Jwk { Kty = "EC", Crv = "P-256", X = "AA" }));
        Assert.Throws<ArgumentException>(() => AcmeAccountKey.FromJwk(new Jwk { Kty = "RSA", N = "AQAB" }));
    }

    [Theory]
    [InlineData(AcmeKeyAlgorithm.ES256)]
    [InlineData(AcmeKeyAlgorithm.RS256)]
    public void ExportPem_Import_RoundTrips(AcmeKeyAlgorithm algorithm)
    {
        using AcmeAccountKey key = AcmeAccountKey.Create(algorithm);
        string pem = key.ExportPem();
        Assert.StartsWith("-----BEGIN PRIVATE KEY-----", pem);
        using AcmeAccountKey imported = AcmeAccountKey.Import(pem);
        Assert.Equal(algorithm, imported.Algorithm);
        Assert.Equal(key.Thumbprint, imported.Thumbprint);
        Assert.True(key.Verify([9, 9], imported.Sign([9, 9])));
    }

    [Fact]
    public void Import_AcceptsLegacyLabels()
    {
        using ECDsa ec = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        using AcmeAccountKey ecKey = AcmeAccountKey.Import(ec.ExportECPrivateKeyPem());
        Assert.Equal(AcmeKeyAlgorithm.ES256, ecKey.Algorithm);

        using RSA rsa = RSA.Create(2048);
        using AcmeAccountKey rsaKey = AcmeAccountKey.Import(rsa.ExportRSAPrivateKeyPem());
        Assert.Equal(AcmeKeyAlgorithm.RS256, rsaKey.Algorithm);
    }

    [Fact]
    public void Import_RejectsUnsupportedInput()
    {
        Assert.Throws<ArgumentException>(() => AcmeAccountKey.Import(""));
        Assert.Throws<ArgumentException>(() => AcmeAccountKey.Import("not pem at all"));
        Assert.Throws<ArgumentException>(() => AcmeAccountKey.Import("-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----"));

        // PKCS#8 with an unsupported algorithm OID (Ed25519 = 1.3.101.112).
        using ECDsa ec = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        byte[] pkcs8 = ec.ExportPkcs8PrivateKey();
        System.Formats.Asn1.AsnWriter writer = new(System.Formats.Asn1.AsnEncodingRules.DER);
        using (writer.PushSequence())
        {
            writer.WriteInteger(0);
            using (writer.PushSequence())
            {
                writer.WriteObjectIdentifier("1.3.101.112");
            }

            writer.WriteOctetString(new byte[4]);
        }

        string pem = PemEncoding.WriteString("PRIVATE KEY", writer.Encode());
        Assert.Throws<ArgumentException>(() => AcmeAccountKey.Import(pem));
        Assert.NotEmpty(pkcs8);
    }
}
