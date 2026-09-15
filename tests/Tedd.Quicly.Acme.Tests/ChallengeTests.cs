using System.Formats.Asn1;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Tedd.Quicly.Acme.Challenges;

namespace Tedd.Quicly.Acme.Tests;

public class ChallengeTests
{
    private const string Token = "evaGxfADs6pSRb2LAv9IZf17Dt3juxGJ-PCt92wr-oA";

    [Fact]
    public void KeyAuthorization_IsTokenDotThumbprint()
    {
        using AcmeAccountKey key = AcmeAccountKey.Create();
        Assert.Equal(Token + "." + key.Thumbprint, KeyAuthorization.Compute(Token, key));
        Assert.Throws<ArgumentException>(() => KeyAuthorization.Compute("", key));
        Assert.Throws<ArgumentException>(() => KeyAuthorization.Compute("tok-123", key)); // too short
        Assert.Throws<ArgumentNullException>(() => KeyAuthorization.Compute(Token, null!));
    }

    [Fact]
    public void Http01_PathAndToken()
    {
        Assert.Equal("/.well-known/acme-challenge/" + Token, Http01Challenge.GetPath(Token));
        Assert.Equal("text/plain", Http01Challenge.ContentType);
        Assert.Equal(Token, Http01Challenge.TryGetToken("/.well-known/acme-challenge/" + Token));
        Assert.Null(Http01Challenge.TryGetToken("/.well-known/acme-challenge/"));
        Assert.Null(Http01Challenge.TryGetToken("/.well-known/acme-challenge/abc")); // too short (ADR 0009)
        Assert.Null(Http01Challenge.TryGetToken("/.well-known/acme-challenge/" + Token + "/b"));
        Assert.Null(Http01Challenge.TryGetToken("/.well-known/acme-challenge/../" + Token));
        Assert.Null(Http01Challenge.TryGetToken("/index.html"));
        Assert.Null(Http01Challenge.TryGetToken(null!));
        Assert.Throws<ArgumentException>(() => Http01Challenge.GetPath(""));
        Assert.Throws<ArgumentException>(() => Http01Challenge.GetPath("abc"));
    }

    [Theory]
    [InlineData("evaGxfADs6pSRb2LAv9IZf", true)] // exactly 22
    [InlineData("evaGxfADs6pSRb2LAv9IZ", false)] // 21
    [InlineData("evaGxfADs6pSRb2LAv9IZf17Dt3juxGJ-PCt92wr-oA", true)]
    [InlineData("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_", true)]
    [InlineData("evaGxfADs6pSRb2LAv9IZf17Dt3juxGJ+PCt92wr-oA", false)] // '+' is not base64url
    [InlineData("evaGxfADs6pSRb2LAv9IZf17Dt3juxGJ/PCt92wr-oA", false)]
    [InlineData("evaGxfADs6pSRb2LAv9IZf17Dt3juxGJ-PCt92wr-oA=", false)] // padding
    [InlineData("evaGxfADs6pSRb2LAv9IZf17Dt3juxGJ PCt92wr-oA", false)]
    [InlineData("", false)]
    public void ChallengeToken_MatchesBase64UrlOfAtLeast22Chars(string token, bool valid)
    {
        Assert.Equal(valid, ChallengeToken.IsValid(token));
        if (valid)
        {
            ChallengeToken.Validate(token);
        }
        else
        {
            Assert.Throws<ArgumentException>(() => ChallengeToken.Validate(token));
        }

        Assert.Equal(22, ChallengeToken.MinimumLength);
        Assert.Throws<ArgumentNullException>(() => ChallengeToken.Validate(null!));
    }

    [Fact]
    public void Dns01_RecordNameAndValue()
    {
        Assert.Equal("_acme-challenge.example.com", Dns01Challenge.GetRecordName("example.com"));
        Assert.Equal("_acme-challenge.example.com", Dns01Challenge.GetRecordName("*.example.com"));
        Assert.Throws<ArgumentException>(() => Dns01Challenge.GetRecordName(""));
        Assert.Throws<ArgumentException>(() => Dns01Challenge.ComputeTxtValue(""));

        string keyAuth = "token.thumb";
        string expected = Base64UrlCodec.Encode(SHA256.HashData(Encoding.ASCII.GetBytes(keyAuth)));
        Assert.Equal(expected, Dns01Challenge.ComputeTxtValue(keyAuth));
        Assert.Equal(43, expected.Length);
    }

    [Fact]
    public void TlsAlpn01_CertificateCarriesCriticalAcmeIdentifierExtension_AndDnsSan()
    {
        string keyAuth = "abc.def";
        using X509Certificate2 cert = TlsAlpn01Challenge.CreateCertificate("game.example.com", keyAuth, TimeSpan.FromHours(1));

        Assert.True(cert.HasPrivateKey);
        Assert.True(cert.MatchesHostname("game.example.com"));
        Assert.Equal("CN=game.example.com", cert.Subject);
        Assert.Equal(cert.Subject, cert.Issuer);
        Assert.True(cert.NotAfter > DateTime.UtcNow.AddMinutes(30));

        X509Extension ext = cert.Extensions[TlsAlpn01Challenge.AcmeIdentifierOid]!;
        Assert.True(ext.Critical);
        byte[] octets = new AsnReader(ext.RawData, AsnEncodingRules.DER).ReadOctetString();
        Assert.Equal(SHA256.HashData(Encoding.ASCII.GetBytes(keyAuth)), octets);

        Assert.True(TlsAlpn01Challenge.TryGetKeyAuthorizationHash(cert, out byte[] hash));
        Assert.Equal(TlsAlpn01Challenge.ComputeExpectedHash(keyAuth), hash);
        Assert.True(TlsAlpn01Challenge.Matches(cert, keyAuth));
        Assert.False(TlsAlpn01Challenge.Matches(cert, "other.keyauth"));
        Assert.Equal("acme-tls/1", TlsAlpn01Challenge.AlpnProtocolName);

        // Exportable for Schannel (ADR 0006).
        Assert.NotEmpty(cert.Export(X509ContentType.Pkcs12));
    }

    [Fact]
    public void TlsAlpn01_IpIdentifier_GetsIpSan()
    {
        using X509Certificate2 cert = TlsAlpn01Challenge.CreateCertificate("192.0.2.10", "k.a");
        X509SubjectAlternativeNameExtension san = cert.Extensions.OfType<X509SubjectAlternativeNameExtension>().Single();
        Assert.Contains(System.Net.IPAddress.Parse("192.0.2.10"), san.EnumerateIPAddresses());
        Assert.Empty(san.EnumerateDnsNames());
    }

    [Fact]
    public void TlsAlpn01_TryGetHash_RejectsBadCertificates()
    {
        using ECDsa key = ECDsa.Create(ECCurve.NamedCurves.nistP256);

        // No extension at all.
        CertificateRequest plain = new("CN=x", key, HashAlgorithmName.SHA256);
        using X509Certificate2 none = plain.CreateSelfSigned(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddDays(1));
        Assert.False(TlsAlpn01Challenge.TryGetKeyAuthorizationHash(none, out _));

        // Not critical.
        AsnWriter w = new(AsnEncodingRules.DER);
        w.WriteOctetString(new byte[32]);
        CertificateRequest nonCritical = new("CN=x", key, HashAlgorithmName.SHA256);
        nonCritical.CertificateExtensions.Add(new X509Extension(new Oid(TlsAlpn01Challenge.AcmeIdentifierOid), w.Encode(), false));
        using X509Certificate2 nc = nonCritical.CreateSelfSigned(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddDays(1));
        Assert.False(TlsAlpn01Challenge.TryGetKeyAuthorizationHash(nc, out _));

        // Wrong length.
        AsnWriter w2 = new(AsnEncodingRules.DER);
        w2.WriteOctetString(new byte[16]);
        CertificateRequest shortHash = new("CN=x", key, HashAlgorithmName.SHA256);
        shortHash.CertificateExtensions.Add(new X509Extension(new Oid(TlsAlpn01Challenge.AcmeIdentifierOid), w2.Encode(), true));
        using X509Certificate2 sh = shortHash.CreateSelfSigned(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddDays(1));
        Assert.False(TlsAlpn01Challenge.TryGetKeyAuthorizationHash(sh, out _));

        // Not an OCTET STRING (a SEQUENCE instead).
        AsnWriter w3 = new(AsnEncodingRules.DER);
        using (w3.PushSequence())
        {
            w3.WriteInteger(1);
        }

        CertificateRequest badDer = new("CN=x", key, HashAlgorithmName.SHA256);
        badDer.CertificateExtensions.Add(new X509Extension(new Oid(TlsAlpn01Challenge.AcmeIdentifierOid), w3.Encode(), true));
        using X509Certificate2 bd = badDer.CreateSelfSigned(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddDays(1));
        Assert.False(TlsAlpn01Challenge.TryGetKeyAuthorizationHash(bd, out _));

        Assert.Throws<ArgumentNullException>(() => TlsAlpn01Challenge.TryGetKeyAuthorizationHash(null!, out _));
        Assert.Throws<ArgumentException>(() => TlsAlpn01Challenge.CreateCertificate("", "k"));
        Assert.Throws<ArgumentException>(() => TlsAlpn01Challenge.ComputeExpectedHash(""));
    }
}
