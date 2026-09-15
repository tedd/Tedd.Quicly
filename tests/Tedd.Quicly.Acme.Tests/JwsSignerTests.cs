using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Tedd.Quicly.Acme.Models;

namespace Tedd.Quicly.Acme.Tests;

public class JwsSignerTests
{
    private static readonly Uri Url = new("https://ca.test/acme/new-order");

    [Theory]
    [InlineData(AcmeKeyAlgorithm.ES256)]
    [InlineData(AcmeKeyAlgorithm.RS256)]
    public void Sign_WithJwk_ProducesVerifiableFlattenedJws(AcmeKeyAlgorithm algorithm)
    {
        using AcmeAccountKey key = AcmeAccountKey.Create(algorithm);
        byte[] payload = Encoding.UTF8.GetBytes("{\"a\":1}");
        JwsEnvelope jws = JwsSigner.Sign(key, Url, "nonce-1", payload, kid: null);

        using JsonDocument header = JsonDocument.Parse(Base64UrlCodec.Decode(jws.Protected));
        Assert.Equal(key.JwsAlgorithmName, header.RootElement.GetProperty("alg").GetString());
        Assert.Equal("nonce-1", header.RootElement.GetProperty("nonce").GetString());
        Assert.Equal(Url.AbsoluteUri, header.RootElement.GetProperty("url").GetString());
        Assert.True(header.RootElement.TryGetProperty("jwk", out JsonElement jwk));
        Assert.False(header.RootElement.TryGetProperty("kid", out _));
        Assert.Equal(key.ExportJwk().Kty, jwk.GetProperty("kty").GetString());

        Assert.Equal(payload, Base64UrlCodec.Decode(jws.Payload));
        byte[] input = JwsSigner.GetSigningInput(jws.Protected, jws.Payload);
        Assert.True(key.Verify(input, Base64UrlCodec.Decode(jws.Signature)));
    }

    [Fact]
    public void Sign_WithKid_OmitsJwk_AndPostAsGetHasEmptyPayload()
    {
        using AcmeAccountKey key = AcmeAccountKey.Create();
        Uri kid = new("https://ca.test/acme/acct/1");
        JwsEnvelope jws = JwsSigner.Sign(key, Url, "n", ReadOnlySpan<byte>.Empty, kid);
        Assert.Equal(string.Empty, jws.Payload);

        using JsonDocument header = JsonDocument.Parse(Base64UrlCodec.Decode(jws.Protected));
        Assert.Equal(kid.AbsoluteUri, header.RootElement.GetProperty("kid").GetString());
        Assert.False(header.RootElement.TryGetProperty("jwk", out _));
        Assert.True(key.Verify(JwsSigner.GetSigningInput(jws.Protected, string.Empty), Base64UrlCodec.Decode(jws.Signature)));
    }

    [Fact]
    public void SignToUtf8_SerializesEnvelope()
    {
        using AcmeAccountKey key = AcmeAccountKey.Create();
        byte[] bytes = JwsSigner.SignToUtf8(key, Url, "n", "{}"u8, null);
        using JsonDocument doc = JsonDocument.Parse(bytes);
        Assert.True(doc.RootElement.TryGetProperty("protected", out _));
        Assert.True(doc.RootElement.TryGetProperty("payload", out _));
        Assert.True(doc.RootElement.TryGetProperty("signature", out _));
        Assert.Equal(3, doc.RootElement.EnumerateObject().Count());
    }

    [Fact]
    public void Sign_ValidatesArguments()
    {
        using AcmeAccountKey key = AcmeAccountKey.Create();
        Assert.Throws<ArgumentNullException>(() => JwsSigner.Sign(null!, Url, "n", default, null));
        Assert.Throws<ArgumentNullException>(() => JwsSigner.Sign(key, null!, "n", default, null));
        Assert.Throws<ArgumentException>(() => JwsSigner.Sign(key, Url, "", default, null));
    }

    [Fact]
    public void ExternalAccountBinding_IsHs256OverAccountJwk()
    {
        using AcmeAccountKey key = AcmeAccountKey.Create();
        byte[] hmacKey = RandomNumberGenerator.GetBytes(32);
        ExternalAccountBinding eab = new("kid-123", Base64UrlCodec.Encode(hmacKey));
        Assert.Equal(hmacKey, eab.DecodeHmacKey());

        Uri newAccount = new("https://ca.test/acme/new-account");
        JwsEnvelope jws = JwsSigner.CreateExternalAccountBinding(key, eab, newAccount);

        using JsonDocument header = JsonDocument.Parse(Base64UrlCodec.Decode(jws.Protected));
        Assert.Equal("HS256", header.RootElement.GetProperty("alg").GetString());
        Assert.Equal("kid-123", header.RootElement.GetProperty("kid").GetString());
        Assert.Equal(newAccount.AbsoluteUri, header.RootElement.GetProperty("url").GetString());
        Assert.False(header.RootElement.TryGetProperty("nonce", out _));

        Jwk? payloadJwk = JsonSerializer.Deserialize(Base64UrlCodec.Decode(jws.Payload), AcmeJsonContext.Default.Jwk);
        Assert.Equal(key.ExportJwk(), payloadJwk);

        byte[] expected = HMACSHA256.HashData(hmacKey, JwsSigner.GetSigningInput(jws.Protected, jws.Payload));
        Assert.Equal(expected, Base64UrlCodec.Decode(jws.Signature));
    }

    [Fact]
    public void ExternalAccountBinding_ValidatesArguments()
    {
        using AcmeAccountKey key = AcmeAccountKey.Create();
        ExternalAccountBinding eab = new("k", "AA");
        Uri url = new("https://ca.test/new-account");
        Assert.Throws<ArgumentNullException>(() => JwsSigner.CreateExternalAccountBinding(null!, eab, url));
        Assert.Throws<ArgumentNullException>(() => JwsSigner.CreateExternalAccountBinding(key, null!, url));
        Assert.Throws<ArgumentNullException>(() => JwsSigner.CreateExternalAccountBinding(key, eab, null!));
    }
}
