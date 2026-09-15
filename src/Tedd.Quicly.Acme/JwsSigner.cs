using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using Tedd.Quicly.Acme.Models;

namespace Tedd.Quicly.Acme;

/// <summary>
/// External Account Binding credentials (RFC 8555 §7.3.4) issued by the CA (ZeroSSL, Google Trust Services, SSL.com, ...).
/// </summary>
/// <param name="KeyId">The CA-provided EAB key identifier (becomes the <c>kid</c> of the EAB JWS).</param>
/// <param name="HmacKey">The CA-provided HMAC key, base64url encoded.</param>
public sealed record ExternalAccountBinding(string KeyId, string HmacKey)
{
    /// <summary>Decodes <see cref="HmacKey"/> to raw bytes.</summary>
    public byte[] DecodeHmacKey() => Base64UrlCodec.Decode(HmacKey);
}

/// <summary>Builds ACME request JWS objects (RFC 8555 §6.2) in flattened JSON serialization.</summary>
public static class JwsSigner
{
    /// <summary>Signs a request payload with the account key.</summary>
    /// <param name="key">Account key.</param>
    /// <param name="url">Exact request URL (goes into the protected header).</param>
    /// <param name="nonce">Fresh anti-replay nonce.</param>
    /// <param name="payload">UTF-8 JSON payload; empty for POST-as-GET.</param>
    /// <param name="kid">Account URL; when <see langword="null"/> the public JWK is embedded instead (newAccount / revokeCert).</param>
    public static JwsEnvelope Sign(AcmeAccountKey key, Uri url, string nonce, ReadOnlySpan<byte> payload, Uri? kid)
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(url);
        ArgumentException.ThrowIfNullOrEmpty(nonce);

        JwsProtectedHeader header = new()
        {
            Alg = key.JwsAlgorithmName,
            Nonce = nonce,
            Url = url.AbsoluteUri,
            Jwk = kid is null ? key.ExportJwk() : null,
            Kid = kid?.AbsoluteUri,
        };
        byte[] headerBytes = JsonSerializer.SerializeToUtf8Bytes(header, AcmeJsonContext.Default.JwsProtectedHeader);
        return Assemble(headerBytes, payload, static (k, input) => k.Sign(input), key);
    }

    /// <summary>Serializes <see cref="Sign"/>'s result as the UTF-8 request body.</summary>
    public static byte[] SignToUtf8(AcmeAccountKey key, Uri url, string nonce, ReadOnlySpan<byte> payload, Uri? kid)
    {
        JwsEnvelope envelope = Sign(key, url, nonce, payload, kid);
        return JsonSerializer.SerializeToUtf8Bytes(envelope, AcmeJsonContext.Default.JwsEnvelope);
    }

    /// <summary>
    /// Builds the External Account Binding JWS (RFC 8555 §7.3.4): HS256 over the account's public JWK with the
    /// CA-provided HMAC key, <c>kid</c> = the CA-provided key identifier, <c>url</c> = the newAccount URL, no nonce.
    /// </summary>
    public static JwsEnvelope CreateExternalAccountBinding(AcmeAccountKey accountKey, ExternalAccountBinding binding, Uri newAccountUrl)
    {
        ArgumentNullException.ThrowIfNull(accountKey);
        ArgumentNullException.ThrowIfNull(binding);
        ArgumentNullException.ThrowIfNull(newAccountUrl);

        JwsProtectedHeader header = new() { Alg = "HS256", Kid = binding.KeyId, Url = newAccountUrl.AbsoluteUri };
        byte[] headerBytes = JsonSerializer.SerializeToUtf8Bytes(header, AcmeJsonContext.Default.JwsProtectedHeader);
        byte[] payload = JsonSerializer.SerializeToUtf8Bytes(accountKey.ExportJwk(), AcmeJsonContext.Default.Jwk);
        byte[] hmacKey = binding.DecodeHmacKey();
        return Assemble(headerBytes, payload, static (k, input) => HMACSHA256.HashData((byte[])k, input), hmacKey);
    }

    /// <summary>Returns the ASCII signing input <c>BASE64URL(header) || '.' || BASE64URL(payload)</c>.</summary>
    internal static byte[] GetSigningInput(string protectedB64, string payloadB64)
    {
        return Encoding.ASCII.GetBytes(string.Concat(protectedB64, ".", payloadB64));
    }

    private static JwsEnvelope Assemble<TKey>(byte[] headerBytes, ReadOnlySpan<byte> payload, Func<TKey, byte[], byte[]> sign, TKey key)
    {
        string protectedB64 = Base64UrlCodec.Encode(headerBytes);
        string payloadB64 = payload.IsEmpty ? string.Empty : Base64UrlCodec.Encode(payload);
        byte[] signature = sign(key, GetSigningInput(protectedB64, payloadB64));
        return new JwsEnvelope { Protected = protectedB64, Payload = payloadB64, Signature = Base64UrlCodec.Encode(signature) };
    }
}
