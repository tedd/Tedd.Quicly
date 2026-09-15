using System.Buffers.Text;

namespace Tedd.Quicly.Acme;

/// <summary>
/// Base64url (RFC 4648 §5, no padding) encoding used throughout JWS/JWK/ACME.
/// Thin wrapper over <see cref="Base64Url"/> so every call site shares one spelling and the
/// span-based overloads stay allocation-free.
/// </summary>
internal static class Base64UrlCodec
{
    /// <summary>Encodes <paramref name="data"/> as an unpadded base64url string.</summary>
    public static string Encode(ReadOnlySpan<byte> data) => Base64Url.EncodeToString(data);

    /// <summary>Encodes into <paramref name="destination"/> without allocating; returns the number of chars written.</summary>
    public static int Encode(ReadOnlySpan<byte> data, Span<char> destination) => Base64Url.EncodeToChars(data, destination);

    /// <summary>Returns the exact number of chars <see cref="Encode(ReadOnlySpan{byte}, Span{char})"/> will write.</summary>
    public static int GetEncodedLength(int byteCount) => Base64Url.GetEncodedLength(byteCount);

    /// <summary>Decodes an unpadded (or padded) base64url string.</summary>
    /// <exception cref="FormatException">The input is not valid base64url.</exception>
    public static byte[] Decode(ReadOnlySpan<char> text) => Base64Url.DecodeFromChars(text);
}
