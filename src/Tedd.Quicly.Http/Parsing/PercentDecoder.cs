using System.Buffers;
using System.Text;
using System.Text.Unicode;

namespace Tedd.Quicly.Http.Parsing;

/// <summary>Percent-decodes a request path into a UTF-8 validated string.</summary>
internal static class PercentDecoder
{
    /// <summary>
    /// Decodes <paramref name="raw"/>. Fails on a malformed escape, an encoded NUL, or invalid UTF-8.
    /// </summary>
    public static bool TryDecode(ReadOnlySpan<byte> raw, out string decoded)
    {
        decoded = string.Empty;
        if (raw.IndexOf((byte)'%') < 0)
        {
            if (raw.IndexOfAnyInRange((byte)0x80, (byte)0xFF) < 0)
            {
                decoded = Encoding.ASCII.GetString(raw);
                return true;
            }
            return TryUtf8(raw, out decoded);
        }

        byte[]? rented = null;
        Span<byte> buffer = raw.Length <= 512 ? stackalloc byte[512] : (rented = ArrayPool<byte>.Shared.Rent(raw.Length));
        try
        {
            int n = 0;
            for (int i = 0; i < raw.Length; i++)
            {
                byte b = raw[i];
                if (b == (byte)'%')
                {
                    if (i + 2 >= raw.Length)
                        return false;
                    int hi = HexValue(raw[i + 1]);
                    int lo = HexValue(raw[i + 2]);
                    if (hi < 0 || lo < 0)
                        return false;
                    b = (byte)((hi << 4) | lo);
                    if (b == 0)
                        return false;
                    i += 2;
                }
                buffer[n++] = b;
            }
            return TryUtf8(buffer[..n], out decoded);
        }
        finally
        {
            if (rented is not null)
                ArrayPool<byte>.Shared.Return(rented);
        }
    }

    private static bool TryUtf8(ReadOnlySpan<byte> bytes, out string decoded)
    {
        if (!Utf8.IsValid(bytes))
        {
            decoded = string.Empty;
            return false;
        }
        decoded = Encoding.UTF8.GetString(bytes);
        return true;
    }

    private static int HexValue(byte c)
    {
        if ((uint)(c - '0') <= 9) return c - '0';
        if ((uint)(c - 'a') <= 5) return c - 'a' + 10;
        if ((uint)(c - 'A') <= 5) return c - 'A' + 10;
        return -1;
    }
}
