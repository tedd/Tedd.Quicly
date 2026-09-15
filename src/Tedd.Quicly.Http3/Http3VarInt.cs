using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace Tedd.Quicly.Http3;

/// <summary>
/// QUIC variable-length integer codec (RFC 9000 §16) used by every HTTP/3 structure.
/// </summary>
/// <remarks>
/// This is a minimal, self-contained copy kept internal to this assembly so that the HTTP/3 codec has no
/// dependency on the (concurrently developed) <c>Tedd.Quicly.Core</c> primitives. It is intended to be
/// replaced by the Core <c>VarInt</c> once that lands; the semantics are identical.
/// </remarks>
internal static class Http3VarInt
{
    /// <summary>Largest encodable value (2^62 - 1).</summary>
    public const ulong MaxValue = (1UL << 62) - 1;

    /// <summary>Largest encoded size in bytes.</summary>
    public const int MaxLength = 8;

    /// <summary>Returns the encoded length of <paramref name="value"/>, or -1 when it exceeds <see cref="MaxValue"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int GetLength(ulong value)
    {
        if (value < 0x40) return 1;
        if (value < 0x4000) return 2;
        if (value < 0x40000000) return 4;
        if (value <= MaxValue) return 8;
        return -1;
    }

    /// <summary>Returns the total encoded length implied by the first byte of a varint (1, 2, 4 or 8).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int GetLengthFromFirstByte(byte first) => 1 << (first >> 6);

    /// <summary>
    /// Writes <paramref name="value"/> into <paramref name="destination"/>.
    /// Returns the number of bytes written, or -1 when the value is too large or the destination is too small.
    /// </summary>
    public static int Write(Span<byte> destination, ulong value)
    {
        int len = GetLength(value);
        if (len < 0 || destination.Length < len) return -1;
        switch (len)
        {
            case 1:
                destination[0] = (byte)value;
                break;
            case 2:
                BinaryPrimitives.WriteUInt16BigEndian(destination, (ushort)(value | 0x4000));
                break;
            case 4:
                BinaryPrimitives.WriteUInt32BigEndian(destination, (uint)(value | 0x80000000));
                break;
            default:
                BinaryPrimitives.WriteUInt64BigEndian(destination, value | 0xC000000000000000);
                break;
        }
        return len;
    }

    /// <summary>
    /// Reads a varint from the start of <paramref name="source"/>.
    /// Returns false when the source is empty or truncated (nothing is consumed in that case).
    /// </summary>
    public static bool TryRead(ReadOnlySpan<byte> source, out ulong value, out int consumed)
    {
        if (source.IsEmpty)
        {
            value = 0;
            consumed = 0;
            return false;
        }
        int len = GetLengthFromFirstByte(source[0]);
        if (source.Length < len)
        {
            value = 0;
            consumed = 0;
            return false;
        }
        switch (len)
        {
            case 1:
                value = (ulong)(source[0] & 0x3F);
                break;
            case 2:
                value = (ulong)(BinaryPrimitives.ReadUInt16BigEndian(source) & 0x3FFF);
                break;
            case 4:
                value = BinaryPrimitives.ReadUInt32BigEndian(source) & 0x3FFFFFFF;
                break;
            default:
                value = BinaryPrimitives.ReadUInt64BigEndian(source) & 0x3FFFFFFFFFFFFFFF;
                break;
        }
        consumed = len;
        return true;
    }
}
