namespace Tedd.Quicly.Http3.Qpack;

/// <summary>Prefix-integer codec shared by HPACK and QPACK (RFC 7541 §5.1, RFC 9204 §4.1.1).</summary>
internal static class QpackInteger
{
    /// <summary>Largest value accepted on decode; larger encodings are rejected as malformed.</summary>
    public const ulong MaxValue = (1UL << 62) - 1;

    /// <summary>Encoded length of <paramref name="value"/> with an N-bit prefix.</summary>
    public static int GetLength(int prefixBits, ulong value)
    {
        ulong max = (1UL << prefixBits) - 1;
        if (value < max) return 1;
        value -= max;
        int n = 1;
        do
        {
            n++;
            value >>= 7;
        } while (value != 0);
        return n;
    }

    /// <summary>
    /// Writes <paramref name="value"/> with an N-bit prefix. <paramref name="highBits"/> supplies the bits above the
    /// prefix in the first byte (already positioned). Returns bytes written or -1 when the destination is too small.
    /// </summary>
    public static int Write(Span<byte> destination, int prefixBits, byte highBits, ulong value)
    {
        if (destination.IsEmpty) return -1;
        ulong max = (1UL << prefixBits) - 1;
        if (value < max)
        {
            destination[0] = (byte)(highBits | (byte)value);
            return 1;
        }
        destination[0] = (byte)(highBits | (byte)max);
        value -= max;
        int pos = 1;
        while (value >= 0x80)
        {
            if (pos == destination.Length) return -1;
            destination[pos++] = (byte)((value & 0x7F) | 0x80);
            value >>= 7;
        }
        if (pos == destination.Length) return -1;
        destination[pos++] = (byte)value;
        return pos;
    }

    /// <summary>
    /// Reads a prefix integer. Reports <see cref="QpackIntegerStatus.Truncated"/> when the input ends inside the
    /// integer and <see cref="QpackIntegerStatus.Overflow"/> when the value exceeds <see cref="MaxValue"/>
    /// (including over-long continuation sequences).
    /// </summary>
    public static QpackIntegerStatus TryRead(ReadOnlySpan<byte> source, int prefixBits, out ulong value, out int consumed)
    {
        value = 0;
        consumed = 0;
        if (source.IsEmpty) return QpackIntegerStatus.Truncated;
        ulong max = (1UL << prefixBits) - 1;
        value = (ulong)(source[0] & (byte)max);
        consumed = 1;
        if (value < max) return QpackIntegerStatus.Ok;

        int shift = 0;
        while (true)
        {
            if (consumed == source.Length)
            {
                value = 0;
                consumed = 0;
                return QpackIntegerStatus.Truncated;
            }
            byte b = source[consumed++];
            if (shift > 56)
            {
                value = 0;
                consumed = 0;
                return QpackIntegerStatus.Overflow;
            }
            value += (ulong)(b & 0x7F) << shift;
            if (value > MaxValue)
            {
                value = 0;
                consumed = 0;
                return QpackIntegerStatus.Overflow;
            }
            if ((b & 0x80) == 0) return QpackIntegerStatus.Ok;
            shift += 7;
        }
    }
}

/// <summary>Outcome of <see cref="QpackInteger.TryRead"/>.</summary>
internal enum QpackIntegerStatus : byte
{
    /// <summary>The integer was read.</summary>
    Ok = 0,
    /// <summary>The input ended inside the integer.</summary>
    Truncated,
    /// <summary>The value exceeds the supported range.</summary>
    Overflow,
}
