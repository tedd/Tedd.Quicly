namespace Tedd.Quicly.Http3.Qpack;

/// <summary>
/// Encodes a QPACK field section (RFC 9204 §4.5) using only the static table and literals, so the peer never
/// needs a dynamic table. Writes the prefix <c>0x00 0x00</c> (Required Insert Count 0, Base 0) and then one
/// field line per <see cref="TryWrite"/>: an Indexed Field Line for exact static matches, a Literal Field Line
/// with Name Reference for name-only matches and a Literal Field Line with Literal Name otherwise. Strings are
/// Huffman coded when that is shorter and Huffman is enabled. Nothing is allocated.
/// </summary>
public ref struct QpackEncoder
{
    /// <summary>Length of the field section prefix written by the constructor.</summary>
    public const int PrefixLength = 2;

    private readonly Span<byte> _destination;
    private readonly bool _huffman;
    private int _position;
    private bool _failed;

    /// <summary>Starts a field section in <paramref name="destination"/>.</summary>
    /// <param name="destination">Output buffer.</param>
    /// <param name="useHuffman">Huffman-code strings when that is shorter than the raw bytes.</param>
    public QpackEncoder(Span<byte> destination, bool useHuffman = true)
    {
        _destination = destination;
        _huffman = useHuffman;
        if (destination.Length < PrefixLength)
        {
            _failed = true;
            return;
        }
        destination[0] = 0;
        destination[1] = 0;
        _position = PrefixLength;
    }

    /// <summary>Bytes written so far (including the prefix).</summary>
    public readonly int BytesWritten => _position;

    /// <summary>True once a write did not fit; subsequent writes are rejected and the output is incomplete.</summary>
    public readonly bool HasFailed => _failed;

    /// <summary>True when Huffman coding is enabled.</summary>
    public readonly bool UseHuffman => _huffman;

    /// <summary>
    /// Appends one field line. Returns false when the destination is too small (the encoder is then failed).
    /// </summary>
    /// <param name="name">Field name (lower-case ASCII).</param>
    /// <param name="value">Field value.</param>
    /// <param name="neverIndex">Set the N bit so intermediaries never add the field to a dynamic table (sensitive values).</param>
    public bool TryWrite(scoped ReadOnlySpan<byte> name, scoped ReadOnlySpan<byte> value, bool neverIndex = false)
    {
        if (_failed) return false;
        Span<byte> dst = _destination.Slice(_position);
        int n;

        if (QpackStaticTable.TryFind(name, value, out int index, out bool exact))
        {
            if (exact)
            {
                // 1 T=1 index(6)
                n = QpackInteger.Write(dst, 6, 0xC0, (ulong)index);
                if (n < 0) return Fail();
                _position += n;
                return true;
            }

            // 01 N T=1 index(4)
            n = QpackInteger.Write(dst, 4, neverIndex ? (byte)0x70 : (byte)0x50, (ulong)index);
            if (n < 0) return Fail();
            int v = WriteString(dst.Slice(n), 7, 0x00, value);
            if (v < 0) return Fail();
            _position += n + v;
            return true;
        }

        // 001 N H length(3) name, H length(7) value
        n = WriteString(dst, 3, neverIndex ? (byte)0x30 : (byte)0x20, name);
        if (n < 0) return Fail();
        int vl = WriteString(dst.Slice(n), 7, 0x00, value);
        if (vl < 0) return Fail();
        _position += n + vl;
        return true;
    }

    private bool Fail()
    {
        _failed = true;
        return false;
    }

    /// <summary>Writes a string literal (H bit at bit <paramref name="prefixBits"/>, N-bit prefix length, bytes). Returns bytes written or -1.</summary>
    private readonly int WriteString(scoped Span<byte> dst, int prefixBits, byte highBits, scoped ReadOnlySpan<byte> s)
    {
        int raw = s.Length;
        if (_huffman)
        {
            int encoded = HpackHuffman.GetEncodedLength(s);
            if (encoded < raw)
            {
                int n = QpackInteger.Write(dst, prefixBits, (byte)(highBits | (1 << prefixBits)), (ulong)encoded);
                if (n < 0) return -1;
                int m = HpackHuffman.Encode(s, dst.Slice(n));
                if (m < 0) return -1;
                return n + m;
            }
        }
        int p = QpackInteger.Write(dst, prefixBits, highBits, (ulong)raw);
        if (p < 0 || dst.Length - p < raw) return -1;
        s.CopyTo(dst.Slice(p));
        return p + raw;
    }

    /// <summary>
    /// Encodes every field of <paramref name="headers"/> as one field section. Returns the number of bytes written
    /// or -1 when <paramref name="destination"/> is too small.
    /// </summary>
    public static int Encode(Http3HeaderCollection headers, Span<byte> destination, bool useHuffman = true)
    {
        ArgumentNullException.ThrowIfNull(headers);
        var encoder = new QpackEncoder(destination, useHuffman);
        for (int i = 0; i < headers.Count; i++)
        {
            if (!encoder.TryWrite(headers.GetName(i), headers.GetValue(i))) return -1;
        }
        return encoder.HasFailed ? -1 : encoder.BytesWritten;
    }
}
