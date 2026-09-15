namespace Tedd.Quicly.Archive.Http3;

/// <summary>
/// V0 of the HPACK/QPACK Huffman decoder: a binary code tree walked one input bit at a time. Superseded by the
/// nibble-table decoder in <c>Tedd.Quicly.Http3.Qpack.HpackHuffman</c> (see docs/benchmarks/http3.md). Kept so
/// the benchmark keeps comparing the two.
/// </summary>
public static class HpackHuffmanV0
{
    public const int InvalidInput = -1;
    public const int DestinationTooSmall = -2;

    private static readonly short[] s_child0;
    private static readonly short[] s_child1;
    private static readonly short[] s_symbol;

    static HpackHuffmanV0()
    {
        ReadOnlySpan<uint> codes = HuffmanTableV0.Codes;
        ReadOnlySpan<byte> lengths = HuffmanTableV0.Lengths;
        const int MaxNodes = 2 * HuffmanTableV0.SymbolCount;
        s_child0 = new short[MaxNodes];
        s_child1 = new short[MaxNodes];
        s_symbol = new short[MaxNodes];
        Array.Fill(s_child0, (short)-1);
        Array.Fill(s_child1, (short)-1);
        Array.Fill(s_symbol, (short)-1);
        int nodeCount = 1;
        for (int s = 0; s < HuffmanTableV0.SymbolCount; s++)
        {
            uint code = codes[s];
            int len = lengths[s];
            int node = 0;
            for (int i = 0; i < len; i++)
            {
                int bit = (int)((code >> (31 - i)) & 1);
                short next = bit == 0 ? s_child0[node] : s_child1[node];
                if (next < 0)
                {
                    next = (short)nodeCount++;
                    if (bit == 0) s_child0[node] = next; else s_child1[node] = next;
                }
                node = next;
            }
            s_symbol[node] = (short)s;
        }
    }

    /// <summary>Bit-by-bit tree walk. Same contract as the shipping decoder.</summary>
    public static int Decode(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        short[] child0 = s_child0;
        short[] child1 = s_child1;
        short[] symbol = s_symbol;
        int node = 0;
        int pos = 0;
        int padBits = 0;
        bool padAllOnes = true;
        for (int i = 0; i < source.Length; i++)
        {
            byte b = source[i];
            for (int bit = 7; bit >= 0; bit--)
            {
                int v = (b >> bit) & 1;
                node = v == 0 ? child0[node] : child1[node];
                padBits++;
                if (v == 0) padAllOnes = false;
                int sym = symbol[node];
                if (sym >= 0)
                {
                    if (sym == HuffmanTableV0.Eos) return InvalidInput;
                    if (pos == destination.Length) return DestinationTooSmall;
                    destination[pos++] = (byte)sym;
                    node = 0;
                    padBits = 0;
                    padAllOnes = true;
                }
            }
        }
        if (padBits > 7 || !padAllOnes) return InvalidInput;
        return pos;
    }
}
