namespace Tedd.Quicly.Http3.Qpack;

/// <summary>
/// HPACK/QPACK Huffman string codec (RFC 7541 §5.2, Appendix B). Encoding walks the code table; decoding is
/// table driven: a 256-state × 16-nibble transition table built once at type initialisation from the code table.
/// Neither path allocates.
/// </summary>
public static class HpackHuffman
{
    /// <summary>Return value of <see cref="Decode"/> when the input is not a valid Huffman string.</summary>
    public const int InvalidInput = -1;

    /// <summary>Return value of <see cref="Encode"/> / <see cref="Decode"/> when the destination is too small.</summary>
    public const int DestinationTooSmall = -2;

    // Transition entry layout: bits 0..7 next state, bits 8..15 emitted symbol, bit 16 emit, bit 17 accepting, bit 18 failure.
    private const uint EmitFlag = 1u << 16;
    private const uint AcceptFlag = 1u << 17;
    private const uint FailFlag = 1u << 18;

    private static readonly uint[] s_transitions = BuildTransitions();

    /// <summary>Returns the number of bytes <paramref name="source"/> occupies when Huffman encoded.</summary>
    public static int GetEncodedLength(ReadOnlySpan<byte> source)
    {
        ReadOnlySpan<byte> lengths = HuffmanTable.Lengths;
        ulong bits = 0;
        for (int i = 0; i < source.Length; i++)
        {
            bits += lengths[source[i]];
        }
        return (int)((bits + 7) >> 3);
    }

    /// <summary>
    /// Huffman-encodes <paramref name="source"/> into <paramref name="destination"/>, padding the final byte with the
    /// most significant bits of EOS. Returns the number of bytes written or <see cref="DestinationTooSmall"/>.
    /// </summary>
    public static int Encode(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        ReadOnlySpan<uint> codes = HuffmanTable.Codes;
        ReadOnlySpan<byte> lengths = HuffmanTable.Lengths;
        ulong acc = 0;
        int bits = 0;
        int pos = 0;
        for (int i = 0; i < source.Length; i++)
        {
            byte symbol = source[i];
            int len = lengths[symbol];
            acc = (acc << len) | (codes[symbol] >> (32 - len));
            bits += len;
            while (bits >= 8)
            {
                if (pos == destination.Length) return DestinationTooSmall;
                bits -= 8;
                destination[pos++] = (byte)(acc >> bits);
            }
        }
        if (bits > 0)
        {
            if (pos == destination.Length) return DestinationTooSmall;
            int pad = 8 - bits;
            destination[pos++] = (byte)((acc << pad) | ((1u << pad) - 1));
        }
        return pos;
    }

    /// <summary>
    /// Decodes a Huffman string. Returns the number of bytes written, <see cref="InvalidInput"/> when the input
    /// contains EOS, ends with more than 7 padding bits, or has padding that is not a prefix of EOS, and
    /// <see cref="DestinationTooSmall"/> when the destination cannot hold the output.
    /// </summary>
    public static int Decode(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        uint[] table = s_transitions;
        uint entry = AcceptFlag; // root state is accepting (empty input decodes to empty output)
        int state = 0;
        int pos = 0;
        for (int i = 0; i < source.Length; i++)
        {
            byte b = source[i];

            entry = table[(state << 4) | (b >> 4)];
            if ((entry & FailFlag) != 0) return InvalidInput;
            if ((entry & EmitFlag) != 0)
            {
                if (pos == destination.Length) return DestinationTooSmall;
                destination[pos++] = (byte)(entry >> 8);
            }
            state = (int)(entry & 0xFF);

            entry = table[(state << 4) | (b & 0x0F)];
            if ((entry & FailFlag) != 0) return InvalidInput;
            if ((entry & EmitFlag) != 0)
            {
                if (pos == destination.Length) return DestinationTooSmall;
                destination[pos++] = (byte)(entry >> 8);
            }
            state = (int)(entry & 0xFF);
        }
        return (entry & AcceptFlag) != 0 ? pos : InvalidInput;
    }

    /// <summary>
    /// Builds the nibble transition table from the code table. The binary code tree has 257 leaves and therefore
    /// exactly 256 internal nodes; each internal node is a decoder state. Because the shortest code is 5 bits, at
    /// most one symbol is emitted per 4-bit step.
    /// </summary>
    private static uint[] BuildTransitions()
    {
        ReadOnlySpan<uint> codes = HuffmanTable.Codes;
        ReadOnlySpan<byte> lengths = HuffmanTable.Lengths;

        // Tree nodes: index 0 = root. child[node,bit] = child index or -1. symbol[node] = leaf symbol or -1.
        const int MaxNodes = 2 * HuffmanTable.SymbolCount;
        int[] child0 = new int[MaxNodes];
        int[] child1 = new int[MaxNodes];
        int[] symbol = new int[MaxNodes];
        int[] onesDepth = new int[MaxNodes]; // depth if reachable from root via 1-bits only, else -1
        Array.Fill(child0, -1);
        Array.Fill(child1, -1);
        Array.Fill(symbol, -1);
        Array.Fill(onesDepth, -1);
        int nodeCount = 1;
        onesDepth[0] = 0;

        for (int s = 0; s < HuffmanTable.SymbolCount; s++)
        {
            uint code = codes[s];
            int len = lengths[s];
            int node = 0;
            bool allOnes = true;
            for (int i = 0; i < len; i++)
            {
                int bit = (int)((code >> (31 - i)) & 1);
                allOnes &= bit == 1;
                int next = bit == 0 ? child0[node] : child1[node];
                if (next < 0)
                {
                    next = nodeCount++;
                    if (bit == 0) child0[node] = next; else child1[node] = next;
                    if (allOnes) onesDepth[next] = i + 1;
                }
                node = next;
            }
            symbol[node] = s;
        }

        // Assign state ids to internal nodes (nodes with children) in tree order.
        int[] stateOf = new int[MaxNodes];
        Array.Fill(stateOf, -1);
        int stateCount = 0;
        for (int n = 0; n < nodeCount; n++)
        {
            if (symbol[n] < 0) stateOf[n] = stateCount++;
        }

        uint[] table = new uint[stateCount * 16];
        for (int n = 0; n < nodeCount; n++)
        {
            int state = stateOf[n];
            if (state < 0) continue;
            for (int nibble = 0; nibble < 16; nibble++)
            {
                int node = n;
                uint entry = 0;
                bool failed = false;
                for (int i = 3; i >= 0; i--)
                {
                    int bit = (nibble >> i) & 1;
                    node = bit == 0 ? child0[node] : child1[node];
                    if (symbol[node] >= 0)
                    {
                        if (symbol[node] == HuffmanTable.Eos)
                        {
                            failed = true;
                            break;
                        }
                        entry |= EmitFlag | ((uint)symbol[node] << 8);
                        node = 0;
                    }
                }
                if (failed)
                {
                    entry = FailFlag;
                }
                else
                {
                    entry |= (uint)stateOf[node];
                    if (onesDepth[node] >= 0 && onesDepth[node] <= 7) entry |= AcceptFlag;
                }
                table[(state << 4) | nibble] = entry;
            }
        }
        return table;
    }
}
