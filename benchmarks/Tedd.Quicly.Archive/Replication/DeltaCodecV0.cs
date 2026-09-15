using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Archive.Replication;

/// <summary>
/// ARCHIVED V0 of <c>Tedd.Quicly.Replication.DeltaCodec</c>: the same wire format, scanned and copied one byte at a
/// time (no SIMD, no bulk copies). Kept for benchmark comparison only; see <c>docs/benchmarks/replication.md</c>.
/// </summary>
/// <remarks>
/// Format: <c>CurrentLength varint</c>, then ops <c>Skip varint, Count varint, Literal[Count]</c> where literals are
/// <c>current[i] ^ baseline'[i]</c> (baseline zero-extended / truncated to the current length); a literal run ends at
/// the first run of three zero XOR bytes; trailing unchanged bytes are implied.
/// </remarks>
public static class DeltaCodecV0
{
    private const int MinZeroRun = 3;

    /// <summary>Encodes <paramref name="current"/> against <paramref name="baseline"/>; -1 when the destination is too small.</summary>
    public static int Encode(ReadOnlySpan<byte> baseline, ReadOnlySpan<byte> current, Span<byte> destination)
    {
        int n = current.Length;
        int overlap = Math.Min(baseline.Length, n);
        if (!VarInt.TryWrite(destination, (ulong)n, out int position))
        {
            return -1;
        }

        int lastEnd = 0;
        int i = 0;
        while (true)
        {
            while (i < n && X(current, baseline, i, overlap) == 0)
            {
                i++;
            }

            if (i >= n)
            {
                return position;
            }

            int start = i;
            int end = start + 1;
            while (end < n && (X(current, baseline, end, overlap) != 0 || X(current, baseline, end + 1, overlap) != 0 || X(current, baseline, end + 2, overlap) != 0))
            {
                end++;
            }

            int count = end - start;
            if (VarInt.GetLength((ulong)(start - lastEnd)) + VarInt.GetLength((ulong)count) + count > destination.Length - position)
            {
                return -1;
            }

            position += VarInt.Write(destination.Slice(position), (ulong)(start - lastEnd));
            position += VarInt.Write(destination.Slice(position), (ulong)count);
            for (int k = start; k < end; k++)
            {
                destination[position++] = X(current, baseline, k, overlap);
            }

            lastEnd = end;
            i = end + MinZeroRun;
        }
    }

    /// <summary>Decodes a delta produced by <see cref="Encode"/>; -1 on malformed input or a too-small destination.</summary>
    public static int Decode(ReadOnlySpan<byte> baseline, ReadOnlySpan<byte> delta, Span<byte> destination)
    {
        if (!VarInt.TryReadMinimal(delta, out ulong length, out int input) || length > (ulong)destination.Length)
        {
            return -1;
        }

        int n = (int)length;
        int overlap = Math.Min(baseline.Length, n);
        int output = 0;
        while (input < delta.Length)
        {
            if (!VarInt.TryReadMinimal(delta.Slice(input), out ulong skip, out int consumed))
            {
                return -1;
            }

            input += consumed;
            if (!VarInt.TryReadMinimal(delta.Slice(input), out ulong count, out consumed))
            {
                return -1;
            }

            input += consumed;
            if (count == 0 || skip > (ulong)(n - output))
            {
                return -1;
            }

            for (int k = 0; k < (int)skip; k++, output++)
            {
                destination[output] = output < overlap ? baseline[output] : (byte)0;
            }

            if (count > (ulong)(n - output) || count > (ulong)(delta.Length - input))
            {
                return -1;
            }

            for (int k = 0; k < (int)count; k++, output++, input++)
            {
                destination[output] = (byte)(delta[input] ^ (output < overlap ? baseline[output] : 0));
            }
        }

        for (; output < n; output++)
        {
            destination[output] = output < overlap ? baseline[output] : (byte)0;
        }

        return n;
    }

    private static byte X(ReadOnlySpan<byte> current, ReadOnlySpan<byte> baseline, int i, int overlap) =>
        i < current.Length ? (byte)(current[i] ^ (i < overlap ? baseline[i] : 0)) : (byte)0;
}
