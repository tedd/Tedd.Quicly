using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Archive.Replication;

/// <summary>
/// ARCHIVED V1 of <c>Tedd.Quicly.Replication.DeltaCodec</c> (Vector256 path): SIMD zero masks, but every op
/// recomputes them — one 32-byte mask to find the next change and two to find the end of the literal — and every
/// skip is a <c>Span.CopyTo</c>, every literal an exact-length XOR. Kept for benchmark comparison only; same format.
/// </summary>
public static class DeltaCodecV1
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

        ref byte cur = ref MemoryMarshal.GetReference(current);
        ref byte bas = ref MemoryMarshal.GetReference(baseline);
        ref byte dst = ref MemoryMarshal.GetReference(destination);
        int capacity = destination.Length;
        int lastEnd = 0;
        int scan = 0;
        while (true)
        {
            int start = FindNonZero(ref cur, ref bas, scan, overlap, n);
            if (start >= n)
            {
                return position;
            }

            int end = FindLiteralEnd(ref cur, ref bas, start + 1, overlap, n);
            int skip = start - lastEnd;
            int count = end - start;
            int skipLength = skip < 64 ? 1 : VarInt.GetLength((ulong)skip);
            int countLength = count < 64 ? 1 : VarInt.GetLength((ulong)count);
            if (skipLength + countLength + count > capacity - position)
            {
                return -1;
            }

            position += WriteVarInt(destination, position, skip);
            position += WriteVarInt(destination, position, count);
            XorRun(ref Unsafe.Add(ref dst, position), ref Unsafe.Add(ref cur, start), ref bas, start, count, Math.Max(0, Math.Min(end, overlap) - start));
            position += count;
            lastEnd = end;
            scan = end + MinZeroRun;
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
        int deltaLength = delta.Length;
        ref byte bas = ref MemoryMarshal.GetReference(baseline);
        ref byte dst = ref MemoryMarshal.GetReference(destination);
        ref byte src = ref MemoryMarshal.GetReference(delta);
        int output = 0;
        while (input < deltaLength)
        {
            ulong skip = Unsafe.Add(ref src, input);
            if (skip < 64)
            {
                input++;
            }
            else if (VarInt.TryReadMinimal(delta.Slice(input), out skip, out int consumed))
            {
                input += consumed;
            }
            else
            {
                return -1;
            }

            ulong count;
            if (input < deltaLength && Unsafe.Add(ref src, input) < 64)
            {
                count = Unsafe.Add(ref src, input);
                input++;
            }
            else if (VarInt.TryReadMinimal(delta.Slice(input), out count, out int consumed))
            {
                input += consumed;
            }
            else
            {
                return -1;
            }

            if (count == 0 || skip > (ulong)(n - output))
            {
                return -1;
            }

            CopyBaseline(baseline, destination, output, (int)skip, overlap);
            output += (int)skip;
            if (count > (ulong)(n - output) || count > (ulong)(deltaLength - input))
            {
                return -1;
            }

            int c = (int)count;
            XorRun(ref Unsafe.Add(ref dst, output), ref Unsafe.Add(ref src, input), ref bas, output, c, Math.Max(0, Math.Min(output + c, overlap) - output));
            output += c;
            input += c;
        }

        CopyBaseline(baseline, destination, output, n - output, overlap);
        return n;
    }

    private static int WriteVarInt(Span<byte> destination, int position, int value)
    {
        if (value < 64)
        {
            destination[position] = (byte)value;
            return 1;
        }

        return VarInt.Write(destination.Slice(position), (ulong)value);
    }

    private static void CopyBaseline(ReadOnlySpan<byte> baseline, Span<byte> destination, int position, int count, int overlap)
    {
        int fromBaseline = Math.Min(count, Math.Max(overlap - position, 0));
        if (fromBaseline > 0)
        {
            baseline.Slice(position, fromBaseline).CopyTo(destination.Slice(position));
        }

        if (count > fromBaseline)
        {
            destination.Slice(position + fromBaseline, count - fromBaseline).Clear();
        }
    }

    private static void XorRun(ref byte output, ref byte source, ref byte baseline, int baselineOffset, int count, int xorCount)
    {
        int k = 0;
        if (xorCount > 0)
        {
            ref byte bas = ref Unsafe.Add(ref baseline, baselineOffset);
            for (; k + 32 <= xorCount; k += 32)
            {
                (Vector256.LoadUnsafe(ref source, (nuint)k) ^ Vector256.LoadUnsafe(ref bas, (nuint)k)).StoreUnsafe(ref output, (nuint)k);
            }

            for (; k + 16 <= xorCount; k += 16)
            {
                (Vector128.LoadUnsafe(ref source, (nuint)k) ^ Vector128.LoadUnsafe(ref bas, (nuint)k)).StoreUnsafe(ref output, (nuint)k);
            }

            for (; k < xorCount; k++)
            {
                Unsafe.Add(ref output, k) = (byte)(Unsafe.Add(ref source, k) ^ Unsafe.Add(ref bas, k));
            }
        }

        if (k < count)
        {
            Unsafe.CopyBlockUnaligned(ref Unsafe.Add(ref output, k), ref Unsafe.Add(ref source, k), (uint)(count - k));
        }
    }

    private static int FindNonZero(ref byte cur, ref byte bas, int position, int overlap, int n)
    {
        while (position < n)
        {
            uint nonZero = ~ZeroMask(ref cur, ref bas, position, overlap, n);
            if (nonZero != 0)
            {
                return position + BitOperations.TrailingZeroCount(nonZero);
            }

            position += 32;
        }

        return n;
    }

    private static int FindLiteralEnd(ref byte cur, ref byte bas, int position, int overlap, int n)
    {
        uint low = ZeroMask(ref cur, ref bas, position, overlap, n);
        while (true)
        {
            uint high = ZeroMask(ref cur, ref bas, position + 32, overlap, n);
            ulong zeros = low | ((ulong)high << 32);
            uint runStarts = (uint)(zeros & (zeros >> 1) & (zeros >> 2));
            if (runStarts != 0)
            {
                return position + BitOperations.TrailingZeroCount(runStarts);
            }

            position += 32;
            low = high;
        }
    }

    private static uint ZeroMask(ref byte cur, ref byte bas, int position, int overlap, int n)
    {
        if (position + 32 <= overlap)
        {
            Vector256<byte> x = Vector256.LoadUnsafe(ref cur, (nuint)position) ^ Vector256.LoadUnsafe(ref bas, (nuint)position);
            return Vector256.Equals(x, Vector256<byte>.Zero).ExtractMostSignificantBits();
        }

        if (position >= overlap && position + 32 <= n)
        {
            return Vector256.Equals(Vector256.LoadUnsafe(ref cur, (nuint)position), Vector256<byte>.Zero).ExtractMostSignificantBits();
        }

        uint mask = 0;
        int limit = Math.Min(n - position, 32);
        int k = 0;
        for (; k < limit; k++)
        {
            int p = position + k;
            int x = Unsafe.Add(ref cur, p) ^ (p < overlap ? Unsafe.Add(ref bas, p) : 0);
            mask |= (x == 0 ? 1u : 0u) << k;
        }

        return k >= 32 ? mask : mask | (uint.MaxValue << Math.Max(k, 0));
    }
}
