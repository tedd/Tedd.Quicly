using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Replication;

/// <summary>
/// Byte-level delta coding of a snapshot against a baseline: XOR against the baseline followed by zero-run-length
/// encoding, with an optional LZ4 stage (<see cref="Lz4Block"/>).
/// </summary>
/// <remarks>
/// <para><b>Format</b> (all integers are QUIC varints, RFC 9000 §16, minimal encoding):</para>
/// <code>
/// Delta   := CurrentLength varint, Op*
/// Op      := Skip varint, Count varint (≥ 1), Literal[Count]
/// </code>
/// <para>
/// Let <c>B'[i]</c> be <c>baseline[i]</c> for <c>i &lt; baseline.Length</c> and 0 beyond (a shorter baseline is
/// implicitly zero-extended; a longer one is truncated to <c>CurrentLength</c>), and <c>X[i] = current[i] ^ B'[i]</c>.
/// Decoding starts at position 0; each op first reproduces <c>Skip</c> bytes of <c>B'</c> unchanged, then writes
/// <c>Count</c> bytes <c>B'[i] ^ Literal[k]</c>. After the last op the remaining bytes up to <c>CurrentLength</c> are
/// <c>B'</c> unchanged. An empty baseline therefore produces a full snapshot (the literals are the current bytes),
/// and equal inputs encode to just the length header.
/// </para>
/// <para>
/// <b>Canonical encoder rule:</b> a literal run ends at the first run of at least <see cref="MinZeroRun"/> zero
/// <c>X</c> bytes or at the end of the input (shorter zero runs stay inside the literal: splitting would cost
/// two varints). Trailing zero <c>X</c> bytes are never encoded. With this rule the output never exceeds
/// <see cref="GetMaxEncodedLength"/>. The decoder accepts any well-formed op sequence (it does not require
/// the canonical split) but rejects non-minimal varints, <c>Count = 0</c>, ops that run past
/// <c>CurrentLength</c>, truncated literals and a <c>CurrentLength</c> larger than the destination.
/// </para>
/// <para>
/// <b>Compressed form</b> (<see cref="EncodeCompressed"/>): <c>RawLength varint</c> then either the plain delta
/// (<c>RawLength = 0</c>) or an LZ4 block that decodes to exactly <c>RawLength</c> bytes of delta — the same
/// convention as PROTOCOL.md §2.1. Compression is attempted only for deltas of at least
/// <see cref="MinCompressLength"/> bytes and kept only when it shrinks the output.
/// </para>
/// <para>
/// Scanning and XOR use <see cref="Vector256"/> / <see cref="Vector128"/> when hardware-accelerated, with a scalar
/// fallback; all paths produce identical output (see <c>docs/benchmarks/replication.md</c> for the measurements
/// against the archived V0 and V1). Every method is allocation-free (the compressed path uses
/// <see cref="Lz4Block"/>'s per-thread table, allocated once per thread) and never reads or writes outside the
/// given spans, whatever the input.
/// </para>
/// <para>
/// Pairing: the sender encodes against the newest snapshot the peer acknowledged
/// (<see cref="SnapshotHistory.TryGetBaseline"/>, or an empty baseline when there is none) and tells the receiver
/// which tick that was; the receiver keeps its own <see cref="SnapshotHistory"/> of decoded snapshots to find it.
/// </para>
/// </remarks>
public static class DeltaCodec
{
    /// <summary>Zero bytes needed to end a literal run in the canonical encoding.</summary>
    public const int MinZeroRun = 3;

    /// <summary>Deltas shorter than this are never LZ4-compressed by <see cref="EncodeCompressed"/>.</summary>
    public const int MinCompressLength = 64;

    /// <summary>Largest <c>currentLength</c> accepted by the size helpers.</summary>
    public const int MaxInputLength = 0x7000_0000;

    /// <summary>
    /// Upper bound on the size of <see cref="Encode"/>'s output for a current snapshot of
    /// <paramref name="currentLength"/> bytes: <c>currentLength + currentLength / 32 + 16</c>.
    /// </summary>
    /// <param name="currentLength">Length of the current snapshot.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="currentLength"/> is negative or above <see cref="MaxInputLength"/>.</exception>
    public static int GetMaxEncodedLength(int currentLength)
    {
        if ((uint)currentLength > MaxInputLength)
        {
            ThrowLength(currentLength);
        }

        // Header ≤ 8; the first op costs at most 2 + (skip + count) / 32 over its input bytes, every later op (skip ≥ 3)
        // at most count / 32 - 1 (varint(x) ≤ 1 + x / 32 and varint(skip) ≤ skip - 2), so the total is ≤ n + n / 32 + 10.
        return currentLength + (currentLength >> 5) + 16;
    }

    /// <summary>Upper bound on the size of <see cref="EncodeCompressed"/>'s output.</summary>
    /// <param name="currentLength">Length of the current snapshot.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="currentLength"/> is negative or above <see cref="MaxInputLength"/>.</exception>
    public static int GetMaxCompressedLength(int currentLength) => GetMaxEncodedLength(currentLength) + 1;

    /// <summary>Reads the <c>CurrentLength</c> header of a (non-compressed) delta.</summary>
    /// <param name="delta">The encoded delta.</param>
    /// <param name="length">The decoded length, or 0 on failure.</param>
    /// <returns><see langword="false"/> if the header is truncated, non-minimal or larger than <see cref="int.MaxValue"/>.</returns>
    public static bool TryGetDecodedLength(ReadOnlySpan<byte> delta, out int length)
    {
        if (VarInt.TryReadMinimal(delta, out ulong value, out _) && value <= int.MaxValue)
        {
            length = (int)value;
            return true;
        }

        length = 0;
        return false;
    }

    /// <summary>Encodes <paramref name="current"/> as a delta against <paramref name="baseline"/>.</summary>
    /// <param name="baseline">The baseline (may be empty: full snapshot; may be shorter or longer than <paramref name="current"/>).</param>
    /// <param name="current">The snapshot to encode.</param>
    /// <param name="destination">Receives the delta; <see cref="GetMaxEncodedLength"/> bytes always suffice. Must not overlap the inputs.</param>
    /// <returns>Bytes written, or <c>-1</c> if <paramref name="destination"/> is too small (its contents are then unspecified).</returns>
    public static int Encode(ReadOnlySpan<byte> baseline, ReadOnlySpan<byte> current, Span<byte> destination) =>
        Encode(PreferredWidth, baseline, current, destination);

    /// <summary>Reconstructs a snapshot from <paramref name="baseline"/> and a delta produced by <see cref="Encode"/>.</summary>
    /// <param name="baseline">The same baseline the delta was encoded against.</param>
    /// <param name="delta">The delta. Malformed input is rejected, never trusted.</param>
    /// <param name="destination">
    /// Receives the snapshot (must hold at least <c>CurrentLength</c> bytes, see <see cref="TryGetDecodedLength"/>).
    /// It may be the very same memory as <paramref name="baseline"/> (in-place update) but must not otherwise overlap
    /// <paramref name="baseline"/> or <paramref name="delta"/>. Bytes beyond <c>CurrentLength</c> are never written.
    /// </param>
    /// <returns>
    /// The snapshot length, or <c>-1</c> if the delta is malformed or the destination is too small (the destination's
    /// contents are then unspecified). Never throws and never touches memory outside the spans.
    /// </returns>
    public static int Decode(ReadOnlySpan<byte> baseline, ReadOnlySpan<byte> delta, Span<byte> destination) =>
        Decode(PreferredWidth, baseline, delta, destination);

    /// <summary>SIMD width the public entry points use: 256, 128 or 0 (scalar). A JIT-time constant.</summary>
    internal static int PreferredWidth => Vector256.IsHardwareAccelerated ? 256 : Vector128.IsHardwareAccelerated ? 128 : 0;

    /// <summary>Encodes with the code path for <paramref name="width"/> (folded away when the width is a constant).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int Encode(int width, ReadOnlySpan<byte> baseline, ReadOnlySpan<byte> current, Span<byte> destination) => width switch
    {
        >= 256 => Encode<Simd256>(baseline, current, destination),
        >= 128 => Encode<Simd128>(baseline, current, destination),
        _ => Encode<SimdNone>(baseline, current, destination),
    };

    /// <summary>Decodes with the code path for <paramref name="width"/> (folded away when the width is a constant).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int Decode(int width, ReadOnlySpan<byte> baseline, ReadOnlySpan<byte> delta, Span<byte> destination) => width switch
    {
        >= 256 => Decode<Simd256>(baseline, delta, destination),
        >= 128 => Decode<Simd128>(baseline, delta, destination),
        _ => Decode<SimdNone>(baseline, delta, destination),
    };

    /// <summary>Encodes a delta (as <see cref="Encode"/>) and then applies the optional LZ4 stage.</summary>
    /// <param name="baseline">The baseline.</param>
    /// <param name="current">The snapshot to encode.</param>
    /// <param name="destination">Receives <c>RawLength varint</c> + body; <see cref="GetMaxCompressedLength"/> bytes always suffice.</param>
    /// <param name="scratch">Work space for the uncompressed delta: at least <see cref="GetMaxEncodedLength"/> of <paramref name="current"/>.Length bytes.</param>
    /// <returns>Bytes written, or <c>-1</c> if <paramref name="destination"/> or <paramref name="scratch"/> is too small.</returns>
    public static int EncodeCompressed(ReadOnlySpan<byte> baseline, ReadOnlySpan<byte> current, Span<byte> destination, Span<byte> scratch)
    {
        int rawLength = Encode(baseline, current, scratch);
        if (rawLength < 0)
        {
            return -1;
        }

        ReadOnlySpan<byte> raw = scratch.Slice(0, rawLength);
        if (rawLength >= MinCompressLength)
        {
            int header = VarInt.GetLength((ulong)rawLength);

            // Allow the block at most rawLength - header bytes, so it is only kept when the total shrinks.
            int limit = Math.Min(destination.Length - header, rawLength - header);
            if (limit > 0)
            {
                int compressed = Lz4Block.Compress(raw, destination.Slice(header, limit));
                if (compressed > 0)
                {
                    VarInt.Write(destination, (ulong)rawLength);
                    return header + compressed;
                }
            }
        }

        if (destination.Length < rawLength + 1)
        {
            return -1;
        }

        destination[0] = 0;
        raw.CopyTo(destination.Slice(1));
        return rawLength + 1;
    }

    /// <summary>Decodes the output of <see cref="EncodeCompressed"/>.</summary>
    /// <param name="baseline">The baseline.</param>
    /// <param name="packet">The compressed delta.</param>
    /// <param name="destination">Receives the snapshot (same rules as <see cref="Decode"/>).</param>
    /// <param name="scratch">
    /// Work space for the decompressed delta; a <c>RawLength</c> larger than it is rejected, so size it with
    /// <see cref="GetMaxEncodedLength"/> of the largest snapshot you accept. Must not overlap the other spans.
    /// </param>
    /// <returns>The snapshot length, or <c>-1</c> on malformed input or insufficient space. Never throws.</returns>
    public static int DecodeCompressed(ReadOnlySpan<byte> baseline, ReadOnlySpan<byte> packet, Span<byte> destination, Span<byte> scratch)
    {
        if (!VarInt.TryReadMinimal(packet, out ulong rawLength, out int header))
        {
            return -1;
        }

        ReadOnlySpan<byte> body = packet.Slice(header);
        if (rawLength == 0)
        {
            return Decode(baseline, body, destination);
        }

        if (rawLength > (ulong)scratch.Length)
        {
            return -1;
        }

        Span<byte> delta = scratch.Slice(0, (int)rawLength);
        if (!Lz4Block.DecompressExact(body, delta))
        {
            return -1;
        }

        return Decode(baseline, delta, destination);
    }

    /// <summary>
    /// V2 encoder: one sliding 64-position window of "X is zero" bits (<c>bit k ⇔ X[chunk + k] == 0</c>) serves both
    /// searches — the next change and the end of the literal — so every 32-byte chunk mask is computed exactly once.
    /// </summary>
    internal static int Encode<TSimd>(ReadOnlySpan<byte> baseline, ReadOnlySpan<byte> current, Span<byte> destination)
        where TSimd : struct, ISimdLevel
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
        int chunk = 0;
        ulong window = ZeroMask<TSimd>(ref cur, ref bas, 0, overlap, n) | ((ulong)ZeroMask<TSimd>(ref cur, ref bas, 32, overlap, n) << 32);
        int lastEnd = 0;
        int scan = 0;
        while (true)
        {
            // Next non-zero X at or after scan. Positions at or beyond n are zero bits, so a hit is always below n.
            int start;
            while (true)
            {
                while (scan - chunk >= 32)
                {
                    chunk += 32;
                    window = (window >> 32) | ((ulong)ZeroMask<TSimd>(ref cur, ref bas, chunk + 32, overlap, n) << 32);
                }

                ulong nonZero = ~window >> (scan - chunk);
                if (nonZero != 0)
                {
                    start = scan + BitOperations.TrailingZeroCount(nonZero);
                    break;
                }

                if (chunk + 64 >= n)
                {
                    return position;
                }

                scan = chunk + 64;
            }

            // End of the literal: first run of MinZeroRun zero bits at or after start + 1. Bits shifted in from beyond the
            // window are 0 (non-zero X), so they can hide a run that crosses the window end but never fake one.
            int probe = start + 1;
            int end;
            while (true)
            {
                while (probe - chunk >= 32)
                {
                    chunk += 32;
                    window = (window >> 32) | ((ulong)ZeroMask<TSimd>(ref cur, ref bas, chunk + 32, overlap, n) << 32);
                }

                ulong zeros = window >> (probe - chunk);
                ulong runs = zeros & (zeros >> 1) & (zeros >> 2);
                if (runs != 0)
                {
                    end = probe + BitOperations.TrailingZeroCount(runs);
                    break;
                }

                // Every run start below chunk + 62 has been ruled out.
                probe = chunk + 62;
            }

            int skip = start - lastEnd;
            int count = end - start;
            if (skip < 64 && count < 64 && count + 2 <= capacity - position)
            {
                Unsafe.Add(ref dst, position) = (byte)skip;
                Unsafe.Add(ref dst, position + 1) = (byte)count;
                position += 2;
            }
            else
            {
                if (VarInt.GetLength((ulong)skip) + VarInt.GetLength((ulong)count) + count > capacity - position)
                {
                    return -1;
                }

                position += VarInt.Write(destination.Slice(position), (ulong)skip);
                position += VarInt.Write(destination.Slice(position), (ulong)count);
            }

            XorRun<TSimd>(ref Unsafe.Add(ref dst, position), ref Unsafe.Add(ref cur, start), ref bas, start, count, Math.Max(0, Math.Min(end, overlap) - start));
            position += count;
            lastEnd = end;

            // X[end .. end + MinZeroRun) is known to be zero.
            scan = end + MinZeroRun;
        }
    }

    /// <summary>
    /// V2 decoder: short skips are one vector copy of baseline bytes and short literals one vector XOR, both allowed to
    /// run past their op (never past <c>CurrentLength</c>) because every later position is rewritten by a later op or
    /// the final copy. The XOR spill writes non-baseline bytes, so it is disabled for in-place decoding.
    /// </summary>
    internal static int Decode<TSimd>(ReadOnlySpan<byte> baseline, ReadOnlySpan<byte> delta, Span<byte> destination)
        where TSimd : struct, ISimdLevel
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
        bool inPlace = overlap > 0 && Unsafe.AreSame(ref dst, ref bas);
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

            int s = (int)skip;
            if (TSimd.Width >= 256 && s <= 32 && output + 32 <= overlap)
            {
                Vector256.LoadUnsafe(ref bas, (nuint)output).StoreUnsafe(ref dst, (nuint)output);
            }
            else if (TSimd.Width >= 128 && s <= 16 && output + 16 <= overlap)
            {
                Vector128.LoadUnsafe(ref bas, (nuint)output).StoreUnsafe(ref dst, (nuint)output);
            }
            else if (s != 0)
            {
                CopyBaseline(baseline, destination, output, s, overlap);
            }

            output += s;
            if (count > (ulong)(n - output) || count > (ulong)(deltaLength - input))
            {
                return -1;
            }

            int c = (int)count;
            if (TSimd.Width >= 128 && c <= 16 && !inPlace && output + 16 <= overlap && input + 16 <= deltaLength)
            {
                (Vector128.LoadUnsafe(ref src, (nuint)input) ^ Vector128.LoadUnsafe(ref bas, (nuint)output)).StoreUnsafe(ref dst, (nuint)output);
            }
            else
            {
                XorRun<TSimd>(ref Unsafe.Add(ref dst, output), ref Unsafe.Add(ref src, input), ref bas, output, c, Math.Max(0, Math.Min(output + c, overlap) - output));
            }

            output += c;
            input += c;
        }

        CopyBaseline(baseline, destination, output, n - output, overlap);
        return n;
    }

    /// <summary>Writes B'[position .. position + count) to the destination (baseline bytes, then zeros past its end).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
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

    /// <summary>
    /// output[k] = source[k] ^ baseline[baselineOffset + k] for k &lt; xorCount, output[k] = source[k] for
    /// xorCount ≤ k &lt; count. Element-wise, so output may alias the baseline at the same position (in-place decode).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void XorRun<TSimd>(ref byte output, ref byte source, ref byte baseline, int baselineOffset, int count, int xorCount)
        where TSimd : struct, ISimdLevel
    {
        int k = 0;
        if (xorCount > 0)
        {
            ref byte bas = ref Unsafe.Add(ref baseline, baselineOffset);
            if (TSimd.Width >= 256)
            {
                for (; k + 32 <= xorCount; k += 32)
                {
                    (Vector256.LoadUnsafe(ref source, (nuint)k) ^ Vector256.LoadUnsafe(ref bas, (nuint)k)).StoreUnsafe(ref output, (nuint)k);
                }
            }

            if (TSimd.Width >= 128)
            {
                for (; k + 16 <= xorCount; k += 16)
                {
                    (Vector128.LoadUnsafe(ref source, (nuint)k) ^ Vector128.LoadUnsafe(ref bas, (nuint)k)).StoreUnsafe(ref output, (nuint)k);
                }
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

    /// <summary>Bit k set ⇔ X[position + k] == 0, for k in 0..31; X is 0 at and beyond <paramref name="n"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint ZeroMask<TSimd>(ref byte cur, ref byte bas, int position, int overlap, int n)
        where TSimd : struct, ISimdLevel
    {
        if (TSimd.Width >= 256)
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
        }
        else if (TSimd.Width >= 128)
        {
            if (position + 32 <= overlap)
            {
                Vector128<byte> lo = Vector128.LoadUnsafe(ref cur, (nuint)position) ^ Vector128.LoadUnsafe(ref bas, (nuint)position);
                Vector128<byte> hi = Vector128.LoadUnsafe(ref cur, (nuint)(position + 16)) ^ Vector128.LoadUnsafe(ref bas, (nuint)(position + 16));
                return Vector128.Equals(lo, Vector128<byte>.Zero).ExtractMostSignificantBits()
                     | (Vector128.Equals(hi, Vector128<byte>.Zero).ExtractMostSignificantBits() << 16);
            }

            if (position >= overlap && position + 32 <= n)
            {
                Vector128<byte> lo = Vector128.LoadUnsafe(ref cur, (nuint)position);
                Vector128<byte> hi = Vector128.LoadUnsafe(ref cur, (nuint)(position + 16));
                return Vector128.Equals(lo, Vector128<byte>.Zero).ExtractMostSignificantBits()
                     | (Vector128.Equals(hi, Vector128<byte>.Zero).ExtractMostSignificantBits() << 16);
            }
        }

        return ZeroMaskScalar(ref cur, ref bas, position, overlap, n);
    }

    /// <summary>Scalar <see cref="ZeroMask"/>: used without SIMD, and for the chunks that straddle the baseline end or the input end.</summary>
    private static uint ZeroMaskScalar(ref byte cur, ref byte bas, int position, int overlap, int n)
    {
        uint mask = 0;
        int limit = Math.Min(n - position, 32);
        int k = 0;
        for (; k < limit; k++)
        {
            int p = position + k;
            int x = Unsafe.Add(ref cur, p) ^ (p < overlap ? Unsafe.Add(ref bas, p) : 0);
            mask |= (x == 0 ? 1u : 0u) << k;
        }

        // Positions at or beyond n are zero.
        return k >= 32 ? mask : mask | (uint.MaxValue << k);
    }

    private static void ThrowLength(int length) =>
        throw new ArgumentOutOfRangeException(nameof(length), length, "Length must be in 0..0x70000000.");
}

/// <summary>Compile-time SIMD width selector for <see cref="DeltaCodec"/>'s generic core (the JIT specializes per struct).</summary>
internal interface ISimdLevel
{
    /// <summary>0 (scalar), 128 or 256.</summary>
    static abstract int Width { get; }
}

/// <summary>Scalar code paths only.</summary>
internal readonly struct SimdNone : ISimdLevel
{
    public static int Width => 0;
}

/// <summary><see cref="Vector128"/> code paths.</summary>
internal readonly struct Simd128 : ISimdLevel
{
    public static int Width => 128;
}

/// <summary><see cref="Vector256"/> code paths.</summary>
internal readonly struct Simd256 : ISimdLevel
{
    public static int Width => 256;
}
