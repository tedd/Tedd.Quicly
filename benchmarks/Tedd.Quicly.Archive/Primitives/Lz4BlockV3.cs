using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Tedd.Quicly.Archive.Primitives;

/// <summary>
/// ARCHIVED V3 of <c>Tedd.Quicly.Core.Primitives.Lz4Block</c>: V2 compressor plus a reference-style "wild copy" decoder (16-byte chunk copies with overshoot, inc32/dec64 short-offset trick). Rejected: slower than V2 (memmove already wins for the 17..64-byte runs game data produces). Kept for benchmark comparison only.
/// </summary>
/// <remarks>
/// <para>
/// Implements the <em>block</em> format (not the frame format): a sequence of tokens, each carrying a literal
/// run, a 16-bit little-endian match offset (1..65535) and a match length (minimum 4). The encoder follows the
/// reference end-of-block rules (the last 5 bytes are literals; the last match starts at least 12 bytes before
/// the end), so its output decodes with any conforming LZ4 decoder and it decodes any conforming block.
/// </para>
/// <para>
/// The compressor uses a hash table of <see cref="int"/> entries. The caller may supply it as a scratch span so
/// the hot path never allocates; the convenience overload uses a per-thread table allocated once.
/// </para>
/// <para>
/// <see cref="Decompress"/> is safe against malicious input: every read and write is bounds-checked and malformed
/// input reports <c>-1</c> instead of throwing. <see cref="DecompressExact"/> additionally requires the decoded
/// length to equal the destination length, which is what protocol receivers need (PROTOCOL.md §2.1).
/// </para>
/// <para>
/// The decoder copies in fixed 8/16-byte chunks that may overshoot the logical end of a copy while at least 16
/// bytes of slack remain in both spans (the reference decoder's "wild copy"), and falls back to exact-length
/// copies for the last 16 bytes of the destination. Overshoot never leaves the destination span, but bytes
/// beyond the returned length may be overwritten. See <c>docs/benchmarks/primitives.md</c>.
/// </para>
/// </remarks>
public static class Lz4BlockV3
{
    /// <summary>The smallest accepted scratch table length (entries).</summary>
    public const int MinScratchLength = 256;

    /// <summary>The largest accepted scratch table length (entries).</summary>
    public const int MaxScratchLength = 65536;

    /// <summary>The scratch table length used by the convenience overload (entries).</summary>
    public const int DefaultScratchLength = 4096;

    /// <summary>The largest input length <see cref="Compress(ReadOnlySpan{byte}, Span{byte}, Span{int})"/> accepts (same as the reference implementation).</summary>
    public const int MaxInputLength = 0x7E000000;

    private const int MinMatch = 4;
    private const int LastLiterals = 5;
    private const int MfLimit = 12;
    private const int MinLength = MfLimit + 1;
    private const int MaxDistance = 65535;
    private const int MlBits = 4;
    private const int MlMask = (1 << MlBits) - 1;
    private const int RunMask = 15;
    private const int SkipTrigger = 6;
    private const uint HashMultiplier = 2654435761u;

    /// <summary>
    /// Slack the decoder requires past the end of a copy before it uses the overshooting 8/16-byte copy loops
    /// (they write up to 15 bytes past the logical end, always inside the destination span).
    /// </summary>
    private const int WildMargin = 16;

    /// <summary>Source adjustment after the first 4 bytes of a short-offset (1..7) match copy (reference LZ4 <c>inc32table</c>).</summary>
    private static ReadOnlySpan<int> Inc32Table => [0, 1, 2, 1, 0, 4, 4, 4];

    /// <summary>Source adjustment after the first 8 bytes of a short-offset (1..7) match copy (reference LZ4 <c>dec64table</c>).</summary>
    private static ReadOnlySpan<int> Dec64Table => [0, 0, 0, -1, -4, 1, 2, 3];

    [ThreadStatic]
    private static int[]? t_scratch;

    /// <summary>Returns an upper bound on the compressed size of <paramref name="inputLength"/> bytes.</summary>
    /// <param name="inputLength">The uncompressed length.</param>
    /// <returns>The largest possible compressed length; a destination of this size never makes <c>Compress</c> fail.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="inputLength"/> is negative or exceeds <see cref="MaxInputLength"/>.</exception>
    public static int GetMaxCompressedLength(int inputLength)
    {
        if ((uint)inputLength > MaxInputLength)
        {
            ThrowInputTooLarge();
        }

        return inputLength + inputLength / 255 + 16;
    }

    /// <summary>
    /// Compresses <paramref name="source"/> into <paramref name="destination"/> using a per-thread scratch table.
    /// </summary>
    /// <param name="source">The bytes to compress.</param>
    /// <param name="destination">The buffer receiving the compressed block.</param>
    /// <returns>The compressed length, or <c>-1</c> if <paramref name="destination"/> is too small.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="source"/> is longer than <see cref="MaxInputLength"/>.</exception>
    public static int Compress(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        int[] scratch = t_scratch ??= new int[DefaultScratchLength];
        return Compress(source, destination, scratch);
    }

    /// <summary>
    /// Compresses <paramref name="source"/> into <paramref name="destination"/> using the supplied scratch table.
    /// </summary>
    /// <param name="source">The bytes to compress.</param>
    /// <param name="destination">The buffer receiving the compressed block.</param>
    /// <param name="scratch">
    /// Hash table work space: a power-of-two length between <see cref="MinScratchLength"/> and
    /// <see cref="MaxScratchLength"/> entries (4096..16384 recommended). Its contents on entry are irrelevant.
    /// </param>
    /// <returns>The compressed length, or <c>-1</c> if <paramref name="destination"/> is too small.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="source"/> is longer than <see cref="MaxInputLength"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="scratch"/> has an unsupported length.</exception>
    public static int Compress(ReadOnlySpan<byte> source, Span<byte> destination, Span<int> scratch)
    {
        int hashShift = GetHashShift(scratch.Length);
        int inputLength = source.Length;
        if (inputLength > MaxInputLength)
        {
            ThrowInputTooLarge();
        }

        // The table is deliberately not cleared: every candidate read from it is validated against the
        // current position and window before use, so stale or garbage entries can only cost a missed match.
        int outputLength = destination.Length;
        ref byte src = ref MemoryMarshal.GetReference(source);
        ref byte dst = ref MemoryMarshal.GetReference(destination);
        ref int table = ref MemoryMarshal.GetReference(scratch);

        int ip = 0;
        int anchor = 0;
        int op = 0;

        if (inputLength < MinLength)
        {
            goto LastLiterals;
        }

        int matchLimit = inputLength - LastLiterals;
        int mfLimitPlusOne = inputLength - MfLimit + 1;

        Unsafe.Add(ref table, Hash(ReadUInt32(ref src, 0), hashShift)) = 0;
        ip = 1;
        uint forwardHash = Hash(ReadUInt32(ref src, ip), hashShift);

        while (true)
        {
            int match;
            int tokenPos;

            // Find a match.
            {
                int step = 1;
                int searchMatchNb = 1 << SkipTrigger;
                int forwardIp = ip;
                do
                {
                    uint h = forwardHash;
                    ip = forwardIp;
                    forwardIp += step;
                    step = searchMatchNb++ >> SkipTrigger;
                    if (forwardIp > mfLimitPlusOne)
                    {
                        goto LastLiterals;
                    }

                    match = Unsafe.Add(ref table, h);
                    forwardHash = Hash(ReadUInt32(ref src, forwardIp), hashShift);
                    Unsafe.Add(ref table, h) = ip;
                }
                while (!IsCandidateValid(ip, match) || ReadUInt32(ref src, match) != ReadUInt32(ref src, ip));
            }

            // Catch up: extend the match backwards into the literal run.
            while (ip > anchor && match > 0 && Unsafe.Add(ref src, ip - 1) == Unsafe.Add(ref src, match - 1))
            {
                ip--;
                match--;
            }

            // Emit the literal run (token + optional length bytes + literals); reserve room for the offset.
            {
                int litLength = ip - anchor;
                tokenPos = op;
                if (op + 1 + litLength + (litLength + 240) / 255 + 2 > outputLength)
                {
                    return -1;
                }

                op++;
                if (litLength >= RunMask)
                {
                    int len = litLength - RunMask;
                    Unsafe.Add(ref dst, tokenPos) = (byte)(RunMask << MlBits);
                    for (; len >= 255; len -= 255)
                    {
                        Unsafe.Add(ref dst, op++) = 255;
                    }

                    Unsafe.Add(ref dst, op++) = (byte)len;
                }
                else
                {
                    Unsafe.Add(ref dst, tokenPos) = (byte)(litLength << MlBits);
                }

                Unsafe.CopyBlockUnaligned(ref Unsafe.Add(ref dst, op), ref Unsafe.Add(ref src, anchor), (uint)litLength);
                op += litLength;
            }

        NextMatch:
            // Offset (room reserved by the caller of this label).
            WriteUInt16LittleEndian(ref dst, op, (ushort)(ip - match));
            op += 2;

            // Match length.
            {
                int matchLength = CountMatch(ref src, ip + MinMatch, match + MinMatch, matchLimit);
                ip += MinMatch + matchLength;

                if (op + (matchLength + 240) / 255 > outputLength)
                {
                    return -1;
                }

                if (matchLength >= MlMask)
                {
                    Unsafe.Add(ref dst, tokenPos) |= MlMask;
                    int len = matchLength - MlMask;
                    for (; len >= 255; len -= 255)
                    {
                        Unsafe.Add(ref dst, op++) = 255;
                    }

                    Unsafe.Add(ref dst, op++) = (byte)len;
                }
                else
                {
                    Unsafe.Add(ref dst, tokenPos) |= (byte)matchLength;
                }
            }

            anchor = ip;
            if (ip >= mfLimitPlusOne)
            {
                goto LastLiterals;
            }

            // Fill the table with the position just before the current one.
            Unsafe.Add(ref table, Hash(ReadUInt32(ref src, ip - 2), hashShift)) = ip - 2;

            // Test the next position for an immediate match (zero literals).
            {
                uint h = Hash(ReadUInt32(ref src, ip), hashShift);
                match = Unsafe.Add(ref table, h);
                Unsafe.Add(ref table, h) = ip;
                if (IsCandidateValid(ip, match) && ReadUInt32(ref src, match) == ReadUInt32(ref src, ip))
                {
                    if (op + 1 + 2 > outputLength)
                    {
                        return -1;
                    }

                    tokenPos = op++;
                    Unsafe.Add(ref dst, tokenPos) = 0;
                    goto NextMatch;
                }
            }

            forwardHash = Hash(ReadUInt32(ref src, ++ip), hashShift);
        }

    LastLiterals:
        {
            int lastRun = inputLength - anchor;
            if (op + 1 + lastRun + (lastRun + 240) / 255 > outputLength)
            {
                return -1;
            }

            if (lastRun >= RunMask)
            {
                int len = lastRun - RunMask;
                Unsafe.Add(ref dst, op++) = (byte)(RunMask << MlBits);
                for (; len >= 255; len -= 255)
                {
                    Unsafe.Add(ref dst, op++) = 255;
                }

                Unsafe.Add(ref dst, op++) = (byte)len;
            }
            else
            {
                Unsafe.Add(ref dst, op++) = (byte)(lastRun << MlBits);
            }

            Unsafe.CopyBlockUnaligned(ref Unsafe.Add(ref dst, op), ref Unsafe.Add(ref src, anchor), (uint)lastRun);
            op += lastRun;
            return op;
        }
    }

    /// <summary>Decompresses the LZ4 block in <paramref name="source"/> into <paramref name="destination"/>.</summary>
    /// <param name="source">The complete compressed block.</param>
    /// <param name="destination">The buffer receiving the decompressed bytes.</param>
    /// <returns>
    /// The decompressed length, or <c>-1</c> if the block is malformed, truncated, references bytes before the
    /// start of the output, or does not fit in <paramref name="destination"/>. Never throws and never touches
    /// memory outside the two spans, whatever the input.
    /// </returns>
    /// <remarks>
    /// The decoder copies in 8- and 16-byte chunks when the destination has room, so bytes of
    /// <paramref name="destination"/> beyond the returned length may be overwritten; on failure the contents of
    /// <paramref name="destination"/> are unspecified.
    /// </remarks>
    public static int Decompress(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        int srcLength = source.Length;
        int dstLength = destination.Length;
        if (srcLength == 0)
        {
            return -1;
        }

        ref byte src = ref MemoryMarshal.GetReference(source);
        ref byte dst = ref MemoryMarshal.GetReference(destination);
        int ip = 0;
        int op = 0;

        while (true)
        {
            if (ip >= srcLength)
            {
                return -1;
            }

            int token = Unsafe.Add(ref src, ip++);

            // Literals.
            int literalLength = token >> MlBits;
            if (literalLength == RunMask)
            {
                int b;
                do
                {
                    if (ip >= srcLength)
                    {
                        return -1;
                    }

                    b = Unsafe.Add(ref src, ip++);
                    literalLength += b;
                    if (literalLength > dstLength)
                    {
                        return -1;
                    }
                }
                while (b == 255);
            }

            if (literalLength > srcLength - ip || literalLength > dstLength - op)
            {
                return -1;
            }

            if (srcLength - ip >= literalLength + WildMargin && dstLength - op >= literalLength + WildMargin)
            {
                // Slack on both sides: copy in 16-byte chunks, overshooting by up to 15 bytes.
                WildCopy16(ref Unsafe.Add(ref dst, op), ref Unsafe.Add(ref src, ip), literalLength);
            }
            else
            {
                Unsafe.CopyBlockUnaligned(ref Unsafe.Add(ref dst, op), ref Unsafe.Add(ref src, ip), (uint)literalLength);
            }

            ip += literalLength;
            op += literalLength;

            if (ip == srcLength)
            {
                // The final sequence carries literals only.
                return op;
            }

            // Offset.
            if (srcLength - ip < 2)
            {
                return -1;
            }

            int offset = Unsafe.Add(ref src, ip) | (Unsafe.Add(ref src, ip + 1) << 8);
            ip += 2;
            if (offset == 0 || offset > op)
            {
                return -1;
            }

            // Match length.
            int matchLength = token & MlMask;
            if (matchLength == MlMask)
            {
                int b;
                do
                {
                    if (ip >= srcLength)
                    {
                        return -1;
                    }

                    b = Unsafe.Add(ref src, ip++);
                    matchLength += b;
                    if (matchLength > dstLength)
                    {
                        return -1;
                    }
                }
                while (b == 255);
            }

            matchLength += MinMatch;
            if (matchLength > dstLength - op)
            {
                return -1;
            }

            int matchPos = op - offset;
            int end = op + matchLength;
            if (dstLength - end >= WildMargin)
            {
                // Room to overshoot: every copy below may write up to 15 bytes past `end`, and every read stays
                // at or below the bytes already written (the source trails the destination by >= 8 bytes).
                if (offset >= 16)
                {
                    WildCopy16(ref Unsafe.Add(ref dst, op), ref Unsafe.Add(ref dst, matchPos), matchLength);
                }
                else
                {
                    if (offset < 8)
                    {
                        // Reference LZ4 idiom: four bytes one at a time, four more from a source adjusted so the
                        // copy is offset-periodic, then move the source back so it trails by a multiple of the
                        // offset that is at least 8 (stride 8, 8, 9, 8, 10, 12, 14 for offsets 1..7).
                        Unsafe.Add(ref dst, op) = Unsafe.Add(ref dst, matchPos);
                        Unsafe.Add(ref dst, op + 1) = Unsafe.Add(ref dst, matchPos + 1);
                        Unsafe.Add(ref dst, op + 2) = Unsafe.Add(ref dst, matchPos + 2);
                        Unsafe.Add(ref dst, op + 3) = Unsafe.Add(ref dst, matchPos + 3);
                        matchPos += Unsafe.Add(ref MemoryMarshal.GetReference(Inc32Table), offset);
                        Unsafe.WriteUnaligned(ref Unsafe.Add(ref dst, op + 4), Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref dst, matchPos)));
                        matchPos -= Unsafe.Add(ref MemoryMarshal.GetReference(Dec64Table), offset);
                    }
                    else
                    {
                        Unsafe.WriteUnaligned(ref Unsafe.Add(ref dst, op), Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref dst, matchPos)));
                        matchPos += 8;
                    }

                    for (int o = op + 8; o < end; o += 8, matchPos += 8)
                    {
                        Unsafe.WriteUnaligned(ref Unsafe.Add(ref dst, o), Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref dst, matchPos)));
                    }
                }

                op = end;
            }
            else if (offset >= matchLength)
            {
                // Near the end of the destination: exact-length copies only.
                Unsafe.CopyBlockUnaligned(ref Unsafe.Add(ref dst, op), ref Unsafe.Add(ref dst, matchPos), (uint)matchLength);
                op = end;
            }
            else
            {
                // Overlapping copy near the end of the destination: the pattern repeats every `offset` bytes.
                if (offset < 8)
                {
                    // Prime up to 16 bytes one at a time, then continue with a stride that is a whole number
                    // of periods (>= 8 bytes) so 8-byte chunks only ever read bytes already written.
                    int primed = Math.Min(16, matchLength);
                    for (int k = 0; k < primed; k++)
                    {
                        Unsafe.Add(ref dst, op++) = Unsafe.Add(ref dst, matchPos++);
                    }

                    int stride = (16 / offset) * offset;
                    matchPos = op - stride;
                }

                while (end - op >= 8)
                {
                    Unsafe.WriteUnaligned(ref Unsafe.Add(ref dst, op), Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref dst, matchPos)));
                    op += 8;
                    matchPos += 8;
                }

                while (op < end)
                {
                    Unsafe.Add(ref dst, op++) = Unsafe.Add(ref dst, matchPos++);
                }
            }
        }
    }

    /// <summary>
    /// Decompresses the LZ4 block in <paramref name="source"/> into <paramref name="destination"/> and requires the
    /// decoded length to be exactly <paramref name="destination"/>.Length — the PROTOCOL.md rule that a compressed
    /// message must decode to precisely its announced <c>RawLength</c>.
    /// </summary>
    /// <param name="source">The complete compressed block.</param>
    /// <param name="destination">A buffer of exactly the expected decoded size.</param>
    /// <returns>
    /// <see langword="true"/> if the block is well-formed and decoded to exactly <paramref name="destination"/>.Length
    /// bytes; <see langword="false"/> if it is malformed, truncated, would overflow the destination, or decodes to
    /// fewer bytes. Never throws; on <see langword="false"/> the contents of <paramref name="destination"/> are unspecified.
    /// </returns>
    public static bool DecompressExact(ReadOnlySpan<byte> source, Span<byte> destination) =>
        Decompress(source, destination) == destination.Length;

    private static int GetHashShift(int scratchLength)
    {
        if (scratchLength < MinScratchLength || scratchLength > MaxScratchLength || !BitOperations.IsPow2(scratchLength))
        {
            ThrowBadScratch(scratchLength);
        }

        return 32 - BitOperations.Log2((uint)scratchLength);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint Hash(uint sequence, int shift) => (sequence * HashMultiplier) >> shift;

    /// <summary>
    /// Counts how many bytes starting at <paramref name="ip"/> equal the bytes starting at <paramref name="match"/>,
    /// stopping at <paramref name="limit"/>. Compares 8 bytes at a time and locates the first difference with a
    /// bit scan; the tail is compared byte-wise.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int CountMatch(ref byte src, int ip, int match, int limit)
    {
        int start = ip;
        while (ip + 8 <= limit)
        {
            ulong diff = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref src, ip)) ^ Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref src, match));
            if (diff != 0)
            {
                int firstDifferentByte = (BitConverter.IsLittleEndian ? BitOperations.TrailingZeroCount(diff) : BitOperations.LeadingZeroCount(diff)) >> 3;
                return ip - start + firstDifferentByte;
            }

            ip += 8;
            match += 8;
        }

        while (ip < limit && Unsafe.Add(ref src, ip) == Unsafe.Add(ref src, match))
        {
            ip++;
            match++;
        }

        return ip - start;
    }

    /// <summary>
    /// Copies <paramref name="length"/> bytes in 16-byte chunks, writing up to 15 bytes past the end; the caller
    /// guarantees <see cref="WildMargin"/> bytes of room after the copy on both sides and that the source region,
    /// when it lies in the destination, trails it by at least 16 bytes.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void WildCopy16(ref byte destination, ref byte source, int length)
    {
        for (int i = 0; i < length; i += 16)
        {
            ulong a = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref source, i));
            ulong b = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref source, i + 8));
            Unsafe.WriteUnaligned(ref Unsafe.Add(ref destination, i), a);
            Unsafe.WriteUnaligned(ref Unsafe.Add(ref destination, i + 8), b);
        }
    }

    /// <summary>
    /// A table entry is usable only if it lies in [0, ip) and within the 64 KiB window, i.e.
    /// 0 &lt;= ip - 1 - match &lt;= min(ip - 1, 65534). Rejects negative and stale (>= ip) garbage in one compare.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsCandidateValid(int ip, int match) =>
        (uint)(ip - 1 - match) <= (uint)Math.Min(ip - 1, MaxDistance - 1);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint ReadUInt32(ref byte p, int offset)
    {
        uint v = Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref p, offset));
        return BitConverter.IsLittleEndian ? v : System.Buffers.Binary.BinaryPrimitives.ReverseEndianness(v);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void WriteUInt16LittleEndian(ref byte p, int offset, ushort value)
    {
        if (!BitConverter.IsLittleEndian)
        {
            value = System.Buffers.Binary.BinaryPrimitives.ReverseEndianness(value);
        }

        Unsafe.WriteUnaligned(ref Unsafe.Add(ref p, offset), value);
    }

    private static void ThrowInputTooLarge() =>
        throw new ArgumentOutOfRangeException("source", $"Input length must be between 0 and {MaxInputLength} bytes.");

    private static void ThrowBadScratch(int length) =>
        throw new ArgumentException($"Scratch length must be a power of two between {MinScratchLength} and {MaxScratchLength} entries; got {length}.", "scratch");
}
