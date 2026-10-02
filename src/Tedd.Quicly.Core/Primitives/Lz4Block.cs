using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Tedd.Quicly.Core.Primitives;

/// <summary>
/// Allocation-free LZ4 block format compressor and decompressor.
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
/// <see cref="TryDecompressInPlace"/> decodes a block in the buffer it arrived in, given
/// <see cref="GetInPlaceMargin"/> bytes beyond the decoded size: a receiver stages a compressed reliable message in a
/// block that large and needs no second buffer for it.
/// </para>
/// <para>
/// Performance history (table clearing, 8-byte match counting, 16-byte short copies, and the rejected
/// reference-style "wild copy" decoder) is recorded with measurements in <c>docs/benchmarks/primitives.md</c>.
/// </para>
/// </remarks>
public static class Lz4Block
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

            if (literalLength <= 16 && srcLength - ip >= 16 && dstLength - op >= 16)
            {
                // Short literal run with slack on both sides: one 16-byte copy instead of a memmove call.
                Copy16(ref Unsafe.Add(ref dst, op), ref Unsafe.Add(ref src, ip));
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
            if (offset >= 16 && matchLength <= 16 && dstLength - op >= 16)
            {
                // Short non-overlapping match with slack: one 16-byte copy.
                Copy16(ref Unsafe.Add(ref dst, op), ref Unsafe.Add(ref dst, matchPos));
                op = end;
            }
            else if (offset >= matchLength)
            {
                Unsafe.CopyBlockUnaligned(ref Unsafe.Add(ref dst, op), ref Unsafe.Add(ref dst, matchPos), (uint)matchLength);
                op = end;
            }
            else
            {
                // Overlapping copy: the pattern repeats every `offset` bytes.
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

    /// <summary>
    /// The room an in-place decode needs beyond the decoded length (<see cref="TryDecompressInPlace"/>): a block of
    /// <c>rawLength + GetInPlaceMargin(compressedLength)</c> bytes decodes any block the compressor wrote in place, with
    /// the compressed bytes moved to its end. The same margin as the reference implementation's
    /// <c>LZ4_DECOMPRESS_INPLACE_MARGIN</c>; this decoder writes at most 16 bytes past a copy, which it covers.
    /// </summary>
    /// <param name="compressedLength">The compressed length.</param>
    /// <returns>The margin in bytes.</returns>
    public static int GetInPlaceMargin(int compressedLength) => (compressedLength >> 8) + 32;

    /// <summary>
    /// Decodes an LZ4 block that occupies the first <paramref name="compressedLength"/> bytes of <paramref name="block"/>
    /// into the first <paramref name="rawLength"/> bytes of the same block, and requires exactly that decoded length
    /// (PROTOCOL.md §2.1), without a second buffer. Allocation-free and stateless.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The compressed bytes are first moved to the end of the block (one memmove); the decode then writes from the start
    /// of the block towards them. Before every write the decoder checks that the bytes it writes, including the slack of a
    /// 16-byte copy, end at or before the first compressed byte it has not read yet. With the margin of
    /// <see cref="GetInPlaceMargin"/> the compressor's output never fails that check; a block that does (hostile input, or
    /// one that needs more room than the block has) is refused, never decoded over its own unread input.
    /// </para>
    /// <para>
    /// Never throws and never touches memory outside <paramref name="block"/>, whatever the input. On
    /// <see langword="false"/> the contents of <paramref name="block"/> are unspecified (the compressed bytes are gone).
    /// </para>
    /// </remarks>
    /// <param name="block">The buffer: the compressed block at its start, room for the decoded payload and the margin.</param>
    /// <param name="compressedLength">Length of the compressed block at the start of <paramref name="block"/>.</param>
    /// <param name="rawLength">The exact decoded length.</param>
    /// <returns>
    /// <see langword="true"/> if the block decoded to exactly <paramref name="rawLength"/> bytes at the start of
    /// <paramref name="block"/>; <see langword="false"/> if the lengths are out of range, <paramref name="block"/> lacks
    /// <c>rawLength + GetInPlaceMargin(compressedLength)</c> bytes, or the input is malformed, truncated, decodes to another
    /// length or would overwrite its own unread bytes.
    /// </returns>
    internal static bool TryDecompressInPlace(Span<byte> block, int compressedLength, int rawLength)
    {
        int blockLength = block.Length;
        if (compressedLength <= 0 || rawLength <= 0 || compressedLength > blockLength
            || rawLength > blockLength - GetInPlaceMargin(compressedLength))
        {
            return false;
        }

        // The source moves to the end of the block; Span.CopyTo is a memmove, so the overlap is fine.
        int s = blockLength - compressedLength;
        block.Slice(0, compressedLength).CopyTo(block.Slice(s));

        // Offsets: ip is relative to the source (block[s..]), op is absolute and the destination is block[..rawLength].
        // Invariant after every write: op <= s + ip, so the decoded bytes lie wholly below the unread source.
        ref byte b = ref MemoryMarshal.GetReference(block);
        int srcLength = compressedLength;
        int dstLength = rawLength;
        int ip = 0;
        int op = 0;

        while (true)
        {
            if (ip >= srcLength)
            {
                return false;
            }

            int token = Unsafe.Add(ref b, s + ip++);

            // Literals.
            int literalLength = token >> MlBits;
            if (literalLength == RunMask)
            {
                int x;
                do
                {
                    if (ip >= srcLength)
                    {
                        return false;
                    }

                    x = Unsafe.Add(ref b, s + ip++);
                    literalLength += x;
                    if (literalLength > dstLength)
                    {
                        return false;
                    }
                }
                while (x == 255);
            }

            if (literalLength > srcLength - ip || literalLength > dstLength - op)
            {
                return false;
            }

            // The first unread source byte after this run, in block offsets.
            long unread = (long)s + ip + literalLength;
            if (literalLength <= 16 && srcLength - ip >= 16 && dstLength - op >= 16 && op + 16 <= unread)
            {
                // Copy16 loads both halves before it stores, and its slack stays below the unread source.
                Copy16(ref Unsafe.Add(ref b, op), ref Unsafe.Add(ref b, s + ip));
            }
            else
            {
                if (op + literalLength > unread)
                {
                    return false;
                }

                // The destination may overlap the run it copies (op <= s + ip): a memmove, never a cpblk.
                block.Slice(s + ip, literalLength).CopyTo(block.Slice(op, literalLength));
            }

            ip += literalLength;
            op += literalLength;

            if (ip == srcLength)
            {
                // The final sequence carries literals only.
                return op == dstLength;
            }

            // Offset.
            if (srcLength - ip < 2)
            {
                return false;
            }

            int offset = Unsafe.Add(ref b, s + ip) | (Unsafe.Add(ref b, s + ip + 1) << 8);
            ip += 2;
            if (offset == 0 || offset > op)
            {
                return false;
            }

            // Match length.
            int matchLength = token & MlMask;
            if (matchLength == MlMask)
            {
                int x;
                do
                {
                    if (ip >= srcLength)
                    {
                        return false;
                    }

                    x = Unsafe.Add(ref b, s + ip++);
                    matchLength += x;
                    if (matchLength > dstLength)
                    {
                        return false;
                    }
                }
                while (x == 255);
            }

            matchLength += MinMatch;
            if (matchLength > dstLength - op)
            {
                return false;
            }

            // The match reads decoded bytes only (below op); its writes must stay below the unread source.
            unread = (long)s + ip;
            int matchPos = op - offset;
            int end = op + matchLength;
            if (offset >= 16 && matchLength <= 16 && dstLength - op >= 16 && op + 16 <= unread)
            {
                Copy16(ref Unsafe.Add(ref b, op), ref Unsafe.Add(ref b, matchPos));
                op = end;
                continue;
            }

            if (end > unread)
            {
                return false;
            }

            if (offset >= matchLength)
            {
                Unsafe.CopyBlockUnaligned(ref Unsafe.Add(ref b, op), ref Unsafe.Add(ref b, matchPos), (uint)matchLength);
                op = end;
            }
            else
            {
                // Overlapping copy: the pattern repeats every `offset` bytes (as Decompress).
                if (offset < 8)
                {
                    int primed = Math.Min(16, matchLength);
                    for (int k = 0; k < primed; k++)
                    {
                        Unsafe.Add(ref b, op++) = Unsafe.Add(ref b, matchPos++);
                    }

                    int stride = (16 / offset) * offset;
                    matchPos = op - stride;
                }

                while (end - op >= 8)
                {
                    Unsafe.WriteUnaligned(ref Unsafe.Add(ref b, op), Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref b, matchPos)));
                    op += 8;
                    matchPos += 8;
                }

                while (op < end)
                {
                    Unsafe.Add(ref b, op++) = Unsafe.Add(ref b, matchPos++);
                }
            }
        }
    }

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

    /// <summary>Copies exactly 16 bytes as two 8-byte loads and stores (the caller guarantees the room).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void Copy16(ref byte destination, ref byte source)
    {
        ulong a = Unsafe.ReadUnaligned<ulong>(ref source);
        ulong b = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref source, 8));
        Unsafe.WriteUnaligned(ref destination, a);
        Unsafe.WriteUnaligned(ref Unsafe.Add(ref destination, 8), b);
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
