using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Tedd.Quicly.Archive.Primitives;

/// <summary>
/// ARCHIVED V0 of <c>Tedd.Quicly.Core.Primitives.Lz4Block</c>: clears the hash table per call, byte-wise match counting, byte-wise decode. Kept for benchmark comparison only.
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
/// input reports <c>-1</c> instead of throwing.
/// </para>
/// </remarks>
public static class Lz4BlockV0
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

        scratch.Clear();

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
                while ((uint)(ip - match - 1) >= MaxDistance || ReadUInt32(ref src, match) != ReadUInt32(ref src, ip));
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
                int cur = ip + MinMatch;
                int m = match + MinMatch;
                while (cur < matchLimit && Unsafe.Add(ref src, cur) == Unsafe.Add(ref src, m))
                {
                    cur++;
                    m++;
                }

                int matchLength = cur - (ip + MinMatch);
                ip = cur;

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
                if ((uint)(ip - match - 1) < MaxDistance && ReadUInt32(ref src, match) == ReadUInt32(ref src, ip))
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

            Unsafe.CopyBlockUnaligned(ref Unsafe.Add(ref dst, op), ref Unsafe.Add(ref src, ip), (uint)literalLength);
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
            if (offset >= matchLength)
            {
                Unsafe.CopyBlockUnaligned(ref Unsafe.Add(ref dst, op), ref Unsafe.Add(ref dst, matchPos), (uint)matchLength);
                op += matchLength;
            }
            else
            {
                // Overlapping copy: the pattern repeats every `offset` bytes.
                int end = op + matchLength;
                while (op < end)
                {
                    Unsafe.Add(ref dst, op++) = Unsafe.Add(ref dst, matchPos++);
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
