using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Tedd.Quicly.Archive.Primitives;

/// <summary>
/// ARCHIVED V1 of <c>Tedd.Quicly.Core.Primitives.VarInt</c>: lzcnt + lookup-table length selection for writes (rejected: slower than the if-chain), single 8-byte load for reads (kept). Kept for benchmark comparison only.
/// </summary>
/// <remarks>
/// <para>
/// The two most significant bits of the first byte select the encoded length (00 = 1 byte, 01 = 2 bytes,
/// 10 = 4 bytes, 11 = 8 bytes); the remaining bits, read big-endian, hold the value. The largest
/// representable value is 2^62 - 1 (<see cref="MaxValue"/>).
/// </para>
/// <para>
/// Every method is allocation-free and never throws on malformed or truncated input (the <c>Try*</c>
/// variants report failure through their return value). Only the non-<c>Try</c> <c>Write</c> overloads throw,
/// and only for caller errors (value too large or destination too small).
/// </para>
/// <para>
/// The encoded length is derived from the bit width of the value (leading-zero count plus a lookup table)
/// rather than a chain of comparisons, and decoding uses one 8-byte big-endian load plus a shift whenever
/// 8 bytes are readable, so neither direction branches on the length in the common case.
/// See <c>docs/benchmarks/primitives.md</c>.
/// </para>
/// </remarks>
public static class VarIntV1
{
    /// <summary>The largest value that can be encoded as a QUIC varint (2^62 - 1).</summary>
    public const ulong MaxValue = (1UL << 62) - 1;

    /// <summary>The largest encoded size in bytes.</summary>
    public const int MaxLength = 8;

    /// <summary>
    /// Length code (0..3, meaning 1/2/4/8 bytes) indexed by the bit width of the value (0..64).
    /// Widths 63 and 64 are invalid and are rejected before the table is consulted.
    /// </summary>
    private static ReadOnlySpan<byte> LengthCodeByBitWidth =>
    [
        0, 0, 0, 0, 0, 0, 0,                                                    // 0..6   -> 1 byte
        1, 1, 1, 1, 1, 1, 1, 1,                                                 // 7..14  -> 2 bytes
        2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2,                         // 15..30 -> 4 bytes
        3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,                         // 31..46 -> 8 bytes
        3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3,                         // 47..62 -> 8 bytes
        3, 3,                                                                   // 63..64 -> invalid (guarded)
    ];

    /// <summary>Returns the length code (0..3) for a value known to be at most <see cref="MaxValue"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int LengthCode(ulong value)
    {
        int bitWidth = 64 - BitOperations.LeadingZeroCount(value | 1);
        return Unsafe.Add(ref MemoryMarshal.GetReference(LengthCodeByBitWidth), bitWidth);
    }

    /// <summary>Returns the number of bytes (1, 2, 4 or 8) needed to encode <paramref name="value"/>.</summary>
    /// <param name="value">The value to measure; must not exceed <see cref="MaxValue"/>.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="value"/> exceeds <see cref="MaxValue"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int GetLength(ulong value)
    {
        if (value > MaxValue)
        {
            ThrowValueTooLarge();
        }

        return 1 << LengthCode(value);
    }

    /// <summary>Returns the encoded length (1, 2, 4 or 8) announced by the first byte of an encoded varint.</summary>
    /// <param name="firstByte">The first byte of the encoding.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int PeekLength(byte firstByte) => 1 << (firstByte >> 6);

    /// <summary>Attempts to encode <paramref name="value"/> into <paramref name="destination"/>.</summary>
    /// <param name="destination">The buffer to write to.</param>
    /// <param name="value">The value to encode.</param>
    /// <param name="bytesWritten">The number of bytes written, or 0 on failure.</param>
    /// <returns><see langword="false"/> if the destination is too small or the value exceeds <see cref="MaxValue"/>.</returns>
    public static bool TryWrite(Span<byte> destination, ulong value, out int bytesWritten)
    {
        if (value > MaxValue)
        {
            goto Fail;
        }

        int code = LengthCode(value);
        int length = 1 << code;
        if (destination.Length < length)
        {
            goto Fail;
        }

        WriteCore(ref MemoryMarshal.GetReference(destination), value, code);
        bytesWritten = length;
        return true;

    Fail:
        bytesWritten = 0;
        return false;
    }

    /// <summary>Encodes <paramref name="value"/> into <paramref name="destination"/>.</summary>
    /// <param name="destination">The buffer to write to.</param>
    /// <param name="value">The value to encode.</param>
    /// <returns>The number of bytes written (1, 2, 4 or 8).</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="value"/> exceeds <see cref="MaxValue"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="destination"/> is too small.</exception>
    public static int Write(Span<byte> destination, ulong value)
    {
        if (!TryWrite(destination, value, out int bytesWritten))
        {
            ThrowWriteFailed(destination.Length, value);
        }

        return bytesWritten;
    }

    /// <summary>
    /// Encodes <paramref name="value"/> at <paramref name="destination"/>. The caller guarantees that at least
    /// <see cref="GetLength"/> bytes are writable.
    /// </summary>
    /// <param name="destination">Pointer to the first byte to write.</param>
    /// <param name="value">The value to encode.</param>
    /// <returns>The number of bytes written (1, 2, 4 or 8).</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="value"/> exceeds <see cref="MaxValue"/>.</exception>
    public static unsafe int Write(byte* destination, ulong value)
    {
        if (value > MaxValue)
        {
            ThrowValueTooLarge();
        }

        int code = LengthCode(value);
        WriteCore(ref *destination, value, code);
        return 1 << code;
    }

    /// <summary>
    /// Writes the big-endian encoding of <paramref name="value"/> with length code <paramref name="code"/>.
    /// The encoding is formed in a register (value shifted to the top bytes, prefix in the top two bits,
    /// byte-swapped) so only the final store depends on the length.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void WriteCore(ref byte destination, ulong value, int code)
    {
        int shift = 64 - (8 << code);
        ulong bigEndian = (value << shift) | ((ulong)(uint)code << 62);
        // Little-endian hosts store the byte-swapped word (its low `length` bytes are the encoding in memory
        // order); big-endian hosts store the encoding shifted down into the low `length` bytes.
        ulong store = BitConverter.IsLittleEndian ? BinaryPrimitives.ReverseEndianness(bigEndian) : bigEndian >> shift;
        switch (code)
        {
            case 0:
                destination = (byte)store;
                break;
            case 1:
                Unsafe.WriteUnaligned(ref destination, (ushort)store);
                break;
            case 2:
                Unsafe.WriteUnaligned(ref destination, (uint)store);
                break;
            default:
                Unsafe.WriteUnaligned(ref destination, store);
                break;
        }
    }

    /// <summary>Attempts to decode a varint from the start of <paramref name="source"/>.</summary>
    /// <param name="source">The encoded bytes.</param>
    /// <param name="value">The decoded value, or 0 on failure.</param>
    /// <param name="bytesConsumed">The number of bytes consumed, or 0 on failure.</param>
    /// <returns><see langword="false"/> if <paramref name="source"/> is empty or shorter than the announced length.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryRead(ReadOnlySpan<byte> source, out ulong value, out int bytesConsumed) =>
        TryReadCore(ref MemoryMarshal.GetReference(source), source.Length, out value, out bytesConsumed);

    /// <summary>Attempts to decode a varint from <paramref name="source"/> given <paramref name="available"/> readable bytes.</summary>
    /// <param name="source">Pointer to the first encoded byte.</param>
    /// <param name="available">The number of bytes readable at <paramref name="source"/>.</param>
    /// <param name="value">The decoded value, or 0 on failure.</param>
    /// <param name="bytesConsumed">The number of bytes consumed, or 0 on failure.</param>
    /// <returns><see langword="false"/> if fewer than the announced number of bytes are available.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static unsafe bool TryRead(byte* source, int available, out ulong value, out int bytesConsumed) =>
        TryReadCore(ref *source, available, out value, out bytesConsumed);

    private static bool TryReadCore(ref byte source, int available, out ulong value, out int bytesConsumed)
    {
        if (available >= 8)
        {
            // One wide load: the prefix lands in the top two bits, the value in the top `length` bytes.
            ulong raw = Unsafe.ReadUnaligned<ulong>(ref source);
            ulong bigEndian = BitConverter.IsLittleEndian ? BinaryPrimitives.ReverseEndianness(raw) : raw;
            int length = 1 << (int)(bigEndian >> 62);
            value = (bigEndian & MaxValue) >> (64 - 8 * length);
            bytesConsumed = length;
            return true;
        }

        if (available <= 0)
        {
            goto Fail;
        }

        byte first = source;
        int shortLength = 1 << (first >> 6);
        if (available < shortLength)
        {
            goto Fail;
        }

        switch (shortLength)
        {
            case 1:
                value = (ulong)(first & 0x3F);
                break;
            case 2:
                value = BinaryPrimitives.ReverseEndianness(Unsafe.ReadUnaligned<ushort>(ref source)) & 0x3FFFu;
                break;
            default:
                value = BinaryPrimitives.ReverseEndianness(Unsafe.ReadUnaligned<uint>(ref source)) & 0x3FFF_FFFFu;
                break;
        }

        bytesConsumed = shortLength;
        return true;

    Fail:
        value = 0;
        bytesConsumed = 0;
        return false;
    }

    private static void ThrowValueTooLarge() =>
        throw new ArgumentOutOfRangeException("value", "Value exceeds the largest QUIC varint (2^62 - 1).");

    private static void ThrowWriteFailed(int destinationLength, ulong value)
    {
        if (value > MaxValue)
        {
            ThrowValueTooLarge();
        }

        throw new ArgumentException($"Destination is too small ({destinationLength} bytes) for the varint encoding of {value}.", "destination");
    }
}
