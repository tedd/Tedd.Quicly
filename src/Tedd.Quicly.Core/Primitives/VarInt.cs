using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Tedd.Quicly.Core.Primitives;

/// <summary>
/// QUIC variable-length integer codec (RFC 9000 §16).
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
/// Encoding selects the length with a short chain of range compares (measured faster than an lzcnt +
/// lookup-table variant, see <c>docs/benchmarks/primitives.md</c>). Decoding uses one 8-byte big-endian load
/// plus a shift whenever 8 bytes are readable, so the common read path does not branch on the length.
/// <see cref="TryRead(ReadOnlySpan{byte}, out ulong, out int)"/> accepts any well-formed encoding;
/// <see cref="TryReadMinimal(ReadOnlySpan{byte}, out ulong, out int)"/> additionally rejects non-minimal
/// encodings and is the variant protocol parsers use.
/// </para>
/// </remarks>
public static class VarInt
{
    /// <summary>The largest value that can be encoded as a QUIC varint (2^62 - 1).</summary>
    public const ulong MaxValue = (1UL << 62) - 1;

    /// <summary>The largest encoded size in bytes.</summary>
    public const int MaxLength = 8;

    private const ulong Max1 = (1UL << 6) - 1;
    private const ulong Max2 = (1UL << 14) - 1;
    private const ulong Max4 = (1UL << 30) - 1;

    /// <summary>Returns the number of bytes (1, 2, 4 or 8) needed to encode <paramref name="value"/>.</summary>
    /// <param name="value">The value to measure; must not exceed <see cref="MaxValue"/>.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="value"/> exceeds <see cref="MaxValue"/>.</exception>
    public static int GetLength(ulong value)
    {
        if (value <= Max1)
        {
            return 1;
        }

        if (value <= Max2)
        {
            return 2;
        }

        if (value <= Max4)
        {
            return 4;
        }

        if (value > MaxValue)
        {
            ThrowValueTooLarge();
        }

        return 8;
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
        // Plain span stores through BinaryPrimitives (the JIT folds their length checks into ours); measured
        // faster than a hoisted `ref byte` + Unsafe.WriteUnaligned on the 4/8-byte and mixed sets.
        if (value <= Max1)
        {
            if (destination.Length < 1)
            {
                goto Fail;
            }

            destination[0] = (byte)value;
            bytesWritten = 1;
            return true;
        }

        if (value <= Max2)
        {
            if (destination.Length < 2)
            {
                goto Fail;
            }

            BinaryPrimitives.WriteUInt16BigEndian(destination, (ushort)(value | 0x4000));
            bytesWritten = 2;
            return true;
        }

        if (value <= Max4)
        {
            if (destination.Length < 4)
            {
                goto Fail;
            }

            BinaryPrimitives.WriteUInt32BigEndian(destination, (uint)value | 0x8000_0000u);
            bytesWritten = 4;
            return true;
        }

        if (value <= MaxValue)
        {
            if (destination.Length < 8)
            {
                goto Fail;
            }

            BinaryPrimitives.WriteUInt64BigEndian(destination, value | 0xC000_0000_0000_0000ul);
            bytesWritten = 8;
            return true;
        }

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
        if (value <= Max1)
        {
            *destination = (byte)value;
            return 1;
        }

        if (value <= Max2)
        {
            Unsafe.WriteUnaligned(destination, ToBigEndian((ushort)(value | 0x4000)));
            return 2;
        }

        if (value <= Max4)
        {
            Unsafe.WriteUnaligned(destination, ToBigEndian((uint)value | 0x8000_0000u));
            return 4;
        }

        if (value > MaxValue)
        {
            ThrowValueTooLarge();
        }

        Unsafe.WriteUnaligned(destination, ToBigEndian(value | 0xC000_0000_0000_0000ul));
        return 8;
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

    /// <summary>
    /// Attempts to decode a varint from the start of <paramref name="source"/>, rejecting non-minimal encodings
    /// (a value written with more bytes than <see cref="GetLength"/> requires), as PROTOCOL.md demands of receivers.
    /// </summary>
    /// <param name="source">The encoded bytes.</param>
    /// <param name="value">The decoded value, or 0 on failure.</param>
    /// <param name="bytesConsumed">The number of bytes consumed, or 0 on failure.</param>
    /// <returns>
    /// <see langword="false"/> if <paramref name="source"/> is empty, shorter than the announced length, or the
    /// encoding is not the shortest one for the value.
    /// </returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryReadMinimal(ReadOnlySpan<byte> source, out ulong value, out int bytesConsumed) =>
        TryReadMinimalCore(ref MemoryMarshal.GetReference(source), source.Length, out value, out bytesConsumed);

    /// <summary>
    /// Attempts to decode a varint from <paramref name="source"/> given <paramref name="available"/> readable bytes,
    /// rejecting non-minimal encodings (see <see cref="TryReadMinimal(ReadOnlySpan{byte}, out ulong, out int)"/>).
    /// </summary>
    /// <param name="source">Pointer to the first encoded byte.</param>
    /// <param name="available">The number of bytes readable at <paramref name="source"/>.</param>
    /// <param name="value">The decoded value, or 0 on failure.</param>
    /// <param name="bytesConsumed">The number of bytes consumed, or 0 on failure.</param>
    /// <returns><see langword="false"/> if the input is truncated or the encoding is not minimal.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static unsafe bool TryReadMinimal(byte* source, int available, out ulong value, out int bytesConsumed) =>
        TryReadMinimalCore(ref *source, available, out value, out bytesConsumed);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool TryReadMinimalCore(ref byte source, int available, out ulong value, out int bytesConsumed)
    {
        if (!TryReadCore(ref source, available, out value, out bytesConsumed))
        {
            return false;
        }

        // An encoding of length L is minimal iff the value does not fit in length L/2, i.e. value >= 2^(4L-2)
        // (64 for L = 2, 16384 for L = 4, 2^30 for L = 8); a 1-byte encoding is always minimal. Evaluated
        // without a data-dependent branch: the shift is non-zero exactly when the value is large enough, and
        // the 1-byte case forces the test true via the OR.
        ulong minimal = (value >> (4 * bytesConsumed - 2)) | Unsafe.BitCast<bool, byte>(bytesConsumed == 1);
        if (minimal != 0)
        {
            return true;
        }

        value = 0;
        bytesConsumed = 0;
        return false;
    }

    private static bool TryReadCore(ref byte source, int available, out ulong value, out int bytesConsumed)
    {
        if (available >= 8)
        {
            // One wide load: the prefix lands in the top two bits, the value in the top `length` bytes.
            ulong bigEndian = FromBigEndian(Unsafe.ReadUnaligned<ulong>(ref source));
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
                value = FromBigEndian(Unsafe.ReadUnaligned<ushort>(ref source)) & 0x3FFFu;
                break;
            default:
                value = FromBigEndian(Unsafe.ReadUnaligned<uint>(ref source)) & 0x3FFF_FFFFu;
                break;
        }

        bytesConsumed = shortLength;
        return true;

    Fail:
        value = 0;
        bytesConsumed = 0;
        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ushort ToBigEndian(ushort v) => BitConverter.IsLittleEndian ? BinaryPrimitives.ReverseEndianness(v) : v;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint ToBigEndian(uint v) => BitConverter.IsLittleEndian ? BinaryPrimitives.ReverseEndianness(v) : v;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong ToBigEndian(ulong v) => BitConverter.IsLittleEndian ? BinaryPrimitives.ReverseEndianness(v) : v;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ushort FromBigEndian(ushort v) => ToBigEndian(v);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint FromBigEndian(uint v) => ToBigEndian(v);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong FromBigEndian(ulong v) => ToBigEndian(v);

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
