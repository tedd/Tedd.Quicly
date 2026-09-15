using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace Tedd.Quicly.Archive.Primitives;

/// <summary>
/// ARCHIVED V0 of <c>Tedd.Quicly.Core.Primitives.VarInt</c>: if-chain length selection, switch-based read. Kept for benchmark comparison only.
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
/// </remarks>
public static class VarIntV0
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

        if (value <= MaxValue)
        {
            return 8;
        }

        ThrowValueTooLarge();
        return 0;
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
            Unsafe.WriteUnaligned(destination, BinaryPrimitives.ReverseEndianness((ushort)(value | 0x4000)));
            return 2;
        }

        if (value <= Max4)
        {
            Unsafe.WriteUnaligned(destination, BinaryPrimitives.ReverseEndianness((uint)value | 0x8000_0000u));
            return 4;
        }

        if (value <= MaxValue)
        {
            Unsafe.WriteUnaligned(destination, BinaryPrimitives.ReverseEndianness(value | 0xC000_0000_0000_0000ul));
            return 8;
        }

        ThrowValueTooLarge();
        return 0;
    }

    /// <summary>Attempts to decode a varint from the start of <paramref name="source"/>.</summary>
    /// <param name="source">The encoded bytes.</param>
    /// <param name="value">The decoded value, or 0 on failure.</param>
    /// <param name="bytesConsumed">The number of bytes consumed, or 0 on failure.</param>
    /// <returns><see langword="false"/> if <paramref name="source"/> is empty or shorter than the announced length.</returns>
    public static bool TryRead(ReadOnlySpan<byte> source, out ulong value, out int bytesConsumed)
    {
        if (source.Length == 0)
        {
            goto Fail;
        }

        byte first = source[0];
        int length = 1 << (first >> 6);
        if (source.Length < length)
        {
            goto Fail;
        }

        switch (length)
        {
            case 1:
                value = (ulong)(first & 0x3F);
                break;
            case 2:
                value = BinaryPrimitives.ReadUInt16BigEndian(source) & 0x3FFFu;
                break;
            case 4:
                value = BinaryPrimitives.ReadUInt32BigEndian(source) & 0x3FFF_FFFFu;
                break;
            default:
                value = BinaryPrimitives.ReadUInt64BigEndian(source) & MaxValue;
                break;
        }

        bytesConsumed = length;
        return true;

    Fail:
        value = 0;
        bytesConsumed = 0;
        return false;
    }

    /// <summary>Attempts to decode a varint from <paramref name="source"/> given <paramref name="available"/> readable bytes.</summary>
    /// <param name="source">Pointer to the first encoded byte.</param>
    /// <param name="available">The number of bytes readable at <paramref name="source"/>.</param>
    /// <param name="value">The decoded value, or 0 on failure.</param>
    /// <param name="bytesConsumed">The number of bytes consumed, or 0 on failure.</param>
    /// <returns><see langword="false"/> if fewer than the announced number of bytes are available.</returns>
    public static unsafe bool TryRead(byte* source, int available, out ulong value, out int bytesConsumed)
    {
        if (available <= 0)
        {
            goto Fail;
        }

        byte first = *source;
        int length = 1 << (first >> 6);
        if (available < length)
        {
            goto Fail;
        }

        switch (length)
        {
            case 1:
                value = (ulong)(first & 0x3F);
                break;
            case 2:
                value = BinaryPrimitives.ReverseEndianness(Unsafe.ReadUnaligned<ushort>(source)) & 0x3FFFu;
                break;
            case 4:
                value = BinaryPrimitives.ReverseEndianness(Unsafe.ReadUnaligned<uint>(source)) & 0x3FFF_FFFFu;
                break;
            default:
                value = BinaryPrimitives.ReverseEndianness(Unsafe.ReadUnaligned<ulong>(source)) & MaxValue;
                break;
        }

        bytesConsumed = length;
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
