using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics.X86;

namespace Tedd.Quicly.Replication;

/// <summary>
/// Packs values of arbitrary bit widths into a caller-supplied buffer, least significant bit first.
/// </summary>
/// <remarks>
/// <para>
/// <b>Bit order:</b> bit <c>k</c> of the stream is bit <c>k % 8</c> of byte <c>k / 8</c>; a value written with
/// <c>WriteBits(v, n)</c> occupies the next <c>n</c> stream bits, its least significant bit first. This is the order
/// <see cref="BitReader"/> reads.
/// </para>
/// <para>
/// Bits are gathered in a 64-bit accumulator and stored eight bytes at a time; <see cref="Flush"/> stores the
/// final partial word and returns the number of bytes used. <b>Call <see cref="Flush"/> before reading the
/// buffer.</b> Bytes of the destination beyond <see cref="Flush"/>'s result may have been written with zeros.
/// </para>
/// <para>
/// Running out of space never throws: the write is dropped, <see cref="HasOverflowed"/> becomes (and stays)
/// <see langword="true"/> and every later write is ignored, so a serializer checks once at the end. Only caller
/// errors (a bit count outside 0..64, an invalid quantization range) throw.
/// </para>
/// </remarks>
public ref struct BitWriter
{
    private readonly Span<byte> _buffer;
    private readonly long _capacityBits;
    private ulong _scratch;
    private int _scratchBits;
    private int _bytePosition;
    private bool _overflowed;

    /// <summary>Creates a writer over <paramref name="destination"/>, starting at bit 0.</summary>
    /// <param name="destination">The buffer to fill.</param>
    public BitWriter(Span<byte> destination)
    {
        _buffer = destination;
        _capacityBits = (long)destination.Length * 8;
        _scratch = 0;
        _scratchBits = 0;
        _bytePosition = 0;
        _overflowed = false;
    }

    /// <summary>Number of bits written so far.</summary>
    public readonly long BitPosition => ((long)_bytePosition << 3) + _scratchBits;

    /// <summary>Capacity of the destination in bits.</summary>
    public readonly long CapacityBits => _capacityBits;

    /// <summary>Bits still available.</summary>
    public readonly long RemainingBits => _capacityBits - BitPosition;

    /// <summary>Number of bytes the stream occupies so far (the last one possibly partial).</summary>
    public readonly int BytesWritten => (int)((BitPosition + 7) >> 3);

    /// <summary><see langword="true"/> once a write did not fit; the stream is then incomplete.</summary>
    public readonly bool HasOverflowed => _overflowed;

    /// <summary>Writes the low <paramref name="bits"/> bits of <paramref name="value"/> (higher bits are ignored).</summary>
    /// <param name="value">The value.</param>
    /// <param name="bits">0..64.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="bits"/> is outside 0..64.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteBits(ulong value, int bits)
    {
        if ((uint)bits > 64)
        {
            ThrowBits(bits);
        }

        int scratchBits = _scratchBits;
        if ((((long)_bytePosition << 3) + scratchBits + bits > _capacityBits) | _overflowed)
        {
            _overflowed = true;
            return;
        }

        value = Mask(value, bits);
        _scratch |= value << scratchBits;
        int total = scratchBits + bits;
        if (total >= 64)
        {
            // Capacity was checked above, so the eight bytes at _bytePosition are inside the buffer.
            Unsafe.WriteUnaligned(
                ref Unsafe.Add(ref MemoryMarshal.GetReference(_buffer), _bytePosition),
                BitConverter.IsLittleEndian ? _scratch : BinaryPrimitives.ReverseEndianness(_scratch));
            _bytePosition += 8;

            // The bits of value that did not fit: value >> (64 - scratchBits), written so that scratchBits == 0 gives 0.
            _scratch = (value >> 1) >> (63 - scratchBits);
            total -= 64;
        }

        _scratchBits = total;
    }

    /// <summary>Writes one bit.</summary>
    /// <param name="value">The bit.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void WriteBool(bool value) => WriteBits(value ? 1UL : 0UL, 1);

    /// <summary>Pads with zero bits up to the next byte boundary (no-op when already aligned).</summary>
    public void AlignToByte()
    {
        int pad = -_scratchBits & 7;
        if (pad != 0)
        {
            WriteBits(0, pad);
        }
    }

    /// <summary>Aligns to a byte boundary and copies <paramref name="source"/> verbatim.</summary>
    /// <param name="source">The bytes to append.</param>
    public void WriteBytes(ReadOnlySpan<byte> source)
    {
        AlignToByte();
        if ((BitPosition + (long)source.Length * 8 > _capacityBits) | _overflowed)
        {
            _overflowed = true;
            return;
        }

        // Move the whole bytes still held in the accumulator to the buffer, then copy directly.
        int pending = _scratchBits >> 3;
        ulong scratch = _scratch;
        for (int i = 0; i < pending; i++)
        {
            _buffer[_bytePosition++] = (byte)scratch;
            scratch >>= 8;
        }

        _scratch = 0;
        _scratchBits = 0;
        source.CopyTo(_buffer.Slice(_bytePosition));
        _bytePosition += source.Length;
    }

    /// <summary>
    /// Writes an unsigned variable-length integer: groups of 8 stream bits, each holding 7 value bits (least
    /// significant group first) and, in bit 7, a continuation flag. 1 group for values below 128, at most 10.
    /// </summary>
    /// <param name="value">The value.</param>
    public void WriteVarUInt(ulong value)
    {
        while (value >= 0x80)
        {
            WriteBits((value & 0x7F) | 0x80, 8);
            value >>= 7;
        }

        WriteBits(value, 8);
    }

    /// <summary>Writes a signed variable-length integer (<see cref="ZigZag"/> followed by <see cref="WriteVarUInt"/>).</summary>
    /// <param name="value">The value.</param>
    public void WriteVarInt(long value) => WriteVarUInt(ZigZag.Encode(value));

    /// <summary>Writes <see cref="Quantization.QuantizeFloat"/> of <paramref name="value"/> in <paramref name="bits"/> bits.</summary>
    /// <param name="value">The value (clamped to the range).</param>
    /// <param name="min">Lower bound of the range.</param>
    /// <param name="max">Upper bound of the range.</param>
    /// <param name="bits">1..32.</param>
    public void WriteQuantizedFloat(float value, float min, float max, int bits) =>
        WriteBits(Quantization.QuantizeFloat(value, min, max, bits), bits);

    /// <summary>Writes <see cref="Quantization.QuantizeUnitVector"/> of <paramref name="direction"/> in 2 × <paramref name="bitsPerComponent"/> bits.</summary>
    /// <param name="direction">The direction.</param>
    /// <param name="bitsPerComponent">2..16.</param>
    public void WriteUnitVector(Vector3 direction, int bitsPerComponent) =>
        WriteBits(Quantization.QuantizeUnitVector(direction, bitsPerComponent), 2 * bitsPerComponent);

    /// <summary>Writes <see cref="Quantization.QuantizeQuaternion"/> of <paramref name="rotation"/> in 2 + 3 × <paramref name="bitsPerComponent"/> bits.</summary>
    /// <param name="rotation">The rotation.</param>
    /// <param name="bitsPerComponent">2..20.</param>
    public void WriteQuaternion(Quaternion rotation, int bitsPerComponent) =>
        WriteBits(Quantization.QuantizeQuaternion(rotation, bitsPerComponent), 2 + 3 * bitsPerComponent);

    /// <summary>
    /// Stores the bits still held in the accumulator (the final partial word) and returns the number of bytes the
    /// stream occupies. Does not change the writer's state; writing may continue afterwards.
    /// </summary>
    /// <returns>Bytes used (see <see cref="BytesWritten"/>). When <see cref="HasOverflowed"/> is set the stream is incomplete.</returns>
    public readonly int Flush()
    {
        int count = (_scratchBits + 7) >> 3;
        Span<byte> tail = _buffer.Slice(_bytePosition, count);
        ulong scratch = _scratch;
        for (int i = 0; i < tail.Length; i++)
        {
            tail[i] = (byte)scratch;
            scratch >>= 8;
        }

        return _bytePosition + count;
    }

    /// <summary>Keeps the low <paramref name="bits"/> (0..64) bits of <paramref name="value"/>: one BZHI where available.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static ulong Mask(ulong value, int bits) =>
        Bmi2.X64.IsSupported ? Bmi2.X64.ZeroHighBits(value, (ulong)bits) : MaskPortable(value, bits);

    /// <summary>Portable <see cref="Mask"/> (C# masks shift counts to 6 bits, so 64 needs its own case).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static ulong MaskPortable(ulong value, int bits) => bits >= 64 ? value : value & ((1UL << bits) - 1);

    internal static void ThrowBits(int bits) =>
        throw new ArgumentOutOfRangeException(nameof(bits), bits, "Bit count must be in 0..64.");
}
