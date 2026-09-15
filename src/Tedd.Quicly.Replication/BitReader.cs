using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Tedd.Quicly.Replication;

/// <summary>
/// Reads a bit stream produced by <see cref="BitWriter"/> (least significant bit first).
/// </summary>
/// <remarks>
/// Never reads outside the buffer and never throws on malformed input: reading past the end, or a malformed
/// variable-length integer, returns 0 (or an empty/zeroed result), sets <see cref="HasError"/> and makes every
/// later read fail the same way, so a deserializer checks once at the end. Only caller errors (a bit count
/// outside 0..64, an invalid quantization range) throw.
/// </remarks>
public ref struct BitReader
{
    private readonly ReadOnlySpan<byte> _buffer;
    private readonly long _capacityBits;
    private long _bitPosition;
    private bool _error;

    /// <summary>Creates a reader over <paramref name="source"/>, starting at bit 0.</summary>
    /// <param name="source">The encoded bytes.</param>
    public BitReader(ReadOnlySpan<byte> source)
    {
        _buffer = source;
        _capacityBits = (long)source.Length * 8;
        _bitPosition = 0;
        _error = false;
    }

    /// <summary>Number of bits consumed so far.</summary>
    public readonly long BitPosition => _bitPosition;

    /// <summary>Bits not yet consumed.</summary>
    public readonly long RemainingBits => _capacityBits - _bitPosition;

    /// <summary><see langword="true"/> once a read ran past the end of the buffer or decoded a malformed varint.</summary>
    public readonly bool HasError => _error;

    /// <summary>Reads <paramref name="bits"/> bits as an unsigned value.</summary>
    /// <param name="bits">0..64.</param>
    /// <returns>The value, or 0 when fewer than <paramref name="bits"/> bits remain (sets <see cref="HasError"/>).</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="bits"/> is outside 0..64.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ulong ReadBits(int bits)
    {
        if ((uint)bits > 64)
        {
            BitWriter.ThrowBits(bits);
        }

        long position = _bitPosition;
        if ((position + bits > _capacityBits) | _error)
        {
            _error = true;
            return 0;
        }

        int byteIndex = (int)(position >> 3);
        int shift = (int)position & 7;
        ref byte first = ref MemoryMarshal.GetReference(_buffer);
        ulong raw;
        if (byteIndex + 8 <= _buffer.Length)
        {
            raw = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref first, byteIndex));
            raw = BitConverter.IsLittleEndian ? raw : BinaryPrimitives.ReverseEndianness(raw);
            raw >>= shift;
            if (shift + bits > 64)
            {
                // A 64-bit read that starts mid-byte spans nine bytes; the capacity check guarantees the ninth exists.
                raw |= (ulong)Unsafe.Add(ref first, byteIndex + 8) << (64 - shift);
            }
        }
        else
        {
            raw = 0;
            for (int i = byteIndex, s = 0; i < _buffer.Length; i++, s += 8)
            {
                raw |= (ulong)Unsafe.Add(ref first, i) << s;
            }

            raw >>= shift;
        }

        _bitPosition = position + bits;
        return BitWriter.Mask(raw, bits);
    }

    /// <summary>Reads one bit.</summary>
    /// <returns>The bit, or <see langword="false"/> past the end.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool ReadBool() => ReadBits(1) != 0;

    /// <summary>Skips to the next byte boundary (no-op when already aligned).</summary>
    public void AlignToByte()
    {
        // The capacity is a whole number of bytes, so aligning never passes the end.
        _bitPosition += -_bitPosition & 7;
    }

    /// <summary>Aligns to a byte boundary and copies the next <paramref name="destination"/>.Length bytes.</summary>
    /// <param name="destination">Receives the bytes; cleared when they are not available (sets <see cref="HasError"/>).</param>
    public void ReadBytes(Span<byte> destination)
    {
        ReadOnlySpan<byte> source = ReadByteSpan(destination.Length);
        if (_error)
        {
            destination.Clear();
            return;
        }

        source.CopyTo(destination);
    }

    /// <summary>Aligns to a byte boundary and returns the next <paramref name="count"/> bytes without copying.</summary>
    /// <param name="count">Number of bytes; must be non-negative.</param>
    /// <returns>A slice of the source buffer, or an empty span when the bytes are not available (sets <see cref="HasError"/>).</returns>
    public ReadOnlySpan<byte> ReadByteSpan(int count)
    {
        AlignToByte();
        if (count < 0 || (_bitPosition + (long)count * 8 > _capacityBits) | _error)
        {
            _error = true;
            return default;
        }

        ReadOnlySpan<byte> slice = _buffer.Slice((int)(_bitPosition >> 3), count);
        _bitPosition += (long)count * 8;
        return slice;
    }

    /// <summary>
    /// Reads a variable-length integer written by <see cref="BitWriter.WriteVarUInt"/>. More than 64 value bits and
    /// non-minimal encodings (a final group of zero after a continuation) are malformed.
    /// </summary>
    /// <returns>The value, or 0 on error (sets <see cref="HasError"/>).</returns>
    public ulong ReadVarUInt()
    {
        ulong result = 0;
        for (int shift = 0; ; shift += 7)
        {
            ulong group = ReadBits(8);
            if (_error)
            {
                return 0;
            }

            ulong data = group & 0x7F;
            bool more = (group & 0x80) != 0;
            if (shift == 63 && group > 1)
            {
                // The tenth group may only carry the single remaining value bit, without a continuation.
                break;
            }

            result |= data << shift;
            if (!more)
            {
                if (data == 0 && shift != 0)
                {
                    break;
                }

                return result;
            }
        }

        _error = true;
        return 0;
    }

    /// <summary>Reads a signed variable-length integer written by <see cref="BitWriter.WriteVarInt"/>.</summary>
    /// <returns>The value, or 0 on error.</returns>
    public long ReadVarInt() => ZigZag.Decode(ReadVarUInt());

    /// <summary>Reads a value written by <see cref="BitWriter.WriteQuantizedFloat"/>.</summary>
    /// <param name="min">Lower bound of the range.</param>
    /// <param name="max">Upper bound of the range.</param>
    /// <param name="bits">1..32.</param>
    /// <returns>The dequantized value (<paramref name="min"/> on error).</returns>
    public float ReadQuantizedFloat(float min, float max, int bits)
    {
        uint q = (uint)ReadBits(bits);
        return Quantization.DequantizeFloat(q, min, max, bits);
    }

    /// <summary>Reads a direction written by <see cref="BitWriter.WriteUnitVector"/>.</summary>
    /// <param name="bitsPerComponent">2..16.</param>
    /// <returns>The unit vector.</returns>
    public Vector3 ReadUnitVector(int bitsPerComponent)
    {
        uint q = (uint)ReadBits(2 * bitsPerComponent);
        return Quantization.DequantizeUnitVector(q, bitsPerComponent);
    }

    /// <summary>Reads a rotation written by <see cref="BitWriter.WriteQuaternion"/>.</summary>
    /// <param name="bitsPerComponent">2..20.</param>
    /// <returns>The unit quaternion.</returns>
    public Quaternion ReadQuaternion(int bitsPerComponent)
    {
        ulong q = ReadBits(2 + 3 * bitsPerComponent);
        return Quantization.DequantizeQuaternion(q, bitsPerComponent);
    }
}
