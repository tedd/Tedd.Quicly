using System.Buffers.Binary;
using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Core.Control;

/// <summary>
/// Cursor over a destination whose size the caller has already checked against the exact encoded length.
/// (Span indexing still bounds-checks, so a sizing bug throws instead of corrupting memory.)
/// </summary>
internal ref struct ControlWriter
{
    private readonly Span<byte> _destination;
    private int _position;

    public ControlWriter(Span<byte> destination)
    {
        _destination = destination;
        _position = 0;
    }

    public readonly int Position => _position;

    public void WriteByte(byte value) => _destination[_position++] = value;

    public void WriteUInt16(ushort value)
    {
        BinaryPrimitives.WriteUInt16LittleEndian(_destination.Slice(_position), value);
        _position += 2;
    }

    public void WriteUInt32(uint value)
    {
        BinaryPrimitives.WriteUInt32LittleEndian(_destination.Slice(_position), value);
        _position += 4;
    }

    public void WriteUInt64(ulong value)
    {
        BinaryPrimitives.WriteUInt64LittleEndian(_destination.Slice(_position), value);
        _position += 8;
    }

    public void WriteVarInt(ulong value) => _position += VarInt.Write(_destination.Slice(_position), value);

    public void WriteBytes(ReadOnlySpan<byte> value)
    {
        value.CopyTo(_destination.Slice(_position));
        _position += value.Length;
    }

    public void WriteLengthPrefixed(ReadOnlySpan<byte> value)
    {
        WriteVarInt((ulong)value.Length);
        WriteBytes(value);
    }
}
