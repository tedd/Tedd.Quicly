using System.Buffers.Binary;
using System.Text.Unicode;
using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Core.Control;

/// <summary>Bounds-checked cursor over a control body. Never throws; every read reports failure.</summary>
internal ref struct ControlReader
{
    private readonly ReadOnlySpan<byte> _source;
    private int _position;

    public ControlReader(ReadOnlySpan<byte> source)
    {
        _source = source;
        _position = 0;
    }

    public readonly int Position => _position;

    public readonly int Remaining => _source.Length - _position;

    public readonly ReadOnlySpan<byte> Rest => _source.Slice(_position);

    public bool TryReadByte(out byte value)
    {
        if (_position < _source.Length)
        {
            value = _source[_position++];
            return true;
        }

        value = 0;
        return false;
    }

    public bool TryReadUInt16(out ushort value)
    {
        if (Remaining >= 2)
        {
            value = BinaryPrimitives.ReadUInt16LittleEndian(_source.Slice(_position));
            _position += 2;
            return true;
        }

        value = 0;
        return false;
    }

    public bool TryReadUInt32(out uint value)
    {
        if (Remaining >= 4)
        {
            value = BinaryPrimitives.ReadUInt32LittleEndian(_source.Slice(_position));
            _position += 4;
            return true;
        }

        value = 0;
        return false;
    }

    public bool TryReadUInt64(out ulong value)
    {
        if (Remaining >= 8)
        {
            value = BinaryPrimitives.ReadUInt64LittleEndian(_source.Slice(_position));
            _position += 8;
            return true;
        }

        value = 0;
        return false;
    }

    public bool TrySkip(int count)
    {
        if (Remaining >= count)
        {
            _position += count;
            return true;
        }

        return false;
    }

    public ReadOnlySpan<byte> ReadSpan(int length)
    {
        ReadOnlySpan<byte> span = _source.Slice(_position, length);
        _position += length;
        return span;
    }

    public ControlParseStatus ReadVarInt(out ulong value)
    {
        ReadOnlySpan<byte> rest = _source.Slice(_position);
        if (VarInt.TryReadMinimal(rest, out value, out int consumed))
        {
            _position += consumed;
            return ControlParseStatus.Ok;
        }

        return VarInt.TryRead(rest, out _, out _) ? ControlParseStatus.NonMinimalVarint : ControlParseStatus.Truncated;
    }

    public ControlParseStatus ReadChannel(out ushort channel)
    {
        ControlParseStatus status = ReadVarInt(out ulong value);
        if (status != ControlParseStatus.Ok)
        {
            channel = 0;
            return status;
        }

        if (value - ControlCodec.MinChannelId > ControlCodec.MaxChannelId - ControlCodec.MinChannelId)
        {
            channel = 0;
            return ControlParseStatus.InvalidChannel;
        }

        channel = (ushort)value;
        return ControlParseStatus.Ok;
    }

    public ControlParseStatus ReadLengthPrefixed(int maxLength, out ReadOnlySpan<byte> bytes)
    {
        ControlParseStatus status = ReadVarInt(out ulong length);
        if (status != ControlParseStatus.Ok)
        {
            bytes = default;
            return status;
        }

        if (length > (ulong)maxLength)
        {
            bytes = default;
            return ControlParseStatus.FieldTooLong;
        }

        if (length > (ulong)Remaining)
        {
            bytes = default;
            return ControlParseStatus.Truncated;
        }

        bytes = ReadSpan((int)length);
        return ControlParseStatus.Ok;
    }

    public ControlParseStatus ReadUtf8(int maxLength, out ReadOnlySpan<byte> text)
    {
        ControlParseStatus status = ReadLengthPrefixed(maxLength, out text);
        if (status == ControlParseStatus.Ok && !Utf8.IsValid(text))
        {
            text = default;
            return ControlParseStatus.InvalidUtf8;
        }

        return status;
    }

    public readonly ControlParseStatus End() => _position == _source.Length ? ControlParseStatus.Ok : ControlParseStatus.TrailingBytes;
}
