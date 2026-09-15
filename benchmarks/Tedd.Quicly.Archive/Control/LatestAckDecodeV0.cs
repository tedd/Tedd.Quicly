using System.Buffers.Binary;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Archive.Control;

/// <summary>
/// V0 of the LatestAck batch decode (superseded; see docs/benchmarks/control.md): whole-batch validation through a
/// general bounds-checked cursor, one <see cref="VarInt.TryReadMinimal(ReadOnlySpan{byte}, out ulong, out int)"/>
/// call per varint field, then a second decode pass with one <see cref="VarInt.TryRead(ReadOnlySpan{byte}, out ulong, out int)"/>
/// call per field. Semantically identical to the shipped version.
/// </summary>
public static class LatestAckDecodeV0
{
    private const int MinEntryLength = 6;

    public static ControlParseStatus TryParse(ReadOnlySpan<byte> body, out ReaderV0 reader)
    {
        reader = default;
        Cursor cursor = new(body);
        ControlParseStatus status = cursor.ReadVarInt(out ulong declared);
        if (status != ControlParseStatus.Ok)
        {
            return status;
        }

        if (declared > (ulong)(cursor.Remaining / MinEntryLength))
        {
            return ControlParseStatus.CountTooLarge;
        }

        int offset = cursor.Position;
        for (int i = 0; i < (int)declared; i++)
        {
            if ((status = cursor.ReadChannel()) != ControlParseStatus.Ok || (status = cursor.ReadVarInt(out _)) != ControlParseStatus.Ok)
            {
                return status;
            }

            if (!cursor.TrySkip(4))
            {
                return ControlParseStatus.Truncated;
            }
        }

        if (cursor.Remaining != 0)
        {
            return ControlParseStatus.TrailingBytes;
        }

        reader = new ReaderV0(body.Slice(offset), (int)declared);
        return ControlParseStatus.Ok;
    }

    public ref struct ReaderV0
    {
        private ReadOnlySpan<byte> _entries;
        private int _remaining;

        internal ReaderV0(ReadOnlySpan<byte> entries, int count)
        {
            _entries = entries;
            _remaining = count;
            Current = default;
        }

        public LatestAckEntry Current { get; private set; }

        public bool MoveNext()
        {
            if (_remaining == 0)
            {
                return false;
            }

            ReadOnlySpan<byte> span = _entries;
            VarInt.TryRead(span, out ulong channel, out int consumed);
            span = span.Slice(consumed);
            VarInt.TryRead(span, out ulong key, out consumed);
            uint version = BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(consumed));
            _entries = span.Slice(consumed + 4);
            Current = new LatestAckEntry((ushort)channel, key, version);
            _remaining--;
            return true;
        }

        public readonly ReaderV0 GetEnumerator() => this;
    }

    private ref struct Cursor(ReadOnlySpan<byte> source)
    {
        private readonly ReadOnlySpan<byte> _source = source;

        public int Position { get; private set; }

        public readonly int Remaining => _source.Length - Position;

        public bool TrySkip(int count)
        {
            if (Remaining < count)
            {
                return false;
            }

            Position += count;
            return true;
        }

        public ControlParseStatus ReadVarInt(out ulong value)
        {
            ReadOnlySpan<byte> rest = _source.Slice(Position);
            if (VarInt.TryReadMinimal(rest, out value, out int consumed))
            {
                Position += consumed;
                return ControlParseStatus.Ok;
            }

            return VarInt.TryRead(rest, out _, out _) ? ControlParseStatus.NonMinimalVarint : ControlParseStatus.Truncated;
        }

        public ControlParseStatus ReadChannel()
        {
            ControlParseStatus status = ReadVarInt(out ulong value);
            if (status != ControlParseStatus.Ok)
            {
                return status;
            }

            return value is < ControlCodec.MinChannelId or > ControlCodec.MaxChannelId ? ControlParseStatus.InvalidChannel : ControlParseStatus.Ok;
        }
    }
}
