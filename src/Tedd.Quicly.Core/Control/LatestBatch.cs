using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Core.Control;

/// <summary>
/// Appends LatestAck entries (PROTOCOL.md §2.3, type 0x03) to one control frame until the destination, the stream
/// frame limit or <c>maxEntries</c> is reached, then <see cref="Finish"/> writes the header. Allocation-free.
/// </summary>
/// <remarks>
/// Entries are written after a header area reserved for the largest possible <c>Length</c>/<c>count</c> varints;
/// <see cref="Finish"/> writes the minimal header and moves the entries down when the reservation was larger, so
/// the frame always starts at offset 0 of the destination. The writer does not de-duplicate (the ack coalescer
/// keeps the highest version per key before writing).
/// </remarks>
public ref struct LatestAckBatchWriter
{
    private LatestBatchWriterCore _core;

    /// <summary>Starts a batch.</summary>
    /// <param name="destination">Receives the frame (for datagrams: sized to the transport's current maximum datagram payload).</param>
    /// <param name="carrier">Datagram or stream framing (a stream frame is additionally capped at 16 384 bytes).</param>
    /// <param name="maxEntries">Largest number of entries to accept (≥ 1).</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="carrier"/> is undefined or <paramref name="maxEntries"/> &lt; 1.</exception>
    public LatestAckBatchWriter(Span<byte> destination, ControlCarrier carrier, int maxEntries = int.MaxValue) =>
        _core = new LatestBatchWriterCore(destination, carrier, ControlType.LatestAck, maxEntries, ControlCodec.LatestAckMinEntryLength);

    /// <summary>Number of entries added so far.</summary>
    public readonly int Count => _core.Count;

    /// <summary>Appends an entry.</summary>
    /// <param name="entry">The entry.</param>
    /// <returns><see langword="false"/> when the entry does not fit (send this batch and start another).</returns>
    /// <exception cref="ArgumentOutOfRangeException">The channel is outside [2, 16383] or the key exceeds 2^62 − 1.</exception>
    /// <exception cref="InvalidOperationException"><see cref="Finish"/> was already called.</exception>
    public bool TryAdd(in LatestAckEntry entry) => _core.TryAdd(entry.Channel, entry.Key, entry.Version, 0, withReason: false);

    /// <summary>Writes the frame header and returns the frame length (0 when no entry was added: nothing to send).</summary>
    /// <exception cref="InvalidOperationException"><see cref="Finish"/> was already called.</exception>
    public int Finish() => _core.Finish();
}

/// <summary>
/// Appends LatestReject entries (PROTOCOL.md §2.3, type 0x04) to one control frame; see <see cref="LatestAckBatchWriter"/>.
/// </summary>
public ref struct LatestRejectBatchWriter
{
    private LatestBatchWriterCore _core;

    /// <summary>Starts a batch.</summary>
    /// <param name="destination">Receives the frame.</param>
    /// <param name="carrier">Datagram or stream framing (a stream frame is additionally capped at 16 384 bytes).</param>
    /// <param name="maxEntries">Largest number of entries to accept (≥ 1).</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="carrier"/> is undefined or <paramref name="maxEntries"/> &lt; 1.</exception>
    public LatestRejectBatchWriter(Span<byte> destination, ControlCarrier carrier, int maxEntries = int.MaxValue) =>
        _core = new LatestBatchWriterCore(destination, carrier, ControlType.LatestReject, maxEntries, ControlCodec.LatestRejectMinEntryLength);

    /// <summary>Number of entries added so far.</summary>
    public readonly int Count => _core.Count;

    /// <summary>Appends an entry.</summary>
    /// <param name="entry">The entry.</param>
    /// <returns><see langword="false"/> when the entry does not fit (send this batch and start another).</returns>
    /// <exception cref="ArgumentOutOfRangeException">The channel is outside [2, 16383], the key exceeds 2^62 − 1 or the reason is undefined.</exception>
    /// <exception cref="InvalidOperationException"><see cref="Finish"/> was already called.</exception>
    public bool TryAdd(in LatestRejectEntry entry) => _core.TryAdd(entry.Channel, entry.Key, entry.Version, entry.Reason, withReason: true);

    /// <summary>Writes the frame header and returns the frame length (0 when no entry was added: nothing to send).</summary>
    /// <exception cref="InvalidOperationException"><see cref="Finish"/> was already called.</exception>
    public int Finish() => _core.Finish();
}

/// <summary>
/// Enumerates the entries of a LatestAck batch validated by
/// <see cref="ControlCodec.TryParse(ReadOnlySpan{byte}, out LatestAckBatchReader)"/>. Use with <c>foreach</c>; each
/// <c>foreach</c> starts from the first entry. Allocation-free.
/// </summary>
public ref struct LatestAckBatchReader
{
    private ReadOnlySpan<byte> _entries;
    private int _remaining;
    private LatestAckEntry _current;

    internal LatestAckBatchReader(ReadOnlySpan<byte> entries, int count)
    {
        _entries = entries;
        _remaining = count;
        _current = default;
        Count = count;
    }

    /// <summary>Number of entries in the batch.</summary>
    public int Count { get; }

    /// <summary>The current entry (valid after <see cref="MoveNext"/> returned <see langword="true"/>).</summary>
    public readonly LatestAckEntry Current => _current;

    /// <summary>Advances to the next entry.</summary>
    /// <returns><see langword="false"/> after the last entry.</returns>
    public bool MoveNext()
    {
        if (_remaining == 0)
        {
            return false;
        }

        LatestBatchFormat.ReadEntry(ref _entries, out ushort channel, out ulong key, out uint version);
        _current = new LatestAckEntry(channel, key, version);
        _remaining--;
        return true;
    }

    /// <summary>Returns a copy positioned before the first entry (enables <c>foreach</c>).</summary>
    public readonly LatestAckBatchReader GetEnumerator() => this;
}

/// <summary>
/// Enumerates the entries of a LatestReject batch validated by
/// <see cref="ControlCodec.TryParse(ReadOnlySpan{byte}, out LatestRejectBatchReader)"/>. Use with <c>foreach</c>.
/// Allocation-free.
/// </summary>
public ref struct LatestRejectBatchReader
{
    private ReadOnlySpan<byte> _entries;
    private int _remaining;
    private LatestRejectEntry _current;

    internal LatestRejectBatchReader(ReadOnlySpan<byte> entries, int count)
    {
        _entries = entries;
        _remaining = count;
        _current = default;
        Count = count;
    }

    /// <summary>Number of entries in the batch.</summary>
    public int Count { get; }

    /// <summary>The current entry (valid after <see cref="MoveNext"/> returned <see langword="true"/>).</summary>
    public readonly LatestRejectEntry Current => _current;

    /// <summary>Advances to the next entry.</summary>
    /// <returns><see langword="false"/> after the last entry.</returns>
    public bool MoveNext()
    {
        if (_remaining == 0)
        {
            return false;
        }

        LatestBatchFormat.ReadEntry(ref _entries, out ushort channel, out ulong key, out uint version);
        LatestRejectReason reason = (LatestRejectReason)_entries[0];
        _entries = _entries.Slice(1);
        _current = new LatestRejectEntry(channel, key, version, reason);
        _remaining--;
        return true;
    }

    /// <summary>Returns a copy positioned before the first entry (enables <c>foreach</c>).</summary>
    public readonly LatestRejectBatchReader GetEnumerator() => this;
}

/// <summary>
/// Varint handling for ack batches, the one control message sent continuously (every AckDelay per peer). The 1- and
/// 2-byte encodings (every minimal channel id, small keys) are decoded inline; longer ones fall back to
/// <see cref="VarInt"/>. Measured against the general-cursor V0 in docs/benchmarks/control.md.
/// </summary>
internal static class LatestBatchFormat
{
    /// <summary>Validating read: minimal encodings only; never reads outside <paramref name="source"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ControlParseStatus ReadVarInt(ReadOnlySpan<byte> source, ref int position, out ulong value)
    {
        int p = position;
        if ((uint)p < (uint)source.Length)
        {
            uint first = source[p];
            if (first < 0x40)
            {
                value = first;
                position = p + 1;
                return ControlParseStatus.Ok;
            }

            if (first < 0x80 && (uint)(p + 1) < (uint)source.Length)
            {
                value = ((first & 0x3F) << 8) | source[p + 1];
                if (value >= 64)
                {
                    position = p + 2;
                    return ControlParseStatus.Ok;
                }

                value = 0;
                return ControlParseStatus.NonMinimalVarint;
            }
        }

        return ReadVarIntSlow(source, ref position, out value);
    }

    /// <summary>Decodes one entry of a batch that <see cref="ControlCodec"/> has already validated.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void ReadEntry(ref ReadOnlySpan<byte> entries, out ushort channel, out ulong key, out uint version)
    {
        ReadOnlySpan<byte> span = entries;
        int position = 0;
        channel = (ushort)ReadTrusted(span, ref position);
        key = ReadTrusted(span, ref position);
        version = BinaryPrimitives.ReadUInt32LittleEndian(span.Slice(position));
        entries = span.Slice(position + 4);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong ReadTrusted(ReadOnlySpan<byte> source, ref int position)
    {
        int p = position;
        uint first = source[p];
        if (first < 0x40)
        {
            position = p + 1;
            return first;
        }

        if (first < 0x80)
        {
            position = p + 2;
            return ((first & 0x3F) << 8) | source[p + 1];
        }

        VarInt.TryRead(source.Slice(p), out ulong value, out int consumed);
        position = p + consumed;
        return value;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static ControlParseStatus ReadVarIntSlow(ReadOnlySpan<byte> source, ref int position, out ulong value)
    {
        ReadOnlySpan<byte> rest = source.Slice(position);
        if (VarInt.TryReadMinimal(rest, out value, out int consumed))
        {
            position += consumed;
            return ControlParseStatus.Ok;
        }

        return VarInt.TryRead(rest, out _, out _) ? ControlParseStatus.NonMinimalVarint : ControlParseStatus.Truncated;
    }
}

/// <summary>Shared implementation of the two batch writers.</summary>
internal ref struct LatestBatchWriterCore
{
    private readonly Span<byte> _destination;
    private readonly int _maxEntries;
    private readonly int _entriesStart;
    private readonly ControlCarrier _carrier;
    private readonly ControlType _type;
    private int _position;
    private int _count;
    private bool _finished;

    public LatestBatchWriterCore(Span<byte> destination, ControlCarrier carrier, ControlType type, int maxEntries, int minEntryLength)
    {
        if (carrier is not (ControlCarrier.Datagram or ControlCarrier.Stream))
        {
            throw new ArgumentOutOfRangeException(nameof(carrier), carrier, "Unknown control carrier.");
        }

        ArgumentOutOfRangeException.ThrowIfLessThan(maxEntries, 1);
        _destination = destination;
        _carrier = carrier;
        _type = type;
        _maxEntries = maxEntries;

        // Reserve room for the largest header this batch can need; Finish shifts the entries down if it needs less.
        int usable = carrier == ControlCarrier.Stream ? Math.Min(destination.Length, ControlCodec.MaxEncodedStreamFrameLength) : destination.Length;
        int headerReserve = carrier == ControlCarrier.Datagram ? 2 : VarInt.GetLength((ulong)Math.Min(usable, ControlCodec.MaxFrameLength)) + 1;
        int countReserve = VarInt.GetLength((ulong)Math.Min(maxEntries, usable / minEntryLength));
        _entriesStart = headerReserve + countReserve;
        _position = _entriesStart;
        _count = 0;
        _finished = false;
    }

    public readonly int Count => _count;

    public bool TryAdd(ushort channel, ulong key, uint version, LatestRejectReason reason, bool withReason)
    {
        if (_finished)
        {
            throw new InvalidOperationException("The batch has already been finished.");
        }

        if (channel < ControlCodec.MinChannelId || channel > ControlCodec.MaxChannelId)
        {
            throw new ArgumentOutOfRangeException(nameof(channel), channel, "Channel ids are 2..16383.");
        }

        if (withReason && !ControlCodec.IsDefined(reason))
        {
            throw new ArgumentOutOfRangeException(nameof(reason), reason, "Undefined LatestReject reason.");
        }

        int keyLength = VarInt.GetLength(key);
        if (_count >= _maxEntries)
        {
            return false;
        }

        int entryLength = VarInt.GetLength(channel) + keyLength + (withReason ? 5 : 4);
        int end = _position + entryLength;
        if (end > _destination.Length)
        {
            return false;
        }

        if (_carrier == ControlCarrier.Stream
            && 1 + VarInt.GetLength((ulong)(_count + 1)) + (end - _entriesStart) > ControlCodec.MaxFrameLength)
        {
            return false;
        }

        Span<byte> entry = _destination.Slice(_position, entryLength);
        int offset = VarInt.Write(entry, channel);
        offset += VarInt.Write(entry.Slice(offset), key);
        BinaryPrimitives.WriteUInt32LittleEndian(entry.Slice(offset), version);
        if (withReason)
        {
            entry[offset + 4] = (byte)reason;
        }

        _position = end;
        _count++;
        return true;
    }

    public int Finish()
    {
        if (_finished)
        {
            throw new InvalidOperationException("The batch has already been finished.");
        }

        _finished = true;
        if (_count == 0)
        {
            return 0;
        }

        int entriesLength = _position - _entriesStart;
        int countLength = VarInt.GetLength((ulong)_count);
        int frameLength = 1 + countLength + entriesLength;
        int headerLength = _carrier == ControlCarrier.Datagram ? 2 : VarInt.GetLength((ulong)frameLength) + 1;
        int start = headerLength + countLength;
        if (start != _entriesStart)
        {
            // Overlapping move towards the start; Span.CopyTo has memmove semantics.
            _destination.Slice(_entriesStart, entriesLength).CopyTo(_destination.Slice(start));
        }

        ControlWriter writer = new(_destination);
        if (_carrier == ControlCarrier.Datagram)
        {
            writer.WriteByte(0);
        }
        else
        {
            writer.WriteVarInt((ulong)frameLength);
        }

        writer.WriteByte((byte)_type);
        writer.WriteVarInt((ulong)_count);
        return start + entriesLength;
    }
}
