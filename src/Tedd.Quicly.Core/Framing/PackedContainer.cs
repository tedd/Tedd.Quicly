using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Core.Framing;

/// <summary>
/// Packed container codec (PROTOCOL.md §2.2): <c>0x01, Flags u8 (bit0 = Tick present), [Tick varint],
/// repeat (Length varint ≥ 1, Message)</c>, where each message is a complete channel-0 or channel-≥2 datagram frame.
/// </summary>
public static class PackedContainer
{
    /// <summary>Channel id of a packed container.</summary>
    public const byte ChannelId = 1;

    /// <summary>
    /// Most messages <em>this implementation packs</em> into one container (PROTOCOL.md §2.2/§8). It is a sender-side
    /// limit only: <see cref="TryParse"/> accepts as many messages as a datagram holds, because a conformant peer may
    /// pack more (about 599 two-byte messages fit 1 200 bytes).
    /// </summary>
    public const int MaxMessages = 64;

    /// <summary>Flags bit: a tick varint follows the flags byte.</summary>
    public const byte FlagTick = 0x01;

    /// <summary>Returns the container header size.</summary>
    /// <param name="tick">The sender's flush tick.</param>
    /// <param name="hasTick">Whether the tick is written.</param>
    public static int GetHeaderLength(uint tick, bool hasTick) => hasTick ? 2 + VarInt.GetLength(tick) : 2;

    /// <summary>Returns the bytes one message of <paramref name="messageLength"/> bytes costs inside a container (length varint + message).</summary>
    /// <param name="messageLength">Encoded message size (header + payload), ≥ 1.</param>
    /// <returns>The entry size, computed in 64 bits so that no <see cref="int"/> length can overflow it.</returns>
    public static long GetEntryLength(int messageLength) => WireReader.VarIntLength((uint)messageLength) + (long)messageLength;

    /// <summary>Writes the container header.</summary>
    /// <param name="destination">Buffer of at least <see cref="GetHeaderLength"/> bytes.</param>
    /// <param name="tick">The sender's flush tick.</param>
    /// <param name="hasTick">Whether to write the tick.</param>
    /// <returns>Bytes written.</returns>
    /// <exception cref="ArgumentException"><paramref name="destination"/> is too small.</exception>
    public static int WriteHeader(Span<byte> destination, uint tick, bool hasTick)
    {
        int length = GetHeaderLength(tick, hasTick);
        if (destination.Length < length)
        {
            throw new ArgumentException($"Destination is too small for the {length}-byte container header.", nameof(destination));
        }

        destination[0] = ChannelId;
        destination[1] = hasTick ? FlagTick : (byte)0;
        if (hasTick)
        {
            VarInt.Write(destination.Slice(2), tick);
        }

        return length;
    }

    /// <summary>
    /// Validates a whole container and returns an iterator over its messages. Validation is complete before the
    /// first message is yielded: channel byte 0x01, no reserved flag bits, tick ≤ 2^32−1, every length ≥ 1 and within
    /// the remaining bytes, no inner channel 1, at least one message, no trailing bytes. The number of messages is
    /// bounded only by the datagram's length (PROTOCOL.md §2.2): <see cref="MaxMessages"/> is what this implementation
    /// packs, not what it accepts.
    /// </summary>
    /// <param name="datagram">The whole datagram, starting with the channel id 0x01.</param>
    /// <param name="reader">The iterator (default on failure).</param>
    /// <returns>
    /// <see cref="ParseStatus.Ok"/>, <see cref="ParseStatus.NotContainer"/>, <see cref="ParseStatus.Truncated"/>,
    /// <see cref="ParseStatus.NonMinimalVarint"/>, <see cref="ParseStatus.BadFlags"/>, <see cref="ParseStatus.ValueOutOfRange"/>,
    /// <see cref="ParseStatus.BadLength"/>, <see cref="ParseStatus.NestedContainer"/> or
    /// <see cref="ParseStatus.ContainerEmpty"/>.
    /// </returns>
    public static ParseStatus TryParse(ReadOnlySpan<byte> datagram, out PackedContainerReader reader)
    {
        reader = default;
        if (datagram.IsEmpty)
        {
            return ParseStatus.Truncated;
        }

        if (datagram[0] != ChannelId)
        {
            return ParseStatus.NotContainer;
        }

        if (datagram.Length < 2)
        {
            return ParseStatus.Truncated;
        }

        byte flags = datagram[1];
        if ((flags & ~FlagTick) != 0)
        {
            return ParseStatus.BadFlags;
        }

        int pos = 2;
        ulong tick = 0;
        ParseStatus status;
        bool hasTick = (flags & FlagTick) != 0;
        if (hasTick)
        {
            status = WireReader.ReadVarInt(datagram, ref pos, out tick);
            if (status != ParseStatus.Ok)
            {
                return status;
            }

            if (tick > uint.MaxValue)
            {
                return ParseStatus.ValueOutOfRange;
            }
        }

        int first = pos;
        int count = 0;
        while (pos < datagram.Length)
        {
            status = WireReader.ReadVarInt(datagram, ref pos, out ulong length);
            if (status != ParseStatus.Ok)
            {
                return status;
            }

            if (length == 0 || length > (ulong)(datagram.Length - pos))
            {
                return ParseStatus.BadLength;
            }

            if (datagram[pos] == ChannelId)
            {
                return ParseStatus.NestedContainer;
            }

            // PROTOCOL.md §2.2: the receiver accepts as many messages as the datagram actually holds (its length is the
            // implicit bound); only the sender keeps to MaxMessages per container.
            count++;
            pos += (int)length;
        }

        if (count == 0)
        {
            return ParseStatus.ContainerEmpty;
        }

        reader = new PackedContainerReader(datagram, first, count, hasTick, (uint)tick);
        return ParseStatus.Ok;
    }
}

/// <summary>
/// Allocation-free iterator over the messages of a container validated by <see cref="PackedContainer.TryParse"/>.
/// Each message is a complete datagram frame (parse it with <see cref="DatagramFraming.TryParse(ReadOnlySpan{byte}, Channels.ChannelTable, out MessageHeader, out int)"/>).
/// Supports <c>foreach</c>.
/// </summary>
public ref struct PackedContainerReader
{
    private readonly ReadOnlySpan<byte> _data;
    private int _pos;
    private ReadOnlySpan<byte> _current;

    internal PackedContainerReader(ReadOnlySpan<byte> data, int firstEntry, int count, bool hasTick, uint tick)
    {
        _data = data;
        _pos = firstEntry;
        _current = default;
        Count = count;
        HasTick = hasTick;
        Tick = tick;
    }

    /// <summary>Number of messages in the container.</summary>
    public int Count { get; }

    /// <summary>Whether the container carries the sender's flush tick.</summary>
    public bool HasTick { get; }

    /// <summary>The sender's flush tick (0 when absent).</summary>
    public uint Tick { get; }

    /// <summary>The current message (valid after <see cref="MoveNext"/> returned <see langword="true"/>).</summary>
    public readonly ReadOnlySpan<byte> Current => _current;

    /// <summary>Advances to the next message.</summary>
    /// <returns><see langword="false"/> after the last message.</returns>
    public bool MoveNext()
    {
        if (_pos >= _data.Length)
        {
            _current = default;
            return false;
        }

        // Already validated by TryParse: minimal, in range.
        VarInt.TryRead(_data.Slice(_pos), out ulong length, out int consumed);
        _pos += consumed;
        _current = _data.Slice(_pos, (int)length);
        _pos += (int)length;
        return true;
    }

    /// <summary>Returns this iterator (for <c>foreach</c>).</summary>
    public readonly PackedContainerReader GetEnumerator() => this;
}

/// <summary>
/// Builds a packed container in caller memory: writes the header, then appends <c>(Length varint, message)</c> entries
/// while they fit (and at most <see cref="PackedContainer.MaxMessages"/>). Allocation-free.
/// </summary>
public ref struct PackedContainerWriter
{
    private readonly Span<byte> _buffer;
    private int _length;
    private int _count;

    /// <summary>Starts a container in <paramref name="buffer"/> (typically sized from the transport's current maximum datagram payload).</summary>
    /// <param name="buffer">The destination.</param>
    /// <param name="tick">The sender's flush tick.</param>
    /// <param name="hasTick">Whether to write the tick.</param>
    /// <exception cref="ArgumentException"><paramref name="buffer"/> cannot hold the header.</exception>
    public PackedContainerWriter(Span<byte> buffer, uint tick = 0, bool hasTick = false)
    {
        _buffer = buffer;
        _length = PackedContainer.WriteHeader(buffer, tick, hasTick);
        _count = 0;
    }

    /// <summary>
    /// Continues a container whose first <paramref name="length"/> bytes (its header and <paramref name="count"/> entries)
    /// were written into <paramref name="buffer"/> earlier. The writer is a <c>ref struct</c> and cannot be stored, so a
    /// packer that appends across calls keeps <see cref="Length"/> and <see cref="Count"/> and resumes with them.
    /// </summary>
    /// <param name="buffer">The destination the container was started in.</param>
    /// <param name="length">Bytes written so far (the previous writer's <see cref="Length"/>).</param>
    /// <param name="count">Messages appended so far (the previous writer's <see cref="Count"/>).</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="length"/> is shorter than a header or longer than the buffer, or <paramref name="count"/> is outside 0 … <see cref="PackedContainer.MaxMessages"/>.</exception>
    /// <exception cref="ArgumentException"><paramref name="buffer"/> does not start with a container header.</exception>
    public PackedContainerWriter(Span<byte> buffer, int length, int count)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(length, 2);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(length, buffer.Length);
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(count, PackedContainer.MaxMessages);
        if (buffer[0] != PackedContainer.ChannelId)
        {
            throw new ArgumentException("The buffer does not start with a packed container header.", nameof(buffer));
        }

        _buffer = buffer;
        _length = length;
        _count = count;
    }

    /// <summary>Bytes written so far (header + entries).</summary>
    public readonly int Length => _length;

    /// <summary>Messages appended so far.</summary>
    public readonly int Count => _count;

    /// <summary>Free bytes left in the buffer.</summary>
    public readonly int Remaining => _buffer.Length - _length;

    /// <summary>The container bytes written so far.</summary>
    public readonly ReadOnlySpan<byte> Written => _buffer.Slice(0, _length);

    /// <summary>Whether a message of <paramref name="messageLength"/> bytes still fits (space and message count).</summary>
    /// <param name="messageLength">Encoded message size, ≥ 1.</param>
    public readonly bool CanAppend(int messageLength) =>
        messageLength > 0 && _count < PackedContainer.MaxMessages && PackedContainer.GetEntryLength(messageLength) <= Remaining;

    /// <summary>Copies a complete message (its channel id first) into the container.</summary>
    /// <param name="message">A channel-0 or channel-≥2 frame of at least one byte.</param>
    /// <returns><see langword="false"/> when it does not fit (nothing is written).</returns>
    /// <exception cref="ArgumentException"><paramref name="message"/> is empty or is itself a container (starts with 0x01).</exception>
    public bool TryAppend(ReadOnlySpan<byte> message)
    {
        if (message.IsEmpty || message[0] == PackedContainer.ChannelId)
        {
            throw new ArgumentException("A container entry must be a non-empty channel-0 or channel->=2 frame.", nameof(message));
        }

        if (!TryReserve(message.Length, out Span<byte> slot))
        {
            return false;
        }

        message.CopyTo(slot);
        return true;
    }

    /// <summary>
    /// Reserves space for a message of exactly <paramref name="messageLength"/> bytes and returns it, so the caller can
    /// write the header and payload in place (zero copy). The caller must fill the whole slot with a valid frame whose
    /// channel id is not 1.
    /// </summary>
    /// <param name="messageLength">Encoded message size, ≥ 1.</param>
    /// <param name="slot">The reserved bytes, or empty when it does not fit.</param>
    /// <returns><see langword="false"/> when it does not fit (nothing is written).</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="messageLength"/> is less than 1.</exception>
    public bool TryReserve(int messageLength, out Span<byte> slot)
    {
        if (messageLength < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(messageLength), messageLength, "A container entry is at least one byte.");
        }

        if (!CanAppend(messageLength))
        {
            slot = default;
            return false;
        }

        _length += VarInt.Write(_buffer.Slice(_length), (ulong)messageLength);
        slot = _buffer.Slice(_length, messageLength);
        _length += messageLength;
        _count++;
        return true;
    }
}
