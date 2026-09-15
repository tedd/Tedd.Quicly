using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Core.Framing;

/// <summary>
/// Application datagram message header codec (PROTOCOL.md §2.1). Allocation-free; the parser never throws.
/// </summary>
/// <remarks>
/// Layout: <c>ChannelId varint</c>, <c>Sequence u16/u32 LE</c> (sequenced modes or fragmentation),
/// <c>Key varint</c> (keyed), <c>FragCount u8</c> + <c>FragIndex u8</c> when <c>FragCount &gt; 1</c> (fragmentation),
/// <c>RawLength varint</c> (compression), payload = the remaining bytes.
/// </remarks>
public static class DatagramFraming
{
    /// <summary>
    /// Upper bound of <see cref="GetHeaderLength"/> for any header: channel 2 + sequence 4 + key 8 + fragment 2 +
    /// RawLength 8 bytes. With <c>RawLength ≤ MaxMessageSize</c> real channels stay at or below 18 bytes (fragmenting
    /// channels cap RawLength at 8 800, a 2-byte varint; other datagram channels at 1 MiB, a 4-byte varint, and have
    /// no fragment fields).
    /// </summary>
    public const int MaxHeaderLength = 24;

    /// <summary>Largest <see cref="MessageHeader.FragCount"/> (PROTOCOL.md §2.1).</summary>
    public const int MaxFragments = 8;

    /// <summary>Returns the encoded header size of <paramref name="header"/> on <paramref name="channel"/>.</summary>
    /// <param name="channel">The channel.</param>
    /// <param name="header">The header; <see cref="MessageHeader.Key"/> must not exceed <see cref="VarInt.MaxValue"/>.</param>
    /// <remarks>
    /// Precomputed per-channel constant part plus one well-predicted branch per optional field. A branch-free
    /// table-driven variant was measured slower on the realistic single-shape stream (docs/benchmarks/framing.md).
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException">The channel is keyed and <see cref="MessageHeader.Key"/> exceeds <see cref="VarInt.MaxValue"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int GetHeaderLength(ChannelDefinition channel, in MessageHeader header)
    {
        int length = channel.FixedHeaderBytesWithoutKey;
        if (channel.Keyed)
        {
            length += VarInt.GetLength(header.Key);
        }

        if (channel.Fragmentation && header.FragCount > 1)
        {
            length++;
        }

        if (channel.Compression != ChannelCompression.None)
        {
            length += VarInt.GetLength((uint)header.RawLength);
        }

        return length;
    }

    /// <summary>Writes the header of a message on <paramref name="channel"/>; the payload follows it.</summary>
    /// <param name="destination">Buffer of at least <see cref="GetHeaderLength"/> bytes.</param>
    /// <param name="channel">The channel (its id is written; <see cref="MessageHeader.Channel"/> is ignored).</param>
    /// <param name="header">Sequence (low 16 bits on a 16-bit channel), key, fragment fields, RawLength.</param>
    /// <returns>Bytes written.</returns>
    /// <exception cref="ArgumentException">
    /// The destination is too small, the channel is stream-only (ReliableOrdered, ReliableUnordered, Bulk: every receiver
    /// rejects such a datagram as <see cref="ParseStatus.ChannelNotDatagram"/>), or the header is invalid for the channel
    /// (key above 2^62−1, FragCount above 8, FragIndex ≥ FragCount, negative RawLength).
    /// </exception>
    public static int WriteHeader(Span<byte> destination, ChannelDefinition channel, in MessageHeader header)
    {
        if (!channel.IsDatagramMode)
        {
            ThrowNotDatagramChannel(channel);
        }

        if ((channel.Keyed && header.Key > VarInt.MaxValue)
            || (channel.Fragmentation && (header.FragCount > MaxFragments || (header.FragCount > 1 && header.FragIndex >= header.FragCount)))
            || header.RawLength < 0)
        {
            ThrowInvalidHeader();
        }

        int length = GetHeaderLength(channel, header);
        if (destination.Length < length)
        {
            ThrowDestinationTooSmall(length);
        }

        int pos;
        if (channel.ChannelIdLength == 1)
        {
            destination[0] = (byte)channel.Id;
            pos = 1;
        }
        else
        {
            BinaryPrimitives.WriteUInt16BigEndian(destination, (ushort)(0x4000 | channel.Id));
            pos = 2;
        }

        int sequenceBytes = channel.SequenceBytes;
        if (sequenceBytes == 2)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(pos), (ushort)header.Sequence);
            pos += 2;
        }
        else if (sequenceBytes == 4)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(pos), header.Sequence);
            pos += 4;
        }

        if (channel.Keyed)
        {
            pos += VarInt.Write(destination.Slice(pos), header.Key);
        }

        if (channel.Fragmentation)
        {
            byte fragCount = header.FragCount == 0 ? (byte)1 : header.FragCount;
            destination[pos++] = fragCount;
            if (fragCount > 1)
            {
                destination[pos++] = header.FragIndex;
            }
        }

        if (channel.Compression != ChannelCompression.None)
        {
            pos += VarInt.Write(destination.Slice(pos), (ulong)header.RawLength);
        }

        return pos;
    }

    /// <summary>Parses the header of a datagram using each channel's own <see cref="ChannelDefinition.MaxMessageSize"/>.</summary>
    /// <param name="datagram">The whole datagram payload.</param>
    /// <param name="table">The session's channel table.</param>
    /// <param name="header">The parsed header (<see cref="MessageHeader.Channel"/> is also set for channels 0 and 1).</param>
    /// <param name="payloadOffset">Offset of the payload (for channels 0 and 1: of the byte after the channel id).</param>
    /// <returns>See <see cref="TryParse(ReadOnlySpan{byte}, ChannelTable, int, out MessageHeader, out int)"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ParseStatus TryParse(ReadOnlySpan<byte> datagram, ChannelTable table, out MessageHeader header, out int payloadOffset) =>
        TryParse(datagram, table, 0, out header, out payloadOffset);

    /// <summary>
    /// Parses the header of a datagram. Channel 0 returns <see cref="ParseStatus.ControlChannel"/> and channel 1
    /// <see cref="ParseStatus.ContainerChannel"/> (with <paramref name="payloadOffset"/> just past the channel id);
    /// every other outcome except <see cref="ParseStatus.Ok"/> means the datagram must be dropped and counted.
    /// </summary>
    /// <param name="datagram">The whole datagram payload (or one message of a packed container).</param>
    /// <param name="table">The session's channel table.</param>
    /// <param name="maxMessageSize">The session's cap (<c>HelloAck.maxMessageSize</c>); 0 = only the channel's limit.</param>
    /// <param name="header">The parsed header.</param>
    /// <param name="payloadOffset">Offset of the payload within <paramref name="datagram"/>.</param>
    /// <returns>
    /// <see cref="ParseStatus.Ok"/>, <see cref="ParseStatus.ControlChannel"/>, <see cref="ParseStatus.ContainerChannel"/>,
    /// <see cref="ParseStatus.Truncated"/>, <see cref="ParseStatus.NonMinimalVarint"/>, <see cref="ParseStatus.UnknownChannel"/>,
    /// <see cref="ParseStatus.ChannelNotDatagram"/>, <see cref="ParseStatus.BadFragment"/>, <see cref="ParseStatus.RawLengthTooLarge"/>,
    /// <see cref="ParseStatus.MessageTooLarge"/> or <see cref="ParseStatus.BadLength"/>.
    /// </returns>
    /// <remarks>
    /// Fragment rules checked from a single fragment: FragCount ∈ [1, 8]; FragIndex &lt; FragCount; every fragment
    /// carries ≥ 1 byte; the smallest total the fragment implies (non-last: size × (count − 1) + 1, last:
    /// size × count, because the last fragment is never larger than fragment 0) must fit the limit. A compressed
    /// payload must be non-empty and shorter than <c>RawLength</c> (compression must shrink, PROTOCOL.md §2.1).
    /// </remarks>
    public static ParseStatus TryParse(ReadOnlySpan<byte> datagram, ChannelTable table, int maxMessageSize, out MessageHeader header, out int payloadOffset)
    {
        header = default;
        payloadOffset = 0;
        if (datagram.IsEmpty)
        {
            return ParseStatus.Truncated;
        }

        int channelId = datagram[0];
        int pos = 1;
        if (channelId >= 0x40)
        {
            ParseStatus channelStatus = ReadWideChannel(datagram, out channelId);
            if (channelStatus != ParseStatus.Ok)
            {
                return channelStatus;
            }

            pos = 2;
        }

        header.Channel = (ushort)channelId;
        if (channelId < ChannelDefinition.MinId)
        {
            payloadOffset = pos;
            return channelId == 0 ? ParseStatus.ControlChannel : ParseStatus.ContainerChannel;
        }

        ChannelDefinition? channel = table[channelId];
        if (channel is null)
        {
            return ParseStatus.UnknownChannel;
        }

        if (!channel.IsDatagramMode)
        {
            return ParseStatus.ChannelNotDatagram;
        }

        int sequenceBytes = channel.SequenceBytes;
        if (sequenceBytes != 0)
        {
            if (datagram.Length - pos < sequenceBytes)
            {
                return ParseStatus.Truncated;
            }

            header.Sequence = sequenceBytes == 2
                ? BinaryPrimitives.ReadUInt16LittleEndian(datagram.Slice(pos))
                : BinaryPrimitives.ReadUInt32LittleEndian(datagram.Slice(pos));
            pos += sequenceBytes;
        }

        ParseStatus status;
        if (channel.Keyed)
        {
            status = WireReader.ReadVarInt(datagram, ref pos, out header.Key);
            if (status != ParseStatus.Ok)
            {
                return status;
            }
        }

        int fragCount = 1;
        int fragIndex = 0;
        if (channel.Fragmentation)
        {
            if (pos >= datagram.Length)
            {
                return ParseStatus.Truncated;
            }

            fragCount = datagram[pos++];
            if ((uint)(fragCount - 1) >= MaxFragments)
            {
                return ParseStatus.BadFragment;
            }

            if (fragCount > 1)
            {
                if (pos >= datagram.Length)
                {
                    return ParseStatus.Truncated;
                }

                fragIndex = datagram[pos++];
                if (fragIndex >= fragCount)
                {
                    return ParseStatus.BadFragment;
                }
            }
        }

        header.FragCount = (byte)fragCount;
        header.FragIndex = (byte)fragIndex;

        int limit = channel.MaxMessageSize;
        if ((uint)(maxMessageSize - 1) < (uint)limit)
        {
            limit = maxMessageSize;
        }

        ulong rawLength = 0;
        if (channel.Compression != ChannelCompression.None)
        {
            status = WireReader.ReadVarInt(datagram, ref pos, out rawLength);
            if (status != ParseStatus.Ok)
            {
                return status;
            }

            if (rawLength > (ulong)limit)
            {
                return ParseStatus.RawLengthTooLarge;
            }
        }

        long payloadLength = datagram.Length - pos;
        long minimumTotal = payloadLength;
        if (fragCount > 1)
        {
            if (payloadLength == 0)
            {
                return ParseStatus.BadFragment;
            }

            minimumTotal = fragIndex == fragCount - 1
                ? payloadLength * fragCount
                : (payloadLength * (fragCount - 1)) + 1;
        }

        if (rawLength == 0)
        {
            if (minimumTotal > limit)
            {
                return ParseStatus.MessageTooLarge;
            }
        }
        else if (payloadLength == 0 || minimumTotal >= (long)rawLength)
        {
            return ParseStatus.BadLength;
        }

        header.RawLength = (int)rawLength;
        payloadOffset = pos;
        return ParseStatus.Ok;
    }

    private static ParseStatus ReadWideChannel(ReadOnlySpan<byte> datagram, out int channelId)
    {
        channelId = 0;
        byte first = datagram[0];
        if (first < 0x80)
        {
            if (datagram.Length < 2)
            {
                return ParseStatus.Truncated;
            }

            int value = ((first & 0x3F) << 8) | datagram[1];
            if (value <= ChannelDefinition.MaxOneByteId)
            {
                return ParseStatus.NonMinimalVarint;
            }

            channelId = value;
            return ParseStatus.Ok;
        }

        // 4- or 8-byte encodings: either non-minimal (value ≤ 16383) or a value no channel can have.
        if (!VarInt.TryRead(datagram, out ulong wide, out int consumed))
        {
            return ParseStatus.Truncated;
        }

        return VarInt.GetLength(wide) != consumed ? ParseStatus.NonMinimalVarint : ParseStatus.UnknownChannel;
    }

    private static void ThrowInvalidHeader() =>
        throw new ArgumentException("Header is invalid for the channel (key above 2^62-1, FragCount above 8, FragIndex >= FragCount, or negative RawLength).", "header");

    private static void ThrowNotDatagramChannel(ChannelDefinition channel) =>
        throw new ArgumentException($"Channel {channel.Id} ('{channel.Name}', {channel.Mode}) is stream-only; its messages cannot be sent as datagrams.", nameof(channel));

    private static void ThrowDestinationTooSmall(int length) =>
        throw new ArgumentException($"Destination is too small for the {length}-byte header.", "destination");
}
