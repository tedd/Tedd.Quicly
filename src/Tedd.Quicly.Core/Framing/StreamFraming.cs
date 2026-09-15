using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Core.Framing;

/// <summary>
/// Header fields of one message frame on a ReliableOrdered (§3.1) or group (§3.2) stream, of a control-stream frame
/// (§3.4, see <see cref="StreamFrameParser.ControlType"/>), or of one bulk body chunk (§3.3).
/// </summary>
public struct StreamMessageHeader
{
    /// <summary>Payload length on the wire (compressed size when <see cref="RawLength"/> &gt; 0).</summary>
    public int Length;

    /// <summary>Version of a ReliableLatest value on a group stream (equals the stream's group id); 0 otherwise.</summary>
    public uint Sequence;

    /// <summary>Key (keyed channels).</summary>
    public ulong Key;

    /// <summary>Request id on RequestResponse channels: 0 plain, odd request, even ≥ 2 response to <c>RequestId − 1</c>.</summary>
    public uint RequestId;

    /// <summary>Decoded size of a compressed payload; 0 = not compressed.</summary>
    public int RawLength;

    /// <summary><see langword="true"/> when the payload is an LZ4 block.</summary>
    public readonly bool Compressed => RawLength > 0;
}

/// <summary>Flags of a bulk stream header (PROTOCOL.md §3.3). Bits 2–3 (hash algorithm) are always 0 = SHA-256.</summary>
[Flags]
public enum BulkFlags : byte
{
    /// <summary>No flag.</summary>
    None = 0,

    /// <summary>A 32-byte SHA-256 of the whole object follows the flags.</summary>
    HashPresent = 0x01,

    /// <summary>The body is a sequence of <c>(ChunkLength, RawLength, bytes)</c> chunks.</summary>
    Chunked = 0x02,
}

/// <summary>The 32-byte object hash of a bulk header.</summary>
[InlineArray(StreamFraming.BulkHashLength)]
public struct BulkHash
{
    private byte _element0;
}

/// <summary>Header of a bulk stream (PROTOCOL.md §3.3).</summary>
public struct BulkHeader
{
    /// <summary>Transfer id, unique per (peer, direction).</summary>
    public ulong TransferId;

    /// <summary>Object id.</summary>
    public ulong ObjectId;

    /// <summary>Object version.</summary>
    public ulong ObjectVersion;

    /// <summary>Size of the whole object.</summary>
    public ulong TotalLength;

    /// <summary>Offset of this transfer's range within the object.</summary>
    public ulong Offset;

    /// <summary>Decoded bytes this transfer carries; &gt; 0.</summary>
    public ulong Length;

    /// <summary>Flags.</summary>
    public BulkFlags Flags;

    /// <summary>SHA-256 of the whole object when <see cref="BulkFlags.HashPresent"/>.</summary>
    public BulkHash Hash;

    /// <summary>Whether <see cref="Hash"/> is present.</summary>
    public readonly bool HasHash => (Flags & BulkFlags.HashPresent) != 0;

    /// <summary>Whether the body is chunked.</summary>
    public readonly bool IsChunked => (Flags & BulkFlags.Chunked) != 0;
}

/// <summary>
/// Stream preambles and frame headers (PROTOCOL.md §3.1–§3.4). Writers throw only for caller errors; the
/// <c>TryParse*</c> methods never throw and parse from one contiguous span (see <see cref="StreamFrameParser"/> for
/// incremental parsing).
/// </summary>
/// <remarks>
/// Message frame: <c>Length varint</c>, <c>Sequence u32 LE</c> (ReliableLatest group streams only),
/// <c>Key varint</c> (keyed), <c>RequestId varint</c> (RequestResponse), <c>RawLength varint</c> (compression), payload.
/// </remarks>
public static class StreamFraming
{
    /// <summary>Largest control frame <c>Length</c> (type byte + body).</summary>
    public const int MaxControlFrameLength = 16384;

    /// <summary>Default <c>BulkMaxChunk</c>: the largest chunk length and chunk raw length.</summary>
    public const int DefaultBulkMaxChunk = 1 << 20;

    /// <summary>Size of the bulk object hash.</summary>
    public const int BulkHashLength = 32;

    /// <summary>Largest message frame header (Length + Key + RequestId + RawLength, 8 bytes each).</summary>
    public const int MaxFrameHeaderLength = 32;

    internal const byte ShapeKeyed = 0x01;
    internal const byte ShapeRequestId = 0x02;
    internal const byte ShapeCompressed = 0x04;
    internal const byte ShapeSequence = 0x08;

    private const int MaxChannelId = ChannelDefinition.MaxId;

    /// <summary>Returns the size of a preamble naming <paramref name="channel"/>.</summary>
    /// <param name="channel">Channel id (0 for the control stream).</param>
    public static int GetPreambleLength(ushort channel) => VarInt.GetLength(channel);

    /// <summary>Writes the preamble of a control (channel 0), ordered or bulk stream.</summary>
    /// <param name="destination">The buffer.</param>
    /// <param name="channel">Channel id, 0…16383.</param>
    /// <returns>Bytes written.</returns>
    /// <exception cref="ArgumentException">The channel id is above 16383 or the destination is too small.</exception>
    public static int WritePreamble(Span<byte> destination, ushort channel)
    {
        ValidateChannel(channel);
        return VarInt.Write(destination, channel);
    }

    /// <summary>Returns the size of a group-stream preamble.</summary>
    /// <param name="channel">Channel id.</param>
    /// <param name="groupId">Group id (for ReliableLatest: the value's version).</param>
    public static int GetGroupPreambleLength(ushort channel, ulong groupId) => VarInt.GetLength(channel) + VarInt.GetLength(groupId);

    /// <summary>Writes the preamble of a group stream: <c>ChannelId varint, GroupId varint</c>.</summary>
    /// <param name="destination">The buffer.</param>
    /// <param name="channel">Channel id, 0…16383.</param>
    /// <param name="groupId">Group id, ≤ 2^62−1.</param>
    /// <returns>Bytes written.</returns>
    /// <exception cref="ArgumentException">A value is out of range or the destination is too small.</exception>
    public static int WriteGroupPreamble(Span<byte> destination, ushort channel, ulong groupId)
    {
        ValidateChannel(channel);
        int length = GetGroupPreambleLength(channel, groupId);
        if (destination.Length < length)
        {
            ThrowDestinationTooSmall(length);
        }

        int pos = VarInt.Write(destination, channel);
        pos += VarInt.Write(destination.Slice(pos), groupId);
        return pos;
    }

    /// <summary>Returns the frame header size of <paramref name="header"/> on <paramref name="channel"/>.</summary>
    /// <param name="channel">A ReliableOrdered, ReliableUnordered or ReliableLatest channel.</param>
    /// <param name="header">The header.</param>
    public static int GetFrameHeaderLength(ChannelDefinition channel, in StreamMessageHeader header)
    {
        int shape = channel.StreamShape;
        int length = WireReader.VarIntLength((uint)header.Length);
        if ((shape & ShapeSequence) != 0)
        {
            length += 4;
        }

        if ((shape & ShapeKeyed) != 0)
        {
            length += WireReader.VarIntLength(header.Key);
        }

        if ((shape & ShapeRequestId) != 0)
        {
            length += WireReader.VarIntLength(header.RequestId);
        }

        if ((shape & ShapeCompressed) != 0)
        {
            length += WireReader.VarIntLength((uint)header.RawLength);
        }

        return length;
    }

    /// <summary>Writes a message frame header; <see cref="StreamMessageHeader.Length"/> payload bytes follow it.</summary>
    /// <param name="destination">Buffer of at least <see cref="GetFrameHeaderLength"/> bytes.</param>
    /// <param name="channel">A ReliableOrdered, ReliableUnordered or ReliableLatest channel.</param>
    /// <param name="header">The header.</param>
    /// <returns>Bytes written.</returns>
    /// <exception cref="ArgumentException">
    /// The channel is not carried in message frames (datagram-only or Bulk), a field is out of range (negative
    /// length, key above 2^62−1) or the destination is too small.
    /// </exception>
    public static int WriteFrameHeader(Span<byte> destination, ChannelDefinition channel, in StreamMessageHeader header)
    {
        if (!channel.IsStreamMode || channel.Mode == ChannelMode.Bulk)
        {
            throw new ArgumentException($"Channel {channel.Id} ({channel.Mode}) is not carried in stream message frames.", nameof(channel));
        }

        int shape = channel.StreamShape;
        if (header.Length < 0 || header.RawLength < 0 || ((shape & ShapeKeyed) != 0 && header.Key > VarInt.MaxValue))
        {
            throw new ArgumentException("Header has a negative length or a key above 2^62-1.", nameof(header));
        }

        int length = GetFrameHeaderLength(channel, header);
        if (destination.Length < length)
        {
            ThrowDestinationTooSmall(length);
        }

        int pos = VarInt.Write(destination, (ulong)header.Length);
        if ((shape & ShapeSequence) != 0)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(destination.Slice(pos), header.Sequence);
            pos += 4;
        }

        if ((shape & ShapeKeyed) != 0)
        {
            pos += VarInt.Write(destination.Slice(pos), header.Key);
        }

        if ((shape & ShapeRequestId) != 0)
        {
            pos += VarInt.Write(destination.Slice(pos), header.RequestId);
        }

        if ((shape & ShapeCompressed) != 0)
        {
            pos += VarInt.Write(destination.Slice(pos), (ulong)header.RawLength);
        }

        return pos;
    }

    /// <summary>Parses a message frame header from one contiguous span.</summary>
    /// <param name="source">Bytes starting at the frame.</param>
    /// <param name="channel">The stream's channel.</param>
    /// <param name="maxMessageSize">Session cap (<c>HelloAck.maxMessageSize</c>); 0 = only the channel's limit.</param>
    /// <param name="header">The header.</param>
    /// <param name="bytesConsumed">Header bytes (the payload follows).</param>
    /// <returns>
    /// <see cref="ParseStatus.Ok"/>, <see cref="ParseStatus.Truncated"/>, <see cref="ParseStatus.NonMinimalVarint"/>,
    /// <see cref="ParseStatus.ChannelNotStream"/> (datagram-only or Bulk channel), <see cref="ParseStatus.ValueOutOfRange"/>
    /// (request id), <see cref="ParseStatus.RawLengthTooLarge"/>, <see cref="ParseStatus.MessageTooLarge"/> or
    /// <see cref="ParseStatus.BadLength"/> (compressed payload empty or not shorter than RawLength).
    /// </returns>
    public static ParseStatus TryParseFrameHeader(ReadOnlySpan<byte> source, ChannelDefinition channel, int maxMessageSize, out StreamMessageHeader header, out int bytesConsumed)
    {
        if (!channel.IsStreamMode || channel.Mode == ChannelMode.Bulk)
        {
            header = default;
            bytesConsumed = 0;
            return ParseStatus.ChannelNotStream;
        }

        return ParseMessageHeader(source, channel.StreamShape, EffectiveLimit(channel, maxMessageSize), out header, out bytesConsumed);
    }

    /// <summary>Returns the size of a control frame header (<c>Length varint</c> + <c>Type u8</c>).</summary>
    /// <param name="bodyLength">Body size, 0…16383.</param>
    public static int GetControlFrameHeaderLength(int bodyLength) => WireReader.VarIntLength((uint)bodyLength + 1) + 1;

    /// <summary>Writes a control frame header: <c>Length = bodyLength + 1</c>, then the type byte.</summary>
    /// <param name="destination">The buffer.</param>
    /// <param name="type">Control message type.</param>
    /// <param name="bodyLength">Body size, 0…16383.</param>
    /// <returns>Bytes written.</returns>
    /// <exception cref="ArgumentException">The body is out of range or the destination is too small.</exception>
    public static int WriteControlFrameHeader(Span<byte> destination, byte type, int bodyLength)
    {
        if ((uint)bodyLength >= MaxControlFrameLength)
        {
            throw new ArgumentOutOfRangeException(nameof(bodyLength), bodyLength, $"A control frame body is 0..{MaxControlFrameLength - 1} bytes.");
        }

        int length = GetControlFrameHeaderLength(bodyLength);
        if (destination.Length < length)
        {
            ThrowDestinationTooSmall(length);
        }

        int pos = VarInt.Write(destination, (ulong)bodyLength + 1);
        destination[pos] = type;
        return pos + 1;
    }

    /// <summary>Parses a control frame header.</summary>
    /// <param name="source">Bytes starting at the frame.</param>
    /// <param name="type">Control message type.</param>
    /// <param name="bodyLength">Body size (<c>Length − 1</c>).</param>
    /// <param name="bytesConsumed">Header bytes.</param>
    /// <returns><see cref="ParseStatus.Ok"/>, <see cref="ParseStatus.Truncated"/>, <see cref="ParseStatus.NonMinimalVarint"/> or <see cref="ParseStatus.BadLength"/> (Length outside 1…16384).</returns>
    public static ParseStatus TryParseControlFrameHeader(ReadOnlySpan<byte> source, out byte type, out int bodyLength, out int bytesConsumed)
    {
        type = 0;
        bodyLength = 0;
        bytesConsumed = 0;
        int pos = 0;
        ParseStatus status = WireReader.ReadVarInt(source, ref pos, out ulong length);
        if (status != ParseStatus.Ok)
        {
            return status;
        }

        if (length - 1 >= MaxControlFrameLength)
        {
            return ParseStatus.BadLength;
        }

        if (pos >= source.Length)
        {
            return ParseStatus.Truncated;
        }

        type = source[pos];
        bodyLength = (int)length - 1;
        bytesConsumed = pos + 1;
        return ParseStatus.Ok;
    }

    /// <summary>Returns the size of a bulk header.</summary>
    /// <param name="header">The header.</param>
    public static int GetBulkHeaderLength(in BulkHeader header) =>
        VarInt.GetLength(header.TransferId) + VarInt.GetLength(header.ObjectId) + VarInt.GetLength(header.ObjectVersion)
        + VarInt.GetLength(header.TotalLength) + VarInt.GetLength(header.Offset) + VarInt.GetLength(header.Length)
        + 1 + (header.HasHash ? BulkHashLength : 0);

    /// <summary>Writes a bulk header (after the preamble).</summary>
    /// <param name="destination">Buffer of at least <see cref="GetBulkHeaderLength"/> bytes.</param>
    /// <param name="header">The header.</param>
    /// <returns>Bytes written.</returns>
    /// <exception cref="ArgumentException">
    /// A value exceeds 2^62−1, <c>Length</c> is 0, <c>Offset + Length &gt; TotalLength</c>, undefined flags are set, or the
    /// destination is too small.
    /// </exception>
    public static int WriteBulkHeader(Span<byte> destination, in BulkHeader header)
    {
        if (header.TotalLength > VarInt.MaxValue || header.Length == 0 || header.Offset > header.TotalLength
            || header.Length > header.TotalLength - header.Offset || ((byte)header.Flags & ~0x03) != 0)
        {
            throw new ArgumentException("Bulk header range or flags are invalid (Length > 0, Offset + Length <= TotalLength <= 2^62-1, flags HashPresent|Chunked only).", nameof(header));
        }

        int length = GetBulkHeaderLength(header);
        if (destination.Length < length)
        {
            ThrowDestinationTooSmall(length);
        }

        int pos = VarInt.Write(destination, header.TransferId);
        pos += VarInt.Write(destination.Slice(pos), header.ObjectId);
        pos += VarInt.Write(destination.Slice(pos), header.ObjectVersion);
        pos += VarInt.Write(destination.Slice(pos), header.TotalLength);
        pos += VarInt.Write(destination.Slice(pos), header.Offset);
        pos += VarInt.Write(destination.Slice(pos), header.Length);
        destination[pos++] = (byte)header.Flags;
        if (header.HasHash)
        {
            ((ReadOnlySpan<byte>)header.Hash).CopyTo(destination.Slice(pos));
            pos += BulkHashLength;
        }

        return pos;
    }

    /// <summary>
    /// Parses a bulk header from one contiguous span and validates it before any state is created: reserved flag bits
    /// and the hash algorithm (bits 2–3 must be 0 = SHA-256), <c>Length &gt; 0</c>, <c>Offset + Length ≤ TotalLength</c>,
    /// <c>Length ≤ maxLength</c>, 32 hash bytes when flagged.
    /// </summary>
    /// <param name="source">Bytes starting after the preamble.</param>
    /// <param name="maxLength">Largest acceptable <c>Length</c> (the channel's effective MaxMessageSize).</param>
    /// <param name="header">The header.</param>
    /// <param name="bytesConsumed">Header bytes.</param>
    /// <returns>
    /// <see cref="ParseStatus.Ok"/>, <see cref="ParseStatus.Truncated"/>, <see cref="ParseStatus.NonMinimalVarint"/>,
    /// <see cref="ParseStatus.BadFlags"/>, <see cref="ParseStatus.BadBulkRange"/> or <see cref="ParseStatus.MessageTooLarge"/>.
    /// </returns>
    public static ParseStatus TryParseBulkHeader(ReadOnlySpan<byte> source, long maxLength, out BulkHeader header, out int bytesConsumed)
    {
        header = default;
        bytesConsumed = 0;
        ParseStatus status = ParseBulkIdentity(source, ref header, out int a);
        if (status != ParseStatus.Ok)
        {
            return status;
        }

        status = ParseBulkRange(source.Slice(a), maxLength, ref header, out int b);
        if (status != ParseStatus.Ok)
        {
            return status;
        }

        int c = 0;
        if (header.HasHash)
        {
            status = ParseBulkHash(source.Slice(a + b), ref header, out c);
            if (status != ParseStatus.Ok)
            {
                return status;
            }
        }

        bytesConsumed = a + b + c;
        return ParseStatus.Ok;
    }

    /// <summary>Returns the size of a bulk chunk header.</summary>
    /// <param name="chunkLength">Bytes of the chunk on the wire.</param>
    /// <param name="rawLength">Decoded size of the chunk; 0 = stored uncompressed.</param>
    public static int GetBulkChunkHeaderLength(int chunkLength, int rawLength) =>
        WireReader.VarIntLength((uint)chunkLength) + WireReader.VarIntLength((uint)rawLength);

    /// <summary>Writes a bulk chunk header: <c>ChunkLength varint, RawLength varint</c>.</summary>
    /// <param name="destination">The buffer.</param>
    /// <param name="chunkLength">Bytes of the chunk on the wire, ≥ 1.</param>
    /// <param name="rawLength">Decoded size (&gt; <paramref name="chunkLength"/>), or 0 when the chunk is stored uncompressed.</param>
    /// <returns>Bytes written.</returns>
    /// <exception cref="ArgumentException">A length is out of range or the destination is too small.</exception>
    public static int WriteBulkChunkHeader(Span<byte> destination, int chunkLength, int rawLength)
    {
        if (chunkLength < 1 || rawLength < 0)
        {
            throw new ArgumentException("ChunkLength must be >= 1 and RawLength >= 0.", nameof(chunkLength));
        }

        int length = GetBulkChunkHeaderLength(chunkLength, rawLength);
        if (destination.Length < length)
        {
            ThrowDestinationTooSmall(length);
        }

        int pos = VarInt.Write(destination, (ulong)chunkLength);
        pos += VarInt.Write(destination.Slice(pos), (ulong)rawLength);
        return pos;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int EffectiveLimit(ChannelDefinition channel, int maxMessageSize)
    {
        int limit = channel.MaxMessageSize;
        return (uint)(maxMessageSize - 1) < (uint)limit ? maxMessageSize : limit;
    }

    internal static ParseStatus ParseMessageHeader(ReadOnlySpan<byte> source, int shape, int limit, out StreamMessageHeader header, out int bytesConsumed)
    {
        header = default;
        bytesConsumed = 0;
        int pos = 0;
        ParseStatus status = WireReader.ReadVarInt(source, ref pos, out ulong length);
        if (status != ParseStatus.Ok)
        {
            return status;
        }

        if ((shape & ShapeSequence) != 0)
        {
            if (source.Length - pos < 4)
            {
                return ParseStatus.Truncated;
            }

            header.Sequence = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(pos));
            pos += 4;
        }

        if ((shape & ShapeKeyed) != 0)
        {
            status = WireReader.ReadVarInt(source, ref pos, out header.Key);
            if (status != ParseStatus.Ok)
            {
                return status;
            }
        }

        if ((shape & ShapeRequestId) != 0)
        {
            status = WireReader.ReadVarInt(source, ref pos, out ulong requestId);
            if (status != ParseStatus.Ok)
            {
                return status;
            }

            if (requestId > uint.MaxValue)
            {
                return ParseStatus.ValueOutOfRange;
            }

            header.RequestId = (uint)requestId;
        }

        ulong rawLength = 0;
        if ((shape & ShapeCompressed) != 0)
        {
            status = WireReader.ReadVarInt(source, ref pos, out rawLength);
            if (status != ParseStatus.Ok)
            {
                return status;
            }

            if (rawLength > (ulong)limit)
            {
                return ParseStatus.RawLengthTooLarge;
            }
        }

        if (rawLength == 0)
        {
            if (length > (ulong)limit)
            {
                return ParseStatus.MessageTooLarge;
            }
        }
        else if (length == 0 || length >= rawLength)
        {
            return ParseStatus.BadLength;
        }

        header.Length = (int)length;
        header.RawLength = (int)rawLength;
        bytesConsumed = pos;
        return ParseStatus.Ok;
    }

    /// <summary>Bulk header stage 1 (≤ 32 bytes): TransferId, ObjectId, ObjectVersion, TotalLength.</summary>
    internal static ParseStatus ParseBulkIdentity(ReadOnlySpan<byte> source, ref BulkHeader header, out int bytesConsumed)
    {
        bytesConsumed = 0;
        int pos = 0;
        ParseStatus status = WireReader.ReadVarInt(source, ref pos, out ulong transferId);
        if (status == ParseStatus.Ok)
        {
            status = WireReader.ReadVarInt(source, ref pos, out ulong objectId);
            if (status == ParseStatus.Ok)
            {
                status = WireReader.ReadVarInt(source, ref pos, out ulong objectVersion);
                if (status == ParseStatus.Ok)
                {
                    status = WireReader.ReadVarInt(source, ref pos, out ulong totalLength);
                    if (status == ParseStatus.Ok)
                    {
                        header.TransferId = transferId;
                        header.ObjectId = objectId;
                        header.ObjectVersion = objectVersion;
                        header.TotalLength = totalLength;
                        bytesConsumed = pos;
                    }
                }
            }
        }

        return status;
    }

    /// <summary>Bulk header stage 2 (≤ 17 bytes): Offset, Length, Flags, with the range and flag validation.</summary>
    internal static ParseStatus ParseBulkRange(ReadOnlySpan<byte> source, long maxLength, ref BulkHeader header, out int bytesConsumed)
    {
        bytesConsumed = 0;
        int pos = 0;
        ParseStatus status = WireReader.ReadVarInt(source, ref pos, out ulong offset);
        if (status != ParseStatus.Ok)
        {
            return status;
        }

        status = WireReader.ReadVarInt(source, ref pos, out ulong length);
        if (status != ParseStatus.Ok)
        {
            return status;
        }

        if (pos >= source.Length)
        {
            return ParseStatus.Truncated;
        }

        byte flags = source[pos++];
        if ((flags & ~0x03) != 0)
        {
            return ParseStatus.BadFlags;
        }

        // Both values ≤ 2^62−1, so the sum cannot overflow.
        if (length == 0 || offset + length > header.TotalLength)
        {
            return ParseStatus.BadBulkRange;
        }

        if (maxLength < 0 || length > (ulong)maxLength)
        {
            return ParseStatus.MessageTooLarge;
        }

        header.Offset = offset;
        header.Length = length;
        header.Flags = (BulkFlags)flags;
        bytesConsumed = pos;
        return ParseStatus.Ok;
    }

    /// <summary>Bulk header stage 3 (32 bytes): the object hash.</summary>
    internal static ParseStatus ParseBulkHash(ReadOnlySpan<byte> source, ref BulkHeader header, out int bytesConsumed)
    {
        if (source.Length < BulkHashLength)
        {
            bytesConsumed = 0;
            return ParseStatus.Truncated;
        }

        source.Slice(0, BulkHashLength).CopyTo(header.Hash);
        bytesConsumed = BulkHashLength;
        return ParseStatus.Ok;
    }

    private static void ValidateChannel(ushort channel)
    {
        if (channel > MaxChannelId)
        {
            throw new ArgumentOutOfRangeException(nameof(channel), channel, $"Channel ids are 0..{MaxChannelId}.");
        }
    }

    private static void ThrowDestinationTooSmall(int length) =>
        throw new ArgumentException($"Destination is too small ({length} bytes needed).", "destination");
}
