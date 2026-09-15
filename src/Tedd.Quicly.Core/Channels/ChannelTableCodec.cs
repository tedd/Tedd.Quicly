using System.Text;
using System.Text.Unicode;
using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Core.Channels;

/// <summary>
/// Canonical channel-table encoding, <c>TableHash</c>, and the <c>HelloAck</c> table section (PROTOCOL.md §1, §3.4).
/// </summary>
/// <remarks>
/// <para>Canonical encoding: <c>count varint</c>, then per channel in ascending id order <c>id varint, mode u8,
/// flags u8, priority u8, maxMessageSize varint</c>; flags = bit0 keyed, bit1 seq32, bit2 fragmentation,
/// bit3 request-response, bit4 coalesce, bits 5–6 compression. Varints are minimal. Names are excluded.</para>
/// <para>Table section of <c>HelloAck</c>: the canonical encoding followed, for each channel in the same order, by
/// <c>name len varint (≤ 64) + UTF-8 bytes</c>.</para>
/// <para>Golden vectors: <c>docs/protocol-vectors.md</c>.</para>
/// </remarks>
public static class ChannelTableCodec
{
    /// <summary>Largest number of channels a table can hold (ids 2…16383).</summary>
    public const int MaxChannels = ChannelDefinition.MaxId - ChannelDefinition.MinId + 1;

    // Smallest possible wire size of one channel: id(1) + mode(1) + flags(1) + priority(1) + maxMessageSize(1) + name length(1).
    private const int MinEntryWithNameBytes = 6;

    /// <summary>Returns the size of the canonical encoding of <paramref name="table"/>.</summary>
    /// <param name="table">The table.</param>
    public static int GetCanonicalLength(ChannelTable table)
    {
        ArgumentNullException.ThrowIfNull(table);
        int length = VarInt.GetLength((ulong)table.Count);
        foreach (ChannelDefinition ch in table.All)
        {
            length += ch.ChannelIdLength + 3 + VarInt.GetLength((ulong)ch.MaxMessageSize);
        }

        return length;
    }

    /// <summary>Returns the size of the <c>HelloAck</c> table section (canonical encoding + names).</summary>
    /// <param name="table">The table.</param>
    public static int GetLengthWithNames(ChannelTable table)
    {
        int length = GetCanonicalLength(table);
        foreach (ChannelDefinition ch in table.All)
        {
            // 0…63 is a one-byte varint but 64 (MaxNameBytes) needs two bytes (0x40 0x40).
            length += VarInt.GetLength((ulong)ch.NameUtf8.Length) + ch.NameUtf8.Length;
        }

        return length;
    }

    /// <summary>Writes the canonical encoding.</summary>
    /// <param name="table">The table.</param>
    /// <param name="destination">Buffer of at least <see cref="GetCanonicalLength"/> bytes.</param>
    /// <returns>Bytes written.</returns>
    /// <exception cref="ArgumentException"><paramref name="destination"/> is too small.</exception>
    public static int WriteCanonical(ChannelTable table, Span<byte> destination)
    {
        if (!TryWriteCanonical(table, destination, out int written))
        {
            throw new ArgumentException($"Destination is too small for the canonical channel table ({GetCanonicalLength(table)} bytes).", nameof(destination));
        }

        return written;
    }

    /// <summary>Attempts to write the canonical encoding.</summary>
    /// <param name="table">The table.</param>
    /// <param name="destination">The buffer.</param>
    /// <param name="bytesWritten">Bytes written, or 0 when the buffer is too small.</param>
    /// <returns><see langword="false"/> when <paramref name="destination"/> is too small.</returns>
    public static bool TryWriteCanonical(ChannelTable table, Span<byte> destination, out int bytesWritten)
    {
        int length = GetCanonicalLength(table);
        if (destination.Length < length)
        {
            bytesWritten = 0;
            return false;
        }

        bytesWritten = WriteCanonicalCore(table, destination);
        return true;
    }

    /// <summary>Writes the <c>HelloAck</c> table section (canonical encoding followed by the names).</summary>
    /// <param name="table">The table.</param>
    /// <param name="destination">Buffer of at least <see cref="GetLengthWithNames"/> bytes.</param>
    /// <returns>Bytes written.</returns>
    /// <exception cref="ArgumentException"><paramref name="destination"/> is too small.</exception>
    public static int WriteWithNames(ChannelTable table, Span<byte> destination)
    {
        if (!TryWriteWithNames(table, destination, out int written))
        {
            throw new ArgumentException($"Destination is too small for the channel table section ({GetLengthWithNames(table)} bytes).", nameof(destination));
        }

        return written;
    }

    /// <summary>Attempts to write the <c>HelloAck</c> table section.</summary>
    /// <param name="table">The table.</param>
    /// <param name="destination">The buffer.</param>
    /// <param name="bytesWritten">Bytes written, or 0 when the buffer is too small.</param>
    /// <returns><see langword="false"/> when <paramref name="destination"/> is too small.</returns>
    public static bool TryWriteWithNames(ChannelTable table, Span<byte> destination, out int bytesWritten)
    {
        int length = GetLengthWithNames(table);
        if (destination.Length < length)
        {
            bytesWritten = 0;
            return false;
        }

        int pos = WriteCanonicalCore(table, destination);
        foreach (ChannelDefinition ch in table.All)
        {
            pos += VarInt.Write(destination.Slice(pos), (ulong)ch.NameUtf8.Length);
            ch.NameUtf8.CopyTo(destination.Slice(pos));
            pos += ch.NameUtf8.Length;
        }

        bytesWritten = pos;
        return true;
    }

    /// <summary>Computes <c>TableHash</c>: XXH64 (seed 0) of the canonical encoding. <see cref="ChannelTable.Hash"/> caches it.</summary>
    /// <param name="table">The table.</param>
    public static ulong ComputeHash(ChannelTable table)
    {
        int length = GetCanonicalLength(table);
        Span<byte> buffer = length <= 1024 ? stackalloc byte[1024] : new byte[length];
        int written = WriteCanonicalCore(table, buffer);
        return XxHash64.Hash(buffer.Slice(0, written));
    }

    /// <summary>
    /// Parses a <c>HelloAck</c> table section into a read-only description. Every bound is checked before memory is
    /// allocated (count ≤ 16 382 and plausible for the input length, names ≤ 64 valid UTF-8 bytes, ids strictly
    /// ascending in 2…16383, defined modes, no reserved flag bits, 1 ≤ maxMessageSize ≤ 16 MiB, minimal varints).
    /// Never throws on malformed input.
    /// </summary>
    /// <param name="source">Bytes starting at the table section (trailing bytes are left unconsumed).</param>
    /// <param name="description">The description, or <see langword="null"/> on failure.</param>
    /// <param name="bytesConsumed">Bytes of <paramref name="source"/> belonging to the section, or 0 on failure.</param>
    /// <returns>The outcome.</returns>
    public static ChannelTableParseStatus TryParseWithNames(ReadOnlySpan<byte> source, out ChannelTableDescription? description, out int bytesConsumed)
    {
        description = null;
        bytesConsumed = 0;
        int pos = 0;

        ChannelTableParseStatus status = ReadVarInt(source, ref pos, out ulong count);
        if (status != ChannelTableParseStatus.Ok)
        {
            return status;
        }

        if (count > MaxChannels)
        {
            return ChannelTableParseStatus.TooManyChannels;
        }

        if ((long)count * MinEntryWithNameBytes > source.Length - pos)
        {
            return ChannelTableParseStatus.Truncated;
        }

        int n = (int)count;
        ushort[] ids = new ushort[n];
        byte[] modes = new byte[n];
        byte[] flags = new byte[n];
        byte[] priorities = new byte[n];
        int[] sizes = new int[n];
        ulong previousId = 1;
        for (int i = 0; i < n; i++)
        {
            status = ReadVarInt(source, ref pos, out ulong id);
            if (status != ChannelTableParseStatus.Ok)
            {
                return status;
            }

            if (id < ChannelDefinition.MinId || id > ChannelDefinition.MaxId)
            {
                return ChannelTableParseStatus.BadChannelId;
            }

            if (id <= previousId)
            {
                return ChannelTableParseStatus.ChannelsNotAscending;
            }

            previousId = id;
            if (source.Length - pos < 3)
            {
                return ChannelTableParseStatus.Truncated;
            }

            byte mode = source[pos];
            byte f = source[pos + 1];
            if (mode > (byte)ChannelMode.Bulk)
            {
                return ChannelTableParseStatus.BadMode;
            }

            if ((f & 0x80) != 0 || ((f >> 5) & 0x03) > (int)ChannelCompression.Lz4)
            {
                return ChannelTableParseStatus.BadFlags;
            }

            priorities[i] = source[pos + 2];
            pos += 3;
            status = ReadVarInt(source, ref pos, out ulong size);
            if (status != ChannelTableParseStatus.Ok)
            {
                return status;
            }

            if (size == 0 || size > ChannelDefinition.BulkMaxMessageSize)
            {
                return ChannelTableParseStatus.BadMaxMessageSize;
            }

            ids[i] = (ushort)id;
            modes[i] = mode;
            flags[i] = f;
            sizes[i] = (int)size;
        }

        ulong hash = XxHash64.Hash(source.Slice(0, pos));
        ChannelDescription[] channels = new ChannelDescription[n];
        for (int i = 0; i < n; i++)
        {
            status = ReadVarInt(source, ref pos, out ulong nameLength);
            if (status != ChannelTableParseStatus.Ok)
            {
                return status;
            }

            if (nameLength > ChannelDefinition.MaxNameBytes)
            {
                return ChannelTableParseStatus.NameTooLong;
            }

            if ((ulong)(source.Length - pos) < nameLength)
            {
                return ChannelTableParseStatus.Truncated;
            }

            ReadOnlySpan<byte> nameBytes = source.Slice(pos, (int)nameLength);
            if (!Utf8.IsValid(nameBytes))
            {
                return ChannelTableParseStatus.InvalidUtf8;
            }

            pos += (int)nameLength;
            channels[i] = new ChannelDescription(ids[i], (ChannelMode)modes[i], flags[i], priorities[i], sizes[i], Encoding.UTF8.GetString(nameBytes));
        }

        description = new ChannelTableDescription(channels, hash);
        bytesConsumed = pos;
        return ChannelTableParseStatus.Ok;
    }

    private static int WriteCanonicalCore(ChannelTable table, Span<byte> destination)
    {
        int pos = VarInt.Write(destination, (ulong)table.Count);
        foreach (ChannelDefinition ch in table.All)
        {
            pos += VarInt.Write(destination.Slice(pos), ch.Id);
            destination[pos] = (byte)ch.Mode;
            destination[pos + 1] = ch.CanonicalFlags;
            destination[pos + 2] = ch.Priority;
            pos += 3;
            pos += VarInt.Write(destination.Slice(pos), (ulong)ch.MaxMessageSize);
        }

        return pos;
    }

    private static ChannelTableParseStatus ReadVarInt(ReadOnlySpan<byte> source, ref int pos, out ulong value)
    {
        ReadOnlySpan<byte> rest = source.Slice(pos);
        if (VarInt.TryReadMinimal(rest, out value, out int consumed))
        {
            pos += consumed;
            return ChannelTableParseStatus.Ok;
        }

        return VarInt.TryRead(rest, out _, out _) ? ChannelTableParseStatus.NonMinimalVarint : ChannelTableParseStatus.Truncated;
    }
}
