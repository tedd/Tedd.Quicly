using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Core.Control;

public static partial class ControlCodec
{
    // magic 4, version 2, flags 2, tableHash 8, lastEpoch 4, maxReceiveDatagram 2, caps 2 (the two token length prefixes are extra).
    private const int HelloFixedLength = 24;

    // status 1, sessionId 8, epoch 4, maxReceiveDatagram 2, caps 2, tableIncluded 1 (varints and length prefixes are extra).
    private const int HelloAckFixedLength = 18;

    // Canonical entry: id varint (≥ 1), mode u8, flags u8, priority u8, maxMessageSize varint (≥ 1); plus a name length (≥ 1).
    private const int MinTableChannelLength = 6;

    /// <summary>Writes a Hello control-stream message. The version field is always <see cref="ProtocolVersion"/>.</summary>
    /// <param name="destination">Receives the frame.</param>
    /// <param name="message">The message.</param>
    /// <param name="written">Bytes written, or 0 when <paramref name="destination"/> is too small.</param>
    /// <returns><see langword="false"/> when <paramref name="destination"/> is too small.</returns>
    /// <exception cref="ArgumentException">A token exceeds <see cref="MaxTokenLength"/> bytes.</exception>
    public static bool TryWrite(Span<byte> destination, in Hello message, out int written)
    {
        ReadOnlySpan<byte> sessionToken = message.SessionToken;
        ReadOnlySpan<byte> authToken = message.AuthToken;
        ThrowIfTokenTooLong(sessionToken, nameof(message.SessionToken));
        ThrowIfTokenTooLong(authToken, nameof(message.AuthToken));
        int bodyLength = HelloFixedLength
            + VarInt.GetLength((ulong)sessionToken.Length) + sessionToken.Length
            + VarInt.GetLength((ulong)authToken.Length) + authToken.Length;
        if (!TryBeginFrame(destination, ControlCarrier.Stream, ControlType.Hello, bodyLength, out ControlWriter writer))
        {
            written = 0;
            return false;
        }

        writer.WriteUInt32(MagicValue);
        writer.WriteUInt16(ProtocolVersion);
        writer.WriteUInt16((ushort)message.Flags);
        writer.WriteUInt64(message.TableHash);
        writer.WriteUInt32(message.LastEpoch);
        writer.WriteLengthPrefixed(sessionToken);
        writer.WriteLengthPrefixed(authToken);
        writer.WriteUInt16(message.MaxReceiveDatagram);
        writer.WriteUInt16((ushort)message.Caps);
        written = writer.Position;
        return true;
    }

    /// <summary>Writes a HelloAck control-stream message.</summary>
    /// <param name="destination">Receives the frame.</param>
    /// <param name="message">The message. <see cref="HelloAck.Table"/>, when not empty, must be a well-formed table section.</param>
    /// <param name="written">Bytes written, or 0 when <paramref name="destination"/> is too small.</param>
    /// <returns><see langword="false"/> when <paramref name="destination"/> is too small.</returns>
    /// <exception cref="ArgumentException">
    /// Undefined status, token over 4 096 bytes, invalid reason, varint field above 2^62 − 1, malformed table section,
    /// or a message larger than one control frame.
    /// </exception>
    public static bool TryWrite(Span<byte> destination, in HelloAck message, out int written)
    {
        if (!IsDefined(message.Status))
        {
            throw new ArgumentOutOfRangeException(nameof(message), message.Status, "Undefined HelloAck status.");
        }

        ReadOnlySpan<byte> sessionToken = message.SessionToken;
        ReadOnlySpan<byte> table = message.Table;
        ReadOnlySpan<byte> reason = message.Reason;
        ThrowIfTokenTooLong(sessionToken, nameof(message.SessionToken));
        ThrowIfInvalidReason(reason);
        if (!table.IsEmpty && (MeasureTableSection(table, out int tableLength) != ControlParseStatus.Ok || tableLength != table.Length))
        {
            throw new ArgumentException("The table section is not a well-formed canonical table followed by its names.", nameof(message));
        }

        long bodyLength = (long)HelloAckFixedLength
            + VarInt.GetLength(message.MaxMessageSize) + VarInt.GetLength(message.HeartbeatMicros) + VarInt.GetLength(message.GraceMicros)
            + VarInt.GetLength((ulong)sessionToken.Length) + sessionToken.Length
            + table.Length
            + VarInt.GetLength((ulong)reason.Length) + reason.Length;
        if (bodyLength >= MaxFrameLength)
        {
            ThrowFrameTooLarge(bodyLength);
        }

        if (!TryBeginFrame(destination, ControlCarrier.Stream, ControlType.HelloAck, (int)bodyLength, out ControlWriter writer))
        {
            written = 0;
            return false;
        }

        writer.WriteByte((byte)message.Status);
        writer.WriteUInt64(message.SessionId);
        writer.WriteUInt32(message.Epoch);
        writer.WriteUInt16(message.MaxReceiveDatagram);
        writer.WriteUInt16((ushort)message.Caps);
        writer.WriteVarInt(message.MaxMessageSize);
        writer.WriteVarInt(message.HeartbeatMicros);
        writer.WriteVarInt(message.GraceMicros);
        writer.WriteLengthPrefixed(sessionToken);
        writer.WriteByte(table.IsEmpty ? (byte)0 : (byte)1);
        writer.WriteBytes(table);
        writer.WriteLengthPrefixed(reason);
        written = writer.Position;
        return true;
    }

    /// <summary>Decodes a Hello body.</summary>
    /// <param name="body">The body returned by <see cref="TryReadStream"/>.</param>
    /// <param name="message">
    /// The message on success (token spans are slices of <paramref name="body"/>). On
    /// <see cref="ControlParseStatus.UnsupportedVersion"/> only <see cref="Hello.Version"/> is set.
    /// </param>
    /// <returns>
    /// <see cref="ControlParseStatus.Ok"/>; <see cref="ControlParseStatus.InvalidMagic"/>;
    /// <see cref="ControlParseStatus.UnsupportedVersion"/> (answer with <see cref="HelloStatus.VersionMismatch"/>); or the
    /// first other violation found.
    /// </returns>
    public static ControlParseStatus TryParse(ReadOnlySpan<byte> body, out Hello message)
    {
        message = default;
        ControlReader reader = new(body);
        if (!reader.TryReadUInt32(out uint magic))
        {
            return ControlParseStatus.Truncated;
        }

        if (magic != MagicValue)
        {
            return ControlParseStatus.InvalidMagic;
        }

        if (!reader.TryReadUInt16(out ushort version))
        {
            return ControlParseStatus.Truncated;
        }

        if (version != ProtocolVersion)
        {
            // The rest of the layout belongs to that version; do not interpret it.
            message = new Hello { Version = version };
            return ControlParseStatus.UnsupportedVersion;
        }

        if (!reader.TryReadUInt16(out ushort flags) || !reader.TryReadUInt64(out ulong tableHash) || !reader.TryReadUInt32(out uint lastEpoch))
        {
            return ControlParseStatus.Truncated;
        }

        ControlParseStatus status;
        if ((status = reader.ReadLengthPrefixed(MaxTokenLength, out ReadOnlySpan<byte> sessionToken)) != ControlParseStatus.Ok
            || (status = reader.ReadLengthPrefixed(MaxTokenLength, out ReadOnlySpan<byte> authToken)) != ControlParseStatus.Ok)
        {
            return status;
        }

        if (!reader.TryReadUInt16(out ushort maxReceiveDatagram) || !reader.TryReadUInt16(out ushort caps))
        {
            return ControlParseStatus.Truncated;
        }

        if ((status = reader.End()) != ControlParseStatus.Ok)
        {
            return status;
        }

        message = new Hello
        {
            Version = version,
            Flags = (HelloFlags)flags,
            TableHash = tableHash,
            LastEpoch = lastEpoch,
            SessionToken = sessionToken,
            AuthToken = authToken,
            MaxReceiveDatagram = maxReceiveDatagram,
            Caps = (PeerCaps)caps,
        };
        return ControlParseStatus.Ok;
    }

    /// <summary>Decodes a HelloAck body.</summary>
    /// <param name="body">The body returned by <see cref="TryReadStream"/>.</param>
    /// <param name="message">The message on success (spans are slices of <paramref name="body"/>).</param>
    /// <returns>
    /// <see cref="ControlParseStatus.Ok"/> or the first violation found (<see cref="ControlParseStatus.InvalidValue"/> for an
    /// undefined status or a <c>tableIncluded</c> byte other than 0/1).
    /// </returns>
    public static ControlParseStatus TryParse(ReadOnlySpan<byte> body, out HelloAck message)
    {
        message = default;
        ControlReader reader = new(body);
        if (!reader.TryReadByte(out byte status) || !reader.TryReadUInt64(out ulong sessionId) || !reader.TryReadUInt32(out uint epoch)
            || !reader.TryReadUInt16(out ushort maxReceiveDatagram) || !reader.TryReadUInt16(out ushort caps))
        {
            return ControlParseStatus.Truncated;
        }

        if (!IsDefined((HelloStatus)status))
        {
            return ControlParseStatus.InvalidValue;
        }

        ControlParseStatus result;
        if ((result = reader.ReadVarInt(out ulong maxMessageSize)) != ControlParseStatus.Ok
            || (result = reader.ReadVarInt(out ulong heartbeatMicros)) != ControlParseStatus.Ok
            || (result = reader.ReadVarInt(out ulong graceMicros)) != ControlParseStatus.Ok
            || (result = reader.ReadLengthPrefixed(MaxTokenLength, out ReadOnlySpan<byte> sessionToken)) != ControlParseStatus.Ok)
        {
            return result;
        }

        if (!reader.TryReadByte(out byte tableIncluded))
        {
            return ControlParseStatus.Truncated;
        }

        ReadOnlySpan<byte> table = default;
        if (tableIncluded == 1)
        {
            if ((result = MeasureTableSection(reader.Rest, out int tableLength)) != ControlParseStatus.Ok)
            {
                return result;
            }

            table = reader.ReadSpan(tableLength);
        }
        else if (tableIncluded != 0)
        {
            return ControlParseStatus.InvalidValue;
        }

        if ((result = reader.ReadUtf8(MaxReasonLength, out ReadOnlySpan<byte> reason)) != ControlParseStatus.Ok
            || (result = reader.End()) != ControlParseStatus.Ok)
        {
            return result;
        }

        message = new HelloAck
        {
            Status = (HelloStatus)status,
            SessionId = sessionId,
            Epoch = epoch,
            MaxReceiveDatagram = maxReceiveDatagram,
            Caps = (PeerCaps)caps,
            MaxMessageSize = maxMessageSize,
            HeartbeatMicros = heartbeatMicros,
            GraceMicros = graceMicros,
            SessionToken = sessionToken,
            Table = table,
            Reason = reason,
        };
        return ControlParseStatus.Ok;
    }

    /// <summary>
    /// Structural walk of a HelloAck table section: <c>count varint</c>, <c>count</c> canonical entries (<c>id varint</c>
    /// in [2, 16383], <c>mode u8</c>, <c>flags u8</c>, <c>priority u8</c>, <c>maxMessageSize varint</c>), then <c>count</c>
    /// names (<c>len varint</c> ≤ 64 + UTF-8). Returns the section's length; bytes after it are not examined.
    /// </summary>
    internal static ControlParseStatus MeasureTableSection(ReadOnlySpan<byte> source, out int length)
    {
        length = 0;
        ControlReader reader = new(source);
        ControlParseStatus status = reader.ReadVarInt(out ulong count);
        if (status != ControlParseStatus.Ok)
        {
            return status;
        }

        if (count > (ulong)(reader.Remaining / MinTableChannelLength))
        {
            return ControlParseStatus.CountTooLarge;
        }

        for (int i = 0; i < (int)count; i++)
        {
            if ((status = reader.ReadChannel(out _)) != ControlParseStatus.Ok)
            {
                return status;
            }

            if (!reader.TrySkip(3))
            {
                return ControlParseStatus.Truncated;
            }

            if ((status = reader.ReadVarInt(out _)) != ControlParseStatus.Ok)
            {
                return status;
            }
        }

        for (int i = 0; i < (int)count; i++)
        {
            if ((status = reader.ReadUtf8(MaxChannelNameLength, out _)) != ControlParseStatus.Ok)
            {
                return status;
            }
        }

        length = reader.Position;
        return ControlParseStatus.Ok;
    }

    internal static bool IsDefined(HelloStatus status) =>
        status is HelloStatus.Accepted or HelloStatus.VersionMismatch or HelloStatus.ChannelTableMismatch or HelloStatus.Rejected
            or HelloStatus.ServerFull or HelloStatus.InternalError or HelloStatus.DatagramsRequired or HelloStatus.Informational;
}
