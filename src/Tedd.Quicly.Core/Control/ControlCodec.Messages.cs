using System.Buffers.Binary;
using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Core.Control;

public static partial class ControlCodec
{
    private const int PingBodyLength = 4;
    private const int PongBodyLength = 12;

    /// <summary>Writes a Ping in the given carrier form.</summary>
    /// <param name="destination">Receives the frame.</param>
    /// <param name="message">The message.</param>
    /// <param name="carrier">Datagram or stream framing.</param>
    /// <param name="written">Bytes written, or 0 when <paramref name="destination"/> is too small.</param>
    /// <returns><see langword="false"/> when <paramref name="destination"/> is too small.</returns>
    public static bool TryWrite(Span<byte> destination, in Ping message, ControlCarrier carrier, out int written)
    {
        if (!TryBeginFrame(destination, carrier, ControlType.Ping, PingBodyLength, out ControlWriter writer))
        {
            written = 0;
            return false;
        }

        writer.WriteUInt32(message.TimeMicros);
        written = writer.Position;
        return true;
    }

    /// <summary>Writes a Pong in the given carrier form.</summary>
    /// <param name="destination">Receives the frame.</param>
    /// <param name="message">The message.</param>
    /// <param name="carrier">Datagram or stream framing.</param>
    /// <param name="written">Bytes written, or 0 when <paramref name="destination"/> is too small.</param>
    /// <returns><see langword="false"/> when <paramref name="destination"/> is too small.</returns>
    public static bool TryWrite(Span<byte> destination, in Pong message, ControlCarrier carrier, out int written)
    {
        if (!TryBeginFrame(destination, carrier, ControlType.Pong, PongBodyLength, out ControlWriter writer))
        {
            written = 0;
            return false;
        }

        writer.WriteUInt32(message.EchoedTimeMicros);
        writer.WriteUInt32(message.ReceiveTimeMicros);
        writer.WriteUInt32(message.SendTimeMicros);
        written = writer.Position;
        return true;
    }

    /// <summary>Writes a BulkProgress in the given carrier form.</summary>
    /// <param name="destination">Receives the frame.</param>
    /// <param name="message">The message; both fields at most 2^62 − 1.</param>
    /// <param name="carrier">Datagram or stream framing.</param>
    /// <param name="written">Bytes written, or 0 when <paramref name="destination"/> is too small.</param>
    /// <returns><see langword="false"/> when <paramref name="destination"/> is too small.</returns>
    /// <exception cref="ArgumentOutOfRangeException">A field exceeds 2^62 − 1.</exception>
    public static bool TryWrite(Span<byte> destination, in BulkProgress message, ControlCarrier carrier, out int written)
    {
        int bodyLength = VarInt.GetLength(message.TransferId) + VarInt.GetLength(message.BytesAccepted);
        if (!TryBeginFrame(destination, carrier, ControlType.BulkProgress, bodyLength, out ControlWriter writer))
        {
            written = 0;
            return false;
        }

        writer.WriteVarInt(message.TransferId);
        writer.WriteVarInt(message.BytesAccepted);
        written = writer.Position;
        return true;
    }

    /// <summary>Writes a Close control-stream message.</summary>
    /// <param name="destination">Receives the frame.</param>
    /// <param name="message">The message.</param>
    /// <param name="written">Bytes written, or 0 when <paramref name="destination"/> is too small.</param>
    /// <returns><see langword="false"/> when <paramref name="destination"/> is too small.</returns>
    /// <exception cref="ArgumentException">The code exceeds 32 bits or the reason is not ≤ 512 bytes of valid UTF-8.</exception>
    public static bool TryWrite(Span<byte> destination, in Close message, out int written)
    {
        uint code = CheckCode(message.Code);
        ReadOnlySpan<byte> reason = message.Reason;
        ThrowIfInvalidReason(reason);
        int bodyLength = 4 + VarInt.GetLength((ulong)reason.Length) + reason.Length;
        if (!TryBeginFrame(destination, ControlCarrier.Stream, ControlType.Close, bodyLength, out ControlWriter writer))
        {
            written = 0;
            return false;
        }

        writer.WriteUInt32(code);
        writer.WriteLengthPrefixed(reason);
        written = writer.Position;
        return true;
    }

    /// <summary>Writes a BulkRequest control-stream message.</summary>
    /// <param name="destination">Receives the frame.</param>
    /// <param name="message">The message.</param>
    /// <param name="written">Bytes written, or 0 when <paramref name="destination"/> is too small.</param>
    /// <returns><see langword="false"/> when <paramref name="destination"/> is too small.</returns>
    /// <exception cref="ArgumentOutOfRangeException">
    /// The channel is outside [2, 16383], a field exceeds 2^62 − 1, or <c>Offset + Length</c> exceeds 2^62 − 1.
    /// </exception>
    public static bool TryWrite(Span<byte> destination, in BulkRequest message, out int written)
    {
        ThrowIfInvalidChannel(message.Channel, nameof(message));
        int bodyLength = VarInt.GetLength(message.RequestId) + VarInt.GetLength(message.Channel)
            + VarInt.GetLength(message.ObjectId) + VarInt.GetLength(message.ObjectVersion)
            + VarInt.GetLength(message.Offset) + VarInt.GetLength(message.Length);
        if (message.Length > VarInt.MaxValue - message.Offset)
        {
            throw new ArgumentOutOfRangeException(nameof(message), "Offset + Length exceeds 2^62 - 1.");
        }

        if (!TryBeginFrame(destination, ControlCarrier.Stream, ControlType.BulkRequest, bodyLength, out ControlWriter writer))
        {
            written = 0;
            return false;
        }

        writer.WriteVarInt(message.RequestId);
        writer.WriteVarInt(message.Channel);
        writer.WriteVarInt(message.ObjectId);
        writer.WriteVarInt(message.ObjectVersion);
        writer.WriteVarInt(message.Offset);
        writer.WriteVarInt(message.Length);
        written = writer.Position;
        return true;
    }

    /// <summary>Writes a BulkCancel control-stream message.</summary>
    /// <param name="destination">Receives the frame.</param>
    /// <param name="message">The message.</param>
    /// <param name="written">Bytes written, or 0 when <paramref name="destination"/> is too small.</param>
    /// <returns><see langword="false"/> when <paramref name="destination"/> is too small.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The transfer id exceeds 2^62 − 1 or the code exceeds 32 bits.</exception>
    public static bool TryWrite(Span<byte> destination, in BulkCancel message, out int written) =>
        TryWriteIdAndCode(destination, ControlType.BulkCancel, message.TransferId, message.Code, out written);

    /// <summary>Writes a BulkReject control-stream message.</summary>
    /// <param name="destination">Receives the frame.</param>
    /// <param name="message">The message.</param>
    /// <param name="written">Bytes written, or 0 when <paramref name="destination"/> is too small.</param>
    /// <returns><see langword="false"/> when <paramref name="destination"/> is too small.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The request id exceeds 2^62 − 1 or the code exceeds 32 bits.</exception>
    public static bool TryWrite(Span<byte> destination, in BulkReject message, out int written) =>
        TryWriteIdAndCode(destination, ControlType.BulkReject, message.RequestId, message.Code, out written);

    /// <summary>Writes a KeyRetired control-stream message.</summary>
    /// <param name="destination">Receives the frame.</param>
    /// <param name="message">The message.</param>
    /// <param name="written">Bytes written, or 0 when <paramref name="destination"/> is too small.</param>
    /// <returns><see langword="false"/> when <paramref name="destination"/> is too small.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The channel is outside [2, 16383] or the key exceeds 2^62 − 1.</exception>
    public static bool TryWrite(Span<byte> destination, in KeyRetired message, out int written)
    {
        ThrowIfInvalidChannel(message.Channel, nameof(message));
        int bodyLength = VarInt.GetLength(message.Channel) + VarInt.GetLength(message.Key);
        if (!TryBeginFrame(destination, ControlCarrier.Stream, ControlType.KeyRetired, bodyLength, out ControlWriter writer))
        {
            written = 0;
            return false;
        }

        writer.WriteVarInt(message.Channel);
        writer.WriteVarInt(message.Key);
        written = writer.Position;
        return true;
    }

    /// <summary>Writes a ChannelTableRequest control-stream message (empty body).</summary>
    /// <param name="destination">Receives the frame.</param>
    /// <param name="written">Bytes written (2), or 0 when <paramref name="destination"/> is too small.</param>
    /// <returns><see langword="false"/> when <paramref name="destination"/> is too small.</returns>
    public static bool TryWriteChannelTableRequest(Span<byte> destination, out int written)
    {
        if (!TryBeginFrame(destination, ControlCarrier.Stream, ControlType.ChannelTableRequest, 0, out ControlWriter writer))
        {
            written = 0;
            return false;
        }

        written = writer.Position;
        return true;
    }

    /// <summary>Decodes a Ping body (exactly 4 bytes).</summary>
    /// <param name="body">The body returned by <see cref="TryReadDatagram"/> or <see cref="TryReadStream"/>.</param>
    /// <param name="message">The message on success.</param>
    /// <returns><see cref="ControlParseStatus.Ok"/>, <see cref="ControlParseStatus.Truncated"/> or <see cref="ControlParseStatus.TrailingBytes"/>.</returns>
    public static ControlParseStatus TryParse(ReadOnlySpan<byte> body, out Ping message)
    {
        if (body.Length == PingBodyLength)
        {
            message = new Ping(BinaryPrimitives.ReadUInt32LittleEndian(body));
            return ControlParseStatus.Ok;
        }

        message = default;
        return body.Length < PingBodyLength ? ControlParseStatus.Truncated : ControlParseStatus.TrailingBytes;
    }

    /// <summary>Decodes a Pong body (exactly 12 bytes).</summary>
    /// <param name="body">The body returned by <see cref="TryReadDatagram"/> or <see cref="TryReadStream"/>.</param>
    /// <param name="message">The message on success.</param>
    /// <returns><see cref="ControlParseStatus.Ok"/>, <see cref="ControlParseStatus.Truncated"/> or <see cref="ControlParseStatus.TrailingBytes"/>.</returns>
    public static ControlParseStatus TryParse(ReadOnlySpan<byte> body, out Pong message)
    {
        if (body.Length == PongBodyLength)
        {
            message = new Pong(
                BinaryPrimitives.ReadUInt32LittleEndian(body),
                BinaryPrimitives.ReadUInt32LittleEndian(body.Slice(4)),
                BinaryPrimitives.ReadUInt32LittleEndian(body.Slice(8)));
            return ControlParseStatus.Ok;
        }

        message = default;
        return body.Length < PongBodyLength ? ControlParseStatus.Truncated : ControlParseStatus.TrailingBytes;
    }

    /// <summary>Decodes a BulkProgress body.</summary>
    /// <param name="body">The body returned by <see cref="TryReadDatagram"/> or <see cref="TryReadStream"/>.</param>
    /// <param name="message">The message on success.</param>
    /// <returns><see cref="ControlParseStatus.Ok"/> or the first violation found.</returns>
    public static ControlParseStatus TryParse(ReadOnlySpan<byte> body, out BulkProgress message)
    {
        message = default;
        ControlReader reader = new(body);
        ControlParseStatus status;
        if ((status = reader.ReadVarInt(out ulong transferId)) != ControlParseStatus.Ok
            || (status = reader.ReadVarInt(out ulong bytesAccepted)) != ControlParseStatus.Ok
            || (status = reader.End()) != ControlParseStatus.Ok)
        {
            return status;
        }

        message = new BulkProgress(transferId, bytesAccepted);
        return ControlParseStatus.Ok;
    }

    /// <summary>Decodes a Close body; the reason must be ≤ 512 bytes of valid UTF-8.</summary>
    /// <param name="body">The body returned by <see cref="TryReadStream"/>.</param>
    /// <param name="message">The message on success; its reason is a slice of <paramref name="body"/>.</param>
    /// <returns><see cref="ControlParseStatus.Ok"/> or the first violation found.</returns>
    public static ControlParseStatus TryParse(ReadOnlySpan<byte> body, out Close message)
    {
        message = default;
        ControlReader reader = new(body);
        if (!reader.TryReadUInt32(out uint code))
        {
            return ControlParseStatus.Truncated;
        }

        ControlParseStatus status;
        if ((status = reader.ReadUtf8(MaxReasonLength, out ReadOnlySpan<byte> reason)) != ControlParseStatus.Ok
            || (status = reader.End()) != ControlParseStatus.Ok)
        {
            return status;
        }

        message = new Close((QuiclyErrorCode)code, reason);
        return ControlParseStatus.Ok;
    }

    /// <summary>Decodes a BulkRequest body.</summary>
    /// <param name="body">The body returned by <see cref="TryReadStream"/>.</param>
    /// <param name="message">The message on success.</param>
    /// <returns>
    /// <see cref="ControlParseStatus.Ok"/> or the first violation found (<see cref="ControlParseStatus.InvalidValue"/>
    /// when <c>Offset + Length</c> exceeds 2^62 − 1).
    /// </returns>
    public static ControlParseStatus TryParse(ReadOnlySpan<byte> body, out BulkRequest message)
    {
        message = default;
        ControlReader reader = new(body);
        ControlParseStatus status;
        if ((status = reader.ReadVarInt(out ulong requestId)) != ControlParseStatus.Ok
            || (status = reader.ReadChannel(out ushort channel)) != ControlParseStatus.Ok
            || (status = reader.ReadVarInt(out ulong objectId)) != ControlParseStatus.Ok
            || (status = reader.ReadVarInt(out ulong objectVersion)) != ControlParseStatus.Ok
            || (status = reader.ReadVarInt(out ulong offset)) != ControlParseStatus.Ok
            || (status = reader.ReadVarInt(out ulong length)) != ControlParseStatus.Ok
            || (status = reader.End()) != ControlParseStatus.Ok)
        {
            return status;
        }

        if (length > VarInt.MaxValue - offset)
        {
            return ControlParseStatus.InvalidValue;
        }

        message = new BulkRequest(requestId, channel, objectId, objectVersion, offset, length);
        return ControlParseStatus.Ok;
    }

    /// <summary>Decodes a BulkCancel body.</summary>
    /// <param name="body">The body returned by <see cref="TryReadStream"/>.</param>
    /// <param name="message">The message on success.</param>
    /// <returns><see cref="ControlParseStatus.Ok"/> or the first violation found.</returns>
    public static ControlParseStatus TryParse(ReadOnlySpan<byte> body, out BulkCancel message)
    {
        ControlParseStatus status = TryParseIdAndCode(body, out ulong id, out uint code);
        message = status == ControlParseStatus.Ok ? new BulkCancel(id, (QuiclyErrorCode)code) : default;
        return status;
    }

    /// <summary>Decodes a BulkReject body.</summary>
    /// <param name="body">The body returned by <see cref="TryReadStream"/>.</param>
    /// <param name="message">The message on success.</param>
    /// <returns><see cref="ControlParseStatus.Ok"/> or the first violation found.</returns>
    public static ControlParseStatus TryParse(ReadOnlySpan<byte> body, out BulkReject message)
    {
        ControlParseStatus status = TryParseIdAndCode(body, out ulong id, out uint code);
        message = status == ControlParseStatus.Ok ? new BulkReject(id, (QuiclyErrorCode)code) : default;
        return status;
    }

    /// <summary>Decodes a KeyRetired body.</summary>
    /// <param name="body">The body returned by <see cref="TryReadStream"/>.</param>
    /// <param name="message">The message on success.</param>
    /// <returns><see cref="ControlParseStatus.Ok"/> or the first violation found.</returns>
    public static ControlParseStatus TryParse(ReadOnlySpan<byte> body, out KeyRetired message)
    {
        message = default;
        ControlReader reader = new(body);
        ControlParseStatus status;
        if ((status = reader.ReadChannel(out ushort channel)) != ControlParseStatus.Ok
            || (status = reader.ReadVarInt(out ulong key)) != ControlParseStatus.Ok
            || (status = reader.End()) != ControlParseStatus.Ok)
        {
            return status;
        }

        message = new KeyRetired(channel, key);
        return ControlParseStatus.Ok;
    }

    /// <summary>Validates a ChannelTableRequest body, which must be empty.</summary>
    /// <param name="body">The body returned by <see cref="TryReadStream"/>.</param>
    /// <returns><see cref="ControlParseStatus.Ok"/> or <see cref="ControlParseStatus.TrailingBytes"/>.</returns>
    public static ControlParseStatus TryParseChannelTableRequest(ReadOnlySpan<byte> body) =>
        body.IsEmpty ? ControlParseStatus.Ok : ControlParseStatus.TrailingBytes;

    private static bool TryWriteIdAndCode(Span<byte> destination, ControlType type, ulong id, QuiclyErrorCode code, out int written)
    {
        uint wireCode = CheckCode(code);
        int bodyLength = VarInt.GetLength(id) + 4;
        if (!TryBeginFrame(destination, ControlCarrier.Stream, type, bodyLength, out ControlWriter writer))
        {
            written = 0;
            return false;
        }

        writer.WriteVarInt(id);
        writer.WriteUInt32(wireCode);
        written = writer.Position;
        return true;
    }

    private static ControlParseStatus TryParseIdAndCode(ReadOnlySpan<byte> body, out ulong id, out uint code)
    {
        code = 0;
        ControlReader reader = new(body);
        ControlParseStatus status = reader.ReadVarInt(out id);
        if (status != ControlParseStatus.Ok)
        {
            return status;
        }

        if (!reader.TryReadUInt32(out code))
        {
            return ControlParseStatus.Truncated;
        }

        return reader.End();
    }
}
