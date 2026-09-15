namespace Tedd.Quicly.Core.Control;

public static partial class ControlCodec
{
    // channel varint (≥ 1) + key varint (≥ 1) + version u32 (4) [+ reason u8].
    internal const int LatestAckMinEntryLength = 6;
    internal const int LatestRejectMinEntryLength = 7;

    /// <summary>
    /// Validates a LatestAck body (<c>count varint</c>, then <c>count</c> × <c>channel varint, key varint, version u32</c>)
    /// and returns a reader over its entries. The whole batch is checked before anything is returned, so a malformed
    /// batch is rejected as a unit and never partially applied.
    /// </summary>
    /// <param name="body">The body returned by <see cref="TryReadDatagram"/> or <see cref="TryReadStream"/>.</param>
    /// <param name="reader">A reader over the entries on success; an empty reader otherwise.</param>
    /// <returns><see cref="ControlParseStatus.Ok"/> or the first violation found.</returns>
    public static ControlParseStatus TryParse(ReadOnlySpan<byte> body, out LatestAckBatchReader reader)
    {
        ControlParseStatus status = ValidateLatestBatch(body, withReason: false, out int count, out int entriesOffset);
        reader = status == ControlParseStatus.Ok ? new LatestAckBatchReader(body.Slice(entriesOffset), count) : default;
        return status;
    }

    /// <summary>
    /// Validates a LatestReject body (<c>count varint</c>, then <c>count</c> × <c>channel varint, key varint, version u32,
    /// reason u8</c>) and returns a reader over its entries. The whole batch is checked first (see
    /// <see cref="TryParse(ReadOnlySpan{byte}, out LatestAckBatchReader)"/>).
    /// </summary>
    /// <param name="body">The body returned by <see cref="TryReadDatagram"/> or <see cref="TryReadStream"/>.</param>
    /// <param name="reader">A reader over the entries on success; an empty reader otherwise.</param>
    /// <returns><see cref="ControlParseStatus.Ok"/> or the first violation found.</returns>
    public static ControlParseStatus TryParse(ReadOnlySpan<byte> body, out LatestRejectBatchReader reader)
    {
        ControlParseStatus status = ValidateLatestBatch(body, withReason: true, out int count, out int entriesOffset);
        reader = status == ControlParseStatus.Ok ? new LatestRejectBatchReader(body.Slice(entriesOffset), count) : default;
        return status;
    }

    // One pass over the whole batch before anything is returned. Varints go through LatestBatchFormat.ReadVarInt,
    // which decodes the 1- and 2-byte forms (every minimal channel id, most keys) inline; see
    // docs/benchmarks/control.md for the V0 (general cursor) comparison.
    private static ControlParseStatus ValidateLatestBatch(ReadOnlySpan<byte> body, bool withReason, out int count, out int entriesOffset)
    {
        count = 0;
        entriesOffset = 0;
        int position = 0;
        ControlParseStatus status = LatestBatchFormat.ReadVarInt(body, ref position, out ulong declared);
        if (status != ControlParseStatus.Ok)
        {
            return status;
        }

        int minEntryLength = withReason ? LatestRejectMinEntryLength : LatestAckMinEntryLength;
        if (declared > (ulong)((body.Length - position) / minEntryLength))
        {
            return ControlParseStatus.CountTooLarge;
        }

        int offset = position;
        int tailLength = withReason ? 5 : 4; // version u32 [+ reason u8]
        for (int i = 0; i < (int)declared; i++)
        {
            if ((status = LatestBatchFormat.ReadVarInt(body, ref position, out ulong channel)) != ControlParseStatus.Ok)
            {
                return status;
            }

            if (channel - MinChannelId > MaxChannelId - MinChannelId)
            {
                return ControlParseStatus.InvalidChannel;
            }

            if ((status = LatestBatchFormat.ReadVarInt(body, ref position, out _)) != ControlParseStatus.Ok)
            {
                return status;
            }

            if (body.Length - position < tailLength)
            {
                return ControlParseStatus.Truncated;
            }

            if (withReason && !IsDefined((LatestRejectReason)body[position + 4]))
            {
                return ControlParseStatus.InvalidValue;
            }

            position += tailLength;
        }

        if (position != body.Length)
        {
            return ControlParseStatus.TrailingBytes;
        }

        count = (int)declared;
        entriesOffset = offset;
        return ControlParseStatus.Ok;
    }

    internal static bool IsDefined(LatestRejectReason reason) => (uint)(reason - LatestRejectReason.RingFull) < 4u;
}
