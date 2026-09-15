namespace Tedd.Quicly.Http3.Qpack;

/// <summary>Outcome of <see cref="QpackDecoder.Decode"/>.</summary>
public enum QpackDecodeStatus : byte
{
    /// <summary>The field section was decoded.</summary>
    Ok = 0,
    /// <summary>The field section ended inside a field line or prefix.</summary>
    Truncated,
    /// <summary>A prefix integer overflowed the supported range.</summary>
    MalformedInteger,
    /// <summary>The Required Insert Count is not 0: the encoder used the dynamic table, which is disabled.</summary>
    RequiredInsertCountNotZero,
    /// <summary>The Base is not 0 (Sign bit set or Delta Base non-zero) while the dynamic table is disabled.</summary>
    InvalidBase,
    /// <summary>A field line references the dynamic table (indexed with T=0, or a post-base form).</summary>
    DynamicTableReference,
    /// <summary>A static-table index is out of range.</summary>
    InvalidStaticIndex,
    /// <summary>A Huffman-encoded string is invalid (EOS, bad padding).</summary>
    InvalidHuffman,
    /// <summary>The decoded field section exceeds the caller's maximum field section size.</summary>
    FieldSectionTooLarge,
    /// <summary>The <see cref="Http3HeaderCollection"/> buffer cannot hold the decoded names and values.</summary>
    InsufficientBuffer,
    /// <summary>The <see cref="Http3HeaderCollection"/> entry array is full.</summary>
    TooManyHeaders,
}

/// <summary>Helpers for <see cref="QpackDecodeStatus"/>.</summary>
public static class QpackDecodeStatusExtensions
{
    /// <summary>
    /// Maps a decode outcome to the HTTP/3 error code a receiver should raise: size and capacity failures map to
    /// <see cref="Http3ErrorCode.ExcessiveLoad"/>, every other failure to
    /// <see cref="Http3ErrorCode.QpackDecompressionFailed"/>, and <see cref="QpackDecodeStatus.Ok"/> to
    /// <see cref="Http3ErrorCode.NoError"/>.
    /// </summary>
    public static Http3ErrorCode ToErrorCode(this QpackDecodeStatus status)
    {
        switch (status)
        {
            case QpackDecodeStatus.Ok:
                return Http3ErrorCode.NoError;
            case QpackDecodeStatus.FieldSectionTooLarge:
            case QpackDecodeStatus.InsufficientBuffer:
            case QpackDecodeStatus.TooManyHeaders:
                return Http3ErrorCode.ExcessiveLoad;
            default:
                return Http3ErrorCode.QpackDecompressionFailed;
        }
    }
}

/// <summary>
/// Decodes a QPACK field section (RFC 9204 §4.5) whose encoder was given a dynamic table capacity of 0: only the
/// static table and literals are accepted. Any dynamic-table reference is a
/// <see cref="Http3ErrorCode.QpackDecompressionFailed"/> error. Nothing is allocated.
/// </summary>
public static class QpackDecoder
{
    /// <summary>
    /// Decodes <paramref name="fieldSection"/> into <paramref name="headers"/> (which is cleared first).
    /// </summary>
    /// <param name="fieldSection">The HEADERS frame payload.</param>
    /// <param name="headers">Receives the decoded fields.</param>
    /// <param name="maxFieldSectionSize">
    /// Upper bound on the decoded size (sum of name + value lengths + 32 per field, RFC 9114 §4.2.2); 0 = no limit.
    /// </param>
    public static QpackDecodeStatus Decode(ReadOnlySpan<byte> fieldSection, Http3HeaderCollection headers, int maxFieldSectionSize = 0)
    {
        ArgumentNullException.ThrowIfNull(headers);
        headers.Clear();
        ReadOnlySpan<byte> src = fieldSection;

        // Encoded Field Section Prefix: Required Insert Count (8-bit prefix), S bit + Delta Base (7-bit prefix).
        QpackIntegerStatus st = QpackInteger.TryRead(src, 8, out ulong requiredInsertCount, out int n);
        if (st != QpackIntegerStatus.Ok) return Map(st);
        if (requiredInsertCount != 0) return QpackDecodeStatus.RequiredInsertCountNotZero;
        src = src.Slice(n);
        if (src.IsEmpty) return QpackDecodeStatus.Truncated;
        bool sign = (src[0] & 0x80) != 0;
        st = QpackInteger.TryRead(src, 7, out ulong deltaBase, out n);
        if (st != QpackIntegerStatus.Ok) return Map(st);
        if (sign || deltaBase != 0) return QpackDecodeStatus.InvalidBase;
        src = src.Slice(n);

        while (!src.IsEmpty)
        {
            byte b = src[0];
            Span<byte> free = headers.FreeSpace;
            int nameLength;
            int valueLength;

            if ((b & 0x80) != 0)
            {
                // 1 T index(6): Indexed Field Line.
                if ((b & 0x40) == 0) return QpackDecodeStatus.DynamicTableReference;
                st = QpackInteger.TryRead(src, 6, out ulong index, out n);
                if (st != QpackIntegerStatus.Ok) return Map(st);
                src = src.Slice(n);
                if (index >= QpackStaticTable.Count) return QpackDecodeStatus.InvalidStaticIndex;
                ReadOnlySpan<byte> name = QpackStaticTable.GetName((int)index);
                ReadOnlySpan<byte> value = QpackStaticTable.GetValue((int)index);
                if (free.Length < name.Length + value.Length) return QpackDecodeStatus.InsufficientBuffer;
                name.CopyTo(free);
                value.CopyTo(free.Slice(name.Length));
                nameLength = name.Length;
                valueLength = value.Length;
            }
            else if ((b & 0x40) != 0)
            {
                // 01 N T index(4): Literal Field Line with Name Reference.
                if ((b & 0x10) == 0) return QpackDecodeStatus.DynamicTableReference;
                st = QpackInteger.TryRead(src, 4, out ulong index, out n);
                if (st != QpackIntegerStatus.Ok) return Map(st);
                src = src.Slice(n);
                if (index >= QpackStaticTable.Count) return QpackDecodeStatus.InvalidStaticIndex;
                ReadOnlySpan<byte> name = QpackStaticTable.GetName((int)index);
                if (free.Length < name.Length) return QpackDecodeStatus.InsufficientBuffer;
                name.CopyTo(free);
                nameLength = name.Length;
                QpackDecodeStatus vs = ReadString(ref src, 7, free.Slice(nameLength), out valueLength);
                if (vs != QpackDecodeStatus.Ok) return vs;
            }
            else if ((b & 0x20) != 0)
            {
                // 001 N H length(3): Literal Field Line with Literal Name.
                QpackDecodeStatus ns = ReadString(ref src, 3, free, out nameLength);
                if (ns != QpackDecodeStatus.Ok) return ns;
                QpackDecodeStatus vs = ReadString(ref src, 7, free.Slice(nameLength), out valueLength);
                if (vs != QpackDecodeStatus.Ok) return vs;
            }
            else
            {
                // 0001 index(4): Indexed Field Line with Post-Base Index.
                // 0000 N index(3): Literal Field Line with Post-Base Name Reference.
                return QpackDecodeStatus.DynamicTableReference;
            }

            if (maxFieldSectionSize > 0 && (long)headers.FieldSectionSize + nameLength + valueLength + Http3HeaderCollection.FieldOverhead > maxFieldSectionSize)
            {
                return QpackDecodeStatus.FieldSectionTooLarge;
            }
            if (!headers.TryCommit(nameLength, valueLength)) return QpackDecodeStatus.TooManyHeaders;
        }
        return QpackDecodeStatus.Ok;
    }

    /// <summary>Reads a string literal (H bit at bit <paramref name="prefixBits"/>, N-bit prefix length, bytes) into <paramref name="destination"/>.</summary>
    private static QpackDecodeStatus ReadString(ref ReadOnlySpan<byte> src, int prefixBits, Span<byte> destination, out int written)
    {
        written = 0;
        if (src.IsEmpty) return QpackDecodeStatus.Truncated;
        bool huffman = (src[0] & (1 << prefixBits)) != 0;
        QpackIntegerStatus st = QpackInteger.TryRead(src, prefixBits, out ulong length, out int n);
        if (st != QpackIntegerStatus.Ok) return Map(st);
        src = src.Slice(n);
        if (length > (ulong)src.Length) return QpackDecodeStatus.Truncated;
        int len = (int)length;
        if (huffman)
        {
            int decoded = HpackHuffman.Decode(src.Slice(0, len), destination);
            if (decoded == HpackHuffman.InvalidInput) return QpackDecodeStatus.InvalidHuffman;
            if (decoded == HpackHuffman.DestinationTooSmall) return QpackDecodeStatus.InsufficientBuffer;
            written = decoded;
        }
        else
        {
            if (len > destination.Length) return QpackDecodeStatus.InsufficientBuffer;
            src.Slice(0, len).CopyTo(destination);
            written = len;
        }
        src = src.Slice(len);
        return QpackDecodeStatus.Ok;
    }

    private static QpackDecodeStatus Map(QpackIntegerStatus status) =>
        status == QpackIntegerStatus.Truncated ? QpackDecodeStatus.Truncated : QpackDecodeStatus.MalformedInteger;
}
