namespace Tedd.Quicly.Http3;

/// <summary>Outcome of <see cref="Http3Settings.Decode"/>.</summary>
public enum Http3SettingsDecodeStatus : byte
{
    /// <summary>The payload was well formed; unknown identifiers were ignored.</summary>
    Ok = 0,
    /// <summary>A varint was truncated or malformed (H3_FRAME_ERROR).</summary>
    Malformed,
    /// <summary>A known identifier occurred more than once (H3_SETTINGS_ERROR).</summary>
    DuplicateSetting,
    /// <summary>A reserved HTTP/2 setting identifier (0x00, 0x02, 0x03, 0x04, 0x05) was present (H3_SETTINGS_ERROR).</summary>
    ReservedSetting,
    /// <summary>A boolean-valued setting (ENABLE_CONNECT_PROTOCOL, H3_DATAGRAM, ENABLE_WEBTRANSPORT) had a value other than 0 or 1 (H3_SETTINGS_ERROR).</summary>
    InvalidValue,
}

/// <summary>
/// The known HTTP/3 settings (RFC 9114 §7.2.4). Each setting is optional (<c>null</c> = absent). Unknown identifiers
/// are ignored on decode and never emitted on encode.
/// </summary>
public struct Http3Settings : IEquatable<Http3Settings>
{
    /// <summary>SETTINGS_QPACK_MAX_TABLE_CAPACITY.</summary>
    public ulong? QpackMaxTableCapacity;
    /// <summary>SETTINGS_MAX_FIELD_SECTION_SIZE.</summary>
    public ulong? MaxFieldSectionSize;
    /// <summary>SETTINGS_QPACK_BLOCKED_STREAMS.</summary>
    public ulong? QpackBlockedStreams;
    /// <summary>SETTINGS_ENABLE_CONNECT_PROTOCOL (0 or 1).</summary>
    public ulong? EnableConnectProtocol;
    /// <summary>SETTINGS_H3_DATAGRAM (0 or 1).</summary>
    public ulong? H3Datagram;
    /// <summary>SETTINGS_ENABLE_WEBTRANSPORT (0 or 1).</summary>
    public ulong? EnableWebTransport;
    /// <summary>SETTINGS_WEBTRANSPORT_MAX_SESSIONS.</summary>
    public ulong? WebTransportMaxSessions;

    /// <summary>
    /// The settings a QUICLY WebTransport server advertises (docs/PROTOCOL.md §5): no dynamic QPACK table,
    /// Extended CONNECT, HTTP Datagrams and WebTransport enabled with <paramref name="maxSessions"/> sessions.
    /// </summary>
    public static Http3Settings CreateWebTransportServerDefaults(ulong maxSessions) => new()
    {
        QpackMaxTableCapacity = 0,
        QpackBlockedStreams = 0,
        EnableConnectProtocol = 1,
        H3Datagram = 1,
        EnableWebTransport = 1,
        WebTransportMaxSessions = maxSessions,
    };

    /// <summary>Encoded size of the SETTINGS frame payload.</summary>
    public readonly int GetPayloadLength()
    {
        int n = 0;
        n += PairLength(Http3SettingId.QpackMaxTableCapacity, QpackMaxTableCapacity);
        n += PairLength(Http3SettingId.MaxFieldSectionSize, MaxFieldSectionSize);
        n += PairLength(Http3SettingId.QpackBlockedStreams, QpackBlockedStreams);
        n += PairLength(Http3SettingId.EnableConnectProtocol, EnableConnectProtocol);
        n += PairLength(Http3SettingId.H3Datagram, H3Datagram);
        n += PairLength(Http3SettingId.EnableWebTransport, EnableWebTransport);
        n += PairLength(Http3SettingId.WebTransportMaxSessions, WebTransportMaxSessions);
        return n;
    }

    /// <summary>Writes the SETTINGS frame payload (identifier/value pairs). Returns bytes written or -1 when the destination is too small.</summary>
    public readonly int WritePayload(Span<byte> destination)
    {
        int pos = 0;
        if (!WritePair(destination, ref pos, Http3SettingId.QpackMaxTableCapacity, QpackMaxTableCapacity)) return -1;
        if (!WritePair(destination, ref pos, Http3SettingId.MaxFieldSectionSize, MaxFieldSectionSize)) return -1;
        if (!WritePair(destination, ref pos, Http3SettingId.QpackBlockedStreams, QpackBlockedStreams)) return -1;
        if (!WritePair(destination, ref pos, Http3SettingId.EnableConnectProtocol, EnableConnectProtocol)) return -1;
        if (!WritePair(destination, ref pos, Http3SettingId.H3Datagram, H3Datagram)) return -1;
        if (!WritePair(destination, ref pos, Http3SettingId.EnableWebTransport, EnableWebTransport)) return -1;
        if (!WritePair(destination, ref pos, Http3SettingId.WebTransportMaxSessions, WebTransportMaxSessions)) return -1;
        return pos;
    }

    /// <summary>Writes a complete SETTINGS frame (header + payload). Returns bytes written or -1 when the destination is too small.</summary>
    public readonly int WriteFrame(Span<byte> destination)
    {
        int payloadLength = GetPayloadLength();
        int h = Http3FrameWriter.WriteHeader(destination, Http3FrameType.Settings, (ulong)payloadLength);
        if (h < 0) return -1;
        int p = WritePayload(destination.Slice(h));
        if (p < 0) return -1;
        return h + p;
    }

    /// <summary>Encoded size of a complete SETTINGS frame (header + payload).</summary>
    public readonly int GetFrameLength()
    {
        int payloadLength = GetPayloadLength();
        return Http3FrameWriter.GetHeaderLength(Http3FrameType.Settings, (ulong)payloadLength) + payloadLength;
    }

    /// <summary>Decodes a SETTINGS frame payload. Unknown identifiers (including grease values) are ignored.</summary>
    public static Http3SettingsDecodeStatus Decode(ReadOnlySpan<byte> payload, out Http3Settings settings)
    {
        settings = default;
        while (!payload.IsEmpty)
        {
            if (!Http3VarInt.TryRead(payload, out ulong id, out int n1)) return Http3SettingsDecodeStatus.Malformed;
            if (!Http3VarInt.TryRead(payload.Slice(n1), out ulong value, out int n2)) return Http3SettingsDecodeStatus.Malformed;
            payload = payload.Slice(n1 + n2);

            switch (id)
            {
                case 0x00:
                case 0x02:
                case 0x03:
                case 0x04:
                case 0x05:
                    return Http3SettingsDecodeStatus.ReservedSetting;
                case (ulong)Http3SettingId.QpackMaxTableCapacity:
                    if (settings.QpackMaxTableCapacity.HasValue) return Http3SettingsDecodeStatus.DuplicateSetting;
                    settings.QpackMaxTableCapacity = value;
                    break;
                case (ulong)Http3SettingId.MaxFieldSectionSize:
                    if (settings.MaxFieldSectionSize.HasValue) return Http3SettingsDecodeStatus.DuplicateSetting;
                    settings.MaxFieldSectionSize = value;
                    break;
                case (ulong)Http3SettingId.QpackBlockedStreams:
                    if (settings.QpackBlockedStreams.HasValue) return Http3SettingsDecodeStatus.DuplicateSetting;
                    settings.QpackBlockedStreams = value;
                    break;
                case (ulong)Http3SettingId.EnableConnectProtocol:
                    if (settings.EnableConnectProtocol.HasValue) return Http3SettingsDecodeStatus.DuplicateSetting;
                    if (value > 1) return Http3SettingsDecodeStatus.InvalidValue;
                    settings.EnableConnectProtocol = value;
                    break;
                case (ulong)Http3SettingId.H3Datagram:
                    if (settings.H3Datagram.HasValue) return Http3SettingsDecodeStatus.DuplicateSetting;
                    if (value > 1) return Http3SettingsDecodeStatus.InvalidValue;
                    settings.H3Datagram = value;
                    break;
                case (ulong)Http3SettingId.EnableWebTransport:
                    if (settings.EnableWebTransport.HasValue) return Http3SettingsDecodeStatus.DuplicateSetting;
                    if (value > 1) return Http3SettingsDecodeStatus.InvalidValue;
                    settings.EnableWebTransport = value;
                    break;
                case (ulong)Http3SettingId.WebTransportMaxSessions:
                    if (settings.WebTransportMaxSessions.HasValue) return Http3SettingsDecodeStatus.DuplicateSetting;
                    settings.WebTransportMaxSessions = value;
                    break;
                default:
                    // Unknown or reserved-grease identifier: ignore (RFC 9114 §7.2.4.1).
                    break;
            }
        }
        return Http3SettingsDecodeStatus.Ok;
    }

    private static int PairLength(Http3SettingId id, ulong? value)
    {
        if (!value.HasValue) return 0;
        return Http3VarInt.GetLength((ulong)id) + Http3VarInt.GetLength(value.GetValueOrDefault());
    }

    private static bool WritePair(Span<byte> destination, ref int pos, Http3SettingId id, ulong? value)
    {
        if (!value.HasValue) return true;
        int a = Http3VarInt.Write(destination.Slice(pos), (ulong)id);
        if (a < 0) return false;
        int b = Http3VarInt.Write(destination.Slice(pos + a), value.GetValueOrDefault());
        if (b < 0) return false;
        pos += a + b;
        return true;
    }

    /// <inheritdoc />
    public readonly bool Equals(Http3Settings other) =>
        QpackMaxTableCapacity == other.QpackMaxTableCapacity
        && MaxFieldSectionSize == other.MaxFieldSectionSize
        && QpackBlockedStreams == other.QpackBlockedStreams
        && EnableConnectProtocol == other.EnableConnectProtocol
        && H3Datagram == other.H3Datagram
        && EnableWebTransport == other.EnableWebTransport
        && WebTransportMaxSessions == other.WebTransportMaxSessions;

    /// <inheritdoc />
    public override readonly bool Equals(object? obj) => obj is Http3Settings other && Equals(other);

    /// <inheritdoc />
    public override readonly int GetHashCode() => HashCode.Combine(QpackMaxTableCapacity, MaxFieldSectionSize, QpackBlockedStreams, EnableConnectProtocol, H3Datagram, EnableWebTransport, WebTransportMaxSessions);

    /// <summary>Value equality.</summary>
    public static bool operator ==(Http3Settings left, Http3Settings right) => left.Equals(right);

    /// <summary>Value inequality.</summary>
    public static bool operator !=(Http3Settings left, Http3Settings right) => !left.Equals(right);
}
