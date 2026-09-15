namespace Tedd.Quicly.Http3;

/// <summary>HTTP/3 frame types (RFC 9114 §7.2, draft-ietf-webtrans-http3).</summary>
public enum Http3FrameType : ulong
{
    /// <summary>DATA (0x00).</summary>
    Data = 0x00,
    /// <summary>HEADERS (0x01).</summary>
    Headers = 0x01,
    /// <summary>CANCEL_PUSH (0x03).</summary>
    CancelPush = 0x03,
    /// <summary>SETTINGS (0x04).</summary>
    Settings = 0x04,
    /// <summary>PUSH_PROMISE (0x05).</summary>
    PushPromise = 0x05,
    /// <summary>GOAWAY (0x07).</summary>
    GoAway = 0x07,
    /// <summary>MAX_PUSH_ID (0x0d).</summary>
    MaxPushId = 0x0d,
    /// <summary>WEBTRANSPORT_STREAM (0x41): signal value opening a WebTransport bidirectional stream.</summary>
    WebTransportStream = 0x41,
}

/// <summary>Helpers for <see cref="Http3FrameType"/> values, including reserved (grease) detection.</summary>
public static class Http3FrameTypeExtensions
{
    /// <summary>
    /// True when the type is a reserved frame type of the form <c>0x1f * N + 0x21</c> (RFC 9114 §7.2.8), which
    /// receivers must ignore.
    /// </summary>
    public static bool IsReservedGrease(this Http3FrameType type) => Http3Grease.IsReserved((ulong)type);

    /// <summary>
    /// True when the type is an HTTP/2 frame type that has no HTTP/3 equivalent (PRIORITY 0x02, PING 0x06,
    /// WINDOW_UPDATE 0x08 and CONTINUATION 0x09); receipt is a connection error of type
    /// <see cref="Http3ErrorCode.FrameUnexpected"/> (RFC 9114 §7.2.8 / §11.2.1).
    /// </summary>
    public static bool IsForbidden(this Http3FrameType type)
    {
        ulong t = (ulong)type;
        return t == 0x02 || t == 0x06 || t == 0x08 || t == 0x09;
    }

    /// <summary>True when the frame type is one this library knows how to interpret.</summary>
    public static bool IsKnown(this Http3FrameType type)
    {
        switch (type)
        {
            case Http3FrameType.Data:
            case Http3FrameType.Headers:
            case Http3FrameType.CancelPush:
            case Http3FrameType.Settings:
            case Http3FrameType.PushPromise:
            case Http3FrameType.GoAway:
            case Http3FrameType.MaxPushId:
            case Http3FrameType.WebTransportStream:
                return true;
            default:
                return false;
        }
    }
}

/// <summary>
/// Reserved ("grease") value helpers shared by frame types, stream types and setting identifiers
/// (RFC 9114 §6.2.3, §7.2.4.1 and §7.2.8).
/// </summary>
public static class Http3Grease
{
    /// <summary>True when <paramref name="value"/> has the form <c>0x1f * N + 0x21</c>.</summary>
    public static bool IsReserved(ulong value) => value >= 0x21 && (value - 0x21) % 0x1f == 0;

    /// <summary>Returns the N-th reserved value <c>0x1f * N + 0x21</c>.</summary>
    public static ulong Make(ulong n) => 0x1f * n + 0x21;
}
