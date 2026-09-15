namespace Tedd.Quicly.Http3;

/// <summary>Unidirectional stream types (RFC 9114 §6.2, RFC 9204 §4.2, draft-ietf-webtrans-http3).</summary>
public enum Http3StreamType : ulong
{
    /// <summary>Control stream (0x00).</summary>
    Control = 0x00,
    /// <summary>Push stream (0x01).</summary>
    Push = 0x01,
    /// <summary>QPACK encoder stream (0x02).</summary>
    QpackEncoder = 0x02,
    /// <summary>QPACK decoder stream (0x03).</summary>
    QpackDecoder = 0x03,
    /// <summary>WebTransport unidirectional stream (0x54).</summary>
    WebTransport = 0x54,
}

/// <summary>Helpers for <see cref="Http3StreamType"/>.</summary>
public static class Http3StreamTypeExtensions
{
    /// <summary>True when the stream type is a reserved (grease) value receivers must ignore (RFC 9114 §6.2.3).</summary>
    public static bool IsReservedGrease(this Http3StreamType type) => Http3Grease.IsReserved((ulong)type);

    /// <summary>True when the stream type is one this library knows how to interpret.</summary>
    public static bool IsKnown(this Http3StreamType type)
    {
        switch (type)
        {
            case Http3StreamType.Control:
            case Http3StreamType.Push:
            case Http3StreamType.QpackEncoder:
            case Http3StreamType.QpackDecoder:
            case Http3StreamType.WebTransport:
                return true;
            default:
                return false;
        }
    }
}
