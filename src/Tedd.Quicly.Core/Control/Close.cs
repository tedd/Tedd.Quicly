namespace Tedd.Quicly.Core.Control;

/// <summary>
/// Close (type 0x12, PROTOCOL.md §3.4). After sending or receiving Close every further QUICLY frame is ignored
/// and the QUIC connection is closed with the same code.
/// </summary>
/// <remarks>
/// A parsed instance's <see cref="Reason"/> is a slice of the received frame and is valid only as long as that
/// buffer is. Use <see cref="ControlCodec.CopySanitizedReason"/> before logging it.
/// </remarks>
public readonly ref struct Close
{
    /// <summary>Creates a Close message.</summary>
    /// <param name="code">Error code; at most <see cref="uint.MaxValue"/> (it travels as a 32-bit field).</param>
    /// <param name="reason">UTF-8 reason, at most <see cref="ControlCodec.MaxReasonLength"/> bytes; may be empty.</param>
    public Close(QuiclyErrorCode code, ReadOnlySpan<byte> reason)
    {
        Code = code;
        Reason = reason;
    }

    /// <summary>The error code (<see cref="QuiclyErrorCode.NoError"/> for an orderly close).</summary>
    public QuiclyErrorCode Code { get; }

    /// <summary>UTF-8 reason text (peer supplied when parsed: untrusted, may contain control characters).</summary>
    public ReadOnlySpan<byte> Reason { get; }
}
