using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text;
using System.Text.Unicode;
using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Core.Control;

/// <summary>
/// Encoder/decoder for the QUICLY control protocol (PROTOCOL.md §2.3 control datagrams, §3.4 control-stream
/// messages).
/// </summary>
/// <remarks>
/// <para>
/// Reading is two-step: <see cref="TryReadDatagram"/> or <see cref="TryReadStream"/> validates the framing and the
/// type byte and returns the body; the matching <c>TryParse</c> overload then decodes the body of that type.
/// Parsers never throw, never allocate and never read outside the input; they return a
/// <see cref="ControlParseStatus"/>. Every bound in PROTOCOL.md is enforced: frame length in [1, 16384], tokens
/// ≤ 4 096 bytes, reasons ≤ 512 bytes of valid UTF-8, channel ids in [2, 16383], minimal varints only, no trailing
/// bytes, entry counts bounded by the remaining length, the Hello magic and version. Byte fields (tokens, reasons,
/// the HelloAck table section) are returned as slices of the input.
/// </para>
/// <para>
/// Writers (<c>TryWrite</c>) return <see langword="false"/> only when the destination is too small (and then write
/// nothing meaningful). A message that violates a PROTOCOL.md bound (a token over 4 096 bytes, an invalid reason, a
/// channel id outside [2, 16383], a varint field above 2^62 − 1, a code above 32 bits, a stream frame over 16 384
/// bytes) is a caller bug and throws <see cref="ArgumentException"/>, so the library can never emit a frame its
/// own parser rejects.
/// </para>
/// </remarks>
public static partial class ControlCodec
{
    /// <summary>The protocol version this codec speaks (Hello <c>version</c> field).</summary>
    public const ushort ProtocolVersion = 1;

    /// <summary>Largest value of a control-stream <c>Length</c> field (type byte + body).</summary>
    public const int MaxFrameLength = 16384;

    /// <summary>Largest encoded control-stream message: a 4-byte <c>Length</c> varint plus <see cref="MaxFrameLength"/>.</summary>
    public const int MaxEncodedStreamFrameLength = 4 + MaxFrameLength;

    /// <summary>Largest session or auth token in a Hello/HelloAck.</summary>
    public const int MaxTokenLength = 4096;

    /// <summary>Largest reason string (UTF-8 bytes) in Close and HelloAck.</summary>
    public const int MaxReasonLength = 512;

    /// <summary>Largest channel name (UTF-8 bytes) in a HelloAck table section.</summary>
    public const int MaxChannelNameLength = 64;

    /// <summary>Smallest application channel id.</summary>
    public const ushort MinChannelId = 2;

    /// <summary>Largest application channel id.</summary>
    public const ushort MaxChannelId = 16383;

    /// <summary>The control stream's preamble byte (channel 0), sent once before the first framed message.</summary>
    public const byte StreamPreamble = 0x00;

    /// <summary>"QLCY" read as a little-endian 32-bit integer.</summary>
    internal const uint MagicValue = 0x59434C51;

    /// <summary>The Hello magic, <c>"QLCY"</c>.</summary>
    public static ReadOnlySpan<byte> Magic => "QLCY"u8;

    /// <summary>
    /// Validates a control datagram (<c>0x00</c>, <c>Type</c>, body = rest of the datagram) and returns its type and body.
    /// </summary>
    /// <param name="source">The complete datagram (or the complete inner message of a packed container).</param>
    /// <param name="type">The message type (one of 0x01–0x05) on success.</param>
    /// <param name="body">The body, a slice of <paramref name="source"/>, on success.</param>
    /// <param name="consumed"><paramref name="source"/>.Length on success, otherwise 0.</param>
    /// <returns>
    /// <see cref="ControlParseStatus.Ok"/>; <see cref="ControlParseStatus.NotControlChannel"/> when the datagram is not on
    /// channel 0 (<see cref="ControlParseStatus.NonMinimalVarint"/> when it is channel 0 encoded with more than one byte);
    /// <see cref="ControlParseStatus.TypeNotAllowed"/> for a stream-only type; <see cref="ControlParseStatus.UnknownType"/>;
    /// <see cref="ControlParseStatus.Truncated"/>.
    /// </returns>
    public static ControlParseStatus TryReadDatagram(ReadOnlySpan<byte> source, out ControlType type, out ReadOnlySpan<byte> body, out int consumed)
    {
        type = 0;
        body = default;
        consumed = 0;
        if (source.IsEmpty)
        {
            return ControlParseStatus.Truncated;
        }

        if (source[0] != 0)
        {
            // Channel 0 written with a longer varint (0x40 0x00, ...) is a protocol error, not another channel.
            return VarInt.TryRead(source, out ulong channel, out _) && channel == 0
                ? ControlParseStatus.NonMinimalVarint
                : ControlParseStatus.NotControlChannel;
        }

        if (source.Length < 2)
        {
            return ControlParseStatus.Truncated;
        }

        byte t = source[1];
        if (!IsDatagramType(t))
        {
            return IsStreamOnlyType(t) ? ControlParseStatus.TypeNotAllowed : ControlParseStatus.UnknownType;
        }

        type = (ControlType)t;
        body = source.Slice(2);
        consumed = source.Length;
        return ControlParseStatus.Ok;
    }

    /// <summary>
    /// Reads one framed control-stream message (<c>Length</c> varint in [1, 16384], <c>Type</c>, body) from the start of
    /// <paramref name="source"/>. The stream preamble (<see cref="StreamPreamble"/>) must already have been consumed.
    /// </summary>
    /// <param name="source">Buffered stream bytes; may hold a partial message or several messages.</param>
    /// <param name="type">The message type on success.</param>
    /// <param name="body">The body (<c>Length − 1</c> bytes), a slice of <paramref name="source"/>, on success.</param>
    /// <param name="consumed">Bytes of <paramref name="source"/> taken by this message on success, otherwise 0.</param>
    /// <returns>
    /// <see cref="ControlParseStatus.Ok"/>; <see cref="ControlParseStatus.NeedMoreData"/> when the message is not complete
    /// yet (never more than <see cref="MaxEncodedStreamFrameLength"/> bytes need buffering); or an error:
    /// <see cref="ControlParseStatus.NonMinimalVarint"/>, <see cref="ControlParseStatus.InvalidFrameLength"/>,
    /// <see cref="ControlParseStatus.UnknownType"/>. Errors are reported as soon as the offending bytes are present.
    /// </returns>
    public static ControlParseStatus TryReadStream(ReadOnlySpan<byte> source, out ControlType type, out ReadOnlySpan<byte> body, out int consumed)
    {
        type = 0;
        body = default;
        consumed = 0;
        if (source.IsEmpty)
        {
            return ControlParseStatus.NeedMoreData;
        }

        if (source[0] >= 0xC0)
        {
            return ControlParseStatus.InvalidFrameLength; // an 8-byte Length is non-minimal or above MaxFrameLength
        }

        if (source.Length < VarInt.PeekLength(source[0]))
        {
            return ControlParseStatus.NeedMoreData;
        }

        if (!VarInt.TryReadMinimal(source, out ulong length, out int prefix))
        {
            return ControlParseStatus.NonMinimalVarint;
        }

        if (length - 1 >= MaxFrameLength)
        {
            return ControlParseStatus.InvalidFrameLength;
        }

        if (source.Length == prefix)
        {
            return ControlParseStatus.NeedMoreData;
        }

        byte t = source[prefix];
        if (!IsDatagramType(t) && !IsStreamOnlyType(t))
        {
            return ControlParseStatus.UnknownType;
        }

        int total = prefix + (int)length;
        if (source.Length < total)
        {
            return ControlParseStatus.NeedMoreData;
        }

        type = (ControlType)t;
        body = source.Slice(prefix + 1, (int)length - 1);
        consumed = total;
        return ControlParseStatus.Ok;
    }

    /// <summary>Returns whether <paramref name="reason"/> satisfies PROTOCOL.md §6: at most 512 bytes of valid UTF-8.</summary>
    /// <param name="reason">Candidate reason bytes.</param>
    public static bool IsValidReason(ReadOnlySpan<byte> reason) => reason.Length <= MaxReasonLength && Utf8.IsValid(reason);

    /// <summary>
    /// Decodes a peer-supplied reason (or channel name) into <paramref name="destination"/> for logging: invalid UTF-8
    /// sequences and every control, format (including bidirectional overrides), line-separator and
    /// paragraph-separator character are replaced with U+FFFD. Allocation-free; output is truncated at a character
    /// boundary when <paramref name="destination"/> is too small. A destination of <c>reason.Length</c> chars always
    /// suffices.
    /// </summary>
    /// <param name="reason">Untrusted UTF-8 bytes (need not be valid).</param>
    /// <param name="destination">Receives the sanitised UTF-16 text.</param>
    /// <returns>The number of chars written.</returns>
    public static int CopySanitizedReason(ReadOnlySpan<byte> reason, Span<char> destination)
    {
        int written = 0;
        while (!reason.IsEmpty)
        {
            OperationStatus status = Rune.DecodeFromUtf8(reason, out Rune rune, out int bytes);
            if (status != OperationStatus.Done || IsUnsafeForLog(rune))
            {
                rune = Rune.ReplacementChar;
            }

            if (!rune.TryEncodeToUtf16(destination.Slice(written), out int chars))
            {
                break;
            }

            written += chars;
            reason = reason.Slice(Math.Max(bytes, 1));
        }

        return written;
    }

    private static bool IsUnsafeForLog(Rune rune)
    {
        UnicodeCategory category = Rune.GetUnicodeCategory(rune);
        return category is UnicodeCategory.Control or UnicodeCategory.Format
            or UnicodeCategory.LineSeparator or UnicodeCategory.ParagraphSeparator;
    }

    internal static bool IsDatagramType(byte type) => (uint)(type - 1) < 5u;

    internal static bool IsStreamOnlyType(byte type) => (uint)(type - 0x10) < 8u;

    /// <summary>
    /// Writes the frame header for a body of <paramref name="bodyLength"/> bytes and positions <paramref name="writer"/>
    /// at the body. Returns false (writing nothing) when the whole frame does not fit.
    /// </summary>
    private static bool TryBeginFrame(Span<byte> destination, ControlCarrier carrier, ControlType type, int bodyLength, out ControlWriter writer)
    {
        int frameLength = bodyLength + 1;
        int headerLength;
        if (carrier == ControlCarrier.Datagram)
        {
            headerLength = 2;
        }
        else if (carrier == ControlCarrier.Stream)
        {
            // Stream bodies are bounded by construction (Hello ≤ 8 220 bytes, Close ≤ 518, ...) or, for HelloAck whose
            // table section is caller-sized, checked by its writer before this point.
            System.Diagnostics.Debug.Assert(frameLength <= MaxFrameLength, "Stream control message over 16 384 bytes.");
            headerLength = VarInt.GetLength((ulong)frameLength) + 1;
        }
        else
        {
            throw new ArgumentOutOfRangeException(nameof(carrier), carrier, "Unknown control carrier.");
        }

        if (destination.Length - headerLength < bodyLength)
        {
            writer = default;
            return false;
        }

        writer = new ControlWriter(destination);
        if (carrier == ControlCarrier.Datagram)
        {
            writer.WriteByte(0);
        }
        else
        {
            writer.WriteVarInt((ulong)frameLength);
        }

        writer.WriteByte((byte)type);
        return true;
    }

    private static void ThrowIfInvalidChannel(ushort channel, string paramName)
    {
        if (channel < MinChannelId || channel > MaxChannelId)
        {
            throw new ArgumentOutOfRangeException(paramName, channel, "Channel ids are 2..16383.");
        }
    }

    private static void ThrowIfTokenTooLong(ReadOnlySpan<byte> token, string paramName)
    {
        if (token.Length > MaxTokenLength)
        {
            throw new ArgumentException($"Tokens are at most {MaxTokenLength} bytes (got {token.Length}).", paramName);
        }
    }

    private static void ThrowIfInvalidReason(ReadOnlySpan<byte> reason)
    {
        if (!IsValidReason(reason))
        {
            throw new ArgumentException($"A reason is at most {MaxReasonLength} bytes of valid UTF-8.", nameof(reason));
        }
    }

    private static uint CheckCode(QuiclyErrorCode code)
    {
        if ((ulong)code > uint.MaxValue)
        {
            throw new ArgumentOutOfRangeException(nameof(code), code, "Control-message error codes are 32-bit.");
        }

        return (uint)code;
    }

    [DoesNotReturn]
    private static void ThrowFrameTooLarge(long bodyLength) =>
        throw new ArgumentException($"A control-stream message body is at most {MaxFrameLength - 1} bytes (got {bodyLength}).", "message");
}
