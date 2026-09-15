namespace Tedd.Quicly.Http3;

/// <summary>Outcome of one <see cref="Http3FrameReader.Read"/> call.</summary>
public enum Http3FrameReadStatus : byte
{
    /// <summary>All input was consumed without completing a frame or producing payload bytes; call again with more data.</summary>
    NeedMoreData = 0,
    /// <summary>A complete frame: the payload span holds the whole payload (zero-copy slice of the input).</summary>
    Frame,
    /// <summary>The payload span holds a part of the current frame's payload; more parts follow in later input.</summary>
    PayloadFragment,
    /// <summary>The payload span holds the final part of the current frame's payload; the frame is now complete.</summary>
    PayloadEnd,
    /// <summary>Protocol error: the frame length exceeds <see cref="Http3FrameReader.MaxPayloadLength"/> (H3_EXCESSIVE_LOAD / H3_FRAME_ERROR). The reader is stuck until <see cref="Http3FrameReader.Reset"/>.</summary>
    FrameTooLarge,
}

/// <summary>
/// Incremental, resumable HTTP/3 frame parser (RFC 9114 §7.1). Feed it arbitrary chunks of a stream; it yields
/// frame type, length and payload without copying. A frame wholly contained in one chunk (or whose payload is
/// wholly contained in one chunk after the header was seen earlier) is delivered as a single
/// <see cref="Http3FrameReadStatus.Frame"/>; otherwise the payload is delivered as fragments.
/// </summary>
/// <remarks>
/// The same (type, length, payload) shape is used by the capsule protocol (RFC 9297 §3.2), so this reader is also
/// used by <c>CapsuleReader</c>. This is a mutable struct: keep it in a field and never copy it between reads.
/// </remarks>
public struct Http3FrameReader
{
    private enum State : byte
    {
        TypeFirst = 0,
        TypeMore,
        LengthFirst,
        LengthMore,
        Payload,
        Failed,
    }

    private readonly ulong _maxPayloadLength;
    private ulong _type;
    private ulong _length;
    private ulong _remaining;
    private ulong _offset;
    private ulong _fragmentOffset;
    private ulong _acc;
    private int _need;
    private State _state;

    /// <summary>Creates a reader. A <paramref name="maxPayloadLength"/> of 0 disables the guard.</summary>
    public Http3FrameReader(ulong maxPayloadLength)
    {
        _maxPayloadLength = maxPayloadLength;
    }

    /// <summary>The payload length guard; 0 means unlimited.</summary>
    public readonly ulong MaxPayloadLength => _maxPayloadLength;

    /// <summary>Raw type of the current (or most recently completed) frame.</summary>
    public readonly ulong Type => _type;

    /// <summary>Type of the current (or most recently completed) frame as an <see cref="Http3FrameType"/>.</summary>
    public readonly Http3FrameType FrameType => (Http3FrameType)_type;

    /// <summary>Declared payload length of the current (or most recently completed) frame.</summary>
    public readonly ulong Length => _length;

    /// <summary>Offset, within the frame payload, of the payload span returned by the last <see cref="Read"/>.</summary>
    public readonly ulong PayloadOffset => _fragmentOffset;

    /// <summary>True while a frame header has been parsed and payload bytes are still outstanding.</summary>
    public readonly bool InPayload => _state == State.Payload;

    /// <summary>True after a protocol error was reported; <see cref="Read"/> keeps returning the error until <see cref="Reset"/>.</summary>
    public readonly bool HasFailed => _state == State.Failed;

    /// <summary>Discards any partial state and starts parsing at a frame boundary.</summary>
    public void Reset()
    {
        _type = 0;
        _length = 0;
        _remaining = 0;
        _offset = 0;
        _fragmentOffset = 0;
        _acc = 0;
        _need = 0;
        _state = State.TypeFirst;
    }

    /// <summary>
    /// Consumes bytes from <paramref name="input"/>. Call repeatedly with <c>input.Slice(consumed)</c> until the
    /// status is <see cref="Http3FrameReadStatus.NeedMoreData"/>.
    /// </summary>
    /// <param name="input">Next bytes of the stream.</param>
    /// <param name="consumed">Bytes consumed from <paramref name="input"/>.</param>
    /// <param name="payload">Payload bytes delivered by this call (a slice of <paramref name="input"/>), or empty.</param>
    public Http3FrameReadStatus Read(ReadOnlySpan<byte> input, out int consumed, out ReadOnlySpan<byte> payload)
    {
        consumed = 0;
        payload = default;

        if (_state == State.Failed)
        {
            return Http3FrameReadStatus.FrameTooLarge;
        }

        if (_state != State.Payload)
        {
            if (_state == State.TypeFirst
                && Http3VarInt.TryRead(input, out ulong t, out int n1)
                && Http3VarInt.TryRead(input.Slice(n1), out ulong l, out int n2))
            {
                // Fast path: the whole header is present in this chunk.
                _type = t;
                _length = l;
                consumed = n1 + n2;
            }
            else if (!ReadHeaderBytewise(input, ref consumed))
            {
                return Http3FrameReadStatus.NeedMoreData;
            }

            if (_maxPayloadLength != 0 && _length > _maxPayloadLength)
            {
                _state = State.Failed;
                return Http3FrameReadStatus.FrameTooLarge;
            }

            _remaining = _length;
            _offset = 0;
            _fragmentOffset = 0;
            _state = State.Payload;
        }

        if (_remaining == 0)
        {
            _state = State.TypeFirst;
            return Http3FrameReadStatus.Frame;
        }

        int available = input.Length - consumed;
        if (available == 0)
        {
            return Http3FrameReadStatus.NeedMoreData;
        }

        int take = (ulong)available < _remaining ? available : (int)_remaining;
        payload = input.Slice(consumed, take);
        consumed += take;
        _fragmentOffset = _offset;
        bool first = _offset == 0;
        _offset += (ulong)take;
        _remaining -= (ulong)take;

        if (_remaining == 0)
        {
            _state = State.TypeFirst;
            return first ? Http3FrameReadStatus.Frame : Http3FrameReadStatus.PayloadEnd;
        }
        return Http3FrameReadStatus.PayloadFragment;
    }

    /// <summary>Byte-wise header accumulation used when a header straddles chunk boundaries. Returns true when the header is complete.</summary>
    private bool ReadHeaderBytewise(ReadOnlySpan<byte> input, ref int consumed)
    {
        while (consumed < input.Length)
        {
            byte b = input[consumed++];
            switch (_state)
            {
                case State.TypeFirst:
                    _need = Http3VarInt.GetLengthFromFirstByte(b) - 1;
                    _acc = (ulong)(b & 0x3F);
                    if (_need == 0)
                    {
                        _type = _acc;
                        _state = State.LengthFirst;
                    }
                    else
                    {
                        _state = State.TypeMore;
                    }
                    break;
                case State.TypeMore:
                    _acc = (_acc << 8) | b;
                    if (--_need == 0)
                    {
                        _type = _acc;
                        _state = State.LengthFirst;
                    }
                    break;
                case State.LengthFirst:
                    _need = Http3VarInt.GetLengthFromFirstByte(b) - 1;
                    _acc = (ulong)(b & 0x3F);
                    if (_need == 0)
                    {
                        _length = _acc;
                        return true;
                    }
                    _state = State.LengthMore;
                    break;
                default: // State.LengthMore
                    _acc = (_acc << 8) | b;
                    if (--_need == 0)
                    {
                        _length = _acc;
                        return true;
                    }
                    break;
            }
        }
        return false;
    }

    /// <summary>
    /// Parses one complete frame from the start of a contiguous buffer. Returns false when the buffer does not
    /// hold a complete frame (header or payload truncated). No max-length guard is applied.
    /// </summary>
    public static bool TryReadFrame(ReadOnlySpan<byte> source, out ulong type, out ReadOnlySpan<byte> payload, out int consumed)
    {
        payload = default;
        consumed = 0;
        if (!Http3VarInt.TryRead(source, out type, out int n1)) return false;
        if (!Http3VarInt.TryRead(source.Slice(n1), out ulong length, out int n2)) return false;
        int header = n1 + n2;
        if (length > (ulong)(source.Length - header)) return false;
        payload = source.Slice(header, (int)length);
        consumed = header + (int)length;
        return true;
    }
}
