using System.Runtime.CompilerServices;
using Tedd.Quicly.Core.Channels;

namespace Tedd.Quicly.Core.Framing;

/// <summary>What a stream carries; known up front for the control stream, decided by the preamble otherwise.</summary>
public enum StreamRole : byte
{
    /// <summary>A unidirectional stream whose preamble has not been parsed yet (any stream-capable channel is accepted).</summary>
    Unknown = 0,

    /// <summary>Persistent ReliableOrdered stream (PROTOCOL.md §3.1).</summary>
    Ordered = 1,

    /// <summary>Group stream of a ReliableUnordered channel or a large ReliableLatest value (§3.2).</summary>
    Group = 2,

    /// <summary>Bulk transfer stream (§3.3).</summary>
    Bulk = 3,

    /// <summary>The bidirectional control stream (§3.4).</summary>
    Control = 4,
}

/// <summary>Events produced by <see cref="StreamFrameParser.Read"/>.</summary>
public enum StreamEvent : byte
{
    /// <summary>All input was consumed (a partial header may be buffered); call again with more bytes.</summary>
    NeedMore = 0,

    /// <summary>The preamble was parsed: <see cref="StreamFrameParser.Channel"/>, <see cref="StreamFrameParser.GroupId"/>, <see cref="StreamFrameParser.Role"/>.</summary>
    Preamble,

    /// <summary>A message (control frame, bulk body or bulk chunk) begins: <see cref="StreamFrameParser.Message"/>.</summary>
    MessageStart,

    /// <summary>Payload bytes of the current message: a slice of the input (never copied).</summary>
    PayloadChunk,

    /// <summary>The current message's payload is complete.</summary>
    MessageEnd,

    /// <summary>A bulk header was parsed and validated: <see cref="StreamFrameParser.Bulk"/>.</summary>
    BulkHeader,

    /// <summary>The stream is malformed: <see cref="StreamFrameParser.Error"/>. Sticky until reset.</summary>
    Error,
}

/// <summary>
/// Incremental, resumable, zero-copy parser for one QUIC stream (PROTOCOL.md §3). Feed it the stream's bytes in any
/// segmentation; it yields events and hands out payload as slices of the input. A header that straddles input
/// segments is kept in an internal 32-byte buffer (the largest header piece is 32 bytes); payload is never buffered.
/// </summary>
/// <remarks>
/// <para>Event sequences:
/// ordered/group — <c>Preamble, (MessageStart, PayloadChunk*, MessageEnd)*</c>;
/// control — <c>Preamble(0), (MessageStart[ControlType], PayloadChunk*, MessageEnd)*</c> with the body as payload;
/// bulk — <c>Preamble, BulkHeader</c>, then the body as one message (unchunked) or one message per chunk
/// (<see cref="StreamMessageHeader.Length"/> = ChunkLength, <see cref="StreamMessageHeader.RawLength"/> = RawLength).</para>
/// <para>This is a mutable struct: keep it in a field or array element and never copy it between calls. Allocation-free;
/// never throws on malformed input. It holds only scalars (no managed references), so per-stream parsers can live in
/// reference-free native or pinned struct arrays (ADR 0008 invariant 12); the channel table is passed to
/// <see cref="Read"/> for the preamble lookup, and the stream's channel is resolved by the caller from
/// <see cref="Channel"/>.</para>
/// </remarks>
public struct StreamFrameParser
{
    /// <summary>Size of the internal partial-header buffer.</summary>
    public const int HeaderBufferSize = 32;

    private const StreamEvent Continue = (StreamEvent)0xFF;

    private BulkHeader _bulk;
    private StreamMessageHeader _message;
    private ulong _groupId;
    private ulong _bulkDecoded;
    private int _remaining;
    private int _maxMessageSize;
    private int _limit;
    private int _bulkMaxChunk;
    private ushort _channel;
    private byte _shape;
    private byte _bufferLength;
    private byte _controlType;
    private bool _latest;
    private State _state;
    private StreamRole _role;
    private StreamRole _expected;
    private ParseStatus _error;
    private HeaderBuffer _buffer;

    private enum State : byte
    {
        Preamble = 0,
        Header,
        BulkIdentity,
        BulkRange,
        BulkHash,
        BodyStart,
        Payload,
        MessageEnd,
        Done,
        Failed,
    }

    /// <summary>The stream's role (<see cref="StreamRole.Unknown"/> until the preamble of a unidirectional stream is parsed).</summary>
    public readonly StreamRole Role => _role;

    /// <summary>The preamble's channel id.</summary>
    public readonly ushort Channel => _channel;

    /// <summary>The group id of a group stream (for ReliableLatest: the value's version).</summary>
    public readonly ulong GroupId => _groupId;

    /// <summary>Header of the current (or most recent) message.</summary>
    public readonly StreamMessageHeader Message => _message;

    /// <summary>Type byte of the current (or most recent) control frame.</summary>
    public readonly byte ControlType => _controlType;

    /// <summary>The bulk header (after <see cref="StreamEvent.BulkHeader"/>).</summary>
    public readonly BulkHeader Bulk => _bulk;

    /// <summary>Payload bytes of the current message not yet handed out.</summary>
    public readonly int RemainingPayload => _remaining;

    /// <summary>The reason of the last <see cref="StreamEvent.Error"/>, or <see cref="ParseStatus.Ok"/>.</summary>
    public readonly ParseStatus Error => _error;

    /// <summary>
    /// Effective maximum message size of the stream's channel (after the preamble): <c>min(channel.MaxMessageSize,
    /// session cap)</c> for message streams, the channel's own <c>MaxMessageSize</c> (largest transfer <c>Length</c>) for
    /// Bulk streams, 16 383 (largest control body) for the control stream.
    /// </summary>
    public readonly int MaxMessageSize => _limit;

    /// <summary>
    /// The state a message event changes (<see cref="StreamEvent.MessageStart"/>, <see cref="StreamEvent.PayloadChunk"/>,
    /// <see cref="StreamEvent.MessageEnd"/>). Take it with <see cref="GetMark"/> before <see cref="Read"/>; <see cref="Rewind"/>
    /// then un-reads the event (the caller hands the input back from the offset it had before the call), at a fraction of the
    /// cost of copying the whole parser. It does not cover the preamble or the header of a bulk stream: copy the parser to
    /// un-read those.
    /// </summary>
    public readonly struct Mark
    {
        internal readonly StreamMessageHeader Message;
        internal readonly ulong BulkDecoded;
        internal readonly int Remaining;
        internal readonly byte State;
        internal readonly byte BufferLength;
        internal readonly byte ControlType;

        internal Mark(in StreamMessageHeader message, ulong bulkDecoded, int remaining, byte state, byte bufferLength, byte controlType)
        {
            Message = message;
            BulkDecoded = bulkDecoded;
            Remaining = remaining;
            State = state;
            BufferLength = bufferLength;
            ControlType = controlType;
        }
    }

    /// <summary>The mark of the current state (see <see cref="Mark"/>).</summary>
    /// <returns>The mark.</returns>
    public readonly Mark GetMark() => new(in _message, _bulkDecoded, _remaining, (byte)_state, _bufferLength, _controlType);

    /// <summary>
    /// Restores a <see cref="Mark"/> taken just before the last <see cref="Read"/>, whose event was a message event: the event is
    /// un-read. Bytes of a header that straddled segments stay in the internal buffer, so the caller hands back only the bytes
    /// of the current input from the offset it had before that call.
    /// </summary>
    /// <param name="mark">The mark.</param>
    public void Rewind(in Mark mark)
    {
        _message = mark.Message;
        _bulkDecoded = mark.BulkDecoded;
        _remaining = mark.Remaining;
        _state = (State)mark.State;
        _bufferLength = mark.BufferLength;
        _controlType = mark.ControlType;
    }

    /// <summary>Prepares the parser for a new stream.</summary>
    /// <param name="role">
    /// <see cref="StreamRole.Control"/> for the control stream (preamble must be 0); <see cref="StreamRole.Unknown"/>
    /// for a unidirectional stream of any stream-capable channel; <see cref="StreamRole.Ordered"/>,
    /// <see cref="StreamRole.Group"/> or <see cref="StreamRole.Bulk"/> to additionally require that kind
    /// (<see cref="ParseStatus.RoleMismatch"/> otherwise).
    /// </param>
    /// <param name="maxMessageSize">
    /// Session cap (<c>HelloAck.maxMessageSize</c>) for ordered and group message frames; 0 = only each channel's limit.
    /// It does not apply to Bulk transfers, whose <c>Length</c> is bounded by the Bulk channel's own
    /// <c>MaxMessageSize</c> (PROTOCOL.md §8).
    /// </param>
    /// <param name="bulkMaxChunk">Largest bulk chunk length and chunk raw length.</param>
    /// <exception cref="ArgumentOutOfRangeException">An argument is out of range.</exception>
    public void Reset(StreamRole role = StreamRole.Unknown, int maxMessageSize = 0, int bulkMaxChunk = StreamFraming.DefaultBulkMaxChunk)
    {
        if ((byte)role > (byte)StreamRole.Control)
        {
            throw new ArgumentOutOfRangeException(nameof(role), role, "Undefined stream role.");
        }

        if (maxMessageSize < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(maxMessageSize), maxMessageSize, "Must be 0 (no cap) or positive.");
        }

        if (bulkMaxChunk < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(bulkMaxChunk), bulkMaxChunk, "Must be positive.");
        }

        this = default;
        _expected = role;
        _maxMessageSize = maxMessageSize;
        _bulkMaxChunk = bulkMaxChunk;
    }

    /// <summary>
    /// Consumes bytes from <paramref name="input"/> (advancing it) and returns the next event. Call repeatedly until
    /// it returns <see cref="StreamEvent.NeedMore"/> (then <paramref name="input"/> is empty) or
    /// <see cref="StreamEvent.Error"/>.
    /// </summary>
    /// <param name="table">
    /// The session's channel table, consulted only to resolve the preamble of a unidirectional stream (may be null for
    /// the control stream; a null table rejects every other preamble as <see cref="ParseStatus.UnknownChannel"/>).
    /// </param>
    /// <param name="input">The unconsumed bytes of the current receive segment; advanced past consumed bytes.</param>
    /// <param name="payload">For <see cref="StreamEvent.PayloadChunk"/>: the payload slice of the input; otherwise empty.</param>
    /// <returns>The event.</returns>
    public StreamEvent Read(ChannelTable? table, scoped ref ReadOnlySpan<byte> input, out ReadOnlySpan<byte> payload)
    {
        payload = default;
        while (true)
        {
            switch (_state)
            {
                case State.Payload:
                {
                    if (input.IsEmpty)
                    {
                        return StreamEvent.NeedMore;
                    }

                    int n = input.Length < _remaining ? input.Length : _remaining;
                    payload = input.Slice(0, n);
                    input = input.Slice(n);
                    _remaining -= n;
                    if (_remaining == 0)
                    {
                        _state = State.MessageEnd;
                    }

                    return StreamEvent.PayloadChunk;
                }

                case State.MessageEnd:
                    _state = StateAfterMessage();
                    return StreamEvent.MessageEnd;

                case State.BodyStart:
                    _message = new StreamMessageHeader { Length = (int)_bulk.Length };
                    _remaining = _message.Length;
                    _state = State.Payload;
                    return StreamEvent.MessageStart;

                case State.Done:
                    if (input.IsEmpty)
                    {
                        return StreamEvent.NeedMore;
                    }

                    return Fail(_latest ? ParseStatus.TooManyMessages : ParseStatus.BadLength);

                case State.Failed:
                    return StreamEvent.Error;

                default:
                {
                    if (input.IsEmpty)
                    {
                        return StreamEvent.NeedMore;
                    }

                    StreamEvent result = ParseNextHeader(table, ref input);
                    if (result != Continue)
                    {
                        return result;
                    }

                    break;
                }
            }
        }
    }

    /// <summary>
    /// Checks that the stream may end here (its FIN arrived). Call once all received bytes were fed to <see cref="Read"/>;
    /// a message whose last payload byte was handed out counts as complete even before <see cref="StreamEvent.MessageEnd"/>
    /// was returned. Ordered, unordered-group and control streams must be at a message boundary
    /// after the preamble; a ReliableLatest group stream must have delivered its one message; a bulk stream its whole body.
    /// </summary>
    /// <returns><see cref="ParseStatus.Ok"/>, <see cref="ParseStatus.Truncated"/>, or the sticky error.</returns>
    public readonly ParseStatus Finish()
    {
        switch (_state)
        {
            case State.Failed:
                return _error;
            case State.Done:
                return ParseStatus.Ok;
            case State.Header when _bufferLength == 0 && !_latest && _role != StreamRole.Bulk:
                return ParseStatus.Ok;
            case State.MessageEnd:
            {
                // The last payload byte was handed out; only the MessageEnd event is pending.
                State next = StateAfterMessage();
                return next == State.Done || (next == State.Header && !_latest && _role != StreamRole.Bulk)
                    ? ParseStatus.Ok
                    : ParseStatus.Truncated;
            }
            default:
                return ParseStatus.Truncated;
        }
    }

    private StreamEvent ParseNextHeader(ChannelTable? table, ref ReadOnlySpan<byte> input)
    {
        ParseStatus status;
        int consumed;
        if (_bufferLength == 0)
        {
            status = ParseHeader(table, input, out consumed);
            if (status != ParseStatus.Truncated)
            {
                goto Parsed;
            }
        }

        // Slow path: the header straddles segments. Top the buffer up and parse from it.
        Span<byte> buffer = _buffer;
        int take = Math.Min(HeaderBufferSize - _bufferLength, input.Length);
        input.Slice(0, take).CopyTo(buffer.Slice(_bufferLength));
        int available = _bufferLength + take;
        status = ParseHeader(table, buffer.Slice(0, available), out consumed);
        if (status == ParseStatus.Truncated)
        {
            if (available == HeaderBufferSize)
            {
                // Every header piece is at most 32 bytes, so a full buffer always parses; defensive only.
                return Fail(ParseStatus.BadLength);
            }

            _bufferLength = (byte)available;
            input = input.Slice(take);
            return StreamEvent.NeedMore;
        }

        consumed -= _bufferLength;
        _bufferLength = 0;

    Parsed:
        if (status != ParseStatus.Ok)
        {
            return Fail(status);
        }

        input = input.Slice(consumed);
        return OnHeaderParsed();
    }

    private ParseStatus ParseHeader(ChannelTable? table, ReadOnlySpan<byte> source, out int consumed)
    {
        switch (_state)
        {
            case State.Preamble:
                return ParsePreamble(table, source, out consumed);
            case State.Header:
                if (_role == StreamRole.Control)
                {
                    return StreamFraming.TryParseControlFrameHeader(source, out _controlType, out _message.Length, out consumed);
                }

                if (_role == StreamRole.Bulk)
                {
                    return ParseChunkHeader(source, out consumed);
                }

                return StreamFraming.ParseMessageHeader(source, _shape, _limit, out _message, out consumed);
            case State.BulkIdentity:
                return StreamFraming.ParseBulkIdentity(source, ref _bulk, out consumed);
            case State.BulkRange:
                return StreamFraming.ParseBulkRange(source, _limit, ref _bulk, out consumed);
            default:
                return StreamFraming.ParseBulkHash(source, ref _bulk, out consumed);
        }
    }

    private StreamEvent OnHeaderParsed()
    {
        switch (_state)
        {
            case State.Preamble:
                _state = _role == StreamRole.Bulk ? State.BulkIdentity : State.Header;
                return StreamEvent.Preamble;

            case State.Header:
                if (_role == StreamRole.Control)
                {
                    _message.Sequence = 0;
                    _message.Key = 0;
                    _message.RequestId = 0;
                    _message.RawLength = 0;
                }
                else if (_role == StreamRole.Bulk)
                {
                    _bulkDecoded += _message.RawLength == 0 ? (ulong)_message.Length : (ulong)_message.RawLength;
                }
                else if (_latest && _message.Sequence != (uint)_groupId)
                {
                    return Fail(ParseStatus.SequenceMismatch);
                }

                _remaining = _message.Length;
                _state = _remaining == 0 ? State.MessageEnd : State.Payload;
                return StreamEvent.MessageStart;

            case State.BulkIdentity:
                _state = State.BulkRange;
                return Continue;

            case State.BulkRange:
                if (_bulk.HasHash)
                {
                    _state = State.BulkHash;
                    return Continue;
                }

                goto default;

            default:
                _state = _bulk.IsChunked ? State.Header : State.BodyStart;
                return StreamEvent.BulkHeader;
        }
    }

    private ParseStatus ParsePreamble(ChannelTable? table, ReadOnlySpan<byte> source, out int consumed)
    {
        consumed = 0;
        int pos = 0;
        ParseStatus status = WireReader.ReadVarInt(source, ref pos, out ulong channel);
        if (status != ParseStatus.Ok)
        {
            return status;
        }

        if (_expected == StreamRole.Control)
        {
            if (channel != 0)
            {
                return ParseStatus.RoleMismatch;
            }

            _channel = 0;
            _role = StreamRole.Control;
            _limit = StreamFraming.MaxControlFrameLength - 1;
            consumed = pos;
            return ParseStatus.Ok;
        }

        if (channel < ChannelDefinition.MinId)
        {
            return ParseStatus.ChannelNotStream;
        }

        ChannelDefinition? definition = channel <= ChannelDefinition.MaxId ? table?[(int)channel] : null;
        if (definition is null)
        {
            return ParseStatus.UnknownChannel;
        }

        if (!definition.IsStreamMode)
        {
            return ParseStatus.ChannelNotStream;
        }

        StreamRole role = definition.Mode switch
        {
            ChannelMode.ReliableOrdered => StreamRole.Ordered,
            ChannelMode.Bulk => StreamRole.Bulk,
            _ => StreamRole.Group,
        };
        if (_expected != StreamRole.Unknown && _expected != role)
        {
            return ParseStatus.RoleMismatch;
        }

        bool latest = definition.Mode == ChannelMode.ReliableLatest;
        ulong groupId = 0;
        if (role == StreamRole.Group)
        {
            status = WireReader.ReadVarInt(source, ref pos, out groupId);
            if (status != ParseStatus.Ok)
            {
                return status;
            }

            if (latest && groupId > uint.MaxValue)
            {
                return ParseStatus.ValueOutOfRange;
            }
        }

        _channel = (ushort)channel;
        _groupId = groupId;
        _role = role;
        _latest = latest;
        _shape = definition.StreamShape;
        // The session cap bounds message frames; a Bulk transfer is bounded by its channel alone (PROTOCOL.md §8).
        _limit = role == StreamRole.Bulk ? definition.MaxMessageSize : StreamFraming.EffectiveLimit(definition, _maxMessageSize);
        consumed = pos;
        return ParseStatus.Ok;
    }

    private ParseStatus ParseChunkHeader(ReadOnlySpan<byte> source, out int consumed)
    {
        consumed = 0;
        int pos = 0;
        ParseStatus status = WireReader.ReadVarInt(source, ref pos, out ulong chunkLength);
        if (status != ParseStatus.Ok)
        {
            return status;
        }

        if (chunkLength == 0)
        {
            return ParseStatus.BadLength;
        }

        if (chunkLength > (ulong)_bulkMaxChunk)
        {
            return ParseStatus.MessageTooLarge;
        }

        status = WireReader.ReadVarInt(source, ref pos, out ulong rawLength);
        if (status != ParseStatus.Ok)
        {
            return status;
        }

        if (rawLength > (ulong)_bulkMaxChunk)
        {
            return ParseStatus.MessageTooLarge;
        }

        if (rawLength != 0 && chunkLength >= rawLength)
        {
            return ParseStatus.BadLength;
        }

        ulong decoded = rawLength == 0 ? chunkLength : rawLength;
        if (decoded > _bulk.Length - _bulkDecoded)
        {
            return ParseStatus.BadLength;
        }

        _message = new StreamMessageHeader { Length = (int)chunkLength, RawLength = (int)rawLength };
        consumed = pos;
        return ParseStatus.Ok;
    }

    private readonly State StateAfterMessage()
    {
        if (_role == StreamRole.Bulk)
        {
            return _bulk.IsChunked && _bulkDecoded < _bulk.Length ? State.Header : State.Done;
        }

        return _latest ? State.Done : State.Header;
    }

    private StreamEvent Fail(ParseStatus status)
    {
        _error = status;
        _state = State.Failed;
        return StreamEvent.Error;
    }

    [InlineArray(HeaderBufferSize)]
    private struct HeaderBuffer
    {
        private byte _element0;
    }
}
