using System.Runtime.CompilerServices;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Session;

// Transport thread: ITransportSink callbacks, datagram and stream receive, control-message parsing (PROTOCOL.md §2-§3).
// Nothing here touches game-thread state; results reach the game thread through signal bits (fields written before
// Signal), the pong rings, the receive/completion rings and PeerCore.RequestClose.
public sealed unsafe partial class QuiclyPeer
{
    private readonly TransportControlPool _controlPool = new();
    private readonly SpscRing<PongSample> _pongs = new(16);
    private readonly SpscRing<PongSample> _streamPings = new(8);
    private TokenBucket _controlBucket;
    private TokenBucket _pongBucket;
    private bool _handshakeMessageSeen;
    private byte[]? _controlAssembly;
    private int _controlAssemblyFill;
    private ControlType _controlType;
    private int _controlLength;
    private long _lastReceiveMicros;
    private volatile bool _ignoreIncoming;
    private long _controlStreamWord;

    // Written by the transport thread before the matching signal, read by the game thread after it.
    private byte[]? _helloBody;
    private byte[]? _helloAckBody;
    private byte[]? _tableInfoBody;
    private QuiclyErrorCode _peerCloseCode;
    private byte[]? _peerCloseReason;
    private TransportCloseReason _transportCloseReason;
    private ulong _transportCloseCode;
    private int _transportCloseStatus;

    /// <summary>The control stream (client: opened by the game thread; server: the peer's first bidirectional stream).</summary>
    private TransportStreamId ControlStreamId
    {
        get
        {
            long word = Volatile.Read(ref _controlStreamWord);
            return new TransportStreamId((int)(uint)word, (uint)((ulong)word >> 32));
        }
    }

    private void SetControlStreamId(TransportStreamId id) =>
        Volatile.Write(ref _controlStreamWord, (long)(((ulong)id.Generation << 32) | (uint)id.Slot));

    // ------------------------------------------------------------------ connection

    private void HandleConnected(in TransportConnectedInfo info)
    {
        long now = _clock.NowMicros;
        _core.ConnectionStartMicros = now;
        Volatile.Write(ref _lastReceiveMicros, now);
        if (info.RemoteEndPoint is not null)
        {
            Volatile.Write(ref _remoteEndPoint, info.RemoteEndPoint);
        }

        _core.SetDatagramCapability(info.Capabilities.Datagrams, info.Capabilities.MaxDatagramPayload);
        _core.SetDatagramStatesReported(info.Capabilities.DatagramSendState);
        _core.SetCancelOnBlocked(info.Capabilities.CancelOnBlocked);
        Signal(SignalConnected);
    }

    private void HandleClosed(TransportCloseReason reason, ulong errorCode, int transportStatus)
    {
        _transportCloseReason = reason;
        _transportCloseCode = errorCode;
        _transportCloseStatus = transportStatus;
        _ignoreIncoming = true;
        _core.MarkTransportClosed();
        Signal(SignalTransportClosed);
    }

    private void OnCallbackFault(Exception exception)
    {
        if (_failFast)
        {
            Environment.FailFast("A QUICLY transport callback threw.", exception);
        }

        _core.Counters.CallbackFaults++;
        Interlocked.CompareExchange(ref _lastFault, exception, null);
        RequestLocalClose(QuiclyErrorCode.InternalError);
    }

    /// <summary>Queues a local close (transport thread) and stops processing further input.</summary>
    /// <param name="code">The close code.</param>
    private void RequestLocalClose(QuiclyErrorCode code)
    {
        _ignoreIncoming = true;
        _core.RequestClose(code);
    }

    // ------------------------------------------------------------------ datagrams

    private void HandleDatagram(ReadOnlySpan<byte> datagram)
    {
        if (_ignoreIncoming)
        {
            return;
        }

        long now = _clock.NowMicros;
        PeerCounters counters = _core.Counters;
        counters.DatagramsReceived++;
        counters.DatagramBytesReceived += datagram.Length;
        Volatile.Write(ref _lastReceiveMicros, now);
        ParseStatus status = DatagramFraming.TryParse(datagram, _core.Table, _core.SessionMaxMessageSize, out MessageHeader header, out int offset);
        switch (status)
        {
            case ParseStatus.Ok:
                DeliverDatagram(in header, datagram.Slice(offset), now);
                break;
            case ParseStatus.ControlChannel:
                HandleControlDatagram(datagram, now);
                break;
            case ParseStatus.ContainerChannel:
                HandleContainer(datagram, now);
                break;
            default:
                counters.MalformedDatagrams++;
                _core.CountDatagramDropped(header.Channel);
                break;
        }
    }

    private void DeliverDatagram(in MessageHeader header, ReadOnlySpan<byte> payload, long now)
    {
        if (!_core.IsAdmitted)
        {
            _core.Counters.DroppedBeforeAdmission++;
            return;
        }

        _core.GetEngine(_core.ChannelIndexOf(header.Channel)).OnDatagram(in header, payload, now);
    }

    private void HandleContainer(ReadOnlySpan<byte> datagram, long now)
    {
        if (PackedContainer.TryParse(datagram, out PackedContainerReader reader) != ParseStatus.Ok)
        {
            _core.Counters.MalformedDatagrams++;
            return;
        }

        _core.CurrentSenderTick = reader.HasTick ? reader.Tick : 0;
        foreach (ReadOnlySpan<byte> message in reader)
        {
            if (_ignoreIncoming)
            {
                break;
            }

            ParseStatus status = DatagramFraming.TryParse(message, _core.Table, _core.SessionMaxMessageSize, out MessageHeader header, out int offset);
            if (status == ParseStatus.Ok)
            {
                DeliverDatagram(in header, message.Slice(offset), now);
            }
            else if (status == ParseStatus.ControlChannel)
            {
                HandleControlDatagram(message, now);
            }
            else
            {
                _core.Counters.MalformedDatagrams++;
                _core.CountDatagramDropped(header.Channel);
            }
        }

        _core.CurrentSenderTick = 0;
    }

    private void HandleControlDatagram(ReadOnlySpan<byte> frame, long now)
    {
        PeerCounters counters = _core.Counters;
        if (ControlCodec.TryReadDatagram(frame, out ControlType type, out ReadOnlySpan<byte> body, out _) != ControlParseStatus.Ok)
        {
            counters.MalformedDatagrams++;
            return;
        }

        counters.ControlMessagesReceived++;
        if (!TakeControlToken(now))
        {
            return;
        }

        if (!_core.IsAdmitted)
        {
            counters.DroppedBeforeAdmission++;
            return;
        }

        bool valid;
        switch (type)
        {
            case ControlType.Ping:
                valid = ControlCodec.TryParse(body, out Ping ping) == ControlParseStatus.Ok;
                if (valid)
                {
                    AnswerPingDatagram(ping.TimeMicros, now);
                }

                break;
            case ControlType.Pong:
                valid = ControlCodec.TryParse(body, out Pong pong) == ControlParseStatus.Ok;
                if (valid)
                {
                    QueuePong(in pong, now);
                }

                break;
            case ControlType.LatestAck:
            case ControlType.LatestReject:
                valid = RouteControl(ChannelMode.ReliableLatest, type, body, onStream: false, now);
                break;
            default:
                valid = RouteControl(ChannelMode.Bulk, type, body, onStream: false, now);
                break;
        }

        if (!valid)
        {
            counters.MalformedDatagrams++;
        }
    }

    private bool TakeControlToken(long now)
    {
        if (_controlBucket.TryTake(now))
        {
            return true;
        }

        // PROTOCOL.md §7: more than the allowed control messages per second closes the connection.
        RequestLocalClose(QuiclyErrorCode.LimitExceeded);
        return false;
    }

    private void AnswerPingDatagram(uint echo, long now)
    {
        PeerCounters counters = _core.Counters;
        if (!_pongBucket.TryTake(now))
        {
            counters.PingsIgnored++;
            return;
        }

        uint t = _core.ToWireMicros(now);
        Span<byte> frame = stackalloc byte[16];
        ControlCodec.TryWrite(frame, new Pong(echo, t, t), ControlCarrier.Datagram, out int written);
        ITransport? transport = _core.Transport;
        if (transport is not null && _controlPool.TrySend(transport, frame.Slice(0, written), TransportSendFlags.Priority))
        {
            counters.PongsSent++;
        }
        else
        {
            counters.PongSendFailures++;
        }
    }

    private void QueuePong(in Pong pong, long now)
    {
        PongSample sample = new()
        {
            Echo = pong.EchoedTimeMicros,
            RemoteReceive = pong.ReceiveTimeMicros,
            RemoteSend = pong.SendTimeMicros,
            LocalReceive = _core.ToWireMicros(now),
        };
        _core.Counters.PongsReceived++;
        if (!_pongs.TryEnqueue(in sample))
        {
            _core.Counters.PongSamplesDropped++;
        }
    }

    private bool RouteControl(ChannelMode mode, ControlType type, ReadOnlySpan<byte> body, bool onStream, long now)
    {
        ChannelEngine? engine = _core.GetEngine(mode);
        if (engine is null)
        {
            // The message names channels of a mode the table does not have.
            return false;
        }

        ControlParseStatus status = type switch
        {
            ControlType.LatestAck => ControlCodec.TryParse(body, out LatestAckBatchReader _),
            ControlType.LatestReject => ControlCodec.TryParse(body, out LatestRejectBatchReader _),
            ControlType.BulkProgress => ControlCodec.TryParse(body, out BulkProgress _),
            ControlType.BulkRequest => ControlCodec.TryParse(body, out BulkRequest _),
            ControlType.BulkCancel => ControlCodec.TryParse(body, out BulkCancel _),
            ControlType.BulkReject => ControlCodec.TryParse(body, out BulkReject _),
            _ => ControlParseStatus.UnknownType,
        };
        return status == ControlParseStatus.Ok && engine.OnControl(type, body, onStream, now);
    }

    // ------------------------------------------------------------------ streams

    private void HandlePeerStreamStarted(TransportStreamId id, StreamKind kind)
    {
        if (_ignoreIncoming)
        {
            _core.Streams.Add(id, StreamTag.Discard);
            return;
        }

        if (kind == StreamKind.Bidirectional)
        {
            if (_role == PeerRole.Server && !ControlStreamId.IsValid)
            {
                ref StreamRecord control = ref _core.Streams.Add(id, StreamTag.Control);
                control.Parser.Reset(StreamRole.Control);
                SetControlStreamId(id);
                return;
            }

            if (_role == PeerRole.Server)
            {
                // PROTOCOL.md §3: a second control stream is a connection-level protocol violation.
                RequestLocalClose(QuiclyErrorCode.ProtocolViolation);
            }

            ResetPeerStream(id, QuiclyErrorCode.UnsupportedChannel);
            return;
        }

        if (!_core.IsAdmitted)
        {
            _core.Counters.DroppedBeforeAdmission++;
            ResetPeerStream(id, QuiclyErrorCode.AdmissionRejected);
            return;
        }

        ref StreamRecord record = ref _core.Streams.Add(id, StreamTag.Preamble);
        record.Parser.Reset(StreamRole.Unknown, _core.SessionMaxMessageSize);
    }

    private void ResetPeerStream(TransportStreamId id, QuiclyErrorCode code)
    {
        ref StreamRecord record = ref _core.Streams.Find(id);
        if (Unsafe.IsNullRef(ref record))
        {
            _core.Streams.Add(id, StreamTag.Discard);
        }
        else
        {
            record.Tag = StreamTag.Discard;
        }

        _core.Counters.StreamsReset++;
        _core.Transport?.AbortStream(id, (ulong)code, StreamAbortDirection.Both);
    }

    private ReceiveResult HandleStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, bool fin)
    {
        int total = TotalLength(segments);
        long now = _clock.NowMicros;
        _core.Counters.StreamBytesReceived += total;
        Volatile.Write(ref _lastReceiveMicros, now);
        if (_ignoreIncoming)
        {
            return ReceiveResult.Consumed(total);
        }

        ref StreamRecord record = ref _core.Streams.Find(id);
        if (Unsafe.IsNullRef(ref record))
        {
            if (_role != PeerRole.Client || id != ControlStreamId)
            {
                return ReceiveResult.Consumed(total);
            }

            // The client's own control stream: the record is created lazily on the transport thread.
            record = ref _core.Streams.Add(id, StreamTag.Control);
            record.Parser.Reset(StreamRole.Control);
        }

        switch (record.Tag)
        {
            case StreamTag.Control:
                ReceiveControl(ref record, segments, fin, now);
                return ReceiveResult.Consumed(total);
            case StreamTag.Preamble:
            case StreamTag.Engine:
                return ReceiveEngineStream(ref record, id, segments, total, fin, now);
            default:
                return ReceiveResult.Consumed(total);
        }
    }

    private static int TotalLength(ReadOnlySpan<TransportSegment> segments)
    {
        long total = 0;
        for (int i = 0; i < segments.Length; i++)
        {
            total += segments[i].Length;
        }

        return (int)Math.Min(total, int.MaxValue);
    }

    private void ReceiveControl(ref StreamRecord record, ReadOnlySpan<TransportSegment> segments, bool fin, long now)
    {
        for (int i = 0; i < segments.Length; i++)
        {
            ReadOnlySpan<byte> input = segments[i].AsSpan();
            ReadOnlySpan<byte> inPlace = default;
            bool hasInPlace = false;
            while (true)
            {
                StreamEvent streamEvent = record.Parser.Read(null, ref input, out ReadOnlySpan<byte> payload);
                if (streamEvent == StreamEvent.NeedMore)
                {
                    break;
                }

                switch (streamEvent)
                {
                    case StreamEvent.MessageStart:
                        _controlType = (ControlType)record.Parser.ControlType;
                        _controlLength = record.Parser.Message.Length;
                        _controlAssemblyFill = 0;
                        hasInPlace = false;
                        break;
                    case StreamEvent.PayloadChunk:
                        if (_controlAssemblyFill == 0 && payload.Length == _controlLength)
                        {
                            inPlace = payload;
                            hasInPlace = true;
                        }
                        else
                        {
                            if (_controlAssembly is null || _controlAssembly.Length < _controlLength)
                            {
                                Array.Resize(ref _controlAssembly, Math.Max(_controlLength, 256));
                            }

                            payload.CopyTo(_controlAssembly.AsSpan(_controlAssemblyFill));
                            _controlAssemblyFill += payload.Length;
                        }

                        break;
                    case StreamEvent.MessageEnd:
                    {
                        ReadOnlySpan<byte> body = hasInPlace ? inPlace
                            : _controlAssemblyFill == 0 ? default : _controlAssembly.AsSpan(0, _controlAssemblyFill);
                        hasInPlace = false;
                        _controlAssemblyFill = 0;
                        HandleControlMessage(_controlType, body, now);
                        if (_ignoreIncoming)
                        {
                            return;
                        }

                        break;
                    }

                    case StreamEvent.Error:
                        RequestLocalClose(QuiclyErrorCode.ProtocolViolation);
                        return;
                }
            }
        }

        if (fin && !_ignoreIncoming)
        {
            // The control stream lives as long as the connection: a FIN without a preceding Close is a violation.
            RequestLocalClose(QuiclyErrorCode.ProtocolViolation);
        }
    }

    private void HandleControlMessage(ControlType type, ReadOnlySpan<byte> body, long now)
    {
        _core.Counters.ControlMessagesReceived++;
        if (!TakeControlToken(now))
        {
            return;
        }

        if (type == ControlType.Close)
        {
            HandlePeerClose(body);
            return;
        }

        bool valid = _role == PeerRole.Server ? HandleServerControl(type, body, now) : HandleClientControl(type, body, now);
        if (!valid)
        {
            RequestLocalClose(QuiclyErrorCode.ProtocolViolation);
        }
    }

    private bool HandleServerControl(ControlType type, ReadOnlySpan<byte> body, long now)
    {
        if (!_handshakeMessageSeen)
        {
            // PROTOCOL.md §3.4: the client MUST send Hello as the first frame.
            if (type != ControlType.Hello)
            {
                return false;
            }

            _handshakeMessageSeen = true;
            ControlParseStatus status = ControlCodec.TryParse(body, out Hello _);
            if (status != ControlParseStatus.Ok && status != ControlParseStatus.UnsupportedVersion)
            {
                return false;
            }

            _helloBody = body.ToArray();
            Signal(SignalHello);
            return true;
        }

        switch (type)
        {
            case ControlType.Hello: // at most one Hello per connection
            case ControlType.HelloAck:
                return false;
            case ControlType.ChannelTableRequest:
                if (ControlCodec.TryParseChannelTableRequest(body) != ControlParseStatus.Ok)
                {
                    return false;
                }

                if (!_core.IsAdmitted)
                {
                    _core.Counters.DroppedBeforeAdmission++;
                    return true;
                }

                Signal(SignalTableRequest);
                return true;
            default:
                return HandleCommonControl(type, body, now);
        }
    }

    private bool HandleClientControl(ControlType type, ReadOnlySpan<byte> body, long now)
    {
        if (type == ControlType.HelloAck)
        {
            if (ControlCodec.TryParse(body, out HelloAck ack) != ControlParseStatus.Ok)
            {
                return false;
            }

            if (!_handshakeMessageSeen)
            {
                if (ack.Status == HelloStatus.Informational || (ack.Status == HelloStatus.Accepted && ack.Epoch == 0))
                {
                    return false;
                }

                _handshakeMessageSeen = true;
                if (ack.Status == HelloStatus.Accepted)
                {
                    _core.SessionMaxMessageSize = ack.MaxMessageSize == 0
                        ? 0
                        : (int)Math.Min(ack.MaxMessageSize, (ulong)ChannelDefinition.BulkMaxMessageSize);
                    _core.IsAdmitted = true;
                }

                _helloAckBody = body.ToArray();
                Signal(SignalHelloAck);
                return true;
            }

            if (ack.Status != HelloStatus.Informational)
            {
                return false;
            }

            Volatile.Write(ref _tableInfoBody, body.ToArray());
            Signal(SignalTableInfo);
            return true;
        }

        if (!_handshakeMessageSeen)
        {
            // The server's first frame answers the Hello.
            return false;
        }

        return type is not (ControlType.Hello or ControlType.ChannelTableRequest) && HandleCommonControl(type, body, now);
    }

    private bool HandleCommonControl(ControlType type, ReadOnlySpan<byte> body, long now)
    {
        if (!_core.IsAdmitted)
        {
            _core.Counters.DroppedBeforeAdmission++;
            return true;
        }

        switch (type)
        {
            case ControlType.Ping:
                if (ControlCodec.TryParse(body, out Ping ping) != ControlParseStatus.Ok)
                {
                    return false;
                }

                QueueStreamPong(ping.TimeMicros, now);
                return true;
            case ControlType.Pong:
                if (ControlCodec.TryParse(body, out Pong pong) != ControlParseStatus.Ok)
                {
                    return false;
                }

                QueuePong(in pong, now);
                return true;
            case ControlType.LatestAck:
            case ControlType.LatestReject:
                return RouteControl(ChannelMode.ReliableLatest, type, body, onStream: true, now);
            case ControlType.BulkProgress:
            case ControlType.BulkRequest:
            case ControlType.BulkCancel:
            case ControlType.BulkReject:
                return RouteControl(ChannelMode.Bulk, type, body, onStream: true, now);
            case ControlType.KeyRetired:
            {
                if (ControlCodec.TryParse(body, out KeyRetired retired) != ControlParseStatus.Ok)
                {
                    return false;
                }

                int index = _core.ChannelIndexOf(retired.Channel);
                return index >= 0 && _core.GetEngine(index).OnControl(type, body, onStream: true, now);
            }

            default:
                return false;
        }
    }

    private void QueueStreamPong(uint echo, long now)
    {
        if (!_pongBucket.TryTake(now))
        {
            _core.Counters.PingsIgnored++;
            return;
        }

        PongSample request = new() { Echo = echo, RemoteReceive = _core.ToWireMicros(now) };
        if (!_streamPings.TryEnqueue(in request))
        {
            _core.Counters.PingsIgnored++;
        }
    }

    private void HandlePeerClose(ReadOnlySpan<byte> body)
    {
        if (ControlCodec.TryParse(body, out Tedd.Quicly.Core.Control.Close close) != ControlParseStatus.Ok)
        {
            RequestLocalClose(QuiclyErrorCode.ProtocolViolation);
            return;
        }

        _peerCloseCode = close.Code;
        _peerCloseReason = close.Reason.IsEmpty ? null : close.Reason.ToArray();
        // PROTOCOL.md §6: after receiving Close every further QUICLY frame is ignored.
        _ignoreIncoming = true;
        Signal(SignalPeerClose);
    }

    private ReceiveResult ReceiveEngineStream(ref StreamRecord record, TransportStreamId id, ReadOnlySpan<TransportSegment> segments, int total, bool fin, long now)
    {
        ChannelTable table = _core.Table;
        int consumed = 0;
        for (int i = 0; i < segments.Length; i++)
        {
            ReadOnlySpan<byte> segment = segments[i].AsSpan();
            ReadOnlySpan<byte> input = segment;
            while (true)
            {
                StreamFrameParser snapshot = record.Parser;
                int before = segment.Length - input.Length;
                StreamEvent streamEvent = record.Parser.Read(table, ref input, out ReadOnlySpan<byte> payload);
                if (streamEvent == StreamEvent.NeedMore)
                {
                    break;
                }

                if (streamEvent == StreamEvent.Error)
                {
                    FailEngineStream(ref record, id, record.Parser.Error);
                    return ReceiveResult.Consumed(total);
                }

                if (streamEvent == StreamEvent.Preamble)
                {
                    if (!OpenEngineStream(ref record, id))
                    {
                        return ReceiveResult.Consumed(total);
                    }

                    continue;
                }

                if (record.Tag != StreamTag.Engine)
                {
                    return ReceiveResult.Consumed(total);
                }

                scoped StreamMessageContext context = default;
                context.Id = id;
                context.Channel = record.Channel;
                context.ChannelIndex = record.ChannelIndex;
                context.GroupId = record.Parser.GroupId;
                context.Cookie = ref record.Cookie;
                context.NowMicros = now;
                context.Header = record.Parser.Message;
                switch (streamEvent)
                {
                    case StreamEvent.MessageStart:
                        context.Phase = StreamMessagePhase.Start;
                        break;
                    case StreamEvent.PayloadChunk:
                        context.Phase = StreamMessagePhase.Chunk;
                        context.Chunk = payload;
                        break;
                    case StreamEvent.MessageEnd:
                        context.Phase = StreamMessagePhase.End;
                        break;
                    default:
                        context.Phase = StreamMessagePhase.BulkHeader;
                        context.Bulk = record.Parser.Bulk;
                        break;
                }

                StreamConsume result = _core.GetEngine(record.ChannelIndex).OnStreamMessage(ref context);
                switch (result.Action)
                {
                    case StreamConsumeAction.Pend:
                        // Un-read the event; the transport holds the rest until Poll resumes the stream.
                        record.Parser = snapshot;
                        _core.NotePendedStream(id);
                        return ReceiveResult.PendingAfter(consumed + before);
                    case StreamConsumeAction.ResetStream:
                        AbortEngineStream(ref record, id, result.Code);
                        return ReceiveResult.Consumed(total);
                    case StreamConsumeAction.CloseConnection:
                        NotifyEngineClosed(ref record, id, aborted: true, (ulong)result.Code);
                        RequestLocalClose(result.Code);
                        return ReceiveResult.Consumed(total);
                }
            }

            consumed += segment.Length;
        }

        if (fin)
        {
            ParseStatus status = record.Parser.Finish();
            if (status != ParseStatus.Ok)
            {
                FailEngineStream(ref record, id, status);
            }
        }

        return ReceiveResult.Consumed(total);
    }

    private bool OpenEngineStream(ref StreamRecord record, TransportStreamId id)
    {
        ushort channel = record.Parser.Channel;
        int index = _core.ChannelIndexOf(channel);
        record.Channel = channel;
        record.ChannelIndex = index;
        record.Mode = _core.GetChannel(index).Mode;
        StreamAccept accept = _core.GetEngine(index).OnStreamOpened(id, channel, record.Parser.GroupId);
        switch (accept.Action)
        {
            case StreamAcceptAction.Accept:
                record.Tag = StreamTag.Engine;
                record.Cookie = accept.Cookie;
                return true;
            case StreamAcceptAction.CloseConnection:
                record.Tag = StreamTag.Discard;
                RequestLocalClose(accept.ResetCode);
                return false;
            default:
                record.Tag = StreamTag.Discard;
                _core.Counters.StreamsReset++;
                _core.Transport?.AbortStream(id, (ulong)accept.ResetCode, StreamAbortDirection.Both);
                return false;
        }
    }

    private void FailEngineStream(ref StreamRecord record, TransportStreamId id, ParseStatus error)
    {
        if (record.Tag == StreamTag.Preamble)
        {
            // PROTOCOL.md §3: a preamble naming a channel that cannot be carried on a stream (or no channel) is reset
            // with UnsupportedChannel; a preamble that is not even well-formed is a protocol violation of the stream.
            QuiclyErrorCode code = error is ParseStatus.UnknownChannel or ParseStatus.ChannelNotStream or ParseStatus.RoleMismatch
                ? QuiclyErrorCode.UnsupportedChannel
                : QuiclyErrorCode.ProtocolViolation;
            record.Tag = StreamTag.Discard;
            _core.Counters.StreamsReset++;
            _core.Transport?.AbortStream(id, (ulong)code, StreamAbortDirection.Both);
            return;
        }

        if (record.Mode == ChannelMode.ReliableOrdered)
        {
            // A persistent ordered stream cannot be resynchronised (PROTOCOL.md §3.1, §6).
            NotifyEngineClosed(ref record, id, aborted: true, (ulong)QuiclyErrorCode.ProtocolViolation);
            RequestLocalClose(QuiclyErrorCode.ProtocolViolation);
            return;
        }

        AbortEngineStream(ref record, id, QuiclyErrorCode.ProtocolViolation);
    }

    private void AbortEngineStream(ref StreamRecord record, TransportStreamId id, QuiclyErrorCode code)
    {
        NotifyEngineClosed(ref record, id, aborted: true, (ulong)code);
        _core.Counters.StreamsReset++;
        _core.Transport?.AbortStream(id, (ulong)code, StreamAbortDirection.Both);
    }

    private void NotifyEngineClosed(ref StreamRecord record, TransportStreamId id, bool aborted, ulong code)
    {
        if (record.Tag == StreamTag.Engine)
        {
            record.Tag = StreamTag.Discard;
            _core.GetEngine(record.ChannelIndex).OnStreamClosed(id, aborted, code);
        }
        else
        {
            record.Tag = StreamTag.Discard;
        }
    }

    private void HandleStreamAborted(TransportStreamId id, ulong errorCode, StreamAbortDirection direction)
    {
        ref StreamRecord record = ref _core.Streams.Find(id);
        if (!Unsafe.IsNullRef(ref record))
        {
            if (record.Tag == StreamTag.Control)
            {
                if (!_ignoreIncoming)
                {
                    RequestLocalClose(QuiclyErrorCode.ProtocolViolation);
                }

                return;
            }

            if ((direction & StreamAbortDirection.Send) != 0)
            {
                NotifyEngineClosed(ref record, id, aborted: true, errorCode);
            }

            return;
        }

        if (id == ControlStreamId)
        {
            if (!_ignoreIncoming)
            {
                RequestLocalClose(QuiclyErrorCode.ProtocolViolation);
            }

            return;
        }

        // A stream this end opened was stopped by the peer.
        ReadOnlySpan<ChannelEngine> engines = _core.ActiveEngines;
        for (int i = 0; i < engines.Length; i++)
        {
            engines[i].OnStreamClosed(id, aborted: true, errorCode);
        }
    }

    private void HandleStreamShutdownComplete(TransportStreamId id)
    {
        ref StreamRecord record = ref _core.Streams.Find(id);
        if (!Unsafe.IsNullRef(ref record))
        {
            NotifyEngineClosed(ref record, id, aborted: false, 0);
            _core.Streams.Remove(id);
        }
        else if (id != ControlStreamId)
        {
            ReadOnlySpan<ChannelEngine> engines = _core.ActiveEngines;
            for (int i = 0; i < engines.Length; i++)
            {
                engines[i].OnStreamClosed(id, aborted: false, 0);
            }
        }

        // Stream credit returns to the peer only when the slot is released (ARCHITECTURE.md §7).
        _core.Transport?.CloseStream(id);
    }

    private void HandleStreamStarted(TransportStreamId id, ulong context, TransportStatus status)
    {
        if (context == PeerCore.ControlStreamContext)
        {
            if (status != TransportStatus.Success && !_ignoreIncoming)
            {
                RequestLocalClose(QuiclyErrorCode.InternalError);
            }

            return;
        }

        // A stream an engine opened: its start (or refusal by the peer's stream limit) goes to the engine of the context's mode.
        if (PeerCore.TryDecodeEngineStreamContext(context, out ChannelMode mode, out _, out _) && (int)mode < ChannelEngines.ModeCount)
        {
            _core.GetEngine(mode)?.OnStreamStarted(id, context, status);
        }
    }

    private void HandleDatagramState(ulong context, DatagramSendState state)
    {
        if (TransportControlPool.IsPoolContext(context))
        {
            if (state.IsFinal() || (state == DatagramSendState.Sent && !_core.DatagramStatesReported))
            {
                _controlPool.Release(context);
            }

            return;
        }

        _core.OnTransportDatagramState(context, state);
    }

    /// <summary>
    /// The peer's <see cref="ITransportSink"/>: a private object so that transport callbacks are not public API. Every
    /// callback is wrapped so that it never throws (ADR 0008 invariant 8): a fault is recorded, counted and turned into a
    /// queued <see cref="QuiclyErrorCode.InternalError"/> close (or a fail-fast when configured).
    /// </summary>
    private sealed class Sink(QuiclyPeer peer) : ITransportSink
    {
        public void OnConnected(in TransportConnectedInfo info)
        {
            if (peer.IsFreed)
            {
                return;
            }

            try
            {
                peer.HandleConnected(in info);
            }
            catch (Exception exception)
            {
                peer.OnCallbackFault(exception);
            }
        }

        public void OnDatagramReceived(ReadOnlySpan<byte> payload)
        {
            if (peer.IsFreed)
            {
                return;
            }

            try
            {
                peer.HandleDatagram(payload);
            }
            catch (Exception exception)
            {
                peer.OnCallbackFault(exception);
            }
        }

        public void OnPeerStreamStarted(TransportStreamId id, StreamKind kind)
        {
            if (peer.IsFreed)
            {
                return;
            }

            try
            {
                peer.HandlePeerStreamStarted(id, kind);
            }
            catch (Exception exception)
            {
                peer.OnCallbackFault(exception);
            }
        }

        public void OnStreamStarted(TransportStreamId id, ulong context, TransportStatus status)
        {
            if (peer.IsFreed)
            {
                return;
            }

            try
            {
                peer.HandleStreamStarted(id, context, status);
            }
            catch (Exception exception)
            {
                peer.OnCallbackFault(exception);
            }
        }

        public ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin)
        {
            if (peer.IsFreed)
            {
                return ReceiveResult.Consumed(TotalLength(segments));
            }

            try
            {
                return peer.HandleStreamReceived(id, segments, fin);
            }
            catch (Exception exception)
            {
                peer.OnCallbackFault(exception);
                return ReceiveResult.Consumed(TotalLength(segments));
            }
        }

        public void OnStreamSendCompleted(TransportStreamId id, ulong context, bool canceled)
        {
            if (peer.IsFreed)
            {
                return;
            }

            try
            {
                peer._core.OnTransportStreamCompleted(context, canceled);
            }
            catch (Exception exception)
            {
                peer.OnCallbackFault(exception);
            }
        }

        public void OnDatagramSendStateChanged(ulong context, DatagramSendState state)
        {
            if (peer.IsFreed)
            {
                return;
            }

            try
            {
                peer.HandleDatagramState(context, state);
            }
            catch (Exception exception)
            {
                peer.OnCallbackFault(exception);
            }
        }

        public void OnStreamAborted(TransportStreamId id, ulong errorCode, StreamAbortDirection direction)
        {
            if (peer.IsFreed)
            {
                return;
            }

            try
            {
                peer.HandleStreamAborted(id, errorCode, direction);
            }
            catch (Exception exception)
            {
                peer.OnCallbackFault(exception);
            }
        }

        public void OnStreamPeerSendShutdown(TransportStreamId id)
        {
            // The FIN is handled with the last receive (fin = true).
        }

        public void OnStreamShutdownComplete(TransportStreamId id)
        {
            if (peer.IsFreed)
            {
                return;
            }

            try
            {
                peer.HandleStreamShutdownComplete(id);
            }
            catch (Exception exception)
            {
                peer.OnCallbackFault(exception);
            }
        }

        public void OnDatagramCapabilityChanged(bool enabled, int maxPayload)
        {
            if (peer.IsFreed)
            {
                return;
            }

            try
            {
                peer._core.SetDatagramCapability(enabled, maxPayload);
                if (peer._core.Transport is { } transport)
                {
                    // Read again with every change of the datagram capability (a client learns it at OnConnected otherwise).
                    peer._core.SetCancelOnBlocked(transport.Capabilities.CancelOnBlocked);
                }
            }
            catch (Exception exception)
            {
                peer.OnCallbackFault(exception);
            }
        }

        public void OnIdealSendBufferSize(TransportStreamId id, ulong bytes)
        {
            // Consumed by the bulk engine (wave C2).
        }

        public void OnStreamsAvailable(ushort bidirectional, ushort unidirectional)
        {
            if (!peer.IsFreed)
            {
                peer._core.NoteStreamCredit();
            }
        }

        public void OnPeerAddressChanged(in TransportConnectedInfo info)
        {
            if (!peer.IsFreed && info.RemoteEndPoint is not null)
            {
                Volatile.Write(ref peer._remoteEndPoint, info.RemoteEndPoint);
            }
        }

        public void OnClosed(TransportCloseReason reason, ulong errorCode, int transportStatus)
        {
            if (peer.IsFreed)
            {
                return;
            }

            try
            {
                peer.HandleClosed(reason, errorCode, transportStatus);
            }
            catch (Exception exception)
            {
                peer.OnCallbackFault(exception);
            }
            finally
            {
                peer.OnClosedSeen();
            }
        }
    }
}
