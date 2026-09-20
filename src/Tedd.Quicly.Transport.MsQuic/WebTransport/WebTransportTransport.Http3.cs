using System.Text;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Http3;
using Tedd.Quicly.Http3.Qpack;
using Tedd.Quicly.Http3.WebTransport;

namespace Tedd.Quicly.Transport.MsQuic.WebTransport;

public sealed unsafe partial class WebTransportTransport
{
    /// <summary>Control and QPACK streams outrank data: SETTINGS must not queue behind a bulk transfer.</summary>
    private const ushort Http3StreamPriority = 0xFFFF;

    private Http3HeaderCollection? _headers;

    // ---------------------------------------------------------------- session set-up

    /// <summary>
    /// Opens the HTTP/3 control stream (SETTINGS) and the two QPACK streams, and on the client the CONNECT stream that
    /// carries the Extended CONNECT request. Runs inside the inner transport's connected callback.
    /// </summary>
    private void OpenHttp3ControlStreams()
    {
        ITransport? inner = _inner;
        if (inner is null) return;

        lock (_gate)
        {
            if (_controlStreamsOpened) return;
            _controlStreamsOpened = true;
        }

        Http3Settings settings = _options.CreateSettings();
        int settingsLength = settings.GetFrameLength();
        Span<byte> control = ReserveControl(SegControl, 1 + settingsLength, out TransportSegment* controlSegment);
        int typeLength = Http3FrameWriter.WriteStreamType(control, Http3StreamType.Control);
        if (typeLength < 0 || settings.WriteFrame(control.Slice(typeLength)) < 0)
        {
            CloseWithHttp3Error(Http3ErrorCode.InternalError, "The carrier could not encode its SETTINGS frame.");
            return;
        }

        controlSegment->Length = (uint)(typeLength + settingsLength);
        if (!OpenInternalStream(inner, controlSegment)) return;

        Span<byte> encoder = ReserveControl(SegQpackEncoder, 1, out TransportSegment* encoderSegment);
        encoder[0] = (byte)Http3StreamType.QpackEncoder;
        if (!OpenInternalStream(inner, encoderSegment)) return;

        Span<byte> decoder = ReserveControl(SegQpackDecoder, 1, out TransportSegment* decoderSegment);
        decoder[0] = (byte)Http3StreamType.QpackDecoder;
        if (!OpenInternalStream(inner, decoderSegment)) return;

        if (_isClient) OpenConnectStream(inner);
    }

    /// <summary>Opens one of the carrier's own unidirectional streams and sends its whole content at once.</summary>
    private bool OpenInternalStream(ITransport inner, TransportSegment* segment)
    {
        TransportStatus status = inner.OpenStream(StreamKind.Unidirectional, 0, Http3StreamPriority, out TransportStreamId id);
        if (status != TransportStatus.Success)
        {
            CloseWithHttp3Error(Http3ErrorCode.InternalError, $"The carrier could not open an HTTP/3 stream ({status}).");
            return false;
        }

        if (EnsureSlot(id, StreamRole.Internal, StreamKind.Unidirectional, local: true) is null)
        {
            inner.CloseStream(id);
            return false;
        }

        status = inner.SendStream(id, segment, 1, 0, TransportSendFlags.Start | TransportSendFlags.DelaySend);
        if (status != TransportStatus.Success)
        {
            CloseWithHttp3Error(Http3ErrorCode.InternalError, $"The carrier could not send on an HTTP/3 stream ({status}).");
            return false;
        }

        return true;
    }

    /// <summary>Opens the client's CONNECT stream and sends the Extended CONNECT request on it.</summary>
    private void OpenConnectStream(ITransport inner)
    {
        Span<byte> fields = stackalloc byte[1024];
        int encoded = EncodeConnectRequest(fields, out bool failed);
        if (failed)
        {
            CloseWithHttp3Error(Http3ErrorCode.InternalError, "The Extended CONNECT request does not fit in the carrier's buffer.");
            return;
        }

        int headerLength = Http3FrameWriter.GetHeaderLength(Http3FrameType.Headers, (ulong)encoded);
        Span<byte> frame = ReserveControl(SegConnect, headerLength + encoded, out TransportSegment* segment);
        Http3FrameWriter.WriteHeader(frame, Http3FrameType.Headers, (ulong)encoded);
        fields.Slice(0, encoded).CopyTo(frame.Slice(headerLength));

        TransportStatus status = inner.OpenStream(StreamKind.Bidirectional, 0, Http3StreamPriority, out TransportStreamId id);
        if (status != TransportStatus.Success)
        {
            CloseWithHttp3Error(Http3ErrorCode.InternalError, $"The carrier could not open the CONNECT stream ({status}).");
            return;
        }

        if (EnsureSlot(id, StreamRole.Connect, StreamKind.Bidirectional, local: true) is null)
        {
            inner.CloseStream(id);
            return;
        }

        lock (_gate)
        {
            _connectStream = id;
        }

        status = inner.SendStream(id, segment, 1, 0, TransportSendFlags.Start);
        if (status != TransportStatus.Success)
        {
            CloseWithHttp3Error(Http3ErrorCode.InternalError, $"The carrier could not send the Extended CONNECT request ({status}).");
        }
    }

    private int EncodeConnectRequest(Span<byte> destination, out bool failed)
    {
        Span<byte> authority = stackalloc byte[256];
        Span<byte> path = stackalloc byte[256];
        Span<byte> origin = stackalloc byte[256];
        bool authorityOk = TryEncodeAscii(_options.Authority, authority, out int authorityLength);
        bool pathOk = TryEncodeAscii(_options.Path, path, out int pathLength);
        bool originOk = TryEncodeAscii(_options.Origin, origin, out int originLength);
        failed = !authorityOk || !pathOk || !originOk;
        if (failed) return 0;

        int written = WebTransportRequest.EncodeConnectRequest(destination, authority.Slice(0, authorityLength), path.Slice(0, pathLength), origin.Slice(0, originLength));
        failed = written < 0;
        return failed ? 0 : written;
    }

    private static bool TryEncodeAscii(string? value, Span<byte> destination, out int written)
    {
        if (string.IsNullOrEmpty(value))
        {
            written = 0;
            return true;
        }

        return Encoding.UTF8.TryGetBytes(value, destination, out written);
    }

    /// <summary>Reserves <paramref name="length"/> bytes of the control arena and points <paramref name="segment"/> at them.</summary>
    private Span<byte> ReserveControl(int index, int length, out TransportSegment* segment)
    {
        lock (_gate)
        {
            if (_controlUsed + length > ControlArenaSize) throw new InvalidOperationException("The carrier's control arena is full.");
            byte* buffer = _controlBytes.Pointer + _controlUsed;
            _controlUsed += length;
            segment = _controlSegments.Pointer + index;
            *segment = new TransportSegment(buffer, length);
            return new Span<byte>(buffer, length);
        }
    }

    /// <summary>The client's CONNECT stream started, so its QUIC id — the session id — is known.</summary>
    private void OnConnectStreamStarted(TransportStreamId id)
    {
        ITransport? inner = _inner;
        if (inner is null) return;
        long quicId = inner.GetQuicStreamId(id);
        if (quicId < 0 || !HttpDatagram.IsValidStreamId((ulong)quicId))
        {
            CloseWithHttp3Error(Http3ErrorCode.IdError, "The CONNECT stream has no usable QUIC stream id.");
            return;
        }

        SetSessionId((ulong)quicId);
    }

    /// <summary>The server saw the client's CONNECT stream; its QUIC id is the session id.</summary>
    private void RecordConnectStream(TransportStreamId id)
    {
        ITransport? inner = _inner;
        if (inner is null) return;
        long quicId = inner.GetQuicStreamId(id);
        if (quicId < 0 || !HttpDatagram.IsValidStreamId((ulong)quicId))
        {
            CloseWithHttp3Error(Http3ErrorCode.IdError, "The peer's CONNECT stream has no usable QUIC stream id.");
            return;
        }

        lock (_gate)
        {
            _connectStream = id;
        }

        SetSessionId((ulong)quicId);
    }

    private void SetSessionId(ulong sessionId)
    {
        bool raiseDatagrams;
        bool enabled;
        int payload;
        lock (_gate)
        {
            _sessionId = sessionId;
            _datagramPrefixLength = HttpDatagram.GetPrefixLength(sessionId);
            _sessionIdKnown = true;
            enabled = _datagramsEnabled;
            payload = _innerMaxDatagramPayload;
            raiseDatagrams = !_datagramCapabilityRaised;
            if (raiseDatagrams) _datagramCapabilityRaised = true;
        }

        // The prefix length is known now, so the payload Core may use is too. The contract allows this before OnConnected.
        if (raiseDatagrams) _sink?.OnDatagramCapabilityChanged(enabled && PeerSupportsDatagrams(), ReducedDatagramPayload(payload));
    }

    private bool PeerSupportsDatagrams() => !_peerSettingsReceived || _peerSettings.H3Datagram == 1;

    // ---------------------------------------------------------------- HTTP/3 stream parsing

    /// <summary>
    /// Feeds a control or CONNECT stream to its frame reader. Everything the reader can take is consumed, so these
    /// streams never hold back the connection.
    /// </summary>
    private ReceiveResult ReceiveHttp3(StreamSlot slot, TransportStreamId id, ReadOnlySpan<TransportSegment> segments, bool fin)
    {
        if (slot.ReplayLength > 0)
        {
            int replay = slot.ReplayLength;
            slot.ReplayLength = 0;
            if (!FeedHttp3(slot, id, slot.Partial.AsSpan(0, replay))) return ReceiveResult.Consumed(TotalLength(segments));
        }

        int total = 0;
        for (int i = 0; i < segments.Length; i++)
        {
            ReadOnlySpan<byte> bytes = segments[i].AsSpan();
            total += bytes.Length;
            if (!FeedHttp3(slot, id, bytes)) return ReceiveResult.Consumed(total);
        }

        if (fin && slot.Role == StreamRole.Connect) CloseSessionFromPeer("The peer finished the CONNECT stream.");
        return ReceiveResult.Consumed(total);
    }

    /// <summary>Runs the frame reader over one chunk. Returns false once the connection is being closed.</summary>
    private bool FeedHttp3(StreamSlot slot, TransportStreamId id, ReadOnlySpan<byte> bytes)
    {
        while (!bytes.IsEmpty)
        {
            Http3FrameReadStatus status = slot.Reader.Read(bytes, out int consumed, out ReadOnlySpan<byte> payload);
            bytes = bytes.Slice(consumed);
            switch (status)
            {
                case Http3FrameReadStatus.NeedMoreData:
                    return true;

                case Http3FrameReadStatus.Frame:
                    if (!DispatchHttp3Frame(slot, id, slot.Reader.Type, payload)) return false;
                    break;

                case Http3FrameReadStatus.PayloadFragment:
                    if (!Accumulate(slot, payload)) return false;
                    break;

                case Http3FrameReadStatus.PayloadEnd:
                    if (!Accumulate(slot, payload)) return false;
                    if (!DispatchHttp3Frame(slot, id, slot.Reader.Type, slot.Accumulator.AsSpan(0, slot.AccumulatorLength))) return false;
                    slot.AccumulatorLength = 0;
                    break;

                default:
                    CloseWithHttp3Error(slot.Role == StreamRole.Connect ? Http3ErrorCode.DatagramError : Http3ErrorCode.ExcessiveLoad, "An HTTP/3 frame exceeded the carrier's limit.");
                    return false;
            }
        }

        return true;
    }

    /// <summary>Collects the payload of a frame that arrived in fragments.</summary>
    private bool Accumulate(StreamSlot slot, ReadOnlySpan<byte> payload)
    {
        int needed = slot.AccumulatorLength + payload.Length;
        int limit = MaxFrameLengthFor(slot.Role);
        if (needed > limit)
        {
            CloseWithHttp3Error(Http3ErrorCode.ExcessiveLoad, "An HTTP/3 frame payload exceeded the carrier's limit.");
            return false;
        }

        if (slot.Accumulator is null || slot.Accumulator.Length < needed)
        {
            var grown = new byte[Math.Max(needed, 256)];
            slot.Accumulator.AsSpan(0, slot.AccumulatorLength).CopyTo(grown);
            slot.Accumulator = grown;
        }

        payload.CopyTo(slot.Accumulator.AsSpan(slot.AccumulatorLength));
        slot.AccumulatorLength = needed;
        return true;
    }

    private bool DispatchHttp3Frame(StreamSlot slot, TransportStreamId id, ulong type, ReadOnlySpan<byte> payload) =>
        slot.Role == StreamRole.Connect ? HandleConnectFrame(slot, id, type, payload) : HandleControlFrame(type, payload);

    // ---------------------------------------------------------------- control stream

    private bool HandleControlFrame(ulong type, ReadOnlySpan<byte> payload)
    {
        switch ((Http3FrameType)type)
        {
            case Http3FrameType.Settings:
                if (_peerSettingsReceived)
                {
                    CloseWithHttp3Error(Http3ErrorCode.FrameUnexpected, "The peer sent SETTINGS twice.");
                    return false;
                }

                return ApplyPeerSettings(payload);

            case Http3FrameType.GoAway:
                CloseSessionFromPeer("The peer sent GOAWAY.");
                return false;

            case Http3FrameType.CancelPush:
            case Http3FrameType.MaxPushId:
                return RequireSettingsFirst();

            case Http3FrameType.Data:
            case Http3FrameType.Headers:
            case Http3FrameType.PushPromise:
                CloseWithHttp3Error(Http3ErrorCode.FrameUnexpected, "A request frame arrived on the control stream.");
                return false;

            default:
                // Unknown and grease frame types are ignored (RFC 9114 §9), but SETTINGS still has to come first.
                return RequireSettingsFirst();
        }
    }

    private bool RequireSettingsFirst()
    {
        if (_peerSettingsReceived) return true;
        CloseWithHttp3Error(Http3ErrorCode.MissingSettings, "The peer's first control frame was not SETTINGS.");
        return false;
    }

    private bool ApplyPeerSettings(ReadOnlySpan<byte> payload)
    {
        if (Http3Settings.Decode(payload, out Http3Settings settings) != Http3SettingsDecodeStatus.Ok)
        {
            CloseWithHttp3Error(Http3ErrorCode.SettingsError, "The peer's SETTINGS frame is malformed.");
            return false;
        }

        if (settings.EnableWebTransport != 1 || settings.EnableConnectProtocol != 1)
        {
            CloseWithHttp3Error(Http3ErrorCode.SettingsError, "The peer does not support WebTransport over HTTP/3.");
            return false;
        }

        bool datagramsLost;
        lock (_gate)
        {
            _peerSettings = settings;
            _peerSettingsReceived = true;
            datagramsLost = settings.H3Datagram != 1 && _datagramCapabilityRaised && _datagramsEnabled;
        }

        // A peer without HTTP datagrams leaves QUICLY with streams only; Core hears that the capability went away.
        if (datagramsLost) _sink?.OnDatagramCapabilityChanged(false, 0);

        TryEstablishSession();
        return true;
    }

    // ---------------------------------------------------------------- CONNECT stream

    private bool HandleConnectFrame(StreamSlot slot, TransportStreamId id, ulong type, ReadOnlySpan<byte> payload)
    {
        if (!slot.HeadersSeen)
        {
            if (type != (ulong)Http3FrameType.Headers)
            {
                CloseWithHttp3Error(Http3ErrorCode.FrameUnexpected, "The CONNECT stream did not start with HEADERS.");
                return false;
            }

            slot.HeadersSeen = true;
            return _isClient ? HandleConnectResponse(payload) : HandleConnectRequest(id, payload);
        }

        // After the header section the stream carries capsules (RFC 9297 §3.2), which share the frame shape.
        return HandleCapsule(type, payload);
    }

    private Http3HeaderCollection Headers()
    {
        _headers ??= new Http3HeaderCollection(_options.MaxFieldSectionSize, 64);
        _headers.Clear();
        return _headers;
    }

    private bool HandleConnectRequest(TransportStreamId id, ReadOnlySpan<byte> payload)
    {
        Http3HeaderCollection headers = Headers();
        if (QpackDecoder.Decode(payload, headers, _options.MaxFieldSectionSize) != QpackDecodeStatus.Ok)
        {
            CloseWithHttp3Error(Http3ErrorCode.QpackDecompressionFailed, "The Extended CONNECT request could not be decoded.");
            return false;
        }

        if (WebTransportRequest.Validate(headers, out WebTransportConnectRequest request) != WebTransportRequestStatus.Ok)
        {
            RejectConnect(id, Http3ErrorCode.MessageError, "The request is not a WebTransport Extended CONNECT.");
            return false;
        }

        Span<byte> expectedPath = stackalloc byte[256];
        if (!TryEncodeAscii(_options.Path, expectedPath, out int pathLength) || !request.Path.SequenceEqual(expectedPath.Slice(0, pathLength)))
        {
            RejectConnect(id, Http3ErrorCode.RequestRejected, "The request path is not the configured WebTransport path.");
            return false;
        }

        if (!IsOriginAllowed(request.Origin))
        {
            RejectConnect(id, Http3ErrorCode.RequestRejected, "The request origin is not allowed.");
            return false;
        }

        return SendConnectResponse(id, request.IsLegacyDraft02);
    }

    private bool IsOriginAllowed(ReadOnlySpan<byte> origin)
    {
        switch (_options.OriginPolicy)
        {
            case WebTransportOriginPolicy.Allow:
                return true;
            case WebTransportOriginPolicy.CheckIfPresent when origin.IsEmpty:
                return true;
            default:
                break;
        }

        if (origin.IsEmpty) return false;
        Span<byte> candidate = stackalloc byte[256];
        foreach (string allowed in _options.AllowedOrigins)
        {
            if (!TryEncodeAscii(allowed, candidate, out int length)) continue;
            if (origin.SequenceEqual(candidate.Slice(0, length))) return true;
        }

        return false;
    }

    private bool SendConnectResponse(TransportStreamId id, bool legacyDraft)
    {
        ITransport? inner = _inner;
        if (inner is null) return false;

        Span<byte> fields = stackalloc byte[256];
        int encoded = WebTransportRequest.EncodeConnectResponse(fields, legacyDraft);
        if (encoded < 0)
        {
            CloseWithHttp3Error(Http3ErrorCode.InternalError, "The CONNECT response does not fit in the carrier's buffer.");
            return false;
        }

        int headerLength = Http3FrameWriter.GetHeaderLength(Http3FrameType.Headers, (ulong)encoded);
        Span<byte> frame = ReserveControl(SegConnect, headerLength + encoded, out TransportSegment* segment);
        Http3FrameWriter.WriteHeader(frame, Http3FrameType.Headers, (ulong)encoded);
        fields.Slice(0, encoded).CopyTo(frame.Slice(headerLength));

        TransportStatus status = inner.SendStream(id, segment, 1, 0, TransportSendFlags.None);
        if (status != TransportStatus.Success)
        {
            CloseWithHttp3Error(Http3ErrorCode.InternalError, $"The carrier could not send the CONNECT response ({status}).");
            return false;
        }

        _connectSettled = true;
        TryEstablishSession();
        return true;
    }

    private void RejectConnect(TransportStreamId id, Http3ErrorCode code, string reason)
    {
        Diagnostic(TransportDiagnosticLevel.Warning, $"WebTransport session refused: {reason}");
        _inner?.AbortStream(id, (ulong)code, StreamAbortDirection.Both);
        CloseWithHttp3Error(code, reason);
    }

    private bool HandleConnectResponse(ReadOnlySpan<byte> payload)
    {
        Http3HeaderCollection headers = Headers();
        if (QpackDecoder.Decode(payload, headers, _options.MaxFieldSectionSize) != QpackDecodeStatus.Ok)
        {
            CloseWithHttp3Error(Http3ErrorCode.QpackDecompressionFailed, "The CONNECT response could not be decoded.");
            return false;
        }

        if (!headers.TryGet(":status"u8, out ReadOnlySpan<byte> status) || !status.SequenceEqual("200"u8))
        {
            CloseWithHttp3Error(Http3ErrorCode.RequestRejected, "The server refused the WebTransport session.");
            return false;
        }

        _connectSettled = true;
        TryEstablishSession();
        return true;
    }

    // ---------------------------------------------------------------- capsules

    private bool HandleCapsule(ulong type, ReadOnlySpan<byte> payload)
    {
        switch ((CapsuleType)type)
        {
            case CapsuleType.CloseWebTransportSession:
                if (!CapsuleReader.TryParseCloseSession(payload, out uint code, out ReadOnlySpan<byte> reason))
                {
                    CloseWithHttp3Error(Http3ErrorCode.DatagramError, "A malformed CLOSE_WEBTRANSPORT_SESSION capsule arrived.");
                    return false;
                }

                lock (_gate)
                {
                    _peerCapsuleCode = code;
                }

                Diagnostic(TransportDiagnosticLevel.Information, $"The peer closed the WebTransport session with code {code} ({reason.Length} byte reason).");
                CloseSessionFromPeer("The peer sent CLOSE_WEBTRANSPORT_SESSION.");
                return false;

            case CapsuleType.DrainWebTransportSession:
                Diagnostic(TransportDiagnosticLevel.Information, "The peer asked to drain the WebTransport session.");
                return true;

            default:
                // Flow-control and unknown capsules are ignored: QUIC's own limits already bound the session.
                return true;
        }
    }

    /// <summary>Sends CLOSE_WEBTRANSPORT_SESSION on the CONNECT stream, finishing it.</summary>
    private void TrySendCloseCapsule(ulong errorCode, ReadOnlySpan<byte> reason)
    {
        ITransport? inner = _inner;
        TransportStreamId connect;
        lock (_gate)
        {
            connect = _connectStream;
        }

        if (inner is null || !connect.IsValid) return;

        ReadOnlySpan<byte> trimmed = reason.Length > CapsuleWriter.MaxCloseReasonLength ? reason.Slice(0, CapsuleWriter.MaxCloseReasonLength) : reason;
        int length = CapsuleWriter.GetLength((ulong)CapsuleType.CloseWebTransportSession, (ulong)(sizeof(uint) + trimmed.Length));
        if (length < 0 || length > ControlArenaSize) return;

        try
        {
            Span<byte> buffer = ReserveControl(SegCapsule, length, out TransportSegment* segment);
            int written = CapsuleWriter.WriteCloseSession(buffer, ToApplicationCode(errorCode), trimmed);
            if (written < 0) return;
            segment->Length = (uint)written;
            inner.SendStream(connect, segment, 1, 0, TransportSendFlags.Fin);
        }
        catch (InvalidOperationException)
        {
            // The arena is full; the mapped HTTP/3 connection error still carries the code.
        }
    }

    // ---------------------------------------------------------------- session lifecycle

    /// <summary>Raises <see cref="ITransportSink.OnConnected"/> once the session exists on both layers.</summary>
    private void TryEstablishSession()
    {
        TransportConnectedInfo info;
        ITransportSink? sink;
        lock (_gate)
        {
            if (_state != StateConnecting || _connectedRaised) return;
            if (!_connectSettled || !_peerSettingsReceived || !_sessionIdKnown) return;
            _state = StateEstablished;
            _connectedRaised = true;
            info = _connectedInfo;
            sink = _sink;
        }

        info.Capabilities.MaxDatagramPayload = ReducedDatagramPayload(info.Capabilities.MaxDatagramPayload);
        info.Capabilities.Datagrams = info.Capabilities.Datagrams && PeerSupportsDatagrams();
        sink?.OnConnected(in info);

        // Streams the peer opened optimistically, before this point, are Core's only now that it has been connected.
        ReleaseDeferredStreams();
    }

    /// <summary>Closes the inner connection with an HTTP/3 error; Core learns of it through the inner transport's close.</summary>
    private void CloseWithHttp3Error(Http3ErrorCode code, string reason)
    {
        ITransport? inner = _inner;
        lock (_gate)
        {
            if (_state is StateClosing or StateClosed) return;
            _state = StateClosing;
        }

        Diagnostic(TransportDiagnosticLevel.Warning, $"WebTransport carrier closing with {code}: {reason}");
        Span<byte> bytes = stackalloc byte[256];
        int length = Encoding.UTF8.TryGetBytes(reason, bytes, out int written) ? written : 0;
        inner?.Close((ulong)code, bytes.Slice(0, length));
    }

    /// <summary>The peer ended the session (capsule, GOAWAY, or the CONNECT stream going away).</summary>
    private void CloseSessionFromPeer(string reason)
    {
        ITransport? inner = _inner;
        lock (_gate)
        {
            // Remembered before the close, because the close comes back as a LOCAL one: it is this end that tears the
            // inner connection down. Without this the application is told it closed a session the peer ended.
            _peerEndedSession = true;
            if (_state is StateClosing or StateClosed) return;
            _state = StateClosing;
        }

        Diagnostic(TransportDiagnosticLevel.Information, $"WebTransport session ended by the peer: {reason}");
        inner?.Close((ulong)Http3ErrorCode.NoError, default);
    }
}
