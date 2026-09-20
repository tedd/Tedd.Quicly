using Tedd.Quicly.Testing.Conformance;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Http3;
using Tedd.Quicly.Http3.WebTransport;

namespace Tedd.Quicly.Transport.MsQuic.Tests.WebTransport;

/// <summary>
/// A hand-written HTTP/3 peer built straight on the raw simulated transport, with no help from the carrier: it sends its
/// own SETTINGS on a control stream and answers an Extended CONNECT with <c>:status 200</c>. Pointing the carrier's
/// client at it proves the carrier puts real HTTP/3 on the wire rather than something only it understands, which is what
/// interoperating with a browser depends on.
/// </summary>
internal sealed unsafe class RawHttp3Peer : ITransportSink, IDisposable
{
    private readonly List<NativeBuffer> _buffers = [];
    private readonly Dictionary<TransportStreamId, List<byte>> _streamBytes = [];
    private readonly Lock _gate = new();

    /// <summary>The transport this peer drives; set before the handshake completes.</summary>
    public ITransport? Transport { get; set; }

    /// <summary>The SETTINGS this peer advertises. WebTransport is on by default.</summary>
    public Http3Settings Settings { get; set; } = Http3Settings.CreateWebTransportServerDefaults(1);

    /// <summary>When false the peer never sends SETTINGS, which must stop the session from establishing.</summary>
    public bool SendSettings { get; set; } = true;

    /// <summary>The status this peer answers the Extended CONNECT with.</summary>
    public int ResponseStatus { get; set; } = 200;

    /// <summary>The bytes of the Extended CONNECT request's HEADERS payload, once it arrived.</summary>
    public byte[] ConnectRequestFields { get; private set; } = [];

    /// <summary>The raw bytes of the client's control stream, type byte included.</summary>
    public byte[] ControlStreamBytes { get; private set; } = [];

    /// <summary>The unidirectional stream types the client opened, in order.</summary>
    public List<ulong> UnidirectionalStreamTypes { get; } = [];

    /// <summary>The raw bytes of every WebTransport data stream the client opened, preamble included.</summary>
    public List<byte[]> DataStreamBytes { get; } = [];

    /// <summary>The raw payloads of every datagram received, RFC 9297 prefix included.</summary>
    public List<byte[]> DatagramPayloads { get; } = [];

    /// <summary>True once this peer answered the Extended CONNECT.</summary>
    public bool ConnectAnswered { get; private set; }

    /// <summary>The QUIC stream id of the CONNECT stream, or <see cref="ulong.MaxValue"/>.</summary>
    public ulong SessionId { get; private set; } = ulong.MaxValue;

    /// <summary>The local id of this peer's control stream, valid once it was opened.</summary>
    public TransportStreamId ControlStream { get; private set; }

    /// <summary>The local id of the CONNECT stream, valid once the client opened it.</summary>
    public TransportStreamId ConnectStream { get; private set; }

    /// <summary>
    /// Bytes sent on the control stream instead of the usual type byte and SETTINGS frame. Lets a test put a first
    /// frame that is not SETTINGS, a malformed SETTINGS, or two of them on the wire.
    /// </summary>
    public byte[]? ControlStreamOverride { get; set; }

    /// <summary>Sends one more frame on the control stream (after whatever went out at connect).</summary>
    public void SendOnControl(ulong frameType, ReadOnlySpan<byte> payload)
    {
        int length = Http3FrameWriter.GetHeaderLength(frameType, (ulong)payload.Length) + payload.Length;
        NativeBuffer buffer = Rent(length);
        Http3FrameWriter.WriteFrame(new Span<byte>(buffer.Pointer, length), frameType, payload);
        Transport!.SendStream(ControlStream, RentSegment(buffer.Segment(0, length)), 1, 3, TransportSendFlags.None);
    }

    /// <summary>Sends a capsule on the CONNECT stream, which is where the session's own signalling lives.</summary>
    public void SendCapsule(ulong type, ReadOnlySpan<byte> payload)
    {
        int length = Http3FrameWriter.GetHeaderLength(type, (ulong)payload.Length) + payload.Length;
        NativeBuffer buffer = Rent(length);
        Http3FrameWriter.WriteFrame(new Span<byte>(buffer.Pointer, length), type, payload);
        Transport!.SendStream(ConnectStream, RentSegment(buffer.Segment(0, length)), 1, 4, TransportSendFlags.None);
    }

    /// <summary>Opens a unidirectional stream carrying exactly <paramref name="bytes"/>.</summary>
    public TransportStreamId OpenUnidirectional(ReadOnlySpan<byte> bytes)
    {
        NativeBuffer buffer = Rent(bytes.Length);
        bytes.CopyTo(new Span<byte>(buffer.Pointer, bytes.Length));
        Transport!.OpenStream(StreamKind.Unidirectional, 5, 32767, out TransportStreamId id);
        Transport.SendStream(id, RentSegment(buffer.Segment(0, bytes.Length)), 1, 5, TransportSendFlags.Start);
        return id;
    }

    /// <summary>Sends more bytes on a stream this peer already opened.</summary>
    public void SendMore(TransportStreamId id, ReadOnlySpan<byte> bytes)
    {
        NativeBuffer buffer = Rent(bytes.Length);
        bytes.CopyTo(new Span<byte>(buffer.Pointer, bytes.Length));
        Transport!.SendStream(id, RentSegment(buffer.Segment(0, bytes.Length)), 1, 7, TransportSendFlags.None);
    }

    /// <summary>Opens a bidirectional stream carrying exactly <paramref name="bytes"/>.</summary>
    public TransportStreamId OpenBidirectional(ReadOnlySpan<byte> bytes)
    {
        NativeBuffer buffer = Rent(bytes.Length);
        bytes.CopyTo(new Span<byte>(buffer.Pointer, bytes.Length));
        Transport!.OpenStream(StreamKind.Bidirectional, 6, 32767, out TransportStreamId id);
        Transport.SendStream(id, RentSegment(buffer.Segment(0, bytes.Length)), 1, 6, TransportSendFlags.Start);
        return id;
    }

    public void OnConnected(in TransportConnectedInfo info)
    {
        if (!SendSettings && ControlStreamOverride is null) return;
        ITransport transport = Transport!;
        if (transport.OpenStream(StreamKind.Unidirectional, 1, 32767, out TransportStreamId id) != TransportStatus.Success) return;
        ControlStream = id;

        NativeBuffer buffer;
        int length;
        if (ControlStreamOverride is { } raw)
        {
            length = raw.Length;
            buffer = Rent(length);
            raw.CopyTo(new Span<byte>(buffer.Pointer, length));
        }
        else
        {
            Http3Settings settings = Settings;
            length = 1 + settings.GetFrameLength();
            buffer = Rent(length);
            var span = new Span<byte>(buffer.Pointer, length);
            span[0] = (byte)Http3StreamType.Control;
            settings.WriteFrame(span.Slice(1));
        }

        transport.SendStream(id, RentSegment(buffer.Segment(0, length)), 1, 1, TransportSendFlags.Start);
    }

    public void OnPeerStreamStarted(TransportStreamId id, StreamKind kind)
    {
        lock (_gate) _streamBytes[id] = [];
    }

    public ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin)
    {
        int total = 0;
        List<byte> bytes;
        lock (_gate)
        {
            if (!_streamBytes.TryGetValue(id, out bytes!)) _streamBytes[id] = bytes = [];
            for (int i = 0; i < segments.Length; i++)
            {
                ReadOnlySpan<byte> span = segments[i].AsSpan();
                total += span.Length;
                foreach (byte b in span) bytes.Add(b);
            }
        }

        Classify(id, bytes);
        return ReceiveResult.Consumed(total);
    }

    /// <summary>Sorts a client stream by its first varint and answers the Extended CONNECT once it is complete.</summary>
    private void Classify(TransportStreamId id, List<byte> bytes)
    {
        byte[] snapshot;
        lock (_gate) snapshot = bytes.ToArray();
        if (!Http3FrameReader.TryPeekType(snapshot, out ulong first, out int firstLength)) return;

        if (first == WebTransportFraming.UnidirectionalSignal)
        {
            lock (_gate)
            {
                if (!DataStreamBytes.Contains(snapshot)) RecordDataStream(snapshot);
            }

            return;
        }

        if (first == (ulong)Http3StreamType.Control)
        {
            lock (_gate)
            {
                ControlStreamBytes = snapshot;
                if (!UnidirectionalStreamTypes.Contains(first)) UnidirectionalStreamTypes.Add(first);
            }

            return;
        }

        if (first is (ulong)Http3StreamType.QpackEncoder or (ulong)Http3StreamType.QpackDecoder)
        {
            lock (_gate)
            {
                if (!UnidirectionalStreamTypes.Contains(first)) UnidirectionalStreamTypes.Add(first);
            }

            return;
        }

        if (first != (ulong)Http3FrameType.Headers || ConnectAnswered) return;
        if (!Http3FrameReader.TryReadFrame(snapshot, out _, out ReadOnlySpan<byte> payload, out _)) return;

        ConnectRequestFields = payload.ToArray();
        ConnectStream = id;
        SessionId = (ulong)(Transport?.GetQuicStreamId(id) ?? -1);
        ConnectAnswered = true;
        AnswerConnect(id);
    }

    private void RecordDataStream(byte[] snapshot)
    {
        for (int i = 0; i < DataStreamBytes.Count; i++)
        {
            if (DataStreamBytes[i].Length <= snapshot.Length && snapshot.AsSpan(0, DataStreamBytes[i].Length).SequenceEqual(DataStreamBytes[i]))
            {
                DataStreamBytes[i] = snapshot;
                return;
            }
        }

        DataStreamBytes.Add(snapshot);
    }

    private void AnswerConnect(TransportStreamId id)
    {
        Span<byte> fields = stackalloc byte[256];
        int encoded = ResponseStatus == 200
            ? WebTransportRequest.EncodeConnectResponse(fields)
            : WebTransportRequest.EncodeResponse(fields, ResponseStatus, default, -1);
        int headerLength = Http3FrameWriter.GetHeaderLength(Http3FrameType.Headers, (ulong)encoded);
        NativeBuffer buffer = Rent(headerLength + encoded);
        var span = new Span<byte>(buffer.Pointer, headerLength + encoded);
        Http3FrameWriter.WriteHeader(span, Http3FrameType.Headers, (ulong)encoded);
        fields.Slice(0, encoded).CopyTo(span.Slice(headerLength));
        TransportSegment* segment = RentSegment(buffer.Segment(0, headerLength + encoded));
        Transport!.SendStream(id, segment, 1, 2, TransportSendFlags.None);
    }

    public void OnDatagramReceived(ReadOnlySpan<byte> payload)
    {
        lock (_gate) DatagramPayloads.Add(payload.ToArray());
    }

    public void OnStreamStarted(TransportStreamId id, ulong context, TransportStatus status)
    {
    }

    public void OnStreamSendCompleted(TransportStreamId id, ulong context, bool canceled)
    {
    }

    public void OnDatagramSendStateChanged(ulong context, DatagramSendState state)
    {
    }

    public void OnStreamAborted(TransportStreamId id, ulong errorCode, StreamAbortDirection direction)
    {
    }

    public void OnStreamPeerSendShutdown(TransportStreamId id)
    {
    }

    public void OnStreamShutdownComplete(TransportStreamId id)
    {
    }

    public void OnDatagramCapabilityChanged(bool enabled, int maxPayload)
    {
    }

    public void OnIdealSendBufferSize(TransportStreamId id, ulong bytes)
    {
    }

    public void OnStreamsAvailable(ushort bidirectional, ushort unidirectional)
    {
    }

    public void OnPeerAddressChanged(in TransportConnectedInfo info)
    {
    }

    public void OnClosed(TransportCloseReason reason, ulong errorCode, int transportStatus)
    {
        CloseReason = reason;
        CloseErrorCode = errorCode;
        Closed = true;
    }

    /// <summary>True once the connection is closed.</summary>
    public bool Closed { get; private set; }

    /// <summary>Why the connection closed.</summary>
    public TransportCloseReason CloseReason { get; private set; }

    /// <summary>The error code the close carried.</summary>
    public ulong CloseErrorCode { get; private set; }

    private NativeBuffer Rent(int length)
    {
        var buffer = new NativeBuffer(length);
        lock (_gate) _buffers.Add(buffer);
        return buffer;
    }

    private TransportSegment* RentSegment(TransportSegment value)
    {
        var segments = new NativeSegments(1);
        segments.Set(0, value);
        lock (_gate) _segments.Add(segments);
        return segments.At(0);
    }

    private readonly List<NativeSegments> _segments = [];

    public void Dispose()
    {
        foreach (NativeBuffer buffer in _buffers) buffer.Dispose();
        foreach (NativeSegments segments in _segments) segments.Dispose();
    }
}
