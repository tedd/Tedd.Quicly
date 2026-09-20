using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Http3;
using Tedd.Quicly.Http3.WebTransport;

namespace Tedd.Quicly.Transport.MsQuic.WebTransport;

public sealed unsafe partial class WebTransportTransport
{
    /// <summary>The three unidirectional streams HTTP/3 needs from each end: control, QPACK encoder, QPACK decoder.</summary>
    private const int Http3UniStreamCount = 3;

    /// <summary>What a stream slot is being used for. A peer stream is <see cref="Unclassified"/> until its first varint arrives.</summary>
    private enum StreamRole : byte
    {
        /// <summary>The slot is not in use for this generation.</summary>
        Free = 0,

        /// <summary>One of our own HTTP/3 streams (control or QPACK): never shown to Core.</summary>
        Internal,

        /// <summary>A peer stream whose type has not been read yet.</summary>
        Unclassified,

        /// <summary>The peer's control stream: SETTINGS, GOAWAY and whatever else RFC 9114 allows there.</summary>
        PeerControl,

        /// <summary>A peer stream whose bytes are discarded (QPACK, grease, a refused request stream).</summary>
        Discard,

        /// <summary>The session's CONNECT stream: the Extended CONNECT exchange and then capsules.</summary>
        Connect,

        /// <summary>A WebTransport data stream, shown to Core once its preamble is stripped.</summary>
        Data,
    }

    /// <summary>
    /// One stream of the inner transport as the carrier sees it. Slots mirror the inner transport's, so the index is
    /// <see cref="TransportStreamId.Slot"/> and the generation tells a live slot from a stale id.
    /// </summary>
    private sealed class StreamSlot
    {
        public uint Generation;
        public StreamRole Role;
        public StreamKind Kind;
        public bool Local;

        /// <summary>The stream's id, so a deferred stream can be resumed without a reverse lookup.</summary>
        public TransportStreamId Id;

        /// <summary>Core has seen <see cref="ITransportSink.OnPeerStreamStarted"/> (or opened the stream itself).</summary>
        public bool Exposed;

        /// <summary>
        /// A peer data stream of this session that arrived before the session was established. Its preamble is stripped
        /// and the rest is left unread on the inner transport until <see cref="ITransportSink.OnConnected"/> has been
        /// raised, so nothing is buffered here and nothing reaches Core out of order.
        /// </summary>
        public bool Deferred;

        /// <summary>The deferred stream's preamble carried the FIN, which has to be replayed when it is exposed.</summary>
        public bool DeferredFin;

        /// <summary>Preamble sends whose completions the carrier still has to swallow (they are not Core's).</summary>
        public int PreambleSendsPending;

        /// <summary>The preamble was written (or queued) on a local stream.</summary>
        public bool PreambleQueued;

        /// <summary>Gather array holding this stream's first send (preamble + the caller's segments), or -1.</summary>
        public int FirstSendScratch = -1;

        /// <summary>Bytes of preamble already taken off a peer stream.</summary>
        public int PreambleBytes;

        /// <summary>The preamble bytes seen so far, while it is split across receives.</summary>
        public readonly byte[] Partial = new byte[PreambleCapacity];

        /// <summary>
        /// Leading bytes of <see cref="Partial"/> that classification took off the stream and that the frame reader of a
        /// control or CONNECT stream must see before the next indication.
        /// </summary>
        public int ReplayLength;

        /// <summary>Frame parser of a control or CONNECT stream.</summary>
        public Http3FrameReader Reader;

        /// <summary>Payload of a frame that arrived in fragments.</summary>
        public byte[]? Accumulator;
        public int AccumulatorLength;

        /// <summary>The CONNECT stream has carried its HEADERS frame.</summary>
        public bool HeadersSeen;
    }

    private StreamSlot?[] _streamSlots = [];
    private readonly Lock _streamLock = new();

    /// <summary>Preamble bytes, <see cref="PreambleCapacity"/> per slot, kept alive until the preamble send completes.</summary>
    private NativeArray<byte> _preambleBytes = null!;

    /// <summary>One gather segment per slot, describing that slot's preamble on its own (a bare <see cref="StartStream"/>).</summary>
    private NativeArray<TransportSegment> _preambleSegments = null!;

    /// <summary>Gather arrays of streams' first sends: preamble plus the caller's segments.</summary>
    private NativeArray<TransportSegment> _firstSendScratch = null!;

    private int[] _firstSendState = [];
    private int _firstSendHint;
    private int _firstSendStride;

    private void InitialiseStreams(WebTransportOptions options)
    {
        _streamSlots = new StreamSlot?[options.MaxStreams];
        _preambleBytes = new NativeArray<byte>(options.MaxStreams * PreambleCapacity);
        _preambleSegments = new NativeArray<TransportSegment>(options.MaxStreams);
        _firstSendStride = options.MaxStreamSegments + 1;
        _firstSendScratch = new NativeArray<TransportSegment>(options.MaxConcurrentStreamStarts * _firstSendStride);
        _firstSendState = new int[options.MaxConcurrentStreamStarts];
    }

    private void DisposeStreams()
    {
        _firstSendScratch.Dispose();
        _preambleSegments.Dispose();
        _preambleBytes.Dispose();
    }

    // ---------------------------------------------------------------- slot table

    /// <summary>The slot for <paramref name="id"/>, re-stamped when the inner transport reused it for a new stream.</summary>
    private StreamSlot? EnsureSlot(TransportStreamId id, StreamRole role, StreamKind kind, bool local)
    {
        if ((uint)id.Slot >= (uint)_streamSlots.Length)
        {
            Diagnostic(TransportDiagnosticLevel.Error, $"Inner stream slot {id.Slot} is outside the carrier's table of {_streamSlots.Length}; raise WebTransportOptions.MaxStreams.");
            return null;
        }

        lock (_streamLock)
        {
            StreamSlot? slot = _streamSlots[id.Slot];
            if (slot is null)
            {
                slot = new StreamSlot();
                _streamSlots[id.Slot] = slot;
            }
            else if (slot.Generation != id.Generation)
            {
                ResetSlot(slot);
            }

            slot.Generation = id.Generation;
            slot.Id = id;
            slot.Role = role;
            slot.Kind = kind;
            slot.Local = local;
            if (role is StreamRole.PeerControl or StreamRole.Connect) slot.Reader = new Http3FrameReader((ulong)MaxFrameLengthFor(role));
            return slot;
        }
    }

    private static void ResetSlot(StreamSlot slot)
    {
        slot.Role = StreamRole.Free;
        slot.Exposed = false;
        slot.Deferred = false;
        slot.DeferredFin = false;
        slot.PreambleSendsPending = 0;
        slot.PreambleQueued = false;
        slot.PreambleBytes = 0;
        slot.AccumulatorLength = 0;
        slot.HeadersSeen = false;
    }

    private int MaxFrameLengthFor(StreamRole role) => role == StreamRole.Connect ? _options.MaxCapsuleLength : _options.MaxControlFrameLength;

    /// <summary>The live slot for <paramref name="id"/>, or null when the id is stale or the slot is free.</summary>
    private StreamSlot? Lookup(TransportStreamId id)
    {
        if ((uint)id.Slot >= (uint)_streamSlots.Length) return null;
        StreamSlot? slot = Volatile.Read(ref _streamSlots[id.Slot]);
        if (slot is null) return null;
        lock (_streamLock)
        {
            return slot.Generation == id.Generation && slot.Role != StreamRole.Free ? slot : null;
        }
    }

    // ---------------------------------------------------------------- ITransport: streams

    /// <inheritdoc/>
    public TransportStatus OpenStream(StreamKind kind, ulong context, ushort priority, out TransportStreamId id)
    {
        id = TransportStreamId.None;
        ITransport? inner = ConnectedInner();
        if (inner is null) return TransportStatus.InvalidState;

        TransportStatus status = inner.OpenStream(kind, context, priority, out id);
        if (status != TransportStatus.Success) return status;

        // OpenStream only allocates, so no callback can reach the slot before it is published here.
        if (EnsureSlot(id, StreamRole.Data, kind, local: true) is not { } slot)
        {
            inner.CloseStream(id);
            id = TransportStreamId.None;
            return TransportStatus.OutOfMemory;
        }

        slot.Exposed = true;
        return TransportStatus.Success;
    }

    /// <inheritdoc/>
    public TransportStatus StartStream(TransportStreamId id)
    {
        ITransport? inner = ConnectedInner();
        if (inner is null) return TransportStatus.InvalidState;
        if (Lookup(id) is not { Role: StreamRole.Data, Local: true } slot) return TransportStatus.InvalidState;
        if (slot.PreambleQueued) return inner.StartStream(id);
        return QueuePreamble(inner, slot, id);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// The first send of a stream carries the WebTransport preamble as one extra gather segment in front of the caller's
    /// (its buffers are never copied), so one call of Core's is always one send of the inner transport's: the status, the
    /// flags and the completion all mean exactly what the contract says, refusals included.
    /// </remarks>
    public TransportStatus SendStream(TransportStreamId id, TransportSegment* segments, int count, ulong context, TransportSendFlags flags)
    {
        ITransport? inner = ConnectedInner();
        if (inner is null) return TransportStatus.InvalidState;
        if (Lookup(id) is not { Role: StreamRole.Data } slot) return TransportStatus.InvalidState;
        // A peer stream carries the peer's preamble, and a local stream needs its own only once.
        if (!slot.Local || slot.PreambleQueued) return inner.SendStream(id, segments, count, context, flags);

        if (!_sessionIdKnown) return TransportStatus.InvalidState;
        if (count < 0 || count > _firstSendStride - 1) return TransportStatus.TooLarge;

        int scratch = RentSlot(_firstSendState, ref _firstSendHint);
        if (scratch < 0) return TransportStatus.OutOfMemory;

        TransportSegment* gather = _firstSendScratch.Pointer + ((long)scratch * _firstSendStride);
        if (!WritePreamble(slot, id, out TransportSegment preamble))
        {
            ReturnSlot(_firstSendState, scratch);
            return TransportStatus.Failed;
        }

        gather[0] = preamble;
        for (int i = 0; i < count; i++) gather[i + 1] = segments[i];

        // The completion can arrive inline, so the slot owns the scratch before the send is made.
        lock (_streamLock)
        {
            slot.FirstSendScratch = scratch;
            slot.PreambleQueued = true;
        }

        TransportStatus status = inner.SendStream(id, gather, count + 1, context, flags);
        if (status != TransportStatus.Success)
        {
            // No completion follows a refused send, so the scratch comes back here.
            lock (_streamLock)
            {
                if (slot.FirstSendScratch == scratch)
                {
                    slot.FirstSendScratch = -1;
                    ReturnSlot(_firstSendState, scratch);
                }

                slot.PreambleQueued = false;
            }
        }

        return status;
    }

    /// <summary>Writes the stream's WebTransport preamble into its slot's reserved bytes.</summary>
    private bool WritePreamble(StreamSlot slot, TransportStreamId id, out TransportSegment segment)
    {
        segment = default;
        byte* buffer = _preambleBytes.Pointer + ((long)id.Slot * PreambleCapacity);
        var span = new Span<byte>(buffer, PreambleCapacity);
        int written = slot.Kind == StreamKind.Bidirectional
            ? WebTransportFraming.WriteBidirectionalPreamble(span, _sessionId)
            : WebTransportFraming.WriteUnidirectionalPreamble(span, _sessionId);
        if (written < 0) return false;
        segment = new TransportSegment(buffer, written);
        return true;
    }

    /// <summary>Sends the preamble on its own, which is what starts a stream that has no data yet.</summary>
    private TransportStatus QueuePreamble(ITransport inner, StreamSlot slot, TransportStreamId id)
    {
        if (!_sessionIdKnown) return TransportStatus.InvalidState;
        if (!WritePreamble(slot, id, out TransportSegment preamble)) return TransportStatus.Failed;

        TransportSegment* segment = _preambleSegments.Pointer + id.Slot;
        *segment = preamble;

        // The completion can arrive inline, so the slot says "swallow one" before the send is made.
        lock (_streamLock)
        {
            slot.PreambleSendsPending++;
            slot.PreambleQueued = true;
        }

        TransportStatus status = inner.SendStream(id, segment, 1, PreambleContext(id), TransportSendFlags.Start);
        if (status != TransportStatus.Success)
        {
            lock (_streamLock)
            {
                if (slot.PreambleSendsPending > 0) slot.PreambleSendsPending--;
                slot.PreambleQueued = false;
            }
        }

        return status;
    }

    /// <summary>A context of the carrier's own preamble sends; never handed to Core.</summary>
    private static ulong PreambleContext(TransportStreamId id) => ((ulong)id.Generation << 32) | (uint)id.Slot;

    /// <inheritdoc/>
    /// <remarks>The error code is mapped into the WebTransport range of the HTTP/3 error space (draft §4.3).</remarks>
    public void AbortStream(TransportStreamId id, ulong errorCode, StreamAbortDirection direction)
    {
        ITransport? inner = _inner;
        if (inner is null) return;
        if (Lookup(id) is not { Role: StreamRole.Data }) return;
        inner.AbortStream(id, WebTransportErrorCode.ToHttp3(ToApplicationCode(errorCode)), direction);
    }

    /// <inheritdoc/>
    public void SetStreamPriority(TransportStreamId id, ushort priority)
    {
        ITransport? inner = _inner;
        if (inner is null) return;
        if (Lookup(id) is not { Role: StreamRole.Data }) return;
        inner.SetStreamPriority(id, priority);
    }

    /// <inheritdoc/>
    public long GetQuicStreamId(TransportStreamId id)
    {
        ITransport? inner = _inner;
        if (inner is null) return -1;
        if (Lookup(id) is not { Role: StreamRole.Data }) return -1;
        return inner.GetQuicStreamId(id);
    }

    /// <inheritdoc/>
    public void ResumeStreamReceive(TransportStreamId id, int bytesConsumed)
    {
        ITransport? inner = _inner;
        if (inner is null) return;
        if (Lookup(id) is not { Role: StreamRole.Data }) return;
        inner.ResumeStreamReceive(id, bytesConsumed);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// The slot is not released here: pending sends still report their completions, and the inner transport recycles the
    /// id with a fresh generation, which is what makes the slot free again.
    /// </remarks>
    public void CloseStream(TransportStreamId id)
    {
        ITransport? inner = _inner;
        if (inner is null) return;
        if (Lookup(id) is not { Role: StreamRole.Data }) return;
        inner.CloseStream(id);
    }

    /// <inheritdoc/>
    /// <remarks>The carrier asks the inner transport for its own HTTP/3 streams on top of what Core wants.</remarks>
    public void UpdatePeerStreamLimits(ushort bidirectional, ushort unidirectional)
    {
        ITransport? inner = _inner;
        inner?.UpdatePeerStreamLimits(AddOverhead(bidirectional, _isClient ? 0 : 1), AddOverhead(unidirectional, Http3UniStreamCount));
    }

    private static ushort AddOverhead(ushort value, int overhead)
    {
        int raised = value + overhead;
        return raised >= ushort.MaxValue ? ushort.MaxValue : (ushort)raised;
    }

    // ---------------------------------------------------------------- inner stream callbacks

    /// <inheritdoc/>
    void ITransportSink.OnPeerStreamStarted(TransportStreamId id, StreamKind kind)
    {
        // Nothing is shown to Core until the stream's first varint says what it is.
        EnsureSlot(id, StreamRole.Unclassified, kind, local: false);
    }

    /// <inheritdoc/>
    void ITransportSink.OnStreamStarted(TransportStreamId id, ulong context, TransportStatus status)
    {
        StreamSlot? slot = Lookup(id);
        if (slot is null) return;

        if (slot.Role == StreamRole.Connect && _isClient && status == TransportStatus.Success)
        {
            OnConnectStreamStarted(id);
            return;
        }

        if (!slot.Exposed) return;
        _sink?.OnStreamStarted(id, context, status);
    }

    /// <inheritdoc/>
    public ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin)
    {
        StreamSlot? slot = Lookup(id);
        if (slot is null) return ReceiveResult.Consumed(TotalLength(segments));

        switch (slot.Role)
        {
            case StreamRole.Data:
                return ReceiveData(slot, id, segments, absoluteOffset, fin);
            case StreamRole.Unclassified:
                return ClassifyPeerStream(slot, id, segments, fin);
            case StreamRole.PeerControl:
            case StreamRole.Connect:
                return ReceiveHttp3(slot, id, segments, fin);
            default:
                return ReceiveResult.Consumed(TotalLength(segments));
        }
    }

    /// <summary>Data of a WebTransport stream: the preamble is stripped, then the bytes are Core's.</summary>
    private ReceiveResult ReceiveData(StreamSlot slot, TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin)
    {
        ITransportSink? sink = _sink;
        if (sink is null) return ReceiveResult.Consumed(TotalLength(segments));

        // A stream held back until the session exists: leave its bytes where they are rather than showing Core a stream
        // it has not been told about. AdoptRole already pended it; this only catches an indication already in flight.
        if (!slot.Exposed) return ReceiveResult.PendingAfter(0);

        ReceiveResult result = sink.OnStreamReceived(id, segments, absoluteOffset - (ulong)slot.PreambleBytes, fin);
        return result;
    }

    /// <summary>
    /// Reads the first varints of a peer stream to decide what it is. Only those bytes are consumed; the transport
    /// re-indicates the rest at once, by which time the stream has its role and its data path.
    /// </summary>
    private ReceiveResult ClassifyPeerStream(StreamSlot slot, TransportStreamId id, ReadOnlySpan<TransportSegment> segments, bool fin)
    {
        int buffered = slot.PreambleBytes;
        Span<byte> window = slot.Partial;
        int copied = CopyInto(window.Slice(buffered), segments);
        int available = buffered + copied;

        ReadOnlySpan<byte> header = window.Slice(0, available);
        if (!Http3FrameReader.TryPeekType(header, out ulong first, out int firstLength))
        {
            if (available >= PreambleCapacity)
            {
                // Sixteen bytes cannot fail to hold two varints; the peer is not speaking HTTP/3.
                CloseWithHttp3Error(Http3ErrorCode.StreamCreationError, "The peer's stream does not start with a valid varint.");
                return ReceiveResult.Consumed(copied);
            }

            slot.PreambleBytes = available;
            return ReceiveResult.Consumed(copied);
        }

        if (slot.Kind == StreamKind.Unidirectional) return ClassifyUnidirectional(slot, id, first, firstLength, header, buffered, copied, fin);
        return ClassifyBidirectional(slot, id, first, segments, buffered, copied, fin);
    }

    private ReceiveResult ClassifyUnidirectional(StreamSlot slot, TransportStreamId id, ulong type, int typeLength, ReadOnlySpan<byte> header, int buffered, int copied, bool fin)
    {
        switch ((Http3StreamType)type)
        {
            case Http3StreamType.Control:
                return AdoptRole(slot, id, StreamRole.PeerControl, typeLength, buffered, copied, fin, exposeAsData: false);
            case Http3StreamType.QpackEncoder:
            case Http3StreamType.QpackDecoder:
                return AdoptRole(slot, id, StreamRole.Discard, typeLength, buffered, copied, fin, exposeAsData: false);
            case Http3StreamType.Push:
                CloseWithHttp3Error(Http3ErrorCode.StreamCreationError, "A server push stream arrived; pushes are not enabled.");
                return ReceiveResult.Consumed(copied);
            case Http3StreamType.WebTransport:
                return AdoptWebTransportStream(slot, id, typeLength, header, buffered, copied, fin);
            default:
                // Unknown stream types, grease included, are discarded (RFC 9114 §6.2).
                return AdoptRole(slot, id, StreamRole.Discard, typeLength, buffered, copied, fin, exposeAsData: false);
        }
    }

    private ReceiveResult ClassifyBidirectional(StreamSlot slot, TransportStreamId id, ulong signal, ReadOnlySpan<TransportSegment> segments, int buffered, int copied, bool fin)
    {
        if (signal == WebTransportFraming.BidirectionalSignal)
        {
            ReadOnlySpan<byte> header = slot.Partial.AsSpan(0, buffered + copied);
            Http3FrameReader.TryPeekType(header, out _, out int signalLength);
            return AdoptWebTransportStream(slot, id, signalLength, header, buffered, copied, fin);
        }


        if (!_isClient && !_connectSettled && signal == (ulong)Http3FrameType.Headers)
        {
            // A request stream: the session's Extended CONNECT. None of the frame is the carrier's, so the frame reader
            // takes the stream from byte zero — including the bytes classification peeked at in earlier indications.
            if (EnsureSlot(id, StreamRole.Connect, slot.Kind, local: false) is not { } connect) return ReceiveResult.Consumed(copied);
            connect.ReplayLength = buffered;
            connect.PreambleBytes = 0;
            RecordConnectStream(id);
            return ReceiveHttp3(connect, id, segments, fin);
        }

        if (!_isClient && _extraRequestStreams < _options.MaxExtraRequestStreams)
        {
            _extraRequestStreams++;
            _inner?.AbortStream(id, (ulong)Http3ErrorCode.RequestRejected, StreamAbortDirection.Both);
            return AdoptRole(slot, id, StreamRole.Discard, 0, buffered, copied, fin, exposeAsData: false);
        }

        CloseWithHttp3Error(Http3ErrorCode.StreamCreationError, "The peer opened an unexpected bidirectional stream.");
        return ReceiveResult.Consumed(copied);
    }

    /// <summary>A WebTransport stream: after the signal comes the session id, and then the bytes are Core's.</summary>
    private ReceiveResult AdoptWebTransportStream(StreamSlot slot, TransportStreamId id, int signalLength, ReadOnlySpan<byte> header, int buffered, int copied, bool fin)
    {
        WebTransportPreambleStatus status = WebTransportFraming.TryReadSessionId(header.Slice(signalLength), out ulong sessionId, out int sessionLength);
        if (status == WebTransportPreambleStatus.NeedMoreData)
        {
            slot.PreambleBytes = buffered + copied;
            return ReceiveResult.Consumed(copied);
        }

        if (status != WebTransportPreambleStatus.Ok)
        {
            CloseWithHttp3Error(Http3ErrorCode.IdError, "A WebTransport stream named an invalid session id.");
            return ReceiveResult.Consumed(copied);
        }

        if (!_sessionIdKnown || sessionId != _sessionId)
        {
            // Streams for a session we do not have are reset at once, never buffered (PROTOCOL.md §5).
            _inner?.AbortStream(id, WebTransportErrorCode.ToHttp3(0), StreamAbortDirection.Both);
            return AdoptRole(slot, id, StreamRole.Discard, signalLength + sessionLength, buffered, copied, fin, exposeAsData: false);
        }

        // The id is known as soon as the CONNECT request is seen, which is before the peer's SETTINGS have arrived and
        // the session has been established. A peer may open streams optimistically in that window, and they are this
        // session's, so they are neither reset nor delivered: the preamble is taken and the rest is left unread on the
        // inner transport until OnConnected has been raised. Nothing is buffered, and Core sees the stream in order.
        if (!IsSessionEstablished)
        {
            return AdoptRole(
                slot, id, StreamRole.Data, signalLength + sessionLength, buffered, copied, fin, exposeAsData: false, defer: true);
        }

        return AdoptRole(slot, id, StreamRole.Data, signalLength + sessionLength, buffered, copied, fin, exposeAsData: true);
    }

    /// <summary>
    /// Settles a peer stream's role once its preamble is complete: only the preamble bytes of this indication are
    /// consumed, so the transport re-indicates the data with the stream already in its final state.
    /// </summary>
    private ReceiveResult AdoptRole(
        StreamSlot slot, TransportStreamId id, StreamRole role, int preambleLength, int buffered, int copied, bool fin, bool exposeAsData, bool defer = false)
    {
        // How much of THIS indication the preamble took; earlier indications already paid for `buffered`.
        int consumedHere = preambleLength - buffered;
        if (consumedHere < 0) consumedHere = 0;
        if (consumedHere > copied) consumedHere = copied;

        lock (_streamLock)
        {
            slot.Role = role;
            slot.PreambleBytes = preambleLength;
            slot.Exposed = exposeAsData;
            slot.Deferred = defer;
            slot.DeferredFin = defer && consumedHere == copied && fin;
            if (role is StreamRole.PeerControl or StreamRole.Connect) slot.Reader = new Http3FrameReader((ulong)MaxFrameLengthFor(role));
        }

        // Stop the inner transport indicating this stream; ReleaseDeferredStreams resumes it once the session exists.
        if (defer) return ReceiveResult.PendingAfter(consumedHere);

        if (exposeAsData)
        {
            ITransportSink? sink = _sink;
            sink?.OnPeerStreamStarted(id, slot.Kind);

            // A preamble-only indication that also carried the FIN has nothing left to re-indicate, so the FIN is
            // passed on here instead.
            if (consumedHere == copied && fin && sink is not null)
            {
                sink.OnStreamReceived(id, default, 0, true);
            }
        }

        return ReceiveResult.Consumed(consumedHere);
    }

    /// <summary>
    /// Hands over the peer data streams that arrived before the session existed, in the order they were adopted, and
    /// lets the inner transport indicate their bytes again. Called once, straight after
    /// <see cref="ITransportSink.OnConnected"/>, so Core learns of these streams after the connection and never before.
    /// </summary>
    private void ReleaseDeferredStreams()
    {
        ITransportSink? sink = _sink;
        ITransport? inner = _inner;
        if (sink is null || inner is null) return;

        StreamSlot?[] slots = _streamSlots;
        for (int i = 0; i < slots.Length; i++)
        {
            StreamSlot? slot = Volatile.Read(ref slots[i]);
            if (slot is null) continue;

            TransportStreamId id;
            StreamKind kind;
            bool fin;
            lock (_streamLock)
            {
                if (!slot.Deferred || slot.Role != StreamRole.Data) continue;
                slot.Deferred = false;
                slot.Exposed = true;
                id = slot.Id;
                kind = slot.Kind;
                fin = slot.DeferredFin;
                slot.DeferredFin = false;
            }

            sink.OnPeerStreamStarted(id, kind);
            if (fin)
            {
                // The preamble indication carried the FIN, so there is nothing left to re-indicate.
                sink.OnStreamReceived(id, default, 0, true);
                continue;
            }

            inner.ResumeStreamReceive(id, 0);
        }
    }

    /// <summary>Copies up to <paramref name="destination"/>.Length bytes from the front of the segment array.</summary>
    private static int CopyInto(Span<byte> destination, ReadOnlySpan<TransportSegment> segments)
    {
        int written = 0;
        for (int i = 0; i < segments.Length && written < destination.Length; i++)
        {
            ReadOnlySpan<byte> source = segments[i].AsSpan();
            int take = Math.Min(source.Length, destination.Length - written);
            source.Slice(0, take).CopyTo(destination.Slice(written));
            written += take;
        }

        return written;
    }

    private static int TotalLength(ReadOnlySpan<TransportSegment> segments)
    {
        long total = 0;
        for (int i = 0; i < segments.Length; i++) total += segments[i].Length;
        return (int)total;
    }

    /// <inheritdoc/>
    void ITransportSink.OnStreamSendCompleted(TransportStreamId id, ulong context, bool canceled)
    {
        StreamSlot? slot = Lookup(id);
        if (slot is null || !slot.Exposed) return;

        // Sends on one stream complete in order, so the first completion is the one the carrier put in front.
        lock (_streamLock)
        {
            if (slot.PreambleSendsPending > 0)
            {
                // A bare StartStream sent the preamble on its own; that completion is not Core's.
                slot.PreambleSendsPending--;
                return;
            }

            if (slot.FirstSendScratch >= 0)
            {
                // The first send gathered the preamble in front of Core's segments; the completion is Core's.
                ReturnSlot(_firstSendState, slot.FirstSendScratch);
                slot.FirstSendScratch = -1;
            }
        }

        _sink?.OnStreamSendCompleted(id, context, canceled);
    }

    /// <inheritdoc/>
    void ITransportSink.OnStreamAborted(TransportStreamId id, ulong errorCode, StreamAbortDirection direction)
    {
        StreamSlot? slot = Lookup(id);
        if (slot is null) return;

        if (slot.Role == StreamRole.Connect)
        {
            CloseSessionFromPeer("The peer aborted the CONNECT stream.");
            return;
        }

        if (!slot.Exposed) return;
        ulong code = WebTransportErrorCode.TryFromHttp3(errorCode, out uint application) ? application : errorCode;
        _sink?.OnStreamAborted(id, code, direction);
    }

    /// <inheritdoc/>
    void ITransportSink.OnStreamPeerSendShutdown(TransportStreamId id)
    {
        StreamSlot? slot = Lookup(id);
        if (slot is null) return;

        if (slot.Role == StreamRole.Connect)
        {
            CloseSessionFromPeer("The peer closed the CONNECT stream.");
            return;
        }

        if (!slot.Exposed) return;
        _sink?.OnStreamPeerSendShutdown(id);
    }

    /// <inheritdoc/>
    void ITransportSink.OnStreamShutdownComplete(TransportStreamId id)
    {
        StreamSlot? slot = Lookup(id);
        if (slot is null) return;

        bool exposed = slot.Exposed;
        if (!exposed)
        {
            // The carrier owns this stream: release it here, Core never hears of it.
            lock (_streamLock) ResetSlot(slot);
            _inner?.CloseStream(id);
            return;
        }

        _sink?.OnStreamShutdownComplete(id);
    }

    /// <inheritdoc/>
    void ITransportSink.OnIdealSendBufferSize(TransportStreamId id, ulong bytes)
    {
        StreamSlot? slot = Lookup(id);
        if (slot is null || !slot.Exposed) return;
        _sink?.OnIdealSendBufferSize(id, bytes);
    }
}
