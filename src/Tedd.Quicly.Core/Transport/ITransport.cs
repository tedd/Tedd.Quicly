namespace Tedd.Quicly.Core.Transport;

/// <summary>
/// The thin contract every carrier (MsQuic raw QUIC, WebTransport over HTTP/3, browser WebTransport, the simulated
/// test link) implements. All game semantics live above it. See ARCHITECTURE section 2.1 and ADR 0008.
/// </summary>
/// <remarks>
/// Lifetime rules: buffers and the segment array passed to a send MUST stay valid and unmodified until the matching
/// completion (<see cref="ITransportSink.OnStreamSendCompleted"/>, or <see cref="ITransportSink.OnDatagramSendStateChanged"/>
/// with a state that releases the payload). If a send call returns anything but <see cref="TransportStatus.Success"/>
/// no completion follows. If it returns <see cref="TransportStatus.Success"/> a completion always follows, and it may
/// arrive on the transport thread before the call returns. Contexts are opaque 64-bit values chosen by the caller;
/// Core uses <c>(generation &lt;&lt; 32) | slot</c>.
/// </remarks>
public unsafe interface ITransport : IDisposable
{
    /// <summary>What this transport can do right now.</summary>
    TransportCapabilities Capabilities { get; }

    /// <summary>Coarse connection state.</summary>
    TransportState State { get; }

    /// <summary>Sends one datagram made of <paramref name="count"/> gathered segments (one QUIC DATAGRAM frame).</summary>
    TransportStatus SendDatagram(TransportSegment* segments, int count, ulong context, TransportSendFlags flags);

    /// <summary>
    /// Allocates a local stream. The stream is started by the first <see cref="SendStream"/> carrying
    /// <see cref="TransportSendFlags.Start"/> (or by an explicit <see cref="StartStream"/>).
    /// </summary>
    TransportStatus OpenStream(StreamKind kind, ulong context, ushort priority, out TransportStreamId id);

    /// <summary>Starts a stream explicitly (only needed when the first send should not carry <see cref="TransportSendFlags.Start"/>).</summary>
    TransportStatus StartStream(TransportStreamId id);

    /// <summary>Queues gathered segments on a stream. <see cref="TransportSendFlags.Fin"/> closes our sending side after them.</summary>
    TransportStatus SendStream(TransportStreamId id, TransportSegment* segments, int count, ulong context, TransportSendFlags flags);

    /// <summary>Aborts one or both directions of a stream with an application error code.</summary>
    void AbortStream(TransportStreamId id, ulong errorCode, StreamAbortDirection direction);

    /// <summary>Sets the scheduling priority of a stream (0 lowest, 65535 highest, 32767 default).</summary>
    void SetStreamPriority(TransportStreamId id, ushort priority);

    /// <summary>The QUIC-level stream id (valid after <see cref="ITransportSink.OnStreamStarted"/>), or -1.</summary>
    long GetQuicStreamId(TransportStreamId id);

    /// <summary>
    /// Resumes delivery on a stream after a receive returned <see cref="ReceiveResult.PendingAfter"/>, crediting
    /// <paramref name="bytesConsumed"/> additional bytes.
    /// </summary>
    void ResumeStreamReceive(TransportStreamId id, int bytesConsumed);

    /// <summary>Releases the local stream slot once the sink has seen <see cref="ITransportSink.OnStreamShutdownComplete"/>.</summary>
    void CloseStream(TransportStreamId id);

    /// <summary>Raises the number of streams the peer may open (called after admission).</summary>
    void UpdatePeerStreamLimits(ushort bidirectional, ushort unidirectional);

    /// <summary>Closes the connection with an application error code and an optional reason phrase (at most 512 bytes of UTF-8).</summary>
    void Close(ulong errorCode, ReadOnlySpan<byte> reason);

    /// <summary>Copies the current statistics into <paramref name="statistics"/>. Never allocates.</summary>
    void GetStatistics(out TransportStatistics statistics);
}

/// <summary>
/// Callbacks a transport raises. Implemented by Core (the peer). Called on the transport's threads, serialised per
/// connection (never concurrently for one transport, but not necessarily on one OS thread). Implementations MUST NOT
/// throw, block, or call <see cref="ITransport.Close"/> from inside a callback.
/// </summary>
public interface ITransportSink
{
    /// <summary>The handshake completed.</summary>
    void OnConnected(in TransportConnectedInfo info);

    /// <summary>A datagram arrived. <paramref name="payload"/> is valid only during the call.</summary>
    void OnDatagramReceived(ReadOnlySpan<byte> payload);

    /// <summary>The peer opened a stream.</summary>
    void OnPeerStreamStarted(TransportStreamId id, StreamKind kind);

    /// <summary>A locally opened stream finished starting; <paramref name="status"/> is <see cref="TransportStatus.Success"/> or the failure.</summary>
    void OnStreamStarted(TransportStreamId id, ulong context, TransportStatus status);

    /// <summary>
    /// Stream data arrived as one or more segments (a frame may be split across segments and across calls).
    /// Return how many bytes were consumed and whether the remainder is held back.
    /// </summary>
    ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin);

    /// <summary>A stream send completed; its segment array and buffers may be released. <paramref name="canceled"/> when the data was not delivered.</summary>
    void OnStreamSendCompleted(TransportStreamId id, ulong context, bool canceled);

    /// <summary>A datagram send changed state (see <see cref="DatagramSendState"/>); reported until a final state.</summary>
    void OnDatagramSendStateChanged(ulong context, DatagramSendState state);

    /// <summary>The peer aborted a direction of a stream.</summary>
    void OnStreamAborted(TransportStreamId id, ulong errorCode, StreamAbortDirection direction);

    /// <summary>The peer closed its sending side gracefully (also signalled by <c>fin</c> on the last receive).</summary>
    void OnStreamPeerSendShutdown(TransportStreamId id);

    /// <summary>The stream is fully shut down in both directions; the sink should call <see cref="ITransport.CloseStream"/>.</summary>
    void OnStreamShutdownComplete(TransportStreamId id);

    /// <summary>Datagram support or the maximum payload changed. May be raised before <see cref="OnConnected"/>.</summary>
    void OnDatagramCapabilityChanged(bool enabled, int maxPayload);

    /// <summary>The transport recommends keeping about <paramref name="bytes"/> outstanding on the stream.</summary>
    void OnIdealSendBufferSize(TransportStreamId id, ulong bytes);

    /// <summary>The peer raised our stream limits.</summary>
    void OnStreamsAvailable(ushort bidirectional, ushort unidirectional);

    /// <summary>The peer's address changed (migration / NAT rebind).</summary>
    void OnPeerAddressChanged(in TransportConnectedInfo info);

    /// <summary>The connection is closed. No further callbacks follow.</summary>
    void OnClosed(TransportCloseReason reason, ulong errorCode, int transportStatus);
}
