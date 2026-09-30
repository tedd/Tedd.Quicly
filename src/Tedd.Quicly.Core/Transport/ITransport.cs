namespace Tedd.Quicly.Core.Transport;

/// <summary>
/// The thin contract every carrier (MsQuic raw QUIC, WebTransport over HTTP/3, browser WebTransport, the simulated
/// test link) implements. All game semantics live above it. See ARCHITECTURE section 2.1 and ADR 0008.
/// </summary>
/// <remarks>
/// <para>Lifetime rules: buffers and the segment array passed to a send MUST stay valid and unmodified until the matching
/// completion (<see cref="ITransportSink.OnStreamSendCompleted"/>, or <see cref="ITransportSink.OnDatagramSendStateChanged"/>
/// with a state that releases the payload). If a send call returns anything but <see cref="TransportStatus.Success"/>
/// no completion follows. If it returns <see cref="TransportStatus.Success"/> a completion always follows, and it may
/// arrive on the transport thread before the call returns. Contexts are opaque 64-bit values chosen by the caller;
/// Core uses <c>(generation &lt;&lt; 32) | slot</c>.</para>
/// <para>Threading: a call made from inside a callback may raise further callbacks of the same transport before it returns
/// (MsQuic executes such calls inline), so publish state before calling. A few members may wait for the transport's own
/// thread (on MsQuic the parameter calls: <see cref="SetStreamPriority"/> on a started stream,
/// <see cref="UpdatePeerStreamLimits"/>, <see cref="Close"/> with a reason, <see cref="GetStatistics"/>); never call those
/// from a callback of another transport, which may share that thread. Every other member returns without waiting.</para>
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
    /// <remarks>
    /// <para>A start beyond the peer's stream limit is refused, either synchronously (the call returns
    /// <see cref="TransportStatus.StreamLimitReached"/> and nothing follows for the stream) or asynchronously (the call returns
    /// <see cref="TransportStatus.Success"/>; then <see cref="ITransportSink.OnStreamStarted"/> reports
    /// <see cref="TransportStatus.StreamLimitReached"/>, every send accepted together with the start completes canceled, and
    /// <see cref="ITransportSink.OnStreamShutdownComplete"/> follows, in that order). MsQuic and the simulator always refuse
    /// asynchronously.</para>
    /// <para>A refused stream never starts: <see cref="StartStream"/>, and a send carrying <see cref="TransportSendFlags.Start"/>,
    /// return <see cref="TransportStatus.InvalidState"/> on it, also after <see cref="ITransportSink.OnStreamsAvailable"/>, and
    /// the peer never hears of it. Release it with <see cref="CloseStream"/> (after its
    /// <see cref="ITransportSink.OnStreamShutdownComplete"/> when the refusal was asynchronous) and open a new stream to retry
    /// once <see cref="ITransportSink.OnStreamsAvailable"/> reports credit.</para>
    /// </remarks>
    TransportStatus StartStream(TransportStreamId id);

    /// <summary>Queues gathered segments on a stream. <see cref="TransportSendFlags.Fin"/> closes our sending side after them.</summary>
    /// <remarks>
    /// With <see cref="TransportSendFlags.Start"/> on a stream that was not started yet, the start behaves as
    /// <see cref="StartStream"/>: when it is refused for the peer's stream limit asynchronously, the send is accepted and
    /// completes canceled; when synchronously, the call returns <see cref="TransportStatus.StreamLimitReached"/>.
    /// </remarks>
    TransportStatus SendStream(TransportStreamId id, TransportSegment* segments, int count, ulong context, TransportSendFlags flags);

    /// <summary>Aborts one or both directions of a stream with an application error code.</summary>
    /// <remarks>
    /// Directions the stream does not have are ignored. Aborting a local stream that was never started (no
    /// <see cref="StartStream"/>, no send with <see cref="TransportSendFlags.Start"/>) releases it like <see cref="CloseStream"/>:
    /// no callback follows for it and its id is stale afterwards.
    /// </remarks>
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
    /// <remarks>
    /// May be called from inside a callback (typically from <see cref="ITransportSink.OnStreamShutdownComplete"/>). Called
    /// earlier, it aborts both directions with error code 0: completions of pending sends are still reported (canceled),
    /// <see cref="ITransportSink.OnStreamShutdownComplete"/> is not. The id is stale afterwards; stale ids are ignored.
    /// </remarks>
    void CloseStream(TransportStreamId id);

    /// <summary>Raises the number of streams the peer may open (called after admission).</summary>
    /// <remarks>
    /// The peer sees <see cref="ITransportSink.OnStreamsAvailable"/> once the new limit arrives. QUIC never takes granted
    /// stream credit back: a lower value takes nothing away (MsQuic only limits the credit it grants later; the simulator
    /// ignores a lower value once the old limit can have reached the peer).
    /// </remarks>
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
    /// <remarks>
    /// <see cref="TransportStatus.StreamLimitReached"/> means the peer's stream limit refused the start: the stream never
    /// starts, sends accepted with the start complete canceled and <see cref="OnStreamShutdownComplete"/> follows; release it
    /// with <see cref="ITransport.CloseStream"/> and open a new stream to retry (see <see cref="ITransport.StartStream"/>).
    /// </remarks>
    void OnStreamStarted(TransportStreamId id, ulong context, TransportStatus status);

    /// <summary>
    /// Stream data arrived as one or more segments (a frame may be split across segments and across calls).
    /// Return how many bytes were consumed and whether the remainder is held back.
    /// </summary>
    /// <remarks>
    /// <paramref name="fin"/> is the end of the stream, and it is shown once: a sink that holds an indication carrying it
    /// with no byte left unconsumed gets no further indication after the resume, only
    /// <see cref="OnStreamPeerSendShutdown"/> (see <see cref="ReceiveResult"/>).
    /// </remarks>
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

    /// <summary>
    /// Datagram support or the maximum payload changed. May be raised before or after <see cref="OnConnected"/>.
    /// An explicit report establishes support or non-support; a negative connection snapshot may precede negotiation.
    /// </summary>
    void OnDatagramCapabilityChanged(bool enabled, int maxPayload);

    /// <summary>The transport recommends keeping about <paramref name="bytes"/> outstanding on the stream.</summary>
    void OnIdealSendBufferSize(TransportStreamId id, ulong bytes);

    /// <summary>The peer raised our stream limits (or returned stream credit). Only raised after <see cref="OnConnected"/>.</summary>
    void OnStreamsAvailable(ushort bidirectional, ushort unidirectional);

    /// <summary>The peer's address changed (migration / NAT rebind).</summary>
    void OnPeerAddressChanged(in TransportConnectedInfo info);

    /// <summary>The connection is closed. No further callbacks follow.</summary>
    /// <remarks>
    /// Before it, every accepted send has completed (in-flight stream sends canceled, datagrams in a final state) and every
    /// stream not yet shut down has reported <see cref="OnStreamShutdownComplete"/>. <paramref name="reason"/> is
    /// <see cref="TransportCloseReason.Local"/> after <see cref="ITransport.Close"/> (with its error code),
    /// <see cref="TransportCloseReason.Peer"/> with the peer's application error code, or
    /// <see cref="TransportCloseReason.Transport"/> (idle timeout, handshake failure, refusal, protocol error, link loss), for
    /// which both <paramref name="errorCode"/> and <paramref name="transportStatus"/> are transport-specific: MsQuic reports the
    /// QUIC transport error code of the close (0 for a silent idle timeout) and the <c>QUIC_STATUS</c>; the simulator reports
    /// 0 and one of its <c>Status*</c> constants. When both ends close at about the same time, each may report
    /// <see cref="TransportCloseReason.Local"/> (its own close won) or <see cref="TransportCloseReason.Peer"/> (the peer's
    /// close arrived first).
    /// </remarks>
    void OnClosed(TransportCloseReason reason, ulong errorCode, int transportStatus);
}
