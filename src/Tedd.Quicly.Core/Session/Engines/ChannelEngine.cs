using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Session.Engines;

/// <summary>
/// The engine boundary (docs/design/session-layer.md §7). One sealed subclass per <see cref="ChannelMode"/>; one instance
/// per mode per peer owns the structure-of-arrays state of <em>all</em> channels of its mode (indexed by a dense per-mode
/// channel index the engine derives in <see cref="Initialize"/>). The peer contains no mode-specific logic: it resolves
/// channels, owns the shared tables (reached through <see cref="PeerCore"/>) and dispatches to the engine of the
/// channel's mode. Engines are created by <see cref="ChannelEngines.Create"/>.
/// </summary>
/// <remarks>
/// <para>Threads: members in the "game thread" group run on the game thread inside <c>Send*</c>,
/// <see cref="QuiclyPeer.Flush"/> and <see cref="QuiclyPeer.Poll"/>; members in the "transport thread" group run inside
/// transport callbacks (serialised per connection). An engine keeps its send-side and receive-side state apart
/// (ADR 0008 invariant 4) and hands data across only through the peer's rings and mailboxes. Transport-thread members
/// never throw on peer input, never block and never call <see cref="ITransport.Close"/> (use
/// <see cref="PeerCore.RequestClose"/>).</para>
/// <para>Wave C2 hooks (fragmentation, request/response, key retirement, bulk) have virtual defaults that answer
/// <see cref="SendStatus.NotSupported"/> or throw <see cref="NotSupportedException"/>, so the engines that implement
/// them replace only their own files.</para>
/// </remarks>
internal abstract class ChannelEngine : IDisposable
{
    /// <summary>The mode this engine implements.</summary>
    public abstract ChannelMode Mode { get; }

    /// <summary>Called once from the peer constructor with every channel of <see cref="Mode"/> (ascending id).</summary>
    /// <param name="core">The peer's shared state.</param>
    /// <param name="channelsOfMode">The channels this engine owns.</param>
    public abstract void Initialize(PeerCore core, ReadOnlySpan<ChannelDefinition> channelsOfMode);

    // ------------------------------------------------------------------ game thread

    /// <summary>Validates and queues a send (game thread). Never calls the transport except to flush an immediate send.</summary>
    /// <param name="request">The request; set <see cref="SendRequest.Token"/> for tracked sends.</param>
    /// <returns>The admission result.</returns>
    public abstract SendStatus Admit(ref SendRequest request);

    /// <summary>
    /// The scheduler's pass over one channel of this engine (game thread, inside <see cref="QuiclyPeer.Flush"/> or at the
    /// end of a <see cref="SendMode.Immediate"/> send): hand the channel's queued messages to the transport in admission
    /// order — datagram engines through <see cref="PeerCore.Packer"/> — dropping those whose expiry has passed
    /// (PROTOCOL.md §4.5) and stopping when <see cref="FlushContext.BudgetBytes"/> runs out. The scheduler calls it once per
    /// pass for every channel of the engine, highest <see cref="ChannelDefinition.Priority"/> first
    /// (<see cref="PeerCore.ScheduleOrder"/>). Entries dropped here are finished through
    /// <see cref="PeerCore.QueueLocalCompletion"/>, never inline, so no continuation runs inside a pass. Default: nothing.
    /// </summary>
    /// <param name="channelIndex">Dense index of a channel of this engine.</param>
    /// <param name="flush">The pass: clock, tick, datagram limits, send budget.</param>
    public virtual void FlushChannel(int channelIndex, ref FlushContext flush)
    {
    }

    /// <summary>
    /// Engine-level work after every channel's <see cref="FlushChannel"/> (game thread, once per scheduler pass): traffic
    /// that PROTOCOL.md §4.5 schedules after fresh real-time messages (retries, bulk) and anything not tied to one channel.
    /// </summary>
    /// <param name="flush">The pass; lower <see cref="FlushContext.NextDeadline"/> for pending work.</param>
    public abstract void Flush(ref FlushContext flush);

    /// <summary>Time-driven work: retries, expiry, timers (game thread, <paramref name="nowMicros"/> read once by the caller).</summary>
    /// <param name="nowMicros">Clock micros of this flush.</param>
    /// <param name="nextDeadline">Lower it to the engine's next deadline.</param>
    public abstract void Tick(long nowMicros, ref long nextDeadline);

    /// <summary>
    /// A send entry of this engine completed (drained from the completion ring, game thread). For a final completion
    /// (<see cref="CompletionEntry.Final"/>) the engine decides the delivery status (fans out containers, keeps
    /// ReliableLatest entries for retries) and eventually calls <see cref="PeerCore.CompleteEntry"/>; for the early
    /// <see cref="DatagramSendState.Sent"/> notice it may release the payload (<see cref="PeerCore.ReleasePayload"/>) and
    /// complete the BufferReleased stage (<see cref="PeerCore.CompleteStage"/>).
    /// </summary>
    /// <param name="entrySlot">The entry.</param>
    /// <param name="completion">What the transport reported.</param>
    public abstract void OnSendCompleted(int entrySlot, in CompletionEntry completion);

    /// <summary>A new epoch starts (PROTOCOL.md §4.1): reset or re-queue per-mode state (game thread, when the session becomes <see cref="PeerState.Connected"/>).</summary>
    /// <param name="resumed">True when the session was resumed (epoch &gt; 1) rather than started fresh.</param>
    public abstract void OnEpochReset(bool resumed);

    /// <summary>Best-effort cancellation of a queued or in-flight entry (game thread). Default: not cancellable.</summary>
    /// <param name="entrySlot">The entry the token belongs to.</param>
    /// <returns><see langword="true"/> when the send will complete <see cref="Threading.DeliveryStatus.Canceled"/>.</returns>
    public virtual bool TryCancel(int entrySlot) => false;

    /// <summary>
    /// The admission stamp (<see cref="PeerCore.StampAdmission"/>) of the oldest message this engine holds queued and not yet
    /// handed to the transport, or <see cref="long.MaxValue"/> when it holds none (game thread; the watermark of
    /// <see cref="QuiclyPeer.FlushAsync"/>). Default: <see cref="long.MaxValue"/>.
    /// </summary>
    /// <returns>The oldest queued stamp.</returns>
    public virtual long OldestQueuedStamp() => long.MaxValue;

    /// <summary>Adds what the engine holds for a channel (queued and in-flight messages and bytes) to its statistics (game thread). Default: nothing.</summary>
    /// <param name="channelIndex">Dense index of a channel of this engine.</param>
    /// <param name="statistics">The snapshot being filled.</param>
    public virtual void AddStatistics(int channelIndex, ref ChannelStatistics statistics)
    {
    }

    /// <summary>
    /// The connection is closed (game thread, from <see cref="QuiclyPeer.Poll"/>, after every transport completion was
    /// drained): complete queued entries <see cref="Threading.DeliveryStatus.Disconnected"/> and release their payloads.
    /// </summary>
    public virtual void OnPeerClosed()
    {
    }

    /// <summary>Retires a key (KeyRetired, wave C2). Default <see cref="SendStatus.NotSupported"/>.</summary>
    /// <param name="channel">A channel of this engine.</param>
    /// <param name="key">The key.</param>
    /// <returns>The outcome.</returns>
    public virtual SendStatus RetireKey(ChannelDefinition channel, ulong key) => SendStatus.NotSupported;

    /// <summary>Sends a request on a request/response channel (wave C2). Default: faults with <see cref="NotSupportedException"/>.</summary>
    /// <param name="request">The request.</param>
    /// <param name="timeoutMicros">Response timeout.</param>
    /// <param name="cancellationToken">Cancels the wait.</param>
    /// <returns>The response.</returns>
    public virtual ValueTask<ReceiveLease> SendRequestAsync(ref SendRequest request, long timeoutMicros, CancellationToken cancellationToken) =>
        ValueTask.FromException<ReceiveLease>(new NotSupportedException($"Request/response is not supported on {Mode} channels yet."));

    /// <summary>Answers a request (wave C2). Default <see cref="SendStatus.NotSupported"/>.</summary>
    /// <param name="requestHeader">The request being answered.</param>
    /// <param name="response">The response payload.</param>
    /// <returns>The outcome.</returns>
    public virtual SendStatus Respond(in ReceiveHeader requestHeader, ref SendRequest response) => SendStatus.NotSupported;

    /// <summary>Starts a bulk transfer (wave C2). Default: faults with <see cref="NotSupportedException"/>.</summary>
    /// <param name="channel">The bulk channel.</param>
    /// <param name="descriptor">What to send.</param>
    /// <param name="source">Where the bytes come from.</param>
    /// <param name="cancellationToken">Cancels the transfer.</param>
    /// <returns>The transfer.</returns>
    public virtual ValueTask<BulkTransfer> BeginBulkSendAsync(ChannelDefinition channel, in BulkDescriptor descriptor, IBulkSource source, CancellationToken cancellationToken) =>
        ValueTask.FromException<BulkTransfer>(new NotSupportedException($"Bulk transfers are not supported on {Mode} channels yet."));

    /// <summary>Requests a bulk range from the peer (wave C2). Default: throws <see cref="NotSupportedException"/>.</summary>
    /// <param name="channel">The bulk channel.</param>
    /// <param name="request">The range.</param>
    public virtual void RequestBulk(ChannelDefinition channel, in BulkRangeRequest request) =>
        throw new NotSupportedException($"Bulk requests are not supported on {Mode} channels yet.");

    // ------------------------------------------------------------------ transport thread

    /// <summary>A validated datagram message of a channel of this engine (transport thread; payload valid during the call; only after admission).</summary>
    /// <param name="header">Parsed header (<see cref="PeerCore.CurrentSenderTick"/> holds the container tick).</param>
    /// <param name="payload">The payload.</param>
    /// <param name="nowMicros">Clock micros of the callback.</param>
    public abstract void OnDatagram(in MessageHeader header, ReadOnlySpan<byte> payload, long nowMicros);

    /// <summary>The peer opened a unidirectional stream whose preamble names a channel of this engine (transport thread; only after admission).</summary>
    /// <param name="id">The stream.</param>
    /// <param name="channel">The channel.</param>
    /// <param name="groupId">Group id (group streams).</param>
    /// <returns>Accept (with a cookie), reject (the stream is reset with the code) or close the connection.</returns>
    public abstract StreamAccept OnStreamOpened(TransportStreamId id, ushort channel, ulong groupId);

    /// <summary>A message event on an accepted stream (transport thread).</summary>
    /// <param name="message">The event.</param>
    /// <returns>What to do next.</returns>
    public abstract StreamConsume OnStreamMessage(ref StreamMessageContext message);

    /// <summary>
    /// An accepted peer stream ended (shutdown complete, reset by the peer, or reset by this end after a malformed frame),
    /// or a stream this end opened was stopped (STOP_SENDING, <paramref name="aborted"/>) or shut down. Every engine sees
    /// the events of locally opened streams and ignores ids it does not own. Transport thread.
    /// </summary>
    /// <remarks>
    /// Called <b>exactly once per stream</b>, peer-opened or locally opened. A started stream that the peer stops raises both
    /// the stop and the shutdown that always follows it (MsQuic completes every started stream with SHUTDOWN_COMPLETE), and the
    /// peer collapses the two into this one call — for a peer stream through its <c>StreamRecord</c>, for a locally opened one
    /// through the <c>Discard</c> record the stop leaves behind. An engine may therefore release a stream's resources here
    /// without a second notice arriving for the same stream (ADR 0008: released exactly once on every path).
    /// </remarks>
    /// <param name="id">The stream.</param>
    /// <param name="aborted">True when the stream was reset or stopped rather than finished.</param>
    /// <param name="errorCode">The reset code.</param>
    public abstract void OnStreamClosed(TransportStreamId id, bool aborted, ulong errorCode);

    /// <summary>
    /// A stream this engine opened with a context from <see cref="PeerCore.MakeEngineStreamContext"/> finished starting
    /// (transport thread, routed by the context's mode). <paramref name="status"/> is <see cref="TransportStatus.Success"/>, or
    /// why the stream never started: with <see cref="TransportStatus.StreamLimitReached"/> the peer's stream limit refused it,
    /// the send that carried <see cref="TransportSendFlags.Start"/> completes canceled and
    /// <see cref="ITransportSink.OnStreamShutdownComplete"/> follows (nothing reached the peer). A start refused synchronously
    /// (the send call itself returned the status) raises nothing. Default: nothing.
    /// </summary>
    /// <param name="id">The stream.</param>
    /// <param name="context">The context given to <see cref="ITransport.OpenStream"/>.</param>
    /// <param name="status">The outcome.</param>
    public virtual void OnStreamStarted(TransportStreamId id, ulong context, TransportStatus status)
    {
    }

    /// <summary>
    /// The transport published the ideal number of bytes to keep outstanding on one of this engine's streams
    /// (<see cref="ITransportSink.OnIdealSendBufferSize"/>, only when
    /// <see cref="TransportCapabilities.IdealSendBufferSize"/> is set). Broadcast to every engine like the local-stream
    /// events, so an engine ignores ids it does not own. Transport thread. Default: nothing.
    /// </summary>
    /// <param name="id">The stream.</param>
    /// <param name="bytes">Bytes the transport would like kept outstanding.</param>
    public virtual void OnIdealSendBufferSize(TransportStreamId id, ulong bytes)
    {
    }

    /// <summary>
    /// A control message addressed to this mode (LatestAck/LatestReject → ReliableLatest; BulkProgress/BulkRequest/
    /// BulkCancel/BulkReject → Bulk; KeyRetired → the channel's mode). The peer has validated the frame with
    /// <see cref="ControlCodec"/> (batch structure, channel ranges) and the session is admitted. Transport thread.
    /// Default: accepted and ignored.
    /// </summary>
    /// <param name="type">The message type.</param>
    /// <param name="body">The validated body.</param>
    /// <param name="onStream">True for a control-stream message, false for a control datagram.</param>
    /// <param name="nowMicros">Clock micros of the callback.</param>
    /// <returns><see langword="false"/> when the message violates the session rules (a protocol violation on the stream, a counted drop for a datagram).</returns>
    public virtual bool OnControl(ControlType type, ReadOnlySpan<byte> body, bool onStream, long nowMicros) => true;

    // ------------------------------------------------------------------ reconnect (game thread)

    /// <summary>
    /// The connection was lost and the peer is about to attach a new transport for the same session
    /// (<see cref="QuiclyPeer.Reconnect"/>, PROTOCOL.md §4.1). Drop everything bound to the old transport — stream ids,
    /// stream phases, half-received messages, queued notices — so the resumed connection starts from scratch. The session's
    /// own channel rules (sequence tables reset, live <c>ReliableLatest</c> keys re-queued, resumable Bulk transfers
    /// re-requested) belong to <see cref="OnEpochReset"/>, which runs when the resume is accepted and the new epoch is known.
    /// </summary>
    /// <remarks>
    /// Game thread, called while no transport callback can arrive (the old transport reported its close, the new one is not
    /// attached yet) and after every send entry of the lost connection completed
    /// <see cref="Threading.DeliveryStatus.Disconnected"/>, so an engine only clears its own bookkeeping here and must not
    /// queue completions or touch the transport. Default: nothing.
    /// </remarks>
    public virtual void OnReconnecting()
    {
    }

    /// <summary>Releases engine resources (called once, after the transport can no longer call back).</summary>
    public virtual void Dispose()
    {
    }
}
