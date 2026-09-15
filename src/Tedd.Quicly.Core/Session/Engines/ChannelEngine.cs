using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Session.Engines;

/// <summary>
/// The engine boundary (docs/design/session-layer.md §7). One sealed subclass per <see cref="ChannelMode"/>; one instance
/// per mode per peer owns the structure-of-arrays state of <em>all</em> channels of its mode. The peer contains no
/// mode-specific logic: it resolves channels, owns the shared tables (reached through <see cref="PeerCore"/>) and
/// dispatches to the engine of the channel's mode.
/// </summary>
/// <remarks>
/// <para>Threads: members in the "game thread" group run on the game thread inside <c>Send*</c>, <see cref="QuiclyPeer.Flush"/>
/// and <see cref="QuiclyPeer.Poll"/>; members in the "transport thread" group run inside transport callbacks (serialised
/// per connection). An engine keeps its send-side and receive-side state apart (ADR 0008 invariant 4) and hands data
/// across only through the peer's rings and mailboxes. Transport-thread members never throw on peer input and never
/// block.</para>
/// <para>Wave C2 hooks (fragmentation, request/response, key retirement, bulk) have virtual defaults that answer
/// <see cref="SendStatus.NotSupported"/> or throw <see cref="NotSupportedException"/>, so the engines that implement them
/// replace only their own files.</para>
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

    /// <summary>Submits queued work to the transport (game thread, inside <see cref="QuiclyPeer.Flush"/>).</summary>
    /// <param name="flush">Flush inputs; lower <see cref="FlushContext.NextDeadline"/> for pending work.</param>
    public abstract void Flush(ref FlushContext flush);

    /// <summary>Time-driven work: retries, expiry, timers (game thread, <paramref name="nowMicros"/> read once by the caller).</summary>
    /// <param name="nowMicros">Clock micros of this flush.</param>
    /// <param name="nextDeadline">Lower it to the engine's next deadline.</param>
    public abstract void Tick(long nowMicros, ref long nextDeadline);

    /// <summary>
    /// A send entry of this engine completed (drained from the completion ring, game thread). The engine decides the
    /// delivery status (fans out containers, keeps ReliableLatest entries for retries) and eventually calls
    /// <see cref="PeerCore.CompleteEntry"/>.
    /// </summary>
    /// <param name="entrySlot">The entry (state <c>Completed</c>).</param>
    /// <param name="completion">What the transport reported.</param>
    public abstract void OnSendCompleted(int entrySlot, in CompletionEntry completion);

    /// <summary>A new epoch starts (PROTOCOL.md §4.1): reset or re-queue per-mode state (game thread).</summary>
    /// <param name="resumed">True when the session was resumed rather than started fresh.</param>
    public abstract void OnEpochReset(bool resumed);

    /// <summary>Best-effort cancellation of a queued or in-flight entry (game thread). Default: not cancellable.</summary>
    /// <param name="entrySlot">The entry the token belongs to.</param>
    /// <returns><see langword="true"/> when the send will complete <see cref="Threading.DeliveryStatus.Canceled"/>.</returns>
    public virtual bool TryCancel(int entrySlot) => false;

    /// <summary>The connection is closed and every completion has been drained (game thread): release queued entries.</summary>
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

    /// <summary>A validated datagram message of a channel of this engine (transport thread; payload valid during the call).</summary>
    /// <param name="header">Parsed header (<see cref="PeerCore.CurrentSenderTick"/> holds the container tick).</param>
    /// <param name="payload">The payload.</param>
    /// <param name="nowMicros">Clock micros of the callback.</param>
    public abstract void OnDatagram(in MessageHeader header, ReadOnlySpan<byte> payload, long nowMicros);

    /// <summary>The peer opened a unidirectional stream whose preamble names a channel of this engine (transport thread).</summary>
    /// <param name="id">The stream.</param>
    /// <param name="channel">The channel.</param>
    /// <param name="groupId">Group id (group streams).</param>
    /// <returns>Accept (with a cookie) or reject (the stream is reset with the code).</returns>
    public abstract StreamAccept OnStreamOpened(TransportStreamId id, ushort channel, ulong groupId);

    /// <summary>A message event on an accepted stream (transport thread).</summary>
    /// <param name="message">The event.</param>
    /// <returns>What to do next.</returns>
    public abstract StreamConsume OnStreamMessage(ref StreamMessageContext message);

    /// <summary>
    /// An accepted peer stream ended (shutdown complete, reset by the peer, or reset by this end after a malformed frame),
    /// or a stream this engine opened was stopped or shut down (the engine ignores ids it does not own). Transport thread.
    /// </summary>
    /// <param name="id">The stream.</param>
    /// <param name="aborted">True when the stream was reset rather than finished.</param>
    /// <param name="errorCode">The reset code.</param>
    public abstract void OnStreamClosed(TransportStreamId id, bool aborted, ulong errorCode);

    /// <summary>
    /// A control message addressed to this mode (LatestAck/LatestReject → ReliableLatest; BulkProgress/BulkRequest/
    /// BulkCancel/BulkReject → Bulk; KeyRetired → the channel's mode). The peer has validated the frame with
    /// <see cref="ControlCodec"/>. Transport thread. Default: ignored.
    /// </summary>
    /// <param name="type">The message type.</param>
    /// <param name="body">The validated body.</param>
    /// <param name="onStream">True for a control-stream message, false for a control datagram.</param>
    /// <param name="nowMicros">Clock micros of the callback.</param>
    /// <returns><see langword="false"/> when the message violates the session rules (a protocol violation on the stream, a counted drop for a datagram).</returns>
    public virtual bool OnControl(ControlType type, ReadOnlySpan<byte> body, bool onStream, long nowMicros) => true;

    /// <summary>Releases engine resources (called from <see cref="QuiclyPeer.Dispose"/>).</summary>
    public virtual void Dispose()
    {
    }
}
