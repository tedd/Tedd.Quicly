using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Session.Engines;

/// <summary>
/// Stand-in for a mode whose engine is not implemented yet: sends answer <see cref="SendStatus.NotSupported"/>,
/// received datagrams are dropped and counted, and peer-opened streams of its channels are reset with
/// <see cref="QuiclyErrorCode.UnsupportedChannel"/>. The real engine replaces it in <see cref="ChannelEngines.Create"/>
/// without touching the peer.
/// </summary>
internal sealed class PlaceholderEngine(ChannelMode mode) : ChannelEngine
{
    private PeerCore _core = null!;

    /// <inheritdoc/>
    public override ChannelMode Mode { get; } = mode;

    /// <inheritdoc/>
    public override void Initialize(PeerCore core, ReadOnlySpan<ChannelDefinition> channelsOfMode) => _core = core;

    /// <inheritdoc/>
    public override SendStatus Admit(ref SendRequest request) => SendStatus.NotSupported;

    /// <inheritdoc/>
    public override void Flush(ref FlushContext flush)
    {
    }

    /// <inheritdoc/>
    public override void Tick(long nowMicros, ref long nextDeadline)
    {
    }

    /// <inheritdoc/>
    public override void OnSendCompleted(int entrySlot, in CompletionEntry completion)
    {
        // A placeholder never submits entries; complete defensively so a slot can never leak.
        if (completion.Final)
        {
            _core.CompleteEntry(entrySlot, DeliveryStatus.Failed);
        }
    }

    /// <inheritdoc/>
    public override void OnEpochReset(bool resumed)
    {
    }

    /// <inheritdoc/>
    /// <remarks>
    /// Nothing to reset: a placeholder never sends and never accepts a stream. Wave C2's <c>ReliableLatest</c> engine
    /// re-queues every live key at its current version here or in <see cref="OnEpochReset"/> (PROTOCOL.md §4.1: a resumed
    /// session gets a free full-state resync), and the Bulk engine re-requests its resumable transfers.
    /// </remarks>
    public override void OnReconnecting()
    {
    }

    /// <inheritdoc/>
    public override void OnDatagram(in MessageHeader header, ReadOnlySpan<byte> payload, long nowMicros) => _core.CountDatagramDropped(header.Channel);

    /// <inheritdoc/>
    public override StreamAccept OnStreamOpened(TransportStreamId id, ushort channel, ulong groupId) => StreamAccept.Reject(QuiclyErrorCode.UnsupportedChannel);

    /// <inheritdoc/>
    public override StreamConsume OnStreamMessage(ref StreamMessageContext message) => StreamConsume.ResetStream(QuiclyErrorCode.UnsupportedChannel);

    /// <inheritdoc/>
    public override void OnStreamClosed(TransportStreamId id, bool aborted, ulong errorCode)
    {
    }
}
