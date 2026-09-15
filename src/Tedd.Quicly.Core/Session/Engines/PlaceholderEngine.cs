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
    public override void OnSendCompleted(int entrySlot, in CompletionEntry completion) => _core.CompleteEntry(entrySlot, DeliveryStatus.Failed);

    /// <inheritdoc/>
    public override void OnEpochReset(bool resumed)
    {
    }

    /// <inheritdoc/>
    public override void OnDatagram(in MessageHeader header, ReadOnlySpan<byte> payload, long nowMicros) => _core.CountReceiveDropped(header.Channel);

    /// <inheritdoc/>
    public override StreamAccept OnStreamOpened(TransportStreamId id, ushort channel, ulong groupId) => StreamAccept.Reject(QuiclyErrorCode.UnsupportedChannel);

    /// <inheritdoc/>
    public override StreamConsume OnStreamMessage(ref StreamMessageContext message) => StreamConsume.ResetStream(QuiclyErrorCode.UnsupportedChannel);

    /// <inheritdoc/>
    public override void OnStreamClosed(TransportStreamId id, bool aborted, ulong errorCode)
    {
    }
}
