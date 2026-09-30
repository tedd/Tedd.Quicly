using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.State;

namespace Tedd.Quicly.Core.Session.Engines;

/// <summary>
/// <see cref="ChannelMode.UnreliableUnordered"/> (ADR 0003, PROTOCOL.md §2.1): messages may be lost and may arrive in any
/// order; every message that arrives is delivered. On a <see cref="ChannelDefinition.CoalesceOnReceive"/> channel the
/// newest arrival per key replaces an unread older one in the key's mailbox. Everything else is the shared
/// <see cref="DatagramEngine"/>.
/// </summary>
internal sealed class UnreliableUnorderedEngine : DatagramEngine
{
    /// <inheritdoc/>
    public override ChannelMode Mode => ChannelMode.UnreliableUnordered;

    /// <inheritdoc/>
    protected override bool TracksKeys(ChannelDefinition channel) => channel.CoalesceOnReceive;

    /// <inheritdoc/>
    protected override bool Accept(int local, in MessageHeader header, out int keySlot, long nowMicros, ref ChannelRecvCounters counters)
    {
        keySlot = -1;
        ReceiveKeyTracker? keys = KeysOf(local);
        if (keys is null || keys.TryTouch(header.Key, out keySlot))
        {
            return true;
        }

        counters.KeyTableFull++;
        return false;
    }
}
