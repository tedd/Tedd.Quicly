using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Primitives;
using Tedd.Quicly.Core.State;

namespace Tedd.Quicly.Core.Session.Engines;

/// <summary>
/// <see cref="ChannelMode.UnreliableSequenced"/> (ADR 0003, ARCHITECTURE.md §5): messages may be lost; only a message newer
/// than the last accepted one of its key (or of the channel, when unkeyed) is delivered, in serial arithmetic of the
/// channel's 16- or 32-bit sequence (PROTOCOL.md §2.1). The sender numbers messages from one counter per channel, so a
/// reused key is never mistaken for its previous holder. Keyed channels track keys in a <see cref="ReceiveKeyTracker"/>
/// (a full hashed table evicts the least recently updated key, PROTOCOL.md §7). On a
/// <see cref="ChannelDefinition.CoalesceOnReceive"/> channel an accepted value replaces an unread older one in the key's
/// mailbox. Default expiry: twice the flush interval (PROTOCOL.md §4.5).
/// </summary>
internal sealed class UnreliableSequencedEngine : DatagramEngine
{
    /// <inheritdoc/>
    public override ChannelMode Mode => ChannelMode.UnreliableSequenced;

    /// <inheritdoc/>
    protected override bool TracksKeys(ChannelDefinition channel) => channel.Keyed;

    /// <inheritdoc/>
    protected override bool Accept(int local, in MessageHeader header, out int keySlot, ref ChannelRecvCounters counters)
    {
        keySlot = -1;
        bool sixteenBit = ChannelAt(local).SequenceBits == 16;
        ReceiveKeyTracker? keys = KeysOf(local);
        if (keys is null)
        {
            ref ChannelRecvState state = ref ReceiveState(local);
            if ((state.Flags & ChannelRecvFlags.HasAccepted) != 0 && !IsNewer(header.Sequence, state.LastAccepted, sixteenBit))
            {
                counters.Dropped++;
                return false;
            }

            state.LastAccepted = header.Sequence;
            state.Flags |= ChannelRecvFlags.HasAccepted;
            return true;
        }

        switch (keys.TryAcceptSequence(header.Key, header.Sequence, sixteenBit, out keySlot))
        {
            case KeyAcceptance.Accepted:
                return true;
            case KeyAcceptance.Stale:
                counters.Dropped++;
                return false;
            default:
                counters.KeyTableFull++;
                return false;
        }
    }

    private static bool IsNewer(uint sequence, uint last, bool sixteenBit) =>
        sixteenBit ? SerialNumber.IsNewer((ushort)sequence, (ushort)last) : SerialNumber.IsNewer(sequence, last);
}
