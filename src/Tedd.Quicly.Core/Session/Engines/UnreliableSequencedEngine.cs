using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Primitives;
using Tedd.Quicly.Core.State;

namespace Tedd.Quicly.Core.Session.Engines;

/// <summary>
/// <see cref="ChannelMode.UnreliableSequenced"/> (ADR 0003, ARCHITECTURE.md §5): messages may be lost; only a message newer
/// than the last accepted one of its key (or of the channel, when unkeyed) is delivered. The sender numbers messages from
/// one 16- or 32-bit counter per channel (PROTOCOL.md §2.1), so a reused key is never mistaken for its previous holder.
/// Keyed channels track keys in a <see cref="ReceiveKeyTracker"/> (a full hashed table evicts the least recently updated
/// key, PROTOCOL.md §7). On a <see cref="ChannelDefinition.CoalesceOnReceive"/> channel an accepted value replaces an unread
/// older one in the key's mailbox. Default expiry: twice the flush interval (PROTOCOL.md §4.5).
/// </summary>
/// <remarks>
/// <para><b>Ordering (transport thread; PROTOCOL.md §8).</b> "Newer" is not decided by comparing two wire sequences in
/// serial arithmetic: that is only valid while the two are less than half the sequence space apart, and a key's last value
/// may be any number of channel messages old (one idle key next to a busy one). Instead the channel keeps a
/// <em>sequence clock</em>, <see cref="ChannelRecvState.NewestSequence"/>: the newest sequence that arrived on the channel
/// for any key, extended to 64 bits. Every arriving sequence is extended against the clock — the one comparison that is
/// between close numbers, since consecutive arrivals on a live channel are — and a key's last accepted value is stored
/// and compared as that 64-bit number. A key may therefore idle indefinitely.</para>
/// <para><b>Resynchronisation.</b> What remains is the clock's own limit: if at least half the sequence space of
/// consecutive channel messages fails to arrive (a blackout, or messages expiring or being canceled unsent, which burn
/// numbers), the next arrival reads as <em>behind</em> the clock. The wire value cannot tell "late by x" from "ahead by
/// span − x"; time can, because QUIC never retransmits a datagram, so a late one arrives near its successors. A sequence
/// behind the clock arriving more than <see cref="ResyncQuietMicros"/> after the clock last advanced is therefore taken
/// for a forward jump: the clock moves to it (counted in <see cref="PeerStatistics.SequenceResyncs"/>) and it is judged
/// like any newer value. The decision uses the receive callback's existing clock stamp — no timer and no extra clock read
/// (ADR 0008 invariant 9; the reassembly expiry is the precedent).</para>
/// </remarks>
internal sealed class UnreliableSequencedEngine : DatagramEngine
{
    /// <summary>
    /// How long a channel's sequence clock must have stood still before a sequence behind it is read as a forward jump
    /// rather than a late message: two seconds, far beyond any reordering of datagrams that are never retransmitted.
    /// </summary>
    internal const long ResyncQuietMicros = 2_000_000;

    private const ulong Span16 = 0x1_0000UL;
    private const ulong Span32 = 0x1_0000_0000UL;

    /// <inheritdoc/>
    public override ChannelMode Mode => ChannelMode.UnreliableSequenced;

    /// <inheritdoc/>
    protected override bool TracksKeys(ChannelDefinition channel) => channel.Keyed;

    /// <inheritdoc/>
    protected override bool Accept(int local, in MessageHeader header, out int keySlot, long nowMicros, ref ChannelRecvCounters counters)
    {
        keySlot = -1;
        bool sixteenBit = ChannelAt(local).SequenceBits == 16;
        ref ChannelRecvState state = ref ReceiveState(local);

        // The clock is observed before anything can refuse the message: it follows the sender's counter, so a value whose
        // key gets no slot (or whose buffer or ring slot is refused later) has still moved it.
        ulong extended = Observe(ref state, header.Sequence, sixteenBit, nowMicros, out bool advanced);
        ReceiveKeyTracker? keys = KeysOf(local);
        if (keys is null)
        {
            // Unkeyed: the channel's last accepted value is the clock itself, so "newer" is "the clock advanced".
            if (!advanced)
            {
                counters.Dropped++;
                return false;
            }

            state.LastAccepted = header.Sequence;
            state.Flags |= ChannelRecvFlags.HasAccepted;
            return true;
        }

        switch (keys.TryAcceptSequence(header.Key, extended, out keySlot))
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

    /// <summary>
    /// Extends <paramref name="sequence"/> against the channel's sequence clock and advances the clock when the sequence is
    /// ahead of it — or behind it after the clock stood still for more than <see cref="ResyncQuietMicros"/>, which is read
    /// as a forward jump of at least half the sequence space (transport thread).
    /// </summary>
    /// <param name="state">The channel's receive state.</param>
    /// <param name="sequence">The arriving sequence (the low 16 bits on a 16-bit channel).</param>
    /// <param name="sixteenBit">The channel's sequences are 16 bits wide.</param>
    /// <param name="nowMicros">The receive callback's clock stamp.</param>
    /// <param name="advanced">The sequence became the channel's newest (always for the first arrival of an epoch).</param>
    /// <returns>The sequence on the channel's 64-bit scale, to compare with a key's last accepted value.</returns>
    private ulong Observe(ref ChannelRecvState state, uint sequence, bool sixteenBit, long nowMicros, out bool advanced)
    {
        ulong newest = state.NewestSequence;
        ulong span = sixteenBit ? Span16 : Span32;
        ulong extended;
        if (newest == 0)
        {
            // First arrival of the epoch. The bias of one span keeps 0 free as "nothing seen" and leaves room below the seed
            // for the half span a later arrival may extend behind it.
            extended = span + (sixteenBit ? (ushort)sequence : sequence);
        }
        else
        {
            int distance = sixteenBit ? SerialNumber.Distance((ushort)sequence, (ushort)newest) : SerialNumber.Distance(sequence, (uint)newest);
            if (distance > 0)
            {
                extended = newest + (ulong)distance;
            }
            else if (distance < 0 && nowMicros - state.LastReceiveMicros > ResyncQuietMicros)
            {
                // Behind the newest, yet the newest has not advanced for longer than any datagram can be late: the sender's
                // counter is at least half the space ahead. Step forward by span − |distance| (in [span / 2, span − 1]); the
                // low bits become the arriving sequence. A duplicate of the newest (distance 0) never gets here.
                extended = newest + span - (ulong)(-(long)distance);
                PeerCounters.SequenceResyncs++;
            }
            else
            {
                // Late or duplicated: its place behind the clock, for the per-key comparison. -(long) is exact for int.MinValue.
                advanced = false;
                return newest - (ulong)(-(long)distance);
            }
        }

        state.NewestSequence = extended;
        state.LastReceiveMicros = nowMicros;
        advanced = true;
        return extended;
    }
}
