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
/// span − x"; two things together can. <em>Distance:</em> a late message is a few numbers behind the newest one, while a
/// jump of J reads as span − J behind, which is small only when almost a whole span was lost. <em>Time:</em> a late
/// datagram normally arrives near its successors. A sequence that is more than the reorder window behind the clock
/// (<see cref="ResyncWindow16"/>, <see cref="ResyncWindow32"/>) and arrives more than <see cref="ResyncQuietMicros"/>
/// after the clock last advanced is therefore taken for a forward jump: the clock moves to it (counted in
/// <see cref="PeerStatistics.SequenceResyncs"/>) and it is judged like any newer value. The decision uses the receive
/// callback's existing clock stamp — no timer and no extra clock read (ADR 0008 invariant 9; the reassembly expiry is the
/// precedent).</para>
/// <para><b>Why time alone is not enough.</b> QUIC never retransmits a datagram, but this library's own sender can make
/// one seconds late relative to its successors: a datagram without the cancel-on-blocked flag (a container that also
/// carries a ReliableLatest value, or any datagram with <see cref="PeerOptions.DropWhenBlocked"/> off) waits in the
/// transport's queue for as long as the link is busy, and a later datagram with the priority flag (an Immediate or
/// high-priority member) overtakes it (PROTOCOL.md §4.5). Such a straggler arrives after a quiet period, behind the
/// clock by the few messages that overtook it; without the window it would be read as a jump, delivered after its
/// successors, and leave the clock almost a span ahead. Inside the window it is judged as what it is: late.</para>
/// <para><b>What the window costs, and what it leaves.</b> A real jump that lands within the window of a whole span
/// (more than 64 512 consecutive messages lost on a 16-bit channel) is not resynchronised: the arrivals read as late
/// until the sender's counter has passed the clock, at most one window of messages — a small fraction of what the
/// blackout itself lost. A straggler <em>more</em> than the window behind its channel's newest sequence that arrives
/// after the quiet period is still read as a jump; that needs more than a window of later messages of the channel to
/// have overtaken it, followed by two seconds of silence on the channel.</para>
/// </remarks>
internal sealed class UnreliableSequencedEngine : DatagramEngine
{
    /// <summary>
    /// How long a channel's sequence clock must have stood still before a sequence behind it is read as a forward jump
    /// rather than a late message: two seconds, far beyond the reordering of a path. It is not beyond what the sender's
    /// own transport queue can add (see the class remarks), which is why the distance is tested as well.
    /// </summary>
    internal const long ResyncQuietMicros = 2_000_000;

    /// <summary>
    /// Reorder window of a 16-bit channel: a sequence at most this far behind the clock is always a late message, however
    /// long the clock stood still. A jump reads as this close only when more than 65 536 − 1 024 messages were lost.
    /// </summary>
    internal const int ResyncWindow16 = 1_024;

    /// <summary>Reorder window of a 32-bit channel (see <see cref="ResyncWindow16"/>).</summary>
    internal const int ResyncWindow32 = 65_536;

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
    /// ahead of it — or more than the reorder window behind it after the clock stood still for more than
    /// <see cref="ResyncQuietMicros"/>, which is read
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
            else if (distance < -(sixteenBit ? ResyncWindow16 : ResyncWindow32) && nowMicros - state.LastReceiveMicros > ResyncQuietMicros)
            {
                // Further behind the newest than a straggler is, and the newest has not advanced for two seconds: the
                // sender's counter is at least half the space ahead. Step forward by span − |distance| (in
                // [span / 2, span − window − 1]); the low bits become the arriving sequence. A duplicate of the newest
                // (distance 0) and a sequence inside the reorder window never get here.
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
