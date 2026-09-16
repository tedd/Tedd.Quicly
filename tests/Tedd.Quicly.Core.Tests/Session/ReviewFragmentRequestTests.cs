using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Findings of the wave C2d review of datagram fragmentation and correlated request/response (PROTOCOL.md is normative;
/// docs/design/session-layer.md §7.8 is the feature's own design). Each test names the rule it checks; none of them
/// asserts an implementation detail and none of them changes production code.
/// </summary>
public class ReviewFragmentRequestTests
{
    private static readonly ChannelTable Fragmenting = FragmentTables.Main;

    /// <summary>
    /// PROTOCOL.md §7 bounds a "fragmented message size" by <c>MaxMessageSize</c>; §8 ("§2.1 fragments") gives the receiver
    /// the two bounds it can compute from a <em>single</em> fragment — a non-last fragment of size <c>s</c> implies at least
    /// <c>s × (count − 1) + 1</c> bytes and the last fragment of size <c>l</c> implies at least <c>l × count</c>. Both are
    /// enforced per fragment (by the framing layer and again by the engine), but the message's <em>real</em> total,
    /// <c>s × (count − 1) + l</c>, is never checked against the limit once both sizes are known. The two per-fragment bounds
    /// leave a gap of up to <c>limit / count − 1</c> bytes, which is reachable whenever the channel's limit is no larger than
    /// what <c>count − 1</c> fragments carry — the shape of any channel sized for "a bit more than one datagram".
    /// <para>
    /// Channel 6 declares <c>MaxMessageSize</c> 2 400. Two non-last fragments of 1 190 bytes each imply only
    /// 1 190 × 2 + 1 = 2 381 bytes, and a last fragment of 800 bytes implies only 800 × 3 = 2 400 — so every fragment passes
    /// on its own, while the message they form is 3 180 bytes: a third more than the channel promised the application, out of
    /// a partial buffer the engine rented at <c>size × count</c> = 3 570 bytes, which also breaks the reassembly sizing rule
    /// of ARCHITECTURE.md §9 (<c>MaxReassemblies × MaxMessageSize</c> per channel).
    /// </para>
    /// <para>
    /// The engine already refuses exactly this message when the fragments arrive in the other order: with the last fragment
    /// first, the non-last branch computes <c>total = s × (count − 1) + LastLength</c> and drops the partial because it
    /// exceeds the limit. So the limit is enforced in one arrival order and not in the other, which is what the two halves of
    /// this test show. Fix: mirror that check in the <c>last</c> branch (drop unless
    /// <c>FragmentSize × (count − 1) + size ≤ limit</c>) and clamp the rent to <c>min(size × count, limit)</c>.
    /// </para>
    /// </summary>
    [Fact]
    public void A_Reassembled_Message_Never_Exceeds_The_Channels_Max_Message_Size()
    {
        using ServerHarness h = new(table: Fragmenting);
        Assert.True(h.Admit(), "the raw client was not admitted");
        QuiclyPeer server = h.Server!;
        ChannelDefinition channel = Fragmenting[6]!;
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        server.RegisterHandler(6, Handlers.Collect(got));

        // Fragment sizes that each pass their own §8 bound while summing to 3 180 bytes on a 2 400-byte channel.
        int[] sizes = [1_190, 1_190, 800];
        int total = sizes.Sum();
        Assert.True(total > channel.MaxMessageSize, "the shape is oversized");

        byte[] Fragment(uint sequence, byte index) => FragmentKit.RawFragment(
            channel: 6,
            sequenceBytes: 2,
            sequence: sequence,
            key: null,
            fragCount: 3,
            fragIndex: index,
            rawLength: null,
            DatagramKit.Payload(index, sizes[index]));

        // The last fragment first: the engine learns the real total in its non-last branch and drops the partial, which is
        // the behaviour PROTOCOL.md §7 asks for.
        foreach (byte index in new byte[] { 2, 0 })
        {
            Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(Fragment(1, index)));
            h.Run(5_000);
        }

        Assert.Empty(got);
        Assert.Equal(1, h.Statistics().FragmentsDropped);
        Assert.Equal(0, FragmentKit.Reassemblies(server, 6));

        // The same three fragments in order: no branch ever adds the two known sizes up, so the message is delivered.
        foreach (byte index in new byte[] { 0, 1, 2 })
        {
            Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(Fragment(2, index)));
            h.Run(5_000);
        }

        h.Run(20_000);
        int delivered = got.Count == 0 ? 0 : got[0].Payload.Length;
        PeerStatistics s = h.Statistics();
        Assert.True(
            delivered <= channel.MaxMessageSize,
            $"a {delivered}-byte message was reassembled and delivered on channel 6, whose MaxMessageSize is "
            + $"{channel.MaxMessageSize} (PROTOCOL.md §7); the same fragments in the other order were refused. "
            + $"FragmentsReceived {s.FragmentsReceived}, FragmentsDropped {s.FragmentsDropped}, "
            + $"MalformedDatagrams {s.MalformedDatagrams}.");
        Assert.Empty(got);
        Assert.Equal(0, FragmentKit.Reassemblies(server, 6));
        Assert.Equal(PeerState.Connected, server.State);
    }

    /// <summary>
    /// PROTOCOL.md §2.1: "the reassembly scope is (channel, key, sequence) … on <c>UnreliableUnordered</c> the sequence is
    /// only a reassembly id", and §7 allows <c>MaxReassemblies</c> (default 16) "concurrent reassemblies per channel". The
    /// engine keys its partials by <em>key alone</em>, so an unkeyed fragmenting channel holds exactly one partial and two
    /// messages whose fragments interleave destroy each other: the second message's first fragment abandons the first
    /// message's partial, and the first message's remaining fragments are then dropped as "older" — even though every
    /// fragment of both messages arrived and the channel's cap of 16 was never approached.
    /// <para>
    /// Interleaving is the normal case, not a hostile one: a fragmented message is several datagrams, and PROTOCOL.md §4.5
    /// packs and paces them, so any reordering or jitter between two consecutive messages produces this order. The "newer
    /// sequence for the same key abandons the older partial" rule of §7 exists to drop <em>obsolete state</em> on a keyed
    /// channel, not to cancel an in-progress reassembly of a different message on a channel whose sequence carries no
    /// ordering at all.
    /// </para>
    /// <para>
    /// Fix: match a partial by (key, sequence) rather than by key, keep the §7 cap as the bound on concurrency (the oldest
    /// partial is evicted at the cap, which is already implemented), and apply the abandon rule only where a newer
    /// sequence really supersedes the older message — a keyed <c>UnreliableSequenced</c> channel.
    /// </para>
    /// </summary>
    [Fact]
    public void Two_Fragmented_Messages_Whose_Fragments_Interleave_Are_Both_Reassembled()
    {
        using ServerHarness h = new(table: Fragmenting);
        Assert.True(h.Admit(), "the raw client was not admitted");
        QuiclyPeer server = h.Server!;
        ChannelDefinition channel = Fragmenting[2]!;
        Assert.True(channel.MaxReassemblies >= 2, "the channel may hold more than one partial");
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        server.RegisterHandler(2, Handlers.Collect(got));

        byte[] first = DatagramKit.Payload(1, 2_000);
        byte[] second = DatagramKit.Payload(2, 2_000);
        List<byte[]> a = FragmentKit.Split(channel, sequence: 1, key: 0, first, count: 2);
        List<byte[]> b = FragmentKit.Split(channel, sequence: 2, key: 0, second, count: 2);

        // The order a paced link produces for two consecutive fragmented messages.
        foreach (byte[] frame in new[] { a[0], b[0], a[1], b[1] })
        {
            Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(frame));
            h.Run(2_000);
        }

        PeerStatistics statistics = h.Statistics();
        Assert.True(
            h.RunUntil(() => got.Count == 2, 200_000),
            $"{got.Count} of 2 interleaved messages arrived, though all 4 fragments were received "
            + $"(FragmentsReceived {statistics.FragmentsReceived}, FragmentsDropped {statistics.FragmentsDropped}, "
            + $"ReassembliesAbandoned {statistics.ReassembliesAbandoned}).");
        Assert.Equal(first, got.Single(m => m.Header.Sequence == 1).Payload);
        Assert.Equal(second, got.Single(m => m.Header.Sequence == 2).Payload);
        Assert.Equal(0, h.Statistics().ReassembliesAbandoned);
        Assert.Equal(0, h.Statistics().FragmentsDropped);
    }

    /// <summary>
    /// Non-blocking. <see cref="QuiclyPeer.SendRequestAsync"/> documents <see cref="TimeSpan.Zero"/> as "wait until the
    /// response arrives, the wait is canceled or the session ends" and every positive timeout as a deadline that faults the
    /// wait with <see cref="TimeoutException"/>. <c>PeerOptions.ToMicros</c> truncates towards zero, so any positive timeout
    /// shorter than one microsecond becomes 0 micros, which the engine reads as the "no timeout" sentinel: the request then
    /// waits forever instead of failing. Fix: round a positive timeout up to 1 micro, or keep the sentinel in its own field.
    /// </summary>
    [Fact]
    public void A_Positive_Sub_Microsecond_Request_Timeout_Still_Elapses()
    {
        ChannelTable table = ChannelTable.Create()
            .Add(10, "rpc", ChannelMode.ReliableOrdered, o => o.RequestResponse = true)
            .Build();
        using SessionHarness h = new(table: table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        // The peer receives the request and never answers it.
        h.Server!.RegisterHandler(10, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => { });

        ValueTask<ReceiveLease> pending = client.SendRequestAsync(new SendHeader(10), new byte[] { 1 }, TimeSpan.FromTicks(5));
        h.Run(60_000);
        client.Poll();

        Assert.True(
            pending.IsCompleted,
            "a positive timeout of 500 ns never elapsed: it was truncated to 0 micros, which SendRequestAsync reads as 'wait forever'.");
        Assert.Throws<TimeoutException>(() => _ = pending.Result);
        Assert.Equal(1, DatagramKit.Statistics(client).RequestsTimedOut);
    }
}
