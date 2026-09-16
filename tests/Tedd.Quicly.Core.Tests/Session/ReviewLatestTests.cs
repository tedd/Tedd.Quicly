using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Defects the wave C2a review of the ReliableLatest engine found. Each test states the contract that is broken, not the
/// implementation that breaks it.
/// </summary>
public class ReviewLatestTests
{
    private static readonly ChannelTable Table = LatestTables.Main;

    /// <summary>
    /// Review finding 1 (ARCHITECTURE.md §4.1, ADR 0008 invariant 1). <see cref="QuiclyPeer.SendShared"/> is admitted on a
    /// ReliableLatest channel, and the engine keeps the value's bytes for up to 30 s of retransmissions — but it takes no
    /// reference on the shared block, so the block goes back to the pool as soon as the caller drops its own reference while
    /// a live value still points into it (and may still retransmit from it).
    /// </summary>
    [Fact]
    public void A_Shared_Value_Is_Retained_For_As_Long_As_The_Engine_May_Retransmit_It()
    {
        using SharedPool pool = new();
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 5_000 }, table: Table,
            client: o =>
            {
                LatestKit.Quiet(o);
                o.Allocator = pool.Allocator;
            },
            server: LatestKit.Quiet);
        h.Run(50_000);

        SharedLease shared = pool.Share(64, seed: 9);
        Assert.Equal(1, pool.Count(in shared));

        // Nothing leaves the host yet: the value is admitted and waits for the next pass.
        SendResult result = h.Client.SendShared(new SendHeader(2, 1), pool.Table, in shared, 64, SendOptions.Tracked);
        Assert.Equal(SendStatus.Admitted, result.Status);
        Assert.Equal(1, LatestKit.LiveKeys(h.Client, 2));

        // The peer must hold exactly one reference while the value lives, like every other mode that takes a shared payload.
        Assert.Equal(2, pool.Count(in shared));

        // ... and release it exactly once when the value is finished.
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(result.Token) == DeliveryStatus.Delivered, 5_000_000),
            $"status {h.Client.GetDeliveryStatus(result.Token)}");
        Assert.Equal(1, pool.Count(in shared));
        Assert.True(pool.Table.Release(in shared));
    }

    /// <summary>
    /// Review finding 2 (PROTOCOL.md §4.1, §4.3, §4.4). A new epoch restarts the channel's version counter, so the receive
    /// side must forget its per-key versions before the next value. The engine only does that when the next value arrives as
    /// a datagram; a large value arrives on a group stream, is dropped as "not newer" against the previous epoch's version,
    /// and its re-ack of that stale version completes the value <see cref="DeliveryStatus.Delivered"/> — a value reported
    /// delivered that the peer never received.
    /// </summary>
    [Fact]
    public void A_Large_Value_Of_A_Resumed_Epoch_Is_Only_Delivered_When_It_Arrived()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 2_000 }, table: Table,
            client: o =>
            {
                LatestKit.Quiet(o);
                LatestKit.Roomy(o);
            },
            server: o =>
            {
                LatestKit.Quiet(o);
                LatestKit.Roomy(o);
            });
        h.Run(50_000);
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(2, LatestKit.Collect(received));

        // Epoch 1: a value too large for one datagram, so it travels on a group stream (PROTOCOL.md §3.2).
        SendResult first = h.Client.SendCopy(new SendHeader(2, 5), LatestKit.Payload(1, 8_000), SendOptions.Tracked);
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(first.Token) == DeliveryStatus.Delivered, 5_000_000),
            $"status {h.Client.GetDeliveryStatus(first.Token)}");
        Assert.Single(received);
        Assert.Equal(1u, received[0].Version);

        // A resumed session (PROTOCOL.md §4.1): the counters restart with the epoch on both sides.
        LatestKit.Engine(h.Client).OnEpochReset(resumed: true);
        LatestKit.Engine(h.Server).OnEpochReset(resumed: true);
        Assert.Equal(1u, LatestKit.NextVersion(h.Client, 2));

        // The first value of the new epoch is large too, so nothing of this key ever passes the datagram path.
        SendResult second = h.Client.SendCopy(new SendHeader(2, 5), LatestKit.Payload(2, 8_000), SendOptions.Tracked);
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(second.Token) is not DeliveryStatus.Pending, 5_000_000),
            "the value never finished");

        // PROTOCOL.md §4.3: Delivered means a LatestAck covering *this* version, so the value must have arrived.
        Assert.Equal(DeliveryStatus.Delivered, h.Client.GetDeliveryStatus(second.Token));
        Assert.Equal(2, received.Count);
        Assert.True(LatestKit.Matches(received[1].Payload, 2, 8_000), "the value of the new epoch was not delivered");
    }

    /// <summary>
    /// Probe for the double-report of a locally opened stream the group-stream review found (the peer broadcasts
    /// <c>OnStreamClosed(aborted: true)</c> for the peer's STOP_SENDING and <c>OnStreamClosed(aborted: false)</c> for the
    /// shutdown that follows). On a channel whose <see cref="ChannelDefinition.MaxGroups"/> is 4 the sender must never hold
    /// more open streams than the cap, so the receiver never has to reset a live stream of ours (PROTOCOL.md §7).
    /// </summary>
    [Fact]
    public void Aborting_Large_Value_Streams_Releases_Each_Per_Channel_Slot_Exactly_Once()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 2_000, BandwidthBitsPerSecond = 8_000_000 }, table: Table,
            client: o =>
            {
                LatestKit.Quiet(o);
                LatestKit.Roomy(o);
            },
            server: o =>
            {
                LatestKit.Quiet(o);
                LatestKit.Roomy(o);
            });
        h.Run(50_000);
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(2, LatestKit.Collect(received));

        // Six rounds of four keys: every round supersedes the previous round's still-sending streams, so each of them is
        // aborted and reported closed twice, while four new ones open.
        for (int round = 0; round < 6; round++)
        {
            for (ulong key = 1; key <= 4; key++)
            {
                Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(2, key), LatestKit.Payload(round, 20_000)).Status);
            }

            h.Client.Flush();
            h.Network.Advance(3_000);
            h.Server.Poll();
            h.Server.Flush();
            h.Client.Poll();
        }

        Assert.True(h.RunUntil(() => LatestKit.LiveKeys(h.Client, 2) == 0, 30_000_000),
            $"{LatestKit.LiveKeys(h.Client, 2)} keys are still live, so a stream slot was stranded");

        // The sender stayed inside the channel's cap, so the receiver never reset one of our live streams.
        Assert.Equal(0, DatagramKit.Statistics(h.Server).StreamsReset);
        Assert.True(received.Count >= 4, $"{received.Count} values arrived");
        Assert.Equal(PeerState.Connected, h.Client.State);
    }
}
