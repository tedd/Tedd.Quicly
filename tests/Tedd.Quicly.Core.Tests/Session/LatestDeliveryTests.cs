using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// ReliableLatest delivery over the simulated transport (PROTOCOL.md §4.4): the latest value of a key is delivered while
/// the epoch lives, a lost value is retransmitted, a lost ack is recovered by the re-ack, superseded values complete
/// <see cref="DeliveryStatus.Superseded"/>, a version that uses up its budget completes <see cref="DeliveryStatus.Failed"/>,
/// versions roll over, a resumed session re-queues every live key, and a retired key can be used again.
/// </summary>
public class LatestDeliveryTests
{
    private static readonly ChannelTable Table = LatestTables.Main;

    private static SessionHarness Harness(LinkOptions? link = null, Action<PeerOptions>? client = null)
    {
        SessionHarness harness = new(link: link ?? new LinkOptions { DelayMicros = 5_000 }, table: Table,
            client: o =>
            {
                LatestKit.Quiet(o);
                client?.Invoke(o);
            },
            server: LatestKit.Quiet);

        // The session's first Ping and its Pong are datagrams too: let them pass before a test arms a targeted drop, so
        // the drop hits the value or the ack it means to hit.
        harness.Run(50_000);
        return harness;
    }

    [Fact]
    public void A_Value_Is_Delivered_Once_And_Acknowledged()
    {
        using SessionHarness h = Harness();
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(2, LatestKit.Collect(received));
        SendResult result = h.Client.SendCopy(new SendHeader(2, 7), LatestKit.Payload(1, 64), SendOptions.Tracked);
        Assert.Equal(SendStatus.Admitted, result.Status);
        Assert.Equal(1, LatestKit.LiveKeys(h.Client, 2));

        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(result.Token) == DeliveryStatus.Delivered), "the value was not acknowledged");
        Assert.Single(received);
        Assert.Equal(7ul, received[0].Key);
        Assert.Equal(1u, received[0].Version);
        Assert.True(LatestKit.Matches(received[0].Payload, 1, 64));
        Assert.Equal(1u, LatestKit.AckedVersion(h.Client, 2, 7));
        Assert.Equal(0, LatestKit.LiveKeys(h.Client, 2));

        // The ack, not the datagram's own acknowledgement, is what completed it (PROTOCOL.md §4.3).
        h.Run(50_000);
        Assert.Single(received);
        Assert.Equal(PeerState.Connected, h.Client.State);
    }

    [Fact]
    public void A_Lost_Final_Update_Is_Eventually_Delivered()
    {
        using SessionHarness h = Harness();
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(2, LatestKit.Collect(received));

        // Three transmissions are lost: only the timer backstop can deliver the value (PROTOCOL.md §4.4).
        DatagramKit.TransportOf(h.Client).DropNextDatagrams(3);
        SendResult result = h.Client.SendCopy(new SendHeader(2, 3), LatestKit.Payload(9, 100), SendOptions.Tracked);
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(result.Token) == DeliveryStatus.Delivered, 2_000_000),
            $"status {h.Client.GetDeliveryStatus(result.Token)} after the retries");
        Assert.Single(received);
        Assert.True(LatestKit.Matches(received[0].Payload, 9, 100));
        Assert.True(DatagramKit.ChannelStats(h.Client, 2).Retries >= 3, $"{DatagramKit.ChannelStats(h.Client, 2).Retries} retries");
    }

    [Fact]
    public void A_Lost_Ack_Is_Recovered_By_The_Re_Ack()
    {
        using SessionHarness h = Harness();
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(2, LatestKit.Collect(received));

        // The value arrives, but the ack that would complete it is lost: the sender retransmits and the receiver re-acks.
        DatagramKit.TransportOf(h.Server).DropNextDatagrams(1);
        SendResult result = h.Client.SendCopy(new SendHeader(2, 11), LatestKit.Payload(2, 32), SendOptions.Tracked);
        Assert.True(h.RunUntil(() => received.Count == 1), "the value did not arrive");
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(result.Token) == DeliveryStatus.Delivered, 2_000_000),
            $"status {h.Client.GetDeliveryStatus(result.Token)} after the lost ack");

        // The duplicate was not delivered twice; it only produced the re-ack.
        Assert.Single(received);
        Assert.True(DatagramKit.ChannelStats(h.Server, 2).Dropped >= 1, "the duplicate was not counted");
    }

    [Fact]
    public void Only_The_Latest_Value_Of_A_Key_Is_Delivered()
    {
        using SessionHarness h = Harness();
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(2, LatestKit.Collect(received));

        // Three values of one key in one tick: the first two are replaced before they are ever submitted.
        SendToken first = h.Client.SendCopy(new SendHeader(2, 4), LatestKit.Payload(1, 40), SendOptions.Tracked).Token;
        SendToken second = h.Client.SendCopy(new SendHeader(2, 4), LatestKit.Payload(2, 40), SendOptions.Tracked).Token;
        SendToken third = h.Client.SendCopy(new SendHeader(2, 4), LatestKit.Payload(3, 40), SendOptions.Tracked).Token;
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(third) == DeliveryStatus.Delivered), "the newest value was not delivered");
        Assert.Equal(DeliveryStatus.Superseded, h.Client.GetDeliveryStatus(first));
        Assert.Equal(DeliveryStatus.Superseded, h.Client.GetDeliveryStatus(second));
        Assert.Single(received);
        Assert.True(LatestKit.Matches(received[0].Payload, 3, 40));
        Assert.Equal(2, DatagramKit.ChannelStats(h.Client, 2).SendSuperseded);
    }

    [Fact]
    public void A_Value_Superseded_While_In_Flight_Completes_Superseded_After_Its_Transmission()
    {
        using SessionHarness h = Harness();
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(2, LatestKit.Collect(received));

        SendToken inFlight = h.Client.SendCopy(new SendHeader(2, 8), LatestKit.Payload(5, 48), SendOptions.Tracked).Token;
        h.Client.Flush();
        Assert.Equal(DeliveryStatus.Pending, h.Client.GetDeliveryStatus(inFlight));

        // Replaced while its datagram is in flight: its payload must stay valid until the transmission completes.
        SendToken newest = h.Client.SendCopy(new SendHeader(2, 8), LatestKit.Payload(6, 48), SendOptions.Tracked).Token;
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(newest) == DeliveryStatus.Delivered), "the newest value was not delivered");
        Assert.Equal(DeliveryStatus.Superseded, h.Client.GetDeliveryStatus(inFlight));
        Assert.True(LatestKit.Matches(received[^1].Payload, 6, 48));
    }

    [Fact]
    public void Versions_Roll_Over_Past_Two_To_The_Thirty_Second()
    {
        using SessionHarness h = Harness();
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(2, LatestKit.Collect(received));
        LatestKit.SetNextVersion(h.Client, 2, uint.MaxValue - 1);

        for (int i = 0; i < 3; i++)
        {
            SendResult result = h.Client.SendCopy(new SendHeader(2, 2), LatestKit.Payload(i, 24), SendOptions.Tracked);
            Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(result.Token) == DeliveryStatus.Delivered),
                $"value {i} (version {received.Count}) was not delivered");
        }

        Assert.Equal(3, received.Count);
        Assert.Equal(uint.MaxValue - 1, received[0].Version);
        Assert.Equal(uint.MaxValue, received[1].Version);

        // 0 is skipped on the wrap, so the counter continues at 1 and the receiver accepts it as newer (serial arithmetic).
        Assert.Equal(1u, received[2].Version);
        Assert.True(LatestKit.Matches(received[2].Payload, 2, 24));
    }

    [Fact]
    public void A_Version_That_Uses_Up_Its_Transmission_Budget_Completes_Failed()
    {
        using SessionHarness h = Harness();
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(2, LatestKit.Collect(received));

        // Exactly the 16 transmissions of PROTOCOL.md §4.4 are lost, so the version uses up its budget and the next value
        // (after the drops are spent) still gets through.
        DatagramKit.TransportOf(h.Client).DropNextDatagrams(ReliableLatestBudget.Transmissions);
        SendResult result = h.Client.SendCopy(new SendHeader(2, 6), LatestKit.Payload(4, 64), SendOptions.Tracked);
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(result.Token) is not DeliveryStatus.Pending, 40_000_000),
            "the version never finished");
        Assert.Equal(DeliveryStatus.Failed, h.Client.GetDeliveryStatus(result.Token));
        Assert.Empty(received);
        Assert.Equal(0, LatestKit.LiveKeys(h.Client, 2));
        Assert.Equal(PeerState.Connected, h.Client.State);
        ChannelStatistics statistics = DatagramKit.ChannelStats(h.Client, 2);
        Assert.Equal(ReliableLatestBudget.Transmissions, statistics.Sent);
        Assert.Equal(ReliableLatestBudget.Transmissions - 1, statistics.Retries);

        // The key is free again: the next value gets through once the link stops dropping.
        SendResult again = h.Client.SendCopy(new SendHeader(2, 6), LatestKit.Payload(5, 64), SendOptions.Tracked);
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(again.Token) == DeliveryStatus.Delivered, 5_000_000), "the next value was not delivered");
        Assert.Single(received);
    }

    [Fact]
    public void The_Per_Peer_Retry_Budget_Bounds_Retransmissions()
    {
        // 300 bytes per second of retries: four 64-byte keys cannot be retried every 20 ms (PROTOCOL.md §4.4).
        using SessionHarness h = Harness(client: o => o.MaxRetryBytesPerSecond = 300);
        DatagramKit.TransportOf(h.Client).DropNextDatagrams(512);
        for (ulong key = 20; key < 24; key++)
        {
            h.Client.SendCopy(new SendHeader(2, key), LatestKit.Payload((int)key, 64));
        }

        h.Run(1_000_000);
        long capped = DatagramKit.ChannelStats(h.Client, 2).Retries;
        Assert.InRange(capped, 1, 12);

        using SessionHarness free = Harness();
        DatagramKit.TransportOf(free.Client).DropNextDatagrams(512);
        for (ulong key = 20; key < 24; key++)
        {
            free.Client.SendCopy(new SendHeader(2, key), LatestKit.Payload((int)key, 64));
        }

        free.Run(1_000_000);
        long uncapped = DatagramKit.ChannelStats(free.Client, 2).Retries;
        Assert.True(uncapped > capped, $"the budget did not bound the retries ({capped} vs {uncapped})");
    }

    [Fact]
    public void A_Resumed_Session_Requeues_Every_Live_Key_At_Its_Current_Value()
    {
        using SessionHarness h = Harness();
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(2, LatestKit.Collect(received));

        // The value is admitted but never reaches the peer, so the key is still live when the session is resumed.
        DatagramKit.TransportOf(h.Client).DropNextDatagrams(4);
        SendResult result = h.Client.SendCopy(new SendHeader(2, 12), LatestKit.Payload(7, 80), SendOptions.Tracked);
        h.Run(30_000);
        Assert.Empty(received);
        Assert.Equal(1, LatestKit.LiveKeys(h.Client, 2));

        // A new epoch (PROTOCOL.md §4.1): the send side re-queues the live key, the receive side forgets its keys.
        LatestKit.Engine(h.Client).OnEpochReset(resumed: true);
        LatestKit.Engine(h.Server).OnEpochReset(resumed: true);

        // The counter restarted and the live key took version 1 of the new epoch, so the next value would be version 2.
        Assert.Equal(2u, LatestKit.NextVersion(h.Client, 2));
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(result.Token) == DeliveryStatus.Delivered, 5_000_000),
            "the re-queued value was not delivered");
        Assert.Single(received);
        Assert.True(LatestKit.Matches(received[0].Payload, 7, 80));
        Assert.Equal(1u, received[0].Version);
    }

    [Fact]
    public void A_Retired_Key_Is_Reported_Freed_And_Can_Be_Used_Again()
    {
        using SessionHarness h = Harness();
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(2, LatestKit.Collect(received));
        SendResult first = h.Client.SendCopy(new SendHeader(2, 33), LatestKit.Payload(1, 16), SendOptions.Tracked);
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(first.Token) == DeliveryStatus.Delivered), "the value was not delivered");

        Assert.Equal(SendStatus.Admitted, h.Client.RetireKey(2, 33));
        Assert.True(h.RunUntil(() => received.Count == 2), "the retirement was not delivered");
        Assert.Equal(ReceiveFlags.KeyRetired, received[1].Flags);
        Assert.Equal(33ul, received[1].Key);
        Assert.Empty(received[1].Payload);

        // The same key id can be used again in this epoch: both sides start from a fresh slot.
        SendResult again = h.Client.SendCopy(new SendHeader(2, 33), LatestKit.Payload(2, 16), SendOptions.Tracked);
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(again.Token) == DeliveryStatus.Delivered), "the reused key was not delivered");
        Assert.Equal(3, received.Count);
        Assert.True(LatestKit.Matches(received[2].Payload, 2, 16));
    }

    [Fact]
    public void Retiring_A_Key_With_An_Unacknowledged_Value_Cancels_It_And_Stops_The_Retries()
    {
        using SessionHarness h = Harness();
        DatagramKit.TransportOf(h.Client).DropNextDatagrams(8);
        SendResult result = h.Client.SendCopy(new SendHeader(2, 44), LatestKit.Payload(3, 32), SendOptions.Tracked);
        h.Run(40_000);
        long retries = DatagramKit.ChannelStats(h.Client, 2).Retries;

        Assert.Equal(SendStatus.Admitted, h.Client.RetireKey(2, 44));
        h.Run(20_000);
        Assert.Equal(DeliveryStatus.Canceled, h.Client.GetDeliveryStatus(result.Token));
        Assert.Equal(0, LatestKit.LiveKeys(h.Client, 2));
        h.Run(500_000);
        Assert.Equal(retries, DatagramKit.ChannelStats(h.Client, 2).Retries);
    }

    [Fact]
    public void Every_Send_Path_Keeps_Its_Own_Copy_Of_The_Value()
    {
        using SessionHarness h = Harness();
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(2, LatestKit.Collect(received));

        byte[] borrowed = LatestKit.Payload(1, 64);
        Assert.Equal(SendStatus.Admitted, h.Client.SendBorrowed(new SendHeader(2, 1), borrowed).Status);
        BufferLease owned = h.Client.RentBuffer(64);
        LatestKit.Payload(2, 64).CopyTo(h.Client.GetBufferSpan(in owned));
        Assert.Equal(SendStatus.Admitted, h.Client.SendOwned(new SendHeader(2, 2), owned, 64).Status);
        BufferLease page = h.Client.RentBuffer(64);
        LatestKit.Payload(3, 64).CopyTo(h.Client.GetBufferSpan(in page));
        Assert.Equal(SendStatus.Admitted, h.Client.SendGather(new SendHeader(2, 3), [page]).Status);
        Assert.Equal(SendStatus.Admitted, SendPinnedValue(h.Client, 4, LatestKit.Payload(4, 64)));

        // The caller's memory is reused at once; a retransmission must still send the right bytes.
        Array.Clear(borrowed);
        DatagramKit.TransportOf(h.Client).DropNextDatagrams(2);
        Assert.True(h.RunUntil(() => received.Count == 4, 5_000_000), $"{received.Count} of 4 values arrived");
        for (int id = 1; id <= 4; id++)
        {
            (ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags) value = received.Single(v => v.Key == (ulong)id);
            Assert.True(LatestKit.Matches(value.Payload, id, 64), $"value {id} was damaged");
        }
    }

    private static unsafe SendStatus SendPinnedValue(QuiclyPeer peer, ulong key, byte[] payload)
    {
        fixed (byte* data = payload)
        {
            return peer.SendPinned(new SendHeader(2, key), data, payload.Length).Status;
        }
    }
}

/// <summary>The budget constants of PROTOCOL.md §4.4, so the tests state them once.</summary>
internal static class ReliableLatestBudget
{
    public const int Transmissions = 16;
}
