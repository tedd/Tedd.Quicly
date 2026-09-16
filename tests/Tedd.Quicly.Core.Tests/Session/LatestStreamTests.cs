using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Large ReliableLatest values travel on one-message group streams (PROTOCOL.md §3.2 and §8 item 8): the preamble carries
/// the version as the group id, the single frame repeats it as its <c>Sequence</c>, a newer version aborts the older
/// stream of the same key, and the receiver bounds the concurrent streams per channel (§7 <c>MaxGroups</c>).
/// </summary>
public class LatestStreamTests
{
    private static readonly ChannelTable Table = LatestTables.Main;

    private static SessionHarness Harness(LinkOptions? link = null) =>
        new(link: link ?? new LinkOptions { DelayMicros = 2_000 }, table: Table,
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

    [Fact]
    public void A_Value_Larger_Than_One_Datagram_Arrives_Byte_Exact_On_A_Group_Stream()
    {
        using SessionHarness h = Harness();
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(2, LatestKit.Collect(received));
        byte[] value = LatestKit.Payload(3, 16_000);
        SendResult result = h.Client.SendCopy(new SendHeader(2, 5), value, SendOptions.Tracked);
        Assert.Equal(SendStatus.Admitted, result.Status);

        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(result.Token) == DeliveryStatus.Delivered, 5_000_000),
            $"status {h.Client.GetDeliveryStatus(result.Token)}");
        Assert.Single(received);
        Assert.Equal(5ul, received[0].Key);
        Assert.Equal(1u, received[0].Version);
        Assert.True(LatestKit.Matches(received[0].Payload, 3, 16_000));
        Assert.True(DatagramKit.Statistics(h.Client).StreamSends >= 1, "the value did not go out on a stream");

        // The stream was closed at both ends, so nothing of it is left behind.
        h.Run(100_000);
        Assert.Equal(0, DatagramKit.Statistics(h.Server).ReceiveBytesOutstanding);
    }

    [Fact]
    public void A_Newer_Version_Replaces_A_Large_Value_And_Only_The_Newest_Is_Delivered()
    {
        using SessionHarness h = Harness(new LinkOptions { DelayMicros = 2_000, BandwidthBitsPerSecond = 4_000_000 });
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(2, LatestKit.Collect(received));

        SendToken stale = h.Client.SendCopy(new SendHeader(2, 9), LatestKit.Payload(1, 20_000), SendOptions.Tracked).Token;
        h.Client.Flush();
        h.Network.Advance(1_000);

        // The first stream is still sending when the newer version arrives: it is aborted and a new stream opened.
        SendToken newest = h.Client.SendCopy(new SendHeader(2, 9), LatestKit.Payload(2, 20_000), SendOptions.Tracked).Token;
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(newest) == DeliveryStatus.Delivered, 10_000_000),
            $"status {h.Client.GetDeliveryStatus(newest)}");
        Assert.Equal(DeliveryStatus.Superseded, h.Client.GetDeliveryStatus(stale));
        Assert.True(LatestKit.Matches(received[^1].Payload, 2, 20_000), "the newest value was damaged");
        Assert.All(received, value => Assert.Equal(9ul, value.Key));

        // Whatever the aborted stream had staged was released.
        h.Run(200_000);
        Assert.Equal(0, DatagramKit.Statistics(h.Server).ReceiveBytesOutstanding);
    }

    [Fact]
    public void Many_Large_Values_In_A_Row_Reuse_The_Stream_Allowance()
    {
        // MaxGroups 1 on channel 8: every value must have released its stream before the next one opens.
        using SessionHarness h = Harness();
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(8, LatestKit.Collect(received));
        for (int i = 0; i < 5; i++)
        {
            SendResult result = h.Client.SendCopy(new SendHeader(8, 1), LatestKit.Payload(i, 4_000), SendOptions.Tracked);
            Assert.Equal(SendStatus.Admitted, result.Status);
            Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(result.Token) == DeliveryStatus.Delivered, 5_000_000),
                $"value {i}: status {h.Client.GetDeliveryStatus(result.Token)}");
        }

        Assert.Equal(5, received.Count);
        Assert.True(LatestKit.Matches(received[4].Payload, 4, 4_000));
    }

    [Fact]
    public void A_Group_Stream_Beyond_The_Receiver_Limit_Is_Reset_And_The_Value_Is_Delivered_Later()
    {
        // Two keys send large values at once on a channel that accepts one stream at a time: the second is reset
        // LimitExceeded (PROTOCOL.md §7) and the retry delivers it once the first stream is gone.
        using SessionHarness h = Harness(new LinkOptions { DelayMicros = 2_000, BandwidthBitsPerSecond = 8_000_000 });
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(8, LatestKit.Collect(received));
        SendToken a = h.Client.SendCopy(new SendHeader(8, 1), LatestKit.Payload(1, 20_000), SendOptions.Tracked).Token;
        SendToken b = h.Client.SendCopy(new SendHeader(8, 2), LatestKit.Payload(2, 20_000), SendOptions.Tracked).Token;

        Assert.True(h.RunUntil(
            () => h.Client.GetDeliveryStatus(a) == DeliveryStatus.Delivered && h.Client.GetDeliveryStatus(b) == DeliveryStatus.Delivered,
            20_000_000), $"a {h.Client.GetDeliveryStatus(a)}, b {h.Client.GetDeliveryStatus(b)}");
        Assert.Equal(2, received.Count(v => v.Payload.Length == 20_000));
        Assert.True(LatestKit.Matches(received.Last(v => v.Key == 1).Payload, 1, 20_000));
        Assert.True(LatestKit.Matches(received.Last(v => v.Key == 2).Payload, 2, 20_000));
    }

    [Fact]
    public void A_Compressed_Large_Value_Round_Trips_Through_The_Stream()
    {
        using SessionHarness h = Harness();
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(5, LatestKit.Collect(received));

        // Channel 5 compresses; 8 000 highly compressible bytes still exceed the datagram limit after compression? No:
        // the value fits one datagram once compressed, which is exactly the point — the engine decides on the wire size.
        byte[] compressible = new byte[8_000];
        SendResult small = h.Client.SendCopy(new SendHeader(5, 1), compressible, SendOptions.Tracked);
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(small.Token) == DeliveryStatus.Delivered, 5_000_000), "the compressed value was not delivered");
        Assert.Single(received);
        Assert.Equal(8_000, received[0].Payload.Length);

        // Noise does not compress, so this one needs the group stream.
        byte[] noise = OrderedKit.Noise(8_000, 7);
        SendResult large = h.Client.SendCopy(new SendHeader(5, 2), noise, SendOptions.Tracked);
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(large.Token) == DeliveryStatus.Delivered, 5_000_000), "the large value was not delivered");
        Assert.Equal(2, received.Count);
        Assert.Equal(noise, received[1].Payload);
    }

    [Fact]
    public void A_Large_Value_Lost_On_Its_Stream_Is_Retransmitted()
    {
        using SessionHarness h = Harness(new LinkOptions { DelayMicros = 2_000, StreamLossPercent = 20 });
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(2, LatestKit.Collect(received));
        SendResult result = h.Client.SendCopy(new SendHeader(2, 77), LatestKit.Payload(8, 12_000), SendOptions.Tracked);
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(result.Token) == DeliveryStatus.Delivered, 20_000_000),
            $"status {h.Client.GetDeliveryStatus(result.Token)}");
        Assert.True(LatestKit.Matches(received[^1].Payload, 8, 12_000));
        Assert.Equal(PeerState.Connected, h.Client.State);
    }
}
