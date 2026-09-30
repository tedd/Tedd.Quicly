using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Review (protocol lens): the same false resynchronisation as <see cref="ReviewProtocolResyncTests"/>, with every option
/// at its default. A sequenced message packed into a container next to a ReliableLatest value is sent without
/// cancel-on-blocked (PROTOCOL.md §4.5: "a packed container carries the flag only when every member is unreliable"), so it
/// waits in the transport's queue for as long as the link is busy; a later container that holds an Immediate (priority)
/// member overtakes it. The sequenced value it carries is then more than two seconds later than its successor.
/// </summary>
/// <remarks>
/// Fixed since: a sequence inside the reorder window (1 024 on a 16-bit channel, 65 536 on a 32-bit one) is a late message
/// however long the channel was quiet (<c>UnreliableSequencedEngine.ResyncWindow16</c>). The tests now pin the fix.
/// </remarks>
public class ReviewProtocolResyncDefaultTests
{
    /// <summary>2 sequenced keyed (default expiry) · 3 sequenced unkeyed (default expiry) · 4 ReliableLatest.</summary>
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "moves", ChannelMode.UnreliableSequenced, o => o.Keyed = true)
        .Add(3, "clock", ChannelMode.UnreliableSequenced)
        .Add(4, "state", ChannelMode.ReliableLatest)
        .Build();

    [Theory]
    [InlineData((ushort)2, 1UL)]
    [InlineData((ushort)3, 0UL)]
    public void With_Default_Options_An_Older_Value_Packed_Next_To_A_Reliable_One_Is_Not_Delivered_After_A_Newer_Value(ushort channel, ulong key)
    {
        // 100 kbit/s: 12 500 bytes per second.
        using SessionHarness h = new(link: new LinkOptions { BandwidthBitsPerSecond = 100_000 }, table: Table,
            client: o =>
            {
                DatagramKit.Quiet(o);
                LatestKit.Roomy(o);
            },
            server: o =>
            {
                DatagramKit.Quiet(o);
                LatestKit.Roomy(o);
            });
        Assert.True(h.ClientOptions.DropWhenBlocked);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(channel, Handlers.Collect(got));
        h.Server.RegisterHandler(4, static (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => { });
        h.Run(50_000);

        // Tick 1: a burst of state (ReliableLatest, 40 keys of 1 000 bytes): more than three seconds of link time.
        for (ulong k = 100; k < 140; k++)
        {
            Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(4, k), new byte[1_000]).Status);
        }

        h.Client.Flush();

        // Tick 2: the old value of the key and one small piece of state, packed into one container.
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(channel, key), [1]).Status);
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(4, 1), [7]).Status);
        h.Client.Flush();

        // Tick 3: the new value of the key, and an urgent piece of state (Immediate) that flushes both in one container.
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(channel, key), [2]).Status);
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(4, 2), [8], SendOptions.Immediate).Status);
        h.Run(10_000_000);

        // Nothing was cancelled or expired on the sender: both values arrived.
        ChannelStatistics sent = DatagramKit.ChannelStats(h.Client, channel);
        Assert.Equal(2, sent.Sent);
        Assert.Equal(0, sent.Expired);
        Assert.Equal(0, sent.TransportCanceled);
        string order = string.Join(",", got.Select(m => m.Payload[0]));
        Assert.True(got.Count > 0 && got[^1].Payload[0] == 2,
            $"delivered in order [{order}]: the older value replaced the newer one (SequenceResyncs {DatagramKit.Statistics(h.Server).SequenceResyncs})");
        Assert.Equal("2", order);
        Assert.Equal(0, DatagramKit.Statistics(h.Server).SequenceResyncs);
    }
}
