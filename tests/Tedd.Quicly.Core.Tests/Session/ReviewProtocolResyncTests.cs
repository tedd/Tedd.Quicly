using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Review (protocol lens): the two-second resynchronisation of the UnreliableSequenced sequence clock (PROTOCOL.md §8)
/// rests on "a datagram is never more than two seconds later than its successors". The library itself breaks that
/// premise: an Immediate send is handed over with the transport's priority flag and overtakes datagrams that wait in the
/// transport's queue, and with <see cref="PeerOptions.DropWhenBlocked"/> off (or a transport that does not honour
/// cancel-on-blocked) those wait "however old they have become" (PROTOCOL.md §4.5).
/// </summary>
/// <remarks>
/// Fixed since: a sequence inside the reorder window (1 024 on a 16-bit channel, 65 536 on a 32-bit one) is a late message
/// however long the channel was quiet (<c>UnreliableSequencedEngine.ResyncWindow16</c>). The tests now pin the fix.
/// </remarks>
public class ReviewProtocolResyncTests
{
    /// <summary>3 = sequenced keyed (32-bit), 6 = sequenced unkeyed (16-bit); expiry off on both.</summary>
    [Theory]
    [InlineData((ushort)3, 1UL)]
    [InlineData((ushort)6, 0UL)]
    public void An_Older_Value_Queued_In_The_Transport_Behind_A_Newer_Immediate_One_Is_Not_Delivered_After_It(ushort channel, ulong key)
    {
        // 100 kbit/s: 12 500 bytes per second, so 40 datagrams of 1 000 bytes keep the link busy for more than three seconds.
        using SessionHarness h = new(link: new LinkOptions { BandwidthBitsPerSecond = 100_000 }, table: DatagramTables.Main,
            client: o =>
            {
                DatagramKit.Quiet(o);
                o.DropWhenBlocked = false;
            },
            server: DatagramKit.Quiet);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(channel, Handlers.Collect(got));
        h.Run(50_000);

        // One tick: a burst on another channel, then the old value of the key. All of it waits in the transport's queue.
        for (int i = 0; i < 40; i++)
        {
            Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(2), new byte[1_000]).Status);
        }

        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(channel, key), [1]).Status);
        h.Client.Flush();

        // The next value of the same key is urgent: Immediate, so it overtakes the queue.
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(channel, key), [2], SendOptions.Immediate).Status);
        h.Run(8_000_000);

        // Both arrived; the old one more than two seconds after the new one.
        Assert.Equal(0, DatagramKit.ChannelStats(h.Client, channel).TransportCanceled);
        Assert.Equal(0, DatagramKit.ChannelStats(h.Client, channel).TransportLost);
        string order = string.Join(",", got.Select(m => m.Payload[0]));

        // UnreliableSequenced: "only a message newer than the last accepted one of its key (or of the channel) is delivered".
        // Value 1 is older than value 2 and must be dropped as stale, as it is on main.
        Assert.True(got.Count > 0 && got[^1].Payload[0] == 2,
            $"delivered in order [{order}]: the older value replaced the newer one (SequenceResyncs {DatagramKit.Statistics(h.Server!).SequenceResyncs})");
        Assert.Equal("2", order);
        Assert.Equal(0, DatagramKit.Statistics(h.Server!).SequenceResyncs);
    }
}
