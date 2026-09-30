using System.Runtime.InteropServices;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Review (regressions lens): contracts that were checked and hold, and that had no test of their own. These pass.
/// </summary>
public class ReviewRegressionsGuardTests
{
    /// <summary>
    /// <see cref="ChannelStatistics"/> is a public sequential struct: its new fields come after InFlightBytes, the last
    /// field of 0.2.x, so every earlier field keeps its offset.
    /// </summary>
    [Fact]
    public void New_ChannelStatistics_Fields_Are_Appended_After_The_Fields_Of_The_Previous_Release()
    {
        int lastOld = (int)Marshal.OffsetOf<ChannelStatistics>(nameof(ChannelStatistics.InFlightBytes));
        foreach (string field in new[] { nameof(ChannelStatistics.TransportCanceled), nameof(ChannelStatistics.TransportLost), nameof(ChannelStatistics.DrainQueueDrops) })
        {
            Assert.True((int)Marshal.OffsetOf<ChannelStatistics>(field) > lastOld, field);
        }
    }

    /// <summary>
    /// The behaviour change of the expiry anchor, with its bound. A host polls every frame (so on 0.2.x the pass clock was
    /// fresh and "expiry from admission" held exactly) and sends on a channel with the default expiry (33 ms) for a second
    /// without flushing. 0.2.x expired everything older than the expiry at the Flush; now the Flush hands over every
    /// message still queued, up to a second old. The burst is bounded by the send table (1 024 entries by default; the
    /// channel has no QueueLimitBytes by default), not by the expiry.
    /// </summary>
    [Fact]
    public void A_Host_That_Sends_For_A_Second_Without_Flushing_Transmits_At_Most_A_Send_Table_Of_Stale_Messages()
    {
        ChannelTable table = ChannelTable.Create().Add(2, "fx", ChannelMode.UnreliableUnordered).Build();
        using SessionHarness h = new(table: table, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        int admitted = 0;
        for (int frame = 0; frame < 60; frame++)
        {
            client.Poll();
            for (int i = 0; i < 40; i++)
            {
                if (client.SendCopy(new SendHeader(2), [1, 2, 3, 4]).IsAdmitted)
                {
                    admitted++;
                }
            }

            h.Network.Advance(16_667);
        }

        Assert.InRange(admitted, 1, h.ClientOptions.SendTableCapacity);
        Assert.True(admitted < 60 * 40, "the send table did not bound the backlog");
        client.Poll();
        client.Flush();
        ChannelStatistics channel = DatagramKit.ChannelStats(client, 2);
        Assert.Equal(0, channel.Expired);
        Assert.Equal(admitted, channel.Sent);
    }
}
