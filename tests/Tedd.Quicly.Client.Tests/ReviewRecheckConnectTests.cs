using System.Buffers.Binary;
using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Client.Tests;

/// <summary>
/// Review (recheck lens) of the pass-and-backlog rule of the drain queues (016038f). A pass starts with every Poll that
/// dispatches, and <c>QuiclyClient.ConnectAsync</c> polls the peer itself: the Poll in which the peer becomes Connected
/// moves whatever the server sent with its HelloAck into the drain queues. The application's first frame then starts with
/// a second Poll, and that makes those messages "backlog left undrained across a Poll" before the application had any
/// chance to drain them.
/// </summary>
/// <remarks>
/// Fixed since: the first pass of a session counts as drained for every channel (<c>ReceiveQueues.BeginPass</c>) — nobody
/// could drain before the pass in which the peer became Connected, whoever ran it — so what that pass queued is not backlog
/// when the application's first Poll begins. The description above is what the test found before the fix.
/// </remarks>
public class ReviewRecheckConnectTests
{
    [Theory]
    [InlineData(60, 1000)]
    [InlineData(300, 100)]
    public async Task What_The_Server_Sends_On_Admission_Reaches_A_Client_That_Polls_And_Drains_Every_Frame(int count, int size)
    {
        await using ClientFixture f = new();
        f.Server.PeerAdmitted += peer =>
        {
            byte[] payload = new byte[size];
            for (int i = 0; i < count; i++)
            {
                BinaryPrimitives.WriteInt32LittleEndian(payload, i);
                Assert.Equal(SendStatus.Admitted, peer.SendCopy(new SendHeader(2), payload, new SendOptions { ExpiryMicros = 10_000_000 }).Status);
            }
        };
        QuiclyClient client = f.CreateClient();
        ClientOptions options = f.Options();
        options.PeerOptions.ReceiveRingCapacity = 4096; // the default (the fixture's 64 would make the queue pool smaller still)

        QuiclyPeer peer = await client.ConnectAsync(f.EndPoint, options, TestContext.Current.CancellationToken);

        // The application's frame loop: Poll, then drain channel 2 completely.
        List<int> got = [];
        ReceivedMessage[] buffer = new ReceivedMessage[512];
        for (int frame = 0; frame < 20; frame++)
        {
            client.Poll();
            int n;
            while ((n = peer.Drain(2, buffer)) > 0)
            {
                for (int i = 0; i < n; i++)
                {
                    got.Add(BinaryPrimitives.ReadInt32LittleEndian(buffer[i].Payload));
                }

                peer.Release(buffer.AsSpan(0, n));
            }

            client.Flush();
            f.Step(1_000);
        }

        peer.GetStatistics(out PeerStatistics stats);
        Assert.Equal(0, stats.ReceiveRingDrops);
        Assert.Equal(0, stats.OutOfReceiveBuffers);
        Assert.True(got.Count == count,
            $"{got.Count} of {count} messages of {size} bytes reached the application that polls and drains every frame " +
            $"(DrainQueueDrops {stats.DrainQueueDrops}, first index delivered {(got.Count > 0 ? got[0] : -1)})");
    }
}
