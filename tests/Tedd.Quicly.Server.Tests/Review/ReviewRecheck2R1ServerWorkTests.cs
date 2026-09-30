using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Server.Tests.Review;

/// <summary>
/// Review (recheck 2, round 1) of 3a18b6d at the server: <c>QuiclyServer.PollSlot</c> asks
/// <see cref="QuiclyPeer.HasPendingWork"/>, which now re-arms the peer's work signal when it answers false. PollAll must
/// neither sleep on a peer with work nor poll a peer that only has values waiting for the application's Drain.
/// </summary>
public class ReviewRecheck2R1ServerWorkTests
{
    /// <summary>2 unordered with a handler; 3 keyed sequenced, coalescing (a mailbox), read with Drain; 6 ReliableLatest, read with Drain.</summary>
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "fx", ChannelMode.UnreliableUnordered)
        .Add(3, "state", ChannelMode.UnreliableSequenced, o =>
        {
            o.Keyed = true;
            o.CoalesceOnReceive = true;
            o.MaxKeys = 64;
        })
        .Build();

    private static long Polled(QuiclyServer server)
    {
        server.GetStatistics(out ServerStatistics statistics);
        return statistics.PeersPolled;
    }

    [Fact]
    public async Task PollAll_Does_Not_Poll_For_Mailbox_Values_Of_A_Drain_Channel_And_Never_Misses_A_Handler_Message_After_Them()
    {
        int handled = 0;
        await using ServerFixture f = new(o => o.Channels = Table);
        f.Server.PeerAdmitted += peer => peer.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => handled++);
        QuiclyPeer client = f.Connect(table: Table);
        Assert.True(f.RunUntil(() => client.State == PeerState.Connected));
        QuiclyPeer peer = f.ServerPeerOf(client);
        f.Run(4_000_000, step: 10_000); // let the fast-lock pings settle
        f.Server.PollAll();
        Assert.False(peer.HasPendingWork);

        ReceivedMessage[] buffer = new ReceivedMessage[64];
        int drained = 0;
        long idlePolls = 0;
        for (int round = 0; round < 200; round++)
        {
            // Values for the Drain-style channel: several transport callbacks, the probe stays clear.
            for (int i = 0; i < 3; i++)
            {
                Assert.True(client.SendCopy(new SendHeader(3, (ulong)(i + 1)), [(byte)round, (byte)i]).IsAdmitted);
                client.Flush();
                f.Step(1_000);
            }

            long before = Polled(f.Server);
            for (int i = 0; i < 5; i++)
            {
                Assert.Equal(0, f.Server.PollAll());
            }

            idlePolls += Polled(f.Server) - before;
            if (round % 3 == 0)
            {
                int n = peer.Drain(3, buffer);
                drained += n;
                peer.Release(buffer.AsSpan(0, n));
            }

            // Ordinary work: the very next PollAll must dispatch it.
            Assert.True(client.SendCopy(new SendHeader(2), [7]).IsAdmitted);
            client.Flush();
            f.Step(1_000);
            int dispatched = f.Server.PollAll();
            Assert.True(dispatched == 1 && handled == round + 1,
                $"round {round}: PollAll dispatched {dispatched}, the handler has seen {handled} of {round + 1} (HasPendingWork {peer.HasPendingWork})");
        }

        Assert.True(drained > 0);
        // 1 000 PollAll calls over 600 mailbox arrivals; the peer's own ping deadline accounts for the odd poll.
        Assert.True(idlePolls <= 3, $"PollAll polled the peer {idlePolls} time(s) although only values for Drain were waiting");
    }
}
