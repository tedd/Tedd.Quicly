using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Server.Tests;

/// <summary>
/// Every peer of a server rents from the server's one pool, and the server disposes a peer right after its
/// <see cref="QuiclyServer.PeerClosed"/> handlers returned. A payload the application retained and releases later — the
/// next tick, a job that finishes after the player left — is therefore always released on a disposed peer, and the block
/// has to reach the shared pool all the same.
/// </summary>
public class LateReleaseTests
{
    [Fact]
    public async Task Peer_Churn_With_Late_Release_Does_Not_Drain_The_Pool()
    {
        await using ServerFixture f = new();
        List<(QuiclyPeer Peer, ReceiveLease Lease)> kept = [];
        f.Server.PeerAdmitted += peer => peer.RegisterHandler(2,
            (QuiclyPeer p, in ReceiveHeader header, ReadOnlySpan<byte> _) => kept.Add((p, p.Retain(in header))));

        // One full cycle first, so the baseline is a pool that has seen a peer come and go.
        Cycle(f, kept, 0);
        long baseline = f.Server.Allocator.GetStatistics().TotalRentedBytes;

        const int Cycles = 200;
        for (int i = 1; i <= Cycles; i++)
        {
            Cycle(f, kept, i);
        }

        Assert.Equal(baseline, f.Server.Allocator.GetStatistics().TotalRentedBytes);
    }

    /// <summary>Connect, send one message the server retains, close, and release the payload only after the server disposed the peer.</summary>
    private static void Cycle(ServerFixture f, List<(QuiclyPeer Peer, ReceiveLease Lease)> kept, int cycle)
    {
        QuiclyPeer client = f.ConnectAdmitted();
        Assert.Equal(SendStatus.Admitted, client.SendCopy(new SendHeader(2), [1, 2, 3]).Status);
        Assert.True(f.RunUntil(() => kept.Count == 1), $"cycle {cycle}: the message never reached the server's handler");

        int closed = f.Closed.Count;
        client.Close(new CloseReason(QuiclyErrorCode.NoError, "bye"));
        Assert.True(f.RunUntil(() => f.Closed.Count == closed + 1 && client.State == PeerState.Closed), $"cycle {cycle}: the peer never closed");
        f.DisposeClient(client);
        f.Run(5_000);

        (QuiclyPeer peer, ReceiveLease lease) = kept[0];
        kept.Clear();
        Assert.True(peer.IsDisposed, "the server disposes a peer right after PeerClosed");
        peer.Release(in lease);
    }
}
