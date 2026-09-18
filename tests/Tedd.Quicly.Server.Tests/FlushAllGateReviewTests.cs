using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Server.Tests;

/// <summary>
/// Adversarial review of the FlushAll gate (QuiclyPeer.FlushGate.cs): each test sets up a peer that the gate proves idle
/// although a Flush of it would still have done something. They pass with the gate disabled (every peer flushed).
/// </summary>
public class FlushAllGateReviewTests
{
    private const long Tick = 16_000;

    private static readonly ChannelTable RequestTable = ChannelTable.Create()
        .Add(2, "state", ChannelMode.UnreliableUnordered)
        .Add(4, "rpc", ChannelMode.ReliableOrdered, o => o.RequestResponse = true)
        .Build();

    /// <summary>Game ticks: network, clients, then the server's PollAll and FlushAll(tick).</summary>
    private static void Ticks(ServerFixture f, int count, ref uint tick)
    {
        for (int i = 0; i < count; i++)
        {
            f.Network.Advance(Tick);
            f.PumpClients();
            f.Server.PollAll();
            f.Server.FlushAll(++tick);
        }
    }

    [Fact]
    public async Task A_Canceled_Request_Gives_Its_Slot_Back_In_The_Next_FlushAll()
    {
        // A canceled SendRequestAsync leaves its slot in the request table until the game thread's next pass sweeps it
        // (RunPollDeadlines, "in every Poll and Flush"). Nothing about the cancel raises the work signal or a level the gate
        // reads, so an idle peer keeps the dead slot until its next ping: with all 256 slots taken, the next request fails.
        static void Quiet(PeerOptions o)
        {
            o.PingInterval = TimeSpan.FromSeconds(10);
            o.FastLockDuration = TimeSpan.Zero;
        }

        await using ServerFixture f = new(o =>
        {
            o.Channels = RequestTable;
            Quiet(o.PeerOptions);
        });
        f.ConfigureClient = Quiet;
        QuiclyPeer client = f.Connect(table: RequestTable);
        Assert.True(f.RunUntil(() => client.State == PeerState.Connected), "not admitted: " + client.State);
        int requestsSeen = 0;
        client.RegisterHandler(4, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => requestsSeen++); // never answers
        QuiclyPeer peer = f.ServerPeerOf(client);
        uint tick = 0;
        Ticks(f, 10, ref tick);

        using CancellationTokenSource cancel = new();
        ValueTask<ReceiveLease> canceled = peer.SendRequestAsync(new SendHeader(4), new byte[] { 0 }, TimeSpan.Zero, cancel.Token);
        List<Task<ReceiveLease>> outstanding = [];
        for (int i = 1; i < 256; i++)
        {
            outstanding.Add(peer.SendRequestAsync(new SendHeader(4), new byte[] { (byte)i }, TimeSpan.Zero).AsTask());
        }

        for (int i = 0; i < 100 && requestsSeen < 256; i++)
        {
            Ticks(f, 1, ref tick);
        }

        Assert.Equal(256, requestsSeen);
        Ticks(f, 5, ref tick); // every carrier acknowledged: the server peer is idle, with 256 requests waiting

        cancel.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await canceled);
        Ticks(f, 2, ref tick); // the pass after the cancel takes the canceled request out of the table

        ValueTask<ReceiveLease> next = peer.SendRequestAsync(new SendHeader(4), new byte[] { 1 }, TimeSpan.Zero);
        Assert.False(next.IsFaulted, "the canceled request still holds its slot: " + (next.IsFaulted ? next.AsTask().Exception!.InnerException!.Message : ""));
        Assert.All(outstanding, t => Assert.False(t.IsCompleted));
    }
}
