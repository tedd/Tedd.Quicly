using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Threading;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// <see cref="QuiclyPeer.SendShared"/> (ARCHITECTURE.md §4.1): one serialisation, zero copies, and the peer's reference on
/// the shared lease is released exactly once on every path — admitted and delivered, discarded, the session closed with the
/// send queued, and the peer disposed with the send in flight.
/// </summary>
public class SharedSendTests
{
    private static SessionHarness NewPair(SharedPool pool, Action<PeerOptions>? client = null) =>
        new(client: o =>
            {
                QuietOptions.Apply(o);
                o.Allocator = pool.Allocator;
                client?.Invoke(o);
            },
            server: QuietOptions.Apply);

    [Fact]
    public void An_Admitted_Shared_Send_Retains_Once_And_Releases_When_The_Transport_Is_Done()
    {
        using SharedPool pool = new();
        using SessionHarness h = NewPair(pool);
        SharedLease shared = pool.Share(64);
        Assert.Equal(1, pool.Count(in shared));

        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(2, Handlers.Collect(got));
        Assert.True(h.Client.SendShared(new SendHeader(2), pool.Table, in shared, 64).IsAdmitted);
        Assert.Equal(2, pool.Count(in shared));

        h.Run(200_000, 1_000);
        Assert.Single(got);
        Assert.Equal(64, got[0].Payload.Length);
        Assert.Equal(1, got[0].Payload[0]);
        Assert.Equal(0, got[0].Header.RawLength);
        // Exactly one release: the host's own reference is all that is left, and it returns the block.
        Assert.Equal(1, pool.Count(in shared));
        Assert.True(pool.Table.Release(in shared));
    }

    [Fact]
    public void A_Shared_Send_Reaches_An_Ordered_Channel_And_Releases_At_The_Acknowledgement()
    {
        using SharedPool pool = new();
        using SessionHarness h = NewPair(pool);
        SharedLease shared = pool.Share(1536, seed: 5);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(4, Handlers.Collect(got));

        SendResult result = h.Client.SendShared(new SendHeader(4), pool.Table, in shared, 1536, SendOptions.Tracked);
        Assert.True(result.IsAdmitted);
        Assert.Equal(2, pool.Count(in shared));

        h.Run(500_000, 1_000);
        Assert.Single(got);
        Assert.Equal(1536, got[0].Payload.Length);
        Assert.Equal(5, got[0].Payload[0]);
        Assert.Equal(DeliveryStatus.Delivered, h.Client.GetDeliveryStatus(result.Token));
        Assert.Equal(1, pool.Count(in shared));
        Assert.True(pool.Table.Release(in shared));
    }

    [Fact]
    public void Two_Shared_Sends_Packed_Into_One_Container_Release_When_Their_Bytes_Were_Copied()
    {
        using SharedPool pool = new();
        using SessionHarness h = NewPair(pool);
        SharedLease a = pool.Share(64, seed: 1);
        SharedLease b = pool.Share(64, seed: 100);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(2, Handlers.Collect(got));

        Assert.True(h.Client.SendShared(new SendHeader(2), pool.Table, in a, 64).IsAdmitted);
        Assert.True(h.Client.SendShared(new SendHeader(2), pool.Table, in b, 64).IsAdmitted);
        Assert.Equal(2, pool.Count(in a));
        Assert.Equal(2, pool.Count(in b));

        // One pass packs both into a container, which copies the bytes and releases the members at once.
        h.Client.Flush();
        Assert.Equal(1, pool.Count(in a));
        Assert.Equal(1, pool.Count(in b));
        h.Run(200_000, 1_000);
        Assert.Equal(2, got.Count);
        Assert.True(pool.Table.Release(in a));
        Assert.True(pool.Table.Release(in b));
    }

    [Fact]
    public void A_Rejected_Shared_Send_Takes_No_Reference()
    {
        using SharedPool pool = new();
        using SessionHarness h = NewPair(pool);
        SharedLease shared = pool.Share(4096);

        // Too large for an unreliable datagram channel: the rejection happens after the payload was prepared.
        Assert.Equal(SendStatus.TooLarge, h.Client.SendShared(new SendHeader(2), pool.Table, in shared, 4096).Status);
        Assert.Equal(1, pool.Count(in shared));

        Assert.Equal(SendStatus.InvalidChannel, h.Client.SendShared(new SendHeader(999), pool.Table, in shared, 64).Status);
        Assert.Equal(1, pool.Count(in shared));
        Assert.True(pool.Table.Release(in shared));
    }

    [Fact]
    public void A_Canceled_Shared_Send_Releases_Once()
    {
        using SharedPool pool = new();
        using SessionHarness h = NewPair(pool);
        SharedLease shared = pool.Share(64);
        SendResult result = h.Client.SendShared(new SendHeader(2), pool.Table, in shared, 64, SendOptions.Tracked);
        Assert.True(result.IsAdmitted);
        Assert.Equal(2, pool.Count(in shared));

        Assert.True(h.Client.TryCancel(result.Token));
        h.Client.Poll();
        Assert.Equal(DeliveryStatus.Canceled, h.Client.GetDeliveryStatus(result.Token));
        Assert.Equal(1, pool.Count(in shared));
        Assert.True(pool.Table.Release(in shared));
    }

    [Fact]
    public void Closing_The_Peer_With_A_Shared_Send_Queued_Releases_Once()
    {
        using SharedPool pool = new();
        using SessionHarness h = NewPair(pool);
        SharedLease shared = pool.Share(1536, seed: 3);
        SendResult result = h.Client.SendShared(new SendHeader(4), pool.Table, in shared, 1536, SendOptions.Tracked);
        Assert.True(result.IsAdmitted);
        Assert.Equal(2, pool.Count(in shared));

        // Queued on an ordered channel and never flushed: the close completes it Disconnected.
        h.Client.Close();
        Assert.True(h.RunUntilClosed());
        Assert.Equal(DeliveryStatus.Disconnected, h.Client.GetDeliveryStatus(result.Token));
        Assert.Equal(1, pool.Count(in shared));
        Assert.True(pool.Table.Release(in shared));
    }

    [Fact]
    public void Disposing_The_Peer_With_A_Shared_Send_In_Flight_Releases_Once()
    {
        using SharedPool pool = new();
        SharedLease shared = pool.Share(64);
        SessionHarness h = NewPair(pool);
        try
        {
            Assert.True(h.Client.SendShared(new SendHeader(2), pool.Table, in shared, 64).IsAdmitted);
            // Handed to the transport, completion not yet delivered (the simulator completes at the next advance).
            h.Client.Flush();
            Assert.Equal(2, pool.Count(in shared));
        }
        finally
        {
            h.Dispose();
        }

        Assert.Equal(1, pool.Count(in shared));
        Assert.True(pool.Table.Release(in shared));
    }

    [Fact]
    public void A_Shared_Send_From_Another_Thread_Is_Copied_And_Takes_No_Reference()
    {
        using SharedPool pool = new();
        using SessionHarness h = NewPair(pool, client: o => o.ThreadSafeSend = true);
        SharedLease shared = pool.Share(64, seed: 11);
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        h.Server!.RegisterHandler(2, Handlers.Collect(got));

        SendStatus status = SendStatus.NotConnected;
        Thread producer = new(() => status = h.Client.SendShared(new SendHeader(2), pool.Table, in shared, 64).Status);
        producer.Start();
        producer.Join();
        Assert.Equal(SendStatus.Admitted, status);
        Assert.Equal(1, pool.Count(in shared));

        h.Run(200_000, 1_000);
        Assert.Single(got);
        Assert.Equal(11, got[0].Payload[0]);
        Assert.Equal(1, pool.Count(in shared));
        Assert.True(pool.Table.Release(in shared));
    }

    [Fact]
    public void Shared_Sends_Do_Not_Allocate_In_Steady_State()
    {
        using SharedPool pool = new();
        using SessionHarness h = NewPair(pool);
        SharedLease shared = pool.Share(64);
        int received = 0;
        h.Server!.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++);
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server;
        SharedLeaseTable table = pool.Table;
        AllocationAssert.NoAllocations(() =>
        {
            client.SendShared(new SendHeader(2), table, in shared, 64);
            client.Flush();
            h.Network.Advance(1_000);
            client.Poll();
            server.Poll();
        });

        Assert.True(received > 1_000);
        // Every one of those sends gave its reference back.
        Assert.Equal(1, pool.Count(in shared));
    }

    [Fact]
    public void Shared_Send_Validates_Its_Arguments()
    {
        using SharedPool pool = new();
        using SessionHarness h = NewPair(pool);
        SharedLease shared = pool.Share(64);
        Assert.Throws<ArgumentNullException>(() => h.Client.SendShared(new SendHeader(2), null!, in shared, 64));
        Assert.Throws<ArgumentOutOfRangeException>(() => h.Client.SendShared(new SendHeader(2), pool.Table, in shared, -1));
        Assert.Throws<ArgumentOutOfRangeException>(() => h.Client.SendShared(new SendHeader(2), pool.Table, in shared, 65));
        // A lease that refers to no block has length 0, so it cannot carry payload bytes.
        SharedLease empty = default;
        Assert.Throws<ArgumentOutOfRangeException>(() => h.Client.SendShared(new SendHeader(2), pool.Table, in empty, 1));
        // An empty payload needs no block and takes no reference.
        Assert.True(h.Client.SendShared(new SendHeader(2), pool.Table, in empty, 0).IsAdmitted);
        h.Run(100_000, 1_000);
        Assert.Equal(1, pool.Count(in shared));
        Assert.True(pool.Table.Release(in shared));
    }
}
