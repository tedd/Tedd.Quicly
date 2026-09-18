using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Contract findings of the review of the performance pass (691f20d..perf/integration). Each test passes on 691f20d and
/// states the documented behaviour the pass changed.
/// </summary>
public class ReviewPerfContractTests
{
    private static SlabAllocatorOptions Pool(int smallBlocks = 256) => new()
    {
        FreeListShards = 1,
        ValidateLeases = true,
        SizeClasses = [new(64, smallBlocks), new(256, 64), new(1536, 32), new(4096, 8)],
    };

    private static SessionHarness Harness(Action<PeerOptions> server) => new(
        table: DatagramTables.Main,
        client: DatagramKit.Quiet,
        server: o =>
        {
            DatagramKit.Quiet(o);
            server(o);
        });

    private static void SendAndDeliver(SessionHarness h, ref int next, int count, int length, Func<int> received, ushort channel = 2)
    {
        int target = received() + count;
        for (int i = 0; i < count; i++)
        {
            Assert.True(h.Client.SendCopy(new SendHeader(channel), DatagramKit.Payload(next++, length)).IsAdmitted);
        }

        Assert.True(h.RunUntil(() => received() >= target, 1_000_000), $"{received()} of {target} messages arrived");
    }

    /// <summary>
    /// <see cref="PeerOptions.ReceiveBudgetBytes"/> bounds the receive leases the peer <em>holds</em> (ring, mailboxes,
    /// staging, reassembly), and a compressed message is decoded on the game thread into a second receive lease within that
    /// budget (PROTOCOL.md §7). Messages whose handlers have returned are no longer held by anyone
    /// (<see cref="PeerStatistics.ReceiveBytesOutstanding"/> says so), so they must not take budget from the decoder: a
    /// message whose compressed and decoded leases fit next to what the application really holds must decode. Receive-lease
    /// recycling (45d6d1f) keeps dispatched leases parked, counted in the budget, and only the transport thread gives them
    /// back before one of <em>its</em> rents would fail; the game-thread decoder (<c>TryRentReceiveFromPool</c>) does not,
    /// so the message is dropped as a decode failure.
    /// </summary>
    [Fact]
    public void A_Compressed_Message_That_Fits_The_Receive_Budget_Next_To_The_Held_Leases_Is_Decoded()
    {
        // Budget 2 KiB. Held by the application: one 256-byte block and one 64-byte block (320 bytes).
        using SessionHarness h = Harness(o =>
        {
            o.AllocatorOptions = Pool();
            o.ReceiveBudgetBytes = 2048;
        });
        QuiclyPeer server = h.Server!;
        List<ReceiveLease> retained = [];
        int plain = 0;
        int decoded = 0;
        byte[]? decodedPayload = null;
        server.RegisterHandler(2, (QuiclyPeer peer, in ReceiveHeader header, ReadOnlySpan<byte> payload) =>
        {
            plain++;
            if (payload.Length != 64)
            {
                retained.Add(peer.Retain(in header));
            }
        });
        server.RegisterHandler(5, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            decoded++;
            decodedPayload = payload.ToArray();
        });

        int next = 0;
        SendAndDeliver(h, ref next, 1, 200, () => plain); // retained: a 256-byte block
        SendAndDeliver(h, ref next, 1, 40, () => plain);  // retained: a 64-byte block
        SendAndDeliver(h, ref next, 4, 64, () => plain);  // handled and finished: nothing held
        Assert.Equal(2, retained.Count);
        Assert.Equal(320, DatagramKit.Statistics(server).ReceiveBytesOutstanding);

        // 1 000 zero bytes compress to a few bytes (a 64-byte block); the decoded copy needs a 1 536-byte block.
        // 320 held + 64 compressed + 1 536 decoded = 1 920 bytes, within the 2 048-byte budget.
        byte[] raw = new byte[1000];
        Assert.True(h.Client.SendCopy(new SendHeader(5), raw).IsAdmitted);
        Assert.True(h.RunUntil(() => decoded > 0 || DatagramKit.Statistics(server).DecodeFailures > 0, 1_000_000));

        Assert.Equal(0, DatagramKit.Statistics(server).DecodeFailures);
        Assert.Equal(1, decoded);
        Assert.Equal(raw, decodedPayload);

        foreach (ReceiveLease lease in retained)
        {
            server.Release(in lease);
        }
    }

    /// <summary>
    /// A peer without <see cref="PeerOptions.Allocator"/> gets a private allocator sized by
    /// <see cref="PeerOptions.AllocatorOptions"/> that serves its send leases and its receive leases. Once the handlers of
    /// received messages have returned, their blocks belong to the pool again, so a burst of received messages must not
    /// leave the send path short of blocks. With receive-lease recycling (45d6d1f, 7988e70) up to an eighth of the receive
    /// budget (32 KiB by default) of dispatched receive blocks stays parked until the <em>transport</em> thread's next receive
    /// reissues them; nothing else can reclaim them, so on an otherwise idle pool a send is refused
    /// <see cref="SendStatus.OutOfBuffers"/>.
    /// </summary>
    [Fact]
    public void Blocks_Of_Handled_Received_Messages_Are_Available_To_The_Send_Path_Of_A_Private_Pool()
    {
        // Eight 64-byte blocks shared by both directions.
        using SessionHarness h = Harness(o => o.AllocatorOptions = Pool(smallBlocks: 8));
        QuiclyPeer server = h.Server!;
        int received = 0;
        server.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++);

        int next = 0;
        SendAndDeliver(h, ref next, 4, 64, () => received);
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveBytesOutstanding);

        // Nothing is held on the receive side, so all eight blocks can carry sends.
        for (int i = 0; i < 8; i++)
        {
            SendResult result = server.SendCopy(new SendHeader(2), DatagramKit.Payload(1000 + i, 40));
            Assert.True(result.IsAdmitted, $"send {i} of 8: {result.Status}");
        }
    }

    /// <summary>
    /// <see cref="PeerOptions.SendBudgetBytes"/> bounds the send leases the peer holds. A datagram packed into a container
    /// is copied into the container's lease, and its own lease is no longer needed from that moment; the packer's contract
    /// (<c>PackedPayloadReturnTests</c>: "the send budget and the pool must look exactly as if each had been returned when
    /// it was copied") says so too. Since 3cd268e the member leases go back only when the container closes, and a Bulk
    /// channel rents its piece's lease <em>before</em> it closes the open container (<c>SubmitPiece</c> →
    /// <c>SubmitPending</c>). With a send budget that fits the container and the piece but not also the copied-away member
    /// leases, the pass that carries both sends no bulk piece.
    /// </summary>
    [Fact]
    public async Task A_Bulk_Piece_Fits_The_Send_Budget_Next_To_A_Container_Of_Datagrams_Sent_In_The_Same_Pass()
    {
        // Budget: one container (1 536) + one 4 KiB bulk piece + 320 bytes. Ten 60-byte datagrams hold 640 bytes until
        // they are copied into the container.
        AcceptRouter router = AcceptRouter.Pattern();
        using SessionHarness h = new(
            table: BulkTables.Main,
            client: o =>
            {
                BulkKit.Quiet(o);
                o.SendBudgetBytes = 1536 + 4096 + 320;
                o.BulkChunkBytes = 4096;
            },
            server: BulkKit.Receiver(router));
        h.Run(20_000);
        Assert.Equal(0, DatagramKit.Statistics(h.Client).SendBytesOutstanding);

        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, 64 * 1024), new PatternSource(64 * 1024));
        for (int i = 0; i < 10; i++)
        {
            Assert.True(h.Client.SendCopy(new SendHeader(2), DatagramKit.Payload(i, 60)).IsAdmitted);
        }

        long streamSends = DatagramKit.Statistics(h.Client).StreamSends;
        long containers = DatagramKit.Statistics(h.Client).ContainersSent;
        h.Client.Flush();

        // One pass: the datagrams (higher priority) went out packed, and the bulk piece went out after them.
        Assert.Equal(containers + 1, DatagramKit.Statistics(h.Client).ContainersSent);
        Assert.Equal(streamSends + 1, DatagramKit.Statistics(h.Client).StreamSends);
        Assert.False(transfer.IsFinished);
    }
}
