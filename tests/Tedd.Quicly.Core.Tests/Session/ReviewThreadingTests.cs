using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Failing tests of the performance pass review (threading / memory / performance lens, 691f20d..perf/integration). Each
/// test pins one defect with public behaviour only, so it compiles and passes on 691f20d; none of them changes production
/// code.
/// </summary>
public class ReviewThreadingTests
{
    // 2: unreliable datagrams (the parking traffic). 6: a reliable ordered stream channel with LZ4.
    private static readonly ChannelTable DecodeTable = ChannelTable.Create()
        .Add(2, "events", ChannelMode.UnreliableUnordered)
        .Add(6, "packed", ChannelMode.ReliableOrdered, o => { o.Compression = ChannelCompression.Lz4; o.MinCompressSize = 16; })
        .Build();

    /// <summary>
    /// Receive-lease recycling (45d6d1f) keeps up to 32 KiB of dispatched receive blocks rented from a peer's private pool,
    /// and only the transport thread gives them back — when one of its own rents would fail. The game thread's send rents
    /// come from the same pool (a client, or any peer without <see cref="PeerOptions.Allocator"/>), so blocks the receive
    /// side no longer uses make sends fail with <see cref="SendStatus.OutOfBuffers"/> that fit the pool and the send budget.
    /// Default options: the compact pool has 1 024 blocks of 256 bytes and the send budget is 256 KiB (exactly those
    /// 1 024 blocks). 150 received 200-byte messages dispatched in one Poll park 128 of them (32 KiB); only a receive rent
    /// that would otherwise fail gives them back, so while the peer receives nothing (or only other size classes) they stay
    /// out of the pool. On 691f20d all 1 000 sends below are admitted; on the integration head only 896 are (1 024 − 128).
    /// With a smaller private pool (<see cref="PeerOptions.AllocatorOptions"/>) the parked blocks can be a whole class, and
    /// a send of that class then fails until the peer happens to receive a message it cannot place.
    /// </summary>
    [Fact]
    public void Receive_Blocks_Parked_For_Reuse_Do_Not_Make_Sends_From_The_Same_Private_Pool_Fail()
    {
        using SessionHarness h = new(table: DatagramTables.Main, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        QuiclyPeer server = h.Server!;
        int received = 0;
        server.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++);
        h.Run(10_000);

        // The burst arrives while the server does not poll (its receive callbacks hold 150 blocks), then one Poll dispatches it.
        h.StopPumpingServer();
        for (int i = 0; i < 150; i++)
        {
            Assert.True(h.Client.SendCopy(new SendHeader(2), DatagramKit.Payload(i, 200)).IsAdmitted);
        }

        Assert.True(h.RunUntil(() => DatagramKit.ChannelStats(server, 2).Received >= 150, 1_000_000), "the burst did not arrive");
        server.Poll();
        Assert.Equal(150, received);
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveBytesOutstanding);

        // Now the server sends a burst of its own: 1 000 × 256-byte blocks fit the 1 024-block class and the 256 KiB budget.
        byte[] payload = DatagramKit.Payload(7, 200);
        int admitted = 0;
        SendStatus firstRefusal = SendStatus.Admitted;
        for (int i = 0; i < 1_000; i++)
        {
            SendResult result = server.SendCopy(new SendHeader(2), payload);
            if (result.IsAdmitted)
            {
                admitted++;
            }
            else if (firstRefusal == SendStatus.Admitted)
            {
                firstRefusal = result.Status;
            }
        }

        Assert.Equal(SendStatus.Admitted, firstRefusal);
        Assert.Equal(1_000, admitted);
    }

    /// <summary>
    /// The game thread decodes an LZ4 message into a receive lease rented with
    /// <c>PeerCore.TryRentReceiveFromPool</c>, which counts parked leases as held and never drains them (only the
    /// transport thread may: it is the recycle ring's consumer). So a decode that fits the receive budget without the parked
    /// bytes fails with them, and the message is dropped (<see cref="PeerStatistics.DecodeFailures"/>) — here a message of a
    /// reliable ordered channel that the transport already delivered, i.e. silent loss of reliable data. This contradicts
    /// the stated rule "parking never makes a rent fail that would have succeeded without it".
    /// Budget 8 KiB (recycle limit 1 KiB, blocks of at most 256 bytes): the application retains 4 × 1 536 bytes, 16 dispatched
    /// 40-byte messages park 16 × 64 bytes, and a 1 000-byte compressible message on channel 6 arrives in one of the parked
    /// blocks. Its decode needs a 1 536-byte block: 6 144 + 64 + 1 536 = 7 744 fits (691f20d); 6 144 + 1 024 + 1 536 = 8 704
    /// does not (integration head).
    /// </summary>
    [Fact]
    public void Parked_Receive_Bytes_Do_Not_Make_The_Decoder_Drop_A_Reliable_Message_That_Fits_The_Budget()
    {
        using SessionHarness h = new(table: DecodeTable, client: DatagramKit.Quiet, server: o =>
        {
            DatagramKit.Quiet(o);
            o.ReceiveBudgetBytes = 8 * 1024;
        });
        QuiclyPeer server = h.Server!;
        List<ReceiveLease> retained = [];
        int small = 0;
        int decoded = 0;
        byte[] packed = new byte[1_000];
        BitConverter.TryWriteBytes(packed, 0x5EED);
        server.RegisterHandler(2, (QuiclyPeer peer, in ReceiveHeader header, ReadOnlySpan<byte> payload) =>
        {
            if (payload.Length == 1_000)
            {
                retained.Add(peer.Retain(in header));
            }
            else
            {
                small++;
            }
        });
        server.RegisterHandler(6, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            Assert.True(payload.SequenceEqual(packed));
            decoded++;
        });
        h.Run(10_000);

        // Four 1 000-byte messages the application keeps (4 × 1 536 bytes of the budget).
        for (int i = 0; i < 4; i++)
        {
            Assert.True(h.Client.SendCopy(new SendHeader(2), DatagramKit.Payload(i, 1_000)).IsAdmitted);
        }

        Assert.True(h.RunUntil(() => retained.Count == 4, 1_000_000), "the retained messages did not arrive");

        // Sixteen small messages arrive before the server polls, then one Poll dispatches them.
        h.StopPumpingServer();
        for (int i = 0; i < 16; i++)
        {
            Assert.True(h.Client.SendCopy(new SendHeader(2), DatagramKit.Payload(100 + i, 40)).IsAdmitted);
        }

        Assert.True(h.RunUntil(() => DatagramKit.ChannelStats(server, 2).Received >= 20, 1_000_000), "the small messages did not arrive");
        server.Poll();
        Assert.Equal(16, small);
        Assert.Equal(4 * 1536, DatagramKit.Statistics(server).ReceiveBytesOutstanding);

        // A compressible 1 000-byte message on the reliable channel: tiny on the wire, 1 000 bytes once decoded.
        Assert.True(h.Client.SendCopy(new SendHeader(6), packed).IsAdmitted);
        Assert.True(h.RunUntil(() => DatagramKit.ChannelStats(server, 6).Received >= 1, 1_000_000), "the packed message did not arrive");
        server.Poll();

        Assert.Equal(0, DatagramKit.Statistics(server).DecodeFailures);
        Assert.Equal(1, decoded);

        foreach (ReceiveLease lease in retained)
        {
            server.Release(in lease);
        }
    }
}
