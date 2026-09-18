using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Failing tests of the performance pass review (threading / memory / performance lens, 691f20d..perf/integration). Each
/// test pins one defect with public behaviour only, so it compiles and passes on 691f20d; none of them changes production
/// code.
/// </summary>
public class ReviewThreadingTests
{
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
}
