using System.Buffers.Binary;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Tests.Session;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Tests.Review;

/// <summary>
/// Adversarial review of the stack on local main at d567ba5, group-limits lens (third round of the group-stream fix,
/// 0ee6b8e). Covers what the earlier rounds did not: the pended-stream ring (<c>PeerCore.NotePendedStream</c>) is sized
/// <c>PeerStreamCapacity + 2</c> on the premise that "a stream can pend only once until resumed", which does not hold for
/// streams that end while they are held.
/// </summary>
public class ReviewStackgrouplimitsTests
{
    private const ushort Groups = 11;

    private static byte[] Payload(int index)
    {
        byte[] payload = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(payload, index);
        return payload;
    }

    private static void AdvanceNetworkOnly(ServerHarness h, long micros)
    {
        long end = h.Network.NowMicros + micros;
        while (h.Network.NowMicros < end)
        {
            h.Network.AdvanceTo(Math.Min(end, h.Network.NowMicros + 1_000));
        }
    }

    /// <summary>
    /// A peer resets group streams the server holds back for a full receive ring and opens new ones, all between two Polls.
    /// Every held stream leaves an entry in <c>PeerCore.PendedStreams</c>, but the transport gives the stream back to the
    /// peer at once, so the peer can make more entries than it has streams. When the ring is full, the next stream that is
    /// held — a live one — is not remembered (<c>CallbackFaults++</c>) and nothing ever resumes it: its message never
    /// arrives, and it holds a stream slot and a group record for the life of the connection. The credit path
    /// (<c>ReceiveCredit</c>, ListGrowth) handles exactly this case by growing its list and then closing the connection
    /// with LimitExceeded; this path has neither. Either outcome here is acceptable — the live message arrives, or the
    /// connection is closed — a stream silently stalled for good is not.
    /// </summary>
    [Fact]
    public void A_Live_Stream_Held_After_The_Peer_Churned_Held_Streams_Is_Resumed_Or_The_Connection_Closed()
    {
        using ServerHarness h = new(table: TestTables.Plumbing, server: o =>
        {
            GroupKit.Prompt(o);
            o.ReceiveRingCapacity = 16;
        });
        List<int> received = [];
        h.Server!.RegisterHandler(Groups, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
            received.Add(BinaryPrimitives.ReadInt32LittleEndian(payload)));
        Assert.True(h.Admit());
        QuiclyPeer server = h.Server!;
        Assert.True(h.RunUntil(() => server.State == PeerState.Connected));

        // Fill the server's receive ring while its game thread is late: 16 messages on one group.
        byte[][] fill = new byte[16][];
        for (int i = 0; i < fill.Length; i++)
        {
            fill[i] = Payload(i);
        }

        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStream(Groups, 1, fill), out _, fin: true));
        AdvanceNetworkOnly(h, 50_000);
        Assert.Equal(16, server.Core.ReceiveRing.Count);

        // Churn: open held streams, reset them, open new ones — the network only, the server never polls.
        int pendedCapacity = server.Core.PendedStreams.Capacity;
        int perRound = server.Core.PeerUnidirectionalStreamLimit - 1;
        ulong group = 100;
        int rounds = 0;
        while (server.Core.PendedStreams.Count < pendedCapacity && rounds++ < 100)
        {
            List<TransportStreamId> ids = [];
            while (ids.Count < perRound && h.Raw.OpenUni(GroupKit.GroupStream(Groups, group++, Payload(1_000)), out TransportStreamId id) == TransportStatus.Success)
            {
                ids.Add(id);
            }

            AdvanceNetworkOnly(h, 20_000);
            foreach (TransportStreamId id in ids)
            {
                h.Raw.Transport.AbortStream(id, 0x77, StreamAbortDirection.Send);
            }

            AdvanceNetworkOnly(h, 20_000);
        }

        server.GetStatistics(out PeerStatistics churned);
        Assert.Equal(pendedCapacity, server.Core.PendedStreams.Count);

        // A live group: its message is held back for the full ring like the others were.
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStream(Groups, group++, Payload(7_777)), out _, fin: true));
        AdvanceNetworkOnly(h, 20_000);

        // The game thread comes back and polls normally from here on.
        bool done = h.RunUntil(() => received.Contains(7_777) || server.State != PeerState.Connected, 3_000_000);
        server.GetStatistics(out PeerStatistics after);
        Assert.True(done && (received.Contains(7_777) || server.CloseReason.Code == QuiclyErrorCode.LimitExceeded),
            $"the live group's message never arrived and the connection is {server.State}: pended ring of {pendedCapacity} "
            + $"filled in {rounds} rounds of {perRound} reset streams (CallbackFaults {churned.CallbackFaults} after the churn, "
            + $"{after.CallbackFaults} now; StreamReceivePends {after.StreamReceivePends}); the server still holds "
            + $"{GroupKit.OpenPeerGroups(server, Groups)} peer group stream(s) open and received {received.Count} messages "
            + $"({received.Count(x => x < 16)} of the 16 that filled the ring)");
    }

    /// <summary>
    /// The same overflow with a host that is never late: it polls once per frame and dispatches everything. Since the
    /// catch-up change (G2L-7), a Poll resumes at most as many pended entries as the receive ring has free slots, entries of
    /// dead streams included, so a peer that fills the ring each frame and resets more held streams per frame than the ring
    /// holds makes the pended-stream ring grow by the difference every frame until it overflows. Before that change every
    /// Poll emptied the pended-stream ring, so a host that polled every frame was not reachable this way.
    /// </summary>
    [Fact]
    public void A_Host_That_Polls_Every_Frame_Still_Resumes_A_Live_Stream_Held_Behind_Churned_Streams()
    {
        using ServerHarness h = new(table: TestTables.Plumbing, server: o =>
        {
            GroupKit.Prompt(o);
            o.ReceiveRingCapacity = 8;
        });
        List<int> received = [];
        h.Server!.RegisterHandler(Groups, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
            received.Add(BinaryPrimitives.ReadInt32LittleEndian(payload)));
        Assert.True(h.Admit());
        QuiclyPeer server = h.Server!;

        int pendedCapacity = server.Core.PendedStreams.Capacity;
        int perRound = 5;
        ulong group = 100;
        int frames = 0;
        long faults = 0;
        int fillIndex = 0;
        while (frames++ < 60)
        {
            // The frame: the peer fills the ring, then resets two rounds of five held streams (more than the 8 a Poll resumes, fewer than the ring holds); then the host polls once.
            byte[][] fill = new byte[8][];
            for (int i = 0; i < fill.Length; i++)
            {
                fill[i] = Payload(fillIndex++);
            }

            Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStream(Groups, group++, fill), out _, fin: true));
            AdvanceNetworkOnly(h, 5_000);
            for (int round = 0; round < 2; round++)
            {
                List<TransportStreamId> ids = [];
                while (ids.Count < perRound && h.Raw.OpenUni(GroupKit.GroupStream(Groups, group++, Payload(1_000_000)), out TransportStreamId id) == TransportStatus.Success)
                {
                    ids.Add(id);
                }

                AdvanceNetworkOnly(h, 3_000);
                foreach (TransportStreamId id in ids)
                {
                    h.Raw.Transport.AbortStream(id, 0x77, StreamAbortDirection.Send);
                }

                AdvanceNetworkOnly(h, 3_000);
            }

            if (server.Core.PendedStreams.Count == pendedCapacity)
            {
                // A live group, held back like the others; the host polls it at the next frame as ever (RunUntil below).
                Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(GroupKit.GroupStream(Groups, group++, Payload(7_777)), out _, fin: true));
                AdvanceNetworkOnly(h, 3_000);
                break;
            }

            server.GetStatistics(out PeerStatistics statistics);
            faults = statistics.CallbackFaults;
            server.Poll();
            server.Flush();
        }

        Assert.True(frames <= 60, $"precondition: the pended-stream ring never filled (it holds {server.Core.PendedStreams.Count} of {pendedCapacity}; CallbackFaults {faults})");

        // The peer stops; the host goes on polling every frame.
        bool done = h.RunUntil(() => received.Contains(7_777) || server.State != PeerState.Connected, 3_000_000);
        server.GetStatistics(out PeerStatistics after);
        Assert.True(done && (received.Contains(7_777) || server.CloseReason.Code == QuiclyErrorCode.LimitExceeded),
            $"the live group's message never arrived and the connection is {server.State}, although the host polled every "
            + $"frame ({frames} frames): pended ring of {pendedCapacity}, CallbackFaults {after.CallbackFaults}, "
            + $"StreamReceivePends {after.StreamReceivePends}; the server still holds {GroupKit.OpenPeerGroups(server, Groups)} "
            + $"peer group stream(s) open; {received.Count(x => x < fillIndex)} of the {fillIndex} fill messages arrived");
    }

    /// <summary>
    /// The Bulk counterpart of the original group defect: a receiver that is behind rejects a stream of a sender that kept
    /// the limit. An honest sender that cancels its transfers gets its slot back on the reset stream's close — on the
    /// receiving end's transport thread, no Poll involved — and may start the next transfer at once. The receiving end,
    /// though, has <c>BulkTransfersPerDirection</c> receive records, and a record of a finished transfer returns to the free
    /// list only when its game thread has sent the final progress (BulkEngine.Receive OnStreamClosed → _retired → _recycle).
    /// So after two cancels during one receiver hitch, the third transfer — started with nothing else live — is rejected
    /// with LimitExceeded (StreamsReset++) and fails, although the sender never had more than one transfer live.
    /// </summary>
    [Fact]
    public async Task A_Transfer_Started_After_Two_Cancels_During_A_Receiver_Hitch_Completes()
    {
        const ushort World = 5;
        AcceptRouter router = AcceptRouter.Pattern();
        using SessionHarness h = new(table: BulkTables.Main, client: BulkKit.Quiet, server: BulkKit.Receiver(router));
        QuiclyPeer server = h.Server!;

        // Only the client and the network run: the server's game thread is late.
        bool ClientOnly(Func<bool> condition, long maxMicros = 5_000_000)
        {
            long end = h.Network.NowMicros + maxMicros;
            while (!condition())
            {
                if (h.Network.NowMicros >= end)
                {
                    return false;
                }

                h.Network.AdvanceTo(h.Network.NowMicros + 1_000);
                h.Client.Poll();
                h.Client.Flush();
            }

            return true;
        }

        for (int i = 0; i < 2; i++)
        {
            int accepted = router.Accepted.Count;
            BulkTransfer canceled = await h.Client.BeginBulkSendAsync(new BulkDescriptor(World, (ulong)(10 + i), 1, 4 * 1024 * 1024), new PatternSource(4 * 1024 * 1024));
            Assert.True(ClientOnly(() => router.Accepted.Count > accepted), $"transfer {i} never reached the receiver's router");
            canceled.Cancel();
            Assert.True(ClientOnly(() => canceled.IsFinished && BulkKit.SendTransfers(h.Client, World) == 0 && BulkKit.SendStreams(h.Client, World) == 0),
                $"cancel {i} did not give the sender its slot back without the receiver's game thread");
        }

        server.GetStatistics(out PeerStatistics before);
        BulkTransfer third = await h.Client.BeginBulkSendAsync(new BulkDescriptor(World, 20, 1, 64 * 1024), new PatternSource(64 * 1024));
        ClientOnly(() => third.IsFinished, 200_000);

        // The server's game thread is back.
        Assert.True(h.RunUntil(() => third.IsFinished, 10_000_000), "the third transfer never finished");
        server.GetStatistics(out PeerStatistics after);
        Assert.True(third.Status == BulkStatus.Completed,
            $"a transfer started with nothing else live ended {third.Status} ({third.Result.Code}); the receiver reset "
            + $"{after.StreamsReset - before.StreamsReset} stream(s) during its hitch and its router accepted {router.Accepted.Count} transfer(s)");
    }
}
