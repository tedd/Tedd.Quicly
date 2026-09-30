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
}
