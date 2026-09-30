using System.Buffers.Binary;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Tests.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Review;

/// <summary>
/// Adversarial review of the stack on d567ba5, lens: the receive credit (9ac4659, d0cd095, e42d7bd). A test marked FINDING
/// fails on d567ba5 for the reason its comment gives; a test marked GUARD passes and pins a promise.
/// </summary>
public class ReviewStackcreditTests
{
    private const int Ring = 64;

    private const ushort Datagrams = 3;
    private const ushort Ordered = 10;
    private const ushort Groups = 11;
    private const ushort Read = 12;

    /// <summary>Two reliable channels the application drains and then abandons (10, 11), and two it reads with handlers (3, 12).</summary>
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "a", ChannelMode.UnreliableUnordered)
        .Add(Datagrams, "b", ChannelMode.UnreliableUnordered)
        .Add(Ordered, "stream", ChannelMode.ReliableOrdered)
        .Add(Groups, "group", ChannelMode.ReliableUnordered)
        .Add(Read, "read", ChannelMode.ReliableOrdered)
        .Add(Packed, "packed", ChannelMode.ReliableOrdered, o =>
        {
            o.Compression = ChannelCompression.Lz4;
            o.MinCompressSize = 1;
        })
        .Build();

    private const ushort Packed = 13;

    private static byte[] Payload(int index, int size = 4)
    {
        byte[] payload = new byte[size];
        BinaryPrimitives.WriteInt32LittleEndian(payload, index);
        return payload;
    }

    private static int IndexOf(ReadOnlySpan<byte> payload) => BinaryPrimitives.ReadInt32LittleEndian(payload);

    private static MessageHandler Collect(List<int> into) =>
        (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => into.Add(IndexOf(payload));

    private static int WaitingBytes(QuiclyPeer peer, ushort channel) => peer.Core.Credit.WaitingBytes(peer.Core.ChannelIndexOf(channel));

    private static CreditState State(QuiclyPeer peer, ushort channel) => peer.Core.Credit.State(peer.Core.ChannelIndexOf(channel));

    private static int DrainAll(QuiclyPeer peer, ushort channel)
    {
        ReceivedMessage[] buffer = new ReceivedMessage[16];
        int total = 0;
        int taken;
        while ((taken = peer.Drain(channel, buffer)) > 0)
        {
            peer.Release(buffer.AsSpan(0, taken));
            total += taken;
        }

        return total;
    }

    private static void PumpSenderOnly(SessionHarness h, QuiclyPeer sender, int steps)
    {
        for (int step = 0; step < steps; step++)
        {
            sender.Poll();
            sender.Flush();
            h.Network.Advance(1_000);
        }
    }

    private sealed class CountingWorkSignal : IPeerWorkSignal
    {
        public int Calls;

        public void OnWork(QuiclyPeer peer) => Calls++;
    }

    /// <summary>
    /// FINDING (release notes "Fixed": "past that only its own streams are held back ... Every other channel keeps working,
    /// nothing is lost"; TROUBLESHOOTING.md, the <c>OutOfBuffers</c> row: "it has one cause: Poll is not called often enough
    /// ... A channel nobody reads does not cause it"; <c>CreditState.Drained</c>: "half the receive budget, because a channel
    /// keeps what it accepted when the application stops draining it, and the other half is then what is left for the
    /// channels that are read").
    /// <para>
    /// The half is per channel, not shared. A host drains two reliable channels (a chat window and a lobby list, say) and
    /// stops draining both — a scene change. Each channel was <see cref="CreditState.Drained"/> and accepted half of the
    /// receive budget; the pass start demotes both to <see cref="CreditState.Unread"/>, and "a narrower limit takes nothing
    /// back". The two now pin the whole receive budget for as long as the application does not come back, and every channel
    /// that <em>is</em> read — here a datagram channel and a ReliableOrdered channel, both with handlers — gets nothing:
    /// its datagrams are dropped on arrival (<c>OutOfReceiveBuffers</c>) and its stream is held for the budget, which is
    /// the stall of every channel that the credit was built to remove. The release notes' "Known limits" admit "two such
    /// channels can hold all of it"; the "Fixed" entry, TROUBLESHOOTING.md and the XML docs of <c>CreditState.Drained</c> say
    /// the opposite. A cheap fix: count the Drained threshold against one half shared by all Drained channels (their sum),
    /// not against a half per channel.
    /// </para>
    /// </summary>
    [Fact]
    public void Two_Channels_The_Application_Stops_Draining_Leave_The_Receive_Budget_To_The_Channels_That_Are_Read()
    {
        CountingWorkSignal signal = new();
        using SessionHarness h = new(
            table: Table,
            client: o =>
            {
                GroupKit.Prompt(o);
                OrderedKit.Roomy(o);
            },
            server: o =>
            {
                GroupKit.Prompt(o);
                o.ReceiveRingCapacity = Ring;
                o.WorkSignal = signal;
            });
        QuiclyPeer server = h.Server!;
        List<int> datagrams = [];
        List<int> read = [];
        server.RegisterHandler(Datagrams, Collect(datagrams));
        server.RegisterHandler(Read, Collect(read));

        // A consumer that drains both channels every frame: nothing to take yet.
        Assert.Equal(0, DrainAll(server, Ordered));
        Assert.Equal(0, DrainAll(server, Groups));
        Assert.Equal(CreditState.Drained, State(server, Ordered));
        Assert.Equal(CreditState.Drained, State(server, Groups));

        // One burst on each within the ring, while the host is between frames; then it only polls.
        for (int i = 0; i < Ring; i++)
        {
            Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Ordered), Payload(i, 4_000)).Status);
        }

        PumpSenderOnly(h, h.Client, 40);
        for (int i = 0; i < Ring; i++)
        {
            Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Groups), Payload(i, 4_000)).Status);
        }

        PumpSenderOnly(h, h.Client, 40);
        server.Poll();
        server.Poll();
        server.Poll();
        Assert.Equal(CreditState.Unread, State(server, Ordered));
        Assert.Equal(CreditState.Unread, State(server, Groups));
        long pinned = WaitingBytes(server, Ordered) + (long)WaitingBytes(server, Groups);

        // The traffic that is read.
        for (int i = 0; i < 20; i++)
        {
            Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Datagrams), Payload(i)).Status);
            Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Read), Payload(i)).Status);
        }

        bool arrived = h.RunUntil(() => datagrams.Count == 20 && read.Count == 20, 2_000_000);
        long pends = DatagramKit.Statistics(server).StreamReceivePends;
        int signals = signal.Calls;
        h.Run(100_000);
        PeerStatistics statistics = DatagramKit.Statistics(server);
        Assert.True(arrived && statistics.OutOfReceiveBuffers == 0,
            $"two channels the application stopped draining are Unread and pin {pinned} of {h.ServerOptions.ReceiveBudgetBytes} receive-budget bytes "
            + $"(each was allowed half, {server.Core.Credit.DrainedByteLimit}). The channels that are read, both with handlers: {datagrams.Count} of 20 "
            + $"datagrams and {read.Count} of 20 ReliableOrdered messages arrived in two seconds; {statistics.OutOfReceiveBuffers} messages were dropped "
            + $"for want of a receive buffer; in the 100 ms after that a stream was held back {statistics.StreamReceivePends - pends} more times and the "
            + $"host was signalled {signal.Calls - signals} more times (HasPendingWork {server.HasPendingWork}).");
    }

    /// <summary>
    /// FINDING, the same half-per-channel with a reliable consequence (release notes "Fixed": "Every other channel keeps
    /// working, nothing is lost"). Two channels the application drained and then abandoned pin all of the receive budget
    /// but one 4 KiB block. A <em>handled</em> ReliableOrdered channel that compresses gets its next message in (its
    /// compressed block fits), and its handler never sees it: the decode needs a second block of the raw size and there is
    /// none, so the message is dropped (<c>DecodeFailures</c>) after the transport acknowledged it — the sender reports it
    /// <c>Delivered</c>. The Known-limits entry for compressed handler messages describes the mechanism; what makes it reach
    /// a channel that is read is that each abandoned channel keeps up to half the budget, and two of them keep all of it.
    /// </summary>
    [Fact]
    public void Channels_The_Application_Stops_Draining_Do_Not_Make_A_Handled_Compressed_Channel_Lose_Messages()
    {
        using SessionHarness h = new(
            table: Table,
            client: o =>
            {
                GroupKit.Prompt(o);
                OrderedKit.Roomy(o);
            },
            server: o =>
            {
                GroupKit.Prompt(o);
                o.ReceiveRingCapacity = Ring;
            });
        QuiclyPeer server = h.Server!;
        List<int> packed = [];
        server.RegisterHandler(Packed, Collect(packed));

        Assert.Equal(0, DrainAll(server, Ordered));
        Assert.Equal(0, DrainAll(server, Groups));

        // 32 messages of 4 000 bytes reach the Drained threshold of the ordered channel (half the budget, 4 KiB blocks);
        // the group channel's sender stops one message short of it.
        for (int i = 0; i < Ring; i++)
        {
            Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Ordered), Payload(i, 4_000)).Status);
        }

        PumpSenderOnly(h, h.Client, 40);
        for (int i = 0; i < 31; i++)
        {
            Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Groups), Payload(i, 4_000)).Status);
        }

        PumpSenderOnly(h, h.Client, 40);
        server.Poll();
        server.Poll();
        server.Poll();
        Assert.Equal(CreditState.Unread, State(server, Ordered));
        Assert.Equal(CreditState.Unread, State(server, Groups));
        long pinned = DatagramKit.Statistics(server).ReceiveBytesOutstanding;

        // A compressible 3 000-byte message on the handled channel: a small block to arrive in, a 4 KiB block to decode into.
        byte[] message = new byte[3_000];
        BinaryPrimitives.WriteInt32LittleEndian(message, 7);
        Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Packed), message).Status);
        h.RunUntil(() => packed.Count == 1 || DatagramKit.Statistics(server).DecodeFailures > 0, 2_000_000);
        h.Run(100_000);
        PeerStatistics statistics = DatagramKit.Statistics(server);
        Assert.True(packed.Count == 1 && statistics.DecodeFailures == 0,
            $"two abandoned channels pin {pinned} of {h.ServerOptions.ReceiveBudgetBytes} receive-budget bytes; the handler of the compressed "
            + $"ReliableOrdered channel got {packed.Count} of 1 message, {statistics.DecodeFailures} were dropped as DecodeFailures "
            + $"(the channel's Received: {DatagramKit.ChannelStats(server, Packed).Received}).");
    }

    private static List<int> DrainIndices(QuiclyPeer peer, ushort channel)
    {
        List<int> got = [];
        ReceivedMessage[] buffer = new ReceivedMessage[16];
        int taken;
        while ((taken = peer.Drain(channel, buffer)) > 0)
        {
            for (int i = 0; i < taken; i++)
            {
                got.Add(IndexOf(buffer[i].Payload));
            }

            peer.Release(buffer.AsSpan(0, taken));
        }

        return got;
    }

    /// <summary>
    /// GUARD ("the UnregisterHandler hold is closed", design §2.3 "Remaining holds"). A handled ReliableOrdered channel
    /// whose receiver is late fills the receive ring (a handled channel has no credit limit); a Drain of another channel
    /// moves all of that into the queue pool as the handler's; the handler is removed. Repeated five times with a Poll
    /// that gives the handler back in between. The channel's messages go beyond the pool, the peer never holds one of
    /// them (the ring stays open, a datagram channel with a handler keeps receiving), and handler plus Drain see every
    /// message once, in order.
    /// </summary>
    [Fact]
    public void Removing_A_Handler_Over_A_Full_Ring_And_A_Full_Pool_Never_Closes_The_Ring()
    {
        using SessionHarness h = new(
            table: Table,
            client: o =>
            {
                GroupKit.Prompt(o);
                OrderedKit.Roomy(o);
            },
            server: o =>
            {
                GroupKit.Prompt(o);
                o.ReceiveRingCapacity = Ring;
            });
        QuiclyPeer server = h.Server!;
        List<int> handled = [];
        List<int> datagrams = [];
        server.RegisterHandler(Datagrams, Collect(datagrams));
        int sent = 0;
        int datagramsSent = 0;
        for (int cycle = 0; cycle < 5; cycle++)
        {
            server.RegisterHandler(Ordered, Collect(handled));
            server.Poll();
            for (int i = 0; i < 3 * Ring; i++)
            {
                Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Ordered), Payload(sent++, 16)).Status);
            }

            PumpSenderOnly(h, h.Client, 30);
            Assert.Equal(0, DrainAll(server, 2));
            Assert.True(server.DrainQueues.Count(server.Core.ChannelIndexOf(Ordered)) >= Ring / 2, $"cycle {cycle}: only {server.DrainQueues.Count(server.Core.ChannelIndexOf(Ordered))} of the handler's messages were queued by the Drain of another channel");
            Assert.True(server.UnregisterHandler(Ordered));
            server.Poll();
            Assert.False(server.HasPendingWork && server.Core.ReceiveRing.IsEmpty && server.DrainQueues.Count(server.Core.ChannelIndexOf(Ordered)) == 0,
                "nothing to do, and the work probe is set");

            for (int i = 0; i < 8; i++)
            {
                Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Datagrams), Payload(datagramsSent++)).Status);
            }

            Assert.True(h.RunUntil(() => datagrams.Count == datagramsSent, 1_000_000),
                $"cycle {cycle}: {datagrams.Count} of {datagramsSent} datagrams of a handled channel arrived while the ordered channel lost its handler "
                + $"over a backlog (queued {server.DrainQueues.Count(server.Core.ChannelIndexOf(Ordered))}, excess nodes used {server.DrainQueues.ExcessUsed})");
        }

        Assert.True(h.RunUntil(() =>
        {
            handled.AddRange(DrainIndices(server, Ordered));
            return handled.Count >= sent;
        }, 5_000_000), $"{handled.Count} of {sent} messages came out");
        Assert.Equal(Enumerable.Range(0, sent), handled);
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveRingDrops);
    }
}
