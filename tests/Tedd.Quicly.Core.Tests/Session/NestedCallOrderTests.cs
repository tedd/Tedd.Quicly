using System.Buffers.Binary;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// The order of a ReliableOrdered channel when the application calls into the peer from inside a handler. A Poll
/// dispatches the drain queues first and the receive ring after them, so a message can only be queued behind the Poll's
/// back by a handler of that Poll: one that drains another channel (<see cref="QuiclyPeer.Drain"/> moves the messages of
/// other channels it meets into their queues), or one that registers a handler for a channel with a backlog. The ring's
/// later messages of that channel must then wait behind the queue, not be dispatched past it.
/// </summary>
public class NestedCallOrderTests
{
    private const ushort Datagrams = 3;
    private const ushort Ordered = 10;
    private const ushort Groups = 11;

    /// <summary>2, 3 unordered datagrams · 6 unordered LZ4 · 8 keyed sequenced · 10 ordered stream · 11 group streams.</summary>
    private static readonly ChannelTable Table = TestTables.Plumbing;

    private static SessionHarness Harness() =>
        new(table: Table, client: GroupKit.Prompt, server: o =>
        {
            GroupKit.Prompt(o);
            o.ReceiveRingCapacity = 64;
        });

    private static byte[] Payload(int index)
    {
        byte[] payload = new byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(payload, index);
        return payload;
    }

    private static int IndexOf(ReadOnlySpan<byte> payload) => BinaryPrimitives.ReadInt32LittleEndian(payload);

    private static MessageHandler Collect(List<int> into) =>
        (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => into.Add(IndexOf(payload));

    private static long Received(QuiclyPeer peer, ushort channel) => DatagramKit.ChannelStats(peer, channel).Received;

    /// <summary>Sends numbered messages while only the sender and the network run: the receiver is late with its Poll.</summary>
    private static void SendToALateReceiver(SessionHarness h, ushort channel, int first, int count)
    {
        for (int i = 0; i < count; i++)
        {
            Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(channel), Payload(first + i)).Status);
        }

        for (int step = 0; step < 8; step++)
        {
            h.Client.Poll();
            h.Client.Flush();
            h.Network.Advance(1_000);
        }
    }

    [Fact]
    public void A_Drain_Of_Another_Channel_From_Inside_A_Handler_Does_Not_Reorder_The_Handlers_Channel()
    {
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        List<int> got = [];
        ReceivedMessage[] one = new ReceivedMessage[1];
        server.RegisterHandler(Ordered, (QuiclyPeer self, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            got.Add(IndexOf(payload));
            if (got.Count == 1)
            {
                // The application reads the datagram channel with Drain, one message at a time. That Drain meets
                // ordered 1 in the ring, queues it for this handler, and stops at the datagram.
                int taken = self.Drain(Datagrams, one);
                self.Release(one.AsSpan(0, taken));
            }
        });

        // The ring, in arrival order: ordered 0, ordered 1, a datagram, ordered 2.
        SendToALateReceiver(h, Ordered, 0, 2);
        SendToALateReceiver(h, Datagrams, 0, 1);
        SendToALateReceiver(h, Ordered, 2, 1);
        Assert.Equal(3, Received(server, Ordered));

        h.Run(10_000);
        Assert.True(got.SequenceEqual(Enumerable.Range(0, 3)),
            $"the handler of a ReliableOrdered channel got its messages out of order: {string.Join(",", got)}");
    }

    [Fact]
    public void A_Drain_Of_A_Group_Channel_From_Inside_A_Handler_Does_Not_Reorder_The_Handlers_Channel()
    {
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        List<int> ordered = [];
        List<int> groups = [];
        ReceivedMessage[] one = new ReceivedMessage[1];
        server.RegisterHandler(Ordered, (QuiclyPeer self, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            ordered.Add(IndexOf(payload));
            if (ordered.Count == 1)
            {
                int taken = self.Drain(Groups, one);
                for (int i = 0; i < taken; i++)
                {
                    groups.Add(IndexOf(one[i].Payload));
                }

                self.Release(one.AsSpan(0, taken));
            }
        });

        // The ring, in arrival order: ordered 0, ordered 1, group 100, ordered 2.
        SendToALateReceiver(h, Ordered, 0, 2);
        SendToALateReceiver(h, Groups, 100, 1);
        SendToALateReceiver(h, Ordered, 2, 1);
        Assert.Equal(3, Received(server, Ordered));
        Assert.Equal(1, Received(server, Groups));

        h.Run(10_000);
        Assert.Equal([100], groups);
        Assert.True(ordered.SequenceEqual(Enumerable.Range(0, 3)),
            $"the handler of a ReliableOrdered channel got its messages out of order: {string.Join(",", ordered)}");
    }

    [Fact]
    public void A_Handler_Registered_From_Inside_Another_Handler_Gets_The_Channels_Backlog_First()
    {
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        List<int> got = [];
        server.RegisterHandler(Datagrams, (QuiclyPeer self, in ReceiveHeader _, ReadOnlySpan<byte> _) =>
            self.RegisterHandler(Ordered, Collect(got)));

        // Three messages of the ordered channel, which has no handler yet, wait in its drain queue.
        for (int i = 0; i < 3; i++)
        {
            Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(Ordered), Payload(i)).Status);
        }

        h.Run(4_000);
        Assert.Equal(3, Received(server, Ordered));

        // The host is late. First the datagram whose handler registers the ordered channel's handler, then three more
        // ordered messages: the ring holds them in that order, and the Poll has dispatched its queues before it gets there.
        SendToALateReceiver(h, Datagrams, 0, 1);
        SendToALateReceiver(h, Ordered, 3, 3);
        Assert.Equal(6, Received(server, Ordered));

        h.Run(10_000);
        Assert.True(got.SequenceEqual(Enumerable.Range(0, 6)),
            $"the handler of a ReliableOrdered channel got its messages out of order: {string.Join(",", got)}");
    }

    [Fact]
    public void A_Channel_That_Queues_Behind_Its_Backlog_Is_Served_Before_The_Ring_Whatever_MaxItems_Is()
    {
        // The message that goes behind a queued one waits for the next Poll. It cannot starve there: a Poll dispatches the
        // queues of channels with a handler before it takes anything from the ring, and it takes nothing from the ring
        // while maxItems is used up. So with a Poll(1) host every call delivers the channel's oldest message.
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        List<int> got = [];
        ReceivedMessage[] one = new ReceivedMessage[1];
        server.RegisterHandler(Ordered, (QuiclyPeer self, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            got.Add(IndexOf(payload));
            if (got.Count == 1)
            {
                int taken = self.Drain(Datagrams, one);
                self.Release(one.AsSpan(0, taken));
            }
        });

        // The ring: ordered 0 .. 4, a datagram, ordered 5 .. 9. The Drain inside the first handler call queues 1 .. 4.
        SendToALateReceiver(h, Ordered, 0, 5);
        SendToALateReceiver(h, Datagrams, 0, 1);
        SendToALateReceiver(h, Ordered, 5, 5);
        Assert.Equal(10, Received(server, Ordered));

        for (int poll = 1; poll <= 10; poll++)
        {
            Assert.Equal(1, server.Poll(1));
            Assert.Equal(Enumerable.Range(0, poll), got);
        }

        Assert.Equal(0, server.Poll(1));
    }
}
