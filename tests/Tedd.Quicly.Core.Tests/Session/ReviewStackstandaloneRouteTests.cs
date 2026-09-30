using System.Buffers.Binary;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Tests.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Adversarial review (lens: standalone) of 6e173a0: Route puts a ring message of a channel with a handler behind the
/// messages its queue already has. GUARD tests: nested calls from handlers, a small maxItems and a small queue pool
/// together, and the handler and Drain styles mixed on one channel, must keep a ReliableOrdered channel complete and in
/// order and let the host come to rest.
/// </summary>
/// <remarks>
/// Guards; the finding of the same review about how the order is argued (S5) was fixed in the documents: a channel's queue is dispatched before any ring message of that channel.
/// </remarks>
public class ReviewStackstandaloneRouteTests
{
    private const ushort Datagrams = 3;
    private const ushort Ordered = 10;

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

    private static void SendToALateReceiver(SessionHarness h, ushort channel, int first, int count)
    {
        for (int i = 0; i < count; i++)
        {
            Assert.Equal(SendStatus.Admitted, h.Client.SendCopy(new SendHeader(channel), Payload(first + i)).Status);
        }

        for (int step = 0; step < 4; step++)
        {
            h.Client.Poll();
            h.Client.Flush();
            h.Network.Advance(1_000);
        }
    }

    private static void PollServerAtLeisure(SessionHarness h, int maxItems, int polls)
    {
        for (int i = 0; i < polls; i++)
        {
            h.Server!.Poll(maxItems);
            h.Server.Flush();
            h.Client.Poll();
            h.Client.Flush();
            h.Network.Advance(500);
        }
    }

    /// <summary>
    /// GUARD. The ordered channel's handler drains the datagram channel one message at a time on every call (so every
    /// dispatch can move the ordered channel's ring messages into its queue), the pool is 64 nodes, and the host polls
    /// with maxItems 3 while bursts of 40 ordered messages and datagrams arrive between its Polls. Every ordered message
    /// arrives, in order, and the host comes to rest.
    /// </summary>
    [Fact]
    public void GUARD_A_Handler_That_Drains_Another_Channel_On_Every_Call_Keeps_Its_Channel_Complete_And_Ordered_Under_Poll3()
    {
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        List<int> got = [];
        ReceivedMessage[] one = new ReceivedMessage[1];
        server.RegisterHandler(Ordered, (QuiclyPeer self, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            got.Add(IndexOf(payload));
            int taken = self.Drain(Datagrams, one);
            self.Release(one.AsSpan(0, taken));
        });

        const int Bursts = 10;
        const int PerBurst = 40;
        for (int burst = 0; burst < Bursts; burst++)
        {
            SendToALateReceiver(h, Ordered, burst * PerBurst, PerBurst / 2);
            SendToALateReceiver(h, Datagrams, 0, 3);
            SendToALateReceiver(h, Ordered, (burst * PerBurst) + (PerBurst / 2), PerBurst / 2);
            PollServerAtLeisure(h, 3, 5);
        }

        Assert.True(h.RunUntil(() => got.Count >= Bursts * PerBurst, 5_000_000), $"{got.Count} of {Bursts * PerBurst} arrived");
        Assert.True(got.SequenceEqual(Enumerable.Range(0, Bursts * PerBurst)),
            $"out of order or duplicated: {string.Join(",", got.Take(60))}...");
        h.Run(50_000);
        Assert.False(server.HasPendingWork, "the host cannot come to rest");
    }

    /// <summary>
    /// GUARD. The handler and Drain styles mixed on one ordered channel: the handler removes itself every fifth message,
    /// the host drains the channel after every Poll and registers the handler again, and the handler also drains its
    /// own channel two at a time on every other call. One list, in order, complete.
    /// </summary>
    [Fact]
    public void GUARD_Handler_And_Drain_Mixed_On_One_Ordered_Channel_With_Nested_Unregister_Keep_Order()
    {
        using SessionHarness h = Harness();
        QuiclyPeer server = h.Server!;
        List<int> got = [];
        ReceivedMessage[] two = new ReceivedMessage[2];
        ReceivedMessage[] batch = new ReceivedMessage[5];
        int calls = 0;
        MessageHandler handler = null!;
        handler = (QuiclyPeer self, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            got.Add(IndexOf(payload));
            calls++;
            if (calls % 2 == 0)
            {
                int taken = self.Drain(Ordered, two);
                for (int i = 0; i < taken; i++)
                {
                    got.Add(IndexOf(two[i].Payload));
                }

                self.Release(two.AsSpan(0, taken));
            }

            if (calls % 5 == 0)
            {
                self.UnregisterHandler(Ordered);
            }
        };
        server.RegisterHandler(Ordered, handler);

        const int Total = 300;
        for (int first = 0; first < Total; first += 30)
        {
            SendToALateReceiver(h, Ordered, first, 30);
            SendToALateReceiver(h, Datagrams, 0, 2);
            for (int step = 0; step < 6; step++)
            {
                server.Poll(4);
                int taken;
                while ((taken = server.Drain(Ordered, batch)) > 0)
                {
                    for (int i = 0; i < taken; i++)
                    {
                        got.Add(IndexOf(batch[i].Payload));
                    }

                    server.Release(batch.AsSpan(0, taken));
                    if (got.Count % 3 == 0)
                    {
                        break; // the host does not always drain to 0 before it registers the handler again
                    }
                }

                if (!server.UnregisterHandler(Ordered))
                {
                    // It had none: give it back.
                }

                server.RegisterHandler(Ordered, handler);
                server.Flush();
                h.Client.Poll();
                h.Client.Flush();
                h.Network.Advance(500);
            }
        }

        Assert.True(h.RunUntil(() => got.Count >= Total, 5_000_000), $"{got.Count} of {Total} arrived");
        Assert.True(got.SequenceEqual(Enumerable.Range(0, Total)), $"out of order or duplicated: {string.Join(",", got.Take(80))}...");
        Assert.Equal(Total, DatagramKit.ChannelStats(server, Ordered).Received);
    }
}
