using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Tests.Session;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Review;

/// <summary>
/// Adversarial review of fix/group-stream-receive-limit, resources lens (ADR 0008): the receive records of the
/// ReliableUnordered engine are now sized from <c>PeerCore.PeerUnidirectionalStreamLimit</c>, on the claim that the
/// transport never lets the peer have more streams open than that.
/// </summary>
public class ReviewGroupResourcesTests
{
    /// <summary>
    /// The fix rests on "the transport admits at most PeerUnidirectionalStreamLimit peer streams". That is only true where the
    /// transport's credit starts at 0 and is raised to the limit after admission (an MsQuic <em>server</em>, and the
    /// simulator's default link). An MsQuic <em>client</em> grants the server 1 024 unidirectional streams in its transport
    /// parameters (<c>MsQuicTransportOptions.ClientPeerUnidiStreamCount</c>, default 1 024), and QUIC never takes credit back,
    /// so <c>UpdatePeerStreamLimits(0, limit)</c> after the HelloAck does not lower it (MsQuicTransport.UpdatePeerStreamLimits
    /// remarks; the simulator models exactly this with <see cref="LinkOptions.PeerUnidiStreams"/>). A client that falls behind
    /// therefore sees more group streams than it has receive records, and the backstop resets them — after the server
    /// completed their messages Delivered. This is the original defect, server → client, the usual direction of a game.
    /// </summary>
    [Fact]
    public void A_Client_That_Granted_The_MsQuic_Default_Stream_Credit_Loses_No_Group_When_It_Falls_Behind()
    {
        using SessionHarness h = new(
            link: new LinkOptions { PeerUnidiStreams = 1024 },
            table: TestTables.Plumbing,
            server: GroupKit.Prompt,
            client: o =>
            {
                DatagramKit.Quiet(o);
                o.ReceiveRingCapacity = 64;
            });
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        List<int> got = [];
        client.RegisterHandler(11, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => got.Add(BitConverter.ToInt32(payload)));

        // The same hitch as GroupStreamTests.A_Receiver_That_Falls_Behind_Loses_No_Group, the other way round: the client is
        // not polled for 80 ms while the server sends twenty groups of sixteen messages.
        List<SendToken> tokens = SendGroupsToALateReceiver(h, server, 11, 320);
        int delivered = tokens.Count(token => server.GetDeliveryStatus(token) == DeliveryStatus.Delivered);
        h.RunUntil(() => got.Count == 320, 2_000_000);
        PeerStatistics statistics = DatagramKit.Statistics(client);
        Assert.True(got.Count == 320 && statistics.StreamsReset == 0,
            $"received {got.Count} of 320 messages; the client reset {statistics.StreamsReset} streams (record limit "
            + $"{client.Core.PeerUnidirectionalStreamLimit}); the sender reported {delivered} Delivered while the client was behind and "
            + $"{tokens.Count(token => server.GetDeliveryStatus(token) == DeliveryStatus.Delivered)} at the end, "
            + $"{tokens.Count(token => server.GetDeliveryStatus(token) == DeliveryStatus.Failed)} Failed");
        Assert.Equal(Enumerable.Range(0, 320), got.Order());
    }

    /// <summary>
    /// Stress: three group channels at once over a link with loss, jitter and reordering, the receiver polled irregularly
    /// (so it holds far more than MaxGroups streams of a channel). Every tracked message reported Delivered must arrive
    /// exactly once, in admission order inside its group, nothing may be reset, and every record, lease and stream comes back.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(7)]
    [InlineData(23)]
    [InlineData(101)]
    public void Irregularly_Polled_Receiver_Gets_Every_Delivered_Message_Exactly_Once_And_Nothing_Leaks(int seed)
    {
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 4_000, JitterMicros = 3_000, LossPercent = 3, StreamLossPercent = 3, ReorderPercent = 5 },
            table: GroupTables.Main,
            seed: seed,
            client: o =>
            {
                GroupKit.Prompt(o);
                GroupKit.Roomy(o);
            },
            server: o =>
            {
                DatagramKit.Quiet(o);
                GroupKit.Roomy(o);
                o.ReceiveRingCapacity = 32;
            });
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        int baselineStreams = server.Core.Streams.Count;
        int baselineRented = Rented(server) + Rented(client);
        ushort[] channels = [5, 9, 11];
        Dictionary<int, int> arrivals = [];
        List<int> order = [];
        MessageHandler handler = (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            int id = BitConverter.ToInt32(payload);
            arrivals[id] = arrivals.GetValueOrDefault(id) + 1;
            order.Add(id);
        };
        foreach (ushort channel in channels)
        {
            server.RegisterHandler(channel, handler);
        }

        Random random = new(seed);
        List<(int Id, SendToken Token, int Batch)> sent = [];
        List<(int Id, SendToken Token)> pending = [];
        Dictionary<int, DeliveryStatus> status = [];
        void Settle()
        {
            // A finished token keeps its status only until its slot is used again, so it is read right after each Poll.
            pending.RemoveAll(m =>
            {
                DeliveryStatus s = client.GetDeliveryStatus(m.Token);
                if (s == DeliveryStatus.Pending)
                {
                    return false;
                }

                status[m.Id] = s;
                return true;
            });
        }

        int next = 0;
        int batch = 0;
        int maxOpen = 0;
        for (int round = 0; round < 400; round++)
        {
            foreach (ushort channel in channels)
            {
                int count = random.Next(1, 9);
                for (int i = 0; i < count; i++)
                {
                    int id = next++;
                    SendResult result = client.SendCopy(new SendHeader(channel), DatagramKit.Payload(id, 4 + random.Next(0, 200)), SendOptions.Tracked);
                    if (result.IsAdmitted)
                    {
                        sent.Add((id, result.Token, batch));
                        pending.Add((id, result.Token));
                    }
                }

                batch++;
            }

            client.Poll();
            Settle();
            client.Flush();
            h.Network.Advance(1_000);

            // The receiver is late: one Poll in about twelve steps, and then only a few messages.
            if (random.Next(12) == 0)
            {
                server.Poll(random.Next(1, 40));
                server.Flush();
            }

            maxOpen = Math.Max(maxOpen, GroupKit.OpenPeerGroups(server, 5) + GroupKit.OpenPeerGroups(server, 9) + GroupKit.OpenPeerGroups(server, 11));
        }

        Assert.True(sent.Count > 1_000, $"only {sent.Count} messages were admitted");
        bool done = h.RunUntil(() =>
        {
            Settle();
            return arrivals.Count == sent.Count && pending.Count == 0;
        }, 60_000_000);
        PeerStatistics serverStatistics = DatagramKit.Statistics(server);
        Assert.True(done, $"received {arrivals.Count} of {sent.Count}; server reset {serverStatistics.StreamsReset}, idle timeouts "
            + $"{serverStatistics.StreamIdleTimeouts}, faults {serverStatistics.CallbackFaults}, pends {serverStatistics.StreamReceivePends}, "
            + $"open {GroupKit.OpenPeerGroups(server, 5)}/{GroupKit.OpenPeerGroups(server, 9)}/{GroupKit.OpenPeerGroups(server, 11)}");
        Assert.True(maxOpen > 8, $"the receiver never held more than {maxOpen} group streams");
        Assert.All(sent, m => Assert.Equal(DeliveryStatus.Delivered, status[m.Id]));
        Assert.All(sent, m => Assert.Equal(1, arrivals.GetValueOrDefault(m.Id)));
        Assert.Equal(sent.Count, order.Count);

        // Messages admitted between two flushes of a channel share a group, and a group's messages arrive in admission order.
        Dictionary<int, int> batchOf = sent.ToDictionary(m => m.Id, m => m.Batch);
        Dictionary<int, int> lastOfBatch = [];
        foreach (int id in order)
        {
            int b = batchOf[id];
            Assert.True(!lastOfBatch.TryGetValue(b, out int previous) || previous < id, $"message {id} arrived after {previous} of the same group");
            lastOfBatch[b] = id;
        }

        Assert.Equal(0, serverStatistics.StreamsReset);
        Assert.Equal(0, serverStatistics.CallbackFaults);
        Assert.Equal(0, DatagramKit.Statistics(client).CallbackFaults);
        Assert.True(h.RunUntil(() => GroupKit.OpenPeerGroups(server, 5) + GroupKit.OpenPeerGroups(server, 9) + GroupKit.OpenPeerGroups(server, 11) == 0
            && server.Core.Streams.Count == baselineStreams
            && GroupKit.Groups(client, 5) + GroupKit.Groups(client, 9) + GroupKit.Groups(client, 11) == 0));
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveBytesOutstanding);
        Assert.Equal(0, DatagramKit.Statistics(client).SendBytesOutstanding);
        Assert.Equal(baselineRented, Rented(server) + Rented(client));

        // The stream credit is all back: the sender can have MaxGroups streams of a channel open again at once.
        got_AfterBurst(h, client, server);
    }

    private static void got_AfterBurst(SessionHarness h, QuiclyPeer client, QuiclyPeer server)
    {
        int before = (int)DatagramKit.ChannelStats(server, 5).Received;
        for (int i = 0; i < 32; i++)
        {
            Assert.True(client.SendCopy(new SendHeader(5), DatagramKit.Payload(1_000_000 + i, 8)).IsAdmitted);
            client.Flush();
        }

        Assert.True(h.RunUntil(() => (int)DatagramKit.ChannelStats(server, 5).Received == before + 32, 20_000_000),
            "groups sent after the burst did not all arrive: stream credit or records were lost");
    }

    /// <summary>
    /// The receiver is not polled at all while the sender opens as many groups as the connection's stream limit allows on
    /// several channels; every one of them is pended behind a full ring. One resume per pended stream must be remembered:
    /// after the Polls every message arrives and nothing stalls.
    /// </summary>
    [Fact]
    public void Every_Stream_Pended_Behind_A_Full_Ring_Is_Resumed()
    {
        using SessionHarness h = new(table: GroupTables.Main, client: o =>
        {
            GroupKit.Prompt(o);
            GroupKit.Roomy(o);
        }, server: o =>
        {
            DatagramKit.Quiet(o);
            o.ReceiveRingCapacity = 2;
        });
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        int limit = server.Core.PeerUnidirectionalStreamLimit;
        ushort[] channels = [5, 6, 7, 8, 9, 10, 11, 12];
        HashSet<int> got = [];
        int duplicates = 0;
        MessageHandler handler = (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            if (!got.Add(BitConverter.ToInt32(payload)))
            {
                duplicates++;
            }
        };
        foreach (ushort channel in channels)
        {
            server.RegisterHandler(channel, handler);
        }

        // 40 ms without a Poll at the receiver; the sender forms a group per channel per step, three messages each.
        int next = 0;
        List<SendToken> tokens = [];
        for (int step = 0; step < 40; step++)
        {
            foreach (ushort channel in channels)
            {
                for (int i = 0; i < 3; i++)
                {
                    SendResult result = client.SendCopy(new SendHeader(channel, (ulong)next), DatagramKit.Payload(next, 24), SendOptions.Tracked);
                    if (result.IsAdmitted)
                    {
                        tokens.Add(result.Token);
                        next++;
                    }
                }
            }

            client.Poll();
            client.Flush();
            h.Network.Advance(1_000);
        }

        int open = channels.Sum(channel => GroupKit.OpenPeerGroups(server, channel));
        Assert.True(open >= limit - 1, $"only {open} group streams were open at the receiver (limit {limit})");
        bool done = h.RunUntil(() => got.Count == next, 20_000_000);
        PeerStatistics statistics = DatagramKit.Statistics(server);
        Assert.True(done, $"received {got.Count} of {next}; reset {statistics.StreamsReset}, faults {statistics.CallbackFaults}, pends {statistics.StreamReceivePends}, "
            + $"still open {channels.Sum(channel => GroupKit.OpenPeerGroups(server, channel))}");
        Assert.Equal(0, duplicates);
        Assert.Equal(0, statistics.StreamsReset);
        Assert.Equal(0, statistics.CallbackFaults);
        Assert.All(tokens, token => Assert.Equal(DeliveryStatus.Delivered, client.GetDeliveryStatus(token)));
        Assert.True(h.RunUntil(() => channels.Sum(channel => GroupKit.OpenPeerGroups(server, channel)) == 0));
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveBytesOutstanding);
    }

    /// <summary>
    /// A receiver that falls behind and catches up again, over and over, must not allocate (ADR 0008): the pended-stream
    /// ring, the receive records and the stream table are all at their peak after the warm-up.
    /// </summary>
    [Fact]
    public void A_Receiver_That_Keeps_Falling_Behind_Does_Not_Allocate()
    {
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 2_000 }, table: TestTables.Plumbing, client: GroupKit.Prompt, server: o =>
        {
            DatagramKit.Quiet(o);
            o.ReceiveRingCapacity = 64;
        });
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        long received = 0;
        server.RegisterHandler(11, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++);
        byte[] payload = new byte[32];
        long sent = 0;
        int maxOpen = 0;
        void Cycle()
        {
            // 64 ms behind: sixteen groups of sixteen while the server is not polled, then it catches up.
            for (int group = 0; group < 16; group++)
            {
                for (int i = 0; i < 16; i++)
                {
                    if (client.SendCopy(new SendHeader(11), payload).IsAdmitted)
                    {
                        sent++;
                    }
                }

                for (int step = 0; step < 4; step++)
                {
                    client.Poll();
                    client.Flush();
                    h.Network.Advance(1_000);
                }
            }

            maxOpen = Math.Max(maxOpen, GroupKit.OpenPeerGroups(server, 11));
            for (int step = 0; step < 100; step++)
            {
                server.Poll();
                server.Flush();
                client.Poll();
                client.Flush();
                h.Network.Advance(1_000);
            }
        }

        for (int i = 0; i < 300; i++)
        {
            Cycle();
        }

        WindowedAllocation.AssertNone(() =>
        {
            for (int i = 0; i < 40; i++)
            {
                Cycle();
            }
        });
        Assert.True(maxOpen > 8, $"the receiver never held more than {maxOpen} group streams");
        Assert.True(h.RunUntil(() => received == sent), $"received {received} of {sent}");
        Assert.Equal(0, DatagramKit.Statistics(server).StreamsReset);
        Assert.Equal(0, DatagramKit.Statistics(server).CallbackFaults);
    }

    /// <summary>
    /// The receiving peer is disposed in the middle of a burst while it holds more than MaxGroups streams, some of them with
    /// a staged message: every lease goes back to a shared allocator.
    /// </summary>
    [Fact]
    public void Disposing_A_Receiver_That_Is_Behind_Returns_Every_Lease_To_A_Shared_Allocator()
    {
        using SlabAllocator shared = new(PeerCore.CreateCompactAllocatorOptions());
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 2_000, StreamLossPercent = 5 }, table: GroupTables.Main, client: o =>
        {
            GroupKit.Prompt(o);
            GroupKit.Roomy(o);
        }, server: o =>
        {
            DatagramKit.Quiet(o);
            o.Allocator = shared;
            o.ReceiveRingCapacity = 8;
        });
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        int baseline = Rented(shared);
        h.StopPumpingServer();
        int next = 0;
        for (int step = 0; step < 30; step++)
        {
            foreach (ushort channel in new ushort[] { 5, 9, 11 })
            {
                // Large enough to span several stream packets, so some streams hold a staged (half-received) message.
                client.SendCopy(new SendHeader(channel), DatagramKit.Payload(next++, 3_000), SendOptions.Tracked);
                client.SendCopy(new SendHeader(channel), DatagramKit.Payload(next++, 40), SendOptions.Tracked);
            }

            client.Poll();
            client.Flush();
            h.Network.Advance(1_000);
        }

        Assert.True(GroupKit.OpenPeerGroups(server, 5) + GroupKit.OpenPeerGroups(server, 9) + GroupKit.OpenPeerGroups(server, 11) > 8);
        Assert.True(Rented(shared) > baseline);
        server.Dispose();
        for (int step = 0; step < 200; step++)
        {
            client.Poll();
            client.Flush();
            h.Network.Advance(1_000);
        }

        Assert.Equal(baseline, Rented(shared));
    }

    /// <summary>
    /// The receiving client loses its connection in the middle of a burst, on a lossy link, while it holds more than
    /// MaxGroups streams and some half-received messages; it reconnects without having polled. Every staging lease and ring
    /// reservation of the lost connection is given back, and the resumed connection carries the same load.
    /// </summary>
    [Theory]
    [InlineData(3)]
    [InlineData(11)]
    public void A_Reconnect_Mid_Burst_Returns_Every_Staging_Lease_And_The_Resumed_Session_Works(int seed)
    {
        using SlabAllocator shared = new(PeerCore.CreateCompactAllocatorOptions());
        using SessionHarness h = new(connect: false, seed: seed, link: new LinkOptions { DelayMicros = 2_000, StreamLossPercent = 5 },
            table: GroupTables.Main, server: o =>
            {
                GroupKit.Prompt(o);
                GroupKit.Roomy(o);
            }, client: o =>
            {
                QuietOptions.Apply(o);
                o.Allocator = shared;
                o.ReceiveRingCapacity = 8;
            });
        h.Admission.Handler = static (in HelloInfo hello, QuiclyPeer _) =>
            AdmissionResult.Accept(new byte[] { 1, 2, 3, 4 }, 77, epoch: hello.SessionToken.IsEmpty ? 1u : 2u);
        Assert.True(h.RunUntilConnected());
        QuiclyPeer client = h.Client;
        QuiclyPeer oldServer = h.Server!;
        int baseline = Rented(shared);
        HashSet<int> got = [];
        int duplicates = 0;
        MessageHandler handler = (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            if (!got.Add(BitConverter.ToInt32(payload)))
            {
                duplicates++;
            }
        };
        ushort[] channels = [5, 9, 11];
        foreach (ushort channel in channels)
        {
            client.RegisterHandler(channel, handler);
        }

        int Burst(QuiclyPeer sender, int firstId)
        {
            int id = firstId;
            for (int step = 0; step < 30; step++)
            {
                foreach (ushort channel in channels)
                {
                    if (sender.SendCopy(new SendHeader(channel), DatagramKit.Payload(id, 3_000)).IsAdmitted)
                    {
                        id++;
                    }

                    if (sender.SendCopy(new SendHeader(channel), DatagramKit.Payload(id, 40)).IsAdmitted)
                    {
                        id++;
                    }
                }

                sender.Poll();
                sender.Flush();
                h.Network.Advance(1_000);
            }

            return id;
        }

        Burst(oldServer, 0);
        Assert.True(channels.Sum(channel => GroupKit.OpenPeerGroups(client, channel)) > 8, "the scenario needs more than MaxGroups streams at the receiver");
        Assert.True(Rented(shared) > baseline);
        oldServer.Core.Transport!.Close((ulong)QuiclyErrorCode.NoError, default);
        for (int step = 0; step < 200 && !client.Core.IsTransportClosed; step++)
        {
            h.Network.Advance(1_000);
        }

        Assert.True(client.Core.IsTransportClosed);
        client.Reconnect(h.Connector, h.Listener.LocalEndPoint, "test", default);
        Assert.Equal(0, channels.Sum(channel => GroupKit.OpenPeerGroups(client, channel)));
        Assert.True(h.RunUntil(() => client.State == PeerState.Connected && h.Server is not null
            && !ReferenceEquals(h.Server, oldServer) && h.Server.State == PeerState.Connected));
        oldServer.Dispose();

        // Everything of the lost connection has been dispatched or dropped by now: nothing of it is still rented.
        Assert.True(h.RunUntil(() => Rented(shared) == baseline, 2_000_000), $"{Rented(shared) - baseline} receive blocks of the lost connection are still rented");
        Assert.Equal(0, DatagramKit.Statistics(client).ReceiveBytesOutstanding);

        got.Clear();
        duplicates = 0;
        int last = Burst(h.Server!, 10_000);
        Assert.True(h.RunUntil(() => got.Count == last - 10_000, 30_000_000), $"received {got.Count} of {last - 10_000} after the reconnect; "
            + $"client reset {DatagramKit.Statistics(client).StreamsReset}, faults {DatagramKit.Statistics(client).CallbackFaults}");
        Assert.Equal(0, duplicates);
        Assert.Equal(0, DatagramKit.Statistics(client).StreamsReset);
        Assert.Equal(0, DatagramKit.Statistics(client).CallbackFaults);
        Assert.True(h.RunUntil(() => channels.Sum(channel => GroupKit.OpenPeerGroups(client, channel)) == 0 && Rented(shared) == baseline));
    }

    /// <summary>
    /// The same at the 4 096-stream cap of the connection (four channels of MaxGroups 1 024): the receiver is not polled
    /// until the sender has every stream the limit allows open at it, each pended behind a full ring; then it polls.
    /// </summary>
    [Fact]
    public void At_The_Stream_Cap_Every_Pended_Stream_Is_Resumed_And_Every_Record_Comes_Back()
    {
        ChannelTable table = ChannelTable.Create()
            .Add(2, "moves", ChannelMode.UnreliableUnordered)
            .Add(5, "a", ChannelMode.ReliableUnordered, o => o.MaxGroups = 1024)
            .Add(6, "b", ChannelMode.ReliableUnordered, o => o.MaxGroups = 1024)
            .Add(7, "c", ChannelMode.ReliableUnordered, o => o.MaxGroups = 1024)
            .Add(8, "d", ChannelMode.ReliableUnordered, o => o.MaxGroups = 1024)
            .Build();
        using SessionHarness h = new(table: table, client: o =>
        {
            GroupKit.Prompt(o);
            GroupKit.Roomy(o);
            o.SendTableCapacity = 16384;
            o.SegmentArenaCapacity = 16384;
        }, server: o =>
        {
            DatagramKit.Quiet(o);
            o.ReceiveRingCapacity = 16;
        });
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        int limit = server.Core.PeerUnidirectionalStreamLimit;
        Assert.Equal(4096, limit);
        ushort[] channels = [5, 6, 7, 8];
        HashSet<int> got = [];
        int duplicates = 0;
        MessageHandler handler = (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            if (!got.Add(BitConverter.ToInt32(payload)))
            {
                duplicates++;
            }
        };
        foreach (ushort channel in channels)
        {
            server.RegisterHandler(channel, handler);
        }

        int next = 0;
        for (int step = 0; step < 1_100; step++)
        {
            foreach (ushort channel in channels)
            {
                if (client.SendCopy(new SendHeader(channel), DatagramKit.Payload(next, 8)).IsAdmitted)
                {
                    next++;
                }
            }

            client.Poll();
            client.Flush();
            h.Network.Advance(1_000);
        }

        int open = channels.Sum(channel => GroupKit.OpenPeerGroups(server, channel));
        Assert.True(open >= limit - 16, $"only {open} group streams were open at the receiver (limit {limit})");
        bool done = h.RunUntil(() => got.Count == next, 60_000_000);
        PeerStatistics statistics = DatagramKit.Statistics(server);
        Assert.True(done, $"received {got.Count} of {next}; reset {statistics.StreamsReset}, faults {statistics.CallbackFaults}, pends {statistics.StreamReceivePends}, "
            + $"still open {channels.Sum(channel => GroupKit.OpenPeerGroups(server, channel))}");
        Assert.Equal(0, duplicates);
        Assert.Equal(0, statistics.StreamsReset);
        Assert.Equal(0, statistics.CallbackFaults);
        Assert.Equal(0, DatagramKit.Statistics(client).CallbackFaults);
        Assert.True(h.RunUntil(() => channels.Sum(channel => GroupKit.OpenPeerGroups(server, channel)) == 0));
        Assert.Equal(0, DatagramKit.Statistics(server).ReceiveBytesOutstanding);
    }

    private static int Rented(QuiclyPeer peer) => Rented(peer.Core.Allocator);

    private static int Rented(SlabAllocator allocator)
    {
        int rented = 0;
        for (int i = 0; i < allocator.ClassCount; i++)
        {
            rented += allocator.GetClassStatistics(i).Rented;
        }

        return rented;
    }

    /// <summary>Sixteen tracked messages per group, the sender pumped for 4 ms after each, the receiver not at all.</summary>
    private static List<SendToken> SendGroupsToALateReceiver(SimFixture h, QuiclyPeer sender, ushort channel, int count)
    {
        List<SendToken> tokens = [];
        for (int i = 0; i < count; i++)
        {
            SendResult result = sender.SendCopy(new SendHeader(channel), DatagramKit.Payload(i, 4), SendOptions.Tracked);
            Assert.True(result.IsAdmitted);
            tokens.Add(result.Token);
            if ((i & 15) == 15)
            {
                for (int step = 0; step < 4; step++)
                {
                    sender.Poll();
                    sender.Flush();
                    h.Network.Advance(1_000);
                }
            }
        }

        return tokens;
    }
}
