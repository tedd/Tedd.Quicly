using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Tests.Session;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Review;

/// <summary>
/// Second adversarial review of fix/group-stream-receive-limit, lens "swap": the re-size of the pended-stream ring and of
/// the group engine's receive records in <c>OnConnected</c> (commits b55144c and cd7bdeb). Each test states a property the
/// branch claims or relies on.
/// </summary>
public class ReviewGroup2SwapTests
{
    private static readonly byte[] ResumeToken = [9, 8, 7, 6];

    /// <summary>
    /// Two group channels (MaxGroups 8 and 2) and an ordered one: session limit 11.
    /// </summary>
    private static readonly ChannelTable TwoGroups = ChannelTable.Create()
        .Add(2, "a", ChannelMode.UnreliableUnordered)
        .Add(10, "stream", ChannelMode.ReliableOrdered)
        .Add(11, "group", ChannelMode.ReliableUnordered)
        .Add(12, "few", ChannelMode.ReliableUnordered, o => o.MaxGroups = 2)
        .Build();

    /// <summary>
    /// What breaks when a transport does not honour "the capacity is reported before any stream of the connection exists".
    /// <c>PeerCore.SetTransportPeerStreams</c> replaces the pended-stream ring and leaves the old one "empty" by assumption.
    /// A transport that reports its grant while streams are pended (a second <c>OnConnected</c>, which the peer otherwise
    /// ignores, or one that reports late) strands every id that is in the old ring: <c>Poll</c> reads the new ring, so those
    /// streams are never resumed. Their sender completed the messages Delivered long ago; this end holds the streams, and
    /// their slots of the stream limit, until the connection ends. Nothing counts it (no reset, no fault). Memory stays safe
    /// (the old ring is kept, the records keep their index); what is lost is the data.
    /// </summary>
    [Fact]
    public void A_Grant_Reported_While_Streams_Are_Pended_Loses_No_Stream()
    {
        using SessionHarness h = new(table: TestTables.Plumbing, client: GroupKit.Prompt, server: o =>
        {
            DatagramKit.Quiet(o);
            o.ReceiveRingCapacity = 64;
        });
        QuiclyPeer server = h.Server!;
        List<int> got = [];
        server.RegisterHandler(11, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => got.Add(BitConverter.ToInt32(payload)));

        List<SendToken> tokens = SendGroups(h, h.Client, 11, 320, 16);
        int pended = server.Core.PendedStreams.Count;
        Assert.True(pended > 0, "the scenario needs streams pended behind the full receive ring");

        // The transport double: the same transport reports itself connected again, now with a larger grant.
        TransportConnectedInfo info = default;
        info.Capabilities = server.Core.Transport!.Capabilities;
        info.Capabilities.PeerUnidirectionalStreams = 1024;
        server.TransportSink.OnConnected(in info);
        Assert.Equal(PeerState.Connected, server.State);

        bool all = h.RunUntil(() => got.Count == 320, 2_000_000);
        int delivered = tokens.Count(token => h.Client.GetDeliveryStatus(token) == DeliveryStatus.Delivered);
        PeerStatistics statistics = DatagramKit.Statistics(server);
        Assert.True(all,
            $"received {got.Count} of 320 messages after the grant was reported with {pended} streams pended; the sender reported {delivered} Delivered; "
            + $"the server counts {statistics.StreamsReset} resets and {statistics.CallbackFaults} callback faults and still holds "
            + $"{GroupKit.OpenPeerGroups(server, 11)} peer streams open");
    }

    /// <summary>
    /// The long run: a client at the MsQuic default grant (1 024) receives groups on two channels from a server that keeps
    /// MaxGroups, over a link with stream loss and jitter, and is late with its Poll in about half of the rounds. After
    /// every round every message the server completed Delivered has arrived exactly once and no peer stream is left open.
    /// At the end the client takes exactly as many concurrent streams as the transport admits without resetting one: a
    /// receive record that leaked in any round would be missing from the free list here.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(7)]
    public void Over_A_Long_Run_With_A_Receiver_That_Is_Sometimes_Late_Every_Receive_Record_Comes_Back(int seed)
    {
        LinkOptions link = new() { PeerUnidiStreams = 1024, DelayMicros = 500, JitterMicros = 200, StreamLossPercent = 2 };
        using SessionHarness h = new(link: link, table: TwoGroups, seed: seed, server: o =>
        {
            GroupKit.Prompt(o);
            GroupKit.Roomy(o);
        }, client: o =>
        {
            DatagramKit.Quiet(o);
            o.ReceiveRingCapacity = 64;
        });
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        Assert.Equal(11, client.Core.PeerUnidirectionalStreamLimit);
        Assert.Equal(1024, client.Core.PeerStreamCapacity);
        Dictionary<int, int> got = [];
        MessageHandler collect = (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            int id = BitConverter.ToInt32(payload);
            got[id] = got.GetValueOrDefault(id) + 1;
        };
        client.RegisterHandler(11, collect);
        client.RegisterHandler(12, collect);

        Random random = new(seed);
        int next = 0;
        List<SendToken> tokens = [];
        for (int round = 0; round < 60; round++)
        {
            ushort channel = random.Next(3) == 0 ? (ushort)12 : (ushort)11;
            int groups = 1 + random.Next(40);
            int perGroup = 1 + random.Next(4);
            bool late = random.Next(2) == 0;
            for (int group = 0; group < groups; group++)
            {
                for (int i = 0; i < perGroup; i++)
                {
                    SendResult result = server.SendCopy(new SendHeader(channel), DatagramKit.Payload(next++, 4), SendOptions.Tracked);
                    Assert.True(result.IsAdmitted, $"round {round}: send refused with {result.Status}");
                    tokens.Add(result.Token);
                }

                for (int step = 0; step < 3; step++)
                {
                    server.Poll();
                    server.Flush();
                    if (!late)
                    {
                        client.Poll();
                        client.Flush();
                    }

                    h.Network.Advance(1_000);
                }
            }

            int expected = next;
            Assert.True(h.RunUntil(() => got.Count == expected, 5_000_000),
                $"round {round}: received {got.Count} of {expected} messages; the client reset {DatagramKit.Statistics(client).StreamsReset} streams");
            Assert.True(h.RunUntil(() => GroupKit.OpenPeerGroups(client, 11) == 0 && GroupKit.OpenPeerGroups(client, 12) == 0, 5_000_000),
                $"round {round}: {GroupKit.OpenPeerGroups(client, 11)} + {GroupKit.OpenPeerGroups(client, 12)} peer streams stayed open after the client caught up");
        }

        Assert.All(got.Values, count => Assert.Equal(1, count));
        Assert.True(h.RunUntil(() => tokens.All(token => server.GetDeliveryStatus(token) == DeliveryStatus.Delivered), 5_000_000),
            "not every token is Delivered: " + string.Join(", ", tokens.GroupBy(token => server.GetDeliveryStatus(token)).Select(g => $"{g.Count()} {g.Key}")));
        PeerStatistics statistics = DatagramKit.Statistics(client);
        Assert.Equal(0, statistics.StreamsReset);
        Assert.Equal(0, statistics.CallbackFaults);
        Assert.Equal(0, statistics.ReceiveBytesOutstanding);

        // Every record is back: the client, not polled, takes one stream per record and resets none.
        int before = next;
        SendStatus refusal = SendStatus.Admitted;
        for (int group = 0; group < 1_200; group++)
        {
            // Once the client holds every stream the transport admits, the server's groups wait for credit and its channel
            // fills: the burst ends there.
            SendResult result = server.SendCopy(new SendHeader(11), DatagramKit.Payload(next, 4), SendOptions.Tracked);
            if (!result.IsAdmitted)
            {
                refusal = result.Status;
                break;
            }

            next++;
            for (int step = 0; step < 3; step++)
            {
                server.Poll();
                server.Flush();
                h.Network.Advance(1_000);
            }
        }

        for (int step = 0; step < 200; step++)
        {
            server.Poll();
            server.Flush();
            h.Network.Advance(1_000);
        }

        int held = GroupKit.OpenPeerGroups(client, 11);
        Assert.Equal(0, DatagramKit.Statistics(client).StreamsReset);
        Assert.True(held == 1024,
            $"the client holds {held} peer streams at a capacity of 1 024 after {next - before} groups (the burst ended with {refusal}); a smaller number means records were not on the free list");
        int total = next;
        Assert.True(h.RunUntil(() => got.Count == total, 20_000_000),
            $"received {got.Count - before} of {total - before} messages of the final burst; the client reset {DatagramKit.Statistics(client).StreamsReset} streams");
        Assert.All(got.Values, count => Assert.Equal(1, count));
        Assert.True(h.RunUntil(() => GroupKit.OpenPeerGroups(client, 11) == 0, 5_000_000));
        statistics = DatagramKit.Statistics(client);
        Assert.Equal(0, statistics.StreamsReset);
        Assert.Equal(0, statistics.CallbackFaults);
        Assert.Equal(0, statistics.ReceiveBytesOutstanding);
    }

    /// <summary>
    /// Reconnects alternate between a transport that grants nothing by itself and one that grants 1 024, with the client
    /// late on every connection and holding streams when the connection is lost. The capacity only grows, the records and
    /// the ring of the wide connection serve the narrow one, and every connection delivers what its server completed.
    /// </summary>
    [Fact]
    public void Reconnects_Between_Narrow_And_Wide_Transports_Keep_Every_Record_And_Every_Pended_Stream()
    {
        using SessionHarness h = new(connect: false, table: TestTables.Plumbing, server: GroupKit.Prompt, client: o =>
        {
            QuietOptions.Apply(o);
            o.ReceiveRingCapacity = 64;
        });
        uint epoch = 0;
        h.Admission.Handler = (in HelloInfo _, QuiclyPeer _) => AdmissionResult.Accept(ResumeToken, 4242, epoch: ++epoch);
        Assert.True(h.RunUntilConnected());
        QuiclyPeer client = h.Client;
        List<int> got = [];
        client.RegisterHandler(11, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => got.Add(BitConverter.ToInt32(payload)));
        Assert.Equal(9, client.Core.PeerStreamCapacity);

        for (int cycle = 0; cycle < 6; cycle++)
        {
            bool wide = (cycle & 1) == 0;
            QuiclyPeer oldServer = h.Server!;

            // The connection is lost while the client holds streams of a burst it has not read.
            SendGroups(h, oldServer, 11, 160, 16);
            oldServer.Core.Transport!.Close((ulong)QuiclyErrorCode.NoError, default);
            for (int step = 0; step < 100 && !client.Core.IsTransportClosed; step++)
            {
                h.Network.Advance(1_000);
            }

            Assert.True(client.Core.IsTransportClosed);
            SimulatedConnector connector = new(h.Network, new LinkOptions { PeerUnidiStreams = (ushort)(wide ? 1024 : 0) });
            client.Reconnect(connector, h.Listener.LocalEndPoint, "test", default);
            Assert.True(h.RunUntil(() => client.State == PeerState.Connected && h.Server is not null
                && !ReferenceEquals(h.Server, oldServer) && h.Server.State == PeerState.Connected), $"cycle {cycle}: the resumed handshake did not complete");
            oldServer.Dispose();
            Assert.Equal(1024, client.Core.PeerStreamCapacity);
            Assert.Equal(0, GroupKit.OpenPeerGroups(client, 11));
            Assert.True(client.Core.PendedStreams.IsEmpty);

            got.Clear();
            List<SendToken> tokens = SendGroups(h, h.Server!, 11, 640, 16);
            int open = GroupKit.OpenPeerGroups(client, 11);
            Assert.True(h.RunUntil(() => got.Count == 640, 3_000_000),
                $"cycle {cycle} ({(wide ? "wide" : "narrow")}): received {got.Count} of 640; the client reset {DatagramKit.Statistics(client).StreamsReset} streams");
            Assert.Equal(Enumerable.Range(0, 640), got.Order());
            Assert.All(tokens, token => Assert.Equal(DeliveryStatus.Delivered, h.Server!.GetDeliveryStatus(token)));
            Assert.True(wide ? open > 9 : open <= 9, $"cycle {cycle}: {open} peer streams were open at the late client on a {(wide ? "wide" : "narrow")} transport");
            Assert.True(h.RunUntil(() => GroupKit.OpenPeerGroups(client, 11) == 0));
        }

        PeerStatistics statistics = DatagramKit.Statistics(client);
        Assert.Equal(0, statistics.StreamsReset);
        Assert.Equal(0, statistics.CallbackFaults);
        Assert.Equal(0, statistics.ReceiveBytesOutstanding);
    }

    /// <summary>
    /// A connection that never reports <c>OnConnected</c> (the handshake fails) leaves the session at its own limit; the
    /// reconnect that follows goes to a transport with the default MsQuic client grant and makes room then.
    /// </summary>
    [Fact]
    public void A_Connection_That_Never_Connected_Is_Followed_By_A_Reconnect_That_Makes_Room()
    {
        using SessionHarness h = new(connect: false, link: new LinkOptions { FailHandshake = true, PeerUnidiStreams = 1024 }, table: TestTables.Plumbing,
            server: GroupKit.Prompt, client: o =>
            {
                QuietOptions.Apply(o);
                o.ReceiveRingCapacity = 64;
            });
        QuiclyPeer client = h.Client;
        Assert.True(h.RunUntil(() => client.State == PeerState.Closed), "the failed handshake did not close the client");
        Assert.Equal(9, client.Core.PeerStreamCapacity);
        QuiclyPeer? refused = h.Server;
        Assert.True(client.CanReconnect);

        client.Reconnect(new SimulatedConnector(h.Network, new LinkOptions { PeerUnidiStreams = 1024 }), h.Listener.LocalEndPoint, "test", default);
        Assert.True(h.RunUntil(() => client.State == PeerState.Connected && h.Server is not null && !ReferenceEquals(h.Server, refused)
            && h.Server.State == PeerState.Connected), "the reconnect did not complete");
        refused?.Dispose();
        Assert.Equal(1024, client.Core.PeerStreamCapacity);
        List<int> got = [];
        client.RegisterHandler(11, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => got.Add(BitConverter.ToInt32(payload)));
        SendGroups(h, h.Server!, 11, 640, 16);
        Assert.True(GroupKit.OpenPeerGroups(client, 11) > 9);
        Assert.True(h.RunUntil(() => got.Count == 640, 3_000_000), $"received {got.Count} of 640; the client reset {DatagramKit.Statistics(client).StreamsReset} streams");
        Assert.Equal(0, DatagramKit.Statistics(client).StreamsReset);
    }

    /// <summary>
    /// The threads of ADR 0008 around the swap: the transport thread grows the capacity step by step while the game thread
    /// polls and a third thread asks <c>HasPendingWork</c>. Every ring that was replaced is still allocated, so none of the
    /// readers touches freed memory (an access violation would take the test host down), and the peer works afterwards.
    /// </summary>
    [Fact]
    public void The_Capacity_Grows_On_One_Thread_While_Another_Polls_And_A_Third_Probes_For_Work()
    {
        using SessionHarness h = new(table: TestTables.Plumbing);
        QuiclyPeer server = h.Server!;
        PeerCore core = server.Core;
        using CancellationTokenSource stop = new();
        long polls = 0;
        long probes = 0;
        Thread prober = new(() =>
        {
            while (!stop.IsCancellationRequested)
            {
                _ = server.HasPendingWork;
                _ = core.PendedStreams.IsEmpty;
                Interlocked.Increment(ref probes);
            }
        });
        Thread grower = new(() =>
        {
            for (int capacity = 16; capacity <= 8192; capacity += 7)
            {
                core.SetTransportPeerStreams(capacity);
                if ((capacity & 63) == 0)
                {
                    Thread.Yield();
                }
            }
        });
        prober.Start();
        grower.Start();
        while (grower.IsAlive)
        {
            server.Poll();
            polls++;
        }

        grower.Join();
        stop.Cancel();
        prober.Join();
        Assert.True(polls > 0 && Interlocked.Read(ref probes) > 0);
        Assert.Equal(8192, core.PeerStreamCapacity);
        Assert.True(core.PendedStreams.Capacity >= 8194);

        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        server.RegisterHandler(11, Handlers.Collect(got));
        Assert.True(h.Client.SendCopy(new SendHeader(11), [1, 2, 3]).IsAdmitted);
        Assert.True(h.RunUntil(() => got.Count == 1));
        Assert.Equal(0, DatagramKit.Statistics(server).CallbackFaults);
    }

    /// <summary>
    /// The numbers ADR 0009 and ARCHITECTURE.md give for a client at the default MsQuic grant: a ring of 2 048 ids of 8
    /// bytes (16 KiB) for a capacity of 1 024, and 1 MiB at the most the option allows.
    /// </summary>
    [Fact]
    public void The_Pended_Stream_Ring_Costs_What_The_Documents_Say()
    {
        using SessionHarness h = new(table: TestTables.Plumbing);
        PeerCore core = h.Server!.Core;
        core.SetTransportPeerStreams(1024);
        Assert.Equal(16 * 1024, core.PendedStreams.ByteLength);
        core.SetTransportPeerStreams(4096);
        Assert.Equal(64 * 1024, core.PendedStreams.ByteLength);
        core.SetTransportPeerStreams(int.MaxValue);
        Assert.Equal(ushort.MaxValue, core.PeerStreamCapacity);
        Assert.Equal(1024 * 1024, core.PendedStreams.ByteLength);
    }

    /// <summary>
    /// Sends <paramref name="count"/> numbered, tracked messages, one group per <paramref name="perGroup"/>, while only the
    /// sender is pumped (the receiver is late with its Poll for 4 ms per group).
    /// </summary>
    private static List<SendToken> SendGroups(SimFixture h, QuiclyPeer sender, ushort channel, int count, int perGroup)
    {
        List<SendToken> tokens = [];
        for (int i = 0; i < count; i++)
        {
            SendResult result = sender.SendCopy(new SendHeader(channel), DatagramKit.Payload(i, 4), SendOptions.Tracked);
            Assert.True(result.IsAdmitted);
            tokens.Add(result.Token);
            if ((i + 1) % perGroup == 0)
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
