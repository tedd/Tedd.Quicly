using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.Tests.Session;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Review;

/// <summary>
/// Third adversarial review of the group-stream fix (0ee6b8e), lens "group-resources" (threading and resources against
/// ADR 0008): the budgeted resume of held streams, the ReliableLatest stream-notice ring, the release of a peer stream's
/// record by its cookie, and exactly-once delivery with every resource back at its baseline under late receivers,
/// reconnects and disposal mid-burst.
/// </summary>
public class ReviewStackgroupresourcesTests
{
    private const ushort Groups = 11;
    private const ushort Few = 12;
    private const ushort Ordered = 10;
    private const ushort Latest = 20;

    /// <summary>Two group channels, an ordered channel and a ReliableLatest channel with room for large values.</summary>
    private static readonly ChannelTable Mixed = ChannelTable.Create()
        .Add(2, "a", ChannelMode.UnreliableUnordered)
        .Add(Ordered, "stream", ChannelMode.ReliableOrdered)
        .Add(Groups, "group", ChannelMode.ReliableUnordered, o => o.MaxMessageSize = 200 * 1024)
        .Add(Few, "few", ChannelMode.ReliableUnordered, o => o.MaxGroups = 2)
        .Add(Latest, "latest", ChannelMode.ReliableLatest, o =>
        {
            o.MaxKeys = 64;
            o.MaxGroups = 4;
            o.MaxMessageSize = 64 * 1024;
        })
        .Build();

    /// <summary>
    /// QuiclyPeer.ResumePendedStreams (04f1eae) resumes "no more of them than the ring has free slots", so that a catch-up
    /// after a long hitch does not resume every held stream at every Poll to let a ring's worth through (second-round
    /// finding G2L-7: O(N² / ring) hold-and-resume cycles). The bound is the ring's free slots only. When what holds the
    /// streams back is the receive <em>budget</em> — the default 256 KiB, a few messages of 20 KiB — the ring has thousands
    /// of free slots, every held stream is resumed at every Poll and all but a handful are held back again at once: the
    /// quadratic catch-up the change was meant to remove is still there. Each cycle is two transport calls and a receive
    /// callback on the transport thread. Measured: pends per delivered message.
    /// </summary>
    [Fact]
    public void A_Catch_Up_Bounded_By_The_Receive_Budget_Resumes_Each_Held_Stream_A_Bounded_Number_Of_Times()
    {
        using SessionHarness h = new(link: new LinkOptions { PeerUnidiStreams = 1024 }, table: Mixed, server: o =>
        {
            GroupKit.Prompt(o);
            GroupKit.Roomy(o);
            o.SendBudgetBytes = 32 * 1024 * 1024;
        }, client: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        HashSet<int> got = [];
        int duplicates = 0;
        client.RegisterHandler(Groups, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) =>
        {
            if (!got.Add(BitConverter.ToInt32(payload)))
            {
                duplicates++;
            }
        });

        // The client is late: only the server is pumped while it sends 600 groups of one 20 KiB message.
        const int Count = 600;
        for (int i = 0; i < Count; i++)
        {
            Assert.True(server.SendCopy(new SendHeader(Groups), DatagramKit.Payload(i, 20 * 1024)).IsAdmitted, $"send {i} refused");
            server.Poll();
            server.Flush();
            h.Network.Advance(200);
        }

        for (int step = 0; step < 200; step++)
        {
            server.Poll();
            server.Flush();
            h.Network.Advance(1_000);
        }

        int held = GroupKit.OpenPeerGroups(client, Groups);
        Assert.True(held > 500, $"the scenario needs most groups held at the late client; {held} are");
        long pendsBefore = DatagramKit.Statistics(client).StreamReceivePends;
        int polls = 0;
        bool all = h.RunUntil(() =>
        {
            polls++;
            return got.Count == Count;
        }, 60_000_000, 1_000);
        PeerStatistics statistics = DatagramKit.Statistics(client);
        long pends = statistics.StreamReceivePends - pendsBefore;
        Assert.True(all, $"received {got.Count} of {Count}");
        Assert.Equal(0, duplicates);
        Assert.True(pends <= 4L * Count,
            $"catching up {Count} messages from {held} held streams took {polls} polls and {pends} hold-and-resume cycles ({(double)pends / Count:0.0} per message): "
            + "the resume is bounded by the ring's free slots, but the receive budget is what binds");
    }

    /// <summary>
    /// Guard for the ReliableLatest stream-notice ring (b9cd90b): two notices per stream the engine can have recorded, and
    /// the game thread frees a counted slot only when it takes the stream's shutdown notice, so the transport thread can
    /// never post more than the ring holds. Hammered with every key updated with a large value at every flush (each update
    /// aborts the previous version's stream) towards a receiver that is late for long stretches: no notice is lost
    /// (CallbackFaults 0), the channel's counted streams are all given back, and every key ends at its last value.
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(5)]
    public void Guard_The_Latest_Stream_Notice_Ring_Does_Not_Overflow_Under_Churn(int seed)
    {
        using SessionHarness h = new(seed: seed, link: new LinkOptions { PeerUnidiStreams = 1024, DelayMicros = 800, JitterMicros = 400, StreamLossPercent = 3 },
            table: Mixed, server: o =>
            {
                GroupKit.Prompt(o);
                GroupKit.Roomy(o);
            }, client: DatagramKit.Quiet);
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        Dictionary<ulong, int> last = [];
        client.RegisterHandler(Latest, (QuiclyPeer _, in ReceiveHeader header, ReadOnlySpan<byte> payload) => last[header.Key] = BitConverter.ToInt32(payload));

        Random random = new(seed);
        int version = 0;
        Dictionary<ulong, int> sentLast = [];
        for (int round = 0; round < 40; round++)
        {
            bool late = random.Next(2) == 0;
            for (int step = 0; step < 20; step++)
            {
                for (ulong key = 0; key < 16; key++)
                {
                    int size = random.Next(4) == 0 ? 40 : 3_000 + random.Next(20_000);
                    if (server.SendCopy(new SendHeader(Latest, key), DatagramKit.Payload(++version, size)).IsAdmitted)
                    {
                        sentLast[key] = version;
                    }
                }

                server.Poll();
                server.Flush();
                if (!late)
                {
                    client.Poll();
                    client.Flush();
                }

                h.Network.Advance(500);
            }
        }

        Assert.True(h.RunUntil(() => sentLast.All(pair => last.TryGetValue(pair.Key, out int value) && value == pair.Value), 30_000_000),
            $"{sentLast.Count(pair => !last.TryGetValue(pair.Key, out int value) || value != pair.Value)} keys are not at their last value");
        Assert.Equal(0, DatagramKit.Statistics(server).CallbackFaults);
        Assert.Equal(0, DatagramKit.Statistics(client).CallbackFaults);
        Assert.Equal(0, DatagramKit.Statistics(client).StreamsReset);
    }

    /// <summary>
    /// Guard, the long stress of the lens: both directions late in turn, two group channels, an ordered channel and large
    /// ReliableLatest values at a client that granted the MsQuic default 1 024 streams, over a lossy link; then a reconnect
    /// in the middle of a burst the client holds, and disposal of the server in the middle of another. While a connection
    /// lasts every message completed Delivered arrives exactly once and nothing Fails, ordered messages in order, every
    /// latest key at its last value; across the reconnect nothing arrives twice; after each loss the shared allocator's
    /// Rented count returns to its baseline and no peer stream stays open.
    /// </summary>
    [Theory]
    [InlineData(2)]
    [InlineData(9)]
    public void Guard_Late_Receivers_In_Both_Directions_Reconnect_And_Dispose_Mid_Burst_Lose_Nothing_And_Leak_Nothing(int seed)
    {
        using SlabAllocator shared = new(PeerCore.CreateCompactAllocatorOptions());
        using SessionHarness h = new(connect: false, seed: seed, link: new LinkOptions { PeerUnidiStreams = 1024, DelayMicros = 1_000, JitterMicros = 300, StreamLossPercent = 2 },
            table: Mixed, server: o =>
            {
                GroupKit.Prompt(o);
                GroupKit.Roomy(o);
            }, client: o =>
            {
                GroupKit.Prompt(o);
                o.Allocator = shared;
                o.ReceiveRingCapacity = 32;
                o.SendBudgetBytes = 4 * 1024 * 1024;
                o.SendTableCapacity = 4096;
                o.SegmentArenaCapacity = 4096;
            });
        h.Admission.Handler = static (in HelloInfo hello, QuiclyPeer _) =>
            AdmissionResult.Accept(new byte[] { 1, 2, 3, 4 }, 77, epoch: hello.SessionToken.IsEmpty ? 1u : 2u);
        Assert.True(h.RunUntilConnected());
        QuiclyPeer client = h.Client;
        Assert.Equal(1024, client.Core.PeerStreamCapacity);
        int baseline = Rented(shared);

        Dictionary<int, int> atClient = [];
        List<int> orderedAtClient = [];
        Dictionary<int, int> atServer = [];
        List<int> orderedAtServer = [];
        Dictionary<ulong, int> latestAtClient = [];
        MessageHandler clientCollect = (QuiclyPeer _, in ReceiveHeader header, ReadOnlySpan<byte> payload) =>
        {
            int id = BitConverter.ToInt32(payload);
            atClient[id] = atClient.GetValueOrDefault(id) + 1;
            if (header.Channel == Ordered)
            {
                orderedAtClient.Add(id);
            }
        };
        client.RegisterHandler(Groups, clientCollect);
        client.RegisterHandler(Few, clientCollect);
        client.RegisterHandler(Ordered, clientCollect);
        client.RegisterHandler(Latest, (QuiclyPeer _, in ReceiveHeader header, ReadOnlySpan<byte> payload) => latestAtClient[header.Key] = BitConverter.ToInt32(payload));

        void RegisterServer(QuiclyPeer server)
        {
            MessageHandler collect = (QuiclyPeer _, in ReceiveHeader header, ReadOnlySpan<byte> payload) =>
            {
                int id = BitConverter.ToInt32(payload);
                atServer[id] = atServer.GetValueOrDefault(id) + 1;
                if (header.Channel == Ordered)
                {
                    orderedAtServer.Add(id);
                }
            };
            server.RegisterHandler(Groups, collect);
            server.RegisterHandler(Few, collect);
            server.RegisterHandler(Ordered, collect);
        }

        RegisterServer(h.Server!);
        Random random = new(seed);
        int next = 0;
        int latestVersion = 0;
        List<(int Id, SendToken Token, bool FromServer)> tracked = [];
        Dictionary<ulong, int> latestSent = [];

        void Burst(QuiclyPeer sender, bool fromServer, bool receiverLate, int steps)
        {
            QuiclyPeer receiver = fromServer ? client : h.Server!;
            for (int step = 0; step < steps; step++)
            {
                ushort channel = random.Next(5) switch { 0 => Ordered, 1 => Few, _ => Groups };
                int size = random.Next(8) == 0 ? 3_000 + random.Next(12_000) : 16 + random.Next(200);
                SendResult result = sender.SendCopy(new SendHeader(channel), DatagramKit.Payload(next, size), SendOptions.Tracked);
                if (result.IsAdmitted)
                {
                    tracked.Add((next, result.Token, fromServer));
                    next++;
                }

                if (fromServer && random.Next(3) == 0)
                {
                    ulong key = (ulong)random.Next(8);
                    int valueSize = random.Next(2) == 0 ? 24 : 2_000 + random.Next(30_000);
                    if (sender.SendCopy(new SendHeader(Latest, key), DatagramKit.Payload(++latestVersion, valueSize)).IsAdmitted)
                    {
                        latestSent[key] = latestVersion;
                    }
                }

                sender.Poll();
                sender.Flush();
                if (!receiverLate)
                {
                    receiver.Poll();
                    receiver.Flush();
                }

                h.Network.Advance(400);
            }
        }

        void Settle(string when)
        {
            Assert.True(h.RunUntil(() => tracked.All(t => (t.FromServer ? h.Server! : client).GetDeliveryStatus(t.Token) != DeliveryStatus.Pending), 30_000_000),
                $"{when}: {tracked.Count(t => (t.FromServer ? h.Server! : client).GetDeliveryStatus(t.Token) == DeliveryStatus.Pending)} messages still pending; "
                + $"client {client.State}, server {h.Server!.State}; from server {tracked.Count(t => t.FromServer)}, from client {tracked.Count(t => !t.FromServer)}");
        }

        // Rounds of late receivers in both directions.
        for (int round = 0; round < 8; round++)
        {
            bool serverSends = (round & 1) == 0;
            Burst(serverSends ? h.Server! : client, serverSends, receiverLate: true, steps: 300);
            Burst(serverSends ? h.Server! : client, serverSends, receiverLate: false, steps: 20);
        }

        Settle("after the late rounds");
        AssertExactlyOnce(tracked, t => t.FromServer ? h.Server! : client, atClient, atServer, "after the late rounds");
        Assert.True(h.RunUntil(() => latestSent.All(pair => latestAtClient.GetValueOrDefault(pair.Key) == pair.Value), 20_000_000),
            "a latest key did not reach its last value");

        // A reconnect in the middle of a burst the client holds unread.
        QuiclyPeer oldServer = h.Server!;
        tracked.Clear();
        Burst(oldServer, fromServer: true, receiverLate: true, steps: 300);
        Assert.True(GroupKit.OpenPeerGroups(client, Groups) > 8, "the reconnect should find the client holding more than MaxGroups streams");
        oldServer.Core.Transport!.Close((ulong)QuiclyErrorCode.NoError, default);
        for (int step = 0; step < 300 && !client.Core.IsTransportClosed; step++)
        {
            h.Network.Advance(1_000);
        }

        Assert.True(client.Core.IsTransportClosed);
        client.Reconnect(new SimulatedConnector(h.Network, new LinkOptions { PeerUnidiStreams = 1024, DelayMicros = 1_000, StreamLossPercent = 2 }),
            h.Listener.LocalEndPoint, "test", default);
        Assert.True(h.RunUntil(() => client.State == PeerState.Connected && h.Server is not null && !ReferenceEquals(h.Server, oldServer)
            && h.Server.State == PeerState.Connected, 10_000_000), "the reconnect did not complete");
        oldServer.Dispose();
        Assert.True(h.RunUntil(() => Rented(shared) == baseline, 5_000_000), $"{Rented(shared) - baseline} blocks of the lost connection are still rented");
        Assert.Equal(0, DatagramKit.Statistics(client).ReceiveBytesOutstanding);
        Assert.Equal(0, GroupKit.OpenPeerGroups(client, Groups) + GroupKit.OpenPeerGroups(client, Few));

        // What the late client held unread when its connection was lost is gone with it (a reconnect is not a resend,
        // docs/TROUBLESHOOTING.md); nothing of it arrived twice.
        Assert.Equal(0, atClient.Values.Count(count => count > 1));

        // The resumed session carries late bursts in both directions again.
        RegisterServer(h.Server!);
        tracked.Clear();
        atClient.Clear();
        atServer.Clear();
        orderedAtClient.Clear();
        orderedAtServer.Clear();
        for (int round = 0; round < 4; round++)
        {
            bool serverSends = (round & 1) == 0;
            Burst(serverSends ? h.Server! : client, serverSends, receiverLate: true, steps: 300);
        }

        Settle("after the reconnect");
        AssertExactlyOnce(tracked, t => t.FromServer ? h.Server! : client, atClient, atServer, "after the reconnect");
        Assert.True(IsIncreasing(orderedAtClient) && IsIncreasing(orderedAtServer), "ordered messages arrived out of order");

        // The server is disposed in the middle of a burst the client holds.
        Burst(h.Server!, fromServer: true, receiverLate: true, steps: 200);
        Assert.True(Rented(shared) > baseline || GroupKit.OpenPeerGroups(client, Groups) > 0);
        h.DisposeServer();
        Assert.True(h.RunUntil(() => client.Core.IsTransportClosed, 20_000_000), "the client did not see the server go");
        Assert.True(h.RunUntil(() => Rented(shared) == baseline, 5_000_000), $"{Rented(shared) - baseline} blocks are still rented after the server went away");
        PeerStatistics statistics = DatagramKit.Statistics(client);
        Assert.Equal(0, statistics.CallbackFaults);
        Assert.Equal(0, statistics.StreamsReset);
        Assert.Equal(0, statistics.ReceiveBytesOutstanding);
    }

    /// <summary>
    /// <c>OpenStream</c> answering <see cref="TransportStatus.OutOfMemory"/> is transient by contract: the MsQuic transport
    /// answers it once this end's own streams hold their share of the table (8702de9: "a quarter of the table is this
    /// end's own"; slots of closed streams come back "a moment later", from the cleanup work item), and it has nothing to
    /// do with the peer's stream credit. The group, ordered and bulk engines retry at their next pass. The ReliableLatest
    /// engine (TrySendLarge) treats any failed open like a refusal by the peer's stream limit: it sets the channel
    /// blocked and waits for <see cref="PeerCore.StreamCreditGeneration"/> to change, which only a STREAMS_AVAILABLE from
    /// the peer does. A connection with no other stream traffic never gets one, so one momentarily full table holds the
    /// channel's large values until the 30-second version budget fails them.
    /// </summary>
    [Fact]
    public void A_Large_Latest_Value_Whose_Stream_Open_Found_The_Table_Full_Once_Goes_Out_When_It_Has_Room()
    {
        OpenFailureConnector? wrapper = null;
        using SessionHarness h = new(table: Mixed, client: GroupKit.Prompt, server: DatagramKit.Quiet,
            connector: inner => wrapper = new OpenFailureConnector(inner));
        QuiclyPeer client = h.Client;
        QuiclyPeer server = h.Server!;
        int? arrived = null;
        server.RegisterHandler(Latest, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => arrived = BitConverter.ToInt32(payload));

        // The table is full for this one open (this end's streams hold its share, or their slots are still being cleaned
        // up); it has room again straight after.
        wrapper!.Transport!.FailLatestOpens = 1;
        SendResult result = client.SendCopy(new SendHeader(Latest, 3), DatagramKit.Payload(42, 8_000), SendOptions.Tracked);
        Assert.True(result.IsAdmitted);
        bool delivered = h.RunUntil(() => arrived == 42, 2_000_000);
        long waited = 0;
        if (!delivered)
        {
            long start = h.Network.NowMicros;
            h.RunUntil(() => arrived == 42 || client.GetDeliveryStatus(result.Token) != DeliveryStatus.Pending, 40_000_000);
            waited = (h.Network.NowMicros - start) / 1_000;
        }

        Assert.Equal(1, wrapper.Transport.FailedOpens);
        Assert.True(delivered,
            $"two seconds after one open found the table full the value had not arrived; it then ended {client.GetDeliveryStatus(result.Token)} "
            + $"{waited} ms later, arrived: {arrived is not null}");
    }

    private static void AssertExactlyOnce(List<(int Id, SendToken Token, bool FromServer)> tracked, Func<(int Id, SendToken Token, bool FromServer), QuiclyPeer> sender,
        Dictionary<int, int> atClient, Dictionary<int, int> atServer, string when)
    {
        int lost = 0;
        int failed = 0;
        foreach ((int id, SendToken token, bool fromServer) in tracked)
        {
            DeliveryStatus status = sender((id, token, fromServer)).GetDeliveryStatus(token);
            Dictionary<int, int> at = fromServer ? atClient : atServer;
            if (status == DeliveryStatus.Delivered && at.GetValueOrDefault(id) != 1)
            {
                lost++;
            }

            if (status is DeliveryStatus.Failed)
            {
                failed++;
            }
        }

        int duplicates = atClient.Values.Count(count => count > 1) + atServer.Values.Count(count => count > 1);
        Assert.True(lost == 0 && duplicates == 0 && failed == 0,
            $"{when}: {lost} Delivered messages not received exactly once, {duplicates} duplicates, {failed} Failed of {tracked.Count}");
    }

    private static bool IsIncreasing(List<int> ids)
    {
        for (int i = 1; i < ids.Count; i++)
        {
            if (ids[i] <= ids[i - 1])
            {
                return false;
            }
        }

        return true;
    }

    private static int Rented(SlabAllocator allocator)
    {
        int rented = 0;
        for (int i = 0; i < allocator.ClassCount; i++)
        {
            rented += allocator.GetClassStatistics(i).Rented;
        }

        return rented;
    }

    /// <summary>Answers <see cref="TransportStatus.OutOfMemory"/> to the next ReliableLatest stream opens (a full stream table).</summary>
    private sealed unsafe class OpenFailureTransport(ITransport inner) : ITransport
    {
        public int FailLatestOpens { get; set; }

        public int FailedOpens { get; private set; }

        public TransportCapabilities Capabilities => inner.Capabilities;

        public TransportState State => inner.State;

        public TransportStatus SendDatagram(TransportSegment* segments, int count, ulong context, TransportSendFlags flags) =>
            inner.SendDatagram(segments, count, context, flags);

        public TransportStatus OpenStream(StreamKind kind, ulong context, ushort priority, out TransportStreamId id)
        {
            if (FailLatestOpens > 0 && PeerCore.TryDecodeEngineStreamContext(context, out ChannelMode mode, out _, out _) && mode == ChannelMode.ReliableLatest)
            {
                FailLatestOpens--;
                FailedOpens++;
                id = TransportStreamId.None;
                return TransportStatus.OutOfMemory;
            }

            return inner.OpenStream(kind, context, priority, out id);
        }

        public TransportStatus StartStream(TransportStreamId id) => inner.StartStream(id);

        public TransportStatus SendStream(TransportStreamId id, TransportSegment* segments, int count, ulong context, TransportSendFlags flags) =>
            inner.SendStream(id, segments, count, context, flags);

        public void AbortStream(TransportStreamId id, ulong errorCode, StreamAbortDirection direction) => inner.AbortStream(id, errorCode, direction);

        public void SetStreamPriority(TransportStreamId id, ushort priority) => inner.SetStreamPriority(id, priority);

        public long GetQuicStreamId(TransportStreamId id) => inner.GetQuicStreamId(id);

        public void ResumeStreamReceive(TransportStreamId id, int bytesConsumed) => inner.ResumeStreamReceive(id, bytesConsumed);

        public void CloseStream(TransportStreamId id) => inner.CloseStream(id);

        public void UpdatePeerStreamLimits(ushort bidirectional, ushort unidirectional) => inner.UpdatePeerStreamLimits(bidirectional, unidirectional);

        public void Close(ulong errorCode, ReadOnlySpan<byte> reason) => inner.Close(errorCode, reason);

        public void GetStatistics(out TransportStatistics statistics) => inner.GetStatistics(out statistics);

        public void Dispose() => inner.Dispose();
    }

    private sealed class OpenFailureConnector(ITransportConnector inner) : ITransportConnector
    {
        public OpenFailureTransport? Transport { get; private set; }

        public ITransport Connect(System.Net.EndPoint endpoint, string? serverName, ITransportSink sink) =>
            Transport = new OpenFailureTransport(inner.Connect(endpoint, serverName, sink));
    }
}
