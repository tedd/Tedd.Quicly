using System.Diagnostics;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Transport;

/// <summary>
/// The receive credit of the reliable stream channels over real MsQuic loopback with the library's default transport
/// options (the simulator's side of it is <c>ReliableCreditTests</c> in the Core tests): a channel nobody reads holds
/// back only its own streams, nothing of it is lost, and the streams it held back — resumed with zero bytes, from the
/// game thread, again and again — all go on when the application reads it.
/// </summary>
[Collection(MsQuicCollection.Name)]
public class ReliableCreditMsQuicTests
{
    /// <summary>2 unordered datagrams · 10 ordered (the one nobody reads) · 11 group streams · 12 ordered.</summary>
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "a", ChannelMode.UnreliableUnordered)
        .Add(10, "unread", ChannelMode.ReliableOrdered)
        .Add(11, "group", ChannelMode.ReliableUnordered)
        .Add(12, "read", ChannelMode.ReliableOrdered)
        .Build();

    private sealed class TrackingConnector(MsQuicTransportHarness harness, ITransportConnector inner) : ITransportConnector
    {
        public ITransport Connect(System.Net.EndPoint endpoint, string? serverName, ITransportSink sink) =>
            harness.Track((MsQuicTransport)inner.Connect(endpoint, serverName, sink));
    }

    private sealed class AcceptAll : IPeerAdmission
    {
        public AdmissionResult Admit(in HelloInfo hello, QuiclyPeer peer) => AdmissionResult.Accept();
    }

    /// <summary>A server and a client peer over MsQuic loopback, both pumped by the test thread.</summary>
    private sealed class Pair : IDisposable
    {
        private readonly MsQuicTransportHarness _harness = new();
        private QuiclyPeer? _server;

        public Pair(ushort clientPeerUnidiStreams = 0)
        {
            PeerOptions serverOptions = new() { GroupMinInterval = TimeSpan.Zero };
            AcceptAll admission = new();
            MsQuicTransportListener listener = _harness.StartListener(new MsQuicTransportOptions(), static (in NewConnectionInfo _) => PreHandshakeDecision.Accept,
                (ITransport transport, in NewConnectionInfo info) =>
                {
                    _harness.Track((MsQuicTransport)transport);
                    QuiclyPeer peer = QuiclyPeer.CreateServerPeer(transport, in info, Table, serverOptions, admission);
                    Volatile.Write(ref _server, peer);
                    return peer.TransportSink;
                });
            MsQuicTransportOptions clientTransport = new()
            {
                ServerCertificateValidation = ServerCertificateValidationMode.PinnedSpki,
                PinnedSpkiSha256 = [_harness.Pin],
                ClientPeerUnidiStreamCount = clientPeerUnidiStreams,
            };
            Client = QuiclyPeer.Connect(new TrackingConnector(_harness, _harness.CreateConnector(clientTransport)), listener.LocalEndPoint, "localhost", Table, new PeerOptions());
            Assert.True(Pump(() => Client.State == PeerState.Connected && Volatile.Read(ref _server)?.State == PeerState.Connected, 10_000), "the handshake did not complete");
        }

        public QuiclyPeer Client { get; }

        public QuiclyPeer Server => Volatile.Read(ref _server)!;

        public bool Pump(Func<bool> condition, int milliseconds)
        {
            Stopwatch watch = Stopwatch.StartNew();
            while (true)
            {
                Client.Poll();
                Client.Flush();
                if (Volatile.Read(ref _server) is { } server)
                {
                    server.Poll();
                    server.Flush();
                }

                if (condition())
                {
                    return true;
                }

                if (watch.ElapsedMilliseconds > milliseconds)
                {
                    return false;
                }

                Thread.Sleep(1);
            }
        }

        /// <summary>
        /// Sends messages numbered from <paramref name="first"/> from the server, pumping both ends (the client's handlers,
        /// not its Drain) every <paramref name="perStep"/> messages and whenever the send queue is full. Gives up when
        /// nothing was admitted for <paramref name="patience"/> milliseconds — the sender of a channel nobody reads is
        /// held by flow control — and returns the tokens of what was admitted.
        /// </summary>
        public List<SendToken> Send(ushort channel, int first, int count, int perStep = 32, int patience = 10_000)
        {
            List<SendToken> tokens = [];
            byte[] payload = new byte[4];
            Stopwatch sinceAdmitted = Stopwatch.StartNew();
            while (tokens.Count < count && sinceAdmitted.ElapsedMilliseconds < patience)
            {
                BitConverter.TryWriteBytes(payload, first + tokens.Count);
                SendResult result = Server.SendCopy(new SendHeader(channel), payload, SendOptions.Tracked);
                if (result.IsAdmitted)
                {
                    tokens.Add(result.Token);
                    sinceAdmitted.Restart();
                    if (tokens.Count % perStep == 0)
                    {
                        // A step: both ends run.
                        Pump(static () => true, 0);
                    }

                    continue;
                }

                // A full send queue: both ends run, and the transport gets a moment (a sleep is a whole timer tick here).
                Pump(static () => true, 0);
                Thread.Sleep(1);
            }

            return tokens;
        }

        public void Dispose()
        {
            Client.Dispose();
            Volatile.Read(ref _server)?.Dispose();
            _harness.Dispose();
        }
    }

    private static List<int> DrainAll(QuiclyPeer peer, ushort channel)
    {
        List<int> got = [];
        ReceivedMessage[] buffer = new ReceivedMessage[256];
        int taken;
        while ((taken = peer.Drain(channel, buffer)) > 0)
        {
            for (int i = 0; i < taken; i++)
            {
                got.Add(BitConverter.ToInt32(buffer[i].Payload));
            }

            peer.Release(buffer.AsSpan(0, taken));
        }

        return got;
    }

    private static ChannelStatistics Channel(QuiclyPeer peer, ushort channel)
    {
        Assert.True(peer.GetChannelStatistics(channel, out ChannelStatistics statistics));
        return statistics;
    }

    [Fact]
    public void An_Unread_Ordered_Channel_Holds_Back_Only_Itself()
    {
        if (!MsQuicApi.TryGetInstance(out _, out _))
        {
            Assert.Skip("MsQuic is not available on this host.");
        }

        using Pair pair = new();
        QuiclyPeer client = pair.Client;
        List<int> groups = [];
        List<int> read = [];
        int datagrams = 0;
        client.RegisterHandler(11, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => groups.Add(BitConverter.ToInt32(payload)));
        client.RegisterHandler(12, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => read.Add(BitConverter.ToInt32(payload)));
        client.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => datagrams++);

        // Messages for a channel the client has no handler for and does not drain, until flow control holds the sender:
        // far more than the channel's share.
        List<SendToken> unread = pair.Send(10, 0, 20_000, patience: 500);
        Assert.True(pair.Pump(() => Channel(client, 10).BacklogHolds > 0, 5_000), "the unread channel's stream was never held back");
        long held = Channel(client, 10).Received;
        Assert.True(held < unread.Count, $"the unread channel took all {unread.Count} messages: the test does not reach its share");

        // The other channels are not touched by it.
        Assert.Equal(2_000, pair.Send(12, 0, 2_000).Count);
        Assert.Equal(640, pair.Send(11, 0, 640).Count);
        for (int i = 0; i < 200; i++)
        {
            pair.Server.SendCopy(new SendHeader(2), [1, 2, 3, 4]);
            if ((i & 15) == 15)
            {
                pair.Pump(static () => true, 0);
            }
        }

        Assert.True(pair.Pump(() => read.Count == 2_000 && groups.Count == 640, 20_000),
            $"the other channels stalled behind the unread one: {read.Count} of 2000 ordered, {groups.Count} of 640 group messages");
        Assert.Equal(Enumerable.Range(0, 2_000), read);
        Assert.Equal(Enumerable.Range(0, 640), groups.Order());
        Assert.True(datagrams > 0, "no datagram arrived while the unread channel was held back");
        Assert.Equal(held, Channel(client, 10).Received);
        client.GetStatistics(out PeerStatistics statistics);
        Assert.Equal(0, statistics.StreamsReset);
        Assert.Equal(0, statistics.ReceiveRingDrops);

        // Nothing of the unread channel was lost: the application reads it, and every message is there, in order.
        List<int> got = [];
        Assert.True(pair.Pump(() =>
        {
            got.AddRange(DrainAll(client, 10));
            return got.Count == unread.Count;
        }, 30_000), $"{got.Count} of {unread.Count} messages came out of the channel that had been unread");
        Assert.Equal(Enumerable.Range(0, unread.Count), got);
        Assert.True(pair.Pump(() => unread.All(token => pair.Server.GetDeliveryStatus(token) == DeliveryStatus.Delivered), 10_000), "not every message was Delivered");
    }

    [Fact]
    public void An_Unread_Group_Channel_Gives_Up_Every_Stream_It_Held_Back_When_It_Is_Read()
    {
        if (!MsQuicApi.TryGetInstance(out _, out _))
        {
            Assert.Skip("MsQuic is not available on this host.");
        }

        // A client that grants its server 1 024 streams in the handshake (0.2.1's default; now an application's choice), so
        // that hundreds of groups can be held.
        using Pair pair = new(clientPeerUnidiStreams: 1024);
        QuiclyPeer client = pair.Client;
        List<int> read = [];
        client.RegisterHandler(12, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => read.Add(BitConverter.ToInt32(payload)));

        // The ordered channel's stream is open before the flood (the connection's stream slots are shared, PROTOCOL.md §7).
        Assert.Single(pair.Send(12, 0, 1));
        Assert.True(pair.Pump(() => read.Count == 1, 5_000));

        // Groups of two messages on a channel nobody reads, until the sender can open no more streams. The sender keeps
        // MaxGroups by its own count, which follows the acknowledgements, so the client ends up holding hundreds of
        // streams: each waits for the channel's credit, and each is resumed from the game thread — with nothing consumed —
        // whenever some comes back.
        List<SendToken> tokens = pair.Send(11, 0, 6_000, perStep: 2, patience: 500);
        Assert.True(pair.Pump(() => Channel(client, 11).BacklogHolds > 100, 10_000), $"only {Channel(client, 11).BacklogHolds} streams were held back");
        Assert.True(Channel(client, 11).Received < tokens.Count, $"the unread channel took all {tokens.Count} messages");

        // The ordered channel still works while they wait.
        Assert.Equal(500, pair.Send(12, 1, 500).Count);
        Assert.True(pair.Pump(() => read.Count == 501, 10_000), $"{read.Count} of 501 ordered messages arrived while the group channel was unread");

        List<int> got = [];
        Assert.True(pair.Pump(() =>
        {
            got.AddRange(DrainAll(client, 11));
            return got.Count == tokens.Count;
        }, 60_000), $"{got.Count} of {tokens.Count} group messages came out; the channel held streams back {Channel(client, 11).BacklogHolds} times");
        Assert.Equal(Enumerable.Range(0, tokens.Count), got.Order());
        client.GetStatistics(out PeerStatistics statistics);
        Assert.Equal(0, statistics.StreamsReset);
        Assert.Equal(0, statistics.CallbackFaults);
        Assert.True(pair.Pump(() => tokens.All(token => pair.Server.GetDeliveryStatus(token) == DeliveryStatus.Delivered), 10_000), "not every group message was Delivered");
    }

    [Fact]
    public void A_Channel_That_Is_Drained_Every_Frame_Is_Not_Held_Back()
    {
        if (!MsQuicApi.TryGetInstance(out _, out _))
        {
            Assert.Skip("MsQuic is not available on this host.");
        }

        using Pair pair = new();
        QuiclyPeer client = pair.Client;

        // The consumer's first frame: nothing to take, and from then on the channel is one the application reads.
        client.Poll();
        Assert.Empty(DrainAll(client, 10));
        long before = Channel(client, 10).BacklogHolds;

        List<int> got = [];
        byte[] payload = new byte[4];
        int sent = 0;
        Stopwatch watch = Stopwatch.StartNew();
        while (got.Count < 20_000 && watch.ElapsedMilliseconds < 30_000)
        {
            // At most a thousand messages on their way (64 KiB of buffer blocks): half of what a drained channel may have
            // waiting (half the receive budget, shared with the other reliable channels no handler reads, which have
            // nothing waiting), so a moment in which the transport delivers several frames at once is still not a hold.
            for (int i = 0; i < 500 && sent < 20_000 && sent - got.Count < 1_000; i++)
            {
                BitConverter.TryWriteBytes(payload, sent);
                if (!pair.Server.SendCopy(new SendHeader(10), payload).IsAdmitted)
                {
                    break;
                }

                sent++;
            }

            pair.Server.Poll();
            pair.Server.Flush();
            client.Poll();
            got.AddRange(DrainAll(client, 10));
            client.Flush();
        }

        Assert.Equal(Enumerable.Range(0, 20_000), got);

        // A frame's burst is far above the share of an unread channel (170 messages here: half of the 1 024 queue nodes,
        // among three reliable channels), and nothing was held for it.
        Assert.Equal(before, Channel(client, 10).BacklogHolds);
    }
}
