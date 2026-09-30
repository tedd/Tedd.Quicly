using System.Diagnostics;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Transport.MsQuic.Tests.Transport;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Review;

/// <summary>
/// Adversarial review of the stack on d567ba5, lens: the receive credit, over real MsQuic loopback with the library's
/// default transport options (a server sending to a client, whose MsQuic grants 1 024 unidirectional streams).
/// </summary>
[Collection(MsQuicCollection.Name)]
public class ReviewStackcreditMsQuicTests
{
    /// <summary>2 unordered datagrams · 11 group streams (the one nobody reads) · 12 ordered (read).</summary>
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "a", ChannelMode.UnreliableUnordered)
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

    private sealed class Pair : IDisposable
    {
        private readonly MsQuicTransportHarness _harness = new();
        private QuiclyPeer? _server;

        public Pair()
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

        public void Dispose()
        {
            Client.Dispose();
            Volatile.Read(ref _server)?.Dispose();
            _harness.Dispose();
        }
    }

    private static ChannelStatistics Channel(QuiclyPeer peer, ushort channel)
    {
        Assert.True(peer.GetChannelStatistics(channel, out ChannelStatistics statistics));
        return statistics;
    }

    /// <summary>
    /// FINDING (docs; release notes "Behaviour changes": "An unread reliable channel now stalls its own sender instead of
    /// the whole peer ... sees its sends queue up and then <c>QueueFull</c>"; Known limits: "... then no stream channel of
    /// the connection receives until the application reads. Datagram channels are not affected.").
    /// <para>
    /// The receive credit bounds what waits in the session, not what waits in MsQuic. A server sends messages of 32 KiB on
    /// a group channel the client does not read (the client grants 1 024 unidirectional streams in its handshake, and the
    /// receiver now accepts every one of them). Each held group keeps its bytes in the client's MsQuic, which does not give
    /// the connection's flow-control credit back; at 16 MiB the connection's window is used up. From then on the server's
    /// sends on a channel the client <em>does</em> read neither arrive nor — once the server's shared send budget has
    /// filled with what cannot leave — are admitted, and that includes the datagram channel: the whole sending peer is
    /// stopped, not the unread channel's sender alone. The test asserts the docs' claims: a message on the read ordered
    /// channel arrives, and a datagram send is admitted and arrives.
    /// </para>
    /// </summary>
    [Fact]
    public void An_Unread_Group_Channel_Stops_Only_Its_Own_Sender_And_Not_The_Datagrams()
    {
        if (!MsQuicApi.TryGetInstance(out _, out _))
        {
            Assert.Skip("MsQuic is not available on this host.");
        }

        using Pair pair = new();
        QuiclyPeer client = pair.Client;
        List<int> read = [];
        int datagrams = 0;
        client.RegisterHandler(12, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => read.Add(BitConverter.ToInt32(payload)));
        client.RegisterHandler(2, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => datagrams++);

        // The ordered stream is open before the flood.
        Assert.True(pair.Server.SendCopy(new SendHeader(12), BitConverter.GetBytes(0)).IsAdmitted);
        Assert.True(pair.Pump(() => read.Count == 1, 5_000));

        // 32 KiB group messages nobody reads, until the sender has admitted nothing for two seconds.
        byte[] big = new byte[32 * 1024];
        int admitted = 0;
        Stopwatch sinceAdmitted = Stopwatch.StartNew();
        SendStatus last = SendStatus.Admitted;
        while (sinceAdmitted.ElapsedMilliseconds < 2_000 && admitted < 2_000)
        {
            SendResult result = pair.Server.SendCopy(new SendHeader(11), big);
            last = result.Status;
            if (result.IsAdmitted)
            {
                admitted++;
                sinceAdmitted.Restart();
                if ((admitted & 7) == 0)
                {
                    pair.Pump(static () => true, 0);
                }

                continue;
            }

            pair.Pump(static () => true, 0);
            Thread.Sleep(1);
        }

        long received = Channel(client, 11).Received;
        long holds = Channel(client, 11).BacklogHolds;

        // The channels the client reads.
        SendStatus ordered = pair.Server.SendCopy(new SendHeader(12), BitConverter.GetBytes(1)).Status;
        SendStatus datagram = pair.Server.SendCopy(new SendHeader(2), [1, 2, 3, 4]).Status;
        bool arrived = pair.Pump(() => read.Count == 2 && datagrams > 0, 5_000);
        Assert.True(arrived && ordered == SendStatus.Admitted && datagram == SendStatus.Admitted,
            $"after {admitted} group messages of 32 KiB ({admitted * 32L / 1024} MiB) into the unread channel (its last send answered {last}; the client "
            + $"received {received} and held its streams back {holds} times), a send on the read ordered channel answered {ordered} and one on the "
            + $"datagram channel {datagram}; in five seconds {read.Count - 1} of 1 ordered message and {datagrams} datagrams arrived.");
    }
}
