using System.Diagnostics;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Transport.MsQuic.Tests.Transport;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Review;

/// <summary>
/// Adversarial review of fix/group-stream-receive-limit (lens: protocol, security and limits), over real MsQuic loopback
/// with the library's default transport options.
/// </summary>
[Collection(MsQuicCollection.Name)]
public class ReviewGroupLimitsMsQuicTests
{
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "a", ChannelMode.UnreliableUnordered)
        .Add(10, "stream", ChannelMode.ReliableOrdered)
        .Add(11, "group", ChannelMode.ReliableUnordered)
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

    /// <summary>
    /// The branch's own regression scenario (320 tracked messages, one group of sixteen per flush, the receiver late with
    /// its Poll), with the server as the sender and the client as the receiver, over MsQuic with default
    /// <see cref="MsQuicTransportOptions"/>. The client's transport grants the server 1 024 unidirectional streams in the
    /// handshake (<see cref="MsQuicTransportOptions.ClientPeerUnidiStreamCount"/>); the session's later
    /// <c>UpdatePeerStreamLimits(0, 9)</c> cannot take that credit back. The group engine has 9 receive records, so the
    /// server's tenth concurrent stream is reset <c>LimitExceeded</c> after the server completed its messages Delivered.
    /// </summary>
    [Fact]
    public void A_Late_Client_Loses_No_Group_Of_A_Server_That_Kept_MaxGroups()
    {
        if (!MsQuicApi.TryGetInstance(out _, out _))
        {
            Assert.Skip("MsQuic is not available on this host.");
        }

        var harness = new MsQuicTransportHarness();
        QuiclyPeer? server = null;
        QuiclyPeer? client = null;
        try
        {
            PeerOptions serverOptions = new() { GroupMinInterval = TimeSpan.Zero };
            PeerOptions clientOptions = new() { ReceiveRingCapacity = 64 };
            AcceptAll admission = new();
            MsQuicTransportListener listener = harness.StartListener(new MsQuicTransportOptions(), static (in NewConnectionInfo _) => PreHandshakeDecision.Accept,
                (ITransport transport, in NewConnectionInfo info) =>
                {
                    harness.Track((MsQuicTransport)transport);
                    QuiclyPeer peer = QuiclyPeer.CreateServerPeer(transport, in info, Table, serverOptions, admission);
                    Volatile.Write(ref server, peer);
                    return peer.TransportSink;
                });

            // The library's defaults as they were when this was written (a grant of 1 024; the default is 0 since the third
            // review round, GR3-1), plus the pin of the test certificate.
            MsQuicTransportOptions clientTransport = new()
            {
                ServerCertificateValidation = ServerCertificateValidationMode.PinnedSpki,
                PinnedSpkiSha256 = [harness.Pin],
                ClientPeerUnidiStreamCount = 1024,
            };
            Assert.Equal(1024, clientTransport.ClientPeerUnidiStreamCount);
            client = QuiclyPeer.Connect(new TrackingConnector(harness, harness.CreateConnector(clientTransport)), listener.LocalEndPoint, "localhost", Table, clientOptions);

            Assert.True(Pump(() => client.State == PeerState.Connected && Volatile.Read(ref server)?.State == PeerState.Connected, 10_000, client, () => Volatile.Read(ref server)),
                "the handshake did not complete");
            QuiclyPeer sender = server!;
            List<int> got = [];
            client.RegisterHandler(11, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => got.Add(BitConverter.ToInt32(payload)));

            // The client is not polled while the server sends twenty groups of sixteen messages, a few milliseconds apart.
            List<SendToken> tokens = [];
            byte[] payload = new byte[4];
            for (int i = 0; i < 320; i++)
            {
                BitConverter.TryWriteBytes(payload, i);
                SendResult result = sender.SendCopy(new SendHeader(11), payload, SendOptions.Tracked);
                Assert.True(result.IsAdmitted);
                tokens.Add(result.Token);
                if ((i & 15) == 15)
                {
                    for (int step = 0; step < 4; step++)
                    {
                        sender.Poll();
                        sender.Flush();
                        Thread.Sleep(2);
                    }
                }
            }

            bool all = Pump(() => got.Count == 320, 5_000, client, () => sender);
            client.GetStatistics(out PeerStatistics statistics);
            int delivered = tokens.Count(token => sender.GetDeliveryStatus(token) == DeliveryStatus.Delivered);
            int failed = tokens.Count(token => sender.GetDeliveryStatus(token) == DeliveryStatus.Failed);
            Assert.True(all,
                $"received {got.Count} of 320 messages; the client reset {statistics.StreamsReset} streams; the server reported {delivered} Delivered and {failed} Failed");
            Assert.Equal(0, statistics.StreamsReset);
        }
        finally
        {
            client?.Dispose();
            server?.Dispose();
            harness.Dispose();
        }
    }

    private static bool Pump(Func<bool> condition, int milliseconds, QuiclyPeer client, Func<QuiclyPeer?> server)
    {
        Stopwatch watch = Stopwatch.StartNew();
        while (true)
        {
            client.Poll();
            client.Flush();
            if (server() is { } peer)
            {
                peer.Poll();
                peer.Flush();
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
}
