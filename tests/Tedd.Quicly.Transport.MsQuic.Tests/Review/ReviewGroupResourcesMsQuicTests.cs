using System.Diagnostics;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Transport.MsQuic.Tests.Transport;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Review;

/// <summary>
/// Adversarial review of fix/group-stream-receive-limit, resources lens, over real MsQuic loopback with the transport's
/// <em>default</em> options. The fix sizes the ReliableUnordered receive records from
/// <c>PeerCore.PeerUnidirectionalStreamLimit</c> and relies on the transport never admitting more peer streams than that.
/// An MsQuic client grants the server <c>ClientPeerUnidiStreamCount</c> = 1 024 unidirectional streams in its transport
/// parameters, before the session exists, and QUIC never takes credit back — so a client that falls behind its server is
/// sent more group streams than it has records for, and the "backstop" resets groups the server already completed Delivered.
/// </summary>
[Collection(MsQuicCollection.Name)]
public class ReviewGroupResourcesMsQuicTests
{
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "a", ChannelMode.UnreliableUnordered)
        .Add(10, "stream", ChannelMode.ReliableOrdered)
        .Add(11, "group", ChannelMode.ReliableUnordered)
        .Build();

    private sealed class AcceptAll : IPeerAdmission
    {
        public AdmissionResult Admit(in HelloInfo hello, QuiclyPeer peer) => AdmissionResult.Accept();
    }

    [Fact]
    public void A_Client_That_Falls_Behind_Its_Server_Loses_No_Group_Over_MsQuic()
    {
        (int received, long reset, int delivered, int failed, int sent) = RunHitch(clientIsReceiver: true);
        Assert.True(received == sent && reset == 0,
            $"server -> client: received {received} of {sent}; the client reset {reset} streams; the server reported {delivered} Delivered, {failed} Failed");
    }

    /// <summary>The direction the branch's own regression test covers (client → server), as a control: it passes.</summary>
    [Fact]
    public void A_Server_That_Falls_Behind_Its_Client_Loses_No_Group_Over_MsQuic()
    {
        (int received, long reset, int delivered, int failed, int sent) = RunHitch(clientIsReceiver: false);
        Assert.True(received == sent && reset == 0,
            $"client -> server: received {received} of {sent}; the server reset {reset} streams; the client reported {delivered} Delivered, {failed} Failed");
    }

    private static (int Received, long Reset, int Delivered, int Failed, int Sent) RunHitch(bool clientIsReceiver)
    {
        using MsQuicTransportHarness harness = new();
        QuiclyPeer? server = null;
        AcceptAll admission = new();
        PeerOptions serverOptions = Options(receiver: !clientIsReceiver);
        MsQuicTransportListener listener = harness.StartListener(
            new MsQuicTransportOptions(),
            static (in NewConnectionInfo _) => PreHandshakeDecision.Accept,
            (ITransport transport, in NewConnectionInfo info) =>
            {
                harness.Track((MsQuicTransport)transport);
                QuiclyPeer peer = QuiclyPeer.CreateServerPeer(transport, in info, Table, serverOptions, admission);
                Volatile.Write(ref server, peer);
                return peer.TransportSink;
            });

        // The client's transport options are the library defaults apart from the certificate pin.
        MsQuicTransportConnector connector = harness.CreateConnector(new MsQuicTransportOptions
        {
            ServerCertificateValidation = ServerCertificateValidationMode.PinnedSpki,
            PinnedSpkiSha256 = [harness.Pin],
        });
        QuiclyPeer client = QuiclyPeer.Connect(connector, listener.LocalEndPoint, "localhost", Table, Options(receiver: clientIsReceiver));
        try
        {
            Assert.True(Pump(() => client.State == PeerState.Connected && Volatile.Read(ref server)?.State == PeerState.Connected, client, () => Volatile.Read(ref server)),
                "the session did not connect");
            QuiclyPeer sender = clientIsReceiver ? server! : client;
            QuiclyPeer receiver = clientIsReceiver ? client : server!;
            int received = 0;
            receiver.RegisterHandler(11, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => received++);

            // A hitch at the receiver: it is not polled while the sender sends forty groups of sixteen messages, a few
            // milliseconds apart. The sender keeps MaxGroups (8) by its own count the whole time.
            const int Sent = 640;
            List<SendToken> tokens = [];
            byte[] payload = new byte[8];
            for (int i = 0; i < Sent; i++)
            {
                BitConverter.TryWriteBytes(payload, i);
                SendResult result = sender.SendCopy(new SendHeader(11), payload, SendOptions.Tracked);
                Assert.True(result.IsAdmitted);
                tokens.Add(result.Token);
                if ((i & 15) == 15)
                {
                    for (int step = 0; step < 5; step++)
                    {
                        sender.Poll();
                        sender.Flush();
                        Thread.Sleep(1);
                    }
                }
            }

            // Let the sender finish what it can while the receiver is still behind, then the receiver catches up.
            Pump(() => tokens.TrueForAll(token => sender.GetDeliveryStatus(token) != DeliveryStatus.Pending), sender, () => null, TimeSpan.FromSeconds(2));
            Pump(() => received == Sent, client, () => server, TimeSpan.FromSeconds(8));
            receiver.GetStatistics(out PeerStatistics statistics);
            return (received, statistics.StreamsReset,
                tokens.Count(token => sender.GetDeliveryStatus(token) == DeliveryStatus.Delivered),
                tokens.Count(token => sender.GetDeliveryStatus(token) == DeliveryStatus.Failed), Sent);
        }
        finally
        {
            client.Dispose();
            Volatile.Read(ref server)?.Dispose();
        }
    }

    private static PeerOptions Options(bool receiver)
    {
        PeerOptions options = new() { GroupMinInterval = TimeSpan.Zero };
        if (receiver)
        {
            // A small ring, as in the branch's own regression test: a late Poll fills it and the streams are held back.
            options.ReceiveRingCapacity = 64;
        }

        return options;
    }

    private static bool Pump(Func<bool> condition, QuiclyPeer client, Func<QuiclyPeer?> server, TimeSpan? timeout = null)
    {
        Stopwatch watch = Stopwatch.StartNew();
        TimeSpan limit = timeout ?? TimeSpan.FromSeconds(15);
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

            if (watch.Elapsed > limit)
            {
                return false;
            }

            Thread.Sleep(1);
        }
    }
}
