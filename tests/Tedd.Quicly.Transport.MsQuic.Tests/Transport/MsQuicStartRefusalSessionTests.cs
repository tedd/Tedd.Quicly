using System.Diagnostics;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Transport;

/// <summary>
/// The session layer over a transport whose refusals always take the combined path of
/// <see cref="MsQuicTransport.DelaySendUntilStartReported"/>: the send that starts a stream returns
/// <see cref="TransportStatus.StreamLimitReached"/> after the refusal callbacks for that stream were delivered. A receiver
/// with a channel nobody reads holds the streams of the groups it was sent open (the receive credit keeps their data in the
/// transport), so MsQuic returns no stream credit and the sender's next group is refused again and again. The engine must
/// abandon the stream once and park until credit returns, exactly as it does when the refusal is asynchronous — not retry
/// on every pass, as it does for a failure that no stream limit caused.
/// </summary>
[Collection(MsQuicCollection.Name)]
public class MsQuicStartRefusalSessionTests
{
    /// <summary>11 group streams (nobody reads it until the end) · 12 ordered.</summary>
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "a", ChannelMode.UnreliableUnordered)
        .Add(11, "group", ChannelMode.ReliableUnordered)
        .Add(12, "ordered", ChannelMode.ReliableOrdered)
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

    [Fact]
    public void A_Sender_Whose_Refused_Starts_Return_StreamLimitReached_Parks_On_Credit_And_Loses_Nothing()
    {
        if (!MsQuicApi.TryGetInstance(out _, out _))
        {
            Assert.Skip("MsQuic is not available on this host.");
        }

        using MsQuicTransportHarness harness = new();
        MsQuicTransport? senderTransport = null;
        QuiclyPeer? sender = null;
        AcceptAll admission = new();
        PeerOptions senderOptions = new() { GroupMinInterval = TimeSpan.Zero };
        MsQuicTransportListener listener = harness.StartListener(new MsQuicTransportOptions(), static (in NewConnectionInfo _) => PreHandshakeDecision.Accept,
            (ITransport transport, in NewConnectionInfo info) =>
            {
                MsQuicTransport tracked = harness.Track((MsQuicTransport)transport);
                tracked.DelaySendUntilStartReported = true;
                Volatile.Write(ref senderTransport, tracked);
                QuiclyPeer peer = QuiclyPeer.CreateServerPeer(transport, in info, Table, senderOptions, admission);
                Volatile.Write(ref sender, peer);
                return peer.TransportSink;
            });
        MsQuicTransportOptions clientTransport = new()
        {
            ServerCertificateValidation = ServerCertificateValidationMode.PinnedSpki,
            PinnedSpkiSha256 = [harness.Pin],
        };
        using QuiclyPeer receiver = QuiclyPeer.Connect(new TrackingConnector(harness, harness.CreateConnector(clientTransport)), listener.LocalEndPoint, "localhost", Table, new PeerOptions());

        bool Pump(Func<bool> condition, int milliseconds)
        {
            Stopwatch watch = Stopwatch.StartNew();
            while (true)
            {
                receiver.Poll();
                receiver.Flush();
                if (Volatile.Read(ref sender) is { } peer)
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

        Assert.True(Pump(() => receiver.State == PeerState.Connected && Volatile.Read(ref sender)?.State == PeerState.Connected, 10_000), "the handshake did not complete");
        QuiclyPeer server = Volatile.Read(ref sender)!;
        MsQuicTransport transport = Volatile.Read(ref senderTransport)!;
        List<int> ordered = [];
        receiver.RegisterHandler(12, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => ordered.Add(BitConverter.ToInt32(payload)));

        // Groups of two messages on a channel nobody reads, until the receiver holds every stream it may have open and the
        // sender's next group is refused; its queue then fills and the loop ends.
        List<SendToken> tokens = [];
        byte[] payload = new byte[4];
        Stopwatch sinceAdmitted = Stopwatch.StartNew();
        while (tokens.Count < 6_000 && sinceAdmitted.ElapsedMilliseconds < 500)
        {
            BitConverter.TryWriteBytes(payload, tokens.Count);
            SendResult result = server.SendCopy(new SendHeader(11), payload, SendOptions.Tracked);
            if (result.IsAdmitted)
            {
                tokens.Add(result.Token);
                sinceAdmitted.Restart();
                if (tokens.Count % 2 != 0)
                {
                    continue;
                }
            }

            Pump(static () => true, 0);
            Thread.Sleep(1);
        }

        Assert.True(transport.StartRefusalRaces > 0, $"no start was refused by the peer's stream limit ({tokens.Count} messages admitted, {transport.StartsWithSend} starts): the test did not reach the path it is about");

        // The sender has groups waiting and the receiver holds every stream: the engine parks on credit. A retry on every
        // pass would make a start attempt of every pass (the pump runs one per millisecond).
        long attempts = transport.StartsWithSend;
        Pump(static () => false, 500);
        long retried = transport.StartsWithSend - attempts;
        Assert.True(retried <= 4, $"the sender made {retried} start attempts in 500 ms while the receiver held every stream: it did not park on credit");

        // The receiver reads the channel: its streams finish, MsQuic returns the credit, and every message arrives.
        List<int> got = [];
        ReceivedMessage[] buffer = new ReceivedMessage[256];
        Assert.True(Pump(() =>
        {
            int taken;
            while ((taken = receiver.Drain(11, buffer)) > 0)
            {
                for (int i = 0; i < taken; i++)
                {
                    got.Add(BitConverter.ToInt32(buffer[i].Payload));
                }

                receiver.Release(buffer.AsSpan(0, taken));
            }

            return got.Count == tokens.Count;
        }, 60_000), $"{got.Count} of {tokens.Count} group messages came out");
        Assert.Equal(Enumerable.Range(0, tokens.Count), got.Order());

        // And the session is still sound: a message on the ordered channel goes through, nothing was reset or faulted.
        Assert.True(server.SendCopy(new SendHeader(12), [7, 0, 0, 0]).IsAdmitted);
        Assert.True(Pump(() => ordered.Count == 1, 10_000), "the ordered channel did not deliver after the group channel was read");
        receiver.GetStatistics(out PeerStatistics receiverStatistics);
        server.GetStatistics(out PeerStatistics senderStatistics);
        Assert.Equal(0, receiverStatistics.StreamsReset);
        Assert.Equal(0, receiverStatistics.CallbackFaults);
        Assert.Equal(0, senderStatistics.CallbackFaults);
        Assert.True(Pump(() => tokens.All(token => server.GetDeliveryStatus(token) == DeliveryStatus.Delivered), 20_000), "not every group message was Delivered");
    }
}
