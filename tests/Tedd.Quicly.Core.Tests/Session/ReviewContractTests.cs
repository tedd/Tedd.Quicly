using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// Protocol and contract conformance findings of the wave C1 review (PROTOCOL.md is normative). Each test names the rule
/// it checks; none of them asserts an implementation detail.
/// </summary>
public class ReviewContractTests
{
    /// <summary>
    /// PROTOCOL.md §4.3: for <c>Unreliable*</c> channels <c>Delivered</c> is "transport ack when the transport reports
    /// datagram send state (caps bit1); <b>otherwise never</b>". A transport that does not report per-datagram states
    /// (<see cref="TransportCapabilities.DatagramSendState"/> false — the WebTransport/browser carriers, and MsQuic before
    /// 2.x datagram state support) only tells the session that the datagram left the host, which proves nothing about
    /// delivery, so a completion must not claim <see cref="DeliveryStatus.Delivered"/> for it.
    /// </summary>
    [Fact]
    public void An_Unreliable_Send_Is_Never_Delivered_When_The_Transport_Does_Not_Report_Datagram_States()
    {
        // The same send on a transport that does report states: Delivered is correct there (the other half of the rule).
        using (SessionHarness reporting = new(link: new LinkOptions { DelayMicros = 2_000 }, table: DatagramTables.Main,
            client: DatagramKit.Quiet, server: DatagramKit.Quiet))
        {
            Assert.True(reporting.Client.Capabilities.DatagramSendState);
            SendResult acknowledged = reporting.Client.SendCopy(new SendHeader(2), [1], SendOptions.Tracked);
            reporting.Client.Flush();
            Assert.True(reporting.RunUntil(() => reporting.Client.GetDeliveryStatus(acknowledged.Token) != DeliveryStatus.Pending));
            Assert.Equal(DeliveryStatus.Delivered, reporting.Client.GetDeliveryStatus(acknowledged.Token));
        }

        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 2_000, DatagramSendStateReporting = false },
            table: DatagramTables.Main, client: DatagramKit.Quiet, server: DatagramKit.Quiet);
        Assert.False(h.Client.Capabilities.DatagramSendState);

        SendResult sent = h.Client.SendCopy(new SendHeader(2), [1], SendOptions.Tracked);
        h.Client.Flush();
        h.Run(200_000);

        // The send is finished: its payload went back to the pool and its slot was freed (only the claim is wrong).
        Assert.Equal(0, DatagramKit.Statistics(h.Client).SendBytesOutstanding);
        Assert.Equal(0, DatagramKit.Statistics(h.Client).SendEntriesInUse);

        // The transport reported nothing but "handed to the network": the send must not be reported as delivered.
        Assert.NotEqual(DeliveryStatus.Delivered, h.Client.GetDeliveryStatus(sent.Token));
    }

    /// <summary>
    /// PROTOCOL.md §7: "stream idle mid-message | 30 s | stream reset <c>Timeout</c>". A peer that starts a message on a
    /// persistent ordered stream and then stops sending pins the staging lease of the declared frame length (and its
    /// receive-ring reservation) for the life of the connection, which starves every other channel of the per-peer
    /// receive budget. The connection-level heartbeat does not cover it: the peer stays live on another channel.
    /// </summary>
    [Fact]
    public void A_Stream_Stalled_Mid_Message_Stops_Pinning_The_Receive_Budget()
    {
        using ServerHarness h = new(table: TestTables.Default, server: o => o.ReceiveBudgetBytes = 64 * 1024);
        Assert.True(h.Admit(), "The raw client was not admitted.");
        ChannelDefinition ordered = h.Table[4]!;

        // The channel is read (a handler): a channel nobody reads would not start a message whose block is larger than
        // its byte share (the receive credit), and this test is about a message that was started.
        h.Server!.RegisterHandler(4, static (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> _) => { });

        // Channel 4 is ReliableOrdered (64 KiB MaxMessageSize): a frame header declaring 60 000 bytes, then one byte of
        // payload. The receiver stages the message in a lease of the declared length and waits for the rest.
        byte[] frame = new byte[StreamFraming.MaxFrameHeaderLength + 8];
        int written = StreamFraming.WritePreamble(frame, ordered.Id);
        StreamMessageHeader header = default;
        header.Length = 60_000;
        written += StreamFraming.WriteFrameHeader(frame.AsSpan(written), ordered, in header);
        frame[written++] = 0xAB;
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(frame.AsSpan(0, written), out TransportStreamId _));
        Assert.True(h.RunUntil(() => h.Statistics().ReceiveBytesOutstanding > 0), "The stalled message was not staged.");

        // The whole receive budget is pinned, so nothing else can be received while the stream stalls.
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(DatagramKit.Frame(h.Table, 2, 0, 0, new byte[100])));
        h.Run(50_000);
        Assert.True(DatagramKit.ChannelStats(h.Server!, 2).OutOfBuffers > 0, "The budget was expected to be exhausted.");

        // 40 s of a live connection (a Ping every second keeps the heartbeat happy) with no progress on the stream.
        for (int second = 0; second < 40; second++)
        {
            Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram(Frames.PingFrame((uint)h.Network.NowMicros, ControlCarrier.Datagram)));
            h.Run(1_000_000, step: 50_000);
        }

        Assert.Equal(PeerState.Connected, h.Server!.State);

        // PROTOCOL.md §7: the stream must have been reset (Timeout) long ago, releasing its staging lease.
        Assert.Equal(0, h.Statistics().ReceiveBytesOutstanding);
    }
}
