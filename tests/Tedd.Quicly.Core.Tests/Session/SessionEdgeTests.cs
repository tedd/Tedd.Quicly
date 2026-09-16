using System.Net;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>Less common paths of the peer core: large control frames, stream edge cases, lifetime and rate limits.</summary>
public class SessionEdgeTests
{
    private static byte[] LatestReject(ControlCarrier carrier)
    {
        byte[] buffer = new byte[64];
        LatestRejectBatchWriter writer = new(buffer, carrier);
        Assert.True(writer.TryAdd(new LatestRejectEntry(6, 1, 5, LatestRejectReason.RingFull)));
        int written = writer.Finish();
        return buffer.AsSpan(0, written).ToArray();
    }

    private static byte[] BulkProgressFrame(ControlCarrier carrier)
    {
        byte[] buffer = new byte[32];
        Assert.True(ControlCodec.TryWrite(buffer, new BulkProgress(1, 10), carrier, out int written));
        return buffer.AsSpan(0, written).ToArray();
    }

    private static byte[] BulkRequestFrame()
    {
        byte[] buffer = new byte[64];
        Assert.True(ControlCodec.TryWrite(buffer, new BulkRequest(1, 7, 1, 1, 0, 10), out int written));
        return buffer.AsSpan(0, written).ToArray();
    }

    private static byte[] BulkRejectFrame()
    {
        byte[] buffer = new byte[32];
        Assert.True(ControlCodec.TryWrite(buffer, new BulkReject(1, QuiclyErrorCode.BulkRejected), out int written));
        return buffer.AsSpan(0, written).ToArray();
    }

    [Fact]
    public void A_Large_Hello_Is_Assembled_Across_Packets()
    {
        byte[] token = new byte[3000];
        for (int i = 0; i < token.Length; i++)
        {
            token[i] = (byte)(i * 7);
        }

        using SessionHarness h = new(authToken: token);
        Assert.Equal(token, h.Admission.LastAuthToken);
    }

    [Fact]
    public void A_Large_HelloAck_Is_Assembled_Across_Packets()
    {
        byte[] token = new byte[4096];
        token[4095] = 9;
        using SessionHarness h = new(connect: false, client: o => o.RequestChannelTable = true);
        h.Admission.Handler = (in HelloInfo _, QuiclyPeer _) => AdmissionResult.Accept(token);
        Assert.True(h.RunUntilConnected());
        Assert.Equal(token, h.Client.SessionToken.ToArray());
        Assert.Equal(h.Table.Count, h.Client.RemoteChannelTable!.Count);
    }

    [Fact]
    public void A_Zero_Length_Control_Frame_Is_A_Protocol_Violation()
    {
        using ServerHarness h = new();
        h.Raw.SendControl([0x00]);
        Assert.True(h.RunUntil(() => h.Raw.IsClosed));
        Assert.Equal((ulong)QuiclyErrorCode.ProtocolViolation, h.Raw.CloseCode);
    }

    [Fact]
    public void A_Malformed_Table_Request_Is_A_Protocol_Violation()
    {
        using ServerHarness h = new();
        Assert.True(h.Admit());
        h.Raw.SendControl(Frames.RawFrame((byte)ControlType.ChannelTableRequest, [1]));
        Assert.True(h.RunUntil(() => h.Raw.IsClosed));
        Assert.Equal((ulong)QuiclyErrorCode.ProtocolViolation, h.Raw.CloseCode);
    }

    [Fact]
    public void A_Malformed_HelloAck_Is_A_Protocol_Violation_For_The_Client()
    {
        using ClientHarness h = new();
        h.SendToClient(Frames.RawFrame((byte)ControlType.HelloAck, [0, 1]));
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Closed));
        Assert.Equal(QuiclyErrorCode.ProtocolViolation, h.Client.CloseReason.Code);
    }

    [Fact]
    public void Every_Engine_Bound_Control_Type_Is_Routed()
    {
        using ServerHarness h = TestEngines.CreateServer(out Func<TestEngine> bulk, ChannelMode.Bulk, table: TestTables.AllModes);
        Assert.True(h.Admit());
        h.Raw.SendDatagram(LatestReject(ControlCarrier.Datagram));
        h.Raw.SendDatagram(BulkProgressFrame(ControlCarrier.Datagram));
        h.Raw.SendControl(LatestReject(ControlCarrier.Stream));
        h.Raw.SendControl(BulkProgressFrame(ControlCarrier.Stream));
        h.Raw.SendControl(BulkRequestFrame());
        h.Raw.SendControl(BulkRejectFrame());
        h.Run(10_000);
        Assert.Equal(4, bulk().ControlMessages);
        Assert.Equal(0, h.Statistics().MalformedDatagrams);
        Assert.Equal(PeerState.Connected, h.Server!.State);

        // Malformed bodies of engine-bound types are dropped (datagram) or close the connection (stream).
        h.Raw.SendDatagram([0x00, (byte)ControlType.BulkProgress, 0x40]);
        h.Run(5_000);
        Assert.Equal(1, h.Statistics().MalformedDatagrams);
        h.Raw.SendControl(Frames.RawFrame((byte)ControlType.KeyRetired, [0x40]));
        Assert.True(h.RunUntil(() => h.Raw.IsClosed));
        Assert.Equal((ulong)QuiclyErrorCode.ProtocolViolation, h.Raw.CloseCode);
    }

    [Fact]
    public void Aborting_The_Control_Stream_Is_A_Protocol_Violation()
    {
        using ServerHarness h = new();
        Assert.True(h.Admit());
        h.Raw.Transport.AbortStream(h.Raw.Control, 9, StreamAbortDirection.Send);
        Assert.True(h.RunUntil(() => h.Server!.State == PeerState.Closed));
        Assert.Equal(QuiclyErrorCode.ProtocolViolation, h.Server!.CloseReason.Code);
    }

    [Fact]
    public void A_Stopped_Control_Stream_Is_A_Protocol_Violation_For_The_Client()
    {
        using ClientHarness h = new();
        h.ServerTransport!.AbortStream(h.ControlStream, 9, StreamAbortDirection.Receive);
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Closed));
        Assert.Equal(QuiclyErrorCode.ProtocolViolation, h.Client.CloseReason.Code);
    }

    [Fact]
    public void Input_After_A_Violation_Is_Ignored()
    {
        using ServerHarness h = new();
        Assert.True(h.Admit());
        h.Raw.SendControl(Frames.RawFrame(0x09, [1]));
        h.Raw.OpenUni([0x04, 0x01, 0x07], out _);
        h.Raw.SendDatagram([0x02, 1]);
        Assert.True(h.RunUntil(() => h.Raw.IsClosed));
        Assert.Equal((ulong)QuiclyErrorCode.ProtocolViolation, h.Raw.CloseCode);
        Assert.Equal(0, h.Statistics().StreamsReset);
    }

    [Fact]
    public void Data_On_A_Reset_Stream_Is_Consumed_And_Dropped()
    {
        using ServerHarness h = new(link: new LinkOptions { DelayMicros = 10_000 });
        Assert.True(h.Admit());
        // Channel 5 (Bulk) is still a placeholder: its stream is reset at the preamble.
        h.Raw.OpenUni([0x05, 0x01, 0x07], out TransportStreamId id);
        h.Run(5_000);
        Assert.Equal(TransportStatus.Success, RawClient.SendStream(h.Raw.Transport, id, [0x01, 0x08], TransportSendFlags.None));
        h.Run(30_000);
        Assert.Equal(1, h.Statistics().StreamsReset);
        Assert.Equal(PeerState.Connected, h.Server!.State);
    }

    [Fact]
    public void Stream_Pings_Beyond_The_Pong_Rate_Are_Ignored()
    {
        using ServerHarness h = new(server: o => o.PongBurst = 2);
        Assert.True(h.Admit());
        byte[] ping = Frames.PingFrame(5, ControlCarrier.Stream);
        h.Raw.SendControl([.. ping, .. ping, .. ping, .. ping, .. ping]);
        h.Run(10_000);
        PeerStatistics stats = h.Statistics();
        Assert.Equal(2, stats.PongsSent);
        Assert.Equal(3, stats.PingsIgnored);
    }

    [Fact]
    public void Stream_Ping_Requests_Beyond_The_Hand_Off_Ring_Are_Ignored()
    {
        using ServerHarness h = new();
        Assert.True(h.Admit());
        byte[] ping = Frames.PingFrame(5, ControlCarrier.Stream);
        byte[] twelve = [.. ping, .. ping, .. ping, .. ping, .. ping, .. ping, .. ping, .. ping, .. ping, .. ping, .. ping, .. ping];
        h.Raw.SendControl(twelve);
        h.Run(10_000);
        PeerStatistics stats = h.Statistics();
        Assert.Equal(8, stats.PongsSent);
        Assert.Equal(4, stats.PingsIgnored);
    }

    [Fact]
    public void Bulk_Stream_Headers_Reach_The_Engine()
    {
        using ServerHarness h = TestEngines.CreateServer(out Func<TestEngine> engine, ChannelMode.Bulk, table: TestTables.AllModes);
        Assert.True(h.Admit());
        byte[] header = new byte[64];
        BulkHeader bulk = new() { TransferId = 1, ObjectId = 2, ObjectVersion = 3, TotalLength = 4, Offset = 0, Length = 4 };
        int length = StreamFraming.WriteBulkHeader(header, in bulk);
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni([0x07, .. header.AsSpan(0, length), 1, 2, 3, 4], out _, fin: true));
        Assert.True(h.RunUntil(() => engine().Ends == 1));
        Assert.Equal(1, engine().BulkHeaders);
        Assert.True(h.RunUntil(() => engine().StreamsClosed == 1));
        Assert.False(engine().LastStreamAborted);
    }

    [Fact]
    public void Engine_Exceptions_In_Stream_Callbacks_Close_With_InternalError()
    {
        using ServerHarness h = TestEngines.CreateServer(out Func<TestEngine> engine, ChannelMode.ReliableUnordered);
        Assert.True(h.Admit());
        engine().ThrowOnOpen = true;
        h.Raw.OpenUni([0x0B, 0x00, 0x01, 0x07], out _);
        Assert.True(h.RunUntil(() => h.Raw.IsClosed));
        Assert.Equal((ulong)QuiclyErrorCode.InternalError, h.Raw.CloseCode);
        Assert.IsType<InvalidOperationException>(h.Server!.LastCallbackFault);
    }

    [Fact]
    public void Engine_Exception_When_A_Stream_Ends_Closes_With_InternalError()
    {
        using ServerHarness h = TestEngines.CreateServer(out Func<TestEngine> engine, ChannelMode.ReliableUnordered);
        Assert.True(h.Admit());
        engine().ThrowOnClosed = true;
        h.Raw.OpenUni([0x0B, 0x00, 0x01, 0x07], out _, fin: true);
        Assert.True(h.RunUntil(() => h.Raw.IsClosed));
        Assert.Equal((ulong)QuiclyErrorCode.InternalError, h.Raw.CloseCode);
    }

    [Fact]
    public void Drain_Moves_Held_And_Foreign_Messages_Without_Losing_Order()
    {
        using SessionHarness h = TestEngines.Create(out _, out _, both: o => o.ReceiveRingCapacity = 2);
        QuiclyPeer server = h.Server!;
        ReceivedMessage[] buffer = new ReceivedMessage[8];
        h.Client.SendCopy(new SendHeader(2), [1]);
        h.Client.SendCopy(new SendHeader(2), [2]);
        h.Network.Advance(1_000);
        server.Poll();
        h.Client.SendCopy(new SendHeader(3), [30]);
        h.Network.Advance(1_000);
        Assert.Equal(0, server.Drain(6, buffer));
        Assert.Equal(0, server.Drain(6, buffer));
        Assert.Equal(2, server.Drain(2, buffer));
        Assert.Equal(1, buffer[0].Payload[0]);
        Assert.Equal(2, buffer[1].Payload[0]);
        server.Release(buffer.AsSpan(0, 2));
        List<(ReceiveHeader Header, byte[] Payload)> got = [];
        server.RegisterHandler(3, Handlers.Collect(got));
        Assert.Equal(1, server.Drain(3, buffer));
        Assert.Equal(30, buffer[0].Payload[0]);
        server.Release(buffer.AsSpan(0, 1));
        Assert.Equal(0, server.Poll());
        Assert.Empty(got);
        server.GetStatistics(out PeerStatistics stats);
        Assert.Equal(0, stats.ReceiveBytesOutstanding);
    }

    [Fact]
    public void Undecodable_Messages_Are_Skipped_By_Drain()
    {
        using ServerHarness h = TestEngines.CreateServer(out Func<TestEngine> engine);
        Assert.True(h.Admit());
        h.Raw.SendDatagram([0x06, 0x40, 0x64, 0xFF, 0xFF, 0xFF]);
        h.Run(5_000);
        Assert.Equal(1, engine().Received);
        Assert.Equal(0, h.Server!.Drain(6, new ReceivedMessage[2]));
        Assert.Equal(1, h.Statistics().DecodeFailures);
    }

    [Fact]
    public void Held_And_Queued_Messages_Are_Released_When_The_Session_Closes()
    {
        using SessionHarness h = TestEngines.Create(out _, out _, both: o => o.ReceiveRingCapacity = 2);
        QuiclyPeer server = h.Server!;
        h.Client.SendCopy(new SendHeader(2), [1]);
        h.Client.SendCopy(new SendHeader(2), [2]);
        h.Network.Advance(1_000);
        server.Poll();
        h.Client.SendCopy(new SendHeader(2), [3]);
        h.Network.Advance(1_000);
        server.Poll();
        server.GetStatistics(out PeerStatistics held);
        Assert.True(held.ReceiveBytesOutstanding > 0);
        server.Close();
        Assert.True(h.RunUntil(() => server.State == PeerState.Closed));
        server.GetStatistics(out PeerStatistics closed);
        Assert.Equal(0, closed.ReceiveBytesOutstanding);
    }

    [Fact]
    public void Disposing_From_The_Closed_Event_Frees_When_Poll_Returns()
    {
        using SessionHarness h = new();
        QuiclyPeer server = h.Server!;
        bool disposed = false;
        server.StateChanged += (peer, _, to) =>
        {
            if (to == PeerState.Closed)
            {
                peer.Dispose();
                disposed = true;
                Assert.False(peer.IsFreed);
            }
        };
        h.StopPumpingServer();
        h.Client.Close();
        Assert.True(h.RunUntil(() =>
        {
            if (!disposed)
            {
                server.Poll();
            }

            return disposed;
        }));
        Assert.True(server.IsFreed);
    }

    [Fact]
    public void A_Second_Bidirectional_Stream_Is_A_Protocol_Violation_For_The_Server()
    {
        FakeTransport transport = new();
        VirtualClock clock = new();
        QuiclyPeer peer = QuiclyPeer.CreateServerPeer(transport, default, TestTables.Default, new PeerOptions { Clock = clock }, new TestAdmission());
        ITransportSink sink = peer.TransportSink;
        TransportConnectedInfo info = new() { Capabilities = new TransportCapabilities { Datagrams = true } };
        sink.OnConnected(in info);
        sink.OnPeerStreamStarted(new TransportStreamId(1, 1), StreamKind.Bidirectional);
        sink.OnPeerStreamStarted(new TransportStreamId(2, 1), StreamKind.Bidirectional);
        peer.Poll();
        Assert.Equal(PeerState.Closing, peer.State);
        Assert.Equal(QuiclyErrorCode.ProtocolViolation, peer.CloseReason.Code);
        List<(ControlType Type, byte[] Body)> sent = Frames.ParseStream(transport.StreamSends[^1].Data);
        Assert.Equal(ControlType.Close, sent[0].Type);
        Assert.Null(transport.ClosedWith);
        clock.AdvanceMicros(1_100_000);
        peer.Poll();
        Assert.Equal((ulong)QuiclyErrorCode.ProtocolViolation, transport.ClosedWith);
        sink.OnClosed(TransportCloseReason.Local, 1, 0);
        peer.Poll();
        Assert.Equal(PeerState.Closed, peer.State);
        peer.Dispose();
        Assert.True(peer.IsFreed);
    }

    [Fact]
    public void Callbacks_After_The_Peer_Was_Freed_Are_Ignored()
    {
        FakeTransport transport = new();
        QuiclyPeer peer = QuiclyPeer.CreateServerPeer(transport, default, TestTables.Default, new PeerOptions { Clock = new VirtualClock() }, new TestAdmission());
        ITransportSink sink = peer.TransportSink;
        sink.OnClosed(TransportCloseReason.Transport, 0, 1);
        peer.Dispose();
        Assert.True(peer.IsFreed);
        TransportConnectedInfo info = new() { RemoteEndPoint = new IPEndPoint(IPAddress.Loopback, 9) };
        sink.OnConnected(in info);
        sink.OnPeerStreamStarted(new TransportStreamId(1, 1), StreamKind.Bidirectional);
        sink.OnStreamStarted(default, 0, TransportStatus.Success);
        sink.OnStreamSendCompleted(default, 1, false);
        sink.OnDatagramSendStateChanged(1, DatagramSendState.Acknowledged);
        sink.OnStreamAborted(default, 0, StreamAbortDirection.Both);
        sink.OnStreamShutdownComplete(default);
        sink.OnDatagramCapabilityChanged(true, 1);
        sink.OnStreamsAvailable(1, 1);
        sink.OnPeerAddressChanged(in info);
        sink.OnClosed(TransportCloseReason.Local, 0, 0);
        Assert.NotEqual(9, peer.RemoteEndPoint?.Port);
        peer.Release(default(ReceiveLease));
        peer.Release(ReadOnlySpan<ReceivedMessage>.Empty);
    }
}
