using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>Handshake over the simulated transport, server side (PROTOCOL.md §3.4, ADR 0009).</summary>
public class HandshakeTests
{
    internal static readonly (PeerState, PeerState)[] ConnectedEvents =
    [
        (PeerState.Connecting, PeerState.Handshaking),
        (PeerState.Handshaking, PeerState.Connected),
    ];

    [Fact]
    public void Accept_Connects_Both_Sides_In_Epoch_1()
    {
        byte[] token = [1, 2, 3, 4];
        using SessionHarness h = new(authToken: "secret"u8.ToArray(), connect: false);
        h.Admission.Handler = (in HelloInfo hello, QuiclyPeer peer) => AdmissionResult.Accept(token, sessionId: 77);
        Assert.True(h.RunUntilConnected());
        QuiclyPeer server = h.Server!;
        Assert.Equal(1u, h.Client.Epoch);
        Assert.Equal(1u, server.Epoch);
        Assert.Equal(77ul, h.Client.SessionId);
        Assert.Equal(77ul, server.SessionId);
        Assert.Equal(token, h.Client.SessionToken.ToArray());
        Assert.Equal(token, server.SessionToken.ToArray());
        Assert.Equal(HelloStatus.Accepted, h.Client.HandshakeStatus);
        Assert.Equal(HelloStatus.Accepted, server.HandshakeStatus);
        Assert.Equal(1, h.Admission.Calls);
        Assert.Equal("secret"u8.ToArray(), h.Admission.LastAuthToken);
        Assert.Empty(h.Admission.LastSessionToken);
        Assert.Equal(Frames.AllCaps, h.Admission.LastCaps);
        Assert.Equal(ConnectedEvents, h.ClientEvents);
        Assert.Equal(ConnectedEvents, h.ServerEvents);
        Assert.Equal(PeerRole.Client, h.Client.Role);
        Assert.Equal(PeerRole.Server, server.Role);
        Assert.NotNull(h.Client.RemoteEndPoint);
        Assert.NotNull(server.RemoteEndPoint);
        Assert.Null(h.Client.RemoteChannelTable);
        Assert.Same(h.Table, h.Client.Channels);
        Assert.True(h.Client.Capabilities.Datagrams);
        Assert.Equal(CloseSource.None, h.Client.CloseReason.Source);
        Assert.Null(h.Client.LastCallbackFault);
    }

    [Fact]
    public void Session_Id_Is_Random_When_Admission_Leaves_It_Zero()
    {
        using SessionHarness h = new();
        Assert.NotEqual(0ul, h.Client.SessionId);
        Assert.Equal(h.Server!.SessionId, h.Client.SessionId);
        Assert.True(h.Client.SessionToken.IsEmpty);
    }

    [Fact]
    public void Client_Receives_The_Channel_Table_When_It_Asks()
    {
        using SessionHarness h = new(client: o => o.RequestChannelTable = true);
        ChannelTableDescription? table = h.Client.RemoteChannelTable;
        Assert.NotNull(table);
        Assert.Equal(h.Table.Count, table.Count);
        Assert.Equal(h.Table.Hash, table.Hash);
        Assert.Equal("chat", table.Channels[2].Name);
        Assert.Equal(HelloFlags.RequestChannelTable, h.Admission.LastFlags);
    }

    [Theory]
    [InlineData(HelloStatus.Rejected)]
    [InlineData(HelloStatus.ServerFull)]
    [InlineData(HelloStatus.InternalError)]
    public void Admission_Refusal_Closes_Both_Sides(HelloStatus status)
    {
        using SessionHarness h = new(connect: false);
        h.Admission.Handler = (in HelloInfo _, QuiclyPeer _) => AdmissionResult.Reject(status, "go away");
        Assert.True(h.RunUntilClosed());
        Assert.Equal(status, h.Client.HandshakeStatus);
        Assert.Equal(status, h.Server!.HandshakeStatus);
        Assert.Equal(new CloseReason(QuiclyErrorCode.AdmissionRejected, "go away") { Source = CloseSource.Peer }, h.Client.CloseReason);
        Assert.Equal(new CloseReason(QuiclyErrorCode.AdmissionRejected, "go away") { Source = CloseSource.Local }, h.Server.CloseReason);
        Assert.Equal(0u, h.Client.Epoch);
        Assert.DoesNotContain(h.ClientEvents, e => e.To == PeerState.Connected);
        Assert.DoesNotContain(h.ServerEvents, e => e.To == PeerState.Connected);
    }

    [Fact]
    public void Channel_Table_Mismatch_Is_Refused_With_Status_2()
    {
        using SessionHarness h = new(serverTable: TestTables.Other, connect: false);
        Assert.True(h.RunUntilClosed());
        Assert.Equal(HelloStatus.ChannelTableMismatch, h.Client.HandshakeStatus);
        Assert.Equal(QuiclyErrorCode.AdmissionRejected, h.Client.CloseReason.Code);
        Assert.Equal("channel table mismatch", h.Client.CloseReason.Reason);
        Assert.Equal(0, h.Admission.Calls);
    }

    [Fact]
    public void Missing_Datagrams_Are_Refused_With_Status_7()
    {
        using SessionHarness h = new(link: new LinkOptions { DatagramsEnabled = false }, connect: false);
        Assert.True(h.RunUntilClosed());
        Assert.Equal(HelloStatus.DatagramsRequired, h.Client.HandshakeStatus);
        Assert.Equal(0, h.Admission.Calls);
    }

    [Fact]
    public void Client_Without_Datagram_Caps_Is_Refused_With_Status_7()
    {
        using ServerHarness h = new();
        h.Raw.SendControl(Frames.HelloFrame(h.Table.Hash, caps: PeerCaps.Lz4));
        Assert.True(h.RunUntil(() => h.Raw.IsClosed));
        Assert.Equal(HelloStatus.DatagramsRequired, Frames.AckStatus(h.Raw.ServerFrames()[0].Body));
        Assert.Equal((ulong)QuiclyErrorCode.AdmissionRejected, h.Raw.CloseCode);
    }

    [Fact]
    public void Streams_Only_Table_Needs_No_Datagrams_And_Pings_On_The_Control_Stream()
    {
        using SessionHarness h = new(link: new LinkOptions { DatagramsEnabled = false }, table: TestTables.StreamsOnly);
        h.Run(500_000);
        h.Client.GetStatistics(out PeerStatistics client);
        h.Server!.GetStatistics(out PeerStatistics server);
        Assert.True(client.PingsSent > 0);
        Assert.True(client.RttSamples > 0);
        Assert.True(server.RttSamples > 0);
        Assert.True(server.PongsSent > 0);
        Assert.Equal(0, client.DatagramsReceived);
    }

    [Fact]
    public void Version_Mismatch_Is_Refused_With_Status_1()
    {
        using ServerHarness h = new();
        h.Raw.SendControl(Frames.HelloFrame(h.Table.Hash, version: 2));
        Assert.True(h.RunUntil(() => h.Raw.IsClosed));
        List<(ControlType Type, byte[] Body)> frames = h.Raw.ServerFrames();
        Assert.Equal(ControlType.HelloAck, frames[0].Type);
        Assert.Equal(HelloStatus.VersionMismatch, Frames.AckStatus(frames[0].Body));
        Assert.Equal((ulong)QuiclyErrorCode.AdmissionRejected, h.Raw.CloseCode);
        Assert.Equal(0, h.Admission.Calls);
        Assert.True(h.RunUntil(() => h.Server!.State == PeerState.Closed));
        Assert.Equal(HelloStatus.VersionMismatch, h.Server!.HandshakeStatus);
        Assert.Equal(QuiclyErrorCode.AdmissionRejected, h.Server.CloseReason.Code);
    }

    [Theory]
    [InlineData("ping")]
    [InlineData("unknown-type")]
    [InlineData("bad-magic")]
    [InlineData("hello-ack")]
    [InlineData("truncated-hello")]
    public void A_Bad_First_Frame_Is_A_Protocol_Violation(string kind)
    {
        using ServerHarness h = new();
        byte[] frame = kind switch
        {
            "ping" => Frames.PingFrame(1, ControlCarrier.Stream),
            "unknown-type" => Frames.RawFrame(0x09, [1, 2]),
            "bad-magic" => Frames.HelloFrame(h.Table.Hash, badMagic: true),
            "hello-ack" => Frames.HelloAckFrame(HelloStatus.Accepted),
            _ => Frames.RawFrame((byte)ControlType.Hello, "QLCY"u8),
        };
        h.Raw.SendControl(frame);
        Assert.True(h.RunUntil(() => h.Raw.IsClosed));
        Assert.Equal((ulong)QuiclyErrorCode.ProtocolViolation, h.Raw.CloseCode);
        Assert.Equal(0, h.Admission.Calls);
        Assert.True(h.RunUntil(() => h.Server!.State == PeerState.Closed));
        Assert.Equal(new CloseReason(QuiclyErrorCode.ProtocolViolation, "protocol violation") { Source = CloseSource.Local }, h.Server!.CloseReason);
    }

    [Fact]
    public void A_Second_Hello_Is_A_Protocol_Violation()
    {
        using ServerHarness h = new();
        Assert.True(h.Admit());
        h.Raw.SendControl(Frames.HelloFrame(h.Table.Hash));
        Assert.True(h.RunUntil(() => h.Raw.IsClosed));
        Assert.Equal((ulong)QuiclyErrorCode.ProtocolViolation, h.Raw.CloseCode);
        Assert.Contains(h.Raw.ServerFrames(), f => f.Type == ControlType.Close && Frames.CloseCode(f.Body) == QuiclyErrorCode.ProtocolViolation);
    }

    [Fact]
    public void Two_Hellos_In_One_Burst_Are_Not_Admitted()
    {
        using ServerHarness h = new();
        byte[] hello = Frames.HelloFrame(h.Table.Hash);
        h.Raw.SendControl([.. hello, .. hello]);
        Assert.True(h.RunUntil(() => h.Raw.IsClosed));
        Assert.Equal((ulong)QuiclyErrorCode.ProtocolViolation, h.Raw.CloseCode);
        Assert.Equal(0, h.Admission.Calls);
        Assert.DoesNotContain(h.Events, e => e.To == PeerState.Connected);
    }

    [Fact]
    public void Missing_Hello_Times_Out()
    {
        using ServerHarness h = new(server: o => o.AdmissionTimeout = TimeSpan.FromMilliseconds(300));
        Assert.True(h.RunUntil(() => h.Raw.IsClosed, 2_000_000));
        Assert.InRange(h.Network.NowMicros, 300_000, 320_000);
        Assert.Equal((ulong)QuiclyErrorCode.Timeout, h.Raw.CloseCode);
        Assert.True(h.RunUntil(() => h.Server!.State == PeerState.Closed));
        Assert.Equal(new CloseReason(QuiclyErrorCode.Timeout, "admission timeout") { Source = CloseSource.Local }, h.Server!.CloseReason);
        (PeerState, PeerState)[] expected =
        [
            (PeerState.Connecting, PeerState.Handshaking),
            (PeerState.Handshaking, PeerState.Closing),
            (PeerState.Closing, PeerState.Closed),
        ];
        Assert.Equal(expected, h.Events);
    }

    [Fact]
    public void Pending_Admission_That_Never_Completes_Times_Out()
    {
        using SessionHarness h = new(server: o => o.AdmissionTimeout = TimeSpan.FromMilliseconds(300), connect: false);
        h.Admission.Handler = (in HelloInfo _, QuiclyPeer _) => AdmissionResult.Pending;
        Assert.True(h.RunUntilClosed(2_000_000));
        Assert.Equal(new CloseReason(QuiclyErrorCode.Timeout, "admission timeout") { Source = CloseSource.Peer }, h.Client.CloseReason);
        Assert.Equal(CloseSource.Local, h.Server!.CloseReason.Source);
        // A decision that arrives after the connection closed is ignored.
        h.Server.CompleteAdmission(AdmissionResult.Accept());
        Assert.Equal(PeerState.Closed, h.Server.State);
    }

    [Fact]
    public void Pending_Admission_Completed_Later_Connects()
    {
        using SessionHarness h = new(connect: false);
        h.Admission.Handler = (in HelloInfo _, QuiclyPeer _) => AdmissionResult.Pending;
        Assert.True(h.RunUntil(() => h.Admission.Calls == 1));
        h.Run(50_000);
        Assert.Equal(PeerState.Handshaking, h.Client.State);
        Assert.Equal(PeerState.Handshaking, h.Server!.State);
        Assert.Throws<ArgumentException>(() => h.Server.CompleteAdmission(AdmissionResult.Pending));
        h.Server.CompleteAdmission(AdmissionResult.Accept(sessionId: 5, epoch: 3));
        Assert.True(h.RunUntilConnected());
        Assert.Equal(3u, h.Client.Epoch);
        Assert.Equal(5ul, h.Client.SessionId);
        Assert.Throws<InvalidOperationException>(() => h.Server.CompleteAdmission(AdmissionResult.Accept()));
    }

    [Fact]
    public void Pending_Admission_Rejected_Later_Closes()
    {
        using SessionHarness h = new(connect: false);
        h.Admission.Handler = (in HelloInfo _, QuiclyPeer _) => AdmissionResult.Pending;
        Assert.True(h.RunUntil(() => h.Admission.Calls == 1));
        h.Server!.CompleteAdmission(AdmissionResult.Reject(HelloStatus.ServerFull));
        Assert.True(h.RunUntilClosed());
        Assert.Equal(HelloStatus.ServerFull, h.Client.HandshakeStatus);
        Assert.Null(h.Client.CloseReason.Reason);
    }

    [Fact]
    public void Admission_Exception_Refuses_With_Internal_Error_And_Surfaces_From_Poll()
    {
        using ServerHarness h = new();
        h.Admission.Handler = (in HelloInfo _, QuiclyPeer _) => throw new InvalidOperationException("policy bug");
        h.Raw.SendControl(Frames.HelloFrame(h.Table.Hash));
        h.Network.Advance(1_000);
        Assert.Throws<InvalidOperationException>(() => h.Server!.Poll());
        Assert.True(h.RunUntil(() => h.Raw.IsClosed));
        Assert.Equal(HelloStatus.InternalError, Frames.AckStatus(h.Raw.ServerFrames()[0].Body));
    }

    [Fact]
    public void Application_Traffic_Before_Admission_Is_Dropped_And_Counted()
    {
        using ServerHarness h = TestEngines.CreateServer(out Func<TestEngine> engine, link: new LinkOptions { PeerUnidiStreams = 4 });
        h.Admission.Handler = (in HelloInfo _, QuiclyPeer _) => AdmissionResult.Pending;
        Assert.Equal(TransportStatus.Success, h.Raw.SendDatagram([0x02, 1, 2, 3]));
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni([0x0A, 0x01, 0x07], out TransportStreamId stream));
        h.Run(10_000);
        Assert.Equal(2, h.Statistics().DroppedBeforeAdmission);
        Assert.Contains(h.Raw.Sink.OfKind(RecordedEventKind.StreamAborted),
            e => e.StreamId == stream && e.ErrorCode == (ulong)QuiclyErrorCode.AdmissionRejected);

        h.Raw.SendControl(Frames.HelloFrame(h.Table.Hash));
        Assert.True(h.RunUntil(() => h.Admission.Calls == 1));
        h.Raw.SendDatagram([0x02, 4]);
        h.Raw.SendControl(Frames.PingFrame(1, ControlCarrier.Stream));
        h.Raw.SendDatagram(Frames.PingFrame(2, ControlCarrier.Datagram));
        h.Run(10_000);
        Assert.Equal(5, h.Statistics().DroppedBeforeAdmission);
        Assert.Equal(0, engine().Received);
        Assert.Equal(PeerState.Handshaking, h.Server!.State);

        h.Server.CompleteAdmission(AdmissionResult.Accept());
        Assert.True(h.RunUntil(() => h.Raw.ServerFrames().Count > 0));
        h.Raw.SendDatagram([0x02, 5]);
        Assert.True(h.RunUntil(() => engine().Received == 1));
    }

    [Fact]
    public void Stream_Limits_Are_Raised_Only_After_Admission()
    {
        using ServerHarness h = new();

        // Before admission the client has no unidirectional credit: the start is refused (asynchronously, like MsQuic).
        Assert.Equal(TransportStatus.Success, h.Raw.Transport.OpenStream(StreamKind.Unidirectional, 3, 1, out TransportStreamId early));
        Assert.Equal(TransportStatus.Success, h.Raw.Transport.StartStream(early));
        Assert.True(h.RunUntil(() => h.Raw.Sink.OfKind(RecordedEventKind.StreamStarted).Any(e => e.StreamId == early)));
        Assert.Equal(TransportStatus.StreamLimitReached, h.Raw.Sink.OfKind(RecordedEventKind.StreamStarted).First(e => e.StreamId == early).Status);
        Assert.True(h.Admit());

        // After admission a new stream starts.
        Assert.Equal(TransportStatus.Success, h.Raw.Transport.OpenStream(StreamKind.Unidirectional, 4, 1, out TransportStreamId late));
        Assert.Equal(TransportStatus.Success, h.Raw.Transport.StartStream(late));
        Assert.True(h.RunUntil(() => h.Raw.Sink.OfKind(RecordedEventKind.StreamStarted).Any(e => e.StreamId == late)));
        Assert.Equal(TransportStatus.Success, h.Raw.Sink.OfKind(RecordedEventKind.StreamStarted).First(e => e.StreamId == late).Status);
    }
}

/// <summary>Handshake over the simulated transport, client side (a raw server answers by hand).</summary>
public class ClientHandshakeTests
{
    [Fact]
    public void Hello_Carries_The_Table_Hash_Tokens_And_Caps()
    {
        using ClientHarness h = new(client: o =>
        {
            o.MaxReceiveDatagram = 1100;
            o.LastEpoch = 5;
            o.SessionToken = new byte[] { 9, 9 };
            o.RequestChannelTable = true;
        });
        List<(ControlType Type, byte[] Body)> frames = h.ClientFrames();
        Assert.Single(frames);
        Assert.Equal(ControlType.Hello, frames[0].Type);
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(frames[0].Body, out Hello hello));
        Assert.Equal(h.Table.Hash, hello.TableHash);
        Assert.Equal("auth"u8.ToArray(), hello.AuthToken.ToArray());
        Assert.Equal(new byte[] { 9, 9 }, hello.SessionToken.ToArray());
        Assert.Equal(5u, hello.LastEpoch);
        Assert.Equal((ushort)1100, hello.MaxReceiveDatagram);
        Assert.Equal(Frames.AllCaps, hello.Caps);
        Assert.Equal(HelloFlags.RequestChannelTable, hello.Flags);
        Assert.Equal(PeerState.Handshaking, h.Client.State);
    }

    [Fact]
    public void Accepted_HelloAck_Connects_And_Raises_The_Servers_Stream_Limits()
    {
        using ClientHarness h = new();
        Assert.True(h.Accept(epoch: 4, sessionId: 99, token: [7, 7]));
        Assert.Equal(4u, h.Client.Epoch);
        Assert.Equal(99ul, h.Client.SessionId);
        Assert.Equal(new byte[] { 7, 7 }, h.Client.SessionToken.ToArray());
        Assert.Equal(HandshakeTests.ConnectedEvents, h.Events);
        Assert.True(h.RunUntil(() => h.Sink.CountOf(RecordedEventKind.StreamsAvailable) > 0));
        Assert.Equal(TransportStatus.Success, h.ServerTransport!.OpenStream(StreamKind.Unidirectional, 5, 1, out TransportStreamId id));
        Assert.Equal(TransportStatus.Success, h.ServerTransport.StartStream(id));
    }

    [Fact]
    public void Session_Message_Size_Cap_Comes_From_The_HelloAck()
    {
        using ClientHarness h = new();
        h.SendToClient(Frames.HelloAckFrame(HelloStatus.Accepted, maxMessageSize: 1000));
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Connected));
        Assert.Equal(1000, h.Client.Core.SessionMaxMessageSize);
        Assert.Equal(1000, h.Client.Core.EffectiveMaxMessageSize(h.Table[4]!));
        Assert.Equal(1000, h.Client.Core.EffectiveMaxMessageSize(h.Table[2]!));
        Assert.Equal(h.Table[5]!.MaxMessageSize, h.Client.Core.EffectiveMaxMessageSize(h.Table[5]!));
    }

    [Fact]
    public void Refusing_HelloAck_Closes_With_Its_Sanitized_Reason()
    {
        using ClientHarness h = new();
        h.SendToClient(Frames.HelloAckFrame(HelloStatus.Rejected, epoch: 0, reason: "noway‮"));
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Closed));
        Assert.Equal(HelloStatus.Rejected, h.Client.HandshakeStatus);
        Assert.Equal(new CloseReason(QuiclyErrorCode.AdmissionRejected, "no�way�") { Source = CloseSource.Peer }, h.Client.CloseReason);
        Assert.True(h.RunUntil(() => h.IsServerClosed));
        Assert.Equal((ulong)QuiclyErrorCode.AdmissionRejected, h.ServerCloseCode);
    }

    [Theory]
    [InlineData("epoch-0")]
    [InlineData("informational-first")]
    [InlineData("ping-first")]
    [InlineData("hello")]
    [InlineData("table-request")]
    [InlineData("second-ack")]
    [InlineData("bad-close")]
    [InlineData("fin")]
    public void Server_Protocol_Violations_Close_The_Client(string kind)
    {
        using ClientHarness h = new();
        switch (kind)
        {
            case "epoch-0":
                h.SendToClient(Frames.HelloAckFrame(HelloStatus.Accepted, epoch: 0));
                break;
            case "informational-first":
                h.SendToClient(Frames.HelloAckFrame(HelloStatus.Informational, epoch: 0));
                break;
            case "ping-first":
                h.SendToClient(Frames.PingFrame(1, ControlCarrier.Stream));
                break;
            case "hello":
                Assert.True(h.Accept());
                h.SendToClient(Frames.HelloFrame(h.Table.Hash));
                break;
            case "table-request":
                Assert.True(h.Accept());
                h.SendToClient(Frames.TableRequestFrame());
                break;
            case "second-ack":
                Assert.True(h.Accept());
                h.SendToClient(Frames.HelloAckFrame(HelloStatus.Accepted));
                break;
            case "bad-close":
                Assert.True(h.Accept());
                h.SendToClient(Frames.RawFrame((byte)ControlType.Close, [1, 2]));
                break;
            default:
                Assert.True(h.Accept());
                Assert.Equal(TransportStatus.Success, RawClient.SendStream(h.ServerTransport!, h.ControlStream, [], TransportSendFlags.Fin));
                break;
        }

        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Closed));
        Assert.Equal(QuiclyErrorCode.ProtocolViolation, h.Client.CloseReason.Code);
        Assert.Equal(CloseSource.Local, h.Client.CloseReason.Source);
        Assert.True(h.RunUntil(() => h.IsServerClosed));
        Assert.Equal((ulong)QuiclyErrorCode.ProtocolViolation, h.ServerCloseCode);
    }

    [Fact]
    public void Missing_HelloAck_Times_Out()
    {
        using ClientHarness h = new(client: o => o.AdmissionTimeout = TimeSpan.FromMilliseconds(300));
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Closed, 2_000_000));
        Assert.Equal(QuiclyErrorCode.Timeout, h.Client.CloseReason.Code);
        Assert.Contains(h.ClientFrames(), f => f.Type == ControlType.Close && Frames.CloseCode(f.Body) == QuiclyErrorCode.Timeout);
        Assert.True(h.RunUntil(() => h.IsServerClosed));
        Assert.Equal((ulong)QuiclyErrorCode.Timeout, h.ServerCloseCode);
    }

    [Fact]
    public void Close_From_The_Server_During_The_Handshake_Closes_The_Client_With_Its_Code()
    {
        using ClientHarness h = new();
        h.SendToClient(Frames.CloseFrame(QuiclyErrorCode.SessionReplaced, "replaced"));
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Closed));
        Assert.Equal(new CloseReason(QuiclyErrorCode.SessionReplaced, "replaced") { Source = CloseSource.Peer }, h.Client.CloseReason);
        Assert.Equal(0u, h.Client.Epoch);
        Assert.True(h.RunUntil(() => h.IsServerClosed));
        Assert.Equal((ulong)QuiclyErrorCode.SessionReplaced, h.ServerCloseCode);
    }

    [Fact]
    public void Informational_HelloAck_Updates_The_Remote_Channel_Table()
    {
        using ClientHarness h = new();
        Assert.True(h.Accept());
        Assert.Null(h.Client.RemoteChannelTable);
        h.SendToClient(Frames.HelloAckFrame(HelloStatus.Informational, epoch: 0, table: Frames.TableSection(h.Table)));
        Assert.True(h.RunUntil(() => h.Client.RemoteChannelTable is not null));
        Assert.Equal(h.Table.Hash, h.Client.RemoteChannelTable!.Hash);
        Assert.Equal(PeerState.Connected, h.Client.State);
    }

    [Fact]
    public void Stream_Ping_Is_Answered_On_The_Control_Stream()
    {
        using ClientHarness h = new();
        Assert.True(h.Accept());
        h.SendToClient(Frames.PingFrame(4242, ControlCarrier.Stream));
        Assert.True(h.RunUntil(() => h.ClientFrames().Exists(f => f.Type == ControlType.Pong)));
        (ControlType _, byte[] body) = h.ClientFrames().Find(f => f.Type == ControlType.Pong);
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(body, out Pong pong));
        Assert.Equal(4242u, pong.EchoedTimeMicros);
    }

    [Fact]
    public void Control_Stream_Start_Failure_Closes_The_Client()
    {
        using ClientHarness h = new();
        // The simulator never fails a start; drive the sink directly.
        h.Client.TransportSink.OnStreamStarted(default, PeerCore.ControlStreamContext, TransportStatus.Failed);
        Assert.True(h.RunUntil(() => h.Client.State == PeerState.Closed));
        Assert.Equal(QuiclyErrorCode.InternalError, h.Client.CloseReason.Code);
    }
}
