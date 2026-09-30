// Purpose: verify datagram admission when transport capability notifications lag the connection callback.
// Responsibilities: cover both handshake roles, notification ordering, unsupported peers, wakeups and admission deadlines.
// Design intent: a provisional false connect snapshot must not reject a supported connection; explicit negative reports remain refusals.
using System.Net;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Tests.Session;

public class DatagramAdmissionOrderingTests
{
    [Fact]
    public void Server_Holds_Valid_Hello_Until_Delayed_Positive_Capability_And_Signals_Admission_Work()
    {
        using AdmissionFixture h = new(PeerRole.Server);
        h.Connect();
        h.ReceiveHello();
        h.Peer.Poll();

        Assert.Equal(PeerState.Handshaking, h.Peer.State);
        Assert.Equal(0, h.Admission.Calls);
        Assert.Empty(h.ControlFrames());
        h.Signal.Take();

        h.ReportDatagrams(true);
        Assert.Equal(1, h.Signal.Calls);
        Assert.True(h.Peer.HasPendingWork);
        h.Peer.Poll();

        Assert.Equal(PeerState.Connected, h.Peer.State);
        Assert.Equal(1, h.Admission.Calls);
        Assert.Equal(HelloStatus.Accepted, Frames.AckStatus(Assert.Single(h.ControlFrames()).Body));
        h.Peer.Poll();
        Assert.Equal(1, h.Admission.Calls);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Server_Admits_When_Positive_Capability_Precedes_Hello(bool beforeConnected)
    {
        using AdmissionFixture h = new(PeerRole.Server);
        if (beforeConnected) h.ReportDatagrams(true);
        h.Connect();
        if (!beforeConnected) h.ReportDatagrams(true);
        h.ReceiveHello();
        h.Peer.Poll();

        Assert.Equal(PeerState.Connected, h.Peer.State);
        Assert.Equal(1, h.Admission.Calls);
        Assert.Equal(HelloStatus.Accepted, Frames.AckStatus(Assert.Single(h.ControlFrames()).Body));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Server_Rejects_Explicitly_Unsupported_Datagrams_With_Status_7(bool reportBeforeHello)
    {
        using AdmissionFixture h = new(PeerRole.Server);
        if (reportBeforeHello) h.ReportDatagrams(false);
        h.Connect();
        h.ReceiveHello();
        h.Peer.Poll();
        if (!reportBeforeHello)
        {
            Assert.Equal(PeerState.Handshaking, h.Peer.State);
            Assert.Empty(h.ControlFrames());
            h.ReportDatagrams(false);
            h.Peer.Poll();
        }

        Assert.Equal(PeerState.Closing, h.Peer.State);
        Assert.Equal(0, h.Admission.Calls);
        Assert.Equal(HelloStatus.DatagramsRequired, Frames.AckStatus(Assert.Single(h.ControlFrames()).Body));
        Assert.Equal(QuiclyErrorCode.AdmissionRejected, h.Peer.CloseReason.Code);
    }

    [Fact]
    public void Server_Rejects_Missing_Client_Caps_While_Local_Capability_Is_Pending()
    {
        using AdmissionFixture h = new(PeerRole.Server);
        h.Connect();
        h.ReceiveHello(PeerCaps.Lz4);
        h.Peer.Poll();

        Assert.Equal(PeerState.Closing, h.Peer.State);
        Assert.Equal(0, h.Admission.Calls);
        Assert.Equal(HelloStatus.DatagramsRequired, Frames.AckStatus(Assert.Single(h.ControlFrames()).Body));
    }

    [Fact]
    public void Server_Pending_Capability_Uses_Original_Admission_Deadline()
    {
        using AdmissionFixture h = new(PeerRole.Server);
        h.Connect();
        h.Clock.AdvanceMicros(100_000);
        h.ReceiveHello();
        h.Peer.Poll();
        h.Clock.AdvanceMicros(199_999);
        h.Peer.Poll();

        Assert.Equal(PeerState.Handshaking, h.Peer.State);
        Assert.Equal(300_000, h.Peer.NextPollDeadlineMicros);
        Assert.Empty(h.ControlFrames());
        h.Clock.AdvanceMicros(1);
        h.Peer.Poll();

        Assert.Equal(PeerState.Closing, h.Peer.State);
        Assert.Equal(QuiclyErrorCode.Timeout, h.Peer.CloseReason.Code);
        Assert.Equal("admission timeout", h.Peer.CloseReason.Reason);
        Assert.Equal(0, h.Admission.Calls);
    }

    [Fact]
    public void Server_Late_Positive_Capability_Cannot_Admit_After_Deadline()
    {
        using AdmissionFixture h = new(PeerRole.Server);
        h.Connect();
        h.ReceiveHello();
        h.Peer.Poll();
        h.Clock.AdvanceMicros(300_000);
        h.Peer.Poll();
        h.ReportDatagrams(true);
        h.Peer.Poll();

        Assert.Equal(PeerState.Closing, h.Peer.State);
        Assert.Equal(QuiclyErrorCode.Timeout, h.Peer.CloseReason.Code);
        Assert.Equal(0, h.Admission.Calls);
        Assert.DoesNotContain(h.ControlFrames(), frame => frame.Type == ControlType.HelloAck
            && Frames.AckStatus(frame.Body) == HelloStatus.Accepted);
    }

    [Fact]
    public void Client_Waits_For_Delayed_Capability_Before_Advertising_Negotiated_Caps()
    {
        using AdmissionFixture h = new(PeerRole.Client);
        h.Connect();

        Assert.Equal(PeerState.Handshaking, h.Peer.State);
        Assert.Empty(h.ControlFrames());
        // The Handshaking transition raised a coalesced edge inside Connect's Poll; consume and re-arm it first.
        h.Peer.Poll();
        Assert.False(h.Peer.HasPendingWork);
        h.Signal.Take();
        h.ReportDatagrams(true);
        Assert.Equal(1, h.Signal.Calls);
        Assert.True(h.Peer.HasPendingWork);
        h.Peer.Poll();

        (ControlType type, byte[] body) = Assert.Single(h.ControlFrames());
        Assert.Equal(ControlType.Hello, type);
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(body, out Hello hello));
        Assert.Equal(Frames.AllCaps, hello.Caps);
        h.Peer.Poll();
        Assert.Single(h.ControlFrames());
    }

    [Fact]
    public void Client_Explicitly_Unsupported_Datagrams_Still_Advertises_Missing_Caps_For_Server_Refusal()
    {
        using AdmissionFixture h = new(PeerRole.Client);
        h.ReportDatagrams(false);
        h.Connect();

        (ControlType type, byte[] body) = Assert.Single(h.ControlFrames());
        Assert.Equal(ControlType.Hello, type);
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(body, out Hello hello));
        Assert.Equal(PeerCaps.Lz4, hello.Caps);
        Assert.Equal(PeerState.Handshaking, h.Peer.State);
    }

    [Fact]
    public void Client_Pending_Capability_Times_Out_Without_Sending_A_False_Hello()
    {
        using AdmissionFixture h = new(PeerRole.Client);
        h.Connect();
        h.Clock.AdvanceMicros(299_999);
        h.Peer.Poll();

        Assert.Equal(PeerState.Handshaking, h.Peer.State);
        Assert.Empty(h.ControlFrames());
        Assert.Equal(300_000, h.Peer.NextPollDeadlineMicros);
        h.Clock.AdvanceMicros(1);
        h.Peer.Poll();

        Assert.Equal(PeerState.Closing, h.Peer.State);
        Assert.Equal(QuiclyErrorCode.Timeout, h.Peer.CloseReason.Code);
        Assert.DoesNotContain(h.ControlFrames(), frame => frame.Type == ControlType.Hello);
    }

    [Fact]
    public void Streams_Only_Client_Does_Not_Wait_For_Datagram_Capability()
    {
        using AdmissionFixture h = new(PeerRole.Client, TestTables.StreamsOnly);
        h.Connect();

        Assert.Equal(ControlType.Hello, Assert.Single(h.ControlFrames()).Type);
        Assert.Equal(PeerState.Handshaking, h.Peer.State);
    }

    [Fact]
    public void Server_Positive_Capability_At_Deadline_Cannot_Admit_Without_A_Preceding_Timeout_Poll()
    {
        using AdmissionFixture h = new(PeerRole.Server);
        h.Connect();
        h.ReceiveHello();
        h.Peer.Poll();
        h.Clock.AdvanceMicros(300_000);
        h.ReportDatagrams(true);
        h.Peer.Poll();

        Assert.Equal(PeerState.Closing, h.Peer.State);
        Assert.Equal(QuiclyErrorCode.Timeout, h.Peer.CloseReason.Code);
        Assert.Equal(0, h.Admission.Calls);
        Assert.DoesNotContain(h.ControlFrames(), frame => frame.Type == ControlType.HelloAck
            && Frames.AckStatus(frame.Body) == HelloStatus.Accepted);
    }

    [Fact]
    public void Client_Positive_Capability_At_Deadline_Does_Not_Send_Hello_Before_Checking_Timeout()
    {
        using AdmissionFixture h = new(PeerRole.Client);
        h.Connect();
        h.Clock.AdvanceMicros(300_000);
        h.ReportDatagrams(true);
        h.Peer.Poll();

        Assert.Equal(PeerState.Closing, h.Peer.State);
        Assert.Equal(QuiclyErrorCode.Timeout, h.Peer.CloseReason.Code);
        Assert.DoesNotContain(h.ControlFrames(), frame => frame.Type == ControlType.Hello);
    }

    [Fact]
    public void Server_Delayed_Capability_Does_Not_Invoke_Pending_Admission_Twice()
    {
        using AdmissionFixture h = new(PeerRole.Server);
        h.Admission.Handler = (in HelloInfo _, QuiclyPeer _) => AdmissionResult.Pending;
        h.Connect();
        h.ReceiveHello();
        h.Peer.Poll();
        h.ReportDatagrams(true);
        h.Peer.Poll();

        Assert.Equal(PeerState.Handshaking, h.Peer.State);
        Assert.Equal(1, h.Admission.Calls);
        h.ReportDatagrams(true);
        h.Peer.Poll();
        Assert.Equal(1, h.Admission.Calls);
        Assert.Empty(h.ControlFrames());
        h.Peer.CompleteAdmission(AdmissionResult.Accept());
        h.Peer.Poll();
        Assert.Equal(PeerState.Connected, h.Peer.State);
        Assert.Equal(1, h.Admission.Calls);
    }

    [Fact]
    public void Server_Pending_Policy_At_Deadline_Cannot_Complete_Without_A_Preceding_Timeout_Poll()
    {
        using AdmissionFixture h = new(PeerRole.Server);
        h.Admission.Handler = (in HelloInfo _, QuiclyPeer _) => AdmissionResult.Pending;
        h.Connect();
        h.ReceiveHello();
        h.Peer.Poll();
        h.ReportDatagrams(true);
        h.Peer.Poll();
        Assert.Equal(1, h.Admission.Calls);

        h.Clock.AdvanceMicros(300_000);
        h.Peer.CompleteAdmission(AdmissionResult.Accept());
        h.Peer.Poll();

        Assert.Equal(PeerState.Closing, h.Peer.State);
        Assert.Equal(QuiclyErrorCode.Timeout, h.Peer.CloseReason.Code);
        Assert.Equal(1, h.Admission.Calls);
        Assert.DoesNotContain(h.ControlFrames(), frame => frame.Type == ControlType.HelloAck
            && Frames.AckStatus(frame.Body) == HelloStatus.Accepted);
    }
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void Client_Synchronous_Capability_Before_Attachment_Preserves_All_Negotiated_Flags(bool positiveConnectSnapshot)
    {
        FakeTransport transport = new()
        {
            ReportedCapabilities = new TransportCapabilities
            {
                Datagrams = true,
                DatagramSendState = true,
                CancelOnBlocked = true,
                MaxDatagramPayload = 1200,
            },
        };
        QuiclyPeer peer = QuiclyPeer.Connect(new SynchronousConnector(transport, positiveConnectSnapshot),
            new IPEndPoint(IPAddress.Loopback, 1), null, TestTables.Default,
            new PeerOptions { Clock = new VirtualClock() });
        try
        {
            peer.Poll();
            (ControlType type, byte[] body) = Assert.Single(Frames.ParseStream(Assert.Single(transport.StreamSends).Data));
            Assert.Equal(ControlType.Hello, type);
            Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(body, out Hello hello));
            Assert.Equal(Frames.AllCaps, hello.Caps);
            Assert.True(peer.Core.CancelOnBlockedHonoured);
            Assert.Equal(1200, peer.Core.MaxDatagramPayload);
        }
        finally
        {
            peer.TransportSink.OnClosed(TransportCloseReason.Local, 0, 0);
            peer.Dispose();
        }
    }

    [Fact]
    public void Client_Stale_Negative_Connect_Snapshot_Does_Not_Erase_Attached_Capability_Flags()
    {
        using AdmissionFixture h = new(PeerRole.Client);
        h.ReportDatagrams(true);
        h.Transport.ReportedCapabilities = new TransportCapabilities
        {
            Datagrams = true,
            DatagramSendState = true,
            CancelOnBlocked = true,
            MaxDatagramPayload = 1200,
        };
        TransportConnectedInfo stale = default;
        h.Peer.TransportSink.OnConnected(in stale);
        h.Peer.Poll();

        byte[] body = Assert.Single(h.ControlFrames()).Body;
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(body, out Hello hello));
        Assert.Equal(Frames.AllCaps, hello.Caps);
        Assert.True(h.Peer.Core.CancelOnBlockedHonoured);
        Assert.Equal(1200, h.Peer.Core.MaxDatagramPayload);
    }

    [Fact]
    public unsafe void Client_Accepted_Ack_At_Deadline_Cannot_Admit_Without_A_Preceding_Timeout_Poll()
    {
        using AdmissionFixture h = new(PeerRole.Client);
        h.ReportDatagrams(true);
        h.Connect();
        TransportStreamId stream = Assert.Single(h.Transport.StreamSends).Id;
        h.Clock.AdvanceMicros(300_000);
        byte[] payload = [0x00, .. Frames.HelloAckFrame(HelloStatus.Accepted)];
        fixed (byte* pointer = payload)
        {
            TransportSegment segment = new(pointer, payload.Length);
            Assert.Equal(ReceiveResult.Consumed(payload.Length),
                h.Peer.TransportSink.OnStreamReceived(stream, new ReadOnlySpan<TransportSegment>(&segment, 1), 0, false));
        }

        h.Peer.Poll();
        Assert.Equal(PeerState.Closing, h.Peer.State);
        Assert.Equal(QuiclyErrorCode.Timeout, h.Peer.CloseReason.Code);
    }

    // A connector may publish callbacks synchronously, before Connect has attached its returned transport.
    private sealed class SynchronousConnector(FakeTransport transport, bool positiveConnectSnapshot) : ITransportConnector
    {
        public ITransport Connect(EndPoint endpoint, string? serverName, ITransportSink sink)
        {
            sink.OnDatagramCapabilityChanged(true, 1200);
            TransportConnectedInfo info = new()
            {
                Capabilities = positiveConnectSnapshot ? transport.Capabilities : default,
            };
            sink.OnConnected(in info);
            return transport;
        }
    }
    // Direct callbacks make the ordering reproducible without socket timing, sleeps or filesystem state.
    private sealed class AdmissionFixture : IDisposable
    {
        public AdmissionFixture(PeerRole role, ChannelTable? table = null)
        {
            Table = table ?? TestTables.Default;
            Transport.ReportedCapabilities = default;
            PeerOptions options = new()
            {
                Clock = Clock,
                WorkSignal = Signal,
                AdmissionTimeout = TimeSpan.FromMilliseconds(300),
            };
            Peer = role == PeerRole.Server
                ? QuiclyPeer.CreateServerPeer(Transport, default, Table, options, Admission)
                : QuiclyPeer.Connect(new FakeConnector(Transport), new IPEndPoint(IPAddress.Loopback, 1), null, Table, options);
        }

        public ChannelTable Table { get; }
        public VirtualClock Clock { get; } = new();
        public FakeTransport Transport { get; } = new();
        public RecordingWorkSignal Signal { get; } = new();
        public TestAdmission Admission { get; } = new();
        public QuiclyPeer Peer { get; }

        public void Connect()
        {
            TransportConnectedInfo info = new() { Capabilities = Transport.ReportedCapabilities };
            Peer.TransportSink.OnConnected(in info);
            Peer.Poll();
        }

        public void ReportDatagrams(bool enabled)
        {
            Transport.ReportedCapabilities = new TransportCapabilities
            {
                Datagrams = enabled,
                DatagramSendState = enabled,
                MaxDatagramPayload = enabled ? 1200 : 0,
            };
            Peer.TransportSink.OnDatagramCapabilityChanged(enabled, enabled ? 1200 : 0);
        }

        public unsafe void ReceiveHello(PeerCaps caps = Frames.AllCaps)
        {
            TransportStreamId stream = new(100, 1);
            Peer.TransportSink.OnPeerStreamStarted(stream, StreamKind.Bidirectional);
            byte[] payload = [0x00, .. Frames.HelloFrame(Table.Hash, caps: caps)];
            fixed (byte* pointer = payload)
            {
                TransportSegment segment = new(pointer, payload.Length);
                Assert.Equal(ReceiveResult.Consumed(payload.Length),
                    Peer.TransportSink.OnStreamReceived(stream, new ReadOnlySpan<TransportSegment>(&segment, 1), 0, false));
            }
        }

        public List<(ControlType Type, byte[] Body)> ControlFrames() =>
            Frames.ParseStream(Transport.StreamSends.SelectMany(send => send.Data).ToArray());

        public void Dispose()
        {
            // FakeTransport never raises close callbacks; deliver the terminal callback before releasing native pools.
            Peer.TransportSink.OnClosed(TransportCloseReason.Local, 0, 0);
            Peer.Dispose();
        }
    }
}
