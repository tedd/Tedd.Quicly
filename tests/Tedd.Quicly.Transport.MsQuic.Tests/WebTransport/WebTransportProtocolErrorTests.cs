using System.Net;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Http3;
using Tedd.Quicly.Http3.WebTransport;
using Tedd.Quicly.Testing.Conformance;
using Tedd.Quicly.Testing.Simulation;
using Tedd.Quicly.Transport.MsQuic.WebTransport;

namespace Tedd.Quicly.Transport.MsQuic.Tests.WebTransport;

/// <summary>
/// How the carrier answers a peer that breaks HTTP/3 or the WebTransport draft: the control-stream rules, capsules and
/// stream types that must be discarded, reset or close the connection (RFC 9114 §6–§8, PROTOCOL.md §5).
/// </summary>
public sealed unsafe class WebTransportProtocolErrorTests : IDisposable
{
    private readonly VirtualClock _clock = new();
    private readonly SimulatedNetwork _network;
    private readonly SimulatedListener _listener;
    private readonly RawHttp3Peer _peer = new();
    private readonly RecordingSink _clientSink;
    private WebTransportConnector? _connector;
    private ITransport? _client;

    public WebTransportProtocolErrorTests()
    {
        _network = new SimulatedNetwork(_clock, seed: 1);
        _clientSink = new RecordingSink(_clock);
        _listener = new SimulatedListener(_network, new IPEndPoint(IPAddress.Loopback, 0));
    }

    private WebTransportTransport Start(WebTransportOptions? options = null)
    {
        _listener.Start(static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, (ITransport transport, in NewConnectionInfo _) =>
        {
            _peer.Transport = transport;
            transport.UpdatePeerStreamLimits(8, 16);
            return _peer;
        });

        _connector = new WebTransportConnector(new SimulatedConnector(_network, new LinkOptions { DelayMicros = 2_000 }), options);
        _client = _connector.Connect(_listener.LocalEndPoint, "localhost", _clientSink);
        _client.UpdatePeerStreamLimits(8, 16);
        return (WebTransportTransport)_client;
    }

    private bool Pump(Func<bool> condition, int millis = 5_000)
    {
        long deadline = _network.NowMicros + (millis * 1000L);
        while (!condition())
        {
            if (_network.NowMicros >= deadline) return false;
            _network.Advance(250);
        }

        return true;
    }

    private WebTransportTransport StartConnected(WebTransportOptions? options = null)
    {
        WebTransportTransport carrier = Start(options);
        Assert.True(Pump(() => _clientSink.CountOf(RecordedEventKind.Connected) == 1), "the client's OnConnected");
        return carrier;
    }

    /// <summary>A control stream whose bytes are the type byte followed by <paramref name="frames"/>.</summary>
    private static byte[] Control(params byte[][] frames)
    {
        var bytes = new List<byte> { (byte)Http3StreamType.Control };
        foreach (byte[] frame in frames) bytes.AddRange(frame);
        return bytes.ToArray();
    }

    private static byte[] Frame(Http3FrameType type, ReadOnlySpan<byte> payload)
    {
        var buffer = new byte[Http3FrameWriter.GetHeaderLength(type, (ulong)payload.Length) + payload.Length];
        Http3FrameWriter.WriteFrame(buffer, type, payload);
        return buffer;
    }

    private static byte[] SettingsFrame()
    {
        Http3Settings settings = Http3Settings.CreateWebTransportServerDefaults(1);
        var buffer = new byte[settings.GetFrameLength()];
        settings.WriteFrame(buffer);
        return buffer;
    }

    // ------------------------------------------------------------------ control stream

    [Fact]
    public void A_first_control_frame_that_is_not_settings_closes_the_connection()
    {
        _peer.ControlStreamOverride = Control(Frame(Http3FrameType.MaxPushId, [0x00]));
        Start();
        Assert.True(Pump(() => _clientSink.IsClosed), "the client's OnClosed");
        Assert.Equal(0, _clientSink.CountOf(RecordedEventKind.Connected));
    }

    [Fact]
    public void A_malformed_settings_frame_closes_the_connection()
    {
        // An identifier with no value: the decoder rejects it.
        _peer.ControlStreamOverride = Control(Frame(Http3FrameType.Settings, [0x06]));
        Start();
        Assert.True(Pump(() => _clientSink.IsClosed), "the client's OnClosed");
    }

    [Fact]
    public void A_second_settings_frame_closes_the_connection()
    {
        _peer.ControlStreamOverride = Control(SettingsFrame(), SettingsFrame());
        Start();
        Assert.True(Pump(() => _clientSink.IsClosed), "the client's OnClosed");
    }

    [Fact]
    public void A_request_frame_on_the_control_stream_closes_the_connection()
    {
        _peer.ControlStreamOverride = Control(SettingsFrame(), Frame(Http3FrameType.Headers, [0x00, 0x00]));
        Start();
        Assert.True(Pump(() => _clientSink.IsClosed), "the client's OnClosed");
    }

    [Fact]
    public void A_grease_frame_on_the_control_stream_is_ignored()
    {
        byte[] grease = Frame((Http3FrameType)Http3Grease.Make(3), [1, 2, 3]);
        _peer.ControlStreamOverride = Control(SettingsFrame(), grease);
        StartConnected();
        Pump(static () => false, 200);
        Assert.False(_clientSink.IsClosed);
    }

    [Fact]
    public void Goaway_ends_the_session()
    {
        StartConnected();
        _peer.SendOnControl((ulong)Http3FrameType.GoAway, [0x00]);
        Assert.True(Pump(() => _clientSink.IsClosed), "the client's OnClosed after GOAWAY");
    }

    [Fact]
    public void A_settings_frame_split_across_receives_is_still_read()
    {
        // The frame reader has to survive fragmentation; a long reason-free SETTINGS plus a big grease frame forces it.
        var padding = new byte[4096];
        _peer.ControlStreamOverride = Control(SettingsFrame(), Frame((Http3FrameType)Http3Grease.Make(7), padding));
        StartConnected();
        Assert.False(_clientSink.IsClosed);
    }

    // ------------------------------------------------------------------ capsules

    [Fact]
    public void A_close_session_capsule_reports_the_peers_application_code()
    {
        StartConnected();
        Span<byte> payload = stackalloc byte[64];
        int written = CapsuleWriter.WriteCloseSession(payload, 0x10, "bulk canceled"u8);
        int headerLength = Http3FrameWriter.GetHeaderLength((ulong)CapsuleType.CloseWebTransportSession, (ulong)(written - Http3FrameWriter.GetHeaderLength((ulong)CapsuleType.CloseWebTransportSession, 0)));

        // WriteCloseSession already emits the whole capsule; resend just its payload through SendCapsule.
        Assert.True(Http3FrameReader.TryReadFrame(payload.Slice(0, written), out ulong type, out ReadOnlySpan<byte> body, out _));
        Assert.Equal((ulong)CapsuleType.CloseWebTransportSession, type);
        Assert.True(headerLength > 0);
        _peer.SendCapsule(type, body);

        Assert.True(Pump(() => _clientSink.IsClosed), "the client's OnClosed after the capsule");
        RecordedEvent closed = _clientSink.OfKind(RecordedEventKind.Closed)[0];
        Assert.Equal(TransportCloseReason.Peer, closed.CloseReason);
        Assert.Equal(0x10ul, closed.ErrorCode);
    }

    [Fact]
    public void A_malformed_close_session_capsule_closes_the_connection()
    {
        StartConnected();
        _peer.SendCapsule((ulong)CapsuleType.CloseWebTransportSession, [1, 2]);
        Assert.True(Pump(() => _clientSink.IsClosed), "the client's OnClosed");
    }

    [Fact]
    public void A_drain_capsule_leaves_the_session_running()
    {
        WebTransportTransport carrier = StartConnected();
        _peer.SendCapsule((ulong)CapsuleType.DrainWebTransportSession, default);
        Pump(static () => false, 200);
        Assert.False(_clientSink.IsClosed);
        Assert.True(carrier.IsSessionEstablished);
    }

    [Fact]
    public void An_unknown_capsule_is_ignored()
    {
        WebTransportTransport carrier = StartConnected();
        _peer.SendCapsule((ulong)CapsuleType.WebTransportMaxData, [0x44, 0x00]);
        _peer.SendCapsule(Http3Grease.Make(11), [9, 9, 9]);
        Pump(static () => false, 200);
        Assert.False(_clientSink.IsClosed);
        Assert.True(carrier.IsSessionEstablished);
    }

    // ------------------------------------------------------------------ peer streams

    [Fact]
    public void A_webtransport_stream_for_another_session_is_reset_and_never_reaches_core()
    {
        WebTransportTransport carrier = StartConnected();
        Span<byte> preamble = stackalloc byte[16];
        int written = WebTransportFraming.WriteUnidirectionalPreamble(preamble, carrier.SessionId + 4);
        _peer.OpenUnidirectional(preamble.Slice(0, written));

        Pump(static () => false, 300);
        Assert.Equal(0, _clientSink.CountOf(RecordedEventKind.PeerStreamStarted));
        Assert.False(_clientSink.IsClosed);
    }

    [Fact]
    public void A_webtransport_stream_for_this_session_reaches_core_without_its_preamble()
    {
        WebTransportTransport carrier = StartConnected();
        Span<byte> bytes = stackalloc byte[32];
        int written = WebTransportFraming.WriteUnidirectionalPreamble(bytes, carrier.SessionId);
        for (int i = 0; i < 8; i++) bytes[written + i] = (byte)(0x40 + i);
        _peer.OpenUnidirectional(bytes.Slice(0, written + 8));

        Assert.True(Pump(() => _clientSink.CountOf(RecordedEventKind.PeerStreamStarted) == 1), "OnPeerStreamStarted");
        RecordedEvent peer = _clientSink.OfKind(RecordedEventKind.PeerStreamStarted)[0];
        Assert.True(Pump(() => _clientSink.GetStreamData(peer.StreamId).Length == 8), "the stream's data without the preamble");
        byte[] data = _clientSink.GetStreamData(peer.StreamId);
        for (int i = 0; i < 8; i++) Assert.Equal((byte)(0x40 + i), data[i]);
    }

    [Fact]
    public void A_webtransport_stream_whose_preamble_names_an_invalid_session_closes_the_connection()
    {
        WebTransportTransport carrier = StartConnected();
        Assert.NotEqual(ulong.MaxValue, carrier.SessionId);

        // The WebTransport stream type (a two-byte varint) followed by a session id that is not a client-initiated
        // bidirectional stream id.
        Span<byte> bytes = stackalloc byte[8];
        int written = Http3FrameWriter.WriteStreamType(bytes, Http3StreamType.WebTransport);
        bytes[written] = 0x01;
        _peer.OpenUnidirectional(bytes.Slice(0, written + 1));
        Assert.True(Pump(() => _clientSink.IsClosed), "the client's OnClosed");
    }

    [Fact]
    public void An_unknown_unidirectional_stream_type_is_discarded()
    {
        StartConnected();
        var bytes = new List<byte>();
        var grease = new byte[8];
        int written = Http3FrameWriter.WriteStreamType(grease, (Http3StreamType)Http3Grease.Make(5));
        bytes.AddRange(grease.AsSpan(0, written).ToArray());
        bytes.AddRange([1, 2, 3, 4]);
        _peer.OpenUnidirectional(bytes.ToArray());

        Pump(static () => false, 300);
        Assert.False(_clientSink.IsClosed);
        Assert.Equal(0, _clientSink.CountOf(RecordedEventKind.PeerStreamStarted));
    }

    [Fact]
    public void A_server_push_stream_closes_the_connection()
    {
        StartConnected();
        _peer.OpenUnidirectional([(byte)Http3StreamType.Push, 0x00]);
        Assert.True(Pump(() => _clientSink.IsClosed), "the client's OnClosed");
    }

    [Fact]
    public void A_qpack_stream_from_the_peer_is_accepted_and_ignored()
    {
        WebTransportTransport carrier = StartConnected();
        _peer.OpenUnidirectional([(byte)Http3StreamType.QpackEncoder]);
        _peer.OpenUnidirectional([(byte)Http3StreamType.QpackDecoder]);
        Pump(static () => false, 300);
        Assert.False(_clientSink.IsClosed);
        Assert.True(carrier.IsSessionEstablished);
    }

    [Fact]
    public void A_bidirectional_stream_the_client_did_not_expect_closes_the_connection()
    {
        StartConnected();

        // Towards a client, a server bidirectional stream can only be a WebTransport stream.
        _peer.OpenBidirectional([(byte)Http3FrameType.Data, 0x01, 0x00]);
        Assert.True(Pump(() => _clientSink.IsClosed), "the client's OnClosed");
    }

    [Fact]
    public void A_preamble_split_across_receives_is_reassembled()
    {
        WebTransportTransport carrier = StartConnected();

        // A session id encoded in more bytes than the minimum still parses, and the split is what the slot's
        // partial buffer is for: the link delivers the stream in small pieces.
        Span<byte> bytes = stackalloc byte[32];
        int written = WebTransportFraming.WriteUnidirectionalPreamble(bytes, carrier.SessionId);
        for (int i = 0; i < 4; i++) bytes[written + i] = (byte)(0x70 + i);
        TransportStreamId id = _peer.OpenUnidirectional(bytes.Slice(0, 1));
        Pump(static () => false, 50);
        _peer.SendMore(id, bytes.Slice(1, written - 1 + 4));

        Assert.True(Pump(() => _clientSink.CountOf(RecordedEventKind.PeerStreamStarted) == 1), "OnPeerStreamStarted");
        RecordedEvent peer = _clientSink.OfKind(RecordedEventKind.PeerStreamStarted)[0];
        Assert.True(Pump(() => _clientSink.GetStreamData(peer.StreamId).Length == 4), "the stream's data");
    }

    public void Dispose()
    {
        _client?.Dispose();
        if (!_network.IsDisposed) _network.RunUntilIdle(10_000_000);
        _connector?.Dispose();
        _listener.Dispose();
        _network.Dispose();
        _peer.Dispose();
    }
}
