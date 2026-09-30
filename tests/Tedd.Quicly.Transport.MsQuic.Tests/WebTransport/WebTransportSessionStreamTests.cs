using System.Net;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Http3.WebTransport;
using Tedd.Quicly.Testing.Simulation;
using Tedd.Quicly.Transport.MsQuic.WebTransport;

namespace Tedd.Quicly.Transport.MsQuic.Tests.WebTransport;

/// <summary>
/// The session streams the carrier shows Core, against a peer that is not this library's carrier: a raw HTTP/3 peer that
/// opens its control stream and nothing else of HTTP/3's. The inner connection grants three unidirectional streams for
/// HTTP/3 on top of the session's, so such a peer can open two more session streams than Core was told; the carrier holds
/// those back, unread, until a stream of the session ends — and its mirror of the inner transport's stream table follows
/// the slots in use, whatever <see cref="WebTransportOptions.MaxStreams"/> says.
/// </summary>
public sealed class WebTransportSessionStreamTests : IDisposable
{
    private readonly VirtualClock _clock = new();
    private readonly SimulatedNetwork _network;
    private readonly SimulatedListener _listener;
    private readonly RawHttp3Peer _peer = new();
    private readonly RecordingSink _clientSink;
    private readonly List<string> _errors = [];
    private WebTransportConnector? _connector;
    private ITransport? _client;

    public WebTransportSessionStreamTests()
    {
        _network = new SimulatedNetwork(_clock, seed: 1);
        _clientSink = new RecordingSink(_clock);
        _listener = new SimulatedListener(_network, new IPEndPoint(IPAddress.Loopback, 0));
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

    /// <summary>Connects a carrier client whose inner transport grants <paramref name="innerGrant"/> unidirectional streams by itself.</summary>
    private void Connect(int innerGrant, ushort peerMayOpen = 16, WebTransportOptions? options = null)
    {
        _listener.Start(static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, (ITransport transport, in NewConnectionInfo _) =>
        {
            _peer.Transport = transport;
            transport.UpdatePeerStreamLimits(8, peerMayOpen);
            return _peer;
        });
        options ??= new WebTransportOptions();
        options.Diagnostic = (level, message, _) =>
        {
            if (level == TransportDiagnosticLevel.Error)
            {
                lock (_errors) _errors.Add(message);
            }
        };
        _connector = new WebTransportConnector(new SimulatedConnector(_network, new LinkOptions { DelayMicros = 2_000, PeerUnidiStreams = (ushort)innerGrant }), options);
        _client = _connector.Connect(_listener.LocalEndPoint, "localhost", _clientSink);
        _clientSink.Transport = _client;
        Assert.True(Pump(() => _clientSink.CountOf(RecordedEventKind.Connected) == 1), "the WebTransport session was not established");
    }

    private TransportStreamId OpenSessionStream(byte payload, bool withPayload = true)
    {
        Span<byte> bytes = stackalloc byte[16];
        int length = WebTransportFraming.WriteUnidirectionalPreamble(bytes, _peer.SessionId);
        if (withPayload) bytes[length++] = payload;
        return _peer.OpenUnidirectional(bytes.Slice(0, length));
    }

    /// <summary>The most streams Core had open at once, from the order of its events.</summary>
    private int MostOpenAtOnce()
    {
        int open = 0;
        int most = 0;
        foreach (RecordedEvent e in _clientSink.Events)
        {
            if (e.Kind == RecordedEventKind.PeerStreamStarted) most = Math.Max(most, ++open);
            else if (e.Kind == RecordedEventKind.StreamShutdownComplete) open--;
        }

        return most;
    }

    [Fact]
    public void Streams_Beyond_What_The_Session_Was_Told_Wait_For_A_Stream_To_End_And_Lose_Nothing()
    {
        // Eight streams from the inner transport: three for HTTP/3, five for the session, and the session is told five.
        Connect(innerGrant: 8);
        Assert.Equal(5, _client!.Capabilities.PeerUnidirectionalStreams);

        // The peer uses one stream for HTTP/3 and opens seven for the session, each with one byte and no end yet.
        var streams = new TransportStreamId[7];
        for (int i = 0; i < streams.Length; i++) streams[i] = OpenSessionStream((byte)(0x40 + i));
        Assert.True(Pump(() => _clientSink.CountOf(RecordedEventKind.PeerStreamStarted) == 5), $"Core was shown {_clientSink.CountOf(RecordedEventKind.PeerStreamStarted)} streams");
        _network.Advance(50_000);
        Assert.Equal(5, _clientSink.CountOf(RecordedEventKind.PeerStreamStarted));

        // One of the five ends (the sink closes it): the sixth is shown. Then another, and the seventh.
        _peer.FinishStream(streams[0]);
        Assert.True(Pump(() => _clientSink.CountOf(RecordedEventKind.PeerStreamStarted) == 6), "the sixth stream was not shown after a stream ended");
        _network.Advance(50_000);
        Assert.Equal(6, _clientSink.CountOf(RecordedEventKind.PeerStreamStarted));
        _peer.FinishStream(streams[1]);
        Assert.True(Pump(() => _clientSink.CountOf(RecordedEventKind.PeerStreamStarted) == 7), "the seventh stream was not shown after a stream ended");

        // Everything arrives: seven streams, seven different bytes, each stream's end after it was finished.
        for (int i = 2; i < streams.Length; i++) _peer.FinishStream(streams[i]);
        Assert.True(Pump(() => _clientSink.CountOf(RecordedEventKind.StreamShutdownComplete) == 7), $"{_clientSink.CountOf(RecordedEventKind.StreamShutdownComplete)} of 7 streams completed");
        byte[] received = [.. _clientSink.OfKind(RecordedEventKind.StreamReceived).SelectMany(e => e.Data).Order()];
        Assert.Equal(Enumerable.Range(0x40, 7).Select(b => (byte)b), received);
        Assert.Equal(7, _clientSink.CountOf(RecordedEventKind.StreamPeerSendShutdown));
        Assert.Equal(5, MostOpenAtOnce());
        Assert.Empty(_errors);
    }

    [Fact]
    public void The_Session_May_Raise_Its_Limit_And_The_Waiting_Streams_Are_Shown()
    {
        Connect(innerGrant: 8);
        for (int i = 0; i < 7; i++) OpenSessionStream((byte)(0x50 + i));
        Assert.True(Pump(() => _clientSink.CountOf(RecordedEventKind.PeerStreamStarted) == 5));
        _network.Advance(50_000);
        Assert.Equal(5, _clientSink.CountOf(RecordedEventKind.PeerStreamStarted));

        // What a session does after admission: it asks for its own limit. Seven fit, and the two that waited are shown.
        _client!.UpdatePeerStreamLimits(0, 7);
        Assert.True(Pump(() => _clientSink.CountOf(RecordedEventKind.PeerStreamStarted) == 7), $"Core was shown {_clientSink.CountOf(RecordedEventKind.PeerStreamStarted)} of 7 streams");
        Assert.True(Pump(() => _clientSink.OfKind(RecordedEventKind.StreamReceived).Sum(e => e.Data.Length) == 7));
        Assert.Empty(_errors);
    }

    [Fact]
    public void A_Stream_That_Waited_With_Nothing_But_Its_End_Is_Shown_And_Ends()
    {
        Connect(innerGrant: 8);
        var first = new TransportStreamId[5];
        for (int i = 0; i < first.Length; i++) first[i] = OpenSessionStream((byte)(0x60 + i));
        Assert.True(Pump(() => _clientSink.CountOf(RecordedEventKind.PeerStreamStarted) == 5));

        // A sixth stream with no payload at all: its preamble and its FIN, which the carrier takes in while the stream waits.
        TransportStreamId empty = OpenSessionStream(0, withPayload: false);
        _peer.FinishStream(empty);
        _network.Advance(50_000);
        Assert.Equal(5, _clientSink.CountOf(RecordedEventKind.PeerStreamStarted));
        Assert.Equal(0, _clientSink.CountOf(RecordedEventKind.StreamPeerSendShutdown));

        // A slot frees: the empty stream is shown, its end is indicated once, and it completes.
        _peer.FinishStream(first[0]);
        Assert.True(Pump(() => _clientSink.CountOf(RecordedEventKind.StreamShutdownComplete) == 2), $"{_clientSink.CountOf(RecordedEventKind.StreamShutdownComplete)} streams completed");
        Assert.Equal(6, _clientSink.CountOf(RecordedEventKind.PeerStreamStarted));
        TransportStreamId shown = _clientSink.OfKind(RecordedEventKind.PeerStreamStarted)[5].StreamId;
        RecordedEvent[] ofEmpty = [.. _clientSink.Events.Where(e => e.StreamId == shown)];
        Assert.Equal(
            [RecordedEventKind.PeerStreamStarted, RecordedEventKind.StreamReceived, RecordedEventKind.StreamPeerSendShutdown, RecordedEventKind.StreamShutdownComplete],
            ofEmpty.Select(e => e.Kind));
        Assert.True(ofEmpty[1].Fin && ofEmpty[1].Data.Length == 0, "the empty stream's end was not indicated as an empty receive with FIN");
        Assert.Empty(_errors);
    }

    [Fact]
    public void The_Mirror_Follows_The_Inner_Transports_Slots_Whatever_The_Option_Says()
    {
        // A table option of four, and an inner transport that grants two hundred streams: the mirror grows with the slots
        // the inner transport uses, in both directions, and no stream is dropped for being outside it.
        Connect(innerGrant: 200, peerMayOpen: 400, options: new WebTransportOptions { MaxStreams = 4 });
        Assert.Equal(197, _client!.Capabilities.PeerUnidirectionalStreams);
        for (int i = 0; i < 150; i++) OpenSessionStream((byte)i);
        Assert.True(Pump(() => _clientSink.CountOf(RecordedEventKind.PeerStreamStarted) == 150), $"Core was shown {_clientSink.CountOf(RecordedEventKind.PeerStreamStarted)} of 150 streams");
        Assert.True(Pump(() => _clientSink.OfKind(RecordedEventKind.StreamReceived).Sum(e => e.Data.Length) == 150));

        // Three hundred local streams, each started on its own: their preambles live in storage that is allocated as the
        // slots come into use.
        for (int i = 0; i < 300; i++)
        {
            Assert.Equal(TransportStatus.Success, _client.OpenStream(StreamKind.Unidirectional, (ulong)(0x1000 + i), 32767, out TransportStreamId id));
            Assert.Equal(TransportStatus.Success, _client.StartStream(id));
        }

        int Started() => _clientSink.OfKind(RecordedEventKind.StreamStarted).Count(e => e.Status == TransportStatus.Success);
        Assert.True(Pump(() => Started() == 300), $"{Started()} of 300 local streams started");

        // The preamble each of them sent is the carrier's own send: its completion is not Core's.
        _network.Advance(50_000);
        Assert.Equal(0, _clientSink.CountOf(RecordedEventKind.StreamSendCompleted));
        Assert.Empty(_errors);
    }

    public void Dispose()
    {
        _client?.Dispose();
        _peer.Transport?.Dispose();
        _network.RunUntilIdle(10_000_000);
        _connector?.Dispose();
        _listener.Dispose();
        _network.Dispose();
        _peer.Dispose();
    }
}
