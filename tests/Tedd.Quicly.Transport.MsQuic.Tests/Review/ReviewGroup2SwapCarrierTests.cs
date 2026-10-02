using System.Net;
using Tedd.Quicly.Core.Time;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Http3.WebTransport;
using Tedd.Quicly.Testing.Simulation;
using Tedd.Quicly.Transport.MsQuic.Tests.WebTransport;
using Tedd.Quicly.Transport.MsQuic.WebTransport;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Review;

/// <summary>
/// Second adversarial review of fix/group-stream-receive-limit, lens "swap": the grant the WebTransport carrier reports
/// (<c>inner - 3</c>) against a peer that is not this library's carrier.
/// </summary>
public sealed class ReviewGroup2SwapCarrierTests : IDisposable
{
    private readonly VirtualClock _clock = new();
    private readonly SimulatedNetwork _network;
    private readonly SimulatedListener _listener;
    private readonly RawHttp3Peer _peer = new();
    private readonly RecordingSink _clientSink;
    private WebTransportConnector? _connector;
    private ITransport? _client;

    public ReviewGroup2SwapCarrierTests()
    {
        _network = new SimulatedNetwork(_clock, seed: 1);
        _clientSink = new RecordingSink(_clock) { ReceiveHandler = static (_, _, _, _) => ReceiveResult.PendingAfter(0) };
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

    /// <summary>
    /// The carrier subtracts three from the inner connection's grant because "three of those streams are HTTP/3's own
    /// (control and the two QPACK streams)". That is what this library's carrier opens. RFC 9114 requires only the control
    /// stream; the QPACK streams are optional for an endpoint that does not use the dynamic table (RFC 9204 §4.2), and this
    /// carrier itself advertises a table capacity of 0. A peer that opens only its control stream can have two more session
    /// streams open than the carrier reported: the session sized its receive state for the reported number (the engine's
    /// own words: "Without one the transport admitted a stream beyond what it reported, which breaks its contract;
    /// resetting it is all that is left"), so a late receiver resets those two after their sender completed them.
    /// </summary>
    [Fact]
    public void The_Carrier_Admits_No_More_Session_Streams_Than_The_Grant_It_Reports()
    {
        _listener.Start(static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, (ITransport transport, in NewConnectionInfo _) =>
        {
            _peer.Transport = transport;
            // Room for the client's three HTTP/3 streams and its CONNECT stream.
            transport.UpdatePeerStreamLimits(8, 16);
            return _peer;
        });

        // The client's inner transport grants its peer eight unidirectional streams by itself: three for HTTP/3, five for the session.
        _connector = new WebTransportConnector(new SimulatedConnector(_network, new LinkOptions { DelayMicros = 2_000, PeerUnidiStreams = 8 }));
        _client = _connector.Connect(_listener.LocalEndPoint, "localhost", _clientSink);
        _clientSink.Transport = _client;
        Assert.True(Pump(() => _clientSink.CountOf(RecordedEventKind.Connected) == 1), "the WebTransport session was not established");
        int reported = _clientSink.OfKind(RecordedEventKind.Connected)[0].Capabilities.PeerUnidirectionalStreams;
        Assert.Equal(5, reported);
        Assert.Equal(5, _client.Capabilities.PeerUnidirectionalStreams);

        // The raw peer opened its control stream and nothing else. It now opens session streams until the transport refuses.
        Span<byte> bytes = stackalloc byte[16];
        int preamble = WebTransportFraming.WriteUnidirectionalPreamble(bytes, _peer.SessionId);
        bytes[preamble] = 0x2A;
        for (int i = 0; i < 8; i++)
        {
            _peer.OpenUnidirectional(bytes.Slice(0, preamble + 1));
        }

        Pump(() => _clientSink.CountOf(RecordedEventKind.PeerStreamStarted) >= 8, 500);
        int admitted = _clientSink.CountOf(RecordedEventKind.PeerStreamStarted);
        Assert.True(admitted <= reported,
            $"the carrier reported a grant of {reported} session streams and handed its sink {admitted} that are open at once");
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
