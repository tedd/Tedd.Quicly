using System.Net;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Certificates;
using Tedd.Quicly.Testing.Conformance;
using Tedd.Quicly.Testing.Simulation;
using Tedd.Quicly.Transport.MsQuic.WebTransport;

namespace Tedd.Quicly.Transport.MsQuic.Tests.WebTransport;

/// <summary>
/// The carrier over real MsQuic loopback, built the way an application builds it
/// (<see cref="WebTransportListener.CreateMsQuic"/> and <see cref="WebTransportConnector.CreateMsQuic"/>): a real TLS 1.3
/// handshake with the <c>h3</c> ALPN, then the HTTP/3 and WebTransport exchange, then data in both directions.
/// </summary>
[Collection(MsQuicCollection.Name)]
public sealed unsafe class WebTransportLoopbackTests : IDisposable
{
    private readonly TestRegistration _registration = new("quicly-webtransport-tests");
    private readonly X509Certificate2 _certificate = TestCertificates.CreateSelfSigned("CN=localhost", TimeSpan.FromDays(1), ecdsa: true, "localhost");
    private readonly List<IDisposable> _owned = [];
    private ITransport? _client;
    private ITransport? _server;

    private (RecordingSink Client, RecordingSink Server) Connect(WebTransportOptions? options = null)
    {
        var clientSink = new RecordingSink();
        var serverSink = new RecordingSink();

        WebTransportListener listener = WebTransportListener.CreateMsQuic(
            new IPEndPoint(IPAddress.Loopback, 0),
            _certificate,
            new MsQuicTransportOptions { ServerPeerBidiStreamCount = 16, ServerPeerUnidiStreamCount = 16 },
            options,
            _registration.Registration);
        _owned.Add(listener);
        listener.Start(static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, (ITransport transport, in NewConnectionInfo _) =>
        {
            _server = transport;
            return serverSink;
        });

        WebTransportConnector connector = WebTransportConnector.CreateMsQuic(
            new MsQuicTransportOptions
            {
                ServerCertificateValidation = ServerCertificateValidationMode.PinnedSpki,
                PinnedSpkiSha256 = [SpkiPin.Compute(_certificate)],
                ClientPeerBidiStreamCount = 16,
                ClientPeerUnidiStreamCount = 16,
            },
            options,
            _registration.Registration);
        _owned.Add(connector);

        _client = connector.Connect(listener.LocalEndPoint, "localhost", clientSink);
        Assert.True(Wait(() => clientSink.CountOf(RecordedEventKind.Connected) == 1 && serverSink.CountOf(RecordedEventKind.Connected) == 1),
            "both ends established the WebTransport session over loopback");
        return (clientSink, serverSink);
    }

    private static bool Wait(Func<bool> condition)
    {
        long deadline = Environment.TickCount64 + (long)TestTimeouts.Default.TotalMilliseconds;
        while (!condition())
        {
            if (Environment.TickCount64 >= deadline) return false;
            Thread.Sleep(5);
        }

        return true;
    }

    [Fact]
    public void A_session_is_established_over_real_quic_with_the_h3_alpn()
    {
        (RecordingSink clientSink, RecordingSink serverSink) = Connect();

        RecordedEvent connected = clientSink.OfKind(RecordedEventKind.Connected)[0];
        Assert.Equal("h3", connected.Alpn);
        Assert.Equal("h3", serverSink.OfKind(RecordedEventKind.Connected)[0].Alpn);

        var client = (WebTransportTransport)_client!;
        var server = (WebTransportTransport)_server!;
        Assert.Equal(client.SessionId, server.SessionId);
        Assert.True(client.IsSessionEstablished && server.IsSessionEstablished);
        Assert.Equal(1ul, client.PeerSettings.EnableWebTransport);
    }

    [Fact]
    public void A_datagram_makes_the_round_trip_with_its_prefix_stripped()
    {
        (_, RecordingSink serverSink) = Connect();

        using var buffer = new NativeBuffer(64);
        for (int i = 0; i < 64; i++) buffer.Pointer[i] = (byte)(i * 7);
        using var segments = new NativeSegments(1);
        segments.Set(0, buffer.Segment(0, 64));
        Assert.Equal(TransportStatus.Success, _client!.SendDatagram(segments.At(0), 1, 1, TransportSendFlags.None));

        Assert.True(Wait(() => serverSink.CountOf(RecordedEventKind.DatagramReceived) == 1), "the datagram at the server");
        byte[] received = serverSink.OfKind(RecordedEventKind.DatagramReceived)[0].Data;
        Assert.Equal(64, received.Length);
        for (int i = 0; i < 64; i++) Assert.Equal((byte)(i * 7), received[i]);
    }

    [Fact]
    public void A_stream_makes_the_round_trip_with_its_preamble_stripped()
    {
        (_, RecordingSink serverSink) = Connect();

        using var buffer = new NativeBuffer(256);
        for (int i = 0; i < 256; i++) buffer.Pointer[i] = (byte)(255 - i);
        using var segments = new NativeSegments(2);
        segments.Set(0, buffer.Segment(0, 128));
        segments.Set(1, buffer.Segment(128, 128));
        Assert.Equal(TransportStatus.Success, _client!.OpenStream(StreamKind.Unidirectional, 5, 32767, out TransportStreamId id));
        Assert.Equal(TransportStatus.Success, _client.SendStream(id, segments.At(0), 2, 6, TransportSendFlags.Start | TransportSendFlags.Fin));

        Assert.True(Wait(() => serverSink.CountOf(RecordedEventKind.PeerStreamStarted) == 1), "OnPeerStreamStarted at the server");
        RecordedEvent peer = serverSink.OfKind(RecordedEventKind.PeerStreamStarted)[0];
        Assert.Equal(StreamKind.Unidirectional, peer.StreamKind);
        Assert.True(Wait(() => serverSink.GetStreamData(peer.StreamId).Length == 256), "all 256 bytes at the server");

        byte[] received = serverSink.GetStreamData(peer.StreamId);
        for (int i = 0; i < 256; i++) Assert.Equal((byte)(255 - i), received[i]);
    }

    [Fact]
    public void A_close_carries_the_application_code_to_the_peer()
    {
        (_, RecordingSink serverSink) = Connect();

        _client!.Close(0x10, "bulk canceled"u8);
        Assert.True(Wait(() => serverSink.IsClosed), "the server's OnClosed");
        RecordedEvent closed = serverSink.OfKind(RecordedEventKind.Closed)[0];
        Assert.Equal(TransportCloseReason.Peer, closed.CloseReason);
        Assert.Equal(0x10ul, closed.ErrorCode);
    }

    [Fact]
    public void A_server_on_another_path_refuses_the_session()
    {
        var clientSink = new RecordingSink();
        WebTransportListener listener = WebTransportListener.CreateMsQuic(
            new IPEndPoint(IPAddress.Loopback, 0),
            _certificate,
            null,
            new WebTransportOptions { Path = "/arena" },
            _registration.Registration);
        _owned.Add(listener);
        listener.Start(static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, (ITransport transport, in NewConnectionInfo _) =>
        {
            _server = transport;
            return new RecordingSink();
        });

        WebTransportConnector connector = WebTransportConnector.CreateMsQuic(
            new MsQuicTransportOptions
            {
                ServerCertificateValidation = ServerCertificateValidationMode.PinnedSpki,
                PinnedSpkiSha256 = [SpkiPin.Compute(_certificate)],
            },
            new WebTransportOptions { Path = "/elsewhere" },
            _registration.Registration);
        _owned.Add(connector);

        _client = connector.Connect(listener.LocalEndPoint, "localhost", clientSink);
        Assert.True(Wait(() => clientSink.IsClosed), "the client's OnClosed");
        Assert.Equal(0, clientSink.CountOf(RecordedEventKind.Connected));
    }

    public void Dispose()
    {
        _client?.Dispose();
        _server?.Dispose();
        for (int i = _owned.Count - 1; i >= 0; i--) _owned[i].Dispose();
        _certificate.Dispose();
        _registration.Dispose();
    }
}
