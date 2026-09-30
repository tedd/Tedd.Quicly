using System.Diagnostics;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Certificates;
using Tedd.Quicly.Testing.Conformance;
using Tedd.Quicly.Testing.Simulation;
using Tedd.Quicly.Transport.MsQuic.Tests.Transport;
using Tedd.Quicly.Transport.MsQuic.Tests.WebTransport;
using Tedd.Quicly.Transport.MsQuic.WebTransport;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Review;

/// <summary>
/// Second adversarial review of fix/group-stream-receive-limit, lens "swap" (commits b55144c and cd7bdeb), transport half:
/// the ordering the session's re-size in <c>OnConnected</c> relies on, pinned per transport; whether the grant a transport
/// reports (<see cref="TransportCapabilities.PeerUnidirectionalStreams"/>) is what its peer can really open; and the
/// stream-table defaults.
/// </summary>
[Collection(MsQuicCollection.Name)]
public unsafe class ReviewGroup2SwapTransportTests
{
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(2, "a", ChannelMode.UnreliableUnordered)
        .Add(10, "stream", ChannelMode.ReliableOrdered)
        .Add(11, "group", ChannelMode.ReliableUnordered)
        .Build();

    // ------------------------------------------------------------------ "no peer stream precedes OnConnected"

    /// <summary>
    /// Opens a unidirectional stream and sends on it from inside <c>OnConnected</c>, the earliest moment an application can:
    /// the peer's stream then travels as close to the handshake's last flight as the transport allows.
    /// </summary>
    private sealed class Eager(TransportSegment* segment) : SinkBase
    {
        public volatile ITransport? Transport;
        public TransportStatus Opened = TransportStatus.InvalidState;
        public TransportStatus Sent = TransportStatus.InvalidState;
        public volatile bool Done;

        public override void OnConnected(in TransportConnectedInfo info)
        {
            // A real transport may raise this before the test has stored the transport it was handed.
            SpinWait spin = default;
            long deadline = Environment.TickCount64 + 5_000;
            while (Transport is null && Environment.TickCount64 < deadline)
            {
                spin.SpinOnce();
            }

            if (Transport is { } transport)
            {
                Opened = transport.OpenStream(StreamKind.Unidirectional, 0x51, 32767, out TransportStreamId id);
                if (Opened == TransportStatus.Success)
                {
                    Sent = transport.SendStream(id, segment, 1, 0x52, TransportSendFlags.Start | TransportSendFlags.Fin);
                }
            }

            Done = true;
        }
    }

    /// <summary>
    /// The scenario: both ends send a stream from inside their <c>OnConnected</c>, and the client also tries one before the
    /// handshake has completed. On either end, no event of a peer stream may be recorded before <c>Connected</c>: the session
    /// sizes its per-stream receive state there (PeerCore.SetTransportPeerStreams).
    /// </summary>
    private static void PeerStreamsFollowOnConnected(ITransportTestHarness harness, NativeBuffer data, NativeSegments segments)
    {
        Eager clientEager = new(segments.At(0));
        Eager serverEager = new(segments.At(1));
        RecordingSink clientSink = new(null, clientEager) { AutoCloseStreams = true };
        RecordingSink serverSink = new(null, serverEager) { AutoCloseStreams = true };
        ITransport? server = null;
        ITransport client = harness.Connect(clientSink, static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, (ITransport transport, in NewConnectionInfo _) =>
        {
            serverEager.Transport = transport;
            serverSink.Transport = transport;
            Volatile.Write(ref server, transport);
            return serverSink;
        }, new ConformancePairOptions());
        clientSink.Transport = client;

        // Before the handshake is over: whatever the transport answers, the peer must not see the stream before it connected.
        if (client.OpenStream(StreamKind.Unidirectional, 0x61, 32767, out TransportStreamId early) == TransportStatus.Success)
        {
            client.SendStream(early, segments.At(2), 1, 0x62, TransportSendFlags.Start | TransportSendFlags.Fin);
        }

        client.UpdatePeerStreamLimits(16, 16);
        clientEager.Transport = client;
        Assert.True(harness.Pump(() => Volatile.Read(ref server) is not null, harness.DefaultTimeout), $"[{harness.Name}] the listener never accepted");
        server!.UpdatePeerStreamLimits(16, 16);

        bool arrived = harness.Pump(() => clientEager.Done && serverEager.Done
            && clientSink.CountOf(RecordedEventKind.PeerStreamStarted) > 0 && serverSink.CountOf(RecordedEventKind.PeerStreamStarted) > 0
            && clientSink.CountOf(RecordedEventKind.StreamPeerSendShutdown) > 0 && serverSink.CountOf(RecordedEventKind.StreamPeerSendShutdown) > 0,
            harness.DefaultTimeout);
        Assert.True(arrived,
            $"[{harness.Name}] the streams sent from OnConnected did not arrive (client: open {clientEager.Opened}, send {clientEager.Sent}; server: open {serverEager.Opened}, send {serverEager.Sent})"
            + Environment.NewLine + Describe("client", clientSink) + Describe("server", serverSink));
        RequirePeerStreamsAfterConnected(harness, "client", clientSink);
        RequirePeerStreamsAfterConnected(harness, "server", serverSink);

        // The capabilities of OnConnected are the transport's: the grant the session sizes itself for does not change later.
        Assert.Equal(clientSink.OfKind(RecordedEventKind.Connected)[0].Capabilities.PeerUnidirectionalStreams, client.Capabilities.PeerUnidirectionalStreams);
        Assert.Equal(serverSink.OfKind(RecordedEventKind.Connected)[0].Capabilities.PeerUnidirectionalStreams, server.Capabilities.PeerUnidirectionalStreams);
    }

    private static void RequirePeerStreamsAfterConnected(ITransportTestHarness harness, string side, RecordingSink sink)
    {
        IReadOnlyList<RecordedEvent> events = sink.Events;
        HashSet<TransportStreamId> peerStreams = [];
        bool connected = false;
        for (int i = 0; i < events.Count; i++)
        {
            RecordedEvent e = events[i];
            if (e.Kind == RecordedEventKind.Connected)
            {
                Assert.False(connected, $"[{harness.Name}] {side}: OnConnected was raised twice" + Environment.NewLine + Describe(side, sink));
                connected = true;
                continue;
            }

            if (e.Kind == RecordedEventKind.PeerStreamStarted)
            {
                peerStreams.Add(e.StreamId);
            }

            bool ofPeerStream = e.Kind == RecordedEventKind.PeerStreamStarted
                || (e.Kind is RecordedEventKind.StreamReceived or RecordedEventKind.StreamPeerSendShutdown or RecordedEventKind.StreamAborted
                    or RecordedEventKind.StreamShutdownComplete && peerStreams.Contains(e.StreamId));
            Assert.True(connected || !ofPeerStream,
                $"[{harness.Name}] {side}: event {i} ({e}) of a peer stream precedes OnConnected" + Environment.NewLine + Describe(side, sink));
        }

        Assert.True(connected, $"[{harness.Name}] {side}: never connected");
    }

    private static string Describe(string side, RecordingSink sink)
    {
        System.Text.StringBuilder text = new();
        text.Append(side).AppendLine(" events:");
        foreach (RecordedEvent e in sink.Events.Take(40))
        {
            text.Append("  ").AppendLine(e.ToString());
        }

        return text.ToString();
    }

    private static void RunOrdering(Func<ITransportTestHarness> create, int pairs)
    {
        NativeBuffer data = new(64);
        data.Fill(5);
        NativeSegments segments = new(3);
        for (int i = 0; i < 3; i++)
        {
            segments.Set(i, data.Segment(i * 16, 16));
        }

        try
        {
            ITransportTestHarness harness = create();
            try
            {
                for (int pair = 0; pair < pairs; pair++)
                {
                    PeerStreamsFollowOnConnected(harness, data, segments);
                }
            }
            finally
            {
                // Closes every transport: only then may the payloads go.
                harness.Dispose();
            }
        }
        finally
        {
            segments.Dispose();
            data.Dispose();
        }
    }

    [Fact]
    public void On_The_Simulator_No_Peer_Stream_Precedes_OnConnected() =>
        RunOrdering(static () => new SimulatedTransportHarness(new LinkOptions { DelayMicros = 2_000, PeerUnidiStreams = 16 }), pairs: 4);

    [Fact]
    public void On_The_Simulator_With_Loss_And_Jitter_No_Peer_Stream_Precedes_OnConnected() =>
        RunOrdering(static () => new SimulatedTransportHarness(
            new LinkOptions { DelayMicros = 5_000, JitterMicros = 4_000, StreamLossPercent = 20, PeerUnidiStreams = 16 }, seed: 9), pairs: 8);

    [Fact]
    public void On_The_WebTransport_Carrier_No_Peer_Stream_Precedes_OnConnected() =>
        RunOrdering(static () => new WebTransportSimulatedHarness(new LinkOptions { DelayMicros = 2_000, PeerUnidiStreams = 19 }), pairs: 4);

    [Fact]
    public void On_The_WebTransport_Carrier_With_Loss_And_Jitter_No_Peer_Stream_Precedes_OnConnected() =>
        RunOrdering(static () => new WebTransportSimulatedHarness(
            new LinkOptions { DelayMicros = 5_000, JitterMicros = 4_000, StreamLossPercent = 20, PeerUnidiStreams = 19 }, seed: 9), pairs: 8);

    [Fact]
    public void On_MsQuic_No_Peer_Stream_Precedes_OnConnected()
    {
        if (!MsQuicApi.TryGetInstance(out _, out _))
        {
            Assert.Skip("MsQuic is not available on this host.");
        }

        MsQuicTransportHarness? created = null;
        RunOrdering(() => created = new MsQuicTransportHarness(), pairs: 20);
        Assert.Null(created!.CleanupError);
        Assert.Equal(0, created.SinkExceptionTotal);
    }

    // ------------------------------------------------------------------ the grant a transport reports

    /// <summary>
    /// What an MsQuic transport reports as its own grant against what MsQuic lets the peer do: the peer opens more streams
    /// than the grant before anyone calls <c>UpdatePeerStreamLimits</c>, and exactly the reported number start. The options
    /// value, a <c>Configure*Settings</c> hook that lowers or raises it, and a hook that clears it (MsQuic's own default) are
    /// all covered, for both roles.
    /// </summary>
    [Theory]
    [InlineData(true, 5, -1)]
    [InlineData(true, 1024, 7)]
    [InlineData(true, 3, 40)]
    [InlineData(true, 9, -2)]
    [InlineData(false, 6, -1)]
    [InlineData(false, 0, 11)]
    [InlineData(false, 9, -2)]
    public void The_Grant_An_MsQuic_Transport_Reports_Is_What_Its_Peer_Can_Open(bool clientGrants, int option, int hook)
    {
        if (!MsQuicApi.TryGetInstance(out _, out _))
        {
            Assert.Skip("MsQuic is not available on this host.");
        }

        int expected = hook == -1 ? option : hook == -2 ? 0 : hook;
        using MsQuicTransportHarness harness = new();
        Action<MsQuicSettings>? configure = hook == -1 ? null : hook == -2 ? s => s.PeerUnidiStreamCount = null : s => s.PeerUnidiStreamCount = (ushort)hook;
        MsQuicTransportOptions serverOptions = harness.ServerOptions();
        MsQuicTransportOptions clientOptions = harness.ClientOptions();
        if (clientGrants)
        {
            clientOptions.ClientPeerUnidiStreamCount = (ushort)option;
            clientOptions.ConfigureClientSettings = configure;
        }
        else
        {
            serverOptions.ServerPeerUnidiStreamCount = (ushort)option;
            serverOptions.ConfigureServerSettings = configure;
        }

        RecordingSink clientSink = new();
        RecordingSink serverSink = new();
        MsQuicTransport? server = null;
        MsQuicTransportListener listener = harness.StartListener(serverOptions, static (in NewConnectionInfo _) => PreHandshakeDecision.Accept,
            (ITransport transport, in NewConnectionInfo _) =>
            {
                serverSink.Transport = transport;
                Volatile.Write(ref server, harness.Track((MsQuicTransport)transport));
                return serverSink;
            });
        MsQuicTransport client = harness.Track(harness.CreateConnector(clientOptions).Connect(listener.LocalEndPoint, "localhost", clientSink));
        clientSink.Transport = client;
        Assert.True(clientSink.WaitForCount(RecordedEventKind.Connected, 1, TestTimeouts.Default) && serverSink.WaitForCount(RecordedEventKind.Connected, 1, TestTimeouts.Default));

        MsQuicTransport granting = clientGrants ? client : server!;
        RecordingSink grantingSink = clientGrants ? clientSink : serverSink;
        MsQuicTransport opening = clientGrants ? server! : client;
        RecordingSink openingSink = clientGrants ? serverSink : clientSink;
        openingSink.AutoCloseStreams = false;
        Assert.Equal(expected, granting.Capabilities.PeerUnidirectionalStreams);
        Assert.Equal(expected, grantingSink.OfKind(RecordedEventKind.Connected)[0].Capabilities.PeerUnidirectionalStreams);

        (int started, int refused) = OpenStreams(opening, openingSink, expected + 4);
        Assert.True(started == expected,
            $"the {(clientGrants ? "client" : "server")} reports a grant of {expected} unidirectional streams (option {option}, hook {hook}), and its peer started {started} and was refused {refused}");
    }

    /// <summary>Opens and starts <paramref name="count"/> unidirectional streams and waits for every start to be answered.</summary>
    private static (int Started, int Refused) OpenStreams(ITransport transport, RecordingSink sink, int count)
    {
        int before = sink.CountOf(RecordedEventKind.StreamStarted);
        int synchronous = 0;
        int asynchronous = 0;
        for (int i = 0; i < count; i++)
        {
            Assert.Equal(TransportStatus.Success, transport.OpenStream(StreamKind.Unidirectional, (ulong)(0x100 + i), 32767, out TransportStreamId id));
            TransportStatus status = transport.StartStream(id);
            if (status == TransportStatus.Success)
            {
                asynchronous++;
            }
            else
            {
                Assert.Equal(TransportStatus.StreamLimitReached, status);
                synchronous++;
            }
        }

        Assert.True(sink.WaitForCount(RecordedEventKind.StreamStarted, before + asynchronous, TestTimeouts.Default), "not every start was answered");
        int started = 0;
        int refused = synchronous;
        foreach (RecordedEvent e in sink.OfKind(RecordedEventKind.StreamStarted).Skip(before))
        {
            if (e.Status == TransportStatus.Success)
            {
                started++;
            }
            else
            {
                refused++;
            }
        }

        return (started, refused);
    }

    /// <summary>A WebTransport session over real MsQuic loopback, built the way an application builds it.</summary>
    private sealed class WebTransportLoopback : IDisposable
    {
        private readonly TestRegistration _registration = new("quicly-review-webtransport");
        private readonly X509Certificate2 _certificate = TestCertificates.CreateSelfSigned("CN=localhost", TimeSpan.FromDays(1), ecdsa: true, "localhost");
        private readonly List<IDisposable> _owned = [];
        private readonly List<ITransport> _transports = [];

        public MsQuicRegistration Registration => _registration.Registration;

        public MsQuicTransportOptions ClientOptions() => new()
        {
            ServerCertificateValidation = ServerCertificateValidationMode.PinnedSpki,
            PinnedSpkiSha256 = [SpkiPin.Compute(_certificate)],
        };

        public WebTransportListener Listen(MsQuicTransportOptions? transport, AcceptCallback accept, WebTransportOptions? options = null)
        {
            WebTransportListener listener = WebTransportListener.CreateMsQuic(new IPEndPoint(IPAddress.Loopback, 0), _certificate, transport, options, Registration);
            _owned.Add(listener);
            listener.Start(static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, (ITransport carrier, in NewConnectionInfo info) =>
            {
                lock (_transports) _transports.Add(carrier);
                return accept(carrier, in info);
            });
            return listener;
        }

        public T Own<T>(T owned) where T : IDisposable
        {
            _owned.Add(owned);
            return owned;
        }

        public ITransport Track(ITransport transport)
        {
            lock (_transports) _transports.Add(transport);
            return transport;
        }

        public void Dispose()
        {
            ITransport[] transports;
            lock (_transports) transports = [.. _transports];
            foreach (ITransport transport in transports) transport.Dispose();
            for (int i = _owned.Count - 1; i >= 0; i--) _owned[i].Dispose();
            _certificate.Dispose();
            _registration.Dispose();
        }
    }

    private sealed class TrackingConnector(WebTransportLoopback loopback, ITransportConnector inner) : ITransportConnector
    {
        public ITransport Connect(EndPoint endpoint, string? serverName, ITransportSink sink) => loopback.Track(inner.Connect(endpoint, serverName, sink));
    }

    /// <summary>
    /// The carrier reports the inner grant less HTTP/3's three streams. Over MsQuic, with both ends this library's carrier
    /// (which opens all three), the peer can open exactly the reported number of session streams, in both roles.
    /// </summary>
    [Theory]
    [InlineData(true, 5)]
    [InlineData(false, 6)]
    public void The_Grant_The_WebTransport_Carrier_Reports_Is_What_Its_Peer_Can_Open_Over_MsQuic(bool clientGrants, int option)
    {
        if (!MsQuicApi.TryGetInstance(out _, out _))
        {
            Assert.Skip("MsQuic is not available on this host.");
        }

        using WebTransportLoopback loopback = new();
        RecordingSink clientSink = new();
        RecordingSink serverSink = new();
        ITransport? server = null;
        MsQuicTransportOptions serverOptions = new() { ServerPeerBidiStreamCount = 1, ServerPeerUnidiStreamCount = (ushort)(clientGrants ? 16 : option) };
        MsQuicTransportOptions clientOptions = loopback.ClientOptions();
        clientOptions.ClientPeerUnidiStreamCount = (ushort)(clientGrants ? option : 16);
        WebTransportListener listener = loopback.Listen(serverOptions, (ITransport carrier, in NewConnectionInfo _) =>
        {
            serverSink.Transport = carrier;
            Volatile.Write(ref server, carrier);
            return serverSink;
        });
        WebTransportConnector connector = loopback.Own(WebTransportConnector.CreateMsQuic(clientOptions, null, loopback.Registration));
        ITransport client = loopback.Track(connector.Connect(listener.LocalEndPoint, "localhost", clientSink));
        clientSink.Transport = client;
        Assert.True(clientSink.WaitForCount(RecordedEventKind.Connected, 1, TestTimeouts.Default) && serverSink.WaitForCount(RecordedEventKind.Connected, 1, TestTimeouts.Default),
            "the WebTransport session was not established");

        ITransport granting = clientGrants ? client : server!;
        RecordingSink grantingSink = clientGrants ? clientSink : serverSink;
        ITransport opening = clientGrants ? server! : client;
        RecordingSink openingSink = clientGrants ? serverSink : clientSink;
        openingSink.AutoCloseStreams = false;
        Assert.Equal(option, granting.Capabilities.PeerUnidirectionalStreams);
        Assert.Equal(option, grantingSink.OfKind(RecordedEventKind.Connected)[0].Capabilities.PeerUnidirectionalStreams);
        (int started, int refused) = OpenStreams(opening, openingSink, option + 4);
        Assert.True(started == option,
            $"the {(clientGrants ? "client" : "server")} carrier reports a grant of {option} session streams, and its peer started {started} and was refused {refused}");
    }

    // ------------------------------------------------------------------ the stream tables

    /// <summary>
    /// <c>WebTransportOptions</c>: "<c>MaxStreams</c> must be at least the <c>MsQuicTransportOptions.MaxStreams</c> of the
    /// transport the carrier runs over, because the carrier mirrors the inner transport's stream slots". The branch raised
    /// the transport's default to 2 048 and left the carrier's at 1 024, so the two defaults no longer satisfy the rule:
    /// a carrier built with its public constructor over a default MsQuic connector or listener has a table half the size of
    /// the one it mirrors, and a peer stream in an inner slot beyond it is dropped without a callback.
    /// </summary>
    [Fact]
    public void The_Default_Carrier_Stream_Table_Covers_The_Default_Transport_Stream_Table()
    {
        int carrier = new WebTransportOptions().MaxStreams;
        int transport = new MsQuicTransportOptions().MaxStreams;
        Assert.True(carrier >= transport,
            $"WebTransportOptions.MaxStreams defaults to {carrier}, MsQuicTransportOptions.MaxStreams to {transport}: the carrier must be at least the inner transport's");
    }

    /// <summary>
    /// The same through the wire. The client is a carrier built with the public constructor and default
    /// <see cref="WebTransportOptions"/> over a default MsQuic connector (apart from the <c>h3</c> ALPN, the three HTTP/3
    /// streams and the certificate pin): it grants its server 1 024 session streams and says so. The server opens that many
    /// and keeps them open, as the streams of a late receiver are. Every one of them must reach the client's sink.
    /// </summary>
    [Fact]
    public void A_Default_Carrier_Over_A_Default_Transport_Sees_Every_Stream_It_Granted() => HoldEveryGrantedStream(carrierMaxStreams: null);

    /// <summary>The control: with the carrier's table as large as the transport's, the same run passes.</summary>
    [Fact]
    public void A_Carrier_As_Large_As_The_Default_Transport_Sees_Every_Stream_It_Granted() =>
        HoldEveryGrantedStream(carrierMaxStreams: new MsQuicTransportOptions().MaxStreams);

    private static void HoldEveryGrantedStream(int? carrierMaxStreams)
    {
        if (!MsQuicApi.TryGetInstance(out _, out _))
        {
            Assert.Skip("MsQuic is not available on this host.");
        }

        using WebTransportLoopback loopback = new();
        List<string> diagnostics = [];
        RecordingSink clientSink = new() { ReceiveHandler = static (_, _, _, _) => ReceiveResult.PendingAfter(0) };
        RecordingSink serverSink = new() { AutoCloseStreams = false };
        ITransport? server = null;
        WebTransportListener listener = loopback.Listen(null, (ITransport carrier, in NewConnectionInfo _) =>
        {
            serverSink.Transport = carrier;
            Volatile.Write(ref server, carrier);
            return serverSink;
        });

        MsQuicTransportOptions inner = loopback.ClientOptions();
        inner.Alpns = [WebTransportOptions.Alpn];
        inner.ClientPeerUnidiStreamCount = (ushort)(inner.ClientPeerUnidiStreamCount + 3);
        WebTransportOptions carrierOptions = new() { Diagnostic = (_, message, _) => { lock (diagnostics) diagnostics.Add(message); } };
        if (carrierMaxStreams is int table)
        {
            carrierOptions.MaxStreams = table;
        }
        WebTransportConnector connector = loopback.Own(new WebTransportConnector(
            loopback.Own(new MsQuicTransportConnector(inner, loopback.Registration)), carrierOptions));
        ITransport client = loopback.Track(connector.Connect(listener.LocalEndPoint, "localhost", clientSink));
        clientSink.Transport = client;
        Assert.True(clientSink.WaitForCount(RecordedEventKind.Connected, 1, TestTimeouts.Default) && serverSink.WaitForCount(RecordedEventKind.Connected, 1, TestTimeouts.Default),
            "the WebTransport session was not established");
        int granted = client.Capabilities.PeerUnidirectionalStreams;
        Assert.Equal(1024, granted);

        NativeBuffer data = new(16);
        NativeSegments segments = new(1);
        segments.Set(0, data.Segment(0, 16));
        try
        {
            int accepted = 0;
            for (int i = 0; i < granted; i++)
            {
                Assert.Equal(TransportStatus.Success, server!.OpenStream(StreamKind.Unidirectional, (ulong)(0x1000 + i), 32767, out TransportStreamId id));

                // The carrier takes a bounded number of stream starts at a time (WebTransportOptions.MaxConcurrentStreamStarts).
                long deadline = Environment.TickCount64 + 5_000;
                TransportStatus status;
                while ((status = server.SendStream(id, segments.At(0), 1, (ulong)(0x2000 + i), TransportSendFlags.Start)) != TransportStatus.Success
                    && Environment.TickCount64 < deadline)
                {
                    Thread.Sleep(1);
                }

                Assert.True(status == TransportStatus.Success, $"the start of stream {i} was refused with {status}");
                accepted++;
            }

            Assert.True(serverSink.WaitForCount(RecordedEventKind.StreamStarted, accepted, TestTimeouts.Default), "not every start was answered");
            int started = serverSink.OfKind(RecordedEventKind.StreamStarted).Count(e => e.Status == TransportStatus.Success);
            Assert.Equal(granted, started);

            clientSink.WaitForCount(RecordedEventKind.PeerStreamStarted, granted, TimeSpan.FromSeconds(5));
            int seen = clientSink.CountOf(RecordedEventKind.PeerStreamStarted);
            string[] said;
            lock (diagnostics) said = [.. diagnostics.Distinct().Take(3)];
            Assert.True(seen == granted,
                $"the client carrier granted {granted} session streams, its server started {started}, and the client's sink was told of {seen}"
                + (said.Length > 0 ? "; the carrier said: " + string.Join(" | ", said) : string.Empty));
        }
        finally
        {
            client.Dispose();
            server?.Dispose();

            // The payload and its segment array may go only once the sending end reported its close (left allocated otherwise).
            clientSink.WaitForCount(RecordedEventKind.Closed, 1, TestTimeouts.Default);
            if (server is not null && serverSink.WaitForCount(RecordedEventKind.Closed, 1, TestTimeouts.Default))
            {
                segments.Dispose();
                data.Dispose();
            }
        }
    }

    /// <summary>
    /// <c>MsQuicTransportOptions.MaxStreams</c>: "The transport keeps a quarter of the table for the local streams
    /// (<c>PeerStreamRoom</c>)"; ADR 0009: "a quarter of it kept for local streams". Nothing is kept: <c>PeerStreamRoom</c>
    /// only decides whether a warning is written. A peer that holds as many streams open as the table has slots leaves
    /// none for this end, and <c>OpenStream</c> answers <c>OutOfMemory</c> — for a session, the control stream's pongs aside,
    /// every engine that needs a new stream.
    /// </summary>
    [Fact]
    public void A_Quarter_Of_The_Stream_Table_Stays_Free_For_Local_Streams()
    {
        if (!MsQuicApi.TryGetInstance(out _, out _))
        {
            Assert.Skip("MsQuic is not available on this host.");
        }

        using MsQuicTransportHarness harness = new();
        MsQuicTransportOptions clientOptions = harness.ClientOptions();
        clientOptions.MaxStreams = 8;
        clientOptions.ClientPeerUnidiStreamCount = 8;
        Assert.Equal(6, MsQuicTransportOptions.PeerStreamRoom(clientOptions.MaxStreams));
        RecordingSink clientSink = new() { ReceiveHandler = static (_, _, _, _) => ReceiveResult.PendingAfter(0) };
        RecordingSink serverSink = new() { AutoCloseStreams = false };
        MsQuicTransport? server = null;
        MsQuicTransportListener listener = harness.StartListener(harness.ServerOptions(), static (in NewConnectionInfo _) => PreHandshakeDecision.Accept,
            (ITransport transport, in NewConnectionInfo _) =>
            {
                serverSink.Transport = transport;
                Volatile.Write(ref server, harness.Track((MsQuicTransport)transport));
                return serverSink;
            });
        MsQuicTransport client = harness.Track(harness.CreateConnector(clientOptions).Connect(listener.LocalEndPoint, "localhost", clientSink));
        clientSink.Transport = client;
        Assert.True(clientSink.WaitForCount(RecordedEventKind.Connected, 1, TestTimeouts.Default) && serverSink.WaitForCount(RecordedEventKind.Connected, 1, TestTimeouts.Default));

        NativeBuffer data = new(16);
        NativeSegments segments = new(1);
        segments.Set(0, data.Segment(0, 16));
        try
        {
            // The server opens every stream the client granted and leaves them open; the client does not read them.
            for (int i = 0; i < 8; i++)
            {
                Assert.Equal(TransportStatus.Success, server!.OpenStream(StreamKind.Unidirectional, (ulong)(0x100 + i), 32767, out TransportStreamId id));
                Assert.Equal(TransportStatus.Success, server.SendStream(id, segments.At(0), 1, (ulong)(0x200 + i), TransportSendFlags.Start));
            }

            clientSink.WaitForCount(RecordedEventKind.PeerStreamStarted, 8, TimeSpan.FromSeconds(3));
            int held = clientSink.CountOf(RecordedEventKind.PeerStreamStarted);
            TransportStatus local = client.OpenStream(StreamKind.Unidirectional, 0x300, 32767, out TransportStreamId mine);
            Assert.True(local == TransportStatus.Success,
                $"the client's table has 8 slots, 'a quarter kept for the local streams'; its peer holds {held} streams open ({client.RefusedPeerStreamCount} refused), "
                + $"{client.OpenStreamCount} slots are in use, and the client's own OpenStream answers {local}");
            client.CloseStream(mine);
        }
        finally
        {
            client.Dispose();
            server?.Dispose();

            // The payload and its segment array may go only once the sending end reported its close (left allocated otherwise).
            clientSink.WaitForCount(RecordedEventKind.Closed, 1, TestTimeouts.Default);
            if (server is not null && serverSink.WaitForCount(RecordedEventKind.Closed, 1, TestTimeouts.Default))
            {
                segments.Dispose();
                data.Dispose();
            }
        }
    }

    // ------------------------------------------------------------------ the session over the carrier

    private sealed class AcceptAll : IPeerAdmission
    {
        public AdmissionResult Admit(in HelloInfo hello, QuiclyPeer peer) => AdmissionResult.Accept();
    }

    /// <summary>
    /// The first review's L1 scenario over WebTransport, which it named and did not test: a client built with
    /// <see cref="WebTransportConnector.CreateMsQuic"/> and default options grants its server 1 027 streams, three of them
    /// HTTP/3's. The client is late with its Poll while the server sends twenty groups; nothing may be reset.
    /// </summary>
    [Fact]
    public void A_Late_WebTransport_Client_Loses_No_Group_Of_A_Server_That_Kept_MaxGroups()
    {
        if (!MsQuicApi.TryGetInstance(out _, out _))
        {
            Assert.Skip("MsQuic is not available on this host.");
        }

        using WebTransportLoopback loopback = new();
        QuiclyPeer? server = null;
        QuiclyPeer? client = null;
        try
        {
            PeerOptions serverOptions = new() { GroupMinInterval = TimeSpan.Zero };
            PeerOptions clientOptions = new() { ReceiveRingCapacity = 64 };
            AcceptAll admission = new();
            WebTransportListener listener = loopback.Listen(null, (ITransport carrier, in NewConnectionInfo info) =>
            {
                QuiclyPeer peer = QuiclyPeer.CreateServerPeer(carrier, in info, Table, serverOptions, admission);
                Volatile.Write(ref server, peer);
                return peer.TransportSink;
            });
            WebTransportConnector connector = loopback.Own(WebTransportConnector.CreateMsQuic(loopback.ClientOptions(), null, loopback.Registration));
            client = QuiclyPeer.Connect(new TrackingConnector(loopback, connector), listener.LocalEndPoint, "localhost", Table, clientOptions);
            Assert.True(Pump(() => client.State == PeerState.Connected && Volatile.Read(ref server)?.State == PeerState.Connected, 10_000, client, () => Volatile.Read(ref server)),
                "the handshake did not complete");
            Assert.Equal(1024, client.Capabilities.PeerUnidirectionalStreams);
            QuiclyPeer sender = server!;
            List<int> got = [];
            client.RegisterHandler(11, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => got.Add(BitConverter.ToInt32(payload)));

            List<SendToken> tokens = [];
            byte[] payload = new byte[4];
            for (int i = 0; i < 320; i++)
            {
                BitConverter.TryWriteBytes(payload, i);
                SendResult result = sender.SendCopy(new SendHeader(11), payload, SendOptions.Tracked);
                Assert.True(result.IsAdmitted);
                tokens.Add(result.Token);
                if ((i & 15) == 15)
                {
                    for (int step = 0; step < 4; step++)
                    {
                        sender.Poll();
                        sender.Flush();
                        Thread.Sleep(2);
                    }
                }
            }

            bool all = Pump(() => got.Count == 320, 5_000, client, () => sender);
            client.GetStatistics(out PeerStatistics statistics);
            int delivered = tokens.Count(token => sender.GetDeliveryStatus(token) == DeliveryStatus.Delivered);
            int failed = tokens.Count(token => sender.GetDeliveryStatus(token) == DeliveryStatus.Failed);
            Assert.True(all,
                $"received {got.Count} of 320 messages; the client reset {statistics.StreamsReset} streams; the server reported {delivered} Delivered and {failed} Failed");
            Assert.Equal(0, statistics.StreamsReset);
            Assert.Equal(0, statistics.CallbackFaults);
        }
        finally
        {
            client?.Dispose();
            server?.Dispose();
        }
    }

    private static bool Pump(Func<bool> condition, int milliseconds, QuiclyPeer client, Func<QuiclyPeer?> server)
    {
        Stopwatch watch = Stopwatch.StartNew();
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

            if (watch.ElapsedMilliseconds > milliseconds)
            {
                return false;
            }

            Thread.Sleep(1);
        }
    }
}
