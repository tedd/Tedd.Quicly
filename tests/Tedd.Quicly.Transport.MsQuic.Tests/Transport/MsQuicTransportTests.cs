using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Certificates;
using Tedd.Quicly.Testing.Conformance;
using Tedd.Quicly.Testing.Simulation;
using Tedd.Quicly.Transport.MsQuic.Interop;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Transport;

/// <summary>MsQuic-specific behaviour of the transport, connector and listener over loopback (beyond the shared conformance suite).</summary>
[Collection(MsQuicCollection.Name)]
public unsafe class MsQuicTransportTests
{
    private static readonly TimeSpan Timeout = TestTimeouts.Default;
    private static readonly PreHandshakeCallback AcceptAll = static (in NewConnectionInfo _) => PreHandshakeDecision.Accept;

    /// <summary>A harness plus native memory that is freed only after every transport released its handles.</summary>
    private sealed class Scope(Action<MsQuicTransportOptions>? configure = null) : IDisposable
    {
        private readonly List<NativeBlock> _blocks = [];
        private readonly List<nint> _segments = [];
        private bool _disposed;

        public MsQuicTransportHarness Harness { get; } = new(configure);

        public long ExpectedSinkExceptions { get; set; }

        public NativeBlock Block(int length)
        {
            var block = new NativeBlock(length);
            block.FillPattern(0);
            _blocks.Add(block);
            return block;
        }

        public TransportSegment* Segments(int count)
        {
            var segments = (TransportSegment*)NativeMemory.AllocZeroed((nuint)(count * sizeof(TransportSegment)));
            _segments.Add((nint)segments);
            return segments;
        }

        /// <summary>Disposes everything and checks the cleanup (call at the end of a passing test).</summary>
        public void Finish()
        {
            DisposeCore();
            Assert.Null(Harness.CleanupError);
            Assert.Equal(ExpectedSinkExceptions, Harness.SinkExceptionTotal);
        }

        public void Dispose() => DisposeCore();

        private void DisposeCore()
        {
            if (_disposed) return;
            _disposed = true;
            Harness.Dispose();
            foreach (NativeBlock block in _blocks) block.Dispose();
            foreach (nint segments in _segments) NativeMemory.Free((void*)segments);
        }
    }

    private static (MsQuicTransport Client, MsQuicTransport Server, RecordingSink ClientSink, RecordingSink ServerSink) Connect(Scope scope, ConformancePairOptions? options = null, ITransportSink? clientInner = null, ITransportSink? serverInner = null)
    {
        var clientSink = new RecordingSink(null, clientInner);
        var serverSink = new RecordingSink(null, serverInner);
        ConformancePair pair = scope.Harness.CreatePair(clientSink, serverSink, options);
        clientSink.Transport = pair.Client;
        serverSink.Transport = pair.Server;
        Assert.True(WaitFor(clientSink, RecordedEventKind.Connected), "client OnConnected");
        Assert.True(WaitFor(serverSink, RecordedEventKind.Connected), "server OnConnected");
        return ((MsQuicTransport)pair.Client, (MsQuicTransport)pair.Server, clientSink, serverSink);
    }

    private static MsQuicTransportListener Listen(Scope scope, ConcurrentQueue<(MsQuicTransport Transport, RecordingSink Sink)> accepted, MsQuicTransportOptions? options = null, PreHandshakeCallback? preHandshake = null, IPEndPoint? endPoint = null)
    {
        return scope.Harness.StartListener(options ?? scope.Harness.ServerOptions(), preHandshake ?? AcceptAll, (ITransport t, in NewConnectionInfo _) =>
        {
            MsQuicTransport transport = scope.Harness.Track((MsQuicTransport)t);
            var sink = new RecordingSink { Transport = transport };
            accepted.Enqueue((transport, sink));
            return sink;
        }, endPoint: endPoint);
    }

    private static (MsQuicTransport Client, RecordingSink Sink) Dial(Scope scope, MsQuicTransportConnector connector, EndPoint endPoint, string? serverName = "localhost")
    {
        var sink = new RecordingSink();
        MsQuicTransport client = scope.Harness.Track(connector.Connect(endPoint, serverName, sink));
        sink.Transport = client;
        return (client, sink);
    }

    private static bool WaitFor(RecordingSink sink, RecordedEventKind kind) => sink.WaitFor(e => e.Kind == kind, Timeout);

    private static RecordedEvent Closed(RecordingSink sink) => sink.OfKind(RecordedEventKind.Closed)[0];

    // ------------------------------------------------------------------ connection basics

    [Fact]
    public void Connected_transports_expose_roles_names_and_capabilities_and_accept_every_mapped_flag()
    {
        using var scope = new Scope();
        (MsQuicTransport client, MsQuicTransport server, RecordingSink cs, RecordingSink ss) = Connect(scope);
        Assert.False(client.IsServer);
        Assert.True(server.IsServer);
        Assert.Equal("localhost", client.ServerName);
        Assert.Equal("localhost", server.ServerName);
        Assert.NotNull(client.RemoteEndPoint);
        Assert.Equal(server.Connection.LocalEndPoint!.Port, client.RemoteEndPoint!.Port);
        Assert.True(client.CancelOnBlockedSupported);
        Assert.Equal(2048, client.MaxStreams);
        Assert.True(Spin.Until(() => client.Capabilities.Datagrams && server.Capabilities.Datagrams, Timeout));
        TransportCapabilities capabilities = client.Capabilities;

        // What each end lets its peer open before anyone asks: the role's initial grant, as the harness configured it.
        Assert.Equal(scope.Harness.ClientOptions().CreateClientSettings().PeerUnidiStreamCount ?? 0, capabilities.PeerUnidirectionalStreams);
        Assert.Equal(scope.Harness.ServerOptions().CreateServerSettings().PeerUnidiStreamCount ?? 0, server.Capabilities.PeerUnidirectionalStreams);
        Assert.True(capabilities.StreamPriority && capabilities.IdealSendBufferSize && capabilities.CancelOnBlocked && capabilities.DatagramSendState);
        Assert.False(capabilities.AppOwnedReceiveBuffers);
        Assert.InRange(capabilities.MaxDatagramPayload, 1100, 1500);
        Assert.Equal("quicly/1", cs.OfKind(RecordedEventKind.Connected)[0].Alpn);

        NativeBlock block = scope.Block(64);
        TransportSegment* segments = scope.Segments(2);
        segments[0] = new TransportSegment(block.Pointer, 32);
        segments[1] = new TransportSegment(block.Pointer + 32, 32);
        TransportSendFlags all = TransportSendFlags.Priority | TransportSendFlags.DelaySend | TransportSendFlags.CancelOnBlocked | TransportSendFlags.Fin | TransportSendFlags.Start | TransportSendFlags.CancelOnLoss;
        Assert.Equal(TransportStatus.Success, client.SendDatagram(segments, 1, 1, all));
        Assert.True(WaitFor(ss, RecordedEventKind.DatagramReceived));

        Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Bidirectional, 5, 100, out TransportStreamId id));
        Assert.Equal(TransportStatus.Success, client.SendStream(id, segments + 1, 1, 2, TransportSendFlags.Start | TransportSendFlags.Priority | TransportSendFlags.DelaySend | TransportSendFlags.CancelOnLoss));
        Assert.True(cs.WaitFor(e => e.Kind == RecordedEventKind.StreamStarted && e.StreamId == id, Timeout));
        Assert.Equal(0, client.GetQuicStreamId(id));
        client.SetStreamPriority(id, 65535);
        Assert.True(ss.WaitFor(e => e.Kind == RecordedEventKind.StreamReceived, Timeout));
        TransportStreamId peer = ss.OfKind(RecordedEventKind.PeerStreamStarted)[0].StreamId;
        Assert.Equal(0, server.GetQuicStreamId(peer));
        scope.Finish();
    }

    // ------------------------------------------------------------------ stream table

    [Fact]
    public void Stream_table_is_bounded_and_slots_are_reused_with_a_new_generation()
    {
        using var scope = new Scope(o => o.MaxStreams = 4);
        (MsQuicTransport client, _, _, _) = Connect(scope);
        var ids = new TransportStreamId[4];
        for (int i = 0; i < ids.Length; i++) Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Bidirectional, (ulong)i, 32767, out ids[i]));
        Assert.Equal(TransportStatus.OutOfMemory, client.OpenStream(StreamKind.Bidirectional, 9, 32767, out TransportStreamId none));
        Assert.False(none.IsValid);
        Assert.Equal(4, client.OpenStreamCount);
        Assert.Equal(4, client.MaxStreams);

        client.CloseStream(ids[2]); // never started: the id is stale at once; the cleanup work item frees the slot a moment later
        Assert.True(Spin.Until(() => client.OpenStreamCount == 3, Timeout), $"open streams {client.OpenStreamCount}");
        Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Unidirectional, 5, 32767, out TransportStreamId reused));
        Assert.Equal(ids[2].Slot, reused.Slot);
        Assert.NotEqual(ids[2].Generation, reused.Generation);

        NativeBlock block = scope.Block(8);
        TransportSegment* segment = scope.Segments(1);
        *segment = new TransportSegment(block.Pointer, 8);
        foreach (TransportStreamId stale in new[] { ids[2], new TransportStreamId(99, 1), TransportStreamId.None, new TransportStreamId(-1, 1) })
        {
            Assert.Equal(-1, client.GetQuicStreamId(stale));
            Assert.Equal(TransportStatus.InvalidState, client.StartStream(stale));
            Assert.Equal(TransportStatus.InvalidState, client.SendStream(stale, segment, 1, 1, TransportSendFlags.Start));
            client.CloseStream(stale);
            client.AbortStream(stale, 1, StreamAbortDirection.Both);
            client.SetStreamPriority(stale, 1);
            client.ResumeStreamReceive(stale, 0);
        }
        Assert.Equal(TransportStatus.NotSupported, client.OpenStream((StreamKind)7, 1, 32767, out _));
        Assert.Throws<ArgumentOutOfRangeException>(() => client.SendStream(reused, segment, -1, 1, TransportSendFlags.Start));
        Assert.Throws<ArgumentNullException>(() => client.SendStream(reused, null, 1, 1, TransportSendFlags.Start));

        client.AbortStream(ids[0], 3, StreamAbortDirection.Send); // never started: released like CloseStream
        Assert.Equal(TransportStatus.InvalidState, client.StartStream(ids[0]));
        Assert.True(Spin.Until(() => client.OpenStreamCount == 3, Timeout), $"open streams {client.OpenStreamCount}");
        scope.Finish();
    }

    [Fact]
    public void A_stream_priority_set_before_the_start_is_applied_when_the_stream_starts()
    {
        using var scope = new Scope();
        (MsQuicTransport client, _, RecordingSink cs, _) = Connect(scope);
        NativeBlock block = scope.Block(16);
        TransportSegment* segment = scope.Segments(1);
        *segment = new TransportSegment(block.Pointer, 16);
        Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Bidirectional, 1, 100, out TransportStreamId opened));
        Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Bidirectional, 2, 32767, out TransportStreamId changed));
        client.SetStreamPriority(changed, 200); // not started yet: stored, applied at the start
        Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Bidirectional, 3, 32767, out TransportStreamId plain));
        Assert.Equal(TransportStatus.Success, client.StartStream(opened));
        Assert.Equal(TransportStatus.Success, client.SendStream(changed, segment, 1, 7, TransportSendFlags.Start));
        Assert.Equal(TransportStatus.Success, client.StartStream(plain));
        Assert.True(Spin.Until(() => cs.CountOf(RecordedEventKind.StreamStarted) == 3, Timeout));
        Assert.Equal(100, client.QueryStreamPriority(opened));
        Assert.Equal(200, client.QueryStreamPriority(changed));
        Assert.Equal(32767, client.QueryStreamPriority(plain));
        client.SetStreamPriority(plain, 300); // started: a parameter call
        Assert.Equal(300, client.QueryStreamPriority(plain));
        Assert.Equal(-1, client.QueryStreamPriority(TransportStreamId.None));
        scope.Finish();
    }

    [Fact]
    public void Peer_streams_beyond_the_stream_table_are_refused_and_counted()
    {
        var diagnostics = new ConcurrentQueue<string>();
        using var scope = new Scope();
        MsQuicTransportOptions serverOptions = scope.Harness.ServerOptions();
        serverOptions.MaxStreams = 4;
        serverOptions.Diagnostic = (_, message, _) => diagnostics.Enqueue(message);
        var accepted = new ConcurrentQueue<(MsQuicTransport Transport, RecordingSink Sink)>();
        MsQuicTransportListener listener = Listen(scope, accepted, serverOptions);
        (MsQuicTransport client, RecordingSink cs) = Dial(scope, scope.Harness.CreateConnector(scope.Harness.ClientOptions()), listener.LocalEndPoint);
        Assert.True(WaitFor(cs, RecordedEventKind.Connected));
        Assert.True(Spin.Until(() => !accepted.IsEmpty, Timeout));
        (MsQuicTransport server, RecordingSink ss) = accepted.ToArray()[0];
        NativeBlock block = scope.Block(10);
        TransportSegment* segment = scope.Segments(1);
        *segment = new TransportSegment(block.Pointer, 10);
        // Five concurrently open peer streams (no FIN) against a table of four.
        for (int i = 0; i < 5; i++)
        {
            Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Unidirectional, (ulong)i, 32767, out TransportStreamId id));
            Assert.Equal(TransportStatus.Success, client.SendStream(id, segment, 1, (ulong)i, TransportSendFlags.Start));
        }
        Assert.True(Spin.Until(() => server.RefusedPeerStreamCount == 1, Timeout), $"refused {server.RefusedPeerStreamCount}");
        Assert.Equal(4, ss.CountOf(RecordedEventKind.PeerStreamStarted));
        Assert.Equal(4, server.OpenStreamCount);
        // The refusal is counted just before the diagnostic is logged on the worker thread: wait for the message.
        Assert.True(Spin.Until(() => diagnostics.Any(m => m.Contains("stream table is full", StringComparison.Ordinal)), Timeout));

        // The server had been told in advance: it let the client open more streams than its table has room for.
        Assert.Single(diagnostics, m => m.Contains("raise MaxStreams", StringComparison.Ordinal));
        scope.Finish();
    }

    [Fact]
    public void A_stream_table_too_small_for_what_the_peer_may_open_is_reported_once()
    {
        var diagnostics = new ConcurrentQueue<string>();
        using var scope = new Scope();
        MsQuicTransportOptions clientOptions = scope.Harness.ClientOptions();
        clientOptions.MaxStreams = 8; // room for six peer streams next to the local ones
        clientOptions.ClientPeerUnidiStreamCount = 64;
        clientOptions.Diagnostic = (_, message, _) => diagnostics.Enqueue(message);
        var accepted = new ConcurrentQueue<(MsQuicTransport Transport, RecordingSink Sink)>();
        MsQuicTransportListener listener = Listen(scope, accepted);
        (MsQuicTransport client, RecordingSink cs) = Dial(scope, scope.Harness.CreateConnector(clientOptions), listener.LocalEndPoint);
        Assert.True(WaitFor(cs, RecordedEventKind.Connected));

        // The grant is in the transport parameters, so it is reported from the start: the session sizes itself for it.
        Assert.Equal(64, client.Capabilities.PeerUnidirectionalStreams);
        Assert.Single(diagnostics, m => m.Contains("raise MaxStreams", StringComparison.Ordinal));
        Assert.Contains("64 streams", diagnostics.Single(m => m.Contains("raise MaxStreams", StringComparison.Ordinal)), StringComparison.Ordinal);

        // A later request of the session is covered by the same warning; it is not repeated.
        client.UpdatePeerStreamLimits(0, 100);
        Assert.Single(diagnostics, m => m.Contains("raise MaxStreams", StringComparison.Ordinal));
        scope.Finish();
    }

    [Fact]
    public void A_stream_table_with_room_for_the_peers_streams_is_not_reported()
    {
        var diagnostics = new ConcurrentQueue<string>();
        using var scope = new Scope();
        MsQuicTransportOptions clientOptions = scope.Harness.ClientOptions();
        clientOptions.MaxStreams = 64; // room for forty-eight
        clientOptions.ClientPeerUnidiStreamCount = 16;
        clientOptions.Diagnostic = (_, message, _) => diagnostics.Enqueue(message);
        var accepted = new ConcurrentQueue<(MsQuicTransport Transport, RecordingSink Sink)>();
        MsQuicTransportListener listener = Listen(scope, accepted);
        (MsQuicTransport client, RecordingSink cs) = Dial(scope, scope.Harness.CreateConnector(clientOptions), listener.LocalEndPoint);
        Assert.True(WaitFor(cs, RecordedEventKind.Connected));
        Assert.Equal(16, client.Capabilities.PeerUnidirectionalStreams);
        client.UpdatePeerStreamLimits(0, 48);
        Assert.DoesNotContain(diagnostics, m => m.Contains("raise MaxStreams", StringComparison.Ordinal));

        // Asking for more than the table can hold is reported when it is asked for.
        client.UpdatePeerStreamLimits(0, 49);
        Assert.Single(diagnostics, m => m.Contains("raise MaxStreams", StringComparison.Ordinal));
        scope.Finish();
    }

    // ------------------------------------------------------------------ closing streams

    [Fact]
    public void CloseStream_from_OnStreamShutdownComplete_is_deferred_and_frees_the_slot()
    {
        using var scope = new Scope();
        (MsQuicTransport client, MsQuicTransport server, RecordingSink cs, RecordingSink ss) = Connect(scope);
        NativeBlock block = scope.Block(2000);
        TransportSegment* segments = scope.Segments(3);
        segments[0] = new TransportSegment(block.Pointer, 1000);
        segments[1] = new TransportSegment(block.Pointer + 1000, 500);
        segments[2] = new TransportSegment(block.Pointer + 1500, 300);
        Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Bidirectional, 1, 32767, out TransportStreamId id));
        Assert.Equal(TransportStatus.Success, client.SendStream(id, segments, 1, 1, TransportSendFlags.Start | TransportSendFlags.Fin));
        Assert.True(WaitFor(ss, RecordedEventKind.StreamPeerSendShutdown));
        TransportStreamId peer = ss.OfKind(RecordedEventKind.PeerStreamStarted)[0].StreamId;
        Assert.Equal(TransportStatus.Success, server.SendStream(peer, segments + 1, 1, 2, TransportSendFlags.Fin));
        Assert.True(WaitFor(cs, RecordedEventKind.StreamShutdownComplete));
        Assert.True(WaitFor(ss, RecordedEventKind.StreamShutdownComplete));
        // The recording sinks call CloseStream inside OnStreamShutdownComplete: the native close runs on the thread pool.
        Assert.True(Spin.Until(() => client.OpenStreamCount == 0 && server.OpenStreamCount == 0, Timeout), $"open streams: client {client.OpenStreamCount}, server {server.OpenStreamCount}");

        // A server-initiated unidirectional stream takes the same path on the client.
        Assert.Equal(TransportStatus.Success, server.OpenStream(StreamKind.Unidirectional, 3, 32767, out TransportStreamId push));
        Assert.Equal(TransportStatus.Success, server.StartStream(push));
        Assert.Equal(TransportStatus.Success, server.SendStream(push, segments + 2, 1, 3, TransportSendFlags.Fin));
        Assert.True(cs.WaitFor(e => e.Kind == RecordedEventKind.PeerStreamStarted && e.StreamKind == StreamKind.Unidirectional, Timeout));
        Assert.True(Spin.Until(() => cs.CountOf(RecordedEventKind.StreamShutdownComplete) == 2 && ss.CountOf(RecordedEventKind.StreamShutdownComplete) == 2, Timeout));
        Assert.True(Spin.Until(() => client.OpenStreamCount == 0 && server.OpenStreamCount == 0, Timeout));
        TransportStreamId pushed = cs.OfKind(RecordedEventKind.PeerStreamStarted)[0].StreamId;
        Assert.Equal(new ReadOnlySpan<byte>(block.Pointer + 1500, 300).ToArray(), cs.GetStreamData(pushed));
        scope.Finish();
    }

    private sealed class CloseOnPeerStream : NullTransportSink
    {
        public ITransport? Transport;

        public override void OnPeerStreamStarted(TransportStreamId id, StreamKind kind) => Transport!.CloseStream(id);

        public override void OnStreamShutdownComplete(TransportStreamId id) => Transport!.CloseStream(id);
    }

    [Fact]
    public void CloseStream_inside_OnPeerStreamStarted_aborts_the_stream_without_a_shutdown_callback()
    {
        using var scope = new Scope();
        var closer = new CloseOnPeerStream();
        (MsQuicTransport client, MsQuicTransport server, RecordingSink cs, RecordingSink ss) = Connect(scope, serverInner: closer);
        closer.Transport = server;
        NativeBlock block = scope.Block(1000);
        TransportSegment* segment = scope.Segments(1);
        *segment = new TransportSegment(block.Pointer, 1000);
        Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Bidirectional, 1, 32767, out TransportStreamId id));
        Assert.Equal(TransportStatus.Success, client.SendStream(id, segment, 1, 1, TransportSendFlags.Start));
        Assert.True(cs.WaitFor(e => e.Kind == RecordedEventKind.StreamShutdownComplete && e.StreamId == id, Timeout));
        Assert.Contains(cs.OfKind(RecordedEventKind.StreamAborted), e => e.StreamId == id && e.ErrorCode == 0);
        Assert.Equal(1, cs.OfKind(RecordedEventKind.StreamSendCompleted).Count);
        Assert.True(Spin.Until(() => server.OpenStreamCount == 0 && client.OpenStreamCount == 0, Timeout));
        Assert.Equal(0, ss.CountOf(RecordedEventKind.StreamShutdownComplete));
        Assert.Equal(1, ss.CountOf(RecordedEventKind.PeerStreamStarted));
        scope.Finish();
    }

    [Fact]
    public void CloseStream_before_shutdown_cancels_pending_sends_and_suppresses_the_shutdown_callback()
    {
        using var scope = new Scope();
        (MsQuicTransport client, _, RecordingSink cs, RecordingSink ss) = Connect(scope);
        ss.ReceiveHandler = static (_, _, _, _) => ReceiveResult.PendingAfter(0);
        NativeBlock block = scope.Block(64 * 1024);
        TransportSegment* segment = scope.Segments(1);
        *segment = new TransportSegment(block.Pointer, 64 * 1024);
        Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Bidirectional, 1, 32767, out TransportStreamId id));
        for (int i = 0; i < 8; i++) Assert.Equal(TransportStatus.Success, client.SendStream(id, segment, 1, (ulong)(i + 1), i == 0 ? TransportSendFlags.Start : TransportSendFlags.None));
        Assert.True(WaitFor(ss, RecordedEventKind.StreamReceived));
        client.CloseStream(id);
        client.CloseStream(id); // a second close of the same id is ignored
        Assert.True(Spin.Until(() => cs.CountOf(RecordedEventKind.StreamSendCompleted) == 8, Timeout));
        Assert.Contains(cs.OfKind(RecordedEventKind.StreamSendCompleted), e => e.Canceled);
        Assert.True(Spin.Until(() => client.OpenStreamCount == 0, Timeout));
        Assert.Equal(0, cs.CountOf(RecordedEventKind.StreamShutdownComplete));
        Assert.Equal(TransportStatus.InvalidState, client.SendStream(id, segment, 1, 9, TransportSendFlags.None));
        scope.Finish();
    }

    private sealed class DisposeOnDatagram : NullTransportSink
    {
        public ITransport? Transport;

        public override void OnDatagramReceived(ReadOnlySpan<byte> payload) => Transport!.Dispose();
    }

    [Fact]
    public void Dispose_inside_a_callback_releases_the_handles_after_shutdown_complete()
    {
        using var scope = new Scope();
        var disposer = new DisposeOnDatagram();
        (MsQuicTransport client, MsQuicTransport server, RecordingSink cs, RecordingSink ss) = Connect(scope, clientInner: disposer);
        disposer.Transport = client;
        Assert.True(Spin.Until(() => server.Capabilities.Datagrams, Timeout));
        NativeBlock block = scope.Block(16);
        TransportSegment* segment = scope.Segments(1);
        *segment = new TransportSegment(block.Pointer, 16);
        Assert.Equal(TransportStatus.Success, server.SendDatagram(segment, 1, 1, TransportSendFlags.None));
        Assert.True(client.WaitForHandlesClosed(Timeout));
        Assert.True(client.HandlesClosed);
        Assert.Equal(TransportState.Closed, client.State);
        Assert.Equal(TransportCloseReason.Local, Closed(cs).CloseReason);
        Assert.True(WaitFor(ss, RecordedEventKind.Closed));
        Assert.Equal(TransportCloseReason.Peer, Closed(ss).CloseReason);
        client.Dispose();
        client.GetStatistics(out TransportStatistics statistics);
        Assert.Equal(default, statistics);
        Assert.Equal(TransportStatus.InvalidState, client.OpenStream(StreamKind.Bidirectional, 1, 32767, out _));
        scope.Finish();
    }

    // ------------------------------------------------------------------ receive back-pressure

    [Fact]
    public void A_resume_that_races_the_receive_callback_is_applied_when_it_returns()
    {
        using var scope = new Scope();
        (MsQuicTransport client, MsQuicTransport server, _, RecordingSink ss) = Connect(scope);
        var offsets = new ConcurrentQueue<ulong>();
        int calls = 0;
        ss.ReceiveHandler = (id, segments, offset, _) =>
        {
            offsets.Enqueue(offset);
            int total = 0;
            foreach (TransportSegment segment in segments) total += (int)segment.Length;
            if (Interlocked.Increment(ref calls) != 1) return ReceiveResult.Consumed(total);
            // Another thread resumes while this callback is still running (the sink signalled it before returning).
            var resumer = new Thread(() => server.ResumeStreamReceive(id, 5));
            resumer.Start();
            resumer.Join();
            return ReceiveResult.PendingAfter(10);
        };
        NativeBlock block = scope.Block(100);
        TransportSegment* segment = scope.Segments(1);
        *segment = new TransportSegment(block.Pointer, 100);
        Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Unidirectional, 1, 32767, out TransportStreamId id));
        Assert.Equal(TransportStatus.Success, client.SendStream(id, segment, 1, 1, TransportSendFlags.Start | TransportSendFlags.Fin));
        Assert.True(WaitFor(ss, RecordedEventKind.StreamPeerSendShutdown));
        Assert.Equal([0UL, 15UL], offsets.ToArray());
        RecordedEvent last = ss.OfKind(RecordedEventKind.StreamReceived)[^1];
        Assert.True(last.Fin);
        Assert.Equal(new ReadOnlySpan<byte>(block.Pointer + 15, 85).ToArray(), last.Data);
        scope.Finish();
    }

    [Fact]
    public void Resume_validates_the_credited_bytes()
    {
        using var scope = new Scope();
        (MsQuicTransport client, MsQuicTransport server, _, RecordingSink ss) = Connect(scope);
        ss.ReceiveHandler = static (_, _, _, _) => ReceiveResult.PendingAfter(40);
        NativeBlock block = scope.Block(100);
        TransportSegment* segment = scope.Segments(1);
        *segment = new TransportSegment(block.Pointer, 100);
        Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Unidirectional, 1, 32767, out TransportStreamId id));
        Assert.Equal(TransportStatus.Success, client.SendStream(id, segment, 1, 1, TransportSendFlags.Start | TransportSendFlags.Fin));
        Assert.True(WaitFor(ss, RecordedEventKind.StreamReceived));
        TransportStreamId peer = ss.OfKind(RecordedEventKind.PeerStreamStarted)[0].StreamId;
        Assert.Throws<ArgumentOutOfRangeException>(() => server.ResumeStreamReceive(peer, -1));
        Assert.Throws<ArgumentOutOfRangeException>(() => server.ResumeStreamReceive(peer, 61));
        ss.ReceiveHandler = null;
        server.ResumeStreamReceive(peer, 60);
        server.ResumeStreamReceive(peer, 1); // nothing pending any more: ignored
        Assert.True(WaitFor(ss, RecordedEventKind.StreamPeerSendShutdown));
        Assert.Single(ss.OfKind(RecordedEventKind.StreamReceived));
        scope.Finish();
    }

    // ------------------------------------------------------------------ sink failures

    private sealed class ThrowOnDatagram : NullTransportSink
    {
        public override void OnDatagramReceived(ReadOnlySpan<byte> payload) => throw new InvalidOperationException("sink failure");
    }

    [Fact]
    public void A_throwing_sink_shuts_the_connection_down_with_InternalError()
    {
        var errors = new ConcurrentQueue<string>();
        using var scope = new Scope(o => o.Diagnostic = (level, message, _) =>
        {
            if (level == TransportDiagnosticLevel.Error) errors.Enqueue(message);
        });
        (MsQuicTransport client, MsQuicTransport server, RecordingSink cs, RecordingSink ss) = Connect(scope, serverInner: new ThrowOnDatagram());
        Assert.True(Spin.Until(() => client.Capabilities.Datagrams, Timeout));
        NativeBlock block = scope.Block(16);
        TransportSegment* segment = scope.Segments(1);
        *segment = new TransportSegment(block.Pointer, 16);
        Assert.Equal(TransportStatus.Success, client.SendDatagram(segment, 1, 1, TransportSendFlags.None));
        Assert.True(WaitFor(ss, RecordedEventKind.Closed));
        Assert.True(WaitFor(cs, RecordedEventKind.Closed));
        Assert.Equal(TransportCloseReason.Local, Closed(ss).CloseReason);
        Assert.Equal(MsQuicTransport.InternalErrorCode, Closed(ss).ErrorCode);
        Assert.Equal(TransportCloseReason.Peer, Closed(cs).CloseReason);
        Assert.Equal(MsQuicTransport.InternalErrorCode, Closed(cs).ErrorCode);
        Assert.Equal(1, server.SinkExceptionCount);
        Assert.IsType<InvalidOperationException>(server.LastSinkException);
        Assert.Contains(errors, m => m.Contains("threw", StringComparison.Ordinal));
        scope.ExpectedSinkExceptions = 1;
        scope.Finish();
    }

    // ------------------------------------------------------------------ statistics

    [Fact]
    public void Statistics_are_filled_after_traffic_without_allocating()
    {
        using var scope = new Scope();
        (MsQuicTransport client, _, _, RecordingSink ss) = Connect(scope);
        Assert.True(Spin.Until(() => client.Capabilities.Datagrams, Timeout));
        NativeBlock block = scope.Block(4096);
        TransportSegment* segment = scope.Segments(1);
        *segment = new TransportSegment(block.Pointer, 4096);
        Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Bidirectional, 1, 32767, out TransportStreamId id));
        Assert.Equal(TransportStatus.Success, client.SendStream(id, segment, 1, 1, TransportSendFlags.Start | TransportSendFlags.Fin));
        Assert.True(WaitFor(ss, RecordedEventKind.StreamPeerSendShutdown));
        client.GetStatistics(out TransportStatistics statistics);
        Assert.True(statistics.SendTotalBytes > 4096, $"SendTotalBytes {statistics.SendTotalBytes}");
        Assert.True(statistics.RecvTotalBytes > 0);
        Assert.True(statistics.SendTotalPackets > 0);
        Assert.True(statistics.RecvTotalPackets > 0);
        Assert.True(statistics.RttMicros > 0);
        Assert.True(statistics.MaxRttMicros >= statistics.MinRttMicros);
        Assert.True(statistics.CongestionWindowBytes > 0);
        Assert.InRange(statistics.PathMtu, (ushort)1200, (ushort)1500);
        Assert.Equal(0UL, statistics.BytesInFlight);
        client.GetStatistics(out _);
        WindowedAllocation.AssertNone(() =>
        {
            for (int i = 0; i < 100; i++) client.GetStatistics(out _);
        });
        scope.Finish();
    }

    // ------------------------------------------------------------------ connecting state

    [Fact]
    public void Calls_while_connecting_follow_the_contract()
    {
        using var scope = new Scope();
        var gate = new ManualResetEventSlim();
        var accepted = new ConcurrentQueue<(MsQuicTransport, RecordingSink)>();
        MsQuicTransportListener listener = Listen(scope, accepted, preHandshake: (in NewConnectionInfo _) =>
        {
            gate.Wait(Timeout);
            return PreHandshakeDecision.Accept;
        });
        MsQuicTransportConnector connector = scope.Harness.CreateConnector(scope.Harness.ClientOptions());
        (MsQuicTransport client, RecordingSink sink) = Dial(scope, connector, listener.LocalEndPoint);
        try
        {
            Assert.Equal(TransportState.Connecting, client.State);
            NativeBlock block = scope.Block(16);
            TransportSegment* segment = scope.Segments(1);
            *segment = new TransportSegment(block.Pointer, 16);
            Assert.Equal(TransportStatus.InvalidState, client.SendDatagram(segment, 1, 1, TransportSendFlags.None));
            Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Bidirectional, 1, 32767, out TransportStreamId id));
            Assert.Equal(TransportStatus.InvalidState, client.StartStream(id));
            Assert.Equal(TransportStatus.InvalidState, client.SendStream(id, segment, 1, 1, TransportSendFlags.Start));
            Assert.Equal(-1, client.GetQuicStreamId(id));
            gate.Set();
            Assert.True(WaitFor(sink, RecordedEventKind.Connected));
            Assert.Equal(TransportStatus.Success, client.StartStream(id));
            Assert.True(sink.WaitFor(e => e.Kind == RecordedEventKind.StreamStarted && e.Status == TransportStatus.Success, Timeout));
        }
        finally
        {
            gate.Set();
        }
        scope.Finish();
    }

    // ------------------------------------------------------------------ admission

    [Fact]
    public void PreHandshake_reject_and_a_null_accept_refuse_the_connection()
    {
        using var scope = new Scope();
        int preHandshakes = 0;
        string? serverName = null;
        string? alpn = null;
        IPEndPoint? remote = null;
        MsQuicTransportListener listener = scope.Harness.StartListener(scope.Harness.ServerOptions(), (in NewConnectionInfo info) =>
        {
            serverName = info.ServerName;
            ReadOnlySpan<byte> negotiated = info.Alpn;
            alpn = Encoding.ASCII.GetString(negotiated[..info.AlpnLength]);
            remote = info.RemoteEndPoint;
            return Interlocked.Increment(ref preHandshakes) == 1 ? PreHandshakeDecision.Reject : PreHandshakeDecision.Accept;
        }, static (ITransport _, in NewConnectionInfo _) => null);
        MsQuicTransportOptions clientOptions = scope.Harness.ClientOptions();
        clientOptions.HandshakeIdleTimeout = TimeSpan.FromSeconds(2);
        MsQuicTransportConnector connector = scope.Harness.CreateConnector(clientOptions);

        (_, RecordingSink rejected) = Dial(scope, connector, listener.LocalEndPoint);
        Assert.True(WaitFor(rejected, RecordedEventKind.Closed));
        Assert.Equal(0, rejected.CountOf(RecordedEventKind.Connected));
        Assert.Equal(TransportCloseReason.Transport, Closed(rejected).CloseReason);
        Assert.Equal(1, listener.PreHandshakeRejectedCount);
        Assert.Equal("localhost", serverName);
        Assert.Equal("quicly/1", alpn);
        Assert.NotNull(remote);
        Assert.True(IPAddress.IsLoopback(remote!.Address));

        (_, RecordingSink refused) = Dial(scope, connector, listener.LocalEndPoint);
        Assert.True(WaitFor(refused, RecordedEventKind.Closed));
        Assert.Equal(0, refused.CountOf(RecordedEventKind.Connected));
        Assert.Equal(TransportCloseReason.Transport, Closed(refused).CloseReason);
        Assert.Equal(1, listener.AcceptRefusedCount);
        Assert.Equal(0, listener.LiveTransportCount);
        scope.Finish();
    }

    [Fact]
    public void A_configuration_failure_after_accept_closes_the_transport()
    {
        using var scope = new Scope();
        var accepted = new ConcurrentQueue<(MsQuicTransport, RecordingSink)>();
        MsQuicTransportListener listener = Listen(scope, accepted);
        var sink = new RecordingSink();
        var connection = new MsQuicConnection(scope.Harness.Registration);
        var transport = new MsQuicTransport(connection, sink, scope.Harness.ServerOptions(), null, null);
        ((IMsQuicListenerEvents)listener).ConnectionConfigurationFailed(null!, connection, MsQuicStatus.QUIC_STATUS_INVALID_STATE);
        Assert.True(WaitFor(sink, RecordedEventKind.Closed));
        Assert.Equal(TransportCloseReason.Transport, Closed(sink).CloseReason);
        Assert.Equal(MsQuicStatus.QUIC_STATUS_INVALID_STATE, Closed(sink).TransportStatus);
        Assert.True(transport.WaitForHandlesClosed(Timeout));
        Assert.Equal(1, listener.OtherRefusedCount);
        scope.Finish();
    }

    [Fact]
    public void A_connection_msquic_refuses_to_start_throws_and_is_released()
    {
        using var scope = new Scope();
        MsQuicTransportConnector connector = scope.Harness.CreateConnector(scope.Harness.ClientOptions());
        var sink = new RecordingSink();
        Assert.Throws<MsQuicException>(() => connector.Connect(new IPEndPoint(IPAddress.Loopback, 0), "localhost", sink));
        Assert.Equal(0, connector.LiveTransportCount);
        Assert.Equal(0, sink.Count);
        Assert.Throws<ArgumentException>(() => connector.Connect(new UnixDomainSocketEndPoint("quicly"), null, sink));
        Assert.Throws<ArgumentNullException>(() => connector.Connect(new IPEndPoint(IPAddress.Loopback, 1), null, null!));
        scope.Finish();
    }

    // ------------------------------------------------------------------ certificates

    [Fact]
    public void Certificate_hot_swap_serves_new_connections_the_new_certificate_and_keeps_old_ones_working()
    {
        using X509Certificate2 next = TestCertificates.CreateSelfSigned("CN=localhost", TimeSpan.FromDays(1), ecdsa: true, "localhost");
        using var scope = new Scope();
        var leaves = new ConcurrentQueue<byte[]>();
        var inValidation = new ManualResetEventSlim();
        var release = new ManualResetEventSlim();
        int validations = 0;
        var clientOptions = new MsQuicTransportOptions
        {
            ServerCertificateValidation = ServerCertificateValidationMode.Callback,
            ServerCertificateValidator = (in ServerCertificateContext context) =>
            {
                leaves.Enqueue(context.LeafDer.ToArray());
                if (Interlocked.Increment(ref validations) == 1)
                {
                    inValidation.Set();
                    release.Wait(Timeout);
                }
                return ServerCertificateDecision.AcceptIgnoringPlatformValidation;
            },
        };
        var retired = new ConcurrentQueue<X509Certificate2>();
        var accepted = new ConcurrentQueue<(MsQuicTransport Transport, RecordingSink Sink)>();
        MsQuicTransportListener listener = Listen(scope, accepted);
        listener.CertificateRetired = retired.Enqueue;
        Assert.Equal(1, listener.OpenConfigurationCount);
        Assert.Same(scope.Harness.Certificate, listener.CurrentCertificate);
        MsQuicTransportConnector connector = scope.Harness.CreateConnector(clientOptions);
        (MsQuicTransport first, RecordingSink firstSink) = Dial(scope, connector, listener.LocalEndPoint);
        try
        {
            Assert.True(inValidation.Wait(Timeout));
            // The first connection is still handshaking with the old configuration, so the swap must keep it open.
            listener.UpdateCertificate(next);
            Assert.Same(next, listener.CurrentCertificate);
            Assert.Equal(2, listener.OpenConfigurationCount);
            Assert.Empty(retired);
        }
        finally
        {
            release.Set();
        }
        Assert.True(WaitFor(firstSink, RecordedEventKind.Connected));
        Assert.True(Spin.Until(() => listener.OpenConfigurationCount == 1 && !retired.IsEmpty, Timeout), $"open configurations {listener.OpenConfigurationCount}");
        Assert.Same(scope.Harness.Certificate, Assert.Single(retired));

        (_, RecordingSink secondSink) = Dial(scope, connector, listener.LocalEndPoint);
        Assert.True(WaitFor(secondSink, RecordedEventKind.Connected));
        byte[][] seen = leaves.ToArray();
        Assert.Equal(2, seen.Length);
        Assert.Equal(scope.Harness.Certificate.RawData, seen[0]);
        Assert.Equal(next.RawData, seen[1]);

        // The connection made with the old certificate keeps working.
        Assert.True(Spin.Until(() => first.Capabilities.Datagrams, Timeout));
        NativeBlock block = scope.Block(16);
        TransportSegment* segment = scope.Segments(1);
        *segment = new TransportSegment(block.Pointer, 16);
        Assert.Equal(TransportStatus.Success, first.SendDatagram(segment, 1, 1, TransportSendFlags.None));
        Assert.True(WaitFor(accepted.ToArray()[0].Sink, RecordedEventKind.DatagramReceived));
        Assert.Throws<ArgumentNullException>(() => listener.UpdateCertificate(null!));
        scope.Finish();
        Assert.Contains(next, retired);
    }

    [Fact]
    public void Pinned_spki_mismatch_fails_the_handshake_and_is_reported()
    {
        using var scope = new Scope();
        var diagnostics = new ConcurrentQueue<string>();
        var accepted = new ConcurrentQueue<(MsQuicTransport, RecordingSink)>();
        MsQuicTransportListener listener = Listen(scope, accepted);
        MsQuicTransportOptions wrong = scope.Harness.ClientOptions();
        wrong.PinnedSpkiSha256 = [new byte[32]];
        wrong.Diagnostic = (_, message, _) => diagnostics.Enqueue(message);
        (_, RecordingSink failed) = Dial(scope, scope.Harness.CreateConnector(wrong), listener.LocalEndPoint);
        Assert.True(WaitFor(failed, RecordedEventKind.Closed));
        Assert.Equal(0, failed.CountOf(RecordedEventKind.Connected));
        Assert.Equal(TransportCloseReason.Transport, Closed(failed).CloseReason);
        Assert.Contains(diagnostics, m => m.Contains("matches no pin", StringComparison.Ordinal));

        (_, RecordingSink pinned) = Dial(scope, scope.Harness.CreateConnector(scope.Harness.ClientOptions()), listener.LocalEndPoint);
        Assert.True(WaitFor(pinned, RecordedEventKind.Connected));
        scope.Finish();
    }

    [Fact]
    public void Callback_validation_decides_with_the_der_certificate_and_the_platform_verdict()
    {
        using var scope = new Scope();
        var accepted = new ConcurrentQueue<(MsQuicTransport, RecordingSink)>();
        MsQuicTransportListener listener = Listen(scope, accepted);
        var levels = new ConcurrentQueue<TransportDiagnosticLevel>();
        ServerCertificateDecision decision = ServerCertificateDecision.Accept;
        byte[]? leaf = null;
        string? name = null;
        bool? platformValid = null;
        var options = new MsQuicTransportOptions
        {
            ServerCertificateValidation = ServerCertificateValidationMode.Callback,
            ServerCertificateValidator = (in ServerCertificateContext context) =>
            {
                leaf = context.LeafDer.ToArray();
                name = context.ServerName;
                platformValid = context.PlatformValid;
                return decision == (ServerCertificateDecision)99 ? throw new InvalidOperationException("validator failure") : decision;
            },
            Diagnostic = (level, _, _) => levels.Enqueue(level),
        };
        MsQuicTransportConnector connector = scope.Harness.CreateConnector(options);

        // Accept still requires the platform verdict, and a self-signed certificate fails it.
        (_, RecordingSink requiresPlatform) = Dial(scope, connector, listener.LocalEndPoint);
        Assert.True(WaitFor(requiresPlatform, RecordedEventKind.Closed));
        Assert.Equal(0, requiresPlatform.CountOf(RecordedEventKind.Connected));
        Assert.False(platformValid);
        Assert.Equal(scope.Harness.Certificate.RawData, leaf);
        Assert.Equal("localhost", name);
        Assert.Contains(TransportDiagnosticLevel.Warning, levels);

        decision = ServerCertificateDecision.AcceptIgnoringPlatformValidation;
        (_, RecordingSink trusted) = Dial(scope, connector, listener.LocalEndPoint);
        Assert.True(WaitFor(trusted, RecordedEventKind.Connected));

        decision = (ServerCertificateDecision)99;
        (_, RecordingSink throwing) = Dial(scope, connector, listener.LocalEndPoint);
        Assert.True(WaitFor(throwing, RecordedEventKind.Closed));
        Assert.Equal(0, throwing.CountOf(RecordedEventKind.Connected));
        Assert.Contains(TransportDiagnosticLevel.Error, levels);
        scope.Finish();
    }

    [Fact]
    public void Insecure_validation_needs_the_environment_guard_and_warns_for_every_connection()
    {
        using var scope = new Scope();
        string? previous = Environment.GetEnvironmentVariable(MsQuicTransportOptions.AllowInsecureEnvironmentVariable);
        try
        {
            Environment.SetEnvironmentVariable(MsQuicTransportOptions.AllowInsecureEnvironmentVariable, null);
            var warnings = new ConcurrentQueue<string>();
            var options = new MsQuicTransportOptions
            {
                ServerCertificateValidation = ServerCertificateValidationMode.DangerousAcceptAnyServerCertificate,
                Diagnostic = (level, message, _) =>
                {
                    if (level == TransportDiagnosticLevel.Warning) warnings.Enqueue(message);
                },
            };
            if (!MsQuicTransportOptions.IsDebugBuild)
            {
                Assert.Throws<InvalidOperationException>(() => new MsQuicTransportConnector(options, scope.Harness.Registration));
            }
            Environment.SetEnvironmentVariable(MsQuicTransportOptions.AllowInsecureEnvironmentVariable, "1");
            MsQuicTransportConnector connector = scope.Harness.CreateConnector(options);
            var accepted = new ConcurrentQueue<(MsQuicTransport, RecordingSink)>();
            MsQuicTransportListener listener = Listen(scope, accepted);
            for (int i = 0; i < 2; i++)
            {
                (_, RecordingSink sink) = Dial(scope, connector, listener.LocalEndPoint);
                Assert.True(WaitFor(sink, RecordedEventKind.Connected));
            }
            int insecure = 0;
            foreach (string warning in warnings) insecure += warning.Contains("WITHOUT validating", StringComparison.Ordinal) ? 1 : 0;
            Assert.Equal(2, insecure);
        }
        finally
        {
            Environment.SetEnvironmentVariable(MsQuicTransportOptions.AllowInsecureEnvironmentVariable, previous);
        }
        scope.Finish();
    }

    // ------------------------------------------------------------------ ownership and end points

    [Fact]
    public void An_owned_registration_is_closed_after_the_last_transport_released_its_handles()
    {
        using var scope = new Scope();
        var accepted = new ConcurrentQueue<(MsQuicTransport Transport, RecordingSink Sink)>();
        MsQuicTransportListener listener = Listen(scope, accepted);
        MsQuicTransportOptions clientOptions = scope.Harness.ClientOptions();
        clientOptions.AppName = "quicly-owned-connector";
        var connector = new MsQuicTransportConnector(clientOptions);
        var sink = new RecordingSink();
        MsQuicTransport client = connector.Connect(listener.LocalEndPoint, "localhost", sink);
        Assert.True(WaitFor(sink, RecordedEventKind.Connected));
        Assert.Equal(1, connector.LiveTransportCount);
        connector.Dispose();
        connector.Dispose();
        Assert.False(connector.ResourcesClosed);
        Assert.Throws<ObjectDisposedException>(() => connector.Connect(listener.LocalEndPoint, "localhost", new RecordingSink()));
        client.Dispose();
        Assert.True(client.WaitForHandlesClosed(Timeout));
        Assert.True(Spin.Until(() => connector.ResourcesClosed, Timeout));
        Assert.Equal(0, connector.LiveTransportCount);

        // A listener that owns its registration closes it once its transports and configurations are gone.
        var ownedListener = new MsQuicTransportListener(new IPEndPoint(IPAddress.Loopback, 0), scope.Harness.Certificate, scope.Harness.ServerOptions());
        var serverSink = new RecordingSink();
        MsQuicTransport? server = null;
        ownedListener.Start(AcceptAll, (ITransport t, in NewConnectionInfo _) =>
        {
            server = (MsQuicTransport)t;
            serverSink.Transport = t;
            return serverSink;
        });
        Assert.Throws<InvalidOperationException>(() => ownedListener.Start(AcceptAll, static (ITransport _, in NewConnectionInfo _) => null));
        (_, RecordingSink other) = Dial(scope, scope.Harness.CreateConnector(scope.Harness.ClientOptions()), ownedListener.LocalEndPoint);
        Assert.True(WaitFor(other, RecordedEventKind.Connected));
        Assert.True(WaitFor(serverSink, RecordedEventKind.Connected));
        Assert.Equal(1, ownedListener.LiveTransportCount);
        ownedListener.Dispose();
        Assert.True(Spin.Until(() => ownedListener.OpenConfigurationCount == 0, Timeout));
        Assert.Throws<ObjectDisposedException>(() => ownedListener.UpdateCertificate(scope.Harness.Certificate));
        server!.Dispose();
        Assert.True(server.WaitForHandlesClosed(Timeout));
        Assert.True(Spin.Until(() => ownedListener.LiveTransportCount == 0, Timeout));
        scope.Finish();
    }

    [Fact]
    public void A_dns_end_point_is_resolved_by_msquic()
    {
        using var scope = new Scope();
        var accepted = new ConcurrentQueue<(MsQuicTransport, RecordingSink)>();
        MsQuicTransportListener listener = Listen(scope, accepted, endPoint: new IPEndPoint(IPAddress.IPv6Any, 0));
        int port = listener.LocalEndPoint.Port;
        Assert.NotEqual(0, port);
        (MsQuicTransport client, RecordingSink sink) = Dial(scope, scope.Harness.CreateConnector(scope.Harness.ClientOptions()), new DnsEndPoint("localhost", port), serverName: null);
        Assert.True(WaitFor(sink, RecordedEventKind.Connected));
        Assert.Equal("localhost", client.ServerName);
        scope.Finish();
    }
}
