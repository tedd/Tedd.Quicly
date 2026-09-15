using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Conformance;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Transport;

/// <summary>
/// Review (contract and lifetime lens): contract rules the shared conformance suite does not encode yet, run against
/// <see cref="MsQuicTransport"/> over loopback. The scenario bodies are harness-agnostic and identical to the ones in
/// <c>tests/Tedd.Quicly.Testing.Tests/Conformance/ReviewContractTests.cs</c> (run against the simulator), so both transports
/// are held to the same rules; they belong in <c>TransportConformance</c> once the findings are fixed.
/// </summary>
[Collection(MsQuicCollection.Name)]
public unsafe class ReviewContractTests
{
    [Fact]
    public void Start_refused_inside_a_callback_is_reported_exactly_once() => Run(StartRefusedInsideACallbackIsReportedOnce);

    [Fact]
    public void Datagrams_in_flight_at_close_reach_a_final_state_before_OnClosed() => Run(DatagramsInFlightAtCloseReachAFinalStateBeforeOnClosed);

    [Fact]
    public void Aborting_a_never_started_stream_releases_it_without_callbacks_and_stale_ids_are_ignored() => Run(AbortingANeverStartedStreamReleasesItWithoutCallbacks);

    [Fact]
    public void CloseStream_before_shutdown_aborts_with_code_zero_and_suppresses_the_shutdown_callback() => Run(CloseStreamBeforeShutdownAbortsWithCodeZeroAndSuppressesTheShutdownCallback);

    [Fact]
    public void A_stream_refused_for_the_stream_limit_cannot_be_started_again() => Run(AStreamRefusedForTheStreamLimitCannotBeStartedAgain);

    /// <summary>
    /// PreHandshake Reject and a null accept: the client sees exactly one OnClosed(Transport) and nothing else, the accept
    /// callback is not called after a Reject, and the transport handed to an accept callback that refused it is Closed
    /// (nothing will ever happen on it), before and after Dispose.
    /// </summary>
    [Fact]
    public void Refusals_close_the_client_once_and_leave_the_refused_transport_closed()
    {
        var harness = new MsQuicTransportHarness();
        try
        {
            ITransport? refused = null;
            int acceptsAfterReject = 0;
            MsQuicTransportListener rejecting = harness.StartListener(harness.ServerOptions(), static (in NewConnectionInfo _) => PreHandshakeDecision.Reject, (ITransport _, in NewConnectionInfo _) =>
            {
                Interlocked.Increment(ref acceptsAfterReject);
                return null;
            });
            MsQuicTransportListener nulling = harness.StartListener(harness.ServerOptions(), static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, (ITransport t, in NewConnectionInfo _) =>
            {
                Volatile.Write(ref refused, t);
                return null;
            });
            MsQuicTransportConnector connector = harness.CreateConnector(harness.ClientOptions());
            foreach ((MsQuicTransportListener listener, string what) in new[] { (rejecting, "PreHandshake Reject"), (nulling, "accept returning null") })
            {
                var sink = new RecordingSink();
                MsQuicTransport client = harness.Track(connector.Connect(listener.LocalEndPoint, "localhost", sink));
                sink.Transport = client;
                Assert.True(sink.WaitFor(static e => e.Kind == RecordedEventKind.Closed, harness.DefaultTimeout), $"{what}: the client never reported OnClosed.");
                Thread.Sleep(200);
                RequireRefusedClient(harness.Name, sink, what);
            }
            Assert.Equal(0, Volatile.Read(ref acceptsAfterReject));
            ITransport? transport = Volatile.Read(ref refused);
            Assert.NotNull(transport);
            Assert.True(transport!.State == TransportState.Closed, $"the transport an accept callback refused reports {transport.State}; nothing will ever happen on it, so it must be Closed.");
            transport.Dispose();
            Assert.True(transport.State == TransportState.Closed, $"after Dispose the refused transport reports {transport.State}.");
        }
        finally
        {
            harness.Dispose();
        }
        Assert.Null(harness.CleanupError);
    }

    /// <summary>
    /// MsQuicTransportListener.CertificateRetired: "Called ... when every configuration that used a certificate has been
    /// closed ... the owner may dispose the certificate then". Re-applying the certificate the listener already serves (a
    /// renewal that returned the same instance) must not report it retired while the new configurations still use it.
    /// </summary>
    [Fact]
    public void Re_applying_the_current_certificate_does_not_retire_it()
    {
        var harness = new MsQuicTransportHarness();
        try
        {
            var retired = new ConcurrentQueue<X509Certificate2>();
            MsQuicTransportListener listener = harness.StartListener(harness.ServerOptions(), static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, (ITransport t, in NewConnectionInfo _) =>
                new RecordingSink { Transport = harness.Track((MsQuicTransport)t) });
            listener.CertificateRetired = retired.Enqueue;
            X509Certificate2 current = listener.CurrentCertificate;
            listener.UpdateCertificate(current);
            Thread.Sleep(200);
            Assert.Same(current, listener.CurrentCertificate);
            Assert.True(retired.IsEmpty, $"CertificateRetired reported {retired.Count} certificate(s) although the listener still serves the same instance: an owner following the contract disposes it while it is in use.");
        }
        finally
        {
            harness.Dispose();
        }
        Assert.Null(harness.CleanupError);
    }

    private static void Run(Action<ITransportTestHarness, Arena> scenario)
    {
        var harness = new MsQuicTransportHarness();
        var arena = new Arena();
        try
        {
            scenario(harness, arena);
        }
        finally
        {
            harness.Dispose();
            // A transport may read payloads until its handles are closed: leak the memory rather than free it under one.
            if (harness.CleanupError is null) arena.Dispose();
        }
        Assert.Null(harness.CleanupError);
        Assert.Equal(0, harness.SinkExceptionTotal);
    }

    // ================================================================== harness-agnostic review scenarios (keep identical in both test projects)

    /// <summary>Native memory for payloads and segment arrays.</summary>
    private sealed class Arena : IDisposable
    {
        private readonly List<nint> _blocks = [];

        public byte* Bytes(int length)
        {
            var p = (byte*)NativeMemory.AllocZeroed((nuint)Math.Max(1, length));
            for (int i = 0; i < length; i++) p[i] = (byte)((i * 7) + 3);
            lock (_blocks) _blocks.Add((nint)p);
            return p;
        }

        public TransportSegment* Segments(int count)
        {
            var p = (TransportSegment*)NativeMemory.AllocZeroed((nuint)(Math.Max(1, count) * sizeof(TransportSegment)));
            lock (_blocks) _blocks.Add((nint)p);
            return p;
        }

        public void Dispose()
        {
            lock (_blocks)
            {
                foreach (nint p in _blocks) NativeMemory.Free((void*)p);
                _blocks.Clear();
            }
        }
    }

    /// <summary>Inner sink for a <see cref="RecordingSink"/>: consumes everything and closes streams at shutdown, as the contract asks.</summary>
    private class ReviewSink : ITransportSink
    {
        public volatile ITransport? Transport;

        public virtual void OnConnected(in TransportConnectedInfo info)
        {
        }

        public virtual void OnDatagramReceived(ReadOnlySpan<byte> payload)
        {
        }

        public virtual void OnPeerStreamStarted(TransportStreamId id, StreamKind kind)
        {
        }

        public virtual void OnStreamStarted(TransportStreamId id, ulong context, TransportStatus status)
        {
        }

        public virtual ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin)
        {
            int total = 0;
            foreach (TransportSegment segment in segments) total += (int)segment.Length;
            return ReceiveResult.Consumed(total);
        }

        public virtual void OnStreamSendCompleted(TransportStreamId id, ulong context, bool canceled)
        {
        }

        public virtual void OnDatagramSendStateChanged(ulong context, DatagramSendState state)
        {
        }

        public virtual void OnStreamAborted(TransportStreamId id, ulong errorCode, StreamAbortDirection direction)
        {
        }

        public virtual void OnStreamPeerSendShutdown(TransportStreamId id)
        {
        }

        public virtual void OnStreamShutdownComplete(TransportStreamId id) => Transport?.CloseStream(id);

        public virtual void OnDatagramCapabilityChanged(bool enabled, int maxPayload)
        {
        }

        public virtual void OnIdealSendBufferSize(TransportStreamId id, ulong bytes)
        {
        }

        public virtual void OnStreamsAvailable(ushort bidirectional, ushort unidirectional)
        {
        }

        public virtual void OnPeerAddressChanged(in TransportConnectedInfo info)
        {
        }

        public virtual void OnClosed(TransportCloseReason reason, ulong errorCode, int transportStatus)
        {
        }
    }

    /// <summary>Starts two streams from inside the first datagram callback (MsQuic runs such calls inline).</summary>
    private sealed class InlineStarter : ReviewSink
    {
        public TransportStreamId StartId;
        public TransportStreamId SendId;
        public TransportSegment* Segment;
        public TransportStatus StartResult;
        public TransportStatus SendResult;
        public volatile bool Done;
        private int _fired;

        public override void OnDatagramReceived(ReadOnlySpan<byte> payload)
        {
            if (Interlocked.Exchange(ref _fired, 1) != 0) return;
            ITransport transport = Transport!;
            StartResult = transport.StartStream(StartId);
            SendResult = transport.SendStream(SendId, Segment, 1, 77, TransportSendFlags.Start | TransportSendFlags.Fin);
            Done = true;
        }
    }

    /// <summary>
    /// A start refused for the peer's stream limit is reported exactly once, also when the call is made from inside a
    /// callback: either synchronously (the call returns StreamLimitReached and nothing follows for the stream: no
    /// OnStreamStarted, no send completion, no shutdown callback) or asynchronously (the call returns Success, exactly one
    /// OnStreamStarted carries StreamLimitReached and a send accepted with the start completes canceled). ITransport: "If a
    /// send call returns anything but Success no completion follows"; TransportStatus: "On any value other than Success no
    /// completion follows".
    /// </summary>
    private static void StartRefusedInsideACallbackIsReportedOnce(ITransportTestHarness h, Arena arena)
    {
        var starter = new InlineStarter();
        (ITransport client, ITransport server, RecordingSink cs, RecordingSink ss) = Connect(h, starter, new ConformancePairOptions { ServerPeerUnidiStreams = 0 });
        Wait(h, () => client.Capabilities.Datagrams && server.Capabilities.Datagrams, "datagram support on both ends");
        byte* data = arena.Bytes(64);
        TransportSegment* segments = arena.Segments(2);
        segments[0] = new TransportSegment(data, 64);
        segments[1] = new TransportSegment(data, 8);
        Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Unidirectional, 0xA, 32767, out TransportStreamId a));
        Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Unidirectional, 0xB, 32767, out TransportStreamId b));
        starter.StartId = a;
        starter.SendId = b;
        starter.Segment = segments;
        Assert.Equal(TransportStatus.Success, server.SendDatagram(segments + 1, 1, 1, TransportSendFlags.None));
        Wait(h, () => starter.Done, "the client callback that starts both streams");
        Settle(h, 300);
        RequireReportedOnce(h, cs, a, starter.StartResult, null, "StartStream inside a callback");
        RequireReportedOnce(h, cs, b, starter.SendResult, 77, "SendStream(Start | Fin) inside a callback");
        client.Close(0, default);
        Wait(h, () => cs.IsClosed && ss.IsClosed, "OnClosed on both ends");
    }

    private static void RequireReportedOnce(ITransportTestHarness h, RecordingSink sink, TransportStreamId id, TransportStatus returned, ulong? sendContext, string what)
    {
        int completions = 0;
        bool allCanceled = true;
        foreach (RecordedEvent e in sink.OfKind(RecordedEventKind.StreamSendCompleted))
        {
            if (sendContext is null || e.Context != sendContext.Value) continue;
            completions++;
            allCanceled &= e.Canceled;
        }
        List<RecordedEvent> started = OfStream(sink, RecordedEventKind.StreamStarted, id);
        if (returned == TransportStatus.Success)
        {
            Assert.True(started.Count == 1 && started[0].Status == TransportStatus.StreamLimitReached,
                $"[{h.Name}] {what} returned Success, so exactly one OnStreamStarted(StreamLimitReached) must follow for {id}; events:{Environment.NewLine}{Dump(sink)}");
            if (sendContext is not null)
            {
                Assert.True(completions == 1 && allCanceled, $"[{h.Name}] {what} returned Success, so its send must complete exactly once, canceled; events:{Environment.NewLine}{Dump(sink)}");
            }
            return;
        }
        Assert.True(returned == TransportStatus.StreamLimitReached, $"[{h.Name}] {what} returned {returned}, expected StreamLimitReached (or Success with an asynchronous report).");
        int streamEvents = 0;
        foreach (RecordedEvent e in sink.Events) streamEvents += IsStreamEvent(e.Kind) && e.StreamId == id ? 1 : 0;
        Assert.True(streamEvents == 0 && completions == 0,
            $"[{h.Name}] {what} returned {returned} synchronously, yet {streamEvents} stream callback(s) and {completions} send completion(s) followed for {id} (a failed call must not be reported again); events:{Environment.NewLine}{Dump(sink)}");
    }

    /// <summary>Records that a datagram was handed to the network.</summary>
    private sealed class SentWatcher : ReviewSink
    {
        public volatile bool SentSeen;

        public override void OnDatagramSendStateChanged(ulong context, DatagramSendState state)
        {
            if (state == DatagramSendState.Sent) SentSeen = true;
        }
    }

    /// <summary>
    /// ITransportSink.OnClosed: "Before it, every accepted send has completed (in-flight stream sends canceled, datagrams in
    /// a final state)". The client closes from another thread the moment its first datagram is reported Sent, so that
    /// others are in flight (sent, not yet acknowledged) or still queued when the connection goes down; every accepted
    /// datagram must still report exactly one final state before OnClosed, and nothing may follow OnClosed.
    /// </summary>
    private static void DatagramsInFlightAtCloseReachAFinalStateBeforeOnClosed(ITransportTestHarness h, Arena arena)
    {
        const int Count = 48;
        const int Length = 1000;
        var watcher = new SentWatcher();
        (ITransport client, ITransport server, RecordingSink cs, RecordingSink ss) = Connect(h, watcher);
        Wait(h, () => client.Capabilities.Datagrams && server.Capabilities.Datagrams, "datagram support on both ends");
        Assert.True(client.Capabilities.MaxDatagramPayload >= Length, $"[{h.Name}] MaxDatagramPayload is {client.Capabilities.MaxDatagramPayload}.");
        byte* data = arena.Bytes(Count * Length);
        TransportSegment* segments = arena.Segments(Count);
        for (int i = 0; i < Count; i++) segments[i] = new TransportSegment(data + (i * Length), Length);
        var closer = new Thread(() =>
        {
            long deadline = Environment.TickCount64 + 30_000;
            while (!watcher.SentSeen && Environment.TickCount64 < deadline) Thread.SpinWait(20);
            client.Close(3, default);
        })
        {
            IsBackground = true,
            Name = "review-closer",
        };
        closer.Start();
        var accepted = new List<ulong>();
        for (int i = 0; i < Count; i++)
        {
            if (client.SendDatagram(segments + i, 1, (ulong)(i + 1), TransportSendFlags.None) == TransportStatus.Success) accepted.Add((ulong)(i + 1));
        }
        Wait(h, () => cs.IsClosed && ss.IsClosed, "OnClosed on both ends");
        Assert.True(closer.Join(TimeSpan.FromSeconds(30)), "the closing thread did not finish.");
        Settle(h, 200);
        IReadOnlyList<RecordedEvent> events = cs.Events;
        int closed = IndexOf(events, RecordedEventKind.Closed);
        Assert.True(closed >= 0 && closed == events.Count - 1, $"[{h.Name}] callbacks followed OnClosed; events:{Environment.NewLine}{Dump(cs)}");
        Assert.True(accepted.Count > 0, $"[{h.Name}] no datagram was accepted.");
        var missing = new List<ulong>();
        foreach (ulong context in accepted)
        {
            int finals = 0;
            for (int i = 0; i < closed; i++)
            {
                RecordedEvent e = events[i];
                if (e.Kind == RecordedEventKind.DatagramSendStateChanged && e.Context == context && e.DatagramState.IsFinal()) finals++;
            }
            if (finals != 1) missing.Add(context);
        }
        Assert.True(missing.Count == 0,
            $"[{h.Name}] {missing.Count} of {accepted.Count} accepted datagrams did not report exactly one final state before OnClosed (contexts {string.Join(", ", missing)}); events:{Environment.NewLine}{Dump(cs)}");
    }

    /// <summary>
    /// ITransport.AbortStream: "Aborting a local stream that was never started releases it like CloseStream: no callback
    /// follows for it and its id is stale afterwards" (every direction, both kinds). Stale ids are ignored even when their
    /// slot has been reused by a newer stream (generation check), which keeps working.
    /// </summary>
    private static void AbortingANeverStartedStreamReleasesItWithoutCallbacks(ITransportTestHarness h, Arena arena)
    {
        (ITransport client, ITransport server, RecordingSink cs, RecordingSink ss) = Connect(h);
        byte* data = arena.Bytes(32);
        TransportSegment* segment = arena.Segments(1);
        *segment = new TransportSegment(data, 32);
        (StreamKind Kind, StreamAbortDirection Direction)[] cases =
        [
            (StreamKind.Bidirectional, StreamAbortDirection.Send), (StreamKind.Bidirectional, StreamAbortDirection.Receive), (StreamKind.Bidirectional, StreamAbortDirection.Both),
            (StreamKind.Unidirectional, StreamAbortDirection.Send), (StreamKind.Unidirectional, StreamAbortDirection.Receive), (StreamKind.Unidirectional, StreamAbortDirection.Both),
        ];
        var aborted = new List<TransportStreamId>();
        for (int i = 0; i < cases.Length; i++)
        {
            Assert.Equal(TransportStatus.Success, client.OpenStream(cases[i].Kind, (ulong)(0x100 + i), 32767, out TransportStreamId id));
            client.AbortStream(id, 0x55, cases[i].Direction);
            Assert.True(client.StartStream(id) == TransportStatus.InvalidState, $"[{h.Name}] StartStream after aborting never-started {id} ({cases[i]}) did not return InvalidState.");
            Assert.True(client.SendStream(id, segment, 1, (ulong)(900 + i), TransportSendFlags.Start) == TransportStatus.InvalidState, $"[{h.Name}] SendStream(Start) after aborting never-started {id} ({cases[i]}) did not return InvalidState.");
            Assert.Equal(-1L, client.GetQuicStreamId(id));
            aborted.Add(id);
        }
        Settle(h, 50);
        Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Bidirectional, 0x200, 32767, out TransportStreamId live));
        foreach (TransportStreamId stale in aborted)
        {
            client.AbortStream(stale, 0x66, StreamAbortDirection.Both);
            client.SetStreamPriority(stale, 1);
            client.ResumeStreamReceive(stale, 0);
            client.CloseStream(stale);
            Assert.Equal(TransportStatus.InvalidState, client.StartStream(stale));
        }
        Assert.Equal(TransportStatus.Success, client.SendStream(live, segment, 1, 1000, TransportSendFlags.Start | TransportSendFlags.Fin));
        Wait(h, () => ss.CountOf(RecordedEventKind.StreamPeerSendShutdown) == 1, "the live stream's data and FIN at the server");
        TransportStreamId peer = ss.OfKind(RecordedEventKind.PeerStreamStarted)[0].StreamId;
        Assert.Equal(new ReadOnlySpan<byte>(data, 32).ToArray(), ss.GetStreamData(peer));
        Assert.Equal(1, ss.CountOf(RecordedEventKind.PeerStreamStarted));
        Assert.Equal(0, ss.CountOf(RecordedEventKind.StreamAborted));
        client.Close(0, default);
        Wait(h, () => cs.IsClosed && ss.IsClosed, "OnClosed on both ends");
        Settle(h, 100);
        foreach (RecordedEvent e in cs.Events)
        {
            Assert.False(IsStreamEvent(e.Kind) && aborted.Contains(e.StreamId), $"[{h.Name}] {e} was raised for a stream AbortStream released before it started; events:{Environment.NewLine}{Dump(cs)}");
            Assert.False(e.Kind == RecordedEventKind.StreamSendCompleted && e.Context >= 900 && e.Context < 900 + (ulong)cases.Length, $"[{h.Name}] a refused send completed: {e}");
        }
    }

    /// <summary>
    /// ITransport.CloseStream before OnStreamShutdownComplete: "aborts both directions with error code 0: completions of
    /// pending sends are still reported (canceled), OnStreamShutdownComplete is not. The id is stale afterwards".
    /// </summary>
    private static void CloseStreamBeforeShutdownAbortsWithCodeZeroAndSuppressesTheShutdownCallback(ITransportTestHarness h, Arena arena)
    {
        const int Sends = 8;
        const int Chunk = 64 * 1024;
        (ITransport client, ITransport server, RecordingSink cs, RecordingSink ss) = Connect(h);
        ss.ReceiveHandler = static (_, _, _, _) => ReceiveResult.PendingAfter(0);
        byte* data = arena.Bytes(Chunk);
        TransportSegment* segment = arena.Segments(1);
        *segment = new TransportSegment(data, Chunk);
        Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Bidirectional, 1, 32767, out TransportStreamId id));
        for (int i = 0; i < Sends; i++)
        {
            Assert.Equal(TransportStatus.Success, client.SendStream(id, segment, 1, (ulong)(i + 1), i == 0 ? TransportSendFlags.Start : TransportSendFlags.None));
        }
        Wait(h, () => ss.CountOf(RecordedEventKind.StreamReceived) > 0, "the stream's first data at the server");
        TransportStreamId peer = ss.OfKind(RecordedEventKind.PeerStreamStarted)[0].StreamId;
        client.CloseStream(id);
        Assert.Equal(TransportStatus.InvalidState, client.SendStream(id, segment, 1, 99, TransportSendFlags.None));
        Wait(h, () => OfStream(cs, RecordedEventKind.StreamSendCompleted, id).Count >= Sends, "a completion for every accepted send");
        Wait(h, () => HasAbort(ss, peer, 0, StreamAbortDirection.Send) && OfStream(ss, RecordedEventKind.StreamShutdownComplete, peer).Count == 1, "the reset (code 0) and the shutdown at the server");
        Settle(h, 200);
        var contexts = new HashSet<ulong>();
        foreach (RecordedEvent e in OfStream(cs, RecordedEventKind.StreamSendCompleted, id))
        {
            Assert.True(contexts.Add(e.Context), $"[{h.Name}] send {e.Context} completed twice; events:{Environment.NewLine}{Dump(cs)}");
        }
        Assert.Equal(Sends, contexts.Count);
        client.Close(0, default);
        Wait(h, () => cs.IsClosed && ss.IsClosed, "OnClosed on both ends");
        Assert.True(OfStream(cs, RecordedEventKind.StreamShutdownComplete, id).Count == 0, $"[{h.Name}] OnStreamShutdownComplete was raised for a stream closed before its shutdown; events:{Environment.NewLine}{Dump(cs)}");
    }

    /// <summary>
    /// A start refused for the peer's stream limit (synchronously or through OnStreamStarted) leaves a stream that never
    /// starts: StartStream on it keeps returning InvalidState even after OnStreamsAvailable, so portable code releases it
    /// with CloseStream and opens a new stream. (MsQuic must shut such a stream down: the task prescribes
    /// FAIL_BLOCKED | SHUTDOWN_ON_FAIL.)
    /// </summary>
    private static void AStreamRefusedForTheStreamLimitCannotBeStartedAgain(ITransportTestHarness h, Arena arena)
    {
        (ITransport client, ITransport server, RecordingSink cs, RecordingSink ss) = Connect(h, options: new ConformancePairOptions { ServerPeerUnidiStreams = 0 });
        Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Unidirectional, 7, 32767, out TransportStreamId refused));
        TransportStatus start = client.StartStream(refused);
        if (start == TransportStatus.Success)
        {
            Wait(h, () => OfStream(cs, RecordedEventKind.StreamStarted, refused).Count == 1, "OnStreamStarted for the refused start");
            Assert.Equal(TransportStatus.StreamLimitReached, OfStream(cs, RecordedEventKind.StreamStarted, refused)[0].Status);
        }
        else
        {
            Assert.Equal(TransportStatus.StreamLimitReached, start);
        }
        int before = cs.Count;
        server.UpdatePeerStreamLimits(16, 4);
        Wait(h, () => HasUnidirectionalCreditAfter(cs, before), "OnStreamsAvailable with unidirectional credit");
        TransportStatus retry = client.StartStream(refused);
        Assert.True(retry == TransportStatus.InvalidState,
            $"[{h.Name}] StartStream on a stream whose start was refused for the stream limit returned {retry} after OnStreamsAvailable; the transports disagree on whether such a stream can be restarted; events:{Environment.NewLine}{Dump(cs)}");
        client.CloseStream(refused);
        byte* data = arena.Bytes(16);
        TransportSegment* segment = arena.Segments(1);
        *segment = new TransportSegment(data, 16);
        Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Unidirectional, 8, 32767, out TransportStreamId fresh));
        Assert.Equal(TransportStatus.Success, client.SendStream(fresh, segment, 1, 1, TransportSendFlags.Start | TransportSendFlags.Fin));
        Wait(h, () => ss.CountOf(RecordedEventKind.StreamPeerSendShutdown) == 1, "the new stream's data at the server");
        client.Close(0, default);
        Wait(h, () => cs.IsClosed && ss.IsClosed, "OnClosed on both ends");
    }

    // ------------------------------------------------------------------ helpers

    private static (ITransport Client, ITransport Server, RecordingSink ClientSink, RecordingSink ServerSink) Connect(ITransportTestHarness h, ReviewSink? clientInner = null, ConformancePairOptions? options = null)
    {
        var clientSink = new RecordingSink(null, clientInner);
        var serverSink = new RecordingSink();
        ConformancePair pair = h.CreatePair(clientSink, serverSink, options);
        clientSink.Transport = pair.Client;
        serverSink.Transport = pair.Server;
        if (clientInner is not null) clientInner.Transport = pair.Client;
        Wait(h, () => clientSink.CountOf(RecordedEventKind.Connected) == 1 && serverSink.CountOf(RecordedEventKind.Connected) == 1, "OnConnected on both ends");
        return (pair.Client, pair.Server, clientSink, serverSink);
    }

    private static void Wait(ITransportTestHarness h, Func<bool> condition, string what)
        => Assert.True(h.Pump(condition, h.DefaultTimeout), $"[{h.Name}] timed out waiting for {what}.");

    private static void Settle(ITransportTestHarness h, int milliseconds) => h.Pump(static () => false, TimeSpan.FromMilliseconds(milliseconds));

    private static List<RecordedEvent> OfStream(RecordingSink sink, RecordedEventKind kind, TransportStreamId id)
    {
        var result = new List<RecordedEvent>();
        foreach (RecordedEvent e in sink.OfKind(kind))
        {
            if (e.StreamId == id) result.Add(e);
        }
        return result;
    }

    private static int IndexOf(IReadOnlyList<RecordedEvent> events, RecordedEventKind kind)
    {
        for (int i = 0; i < events.Count; i++)
        {
            if (events[i].Kind == kind) return i;
        }
        return -1;
    }

    private static bool HasAbort(RecordingSink sink, TransportStreamId id, ulong code, StreamAbortDirection direction)
    {
        foreach (RecordedEvent e in sink.OfKind(RecordedEventKind.StreamAborted))
        {
            if (e.StreamId == id && e.ErrorCode == code && e.Direction == direction) return true;
        }
        return false;
    }

    private static bool HasUnidirectionalCreditAfter(RecordingSink sink, int index)
    {
        IReadOnlyList<RecordedEvent> events = sink.Events;
        for (int i = index; i < events.Count; i++)
        {
            if (events[i].Kind == RecordedEventKind.StreamsAvailable && events[i].Unidirectional > 0) return true;
        }
        return false;
    }

    private static bool IsStreamEvent(RecordedEventKind kind) => kind is RecordedEventKind.PeerStreamStarted or RecordedEventKind.StreamStarted
        or RecordedEventKind.StreamReceived or RecordedEventKind.StreamSendCompleted or RecordedEventKind.StreamAborted
        or RecordedEventKind.StreamPeerSendShutdown or RecordedEventKind.StreamShutdownComplete or RecordedEventKind.IdealSendBufferSize;

    private static void RequireRefusedClient(string name, RecordingSink sink, string what)
    {
        IReadOnlyList<RecordedEvent> events = sink.Events;
        Assert.True(events.Count > 0 && events[^1].Kind == RecordedEventKind.Closed, $"[{name}] {what}: OnClosed is not the last event; events:{Environment.NewLine}{Dump(sink)}");
        Assert.Equal(1, sink.CountOf(RecordedEventKind.Closed));
        Assert.Equal(TransportCloseReason.Transport, events[^1].CloseReason);
        foreach (RecordedEvent e in events)
        {
            Assert.True(e.Kind is RecordedEventKind.Closed or RecordedEventKind.DatagramCapabilityChanged, $"[{name}] {what}: {e.Kind} was raised on a refused connection; events:{Environment.NewLine}{Dump(sink)}");
        }
    }

    private static string Dump(RecordingSink sink)
    {
        var text = new StringBuilder();
        IReadOnlyList<RecordedEvent> events = sink.Events;
        int start = Math.Max(0, events.Count - 80);
        if (start > 0) text.Append("  ... ").Append(start).AppendLine(" earlier events");
        for (int i = start; i < events.Count; i++) text.Append("  ").AppendLine(events[i].ToString());
        return text.ToString();
    }
}
