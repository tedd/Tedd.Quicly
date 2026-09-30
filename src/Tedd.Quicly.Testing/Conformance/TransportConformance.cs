using System.Buffers.Binary;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Testing.Conformance;

/// <summary>
/// Transport-agnostic scenarios that check the <see cref="ITransport"/> / <see cref="ITransportSink"/> contract. Each scenario
/// is a static method that runs against an <see cref="ITransportTestHarness"/> and throws <see cref="ConformanceException"/>
/// on a violation, so every transport's test project can wrap them in tests (see <see cref="ScenarioNames"/> and
/// <see cref="Run"/>).
/// </summary>
/// <remarks>
/// The scenarios encode the contract, not one implementation. Where transports legitimately differ, every allowed outcome is
/// accepted: callbacks may run inline inside an API call (MsQuic) or only later (simulator); a stream-limit failure may be
/// returned by <see cref="ITransport.StartStream"/> or reported through <see cref="ITransportSink.OnStreamStarted"/>; a
/// capability report may come before or after <see cref="ITransportSink.OnConnected"/>; when both ends close at once either
/// may win. Scenarios that need a listener's callbacks, a failing handshake or a connection that ends on its own use
/// <see cref="ITransportTestHarness.Connect"/>, <see cref="ConformancePairOptions.FailHandshake"/> and
/// <see cref="ConformancePairOptions.TransportCloseAfter"/>.
/// </remarks>
public static unsafe partial class TransportConformance
{
    private static readonly (string Name, Action<ITransportTestHarness> Run)[] s_scenarios =
    [
        (nameof(ConnectRaisesOnConnectedOnBothEnds), ConnectRaisesOnConnectedOnBothEnds),
        (nameof(CapabilityChangeMayPrecedeOnConnected), CapabilityChangeMayPrecedeOnConnected),
        (nameof(DatagramRoundTrip), DatagramRoundTrip),
        (nameof(DatagramSendStatesReachExactlyOneFinalState), DatagramSendStatesReachExactlyOneFinalState),
        (nameof(OversizedDatagramIsTooLargeSynchronously), OversizedDatagramIsTooLargeSynchronously),
        (nameof(StreamLifecycleOrdering), StreamLifecycleOrdering),
        (nameof(StreamDataIntegrityWithArbitraryConsumption), StreamDataIntegrityWithArbitraryConsumption),
        (nameof(PartialConsumptionIsIndicatedAgain), PartialConsumptionIsIndicatedAgain),
        (nameof(PeerStreamLimitThenUpdatePeerStreamLimits), PeerStreamLimitThenUpdatePeerStreamLimits),
        (nameof(AbortStreamPropagatesCodesInBothDirections), AbortStreamPropagatesCodesInBothDirections),
        (nameof(CloseReportsLocalAndPeerWithCode), CloseReportsLocalAndPeerWithCode),
        (nameof(NoCallbacksAfterOnClosed), NoCallbacksAfterOnClosed),
        (nameof(InFlightSendsCompleteCanceledAtClose), InFlightSendsCompleteCanceledAtClose),
        (nameof(RefusedStreamNeverStartsAndIsRetriedOnANewStream), RefusedStreamNeverStartsAndIsRetriedOnANewStream),
        (nameof(StartRefusedInsideACallbackIsReportedOnce), StartRefusedInsideACallbackIsReportedOnce),
        (nameof(AbortingANeverStartedStreamReleasesItWithoutCallbacks), AbortingANeverStartedStreamReleasesItWithoutCallbacks),
        (nameof(CloseStreamBeforeShutdownAbortsWithCodeZero), CloseStreamBeforeShutdownAbortsWithCodeZero),
        (nameof(DatagramsInFlightAtCloseReachAFinalStateBeforeOnClosed), DatagramsInFlightAtCloseReachAFinalStateBeforeOnClosed),
        (nameof(ResumeRacingTheReceiveCallbackIsApplied), ResumeRacingTheReceiveCallbackIsApplied),
        (nameof(HeldStreamIsIndicatedAgainAfterEveryResume), HeldStreamIsIndicatedAgainAfterEveryResume),
        (nameof(RefusedConnectionsCloseTheClientOnceAndLeaveTheRefusedTransportClosed), RefusedConnectionsCloseTheClientOnceAndLeaveTheRefusedTransportClosed),
        (nameof(HandshakeFailureClosesBothEndsWithoutConnecting), HandshakeFailureClosesBothEndsWithoutConnecting),
        (nameof(CloseWhileConnecting), CloseWhileConnecting),
        (nameof(BothEndsClosingAtOnce), BothEndsClosingAtOnce),
        (nameof(TransportInitiatedCloseReportsTransport), TransportInitiatedCloseReportsTransport),
    ];

    /// <summary>Names of every scenario, in a stable order.</summary>
    public static IReadOnlyList<string> ScenarioNames { get; } = Array.ConvertAll(s_scenarios, static s => s.Name);

    /// <summary>Runs the scenario called <paramref name="name"/> against <paramref name="harness"/>.</summary>
    /// <exception cref="ArgumentException">No scenario has that name.</exception>
    /// <exception cref="ConformanceException">The transport violated the contract.</exception>
    public static void Run(string name, ITransportTestHarness harness)
    {
        ArgumentNullException.ThrowIfNull(name);
        ArgumentNullException.ThrowIfNull(harness);
        foreach ((string scenario, Action<ITransportTestHarness> run) in s_scenarios)
        {
            if (scenario == name)
            {
                run(harness);
                return;
            }
        }
        throw new ArgumentException($"Unknown conformance scenario '{name}'.", nameof(name));
    }

    // ------------------------------------------------------------------ connection

    /// <summary>
    /// Both ends raise <see cref="ITransportSink.OnConnected"/> exactly once, with the same ALPN and matching end points;
    /// before it only capability reports may arrive; the state is <see cref="TransportState.Connected"/>.
    /// </summary>
    public static void ConnectRaisesOnConnectedOnBothEnds(ITransportTestHarness harness)
    {
        using var s = new Session(harness);
        s.WaitConnected();
        RecordedEvent client = Single(s, s.ClientSink, static e => e.Kind == RecordedEventKind.Connected, "client OnConnected");
        RecordedEvent server = Single(s, s.ServerSink, static e => e.Kind == RecordedEventKind.Connected, "server OnConnected");
        s.Require(s.Client.State == TransportState.Connected, $"client State is {s.Client.State} after OnConnected.");
        s.Require(s.Server.State == TransportState.Connected, $"server State is {s.Server.State} after OnConnected.");
        s.Require(!string.IsNullOrEmpty(client.Alpn) && client.Alpn == server.Alpn, $"negotiated ALPN differs: client '{client.Alpn}', server '{server.Alpn}'.");
        s.Require(client.RemoteEndPoint is not null && client.LocalEndPoint is not null && server.RemoteEndPoint is not null && server.LocalEndPoint is not null,
            "TransportConnectedInfo lacks an end point.");
        s.Require(Session.SameEndPoint(client.RemoteEndPoint, server.LocalEndPoint), $"client remote {client.RemoteEndPoint} is not server local {server.LocalEndPoint}.");
        s.Require(Session.SameEndPoint(server.RemoteEndPoint, client.LocalEndPoint), $"server remote {server.RemoteEndPoint} is not client local {client.LocalEndPoint}.");
        RequireOnlyCapabilityBeforeConnected(s, s.ClientSink, "client");
        RequireOnlyCapabilityBeforeConnected(s, s.ServerSink, "server");
        s.CloseAndWait();
    }

    /// <summary>
    /// <see cref="ITransportSink.OnDatagramCapabilityChanged"/> may precede <see cref="ITransportSink.OnConnected"/>; the
    /// capabilities in <see cref="TransportConnectedInfo"/> agree with the last report before it, and
    /// <see cref="ITransport.Capabilities"/> agrees with the last report overall.
    /// </summary>
    public static void CapabilityChangeMayPrecedeOnConnected(ITransportTestHarness harness)
    {
        using var s = new Session(harness);
        s.WaitConnected();
        s.WaitDatagrams();
        CheckCapabilities(s, s.ClientSink, s.Client, "client");
        CheckCapabilities(s, s.ServerSink, s.Server, "server");
        s.CloseAndWait();
    }

    // ------------------------------------------------------------------ datagrams

    /// <summary>A gathered datagram from the client and a single-segment datagram from the server arrive intact.</summary>
    public static void DatagramRoundTrip(ITransportTestHarness harness)
    {
        using var s = new Session(harness);
        s.WaitConnected();
        s.WaitDatagrams();
        NativeBuffer data = s.Rent(400, seed: 3);
        NativeSegments segments = s.RentSegments(3);
        segments.Set(0, data.Segment(0, 100));
        segments.Set(1, data.Segment(100, 57));
        s.Require(s.Client.SendDatagram(segments.At(0), 2, 1, TransportSendFlags.None) == TransportStatus.Success, "client SendDatagram (two segments) was refused.");
        s.Wait(() => s.ServerSink.CountOf(RecordedEventKind.DatagramReceived) >= 1, "the client's datagram at the server");
        s.Require(s.ServerSink.OfKind(RecordedEventKind.DatagramReceived)[0].Data.AsSpan().SequenceEqual(data.ToArray(0, 157)), "the server received other bytes than the client sent.");
        segments.Set(2, data.Segment(200, 150));
        s.Require(s.Server.SendDatagram(segments.At(2), 1, 2, TransportSendFlags.Priority) == TransportStatus.Success, "server SendDatagram was refused.");
        s.Wait(() => s.ClientSink.CountOf(RecordedEventKind.DatagramReceived) >= 1, "the server's datagram at the client");
        s.Require(s.ClientSink.OfKind(RecordedEventKind.DatagramReceived)[0].Data.AsSpan().SequenceEqual(data.ToArray(200, 150)), "the client received other bytes than the server sent.");
        s.Wait(() => HasFinalState(s.ClientSink, 1) && HasFinalState(s.ServerSink, 2), "final send states of both datagrams");
        s.CloseAndWait();
    }

    /// <summary>
    /// Every accepted datagram reports its states in a legal order and exactly one final state; the payload is released at
    /// <see cref="DatagramSendState.Sent"/> or <see cref="DatagramSendState.Canceled"/>: overwriting it from inside that
    /// callback never corrupts what the peer receives.
    /// </summary>
    public static void DatagramSendStatesReachExactlyOneFinalState(ITransportTestHarness harness)
    {
        const int Count = 64;
        const int PayloadLength = 200;
        var payloads = new NativeBuffer(Count * PayloadLength);
        for (int i = 0; i < Count; i++) WriteDatagramPayload(payloads, i, PayloadLength);
        var scribbler = new ReleaseScribbler(payloads, PayloadLength, Count);
        using var s = new Session(harness, clientInner: scribbler);
        try
        {
            s.WaitConnected();
            s.WaitDatagrams();
            NativeSegments segments = s.RentSegments(Count);
            var accepted = new bool[Count + 1];
            int acceptedCount = 0;
            for (int i = 0; i < Count; i++)
            {
                segments.Set(i, payloads.Segment(i * PayloadLength, PayloadLength));
                TransportStatus status = s.Client.SendDatagram(segments.At(i), 1, (ulong)(i + 1), TransportSendFlags.None);
                s.Require(status == TransportStatus.Success, $"SendDatagram #{i + 1} returned {status}.");
                accepted[i + 1] = true;
                acceptedCount++;
            }
            s.Wait(() => CountFinalStates(s.ClientSink, Count) >= acceptedCount, $"a final state for each of the {acceptedCount} datagrams");
            s.Settle(TimeSpan.FromMilliseconds(100));

            var states = new List<DatagramSendState>[Count + 1];
            foreach (RecordedEvent e in s.ClientSink.OfKind(RecordedEventKind.DatagramSendStateChanged))
            {
                s.Require(e.Context >= 1 && e.Context <= Count, $"send state for unknown context {e.Context}.");
                (states[e.Context] ??= []).Add(e.DatagramState);
            }
            int acknowledged = 0;
            for (int context = 1; context <= Count; context++)
            {
                if (!accepted[context]) continue;
                List<DatagramSendState>? sequence = states[context];
                s.Require(sequence is { Count: > 0 }, $"datagram {context} reported no state.");
                string text = string.Join(",", sequence!);
                s.Require(!sequence!.Contains(DatagramSendState.Unknown), $"datagram {context} reported Unknown ({text}).");
                s.Require(sequence[^1].IsFinal(), $"datagram {context} did not end in a final state ({text}).");
                int finals = 0;
                foreach (DatagramSendState state in sequence) finals += state.IsFinal() ? 1 : 0;
                s.Require(finals == 1, $"datagram {context} reported {finals} final states ({text}).");
                int sent = sequence.IndexOf(DatagramSendState.Sent);
                s.Require(sent <= 0 && sequence.LastIndexOf(DatagramSendState.Sent) == sent, $"datagram {context}: Sent must come first and once ({text}).");
                if (sequence[^1] is DatagramSendState.Acknowledged or DatagramSendState.AcknowledgedSpurious) acknowledged++;
            }
            s.Require(acknowledged > 0, "no datagram was reported Acknowledged.");

            IReadOnlyList<RecordedEvent> received = s.ServerSink.OfKind(RecordedEventKind.DatagramReceived);
            s.Require(received.Count > 0, "the server received none of the datagrams.");
            foreach (RecordedEvent e in received)
            {
                s.Require(e.Data.Length == PayloadLength, $"received a datagram of {e.Data.Length} bytes, expected {PayloadLength}.");
                int index = BinaryPrimitives.ReadInt32LittleEndian(e.Data);
                s.Require(index >= 0 && index < Count && e.Data.AsSpan().SequenceEqual(ExpectedDatagramPayload(index, PayloadLength)),
                    $"datagram {index + 1} arrived corrupted: the transport read the payload after releasing it.");
            }
            s.CloseAndWait();
        }
        finally
        {
            if (s.BothClosed) payloads.Dispose();
        }
    }

    /// <summary>
    /// A datagram beyond <see cref="TransportCapabilities.MaxDatagramPayload"/> (single or gathered) is refused synchronously
    /// with <see cref="TransportStatus.TooLarge"/> and no state follows for it; one of exactly the maximum is accepted.
    /// </summary>
    public static void OversizedDatagramIsTooLargeSynchronously(ITransportTestHarness harness)
    {
        using var s = new Session(harness);
        s.WaitConnected();
        s.WaitDatagrams();
        int max = s.Client.Capabilities.MaxDatagramPayload;
        s.Require(max > 0, $"MaxDatagramPayload is {max} while datagrams are enabled.");
        // Well beyond the maximum, so a concurrent path-MTU increase cannot make it fit.
        int oversized = max + 1500;
        NativeBuffer data = s.Rent(oversized, seed: 5);
        NativeSegments segments = s.RentSegments(3);
        segments.Set(0, data.Segment(0, oversized));
        TransportStatus status = s.Client.SendDatagram(segments.At(0), 1, 99, TransportSendFlags.None);
        s.Require(status == TransportStatus.TooLarge, $"a {oversized}-byte datagram (max {max}) returned {status}, expected TooLarge.");
        segments.Set(1, data.Segment(0, oversized / 2));
        segments.Set(2, data.Segment(oversized / 2, oversized - (oversized / 2)));
        status = s.Client.SendDatagram(segments.At(1), 2, 98, TransportSendFlags.None);
        s.Require(status == TransportStatus.TooLarge, $"a gathered {oversized}-byte datagram returned {status}, expected TooLarge.");
        segments.Set(0, data.Segment(0, max));
        status = s.Client.SendDatagram(segments.At(0), 1, 100, TransportSendFlags.None);
        s.Require(status == TransportStatus.Success, $"a datagram of exactly MaxDatagramPayload ({max}) returned {status}.");
        s.Wait(() => HasFinalState(s.ClientSink, 100), "the final state of the maximum-size datagram");
        s.Settle(TimeSpan.FromMilliseconds(50));
        foreach (RecordedEvent e in s.ClientSink.OfKind(RecordedEventKind.DatagramSendStateChanged))
        {
            s.Require(e.Context is not (98 or 99), $"a refused datagram (context {e.Context}) reported {e.DatagramState}.");
        }
        s.CloseAndWait();
    }

    // ------------------------------------------------------------------ streams

    /// <summary>
    /// Open → start (with the first send) → send → FIN → peer-send-shutdown → shutdown-complete → CloseStream, in order on
    /// both ends, for a bidirectional and a unidirectional stream; invalid calls return <see cref="TransportStatus.InvalidState"/>.
    /// </summary>
    public static void StreamLifecycleOrdering(ITransportTestHarness harness)
    {
        using var s = new Session(harness);
        s.WaitConnected();
        NativeBuffer data = s.Rent(4096, seed: 6);
        NativeSegments segments = s.RentSegments(8);
        ITransport client = s.Client;
        ITransport server = s.Server;

        s.Require(client.OpenStream(StreamKind.Bidirectional, 0x51, 32767, out TransportStreamId id) == TransportStatus.Success && id.IsValid, "OpenStream(Bidirectional) failed.");
        s.Require(client.GetQuicStreamId(id) == -1, "GetQuicStreamId must be -1 before the stream started.");
        segments.Set(0, data.Segment(0, 1000));
        TransportStatus status = client.SendStream(id, segments.At(0), 1, 1, TransportSendFlags.None);
        s.Require(status == TransportStatus.InvalidState, $"a send without Start on an unstarted stream returned {status}.");
        status = client.SendStream(id, segments.At(0), 1, 1, TransportSendFlags.Start);
        s.Require(status == TransportStatus.Success, $"the first send with Start returned {status}.");
        segments.Set(1, data.Segment(1000, 500));
        status = client.SendStream(id, segments.At(1), 1, 2, TransportSendFlags.Fin);
        s.Require(status == TransportStatus.Success, $"the send with Fin returned {status}.");
        segments.Set(2, data.Segment(1500, 10));
        status = client.SendStream(id, segments.At(2), 1, 3, TransportSendFlags.None);
        s.Require(status == TransportStatus.InvalidState, $"a send after Fin returned {status}.");
        status = client.StartStream(id);
        s.Require(status == TransportStatus.InvalidState, $"starting a started stream returned {status}.");

        s.Wait(() => s.ServerSink.CountOf(RecordedEventKind.StreamPeerSendShutdown) >= 1 && CountStream(s.ClientSink, RecordedEventKind.StreamSendCompleted, id) >= 2,
            "the client's data, FIN and both send completions");
        RecordedEvent started = Single(s, s.ClientSink, e => e.Kind == RecordedEventKind.StreamStarted && e.StreamId == id, "OnStreamStarted");
        s.Require(started.Context == 0x51 && started.Status == TransportStatus.Success, $"OnStreamStarted reported context {started.Context:X} status {started.Status}.");
        RecordedEvent peer = Single(s, s.ServerSink, static e => e.Kind == RecordedEventKind.PeerStreamStarted, "OnPeerStreamStarted");
        s.Require(peer.StreamKind == StreamKind.Bidirectional, $"the peer stream is {peer.StreamKind}.");
        TransportStreamId peerId = peer.StreamId;
        long quicId = client.GetQuicStreamId(id);
        s.Require(quicId >= 0 && quicId == server.GetQuicStreamId(peerId), $"QUIC stream ids differ: client {quicId}, server {server.GetQuicStreamId(peerId)}.");
        s.Require(s.ServerSink.GetStreamData(peerId).AsSpan().SequenceEqual(data.ToArray(0, 1500)), "the server received other bytes than the client sent.");
        RequireCompletions(s, s.ClientSink, id, [1, 2], expectCanceled: false);
        RequireStartBeforeCompletions(s, s.ClientSink, id);
        RequireReceiveOrdering(s, s.ServerSink, peerId, "server");

        segments.Set(3, data.Segment(2000, 700));
        status = server.SendStream(peerId, segments.At(3), 1, 9, TransportSendFlags.Fin);
        s.Require(status == TransportStatus.Success, $"the server's reply on the peer stream returned {status}.");
        s.Wait(() => CountStream(s.ClientSink, RecordedEventKind.StreamShutdownComplete, id) == 1 && CountStream(s.ServerSink, RecordedEventKind.StreamShutdownComplete, peerId) == 1,
            "OnStreamShutdownComplete on both ends");
        s.Require(s.ClientSink.GetStreamData(id).AsSpan().SequenceEqual(data.ToArray(2000, 700)), "the client received other bytes than the server sent.");
        RequireCompletions(s, s.ServerSink, peerId, [9], expectCanceled: false);
        RequireReceiveOrdering(s, s.ClientSink, id, "client");
        RequireShutdownLast(s, s.ClientSink, id, "client");
        RequireShutdownLast(s, s.ServerSink, peerId, "server");
        status = client.SendStream(id, segments.At(2), 1, 4, TransportSendFlags.None);
        s.Require(status == TransportStatus.InvalidState, $"a send on a closed stream returned {status}.");

        s.Require(client.OpenStream(StreamKind.Unidirectional, 0x61, 32767, out TransportStreamId uni) == TransportStatus.Success, "OpenStream(Unidirectional) failed.");
        segments.Set(4, data.Segment(3000, 300));
        status = client.SendStream(uni, segments.At(4), 1, 11, TransportSendFlags.Start | TransportSendFlags.Fin);
        s.Require(status == TransportStatus.Success, $"a unidirectional Start|Fin send returned {status}.");
        s.Wait(() => s.ServerSink.CountOf(RecordedEventKind.PeerStreamStarted) >= 2, "the unidirectional peer stream");
        RecordedEvent uniPeer = s.ServerSink.OfKind(RecordedEventKind.PeerStreamStarted)[1];
        s.Require(uniPeer.StreamKind == StreamKind.Unidirectional, $"the second peer stream is {uniPeer.StreamKind}.");
        segments.Set(5, data.Segment(3500, 10));
        status = server.SendStream(uniPeer.StreamId, segments.At(5), 1, 12, TransportSendFlags.None);
        s.Require(status == TransportStatus.InvalidState, $"sending on a peer unidirectional stream returned {status}.");
        s.Wait(() => CountStream(s.ClientSink, RecordedEventKind.StreamShutdownComplete, uni) == 1 && CountStream(s.ServerSink, RecordedEventKind.StreamShutdownComplete, uniPeer.StreamId) == 1,
            "shutdown of the unidirectional stream on both ends");
        s.Require(s.ServerSink.GetStreamData(uniPeer.StreamId).AsSpan().SequenceEqual(data.ToArray(3000, 300)), "the unidirectional data differs.");
        RequireCompletions(s, s.ClientSink, uni, [11], expectCanceled: false);
        RequireReceiveOrdering(s, s.ServerSink, uniPeer.StreamId, "server");
        RequireShutdownLast(s, s.ClientSink, uni, "client");
        RequireShutdownLast(s, s.ServerSink, uniPeer.StreamId, "server");
        s.CloseAndWait();
    }

    /// <summary>
    /// Many gathered sends arrive byte-exact and in order while the receiver consumes arbitrary amounts: everything, part
    /// (the rest is indicated again), <see cref="ReceiveResult.PendingAfter"/> with a later
    /// <see cref="ITransport.ResumeStreamReceive"/> crediting more, and <c>Consumed(0)</c> (which counts as <c>PendingAfter(0)</c>).
    /// Every indication starts exactly where consumption stopped.
    /// </summary>
    public static void StreamDataIntegrityWithArbitraryConsumption(ITransportTestHarness harness)
    {
        const int Sends = 160;
        var receiver = new IntegrityReceiver(seed: 7);
        using var s = new Session(harness, serverInner: receiver);
        receiver.Transport = s.Server;
        s.WaitConnected();
        var random = new Random(11);
        var sizes = new int[Sends];
        int total = 0;
        for (int i = 0; i < Sends; i++)
        {
            sizes[i] = random.Next(1, 4000);
            total += sizes[i];
        }
        NativeBuffer data = s.Rent(total, seed: 9);
        NativeSegments segments = s.RentSegments(Sends * 2);
        s.Require(s.Client.OpenStream(StreamKind.Unidirectional, 0x70, 32767, out TransportStreamId id) == TransportStatus.Success, "OpenStream failed.");
        int offset = 0;
        for (int i = 0; i < Sends; i++)
        {
            // Two segments per send (gathered), split at an arbitrary point.
            int first = sizes[i] / 3;
            segments.Set(2 * i, data.Segment(offset, first));
            segments.Set((2 * i) + 1, data.Segment(offset + first, sizes[i] - first));
            TransportSendFlags flags = (i == 0 ? TransportSendFlags.Start : TransportSendFlags.None) | (i == Sends - 1 ? TransportSendFlags.Fin : TransportSendFlags.None);
            TransportStatus status = s.Client.SendStream(id, segments.At(2 * i), 2, (ulong)(i + 1), flags);
            s.Require(status == TransportStatus.Success, $"SendStream #{i + 1} returned {status}.");
            offset += sizes[i];
        }
        bool Done() => receiver.PeerShutdown && CountStream(s.ClientSink, RecordedEventKind.StreamSendCompleted, id) == Sends;
        while (!Done())
        {
            s.Wait(() => receiver.NeedsResume || Done(), $"stream data, a pending receive or send completions (consumed {receiver.ConsumedLength} of {total} bytes)");
            receiver.Resume();
        }
        s.Require(receiver.OffsetErrors == 0, $"{receiver.OffsetErrors} indications did not start where consumption stopped.");
        s.Require(receiver.ConsumedBytes.AsSpan().SequenceEqual(data.ToArray(0, total)), $"the consumed bytes ({receiver.ConsumedLength}) differ from the {total} bytes sent.");
        s.Require(CountStream(s.ServerSink, RecordedEventKind.StreamPeerSendShutdown, default) <= 1, "OnStreamPeerSendShutdown was raised more than once.");
        foreach (RecordedEvent e in s.ClientSink.OfKind(RecordedEventKind.StreamSendCompleted))
        {
            s.Require(!e.Canceled, $"send {e.Context} completed canceled.");
        }
        s.CloseAndWait();
    }

    /// <summary>
    /// A partial <see cref="ReceiveResult.Consumed"/> gets the rest indicated again at the next offset without new data;
    /// <c>Consumed(0)</c> of a non-empty indication holds delivery until <see cref="ITransport.ResumeStreamReceive"/>, which
    /// credits the resumed bytes.
    /// </summary>
    public static void PartialConsumptionIsIndicatedAgain(ITransportTestHarness harness)
    {
        using var s = new Session(harness);
        var offsets = new List<ulong>();
        s.ServerSink.ReceiveHandler = (_, segments, absoluteOffset, _) =>
        {
            int call;
            lock (offsets)
            {
                offsets.Add(absoluteOffset);
                call = offsets.Count;
            }
            int total = 0;
            foreach (TransportSegment segment in segments) total += (int)segment.Length;
            return call switch
            {
                1 => ReceiveResult.Consumed(Math.Min(10, total)),
                2 => ReceiveResult.Consumed(0),
                _ => ReceiveResult.Consumed(total),
            };
        };
        int Calls()
        {
            lock (offsets) return offsets.Count;
        }
        s.WaitConnected();
        NativeBuffer data = s.Rent(100, seed: 8);
        NativeSegments segments = s.RentSegments(1);
        segments.Set(0, data.Segment(0, 100));
        s.Require(s.Client.OpenStream(StreamKind.Unidirectional, 1, 32767, out TransportStreamId id) == TransportStatus.Success, "OpenStream failed.");
        s.Require(s.Client.SendStream(id, segments.At(0), 1, 1, TransportSendFlags.Start | TransportSendFlags.Fin) == TransportStatus.Success, "SendStream failed.");
        s.Wait(() => Calls() >= 2, "the remainder of a partially consumed indication to be indicated again");
        s.Settle(TimeSpan.FromMilliseconds(100));
        s.Require(Calls() == 2, $"after Consumed(0) of a non-empty indication the stream must wait for ResumeStreamReceive, but {Calls()} indications arrived.");
        TransportStreamId peer = s.ServerSink.OfKind(RecordedEventKind.PeerStreamStarted)[0].StreamId;
        s.Server.ResumeStreamReceive(peer, 5);
        s.Wait(() => Calls() >= 3 && s.ServerSink.CountOf(RecordedEventKind.StreamPeerSendShutdown) == 1, "the indication after ResumeStreamReceive and the peer send shutdown");
        ulong[] seen;
        lock (offsets) seen = [.. offsets];
        s.Require(seen.Length == 3 && seen[0] == 0 && seen[1] == 10 && seen[2] == 15, $"indication offsets were [{string.Join(", ", seen)}], expected [0, 10, 15].");
        IReadOnlyList<RecordedEvent> receives = s.ServerSink.OfKind(RecordedEventKind.StreamReceived);
        s.Require(receives[1].Data.AsSpan().SequenceEqual(data.ToArray(10, 90)), "the second indication did not carry the unconsumed remainder.");
        s.Require(receives[^1].Fin && receives[^1].Data.AsSpan().SequenceEqual(data.ToArray(15, 85)), "the indication after the resume did not carry the rest with FIN.");
        s.CloseAndWait();
    }

    /// <summary>
    /// Starting a stream beyond the peer's limit fails with <see cref="TransportStatus.StreamLimitReached"/> (synchronously or
    /// through <see cref="ITransportSink.OnStreamStarted"/>); after the peer's <see cref="ITransport.UpdatePeerStreamLimits"/>
    /// the opener sees <see cref="ITransportSink.OnStreamsAvailable"/> and a new stream starts.
    /// </summary>
    public static void PeerStreamLimitThenUpdatePeerStreamLimits(ITransportTestHarness harness)
    {
        using var s = new Session(harness, new ConformancePairOptions { ServerPeerUnidiStreams = 0 });
        s.WaitConnected();
        s.Require(s.Client.OpenStream(StreamKind.Unidirectional, 7, 32767, out TransportStreamId blocked) == TransportStatus.Success, "OpenStream must succeed without peer credit.");
        TransportStatus start = s.Client.StartStream(blocked);
        if (start == TransportStatus.Success)
        {
            s.Wait(() => CountStream(s.ClientSink, RecordedEventKind.StreamStarted, blocked) > 0, "OnStreamStarted for a start beyond the peer's limit");
            RecordedEvent started = s.ClientSink.OfKind(RecordedEventKind.StreamStarted)[0];
            s.Require(started.Status == TransportStatus.StreamLimitReached, $"a start beyond the peer's limit reported {started.Status}, expected StreamLimitReached.");
        }
        else
        {
            s.Require(start == TransportStatus.StreamLimitReached, $"StartStream beyond the peer's limit returned {start}, expected StreamLimitReached.");
        }
        int before = s.ClientSink.Count;
        s.Server.UpdatePeerStreamLimits(16, 1);
        s.Wait(() => HasStreamsAvailableAfter(s.ClientSink, before), "OnStreamsAvailable with unidirectional credit after UpdatePeerStreamLimits");
        s.Client.CloseStream(blocked);
        s.Require(s.Client.OpenStream(StreamKind.Unidirectional, 8, 32767, out TransportStreamId fresh) == TransportStatus.Success, "OpenStream after the limit was raised failed.");
        start = s.Client.StartStream(fresh);
        s.Require(start == TransportStatus.Success, $"StartStream after the limit was raised returned {start}.");
        NativeBuffer data = s.Rent(64, seed: 2);
        NativeSegments segments = s.RentSegments(1);
        segments.Set(0, data.Segment(0, 64));
        s.Require(s.Client.SendStream(fresh, segments.At(0), 1, 9, TransportSendFlags.Fin) == TransportStatus.Success, "SendStream on the started stream failed.");
        s.Wait(() => s.ServerSink.CountOf(RecordedEventKind.StreamPeerSendShutdown) == 1 && CountStream(s.ClientSink, RecordedEventKind.StreamStarted, fresh) == 1,
            "the new stream's start and its data at the server");
        RecordedEvent freshStarted = Single(s, s.ClientSink, e => e.Kind == RecordedEventKind.StreamStarted && e.StreamId == fresh, "OnStreamStarted of the new stream");
        s.Require(freshStarted.Status == TransportStatus.Success && freshStarted.Context == 8, $"the new stream started with {freshStarted.Status}, context {freshStarted.Context}.");
        RecordedEvent peer = Single(s, s.ServerSink, static e => e.Kind == RecordedEventKind.PeerStreamStarted, "the server's only OnPeerStreamStarted");
        s.Require(peer.StreamKind == StreamKind.Unidirectional, $"the peer stream is {peer.StreamKind}.");
        s.Require(s.ServerSink.GetStreamData(peer.StreamId).AsSpan().SequenceEqual(data.ToArray(0, 64)), "the server received other bytes.");
        s.CloseAndWait();
    }

    /// <summary>
    /// RESET_STREAM and STOP_SENDING carry their error codes in both directions: an abort of the send direction reaches the
    /// peer as <see cref="StreamAbortDirection.Send"/>, an abort of the receive direction as <see cref="StreamAbortDirection.Receive"/>;
    /// both streams then shut down on both ends and every send completes once.
    /// </summary>
    public static void AbortStreamPropagatesCodesInBothDirections(ITransportTestHarness harness)
    {
        using var s = new Session(harness);
        s.WaitConnected();
        NativeBuffer data = s.Rent(64, seed: 4);
        NativeSegments segments = s.RentSegments(2);
        segments.Set(0, data.Segment(0, 32));
        segments.Set(1, data.Segment(32, 32));

        s.Require(s.Client.OpenStream(StreamKind.Bidirectional, 1, 32767, out TransportStreamId reset) == TransportStatus.Success, "OpenStream failed.");
        s.Require(s.Client.SendStream(reset, segments.At(0), 1, 11, TransportSendFlags.Start) == TransportStatus.Success, "SendStream failed.");
        s.Wait(() => s.ServerSink.CountOf(RecordedEventKind.StreamReceived) >= 1, "the first stream's data at the server");
        TransportStreamId resetPeer = s.ServerSink.OfKind(RecordedEventKind.PeerStreamStarted)[0].StreamId;
        s.Client.AbortStream(reset, 0x11, StreamAbortDirection.Send);
        s.Wait(() => HasAbort(s.ServerSink, resetPeer, 0x11, StreamAbortDirection.Send), "OnStreamAborted(Send, 0x11) at the server");
        s.Server.AbortStream(resetPeer, 0x22, StreamAbortDirection.Send);
        s.Wait(() => HasAbort(s.ClientSink, reset, 0x22, StreamAbortDirection.Send), "OnStreamAborted(Send, 0x22) at the client");
        s.Wait(() => CountStream(s.ClientSink, RecordedEventKind.StreamShutdownComplete, reset) == 1 && CountStream(s.ServerSink, RecordedEventKind.StreamShutdownComplete, resetPeer) == 1,
            "shutdown of the reset stream on both ends");

        s.Require(s.Client.OpenStream(StreamKind.Bidirectional, 2, 32767, out TransportStreamId stop) == TransportStatus.Success, "OpenStream failed.");
        s.Require(s.Client.SendStream(stop, segments.At(1), 1, 12, TransportSendFlags.Start) == TransportStatus.Success, "SendStream failed.");
        s.Wait(() => s.ServerSink.CountOf(RecordedEventKind.PeerStreamStarted) >= 2, "the second stream at the server");
        TransportStreamId stopPeer = s.ServerSink.OfKind(RecordedEventKind.PeerStreamStarted)[1].StreamId;
        s.Server.AbortStream(stopPeer, 0x33, StreamAbortDirection.Receive);
        s.Wait(() => HasAbort(s.ClientSink, stop, 0x33, StreamAbortDirection.Receive), "OnStreamAborted(Receive, 0x33) at the client");
        s.Client.AbortStream(stop, 0x44, StreamAbortDirection.Receive);
        s.Wait(() => HasAbort(s.ServerSink, stopPeer, 0x44, StreamAbortDirection.Receive), "OnStreamAborted(Receive, 0x44) at the server");
        s.Wait(() => CountStream(s.ClientSink, RecordedEventKind.StreamShutdownComplete, stop) == 1 && CountStream(s.ServerSink, RecordedEventKind.StreamShutdownComplete, stopPeer) == 1,
            "shutdown of the stopped stream on both ends");

        s.Require(CountStream(s.ClientSink, RecordedEventKind.StreamSendCompleted, reset) == 1, "the reset stream's send did not complete exactly once.");
        s.Require(CountStream(s.ClientSink, RecordedEventKind.StreamSendCompleted, stop) == 1, "the stopped stream's send did not complete exactly once.");
        RequireShutdownLast(s, s.ClientSink, reset, "client");
        RequireShutdownLast(s, s.ClientSink, stop, "client");
        RequireShutdownLast(s, s.ServerSink, resetPeer, "server");
        RequireShutdownLast(s, s.ServerSink, stopPeer, "server");
        s.CloseAndWait();
    }

    // ------------------------------------------------------------------ close

    /// <summary>
    /// <see cref="ITransport.Close"/> reports <see cref="TransportCloseReason.Local"/> with its code locally and
    /// <see cref="TransportCloseReason.Peer"/> with the same code at the peer, exactly once; later closes are ignored and a
    /// reason beyond 512 bytes is refused.
    /// </summary>
    public static void CloseReportsLocalAndPeerWithCode(ITransportTestHarness harness)
    {
        using var s = new Session(harness);
        s.WaitConnected();
        bool refused = false;
        try
        {
            s.Client.Close(9, new byte[513]);
        }
        catch (ArgumentOutOfRangeException)
        {
            refused = true;
        }
        s.Require(refused, "Close accepted a 513-byte reason.");
        s.Require(s.Client.State == TransportState.Connected, "a refused Close changed the state.");
        s.Client.Close(0x1234, "bye"u8);
        s.Wait(() => s.BothClosed, "OnClosed on both ends");
        RecordedEvent local = Single(s, s.ClientSink, static e => e.Kind == RecordedEventKind.Closed, "client OnClosed");
        RecordedEvent peer = Single(s, s.ServerSink, static e => e.Kind == RecordedEventKind.Closed, "server OnClosed");
        s.Require(local.CloseReason == TransportCloseReason.Local && local.ErrorCode == 0x1234, $"the closing end reported {local.CloseReason} code {local.ErrorCode:X}.");
        s.Require(peer.CloseReason == TransportCloseReason.Peer && peer.ErrorCode == 0x1234, $"the peer reported {peer.CloseReason} code {peer.ErrorCode:X}.");
        s.Require(s.Client.State == TransportState.Closed && s.Server.State == TransportState.Closed, $"states after OnClosed: client {s.Client.State}, server {s.Server.State}.");
        s.Client.Close(1, default);
        s.Server.Close(2, default);
        s.Settle(TimeSpan.FromMilliseconds(200));
        s.Require(s.ClientSink.CountOf(RecordedEventKind.Closed) == 1 && s.ServerSink.CountOf(RecordedEventKind.Closed) == 1, "a second OnClosed was raised.");
    }

    /// <summary>
    /// After <see cref="ITransportSink.OnClosed"/> nothing is raised; before it every stream reported
    /// <see cref="ITransportSink.OnStreamShutdownComplete"/> (also one that never started); calls afterwards are refused or ignored.
    /// </summary>
    public static void NoCallbacksAfterOnClosed(ITransportTestHarness harness)
    {
        using var s = new Session(harness);
        s.WaitConnected();
        s.WaitDatagrams();
        NativeBuffer data = s.Rent(2048, seed: 1);
        NativeSegments segments = s.RentSegments(4);
        segments.Set(0, data.Segment(0, 1024));
        segments.Set(1, data.Segment(1024, 100));
        s.Require(s.Client.OpenStream(StreamKind.Bidirectional, 1, 32767, out TransportStreamId active) == TransportStatus.Success, "OpenStream failed.");
        s.Require(s.Client.SendStream(active, segments.At(0), 1, 1, TransportSendFlags.Start) == TransportStatus.Success, "SendStream failed.");
        s.Require(s.Client.OpenStream(StreamKind.Bidirectional, 2, 32767, out TransportStreamId idle) == TransportStatus.Success, "OpenStream (never started) failed.");
        s.Require(s.Client.SendDatagram(segments.At(1), 1, 50, TransportSendFlags.None) == TransportStatus.Success, "SendDatagram failed.");
        s.Wait(() => s.ServerSink.CountOf(RecordedEventKind.StreamReceived) >= 1, "stream data at the server");
        s.Server.Close(5, default);
        s.Wait(() => s.BothClosed, "OnClosed on both ends");
        s.Settle(TimeSpan.FromMilliseconds(200));
        RequireClosedLast(s, s.ClientSink, "client");
        RequireClosedLast(s, s.ServerSink, "server");
        RequireShutdownBeforeClosed(s, s.ClientSink, active, "client (started stream)");
        RequireShutdownBeforeClosed(s, s.ClientSink, idle, "client (never started stream)");
        s.Require(Single(s, s.ServerSink, static e => e.Kind == RecordedEventKind.Closed, "server OnClosed").CloseReason == TransportCloseReason.Local, "the closing server did not report Local.");
        s.Require(Single(s, s.ClientSink, static e => e.Kind == RecordedEventKind.Closed, "client OnClosed").CloseReason == TransportCloseReason.Peer, "the client did not report Peer.");
        s.Require(HasFinalState(s.ClientSink, 50), "the datagram sent before the close reached no final state before OnClosed.");

        TransportStatus status = s.Client.SendDatagram(segments.At(1), 1, 51, TransportSendFlags.None);
        s.Require(status == TransportStatus.InvalidState, $"SendDatagram after OnClosed returned {status}.");
        status = s.Client.OpenStream(StreamKind.Bidirectional, 3, 32767, out _);
        s.Require(status == TransportStatus.InvalidState, $"OpenStream after OnClosed returned {status}.");
        status = s.Client.SendStream(active, segments.At(0), 1, 2, TransportSendFlags.None);
        s.Require(status == TransportStatus.InvalidState, $"SendStream after OnClosed returned {status}.");
        status = s.Client.StartStream(idle);
        s.Require(status == TransportStatus.InvalidState, $"StartStream after OnClosed returned {status}.");
        s.Client.AbortStream(active, 1, StreamAbortDirection.Both);
        s.Client.CloseStream(active);
        s.Client.UpdatePeerStreamLimits(4, 4);
        s.Client.GetStatistics(out _);
        s.Settle(TimeSpan.FromMilliseconds(100));
        RequireClosedLast(s, s.ClientSink, "client (after calls on the closed transport)");
    }

    /// <summary>
    /// Stream sends still in flight when the connection closes complete canceled, every accepted send completes exactly once
    /// before <see cref="ITransportSink.OnClosed"/>, and every accepted datagram reaches a final state before it.
    /// </summary>
    public static void InFlightSendsCompleteCanceledAtClose(ITransportTestHarness harness)
    {
        const int Sends = 16;
        const int Chunk = 64 * 1024;
        const int Datagrams = 8;
        using var s = new Session(harness);
        s.ServerSink.ReceiveHandler = static (_, _, _, _) => ReceiveResult.PendingAfter(0);
        s.WaitConnected();
        s.WaitDatagrams();
        NativeBuffer data = s.Rent(Chunk, seed: 12);
        NativeSegments segments = s.RentSegments(Sends + Datagrams);
        s.Require(s.Client.OpenStream(StreamKind.Bidirectional, 1, 32767, out TransportStreamId id) == TransportStatus.Success, "OpenStream failed.");
        var acceptedStream = new List<ulong>();
        for (int i = 0; i < Sends; i++)
        {
            segments.Set(i, data.Segment(0, Chunk));
            TransportStatus status = s.Client.SendStream(id, segments.At(i), 1, (ulong)(i + 1), i == 0 ? TransportSendFlags.Start : TransportSendFlags.None);
            s.Require(status == TransportStatus.Success, $"SendStream #{i + 1} returned {status}.");
            acceptedStream.Add((ulong)(i + 1));
        }
        var acceptedDatagrams = new List<ulong>();
        for (int i = 0; i < Datagrams; i++)
        {
            segments.Set(Sends + i, data.Segment(i * 100, 100));
            if (s.Client.SendDatagram(segments.At(Sends + i), 1, (ulong)(100 + i), TransportSendFlags.None) == TransportStatus.Success) acceptedDatagrams.Add((ulong)(100 + i));
        }
        s.Client.Close(7, default);
        s.Wait(() => s.BothClosed, "OnClosed on both ends");
        s.Settle(TimeSpan.FromMilliseconds(100));

        IReadOnlyList<RecordedEvent> events = s.ClientSink.Events;
        int closed = IndexOf(events, static e => e.Kind == RecordedEventKind.Closed);
        int canceled = 0;
        var completed = new HashSet<ulong>();
        for (int i = 0; i < events.Count; i++)
        {
            RecordedEvent e = events[i];
            if (e.Kind != RecordedEventKind.StreamSendCompleted) continue;
            s.Require(i < closed, $"send {e.Context} completed after OnClosed.");
            s.Require(completed.Add(e.Context), $"send {e.Context} completed twice.");
            canceled += e.Canceled ? 1 : 0;
        }
        foreach (ulong context in acceptedStream) s.Require(completed.Contains(context), $"accepted send {context} never completed.");
        s.Require(canceled > 0, "no send was still in flight at the close (none completed canceled).");
        foreach (ulong context in acceptedDatagrams)
        {
            int final = IndexOf(events, e => e.Kind == RecordedEventKind.DatagramSendStateChanged && e.Context == context && e.DatagramState.IsFinal());
            s.Require(final >= 0 && final < closed, $"datagram {context} reached no final state before OnClosed.");
        }
        RecordedEvent peer = Single(s, s.ServerSink, static e => e.Kind == RecordedEventKind.Closed, "server OnClosed");
        s.Require(peer.CloseReason == TransportCloseReason.Peer && peer.ErrorCode == 7, $"the peer reported {peer.CloseReason} code {peer.ErrorCode}.");
        RequireClosedLast(s, s.ClientSink, "client");
        RequireClosedLast(s, s.ServerSink, "server");
    }

    // ------------------------------------------------------------------ helpers

    private static int IndexOf(IReadOnlyList<RecordedEvent> events, Func<RecordedEvent, bool> predicate)
    {
        for (int i = 0; i < events.Count; i++)
        {
            if (predicate(events[i])) return i;
        }
        return -1;
    }

    private static RecordedEvent Single(Session s, RecordingSink sink, Func<RecordedEvent, bool> predicate, string what)
    {
        RecordedEvent? found = null;
        int count = 0;
        foreach (RecordedEvent e in sink.Events)
        {
            if (!predicate(e)) continue;
            found ??= e;
            count++;
        }
        s.Require(count == 1, $"expected exactly one {what}, saw {count}.");
        return found!;
    }

    private static bool IsStreamEvent(RecordedEventKind kind) => kind is RecordedEventKind.PeerStreamStarted or RecordedEventKind.StreamStarted
        or RecordedEventKind.StreamReceived or RecordedEventKind.StreamSendCompleted or RecordedEventKind.StreamAborted
        or RecordedEventKind.StreamPeerSendShutdown or RecordedEventKind.StreamShutdownComplete or RecordedEventKind.IdealSendBufferSize;

    /// <summary>Events of <paramref name="kind"/> for <paramref name="id"/> (any stream when <paramref name="id"/> is default).</summary>
    private static int CountStream(RecordingSink sink, RecordedEventKind kind, TransportStreamId id)
    {
        int count = 0;
        foreach (RecordedEvent e in sink.OfKind(kind))
        {
            if (id == default || e.StreamId == id) count++;
        }
        return count;
    }

    private static bool HasFinalState(RecordingSink sink, ulong context)
    {
        foreach (RecordedEvent e in sink.OfKind(RecordedEventKind.DatagramSendStateChanged))
        {
            if (e.Context == context && e.DatagramState.IsFinal()) return true;
        }
        return false;
    }

    private static int CountFinalStates(RecordingSink sink, int maxContext)
    {
        int count = 0;
        foreach (RecordedEvent e in sink.OfKind(RecordedEventKind.DatagramSendStateChanged))
        {
            if (e.Context >= 1 && e.Context <= (ulong)maxContext && e.DatagramState.IsFinal()) count++;
        }
        return count;
    }

    private static bool HasAbort(RecordingSink sink, TransportStreamId id, ulong code, StreamAbortDirection direction)
    {
        foreach (RecordedEvent e in sink.OfKind(RecordedEventKind.StreamAborted))
        {
            if (e.StreamId == id && e.ErrorCode == code && e.Direction == direction) return true;
        }
        return false;
    }

    private static bool HasStreamsAvailableAfter(RecordingSink sink, int index)
    {
        IReadOnlyList<RecordedEvent> events = sink.Events;
        for (int i = index; i < events.Count; i++)
        {
            if (events[i].Kind == RecordedEventKind.StreamsAvailable && events[i].Unidirectional > 0) return true;
        }
        return false;
    }

    private static void RequireOnlyCapabilityBeforeConnected(Session s, RecordingSink sink, string end)
    {
        IReadOnlyList<RecordedEvent> events = sink.Events;
        int connected = IndexOf(events, static e => e.Kind == RecordedEventKind.Connected);
        for (int i = 0; i < connected; i++)
        {
            s.Require(events[i].Kind == RecordedEventKind.DatagramCapabilityChanged, $"{end}: {events[i].Kind} was raised before OnConnected.");
        }
    }

    private static void CheckCapabilities(Session s, RecordingSink sink, ITransport transport, string end)
    {
        for (int attempt = 0; ; attempt++)
        {
            IReadOnlyList<RecordedEvent> events = sink.Events;
            TransportCapabilities now = transport.Capabilities;
            if (sink.Count != events.Count && attempt < 10) continue; // a report raced the snapshot
            int connected = IndexOf(events, static e => e.Kind == RecordedEventKind.Connected);
            RecordedEvent? lastBefore = null;
            RecordedEvent? last = null;
            for (int i = 0; i < events.Count; i++)
            {
                RecordedEvent e = events[i];
                if (e.Kind != RecordedEventKind.DatagramCapabilityChanged) continue;
                s.Require(!e.Enabled || e.MaxPayload > 0, $"{end}: datagrams reported enabled with a maximum payload of {e.MaxPayload}.");
                if (i < connected) lastBefore = e;
                last = e;
            }
            TransportCapabilities atConnect = events[connected].Capabilities;
            if (lastBefore is not null)
            {
                s.Require(atConnect.Datagrams == lastBefore.Enabled, $"{end}: OnConnected reports Datagrams={atConnect.Datagrams} after a report of {lastBefore.Enabled}.");
                if (lastBefore.Enabled)
                {
                    s.Require(atConnect.MaxDatagramPayload == lastBefore.MaxPayload, $"{end}: OnConnected reports MaxDatagramPayload {atConnect.MaxDatagramPayload} after a report of {lastBefore.MaxPayload}.");
                }
            }
            s.Require(now.Datagrams && now.DatagramSendState, $"{end}: Capabilities report Datagrams={now.Datagrams}, DatagramSendState={now.DatagramSendState} with datagrams negotiated.");
            int expected = last is not null ? last.MaxPayload : atConnect.MaxDatagramPayload;
            s.Require(now.MaxDatagramPayload == expected, $"{end}: Capabilities.MaxDatagramPayload is {now.MaxDatagramPayload}, the last report said {expected}.");
            return;
        }
    }

    private static void RequireCompletions(Session s, RecordingSink sink, TransportStreamId id, ulong[] contexts, bool expectCanceled)
    {
        var seen = new List<ulong>();
        foreach (RecordedEvent e in sink.OfKind(RecordedEventKind.StreamSendCompleted))
        {
            if (e.StreamId != id) continue;
            s.Require(e.Canceled == expectCanceled, $"send {e.Context} on {id} completed with canceled={e.Canceled}.");
            seen.Add(e.Context);
        }
        s.Require(seen.Count == contexts.Length && seen.SequenceEqual(contexts), $"completions on {id} were [{string.Join(", ", seen)}], expected [{string.Join(", ", contexts)}].");
    }

    private static void RequireStartBeforeCompletions(Session s, RecordingSink sink, TransportStreamId id)
    {
        IReadOnlyList<RecordedEvent> events = sink.Events;
        int started = IndexOf(events, e => e.Kind == RecordedEventKind.StreamStarted && e.StreamId == id);
        int firstCompletion = IndexOf(events, e => e.Kind == RecordedEventKind.StreamSendCompleted && e.StreamId == id);
        s.Require(started >= 0 && (firstCompletion < 0 || started < firstCompletion), $"OnStreamStarted for {id} must precede its send completions.");
    }

    private static void RequireReceiveOrdering(Session s, RecordingSink sink, TransportStreamId id, string end)
    {
        IReadOnlyList<RecordedEvent> events = sink.Events;
        int opened = IndexOf(events, e => e.Kind is RecordedEventKind.PeerStreamStarted or RecordedEventKind.StreamStarted && e.StreamId == id);
        int lastReceive = -1;
        int shutdown = -1;
        int shutdownCount = 0;
        for (int i = 0; i < events.Count; i++)
        {
            RecordedEvent e = events[i];
            if (e.StreamId != id) continue;
            if (e.Kind == RecordedEventKind.StreamReceived)
            {
                s.Require(i > opened, $"{end}: data on {id} arrived before the stream was announced.");
                lastReceive = i;
            }
            if (e.Kind == RecordedEventKind.StreamPeerSendShutdown)
            {
                shutdown = i;
                shutdownCount++;
            }
        }
        s.Require(shutdownCount == 1, $"{end}: OnStreamPeerSendShutdown for {id} was raised {shutdownCount} times.");
        s.Require(lastReceive >= 0 && events[lastReceive].Fin, $"{end}: the last receive on {id} did not carry FIN.");
        s.Require(shutdown > lastReceive, $"{end}: OnStreamPeerSendShutdown for {id} came before its last receive.");
    }

    private static void RequireShutdownLast(Session s, RecordingSink sink, TransportStreamId id, string end)
    {
        IReadOnlyList<RecordedEvent> events = sink.Events;
        int shutdown = -1;
        int count = 0;
        for (int i = 0; i < events.Count; i++)
        {
            RecordedEvent e = events[i];
            if (e.StreamId != id || !IsStreamEvent(e.Kind)) continue;
            s.Require(shutdown < 0, $"{end}: {e.Kind} for {id} came after its OnStreamShutdownComplete.");
            if (e.Kind == RecordedEventKind.StreamShutdownComplete)
            {
                shutdown = i;
                count++;
            }
        }
        s.Require(count == 1, $"{end}: OnStreamShutdownComplete for {id} was raised {count} times.");
    }

    private static void RequireShutdownBeforeClosed(Session s, RecordingSink sink, TransportStreamId id, string end)
    {
        IReadOnlyList<RecordedEvent> events = sink.Events;
        int shutdown = IndexOf(events, e => e.Kind == RecordedEventKind.StreamShutdownComplete && e.StreamId == id);
        int closed = IndexOf(events, static e => e.Kind == RecordedEventKind.Closed);
        s.Require(shutdown >= 0 && shutdown < closed, $"{end}: {id} did not report OnStreamShutdownComplete before OnClosed.");
    }

    private static void RequireClosedLast(Session s, RecordingSink sink, string end)
    {
        IReadOnlyList<RecordedEvent> events = sink.Events;
        int closed = IndexOf(events, static e => e.Kind == RecordedEventKind.Closed);
        s.Require(closed >= 0 && closed == events.Count - 1, $"{end}: {(closed < 0 ? "no OnClosed" : events[^1].Kind + " was raised after OnClosed")}.");
        s.Require(sink.CountOf(RecordedEventKind.Closed) == 1, $"{end}: OnClosed was raised {sink.CountOf(RecordedEventKind.Closed)} times.");
    }

    private static void WriteDatagramPayload(NativeBuffer buffer, int index, int length)
    {
        byte[] expected = ExpectedDatagramPayload(index, length);
        expected.CopyTo(new Span<byte>(buffer.Pointer + (index * length), length));
    }

    private static byte[] ExpectedDatagramPayload(int index, int length)
    {
        var payload = new byte[length];
        for (int i = 0; i < length; i++) payload[i] = NativeBuffer.PatternAt(i, 100 + index);
        BinaryPrimitives.WriteInt32LittleEndian(payload, index);
        return payload;
    }

    /// <summary>Overwrites a datagram's payload as soon as the transport releases it (Sent or Canceled).</summary>
    private sealed class ReleaseScribbler(NativeBuffer payloads, int payloadLength, int count) : SinkBase
    {
        public override void OnDatagramSendStateChanged(ulong context, DatagramSendState state)
        {
            if (state is not (DatagramSendState.Sent or DatagramSendState.Canceled) || context < 1 || context > (ulong)count) return;
            new Span<byte>(payloads.Pointer + ((int)(context - 1) * payloadLength), payloadLength).Fill(0xEE);
        }
    }

    /// <summary>Consumes stream data in random amounts and patterns, keeping its own log of consumed bytes and offsets.</summary>
    private sealed class IntegrityReceiver(int seed) : SinkBase
    {
        private readonly Lock _gate = new();
        private readonly Random _random = new(seed);
        private readonly MemoryStream _consumed = new();
        private byte[] _held = [];
        private TransportStreamId _pendingId;
        private bool _needsResume;
        private bool _peerShutdown;

        public ITransport? Transport { get; set; }

        public int OffsetErrors { get; private set; }

        public bool NeedsResume
        {
            get
            {
                lock (_gate) return _needsResume;
            }
        }

        public bool PeerShutdown
        {
            get
            {
                lock (_gate) return _peerShutdown;
            }
        }

        public long ConsumedLength
        {
            get
            {
                lock (_gate) return _consumed.Length;
            }
        }

        public byte[] ConsumedBytes
        {
            get
            {
                lock (_gate) return _consumed.ToArray();
            }
        }

        public override ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin)
        {
            lock (_gate)
            {
                if ((long)absoluteOffset != _consumed.Length) OffsetErrors++;
                int total = 0;
                foreach (TransportSegment segment in segments) total += (int)segment.Length;
                var all = new byte[total];
                int at = 0;
                foreach (TransportSegment segment in segments)
                {
                    segment.AsSpan().CopyTo(all.AsSpan(at));
                    at += (int)segment.Length;
                }
                if (total == 0) return ReceiveResult.Consumed(0);
                int roll = _random.Next(100);
                if (roll < 40)
                {
                    _consumed.Write(all);
                    return ReceiveResult.Consumed(total);
                }
                if (roll < 70 && total > 1)
                {
                    int part = _random.Next(1, total);
                    _consumed.Write(all, 0, part);
                    return ReceiveResult.Consumed(part);
                }
                bool explicitPending = roll < 90;
                int keep = explicitPending ? _random.Next(0, total + 1) : 0;
                _consumed.Write(all, 0, keep);
                _held = all[keep..];
                _pendingId = id;
                _needsResume = true;
                return explicitPending ? ReceiveResult.PendingAfter(keep) : ReceiveResult.Consumed(0);
            }
        }

        /// <summary>On the scenario thread: consumes a random part of the held bytes out of band and resumes the stream.</summary>
        public void Resume()
        {
            TransportStreamId id;
            int credit;
            lock (_gate)
            {
                if (!_needsResume) return;
                credit = _random.Next(0, _held.Length + 1);
                _consumed.Write(_held, 0, credit);
                _needsResume = false;
                id = _pendingId;
            }
            Transport!.ResumeStreamReceive(id, credit);
        }

        public override void OnStreamPeerSendShutdown(TransportStreamId id)
        {
            lock (_gate) _peerShutdown = true;
        }

        public override void OnStreamShutdownComplete(TransportStreamId id) => Transport?.CloseStream(id);
    }
}
