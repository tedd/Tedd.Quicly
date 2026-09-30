using System.Text;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Testing.Conformance;

/// <summary>Contract scenarios for refused and released streams, refused connections and the ways a connection ends.</summary>
public static unsafe partial class TransportConformance
{
    // ------------------------------------------------------------------ refused and released streams

    /// <summary>
    /// A start the peer's stream limit refuses, made with <see cref="ITransport.StartStream"/> and with a send carrying
    /// <see cref="TransportSendFlags.Start"/>: refused synchronously (StreamLimitReached, nothing follows) or asynchronously
    /// (Success, then OnStreamStarted(StreamLimitReached), the send's canceled completion and OnStreamShutdownComplete, in that
    /// order, and nothing else for the stream). Either way the stream never starts: StartStream and SendStream(Start) on it
    /// return InvalidState, also after OnStreamsAvailable, and the peer never hears of it. CloseStream releases it, and a new
    /// stream started after OnStreamsAvailable delivers its data.
    /// </summary>
    public static void RefusedStreamNeverStartsAndIsRetriedOnANewStream(ITransportTestHarness harness)
    {
        using var s = new Session(harness, new ConformancePairOptions { ServerPeerUnidiStreams = 0 });
        s.ClientSink.AutoCloseStreams = false; // keep the refused streams around to see that they never start
        s.WaitConnected();
        NativeBuffer data = s.Rent(64, seed: 13);
        NativeSegments segments = s.RentSegments(2);
        segments.Set(0, data.Segment(0, 32));
        segments.Set(1, data.Segment(32, 32));
        s.Require(s.Client.OpenStream(StreamKind.Unidirectional, 0x21, 32767, out TransportStreamId viaStart) == TransportStatus.Success, "OpenStream failed.");
        s.Require(s.Client.OpenStream(StreamKind.Unidirectional, 0x22, 32767, out TransportStreamId viaSend) == TransportStatus.Success, "OpenStream failed.");
        TransportStatus started = s.Client.StartStream(viaStart);
        TransportStatus sent = s.Client.SendStream(viaSend, segments.At(0), 1, 0x31, TransportSendFlags.Start | TransportSendFlags.Fin);
        s.Require(started is TransportStatus.Success or TransportStatus.StreamLimitReached, $"StartStream beyond the peer's stream limit returned {started}.");
        s.Require(sent is TransportStatus.Success or TransportStatus.StreamLimitReached, $"SendStream(Start) beyond the peer's stream limit returned {sent}.");
        if (started == TransportStatus.Success)
        {
            s.Wait(() => CountStream(s.ClientSink, RecordedEventKind.StreamShutdownComplete, viaStart) == 1, "OnStreamStarted and OnStreamShutdownComplete of the refused start");
        }
        if (sent == TransportStatus.Success)
        {
            s.Wait(() => CountStream(s.ClientSink, RecordedEventKind.StreamShutdownComplete, viaSend) == 1, "the refused send's completion and OnStreamShutdownComplete");
        }
        s.Settle(TimeSpan.FromMilliseconds(100));
        RequireRefusedSequence(s, viaStart, started, sendContext: null);
        RequireRefusedSequence(s, viaSend, sent, sendContext: 0x31);

        int before = s.ClientSink.Count;
        s.Server.UpdatePeerStreamLimits(16, 4);
        s.Wait(() => HasStreamsAvailableAfter(s.ClientSink, before), "OnStreamsAvailable with unidirectional credit");
        foreach (TransportStreamId refused in new[] { viaStart, viaSend })
        {
            TransportStatus again = s.Client.StartStream(refused);
            s.Require(again == TransportStatus.InvalidState, $"StartStream on refused stream {refused} returned {again} after OnStreamsAvailable; a refused stream never starts.");
            again = s.Client.SendStream(refused, segments.At(1), 1, 0x32, TransportSendFlags.Start);
            s.Require(again == TransportStatus.InvalidState, $"SendStream(Start) on refused stream {refused} returned {again}; a refused stream never starts.");
            s.Require(s.Client.GetQuicStreamId(refused) == -1, $"refused stream {refused} reports a QUIC stream id.");
            s.Client.CloseStream(refused);
        }
        s.Require(s.Client.OpenStream(StreamKind.Unidirectional, 0x23, 32767, out TransportStreamId fresh) == TransportStatus.Success, "OpenStream after the limit was raised failed.");
        TransportStatus retried = s.Client.SendStream(fresh, segments.At(1), 1, 0x33, TransportSendFlags.Start | TransportSendFlags.Fin);
        s.Require(retried == TransportStatus.Success, $"SendStream(Start) on a new stream after OnStreamsAvailable returned {retried}.");
        s.Wait(() => s.ServerSink.CountOf(RecordedEventKind.StreamPeerSendShutdown) == 1, "the new stream's data at the server");
        RecordedEvent peer = Single(s, s.ServerSink, static e => e.Kind == RecordedEventKind.PeerStreamStarted, "OnPeerStreamStarted at the server (a refused stream never reaches the peer)");
        s.Require(s.ServerSink.GetStreamData(peer.StreamId).AsSpan().SequenceEqual(data.ToArray(32, 32)), "the server received other bytes than the new stream sent.");
        foreach (RecordedEvent e in s.ClientSink.OfKind(RecordedEventKind.StreamSendCompleted))
        {
            s.Require(e.Context != 0x32, "a send refused with InvalidState completed.");
        }
        s.CloseAndWait();
    }

    /// <summary>The exact callbacks a refused start leaves behind (see <see cref="RefusedStreamNeverStartsAndIsRetriedOnANewStream"/>).</summary>
    private static void RequireRefusedSequence(Session s, TransportStreamId id, TransportStatus returned, ulong? sendContext)
    {
        var actual = new List<string>();
        foreach (RecordedEvent e in s.ClientSink.Events)
        {
            if (!IsStreamEvent(e.Kind) || e.StreamId != id) continue;
            actual.Add(e.Kind switch
            {
                RecordedEventKind.StreamStarted => "StreamStarted " + e.Status,
                RecordedEventKind.StreamSendCompleted => $"StreamSendCompleted {e.Context}{(e.Canceled ? " canceled" : "")}",
                _ => e.Kind.ToString(),
            });
        }
        if (returned != TransportStatus.Success)
        {
            s.Require(actual.Count == 0, $"a start refused synchronously ({returned}) was reported again for {id}: [{string.Join(", ", actual)}].");
            return;
        }
        var expected = new List<string> { "StreamStarted " + TransportStatus.StreamLimitReached };
        if (sendContext is not null) expected.Add($"StreamSendCompleted {sendContext.Value} canceled");
        expected.Add(nameof(RecordedEventKind.StreamShutdownComplete));
        s.Require(actual.SequenceEqual(expected), $"refused stream {id}: expected [{string.Join(", ", expected)}], saw [{string.Join(", ", actual)}].");
    }

    /// <summary>
    /// A start refused for the peer's stream limit is reported exactly once, also when the call is made from inside a
    /// callback (where MsQuic runs it inline): either synchronously (the call returns StreamLimitReached and nothing follows
    /// for the stream) or asynchronously (the call returns Success, exactly one OnStreamStarted carries StreamLimitReached and
    /// a send accepted with the start completes canceled, exactly once). ITransport: "If a send call returns anything but
    /// Success no completion follows".
    /// </summary>
    public static void StartRefusedInsideACallbackIsReportedOnce(ITransportTestHarness harness)
    {
        var starter = new InlineStarter();
        using var s = new Session(harness, new ConformancePairOptions { ServerPeerUnidiStreams = 0 }, clientInner: starter);
        starter.Transport = s.Client;
        s.WaitConnected();
        s.WaitDatagrams();
        NativeBuffer data = s.Rent(64, seed: 15);
        NativeSegments segments = s.RentSegments(2);
        segments.Set(0, data.Segment(0, 64));
        segments.Set(1, data.Segment(0, 8));
        s.Require(s.Client.OpenStream(StreamKind.Unidirectional, 0xA, 32767, out TransportStreamId a) == TransportStatus.Success, "OpenStream failed.");
        s.Require(s.Client.OpenStream(StreamKind.Unidirectional, 0xB, 32767, out TransportStreamId b) == TransportStatus.Success, "OpenStream failed.");
        starter.StartId = a;
        starter.SendId = b;
        starter.Segment = segments.At(0);
        s.Require(s.Server.SendDatagram(segments.At(1), 1, 1, TransportSendFlags.None) == TransportStatus.Success, "the server's datagram was refused.");
        s.Wait(() => starter.Done, "the client callback that starts both streams");
        s.Settle(TimeSpan.FromMilliseconds(300));
        RequireReportedOnce(s, a, starter.StartResult, null, "StartStream inside a callback");
        RequireReportedOnce(s, b, starter.SendResult, 77, "SendStream(Start | Fin) inside a callback");
        s.CloseAndWait();
    }

    private static void RequireReportedOnce(Session s, TransportStreamId id, TransportStatus returned, ulong? sendContext, string what)
    {
        int completions = 0;
        bool allCanceled = true;
        foreach (RecordedEvent e in s.ClientSink.OfKind(RecordedEventKind.StreamSendCompleted))
        {
            if (sendContext is null || e.Context != sendContext.Value) continue;
            completions++;
            allCanceled &= e.Canceled;
        }
        int started = 0;
        TransportStatus status = TransportStatus.Success;
        foreach (RecordedEvent e in s.ClientSink.OfKind(RecordedEventKind.StreamStarted))
        {
            if (e.StreamId != id) continue;
            started++;
            status = e.Status;
        }
        if (returned == TransportStatus.Success)
        {
            s.Require(started == 1 && status == TransportStatus.StreamLimitReached, $"{what} returned Success, so exactly one OnStreamStarted(StreamLimitReached) must follow for {id}.");
            if (sendContext is not null) s.Require(completions == 1 && allCanceled, $"{what} returned Success, so its send must complete exactly once, canceled.");
            return;
        }
        s.Require(returned == TransportStatus.StreamLimitReached, $"{what} returned {returned}, expected StreamLimitReached (or Success with an asynchronous report).");
        int streamEvents = 0;
        foreach (RecordedEvent e in s.ClientSink.Events) streamEvents += IsStreamEvent(e.Kind) && e.StreamId == id ? 1 : 0;
        s.Require(streamEvents == 0 && completions == 0, $"{what} returned {returned} synchronously, yet {streamEvents} stream callback(s) and {completions} send completion(s) followed for {id}.");
    }

    /// <summary>Starts one stream and sends on another from inside the first datagram callback (MsQuic runs such calls inline).</summary>
    private sealed class InlineStarter : SinkBase
    {
        public volatile ITransport? Transport;
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

        public override void OnStreamShutdownComplete(TransportStreamId id) => Transport?.CloseStream(id);
    }

    /// <summary>
    /// ITransport.AbortStream: "Aborting a local stream that was never started releases it like CloseStream: no callback
    /// follows for it and its id is stale afterwards" (every direction, both kinds). Stale ids are ignored even when their
    /// slot has been reused by a newer stream (generation check), which keeps working.
    /// </summary>
    public static void AbortingANeverStartedStreamReleasesItWithoutCallbacks(ITransportTestHarness harness)
    {
        using var s = new Session(harness);
        s.WaitConnected();
        NativeBuffer data = s.Rent(32, seed: 17);
        NativeSegments segments = s.RentSegments(1);
        segments.Set(0, data.Segment(0, 32));
        (StreamKind Kind, StreamAbortDirection Direction)[] cases =
        [
            (StreamKind.Bidirectional, StreamAbortDirection.Send), (StreamKind.Bidirectional, StreamAbortDirection.Receive), (StreamKind.Bidirectional, StreamAbortDirection.Both),
            (StreamKind.Unidirectional, StreamAbortDirection.Send), (StreamKind.Unidirectional, StreamAbortDirection.Receive), (StreamKind.Unidirectional, StreamAbortDirection.Both),
        ];
        var aborted = new List<TransportStreamId>();
        for (int i = 0; i < cases.Length; i++)
        {
            s.Require(s.Client.OpenStream(cases[i].Kind, (ulong)(0x100 + i), 32767, out TransportStreamId id) == TransportStatus.Success, "OpenStream failed.");
            s.Client.AbortStream(id, 0x55, cases[i].Direction);
            s.Require(s.Client.StartStream(id) == TransportStatus.InvalidState, $"StartStream after aborting never-started {id} ({cases[i]}) did not return InvalidState.");
            s.Require(s.Client.SendStream(id, segments.At(0), 1, (ulong)(900 + i), TransportSendFlags.Start) == TransportStatus.InvalidState, $"SendStream(Start) after aborting never-started {id} ({cases[i]}) did not return InvalidState.");
            s.Require(s.Client.GetQuicStreamId(id) == -1, $"GetQuicStreamId of the aborted never-started {id} is not -1.");
            aborted.Add(id);
        }
        s.Settle(TimeSpan.FromMilliseconds(50));
        s.Require(s.Client.OpenStream(StreamKind.Bidirectional, 0x200, 32767, out TransportStreamId live) == TransportStatus.Success, "OpenStream failed.");
        foreach (TransportStreamId stale in aborted)
        {
            s.Client.AbortStream(stale, 0x66, StreamAbortDirection.Both);
            s.Client.SetStreamPriority(stale, 1);
            s.Client.ResumeStreamReceive(stale, 0);
            s.Client.CloseStream(stale);
            s.Require(s.Client.StartStream(stale) == TransportStatus.InvalidState, $"StartStream on the stale {stale} did not return InvalidState.");
        }
        s.Require(s.Client.SendStream(live, segments.At(0), 1, 1000, TransportSendFlags.Start | TransportSendFlags.Fin) == TransportStatus.Success, "SendStream on the live stream failed.");
        s.Wait(() => s.ServerSink.CountOf(RecordedEventKind.StreamPeerSendShutdown) == 1, "the live stream's data and FIN at the server");
        TransportStreamId peer = s.ServerSink.OfKind(RecordedEventKind.PeerStreamStarted)[0].StreamId;
        s.Require(s.ServerSink.GetStreamData(peer).AsSpan().SequenceEqual(data.ToArray(0, 32)), "the live stream's data differs.");
        s.Require(s.ServerSink.CountOf(RecordedEventKind.PeerStreamStarted) == 1, "an aborted never-started stream reached the peer.");
        s.Require(s.ServerSink.CountOf(RecordedEventKind.StreamAborted) == 0, "the abort of a never-started stream reached the peer.");
        s.CloseAndWait();
        s.Settle(TimeSpan.FromMilliseconds(100));
        foreach (RecordedEvent e in s.ClientSink.Events)
        {
            s.Require(!(IsStreamEvent(e.Kind) && aborted.Contains(e.StreamId)), $"{e} was raised for a stream AbortStream released before it started.");
            s.Require(!(e.Kind == RecordedEventKind.StreamSendCompleted && e.Context >= 900 && e.Context < 900 + (ulong)cases.Length), $"a refused send completed: {e}");
        }
    }

    /// <summary>
    /// ITransport.CloseStream before OnStreamShutdownComplete: "aborts both directions with error code 0: completions of
    /// pending sends are still reported (canceled), OnStreamShutdownComplete is not. The id is stale afterwards".
    /// </summary>
    public static void CloseStreamBeforeShutdownAbortsWithCodeZero(ITransportTestHarness harness)
    {
        const int Sends = 8;
        const int Chunk = 64 * 1024;
        using var s = new Session(harness);
        s.ServerSink.ReceiveHandler = static (_, _, _, _) => ReceiveResult.PendingAfter(0);
        s.WaitConnected();
        NativeBuffer data = s.Rent(Chunk, seed: 18);
        NativeSegments segments = s.RentSegments(1);
        segments.Set(0, data.Segment(0, Chunk));
        s.Require(s.Client.OpenStream(StreamKind.Bidirectional, 1, 32767, out TransportStreamId id) == TransportStatus.Success, "OpenStream failed.");
        for (int i = 0; i < Sends; i++)
        {
            TransportStatus status = s.Client.SendStream(id, segments.At(0), 1, (ulong)(i + 1), i == 0 ? TransportSendFlags.Start : TransportSendFlags.None);
            s.Require(status == TransportStatus.Success, $"SendStream #{i + 1} returned {status}.");
        }
        s.Wait(() => s.ServerSink.CountOf(RecordedEventKind.StreamReceived) > 0, "the stream's first data at the server");
        TransportStreamId peer = s.ServerSink.OfKind(RecordedEventKind.PeerStreamStarted)[0].StreamId;
        s.Client.CloseStream(id);
        TransportStatus after = s.Client.SendStream(id, segments.At(0), 1, 99, TransportSendFlags.None);
        s.Require(after == TransportStatus.InvalidState, $"SendStream on a closed stream returned {after}.");
        s.Wait(() => CountStream(s.ClientSink, RecordedEventKind.StreamSendCompleted, id) >= Sends, "a completion for every accepted send");
        s.Wait(() => HasAbort(s.ServerSink, peer, 0, StreamAbortDirection.Send) && CountStream(s.ServerSink, RecordedEventKind.StreamShutdownComplete, peer) == 1, "the reset (code 0) and the shutdown at the server");
        s.Settle(TimeSpan.FromMilliseconds(200));
        var contexts = new HashSet<ulong>();
        foreach (RecordedEvent e in s.ClientSink.OfKind(RecordedEventKind.StreamSendCompleted))
        {
            if (e.StreamId != id) continue;
            s.Require(contexts.Add(e.Context), $"send {e.Context} completed twice.");
        }
        s.Require(contexts.Count == Sends, $"{contexts.Count} of {Sends} sends completed.");
        s.CloseAndWait();
        s.Require(CountStream(s.ClientSink, RecordedEventKind.StreamShutdownComplete, id) == 0, "OnStreamShutdownComplete was raised for a stream closed before its shutdown.");
    }

    // ------------------------------------------------------------------ datagrams and receive at the edges

    /// <summary>
    /// ITransportSink.OnClosed: "Before it, every accepted send has completed (... datagrams in a final state)". The client
    /// closes from another thread the moment its first datagram is reported Sent, so that others are in flight or still
    /// queued when the connection goes down; every accepted datagram still reports exactly one final state before OnClosed,
    /// and nothing follows OnClosed.
    /// </summary>
    public static void DatagramsInFlightAtCloseReachAFinalStateBeforeOnClosed(ITransportTestHarness harness)
    {
        const int Count = 48;
        const int Length = 1000;
        var watcher = new SentWatcher();
        using var s = new Session(harness, clientInner: watcher);
        s.WaitConnected();
        s.WaitDatagrams();
        s.Require(s.Client.Capabilities.MaxDatagramPayload >= Length, $"MaxDatagramPayload is {s.Client.Capabilities.MaxDatagramPayload}.");
        NativeBuffer data = s.Rent(Count * Length, seed: 16);
        NativeSegments segments = s.RentSegments(Count);
        for (int i = 0; i < Count; i++) segments.Set(i, data.Segment(i * Length, Length));
        ITransport client = s.Client;
        var closer = new Thread(() =>
        {
            long deadline = Environment.TickCount64 + 30_000;
            while (!watcher.SentSeen && Environment.TickCount64 < deadline) Thread.SpinWait(20);
            client.Close(3, default);
        })
        {
            IsBackground = true,
            Name = "conformance-closer",
        };
        closer.Start();
        var accepted = new List<ulong>();
        for (int i = 0; i < Count; i++)
        {
            if (s.Client.SendDatagram(segments.At(i), 1, (ulong)(i + 1), TransportSendFlags.None) == TransportStatus.Success) accepted.Add((ulong)(i + 1));
        }
        s.Wait(() => s.BothClosed, "OnClosed on both ends");
        s.Require(closer.Join(TimeSpan.FromSeconds(30)), "the closing thread did not finish.");
        s.Settle(TimeSpan.FromMilliseconds(200));
        IReadOnlyList<RecordedEvent> events = s.ClientSink.Events;
        int closed = IndexOf(events, static e => e.Kind == RecordedEventKind.Closed);
        s.Require(closed >= 0 && closed == events.Count - 1, "callbacks followed OnClosed.");
        s.Require(accepted.Count > 0, "no datagram was accepted.");
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
        s.Require(missing.Count == 0, $"{missing.Count} of {accepted.Count} accepted datagrams did not report exactly one final state before OnClosed (contexts {string.Join(", ", missing)}).");
    }

    /// <summary>Records that a datagram was handed to the network.</summary>
    private sealed class SentWatcher : SinkBase
    {
        public volatile bool SentSeen;

        public override void OnDatagramSendStateChanged(ulong context, DatagramSendState state)
        {
            if (state == DatagramSendState.Sent) SentSeen = true;
        }
    }

    /// <summary>
    /// ReceiveResult: "A resume issued on another thread while the receive callback is still returning takes effect once it
    /// has returned." The server's receive callback starts a thread that resumes the stream (crediting 5 bytes) and gives it
    /// a moment to make the call before returning PendingAfter(10): the rest of the data then arrives from offset 15, with FIN.
    /// </summary>
    public static void ResumeRacingTheReceiveCallbackIsApplied(ITransportTestHarness harness)
    {
        using var s = new Session(harness);
        var offsets = new List<ulong>();
        Thread? resumer = null;
        ITransport server = s.Server;
        s.ServerSink.ReceiveHandler = (id, segments, offset, _) =>
        {
            int call;
            lock (offsets)
            {
                offsets.Add(offset);
                call = offsets.Count;
            }
            int total = 0;
            foreach (TransportSegment segment in segments) total += (int)segment.Length;
            if (call != 1) return ReceiveResult.Consumed(total);
            var issued = new ManualResetEventSlim();
            var returned = new ManualResetEventSlim();
            resumer = new Thread(() =>
            {
                issued.Set();
                server.ResumeStreamReceive(id, 5);
                returned.Set();
            })
            {
                IsBackground = true,
                Name = "conformance-resumer",
            };
            resumer.Start();
            issued.Wait(TimeSpan.FromSeconds(5));
            // A real transport usually takes the resume while this callback still runs; a simulator (one lock) takes it once the
            // callback has returned. The contract allows both.
            returned.Wait(TimeSpan.FromMilliseconds(50));
            return ReceiveResult.PendingAfter(10);
        };
        s.WaitConnected();
        NativeBuffer data = s.Rent(100, seed: 20);
        NativeSegments segments = s.RentSegments(1);
        segments.Set(0, data.Segment(0, 100));
        s.Require(s.Client.OpenStream(StreamKind.Unidirectional, 1, 32767, out TransportStreamId id) == TransportStatus.Success, "OpenStream failed.");
        s.Require(s.Client.SendStream(id, segments.At(0), 1, 1, TransportSendFlags.Start | TransportSendFlags.Fin) == TransportStatus.Success, "SendStream failed.");
        s.Wait(() => s.ServerSink.CountOf(RecordedEventKind.StreamPeerSendShutdown) == 1, "the rest of the stream after the racing resume");
        s.Require(resumer is null || resumer.Join(TimeSpan.FromSeconds(10)), "the resuming thread did not finish.");
        ulong[] seen;
        lock (offsets) seen = [.. offsets];
        s.Require(seen.Length == 2 && seen[0] == 0 && seen[1] == 15, $"indication offsets were [{string.Join(", ", seen)}], expected [0, 15].");
        RecordedEvent last = s.ServerSink.OfKind(RecordedEventKind.StreamReceived)[^1];
        s.Require(last.Fin && last.Data.AsSpan().SequenceEqual(data.ToArray(15, 85)), "the indication after the resume did not carry the rest of the data with FIN.");
        s.CloseAndWait();
    }

    /// <summary>
    /// ReceiveResult: a stream held with <see cref="ReceiveResult.PendingAfter"/> is indicated again after every
    /// <see cref="ITransport.ResumeStreamReceive"/>, however the resume is timed against the receive callback that held the
    /// stream — also when it credits no bytes, which is how a session resumes a stream it could not take a message from.
    /// </summary>
    /// <remarks>
    /// Three streams. The server holds the first one two thousand times with <c>PendingAfter(0)</c> while another thread
    /// resumes it as soon as each hold is announced — before the callback has returned, as it returns, or after: every one
    /// of those indications starts at offset 0, and the stream then arrives whole. The second one is consumed as it is
    /// indicated and held at the same time (<c>PendingAfter(all)</c>): nothing more happens on it until the resume, and then
    /// the rest and the end arrive with no byte indicated twice. The third one's FIN arrives alone and is held: the peer's
    /// send shutdown is reported after the resume, not before.
    /// </remarks>
    public static void HeldStreamIsIndicatedAgainAfterEveryResume(ITransportTestHarness harness)
    {
        const int Holds = 2000;
        const int Length = 100;
        using var s = new Session(harness);
        ITransport server = s.Server;
        var gate = new object();
        var taken = new MemoryStream();
        TransportStreamId announced = default;
        int phase = 1;
        int signal = 0;
        int stop = 0;
        int holds = 0;
        int calls = 0;
        int errors = 0;
        int finHeld = 0;
        s.ServerSink.ReceiveHandler = (id, segments, offset, fin) =>
        {
            int total = 0;
            foreach (TransportSegment segment in segments) total += (int)segment.Length;
            lock (gate)
            {
                calls++;
                // Whatever the stream, the sink is never shown a byte twice and never skips one.
                bool held = phase == 1 && holds < Holds;
                if (offset != (ulong)taken.Length) errors++;
                if (held)
                {
                    holds++;
                    announced = id;
                    Volatile.Write(ref signal, 1);
                    return ReceiveResult.PendingAfter(0);
                }

                foreach (TransportSegment segment in segments) taken.Write(segment.AsSpan());
                if (phase == 2 && calls == 1)
                {
                    announced = id;
                    return ReceiveResult.PendingAfter(total);
                }

                if (phase == 3 && total == 0 && fin && finHeld == 0)
                {
                    finHeld = 1;
                    announced = id;
                    return ReceiveResult.PendingAfter(0);
                }

                return ReceiveResult.Consumed(total);
            }
        };
        var resumer = new Thread(() =>
        {
            var random = new Random(7);
            while (Volatile.Read(ref stop) == 0)
            {
                if (Interlocked.Exchange(ref signal, 0) == 0)
                {
                    Thread.Yield();
                    continue;
                }

                // A varying moment after the hold was announced: the callback that holds the stream may still be running,
                // may be returning, or may have returned.
                int spins = random.Next(0, 60);
                for (int i = 0; i < spins; i++) Thread.SpinWait(1);
                TransportStreamId id;
                lock (gate) id = announced;
                server.ResumeStreamReceive(id, 0);
            }
        })
        {
            IsBackground = true,
            Name = "conformance-resumer",
        };
        int Read(ref int value)
        {
            lock (gate) return value;
        }
        void Begin(int next)
        {
            lock (gate)
            {
                phase = next;
                calls = 0;
                taken.SetLength(0);
            }
        }
        byte[] Taken()
        {
            lock (gate) return taken.ToArray();
        }

        s.WaitConnected();
        NativeSegments segments = s.RentSegments(3);
        try
        {
            resumer.Start();

            // 1. Held and resumed with zero bytes, again and again, by a thread that races the callback.
            NativeBuffer first = s.Rent(Length, seed: 31);
            segments.Set(0, first.Segment(0, Length));
            s.Require(s.Client.OpenStream(StreamKind.Unidirectional, 1, 32767, out TransportStreamId one) == TransportStatus.Success, "OpenStream failed.");
            s.Require(s.Client.SendStream(one, segments.At(0), 1, 1, TransportSendFlags.Start | TransportSendFlags.Fin) == TransportStatus.Success, "SendStream failed.");
            bool ended = s.Harness.Pump(() => s.ServerSink.CountOf(RecordedEventKind.StreamPeerSendShutdown) == 1 || Read(ref errors) != 0, s.Harness.DefaultTimeout);
            s.Require(ended, $"a stream that was held and resumed with zero bytes was indicated {Read(ref holds)} times and then never again.");
            s.Require(Read(ref errors) == 0, "an indication of the held stream did not start where the sink had stopped consuming.");
            s.Require(Read(ref holds) == Holds, $"the stream was held {Read(ref holds)} times, expected {Holds}.");
            s.Require(Taken().AsSpan().SequenceEqual(first.ToArray(0, Length)), "the held stream did not arrive whole after its last resume.");
        }
        finally
        {
            Volatile.Write(ref stop, 1);
        }

        s.Require(resumer.Join(TimeSpan.FromSeconds(10)), "the resuming thread did not finish.");

        // 2. Consumed whole and held: nothing more until the resume, then the end of the stream, and no byte twice.
        Begin(2);
        NativeBuffer second = s.Rent(Length, seed: 32);
        segments.Set(1, second.Segment(0, Length));
        s.Require(s.Client.OpenStream(StreamKind.Unidirectional, 2, 32767, out TransportStreamId two) == TransportStatus.Success, "OpenStream failed.");
        s.Require(s.Client.SendStream(two, segments.At(1), 1, 2, TransportSendFlags.Start | TransportSendFlags.Fin) == TransportStatus.Success, "SendStream failed.");
        s.Wait(() => Read(ref calls) >= 1, "the second stream's first indication");
        s.Settle(TimeSpan.FromMilliseconds(100));
        s.Require(Read(ref calls) == 1, $"a stream held with PendingAfter(all) was indicated {Read(ref calls)} times before it was resumed.");
        s.Require(s.ServerSink.CountOf(RecordedEventKind.StreamPeerSendShutdown) == 1, "a stream held with PendingAfter(all) reported its end before it was resumed.");
        TransportStreamId held;
        lock (gate) held = announced;
        server.ResumeStreamReceive(held, 0);
        s.Wait(() => s.ServerSink.CountOf(RecordedEventKind.StreamPeerSendShutdown) == 2, "the end of the stream that was consumed whole and held, after its resume");
        s.Require(Read(ref errors) == 0, "after a hold with PendingAfter(all) an indication did not start where the sink had stopped consuming.");
        s.Require(Taken().AsSpan().SequenceEqual(second.ToArray(0, Length)), "the stream that was consumed whole and held did not arrive exactly once.");

        // 3. The FIN arrives alone and is held.
        Begin(3);
        NativeBuffer third = s.Rent(Length, seed: 33);
        segments.Set(2, third.Segment(0, Length));
        s.Require(s.Client.OpenStream(StreamKind.Unidirectional, 3, 32767, out TransportStreamId three) == TransportStatus.Success, "OpenStream failed.");
        s.Require(s.Client.SendStream(three, segments.At(2), 1, 3, TransportSendFlags.Start) == TransportStatus.Success, "SendStream failed.");
        s.Wait(() => Taken().Length == Length, "the third stream's data");
        s.Require(s.Client.SendStream(three, null, 0, 4, TransportSendFlags.Fin) == TransportStatus.Success, "SendStream of the FIN alone failed.");
        s.Wait(() => Read(ref finHeld) == 1, "the indication that carries only the FIN");
        s.Settle(TimeSpan.FromMilliseconds(100));
        s.Require(s.ServerSink.CountOf(RecordedEventKind.StreamPeerSendShutdown) == 2, "a held FIN was reported as the peer's send shutdown before the stream was resumed.");
        lock (gate) held = announced;
        server.ResumeStreamReceive(held, 0);
        s.Wait(() => s.ServerSink.CountOf(RecordedEventKind.StreamPeerSendShutdown) == 3, "the peer's send shutdown after the held FIN was resumed");
        s.Require(Read(ref errors) == 0, "after a held FIN an indication did not start at the end of the stream.");
        s.Require(Taken().AsSpan().SequenceEqual(third.ToArray(0, Length)), "the third stream's data did not arrive exactly once.");
        s.CloseAndWait();
    }

    // ------------------------------------------------------------------ refused connections and the ways a connection ends

    /// <summary>
    /// PreHandshake Reject and an accept callback that returns null: each client sees exactly one OnClosed(Transport), last,
    /// and nothing but capability reports before it; the accept callback is not called after a Reject; the transport handed
    /// to an accept callback that refused it is Closed (nothing will ever happen on it), before and after Dispose.
    /// </summary>
    public static void RefusedConnectionsCloseTheClientOnceAndLeaveTheRefusedTransportClosed(ITransportTestHarness harness)
    {
        int acceptsAfterReject = 0;
        var rejectedSink = new RecordingSink();
        ITransport rejected = harness.Connect(rejectedSink, static (in NewConnectionInfo _) => PreHandshakeDecision.Reject, (ITransport _, in NewConnectionInfo _) =>
        {
            Interlocked.Increment(ref acceptsAfterReject);
            return null;
        });
        rejectedSink.Transport = rejected;
        ITransport? refused = null;
        var refusedSink = new RecordingSink();
        ITransport nulled = harness.Connect(refusedSink, static (in NewConnectionInfo _) => PreHandshakeDecision.Accept, (ITransport transport, in NewConnectionInfo _) =>
        {
            Volatile.Write(ref refused, transport);
            return null;
        });
        refusedSink.Transport = nulled;
        if (!harness.Pump(() => rejectedSink.IsClosed && refusedSink.IsClosed, harness.DefaultTimeout))
        {
            throw Fail(harness, "timed out waiting for OnClosed on both refused clients.", ("rejected client", rejectedSink), ("refused client", refusedSink));
        }
        harness.Pump(static () => false, TimeSpan.FromMilliseconds(200));
        RequireNeverConnected(harness, rejectedSink, "the client PreHandshake rejected", TransportCloseReason.Transport, errorCode: null);
        RequireNeverConnected(harness, refusedSink, "the client the accept callback refused", TransportCloseReason.Transport, errorCode: null);
        if (Volatile.Read(ref acceptsAfterReject) != 0) throw Fail(harness, "the accept callback ran for a connection PreHandshake rejected.");
        ITransport? transport = Volatile.Read(ref refused);
        if (transport is null) throw Fail(harness, "the accept callback never ran.");
        if (transport.State != TransportState.Closed) throw Fail(harness, $"the transport an accept callback refused reports {transport.State}; nothing will ever happen on it, so it must be Closed.");
        transport.Dispose();
        if (transport.State != TransportState.Closed) throw Fail(harness, $"after Dispose the refused transport reports {transport.State}.");
        if (rejected.State != TransportState.Closed || nulled.State != TransportState.Closed) throw Fail(harness, $"the refused clients report {rejected.State} and {nulled.State} after OnClosed.");
    }

    /// <summary>
    /// A handshake that fails after the listener accepted the connection (MsQuic: the client rejects the server's
    /// certificate): neither end raises OnConnected, and each reports exactly one OnClosed(Transport), last, with nothing
    /// but capability reports before it.
    /// </summary>
    public static void HandshakeFailureClosesBothEndsWithoutConnecting(ITransportTestHarness harness)
    {
        using var s = new Session(harness, new ConformancePairOptions { FailHandshake = true });
        s.Wait(() => s.BothClosed, "OnClosed on both ends of the failed handshake");
        s.Settle(TimeSpan.FromMilliseconds(100));
        RequireNeverConnected(harness, s.ClientSink, "client", TransportCloseReason.Transport, errorCode: null);
        RequireNeverConnected(harness, s.ServerSink, "server", TransportCloseReason.Transport, errorCode: null);
        s.Require(s.Client.State == TransportState.Closed && s.Server.State == TransportState.Closed, $"states after the failed handshake: client {s.Client.State}, server {s.Server.State}.");
    }

    /// <summary>
    /// <see cref="ITransport.Close"/> while the handshake is still running: the closing end reports exactly one
    /// OnClosed(Local) with its error code, last, and never OnConnected; a transport the listener accepted for the attempt
    /// (if any) reports exactly one OnClosed, last, and never OnConnected either.
    /// </summary>
    public static void CloseWhileConnecting(ITransportTestHarness harness)
    {
        var clientSink = new RecordingSink();
        var servers = new List<RecordingSink>();
        int closeCalled = 0;
        ITransport client = harness.Connect(
            clientSink,
            (in NewConnectionInfo _) =>
            {
                // Hold the handshake until the client has called Close, so the close surely lands while connecting: a real
                // transport runs this on its own thread, a simulator only inside Pump (after Close).
                long deadline = Environment.TickCount64 + 5_000;
                while (Volatile.Read(ref closeCalled) == 0 && Environment.TickCount64 < deadline) Thread.Sleep(1);
                return PreHandshakeDecision.Accept;
            },
            (ITransport transport, in NewConnectionInfo _) =>
            {
                var sink = new RecordingSink { Transport = transport };
                lock (servers) servers.Add(sink);
                return sink;
            });
        clientSink.Transport = client;
        TransportState before = client.State;
        // No reason: with one, MsQuic makes a parameter call that waits for the worker the gate above may be holding.
        client.Close(0x21, default);
        Volatile.Write(ref closeCalled, 1);
        if (before != TransportState.Connecting) throw Fail(harness, $"the client reported {before} right after Connect, expected Connecting.", ("client", clientSink));
        bool AllClosed()
        {
            if (!clientSink.IsClosed) return false;
            lock (servers)
            {
                foreach (RecordingSink sink in servers)
                {
                    if (!sink.IsClosed) return false;
                }
            }
            return true;
        }
        if (!harness.Pump(AllClosed, harness.DefaultTimeout)) throw Fail(harness, "timed out waiting for OnClosed after closing while connecting.", ("client", clientSink));
        harness.Pump(static () => false, TimeSpan.FromMilliseconds(200));
        RequireNeverConnected(harness, clientSink, "the client that closed while connecting", TransportCloseReason.Local, 0x21);
        RecordingSink[] accepted;
        lock (servers) accepted = [.. servers];
        foreach (RecordingSink sink in accepted) RequireNeverConnected(harness, sink, "the server end of the abandoned attempt", reason: null, errorCode: null);
        if (client.State != TransportState.Closed) throw Fail(harness, $"the client reports {client.State} after OnClosed.", ("client", clientSink));
    }

    /// <summary>
    /// Both ends call <see cref="ITransport.Close"/> at about the same time: each reports exactly one OnClosed, last, either
    /// Local with its own code or Peer with the other end's code (when that close arrived first), and at least one end
    /// reports Local; before it every accepted send completed exactly once, every accepted datagram reached a final state
    /// and every stream reported its shutdown.
    /// </summary>
    public static void BothEndsClosingAtOnce(ITransportTestHarness harness)
    {
        using var s = new Session(harness);
        s.ServerSink.ReceiveHandler = static (_, _, _, _) => ReceiveResult.PendingAfter(0); // keeps the client's sends in flight
        s.WaitConnected();
        s.WaitDatagrams();
        NativeBuffer data = s.Rent(16 * 1024, seed: 19);
        NativeSegments segments = s.RentSegments(3);
        segments.Set(0, data.Segment(0, 16 * 1024));
        segments.Set(1, data.Segment(0, 1024));
        segments.Set(2, data.Segment(0, 100));
        s.Require(s.Client.OpenStream(StreamKind.Bidirectional, 1, 32767, out TransportStreamId clientStream) == TransportStatus.Success, "client OpenStream failed.");
        for (int i = 0; i < 4; i++)
        {
            TransportStatus status = s.Client.SendStream(clientStream, segments.At(0), 1, (ulong)(i + 1), i == 0 ? TransportSendFlags.Start : TransportSendFlags.None);
            s.Require(status == TransportStatus.Success, $"client SendStream #{i + 1} returned {status}.");
        }
        s.Require(s.Server.OpenStream(StreamKind.Unidirectional, 2, 32767, out TransportStreamId serverStream) == TransportStatus.Success, "server OpenStream failed.");
        s.Require(s.Server.SendStream(serverStream, segments.At(1), 1, 11, TransportSendFlags.Start) == TransportStatus.Success, "server SendStream failed.");
        s.Require(s.Client.SendDatagram(segments.At(2), 1, 21, TransportSendFlags.None) == TransportStatus.Success, "client SendDatagram failed.");
        s.Require(s.Server.SendDatagram(segments.At(2), 1, 22, TransportSendFlags.None) == TransportStatus.Success, "server SendDatagram failed.");
        s.Client.Close(0x31, default);
        s.Server.Close(0x32, default);
        s.Wait(() => s.BothClosed, "OnClosed on both ends");
        s.Settle(TimeSpan.FromMilliseconds(100));
        RecordedEvent clientClosed = Single(s, s.ClientSink, static e => e.Kind == RecordedEventKind.Closed, "client OnClosed");
        RecordedEvent serverClosed = Single(s, s.ServerSink, static e => e.Kind == RecordedEventKind.Closed, "server OnClosed");
        bool clientLocal = clientClosed.CloseReason == TransportCloseReason.Local && clientClosed.ErrorCode == 0x31;
        bool clientPeer = clientClosed.CloseReason == TransportCloseReason.Peer && clientClosed.ErrorCode == 0x32;
        bool serverLocal = serverClosed.CloseReason == TransportCloseReason.Local && serverClosed.ErrorCode == 0x32;
        bool serverPeer = serverClosed.CloseReason == TransportCloseReason.Peer && serverClosed.ErrorCode == 0x31;
        s.Require(clientLocal || clientPeer, $"the client reported {clientClosed.CloseReason} code 0x{clientClosed.ErrorCode:X}; expected Local 0x31 or Peer 0x32.");
        s.Require(serverLocal || serverPeer, $"the server reported {serverClosed.CloseReason} code 0x{serverClosed.ErrorCode:X}; expected Local 0x32 or Peer 0x31.");
        s.Require(clientLocal || serverLocal, "neither end reported Local although both called Close.");
        RequireClosedLast(s, s.ClientSink, "client");
        RequireClosedLast(s, s.ServerSink, "server");
        RequireSendsSettledBeforeClosed(s, s.ClientSink, [1, 2, 3, 4], [21], "client");
        RequireSendsSettledBeforeClosed(s, s.ServerSink, [11], [22], "server");
        RequireShutdownBeforeClosed(s, s.ClientSink, clientStream, "client");
        RequireShutdownBeforeClosed(s, s.ServerSink, serverStream, "server");
    }

    private static void RequireSendsSettledBeforeClosed(Session s, RecordingSink sink, ulong[] streamSends, ulong[] datagrams, string end)
    {
        IReadOnlyList<RecordedEvent> events = sink.Events;
        int closed = IndexOf(events, static e => e.Kind == RecordedEventKind.Closed);
        foreach (ulong context in streamSends)
        {
            int count = 0;
            for (int i = 0; i < events.Count; i++)
            {
                RecordedEvent e = events[i];
                if (e.Kind != RecordedEventKind.StreamSendCompleted || e.Context != context) continue;
                s.Require(i < closed, $"{end}: send {context} completed after OnClosed.");
                count++;
            }
            s.Require(count == 1, $"{end}: send {context} completed {count} times.");
        }
        foreach (ulong context in datagrams)
        {
            int final = IndexOf(events, e => e.Kind == RecordedEventKind.DatagramSendStateChanged && e.Context == context && e.DatagramState.IsFinal());
            s.Require(final >= 0 && final < closed, $"{end}: datagram {context} reached no final state before OnClosed.");
        }
    }

    /// <summary>
    /// A connection the transport closes on its own (MsQuic: the idle timeout; the simulator: link loss) reports
    /// OnClosed(Transport) exactly once and last on both ends, after OnStreamShutdownComplete for every stream, started or
    /// not; the error code and status are transport-specific. Calls afterwards are refused.
    /// </summary>
    public static void TransportInitiatedCloseReportsTransport(ITransportTestHarness harness)
    {
        using var s = new Session(harness, new ConformancePairOptions { TransportCloseAfter = TimeSpan.FromSeconds(1) });
        s.WaitConnected();
        NativeBuffer data = s.Rent(256, seed: 14);
        NativeSegments segments = s.RentSegments(1);
        segments.Set(0, data.Segment(0, 256));
        s.Require(s.Client.OpenStream(StreamKind.Bidirectional, 1, 32767, out TransportStreamId open) == TransportStatus.Success, "OpenStream failed.");
        s.Require(s.Client.SendStream(open, segments.At(0), 1, 1, TransportSendFlags.Start) == TransportStatus.Success, "SendStream failed.");
        s.Require(s.Client.OpenStream(StreamKind.Bidirectional, 2, 32767, out TransportStreamId idle) == TransportStatus.Success, "OpenStream (never started) failed.");
        s.Wait(() => s.ServerSink.CountOf(RecordedEventKind.StreamReceived) > 0 && CountStream(s.ClientSink, RecordedEventKind.StreamSendCompleted, open) == 1, "the stream's data at the server and its completion");
        s.Wait(() => s.BothClosed, "the transport closing the idle connection on both ends");
        s.Settle(TimeSpan.FromMilliseconds(100));
        foreach ((RecordingSink sink, string end) in new[] { (s.ClientSink, "client"), (s.ServerSink, "server") })
        {
            RecordedEvent closed = Single(s, sink, static e => e.Kind == RecordedEventKind.Closed, end + " OnClosed");
            s.Require(closed.CloseReason == TransportCloseReason.Transport, $"{end}: a connection the transport closed reported {closed.CloseReason}.");
            RequireClosedLast(s, sink, end);
        }
        RequireShutdownBeforeClosed(s, s.ClientSink, open, "client (started stream)");
        RequireShutdownBeforeClosed(s, s.ClientSink, idle, "client (never started stream)");
        RequireShutdownBeforeClosed(s, s.ServerSink, s.ServerSink.OfKind(RecordedEventKind.PeerStreamStarted)[0].StreamId, "server");
        s.Require(s.Client.State == TransportState.Closed && s.Server.State == TransportState.Closed, $"states after the transport closed: client {s.Client.State}, server {s.Server.State}.");
        TransportStatus status = s.Client.OpenStream(StreamKind.Bidirectional, 3, 32767, out _);
        s.Require(status == TransportStatus.InvalidState, $"OpenStream after the transport closed returned {status}.");
        status = s.Client.SendDatagram(segments.At(0), 1, 9, TransportSendFlags.None);
        s.Require(status == TransportStatus.InvalidState, $"SendDatagram after the transport closed returned {status}.");
    }

    // ------------------------------------------------------------------ helpers for scenarios without a Session

    /// <summary>
    /// An end that never connected: exactly one OnClosed, last, with nothing but capability reports before it; optionally
    /// with the given reason and error code.
    /// </summary>
    private static void RequireNeverConnected(ITransportTestHarness harness, RecordingSink sink, string end, TransportCloseReason? reason, ulong? errorCode)
    {
        IReadOnlyList<RecordedEvent> events = sink.Events;
        if (events.Count == 0 || events[^1].Kind != RecordedEventKind.Closed || sink.CountOf(RecordedEventKind.Closed) != 1)
        {
            throw Fail(harness, $"{end}: OnClosed must be raised exactly once and last.", (end, sink));
        }
        foreach (RecordedEvent e in events)
        {
            if (e.Kind is not (RecordedEventKind.Closed or RecordedEventKind.DatagramCapabilityChanged))
            {
                throw Fail(harness, $"{end}: {e.Kind} was raised on a connection that never connected.", (end, sink));
            }
        }
        RecordedEvent closed = events[^1];
        if (reason is not null && closed.CloseReason != reason.Value) throw Fail(harness, $"{end}: OnClosed reported {closed.CloseReason}, expected {reason.Value}.", (end, sink));
        if (errorCode is not null && closed.ErrorCode != errorCode.Value) throw Fail(harness, $"{end}: OnClosed reported error code 0x{closed.ErrorCode:X}, expected 0x{errorCode.Value:X}.", (end, sink));
    }

    private static ConformanceException Fail(ITransportTestHarness harness, string message, params (string Name, RecordingSink Sink)[] ends)
    {
        var text = new StringBuilder();
        text.Append('[').Append(harness.Name).Append("] ").Append(message);
        foreach ((string name, RecordingSink sink) in ends)
        {
            text.AppendLine().Append(name).Append(" events:");
            IReadOnlyList<RecordedEvent> events = sink.Events;
            int start = Math.Max(0, events.Count - 60);
            if (start > 0) text.AppendLine().Append("  ... ").Append(start).Append(" earlier events");
            for (int i = start; i < events.Count; i++) text.AppendLine().Append("  ").Append(events[i].ToString());
        }
        return new ConformanceException(text.ToString());
    }
}
