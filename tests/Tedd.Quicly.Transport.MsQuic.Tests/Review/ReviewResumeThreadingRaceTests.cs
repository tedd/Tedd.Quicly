using System.Collections.Concurrent;
using System.Diagnostics;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Conformance;
using Tedd.Quicly.Transport.MsQuic.Tests.Transport;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Review;

/// <summary>
/// Adversarial review of fix/msquic-held-stream-resume (8107fc3), threading lens, at the transport: the per-slot state the
/// commit added (the skip count, FinHeld) and changed (ReceiveState, PendingConsumed/PendingTotal) is written by the MsQuic
/// worker inside the receive callback and by whatever thread calls <c>ResumeStreamReceive</c>, and read by both. These
/// tests race the two over real loopback and check the one thing the sink is promised: every byte exactly once, in
/// order, then the end of the stream — whatever mix of full, partial and zero consumption, holds (also of a whole
/// indication and of a FIN that arrives alone) and credits the sink chooses, and however the resume is timed.
/// </summary>
[Collection(MsQuicCollection.Name)]
public unsafe class ReviewResumeThreadingRaceTests
{
    private sealed class StreamState(TransportStreamId id, int seed)
    {
        public readonly TransportStreamId Id = id;
        public readonly Random Random = new(seed);
        public readonly MemoryStream Taken = new();
        public byte[] Remainder = [];
        public bool Held;
        public bool FinSeen;
        public int PeerShutdowns;
        public bool Complete;
        public int Holds;
        public int Calls;
        public string? Error;
    }

    /// <summary>A sink that consumes, holds and credits at random and records what it was shown.</summary>
    private sealed class ChaosSink(int seed) : NullTransportSink
    {
        private int _streams;

        public readonly ConcurrentDictionary<TransportStreamId, StreamState> States = new();
        public readonly ConcurrentQueue<StreamState> ToResume = new();
        public ITransport? Transport;
        public long Progress;
        public int Completed;
        public volatile bool Connected;

        public override void OnConnected(in TransportConnectedInfo info) => Connected = true;

        public override void OnPeerStreamStarted(TransportStreamId id, StreamKind kind) =>
            States[id] = new StreamState(id, seed + Interlocked.Increment(ref _streams));

        public override ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin)
        {
            StreamState state = States[id];
            int total = 0;
            foreach (TransportSegment segment in segments) total += (int)segment.Length;
            byte[] data = new byte[total];
            int at = 0;
            foreach (TransportSegment segment in segments)
            {
                segment.AsSpan().CopyTo(data.AsSpan(at));
                at += (int)segment.Length;
            }

            ReceiveResult result;
            int spins = 0;
            lock (state)
            {
                Interlocked.Increment(ref Progress);
                state.Calls++;
                if (state.Held) state.Error ??= $"indication {state.Calls} arrived while the stream was held and not yet resumed";
                if (absoluteOffset != (ulong)state.Taken.Length)
                {
                    state.Error ??= $"indication {state.Calls} starts at offset {absoluteOffset}, but the sink has taken {state.Taken.Length} bytes ({total} indicated, fin {fin})";
                }

                if (state.FinSeen && total == 0) state.Error ??= $"indication {state.Calls} shows the sink nothing new: the FIN alone, a second time";
                state.FinSeen |= fin;
                int roll = state.Random.Next(100);
                int keep;
                bool hold;
                if (total == 0)
                {
                    keep = 0;
                    hold = roll < 40;
                }
                else if (roll < 35)
                {
                    keep = total;
                    hold = false;
                }
                else if (roll < 50)
                {
                    // A partial consumption of at least one byte: indicated again at once, no resume.
                    keep = total == 1 ? 1 : state.Random.Next(1, total);
                    hold = false;
                }
                else if (roll < 60)
                {
                    // Consumed(0) of a non-empty indication counts as PendingAfter(0).
                    keep = 0;
                    hold = true;
                }
                else if (roll < 85)
                {
                    keep = state.Random.Next(0, total);
                    hold = true;
                }
                else
                {
                    keep = total;
                    hold = true;
                }

                state.Taken.Write(data, 0, keep);
                if (hold)
                {
                    state.Holds++;
                    state.Held = true;
                    state.Remainder = data.AsSpan(keep).ToArray();
                    ToResume.Enqueue(state);
                    result = roll is >= 50 and < 60 && total != 0 ? ReceiveResult.Consumed(0) : ReceiveResult.PendingAfter(keep);

                    // Keep the callback running for a varying moment, so that the resume lands before it has returned as
                    // often as after.
                    spins = state.Random.Next(3) == 0 ? 0 : state.Random.Next(1, 400);
                }
                else
                {
                    result = ReceiveResult.Consumed(keep);
                }
            }

            for (int i = 0; i < spins; i++) Thread.SpinWait(1);
            return result;
        }

        public override void OnStreamPeerSendShutdown(TransportStreamId id)
        {
            StreamState state = States[id];
            lock (state)
            {
                state.PeerShutdowns++;
                if (state.Held) state.Error ??= "the peer's send shutdown was reported while the stream was held";
            }
        }

        public override void OnStreamShutdownComplete(TransportStreamId id)
        {
            StreamState state = States[id];
            lock (state) state.Complete = true;
            Interlocked.Increment(ref Completed);
            Interlocked.Increment(ref Progress);
            Transport?.CloseStream(id);
        }
    }

    private static void Resumer(ChaosSink sink, ITransport server, int seed, ref int stop, ConcurrentQueue<string> faults)
    {
        Random random = new(seed);
        while (Volatile.Read(ref stop) == 0)
        {
            if (!sink.ToResume.TryDequeue(out StreamState? state))
            {
                Thread.Yield();
                continue;
            }

            int spins = random.Next(0, 120);
            for (int i = 0; i < spins; i++) Thread.SpinWait(1);
            int credit;
            lock (state)
            {
                int roll = random.Next(100);
                credit = roll < 50 ? 0 : roll < 75 ? state.Remainder.Length : random.Next(0, state.Remainder.Length + 1);
                state.Taken.Write(state.Remainder, 0, credit);
                state.Remainder = [];
                state.Held = false;
            }

            try
            {
                server.ResumeStreamReceive(state.Id, credit);
            }
            catch (Exception ex)
            {
                faults.Enqueue($"ResumeStreamReceive({credit}) threw {ex.GetType().Name}: {ex.Message}");
            }
        }
    }

    private sealed class ConnectedSink : NullTransportSink
    {
        public volatile bool Connected;

        public override void OnConnected(in TransportConnectedInfo info) => Connected = true;
    }

    [Fact]
    public void Every_Byte_Arrives_Exactly_Once_Whatever_The_Sink_Holds_Credits_And_However_The_Resume_Is_Timed()
    {
        if (!MsQuicApi.TryGetInstance(out _, out _))
        {
            Assert.Skip("MsQuic is not available on this host.");
        }

        MsQuicTransportHarness harness = new();
        List<string> problems = RunChaos(harness, streams: 12, rounds: 3, out long holds);
        Assert.True(problems.Count == 0, string.Join("\n", problems.Take(12)));
        Assert.True(holds > 100, $"only {holds} holds: the test did not exercise the resume path");
        Assert.Null(harness.CleanupError);
        Assert.Equal(0, harness.SinkExceptionTotal);
    }

    /// <summary>Runs the chaos sink against pairs of <paramref name="harness"/> and disposes it; returns what went wrong.</summary>
    internal static List<string> RunChaos(ITransportTestHarness harness, int streams, int rounds, out long holds)
    {
        int Streams = streams;
        int Rounds = rounds;
        List<string> problems = [];
        List<IDisposable> native = [];
        holds = 0;
        try
        {
            for (int round = 0; round < Rounds && problems.Count == 0; round++)
            {
                ChaosSink sink = new(1000 * (round + 1));
                ConnectedSink clientSink = new();
                ConformancePair pair = harness.CreatePair(clientSink, sink, new ConformancePairOptions { ServerPeerUnidiStreams = (ushort)Streams });
                ITransport client = pair.Client;
                ITransport server = pair.Server;
                sink.Transport = server;
                Assert.True(Spin.Until(() => clientSink.Connected && sink.Connected, TimeSpan.FromSeconds(10)), "the pair never connected");

                int stop = 0;
                ConcurrentQueue<string> faults = new();
                Thread[] resumers = new Thread[2];
                for (int i = 0; i < resumers.Length; i++)
                {
                    int seed = (round * 10) + i;
                    resumers[i] = new Thread(() => Resumer(sink, server, seed, ref stop, faults)) { IsBackground = true, Name = "review-resumer" };
                    resumers[i].Start();
                }

                // The senders: every stream in several sends of varying size, the FIN with the last one or on its own.
                Random random = new(round + 77);
                byte[][] expected = new byte[Streams][];
                for (int index = 0; index < Streams; index++)
                {
                    int length = index == 0 ? 1 : index == 1 ? 300_000 : random.Next(1, 120_000);
                    NativeBuffer buffer = new(length);
                    native.Add(buffer);
                    buffer.Fill(seed: index + 1);
                    buffer.Pointer[0] = (byte)index;
                    expected[index] = buffer.ToArray(0, length);
                    List<int> cuts = [];
                    for (int rest = length; rest > 0;)
                    {
                        int take = Math.Min(rest, random.Next(1, 40_000));
                        cuts.Add(take);
                        rest -= take;
                    }

                    bool finAlone = random.Next(2) == 0;
                    NativeSegments segments = new(cuts.Count);
                    native.Add(segments);
                    Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Unidirectional, (ulong)index, 32767, out TransportStreamId id));
                    int offset = 0;
                    for (int i = 0; i < cuts.Count; i++)
                    {
                        segments.Set(i, buffer.Segment(offset, cuts[i]));
                        offset += cuts[i];
                        TransportSendFlags flags = i == 0 ? TransportSendFlags.Start : TransportSendFlags.None;
                        if (i == cuts.Count - 1 && !finAlone) flags |= TransportSendFlags.Fin;
                        Assert.Equal(TransportStatus.Success, client.SendStream(id, segments.At(i), 1, (ulong)i, flags));
                        if (random.Next(4) == 0) Thread.Sleep(random.Next(0, 2));
                    }

                    if (finAlone)
                    {
                        if (random.Next(2) == 0) Thread.Sleep(random.Next(1, 20));
                        Assert.Equal(TransportStatus.Success, client.SendStream(id, null, 0, 999, TransportSendFlags.Fin));
                    }
                }

                long lastProgress = -1;
                long lastChange = Stopwatch.GetTimestamp();
                bool stalled = false;
                while (Volatile.Read(ref sink.Completed) < Streams)
                {
                    long progress = Interlocked.Read(ref sink.Progress);
                    long now = Stopwatch.GetTimestamp();
                    if (progress != lastProgress)
                    {
                        lastProgress = progress;
                        lastChange = now;
                    }
                    else if (now - lastChange > 8 * Stopwatch.Frequency)
                    {
                        stalled = true;
                        break;
                    }

                    Thread.Sleep(2);
                }

                Volatile.Write(ref stop, 1);
                foreach (Thread resumer in resumers) resumer.Join(TimeSpan.FromSeconds(10));
                foreach (string fault in faults) problems.Add($"round {round}: {fault}");
                foreach (StreamState state in sink.States.Values)
                {
                    lock (state)
                    {
                        holds += state.Holds;
                        byte[] taken = state.Taken.ToArray();
                        int index = taken.Length > 0 ? taken[0] : -1;
                        string label = $"round {round}, stream {index} (slot {state.Id.Slot}, {state.Calls} indications, {state.Holds} holds)";
                        if (state.Error is not null) problems.Add($"{label}: {state.Error}");
                        if (!state.Complete)
                        {
                            problems.Add($"{label}: never completed — {taken.Length} bytes taken of {(index >= 0 && index < Streams ? expected[index].Length : -1)}, "
                                + $"held {state.Held}, FIN seen {state.FinSeen}, peer shutdowns {state.PeerShutdowns}{(stalled ? ", no callback for eight seconds" : string.Empty)}");
                        }
                        else if (index < 0 || index >= Streams || !taken.AsSpan().SequenceEqual(expected[index]))
                        {
                            problems.Add($"{label}: the bytes taken ({taken.Length}) are not the bytes sent ({(index >= 0 && index < Streams ? expected[index].Length : -1)})");
                        }
                        else if (state.PeerShutdowns != 1)
                        {
                            problems.Add($"{label}: the peer's send shutdown was reported {state.PeerShutdowns} times");
                        }
                    }
                }

                if (sink.States.Count != Streams) problems.Add($"round {round}: {sink.States.Count} streams arrived, {Streams} were sent");
            }
        }
        finally
        {
            harness.Dispose();
            foreach (IDisposable item in native) item.Dispose();
        }

        return problems;
    }

    private sealed class HoldOnceSink : NullTransportSink
    {
        public readonly List<(TransportStreamId Id, ulong Offset, int Length, bool Fin)> Calls = [];
        public int HoldAfter = -1;
        public int HoldCalls = 1;
        public int Completed;
        public MsQuicTransport? Transport;

        public override ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin)
        {
            int total = 0;
            foreach (TransportSegment segment in segments) total += (int)segment.Length;
            lock (Calls)
            {
                Calls.Add((id, absoluteOffset, total, fin));
                if (HoldCalls > 0 && HoldAfter >= 0)
                {
                    HoldCalls--;
                    return ReceiveResult.PendingAfter(Math.Min(HoldAfter, total));
                }
            }

            return ReceiveResult.Consumed(total);
        }

        public (TransportStreamId Id, ulong Offset, int Length, bool Fin)[] Snapshot()
        {
            lock (Calls) return [.. Calls];
        }

        public override void OnStreamShutdownComplete(TransportStreamId id)
        {
            Interlocked.Increment(ref Completed);
            Transport?.CloseStream(id);
        }
    }

    [Fact]
    public void A_Late_Resume_For_A_Closed_Stream_Does_Not_Touch_The_Stream_That_Reuses_Its_Slot()
    {
        if (!MsQuicApi.TryGetInstance(out _, out _))
        {
            Assert.Skip("MsQuic is not available on this host.");
        }

        MsQuicTransportHarness harness = new();
        using NativeBuffer buffer = new(100);
        using NativeSegments segments = new(3);
        try
        {
            buffer.Fill(3);
            HoldOnceSink sink = new() { HoldAfter = 0 };
            ConformancePair pair = harness.CreatePair(new NullTransportSink(), sink, new ConformancePairOptions { ServerPeerUnidiStreams = 4 });
            MsQuicTransport client = (MsQuicTransport)pair.Client;
            MsQuicTransport server = (MsQuicTransport)pair.Server;
            sink.Transport = server;
            Assert.True(Spin.Until(() => client.State == TransportState.Connected && server.State == TransportState.Connected, TimeSpan.FromSeconds(10)));

            // The first stream is held, then released by the application while it is held: its id goes stale.
            segments.Set(0, buffer.Segment(0, 100));
            Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Unidirectional, 1, 32767, out TransportStreamId one));
            Assert.Equal(TransportStatus.Success, client.SendStream(one, segments.At(0), 1, 1, TransportSendFlags.Start | TransportSendFlags.Fin));
            Assert.True(Spin.Until(() => sink.Snapshot().Length == 1, TimeSpan.FromSeconds(10)), "the first stream was never indicated");
            TransportStreamId stale = sink.Snapshot()[0].Id;
            server.CloseStream(stale);
            Assert.True(Spin.Until(() => server.OpenStreamCount == 0, TimeSpan.FromSeconds(10)), "the closed stream's slot was never freed");

            // The second stream takes the freed slot and is held after 40 of its 60 bytes.
            lock (sink.Calls)
            {
                sink.HoldAfter = 40;
                sink.HoldCalls = 1;
            }

            segments.Set(1, buffer.Segment(0, 60));
            Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Unidirectional, 2, 32767, out TransportStreamId two));
            Assert.Equal(TransportStatus.Success, client.SendStream(two, segments.At(1), 1, 2, TransportSendFlags.Start));
            Assert.True(Spin.Until(() => sink.Snapshot().Length == 2, TimeSpan.FromSeconds(10)), "the second stream was never indicated");
            TransportStreamId held = sink.Snapshot()[1].Id;
            Assert.Equal(stale.Slot, held.Slot);
            Assert.NotEqual(stale, held);

            // Late resumes for the stale id, with and without a credit: nothing may happen to the stream in its slot.
            server.ResumeStreamReceive(stale, 0);
            server.ResumeStreamReceive(stale, 10);
            server.ResumeStreamReceive(stale, 20);
            Thread.Sleep(300);
            Assert.Equal(2, sink.Snapshot().Length);

            // The real resume credits 5 bytes: the remainder starts at 45, exactly once.
            server.ResumeStreamReceive(held, 5);
            Assert.True(Spin.Until(() => sink.Snapshot().Length == 3, TimeSpan.FromSeconds(10)), "the held stream was not indicated again after its resume");
            Assert.Equal((held, 45ul, 15, false), sink.Snapshot()[2]);

            // A second resume of a stream that is not held is ignored, and so is one more for the stale id.
            server.ResumeStreamReceive(held, 0);
            server.ResumeStreamReceive(stale, 3);
            segments.Set(2, buffer.Segment(60, 40));
            Assert.Equal(TransportStatus.Success, client.SendStream(two, segments.At(2), 1, 3, TransportSendFlags.Fin));
            Assert.True(Spin.Until(() => Volatile.Read(ref sink.Completed) == 1, TimeSpan.FromSeconds(10)), "the second stream never completed");
            (TransportStreamId Id, ulong Offset, int Length, bool Fin)[] calls = sink.Snapshot();
            ulong next = 60;
            for (int i = 3; i < calls.Length; i++)
            {
                Assert.Equal(held, calls[i].Id);
                Assert.Equal(next, calls[i].Offset);
                next += (ulong)calls[i].Length;
            }

            Assert.Equal(100ul, next);
            Assert.True(calls[^1].Fin);
        }
        finally
        {
            harness.Dispose();
        }

        Assert.Null(harness.CleanupError);
        Assert.Equal(0, harness.SinkExceptionTotal);
    }

    /// <summary>What the receiver's sink was shown for one stream (written on the worker, read by the test when a round fails).</summary>
    private sealed class HeldTrack
    {
        public int Indications;
        public ulong LastOffset;
        public int LastTotal;
        public int LastKept;
        public bool LastFin;
        public long Taken;
        public int Aborted;
        public int PeerShutdowns;
        public int ShutdownCompletes;
    }

    private sealed class AlwaysHoldSink : NullTransportSink
    {
        public readonly ConcurrentDictionary<TransportStreamId, int> Held = new();
        public readonly ConcurrentDictionary<TransportStreamId, HeldTrack> Tracks = new();
        public readonly Random Random = new(5);
        public MsQuicTransport? Transport;
        public volatile bool Closed;

        /// <summary>Set by the test when it lets go of the streams it holds: from then on every indication is consumed whole.</summary>
        public volatile bool Release;

        public override ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin)
        {
            int total = 0;
            foreach (TransportSegment segment in segments) total += (int)segment.Length;
            HeldTrack track = Tracks.GetOrAdd(id, static _ => new HeldTrack());
            bool release = Release;

            // Worker thread only (callbacks of one connection are serialised).
            int roll = Random.Next(4);
            int kept = release || roll == 0 ? total : roll == 1 ? Random.Next(0, total + 1) : 0;
            lock (track)
            {
                track.Indications++;
                track.LastOffset = absoluteOffset;
                track.LastTotal = total;
                track.LastKept = kept;
                track.LastFin = fin;
                track.Taken += kept;
            }

            Held.AddOrUpdate(id, 1, static (_, n) => n + 1);
            return release ? ReceiveResult.Consumed(total) : ReceiveResult.PendingAfter(kept);
        }

        public override void OnStreamAborted(TransportStreamId id, ulong errorCode, StreamAbortDirection direction)
        {
            HeldTrack track = Tracks.GetOrAdd(id, static _ => new HeldTrack());
            lock (track) track.Aborted++;
        }

        public override void OnStreamPeerSendShutdown(TransportStreamId id)
        {
            HeldTrack track = Tracks.GetOrAdd(id, static _ => new HeldTrack());
            lock (track) track.PeerShutdowns++;
        }

        public override void OnStreamShutdownComplete(TransportStreamId id)
        {
            HeldTrack track = Tracks.GetOrAdd(id, static _ => new HeldTrack());
            lock (track) track.ShutdownCompletes++;
            Transport?.CloseStream(id);
        }

        public override void OnClosed(TransportCloseReason reason, ulong errorCode, int transportStatus) => Closed = true;
    }

    /// <summary>The sender's side of the teardown rounds: which sends completed (and how) and which streams shut down.</summary>
    private sealed class SenderSink : NullTransportSink
    {
        public readonly ConcurrentDictionary<TransportStreamId, bool> SendCanceled = new();
        public readonly ConcurrentDictionary<TransportStreamId, bool> ShutDown = new();

        public override void OnStreamSendCompleted(TransportStreamId id, ulong context, bool canceled) => SendCanceled[id] = canceled;

        public override void OnStreamShutdownComplete(TransportStreamId id) => ShutDown[id] = true;
    }

    /// <summary>
    /// One line per stream the receiver still has open: what its sink was shown, the transport's receive state for its slot
    /// (read by reflection: 0 idle, 1 in the callback, 2 held, 3 resumed early), and what the sender had seen of the same
    /// QUIC stream before it called <c>AbortStream</c>.
    /// </summary>
    private static string DescribeOpenStreams(MsQuicTransport server, AlwaysHoldSink sink, MsQuicTransport client, TransportStreamId[] clientIds, SenderSink sender, bool[]? shutDownBeforeAbort, bool[]? sendDoneBeforeAbort)
    {
        const System.Reflection.BindingFlags Any = System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic;
        System.Text.StringBuilder text = new();
        Array slots = (Array)typeof(MsQuicTransport).GetField("_slots", Any)!.GetValue(server)!;
        foreach (object? slot in slots)
        {
            if (slot is null) continue;
            Type type = slot.GetType();
            object? Field(string name) => type.GetField(name, Any)!.GetValue(slot);
            if (Field("Stream") is null) continue;
            TransportStreamId id = new((int)Field("Index")!, (uint)Field("Generation")!);
            long quicId = server.GetQuicStreamId(id);
            text.Append($"\n    slot {id.Slot} (QUIC stream {quicId}): receive state {Field("ReceiveState")}, skip {Field("SkipBytes")}, FIN held {Field("FinHeld")}, "
                + $"pending {Field("PendingConsumed")}/{Field("PendingTotal")}, close flags {Field("CloseFlags")}, close queued {Field("CloseQueued")}");
            if (sink.Tracks.TryGetValue(id, out HeldTrack? track))
            {
                lock (track)
                {
                    text.Append($"; sink: {track.Indications} indications, last at {track.LastOffset} of {track.LastTotal} bytes (FIN {track.LastFin}) kept {track.LastKept}, "
                        + $"{track.Taken} taken, aborted {track.Aborted}, peer send shutdown {track.PeerShutdowns}, shutdown complete {track.ShutdownCompletes}");
                }
            }
            else
            {
                text.Append("; sink: never indicated");
            }

            int index = -1;
            for (int i = 0; i < clientIds.Length; i++)
            {
                if (client.GetQuicStreamId(clientIds[i]) == quicId) index = i;
            }

            if (index < 0)
            {
                text.Append("; sender: no such stream (or the sender is gone)");
                continue;
            }

            string send = sender.SendCanceled.TryGetValue(clientIds[index], out bool canceled) ? (canceled ? "completed canceled" : "completed (acknowledged)") : "not completed";
            text.Append($"; sender: send {send}, stream shut down {sender.ShutDown.ContainsKey(clientIds[index])}");
            if (shutDownBeforeAbort is not null && sendDoneBeforeAbort is not null)
            {
                text.Append($", before its AbortStream call: send completed {sendDoneBeforeAbort[index]}, stream shut down {shutDownBeforeAbort[index]}");
            }
        }

        return text.ToString();
    }

    [Fact]
    public void Resumes_Racing_Close_Abort_And_Dispose_Of_Held_Streams_Leave_Nothing_Behind()
    {
        if (!MsQuicApi.TryGetInstance(out _, out _))
        {
            Assert.Skip("MsQuic is not available on this host.");
        }

        const int Streams = 6;
        const int Rounds = 36;
        MsQuicTransportHarness harness = new();
        ConcurrentQueue<string> faults = new();
        List<IDisposable> native = [];
        try
        {
            Random random = new(99);
            for (int round = 0; round < Rounds; round++)
            {
                // Case 5 (the sender aborts its streams) alternates: streams left unfinished, which the abort resets, and streams
                // sent whole, where the abort races the acknowledgement of the FIN.
                int kind = round % 6;
                bool fin = kind != 5 || (round / 6) % 2 == 1;
                AlwaysHoldSink sink = new();
                SenderSink sender = new();
                bool[]? shutDownBeforeAbort = null;
                bool[]? sendDoneBeforeAbort = null;
                ConformancePair pair = harness.CreatePair(sender, sink, new ConformancePairOptions { ServerPeerUnidiStreams = Streams });
                MsQuicTransport client = (MsQuicTransport)pair.Client;
                MsQuicTransport server = (MsQuicTransport)pair.Server;
                sink.Transport = server;
                Assert.True(Spin.Until(() => client.State == TransportState.Connected && server.State == TransportState.Connected, TimeSpan.FromSeconds(10)));
                NativeBuffer buffer = new(5000);
                NativeSegments segments = new(Streams);
                native.Add(buffer);
                native.Add(segments);
                TransportStreamId[] clientIds = new TransportStreamId[Streams];
                for (int i = 0; i < Streams; i++)
                {
                    segments.Set(i, buffer.Segment(0, 5000));
                    Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Unidirectional, (ulong)i, 32767, out clientIds[i]));
                    Assert.Equal(TransportStatus.Success, client.SendStream(clientIds[i], segments.At(i), 1, (ulong)i, fin ? TransportSendFlags.Start | TransportSendFlags.Fin : TransportSendFlags.Start));
                }

                Assert.True(Spin.Until(() => sink.Held.Count == Streams, TimeSpan.FromSeconds(10)), $"round {round}: only {sink.Held.Count} of {Streams} streams were indicated");
                TransportStreamId[] ids = [.. sink.Held.Keys];
                int stop = 0;
                int label = round;
                Thread resumer = new(() =>
                {
                    while (Volatile.Read(ref stop) == 0)
                    {
                        foreach (TransportStreamId id in ids)
                        {
                            try
                            {
                                server.ResumeStreamReceive(id, 0);
                            }
                            catch (Exception ex)
                            {
                                faults.Enqueue($"round {label}: ResumeStreamReceive threw {ex.GetType().Name}: {ex.Message}");
                                return;
                            }
                        }
                    }
                })
                {
                    IsBackground = true,
                    Name = "review-teardown-resumer",
                };
                resumer.Start();
                int spins = random.Next(0, 3000);
                for (int i = 0; i < spins; i++) Thread.SpinWait(1);
                try
                {
                    switch (kind)
                    {
                        case 0:
                            foreach (TransportStreamId id in ids) server.CloseStream(id);
                            break;
                        case 1:
                            foreach (TransportStreamId id in ids) server.AbortStream(id, 7, StreamAbortDirection.Receive);
                            break;
                        case 2:
                            server.Close(3, default);
                            break;
                        case 3:
                            server.Dispose();
                            break;
                        case 4:
                            client.Close(4, default);
                            break;
                        default:
                            shutDownBeforeAbort = new bool[Streams];
                            sendDoneBeforeAbort = new bool[Streams];
                            for (int i = 0; i < Streams; i++)
                            {
                                shutDownBeforeAbort[i] = sender.ShutDown.ContainsKey(clientIds[i]);
                                sendDoneBeforeAbort[i] = sender.SendCanceled.ContainsKey(clientIds[i]);
                                client.AbortStream(clientIds[i], 9, StreamAbortDirection.Send);
                            }

                            break;
                    }
                }
                catch (Exception ex)
                {
                    faults.Enqueue($"round {round}: the teardown call threw {ex.GetType().Name}: {ex.Message}");
                }

                int resumeFor = random.Next(1, 25);
                long resumeStart = Stopwatch.GetTimestamp();
                Thread.Sleep(resumeFor);

                Volatile.Write(ref stop, 1);
                Assert.True(resumer.Join(TimeSpan.FromSeconds(10)), $"round {round}: the resuming thread hung");
                double resumedMs = Stopwatch.GetElapsedTime(resumeStart).TotalMilliseconds;
                if (kind is 0 or 1 or 5)
                {
                    // The connection lives on: every stream must end and give its slot back. When the receiver closed or
                    // aborted them, and when the sender reset streams it had not finished, that needs nothing more from the
                    // sink. A stream the sender had finished is different: once its data and FIN are acknowledged the
                    // sender's abort does nothing (MsQuic sends no RESET_STREAM for a send direction whose close is
                    // acknowledged), so a stream the sink still holds when the resumer stops is held rightly, for as long as
                    // the sink holds it. There the sink lets go, and then nothing may be left behind either.
                    bool letGo = kind == 5 && fin;
                    if (letGo) sink.Release = true;
                    bool ended = Spin.Until(
                        () =>
                        {
                            if (letGo)
                            {
                                foreach (TransportStreamId id in ids) server.ResumeStreamReceive(id, 0);
                            }

                            return server.OpenStreamCount == 0;
                        },
                        TimeSpan.FromSeconds(10));
                    if (!ended)
                    {
                        faults.Enqueue($"round {round} (case {kind}, streams sent {(fin ? "whole" : "without a FIN")}): {server.OpenStreamCount} streams still open at the receiver ten seconds after "
                            + $"they were closed or aborted{(letGo ? " and the sink let go of them" : string.Empty)} (resumed for {resumedMs:F1} ms after the teardown, {spins} spins before it)"
                            + DescribeOpenStreams(server, sink, client, clientIds, sender, shutDownBeforeAbort, sendDoneBeforeAbort));
                    }
                    else if (kind == 5 && !fin)
                    {
                        // The reset of a stream the sink holds reaches the sink: that is what makes the layer above let go
                        // of what it keeps for the stream.
                        foreach (TransportStreamId id in ids)
                        {
                            HeldTrack track = sink.Tracks[id];
                            lock (track)
                            {
                                if (track.Aborted != 1 || track.ShutdownCompletes != 1 || track.PeerShutdowns != 0)
                                {
                                    faults.Enqueue($"round {round} (case {kind}): the sender reset an unfinished stream the sink held (slot {id.Slot}), and the sink saw {track.Aborted} aborts, "
                                        + $"{track.PeerShutdowns} peer send shutdowns and {track.ShutdownCompletes} shutdown completions for it");
                                }
                            }
                        }
                    }
                }

                client.Dispose();
                server.Dispose();
            }
        }
        finally
        {
            harness.Dispose();
            foreach (IDisposable item in native) item.Dispose();
        }

        Assert.True(faults.IsEmpty, string.Join("\n", faults.Take(10)));
        Assert.Null(harness.CleanupError);
        Assert.Equal(0, harness.SinkExceptionTotal);
    }

    private sealed class HoldEverySink : NullTransportSink
    {
        public readonly CallbackAllocationProbe Probe = new();
        public TransportStreamId Stream;
        public int Signal;
        public long Holds;
        public long Bytes;
        public ulong Next;
        public int OffsetErrors;
        public volatile bool Done;

        public override ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin)
        {
            Probe.Sample();
            int total = 0;
            for (int i = 0; i < segments.Length; i++) total += (int)segments[i].Length;
            if (absoluteOffset != Next) OffsetErrors++;
            if (total >= 3)
            {
                // One byte now, one more with the resume: the next indication starts two bytes on, and the transport has
                // to skip the credited byte at its head.
                Next = absoluteOffset + 2;
                Stream = id;
                Holds++;
                Bytes += 2;
                Volatile.Write(ref Signal, 1);
                return ReceiveResult.PendingAfter(1);
            }

            Next = absoluteOffset + (ulong)total;
            Bytes += total;
            if (fin) Done = true;
            return ReceiveResult.Consumed(total);
        }
    }

    /// <summary>ADR 0008: the receive callback and the resume are hot paths. A hold, a crediting resume and the skip allocate nothing.</summary>
    [Fact]
    public void Holding_Crediting_And_Skipping_Allocate_Nothing()
    {
        if (!MsQuicApi.TryGetInstance(out _, out _))
        {
            Assert.Skip("MsQuic is not available on this host.");
        }

        const int Length = 40_000;
        const int Warmup = 4_000;
        MsQuicTransportHarness harness = new();
        using NativeBuffer buffer = new(Length);
        using NativeSegments segments = new(1);
        long resumerAllocated = -1;
        HoldEverySink sink = new();
        try
        {
            ConformancePair pair = harness.CreatePair(new NullTransportSink(), sink, new ConformancePairOptions { ServerPeerUnidiStreams = 2 });
            MsQuicTransport client = (MsQuicTransport)pair.Client;
            MsQuicTransport server = (MsQuicTransport)pair.Server;
            Assert.True(Spin.Until(() => client.State == TransportState.Connected && server.State == TransportState.Connected, TimeSpan.FromSeconds(10)));
            segments.Set(0, buffer.Segment(0, Length));
            Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Unidirectional, 1, 32767, out TransportStreamId id));
            Assert.Equal(TransportStatus.Success, client.SendStream(id, segments.At(0), 1, 1, TransportSendFlags.Start | TransportSendFlags.Fin));

            long resumes = 0;
            long before = 0;
            long deadline = Stopwatch.GetTimestamp() + (60 * Stopwatch.Frequency);
            while (!sink.Done && Stopwatch.GetTimestamp() < deadline)
            {
                if (Interlocked.Exchange(ref sink.Signal, 0) == 0) continue;
                if (++resumes == Warmup)
                {
                    sink.Probe.Arm();
                    before = GC.GetAllocatedBytesForCurrentThread();
                }

                server.ResumeStreamReceive(sink.Stream, 1);
            }

            resumerAllocated = GC.GetAllocatedBytesForCurrentThread() - before;
            Assert.True(sink.Done, $"the stream did not finish: {sink.Holds} holds, {sink.Bytes} bytes");
        }
        finally
        {
            harness.Dispose();
        }

        Assert.Equal(0, sink.OffsetErrors);
        Assert.Equal(Length, sink.Bytes);
        Assert.True(sink.Holds > Warmup * 2, $"only {sink.Holds} holds");
        Assert.True(sink.Probe.Samples > Warmup, $"only {sink.Probe.Samples} samples on the worker");
        Assert.Equal(0, sink.Probe.Allocated);
        Assert.Equal(0, resumerAllocated);
        Assert.Null(harness.CleanupError);
        Assert.Equal(0, harness.SinkExceptionTotal);
    }
}