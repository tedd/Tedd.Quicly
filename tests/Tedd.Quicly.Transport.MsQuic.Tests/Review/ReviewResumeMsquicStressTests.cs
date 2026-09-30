using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Conformance;
using Tedd.Quicly.Transport.MsQuic.Tests.Transport;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Review;

/// <summary>
/// Adversarial review of 8107fc3 (a held stream is answered to MsQuic as a partial consumption), lens "msquic": the new
/// receive protocol against what MsQuic 2.5 really does. Real loopback, many streams, a sink that holds, consumes partly,
/// credits bytes on the resume, and resumer threads that race the receive callback's return. Every byte must reach the sink
/// exactly once and in order, the end of the stream exactly once, and every stream must close.
/// </summary>
[Collection(MsQuicCollection.Name)]
public unsafe class ReviewResumeMsquicStressTests
{
    private static void Log(string line)
    {
        TestContext.Current.TestOutputHelper?.WriteLine(line);
        if (Environment.GetEnvironmentVariable("REVIEW_RESUME_LOG") is { Length: > 0 } path)
        {
            lock (typeof(ReviewResumeMsquicStressTests)) File.AppendAllText(path, line + Environment.NewLine);
        }
    }

    internal static byte PatternAt(long index, int seed) => (byte)((index * 131) + (seed * 17) + ((index >> 8) * 7) + (index >> 16));

    private sealed class Plan
    {
        public int Index;
        public int Length;
        public int[] ChunkEnds = [];
        public bool FinAlone;
        public int AbortAfterChunks = -1; // the client aborts its send side after this many chunks (no FIN)
        public bool ServerAbort; // the server aborts its receive side at the stream's first hold
        public byte* Data;
        public TransportSegment* Segments;
        public TransportStreamId Id;
        public int NextChunk;
        public bool FinSent;
    }

    private sealed class Rx
    {
        public TransportStreamId Id;
        public int Index = -1;
        public long Next;
        public bool FinSeen;
        public volatile bool Held;
        public volatile bool ResumeIssued;
        public volatile bool ServerAborted;
        public int Shutdowns;
        public int Completes;
        public int Aborts;
        public int Indications;
        public int Holds;
        public long LastTotal;
    }

    private sealed class StressServerSink(Plan[] plans, int seed) : NullTransportSink
    {
        private readonly Random _random = new(seed);
        public readonly ConcurrentDictionary<TransportStreamId, Rx> Streams = new();
        public readonly ConcurrentQueue<(Rx Stream, int Credit)> Resumes = new();
        public readonly ConcurrentQueue<string> Errors = new();
        public readonly List<Rx> All = [];
        public MsQuicTransport? Transport;
        public int Completed;
        public long Indications;
        public long Holds;
        public long FinOnlyHolds;
        public long WholeHolds;
        public long MultiSegment;

        private void Error(Rx rx, string message)
        {
            if (Errors.Count < 40) Errors.Enqueue($"stream #{rx.Index} ({rx.Id}): {message}");
        }

        public override void OnPeerStreamStarted(TransportStreamId id, StreamKind kind)
        {
            var rx = new Rx { Id = id };
            Streams[id] = rx;
            lock (All) All.Add(rx);
        }

        public override ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin)
        {
            int total = 0;
            for (int i = 0; i < segments.Length; i++) total += (int)segments[i].Length;
            if (!Streams.TryGetValue(id, out Rx? rx))
            {
                Errors.Enqueue($"a receive for a stream that was never announced ({id})");
                return ReceiveResult.Consumed(total);
            }

            if (rx.ServerAborted) return ReceiveResult.Consumed(total);
            Interlocked.Increment(ref Indications);
            if (segments.Length > 1) Interlocked.Increment(ref MultiSegment);
            rx.Indications++;
            if (rx.Index < 0)
            {
                long quicId = Transport!.GetQuicStreamId(id);
                rx.Index = (int)((quicId - 2) / 4);
                if (quicId < 0 || rx.Index >= plans.Length)
                {
                    Errors.Enqueue($"unexpected QUIC stream id {quicId}");
                    rx.ServerAborted = true;
                    return ReceiveResult.Consumed(total);
                }
            }

            Plan plan = plans[rx.Index];
            if (rx.Held && !rx.ResumeIssued) Error(rx, $"indicated (offset {absoluteOffset}, {total} bytes, fin {fin}) while held and not resumed");
            rx.Held = false;
            if ((long)absoluteOffset != rx.Next) Error(rx, $"indication at offset {absoluteOffset}, the sink stopped at {rx.Next} ({total} bytes, fin {fin}, indication {rx.Indications})");
            if (rx.FinSeen && rx.Next == plan.Length) Error(rx, $"indicated again ({total} bytes, fin {fin}) after the sink had every byte and the FIN");
            if ((long)absoluteOffset + total > plan.Length) Error(rx, $"indication runs to {(long)absoluteOffset + total}, the stream is {plan.Length} long");
            if (fin && (long)absoluteOffset + total != plan.Length) Error(rx, $"FIN at {(long)absoluteOffset + total}, the stream is {plan.Length} long");
            if (fin && plan.AbortAfterChunks >= 0) Error(rx, "FIN on a stream its sender aborted");
            if (total == 0 && !fin) Error(rx, "an empty indication without FIN");

            int r = _random.Next(100);
            int consumed;
            bool pend;
            if (total == 0)
            {
                consumed = 0;
                pend = r < 50;
            }
            else if (r < 35)
            {
                consumed = total;
                pend = false;
            }
            else if (r < 50)
            {
                consumed = total > 1 ? _random.Next(1, total) : total;
                pend = false;
            }
            else if (r < 63)
            {
                consumed = 0;
                pend = true;
            }
            else if (r < 70)
            {
                consumed = 0;
                pend = false; // counts as PendingAfter(0)
            }
            else if (r < 85)
            {
                consumed = _random.Next(0, total + 1);
                pend = true;
            }
            else
            {
                consumed = total;
                pend = true;
            }

            bool hold = pend || (consumed == 0 && total != 0);
            int credit = 0;
            if (hold)
            {
                int rest = total - consumed;
                int c = _random.Next(4);
                credit = c == 0 ? rest : c == 1 ? _random.Next(0, rest + 1) : 0;
            }

            // The sink takes consumed + credit bytes of this indication: they must be the next bytes of the stream.
            int take = consumed + credit;
            long position = (long)absoluteOffset;
            int left = take;
            for (int i = 0; i < segments.Length && left > 0; i++)
            {
                ReadOnlySpan<byte> span = segments[i].AsSpan();
                int n = Math.Min(span.Length, left);
                for (int k = 0; k < n; k++)
                {
                    if (span[k] != PatternAt(position + k, plan.Index))
                    {
                        Error(rx, $"wrong byte at offset {position + k}");
                        break;
                    }
                }

                position += n;
                left -= n;
            }

            rx.Next = (long)absoluteOffset + take;
            rx.LastTotal = total;
            if (fin) rx.FinSeen = true;
            if (hold)
            {
                rx.Holds++;
                Interlocked.Increment(ref Holds);
                if (total == 0) Interlocked.Increment(ref FinOnlyHolds);
                else if (consumed == total) Interlocked.Increment(ref WholeHolds);
                rx.ResumeIssued = false;
                rx.Held = true;
                Resumes.Enqueue((rx, credit));
            }

            return pend ? ReceiveResult.PendingAfter(consumed) : ReceiveResult.Consumed(consumed);
        }

        public override void OnStreamPeerSendShutdown(TransportStreamId id)
        {
            if (!Streams.TryGetValue(id, out Rx? rx) || rx.ServerAborted) return;
            rx.Shutdowns++;
            if (rx.Index < 0)
            {
                Errors.Enqueue($"peer send shutdown of a stream that was never indicated ({id})");
                return;
            }

            Plan plan = plans[rx.Index];
            if (rx.Shutdowns > 1) Error(rx, "the peer's send shutdown was reported twice");
            if (!rx.FinSeen) Error(rx, "the peer's send shutdown was reported, the sink never saw the FIN");
            if (rx.Next != plan.Length) Error(rx, $"the peer's send shutdown was reported, the sink has {rx.Next} of {plan.Length} bytes");
            if (rx.Held && !rx.ResumeIssued) Error(rx, "the peer's send shutdown was reported while the stream was held and not resumed");
        }

        public override void OnStreamAborted(TransportStreamId id, ulong errorCode, StreamAbortDirection direction)
        {
            if (Streams.TryGetValue(id, out Rx? rx)) rx.Aborts++;
        }

        public override void OnStreamShutdownComplete(TransportStreamId id)
        {
            if (Streams.TryGetValue(id, out Rx? rx)) rx.Completes++;
            Interlocked.Increment(ref Completed);
            Transport?.CloseStream(id);
        }
    }

    private sealed class StressClientSink : NullTransportSink
    {
        public MsQuicTransport? Transport;
        public int Completed;
        public readonly ConcurrentQueue<string> Errors = new();

        public override void OnStreamStarted(TransportStreamId id, ulong context, TransportStatus status)
        {
            if (status != TransportStatus.Success)
            {
                Errors.Enqueue($"stream #{context} did not start: {status}");
                return;
            }

            long quicId = Transport!.GetQuicStreamId(id);
            if (quicId != ((long)context * 4) + 2) Errors.Enqueue($"stream #{context} has QUIC id {quicId}");
        }

        public override void OnStreamShutdownComplete(TransportStreamId id)
        {
            Interlocked.Increment(ref Completed);
            Transport?.CloseStream(id);
        }
    }

    private static Plan[] MakePlans(Random random, int count, int maxLength)
    {
        var plans = new Plan[count];
        for (int i = 0; i < count; i++)
        {
            int kind = random.Next(10);
            int length = kind < 3 ? random.Next(1, 65)
                : kind < 6 ? random.Next(1, 4097)
                : kind < 9 ? random.Next(1, Math.Min(maxLength, 100_000) + 1)
                : random.Next(1, maxLength + 1);
            if (i == 0) length = 1;
            if (i == 1) length = maxLength;
            int chunks = Math.Min(length, random.Next(1, 5));
            int[] ends = new int[chunks];
            for (int c = 0; c < chunks - 1; c++) ends[c] = random.Next(1, length);
            ends[chunks - 1] = length;
            Array.Sort(ends);
            var plan = new Plan { Index = i, Length = length, ChunkEnds = ends, FinAlone = random.Next(3) == 0 };
            int fate = random.Next(20);
            if (fate == 0 && i > 1) plan.AbortAfterChunks = random.Next(1, chunks + 1);
            else if (fate == 1 && i > 1) plan.ServerAbort = true;
            plan.Data = (byte*)NativeMemory.Alloc((nuint)length);
            for (int k = 0; k < length; k++) plan.Data[k] = PatternAt(k, i);
            plan.Segments = (TransportSegment*)NativeMemory.AllocZeroed((nuint)((chunks + 1) * sizeof(TransportSegment)));
            plans[i] = plan;
        }

        return plans;
    }

    private static void Free(Plan[] plans)
    {
        foreach (Plan plan in plans)
        {
            NativeMemory.Free(plan.Data);
            NativeMemory.Free(plan.Segments);
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(0)] // a fresh seed on every run
    public void Streams_held_and_resumed_at_random_arrive_exactly_once_and_close(int seed)
    {
        if (!MsQuicApi.TryGetInstance(out _, out _))
        {
            Assert.Skip("MsQuic is not available on this host.");
        }

        if (seed == 0) seed = Environment.TickCount & 0x7FFFFFFF;
        const int Count = 96;
        var random = new Random(seed);
        Plan[] plans = MakePlans(random, Count, 1024 * 1024);
        var harness = new MsQuicTransportHarness();
        var serverSink = new StressServerSink(plans, seed + 1);
        var clientSink = new StressClientSink();
        int stop = 0;
        var resumerErrors = new ConcurrentQueue<string>();
        string? failure = null;
        try
        {
            ConformancePair pair = harness.CreatePair(clientSink, serverSink, new ConformancePairOptions { ServerPeerUnidiStreams = Count });
            var client = (MsQuicTransport)pair.Client;
            var server = (MsQuicTransport)pair.Server;
            serverSink.Transport = server;
            clientSink.Transport = client;
            Assert.True(Spin.Until(() => client.State == TransportState.Connected && server.State == TransportState.Connected, TimeSpan.FromSeconds(10)));

            var resumers = new Thread[2];
            for (int t = 0; t < resumers.Length; t++)
            {
                var own = new Random(seed + 100 + t);
                resumers[t] = new Thread(() =>
                {
                    while (Volatile.Read(ref stop) == 0)
                    {
                        if (!serverSink.Resumes.TryDequeue(out (Rx Stream, int Credit) item))
                        {
                            Thread.Yield();
                            continue;
                        }

                        int delay = own.Next(10);
                        if (delay < 6)
                        {
                            int spins = own.Next(0, 60);
                            for (int i = 0; i < spins; i++) Thread.SpinWait(1);
                        }
                        else if (delay < 9)
                        {
                            Thread.Yield();
                        }
                        else
                        {
                            Thread.Sleep(1);
                        }

                        try
                        {
                            Rx rx = item.Stream;
                            if (rx.Index >= 0 && plans[rx.Index].ServerAbort)
                            {
                                rx.ServerAborted = true;
                                rx.ResumeIssued = true;
                                server.AbortStream(rx.Id, 99, StreamAbortDirection.Receive);
                                server.ResumeStreamReceive(rx.Id, item.Credit); // nothing is held any more: ignored
                            }
                            else
                            {
                                rx.ResumeIssued = true;
                                server.ResumeStreamReceive(rx.Id, item.Credit);
                            }
                        }
                        catch (Exception ex)
                        {
                            resumerErrors.Enqueue(ex.ToString());
                        }
                    }
                })
                {
                    IsBackground = true,
                    Name = "review-resumer-" + t,
                };
                resumers[t].Start();
            }

            // The sender: opens the streams in order (the QUIC id tells the receiver which one it is) and interleaves their chunks.
            var active = new List<Plan>();
            int opened = 0;
            var sendErrors = new List<string>();
            while (opened < Count || active.Count > 0)
            {
                bool open = opened < Count && active.Count < 12 && (active.Count == 0 || random.Next(3) == 0);
                Plan plan;
                if (open)
                {
                    plan = plans[opened++];
                    TransportStatus status = client.OpenStream(StreamKind.Unidirectional, (ulong)plan.Index, 32767, out plan.Id);
                    if (status != TransportStatus.Success)
                    {
                        sendErrors.Add($"OpenStream #{plan.Index}: {status}");
                        break;
                    }

                    active.Add(plan);
                }
                else
                {
                    plan = active[random.Next(active.Count)];
                }

                bool done = false;
                if (plan.NextChunk < plan.ChunkEnds.Length)
                {
                    int chunk = plan.NextChunk++;
                    int start = chunk == 0 ? 0 : plan.ChunkEnds[chunk - 1];
                    int end = plan.ChunkEnds[chunk];
                    bool last = chunk == plan.ChunkEnds.Length - 1;
                    bool abort = plan.AbortAfterChunks == chunk + 1;
                    TransportSendFlags flags = chunk == 0 ? TransportSendFlags.Start : TransportSendFlags.None;
                    if (last && !plan.FinAlone && !abort)
                    {
                        flags |= TransportSendFlags.Fin;
                        done = true;
                    }

                    plan.Segments[chunk] = new TransportSegment(plan.Data + start, end - start);
                    TransportStatus status = client.SendStream(plan.Id, plan.Segments + chunk, 1, (ulong)plan.Index, flags);
                    if (status != TransportStatus.Success)
                    {
                        if (!plan.ServerAbort) sendErrors.Add($"SendStream #{plan.Index} chunk {chunk}: {status}");
                        done = true;
                    }
                    else if (abort)
                    {
                        if (random.Next(2) == 0) Thread.Sleep(1);
                        client.AbortStream(plan.Id, 77, StreamAbortDirection.Send);
                        done = true;
                    }
                }
                else
                {
                    TransportStatus status = client.SendStream(plan.Id, null, 0, (ulong)plan.Index, TransportSendFlags.Fin);
                    if (status != TransportStatus.Success && !plan.ServerAbort) sendErrors.Add($"SendStream #{plan.Index} FIN: {status}");
                    done = true;
                }

                if (done) active.Remove(plan);
                int pause = random.Next(8);
                if (pause == 0) Thread.Sleep(1);
                else if (pause < 3) Thread.Yield();
            }

            bool finished = Spin.Until(
                () => Volatile.Read(ref serverSink.Completed) >= Count && Volatile.Read(ref clientSink.Completed) >= Count
                    || !serverSink.Errors.IsEmpty,
                TimeSpan.FromSeconds(60));
            Volatile.Write(ref stop, 1);
            foreach (Thread thread in resumers) thread.Join(TimeSpan.FromSeconds(10));

            var problems = new List<string>();
            problems.AddRange(sendErrors);
            problems.AddRange(serverSink.Errors);
            problems.AddRange(clientSink.Errors);
            problems.AddRange(resumerErrors);
            Rx[] all;
            lock (serverSink.All) all = [.. serverSink.All];
            var seen = new bool[Count];
            foreach (Rx rx in all)
            {
                if (rx.Index < 0) continue;
                seen[rx.Index] = true;
                Plan plan = plans[rx.Index];
                bool normal = plan.AbortAfterChunks < 0 && !rx.ServerAborted;
                if (rx.Completes != 1) problems.Add($"stream #{rx.Index} (length {plan.Length}, finAlone {plan.FinAlone}, clientAbort {plan.AbortAfterChunks}, serverAbort {rx.ServerAborted}): never closed; sink has {rx.Next} bytes, fin {rx.FinSeen}, held {rx.Held}, resumed {rx.ResumeIssued}, {rx.Indications} indications, {rx.Holds} holds, last indication {rx.LastTotal} bytes, shutdowns {rx.Shutdowns}, aborts {rx.Aborts}");
                else if (normal && (rx.Next != plan.Length || !rx.FinSeen || rx.Shutdowns != 1)) problems.Add($"stream #{rx.Index} (length {plan.Length}): closed with {rx.Next} bytes, fin {rx.FinSeen}, shutdowns {rx.Shutdowns}, aborts {rx.Aborts}");
            }

            for (int i = 0; i < Count; i++)
            {
                if (!seen[i] && plans[i].AbortAfterChunks < 0) problems.Add($"stream #{i} (length {plans[i].Length}) was never indicated");
            }

            if (!finished) problems.Insert(0, $"not every stream closed within 60 s: server {serverSink.Completed}/{Count}, client {clientSink.Completed}/{Count}");
            if (problems.Count > 0)
            {
                failure = $"seed {seed}; {serverSink.Indications} indications, {serverSink.Holds} holds ({serverSink.WholeHolds} whole, {serverSink.FinOnlyHolds} FIN-only):\n" + string.Join("\n", problems.Take(25));
            }
        }
        finally
        {
            Volatile.Write(ref stop, 1);
            harness.Dispose();
            Free(plans);
        }

        Log($"stress seed {seed}: {serverSink.Indications} indications, {serverSink.Holds} holds ({serverSink.WholeHolds} whole, {serverSink.FinOnlyHolds} FIN-only), {serverSink.MultiSegment} indications of several buffers, failure: {failure ?? "none"}");
        Assert.Null(failure);
        Assert.Null(harness.CleanupError);
        Assert.Equal(0, harness.SinkExceptionTotal);
        // The run exercised what it is meant to.
        Assert.True(serverSink.Holds > 50, $"only {serverSink.Holds} holds");
    }

    // ------------------------------------------------------------------ scripted cases

    private sealed class ScriptSink : NullTransportSink
    {
        public delegate ReceiveResult Handler(int call, TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong offset, bool fin, int total);

        public MsQuicTransport? Transport;
        public Handler? Script;
        public readonly List<(ulong Offset, int Total, bool Fin, byte[] Data)> Calls = [];
        public int Shutdowns;
        public int Completes;
        public TransportStreamId Stream;

        public int CallCount
        {
            get
            {
                lock (Calls) return Calls.Count;
            }
        }

        public (ulong Offset, int Total, bool Fin, byte[] Data) Call(int index)
        {
            lock (Calls) return Calls[index];
        }

        public override ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin)
        {
            int total = 0;
            var data = new MemoryStream();
            for (int i = 0; i < segments.Length; i++)
            {
                total += (int)segments[i].Length;
                data.Write(segments[i].AsSpan());
            }

            int call;
            lock (Calls)
            {
                Stream = id;
                Calls.Add((absoluteOffset, total, fin, data.ToArray()));
                call = Calls.Count;
            }

            return Script?.Invoke(call, id, segments, absoluteOffset, fin, total) ?? ReceiveResult.Consumed(total);
        }

        public override void OnStreamPeerSendShutdown(TransportStreamId id) => Interlocked.Increment(ref Shutdowns);

        public override void OnStreamShutdownComplete(TransportStreamId id)
        {
            Interlocked.Increment(ref Completes);
            Transport?.CloseStream(id);
        }
    }

    private sealed class Fixture : IDisposable
    {
        public readonly MsQuicTransportHarness Harness = new();
        public readonly ScriptSink Sink = new();
        public readonly MsQuicTransport Client;
        public readonly MsQuicTransport Server;
        private readonly List<nint> _memory = [];

        public Fixture(ushort streams = 16)
        {
            var clientSink = new StressClientSink();
            ConformancePair pair = Harness.CreatePair(clientSink, Sink, new ConformancePairOptions { ServerPeerUnidiStreams = streams });
            Client = (MsQuicTransport)pair.Client;
            Server = (MsQuicTransport)pair.Server;
            Sink.Transport = Server;
            clientSink.Transport = Client;
            MsQuicTransport client = Client;
            MsQuicTransport server = Server;
            Assert.True(Spin.Until(() => client.State == TransportState.Connected && server.State == TransportState.Connected, TimeSpan.FromSeconds(10)));
        }

        public TransportStreamId Open()
        {
            Assert.Equal(TransportStatus.Success, Client.OpenStream(StreamKind.Unidirectional, 0, 32767, out TransportStreamId id));
            return id;
        }

        /// <summary>Sends <paramref name="length"/> pattern bytes starting at stream offset <paramref name="offset"/> (seed 0).</summary>
        public void Send(TransportStreamId id, long offset, int length, TransportSendFlags flags)
        {
            byte* data = (byte*)NativeMemory.Alloc((nuint)Math.Max(1, length));
            for (int i = 0; i < length; i++) data[i] = PatternAt(offset + i, 0);
            var segment = (TransportSegment*)NativeMemory.Alloc((nuint)sizeof(TransportSegment));
            *segment = new TransportSegment(data, length);
            lock (_memory)
            {
                _memory.Add((nint)data);
                _memory.Add((nint)segment);
            }

            Assert.Equal(TransportStatus.Success, Client.SendStream(id, segment, 1, 1, flags));
        }

        public void SendFinAlone(TransportStreamId id) => Assert.Equal(TransportStatus.Success, Client.SendStream(id, null, 0, 2, TransportSendFlags.Fin));

        public bool Wait(Func<bool> condition, int seconds = 10) => Spin.Until(condition, TimeSpan.FromSeconds(seconds));

        public void Dispose()
        {
            Harness.Dispose();
            lock (_memory)
            {
                foreach (nint p in _memory) NativeMemory.Free((void*)p);
                _memory.Clear();
            }
        }
    }

    private static byte[] Pattern(long offset, int length)
    {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) bytes[i] = PatternAt(offset + i, 0);
        return bytes;
    }

    /// <summary>The "keeps one byte back" rule on the smallest indication there is: one byte and the FIN, consumed whole and held.</summary>
    [Fact]
    public void One_byte_and_the_fin_consumed_whole_and_held_ends_once_after_the_resume()
    {
        using var f = new Fixture();
        f.Sink.Script = (call, _, _, _, _, total) => call == 1 ? ReceiveResult.PendingAfter(total) : ReceiveResult.Consumed(total);
        TransportStreamId id = f.Open();
        f.Send(id, 0, 1, TransportSendFlags.Start | TransportSendFlags.Fin);
        Assert.True(f.Wait(() => f.Sink.CallCount == 1), "the indication");
        Thread.Sleep(150);
        Assert.Equal(1, f.Sink.CallCount);
        Assert.Equal(0, Volatile.Read(ref f.Sink.Shutdowns));
        Assert.True(f.Sink.Call(0).Fin);
        f.Server.ResumeStreamReceive(f.Sink.Stream, 0);
        Assert.True(f.Wait(() => Volatile.Read(ref f.Sink.Completes) == 1), "the stream's shutdown after the resume");
        Assert.Equal(1, Volatile.Read(ref f.Sink.Shutdowns));
        Assert.Equal(1, f.Sink.CallCount); // the FIN the sink saw is not shown again
    }

    /// <summary>
    /// A whole indication is held (one byte kept back), then the FIN arrives alone: MsQuic indicates the kept byte with the
    /// FIN, the sink must see an empty indication with the FIN at the end of the stream — and may hold that one too.
    /// </summary>
    [Fact]
    public void A_fin_that_arrives_alone_after_a_whole_hold_is_shown_empty_and_can_be_held_again()
    {
        using var f = new Fixture();
        f.Sink.Script = (call, _, _, _, _, total) => call <= 2 ? ReceiveResult.PendingAfter(total) : ReceiveResult.Consumed(total);
        TransportStreamId id = f.Open();
        f.Send(id, 0, 100, TransportSendFlags.Start);
        Assert.True(f.Wait(() => f.Sink.CallCount == 1), "the first indication");
        f.SendFinAlone(id);
        Thread.Sleep(150);
        Assert.Equal(1, f.Sink.CallCount);
        Assert.Equal(0, Volatile.Read(ref f.Sink.Shutdowns));
        f.Server.ResumeStreamReceive(f.Sink.Stream, 0);
        Assert.True(f.Wait(() => f.Sink.CallCount == 2), "the FIN's indication after the resume");
        (ulong offset, int total, bool fin, _) = f.Sink.Call(1);
        Assert.Equal(100UL, offset);
        Assert.Equal(0, total);
        Assert.True(fin);
        Thread.Sleep(150);
        Assert.Equal(0, Volatile.Read(ref f.Sink.Shutdowns)); // held: the end is not reported yet
        f.Server.ResumeStreamReceive(f.Sink.Stream, 0);
        Assert.True(f.Wait(() => Volatile.Read(ref f.Sink.Completes) == 1), "the stream's shutdown after the second resume");
        Assert.Equal(1, Volatile.Read(ref f.Sink.Shutdowns));
        Assert.Equal(2, f.Sink.CallCount);
    }

    /// <summary>Data that arrives while a whole indication is held is indicated after the resume, without the byte that was kept back.</summary>
    [Fact]
    public void Data_that_arrives_during_a_whole_hold_is_indicated_after_it_without_the_kept_byte()
    {
        using var f = new Fixture();
        f.Sink.Script = (call, _, _, _, _, total) => call == 1 ? ReceiveResult.PendingAfter(total) : ReceiveResult.Consumed(total);
        TransportStreamId id = f.Open();
        f.Send(id, 0, 100, TransportSendFlags.Start);
        Assert.True(f.Wait(() => f.Sink.CallCount == 1), "the first indication");
        f.Send(id, 100, 50, TransportSendFlags.Fin);
        Thread.Sleep(150);
        Assert.Equal(1, f.Sink.CallCount);
        f.Server.ResumeStreamReceive(f.Sink.Stream, 0);
        Assert.True(f.Wait(() => Volatile.Read(ref f.Sink.Completes) == 1), "the stream's shutdown");
        Assert.Equal(2, f.Sink.CallCount);
        (ulong offset, int total, bool fin, byte[] data) = f.Sink.Call(1);
        Assert.Equal(100UL, offset);
        Assert.Equal(50, total);
        Assert.True(fin);
        Assert.Equal(Pattern(100, 50), data);
        Assert.Equal(1, Volatile.Read(ref f.Sink.Shutdowns));
    }

    /// <summary>A partial hold whose resume credits part of the rest, while more data arrives: the next indication starts after the credit.</summary>
    [Fact]
    public void A_credit_is_skipped_at_the_head_of_an_indication_that_has_grown()
    {
        using var f = new Fixture();
        f.Sink.Script = (call, _, _, _, _, total) => call == 1 ? ReceiveResult.PendingAfter(10) : ReceiveResult.Consumed(total);
        TransportStreamId id = f.Open();
        f.Send(id, 0, 100, TransportSendFlags.Start);
        Assert.True(f.Wait(() => f.Sink.CallCount == 1), "the first indication");
        f.Send(id, 100, 5000, TransportSendFlags.Fin);
        Thread.Sleep(150);
        Assert.Throws<ArgumentOutOfRangeException>(() => f.Server.ResumeStreamReceive(f.Sink.Stream, 91));
        f.Server.ResumeStreamReceive(f.Sink.Stream, 90); // everything that was indicated
        Assert.True(f.Wait(() => Volatile.Read(ref f.Sink.Completes) == 1), "the stream's shutdown");
        Assert.Equal(2, f.Sink.CallCount);
        (ulong offset, int total, bool fin, byte[] data) = f.Sink.Call(1);
        Assert.Equal(100UL, offset);
        Assert.Equal(5000, total);
        Assert.True(fin);
        Assert.Equal(Pattern(100, 5000), data);
    }

    /// <summary>
    /// A resume that comes after the stream stopped being held for another reason — the peer reset it, the receiver aborted
    /// it, it was closed — is ignored, whatever it credits, and the stream still closes.
    /// </summary>
    [Theory]
    [InlineData(0)] // the peer resets the stream
    [InlineData(1)] // the receiver aborts its receive side
    [InlineData(2)] // the receiver closes the stream
    public void A_resume_after_the_hold_ended_another_way_is_ignored(int how)
    {
        using var f = new Fixture();
        f.Sink.Script = (_, _, _, _, _, _) => ReceiveResult.PendingAfter(10);
        TransportStreamId id = f.Open();
        f.Send(id, 0, 100, TransportSendFlags.Start);
        Assert.True(f.Wait(() => f.Sink.CallCount == 1), "the indication");
        TransportStreamId held = f.Sink.Stream;
        switch (how)
        {
            case 0:
                f.Client.AbortStream(id, 5, StreamAbortDirection.Send);
                Assert.True(f.Wait(() => Volatile.Read(ref f.Sink.Completes) == 1), "the reset stream's shutdown, while it was held");
                break;
            case 1:
                f.Server.AbortStream(held, 6, StreamAbortDirection.Receive);
                break;
            default:
                f.Server.CloseStream(held);
                break;
        }

        f.Server.ResumeStreamReceive(held, 90);
        f.Server.ResumeStreamReceive(held, 1_000_000);
        f.Server.ResumeStreamReceive(held, -1);
        if (how == 1) Assert.True(f.Wait(() => Volatile.Read(ref f.Sink.Completes) == 1), "the aborted stream's shutdown");
        MsQuicTransport server = f.Server;
        Assert.True(f.Wait(() => server.OpenStreamCount == 0), $"the stream's slot was not released ({server.OpenStreamCount} open)");
        Assert.Equal(1, f.Sink.CallCount);
        Assert.Equal(0, Volatile.Read(ref f.Sink.Shutdowns));

        // The slot is reused by the next stream: nothing of the old hold (skip count, held FIN) leaks into it.
        f.Sink.Script = null;
        TransportStreamId next = f.Open();
        f.Send(next, 0, 50, TransportSendFlags.Start | TransportSendFlags.Fin);
        Assert.True(f.Wait(() => Volatile.Read(ref f.Sink.Shutdowns) == 1), "the next stream's end");
        (ulong offset, int total, bool fin, byte[] data) = f.Sink.Call(1);
        Assert.Equal(0UL, offset);
        Assert.Equal(50, total);
        Assert.True(fin);
        Assert.Equal(Pattern(0, 50), data);
    }

    /// <summary>
    /// The one path that still answers QUIC_STATUS_PENDING: an indication that carries only the FIN. Many streams, each FIN
    /// held and resumed by a thread that races the callback's return.
    /// </summary>
    [Fact]
    public void A_held_fin_only_indication_is_never_lost_when_its_resume_races_the_callback()
    {
        const int Streams = 400;
        using var f = new Fixture(8);
        int signal = 0;
        int stop = 0;
        int finHolds = 0;
        TransportStreamId announced = default;
        var gate = new object();
        f.Sink.Script = (_, id, _, _, fin, total) =>
        {
            if (total == 0 && fin)
            {
                lock (gate) announced = id;
                Interlocked.Increment(ref finHolds);
                Volatile.Write(ref signal, 1);
                return ReceiveResult.PendingAfter(0);
            }

            return ReceiveResult.Consumed(total);
        };
        MsQuicTransport server = f.Server;
        var resumer = new Thread(() =>
        {
            var random = new Random(5);
            while (Volatile.Read(ref stop) == 0)
            {
                if (Interlocked.Exchange(ref signal, 0) == 0)
                {
                    Thread.Yield();
                    continue;
                }

                int spins = random.Next(0, 80);
                for (int i = 0; i < spins; i++) Thread.SpinWait(1);
                TransportStreamId id;
                lock (gate) id = announced;
                server.ResumeStreamReceive(id, 0);
            }
        })
        {
            IsBackground = true,
        };
        resumer.Start();
        try
        {
            for (int i = 0; i < Streams; i++)
            {
                int calls = f.Sink.CallCount;
                TransportStreamId id = f.Open();
                f.Send(id, 0, 10, TransportSendFlags.Start);
                Assert.True(f.Wait(() => f.Sink.CallCount > calls), $"stream {i}: its data");
                f.SendFinAlone(id);
                int target = i + 1;
                Assert.True(f.Wait(() => Volatile.Read(ref f.Sink.Completes) >= target, 5),
                    $"stream {i}: its FIN was held and resumed and the stream never ended ({Volatile.Read(ref finHolds)} FIN holds, {Volatile.Read(ref f.Sink.Shutdowns)} shutdowns)");
            }
        }
        finally
        {
            Volatile.Write(ref stop, 1);
            resumer.Join(TimeSpan.FromSeconds(5));
        }

        Assert.Equal(Streams, Volatile.Read(ref f.Sink.Shutdowns));
        Assert.True(Volatile.Read(ref finHolds) > Streams / 2, $"only {finHolds} FIN-only indications were seen");
    }

    /// <summary>A stream held (data, or the FIN alone, which leaves a receive pending in MsQuic) and never resumed does not keep the connection from closing.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void A_stream_that_is_held_and_never_resumed_does_not_block_the_close(bool finOnly)
    {
        var f = new Fixture();
        try
        {
            f.Sink.Script = (_, _, _, _, fin, total) => finOnly && total != 0 ? ReceiveResult.Consumed(total) : ReceiveResult.PendingAfter(total);
            TransportStreamId id = f.Open();
            f.Send(id, 0, 100, finOnly ? TransportSendFlags.Start : TransportSendFlags.Start | TransportSendFlags.Fin);
            Assert.True(f.Wait(() => f.Sink.CallCount == 1), "the first indication");
            if (finOnly)
            {
                f.SendFinAlone(id);
                Assert.True(f.Wait(() => f.Sink.CallCount == 2), "the FIN's indication");
            }

            Thread.Sleep(100);
            Assert.Equal(0, Volatile.Read(ref f.Sink.Shutdowns));
        }
        finally
        {
            f.Dispose();
        }

        Assert.Null(f.Harness.CleanupError);
        Assert.Equal(0, f.Harness.SinkExceptionTotal);
    }

    /// <summary>
    /// A stream far larger than the stream receive window (2 MiB) through a receiver that holds every indication: the
    /// window must keep opening (the byte kept back costs nothing) and the indications must not shrink to a dribble.
    /// </summary>
    [Theory]
    [InlineData(0)] // PendingAfter(all) every time: one byte kept back per indication
    [InlineData(1)] // PendingAfter(0) once, then the same indication consumed
    [InlineData(2)] // PendingAfter(half), the other half credited by the resume
    public void A_stream_larger_than_the_receive_window_flows_through_a_receiver_that_holds_every_indication(int mode)
    {
        const int Length = 12 * 1024 * 1024;
        long next = 0;
        long calls = 0;
        long smallCalls = 0;
        int errors = 0;
        int signal = 0;
        int credit = 0;
        int stop = 0;
        bool toggle = false;
        TransportStreamId held = default;
        var sink = new LargeSink();
        MsQuicTransport? server = null;

        sink.Handler = (id, segments, offset, fin) =>
        {
            int total = 0;
            for (int i = 0; i < segments.Length; i++) total += (int)segments[i].Length;
            calls++;
            if (total <= 1 && !fin) smallCalls++;
            if ((long)offset != next) errors++;
            int consumed;
            int resumeCredit = 0;
            bool hold = true;
            switch (mode)
            {
                case 0:
                    consumed = total;
                    break;
                case 1:
                    toggle = !toggle;
                    consumed = toggle ? 0 : total;
                    hold = toggle;
                    break;
                default:
                    consumed = total / 2;
                    resumeCredit = total - consumed;
                    break;
            }

            if (total == 0) hold = false;
            int take = consumed + resumeCredit;
            long position = (long)offset;
            int left = take;
            for (int i = 0; i < segments.Length && left > 0; i++)
            {
                ReadOnlySpan<byte> span = segments[i].AsSpan();
                int n = Math.Min(span.Length, left);
                for (int k = 0; k < n; k += 97)
                {
                    if (span[k] != PatternAt(position + k, 0)) errors++;
                }

                position += n;
                left -= n;
            }

            next = (long)offset + take;
            if (!hold) return ReceiveResult.Consumed(consumed);
            held = id;
            Volatile.Write(ref credit, resumeCredit);
            Volatile.Write(ref signal, 1);
            return ReceiveResult.PendingAfter(consumed);
        };
        var resumer = new Thread(() =>
        {
            while (Volatile.Read(ref stop) == 0)
            {
                if (Interlocked.Exchange(ref signal, 0) == 0)
                {
                    Thread.Yield();
                    continue;
                }

                server!.ResumeStreamReceive(held, Volatile.Read(ref credit));
            }
        })
        {
            IsBackground = true,
        };

        // A second pair whose server sink is the lean one.
        using var harness = new MsQuicTransportHarness();
        var clientSink = new StressClientSink();
        ConformancePair pair = harness.CreatePair(clientSink, sink, new ConformancePairOptions());
        var client = (MsQuicTransport)pair.Client;
        MsQuicTransport accepted = (MsQuicTransport)pair.Server;
        server = accepted;
        sink.Transport = accepted;
        clientSink.Transport = client;
        Assert.True(Spin.Until(() => client.State == TransportState.Connected && accepted.State == TransportState.Connected, TimeSpan.FromSeconds(10)));
        byte* data = (byte*)NativeMemory.Alloc(Length);
        var segment = (TransportSegment*)NativeMemory.Alloc((nuint)sizeof(TransportSegment));
        try
        {
            for (int i = 0; i < Length; i++) data[i] = PatternAt(i, 0);
            *segment = new TransportSegment(data, Length);
            resumer.Start();
            long started = Stopwatch.GetTimestamp();
            Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Unidirectional, 0, 32767, out TransportStreamId id));
            Assert.Equal(TransportStatus.Success, client.SendStream(id, segment, 1, 1, TransportSendFlags.Start | TransportSendFlags.Fin));
            bool ended = Spin.Until(() => Volatile.Read(ref sink.Completes) == 1, TimeSpan.FromSeconds(60));
            double seconds = Stopwatch.GetElapsedTime(started).TotalSeconds;
            Volatile.Write(ref stop, 1);
            resumer.Join(TimeSpan.FromSeconds(5));
            string summary = $"mode {mode}: {next} of {Length} bytes in {seconds:F2} s, {calls} indications ({(calls == 0 ? 0 : next / calls)} bytes each on average, {smallCalls} of one byte or less), {errors} errors";
            Assert.True(ended, "the stream stalled: " + summary);
            Assert.True(errors == 0, summary);
            Assert.True(next == Length, summary);
            Assert.Equal(1, Volatile.Read(ref sink.Shutdowns));
            // Not a dribble: the average indication is far larger than a packet's worth of a callback per byte.
            Assert.True(next / Math.Max(1, calls) >= 1024, "the stream dribbled: " + summary);
            Log(summary);
        }
        finally
        {
            Volatile.Write(ref stop, 1);
            harness.Dispose();
            NativeMemory.Free(data);
            NativeMemory.Free(segment);
        }

        Assert.Null(harness.CleanupError);
        Assert.Equal(0, harness.SinkExceptionTotal);
    }

    private sealed class LargeSink : NullTransportSink
    {
        public delegate ReceiveResult LargeHandler(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong offset, bool fin);

        public MsQuicTransport? Transport;
        public LargeHandler? Handler;
        public int Shutdowns;
        public int Completes;

        public override ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin) => Handler!(id, segments, absoluteOffset, fin);

        public override void OnStreamPeerSendShutdown(TransportStreamId id) => Interlocked.Increment(ref Shutdowns);

        public override void OnStreamShutdownComplete(TransportStreamId id)
        {
            Interlocked.Increment(ref Completes);
            Transport?.CloseStream(id);
        }
    }
}
