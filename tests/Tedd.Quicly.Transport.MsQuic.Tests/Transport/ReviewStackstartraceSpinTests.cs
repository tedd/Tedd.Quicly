using System.Diagnostics;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Conformance;
using Tedd.Quicly.Testing.Simulation;
using Tedd.Quicly.Transport.MsQuic.Tests.Transport;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Transport;

/// <summary>
/// Adversarial review of 2ddf1ed, lens: start-race. SendStream(Start) on an unstarted stream now spins (yielding, never
/// sleeping) on the calling thread — the game thread — for up to one second when StreamSend fails with INVALID_STATE
/// before START_COMPLETE was indicated. MsQuic 2.5 indicates START_COMPLETE before SHUTDOWN_ON_FAIL clears SendEnabled
/// (QuicStreamStart: QuicStreamIndicateStartComplete, then QuicStreamShutdown), so the refusal race itself never reaches
/// that wait. The one way found to reach it is a shutdown of the same stream from another thread (ITransport allows
/// AbortStream/CloseStream from any thread) that lands after StartCore set StartRequested. These GUARDS show that the
/// wait then ends as soon as the worker indicates START_COMPLETE, which it still does: measured on this box, 6 000 swept
/// races per case gave a few dozen InvalidState answers and a worst SendStream of under 1 ms (20 000 races: 0.35 ms and
/// 0.69 ms), never the one-second bound.
/// </summary>
[Collection(MsQuicCollection.Name)]
public unsafe class ReviewStackstartraceSpinTests
{
    public enum Racer
    {
        AbortSend,
        Close,
    }

    /// <summary>
    /// GUARD: a SendStream(Start) racing an AbortStream(Send) or CloseStream of the same stream from another thread does not
    /// block its caller for long (the bound is 100 ms; the one-second wait of the new INVALID_STATE path would fail it). The
    /// race window is a few instructions wide (between StartCore's StartRequested = true and its StreamStart), so the other
    /// thread's call is swept across it with a growing spin delay.
    /// </summary>
    [Theory]
    [InlineData(Racer.AbortSend)]
    [InlineData(Racer.Close)]
    public void SendStream_Start_Racing_A_Shutdown_From_Another_Thread_Never_Blocks_The_Caller(Racer racer)
    {
        if (!MsQuicApi.TryGetInstance(out _, out _))
        {
            Assert.Skip("MsQuic is not available on this host.");
        }

        using MsQuicTransportHarness harness = new();
        TimeSpan timeout = harness.DefaultTimeout;
        RecordingSink clientSink = new() { AutoCloseStreams = true };
        RecordingSink serverSink = new();
        ConformancePair pair = harness.CreatePair(clientSink, serverSink, new ConformancePairOptions { ServerPeerUnidiStreams = 1000 });
        MsQuicTransport client = (MsQuicTransport)pair.Client;
        clientSink.Transport = pair.Client;
        serverSink.Transport = pair.Server;
        Assert.True(clientSink.WaitForCount(RecordedEventKind.Connected, 1, timeout), "the client never connected");

        TransportStreamId shared = TransportStreamId.None;
        long go = 0;
        long done = 0;
        int delay = 0;
        bool stop = false;
        Thread other = new(() =>
        {
            long seen = 0;
            while (!Volatile.Read(ref stop))
            {
                long now = Volatile.Read(ref go);
                if (now == seen)
                {
                    Thread.SpinWait(1);
                    continue;
                }

                seen = now;
                Thread.SpinWait(Volatile.Read(ref delay));
                TransportStreamId id = shared;
                if (racer == Racer.AbortSend)
                {
                    client.AbortStream(id, 7, StreamAbortDirection.Send);
                }
                else
                {
                    client.CloseStream(id);
                }

                Volatile.Write(ref done, now);
            }
        }) { IsBackground = true, Name = "racer" };
        other.Start();

        byte[] data = [1, 2, 3, 4, 5, 6, 7, 8];
        long worstTicks = 0;
        TransportStatus worstStatus = TransportStatus.Success;
        int slow = 0;
        Dictionary<TransportStatus, int> answers = [];
        Stopwatch total = Stopwatch.StartNew();
        try
        {
            for (int i = 1; i <= 6_000 && slow == 0 && total.Elapsed < TimeSpan.FromSeconds(60); i++)
            {
                TransportStreamId id;
                TransportStatus opened;
                Stopwatch openWait = Stopwatch.StartNew();
                while ((opened = client.OpenStream(StreamKind.Unidirectional, (ulong)i, 32767, out id)) == TransportStatus.OutOfMemory && openWait.ElapsedMilliseconds < 5_000)
                {
                    Thread.Yield();
                }

                Assert.Equal(TransportStatus.Success, opened);
                shared = id;
                Volatile.Write(ref delay, i % 160);
                TransportStatus sent;
                long started;
                fixed (byte* pointer = data)
                {
                    TransportSegment segment = new(pointer, data.Length);
                    Volatile.Write(ref go, i);
                    started = Stopwatch.GetTimestamp();
                    sent = client.SendStream(id, &segment, 1, (ulong)i, TransportSendFlags.Start | TransportSendFlags.Fin);
                }

                long ticks = Stopwatch.GetTimestamp() - started;
                answers[sent] = answers.GetValueOrDefault(sent) + 1;
                if (ticks > worstTicks)
                {
                    worstTicks = ticks;
                    worstStatus = sent;
                }

                if (ticks > Stopwatch.Frequency / 10)
                {
                    slow++;
                }

                SpinWait wait = default;
                while (Volatile.Read(ref done) != i)
                {
                    wait.SpinOnce();
                }

                client.CloseStream(id);
            }
        }
        finally
        {
            Volatile.Write(ref stop, true);
            other.Join();
        }

        double worstMs = worstTicks * 1000.0 / Stopwatch.Frequency;
        string summary = string.Join(", ", answers.Select(a => $"{a.Key} {a.Value}"));
        Assert.True(slow == 0, $"SendStream(Start) blocked its caller for {worstMs:F1} ms (answer {worstStatus}) while another thread's {racer} of the same stream raced it; answers: {summary}");
    }

    /// <summary>
    /// Probe of the mechanism, made deterministic with reflection (no seam exists for it): the shutdown of the stream is
    /// processed by the worker before the start that SendStream queues. This is the state the race above produces when the
    /// other thread's call lands between StartCore's <c>StartRequested = true</c> and its StreamStart.
    /// </summary>
    [Theory]
    [InlineData(Racer.AbortSend)]
    [InlineData(Racer.Close)]
    public void SendStream_Start_On_A_Stream_MsQuic_Shut_Down_Before_The_Start_Answers_At_Once(Racer racer)
    {
        if (!MsQuicApi.TryGetInstance(out _, out _))
        {
            Assert.Skip("MsQuic is not available on this host.");
        }

        using MsQuicTransportHarness harness = new();
        TimeSpan timeout = harness.DefaultTimeout;
        RecordingSink clientSink = new() { AutoCloseStreams = false };
        RecordingSink serverSink = new();
        ConformancePair pair = harness.CreatePair(clientSink, serverSink, new ConformancePairOptions { ServerPeerUnidiStreams = 16 });
        MsQuicTransport client = (MsQuicTransport)pair.Client;
        clientSink.Transport = pair.Client;
        serverSink.Transport = pair.Server;
        Assert.True(clientSink.WaitForCount(RecordedEventKind.Connected, 1, timeout), "the client never connected");

        Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Unidirectional, 0x41, 32767, out TransportStreamId id));
        object slot = Slot(client, id);
        System.Reflection.FieldInfo startRequested = slot.GetType().GetField("StartRequested")!;

        // The other thread's call as it lands inside the window: StartRequested is already set, the start not yet queued.
        startRequested.SetValue(slot, true);
        if (racer == Racer.AbortSend)
        {
            client.AbortStream(id, 7, StreamAbortDirection.Send);
        }
        else
        {
            client.CloseStream(id);
        }

        Thread.Sleep(200); // the worker runs the shutdown
        startRequested.SetValue(slot, false); // back to the calling thread, which is just about to call StreamStart

        byte[] data = [1, 2, 3, 4, 5, 6, 7, 8];
        TransportStatus sent;
        Stopwatch watch = Stopwatch.StartNew();
        fixed (byte* pointer = data)
        {
            TransportSegment segment = new(pointer, data.Length);
            sent = racer == Racer.AbortSend
                ? client.SendStream(id, &segment, 1, 0x51, TransportSendFlags.Start | TransportSendFlags.Fin)
                : SendOnClosed(client, id, slot, &segment);
        }

        watch.Stop();
        Assert.True(watch.ElapsedMilliseconds < 100, $"SendStream(Start) spun {watch.ElapsedMilliseconds} ms on the calling thread and answered {sent}; START_COMPLETE of a start queued behind the stream's shutdown is never indicated");
    }

    /// <summary>CloseStream marks the slot AppClosed, so Enter refuses it; the racing SendStream had entered before that.</summary>
    private static TransportStatus SendOnClosed(MsQuicTransport client, TransportStreamId id, object slot, TransportSegment* segment)
    {
        System.Reflection.FieldInfo flags = slot.GetType().GetField("CloseFlags")!;
        int saved = (int)flags.GetValue(slot)!;
        flags.SetValue(slot, saved & ~1); // the SendStream call entered before the other thread's CloseStream
        try
        {
            return client.SendStream(id, segment, 1, 0x51, TransportSendFlags.Start | TransportSendFlags.Fin);
        }
        finally
        {
            flags.SetValue(slot, (int)flags.GetValue(slot)! | saved);
        }
    }

    private static object Slot(MsQuicTransport transport, TransportStreamId id)
    {
        var slots = (Array)typeof(MsQuicTransport).GetField("_slots", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!.GetValue(transport)!;
        return slots.GetValue(id.Slot)!;
    }
}
