using System.Diagnostics;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Conformance;
using Tedd.Quicly.Transport.MsQuic.Tests.Transport;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Review;

/// <summary>
/// Second adversarial review of fix/group-stream-receive-limit. The fix replaces "reset the stream" by "hold the stream and
/// resume it from Poll", so its promise — nothing the sender completed as Delivered is lost — now rests on every resume of a
/// held stream taking effect. Over MsQuic it does not always: <c>MsQuicTransport.ResumeStreamReceive(id, 0)</c> from the game
/// thread calls <c>StreamReceiveComplete(0)</c> and <c>StreamReceiveSetEnabled(true)</c>. When that lands after the transport
/// published <c>ReceivePending</c> but before its receive callback has returned to MsQuic, MsQuic 2.5 only adds the length to
/// the stream's completion counter ("no need to queue a completion operation when there is an active receive"), and when the
/// callback then returns PENDING it completes the receive only if that counter is not zero. A completion of zero bytes is
/// therefore dropped; the following set-enabled finds a read still pending and queues no flush. The stream is never
/// indicated again: its messages, which the sender completed as Delivered when the FIN was acknowledged, never arrive, no
/// counter moves, and the mid-message idle rule does not watch it. A stream that is held a second time is always resumed
/// with zero bytes (the indication starts at the message that was un-read).
/// </summary>
[Collection(MsQuicCollection.Name)]
public unsafe class ReviewGroup2LatestMsQuicResumeTests
{
    private sealed class HoldingSink : NullTransportSink
    {
        public MsQuicTransport? Transport;
        public TransportStreamId Stream;
        public int Signal;
        public long Pends;
        public long Deadline;
        public volatile bool Done;

        public override ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin)
        {
            int total = 0;
            foreach (TransportSegment segment in segments)
            {
                total += (int)segment.Length;
            }

            if (Stopwatch.GetTimestamp() < Deadline)
            {
                // Held back, exactly as the session holds a group stream whose message finds the receive ring full; the game
                // thread is told (the session's PendedStreams ring) and resumes it from its next Poll.
                Stream = id;
                Interlocked.Increment(ref Pends);
                Volatile.Write(ref Signal, 1);
                return ReceiveResult.PendingAfter(0);
            }

            Done |= fin;
            return ReceiveResult.Consumed(total);
        }

        public override void OnStreamShutdownComplete(TransportStreamId id) => Transport?.CloseStream(id);
    }

    [Fact]
    public void A_Held_Stream_Is_Always_Indicated_Again_After_It_Is_Resumed_With_Zero_Bytes()
    {
        if (!MsQuicApi.TryGetInstance(out _, out _))
        {
            Assert.Skip("MsQuic is not available on this host.");
        }

        using MsQuicTransportHarness harness = new();
        HoldingSink serverSink = new();
        ConformancePair pair = harness.CreatePair(new NullTransportSink(), serverSink, new ConformancePairOptions { ServerPeerUnidiStreams = 4 });
        MsQuicTransport client = (MsQuicTransport)pair.Client;
        MsQuicTransport server = (MsQuicTransport)pair.Server;
        serverSink.Transport = server;
        Assert.True(Spin.Until(() => client.State == TransportState.Connected && server.State == TransportState.Connected, TimeSpan.FromSeconds(10)));
        serverSink.Deadline = Stopwatch.GetTimestamp() + (20 * Stopwatch.Frequency);

        using NativeBuffer buffer = new(36);
        using NativeSegments segments = new(1);
        segments.Set(0, buffer.Segment(0, 36));
        Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Unidirectional, 1, 32767, out TransportStreamId id));
        Assert.Equal(TransportStatus.Success, client.SendStream(id, segments.At(0), 1, 1, TransportSendFlags.Start | TransportSendFlags.Fin));

        // The "game thread": resumes the stream as soon as it is told, a varying moment after the callback signalled it.
        long stalledAt = -1;
        Random random = new(11);
        long lastPends = 0;
        long lastProgress = Stopwatch.GetTimestamp();
        while (!serverSink.Done)
        {
            if (Interlocked.Exchange(ref serverSink.Signal, 0) == 1)
            {
                int spins = random.Next(0, 40);
                for (int i = 0; i < spins; i++)
                {
                    Thread.SpinWait(1);
                }

                server.ResumeStreamReceive(serverSink.Stream, 0);
            }

            long pends = Interlocked.Read(ref serverSink.Pends);
            long now = Stopwatch.GetTimestamp();
            if (pends != lastPends)
            {
                lastPends = pends;
                lastProgress = now;
            }
            else if (now - lastProgress > 3 * Stopwatch.Frequency)
            {
                stalledAt = pends;
                break;
            }
        }

        Assert.True(stalledAt < 0,
            $"the stream was held and resumed {stalledAt} times, then never indicated again: three seconds without a receive callback, "
            + $"its FIN still undelivered, {server.OpenStreamCount} stream open at the receiver");
    }
}
