using System.Collections.Concurrent;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Conformance;
using Tedd.Quicly.Transport.MsQuic.Tests.Transport;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Review;

/// <summary>
/// Verification of the held-stream teardown race (the release gate's one intermittent failure): a stream the receiver's
/// sink holds — MsQuic has it paused, receive disabled, with data the sink has not taken — is reset by its sender while
/// the sender's close is certainly not acknowledged, so a RESET_STREAM is really sent. The sink must be told of the abort
/// and of the end of the stream, exactly once each, and the stream must give its slot back, with no resume from anyone.
/// </summary>
[Collection(MsQuicCollection.Name)]
public unsafe class ReviewRaceVerifyTests
{
    private sealed class Track
    {
        public int Indications;
        public int Aborted;
        public ulong AbortCode;
        public StreamAbortDirection AbortDirection;
        public int PeerShutdowns;
        public int ShutdownCompletes;
        public bool AbortBeforeShutdown;
    }

    /// <summary>Holds every indication and never resumes; how much of the indication it takes is the variant under test.</summary>
    private sealed class HoldForeverSink(int mode) : NullTransportSink
    {
        public readonly ConcurrentDictionary<TransportStreamId, Track> Tracks = new();
        public readonly Random Random = new(17);
        public MsQuicTransport? Transport;

        public override ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin)
        {
            int total = 0;
            foreach (TransportSegment segment in segments) total += (int)segment.Length;
            Track track = Tracks.GetOrAdd(id, static _ => new Track());
            lock (track) track.Indications++;

            // Worker thread only. 0: nothing taken; 1: all taken (the transport keeps one byte back); 2: a part.
            int kept = mode == 0 ? 0 : mode == 1 ? total : Random.Next(1, Math.Max(2, total));
            return ReceiveResult.PendingAfter(Math.Min(kept, total));
        }

        public override void OnStreamAborted(TransportStreamId id, ulong errorCode, StreamAbortDirection direction)
        {
            Track track = Tracks.GetOrAdd(id, static _ => new Track());
            lock (track)
            {
                track.Aborted++;
                track.AbortCode = errorCode;
                track.AbortDirection = direction;
                if (track.ShutdownCompletes == 0) track.AbortBeforeShutdown = true;
            }
        }

        public override void OnStreamPeerSendShutdown(TransportStreamId id)
        {
            Track track = Tracks.GetOrAdd(id, static _ => new Track());
            lock (track) track.PeerShutdowns++;
        }

        public override void OnStreamShutdownComplete(TransportStreamId id)
        {
            Track track = Tracks.GetOrAdd(id, static _ => new Track());
            lock (track) track.ShutdownCompletes++;
            Transport?.CloseStream(id);
        }
    }

    private sealed class SenderSink : NullTransportSink
    {
        public readonly ConcurrentDictionary<TransportStreamId, bool> SendCanceled = new();

        public override void OnStreamSendCompleted(TransportStreamId id, ulong context, bool canceled) => SendCanceled[id] = canceled;
    }

    /// <param name="mode">What the sink takes of the indication it holds: 0 nothing, 1 everything, 2 a part.</param>
    /// <param name="length">Bytes per stream.</param>
    /// <param name="fin">
    /// Whether the streams are sent with a FIN. With one the payload must be more than twice the stream's receive window, so
    /// that the held receiver keeps the sender from ever sending its FIN, and the abort finds the close unacknowledged.
    /// </param>
    private static void Run(int mode, int length, bool fin)
    {
        if (!MsQuicApi.TryGetInstance(out _, out _))
        {
            Assert.Skip("MsQuic is not available on this host.");
        }

        const int Streams = 4;
        const int Rounds = 50;
        MsQuicTransportHarness harness = new();
        List<string> faults = [];
        List<IDisposable> native = [];
        try
        {
            Random random = new(1234 + mode);
            NativeBuffer buffer = new(length);
            buffer.Fill(3);
            native.Add(buffer);
            for (int round = 0; round < Rounds; round++)
            {
                HoldForeverSink sink = new(mode);
                SenderSink sender = new();
                ConformancePair pair = harness.CreatePair(sender, sink, new ConformancePairOptions { ServerPeerUnidiStreams = Streams });
                MsQuicTransport client = (MsQuicTransport)pair.Client;
                MsQuicTransport server = (MsQuicTransport)pair.Server;
                sink.Transport = server;
                Assert.True(Spin.Until(() => client.State == TransportState.Connected && server.State == TransportState.Connected, TimeSpan.FromSeconds(10)));
                NativeSegments segments = new(Streams);
                native.Add(segments);
                TransportStreamId[] clientIds = new TransportStreamId[Streams];
                for (int i = 0; i < Streams; i++)
                {
                    segments.Set(i, buffer.Segment(0, length));
                    Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Unidirectional, (ulong)i, 32767, out clientIds[i]));
                    Assert.Equal(TransportStatus.Success, client.SendStream(clientIds[i], segments.At(i), 1, (ulong)i, fin ? TransportSendFlags.Start | TransportSendFlags.Fin : TransportSendFlags.Start));
                }

                Assert.True(Spin.Until(() => sink.Tracks.Count == Streams, TimeSpan.FromSeconds(10)), $"round {round}: only {sink.Tracks.Count} of {Streams} streams were indicated");
                Assert.Equal(Streams, server.OpenStreamCount);
                int wait = random.Next(0, 6);
                if (wait != 0) Thread.Sleep(wait);

                // Held, and nobody resumes. The sender resets.
                for (int i = 0; i < Streams; i++) client.AbortStream(clientIds[i], 9, StreamAbortDirection.Send);

                bool ended = Spin.Until(() => server.OpenStreamCount == 0, TimeSpan.FromSeconds(10));
                if (!ended) faults.Add($"round {round}: {server.OpenStreamCount} held streams still open at the receiver ten seconds after the sender reset them");
                foreach (KeyValuePair<TransportStreamId, Track> entry in sink.Tracks)
                {
                    Track track = entry.Value;
                    lock (track)
                    {
                        if (track.Indications != 1 || track.Aborted != 1 || track.ShutdownCompletes != 1 || track.PeerShutdowns != 0
                            || !track.AbortBeforeShutdown || track.AbortCode != 9 || track.AbortDirection != StreamAbortDirection.Send)
                        {
                            faults.Add($"round {round}, slot {entry.Key.Slot}: {track.Indications} indications, {track.Aborted} aborts (code {track.AbortCode}, {track.AbortDirection}, "
                                + $"before the shutdown: {track.AbortBeforeShutdown}), {track.PeerShutdowns} peer send shutdowns, {track.ShutdownCompletes} shutdown completions");
                        }
                    }
                }

                if (fin)
                {
                    // The proof that the abort was a real reset: the send never completed gracefully.
                    for (int i = 0; i < Streams; i++)
                    {
                        int index = i;
                        if (!Spin.Until(() => sender.SendCanceled.ContainsKey(clientIds[index]), TimeSpan.FromSeconds(10)))
                        {
                            faults.Add($"round {round}, stream {i}: the sender's send never completed after its abort");
                        }
                        else if (!sender.SendCanceled[clientIds[i]])
                        {
                            faults.Add($"round {round}, stream {i}: the sender's send completed acknowledged, not canceled, so this round did not test a reset");
                        }
                    }
                }

                client.Dispose();
                server.Dispose();
                segments.Dispose();
                native.Remove(segments);
            }
        }
        finally
        {
            harness.Dispose();
            foreach (IDisposable item in native) item.Dispose();
        }

        Assert.True(faults.Count == 0, string.Join("\n", faults.Take(10)));
        Assert.Null(harness.CleanupError);
        Assert.Equal(0, harness.SinkExceptionTotal);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public void A_Held_Unfinished_Stream_The_Sender_Resets_Is_Aborted_And_Ends_Without_A_Resume(int mode) => Run(mode, 5000, fin: false);

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public void A_Held_Stream_Whose_Fin_Is_Behind_Flow_Control_Is_Aborted_And_Ends_Without_A_Resume_When_The_Sender_Resets(int mode) => Run(mode, FinBehindFlowControl, fin: true);

    /// <summary>
    /// More than twice the receive window of a peer unidirectional stream (2 MiB by default): the sink takes at most the
    /// first indication, which is less than one window, so the sender can never send more than two windows, and its FIN
    /// stays unsent.
    /// </summary>
    private const int FinBehindFlowControl = 5 << 20;
}
