using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Conformance;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Testing.Tests.Review;

/// <summary>
/// Adversarial review of 2ddf1ed, lens: start-race — do the two refused-start scenarios accept exactly the shapes the
/// contract allows? ITransport.SendStream allows one combined case for a send that carries the start: the call returns
/// StreamLimitReached and OnStreamStarted(StreamLimitReached), then OnStreamShutdownComplete, were delivered for the
/// stream. ITransport.StartStream allows no combined case: "the call returns StreamLimitReached and nothing follows for the
/// stream". <see cref="LyingHarness"/> wraps the simulator's client transport in one that answers starts of unidirectional
/// streams with shapes outside that contract; a scenario that passes against it accepts a transport that breaks the
/// contract.
/// </summary>
public class ReviewStackstartraceConformanceTests
{
    /// <summary>
    /// FINDING (minor): StartRefusedInsideACallbackIsReportedOnce accepts a synchronous StreamLimitReached from
    /// SendStream(Start) that is followed by OnStreamShutdownComplete alone — no OnStreamStarted. No shape of the contract
    /// has a shutdown without the start's report (RefusedStreamNeverStartsAndIsRetriedOnANewStream rejects it: it accepts
    /// only [StreamStarted StreamLimitReached] and [StreamStarted StreamLimitReached, StreamShutdownComplete]). The check
    /// <c>started &lt;= 1 &amp;&amp; shutdowns &lt;= 1 &amp;&amp; streamEvents == started + shutdowns</c> lets started = 0,
    /// shutdowns = 1 through (and does not check the order either).
    /// </summary>
    [Fact]
    public void StartRefusedInsideACallback_Rejects_A_Shutdown_Without_The_Starts_Report()
    {
        using var harness = new LyingHarness(new SimulatedTransportHarness()) { SendShape = Lie.ShutdownOnly };
        Exception? thrown = Record.Exception(() => TransportConformance.StartRefusedInsideACallbackIsReportedOnce(harness));
        Assert.True(harness.Lies > 0, "the lie was never told");
        Assert.IsType<ConformanceException>(thrown);
    }

    /// <summary>GUARD: the other scenario does reject that shape.</summary>
    [Fact]
    public void RefusedStreamNeverStarts_Rejects_A_Shutdown_Without_The_Starts_Report()
    {
        using var harness = new LyingHarness(new SimulatedTransportHarness()) { SendShape = Lie.ShutdownOnly };
        ConformanceException thrown = Assert.Throws<ConformanceException>(() => TransportConformance.RefusedStreamNeverStartsAndIsRetriedOnANewStream(harness));
        Assert.Contains("was reported again", thrown.Message);
    }

    /// <summary>GUARD: the allowed combined case of SendStream(Start) passes both scenarios.</summary>
    [Theory]
    [InlineData(Lie.StartedOnly)]
    [InlineData(Lie.StartedAndShutdown)]
    public void The_Allowed_Combined_Case_Of_A_Send_Passes(Lie shape)
    {
        using (var harness = new LyingHarness(new SimulatedTransportHarness()) { SendShape = shape })
        {
            TransportConformance.RefusedStreamNeverStartsAndIsRetriedOnANewStream(harness);
            Assert.True(harness.Lies > 0, "the lie was never told");
        }

        using (var harness = new LyingHarness(new SimulatedTransportHarness()) { SendShape = shape })
        {
            TransportConformance.StartRefusedInsideACallbackIsReportedOnce(harness);
            Assert.True(harness.Lies > 0, "the lie was never told");
        }
    }

    /// <summary>
    /// FINDING (doc): both scenarios accept the combined case from StartStream as well, which ITransport.StartStream forbade
    /// ("the call returns StreamLimitReached and nothing follows for the stream"; "MsQuic and the simulator always refuse
    /// asynchronously"). The WebTransport carrier does produce it: its StartStream is a SendStream(Start) of the preamble on
    /// the inner MsQuic transport (QueuePreamble), so it inherits the race. Either the StartStream contract names the
    /// combined case or the scenarios must reject it; the two disagreed.
    /// <para>
    /// As fixed: the contract names it (ITransport.StartStream: a transport whose StartStream is a send carrying the start
    /// has the combined case of SendStream), so the scenario and the contract agree that it is accepted — and both still
    /// reject a shutdown without the start's report from StartStream.
    /// </para>
    /// </summary>
    [Theory]
    [InlineData(Lie.StartedOnly)]
    [InlineData(Lie.StartedAndShutdown)]
    public void RefusedStreamNeverStarts_Accepts_The_Combined_Case_From_StartStream_That_ITransport_Documents(Lie shape)
    {
        using var harness = new LyingHarness(new SimulatedTransportHarness()) { StartShape = shape };
        TransportConformance.RefusedStreamNeverStartsAndIsRetriedOnANewStream(harness);
        Assert.True(harness.Lies > 0, "the lie was never told");
    }

    /// <summary>GUARD, the other half of the fix of the finding above: a shutdown without the start's report from StartStream is rejected.</summary>
    [Fact]
    public void RefusedStreamNeverStarts_Rejects_A_Shutdown_Without_The_Starts_Report_From_StartStream()
    {
        using var harness = new LyingHarness(new SimulatedTransportHarness()) { StartShape = Lie.ShutdownOnly };
        Exception? thrown = Record.Exception(() => TransportConformance.RefusedStreamNeverStartsAndIsRetriedOnANewStream(harness));
        Assert.True(harness.Lies > 0, "the lie was never told");
        Assert.IsType<ConformanceException>(thrown);
    }

    public enum Lie
    {
        /// <summary>Behave like the simulator (asynchronous refusal).</summary>
        None,

        /// <summary>Return StreamLimitReached after OnStreamStarted(StreamLimitReached).</summary>
        StartedOnly,

        /// <summary>Return StreamLimitReached after OnStreamStarted(StreamLimitReached) and OnStreamShutdownComplete.</summary>
        StartedAndShutdown,

        /// <summary>Return StreamLimitReached after OnStreamShutdownComplete only.</summary>
        ShutdownOnly,
    }

    /// <summary>A harness whose client transport answers starts of unidirectional streams with <see cref="SendShape"/> / <see cref="StartShape"/> while the peer grants none.</summary>
    private sealed class LyingHarness(SimulatedTransportHarness inner) : ITransportTestHarness
    {
        public Lie SendShape { get; init; }

        public Lie StartShape { get; init; }

        /// <summary>Starts the client transport answered with a lie (a test that expects a rejection checks the lie was told).</summary>
        public int Lies { get; set; }

        public string Name => "LyingSimulatedTransport";

        public TimeSpan DefaultTimeout => inner.DefaultTimeout;

        public ConformancePair CreatePair(ITransportSink clientSink, ITransportSink serverSink, ConformancePairOptions? options = null)
        {
            // The relay sees every client callback first, so the lie knows when the peer granted unidirectional credit.
            var relay = new RecordingSink(null, clientSink) { AutoCloseStreams = false };
            ConformancePair pair = inner.CreatePair(relay, serverSink, options);
            bool refuse = options?.ServerPeerUnidiStreams == 0;
            var client = new LyingTransport(this, pair.Client, clientSink, relay, refuse ? SendShape : Lie.None, refuse ? StartShape : Lie.None);
            return new ConformancePair(client, pair.Server);
        }

        public ITransport Connect(ITransportSink clientSink, PreHandshakeCallback preHandshake, AcceptCallback accept, ConformancePairOptions? options = null) =>
            inner.Connect(clientSink, preHandshake, accept, options);

        public bool Pump(Func<bool> condition, TimeSpan timeout) => inner.Pump(condition, timeout);

        public void Dispose() => inner.Dispose();
    }

    private sealed unsafe class LyingTransport(LyingHarness owner, ITransport inner, ITransportSink sink, RecordingSink relay, Lie sendShape, Lie startShape) : ITransport
    {
        private readonly Dictionary<TransportStreamId, ulong> _uni = [];
        private readonly HashSet<TransportStreamId> _refused = [];

        private bool _creditRaised => relay.OfKind(RecordedEventKind.StreamsAvailable).Any(e => e.Unidirectional > 0);

        public TransportCapabilities Capabilities => inner.Capabilities;

        public TransportState State => inner.State;

        public TransportStatus SendDatagram(TransportSegment* segments, int count, ulong context, TransportSendFlags flags) => inner.SendDatagram(segments, count, context, flags);

        public TransportStatus OpenStream(StreamKind kind, ulong context, ushort priority, out TransportStreamId id)
        {
            TransportStatus status = inner.OpenStream(kind, context, priority, out id);
            if (status == TransportStatus.Success && kind == StreamKind.Unidirectional)
            {
                _uni[id] = context;
            }

            return status;
        }

        public TransportStatus StartStream(TransportStreamId id)
        {
            if (_refused.Contains(id))
            {
                return TransportStatus.InvalidState;
            }

            return startShape != Lie.None && !_creditRaised && _uni.TryGetValue(id, out ulong open) ? Refuse(id, open, startShape) : inner.StartStream(id);
        }

        public TransportStatus SendStream(TransportStreamId id, TransportSegment* segments, int count, ulong context, TransportSendFlags flags)
        {
            if (_refused.Contains(id))
            {
                return TransportStatus.InvalidState;
            }

            if ((flags & TransportSendFlags.Start) != 0 && sendShape != Lie.None && !_creditRaised && _uni.TryGetValue(id, out ulong open) && inner.GetQuicStreamId(id) < 0)
            {
                return Refuse(id, open, sendShape);
            }

            return inner.SendStream(id, segments, count, context, flags);
        }

        private TransportStatus Refuse(TransportStreamId id, ulong open, Lie shape)
        {
            _refused.Add(id);
            owner.Lies++;
            if (shape != Lie.ShutdownOnly)
            {
                sink.OnStreamStarted(id, open, TransportStatus.StreamLimitReached);
            }

            if (shape is Lie.StartedAndShutdown or Lie.ShutdownOnly)
            {
                sink.OnStreamShutdownComplete(id);
            }

            return TransportStatus.StreamLimitReached;
        }

        public void AbortStream(TransportStreamId id, ulong errorCode, StreamAbortDirection direction) => inner.AbortStream(id, errorCode, direction);

        public void SetStreamPriority(TransportStreamId id, ushort priority) => inner.SetStreamPriority(id, priority);

        public long GetQuicStreamId(TransportStreamId id) => inner.GetQuicStreamId(id);

        public void ResumeStreamReceive(TransportStreamId id, int bytesConsumed) => inner.ResumeStreamReceive(id, bytesConsumed);

        public void CloseStream(TransportStreamId id) => inner.CloseStream(id);

        public void UpdatePeerStreamLimits(ushort bidirectional, ushort unidirectional)
        {
            inner.UpdatePeerStreamLimits(bidirectional, unidirectional);
        }

        public void Close(ulong errorCode, ReadOnlySpan<byte> reason) => inner.Close(errorCode, reason);

        public void GetStatistics(out TransportStatistics statistics) => inner.GetStatistics(out statistics);

        public void Dispose() => inner.Dispose();
    }
}
