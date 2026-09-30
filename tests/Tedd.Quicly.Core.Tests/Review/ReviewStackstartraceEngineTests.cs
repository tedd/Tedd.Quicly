using System.Net;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Tests.Session;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Review;

/// <summary>
/// Adversarial review of the StreamLimitReached answer of <c>SendStream(Start)</c> (2ddf1ed, 8d98702), lens: start-race.
/// The contract (ITransport.SendStream, ITransportSink.OnStreamStarted) now allows a "combined" refusal: the send that
/// carries the start returns StreamLimitReached although OnStreamStarted(StreamLimitReached) — and possibly
/// OnStreamShutdownComplete — were delivered for the same stream. RequireRefusedSequence accepts exactly two shapes:
/// <list type="bullet">
/// <item>A: [StreamStarted StreamLimitReached, StreamShutdownComplete] (what the MsQuic seam DelaySendUntilStartReported
/// always produces);</item>
/// <item>B: [StreamStarted StreamLimitReached] alone. MsQuic produces it when the send finds the stream shut down
/// (SendEnabled cleared by SHUTDOWN_ON_FAIL, which MsQuic runs right after it indicates START_COMPLETE) before the worker
/// indicates SHUTDOWN_COMPLETE: the caller's CloseStream then lands first, and CloseStream suppresses the shutdown
/// callback (MsQuicTransport.ShutdownComplete returns for an AppClosed slot).</item>
/// </list>
/// The simulator never produces either shape, so no engine test covered them before this file.
/// <see cref="CombinedRefusalTransport"/> produces them over the simulator.
/// </summary>
public class ReviewStackstartraceEngineTests
{
    /// <summary>One ReliableLatest channel that may hold one large-value stream at a time (so its tx record table has 2 entries).</summary>
    private static readonly ChannelTable LatestOne = ChannelTable.Create()
        .Add(2, "state", ChannelMode.ReliableLatest, o => o.MaxGroups = 1)
        .Build();

    /// <summary>An ordered channel and a group channel.</summary>
    private static readonly ChannelTable Streams = ChannelTable.Create()
        .Add(11, "group", ChannelMode.ReliableUnordered)
        .Add(12, "ordered", ChannelMode.ReliableOrdered)
        .Build();

    private static SessionHarness LatestHarness(ChannelTable table, Func<ITransportConnector, ITransportConnector> connector) =>
        new(link: new LinkOptions { DelayMicros = 2_000 }, table: table,
            client: o =>
            {
                LatestKit.Quiet(o);
                LatestKit.Roomy(o);
            },
            server: o =>
            {
                LatestKit.Quiet(o);
                LatestKit.Roomy(o);
            },
            connector: connector);

    /// <summary>
    /// FINDING (major). Shape B leaks a record of ReliableLatestEngine._txStreams per refusal: OnStreamStarted records every
    /// stream it hears of (the refused one too, "until its shutdown arrives"), and only OnStreamClosed frees a record — but
    /// no shutdown arrives for a stream the engine closed first. The table has MaxGroups + 1 records per channel, so after
    /// two such refusals on a MaxGroups-1 channel the next stream that really carries a value is not recorded, its shutdown
    /// posts no StreamClosed, its per-channel slot (_openStreams) is never given back, and every later large value of the
    /// channel waits for ever although the peer has credit.
    /// </summary>
    [Fact]
    public void Latest_Combined_Refusals_Without_A_Shutdown_Callback_Do_Not_Stall_The_Channels_Large_Values() =>
        LatestAfterStartedOnlyRefusals(2);

    /// <summary>
    /// GUARD (pins the mechanism of the finding above): one such refusal is absorbed by the table's spare record, so the
    /// channel still works — the stall needs one refusal more than the spare records of the channel.
    /// </summary>
    [Fact]
    public void Latest_One_Combined_Refusal_Without_A_Shutdown_Callback_Is_Absorbed_By_The_Spare_Record() =>
        LatestAfterStartedOnlyRefusals(1);

    private static void LatestAfterStartedOnlyRefusals(int refusals)
    {
        CombinedRefusalConnector connector = null!;
        using SessionHarness h = LatestHarness(LatestOne, inner => connector = new CombinedRefusalConnector(inner));
        h.Run(50_000);
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(2, LatestKit.Collect(received));
        CombinedRefusalTransport transport = connector.Transport!;
        transport.Shape = CombinedShape.StartedOnly;
        transport.RefuseStarts = refusals;

        SendResult first = h.Client.SendCopy(new SendHeader(2, 1), LatestKit.Payload(1, 8_000), SendOptions.Tracked);
        Assert.Equal(SendStatus.Admitted, first.Status);
        for (int round = 0; round < 3 && h.Client.GetDeliveryStatus(first.Token) != DeliveryStatus.Delivered; round++)
        {
            h.Run(30_000);
            transport.GrantCredit();
        }

        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(first.Token) == DeliveryStatus.Delivered, 10_000_000),
            $"the first value: status {h.Client.GetDeliveryStatus(first.Token)}");
        Assert.Equal(refusals, transport.Refused);
        h.Run(200_000); // the first value's stream shuts down at both ends

        // The peer has credit and the first stream is gone: a value of another key goes out.
        SendResult second = h.Client.SendCopy(new SendHeader(2, 2), LatestKit.Payload(2, 8_000), SendOptions.Tracked);
        Assert.Equal(SendStatus.Admitted, second.Status);
        transport.GrantCredit();
        Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(second.Token) == DeliveryStatus.Delivered, 10_000_000),
            $"the second large value was never sent after {refusals} combined refusal(s) (status {h.Client.GetDeliveryStatus(second.Token)}, {transport.StartsSent} starts sent): the channel's stream slot was never given back");
    }

    /// <summary>GUARD: the same sequence with shape A (the refusal's shutdown is delivered too) keeps the channel going.</summary>
    [Fact]
    public void Latest_Combined_Refusals_With_A_Shutdown_Callback_Keep_The_Channel_Going()
    {
        CombinedRefusalConnector connector = null!;
        using SessionHarness h = LatestHarness(LatestOne, inner => connector = new CombinedRefusalConnector(inner));
        h.Run(50_000);
        List<(ulong Key, uint Version, byte[] Payload, ReceiveFlags Flags)> received = [];
        h.Server!.RegisterHandler(2, LatestKit.Collect(received));
        CombinedRefusalTransport transport = connector.Transport!;
        transport.Shape = CombinedShape.StartedAndShutdown;
        transport.RefuseStarts = 4;

        for (int key = 1; key <= 3; key++)
        {
            SendResult result = h.Client.SendCopy(new SendHeader(2, (ulong)key), LatestKit.Payload(key, 8_000), SendOptions.Tracked);
            Assert.Equal(SendStatus.Admitted, result.Status);
            for (int round = 0; round < 4 && h.Client.GetDeliveryStatus(result.Token) != DeliveryStatus.Delivered; round++)
            {
                h.Run(30_000);
                transport.GrantCredit();
            }

            Assert.True(h.RunUntil(() => h.Client.GetDeliveryStatus(result.Token) == DeliveryStatus.Delivered, 10_000_000),
                $"key {key}: status {h.Client.GetDeliveryStatus(result.Token)}");
            h.Run(200_000);
        }

        Assert.Equal(4, transport.Refused);
        Assert.Equal(0, DatagramKit.Statistics(h.Client).CallbackFaults);
    }

    /// <summary>
    /// GUARD: the ordered and group engines take a synchronous StreamLimitReached as final in both combined shapes: every
    /// message arrives once credit returns, none is failed, nothing is reset or faulted, and the notices of the abandoned
    /// stream (same serial, phase no longer Starting) change nothing.
    /// </summary>
    [Theory]
    [InlineData(CombinedShape.StartedOnly)]
    [InlineData(CombinedShape.StartedAndShutdown)]
    public void Ordered_And_Group_Treat_A_Combined_Refusal_As_Final(CombinedShape shape)
    {
        CombinedRefusalConnector connector = null!;
        using SessionHarness h = new(link: new LinkOptions { DelayMicros = 2_000 }, table: Streams,
            client: o =>
            {
                QuietOptions.Apply(o);
                o.GroupMinInterval = TimeSpan.Zero;
            },
            server: QuietOptions.Apply,
            connector: inner => connector = new CombinedRefusalConnector(inner));
        h.Run(50_000);
        List<int> ordered = [];
        List<int> group = [];
        h.Server!.RegisterHandler(11, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => group.Add(BitConverter.ToInt32(payload)));
        h.Server.RegisterHandler(12, (QuiclyPeer _, in ReceiveHeader _, ReadOnlySpan<byte> payload) => ordered.Add(BitConverter.ToInt32(payload)));
        CombinedRefusalTransport transport = connector.Transport!;
        transport.Shape = shape;
        transport.RefuseStarts = 6;

        List<SendToken> tokens = [];
        for (int i = 0; i < 40; i++)
        {
            tokens.Add(h.Client.SendCopy(new SendHeader(11), BitConverter.GetBytes(i), SendOptions.Tracked).Token);
            tokens.Add(h.Client.SendCopy(new SendHeader(12), BitConverter.GetBytes(i), SendOptions.Tracked).Token);
            h.Run(5_000);
            if (i % 5 == 4)
            {
                transport.GrantCredit();
            }
        }

        for (int round = 0; round < 20 && transport.RefuseStarts > 0; round++)
        {
            transport.GrantCredit();
            h.Run(10_000);
        }

        Assert.True(h.RunUntil(() => ordered.Count == 40 && group.Count == 40, 10_000_000), $"ordered {ordered.Count}, group {group.Count} of 40");
        Assert.Equal(Enumerable.Range(0, 40), ordered);
        Assert.Equal(Enumerable.Range(0, 40), group.Order());
        Assert.True(h.RunUntil(() => tokens.All(t => h.Client.GetDeliveryStatus(t) == DeliveryStatus.Delivered), 10_000_000),
            string.Join(",", tokens.Select(t => h.Client.GetDeliveryStatus(t)).Distinct()));
        Assert.True(transport.Refused >= 2, $"only {transport.Refused} refusals: the test did not reach its path");
        Assert.Equal(0, DatagramKit.Statistics(h.Client).CallbackFaults);
        Assert.Equal(0, DatagramKit.Statistics(h.Server).StreamsReset);
    }
}

/// <summary>The bulk engine under the combined refusal (GUARD).</summary>
public class ReviewStackstartraceBulkTests
{
    /// <summary>
    /// GUARD: a bulk transfer whose stream start loses the race in either combined shape waits for credit (it does not
    /// retry on every pass), then completes byte-exact on a new stream; the abandoned stream's notices change nothing.
    /// </summary>
    [Theory]
    [InlineData(CombinedShape.StartedOnly)]
    [InlineData(CombinedShape.StartedAndShutdown)]
    public async Task Bulk_Treats_A_Combined_Refusal_As_Final(CombinedShape shape)
    {
        byte[] payload = new byte[60_000];
        new Random(5).NextBytes(payload);
        AcceptRouter router = AcceptRouter.Memory(payload.Length);
        CombinedRefusalConnector? connector = null;
        using SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 1_000 },
            table: BulkTables.Main,
            client: BulkKit.Quiet,
            server: BulkKit.Receiver(router),
            connector: inner => connector = new CombinedRefusalConnector(inner));

        CombinedRefusalTransport transport = connector!.Transport!;
        transport.Shape = shape;
        transport.RefuseStarts = 1;
        BulkTransfer transfer = await h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, payload.Length), new MemorySource(payload));
        h.Run(50_000);

        // Refused once and parked: no further start while no credit arrives.
        Assert.Equal(1, transport.Refused);
        Assert.Equal(1, transport.StartsSent);
        Assert.Equal(BulkStatus.Running, transfer.Status);
        Assert.Equal(0, transfer.BytesTransferred);

        transport.GrantCredit();
        Assert.True(h.RunUntil(() => transfer.IsFinished, 30_000_000), "the transfer did not go out after credit returned");
        Assert.Equal(BulkStatus.Completed, transfer.Status);
        Assert.Equal(payload, router.Sink<MemorySink>().Bytes);
        Assert.Equal(2, transport.StartsSent);
        Assert.Equal(0, DatagramKit.Statistics(h.Client).CallbackFaults);
    }
}

/// <summary>Which callbacks a combined refusal delivers before the call returns StreamLimitReached.</summary>
public enum CombinedShape
{
    /// <summary>OnStreamStarted(StreamLimitReached) only: the caller's CloseStream comes before the shutdown and suppresses it.</summary>
    StartedOnly,

    /// <summary>OnStreamStarted(StreamLimitReached), then OnStreamShutdownComplete (the MsQuic seam's shape).</summary>
    StartedAndShutdown,
}

/// <summary>
/// Refuses the start of engine streams the way the MsQuic transport answers a start that loses the race: the refusal
/// callbacks for the stream are delivered, then the call that carried the start returns StreamLimitReached (both shapes
/// RequireRefusedSequence accepts). The simulator's transport thread is the test thread, so "delivered before the call
/// returns" is a nested call. <see cref="GrantCredit"/> raises OnStreamsAvailable.
/// </summary>
internal sealed unsafe class CombinedRefusalTransport(ITransport inner, ITransportSink sink) : ITransport
{
    private readonly Dictionary<TransportStreamId, ulong> _openContexts = [];

    public CombinedShape Shape { get; set; }

    public int RefuseStarts { get; set; }

    public int Refused { get; private set; }

    public int StartsSent { get; private set; }

    public TransportCapabilities Capabilities => inner.Capabilities;

    public TransportState State => inner.State;

    public void GrantCredit() => sink.OnStreamsAvailable(0, 1);

    public TransportStatus SendDatagram(TransportSegment* segments, int count, ulong context, TransportSendFlags flags) =>
        inner.SendDatagram(segments, count, context, flags);

    public TransportStatus OpenStream(StreamKind kind, ulong context, ushort priority, out TransportStreamId id)
    {
        TransportStatus status = inner.OpenStream(kind, context, priority, out id);
        if (status == TransportStatus.Success)
        {
            _openContexts[id] = context;
        }

        return status;
    }

    public TransportStatus StartStream(TransportStreamId id) => inner.StartStream(id);

    public TransportStatus SendStream(TransportStreamId id, TransportSegment* segments, int count, ulong context, TransportSendFlags flags)
    {
        if ((flags & TransportSendFlags.Start) != 0 && _openContexts.TryGetValue(id, out ulong open)
            && PeerCore.TryDecodeEngineStreamContext(open, out _, out _, out _))
        {
            StartsSent++;
            if (RefuseStarts > 0)
            {
                RefuseStarts--;
                Refused++;
                sink.OnStreamStarted(id, open, TransportStatus.StreamLimitReached);
                if (Shape == CombinedShape.StartedAndShutdown)
                {
                    sink.OnStreamShutdownComplete(id);
                }

                return TransportStatus.StreamLimitReached;
            }
        }

        return inner.SendStream(id, segments, count, context, flags);
    }

    public void AbortStream(TransportStreamId id, ulong errorCode, StreamAbortDirection direction) => inner.AbortStream(id, errorCode, direction);

    public void SetStreamPriority(TransportStreamId id, ushort priority) => inner.SetStreamPriority(id, priority);

    public long GetQuicStreamId(TransportStreamId id) => inner.GetQuicStreamId(id);

    public void ResumeStreamReceive(TransportStreamId id, int bytesConsumed) => inner.ResumeStreamReceive(id, bytesConsumed);

    public void CloseStream(TransportStreamId id) => inner.CloseStream(id);

    public void UpdatePeerStreamLimits(ushort bidirectional, ushort unidirectional) => inner.UpdatePeerStreamLimits(bidirectional, unidirectional);

    public void Close(ulong errorCode, ReadOnlySpan<byte> reason) => inner.Close(errorCode, reason);

    public void GetStatistics(out TransportStatistics statistics) => inner.GetStatistics(out statistics);

    public void Dispose() => inner.Dispose();
}

/// <summary>Wraps the transport of a connector in <see cref="CombinedRefusalTransport"/>.</summary>
internal sealed class CombinedRefusalConnector(ITransportConnector inner) : ITransportConnector
{
    public CombinedRefusalTransport? Transport { get; private set; }

    public ITransport Connect(EndPoint endpoint, string? serverName, ITransportSink sink) =>
        Transport = new CombinedRefusalTransport(inner.Connect(endpoint, serverName, sink), sink);
}
