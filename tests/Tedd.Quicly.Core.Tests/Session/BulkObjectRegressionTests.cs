using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Primitives;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>Regression probes of the bulk object driver, each pinning a defect an adversarial review found.</summary>
public class BulkObjectRegressionTests
{
    private static ChannelTable SmallRanges { get; } = ChannelTable.Create()
        .Add(2, "moves", ChannelMode.UnreliableUnordered)
        .Add(5, "objects", ChannelMode.Bulk, o =>
        {
            o.Priority = 0;
            o.MaxMessageSize = 64 * 1024;
            o.MaxGroups = 4;
        })
        .Build();

    /// <summary>One range of <paramref name="payload"/> as a hand-built bulk stream.</summary>
    private static byte[] Range(ulong transferId, byte[] payload, int offset, int length, ulong objectId = 11)
    {
        BulkHeader header = new()
        {
            TransferId = transferId,
            ObjectId = objectId,
            ObjectVersion = 1,
            TotalLength = (ulong)payload.Length,
            Offset = (ulong)offset,
            Length = (ulong)length,
            Flags = BulkFlags.ChecksumPresent,
        };

        ReadOnlySpan<byte> body = payload.AsSpan(offset, length);
        return BulkKit.BulkStream(5, in header, body);
    }

    /// <summary>
    /// The object's ranges arrive out of order: the tail first, then the head. Both are legal transfers of one object
    /// and the receiver's own docs say ranges of one object may interleave.
    /// </summary>
    [Fact]
    public void A_Tail_Range_Arriving_Before_The_Head_Still_Assembles_The_Object()
    {
        byte[] payload = new byte[4096];
        new Random(7).NextBytes(payload);
        AcceptObjectRouter router = AcceptObjectRouter.Memory(payload.Length);
        using ServerHarness h = new(
            link: new LinkOptions { DelayMicros = 1_000 },
            table: BulkTables.Main,
            server: BulkKit.ObjectReceiver(router));
        Assert.True(h.Admit(), "the raw client was not admitted");

        // The second half first.
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(Range(1, payload, 2048, 2048), out _, fin: true));
        Assert.True(h.RunUntil(() => router.Accepted.Count == 1), "the object never began");

        MemoryObjectSink sink = router.Sink<MemoryObjectSink>();
        h.Run(200_000);

        // The object must not be complete: half its bytes are still missing.
        Assert.False(sink.IsFinished, $"the object finished on {sink.BytesWritten} of {payload.Length} bytes: {sink.Result?.Status}");

        // Then the first half.
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(Range(2, payload, 0, 2048), out _, fin: true));
        Assert.True(h.RunUntil(() => sink.IsFinished), "the object never finished");
        Assert.Equal(BulkStatus.Completed, sink.Result!.Value.Status);
        Assert.Equal(payload, sink.Bytes);
        Assert.Single(router.Accepted);
    }

    /// <summary>The same thing, with the shipped sender across a reordering link instead of a hand-built peer.</summary>
    [Theory]
    [InlineData(0)]
    [InlineData(40)]
    public void A_Reordering_Link_Does_Not_Truncate_An_Object(double reorderPercent)
    {
        List<string> outcomes = [];
        for (int seed = 1; seed <= 10; seed++)
        {
            byte[] payload = new byte[640 * 1024];
            new Random(seed).NextBytes(payload);
            AcceptObjectRouter router = AcceptObjectRouter.Memory(payload.Length);
            using SessionHarness h = new(
                link: new LinkOptions { DelayMicros = 2_000, JitterMicros = 1_500, ReorderPercent = reorderPercent },
                table: SmallRanges,
                client: BulkKit.Quiet,
                server: BulkKit.ObjectReceiver(router),
                seed: seed);

            BulkObjectDescriptor descriptor = new(5, 7, 1, payload.Length);
            BulkObjectTransfer transfer = h.Client.BeginBulkObjectSend(in descriptor, new MemorySource(payload));
            h.RunUntil(() => transfer.IsFinished && router.Sinks.Count > 0 && router.Sink<MemoryObjectSink>().IsFinished, maxMicros: 60_000_000);

            if (router.Sinks.Count == 0)
            {
                outcomes.Add($"seed {seed}: never began");
                continue;
            }

            MemoryObjectSink sink = router.Sink<MemoryObjectSink>();
            BulkObjectResult? result = sink.Result;
            outcomes.Add(
                $"seed {seed}: objects={router.Accepted.Count} sender={transfer.Status}/{transfer.Result.Code} "
                + $"receiver={(result is null ? "unfinished" : $"{result.Value.Status}/{result.Value.Code} {result.Value.BytesTransferred}/{result.Value.Length}")} "
                + $"bytes-match={(result is { Status: BulkStatus.Completed } && sink.Bytes.AsSpan().SequenceEqual(payload))}");
        }

        Assert.All(outcomes, o => Assert.EndsWith($"Completed/NoError {ReorderBytes}/{ReorderBytes} bytes-match=True", o));
    }

    private const int ReorderBytes = 640 * 1024;

    /// <summary>The Length - 1 progress holdback on a one-byte range: Length - 1 is 0, which is also "nothing accepted".</summary>
    [Fact]
    public void A_One_Byte_Checksummed_Range_Completes()
    {
        byte[] payload = [0x5A];
        AcceptObjectRouter router = AcceptObjectRouter.Memory(1);
        using ServerHarness h = new(
            link: new LinkOptions { DelayMicros = 1_000 },
            table: BulkTables.Main,
            server: BulkKit.ObjectReceiver(router));
        Assert.True(h.Admit(), "the raw client was not admitted");

        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(Range(1, payload, 0, 1), out _, fin: true));
        Assert.True(h.RunUntil(() => router.Sinks.Count > 0 && router.Sink<MemoryObjectSink>().IsFinished), "the one-byte object never finished");
        Assert.Equal(BulkStatus.Completed, router.Sink<MemoryObjectSink>().Result!.Value.Status);
        Assert.Equal(payload, router.Sink<MemoryObjectSink>().Bytes);
        Assert.Equal(PeerState.Connected, h.Server!.State);
    }

    /// <summary>A stream that ends before its promised trailer is truncated (PROTOCOL.md §8), not a completed range.</summary>
    [Fact]
    public void A_Range_Whose_Trailer_Is_Missing_Is_Truncated()
    {
        byte[] payload = new byte[256];
        new Random(23).NextBytes(payload);
        AcceptObjectRouter router = AcceptObjectRouter.Memory(payload.Length);
        using ServerHarness h = new(
            link: new LinkOptions { DelayMicros = 1_000 },
            table: BulkTables.Main,
            server: BulkKit.ObjectReceiver(router));
        Assert.True(h.Admit(), "the raw client was not admitted");

        byte[] full = Range(1, payload, 0, payload.Length);

        // Everything but the 8-byte trailer, then FIN.
        byte[] short1 = full.AsSpan(0, full.Length - StreamFraming.BulkChecksumLength).ToArray();
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(short1, out _, fin: true));
        Assert.True(h.RunUntil(() => router.Sinks.Count > 0 && router.Sink<MemoryObjectSink>().IsFinished), "the truncated range never ended");
        Assert.NotEqual(BulkStatus.Completed, router.Sink<MemoryObjectSink>().Result!.Value.Status);

        // Half a trailer, then FIN.
        byte[] short2 = full.AsSpan(0, full.Length - 4).ToArray();
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(short2, out _, fin: true));
        h.Run(200_000);
        Assert.Equal(PeerState.Connected, h.Server!.State);

        // A trailer followed by a stray byte.
        byte[] tooLong = [.. full, 0x00];
        Assert.Equal(TransportStatus.Success, h.Raw.OpenUni(tooLong, out _, fin: true));
        h.Run(200_000);
        Assert.Equal(PeerState.Connected, h.Server!.State);
    }
}
