using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// The bulk streaming path allocates nothing in its steady state (ADR 0008): reading from the source into a pooled block,
/// the stream send and its completion, the progressive write into the application's sink, and the <c>BulkProgress</c> the
/// receiver owes. The window measures passes in the <em>middle</em> of one transfer, because starting a transfer allocates
/// its <see cref="BulkTransfer"/> once — per object, not per byte.
/// </summary>
public class BulkZeroAllocationTests
{
    private const int Mib = 1024 * 1024;

    [Fact]
    public async Task Streaming_A_Bulk_Object_Does_Not_Allocate()
    {
        using SessionHarness h = Harness(compress: false, out CountingSink sink, out BulkTransfer transfer);
        Pump(h, 40);
        Assert.Equal(BulkStatus.Running, transfer.Status);

        WindowedAllocation.AssertNone(() => Pump(h, 10));

        Assert.Equal(BulkStatus.Running, transfer.Status);
        Assert.True(sink.BytesWritten > 0, "nothing was streamed during the windows");
        await Task.CompletedTask;
    }

    [Fact]
    public async Task Streaming_A_Chunked_Compressed_Object_Does_Not_Allocate()
    {
        // The chunked path stages each compressed chunk in a pooled block and decodes it into a second one, both returned
        // to the pool at the chunk's end, so it allocates nothing either.
        using SessionHarness h = Harness(compress: true, out CountingSink sink, out BulkTransfer transfer);
        Pump(h, 40);
        Assert.Equal(BulkStatus.Running, transfer.Status);

        WindowedAllocation.AssertNone(() => Pump(h, 10));

        Assert.Equal(BulkStatus.Running, transfer.Status);
        Assert.True(sink.BytesWritten > 0, "nothing was streamed during the windows");
        await Task.CompletedTask;
    }

    /// <summary>
    /// A transfer of 16 MiB (the largest one range may carry) with a 64 KiB send window, so each pass hands over one piece
    /// and the object outlives the warm-up and every measured window.
    /// </summary>
    private static SessionHarness Harness(bool compress, out CountingSink sink, out BulkTransfer transfer)
    {
        CountingSink target = new();
        AcceptRouter router = new(_ => target);
        SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 1_000 },
            table: BulkTables.Main,
            client: o =>
            {
                BulkKit.Quiet(o);
                o.BulkSendWindowBytes = 64 * 1024;
            },
            server: BulkKit.Receiver(router));

        transfer = h.Client.BeginBulkSendAsync(new BulkDescriptor(5, 1, 1, 16 * Mib, 0, 0, compress), new PatternSource(16 * Mib))
            .AsTask().GetAwaiter().GetResult();
        sink = target;
        return h;
    }

    private static void Pump(SessionHarness h, int passes)
    {
        for (int pass = 0; pass < passes; pass++)
        {
            h.Client.Flush();
            h.Network.Advance(0);
            h.Server!.Poll();
            h.Server!.Flush();
            h.Network.Advance(1_000);
            h.Client.Poll();
        }
    }
}
