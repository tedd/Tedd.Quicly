using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// The object driver adds nothing to the bulk streaming path's steady state (ADR 0008): the pump folds and issues over
/// preallocated slots, the receiving side routes each range's bytes through a preallocated shim, and the whole-object
/// hash reads the bytes where they already are. The window measures passes in the <em>middle</em> of a range, because
/// crossing into the next one registers a transfer with the engine and that allocates its <see cref="BulkTransfer"/> —
/// once per range, never per byte.
/// </summary>
public class BulkObjectZeroAllocationTests
{
    private const int Mib = 1024 * 1024;

    [Fact]
    public async Task Streaming_A_Bulk_Object_Through_The_Driver_Does_Not_Allocate()
    {
        using SessionHarness h = Harness(checksum: false, out PatternObjectSink sink, out BulkObjectTransfer transfer);
        Pump(h, 40);
        Assert.Equal(BulkStatus.Running, transfer.Status);

        WindowedAllocation.AssertNone(() => Pump(h, 10));

        Assert.Equal(BulkStatus.Running, transfer.Status);
        Assert.True(sink.BytesWritten > 0, "nothing was streamed during the windows");
        Assert.Equal(0, sink.Mismatches);
        await Task.CompletedTask;
    }

    [Fact]
    public async Task Hashing_A_Multi_Range_Object_As_It_Arrives_Does_Not_Allocate()
    {
        // The hash is the driver's own work on the receive side, running on every byte that arrives: it must cost no
        // allocation either, whether a byte continues the hash or waits in the reorder shadow.
        using SessionHarness h = Harness(checksum: true, out PatternObjectSink sink, out BulkObjectTransfer transfer);
        Pump(h, 40);
        Assert.Equal(BulkStatus.Running, transfer.Status);

        WindowedAllocation.AssertNone(() => Pump(h, 10));

        Assert.Equal(BulkStatus.Running, transfer.Status);
        Assert.True(sink.BytesWritten > 0, "nothing was streamed during the windows");
        Assert.Equal(0, sink.Mismatches);
        await Task.CompletedTask;
    }

    /// <summary>
    /// A 64 MiB object in 16 MiB ranges with a 64 KiB send window, so each pass hands over one piece and every measured
    /// window sits inside one range rather than on a boundary.
    /// </summary>
    private static SessionHarness Harness(bool checksum, out PatternObjectSink sink, out BulkObjectTransfer transfer)
    {
        const long total = 64L * Mib;
        PatternObjectSink target = new();
        AcceptObjectRouter router = new(_ => target) { ReportProgress = false };
        SessionHarness h = new(
            link: new LinkOptions { DelayMicros = 1_000 },
            table: BulkTables.Main,
            client: o =>
            {
                BulkKit.Quiet(o);
                o.BulkSendWindowBytes = 64 * 1024;
                o.BulkObjectRangeBytes = 16 * Mib;
            },
            server: o => BulkKit.ObjectReceiver(router)(o));

        BulkObjectDescriptor descriptor = new(5, 1, 1, total) { Checksum = checksum };
        transfer = h.Client.BeginBulkObjectSend(in descriptor, new PatternSource(total));
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
