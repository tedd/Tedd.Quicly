using System.Runtime.InteropServices;
using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Conformance;

namespace Tedd.Quicly.Transport.MsQuic.Tests.Transport;

/// <summary>The allocation tests run alone: GC.GetTotalAllocatedBytes counts every thread of the process.</summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public class MsQuicAllocationCollection
{
    public const string Name = "MsQuicAllocation";
}

/// <summary>
/// Zero GC allocation in steady state (after warm-up) on the datagram and stream send/receive paths: the sending thread
/// allocates nothing (<see cref="GC.GetAllocatedBytesForCurrentThread"/>), the callback path allocates nothing on any MsQuic
/// worker thread (<see cref="CallbackAllocationProbe"/>), and the whole process allocates less than
/// <see cref="ProcessBudgetBytes"/> per <see cref="Round"/> messages (runtime and test-runner background work included).
/// </summary>
/// <remarks>
/// The sending thread is measured over the rounds of windows <see cref="WindowedAllocation"/> describes, a window of
/// <see cref="PerWindow"/> messages at a time, because the test host's runtime now and then allocates a few kilobytes on the
/// test thread. A window drains its own messages before it ends, so a round that has to be measured again leaves no more
/// work outstanding than a single window does, and the worker-thread probes and the process budget cover every round run.
/// The probes stay a single measurement: they sample a different thread, which the test host's transient does not touch.
/// </remarks>
[Collection(MsQuicAllocationCollection.Name)]
public unsafe class MsQuicTransportAllocationTests
{
    /// <summary>Process-wide allocation budget for <see cref="Round"/> messages; documented in docs/benchmarks/msquic-transport.md.</summary>
    public const long ProcessBudgetBytes = 64 * 1024;

    private const int Warmup = 2_000;
    private const int PerWindow = 2_000;

    /// <summary>Messages in one round of windows: the 10 000 the budget above is written for.</summary>
    private const int Round = WindowedAllocation.Windows * PerWindow;
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    [Fact]
    public void Datagram_send_and_receive_do_not_allocate_in_steady_state()
    {
        var clientSink = new CountingTransportSink();
        var serverSink = new CountingTransportSink();
        var harness = new MsQuicTransportHarness();
        byte* payload = (byte*)NativeMemory.AllocZeroed(64);
        var segment = (TransportSegment*)NativeMemory.AllocZeroed((nuint)sizeof(TransportSegment));
        *segment = new TransportSegment(payload, 64);
        long process = -1;
        int windows = 0, measured = 0;
        try
        {
            ConformancePair pair = harness.CreatePair(clientSink, serverSink);
            ITransport client = pair.Client;
            Assert.True(Spin.UntilAtLeast(ref clientSink.Connected, 1, Timeout) && Spin.UntilAtLeast(ref serverSink.Connected, 1, Timeout));
            Assert.True(Spin.Until(() => client.Capabilities.Datagrams, Timeout));

            SendDatagrams(client, segment, 0, Warmup);
            Assert.True(Spin.UntilAtLeast(ref clientSink.FinalStates, Warmup, Timeout), $"warm-up finals {clientSink.FinalStates}");

            clientSink.Probe.Arm();
            serverSink.Probe.Arm();
            process = GC.GetTotalAllocatedBytes(precise: true);
            int sent = Warmup;
            windows = WindowedAllocation.AssertNone(() =>
            {
                SendDatagrams(client, segment, sent, PerWindow);
                sent += PerWindow;
                if (!Spin.UntilAtLeast(ref clientSink.FinalStates, sent, Timeout))
                    throw new InvalidOperationException($"final states {clientSink.FinalStates} of {sent}");
            });
            measured = windows * PerWindow;
            Assert.Equal(Warmup + measured, clientSink.FinalStates);
            bool received = Spin.UntilAtLeast(ref serverSink.DatagramsReceived, (Warmup + measured) * 9 / 10, Timeout);
            process = GC.GetTotalAllocatedBytes(precise: true) - process;
            long budget = (long)ProcessBudgetBytes * measured / Round;
            Assert.True(received, $"received {serverSink.DatagramsReceived}");
            Assert.Equal(0, clientSink.Probe.Allocated);
            Assert.Equal(0, serverSink.Probe.Allocated);
            Assert.True(clientSink.Probe.Samples > measured / 2, $"client samples {clientSink.Probe.Samples}");
            Assert.True(serverSink.Probe.Samples > measured / 2, $"server samples {serverSink.Probe.Samples}");
            Assert.True(process < budget, $"the process allocated {process} bytes over {measured} datagrams (budget {budget})");
        }
        finally
        {
            harness.Dispose();
            NativeMemory.Free(segment);
            NativeMemory.Free(payload);
        }
        Assert.Null(harness.CleanupError);
        TestContext.Current.SendDiagnosticMessage($"datagrams: process {process} B over {measured} messages in {windows} windows");
    }

    [Fact]
    public void Stream_send_and_receive_do_not_allocate_in_steady_state()
    {
        var clientSink = new CountingTransportSink();
        var serverSink = new CountingTransportSink();
        var harness = new MsQuicTransportHarness();
        byte* payload = (byte*)NativeMemory.AllocZeroed(1024);
        var segment = (TransportSegment*)NativeMemory.AllocZeroed((nuint)sizeof(TransportSegment));
        *segment = new TransportSegment(payload, 1024);
        long process = -1;
        int windows = 0, measured = 0;
        try
        {
            ConformancePair pair = harness.CreatePair(clientSink, serverSink);
            ITransport client = pair.Client;
            clientSink.Transport = client;
            serverSink.Transport = pair.Server;
            Assert.True(Spin.UntilAtLeast(ref clientSink.Connected, 1, Timeout) && Spin.UntilAtLeast(ref serverSink.Connected, 1, Timeout));
            Assert.Equal(TransportStatus.Success, client.OpenStream(StreamKind.Bidirectional, 1, 32767, out TransportStreamId id));
            Assert.Equal(TransportStatus.Success, client.SendStream(id, segment, 1, 0, TransportSendFlags.Start));
            SendStream(client, id, segment, 1, Warmup - 1);
            Assert.True(Spin.UntilAtLeast(ref clientSink.SendCompletions, Warmup, Timeout), $"warm-up completions {clientSink.SendCompletions}");
            Assert.True(Spin.UntilAtLeast(ref serverSink.StreamBytes, Warmup * 1024L, Timeout), $"warm-up bytes {serverSink.StreamBytes}");

            clientSink.Probe.Arm();
            serverSink.Probe.Arm();
            process = GC.GetTotalAllocatedBytes(precise: true);
            int sent = Warmup;
            windows = WindowedAllocation.AssertNone(() =>
            {
                SendStream(client, id, segment, sent, PerWindow);
                sent += PerWindow;
                if (!Spin.UntilAtLeast(ref clientSink.SendCompletions, sent, Timeout))
                    throw new InvalidOperationException($"completions {clientSink.SendCompletions} of {sent}");
            });
            measured = windows * PerWindow;
            Assert.Equal(Warmup + measured, clientSink.SendCompletions);
            bool bytes = Spin.UntilAtLeast(ref serverSink.StreamBytes, (Warmup + measured) * 1024L, Timeout);
            process = GC.GetTotalAllocatedBytes(precise: true) - process;
            long budget = (long)ProcessBudgetBytes * measured / Round;
            Assert.True(bytes, $"bytes {serverSink.StreamBytes}");
            Assert.Equal(0, clientSink.CanceledCompletions);
            Assert.Equal(0, clientSink.Probe.Allocated);
            Assert.Equal(0, serverSink.Probe.Allocated);
            Assert.True(clientSink.Probe.Samples > measured / 2, $"client samples {clientSink.Probe.Samples}");
            Assert.True(serverSink.Probe.Samples > 0, $"server samples {serverSink.Probe.Samples}");
            Assert.True(process < budget, $"the process allocated {process} bytes over {measured} stream sends (budget {budget})");
        }
        finally
        {
            harness.Dispose();
            NativeMemory.Free(segment);
            NativeMemory.Free(payload);
        }
        Assert.Null(harness.CleanupError);
        TestContext.Current.SendDiagnosticMessage($"streams: process {process} B over {measured} messages in {windows} windows");
    }

    private static void SendDatagrams(ITransport transport, TransportSegment* segment, int first, int count)
    {
        for (int i = 0; i < count; i++)
        {
            TransportStatus status = transport.SendDatagram(segment, 1, (ulong)(first + i + 1), TransportSendFlags.None);
            if (status != TransportStatus.Success) throw new InvalidOperationException("SendDatagram returned " + status);
        }
    }

    private static void SendStream(ITransport transport, TransportStreamId id, TransportSegment* segment, int first, int count)
    {
        for (int i = 0; i < count; i++)
        {
            TransportStatus status = transport.SendStream(id, segment, 1, (ulong)(first + i + 1), TransportSendFlags.None);
            if (status != TransportStatus.Success) throw new InvalidOperationException("SendStream returned " + status);
        }
    }
}
