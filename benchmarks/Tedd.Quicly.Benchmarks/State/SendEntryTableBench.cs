using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Benchmarks.State;

/// <summary>
/// The <see cref="SendEntryTable"/> slot protocol, single-threaded: allocate → header into scratch → payload
/// segment → publish → complete through the context (generation-checked CAS) → free. <c>Single</c> does one entry
/// at a time; <c>Batch32</c> keeps 32 in flight (the free stack and cold arrays see a wider working set);
/// <c>Container16</c> packs 15 published members into a container entry and fans its completion out. Reported
/// time is per entry.
/// </summary>
[Config(typeof(InProcessShortRunConfig))]
public unsafe class SendEntryTableBench
{
    private const int Ops = 1_024;

    private static readonly byte[] s_header = [0x40, 0x02, 0x81, 0x7F, 0x00, 0x10, 0x22, 0x33];
    private SendEntryTable _table = null!;
    private readonly int[] _slots = new int[32];
    private byte* _payload;

    [GlobalSetup]
    public void Setup()
    {
        _table = new SendEntryTable(256);
        _payload = (byte*)System.Runtime.InteropServices.NativeMemory.AlignedAlloc(1536, 64);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _table.Dispose();
        System.Runtime.InteropServices.NativeMemory.AlignedFree(_payload);
    }

    private void Fill(int slot)
    {
        _table.SetHeader(slot, s_header);
        ref SendEntry e = ref _table[slot];
        e.Channel = 7;
        e.Flags = SendEntryFlags.Tracked;
        e.Payload = new TransportSegment(_payload, 1200);
        _table.Sequences[slot] = (uint)slot;
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = Ops)]
    public int Single()
    {
        SendEntryTable table = _table;
        int sum = 0;
        for (int i = 0; i < Ops; i++)
        {
            table.TryAllocate(out int slot);
            Fill(slot);
            table.Publish(slot);
            table.TryTransitionContext(table.Contexts[slot], SendEntryState.InFlight, SendEntryState.Completed, out int done);
            table.Free(done);
            sum += done;
        }

        return sum;
    }

    [Benchmark(OperationsPerInvoke = Ops)]
    public int Batch32()
    {
        SendEntryTable table = _table;
        int[] slots = _slots;
        int sum = 0;
        for (int round = 0; round < Ops / 32; round++)
        {
            for (int i = 0; i < 32; i++)
            {
                table.TryAllocate(out int slot);
                Fill(slot);
                table.Publish(slot);
                slots[i] = slot;
            }

            for (int i = 0; i < 32; i++)
            {
                table.TryTransitionContext(table.Contexts[slots[i]], SendEntryState.InFlight, SendEntryState.Completed, out int done);
                sum += done;
            }

            for (int i = 0; i < 32; i++)
                table.Free(slots[i]);
        }

        return sum;
    }

    [Benchmark(OperationsPerInvoke = Ops)]
    public int Container16()
    {
        SendEntryTable table = _table;
        int sum = 0;
        for (int round = 0; round < Ops / 16; round++)
        {
            table.TryAllocate(out int container);
            table[container].Flags = SendEntryFlags.Container | SendEntryFlags.Datagram;
            for (int i = 0; i < 15; i++)
            {
                table.TryAllocate(out int member);
                Fill(member);
                table.Publish(member);
                table.AddToBatch(container, member);
            }

            table.Publish(container);
            table.TryTransitionContext(table.Contexts[container], SendEntryState.InFlight, SendEntryState.Completed, out _);
            foreach (int member in table.GetBatch(container))
            {
                table.TryTransition(member, SendEntryState.InFlight, SendEntryState.Completed);
                table.Free(member);
                sum += member;
            }

            table.ClearBatch(container);
            table.Free(container);
        }

        return sum;
    }
}
