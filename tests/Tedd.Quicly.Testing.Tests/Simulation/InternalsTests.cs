using Tedd.Quicly.Core.Transport;
using Tedd.Quicly.Testing.Simulation;

namespace Tedd.Quicly.Testing.Tests.Simulation;

public class InternalsTests
{
    [Fact]
    public void EventQueue_PopsByDueTime_FifoAmongEqualTimes_AndGrows()
    {
        EventQueue queue = new();
        DeterministicRandom random = new(1);
        for (int i = 0; i < 1000; i++)
        {
            SimEvent e = new() { Due = random.NextInt64(50), I0 = i };
            queue.Push(ref e);
        }
        Assert.Equal(1000, queue.Count);
        long lastDue = -1, lastSeq = -1;
        while (queue.Count > 0)
        {
            long due = queue.PeekDue;
            SimEvent e = queue.Pop();
            Assert.Equal(due, e.Due);
            Assert.True(e.Due > lastDue || (e.Due == lastDue && e.Seq > lastSeq));
            lastDue = e.Due;
            lastSeq = e.Seq;
        }
        SimEvent again = new() { Due = 5 };
        queue.Push(ref again);
        queue.Clear();
        Assert.Equal(0, queue.Count);
    }

    [Fact]
    public void TxQueue_ServesHighestPriorityFirst_FifoWithinAPriority()
    {
        TxQueue queue = new();
        for (int i = 0; i < 200; i++)
            queue.Push(new TxPacket { Priority = i % 3, Bytes = 10, Record = i });
        Assert.Equal(2000, queue.QueuedBytes);
        int lastPriority = int.MaxValue, lastRecord = -1;
        while (queue.Count > 0)
        {
            TxPacket packet = queue.Pop();
            if (packet.Priority != lastPriority)
            {
                Assert.True(packet.Priority < lastPriority);
                lastPriority = packet.Priority;
                lastRecord = -1;
            }
            Assert.True(packet.Record > lastRecord);
            lastRecord = packet.Record;
        }
        Assert.Equal(0, queue.QueuedBytes);
    }

    [Fact]
    public void TxQueue_RemoveOversizedDatagrams_KeepsTheRestInOrder()
    {
        TxQueue queue = new();
        queue.Push(new TxPacket { IsDatagram = true, Priority = 5, Bytes = 900, Record = 0 });
        queue.Push(new TxPacket { IsDatagram = false, Priority = 5, Bytes = 900, Record = 1 });
        queue.Push(new TxPacket { IsDatagram = true, Priority = 1, Bytes = 100, Record = 2 });
        queue.Push(new TxPacket { IsDatagram = true, Priority = 9, Bytes = 800, Record = 3 });
        queue.Push(new TxPacket { IsDatagram = true, Priority = 5, Bytes = 400, Record = 4 });
        List<TxPacket> removed = new();
        queue.RemoveOversizedDatagrams(500, removed);
        Assert.Equal([0, 3], removed.Select(p => p.Record).ToArray());
        Assert.Equal(1400, queue.QueuedBytes);
        Assert.Equal([1, 4, 2], new[] { queue.Pop().Record, queue.Pop().Record, queue.Pop().Record });

        queue.Push(new TxPacket { Bytes = 1, Record = 7 });
        queue.Push(new TxPacket { Bytes = 1, Record = 8, Priority = 3 });
        removed.Clear();
        queue.Clear(removed);
        Assert.Equal([7, 8], removed.Select(p => p.Record).ToArray());
        Assert.Equal(0, queue.Count);
        Assert.Equal(0, queue.QueuedBytes);
    }

    [Fact]
    public void DeterministicRandom_IsReproducibleAndBounded()
    {
        DeterministicRandom a = new(77), b = new(77), c = new(78);
        Assert.Equal(a.NextUInt64(), b.NextUInt64());
        Assert.NotEqual(a.NextUInt64(), c.NextUInt64());
        Assert.Equal(0, a.NextInt64(0));
        Assert.Equal(0, a.NextInt64(1));
        Assert.False(a.Chance(0));
        Assert.False(a.Chance(-5));
        Assert.True(a.Chance(100));
        int hits = 0;
        for (int i = 0; i < 100_000; i++)
        {
            long value = a.NextInt64(10);
            Assert.InRange(value, 0, 9);
            if (a.Chance(25))
                hits++;
        }
        Assert.InRange(hits, 24_000, 26_000);
    }

    [Fact]
    public void SimBufferPool_ReusesPowerOfTwoPinnedArrays()
    {
        SimBufferPool pool = new();
        byte[] small = pool.Rent(0);
        Assert.Equal(64, small.Length);
        byte[] medium = pool.Rent(65);
        Assert.Equal(128, medium.Length);
        pool.Return(medium);
        Assert.Same(medium, pool.Rent(100));
        pool.Return(null);
        pool.Return(new byte[100]); // not from the pool: ignored
        Assert.Equal(128, pool.Rent(100).Length);
        unsafe
        {
            Assert.True(SimBufferPool.AddressOf(small) != null);
        }
    }

    [Fact]
    public unsafe void SimStream_ReassemblesOutOfOrderChunks_AndSplitsSegmentsAtChunkBoundaries()
    {
        SimBufferPool pool = new();
        SimStream s = new() { Generation = 1, InUse = true };
        byte[] data = Sim.Pattern(40);
        Assert.False(s.WriteChunk(pool, data.AsSpan(20, 10), 20, fin: false));
        Assert.False(s.WriteChunk(pool, data.AsSpan(10, 10), 10, fin: false));
        Assert.Equal(0, s.Frontier);
        Assert.True(s.WriteChunk(pool, data.AsSpan(0, 10), 0, fin: false));
        Assert.Equal(30, s.Frontier);
        Assert.True(s.WriteChunk(pool, data.AsSpan(30, 10), 30, fin: true));
        Assert.True(s.FinArrived);

        TransportSegment[] segments = new TransportSegment[1];
        Assert.Equal(4, s.BuildSegments(ref segments));
        Assert.Equal(data[..10], segments[0].AsSpan().ToArray());
        Assert.Equal(data[30..], segments[3].AsSpan().ToArray());
        s.Head = 15;
        Assert.Equal(3, s.BuildSegments(ref segments));
        Assert.Equal(data[15..20], segments[0].AsSpan().ToArray());
        s.Head = 40;
        Assert.Equal(0, s.BuildSegments(ref segments));

        // Growth after consumption compacts first, then grows.
        byte[] big = Sim.Pattern(5000, 3);
        s.FinalSize = -1;
        Assert.True(s.WriteChunk(pool, big, 40, fin: false));
        Assert.Equal(1, s.BuildSegments(ref segments));
        Assert.Equal(big, segments[0].AsSpan().ToArray());
        s.DiscardReceive(pool);
        Assert.Null(s.RecvBuffer);
    }

    [Fact]
    public void SimStream_ManyChunks_GrowTheBoundaryRing_AndPendingFifo()
    {
        SimBufferPool pool = new();
        SimStream s = new() { Generation = 1, InUse = true };
        for (int i = 0; i < 40; i++)
            Assert.True(s.WriteChunk(pool, [(byte)i], i, fin: false));
        TransportSegment[] segments = new TransportSegment[2];
        Assert.Equal(40, s.BuildSegments(ref segments));

        for (int i = 0; i < 20; i++)
            s.EnqueueSend(i);
        Assert.Equal(0, s.DequeueSend());
        s.RemoveSend(5);
        s.RemoveSend(1000); // absent: ignored
        Assert.Equal(18, s.PendingCount);
        Assert.Equal(1, s.PendingAt(0));
        Assert.Equal(6, s.PendingAt(4));
        s.Reset();
        Assert.Equal(0, s.PendingCount);
        Assert.False(s.InUse);
    }
}
