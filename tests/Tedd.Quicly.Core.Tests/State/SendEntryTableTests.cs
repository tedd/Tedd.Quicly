using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Tests.State;

public unsafe class SendEntryTableTests
{
    [Fact]
    public void SendEntry_Is_One_Cache_Line_With_The_Documented_Offsets()
    {
        Assert.Equal(64, sizeof(SendEntry));
        Assert.Equal(SendEntry.Size, sizeof(SendEntry));

        SendEntry e = default;
        byte* b = (byte*)&e;
        Assert.Equal(SendEntry.StateOffset, (int)((byte*)&e.State - b));
        Assert.Equal(SendEntry.GenerationOffset, (int)((byte*)&e.Generation - b));
        Assert.Equal(SendEntry.ChannelOffset, (int)((byte*)&e.Channel - b));
        Assert.Equal(SendEntry.FlagsOffset, (int)((byte*)&e.Flags - b));
        Assert.Equal(SendEntry.HeaderLengthOffset, (int)((byte*)&e.HeaderLength - b));
        Assert.Equal(SendEntry.HeaderOffset, (int)((byte*)&e.Header - b));
        Assert.Equal(SendEntry.PayloadOffset, (int)((byte*)&e.Payload - b));
        Assert.Equal(SendEntry.HeaderScratchOffset, (int)(e.HeaderScratch - b));
        Assert.Equal(0, SendEntry.StateOffset);
        Assert.Equal(4, SendEntry.GenerationOffset);
        Assert.Equal(8, SendEntry.ChannelOffset);
        Assert.Equal(10, SendEntry.FlagsOffset);
        Assert.Equal(11, SendEntry.HeaderLengthOffset);
        Assert.Equal(16, SendEntry.HeaderOffset);
        Assert.Equal(32, SendEntry.PayloadOffset);
        Assert.Equal(48, SendEntry.HeaderScratchOffset);
        Assert.Equal(16, SendEntry.HeaderScratchSize);
    }

    [Fact]
    public void Header_And_Payload_Are_Adjacent_Segments()
    {
        SendEntry e = default;
        TransportSegment* segments = &e.Header;
        Assert.True(segments + 1 == &e.Payload);
        Assert.Equal(16, sizeof(TransportSegment));

        e.Header = new TransportSegment(e.HeaderScratch, 5);
        e.Payload = new TransportSegment((byte*)0x1000, 77);
        Assert.Equal(5u, segments[0].Length);
        Assert.Equal(77u, segments[1].Length);
        Assert.True(segments[0].Buffer == e.HeaderScratch);
        Assert.True(e.HeaderScratch + SendEntry.HeaderScratchSize == (byte*)&e + SendEntry.Size);
    }

    [Fact]
    public void Enum_Values_Are_Fixed()
    {
        Assert.Equal(0, (int)SendEntryState.Free);
        Assert.Equal(1, (int)SendEntryState.Filling);
        Assert.Equal(2, (int)SendEntryState.InFlight);
        Assert.Equal(3, (int)SendEntryState.Cancelling);
        Assert.Equal(4, (int)SendEntryState.Completed);
        Assert.Equal(1, (byte)SendEntryFlags.Tracked);
        Assert.Equal(2, (byte)SendEntryFlags.Datagram);
        Assert.Equal(4, (byte)SendEntryFlags.Container);
        Assert.Equal(8, (byte)SendEntryFlags.Immediate);
        Assert.Equal(16, (byte)SendEntryFlags.Fin);
        Assert.Equal(32, (byte)SendEntryFlags.Retry);
        Assert.Equal(64, (byte)SendEntryFlags.Pinned);
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(3, 4)]
    [InlineData(1000, 1024)]
    public void Capacity_Rounds_Up_To_Power_Of_Two(int requested, int expected)
    {
        using var table = new SendEntryTable(requested);
        Assert.Equal(expected, table.Capacity);
        Assert.Equal(expected, table.Available);
        Assert.Equal(0, table.Count);
        Assert.Equal(expected, table.Entries.Length);
        Assert.Equal(expected, table.PinHandles.Length);
        Assert.Equal(0, (nint)table.Entries.Pointer % 64);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData((1 << 30) + 1)]
    public void Constructor_Rejects_Invalid_Capacity(int requested)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new SendEntryTable(requested));
    }

    [Fact]
    public void Allocate_Sets_Filling_And_A_Fresh_Odd_Generation_And_Resets_Cold_Fields()
    {
        using var table = new SendEntryTable(4);
        Assert.True(table.TryAllocate(out int slot));
        Assert.Equal(0, slot);
        Assert.Equal(1, table.Count);
        Assert.Equal(3, table.Available);

        ref SendEntry e = ref table[slot];
        Assert.Equal((int)SendEntryState.Filling, e.State);
        Assert.Equal(SendEntryState.Filling, table.GetState(slot));
        Assert.Equal(1u, e.Generation);
        Assert.Equal(1u, e.Generation & 1);
        Assert.Equal(-1, table.Next[slot]);
        Assert.Equal(-1, table.BatchHead[slot]);
        Assert.Equal(0, table.BatchCount[slot]);
        Assert.Equal(table.MakeContext(slot), table.Contexts[slot]);
        Assert.True(table.Leases[slot].IsEmpty);

        // Dirty every field, complete and free, then re-allocate: everything is reset and the generation moved on.
        e.Channel = 7;
        e.Flags = SendEntryFlags.Tracked | SendEntryFlags.Fin;
        e.HeaderLength = 9;
        e.Payload = new TransportSegment((byte*)0x10, 10);
        table.Leases[slot] = new BufferLease(1, 0, 1, 2, 3, 4);
        table.Keys[slot] = 99;
        table.Sequences[slot] = 5;
        table.Deadlines[slot] = 123;
        table.Next[slot] = 2;
        table.BatchHead[slot] = 3;
        table.BatchCount[slot] = 1;
        table.PinHandles[slot] = 55;
        table.Publish(slot);
        Assert.True(table.TryTransition(slot, SendEntryState.InFlight, SendEntryState.Completed));
        table.Free(slot);
        Assert.Equal(0, table.Count);

        Assert.True(table.TryAllocate(out int again));
        Assert.Equal(slot, again);
        Assert.Equal(3u, e.Generation);
        Assert.Equal(0, e.Channel);
        Assert.Equal(SendEntryFlags.None, e.Flags);
        Assert.Equal(0, e.HeaderLength);
        Assert.Equal(0u, e.Payload.Length);
        Assert.True(table.Leases[slot].IsEmpty);
        Assert.Equal(0UL, table.Keys[slot]);
        Assert.Equal(0u, table.Sequences[slot]);
        Assert.Equal(0, table.Deadlines[slot]);
        Assert.Equal(-1, table.Next[slot]);
        Assert.Equal(-1, table.BatchHead[slot]);
        Assert.Equal(0, table.BatchCount[slot]);
        Assert.Equal(0, table.PinHandles[slot]);
        Assert.Equal(table.MakeContext(slot), table.Contexts[slot]);
    }

    [Fact]
    public void Generation_Never_Becomes_Zero_Or_Even()
    {
        using var table = new SendEntryTable(1);
        Assert.True(table.TryAllocate(out int slot));
        table.Discard(slot);
        table[slot].Generation = uint.MaxValue; // simulate a slot that has wrapped
        Assert.True(table.TryAllocate(out slot));
        Assert.Equal(1u, table[slot].Generation);
        table.Discard(slot);
        table[slot].Generation = 0xFFFFFFFE;
        Assert.True(table.TryAllocate(out slot));
        Assert.Equal(uint.MaxValue, table[slot].Generation);
    }

    [Fact]
    public void Exhaustion_Returns_False_And_Free_Restores_Capacity()
    {
        using var table = new SendEntryTable(4);
        int[] slots = new int[4];
        for (int i = 0; i < 4; i++)
            Assert.True(table.TryAllocate(out slots[i]));
        Assert.Equal(new[] { 0, 1, 2, 3 }, slots);
        Assert.False(table.TryAllocate(out int none));
        Assert.Equal(-1, none);
        Assert.Equal(0, table.Available);
        Assert.Equal(4, table.Count);

        table.Discard(slots[2]);
        Assert.True(table.TryAllocate(out int reused));
        Assert.Equal(2, reused);
    }

    [Fact]
    public void Publish_Requires_Filling()
    {
        using var table = new SendEntryTable(2);
        Assert.True(table.TryAllocate(out int slot));
        table.Publish(slot);
        Assert.Equal(SendEntryState.InFlight, table.GetState(slot));
        Assert.Throws<InvalidOperationException>(() => table.Publish(slot));
        table.Discard(slot);
        Assert.Throws<InvalidOperationException>(() => table.Publish(slot));
    }

    [Fact]
    public void TryTransition_Is_A_Compare_Exchange()
    {
        using var table = new SendEntryTable(2);
        Assert.True(table.TryAllocate(out int slot));
        table.Publish(slot);
        Assert.False(table.TryTransition(slot, SendEntryState.Filling, SendEntryState.Completed));
        Assert.Equal(SendEntryState.InFlight, table.GetState(slot));
        Assert.True(table.TryTransition(slot, SendEntryState.InFlight, SendEntryState.Cancelling));
        Assert.False(table.TryTransition(slot, SendEntryState.InFlight, SendEntryState.Completed));
        Assert.True(table.TryTransition(slot, SendEntryState.Cancelling, SendEntryState.Completed));
        Assert.Equal(SendEntryState.Completed, table.GetState(slot));
        table.Free(slot);
        Assert.Equal(SendEntryState.Free, table.GetState(slot));
    }

    [Fact]
    public void Free_Requires_Completed_And_Discard_Requires_Unfinished()
    {
        using var table = new SendEntryTable(2);
        Assert.True(table.TryAllocate(out int slot));
        Assert.Throws<InvalidOperationException>(() => table.Free(slot));      // Filling
        table.Publish(slot);
        Assert.Throws<InvalidOperationException>(() => table.Free(slot));      // InFlight
        Assert.True(table.TryTransition(slot, SendEntryState.InFlight, SendEntryState.Completed));
        Assert.Throws<InvalidOperationException>(() => table.Discard(slot));   // Completed
        table.Free(slot);
        Assert.Throws<InvalidOperationException>(() => table.Free(slot));      // Free
        Assert.Throws<InvalidOperationException>(() => table.Discard(slot));   // Free
        Assert.Equal(2, table.Available);
    }

    [Fact]
    public void Context_Round_Trips_And_Rejects_Stale_Or_Malformed_Values()
    {
        using var table = new SendEntryTable(8);
        Assert.True(table.TryAllocate(out int slot));
        ulong context = table.MakeContext(slot);
        Assert.Equal(((ulong)table[slot].Generation << 32) | (uint)slot, context);
        Assert.Equal(context, table.Contexts[slot]);

        Assert.True(table.TryResolveContext(context, out int resolved));
        Assert.Equal(slot, resolved);

        // Wrong generation.
        Assert.False(table.TryResolveContext(context + (1UL << 32), out resolved));
        Assert.Equal(-1, resolved);
        // Slot outside the table.
        Assert.False(table.TryResolveContext(((ulong)table[slot].Generation << 32) | 1000, out resolved));
        Assert.Equal(-1, resolved);
        Assert.False(table.TryResolveContext(((ulong)table[slot].Generation << 32) | 0xFFFFFFFF, out _));

        // After free the slot is Free: even the matching generation is rejected.
        table.Publish(slot);
        Assert.True(table.TryTransition(slot, SendEntryState.InFlight, SendEntryState.Completed));
        table.Free(slot);
        Assert.False(table.TryResolveContext(context, out _));

        // Re-allocation bumps the generation: the old context is stale, the new one resolves.
        Assert.True(table.TryAllocate(out int again));
        Assert.Equal(slot, again);
        Assert.False(table.TryResolveContext(context, out _));
        Assert.True(table.TryResolveContext(table.MakeContext(slot), out resolved));
        Assert.Equal(slot, resolved);
    }

    [Fact]
    public void SetHeader_Copies_Into_Scratch_And_Points_The_Header_Segment_At_It()
    {
        using var table = new SendEntryTable(4);
        Assert.True(table.TryAllocate(out int slot));
        byte[] header = [1, 2, 3, 4, 5, 6, 7];
        table.SetHeader(slot, header);

        ref SendEntry e = ref table[slot];
        Assert.Equal(7, e.HeaderLength);
        Assert.Equal(7u, e.Header.Length);
        Assert.True(e.Header.Buffer == table.GetHeaderScratch(slot).GetPinnableReference() switch { _ => (table.Entries.Pointer + slot)->HeaderScratch });
        Assert.True(e.Header.Buffer >= (byte*)table.Entries.Pointer);
        Assert.True(e.Header.Buffer < (byte*)(table.Entries.Pointer + table.Capacity));
        Assert.Equal(header, e.Header.AsSpan().ToArray());
        Assert.Equal(header, table.GetHeaderScratch(slot)[..7].ToArray());
        Assert.Equal(SendEntry.HeaderScratchSize, table.GetHeaderScratch(slot).Length);

        table.SetHeader(slot, new byte[16]);
        Assert.Equal(16, e.HeaderLength);
        Assert.Throws<ArgumentOutOfRangeException>(() => table.SetHeader(slot, new byte[17]));

        table.SetHeader(slot, ReadOnlySpan<byte>.Empty);
        Assert.Equal(0, e.HeaderLength);
        Assert.Equal(0u, e.Header.Length);
    }

    [Fact]
    public void GetSegments_Covers_Consecutive_Slots_As_One_Gather()
    {
        using var table = new SendEntryTable(4);
        Assert.True(table.TryAllocate(out int a));
        Assert.True(table.TryAllocate(out int b));
        Assert.Equal(a + 1, b);
        table.SetHeader(a, [0xA]);
        table.SetHeader(b, [0xB, 0xB]);
        table[a].Payload = new TransportSegment((byte*)0x100, 100);
        table[b].Payload = new TransportSegment((byte*)0x200, 200);

        TransportSegment* segments = table.GetSegments(a);
        Assert.True(segments == &table[a].Header);
        Assert.Equal(1u, segments[0].Length);
        Assert.Equal(100u, segments[1].Length);
        Assert.True(segments + 4 == table.GetSegments(b) + 2);
        // Slot b's segments sit at +4 (after the 16-byte scratch area of slot a), not at +2.
        Assert.Equal(2u, segments[4].Length);
        Assert.Equal(200u, segments[5].Length);
    }

    [Fact]
    public void Container_Batches_Link_Members_Through_Next()
    {
        using var table = new SendEntryTable(8);
        Assert.True(table.TryAllocate(out int container));
        table[container].Flags = SendEntryFlags.Container;
        ContainerBatch empty = table.GetBatch(container);
        Assert.Equal(0, empty.Count);
        Assert.Equal(-1, empty.Head);
        foreach (int _ in empty)
            Assert.Fail("empty batch yielded a member");

        int[] members = new int[3];
        for (int i = 0; i < members.Length; i++)
        {
            Assert.True(table.TryAllocate(out members[i]));
            table.AddToBatch(container, members[i]);
        }

        ContainerBatch batch = table.GetBatch(container);
        Assert.Equal(3, batch.Count);
        Assert.Equal(members[2], batch.Head);
        var seen = new List<int>();
        foreach (int member in batch)
            seen.Add(member);
        Assert.Equal(new[] { members[2], members[1], members[0] }, seen);
        Assert.Equal(-1, table.Next[members[0]]);

        table.ClearBatch(container);
        Assert.Equal(0, table.GetBatch(container).Count);
        Assert.Equal(-1, table.GetBatch(container).Head);
    }

    [Fact]
    public void Dispose_Is_Idempotent()
    {
        var table = new SendEntryTable(4);
        table.Dispose();
        table.Dispose();
        Assert.True(table.Entries.IsDisposed);
        Assert.True(table.Leases.IsDisposed);
    }

    [Fact]
    public void Allocate_Publish_Complete_Free_Does_Not_Allocate()
    {
        using var table = new SendEntryTable(64);
        RunLoop(table, 1_000);

        long before = GC.GetAllocatedBytesForCurrentThread();
        RunLoop(table, 100_000);
        long after = GC.GetAllocatedBytesForCurrentThread();
        Assert.Equal(0, after - before);

        static void RunLoop(SendEntryTable table, int iterations)
        {
            Span<byte> header = stackalloc byte[8];
            for (int i = 0; i < iterations; i++)
            {
                table.TryAllocate(out int slot);
                table.SetHeader(slot, header);
                table.Publish(slot);
                table.TryResolveContext(table.Contexts[slot], out int resolved);
                table.TryTransition(resolved, SendEntryState.InFlight, SendEntryState.Completed);
                table.Free(slot);
                table.TryAllocate(out int container);
                table.TryAllocate(out int member);
                table.AddToBatch(container, member);
                foreach (int m in table.GetBatch(container))
                    _ = m;
                table.ClearBatch(container);
                table.Discard(member);
                table.Discard(container);
                _ = table.Count;
                _ = table.Available;
            }
        }
    }

    [Fact]
    public void Transport_Thread_Completes_While_Owner_Allocates_And_Frees()
    {
        // Owner: allocate → publish → enqueue context on a ring. Transport: dequeue → resolve → InFlight→Completed →
        // enqueue slot on the completion ring. Owner frees only after observing the completion (ADR 0008 §3/§4).
        const int Sends = 200_000;
        using var table = new SendEntryTable(64);
        var submitted = new SpscRing<ulong>(64);
        var completed = new SpscRing<int>(65);
        int stale = 0;

        var transport = new Thread(() =>
        {
            SpinWait spinner = default;
            for (int n = 0; n < Sends;)
            {
                if (!submitted.TryDequeue(out ulong context))
                {
                    spinner.SpinOnce(sleep1Threshold: -1);
                    continue;
                }

                if (!table.TryResolveContext(context, out int slot) || !table.TryTransition(slot, SendEntryState.InFlight, SendEntryState.Completed))
                    stale++;
                while (!completed.TryEnqueue(slot))
                    spinner.SpinOnce(sleep1Threshold: -1);
                n++;
            }
        })
        { IsBackground = true };
        transport.Start();

        SpinWait owner = default;
        int freed = 0;
        for (int sent = 0; sent < Sends;)
        {
            if (table.TryAllocate(out int slot))
            {
                table[slot].Channel = (ushort)(sent & 0xFF);
                table.Publish(slot);
                ulong context = table.Contexts[slot];
                while (!submitted.TryEnqueue(context))
                    owner.SpinOnce(sleep1Threshold: -1);
                sent++;
            }

            while (completed.TryDequeue(out int done))
            {
                Assert.Equal(SendEntryState.Completed, table.GetState(done));
                table.Free(done);
                freed++;
            }
        }

        transport.Join();
        while (completed.TryDequeue(out int done))
        {
            table.Free(done);
            freed++;
        }

        Assert.Equal(Sends, freed);
        Assert.Equal(0, stale);
        Assert.Equal(0, table.Count);
    }
}
