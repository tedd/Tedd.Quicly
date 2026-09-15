using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Tests.State;

/// <summary>The native segment arena of stream gathers (ADR 0008 invariant 1 as amended).</summary>
public unsafe class SegmentArenaTests
{
    [Fact]
    public void Runs_Are_Contiguous_And_Reclaimed_In_Allocation_Order()
    {
        using SegmentArena arena = new(8);
        Assert.Equal(8, arena.Capacity);
        Assert.True(arena.TryAllocate(3, out int a));
        Assert.Equal(0, a);
        Assert.True(arena.TryAllocate(3, out int b));
        Assert.Equal(3, b);
        Assert.Equal(6, arena.Used);
        Assert.Equal(2, arena.Available);
        Assert.False(arena.TryAllocate(3, out int none));
        Assert.Equal(-1, none);
        arena.Free(a);
        Assert.Equal(3, arena.Used);
        Assert.True(arena.TryAllocate(3, out int wrapped));
        Assert.Equal(0, wrapped);
        Assert.Equal(8, arena.Used);
        arena.Free(b);
        Assert.Equal(3, arena.Used);
        arena.Free(wrapped);
        Assert.Equal(0, arena.Used);
        Assert.True(arena.TryAllocate(8, out int whole));
        Assert.Equal(0, whole);
        arena.Free(whole);
    }

    [Fact]
    public void Out_Of_Order_Frees_Are_Reclaimed_When_The_Older_Runs_Go()
    {
        using SegmentArena arena = new(4);
        Assert.True(arena.TryAllocate(2, out int a));
        Assert.True(arena.TryAllocate(2, out int b));
        arena.Free(b);
        Assert.Equal(4, arena.Used);
        Assert.False(arena.TryAllocate(1, out _));
        arena.Free(a);
        Assert.Equal(0, arena.Used);
        Assert.Equal(4, arena.Available);
    }

    [Fact]
    public void A_Wrap_Needs_Room_At_The_Start()
    {
        using SegmentArena arena = new(8);
        Assert.True(arena.TryAllocate(5, out int a));
        Assert.True(arena.TryAllocate(1, out int b));
        arena.Free(a);
        Assert.False(arena.TryAllocate(6, out _));
        Assert.Equal(1, arena.Used);
        Assert.True(arena.TryAllocate(5, out int c));
        Assert.Equal(0, c);
        arena.Free(b);
        arena.Free(c);
        Assert.Equal(0, arena.Used);
    }

    [Fact]
    public void The_Tail_Wraps_Exactly_At_The_End()
    {
        using SegmentArena arena = new(4);
        Assert.True(arena.TryAllocate(2, out int a));
        Assert.True(arena.TryAllocate(2, out int b));
        arena.Free(a);
        Assert.True(arena.TryAllocate(2, out int c));
        Assert.Equal(0, c);
        Assert.False(arena.TryAllocate(1, out _));
        arena.Free(b);
        arena.Free(c);
        Assert.Equal(0, arena.Used);
    }

    [Fact]
    public void Segments_Are_Addressable_While_Allocated()
    {
        using SegmentArena arena = new(4);
        Assert.True(arena.TryAllocate(2, out int start));
        Span<TransportSegment> run = arena.GetSpan(start, 2);
        byte value = 7;
        run[1] = new TransportSegment(&value, 1);
        TransportSegment* pointer = arena.GetPointer(start);
        Assert.Equal(1u, pointer[1].Length);
        Assert.True(pointer[1].Buffer == &value);
        arena.Free(start);
    }

    [Fact]
    public void Misuse_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new SegmentArena(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new SegmentArena((1 << 24) + 1));
        SegmentArena arena = new(4);
        Assert.Throws<ArgumentOutOfRangeException>(() => arena.TryAllocate(0, out _));
        Assert.Throws<ArgumentOutOfRangeException>(() => arena.TryAllocate(5, out _));
        Assert.Throws<ArgumentOutOfRangeException>(() => arena.Free(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => arena.Free(4));
        Assert.Throws<ArgumentOutOfRangeException>(() => arena.GetPointer(4)[0].Length);
        Assert.Throws<InvalidOperationException>(() => arena.Free(1));
        Assert.True(arena.TryAllocate(1, out int start));
        arena.Free(start);
        Assert.Throws<InvalidOperationException>(() => arena.Free(start));
        arena.Dispose();
        arena.Dispose();
        Assert.Equal(0, arena.Used);
    }
}
