using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Tedd.Quicly.Archive.Memory;

/// <summary>Lease handed out by <see cref="SlabAllocatorV1"/>; same 16-byte layout as the shipping lease.</summary>
[StructLayout(LayoutKind.Sequential, Pack = 4, Size = 16)]
public readonly struct BufferLeaseV1
{
    internal BufferLeaseV1(byte classIndex, ushort generation, int blockIndex, int offset, int length)
    {
        ClassIndex = classIndex;
        Reserved = 0;
        Generation = generation;
        BlockIndex = blockIndex;
        Offset = offset;
        Length = length;
    }

    public readonly byte ClassIndex;
    public readonly byte Reserved;
    public readonly ushort Generation;
    public readonly int BlockIndex;
    public readonly int Offset;
    public readonly int Length;

    public bool IsValid => Length != 0;
}

/// <summary>
/// V1 of the slab allocator (superseded by <c>Tedd.Quicly.Core.Memory.SlabAllocator</c>, see
/// docs/benchmarks/memory.md). One Treiber stack per size class: a single 64-bit head word (32-bit ABA tag +
/// 32-bit top index) and one global rented counter per class. Fast single-threaded, but every thread that
/// rents or returns from the class hammers the same two cache lines, which collapses under contention. Kept
/// so the sharded version stays measurable against it. Hot-path code is verbatim from the version it replaced.
/// </summary>
public sealed unsafe class SlabAllocatorV1 : IDisposable
{
    private const ulong TagIncrement = 1UL << 32;
    private const ulong TagMask = 0xFFFF_FFFF_0000_0000UL;
    private const byte StateFree = 0;
    private const byte StateRented = 1;

    [StructLayout(LayoutKind.Explicit, Size = 128)]
    private struct ClassHeader
    {
        [FieldOffset(0)] public long Head;
        [FieldOffset(64)] public int Rented;
        [FieldOffset(68)] public int Peak;
        [FieldOffset(72)] public long Exhaustions;
    }

    [StructLayout(LayoutKind.Sequential, Pack = 4, Size = 8)]
    private struct BlockMeta
    {
        public int Next;
        public ushort Generation;
        public byte State;
        public byte Reserved;
    }

    private struct SizeClass
    {
        public byte* Base;
        public ClassHeader* Header;
        public int BlockSize;
        public int BlockCount;
        public int MaxOffset;
    }

    private readonly bool _validateLeases;
    private SizeClass[] _classes;
    private bool _disposed;

    public SlabAllocatorV1(ReadOnlySpan<(int BlockSize, int BlockCount)> classes, bool validateLeases)
    {
        _validateLeases = validateLeases;
        _classes = new SizeClass[classes.Length];
        for (int ci = 0; ci < classes.Length; ci++)
        {
            (int blockSize, int blockCount) = classes[ci];
            ref SizeClass c = ref _classes[ci];
            c.BlockSize = blockSize;
            c.BlockCount = blockCount;
            c.MaxOffset = blockSize * (blockCount - 1);
            c.Base = (byte*)NativeMemory.AlignedAlloc((nuint)blockSize * (nuint)blockCount, 64);

            nuint metaBytes = (nuint)sizeof(ClassHeader) + (nuint)blockCount * (nuint)sizeof(BlockMeta);
            c.Header = (ClassHeader*)NativeMemory.AlignedAlloc(metaBytes, 64);
            NativeMemory.Clear(c.Header, metaBytes);

            BlockMeta* blocks = (BlockMeta*)(c.Header + 1);
            for (int i = 0; i < blockCount - 1; i++)
                blocks[i].Next = i + 1;
            blocks[blockCount - 1].Next = -1;
            c.Header->Head = 0;
        }
    }

    public bool TryRent(int minimumLength, out BufferLeaseV1 lease)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(minimumLength);
        SizeClass[] classes = _classes;
        for (int ci = 0; ci < classes.Length; ci++)
        {
            if (classes[ci].BlockSize >= minimumLength)
                return TryRentFromClass(classes, ci, out lease);
        }

        ObjectDisposedException.ThrowIf(_disposed, this);
        lease = default;
        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryRentFromClass(SizeClass[] classes, int ci, out BufferLeaseV1 lease)
    {
        ref SizeClass c = ref classes[ci];
        ClassHeader* header = c.Header;
        BlockMeta* blocks = (BlockMeta*)(header + 1);

        long head = Volatile.Read(ref header->Head);
        int index;
        while (true)
        {
            index = (int)head;
            if (index < 0)
            {
                Interlocked.Increment(ref header->Exhaustions);
                lease = default;
                return false;
            }

            int next = blocks[index].Next;
            long newHead = (long)((((ulong)head + TagIncrement) & TagMask) | (uint)next);
            long seen = Interlocked.CompareExchange(ref header->Head, newHead, head);
            if (seen == head)
                break;
            head = seen;
        }

        BlockMeta* block = blocks + index;
        ushort generation = (ushort)(block->Generation + 1);
        block->Generation = generation;
        if (_validateLeases)
            block->State = StateRented;

        int rented = Interlocked.Increment(ref header->Rented);
        if (rented > Volatile.Read(ref header->Peak))
            RaisePeak(header, rented);

        int blockSize = c.BlockSize;
        lease = new BufferLeaseV1((byte)ci, generation, index, index * blockSize, blockSize);
        return true;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void RaisePeak(ClassHeader* header, int rented)
    {
        int peak = Volatile.Read(ref header->Peak);
        while (rented > peak)
        {
            int seen = Interlocked.CompareExchange(ref header->Peak, rented, peak);
            if (seen == peak)
                return;
            peak = seen;
        }
    }

    public void Return(in BufferLeaseV1 lease)
    {
        ref SizeClass c = ref ResolveClass(in lease);
        ClassHeader* header = c.Header;
        int index = lease.BlockIndex;
        BlockMeta* block = (BlockMeta*)(header + 1) + index;

        if (_validateLeases)
        {
            if (block->Generation != lease.Generation)
                throw new InvalidOperationException("stale lease");
            if (Interlocked.Exchange(ref block->State, StateFree) != StateRented)
                throw new InvalidOperationException("double return");
        }

        Interlocked.Decrement(ref header->Rented);

        long head = Volatile.Read(ref header->Head);
        while (true)
        {
            block->Next = (int)head;
            long newHead = (long)((((ulong)head + TagIncrement) & TagMask) | (uint)index);
            long seen = Interlocked.CompareExchange(ref header->Head, newHead, head);
            if (seen == head)
                return;
            head = seen;
        }
    }

    public Span<byte> GetSpan(in BufferLeaseV1 lease)
    {
        ref SizeClass c = ref ResolveClass(in lease);
        return new Span<byte>(c.Base + lease.Offset, lease.Length);
    }

    public byte* GetPointer(in BufferLeaseV1 lease) => ResolveClass(in lease).Base + lease.Offset;

    public (int Rented, int Peak, long Exhaustions) GetClassStatistics(int classIndex)
    {
        ClassHeader* h = _classes[classIndex].Header;
        return (Volatile.Read(ref h->Rented), Volatile.Read(ref h->Peak), Volatile.Read(ref h->Exhaustions));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ref SizeClass ResolveClass(in BufferLeaseV1 lease)
    {
        SizeClass[] classes = _classes;
        int ci = lease.ClassIndex;
        if ((uint)ci < (uint)classes.Length)
        {
            ref SizeClass c = ref classes[ci];
            if ((uint)lease.BlockIndex < (uint)c.BlockCount
                && lease.Length == c.BlockSize
                && (uint)lease.Offset <= (uint)c.MaxOffset)
            {
                return ref c;
            }
        }

        ObjectDisposedException.ThrowIf(_disposed, this);
        throw new ArgumentException("Invalid lease.", nameof(lease));
    }

    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;
        SizeClass[] classes = _classes;
        _classes = [];
        for (int ci = 0; ci < classes.Length; ci++)
        {
            NativeMemory.AlignedFree(classes[ci].Base);
            NativeMemory.AlignedFree(classes[ci].Header);
        }
    }
}
