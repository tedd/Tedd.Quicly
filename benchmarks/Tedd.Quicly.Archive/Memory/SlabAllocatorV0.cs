using System.Runtime.InteropServices;

namespace Tedd.Quicly.Archive.Memory;

/// <summary>Lease handed out by <see cref="SlabAllocatorV0"/>; same 16-byte layout as the shipping lease.</summary>
[StructLayout(LayoutKind.Sequential, Pack = 4, Size = 16)]
public readonly struct BufferLeaseV0
{
    internal BufferLeaseV0(byte classIndex, ushort generation, int blockIndex, int offset, int length)
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
/// V0 of the slab allocator (superseded by <c>Tedd.Quicly.Core.Memory.SlabAllocator</c>, see
/// docs/benchmarks/memory.md). Same native, size-classed slabs as the shipping version, but each class keeps a
/// plain array-backed stack of free block indices guarded by a <see langword="lock"/>. Kept so the lock-free
/// free list stays measurable against the simple version it replaced.
/// </summary>
public sealed unsafe class SlabAllocatorV0 : IDisposable
{
    private sealed class SizeClass
    {
        public byte* Base;
        public int BlockSize;
        public int BlockCount;
        public int[] FreeStack = [];
        public int Top;
        public ushort[] Generations = [];
        public int Rented;
        public int Peak;
        public long Exhaustions;
        public readonly Lock Gate = new();
    }

    private readonly SizeClass[] _classes;
    private bool _disposed;

    public SlabAllocatorV0(ReadOnlySpan<(int BlockSize, int BlockCount)> classes)
    {
        _classes = new SizeClass[classes.Length];
        for (int ci = 0; ci < classes.Length; ci++)
        {
            (int blockSize, int blockCount) = classes[ci];
            var c = new SizeClass
            {
                BlockSize = blockSize,
                BlockCount = blockCount,
                Base = (byte*)NativeMemory.AlignedAlloc((nuint)blockSize * (nuint)blockCount, 64),
                FreeStack = new int[blockCount],
                Generations = new ushort[blockCount],
                Top = blockCount,
            };
            for (int i = 0; i < blockCount; i++)
                c.FreeStack[i] = blockCount - 1 - i; // block 0 on top, like the shipping version
            _classes[ci] = c;
        }
    }

    public bool TryRent(int minimumLength, out BufferLeaseV0 lease)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(minimumLength);
        SizeClass[] classes = _classes;
        for (int ci = 0; ci < classes.Length; ci++)
        {
            SizeClass c = classes[ci];
            if (c.BlockSize < minimumLength)
                continue;

            lock (c.Gate)
            {
                if (c.Top == 0)
                {
                    c.Exhaustions++;
                    lease = default;
                    return false;
                }

                int index = c.FreeStack[--c.Top];
                ushort generation = ++c.Generations[index];
                c.Rented++;
                if (c.Rented > c.Peak)
                    c.Peak = c.Rented;
                lease = new BufferLeaseV0((byte)ci, generation, index, index * c.BlockSize, c.BlockSize);
                return true;
            }
        }

        ObjectDisposedException.ThrowIf(_disposed, this);
        lease = default;
        return false;
    }

    public void Return(in BufferLeaseV0 lease)
    {
        SizeClass c = Resolve(in lease);
        lock (c.Gate)
        {
            c.FreeStack[c.Top++] = lease.BlockIndex;
            c.Rented--;
        }
    }

    public Span<byte> GetSpan(in BufferLeaseV0 lease)
    {
        SizeClass c = Resolve(in lease);
        return new Span<byte>(c.Base + lease.Offset, lease.Length);
    }

    public byte* GetPointer(in BufferLeaseV0 lease) => Resolve(in lease).Base + lease.Offset;

    public (int Rented, int Peak, long Exhaustions) GetClassStatistics(int classIndex)
    {
        SizeClass c = _classes[classIndex];
        lock (c.Gate)
            return (c.Rented, c.Peak, c.Exhaustions);
    }

    private SizeClass Resolve(in BufferLeaseV0 lease)
    {
        SizeClass[] classes = _classes;
        int ci = lease.ClassIndex;
        if ((uint)ci < (uint)classes.Length)
        {
            SizeClass c = classes[ci];
            if ((uint)lease.BlockIndex < (uint)c.BlockCount && lease.Length == c.BlockSize)
                return c;
        }

        ObjectDisposedException.ThrowIf(_disposed, this);
        throw new ArgumentException("Invalid lease.", nameof(lease));
    }

    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;
        foreach (SizeClass c in _classes)
        {
            NativeMemory.AlignedFree(c.Base);
            c.Base = null;
            c.BlockCount = 0;
            c.Top = 0;
        }
    }
}
