using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Tedd.Quicly.Core.Memory;

/// <summary>
/// Pre-allocated, size-classed native memory pool. Every size class is one contiguous 64-byte aligned slab
/// (<see cref="NativeMemory.AlignedAlloc(nuint, nuint)"/>) split into equally sized blocks; blocks are handed
/// out as <see cref="BufferLease"/> values and recycled through lock-free per-class free lists.
/// </summary>
/// <remarks>
/// <para><b>Free lists.</b> Each class is split into <see cref="SlabAllocatorOptions.FreeListShards"/> shards,
/// each a Treiber stack of block indices: the head is a single 64-bit word holding a 32-bit ABA tag in the high
/// half and the top block index in the low half (all ones = empty), and each block's <c>Next</c> link lives in
/// a per-block metadata record next to its generation and state. Both rent (pop) and return (push) are one
/// <see cref="Interlocked.CompareExchange(ref long, long, long)"/> on the head; the tag is incremented on every
/// successful operation, so a thread that read the head, then lost the CPU while other threads popped and
/// re-pushed the same index, has its CAS rejected and retries (with exponential back-off) using fresh state.
/// A stack was chosen over a Vyukov MPMC ring because (1) it is LIFO, so the block most recently returned (and
/// therefore hottest in cache) is the next one rented, (2) it needs no per-slot sequence counters or two
/// cursors, so an operation touches the shard head line plus the block's own metadata line and nothing else,
/// and (3) rent and return from the same thread — the dominant pattern for send buffers — do not bounce
/// between separate producer and consumer cache lines.</para>
/// <para><b>Sharding.</b> A single stack per class collapses when several threads hammer its head line (see
/// docs/benchmarks/memory.md). Each thread is therefore assigned a shard round-robin the first time it rents;
/// it pops from that shard first and steals from the others only when its own is empty. A block always goes
/// back to the shard it came from (recorded in <see cref="BufferLease.Shard"/>), so every shard keeps an exact
/// rented count and peak on the same cache line as its head. Threads that only rent and threads that only
/// return therefore meet on one shard's line instead of all threads meeting on one line.</para>
/// <para><b>Thread safety.</b> <see cref="TryRent"/>, <see cref="Return"/>, <see cref="GetSpan"/>,
/// <see cref="GetPointer"/> and the statistics methods may be called concurrently from any threads.
/// <see cref="Dispose"/> must not run concurrently with any other call.</para>
/// <para><b>Memory contents.</b> Blocks are not cleared on rent or return; a freshly rented block contains
/// whatever the previous owner left there.</para>
/// <para><b>Exhaustion.</b> A class whose shards are all empty makes <see cref="TryRent"/> return
/// <see langword="false"/> and increments the class's exhaustion counter. The allocator never falls back to a
/// larger class and never allocates on the GC heap after construction.</para>
/// </remarks>
public sealed unsafe class SlabAllocator : IDisposable
{
    private const ulong TagIncrement = 1UL << 32;
    private const ulong TagMask = 0xFFFF_FFFF_0000_0000UL;
    private const byte StateFree = 0;
    private const byte StateRented = 1;
    private const int MaxBackoff = 32;

    /// <summary>Per-class cold metadata, one cache line.</summary>
    [StructLayout(LayoutKind.Explicit, Size = 64)]
    private struct ClassHeader
    {
        [FieldOffset(0)] public long Exhaustions;
    }

    /// <summary>
    /// Per-shard hot metadata: the free-list head on one cache line and the counters on the next. Keeping the
    /// counters off the head line means a CAS retry storm on the head does not also stall the counter
    /// increment of a thread that already won its pop (design choice, see docs/benchmarks/memory.md).
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 128)]
    private struct ShardHeader
    {
        [FieldOffset(0)] public long Head;
        [FieldOffset(8)] public int BlockCount;
        [FieldOffset(64)] public int Rented;
        [FieldOffset(68)] public int Peak;
    }

    /// <summary>Per-block metadata: free-list link, generation and rented/free state, one 8-byte record.</summary>
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
        public ShardHeader* Shards;
        public BlockMeta* Blocks;
        public int BlockSize;
        public int BlockCount;
        public int MaxOffset;
    }

    [ThreadStatic]
    private static int t_shardSlot;
    private static int s_nextShardSlot;

    private readonly SizeClassDefinition[] _definitions;
    private readonly bool _validateLeases;
    private readonly int _shardMask;
    private SizeClass[] _classes;
    private bool _disposed;

    /// <summary>Creates an allocator and reserves all slabs up front.</summary>
    /// <param name="options">Size classes, sharding and validation mode; <see langword="null"/> uses <see cref="SlabAllocatorOptions"/> defaults.</param>
    /// <exception cref="ArgumentException">The options are invalid.</exception>
    /// <exception cref="OutOfMemoryException">Native memory could not be reserved.</exception>
    public SlabAllocator(SlabAllocatorOptions? options = null)
    {
        options ??= new SlabAllocatorOptions();
        _definitions = options.ValidateAndCopyClasses();
        _validateLeases = options.ValidateLeases;
        int shardCount = options.FreeListShards;
        _shardMask = shardCount - 1;
        _classes = new SizeClass[_definitions.Length];
        try
        {
            for (int ci = 0; ci < _definitions.Length; ci++)
            {
                SizeClassDefinition def = _definitions[ci];
                ref SizeClass c = ref _classes[ci];
                c.BlockSize = def.BlockSize;
                c.BlockCount = def.BlockCount;
                c.MaxOffset = def.BlockSize * (def.BlockCount - 1);
                c.Base = (byte*)NativeMemory.AlignedAlloc((nuint)def.SlabBytes, SlabAllocatorOptions.BlockAlignment);

                nuint metaBytes = (nuint)sizeof(ClassHeader) + (nuint)shardCount * (nuint)sizeof(ShardHeader) + (nuint)def.BlockCount * (nuint)sizeof(BlockMeta);
                c.Header = (ClassHeader*)NativeMemory.AlignedAlloc(metaBytes, SlabAllocatorOptions.BlockAlignment);
                NativeMemory.Clear(c.Header, metaBytes);
                c.Shards = (ShardHeader*)(c.Header + 1);
                c.Blocks = (BlockMeta*)(c.Shards + shardCount);

                // Contiguous block ranges per shard so that threads on different shards touch different metadata lines.
                for (int s = 0; s < shardCount; s++)
                {
                    int first = (int)((long)def.BlockCount * s / shardCount);
                    int end = (int)((long)def.BlockCount * (s + 1) / shardCount);
                    ShardHeader* shard = c.Shards + s;
                    shard->BlockCount = end - first;
                    if (end == first)
                    {
                        shard->Head = (long)uint.MaxValue; // tag 0, empty
                        continue;
                    }

                    for (int i = first; i < end - 1; i++)
                        c.Blocks[i].Next = i + 1;
                    c.Blocks[end - 1].Next = -1;
                    shard->Head = (long)(uint)first; // tag 0, top = first block of the range
                }
            }
        }
        catch
        {
            FreeNative(_classes);
            throw;
        }
    }

    /// <summary>Frees the native slabs if <see cref="Dispose"/> was never called.</summary>
    ~SlabAllocator()
    {
        FreeNative(_classes);
    }

    /// <summary>Number of size classes.</summary>
    public int ClassCount => _definitions.Length;

    /// <summary>Number of free-list shards per class (<see cref="SlabAllocatorOptions.FreeListShards"/>).</summary>
    public int ShardCount => _shardMask + 1;

    /// <summary>The validated size classes, in increasing block size.</summary>
    public ReadOnlySpan<SizeClassDefinition> SizeClasses => _definitions;

    /// <summary>Largest block size available; <see cref="TryRent"/> fails for anything larger.</summary>
    public int MaxBlockSize => _definitions[^1].BlockSize;

    /// <summary>Whether double returns and stale leases are detected (see <see cref="SlabAllocatorOptions.ValidateLeases"/>).</summary>
    public bool ValidateLeases => _validateLeases;

    /// <summary>
    /// True once <see cref="Dispose"/> has run: every lease is invalid and <see cref="Return"/> throws
    /// <see cref="ObjectDisposedException"/>. Lets an owner that may outlive the allocator (a lease released after its peer
    /// and the pool are gone) skip the return. A plain read: it is only meaningful to a caller that does not race
    /// <see cref="Dispose"/>, as for every other member.
    /// </summary>
    public bool IsDisposed => _disposed;

    /// <summary>
    /// Rents the smallest block that holds at least <paramref name="minimumLength"/> bytes.
    /// </summary>
    /// <param name="minimumLength">Minimum usable length in bytes (zero rents a block of the smallest class).</param>
    /// <param name="lease">The rented block, or <see cref="BufferLease.Empty"/> on failure.</param>
    /// <returns>
    /// <see langword="false"/> when <paramref name="minimumLength"/> exceeds <see cref="MaxBlockSize"/> (no
    /// counter is touched) or when the matching class has no free block (its exhaustion counter is incremented).
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="minimumLength"/> is negative.</exception>
    /// <exception cref="ObjectDisposedException">The allocator has been disposed.</exception>
    public bool TryRent(int minimumLength, out BufferLease lease)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(minimumLength);
        SizeClass[] classes = _classes;
        for (int ci = 0; ci < classes.Length; ci++)
        {
            if (classes[ci].BlockSize >= minimumLength)
                return TryRentFromClass(classes, ci, out lease);
        }

        return RentTooLarge(out lease);
    }

    /// <summary>
    /// Fast path: one pop attempt on the calling thread's own shard, straight-line code with no loops so the
    /// JIT keeps everything in registers. A failed CAS or an empty shard goes to <see cref="TryRentSlow"/>.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool TryRentFromClass(SizeClass[] classes, int ci, out BufferLease lease)
    {
        ref SizeClass c = ref classes[ci];
        int s = CurrentShardSlot() & _shardMask;
        ShardHeader* shard = c.Shards + s;
        BlockMeta* blocks = c.Blocks;

        long head = Volatile.Read(ref shard->Head);
        int index = (int)head;
        if (index >= 0)
        {
            int next = blocks[index].Next;
            long newHead = (long)((((ulong)head + TagIncrement) & TagMask) | (uint)next);
            if (Interlocked.CompareExchange(ref shard->Head, newHead, head) == head)
                return CompleteRent(ref c, ci, s, shard, blocks + index, index, out lease);
        }

        return TryRentSlow(ref c, ci, s, out lease);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool CompleteRent(ref SizeClass c, int ci, int s, ShardHeader* shard, BlockMeta* block, int index, out BufferLease lease)
    {
        ushort generation = (ushort)(block->Generation + 1);
        block->Generation = generation;
        if (_validateLeases)
            block->State = StateRented;

        int rented = Interlocked.Increment(ref shard->Rented);
        if (rented > Volatile.Read(ref shard->Peak))
            RaisePeak(shard, rented);

        int blockSize = c.BlockSize;
        lease = new BufferLease((byte)ci, (byte)s, generation, index, index * blockSize, blockSize);
        return true;
    }

    /// <summary>Slow path: retry the own shard with back-off, then steal from the other shards in order.</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private bool TryRentSlow(ref SizeClass c, int ci, int start, out BufferLease lease)
    {
        int mask = _shardMask;
        BlockMeta* blocks = c.Blocks;
        for (int n = 0; n <= mask; n++)
        {
            int s = (start + n) & mask;
            ShardHeader* shard = c.Shards + s;
            if (TryPop(shard, blocks, out int index))
                return CompleteRent(ref c, ci, s, shard, blocks + index, index, out lease);
        }

        return RentExhausted(c.Header, out lease);
    }

    private static bool TryPop(ShardHeader* shard, BlockMeta* blocks, out int index)
    {
        long head = Volatile.Read(ref shard->Head);
        int backoff = 1;
        while (true)
        {
            index = (int)head;
            if (index < 0)
                return false;

            int next = blocks[index].Next;
            long newHead = (long)((((ulong)head + TagIncrement) & TagMask) | (uint)next);
            long seen = Interlocked.CompareExchange(ref shard->Head, newHead, head);
            if (seen == head)
                return true;
            head = seen;
            Backoff(ref backoff);
        }
    }

    /// <summary>Slow path of <see cref="Return"/>: retry the push with back-off after a lost CAS.</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void PushSlow(ShardHeader* shard, BlockMeta* block, int index, long head)
    {
        int backoff = 1;
        while (true)
        {
            Backoff(ref backoff);
            block->Next = (int)head;
            long newHead = (long)((((ulong)head + TagIncrement) & TagMask) | (uint)index);
            long seen = Interlocked.CompareExchange(ref shard->Head, newHead, head);
            if (seen == head)
                return;
            head = seen;
        }
    }

    private static void Backoff(ref int backoff)
    {
        Thread.SpinWait(backoff);
        if (backoff < MaxBackoff)
            backoff <<= 1;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int CurrentShardSlot()
    {
        int slot = t_shardSlot;
        return slot != 0 ? slot : AssignShardSlot();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static int AssignShardSlot()
    {
        int slot = Interlocked.Increment(ref s_nextShardSlot);
        t_shardSlot = slot;
        return slot;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private bool RentExhausted(ClassHeader* header, out BufferLease lease)
    {
        Interlocked.Increment(ref header->Exhaustions);
        lease = default;
        return false;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private bool RentTooLarge(out BufferLease lease)
    {
        ThrowIfDisposed();
        lease = default;
        return false;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void RaisePeak(ShardHeader* shard, int rented)
    {
        int peak = Volatile.Read(ref shard->Peak);
        while (rented > peak)
        {
            int seen = Interlocked.CompareExchange(ref shard->Peak, rented, peak);
            if (seen == peak)
                return;
            peak = seen;
        }
    }

    /// <summary>Returns a rented block to the shard it came from so it can be rented again.</summary>
    /// <param name="lease">A lease obtained from <see cref="TryRent"/> on this allocator.</param>
    /// <exception cref="ArgumentException">The lease is empty or does not address a block of this allocator.</exception>
    /// <exception cref="InvalidOperationException">
    /// Lease validation is enabled and the block is not currently rented (double return) or the lease's
    /// generation does not match the block (stale lease).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The allocator has been disposed.</exception>
    public void Return(in BufferLease lease)
    {
        ref SizeClass c = ref ResolveClass(in lease);
        int index = lease.BlockIndex;
        BlockMeta* block = c.Blocks + index;
        ShardHeader* shard = c.Shards + lease.Shard;

        if (_validateLeases)
        {
            if (block->Generation != lease.Generation)
                ThrowStaleLease(in lease, block->Generation);
            if (Interlocked.Exchange(ref block->State, StateFree) != StateRented)
                ThrowDoubleReturn(in lease);
        }

        Interlocked.Decrement(ref shard->Rented);

        // Fast path: one push attempt; a lost CAS continues in PushSlow with back-off.
        long head = Volatile.Read(ref shard->Head);
        block->Next = (int)head;
        long newHead = (long)((((ulong)head + TagIncrement) & TagMask) | (uint)index);
        long seen = Interlocked.CompareExchange(ref shard->Head, newHead, head);
        if (seen != head)
            PushSlow(shard, block, index, seen);
    }

    /// <summary>
    /// Returns several rented blocks, as <see cref="Return"/> does for each of them, with one push per run of consecutive
    /// leases of the same class and shard: the run is linked into a chain through the blocks' own free-list links and
    /// pushed with a single compare-exchange, and the shard's rented count drops by the run's length in one atomic add.
    /// Blocks rented by one thread from one class — a send path's payloads — form one run.
    /// </summary>
    /// <param name="leases">Leases obtained from this allocator, each returned exactly once (no duplicates).</param>
    /// <exception cref="ArgumentException">A lease is empty or does not address a block of this allocator; the leases before its run were returned, the leases of its run were not (they are still rented).</exception>
    /// <exception cref="InvalidOperationException">Lease validation is enabled and a lease is stale or its block is not rented; the leases before its run were returned, the leases of its run were not (they are still rented).</exception>
    /// <exception cref="ObjectDisposedException">The allocator has been disposed.</exception>
    public void ReturnMany(ReadOnlySpan<BufferLease> leases)
    {
        int i = 0;
        while (i < leases.Length)
        {
            ref readonly BufferLease first = ref leases[i];
            ref SizeClass c = ref ResolveClass(in first);
            int ci = first.ClassIndex;
            int s = first.Shard;
            BlockMeta* blocks = c.Blocks;
            int end = i + 1;
            while (end < leases.Length && leases[end].ClassIndex == ci && leases[end].Shard == s)
            {
                ResolveClass(in leases[end]);
                end++;
            }

            if (_validateLeases)
            {
                ValidateRun(blocks, leases[i..end]);
            }

            // Link the run top-down in the order given; the last block will point at the current head.
            int top = first.BlockIndex;
            int last = top;
            for (int k = i + 1; k < end; k++)
            {
                int index = leases[k].BlockIndex;
                blocks[last].Next = index;
                last = index;
            }

            ShardHeader* shard = c.Shards + s;
            Interlocked.Add(ref shard->Rented, i - end);
            long head = Volatile.Read(ref shard->Head);
            BlockMeta* tail = blocks + last;
            tail->Next = (int)head;
            long newHead = (long)((((ulong)head + TagIncrement) & TagMask) | (uint)top);
            long seen = Interlocked.CompareExchange(ref shard->Head, newHead, head);
            if (seen != head)
                PushChainSlow(shard, tail, top, seen);
            i = end;
        }
    }

    // Validates a run and marks its blocks free, all or nothing: when a lease is stale or its block is not rented (a double
    // return, or the same lease twice in the run), the blocks of the run marked free before it are marked rented again
    // before the exception, so every lease of the rejected run is still rented and can be returned later.
    private static void ValidateRun(BlockMeta* blocks, ReadOnlySpan<BufferLease> run)
    {
        for (int k = 0; k < run.Length; k++)
        {
            ref readonly BufferLease lease = ref run[k];
            BlockMeta* block = blocks + lease.BlockIndex;
            if (block->Generation != lease.Generation)
                RejectRun(blocks, run[..k], in lease, stale: true, block->Generation);
            if (Interlocked.Exchange(ref block->State, StateFree) != StateRented)
                RejectRun(blocks, run[..k], in lease, stale: false, block->Generation);
        }
    }

    /// <summary>Slow path of <see cref="ValidateRun"/>: marks the blocks of <paramref name="validated"/> rented again, then throws for <paramref name="lease"/>.</summary>
    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void RejectRun(BlockMeta* blocks, ReadOnlySpan<BufferLease> validated, in BufferLease lease, bool stale, ushort currentGeneration)
    {
        foreach (ref readonly BufferLease done in validated)
            Volatile.Write(ref blocks[done.BlockIndex].State, StateRented);
        if (stale)
            ThrowStaleLease(in lease, currentGeneration);
        ThrowDoubleReturn(in lease);
    }

    /// <summary>Slow path of <see cref="ReturnMany"/>: retry pushing the chain with back-off after a lost CAS.</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void PushChainSlow(ShardHeader* shard, BlockMeta* tail, int top, long head)
    {
        int backoff = 1;
        while (true)
        {
            Backoff(ref backoff);
            tail->Next = (int)head;
            long newHead = (long)((((ulong)head + TagIncrement) & TagMask) | (uint)top);
            long seen = Interlocked.CompareExchange(ref shard->Head, newHead, head);
            if (seen == head)
                return;
            head = seen;
        }
    }

    /// <summary>The block's bytes as a span of <see cref="BufferLease.Length"/> bytes.</summary>
    /// <exception cref="ArgumentException">The lease is empty or does not address a block of this allocator.</exception>
    /// <exception cref="ObjectDisposedException">The allocator has been disposed.</exception>
    public Span<byte> GetSpan(in BufferLease lease)
    {
        ref SizeClass c = ref ResolveClass(in lease);
        return new Span<byte>(c.Base + lease.Offset, lease.Length);
    }

    /// <summary>Pointer to the first byte of the block; valid until the lease is returned.</summary>
    /// <exception cref="ArgumentException">The lease is empty or does not address a block of this allocator.</exception>
    /// <exception cref="ObjectDisposedException">The allocator has been disposed.</exception>
    public byte* GetPointer(in BufferLease lease)
    {
        ref SizeClass c = ref ResolveClass(in lease);
        return c.Base + lease.Offset;
    }

    /// <summary>Validates that <paramref name="lease"/> addresses a block of this allocator (bounds only, not ownership).</summary>
    /// <exception cref="ArgumentException">The lease is empty or does not address a block of this allocator.</exception>
    /// <exception cref="ObjectDisposedException">The allocator has been disposed.</exception>
    public void ValidateBounds(in BufferLease lease) => ResolveClass(in lease);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ref SizeClass ResolveClass(in BufferLease lease)
    {
        SizeClass[] classes = _classes;
        int ci = lease.ClassIndex;
        if ((uint)ci < (uint)classes.Length)
        {
            ref SizeClass c = ref classes[ci];
            if ((uint)lease.BlockIndex < (uint)c.BlockCount
                && lease.Length == c.BlockSize
                && (uint)lease.Offset <= (uint)c.MaxOffset
                && lease.Shard <= _shardMask)
            {
                return ref c;
            }
        }

        ThrowInvalidLease(in lease);
        return ref Unsafe.NullRef<SizeClass>();
    }

    /// <summary>
    /// Snapshot of every size class. Allocation-free; the snapshot is not atomic across classes or shards.
    /// <see cref="SizeClassStatistics.Peak"/> is the sum of the per-shard peaks: exact when the class is used
    /// from one thread, otherwise an upper bound on the true concurrent maximum.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The allocator has been disposed.</exception>
    public SlabStatistics GetStatistics()
    {
        ThrowIfDisposed();
        SlabStatistics stats = default;
        for (int ci = 0; ci < _classes.Length; ci++)
            stats.Add(ReadClassStatistics(ci));
        return stats;
    }

    /// <summary>Snapshot of one size class (see <see cref="GetStatistics"/> for the meaning of <see cref="SizeClassStatistics.Peak"/>).</summary>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="classIndex"/> is not a valid class index.</exception>
    /// <exception cref="ObjectDisposedException">The allocator has been disposed.</exception>
    public SizeClassStatistics GetClassStatistics(int classIndex)
    {
        ThrowIfDisposed();
        if ((uint)classIndex >= (uint)_classes.Length)
            throw new ArgumentOutOfRangeException(nameof(classIndex));
        return ReadClassStatistics(classIndex);
    }

    private SizeClassStatistics ReadClassStatistics(int ci)
    {
        ref SizeClass c = ref _classes[ci];
        int rented = 0;
        int peak = 0;
        for (int s = 0; s <= _shardMask; s++)
        {
            ShardHeader* shard = c.Shards + s;
            rented += Volatile.Read(ref shard->Rented);
            peak += Volatile.Read(ref shard->Peak);
        }

        return new SizeClassStatistics(c.BlockSize, c.BlockCount, rented, peak, Volatile.Read(ref c.Header->Exhaustions));
    }

    /// <summary>
    /// Frees all native memory. Every outstanding lease becomes invalid; every later call on the hot path
    /// fails with <see cref="ObjectDisposedException"/> (or <see cref="ArgumentException"/> for leases) via
    /// its slow path. Idempotent. Must not run concurrently with other calls.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;
        SizeClass[] classes = _classes;
        _classes = [];
        FreeNative(classes);
        GC.SuppressFinalize(this);
    }

    private static void FreeNative(SizeClass[]? classes)
    {
        if (classes is null)
            return; // the constructor threw while validating options; nothing was reserved
        for (int ci = 0; ci < classes.Length; ci++)
        {
            ref SizeClass c = ref classes[ci];
            if (c.Base is not null)
            {
                NativeMemory.AlignedFree(c.Base);
                c.Base = null;
            }

            if (c.Header is not null)
            {
                NativeMemory.AlignedFree(c.Header);
                c.Header = null;
                c.Shards = null;
                c.Blocks = null;
            }
        }
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }

    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    private void ThrowInvalidLease(in BufferLease lease)
    {
        ThrowIfDisposed();
        if (lease.IsEmpty)
            throw new ArgumentException("The lease is empty.", nameof(lease));
        throw new ArgumentException($"{lease} does not address a block of this allocator.", nameof(lease));
    }

    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowStaleLease(in BufferLease lease, ushort currentGeneration) =>
        throw new InvalidOperationException($"{lease} is stale: the block is at generation {currentGeneration}.");

    [DoesNotReturn]
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ThrowDoubleReturn(in BufferLease lease) =>
        throw new InvalidOperationException($"{lease} is not rented; it was returned twice.");
}
