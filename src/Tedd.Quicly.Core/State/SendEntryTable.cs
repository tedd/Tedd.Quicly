using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Runtime.CompilerServices;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.State;

/// <summary>
/// Pre-allocated table of <see cref="SendEntry"/> slots (hot, one cache line each, native memory) plus the cold
/// per-slot side tables, implementing the slot protocol of ADR 0008: allocate → fill → publish → transport →
/// completion → free.
/// </summary>
/// <remarks>
/// <para><b>Threads.</b> <see cref="TryAllocate"/>, <see cref="Publish"/>, <see cref="Discard"/>, <see cref="Free"/>,
/// the header/batch helpers and every cold array belong to the owner (game) thread. <see cref="TryTransition"/>,
/// <see cref="TryTransitionContext"/>, <see cref="TryResolveContext"/> and <see cref="GetState"/> may be called
/// from any thread (typically the transport thread inside a completion callback). The free list is owned by the
/// game thread alone, so allocating and freeing never need an interlocked operation.</para>
/// <para><b>State word.</b> <see cref="SendEntry.State"/> (offset 0) and <see cref="SendEntry.Generation"/>
/// (offset 4) form one naturally aligned 64-bit word, <c>(generation &lt;&lt; 32) | state</c>. It is the only
/// memory written by both threads, and every write to it is a 64-bit volatile store (owner) or a 64-bit
/// compare-exchange (either thread). Reading it with one 64-bit load gives a consistent (generation, state) pair,
/// and <see cref="TryTransitionContext"/> checks the generation <em>inside</em> its compare-exchange, so a
/// completion carrying the context of a previous occupant can never move the state of the slot's current
/// occupant (no ABA window between "validate generation" and "change state").</para>
/// <para><b>Contexts.</b> <see cref="MakeContext"/> packs <c>(generation &lt;&lt; 32) | slot</c>. Generations are
/// odd and bumped on every allocation, so a context is never zero and a stale one fails validation; callers count
/// and ignore it (ADR 0008 invariant 2).</para>
/// <para><b>Cold side tables</b> (structure of arrays, indexed by slot, game thread): <see cref="Leases"/> (payload
/// block to return on completion), <see cref="Keys"/>, <see cref="Sequences"/>, <see cref="Contexts"/> (the
/// transport context), <see cref="Deadlines"/> (expiry in clock micros, scanned every flush), <see cref="Next"/>
/// (intrusive link: channel queue while queued, then container membership once packed),
/// <see cref="BatchHead"/>/<see cref="BatchCount"/> (members of a container entry) and <see cref="PinHandles"/>
/// (a managed <see cref="nint"/> array for the pin handles of the <c>SendBorrowed</c> convenience path,
/// ADR 0008 invariant 11).</para>
/// <para><b>Header blocks.</b> The encoded header of each slot lives in a cold native array of
/// <see cref="HeaderBlockSize"/>-byte blocks (<see cref="GetHeaderBlock"/>), large enough for the 24-byte maximum
/// datagram header; <see cref="SetHeader"/> points <see cref="SendEntry.Header"/> at it. The block never moves and
/// is reused only after the slot is freed, so it satisfies ADR 0008 invariant 1.</para>
/// <para><b>Gathers.</b> One entry's <see cref="SendEntry.Header"/>/<see cref="SendEntry.Payload"/> pair is a
/// contiguous <c>QUIC_BUFFER[2]</c> (<see cref="GetSegments"/>). Consecutive entries are <em>not</em> one contiguous
/// segment array (each pair is followed by the entry's two scratch words); a multi-entry stream gather copies the pairs
/// into a per-submission contiguous array taken from a <see cref="SegmentArena"/>.</para>
/// </remarks>
public sealed unsafe class SendEntryTable : IDisposable
{
    /// <summary>Size of one slot's header block in bytes (covers the 24-byte maximum datagram header).</summary>
    public const int HeaderBlockSize = 32;

    private const long GenerationMask = unchecked((long)0xFFFF_FFFF_0000_0000UL);

    private readonly NativeArray<SendEntry> _entries;
    private readonly NativeArray<byte> _headerBlocks;
    private readonly NativeArray<int> _freeStack;
    private int _freeCount;
    private bool _disposed;

    /// <summary>Creates a table with at least <paramref name="minimumCapacity"/> slots (rounded up to a power of two).</summary>
    /// <param name="minimumCapacity">Requested number of slots (1 … 2^30).</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="minimumCapacity"/> is not positive or exceeds 2^30.</exception>
    public SendEntryTable(int minimumCapacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(minimumCapacity, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(minimumCapacity, 1 << 30);
        int capacity = (int)BitOperations.RoundUpToPowerOf2((uint)minimumCapacity);

        _entries = new NativeArray<SendEntry>(capacity);
        _headerBlocks = new NativeArray<byte>(capacity * HeaderBlockSize);
        _freeStack = new NativeArray<int>(capacity);
        Leases = new NativeArray<BufferLease>(capacity);
        Keys = new NativeArray<ulong>(capacity);
        Sequences = new NativeArray<uint>(capacity);
        Contexts = new NativeArray<ulong>(capacity);
        Deadlines = new NativeArray<long>(capacity);
        Next = new NativeArray<int>(capacity);
        BatchHead = new NativeArray<int>(capacity);
        BatchCount = new NativeArray<int>(capacity);
        PinHandles = new nint[capacity];

        Next.Fill(-1);
        BatchHead.Fill(-1);
        // Push in reverse so that slot 0 is handed out first.
        int* stack = _freeStack.Pointer;
        for (int i = capacity - 1; i >= 0; i--)
            stack[_freeCount++] = i;
    }

    /// <summary>Number of slots; a power of two.</summary>
    public int Capacity => _entries.Length;

    /// <summary>Number of allocated slots (any state other than <see cref="SendEntryState.Free"/>).</summary>
    public int Count => _entries.Length - _freeCount;

    /// <summary>Number of slots <see cref="TryAllocate"/> can still hand out.</summary>
    public int Available => _freeCount;

    /// <summary>The hot entries, indexed by slot.</summary>
    public NativeArray<SendEntry> Entries => _entries;

    /// <summary>Payload lease to return when the entry completes (<see cref="BufferLease.Empty"/> when the payload is not slab memory).</summary>
    public NativeArray<BufferLease> Leases { get; }

    /// <summary>Message key (keyed channels).</summary>
    public NativeArray<ulong> Keys { get; }

    /// <summary>Message sequence number.</summary>
    public NativeArray<uint> Sequences { get; }

    /// <summary>Transport context of the entry (<see cref="MakeContext"/>), stored at allocation.</summary>
    public NativeArray<ulong> Contexts { get; }

    /// <summary>Expiry deadline in clock microseconds (0 = none). Scanned structure-of-arrays every flush.</summary>
    public NativeArray<long> Deadlines { get; }

    /// <summary>Intrusive singly linked list link (-1 = end): channel queue while queued, container membership once packed.</summary>
    public NativeArray<int> Next { get; }

    /// <summary>For a container entry: slot of its first member (-1 = none). Members chain through <see cref="Next"/>.</summary>
    public NativeArray<int> BatchHead { get; }

    /// <summary>For a container entry: number of members.</summary>
    public NativeArray<int> BatchCount { get; }

    /// <summary>Opaque pin handle per slot for <see cref="SendEntryFlags.Pinned"/> entries (0 = none). Managed, cold.</summary>
    public nint[] PinHandles { get; }

    /// <summary>Reference to the hot entry of <paramref name="slot"/> (bounds-checked).</summary>
    /// <param name="slot">Slot index.</param>
    public ref SendEntry this[int slot]
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => ref _entries[slot];
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ref long StateWord(int slot) => ref Unsafe.As<int, long>(ref _entries[slot].State);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long Word(uint generation, SendEntryState state) => (long)(((ulong)generation << 32) | (uint)state);

    /// <summary>
    /// Takes a free slot, gives it a new odd generation and the state <see cref="SendEntryState.Filling"/>, and
    /// resets its hot fields and cold side-table entries. Owner thread only.
    /// </summary>
    /// <param name="slot">The allocated slot, or -1 when the table is exhausted.</param>
    /// <returns><see langword="false"/> when every slot is in use.</returns>
    /// <remarks>
    /// Inlined into <see cref="Session.PeerCore.TryAllocateEntry"/> (and from there into the engines' admission), so the slot
    /// stays in a register instead of being written back through the <see langword="out"/> parameter and reloaded per use.
    /// </remarks>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryAllocate(out int slot)
    {
        if (_freeCount == 0)
        {
            slot = -1;
            return false;
        }

        int s = _freeStack.Pointer[--_freeCount];
        slot = s;
        SendEntry* e = _entries.Pointer + s;
        Debug.Assert(e->State == (int)SendEntryState.Free, "slot on the free list is not free");

        // Generations are always odd and never zero: 0 → 1 → 3 → … → 0xFFFFFFFF → 1.
        uint generation = (e->Generation + 1) | 1;
        e->Channel = 0;
        e->Flags = SendEntryFlags.None;
        e->HeaderLength = 0;
        e->Header = default;
        e->Payload = default;
        e->Aux0 = 0;
        e->Aux1 = 0;

        Leases.Pointer[s] = BufferLease.Empty;
        Keys.Pointer[s] = 0;
        Sequences.Pointer[s] = 0;
        Contexts.Pointer[s] = ((ulong)generation << 32) | (uint)s;
        Deadlines.Pointer[s] = 0;
        Next.Pointer[s] = -1;
        BatchHead.Pointer[s] = -1;
        BatchCount.Pointer[s] = 0;
        PinHandles[s] = 0;

        // Generation and state change together, last: a reader that sees Filling sees the new generation.
        Volatile.Write(ref *(long*)e, Word(generation, SendEntryState.Filling));
        return true;
    }

    /// <summary>
    /// Copies <paramref name="header"/> into the slot's header block and points <see cref="SendEntry.Header"/> at it.
    /// Owner thread, while <see cref="SendEntryState.Filling"/>.
    /// </summary>
    /// <param name="slot">Slot index.</param>
    /// <param name="header">Encoded header, at most <see cref="HeaderBlockSize"/> bytes.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="header"/> does not fit the block, or <paramref name="slot"/> is outside the table.</exception>
    public void SetHeader(int slot, ReadOnlySpan<byte> header)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(header.Length, HeaderBlockSize);
        SendEntry* e = (SendEntry*)Unsafe.AsPointer(ref _entries[slot]);
        byte* block = _headerBlocks.Pointer + ((nint)slot * HeaderBlockSize);
        header.CopyTo(new Span<byte>(block, HeaderBlockSize));
        e->HeaderLength = (byte)header.Length;
        e->Header = new TransportSegment(block, header.Length);
    }

    /// <summary>
    /// Points <see cref="SendEntry.Header"/> at the first <paramref name="length"/> bytes of the slot's header block,
    /// after the caller encoded the header in place through <see cref="GetHeaderBlock"/>. Owner thread.
    /// </summary>
    /// <param name="slot">Slot index.</param>
    /// <param name="length">Encoded header length, 0 … <see cref="HeaderBlockSize"/>.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="length"/> or <paramref name="slot"/> is out of range.</exception>
    public void SetHeaderLength(int slot, int length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(length, HeaderBlockSize);
        SendEntry* e = (SendEntry*)Unsafe.AsPointer(ref _entries[slot]);
        e->HeaderLength = (byte)length;
        e->Header = new TransportSegment(_headerBlocks.Pointer + ((nint)slot * HeaderBlockSize), length);
    }

    /// <summary>
    /// The <see cref="HeaderBlockSize"/>-byte header block of <paramref name="slot"/> as a writable span (for encoding a
    /// header in place; then call <see cref="SetHeaderLength"/>). The block lives in native memory and never moves.
    /// </summary>
    /// <param name="slot">Slot index.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="slot"/> is outside the table.</exception>
    public Span<byte> GetHeaderBlock(int slot)
    {
        if ((uint)slot >= (uint)_entries.Length)
            ThrowSlotOutOfRange(slot);
        return new Span<byte>(_headerBlocks.Pointer + ((nint)slot * HeaderBlockSize), HeaderBlockSize);
    }

    /// <summary>
    /// Pointer to the entry's <see cref="SendEntry.Header"/>, which together with the adjacent
    /// <see cref="SendEntry.Payload"/> is the contiguous <c>QUIC_BUFFER[2]</c> for one transport call. The pointer
    /// covers exactly two segments; consecutive slots are not contiguous segment arrays (see the class remarks).
    /// </summary>
    /// <param name="slot">Slot index.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="slot"/> is outside the table.</exception>
    public TransportSegment* GetSegments(int slot) => (TransportSegment*)Unsafe.AsPointer(ref _entries[slot].Header);

    /// <summary>
    /// Makes the entry visible to the transport thread: the state becomes <see cref="SendEntryState.InFlight"/>
    /// with release semantics, so every field written before this call is visible to a thread that observes the
    /// new state. Owner thread only; the entry must be <see cref="SendEntryState.Filling"/>.
    /// </summary>
    /// <param name="slot">Slot index.</param>
    /// <exception cref="InvalidOperationException">The entry is not <see cref="SendEntryState.Filling"/>.</exception>
    public void Publish(int slot)
    {
        ref long word = ref StateWord(slot);
        long current = word;
        if ((int)current != (int)SendEntryState.Filling)
            ThrowWrongState(slot, (SendEntryState)(int)current, SendEntryState.Filling);
        Volatile.Write(ref word, (current & GenerationMask) | (uint)SendEntryState.InFlight);
    }

    /// <summary>
    /// Atomically moves the entry of <paramref name="slot"/> from <paramref name="from"/> to <paramref name="to"/>,
    /// whatever its generation. Any thread. Use it where the slot is known to be the right occupant (the owner's
    /// InFlight → Cancelling); a callback holding a context should use <see cref="TryTransitionContext"/>.
    /// </summary>
    /// <param name="slot">Slot index.</param>
    /// <param name="from">Expected current state.</param>
    /// <param name="to">New state.</param>
    /// <returns><see langword="false"/> when the state was not <paramref name="from"/>; nothing changes.</returns>
    public bool TryTransition(int slot, SendEntryState from, SendEntryState to)
    {
        ref long word = ref StateWord(slot);
        long current = Volatile.Read(ref word);
        while ((int)current == (int)from)
        {
            long seen = Interlocked.CompareExchange(ref word, (current & GenerationMask) | (uint)to, current);
            if (seen == current)
                return true;
            current = seen;
        }

        return false;
    }

    /// <summary>
    /// Atomically moves the entry named by <paramref name="context"/> from <paramref name="from"/> to
    /// <paramref name="to"/>, provided the slot still carries the context's generation. The generation is part
    /// of the compare-exchange, so a stale context can never change a reused slot. Any thread (typically the
    /// transport thread: InFlight → Completed, Cancelling → Completed).
    /// </summary>
    /// <param name="context">Context produced by <see cref="MakeContext"/>.</param>
    /// <param name="from">Expected current state.</param>
    /// <param name="to">New state.</param>
    /// <param name="slot">The slot, or -1 when the transition did not happen.</param>
    /// <returns><see langword="false"/> when the context is malformed or stale, or the state was not <paramref name="from"/>.</returns>
    public bool TryTransitionContext(ulong context, SendEntryState from, SendEntryState to, out int slot)
    {
        uint index = (uint)context;
        if (index < (uint)_entries.Length)
        {
            long generationBits = (long)(context & 0xFFFF_FFFF_0000_0000UL);
            ref long word = ref *(long*)(_entries.Pointer + index);
            if (Interlocked.CompareExchange(ref word, generationBits | (uint)to, generationBits | (uint)from) == (generationBits | (uint)from))
            {
                slot = (int)index;
                return true;
            }
        }

        slot = -1;
        return false;
    }

    /// <summary>Current state of the entry, read with acquire semantics. Any thread.</summary>
    /// <param name="slot">Slot index.</param>
    public SendEntryState GetState(int slot) => (SendEntryState)Volatile.Read(ref _entries[slot].State);

    /// <summary>
    /// Returns a <see cref="SendEntryState.Completed"/> entry to the free list. Owner thread only, and only after the
    /// completion was observed through the completion ring (never by polling the state).
    /// </summary>
    /// <param name="slot">Slot index.</param>
    /// <exception cref="InvalidOperationException">The entry is not <see cref="SendEntryState.Completed"/> (a protocol bug).</exception>
    public void Free(int slot)
    {
        ref long word = ref StateWord(slot);
        long current = Volatile.Read(ref word);
        if ((int)current != (int)SendEntryState.Completed)
            ThrowWrongState(slot, (SendEntryState)(int)current, SendEntryState.Completed);
        Release(slot, ref word, current);
    }

    /// <summary>
    /// Unwinds an entry that will never complete: one still <see cref="SendEntryState.Filling"/> (the send was
    /// abandoned before publishing) or one the transport rejected synchronously after <see cref="Publish"/> (a
    /// non-<see cref="TransportStatus.Success"/> result means no completion follows). Owner thread only.
    /// </summary>
    /// <param name="slot">Slot index.</param>
    /// <exception cref="InvalidOperationException">The entry is neither <see cref="SendEntryState.Filling"/> nor <see cref="SendEntryState.InFlight"/>.</exception>
    public void Discard(int slot)
    {
        ref long word = ref StateWord(slot);
        long current = Volatile.Read(ref word);
        int state = (int)current;
        if (state != (int)SendEntryState.Filling && state != (int)SendEntryState.InFlight)
            ThrowWrongState(slot, (SendEntryState)state, SendEntryState.Filling);
        Release(slot, ref word, current);
    }

    private void Release(int slot, ref long word, long current)
    {
        // Keep the generation: a late context for this occupant still fails (state Free), and the next
        // allocation bumps it.
        Volatile.Write(ref word, (current & GenerationMask) | (uint)SendEntryState.Free);
        _freeStack.Pointer[_freeCount++] = slot;
    }

    /// <summary>Transport context for <paramref name="slot"/>: <c>(generation &lt;&lt; 32) | slot</c>.</summary>
    /// <param name="slot">Slot index.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ulong MakeContext(int slot) => ((ulong)_entries[slot].Generation << 32) | (uint)slot;

    /// <summary>
    /// Recovers the slot from a transport context and validates it: the slot must exist, be allocated (not
    /// <see cref="SendEntryState.Free"/>) and carry the context's generation, all read in one atomic snapshot.
    /// Any thread. The answer can be outdated by the time it returns; a transport-thread caller that goes on to
    /// change the state uses <see cref="TryTransitionContext"/>, which re-validates atomically.
    /// </summary>
    /// <param name="context">Context previously produced by <see cref="MakeContext"/>.</param>
    /// <param name="slot">The slot, or -1 when the context is stale or malformed.</param>
    /// <returns><see langword="false"/> for a stale or malformed context; the caller counts and ignores it.</returns>
    public bool TryResolveContext(ulong context, out int slot)
    {
        uint index = (uint)context;
        if (index < (uint)_entries.Length)
        {
            long word = Volatile.Read(ref *(long*)(_entries.Pointer + index));
            if ((int)word != (int)SendEntryState.Free && (uint)((ulong)word >> 32) == (uint)(context >> 32))
            {
                slot = (int)index;
                return true;
            }
        }

        slot = -1;
        return false;
    }

    /// <summary>
    /// Links <paramref name="memberSlot"/> into the member list of <paramref name="containerSlot"/> (a
    /// <see cref="SendEntryFlags.Container"/> entry). The member's <see cref="Next"/> link is reused, so it must
    /// already have left its channel queue. Owner thread only.
    /// </summary>
    /// <param name="containerSlot">The container entry.</param>
    /// <param name="memberSlot">The entry packed into the container.</param>
    public void AddToBatch(int containerSlot, int memberSlot)
    {
        Next[memberSlot] = BatchHead[containerSlot];
        BatchHead[containerSlot] = memberSlot;
        BatchCount[containerSlot]++;
    }

    /// <summary>Members of a container entry, enumerated without allocation (most recently added first).</summary>
    /// <param name="containerSlot">The container entry.</param>
    public ContainerBatch GetBatch(int containerSlot) => new(this, containerSlot);

    /// <summary>Detaches every member from <paramref name="containerSlot"/> (the members' own links are left as they are). Owner thread only.</summary>
    /// <param name="containerSlot">The container entry.</param>
    public void ClearBatch(int containerSlot)
    {
        BatchHead[containerSlot] = -1;
        BatchCount[containerSlot] = 0;
    }

    /// <summary>Frees the native memory. Idempotent; must not race with other calls. Afterwards <see cref="TryAllocate"/> fails and slot accessors throw.</summary>
    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;
        _freeCount = 0;
        _entries.Dispose();
        _headerBlocks.Dispose();
        _freeStack.Dispose();
        Leases.Dispose();
        Keys.Dispose();
        Sequences.Dispose();
        Contexts.Dispose();
        Deadlines.Dispose();
        Next.Dispose();
        BatchHead.Dispose();
        BatchCount.Dispose();
    }

    [DoesNotReturn]
    private static void ThrowSlotOutOfRange(int slot) =>
        throw new ArgumentOutOfRangeException(nameof(slot), slot, "Slot is outside the send entry table.");

    [DoesNotReturn]
    private static void ThrowWrongState(int slot, SendEntryState actual, SendEntryState expected) =>
        throw new InvalidOperationException($"Send entry {slot} is {actual}; expected {expected}.");
}

/// <summary>The member slots of a container entry; a <c>foreach</c>-able view over <see cref="SendEntryTable.BatchHead"/> and <see cref="SendEntryTable.Next"/>.</summary>
public readonly struct ContainerBatch
{
    private readonly SendEntryTable _table;
    private readonly int _container;

    internal ContainerBatch(SendEntryTable table, int container)
    {
        _table = table;
        _container = container;
    }

    /// <summary>Number of members.</summary>
    public int Count => _table.BatchCount[_container];

    /// <summary>Slot of the first member, or -1.</summary>
    public int Head => _table.BatchHead[_container];

    /// <summary>Allocation-free enumerator over member slots.</summary>
    public Enumerator GetEnumerator() => new(_table, _table.BatchHead[_container]);

    /// <summary>Enumerates member slots by following <see cref="SendEntryTable.Next"/> links.</summary>
    public struct Enumerator
    {
        private readonly SendEntryTable _table;
        private int _next;
        private int _current;

        internal Enumerator(SendEntryTable table, int head)
        {
            _table = table;
            _next = head;
            _current = -1;
        }

        /// <summary>The current member slot.</summary>
        public readonly int Current => _current;

        /// <summary>Advances to the next member.</summary>
        public bool MoveNext()
        {
            if (_next < 0)
                return false;
            _current = _next;
            _next = _table.Next[_next];
            return true;
        }
    }
}
