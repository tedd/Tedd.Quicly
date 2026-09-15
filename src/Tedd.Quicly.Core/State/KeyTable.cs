using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Tedd.Quicly.Core.State;

/// <summary>One live key and its slot, as yielded by the key-table enumerators.</summary>
/// <param name="Key">The key.</param>
/// <param name="Slot">Its dense slot index.</param>
public readonly record struct KeyTableEntry(ulong Key, int Slot);

/// <summary>
/// Hash table from 64-bit keys to dense, stable slot indices: open addressing with linear probing over two
/// structure-of-arrays native arrays (<c>keys[]</c>, <c>slots[]</c>), power-of-two capacity, load factor at most
/// 0.5, seeded <c>fmix64</c> mixing and backward-shift deletion (no tombstones, so lookups never degrade after
/// churn).
/// </summary>
/// <remarks>
/// <para><b>Every 64-bit value is a valid key.</b> Occupancy is tracked in the slot array (<c>slots[i] == -1</c>
/// marks an empty bucket), not by a sentinel key, so 0 and <see cref="ulong.MaxValue"/> are ordinary keys.</para>
/// <para><b>Slots</b> are allocated from <c>[0, MaxKeys)</c>: fresh indices in increasing order first, then recycled
/// ones from an internal free stack (most recently freed first). A key keeps its slot until it is removed, so
/// per-key state can live in arrays indexed by slot (<see cref="KeySendSlot"/>, <see cref="KeyRecvSlot"/>,
/// <see cref="Mailboxes"/>), and the slots in use are always a subset of <c>[0, high-water mark)</c>.</para>
/// <para><b>Growth.</b> The bucket array is sized for <c>initialKeys</c> at construction and doubles (rehashing
/// into fresh native memory) when the load would exceed 0.5, up to the capacity needed for <see cref="MaxKeys"/>;
/// at <see cref="MaxKeys"/> live keys <see cref="TryAdd"/> returns <see langword="false"/>. A table created with
/// the one-argument constructor is sized for <see cref="MaxKeys"/> up front and never grows, which is what the
/// hot path wants.</para>
/// <para><b>Hashing.</b> Keys are XORed with a random per-table seed and mixed with <c>fmix64</c>; the bucket is the
/// low bits. The seed keeps a remote peer from constructing colliding keys (ADR 0009). docs/benchmarks/state.md
/// records the identity-hash comparison.</para>
/// <para><b>Threads.</b> Single owner; no internal synchronisation. Using the table after <see cref="Dispose"/>
/// is safe but useless: lookups miss and mutations throw <see cref="ObjectDisposedException"/>.</para>
/// </remarks>
public sealed unsafe class KeyTable : IKeyTable, IDisposable
{
    /// <summary>Smallest bucket array; keeps tiny tables from rehashing constantly.</summary>
    public const int MinCapacity = 8;

    /// <summary>Largest <see cref="MaxKeys"/> (the bucket array must stay below 2^31 elements at load 0.5).</summary>
    public const int MaxKeysLimit = 1 << 29;

    // One empty bucket (slot -1) that a disposed table points at, so a stray lookup misses instead of faulting.
    private static readonly int* s_emptySlots = CreateEmptySlots();

    private NativeArray<ulong> _keys;
    private NativeArray<int> _slots;
    private ulong* _keyPtr;
    private int* _slotPtr;
    private int _mask;
    private int _count;
    private readonly ulong _seed;
    private readonly int _maxKeys;
    private readonly NativeArray<int> _freeSlots;
    private int _freeCount;
    private int _nextFreshSlot;
    private bool _disposed;

    /// <summary>Creates a table sized for <paramref name="maxKeys"/> keys that never grows.</summary>
    /// <param name="maxKeys">Maximum number of live keys (1 … <see cref="MaxKeysLimit"/>).</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxKeys"/> is outside its range.</exception>
    public KeyTable(int maxKeys) : this(maxKeys, maxKeys)
    {
    }

    /// <summary>Creates a table sized for <paramref name="initialKeys"/> that grows by doubling up to <paramref name="maxKeys"/>.</summary>
    /// <param name="maxKeys">Maximum number of live keys (1 … <see cref="MaxKeysLimit"/>).</param>
    /// <param name="initialKeys">Number of keys the initial bucket array holds without rehashing (0 … <paramref name="maxKeys"/>).</param>
    /// <exception cref="ArgumentOutOfRangeException">An argument is outside its range.</exception>
    public KeyTable(int maxKeys, int initialKeys) : this(maxKeys, initialKeys, KeyHash.NewSeed())
    {
    }

    /// <summary>Creates a table with a fixed hash seed (deterministic layout for tests and benchmarks).</summary>
    internal KeyTable(int maxKeys, int initialKeys, ulong seed)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxKeys, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(maxKeys, MaxKeysLimit);
        ArgumentOutOfRangeException.ThrowIfNegative(initialKeys);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(initialKeys, maxKeys);

        _seed = seed;
        _maxKeys = maxKeys;
        int capacity = CapacityFor(initialKeys);
        _keys = new NativeArray<ulong>(capacity);
        _slots = new NativeArray<int>(capacity);
        _slots.Fill(-1);
        _keyPtr = _keys.Pointer;
        _slotPtr = _slots.Pointer;
        _mask = capacity - 1;
        _freeSlots = new NativeArray<int>(maxKeys);
    }

    private static int* CreateEmptySlots()
    {
        // Process-lifetime singleton; intentionally never freed.
        int* p = (int*)NativeMemory.Alloc(sizeof(int));
        *p = -1;
        return p;
    }

    private static int CapacityFor(int keys) => (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(2 * keys, MinCapacity));

    /// <inheritdoc />
    public int Count => _count;

    /// <inheritdoc />
    public int MaxKeys => _maxKeys;

    /// <summary>Current number of buckets; a power of two, at least twice <see cref="Count"/>.</summary>
    public int Capacity => _mask + 1;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int Home(ulong key, int mask) => (int)KeyHash.Fmix64(key ^ _seed) & mask;

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGetSlot(ulong key, out int slot)
    {
        ulong* keys = _keyPtr;
        int* slots = _slotPtr;
        int mask = _mask;
        int i = Home(key, mask);
        while (true)
        {
            int s = slots[i];
            if (s < 0)
            {
                slot = -1;
                return false;
            }

            if (keys[i] == key)
            {
                slot = s;
                return true;
            }

            i = (i + 1) & mask;
        }
    }

    /// <summary>Adds <paramref name="key"/> and allocates a slot for it.</summary>
    /// <param name="key">The key (any 64-bit value).</param>
    /// <param name="slot">The new slot; the existing slot when the key was already present; -1 when the table holds <see cref="MaxKeys"/> keys.</param>
    /// <returns><see langword="true"/> only when the key was added by this call.</returns>
    /// <exception cref="ObjectDisposedException">The table was disposed.</exception>
    public bool TryAdd(ulong key, out int slot)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        int i = FindBucket(key, out bool found);
        if (found)
        {
            slot = _slotPtr[i];
            return false;
        }

        return Insert(key, i, out slot);
    }

    /// <summary>Finds the slot of <paramref name="key"/>, adding the key when absent.</summary>
    /// <param name="key">The key (any 64-bit value).</param>
    /// <param name="slot">The slot, or -1 when the key is absent and the table holds <see cref="MaxKeys"/> keys.</param>
    /// <returns><see langword="false"/> only when the key is absent and the table is full.</returns>
    /// <exception cref="ObjectDisposedException">The table was disposed.</exception>
    public bool TryGetOrAdd(ulong key, out int slot)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        int i = FindBucket(key, out bool found);
        if (found)
        {
            slot = _slotPtr[i];
            return true;
        }

        return Insert(key, i, out slot);
    }

    /// <summary>Returns the bucket holding <paramref name="key"/>, or the empty bucket that ends its probe run.</summary>
    private int FindBucket(ulong key, out bool found)
    {
        ulong* keys = _keyPtr;
        int* slots = _slotPtr;
        int mask = _mask;
        int i = Home(key, mask);
        while (true)
        {
            if (slots[i] < 0)
            {
                found = false;
                return i;
            }

            if (keys[i] == key)
            {
                found = true;
                return i;
            }

            i = (i + 1) & mask;
        }
    }

    private bool Insert(ulong key, int bucket, out int slot)
    {
        if (_count == _maxKeys)
        {
            slot = -1;
            return false;
        }

        if ((_count + 1) * 2 > _mask + 1)
        {
            // Load would exceed 0.5. The capacity for MaxKeys has at least 2 × MaxKeys buckets, so this only
            // triggers while the table is still smaller than that.
            Grow();
            bucket = FindBucket(key, out _);
        }

        int s = _freeCount > 0 ? _freeSlots.Pointer[--_freeCount] : _nextFreshSlot++;
        _keyPtr[bucket] = key;
        _slotPtr[bucket] = s;
        _count++;
        slot = s;
        return true;
    }

    private void Grow()
    {
        int newCapacity = (_mask + 1) * 2;
        Debug.Assert(newCapacity <= CapacityFor(_maxKeys), "growth past the capacity for MaxKeys");
        var newKeys = new NativeArray<ulong>(newCapacity);
        var newSlots = new NativeArray<int>(newCapacity);
        newSlots.Fill(-1);
        int newMask = newCapacity - 1;

        ulong* oldKeys = _keyPtr;
        int* oldSlots = _slotPtr;
        ulong* nk = newKeys.Pointer;
        int* ns = newSlots.Pointer;
        for (int i = 0; i <= _mask; i++)
        {
            int s = oldSlots[i];
            if (s < 0)
                continue;
            ulong key = oldKeys[i];
            int j = Home(key, newMask);
            while (ns[j] >= 0)
                j = (j + 1) & newMask;
            nk[j] = key;
            ns[j] = s;
        }

        _keys.Dispose();
        _slots.Dispose();
        _keys = newKeys;
        _slots = newSlots;
        _keyPtr = nk;
        _slotPtr = ns;
        _mask = newMask;
    }

    /// <summary>Removes <paramref name="key"/> and recycles its slot.</summary>
    /// <param name="key">The key.</param>
    /// <param name="slot">The slot the key had, or -1.</param>
    /// <returns><see langword="false"/> when the key was not present.</returns>
    /// <exception cref="ObjectDisposedException">The table was disposed.</exception>
    public bool Remove(ulong key, out int slot)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        int i = FindBucket(key, out bool found);
        if (!found)
        {
            slot = -1;
            return false;
        }

        ulong* keys = _keyPtr;
        int* slots = _slotPtr;
        int mask = _mask;
        slot = slots[i];
        _freeSlots.Pointer[_freeCount++] = slot;
        _count--;

        // Backward shift: walk the probe run after the hole and pull back every entry whose home bucket lies
        // cyclically at or before the hole, so the run stays unbroken without a tombstone.
        int j = i;
        while (true)
        {
            j = (j + 1) & mask;
            int sj = slots[j];
            if (sj < 0)
                break;
            int home = Home(keys[j], mask);
            if (((j - home) & mask) >= ((j - i) & mask))
            {
                keys[i] = keys[j];
                slots[i] = sj;
                i = j;
            }
        }

        slots[i] = -1;
        return true;
    }

    /// <summary>Removes every key and resets slot allocation to 0. Keeps the current bucket array.</summary>
    /// <exception cref="ObjectDisposedException">The table was disposed.</exception>
    public void Clear()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _slots.Fill(-1);
        _count = 0;
        _freeCount = 0;
        _nextFreshSlot = 0;
    }

    /// <summary>Allocation-free enumerator over every live (key, slot) pair, in bucket order. Do not mutate while enumerating.</summary>
    public Enumerator GetEnumerator() => new(this);

    /// <summary>Number of buckets probed to find <paramref name="key"/> (1 = its home bucket), or -1 when absent. Diagnostics.</summary>
    internal int GetProbeLength(ulong key)
    {
        int i = Home(key, _mask);
        int probes = 1;
        while (true)
        {
            if (_slotPtr[i] < 0)
                return -1;
            if (_keyPtr[i] == key)
                return probes;
            i = (i + 1) & _mask;
            probes++;
        }
    }

    /// <summary>Longest probe sequence over every live key. Diagnostics.</summary>
    internal int MaxProbeLength()
    {
        int max = 0;
        for (int i = 0; i <= _mask; i++)
        {
            if (_slotPtr[i] < 0)
                continue;
            int probes = ((i - Home(_keyPtr[i], _mask)) & _mask) + 1;
            if (probes > max)
                max = probes;
        }

        return max;
    }

    /// <summary>Frees the native memory. Idempotent; must not race with other calls.</summary>
    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;
        _keyPtr = null;
        _slotPtr = s_emptySlots;
        _mask = 0;
        _count = 0;
        _keys.Dispose();
        _slots.Dispose();
        _freeSlots.Dispose();
    }

    /// <summary>Enumerates the live entries of a <see cref="KeyTable"/> without allocating.</summary>
    public struct Enumerator
    {
        private readonly KeyTable _table;
        private int _index;
        private KeyTableEntry _current;

        internal Enumerator(KeyTable table)
        {
            _table = table;
            _index = -1;
            _current = default;
        }

        /// <summary>The current entry.</summary>
        public readonly KeyTableEntry Current => _current;

        /// <summary>Advances to the next live bucket.</summary>
        public bool MoveNext()
        {
            KeyTable table = _table;
            if (table._disposed)
                return false;
            int* slots = table._slotPtr;
            int mask = table._mask;
            while (++_index <= mask)
            {
                int s = slots[_index];
                if (s >= 0)
                {
                    _current = new KeyTableEntry(table._keyPtr[_index], s);
                    return true;
                }
            }

            _index = mask + 1;
            return false;
        }
    }
}
