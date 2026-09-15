using System.Numerics;
using System.Runtime.CompilerServices;

namespace Tedd.Quicly.Core.State;

/// <summary>One live key and its slot, as yielded by the key-table enumerators.</summary>
/// <param name="Key">The key.</param>
/// <param name="Slot">Its dense slot index.</param>
public readonly record struct KeyTableEntry(ulong Key, int Slot);

/// <summary>
/// Hash table from 64-bit keys to dense, stable slot indices: open addressing with linear probing over two
/// structure-of-arrays native arrays (<c>keys[]</c>, <c>slots[]</c>), power-of-two capacity, load factor at most
/// 0.5, <see cref="KeyHash.Fmix64"/> mixing and backward-shift deletion (no tombstones, so lookups never degrade
/// after churn).
/// </summary>
/// <remarks>
/// <para><b>Empty buckets</b> are marked by <c>slots[i] == -1</c>; the key array carries no sentinel, so every
/// 64-bit value is a valid key.</para>
/// <para><b>Slots</b> are allocated from <c>[0, MaxKeys)</c>: fresh indices in increasing order first, then recycled
/// ones from a free stack. A key keeps its slot until it is removed, so per-key state can live in arrays indexed
/// by slot (<see cref="KeySendSlot"/>, <see cref="KeyRecvSlot"/>, <see cref="Mailboxes"/>).</para>
/// <para><b>Growth.</b> The bucket array is sized for <c>initialKeys</c> at construction and doubles (rehashing
/// into fresh native memory) when the load would exceed 0.5, up to the capacity needed for <see cref="MaxKeys"/>.
/// A table created with <c>initialKeys == maxKeys</c> (the one-argument constructor) never grows, which is what
/// the hot path wants; pre-size when growth during play is unacceptable.</para>
/// <para><b>Threads.</b> Single owner; no internal synchronisation.</para>
/// </remarks>
public sealed unsafe class KeyTable : IKeyTable, IDisposable
{
    /// <summary>Smallest bucket array; keeps tiny tables from rehashing constantly.</summary>
    public const int MinCapacity = 8;

    /// <summary>Largest <see cref="MaxKeys"/> (the bucket array must stay below 2^31 elements at load 0.5).</summary>
    public const int MaxKeysLimit = 1 << 29;

    private NativeArray<ulong> _keys;
    private NativeArray<int> _slots;
    private int _mask;
    private int _count;
    private readonly int _maxKeys;
    private readonly int _maxCapacity;
    private readonly NativeArray<int> _freeSlots;
    private int _freeCount;
    private int _nextFreshSlot;
    private bool _disposed;

    /// <summary>Creates a table pre-sized for <paramref name="maxKeys"/> keys that never grows.</summary>
    /// <param name="maxKeys">Maximum number of live keys (1 … 2^29).</param>
    public KeyTable(int maxKeys) : this(maxKeys, maxKeys)
    {
    }

    /// <summary>Creates a table sized for <paramref name="initialKeys"/> that grows by doubling up to <paramref name="maxKeys"/>.</summary>
    /// <param name="maxKeys">Maximum number of live keys (1 … 2^29).</param>
    /// <param name="initialKeys">Number of keys the initial bucket array holds without rehashing (0 … <paramref name="maxKeys"/>).</param>
    /// <exception cref="ArgumentOutOfRangeException">An argument is outside its range.</exception>
    public KeyTable(int maxKeys, int initialKeys)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxKeys, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(maxKeys, MaxKeysLimit);
        ArgumentOutOfRangeException.ThrowIfNegative(initialKeys);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(initialKeys, maxKeys);

        _maxKeys = maxKeys;
        _maxCapacity = CapacityFor(maxKeys);
        int capacity = CapacityFor(initialKeys);
        _keys = new NativeArray<ulong>(capacity);
        _slots = new NativeArray<int>(capacity);
        _slots.Fill(-1);
        _mask = capacity - 1;
        _freeSlots = new NativeArray<int>(maxKeys);
    }

    private static int CapacityFor(int keys) => (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(2 * keys, MinCapacity));

    /// <inheritdoc />
    public int Count => _count;

    /// <inheritdoc />
    public int MaxKeys => _maxKeys;

    /// <summary>Current number of buckets; a power of two, at least twice <see cref="Count"/>.</summary>
    public int Capacity => _mask + 1;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int IndexOf(ulong key) => (int)KeyHash.Fmix64(key) & _mask;

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGetSlot(ulong key, out int slot)
    {
        ulong* keys = _keys.Pointer;
        int* slots = _slots.Pointer;
        int mask = _mask;
        int i = (int)KeyHash.Fmix64(key) & mask;
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

    /// <inheritdoc />
    public bool TryAdd(ulong key, out int slot)
    {
        int i = FindBucket(key, out bool found);
        if (found)
        {
            slot = _slots[i];
            return false;
        }

        return Insert(key, i, out slot);
    }

    /// <inheritdoc />
    public bool TryGetOrAdd(ulong key, out int slot)
    {
        int i = FindBucket(key, out bool found);
        if (found)
        {
            slot = _slots[i];
            return true;
        }

        return Insert(key, i, out slot);
    }

    /// <summary>Returns the bucket holding <paramref name="key"/>, or the empty bucket where it would go.</summary>
    private int FindBucket(ulong key, out bool found)
    {
        ulong* keys = _keys.Pointer;
        int* slots = _slots.Pointer;
        int mask = _mask;
        int i = (int)KeyHash.Fmix64(key) & mask;
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
            // Load would exceed 0.5. The capacity for MaxKeys holds 2 × MaxKeys buckets, so this only triggers
            // while the table is still below that size.
            Grow();
            bucket = FindBucket(key, out _);
        }

        slot = _freeCount > 0 ? _freeSlots[--_freeCount] : _nextFreshSlot++;
        _keys[bucket] = key;
        _slots[bucket] = slot;
        _count++;
        return true;
    }

    private void Grow()
    {
        int newCapacity = (_mask + 1) * 2;
        System.Diagnostics.Debug.Assert(newCapacity <= _maxCapacity, "growth past the capacity for MaxKeys");
        var newKeys = new NativeArray<ulong>(newCapacity);
        var newSlots = new NativeArray<int>(newCapacity);
        newSlots.Fill(-1);
        int newMask = newCapacity - 1;

        ulong* oldKeys = _keys.Pointer;
        int* oldSlots = _slots.Pointer;
        ulong* nk = newKeys.Pointer;
        int* ns = newSlots.Pointer;
        for (int i = 0; i <= _mask; i++)
        {
            int s = oldSlots[i];
            if (s < 0)
                continue;
            ulong key = oldKeys[i];
            int j = (int)KeyHash.Fmix64(key) & newMask;
            while (ns[j] >= 0)
                j = (j + 1) & newMask;
            nk[j] = key;
            ns[j] = s;
        }

        _keys.Dispose();
        _slots.Dispose();
        _keys = newKeys;
        _slots = newSlots;
        _mask = newMask;
    }

    /// <inheritdoc />
    public bool Remove(ulong key, out int slot)
    {
        int i = FindBucket(key, out bool found);
        if (!found)
        {
            slot = -1;
            return false;
        }

        ulong* keys = _keys.Pointer;
        int* slots = _slots.Pointer;
        int mask = _mask;
        slot = slots[i];
        _freeSlots[_freeCount++] = slot;
        _count--;

        // Backward shift: walk the probe run after i and pull back every entry whose home bucket lies at or
        // before the hole (cyclically), closing the hole without leaving a tombstone.
        int j = i;
        while (true)
        {
            j = (j + 1) & mask;
            int sj = slots[j];
            if (sj < 0)
                break;
            int home = (int)KeyHash.Fmix64(keys[j]) & mask;
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

    /// <inheritdoc />
    public void Clear()
    {
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
        int i = IndexOf(key);
        int probes = 1;
        while (true)
        {
            int s = _slots[i];
            if (s < 0)
                return -1;
            if (_keys[i] == key)
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
            if (_slots[i] < 0)
                continue;
            int probes = ((i - IndexOf(_keys[i])) & _mask) + 1;
            if (probes > max)
                max = probes;
        }

        return max;
    }

    /// <summary>Frees the native memory. Idempotent.</summary>
    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;
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
        public KeyTableEntry Current => _current;

        /// <summary>Advances to the next live bucket.</summary>
        public bool MoveNext()
        {
            int* slots = _table._slots.Pointer;
            int mask = _table._mask;
            while (++_index <= mask)
            {
                int s = slots[_index];
                if (s >= 0)
                {
                    _current = new KeyTableEntry(_table._keys.Pointer[_index], s);
                    return true;
                }
            }

            _index = mask + 1;
            return false;
        }
    }
}
