using System.Numerics;
using System.Runtime.CompilerServices;

namespace Tedd.Quicly.Core.State;

/// <summary>
/// Key table for <c>KeySpace.Dense(max)</c>: keys are small integers and the slot <em>is</em> the key
/// (<c>slot == key</c> for <c>key &lt; MaxKeys</c>), so lookup is a bounds check plus one bit test in an occupancy
/// bitset. Keys at or above <see cref="MaxKeys"/> are rejected.
/// </summary>
/// <remarks>Single owner; no internal synchronisation.</remarks>
public sealed unsafe class DenseKeyTable : IKeyTable, IDisposable
{
    private readonly NativeArray<ulong> _occupied;
    private readonly int _maxKeys;
    private int _count;
    private bool _disposed;

    /// <summary>Creates a table for keys <c>0 … maxKeys - 1</c>.</summary>
    /// <param name="maxKeys">Number of keys in the key space (1 … 2^30).</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxKeys"/> is outside its range.</exception>
    public DenseKeyTable(int maxKeys)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxKeys, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(maxKeys, 1 << 30);
        _maxKeys = maxKeys;
        _occupied = new NativeArray<ulong>((maxKeys + 63) >> 6);
    }

    /// <inheritdoc />
    public int Count => _count;

    /// <inheritdoc />
    public int MaxKeys => _maxKeys;

    /// <summary>Same as <see cref="MaxKeys"/>: every key has its own slot.</summary>
    public int Capacity => _maxKeys;

    /// <summary>True when <paramref name="slot"/> (equivalently, key) is live.</summary>
    /// <param name="slot">Slot index in <c>[0, MaxKeys)</c>.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsOccupied(int slot) =>
        (uint)slot < (uint)_maxKeys && (_occupied.Pointer[slot >> 6] & (1UL << (slot & 63))) != 0;

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGetSlot(ulong key, out int slot)
    {
        if (key < (ulong)_maxKeys)
        {
            int k = (int)key;
            if ((_occupied.Pointer[k >> 6] & (1UL << (k & 63))) != 0)
            {
                slot = k;
                return true;
            }
        }

        slot = -1;
        return false;
    }

    /// <inheritdoc />
    public bool TryAdd(ulong key, out int slot)
    {
        if (key >= (ulong)_maxKeys)
        {
            slot = -1;
            return false;
        }

        int k = (int)key;
        ref ulong word = ref _occupied[k >> 6];
        ulong bit = 1UL << (k & 63);
        slot = k;
        if ((word & bit) != 0)
            return false;
        word |= bit;
        _count++;
        return true;
    }

    /// <inheritdoc />
    public bool TryGetOrAdd(ulong key, out int slot)
    {
        if (key >= (ulong)_maxKeys)
        {
            slot = -1;
            return false;
        }

        int k = (int)key;
        ref ulong word = ref _occupied[k >> 6];
        ulong bit = 1UL << (k & 63);
        slot = k;
        if ((word & bit) == 0)
        {
            word |= bit;
            _count++;
        }

        return true;
    }

    /// <inheritdoc />
    public bool Remove(ulong key, out int slot)
    {
        if (key < (ulong)_maxKeys)
        {
            int k = (int)key;
            ref ulong word = ref _occupied[k >> 6];
            ulong bit = 1UL << (k & 63);
            if ((word & bit) != 0)
            {
                word &= ~bit;
                _count--;
                slot = k;
                return true;
            }
        }

        slot = -1;
        return false;
    }

    /// <inheritdoc />
    public void Clear()
    {
        _occupied.Clear();
        _count = 0;
    }

    /// <summary>Allocation-free enumerator over live keys in increasing order. Do not mutate while enumerating.</summary>
    public Enumerator GetEnumerator() => new(this);

    /// <summary>Frees the native memory. Idempotent.</summary>
    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;
        _occupied.Dispose();
    }

    /// <summary>Enumerates the live keys of a <see cref="DenseKeyTable"/> by scanning its occupancy bitset.</summary>
    public struct Enumerator
    {
        private readonly DenseKeyTable _table;
        private int _word;
        private ulong _bits;
        private KeyTableEntry _current;

        internal Enumerator(DenseKeyTable table)
        {
            _table = table;
            _word = -1;
            _bits = 0;
            _current = default;
        }

        /// <summary>The current entry (its slot equals its key).</summary>
        public KeyTableEntry Current => _current;

        /// <summary>Advances to the next live key.</summary>
        public bool MoveNext()
        {
            while (_bits == 0)
            {
                if (++_word >= _table._occupied.Length)
                    return false;
                _bits = _table._occupied.Pointer[_word];
            }

            int k = (_word << 6) + BitOperations.TrailingZeroCount(_bits);
            _bits &= _bits - 1;
            _current = new KeyTableEntry((ulong)k, k);
            return true;
        }
    }
}
