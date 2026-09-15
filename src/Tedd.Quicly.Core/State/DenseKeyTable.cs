using System.Numerics;
using System.Runtime.CompilerServices;

namespace Tedd.Quicly.Core.State;

/// <summary>
/// Key table for <c>KeySpace.Dense(max)</c>: keys are small integers and the slot <em>is</em> the key
/// (<c>slot == key</c> for <c>key &lt; MaxKeys</c>), so a lookup is one bounds check plus one bit test in an
/// occupancy bitset. Keys at or above <see cref="MaxKeys"/> are rejected (<see cref="TryAdd"/> returns
/// <see langword="false"/> with slot -1).
/// </summary>
/// <remarks>Single owner; no internal synchronisation. After <see cref="Dispose"/> lookups miss and mutations throw
/// <see cref="ObjectDisposedException"/>.</remarks>
public sealed unsafe class DenseKeyTable : IKeyTable, IDisposable
{
    /// <summary>Largest <see cref="MaxKeys"/>.</summary>
    public const int MaxKeysLimit = 1 << 30;

    private readonly NativeArray<ulong> _occupied;
    private ulong* _bits;
    private ulong _limit;
    private readonly int _maxKeys;
    private int _count;
    private bool _disposed;

    /// <summary>Creates a table for keys <c>0 … maxKeys - 1</c>.</summary>
    /// <param name="maxKeys">Number of keys in the key space (1 … <see cref="MaxKeysLimit"/>).</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="maxKeys"/> is outside its range.</exception>
    public DenseKeyTable(int maxKeys)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxKeys, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(maxKeys, MaxKeysLimit);
        _maxKeys = maxKeys;
        _limit = (ulong)maxKeys;
        _occupied = new NativeArray<ulong>((maxKeys + 63) >> 6);
        _bits = _occupied.Pointer;
    }

    /// <inheritdoc />
    public int Count => _count;

    /// <inheritdoc />
    public int MaxKeys => _maxKeys;

    /// <inheritdoc />
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryGetSlot(ulong key, out int slot)
    {
        if (key < _limit)
        {
            int k = (int)key;
            if ((_bits[k >> 6] & (1UL << k)) != 0)
            {
                slot = k;
                return true;
            }
        }

        slot = -1;
        return false;
    }

    /// <summary>Adds <paramref name="key"/>; its slot is the key itself.</summary>
    /// <param name="key">The key.</param>
    /// <param name="slot"><paramref name="key"/> as an <see cref="int"/>, or -1 when the key is outside the key space.</param>
    /// <returns><see langword="true"/> only when the key was added by this call.</returns>
    /// <exception cref="ObjectDisposedException">The table was disposed.</exception>
    public bool TryAdd(ulong key, out int slot)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (key >= _limit)
        {
            slot = -1;
            return false;
        }

        int k = (int)key;
        ulong* word = _bits + (k >> 6);
        ulong bit = 1UL << k;
        slot = k;
        if ((*word & bit) != 0)
            return false;
        *word |= bit;
        _count++;
        return true;
    }

    /// <summary>Finds or adds <paramref name="key"/>; its slot is the key itself.</summary>
    /// <param name="key">The key.</param>
    /// <param name="slot"><paramref name="key"/> as an <see cref="int"/>, or -1 when the key is outside the key space.</param>
    /// <returns><see langword="false"/> only when the key is outside the key space.</returns>
    /// <exception cref="ObjectDisposedException">The table was disposed.</exception>
    public bool TryGetOrAdd(ulong key, out int slot)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (key >= _limit)
        {
            slot = -1;
            return false;
        }

        int k = (int)key;
        ulong* word = _bits + (k >> 6);
        ulong bit = 1UL << k;
        slot = k;
        if ((*word & bit) == 0)
        {
            *word |= bit;
            _count++;
        }

        return true;
    }

    /// <inheritdoc />
    /// <exception cref="ObjectDisposedException">The table was disposed.</exception>
    public bool Remove(ulong key, out int slot)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (key < _limit)
        {
            int k = (int)key;
            ulong* word = _bits + (k >> 6);
            ulong bit = 1UL << k;
            if ((*word & bit) != 0)
            {
                *word &= ~bit;
                _count--;
                slot = k;
                return true;
            }
        }

        slot = -1;
        return false;
    }

    /// <inheritdoc />
    /// <exception cref="ObjectDisposedException">The table was disposed.</exception>
    public void Clear()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _occupied.Clear();
        _count = 0;
    }

    /// <summary>Allocation-free enumerator over live keys in increasing order. Do not mutate while enumerating.</summary>
    public Enumerator GetEnumerator() => new(this);

    /// <summary>Frees the native memory. Idempotent; must not race with other calls.</summary>
    public void Dispose()
    {
        if (_disposed)
            return;
        _disposed = true;
        _limit = 0;
        _bits = null;
        _count = 0;
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
        public readonly KeyTableEntry Current => _current;

        /// <summary>Advances to the next live key.</summary>
        public bool MoveNext()
        {
            while (_bits == 0)
            {
                // Length is 0 after Dispose, so a disposed table enumerates nothing.
                if (++_word >= _table._occupied.Length)
                {
                    _word = _table._occupied.Length;
                    return false;
                }

                _bits = _table._bits[_word];
            }

            int k = (_word << 6) + BitOperations.TrailingZeroCount(_bits);
            _bits &= _bits - 1;
            _current = new KeyTableEntry((ulong)k, k);
            return true;
        }
    }
}
