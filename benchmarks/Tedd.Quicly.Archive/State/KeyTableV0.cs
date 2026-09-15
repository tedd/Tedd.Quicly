using System.Numerics;
using Tedd.Quicly.Core.State;

namespace Tedd.Quicly.Archive.State;

/// <summary>
/// V0 of the key table: the same open-addressing, linear-probing, backward-shift structure as
/// <c>Tedd.Quicly.Core.State.KeyTable</c>, but with the identity hash (home bucket = low bits of the key) and a
/// fixed capacity. Superseded by the seeded fmix64 version; kept for the comparison in docs/benchmarks/state.md.
/// </summary>
public sealed unsafe class KeyTableV0 : IDisposable
{
    private readonly NativeArray<ulong> _keys;
    private readonly NativeArray<int> _slots;
    private readonly NativeArray<int> _freeSlots;
    private readonly ulong* _keyPtr;
    private readonly int* _slotPtr;
    private readonly int _mask;
    private readonly int _maxKeys;
    private int _count;
    private int _freeCount;
    private int _nextFreshSlot;

    public KeyTableV0(int maxKeys)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxKeys, 1);
        int capacity = (int)BitOperations.RoundUpToPowerOf2((uint)Math.Max(2 * maxKeys, 8));
        _maxKeys = maxKeys;
        _keys = new NativeArray<ulong>(capacity);
        _slots = new NativeArray<int>(capacity);
        _slots.Fill(-1);
        _freeSlots = new NativeArray<int>(maxKeys);
        _keyPtr = _keys.Pointer;
        _slotPtr = _slots.Pointer;
        _mask = capacity - 1;
    }

    public int Count => _count;

    public bool TryGetSlot(ulong key, out int slot)
    {
        ulong* keys = _keyPtr;
        int* slots = _slotPtr;
        int mask = _mask;
        int i = (int)key & mask;
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

    public bool TryAdd(ulong key, out int slot)
    {
        int i = (int)key & _mask;
        while (_slotPtr[i] >= 0)
        {
            if (_keyPtr[i] == key)
            {
                slot = _slotPtr[i];
                return false;
            }

            i = (i + 1) & _mask;
        }

        if (_count == _maxKeys)
        {
            slot = -1;
            return false;
        }

        slot = _freeCount > 0 ? _freeSlots.Pointer[--_freeCount] : _nextFreshSlot++;
        _keyPtr[i] = key;
        _slotPtr[i] = slot;
        _count++;
        return true;
    }

    public bool Remove(ulong key, out int slot)
    {
        int mask = _mask;
        int i = (int)key & mask;
        while (true)
        {
            if (_slotPtr[i] < 0)
            {
                slot = -1;
                return false;
            }

            if (_keyPtr[i] == key)
                break;
            i = (i + 1) & mask;
        }

        slot = _slotPtr[i];
        _freeSlots.Pointer[_freeCount++] = slot;
        _count--;
        int j = i;
        while (true)
        {
            j = (j + 1) & mask;
            int sj = _slotPtr[j];
            if (sj < 0)
                break;
            int home = (int)_keyPtr[j] & mask;
            if (((j - home) & mask) >= ((j - i) & mask))
            {
                _keyPtr[i] = _keyPtr[j];
                _slotPtr[i] = sj;
                i = j;
            }
        }

        _slotPtr[i] = -1;
        return true;
    }

    public void Dispose()
    {
        _keys.Dispose();
        _slots.Dispose();
        _freeSlots.Dispose();
    }
}
