using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Primitives;
using Tedd.Quicly.Core.State;

namespace Tedd.Quicly.Core.Session.Engines;

/// <summary>Outcome of <see cref="ReceiveKeyTracker.TryAcceptSequence"/>.</summary>
internal enum KeyAcceptance : byte
{
    /// <summary>The key is new, or the value is newer than the key's last accepted one.</summary>
    Accepted = 0,

    /// <summary>Not newer than the key's last accepted value (reordered or duplicated): drop it.</summary>
    Stale = 1,

    /// <summary>The key cannot get a slot (outside a dense key space): drop it and count <c>KeyTableFull</c>.</summary>
    Rejected = 2,
}

/// <summary>
/// Receive-side per-key state of one datagram channel (transport thread only, ADR 0008 invariant 4): the channel's key
/// table (hashed or dense), the last accepted sequence of every key and — for a hashed table — a least-recently-updated
/// list, so a full table evicts the key updated longest ago (PROTOCOL.md §7: an evicted key re-accepts any sequence when
/// it returns; that is the documented replay window). Slots come from the key table; they index the per-key arrays here
/// and the channel's mailboxes. The arrays grow with the slots in use (fresh slots are handed out in increasing order) up
/// to <see cref="ChannelDefinition.MaxKeys"/>, so an idle channel costs little memory.
/// </summary>
internal sealed class ReceiveKeyTracker : IDisposable
{
    private const int InitialSlots = 64;

    private readonly KeyTable? _hashed;
    private readonly DenseKeyTable? _dense;
    private readonly int _maxKeys;
    private NativeArray<uint> _last;
    private NativeArray<ulong>? _keys;
    private NativeArray<int>? _older;
    private NativeArray<int>? _newer;
    private int _newest = -1;
    private int _oldest = -1;

    /// <summary>Creates the tracker of <paramref name="channel"/> (keyed).</summary>
    /// <param name="channel">The channel.</param>
    public ReceiveKeyTracker(ChannelDefinition channel)
    {
        _maxKeys = channel.MaxKeys;
        int initial = Math.Min(_maxKeys, InitialSlots);
        _last = new NativeArray<uint>(initial);
        if (channel.KeySpace.IsDense)
        {
            _dense = new DenseKeyTable(_maxKeys);
        }
        else
        {
            _hashed = new KeyTable(_maxKeys, initial);
            _keys = new NativeArray<ulong>(initial);
            _older = new NativeArray<int>(initial);
            _newer = new NativeArray<int>(initial);
        }
    }

    /// <summary>Keys held now.</summary>
    public int Count => _hashed?.Count ?? _dense!.Count;

    /// <summary>Keys evicted to make room for new ones (hashed tables only).</summary>
    public long Evictions { get; private set; }

    /// <summary>
    /// Sequenced acceptance of a value of <paramref name="key"/> (PROTOCOL.md §5: only values newer than the key's last
    /// accepted one, in serial arithmetic of the channel's width). A new key accepts any sequence.
    /// </summary>
    /// <param name="key">The key.</param>
    /// <param name="sequence">The value's sequence (the low 16 bits on a 16-bit channel).</param>
    /// <param name="sixteenBit">The channel's sequences are 16 bits wide.</param>
    /// <param name="slot">The key's slot (valid unless <see cref="KeyAcceptance.Rejected"/>).</param>
    /// <returns>Whether to deliver the value.</returns>
    public KeyAcceptance TryAcceptSequence(ulong key, uint sequence, bool sixteenBit, out int slot)
    {
        if (TryGetSlot(key, out slot))
        {
            if (Diag.Mode == 51)
            {
                Touch(slot);
                return KeyAcceptance.Accepted;
            }

            uint last = _last[slot];
            bool newer = sixteenBit ? SerialNumber.IsNewer((ushort)sequence, (ushort)last) : SerialNumber.IsNewer(sequence, last);
            if (!newer)
            {
                return KeyAcceptance.Stale;
            }

            _last[slot] = sequence;
            Touch(slot);
            return KeyAcceptance.Accepted;
        }

        if (!TryAdd(key, out slot))
        {
            return KeyAcceptance.Rejected;
        }

        _last[slot] = sequence;
        return KeyAcceptance.Accepted;
    }

    /// <summary>The slot of <paramref name="key"/> for a latest-arrival channel (no sequence), adding the key (and evicting) when needed.</summary>
    /// <param name="key">The key.</param>
    /// <param name="slot">The key's slot, or -1.</param>
    /// <returns><see langword="false"/> when the key cannot get a slot (outside a dense key space).</returns>
    public bool TryTouch(ulong key, out int slot)
    {
        if (TryGetSlot(key, out slot))
        {
            Touch(slot);
            return true;
        }

        return TryAdd(key, out slot);
    }

    /// <summary>Forgets every key (a new epoch, PROTOCOL.md §4.1). The arrays keep their size.</summary>
    public void Clear()
    {
        _hashed?.Clear();
        _dense?.Clear();
        _newest = -1;
        _oldest = -1;
    }

    /// <summary>Frees the native memory. Idempotent.</summary>
    public void Dispose()
    {
        _hashed?.Dispose();
        _dense?.Dispose();
        _last.Dispose();
        _keys?.Dispose();
        _older?.Dispose();
        _newer?.Dispose();
    }

    private bool TryGetSlot(ulong key, out int slot) => _hashed is not null ? _hashed.TryGetSlot(key, out slot) : _dense!.TryGetSlot(key, out slot);

    private bool TryAdd(ulong key, out int slot)
    {
        if (_hashed is null)
        {
            if (!_dense!.TryAdd(key, out slot))
            {
                slot = -1;
                return false;
            }

            EnsureSlot(slot);
            return true;
        }

        if (!_hashed.TryAdd(key, out slot))
        {
            // Full, so every live key is on the list: evict the key updated longest ago (it re-accepts any sequence if it
            // comes back); the freed slot takes the new key.
            int victim = _oldest;
            System.Diagnostics.Debug.Assert(victim >= 0, "a full table has a least recently updated key");
            Unlink(victim);
            _hashed.Remove(_keys![victim], out _);
            Evictions++;
            bool added = _hashed.TryAdd(key, out slot);
            System.Diagnostics.Debug.Assert(added, "a slot was just freed");
        }

        EnsureSlot(slot);
        _keys![slot] = key;
        PushNewest(slot);
        return true;
    }

    private void Touch(int slot)
    {
        if (Diag.Mode == 50)
        {
            return;
        }

        if (_hashed is null || slot == _newest)
        {
            return;
        }

        Unlink(slot);
        PushNewest(slot);
    }

    private void PushNewest(int slot)
    {
        NativeArray<int> older = _older!;
        NativeArray<int> newer = _newer!;
        older[slot] = _newest;
        newer[slot] = -1;
        if (_newest >= 0)
        {
            newer[_newest] = slot;
        }
        else
        {
            _oldest = slot;
        }

        _newest = slot;
    }

    private void Unlink(int slot)
    {
        NativeArray<int> older = _older!;
        NativeArray<int> newer = _newer!;
        int o = older[slot];
        int n = newer[slot];
        if (n >= 0)
        {
            older[n] = o;
        }
        else
        {
            _newest = o;
        }

        if (o >= 0)
        {
            newer[o] = n;
        }
        else
        {
            _oldest = n;
        }
    }

    private void EnsureSlot(int slot)
    {
        if (slot < _last.Length)
        {
            return;
        }

        int length = (int)Math.Min(_maxKeys, Math.Max(slot + 1L, _last.Length * 2L));
        _last = Grow(_last, length);
        if (_keys is not null)
        {
            _keys = Grow(_keys, length);
            _older = Grow(_older!, length);
            _newer = Grow(_newer!, length);
        }
    }

    private static NativeArray<T> Grow<T>(NativeArray<T> array, int length)
        where T : unmanaged
    {
        NativeArray<T> grown = new(length);
        array.AsSpan().CopyTo(grown.AsSpan());
        array.Dispose();
        return grown;
    }
}
