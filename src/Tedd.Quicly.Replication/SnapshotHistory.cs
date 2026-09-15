using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Replication;

/// <summary>
/// Ring of the last N serialized snapshots (variable size) in one pre-allocated byte arena, plus per-peer tracking
/// of the newest snapshot each peer is known to have received — the baseline for the next delta
/// (<see cref="DeltaCodec"/>).
/// </summary>
/// <remarks>
/// <para>
/// <b>Storage:</b> snapshots are appended to a circular byte arena in tick order; each slot records tick, offset and
/// length. A store evicts the oldest snapshots whose bytes it would overwrite (and the oldest one when all
/// <see cref="Capacity"/> slots are used). Nothing is allocated after construction.
/// </para>
/// <para>
/// <b>Ticks</b> are <see cref="uint"/> serial numbers (RFC 1982, <see cref="SerialNumber"/>) — the same width as
/// QUICLY's <c>SenderTick</c> — and must strictly increase across <see cref="Store"/> calls.
/// </para>
/// <para>
/// <b>Acknowledged baselines — pairing with QUICLY tracked unreliable sends:</b> send each peer's delta on an
/// unreliable channel with <c>SendOptions { Track = true, Context = tick }</c>. When the completion for that send
/// reports <c>Delivered</c> (the transport acknowledged the datagram, PROTOCOL.md §4.3), call
/// <c>AckBaseline(peer.Index, (uint)context)</c>. <see cref="TryGetBaseline"/> then returns the newest acknowledged
/// snapshot that is still in the ring; when it returns <see langword="false"/> the next message must be encoded
/// against an empty baseline (a full snapshot). The receiver keeps its own history of decoded snapshots keyed by
/// the same ticks, so any acknowledged baseline is guaranteed to be available there too.
/// </para>
/// <para>
/// Spans returned by <see cref="TryGet"/>, <see cref="TryGetLatest"/> and <see cref="TryGetBaseline"/> alias the arena
/// and are valid only until the next <see cref="Store"/> or <see cref="Clear"/>. Not thread-safe.
/// </para>
/// </remarks>
public sealed class SnapshotHistory
{
    /// <summary>Default number of snapshot slots.</summary>
    public const int DefaultCapacity = 32;

    /// <summary>Default arena size in bytes.</summary>
    public const int DefaultArenaBytes = 256 * 1024;

    /// <summary>Default number of peers tracked.</summary>
    public const int DefaultMaxPeers = 64;

    private readonly byte[] _arena;
    private readonly uint[] _ticks;
    private readonly int[] _offsets;
    private readonly int[] _lengths;
    private readonly uint[] _peerTick;
    private readonly bool[] _peerHasTick;
    private int _oldest;
    private int _count;
    private int _head;

    /// <summary>Creates a history.</summary>
    /// <param name="capacity">Number of snapshot slots (at least 1).</param>
    /// <param name="arenaBytes">Bytes reserved for snapshot data (at least 1); a snapshot larger than this cannot be stored.</param>
    /// <param name="maxPeers">Number of peer indices (0..maxPeers−1) whose baselines are tracked; 0 or more.</param>
    /// <exception cref="ArgumentOutOfRangeException">An argument is out of range.</exception>
    public SnapshotHistory(int capacity = DefaultCapacity, int arenaBytes = DefaultArenaBytes, int maxPeers = DefaultMaxPeers)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        ArgumentOutOfRangeException.ThrowIfLessThan(arenaBytes, 1);
        ArgumentOutOfRangeException.ThrowIfNegative(maxPeers);
        _arena = new byte[arenaBytes];
        _ticks = new uint[capacity];
        _offsets = new int[capacity];
        _lengths = new int[capacity];
        _peerTick = new uint[maxPeers];
        _peerHasTick = new bool[maxPeers];
    }

    /// <summary>Number of snapshot slots.</summary>
    public int Capacity => _ticks.Length;

    /// <summary>Number of snapshots currently stored.</summary>
    public int Count => _count;

    /// <summary>Size of the byte arena.</summary>
    public int ArenaBytes => _arena.Length;

    /// <summary>Number of peer indices tracked.</summary>
    public int MaxPeers => _peerTick.Length;

    /// <summary>Copies <paramref name="snapshot"/> into the ring as the snapshot of <paramref name="tick"/>, evicting old snapshots as needed.</summary>
    /// <param name="tick">The snapshot's tick; must be newer (serial arithmetic) than every stored tick.</param>
    /// <param name="snapshot">The serialized snapshot (may be empty).</param>
    /// <returns><see langword="false"/> (nothing changes) when the snapshot is larger than the arena or the tick is not newer than the latest stored tick.</returns>
    public bool Store(uint tick, ReadOnlySpan<byte> snapshot)
    {
        int length = snapshot.Length;
        if (length > _arena.Length || (_count > 0 && !SerialNumber.IsNewer(tick, _ticks[Slot(_count - 1)])))
        {
            return false;
        }

        if (_count == _ticks.Length)
        {
            EvictOldest();
        }

        if (_count == 0)
        {
            _head = 0;
        }

        int head = _head;
        bool wrap = head + length > _arena.Length;
        int offset = wrap ? 0 : head;

        // Live bytes form one circular region that starts at the oldest snapshot and ends at _head, so the slots the
        // new bytes would overwrite (plus, after a wrap, the skipped tail between _head and the arena end, which holds
        // the oldest snapshots) are a prefix of the FIFO order. Zero-length slots occupy nothing; they only go when a
        // later slot must.
        int evict = 0;
        for (int i = 0; i < _count; i++)
        {
            int slot = Slot(i);
            int slotLength = _lengths[slot];
            if (slotLength == 0)
            {
                continue;
            }

            int slotOffset = _offsets[slot];
            bool overwritten = slotOffset < offset + length && offset < slotOffset + slotLength;
            bool skipped = wrap && slotOffset >= head;
            if (!overwritten && !skipped)
            {
                break;
            }

            evict = i + 1;
        }

        for (; evict > 0; evict--)
        {
            EvictOldest();
        }

        int newSlot = Slot(_count);
        _ticks[newSlot] = tick;
        _offsets[newSlot] = offset;
        _lengths[newSlot] = length;
        _count++;
        snapshot.CopyTo(_arena.AsSpan(offset));
        _head = offset + length;
        return true;
    }

    /// <summary>Looks up the snapshot of <paramref name="tick"/>.</summary>
    /// <param name="tick">The tick.</param>
    /// <param name="snapshot">The snapshot bytes (valid until the next <see cref="Store"/>), or empty when not found.</param>
    /// <returns><see langword="true"/> when the snapshot is still in the ring.</returns>
    public bool TryGet(uint tick, out ReadOnlySpan<byte> snapshot)
    {
        int slot = FindSlot(tick);
        if (slot < 0)
        {
            snapshot = default;
            return false;
        }

        snapshot = _arena.AsSpan(_offsets[slot], _lengths[slot]);
        return true;
    }

    /// <summary>Returns <see langword="true"/> when the snapshot of <paramref name="tick"/> is still in the ring.</summary>
    /// <param name="tick">The tick.</param>
    public bool Contains(uint tick) => FindSlot(tick) >= 0;

    /// <summary>Returns the newest stored snapshot.</summary>
    /// <param name="tick">Its tick, or 0 when empty.</param>
    /// <param name="snapshot">Its bytes (valid until the next <see cref="Store"/>), or empty.</param>
    /// <returns><see langword="false"/> when the history is empty.</returns>
    public bool TryGetLatest(out uint tick, out ReadOnlySpan<byte> snapshot)
    {
        if (_count == 0)
        {
            tick = 0;
            snapshot = default;
            return false;
        }

        int slot = Slot(_count - 1);
        tick = _ticks[slot];
        snapshot = _arena.AsSpan(_offsets[slot], _lengths[slot]);
        return true;
    }

    /// <summary>
    /// Records that <paramref name="peerIndex"/> has received the snapshot of <paramref name="tick"/> (typically from a
    /// <c>Delivered</c> completion whose send context was the tick). The peer's baseline only moves forward.
    /// </summary>
    /// <param name="peerIndex">The peer index, 0..<see cref="MaxPeers"/>−1.</param>
    /// <param name="tick">The acknowledged tick.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="tick"/> became the peer's baseline; <see langword="false"/> when it is
    /// no longer (or never was) in the ring, or is older than the peer's current baseline.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="peerIndex"/> is out of range.</exception>
    public bool AckBaseline(int peerIndex, uint tick)
    {
        ValidatePeer(peerIndex);
        if (FindSlot(tick) < 0)
        {
            return false;
        }

        if (_peerHasTick[peerIndex])
        {
            uint current = _peerTick[peerIndex];
            if (!SerialNumber.IsNewer(tick, current) && FindSlot(current) >= 0)
            {
                return false;
            }
        }

        _peerTick[peerIndex] = tick;
        _peerHasTick[peerIndex] = true;
        return true;
    }

    /// <summary>Returns the baseline to encode the next delta for <paramref name="peerIndex"/> against.</summary>
    /// <param name="peerIndex">The peer index, 0..<see cref="MaxPeers"/>−1.</param>
    /// <param name="tick">The baseline tick, or 0 when there is none.</param>
    /// <param name="snapshot">The baseline bytes (valid until the next <see cref="Store"/>), or empty when there is none.</param>
    /// <returns>
    /// <see langword="true"/> when the peer's newest acknowledged snapshot is still in the ring; <see langword="false"/>
    /// means "none": send a full snapshot (encode against an empty baseline).
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="peerIndex"/> is out of range.</exception>
    public bool TryGetBaseline(int peerIndex, out uint tick, out ReadOnlySpan<byte> snapshot)
    {
        ValidatePeer(peerIndex);
        if (_peerHasTick[peerIndex])
        {
            tick = _peerTick[peerIndex];
            if (TryGet(tick, out snapshot))
            {
                return true;
            }
        }

        tick = 0;
        snapshot = default;
        return false;
    }

    /// <summary>Forgets the baseline of <paramref name="peerIndex"/> (new connection, new epoch, or a peer slot reused).</summary>
    /// <param name="peerIndex">The peer index, 0..<see cref="MaxPeers"/>−1.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="peerIndex"/> is out of range.</exception>
    public void ResetPeer(int peerIndex)
    {
        ValidatePeer(peerIndex);
        _peerHasTick[peerIndex] = false;
        _peerTick[peerIndex] = 0;
    }

    /// <summary>Removes every snapshot and every peer baseline.</summary>
    public void Clear()
    {
        _count = 0;
        _oldest = 0;
        _head = 0;
        Array.Clear(_peerHasTick);
        Array.Clear(_peerTick);
    }

    private int Slot(int logical)
    {
        int slot = _oldest + logical;
        return slot >= _ticks.Length ? slot - _ticks.Length : slot;
    }

    private int FindSlot(uint tick)
    {
        // Newest first: baselines and lookups are almost always recent.
        for (int i = _count - 1; i >= 0; i--)
        {
            int slot = Slot(i);
            if (_ticks[slot] == tick)
            {
                return slot;
            }
        }

        return -1;
    }

    private void EvictOldest()
    {
        _oldest = Slot(1);
        _count--;
    }

    private void ValidatePeer(int peerIndex)
    {
        if ((uint)peerIndex >= (uint)_peerTick.Length)
        {
            ThrowPeer(peerIndex);
        }
    }

    private static void ThrowPeer(int peerIndex) =>
        throw new ArgumentOutOfRangeException(nameof(peerIndex), peerIndex, "Peer index is outside the tracked range.");
}
