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
/// length. A store evicts the oldest snapshot when all <see cref="Capacity"/> slots are used, and then every older
/// snapshot that is in its way: those whose bytes it would overwrite and, when the new snapshot does not fit
/// between the write position and the arena end (it then wraps to offset 0), also the older snapshots in that
/// skipped tail, even though their bytes are not overwritten (eviction is strictly oldest-first, so the retained
/// snapshots are always the newest ones, contiguous in tick order). An undersized arena therefore keeps fewer
/// than <see cref="Capacity"/> snapshots. <b>Sizing:</b> with <c>ArenaBytes ≥ (Capacity + 1) × S</c>, where
/// <c>S</c> is the largest snapshot stored, the arena never evicts anything the slot limit would keep
/// (<c>2 × Capacity × S</c> leaves generous headroom). Nothing is allocated after construction.
/// </para>
/// <para>
/// <b>Ticks</b> are <see cref="uint"/> serial numbers (RFC 1982, <see cref="SerialNumber"/>) — the same width as
/// QUICLY's <c>SenderTick</c> — and must strictly increase across <see cref="Store"/> calls.
/// </para>
/// <para>
/// <b>Acknowledged baselines — use application-level acknowledgements.</b> A baseline may only be a snapshot the
/// receiver has actually <em>decoded and stored</em>, and only the receiving application can know that. The receiver
/// keeps its own <see cref="SnapshotHistory"/> of decoded snapshots and echoes the newest tick it decoded in every
/// message it sends back (for example next to its <see cref="InputBatch"/> in the same datagram, which it sends every
/// tick anyway, so a lost echo is repaired by the next one). On receiving an echo the sender calls
/// <c>AckBaseline(peer, echoedTick)</c>; out-of-order echoes are harmless because the baseline only moves forward.
/// Each delta message carries its own tick and the baseline tick it was encoded against (or "none"), so the
/// receiver can look the baseline up with <see cref="TryGet"/>.
/// </para>
/// <para>
/// <b>Do not use transport acknowledgements</b> (a tracked unreliable send completing <c>Delivered</c>) as baseline
/// acknowledgements. PROTOCOL.md §4.3: a QUIC acknowledgement proves delivery to the peer's transport, not that the
/// application processed the message. The receiver can drop a transport-acknowledged datagram — an
/// <c>UnreliableSequenced</c> channel drops one that arrives after a newer one, receive-side queue and budget limits
/// (ADR 0009, PROTOCOL.md §7) drop under load, and a history that only accepts increasing ticks cannot store one
/// that arrives out of order on an <c>UnreliableUnordered</c> channel. A sender that then encodes against that tick
/// produces a delta the receiver cannot decode; that datagram is transport-acknowledged in turn and becomes the next
/// baseline, and the stream never recovers although every later datagram arrives. <c>Delivered</c> / <c>Lost</c>
/// completions remain useful for statistics, but they must not call <see cref="AckBaseline"/>.
/// </para>
/// <para>
/// <b>Guarantee and sizing:</b> with application-level acknowledgements, if the receiver's history has at least as
/// many slots as the sender's and its arena is sized as above, every baseline <see cref="TryGetBaseline"/> returns is
/// still in the receiver's history when the delta arrives (the receiver cannot have decoded more than
/// <c>Capacity − 1</c> ticks newer than the baseline while the sender still holds it).
/// </para>
/// <para>
/// <b>Recovery:</b> a receiver that cannot find a baseline (its history was cleared, e.g. after a reconnect or a
/// load hitch that exceeded its sizing) asks for a full snapshot on a reliable or <c>RequestResponse</c> channel; the
/// sender answers with <see cref="ResetPeer"/>, which makes the next messages full snapshots and ignores echoes of
/// ticks stored before the reset (they may still be in flight on another channel, PROTOCOL.md §4.7, and refer to
/// snapshots the receiver no longer has). The first full snapshot the receiver decodes after that is echoed and
/// becomes the new baseline.
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
    private readonly ulong[] _storeSequence;
    private readonly uint[] _peerTick;
    private readonly bool[] _peerHasTick;
    private readonly ulong[] _peerFence;
    private ulong _nextStoreSequence;
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
        _storeSequence = new ulong[capacity];
        _peerTick = new uint[maxPeers];
        _peerHasTick = new bool[maxPeers];
        _peerFence = new ulong[maxPeers];
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
        _storeSequence[newSlot] = _nextStoreSequence++;
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
    /// Records that <paramref name="peerIndex"/> has decoded and stored the snapshot of <paramref name="tick"/> — an
    /// application-level acknowledgement (the tick the receiver echoes back), never a transport <c>Delivered</c>
    /// completion (see the class remarks). The peer's baseline only moves forward.
    /// </summary>
    /// <param name="peerIndex">The peer index, 0..<see cref="MaxPeers"/>−1.</param>
    /// <param name="tick">The acknowledged tick.</param>
    /// <returns>
    /// <see langword="true"/> when <paramref name="tick"/> became the peer's baseline; <see langword="false"/> when it is
    /// no longer (or never was) in the ring, is older than the peer's current baseline, or was stored before the
    /// peer's last <see cref="ResetPeer"/>.
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="peerIndex"/> is out of range.</exception>
    public bool AckBaseline(int peerIndex, uint tick)
    {
        ValidatePeer(peerIndex);
        int slot = FindSlot(tick);
        if (slot < 0 || _storeSequence[slot] < _peerFence[peerIndex])
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

    /// <summary>
    /// Forgets the baseline of <paramref name="peerIndex"/>, so the next messages are full snapshots — on a new
    /// connection or epoch, when a peer slot is reused, or when the receiver asked for a full snapshot because it is
    /// missing a baseline. Acknowledgements of snapshots stored before the reset are ignored from now on (they may
    /// still arrive, and refer to snapshots the receiver no longer has); only snapshots stored afterwards can become
    /// the peer's baseline again.
    /// </summary>
    /// <param name="peerIndex">The peer index, 0..<see cref="MaxPeers"/>−1.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="peerIndex"/> is out of range.</exception>
    public void ResetPeer(int peerIndex)
    {
        ValidatePeer(peerIndex);
        _peerHasTick[peerIndex] = false;
        _peerTick[peerIndex] = 0;
        _peerFence[peerIndex] = _nextStoreSequence;
    }

    /// <summary>Removes every snapshot and every peer baseline.</summary>
    public void Clear()
    {
        _count = 0;
        _oldest = 0;
        _head = 0;
        Array.Clear(_peerHasTick);
        Array.Clear(_peerTick);

        // Every snapshot stored from now on has a store sequence at or above every fence, so fences can go too.
        Array.Clear(_peerFence);
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
