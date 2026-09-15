using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Threading;

namespace Tedd.Quicly.Core.Session;

/// <summary>
/// Latest-wins receive hand-off for one keyed coalescing channel (ADR 0008 invariant 6): the transport thread writes a
/// complete message descriptor into a free record and exchanges the record index into the key's mailbox; a displaced
/// record is a message the game thread never saw, whose lease the transport thread returns at once. The game thread
/// claims records with <see cref="TryTake"/> and hands their indices back through an SPSC return ring.
/// </summary>
/// <remarks>
/// Records = key slots + 2, so the transport thread always finds a free record: at most one per key sits in a mailbox,
/// and the game thread holds none beyond the <see cref="TryTake"/> call (it copies the descriptor out and returns the
/// index before dispatching). Created by an engine in <see cref="Engines.ChannelEngine.Initialize"/> through
/// <see cref="PeerCore.CreateMailbox"/>; <see cref="QuiclyPeer.Poll"/> dispatches it to the channel's handler and
/// <see cref="QuiclyPeer.Drain"/> takes from it directly.
/// </remarks>
internal sealed class ReceiveMailbox : IDisposable
{
    private readonly NativeArray<ReceiveEntry> _records;
    private readonly int[] _free;
    private readonly SpscRing<int> _returned;
    private int _freeCount;

    /// <summary>Creates mailboxes for <paramref name="keySlots"/> key slots of <paramref name="channel"/>.</summary>
    /// <param name="channel">Channel id.</param>
    /// <param name="channelIndex">Dense channel index.</param>
    /// <param name="keySlots">Key slots (the channel's key table capacity).</param>
    public ReceiveMailbox(ushort channel, int channelIndex, int keySlots)
    {
        Channel = channel;
        ChannelIndex = channelIndex;
        Boxes = new Mailboxes(keySlots);
        int records = keySlots + 2;
        _records = new NativeArray<ReceiveEntry>(records);
        _free = new int[records];
        for (int i = 0; i < records; i++)
        {
            _free[i] = records - 1 - i;
        }

        _freeCount = records;
        _returned = new SpscRing<int>(records);
    }

    /// <summary>Channel id.</summary>
    public ushort Channel { get; }

    /// <summary>Dense channel index.</summary>
    public int ChannelIndex { get; }

    /// <summary>The mailbox words and dirty bitset.</summary>
    public Mailboxes Boxes { get; }

    /// <summary>Number of key slots.</summary>
    public int KeySlots => Boxes.Capacity;

    /// <summary>
    /// Posts <paramref name="entry"/> as the latest value of <paramref name="keySlot"/>. Transport thread. On success,
    /// <paramref name="displaced"/> is the lease of a value the game thread never saw (the caller returns it and counts
    /// a supersede), or empty.
    /// </summary>
    /// <param name="keySlot">Key slot.</param>
    /// <param name="entry">The complete message descriptor (its lease moves into the mailbox).</param>
    /// <param name="displaced">The displaced lease, or <see cref="BufferLease.Empty"/>.</param>
    /// <returns><see langword="false"/> only if no record is free (cannot happen with the documented sizing).</returns>
    public bool TryPost(int keySlot, in ReceiveEntry entry, out BufferLease displaced) => TryPost(keySlot, in entry, out displaced, out _);

    /// <summary>
    /// Posts <paramref name="entry"/> as the latest value of <paramref name="keySlot"/> and reports whether a value the game
    /// thread never saw was displaced (its lease is <paramref name="displaced"/>, which is empty for an empty payload).
    /// Transport thread.
    /// </summary>
    /// <param name="keySlot">Key slot.</param>
    /// <param name="entry">The complete message descriptor (its lease moves into the mailbox).</param>
    /// <param name="displaced">The displaced lease, or <see cref="BufferLease.Empty"/>.</param>
    /// <param name="replaced">True when an unseen value was displaced (count a supersede).</param>
    /// <returns><see langword="false"/> only if no record is free (cannot happen with the documented sizing).</returns>
    public bool TryPost(int keySlot, in ReceiveEntry entry, out BufferLease displaced, out bool replaced)
    {
        displaced = BufferLease.Empty;
        replaced = false;
        if (_freeCount == 0)
        {
            while (_returned.TryDequeue(out int returned))
            {
                _free[_freeCount++] = returned;
            }

            if (_freeCount == 0)
            {
                return false;
            }
        }

        int record = _free[--_freeCount];
        _records[record] = entry;
        int previous = Boxes.Post(keySlot, record);
        if (previous >= 0)
        {
            displaced = _records[previous].Lease;
            replaced = true;
            _free[_freeCount++] = previous;
        }

        return true;
    }

    /// <summary>Collects dirty key slots (game thread). Every returned key must be passed to <see cref="TryTake"/>.</summary>
    /// <param name="keySlots">Receives key slots.</param>
    /// <returns>Keys written.</returns>
    public int PopDirty(Span<int> keySlots) => Boxes.PopDirty(keySlots);

    /// <summary>Claims the latest value of <paramref name="keySlot"/> (game thread).</summary>
    /// <param name="keySlot">Key slot.</param>
    /// <param name="entry">The descriptor; its lease now belongs to the caller.</param>
    /// <returns><see langword="false"/> when the mailbox is empty.</returns>
    public bool TryTake(int keySlot, out ReceiveEntry entry)
    {
        int record = Boxes.Take(keySlot);
        if (record < 0)
        {
            entry = default;
            return false;
        }

        entry = _records[record];
        _returned.TryEnqueue(record);
        return true;
    }

    /// <summary>Returns every lease still in a mailbox (game thread, after the transport closed).</summary>
    /// <param name="core">Owner of the receive budget.</param>
    public void ReleaseAll(PeerCore core)
    {
        for (int k = 0; k < KeySlots; k++)
        {
            if (TryTake(k, out ReceiveEntry entry) && !entry.Lease.IsEmpty)
            {
                core.ReturnReceive(entry.Lease);
            }
        }
    }

    /// <summary>Frees the native memory.</summary>
    public void Dispose()
    {
        Boxes.Dispose();
        _records.Dispose();
    }
}
