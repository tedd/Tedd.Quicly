using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Server;

// Game thread: SendShared (one serialisation to many peers) and the tracking behind ServerStatistics.SharedSendsOutstanding.
public sealed partial class QuiclyServer
{
    private readonly PeerSet _sharedAdmitted;
    private readonly PeerSet _sharedRejected;
    private readonly byte[] _sharedStatus;
    private SharedTracking[] _sharedTracked = new SharedTracking[16];
    private int _sharedTrackedCount;
    private long _sharedAdmittedTotal;

    /// <summary>
    /// Sends one serialisation to every peer of <paramref name="set"/> without copying it (game thread): each peer takes one
    /// reference to <paramref name="lease"/> when it admits the message (<see cref="QuiclyPeer.SendShared"/>) and releases it
    /// exactly once — when its transport released the payload, when the send is discarded (expired, canceled, refused), or
    /// when its session closes — so the block returns to <see cref="Allocator"/> when the last peer is done and the caller
    /// released its own reference. A peer that refuses the message takes no reference at all.
    /// </summary>
    /// <remarks>
    /// <para>Usage: rent a block from <see cref="Allocator"/>, serialise into it, <c>SharedLeases.Share(block, 1)</c>, call
    /// <see cref="SendShared"/>, then <c>SharedLeases.Release(lease)</c>. Do not modify the block until its reference count
    /// dropped to zero.</para>
    /// <para>The payload is <b>never compressed</b>, even on an LZ4 channel: the point of a shared lease is that the bytes
    /// are serialised (and, if wanted, compressed) once by the caller instead of once per peer, so the message must fit each
    /// channel's limits as it is (ARCHITECTURE.md §4.1). <see cref="SendOptions.Track"/> is the caller's choice — the peers
    /// release their own references, so the server needs no token of its own — and a tracked send's per-peer tokens are not
    /// reported: use <see cref="QuiclyPeer.SendShared"/> directly when a single peer's completion matters.</para>
    /// <para>A peer the application disposed itself, and a free slot, are reported <see cref="SendStatus.NotConnected"/>.
    /// Allocation-free in steady state.</para>
    /// </remarks>
    /// <param name="set">The target peers (indices of this server's slot table).</param>
    /// <param name="header">Channel and key.</param>
    /// <param name="lease">A live shared lease of <see cref="SharedLeases"/>; the caller keeps its own reference during the call.</param>
    /// <param name="length">Payload bytes at the start of the block.</param>
    /// <param name="options">Mode, tracking, expiry.</param>
    /// <returns>Admitted and rejected masks with per-peer statuses (valid until the next call).</returns>
    /// <exception cref="ArgumentException">The set is larger than this server's slot table, or the lease is not a live shared lease of <see cref="SharedLeases"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="length"/> is negative or exceeds the block.</exception>
    /// <exception cref="ObjectDisposedException">The server is disposed.</exception>
    public SharedSendResult SendShared(PeerSet set, in SendHeader header, SharedLease lease, int length, SendOptions options = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(set);
        if (set.Capacity > _slots.Length)
        {
            throw new ArgumentException("The set is larger than this server's slot table; create sets with CreateSet.", nameof(set));
        }

        if (!lease.IsValid || _sharedLeases.GetReferenceCount(in lease) <= 0)
        {
            throw new ArgumentException("The lease is not a live shared lease of this server's SharedLeases.", nameof(lease));
        }

        ArgumentOutOfRangeException.ThrowIfNegative(length);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(length, lease.Lease.Length);

        _sharedAdmitted.ClearCore();
        _sharedRejected.ClearCore();
        int admitted = 0;
        int rejected = 0;
        try
        {
            foreach (int slot in set)
            {
                QuiclyPeer? peer = _slots[slot].Peer;
                if (peer is null || peer.IsDisposed)
                {
                    _sharedRejected.AddCore(slot);
                    _sharedStatus[slot] = (byte)SendStatus.NotConnected;
                    rejected++;
                    continue;
                }

                SendResult result = peer.SendShared(in header, _sharedLeases, in lease, length, options);
                if (result.IsAdmitted)
                {
                    _sharedAdmitted.AddCore(slot);
                    _sharedStatus[slot] = (byte)SendStatus.Admitted;
                    admitted++;
                }
                else
                {
                    _sharedRejected.AddCore(slot);
                    _sharedStatus[slot] = (byte)result.Status;
                    rejected++;
                }
            }
        }
        finally
        {
            if (admitted != 0)
            {
                _sharedAdmittedTotal += admitted;
                Track(in lease, admitted);
            }
        }

        return new SharedSendResult(admitted, rejected, _sharedAdmitted, _sharedRejected, _sharedStatus);
    }

    /// <summary>
    /// References handed to peers that are not released yet, read from the leases' reference counts now: the peers release on
    /// their own (in a Poll, a Flush, a close or a transport callback) without telling the server, so this is recomputed on
    /// demand. Exact while the caller keeps the references it held when it sent; a caller that releases its own reference
    /// earlier makes it a lower bound (never an over-count).
    /// </summary>
    private int SharedReferencesOutstanding()
    {
        PruneShared();
        int outstanding = 0;
        for (int i = 0; i < _sharedTrackedCount; i++)
        {
            outstanding += Outstanding(in _sharedTracked[i]);
        }

        return outstanding;
    }

    /// <summary>Forgets the payloads no peer holds a reference to any more. Allocation-free.</summary>
    private void PruneShared()
    {
        for (int i = _sharedTrackedCount - 1; i >= 0; i--)
        {
            if (Outstanding(in _sharedTracked[i]) == 0)
            {
                _sharedTracked[i] = _sharedTracked[--_sharedTrackedCount];
                _sharedTracked[_sharedTrackedCount] = default;
            }
        }
    }

    /// <summary>
    /// How many of one payload's peer references are still out: its reference count now, minus the references that were not
    /// the server's when it sent (the caller's own), clamped to what the server handed out.
    /// </summary>
    private int Outstanding(in SharedTracking tracked) =>
        Math.Clamp(_sharedLeases.GetReferenceCount(in tracked.Lease) - tracked.Others, 0, tracked.Peers);

    /// <summary>Records that <paramref name="peers"/> peers took a reference to <paramref name="lease"/> (merged with the newest entry of the same block).</summary>
    private void Track(in SharedLease lease, int peers)
    {
        if (_sharedTrackedCount != 0)
        {
            ref SharedTracking newest = ref _sharedTracked[_sharedTrackedCount - 1];
            if (newest.Lease == lease)
            {
                newest.Peers += peers;
                return;
            }
        }

        if (_sharedTrackedCount == _sharedTracked.Length)
        {
            PruneShared();
            if (_sharedTrackedCount == _sharedTracked.Length)
            {
                Array.Resize(ref _sharedTracked, _sharedTracked.Length * 2);
            }
        }

        // Whatever the block was referenced by besides this send (the caller's own reference, by the documented usage).
        int others = Math.Max(0, _sharedLeases.GetReferenceCount(in lease) - peers);
        _sharedTracked[_sharedTrackedCount++] = new SharedTracking { Lease = lease, Peers = peers, Others = others };
    }

    /// <summary>One shared payload the server handed out: how many peer references it gave it, and what else referenced it then.</summary>
    private struct SharedTracking
    {
        public SharedLease Lease;

        public int Peers;

        public int Others;
    }
}
