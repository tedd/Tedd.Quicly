using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Threading;

namespace Tedd.Quicly.Server;

// Game thread: SendShared and the per-peer tracking of the shared references it hands out.
public sealed partial class QuiclyServer
{
    private readonly PeerSet _sharedAdmitted;
    private readonly PeerSet _sharedRejected;
    private readonly byte[] _sharedStatus;
    private readonly int[] _sharedHead;
    private SharedEntry[] _sharedEntries = new SharedEntry[64];
    private int _sharedFree = -1;
    private int _sharedNext;
    private int _sharedOutstanding;

    /// <summary>How <see cref="SendShared"/> reaches the peers (a test seam; the default uses the peers' public API).</summary>
    internal ISharedSendPort SharedPort { get; set; } = PeerSharedSendPort.Instance;

    /// <summary>
    /// Sends one serialisation to every peer of <paramref name="set"/> without copying it (game thread): each peer gets the
    /// same pinned payload (<see cref="QuiclyPeer.SendPinned"/>) and holds one reference to <paramref name="lease"/> until
    /// its transport released the payload, so the block returns to <see cref="Allocator"/> when the last peer is done and the
    /// caller released its own reference.
    /// </summary>
    /// <remarks>
    /// <para>Usage: rent a block from <see cref="Allocator"/>, serialise into it, <c>SharedLeases.Share(block, 1)</c>, call
    /// <see cref="SendShared"/>, then <c>SharedLeases.Release(lease)</c>. Do not modify the block until its reference count
    /// dropped to zero.</para>
    /// <para>Each admitted send is tracked (<see cref="SendOptions.Track"/> is forced on; the per-peer tokens are internal) so
    /// the server learns when to drop that peer's reference: it checks the BufferReleased stage of the peers it polls, so
    /// releases happen inside <see cref="PollAll"/>. A peer that closes drops its references at once. Allocation-free in
    /// steady state.</para>
    /// </remarks>
    /// <param name="set">The target peers (indices of this server's slot table; free slots are reported as <see cref="SendStatus.NotConnected"/>).</param>
    /// <param name="header">Channel and key.</param>
    /// <param name="lease">A live shared lease of <see cref="SharedLeases"/>; the caller keeps its own reference during the call.</param>
    /// <param name="length">Payload bytes at the start of the block.</param>
    /// <param name="options">Mode and expiry; tracking is always on.</param>
    /// <returns>Admitted and rejected masks with per-peer statuses (valid until the next call).</returns>
    /// <exception cref="ArgumentException">The set is larger than this server's slot table, or the lease is not a live shared lease of <see cref="SharedLeases"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="length"/> is negative or exceeds the block.</exception>
    public unsafe SharedSendResult SendShared(PeerSet set, in SendHeader header, SharedLease lease, int length, SendOptions options = default)
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

        byte* payload = _allocator.GetPointer(lease.Lease);
        SendOptions tracked = options with { Track = true };
        ISharedSendPort port = SharedPort;
        _sharedAdmitted.ClearCore();
        _sharedRejected.ClearCore();
        int admitted = 0;
        int rejected = 0;
        foreach (int slot in set)
        {
            QuiclyPeer? peer = _slots[slot].Peer;
            if (peer is null)
            {
                _sharedRejected.AddCore(slot);
                _sharedStatus[slot] = (byte)SendStatus.NotConnected;
                rejected++;
                continue;
            }

            _sharedLeases.Retain(in lease);
            SendResult result;
            try
            {
                result = port.Send(peer, in header, payload, length, tracked);
            }
            catch
            {
                _sharedLeases.Release(in lease);
                throw;
            }

            if (result.IsAdmitted)
            {
                Track(slot, result.Token, in lease);
                _sharedAdmitted.AddCore(slot);
                _sharedStatus[slot] = (byte)SendStatus.Admitted;
                admitted++;
            }
            else
            {
                _sharedLeases.Release(in lease);
                _sharedRejected.AddCore(slot);
                _sharedStatus[slot] = (byte)result.Status;
                rejected++;
            }
        }

        return new SharedSendResult(admitted, rejected, _sharedAdmitted, _sharedRejected, _sharedStatus);
    }

    private void Track(int slot, SendToken token, in SharedLease lease)
    {
        int entry = _sharedFree;
        if (entry >= 0)
        {
            _sharedFree = _sharedEntries[entry].Next;
        }
        else
        {
            if (_sharedNext == _sharedEntries.Length)
            {
                Array.Resize(ref _sharedEntries, _sharedEntries.Length * 2);
            }

            entry = _sharedNext++;
        }

        ref SharedEntry tracked = ref _sharedEntries[entry];
        tracked.Token = token;
        tracked.Lease = lease;
        tracked.Wait = default;
        tracked.Armed = false;
        tracked.Next = _sharedHead[slot];
        _sharedHead[slot] = entry;
        _sharedOutstanding++;
    }

    /// <summary>Drops the references of <paramref name="peer"/>'s shared sends whose payload the transport released.</summary>
    private void ProcessShared(int slot, QuiclyPeer peer)
    {
        ISharedSendPort port = SharedPort;
        int previous = -1;
        int entry = _sharedHead[slot];
        while (entry >= 0)
        {
            ref SharedEntry tracked = ref _sharedEntries[entry];
            int next = tracked.Next;
            if (port.TryRelease(peer, ref tracked))
            {
                if (previous < 0)
                {
                    _sharedHead[slot] = next;
                }
                else
                {
                    _sharedEntries[previous].Next = next;
                }

                FreeShared(entry);
            }
            else
            {
                previous = entry;
            }

            entry = next;
        }
    }

    /// <summary>The peer is closed (its transport holds nothing any more) or being force-closed: drop every reference it holds.</summary>
    private void ReleaseAllShared(int slot, QuiclyPeer peer)
    {
        ISharedSendPort port = SharedPort;
        int entry = _sharedHead[slot];
        _sharedHead[slot] = -1;
        while (entry >= 0)
        {
            ref SharedEntry tracked = ref _sharedEntries[entry];
            int next = tracked.Next;
            port.Abandon(peer, ref tracked);
            FreeShared(entry);
            entry = next;
        }
    }

    private void FreeShared(int entry)
    {
        ref SharedEntry tracked = ref _sharedEntries[entry];
        SharedLease lease = tracked.Lease;
        tracked = default;
        tracked.Next = _sharedFree;
        _sharedFree = entry;
        _sharedOutstanding--;
        _sharedLeases.Release(in lease);
    }
}
