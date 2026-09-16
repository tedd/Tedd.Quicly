using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session.Engines;

namespace Tedd.Quicly.Core.Session;

// Game thread: the fan-out send path (ARCHITECTURE.md §4.1, docs/design/session-layer.md §4.1).
public sealed unsafe partial class QuiclyPeer
{
    /// <summary>
    /// Sends <paramref name="length"/> bytes of a reference-counted lease without copying them: one serialisation can be
    /// handed to many peers at once (a server's fan-out). The peer takes one reference
    /// (<see cref="SharedLeaseTable.Retain"/>) when the send is admitted and releases it exactly once
    /// (<see cref="SharedLeaseTable.Release"/>) when the transport has released the payload, when the send is discarded
    /// (expired, canceled, refused) or when the session closes — so the block goes back to the pool when the last peer is
    /// done with it. A rejected send takes no reference at all.
    /// </summary>
    /// <remarks>
    /// <para>Like <see cref="SendPinned"/> over the lease's memory: zero copy, no handle, and the bytes must stay unchanged
    /// until every peer's send completed. The payload is <b>never</b> compressed even on an LZ4 channel — the point of a
    /// shared lease is that the bytes are serialised (and, if wanted, compressed) once by the caller instead of once per
    /// peer — so <c>RawLength</c> on the wire is 0 and the message must fit the channel's limits as it is.</para>
    /// <para>Works on every channel mode the peer implements today: the unreliable datagram modes (alone or packed into a
    /// container, where the reference is released as soon as the bytes were copied into the container),
    /// <see cref="Channels.ChannelMode.ReliableOrdered"/> (the lease travels in a carrier's segment run and the reference is
    /// released when the carrier is acknowledged) and <see cref="Channels.ChannelMode.ReliableLatest"/> (the key's live
    /// value holds the reference for as long as that version may be retransmitted — up to 30 s — and releases it when the
    /// value is acknowledged, superseded or fails).</para>
    /// <para>With <see cref="PeerOptions.ThreadSafeSend"/> a call from a thread other than the game thread copies the bytes
    /// into a send lease at the call and takes no reference, exactly as <see cref="SendPinned"/> does there.</para>
    /// </remarks>
    /// <param name="header">Channel and key.</param>
    /// <param name="table">The table that counts the lease's references (the one that shared it).</param>
    /// <param name="lease">The shared payload, from <see cref="SharedLeaseTable.Share"/>.</param>
    /// <param name="length">Payload bytes at the start of the lease.</param>
    /// <param name="options">Mode, tracking, expiry.</param>
    /// <returns>The admission result (and the token of a tracked send).</returns>
    /// <exception cref="ArgumentNullException"><paramref name="table"/> is <see langword="null"/>.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="length"/> is negative or exceeds the lease (a lease that refers to no block has length 0, so it can only
    /// carry an empty payload), or the channel is keyed and the key exceeds 2^62 − 1.
    /// </exception>
    /// <exception cref="InvalidOperationException">
    /// <paramref name="lease"/> is not currently shared by <paramref name="table"/> (<see cref="SharedLeaseTable.Retain"/> rejects
    /// it when the send commits).
    /// </exception>
    /// <exception cref="ObjectDisposedException">The peer is disposed.</exception>
    public SendResult SendShared(in SendHeader header, SharedLeaseTable table, in SharedLease lease, int length, SendOptions options = default)
    {
        ArgumentNullException.ThrowIfNull(table);
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(length, lease.Lease.Length);
        SendRequest request = default;
        request.Kind = SendPayloadKind.Shared;
        request.SharedTable = table;
        request.Shared = lease;
        request.Length = length;
        return Submit(in header, ref request, in options);
    }
}
