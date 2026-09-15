using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Threading;

namespace Tedd.Quicly.Server;

/// <summary>One peer's outstanding part of a <see cref="QuiclyServer.SendShared"/>: its token and the shared reference it holds.</summary>
internal struct SharedEntry
{
    /// <summary>The tracked send's token.</summary>
    public SendToken Token;

    /// <summary>The shared lease this entry holds one reference to.</summary>
    public SharedLease Lease;

    /// <summary>The armed BufferReleased wait (consumed exactly once when it completes).</summary>
    public ValueTask<DeliveryStatus> Wait;

    /// <summary>Whether <see cref="Wait"/> was armed.</summary>
    public bool Armed;

    /// <summary>Next entry of the same peer, or -1 (also the free-list link).</summary>
    public int Next;
}

/// <summary>
/// How <see cref="QuiclyServer.SendShared"/> talks to a peer: send a pinned payload, then learn when the transport released
/// it. The default goes through the peer's public API; tests substitute a fake to exercise the admitted path while the
/// delivery engines are placeholders.
/// </summary>
internal unsafe interface ISharedSendPort
{
    /// <summary>Sends <paramref name="length"/> bytes at <paramref name="payload"/> (tracked).</summary>
    SendResult Send(QuiclyPeer peer, in SendHeader header, byte* payload, int length, SendOptions options);

    /// <summary>True once the transport released the payload of <paramref name="entry"/> (game thread).</summary>
    bool TryRelease(QuiclyPeer peer, ref SharedEntry entry);

    /// <summary>The peer is closed: drop the entry's wait without waiting (the transport no longer holds the payload).</summary>
    void Abandon(QuiclyPeer peer, ref SharedEntry entry);
}

/// <summary>
/// The public-API implementation: <see cref="QuiclyPeer.SendPinned"/> (no copy, no handle; the payload must stay unchanged
/// until the BufferReleased completion of a tracked send, ARCHITECTURE.md §4.1) and a BufferReleased wait polled with
/// <see cref="ValueTask{TResult}.IsCompleted"/>, which neither allocates nor registers a continuation.
/// </summary>
internal sealed unsafe class PeerSharedSendPort : ISharedSendPort
{
    /// <summary>The shared instance.</summary>
    public static readonly PeerSharedSendPort Instance = new();

    /// <inheritdoc/>
    public SendResult Send(QuiclyPeer peer, in SendHeader header, byte* payload, int length, SendOptions options) =>
        peer.SendPinned(in header, payload, length, options);

    /// <inheritdoc/>
    public bool TryRelease(QuiclyPeer peer, ref SharedEntry entry)
    {
        if (!entry.Armed)
        {
            // Completes synchronously (no source object) when the stage is already done or the token is stale.
            entry.Wait = peer.WaitAsync(entry.Token, CompletionStage.BufferReleased);
            entry.Armed = true;
        }

        if (!entry.Wait.IsCompleted)
        {
            return false;
        }

        _ = entry.Wait.Result; // consume exactly once: an unconsumed wait keeps the completion slot alive
        entry.Wait = default;
        return true;
    }

    /// <inheritdoc/>
    public void Abandon(QuiclyPeer peer, ref SharedEntry entry)
    {
        if (entry.Armed && entry.Wait.IsCompleted)
        {
            _ = entry.Wait.Result;
        }

        entry.Wait = default;
    }
}
