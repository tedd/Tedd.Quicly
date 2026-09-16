using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Server;

/// <summary>
/// The sink a server hands to an accepted transport. It forwards every callback to the peer's own sink and adds the two
/// things a peer cannot tell the server itself: the connection's per-address accounting (<see cref="AddressKey"/>, moved by
/// <see cref="OnPeerAddressChanged"/>) and the moment the peer's native memory is free — the transport reported its close
/// <em>and</em> the server disposed the peer — so the shared buffer pool is disposed only after every peer let go.
/// </summary>
/// <remarks>
/// <para><b>Not</b> work tracking any more: <see cref="PeerOptions.WorkSignal"/> tells the server when a peer publishes
/// game-thread work (one call per <see cref="QuiclyPeer.Poll"/>, see <see cref="QuiclyServer.MarkWork"/>) and
/// <see cref="QuiclyPeer.HasPendingWork"/> is the level behind that edge, so a callback that produces nothing for the game
/// thread no longer costs a poll. Shared payload references are the peer's own as well: it takes one per admitted
/// <see cref="QuiclyPeer.SendShared"/> and releases it exactly once, and a peer disposed with a send in flight holds its
/// references until its transport reported the close (ADR 0008 invariant 1).</para>
/// <para>Callbacks never throw: the peer's sink wraps its own work, and the server's parts are lock-and-count only.</para>
/// </remarks>
internal sealed class ConnectionSink : ITransportSink
{
    private const int LifetimeReleased = 1; // the server disposed the peer
    private const int LifetimeClosed = 2;   // the transport reported OnClosed (no callback follows)

    private readonly QuiclyServer _server;
    private readonly ITransportSink _inner;
    private readonly int _slot;
    private int _retired;
    private int _lifetime;
    private int _limitClose;

    /// <summary>Wraps <paramref name="inner"/> for the peer in <paramref name="slot"/>.</summary>
    public ConnectionSink(QuiclyServer server, int slot, ITransportSink inner)
    {
        _server = server;
        _slot = slot;
        _inner = inner;
    }

    /// <summary>The key the connection is counted under (guarded by the server's gate).</summary>
    public AddressKey AddressKey;

    /// <summary>Whether <see cref="AddressKey"/> is set (guarded by the server's gate).</summary>
    public bool HasAddress;

    /// <summary>True once the server released the slot: address changes no longer move counts.</summary>
    public bool IsRetired => Volatile.Read(ref _retired) != 0;

    /// <summary>Retires the sink (called under the server's gate when the slot is released).</summary>
    public void RetireLocked() => Volatile.Write(ref _retired, 1);

    /// <summary>The server disposed the peer.</summary>
    public void MarkReleased() => SetLifetime(LifetimeReleased);

    /// <summary>The address changed to one over the per-address limit: the game thread closes the connection.</summary>
    public void RequestLimitClose()
    {
        Volatile.Write(ref _limitClose, 1);
        if (Volatile.Read(ref _retired) == 0)
        {
            _server.MarkWork(_slot); // the peer itself has no work to publish: the server must still visit the slot
        }
    }

    /// <summary>Takes a pending limit-close request (game thread).</summary>
    public bool TakeLimitClose() => Volatile.Read(ref _limitClose) != 0 && Interlocked.Exchange(ref _limitClose, 0) != 0;

    public void OnConnected(in TransportConnectedInfo info) => _inner.OnConnected(in info);

    public void OnDatagramReceived(ReadOnlySpan<byte> payload) => _inner.OnDatagramReceived(payload);

    public void OnPeerStreamStarted(TransportStreamId id, StreamKind kind) => _inner.OnPeerStreamStarted(id, kind);

    public void OnStreamStarted(TransportStreamId id, ulong context, TransportStatus status) => _inner.OnStreamStarted(id, context, status);

    public ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin) =>
        _inner.OnStreamReceived(id, segments, absoluteOffset, fin);

    public void OnStreamSendCompleted(TransportStreamId id, ulong context, bool canceled) => _inner.OnStreamSendCompleted(id, context, canceled);

    public void OnDatagramSendStateChanged(ulong context, DatagramSendState state) => _inner.OnDatagramSendStateChanged(context, state);

    public void OnStreamAborted(TransportStreamId id, ulong errorCode, StreamAbortDirection direction) => _inner.OnStreamAborted(id, errorCode, direction);

    public void OnStreamPeerSendShutdown(TransportStreamId id) => _inner.OnStreamPeerSendShutdown(id);

    public void OnStreamShutdownComplete(TransportStreamId id) => _inner.OnStreamShutdownComplete(id);

    public void OnDatagramCapabilityChanged(bool enabled, int maxPayload) => _inner.OnDatagramCapabilityChanged(enabled, maxPayload);

    public void OnIdealSendBufferSize(TransportStreamId id, ulong bytes) => _inner.OnIdealSendBufferSize(id, bytes);

    public void OnStreamsAvailable(ushort bidirectional, ushort unidirectional) => _inner.OnStreamsAvailable(bidirectional, unidirectional);

    public void OnPeerAddressChanged(in TransportConnectedInfo info)
    {
        _inner.OnPeerAddressChanged(in info);
        _server.OnAddressChanged(this, info.RemoteEndPoint);
    }

    public void OnClosed(TransportCloseReason reason, ulong errorCode, int transportStatus)
    {
        _inner.OnClosed(reason, errorCode, transportStatus); // the peer releases the payloads the transport still held here
        SetLifetime(LifetimeClosed);
    }

    private void SetLifetime(int bit)
    {
        int previous = Interlocked.Or(ref _lifetime, bit);
        if ((previous & bit) == 0 && (previous | bit) == (LifetimeReleased | LifetimeClosed))
        {
            _server.OnPeerFreed();
        }
    }
}
