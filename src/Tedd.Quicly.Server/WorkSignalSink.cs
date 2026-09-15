using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Server;

/// <summary>
/// The sink a server hands to an accepted transport. It forwards every callback to the peer's own sink and then marks the
/// peer's slot as having work, so <see cref="QuiclyServer.PollAll"/> polls only peers whose transport did something (the
/// peer offers no public "has work" signal). It also carries the connection's per-address key (guarded by the server's
/// gate) and tracks when the peer's memory is freed, so the server's shared pool is disposed only after every peer let go.
/// </summary>
/// <remarks>Callbacks never throw: the peer's sink wraps its own work, and the server's parts are lock-and-count only.</remarks>
internal sealed class WorkSignalSink : ITransportSink
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
    public WorkSignalSink(QuiclyServer server, int slot, ITransportSink inner)
    {
        _server = server;
        _slot = slot;
        _inner = inner;
    }

    /// <summary>The key the connection is counted under (guarded by the server's gate).</summary>
    public AddressKey AddressKey;

    /// <summary>Whether <see cref="AddressKey"/> is set (guarded by the server's gate).</summary>
    public bool HasAddress;

    /// <summary>True once the server released the slot: callbacks no longer mark it.</summary>
    public bool IsRetired => Volatile.Read(ref _retired) != 0;

    /// <summary>Stops marking the slot (called under the server's gate when the slot is released).</summary>
    public void RetireLocked() => Volatile.Write(ref _retired, 1);

    /// <summary>The server disposed the peer.</summary>
    public void MarkReleased() => SetLifetime(LifetimeReleased);

    /// <summary>The address changed to one over the per-address limit: the game thread closes the connection.</summary>
    public void RequestLimitClose()
    {
        Volatile.Write(ref _limitClose, 1);
        Signal();
    }

    /// <summary>Takes a pending limit-close request (game thread).</summary>
    public bool TakeLimitClose() => Volatile.Read(ref _limitClose) != 0 && Interlocked.Exchange(ref _limitClose, 0) != 0;

    public void OnConnected(in TransportConnectedInfo info)
    {
        _inner.OnConnected(in info);
        Signal();
    }

    public void OnDatagramReceived(ReadOnlySpan<byte> payload)
    {
        _inner.OnDatagramReceived(payload);
        Signal();
    }

    public void OnPeerStreamStarted(TransportStreamId id, StreamKind kind)
    {
        _inner.OnPeerStreamStarted(id, kind);
        Signal();
    }

    public void OnStreamStarted(TransportStreamId id, ulong context, TransportStatus status)
    {
        _inner.OnStreamStarted(id, context, status);
        Signal();
    }

    public ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin)
    {
        ReceiveResult result = _inner.OnStreamReceived(id, segments, absoluteOffset, fin);
        Signal();
        return result;
    }

    public void OnStreamSendCompleted(TransportStreamId id, ulong context, bool canceled)
    {
        _inner.OnStreamSendCompleted(id, context, canceled);
        Signal();
    }

    public void OnDatagramSendStateChanged(ulong context, DatagramSendState state)
    {
        _inner.OnDatagramSendStateChanged(context, state);
        Signal();
    }

    public void OnStreamAborted(TransportStreamId id, ulong errorCode, StreamAbortDirection direction)
    {
        _inner.OnStreamAborted(id, errorCode, direction);
        Signal();
    }

    public void OnStreamPeerSendShutdown(TransportStreamId id)
    {
        _inner.OnStreamPeerSendShutdown(id);
        Signal();
    }

    public void OnStreamShutdownComplete(TransportStreamId id)
    {
        _inner.OnStreamShutdownComplete(id);
        Signal();
    }

    public void OnDatagramCapabilityChanged(bool enabled, int maxPayload)
    {
        _inner.OnDatagramCapabilityChanged(enabled, maxPayload);
        Signal();
    }

    public void OnIdealSendBufferSize(TransportStreamId id, ulong bytes)
    {
        _inner.OnIdealSendBufferSize(id, bytes);
        Signal();
    }

    public void OnStreamsAvailable(ushort bidirectional, ushort unidirectional)
    {
        _inner.OnStreamsAvailable(bidirectional, unidirectional);
        Signal();
    }

    public void OnPeerAddressChanged(in TransportConnectedInfo info)
    {
        _inner.OnPeerAddressChanged(in info);
        _server.OnAddressChanged(this, info.RemoteEndPoint);
        Signal();
    }

    public void OnClosed(TransportCloseReason reason, ulong errorCode, int transportStatus)
    {
        _inner.OnClosed(reason, errorCode, transportStatus);
        Signal();
        SetLifetime(LifetimeClosed);
    }

    private void Signal()
    {
        if (Volatile.Read(ref _retired) == 0)
        {
            _server.MarkWork(_slot);
        }
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
