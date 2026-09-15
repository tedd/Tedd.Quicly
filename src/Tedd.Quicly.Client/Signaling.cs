using System.Net;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Client;

/// <summary>
/// Wakes <see cref="QuiclyClient.ConnectAsync"/> when a transport callback arrives, so it polls the connecting peer promptly
/// instead of on a fixed interval (the peer exposes no public "has work" signal). Any thread may set it.
/// </summary>
internal sealed class WorkSignal : IDisposable
{
    private readonly SemaphoreSlim _semaphore = new(0);
    private int _pending;

    /// <summary>Releases a waiter (or the next one) unless a release is already outstanding; never blocks.</summary>
    public void Set()
    {
        if (Interlocked.Exchange(ref _pending, 1) != 0)
        {
            return;
        }

        try
        {
            _semaphore.Release();
        }
        catch (ObjectDisposedException)
        {
            // A late callback after the client was disposed.
        }
    }

    /// <summary>Waits until <see cref="Set"/> or <paramref name="timeout"/>.</summary>
    /// <remarks>
    /// The pending flag is cleared before waiting, so a <see cref="Set"/> racing the wait releases again and is never lost;
    /// at worst a stale release makes the next wait return at once.
    /// </remarks>
    public async ValueTask WaitAsync(TimeSpan timeout, CancellationToken cancellationToken)
    {
        Volatile.Write(ref _pending, 0);
        await _semaphore.WaitAsync(timeout, cancellationToken).ConfigureAwait(false);
    }

    public void Dispose() => _semaphore.Dispose();
}

/// <summary>Hands every connection a <see cref="SignalingSink"/> in front of the peer's own sink.</summary>
internal sealed class SignalingConnector(ITransportConnector inner, WorkSignal signal) : ITransportConnector
{
    public ITransport Connect(EndPoint endpoint, string? serverName, ITransportSink sink) =>
        inner.Connect(endpoint, serverName, new SignalingSink(sink, signal));
}

/// <summary>Forwards every callback to the peer's sink, then sets the client's <see cref="WorkSignal"/>.</summary>
internal sealed class SignalingSink(ITransportSink inner, WorkSignal signal) : ITransportSink
{
    public void OnConnected(in TransportConnectedInfo info)
    {
        inner.OnConnected(in info);
        signal.Set();
    }

    public void OnDatagramReceived(ReadOnlySpan<byte> payload)
    {
        inner.OnDatagramReceived(payload);
        signal.Set();
    }

    public void OnPeerStreamStarted(TransportStreamId id, StreamKind kind)
    {
        inner.OnPeerStreamStarted(id, kind);
        signal.Set();
    }

    public void OnStreamStarted(TransportStreamId id, ulong context, TransportStatus status)
    {
        inner.OnStreamStarted(id, context, status);
        signal.Set();
    }

    public ReceiveResult OnStreamReceived(TransportStreamId id, ReadOnlySpan<TransportSegment> segments, ulong absoluteOffset, bool fin)
    {
        ReceiveResult result = inner.OnStreamReceived(id, segments, absoluteOffset, fin);
        signal.Set();
        return result;
    }

    public void OnStreamSendCompleted(TransportStreamId id, ulong context, bool canceled)
    {
        inner.OnStreamSendCompleted(id, context, canceled);
        signal.Set();
    }

    public void OnDatagramSendStateChanged(ulong context, DatagramSendState state)
    {
        inner.OnDatagramSendStateChanged(context, state);
        signal.Set();
    }

    public void OnStreamAborted(TransportStreamId id, ulong errorCode, StreamAbortDirection direction)
    {
        inner.OnStreamAborted(id, errorCode, direction);
        signal.Set();
    }

    public void OnStreamPeerSendShutdown(TransportStreamId id)
    {
        inner.OnStreamPeerSendShutdown(id);
        signal.Set();
    }

    public void OnStreamShutdownComplete(TransportStreamId id)
    {
        inner.OnStreamShutdownComplete(id);
        signal.Set();
    }

    public void OnDatagramCapabilityChanged(bool enabled, int maxPayload)
    {
        inner.OnDatagramCapabilityChanged(enabled, maxPayload);
        signal.Set();
    }

    public void OnIdealSendBufferSize(TransportStreamId id, ulong bytes)
    {
        inner.OnIdealSendBufferSize(id, bytes);
        signal.Set();
    }

    public void OnStreamsAvailable(ushort bidirectional, ushort unidirectional)
    {
        inner.OnStreamsAvailable(bidirectional, unidirectional);
        signal.Set();
    }

    public void OnPeerAddressChanged(in TransportConnectedInfo info)
    {
        inner.OnPeerAddressChanged(in info);
        signal.Set();
    }

    public void OnClosed(TransportCloseReason reason, ulong errorCode, int transportStatus)
    {
        inner.OnClosed(reason, errorCode, transportStatus);
        signal.Set();
    }
}
