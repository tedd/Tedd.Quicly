using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session.Engines;

namespace Tedd.Quicly.Core.Session;

// Game thread: the public send surface (ARCHITECTURE.md §4.1, §6). The peer resolves the channel and checks the session
// state; admission itself belongs to the engine of the channel's mode (ChannelEngine.Admit).
public sealed unsafe partial class QuiclyPeer
{
    /// <summary>Most payload pages of one <see cref="SendGather"/>.</summary>
    public const int MaxGatherSegments = 8;

    /// <summary>
    /// Rents library memory to serialise a message into (zero-copy path: fill it, then <see cref="SendOwned"/>). Counts
    /// against the send budget until it is sent and completed, or given back with <see cref="ReturnBuffer"/>.
    /// </summary>
    /// <param name="size">Bytes needed.</param>
    /// <returns>The lease, or <see cref="BufferLease.Empty"/> when the send budget or the pool is exhausted.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="size"/> is negative.</exception>
    public BufferLease RentBuffer(int size)
    {
        ThrowIfDisposed();
        ArgumentOutOfRangeException.ThrowIfNegative(size);
        return _core.TryRentSend(size, out BufferLease lease) ? lease : BufferLease.Empty;
    }

    /// <summary>The memory of a lease from <see cref="RentBuffer"/> (its whole block).</summary>
    /// <param name="lease">The lease.</param>
    /// <returns>The block; empty for an empty lease.</returns>
    public Span<byte> GetBufferSpan(in BufferLease lease)
    {
        ThrowIfDisposed();
        return lease.IsEmpty ? default : _core.GetSpan(in lease);
    }

    /// <summary>Gives back a lease from <see cref="RentBuffer"/> that will not be sent.</summary>
    /// <param name="lease">The lease.</param>
    public void ReturnBuffer(in BufferLease lease)
    {
        ThrowIfDisposed();
        _core.ReturnSend(in lease);
    }

    /// <summary>Sends a copy of <paramref name="payload"/> (the span may be reused as soon as the call returns).</summary>
    /// <param name="header">Channel and key.</param>
    /// <param name="payload">The message.</param>
    /// <param name="options">Mode, tracking, expiry.</param>
    /// <returns>The admission result (and the token of a tracked send).</returns>
    public SendResult SendCopy(in SendHeader header, ReadOnlySpan<byte> payload, SendOptions options = default)
    {
        SendRequest request = default;
        request.Kind = SendPayloadKind.Copy;
        request.Source = payload;
        request.Length = payload.Length;
        return Submit(in header, ref request, in options);
    }

    /// <summary>Sends <paramref name="length"/> bytes of a lease from <see cref="RentBuffer"/>; ownership moves to the peer when admitted.</summary>
    /// <param name="header">Channel and key.</param>
    /// <param name="lease">The lease.</param>
    /// <param name="length">Payload bytes at the start of the lease.</param>
    /// <param name="options">Mode, tracking, expiry.</param>
    /// <returns>The admission result; on a rejection the caller still owns the lease.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="length"/> is negative or exceeds the lease.</exception>
    public SendResult SendOwned(in SendHeader header, BufferLease lease, int length, SendOptions options = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(length, lease.Length);
        SendRequest request = default;
        request.Kind = SendPayloadKind.Owned;
        request.Lease = lease;
        request.Length = length;
        return Submit(in header, ref request, in options);
    }

    /// <summary>
    /// Sends pinned or native memory without copying or creating a handle; it must stay valid and unchanged until the
    /// BufferReleased completion of a tracked send (or until the peer is closed).
    /// </summary>
    /// <param name="header">Channel and key.</param>
    /// <param name="payload">The message.</param>
    /// <param name="length">Bytes.</param>
    /// <param name="options">Mode, tracking, expiry.</param>
    /// <returns>The admission result.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="length"/> is negative.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="payload"/> is null and <paramref name="length"/> is positive.</exception>
    public SendResult SendPinned(in SendHeader header, byte* payload, int length, SendOptions options = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        if (payload == null && length > 0)
        {
            throw new ArgumentNullException(nameof(payload));
        }

        SendRequest request = default;
        request.Kind = SendPayloadKind.Pinned;
        request.Pointer = payload;
        request.Length = length;
        return Submit(in header, ref request, in options);
    }

    /// <summary>
    /// Sends caller memory without copying; the peer pins it until the BufferReleased completion (convenience path, ADR
    /// 0008 invariant 11). The memory must not change until then.
    /// </summary>
    /// <param name="header">Channel and key.</param>
    /// <param name="payload">The message.</param>
    /// <param name="options">Mode, tracking, expiry.</param>
    /// <returns>The admission result.</returns>
    public SendResult SendBorrowed(in SendHeader header, ReadOnlyMemory<byte> payload, SendOptions options = default)
    {
        SendRequest request = default;
        request.Kind = SendPayloadKind.Borrowed;
        request.Borrowed = payload;
        request.Length = payload.Length;
        return Submit(in header, ref request, in options);
    }

    /// <summary>Sends existing payload pages as one message (at most <see cref="MaxGatherSegments"/>); ownership moves to the peer when admitted.</summary>
    /// <param name="header">Channel and key.</param>
    /// <param name="segments">The pages, in order.</param>
    /// <param name="options">Mode, tracking, expiry.</param>
    /// <returns>The admission result.</returns>
    /// <exception cref="ArgumentException">More than <see cref="MaxGatherSegments"/> pages.</exception>
    public SendResult SendGather(in SendHeader header, ReadOnlySpan<BufferLease> segments, SendOptions options = default)
    {
        if (segments.Length > MaxGatherSegments)
        {
            throw new ArgumentException($"A gather send has at most {MaxGatherSegments} segments.", nameof(segments));
        }

        SendRequest request = default;
        request.Kind = SendPayloadKind.Gather;
        request.Gather = segments;
        return Submit(in header, ref request, in options);
    }

    /// <summary>
    /// Sends a copy of <paramref name="payload"/>. In this wave admission is attempted once and the task completes with its
    /// result synchronously; waiting for queue space on reliable channels arrives with the ordered-stream engine.
    /// </summary>
    /// <param name="header">Channel and key.</param>
    /// <param name="payload">The message.</param>
    /// <param name="options">Mode, tracking, expiry.</param>
    /// <param name="cancellationToken">Cancels the wait for admission.</param>
    /// <returns>The admission result.</returns>
    public ValueTask<SendResult> SendAsync(in SendHeader header, ReadOnlyMemory<byte> payload, SendOptions options = default, CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return ValueTask.FromCanceled<SendResult>(cancellationToken);
        }

        return new ValueTask<SendResult>(SendCopy(in header, payload.Span, options));
    }

    /// <summary>Sends a request on a request/response channel and waits for the response (routed to the channel's engine).</summary>
    /// <param name="header">Channel and key.</param>
    /// <param name="payload">The request.</param>
    /// <param name="timeout">Response timeout.</param>
    /// <param name="cancellationToken">Cancels the wait.</param>
    /// <returns>The response payload (release it with <see cref="Release(in ReceiveLease)"/>).</returns>
    /// <exception cref="ArgumentException">The channel is not in the table.</exception>
    public ValueTask<ReceiveLease> SendRequestAsync(in SendHeader header, ReadOnlyMemory<byte> payload, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        int index = ChannelIndexOrThrow(header.Channel);
        if (_state != PeerState.Connected)
        {
            return ValueTask.FromException<ReceiveLease>(new InvalidOperationException("The peer is not connected."));
        }

        SendRequest request = default;
        request.Channel = _core.GetChannel(index);
        request.ChannelIndex = index;
        request.Key = header.Key;
        request.Kind = SendPayloadKind.Borrowed;
        request.Borrowed = payload;
        request.Length = payload.Length;
        return _core.GetEngine(index).SendRequestAsync(ref request, PeerOptions.ToMicros(timeout), cancellationToken);
    }

    /// <summary>Answers a request received on a request/response channel (routed to the channel's engine).</summary>
    /// <param name="request">The request's header.</param>
    /// <param name="payload">The response (copied).</param>
    /// <returns>The admission result.</returns>
    public SendResult Respond(in ReceiveHeader request, ReadOnlySpan<byte> payload)
    {
        ThrowIfDisposed();
        int index = _core.ChannelIndexOf(request.Channel);
        if (index < 0)
        {
            return SendResult.Rejected(SendStatus.InvalidChannel);
        }

        if (_state != PeerState.Connected)
        {
            return SendResult.Rejected(SendStatus.NotConnected);
        }

        SendRequest response = default;
        response.Channel = _core.GetChannel(index);
        response.ChannelIndex = index;
        response.Key = request.Key;
        response.Kind = SendPayloadKind.Copy;
        response.Source = payload;
        response.Length = payload.Length;
        SendStatus status = _core.GetEngine(index).Respond(in request, ref response);
        return new SendResult(status, status == SendStatus.Admitted ? response.Token : default);
    }

    /// <summary>Announces that <paramref name="key"/> will not be used again in this epoch (KeyRetired; routed to the channel's engine).</summary>
    /// <param name="channel">A keyed channel.</param>
    /// <param name="key">The key.</param>
    /// <returns>The outcome (<see cref="SendStatus.NotSupported"/> until the engine implements key retirement).</returns>
    public SendStatus RetireKey(ushort channel, ulong key)
    {
        ThrowIfDisposed();
        int index = _core.ChannelIndexOf(channel);
        if (index < 0)
        {
            return SendStatus.InvalidChannel;
        }

        return _state != PeerState.Connected ? SendStatus.NotConnected : _core.GetEngine(index).RetireKey(_core.GetChannel(index), key);
    }

    /// <summary>Starts sending a bulk object (routed to the Bulk channel's engine).</summary>
    /// <param name="descriptor">What to send.</param>
    /// <param name="source">Where the bytes come from.</param>
    /// <param name="cancellationToken">Cancels the transfer.</param>
    /// <returns>The transfer.</returns>
    /// <exception cref="ArgumentException">The channel is not in the table.</exception>
    public ValueTask<BulkTransfer> BeginBulkSendAsync(BulkDescriptor descriptor, IBulkSource source, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(source);
        int index = ChannelIndexOrThrow(descriptor.Channel);
        return _core.GetEngine(index).BeginBulkSendAsync(_core.GetChannel(index), in descriptor, source, cancellationToken);
    }

    /// <summary>Asks the peer to send a range of a bulk object (routed to the Bulk channel's engine).</summary>
    /// <param name="request">The range.</param>
    /// <exception cref="ArgumentException">The channel is not in the table.</exception>
    public void RequestBulk(in BulkRangeRequest request)
    {
        ThrowIfDisposed();
        int index = ChannelIndexOrThrow(request.Channel);
        _core.GetEngine(index).RequestBulk(_core.GetChannel(index), in request);
    }

    private SendResult Submit(in SendHeader header, ref SendRequest request, in SendOptions options)
    {
        ThrowIfDisposed();
        int index = _core.ChannelIndexOf(header.Channel);
        if (index < 0)
        {
            return SendResult.Rejected(SendStatus.InvalidChannel);
        }

        if (_state != PeerState.Connected)
        {
            return SendResult.Rejected(SendStatus.NotConnected);
        }

        request.Channel = _core.GetChannel(index);
        request.ChannelIndex = index;
        request.Key = header.Key;
        request.Options = options;
        SendStatus status = _core.GetEngine(index).Admit(ref request);
        return new SendResult(status, status == SendStatus.Admitted ? request.Token : default);
    }
}
