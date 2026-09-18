using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Primitives;
using Tedd.Quicly.Core.Session.Engines;
using Tedd.Quicly.Core.Threading;

namespace Tedd.Quicly.Core.Session;

// Game thread: the public send surface (ARCHITECTURE.md §4.1, §6). The peer resolves the channel and checks the session
// state; admission itself belongs to the engine of the channel's mode (ChannelEngine.Admit). An Immediate send runs one
// scheduler pass before it returns (QuiclyPeer.Flush.cs). SendAsync waits for admission on reliable channels (waiters are
// retried after completions are drained in Poll and Flush). With PeerOptions.ThreadSafeSend, sends from other threads go
// through a Vyukov multi-producer ring of 64-byte requests that the game thread admits at the start of Poll and Flush.
public sealed unsafe partial class QuiclyPeer
{
    /// <summary>Most payload pages of one <see cref="SendGather"/>.</summary>
    public const int MaxGatherSegments = 8;

    private bool _threadSafeSend;
    private MpscRing<ForeignSend>? _front;
    private ForeignSend _heldForeign;
    private bool _hasHeldForeign;
    private int _gameThreadId;
    private readonly List<SendWaiter> _sendWaiters = [];
    private readonly List<SendWaiter> _readyWaiters = [];
    private int[] _waitingPerChannel = [];
    private bool[] _retryBlocked = [];
    private bool _retryingWaiters;
    private bool _completingWaiters;

    /// <summary>
    /// Rents library memory to serialise a message into (zero-copy path: fill it, then <see cref="SendOwned"/>). Counts
    /// against the send budget until it is sent and completed, or given back with <see cref="ReturnBuffer"/>. Any thread when
    /// <see cref="PeerOptions.ThreadSafeSend"/> is on.
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
    /// <exception cref="ArgumentOutOfRangeException">The channel is keyed and the key exceeds 2^62 − 1.</exception>
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
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="length"/> is negative or exceeds the lease, or the channel is keyed and the key exceeds 2^62 − 1.</exception>
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
    /// BufferReleased completion of a tracked send (or until the peer is closed). From another thread
    /// (<see cref="PeerOptions.ThreadSafeSend"/>) the bytes are copied at the call.
    /// </summary>
    /// <param name="header">Channel and key.</param>
    /// <param name="payload">The message.</param>
    /// <param name="length">Bytes.</param>
    /// <param name="options">Mode, tracking, expiry.</param>
    /// <returns>The admission result.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="length"/> is negative, or the channel is keyed and the key exceeds 2^62 − 1.</exception>
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
    /// <remarks>
    /// Array-backed memory is pinned with a handle kept in the entry's pin side table and freed with the payload. Memory
    /// that is not backed by an array (a <see cref="System.Buffers.MemoryManager{T}"/>) is copied at admission instead, and
    /// so is every borrowed send made from another thread (<see cref="PeerOptions.ThreadSafeSend"/>).
    /// </remarks>
    /// <param name="header">Channel and key.</param>
    /// <param name="payload">The message.</param>
    /// <param name="options">Mode, tracking, expiry.</param>
    /// <returns>The admission result.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The channel is keyed and the key exceeds 2^62 − 1.</exception>
    public SendResult SendBorrowed(in SendHeader header, ReadOnlyMemory<byte> payload, SendOptions options = default)
    {
        SendRequest request = default;
        request.Kind = SendPayloadKind.Borrowed;
        request.Borrowed = payload;
        request.Length = payload.Length;
        return Submit(in header, ref request, in options);
    }

    /// <summary>Sends existing payload pages as one message (at most <see cref="MaxGatherSegments"/>); ownership moves to the peer when admitted.</summary>
    /// <remarks>
    /// Each page contributes its whole <see cref="BufferLease.Length"/>. On datagram channels the pages are copied into one
    /// payload buffer at admission (a datagram message fits in one datagram) and given back to the pool. On ordered channels
    /// a single page that is not compressed is sent as it is (zero copy); several pages are copied into one buffer.
    /// </remarks>
    /// <param name="header">Channel and key.</param>
    /// <param name="segments">The pages, in order.</param>
    /// <param name="options">Mode, tracking, expiry.</param>
    /// <returns>The admission result.</returns>
    /// <exception cref="ArgumentException">More than <see cref="MaxGatherSegments"/> pages.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The channel is keyed and the key exceeds 2^62 − 1.</exception>
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
    /// Sends a copy of <paramref name="payload"/>, waiting for admission while a reliable channel cannot take it yet: its
    /// queue limit, the send table or the send budget is full (<see cref="SendStatus.QueueFull"/>,
    /// <see cref="SendStatus.OutOfBuffers"/>). A waiting send is retried in call order per channel after completions were
    /// drained in <see cref="Poll"/> or <see cref="Flush"/>, and the task completes there with the admission result
    /// (<see cref="SendStatus.NotConnected"/> when the session ends first). Unreliable channels and every other refusal answer
    /// at once. The synchronous path allocates nothing. The payload is copied at admission, so it must stay unchanged until
    /// the task completes. While sends wait on a channel, synchronous sends on it answer <see cref="SendStatus.QueueFull"/> so
    /// that none overtakes them. From another thread (<see cref="PeerOptions.ThreadSafeSend"/>) the send is queued for the
    /// game thread; while that queue is full the call waits with short delays.
    /// </summary>
    /// <param name="header">Channel and key.</param>
    /// <param name="payload">The message.</param>
    /// <param name="options">Mode, tracking, expiry.</param>
    /// <param name="cancellationToken">Cancels the wait for admission (a canceled send is never admitted).</param>
    /// <returns>The admission result.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The channel is keyed and the key exceeds 2^62 − 1.</exception>
    /// <exception cref="ObjectDisposedException">The peer is disposed (also completes a waiting send).</exception>
    public ValueTask<SendResult> SendAsync(in SendHeader header, ReadOnlyMemory<byte> payload, SendOptions options = default, CancellationToken cancellationToken = default)
    {
        if (cancellationToken.IsCancellationRequested)
        {
            return ValueTask.FromCanceled<SendResult>(cancellationToken);
        }

        ThrowIfDisposed();
        if (IsForeignThread)
        {
            return SendAsyncForeign(header, payload, options, cancellationToken);
        }

        SendResult result = SendCopy(in header, payload.Span, options);
        if (!ShouldWait(header.Channel, result.Status))
        {
            return new ValueTask<SendResult>(result);
        }

        int index = _core.ChannelIndexOf(header.Channel);
        SendWaiter waiter = new(header, payload, options, index, _core.CompletionMode == CompletionMode.ThreadPool);
        if (cancellationToken.CanBeCanceled)
        {
            waiter.Registration = cancellationToken.UnsafeRegister(static (state, token) => ((SendWaiter)state!).Source.TrySetCanceled(token), waiter);
        }

        _sendWaiters.Add(waiter);
        _waitingPerChannel[index]++;
        return new ValueTask<SendResult>(waiter.Source.Task);
    }

    /// <summary>
    /// Sends a request on a <see cref="Channels.ChannelDefinition.RequestResponse"/> channel and waits for the peer's
    /// response (PROTOCOL.md §3.1, §4.3: the only application-level acknowledgement in v1). The frame carries an odd
    /// <c>RequestId</c> from the channel's counter and the peer answers with <c>RequestId + 1</c>
    /// (<see cref="Respond"/>); the response is matched on the game thread inside <see cref="Poll"/> — or inside
    /// <see cref="Flush"/>, which is where timeouts are processed as well — and never reaches the channel's
    /// <see cref="MessageHandler"/>.
    /// </summary>
    /// <remarks>
    /// The request itself is an ordinary buffered message of the channel, so a <see cref="Flush"/> transmits it; only the
    /// wait is asynchronous. The value task is backed by a pooled source, so the library allocates nothing per request in
    /// steady state (the caller's own <c>await</c> state machine is the caller's cost). The payload is pinned or copied like
    /// <see cref="SendBorrowed"/>, so it must stay unchanged until the request has been handed to the transport.
    /// An empty response yields a lease with an empty payload; releasing it is a no-op.
    /// </remarks>
    /// <param name="header">Channel and key.</param>
    /// <param name="payload">The request.</param>
    /// <param name="timeout">
    /// How long to wait for the response; <see cref="TimeSpan.Zero"/> waits until the response arrives, the wait is canceled
    /// or the session ends.
    /// </param>
    /// <param name="cancellationToken">
    /// Cancels the <em>wait</em>, never the send (ADR 0004): the request still goes out and its response is dropped and
    /// counted when it arrives.
    /// </param>
    /// <returns>
    /// The response payload (release it with <see cref="Release(in ReceiveLease)"/>). Faults with
    /// <see cref="TimeoutException"/> when the timeout elapses first, with <see cref="OperationCanceledException"/> when the
    /// wait is canceled, with <see cref="InvalidOperationException"/> when the request is not admitted or the session ends
    /// (close or reconnect) and with <see cref="ObjectDisposedException"/> when the peer is disposed.
    /// </returns>
    /// <exception cref="ArgumentException">The channel is not in the table.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout"/> is negative, or the channel is keyed and the key exceeds 2^62 − 1.</exception>
    public ValueTask<ReceiveLease> SendRequestAsync(in SendHeader header, ReadOnlyMemory<byte> payload, TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        ThrowIfDisposed();
        ArgumentOutOfRangeException.ThrowIfLessThan(timeout, TimeSpan.Zero);
        int index = ChannelIndexOrThrow(header.Channel);
        if (_core.GetChannel(index).Keyed && header.Key > VarInt.MaxValue)
        {
            ThrowKeyOutOfRange(header.Key);
        }

        if (cancellationToken.IsCancellationRequested)
        {
            return ValueTask.FromCanceled<ReceiveLease>(cancellationToken);
        }

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

    /// <summary>Sets up the send side (constructor): SendAsync bookkeeping and, with <see cref="PeerOptions.ThreadSafeSend"/>, the multi-producer front.</summary>
    private void InitializeSendSide(PeerOptions options)
    {
        _waitingPerChannel = new int[_core.ChannelCount];
        _retryBlocked = new bool[_core.ChannelCount];
        _threadSafeSend = options.ThreadSafeSend;
        _gameThreadId = Environment.CurrentManagedThreadId;
        if (_threadSafeSend)
        {
            _front = new MpscRing<ForeignSend>(options.SendTableCapacity);
        }
    }

    /// <summary>
    /// True when <see cref="PeerOptions.ThreadSafeSend"/> is on and the caller is not the game thread (the thread that last
    /// entered <see cref="Poll"/> or <see cref="Flush"/>, or created the peer before that).
    /// </summary>
    private bool IsForeignThread => _threadSafeSend && Environment.CurrentManagedThreadId != Volatile.Read(ref _gameThreadId);

    /// <summary>Records the game thread (entry of Poll and Flush) when sends may come from other threads.</summary>
    private void NoteGameThread()
    {
        if (_threadSafeSend)
        {
            Volatile.Write(ref _gameThreadId, Environment.CurrentManagedThreadId);
        }
    }

    private SendResult Submit(in SendHeader header, ref SendRequest request, in SendOptions options)
    {
        ThrowIfDisposed();
        if (_threadSafeSend && Environment.CurrentManagedThreadId != Volatile.Read(ref _gameThreadId))
        {
            return SubmitForeign(in header, ref request, in options);
        }

        int index = _core.ChannelIndexOf(header.Channel);
        if (index < 0)
        {
            return SendResult.Rejected(SendStatus.InvalidChannel);
        }

        if (_state != PeerState.Connected)
        {
            return SendResult.Rejected(SendStatus.NotConnected);
        }

        ChannelDefinition channel = _core.GetChannel(index);
        if (channel.Keyed && header.Key > VarInt.MaxValue)
        {
            ThrowKeyOutOfRange(header.Key);
        }

        if (_waitingPerChannel[index] > 0 && !_retryingWaiters)
        {
            // SendAsync calls wait for room on this channel: nothing overtakes them.
            _core.SendCounters(index).QueueFull++;
            return SendResult.Rejected(SendStatus.QueueFull);
        }

        request.Channel = channel;
        request.ChannelIndex = index;
        request.Key = header.Key;
        request.Options = options;
        SendStatus status = _core.GetEngine(index).Admit(ref request);
        if (status != SendStatus.Admitted)
        {
            return SendResult.Rejected(status);
        }

        SendToken token = request.Token;
        if (options.Mode == SendMode.Immediate)
        {
            FlushImmediate();
        }

        return new SendResult(SendStatus.Admitted, token);
    }

    // ------------------------------------------------------------------ SendAsync waiters (game thread)

    /// <summary>Whether a refused SendAsync waits: a transient refusal on a reliable channel of a connected session.</summary>
    private bool ShouldWait(ushort channel, SendStatus status)
    {
        if (status is not (SendStatus.QueueFull or SendStatus.OutOfBuffers) || _state != PeerState.Connected)
        {
            return false;
        }

        int index = _core.ChannelIndexOf(channel);
        return index >= 0 && _core.GetChannel(index).Mode is not (ChannelMode.UnreliableUnordered or ChannelMode.UnreliableSequenced);
    }

    /// <summary>
    /// Retries the waiting SendAsync calls in call order, stopping each channel at its first send that still does not fit
    /// (after completions were drained in Poll and Flush). The admitted ones complete after the list is settled, so a
    /// continuation that runs inline (<see cref="CompletionMode.PollOnly"/>) may send again.
    /// </summary>
    private void RetrySendWaiters()
    {
        List<SendWaiter> waiters = _sendWaiters;
        if (waiters.Count == 0 || _retryingWaiters)
        {
            return;
        }

        _retryingWaiters = true;
        try
        {
            Array.Clear(_retryBlocked);
            int kept = 0;
            for (int i = 0; i < waiters.Count; i++)
            {
                SendWaiter waiter = waiters[i];
                if (waiter.Source.Task.IsCompleted)
                {
                    // Canceled while waiting.
                    _waitingPerChannel[waiter.ChannelIndex]--;
                    waiter.Registration.Dispose();
                    continue;
                }

                if (!_retryBlocked[waiter.ChannelIndex])
                {
                    SendResult result = SendCopy(in waiter.Header, waiter.Payload.Span, waiter.Options);
                    if (!ShouldWait(waiter.Header.Channel, result.Status))
                    {
                        waiter.Result = result;
                        _waitingPerChannel[waiter.ChannelIndex]--;
                        _readyWaiters.Add(waiter);
                        continue;
                    }

                    _retryBlocked[waiter.ChannelIndex] = true;
                }

                waiters[kept++] = waiter;
            }

            waiters.RemoveRange(kept, waiters.Count - kept);
        }
        finally
        {
            _retryingWaiters = false;
        }

        CompleteReadyWaiters();
    }

    private void CompleteReadyWaiters()
    {
        if (_completingWaiters)
        {
            return;
        }

        _completingWaiters = true;
        try
        {
            List<SendWaiter> ready = _readyWaiters;
            for (int i = 0; i < ready.Count; i++)
            {
                SendWaiter waiter = ready[i];
                waiter.Registration.Dispose();
                waiter.Source.TrySetResult(waiter.Result);
            }

            ready.Clear();
        }
        finally
        {
            _completingWaiters = false;
        }
    }

    /// <summary>
    /// Completes every waiting SendAsync: with <see cref="SendStatus.NotConnected"/> when the session closed
    /// (<paramref name="exception"/> null) or with the exception (the peer was disposed).
    /// </summary>
    private void CompleteSendWaiters(Exception? exception)
    {
        List<SendWaiter> waiters = _sendWaiters;
        if (waiters.Count == 0)
        {
            return;
        }

        for (int i = 0; i < waiters.Count; i++)
        {
            SendWaiter waiter = waiters[i];
            waiter.Result = SendResult.Rejected(SendStatus.NotConnected);
            _readyWaiters.Add(waiter);
        }

        waiters.Clear();
        Array.Clear(_waitingPerChannel);
        if (exception is null)
        {
            CompleteReadyWaiters();
            return;
        }

        List<SendWaiter> ready = _readyWaiters;
        for (int i = 0; i < ready.Count; i++)
        {
            ready[i].Registration.Dispose();
            ready[i].Source.TrySetException(exception);
        }

        ready.Clear();
    }

    /// <summary>The waiters of <see cref="SendAsync"/> and <see cref="FlushAsync"/> fail with <see cref="ObjectDisposedException"/> (Dispose).</summary>
    private void FailWaitersOnDispose()
    {
        // And the requests still waiting for a response (QuiclyPeer.Poll.cs, docs/design/session-layer.md §7.8).
        FailRequestsOnDispose();
        if (_sendWaiters.Count == 0 && _flushWaiters.Count == 0)
        {
            return;
        }

        ObjectDisposedException disposed = new(nameof(QuiclyPeer));
        CompleteSendWaiters(disposed);
        FailFlushWaiters(disposed);
    }

    // ------------------------------------------------------------------ sends from other threads (ThreadSafeSend)

    /// <summary>
    /// A send from another thread (<see cref="PeerOptions.ThreadSafeSend"/>): the payload is copied into a send lease (an owned
    /// lease moves as it is) and a 64-byte request is queued for the game thread, which admits it at the start of its next
    /// Poll or Flush. Answers <see cref="SendStatus.Admitted"/> once queued (without a token: tracking needs the game thread,
    /// so a tracked send answers <see cref="SendStatus.NotSupported"/>), <see cref="SendStatus.QueueFull"/> when the front is
    /// full; a request the game thread then refuses is dropped and counted (<see cref="PeerStatistics.ThreadSafeSendDrops"/>).
    /// </summary>
    private SendResult SubmitForeign(in SendHeader header, ref SendRequest request, in SendOptions options)
    {
        int index = _core.ChannelIndexOf(header.Channel);
        if (index < 0)
        {
            return SendResult.Rejected(SendStatus.InvalidChannel);
        }

        if (_state != PeerState.Connected)
        {
            return SendResult.Rejected(SendStatus.NotConnected);
        }

        ChannelDefinition channel = _core.GetChannel(index);
        if (channel.Keyed && header.Key > VarInt.MaxValue)
        {
            ThrowKeyOutOfRange(header.Key);
        }

        if (options.Track)
        {
            return SendResult.Rejected(SendStatus.NotSupported);
        }

        int length = request.Kind == SendPayloadKind.Gather ? EnginePayload.GatherLength(request.Gather) : request.Length;
        if (length > _core.EffectiveMaxMessageSize(channel))
        {
            return SendResult.Rejected(SendStatus.TooLarge);
        }

        BufferLease lease = BufferLease.Empty;
        bool rented = false;
        if (request.Kind == SendPayloadKind.Owned)
        {
            lease = request.Lease;
        }
        else if (length > 0)
        {
            if (!_core.TryRentSend(length, out lease))
            {
                return SendResult.Rejected(SendStatus.OutOfBuffers);
            }

            rented = true;
            Span<byte> target = _core.GetSpan(in lease);
            switch (request.Kind)
            {
                case SendPayloadKind.Copy:
                    request.Source.CopyTo(target);
                    break;
                case SendPayloadKind.Pinned:
                    new ReadOnlySpan<byte>(request.Pointer, length).CopyTo(target);
                    break;
                case SendPayloadKind.Borrowed:
                    request.Borrowed.Span.CopyTo(target);
                    break;
                case SendPayloadKind.Shared:
                {
                    // Copied at the call, like every other foreign-thread path: the caller's reference is untouched, so
                    // a fan-out never depends on when the game thread gets round to admitting the request.
                    BufferLease block = request.Shared.Lease;
                    new ReadOnlySpan<byte>(_core.GetPointer(in block), length).CopyTo(target);
                    break;
                }
                default:
                {
                    int offset = 0;
                    foreach (BufferLease page in request.Gather)
                    {
                        if (!page.IsEmpty)
                        {
                            _core.GetSpan(in page).Slice(0, page.Length).CopyTo(target.Slice(offset));
                            offset += page.Length;
                        }
                    }

                    break;
                }
            }
        }

        ForeignSend item = default;
        item.Lease = lease;
        item.Length = length;
        item.Key = channel.Keyed ? header.Key : 0;
        item.Context = options.Context;
        item.ExpiryMicros = options.ExpiryMicros;
        item.Channel = header.Channel;
        item.Mode = options.Mode;
        if (!_front!.TryEnqueue(in item))
        {
            if (rented)
            {
                _core.ReturnSend(in lease);
            }

            return SendResult.Rejected(SendStatus.QueueFull);
        }

        if (request.Kind == SendPayloadKind.Gather)
        {
            foreach (BufferLease page in request.Gather)
            {
                _core.ReturnSend(in page);
            }
        }

        Interlocked.Increment(ref _core.Counters.ThreadSafeSends);
        // The game thread has to admit it: that is work, even though nothing arrived from the network.
        NoteWork();
        return new SendResult(SendStatus.Admitted, default);
    }

    private ValueTask<SendResult> SendAsyncForeign(SendHeader header, ReadOnlyMemory<byte> payload, SendOptions options, CancellationToken cancellationToken)
    {
        SendResult result = SendCopy(in header, payload.Span, options);
        return result.Status != SendStatus.QueueFull || _state != PeerState.Connected
            ? new ValueTask<SendResult>(result)
            : ForeignSendRetry.WhenRoomAsync(this, header, payload, options, cancellationToken);
    }

    /// <summary>
    /// Admits the requests queued by other threads, oldest first (start of Poll and Flush). A request that does not fit yet
    /// (<see cref="SendStatus.QueueFull"/>, <see cref="SendStatus.OutOfBuffers"/>) stays first and is retried at the next
    /// call; one refused for good is dropped (its lease goes back to the pool) and counted.
    /// </summary>
    /// <returns><see langword="true"/> when an admitted request was an <see cref="SendMode.Immediate"/> send.</returns>
    private bool DrainForeignSends()
    {
        MpscRing<ForeignSend>? front = _front;
        if (front is null || (!_hasHeldForeign && front.IsEmpty))
        {
            return false;
        }

        bool immediate = false;
        while (true)
        {
            ForeignSend item;
            if (_hasHeldForeign)
            {
                item = _heldForeign;
                _hasHeldForeign = false;
            }
            else if (!front.TryDequeue(out item))
            {
                break;
            }

            SendStatus status = AdmitForeign(in item);
            if (status == SendStatus.Admitted)
            {
                immediate |= item.Mode == SendMode.Immediate;
                continue;
            }

            if (status is SendStatus.QueueFull or SendStatus.OutOfBuffers && _state == PeerState.Connected)
            {
                _heldForeign = item;
                _hasHeldForeign = true;
                break;
            }

            _core.ReturnSend(in item.Lease);
            Interlocked.Increment(ref _core.Counters.ThreadSafeSendDrops);
        }

        return immediate;
    }

    private SendStatus AdmitForeign(in ForeignSend item)
    {
        if (_state != PeerState.Connected)
        {
            return SendStatus.NotConnected;
        }

        int index = _core.ChannelIndexOf(item.Channel);
        if (_waitingPerChannel[index] > 0)
        {
            return SendStatus.QueueFull;
        }

        SendRequest request = default;
        request.Channel = _core.GetChannel(index);
        request.ChannelIndex = index;
        request.Key = item.Key;
        request.Options = new SendOptions { Mode = item.Mode, Context = item.Context, ExpiryMicros = item.ExpiryMicros };
        if (item.Lease.IsEmpty)
        {
            request.Kind = SendPayloadKind.Copy;
        }
        else
        {
            request.Kind = SendPayloadKind.Owned;
            request.Lease = item.Lease;
            request.Length = item.Length;
        }

        return _core.GetEngine(index).Admit(ref request);
    }

    /// <summary>Returns the leases of every request still queued by other threads (the session closed or the peer is freed).</summary>
    private void ReleaseForeignSends()
    {
        MpscRing<ForeignSend>? front = _front;
        if (front is null)
        {
            return;
        }

        if (_hasHeldForeign)
        {
            _hasHeldForeign = false;
            _core.ReturnSend(in _heldForeign.Lease);
            Interlocked.Increment(ref _core.Counters.ThreadSafeSendDrops);
        }

        while (front.TryDequeue(out ForeignSend item))
        {
            _core.ReturnSend(in item.Lease);
            Interlocked.Increment(ref _core.Counters.ThreadSafeSendDrops);
        }
    }

    [DoesNotReturn]
    private static void ThrowKeyOutOfRange(ulong key) =>
        throw new ArgumentOutOfRangeException("header", key, "A message key is at most 2^62 - 1 (PROTOCOL.md §2.1).");

    /// <summary>One send from another thread (56 bytes, so a ring slot with its sequence word is one 64-byte cache line).</summary>
    [StructLayout(LayoutKind.Sequential, Size = 56)]
    private struct ForeignSend
    {
        public BufferLease Lease;
        public ulong Key;
        public ulong Context;
        public long ExpiryMicros;
        public int Length;
        public ushort Channel;
        public SendMode Mode;
    }

    /// <summary>A SendAsync waiting for admission (game thread; allocated only when a send must wait).</summary>
    private sealed class SendWaiter(SendHeader header, ReadOnlyMemory<byte> payload, SendOptions options, int channelIndex, bool asynchronous)
    {
        public readonly SendHeader Header = header;
        public readonly ReadOnlyMemory<byte> Payload = payload;
        public readonly SendOptions Options = options;
        public readonly int ChannelIndex = channelIndex;
        public readonly TaskCompletionSource<SendResult> Source = new(asynchronous ? TaskCreationOptions.RunContinuationsAsynchronously : TaskCreationOptions.None);
        public CancellationTokenRegistration Registration;
        public SendResult Result;
    }
}

/// <summary>
/// The waiting loop of a <see cref="QuiclyPeer.SendAsync"/> made from another thread while the ThreadSafeSend front is full
/// (kept outside the peer's unsafe context, where <c>await</c> is not allowed). Allocates: it only runs when the front is full.
/// </summary>
internal static class ForeignSendRetry
{
    /// <summary>Retries the send every millisecond until the front takes it or refuses it for another reason.</summary>
    /// <param name="peer">The peer.</param>
    /// <param name="header">Channel and key.</param>
    /// <param name="payload">The message.</param>
    /// <param name="options">Mode, tracking, expiry.</param>
    /// <param name="cancellationToken">Cancels the wait.</param>
    /// <returns>The result of the last attempt.</returns>
    public static async ValueTask<SendResult> WhenRoomAsync(QuiclyPeer peer, SendHeader header, ReadOnlyMemory<byte> payload, SendOptions options, CancellationToken cancellationToken)
    {
        while (true)
        {
            // The front is full until the game thread's next Poll or Flush drains it.
            await Task.Delay(1, cancellationToken).ConfigureAwait(false);
            SendResult result = peer.SendCopy(in header, payload.Span, options);
            if (result.Status != SendStatus.QueueFull || peer.State != PeerState.Connected)
            {
                return result;
            }
        }
    }
}
