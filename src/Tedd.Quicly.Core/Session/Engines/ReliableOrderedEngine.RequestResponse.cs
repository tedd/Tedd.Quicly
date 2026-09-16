using System.Threading.Tasks.Sources;
using Tedd.Quicly.Core.Channels;

namespace Tedd.Quicly.Core.Session.Engines;

// Correlated request/response on ReliableOrdered channels with ChannelDefinition.RequestResponse (PROTOCOL.md §3.1, §4.3;
// docs/design/session-layer.md §7.8). Game thread only: the request table is send-side state, and a response is matched
// where it is dispatched — inside Poll (or Drain) — never on the transport thread.
internal sealed unsafe partial class ReliableOrderedEngine
{
    /// <summary>Requests one peer may have outstanding at once (ARCHITECTURE.md §9); a further one is refused.</summary>
    internal const int MaxOutstandingRequests = 256;

    private RequestSlot?[] _requests = [];
    private uint[] _nextRequestId = [];
    private int _requestFreeHead = -1;
    private int _requestCreated;
    private int _requestCount;
    private long _requestDeadline = long.MaxValue;
    private bool _hasRequestResponse;

    /// <summary>Requests waiting for a response right now (game thread; tests and statistics).</summary>
    internal int OutstandingRequests => _requestCount;

    /// <summary>Clock micros of the earliest request timeout, or <see cref="long.MaxValue"/> (game thread; tests).</summary>
    internal long RequestDeadlineMicros => _requestDeadline;

    /// <inheritdoc/>
    /// <remarks>
    /// The request is an ordinary buffered message of the channel with an odd <c>RequestId</c> from the channel's counter
    /// (PROTOCOL.md §3.1); only the wait is asynchronous. The value task is backed by a pooled
    /// <see cref="IValueTaskSource{TResult}"/> that is reused for the life of the peer, so a request allocates nothing once
    /// the table's slots exist. The timeout is processed by <see cref="RunPollDeadlines"/> in every Poll and Flush; there is
    /// no timer thread (ADR 0008 invariant 9).
    /// </remarks>
    public override ValueTask<ReceiveLease> SendRequestAsync(ref SendRequest request, long timeoutMicros, CancellationToken cancellationToken)
    {
        ChannelDefinition channel = request.Channel;
        if (!channel.RequestResponse)
        {
            return ValueTask.FromException<ReceiveLease>(new NotSupportedException(
                $"Channel {channel.Id} ('{channel.Name}') does not carry request ids; set ChannelOptions.RequestResponse on it (PROTOCOL.md §3.1)."));
        }

        int local = _localOf[request.ChannelIndex];
        if (!TryTakeRequestSlot(out int index))
        {
            return ValueTask.FromException<ReceiveLease>(new InvalidOperationException(
                $"{MaxOutstandingRequests} requests are already waiting for a response on this peer."));
        }

        RequestSlot slot = _requests[index]!;
        request.RequestId = NextRequestId(local);
        SendStatus status = Admit(ref request);
        if (status != SendStatus.Admitted)
        {
            ReturnRequestSlot(index, slot);
            return ValueTask.FromException<ReceiveLease>(new InvalidOperationException($"The request was not admitted ({status})."));
        }

        slot.Local = local;
        slot.RequestId = request.RequestId;
        // The pass's clock stamp, not a QPC per request (ADR 0008 invariant 9); 0 = wait until the response or the session ends.
        slot.DeadlineMicros = timeoutMicros > 0 ? _core.CurrentPassMicros + timeoutMicros : 0;
        slot.InUse = true;
        _requestCount++;
        if (slot.DeadlineMicros != 0 && slot.DeadlineMicros < _requestDeadline)
        {
            _requestDeadline = slot.DeadlineMicros;
        }

        if (cancellationToken.CanBeCanceled)
        {
            // Cancels the wait, never the send (ADR 0004): the request keeps flowing and its response is dropped and counted.
            slot.Registration = cancellationToken.UnsafeRegister(static (state, token) => ((RequestSlot)state!).Cancel(token), slot);
        }

        _core.Counters.RequestsSent++;
        return new ValueTask<ReceiveLease>(slot, slot.Version);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// PROTOCOL.md §3.1: the response carries <c>RequestId + 1</c> (even), so the peer can correlate it. A header that is not
    /// a request — <c>RequestId</c> 0, an even id, or the one odd id whose response would not fit 32 bits — answers
    /// <see cref="SendStatus.NotSupported"/>, and so does a channel without <see cref="ChannelDefinition.RequestResponse"/>.
    /// Otherwise the response is admitted exactly like any other message of the channel.
    /// </remarks>
    public override SendStatus Respond(in ReceiveHeader requestHeader, ref SendRequest response)
    {
        uint id = requestHeader.RequestId;
        if (!response.Channel.RequestResponse || id == 0 || (id & 1) == 0 || id == uint.MaxValue)
        {
            return SendStatus.NotSupported;
        }

        response.RequestId = id + 1;
        return Admit(ref response);
    }

    /// <inheritdoc/>
    /// <remarks>
    /// The response of an outstanding request completes its <see cref="SendRequestAsync"/> with the payload, and the engine
    /// takes the lease: the application releases it with <see cref="QuiclyPeer.Release(in ReceiveLease)"/>. Anything else —
    /// a response to a request that timed out, was canceled or never existed — is left to the peer, which drops it and counts
    /// <see cref="PeerStatistics.ResponsesUnmatched"/> (PROTOCOL.md §3.1). At most
    /// <see cref="MaxOutstandingRequests"/> slots are scanned, and only for messages that really are responses.
    /// </remarks>
    public override bool TryTakeResponse(in ReceiveLease response)
    {
        if (!_hasRequestResponse || _requestCount == 0)
        {
            return false;
        }

        uint id = response.Header.RequestId;
        if (id == 0 || (id & 1) != 0)
        {
            return false;
        }

        int dense = _core.ChannelIndexOf(response.Header.Channel);
        int local = dense >= 0 ? _localOf[dense] : -1;
        if (local < 0)
        {
            return false;
        }

        uint requestId = id - 1;
        RequestSlot?[] slots = _requests;
        for (int i = 0; i < slots.Length; i++)
        {
            RequestSlot? slot = slots[i];
            if (slot is null || !slot.InUse || slot.Local != local || slot.RequestId != requestId)
            {
                continue;
            }

            RetireRequest(slot);
            slot.SetResult(in response);
            return true;
        }

        return false;
    }

    /// <inheritdoc/>
    /// <remarks>
    /// The request timeouts of PROTOCOL.md §4.3, processed in every Poll and Flush. The published deadline is always in the
    /// future: a deadline at or before <paramref name="nowMicros"/> would make a host that sleeps on it spin.
    /// </remarks>
    public override void RunPollDeadlines(long nowMicros, ref long nextDeadline)
    {
        if (_requestCount == 0)
        {
            return;
        }

        if (nowMicros >= _requestDeadline)
        {
            ExpireRequests(nowMicros);
        }

        long deadline = _requestDeadline;
        if (deadline > nowMicros && deadline < nextDeadline)
        {
            nextDeadline = deadline;
        }
    }

    /// <summary>Sets up the request table when a channel of this engine carries request ids (constructor time, game thread).</summary>
    /// <param name="channelsOfMode">The channels of this engine.</param>
    private void InitializeRequestResponse(ReadOnlySpan<ChannelDefinition> channelsOfMode)
    {
        _nextRequestId = new uint[channelsOfMode.Length];
        for (int local = 0; local < channelsOfMode.Length; local++)
        {
            // PROTOCOL.md §3.1: 0 is a plain message, odd ids are requests.
            _nextRequestId[local] = 1;
            _hasRequestResponse |= channelsOfMode[local].RequestResponse;
        }

        if (!_hasRequestResponse)
        {
            return;
        }

        // The slots themselves are created on first use, so a peer that never sends a request costs one array.
        _requests = new RequestSlot?[MaxOutstandingRequests];
    }

    /// <summary>The channel's next request id: odd, never 0 and never the one id whose response would not fit 32 bits.</summary>
    private uint NextRequestId(int local)
    {
        uint id = _nextRequestId[local];
        _nextRequestId[local] = id >= uint.MaxValue - 2 ? 1 : id + 2;
        return id;
    }

    /// <summary>
    /// Takes a free request slot, creating its pooled source on first use (game thread). Slots return to the free list when
    /// their value task has been consumed, which may be a thread-pool thread in
    /// <see cref="CompletionMode.ThreadPool"/> mode, so the list is a lock-free stack.
    /// </summary>
    private bool TryTakeRequestSlot(out int index)
    {
        while (true)
        {
            int head = Volatile.Read(ref _requestFreeHead);
            if (head < 0)
            {
                break;
            }

            RequestSlot slot = _requests[head]!;
            if (Interlocked.CompareExchange(ref _requestFreeHead, slot.Next, head) == head)
            {
                index = head;
                return true;
            }
        }

        // No slot has been given back: create the next one until the table's cap is reached (game thread only).
        if (_requestCreated < MaxOutstandingRequests)
        {
            index = _requestCreated++;
            _requests[index] = new RequestSlot(this, index, _core.CompletionMode == CompletionMode.ThreadPool);
            return true;
        }

        index = -1;
        return false;
    }

    /// <summary>Gives a slot back without ever having armed it (an admission that failed).</summary>
    private void ReturnRequestSlot(int index, RequestSlot slot)
    {
        slot.Reset();
        PushRequestSlot(index, slot);
    }

    /// <summary>Pushes a consumed slot onto the free list (any thread: the awaiter's thread consumed its value task).</summary>
    private void PushRequestSlot(int index, RequestSlot slot)
    {
        while (true)
        {
            int head = Volatile.Read(ref _requestFreeHead);
            slot.Next = head;
            if (Interlocked.CompareExchange(ref _requestFreeHead, index, head) == head)
            {
                return;
            }
        }
    }

    /// <summary>Takes a request out of the live set before it is completed (game thread); the slot returns when consumed.</summary>
    private void RetireRequest(RequestSlot slot)
    {
        slot.InUse = false;
        slot.DeadlineMicros = 0;
        _requestCount--;
    }

    /// <summary>Fails every request whose timeout has passed and recomputes the earliest deadline (game thread).</summary>
    private void ExpireRequests(long nowMicros)
    {
        long earliest = long.MaxValue;
        RequestSlot?[] slots = _requests;
        for (int i = 0; i < slots.Length; i++)
        {
            RequestSlot? slot = slots[i];
            if (slot is null || !slot.InUse || slot.DeadlineMicros == 0)
            {
                continue;
            }

            if (nowMicros < slot.DeadlineMicros)
            {
                if (slot.DeadlineMicros < earliest)
                {
                    earliest = slot.DeadlineMicros;
                }

                continue;
            }

            RetireRequest(slot);
            _core.Counters.RequestsTimedOut++;
            slot.SetException(new TimeoutException("No response arrived within the request's timeout."));
        }

        _requestDeadline = earliest;
    }

    /// <summary>
    /// Fails every outstanding request (game thread): the session closed, the connection was lost before a resume, or the
    /// peer was disposed. A response that still arrives afterwards is dropped and counted like any unmatched one.
    /// </summary>
    /// <param name="reason">What the awaiters see.</param>
    private void FailRequests(Exception reason)
    {
        if (!_hasRequestResponse || _requestCount == 0)
        {
            return;
        }

        RequestSlot?[] slots = _requests;
        for (int i = 0; i < slots.Length; i++)
        {
            RequestSlot? slot = slots[i];
            if (slot is null || !slot.InUse)
            {
                continue;
            }

            RetireRequest(slot);
            slot.SetException(reason);
        }

        _requestDeadline = long.MaxValue;
    }

    /// <summary>
    /// One outstanding request: a pooled <see cref="IValueTaskSource{TResult}"/> plus its bookkeeping, reused for the life of
    /// the peer (the version of its source invalidates the value task of every earlier occupant). The slot leaves the live set
    /// when it is completed and returns to the free list when its value task has been consumed, so a value task that is never
    /// awaited costs its slot — the contract of <see cref="Threading.CompletionTable"/> applies here too.
    /// </summary>
    private sealed class RequestSlot(ReliableOrderedEngine engine, int index, bool runContinuationsAsynchronously) : IValueTaskSource<ReceiveLease>
    {
        private ManualResetValueTaskSourceCore<ReceiveLease> _source = new() { RunContinuationsAsynchronously = runContinuationsAsynchronously };

        /// <summary>Engine-local index of the request's channel.</summary>
        public int Local;

        /// <summary>The request's odd id (PROTOCOL.md §3.1).</summary>
        public uint RequestId;

        /// <summary>Clock micros the request times out at; 0 = never.</summary>
        public long DeadlineMicros;

        /// <summary>Free-list link.</summary>
        public int Next = -1;

        /// <summary>Whether the request is waiting for a response.</summary>
        public bool InUse;

        /// <summary>Registration of the caller's cancellation token, if any.</summary>
        public CancellationTokenRegistration Registration;

        /// <summary>Version of the current occupant's value task.</summary>
        public short Version => _source.Version;

        /// <summary>Resets the source for the next occupant (a slot that was never armed).</summary>
        public void Reset()
        {
            Registration.Dispose();
            Registration = default;
            _source.Reset();
        }

        /// <summary>Completes the request with its response (game thread).</summary>
        /// <param name="response">The response; its lease belongs to the awaiter.</param>
        public void SetResult(in ReceiveLease response) => _source.SetResult(response);

        /// <summary>Completes the request with a failure (game thread).</summary>
        /// <param name="exception">Why no response will arrive.</param>
        public void SetException(Exception exception) => _source.SetException(exception);

        /// <summary>Cancels the wait (any thread, from the caller's cancellation token). The request itself keeps flowing.</summary>
        /// <param name="token">The token that was canceled.</param>
        public void Cancel(CancellationToken token)
        {
            if (Interlocked.Exchange(ref _canceling, 1) != 0)
            {
                return;
            }

            _source.SetException(new OperationCanceledException(token));
        }

        private int _canceling;

        /// <inheritdoc/>
        public ReceiveLease GetResult(short token)
        {
            try
            {
                return _source.GetResult(token);
            }
            finally
            {
                // The value task has been consumed, so the slot (and its version) may serve the next request.
                Registration.Dispose();
                Registration = default;
                _canceling = 0;
                _source.Reset();
                engine.PushRequestSlot(index, this);
            }
        }

        /// <inheritdoc/>
        public ValueTaskSourceStatus GetStatus(short token) => _source.GetStatus(token);

        /// <inheritdoc/>
        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
            _source.OnCompleted(continuation, state, token, flags);
    }
}
