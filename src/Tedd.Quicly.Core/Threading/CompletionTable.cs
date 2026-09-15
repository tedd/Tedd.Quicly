using System.Diagnostics;
using System.Threading.Tasks.Sources;

namespace Tedd.Quicly.Core.Threading;

/// <summary>
/// Pre-allocated table of completion slots for tracked sends.
/// </summary>
/// <remarks>
/// <para>
/// Each slot tracks two independent stages (<see cref="CompletionStage.BufferReleased"/> and
/// <see cref="CompletionStage.RemoteAccepted"/>) and one final <see cref="DeliveryStatus"/>. Waiting on a
/// stage is allocation-free: every slot is an <see cref="IValueTaskSource{TResult}"/> backed by one
/// <see cref="ManualResetValueTaskSourceCore{TResult}"/> per stage, reset when the slot is reused.
/// </para>
/// <para>
/// Threading: <see cref="TryAllocate"/>, <see cref="WaitAsync"/>, <see cref="Wait"/> and <see cref="Release"/>
/// belong to the owner (game) thread. <see cref="Complete"/>, <see cref="IsCompleted"/> and
/// <see cref="GetStatus"/> may be called from any thread, typically the transport thread, concurrently with
/// the owner's calls. A stage may have at most one outstanding wait at a time.
/// </para>
/// <para>
/// Lifetime: a slot is returned to the free list when both stages are complete and no wait is outstanding,
/// or when <see cref="Release"/> is called (which completes the remaining stages with
/// <see cref="DeliveryStatus.Canceled"/> unless a terminal status is already known). Afterwards the token is
/// stale: <see cref="Complete"/> ignores it, <see cref="IsCompleted"/> reports true, and
/// <see cref="GetStatus"/>, <see cref="WaitAsync"/> and <see cref="Wait"/> return the status the slot had when it
/// was last released. That answer is exact for the most recently released occupant of the slot and best-effort
/// for older ones.
/// </para>
/// <para>
/// Cancelling a wait only cancels the wait: the slot stays alive and the send is not affected. Every
/// <see cref="ValueTask{TResult}"/> returned by <see cref="WaitAsync"/> must be consumed exactly once (awaited
/// or its result read); an unconsumed one keeps its slot alive.
/// </para>
/// </remarks>
public sealed class CompletionTable
{
    private readonly Slot[] _slots;
    private readonly MpscRing<int> _free;

    /// <summary>Creates a table with <paramref name="capacity"/> slots.</summary>
    /// <param name="capacity">Number of sends that can be tracked at the same time (1 … 2^30).</param>
    /// <param name="runContinuationsAsynchronously">
    /// When true, continuations of <see cref="WaitAsync"/> are queued to the thread pool instead of running
    /// inline on the thread that calls <see cref="Complete"/>.
    /// </param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="capacity"/> is not positive or exceeds 2^30.</exception>
    public CompletionTable(int capacity, bool runContinuationsAsynchronously = false)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(capacity, 1 << 30);
        _slots = new Slot[capacity];
        _free = new MpscRing<int>(capacity);
        for (int i = 0; i < capacity; i++)
        {
            _slots[i] = new Slot(this, i, runContinuationsAsynchronously);
            _free.TryEnqueue(i);
        }
    }

    /// <summary>Number of slots in the table.</summary>
    public int Capacity => _slots.Length;

    /// <summary>Approximate number of slots currently available to <see cref="TryAllocate"/>.</summary>
    public int Available => _free.Count;

    /// <summary>Takes a free slot. Owner thread only.</summary>
    /// <param name="token">The new token, or <see langword="default"/> when the table is exhausted.</param>
    /// <returns><see langword="false"/> when every slot is in use.</returns>
    public bool TryAllocate(out SendToken token)
    {
        if (!_free.TryDequeue(out int index))
        {
            token = default;
            return false;
        }

        Slot slot = _slots[index];
        token = new SendToken(index, slot.Allocate());
        return true;
    }

    /// <summary>
    /// Marks <paramref name="stage"/> complete and, when <paramref name="status"/> is not
    /// <see cref="DeliveryStatus.Pending"/>, records it as the send's status. Any thread.
    /// </summary>
    /// <remarks>Ignored for stale tokens and for stages that are already complete.</remarks>
    /// <param name="token">Token of the send.</param>
    /// <param name="stage">Stage that completed.</param>
    /// <param name="status">Outcome known at this point; <see cref="DeliveryStatus.Pending"/> keeps the current status.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="token"/> names a slot outside the table.</exception>
    public void Complete(SendToken token, CompletionStage stage, DeliveryStatus status)
    {
        Slot slot = GetSlot(token);
        if (Volatile.Read(ref slot.Generation) != token.Generation)
            return;
        slot.Complete((int)stage, status);
    }

    /// <summary>True when <paramref name="stage"/> has completed, or when the token is stale. Any thread.</summary>
    /// <param name="token">Token of the send.</param>
    /// <param name="stage">Stage to query.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="token"/> names a slot outside the table.</exception>
    public bool IsCompleted(SendToken token, CompletionStage stage)
    {
        Slot slot = GetSlot(token);
        if (Volatile.Read(ref slot.Generation) != token.Generation)
            return true;
        return slot.IsCompleted((int)stage);
    }

    /// <summary>Current status of the send; for a stale token, the status recorded when the slot was released. Any thread.</summary>
    /// <param name="token">Token of the send.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="token"/> names a slot outside the table.</exception>
    public DeliveryStatus GetStatus(SendToken token)
    {
        Slot slot = GetSlot(token);
        if (Volatile.Read(ref slot.Generation) != token.Generation)
            return slot.ReleasedStatus;
        return slot.GetStatus();
    }

    /// <summary>
    /// Waits for <paramref name="stage"/> to complete. Owner thread only. Completes synchronously (no allocation,
    /// no continuation) when the stage is already complete or the token is stale.
    /// </summary>
    /// <remarks>
    /// The wait itself never allocates. A continuation attached before the stage completes runs inline on the
    /// completing thread (or on the thread pool when the table was created with
    /// <c>runContinuationsAsynchronously</c>). If the stage completes in the short window between the awaiter's
    /// <c>IsCompleted</c> check and its <c>OnCompleted</c> call, the runtime queues the continuation to the
    /// thread pool, which allocates one small work item; polling completions on the owner thread avoids that
    /// race entirely.
    /// </remarks>
    /// <param name="token">Token of the send.</param>
    /// <param name="stage">Stage to wait for.</param>
    /// <param name="cancellationToken">Cancels the wait, not the send.</param>
    /// <returns>The send's status when the stage completed.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="token"/> names a slot outside the table.</exception>
    /// <exception cref="InvalidOperationException">A wait for this stage is already outstanding.</exception>
    /// <exception cref="OperationCanceledException">Thrown by the awaiter when <paramref name="cancellationToken"/> is canceled first.</exception>
    public ValueTask<DeliveryStatus> WaitAsync(SendToken token, CompletionStage stage, CancellationToken cancellationToken = default)
    {
        Slot slot = GetSlot(token);
        if (Volatile.Read(ref slot.Generation) != token.Generation)
            return new ValueTask<DeliveryStatus>(slot.ReleasedStatus);
        return slot.WaitAsync((int)stage, cancellationToken);
    }

    /// <summary>
    /// Blocks until <paramref name="stage"/> completes or <paramref name="timeout"/> elapses. Owner thread only.
    /// Spins briefly, then parks on an event that is allocated once per slot and reused.
    /// </summary>
    /// <param name="token">Token of the send.</param>
    /// <param name="stage">Stage to wait for.</param>
    /// <param name="timeout">Maximum time to wait; <see cref="Timeout.InfiniteTimeSpan"/> waits forever.</param>
    /// <returns>
    /// The send's status once the stage completed, or <see cref="DeliveryStatus.Pending"/> when the timeout
    /// elapsed first (use <see cref="IsCompleted"/> to tell the two apart when the stage itself may complete
    /// with a pending status).
    /// </returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="token"/> names a slot outside the table, or <paramref name="timeout"/> is negative and not infinite.</exception>
    public DeliveryStatus Wait(SendToken token, CompletionStage stage, TimeSpan timeout)
    {
        long timeoutMs = (long)timeout.TotalMilliseconds;
        if (timeoutMs < -1 || timeoutMs > int.MaxValue)
            throw new ArgumentOutOfRangeException(nameof(timeout));
        Slot slot = GetSlot(token);
        if (Volatile.Read(ref slot.Generation) != token.Generation)
            return slot.ReleasedStatus;
        return slot.Wait((int)stage, (int)timeoutMs);
    }

    /// <summary>
    /// Releases the slot early. Stages that have not completed are completed with the current status, or
    /// <see cref="DeliveryStatus.Canceled"/> when no outcome is known; outstanding waits observe that status.
    /// Ignored for stale tokens. Owner thread only.
    /// </summary>
    /// <param name="token">Token of the send.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="token"/> names a slot outside the table.</exception>
    public void Release(SendToken token)
    {
        Slot slot = GetSlot(token);
        if (Volatile.Read(ref slot.Generation) != token.Generation)
            return;
        DeliveryStatus status = slot.GetStatus();
        if (status == DeliveryStatus.Pending)
            status = DeliveryStatus.Canceled;
        slot.Complete(0, status);
        slot.Complete(1, status);
    }

    private Slot GetSlot(SendToken token)
    {
        Slot[] slots = _slots;
        if ((uint)token.Slot >= (uint)slots.Length)
            throw new ArgumentOutOfRangeException(nameof(token), "Token does not belong to this table.");
        return slots[token.Slot];
    }

    private void ReturnToFreeList(int index) => _free.TryEnqueue(index);

    private sealed class Slot : IValueTaskSource<DeliveryStatus>
    {
        // State bits. Stage-specific bits are shifted left by the stage index (0 or 1).
        private const int Allocated = 1 << 0;
        private const int Done = 1 << 1;      // stage completed
        private const int Pending = 1 << 3;   // a ValueTask for the stage is outstanding (blocks release)
        private const int Armed = 1 << 5;     // the stage's core has not been transitioned yet
        private const int DoneBoth = Done | (Done << 1);
        private const int PendingAny = Pending | (Pending << 1);

        private static readonly Action<object?, CancellationToken> s_cancel0 = static (s, ct) => ((Slot)s!).OnCanceled(0, ct);
        private static readonly Action<object?, CancellationToken> s_cancel1 = static (s, ct) => ((Slot)s!).OnCanceled(1, ct);

        private readonly CompletionTable _owner;
        private readonly int _index;

        // One core per stage. A ValueTask's token is the version of the core it belongs to; the two versions are
        // kept different at all times (see ResetCore) so a token identifies its stage unambiguously. A core is
        // reset as soon as its ValueTask has been consumed, which makes it reusable after a canceled wait
        // without touching the other stage's outstanding ValueTask.
        private ManualResetValueTaskSourceCore<DeliveryStatus> _core0;
        private ManualResetValueTaskSourceCore<DeliveryStatus> _core1;
        private CancellationTokenRegistration _registration0;
        private CancellationTokenRegistration _registration1;
        private ManualResetEventSlim? _event;
        private int _state;
        private int _status;
        private int _releasedStatus;

        public uint Generation = 1;

        public Slot(CompletionTable owner, int index, bool runContinuationsAsynchronously)
        {
            _owner = owner;
            _index = index;
            _core0.RunContinuationsAsynchronously = runContinuationsAsynchronously;
            _core1.RunContinuationsAsynchronously = runContinuationsAsynchronously;
            _core1.Reset(); // versions start out different: 0 and 1
        }

        public DeliveryStatus ReleasedStatus => (DeliveryStatus)Volatile.Read(ref _releasedStatus);

        public DeliveryStatus GetStatus() => (DeliveryStatus)Volatile.Read(ref _status);

        /// <summary>Owner thread, slot must be free. Returns the generation for the token.</summary>
        public uint Allocate()
        {
            _status = (int)DeliveryStatus.Pending;
            Volatile.Write(ref _state, Allocated);
            return Generation;
        }

        /// <summary>True unless the slot is allocated and the stage is not done (a slot mid-release counts as complete).</summary>
        public bool IsCompleted(int stage) => (Volatile.Read(ref _state) & (Allocated | (Done << stage))) != Allocated;

        public void Complete(int stage, DeliveryStatus status)
        {
            int done = Done << stage;
            int armed = Armed << stage;
            int s = Volatile.Read(ref _state);
            while (true)
            {
                if ((s & Allocated) == 0 || (s & done) != 0)
                    return;
                if (status != DeliveryStatus.Pending)
                    Volatile.Write(ref _status, (int)status);
                int next = (s | done) & ~armed;
                int seen = Interlocked.CompareExchange(ref _state, next, s);
                if (seen == s)
                    break;
                s = seen;
            }

            if ((s & armed) != 0)
            {
                DeliveryStatus result = GetStatus();
                if (stage == 0)
                    _core0.SetResult(result);
                else
                    _core1.SetResult(result);
            }

            Volatile.Read(ref _event)?.Set();
            TryRelease();
        }

        public ValueTask<DeliveryStatus> WaitAsync(int stage, CancellationToken cancellationToken)
        {
            int done = Done << stage;
            int pending = Pending << stage;
            int s = Volatile.Read(ref _state);
            while (true)
            {
                if ((s & Allocated) == 0)
                    return new ValueTask<DeliveryStatus>(ReleasedStatus);
                if ((s & done) != 0)
                    return new ValueTask<DeliveryStatus>(GetStatus());
                if ((s & pending) != 0)
                    throw new InvalidOperationException("A wait for this stage is already outstanding.");
                int seen = Interlocked.CompareExchange(ref _state, s | pending | (Armed << stage), s);
                if (seen == s)
                    break;
                s = seen;
            }

            short token;
            if (stage == 0)
            {
                token = _core0.Version;
                if (cancellationToken.CanBeCanceled)
                    _registration0 = cancellationToken.UnsafeRegister(s_cancel0, this);
            }
            else
            {
                token = _core1.Version;
                if (cancellationToken.CanBeCanceled)
                    _registration1 = cancellationToken.UnsafeRegister(s_cancel1, this);
            }

            return new ValueTask<DeliveryStatus>(this, token);
        }

        /// <summary>Resets a consumed core, keeping the two versions different.</summary>
        private void ResetCore(int stage)
        {
            if (stage == 0)
            {
                _core0.Reset();
                if (_core0.Version == _core1.Version)
                    _core0.Reset();
            }
            else
            {
                _core1.Reset();
                if (_core0.Version == _core1.Version)
                    _core1.Reset();
            }
        }

        private void OnCanceled(int stage, CancellationToken cancellationToken)
        {
            // Whoever clears the Armed bit (this callback or Complete) owns the core's transition.
            int armed = Armed << stage;
            if ((Interlocked.And(ref _state, ~armed) & armed) == 0)
                return;

            var exception = new OperationCanceledException(cancellationToken);
            if (stage == 0)
                _core0.SetException(exception);
            else
                _core1.SetException(exception);
        }

        public DeliveryStatus Wait(int stage, int timeoutMs)
        {
            if (TryGetCompleted(stage, out DeliveryStatus status))
                return status;

            SpinWait spinner = default;
            while (!spinner.NextSpinWillYield)
            {
                spinner.SpinOnce();
                if (TryGetCompleted(stage, out status))
                    return status;
            }

            ManualResetEventSlim? ev = _event;
            if (ev is null)
            {
                ev = new ManualResetEventSlim(false);
                Volatile.Write(ref _event, ev);
            }

            long start = Stopwatch.GetTimestamp();
            while (true)
            {
                ev.Reset();
                Interlocked.MemoryBarrier();
                if (TryGetCompleted(stage, out status))
                    return status;

                int remaining = timeoutMs;
                if (timeoutMs != Timeout.Infinite)
                {
                    long elapsed = (long)Stopwatch.GetElapsedTime(start).TotalMilliseconds;
                    remaining = elapsed >= timeoutMs ? 0 : (int)(timeoutMs - elapsed);
                }

                bool signaled = ev.Wait(remaining);
                if (TryGetCompleted(stage, out status))
                    return status;
                if (!signaled)
                    return DeliveryStatus.Pending;

                // Woken by the other stage completing: go back to sleep for the remaining time.
            }
        }

        private bool TryGetCompleted(int stage, out DeliveryStatus status)
        {
            int s = Volatile.Read(ref _state);
            if ((s & Allocated) == 0)
            {
                status = ReleasedStatus;
                return true;
            }

            if ((s & (Done << stage)) != 0)
            {
                status = GetStatus();
                return true;
            }

            status = DeliveryStatus.Pending;
            return false;
        }

        private void TryRelease()
        {
            int s = Volatile.Read(ref _state);
            while (true)
            {
                if ((s & (Allocated | DoneBoth)) != (Allocated | DoneBoth) || (s & PendingAny) != 0)
                    return;
                Volatile.Write(ref _releasedStatus, Volatile.Read(ref _status));
                int seen = Interlocked.CompareExchange(ref _state, 0, s);
                if (seen == s)
                    break;
                s = seen;
            }

            // Generations are always odd (start at 1, step 2), so they never wrap to 0, the invalid generation.
            Volatile.Write(ref Generation, Generation + 2);
            _owner.ReturnToFreeList(_index);
        }

        /// <summary>Maps a ValueTask token to its stage. Only a stage with an outstanding ValueTask matches.</summary>
        private int DecodeStage(short token)
        {
            int s = Volatile.Read(ref _state);
            if (token == _core0.Version && (s & Pending) != 0)
                return 0;
            if (token == _core1.Version && (s & (Pending << 1)) != 0)
                return 1;
            throw new InvalidOperationException("The ValueTask has already been consumed or does not belong to the current occupant of this completion slot.");
        }

        DeliveryStatus IValueTaskSource<DeliveryStatus>.GetResult(short token)
        {
            int stage = DecodeStage(token);
            try
            {
                return stage == 0 ? _core0.GetResult(token) : _core1.GetResult(token);
            }
            finally
            {
                if (stage == 0)
                {
                    _registration0.Dispose();
                    _registration0 = default;
                }
                else
                {
                    _registration1.Dispose();
                    _registration1 = default;
                }

                ResetCore(stage);
                Interlocked.And(ref _state, ~(Pending << stage));
                TryRelease();
            }
        }

        ValueTaskSourceStatus IValueTaskSource<DeliveryStatus>.GetStatus(short token)
        {
            int stage = DecodeStage(token);
            return stage == 0 ? _core0.GetStatus(token) : _core1.GetStatus(token);
        }

        void IValueTaskSource<DeliveryStatus>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags)
        {
            int stage = DecodeStage(token);
            if (stage == 0)
                _core0.OnCompleted(continuation, state, token, flags);
            else
                _core1.OnCompleted(continuation, state, token, flags);
        }
    }
}
