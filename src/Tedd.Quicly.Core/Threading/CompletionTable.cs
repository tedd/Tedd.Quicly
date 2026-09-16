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
/// A slot's whole state (the generation, the stage bits and the status) lives in one 64-bit word and every
/// transition is a single compare-exchange whose expected value carries the token's generation. A call with a
/// stale token therefore cannot touch the slot's next occupant, however late it arrives, and an ignored
/// completion never changes the status.
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
public sealed class CompletionTable : IDisposable
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

    /// <summary>
    /// Number of slots currently available to <see cref="TryAllocate"/>, including slots whose release is still
    /// being published by another thread (<see cref="TryAllocate"/> waits for those, so it succeeds whenever
    /// this is positive).
    /// </summary>
    public int Available => _free.Count;

    /// <summary>Takes a free slot. Owner thread only.</summary>
    /// <remarks>
    /// Returns <see langword="false"/> only when the free list is genuinely empty. A slot that a completing
    /// thread is in the middle of returning (it has claimed its place in the free list but not yet published
    /// the index, a window of a few instructions) is waited for rather than reported as exhaustion.
    /// </remarks>
    /// <param name="token">The new token, or <see langword="default"/> when the table is exhausted.</param>
    /// <returns><see langword="false"/> when every slot is in use.</returns>
    public bool TryAllocate(out SendToken token)
    {
        int index;
        SpinWait spinner = default;
        while (!_free.TryDequeue(out index))
        {
            if (_free.IsEmpty)
            {
                token = default;
                return false;
            }

            spinner.SpinOnce(sleep1Threshold: -1);
        }

        Slot slot = _slots[index];
        token = new SendToken(index, slot.Allocate());
        return true;
    }

    /// <summary>
    /// Marks <paramref name="stage"/> complete and, when <paramref name="status"/> is not
    /// <see cref="DeliveryStatus.Pending"/>, records it as the send's status. Any thread.
    /// </summary>
    /// <remarks>Ignored for stale tokens and for stages that are already complete; an ignored call never changes the status.</remarks>
    /// <param name="token">Token of the send.</param>
    /// <param name="stage">Stage that completed.</param>
    /// <param name="status">Outcome known at this point; <see cref="DeliveryStatus.Pending"/> keeps the current status.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="token"/> names a slot outside the table.</exception>
    public void Complete(SendToken token, CompletionStage stage, DeliveryStatus status)
        => GetSlot(token).Complete(token.Generation, (int)stage, status);

    /// <summary>True when <paramref name="stage"/> has completed, or when the token is stale. Any thread.</summary>
    /// <param name="token">Token of the send.</param>
    /// <param name="stage">Stage to query.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="token"/> names a slot outside the table.</exception>
    public bool IsCompleted(SendToken token, CompletionStage stage)
        => GetSlot(token).IsCompleted(token.Generation, (int)stage);

    /// <summary>Current status of the send; for a stale token, the status recorded when the slot was released. Any thread.</summary>
    /// <param name="token">Token of the send.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="token"/> names a slot outside the table.</exception>
    public DeliveryStatus GetStatus(SendToken token)
        => GetSlot(token).GetStatus(token.Generation);

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
        => GetSlot(token).WaitAsync(token.Generation, (int)stage, cancellationToken);

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
        return GetSlot(token).Wait(token.Generation, (int)stage, (int)timeoutMs);
    }

    /// <summary>
    /// Releases the slot early. Stages that have not completed are completed with the current status, or
    /// <see cref="DeliveryStatus.Canceled"/> when no outcome is known; outstanding waits observe that status.
    /// Ignored for stale tokens. Owner thread only.
    /// </summary>
    /// <param name="token">Token of the send.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="token"/> names a slot outside the table.</exception>
    public void Release(SendToken token)
        => GetSlot(token).Release(token.Generation);

    /// <summary>Frees the native memory of the free list. Call it once, after no thread can complete a send any more.</summary>
    public void Dispose() => _free.Dispose();

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
        // Layout of the packed state word:
        //   bits 63..32  generation of the current (or, for a free slot, the next) occupant; always odd
        //   bits 15..8   DeliveryStatus of the current occupant
        //   bits  7..0   stage bits; stage-specific bits are shifted left by the stage index (0 or 1)
        // A free slot has no stage bits and status Pending; an occupied one has Allocated set. Because the
        // generation is part of the word, a transition attempted with a stale generation cannot succeed.
        private const long Allocated = 1L << 0;
        private const long Done = 1L << 1;      // stage completed
        private const long Pending = 1L << 3;   // a ValueTask for the stage is outstanding (blocks release)
        private const long Armed = 1L << 5;     // the stage's core has not been transitioned yet
        private const long DoneBoth = Done | (Done << 1);
        private const long PendingAny = Pending | (Pending << 1);
        private const long Releasable = Allocated | DoneBoth;
        private const int StatusShift = 8;
        private const long StatusMask = 0xFFL << StatusShift;
        private const int GenerationShift = 32;

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

        // Generations start at 1 and step by 2, so they are always odd and never wrap to 0, the invalid generation.
        private long _state = 1L << GenerationShift;
        private int _releasedStatus;

        public Slot(CompletionTable owner, int index, bool runContinuationsAsynchronously)
        {
            _owner = owner;
            _index = index;
            _core0.RunContinuationsAsynchronously = runContinuationsAsynchronously;
            _core1.RunContinuationsAsynchronously = runContinuationsAsynchronously;
            _core1.Reset(); // versions start out different: 0 and 1
        }

        private static uint GenerationOf(long state) => (uint)(state >> GenerationShift);

        private static DeliveryStatus StatusOf(long state) => (DeliveryStatus)((state & StatusMask) >> StatusShift);

        private static bool IsLive(long state, uint generation) => GenerationOf(state) == generation;

        private DeliveryStatus ReleasedStatus => (DeliveryStatus)Volatile.Read(ref _releasedStatus);

        /// <summary>Owner thread, slot must be free. Returns the generation for the token.</summary>
        public uint Allocate()
        {
            // A free slot's word holds the next generation and nothing else. No other thread can transition a
            // free slot (every other transition needs a live generation in its expected value), so a plain
            // write suffices here.
            long free = Volatile.Read(ref _state);
            Debug.Assert((free & ~(0xFFFFFFFFL << GenerationShift)) == 0, "slot on the free list is not free");
            Volatile.Write(ref _state, free | Allocated);
            return GenerationOf(free);
        }

        /// <summary>True unless the token is live and the stage is not done.</summary>
        public bool IsCompleted(uint generation, int stage)
        {
            long s = Volatile.Read(ref _state);
            return !IsLive(s, generation) || (s & (Done << stage)) != 0;
        }

        public DeliveryStatus GetStatus(uint generation)
        {
            long s = Volatile.Read(ref _state);
            return IsLive(s, generation) ? StatusOf(s) : ReleasedStatus;
        }

        public void Complete(uint generation, int stage, DeliveryStatus status)
        {
            long done = Done << stage;
            long armed = Armed << stage;
            long s = Volatile.Read(ref _state);
            long next;
            while (true)
            {
                if (!IsLive(s, generation) || (s & done) != 0)
                    return;
                next = (s | done) & ~armed;
                if (status != DeliveryStatus.Pending)
                    next = (next & ~StatusMask) | ((long)status << StatusShift);
                long seen = Interlocked.CompareExchange(ref _state, next, s);
                if (seen == s)
                    break;
                s = seen;
            }

            if ((s & armed) != 0)
            {
                DeliveryStatus result = StatusOf(next);
                if (stage == 0)
                    _core0.SetResult(result);
                else
                    _core1.SetResult(result);
            }

            Volatile.Read(ref _event)?.Set();
            ReleaseIfDone(next);
        }

        /// <summary>Owner thread. Completes the remaining stages with the known status (or Canceled) and thereby releases the slot.</summary>
        public void Release(uint generation)
        {
            long s = Volatile.Read(ref _state);
            if (!IsLive(s, generation))
                return;
            DeliveryStatus status = StatusOf(s);
            if (status == DeliveryStatus.Pending)
                status = DeliveryStatus.Canceled;
            Complete(generation, 0, status);
            Complete(generation, 1, status);
        }

        public ValueTask<DeliveryStatus> WaitAsync(uint generation, int stage, CancellationToken cancellationToken)
        {
            long done = Done << stage;
            long pending = Pending << stage;
            long s = Volatile.Read(ref _state);
            while (true)
            {
                if (!IsLive(s, generation))
                    return new ValueTask<DeliveryStatus>(ReleasedStatus);
                if ((s & done) != 0)
                    return new ValueTask<DeliveryStatus>(StatusOf(s));
                if ((s & pending) != 0)
                    throw new InvalidOperationException("A wait for this stage is already outstanding.");
                long seen = Interlocked.CompareExchange(ref _state, s | pending | (Armed << stage), s);
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
            // The registration lives only while the stage's ValueTask is outstanding (Pending set, which blocks
            // release, and GetResult disposes the registration before clearing Pending), so the word can only
            // belong to the occupant that registered. Whoever clears the Armed bit (this callback or Complete)
            // owns the core's transition.
            long armed = Armed << stage;
            if ((Interlocked.And(ref _state, ~armed) & armed) == 0)
                return;

            var exception = new OperationCanceledException(cancellationToken);
            if (stage == 0)
                _core0.SetException(exception);
            else
                _core1.SetException(exception);
        }

        public DeliveryStatus Wait(uint generation, int stage, int timeoutMs)
        {
            if (TryGetCompleted(generation, stage, out DeliveryStatus status))
                return status;

            SpinWait spinner = default;
            while (!spinner.NextSpinWillYield)
            {
                spinner.SpinOnce();
                if (TryGetCompleted(generation, stage, out status))
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
                if (TryGetCompleted(generation, stage, out status))
                    return status;

                int remaining = timeoutMs;
                if (timeoutMs != Timeout.Infinite)
                {
                    long elapsed = (long)Stopwatch.GetElapsedTime(start).TotalMilliseconds;
                    remaining = elapsed >= timeoutMs ? 0 : (int)(timeoutMs - elapsed);
                }

                bool signaled = ev.Wait(remaining);
                if (TryGetCompleted(generation, stage, out status))
                    return status;
                if (!signaled)
                    return DeliveryStatus.Pending;

                // Woken by the other stage completing: go back to sleep for the remaining time.
            }
        }

        private bool TryGetCompleted(uint generation, int stage, out DeliveryStatus status)
        {
            long s = Volatile.Read(ref _state);
            if (!IsLive(s, generation))
            {
                status = ReleasedStatus;
                return true;
            }

            if ((s & (Done << stage)) != 0)
            {
                status = StatusOf(s);
                return true;
            }

            status = DeliveryStatus.Pending;
            return false;
        }

        /// <summary>
        /// Releases the slot when <paramref name="state"/>, the word the caller has just installed, says both
        /// stages are done and no wait is outstanding. Exactly one transition per occupancy makes that true
        /// (Done bits are only ever set, and Pending bits are only cleared, once both are set), so the caller
        /// that installed it owns the release and no other thread can transition the word any more.
        /// </summary>
        private void ReleaseIfDone(long state)
        {
            if ((state & (Releasable | PendingAny)) != Releasable)
                return;

            Volatile.Write(ref _releasedStatus, (int)StatusOf(state));
            Volatile.Write(ref _state, (long)(GenerationOf(state) + 2) << GenerationShift);
            _owner.ReturnToFreeList(_index);
        }

        /// <summary>Maps a ValueTask token to its stage. Only a stage with an outstanding ValueTask matches.</summary>
        private int DecodeStage(short token)
        {
            long s = Volatile.Read(ref _state);
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
                // Pending blocks release, so the word still belongs to this occupant; clearing the bit is the
                // transition that may complete the release (see ReleaseIfDone).
                long pending = Pending << stage;
                ReleaseIfDone(Interlocked.And(ref _state, ~pending) & ~pending);
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
