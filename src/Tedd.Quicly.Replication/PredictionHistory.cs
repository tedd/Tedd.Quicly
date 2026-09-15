using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Replication;

/// <summary>
/// Re-simulates one input during reconciliation. Implemented by a struct so
/// <see cref="PredictionHistory{TState}.Reconcile"/> is specialized by the JIT and allocates no delegate.
/// </summary>
/// <typeparam name="TState">The predicted state.</typeparam>
public interface IReplay<TState>
    where TState : unmanaged
{
    /// <summary>Applies input <paramref name="sequence"/> to <paramref name="state"/> (the state after the previous input).</summary>
    /// <param name="sequence">Sequence number of the input to apply (fetch it from an <see cref="InputBuffer{TInput}"/>).</param>
    /// <param name="state">In: the state before the input; out: the state after it.</param>
    void Replay(uint sequence, ref TState state);
}

/// <summary>
/// Client-side prediction history for server reconciliation: the predicted state after each local input, replayed
/// on top of the server's authoritative state when it arrives.
/// </summary>
/// <typeparam name="TState">The predicted state (for example a player's position/velocity).</typeparam>
/// <remarks>
/// <para>
/// Each tick: apply input <c>s</c> locally, then <c>Record(s, state)</c>. When the server reports the authoritative
/// state after input <c>S</c>, call <see cref="Reconcile"/>: every entry up to and including <c>S</c> is dropped, the
/// later inputs are re-applied through <see cref="IReplay{TState}"/> starting from the authoritative state, their
/// recorded states are overwritten with the corrected ones, and the corrected newest state is returned.
/// </para>
/// <para>Sequences use 32-bit serial arithmetic. Allocation-free after construction; not thread-safe.</para>
/// </remarks>
public sealed class PredictionHistory<TState>
    where TState : unmanaged
{
    /// <summary>Default number of predicted states remembered.</summary>
    public const int DefaultCapacity = 64;

    private readonly uint[] _sequences;
    private readonly TState[] _states;
    private int _oldest;
    private int _count;
    private uint _lastReconciled;
    private bool _hasReconciled;

    /// <summary>Creates a history.</summary>
    /// <param name="capacity">Predicted states remembered (at least 1); the oldest is dropped when full.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="capacity"/> is less than 1.</exception>
    public PredictionHistory(int capacity = DefaultCapacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        _sequences = new uint[capacity];
        _states = new TState[capacity];
    }

    /// <summary>Number of predicted states remembered at most.</summary>
    public int Capacity => _sequences.Length;

    /// <summary>Number of predicted states not yet confirmed by the server.</summary>
    public int Count => _count;

    /// <summary><see langword="true"/> once <see cref="Reconcile"/> has accepted an authoritative state.</summary>
    public bool HasReconciled => _hasReconciled;

    /// <summary>Sequence of the newest authoritative state accepted by <see cref="Reconcile"/> (meaningful when <see cref="HasReconciled"/>).</summary>
    public uint LastReconciledSequence => _lastReconciled;

    /// <summary>Records the predicted state after input <paramref name="sequence"/>.</summary>
    /// <param name="sequence">The input's sequence; must be newer than the previously recorded one and than the last reconciled one.</param>
    /// <param name="predicted">The state after applying the input.</param>
    /// <returns><see langword="false"/> (ignored) when the sequence is not newer.</returns>
    public bool Record(uint sequence, in TState predicted)
    {
        if ((_count > 0 && !SerialNumber.IsNewer(sequence, _sequences[Slot(_count - 1)]))
            || (_hasReconciled && !SerialNumber.IsNewer(sequence, _lastReconciled)))
        {
            return false;
        }

        if (_count == _sequences.Length)
        {
            _oldest = Slot(1);
            _count--;
        }

        int slot = Slot(_count);
        _sequences[slot] = sequence;
        _states[slot] = predicted;
        _count++;
        return true;
    }

    /// <summary>Returns the recorded (or, after a reconciliation, corrected) state after input <paramref name="sequence"/>.</summary>
    /// <param name="sequence">The input's sequence.</param>
    /// <param name="state">The state, or <see langword="default"/>.</param>
    /// <returns><see langword="false"/> when no state is recorded for the sequence.</returns>
    public bool TryGetPredicted(uint sequence, out TState state)
    {
        for (int i = _count - 1; i >= 0; i--)
        {
            int slot = Slot(i);
            if (_sequences[slot] == sequence)
            {
                state = _states[slot];
                return true;
            }
        }

        state = default;
        return false;
    }

    /// <summary>
    /// Applies the server's authoritative state after input <paramref name="sequence"/>: drops every entry up to and
    /// including it, replays the later inputs from <paramref name="authoritative"/> and stores the corrected states.
    /// </summary>
    /// <typeparam name="TReplay">The replay implementation (a struct: no delegate, no allocation).</typeparam>
    /// <param name="sequence">The last input the server applied.</param>
    /// <param name="authoritative">The server's state after that input.</param>
    /// <param name="replay">Re-applies one input; called once per remaining entry, oldest first.</param>
    /// <returns>
    /// The corrected current state (the authoritative state when no later input is recorded). A stale update — a
    /// sequence not newer than <see cref="LastReconciledSequence"/> — changes nothing and returns the newest recorded
    /// state (or <paramref name="authoritative"/> when nothing is recorded).
    /// </returns>
    public TState Reconcile<TReplay>(uint sequence, in TState authoritative, ref TReplay replay)
        where TReplay : struct, IReplay<TState>
    {
        if (_hasReconciled && !SerialNumber.IsNewer(sequence, _lastReconciled))
        {
            return _count > 0 ? _states[Slot(_count - 1)] : authoritative;
        }

        _lastReconciled = sequence;
        _hasReconciled = true;
        while (_count > 0 && !SerialNumber.IsNewer(_sequences[_oldest], sequence))
        {
            _oldest = Slot(1);
            _count--;
        }

        TState state = authoritative;
        for (int i = 0; i < _count; i++)
        {
            int slot = Slot(i);
            replay.Replay(_sequences[slot], ref state);
            _states[slot] = state;
        }

        return state;
    }

    /// <summary>Forgets every recorded state and the reconciliation position.</summary>
    public void Clear()
    {
        _oldest = 0;
        _count = 0;
        _hasReconciled = false;
        _lastReconciled = 0;
    }

    private int Slot(int logical)
    {
        int slot = _oldest + logical;
        return slot >= _sequences.Length ? slot - _sequences.Length : slot;
    }
}
