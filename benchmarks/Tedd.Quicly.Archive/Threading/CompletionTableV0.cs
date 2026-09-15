using System.Threading.Tasks.Sources;
using Tedd.Quicly.Core.Threading;

namespace Tedd.Quicly.Archive.Threading;

/// <summary>
/// V0 of the completion table: the simple correct version. Every state transition (allocate, complete, wait,
/// release) takes the slot's monitor lock, and the free list is a locked stack. Same public contract as
/// <c>Tedd.Quicly.Core.Threading.CompletionTable</c>, which replaced the locks with a compare-exchanged state
/// word per slot and an MPSC ring free list. Kept for the benchmark comparison in docs/benchmarks/threading.md.
/// </summary>
public sealed class CompletionTableV0
{
    private readonly Slot[] _slots;
    private readonly Stack<int> _free;
    private readonly Lock _freeLock = new();

    public CompletionTableV0(int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        _slots = new Slot[capacity];
        _free = new Stack<int>(capacity);
        for (int i = capacity - 1; i >= 0; i--)
        {
            _slots[i] = new Slot(this, i);
            _free.Push(i);
        }
    }

    public int Capacity => _slots.Length;

    public bool TryAllocate(out SendToken token)
    {
        int index;
        lock (_freeLock)
        {
            if (_free.Count == 0)
            {
                token = default;
                return false;
            }

            index = _free.Pop();
        }

        token = new SendToken(index, _slots[index].Allocate());
        return true;
    }

    public void Complete(SendToken token, CompletionStage stage, DeliveryStatus status) => _slots[token.Slot].Complete(token.Generation, (int)stage, status);

    public bool IsCompleted(SendToken token, CompletionStage stage) => _slots[token.Slot].IsCompleted(token.Generation, (int)stage);

    public DeliveryStatus GetStatus(SendToken token) => _slots[token.Slot].GetStatus(token.Generation);

    public ValueTask<DeliveryStatus> WaitAsync(SendToken token, CompletionStage stage) => _slots[token.Slot].WaitAsync(token.Generation, (int)stage);

    private void ReturnToFreeList(int index)
    {
        lock (_freeLock)
            _free.Push(index);
    }

    private sealed class Slot : IValueTaskSource<DeliveryStatus>
    {
        private readonly CompletionTableV0 _owner;
        private readonly int _index;
        private readonly Lock _lock = new();
        private ManualResetValueTaskSourceCore<DeliveryStatus> _core0;
        private ManualResetValueTaskSourceCore<DeliveryStatus> _core1;
        private bool _allocated;
        private bool _done0, _done1;
        private bool _pending0, _pending1;
        private bool _armed0, _armed1;
        private DeliveryStatus _status;
        private DeliveryStatus _releasedStatus;
        private uint _generation = 1;

        public Slot(CompletionTableV0 owner, int index)
        {
            _owner = owner;
            _index = index;
            _core1.Reset();
        }

        public uint Allocate()
        {
            lock (_lock)
            {
                _allocated = true;
                _status = DeliveryStatus.Pending;
                return _generation;
            }
        }

        public void Complete(uint generation, int stage, DeliveryStatus status)
        {
            bool signal;
            DeliveryStatus result;
            lock (_lock)
            {
                if (generation != _generation || !_allocated)
                    return;
                ref bool done = ref (stage == 0 ? ref _done0 : ref _done1);
                if (done)
                    return;
                done = true;
                if (status != DeliveryStatus.Pending)
                    _status = status;
                ref bool armed = ref (stage == 0 ? ref _armed0 : ref _armed1);
                signal = armed;
                armed = false;
                result = _status;
                TryReleaseLocked();
            }

            if (signal)
            {
                if (stage == 0)
                    _core0.SetResult(result);
                else
                    _core1.SetResult(result);
            }
        }

        public bool IsCompleted(uint generation, int stage)
        {
            lock (_lock)
                return generation != _generation || !_allocated || (stage == 0 ? _done0 : _done1);
        }

        public DeliveryStatus GetStatus(uint generation)
        {
            lock (_lock)
                return generation != _generation || !_allocated ? _releasedStatus : _status;
        }

        public ValueTask<DeliveryStatus> WaitAsync(uint generation, int stage)
        {
            lock (_lock)
            {
                if (generation != _generation || !_allocated)
                    return new ValueTask<DeliveryStatus>(_releasedStatus);
                if (stage == 0 ? _done0 : _done1)
                    return new ValueTask<DeliveryStatus>(_status);
                if (stage == 0)
                {
                    _pending0 = true;
                    _armed0 = true;
                    return new ValueTask<DeliveryStatus>(this, _core0.Version);
                }

                _pending1 = true;
                _armed1 = true;
                return new ValueTask<DeliveryStatus>(this, _core1.Version);
            }
        }

        private void TryReleaseLocked()
        {
            if (!_allocated || !_done0 || !_done1 || _pending0 || _pending1)
                return;
            _releasedStatus = _status;
            _allocated = false;
            _done0 = _done1 = false;
            _generation++;
            if (_generation == 0)
                _generation = 1;
            _owner.ReturnToFreeList(_index);
        }

        private int DecodeStage(short token)
        {
            if (token == _core0.Version && _pending0)
                return 0;
            if (token == _core1.Version && _pending1)
                return 1;
            throw new InvalidOperationException();
        }

        DeliveryStatus IValueTaskSource<DeliveryStatus>.GetResult(short token)
        {
            int stage = DecodeStage(token);
            DeliveryStatus result = stage == 0 ? _core0.GetResult(token) : _core1.GetResult(token);
            lock (_lock)
            {
                if (stage == 0)
                {
                    _core0.Reset();
                    if (_core0.Version == _core1.Version)
                        _core0.Reset();
                    _pending0 = false;
                }
                else
                {
                    _core1.Reset();
                    if (_core0.Version == _core1.Version)
                        _core1.Reset();
                    _pending1 = false;
                }

                TryReleaseLocked();
            }

            return result;
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
