using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Replication;

/// <summary>
/// Server-side queue of one client's inputs: accepts redundant batches (<see cref="InputBatch"/>), drops duplicates
/// and inputs already consumed, keeps the rest in sequence order within a bounded window, and reports gaps.
/// </summary>
/// <typeparam name="TInput">The input struct (same definition as the client's <see cref="InputBuffer{TInput}"/>).</typeparam>
/// <remarks>
/// <para>
/// The window is <c>[NextSequence, NextSequence + Capacity)</c> in 32-bit serial arithmetic. Inputs before it were
/// consumed or skipped (counted as <see cref="StaleCount"/>); inputs beyond it are dropped
/// (<see cref="TooFarAheadCount"/>) — a client that far ahead is misbehaving or the server is not consuming.
/// Until the first batch arrives (or <see cref="Reset(uint)"/> is called) the window starts at the first
/// sequence of the first batch received.
/// </para>
/// <para>
/// The input bytes come from the network: every batch is validated for exact length before any input is read, and
/// input <em>contents</em> must be validated by the game like any other untrusted data. Allocation-free after
/// construction; not thread-safe.
/// </para>
/// </remarks>
public sealed class InputQueue<TInput>
    where TInput : unmanaged
{
    /// <summary>Default window size.</summary>
    public const int DefaultCapacity = 64;

    private readonly TInput[] _inputs;
    private readonly bool[] _present;
    private readonly uint _mask;
    private uint _next;
    private bool _started;
    private int _count;

    /// <summary>Creates a queue.</summary>
    /// <param name="capacity">Window size in inputs; rounded up to a power of two (at least 1).</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="capacity"/> is out of range.</exception>
    public InputQueue(int capacity = DefaultCapacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(capacity, 1 << 30);
        int size = (int)BitOperations.RoundUpToPowerOf2((uint)capacity);
        _inputs = new TInput[size];
        _present = new bool[size];
        _mask = (uint)size - 1;
    }

    /// <summary>Window size in inputs.</summary>
    public int Capacity => _inputs.Length;

    /// <summary>Number of queued inputs.</summary>
    public int Count => _count;

    /// <summary>Sequence number of the next input <see cref="TryDequeue"/> will return.</summary>
    public uint NextSequence => _next;

    /// <summary><see langword="true"/> once the window has been anchored by a batch or <see cref="Reset(uint)"/>.</summary>
    public bool IsStarted => _started;

    /// <summary>Inputs received that were already queued.</summary>
    public long DuplicateCount { get; private set; }

    /// <summary>Inputs received for sequences already consumed or skipped.</summary>
    public long StaleCount { get; private set; }

    /// <summary>Inputs received beyond the window and dropped.</summary>
    public long TooFarAheadCount { get; private set; }

    /// <summary>Total sequences skipped by <see cref="SkipGap"/>.</summary>
    public long SkippedCount { get; private set; }

    /// <summary>Number of missing sequences before the next queued input (0 when the next input is present or nothing is queued).</summary>
    public int GapLength
    {
        get
        {
            if (_count == 0)
            {
                return 0;
            }

            int length = 0;
            while (!_present[(_next + (uint)length) & _mask])
            {
                length++;
            }

            return length;
        }
    }

    /// <summary>Validates a batch and queues its new inputs.</summary>
    /// <param name="batch">The received batch.</param>
    /// <param name="accepted">Number of inputs newly queued.</param>
    /// <returns><see cref="InputBatchStatus.Ok"/>, or why the batch was rejected (nothing is queued then).</returns>
    public InputBatchStatus Receive(ReadOnlySpan<byte> batch, out int accepted)
    {
        accepted = 0;
        InputBatchStatus status = InputBatch.TryReadHeader<TInput>(batch, out uint first, out int count);
        if (status != InputBatchStatus.Ok)
        {
            return status;
        }

        if (!_started)
        {
            _next = first;
            _started = true;
        }

        int size = Unsafe.SizeOf<TInput>();
        ReadOnlySpan<byte> body = batch.Slice(InputBatch.HeaderSize);
        for (int i = 0; i < count; i++)
        {
            uint sequence = first + (uint)i;
            int distance = SerialNumber.Distance(sequence, _next);
            if (distance < 0)
            {
                StaleCount++;
                continue;
            }

            if (distance >= _inputs.Length)
            {
                TooFarAheadCount++;
                continue;
            }

            // Each slot maps to exactly one sequence inside the window, so a present slot is this sequence.
            uint slot = sequence & _mask;
            if (_present[slot])
            {
                DuplicateCount++;
                continue;
            }

            _inputs[slot] = MemoryMarshal.Read<TInput>(body.Slice(i * size, size));
            _present[slot] = true;
            _count++;
            accepted++;
        }

        return InputBatchStatus.Ok;
    }

    /// <summary>Returns the next input in sequence order.</summary>
    /// <param name="sequence">The input's sequence (for <see cref="InputDequeueStatus.Gap"/> and <see cref="InputDequeueStatus.Empty"/>: the missing next sequence).</param>
    /// <param name="input">The input, or <see langword="default"/>.</param>
    /// <returns><see cref="InputDequeueStatus.Ok"/>, <see cref="InputDequeueStatus.Empty"/> or <see cref="InputDequeueStatus.Gap"/>.</returns>
    public InputDequeueStatus TryDequeue(out uint sequence, out TInput input)
    {
        sequence = _next;
        if (_count == 0)
        {
            input = default;
            return InputDequeueStatus.Empty;
        }

        uint slot = _next & _mask;
        if (!_present[slot])
        {
            input = default;
            return InputDequeueStatus.Gap;
        }

        input = _inputs[slot];
        _present[slot] = false;
        _next++;
        _count--;
        return InputDequeueStatus.Ok;
    }

    /// <summary>Gives up on the missing inputs before the next queued one (they become stale if they arrive later).</summary>
    /// <returns>Number of sequences skipped (0 when nothing is queued or the next input is present).</returns>
    public int SkipGap()
    {
        int skipped = GapLength;
        _next += (uint)skipped;
        SkippedCount += skipped;
        return skipped;
    }

    /// <summary>Drops every queued input and un-anchors the window (the next batch anchors it again). Counters are kept.</summary>
    public void Reset()
    {
        Array.Clear(_present);
        _count = 0;
        _started = false;
        _next = 0;
    }

    /// <summary>Drops every queued input and anchors the window at <paramref name="nextSequence"/>. Counters are kept.</summary>
    /// <param name="nextSequence">The next sequence expected.</param>
    public void Reset(uint nextSequence)
    {
        Reset();
        _next = nextSequence;
        _started = true;
    }
}
