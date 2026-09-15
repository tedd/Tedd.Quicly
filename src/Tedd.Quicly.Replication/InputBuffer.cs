using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Replication;

/// <summary>
/// Client-side ring of the last inputs with their sequence numbers; writes redundant batches that repeat the newest
/// <see cref="Redundancy"/> unacknowledged inputs, so a lost datagram does not lose inputs.
/// </summary>
/// <typeparam name="TInput">A blittable input struct (see <see cref="InputBatch"/> for the wire rules).</typeparam>
/// <remarks>
/// <para>
/// Typical use per client tick: <c>seq = Add(input)</c>, predict, then <c>WriteRedundant(datagram)</c> and send it on
/// an unreliable channel. With redundancy N, any loss pattern that drops fewer than N consecutive datagrams
/// delivers every input. When the server reports the last input it processed, call <see cref="Acknowledge"/> so
/// acknowledged inputs are no longer repeated.
/// </para>
/// <para>The ring also serves reconciliation replays (<see cref="TryGet"/>). Allocation-free after construction; not thread-safe.</para>
/// </remarks>
public sealed class InputBuffer<TInput>
    where TInput : unmanaged
{
    /// <summary>Default ring capacity.</summary>
    public const int DefaultCapacity = 64;

    /// <summary>Default number of inputs per batch.</summary>
    public const int DefaultRedundancy = 3;

    private readonly TInput[] _inputs;
    private readonly uint _mask;
    private uint _next;
    private int _count;
    private uint _acked;
    private bool _hasAck;
    private int _redundancy;

    /// <summary>Creates a buffer.</summary>
    /// <param name="capacity">Inputs remembered; rounded up to a power of two (at least 1).</param>
    /// <param name="redundancy">Inputs per batch, 1..255.</param>
    /// <param name="firstSequence">Sequence number of the first input.</param>
    /// <exception cref="ArgumentOutOfRangeException">An argument is out of range.</exception>
    public InputBuffer(int capacity = DefaultCapacity, int redundancy = DefaultRedundancy, uint firstSequence = 0)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(capacity, 1 << 30);
        int size = (int)BitOperations.RoundUpToPowerOf2((uint)capacity);
        _inputs = new TInput[size];
        _mask = (uint)size - 1;
        _next = firstSequence;
        Redundancy = redundancy;
    }

    /// <summary>Number of inputs the ring remembers.</summary>
    public int Capacity => _inputs.Length;

    /// <summary>Number of inputs currently remembered.</summary>
    public int Count => _count;

    /// <summary>The sequence number the next <see cref="Add"/> will assign.</summary>
    public uint NextSequence => _next;

    /// <summary>Number of remembered inputs newer than the last acknowledged one.</summary>
    public int PendingCount
    {
        get
        {
            if (!_hasAck)
            {
                return _count;
            }

            int newer = SerialNumber.Distance(_next - 1, _acked);
            return Math.Clamp(newer, 0, _count);
        }
    }

    /// <summary>Inputs per batch, 1..255.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The value is out of range.</exception>
    public int Redundancy
    {
        get => _redundancy;
        set
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(value, 1);
            ArgumentOutOfRangeException.ThrowIfGreaterThan(value, InputBatch.MaxCount);
            _redundancy = value;
        }
    }

    /// <summary>Appends an input and returns its sequence number. The oldest input is forgotten when the ring is full.</summary>
    /// <param name="input">The input.</param>
    /// <returns>The sequence number assigned.</returns>
    public uint Add(in TInput input)
    {
        uint sequence = _next++;
        _inputs[sequence & _mask] = input;
        if (_count < _inputs.Length)
        {
            _count++;
        }

        return sequence;
    }

    /// <summary>Returns a remembered input.</summary>
    /// <param name="sequence">Its sequence number.</param>
    /// <param name="input">The input, or <see langword="default"/> when not remembered.</param>
    /// <returns><see langword="false"/> when the input was never added or has been forgotten.</returns>
    public bool TryGet(uint sequence, out TInput input)
    {
        uint age = _next - 1 - sequence;
        if (age < (uint)_count)
        {
            input = _inputs[sequence & _mask];
            return true;
        }

        input = default;
        return false;
    }

    /// <summary>Marks every input up to and including <paramref name="sequence"/> as received by the server (cumulative).</summary>
    /// <param name="sequence">The newest input the server reported.</param>
    /// <returns><see langword="false"/> (ignored) for a sequence that was never assigned or is older than the current acknowledgement.</returns>
    public bool Acknowledge(uint sequence)
    {
        if (!SerialNumber.IsNewer(_next, sequence) || (_hasAck && !SerialNumber.IsNewer(sequence, _acked)))
        {
            return false;
        }

        _acked = sequence;
        _hasAck = true;
        return true;
    }

    /// <summary>Returns the size of the batch <see cref="WriteRedundant"/> would write now (0 when nothing is pending).</summary>
    public int GetRedundantLength()
    {
        int n = Math.Min(_redundancy, PendingCount);
        return n == 0 ? 0 : InputBatch.HeaderSize + n * Unsafe.SizeOf<TInput>();
    }

    /// <summary>Writes a batch of the newest min(<see cref="Redundancy"/>, <see cref="PendingCount"/>) inputs (format: <see cref="InputBatch"/>).</summary>
    /// <param name="destination">Buffer to write to.</param>
    /// <returns>Bytes written; 0 when no unacknowledged input exists; <c>-1</c> when <paramref name="destination"/> is too small.</returns>
    public int WriteRedundant(Span<byte> destination)
    {
        int n = Math.Min(_redundancy, PendingCount);
        if (n == 0)
        {
            return 0;
        }

        int size = Unsafe.SizeOf<TInput>();
        int total = InputBatch.HeaderSize + n * size;
        if (destination.Length < total)
        {
            return -1;
        }

        uint first = _next - (uint)n;
        BinaryPrimitives.WriteUInt32LittleEndian(destination, first);
        destination[4] = (byte)n;
        Span<byte> body = destination.Slice(InputBatch.HeaderSize, n * size);
        for (int i = 0; i < n; i++)
        {
            MemoryMarshal.Write(body.Slice(i * size, size), in _inputs[(first + (uint)i) & _mask]);
        }

        return total;
    }

    /// <summary>Forgets every input and acknowledgement; the next input gets <paramref name="nextSequence"/>.</summary>
    /// <param name="nextSequence">Sequence number of the next input.</param>
    public void Reset(uint nextSequence)
    {
        _next = nextSequence;
        _count = 0;
        _hasAck = false;
        _acked = 0;
    }
}
