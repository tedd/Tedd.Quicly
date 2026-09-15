using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace Tedd.Quicly.Replication;

/// <summary>
/// Wire format of a redundant input batch, written by <see cref="InputBuffer{TInput}.WriteRedundant"/> and accepted
/// by <see cref="InputQueue{TInput}.Receive"/>.
/// </summary>
/// <remarks>
/// <code>
/// FirstSequence : u32 LE     sequence number of the first (oldest) input in the batch
/// Count         : u8         1..255
/// Inputs        : Count × sizeof(TInput) raw bytes, oldest first (input k has sequence FirstSequence + k)
/// </code>
/// <para>
/// Inputs are copied as raw struct bytes (<c>MemoryMarshal</c>), so <c>TInput</c> must be a blittable struct with a
/// fixed layout, the same definition on both ends, and hosts of the same endianness (every mainstream .NET
/// target is little-endian). A batch whose length is not exactly <c>5 + Count × sizeof(TInput)</c> is malformed.
/// </para>
/// </remarks>
public static class InputBatch
{
    /// <summary>Size of the batch header in bytes.</summary>
    public const int HeaderSize = 5;

    /// <summary>Largest number of inputs in one batch.</summary>
    public const int MaxCount = 255;

    /// <summary>Returns the size in bytes of a batch of <paramref name="count"/> inputs.</summary>
    /// <typeparam name="TInput">The input struct.</typeparam>
    /// <param name="count">Number of inputs, 0..<see cref="MaxCount"/>.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="count"/> is out of range.</exception>
    public static int GetLength<TInput>(int count)
        where TInput : unmanaged
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(count, MaxCount);
        return HeaderSize + count * Unsafe.SizeOf<TInput>();
    }

    /// <summary>Validates a batch and reads its header. Never throws on malformed input.</summary>
    /// <typeparam name="TInput">The input struct.</typeparam>
    /// <param name="batch">The received bytes.</param>
    /// <param name="firstSequence">Sequence of the first input, or 0 on failure.</param>
    /// <param name="count">Number of inputs, or 0 on failure.</param>
    /// <returns>The validation result.</returns>
    public static InputBatchStatus TryReadHeader<TInput>(ReadOnlySpan<byte> batch, out uint firstSequence, out int count)
        where TInput : unmanaged
    {
        firstSequence = 0;
        count = 0;
        if (batch.Length < HeaderSize)
        {
            return InputBatchStatus.Truncated;
        }

        int n = batch[4];
        long expected = HeaderSize + (long)n * Unsafe.SizeOf<TInput>();
        if (n == 0 || batch.Length > expected)
        {
            return InputBatchStatus.Malformed;
        }

        if (batch.Length < expected)
        {
            return InputBatchStatus.Truncated;
        }

        firstSequence = BinaryPrimitives.ReadUInt32LittleEndian(batch);
        count = n;
        return InputBatchStatus.Ok;
    }
}

/// <summary>Result of parsing a redundant input batch.</summary>
public enum InputBatchStatus
{
    /// <summary>The batch is well-formed.</summary>
    Ok = 0,

    /// <summary>The batch is shorter than its header or its declared inputs.</summary>
    Truncated = 1,

    /// <summary>The batch declares zero inputs or has trailing bytes.</summary>
    Malformed = 2,
}

/// <summary>Result of <see cref="InputQueue{TInput}.TryDequeue"/>.</summary>
public enum InputDequeueStatus
{
    /// <summary>The next input in sequence order was returned.</summary>
    Ok = 0,

    /// <summary>No inputs are queued.</summary>
    Empty = 1,

    /// <summary>
    /// The next expected input is missing while later ones are queued. Wait for it (a redundant batch may still
    /// bring it) or call <see cref="InputQueue{TInput}.SkipGap"/>.
    /// </summary>
    Gap = 2,
}
