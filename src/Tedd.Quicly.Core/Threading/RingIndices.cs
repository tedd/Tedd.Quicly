using System.Runtime.InteropServices;

namespace Tedd.Quicly.Core.Threading;

// These live outside the generic ring classes because the runtime refuses explicit layout on generic types
// (a struct nested in SpscRing<T> is itself generic).

/// <summary>Producer and consumer indices of <see cref="SpscRing{T}"/>, each pair on its own cache lines.</summary>
[StructLayout(LayoutKind.Explicit, Size = CacheLine.Stride * 3)]
internal struct SpscIndices
{
    /// <summary>Next slot the producer writes. Written by the producer, read by the consumer.</summary>
    [FieldOffset(CacheLine.Stride)] public long Tail;

    /// <summary>Producer's snapshot of <see cref="Head"/>. Producer-private.</summary>
    [FieldOffset(CacheLine.Stride + 8)] public long CachedHead;

    /// <summary>Next slot the consumer reads. Written by the consumer, read by the producer.</summary>
    [FieldOffset(CacheLine.Stride * 2)] public long Head;

    /// <summary>Consumer's snapshot of <see cref="Tail"/>. Consumer-private.</summary>
    [FieldOffset(CacheLine.Stride * 2 + 8)] public long CachedTail;
}

/// <summary>Enqueue and dequeue positions of <see cref="MpscRing{T}"/>, each on its own cache lines.</summary>
[StructLayout(LayoutKind.Explicit, Size = CacheLine.Stride * 3)]
internal struct MpscPositions
{
    /// <summary>Next slot to claim. Compare-exchanged by producers.</summary>
    [FieldOffset(CacheLine.Stride)] public long Enqueue;

    /// <summary>Next slot to read. Written by the consumer only.</summary>
    [FieldOffset(CacheLine.Stride * 2)] public long Dequeue;
}
