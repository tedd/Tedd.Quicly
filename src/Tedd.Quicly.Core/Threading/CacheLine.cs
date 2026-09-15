using System.Runtime.InteropServices;

namespace Tedd.Quicly.Core.Threading;

/// <summary>
/// Cache-line constants used to lay out concurrently accessed fields so that writers on different
/// threads never share a line (false sharing).
/// </summary>
public static class CacheLine
{
    /// <summary>Size of one cache line in bytes on every platform we target (x64, arm64).</summary>
    public const int Size = 64;

    /// <summary>
    /// Distance between two hot fields written by different threads. Twice <see cref="Size"/> keeps the
    /// adjacent-line prefetcher of modern x64 cores from pairing the two lines.
    /// </summary>
    public const int Stride = Size * 2;
}

/// <summary>
/// A 64-bit value surrounded by padding so that it occupies its own pair of cache lines.
/// Use one per writer thread for indices, counters and flags that other threads only read.
/// </summary>
[StructLayout(LayoutKind.Explicit, Size = CacheLine.Stride * 2)]
public struct PaddedLong
{
    /// <summary>The value. It sits <see cref="CacheLine.Stride"/> bytes from both ends of the struct.</summary>
    [FieldOffset(CacheLine.Stride)]
    public long Value;
}
