namespace Tedd.Quicly.Core.Tests;

/// <summary>
/// Zero-allocation checks for steady-state hot paths (ADR 0008), measured with
/// <see cref="GC.GetAllocatedBytesForCurrentThread"/> over several windows of work. A steady-state allocation shows up in
/// every window; a one-off runtime event on the test thread (tier-up or OSR compilation, call-counting installation, seen
/// while the rest of the suite keeps the JIT busy) lands in at most one, so a single allocating window is tolerated.
/// </summary>
internal static class WindowedAllocation
{
    /// <summary>Number of measured windows.</summary>
    public const int Windows = 5;

    /// <summary>
    /// Runs <paramref name="window"/> <see cref="Windows"/> times and fails when more than one run allocated. The caller warms
    /// the path up first; the delegate and everything it captures exist before the first window.
    /// </summary>
    /// <param name="window">One window of work, typically a loop of thousands of calls.</param>
    public static void AssertNone(Action window)
    {
        long[] deltas = new long[Windows];
        int allocating = 0;
        for (int w = 0; w < Windows; w++)
        {
            long before = GC.GetAllocatedBytesForCurrentThread();
            window();
            deltas[w] = GC.GetAllocatedBytesForCurrentThread() - before;
            if (deltas[w] != 0)
            {
                allocating++;
            }
        }

        Assert.True(allocating <= 1, $"Allocated in {allocating} of {Windows} windows: {string.Join(", ", deltas)} bytes.");
    }
}
