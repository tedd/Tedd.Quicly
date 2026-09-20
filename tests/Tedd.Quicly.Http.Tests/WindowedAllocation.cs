namespace Tedd.Quicly.Http.Tests;

/// <summary>
/// Zero-allocation checks for steady-state hot paths (ADR 0007/0008), measured with
/// <see cref="GC.GetAllocatedBytesForCurrentThread"/> over rounds of several windows of work. A steady-state allocation shows
/// up in every window of every round. Inside the test host the runtime also allocates on the test thread now and then for its
/// own purposes: a few kilobytes in a single window, in roughly one measurement in ten, in any window, with or without a GC in
/// the window and with tiered compilation switched off as well. Such an event does not repeat within a measurement, but two
/// of them can still land in the same round, so a round passes with at most one allocating window and a round with more is
/// measured again, up to <see cref="Rounds"/> rounds.
/// </summary>
/// <remarks>
/// A copy of the Core tests' helper of the same name: test projects do not reference one another. Keep the two in step.
/// </remarks>
internal static class WindowedAllocation
{
    /// <summary>Number of measured windows per round.</summary>
    public const int Windows = 5;

    /// <summary>Number of rounds measured before the check fails.</summary>
    public const int Rounds = 3;

    /// <summary>
    /// Runs rounds of <see cref="Windows"/> calls of <paramref name="window"/> until a round allocates in at most one of them,
    /// and fails when <see cref="Rounds"/> rounds in a row allocated in more than one. The caller warms the path up first; the
    /// delegate and everything it captures exist before the first window.
    /// </summary>
    /// <param name="window">One window of work, typically a loop of thousands of calls.</param>
    /// <returns>The number of windows run, a multiple of <see cref="Windows"/>, for callers that count the work done in them.</returns>
    public static int AssertNone(Action window)
    {
        long[] deltas = new long[Rounds * Windows];
        int run = 0;
        int allocating;
        do
        {
            allocating = 0;
            for (int w = 0; w < Windows; w++, run++)
            {
                long before = GC.GetAllocatedBytesForCurrentThread();
                window();
                deltas[run] = GC.GetAllocatedBytesForCurrentThread() - before;
                if (deltas[run] != 0)
                {
                    allocating++;
                }
            }
        }
        while (allocating > 1 && run < deltas.Length);

        Assert.True(allocating <= 1, $"Allocated in more than one of {Windows} windows in each of {Rounds} rounds: {Describe(deltas)} bytes.");
        return run;
    }

    private static string Describe(long[] deltas)
    {
        string[] rounds = new string[Rounds];
        for (int r = 0; r < Rounds; r++)
        {
            rounds[r] = string.Join(", ", deltas[(r * Windows)..((r + 1) * Windows)]);
        }

        return string.Join(" | ", rounds);
    }
}
