using Xunit.Sdk;

namespace Tedd.Quicly.Server.Tests;

/// <summary>The rounds of <see cref="AllocationAssert"/>, with one call per window and no warm-up.</summary>
public class AllocationAssertTests
{
    [Fact]
    public void An_Allocation_In_Every_Window_Fails_After_Every_Round()
    {
        int calls = 0;
        var ex = Assert.Throws<TrueException>(() => AllocationAssert.NoAllocations(() =>
        {
            calls++;
            GC.KeepAlive(new byte[32]);
        }, warmup: 0, iterations: 1));

        Assert.Equal(AllocationAssert.Rounds * AllocationAssert.Windows, calls);
        Assert.Contains($"in each of {AllocationAssert.Rounds} rounds", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Two_Allocating_Windows_In_Every_Round_Fail()
    {
        int calls = 0;
        Assert.Throws<TrueException>(() => AllocationAssert.NoAllocations(() =>
        {
            if (calls++ % AllocationAssert.Windows is 1 or 3)
            {
                GC.KeepAlive(new byte[32]);
            }
        }, warmup: 0, iterations: 1));

        Assert.Equal(AllocationAssert.Rounds * AllocationAssert.Windows, calls);
    }

    [Fact]
    public void Two_One_Off_Allocations_In_A_Round_Are_Measured_Again()
    {
        int calls = 0;
        int windows = AllocationAssert.NoAllocations(() =>
        {
            if (calls++ is 0 or 3)
            {
                GC.KeepAlive(new byte[32]);
            }
        }, warmup: 0, iterations: 1);

        // A one-off runtime allocation in the second round is tolerated like any single allocating window.
        Assert.Equal(2 * AllocationAssert.Windows, windows);
        Assert.Equal(windows, calls);
    }
}
