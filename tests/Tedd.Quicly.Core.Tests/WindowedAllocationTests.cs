using Xunit.Sdk;

namespace Tedd.Quicly.Core.Tests;

public class WindowedAllocationTests
{
    [Fact]
    public void An_Allocation_In_Every_Window_Fails_After_Every_Round()
    {
        int calls = 0;
        var ex = Assert.Throws<TrueException>(() => WindowedAllocation.AssertNone(() =>
        {
            calls++;
            GC.KeepAlive(new byte[32]);
        }));

        Assert.Equal(WindowedAllocation.Rounds * WindowedAllocation.Windows, calls);
        Assert.Contains($"in each of {WindowedAllocation.Rounds} rounds", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Two_Allocating_Windows_In_Every_Round_Fail()
    {
        int calls = 0;
        Assert.Throws<TrueException>(() => WindowedAllocation.AssertNone(() =>
        {
            if (calls++ % WindowedAllocation.Windows is 1 or 3)
            {
                GC.KeepAlive(new byte[32]);
            }
        }));

        Assert.Equal(WindowedAllocation.Rounds * WindowedAllocation.Windows, calls);
    }

    [Fact]
    public void Two_One_Off_Allocations_In_A_Round_Are_Measured_Again()
    {
        int calls = 0;
        int windows = WindowedAllocation.AssertNone(() =>
        {
            if (calls++ is 0 or 3)
            {
                GC.KeepAlive(new byte[32]);
            }
        });

        // A one-off runtime allocation in the second round is tolerated like any single allocating window.
        Assert.Equal(2 * WindowedAllocation.Windows, windows);
        Assert.Equal(windows, calls);
    }
}
