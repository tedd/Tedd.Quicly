using Tedd.Quicly.Testing.Conformance;

namespace Tedd.Quicly.Testing.Tests.Review;

/// <summary>
/// Adversarial review of fix/msquic-held-stream-resume (8107fc3), threading lens: the new conformance scenario
/// <c>HeldStreamIsIndicatedAgainAfterEveryResume</c> on the simulator. The scenario resumes the held stream from a second,
/// real thread, while the simulated harness burns virtual time as fast as the pumping thread can spin
/// (<see cref="SimulatedTransportHarness.Pump"/>: 250 virtual microseconds per step, no waiting). The two are coupled by
/// nothing but the scheduler: every one of the 2 000 resumes costs whatever virtual time the pump gets through before the
/// resuming thread has run, and the scenario's 30 virtual seconds are over in a fraction of a wall-clock second. The
/// simulator is otherwise deterministic (same seed, same run), and <see cref="ITransportTestHarness"/> says scenarios call
/// the transport from the thread that calls Pump.
/// </summary>
[Collection(ReviewResumeThreadingSimulatorCollection.Name)]
public class ReviewResumeThreadingSimulatorTests
{
    private static long Run(out string? failure)
    {
        using var harness = new SimulatedTransportHarness();
        failure = null;
        try
        {
            TransportConformance.Run(nameof(TransportConformance.HeldStreamIsIndicatedAgainAfterEveryResume), harness);
        }
        catch (ConformanceException ex)
        {
            failure = ex.Message;
        }

        return harness.Network.NowMicros;
    }

    [Fact]
    public void The_Held_Stream_Scenario_Takes_The_Same_Virtual_Time_Every_Run_On_The_Simulator()
    {
        long first = Run(out string? firstFailure);
        long second = Run(out string? secondFailure);
        long third = Run(out string? thirdFailure);
        Assert.Null(firstFailure);
        Assert.Null(secondFailure);
        Assert.Null(thirdFailure);
        Assert.True(first == second && second == third,
            $"three runs of the same scenario with the same seed ended at {first}, {second} and {third} virtual microseconds: "
            + "what the scenario does on the simulator depends on how two real threads were scheduled");
    }

    [Theory]
    [InlineData(100)]
    [InlineData(200)]
    public void The_Held_Stream_Scenario_Passes_On_The_Simulator_When_The_Machine_Is_Busy(int loadPercent)
    {
        // A loaded machine (a build server running test assemblies side by side): every core has something else to do.
        int stop = 0;
        Thread[] load = new Thread[Math.Max(1, Environment.ProcessorCount * loadPercent / 100)];
        for (int i = 0; i < load.Length; i++)
        {
            load[i] = new Thread(() =>
            {
                while (Volatile.Read(ref stop) == 0) Thread.SpinWait(200);
            })
            {
                IsBackground = true,
                Name = "review-load",
            };
            load[i].Start();
        }

        List<string> failures = [];
        List<long> virtualMicros = [];
        try
        {
            for (int i = 0; i < 6; i++)
            {
                virtualMicros.Add(Run(out string? failure));
                if (failure is not null) failures.Add($"run {i}: {failure}");
            }
        }
        finally
        {
            Volatile.Write(ref stop, 1);
            foreach (Thread thread in load) thread.Join();
        }

        Assert.True(failures.Count == 0,
            $"{failures.Count} of 6 runs failed on the simulator with {load.Length} busy threads on {Environment.ProcessorCount} cores, which loses nothing, because the virtual timeout ran out while the resuming thread waited for a core "
            + $"(virtual microseconds per run: {string.Join(", ", virtualMicros)}): {string.Join(" | ", failures.Take(3))}");
    }
}

/// <summary>Runs alone: the load test saturates every core, which must not disturb the tests of other classes.</summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public class ReviewResumeThreadingSimulatorCollection
{
    public const string Name = "review-resume-threading-simulator";
}