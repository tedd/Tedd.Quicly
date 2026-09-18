using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using BenchmarkDotNet.Attributes;

namespace Tedd.Quicly.Benchmarks.Profiling;

/// <summary>
/// Sustained-workload driver: runs one <c>[Benchmark]</c> method of a benchmark class in a loop for a fixed wall time after
/// its <c>[GlobalSetup]</c> and a 2 s warm-up, printing ns per operation (<c>OperationsPerInvoke</c> honoured) per window.
/// For attaching a profiler (dotnet-trace) to exactly the workload a benchmark measures, and for quick sanity runs.
/// </summary>
public static class Sustain
{
    /// <summary>Runs <paramref name="className"/>.<paramref name="methodName"/>.</summary>
    public static void Run(string className, string methodName, double seconds, int windows)
    {
        Type type = typeof(Sustain).Assembly.GetTypes().Single(t => t.Name == className);
        MethodInfo method = type.GetMethod(methodName) ?? throw new ArgumentException($"{className} has no method {methodName}.", nameof(methodName));
        int opsPerInvoke = method.GetCustomAttribute<BenchmarkAttribute>()?.OperationsPerInvoke ?? 1;
        object instance = Activator.CreateInstance(type)!;
        foreach (MethodInfo setup in type.GetMethods().Where(m => m.GetCustomAttribute<GlobalSetupAttribute>() is not null))
        {
            setup.Invoke(instance, null);
        }

        Action run = method.CreateDelegate<Action>(instance);
        var warm = Stopwatch.StartNew();
        while (warm.Elapsed.TotalSeconds < 2)
        {
            run();
        }

        Console.WriteLine($"{Environment.Version} {type.Name}.{method.Name} ops/invoke={opsPerInvoke}");
        double windowSeconds = seconds / windows;
        for (int w = 0; w < windows; w++)
        {
            long invokes = 0;
            int gen0 = GC.CollectionCount(0);
            long allocated = GC.GetAllocatedBytesForCurrentThread();
            var sw = Stopwatch.StartNew();
            while (sw.Elapsed.TotalSeconds < windowSeconds)
            {
                for (int i = 0; i < 16; i++)
                {
                    run();
                }

                invokes += 16;
            }

            sw.Stop();
            double ns = sw.Elapsed.TotalNanoseconds / (invokes * (double)opsPerInvoke);
            Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
                $"window {w}: {ns:F1} ns/op  ({invokes * opsPerInvoke} ops, gen0 {GC.CollectionCount(0) - gen0}, alloc {GC.GetAllocatedBytesForCurrentThread() - allocated} B)"));
        }

        foreach (MethodInfo cleanup in type.GetMethods().Where(m => m.GetCustomAttribute<GlobalCleanupAttribute>() is not null))
        {
            cleanup.Invoke(instance, null);
        }
    }

    /// <summary>
    /// Pins the process to a fixed pair of logical CPUs (hex mask in <c>PROF_AFFINITY</c>, default 0x50 = CPUs 4 and 6, two
    /// physical cores of one CCD on a Ryzen 9 5950X; 0 = no pinning) and raises its priority, so separate runs measure on the
    /// same silicon. Used by the <c>stages</c> and <c>sustain</c> modes only, never by BenchmarkDotNet runs.
    /// </summary>
    public static void Place()
    {
        string? text = Environment.GetEnvironmentVariable("PROF_AFFINITY");
        long mask = text is null ? 0x50 : Convert.ToInt64(text, 16);
        using (Process self = Process.GetCurrentProcess())
        {
            if (mask != 0 && (OperatingSystem.IsWindows() || OperatingSystem.IsLinux()))
            {
                self.ProcessorAffinity = (nint)mask;
            }

            self.PriorityClass = ProcessPriorityClass.High;
        }

        Thread.CurrentThread.Priority = ThreadPriority.Highest;
    }
}
