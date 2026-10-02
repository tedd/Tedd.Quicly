using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using System.Runtime.Loader;

// In-process paired A/B: loads two driver arms (each a frozen build of Tedd.Quicly.Benchmarks.dll + its own copies of the
// QUICLY assemblies) into two isolated AssemblyLoadContexts, sets both workloads up, warms both, then alternates short
// measurement windows A,B / B,A / A,B ... on one pinned, high-priority thread. Clock, SMT-sibling and load drift then hit
// both arms alike, and each adjacent pair gives one ratio.
//
// Usage: PairHost <armA> <armB> <Class.Method[:Prop=Value,...] | stages.<workload>> [pairs=40] [windowSeconds=0.5] [tfm=net10.0]
// Output: per-arm median/mean, the paired ratio B/A (median, mean, 95 % CI of the geometric mean) and, for stages, the
// per-phase medians. PROF_AFFINITY (hex mask, default 0x50 = CPUs 4 and 6; 0 = off) as in the driver. Prop=Value sets a
// public property of the benchmark class (a [Params] value, for example DelayUs=10) before its [GlobalSetup] runs.
public static class PairHost
{
    public static int Main(string[] args)
    {
        if (args.Length < 3)
        {
            Console.Error.WriteLine("usage: PairHost <armA> <armB> <Class.Method[:Prop=Value,...]|stages.<workload>> [pairs] [windowSeconds] [tfm]");
            return 2;
        }

        string? affinityText = Environment.GetEnvironmentVariable("PROF_AFFINITY");
        long affinity = affinityText is null ? 0x50 : Convert.ToInt64(affinityText, 16);
        using (Process self = Process.GetCurrentProcess())
        {
            if (affinity != 0 && OperatingSystem.IsWindows())
            {
                self.ProcessorAffinity = (nint)affinity;
            }

            self.PriorityClass = ProcessPriorityClass.High;
        }

        Thread.CurrentThread.Priority = ThreadPriority.Highest;
        string tfm = args.Length > 5 ? args[5] : "net10.0";
        int pairs = args.Length > 3 ? int.Parse(args[3], CultureInfo.InvariantCulture) : 40;
        double window = args.Length > 4 ? double.Parse(args[4], CultureInfo.InvariantCulture) : 0.5;
        string workload = args[2];

        // Load order alternates between launches (PAIR_SWAP=1 loads B first) so first-loaded effects can be checked.
        bool swap = Environment.GetEnvironmentVariable("PAIR_SWAP") == "1";
        Arm a, b;
        if (swap)
        {
            b = new Arm("B", args[1], tfm, workload);
            a = new Arm("A", args[0], tfm, workload);
        }
        else
        {
            a = new Arm("A", args[0], tfm, workload);
            b = new Arm("B", args[1], tfm, workload);
        }

        // Warm-up: both arms, alternating, until tiering has settled.
        for (int i = 0; i < 4; i++)
        {
            a.Measure(1.0);
            b.Measure(1.0);
        }

        var resA = new List<double[]>();
        var resB = new List<double[]>();
        for (int i = 0; i < pairs; i++)
        {
            if ((i & 1) == 0)
            {
                resA.Add(a.Measure(window));
                resB.Add(b.Measure(window));
            }
            else
            {
                resB.Add(b.Measure(window));
                resA.Add(a.Measure(window));
            }
        }

        int total = a.Names.Length - 1; // the last value is the headline (total or ns/op)
        double[] ta = resA.Select(r => r[total]).ToArray();
        double[] tb = resB.Select(r => r[total]).ToArray();
        double[] logRatio = ta.Zip(tb, (x, y) => Math.Log(y / x)).ToArray();
        double meanLog = logRatio.Average();
        double sdLog = Math.Sqrt(logRatio.Sum(v => (v - meanLog) * (v - meanLog)) / (logRatio.Length - 1));
        double half = 1.96 * sdLog / Math.Sqrt(logRatio.Length);
        Console.WriteLine($"# {Environment.Version} {workload} pairs={pairs} window={window}s swap={swap}");
        Console.WriteLine($"#   A={args[0]}");
        Console.WriteLine($"#   B={args[1]}");
        if (a.Names.Length > 1)
        {
            string Phases(List<double[]> res) => string.Join(" | ", Enumerable.Range(0, a.Names.Length).Select(k => $"{a.Names[k]} {Median(res.Select(r => r[k])),6:F1}"));
            Console.WriteLine($"  A phases (median): {Phases(resA)}");
            Console.WriteLine($"  B phases (median): {Phases(resB)}");
        }

        Console.WriteLine(string.Create(CultureInfo.InvariantCulture,
            $"PAIR A median {Median(ta):F1} mean {ta.Average():F1} | B median {Median(tb):F1} mean {tb.Average():F1} | B/A geo {Math.Exp(meanLog):F4} [95% CI {Math.Exp(meanLog - half):F4} .. {Math.Exp(meanLog + half):F4}] median-ratio {Median(ta.Zip(tb, (x, y) => y / x)):F4} (n={pairs})"));
        return 0;
    }

    private static double Median(IEnumerable<double> values)
    {
        double[] s = values.OrderBy(v => v).ToArray();
        return s.Length % 2 == 1 ? s[s.Length / 2] : (s[s.Length / 2 - 1] + s[s.Length / 2]) / 2;
    }

    private sealed class Arm
    {
        private readonly Func<double, double[]> _measure;

        public Arm(string name, string armDir, string tfm, string workload)
        {
            string main = Path.Combine(armDir, "bin", "Release", tfm, "Tedd.Quicly.Benchmarks.dll");
            var context = new ArmContext(name, main);
            Assembly asm = context.LoadFromAssemblyPath(main);
            if (workload.StartsWith("stages.", StringComparison.Ordinal))
            {
                Type stages = asm.GetType("Tedd.Quicly.Benchmarks.Profiling.Stages") ?? asm.GetType("Stages", throwOnError: true)!;
                object state = stages.GetMethod("Create")!.Invoke(null, [workload["stages.".Length..]])!;
                MethodInfo measure = stages.GetMethod("Measure")!;
                Names = (string[])stages.GetField("Names")!.GetValue(null)!;
                _measure = seconds => (double[])measure.Invoke(null, [state, seconds])!;
            }
            else
            {
                int colon = workload.IndexOf(':');
                string target = colon < 0 ? workload : workload[..colon];
                string cls = target[..target.IndexOf('.')];
                string method = target[(target.IndexOf('.') + 1)..];
                Type type = asm.GetTypes().Single(t => t.Name == cls);
                MethodInfo m = type.GetMethod(method) ?? throw new ArgumentException("no method " + method);
                int ops = 1;
                foreach (CustomAttributeData attribute in m.CustomAttributes.Where(c => c.AttributeType.Name == "BenchmarkAttribute"))
                {
                    foreach (CustomAttributeNamedArgument named in attribute.NamedArguments.Where(n => n.MemberName == "OperationsPerInvoke"))
                    {
                        ops = (int)named.TypedValue.Value!;
                    }
                }

                object instance = Activator.CreateInstance(type)!;
                if (colon >= 0)
                {
                    foreach (string assignment in workload[(colon + 1)..].Split(',', StringSplitOptions.RemoveEmptyEntries))
                    {
                        string[] parts = assignment.Split('=', 2);
                        PropertyInfo property = type.GetProperty(parts[0]) ?? throw new ArgumentException($"{cls} has no property {parts[0]}");
                        object value = property.PropertyType.IsEnum
                            ? Enum.Parse(property.PropertyType, parts[1])
                            : Convert.ChangeType(parts[1], property.PropertyType, CultureInfo.InvariantCulture);
                        property.SetValue(instance, value);
                    }
                }

                foreach (MethodInfo setup in type.GetMethods().Where(x => x.CustomAttributes.Any(c => c.AttributeType.Name == "GlobalSetupAttribute")))
                {
                    setup.Invoke(instance, null);
                }

                Action run = m.CreateDelegate<Action>(instance);
                Names = ["ns/op"];
                _measure = seconds =>
                {
                    long invokes = 0;
                    var sw = Stopwatch.StartNew();
                    while (sw.Elapsed.TotalSeconds < seconds)
                    {
                        for (int i = 0; i < 16; i++)
                        {
                            run();
                        }

                        invokes += 16;
                    }

                    return [sw.Elapsed.TotalNanoseconds / (invokes * (double)ops)];
                };
            }
        }

        public string[] Names { get; }

        public double[] Measure(double seconds) => _measure(seconds);
    }

    private sealed class ArmContext(string name, string mainPath) : AssemblyLoadContext(name)
    {
        private readonly AssemblyDependencyResolver _resolver = new(mainPath);

        protected override Assembly? Load(AssemblyName assemblyName)
        {
            string? path = _resolver.ResolveAssemblyToPath(assemblyName);
            return path is null ? null : LoadFromAssemblyPath(path);
        }

        protected override nint LoadUnmanagedDll(string unmanagedDllName)
        {
            string? path = _resolver.ResolveUnmanagedDllToPath(unmanagedDllName);
            return path is null ? 0 : LoadUnmanagedDllFromPath(path);
        }
    }
}
