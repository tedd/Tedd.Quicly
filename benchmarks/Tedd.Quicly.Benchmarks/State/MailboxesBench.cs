using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Archive.State;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Threading;

namespace Tedd.Quicly.Benchmarks.State;

/// <summary>
/// One receive→poll cycle of a coalescing keyed channel with 65 536 key slots, single-threaded (the cost of the
/// instructions and fences, not of cross-core traffic): post <see cref="DirtyKeys"/> updates (exchange + dirty OR),
/// then pop the dirty set and take each mailbox. The shipping <see cref="Mailboxes"/> (V1: clean cache lines skipped
/// with one vector test) against the archived scalar scan (<see cref="MailboxesV0"/>), against pushing the same items
/// through an <see cref="SpscRing{T}"/> (what a non-coalescing channel does), and the bare bitset scan on a clean
/// table (the per-poll floor of an idle channel). Reported time is per cycle.
/// </summary>
[Config(typeof(InProcessShortRunConfig))]
public class MailboxesBench
{
    private const int Slots = 65_536;

    private Mailboxes _mailboxes = null!;
    private Mailboxes _clean = null!;
    private MailboxesV0 _mailboxesV0 = null!;
    private MailboxesV0 _cleanV0 = null!;
    private SpscRing<int> _ring = null!;
    private int[] _keys = null!;
    private int[] _buffer = null!;

    [Params(16, 1_024)]
    public int DirtyKeys { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var rng = new Random(3);
        var used = new HashSet<int>();
        _keys = new int[DirtyKeys];
        for (int i = 0; i < DirtyKeys; i++)
        {
            int k;
            do
                k = rng.Next(Slots);
            while (!used.Add(k));
            _keys[i] = k;
        }

        _buffer = new int[DirtyKeys];
        _mailboxes = new Mailboxes(Slots);
        _clean = new Mailboxes(Slots);
        _mailboxesV0 = new MailboxesV0(Slots);
        _cleanV0 = new MailboxesV0(Slots);
        _ring = new SpscRing<int>(DirtyKeys);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _mailboxes.Dispose();
        _clean.Dispose();
        _mailboxesV0.Dispose();
        _cleanV0.Dispose();
    }

    [Benchmark(Baseline = true)]
    public int V1_PostPopTake()
    {
        int sum = 0;
        Mailboxes mailboxes = _mailboxes;
        int[] keys = _keys;
        for (int i = 0; i < keys.Length; i++)
            sum += mailboxes.Post(keys[i], i);

        int n = mailboxes.PopDirty(_buffer);
        for (int i = 0; i < n; i++)
            sum += mailboxes.Take(_buffer[i]);
        return sum;
    }

    [Benchmark]
    public int V0_PostPopTake()
    {
        int sum = 0;
        MailboxesV0 mailboxes = _mailboxesV0;
        int[] keys = _keys;
        for (int i = 0; i < keys.Length; i++)
            sum += mailboxes.Post(keys[i], i);

        int n = mailboxes.PopDirty(_buffer);
        for (int i = 0; i < n; i++)
            sum += mailboxes.Take(_buffer[i]);
        return sum;
    }

    [Benchmark]
    public int Ring_EnqueueDequeue()
    {
        int sum = 0;
        SpscRing<int> ring = _ring;
        int[] keys = _keys;
        for (int i = 0; i < keys.Length; i++)
            ring.TryEnqueue(keys[i]);
        while (ring.TryDequeue(out int item))
            sum += item;
        return sum;
    }

    [Benchmark]
    public int V1_PopDirty_Clean() => _clean.PopDirty(_buffer);

    [Benchmark]
    public int V0_PopDirty_Clean() => _cleanV0.PopDirty(_buffer);
}
