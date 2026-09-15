using System.Collections.Concurrent;
using System.Threading.Channels;
using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Archive.Threading;
using Tedd.Quicly.Core.Threading;

namespace Tedd.Quicly.Benchmarks.Threading;

/// <summary>
/// One producer thread pushes <see cref="Items"/> longs while the benchmark thread drains them; reported time is
/// per item. Compares the shipping SpscRing (padded, cached indices) with the archived V0 (bare volatile
/// indices), ConcurrentQueue and a bounded Channel used through its synchronous TryWrite/TryRead API.
/// </summary>
[Config(typeof(ThreadingBenchmarkConfig))]
public class SpscRingBenchmarks
{
    private const int Items = 1 << 20;
    private const int Capacity = 1024;

    private SpscRing<long> _ring = null!;
    private SpscRingV0<long> _ringV0 = null!;
    private ConcurrentQueue<long> _queue = null!;
    private Channel<long> _channel = null!;

    [GlobalSetup]
    public void Setup()
    {
        _ring = new SpscRing<long>(Capacity);
        _ringV0 = new SpscRingV0<long>(Capacity);
        _queue = new ConcurrentQueue<long>();
        _channel = Channel.CreateBounded<long>(new BoundedChannelOptions(Capacity) { SingleReader = true, SingleWriter = true });
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = Items)]
    public long SpscRing_V1()
    {
        SpscRing<long> ring = _ring;
        Thread producer = Start(() =>
        {
            SpinWait spinner = default;
            for (long i = 0; i < Items; i++)
            {
                while (!ring.TryEnqueue(i))
                    spinner.SpinOnce(sleep1Threshold: -1);
            }
        });

        long sum = 0;
        SpinWait consumer = default;
        for (int n = 0; n < Items;)
        {
            if (ring.TryDequeue(out long item))
            {
                sum += item;
                n++;
            }
            else
            {
                consumer.SpinOnce(sleep1Threshold: -1);
            }
        }

        producer.Join();
        return sum;
    }

    [Benchmark(OperationsPerInvoke = Items)]
    public long SpscRing_V0_Archive()
    {
        SpscRingV0<long> ring = _ringV0;
        Thread producer = Start(() =>
        {
            SpinWait spinner = default;
            for (long i = 0; i < Items; i++)
            {
                while (!ring.TryEnqueue(i))
                    spinner.SpinOnce(sleep1Threshold: -1);
            }
        });

        long sum = 0;
        SpinWait consumer = default;
        for (int n = 0; n < Items;)
        {
            if (ring.TryDequeue(out long item))
            {
                sum += item;
                n++;
            }
            else
            {
                consumer.SpinOnce(sleep1Threshold: -1);
            }
        }

        producer.Join();
        return sum;
    }

    [Benchmark(OperationsPerInvoke = Items)]
    public long ConcurrentQueue()
    {
        ConcurrentQueue<long> queue = _queue;
        Thread producer = Start(() =>
        {
            for (long i = 0; i < Items; i++)
                queue.Enqueue(i);
        });

        long sum = 0;
        SpinWait consumer = default;
        for (int n = 0; n < Items;)
        {
            if (queue.TryDequeue(out long item))
            {
                sum += item;
                n++;
            }
            else
            {
                consumer.SpinOnce(sleep1Threshold: -1);
            }
        }

        producer.Join();
        return sum;
    }

    [Benchmark(OperationsPerInvoke = Items)]
    public long BoundedChannel()
    {
        ChannelWriter<long> writer = _channel.Writer;
        ChannelReader<long> reader = _channel.Reader;
        Thread producer = Start(() =>
        {
            SpinWait spinner = default;
            for (long i = 0; i < Items; i++)
            {
                while (!writer.TryWrite(i))
                    spinner.SpinOnce(sleep1Threshold: -1);
            }
        });

        long sum = 0;
        SpinWait consumer = default;
        for (int n = 0; n < Items;)
        {
            if (reader.TryRead(out long item))
            {
                sum += item;
                n++;
            }
            else
            {
                consumer.SpinOnce(sleep1Threshold: -1);
            }
        }

        producer.Join();
        return sum;
    }

    internal static Thread Start(ThreadStart body)
    {
        var thread = new Thread(body) { IsBackground = true };
        thread.Start();
        return thread;
    }
}
