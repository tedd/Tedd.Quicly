using System.Collections.Concurrent;
using System.Threading.Channels;
using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Archive.Threading;
using Tedd.Quicly.Core.Threading;

namespace Tedd.Quicly.Benchmarks.Threading;

/// <summary>
/// <see cref="Producers"/> threads push a total of <see cref="Items"/> longs while the benchmark thread drains
/// them; reported time is per item. Compares the shipping MpscRing (Vyukov bounded queue) with the archived V0
/// (monitor lock around a plain ring), ConcurrentQueue and a bounded Channel through its synchronous API.
/// </summary>
[Config(typeof(ThreadingBenchmarkConfig))]
public class MpscRingBenchmarks
{
    private const int Items = 1 << 20;
    private const int Capacity = 1024;

    [Params(1, 4)]
    public int Producers { get; set; }

    private MpscRing<long> _ring = null!;
    private MpscRingV0<long> _ringV0 = null!;
    private ConcurrentQueue<long> _queue = null!;
    private Channel<long> _channel = null!;

    [GlobalSetup]
    public void Setup()
    {
        _ring = new MpscRing<long>(Capacity);
        _ringV0 = new MpscRingV0<long>(Capacity);
        _queue = new ConcurrentQueue<long>();
        _channel = Channel.CreateBounded<long>(new BoundedChannelOptions(Capacity) { SingleReader = true, SingleWriter = false });
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = Items)]
    public long MpscRing_V1()
    {
        MpscRing<long> ring = _ring;
        Thread[] producers = StartProducers(i =>
        {
            SpinWait spinner = default;
            while (!ring.TryEnqueue(i))
                spinner.SpinOnce(sleep1Threshold: -1);
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

        Join(producers);
        return sum;
    }

    [Benchmark(OperationsPerInvoke = Items)]
    public long MpscRing_V0_Archive()
    {
        MpscRingV0<long> ring = _ringV0;
        Thread[] producers = StartProducers(i =>
        {
            SpinWait spinner = default;
            while (!ring.TryEnqueue(i))
                spinner.SpinOnce(sleep1Threshold: -1);
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

        Join(producers);
        return sum;
    }

    [Benchmark(OperationsPerInvoke = Items)]
    public long ConcurrentQueue()
    {
        ConcurrentQueue<long> queue = _queue;
        Thread[] producers = StartProducers(queue.Enqueue);

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

        Join(producers);
        return sum;
    }

    [Benchmark(OperationsPerInvoke = Items)]
    public long BoundedChannel()
    {
        ChannelWriter<long> writer = _channel.Writer;
        ChannelReader<long> reader = _channel.Reader;
        Thread[] producers = StartProducers(i =>
        {
            SpinWait spinner = default;
            while (!writer.TryWrite(i))
                spinner.SpinOnce(sleep1Threshold: -1);
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

        Join(producers);
        return sum;
    }

    private Thread[] StartProducers(Action<long> enqueue)
    {
        int producerCount = Producers;
        int perProducer = Items / producerCount;
        var threads = new Thread[producerCount];
        for (int p = 0; p < producerCount; p++)
        {
            long first = (long)p * perProducer;
            threads[p] = SpscRingBenchmarks.Start(() =>
            {
                for (long i = first; i < first + perProducer; i++)
                    enqueue(i);
            });
        }

        return threads;
    }

    private static void Join(Thread[] threads)
    {
        foreach (Thread t in threads)
            t.Join();
    }
}
