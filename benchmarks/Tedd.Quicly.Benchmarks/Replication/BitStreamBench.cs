using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Replication;

namespace Tedd.Quicly.Benchmarks.Replication;

/// <summary>
/// BitWriter / BitReader throughput: 256 fields of pseudo-random widths (1..32 bits, the shape of a quantized entity
/// record) per invocation, plus 256 ZigZag bit-varints.
/// </summary>
[Config(typeof(InProcessShortRunConfig))]
public class BitStreamBench
{
    private const int Fields = 256;
    private readonly byte[] _buffer = new byte[4096];
    private readonly ulong[] _values = new ulong[Fields];
    private readonly int[] _bits = new int[Fields];
    private readonly long[] _signed = new long[Fields];
    private int _packedLength;
    private int _varLength;
    private readonly byte[] _varBuffer = new byte[4096];

    [GlobalSetup]
    public void Setup()
    {
        Random random = new(7);
        for (int i = 0; i < Fields; i++)
        {
            _bits[i] = random.Next(1, 33);
            _values[i] = (ulong)random.NextInt64() & ((1UL << _bits[i]) - 1);
            _signed[i] = random.Next(-5000, 5000);
        }

        _packedLength = WriteFields();
        BitWriter writer = new(_varBuffer);
        for (int i = 0; i < Fields; i++)
        {
            writer.WriteVarInt(_signed[i]);
        }

        _varLength = writer.Flush();
    }

    [Benchmark]
    public int WriteFields()
    {
        BitWriter writer = new(_buffer);
        for (int i = 0; i < Fields; i++)
        {
            writer.WriteBits(_values[i], _bits[i]);
        }

        return writer.Flush();
    }

    [Benchmark]
    public ulong ReadFields()
    {
        BitReader reader = new(_buffer.AsSpan(0, _packedLength));
        ulong sum = 0;
        for (int i = 0; i < Fields; i++)
        {
            sum += reader.ReadBits(_bits[i]);
        }

        return sum;
    }

    [Benchmark]
    public int WriteVarInts()
    {
        BitWriter writer = new(_buffer);
        for (int i = 0; i < Fields; i++)
        {
            writer.WriteVarInt(_signed[i]);
        }

        return writer.Flush();
    }

    [Benchmark]
    public long ReadVarInts()
    {
        BitReader reader = new(_varBuffer.AsSpan(0, _varLength));
        long sum = 0;
        for (int i = 0; i < Fields; i++)
        {
            sum += reader.ReadVarInt();
        }

        return sum;
    }
}
