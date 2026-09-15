using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Archive.Primitives;
using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Benchmarks.Primitives;

/// <summary>
/// VarInt write/read, 256 values per invocation. <c>Set</c> selects values of one encoded length or a
/// pseudo-random mix of all four lengths (the mix defeats branch prediction on the length selection).
/// </summary>
[Config(typeof(InProcessShortRunConfig))]
public class VarIntBench
{
    private const int Count = 256;

    private readonly byte[] _writeBuffer = new byte[16];
    private ulong[] _values = Array.Empty<ulong>();
    private byte[] _encoded = Array.Empty<byte>();

    [Params("1", "2", "4", "8", "mixed")]
    public string Set { get; set; } = "1";

    [GlobalSetup]
    public void Setup()
    {
        Random random = new(17);
        _values = new ulong[Count];
        for (int i = 0; i < Count; i++)
        {
            int length = Set switch
            {
                "1" => 1,
                "2" => 2,
                "4" => 4,
                "8" => 8,
                _ => 1 << random.Next(0, 4),
            };
            _values[i] = length switch
            {
                1 => (ulong)random.Next(0, 64),
                2 => (ulong)random.Next(64, 16384),
                4 => (ulong)random.Next(16384, 1 << 30),
                _ => (ulong)random.NextInt64(1L << 30, (long)VarInt.MaxValue),
            };
        }

        _encoded = new byte[Count * 8];
        int offset = 0;
        for (int i = 0; i < Count; i++)
        {
            offset += VarInt.Write(_encoded.AsSpan(offset), _values[i]);
        }

        _encoded = _encoded.AsSpan(0, offset).ToArray();
    }

    [Benchmark(Baseline = true)]
    public int Write_V0()
    {
        int total = 0;
        ulong[] values = _values;
        Span<byte> buffer = _writeBuffer;
        for (int i = 0; i < values.Length; i++)
        {
            VarIntV0.TryWrite(buffer, values[i], out int written);
            total += written;
        }

        return total;
    }

    [Benchmark]
    public int Write_V1()
    {
        int total = 0;
        ulong[] values = _values;
        Span<byte> buffer = _writeBuffer;
        for (int i = 0; i < values.Length; i++)
        {
            VarIntV1.TryWrite(buffer, values[i], out int written);
            total += written;
        }

        return total;
    }

    [Benchmark]
    public int Write_Current()
    {
        int total = 0;
        ulong[] values = _values;
        Span<byte> buffer = _writeBuffer;
        for (int i = 0; i < values.Length; i++)
        {
            VarInt.TryWrite(buffer, values[i], out int written);
            total += written;
        }

        return total;
    }

    [Benchmark]
    public int GetLength_V1()
    {
        int total = 0;
        ulong[] values = _values;
        for (int i = 0; i < values.Length; i++)
        {
            total += VarIntV1.GetLength(values[i]);
        }

        return total;
    }

    [Benchmark]
    public ulong Read_V1()
    {
        ulong sum = 0;
        ReadOnlySpan<byte> encoded = _encoded;
        int offset = 0;
        while (offset < encoded.Length)
        {
            VarIntV1.TryRead(encoded.Slice(offset), out ulong value, out int consumed);
            sum += value;
            offset += consumed;
        }

        return sum;
    }

    [Benchmark]
    public unsafe ulong ReadPointer_V1()
    {
        ulong sum = 0;
        fixed (byte* p = _encoded)
        {
            int available = _encoded.Length;
            byte* cursor = p;
            while (available > 0)
            {
                VarIntV1.TryRead(cursor, available, out ulong value, out int consumed);
                sum += value;
                cursor += consumed;
                available -= consumed;
            }
        }

        return sum;
    }

    [Benchmark]
    public int GetLength_V0()
    {
        int total = 0;
        ulong[] values = _values;
        for (int i = 0; i < values.Length; i++)
        {
            total += VarIntV0.GetLength(values[i]);
        }

        return total;
    }

    [Benchmark]
    public int GetLength_Current()
    {
        int total = 0;
        ulong[] values = _values;
        for (int i = 0; i < values.Length; i++)
        {
            total += VarInt.GetLength(values[i]);
        }

        return total;
    }

    [Benchmark]
    public ulong Read_V0()
    {
        ulong sum = 0;
        ReadOnlySpan<byte> encoded = _encoded;
        int offset = 0;
        while (offset < encoded.Length)
        {
            VarIntV0.TryRead(encoded.Slice(offset), out ulong value, out int consumed);
            sum += value;
            offset += consumed;
        }

        return sum;
    }

    [Benchmark]
    public ulong Read_Current()
    {
        ulong sum = 0;
        ReadOnlySpan<byte> encoded = _encoded;
        int offset = 0;
        while (offset < encoded.Length)
        {
            VarInt.TryRead(encoded.Slice(offset), out ulong value, out int consumed);
            sum += value;
            offset += consumed;
        }

        return sum;
    }

    [Benchmark]
    public ulong ReadMinimal_Current()
    {
        ulong sum = 0;
        ReadOnlySpan<byte> encoded = _encoded;
        int offset = 0;
        while (offset < encoded.Length)
        {
            VarInt.TryReadMinimal(encoded.Slice(offset), out ulong value, out int consumed);
            sum += value;
            offset += consumed;
        }

        return sum;
    }

    [Benchmark]
    public unsafe ulong ReadPointer_V0()
    {
        ulong sum = 0;
        fixed (byte* p = _encoded)
        {
            int available = _encoded.Length;
            byte* cursor = p;
            while (available > 0)
            {
                VarIntV0.TryRead(cursor, available, out ulong value, out int consumed);
                sum += value;
                cursor += consumed;
                available -= consumed;
            }
        }

        return sum;
    }

    [Benchmark]
    public unsafe ulong ReadPointer_Current()
    {
        ulong sum = 0;
        fixed (byte* p = _encoded)
        {
            int available = _encoded.Length;
            byte* cursor = p;
            while (available > 0)
            {
                VarInt.TryRead(cursor, available, out ulong value, out int consumed);
                sum += value;
                cursor += consumed;
                available -= consumed;
            }
        }

        return sum;
    }
}
