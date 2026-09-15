using System.Numerics;
using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Archive.Replication;
using Tedd.Quicly.Replication;

namespace Tedd.Quicly.Benchmarks.Replication;

/// <summary>
/// Quantization throughput over 256 values per invocation: smallest-three quaternions (10 bits per component, 32 bits
/// total), octahedral unit vectors (12 bits per component) and range-quantized floats (16 bits).
/// </summary>
[Config(typeof(InProcessShortRunConfig))]
public class QuantizationBench
{
    private const int Count = 256;
    private readonly Quaternion[] _rotations = new Quaternion[Count];
    private readonly Vector3[] _directions = new Vector3[Count];
    private readonly float[] _floats = new float[Count];
    private readonly ulong[] _packedRotations = new ulong[Count];
    private readonly uint[] _packedDirections = new uint[Count];

    [GlobalSetup]
    public void Setup()
    {
        Random random = new(9);
        for (int i = 0; i < Count; i++)
        {
            _rotations[i] = Quaternion.Normalize(new Quaternion(
                (float)random.NextDouble() - 0.5f, (float)random.NextDouble() - 0.5f, (float)random.NextDouble() - 0.5f, (float)random.NextDouble() - 0.5f));
            _directions[i] = Vector3.Normalize(new Vector3((float)random.NextDouble() - 0.5f, (float)random.NextDouble() - 0.5f, (float)random.NextDouble() - 0.5f));
            _floats[i] = (float)(random.NextDouble() * 2000 - 1000);
            _packedRotations[i] = Quantization.QuantizeQuaternion(_rotations[i], 10);
            _packedDirections[i] = Quantization.QuantizeUnitVector(_directions[i], 12);
        }
    }

    [Benchmark]
    public ulong QuantizeQuaternion()
    {
        ulong x = 0;
        for (int i = 0; i < Count; i++)
        {
            x ^= Quantization.QuantizeQuaternion(_rotations[i], 10);
        }

        return x;
    }

    [Benchmark]
    public ulong QuantizeQuaternion_V0()
    {
        ulong x = 0;
        for (int i = 0; i < Count; i++)
        {
            x ^= QuantizationV0.QuantizeQuaternion(_rotations[i], 10);
        }

        return x;
    }

    [Benchmark]
    public ulong QuantizeQuaternion_V1()
    {
        ulong x = 0;
        for (int i = 0; i < Count; i++)
        {
            x ^= QuantizationV1.QuantizeQuaternion(_rotations[i], 10);
        }

        return x;
    }

    [Benchmark]
    public ulong QuantizeQuaternion_V2()
    {
        ulong x = 0;
        for (int i = 0; i < Count; i++)
        {
            x ^= QuantizationV2.QuantizeQuaternion(_rotations[i], 10);
        }

        return x;
    }

    [Benchmark]
    public float DequantizeQuaternion()
    {
        float x = 0;
        for (int i = 0; i < Count; i++)
        {
            x += Quantization.DequantizeQuaternion(_packedRotations[i], 10).W;
        }

        return x;
    }

    [Benchmark]
    public uint QuantizeUnitVector()
    {
        uint x = 0;
        for (int i = 0; i < Count; i++)
        {
            x ^= Quantization.QuantizeUnitVector(_directions[i], 12);
        }

        return x;
    }

    [Benchmark]
    public uint QuantizeUnitVector_V0()
    {
        uint x = 0;
        for (int i = 0; i < Count; i++)
        {
            x ^= QuantizationV0.QuantizeUnitVector(_directions[i], 12);
        }

        return x;
    }

    [Benchmark]
    public uint QuantizeUnitVector_V1()
    {
        uint x = 0;
        for (int i = 0; i < Count; i++)
        {
            x ^= QuantizationV1.QuantizeUnitVector(_directions[i], 12);
        }

        return x;
    }

    [Benchmark]
    public uint QuantizeUnitVector_V2()
    {
        uint x = 0;
        for (int i = 0; i < Count; i++)
        {
            x ^= QuantizationV2.QuantizeUnitVector(_directions[i], 12);
        }

        return x;
    }

    [Benchmark]
    public float DequantizeUnitVector()
    {
        float x = 0;
        for (int i = 0; i < Count; i++)
        {
            x += Quantization.DequantizeUnitVector(_packedDirections[i], 12).Z;
        }

        return x;
    }

    [Benchmark]
    public uint QuantizeFloat()
    {
        uint x = 0;
        for (int i = 0; i < Count; i++)
        {
            x ^= Quantization.QuantizeFloat(_floats[i], -1000f, 1000f, 16);
        }

        return x;
    }
}
