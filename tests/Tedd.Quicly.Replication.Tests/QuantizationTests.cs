using System.Numerics;

namespace Tedd.Quicly.Replication.Tests;

/// <summary>Dense sweeps that prove the error bounds documented on <see cref="Quantization"/>.</summary>
public class QuantizationTests
{
    private const double UnitVectorBoundFactor = 2.25;
    private const double QuaternionBoundFactor = 2.5;
    private const double FloatRounding = 1e-6;

    /// <summary>Angle between two directions, computed in double from the chord (accurate for tiny angles).</summary>
    private static double Angle(Vector3 a, Vector3 b)
    {
        double ax = a.X, ay = a.Y, az = a.Z, bx = b.X, by = b.Y, bz = b.Z;
        double la = Math.Sqrt(ax * ax + ay * ay + az * az);
        double lb = Math.Sqrt(bx * bx + by * by + bz * bz);
        double dx = ax / la - bx / lb, dy = ay / la - by / lb, dz = az / la - bz / lb;
        return 2 * Math.Asin(Math.Min(1, Math.Sqrt(dx * dx + dy * dy + dz * dz) / 2));
    }

    /// <summary>Rotation angle between two rotations (q and −q are the same rotation), from the 4-D chord.</summary>
    private static double RotationAngle(Quaternion a, Quaternion b)
    {
        double[] p = [a.X, a.Y, a.Z, a.W];
        double[] q = [b.X, b.Y, b.Z, b.W];
        double lp = 0, lq = 0, dot = 0;
        for (int i = 0; i < 4; i++)
        {
            lp += p[i] * p[i];
            lq += q[i] * q[i];
        }

        lp = Math.Sqrt(lp);
        lq = Math.Sqrt(lq);
        for (int i = 0; i < 4; i++)
        {
            dot += p[i] / lp * (q[i] / lq);
        }

        double sign = dot < 0 ? -1 : 1;
        double chord = 0;
        for (int i = 0; i < 4; i++)
        {
            double d = p[i] / lp - sign * q[i] / lq;
            chord += d * d;
        }

        return 4 * Math.Asin(Math.Min(1, Math.Sqrt(chord) / 2));
    }

    private static Quaternion RandomRotation(Random random)
    {
        // Uniform over SO(3) (Shoemake).
        double u1 = random.NextDouble(), u2 = random.NextDouble(), u3 = random.NextDouble();
        return new Quaternion(
            (float)(Math.Sqrt(1 - u1) * Math.Sin(2 * Math.PI * u2)),
            (float)(Math.Sqrt(1 - u1) * Math.Cos(2 * Math.PI * u2)),
            (float)(Math.Sqrt(u1) * Math.Sin(2 * Math.PI * u3)),
            (float)(Math.Sqrt(u1) * Math.Cos(2 * Math.PI * u3)));
    }

    private static IEnumerable<Vector3> Directions(Random random)
    {
        const int Sphere = 100_000;
        double golden = Math.PI * (3 - Math.Sqrt(5));
        for (int i = 0; i < Sphere; i++)
        {
            double y = 1 - (i + 0.5) / Sphere * 2;
            double r = Math.Sqrt(1 - y * y);
            yield return new Vector3((float)(Math.Cos(golden * i) * r), (float)y, (float)(Math.Sin(golden * i) * r));
        }

        for (int i = 0; i < 30_000; i++)
        {
            float Next() => (float)(random.NextDouble() * 2 - 1);
            yield return new Vector3(Next(), Next(), Next());
            yield return new Vector3(Next(), Next(), Next() * 1e-3f);                // the fold plane z ≈ 0
            float t = Next();
            yield return new Vector3(t, t * (1 + (float)random.NextDouble() * 1e-3f), Next());   // diagonals |x| ≈ |y|
            yield return new Vector3(Next() * 1e-4f, Next(), Next()) * 57f;           // near x = 0, not normalized
        }
    }

    private static double Ulp(float value)
    {
        float magnitude = MathF.Abs(value);
        return MathF.BitIncrement(magnitude) - magnitude;
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(10)]
    [InlineData(12)]
    [InlineData(16)]
    [InlineData(20)]
    [InlineData(24)]
    [InlineData(31)]
    [InlineData(32)]
    public void Float_Error_Is_At_Most_Half_A_Step(int bits)
    {
        (float Min, float Max)[] ranges = [(0f, 1f), (-100f, 100f), (-1e-3f, 5e-4f), (1000f, 1000.5f), (-1e6f, 1e6f)];
        Random random = new(bits);
        uint maxQ = (uint)((1UL << bits) - 1);
        foreach ((float min, float max) in ranges)
        {
            double range = (double)max - min;
            double step = range / maxQ;
            Assert.Equal(0u, Quantization.QuantizeFloat(min, min, max, bits));
            Assert.Equal(maxQ, Quantization.QuantizeFloat(max, min, max, bits));
            Assert.Equal(min, Quantization.DequantizeFloat(0, min, max, bits));
            Assert.Equal(max, Quantization.DequantizeFloat(maxQ, min, max, bits));
            Assert.Equal(0u, Quantization.QuantizeFloat(min - (float)range, min, max, bits));
            Assert.Equal(maxQ, Quantization.QuantizeFloat(max + (float)range, min, max, bits));
            Assert.Equal(0u, Quantization.QuantizeFloat(float.NaN, min, max, bits));
            Assert.Equal(0u, Quantization.QuantizeFloat(float.NegativeInfinity, min, max, bits));
            Assert.Equal(maxQ, Quantization.QuantizeFloat(float.PositiveInfinity, min, max, bits));
            if (bits < 32)
            {
                Assert.Equal(max, Quantization.DequantizeFloat(maxQ + 1, min, max, bits));
                Assert.Equal(max, Quantization.DequantizeFloat(uint.MaxValue, min, max, bits));
            }

            uint previous = 0;
            const int Steps = 20_000;
            for (int i = 0; i <= Steps + 2000; i++)
            {
                double position = i <= Steps ? (double)i / Steps * 1.2 - 0.1 : random.NextDouble();
                float value = (float)(min + position * range);
                uint q = Quantization.QuantizeFloat(value, min, max, bits);
                Assert.True(q <= maxQ);
                if (i <= Steps)
                {
                    Assert.True(q >= previous, "quantization must be monotonic");
                    previous = q;
                }

                float clamped = Math.Clamp(value, min, max);
                float back = Quantization.DequantizeFloat(q, min, max, bits);
                double error = Math.Abs((double)back - clamped);
                Assert.True(error <= step / 2 + Ulp(back) + 1e-12 * range, $"bits={bits} [{min},{max}] value={value} back={back} error={error} step={step}");
            }
        }
    }

    [Fact]
    public void Float_Arguments_Are_Validated()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => Quantization.QuantizeFloat(0, 0, 1, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => Quantization.QuantizeFloat(0, 0, 1, 33));
        Assert.Throws<ArgumentOutOfRangeException>(() => Quantization.DequantizeFloat(0, 0, 1, 0));
        Assert.Throws<ArgumentException>(() => Quantization.QuantizeFloat(0, 1, 1, 8));
        Assert.Throws<ArgumentException>(() => Quantization.QuantizeFloat(0, 2, 1, 8));
        Assert.Throws<ArgumentException>(() => Quantization.QuantizeFloat(0, float.NaN, 1, 8));
        Assert.Throws<ArgumentException>(() => Quantization.QuantizeFloat(0, 0, float.PositiveInfinity, 8));
        Assert.Throws<ArgumentException>(() => Quantization.DequantizeFloat(0, float.NegativeInfinity, 0, 8));
    }

    public static TheoryData<int> UnitVectorBits() => [.. Enumerable.Range(Quantization.MinUnitVectorBits, Quantization.MaxUnitVectorBits - 1)];

    [Theory]
    [MemberData(nameof(UnitVectorBits))]
    public void UnitVector_Error_Is_Bounded(int bits)
    {
        int m = (1 << (bits - 1)) - 1;
        double bound = UnitVectorBoundFactor / m + FloatRounding;
        double worst = 0;
        foreach (Vector3 direction in Directions(new Random(bits)))
        {
            uint packed = Quantization.QuantizeUnitVector(direction, bits);
            if (bits < 16)
            {
                Assert.Equal(0u, packed >> (2 * bits));
            }

            Vector3 back = Quantization.DequantizeUnitVector(packed, bits);
            Assert.Equal(1f, back.Length(), 5);
            worst = Math.Max(worst, Angle(direction, back));
        }

        Assert.True(worst <= bound, $"bits={bits}: worst {worst} rad > bound {bound} rad");
    }

    [Fact]
    public void UnitVector_Axes_Are_Exact_And_Degenerate_Inputs_Are_Safe()
    {
        Vector3[] axes = [Vector3.UnitX, -Vector3.UnitX, Vector3.UnitY, -Vector3.UnitY, Vector3.UnitZ, -Vector3.UnitZ, new(-0f, 0f, -1f), new(0f, -0f, -1f)];
        for (int bits = Quantization.MinUnitVectorBits; bits <= Quantization.MaxUnitVectorBits; bits++)
        {
            foreach (Vector3 axis in axes)
            {
                Vector3 back = Quantization.DequantizeUnitVector(Quantization.QuantizeUnitVector(axis, bits), bits);
                Assert.Equal(axis.X, back.X);
                Assert.Equal(axis.Y, back.Y);
                Assert.Equal(axis.Z, back.Z);
            }

            foreach (Vector3 degenerate in new[] { Vector3.Zero, new Vector3(float.NaN, 0, 0), new Vector3(float.PositiveInfinity, 1, 0) })
            {
                Assert.Equal(Vector3.UnitZ, Quantization.DequantizeUnitVector(Quantization.QuantizeUnitVector(degenerate, bits), bits));
            }

            // Every code decodes to a unit vector, including the unused top code; bits above the encoding are ignored.
            uint all = bits == 16 ? uint.MaxValue : (1u << (2 * bits)) - 1;
            Assert.Equal(1f, Quantization.DequantizeUnitVector(all, bits).Length(), 5);
            if (bits < 16)
            {
                uint packed = Quantization.QuantizeUnitVector(new Vector3(0.3f, 0.4f, -0.5f), bits);
                Assert.Equal(Quantization.DequantizeUnitVector(packed, bits), Quantization.DequantizeUnitVector(packed | (1u << (2 * bits)), bits));
            }
        }

        Assert.Throws<ArgumentOutOfRangeException>(() => Quantization.QuantizeUnitVector(Vector3.UnitX, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => Quantization.QuantizeUnitVector(Vector3.UnitX, 17));
        Assert.Throws<ArgumentOutOfRangeException>(() => Quantization.DequantizeUnitVector(0, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => Quantization.DequantizeUnitVector(0, 17));
    }

    public static TheoryData<int> QuaternionBits() => [.. Enumerable.Range(Quantization.MinQuaternionBits, Quantization.MaxQuaternionBits - 1)];

    [Theory]
    [MemberData(nameof(QuaternionBits))]
    public void Quaternion_Error_Is_Bounded(int bits)
    {
        int m = (1 << (bits - 1)) - 1;
        double bound = QuaternionBoundFactor / m + FloatRounding;
        Random random = new(bits);
        double worst = 0;
        for (int i = 0; i < 100_000; i++)
        {
            Quaternion rotation = RandomRotation(random);
            if (i % 7 == 0)
            {
                rotation *= 3.5f;   // not normalized
            }

            ulong packed = Quantization.QuantizeQuaternion(rotation, bits);
            Assert.Equal(0UL, packed >> (2 + 3 * bits));
            Assert.Equal(packed, Quantization.QuantizeQuaternion(Quaternion.Negate(rotation), bits));

            Quaternion back = Quantization.DequantizeQuaternion(packed, bits);
            Assert.Equal(1f, back.Length(), 5);
            float[] components = [back.X, back.Y, back.Z, back.W];
            Assert.True(components[(int)(packed & 3)] >= 0f);
            worst = Math.Max(worst, RotationAngle(rotation, back));
        }

        if (bits >= 4)
        {
            Assert.True(worst <= bound, $"bits={bits}: worst {worst} rad > bound {bound} rad");
        }
        else
        {
            Assert.True(worst <= Math.PI);
        }
    }

    [Fact]
    public void Quaternion_Special_Cases()
    {
        Quaternion[] exact = [Quaternion.Identity, new(1, 0, 0, 0), new(0, 1, 0, 0), new(0, 0, 1, 0), new(0, 0, 0, -1)];
        for (int bits = Quantization.MinQuaternionBits; bits <= Quantization.MaxQuaternionBits; bits++)
        {
            int m = (1 << (bits - 1)) - 1;
            ulong identity = Quantization.QuantizeQuaternion(Quaternion.Identity, bits);
            Assert.Equal(3UL | ((ulong)m << 2) | ((ulong)m << (2 + bits)) | ((ulong)m << (2 + 2 * bits)), identity);
            foreach (Quaternion q in exact)
            {
                Quaternion back = Quantization.DequantizeQuaternion(Quantization.QuantizeQuaternion(q, bits), bits);
                Assert.Equal(0, RotationAngle(q, back));
            }

            foreach (Quaternion degenerate in new[] { default, new Quaternion(float.NaN, 0, 0, 1), new Quaternion(float.PositiveInfinity, 0, 0, 0) })
            {
                Assert.Equal(Quaternion.Identity, Quantization.DequantizeQuaternion(Quantization.QuantizeQuaternion(degenerate, bits), bits));
            }

            // Four equal components: the first wins the tie.
            ulong tie = Quantization.QuantizeQuaternion(new Quaternion(0.5f, 0.5f, 0.5f, 0.5f), bits);
            Assert.Equal(0UL, tie & 3);

            // Every code decodes to a unit quaternion; bits above the encoding are ignored.
            ulong all = (1UL << (2 + 3 * bits)) - 1;
            Assert.Equal(1f, Quantization.DequantizeQuaternion(all, bits).Length(), 5);
            if (2 + 3 * bits < 64)
            {
                Assert.Equal(Quantization.DequantizeQuaternion(identity, bits), Quantization.DequantizeQuaternion(identity | (1UL << (2 + 3 * bits)), bits));
            }
        }

        Assert.Throws<ArgumentOutOfRangeException>(() => Quantization.QuantizeQuaternion(Quaternion.Identity, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => Quantization.QuantizeQuaternion(Quaternion.Identity, 21));
        Assert.Throws<ArgumentOutOfRangeException>(() => Quantization.DequantizeQuaternion(0, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => Quantization.DequantizeQuaternion(0, 21));
    }
}
