using System.Numerics;
using System.Runtime.CompilerServices;

namespace Tedd.Quicly.Archive.Replication;

/// <summary>
/// ARCHIVED V2 of the encoders in <c>Tedd.Quicly.Replication.Quantization</c>: the over/underflow fix done by always
/// prescaling the components by an exact power of two (from the largest component's exponent) before normalising in
/// float. Correct for every finite input, but the prescale on the hot path (<see cref="MathF.Max(float, float)"/> with
/// IEEE NaN semantics, exponent extraction, four extra multiplies) cost more than V1's double math for unit vectors
/// and ~30 % over V0 for quaternions. The shipped V3 runs V0's float path and prescales only outside a safe
/// magnitude range. Kept for benchmark comparison only; see <c>docs/benchmarks/replication.md</c>.
/// </summary>
public static class QuantizationV2
{
    private const float Sqrt2 = 1.41421356237309505f;

    /// <summary>Octahedral unit-vector encoding (see the shipped <c>Quantization.QuantizeUnitVector</c>).</summary>
    public static uint QuantizeUnitVector(Vector3 direction, int bitsPerComponent)
    {
        float s = PowerOfTwoScale(MathF.Max(MathF.Max(MathF.Abs(direction.X), MathF.Abs(direction.Y)), MathF.Abs(direction.Z)));
        float x = direction.X * s, y = direction.Y * s, z = direction.Z * s;
        float sum = MathF.Abs(x) + MathF.Abs(y) + MathF.Abs(z);
        if (!(sum > 0f) || !float.IsFinite(sum))
        {
            x = 0f;
            y = 0f;
            z = 1f;
            sum = 1f;
        }

        float inv = 1f / sum;
        float px = x * inv;
        float py = y * inv;
        float fx = (1f - MathF.Abs(py)) * MathF.CopySign(1f, px);
        float fy = (1f - MathF.Abs(px)) * MathF.CopySign(1f, py);
        bool lower = z < 0f;
        px = lower ? fx : px;
        py = lower ? fy : py;

        int m = (1 << (bitsPerComponent - 1)) - 1;
        uint qx = (uint)(QuantizeSnorm(px, m) + m);
        uint qy = (uint)(QuantizeSnorm(py, m) + m);
        return qx | (qy << bitsPerComponent);
    }

    /// <summary>Smallest-three quaternion encoding (see the shipped <c>Quantization.QuantizeQuaternion</c>).</summary>
    public static ulong QuantizeQuaternion(Quaternion rotation, int bitsPerComponent)
    {
        float x = rotation.X, y = rotation.Y, z = rotation.Z, w = rotation.W;
        float ax = MathF.Abs(x), ay = MathF.Abs(y), az = MathF.Abs(z), aw = MathF.Abs(w);
        int index = 0;
        float largestAbs = ax;
        float largest = x;
        bool b1 = ay > largestAbs;
        index = b1 ? 1 : index;
        largestAbs = b1 ? ay : largestAbs;
        largest = b1 ? y : largest;
        bool b2 = az > largestAbs;
        index = b2 ? 2 : index;
        largestAbs = b2 ? az : largestAbs;
        largest = b2 ? z : largest;
        bool b3 = aw > largestAbs;
        index = b3 ? 3 : index;
        largestAbs = b3 ? aw : largestAbs;
        largest = b3 ? w : largest;

        float s = PowerOfTwoScale(largestAbs);
        x *= s;
        y *= s;
        z *= s;
        w *= s;
        float lengthSquared = x * x + y * y + z * z + w * w;
        if (!(lengthSquared > 0f) || !float.IsFinite(lengthSquared))
        {
            x = 0f;
            y = 0f;
            z = 0f;
            w = 1f;
            index = 3;
            largest = 1f;
            lengthSquared = 1f;
        }

        float scale = MathF.CopySign(Sqrt2, largest) / MathF.Sqrt(lengthSquared);
        float a = index == 0 ? y : x;
        float b = index <= 1 ? z : y;
        float c = index <= 2 ? w : z;

        int m = (1 << (bitsPerComponent - 1)) - 1;
        ulong qa = (ulong)(QuantizeSnorm(a * scale, m) + m);
        ulong qb = (ulong)(QuantizeSnorm(b * scale, m) + m);
        ulong qc = (ulong)(QuantizeSnorm(c * scale, m) + m);
        return (ulong)(uint)index | (qa << 2) | (qb << (2 + bitsPerComponent)) | (qc << (2 + 2 * bitsPerComponent));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static float PowerOfTwoScale(float largestAbs)
    {
        int e = (BitConverter.SingleToInt32Bits(largestAbs) >> 23) & 0xFF;
        int biased = Math.Clamp(254 - e, 1, 254);
        return BitConverter.Int32BitsToSingle(biased << 23);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int QuantizeSnorm(float value, int m)
    {
        float v = value >= -1f ? value : -1f;
        v = v <= 1f ? v : 1f;
        v = float.IsNaN(value) ? 0f : v;
        return (int)MathF.Round(v * m);
    }
}
