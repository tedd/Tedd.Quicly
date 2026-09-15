using System.Numerics;
using System.Runtime.CompilerServices;

namespace Tedd.Quicly.Archive.Replication;

/// <summary>
/// ARCHIVED V1 of the encoders in <c>Tedd.Quicly.Replication.Quantization</c>: the over/underflow fix done by
/// normalising in double precision (the sum / squared length of finite floats cannot overflow or underflow a
/// double). Correct for every finite input, but the double square root and division made the quaternion encoder
/// about 1.75× slower than V0; the shipped V2 prescales by an exact power of two in float instead. Kept for benchmark
/// comparison only; see <c>docs/benchmarks/replication.md</c>.
/// </summary>
public static class QuantizationV1
{
    private const double Sqrt2 = 1.41421356237309505;

    /// <summary>Octahedral unit-vector encoding (see the shipped <c>Quantization.QuantizeUnitVector</c>).</summary>
    public static uint QuantizeUnitVector(Vector3 direction, int bitsPerComponent)
    {
        float x = direction.X, y = direction.Y, z = direction.Z;
        double sum = (double)MathF.Abs(x) + MathF.Abs(y) + MathF.Abs(z);
        if (!(sum > 0.0) || !double.IsFinite(sum))
        {
            x = 0f;
            y = 0f;
            z = 1f;
            sum = 1.0;
        }

        double inv = 1.0 / sum;
        float px = (float)(x * inv);
        float py = (float)(y * inv);
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
        double lengthSquared = ((double)x * x) + ((double)y * y) + ((double)z * z) + ((double)w * w);
        if (!(lengthSquared > 0.0) || !double.IsFinite(lengthSquared))
        {
            x = 0f;
            y = 0f;
            z = 0f;
            w = 1f;
            lengthSquared = 1.0;
        }

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
        largest = b3 ? w : largest;

        double scale = Math.CopySign(Sqrt2, largest) / Math.Sqrt(lengthSquared);
        float a = index == 0 ? y : x;
        float b = index <= 1 ? z : y;
        float c = index <= 2 ? w : z;

        int m = (1 << (bitsPerComponent - 1)) - 1;
        ulong qa = (ulong)(QuantizeSnorm((float)(a * scale), m) + m);
        ulong qb = (ulong)(QuantizeSnorm((float)(b * scale), m) + m);
        ulong qc = (ulong)(QuantizeSnorm((float)(c * scale), m) + m);
        return (ulong)(uint)index | (qa << 2) | (qb << (2 + bitsPerComponent)) | (qc << (2 + 2 * bitsPerComponent));
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
