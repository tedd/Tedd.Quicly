using System.Numerics;
using System.Runtime.CompilerServices;

namespace Tedd.Quicly.Replication;

/// <summary>
/// Lossy fixed-point encodings for replicated floats, unit vectors (octahedral) and rotations (smallest-three
/// quaternions). Every function is allocation-free and branch-light; out-of-range and non-finite inputs are
/// clamped or replaced (documented per method) and never throw. Only invalid <em>bit counts</em> or ranges — caller
/// errors — throw.
/// </summary>
/// <remarks>
/// The documented error bounds are proven by dense sweeps in the unit tests
/// (<c>tests/Tedd.Quicly.Replication.Tests/QuantizationTests.cs</c>).
/// </remarks>
public static class Quantization
{
    /// <summary>Largest bit count accepted by <see cref="QuantizeFloat"/>.</summary>
    public const int MaxFloatBits = 32;

    /// <summary>Smallest bits-per-component accepted by the unit-vector encoding.</summary>
    public const int MinUnitVectorBits = 2;

    /// <summary>Largest bits-per-component accepted by the unit-vector encoding (2 × 16 = 32 bits total).</summary>
    public const int MaxUnitVectorBits = 16;

    /// <summary>Smallest bits-per-component accepted by the quaternion encoding.</summary>
    public const int MinQuaternionBits = 2;

    /// <summary>Largest bits-per-component accepted by the quaternion encoding (2 + 3 × 20 = 62 bits total).</summary>
    public const int MaxQuaternionBits = 20;

    private const float Sqrt2 = 1.41421356237309505f;
    private const float InvSqrt2 = 0.70710678118654752f;

    // Inputs whose |x| + |y| + |z| (unit vector) or squared length (quaternion) lies in this range are normalised on the
    // plain float path; anything else (tiny, huge, zero or non-finite) is prescaled by a power of two first.
    private const float SafeMin = 1e-30f;
    private const float SafeMax = 1e30f;

    /// <summary>
    /// Maps <paramref name="value"/> in [<paramref name="min"/>, <paramref name="max"/>] to an integer in
    /// [0, 2^<paramref name="bits"/> − 1] by rounding to the nearest step.
    /// </summary>
    /// <param name="value">The value; clamped to the range, NaN maps to <paramref name="min"/>.</param>
    /// <param name="min">Lower bound of the range (finite).</param>
    /// <param name="max">Upper bound of the range (finite, greater than <paramref name="min"/>).</param>
    /// <param name="bits">1..32.</param>
    /// <returns>The quantized value.</returns>
    /// <remarks>
    /// <see cref="DequantizeFloat"/> of the result differs from the clamped input by at most
    /// <c>(max − min) / (2 · (2^bits − 1))</c> plus one float ULP of the result (the final rounding to
    /// <see cref="float"/>). <paramref name="min"/> and <paramref name="max"/> themselves round-trip exactly.
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="bits"/> is outside 1..32.</exception>
    /// <exception cref="ArgumentException">The range is empty or not finite.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint QuantizeFloat(float value, float min, float max, int bits)
    {
        ValidateFloat(min, max, bits);
        double maxQ = (1UL << bits) - 1;
        double v = value;
        v = v >= min ? v : min;
        v = v <= max ? v : max;
        return (uint)((v - min) * (maxQ / ((double)max - min)) + 0.5);
    }

    /// <summary>Inverse of <see cref="QuantizeFloat"/>. Values above 2^<paramref name="bits"/> − 1 are clamped to <paramref name="max"/>.</summary>
    /// <param name="quantized">The quantized value.</param>
    /// <param name="min">Lower bound of the range used when quantizing.</param>
    /// <param name="max">Upper bound of the range used when quantizing.</param>
    /// <param name="bits">1..32, as used when quantizing.</param>
    /// <returns>The reconstructed value in [<paramref name="min"/>, <paramref name="max"/>].</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="bits"/> is outside 1..32.</exception>
    /// <exception cref="ArgumentException">The range is empty or not finite.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static float DequantizeFloat(uint quantized, float min, float max, int bits)
    {
        ValidateFloat(min, max, bits);
        ulong maxQ = (1UL << bits) - 1;
        double q = Math.Min(quantized, maxQ);
        return (float)(min + q * (((double)max - min) / maxQ));
    }

    /// <summary>
    /// Encodes a direction with the octahedral mapping: the vector is projected onto the octahedron
    /// |x| + |y| + |z| = 1, the lower hemisphere is folded over the diagonals, and the two resulting coordinates
    /// in [−1, 1] are stored as symmetric fixed point (−1, 0 and +1 are exact) with
    /// <paramref name="bitsPerComponent"/> bits each: x in the low bits, y above it.
    /// </summary>
    /// <param name="direction">
    /// The direction; it need not be normalized. Every finite non-zero vector keeps its direction, whatever its
    /// magnitude (a vector outside a safe magnitude range is first scaled by an exact power of two, so no finite
    /// input overflows or underflows). Zero or non-finite vectors encode +Z.
    /// </param>
    /// <param name="bitsPerComponent">2..16 (the result uses 2 × bitsPerComponent bits).</param>
    /// <returns>The packed encoding.</returns>
    /// <remarks>
    /// Maximum angular error after <see cref="DequantizeUnitVector"/>, with M = 2^(bits−1) − 1:
    /// <c>≤ 2.25 / M</c> radians for every supported bit count (measured maximum 2.12 / M; for example 8 bits: 1.0°,
    /// 12 bits: 0.063°, 16 bits: 0.0039°). The axes ±X, ±Y, ±Z round-trip exactly.
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="bitsPerComponent"/> is outside 2..16.</exception>
    public static uint QuantizeUnitVector(Vector3 direction, int bitsPerComponent)
    {
        ValidateRange(bitsPerComponent, MinUnitVectorBits, MaxUnitVectorBits);
        float x = direction.X, y = direction.Y, z = direction.Z;
        float sum = MathF.Abs(x) + MathF.Abs(y) + MathF.Abs(z);
        if (!(sum >= SafeMin && sum <= SafeMax))
        {
            // Rare path — tiny, huge, zero or non-finite input: prescale by an exact power of two so the largest
            // component lands in [2^-22, 4), where the sum and its reciprocal can neither overflow nor underflow. Zero,
            // NaN and infinities still give a zero, NaN or infinite sum and are the only degenerate inputs.
            float s = PowerOfTwoScale(MathF.Max(MathF.Max(MathF.Abs(x), MathF.Abs(y)), MathF.Abs(z)));
            x *= s;
            y *= s;
            z *= s;
            sum = MathF.Abs(x) + MathF.Abs(y) + MathF.Abs(z);
            if (!(sum > 0f) || !float.IsFinite(sum))
            {
                x = 0f;
                y = 0f;
                z = 1f;
                sum = 1f;
            }
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

    /// <summary>Decodes an encoding produced by <see cref="QuantizeUnitVector"/>. Always returns a unit vector; bits above 2 × <paramref name="bitsPerComponent"/> are ignored.</summary>
    /// <param name="packed">The packed encoding.</param>
    /// <param name="bitsPerComponent">2..16, as used when encoding.</param>
    /// <returns>The reconstructed unit vector.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="bitsPerComponent"/> is outside 2..16.</exception>
    public static Vector3 DequantizeUnitVector(uint packed, int bitsPerComponent)
    {
        ValidateRange(bitsPerComponent, MinUnitVectorBits, MaxUnitVectorBits);
        int m = (1 << (bitsPerComponent - 1)) - 1;
        uint mask = (1u << bitsPerComponent) - 1;

        // Divide (rather than multiply by 1/m) so that ±m and 0 decode to exactly ±1 and 0: the axes round-trip exactly.
        int cx = (int)(packed & mask) - m;
        int cy = (int)((packed >> bitsPerComponent) & mask) - m;
        float px = (cx <= m ? cx : m) / (float)m;
        float py = (cy <= m ? cy : m) / (float)m;
        float z = 1f - MathF.Abs(px) - MathF.Abs(py);
        float t = MathF.Max(-z, 0f);
        float x = px - MathF.CopySign(t, px);
        float y = py - MathF.CopySign(t, py);
        float invLength = 1f / MathF.Sqrt(x * x + y * y + z * z);
        return new Vector3(x * invLength, y * invLength, z * invLength);
    }

    /// <summary>
    /// Encodes a rotation with the smallest-three scheme: the component with the largest magnitude is dropped
    /// (its index is stored in 2 bits and its sign is made positive by negating the quaternion, which represents
    /// the same rotation) and the other three, each within ±1/√2, are stored as symmetric fixed point with
    /// <paramref name="bitsPerComponent"/> bits. Layout from the least significant bit: index (2 bits), then the
    /// three remaining components in x, y, z, w order.
    /// </summary>
    /// <param name="rotation">
    /// The rotation; it is normalized first (a quaternion outside a safe magnitude range is first scaled by an exact
    /// power of two, so every finite non-zero quaternion keeps its rotation whatever its magnitude). Zero or
    /// non-finite quaternions encode identity.
    /// </param>
    /// <param name="bitsPerComponent">2..20 (the result uses 2 + 3 × bitsPerComponent bits).</param>
    /// <returns>The packed encoding.</returns>
    /// <remarks>
    /// Each stored component has error ≤ 1 / (2√2 · M) with M = 2^(bits−1) − 1. The rotation-angle error after
    /// <see cref="DequantizeQuaternion"/> (2 · acos |q · q′|) is <c>≤ 2.5 / M</c> radians for bits ≥ 4, plus about
    /// 10^-6 rad of float rounding (measured maximum 2.36 / M; for example 9 bits = 29 total: 0.56°,
    /// 10 bits = 32 total: 0.28°, 16 bits: 0.0044°). Identity and the 180° axis rotations round-trip exactly.
    /// </remarks>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="bitsPerComponent"/> is outside 2..20.</exception>
    public static ulong QuantizeQuaternion(Quaternion rotation, int bitsPerComponent)
    {
        ValidateRange(bitsPerComponent, MinQuaternionBits, MaxQuaternionBits);
        float x = rotation.X, y = rotation.Y, z = rotation.Z, w = rotation.W;
        float lengthSquared = x * x + y * y + z * z + w * w;
        if (!(lengthSquared >= SafeMin && lengthSquared <= SafeMax))
        {
            // Rare path — tiny, huge, zero or non-finite input: prescale by an exact power of two so the largest
            // component lands in [2^-22, 4), where the squared length can neither overflow nor underflow. Zero, NaN
            // and infinities still give a zero, NaN or infinite length and are the only degenerate inputs.
            float s = PowerOfTwoScale(MathF.Max(MathF.Max(MathF.Abs(x), MathF.Abs(y)), MathF.Max(MathF.Abs(z), MathF.Abs(w))));
            x *= s;
            y *= s;
            z *= s;
            w *= s;
            lengthSquared = x * x + y * y + z * z + w * w;
            if (!(lengthSquared > 0f) || !float.IsFinite(lengthSquared))
            {
                x = 0f;
                y = 0f;
                z = 0f;
                w = 1f;
                lengthSquared = 1f;
            }
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

        // The three remaining components in x, y, z, w order, scaled by 1/length, by the sign that makes the
        // dropped component positive, and by √2 so that ±1/√2 maps to ±1.
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

    /// <summary>Decodes an encoding produced by <see cref="QuantizeQuaternion"/>. Always returns a unit quaternion; bits above 2 + 3 × <paramref name="bitsPerComponent"/> are ignored.</summary>
    /// <param name="packed">The packed encoding.</param>
    /// <param name="bitsPerComponent">2..20, as used when encoding.</param>
    /// <returns>The reconstructed rotation (the representative with a non-negative dropped component).</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="bitsPerComponent"/> is outside 2..20.</exception>
    public static Quaternion DequantizeQuaternion(ulong packed, int bitsPerComponent)
    {
        ValidateRange(bitsPerComponent, MinQuaternionBits, MaxQuaternionBits);
        int m = (1 << (bitsPerComponent - 1)) - 1;
        ulong mask = (1UL << bitsPerComponent) - 1;
        float invM = InvSqrt2 / m;
        int index = (int)(packed & 3);
        float a = DequantizeSnorm((int)((packed >> 2) & mask) - m, m, invM);
        float b = DequantizeSnorm((int)((packed >> (2 + bitsPerComponent)) & mask) - m, m, invM);
        float c = DequantizeSnorm((int)((packed >> (2 + 2 * bitsPerComponent)) & mask) - m, m, invM);
        float d = MathF.Sqrt(MathF.Max(0f, 1f - a * a - b * b - c * c));

        float x = index == 0 ? d : a;
        float y = index == 0 ? a : (index == 1 ? d : b);
        float z = index <= 1 ? b : (index == 2 ? d : c);
        float w = index == 3 ? d : c;
        float invLength = 1f / MathF.Sqrt(x * x + y * y + z * z + w * w);
        return new Quaternion(x * invLength, y * invLength, z * invLength, w * invLength);
    }

    /// <summary>
    /// Returns an exact power of two 2^k such that <paramref name="largestAbs"/> · 2^k lies in [1, 2) for normal floats
    /// below 2^127, in [2, 4) for those at or above it, and in [2^-22, 2) for subnormals. Branch-free: the scale's
    /// biased exponent is <c>254 − e</c> clamped to the normal range. The result for zero, NaN or infinity is
    /// irrelevant (the caller detects those afterwards).
    /// </summary>
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
        // Clamp (NaN-safe: comparisons with NaN are false, so NaN becomes 0) then round to nearest.
        float v = value >= -1f ? value : -1f;
        v = v <= 1f ? v : 1f;
        v = float.IsNaN(value) ? 0f : v;
        return (int)MathF.Round(v * m);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static float DequantizeSnorm(int q, int m, float scale)
    {
        // A code of 2^bits − 1 decodes to m + 1; clamp it so every code maps into the valid range.
        q = q <= m ? q : m;
        return q * scale;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateFloat(float min, float max, int bits)
    {
        if ((uint)(bits - 1) >= MaxFloatBits)
        {
            ThrowBits(bits, 1, MaxFloatBits);
        }

        if (!(max > min) || !float.IsFinite(min) || !float.IsFinite(max))
        {
            ThrowRange();
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void ValidateRange(int bits, int min, int max)
    {
        if ((uint)(bits - min) > (uint)(max - min))
        {
            ThrowBits(bits, min, max);
        }
    }

    private static void ThrowBits(int bits, int min, int max) =>
        throw new ArgumentOutOfRangeException(nameof(bits), bits, $"Bit count must be in {min}..{max}.");

    private static void ThrowRange() =>
        throw new ArgumentException("The range must be finite with max > min.", "max");
}
