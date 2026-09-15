using System.Runtime.CompilerServices;

namespace Tedd.Quicly.Core.Primitives;

/// <summary>
/// RFC 1982 serial-number arithmetic for 16-bit and 32-bit sequence numbers that wrap around.
/// </summary>
/// <remarks>
/// <para>
/// Two serial numbers are compared by the sign of their difference computed in the wrapping space:
/// <c>a</c> is newer than <c>b</c> when <c>(a - b)</c>, interpreted as a signed number of the same width,
/// is positive. This makes <c>0</c> newer than <c>0xFFFF</c> (or <c>0xFFFFFFFF</c>) and keeps the
/// comparison correct as long as the two numbers are less than half the space apart:
/// 32 768 for 16-bit numbers and 2 147 483 648 for 32-bit numbers.
/// </para>
/// <para>
/// Exactly half the space apart is undefined in RFC 1982; this implementation treats that case as
/// <c>a</c> being <em>older</em> (the signed difference is the most negative value), so
/// <see cref="IsNewer(ushort, ushort)"/> is never true in both directions at once.
/// </para>
/// </remarks>
public static class SerialNumber
{
    /// <summary>The largest forward distance a 16-bit comparison can express (half the space minus one).</summary>
    public const int MaxDistance16 = short.MaxValue;

    /// <summary>The largest forward distance a 32-bit comparison can express (half the space minus one).</summary>
    public const int MaxDistance32 = int.MaxValue;

    /// <summary>Returns <see langword="true"/> when <paramref name="a"/> is strictly newer than <paramref name="b"/> (16-bit).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsNewer(ushort a, ushort b) => Distance(a, b) > 0;

    /// <summary>Returns <see langword="true"/> when <paramref name="a"/> is newer than or equal to <paramref name="b"/> (16-bit).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsNewerOrEqual(ushort a, ushort b) => Distance(a, b) >= 0;

    /// <summary>
    /// Returns the signed distance <c>a - b</c> in 16-bit serial space: positive when <paramref name="a"/> is newer,
    /// negative when it is older, zero when equal. The result lies in [-32768, 32767].
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int Distance(ushort a, ushort b) => unchecked((short)(a - b));

    /// <summary>Returns <see langword="true"/> when <paramref name="a"/> is strictly newer than <paramref name="b"/> (32-bit).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsNewer(uint a, uint b) => Distance(a, b) > 0;

    /// <summary>Returns <see langword="true"/> when <paramref name="a"/> is newer than or equal to <paramref name="b"/> (32-bit).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsNewerOrEqual(uint a, uint b) => Distance(a, b) >= 0;

    /// <summary>
    /// Returns the signed distance <c>a - b</c> in 32-bit serial space: positive when <paramref name="a"/> is newer,
    /// negative when it is older, zero when equal. The result lies in [-2147483648, 2147483647].
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int Distance(uint a, uint b) => unchecked((int)(a - b));
}
