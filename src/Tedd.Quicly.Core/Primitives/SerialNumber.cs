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
/// <para>
/// A pairwise comparison is therefore only valid between two numbers that are known to be close. State that must
/// outlive half the space (the last accepted value of a key that may idle while its channel's counter runs on) is kept
/// as a 64-bit <em>extended</em> sequence instead: <see cref="Extend(ulong, ushort)"/> unwraps each arriving number
/// against the newest one seen, and extended values compare with plain <c>&gt;</c> however far apart they are
/// (PROTOCOL.md §8).
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

    /// <summary>
    /// Extends a 16-bit sequence to 64 bits against <paramref name="newest"/>, the extended value of the newest sequence
    /// seen so far: the result is the value nearest to <paramref name="newest"/> whose low 16 bits are
    /// <paramref name="sequence"/>, in [<paramref name="newest"/> − 32768, <paramref name="newest"/> + 32767]. Exactly
    /// half the space apart extends <em>behind</em>, consistent with <see cref="IsNewer(ushort, ushort)"/>.
    /// </summary>
    /// <param name="newest">The extended newest sequence; at least 2^16 (the caller seeds it as 2^16 + the first sequence), so the result cannot underflow.</param>
    /// <param name="sequence">The sequence to extend.</param>
    /// <returns>The extended sequence; greater than <paramref name="newest"/> exactly when <paramref name="sequence"/> is serially newer than its low bits.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong Extend(ulong newest, ushort sequence) => unchecked((ulong)((long)newest + Distance(sequence, (ushort)newest)));

    /// <summary>
    /// Extends a 32-bit sequence to 64 bits against <paramref name="newest"/>, the extended value of the newest sequence
    /// seen so far: the result is the value nearest to <paramref name="newest"/> whose low 32 bits are
    /// <paramref name="sequence"/>, in [<paramref name="newest"/> − 2^31, <paramref name="newest"/> + 2^31 − 1]. Exactly
    /// half the space apart extends <em>behind</em>, consistent with <see cref="IsNewer(uint, uint)"/>.
    /// </summary>
    /// <param name="newest">The extended newest sequence; at least 2^32 (the caller seeds it as 2^32 + the first sequence), so the result cannot underflow.</param>
    /// <param name="sequence">The sequence to extend.</param>
    /// <returns>The extended sequence; greater than <paramref name="newest"/> exactly when <paramref name="sequence"/> is serially newer than its low bits.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong Extend(ulong newest, uint sequence) => unchecked((ulong)((long)newest + Distance(sequence, (uint)newest)));
}
