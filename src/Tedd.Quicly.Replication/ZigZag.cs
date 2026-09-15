using System.Runtime.CompilerServices;

namespace Tedd.Quicly.Replication;

/// <summary>
/// ZigZag mapping between signed and unsigned integers (0 → 0, −1 → 1, 1 → 2, −2 → 3, …), so small magnitudes of
/// either sign become small unsigned numbers that variable-length encodings store in few bits. Branch-free.
/// </summary>
public static class ZigZag
{
    /// <summary>Maps a signed 64-bit value to its ZigZag form.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong Encode(long value) => (ulong)((value << 1) ^ (value >> 63));

    /// <summary>Inverse of <see cref="Encode(long)"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static long Decode(ulong value) => (long)(value >> 1) ^ -(long)(value & 1);

    /// <summary>Maps a signed 32-bit value to its ZigZag form.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static uint Encode(int value) => (uint)((value << 1) ^ (value >> 31));

    /// <summary>Inverse of <see cref="Encode(int)"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int Decode(uint value) => (int)(value >> 1) ^ -(int)(value & 1);
}
