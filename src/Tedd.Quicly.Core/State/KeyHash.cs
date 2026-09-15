using System.Runtime.CompilerServices;
using System.Security.Cryptography;

namespace Tedd.Quicly.Core.State;

/// <summary>The key mixing function used by <see cref="KeyTable"/>.</summary>
internal static class KeyHash
{
    /// <summary>
    /// MurmurHash3's 64-bit finalizer (<c>fmix64</c>): a bijection on 64-bit values whose every output bit depends
    /// on every input bit, so keys that differ only in high bits or share a stride (entity ids × 1024, aligned
    /// handles) spread evenly over a power-of-two bucket range. See docs/benchmarks/state.md for the identity-hash
    /// comparison that chose it.
    /// </summary>
    /// <param name="key">The key.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ulong Fmix64(ulong key)
    {
        key ^= key >> 33;
        key *= 0xff51afd7ed558ccdUL;
        key ^= key >> 33;
        key *= 0xc4ceb9fe1a85ec53UL;
        key ^= key >> 33;
        return key;
    }

    /// <summary>
    /// A random per-table seed, XORed into every key before <see cref="Fmix64"/>. <c>fmix64</c> alone is public and
    /// invertible, so a peer that chooses keys could compute any number of keys with the same home bucket and turn
    /// every lookup into a scan of the whole table (ADR 0009: remote-chosen identifiers); with an unknown seed it
    /// cannot target a bucket.
    /// </summary>
    public static ulong NewSeed()
    {
        Span<byte> bytes = stackalloc byte[sizeof(ulong)];
        RandomNumberGenerator.Fill(bytes);
        return BitConverter.ToUInt64(bytes);
    }
}
