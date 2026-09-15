using System.Runtime.CompilerServices;

namespace Tedd.Quicly.Core.State;

/// <summary>The key mixing function used by <see cref="KeyTable"/>.</summary>
public static class KeyHash
{
    /// <summary>
    /// MurmurHash3's 64-bit finalizer (<c>fmix64</c>): a bijection on 64-bit values whose every output bit depends
    /// on every input bit, so keys that differ only in high bits or share a stride (entity ids × 1024, aligned
    /// handles) spread evenly over a power-of-two bucket range. See docs/benchmarks/state.md for the identity-hash
    /// comparison that motivated it.
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
}
