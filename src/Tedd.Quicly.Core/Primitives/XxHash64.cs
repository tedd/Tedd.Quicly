using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Tedd.Quicly.Core.Primitives;

/// <summary>
/// The classic 64-bit xxHash (XXH64) by Yann Collet. Not XXH3.
/// </summary>
/// <remarks>
/// Produces the same digest as the reference implementation and <c>System.IO.Hashing.XxHash64</c> for
/// any input and seed. The implementation is allocation-free and processes the input in a single pass.
/// </remarks>
public static class XxHash64
{
    private const ulong Prime1 = 11400714785074694791UL;
    private const ulong Prime2 = 14029467366897019727UL;
    private const ulong Prime3 = 1609587929392839161UL;
    private const ulong Prime4 = 9650029242287828579UL;
    private const ulong Prime5 = 2870177450012600261UL;

    /// <summary>Computes the XXH64 digest of <paramref name="data"/>.</summary>
    /// <param name="data">The bytes to hash.</param>
    /// <param name="seed">The seed; 0 gives the canonical unseeded hash.</param>
    /// <returns>The 64-bit digest.</returns>
    public static ulong Hash(ReadOnlySpan<byte> data, ulong seed = 0)
    {
        ref byte p = ref MemoryMarshal.GetReference(data);
        int length = data.Length;
        int i = 0;
        ulong h;

        if (length >= 32)
        {
            ulong v1 = seed + Prime1 + Prime2;
            ulong v2 = seed + Prime2;
            ulong v3 = seed;
            ulong v4 = seed - Prime1;
            int limit = length - 32;
            do
            {
                v1 = Round(v1, ReadUInt64(ref p, i));
                v2 = Round(v2, ReadUInt64(ref p, i + 8));
                v3 = Round(v3, ReadUInt64(ref p, i + 16));
                v4 = Round(v4, ReadUInt64(ref p, i + 24));
                i += 32;
            }
            while (i <= limit);

            h = BitOperations.RotateLeft(v1, 1) + BitOperations.RotateLeft(v2, 7)
              + BitOperations.RotateLeft(v3, 12) + BitOperations.RotateLeft(v4, 18);
            h = MergeRound(h, v1);
            h = MergeRound(h, v2);
            h = MergeRound(h, v3);
            h = MergeRound(h, v4);
        }
        else
        {
            h = seed + Prime5;
        }

        h += (ulong)length;

        while (i + 8 <= length)
        {
            ulong k1 = Round(0, ReadUInt64(ref p, i));
            h ^= k1;
            h = BitOperations.RotateLeft(h, 27) * Prime1 + Prime4;
            i += 8;
        }

        if (i + 4 <= length)
        {
            h ^= ReadUInt32(ref p, i) * Prime1;
            h = BitOperations.RotateLeft(h, 23) * Prime2 + Prime3;
            i += 4;
        }

        while (i < length)
        {
            h ^= Unsafe.Add(ref p, i) * Prime5;
            h = BitOperations.RotateLeft(h, 11) * Prime1;
            i++;
        }

        h ^= h >> 33;
        h *= Prime2;
        h ^= h >> 29;
        h *= Prime3;
        h ^= h >> 32;
        return h;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong Round(ulong acc, ulong input)
    {
        acc += input * Prime2;
        acc = BitOperations.RotateLeft(acc, 31);
        return acc * Prime1;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong MergeRound(ulong acc, ulong value)
    {
        value = Round(0, value);
        acc ^= value;
        return acc * Prime1 + Prime4;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong ReadUInt64(ref byte p, int offset)
    {
        ulong v = Unsafe.ReadUnaligned<ulong>(ref Unsafe.Add(ref p, offset));
        return BitConverter.IsLittleEndian ? v : BinaryPrimitives.ReverseEndianness(v);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static uint ReadUInt32(ref byte p, int offset)
    {
        uint v = Unsafe.ReadUnaligned<uint>(ref Unsafe.Add(ref p, offset));
        return BitConverter.IsLittleEndian ? v : BinaryPrimitives.ReverseEndianness(v);
    }
}
