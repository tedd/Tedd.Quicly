using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Tedd.Quicly.Core.Primitives;

/// <summary>
/// XXH64 over bytes that arrive a piece at a time, for data that is never all in one place: a bulk range being read
/// from its source piece by piece, or written to its sink as it arrives (PROTOCOL.md §3.3 checksum trailer).
/// </summary>
/// <remarks>
/// <para>Produces exactly the digest <see cref="XxHash64.Hash(ReadOnlySpan{byte}, ulong)"/> gives for the concatenation
/// of everything appended, whatever the pieces were.</para>
/// <para>An <b>unmanaged struct</b>, deliberately: it lives inside the engine's per-transfer records, which are native
/// memory, and it costs no allocation to have one per transfer. Copying the struct snapshots the hash — which is how the
/// send side rolls back a piece the transport refused, since that piece's bytes are read and hashed again next pass.</para>
/// </remarks>
public struct XxHash64Builder
{
    private const ulong Prime1 = 11400714785074694791UL;
    private const ulong Prime2 = 14029467366897019727UL;
    private const ulong Prime3 = 1609587929392839161UL;
    private const ulong Prime4 = 9650029242287828579UL;
    private const ulong Prime5 = 2870177450012600261UL;

    /// <summary>Bytes one XXH64 stripe takes; the tail holds less than this between appends.</summary>
    private const int Stripe = 32;

    private ulong _v1;
    private ulong _v2;
    private ulong _v3;
    private ulong _v4;
    private ulong _seed;
    private ulong _length;
    private Tail _tail;
    private int _fill;

    /// <summary>A builder seeded with <paramref name="seed"/> and nothing appended.</summary>
    /// <param name="seed">The seed; 0 gives the canonical unseeded hash.</param>
    public static XxHash64Builder Create(ulong seed = 0)
    {
        XxHash64Builder builder = default;
        builder.Reset(seed);
        return builder;
    }

    /// <summary>Bytes appended so far.</summary>
    public readonly ulong Length => _length;

    /// <summary>Returns the builder to "nothing appended".</summary>
    /// <param name="seed">The seed; 0 gives the canonical unseeded hash.</param>
    public void Reset(ulong seed = 0)
    {
        _seed = seed;
        _v1 = seed + Prime1 + Prime2;
        _v2 = seed + Prime2;
        _v3 = seed;
        _v4 = seed - Prime1;
        _length = 0;
        _fill = 0;
    }

    /// <summary>Takes the next piece of the input.</summary>
    /// <param name="data">The bytes; they need not align to anything.</param>
    public void Append(ReadOnlySpan<byte> data)
    {
        _length += (ulong)data.Length;
        Span<byte> tail = MemoryMarshal.CreateSpan(ref Unsafe.As<Tail, byte>(ref _tail), Stripe);
        if (_fill != 0)
        {
            int need = Stripe - _fill;
            if (data.Length < need)
            {
                data.CopyTo(tail.Slice(_fill));
                _fill += data.Length;
                return;
            }

            data.Slice(0, need).CopyTo(tail.Slice(_fill));
            Absorb(tail);
            data = data.Slice(need);
            _fill = 0;
        }

        while (data.Length >= Stripe)
        {
            Absorb(data);
            data = data.Slice(Stripe);
        }

        if (!data.IsEmpty)
        {
            data.CopyTo(tail);
            _fill = data.Length;
        }
    }

    /// <summary>
    /// The digest of everything appended so far. Pure: the builder is unchanged, so appending may continue afterwards.
    /// </summary>
    public readonly ulong Digest()
    {
        ulong h;
        if (_length >= Stripe)
        {
            h = BitOperations.RotateLeft(_v1, 1) + BitOperations.RotateLeft(_v2, 7)
              + BitOperations.RotateLeft(_v3, 12) + BitOperations.RotateLeft(_v4, 18);
            h = MergeRound(h, _v1);
            h = MergeRound(h, _v2);
            h = MergeRound(h, _v3);
            h = MergeRound(h, _v4);
        }
        else
        {
            h = _seed + Prime5;
        }

        h += _length;

        // The bytes the stripes did not take: exactly `length % 32`, which is what the tail holds.
        ref byte p = ref Unsafe.As<Tail, byte>(ref Unsafe.AsRef(in _tail));
        int i = 0;
        while (i + 8 <= _fill)
        {
            h ^= XxHash64.Round(0, ReadUInt64(ref p, i));
            h = BitOperations.RotateLeft(h, 27) * Prime1 + Prime4;
            i += 8;
        }

        if (i + 4 <= _fill)
        {
            h ^= ReadUInt32(ref p, i) * Prime1;
            h = BitOperations.RotateLeft(h, 23) * Prime2 + Prime3;
            i += 4;
        }

        while (i < _fill)
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

    /// <summary>Takes one 32-byte stripe into the four accumulators.</summary>
    private void Absorb(ReadOnlySpan<byte> stripe)
    {
        ref byte p = ref MemoryMarshal.GetReference(stripe);
        _v1 = XxHash64.Round(_v1, ReadUInt64(ref p, 0));
        _v2 = XxHash64.Round(_v2, ReadUInt64(ref p, 8));
        _v3 = XxHash64.Round(_v3, ReadUInt64(ref p, 16));
        _v4 = XxHash64.Round(_v4, ReadUInt64(ref p, 24));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ulong MergeRound(ulong acc, ulong value)
    {
        value = XxHash64.Round(0, value);
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

    /// <summary>The bytes waiting for their stripe to fill.</summary>
    [InlineArray(Stripe)]
    private struct Tail
    {
        private byte _element0;
    }
}
