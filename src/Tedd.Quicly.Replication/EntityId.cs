using System.Runtime.CompilerServices;
using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Replication;

/// <summary>
/// A generation-tagged entity identifier: a dense <see cref="Index"/> (usable as an array index) plus a
/// <see cref="Generation"/> that changes every time the index is reused.
/// </summary>
/// <remarks>
/// <para>
/// Generations are the answer to PROTOCOL.md §4.7 (no cross-channel ordering): a state update that arrives for
/// an index whose generation differs from the one the receiver knows is either for a newer entity (spawn not
/// seen yet) or a stale packet for a despawned one, and is never applied to the wrong entity.
/// </para>
/// <para>
/// <b>Wire form:</b> one QUIC varint (RFC 9000 §16, minimal encoding) of
/// <c>(Index &lt;&lt; generationBits) | (Generation &amp; (2^generationBits − 1))</c>. Only the low
/// <c>generationBits</c> of the generation travel; <see cref="EntityIdAllocator"/> keeps generations inside that
/// range so the round trip is exact. <c>generationBits</c> is an application constant in [0, 30]
/// (default <see cref="DefaultGenerationBits"/>); both sides must use the same value. With 30 bits every
/// 32-bit index still fits the 62-bit varint range.
/// </para>
/// <para>
/// <see langword="default"/>(<see cref="EntityId"/>) (index 0, generation 0) is never issued by an allocator with
/// at least one generation bit, so it can serve as "no entity".
/// </para>
/// </remarks>
public readonly struct EntityId : IEquatable<EntityId>
{
    /// <summary>Default number of generation bits carried on the wire.</summary>
    public const int DefaultGenerationBits = 8;

    /// <summary>Largest supported number of generation bits (keeps any 32-bit index inside the 62-bit varint range).</summary>
    public const int MaxGenerationBits = 30;

    /// <summary>Creates an id from its parts.</summary>
    /// <param name="index">The dense index.</param>
    /// <param name="generation">The generation of the index.</param>
    public EntityId(uint index, uint generation)
    {
        Index = index;
        Generation = generation;
    }

    /// <summary>The dense index (array slot) of the entity.</summary>
    public uint Index { get; }

    /// <summary>The generation of <see cref="Index"/>; changes every time the index is freed.</summary>
    public uint Generation { get; }

    /// <summary>Returns the mask of the generation bits carried on the wire.</summary>
    /// <param name="generationBits">Number of generation bits, 0..<see cref="MaxGenerationBits"/>.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="generationBits"/> is outside 0..30.</exception>
    public static uint GetGenerationMask(int generationBits)
    {
        ValidateBits(generationBits);
        return (uint)((1UL << generationBits) - 1);
    }

    /// <summary>Returns the packed wire value (before varint encoding) of this id.</summary>
    /// <param name="generationBits">Number of generation bits, 0..<see cref="MaxGenerationBits"/>.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="generationBits"/> is outside 0..30.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ulong ToWire(int generationBits = DefaultGenerationBits)
    {
        ValidateBits(generationBits);
        return ((ulong)Index << generationBits) | (Generation & (uint)((1UL << generationBits) - 1));
    }

    /// <summary>Unpacks a wire value produced by <see cref="ToWire"/>.</summary>
    /// <param name="wire">The packed value.</param>
    /// <param name="generationBits">Number of generation bits, 0..<see cref="MaxGenerationBits"/>.</param>
    /// <param name="id">The unpacked id, or <see langword="default"/> on failure.</param>
    /// <returns><see langword="false"/> when the index part does not fit 32 bits (malformed input).</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="generationBits"/> is outside 0..30.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool TryFromWire(ulong wire, int generationBits, out EntityId id)
    {
        ValidateBits(generationBits);
        ulong index = wire >> generationBits;
        if (index > uint.MaxValue)
        {
            id = default;
            return false;
        }

        id = new EntityId((uint)index, (uint)wire & (uint)((1UL << generationBits) - 1));
        return true;
    }

    /// <summary>Returns the number of bytes <see cref="TryWrite"/> needs for this id.</summary>
    /// <param name="generationBits">Number of generation bits, 0..<see cref="MaxGenerationBits"/>.</param>
    public int GetWireLength(int generationBits = DefaultGenerationBits) => VarInt.GetLength(ToWire(generationBits));

    /// <summary>Writes the varint wire form of this id.</summary>
    /// <param name="destination">Buffer to write to.</param>
    /// <param name="generationBits">Number of generation bits, 0..<see cref="MaxGenerationBits"/>.</param>
    /// <param name="bytesWritten">Bytes written (1, 2, 4 or 8), or 0 when the destination is too small.</param>
    /// <returns><see langword="false"/> when <paramref name="destination"/> is too small.</returns>
    public bool TryWrite(Span<byte> destination, int generationBits, out int bytesWritten) =>
        VarInt.TryWrite(destination, ToWire(generationBits), out bytesWritten);

    /// <summary>
    /// Reads the varint wire form of an id. Non-minimal varints and index parts wider than 32 bits are rejected.
    /// Never throws on malformed input.
    /// </summary>
    /// <param name="source">Encoded bytes.</param>
    /// <param name="generationBits">Number of generation bits, 0..<see cref="MaxGenerationBits"/>.</param>
    /// <param name="id">The decoded id, or <see langword="default"/> on failure.</param>
    /// <param name="bytesConsumed">Bytes consumed, or 0 on failure.</param>
    /// <returns><see langword="false"/> for truncated, non-minimal or out-of-range input.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="generationBits"/> is outside 0..30.</exception>
    public static bool TryRead(ReadOnlySpan<byte> source, int generationBits, out EntityId id, out int bytesConsumed)
    {
        ValidateBits(generationBits);
        if (VarInt.TryReadMinimal(source, out ulong wire, out bytesConsumed) && TryFromWire(wire, generationBits, out id))
        {
            return true;
        }

        id = default;
        bytesConsumed = 0;
        return false;
    }

    /// <inheritdoc/>
    public bool Equals(EntityId other) => Index == other.Index && Generation == other.Generation;

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is EntityId other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode() => (int)(Index * 0x9E3779B1u ^ Generation);

    /// <summary>Returns <c>"index:generation"</c>.</summary>
    public override string ToString() => $"{Index}:{Generation}";

    /// <summary>Equality of index and generation.</summary>
    public static bool operator ==(EntityId left, EntityId right) => left.Equals(right);

    /// <summary>Inequality of index or generation.</summary>
    public static bool operator !=(EntityId left, EntityId right) => !left.Equals(right);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void ValidateBits(int generationBits)
    {
        if ((uint)generationBits > MaxGenerationBits)
        {
            ThrowBits(generationBits);
        }
    }

    private static void ThrowBits(int generationBits) =>
        throw new ArgumentOutOfRangeException(nameof(generationBits), generationBits, "Generation bits must be in 0..30.");
}
