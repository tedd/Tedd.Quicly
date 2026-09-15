using System.Runtime.CompilerServices;
using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Core.Framing;

/// <summary>Small shared helpers of the framing parsers and writers.</summary>
internal static class WireReader
{
    /// <summary>Reads a minimal varint at <paramref name="pos"/> and advances it; distinguishes truncation from non-minimal encodings.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ParseStatus ReadVarInt(ReadOnlySpan<byte> source, ref int pos, out ulong value)
    {
        ReadOnlySpan<byte> rest = source.Slice(pos);
        if (VarInt.TryReadMinimal(rest, out value, out int consumed))
        {
            pos += consumed;
            return ParseStatus.Ok;
        }

        return Classify(rest);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static ParseStatus Classify(ReadOnlySpan<byte> rest) =>
        VarInt.TryRead(rest, out _, out _) ? ParseStatus.NonMinimalVarint : ParseStatus.Truncated;

    /// <summary>Branch-free varint length (1, 2, 4 or 8) of a value ≤ <see cref="VarInt.MaxValue"/>; larger values report 8.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int VarIntLength(ulong value)
    {
        int code = Unsafe.BitCast<bool, byte>(value > 63)
                 + Unsafe.BitCast<bool, byte>(value > 16383)
                 + Unsafe.BitCast<bool, byte>(value > 1073741823);
        return 1 << code;
    }
}
