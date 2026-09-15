using System.Runtime.CompilerServices;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Framing;

namespace Tedd.Quicly.Archive.Framing;

/// <summary>
/// ARCHIVED V1 of <c>Tedd.Quicly.Core.Framing.DatagramFraming.GetHeaderLength</c>: table driven and branch free — a
/// precomputed per-channel constant plus per-field masks, and a branch-free varint length (three compares summed
/// into the length code). Measured slower than branches on the realistic single-shape stream and only marginally
/// faster on randomly mixed shapes; kept for benchmark comparison only (see docs/benchmarks/framing.md).
/// </summary>
public static class DatagramHeaderLengthV1
{
    /// <summary>Per-channel precomputed values (the V1 shipped them as internal fields of the channel definition).</summary>
    public readonly struct Entry
    {
        public Entry(ChannelDefinition channel)
        {
            Fixed = channel.FixedHeaderBytesWithoutKey;
            KeyMask = channel.Keyed ? -1 : 0;
            FragmentMask = channel.Fragmentation ? -1 : 0;
            CompressionMask = channel.Compression != ChannelCompression.None ? -1 : 0;
        }

        public int Fixed { get; }

        public int KeyMask { get; }

        public int FragmentMask { get; }

        public int CompressionMask { get; }
    }

    /// <summary>Returns the encoded datagram header size of <paramref name="header"/> for the precomputed channel entry.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int GetHeaderLength(in Entry channel, in MessageHeader header) =>
        channel.Fixed
        + (channel.KeyMask & VarIntLength(header.Key))
        + (channel.FragmentMask & Unsafe.BitCast<bool, byte>(header.FragCount > 1))
        + (channel.CompressionMask & VarIntLength((uint)header.RawLength));

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int VarIntLength(ulong value)
    {
        int code = Unsafe.BitCast<bool, byte>(value > 63)
                 + Unsafe.BitCast<bool, byte>(value > 16383)
                 + Unsafe.BitCast<bool, byte>(value > 1073741823);
        return 1 << code;
    }
}
