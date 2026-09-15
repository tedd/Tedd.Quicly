using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Archive.Framing;

/// <summary>
/// ARCHIVED V0 of <c>Tedd.Quicly.Core.Framing.DatagramFraming.GetHeaderLength</c>: one branch per optional header
/// field, recomputed from the channel flags on every call, with the if-chain <see cref="VarInt.GetLength"/>. Kept for
/// benchmark comparison only (see docs/benchmarks/framing.md).
/// </summary>
public static class DatagramHeaderLengthV0
{
    /// <summary>Returns the encoded datagram header size of <paramref name="header"/> on <paramref name="channel"/>.</summary>
    public static int GetHeaderLength(ChannelDefinition channel, in MessageHeader header)
    {
        int length = channel.Id <= 63 ? 1 : 2;
        if (channel.HasSequence)
        {
            length += channel.SequenceBits == 32 ? 4 : 2;
        }

        if (channel.Keyed)
        {
            length += VarInt.GetLength(header.Key);
        }

        if (channel.Fragmentation)
        {
            length += header.FragCount > 1 ? 2 : 1;
        }

        if (channel.Compression != ChannelCompression.None)
        {
            length += VarInt.GetLength((ulong)header.RawLength);
        }

        return length;
    }
}
