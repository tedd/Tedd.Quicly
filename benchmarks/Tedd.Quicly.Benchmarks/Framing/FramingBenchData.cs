using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Framing;

namespace Tedd.Quicly.Benchmarks.Framing;

/// <summary>Channel table and header generators shared by the framing benchmarks.</summary>
internal static class FramingBenchData
{
    public const int Unkeyed16 = 2;
    public const int Keyed32 = 3;
    public const int Plain = 4;
    public const int Fragmenting = 5;
    public const int LatestLz4 = 6;
    public const int Ordered = 8;
    public const int WideEverything = 70;

    public static readonly ChannelTable Table = ChannelTable.Create()
        .Add(Unkeyed16, "unkeyed16", ChannelMode.UnreliableSequenced)
        .Add(Keyed32, "keyed32", ChannelMode.UnreliableSequenced, o => o.Keyed = true)
        .Add(Plain, "plain", ChannelMode.UnreliableUnordered)
        .Add(Fragmenting, "frag", ChannelMode.UnreliableUnordered, o => { o.Fragmentation = true; o.MaxMessageSize = 8800; })
        .Add(LatestLz4, "latestlz4", ChannelMode.ReliableLatest, o => o.Compression = ChannelCompression.Lz4)
        .Add(Ordered, "ordered", ChannelMode.ReliableOrdered)
        .Add(WideEverything, "wide", ChannelMode.UnreliableSequenced, o =>
        {
            o.Keyed = true;
            o.Fragmentation = true;
            o.Compression = ChannelCompression.Lz4;
            o.MaxMessageSize = 8800;
        })
        .Build();

    public static readonly int[] DatagramChannels = { Unkeyed16, Keyed32, Plain, Fragmenting, LatestLz4, WideEverything };

    /// <summary>A game-like header: sequence counter, entity key of 1–2 bytes (0…4095).</summary>
    public static MessageHeader Typical(Random random, uint sequence) => new() { Sequence = sequence, Key = (ulong)random.Next(0, 4096) };

    /// <summary>Any-shape header with keys of every varint length, fragment counts and raw lengths.</summary>
    public static MessageHeader Mixed(Random random, uint sequence)
    {
        byte fragCount = (byte)random.Next(1, 9);
        ulong key = (1 << random.Next(0, 4)) switch
        {
            1 => (ulong)random.Next(0, 64),
            2 => (ulong)random.Next(64, 16384),
            4 => (ulong)random.Next(16384, 1 << 30),
            _ => (ulong)random.NextInt64(1L << 30, 1L << 40),
        };
        return new MessageHeader
        {
            Sequence = sequence,
            Key = key,
            FragCount = fragCount,
            FragIndex = (byte)random.Next(0, fragCount),
            RawLength = random.Next(2) == 0 ? 0 : random.Next(1, 60000),
        };
    }
}
