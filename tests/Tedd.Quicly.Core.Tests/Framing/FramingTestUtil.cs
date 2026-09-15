using System.Text;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Framing;

namespace Tedd.Quicly.Core.Tests.Framing;

/// <summary>Channel tables shared by the framing tests: one channel per header shape.</summary>
internal static class TestTables
{
    public const int UU = 2;                 // unreliable unordered, no sequence, unkeyed
    public const int USeq16 = 3;             // sequenced, 16-bit, unkeyed
    public const int USeq32Keyed = 4;        // sequenced, 32-bit, keyed
    public const int USeq16Keyed = 5;        // sequenced, 16-bit, keyed
    public const int UUFrag = 6;             // unordered + fragmentation (16-bit reassembly id), max 8800
    public const int USFragKeyed = 7;        // sequenced keyed + fragmentation, max 4000
    public const int UULz4 = 8;              // unordered + LZ4
    public const int USFragKeyedLz4 = 9;     // sequenced keyed + fragmentation + LZ4, max 8800
    public const int Latest = 10;            // ReliableLatest
    public const int LatestLz4 = 11;         // ReliableLatest + LZ4
    public const int Ordered = 12;           // ReliableOrdered plain
    public const int OrderedFull = 13;       // ReliableOrdered keyed + request/response + LZ4
    public const int Unordered = 14;         // ReliableUnordered plain
    public const int UnorderedKeyedLz4 = 15; // ReliableUnordered keyed + LZ4
    public const int BulkChannel = 16;       // Bulk
    public const int Wide = 100;             // two-byte id, sequenced keyed 32-bit
    public const int WidePlain = 200;        // two-byte id, unordered plain
    public const int WideOrdered = 300;      // two-byte id, ReliableOrdered
    public const int MaxLatest = 16383;      // largest id, ReliableLatest + LZ4
    public const int Missing = 20;           // not in the table

    public static readonly ChannelTable All = ChannelTable.Create()
        .Add(UU, "uu", ChannelMode.UnreliableUnordered)
        .Add(USeq16, "us16", ChannelMode.UnreliableSequenced)
        .Add(USeq32Keyed, "us32k", ChannelMode.UnreliableSequenced, o => o.Keyed = true)
        .Add(USeq16Keyed, "us16k", ChannelMode.UnreliableSequenced, o => { o.Keyed = true; o.SequenceBits = 16; })
        .Add(UUFrag, "uufrag", ChannelMode.UnreliableUnordered, o => { o.Fragmentation = true; o.MaxMessageSize = 8800; })
        .Add(USFragKeyed, "usfragk", ChannelMode.UnreliableSequenced, o => { o.Keyed = true; o.Fragmentation = true; o.MaxMessageSize = 4000; })
        .Add(UULz4, "uulz4", ChannelMode.UnreliableUnordered, o => o.Compression = ChannelCompression.Lz4)
        .Add(USFragKeyedLz4, "usfragklz4", ChannelMode.UnreliableSequenced, o => { o.Keyed = true; o.Fragmentation = true; o.Compression = ChannelCompression.Lz4; o.MaxMessageSize = 8800; })
        .Add(Latest, "latest", ChannelMode.ReliableLatest)
        .Add(LatestLz4, "latestlz4", ChannelMode.ReliableLatest, o => o.Compression = ChannelCompression.Lz4)
        .Add(Ordered, "ordered", ChannelMode.ReliableOrdered)
        .Add(OrderedFull, "orderedfull", ChannelMode.ReliableOrdered, o => { o.Keyed = true; o.RequestResponse = true; o.Compression = ChannelCompression.Lz4; })
        .Add(Unordered, "unordered", ChannelMode.ReliableUnordered)
        .Add(UnorderedKeyedLz4, "unorderedklz4", ChannelMode.ReliableUnordered, o => { o.Keyed = true; o.Compression = ChannelCompression.Lz4; })
        .Add(BulkChannel, "bulk", ChannelMode.Bulk)
        .Add(Wide, "wide", ChannelMode.UnreliableSequenced, o => o.Keyed = true)
        .Add(WidePlain, "wideplain", ChannelMode.UnreliableUnordered)
        .Add(WideOrdered, "wideordered", ChannelMode.ReliableOrdered)
        .Add(MaxLatest, "maxlatest", ChannelMode.ReliableLatest, o => o.Compression = ChannelCompression.Lz4)
        .Build();

    public static ChannelDefinition Get(int id) => All[id] ?? throw new InvalidOperationException($"no channel {id}");

    /// <summary>The reference table of docs/protocol-vectors.md.</summary>
    public static ChannelTable Reference() => ChannelTable.Create()
        .Add(2, "move", ChannelMode.UnreliableSequenced, o => { o.Keyed = true; o.Priority = 200; })
        .Add(3, "chat", ChannelMode.ReliableOrdered, o => o.RequestResponse = true)
        .Add(5, "fx", ChannelMode.UnreliableUnordered, o => { o.Fragmentation = true; o.MaxMessageSize = 8800; })
        .Add(10, "state", ChannelMode.ReliableLatest, o => o.Compression = ChannelCompression.Lz4)
        .Add(64, "world", ChannelMode.Bulk, o => o.Priority = 16)
        .Build();
}

internal static class Bytes
{
    public static byte[] Hex(string hex) => Convert.FromHexString(hex.Replace(" ", "", StringComparison.Ordinal));

    public static string ToHex(ReadOnlySpan<byte> bytes) => Convert.ToHexString(bytes);

    public static byte[] Concat(params byte[][] parts)
    {
        byte[] result = new byte[parts.Sum(p => p.Length)];
        int pos = 0;
        foreach (byte[] p in parts)
        {
            p.CopyTo(result, pos);
            pos += p.Length;
        }

        return result;
    }

    public static byte[] Fill(int length, byte seed = 0)
    {
        byte[] b = new byte[length];
        for (int i = 0; i < length; i++)
        {
            b[i] = (byte)(seed + i * 7);
        }

        return b;
    }
}

/// <summary>Builds stream byte sequences with the production writers.</summary>
internal sealed class StreamBuilder
{
    private readonly List<byte> _bytes = new();

    public StreamBuilder Raw(params byte[] bytes)
    {
        _bytes.AddRange(bytes);
        return this;
    }

    public StreamBuilder Preamble(ushort channel)
    {
        Span<byte> b = stackalloc byte[8];
        return Raw(b.Slice(0, StreamFraming.WritePreamble(b, channel)).ToArray());
    }

    public StreamBuilder GroupPreamble(ushort channel, ulong groupId)
    {
        Span<byte> b = stackalloc byte[16];
        return Raw(b.Slice(0, StreamFraming.WriteGroupPreamble(b, channel, groupId)).ToArray());
    }

    public StreamBuilder Frame(ChannelDefinition channel, StreamMessageHeader header, byte[] payload)
    {
        header.Length = payload.Length;
        Span<byte> b = stackalloc byte[StreamFraming.MaxFrameHeaderLength];
        Raw(b.Slice(0, StreamFraming.WriteFrameHeader(b, channel, header)).ToArray());
        return Raw(payload);
    }

    public StreamBuilder Control(byte type, byte[] body)
    {
        Span<byte> b = stackalloc byte[16];
        Raw(b.Slice(0, StreamFraming.WriteControlFrameHeader(b, type, body.Length)).ToArray());
        return Raw(body);
    }

    public StreamBuilder Bulk(BulkHeader header)
    {
        byte[] b = new byte[StreamFraming.GetBulkHeaderLength(header)];
        StreamFraming.WriteBulkHeader(b, header);
        return Raw(b);
    }

    public StreamBuilder Chunk(int rawLength, byte[] bytes)
    {
        Span<byte> b = stackalloc byte[16];
        Raw(b.Slice(0, StreamFraming.WriteBulkChunkHeader(b, bytes.Length, rawLength)).ToArray());
        return Raw(bytes);
    }

    public byte[] ToArray() => _bytes.ToArray();
}

/// <summary>Drives a <see cref="StreamFrameParser"/> over segments and records a canonical event log.</summary>
internal static class StreamDriver
{
    public static List<string> Run(ref StreamFrameParser parser, IEnumerable<byte[]> segments, bool finish = true)
    {
        List<string> log = new();
        List<byte> payload = new();
        foreach (byte[] segment in segments)
        {
            ReadOnlySpan<byte> input = segment;
            int guard = 0;
            while (true)
            {
                Assert.True(++guard < 100_000, "parser made no progress");
                StreamEvent ev = parser.Read(ref input, out ReadOnlySpan<byte> chunk);
                if (ev != StreamEvent.PayloadChunk)
                {
                    Assert.True(chunk.IsEmpty);
                }

                switch (ev)
                {
                    case StreamEvent.NeedMore:
                        Assert.True(input.IsEmpty);
                        goto NextSegment;
                    case StreamEvent.Preamble:
                        log.Add($"P ch={parser.Channel} g={parser.GroupId} role={parser.Role}");
                        break;
                    case StreamEvent.MessageStart:
                        StreamMessageHeader m = parser.Message;
                        log.Add($"S len={m.Length} seq={m.Sequence} key={m.Key} rid={m.RequestId} raw={m.RawLength} type={parser.ControlType}");
                        Assert.Equal(m.Length, parser.RemainingPayload);
                        payload.Clear();
                        break;
                    case StreamEvent.PayloadChunk:
                        Assert.False(chunk.IsEmpty);
                        payload.AddRange(chunk.ToArray());
                        break;
                    case StreamEvent.MessageEnd:
                        Assert.Equal(0, parser.RemainingPayload);
                        log.Add($"E {Convert.ToHexString(payload.ToArray())}");
                        payload.Clear();
                        break;
                    case StreamEvent.BulkHeader:
                        BulkHeader b = parser.Bulk;
                        ReadOnlySpan<byte> hash = b.Hash;
                        log.Add($"B t={b.TransferId} o={b.ObjectId} v={b.ObjectVersion} total={b.TotalLength} off={b.Offset} len={b.Length} flags={b.Flags} hash={(b.HasHash ? Convert.ToHexString(hash) : "-")}");
                        break;
                    case StreamEvent.Error:
                        log.Add($"X {parser.Error}");
                        Assert.Equal(StreamEvent.Error, parser.Read(ref input, out _));
                        return log;
                }
            }

        NextSegment:;
        }

        if (finish)
        {
            log.Add($"F {parser.Finish()}");
        }

        return log;
    }

    public static List<string> Run(StreamRole role, ChannelTable? table, byte[] stream, int maxMessageSize = 0, int bulkMaxChunk = StreamFraming.DefaultBulkMaxChunk)
    {
        StreamFrameParser parser = default;
        parser.Reset(role, table, maxMessageSize, bulkMaxChunk);
        return Run(ref parser, new[] { stream });
    }

    public static List<string> RunSegments(StreamRole role, ChannelTable? table, IEnumerable<byte[]> segments, int maxMessageSize = 0, int bulkMaxChunk = StreamFraming.DefaultBulkMaxChunk)
    {
        StreamFrameParser parser = default;
        parser.Reset(role, table, maxMessageSize, bulkMaxChunk);
        return Run(ref parser, segments);
    }

    public static IEnumerable<byte[]> SplitAt(byte[] stream, params int[] cuts)
    {
        int previous = 0;
        foreach (int cut in cuts)
        {
            yield return stream[previous..cut];
            previous = cut;
        }

        yield return stream[previous..];
    }

    public static IEnumerable<byte[]> RandomSplit(byte[] stream, Random random, int maxPieces)
    {
        int pieces = random.Next(1, maxPieces + 1);
        int[] cuts = new int[pieces - 1];
        for (int i = 0; i < cuts.Length; i++)
        {
            cuts[i] = random.Next(0, stream.Length + 1);
        }

        Array.Sort(cuts);
        return SplitAt(stream, cuts);
    }

    public static IEnumerable<byte[]> ByteByByte(byte[] stream)
    {
        foreach (byte b in stream)
        {
            yield return new[] { b };
        }
    }

    public static string Utf8(string s) => Convert.ToHexString(Encoding.UTF8.GetBytes(s));
}

internal static class AllocationAssert
{
    public static void None(Action body, int iterations = 2_000)
    {
        for (int i = 0; i < 50; i++)
        {
            body();
        }

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < iterations; i++)
        {
            body();
        }

        long after = GC.GetAllocatedBytesForCurrentThread();
        Assert.Equal(0, after - before);
    }
}
