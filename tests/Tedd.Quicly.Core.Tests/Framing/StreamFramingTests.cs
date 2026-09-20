using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Primitives;
using static Tedd.Quicly.Core.Tests.Framing.TestTables;

namespace Tedd.Quicly.Core.Tests.Framing;

public class StreamFramingTests
{
    [Theory]
    [InlineData(0, "00")]
    [InlineData(2, "02")]
    [InlineData(63, "3F")]
    [InlineData(64, "4040")]
    [InlineData(16383, "7FFF")]
    public void Preamble_Vectors(int channel, string hex)
    {
        byte[] b = new byte[8];
        int n = StreamFraming.WritePreamble(b, (ushort)channel);
        Assert.Equal(hex, Bytes.ToHex(b.AsSpan(0, n)));
        Assert.Equal(n, StreamFraming.GetPreambleLength((ushort)channel));
    }

    [Theory]
    [InlineData(14, 0UL, "0E00")]
    [InlineData(14, 64UL, "0E4040")]
    [InlineData(300, 1UL << 30, "412CC000000040000000")]
    public void Group_Preamble_Vectors(int channel, ulong group, string hex)
    {
        byte[] b = new byte[16];
        int n = StreamFraming.WriteGroupPreamble(b, (ushort)channel, group);
        Assert.Equal(hex, Bytes.ToHex(b.AsSpan(0, n)));
        Assert.Equal(n, StreamFraming.GetGroupPreambleLength((ushort)channel, group));
    }

    [Fact]
    public void Preamble_Writers_Validate()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => StreamFraming.WritePreamble(new byte[8], 16384));
        Assert.Throws<ArgumentOutOfRangeException>(() => StreamFraming.WriteGroupPreamble(new byte[16], 16384, 0));
        Assert.Throws<ArgumentException>(() => StreamFraming.WritePreamble(Span<byte>.Empty, 2));
        Assert.Throws<ArgumentException>(() => StreamFraming.WriteGroupPreamble(new byte[2], 64, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => StreamFraming.WriteGroupPreamble(new byte[16], 2, VarInt.MaxValue + 1));
    }

    public static TheoryData<int, int, uint, ulong, uint, int, string> FrameVectors => new()
    {
        { Ordered, 5, 0, 0, 0, 0, "05" },
        { Ordered, 64, 0, 0, 0, 0, "4040" },
        { OrderedFull, 10, 0, 7, 3, 0, "0A 07 03 00" },
        { OrderedFull, 10, 0, 16384, 64, 200, "0A 80004000 4040 40C8" },
        { Unordered, 0, 0, 0, 0, 0, "00" },
        { UnorderedKeyedLz4, 3, 0, 1, 0, 9, "03 01 09" },
        { Latest, 4, 0x01020304, 9, 0, 0, "04 04030201 09" },
        { LatestLz4, 4, 1, 9, 0, 10, "04 01000000 09 0A" },
        { WideOrdered, 1, 0, 0, 0, 0, "01" },
    };

    [Theory]
    [MemberData(nameof(FrameVectors))]
    public void Frame_Header_Vectors(int channelId, int length, uint sequence, ulong key, uint requestId, int raw, string hex)
    {
        ChannelDefinition ch = Get(channelId);
        StreamMessageHeader h = new() { Length = length, Sequence = sequence, Key = key, RequestId = requestId, RawLength = raw };
        byte[] expected = Bytes.Hex(hex);
        Assert.Equal(expected.Length, StreamFraming.GetFrameHeaderLength(ch, h));
        byte[] b = new byte[StreamFraming.MaxFrameHeaderLength];
        int n = StreamFraming.WriteFrameHeader(b, ch, h);
        Assert.Equal(Bytes.ToHex(expected), Bytes.ToHex(b.AsSpan(0, n)));

        Assert.Equal(ParseStatus.Ok, StreamFraming.TryParseFrameHeader(Bytes.Concat(expected, new byte[] { 0xEE }), ch, 0, out StreamMessageHeader p, out int consumed));
        Assert.Equal(n, consumed);
        Assert.Equal(length, p.Length);
        Assert.Equal(ch.Mode == ChannelMode.ReliableLatest ? sequence : 0, p.Sequence);
        Assert.Equal(ch.Keyed ? key : 0, p.Key);
        Assert.Equal(ch.RequestResponse ? requestId : 0, p.RequestId);
        Assert.Equal(raw, p.RawLength);
        Assert.Equal(raw > 0, p.Compressed);
    }

    [Fact]
    public void Largest_Frame_Header_Is_32_Bytes()
    {
        ChannelTable t = ChannelTable.Create().Add(2, "x", ChannelMode.ReliableOrdered, o =>
        {
            o.Keyed = true;
            o.RequestResponse = true;
            o.Compression = ChannelCompression.Lz4;
        }).Build();
        StreamMessageHeader h = new() { Length = int.MaxValue, Key = VarInt.MaxValue, RequestId = uint.MaxValue, RawLength = int.MaxValue };
        Assert.Equal(StreamFraming.MaxFrameHeaderLength, StreamFraming.GetFrameHeaderLength(t[2]!, h));
    }

    [Fact]
    public void WriteFrameHeader_Validates()
    {
        Assert.Throws<ArgumentException>(() => StreamFraming.WriteFrameHeader(new byte[32], Get(UU), default));
        Assert.Throws<ArgumentException>(() => StreamFraming.WriteFrameHeader(new byte[32], Get(BulkChannel), default));
        Assert.Throws<ArgumentException>(() => StreamFraming.WriteFrameHeader(new byte[32], Get(Ordered), new StreamMessageHeader { Length = -1 }));
        Assert.Throws<ArgumentException>(() => StreamFraming.WriteFrameHeader(new byte[32], Get(OrderedFull), new StreamMessageHeader { RawLength = -1 }));
        Assert.Throws<ArgumentException>(() => StreamFraming.WriteFrameHeader(new byte[32], Get(OrderedFull), new StreamMessageHeader { Key = 1UL << 62 }));
        Assert.Throws<ArgumentException>(() => StreamFraming.WriteFrameHeader(new byte[1], Get(Ordered), new StreamMessageHeader { Length = 64 }));
        // A key on an unkeyed channel is ignored.
        Assert.Equal(1, StreamFraming.WriteFrameHeader(new byte[1], Get(Ordered), new StreamMessageHeader { Key = ulong.MaxValue }));
    }

    public static TheoryData<int, string, int, ParseStatus> FrameStatuses => new()
    {
        { UU, "00", 0, ParseStatus.ChannelNotStream },
        { BulkChannel, "00", 0, ParseStatus.ChannelNotStream },
        { Ordered, "", 0, ParseStatus.Truncated },
        { Ordered, "40", 0, ParseStatus.Truncated },
        { Ordered, "4001", 0, ParseStatus.NonMinimalVarint },
        { Ordered, "80010000", 0, ParseStatus.Ok },                 // 65536 = limit
        { Ordered, "80010001", 0, ParseStatus.MessageTooLarge },
        { Ordered, "C000000100000000", 0, ParseStatus.MessageTooLarge },
        { Ordered, "4065", 100, ParseStatus.MessageTooLarge },       // 101 > session cap
        { Ordered, "4064", 100, ParseStatus.Ok },
        { OrderedFull, "05", 0, ParseStatus.Truncated },
        { OrderedFull, "05 4001 00 00", 0, ParseStatus.NonMinimalVarint },
        { OrderedFull, "05 01", 0, ParseStatus.Truncated },
        { OrderedFull, "05 01 4001 00", 0, ParseStatus.NonMinimalVarint },
        { OrderedFull, "05 01 C000000100000000 00", 0, ParseStatus.ValueOutOfRange },
        { OrderedFull, "05 01 C0000000FFFFFFFF 00", 0, ParseStatus.Ok },
        { OrderedFull, "05 01 01", 0, ParseStatus.Truncated },
        { OrderedFull, "05 01 01 4001", 0, ParseStatus.NonMinimalVarint },
        { OrderedFull, "05 01 01 80010001", 0, ParseStatus.RawLengthTooLarge },
        { OrderedFull, "05 01 01 4065", 100, ParseStatus.RawLengthTooLarge },
        { OrderedFull, "00 01 01 05", 0, ParseStatus.BadLength },    // compressed but empty
        { OrderedFull, "05 01 01 05", 0, ParseStatus.BadLength },    // did not shrink
        { OrderedFull, "05 01 01 06", 0, ParseStatus.Ok },
        { OrderedFull, "80010001 01 01 00", 0, ParseStatus.MessageTooLarge },
        { Latest, "05", 0, ParseStatus.Truncated },
        { Latest, "05 010203", 0, ParseStatus.Truncated },
        { Latest, "05 01020304", 0, ParseStatus.Truncated },
        { Latest, "05 01020304 4001", 0, ParseStatus.NonMinimalVarint },
        { Latest, "05 01020304 01", 0, ParseStatus.Ok },
    };

    [Theory]
    [MemberData(nameof(FrameStatuses))]
    public void Frame_Header_Parse_Statuses(int channelId, string hex, int cap, ParseStatus expected)
    {
        Assert.Equal(expected, StreamFraming.TryParseFrameHeader(Bytes.Hex(hex), Get(channelId), cap, out _, out int consumed));
        if (expected != ParseStatus.Ok)
        {
            Assert.Equal(0, consumed);
        }
    }

    [Theory]
    [InlineData(0, "01AA")]
    [InlineData(5, "06AA")]
    [InlineData(62, "3FAA")]
    [InlineData(63, "4040AA")]
    [InlineData(16383, "80004000 AA")]
    public void Control_Frame_Header_Vectors(int bodyLength, string hex)
    {
        byte[] b = new byte[8];
        int n = StreamFraming.WriteControlFrameHeader(b, 0xAA, bodyLength);
        Assert.Equal(hex.Replace(" ", "", StringComparison.Ordinal), Bytes.ToHex(b.AsSpan(0, n)));
        Assert.Equal(n, StreamFraming.GetControlFrameHeaderLength(bodyLength));
        Assert.Equal(ParseStatus.Ok, StreamFraming.TryParseControlFrameHeader(b.AsSpan(0, n), out byte type, out int body, out int consumed));
        Assert.Equal(0xAA, type);
        Assert.Equal(bodyLength, body);
        Assert.Equal(n, consumed);
    }

    [Fact]
    public void Control_Frame_Header_Validation()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => StreamFraming.WriteControlFrameHeader(new byte[8], 1, 16384));
        Assert.Throws<ArgumentOutOfRangeException>(() => StreamFraming.WriteControlFrameHeader(new byte[8], 1, -1));
        Assert.Throws<ArgumentException>(() => StreamFraming.WriteControlFrameHeader(new byte[1], 1, 0));

        Assert.Equal(ParseStatus.BadLength, StreamFraming.TryParseControlFrameHeader(Bytes.Hex("00 10"), out _, out _, out _));
        Assert.Equal(ParseStatus.BadLength, StreamFraming.TryParseControlFrameHeader(Bytes.Hex("80004001 10"), out _, out _, out _));
        Assert.Equal(ParseStatus.Ok, StreamFraming.TryParseControlFrameHeader(Bytes.Hex("80004000 10"), out _, out int body, out _));
        Assert.Equal(16383, body);
        Assert.Equal(ParseStatus.Truncated, StreamFraming.TryParseControlFrameHeader(Bytes.Hex("05"), out _, out _, out _));
        Assert.Equal(ParseStatus.Truncated, StreamFraming.TryParseControlFrameHeader(ReadOnlySpan<byte>.Empty, out _, out _, out _));
        Assert.Equal(ParseStatus.NonMinimalVarint, StreamFraming.TryParseControlFrameHeader(Bytes.Hex("4005 10"), out _, out _, out _));
    }

    internal static BulkHeader SampleBulk(bool checksum, bool chunked, ulong length = 100) => new()
    {
        TransferId = 7,
        ObjectId = 1000,
        ObjectVersion = 3,
        TotalLength = 1_000_000,
        Offset = 64,
        Length = length,
        Flags = (checksum ? BulkFlags.ChecksumPresent : 0) | (chunked ? BulkFlags.Chunked : 0),
    };

    [Fact]
    public void Bulk_Checksum_Trailer_Vector()
    {
        // docs/protocol-vectors.md: the trailer is the range's xxHash64 as 8 little-endian bytes.
        byte[] b = new byte[StreamFraming.BulkChecksumLength];
        Assert.Equal(b.Length, StreamFraming.WriteBulkChecksum(b, XxHash64.Hash(new byte[100])));
        Assert.Equal("2F502CC90311BB17", Bytes.ToHex(b));

        Assert.Equal(ParseStatus.Ok, StreamFraming.ParseBulkChecksum(b, out ulong parsed, out int consumed));
        Assert.Equal(b.Length, consumed);
        Assert.Equal(0x17BB1103C92C502FUL, parsed);
        Assert.Equal(ParseStatus.Truncated, StreamFraming.ParseBulkChecksum(b.AsSpan(0, 7), out _, out int none));
        Assert.Equal(0, none);
    }

    [Fact]
    public void Bulk_Header_Vector()
    {
        BulkHeader h = SampleBulk(checksum: false, chunked: true);
        byte[] b = new byte[StreamFraming.GetBulkHeaderLength(h)];
        Assert.Equal(b.Length, StreamFraming.WriteBulkHeader(b, h));
        Assert.Equal("07 43E8 03 800F4240 4040 4064 02".Replace(" ", "", StringComparison.Ordinal), Bytes.ToHex(b));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public void Bulk_Header_Round_Trip(bool hash, bool chunked)
    {
        BulkHeader h = SampleBulk(hash, chunked);
        byte[] b = new byte[StreamFraming.GetBulkHeaderLength(h) + 3];
        int n = StreamFraming.WriteBulkHeader(b, h);
        Assert.Equal(n + 3, b.Length);
        Assert.Equal(ParseStatus.Ok, StreamFraming.TryParseBulkHeader(b, 1000, out BulkHeader p, out int consumed));
        Assert.Equal(n, consumed);
        Assert.Equal(h.TransferId, p.TransferId);
        Assert.Equal(h.ObjectId, p.ObjectId);
        Assert.Equal(h.ObjectVersion, p.ObjectVersion);
        Assert.Equal(h.TotalLength, p.TotalLength);
        Assert.Equal(h.Offset, p.Offset);
        Assert.Equal(h.Length, p.Length);
        Assert.Equal(h.Flags, p.Flags);
        Assert.Equal(hash, p.HasChecksum);
        Assert.Equal(chunked, p.IsChunked);

        for (int length = 0; length < n; length++)
        {
            Assert.Equal(ParseStatus.Truncated, StreamFraming.TryParseBulkHeader(b.AsSpan(0, length), 1000, out _, out int c));
            Assert.Equal(0, c);
        }
    }

    [Fact]
    public void WriteBulkHeader_Validates()
    {
        BulkHeader ok = SampleBulk(false, false);
        BulkHeader zero = ok;
        zero.Length = 0;
        BulkHeader over = ok;
        over.Offset = ok.TotalLength - 10;
        over.Length = 11;
        BulkHeader offsetBeyond = ok;
        offsetBeyond.Offset = ok.TotalLength + 1;
        BulkHeader huge = ok;
        huge.TotalLength = VarInt.MaxValue + 1;
        BulkHeader flags = ok;
        flags.Flags = (BulkFlags)0x04;
        foreach (BulkHeader bad in new[] { zero, over, offsetBeyond, huge, flags })
        {
            Assert.Throws<ArgumentException>(() => StreamFraming.WriteBulkHeader(new byte[100], bad));
        }

        Assert.Throws<ArgumentException>(() => StreamFraming.WriteBulkHeader(new byte[5], ok));
        BulkHeader exact = ok;
        exact.Offset = 0;
        exact.Length = exact.TotalLength;
        Assert.True(StreamFraming.WriteBulkHeader(new byte[100], exact) > 0);
    }

    public static TheoryData<string, long, ParseStatus> BulkStatuses => new()
    {
        // TotalLength 0x3C = 60 (one-byte varints stay below 0x40).
        { "01 02 03 3C 00 0A 00", 1000, ParseStatus.Ok },
        { "01 02 03 3C 00 0A 00", 10, ParseStatus.Ok },
        { "01 02 03 3C 00 0A 00", 9, ParseStatus.MessageTooLarge },
        { "01 02 03 3C 00 0A 00", -1, ParseStatus.MessageTooLarge },
        { "01 02 03 3C 00 00 00", 1000, ParseStatus.BadBulkRange },          // Length = 0
        { "01 02 03 3C 32 0B 00", 1000, ParseStatus.BadBulkRange },          // 50 + 11 > 60
        { "01 02 03 3C 32 0A 00", 1000, ParseStatus.Ok },                    // 50 + 10 = 60
        { "01 02 03 3C 00 0A 04", 1000, ParseStatus.BadFlags },              // reserved bit 2
        { "01 02 03 3C 00 0A 0C", 1000, ParseStatus.BadFlags },              // reserved bits 2-3
        { "01 02 03 3C 00 0A 10", 1000, ParseStatus.BadFlags },              // reserved bit 4
        { "01 02 03 3C 00 0A 80", 1000, ParseStatus.BadFlags },
        { "01 02 03 3C 00 0A 01", 1000, ParseStatus.Ok },                    // ChecksumPresent: the trailer is after the body
        { "4001 02 03 3C 00 0A 00", 1000, ParseStatus.NonMinimalVarint },
        { "01 4002 03 3C 00 0A 00", 1000, ParseStatus.NonMinimalVarint },
        { "01 02 4003 3C 00 0A 00", 1000, ParseStatus.NonMinimalVarint },
        { "01 02 03 8000003C 00 0A 00", 1000, ParseStatus.NonMinimalVarint },
        { "01 02 03 3C 4000 0A 00", 1000, ParseStatus.NonMinimalVarint },
        { "01 02 03 3C 00 400A 00", 1000, ParseStatus.NonMinimalVarint },
        { "01 02 03 3C 00 0A", 1000, ParseStatus.Truncated },
        { "01 02 03 3C 00", 1000, ParseStatus.Truncated },
        { "01 02 03 3C", 1000, ParseStatus.Truncated },
        { "01 02 03", 1000, ParseStatus.Truncated },
        { "FFFFFFFFFFFFFFFF 00 00 FFFFFFFFFFFFFFFF 00 FFFFFFFFFFFFFFFF 00", long.MaxValue, ParseStatus.Ok },
        { "00 00 00 FFFFFFFFFFFFFFFF FFFFFFFFFFFFFFFF 01 00", long.MaxValue, ParseStatus.BadBulkRange },
    };

    [Theory]
    [MemberData(nameof(BulkStatuses))]
    public void Bulk_Header_Statuses(string hex, long maxLength, ParseStatus expected)
    {
        Assert.Equal(expected, StreamFraming.TryParseBulkHeader(Bytes.Hex(hex), maxLength, out _, out int consumed));
        if (expected != ParseStatus.Ok)
        {
            Assert.Equal(0, consumed);
        }
    }

    [Fact]
    public void Bulk_Chunk_Header()
    {
        byte[] b = new byte[16];
        int n = StreamFraming.WriteBulkChunkHeader(b, 100, 0);
        Assert.Equal("4064 00".Replace(" ", "", StringComparison.Ordinal), Bytes.ToHex(b.AsSpan(0, n)));
        Assert.Equal(n, StreamFraming.GetBulkChunkHeaderLength(100, 0));
        n = StreamFraming.WriteBulkChunkHeader(b, 1, 1 << 20);
        Assert.Equal("01 80100000".Replace(" ", "", StringComparison.Ordinal), Bytes.ToHex(b.AsSpan(0, n)));
        Assert.Throws<ArgumentException>(() => StreamFraming.WriteBulkChunkHeader(b, 0, 0));
        Assert.Throws<ArgumentException>(() => StreamFraming.WriteBulkChunkHeader(b, 5, -1));
        Assert.Throws<ArgumentException>(() => StreamFraming.WriteBulkChunkHeader(new byte[2], 64, 64));
    }

    [Fact]
    public void Fuzz_Stateless_Stream_Parsers_Never_Throw()
    {
        Random random = new(8080);
        ChannelDefinition[] channels = { Get(Ordered), Get(OrderedFull), Get(UnorderedKeyedLz4), Get(Latest), Get(LatestLz4), Get(UU), Get(BulkChannel) };
        for (int i = 0; i < 60_000; i++)
        {
            byte[] input = new byte[random.Next(0, 40)];
            random.NextBytes(input);
            ParseStatus s = StreamFraming.TryParseFrameHeader(input, channels[random.Next(channels.Length)], random.Next(0, 3) * 50, out StreamMessageHeader h, out int consumed);
            Assert.InRange(consumed, 0, input.Length);
            if (s == ParseStatus.Ok)
            {
                Assert.True(h.Length >= 0 && h.RawLength >= 0);
            }

            s = StreamFraming.TryParseControlFrameHeader(input, out _, out int body, out consumed);
            Assert.InRange(consumed, 0, input.Length);
            if (s == ParseStatus.Ok)
            {
                Assert.InRange(body, 0, 16383);
            }

            s = StreamFraming.TryParseBulkHeader(input, random.NextInt64(-5, long.MaxValue), out BulkHeader b, out consumed);
            Assert.InRange(consumed, 0, input.Length);
            if (s == ParseStatus.Ok)
            {
                Assert.True(b.Length > 0 && b.Offset + b.Length <= b.TotalLength);
            }
        }
    }
}
