using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Framing;
using static Tedd.Quicly.Core.Tests.Framing.TestTables;

namespace Tedd.Quicly.Core.Tests.Framing;

public class StreamFrameParserTests
{
    private static string S(int len, uint seq = 0, ulong key = 0, uint rid = 0, int raw = 0, byte type = 0) =>
        $"S len={len} seq={seq} key={key} rid={rid} raw={raw} type={type}";

    private static string E(byte[] payload) => $"E {Convert.ToHexString(payload)}";

    private static string P(int channel, ulong group, StreamRole role) => $"P ch={channel} g={group} role={role}";

    private static string X(ParseStatus status) => $"X {status}";

    private static string F(ParseStatus status) => $"F {status}";

    private static readonly byte[] P5 = Bytes.Fill(5, 1);
    private static readonly byte[] P100 = Bytes.Fill(100, 3);
    private static readonly byte[] P1 = { 0x42 };

    private static byte[] OrderedStream() => new StreamBuilder()
        .Preamble(Ordered)
        .Frame(Get(Ordered), default, P5)
        .Frame(Get(Ordered), default, Array.Empty<byte>())
        .Frame(Get(Ordered), default, P100)
        .Frame(Get(Ordered), default, P1)
        .ToArray();

    [Fact]
    public void Ordered_Stream_Events()
    {
        List<string> log = StreamDriver.Run(StreamRole.Unknown, All, OrderedStream());
        Assert.Equal(new[]
        {
            P(Ordered, 0, StreamRole.Ordered),
            S(5), E(P5),
            S(0), E(Array.Empty<byte>()),
            S(100), E(P100),
            S(1), E(P1),
            F(ParseStatus.Ok),
        }, log);
    }

    [Fact]
    public void Ordered_Stream_With_Key_RequestId_And_Compression()
    {
        byte[] stream = new StreamBuilder()
            .Preamble(OrderedFull)
            .Frame(Get(OrderedFull), new StreamMessageHeader { Key = 7, RequestId = 1 }, P5)
            .Frame(Get(OrderedFull), new StreamMessageHeader { Key = 1UL << 40, RequestId = 2, RawLength = 500 }, P100)
            .ToArray();
        Assert.Equal(new[]
        {
            P(OrderedFull, 0, StreamRole.Ordered),
            S(5, key: 7, rid: 1), E(P5),
            S(100, key: 1UL << 40, rid: 2, raw: 500), E(P100),
            F(ParseStatus.Ok),
        }, StreamDriver.Run(StreamRole.Ordered, All, stream));
    }

    [Fact]
    public void Group_Stream_Events()
    {
        byte[] stream = new StreamBuilder()
            .GroupPreamble(UnorderedKeyedLz4, 12345)
            .Frame(Get(UnorderedKeyedLz4), new StreamMessageHeader { Key = 3 }, P5)
            .Frame(Get(UnorderedKeyedLz4), new StreamMessageHeader { Key = 4, RawLength = 101 }, P100)
            .ToArray();
        Assert.Equal(new[]
        {
            P(UnorderedKeyedLz4, 12345, StreamRole.Group),
            S(5, key: 3), E(P5),
            S(100, key: 4, raw: 101), E(P100),
            F(ParseStatus.Ok),
        }, StreamDriver.Run(StreamRole.Group, All, stream));
    }

    [Fact]
    public void Empty_Group_Stream_May_End_After_Preamble()
    {
        byte[] stream = new StreamBuilder().GroupPreamble(Unordered, 1).ToArray();
        Assert.Equal(new[] { P(Unordered, 1, StreamRole.Group), F(ParseStatus.Ok) }, StreamDriver.Run(StreamRole.Unknown, All, stream));
    }

    private static byte[] LatestStream(uint version, uint sequence, bool compressed = false, int extraMessages = 0)
    {
        int channel = compressed ? LatestLz4 : Latest;
        StreamBuilder b = new StreamBuilder()
            .GroupPreamble((ushort)channel, version)
            .Frame(Get(channel), new StreamMessageHeader { Sequence = sequence, Key = 99, RawLength = compressed ? 200 : 0 }, P100);
        for (int i = 0; i < extraMessages; i++)
        {
            b.Frame(Get(channel), new StreamMessageHeader { Sequence = sequence, Key = 99 }, P1);
        }

        return b.ToArray();
    }

    [Fact]
    public void Latest_Group_Stream_Holds_Exactly_One_Message()
    {
        Assert.Equal(new[]
        {
            P(Latest, 0xFFFFFFFF, StreamRole.Group),
            S(100, seq: 0xFFFFFFFF, key: 99), E(P100),
            F(ParseStatus.Ok),
        }, StreamDriver.Run(StreamRole.Unknown, All, LatestStream(0xFFFFFFFF, 0xFFFFFFFF)));

        Assert.Equal(new[]
        {
            P(LatestLz4, 7, StreamRole.Group),
            S(100, seq: 7, key: 99, raw: 200), E(P100),
            F(ParseStatus.Ok),
        }, StreamDriver.Run(StreamRole.Group, All, LatestStream(7, 7, compressed: true)));

        List<string> log = StreamDriver.Run(StreamRole.Unknown, All, LatestStream(7, 7, extraMessages: 1));
        Assert.Equal(X(ParseStatus.TooManyMessages), log[^1]);
        Assert.Contains(E(P100), log);

        log = StreamDriver.Run(StreamRole.Unknown, All, LatestStream(7, 8));
        Assert.Equal(new[] { P(Latest, 7, StreamRole.Group), X(ParseStatus.SequenceMismatch) }, log);

        byte[] preambleOnly = new StreamBuilder().GroupPreamble(Latest, 7).ToArray();
        Assert.Equal(F(ParseStatus.Truncated), StreamDriver.Run(StreamRole.Unknown, All, preambleOnly)[^1]);

        byte[] tooLargeVersion = new StreamBuilder().GroupPreamble(Latest, 1UL << 32).ToArray();
        Assert.Equal(new[] { X(ParseStatus.ValueOutOfRange) }, StreamDriver.Run(StreamRole.Unknown, All, tooLargeVersion));
    }

    private static byte[] ControlStream() => new StreamBuilder()
        .Preamble(0)
        .Control(0x10, Bytes.Fill(30))
        .Control(0x16, Array.Empty<byte>())
        .Control(0x01, new byte[] { 1, 2, 3, 4 })
        .ToArray();

    [Fact]
    public void Control_Stream_Events()
    {
        Assert.Equal(new[]
        {
            P(0, 0, StreamRole.Control),
            S(30, type: 0x10), E(Bytes.Fill(30)),
            S(0, type: 0x16), E(Array.Empty<byte>()),
            S(4, type: 0x01), E(new byte[] { 1, 2, 3, 4 }),
            F(ParseStatus.Ok),
        }, StreamDriver.Run(StreamRole.Control, null, ControlStream()));
    }

    [Fact]
    public void Control_Stream_Frame_Limits()
    {
        byte[] max = new StreamBuilder().Preamble(0).Control(0x10, new byte[16383]).ToArray();
        List<string> log = StreamDriver.Run(StreamRole.Control, All, max);
        Assert.Equal(S(16383, type: 0x10), log[1]);
        Assert.Equal(F(ParseStatus.Ok), log[^1]);

        Assert.Equal(X(ParseStatus.BadLength), StreamDriver.Run(StreamRole.Control, All, Bytes.Hex("00 00 10"))[^1]);
        Assert.Equal(X(ParseStatus.BadLength), StreamDriver.Run(StreamRole.Control, All, Bytes.Hex("00 80004001 10"))[^1]);
        Assert.Equal(X(ParseStatus.RoleMismatch), StreamDriver.Run(StreamRole.Control, All, Bytes.Hex("0C"))[^1]);
        Assert.Equal(X(ParseStatus.NonMinimalVarint), StreamDriver.Run(StreamRole.Control, All, Bytes.Hex("4000"))[^1]);
        Assert.Equal(F(ParseStatus.Truncated), StreamDriver.Run(StreamRole.Control, All, Bytes.Hex("00 05 10 AA"))[^1]);
        Assert.Equal(F(ParseStatus.Truncated), StreamDriver.Run(StreamRole.Control, All, Bytes.Hex("00 40"))[^1]);
        Assert.Equal(F(ParseStatus.Truncated), StreamDriver.Run(StreamRole.Control, All, Array.Empty<byte>())[^1]);
    }

    private static byte[] BulkStream(bool chunked, bool hash, params (int Raw, byte[] Bytes)[] chunks)
    {
        ulong length = chunked ? (ulong)chunks.Sum(c => c.Raw == 0 ? c.Bytes.Length : c.Raw) : (ulong)chunks[0].Bytes.Length;
        BulkHeader header = StreamFramingTests.SampleBulk(hash, chunked, length);
        StreamBuilder b = new StreamBuilder().Preamble(BulkChannel).Bulk(header);
        foreach ((int raw, byte[] bytes) in chunks)
        {
            if (chunked)
            {
                b.Chunk(raw, bytes);
            }
            else
            {
                b.Raw(bytes);
            }
        }

        return b.ToArray();
    }

    private static string B(bool hash, bool chunked, ulong length)
    {
        BulkHeader h = StreamFramingTests.SampleBulk(hash, chunked, length);
        ReadOnlySpan<byte> digest = h.Hash;
        return $"B t=7 o=1000 v=3 total=1000000 off=64 len={length} flags={h.Flags} hash={(hash ? Convert.ToHexString(digest) : "-")}";
    }

    [Fact]
    public void Bulk_Unchunked_Body_Is_One_Message()
    {
        byte[] stream = BulkStream(chunked: false, hash: true, (0, P100));
        Assert.Equal(new[]
        {
            P(BulkChannel, 0, StreamRole.Bulk),
            B(true, false, 100),
            S(100), E(P100),
            F(ParseStatus.Ok),
        }, StreamDriver.Run(StreamRole.Bulk, All, stream));

        Assert.Equal(X(ParseStatus.BadLength), StreamDriver.Run(StreamRole.Unknown, All, Bytes.Concat(stream, P1))[^1]);
        Assert.Equal(F(ParseStatus.Truncated), StreamDriver.Run(StreamRole.Unknown, All, stream[..^1])[^1]);
        Assert.Equal(F(ParseStatus.Truncated), StreamDriver.Run(StreamRole.Unknown, All, stream[..10])[^1]);
    }

    [Fact]
    public void Bulk_Chunked_Body_Is_One_Message_Per_Chunk()
    {
        byte[] stream = BulkStream(chunked: true, hash: false, (0, P5), (300, P100), (0, P1));
        Assert.Equal(new[]
        {
            P(BulkChannel, 0, StreamRole.Bulk),
            B(false, true, 306),
            S(5), E(P5),
            S(100, raw: 300), E(P100),
            S(1), E(P1),
            F(ParseStatus.Ok),
        }, StreamDriver.Run(StreamRole.Unknown, All, stream));

        Assert.Equal(X(ParseStatus.BadLength), StreamDriver.Run(StreamRole.Unknown, All, Bytes.Concat(stream, Bytes.Hex("01 00 AA")))[^1]);
        Assert.Equal(F(ParseStatus.Truncated), StreamDriver.Run(StreamRole.Unknown, All, stream[..^2])[^1]);
    }

    public static TheoryData<string, string> BulkChunkErrors => new()
    {
        // Transfer Length = 60, bulkMaxChunk = 64.
        { "00 00", nameof(ParseStatus.BadLength) },            // empty chunk
        { "05 05 AABBCCDDEE", nameof(ParseStatus.BadLength) }, // compressed chunk did not shrink
        { "05 3D AABBCCDDEE", nameof(ParseStatus.BadLength) }, // decodes past Length (61 > 60)
        { "3D 00", nameof(ParseStatus.BadLength) },            // stored chunk past Length (61 > 60)
        { "4041 00", nameof(ParseStatus.MessageTooLarge) },    // chunk 65 > max chunk 64
        { "05 4041", nameof(ParseStatus.MessageTooLarge) },    // raw 65 > max chunk 64
        { "4005 00", nameof(ParseStatus.NonMinimalVarint) },
        { "05 4001", nameof(ParseStatus.NonMinimalVarint) },
    };

    [Theory]
    [MemberData(nameof(BulkChunkErrors))]
    public void Bulk_Chunk_Validation(string chunkHex, string expected)
    {
        BulkHeader header = StreamFramingTests.SampleBulk(hash: false, chunked: true, length: 60);
        byte[] stream = new StreamBuilder().Preamble(BulkChannel).Bulk(header).Raw(Bytes.Hex(chunkHex)).ToArray();
        List<string> log = StreamDriver.Run(StreamRole.Unknown, All, stream, bulkMaxChunk: 64);
        Assert.Equal($"X {expected}", log[^1]);
    }

    [Fact]
    public void Bulk_Header_Is_Validated_Through_The_Parser()
    {
        BulkHeader tooLong = StreamFramingTests.SampleBulk(false, false, 1001);
        byte[] stream = new StreamBuilder().Preamble(BulkChannel).Bulk(tooLong).ToArray();
        Assert.Equal(X(ParseStatus.MessageTooLarge), StreamDriver.Run(StreamRole.Unknown, All, stream, maxMessageSize: 1000)[^1]);
        Assert.Equal(F(ParseStatus.Truncated), StreamDriver.Run(StreamRole.Unknown, All, stream, maxMessageSize: 1001)[^1]);

        Assert.Equal(X(ParseStatus.BadBulkRange), StreamDriver.Run(StreamRole.Unknown, All, Bytes.Hex("10 01 02 03 3C 32 0B 00"))[^1]);
        Assert.Equal(X(ParseStatus.BadFlags), StreamDriver.Run(StreamRole.Unknown, All, Bytes.Hex("10 01 02 03 3C 00 0A 04"))[^1]);
        Assert.Equal(X(ParseStatus.NonMinimalVarint), StreamDriver.Run(StreamRole.Unknown, All, Bytes.Hex("10 4001"))[^1]);
        Assert.Equal(F(ParseStatus.Truncated), StreamDriver.Run(StreamRole.Unknown, All, Bytes.Hex("10 01 02 03 3C 00 0A 01 AABB"))[^1]);
    }

    public static TheoryData<StreamRole, string, string> PreambleErrors => new()
    {
        { StreamRole.Unknown, "00", nameof(ParseStatus.ChannelNotStream) },
        { StreamRole.Unknown, "01", nameof(ParseStatus.ChannelNotStream) },
        { StreamRole.Unknown, "02", nameof(ParseStatus.ChannelNotStream) },
        { StreamRole.Unknown, "03", nameof(ParseStatus.ChannelNotStream) },
        { StreamRole.Unknown, "06", nameof(ParseStatus.ChannelNotStream) },
        { StreamRole.Unknown, "40C8", nameof(ParseStatus.ChannelNotStream) },
        { StreamRole.Unknown, "14", nameof(ParseStatus.UnknownChannel) },
        { StreamRole.Unknown, "7FFE", nameof(ParseStatus.UnknownChannel) },
        { StreamRole.Unknown, "80004000", nameof(ParseStatus.UnknownChannel) },
        { StreamRole.Unknown, "C000000040000000", nameof(ParseStatus.UnknownChannel) },
        { StreamRole.Unknown, "4001", nameof(ParseStatus.NonMinimalVarint) },
        { StreamRole.Unknown, "400C", nameof(ParseStatus.NonMinimalVarint) },
        { StreamRole.Unknown, "0E 4001", nameof(ParseStatus.NonMinimalVarint) },
        { StreamRole.Ordered, "0E 00", nameof(ParseStatus.RoleMismatch) },
        { StreamRole.Group, "0C", nameof(ParseStatus.RoleMismatch) },
        { StreamRole.Bulk, "0C", nameof(ParseStatus.RoleMismatch) },
        { StreamRole.Ordered, "10", nameof(ParseStatus.RoleMismatch) },
        { StreamRole.Control, "02", nameof(ParseStatus.RoleMismatch) },
        { StreamRole.Control, "01", nameof(ParseStatus.RoleMismatch) },
    };

    [Theory]
    [MemberData(nameof(PreambleErrors))]
    public void Preamble_Validation(StreamRole role, string hex, string expected)
    {
        List<string> log = StreamDriver.Run(role, All, Bytes.Hex(hex));
        Assert.Equal(new[] { $"X {expected}" }, log);
    }

    [Fact]
    public void Preamble_With_Null_Table_Or_Default_Parser_Is_Unknown()
    {
        Assert.Equal(new[] { X(ParseStatus.UnknownChannel) }, StreamDriver.Run(StreamRole.Unknown, null, Bytes.Hex("0C")));

        StreamFrameParser parser = default;
        Assert.Equal(new[] { X(ParseStatus.UnknownChannel) }, StreamDriver.Run(ref parser, new[] { Bytes.Hex("0C 00") }));
    }

    [Fact]
    public void Wide_Channel_Preamble()
    {
        byte[] stream = new StreamBuilder().Preamble(WideOrdered).Frame(Get(WideOrdered), default, P5).ToArray();
        Assert.Equal(new[] { P(WideOrdered, 0, StreamRole.Ordered), S(5), E(P5), F(ParseStatus.Ok) }, StreamDriver.Run(StreamRole.Unknown, All, stream));
    }

    [Fact]
    public void Message_Size_Limits()
    {
        byte[] stream = new StreamBuilder().Preamble(Ordered).Frame(Get(Ordered), default, P100).ToArray();
        Assert.Equal(X(ParseStatus.MessageTooLarge), StreamDriver.Run(StreamRole.Unknown, All, stream, maxMessageSize: 99)[^1]);
        Assert.Equal(F(ParseStatus.Ok), StreamDriver.Run(StreamRole.Unknown, All, stream, maxMessageSize: 100)[^1]);
        Assert.Equal(X(ParseStatus.MessageTooLarge), StreamDriver.Run(StreamRole.Unknown, All, Bytes.Hex("0C 80010001"))[^1]);

        byte[] compressed = new StreamBuilder().Preamble(OrderedFull).Frame(Get(OrderedFull), new StreamMessageHeader { RawLength = 150 }, P100).ToArray();
        Assert.Equal(X(ParseStatus.RawLengthTooLarge), StreamDriver.Run(StreamRole.Unknown, All, compressed, maxMessageSize: 149)[^1]);
        Assert.Equal(F(ParseStatus.Ok), StreamDriver.Run(StreamRole.Unknown, All, compressed, maxMessageSize: 150)[^1]);
    }

    [Fact]
    public void Properties_Reflect_The_Stream()
    {
        StreamFrameParser parser = default;
        parser.Reset(All, maxMessageSize: 1000);
        Assert.Equal(StreamRole.Unknown, parser.Role);
        Assert.Null(parser.Definition);
        ReadOnlySpan<byte> input = new StreamBuilder().GroupPreamble(UnorderedKeyedLz4, 5).Frame(Get(UnorderedKeyedLz4), new StreamMessageHeader { Key = 1 }, P100).ToArray();
        Assert.Equal(StreamEvent.Preamble, parser.Read(ref input, out _));
        Assert.Same(Get(UnorderedKeyedLz4), parser.Definition);
        Assert.Equal(1000, parser.MaxMessageSize);
        Assert.Equal(5UL, parser.GroupId);
        Assert.Equal(StreamEvent.MessageStart, parser.Read(ref input, out _));
        Assert.Equal(100, parser.RemainingPayload);
        Assert.Equal(ParseStatus.Ok, parser.Error);
        Assert.Equal(ParseStatus.Truncated, parser.Finish());
        Assert.Equal(StreamEvent.PayloadChunk, parser.Read(ref input, out ReadOnlySpan<byte> payload));
        Assert.Equal(100, payload.Length);
        Assert.True(input.IsEmpty);
        Assert.Equal(ParseStatus.Truncated, parser.Finish());
        Assert.Equal(StreamEvent.MessageEnd, parser.Read(ref input, out _));
        Assert.Equal(StreamEvent.NeedMore, parser.Read(ref input, out _));
        Assert.Equal(StreamEvent.NeedMore, parser.Read(ref input, out _));
        Assert.Equal(ParseStatus.Ok, parser.Finish());
    }

    [Fact]
    public void Payload_Is_A_Slice_Of_The_Input()
    {
        byte[] stream = OrderedStream();
        StreamFrameParser parser = default;
        parser.Reset(All);
        ReadOnlySpan<byte> input = stream;
        parser.Read(ref input, out _); // preamble
        parser.Read(ref input, out _); // start
        Assert.Equal(StreamEvent.PayloadChunk, parser.Read(ref input, out ReadOnlySpan<byte> payload));
        Assert.True(System.Runtime.CompilerServices.Unsafe.AreSame(
            ref System.Runtime.InteropServices.MemoryMarshal.GetReference(payload), ref stream[2]));
    }

    [Fact]
    public void Error_Is_Sticky_Until_Reset()
    {
        StreamFrameParser parser = default;
        parser.Reset(All);
        ReadOnlySpan<byte> input = Bytes.Hex("02 0C 05");
        Assert.Equal(StreamEvent.Error, parser.Read(ref input, out _));
        Assert.Equal(ParseStatus.ChannelNotStream, parser.Error);
        int remaining = input.Length;
        Assert.Equal(StreamEvent.Error, parser.Read(ref input, out _));
        Assert.Equal(remaining, input.Length);
        Assert.Equal(ParseStatus.ChannelNotStream, parser.Finish());

        parser.Reset(StreamRole.Unknown, All);
        Assert.Equal(ParseStatus.Ok, parser.Error);
        input = OrderedStream();
        Assert.Equal(StreamEvent.Preamble, parser.Read(ref input, out _));
    }

    [Fact]
    public void Reset_Validates_Arguments()
    {
        StreamFrameParser parser = default;
        Assert.Throws<ArgumentOutOfRangeException>(() => { StreamFrameParser p = default; p.Reset((StreamRole)5, All); });
        Assert.Throws<ArgumentOutOfRangeException>(() => { StreamFrameParser p = default; p.Reset(StreamRole.Unknown, All, -1); });
        Assert.Throws<ArgumentOutOfRangeException>(() => { StreamFrameParser p = default; p.Reset(StreamRole.Unknown, All, 0, 0); });
        parser.Reset(StreamRole.Bulk, All, 0, 1);
        Assert.Equal(StreamRole.Unknown, parser.Role);
    }

    [Fact]
    public void Finish_Requires_A_Clean_Boundary()
    {
        byte[] stream = OrderedStream();
        Assert.Equal(F(ParseStatus.Truncated), StreamDriver.Run(StreamRole.Unknown, All, Array.Empty<byte>())[^1]);
        Assert.Equal(F(ParseStatus.Truncated), StreamDriver.Run(StreamRole.Unknown, All, stream[..3])[^1]);   // inside payload
        Assert.Equal(F(ParseStatus.Truncated), StreamDriver.Run(StreamRole.Unknown, All, Bytes.Hex("0C 40"))[^1]); // inside header
        Assert.Equal(F(ParseStatus.Truncated), StreamDriver.Run(StreamRole.Unknown, All, Bytes.Hex("40"))[^1]);    // inside preamble
        Assert.Equal(F(ParseStatus.Ok), StreamDriver.Run(StreamRole.Unknown, All, Bytes.Hex("0C"))[^1]);
    }

    public static TheoryData<string> SplitStreams => new()
    {
        "ordered", "orderedfull", "wide", "group", "latest", "latestlz4", "bulk", "bulkchunked", "control", "bigheaders",
    };

    private static byte[] Named(string name) => name switch
    {
        "ordered" => OrderedStream(),
        "orderedfull" => new StreamBuilder()
            .Preamble(OrderedFull)
            .Frame(Get(OrderedFull), new StreamMessageHeader { Key = 1UL << 61, RequestId = uint.MaxValue, RawLength = 70 }, Bytes.Fill(64))
            .Frame(Get(OrderedFull), new StreamMessageHeader { Key = 1, RequestId = 0 }, Array.Empty<byte>())
            .Frame(Get(OrderedFull), new StreamMessageHeader { Key = 16384, RequestId = 16384, RawLength = 0 }, P5)
            .ToArray(),
        "wide" => new StreamBuilder().Preamble(WideOrdered).Frame(Get(WideOrdered), default, P100).Frame(Get(WideOrdered), default, P1).ToArray(),
        "group" => new StreamBuilder()
            .GroupPreamble(UnorderedKeyedLz4, 1UL << 50)
            .Frame(Get(UnorderedKeyedLz4), new StreamMessageHeader { Key = 1 << 20 }, P100)
            .Frame(Get(UnorderedKeyedLz4), new StreamMessageHeader { Key = 2, RawLength = 9 }, P5)
            .ToArray(),
        "latest" => LatestStream(123456, 123456),
        "latestlz4" => LatestStream(uint.MaxValue, uint.MaxValue, compressed: true),
        "bulk" => BulkStream(chunked: false, hash: true, (0, Bytes.Fill(200))),
        "bulkchunked" => BulkStream(chunked: true, hash: true, (0, P5), (1000, P100), (0, P1), (70, Bytes.Fill(69))),
        "control" => ControlStream(),
        _ => new StreamBuilder() // 32-byte message headers back to back
            .Preamble(OrderedFull)
            .Frame(Get(OrderedFull), new StreamMessageHeader { Key = (1UL << 62) - 1, RequestId = 1U << 31, RawLength = 1 << 30 >> 14 }, Bytes.Fill(10))
            .Frame(Get(OrderedFull), new StreamMessageHeader { Key = (1UL << 62) - 1, RequestId = 1U << 31, RawLength = 65536 }, Bytes.Fill(1))
            .ToArray(),
    };

    [Theory]
    [MemberData(nameof(SplitStreams))]
    public void Every_Two_Way_Split_Gives_Identical_Events(string name)
    {
        byte[] stream = Named(name);
        List<string> baseline = StreamDriver.Run(StreamRole.Unknown, All, stream);
        if (name == "control")
        {
            baseline = StreamDriver.Run(StreamRole.Control, All, stream);
        }

        Assert.Equal(F(ParseStatus.Ok), baseline[^1]);
        StreamRole role = name == "control" ? StreamRole.Control : StreamRole.Unknown;
        for (int cut = 0; cut <= stream.Length; cut++)
        {
            Assert.Equal(baseline, StreamDriver.RunSegments(role, All, StreamDriver.SplitAt(stream, cut)));
        }

        Assert.Equal(baseline, StreamDriver.RunSegments(role, All, StreamDriver.ByteByByte(stream)));
    }

    [Theory]
    [MemberData(nameof(SplitStreams))]
    public void Random_Multi_Way_Splits_Give_Identical_Events(string name)
    {
        byte[] stream = Named(name);
        StreamRole role = name == "control" ? StreamRole.Control : StreamRole.Unknown;
        List<string> baseline = StreamDriver.Run(role, All, stream);
        Random random = new(name.Length * 7919);
        for (int i = 0; i < 300; i++)
        {
            List<byte[]> segments = StreamDriver.RandomSplit(stream, random, 16).ToList();
            Assert.Equal(baseline, StreamDriver.RunSegments(role, All, segments));
        }
    }

    [Fact]
    public void Errors_Are_Split_Invariant()
    {
        byte[] bad = Bytes.Concat(OrderedStream(), Bytes.Hex("80010001"));
        List<string> baseline = StreamDriver.Run(StreamRole.Unknown, All, bad);
        Assert.Equal(X(ParseStatus.MessageTooLarge), baseline[^1]);
        for (int cut = 0; cut <= bad.Length; cut++)
        {
            Assert.Equal(baseline, StreamDriver.RunSegments(StreamRole.Unknown, All, StreamDriver.SplitAt(bad, cut)));
        }
    }

    [Fact]
    public void Many_Small_Messages_Across_Odd_Segments()
    {
        StreamBuilder b = new StreamBuilder().Preamble(Ordered);
        for (int i = 0; i < 500; i++)
        {
            b.Frame(Get(Ordered), default, Bytes.Fill(i % 70, (byte)i));
        }

        byte[] stream = b.ToArray();
        List<string> baseline = StreamDriver.Run(StreamRole.Unknown, All, stream);
        Assert.Equal(1 + (500 * 2) + 1, baseline.Count);
        foreach (int size in new[] { 1, 2, 3, 7, 31, 32, 33, 1350 })
        {
            List<byte[]> segments = new();
            for (int offset = 0; offset < stream.Length; offset += size)
            {
                segments.Add(stream[offset..Math.Min(stream.Length, offset + size)]);
            }

            Assert.Equal(baseline, StreamDriver.RunSegments(StreamRole.Unknown, All, segments));
        }
    }

    [Fact]
    public void Fuzz_Random_Streams_Never_Throw_And_Always_Progress()
    {
        Random random = new(31337);
        byte[][] prefixes = { Bytes.Hex("0C"), Bytes.Hex("0D"), Bytes.Hex("0E 05"), Bytes.Hex("0F 05"), Bytes.Hex("0A 05"), Bytes.Hex("0B 05"), Bytes.Hex("10"), Bytes.Hex("00") };
        for (int i = 0; i < 20_000; i++)
        {
            int kind = random.Next(prefixes.Length);
            byte[] body = new byte[random.Next(0, 120)];
            random.NextBytes(body);
            for (int j = 0; j < body.Length; j++)
            {
                if (random.Next(3) == 0)
                {
                    body[j] &= 0x3F; // favour short varints so frames parse further
                }
            }

            byte[] stream = Bytes.Concat(prefixes[kind], body);
            StreamRole role = kind == 7 ? StreamRole.Control : StreamRole.Unknown;
            List<string> log = StreamDriver.RunSegments(role, All, StreamDriver.RandomSplit(stream, random, 6).ToList(), random.Next(2) * 100, 1 + random.Next(0, 200));
            Assert.NotEmpty(log);
        }
    }

    [Fact]
    public void Fuzz_Mutated_Valid_Streams_Never_Throw()
    {
        Random random = new(4242);
        string[] names = { "ordered", "orderedfull", "group", "latest", "bulk", "bulkchunked", "control", "bigheaders" };
        for (int i = 0; i < 10_000; i++)
        {
            string name = names[random.Next(names.Length)];
            byte[] stream = (byte[])Named(name).Clone();
            int mutations = random.Next(1, 4);
            for (int m = 0; m < mutations; m++)
            {
                stream[random.Next(stream.Length)] = (byte)random.Next(256);
            }

            StreamRole role = name == "control" ? StreamRole.Control : StreamRole.Unknown;
            List<string> whole = StreamDriver.Run(role, All, stream);
            List<string> split = StreamDriver.RunSegments(role, All, StreamDriver.RandomSplit(stream, random, 8).ToList());
            Assert.Equal(whole, split);
        }
    }
}
