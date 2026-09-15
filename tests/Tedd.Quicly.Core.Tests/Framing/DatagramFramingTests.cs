using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Primitives;
using static Tedd.Quicly.Core.Tests.Framing.TestTables;

namespace Tedd.Quicly.Core.Tests.Framing;

public class DatagramFramingTests
{
    private static byte[] Encode(int channelId, MessageHeader header, byte[] payload)
    {
        ChannelDefinition ch = Get(channelId);
        byte[] buffer = new byte[DatagramFraming.MaxHeaderLength + payload.Length];
        int n = DatagramFraming.WriteHeader(buffer, ch, header);
        Assert.Equal(DatagramFraming.GetHeaderLength(ch, header), n);
        payload.CopyTo(buffer, n);
        return buffer[..(n + payload.Length)];
    }

    private static ParseStatus Parse(byte[] datagram, out MessageHeader header, out int offset, int max = 0) =>
        DatagramFraming.TryParse(datagram, All, max, out header, out offset);

    // PROTOCOL.md §2.1 header cost examples plus one vector per shape.
    public static TheoryData<int, uint, ulong, byte, byte, int, string> Vectors => new()
    {
        { UU, 0, 0, 0, 0, 0, "02" },                                // 1 byte: unsequenced, unkeyed
        { USeq16, 0x1234, 0, 0, 0, 0, "03 3412" },                  // 3 bytes: 16-bit sequenced
        { USeq32Keyed, 0x01020304, 5, 0, 0, 0, "04 04030201 05" },  // 5 bytes: 32-bit + one-byte key
        { USeq16Keyed, 0xABCD, 300, 0, 0, 0, "05 CDAB 412C" },
        { UUFrag, 7, 0, 1, 0, 0, "06 0700 01" },
        { UUFrag, 7, 0, 3, 2, 0, "06 0700 03 02" },
        { USFragKeyed, 1, 64, 8, 7, 0, "07 01000000 4040 08 07" },
        { UULz4, 0, 0, 0, 0, 0, "08 00" },
        { UULz4, 0, 0, 0, 0, 1000, "08 43E8" },
        { USFragKeyedLz4, 2, 1, 2, 1, 5000, "09 02000000 01 02 01 5388" },
        { Latest, 0xFFFFFFFF, 16384, 0, 0, 0, "0A FFFFFFFF 80004000" },
        { LatestLz4, 9, 1UL << 30, 0, 0, 60000, "0B 09000000 C000000040000000 8000EA60" },
        { Wide, 3, 2, 0, 0, 0, "4064 03000000 02" },
        { WidePlain, 0, 0, 0, 0, 0, "40C8" },
        { MaxLatest, 1, 0, 0, 0, 0, "7FFF 01000000 00 00" },
    };

    [Theory]
    [MemberData(nameof(Vectors))]
    public void Header_Vectors_Write_And_Parse(int channelId, uint sequence, ulong key, byte fragCount, byte fragIndex, int rawLength, string hex)
    {
        MessageHeader header = new() { Sequence = sequence, Key = key, FragCount = fragCount, FragIndex = fragIndex, RawLength = rawLength };
        byte[] expected = Bytes.Hex(hex);
        ChannelDefinition ch = Get(channelId);
        Assert.Equal(expected.Length, DatagramFraming.GetHeaderLength(ch, header));

        byte[] buffer = new byte[DatagramFraming.MaxHeaderLength];
        int written = DatagramFraming.WriteHeader(buffer, ch, header);
        Assert.Equal(hex.Replace(" ", "", StringComparison.Ordinal), Bytes.ToHex(buffer.AsSpan(0, written)));

        // Payload long enough to satisfy the fragment/compression size rules, short enough for the limits.
        int payloadLength = rawLength > 0 ? 1 : 4;
        byte[] datagram = Bytes.Concat(expected, Bytes.Fill(payloadLength));
        Assert.Equal(ParseStatus.Ok, Parse(datagram, out MessageHeader parsed, out int offset));
        Assert.Equal(expected.Length, offset);
        Assert.Equal(channelId, parsed.Channel);
        Assert.Equal(ch.HasSequence ? (ch.SequenceBits == 16 ? (ushort)sequence : sequence) : 0u, parsed.Sequence);
        Assert.Equal(ch.Keyed ? key : 0UL, parsed.Key);
        Assert.Equal(fragCount == 0 ? 1 : fragCount, parsed.FragCount);
        Assert.Equal(fragCount > 1 ? fragIndex : 0, parsed.FragIndex);
        Assert.Equal(rawLength, parsed.RawLength);
        Assert.Equal(rawLength > 0, parsed.Compressed);
        Assert.Equal(fragCount > 1, parsed.IsFragment);
    }

    [Fact]
    public void Fields_The_Channel_Does_Not_Carry_Are_Ignored_When_Writing()
    {
        MessageHeader header = new() { Channel = 999, Sequence = 5, Key = ulong.MaxValue, FragCount = 200, FragIndex = 250, RawLength = 77 };
        byte[] buffer = new byte[4];
        Assert.Equal(1, DatagramFraming.WriteHeader(buffer, Get(UU), header));
        Assert.Equal(2, buffer[0]);
        Assert.Equal(1, DatagramFraming.GetHeaderLength(Get(UU), header));
    }

    [Fact]
    public void Sequence_Is_Truncated_To_16_Bits()
    {
        byte[] d = Encode(USeq16, new MessageHeader { Sequence = 0x0001_FFFE }, new byte[1]);
        Assert.Equal("03FEFF00", Bytes.ToHex(d));
        Assert.Equal(ParseStatus.Ok, Parse(d, out MessageHeader h, out _));
        Assert.Equal(0xFFFEu, h.Sequence);
    }

    [Theory]
    [InlineData(USeq32Keyed, 1UL << 62, 1, 0, 0)]
    [InlineData(UUFrag, 0UL, 9, 0, 0)]
    [InlineData(UUFrag, 0UL, 2, 2, 0)]
    [InlineData(UULz4, 0UL, 0, 0, -1)]
    [InlineData(UU, 0UL, 0, 0, -5)]
    public void WriteHeader_Rejects_Invalid_Headers(int channelId, ulong key, int fragCount, int fragIndex, int rawLength)
    {
        MessageHeader header = new() { Key = key, FragCount = (byte)fragCount, FragIndex = (byte)fragIndex, RawLength = rawLength };
        ArgumentException ex = Assert.Throws<ArgumentException>(() => DatagramFraming.WriteHeader(new byte[32], Get(channelId), header));
        Assert.Equal("header", ex.ParamName);
    }

    [Fact]
    public void WriteHeader_Rejects_Small_Destination()
    {
        MessageHeader header = new() { Key = 16384 };
        Assert.Equal(9, DatagramFraming.GetHeaderLength(Get(USeq32Keyed), header));
        ArgumentException ex = Assert.Throws<ArgumentException>(() => DatagramFraming.WriteHeader(new byte[8], Get(USeq32Keyed), header));
        Assert.Equal("destination", ex.ParamName);
        Assert.Equal(9, DatagramFraming.WriteHeader(new byte[9], Get(USeq32Keyed), header));
    }

    [Fact]
    public void Header_Length_Upper_Bound()
    {
        // Worst real shape: two-byte id, 32-bit sequence, 8-byte key, fragment pair, 2-byte RawLength.
        ChannelTable t = ChannelTable.Create().Add(9000, "w", ChannelMode.UnreliableSequenced, o =>
        {
            o.Keyed = true;
            o.Fragmentation = true;
            o.Compression = ChannelCompression.Lz4;
            o.MaxMessageSize = 8800;
        }).Build();
        MessageHeader h = new() { Key = VarInt.MaxValue, FragCount = 8, FragIndex = 7, RawLength = 8800 };
        Assert.Equal(18, DatagramFraming.GetHeaderLength(t[9000]!, h));
        h.RawLength = int.MaxValue;
        Assert.Equal(DatagramFraming.MaxHeaderLength, DatagramFraming.GetHeaderLength(t[9000]!, h));
    }

    [Fact]
    public void GetHeaderLength_Matches_Branchy_Reference_For_Random_Headers()
    {
        Random random = new(99);
        foreach (ChannelDefinition ch in All.All)
        {
            for (int i = 0; i < 500; i++)
            {
                MessageHeader h = RandomHeader(random);
                int expected = ch.ChannelIdLength
                    + (ch.HasSequence ? ch.SequenceBits / 8 : 0)
                    + (ch.Keyed ? VarInt.GetLength(h.Key) : 0)
                    + (ch.Fragmentation ? (h.FragCount > 1 ? 2 : 1) : 0)
                    + (ch.Compression != ChannelCompression.None ? VarInt.GetLength((ulong)h.RawLength) : 0);
                Assert.Equal(expected, DatagramFraming.GetHeaderLength(ch, h));
            }
        }
    }

    private static MessageHeader RandomHeader(Random random)
    {
        int keyBytes = 1 << random.Next(0, 4);
        ulong key = keyBytes switch
        {
            1 => (ulong)random.Next(0, 64),
            2 => (ulong)random.Next(64, 16384),
            4 => (ulong)random.Next(16384, 1 << 30),
            _ => (ulong)random.NextInt64(1L << 30, (long)VarInt.MaxValue),
        };
        byte fragCount = (byte)random.Next(1, 9);
        return new MessageHeader
        {
            Sequence = (uint)random.NextInt64(0, uint.MaxValue + 1L),
            Key = key,
            FragCount = fragCount,
            FragIndex = (byte)random.Next(0, fragCount),
            RawLength = random.Next(3) == 0 ? 0 : random.Next(1, 1 << random.Next(1, 20)),
        };
    }

    [Fact]
    public void Random_Round_Trip_Every_Datagram_Shape()
    {
        Random random = new(1234);
        foreach (ChannelDefinition ch in All.All)
        {
            if (!ch.IsDatagramMode)
            {
                continue;
            }

            for (int i = 0; i < 300; i++)
            {
                MessageHeader h = RandomHeader(random);
                if (!ch.Fragmentation)
                {
                    h.FragCount = 1;
                    h.FragIndex = 0;
                }

                int payloadLength = random.Next(1, 30);
                h.RawLength = ch.Compression == ChannelCompression.None || random.Next(2) == 0
                    ? 0
                    : (payloadLength * h.FragCount) + 1 + random.Next(0, 100);
                byte[] datagram = Encode(ch.Id, h, Bytes.Fill(payloadLength, (byte)i));
                Assert.Equal(ParseStatus.Ok, Parse(datagram, out MessageHeader p, out int offset));
                Assert.Equal(datagram.Length - payloadLength, offset);
                Assert.Equal(ch.Id, p.Channel);
                Assert.Equal(ch.HasSequence ? (ch.SequenceBits == 16 ? h.Sequence & 0xFFFF : h.Sequence) : 0, p.Sequence);
                Assert.Equal(ch.Keyed ? h.Key : 0, p.Key);
                Assert.Equal(h.FragCount, p.FragCount);
                Assert.Equal(h.FragCount > 1 ? h.FragIndex : 0, p.FragIndex);
                Assert.Equal(h.RawLength, p.RawLength);
            }
        }
    }

    public static TheoryData<string, ParseStatus> Statuses => new()
    {
        { "", ParseStatus.Truncated },
        { "40", ParseStatus.Truncated },
        { "4000", ParseStatus.NonMinimalVarint },
        { "4001", ParseStatus.NonMinimalVarint },
        { "4005 00", ParseStatus.NonMinimalVarint },
        { "403F", ParseStatus.NonMinimalVarint },
        { "4040", ParseStatus.UnknownChannel },
        { "7FFE", ParseStatus.UnknownChannel },
        { "80", ParseStatus.Truncated },
        { "800000", ParseStatus.Truncated },
        { "80000002", ParseStatus.NonMinimalVarint },
        { "80003FFF", ParseStatus.NonMinimalVarint },
        { "80004000", ParseStatus.UnknownChannel },
        { "C000000000000002", ParseStatus.NonMinimalVarint },
        { "C000000040000000", ParseStatus.UnknownChannel },
        { "C0000000", ParseStatus.Truncated },
        { "14", ParseStatus.UnknownChannel },
        { "3F", ParseStatus.UnknownChannel },
        { "0C 00", ParseStatus.ChannelNotDatagram },
        { "0E 00", ParseStatus.ChannelNotDatagram },
        { "10 00", ParseStatus.ChannelNotDatagram },
        { "412C", ParseStatus.ChannelNotDatagram },
        { "03", ParseStatus.Truncated },
        { "03 34", ParseStatus.Truncated },
        { "04 010203", ParseStatus.Truncated },
        { "04 01020304", ParseStatus.Truncated },
        { "04 01020304 40", ParseStatus.Truncated },
        { "04 01020304 4005", ParseStatus.NonMinimalVarint },
        { "04 01020304 80000005", ParseStatus.NonMinimalVarint },
        { "06 0100", ParseStatus.Truncated },
        { "06 0100 00 AA", ParseStatus.BadFragment },
        { "06 0100 09 00 AA", ParseStatus.BadFragment },
        { "06 0100 FF 00 AA", ParseStatus.BadFragment },
        { "06 0100 02", ParseStatus.Truncated },
        { "06 0100 02 02 AA", ParseStatus.BadFragment },
        { "06 0100 02 00", ParseStatus.BadFragment },
        { "06 0100 02 01", ParseStatus.BadFragment },
        { "08", ParseStatus.Truncated },
        { "08 40", ParseStatus.Truncated },
        { "08 4001 AA", ParseStatus.NonMinimalVarint },
        { "08 44B1 AA", ParseStatus.RawLengthTooLarge },
        { "08 05", ParseStatus.BadLength },
        { "08 03 AABBCC", ParseStatus.BadLength },
        { "08 03 AABBCCDD", ParseStatus.BadLength },
        { "08 03 AABB", ParseStatus.Ok },
        { "08 00", ParseStatus.Ok },
        { "02", ParseStatus.Ok },
    };

    [Theory]
    [MemberData(nameof(Statuses))]
    public void Every_Parse_Status(string hex, ParseStatus expected)
    {
        Assert.Equal(expected, Parse(Bytes.Hex(hex), out MessageHeader h, out int offset));
        if (expected != ParseStatus.Ok)
        {
            Assert.Equal(0, offset);
        }
    }

    [Theory]
    [InlineData("00", 0, ParseStatus.ControlChannel, 1)]
    [InlineData("00 01 AABB", 0, ParseStatus.ControlChannel, 1)]
    [InlineData("01", 1, ParseStatus.ContainerChannel, 1)]
    [InlineData("01 00 02", 1, ParseStatus.ContainerChannel, 1)]
    public void Control_And_Container_Channels_Are_Redirected(string hex, int channel, ParseStatus expected, int offset)
    {
        Assert.Equal(expected, Parse(Bytes.Hex(hex), out MessageHeader h, out int payloadOffset));
        Assert.Equal(channel, h.Channel);
        Assert.Equal(offset, payloadOffset);
    }

    [Fact]
    public void Four_Argument_Overload_Uses_Channel_Limit()
    {
        byte[] d = Bytes.Concat(new byte[] { 0x02 }, new byte[1200]);
        Assert.Equal(ParseStatus.Ok, DatagramFraming.TryParse(d, All, out _, out int offset));
        Assert.Equal(1, offset);
        d = Bytes.Concat(new byte[] { 0x02 }, new byte[1201]);
        Assert.Equal(ParseStatus.MessageTooLarge, DatagramFraming.TryParse(d, All, out _, out _));
    }

    [Theory]
    [InlineData(0, 1200, ParseStatus.Ok)]
    [InlineData(0, 1201, ParseStatus.MessageTooLarge)]
    [InlineData(500, 500, ParseStatus.Ok)]
    [InlineData(500, 501, ParseStatus.MessageTooLarge)]
    [InlineData(5000, 1201, ParseStatus.MessageTooLarge)]
    [InlineData(-3, 1200, ParseStatus.Ok)]
    [InlineData(1, 1, ParseStatus.Ok)]
    [InlineData(1, 2, ParseStatus.MessageTooLarge)]
    public void Session_Cap_Applies_Below_The_Channel_Limit(int cap, int payload, ParseStatus expected)
    {
        byte[] d = Bytes.Concat(new byte[] { 0x02 }, new byte[payload]);
        Assert.Equal(expected, Parse(d, out _, out _, cap));
    }

    [Theory]
    [InlineData(1, 0, 1100, ParseStatus.Ok)]              // unfragmented on a fragmenting channel
    [InlineData(1, 0, 8800, ParseStatus.Ok)]
    [InlineData(1, 0, 8801, ParseStatus.MessageTooLarge)]
    [InlineData(8, 0, 1257, ParseStatus.Ok)]              // non-last: 1257 * 7 + 1 = 8800
    [InlineData(8, 6, 1258, ParseStatus.MessageTooLarge)] // non-last: 1258 * 7 + 1 = 8807
    [InlineData(8, 7, 1100, ParseStatus.Ok)]              // last: 1100 * 8 = 8800
    [InlineData(8, 7, 1101, ParseStatus.MessageTooLarge)] // last: 1101 * 8 = 8808
    [InlineData(2, 1, 4400, ParseStatus.Ok)]
    [InlineData(2, 1, 4401, ParseStatus.MessageTooLarge)]
    public void Fragment_Implied_Size_Is_Checked_From_One_Fragment(int count, int index, int payload, ParseStatus expected)
    {
        byte[] d = Encode(UUFrag, new MessageHeader { FragCount = (byte)count, FragIndex = (byte)index }, new byte[payload]);
        Assert.Equal(expected, Parse(d, out _, out _));
    }

    [Theory]
    [InlineData(1, 0, 10, 11, ParseStatus.Ok)]
    [InlineData(1, 0, 10, 10, ParseStatus.BadLength)]         // compression did not shrink
    [InlineData(3, 0, 10, 22, ParseStatus.Ok)]                // 10 * 2 + 1 = 21 < 22
    [InlineData(3, 1, 10, 21, ParseStatus.BadLength)]         // 21 >= 21
    [InlineData(3, 2, 10, 31, ParseStatus.Ok)]                // last: 30 < 31
    [InlineData(3, 2, 10, 30, ParseStatus.BadLength)]
    [InlineData(3, 2, 10, 8801, ParseStatus.RawLengthTooLarge)]
    public void Compressed_Fragments_Must_Shrink(int count, int index, int payload, int raw, ParseStatus expected)
    {
        byte[] d = Encode(USFragKeyedLz4, new MessageHeader { Key = 1, FragCount = (byte)count, FragIndex = (byte)index, RawLength = raw }, new byte[payload]);
        Assert.Equal(expected, Parse(d, out _, out _));
    }

    [Fact]
    public void Fuzz_Random_Datagrams_Never_Throw_And_Ok_Headers_Reencode_Identically()
    {
        Random random = new(2024);
        byte[] buffer = new byte[DatagramFraming.MaxHeaderLength];
        int ok = 0;
        for (int i = 0; i < 100_000; i++)
        {
            byte[] input = new byte[random.Next(0, 24)];
            random.NextBytes(input);
            if (input.Length > 0 && random.Next(4) != 0)
            {
                input[0] = (byte)random.Next(0, 18); // mostly known channels
            }

            ParseStatus status = Parse(input, out MessageHeader h, out int offset, random.Next(3) == 0 ? random.Next(0, 50) : 0);
            Assert.InRange(offset, 0, input.Length);
            if (status == ParseStatus.Ok)
            {
                ok++;
                ChannelDefinition ch = All[h.Channel]!;
                int written = DatagramFraming.WriteHeader(buffer, ch, h);
                Assert.Equal(offset, written);
                Assert.Equal(Bytes.ToHex(input.AsSpan(0, offset)), Bytes.ToHex(buffer.AsSpan(0, written)));
            }
        }

        Assert.True(ok > 1000);
    }

    [Fact]
    public void Fuzz_Mutations_Of_Valid_Datagrams_Never_Throw()
    {
        Random random = new(77);
        List<byte[]> valid = new();
        foreach (ChannelDefinition ch in All.All)
        {
            if (ch.IsDatagramMode)
            {
                MessageHeader h = new() { Sequence = 77, Key = 1000, FragCount = (byte)(ch.Fragmentation ? 3 : 1), FragIndex = 1, RawLength = ch.Compression != 0 ? 100 : 0 };
                valid.Add(Encode(ch.Id, h, Bytes.Fill(20)));
            }
        }

        for (int i = 0; i < 50_000; i++)
        {
            byte[] input = (byte[])valid[random.Next(valid.Count)].Clone();
            switch (random.Next(3))
            {
                case 0:
                    input[random.Next(input.Length)] ^= (byte)(1 << random.Next(8));
                    break;
                case 1:
                    input = input[..random.Next(input.Length)];
                    break;
                default:
                    input[random.Next(input.Length)] = (byte)random.Next(256);
                    input = Bytes.Concat(input, Bytes.Fill(random.Next(0, 3000)));
                    break;
            }

            _ = Parse(input, out _, out int offset);
            Assert.InRange(offset, 0, input.Length);
        }
    }
}
