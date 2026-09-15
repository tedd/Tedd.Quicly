using System.Text;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Primitives;
using Tedd.Quicly.Core.Tests.Framing;

namespace Tedd.Quicly.Core.Tests.Channels;

public class ChannelTableCodecTests
{
    // Golden vectors, also published in docs/protocol-vectors.md. Changing any of these breaks interoperability.
    internal const string ReferenceCanonicalHex =
        "05" +
        "02 01 03 C8 44B0" +
        "03 02 08 80 80010000" +
        "05 00 04 80 6260" +
        "0A 04 33 80 80010000" +
        "4040 05 00 10 81000000";

    internal const string ReferenceNamesHex =
        "04 6D6F7665" +   // move
        "04 63686174" +   // chat
        "02 6678" +       // fx
        "05 7374617465" + // state
        "05 776F726C64";  // world

    internal const ulong ReferenceHash = 0xB9ACCCD14847183CUL; // 13379293792243750972

    [Fact]
    public void Golden_Canonical_Encoding()
    {
        ChannelTable table = TestTables.Reference();
        byte[] expected = Bytes.Hex(ReferenceCanonicalHex);
        Assert.Equal(38, expected.Length);
        Assert.Equal(expected.Length, ChannelTableCodec.GetCanonicalLength(table));
        byte[] actual = new byte[64];
        int written = ChannelTableCodec.WriteCanonical(table, actual);
        Assert.Equal(Bytes.ToHex(expected), Bytes.ToHex(actual.AsSpan(0, written)));
    }

    [Fact]
    public void Golden_Hash()
    {
        ChannelTable table = TestTables.Reference();
        byte[] canonical = Bytes.Hex(ReferenceCanonicalHex);
        ulong reference = System.IO.Hashing.XxHash64.HashToUInt64(canonical);
        Assert.Equal(reference, table.Hash);
        Assert.Equal(reference, ChannelTableCodec.ComputeHash(table));
        Assert.Equal(ReferenceHash, table.Hash);
    }

    [Fact]
    public void Golden_Table_Section_With_Names()
    {
        ChannelTable table = TestTables.Reference();
        byte[] expected = Bytes.Hex(ReferenceCanonicalHex + ReferenceNamesHex);
        Assert.Equal(expected.Length, ChannelTableCodec.GetLengthWithNames(table));
        byte[] actual = new byte[expected.Length];
        Assert.Equal(expected.Length, ChannelTableCodec.WriteWithNames(table, actual));
        Assert.Equal(Bytes.ToHex(expected), Bytes.ToHex(actual));
    }

    [Fact]
    public void Golden_64_Byte_Name_Uses_A_Two_Byte_Length()
    {
        // docs/protocol-vectors.md, "Name of exactly 64 bytes".
        string name = new('a', ChannelDefinition.MaxNameBytes);
        ChannelTable table = ChannelTable.Create().Add(2, name, ChannelMode.UnreliableUnordered).Build();
        byte[] expected = Bytes.Hex("01 02 00 00 80 44B0" + "4040" + string.Concat(Enumerable.Repeat("61", 64)));
        Assert.Equal(7 + 2 + 64, expected.Length);
        Assert.Equal(expected.Length, ChannelTableCodec.GetLengthWithNames(table));
        byte[] actual = new byte[expected.Length];
        Assert.True(ChannelTableCodec.TryWriteWithNames(table, actual, out int written));
        Assert.Equal(expected.Length, written);
        Assert.Equal(Bytes.ToHex(expected), Bytes.ToHex(actual));
        Assert.False(ChannelTableCodec.TryWriteWithNames(table, actual.AsSpan(0, expected.Length - 1), out written));
        Assert.Equal(0, written);

        Assert.Equal(ChannelTableParseStatus.Ok, ChannelTableCodec.TryParseWithNames(expected, out ChannelTableDescription? d, out int consumed));
        Assert.Equal(expected.Length, consumed);
        Assert.Equal(name, d!.Channels[0].Name);
        Assert.Equal(table.Hash, d.Hash);

        // The same length written non-minimally (a four-byte varint) is rejected.
        byte[] nonMinimal = Bytes.Hex("01 02 00 00 80 44B0" + "80000040" + string.Concat(Enumerable.Repeat("61", 64)));
        Assert.Equal(ChannelTableParseStatus.NonMinimalVarint, ChannelTableCodec.TryParseWithNames(nonMinimal, out _, out _));
    }

    [Fact]
    public void Golden_Table_Section_Parses()
    {
        byte[] section = Bytes.Hex(ReferenceCanonicalHex + ReferenceNamesHex + "FFFF");
        Assert.Equal(ChannelTableParseStatus.Ok, ChannelTableCodec.TryParseWithNames(section, out ChannelTableDescription? d, out int consumed));
        Assert.NotNull(d);
        Assert.Equal(section.Length - 2, consumed);
        Assert.Equal(ReferenceHash, d.Hash);
        Assert.Equal(5, d.Count);
        ChannelDescription move = d.Channels[0];
        Assert.Equal(2, move.Id);
        Assert.Equal("move", move.Name);
        Assert.Equal(ChannelMode.UnreliableSequenced, move.Mode);
        Assert.True(move.Keyed);
        Assert.Equal(32, move.SequenceBits);
        Assert.False(move.Fragmentation);
        Assert.False(move.RequestResponse);
        Assert.False(move.CoalesceOnReceive);
        Assert.Equal(ChannelCompression.None, move.Compression);
        Assert.Equal(200, move.Priority);
        Assert.Equal(1200, move.MaxMessageSize);
        Assert.Equal(0x03, move.Flags);
        Assert.Equal("2 'move' UnreliableSequenced flags=0x03 prio=200 max=1200", move.ToString());

        ChannelDescription chat = d.Channels[1];
        Assert.True(chat.RequestResponse);
        Assert.Equal(16, chat.SequenceBits);
        ChannelDescription fx = d.Channels[2];
        Assert.True(fx.Fragmentation);
        Assert.Equal(8800, fx.MaxMessageSize);
        ChannelDescription state = d.Channels[3];
        Assert.True(state.CoalesceOnReceive);
        Assert.Equal(ChannelCompression.Lz4, state.Compression);
        ChannelDescription world = d.Channels[4];
        Assert.Equal(64, world.Id);
        Assert.Equal(ChannelMode.Bulk, world.Mode);
        Assert.Equal(16 * 1024 * 1024, world.MaxMessageSize);
    }

    [Fact]
    public void Round_Trip_Matches_Every_Definition()
    {
        ChannelTable table = TestTables.All;
        byte[] section = new byte[ChannelTableCodec.GetLengthWithNames(table)];
        ChannelTableCodec.WriteWithNames(table, section);
        Assert.Equal(ChannelTableParseStatus.Ok, ChannelTableCodec.TryParseWithNames(section, out ChannelTableDescription? d, out int consumed));
        Assert.Equal(section.Length, consumed);
        Assert.Equal(table.Hash, d!.Hash);
        Assert.Equal(table.Count, d.Count);
        for (int i = 0; i < table.Count; i++)
        {
            ChannelDefinition def = table.All[i];
            ChannelDescription desc = d.Channels[i];
            Assert.Equal(def.Id, desc.Id);
            Assert.Equal(def.Name, desc.Name);
            Assert.Equal(def.Mode, desc.Mode);
            Assert.Equal(def.Keyed, desc.Keyed);
            Assert.Equal(def.SequenceBits, desc.SequenceBits);
            Assert.Equal(def.Fragmentation, desc.Fragmentation);
            Assert.Equal(def.RequestResponse, desc.RequestResponse);
            Assert.Equal(def.CoalesceOnReceive, desc.CoalesceOnReceive);
            Assert.Equal(def.Compression, desc.Compression);
            Assert.Equal(def.Priority, desc.Priority);
            Assert.Equal(def.MaxMessageSize, desc.MaxMessageSize);
        }
    }

    [Fact]
    public void Hash_Ignores_Names_Declaration_Order_And_Local_Options()
    {
        ulong a = ChannelTable.Create()
            .Add(2, "a", ChannelMode.UnreliableUnordered)
            .Add(3, "b", ChannelMode.ReliableOrdered)
            .Build().Hash;
        ulong b = ChannelTable.Create()
            .Add(3, "renamed", ChannelMode.ReliableOrdered, o => { o.QueueLimitBytes = 5; o.MaxGroups = 3; o.ExpiryMicros = 7; })
            .Add(2, "x", ChannelMode.UnreliableUnordered, o => { o.MaxReassemblies = 2; o.MinCompressSize = 1; })
            .Build().Hash;
        Assert.Equal(a, b);
    }

    [Theory]
    [InlineData("priority")]
    [InlineData("max")]
    [InlineData("mode")]
    [InlineData("keyed")]
    [InlineData("seq")]
    [InlineData("frag")]
    [InlineData("rr")]
    [InlineData("coalesce")]
    [InlineData("lz4")]
    [InlineData("id")]
    [InlineData("extra")]
    public void Hash_Changes_With_Every_Canonical_Field(string change)
    {
        static ChannelTable Make(string change) => change switch
        {
            "priority" => ChannelTable.Create().Add(2, "a", ChannelMode.UnreliableSequenced, o => o.Priority = 1).Build(),
            "max" => ChannelTable.Create().Add(2, "a", ChannelMode.UnreliableSequenced, o => o.MaxMessageSize = 1000).Build(),
            "mode" => ChannelTable.Create().Add(2, "a", ChannelMode.UnreliableUnordered).Build(),
            "keyed" => ChannelTable.Create().Add(2, "a", ChannelMode.UnreliableSequenced, o => { o.Keyed = true; o.SequenceBits = 16; }).Build(),
            "seq" => ChannelTable.Create().Add(2, "a", ChannelMode.UnreliableSequenced, o => o.SequenceBits = 32).Build(),
            "frag" => ChannelTable.Create().Add(2, "a", ChannelMode.UnreliableSequenced, o => o.Fragmentation = true).Build(),
            "coalesce" => ChannelTable.Create().Add(2, "a", ChannelMode.UnreliableSequenced, o => { o.Keyed = true; o.SequenceBits = 16; o.CoalesceOnReceive = true; }).Build(),
            "lz4" => ChannelTable.Create().Add(2, "a", ChannelMode.UnreliableSequenced, o => o.Compression = ChannelCompression.Lz4).Build(),
            "id" => ChannelTable.Create().Add(3, "a", ChannelMode.UnreliableSequenced).Build(),
            "extra" => ChannelTable.Create().Add(2, "a", ChannelMode.UnreliableSequenced).Add(4, "b", ChannelMode.UnreliableSequenced).Build(),
            _ => ChannelTable.Create().Add(2, "a", ChannelMode.UnreliableSequenced).Build(),
        };

        ulong baseline = Make("none").Hash;
        if (change == "rr")
        {
            ulong plain = ChannelTable.Create().Add(2, "a", ChannelMode.ReliableOrdered).Build().Hash;
            ulong rr = ChannelTable.Create().Add(2, "a", ChannelMode.ReliableOrdered, o => o.RequestResponse = true).Build().Hash;
            Assert.NotEqual(plain, rr);
            return;
        }

        Assert.NotEqual(baseline, Make(change).Hash);
    }

    [Fact]
    public void Large_Table_Hash_Uses_Heap_Buffer_And_Matches_Reference()
    {
        ChannelTableBuilder builder = ChannelTable.Create();
        for (int id = 2; id < 400; id++)
        {
            builder.Add(id, $"c{id}", (ChannelMode)(id % 6), o =>
            {
                if (id % 6 == 4)
                {
                    return;
                }

                o.Keyed = id % 2 == 0;
                o.Priority = (byte)id;
            });
        }

        ChannelTable table = builder.Build();
        int length = ChannelTableCodec.GetCanonicalLength(table);
        Assert.True(length > 1024);
        byte[] canonical = new byte[length];
        ChannelTableCodec.WriteCanonical(table, canonical);
        Assert.Equal(System.IO.Hashing.XxHash64.HashToUInt64(canonical), table.Hash);
        Assert.Equal(XxHash64.Hash(canonical), ChannelTableCodec.ComputeHash(table));
    }

    [Fact]
    public void Writers_Report_Small_Destinations()
    {
        ChannelTable table = TestTables.Reference();
        byte[] small = new byte[37];
        Assert.False(ChannelTableCodec.TryWriteCanonical(table, small, out int written));
        Assert.Equal(0, written);
        Assert.Throws<ArgumentException>(() => ChannelTableCodec.WriteCanonical(table, small));
        byte[] exact = new byte[38];
        Assert.True(ChannelTableCodec.TryWriteCanonical(table, exact, out written));
        Assert.Equal(38, written);

        int withNames = ChannelTableCodec.GetLengthWithNames(table);
        byte[] smallNames = new byte[withNames - 1];
        Assert.False(ChannelTableCodec.TryWriteWithNames(table, smallNames, out written));
        Assert.Equal(0, written);
        Assert.Throws<ArgumentException>(() => ChannelTableCodec.WriteWithNames(table, smallNames));
        Assert.Throws<ArgumentNullException>(() => ChannelTableCodec.GetCanonicalLength(null!));
    }

    public static TheoryData<string, ChannelTableParseStatus> MalformedSections => new()
    {
        { "", ChannelTableParseStatus.Truncated },
        { "40", ChannelTableParseStatus.Truncated },
        { "4001 02 00 00 80 01 00", ChannelTableParseStatus.NonMinimalVarint },
        { "7FFF", ChannelTableParseStatus.TooManyChannels },
        { "80004000", ChannelTableParseStatus.TooManyChannels },
        { "80003FFF", ChannelTableParseStatus.NonMinimalVarint },
        { "01 02 00 00 80 01", ChannelTableParseStatus.Truncated },
        { "02 02 00 00 80 01 00", ChannelTableParseStatus.Truncated },
        { "01 01 00 00 80 01 00", ChannelTableParseStatus.BadChannelId },
        { "01 00 00 00 80 01 00", ChannelTableParseStatus.BadChannelId },
        { "01 80004000 00 00 80 01 00", ChannelTableParseStatus.BadChannelId },
        { "01 4002 00 00 80 01 00", ChannelTableParseStatus.NonMinimalVarint },
        { "02 03 00 00 80 01 02 00 00 80 01 00 00", ChannelTableParseStatus.ChannelsNotAscending },
        { "02 03 00 00 80 01 03 00 00 80 01 00 00", ChannelTableParseStatus.ChannelsNotAscending },
        { "01 02 06 00 80 01 00", ChannelTableParseStatus.BadMode },
        { "01 02 FF 00 80 01 00", ChannelTableParseStatus.BadMode },
        { "01 02 00 80 80 01 00", ChannelTableParseStatus.BadFlags },
        { "01 02 00 40 80 01 00", ChannelTableParseStatus.BadFlags },
        { "01 02 00 60 80 01 00", ChannelTableParseStatus.BadFlags },
        { "01 02 00 00 80 00 00", ChannelTableParseStatus.BadMaxMessageSize },
        { "01 02 00 00 80 81000001 00", ChannelTableParseStatus.BadMaxMessageSize },
        { "01 02 00 00 80 C000000040000000 00", ChannelTableParseStatus.BadMaxMessageSize },
        { "01 02 00 00 80 C000000001000000 00", ChannelTableParseStatus.NonMinimalVarint },
        { "01 02 00 00 80 4001 00", ChannelTableParseStatus.NonMinimalVarint },
        { "01 02 00 00 80 40", ChannelTableParseStatus.Truncated },
        { "01 02 00 00 80 01 4001 61", ChannelTableParseStatus.NonMinimalVarint },
        { "01 02 00 00 80 01 41 00 00", ChannelTableParseStatus.NameTooLong },
        { "01 02 00 00 80 01 03 6162", ChannelTableParseStatus.Truncated },
        { "01 02 00 00 80 01 02 C328", ChannelTableParseStatus.InvalidUtf8 },
        { "01 02 00 00 80 01 01 FF", ChannelTableParseStatus.InvalidUtf8 },
        { "01 02 00 00 80 01 40", ChannelTableParseStatus.Truncated },
        { "01 02 00 00", ChannelTableParseStatus.Truncated },
    };

    [Theory]
    [MemberData(nameof(MalformedSections))]
    public void Malformed_Sections_Are_Rejected(string hex, ChannelTableParseStatus expected)
    {
        byte[] bytes = Bytes.Hex(hex);
        Assert.Equal(expected, ChannelTableCodec.TryParseWithNames(bytes, out ChannelTableDescription? d, out int consumed));
        Assert.Null(d);
        Assert.Equal(0, consumed);
    }

    [Fact]
    public void Truncated_Section_Parse_Never_Succeeds_Or_Throws()
    {
        byte[] section = Bytes.Hex(ReferenceCanonicalHex + ReferenceNamesHex);
        for (int length = 0; length < section.Length; length++)
        {
            ChannelTableParseStatus status = ChannelTableCodec.TryParseWithNames(section.AsSpan(0, length), out ChannelTableDescription? d, out _);
            Assert.Equal(ChannelTableParseStatus.Truncated, status);
            Assert.Null(d);
        }
    }

    [Fact]
    public void Largest_Table_Section_Parses_And_Empty_Names_Are_Allowed()
    {
        ChannelTableBuilder builder = ChannelTable.Create();
        for (int id = ChannelDefinition.MinId; id <= ChannelDefinition.MaxId; id++)
        {
            builder.Add(id, id == 2 ? string.Empty : "n", ChannelMode.UnreliableUnordered);
        }

        ChannelTable table = builder.Build();
        byte[] section = new byte[ChannelTableCodec.GetLengthWithNames(table)];
        ChannelTableCodec.WriteWithNames(table, section);
        Assert.Equal(ChannelTableParseStatus.Ok, ChannelTableCodec.TryParseWithNames(section, out ChannelTableDescription? d, out int consumed));
        Assert.Equal(section.Length, consumed);
        Assert.Equal(16382, d!.Count);
        Assert.Equal(string.Empty, d.Channels[0].Name);
        Assert.Equal(table.Hash, d.Hash);
    }

    [Fact]
    public void Empty_Table_Section()
    {
        ChannelTable table = ChannelTable.Create().Build();
        byte[] section = new byte[ChannelTableCodec.GetLengthWithNames(table)];
        Assert.Equal(1, ChannelTableCodec.WriteWithNames(table, section));
        Assert.Equal(ChannelTableParseStatus.Ok, ChannelTableCodec.TryParseWithNames(section, out ChannelTableDescription? d, out int consumed));
        Assert.Equal(1, consumed);
        Assert.Equal(0, d!.Count);
        Assert.Equal(table.Hash, d.Hash);
    }

    [Fact]
    public void Multibyte_Names_Round_Trip()
    {
        string name = "køøø-\U0001F600";
        ChannelTable table = ChannelTable.Create().Add(2, name, ChannelMode.UnreliableUnordered).Build();
        byte[] section = new byte[ChannelTableCodec.GetLengthWithNames(table)];
        ChannelTableCodec.WriteWithNames(table, section);
        // count + (id, mode, flags, priority, 2-byte maxMessageSize 1200) + name length + name bytes
        Assert.Equal(1 + 6 + 1 + Encoding.UTF8.GetByteCount(name), section.Length);
        Assert.Equal(ChannelTableParseStatus.Ok, ChannelTableCodec.TryParseWithNames(section, out ChannelTableDescription? d, out _));
        Assert.Equal(name, d!.Channels[0].Name);
    }

    [Fact]
    public void Fuzz_Random_And_Mutated_Sections_Never_Throw()
    {
        Random random = new(4711);
        byte[] valid = new byte[ChannelTableCodec.GetLengthWithNames(TestTables.All)];
        ChannelTableCodec.WriteWithNames(TestTables.All, valid);
        for (int i = 0; i < 20_000; i++)
        {
            byte[] input;
            if (i % 2 == 0)
            {
                input = new byte[random.Next(0, 80)];
                random.NextBytes(input);
                if (input.Length > 0 && random.Next(2) == 0)
                {
                    input[0] = (byte)random.Next(0, 4);
                }
            }
            else
            {
                input = (byte[])valid.Clone();
                int mutations = random.Next(1, 4);
                for (int m = 0; m < mutations; m++)
                {
                    input[random.Next(input.Length)] = (byte)random.Next(256);
                }

                if (random.Next(3) == 0)
                {
                    input = input[..random.Next(input.Length)];
                }
            }

            ChannelTableParseStatus status = ChannelTableCodec.TryParseWithNames(input, out ChannelTableDescription? d, out int consumed);
            if (status == ChannelTableParseStatus.Ok)
            {
                Assert.NotNull(d);
                Assert.InRange(consumed, 1, input.Length);
                Assert.Equal(System.IO.Hashing.XxHash64.HashToUInt64(CanonicalPart(input, d)), d.Hash);
            }
            else
            {
                Assert.Null(d);
                Assert.Equal(0, consumed);
            }
        }
    }

    private static byte[] CanonicalPart(byte[] input, ChannelTableDescription d)
    {
        int length = VarInt.GetLength((ulong)d.Count);
        foreach (ChannelDescription c in d.Channels)
        {
            length += VarInt.GetLength(c.Id) + 3 + VarInt.GetLength((ulong)c.MaxMessageSize);
        }

        return input[..length];
    }
}
