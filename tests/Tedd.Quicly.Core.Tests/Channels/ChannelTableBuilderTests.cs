using Tedd.Quicly.Core.Channels;

namespace Tedd.Quicly.Core.Tests.Channels;

public class ChannelTableBuilderTests
{
    private static ChannelDefinition Single(int id, ChannelMode mode, Action<ChannelOptions>? configure = null, string name = "c") =>
        ChannelTable.Create().Add(id, name, mode, configure).Build()[id]!;

    [Theory]
    [InlineData(ChannelMode.UnreliableUnordered, false, 16, false, 1200, 0L, 0)]
    [InlineData(ChannelMode.UnreliableSequenced, false, 16, false, 1200, ChannelDefinition.ExpiryTwiceFlushInterval, 0)]
    [InlineData(ChannelMode.ReliableOrdered, false, 16, false, 64 * 1024, 0L, 1)]
    [InlineData(ChannelMode.ReliableUnordered, false, 16, false, 64 * 1024, 0L, 8)]
    [InlineData(ChannelMode.ReliableLatest, true, 32, true, 64 * 1024, 0L, 4)]
    [InlineData(ChannelMode.Bulk, false, 16, false, 16 * 1024 * 1024, 0L, 2)]
    public void Defaults_Per_Mode(ChannelMode mode, bool keyed, int sequenceBits, bool coalesce, int maxMessageSize, long expiry, int maxGroups)
    {
        ChannelDefinition ch = Single(7, mode);
        Assert.Equal(7, ch.Id);
        Assert.Equal("c", ch.Name);
        Assert.Equal(mode, ch.Mode);
        Assert.Equal(keyed, ch.Keyed);
        Assert.Equal(sequenceBits, ch.SequenceBits);
        Assert.Equal(coalesce, ch.CoalesceOnReceive);
        Assert.Equal(maxMessageSize, ch.MaxMessageSize);
        Assert.Equal(expiry, ch.ExpiryMicros);
        Assert.Equal(maxGroups, ch.MaxGroups);
        Assert.Equal(128, ch.Priority);
        Assert.False(ch.Fragmentation);
        Assert.False(ch.RequestResponse);
        Assert.Equal(ChannelCompression.None, ch.Compression);
        Assert.Equal(0, ch.QueueLimitBytes);
        Assert.Equal(4096, ch.MaxKeys);
        Assert.Equal(16, ch.MaxReassemblies);
        Assert.Equal(64 * 1024, ch.GroupMaxBytes);
        Assert.Equal(64, ch.MinCompressSize);
        Assert.False(ch.KeySpace.IsDense);
    }

    [Fact]
    public void Keyed_Channel_Defaults_To_32_Bit_Sequence()
    {
        Assert.Equal(32, Single(2, ChannelMode.UnreliableSequenced, o => o.Keyed = true).SequenceBits);
        Assert.Equal(16, Single(2, ChannelMode.UnreliableSequenced, o => { o.Keyed = true; o.SequenceBits = 16; }).SequenceBits);
        Assert.Equal(32, Single(2, ChannelMode.UnreliableSequenced, o => o.SequenceBits = 32).SequenceBits);
    }

    [Fact]
    public void Explicit_Options_Are_Kept()
    {
        ChannelDefinition ch = Single(9, ChannelMode.UnreliableSequenced, o =>
        {
            o.Keyed = true;
            o.Fragmentation = true;
            o.Compression = ChannelCompression.Lz4;
            o.CoalesceOnReceive = true;
            o.Priority = 255;
            o.MaxMessageSize = 8800;
            o.QueueLimitBytes = 1000;
            o.ExpiryMicros = 5000;
            o.MaxKeys = 100;
            o.MaxReassemblies = 1024;
            o.MaxGroups = 0;
            o.GroupMaxBytes = 1;
            o.KeySpace = KeySpace.Dense(99);
            o.MinCompressSize = 0;
        });
        Assert.True(ch.Fragmentation);
        Assert.Equal(ChannelCompression.Lz4, ch.Compression);
        Assert.True(ch.CoalesceOnReceive);
        Assert.Equal(255, ch.Priority);
        Assert.Equal(8800, ch.MaxMessageSize);
        Assert.Equal(1000, ch.QueueLimitBytes);
        Assert.Equal(5000, ch.ExpiryMicros);
        Assert.Equal(100, ch.MaxKeys);
        Assert.Equal(1024, ch.MaxReassemblies);
        Assert.Equal(0, ch.MaxGroups);
        Assert.Equal(1, ch.GroupMaxBytes);
        Assert.True(ch.KeySpace.IsDense);
        Assert.Equal(99, ch.KeySpace.MaxKey);
        Assert.Equal(0, ch.MinCompressSize);
    }

    [Fact]
    public void Dense_Key_Space_Sizes_MaxKeys_By_Default()
    {
        ChannelDefinition ch = Single(2, ChannelMode.UnreliableSequenced, o => { o.Keyed = true; o.KeySpace = KeySpace.Dense(9999); });
        Assert.Equal(10000, ch.MaxKeys);
        Assert.Equal("Dense(9999)", ch.KeySpace.ToString());
    }

    [Fact]
    public void KeySpace_Factory_And_Properties()
    {
        Assert.False(KeySpace.Hashed.IsDense);
        Assert.Equal(-1, KeySpace.Hashed.MaxKey);
        Assert.Equal("Hashed", KeySpace.Hashed.ToString());
        Assert.Equal(0, KeySpace.Dense(0).MaxKey);
        Assert.Throws<ArgumentOutOfRangeException>(() => KeySpace.Dense(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => KeySpace.Dense(int.MaxValue));
    }

    [Theory]
    [InlineData(ChannelMode.UnreliableUnordered, false, false, true, false)]
    [InlineData(ChannelMode.UnreliableUnordered, true, true, true, false)]
    [InlineData(ChannelMode.UnreliableSequenced, false, true, true, false)]
    [InlineData(ChannelMode.UnreliableSequenced, true, true, true, false)]
    [InlineData(ChannelMode.ReliableOrdered, false, false, false, true)]
    [InlineData(ChannelMode.ReliableUnordered, false, false, false, true)]
    [InlineData(ChannelMode.ReliableLatest, false, true, true, true)]
    [InlineData(ChannelMode.Bulk, false, false, false, true)]
    public void Derived_Mode_Flags(ChannelMode mode, bool fragmentation, bool hasSequence, bool isDatagram, bool isStream)
    {
        ChannelDefinition ch = Single(2, mode, o => o.Fragmentation = fragmentation);
        Assert.Equal(hasSequence, ch.HasSequence);
        Assert.Equal(isDatagram, ch.IsDatagramMode);
        Assert.Equal(isStream, ch.IsStreamMode);
    }

    [Theory]
    [InlineData(2, ChannelMode.UnreliableUnordered, false, false, 1, 1)]
    [InlineData(63, ChannelMode.UnreliableSequenced, false, false, 1, 3)]
    [InlineData(63, ChannelMode.UnreliableSequenced, true, false, 1, 5)]
    [InlineData(64, ChannelMode.UnreliableSequenced, true, false, 2, 6)]
    [InlineData(16383, ChannelMode.UnreliableUnordered, false, true, 2, 5)]
    [InlineData(5, ChannelMode.UnreliableUnordered, false, true, 1, 4)]
    [InlineData(5, ChannelMode.ReliableLatest, true, false, 1, 5)]
    [InlineData(5, ChannelMode.ReliableOrdered, true, false, 1, 1)]
    public void Channel_Id_Length_And_Fixed_Header(int id, ChannelMode mode, bool keyed, bool fragmentation, int idLength, int fixedHeader)
    {
        ChannelDefinition ch = Single(id, mode, o => { o.Keyed = keyed; o.Fragmentation = fragmentation; });
        Assert.Equal(idLength, ch.ChannelIdLength);
        Assert.Equal(fixedHeader, ch.FixedHeaderBytesWithoutKey);
    }

    [Theory]
    [InlineData(ChannelMode.UnreliableUnordered, false, 1200)]
    [InlineData(ChannelMode.UnreliableSequenced, true, 8800)]
    [InlineData(ChannelMode.ReliableOrdered, false, 1024 * 1024)]
    [InlineData(ChannelMode.ReliableUnordered, false, 1024 * 1024)]
    [InlineData(ChannelMode.ReliableLatest, false, 1024 * 1024)]
    [InlineData(ChannelMode.Bulk, false, 16 * 1024 * 1024)]
    public void Largest_MaxMessageSize_Is_Accepted(ChannelMode mode, bool fragmentation, int max)
    {
        Assert.Equal(max, Single(2, mode, o => { o.Fragmentation = fragmentation; o.MaxMessageSize = max; }).MaxMessageSize);
        Assert.Equal(1, Single(2, mode, o => { o.Fragmentation = fragmentation; o.MaxMessageSize = 1; }).MaxMessageSize);
    }

    public static TheoryData<string, int, ChannelMode, string> InvalidCases => new()
    {
        { "none", 0, ChannelMode.UnreliableUnordered, "id must be in 2..16383" },
        { "none", 1, ChannelMode.UnreliableUnordered, "id must be in 2..16383" },
        { "none", 16384, ChannelMode.UnreliableUnordered, "id must be in 2..16383" },
        { "none", -1, ChannelMode.UnreliableUnordered, "id must be in 2..16383" },
        { "none", 5, (ChannelMode)6, "mode 6 is not a defined ChannelMode" },
        { "unkeyed", 5, ChannelMode.ReliableLatest, "ReliableLatest channels must be keyed" },
        { "seq16", 5, ChannelMode.ReliableLatest, "must use 32-bit sequences" },
        { "nocoalesce", 5, ChannelMode.ReliableLatest, "CoalesceOnReceive cannot be turned off" },
        { "seq8", 5, ChannelMode.UnreliableSequenced, "SequenceBits must be 16 or 32 (was 8)" },
        { "coalesce", 5, ChannelMode.UnreliableSequenced, "CoalesceOnReceive requires a keyed channel" },
        { "rr", 5, ChannelMode.ReliableUnordered, "RequestResponse requires mode ReliableOrdered (was ReliableUnordered)" },
        { "rr", 5, ChannelMode.UnreliableSequenced, "RequestResponse requires mode ReliableOrdered" },
        { "frag", 5, ChannelMode.ReliableOrdered, "Fragmentation requires mode UnreliableUnordered or UnreliableSequenced (was ReliableOrdered)" },
        { "frag", 5, ChannelMode.ReliableLatest, "Fragmentation requires mode" },
        { "frag", 5, ChannelMode.Bulk, "Fragmentation requires mode" },
        { "lz2", 5, ChannelMode.UnreliableUnordered, "compression codec 2 is reserved" },
        { "lz3", 5, ChannelMode.ReliableOrdered, "compression codec 3 is reserved" },
        { "max0", 5, ChannelMode.UnreliableUnordered, "MaxMessageSize must be in 1..1200 for an unreliable channel without fragmentation (was 0)" },
        { "max1201", 5, ChannelMode.UnreliableSequenced, "MaxMessageSize must be in 1..1200" },
        { "fragmax8801", 5, ChannelMode.UnreliableUnordered, "MaxMessageSize must be in 1..8800 for a fragmenting channel" },
        { "max16m1", 5, ChannelMode.Bulk, "MaxMessageSize must be in 1..16777216 for a Bulk channel" },
        { "max1m1", 5, ChannelMode.ReliableOrdered, "MaxMessageSize must be in 1..1048576 for a ReliableOrdered channel" },
        { "max1m1", 5, ChannelMode.ReliableLatest, "MaxMessageSize must be in 1..1048576 for a ReliableLatest channel" },
        { "queue", 5, ChannelMode.ReliableOrdered, "QueueLimitBytes must not be negative" },
        { "expiry", 5, ChannelMode.UnreliableSequenced, "ExpiryMicros must be 0 (never)" },
        { "dense", 5, ChannelMode.UnreliableSequenced, "a dense KeySpace requires a keyed channel" },
        { "keys0", 5, ChannelMode.UnreliableSequenced, "MaxKeys must be in 1..16777216 (was 0)" },
        { "keysbig", 5, ChannelMode.UnreliableSequenced, "MaxKeys must be in 1..16777216" },
        { "densesmall", 5, ChannelMode.UnreliableSequenced, "needs MaxKeys >= 101 (was 50)" },
        { "reasm0", 5, ChannelMode.UnreliableUnordered, "MaxReassemblies must be in 1..1024" },
        { "reasmbig", 5, ChannelMode.UnreliableUnordered, "MaxReassemblies must be in 1..1024" },
        { "groups0", 5, ChannelMode.ReliableUnordered, "MaxGroups must be in 1..1024 for a ReliableUnordered channel" },
        { "groups0", 5, ChannelMode.Bulk, "MaxGroups must be in 1..1024" },
        { "groupsneg", 5, ChannelMode.UnreliableUnordered, "MaxGroups must be in 0..1024" },
        { "groupsbig", 5, ChannelMode.ReliableLatest, "MaxGroups must be in 1..1024" },
        { "gbytes0", 5, ChannelMode.ReliableUnordered, "GroupMaxBytes must be in 1..16777216" },
        { "gbytesbig", 5, ChannelMode.ReliableUnordered, "GroupMaxBytes must be in 1..16777216" },
        { "mincompress", 5, ChannelMode.UnreliableUnordered, "MinCompressSize must not be negative" },
    };

    private static void Configure(string key, ChannelOptions o)
    {
        switch (key)
        {
            case "unkeyed": o.Keyed = false; break;
            case "seq16": o.SequenceBits = 16; break;
            case "nocoalesce": o.CoalesceOnReceive = false; break;
            case "seq8": o.SequenceBits = 8; break;
            case "coalesce": o.CoalesceOnReceive = true; break;
            case "rr": o.RequestResponse = true; break;
            case "frag": o.Fragmentation = true; break;
            case "lz2": o.Compression = (ChannelCompression)2; break;
            case "lz3": o.Compression = (ChannelCompression)3; break;
            case "max0": o.MaxMessageSize = 0; break;
            case "max1201": o.MaxMessageSize = 1201; break;
            case "fragmax8801": o.Fragmentation = true; o.MaxMessageSize = 8801; break;
            case "max16m1": o.MaxMessageSize = (16 * 1024 * 1024) + 1; break;
            case "max1m1": o.MaxMessageSize = (1024 * 1024) + 1; break;
            case "queue": o.QueueLimitBytes = -1; break;
            case "expiry": o.ExpiryMicros = -2; break;
            case "dense": o.KeySpace = KeySpace.Dense(10); break;
            case "keys0": o.MaxKeys = 0; break;
            case "keysbig": o.MaxKeys = (1 << 24) + 1; break;
            case "densesmall": o.Keyed = true; o.KeySpace = KeySpace.Dense(100); o.MaxKeys = 50; break;
            case "reasm0": o.MaxReassemblies = 0; break;
            case "reasmbig": o.MaxReassemblies = 1025; break;
            case "groups0": o.MaxGroups = 0; break;
            case "groupsneg": o.MaxGroups = -1; break;
            case "groupsbig": o.MaxGroups = 1025; break;
            case "gbytes0": o.GroupMaxBytes = 0; break;
            case "gbytesbig": o.GroupMaxBytes = (16 * 1024 * 1024) + 1; break;
            case "mincompress": o.MinCompressSize = -1; break;
        }
    }

    [Theory]
    [MemberData(nameof(InvalidCases))]
    public void Invalid_Declarations_Are_Rejected_With_Precise_Messages(string key, int id, ChannelMode mode, string expected)
    {
        ChannelTableBuilder builder = ChannelTable.Create().Add(id, "bad", mode, o => Configure(key, o));
        ArgumentException ex = Assert.Throws<ArgumentException>(() => builder.Build());
        Assert.StartsWith($"Channel {id} ('bad'): ", ex.Message, StringComparison.Ordinal);
        Assert.Contains(expected, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Name_Length_Is_Measured_In_Utf8_Bytes()
    {
        Assert.Equal(new string('a', 64), Single(2, ChannelMode.UnreliableUnordered, name: new string('a', 64)).Name);
        Assert.Equal(32, Single(2, ChannelMode.UnreliableUnordered, name: new string('é', 32)).Name.Length); // 64 bytes

        ArgumentException ex = Assert.Throws<ArgumentException>(() => ChannelTable.Create().Add(2, new string('a', 65), ChannelMode.UnreliableUnordered).Build());
        Assert.Contains("name is 65 UTF-8 bytes; the maximum is 64", ex.Message, StringComparison.Ordinal);

        ex = Assert.Throws<ArgumentException>(() => ChannelTable.Create().Add(2, new string('é', 33), ChannelMode.UnreliableUnordered).Build());
        Assert.Contains("name is 66 UTF-8 bytes", ex.Message, StringComparison.Ordinal);

        ex = Assert.Throws<ArgumentException>(() => ChannelTable.Create().Add(2, "a\uD800b", ChannelMode.UnreliableUnordered).Build());
        Assert.Contains("not valid Unicode", ex.Message, StringComparison.Ordinal);

        Assert.Equal(string.Empty, Single(2, ChannelMode.UnreliableUnordered, name: string.Empty).Name);
        Assert.Throws<ArgumentNullException>(() => ChannelTable.Create().Add(2, null!, ChannelMode.UnreliableUnordered));
    }

    [Fact]
    public void Duplicate_Ids_Are_Rejected()
    {
        ArgumentException ex = Assert.Throws<ArgumentException>(() => ChannelTable.Create()
            .Add(9, "first", ChannelMode.UnreliableUnordered)
            .Add(3, "other", ChannelMode.ReliableOrdered)
            .Add(9, "second", ChannelMode.ReliableOrdered)
            .Build());
        Assert.Contains("Channel id 9 is declared more than once", ex.Message, StringComparison.Ordinal);
        Assert.Contains("'first'", ex.Message, StringComparison.Ordinal);
        Assert.Contains("'second'", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Configure_Callback_Is_Optional_And_Invoked_Once()
    {
        int calls = 0;
        ChannelTableBuilder builder = ChannelTable.Create().Add(2, "a", ChannelMode.UnreliableUnordered, _ => calls++);
        Assert.Equal(1, calls);
        builder.Build();
        builder.Build();
        Assert.Equal(1, calls);
    }

    [Fact]
    public void Table_Lookup_Count_And_Order()
    {
        ChannelTable table = ChannelTable.Create()
            .Add(300, "c", ChannelMode.ReliableOrdered)
            .Add(2, "a", ChannelMode.UnreliableUnordered)
            .Add(64, "b", ChannelMode.Bulk)
            .Build();
        Assert.Equal(3, table.Count);
        Assert.Equal(300, table.MaxId);
        Assert.Equal(new ushort[] { 2, 64, 300 }, table.All.ToArray().Select(c => c.Id).ToArray());
        Assert.Equal("b", table[64]!.Name);
        Assert.Null(table[3]);
        Assert.Null(table[0]);
        Assert.Null(table[-1]);
        Assert.Null(table[301]);
        Assert.Null(table[int.MaxValue]);
        Assert.Null(table[int.MinValue]);
        Assert.True(table.TryGet(300, out ChannelDefinition? c));
        Assert.Equal("c", c.Name);
        Assert.False(table.TryGet(299, out c));
        Assert.Null(c);
        Assert.Equal("64 'b' Bulk", table[64]!.ToString());
    }

    [Fact]
    public void Empty_Table()
    {
        ChannelTable table = ChannelTable.Create().Build();
        Assert.Equal(0, table.Count);
        Assert.Equal(0, table.MaxId);
        Assert.True(table.All.IsEmpty);
        Assert.Null(table[0]);
        Assert.Null(table[2]);
        Assert.Equal(System.IO.Hashing.XxHash64.HashToUInt64(new byte[] { 0x00 }), table.Hash);
    }

    [Fact]
    public void Largest_Table_Builds()
    {
        ChannelTableBuilder builder = ChannelTable.Create();
        for (int id = ChannelDefinition.MinId; id <= ChannelDefinition.MaxId; id++)
        {
            builder.Add(id, string.Empty, ChannelMode.UnreliableUnordered);
        }

        ChannelTable table = builder.Build();
        Assert.Equal(ChannelTableCodec.MaxChannels, table.Count);
        Assert.Equal(16382, ChannelTableCodec.MaxChannels);
        Assert.Same(table.All[^1], table[16383]);
    }
}
