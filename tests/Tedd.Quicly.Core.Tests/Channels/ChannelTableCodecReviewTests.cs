using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Tests.Framing;

namespace Tedd.Quicly.Core.Tests.Channels;

/// <summary>Review findings for <see cref="ChannelTableCodec"/> (expected to fail until fixed).</summary>
public class ChannelTableCodecReviewTests
{
    // The builder accepts names of exactly 64 UTF-8 bytes (MaxNameBytes), but 64 is NOT a one-byte QUIC varint
    // (one byte holds 0..63). GetLengthWithNames/TryWriteWithNames write the length as the single byte 0x40, which
    // is the first byte of a two-byte varint, so the HelloAck table section is corrupt and TryParseWithNames fails.
    [Fact]
    public void Review_64_Byte_Name_Is_Written_As_A_Two_Byte_Varint_And_Round_Trips()
    {
        string name = new('a', ChannelDefinition.MaxNameBytes);
        ChannelTable table = ChannelTable.Create().Add(2, name, ChannelMode.UnreliableUnordered).Build();
        int canonical = ChannelTableCodec.GetCanonicalLength(table);

        Assert.Equal(canonical + 2 + 64, ChannelTableCodec.GetLengthWithNames(table));

        byte[] section = new byte[canonical + 2 + 64 + 8];
        int written = ChannelTableCodec.WriteWithNames(table, section);
        Assert.Equal(canonical + 2 + 64, written);
        Assert.Equal("4040", Bytes.ToHex(section.AsSpan(canonical, 2)));

        Assert.Equal(ChannelTableParseStatus.Ok, ChannelTableCodec.TryParseWithNames(section.AsSpan(0, written), out ChannelTableDescription? d, out int consumed));
        Assert.Equal(written, consumed);
        Assert.Equal(name, d!.Channels[0].Name);
    }

    [Fact]
    public void Review_64_Byte_Multibyte_Name_Round_Trips()
    {
        string name = new('é', 32); // 64 UTF-8 bytes
        ChannelTable table = ChannelTable.Create().Add(64, name, ChannelMode.ReliableOrdered).Build();
        byte[] section = new byte[ChannelTableCodec.GetLengthWithNames(table) + 8];
        int written = ChannelTableCodec.WriteWithNames(table, section);

        Assert.Equal(ChannelTableParseStatus.Ok, ChannelTableCodec.TryParseWithNames(section.AsSpan(0, written), out ChannelTableDescription? d, out int consumed));
        Assert.Equal(written, consumed);
        Assert.Equal(name, d!.Channels[0].Name);
    }
}
