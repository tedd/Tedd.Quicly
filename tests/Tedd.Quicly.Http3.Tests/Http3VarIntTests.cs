namespace Tedd.Quicly.Http3.Tests;

public class Http3VarIntTests
{
    [Theory]
    [InlineData(0UL, 1)]
    [InlineData(63UL, 1)]
    [InlineData(64UL, 2)]
    [InlineData(16383UL, 2)]
    [InlineData(16384UL, 4)]
    [InlineData(1073741823UL, 4)]
    [InlineData(1073741824UL, 8)]
    [InlineData(4611686018427387903UL, 8)]
    public void GetLength_Matches_Rfc9000(ulong value, int expected)
    {
        Assert.Equal(expected, Http3VarInt.GetLength(value));
    }

    [Fact]
    public void GetLength_Rejects_Out_Of_Range()
    {
        Assert.Equal(-1, Http3VarInt.GetLength(Http3VarInt.MaxValue + 1));
        Assert.Equal(-1, Http3VarInt.GetLength(ulong.MaxValue));
    }

    [Theory]
    [InlineData("25", 37UL)]
    [InlineData("7bbd", 15293UL)]
    [InlineData("9d7f3e7d", 494878333UL)]
    [InlineData("c2197c5eff14e88c", 151288809941952652UL)]
    public void Rfc9000_Appendix_A_Vectors_Round_Trip(string hex, ulong expected)
    {
        byte[] bytes = TestUtil.Hex(hex);
        Assert.True(Http3VarInt.TryRead(bytes, out ulong value, out int consumed));
        Assert.Equal(expected, value);
        Assert.Equal(bytes.Length, consumed);

        Span<byte> dst = stackalloc byte[8];
        int written = Http3VarInt.Write(dst, expected);
        Assert.Equal(bytes.Length, written);
        Assert.Equal(hex, TestUtil.ToHex(dst.Slice(0, written)));
    }

    [Fact]
    public void Write_Fails_When_Destination_Too_Small_Or_Value_Too_Large()
    {
        Span<byte> dst = stackalloc byte[3];
        Assert.Equal(-1, Http3VarInt.Write(dst, 16384));
        Assert.Equal(-1, Http3VarInt.Write(Span<byte>.Empty, 0));
        Assert.Equal(-1, Http3VarInt.Write(stackalloc byte[8], Http3VarInt.MaxValue + 1));
    }

    [Fact]
    public void TryRead_Fails_On_Empty_And_Truncated()
    {
        Assert.False(Http3VarInt.TryRead(ReadOnlySpan<byte>.Empty, out ulong v, out int c));
        Assert.Equal(0UL, v);
        Assert.Equal(0, c);
        Assert.False(Http3VarInt.TryRead(new byte[] { 0x7b }, out v, out c));
        Assert.Equal(0, c);
        Assert.False(Http3VarInt.TryRead(new byte[] { 0xc2, 0x19, 0x7c }, out _, out _));
    }

    [Fact]
    public void GetLengthFromFirstByte_Decodes_Prefix()
    {
        Assert.Equal(1, Http3VarInt.GetLengthFromFirstByte(0x3f));
        Assert.Equal(2, Http3VarInt.GetLengthFromFirstByte(0x40));
        Assert.Equal(4, Http3VarInt.GetLengthFromFirstByte(0x80));
        Assert.Equal(8, Http3VarInt.GetLengthFromFirstByte(0xc0));
    }

    [Fact]
    public void Random_Round_Trips()
    {
        var rng = new Random(1234);
        Span<byte> dst = stackalloc byte[8];
        for (int i = 0; i < 10_000; i++)
        {
            int bits = rng.Next(0, 63);
            ulong value = ((ulong)rng.NextInt64() & ((1UL << bits) - 1));
            int n = Http3VarInt.Write(dst, value);
            Assert.Equal(Http3VarInt.GetLength(value), n);
            Assert.True(Http3VarInt.TryRead(dst.Slice(0, n), out ulong back, out int consumed));
            Assert.Equal(value, back);
            Assert.Equal(n, consumed);
        }
    }
}
