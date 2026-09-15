using Tedd.Quicly.Http3.Qpack;

namespace Tedd.Quicly.Http3.Tests;

public class QpackIntegerTests
{
    [Theory]
    [InlineData(5, 10UL, "0a")]          // RFC 7541 C.1.1
    [InlineData(5, 1337UL, "1f9a0a")]    // RFC 7541 C.1.2
    [InlineData(8, 42UL, "2a")]          // RFC 7541 C.1.3
    [InlineData(7, 127UL, "7f00")]
    [InlineData(6, 63UL, "3f00")]
    [InlineData(6, 64UL, "3f01")]
    [InlineData(4, 15UL, "0f00")]
    [InlineData(3, 7UL, "0700")]
    public void Rfc7541_Vectors(int prefix, ulong value, string hex)
    {
        Span<byte> dst = stackalloc byte[16];
        int n = QpackInteger.Write(dst, prefix, 0, value);
        Assert.Equal(hex, TestUtil.ToHex(dst.Slice(0, n)));
        Assert.Equal(n, QpackInteger.GetLength(prefix, value));
        Assert.Equal(QpackIntegerStatus.Ok, QpackInteger.TryRead(dst.Slice(0, n), prefix, out ulong back, out int consumed));
        Assert.Equal(value, back);
        Assert.Equal(n, consumed);
    }

    [Fact]
    public void High_Bits_Are_Preserved()
    {
        Span<byte> dst = stackalloc byte[4];
        Assert.Equal(1, QpackInteger.Write(dst, 6, 0xC0, 5));
        Assert.Equal(0xC5, dst[0]);
        Assert.Equal(2, QpackInteger.Write(dst, 6, 0xC0, 63));
        Assert.Equal(0xFF, dst[0]);
        Assert.Equal(0x00, dst[1]);
        // Reading masks the high bits away.
        Assert.Equal(QpackIntegerStatus.Ok, QpackInteger.TryRead(dst.Slice(0, 2), 6, out ulong v, out _));
        Assert.Equal(63UL, v);
    }

    [Fact]
    public void Write_Fails_When_Too_Small()
    {
        Assert.Equal(-1, QpackInteger.Write(Span<byte>.Empty, 5, 0, 1));
        Assert.Equal(-1, QpackInteger.Write(stackalloc byte[1], 5, 0, 31));
        Assert.Equal(-1, QpackInteger.Write(stackalloc byte[2], 5, 0, 1337));
        Assert.Equal(-1, QpackInteger.Write(stackalloc byte[1], 5, 0, 1337)); // fails inside the continuation loop
        Assert.Equal(3, QpackInteger.Write(stackalloc byte[3], 5, 0, 1337));
    }

    [Fact]
    public void Read_Detects_Truncation()
    {
        Assert.Equal(QpackIntegerStatus.Truncated, QpackInteger.TryRead(ReadOnlySpan<byte>.Empty, 5, out _, out _));
        Assert.Equal(QpackIntegerStatus.Truncated, QpackInteger.TryRead(new byte[] { 0x1f }, 5, out ulong v, out int c));
        Assert.Equal(0UL, v);
        Assert.Equal(0, c);
        Assert.Equal(QpackIntegerStatus.Truncated, QpackInteger.TryRead(new byte[] { 0x1f, 0x9a }, 5, out _, out _));
    }

    [Fact]
    public void Read_Detects_Overflow()
    {
        // 10 continuation bytes: shift reaches 63.
        byte[] tooLong = [0xff, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01];
        Assert.Equal(QpackIntegerStatus.Overflow, QpackInteger.TryRead(tooLong, 8, out ulong v, out int c));
        Assert.Equal(0UL, v);
        Assert.Equal(0, c);
        // Value above 2^62-1 within 9 continuation bytes.
        byte[] tooBig = [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f];
        Assert.Equal(QpackIntegerStatus.Overflow, QpackInteger.TryRead(tooBig, 8, out _, out _));
    }

    [Fact]
    public void Large_Values_Round_Trip()
    {
        Span<byte> dst = stackalloc byte[16];
        ulong[] values = [0, 1, 254, 255, 256, 16383, 16384, 1UL << 32, QpackInteger.MaxValue];
        foreach (ulong value in values)
        {
            for (int prefix = 1; prefix <= 8; prefix++)
            {
                int n = QpackInteger.Write(dst, prefix, 0, value);
                Assert.Equal(QpackInteger.GetLength(prefix, value), n);
                Assert.Equal(QpackIntegerStatus.Ok, QpackInteger.TryRead(dst.Slice(0, n), prefix, out ulong back, out int consumed));
                Assert.Equal(value, back);
                Assert.Equal(n, consumed);
            }
        }
    }
}
