namespace Tedd.Quicly.Http3.Tests;

public class Http3FrameWriterTests
{
    [Fact]
    public void Header_Length_Matches_VarInt_Sizes()
    {
        Assert.Equal(2, Http3FrameWriter.GetHeaderLength(Http3FrameType.Data, 10));
        Assert.Equal(3, Http3FrameWriter.GetHeaderLength(Http3FrameType.Headers, 100));
        Assert.Equal(5, Http3FrameWriter.GetHeaderLength(Http3FrameType.Data, 100_000));
        Assert.Equal(9, Http3FrameWriter.GetHeaderLength(Http3FrameType.Data, 1UL << 40));
        Assert.Equal(16, Http3FrameWriter.GetHeaderLength(1UL << 40, 1UL << 40));
        Assert.Equal(-1, Http3FrameWriter.GetHeaderLength(ulong.MaxValue, 1));
        Assert.Equal(-1, Http3FrameWriter.GetHeaderLength(1, ulong.MaxValue));
        Assert.Equal(16, Http3FrameWriter.MaxHeaderLength);
    }

    [Fact]
    public void WriteHeader_Encodes_Type_And_Length()
    {
        Span<byte> dst = stackalloc byte[16];
        int n = Http3FrameWriter.WriteHeader(dst, Http3FrameType.Headers, 300);
        Assert.Equal(3, n);
        Assert.Equal("01412c", TestUtil.ToHex(dst.Slice(0, n)));

        n = Http3FrameWriter.WriteHeader(dst, 0x21, 0);
        Assert.Equal(2, n);
        Assert.Equal("2100", TestUtil.ToHex(dst.Slice(0, n)));
    }

    [Fact]
    public void WriteHeader_Fails_When_Too_Small()
    {
        Span<byte> dst = stackalloc byte[2];
        Assert.Equal(-1, Http3FrameWriter.WriteHeader(dst, Http3FrameType.Data, 300)); // length needs 2 bytes
        Assert.Equal(-1, Http3FrameWriter.WriteHeader(Span<byte>.Empty, Http3FrameType.Data, 1));
        Assert.Equal(-1, Http3FrameWriter.WriteHeader(dst, ulong.MaxValue, 1));
    }

    [Fact]
    public void WriteFrame_Writes_Header_And_Payload()
    {
        Span<byte> dst = stackalloc byte[32];
        byte[] payload = [1, 2, 3, 4, 5];
        int n = Http3FrameWriter.WriteFrame(dst, Http3FrameType.Data, payload);
        Assert.Equal(7, n);
        Assert.Equal("00050102030405", TestUtil.ToHex(dst.Slice(0, n)));

        n = Http3FrameWriter.WriteFrame(dst, 0x41UL, ReadOnlySpan<byte>.Empty);
        Assert.Equal("404100", TestUtil.ToHex(dst.Slice(0, n))); // 0x41 >= 0x40 -> 2-byte varint
    }

    [Fact]
    public void WriteFrame_Fails_When_Too_Small()
    {
        byte[] payload = [1, 2, 3, 4, 5];
        Assert.Equal(-1, Http3FrameWriter.WriteFrame(stackalloc byte[6], Http3FrameType.Data, payload));
        Assert.Equal(-1, Http3FrameWriter.WriteFrame(stackalloc byte[1], Http3FrameType.Data, payload));
    }

    [Fact]
    public void Stream_Type_Round_Trip()
    {
        Span<byte> dst = stackalloc byte[8];
        Assert.Equal(2, Http3FrameWriter.WriteStreamType(dst, Http3StreamType.WebTransport)); // 0x54 -> 2-byte varint
        Assert.Equal("4054", TestUtil.ToHex(dst.Slice(0, 2)));
        Assert.True(Http3FrameWriter.TryReadStreamType(dst, out Http3StreamType type, out int consumed));
        Assert.Equal(Http3StreamType.WebTransport, type);
        Assert.Equal(2, consumed);
        Assert.Equal(1, Http3FrameWriter.WriteStreamType(dst, Http3StreamType.Control));
        Assert.Equal(0x00, dst[0]);
        Assert.False(Http3FrameWriter.TryReadStreamType(ReadOnlySpan<byte>.Empty, out _, out _));
        Assert.Equal(-1, Http3FrameWriter.WriteStreamType(Span<byte>.Empty, Http3StreamType.Control));
    }
}
