namespace Tedd.Quicly.Http3.Tests;

public class HttpDatagramTests
{
    [Theory]
    [InlineData(0UL, 0UL, 1)]
    [InlineData(4UL, 1UL, 1)]
    [InlineData(252UL, 63UL, 1)]
    [InlineData(256UL, 64UL, 2)]
    [InlineData(1UL << 40, 1UL << 38, 8)]
    public void Quarter_Stream_Id_Prefix(ulong streamId, ulong quarter, int prefixLength)
    {
        Assert.True(HttpDatagram.IsValidStreamId(streamId));
        Assert.Equal(quarter, HttpDatagram.ToQuarterStreamId(streamId));
        Assert.Equal(streamId, HttpDatagram.FromQuarterStreamId(quarter));
        Assert.Equal(prefixLength, HttpDatagram.GetPrefixLength(streamId));

        Span<byte> dst = stackalloc byte[16];
        int n = HttpDatagram.WritePrefix(dst, streamId);
        Assert.Equal(prefixLength, n);
        Assert.True(Http3VarInt.TryRead(dst.Slice(0, n), out ulong q, out _));
        Assert.Equal(quarter, q);
    }

    [Theory]
    [InlineData(1UL)]
    [InlineData(2UL)]
    [InlineData(3UL)]
    [InlineData(7UL)]
    [InlineData((1UL << 62))]
    public void Invalid_Stream_Ids_Are_Rejected(ulong streamId)
    {
        Assert.False(HttpDatagram.IsValidStreamId(streamId));
        Assert.Equal(-1, HttpDatagram.GetPrefixLength(streamId));
        Assert.Equal(-1, HttpDatagram.WritePrefix(stackalloc byte[16], streamId));
        Assert.Equal(-1, HttpDatagram.Write(stackalloc byte[16], streamId, new byte[] { 1 }));
    }

    [Fact]
    public void Write_And_Read_Round_Trip()
    {
        byte[] payload = [1, 2, 3, 4];
        Span<byte> dst = stackalloc byte[16];
        int n = HttpDatagram.Write(dst, 8, payload);
        Assert.Equal(5, n);
        Assert.Equal("0201020304", TestUtil.ToHex(dst.Slice(0, n)));
        Assert.True(HttpDatagram.TryRead(dst.Slice(0, n), out ulong streamId, out ReadOnlySpan<byte> back));
        Assert.Equal(8UL, streamId);
        Assert.Equal(payload, back.ToArray());

        // Empty payload is fine.
        n = HttpDatagram.Write(dst, 0, ReadOnlySpan<byte>.Empty);
        Assert.Equal(1, n);
        Assert.True(HttpDatagram.TryRead(dst.Slice(0, n), out streamId, out back));
        Assert.Equal(0UL, streamId);
        Assert.True(back.IsEmpty);
    }

    [Fact]
    public void Write_Fails_When_Too_Small()
    {
        Assert.Equal(-1, HttpDatagram.WritePrefix(Span<byte>.Empty, 0));
        Assert.Equal(-1, HttpDatagram.Write(stackalloc byte[4], 8, new byte[] { 1, 2, 3, 4 }));
        Assert.Equal(-1, HttpDatagram.Write(Span<byte>.Empty, 8, ReadOnlySpan<byte>.Empty));
    }

    [Fact]
    public void Read_Fails_On_Truncated_Prefix()
    {
        Assert.False(HttpDatagram.TryRead(ReadOnlySpan<byte>.Empty, out ulong id, out ReadOnlySpan<byte> payload));
        Assert.Equal(0UL, id);
        Assert.True(payload.IsEmpty);
        Assert.False(HttpDatagram.TryRead(new byte[] { 0x40 }, out _, out _));
    }

    [Fact]
    public void Read_Rejects_Quarter_Stream_Id_Above_Varint_Range()
    {
        // Quarter Stream ID 2^60 would be stream id 2^62, above the varint maximum 2^62-1 (ADR 0009: every parser is bounded).
        Assert.False(HttpDatagram.TryRead(TestUtil.Hex("d000000000000000 aa"), out ulong id, out ReadOnlySpan<byte> payload));
        Assert.Equal(0UL, id);
        Assert.True(payload.IsEmpty);
        Assert.False(HttpDatagram.TryRead(TestUtil.Hex("ffffffffffffffff"), out _, out _));
        // 2^60 - 1 is the largest legal quarter id: stream id 2^62 - 4.
        Assert.True(HttpDatagram.TryRead(TestUtil.Hex("cfffffffffffffff 01"), out id, out payload));
        Assert.Equal((1UL << 62) - 4, id);
        Assert.True(HttpDatagram.IsValidStreamId(id));
        Assert.Equal(1, payload.Length);
    }
}
