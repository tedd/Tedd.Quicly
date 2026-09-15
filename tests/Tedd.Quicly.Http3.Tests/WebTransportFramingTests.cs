using Tedd.Quicly.Http3.WebTransport;

namespace Tedd.Quicly.Http3.Tests;

public class WebTransportFramingTests
{
    [Fact]
    public void Signals_Match_Draft()
    {
        Assert.Equal(0x54UL, WebTransportFraming.UnidirectionalSignal);
        Assert.Equal(0x41UL, WebTransportFraming.BidirectionalSignal);
    }

    // Both signal values are >= 0x40 and therefore occupy a 2-byte varint (0x4054 / 0x4041).
    [Theory]
    [InlineData(0UL, "405400", "404100")]
    [InlineData(4UL, "405404", "404104")]
    [InlineData(256UL, "40544100", "40414100")]
    [InlineData(1UL << 40, "4054c000010000000000", "4041c000010000000000")]
    public void Preambles_Round_Trip(ulong sessionId, string uniHex, string bidiHex)
    {
        Span<byte> dst = stackalloc byte[16];
        int n = WebTransportFraming.WriteUnidirectionalPreamble(dst, sessionId);
        Assert.Equal(uniHex, TestUtil.ToHex(dst.Slice(0, n)));
        Assert.Equal(n, WebTransportFraming.GetPreambleLength(sessionId, false));
        Assert.Equal(WebTransportPreambleStatus.Ok, WebTransportFraming.TryReadUnidirectionalPreamble(dst.Slice(0, n), out ulong id, out int consumed));
        Assert.Equal(sessionId, id);
        Assert.Equal(n, consumed);

        n = WebTransportFraming.WriteBidirectionalPreamble(dst, sessionId);
        Assert.Equal(bidiHex, TestUtil.ToHex(dst.Slice(0, n)));
        Assert.Equal(n, WebTransportFraming.GetPreambleLength(sessionId, true));
        Assert.Equal(WebTransportPreambleStatus.Ok, WebTransportFraming.TryReadBidirectionalPreamble(dst.Slice(0, n), out id, out consumed));
        Assert.Equal(sessionId, id);
        Assert.Equal(n, consumed);

        // Session id alone (signal already consumed by the caller).
        Assert.Equal(WebTransportPreambleStatus.Ok, WebTransportFraming.TryReadSessionId(dst.Slice(2, n - 2), out id, out consumed));
        Assert.Equal(sessionId, id);
        Assert.Equal(n - 2, consumed);
    }

    [Fact]
    public void Invalid_Session_Ids()
    {
        Assert.False(WebTransportFraming.IsValidSessionId(1));
        Assert.False(WebTransportFraming.IsValidSessionId(2));
        Assert.False(WebTransportFraming.IsValidSessionId(3));
        Assert.True(WebTransportFraming.IsValidSessionId(4));
        Assert.Equal(-1, WebTransportFraming.GetPreambleLength(2, true));
        Assert.Equal(-1, WebTransportFraming.WriteUnidirectionalPreamble(stackalloc byte[16], 2));
        Assert.Equal(-1, WebTransportFraming.WriteBidirectionalPreamble(stackalloc byte[16], 3));

        // On the wire: 0x54 (2-byte varint) then session id 2.
        Assert.Equal(WebTransportPreambleStatus.InvalidSessionId, WebTransportFraming.TryReadUnidirectionalPreamble(new byte[] { 0x40, 0x54, 0x02 }, out ulong id, out int consumed));
        Assert.Equal(0, consumed);
        Assert.Equal(WebTransportPreambleStatus.InvalidSessionId, WebTransportFraming.TryReadBidirectionalPreamble(new byte[] { 0x40, 0x41, 0x01 }, out id, out consumed));
        Assert.Equal(0, consumed);
        Assert.Equal(WebTransportPreambleStatus.InvalidSessionId, WebTransportFraming.TryReadSessionId(new byte[] { 0x03 }, out id, out consumed));
        Assert.Equal(0, consumed);
    }

    [Fact]
    public void Write_Fails_When_Too_Small()
    {
        Assert.Equal(-1, WebTransportFraming.WriteUnidirectionalPreamble(Span<byte>.Empty, 0));
        Assert.Equal(-1, WebTransportFraming.WriteUnidirectionalPreamble(stackalloc byte[1], 0)); // signal needs 2 bytes
        Assert.Equal(-1, WebTransportFraming.WriteUnidirectionalPreamble(stackalloc byte[2], 0)); // session id needs 1 more
        Assert.Equal(-1, WebTransportFraming.WriteBidirectionalPreamble(stackalloc byte[3], 256));
        Assert.Equal(4, WebTransportFraming.WriteBidirectionalPreamble(stackalloc byte[4], 256));
    }

    [Fact]
    public void Read_Reports_Need_More_Data_And_Wrong_Signal()
    {
        Assert.Equal(WebTransportPreambleStatus.NeedMoreData, WebTransportFraming.TryReadUnidirectionalPreamble(ReadOnlySpan<byte>.Empty, out _, out _));
        Assert.Equal(WebTransportPreambleStatus.NeedMoreData, WebTransportFraming.TryReadUnidirectionalPreamble(new byte[] { 0x40 }, out _, out int consumed)); // signal truncated
        Assert.Equal(0, consumed);
        Assert.Equal(WebTransportPreambleStatus.NeedMoreData, WebTransportFraming.TryReadUnidirectionalPreamble(new byte[] { 0x40, 0x54 }, out _, out consumed)); // session id missing
        Assert.Equal(0, consumed);
        Assert.Equal(WebTransportPreambleStatus.NeedMoreData, WebTransportFraming.TryReadUnidirectionalPreamble(new byte[] { 0x40, 0x54, 0x41 }, out _, out _)); // session id truncated
        Assert.Equal(WebTransportPreambleStatus.NeedMoreData, WebTransportFraming.TryReadSessionId(ReadOnlySpan<byte>.Empty, out _, out _));

        Assert.Equal(WebTransportPreambleStatus.NotWebTransport, WebTransportFraming.TryReadUnidirectionalPreamble(new byte[] { 0x00, 0x04 }, out _, out consumed));
        Assert.Equal(0, consumed);
        Assert.Equal(WebTransportPreambleStatus.NotWebTransport, WebTransportFraming.TryReadBidirectionalPreamble(new byte[] { 0x40, 0x54, 0x04 }, out _, out _));
        Assert.Equal(WebTransportPreambleStatus.NotWebTransport, WebTransportFraming.TryReadUnidirectionalPreamble(new byte[] { 0x40, 0x41, 0x04 }, out _, out _));
    }

    [Fact]
    public void Error_Code_Mapping_Matches_Draft()
    {
        Assert.Equal(0x52e4a40fa8dbUL, WebTransportErrorCode.ToHttp3(0));
        Assert.Equal(0x52e4a40fa8dbUL + 0x1d, WebTransportErrorCode.ToHttp3(0x1d));
        Assert.Equal(0x52e4a40fa8dbUL + 0x1f, WebTransportErrorCode.ToHttp3(0x1e)); // skips the grease slot
        Assert.Equal(0x52e5ac983162UL, WebTransportErrorCode.ToHttp3(uint.MaxValue));

        Assert.True(WebTransportErrorCode.TryFromHttp3(0x52e4a40fa8dbUL, out uint app));
        Assert.Equal(0u, app);
        Assert.True(WebTransportErrorCode.TryFromHttp3(0x52e5ac983162UL, out app));
        Assert.Equal(uint.MaxValue, app);
        Assert.False(WebTransportErrorCode.TryFromHttp3(0x52e4a40fa8daUL, out app));
        Assert.Equal(0u, app);
        Assert.False(WebTransportErrorCode.TryFromHttp3(0x52e5ac983163UL, out _));
        Assert.False(WebTransportErrorCode.TryFromHttp3(0x52e4a40fa8dbUL + 0x1e, out _)); // grease slot
        Assert.False(WebTransportErrorCode.TryFromHttp3(0x100, out _));

        var rng = new Random(3);
        for (int i = 0; i < 10_000; i++)
        {
            uint code = (uint)rng.NextInt64(0, 1L << 32);
            ulong h3 = WebTransportErrorCode.ToHttp3(code);
            Assert.True(WebTransportErrorCode.TryFromHttp3(h3, out uint back));
            Assert.Equal(code, back);
            Assert.False(Http3Grease.IsReserved(h3));
        }
    }
}
