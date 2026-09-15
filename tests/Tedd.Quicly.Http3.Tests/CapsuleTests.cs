namespace Tedd.Quicly.Http3.Tests;

public class CapsuleTests
{
    [Fact]
    public void Capsule_Type_Values()
    {
        Assert.Equal(0x00UL, (ulong)CapsuleType.Datagram);
        Assert.Equal(0x2843UL, (ulong)CapsuleType.CloseWebTransportSession);
        Assert.Equal(0x78aeUL, (ulong)CapsuleType.DrainWebTransportSession);
    }

    [Fact]
    public void Generic_Capsule_Round_Trip()
    {
        Span<byte> dst = stackalloc byte[32];
        byte[] payload = [0xaa, 0xbb];
        int n = CapsuleWriter.Write(dst, CapsuleType.Datagram, payload);
        Assert.Equal(4, n);
        Assert.Equal("0002aabb", TestUtil.ToHex(dst.Slice(0, n)));
        Assert.Equal(n, CapsuleWriter.GetLength((ulong)CapsuleType.Datagram, 2));
        Assert.True(CapsuleReader.TryRead(dst.Slice(0, n), out ulong type, out ReadOnlySpan<byte> back, out int consumed));
        Assert.Equal((ulong)CapsuleType.Datagram, type);
        Assert.Equal(payload, back.ToArray());
        Assert.Equal(n, consumed);

        n = CapsuleWriter.Write(dst, 0x1234UL, ReadOnlySpan<byte>.Empty);
        Assert.Equal(3, n);
        Assert.Equal("523400", TestUtil.ToHex(dst.Slice(0, n)));
        Assert.Equal(2, CapsuleWriter.WriteHeader(dst, 0x21UL, 5));
        Assert.Equal("2105", TestUtil.ToHex(dst.Slice(0, 2)));
        Assert.Equal(-1, CapsuleWriter.GetLength(ulong.MaxValue, 0));
        Assert.Equal(-1, CapsuleWriter.WriteHeader(Span<byte>.Empty, CapsuleType.Datagram, 0));
        Assert.Equal(-1, CapsuleWriter.Write(stackalloc byte[2], CapsuleType.Datagram, payload));
        Assert.False(CapsuleReader.TryRead(new byte[] { 0x00, 0x05, 0x01 }, out _, out _, out _));
    }

    [Fact]
    public void Close_Session_Capsule_Round_Trip()
    {
        Span<byte> dst = stackalloc byte[64];
        byte[] reason = TestUtil.Ascii("bye");
        int n = CapsuleWriter.WriteCloseSession(dst, 0x01020304, reason);
        // type 0x2843 -> 2-byte varint 0x6843, length 7, code BE, reason
        Assert.Equal("684307" + "01020304" + "627965", TestUtil.ToHex(dst.Slice(0, n)));
        Assert.True(CapsuleReader.TryRead(dst.Slice(0, n), out ulong type, out ReadOnlySpan<byte> payload, out int consumed));
        Assert.Equal((ulong)CapsuleType.CloseWebTransportSession, type);
        Assert.Equal(n, consumed);
        Assert.True(CapsuleReader.TryParseCloseSession(payload, out uint code, out ReadOnlySpan<byte> back));
        Assert.Equal(0x01020304u, code);
        Assert.Equal("bye", TestUtil.AsciiString(back));

        // Empty reason.
        n = CapsuleWriter.WriteCloseSession(dst, 0, ReadOnlySpan<byte>.Empty);
        Assert.Equal("684304" + "00000000", TestUtil.ToHex(dst.Slice(0, n)));
        Assert.True(CapsuleReader.TryRead(dst.Slice(0, n), out _, out payload, out _));
        Assert.True(CapsuleReader.TryParseCloseSession(payload, out code, out back));
        Assert.Equal(0u, code);
        Assert.True(back.IsEmpty);
    }

    [Fact]
    public void Close_Session_Limits_And_Validation()
    {
        Span<byte> dst = stackalloc byte[2048];
        Assert.Equal(-1, CapsuleWriter.WriteCloseSession(dst, 1, new byte[CapsuleWriter.MaxCloseReasonLength + 1]));
        Assert.True(CapsuleWriter.WriteCloseSession(dst, 1, new byte[CapsuleWriter.MaxCloseReasonLength]) > 0);
        Assert.Equal(-1, CapsuleWriter.WriteCloseSession(stackalloc byte[2], 1, ReadOnlySpan<byte>.Empty));
        Assert.Equal(-1, CapsuleWriter.WriteCloseSession(stackalloc byte[6], 1, ReadOnlySpan<byte>.Empty));

        Assert.False(CapsuleReader.TryParseCloseSession(new byte[] { 0, 0, 0 }, out uint code, out ReadOnlySpan<byte> reason));
        Assert.Equal(0u, code);
        Assert.True(reason.IsEmpty);
        byte[] tooLong = new byte[4 + CapsuleWriter.MaxCloseReasonLength + 1];
        Assert.False(CapsuleReader.TryParseCloseSession(tooLong, out _, out _));
        byte[] invalidUtf8 = [0, 0, 0, 1, 0xff, 0xfe];
        Assert.False(CapsuleReader.TryParseCloseSession(invalidUtf8, out _, out _));
        byte[] validUtf8 = [0, 0, 0, 1, 0xc3, 0xa6];
        Assert.True(CapsuleReader.TryParseCloseSession(validUtf8, out code, out reason));
        Assert.Equal(1u, code);
        Assert.Equal(2, reason.Length);
    }

    [Fact]
    public void Drain_Session_Capsule()
    {
        Span<byte> dst = stackalloc byte[8];
        int n = CapsuleWriter.WriteDrainSession(dst);
        Assert.Equal(5, n);
        Assert.Equal("800078ae00", TestUtil.ToHex(dst.Slice(0, n))); // 0x78ae needs a 4-byte varint
        Assert.True(CapsuleReader.TryRead(dst.Slice(0, n), out ulong type, out ReadOnlySpan<byte> payload, out _));
        Assert.Equal((ulong)CapsuleType.DrainWebTransportSession, type);
        Assert.True(payload.IsEmpty);
        Assert.Equal(-1, CapsuleWriter.WriteDrainSession(stackalloc byte[2]));
    }

    [Fact]
    public void Capsules_Split_Across_Chunks_Via_Frame_Reader()
    {
        byte[] stream = new byte[64];
        int a = CapsuleWriter.WriteCloseSession(stream, 7, TestUtil.Ascii("reason"));
        int b = CapsuleWriter.WriteDrainSession(stream.AsSpan(a));
        int total = a + b;
        var reader = new Http3FrameReader(1024);
        var got = new List<(ulong Type, byte[] Payload)>();
        var partial = new MemoryStream();
        for (int i = 0; i < total; i++)
        {
            Http3FrameReadStatus s = reader.Read(stream.AsSpan(i, 1), out int consumed, out ReadOnlySpan<byte> payload);
            Assert.Equal(1, consumed);
            if (s == Http3FrameReadStatus.PayloadFragment || s == Http3FrameReadStatus.PayloadEnd) partial.Write(payload);
            if (s == Http3FrameReadStatus.Frame) partial.Write(payload);
            if (s == Http3FrameReadStatus.Frame || s == Http3FrameReadStatus.PayloadEnd)
            {
                got.Add((reader.Type, partial.ToArray()));
                partial.SetLength(0);
            }
        }
        Assert.Equal(2, got.Count);
        Assert.Equal((ulong)CapsuleType.CloseWebTransportSession, got[0].Type);
        Assert.True(CapsuleReader.TryParseCloseSession(got[0].Payload, out uint code, out ReadOnlySpan<byte> reason));
        Assert.Equal(7u, code);
        Assert.Equal("reason", TestUtil.AsciiString(reason));
        Assert.Equal((ulong)CapsuleType.DrainWebTransportSession, got[1].Type);
        Assert.Empty(got[1].Payload);
    }

    [Fact]
    public void Flow_Control_Capsule_Type_Values()
    {
        Assert.Equal(0x190B4D3DUL, (ulong)CapsuleType.WebTransportMaxData);
        Assert.Equal(0x190B4D3FUL, (ulong)CapsuleType.WebTransportMaxStreamsBidi);
        Assert.Equal(0x190B4D40UL, (ulong)CapsuleType.WebTransportMaxStreamsUni);
        Assert.Equal(0x190B4D41UL, (ulong)CapsuleType.WebTransportDataBlocked);
        Assert.Equal(0x190B4D43UL, (ulong)CapsuleType.WebTransportStreamsBlockedBidi);
        Assert.Equal(0x190B4D44UL, (ulong)CapsuleType.WebTransportStreamsBlockedUni);
        Assert.Equal(32UL * 1024, CapsuleReader.MaxCapsuleLength);

        foreach (CapsuleType t in Enum.GetValues<CapsuleType>())
        {
            bool flow = t is CapsuleType.WebTransportMaxData or CapsuleType.WebTransportMaxStreamsBidi or CapsuleType.WebTransportMaxStreamsUni
                or CapsuleType.WebTransportDataBlocked or CapsuleType.WebTransportStreamsBlockedBidi or CapsuleType.WebTransportStreamsBlockedUni;
            bool streams = t is CapsuleType.WebTransportMaxStreamsBidi or CapsuleType.WebTransportMaxStreamsUni
                or CapsuleType.WebTransportStreamsBlockedBidi or CapsuleType.WebTransportStreamsBlockedUni;
            Assert.Equal(flow, t.IsWebTransportFlowControl());
            Assert.Equal(streams, t.IsWebTransportStreamCount());
        }
    }

    [Theory]
    [InlineData(CapsuleType.WebTransportMaxData, 0UL, "990b4d3d" + "01" + "00")]
    [InlineData(CapsuleType.WebTransportMaxData, 1UL << 20, "990b4d3d" + "04" + "80100000")]
    [InlineData(CapsuleType.WebTransportMaxStreamsBidi, 16UL, "990b4d3f" + "01" + "10")]
    [InlineData(CapsuleType.WebTransportMaxStreamsUni, 1UL << 60, "990b4d40" + "08" + "d000000000000000")]
    [InlineData(CapsuleType.WebTransportDataBlocked, 300UL, "990b4d41" + "02" + "412c")]
    [InlineData(CapsuleType.WebTransportStreamsBlockedBidi, 1UL, "990b4d43" + "01" + "01")]
    [InlineData(CapsuleType.WebTransportStreamsBlockedUni, 63UL, "990b4d44" + "01" + "3f")]
    public void Flow_Control_Capsule_Round_Trip(CapsuleType type, ulong value, string hex)
    {
        Span<byte> dst = stackalloc byte[32];
        int n = CapsuleWriter.WriteFlowControl(dst, type, value);
        Assert.Equal(hex, TestUtil.ToHex(dst.Slice(0, n)));
        Assert.Equal(n, CapsuleWriter.GetFlowControlLength(type, value));
        Assert.True(CapsuleReader.TryRead(dst.Slice(0, n), out ulong t, out ReadOnlySpan<byte> payload, out int consumed));
        Assert.Equal((ulong)type, t);
        Assert.Equal(n, consumed);
        Assert.True(CapsuleReader.TryParseFlowControl(type, payload, out ulong back));
        Assert.Equal(value, back);
        for (int size = 0; size < n; size++) Assert.Equal(-1, CapsuleWriter.WriteFlowControl(dst.Slice(0, size), type, value));
    }

    [Fact]
    public void Flow_Control_Capsule_Validation()
    {
        Span<byte> dst = stackalloc byte[32];
        // Not a flow-control capsule.
        Assert.Equal(-1, CapsuleWriter.WriteFlowControl(dst, CapsuleType.Datagram, 1));
        Assert.Equal(-1, CapsuleWriter.WriteFlowControl(dst, CapsuleType.CloseWebTransportSession, 1));
        Assert.Equal(-1, CapsuleWriter.GetFlowControlLength(CapsuleType.DrainWebTransportSession, 1));
        Assert.False(CapsuleReader.TryParseFlowControl(CapsuleType.Datagram, new byte[] { 0x01 }, out ulong v));
        Assert.Equal(0UL, v);

        // Stream counts above 2^60 are illegal; data values up to 2^62-1 are fine, above is unencodable.
        Assert.Equal(-1, CapsuleWriter.WriteFlowControl(dst, CapsuleType.WebTransportMaxStreamsBidi, (1UL << 60) + 1));
        Assert.Equal(-1, CapsuleWriter.GetFlowControlLength(CapsuleType.WebTransportStreamsBlockedUni, (1UL << 60) + 1));
        Assert.True(CapsuleWriter.WriteFlowControl(dst, CapsuleType.WebTransportMaxData, (1UL << 62) - 1) > 0);
        Assert.Equal(-1, CapsuleWriter.WriteFlowControl(dst, CapsuleType.WebTransportMaxData, 1UL << 62));
        Assert.Equal(-1, CapsuleWriter.GetFlowControlLength(CapsuleType.WebTransportMaxData, 1UL << 62));
        Assert.False(CapsuleReader.TryParseFlowControl(CapsuleType.WebTransportMaxStreamsUni, TestUtil.Hex("d000000000000001"), out _)); // 2^60 + 1
        Assert.True(CapsuleReader.TryParseFlowControl(CapsuleType.WebTransportMaxData, TestUtil.Hex("d000000000000001"), out v));
        Assert.Equal((1UL << 60) + 1, v);

        // Payload must be exactly one varint: empty, truncated and trailing bytes all fail.
        Assert.False(CapsuleReader.TryParseFlowControl(CapsuleType.WebTransportMaxData, ReadOnlySpan<byte>.Empty, out _));
        Assert.False(CapsuleReader.TryParseFlowControl(CapsuleType.WebTransportMaxData, new byte[] { 0x40 }, out _));
        Assert.False(CapsuleReader.TryParseFlowControl(CapsuleType.WebTransportMaxData, new byte[] { 0x01, 0x00 }, out _));
    }

    [Fact]
    public void Max_Capsule_Length_Guard_Rejects_Oversized_Capsule()
    {
        var reader = new Http3FrameReader(CapsuleReader.MaxCapsuleLength);
        byte[] header = new byte[8];
        int n = CapsuleWriter.WriteHeader(header, CapsuleType.Datagram, CapsuleReader.MaxCapsuleLength + 1);
        Assert.Equal(Http3FrameReadStatus.FrameTooLarge, reader.Read(header.AsSpan(0, n), out _, out _));
        reader.Reset();
        n = CapsuleWriter.WriteHeader(header, CapsuleType.Datagram, CapsuleReader.MaxCapsuleLength);
        Assert.Equal(Http3FrameReadStatus.NeedMoreData, reader.Read(header.AsSpan(0, n), out int consumed, out _));
        Assert.Equal(n, consumed);
        Assert.True(reader.InPayload);
    }
}
