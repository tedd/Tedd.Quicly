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
}
