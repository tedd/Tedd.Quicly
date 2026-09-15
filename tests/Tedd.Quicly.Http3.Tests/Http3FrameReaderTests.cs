namespace Tedd.Quicly.Http3.Tests;

public class Http3FrameReaderTests
{
    private sealed record Frame(ulong Type, byte[] Payload);

    private static byte[] Build(List<Frame> frames)
    {
        var buffer = new byte[1 << 16];
        int pos = 0;
        foreach (Frame f in frames)
        {
            int n = Http3FrameWriter.WriteFrame(buffer.AsSpan(pos), f.Type, f.Payload);
            Assert.True(n > 0);
            pos += n;
        }
        return buffer.AsSpan(0, pos).ToArray();
    }

    private static List<Frame> SampleFrames()
    {
        var rng = new Random(42);
        byte[] Rand(int n) { var b = new byte[n]; rng.NextBytes(b); return b; }
        return
        [
            new Frame((ulong)Http3FrameType.Settings, Rand(6)),
            new Frame((ulong)Http3FrameType.Headers, Rand(63)),
            new Frame((ulong)Http3FrameType.Data, Rand(64)),
            new Frame((ulong)Http3FrameType.Data, []),
            new Frame(0x21, Rand(3)),                         // grease frame
            new Frame(1UL << 40, Rand(1)),                    // 8-byte type varint
            new Frame((ulong)Http3FrameType.Data, Rand(300)), // 2-byte length varint
            new Frame((ulong)Http3FrameType.GoAway, Rand(2)),
            new Frame((ulong)Http3FrameType.Data, Rand(17000)), // 4-byte length varint
            new Frame((ulong)Http3FrameType.WebTransportStream, []),
        ];
    }

    /// <summary>Feeds chunks through the reader and reassembles frames (payload fragments concatenated).</summary>
    private static List<Frame> Drive(ref Http3FrameReader reader, IEnumerable<byte[]> chunks, out int zeroCopyFrames)
    {
        var result = new List<Frame>();
        var partial = new MemoryStream();
        zeroCopyFrames = 0;
        foreach (byte[] chunk in chunks)
        {
            ReadOnlySpan<byte> input = chunk;
            while (true)
            {
                Http3FrameReadStatus s = reader.Read(input, out int consumed, out ReadOnlySpan<byte> payload);
                input = input.Slice(consumed);
                switch (s)
                {
                    case Http3FrameReadStatus.NeedMoreData:
                        Assert.True(input.IsEmpty);
                        break;
                    case Http3FrameReadStatus.Frame:
                        Assert.Equal(0UL, reader.PayloadOffset);
                        Assert.Equal((ulong)payload.Length, reader.Length);
                        Assert.Equal(0, partial.Length);
                        result.Add(new Frame(reader.Type, payload.ToArray()));
                        zeroCopyFrames++;
                        break;
                    case Http3FrameReadStatus.PayloadFragment:
                        Assert.Equal((ulong)partial.Length, reader.PayloadOffset);
                        Assert.True(reader.InPayload);
                        partial.Write(payload);
                        break;
                    case Http3FrameReadStatus.PayloadEnd:
                        Assert.Equal((ulong)partial.Length, reader.PayloadOffset);
                        partial.Write(payload);
                        Assert.Equal((ulong)partial.Length, reader.Length);
                        result.Add(new Frame(reader.Type, partial.ToArray()));
                        partial.SetLength(0);
                        Assert.False(reader.InPayload);
                        break;
                    default:
                        Assert.Fail("unexpected status " + s);
                        break;
                }
                if (s == Http3FrameReadStatus.NeedMoreData) break;
            }
        }
        return result;
    }

    private static void AssertSame(List<Frame> expected, List<Frame> actual)
    {
        Assert.Equal(expected.Count, actual.Count);
        for (int i = 0; i < expected.Count; i++)
        {
            Assert.Equal(expected[i].Type, actual[i].Type);
            Assert.Equal(expected[i].Payload, actual[i].Payload);
        }
    }

    [Fact]
    public void Contiguous_Buffer_Yields_All_Frames_Zero_Copy()
    {
        List<Frame> frames = SampleFrames();
        byte[] stream = Build(frames);
        var reader = new Http3FrameReader(0);
        List<Frame> got = Drive(ref reader, [stream], out int zeroCopy);
        AssertSame(frames, got);
        Assert.Equal(frames.Count, zeroCopy);
        Assert.Equal(0UL, reader.MaxPayloadLength);
    }

    [Fact]
    public void Split_At_Every_Byte_Boundary()
    {
        List<Frame> frames = SampleFrames();
        byte[] stream = Build(frames);
        for (int split = 0; split <= stream.Length; split++)
        {
            var reader = new Http3FrameReader(1 << 20);
            List<Frame> got = Drive(ref reader, [stream[..split], stream[split..]], out _);
            AssertSame(frames, got);
        }
    }

    [Fact]
    public void Byte_By_Byte()
    {
        List<Frame> frames = SampleFrames();
        byte[] stream = Build(frames);
        var reader = new Http3FrameReader(1 << 20);
        List<Frame> got = Drive(ref reader, stream.Select(b => new[] { b }), out int zeroCopy);
        AssertSame(frames, got);
        // Only the empty-payload frames and the 1-byte payload frame can be zero-copy when fed one byte at a time.
        Assert.Equal(frames.Count(f => f.Payload.Length <= 1), zeroCopy);
    }

    [Fact]
    public void Random_Chunking()
    {
        List<Frame> frames = SampleFrames();
        byte[] stream = Build(frames);
        var rng = new Random(7);
        for (int iter = 0; iter < 200; iter++)
        {
            var chunks = new List<byte[]>();
            int pos = 0;
            while (pos < stream.Length)
            {
                int n = Math.Min(rng.Next(1, 500), stream.Length - pos);
                chunks.Add(stream[pos..(pos + n)]);
                pos += n;
            }
            var reader = new Http3FrameReader(1 << 20);
            AssertSame(frames, Drive(ref reader, chunks, out _));
        }
    }

    [Fact]
    public void Header_Then_Full_Payload_In_Next_Chunk_Is_Zero_Copy_Frame()
    {
        byte[] payload = new byte[100];
        byte[] stream = Build([new Frame(0, payload)]);
        var reader = new Http3FrameReader(0);
        // Feed exactly the header (type 1 byte + length 100 as a 2-byte varint).
        Assert.Equal(Http3FrameReadStatus.NeedMoreData, reader.Read(stream.AsSpan(0, 3), out int consumed, out _));
        Assert.Equal(3, consumed);
        Assert.True(reader.InPayload);
        Assert.Equal(Http3FrameReadStatus.Frame, reader.Read(stream.AsSpan(3), out consumed, out ReadOnlySpan<byte> p));
        Assert.Equal(100, consumed);
        Assert.Equal(100, p.Length);
        Assert.False(reader.InPayload);
        Assert.Equal(Http3FrameType.Data, reader.FrameType);
    }

    [Fact]
    public void Empty_Input_Returns_NeedMoreData()
    {
        var reader = new Http3FrameReader(0);
        Assert.Equal(Http3FrameReadStatus.NeedMoreData, reader.Read(ReadOnlySpan<byte>.Empty, out int consumed, out ReadOnlySpan<byte> payload));
        Assert.Equal(0, consumed);
        Assert.True(payload.IsEmpty);
    }

    [Fact]
    public void Max_Payload_Guard_Fails_And_Sticks_Until_Reset()
    {
        byte[] stream = Build([new Frame(0, new byte[10])]);
        var reader = new Http3FrameReader(9);
        Assert.Equal(Http3FrameReadStatus.FrameTooLarge, reader.Read(stream, out int consumed, out _));
        Assert.Equal(2, consumed);
        Assert.True(reader.HasFailed);
        Assert.Equal(Http3FrameReadStatus.FrameTooLarge, reader.Read(stream, out consumed, out _));
        Assert.Equal(0, consumed);
        reader.Reset();
        Assert.False(reader.HasFailed);
        var ok = new Http3FrameReader(10);
        Assert.Equal(Http3FrameReadStatus.Frame, ok.Read(stream, out consumed, out ReadOnlySpan<byte> p));
        Assert.Equal(10, p.Length);
    }

    [Fact]
    public void Max_Payload_Guard_Applies_On_Bytewise_Header_Path()
    {
        byte[] stream = Build([new Frame(0, new byte[300])]);
        var reader = new Http3FrameReader(100);
        Assert.Equal(Http3FrameReadStatus.NeedMoreData, reader.Read(stream.AsSpan(0, 2), out _, out _));
        Assert.Equal(Http3FrameReadStatus.FrameTooLarge, reader.Read(stream.AsSpan(2), out int consumed, out _));
        Assert.Equal(1, consumed);
    }

    [Fact]
    public void Reset_Discards_Partial_State()
    {
        byte[] stream = Build([new Frame(0x21, new byte[50])]);
        var reader = new Http3FrameReader(0);
        Assert.Equal(Http3FrameReadStatus.PayloadFragment, reader.Read(stream.AsSpan(0, 20), out _, out _));
        Assert.True(reader.InPayload);
        reader.Reset();
        Assert.False(reader.InPayload);
        Assert.Equal(0UL, reader.Type);
        Assert.Equal(0UL, reader.Length);
        Assert.Equal(Http3FrameReadStatus.Frame, reader.Read(stream, out _, out ReadOnlySpan<byte> p));
        Assert.Equal(50, p.Length);
        Assert.Equal(0x21UL, reader.Type);
    }

    [Fact]
    public void Default_Struct_Is_Usable()
    {
        Http3FrameReader reader = default;
        byte[] stream = Build([new Frame(1, [9, 9])]);
        Assert.Equal(Http3FrameReadStatus.Frame, reader.Read(stream, out int consumed, out ReadOnlySpan<byte> p));
        Assert.Equal(stream.Length, consumed);
        Assert.Equal(2, p.Length);
    }

    [Fact]
    public void TryReadFrame_Parses_Contiguous_Frames()
    {
        byte[] stream = Build([new Frame(4, [1, 2, 3]), new Frame(0, [])]);
        Assert.True(Http3FrameReader.TryReadFrame(stream, out ulong type, out ReadOnlySpan<byte> payload, out int consumed));
        Assert.Equal(4UL, type);
        Assert.Equal(new byte[] { 1, 2, 3 }, payload.ToArray());
        Assert.Equal(5, consumed);
        Assert.True(Http3FrameReader.TryReadFrame(stream.AsSpan(consumed), out type, out payload, out consumed));
        Assert.Equal(0UL, type);
        Assert.True(payload.IsEmpty);
        Assert.Equal(2, consumed);

        Assert.False(Http3FrameReader.TryReadFrame(ReadOnlySpan<byte>.Empty, out _, out _, out _));
        Assert.False(Http3FrameReader.TryReadFrame(new byte[] { 0x04 }, out _, out _, out _));
        Assert.False(Http3FrameReader.TryReadFrame(new byte[] { 0x04, 0x03, 0x01 }, out _, out _, out consumed));
        Assert.Equal(0, consumed);
    }
}
