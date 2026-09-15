using K4os.Compression.LZ4;
using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Core.Tests.Primitives;

public class Lz4BlockTests
{
    private static int ReferenceEncode(ReadOnlySpan<byte> source, Span<byte> target, LZ4Level level = LZ4Level.L00_FAST) =>
        LZ4Codec.Encode(source, target, level);

    private static int ReferenceDecode(ReadOnlySpan<byte> source, Span<byte> target) =>
        LZ4Codec.Decode(source, target);

    private static byte[] CompressWithOurs(byte[] input, Span<int> scratch)
    {
        byte[] buffer = new byte[Lz4Block.GetMaxCompressedLength(input.Length)];
        int n = scratch.IsEmpty ? Lz4Block.Compress(input, buffer) : Lz4Block.Compress(input, buffer, scratch);
        Assert.InRange(n, 1, buffer.Length);
        return buffer.AsSpan(0, n).ToArray();
    }

    private static void AssertRoundTrip(byte[] input, Span<int> scratch)
    {
        byte[] compressed = CompressWithOurs(input, scratch);

        // Ours -> ours.
        byte[] output = new byte[input.Length];
        Assert.Equal(input.Length, Lz4Block.Decompress(compressed, output));
        Assert.Equal(input, output);

        // Ours -> reference.
        if (input.Length > 0)
        {
            byte[] referenceOutput = new byte[input.Length];
            Assert.Equal(input.Length, ReferenceDecode(compressed, referenceOutput));
            Assert.Equal(input, referenceOutput);
        }

        // Reference -> ours (fast and high-compression encoders produce different sequences).
        if (input.Length > 0)
        {
            foreach (LZ4Level level in new[] { LZ4Level.L00_FAST, LZ4Level.L09_HC, LZ4Level.L12_MAX })
            {
                byte[] referenceBuffer = new byte[LZ4Codec.MaximumOutputSize(input.Length)];
                int rn = ReferenceEncode(input, referenceBuffer, level);
                Assert.InRange(rn, 1, referenceBuffer.Length);
                output.AsSpan().Clear();
                Assert.Equal(input.Length, Lz4Block.Decompress(referenceBuffer.AsSpan(0, rn), output));
                Assert.Equal(input, output);
            }
        }

        // Larger destination is fine, exact destination is fine, one byte short is not.
        byte[] large = new byte[input.Length + 17];
        Assert.Equal(input.Length, Lz4Block.Decompress(compressed, large));
        Assert.Equal(input, large.AsSpan(0, input.Length).ToArray());
        if (input.Length > 0)
        {
            Assert.Equal(-1, Lz4Block.Decompress(compressed, new byte[input.Length - 1]));
        }

        // Exact mode: only a destination of precisely the decoded size is accepted.
        output.AsSpan().Clear();
        Assert.True(Lz4Block.DecompressExact(compressed, output));
        Assert.Equal(input, output);
        Assert.False(Lz4Block.DecompressExact(compressed, new byte[input.Length + 1]));
        Assert.False(Lz4Block.DecompressExact(compressed, large));
        if (input.Length > 0)
        {
            Assert.False(Lz4Block.DecompressExact(compressed, new byte[input.Length - 1]));
        }
    }

    [Fact]
    public void DecompressExact_Requires_Exact_Length()
    {
        // Empty block into an empty destination is exact; into anything larger it is not.
        Assert.True(Lz4Block.DecompressExact(new byte[] { 0 }, Span<byte>.Empty));
        Assert.False(Lz4Block.DecompressExact(new byte[] { 0 }, new byte[1]));
        Assert.False(Lz4Block.DecompressExact(ReadOnlySpan<byte>.Empty, Span<byte>.Empty));

        // "AAAAABC" (7 bytes) from a hand-built block.
        byte[] block = { 0x10, (byte)'A', 0x01, 0x00, 0x20, (byte)'B', (byte)'C' };
        Assert.True(Lz4Block.DecompressExact(block, new byte[7]));
        Assert.False(Lz4Block.DecompressExact(block, new byte[6]));
        Assert.False(Lz4Block.DecompressExact(block, new byte[8]));
        Assert.False(Lz4Block.DecompressExact(block, new byte[64]));

        // Malformed input is never "exact", whatever the destination size.
        foreach ((byte[] malformed, _) in MalformedBlockList)
        {
            for (int size = 0; size <= 8; size++)
            {
                Assert.False(Lz4Block.DecompressExact(malformed, new byte[size]));
            }
        }

        // A message whose RawLength claim is too small (destination shorter than the real payload) fails even
        // though a lenient decoder with a larger buffer would succeed.
        byte[] payload = PrimitiveTestData.GameLike(5000, 21);
        byte[] compressed = CompressWithOurs(payload, new int[Lz4Block.DefaultScratchLength]);
        Assert.True(Lz4Block.DecompressExact(compressed, new byte[5000]));
        Assert.False(Lz4Block.DecompressExact(compressed, new byte[4999]));
        Assert.False(Lz4Block.DecompressExact(compressed, new byte[5001]));
    }

    [Fact]
    public void Constants()
    {
        Assert.Equal(256, Lz4Block.MinScratchLength);
        Assert.Equal(65536, Lz4Block.MaxScratchLength);
        Assert.Equal(4096, Lz4Block.DefaultScratchLength);
        Assert.Equal(0x7E000000, Lz4Block.MaxInputLength);
    }

    [Theory]
    [InlineData(0, 16)]
    [InlineData(1, 17)]
    [InlineData(254, 270)]
    [InlineData(255, 272)]
    [InlineData(65536, 65536 + 257 + 16)]
    public void GetMaxCompressedLength_Matches_Reference_Bound(int input, int expected)
    {
        Assert.Equal(expected, Lz4Block.GetMaxCompressedLength(input));
        Assert.Equal(LZ4Codec.MaximumOutputSize(input), Lz4Block.GetMaxCompressedLength(input));
    }

    [Fact]
    public void GetMaxCompressedLength_Rejects_Out_Of_Range()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => Lz4Block.GetMaxCompressedLength(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => Lz4Block.GetMaxCompressedLength(Lz4Block.MaxInputLength + 1));
        Assert.Equal(Lz4Block.MaxInputLength + Lz4Block.MaxInputLength / 255 + 16, Lz4Block.GetMaxCompressedLength(Lz4Block.MaxInputLength));
    }

    [Fact]
    public unsafe void Compress_Rejects_Input_Longer_Than_Max()
    {
        // A span with a fake length: the length check runs before any byte is touched.
        byte* p = stackalloc byte[16];
        byte* q = stackalloc byte[64];
        nint source = (nint)p;
        nint target = (nint)q;
        int[] scratch = new int[Lz4Block.DefaultScratchLength];
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            Lz4Block.Compress(new ReadOnlySpan<byte>((byte*)source, Lz4Block.MaxInputLength + 1), new Span<byte>((byte*)target, 64), scratch));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            Lz4Block.Compress(new ReadOnlySpan<byte>((byte*)source, Lz4Block.MaxInputLength + 1), new Span<byte>((byte*)target, 64)));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(255)]
    [InlineData(300)]
    [InlineData(4095)]
    [InlineData(6144)]
    [InlineData(131072)]
    public void Compress_Rejects_Bad_Scratch_Length(int length)
    {
        byte[] input = PrimitiveTestData.GameLike(100, 1);
        byte[] output = new byte[Lz4Block.GetMaxCompressedLength(input.Length)];
        int[] scratch = new int[length];
        ArgumentException ex = Assert.Throws<ArgumentException>(() => Lz4Block.Compress(input, output, scratch));
        Assert.Equal("scratch", ex.ParamName);
    }

    [Theory]
    [InlineData(256)]
    [InlineData(4096)]
    [InlineData(16384)]
    [InlineData(65536)]
    public void Compress_Accepts_Every_Supported_Scratch_Length(int length)
    {
        byte[] input = PrimitiveTestData.GameLike(20_000, 2);
        AssertRoundTrip(input, new int[length]);
    }

    [Fact]
    public void Garbage_In_Scratch_Does_Not_Matter()
    {
        int[] scratch = new int[Lz4Block.DefaultScratchLength];
        Random random = new(5);
        for (int i = 0; i < scratch.Length; i++)
        {
            scratch[i] = (i % 4) switch
            {
                0 => int.MinValue,
                1 => int.MaxValue,
                2 => random.Next(),
                _ => -random.Next(),
            };
        }

        AssertRoundTrip(PrimitiveTestData.GameLike(3_000, 3), scratch);
        AssertRoundTrip(PrimitiveTestData.Repetitive(3_000, 3), scratch);
        AssertRoundTrip(PrimitiveTestData.Random(3_000, 3), scratch);

        // Reuse across calls with stale positions from a longer previous input.
        AssertRoundTrip(PrimitiveTestData.GameLike(200_000, 4), scratch);
        AssertRoundTrip(PrimitiveTestData.GameLike(50, 4), scratch);
        AssertRoundTrip(PrimitiveTestData.Repetitive(20, 4), scratch);
    }

    [Fact]
    public void Empty_Input_Is_A_Single_Zero_Token()
    {
        byte[] output = new byte[16];
        Assert.Equal(1, Lz4Block.Compress(ReadOnlySpan<byte>.Empty, output));
        Assert.Equal(0, output[0]);
        Assert.Equal(1, Lz4Block.Compress(ReadOnlySpan<byte>.Empty, output, new int[4096]));
        Assert.Equal(-1, Lz4Block.Compress(ReadOnlySpan<byte>.Empty, Span<byte>.Empty));

        Assert.Equal(0, Lz4Block.Decompress(new byte[] { 0 }, Span<byte>.Empty));
        Assert.Equal(0, Lz4Block.Decompress(new byte[] { 0 }, new byte[8]));
        Assert.Equal(-1, Lz4Block.Decompress(ReadOnlySpan<byte>.Empty, new byte[8]));
        Assert.Equal(-1, Lz4Block.Decompress(ReadOnlySpan<byte>.Empty, Span<byte>.Empty));
    }

    [Fact]
    public void Round_Trips_Every_Length_0_To_70()
    {
        int[] scratch = new int[Lz4Block.DefaultScratchLength];
        for (int length = 0; length <= 70; length++)
        {
            AssertRoundTrip(PrimitiveTestData.Random(length, length), scratch);
            AssertRoundTrip(PrimitiveTestData.Repetitive(length, length, period: 3), scratch);
            AssertRoundTrip(PrimitiveTestData.Repetitive(length, length, period: 7), scratch);
            AssertRoundTrip(PrimitiveTestData.GameLike(length, length), scratch);
            AssertRoundTrip(new byte[length], scratch);
            AssertRoundTrip(PrimitiveTestData.Random(length, length), Span<int>.Empty);
        }
    }

    [Theory]
    [InlineData(71)]
    [InlineData(128)]
    [InlineData(1000)]
    [InlineData(1024)]
    [InlineData(4097)]
    [InlineData(65535)]
    [InlineData(65536)]
    [InlineData(65537)]
    [InlineData(70000)]
    [InlineData(131072 + 13)]
    [InlineData(1024 * 1024)]
    public void Round_Trips_Larger_Inputs(int length)
    {
        int[] scratch = new int[Lz4Block.DefaultScratchLength];
        AssertRoundTrip(PrimitiveTestData.Random(length, length), scratch);
        AssertRoundTrip(PrimitiveTestData.Repetitive(length, length), scratch);
        AssertRoundTrip(PrimitiveTestData.Repetitive(length, length, period: 1), scratch);
        AssertRoundTrip(PrimitiveTestData.GameLike(length, length), scratch);
        AssertRoundTrip(new byte[length], scratch);
    }

    [Fact]
    public void Round_Trips_Random_Sizes_Up_To_1_MiB()
    {
        Random random = new(99);
        int[] scratch = new int[Lz4Block.DefaultScratchLength];
        for (int i = 0; i < 40; i++)
        {
            int length = random.Next(0, 1024 * 1024 + 1);
            int seed = random.Next();
            switch (i % 4)
            {
                case 0:
                    AssertRoundTrip(PrimitiveTestData.Random(length, seed), scratch);
                    break;
                case 1:
                    AssertRoundTrip(PrimitiveTestData.Repetitive(length, seed, period: random.Next(1, 200)), scratch);
                    break;
                case 2:
                    AssertRoundTrip(PrimitiveTestData.GameLike(length, seed), scratch);
                    break;
                default:
                    AssertRoundTrip(PrimitiveTestData.GameLike(length, seed), Span<int>.Empty);
                    break;
            }
        }
    }

    [Fact]
    public void Compresses_Repetitive_Data_Well_And_Random_Data_Barely_Expands()
    {
        byte[] zeros = new byte[65536];
        byte[] buffer = new byte[Lz4Block.GetMaxCompressedLength(zeros.Length)];
        int n = Lz4Block.Compress(zeros, buffer);
        Assert.InRange(n, 1, 300);

        byte[] random = PrimitiveTestData.Random(65536, 1);
        n = Lz4Block.Compress(random, buffer);
        Assert.InRange(n, random.Length, Lz4Block.GetMaxCompressedLength(random.Length));

        byte[] game = PrimitiveTestData.GameLike(65536, 1);
        n = Lz4Block.Compress(game, buffer);
        Assert.InRange(n, 1, game.Length * 3 / 4);
    }

    [Theory]
    [InlineData(270)]
    [InlineData(300)]
    [InlineData(15 + 255 * 2)]
    [InlineData(15 + 255 * 3 + 7)]
    [InlineData(2000)]
    public void Long_Literal_Run_Before_A_Match_Uses_Extra_Length_Bytes(int literalRun)
    {
        // An incompressible run of `literalRun` bytes followed by a copy of its first 100 bytes and a tail: the
        // first sequence must carry a literal length of 15 + k*255 + r *and* a match, which is the only place
        // the mid-block length-byte loop runs (the final literal run has its own).
        byte[] head = PrimitiveTestData.Random(literalRun, literalRun);
        byte[] input = new byte[literalRun + 100 + 20];
        head.CopyTo(input, 0);
        head.AsSpan(0, 100).CopyTo(input.AsSpan(literalRun));
        PrimitiveTestData.Random(20, 3).CopyTo(input, literalRun + 100);

        int[] scratch = new int[Lz4Block.DefaultScratchLength];
        byte[] compressed = CompressWithOurs(input, scratch);

        // Token announces 15 literals, then (literalRun - 15) / 255 bytes of 255 and one remainder byte.
        Assert.Equal(0xF0, compressed[0] & 0xF0);
        int extra = (literalRun - 15) / 255;
        for (int i = 1; i <= extra; i++)
        {
            Assert.Equal(255, compressed[i]);
        }

        Assert.Equal((literalRun - 15) % 255, compressed[extra + 1]);
        Assert.True(compressed.Length < input.Length - 50, "the 100-byte repeat must have been matched");
        AssertRoundTrip(input, scratch);
    }

    [Fact]
    public void Long_Distance_Matches_Are_Limited_To_65535()
    {
        // Two identical 100-byte blobs 70 000 bytes apart cannot be matched; the same blobs 60 000 apart can.
        byte[] blob = PrimitiveTestData.Random(100, 77);
        byte[] far = new byte[70_000 + 100];
        blob.CopyTo(far, 0);
        blob.CopyTo(far, 70_000);
        byte[] near = new byte[60_000 + 100];
        blob.CopyTo(near, 0);
        blob.CopyTo(near, 60_000);
        int[] scratch = new int[Lz4Block.DefaultScratchLength];
        byte[] buffer = new byte[Lz4Block.GetMaxCompressedLength(far.Length)];
        int nFar = Lz4Block.Compress(far, buffer, scratch);
        int nNear = Lz4Block.Compress(near, buffer, scratch);
        Assert.True(nNear < nFar - 50);
        AssertRoundTrip(far, scratch);
        AssertRoundTrip(near, scratch);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(13)]
    [InlineData(64)]
    [InlineData(300)]
    [InlineData(1000)]
    [InlineData(4000)]
    public void Compress_Returns_Minus_One_For_Every_Too_Small_Destination(int length)
    {
        int[] scratch = new int[Lz4Block.DefaultScratchLength];
        foreach (byte[] input in new[] { PrimitiveTestData.Random(length, 1), PrimitiveTestData.Repetitive(length, 1), PrimitiveTestData.GameLike(length, 1), new byte[length] })
        {
            byte[] compressed = CompressWithOurs(input, scratch);
            byte[] buffer = new byte[compressed.Length];
            Assert.Equal(compressed.Length, Lz4Block.Compress(input, buffer, scratch));
            Assert.Equal(compressed, buffer);
            for (int size = 0; size < compressed.Length; size++)
            {
                Assert.Equal(-1, Lz4Block.Compress(input, buffer.AsSpan(0, size), scratch));
            }
        }
    }

    [Fact]
    public void Decompress_Returns_Minus_One_For_Every_Too_Small_Destination()
    {
        int[] scratch = new int[Lz4Block.DefaultScratchLength];
        foreach (byte[] input in new[] { PrimitiveTestData.Random(500, 2), PrimitiveTestData.Repetitive(500, 2), PrimitiveTestData.GameLike(500, 2), new byte[500] })
        {
            byte[] compressed = CompressWithOurs(input, scratch);
            byte[] output = new byte[input.Length];
            for (int size = 0; size < input.Length; size++)
            {
                Assert.Equal(-1, Lz4Block.Decompress(compressed, output.AsSpan(0, size)));
            }
        }
    }

    private static readonly (byte[] Block, string Description)[] MalformedBlockList =
    {
        ( new byte[] { 0x10 }, "literal run longer than input" ),
        ( new byte[] { 0xF0 }, "truncated literal length" ),
        ( new byte[] { 0xF0, 0xFF }, "truncated literal length after 255" ),
        ( new byte[] { 0xF0, 0x00 }, "15 literals announced, none present" ),
        ( new byte[] { 0x20, 0x41 }, "two literals announced, one present" ),
        ( new byte[] { 0x10, 0x41, 0x01 }, "one literal then half an offset" ),
        ( new byte[] { 0x10, 0x41, 0x00, 0x00 }, "offset zero" ),
        ( new byte[] { 0x10, 0x41, 0x02, 0x00 }, "offset before start of output" ),
        ( new byte[] { 0x1F, 0x41, 0x01, 0x00 }, "truncated match length" ),
        ( new byte[] { 0x1F, 0x41, 0x01, 0x00, 0xFF }, "truncated match length after 255" ),
        ( new byte[] { 0x10, 0x41, 0x01, 0x00 }, "block ends with a match instead of a literal-only sequence" ),
        ( new byte[] { 0x00, 0x00 }, "empty literals then truncated offset" ),
        ( new byte[] { 0x10, 0x41, 0x01, 0x00, 0x00, 0x01, 0x00 }, "second sequence: offset 1 then nothing" ),
    };

    public static TheoryData<byte[], string> MalformedBlocks
    {
        get
        {
            TheoryData<byte[], string> data = new();
            foreach ((byte[] block, string description) in MalformedBlockList)
            {
                data.Add(block, description);
            }

            return data;
        }
    }

    [Theory]
    [MemberData(nameof(MalformedBlocks))]
    public void Decompress_Rejects_Malformed_Blocks(byte[] block, string description)
    {
        byte[] output = new byte[64];
        int result = Lz4Block.Decompress(block, output);
        Assert.True(result == -1, $"{description}: expected -1, got {result}");
    }

    [Fact]
    public void Decompress_Accepts_Hand_Built_Sequences()
    {
        // "A" then match offset 1 length 4 => "AAAAA", then 2 literals "BC".
        byte[] block = { 0x10, (byte)'A', 0x01, 0x00, 0x20, (byte)'B', (byte)'C' };
        byte[] output = new byte[16];
        Assert.Equal(7, Lz4Block.Decompress(block, output));
        Assert.Equal("AAAAABC"u8.ToArray(), output.AsSpan(0, 7).ToArray());

        // Long literal run (15 + 255 + 3 = 273) then a long match (15 + 4 + 255 + 10 = 284) with a non-overlapping
        // offset, then the 5 trailing literals the reference decoder insists on.
        byte[] literals = PrimitiveTestData.Random(273, 11);
        List<byte> b = new() { 0xFF, 0xFF, 0x03 };
        b.AddRange(literals);
        b.Add(0x00); b.Add(0x01);          // offset 256
        b.Add(0xFF); b.Add(0x0A);          // match length extra 255 + 10
        b.Add(0x50);                       // final token: 5 literals
        b.AddRange("tail!"u8.ToArray());
        byte[] block2 = b.ToArray();
        const int expectedLength = 273 + 284 + 5;
        byte[] output2 = new byte[expectedLength];
        Assert.Equal(expectedLength, Lz4Block.Decompress(block2, output2));
        for (int i = 273; i < 273 + 284; i++)
        {
            Assert.Equal(output2[i - 256], output2[i]);
        }

        Assert.Equal("tail!"u8.ToArray(), output2.AsSpan(273 + 284).ToArray());
        Assert.Equal(expectedLength, ReferenceDecode(block2, new byte[expectedLength]));

        // Our decoder is more lenient than the reference: a match may run up to the very end of the block.
        byte[] block3 = b.ToArray().AsSpan(0, b.Count - 6).ToArray();
        block3 = block3.Concat(new byte[] { 0x00 }).ToArray();
        Assert.Equal(273 + 284, Lz4Block.Decompress(block3, output2));
        Assert.True(ReferenceDecode(block3, new byte[273 + 284]) < 0);
    }

    [Fact]
    public void Decompress_Handles_Every_Short_Offset_On_Both_Copy_Paths()
    {
        // Hand-built blocks: `offset` literal bytes of a distinct pattern, then a match of `matchLength` at that
        // offset (period = offset), then `tail` trailing literals. With tail >= 16 the match runs through the
        // overshooting fast path; with tail in 5..15 it runs through the exact-length path near the end of the
        // destination; a destination one byte larger than needed exercises the boundary of the slack check.
        foreach (int offset in new[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 12, 15, 16, 17, 31, 32, 33 })
        {
            foreach (int matchLength in new[] { 4, 5, 7, 8, 9, 15, 16, 17, 23, 24, 31, 32, 33, 64, 100, 1000 })
            {
                foreach (int tail in new[] { 5, 6, 12, 15, 16, 17, 40 })
                {
                    byte[] block = BuildBlock(offset, matchLength, tail, out byte[] expected);
                    foreach (int slack in new[] { 0, 1, 15, 16, 64 })
                    {
                        byte[] output = new byte[expected.Length + slack];
                        int decoded = Lz4Block.Decompress(block, output);
                        Assert.True(expected.Length == decoded, $"offset={offset} matchLength={matchLength} tail={tail} slack={slack}: expected {expected.Length}, got {decoded}");
                        Assert.Equal(expected, output.AsSpan(0, expected.Length).ToArray());
                        Assert.Equal(slack == 0, Lz4Block.DecompressExact(block, output));
                    }

                    if (tail >= 12)
                    {
                        // The reference decoder wants the encoder-side end rules (last match >= 12 bytes before the end).
                        int referenceDecoded = ReferenceDecode(block, new byte[expected.Length]);
                        Assert.True(expected.Length == referenceDecoded, $"offset={offset} matchLength={matchLength} tail={tail}: reference returned {referenceDecoded}");
                    }
                }
            }
        }

        static byte[] BuildBlock(int offset, int matchLength, int tail, out byte[] expected)
        {
            byte[] literals = new byte[offset];
            for (int i = 0; i < offset; i++)
            {
                literals[i] = (byte)(0x40 + i * 7);
            }

            byte[] tailBytes = PrimitiveTestData.Random(tail, offset * 1000 + matchLength);
            List<byte> b = new();
            AddSequence(b, literals, offset, matchLength);
            AddSequence(b, tailBytes, 0, 0);

            List<byte> e = new(literals);
            for (int i = 0; i < matchLength; i++)
            {
                e.Add(e[e.Count - offset]);
            }

            e.AddRange(tailBytes);
            expected = e.ToArray();
            return b.ToArray();
        }

        static void AddSequence(List<byte> b, byte[] literals, int offset, int matchLength)
        {
            int ml = matchLength == 0 ? 0 : matchLength - 4;
            int tokenIndex = b.Count;
            b.Add(0);
            int lit = literals.Length;
            if (lit >= 15)
            {
                b[tokenIndex] |= 0xF0;
                for (int n = lit - 15; ; n -= 255)
                {
                    if (n < 255)
                    {
                        b.Add((byte)n);
                        break;
                    }

                    b.Add(255);
                }
            }
            else
            {
                b[tokenIndex] |= (byte)(lit << 4);
            }

            b.AddRange(literals);
            if (matchLength == 0)
            {
                return;
            }

            b.Add((byte)offset);
            b.Add((byte)(offset >> 8));
            if (ml >= 15)
            {
                b[tokenIndex] |= 0x0F;
                for (int n = ml - 15; ; n -= 255)
                {
                    if (n < 255)
                    {
                        b.Add((byte)n);
                        break;
                    }

                    b.Add(255);
                }
            }
            else
            {
                b[tokenIndex] |= (byte)ml;
            }
        }
    }

    [Fact]
    public void Decompress_Never_Throws_On_Corrupted_Input()
    {
        Random random = new(2024);
        int[] scratch = new int[Lz4Block.DefaultScratchLength];
        byte[][] inputs =
        {
            PrimitiveTestData.GameLike(2000, 1),
            PrimitiveTestData.Repetitive(2000, 1),
            PrimitiveTestData.Random(200, 1),
            new byte[5000],
        };
        byte[] output = new byte[8192];
        int rejected = 0;
        int accepted = 0;
        foreach (byte[] input in inputs)
        {
            byte[] compressed = CompressWithOurs(input, scratch);
            for (int iteration = 0; iteration < 3000; iteration++)
            {
                byte[] corrupted = (byte[])compressed.Clone();
                switch (iteration % 5)
                {
                    case 0:
                        corrupted[random.Next(corrupted.Length)] = (byte)random.Next(256);
                        break;
                    case 1:
                        corrupted[random.Next(corrupted.Length)] ^= (byte)(1 << random.Next(8));
                        break;
                    case 2:
                        corrupted = corrupted.AsSpan(0, random.Next(corrupted.Length)).ToArray();
                        break;
                    case 3:
                        for (int k = 0; k < 8; k++)
                        {
                            corrupted[random.Next(corrupted.Length)] = (byte)random.Next(256);
                        }

                        break;
                    default:
                        corrupted = corrupted.Concat(PrimitiveTestData.Random(random.Next(1, 8), iteration)).ToArray();
                        break;
                }

                int result = Lz4Block.Decompress(corrupted, output);
                Assert.InRange(result, -1, output.Length);
                if (result < 0)
                {
                    rejected++;
                }
                else
                {
                    accepted++;
                }
            }
        }

        Assert.True(rejected > 0);
        Assert.True(accepted > 0);

        // Fully random "blocks".
        for (int iteration = 0; iteration < 5000; iteration++)
        {
            byte[] garbage = PrimitiveTestData.Random(random.Next(1, 64), iteration);
            Assert.InRange(Lz4Block.Decompress(garbage, output.AsSpan(0, random.Next(0, output.Length))), -1, output.Length);
        }
    }

    [Fact]
    public void Decompress_Rejects_Length_Overflow_Attempts()
    {
        // 15 + 255 * N literals where N makes the sum exceed any destination; must fail without overflow.
        byte[] block = new byte[100_000];
        block[0] = 0xF0;
        block.AsSpan(1).Fill(0xFF);
        Assert.Equal(-1, Lz4Block.Decompress(block, new byte[1024]));

        // Same for the match length after a valid single literal.
        block[0] = 0x1F;
        block[1] = 0x41;
        block[2] = 0x01;
        block[3] = 0x00;
        Assert.Equal(-1, Lz4Block.Decompress(block, new byte[1024]));
    }

    [Fact]
    public void Steady_State_Does_Not_Allocate()
    {
        byte[] input = PrimitiveTestData.GameLike(4096, 8);
        byte[] compressed = new byte[Lz4Block.GetMaxCompressedLength(input.Length)];
        byte[] output = new byte[input.Length];
        int[] scratch = new int[Lz4Block.DefaultScratchLength];
        int sink = 0;

        Run(input, compressed, output, scratch, ref sink);
        WindowedAllocation.AssertNone(() => Run(input, compressed, output, scratch, ref sink));
        Assert.NotEqual(0, sink);

        static void Run(byte[] input, byte[] compressed, byte[] output, int[] scratch, ref int sink)
        {
            for (int i = 0; i < 200; i++)
            {
                int n = Lz4Block.Compress(input, compressed, scratch);
                sink += n;
                sink += Lz4Block.Decompress(compressed.AsSpan(0, n), output);
                sink += Lz4Block.DecompressExact(compressed.AsSpan(0, n), output) ? 1 : 0;
                sink += Lz4Block.Compress(input, compressed);
                sink += Lz4Block.GetMaxCompressedLength(i);
            }
        }
    }
}
