using System.Runtime.InteropServices;
using K4os.Compression.LZ4;
using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Core.Tests.Primitives;

/// <summary>
/// <see cref="Lz4Block.TryDecompressInPlace"/>: the decode a compressed message of a reliable channel gets in its own
/// staging block (RC2-1). It must give exactly what the out-of-place decoder gives for every block the compressor writes
/// when the block has the margin, refuse a block one byte short, and never read or write outside the block whatever it is
/// handed.
/// </summary>
public unsafe class Lz4BlockInPlaceTests
{
    private static byte[] Compress(byte[] input)
    {
        byte[] buffer = new byte[Lz4Block.GetMaxCompressedLength(input.Length)];
        int n = Lz4Block.Compress(input, buffer);
        Assert.InRange(n, 1, buffer.Length);
        return buffer.AsSpan(0, n).ToArray();
    }

    /// <summary>The staging block a message gets: room for the compressed bytes, and for the decoded ones plus the margin.</summary>
    private static int BlockFor(int compressed, int raw) => Math.Max(compressed, raw + Lz4Block.GetInPlaceMargin(compressed));

    /// <summary>Decodes <paramref name="compressed"/> in place in a block of <paramref name="blockLength"/> bytes; null when refused.</summary>
    private static byte[]? InPlace(byte[] compressed, int raw, int blockLength)
    {
        byte[] block = new byte[blockLength];
        new Random(blockLength).NextBytes(block); // what the pool block held before: must not matter
        compressed.CopyTo(block, 0);
        return Lz4Block.TryDecompressInPlace(block, compressed.Length, raw) ? block.AsSpan(0, raw).ToArray() : null;
    }

    private static void AssertDecodesInPlace(byte[] input, byte[] compressed, string what)
    {
        // The out-of-place decoder is the reference: the same bytes, and the same verdict on the length.
        byte[] reference = new byte[input.Length];
        Assert.True(Lz4Block.DecompressExact(compressed, reference), what);
        Assert.Equal(input, reference);

        int block = BlockFor(compressed.Length, input.Length);
        byte[]? decoded = InPlace(compressed, input.Length, block);
        Assert.True(decoded is not null, $"{what}: refused in a block of raw + margin ({block} bytes, C {compressed.Length}, R {input.Length})");
        Assert.Equal(input, decoded);

        // Larger blocks decode too.
        Assert.Equal(input, InPlace(compressed, input.Length, block + 1));
        Assert.Equal(input, InPlace(compressed, input.Length, block + 4096));

        // One byte short of raw + margin is refused (unless the compressed bytes alone need the larger block).
        if (input.Length + Lz4Block.GetInPlaceMargin(compressed.Length) - 1 >= compressed.Length)
        {
            Assert.Null(InPlace(compressed, input.Length, input.Length + Lz4Block.GetInPlaceMargin(compressed.Length) - 1));
        }
    }

    /// <summary>Many 15-literal sequences with a 4-byte match each, then a long final literal run: the output gains on the input.</summary>
    private static byte[] Adversarial(int length, int seed)
    {
        Random random = new(seed);
        byte[] data = new byte[length];
        int tail = length / 3;
        int at = 0;
        while (at + 19 <= length - tail)
        {
            random.NextBytes(data.AsSpan(at, 15));
            data.AsSpan(at, 4).CopyTo(data.AsSpan(at + 15, 4)); // a 4-byte match 15 bytes back
            at += 19;
        }

        random.NextBytes(data.AsSpan(at));
        return data;
    }

    /// <summary>Random runs and zero runs of random lengths.</summary>
    private static byte[] Mixed(int length, int seed)
    {
        Random random = new(seed);
        byte[] data = new byte[length];
        int at = 0;
        while (at < length)
        {
            int run = Math.Min(length - at, random.Next(1, 4000));
            if (random.Next(2) == 0)
            {
                random.NextBytes(data.AsSpan(at, run));
            }

            at += run;
        }

        return data;
    }

    public static TheoryData<string, int> Shapes()
    {
        TheoryData<string, int> data = new();
        foreach (string kind in new[] { "zeroes", "repetitive", "game", "random", "mixed", "adversarial" })
        {
            foreach (int size in new[] { 1, 5, 12, 13, 16, 17, 31, 64, 255, 256, 1000, 4096, 16_000, 60_000, 200_000, 262_000 })
            {
                data.Add(kind, size);
            }
        }

        return data;
    }

    private static byte[] Make(string kind, int size, int seed) => kind switch
    {
        "zeroes" => new byte[size],
        "repetitive" => PrimitiveTestData.Repetitive(size, seed),
        "game" => PrimitiveTestData.GameLike(size, seed),
        "random" => PrimitiveTestData.Random(size, seed),
        "mixed" => Mixed(size, seed),
        _ => Adversarial(size, seed),
    };

    [Theory]
    [MemberData(nameof(Shapes))]
    public void Decodes_What_The_Compressor_Wrote_In_A_Block_Of_Raw_Plus_Margin(string kind, int size)
    {
        for (int seed = 1; seed <= 3; seed++)
        {
            byte[] input = Make(kind, size, seed);
            AssertDecodesInPlace(input, Compress(input), $"{kind} {size} seed {seed}");
        }
    }

    [Theory]
    [MemberData(nameof(Shapes))]
    public void Decodes_What_The_Reference_Encoders_Wrote_In_A_Block_Of_Raw_Plus_Margin(string kind, int size)
    {
        byte[] input = Make(kind, size, 7);
        foreach (LZ4Level level in new[] { LZ4Level.L00_FAST, LZ4Level.L09_HC, LZ4Level.L12_MAX })
        {
            byte[] buffer = new byte[LZ4Codec.MaximumOutputSize(input.Length)];
            int n = LZ4Codec.Encode(input, buffer, level);
            Assert.InRange(n, 1, buffer.Length);
            AssertDecodesInPlace(input, buffer.AsSpan(0, n).ToArray(), $"{kind} {size} {level}");
        }
    }

    [Fact]
    public void Decodes_At_And_Around_Every_Size_Class_Boundary()
    {
        // The staging block is a pool block: messages whose raw + margin is just at, under and over a class's size.
        foreach (int blockSize in new[] { 1536, 4096, 16_384, 65_536, 262_144 })
        {
            foreach (string kind in new[] { "game", "random", "mixed", "adversarial" })
            {
                for (int delta = -40; delta <= 2; delta++)
                {
                    int size = blockSize + delta - 32 - (blockSize >> 8);
                    if (size <= 0)
                    {
                        continue;
                    }

                    byte[] input = Make(kind, size, delta + 100);
                    byte[] compressed = Compress(input);
                    AssertDecodesInPlace(input, compressed, $"{kind} {size} around {blockSize}");
                    if (BlockFor(compressed.Length, size) <= blockSize)
                    {
                        Assert.Equal(input, InPlace(compressed, size, blockSize));
                    }
                }
            }
        }
    }

    [Fact]
    public void Decodes_When_The_Compressed_Bytes_Fill_More_Than_Half_The_Block()
    {
        // The move to the end of the block overlaps the bytes it moves; incompressible input needs a block of its compressed length.
        foreach (int size in new[] { 100, 5_000, 70_000, 250_000 })
        {
            byte[] input = PrimitiveTestData.Random(size, size);
            byte[] compressed = Compress(input);
            Assert.True(compressed.Length > BlockFor(compressed.Length, size) / 2);
            AssertDecodesInPlace(input, compressed, $"random {size}");
        }
    }

    [Fact]
    public void Refuses_Lengths_Out_Of_Range()
    {
        byte[] input = PrimitiveTestData.GameLike(1000, 1);
        byte[] compressed = Compress(input);
        byte[] block = new byte[BlockFor(compressed.Length, input.Length)];
        compressed.CopyTo(block, 0);
        Assert.False(Lz4Block.TryDecompressInPlace(block, 0, input.Length));
        Assert.False(Lz4Block.TryDecompressInPlace(block, -1, input.Length));
        Assert.False(Lz4Block.TryDecompressInPlace(block, compressed.Length, 0));
        Assert.False(Lz4Block.TryDecompressInPlace(block, compressed.Length, -5));
        Assert.False(Lz4Block.TryDecompressInPlace(block, block.Length + 1, input.Length));
        Assert.False(Lz4Block.TryDecompressInPlace(block, compressed.Length, int.MaxValue));
        Assert.False(Lz4Block.TryDecompressInPlace(Span<byte>.Empty, 1, 1));

        // A wrong raw length is refused like DecompressExact refuses it: one more, and one less.
        Assert.Null(InPlace(compressed, input.Length - 1, block.Length));
        Assert.Null(InPlace(compressed, input.Length + 1, block.Length + 1));
        Assert.Equal(input, InPlace(compressed, input.Length, block.Length));
    }

    [Fact]
    public void Garbage_And_Corrupted_Blocks_Never_Throw_Or_Touch_Memory_Outside_The_Block()
    {
        // The block is native memory between two canary zones: anything written or read outside it would be a bug the
        // managed bounds checks do not catch (the decoder reads and writes through refs).
        const int Canary = 256;
        const int MaxBlock = 70_000;
        byte* memory = (byte*)NativeMemory.Alloc(MaxBlock + (2 * Canary));
        try
        {
            Random random = new(4242);
            byte[][] sources =
            [
                Compress(PrimitiveTestData.GameLike(20_000, 1)),
                Compress(PrimitiveTestData.Repetitive(30_000, 2)),
                Compress(Mixed(60_000, 3)),
                Compress(Adversarial(40_000, 4)),
                Compress(new byte[50_000]),
            ];
            int accepted = 0;
            for (int iteration = 0; iteration < 20_000; iteration++)
            {
                byte[] source;
                if (iteration % 4 == 0)
                {
                    source = PrimitiveTestData.Random(random.Next(1, 2_000), iteration);
                }
                else
                {
                    source = (byte[])sources[iteration % sources.Length].Clone();
                    int edits = random.Next(1, 6);
                    for (int k = 0; k < edits; k++)
                    {
                        source[random.Next(source.Length)] = (byte)random.Next(256);
                    }

                    if (iteration % 7 == 0)
                    {
                        source = source.AsSpan(0, random.Next(1, source.Length)).ToArray();
                    }
                }

                int raw = random.Next(1, MaxBlock);
                int blockLength = Math.Min(MaxBlock, Math.Max(source.Length, random.Next(source.Length, MaxBlock + 1)));
                new Span<byte>(memory, Canary).Fill(0xA5);
                new Span<byte>(memory + Canary + blockLength, Canary).Fill(0x5A);
                Span<byte> block = new(memory + Canary, blockLength);
                source.CopyTo(block);
                if (Lz4Block.TryDecompressInPlace(block, source.Length, raw))
                {
                    accepted++;
                }

                Assert.True(new ReadOnlySpan<byte>(memory, Canary).IndexOfAnyExcept((byte)0xA5) < 0, $"iteration {iteration}: wrote before the block");
                Assert.True(new ReadOnlySpan<byte>(memory + Canary + blockLength, Canary).IndexOfAnyExcept((byte)0x5A) < 0, $"iteration {iteration}: wrote after the block");
            }

            // Random raw lengths make most of them fail; the point is that none throws or strays outside its block.
            Assert.True(accepted < 20_000);
        }
        finally
        {
            NativeMemory.Free(memory);
        }
    }

    [Fact]
    public void Steady_State_Does_Not_Allocate()
    {
        byte[] input = PrimitiveTestData.GameLike(20_000, 8);
        byte[] compressed = Compress(input);
        byte[] block = new byte[BlockFor(compressed.Length, input.Length)];
        int sink = 0;

        Run(input, compressed, block, ref sink);
        WindowedAllocation.AssertNone(() => Run(input, compressed, block, ref sink));
        Assert.NotEqual(0, sink);

        static void Run(byte[] input, byte[] compressed, byte[] block, ref int sink)
        {
            for (int i = 0; i < 50; i++)
            {
                compressed.CopyTo(block, 0);
                sink += Lz4Block.TryDecompressInPlace(block, compressed.Length, input.Length) ? 1 : 0;
                sink += Lz4Block.GetInPlaceMargin(i);
            }
        }
    }
}
