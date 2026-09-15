using K4os.Compression.LZ4;
using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Core.Tests.Primitives;

/// <summary>
/// Adversarial review tests for <see cref="Lz4Block.Decompress"/>: guard-region checks that catch any
/// out-of-bounds write past the destination on hostile input, and a differential comparison against the
/// K4os reference decoder on random valid blocks.
/// </summary>
public class Lz4BlockReviewTests
{
    private const byte Guard = 0xCD;

    /// <summary>
    /// Decompresses into a destination surrounded by guard bytes and asserts nothing outside
    /// [0, dstLength) was touched, whatever the input. Returns the decoder result.
    /// </summary>
    private static int DecompressGuarded(ReadOnlySpan<byte> source, int dstLength, out byte[] backing)
    {
        const int pad = 64;
        backing = new byte[pad + dstLength + pad];
        backing.AsSpan().Fill(Guard);
        int result = Lz4Block.Decompress(source, backing.AsSpan(pad, dstLength));

        for (int i = 0; i < pad; i++)
        {
            Assert.True(backing[i] == Guard, $"underflow write at -{pad - i}");
            Assert.True(backing[pad + dstLength + i] == Guard, $"overflow write at +{i} (dstLength={dstLength}, result={result})");
        }

        return result;
    }

    [Fact]
    public void Decompress_Never_Writes_Out_Of_Bounds_On_Hostile_Input()
    {
        Random random = new(1234567);

        // Corruptions of valid blocks, decoded into a wide range of destination sizes (including much smaller
        // than the true payload, which forces the internal length/room checks to trip mid-stream).
        int[] scratch = new int[Lz4Block.DefaultScratchLength];
        byte[][] seeds =
        {
            PrimitiveTestData.GameLike(4096, 3),
            PrimitiveTestData.Repetitive(4096, 3, period: 5),
            PrimitiveTestData.Repetitive(300, 3, period: 1),
            PrimitiveTestData.Random(1024, 3),
            new byte[4096],
        };

        foreach (byte[] seed in seeds)
        {
            byte[] buffer = new byte[Lz4Block.GetMaxCompressedLength(seed.Length)];
            int n = Lz4Block.Compress(seed, buffer, scratch);
            byte[] valid = buffer.AsSpan(0, n).ToArray();

            for (int iter = 0; iter < 20000; iter++)
            {
                byte[] corrupt = (byte[])valid.Clone();
                int mutations = random.Next(1, 12);
                for (int m = 0; m < mutations; m++)
                {
                    corrupt[random.Next(corrupt.Length)] = (byte)random.Next(256);
                }

                // Occasionally truncate or extend.
                if (random.Next(3) == 0)
                {
                    corrupt = corrupt.AsSpan(0, random.Next(0, corrupt.Length + 1)).ToArray();
                }
                else if (random.Next(5) == 0)
                {
                    byte[] more = new byte[random.Next(1, 32)];
                    random.NextBytes(more);
                    corrupt = [.. corrupt, .. more];
                }

                // Destinations from tiny to somewhat larger than the true payload.
                int dstLength = random.Next(0, seed.Length + 33);
                int result = DecompressGuarded(corrupt, dstLength, out _);
                Assert.InRange(result, -1, dstLength);
            }
        }
    }

    [Fact]
    public void Decompress_Never_Writes_Out_Of_Bounds_On_Pure_Garbage()
    {
        Random random = new(98765);
        for (int iter = 0; iter < 200000; iter++)
        {
            byte[] garbage = new byte[random.Next(0, 96)];
            random.NextBytes(garbage);

            // Bias the token nibbles so more paths (long literal/match extension, overlaps) are reached.
            if (garbage.Length > 0 && random.Next(2) == 0)
            {
                garbage[0] = (byte)(random.Next(2) == 0 ? 0xFF : (random.Next(16) << 4) | random.Next(16));
            }

            int dstLength = random.Next(0, 160);
            int result = DecompressGuarded(garbage, dstLength, out _);
            Assert.InRange(result, -1, dstLength);
        }
    }

    [Fact]
    public void Decompress_Matches_Reference_Decoder_On_Random_Valid_Blocks()
    {
        Random random = new(424242);
        int[] scratch = new int[Lz4Block.DefaultScratchLength];

        for (int iter = 0; iter < 4000; iter++)
        {
            int length = random.Next(0, 20000);
            byte[] input = (random.Next(3)) switch
            {
                0 => PrimitiveTestData.Random(length, iter),
                1 => PrimitiveTestData.Repetitive(length, iter, period: random.Next(1, 64)),
                _ => PrimitiveTestData.GameLike(length, iter),
            };

            // Encode with the reference at a random level, decode with ours (guarded), compare to input.
            LZ4Level level = (iter % 3) switch
            {
                0 => LZ4Level.L00_FAST,
                1 => LZ4Level.L09_HC,
                _ => LZ4Level.L12_MAX,
            };
            byte[] refBuffer = new byte[LZ4Codec.MaximumOutputSize(Math.Max(length, 1))];
            int rn = LZ4Codec.Encode(input, refBuffer, level);
            if (rn <= 0)
            {
                continue; // empty input: reference cannot encode a zero-length block
            }

            int result = DecompressGuarded(refBuffer.AsSpan(0, rn), length, out byte[] backing);
            Assert.Equal(length, result);
            Assert.Equal(input, backing.AsSpan(64, length).ToArray());
        }
    }
}
