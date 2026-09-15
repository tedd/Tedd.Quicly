using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Replication.Tests;

public class DeltaCodecTests
{
    private const int Levels = 4;

    /// <summary>Straightforward implementation of the documented canonical format, used as the oracle.</summary>
    internal static byte[] ReferenceEncode(byte[] baseline, byte[] current)
    {
        int n = current.Length;
        byte[] x = new byte[n + 3];
        for (int i = 0; i < n; i++)
        {
            x[i] = (byte)(current[i] ^ (i < baseline.Length ? baseline[i] : 0));
        }

        List<byte> output = [];
        WriteVar(output, (ulong)n);
        int lastEnd = 0;
        int position = 0;
        while (true)
        {
            while (position < n && x[position] == 0)
            {
                position++;
            }

            if (position >= n)
            {
                break;
            }

            int start = position;
            int end = start + 1;
            while (x[end] != 0 || x[end + 1] != 0 || x[end + 2] != 0)
            {
                end++;
            }

            WriteVar(output, (ulong)(start - lastEnd));
            WriteVar(output, (ulong)(end - start));
            for (int k = start; k < end; k++)
            {
                output.Add(x[k]);
            }

            lastEnd = end;
            position = end;
        }

        return output.ToArray();
    }

    private static void WriteVar(List<byte> output, ulong value)
    {
        byte[] encoded = new byte[8];
        int length = VarInt.Write(encoded, value);
        for (int k = 0; k < length; k++)
        {
            output.Add(encoded[k]);
        }
    }

    private static int EncodeLevel(int level, ReadOnlySpan<byte> baseline, ReadOnlySpan<byte> current, Span<byte> destination) => level switch
    {
        0 => DeltaCodec.Encode<SimdNone>(baseline, current, destination),
        1 => DeltaCodec.Encode<Simd128>(baseline, current, destination),
        2 => DeltaCodec.Encode<Simd256>(baseline, current, destination),
        _ => DeltaCodec.Encode(baseline, current, destination),
    };

    private static int DecodeLevel(int level, ReadOnlySpan<byte> baseline, ReadOnlySpan<byte> delta, Span<byte> destination) => level switch
    {
        0 => DeltaCodec.Decode<SimdNone>(baseline, delta, destination),
        1 => DeltaCodec.Decode<Simd128>(baseline, delta, destination),
        2 => DeltaCodec.Decode<Simd256>(baseline, delta, destination),
        _ => DeltaCodec.Decode(baseline, delta, destination),
    };

    private static bool Untouched(ReadOnlySpan<byte> guard) => guard.IndexOfAnyExcept((byte)0xCC) < 0;

    /// <summary>Every SIMD level produces the oracle's bytes, decodes them back, and never writes past the span it was given.</summary>
    private static byte[] AssertCodec(byte[] baseline, byte[] current, bool checkShortDestinations = false)
    {
        byte[] expected = ReferenceEncode(baseline, current);
        Assert.True(expected.Length <= DeltaCodec.GetMaxEncodedLength(current.Length));
        for (int level = 0; level < Levels; level++)
        {
            byte[] destination = new byte[expected.Length + 8];
            destination.AsSpan().Fill(0xCC);
            int n = EncodeLevel(level, baseline, current, destination.AsSpan(0, expected.Length));
            Assert.Equal(expected.Length, n);
            Assert.True(destination.AsSpan(0, n).SequenceEqual(expected));
            Assert.True(Untouched(destination.AsSpan(n)));

            byte[] output = new byte[current.Length + 8];
            output.AsSpan().Fill(0xCC);
            int m = DecodeLevel(level, baseline, expected, output);
            Assert.Equal(current.Length, m);
            Assert.True(output.AsSpan(0, m).SequenceEqual(current));
            Assert.True(Untouched(output.AsSpan(m)));
        }

        if (current.Length > 0)
        {
            Assert.Equal(-1, DeltaCodec.Decode(baseline, expected, new byte[current.Length - 1]));
        }

        if (checkShortDestinations)
        {
            for (int length = 0; length < expected.Length; length++)
            {
                for (int level = 0; level < Levels; level++)
                {
                    byte[] destination = new byte[length + 8];
                    destination.AsSpan().Fill(0xCC);
                    Assert.Equal(-1, EncodeLevel(level, baseline, current, destination.AsSpan(0, length)));
                    Assert.True(Untouched(destination.AsSpan(length)));
                }
            }
        }

        return expected;
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(31)]
    [InlineData(32)]
    [InlineData(33)]
    [InlineData(64)]
    [InlineData(1000)]
    [InlineData(70_000)]
    public void Equal_Inputs_Encode_To_The_Length_Header_Only(int length)
    {
        byte[] data = TestData.Random(new Random(length), length);
        byte[] delta = AssertCodec(data, data);
        Assert.Equal(VarInt.GetLength((ulong)length), delta.Length);
    }

    [Fact]
    public void Known_Vectors()
    {
        // X = [0,0,10,0,0,0,0,0,9,10,0,0] (3 ^ 9 = 10; beyond the baseline X = current): one change, a zero run of 5,
        // two changes, trailing zeros implied.
        byte[] baseline = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        byte[] current = [1, 2, 9, 4, 5, 6, 7, 8, 0, 0, 0, 0];
        Assert.Equal(new byte[] { 12, 2, 1, 10, 5, 2, 9, 10 }, AssertCodec(baseline, current, checkShortDestinations: true));

        // Zero runs shorter than three stay inside the literal.
        byte[] sparse = [5, 0, 6, 0, 0, 7, 0, 0, 0, 8];
        Assert.Equal(new byte[] { 10, 0, 6, 5, 0, 6, 0, 0, 7, 3, 1, 8 }, AssertCodec([], sparse, checkShortDestinations: true));

        // A shorter current truncates the baseline; nothing changed in the kept prefix.
        Assert.Equal(new byte[] { 3 }, AssertCodec(baseline, [1, 2, 3]));
    }

    [Fact]
    public void Empty_Baseline_Gives_A_Full_Snapshot()
    {
        byte[] current = TestData.Sparse(new Random(1), 50, 0);
        byte[] delta = AssertCodec([], current);
        Assert.Equal(new byte[] { 50, 0, 50 }, delta.AsSpan(0, 3).ToArray());
        Assert.True(delta.AsSpan(3).SequenceEqual(current));
    }

    [Fact]
    public void Exhaustive_Small_Inputs_Around_Chunk_And_Baseline_Boundaries()
    {
        Random random = new(2);
        for (int n = 0; n <= 70; n++)
        {
            foreach (int baselineLength in new[] { 0, n / 2, n, n + 5 })
            {
                byte[] baseline = TestData.Sparse(random, baselineLength, 0.1);
                byte[] current = new byte[n];
                Array.Copy(baseline, current, Math.Min(n, baselineLength));
                AssertCodec(baseline, current);
                for (int p = 0; p < n; p++)
                {
                    byte[] one = (byte[])current.Clone();
                    one[p] ^= 0x5A;
                    AssertCodec(baseline, one);
                }

                for (int p = 0; p < n; p += 5)
                {
                    for (int gap = 1; gap <= 4 && p + gap < n; gap++)
                    {
                        byte[] two = (byte[])current.Clone();
                        two[p] ^= 0x11;
                        two[p + gap] ^= 0x22;
                        AssertCodec(baseline, two);
                    }
                }
            }
        }
    }

    [Fact]
    public void Random_Pairs_Of_Every_Shape()
    {
        Random random = new(3);
        double[] densities = [0, 0.01, 0.05, 0.2, 0.5, 1];
        for (int iteration = 0; iteration < 3000; iteration++)
        {
            int baselineLength = random.Next(0, 300);
            int currentLength = random.Next(3) == 0 ? baselineLength : random.Next(0, 300);
            byte[] baseline = TestData.Sparse(random, baselineLength, random.NextDouble() * 0.6);
            byte[] current = TestData.Mutate(random, baseline, currentLength, densities[random.Next(densities.Length)]);
            if (random.Next(4) == 0)
            {
                // Zero runs in the tail beyond the baseline behave like unchanged bytes of a zero-extended baseline.
                for (int i = baselineLength; i < currentLength; i++)
                {
                    if (random.Next(2) == 0)
                    {
                        current[i] = 0;
                    }
                }
            }

            AssertCodec(baseline, current, checkShortDestinations: iteration % 100 == 0);
        }
    }

    [Theory]
    [InlineData(1024, 0.05)]
    [InlineData(1024, 0.5)]
    [InlineData(16384, 0.05)]
    [InlineData(16384, 0.5)]
    public void Benchmark_Shaped_Inputs(int length, double density)
    {
        Random random = new(length);
        byte[] baseline = TestData.Random(random, length);
        AssertCodec(baseline, TestData.Mutate(random, baseline, length, density));
    }

    [Fact]
    public void Large_Inputs_Use_Multi_Byte_Varints()
    {
        Random random = new(4);
        byte[] baseline = TestData.Random(random, 70_000);
        byte[] current = (byte[])baseline.Clone();
        current[30_000] ^= 1;                                  // skip of 30 000: 4-byte varint
        for (int i = 40_000; i < 60_000; i++)
        {
            current[i] ^= 0xFF;                                // literal of 20 000: 4-byte varint
        }

        for (int i = 60_100; i < 69_000; i += 100)
        {
            current[i] ^= 3;                                   // skips of 99: 2-byte varints
        }

        for (int i = 69_100; i < 69_200; i++)
        {
            current[i] ^= 7;                                   // literal of 100: 2-byte count
        }

        AssertCodec(baseline, current);

        // A long tail beyond a short baseline.
        AssertCodec(baseline.AsSpan(0, 100).ToArray(), current);
        AssertCodec(current, baseline.AsSpan(0, 100).ToArray());
    }

    [Fact]
    public void Output_Never_Exceeds_The_Documented_Maximum()
    {
        static byte[] Repeat(int length, Func<int, byte> pattern)
        {
            byte[] data = new byte[length];
            for (int i = 0; i < length; i++)
            {
                data[i] = pattern(i);
            }

            return data;
        }

        foreach (int length in new[] { 1, 2, 3, 4, 5, 63, 64, 65, 1000, 5000, 40_000 })
        {
            List<byte[]> shapes =
            [
                Repeat(length, _ => 0xFF),
                Repeat(length, i => (byte)(i % 4 == 0 ? 1 : 0)),
                Repeat(length, i => (byte)(i % 3 == 0 ? 1 : 0)),
                Repeat(length, i => (byte)(i % 65 == 0 ? 1 : 0)),
                Repeat(length, i => (byte)(i % 67 < 64 ? 1 : 0)),
                Repeat(length, i => (byte)(i % 16387 < 16384 ? 1 : 0)),
                Repeat(length, i => (byte)(i % 16385 == 16384 ? 1 : 0)),
                TestData.Sparse(new Random(length), length, 0.5),
            ];
            foreach (byte[] shape in shapes)
            {
                AssertCodec([], shape);
            }
        }
    }

    public static TheoryData<byte[]> MalformedDeltas() => new()
    {
        Array.Empty<byte>(),
        new byte[] { 0x40, 0x05 },                                   // non-minimal header
        new byte[] { 0xC0, 0, 0, 0, 0, 0, 0, 0x05 },                 // non-minimal 8-byte header
        new byte[] { 0x80, 0x00, 0x40, 0x00 },                       // 16 384 > destination
        new byte[] { 11 },                                           // 11 > destination
        new byte[] { 5, 0, 0 },                                      // count 0
        new byte[] { 5, 6, 1, 1 },                                   // skip past the end
        new byte[] { 5, 0, 6, 1, 2, 3, 4, 5, 6 },                    // count past the end
        new byte[] { 5, 0, 3, 1, 2 },                                // literal truncated
        new byte[] { 5, 0x40, 0x01, 1, 9 },                          // non-minimal skip
        new byte[] { 5, 0, 0x40, 0x01, 9 },                          // non-minimal count
        new byte[] { 5, 1 },                                         // count missing
        new byte[] { 5, 0x40 },                                      // skip truncated
        new byte[] { 5, 1, 0x40 },                                   // count truncated
        new byte[] { 5, 2, 1, 9, 2, 1, 9 },                          // second op runs past the end
    };

    [Theory]
    [MemberData(nameof(MalformedDeltas))]
    public void Decode_Rejects_Malformed_Deltas(byte[] delta)
    {
        byte[] baseline = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        for (int level = 0; level < Levels; level++)
        {
            byte[] output = new byte[18];
            output.AsSpan().Fill(0xCC);
            Assert.Equal(-1, DecodeLevel(level, baseline, delta, output.AsSpan(0, 10)));
            Assert.True(Untouched(output.AsSpan(10)));
        }
    }

    [Fact]
    public void Decode_Accepts_Well_Formed_Non_Canonical_Ops()
    {
        byte[] baseline = [1, 2, 3, 4, 5];
        byte[] output = new byte[5];
        Assert.Equal(5, DeltaCodec.Decode(baseline, [5, 0, 1, 7, 0, 1, 8], output));
        Assert.Equal(new byte[] { 1 ^ 7, 2 ^ 8, 3, 4, 5 }, output);
        Assert.Equal(3, DeltaCodec.Decode(baseline, [3, 0, 3, 0, 0, 0], output));
        Assert.Equal(new byte[] { 1, 2, 3 }, output.AsSpan(0, 3).ToArray());
        Assert.Equal(0, DeltaCodec.Decode(baseline, [0], Span<byte>.Empty));
    }

    [Fact]
    public void Decode_In_Place_Over_The_Baseline()
    {
        Random random = new(5);
        for (int iteration = 0; iteration < 500; iteration++)
        {
            int baselineLength = random.Next(0, 200);
            int currentLength = random.Next(0, 200);
            byte[] baseline = TestData.Random(random, baselineLength);
            byte[] current = TestData.Mutate(random, baseline, currentLength, random.NextDouble());
            byte[] delta = ReferenceEncode(baseline, current);
            for (int level = 0; level < Levels; level++)
            {
                byte[] arena = new byte[Math.Max(baselineLength, currentLength)];
                baseline.CopyTo(arena, 0);
                Assert.Equal(currentLength, DecodeLevel(level, arena.AsSpan(0, baselineLength), delta, arena));
                Assert.True(arena.AsSpan(0, currentLength).SequenceEqual(current));
            }
        }
    }

    [Fact]
    public void Fuzz_Decode_Never_Throws_Or_Touches_Memory_Outside_The_Spans()
    {
        Random random = new(6);
        using GuardedBuffer baselineMemory = new(512);
        using GuardedBuffer deltaMemory = new(1024);
        for (int iteration = 0; iteration < 20_000; iteration++)
        {
            byte[] baseline = TestData.Sparse(random, random.Next(0, 200), 0.3);
            byte[] delta;
            if (random.Next(3) == 0)
            {
                delta = TestData.Random(random, random.Next(0, 64));
                if (delta.Length > 0 && random.Next(2) == 0)
                {
                    delta[0] = (byte)random.Next(0, 64);
                }
            }
            else
            {
                byte[] current = TestData.Mutate(random, baseline, random.Next(0, 200), random.NextDouble() * 0.3);
                delta = ReferenceEncode(baseline, current);
                int mutations = random.Next(1, 4);
                for (int m = 0; m < mutations; m++)
                {
                    switch (random.Next(4))
                    {
                        case 0 when delta.Length > 0:
                            delta[random.Next(delta.Length)] ^= (byte)(1 << random.Next(8));
                            break;
                        case 1 when delta.Length > 0:
                            delta = delta.AsSpan(0, random.Next(delta.Length)).ToArray();
                            break;
                        case 2:
                            delta = [.. delta, .. TestData.Random(random, random.Next(1, 6))];
                            break;
                        default:
                            if (delta.Length > 0)
                            {
                                delta[random.Next(delta.Length)] = (byte)random.Next(256);
                            }

                            break;
                    }
                }
            }

            // Alternate between spans that end at a guard page and spans that start right after one, so reads past
            // the end and before the start both fault.
            bool atHead = (iteration & 1) != 0;
            ReadOnlySpan<byte> baselineSpan = baselineMemory.CopyGuarded(baseline, atHead);
            ReadOnlySpan<byte> deltaSpan = deltaMemory.CopyGuarded(delta, atHead);
            int destinationLength = random.Next(0, 260);
            int reference = int.MinValue;
            byte[]? referenceOutput = null;
            for (int level = 0; level < Levels; level++)
            {
                byte[] output = new byte[destinationLength + 16];
                output.AsSpan().Fill(0xCC);
                int result = DecodeLevel(level, baselineSpan, deltaSpan, output.AsSpan(0, destinationLength));
                Assert.InRange(result, -1, destinationLength);
                Assert.True(Untouched(output.AsSpan(destinationLength)));
                if (level == 0)
                {
                    reference = result;
                    referenceOutput = output;
                }
                else
                {
                    Assert.Equal(reference, result);
                    if (result >= 0)
                    {
                        Assert.True(output.AsSpan(0, result).SequenceEqual(referenceOutput.AsSpan(0, result)));
                    }
                }
            }
        }
    }

    [Fact]
    public void Encode_Reads_Only_Inside_Its_Inputs()
    {
        Random random = new(7);
        using GuardedBuffer baselineMemory = new(4096);
        using GuardedBuffer currentMemory = new(4096);
        for (int iteration = 0; iteration < 2000; iteration++)
        {
            byte[] baseline = TestData.Random(random, random.Next(0, 300));
            byte[] current = TestData.Mutate(random, baseline, random.Next(0, 300), random.NextDouble() * 0.5);
            byte[] expected = ReferenceEncode(baseline, current);
            bool atHead = (iteration & 1) != 0;
            ReadOnlySpan<byte> b = baselineMemory.CopyGuarded(baseline, atHead);
            ReadOnlySpan<byte> c = currentMemory.CopyGuarded(current, atHead);
            byte[] destination = new byte[expected.Length];
            for (int level = 0; level < Levels; level++)
            {
                Assert.Equal(expected.Length, EncodeLevel(level, b, c, destination));
                Assert.Equal(expected, destination);
            }
        }
    }

    [Fact]
    public void Dispatch_Selects_Every_Width_Consistently()
    {
        Assert.Contains(DeltaCodec.PreferredWidth, new[] { 0, 128, 256 });
        if (System.Runtime.Intrinsics.Vector256.IsHardwareAccelerated)
        {
            Assert.Equal(256, DeltaCodec.PreferredWidth);
        }

        Random random = new(12);
        for (int iteration = 0; iteration < 200; iteration++)
        {
            byte[] baseline = TestData.Random(random, random.Next(0, 200));
            byte[] current = TestData.Mutate(random, baseline, random.Next(0, 200), 0.1);
            byte[] expected = ReferenceEncode(baseline, current);
            foreach (int width in new[] { 0, 64, 128, 200, 256, 512 })
            {
                byte[] delta = new byte[expected.Length];
                Assert.Equal(expected.Length, DeltaCodec.Encode(width, baseline, current, delta));
                Assert.Equal(expected, delta);
                byte[] output = new byte[current.Length];
                Assert.Equal(current.Length, DeltaCodec.Decode(width, baseline, delta, output));
                Assert.Equal(current, output);
            }
        }
    }

    [Fact]
    public void Size_Helpers()
    {
        Assert.Equal(16, DeltaCodec.GetMaxEncodedLength(0));
        Assert.Equal(1024 + 32 + 16, DeltaCodec.GetMaxEncodedLength(1024));
        Assert.Equal(DeltaCodec.GetMaxEncodedLength(100) + 1, DeltaCodec.GetMaxCompressedLength(100));
        Assert.Throws<ArgumentOutOfRangeException>(() => DeltaCodec.GetMaxEncodedLength(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => DeltaCodec.GetMaxEncodedLength(DeltaCodec.MaxInputLength + 1));
        DeltaCodec.GetMaxEncodedLength(DeltaCodec.MaxInputLength);

        Assert.True(DeltaCodec.TryGetDecodedLength([0x40, 0x64, 7], out int length));
        Assert.Equal(100, length);
        Assert.False(DeltaCodec.TryGetDecodedLength([], out length));
        Assert.Equal(0, length);
        Assert.False(DeltaCodec.TryGetDecodedLength([0x40, 0x05], out _));
        byte[] huge = new byte[8];
        VarInt.Write(huge, (ulong)int.MaxValue + 1);
        Assert.False(DeltaCodec.TryGetDecodedLength(huge, out _));
    }

    private static byte[] Compressed(byte[] baseline, byte[] current, out int rawLength)
    {
        byte[] scratch = new byte[DeltaCodec.GetMaxEncodedLength(current.Length)];
        byte[] destination = new byte[DeltaCodec.GetMaxCompressedLength(current.Length)];
        int n = DeltaCodec.EncodeCompressed(baseline, current, destination, scratch);
        Assert.InRange(n, 1, destination.Length);
        rawLength = ReferenceEncode(baseline, current).Length;
        byte[] output = new byte[current.Length];
        Assert.Equal(current.Length, DeltaCodec.DecodeCompressed(baseline, destination.AsSpan(0, n), output, scratch));
        Assert.Equal(current, output);
        return destination.AsSpan(0, n).ToArray();
    }

    [Fact]
    public void Compressed_Small_Deltas_Are_Stored()
    {
        byte[] baseline = TestData.Random(new Random(8), 40);
        byte[] current = (byte[])baseline.Clone();
        current[7] ^= 1;
        byte[] packet = Compressed(baseline, current, out int rawLength);
        Assert.Equal(0, packet[0]);
        Assert.Equal(rawLength + 1, packet.Length);
        Assert.Equal(ReferenceEncode(baseline, current), packet.AsSpan(1).ToArray());
    }

    [Fact]
    public void Compressed_Repetitive_Deltas_Shrink()
    {
        byte[] baseline = new byte[4096];
        byte[] current = new byte[4096];
        for (int i = 0; i < current.Length; i += 8)
        {
            current[i] = 0x11;
        }

        byte[] packet = Compressed(baseline, current, out int rawLength);
        Assert.True(VarInt.TryReadMinimal(packet, out ulong header, out _));
        Assert.Equal((ulong)rawLength, header);
        Assert.True(packet.Length < rawLength / 4);
    }

    [Fact]
    public void Compressed_Incompressible_Deltas_Fall_Back_To_Stored()
    {
        byte[] current = TestData.Random(new Random(9), 4096);
        byte[] packet = Compressed([], current, out int rawLength);
        Assert.Equal(0, packet[0]);
        Assert.Equal(rawLength + 1, packet.Length);
        Assert.True(packet.Length <= DeltaCodec.GetMaxCompressedLength(current.Length));
    }

    [Fact]
    public void Compressed_Errors_Are_Reported()
    {
        byte[] repetitive = new byte[4096];
        for (int i = 0; i < repetitive.Length; i += 8)
        {
            repetitive[i] = 0x11;
        }

        byte[] random = TestData.Random(new Random(10), 4096);
        byte[] scratch = new byte[DeltaCodec.GetMaxEncodedLength(4096)];
        foreach (byte[] current in new[] { repetitive, random })
        {
            Assert.Equal(-1, DeltaCodec.EncodeCompressed([], current, new byte[2], scratch));
            Assert.Equal(-1, DeltaCodec.EncodeCompressed([], current, new byte[8192], new byte[100]));
        }

        byte[] output = new byte[4096];
        Assert.Equal(-1, DeltaCodec.DecodeCompressed([], [], output, scratch));
        Assert.Equal(-1, DeltaCodec.DecodeCompressed([], [0x40, 0x00, 1], output, scratch));
        Assert.Equal(-1, DeltaCodec.DecodeCompressed([], [0x43, 0xE8, 1, 2, 3], output, new byte[10]));
        Assert.Equal(-1, DeltaCodec.DecodeCompressed([], [100, 1, 2, 3, 4, 5], output, scratch));
        Assert.Equal(-1, DeltaCodec.DecodeCompressed([], [0, 5, 0, 0], output, scratch));

        // A valid LZ4 block whose decoded length differs from RawLength.
        byte[] delta = ReferenceEncode([], repetitive.AsSpan(0, 400).ToArray());
        byte[] block = new byte[Lz4Block.GetMaxCompressedLength(delta.Length)];
        int blockLength = Lz4Block.Compress(delta, block);
        byte[] packet = new byte[blockLength + 2];
        VarInt.Write(packet, (ulong)delta.Length + 1);
        block.AsSpan(0, blockLength).CopyTo(packet.AsSpan(2));
        Assert.Equal(-1, DeltaCodec.DecodeCompressed([], packet, output, scratch));
        VarInt.Write(packet, (ulong)delta.Length);
        Assert.Equal(400, DeltaCodec.DecodeCompressed([], packet, output, scratch));
    }

    [Fact]
    public void Fuzz_DecodeCompressed_Never_Throws()
    {
        Random random = new(11);
        byte[] scratch = new byte[DeltaCodec.GetMaxEncodedLength(512)];
        byte[] sourceScratch = new byte[DeltaCodec.GetMaxEncodedLength(512)];
        byte[] packetBuffer = new byte[DeltaCodec.GetMaxCompressedLength(512)];
        for (int iteration = 0; iteration < 5000; iteration++)
        {
            byte[] baseline = TestData.Sparse(random, random.Next(0, 256), 0.5);
            byte[] packet;
            if (random.Next(2) == 0)
            {
                packet = TestData.Random(random, random.Next(0, 80));
            }
            else
            {
                byte[] current = TestData.Mutate(random, baseline, random.Next(0, 512), random.NextDouble() * 0.2);
                int n = DeltaCodec.EncodeCompressed(baseline, current, packetBuffer, sourceScratch);
                packet = packetBuffer.AsSpan(0, n).ToArray();
                for (int m = random.Next(1, 3); m > 0 && packet.Length > 0; m--)
                {
                    packet[random.Next(packet.Length)] ^= (byte)(1 << random.Next(8));
                }
            }

            int destinationLength = random.Next(0, 520);
            byte[] output = new byte[destinationLength + 16];
            output.AsSpan().Fill(0xCC);
            int result = DeltaCodec.DecodeCompressed(baseline, packet, output.AsSpan(0, destinationLength), scratch);
            Assert.InRange(result, -1, destinationLength);
            Assert.True(Untouched(output.AsSpan(destinationLength)));
        }
    }
}
