using System.Numerics;

namespace Tedd.Quicly.Replication.Tests;

public class BitStreamTests
{
    /// <summary>Reference packer: one bool per stream bit, least significant bit first.</summary>
    private static byte[] ReferencePack(IReadOnlyList<(ulong Value, int Bits)> writes)
    {
        List<bool> bits = [];
        foreach ((ulong value, int count) in writes)
        {
            for (int i = 0; i < count; i++)
            {
                bits.Add(((value >> i) & 1) != 0);
            }
        }

        byte[] bytes = new byte[(bits.Count + 7) / 8];
        for (int i = 0; i < bits.Count; i++)
        {
            if (bits[i])
            {
                bytes[i >> 3] |= (byte)(1 << (i & 7));
            }
        }

        return bytes;
    }

    private static ulong Masked(ulong value, int bits) => bits == 64 ? value : value & ((1UL << bits) - 1);

    [Fact]
    public void Known_Layout_Is_Lsb_First()
    {
        byte[] buffer = new byte[4];
        BitWriter writer = new(buffer);
        writer.WriteBits(0xABC, 12);
        writer.WriteBits(0x5, 3);
        Assert.Equal(15, writer.BitPosition);
        Assert.Equal(2, writer.BytesWritten);
        Assert.Equal(2, writer.Flush());
        Assert.Equal(new byte[] { 0xBC, 0x5A, 0, 0 }, buffer);

        BitReader reader = new(buffer.AsSpan(0, 2));
        Assert.Equal(0xBCUL, reader.ReadBits(8));
        Assert.Equal(0xAUL, reader.ReadBits(4));
        Assert.Equal(0x5UL, reader.ReadBits(3));
        Assert.Equal(1, reader.RemainingBits);
        Assert.False(reader.HasError);
    }

    [Fact]
    public void Random_Sequences_Match_The_Reference_And_Round_Trip()
    {
        Random random = new(10);
        for (int iteration = 0; iteration < 400; iteration++)
        {
            List<(ulong, int)> writes = [];
            int total = 0;
            int count = random.Next(0, 60);
            for (int i = 0; i < count; i++)
            {
                int bits = random.Next(0, 65);
                ulong value = (ulong)random.NextInt64() ^ ((ulong)random.NextInt64() << 1);
                writes.Add((value, bits));
                total += bits;
            }

            byte[] expected = ReferencePack(writes);
            byte[] buffer = new byte[expected.Length + random.Next(0, 3)];
            BitWriter writer = new(buffer);
            foreach ((ulong value, int bits) in writes)
            {
                writer.WriteBits(value, bits);
            }

            Assert.False(writer.HasOverflowed);
            Assert.Equal(total, writer.BitPosition);
            Assert.Equal(expected.Length, writer.Flush());
            Assert.Equal(expected, buffer.AsSpan(0, expected.Length).ToArray());

            BitReader reader = new(buffer.AsSpan(0, expected.Length));
            foreach ((ulong value, int bits) in writes)
            {
                Assert.Equal(Masked(value, bits), reader.ReadBits(bits));
            }

            Assert.False(reader.HasError);
            Assert.Equal(total, reader.BitPosition);
        }
    }

    [Fact]
    public void Full_Width_Values_At_Every_Alignment()
    {
        Random random = new(11);
        for (int offset = 0; offset < 64; offset++)
        {
            ulong value = (ulong)random.NextInt64() | (1UL << 63);
            byte[] buffer = new byte[(offset + 64 + 7) / 8];
            BitWriter writer = new(buffer);
            writer.WriteBits(ulong.MaxValue, offset);
            writer.WriteBits(value, 64);
            Assert.False(writer.HasOverflowed);
            Assert.Equal(buffer.Length, writer.Flush());

            BitReader reader = new(buffer);
            Assert.Equal(Masked(ulong.MaxValue, offset), reader.ReadBits(offset));
            Assert.Equal(value, reader.ReadBits(64));
            Assert.False(reader.HasError);
        }
    }

    [Fact]
    public void Writer_Overflow_Is_Sticky_And_Keeps_Earlier_Bits()
    {
        byte[] buffer = new byte[2];
        BitWriter writer = new(buffer);
        writer.WriteBits(0xFF, 8);
        writer.WriteBits(0x3, 7);
        Assert.Equal(1, writer.RemainingBits);
        Assert.Equal(16, writer.CapacityBits);
        writer.WriteBits(0, 2);
        Assert.True(writer.HasOverflowed);
        Assert.Equal(15, writer.BitPosition);
        writer.WriteBits(1, 1);
        writer.WriteBits(0, 0);
        writer.WriteBool(true);
        writer.WriteBytes(ReadOnlySpan<byte>.Empty);
        Assert.Equal(15, writer.BitPosition);
        Assert.Equal(2, writer.Flush());
        Assert.Equal(new byte[] { 0xFF, 0x03 }, buffer);

        byte[] exact = new byte[8];
        BitWriter full = new(exact);
        full.WriteBits(0x0123_4567_89AB_CDEF, 64);
        Assert.False(full.HasOverflowed);
        Assert.Equal(0, full.RemainingBits);
        Assert.Equal(8, full.Flush());
        Assert.Equal(0x0123_4567_89AB_CDEFUL, BitConverter.ToUInt64(exact));

        BitWriter empty = new(Span<byte>.Empty);
        empty.WriteBits(0, 0);
        Assert.False(empty.HasOverflowed);
        empty.WriteBool(false);
        Assert.True(empty.HasOverflowed);
        Assert.Equal(0, empty.Flush());
    }

    [Fact]
    public void Reader_Past_End_Returns_Zero_And_Stays_Failed()
    {
        byte[] buffer = [0xFF, 0x01, 0x80];
        BitReader reader = new(buffer);
        Assert.Equal(0x1FFUL, reader.ReadBits(9));
        Assert.Equal(0x4000UL, reader.ReadBits(15));    // 7 zero bits of byte 1, then 0x80 shifted up by 7
        Assert.Equal(0, reader.RemainingBits);
        Assert.Equal(0UL, reader.ReadBits(0));
        Assert.False(reader.HasError);
        Assert.Equal(0UL, reader.ReadBits(1));
        Assert.True(reader.HasError);
        Assert.Equal(24, reader.BitPosition);

        BitReader short1 = new(buffer);
        Assert.Equal(0UL, short1.ReadBits(25));
        Assert.True(short1.HasError);
        Assert.Equal(0UL, short1.ReadBits(1));
        Assert.False(short1.ReadBool());

        // Near the end the reader assembles the value from fewer than eight bytes.
        byte[] five = [1, 2, 3, 4, 5];
        BitReader tail = new(five);
        Assert.Equal(0x1UL, tail.ReadBits(4) | (tail.ReadBits(4) << 4));
        Assert.Equal(0x0504_0302UL, tail.ReadBits(32));
    }

    [Fact]
    public void Portable_Mask_Matches_The_Intrinsic_For_Every_Width()
    {
        Random random = new(14);
        for (int bits = 0; bits <= 64; bits++)
        {
            for (int i = 0; i < 50; i++)
            {
                ulong value = i == 0 ? ulong.MaxValue : (ulong)random.NextInt64() ^ ((ulong)random.NextInt64() << 1);
                Assert.Equal(Masked(value, bits), BitWriter.MaskPortable(value, bits));
                Assert.Equal(Masked(value, bits), BitWriter.Mask(value, bits));
            }
        }
    }

    [Fact]
    public void Invalid_Bit_Counts_Throw()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new BitWriter(new byte[16]).WriteBits(0, 65));
        Assert.Throws<ArgumentOutOfRangeException>(() => new BitWriter(new byte[16]).WriteBits(0, -1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new BitReader(new byte[16]).ReadBits(65));
        Assert.Throws<ArgumentOutOfRangeException>(() => new BitReader(new byte[16]).ReadBits(-1));
    }

    [Fact]
    public void Bools_Alignment_And_Byte_Copies()
    {
        byte[] payload = [9, 8, 7, 6, 5];
        byte[] buffer = new byte[16];
        BitWriter writer = new(buffer);
        writer.WriteBool(true);
        writer.WriteBool(false);
        writer.WriteBool(true);
        writer.AlignToByte();
        Assert.Equal(8, writer.BitPosition);
        writer.AlignToByte();
        Assert.Equal(8, writer.BitPosition);
        writer.WriteBits(0xABCDEF, 24);
        writer.WriteBits(1, 3);
        writer.WriteBytes(payload);
        Assert.Equal((1 + 3 + 1 + 5) * 8, writer.BitPosition);
        writer.WriteBits(0x3, 2);
        int length = writer.Flush();
        Assert.Equal(11, length);

        BitReader reader = new(buffer.AsSpan(0, length));
        Assert.True(reader.ReadBool());
        Assert.False(reader.ReadBool());
        Assert.True(reader.ReadBool());
        reader.AlignToByte();
        Assert.Equal(8, reader.BitPosition);
        Assert.Equal(0xABCDEFUL, reader.ReadBits(24));
        Assert.Equal(1UL, reader.ReadBits(3));
        byte[] back = new byte[3];
        reader.ReadBytes(back);
        Assert.Equal(new byte[] { 9, 8, 7 }, back);
        Assert.Equal(new byte[] { 6, 5 }, reader.ReadByteSpan(2).ToArray());
        Assert.Equal(3UL, reader.ReadBits(2));
        Assert.False(reader.HasError);

        // Byte copies that do not fit.
        byte[] target = [1, 2, 3, 4];
        reader.ReadBytes(target);
        Assert.True(reader.HasError);
        Assert.Equal(new byte[4], target);

        BitReader negative = new(buffer);
        Assert.True(negative.ReadByteSpan(-1).IsEmpty);
        Assert.True(negative.HasError);

        byte[] small = new byte[3];
        BitWriter overflow = new(small);
        overflow.WriteBits(1, 1);
        overflow.WriteBytes([1, 2, 3]);
        Assert.True(overflow.HasOverflowed);
    }

    [Fact]
    public void Flush_Is_Idempotent_And_Writing_Can_Continue()
    {
        byte[] buffer = new byte[16];
        BitWriter writer = new(buffer);
        writer.WriteBits(0x5, 3);
        Assert.Equal(1, writer.Flush());
        Assert.Equal(1, writer.Flush());
        Assert.Equal(0x5, buffer[0]);
        writer.WriteBits(0xFFFF_FFFF_FFFF, 61);
        Assert.Equal(8, writer.Flush());
        writer.WriteBits(0x1F, 5);
        Assert.Equal(9, writer.Flush());
        BitReader reader = new(buffer.AsSpan(0, 9));
        Assert.Equal(0x5UL, reader.ReadBits(3));
        Assert.Equal(0xFFFF_FFFF_FFFFUL, reader.ReadBits(61));
        Assert.Equal(0x1FUL, reader.ReadBits(5));
    }

    [Theory]
    [InlineData(0UL, 1)]
    [InlineData(1UL, 1)]
    [InlineData(127UL, 1)]
    [InlineData(128UL, 2)]
    [InlineData(16383UL, 2)]
    [InlineData(16384UL, 3)]
    [InlineData(1UL << 63, 10)]
    [InlineData(ulong.MaxValue, 10)]
    public void VarUInt_Round_Trips(ulong value, int groups)
    {
        byte[] buffer = new byte[16];
        BitWriter writer = new(buffer);
        writer.WriteBits(1, 1);
        writer.WriteVarUInt(value);
        Assert.Equal(1 + 8 * groups, writer.BitPosition);
        int length = writer.Flush();
        BitReader reader = new(buffer.AsSpan(0, length));
        Assert.Equal(1UL, reader.ReadBits(1));
        Assert.Equal(value, reader.ReadVarUInt());
        Assert.False(reader.HasError);
    }

    [Fact]
    public void VarInt_Zigzag_Round_Trips()
    {
        long[] values = [0, 1, -1, 63, -64, 64, -65, long.MaxValue, long.MinValue, 123456789, -987654321];
        byte[] buffer = new byte[values.Length * 10];
        BitWriter writer = new(buffer);
        foreach (long v in values)
        {
            writer.WriteVarInt(v);
        }

        int length = writer.Flush();
        BitReader reader = new(buffer.AsSpan(0, length));
        foreach (long v in values)
        {
            Assert.Equal(v, reader.ReadVarInt());
        }

        Assert.False(reader.HasError);
    }

    [Theory]
    [InlineData(new byte[] { 0x80 })]
    [InlineData(new byte[] { 0x80, 0x00 })]
    [InlineData(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x02 })]
    [InlineData(new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x81, 0x00 })]
    [InlineData(new byte[] { 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x00 })]
    [InlineData(new byte[0])]
    public void VarUInt_Rejects_Malformed(byte[] encoded)
    {
        BitReader reader = new(encoded);
        Assert.Equal(0UL, reader.ReadVarUInt());
        Assert.True(reader.HasError);
        Assert.Equal(0L, reader.ReadVarInt());
    }

    [Fact]
    public void Quantized_Values_Through_The_Stream()
    {
        byte[] buffer = new byte[32];
        BitWriter writer = new(buffer);
        Vector3 direction = Vector3.Normalize(new Vector3(0.3f, -0.5f, 0.8f));
        Quaternion rotation = Quaternion.CreateFromYawPitchRoll(0.4f, -1.1f, 2.0f);
        writer.WriteQuantizedFloat(12.34f, -100f, 100f, 16);
        writer.WriteUnitVector(direction, 12);
        writer.WriteQuaternion(rotation, 10);
        Assert.Equal(16 + 24 + 32, writer.BitPosition);
        int length = writer.Flush();

        BitReader reader = new(buffer.AsSpan(0, length));
        Assert.Equal(Quantization.DequantizeFloat(Quantization.QuantizeFloat(12.34f, -100f, 100f, 16), -100f, 100f, 16), reader.ReadQuantizedFloat(-100f, 100f, 16));
        Assert.Equal(Quantization.DequantizeUnitVector(Quantization.QuantizeUnitVector(direction, 12), 12), reader.ReadUnitVector(12));
        Assert.Equal(Quantization.DequantizeQuaternion(Quantization.QuantizeQuaternion(rotation, 10), 10), reader.ReadQuaternion(10));
        Assert.False(reader.HasError);
    }

    [Fact]
    public void Fuzz_Reader_Never_Throws_Or_Overruns()
    {
        Random random = new(12);
        using GuardedBuffer guarded = new(64);
        byte[] scratch = new byte[64];
        for (int iteration = 0; iteration < 20_000; iteration++)
        {
            int length = random.Next(0, 40);
            Span<byte> source = guarded.Tail(length);
            random.NextBytes(source);
            BitReader reader = new(source);
            for (int op = 0; op < 12; op++)
            {
                switch (random.Next(6))
                {
                    case 0:
                        reader.ReadBits(random.Next(0, 65));
                        break;
                    case 1:
                        reader.ReadVarUInt();
                        break;
                    case 2:
                        reader.ReadBytes(scratch.AsSpan(0, random.Next(0, 9)));
                        break;
                    case 3:
                        reader.AlignToByte();
                        break;
                    case 4:
                        reader.ReadQuaternion(random.Next(2, 21));
                        break;
                    default:
                        reader.ReadByteSpan(random.Next(0, 5));
                        break;
                }

                Assert.InRange(reader.BitPosition, 0, length * 8L);
                if (reader.HasError)
                {
                    Assert.Equal(0UL, reader.ReadBits(1));
                }
            }
        }
    }
}

public class ZigZagTests
{
    [Theory]
    [InlineData(0L, 0UL)]
    [InlineData(-1L, 1UL)]
    [InlineData(1L, 2UL)]
    [InlineData(-2L, 3UL)]
    [InlineData(long.MaxValue, ulong.MaxValue - 1)]
    [InlineData(long.MinValue, ulong.MaxValue)]
    public void Maps_64_Bit(long value, ulong encoded)
    {
        Assert.Equal(encoded, ZigZag.Encode(value));
        Assert.Equal(value, ZigZag.Decode(encoded));
    }

    [Theory]
    [InlineData(0, 0u)]
    [InlineData(-1, 1u)]
    [InlineData(1, 2u)]
    [InlineData(int.MaxValue, uint.MaxValue - 1)]
    [InlineData(int.MinValue, uint.MaxValue)]
    public void Maps_32_Bit(int value, uint encoded)
    {
        Assert.Equal(encoded, ZigZag.Encode(value));
        Assert.Equal(value, ZigZag.Decode(encoded));
    }

    [Fact]
    public void Random_Round_Trips()
    {
        Random random = new(13);
        for (int i = 0; i < 10_000; i++)
        {
            long v = random.NextInt64(long.MinValue, long.MaxValue);
            Assert.Equal(v, ZigZag.Decode(ZigZag.Encode(v)));
            int w = random.Next(int.MinValue, int.MaxValue);
            Assert.Equal(w, ZigZag.Decode(ZigZag.Encode(w)));
        }
    }
}
