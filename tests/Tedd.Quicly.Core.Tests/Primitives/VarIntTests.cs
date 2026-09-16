using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Core.Tests.Primitives;

public class VarIntTests
{
    // (value, expected length, expected big-endian encoding) — RFC 9000 §16 and Appendix A.1 examples plus boundaries.
    public static TheoryData<ulong, int, byte[]> Vectors => new()
    {
        { 0, 1, new byte[] { 0x00 } },
        { 1, 1, new byte[] { 0x01 } },
        { 37, 1, new byte[] { 0x25 } },
        { 63, 1, new byte[] { 0x3F } },
        { 64, 2, new byte[] { 0x40, 0x40 } },
        { 15293, 2, new byte[] { 0x7B, 0xBD } },
        { 16383, 2, new byte[] { 0x7F, 0xFF } },
        { 16384, 4, new byte[] { 0x80, 0x00, 0x40, 0x00 } },
        { 494878333, 4, new byte[] { 0x9D, 0x7F, 0x3E, 0x7D } },
        { 1073741823, 4, new byte[] { 0xBF, 0xFF, 0xFF, 0xFF } },
        { 1073741824, 8, new byte[] { 0xC0, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00 } },
        { 151288809941952652, 8, new byte[] { 0xC2, 0x19, 0x7C, 0x5E, 0xFF, 0x14, 0xE8, 0x8C } },
        { VarInt.MaxValue, 8, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF } },
    };

    [Fact]
    public void Constants()
    {
        Assert.Equal((1UL << 62) - 1, VarInt.MaxValue);
        Assert.Equal(8, VarInt.MaxLength);
    }

    [Theory]
    [MemberData(nameof(Vectors))]
    public void GetLength_Matches_Vector(ulong value, int length, byte[] encoded)
    {
        Assert.Equal(length, VarInt.GetLength(value));
        Assert.Equal(encoded.Length, length);
    }

    [Fact]
    public void GetLength_Throws_Above_Max()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => VarInt.GetLength(VarInt.MaxValue + 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => VarInt.GetLength(ulong.MaxValue));
    }

    [Theory]
    [InlineData(0x00, 1)]
    [InlineData(0x3F, 1)]
    [InlineData(0x40, 2)]
    [InlineData(0x7F, 2)]
    [InlineData(0x80, 4)]
    [InlineData(0xBF, 4)]
    [InlineData(0xC0, 8)]
    [InlineData(0xFF, 8)]
    public void PeekLength_Uses_Top_Two_Bits(byte first, int expected) => Assert.Equal(expected, VarInt.PeekLength(first));

    [Theory]
    [MemberData(nameof(Vectors))]
    public void TryWrite_Produces_Vector(ulong value, int length, byte[] encoded)
    {
        Span<byte> buffer = stackalloc byte[9];
        buffer.Fill(0xAA);
        Assert.True(VarInt.TryWrite(buffer, value, out int written));
        Assert.Equal(length, written);
        Assert.Equal(encoded, buffer.Slice(0, written).ToArray());
        Assert.Equal(0xAA, buffer[written]);
    }

    [Theory]
    [MemberData(nameof(Vectors))]
    public void TryWrite_Exact_Size_Succeeds_And_One_Less_Fails(ulong value, int length, byte[] encoded)
    {
        Span<byte> exact = stackalloc byte[length];
        Assert.True(VarInt.TryWrite(exact, value, out int written));
        Assert.Equal(length, written);
        Assert.Equal(encoded, exact.ToArray());

        Span<byte> small = stackalloc byte[length - 1];
        Assert.False(VarInt.TryWrite(small, value, out written));
        Assert.Equal(0, written);
    }

    [Fact]
    public void TryWrite_Rejects_Value_Above_Max()
    {
        Span<byte> buffer = stackalloc byte[16];
        Assert.False(VarInt.TryWrite(buffer, VarInt.MaxValue + 1, out int written));
        Assert.Equal(0, written);
        Assert.False(VarInt.TryWrite(buffer, ulong.MaxValue, out written));
        Assert.Equal(0, written);
    }

    [Theory]
    [MemberData(nameof(Vectors))]
    public void Write_Span_Produces_Vector(ulong value, int length, byte[] encoded)
    {
        byte[] buffer = new byte[8];
        Assert.Equal(length, VarInt.Write(buffer, value));
        Assert.Equal(encoded, buffer.AsSpan(0, length).ToArray());
    }

    [Fact]
    public void Write_Span_Throws_On_Small_Destination()
    {
        ArgumentException ex = Assert.Throws<ArgumentException>(() => VarInt.Write(new byte[1], 64));
        Assert.Equal("destination", ex.ParamName);
        Assert.Throws<ArgumentException>(() => VarInt.Write(Span<byte>.Empty, 0));
    }

    [Fact]
    public void Write_Span_Throws_On_Value_Too_Large()
    {
        ArgumentOutOfRangeException ex = Assert.Throws<ArgumentOutOfRangeException>(() => VarInt.Write(new byte[8], VarInt.MaxValue + 1));
        Assert.Equal("value", ex.ParamName);
        // Too large takes precedence over a too-small destination.
        Assert.Throws<ArgumentOutOfRangeException>(() => VarInt.Write(new byte[1], ulong.MaxValue));
    }

    [Theory]
    [MemberData(nameof(Vectors))]
    public unsafe void Write_Pointer_Produces_Vector(ulong value, int length, byte[] encoded)
    {
        byte* buffer = stackalloc byte[8];
        Assert.Equal(length, VarInt.Write(buffer, value));
        Assert.Equal(encoded, new ReadOnlySpan<byte>(buffer, length).ToArray());
    }

    [Fact]
    public unsafe void Write_Pointer_Throws_On_Value_Too_Large()
    {
        byte* buffer = stackalloc byte[8];
        Assert.Throws<ArgumentOutOfRangeException>(() => VarInt.Write(buffer, VarInt.MaxValue + 1));
    }

    [Theory]
    [MemberData(nameof(Vectors))]
    public void TryRead_Span_Decodes_Vector(ulong value, int length, byte[] encoded)
    {
        Assert.True(VarInt.TryRead(encoded, out ulong read, out int consumed));
        Assert.Equal(value, read);
        Assert.Equal(length, consumed);

        // Trailing bytes are ignored.
        byte[] padded = new byte[encoded.Length + 3];
        encoded.CopyTo(padded, 0);
        padded[encoded.Length] = 0xFF;
        Assert.True(VarInt.TryRead(padded, out read, out consumed));
        Assert.Equal(value, read);
        Assert.Equal(length, consumed);
    }

    [Theory]
    [MemberData(nameof(Vectors))]
    public void TryRead_Span_Fails_On_Truncation(ulong value, int length, byte[] encoded)
    {
        for (int available = 0; available < length; available++)
        {
            Assert.False(VarInt.TryRead(encoded.AsSpan(0, available), out ulong read, out int consumed));
            Assert.Equal(0UL, read);
            Assert.Equal(0, consumed);
        }

        _ = value;
    }

    [Theory]
    [MemberData(nameof(Vectors))]
    public unsafe void TryRead_Pointer_Decodes_Vector(ulong value, int length, byte[] encoded)
    {
        fixed (byte* p = encoded)
        {
            Assert.True(VarInt.TryRead(p, encoded.Length, out ulong read, out int consumed));
            Assert.Equal(value, read);
            Assert.Equal(length, consumed);

            Assert.True(VarInt.TryRead(p, int.MaxValue, out read, out consumed));
            Assert.Equal(value, read);
        }
    }

    [Theory]
    [MemberData(nameof(Vectors))]
    public unsafe void TryRead_Pointer_Fails_On_Truncation(ulong value, int length, byte[] encoded)
    {
        Assert.Equal(length, encoded.Length);
        fixed (byte* p = encoded)
        {
            for (int available = -1; available < length; available++)
            {
                Assert.False(VarInt.TryRead(p, available, out ulong read, out int consumed));
                Assert.Equal(0UL, read);
                Assert.Equal(0, consumed);
            }
        }

        _ = value;
    }

    [Fact]
    public void Round_Trips_All_Length_Boundaries_And_Random_Values()
    {
        Random random = new(1234);
        Span<byte> buffer = stackalloc byte[8];
        for (int i = 0; i < 100_000; i++)
        {
            ulong value = i < 64
                ? (ulong)(1UL << i) - (ulong)(i & 1)
                : (ulong)random.NextInt64() >> (random.Next(0, 63));
            if (value > VarInt.MaxValue)
            {
                continue;
            }

            int expected = VarInt.GetLength(value);
            Assert.True(VarInt.TryWrite(buffer, value, out int written));
            Assert.Equal(expected, written);
            Assert.Equal(expected, VarInt.PeekLength(buffer[0]));
            Assert.True(VarInt.TryRead(buffer, out ulong read, out int consumed));
            Assert.Equal(value, read);
            Assert.Equal(expected, consumed);
        }
    }

    [Fact]
    public void Read_Of_Non_Canonical_Encoding_Yields_Value()
    {
        // 0 encoded in 8 bytes is not minimal but is valid input for the lenient reader.
        byte[] encoded = { 0xC0, 0, 0, 0, 0, 0, 0, 0 };
        Assert.True(VarInt.TryRead(encoded, out ulong value, out int consumed));
        Assert.Equal(0UL, value);
        Assert.Equal(8, consumed);
    }

    /// <summary>Encodes <paramref name="value"/> in exactly <paramref name="length"/> bytes (1, 2, 4 or 8), minimal or not.</summary>
    private static byte[] EncodeWithLength(ulong value, int length)
    {
        byte[] bytes = new byte[length];
        for (int i = length - 1; i >= 0; i--)
        {
            bytes[i] = (byte)value;
            value >>= 8;
        }

        bytes[0] |= (byte)(System.Numerics.BitOperations.Log2((uint)length) << 6);
        return bytes;
    }

    [Theory]
    [MemberData(nameof(Vectors))]
    public unsafe void TryReadMinimal_Accepts_Minimal_Vector(ulong value, int length, byte[] encoded)
    {
        Assert.True(VarInt.TryReadMinimal(encoded, out ulong read, out int consumed));
        Assert.Equal(value, read);
        Assert.Equal(length, consumed);

        // Padded so the wide 8-byte path is taken for the short encodings as well.
        byte[] padded = new byte[encoded.Length + 8];
        encoded.CopyTo(padded, 0);
        padded.AsSpan(encoded.Length).Fill(0xFF);
        Assert.True(VarInt.TryReadMinimal(padded, out read, out consumed));
        Assert.Equal(value, read);
        Assert.Equal(length, consumed);

        fixed (byte* p = padded)
        {
            Assert.True(VarInt.TryReadMinimal(p, encoded.Length, out read, out consumed));
            Assert.Equal(value, read);
            Assert.Equal(length, consumed);
            Assert.True(VarInt.TryReadMinimal(p, padded.Length, out read, out consumed));
            Assert.Equal(value, read);
            Assert.Equal(length, consumed);
        }
    }

    [Theory]
    [MemberData(nameof(Vectors))]
    public unsafe void TryReadMinimal_Rejects_Every_Longer_Encoding(ulong value, int length, byte[] encoded)
    {
        Assert.Equal(length, encoded.Length);
        for (int longer = length * 2; longer <= 8; longer *= 2)
        {
            byte[] wide = EncodeWithLength(value, longer);
            Assert.Equal(longer, VarInt.PeekLength(wide[0]));

            // The lenient reader still decodes it ...
            Assert.True(VarInt.TryRead(wide, out ulong read, out int consumed));
            Assert.Equal(value, read);
            Assert.Equal(longer, consumed);

            // ... the strict reader rejects it on both the short (exact length) and wide (>= 8 bytes) paths.
            Assert.False(VarInt.TryReadMinimal(wide, out read, out consumed));
            Assert.Equal(0UL, read);
            Assert.Equal(0, consumed);

            byte[] padded = new byte[wide.Length + 8];
            wide.CopyTo(padded, 0);
            Assert.False(VarInt.TryReadMinimal(padded, out read, out consumed));
            Assert.Equal(0UL, read);
            Assert.Equal(0, consumed);

            fixed (byte* p = padded)
            {
                Assert.False(VarInt.TryReadMinimal(p, wide.Length, out read, out consumed));
                Assert.Equal(0UL, read);
                Assert.Equal(0, consumed);
                Assert.False(VarInt.TryReadMinimal(p, padded.Length, out read, out consumed));
                Assert.Equal(0UL, read);
                Assert.Equal(0, consumed);
            }
        }
    }

    [Theory]
    [MemberData(nameof(Vectors))]
    public unsafe void TryReadMinimal_Fails_On_Truncation(ulong value, int length, byte[] encoded)
    {
        for (int available = 0; available < length; available++)
        {
            Assert.False(VarInt.TryReadMinimal(encoded.AsSpan(0, available), out ulong read, out int consumed));
            Assert.Equal(0UL, read);
            Assert.Equal(0, consumed);
        }

        fixed (byte* p = encoded)
        {
            for (int available = -1; available < length; available++)
            {
                Assert.False(VarInt.TryReadMinimal(p, available, out ulong read, out int consumed));
                Assert.Equal(0UL, read);
                Assert.Equal(0, consumed);
            }
        }

        _ = value;
    }

    [Fact]
    public void TryReadMinimal_Boundaries_Of_Every_Length()
    {
        // The largest value of each length is minimal at that length; the smallest value of the next length is
        // minimal there and non-minimal one step up. Also, the same values one length wider are rejected.
        (ulong Value, int Length)[] boundaries =
        {
            (0, 1), (63, 1), (64, 2), (16383, 2), (16384, 4), ((1UL << 30) - 1, 4), (1UL << 30, 8), (VarInt.MaxValue, 8),
        };
        foreach ((ulong value, int length) in boundaries)
        {
            for (int len = 1; len <= 8; len *= 2)
            {
                if (len < length)
                {
                    continue; // does not fit
                }

                byte[] bytes = EncodeWithLength(value, len);
                bool ok = VarInt.TryReadMinimal(bytes, out ulong read, out int consumed);
                Assert.Equal(len == length, ok);
                Assert.Equal(ok ? value : 0UL, read);
                Assert.Equal(ok ? len : 0, consumed);
            }
        }
    }

    [Fact]
    public void TryReadMinimal_Random_Round_Trip_And_Widened_Rejection()
    {
        Random random = new(4321);
        Span<byte> buffer = stackalloc byte[16];
        for (int i = 0; i < 50_000; i++)
        {
            ulong value = (ulong)random.NextInt64() >> random.Next(0, 63);
            if (value > VarInt.MaxValue)
            {
                continue;
            }

            int length = VarInt.Write(buffer, value);
            Assert.True(VarInt.TryReadMinimal(buffer, out ulong read, out int consumed));
            Assert.Equal(value, read);
            Assert.Equal(length, consumed);

            if (length < 8)
            {
                byte[] wide = EncodeWithLength(value, length * 2);
                Assert.False(VarInt.TryReadMinimal(wide, out _, out _));
            }
        }
    }

    [Fact]
    public unsafe void Steady_State_Does_Not_Allocate()
    {
        byte[] buffer = new byte[16];
        ulong[] values = { 5, 300, 70_000, 5_000_000_000 };
        ulong sink = 0;

        Run(buffer, values, ref sink); // warm-up
        WindowedAllocation.AssertNone(() => Run(buffer, values, ref sink));
        Assert.NotEqual(0UL, sink);

        static void Run(byte[] buffer, ulong[] values, ref ulong sink)
        {
            for (int i = 0; i < 10_000; i++)
            {
                for (int j = 0; j < values.Length; j++)
                {
                    ulong v = values[j];
                    int n = VarInt.Write(buffer, v);
                    VarInt.TryWrite(buffer, v, out _);
                    VarInt.TryRead(buffer, out ulong r, out _);
                    sink += r + (ulong)n + (ulong)VarInt.GetLength(v) + (ulong)VarInt.PeekLength(buffer[0]);
                    VarInt.TryReadMinimal(buffer, out r, out _);
                    sink += r;
                    fixed (byte* p = buffer)
                    {
                        VarInt.Write(p, v);
                        VarInt.TryRead(p, buffer.Length, out r, out _);
                        sink += r;
                        VarInt.TryReadMinimal(p, buffer.Length, out r, out _);
                        sink += r;
                    }
                }
            }
        }
    }
}
