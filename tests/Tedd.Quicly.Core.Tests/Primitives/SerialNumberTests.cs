using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Core.Tests.Primitives;

public class SerialNumberTests
{
    [Fact]
    public void Constants_Are_Half_The_Space_Minus_One()
    {
        Assert.Equal(32767, SerialNumber.MaxDistance16);
        Assert.Equal(2147483647, SerialNumber.MaxDistance32);
    }

    [Theory]
    [InlineData((ushort)1, (ushort)0, true)]
    [InlineData((ushort)0, (ushort)1, false)]
    [InlineData((ushort)0, (ushort)0, false)]
    [InlineData((ushort)0, (ushort)0xFFFF, true)]      // wrap-around: 0 is newer than 65535
    [InlineData((ushort)0xFFFF, (ushort)0, false)]
    [InlineData((ushort)5, (ushort)0xFFFB, true)]      // 5 is 10 ahead of 65531
    [InlineData((ushort)0xFFFB, (ushort)5, false)]
    [InlineData((ushort)32767, (ushort)0, true)]       // largest forward distance
    [InlineData((ushort)32768, (ushort)0, false)]      // exactly half the space: treated as older
    [InlineData((ushort)0, (ushort)32768, false)]      // ... and never newer in both directions
    [InlineData((ushort)32769, (ushort)0, false)]
    [InlineData((ushort)0, (ushort)32769, true)]
    public void IsNewer_16(ushort a, ushort b, bool expected)
    {
        Assert.Equal(expected, SerialNumber.IsNewer(a, b));
        Assert.Equal(expected || a == b, SerialNumber.IsNewerOrEqual(a, b));
    }

    [Theory]
    [InlineData((ushort)0, (ushort)0, 0)]
    [InlineData((ushort)1, (ushort)0, 1)]
    [InlineData((ushort)0, (ushort)1, -1)]
    [InlineData((ushort)0, (ushort)0xFFFF, 1)]
    [InlineData((ushort)0xFFFF, (ushort)0, -1)]
    [InlineData((ushort)32767, (ushort)0, 32767)]
    [InlineData((ushort)32768, (ushort)0, -32768)]
    [InlineData((ushort)0, (ushort)32768, -32768)]
    [InlineData((ushort)0, (ushort)32767, -32767)]
    [InlineData((ushort)100, (ushort)0xFF9C, 200)]     // 100 - 65436 == +200 in serial space
    public void Distance_16(ushort a, ushort b, int expected) => Assert.Equal(expected, SerialNumber.Distance(a, b));

    [Theory]
    [InlineData(1u, 0u, true)]
    [InlineData(0u, 1u, false)]
    [InlineData(0u, 0u, false)]
    [InlineData(0u, 0xFFFFFFFFu, true)]
    [InlineData(0xFFFFFFFFu, 0u, false)]
    [InlineData(0x7FFFFFFFu, 0u, true)]
    [InlineData(0x80000000u, 0u, false)]
    [InlineData(0u, 0x80000000u, false)]
    [InlineData(0x80000001u, 0u, false)]
    [InlineData(0u, 0x80000001u, true)]
    public void IsNewer_32(uint a, uint b, bool expected)
    {
        Assert.Equal(expected, SerialNumber.IsNewer(a, b));
        Assert.Equal(expected || a == b, SerialNumber.IsNewerOrEqual(a, b));
    }

    [Theory]
    [InlineData(0u, 0u, 0)]
    [InlineData(1u, 0u, 1)]
    [InlineData(0u, 1u, -1)]
    [InlineData(0u, 0xFFFFFFFFu, 1)]
    [InlineData(0xFFFFFFFFu, 0u, -1)]
    [InlineData(0x7FFFFFFFu, 0u, int.MaxValue)]
    [InlineData(0x80000000u, 0u, int.MinValue)]
    [InlineData(0u, 0x80000000u, int.MinValue)]
    [InlineData(10u, 0xFFFFFFF6u, 20)]
    public void Distance_32(uint a, uint b, int expected) => Assert.Equal(expected, SerialNumber.Distance(a, b));

    [Fact]
    public void Exhaustive_16_Bit_Against_Reference_Definition()
    {
        // RFC 1982 for every (a, b) with b fixed at a few bases: a > b iff (a < b && b - a > 2^15) || (a > b && a - b < 2^15).
        ushort[] bases = { 0, 1, 12345, 32767, 32768, 65534, 65535 };
        foreach (ushort b in bases)
        {
            for (int ai = 0; ai <= ushort.MaxValue; ai++)
            {
                ushort a = (ushort)ai;
                bool expected = (a < b && b - a > 32768) || (a > b && a - b < 32768);
                Assert.Equal(expected, SerialNumber.IsNewer(a, b));
                Assert.Equal(expected || a == b, SerialNumber.IsNewerOrEqual(a, b));
                int d = SerialNumber.Distance(a, b);
                Assert.InRange(d, short.MinValue, short.MaxValue);
                Assert.Equal(a, (ushort)(b + d));
            }
        }
    }

    [Fact]
    public void Advancing_Past_Wrap_Stays_Monotonic()
    {
        ushort s16 = 0xFFF0;
        uint s32 = 0xFFFFFFF0;
        for (int i = 0; i < 64; i++)
        {
            ushort n16 = (ushort)(s16 + 1);
            uint n32 = s32 + 1;
            Assert.True(SerialNumber.IsNewer(n16, s16));
            Assert.False(SerialNumber.IsNewer(s16, n16));
            Assert.Equal(1, SerialNumber.Distance(n16, s16));
            Assert.True(SerialNumber.IsNewer(n32, s32));
            Assert.False(SerialNumber.IsNewer(s32, n32));
            Assert.Equal(1, SerialNumber.Distance(n32, s32));
            s16 = n16;
            s32 = n32;
        }
    }

    [Fact]
    public void Steady_State_Does_Not_Allocate()
    {
        int sink = 0;
        Run(ref sink);
        long before = GC.GetAllocatedBytesForCurrentThread();
        Run(ref sink);
        long after = GC.GetAllocatedBytesForCurrentThread();
        Assert.Equal(0, after - before);
        Assert.NotEqual(0, sink);

        static void Run(ref int sink)
        {
            for (int i = 0; i < 100_000; i++)
            {
                ushort a = (ushort)i;
                ushort b = (ushort)(i * 7);
                uint c = (uint)i * 3u;
                uint d = (uint)i * 11u;
                sink += SerialNumber.Distance(a, b) + SerialNumber.Distance(c, d);
                sink += SerialNumber.IsNewer(a, b) ? 1 : 0;
                sink += SerialNumber.IsNewerOrEqual(a, b) ? 1 : 0;
                sink += SerialNumber.IsNewer(c, d) ? 1 : 0;
                sink += SerialNumber.IsNewerOrEqual(c, d) ? 1 : 0;
            }
        }
    }
}
