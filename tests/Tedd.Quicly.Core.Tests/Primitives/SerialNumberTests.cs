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

    [Theory]
    [InlineData(0x1_0064UL, (ushort)100, 0x1_0064UL)]        // duplicate of the newest
    [InlineData(0x1_0064UL, (ushort)101, 0x1_0065UL)]        // ahead
    [InlineData(0x1_0064UL, (ushort)32_867, 0x1_8063UL)]     // furthest ahead
    [InlineData(0x1_0064UL, (ushort)32_868, 0x0_8064UL)]     // exactly half the space: behind
    [InlineData(0x1_0064UL, (ushort)99, 0x1_0063UL)]         // behind
    [InlineData(0x1_FFFFUL, (ushort)0, 0x2_0000UL)]          // across the wrap: the next cycle
    [InlineData(0x2_0000UL, (ushort)0xFFFF, 0x1_FFFFUL)]     // ... and back behind it
    [InlineData(0x1_0000UL, (ushort)0x8000, 0x0_8000UL)]     // the lowest seed, half behind: no underflow
    [InlineData(0x7_0005UL, (ushort)5, 0x7_0005UL)]          // the cycle count is carried
    public void Extend_16(ulong newest, ushort sequence, ulong expected)
    {
        ulong extended = SerialNumber.Extend(newest, sequence);
        Assert.Equal(expected, extended);
        Assert.Equal(sequence, (ushort)extended);
        Assert.Equal(SerialNumber.IsNewer(sequence, (ushort)newest), extended > newest);
    }

    [Theory]
    [InlineData(0x1_0000_0064UL, 100u, 0x1_0000_0064UL)]
    [InlineData(0x1_0000_0064UL, 101u, 0x1_0000_0065UL)]
    [InlineData(0x1_0000_0064UL, 0x8000_0063u, 0x1_8000_0063UL)]   // furthest ahead
    [InlineData(0x1_0000_0064UL, 0x8000_0064u, 0x0_8000_0064UL)]   // exactly half the space: behind
    [InlineData(0x1_0000_0064UL, 99u, 0x1_0000_0063UL)]
    [InlineData(0x1_FFFF_FFFFUL, 0u, 0x2_0000_0000UL)]             // across the wrap
    [InlineData(0x2_0000_0000UL, 0xFFFF_FFFFu, 0x1_FFFF_FFFFUL)]
    [InlineData(0x1_0000_0000UL, 0x8000_0000u, 0x0_8000_0000UL)]   // the lowest seed, half behind: no underflow
    public void Extend_32(ulong newest, uint sequence, ulong expected)
    {
        ulong extended = SerialNumber.Extend(newest, sequence);
        Assert.Equal(expected, extended);
        Assert.Equal(sequence, (uint)extended);
        Assert.Equal(SerialNumber.IsNewer(sequence, (uint)newest), extended > newest);
    }

    [Fact]
    public void Extend_Follows_A_Counter_Through_Many_Wraps()
    {
        // A 16-bit counter stepping by 30 000 (under half the space) for more than four wraps: the extended value advances
        // by exactly the step every time.
        ulong newest = 0x1_0000UL + 5;
        ushort sequence = 5;
        for (int i = 0; i < 12; i++)
        {
            sequence = (ushort)(sequence + 30_000);
            ulong extended = SerialNumber.Extend(newest, sequence);
            Assert.Equal(newest + 30_000, extended);
            newest = extended;
        }

        Assert.True(newest > 0x5_0000UL);
    }

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
        WindowedAllocation.AssertNone(() => Run(ref sink));
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
                sink += (int)SerialNumber.Extend(0x1_0000UL + b, a) + (int)SerialNumber.Extend(0x1_0000_0000UL + d, c);
            }
        }
    }
}
