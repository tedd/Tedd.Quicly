using System.Text;
using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Core.Tests.Primitives;

public class XxHash64Tests
{
    private static ulong Reference(ReadOnlySpan<byte> data, ulong seed = 0) =>
        System.IO.Hashing.XxHash64.HashToUInt64(data, (long)seed);

    [Fact]
    public void Known_Vector_Empty()
    {
        Assert.Equal(0xEF46DB3751D8E999UL, XxHash64.Hash(ReadOnlySpan<byte>.Empty));
        Assert.Equal(0xEF46DB3751D8E999UL, Reference(ReadOnlySpan<byte>.Empty));
    }

    [Fact]
    public void Known_Vector_Abc()
    {
        byte[] abc = Encoding.ASCII.GetBytes("abc");
        Assert.Equal(0x44BC2CF5AD770999UL, XxHash64.Hash(abc));
        Assert.Equal(0x44BC2CF5AD770999UL, Reference(abc));
    }

    [Fact]
    public void Known_Vector_Seeded()
    {
        // XXH64("abc", seed = 1) from the reference implementation.
        byte[] abc = Encoding.ASCII.GetBytes("abc");
        Assert.Equal(Reference(abc, 1), XxHash64.Hash(abc, 1));
        Assert.NotEqual(XxHash64.Hash(abc, 0), XxHash64.Hash(abc, 1));
    }

    [Fact]
    public void Matches_Reference_For_Every_Length_Up_To_300()
    {
        // Covers: < 32 (no stripes), exactly 32, tails of 8, 4 and 1..3 bytes, and multi-stripe inputs.
        byte[] data = new byte[300];
        new Random(42).NextBytes(data);
        for (int length = 0; length <= data.Length; length++)
        {
            ReadOnlySpan<byte> slice = data.AsSpan(0, length);
            Assert.Equal(Reference(slice), XxHash64.Hash(slice));
            Assert.Equal(Reference(slice, 0x9E3779B97F4A7C15UL), XxHash64.Hash(slice, 0x9E3779B97F4A7C15UL));
        }
    }

    [Fact]
    public void Matches_Reference_For_Random_Inputs_And_Seeds()
    {
        Random random = new(7);
        byte[] data = new byte[70_000];
        for (int i = 0; i < 400; i++)
        {
            int length = random.Next(0, 4) switch
            {
                0 => random.Next(0, 64),
                1 => random.Next(0, 1024),
                2 => random.Next(0, 8192),
                _ => random.Next(0, data.Length + 1),
            };
            random.NextBytes(data.AsSpan(0, length));
            ulong seed = (ulong)random.NextInt64() ^ ((ulong)random.NextInt64() << 1);
            Assert.Equal(Reference(data.AsSpan(0, length)), XxHash64.Hash(data.AsSpan(0, length)));
            Assert.Equal(Reference(data.AsSpan(0, length), seed), XxHash64.Hash(data.AsSpan(0, length), seed));
        }
    }

    [Fact]
    public void Unaligned_Input_Gives_Same_Hash()
    {
        byte[] data = new byte[1024 + 7];
        new Random(3).NextBytes(data);
        ulong aligned = XxHash64.Hash(data.AsSpan(0, 1024));
        byte[] shifted = new byte[1024 + 7];
        data.AsSpan(0, 1024).CopyTo(shifted.AsSpan(3));
        Assert.Equal(aligned, XxHash64.Hash(shifted.AsSpan(3, 1024)));
        Assert.Equal(Reference(shifted.AsSpan(3, 1024)), aligned);
    }

    [Fact]
    public void Steady_State_Does_Not_Allocate()
    {
        byte[] data = new byte[1500];
        new Random(9).NextBytes(data);
        ulong sink = 0;
        Run(data, ref sink);
        WindowedAllocation.AssertNone(() => Run(data, ref sink));
        Assert.NotEqual(0UL, sink);

        static void Run(byte[] data, ref ulong sink)
        {
            for (int i = 0; i < 2_000; i++)
            {
                sink += XxHash64.Hash(data.AsSpan(0, i % data.Length), (ulong)i);
            }
        }
    }
}
