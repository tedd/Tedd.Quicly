using System.Runtime.CompilerServices;
using Tedd.Quicly.Core.State;

namespace Tedd.Quicly.Core.Tests.State;

public unsafe class NativeArrayTests
{
    [Fact]
    public void Allocates_Zeroed_And_64_Byte_Aligned()
    {
        using var array = new NativeArray<long>(1000);
        Assert.Equal(1000, array.Length);
        Assert.Equal(8000, array.ByteLength);
        Assert.Equal(0, (nint)array.Pointer % NativeArray<long>.Alignment);
        Assert.False(array.IsDisposed);
        foreach (long value in array.AsSpan())
            Assert.Equal(0, value);
    }

    [Fact]
    public void Zero_Length_Allocates_Nothing()
    {
        using var array = new NativeArray<int>(0);
        Assert.Equal(0, array.Length);
        Assert.Equal(0, array.ByteLength);
        Assert.True(array.Pointer is null);
        Assert.True(array.AsSpan().IsEmpty);
        array.Clear();
        array.Fill(5);
        Assert.Throws<ArgumentOutOfRangeException>(() => array.At(0));
    }

    [Fact]
    public void Negative_Length_Throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new NativeArray<int>(-1));
    }

    [Fact]
    public void Indexer_And_At_Return_The_Same_Reference()
    {
        using var array = new NativeArray<int>(4);
        array[2] = 42;
        Assert.Equal(42, array.At(2));
        Assert.True(Unsafe.AreSame(ref array[2], ref array.At(2)));
        Assert.True(Unsafe.AreSame(ref array[3], ref array.AsSpan()[3]));
        Assert.True(&array[1] == array.Pointer + 1);
    }

    [Theory]
    [InlineData(-1)]
    [InlineData(4)]
    [InlineData(int.MaxValue)]
    [InlineData(int.MinValue)]
    public void At_Rejects_Out_Of_Range(int index)
    {
        using var array = new NativeArray<int>(4);
        Assert.Throws<ArgumentOutOfRangeException>(() => array.At(index));
    }

    [Fact]
    public void Slice_Is_Bounds_Checked()
    {
        using var array = new NativeArray<byte>(8);
        for (int i = 0; i < 8; i++)
            array[i] = (byte)i;

        Span<byte> slice = array.AsSpan(2, 3);
        Assert.Equal(new byte[] { 2, 3, 4 }, slice.ToArray());
        Assert.True(array.AsSpan(8, 0).IsEmpty);

        Assert.Throws<ArgumentOutOfRangeException>(() => array.AsSpan(-1, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => array.AsSpan(9, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => array.AsSpan(6, 3));
        Assert.Throws<ArgumentOutOfRangeException>(() => array.AsSpan(0, -1));
    }

    [Fact]
    public void Fill_And_Clear_Touch_Every_Element()
    {
        using var array = new NativeArray<ulong>(100);
        array.Fill(ulong.MaxValue);
        foreach (ulong value in array.AsSpan())
            Assert.Equal(ulong.MaxValue, value);

        array.Clear();
        foreach (ulong value in array.AsSpan())
            Assert.Equal(0UL, value);
    }

    [Fact]
    public void Dispose_Frees_And_Is_Idempotent()
    {
        var array = new NativeArray<int>(16);
        array.Dispose();
        Assert.True(array.IsDisposed);
        Assert.Equal(0, array.Length);
        Assert.True(array.Pointer is null);
        Assert.True(array.AsSpan().IsEmpty);
        Assert.Throws<ArgumentOutOfRangeException>(() => array.At(0));

        array.Dispose();
        Assert.True(array.IsDisposed);
    }

    [Fact]
    public void Finalizer_Frees_An_Undisposed_Array()
    {
        AllocateAndDrop();
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        [MethodImpl(MethodImplOptions.NoInlining)]
        static void AllocateAndDrop()
        {
            var array = new NativeArray<long>(64);
            array[0] = 1;
        }
    }
}
