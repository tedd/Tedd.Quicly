using System.Runtime.CompilerServices;
using Tedd.Quicly.Core.Threading;

namespace Tedd.Quicly.Core.Tests.Threading;

public class CacheLineTests
{
    [Fact]
    public void Constants_Match_Target_Hardware()
    {
        Assert.Equal(64, CacheLine.Size);
        Assert.Equal(128, CacheLine.Stride);
    }

    [Fact]
    public void PaddedLong_Isolates_Its_Value()
    {
        Assert.Equal(256, Unsafe.SizeOf<PaddedLong>());
        PaddedLong[] pair = new PaddedLong[2];
        pair[0].Value = 1;
        pair[1].Value = 2;
        Assert.Equal(1, pair[0].Value);
        Assert.Equal(2, pair[1].Value);

        // The value sits a full stride from both ends: two adjacent PaddedLongs are 256 bytes apart.
        nint distance = Unsafe.ByteOffset(ref pair[0].Value, ref pair[1].Value);
        Assert.Equal((nint)256, distance);
    }
}
