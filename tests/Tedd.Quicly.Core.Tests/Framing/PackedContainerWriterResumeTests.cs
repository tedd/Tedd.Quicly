using Tedd.Quicly.Core.Framing;

namespace Tedd.Quicly.Core.Tests.Framing;

/// <summary>The resume constructor of <see cref="PackedContainerWriter"/> (used by the session's packer across calls).</summary>
public class PackedContainerWriterResumeTests
{
    [Fact]
    public void A_Resumed_Writer_Continues_The_Same_Container()
    {
        byte[] buffer = new byte[64];
        PackedContainerWriter writer = new(buffer, 7, hasTick: true);
        Assert.True(writer.TryAppend([2, 1, 2]));
        int length = writer.Length;
        int count = writer.Count;

        PackedContainerWriter resumed = new(buffer, length, count);
        Assert.Equal(length, resumed.Length);
        Assert.Equal(count, resumed.Count);
        Assert.Equal(buffer.Length - length, resumed.Remaining);
        Assert.True(resumed.TryReserve(2, out Span<byte> slot));
        slot[0] = 3;
        slot[1] = 9;
        Assert.Equal(ParseStatus.Ok, PackedContainer.TryParse(buffer.AsSpan(0, resumed.Length), out PackedContainerReader reader));
        Assert.Equal(2, reader.Count);
        Assert.True(reader.HasTick);
        Assert.Equal(7u, reader.Tick);
        Assert.True(reader.MoveNext());
        Assert.Equal(new byte[] { 2, 1, 2 }, reader.Current.ToArray());
        Assert.True(reader.MoveNext());
        Assert.Equal(new byte[] { 3, 9 }, reader.Current.ToArray());
    }

    [Fact]
    public void A_Resumed_Writer_Keeps_The_Message_Limit()
    {
        byte[] buffer = new byte[512];
        PackedContainerWriter writer = new(buffer);
        for (int i = 0; i < PackedContainer.MaxMessages; i++)
        {
            Assert.True(writer.TryAppend([2]));
        }

        PackedContainerWriter resumed = new(buffer, writer.Length, writer.Count);
        Assert.False(resumed.CanAppend(1));
        Assert.False(resumed.TryAppend([2]));
    }

    [Fact]
    public void Resuming_Checks_Its_Arguments()
    {
        byte[] buffer = new byte[16];
        PackedContainer.WriteHeader(buffer, 0, hasTick: false);
        Assert.Throws<ArgumentOutOfRangeException>(() => { _ = new PackedContainerWriter(buffer, 1, 0); });
        Assert.Throws<ArgumentOutOfRangeException>(() => { _ = new PackedContainerWriter(buffer, 17, 0); });
        Assert.Throws<ArgumentOutOfRangeException>(() => { _ = new PackedContainerWriter(buffer, 2, -1); });
        Assert.Throws<ArgumentOutOfRangeException>(() => { _ = new PackedContainerWriter(buffer, 2, PackedContainer.MaxMessages + 1); });
        byte[] other = new byte[16];
        Assert.Throws<ArgumentException>(() => { _ = new PackedContainerWriter(other, 2, 0); });
    }
}
