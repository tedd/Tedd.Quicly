namespace Tedd.Quicly.Replication.Tests;

public class DedupWindowTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(63)]
    [InlineData(65)]
    [InlineData(-64)]
    [InlineData((1 << 24) + 64)]
    public void Constructor_Rejects_Bad_Sizes(int bits) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => new DedupWindow(bits));

    [Fact]
    public void Accepts_Once_And_Rejects_Duplicates()
    {
        DedupWindow window = new();
        Assert.Equal(DedupWindow.DefaultWindowBits, window.WindowBits);
        Assert.False(window.HasAccepted);
        Assert.Equal(DedupStatus.New, window.Check(42));
        Assert.True(window.TryAccept(42));
        Assert.True(window.HasAccepted);
        Assert.Equal(42UL, window.Highest);
        Assert.False(window.TryAccept(42));
        Assert.Equal(DedupStatus.Duplicate, window.Check(42));
        Assert.Equal(DedupStatus.Duplicate, window.Accept(42));
        Assert.True(window.TryAccept(40));
        Assert.True(window.TryAccept(43));
        Assert.False(window.TryAccept(40));
        Assert.Equal(43UL, window.Highest);
    }

    [Fact]
    public void Window_Edges()
    {
        DedupWindow window = new(1024);
        Assert.True(window.TryAccept(5000));
        Assert.Equal(DedupStatus.New, window.Check(5000 - 1023));
        Assert.Equal(DedupStatus.TooOld, window.Check(5000 - 1024));
        Assert.Equal(DedupStatus.New, window.Accept(5000 - 1023));
        Assert.Equal(DedupStatus.TooOld, window.Accept(5000 - 1024));
        Assert.Equal(DedupStatus.Duplicate, window.Accept(5000 - 1023));

        // Sliding by less than the window keeps what is still inside it.
        Assert.True(window.TryAccept(5100));
        Assert.False(window.TryAccept(5000));
        Assert.Equal(DedupStatus.TooOld, window.Check(5000 - 1023));
        Assert.True(window.TryAccept(5100 - 1023));

        // Far ahead: the window slides past everything.
        Assert.True(window.TryAccept(100_000));
        Assert.Equal(DedupStatus.TooOld, window.Accept(5100));
        Assert.True(window.TryAccept(100_000 - 1));
        Assert.True(window.TryAccept(100_000 - 1023));
    }

    [Fact]
    public void Extreme_Ids()
    {
        DedupWindow window = new(64);
        Assert.True(window.TryAccept(0));
        Assert.True(window.TryAccept(ulong.MaxValue - 5));
        Assert.Equal(DedupStatus.TooOld, window.Accept(0));
        Assert.True(window.TryAccept(ulong.MaxValue));
        Assert.False(window.TryAccept(ulong.MaxValue));
        Assert.False(window.TryAccept(ulong.MaxValue - 5));
        Assert.True(window.TryAccept(ulong.MaxValue - 63));
        Assert.Equal(DedupStatus.TooOld, window.Accept(ulong.MaxValue - 64));

        window.Reset();
        Assert.False(window.HasAccepted);
        Assert.Equal(0UL, window.Highest);
        Assert.True(window.TryAccept(ulong.MaxValue));
        Assert.True(window.TryAccept(ulong.MaxValue - 1));
    }

    [Theory]
    [InlineData(64)]
    [InlineData(128)]
    [InlineData(1024)]
    [InlineData(4096)]
    public void Random_Traffic_Matches_A_Model(int windowBits)
    {
        Random random = new(windowBits);
        DedupWindow window = new(windowBits);
        HashSet<ulong> accepted = [];
        ulong highest = 0;
        bool started = false;
        ulong center = 1_000_000;
        for (int i = 0; i < 100_000; i++)
        {
            center += (ulong)random.Next(0, 4);
            if (random.Next(500) == 0)
            {
                center += (ulong)random.Next(windowBits / 2, windowBits * 70);
            }

            ulong id = center - (ulong)random.Next(0, windowBits + windowBits / 2) + (ulong)random.Next(0, 8);
            DedupStatus expected = !started || id > highest ? DedupStatus.New
                : highest - id >= (ulong)windowBits ? DedupStatus.TooOld
                : accepted.Contains(id) ? DedupStatus.Duplicate
                : DedupStatus.New;
            Assert.Equal(expected, window.Check(id));
            Assert.Equal(expected, window.Accept(id));
            if (expected == DedupStatus.New)
            {
                accepted.Add(id);
                if (!started || id > highest)
                {
                    highest = id;
                }

                started = true;
            }

            Assert.Equal(highest, window.Highest);
        }
    }
}
