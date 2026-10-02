namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>
/// The bulk tests' <see cref="PatternSource"/> writes and checks its pattern a block at a time; these pin that to the byte
/// formula <see cref="PatternSource.At"/>, which is what every byte-exact bulk test relies on.
/// </summary>
public class PatternSourceTests
{
    [Theory]
    [InlineData(0L, 1)]
    [InlineData(0L, 70_000)]
    [InlineData(1L, 4_095)]
    [InlineData(2_047L, 2)]
    [InlineData(255L, 2_049)]
    [InlineData(1_000_003L, 65_536)]
    [InlineData((1L << 31) - 65_536, 65_536)]
    [InlineData((1L << 31) - 3, 7)]
    [InlineData((5L << 30) + 12_345, 33_333)]
    public void Fill_Writes_At_Of_Every_Offset(long offset, int length)
    {
        byte[] bytes = new byte[length];
        PatternSource.Fill(offset, bytes);
        for (int i = 0; i < length; i++)
        {
            Assert.Equal(PatternSource.At(offset + i), bytes[i]);
        }

        Assert.Equal(0, PatternSource.Mismatches(offset, bytes));
    }

    [Fact]
    public void Read_Stops_At_The_Total()
    {
        PatternSource source = new(10_000);
        byte[] bytes = new byte[4_096];
        Assert.Equal(4_096, source.Read(0, bytes));
        Assert.Equal(1_808, source.Read(8_192, bytes));
        Assert.Equal(PatternSource.At(9_999), bytes[1_807]);
        Assert.Equal(0, source.Read(10_000, bytes));
    }

    [Fact]
    public void Mismatches_Counts_Every_Wrong_Byte_Wherever_It_Is()
    {
        Random random = new(7);
        for (int round = 0; round < 200; round++)
        {
            long offset = random.NextInt64(0, 1L << 33);
            byte[] bytes = new byte[random.Next(1, 10_000)];
            PatternSource.Fill(offset, bytes);
            int wrong = random.Next(0, Math.Min(bytes.Length, 50) + 1);
            HashSet<int> flipped = [];
            while (flipped.Count < wrong)
            {
                int at = random.Next(bytes.Length);
                if (flipped.Add(at))
                {
                    bytes[at] ^= (byte)random.Next(1, 256);
                }
            }

            Assert.Equal(wrong, PatternSource.Mismatches(offset, bytes));
        }
    }
}
