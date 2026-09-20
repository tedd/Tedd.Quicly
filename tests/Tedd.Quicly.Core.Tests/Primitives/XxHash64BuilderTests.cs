using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Core.Tests.Primitives;

/// <summary>
/// <see cref="XxHash64Builder"/> is the streaming form of <see cref="XxHash64"/>, and the only thing it has to get right
/// is that the pieces never show: the digest must be the one-shot digest of everything appended, however the bytes were
/// cut up. The one-shot is itself checked against the reference vectors elsewhere, so it is the oracle here.
/// </summary>
public class XxHash64BuilderTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(3)]
    [InlineData(4)]
    [InlineData(7)]
    [InlineData(8)]
    [InlineData(31)]
    [InlineData(32)]
    [InlineData(33)]
    [InlineData(63)]
    [InlineData(64)]
    [InlineData(65)]
    [InlineData(1000)]
    public void One_Append_Matches_The_One_Shot(int length)
    {
        byte[] data = Payload(length);
        XxHash64Builder builder = XxHash64Builder.Create();
        builder.Append(data);
        Assert.Equal(XxHash64.Hash(data), builder.Digest());
        Assert.Equal((ulong)length, builder.Length);
    }

    [Fact]
    public void Every_Way_Of_Splitting_The_Same_Bytes_Gives_The_Same_Digest()
    {
        // Every split point of a 96-byte input (three stripes), so the tail is exercised empty, part-full and straddling
        // a stripe boundary in both halves.
        byte[] data = Payload(96);
        ulong expected = XxHash64.Hash(data);
        for (int cut = 0; cut <= data.Length; cut++)
        {
            XxHash64Builder builder = XxHash64Builder.Create();
            builder.Append(data.AsSpan(0, cut));
            builder.Append(data.AsSpan(cut));
            Assert.Equal(expected, builder.Digest());
        }
    }

    [Fact]
    public void Randomly_Chopped_Input_Matches_The_One_Shot()
    {
        // The shape the engine really produces: a long object handed over in pieces of no particular size.
        Random random = new(20260920);
        for (int trial = 0; trial < 200; trial++)
        {
            byte[] data = Payload(random.Next(0, 5000));
            XxHash64Builder builder = XxHash64Builder.Create();
            int at = 0;
            while (at < data.Length)
            {
                int take = Math.Min(random.Next(1, 200), data.Length - at);
                builder.Append(data.AsSpan(at, take));
                at += take;
            }

            Assert.Equal(XxHash64.Hash(data), builder.Digest());
        }
    }

    [Fact]
    public void Digest_Does_Not_Consume_The_Builder()
    {
        // The send side digests when it writes the trailer and the receive side when the body ends; neither should have
        // to care that the other might have digested already.
        byte[] data = Payload(200);
        XxHash64Builder builder = XxHash64Builder.Create();
        builder.Append(data.AsSpan(0, 120));
        ulong prefix = builder.Digest();
        Assert.Equal(XxHash64.Hash(data.AsSpan(0, 120)), prefix);
        Assert.Equal(prefix, builder.Digest());

        builder.Append(data.AsSpan(120));
        Assert.Equal(XxHash64.Hash(data), builder.Digest());
    }

    [Fact]
    public void A_Copy_Of_The_Builder_Snapshots_It()
    {
        // How the send side rolls back a piece the transport refused: the bytes are read and hashed again next pass, so
        // the state before the piece has to be restorable by plain assignment.
        byte[] data = Payload(300);
        XxHash64Builder builder = XxHash64Builder.Create();
        builder.Append(data.AsSpan(0, 100));

        XxHash64Builder saved = builder;
        builder.Append(data.AsSpan(100, 75));
        builder = saved;
        builder.Append(data.AsSpan(100));

        Assert.Equal(XxHash64.Hash(data), builder.Digest());
    }

    [Fact]
    public void A_Seed_Carries_Through()
    {
        byte[] data = Payload(300);
        XxHash64Builder builder = XxHash64Builder.Create(0xDEADBEEFUL);
        builder.Append(data.AsSpan(0, 40));
        builder.Append(data.AsSpan(40));
        Assert.Equal(XxHash64.Hash(data, 0xDEADBEEFUL), builder.Digest());
    }

    [Fact]
    public void Reset_Forgets_Everything()
    {
        byte[] data = Payload(300);
        XxHash64Builder builder = XxHash64Builder.Create();
        builder.Append(data);
        builder.Reset();
        Assert.Equal(0UL, builder.Length);
        builder.Append(data.AsSpan(0, 50));
        Assert.Equal(XxHash64.Hash(data.AsSpan(0, 50)), builder.Digest());
    }

    private static byte[] Payload(int length)
    {
        byte[] bytes = new byte[length];
        new Random(length + 1).NextBytes(bytes);
        return bytes;
    }
}
