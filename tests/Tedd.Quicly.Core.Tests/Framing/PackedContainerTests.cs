using Tedd.Quicly.Core.Framing;
using static Tedd.Quicly.Core.Tests.Framing.TestTables;

namespace Tedd.Quicly.Core.Tests.Framing;

public class PackedContainerTests
{
    [Theory]
    [InlineData(0u, false, "0100")]
    [InlineData(123u, false, "0100")]
    [InlineData(5u, true, "010105")]
    [InlineData(1000u, true, "010143E8")]
    [InlineData(uint.MaxValue, true, "0101C0000000FFFFFFFF")]
    public void WriteHeader_Vectors(uint tick, bool hasTick, string hex)
    {
        byte[] buffer = new byte[16];
        int n = PackedContainer.WriteHeader(buffer, tick, hasTick);
        Assert.Equal(hex, Bytes.ToHex(buffer.AsSpan(0, n)));
        Assert.Equal(n, PackedContainer.GetHeaderLength(tick, hasTick));
    }

    [Fact]
    public void WriteHeader_Rejects_Small_Destination()
    {
        Assert.Throws<ArgumentException>(() => PackedContainer.WriteHeader(new byte[1], 0, false));
        Assert.Throws<ArgumentException>(() => PackedContainer.WriteHeader(new byte[3], 1000, true));
        Assert.Throws<ArgumentException>(() => new PackedContainerWriter(new byte[1]));
    }

    [Theory]
    [InlineData(1, 2)]
    [InlineData(63, 64)]
    [InlineData(64, 66)]
    [InlineData(1200, 1202)]
    [InlineData(16384, 16388)]
    public void Entry_Length(int messageLength, int expected) => Assert.Equal((long)expected, PackedContainer.GetEntryLength(messageLength));

    [Fact]
    public void Writer_And_Reader_Round_Trip()
    {
        byte[] buffer = new byte[1200];
        PackedContainerWriter writer = new(buffer, 42, hasTick: true);
        Assert.Equal(3, writer.Length);
        Assert.Equal(0, writer.Count);
        Assert.Equal(1197, writer.Remaining);

        byte[] m1 = { 0x02, 0xAA, 0xBB };
        byte[] m2 = { 0x00, 0x01, 0x10, 0x20, 0x30, 0x40 };
        byte[] m3 = Bytes.Concat(new byte[] { 0x03, 0x01, 0x00 }, Bytes.Fill(100));
        Assert.True(writer.TryAppend(m1));
        Assert.True(writer.TryAppend(m2));
        Assert.True(writer.TryReserve(m3.Length, out Span<byte> slot));
        Assert.Equal(m3.Length, slot.Length);
        m3.CopyTo(slot);
        Assert.Equal(3, writer.Count);
        Assert.Equal(3 + 4 + 7 + 2 + m3.Length, writer.Length);
        Assert.Equal(1200 - writer.Length, writer.Remaining);

        byte[] container = writer.Written.ToArray();
        Assert.Equal(ParseStatus.Ok, PackedContainer.TryParse(container, out PackedContainerReader reader));
        Assert.Equal(3, reader.Count);
        Assert.True(reader.HasTick);
        Assert.Equal(42u, reader.Tick);
        List<string> messages = new();
        foreach (ReadOnlySpan<byte> m in reader)
        {
            messages.Add(Bytes.ToHex(m));
        }

        Assert.Equal(new[] { Bytes.ToHex(m1), Bytes.ToHex(m2), Bytes.ToHex(m3) }, messages);

        // Inner messages parse as datagrams.
        Assert.Equal(ParseStatus.Ok, PackedContainer.TryParse(container, out reader));
        Assert.True(reader.MoveNext());
        Assert.Equal(ParseStatus.Ok, DatagramFraming.TryParse(reader.Current, All, out _, out int offset));
        Assert.Equal(1, offset);
        Assert.True(reader.MoveNext());
        Assert.Equal(ParseStatus.ControlChannel, DatagramFraming.TryParse(reader.Current, All, out _, out _));
        Assert.True(reader.MoveNext());
        Assert.False(reader.MoveNext());
        Assert.True(reader.Current.IsEmpty);
        Assert.False(reader.MoveNext());
    }

    [Fact]
    public void Writer_Without_Tick()
    {
        byte[] buffer = new byte[10];
        PackedContainerWriter writer = new(buffer);
        Assert.True(writer.TryAppend(new byte[] { 0x02 }));
        Assert.Equal("01000102", Bytes.ToHex(writer.Written));
        Assert.Equal(ParseStatus.Ok, PackedContainer.TryParse(writer.Written, out PackedContainerReader reader));
        Assert.False(reader.HasTick);
        Assert.Equal(0u, reader.Tick);
        Assert.Equal(1, reader.Count);
    }

    [Fact]
    public void Writer_Refuses_When_Full_By_Space()
    {
        byte[] buffer = new byte[2 + 5];
        PackedContainerWriter writer = new(buffer);
        Assert.True(writer.CanAppend(4));
        Assert.False(writer.CanAppend(5));
        Assert.False(writer.CanAppend(0));
        Assert.False(writer.CanAppend(-1));
        Assert.False(writer.TryAppend(new byte[] { 0x02, 1, 2, 3, 4 }));
        Assert.Equal(0, writer.Count);
        Assert.False(writer.TryReserve(5, out Span<byte> slot));
        Assert.True(slot.IsEmpty);
        Assert.True(writer.TryAppend(new byte[] { 0x02, 1, 2, 3 }));
        Assert.Equal(0, writer.Remaining);
        Assert.False(writer.TryAppend(new byte[] { 0x02 }));
    }

    [Fact]
    public void Writer_Refuses_The_65th_Message()
    {
        byte[] buffer = new byte[1000];
        PackedContainerWriter writer = new(buffer);
        for (int i = 0; i < PackedContainer.MaxMessages; i++)
        {
            Assert.True(writer.TryAppend(new byte[] { 0x02 }));
        }

        Assert.False(writer.CanAppend(1));
        Assert.False(writer.TryAppend(new byte[] { 0x02 }));
        Assert.Equal(64, writer.Count);
        Assert.Equal(ParseStatus.Ok, PackedContainer.TryParse(writer.Written, out PackedContainerReader reader));
        Assert.Equal(64, reader.Count);
    }

    [Fact]
    public void Writer_Rejects_Invalid_Messages()
    {
        byte[] buffer = new byte[100];
        Assert.Throws<ArgumentException>(() => new PackedContainerWriter(buffer).TryAppend(ReadOnlySpan<byte>.Empty));
        Assert.Throws<ArgumentException>(() => new PackedContainerWriter(buffer).TryAppend(new byte[] { 0x01, 0x00, 0x02 }));
        Assert.Throws<ArgumentOutOfRangeException>(() => new PackedContainerWriter(buffer).TryReserve(0, out _));
    }

    [Fact]
    public void Default_Reader_Is_Empty()
    {
        PackedContainerReader reader = default;
        Assert.False(reader.MoveNext());
        Assert.Equal(0, reader.Count);
    }

    public static TheoryData<string, ParseStatus> Statuses => new()
    {
        { "", ParseStatus.Truncated },
        { "00", ParseStatus.NotContainer },
        { "02 00", ParseStatus.NotContainer },
        { "4001 00 0102", ParseStatus.NotContainer },
        { "01", ParseStatus.Truncated },
        { "01 02 0102", ParseStatus.BadFlags },
        { "01 80 0102", ParseStatus.BadFlags },
        { "01 01", ParseStatus.Truncated },
        { "01 01 40", ParseStatus.Truncated },
        { "01 01 4005 0102", ParseStatus.NonMinimalVarint },
        { "01 01 C000000100000000 0102", ParseStatus.ValueOutOfRange },
        { "01 01 C0000000FFFFFFFF 0102", ParseStatus.Ok },
        { "01 00", ParseStatus.ContainerEmpty },
        { "01 01 05", ParseStatus.ContainerEmpty },
        { "01 00 00", ParseStatus.BadLength },
        { "01 00 00 02", ParseStatus.BadLength },
        { "01 00 03 0203", ParseStatus.BadLength },
        { "01 00 40", ParseStatus.Truncated },
        { "01 00 4001 02", ParseStatus.NonMinimalVarint },
        { "01 00 02 0100", ParseStatus.NestedContainer },
        { "01 00 01 02 01 01", ParseStatus.NestedContainer },
        { "01 00 01 02 00", ParseStatus.BadLength },            // trailing zero byte
        { "01 00 01 02 05 AA", ParseStatus.BadLength },         // trailing garbage longer than the rest
        { "01 00 01 02 40", ParseStatus.Truncated },            // trailing partial varint
        { "01 00 02 4001", ParseStatus.Ok },                    // inner non-minimal channel: the inner parser rejects it
        { "01 00 01 00", ParseStatus.Ok },
    };

    [Theory]
    [MemberData(nameof(Statuses))]
    public void Every_Parse_Status(string hex, ParseStatus expected)
    {
        Assert.Equal(expected, PackedContainer.TryParse(Bytes.Hex(hex), out PackedContainerReader reader));
        if (expected != ParseStatus.Ok)
        {
            Assert.Equal(0, reader.Count);
            Assert.False(reader.MoveNext());
        }
    }

    [Fact]
    public void More_Than_64_Messages_Is_Rejected()
    {
        List<byte> bytes = new() { 0x01, 0x00 };
        for (int i = 0; i < 65; i++)
        {
            bytes.Add(0x01);
            bytes.Add(0x02);
        }

        Assert.Equal(ParseStatus.TooManyMessages, PackedContainer.TryParse(bytes.ToArray(), out _));
        bytes.RemoveRange(bytes.Count - 2, 2);
        Assert.Equal(ParseStatus.Ok, PackedContainer.TryParse(bytes.ToArray(), out PackedContainerReader reader));
        Assert.Equal(64, reader.Count);
    }

    [Fact]
    public void Fuzz_Random_And_Mutated_Containers_Never_Throw()
    {
        Random random = new(5150);
        byte[] valid;
        {
            byte[] buffer = new byte[600];
            PackedContainerWriter w = new(buffer, 7, true);
            for (int i = 0; i < 20; i++)
            {
                w.TryAppend(Bytes.Concat(new byte[] { 0x04 }, Bytes.Fill(i + 5)));
            }

            valid = w.Written.ToArray();
        }

        for (int i = 0; i < 50_000; i++)
        {
            byte[] input;
            if (i % 2 == 0)
            {
                input = new byte[random.Next(0, 64)];
                random.NextBytes(input);
                if (input.Length > 1)
                {
                    input[0] = 0x01;
                    input[1] = (byte)random.Next(0, 3);
                }
            }
            else
            {
                input = (byte[])valid.Clone();
                input[random.Next(input.Length)] = (byte)random.Next(256);
                if (random.Next(3) == 0)
                {
                    input = input[..random.Next(input.Length)];
                }
            }

            ParseStatus status = PackedContainer.TryParse(input, out PackedContainerReader reader);
            if (status == ParseStatus.Ok)
            {
                int count = 0;
                long total = 0;
                foreach (ReadOnlySpan<byte> m in reader)
                {
                    Assert.False(m.IsEmpty);
                    Assert.NotEqual(0x01, m[0]);
                    total += PackedContainer.GetEntryLength(m.Length);
                    count++;
                }

                Assert.Equal(reader.Count, count);
                Assert.InRange(count, 1, 64);
                Assert.Equal(input.Length, total + PackedContainer.GetHeaderLength(reader.Tick, reader.HasTick));
            }
        }
    }
}
