using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Primitives;
using static Tedd.Quicly.Core.Tests.Control.ControlTestData;

namespace Tedd.Quicly.Core.Tests.Control;

public class LatestBatchTests
{
    private static LatestAckEntry[] AckEntries(int count, int seed)
    {
        Random random = new(seed);
        var entries = new LatestAckEntry[count];
        for (int i = 0; i < count; i++)
        {
            ushort channel = (ushort)random.Next(2, 16384);
            ulong key = (ulong)random.NextInt64(0, long.MaxValue) >> random.Next(2, 64);
            entries[i] = new LatestAckEntry(channel, key, (uint)random.Next());
        }

        return entries;
    }

    private static List<LatestAckEntry> ReadAcks(byte[] frame, ControlCarrier carrier)
    {
        ReadOnlySpan<byte> body = carrier == ControlCarrier.Datagram ? DatagramBody(frame) : StreamBody(frame);
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(body, out LatestAckBatchReader reader));
        List<LatestAckEntry> list = [];
        foreach (LatestAckEntry entry in reader)
        {
            list.Add(entry);
        }

        Assert.Equal(reader.Count, list.Count);
        return list;
    }

    [Theory]
    [InlineData(1, ControlCarrier.Datagram)]
    [InlineData(32, ControlCarrier.Datagram)]
    [InlineData(63, ControlCarrier.Datagram)]
    [InlineData(64, ControlCarrier.Datagram)]
    [InlineData(200, ControlCarrier.Datagram)]
    [InlineData(1, ControlCarrier.Stream)]
    [InlineData(9, ControlCarrier.Stream)]
    [InlineData(64, ControlCarrier.Stream)]
    [InlineData(1000, ControlCarrier.Stream)]
    public void Ack_Batch_Round_Trips(int count, ControlCarrier carrier)
    {
        LatestAckEntry[] entries = AckEntries(count, count);
        byte[] frame = EncodeAcks(carrier, entries);
        Assert.Equal(entries, ReadAcks(frame, carrier));
    }

    [Fact]
    public void Reject_Batch_Round_Trips()
    {
        foreach (ControlCarrier carrier in new[] { ControlCarrier.Datagram, ControlCarrier.Stream })
        {
            LatestRejectEntry[] entries = new LatestRejectEntry[70];
            for (int i = 0; i < entries.Length; i++)
            {
                entries[i] = new LatestRejectEntry((ushort)(2 + i * 200), (ulong)i << (i % 50), (uint)(i * 7919), (LatestRejectReason)(1 + (i % 4)));
            }

            byte[] frame = EncodeRejects(carrier, entries);
            ReadOnlySpan<byte> body = carrier == ControlCarrier.Datagram ? DatagramBody(frame) : StreamBody(frame);
            Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(body, out LatestRejectBatchReader reader));
            Assert.Equal(entries.Length, reader.Count);
            int i2 = 0;
            foreach (LatestRejectEntry entry in reader)
            {
                Assert.Equal(entries[i2++], entry);
            }

            Assert.Equal(entries.Length, i2);
        }
    }

    [Fact]
    public void Ack_Batch_Exact_Encoding()
    {
        byte[] frame = EncodeAcks(ControlCarrier.Datagram, new LatestAckEntry(2, 5, 0x01020304), new LatestAckEntry(64, 64, 1));
        Assert.Equal(Cat([0x00, 0x03], V(2), V(2), V(5), U32(0x01020304), V(64), V(64), U32(1)), frame);

        byte[] stream = EncodeAcks(ControlCarrier.Stream, new LatestAckEntry(2, 5, 7));
        Assert.Equal(Cat([0x08, 0x03], V(1), V(2), V(5), U32(7)), stream);
    }

    [Fact]
    public void Writer_Fills_A_Datagram_And_Stops()
    {
        byte[] datagram = new byte[1200];
        LatestAckBatchWriter writer = new(datagram, ControlCarrier.Datagram);
        LatestAckEntry largest = new(16383, VarInt.MaxValue, uint.MaxValue); // 2 + 8 + 4 = 14 bytes
        int added = 0;
        while (writer.TryAdd(largest))
        {
            added++;
        }

        Assert.Equal(added, writer.Count);
        Assert.False(writer.TryAdd(new LatestAckEntry(2, 0, 0)) && added * 14 + 6 + 2 + 2 > 1200);
        int length = writer.Finish();
        Assert.InRange(length, 1200 - 14 - 6, 1200);
        byte[] frame = datagram.AsSpan(0, length).ToArray();
        List<LatestAckEntry> read = ReadAcks(frame, ControlCarrier.Datagram);
        Assert.Equal(writer.Count, read.Count);
        Assert.All(read.Take(added), e => Assert.Equal(largest, e));
    }

    [Fact]
    public void Writer_Respects_Max_Entries()
    {
        byte[] buffer = new byte[256];
        LatestAckBatchWriter writer = new(buffer, ControlCarrier.Datagram, maxEntries: 3);
        for (int i = 0; i < 3; i++)
        {
            Assert.True(writer.TryAdd(new LatestAckEntry(2, (ulong)i, 1)));
        }

        Assert.False(writer.TryAdd(new LatestAckEntry(2, 9, 1)));
        Assert.Equal(3, writer.Count);
        byte[] frame = buffer.AsSpan(0, writer.Finish()).ToArray();
        Assert.Equal(3, ReadAcks(frame, ControlCarrier.Datagram).Count);

        LatestRejectBatchWriter rejects = new(buffer, ControlCarrier.Stream, maxEntries: 1);
        Assert.True(rejects.TryAdd(new LatestRejectEntry(2, 1, 1, LatestRejectReason.TooLarge)));
        Assert.False(rejects.TryAdd(new LatestRejectEntry(2, 2, 1, LatestRejectReason.TooLarge)));
        Assert.Equal(1, rejects.Count);
        Assert.Equal(2 + 1 + 1 + 1 + 4 + 1, rejects.Finish());
    }

    [Fact]
    public void Stream_Writer_Stops_At_The_Frame_Limit()
    {
        byte[] buffer = new byte[40000];
        LatestAckBatchWriter writer = new(buffer, ControlCarrier.Stream);
        LatestAckEntry largest = new(16383, VarInt.MaxValue, 1);
        while (writer.TryAdd(largest))
        {
        }

        int length = writer.Finish();
        Assert.InRange(length, ControlCodec.MaxEncodedStreamFrameLength - 14, ControlCodec.MaxEncodedStreamFrameLength);
        byte[] frame = buffer.AsSpan(0, length).ToArray();
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryReadStream(frame, out _, out _, out int consumed));
        Assert.Equal(length, consumed);
        Assert.Equal(writer.Count, ReadAcks(frame, ControlCarrier.Stream).Count);

        // Small entries: the count needs a 2-byte varint; the frame length ends at 16383, a 2-byte varint, so Finish
        // moves the entries down from the 4-byte length reservation.
        writer = new LatestAckBatchWriter(buffer, ControlCarrier.Stream);
        while (writer.TryAdd(new LatestAckEntry(2, 0, 0)))
        {
        }

        Assert.Equal((16384 - 1 - 2) / 6, writer.Count);
        frame = buffer.AsSpan(0, writer.Finish()).ToArray();
        Assert.Equal(16383, 1 + 2 + (writer.Count * 6));
        Assert.Equal(2 + 16383, frame.Length);
        Assert.Equal(writer.Count, ReadAcks(frame, ControlCarrier.Stream).Count);
    }

    [Fact]
    public void Writer_Too_Small_Destination_Produces_Nothing()
    {
        foreach (int size in new[] { 0, 1, 2, 8 })
        {
            LatestAckBatchWriter writer = new(new byte[size], ControlCarrier.Datagram);
            Assert.False(writer.TryAdd(new LatestAckEntry(2, 0, 0)) && size < 9);
            if (size < 9)
            {
                Assert.Equal(0, writer.Count);
                Assert.Equal(0, writer.Finish());
            }
        }

        LatestAckBatchWriter exact = new(new byte[9], ControlCarrier.Datagram);
        Assert.True(exact.TryAdd(new LatestAckEntry(2, 0, 0)));
        Assert.Equal(9, exact.Finish());
    }

    [Fact]
    public void Writer_Misuse_Throws()
    {
        byte[] buffer = new byte[64];
        Assert.Throws<ArgumentOutOfRangeException>(() => new LatestAckBatchWriter(buffer, (ControlCarrier)5));
        Assert.Throws<ArgumentOutOfRangeException>(() => new LatestRejectBatchWriter(buffer, ControlCarrier.Datagram, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new LatestAckBatchWriter(buffer, ControlCarrier.Datagram).TryAdd(new LatestAckEntry(1, 0, 0)));
        Assert.Throws<ArgumentOutOfRangeException>(() => new LatestAckBatchWriter(buffer, ControlCarrier.Datagram).TryAdd(new LatestAckEntry(16384, 0, 0)));
        Assert.Throws<ArgumentOutOfRangeException>(() => new LatestAckBatchWriter(buffer, ControlCarrier.Datagram).TryAdd(new LatestAckEntry(2, VarInt.MaxValue + 1, 0)));
        Assert.Throws<ArgumentOutOfRangeException>(() => new LatestRejectBatchWriter(buffer, ControlCarrier.Datagram).TryAdd(new LatestRejectEntry(2, 0, 0, 0)));
        Assert.Throws<ArgumentOutOfRangeException>(() => new LatestRejectBatchWriter(buffer, ControlCarrier.Datagram).TryAdd(new LatestRejectEntry(2, 0, 0, (LatestRejectReason)5)));

        Assert.Throws<InvalidOperationException>(() =>
        {
            LatestAckBatchWriter writer = new(buffer, ControlCarrier.Datagram);
            writer.TryAdd(new LatestAckEntry(2, 0, 0));
            writer.Finish();
            writer.TryAdd(new LatestAckEntry(2, 0, 0));
        });
        Assert.Throws<InvalidOperationException>(() =>
        {
            LatestRejectBatchWriter writer = new(buffer, ControlCarrier.Datagram);
            writer.Finish();
            writer.Finish();
        });
    }

    [Fact]
    public void Default_Writer_And_Reader_Are_Empty()
    {
        LatestAckBatchWriter writer = default;
        Assert.False(writer.TryAdd(new LatestAckEntry(2, 0, 0)));
        Assert.Equal(0, writer.Finish());

        LatestAckBatchReader reader = default;
        Assert.Equal(0, reader.Count);
        Assert.False(reader.MoveNext());
        LatestRejectBatchReader rejects = default;
        Assert.Equal(0, rejects.Count);
        Assert.False(rejects.MoveNext());
    }

    [Fact]
    public void Reader_Enumerates_Again_From_A_Copy_And_Stops_At_End()
    {
        byte[] frame = EncodeAcks(ControlCarrier.Datagram, AckEntries(5, 3));
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(DatagramBody(frame), out LatestAckBatchReader reader));
        int first = 0, second = 0;
        foreach (LatestAckEntry _ in reader)
        {
            first++;
        }

        foreach (LatestAckEntry _ in reader)
        {
            second++;
        }

        Assert.Equal(5, first);
        Assert.Equal(5, second);
        while (reader.MoveNext())
        {
        }

        Assert.False(reader.MoveNext());
        Assert.Equal(5, reader.Count);
    }

    [Fact]
    public void Empty_Batch_Parses()
    {
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse([0x00], out LatestAckBatchReader acks));
        Assert.Equal(0, acks.Count);
        Assert.False(acks.MoveNext());
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse([0x00], out LatestRejectBatchReader rejects));
        Assert.Equal(0, rejects.Count);
    }

    public static TheoryData<byte[], ControlParseStatus> MalformedAckBodies => new()
    {
        { [], ControlParseStatus.Truncated },
        { [0x40], ControlParseStatus.Truncated },
        { Cat(VWide(1, 2), V(2), V(0), U32(0)), ControlParseStatus.NonMinimalVarint },
        { V(1), ControlParseStatus.CountTooLarge },
        { Cat(V(2), V(2), V(0), U32(0)), ControlParseStatus.CountTooLarge },
        { Cat(V(VarInt.MaxValue), V(2), V(0), U32(0)), ControlParseStatus.CountTooLarge },
        { Cat(V(1), V(2), V(0), U32(0), [0]), ControlParseStatus.TrailingBytes },
        { Cat(V(1), V(2), V(0), [0, 0, 0], [0, 0]), ControlParseStatus.TrailingBytes },
        // Mixed: first entry valid, second has a bad channel / non-minimal key / truncated version.
        { Cat(V(2), V(2), V(0), U32(0), V(1), V(0), U32(0)), ControlParseStatus.InvalidChannel },
        { Cat(V(2), V(2), V(0), U32(0), V(16384), V(0), U32(0)), ControlParseStatus.InvalidChannel },
        { Cat(V(2), V(2), V(0), U32(0), V(2), VWide(9, 2), U32(0)), ControlParseStatus.NonMinimalVarint },
        { Cat(V(2), V(2), V(0), U32(0), V(2), V(64), [1, 2, 3]), ControlParseStatus.Truncated },
        { Cat(V(2), V(2), V(0), U32(0), V(2), [0xC0, 0, 0, 0, 0]), ControlParseStatus.Truncated },
        { Cat(V(2), V(2), V(0), U32(0), V(2), [0x80, 0, 0, 0], [0, 0, 0, 0]), ControlParseStatus.NonMinimalVarint },
        { Cat(V(2), V(2), V(0), U32(0), V(2), V(0), [1, 2, 3, 4], [0, 0]), ControlParseStatus.TrailingBytes },
        { Cat(V(2), V(2), V(0), U32(0), VWide(2, 2), V(0), U32(0)), ControlParseStatus.NonMinimalVarint },
    };

    [Theory]
    [MemberData(nameof(MalformedAckBodies))]
    public void Malformed_Ack_Batch_Is_Rejected_As_A_Whole(byte[] body, ControlParseStatus expected)
    {
        Assert.Equal(expected, ControlCodec.TryParse(body, out LatestAckBatchReader reader));
        Assert.Equal(0, reader.Count);
        Assert.False(reader.MoveNext());
    }

    public static TheoryData<byte[], ControlParseStatus> MalformedRejectBodies => new()
    {
        { Cat(V(1), V(2), V(0), U32(0)), ControlParseStatus.CountTooLarge },
        { Cat(V(1), V(2), V(0), U32(0), [0]), ControlParseStatus.InvalidValue },
        { Cat(V(1), V(2), V(0), U32(0), [5]), ControlParseStatus.InvalidValue },
        { Cat(V(2), V(2), V(0), U32(0), [4], V(2), V(0), U32(0), [0xFF]), ControlParseStatus.InvalidValue },
        { Cat(V(1), V(2), V(0), U32(0), [1], [0]), ControlParseStatus.TrailingBytes },
        { Cat(V(2), V(2), V(0), U32(0), [1], V(2), V(64), U32(0)), ControlParseStatus.Truncated },
        { Cat(V(2), V(2), V(0), U32(0), [1], V(1), V(0), U32(0), [1]), ControlParseStatus.InvalidChannel },
    };

    [Theory]
    [MemberData(nameof(MalformedRejectBodies))]
    public void Malformed_Reject_Batch_Is_Rejected_As_A_Whole(byte[] body, ControlParseStatus expected)
    {
        Assert.Equal(expected, ControlCodec.TryParse(body, out LatestRejectBatchReader reader));
        Assert.Equal(0, reader.Count);
        Assert.False(reader.MoveNext());
    }

    [Fact]
    public void Count_Bound_Uses_Minimum_Entry_Size()
    {
        // 3 minimal ack entries = 18 bytes; a count of 3 fits, 4 does not.
        byte[] entries = Cat(V(2), V(0), U32(0), V(2), V(1), U32(0), V(2), V(2), U32(0));
        Assert.Equal(ControlParseStatus.Ok, ControlCodec.TryParse(Cat(V(3), entries), out LatestAckBatchReader reader));
        Assert.Equal(3, reader.Count);
        Assert.Equal(ControlParseStatus.CountTooLarge, ControlCodec.TryParse(Cat(V(4), entries), out reader));
        // As reject entries (7 bytes minimum) 18 bytes hold at most 2.
        Assert.Equal(ControlParseStatus.CountTooLarge, ControlCodec.TryParse(Cat(V(3), entries), out LatestRejectBatchReader _));
    }
}
