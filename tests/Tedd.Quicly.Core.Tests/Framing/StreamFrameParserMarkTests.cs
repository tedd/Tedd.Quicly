using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Framing;
using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Core.Tests.Framing;

/// <summary>
/// <see cref="StreamFrameParser.Mark"/>: un-reading a message event (the session layer's Pend) from a mark taken before
/// <see cref="StreamFrameParser.Read"/> replays exactly the same messages, in any segmentation, headers straddling segments
/// included.
/// </summary>
public class StreamFrameParserMarkTests
{
    private static readonly ChannelTable Table = ChannelTable.Create()
        .Add(4, "ordered", ChannelMode.ReliableOrdered, o =>
        {
            o.Keyed = true;
            o.Compression = ChannelCompression.Lz4;
            o.RequestResponse = true;
        })
        .Build();

    private static readonly int[] Lengths = [0, 1, 50, 300, 7, 0, 1_500];

    private static byte[] BuildStream()
    {
        ChannelDefinition channel = Table[4]!;
        List<byte> data = [];
        byte[] scratch = new byte[64];
        int written = StreamFraming.WritePreamble(scratch, 4);
        data.AddRange(scratch.AsSpan(0, written).ToArray());
        for (int m = 0; m < Lengths.Length; m++)
        {
            StreamMessageHeader header = new() { Length = Lengths[m], Key = (ulong)(m * 1_000_003), RequestId = (uint)m };
            written = StreamFraming.WriteFrameHeader(scratch, channel, in header);
            data.AddRange(scratch.AsSpan(0, written).ToArray());
            for (int i = 0; i < Lengths[m]; i++)
            {
                data.Add((byte)(m + (i * 3)));
            }
        }

        return [.. data];
    }

    /// <summary>
    /// Parses <paramref name="data"/> in segments of <paramref name="segmentSize"/>. With <paramref name="pendEveryEvent"/> every
    /// message event is un-read once (rewound, and the input handed back from the offset it had), as the peer does on Pend.
    /// Returns one line per complete message. With <paramref name="whole"/> a message whose payload lies wholly in the segment
    /// is taken in one step after its <see cref="StreamEvent.MessageStart"/> (<see cref="StreamFrameParser.TryTakeWholePayload"/>,
    /// the peer's fast path), counted in <paramref name="wholes"/>, and un-read as one event. With <paramref name="fast"/> every
    /// event first tries <see cref="StreamFrameParser.TryReadWholeMessage"/> (the peer's whole-frame path, counted in
    /// <paramref name="fastReads"/>) and falls back to <see cref="StreamFrameParser.Read"/>, which must then see an unchanged parser.
    /// </summary>
    private static List<string> Parse(byte[] data, int segmentSize, bool pendEveryEvent, out int pends, bool whole = false) =>
        Parse(data, segmentSize, pendEveryEvent, out pends, whole, fast: false, out _, out _);

    private static List<string> Parse(byte[] data, int segmentSize, bool pendEveryEvent, out int pends, bool whole, bool fast, out int wholes, out int fastReads)
    {
        fastReads = 0;
        StreamFrameParser parser = default;
        parser.Reset(StreamRole.Unknown);
        List<string> messages = [];
        List<byte> payload = [];
        HashSet<int> pended = [];
        int delivered = 0;
        pends = 0;
        wholes = 0;
        int offset = 0;
        bool replay = false;

        // An event un-read after the last byte is handed back as an empty indication (the transport's resume).
        while (offset < data.Length || replay)
        {
            replay = false;
            int end = Math.Min(offset + segmentSize, data.Length);
            ReadOnlySpan<byte> segment = data.AsSpan(offset, end - offset);
            ReadOnlySpan<byte> input = segment;
            int next = end;
            while (true)
            {
                parser.GetMark(out StreamFrameParser.Mark mark);
                int before = segment.Length - input.Length;
                StreamEvent streamEvent;
                ReadOnlySpan<byte> chunk;
                bool taken = false;
                if (fast && parser.TryReadWholeMessage(ref input, out chunk))
                {
                    streamEvent = StreamEvent.MessageStart;
                    taken = true;
                    fastReads++;
                }
                else
                {
                    if (fast)
                    {
                        // Nothing changed: the same bytes go to Read.
                        Assert.Equal(segment.Length - before, input.Length);
                        parser.GetMark(out StreamFrameParser.Mark after);
                        Assert.Equal(mark.Message, after.Message);
                        Assert.Equal(mark.Remaining, after.Remaining);
                        Assert.Equal(mark.State, after.State);
                    }

                    streamEvent = parser.Read(Table, ref input, out chunk);
                }

                if (streamEvent == StreamEvent.NeedMore)
                {
                    break;
                }

                Assert.NotEqual(StreamEvent.Error, streamEvent);
                if (!taken && whole && streamEvent == StreamEvent.MessageStart)
                {
                    int remaining = parser.RemainingPayload;
                    int available = input.Length;
                    taken = parser.TryTakeWholePayload(ref input, out chunk);
                    Assert.Equal(remaining <= available, taken);
                    if (!taken)
                    {
                        // Nothing changed: the message continues event by event.
                        Assert.Equal(remaining, parser.RemainingPayload);
                        Assert.Equal(available, input.Length);
                    }
                }

                bool message = streamEvent is StreamEvent.MessageStart or StreamEvent.PayloadChunk or StreamEvent.MessageEnd;
                if (pendEveryEvent && message && pended.Add(delivered))
                {
                    parser.Rewind(in mark);
                    pends++;
                    next = offset + before;
                    replay = next >= data.Length;
                    break;
                }

                if (message)
                {
                    delivered++;
                }

                if (taken)
                {
                    wholes++;
                    StreamMessageHeader header = parser.Message;
                    Assert.Equal(header.Length, chunk.Length);
                    messages.Add($"{header.Length}/{header.Key}/{header.RequestId}/{Convert.ToHexString(chunk)}");
                    continue;
                }

                switch (streamEvent)
                {
                    case StreamEvent.MessageStart:
                        payload.Clear();
                        break;
                    case StreamEvent.PayloadChunk:
                        payload.AddRange(chunk.ToArray());
                        break;
                    case StreamEvent.MessageEnd:
                        StreamMessageHeader header = parser.Message;
                        messages.Add($"{header.Length}/{header.Key}/{header.RequestId}/{Convert.ToHexString([.. payload])}");
                        break;
                }
            }

            offset = next;
        }

        Assert.Equal(ParseStatus.Ok, parser.Finish());
        return messages;
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(7)]
    [InlineData(64)]
    [InlineData(1_200)]
    [InlineData(100_000)]
    public void Rewinding_Every_Message_Event_Replays_The_Same_Messages(int segmentSize)
    {
        byte[] data = BuildStream();
        List<string> reference = Parse(data, segmentSize, pendEveryEvent: false, out _);
        Assert.Equal(Lengths.Length, reference.Count);
        List<string> replayed = Parse(data, segmentSize, pendEveryEvent: true, out int pends);
        Assert.Equal(reference, replayed);
        Assert.True(pends >= Lengths.Length * 2, $"{pends} events were un-read");
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(7)]
    [InlineData(64)]
    [InlineData(1_200)]
    [InlineData(100_000)]
    public void Whole_Messages_Parse_And_Rewind_Like_Their_Events(int segmentSize)
    {
        byte[] data = BuildStream();
        List<string> reference = Parse(data, segmentSize, pendEveryEvent: false, out _);
        List<string> whole = Parse(data, segmentSize, pendEveryEvent: false, out _, whole: true, fast: false, out int wholes, out _);
        Assert.Equal(reference, whole);
        List<string> replayed = Parse(data, segmentSize, pendEveryEvent: true, out int pends, whole: true, fast: false, out _, out _);
        Assert.Equal(reference, replayed);
        Assert.True(pends >= Lengths.Length, $"{pends} events were un-read");
        if (segmentSize >= data.Length)
        {
            Assert.Equal(Lengths.Length, wholes);
        }
        else if (segmentSize == 1)
        {
            // Only the empty messages fit a one-byte segment whole.
            Assert.Equal(Lengths.Count(l => l == 0), wholes);
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(7)]
    [InlineData(64)]
    [InlineData(1_200)]
    [InlineData(100_000)]
    public void Whole_Frames_Read_Without_The_State_Machine_Parse_And_Rewind_Like_Their_Events(int segmentSize)
    {
        byte[] data = BuildStream();
        List<string> reference = Parse(data, segmentSize, pendEveryEvent: false, out _);
        List<string> fast = Parse(data, segmentSize, pendEveryEvent: false, out _, whole: true, fast: true, out int wholes, out int fastReads);
        Assert.Equal(reference, fast);
        List<string> replayed = Parse(data, segmentSize, pendEveryEvent: true, out int pends, whole: true, fast: true, out _, out _);
        Assert.Equal(reference, replayed);
        Assert.True(pends >= Lengths.Length, $"{pends} events were un-read");
        if (segmentSize >= data.Length)
        {
            // After the preamble every frame is read whole in one step.
            Assert.Equal(Lengths.Length, fastReads);
            Assert.Equal(Lengths.Length, wholes);
        }
        else if (segmentSize < 4)
        {
            // No header of this channel (Length, Key, RequestId, RawLength: at least four bytes) fits such a segment.
            Assert.Equal(0, fastReads);
        }
    }

    [Fact]
    public void A_Whole_Frame_Is_Left_To_Read_When_It_Is_Not_A_Valid_Message_At_A_Boundary()
    {
        ChannelTable table = ChannelTable.Create().Add(6, "latest", ChannelMode.ReliableLatest).Build();
        ChannelDefinition channel = table[6]!;
        byte[] data = new byte[64];
        int length = StreamFraming.WriteGroupPreamble(data, 6, 9);
        int start = length;

        // The value's version does not match the stream's group id (PROTOCOL.md §8 item 8).
        StreamMessageHeader header = new() { Length = 2, Sequence = 8, Key = 5 };
        length += StreamFraming.WriteFrameHeader(data.AsSpan(length), channel, in header);
        length += 2;

        StreamFrameParser parser = default;
        parser.Reset(StreamRole.Unknown);
        ReadOnlySpan<byte> input = data.AsSpan(0, length);
        Assert.False(parser.TryReadWholeMessage(ref input, out _)); // before the preamble
        Assert.Equal(StreamEvent.Preamble, parser.Read(table, ref input, out _));
        Assert.False(parser.TryReadWholeMessage(ref input, out ReadOnlySpan<byte> payload));
        Assert.True(payload.IsEmpty);
        Assert.Equal(length - start, input.Length);
        Assert.Equal(StreamEvent.Error, parser.Read(table, ref input, out _));
        Assert.Equal(ParseStatus.SequenceMismatch, parser.Error);

        // A header cut short is left to Read too, which buffers it.
        parser.Reset(StreamRole.Unknown);
        input = data.AsSpan(0, start + 1);
        Assert.Equal(StreamEvent.Preamble, parser.Read(table, ref input, out _));
        Assert.False(parser.TryReadWholeMessage(ref input, out _));
        Assert.Equal(1, input.Length);
        Assert.Equal(StreamEvent.NeedMore, parser.Read(table, ref input, out _));
    }

    [Fact]
    public void A_Whole_Message_Ends_A_ReliableLatest_Group_Stream()
    {
        ChannelTable table = ChannelTable.Create().Add(6, "latest", ChannelMode.ReliableLatest, o => o.Keyed = true).Build();
        ChannelDefinition channel = table[6]!;
        byte[] data = new byte[64];
        int length = StreamFraming.WriteGroupPreamble(data, 6, 9);
        StreamMessageHeader header = new() { Length = 3, Sequence = 9, Key = 5 };
        length += StreamFraming.WriteFrameHeader(data.AsSpan(length), channel, in header);
        data[length++] = 1;
        data[length++] = 2;
        data[length++] = 3;

        StreamFrameParser parser = default;
        parser.Reset(StreamRole.Unknown);
        ReadOnlySpan<byte> input = data.AsSpan(0, length);
        Assert.Equal(StreamEvent.Preamble, parser.Read(table, ref input, out _));
        Assert.Equal(StreamEvent.MessageStart, parser.Read(table, ref input, out _));
        Assert.True(parser.TryTakeWholePayload(ref input, out ReadOnlySpan<byte> payload));
        Assert.Equal(new byte[] { 1, 2, 3 }, payload.ToArray());
        Assert.True(input.IsEmpty);
        Assert.Equal(ParseStatus.Ok, parser.Finish());
        Assert.Equal(StreamEvent.NeedMore, parser.Read(table, ref input, out _));

        // The value is the stream's only message: anything after it is still an error.
        ReadOnlySpan<byte> extra = [0];
        Assert.Equal(StreamEvent.Error, parser.Read(table, ref extra, out _));
    }

    [Fact]
    public void A_Whole_Message_Is_Not_Taken_Outside_A_Message_Start()
    {
        byte[] data = BuildStream();
        StreamFrameParser parser = default;
        parser.Reset(StreamRole.Unknown);
        ReadOnlySpan<byte> input = data;
        Assert.False(parser.TryTakeWholePayload(ref input, out _));
        Assert.Equal(StreamEvent.Preamble, parser.Read(Table, ref input, out _));
        int before = input.Length;
        Assert.False(parser.TryTakeWholePayload(ref input, out ReadOnlySpan<byte> payload));
        Assert.True(payload.IsEmpty);
        Assert.Equal(before, input.Length);
    }

    [Fact]
    public void A_Mark_Restores_A_Header_That_Straddled_Segments()
    {
        byte[] data = BuildStream();
        StreamFrameParser parser = default;
        parser.Reset(StreamRole.Unknown);
        ReadOnlySpan<byte> input = data.AsSpan(0, 1);
        Assert.Equal(StreamEvent.Preamble, parser.Read(Table, ref input, out _));

        // Feed the first message's header one byte at a time: the parser buffers it until the last byte completes it.
        int offset = 1;
        StreamEvent last = StreamEvent.NeedMore;
        StreamFrameParser.Mark mark = default;
        while (last == StreamEvent.NeedMore)
        {
            parser.GetMark(out mark);
            input = data.AsSpan(offset, 1);
            last = parser.Read(Table, ref input, out _);
            offset++;
        }

        Assert.Equal(StreamEvent.MessageStart, last);
        StreamMessageHeader first = parser.Message;

        // Un-read it and hand the last byte back: the buffered bytes come back with the mark.
        parser.Rewind(in mark);
        input = data.AsSpan(offset - 1, 1);
        Assert.Equal(StreamEvent.MessageStart, parser.Read(Table, ref input, out _));
        Assert.Equal(first, parser.Message);
    }
}
