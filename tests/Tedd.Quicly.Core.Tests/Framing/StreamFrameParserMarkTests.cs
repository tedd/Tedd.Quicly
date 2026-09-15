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
    /// Returns one line per complete message.
    /// </summary>
    private static List<string> Parse(byte[] data, int segmentSize, bool pendEveryEvent, out int pends)
    {
        StreamFrameParser parser = default;
        parser.Reset(StreamRole.Unknown);
        List<string> messages = [];
        List<byte> payload = [];
        HashSet<int> pended = [];
        int delivered = 0;
        pends = 0;
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
                StreamFrameParser.Mark mark = parser.GetMark();
                int before = segment.Length - input.Length;
                StreamEvent streamEvent = parser.Read(Table, ref input, out ReadOnlySpan<byte> chunk);
                if (streamEvent == StreamEvent.NeedMore)
                {
                    break;
                }

                Assert.NotEqual(StreamEvent.Error, streamEvent);
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
            mark = parser.GetMark();
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
