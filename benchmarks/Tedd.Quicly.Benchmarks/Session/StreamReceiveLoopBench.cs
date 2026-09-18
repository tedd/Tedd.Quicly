using System.Runtime.CompilerServices;
using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Archive.Session;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Framing;

namespace Tedd.Quicly.Benchmarks.Session;

/// <summary>
/// ADR 0007 loop on the ordered receive path: the peer's engine-stream loop must be able to un-read any message event when
/// an engine pends. V0 (archived) copies the whole <see cref="StreamFrameParser"/> before every event; V1 (archived) takes a
/// <see cref="StreamFrameParser.Mark"/> of the state a message event changes and hands every message over as three events;
/// V2 (current) hands a message whose payload lies wholly in the segment over as one event (<c>StreamMessagePhase.Whole</c>,
/// via the parser's internal <c>TryTakeWholePayload</c>). Workload: 1 000 frames of 64 bytes on an unkeyed, uncompressed
/// ordered channel, received in 1 200-byte segments (three events per message in V0 and V1; one for most messages in V2, three
/// for the few that straddle a segment boundary). Per message.
/// </summary>
[Config(typeof(InProcessShortRunConfig))]
public class StreamReceiveLoopBench
{
    private const int Messages = 1_000;
    private const int SegmentSize = 1_200;

    private static readonly ChannelTable Table = ChannelTable.Create().Add(4, "ordered", ChannelMode.ReliableOrdered).Build();

    private byte[] _stream = [];

    [GlobalSetup]
    public void Setup()
    {
        ChannelDefinition channel = Table[4]!;
        List<byte> data = [];
        byte[] scratch = new byte[64];
        int written = StreamFraming.WritePreamble(scratch, 4);
        data.AddRange(scratch.AsSpan(0, written).ToArray());
        StreamMessageHeader header = new() { Length = 64 };
        written = StreamFraming.WriteFrameHeader(scratch, channel, in header);
        for (int m = 0; m < Messages; m++)
        {
            data.AddRange(scratch.AsSpan(0, written).ToArray());
            for (int i = 0; i < 64; i++)
            {
                data.Add((byte)(m + i));
            }
        }

        _stream = [.. data];
        long v0 = V0_CopyParserPerEvent();
        if (v0 != V1_MarkPerEvent() || v0 != V2_WholeMessage())
        {
            throw new InvalidOperationException("The loops disagree.");
        }
    }

    [Benchmark(Baseline = true, OperationsPerInvoke = Messages)]
    public long V0_CopyParserPerEvent()
    {
        StreamFrameParser parser = default;
        parser.Reset(StreamRole.Unknown);
        int events = 0;
        long sum = 0;
        for (int offset = 0; offset < _stream.Length; offset += SegmentSize)
        {
            ReadOnlySpan<byte> segment = _stream.AsSpan(offset, Math.Min(SegmentSize, _stream.Length - offset));
            sum += StreamReceiveLoopV0.Run(ref parser, Table, segment, -1, ref events);
        }

        return sum;
    }

    [Benchmark(OperationsPerInvoke = Messages)]
    public long V1_MarkPerEvent()
    {
        StreamFrameParser parser = default;
        parser.Reset(StreamRole.Unknown);
        int events = 0;
        long sum = 0;
        for (int offset = 0; offset < _stream.Length; offset += SegmentSize)
        {
            ReadOnlySpan<byte> segment = _stream.AsSpan(offset, Math.Min(SegmentSize, _stream.Length - offset));
            sum += StreamReceiveLoopV1.Run(ref parser, Table, segment, -1, ref events);
        }

        return sum;
    }

    [Benchmark(OperationsPerInvoke = Messages)]
    public long V2_WholeMessage()
    {
        StreamFrameParser parser = default;
        parser.Reset(StreamRole.Unknown);
        int events = 0;
        long sum = 0;
        for (int offset = 0; offset < _stream.Length; offset += SegmentSize)
        {
            ReadOnlySpan<byte> segment = _stream.AsSpan(offset, Math.Min(SegmentSize, _stream.Length - offset));
            sum += RunV2(ref parser, Table, segment, -1, ref events);
        }

        return sum;
    }

    /// <summary>
    /// The current loop's shape (QuiclyPeer.ReceiveEngineStream): a mark per event, a whole copy only on bulk streams, and a
    /// message whose payload lies wholly in the segment taken in one event. The consumer adds what the three events it replaces
    /// would have added, so all versions produce the same checksum.
    /// </summary>
    private static long RunV2(ref StreamFrameParser parser, ChannelTable table, ReadOnlySpan<byte> segment, int pendAt, ref int events)
    {
        ReadOnlySpan<byte> input = segment;
        StreamFrameParser snapshot = default;
        long sum = 0;
        while (true)
        {
            bool copy = parser.Role == StreamRole.Bulk;
            parser.GetMark(out StreamFrameParser.Mark mark);
            if (copy)
            {
                snapshot = parser;
            }

            StreamEvent streamEvent = parser.Read(table, ref input, out ReadOnlySpan<byte> payload);
            if (streamEvent is StreamEvent.NeedMore or StreamEvent.Error)
            {
                return sum;
            }

            if (streamEvent == StreamEvent.Preamble)
            {
                continue;
            }

            bool whole = streamEvent == StreamEvent.MessageStart && TryTakeWholePayload(ref parser, ref input, out payload);
            if (events++ == pendAt)
            {
                if (copy)
                {
                    parser = snapshot;
                }
                else
                {
                    parser.Rewind(in mark);
                }

                return -1;
            }

            int length = parser.Message.Length;
            sum += whole
                ? (long)StreamEvent.MessageStart + (long)StreamEvent.PayloadChunk + (long)StreamEvent.MessageEnd + (3L * length) + payload.Length
                : (long)streamEvent + length + payload.Length;
        }
    }

    [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "TryTakeWholePayload")]
    private static extern bool TryTakeWholePayload(ref StreamFrameParser parser, scoped ref ReadOnlySpan<byte> input, out ReadOnlySpan<byte> payload);
}
