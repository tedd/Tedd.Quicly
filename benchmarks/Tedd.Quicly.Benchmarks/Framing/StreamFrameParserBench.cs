using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Framing;

namespace Tedd.Quicly.Benchmarks.Framing;

/// <summary>
/// <see cref="StreamFrameParser"/> throughput over ≈ 1 MiB of a ReliableOrdered stream carrying 40-byte messages
/// (1-byte length prefix, 25 575 messages), fed as one span or as receive segments of <see cref="SegmentSize"/> bytes
/// (1 350 ≈ one QUIC packet of stream data: headers straddle segment boundaries regularly).
/// </summary>
[Config(typeof(InProcessShortRunConfig))]
public class StreamFrameParserBench
{
    private const int PayloadLength = 40;

    private byte[] _stream = Array.Empty<byte>();
    private StreamFrameParser _parser;

    [Params(1 << 20, 16384, 1350)]
    public int SegmentSize { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        ChannelDefinition channel = FramingBenchData.Table[FramingBenchData.Ordered]!;
        int messages = (1 << 20) / (PayloadLength + 1);
        _stream = new byte[1 + (messages * (PayloadLength + 1))];
        int pos = StreamFraming.WritePreamble(_stream, channel.Id);
        StreamMessageHeader header = new() { Length = PayloadLength };
        for (int i = 0; i < messages; i++)
        {
            pos += StreamFraming.WriteFrameHeader(_stream.AsSpan(pos), channel, header);
            _stream.AsSpan(pos, PayloadLength).Fill((byte)i);
            pos += PayloadLength;
        }
    }

    [Benchmark]
    public long Parse()
    {
        _parser.Reset();
        long payload = 0;
        long messages = 0;
        byte[] stream = _stream;
        int segment = SegmentSize;
        for (int offset = 0; offset < stream.Length; offset += segment)
        {
            ReadOnlySpan<byte> input = stream.AsSpan(offset, Math.Min(segment, stream.Length - offset));
            while (true)
            {
                StreamEvent ev = _parser.Read(FramingBenchData.Table, ref input, out ReadOnlySpan<byte> chunk);
                if (ev == StreamEvent.NeedMore)
                {
                    break;
                }

                if (ev == StreamEvent.PayloadChunk)
                {
                    payload += chunk.Length;
                }
                else if (ev == StreamEvent.MessageEnd)
                {
                    messages++;
                }
            }
        }

        return payload + messages;
    }
}
