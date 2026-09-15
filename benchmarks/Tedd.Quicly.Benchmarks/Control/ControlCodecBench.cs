using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Archive.Control;
using Tedd.Quicly.Core.Control;

namespace Tedd.Quicly.Benchmarks.Control;

/// <summary>
/// Control-protocol codec hot paths: a Ping/Pong exchange (encode + frame read + parse, both messages) and a
/// 32-entry LatestAck datagram (the shape the ack coalescer sends every AckDelay per peer).
/// </summary>
[Config(typeof(InProcessShortRunConfig))]
public class ControlCodecBench
{
    private const int AckEntries = 32;

    private readonly byte[] _buffer = new byte[1200];
    private LatestAckEntry[] _entries = [];
    private byte[] _ackFrame = [];

    [GlobalSetup]
    public void Setup()
    {
        // Game-like: a handful of channels (1-byte ids), keys spread over 1-, 2- and 4-byte varints, random versions.
        Random random = new(17);
        _entries = new LatestAckEntry[AckEntries];
        for (int i = 0; i < AckEntries; i++)
        {
            ulong key = (i % 3) switch
            {
                0 => (ulong)random.Next(0, 64),
                1 => (ulong)random.Next(64, 16384),
                _ => (ulong)random.Next(16384, 1 << 30),
            };
            _entries[i] = new LatestAckEntry((ushort)random.Next(2, 20), key, (uint)random.Next());
        }

        _ackFrame = new byte[1200];
        LatestAckBatchWriter writer = new(_ackFrame, ControlCarrier.Datagram);
        foreach (LatestAckEntry entry in _entries)
        {
            writer.TryAdd(entry);
        }

        _ackFrame = _ackFrame.AsSpan(0, writer.Finish()).ToArray();
    }

    [Benchmark]
    public uint PingPong_Datagram()
    {
        byte[] buffer = _buffer;
        ControlCodec.TryWrite(buffer, new Ping(123456), ControlCarrier.Datagram, out int n);
        ControlCodec.TryReadDatagram(buffer.AsSpan(0, n), out _, out ReadOnlySpan<byte> body, out _);
        ControlCodec.TryParse(body, out Ping ping);
        ControlCodec.TryWrite(buffer, new Pong(ping.TimeMicros, 7, 8), ControlCarrier.Datagram, out n);
        ControlCodec.TryReadDatagram(buffer.AsSpan(0, n), out _, out body, out _);
        ControlCodec.TryParse(body, out Pong pong);
        return pong.EchoedTimeMicros + pong.SendTimeMicros;
    }

    [Benchmark]
    public uint PingPong_Stream()
    {
        byte[] buffer = _buffer;
        ControlCodec.TryWrite(buffer, new Ping(123456), ControlCarrier.Stream, out int n);
        ControlCodec.TryReadStream(buffer.AsSpan(0, n), out _, out ReadOnlySpan<byte> body, out _);
        ControlCodec.TryParse(body, out Ping ping);
        ControlCodec.TryWrite(buffer, new Pong(ping.TimeMicros, 7, 8), ControlCarrier.Stream, out n);
        ControlCodec.TryReadStream(buffer.AsSpan(0, n), out _, out body, out _);
        ControlCodec.TryParse(body, out Pong pong);
        return pong.EchoedTimeMicros + pong.SendTimeMicros;
    }

    [Benchmark]
    public int LatestAck32_Encode()
    {
        LatestAckBatchWriter writer = new(_buffer, ControlCarrier.Datagram);
        LatestAckEntry[] entries = _entries;
        for (int i = 0; i < entries.Length; i++)
        {
            writer.TryAdd(entries[i]);
        }

        return writer.Finish();
    }

    [Benchmark]
    public ulong LatestAck32_Decode_V0()
    {
        ControlCodec.TryReadDatagram(_ackFrame, out _, out ReadOnlySpan<byte> body, out _);
        LatestAckDecodeV0.TryParse(body, out LatestAckDecodeV0.ReaderV0 reader);
        ulong sum = 0;
        foreach (LatestAckEntry entry in reader)
        {
            sum += entry.Key + entry.Version;
        }

        return sum;
    }

    [Benchmark]
    public ulong LatestAck32_Decode()
    {
        ControlCodec.TryReadDatagram(_ackFrame, out _, out ReadOnlySpan<byte> body, out _);
        ControlCodec.TryParse(body, out LatestAckBatchReader reader);
        ulong sum = 0;
        foreach (LatestAckEntry entry in reader)
        {
            sum += entry.Key + entry.Version;
        }

        return sum;
    }

    [Benchmark]
    public ulong LatestAck32_RoundTrip()
    {
        int length = LatestAck32_Encode();
        ControlCodec.TryReadDatagram(_buffer.AsSpan(0, length), out _, out ReadOnlySpan<byte> body, out _);
        ControlCodec.TryParse(body, out LatestAckBatchReader reader);
        ulong sum = 0;
        foreach (LatestAckEntry entry in reader)
        {
            sum += entry.Key + entry.Version;
        }

        return sum;
    }
}
