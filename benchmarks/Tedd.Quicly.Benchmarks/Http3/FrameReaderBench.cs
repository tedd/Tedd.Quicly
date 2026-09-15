using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Http3;

namespace Tedd.Quicly.Benchmarks.Http3;

/// <summary>
/// Frame reader throughput: a 1 MiB stream of DATA frames (1200-byte payloads, i.e. one per QUIC packet)
/// interleaved with small HEADERS frames, consumed either as one contiguous buffer or in 1200-byte receive chunks.
/// </summary>
[Config(typeof(Http3BenchConfig))]
public class FrameReaderBench
{
    private const int ChunkSize = 1200;
    private byte[] _stream = [];
    private Http3FrameReader _reader;

    [GlobalSetup]
    public void Setup()
    {
        byte[] payload = new byte[ChunkSize];
        byte[] headers = new byte[40];
        var buffer = new byte[1 << 20];
        int pos = 0;
        int i = 0;
        while (true)
        {
            ReadOnlySpan<byte> p = (i++ % 8 == 0) ? headers : payload;
            Http3FrameType t = p.Length == headers.Length ? Http3FrameType.Headers : Http3FrameType.Data;
            int n = Http3FrameWriter.WriteFrame(buffer.AsSpan(pos), t, p);
            if (n < 0) break;
            pos += n;
        }
        _stream = buffer.AsSpan(0, pos).ToArray();
        _reader = new Http3FrameReader(1 << 20);
    }

    [Benchmark(Baseline = true)]
    public long Contiguous()
    {
        _reader.Reset();
        long bytes = 0;
        ReadOnlySpan<byte> input = _stream;
        while (!input.IsEmpty)
        {
            Http3FrameReadStatus s = _reader.Read(input, out int consumed, out ReadOnlySpan<byte> payload);
            if (s == Http3FrameReadStatus.NeedMoreData) break;
            bytes += payload.Length;
            input = input.Slice(consumed);
        }
        return bytes;
    }

    [Benchmark]
    public long Chunked1200()
    {
        _reader.Reset();
        long bytes = 0;
        ReadOnlySpan<byte> stream = _stream;
        for (int offset = 0; offset < stream.Length; offset += ChunkSize)
        {
            ReadOnlySpan<byte> input = stream.Slice(offset, Math.Min(ChunkSize, stream.Length - offset));
            while (!input.IsEmpty)
            {
                Http3FrameReadStatus s = _reader.Read(input, out int consumed, out ReadOnlySpan<byte> payload);
                bytes += payload.Length;
                input = input.Slice(consumed);
                if (s == Http3FrameReadStatus.NeedMoreData) break;
            }
        }
        return bytes;
    }
}
