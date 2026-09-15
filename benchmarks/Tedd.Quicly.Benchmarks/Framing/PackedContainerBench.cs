using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Framing;

namespace Tedd.Quicly.Benchmarks.Framing;

/// <summary>
/// A packed container of 32 small keyed messages (32-bit sequence, 1–2 byte key, 16-byte payload; ≈ 750 bytes):
/// validate + iterate, validate + iterate + parse every inner header, and build it in place.
/// </summary>
[Config(typeof(InProcessShortRunConfig))]
public class PackedContainerBench
{
    private const int Messages = 32;
    private const int PayloadLength = 16;

    private readonly byte[] _buffer = new byte[1200];
    private readonly MessageHeader[] _headers = new MessageHeader[Messages];
    private byte[] _container = Array.Empty<byte>();
    private ChannelDefinition _channel = null!;

    [GlobalSetup]
    public void Setup()
    {
        Random random = new(3);
        _channel = FramingBenchData.Table[FramingBenchData.Keyed32]!;
        for (int i = 0; i < Messages; i++)
        {
            _headers[i] = FramingBenchData.Typical(random, (uint)i);
        }

        _container = Build(_buffer).ToArray();
    }

    private ReadOnlySpan<byte> Build(Span<byte> buffer)
    {
        PackedContainerWriter writer = new(buffer, 1234, hasTick: true);
        ChannelDefinition channel = _channel;
        MessageHeader[] headers = _headers;
        for (int i = 0; i < headers.Length; i++)
        {
            int headerLength = DatagramFraming.GetHeaderLength(channel, headers[i]);
            writer.TryReserve(headerLength + PayloadLength, out Span<byte> slot);
            DatagramFraming.WriteHeader(slot, channel, headers[i]);
            slot.Slice(headerLength).Fill((byte)i);
        }

        return buffer.Slice(0, writer.Length);
    }

    [Benchmark]
    public int Iterate()
    {
        int total = 0;
        PackedContainer.TryParse(_container, out PackedContainerReader reader);
        foreach (ReadOnlySpan<byte> message in reader)
        {
            total += message.Length;
        }

        return total;
    }

    [Benchmark]
    public int IterateAndParse()
    {
        int total = 0;
        ChannelTable table = FramingBenchData.Table;
        PackedContainer.TryParse(_container, out PackedContainerReader reader);
        foreach (ReadOnlySpan<byte> message in reader)
        {
            DatagramFraming.TryParse(message, table, out MessageHeader h, out int payloadOffset);
            total += payloadOffset + (int)h.Key;
        }

        return total;
    }

    [Benchmark]
    public int Build() => Build(_buffer).Length;
}
