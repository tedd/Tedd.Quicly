using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Archive.Framing;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Framing;

namespace Tedd.Quicly.Benchmarks.Framing;

/// <summary>
/// <see cref="DatagramFraming"/> write and parse of 256 headers per invocation for the two common shapes:
/// <c>unkeyed16</c> (UnreliableSequenced, 16-bit sequence, 3-byte header) and <c>keyed32</c> (32-bit sequence plus a
/// 1–2 byte entity key). Each datagram carries a 24-byte payload.
/// </summary>
[Config(typeof(InProcessShortRunConfig))]
public class DatagramFramingBench
{
    private const int Count = 256;
    private const int PayloadLength = 24;

    private readonly byte[] _scratch = new byte[64];
    private ChannelDefinition _channel = null!;
    private MessageHeader[] _headers = Array.Empty<MessageHeader>();
    private byte[] _datagrams = Array.Empty<byte>();
    private int[] _offsets = Array.Empty<int>();

    [Params("unkeyed16", "keyed32")]
    public string Shape { get; set; } = "unkeyed16";

    [GlobalSetup]
    public void Setup()
    {
        Random random = new(42);
        _channel = FramingBenchData.Table[Shape == "unkeyed16" ? FramingBenchData.Unkeyed16 : FramingBenchData.Keyed32]!;
        _headers = new MessageHeader[Count];
        _datagrams = new byte[Count * (DatagramFraming.MaxHeaderLength + PayloadLength)];
        _offsets = new int[Count + 1];
        int offset = 0;
        for (int i = 0; i < Count; i++)
        {
            _headers[i] = FramingBenchData.Typical(random, (uint)i * 3);
            _offsets[i] = offset;
            offset += DatagramFraming.WriteHeader(_datagrams.AsSpan(offset), _channel, _headers[i]) + PayloadLength;
        }

        _offsets[Count] = offset;
    }

    [Benchmark]
    public int WriteHeader()
    {
        int total = 0;
        ChannelDefinition channel = _channel;
        MessageHeader[] headers = _headers;
        Span<byte> scratch = _scratch;
        for (int i = 0; i < headers.Length; i++)
        {
            total += DatagramFraming.WriteHeader(scratch, channel, headers[i]);
        }

        return total;
    }

    [Benchmark]
    public int TryParse()
    {
        int total = 0;
        ChannelTable table = FramingBenchData.Table;
        ReadOnlySpan<byte> all = _datagrams;
        int[] offsets = _offsets;
        for (int i = 0; i < Count; i++)
        {
            DatagramFraming.TryParse(all.Slice(offsets[i], offsets[i + 1] - offsets[i]), table, out MessageHeader h, out int payloadOffset);
            total += payloadOffset + (int)h.Sequence;
        }

        return total;
    }

    [Benchmark]
    public int WriteAndParse()
    {
        int total = 0;
        ChannelTable table = FramingBenchData.Table;
        ChannelDefinition channel = _channel;
        MessageHeader[] headers = _headers;
        Span<byte> scratch = _scratch;
        for (int i = 0; i < headers.Length; i++)
        {
            int n = DatagramFraming.WriteHeader(scratch, channel, headers[i]);
            DatagramFraming.TryParse(scratch.Slice(0, n + PayloadLength), table, out MessageHeader h, out int payloadOffset);
            total += payloadOffset + (int)h.Key;
        }

        return total;
    }
}

/// <summary>
/// ADR 0007 loop for <see cref="DatagramFraming.GetHeaderLength"/>: V0 (archived, one branch per optional field
/// recomputed from the channel flags, if-chain varint length), V1 (archived, precomputed constant + field masks +
/// branch-free varint length) and Current (precomputed constant + one branch per optional field).
/// <c>keyed32</c> = one predictable shape; <c>mixed</c> = random channel shapes and key sizes.
/// </summary>
[Config(typeof(InProcessShortRunConfig))]
public class DatagramHeaderLengthBench
{
    private const int Count = 256;

    private ChannelDefinition[] _channels = Array.Empty<ChannelDefinition>();
    private DatagramHeaderLengthV1.Entry[] _entries = Array.Empty<DatagramHeaderLengthV1.Entry>();
    private MessageHeader[] _headers = Array.Empty<MessageHeader>();

    [Params("keyed32", "mixed")]
    public string Set { get; set; } = "keyed32";

    [GlobalSetup]
    public void Setup()
    {
        Random random = new(7);
        _channels = new ChannelDefinition[Count];
        _headers = new MessageHeader[Count];
        for (int i = 0; i < Count; i++)
        {
            if (Set == "keyed32")
            {
                _channels[i] = FramingBenchData.Table[FramingBenchData.Keyed32]!;
                _headers[i] = FramingBenchData.Typical(random, (uint)i);
            }
            else
            {
                int[] ids = FramingBenchData.DatagramChannels;
                _channels[i] = FramingBenchData.Table[ids[random.Next(ids.Length)]]!;
                _headers[i] = FramingBenchData.Mixed(random, (uint)i);
            }
        }

        _entries = _channels.Select(c => new DatagramHeaderLengthV1.Entry(c)).ToArray();
    }

    [Benchmark(Baseline = true)]
    public int V0_Branchy()
    {
        int total = 0;
        ChannelDefinition[] channels = _channels;
        MessageHeader[] headers = _headers;
        for (int i = 0; i < channels.Length && i < headers.Length; i++)
        {
            total += DatagramHeaderLengthV0.GetHeaderLength(channels[i], headers[i]);
        }

        return total;
    }

    [Benchmark]
    public int V1_TableDriven()
    {
        int total = 0;
        DatagramHeaderLengthV1.Entry[] entries = _entries;
        MessageHeader[] headers = _headers;
        for (int i = 0; i < entries.Length && i < headers.Length; i++)
        {
            total += DatagramHeaderLengthV1.GetHeaderLength(entries[i], headers[i]);
        }

        return total;
    }

    [Benchmark]
    public int Current_FixedPlusBranches()
    {
        int total = 0;
        ChannelDefinition[] channels = _channels;
        MessageHeader[] headers = _headers;
        for (int i = 0; i < channels.Length && i < headers.Length; i++)
        {
            total += DatagramFraming.GetHeaderLength(channels[i], headers[i]);
        }

        return total;
    }
}
