using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Framing;
using static Tedd.Quicly.Core.Tests.Framing.TestTables;

namespace Tedd.Quicly.Core.Tests.Framing;

/// <summary>Hot paths must not allocate on the GC heap in steady state (ADR 0007/0008).</summary>
public class FramingZeroAllocationTests
{
    [Fact]
    public void Datagram_WriteHeader_And_TryParse()
    {
        ChannelTable table = All;
        ChannelDefinition keyed = Get(USeq32Keyed);
        ChannelDefinition frag = Get(USFragKeyedLz4);
        byte[] buffer = new byte[64];
        AllocationAssert.None(() =>
        {
            for (uint i = 0; i < 16; i++)
            {
                MessageHeader h = new() { Sequence = i, Key = i * 1000 };
                int n = DatagramFraming.WriteHeader(buffer, keyed, h);
                if (DatagramFraming.TryParse(buffer.AsSpan(0, n + 8), table, out MessageHeader p, out int offset) != ParseStatus.Ok
                    || offset != n || p.Key != h.Key)
                {
                    throw new InvalidOperationException();
                }

                h = new MessageHeader { Sequence = i, Key = 5, FragCount = 3, FragIndex = 1, RawLength = 500 };
                n = DatagramFraming.WriteHeader(buffer, frag, h);
                if (DatagramFraming.TryParse(buffer.AsSpan(0, n + 8), table, 1000, out p, out _) != ParseStatus.Ok || p.RawLength != 500)
                {
                    throw new InvalidOperationException();
                }
            }
        });
    }

    [Fact]
    public void Container_Write_And_Iterate()
    {
        ChannelTable table = All;
        ChannelDefinition keyed = Get(USeq32Keyed);
        byte[] buffer = new byte[1200];
        AllocationAssert.None(() =>
        {
            PackedContainerWriter writer = new(buffer, 77, true);
            for (uint i = 0; i < 32; i++)
            {
                MessageHeader h = new() { Sequence = i, Key = i };
                int headerLength = DatagramFraming.GetHeaderLength(keyed, h);
                if (!writer.TryReserve(headerLength + 16, out Span<byte> slot))
                {
                    throw new InvalidOperationException();
                }

                DatagramFraming.WriteHeader(slot, keyed, h);
                slot.Slice(headerLength).Fill((byte)i);
            }

            if (PackedContainer.TryParse(writer.Written, out PackedContainerReader reader) != ParseStatus.Ok)
            {
                throw new InvalidOperationException();
            }

            int count = 0;
            foreach (ReadOnlySpan<byte> message in reader)
            {
                if (DatagramFraming.TryParse(message, table, out _, out _) != ParseStatus.Ok)
                {
                    throw new InvalidOperationException();
                }

                count++;
            }

            if (count != 32)
            {
                throw new InvalidOperationException();
            }
        });
    }

    [Fact]
    public void Stream_Parser_Over_Segments()
    {
        StreamBuilder b = new StreamBuilder().Preamble(OrderedFull);
        for (int i = 0; i < 50; i++)
        {
            b.Frame(Get(OrderedFull), new StreamMessageHeader { Key = (ulong)i * 100, RequestId = (uint)i, RawLength = i % 2 == 0 ? 0 : 200 }, Bytes.Fill(40, (byte)i));
        }

        byte[] stream = b.ToArray();
        ChannelTable table = All;
        StreamFrameParser parser = default;
        AllocationAssert.None(() =>
        {
            parser.Reset(table);
            long payload = 0;
            int ends = 0;
            for (int offset = 0; offset < stream.Length; offset += 7)
            {
                ReadOnlySpan<byte> input = stream.AsSpan(offset, Math.Min(7, stream.Length - offset));
                while (true)
                {
                    StreamEvent ev = parser.Read(ref input, out ReadOnlySpan<byte> chunk);
                    if (ev == StreamEvent.NeedMore)
                    {
                        break;
                    }

                    if (ev == StreamEvent.Error)
                    {
                        throw new InvalidOperationException();
                    }

                    payload += chunk.Length;
                    if (ev == StreamEvent.MessageEnd)
                    {
                        ends++;
                    }
                }
            }

            if (payload != 50 * 40 || ends != 50 || parser.Finish() != ParseStatus.Ok)
            {
                throw new InvalidOperationException();
            }
        }, iterations: 500);
    }

    [Fact]
    public void Bulk_And_Control_Streams()
    {
        BulkHeader header = StreamFramingTests.SampleBulk(hash: true, chunked: true, length: 300);
        byte[] bulk = new StreamBuilder().Preamble(BulkChannel).Bulk(header).Chunk(0, Bytes.Fill(100)).Chunk(200, Bytes.Fill(100)).ToArray();
        byte[] control = new StreamBuilder().Preamble(0).Control(0x10, Bytes.Fill(30)).Control(0x01, Bytes.Fill(4)).ToArray();
        ChannelTable table = All;
        StreamFrameParser parser = default;
        AllocationAssert.None(() =>
        {
            parser.Reset(table);
            ReadOnlySpan<byte> input = bulk;
            while (parser.Read(ref input, out _) is not (StreamEvent.NeedMore or StreamEvent.Error))
            {
            }

            parser.Reset(StreamRole.Control, null);
            ReadOnlySpan<byte> input2 = control;
            while (parser.Read(ref input2, out _) is not (StreamEvent.NeedMore or StreamEvent.Error))
            {
            }

            if (parser.Finish() != ParseStatus.Ok)
            {
                throw new InvalidOperationException();
            }
        });
    }

    [Fact]
    public void Stream_Frame_And_Bulk_Header_Codecs()
    {
        ChannelDefinition ch = Get(OrderedFull);
        byte[] buffer = new byte[128];
        BulkHeader bulk = StreamFramingTests.SampleBulk(hash: true, chunked: false);
        AllocationAssert.None(() =>
        {
            StreamMessageHeader h = new() { Length = 40, Key = 12345, RequestId = 3, RawLength = 90 };
            int n = StreamFraming.WriteFrameHeader(buffer, ch, h);
            if (StreamFraming.TryParseFrameHeader(buffer.AsSpan(0, n), ch, 0, out StreamMessageHeader p, out int consumed) != ParseStatus.Ok || consumed != n || p.Key != 12345)
            {
                throw new InvalidOperationException();
            }

            n = StreamFraming.WriteBulkHeader(buffer, bulk);
            if (StreamFraming.TryParseBulkHeader(buffer.AsSpan(0, n), 1000, out BulkHeader parsed, out _) != ParseStatus.Ok || parsed.Length != bulk.Length)
            {
                throw new InvalidOperationException();
            }

            n = StreamFraming.WriteControlFrameHeader(buffer, 0x12, 20);
            if (StreamFraming.TryParseControlFrameHeader(buffer.AsSpan(0, n), out _, out int body, out _) != ParseStatus.Ok || body != 20)
            {
                throw new InvalidOperationException();
            }
        });
    }

    [Fact]
    public void Table_Lookup()
    {
        ChannelTable table = All;
        AllocationAssert.None(() =>
        {
            int found = 0;
            for (int id = -2; id < 400; id++)
            {
                if (table[id] is not null)
                {
                    found++;
                }
            }

            if (found != 18)
            {
                throw new InvalidOperationException();
            }
        });
    }
}
