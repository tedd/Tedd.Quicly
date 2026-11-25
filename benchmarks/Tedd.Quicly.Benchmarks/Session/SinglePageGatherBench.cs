using BenchmarkDotNet.Attributes;
using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Benchmarks.Session;

/// <summary>End-to-end cost per one-page, 1 KiB datagram in a batch of 64 on a lossless simulated link.</summary>
[Config(typeof(InProcessShortRunConfig))]
public class SinglePageGatherBench
{
    private const int Batch = 64;
    private const int PayloadBytes = 1024;
    private const ushort Channel = 2;

    private SessionFixture _fixture = null!;
    private QuiclyPeer _client = null!;
    private ulong _delivered;

    [GlobalSetup]
    public void Setup()
    {
        // SendGather sends the entire size-class block. Two 1 KiB messages cannot share a 1,200-byte datagram.
        _fixture = new SessionFixture(1, clientAllocator: new SlabAllocatorOptions
        {
            FreeListShards = 1,
            SizeClasses = [new(64, 512), new(256, 256), new(PayloadBytes, 256), new(4096, 32), new(65536, 2)],
        });
        _client = _fixture.Clients[0];
        _fixture.Servers[0].RegisterHandler(Channel, Receive);
    }

    [GlobalCleanup]
    public void Cleanup() => _fixture.Dispose();

    [Benchmark(OperationsPerInvoke = Batch)]
    public void SendGather()
    {
        _delivered = 0;
        Span<BufferLease> page = stackalloc BufferLease[1];
        for (int i = 0; i < Batch; i++)
        {
            page[0] = _client.RentBuffer(PayloadBytes);
            if (page[0].Length != PayloadBytes)
            {
                throw new InvalidOperationException("The benchmark could not rent a 1 KiB page.");
            }

            _client.GetBufferSpan(in page[0]).Fill((byte)i);
            SendResult result = _client.SendGather(new SendHeader(Channel), page);
            if (!result.IsAdmitted)
            {
                _client.ReturnBuffer(in page[0]);
                throw new InvalidOperationException($"Gather admission failed with {result.Status}.");
            }
        }

        _fixture.Cycle();
        if (_delivered != ulong.MaxValue)
        {
            throw new InvalidOperationException("The receiver did not receive every payload in the batch intact.");
        }
    }

    private void Receive(QuiclyPeer peer, in ReceiveHeader header, ReadOnlySpan<byte> payload)
    {
        if (payload.Length == PayloadBytes && payload[0] < Batch && !payload.ContainsAnyExcept(payload[0]))
        {
            _delivered |= 1UL << payload[0];
        }
    }
}
