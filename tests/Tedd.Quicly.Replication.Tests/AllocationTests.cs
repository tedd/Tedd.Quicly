using System.Numerics;

namespace Tedd.Quicly.Replication.Tests;

/// <summary>Steady-state hot paths must not allocate (ADR 0007/0008).</summary>
public class AllocationTests
{
    private static void AssertNoAllocations(Action body, int iterations = 5000)
    {
        for (int i = 0; i < 200; i++)
        {
            body();
        }

        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < iterations; i++)
        {
            body();
        }

        long after = GC.GetAllocatedBytesForCurrentThread();
        Assert.Equal(0, after - before);
    }

    [Fact]
    public void EntityIdAllocator()
    {
        EntityIdAllocator allocator = new(128, 8, 2);
        long tick = 0;
        AssertNoAllocations(() =>
        {
            allocator.TryAllocate(tick, out EntityId a);
            allocator.TryAllocate(tick, out EntityId b);
            allocator.IsAlive(a);
            allocator.Free(a, tick);
            allocator.Free(b, tick);
            tick++;
        });
    }

    [Fact]
    public void BitWriter_And_BitReader()
    {
        byte[] buffer = new byte[256];
        Quaternion rotation = Quaternion.CreateFromYawPitchRoll(0.2f, 0.4f, 0.6f);
        Vector3 direction = Vector3.Normalize(new Vector3(1, 2, 3));
        AssertNoAllocations(() =>
        {
            BitWriter writer = new(buffer);
            for (int i = 0; i < 8; i++)
            {
                writer.WriteBits((ulong)i * 12345, 17);
                writer.WriteVarInt(-i * 1000);
                writer.WriteQuantizedFloat(i * 1.5f, -100f, 100f, 18);
                writer.WriteUnitVector(direction, 11);
                writer.WriteQuaternion(rotation, 9);
            }

            writer.WriteBytes(buffer.AsSpan(0, 4));
            int length = writer.Flush();
            BitReader reader = new(buffer.AsSpan(0, length));
            for (int i = 0; i < 8; i++)
            {
                reader.ReadBits(17);
                reader.ReadVarInt();
                reader.ReadQuantizedFloat(-100f, 100f, 18);
                reader.ReadUnitVector(11);
                reader.ReadQuaternion(9);
            }

            reader.ReadByteSpan(4);
        });
    }

    [Fact]
    public void DeltaCodec()
    {
        Random random = new(50);
        byte[] baseline = TestData.Random(random, 1024);
        byte[] current = TestData.Mutate(random, baseline, 1100, 0.05);
        byte[] delta = new byte[Replication.DeltaCodec.GetMaxCompressedLength(1100)];
        byte[] scratch = new byte[Replication.DeltaCodec.GetMaxEncodedLength(1100)];
        byte[] output = new byte[1100];
        AssertNoAllocations(() =>
        {
            int n = Replication.DeltaCodec.Encode(baseline, current, delta);
            Replication.DeltaCodec.Decode(baseline, delta.AsSpan(0, n), output);
            n = Replication.DeltaCodec.EncodeCompressed(baseline, current, delta, scratch);
            Replication.DeltaCodec.DecodeCompressed(baseline, delta.AsSpan(0, n), output, scratch);
        }, 2000);
    }

    [Fact]
    public void SnapshotHistory()
    {
        SnapshotHistory history = new(32, 64 * 1024, 4);
        byte[] snapshot = new byte[1500];
        uint tick = 0;
        AssertNoAllocations(() =>
        {
            tick++;
            history.Store(tick, snapshot.AsSpan(0, (int)(tick % 1500)));
            history.TryGet(tick - 3, out _);
            history.AckBaseline((int)(tick & 3), tick - 2);
            history.TryGetBaseline((int)(tick & 3), out _, out _);
            history.TryGetLatest(out _, out _);
        });
    }

    [Fact]
    public void Input_Buffer_And_Queue()
    {
        InputBuffer<Move> client = new(64, 3);
        InputQueue<Move> server = new(64);
        byte[] datagram = new byte[64];
        int tick = 0;
        AssertNoAllocations(() =>
        {
            uint sequence = client.Add(new Move(tick++, 1, 2));
            int n = client.WriteRedundant(datagram);
            if ((tick & 7) != 0)
            {
                server.Receive(datagram.AsSpan(0, n), out _);
            }

            while (server.TryDequeue(out uint s, out _) != InputDequeueStatus.Empty)
            {
                if (server.SkipGap() == 0)
                {
                    client.Acknowledge(s);
                }
            }

            client.TryGet(sequence, out _);
        });
    }

    private struct NoAllocReplay : IReplay<PlayerState>
    {
        public InputBuffer<Step> Inputs;

        public void Replay(uint sequence, ref PlayerState state)
        {
            Inputs.TryGet(sequence, out Step step);
            state = new PlayerState(state.X + step.Dx, state.Y + step.Dy);
        }
    }

    [Fact]
    public void PredictionHistory()
    {
        InputBuffer<Step> inputs = new(64);
        PredictionHistory<PlayerState> history = new(64);
        NoAllocReplay replay = new() { Inputs = inputs };
        PlayerState state = default;
        AssertNoAllocations(() =>
        {
            uint sequence = inputs.Add(new Step(1, 2));
            state = new PlayerState(state.X + 1, state.Y + 2);
            history.Record(sequence, state);
            if (sequence > 5)
            {
                state = history.Reconcile(sequence - 5, new PlayerState((int)sequence, 0), ref replay);
            }
        });
    }

    [Fact]
    public void InterpolationBuffer()
    {
        InterpolationBuffer<Vector3> buffer = new(32, 50_000, 20_000);
        long time = 0;
        AssertNoAllocations(() =>
        {
            time += 16_666;
            buffer.Add(time, new Vector3(time, 0, 0));
            buffer.Add(time - 5_000, new Vector3(time, 1, 0));
            buffer.Sample(time, out _, out _, out _);
            buffer.TryGetNewest(out _, out _);
        });
    }

    [Fact]
    public void DedupWindow()
    {
        DedupWindow window = new();
        ulong id = 0;
        AssertNoAllocations(() =>
        {
            id += 3;
            window.TryAccept(id);
            window.Accept(id - 7);
            window.Check(id - 2000);
        });
    }

    [Fact]
    public void Clocks_And_Quantization()
    {
        FixedTickClock clock = new(60);
        ServerTickEstimator estimator = new(60);
        long now = 0;
        AssertNoAllocations(() =>
        {
            now += 7_000;
            clock.Advance(7_000);
            clock.AdvanceTo(now);
            _ = clock.InterpolationAlpha;
            estimator.SetClockOffset(now / 100);
            estimator.ObserveServerTick(now / 16_666, now);
            estimator.Update(now);
            Quantization.DequantizeFloat(Quantization.QuantizeFloat(now, 0, 1e9f, 24), 0, 1e9f, 24);
            Quantization.DequantizeUnitVector(Quantization.QuantizeUnitVector(new Vector3(now, 1, 2), 12), 12);
            Quantization.DequantizeQuaternion(Quantization.QuantizeQuaternion(new Quaternion(1, 2, 3, now), 12), 12);
        });
    }
}
