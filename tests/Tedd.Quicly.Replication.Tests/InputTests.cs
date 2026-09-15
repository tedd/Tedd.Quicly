using System.Buffers.Binary;
using System.Runtime.InteropServices;

namespace Tedd.Quicly.Replication.Tests;

public record struct Move(int Dx, short Dy, byte Buttons);

public class InputBufferTests
{
    [Fact]
    public void Constructor_And_Properties_Validate()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new InputBuffer<Move>(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new InputBuffer<Move>((1 << 30) + 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new InputBuffer<Move>(4, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new InputBuffer<Move>(4, 256));
        InputBuffer<Move> buffer = new(5, 2, 7);
        Assert.Equal(8, buffer.Capacity);
        Assert.Equal(2, buffer.Redundancy);
        Assert.Equal(7u, buffer.NextSequence);
        Assert.Equal(0, buffer.Count);
        Assert.Throws<ArgumentOutOfRangeException>(() => buffer.Redundancy = 0);
        Assert.Throws<ArgumentOutOfRangeException>(() => buffer.Redundancy = 256);
        buffer.Redundancy = 255;
        Assert.Equal(255, buffer.Redundancy);
    }

    [Fact]
    public void Remembers_The_Last_Capacity_Inputs()
    {
        InputBuffer<Move> buffer = new(4, 3, 10);
        for (int i = 0; i < 6; i++)
        {
            Assert.Equal((uint)(10 + i), buffer.Add(new Move(i, 0, 0)));
        }

        Assert.Equal(4, buffer.Count);
        Assert.Equal(16u, buffer.NextSequence);
        Assert.False(buffer.TryGet(11, out Move forgotten));
        Assert.Equal(default, forgotten);
        Assert.False(buffer.TryGet(16, out _));
        for (uint s = 12; s <= 15; s++)
        {
            Assert.True(buffer.TryGet(s, out Move move));
            Assert.Equal((int)s - 10, move.Dx);
        }
    }

    [Fact]
    public void Redundant_Batch_Layout()
    {
        InputBuffer<Move> buffer = new(16, 3, 100);
        Assert.Equal(0, buffer.GetRedundantLength());
        Assert.Equal(0, buffer.WriteRedundant(new byte[64]));
        for (int i = 0; i < 5; i++)
        {
            buffer.Add(new Move(i, (short)(-i), (byte)(i * 3)));
        }

        int size = System.Runtime.CompilerServices.Unsafe.SizeOf<Move>();
        Assert.Equal(InputBatch.HeaderSize + 3 * size, buffer.GetRedundantLength());
        byte[] batch = new byte[64];
        Assert.Equal(-1, buffer.WriteRedundant(batch.AsSpan(0, buffer.GetRedundantLength() - 1)));
        int n = buffer.WriteRedundant(batch);
        Assert.Equal(buffer.GetRedundantLength(), n);
        Assert.Equal(102u, BinaryPrimitives.ReadUInt32LittleEndian(batch));
        Assert.Equal(3, batch[4]);
        for (int i = 0; i < 3; i++)
        {
            Move move = MemoryMarshal.Read<Move>(batch.AsSpan(InputBatch.HeaderSize + i * size, size));
            Assert.Equal(new Move(2 + i, (short)(-(2 + i)), (byte)((2 + i) * 3)), move);
        }

        Assert.Equal(InputBatchStatus.Ok, InputBatch.TryReadHeader<Move>(batch.AsSpan(0, n), out uint first, out int count));
        Assert.Equal(102u, first);
        Assert.Equal(3, count);

        InputBuffer<Move> fewer = new(16, 3);
        fewer.Add(default);
        Assert.Equal(InputBatch.HeaderSize + size, fewer.WriteRedundant(batch));
        Assert.Equal(1, batch[4]);
    }

    [Fact]
    public void Acknowledgements_Stop_Repeating_Received_Inputs()
    {
        InputBuffer<Move> buffer = new(16, 3);
        for (int i = 0; i < 10; i++)
        {
            buffer.Add(new Move(i, 0, 0));
        }

        Assert.Equal(10, buffer.PendingCount);
        Assert.False(buffer.Acknowledge(20));
        Assert.False(buffer.Acknowledge(10));
        Assert.True(buffer.Acknowledge(5));
        Assert.Equal(4, buffer.PendingCount);
        byte[] batch = new byte[64];
        buffer.WriteRedundant(batch);
        Assert.Equal(7u, BinaryPrimitives.ReadUInt32LittleEndian(batch));
        Assert.False(buffer.Acknowledge(4));
        Assert.False(buffer.Acknowledge(5));
        Assert.True(buffer.Acknowledge(8));
        Assert.Equal(1, buffer.PendingCount);
        buffer.WriteRedundant(batch);
        Assert.Equal(9u, BinaryPrimitives.ReadUInt32LittleEndian(batch));
        Assert.Equal(1, batch[4]);
        Assert.True(buffer.Acknowledge(9));
        Assert.Equal(0, buffer.PendingCount);
        Assert.Equal(0, buffer.WriteRedundant(batch));
        buffer.Add(default);
        Assert.Equal(1, buffer.PendingCount);

        buffer.Reset(1000);
        Assert.Equal(0, buffer.Count);
        Assert.Equal(0, buffer.PendingCount);
        Assert.Equal(1000u, buffer.NextSequence);
        Assert.Equal(1000u, buffer.Add(default));
    }

    [Fact]
    public void Batch_Header_Validation()
    {
        Assert.Equal(InputBatch.HeaderSize + 24, InputBatch.GetLength<Move>(3));
        Assert.Throws<ArgumentOutOfRangeException>(() => InputBatch.GetLength<Move>(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => InputBatch.GetLength<Move>(256));
        Assert.Equal(InputBatchStatus.Truncated, InputBatch.TryReadHeader<Move>(new byte[4], out uint first, out int count));
        Assert.Equal(0u, first);
        Assert.Equal(0, count);
        Assert.Equal(InputBatchStatus.Malformed, InputBatch.TryReadHeader<Move>(new byte[5], out _, out _));
        byte[] batch = new byte[5 + 16];
        batch[4] = 2;
        Assert.Equal(InputBatchStatus.Ok, InputBatch.TryReadHeader<Move>(batch, out _, out count));
        Assert.Equal(2, count);
        Assert.Equal(InputBatchStatus.Truncated, InputBatch.TryReadHeader<Move>(batch.AsSpan(0, 20), out _, out _));
        byte[] trailing = new byte[5 + 17];
        trailing[4] = 2;
        Assert.Equal(InputBatchStatus.Malformed, InputBatch.TryReadHeader<Move>(trailing, out _, out _));
    }
}

public class InputQueueTests
{
    private static byte[] Batch(uint first, int count, Func<uint, Move>? make = null)
    {
        make ??= s => new Move((int)s, 1, 2);
        int size = System.Runtime.CompilerServices.Unsafe.SizeOf<Move>();
        byte[] batch = new byte[InputBatch.HeaderSize + count * size];
        BinaryPrimitives.WriteUInt32LittleEndian(batch, first);
        batch[4] = (byte)count;
        for (int i = 0; i < count; i++)
        {
            Move move = make(first + (uint)i);
            MemoryMarshal.Write(batch.AsSpan(InputBatch.HeaderSize + i * size, size), in move);
        }

        return batch;
    }

    [Fact]
    public void Constructor_Validates()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new InputQueue<Move>(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new InputQueue<Move>((1 << 30) + 1));
        InputQueue<Move> queue = new(20);
        Assert.Equal(32, queue.Capacity);
        Assert.False(queue.IsStarted);
        Assert.Equal(InputDequeueStatus.Empty, queue.TryDequeue(out uint sequence, out Move input));
        Assert.Equal(0u, sequence);
        Assert.Equal(default, input);
        Assert.Equal(0, queue.GapLength);
        Assert.Equal(0, queue.SkipGap());
    }

    [Fact]
    public void Dedups_Orders_And_Bounds()
    {
        InputQueue<Move> queue = new(8);
        Assert.Equal(InputBatchStatus.Ok, queue.Receive(Batch(10, 3), out int accepted));
        Assert.Equal(3, accepted);
        Assert.True(queue.IsStarted);
        Assert.Equal(10u, queue.NextSequence);
        Assert.Equal(InputBatchStatus.Ok, queue.Receive(Batch(11, 3), out accepted));
        Assert.Equal(1, accepted);
        Assert.Equal(2, queue.DuplicateCount);
        Assert.Equal(4, queue.Count);

        for (uint s = 10; s <= 13; s++)
        {
            Assert.Equal(InputDequeueStatus.Ok, queue.TryDequeue(out uint sequence, out Move input));
            Assert.Equal(s, sequence);
            Assert.Equal(new Move((int)s, 1, 2), input);
        }

        Assert.Equal(InputDequeueStatus.Empty, queue.TryDequeue(out uint next, out _));
        Assert.Equal(14u, next);

        Assert.Equal(InputBatchStatus.Ok, queue.Receive(Batch(12, 3), out accepted));
        Assert.Equal(1, accepted);
        Assert.Equal(2, queue.StaleCount);

        // Window is [14, 22): 20 and 21 fit, 22 is too far ahead.
        Assert.Equal(InputBatchStatus.Ok, queue.Receive(Batch(20, 3), out accepted));
        Assert.Equal(2, accepted);
        Assert.Equal(1, queue.TooFarAheadCount);

        Assert.Equal(InputBatchStatus.Truncated, queue.Receive(Batch(15, 3).AsSpan(0, 10), out accepted));
        Assert.Equal(0, accepted);
        Assert.Equal(InputBatchStatus.Malformed, queue.Receive(new byte[5], out _));
    }

    [Fact]
    public void Gaps_Are_Reported_And_Can_Be_Filled_Or_Skipped()
    {
        InputQueue<Move> queue = new(16);
        queue.Receive(Batch(0, 3), out _);
        queue.Receive(Batch(5, 2), out _);
        for (uint s = 0; s < 3; s++)
        {
            Assert.Equal(InputDequeueStatus.Ok, queue.TryDequeue(out _, out _));
        }

        Assert.Equal(InputDequeueStatus.Gap, queue.TryDequeue(out uint missing, out Move none));
        Assert.Equal(3u, missing);
        Assert.Equal(default, none);
        Assert.Equal(2, queue.GapLength);

        // A late redundant batch fills the first missing input; 4 is still missing after it.
        queue.Receive(Batch(3, 1), out _);
        Assert.Equal(0, queue.GapLength);
        Assert.Equal(InputDequeueStatus.Ok, queue.TryDequeue(out uint three, out _));
        Assert.Equal(3u, three);
        Assert.Equal(1, queue.GapLength);

        // Or the server gives up on it.
        Assert.Equal(1, queue.SkipGap());
        Assert.Equal(1, queue.SkippedCount);
        Assert.Equal(0, queue.SkipGap());
        Assert.Equal(InputDequeueStatus.Ok, queue.TryDequeue(out uint five, out _));
        Assert.Equal(5u, five);
        queue.Receive(Batch(4, 1), out int accepted);
        Assert.Equal(0, accepted);
        Assert.Equal(1, queue.StaleCount);
    }

    [Fact]
    public void Reset_Unanchors_Or_Reanchors_The_Window()
    {
        InputQueue<Move> queue = new(8);
        queue.Receive(Batch(50, 2), out _);
        queue.Reset();
        Assert.False(queue.IsStarted);
        Assert.Equal(0, queue.Count);
        queue.Receive(Batch(7, 1), out _);
        Assert.Equal(7u, queue.NextSequence);

        queue.Reset(100);
        Assert.True(queue.IsStarted);
        Assert.Equal(100u, queue.NextSequence);
        queue.Receive(Batch(98, 3), out int accepted);
        Assert.Equal(1, accepted);
        Assert.Equal(InputDequeueStatus.Ok, queue.TryDequeue(out uint s, out _));
        Assert.Equal(100u, s);
    }

    [Fact]
    public void Sequence_Wraparound()
    {
        InputBuffer<Move> client = new(16, 3, uint.MaxValue - 1);
        InputQueue<Move> server = new(16);
        byte[] datagram = new byte[64];
        List<uint> received = [];
        for (int i = 0; i < 6; i++)
        {
            uint seq = client.Add(new Move(i, 0, 0));
            int n = client.WriteRedundant(datagram);
            Assert.Equal(InputBatchStatus.Ok, server.Receive(datagram.AsSpan(0, n), out _));
            while (server.TryDequeue(out uint s, out Move m) == InputDequeueStatus.Ok)
            {
                received.Add(s);
                Assert.True(client.TryGet(s, out Move sent));
                Assert.Equal(sent, m);
            }

            Assert.Equal(seq, received[^1]);
        }

        Assert.Equal(new[] { uint.MaxValue - 1, uint.MaxValue, 0u, 1u, 2u, 3u }, received);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(4)]
    public void Redundancy_Survives_Every_Loss_Burst_Shorter_Than_It(int seed)
    {
        Random random = new(seed);
        const int Ticks = 3000;
        bool[] lost = new bool[Ticks];
        for (int t = 0; t < Ticks - 3;)
        {
            int burst = random.Next(0, 3);           // 0..2 consecutive losses, then at least one delivery
            for (int k = 0; k < burst && t < Ticks - 3; k++)
            {
                lost[t++] = true;
            }

            t += random.Next(1, 4);
        }

        InputBuffer<Move> client = new(64, 3);
        InputQueue<Move> server = new(64);
        byte[] datagram = new byte[64];
        uint expected = 0;
        for (int t = 0; t < Ticks; t++)
        {
            client.Add(new Move(t, (short)(t & 0x7FFF), (byte)t));
            int n = client.WriteRedundant(datagram);
            if (!lost[t])
            {
                Assert.Equal(InputBatchStatus.Ok, server.Receive(datagram.AsSpan(0, n), out _));
            }

            InputDequeueStatus status;
            while ((status = server.TryDequeue(out uint s, out Move m)) == InputDequeueStatus.Ok)
            {
                Assert.Equal(expected, s);
                Assert.Equal(new Move((int)s, (short)(s & 0x7FFF), (byte)s), m);
                expected++;
                client.Acknowledge(s);
            }

            Assert.NotEqual(InputDequeueStatus.Gap, status);
        }

        Assert.Equal((uint)Ticks, expected);
        Assert.Equal(0, server.SkippedCount);
    }

    [Fact]
    public void Inputs_Are_Lost_Only_When_Every_Redundant_Copy_Is_Lost()
    {
        Random random = new(5);
        const int Ticks = 3000;
        const int Redundancy = 3;
        bool[] lost = new bool[Ticks];
        for (int t = 0; t < Ticks; t++)
        {
            lost[t] = random.NextDouble() < 0.4;
        }

        lost[0] = false;
        InputBuffer<Move> client = new(64, Redundancy);
        InputQueue<Move> server = new(64);
        byte[] datagram = new byte[64];
        HashSet<uint> received = [];
        for (int t = 0; t < Ticks; t++)
        {
            client.Add(new Move(t, 0, 0));
            int n = client.WriteRedundant(datagram);
            if (lost[t])
            {
                continue;
            }

            server.Receive(datagram.AsSpan(0, n), out _);
            while (true)
            {
                InputDequeueStatus status = server.TryDequeue(out uint s, out _);
                if (status == InputDequeueStatus.Ok)
                {
                    Assert.True(received.Add(s));
                }
                else if (status == InputDequeueStatus.Gap)
                {
                    // Batches arrive in order, so inputs older than the newest batch can no longer arrive.
                    Assert.True(server.SkipGap() > 0);
                }
                else
                {
                    break;
                }
            }
        }

        int missing = 0;
        for (int s = 0; s < Ticks; s++)
        {
            bool delivered = false;
            for (int k = 0; k < Redundancy && s + k < Ticks; k++)
            {
                delivered |= !lost[s + k];
            }

            Assert.Equal(delivered, received.Contains((uint)s));
            missing += delivered ? 0 : 1;
        }

        Assert.True(missing > 0);
        Assert.Equal(missing, server.SkippedCount + (Ticks - 1 - (int)server.NextSequence + 1));
    }

    [Fact]
    public void Reordered_Batches_Are_Dequeued_In_Sequence()
    {
        Random random = new(6);
        InputBuffer<Move> client = new(64, 2);
        List<byte[]> batches = [];
        byte[] datagram = new byte[64];
        for (int t = 0; t < 400; t++)
        {
            client.Add(new Move(t, 0, 0));
            batches.Add(datagram.AsSpan(0, client.WriteRedundant(datagram)).ToArray());
        }

        for (int start = 0; start < batches.Count; start += 5)
        {
            int end = Math.Min(start + 5, batches.Count);
            for (int i = end - 1; i > start; i--)
            {
                int j = random.Next(start, i + 1);
                (batches[i], batches[j]) = (batches[j], batches[i]);
            }
        }

        InputQueue<Move> server = new(64);
        server.Reset(0);
        uint expected = 0;
        foreach (byte[] batch in batches)
        {
            Assert.Equal(InputBatchStatus.Ok, server.Receive(batch, out _));
            while (server.TryDequeue(out uint s, out Move m) == InputDequeueStatus.Ok)
            {
                Assert.Equal(expected, s);
                Assert.Equal((int)s, m.Dx);
                expected++;
            }
        }

        Assert.Equal(400u, expected);
        Assert.True(server.DuplicateCount > 0);
    }
}
