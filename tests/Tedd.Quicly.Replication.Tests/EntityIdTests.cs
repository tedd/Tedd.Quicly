using Tedd.Quicly.Core.Primitives;

namespace Tedd.Quicly.Replication.Tests;

public class EntityIdTests
{
    [Fact]
    public void Wire_Form_Packs_Index_Above_Masked_Generation()
    {
        Assert.Equal((5UL << 8) | 3, new EntityId(5, 3).ToWire());
        Assert.Equal((1UL << 8) | 0xFF, new EntityId(1, 0x1FF).ToWire(8));
        Assert.Equal(7UL, new EntityId(7, 99).ToWire(0));
        Assert.Equal(((ulong)uint.MaxValue << 30) | 0x3FFF_FFFF, new EntityId(uint.MaxValue, uint.MaxValue).ToWire(30));
        Assert.True(new EntityId(uint.MaxValue, uint.MaxValue).ToWire(30) <= VarInt.MaxValue);
        Assert.Equal(0xFFu, EntityId.GetGenerationMask(8));
        Assert.Equal(0u, EntityId.GetGenerationMask(0));
        Assert.Equal(0x3FFF_FFFFu, EntityId.GetGenerationMask(30));
    }

    [Fact]
    public void RoundTrips_For_Every_Generation_Width()
    {
        Random random = new(1);
        byte[] buffer = new byte[8];
        for (int bits = 0; bits <= EntityId.MaxGenerationBits; bits++)
        {
            uint mask = EntityId.GetGenerationMask(bits);
            for (int i = 0; i < 500; i++)
            {
                uint index = i switch { 0 => 0, 1 => uint.MaxValue, _ => (uint)random.NextInt64(0, 1L << 32) >> random.Next(0, 32) };
                EntityId id = new(index, (uint)random.NextInt64(0, 1L << 32) & mask);
                Assert.True(id.TryWrite(buffer, bits, out int written));
                Assert.Equal(id.GetWireLength(bits), written);
                Assert.True(EntityId.TryRead(buffer.AsSpan(0, written), bits, out EntityId back, out int consumed));
                Assert.Equal(written, consumed);
                Assert.Equal(id, back);
                Assert.True(EntityId.TryFromWire(id.ToWire(bits), bits, out EntityId fromWire));
                Assert.Equal(id, fromWire);
            }
        }
    }

    [Fact]
    public void Reading_Rejects_Malformed_Input()
    {
        // Index part wider than 32 bits.
        Assert.False(EntityId.TryFromWire(1UL << 40, 0, out EntityId id));
        Assert.Equal(default, id);
        byte[] wide = new byte[8];
        VarInt.Write(wide, 1UL << 40);
        Assert.False(EntityId.TryRead(wide, 8, out id, out int consumed));
        Assert.Equal(0, consumed);
        Assert.True(EntityId.TryRead(wide, 9, out id, out consumed));
        Assert.Equal(8, consumed);
        Assert.Equal(new EntityId(1u << 31, 0), id);

        // Truncated and non-minimal encodings.
        Assert.False(EntityId.TryRead(ReadOnlySpan<byte>.Empty, 8, out _, out _));
        Assert.False(EntityId.TryRead(new byte[] { 0x40 }, 8, out _, out _));
        Assert.False(EntityId.TryRead(new byte[] { 0x40, 0x05 }, 8, out _, out _));
        Assert.True(EntityId.TryRead(new byte[] { 0x05 }, 2, out id, out consumed));
        Assert.Equal(new EntityId(1, 1), id);
        Assert.Equal(1, consumed);

        // Destination too small.
        Assert.False(new EntityId(1000, 0).TryWrite(new byte[1], 8, out int written));
        Assert.Equal(0, written);
    }

    [Fact]
    public void Fuzz_Read_Never_Throws()
    {
        Random random = new(2);
        byte[] buffer = new byte[10];
        for (int i = 0; i < 20_000; i++)
        {
            random.NextBytes(buffer);
            int length = random.Next(0, buffer.Length + 1);
            int bits = random.Next(0, 31);
            if (EntityId.TryRead(buffer.AsSpan(0, length), bits, out EntityId id, out int consumed))
            {
                Assert.InRange(consumed, 1, length);
                Assert.True(id.Generation <= EntityId.GetGenerationMask(bits));
            }
        }
    }

    [Fact]
    public void Invalid_Generation_Bits_Throw()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => EntityId.GetGenerationMask(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => EntityId.GetGenerationMask(31));
        Assert.Throws<ArgumentOutOfRangeException>(() => new EntityId(1, 1).ToWire(31));
        Assert.Throws<ArgumentOutOfRangeException>(() => EntityId.TryFromWire(0, -1, out _));
        Assert.Throws<ArgumentOutOfRangeException>(() => EntityId.TryRead(new byte[1], 31, out _, out _));
    }

    [Fact]
    public void Equality_And_Formatting()
    {
        EntityId a = new(3, 4);
        EntityId b = new(3, 4);
        EntityId c = new(3, 5);
        EntityId d = new(2, 4);
        Assert.True(a == b);
        Assert.False(a != b);
        Assert.True(a != c);
        Assert.True(a != d);
        Assert.True(a.Equals((object)b));
        Assert.False(a.Equals((object)c));
        Assert.False(a.Equals(null));
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.Equal("3:4", a.ToString());
        Assert.Equal(3u, a.Index);
        Assert.Equal(4u, a.Generation);
    }
}

public class EntityIdAllocatorTests
{
    [Fact]
    public void Constructor_Validates()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new EntityIdAllocator(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new EntityIdAllocator(1, 8, -1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new EntityIdAllocator(1, 31));
        EntityIdAllocator allocator = new(10, 6, 3);
        Assert.Equal(10, allocator.Capacity);
        Assert.Equal(6, allocator.GenerationBits);
        Assert.Equal(3, allocator.ReuseDelayTicks);
        Assert.Equal(0, allocator.Count);
        Assert.Equal(0, allocator.HighWaterMark);
    }

    [Fact]
    public void Allocates_Up_To_Capacity_Then_Reuses_With_A_New_Generation()
    {
        EntityIdAllocator allocator = new(3);
        Assert.True(allocator.TryAllocate(0, out EntityId a));
        Assert.True(allocator.TryAllocate(0, out EntityId b));
        Assert.True(allocator.TryAllocate(0, out EntityId c));
        Assert.Equal([0u, 1u, 2u], new[] { a.Index, b.Index, c.Index });
        Assert.Equal(1u, a.Generation);
        Assert.False(allocator.TryAllocate(0, out EntityId none));
        Assert.Equal(default, none);
        Assert.Equal(3, allocator.Count);
        Assert.Equal(3, allocator.HighWaterMark);

        Assert.True(allocator.Free(b, 0));
        Assert.False(allocator.IsAlive(b));
        Assert.False(allocator.Free(b, 0));
        Assert.Equal(2, allocator.Count);
        Assert.True(allocator.TryAllocate(0, out EntityId b2));
        Assert.Equal(1u, b2.Index);
        Assert.Equal(2u, b2.Generation);
        Assert.True(allocator.IsAlive(b2));
        Assert.False(allocator.IsAlive(b));
        Assert.True(allocator.IsAlive(a));
        Assert.False(allocator.IsAlive(default));
        Assert.False(allocator.IsAlive(new EntityId(uint.MaxValue, 1)));
        Assert.False(allocator.Free(new EntityId(uint.MaxValue, 1), 0));
    }

    [Fact]
    public void Generations_Wrap_Inside_The_Wire_Mask_And_Skip_Zero()
    {
        EntityIdAllocator allocator = new(1, generationBits: 2);
        uint[] seen = new uint[7];
        for (int i = 0; i < seen.Length; i++)
        {
            Assert.True(allocator.TryAllocate(0, out EntityId id));
            seen[i] = id.Generation;
            Assert.True(allocator.Free(id, 0));
        }

        Assert.Equal([1u, 2u, 3u, 1u, 2u, 3u, 1u], seen);

        // A decoded wire id (generation already masked) is recognised.
        Assert.True(allocator.TryAllocate(0, out EntityId live));
        Assert.True(EntityId.TryFromWire(live.ToWire(2), 2, out EntityId decoded));
        Assert.True(allocator.IsAlive(decoded));
    }

    [Fact]
    public void Zero_Generation_Bits_Means_No_Generation_Protection()
    {
        EntityIdAllocator allocator = new(2, generationBits: 0);
        Assert.True(allocator.TryAllocate(0, out EntityId id));
        Assert.Equal(new EntityId(0, 0), id);
        Assert.True(allocator.IsAlive(default));
        Assert.True(allocator.Free(id, 0));
        Assert.True(allocator.TryAllocate(0, out EntityId again));
        Assert.Equal(id, again);
    }

    [Fact]
    public void Reuse_Delay_Holds_Freed_Indices_Back()
    {
        EntityIdAllocator allocator = new(2, reuseDelayTicks: 5);
        Assert.True(allocator.TryAllocate(0, out EntityId a));
        Assert.True(allocator.Free(a, 10));

        // Not yet reusable: a fresh index is handed out instead.
        Assert.True(allocator.TryAllocate(14, out EntityId b));
        Assert.Equal(1u, b.Index);

        // Everything is in use or delayed.
        Assert.False(allocator.TryAllocate(14, out _));
        Assert.True(allocator.TryAllocate(15, out EntityId c));
        Assert.Equal(0u, c.Index);
        Assert.Equal(2u, c.Generation);
    }

    [Fact]
    public void Freed_Indices_Are_Reused_In_Fifo_Order_Across_The_Queue_Wrap()
    {
        EntityIdAllocator allocator = new(4);
        EntityId[] ids = new EntityId[4];
        for (int i = 0; i < 4; i++)
        {
            Assert.True(allocator.TryAllocate(0, out ids[i]));
        }

        for (int round = 0; round < 10; round++)
        {
            int[] order = [2, 0, 3, 1];
            foreach (int k in order)
            {
                Assert.True(allocator.Free(ids[k], round));
            }

            foreach (int k in order)
            {
                Assert.True(allocator.TryAllocate(round, out EntityId id));
                Assert.Equal((uint)k, id.Index);
                ids[k] = id;
            }
        }
    }

    [Fact]
    public void Clear_Kills_Every_Id_And_Makes_Indices_Reusable()
    {
        EntityIdAllocator allocator = new(3, reuseDelayTicks: 100);
        Assert.True(allocator.TryAllocate(0, out EntityId a));
        Assert.True(allocator.TryAllocate(0, out EntityId b));
        Assert.True(allocator.Free(b, 0));
        allocator.Clear();
        Assert.Equal(0, allocator.Count);
        Assert.False(allocator.IsAlive(a));
        Assert.True(allocator.TryAllocate(0, out EntityId a2));
        Assert.Equal(0u, a2.Index);
        Assert.NotEqual(a.Generation, a2.Generation);
        Assert.True(allocator.TryAllocate(0, out EntityId b2));
        Assert.Equal(1u, b2.Index);
        Assert.True(allocator.TryAllocate(0, out EntityId c));
        Assert.Equal(2u, c.Index);
        Assert.False(allocator.TryAllocate(0, out _));
    }

    [Fact]
    public void Random_Operations_Match_A_Model()
    {
        Random random = new(3);
        EntityIdAllocator allocator = new(64, generationBits: 8, reuseDelayTicks: 3);
        List<EntityId> alive = [];
        HashSet<EntityId> dead = [];
        for (long tick = 0; tick < 20_000; tick++)
        {
            if (alive.Count > 0 && random.Next(2) == 0)
            {
                int k = random.Next(alive.Count);
                EntityId id = alive[k];
                alive.RemoveAt(k);
                Assert.True(allocator.Free(id, tick));
                dead.Add(id);
            }
            else if (allocator.TryAllocate(tick, out EntityId id))
            {
                Assert.DoesNotContain(id, alive);
                alive.Add(id);
                dead.Remove(id);
            }

            Assert.Equal(alive.Count, allocator.Count);
            if (tick % 100 == 0)
            {
                foreach (EntityId id in alive)
                {
                    Assert.True(allocator.IsAlive(id));
                }

                foreach (EntityId id in dead)
                {
                    Assert.False(allocator.IsAlive(id));
                }
            }
        }
    }
}
