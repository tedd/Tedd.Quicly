using Tedd.Quicly.Core.Memory;

namespace Tedd.Quicly.Core.Tests.Memory;

public class SlabAllocatorOptionsTests
{
    [Fact]
    public void Defaults_Are_Seven_Classes_Totalling_16_MiB()
    {
        var options = new SlabAllocatorOptions();
        SizeClassDefinition[] classes = options.SizeClasses;
        Assert.Equal(7, classes.Length);
        Assert.Equal([64, 256, 1536, 4096, 16384, 65536, 262144], classes.Select(c => c.BlockSize).ToArray());
        long total = 0;
        foreach (SizeClassDefinition c in classes)
            total += c.SlabBytes;
        Assert.Equal(16L * 1024 * 1024, total);
    }

    [Fact]
    public void CreateDefaultSizeClasses_Returns_Fresh_Array()
    {
        SizeClassDefinition[] a = SlabAllocatorOptions.CreateDefaultSizeClasses();
        SizeClassDefinition[] b = SlabAllocatorOptions.CreateDefaultSizeClasses();
        Assert.NotSame(a, b);
        Assert.Equal(a, b);
    }

    [Fact]
    public void ValidateLeases_Default_Matches_Build_Configuration()
    {
        var options = new SlabAllocatorOptions();
#if DEBUG
        Assert.True(options.ValidateLeases);
#else
        Assert.False(options.ValidateLeases);
#endif
    }

    [Fact]
    public void SizeClassDefinition_SlabBytes()
    {
        var def = new SizeClassDefinition(1536, 2048);
        Assert.Equal(1536L * 2048, def.SlabBytes);
    }

    public static TheoryData<SizeClassDefinition[]?, string> InvalidClasses => new()
    {
        { null, "null" },
        { [], "At least one" },
        { Enumerable.Range(1, SlabAllocatorOptions.MaxSizeClasses + 1).Select(i => new SizeClassDefinition(i * 64, 1)).ToArray(), "At most" },
        { [new(0, 1)], "positive multiple" },
        { [new(-64, 1)], "positive multiple" },
        { [new(100, 1)], "positive multiple" },
        { [new(64, 1), new(64, 1)], "greater than the previous" },
        { [new(256, 1), new(64, 1)], "greater than the previous" },
        { [new(64, 0)], "BlockCount 0 must be positive" },
        { [new(64, -1)], "BlockCount -1 must be positive" },
        { [new(65536, int.MaxValue)], "exceeds" },
    };

    [Theory]
    [MemberData(nameof(InvalidClasses))]
    public void Invalid_Options_Throw(SizeClassDefinition[]? classes, string messageFragment)
    {
        var options = new SlabAllocatorOptions { SizeClasses = classes! };
        var ex = Assert.Throws<ArgumentException>(() => new SlabAllocator(options));
        Assert.Contains(messageFragment, ex.Message, StringComparison.Ordinal);
        Assert.Equal(nameof(SlabAllocatorOptions.SizeClasses), ex.ParamName);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(3)]
    [InlineData(6)]
    [InlineData(SlabAllocatorOptions.MaxFreeListShards * 2)]
    public void Invalid_Shard_Count_Throws(int shards)
    {
        var options = new SlabAllocatorOptions { FreeListShards = shards };
        var ex = Assert.Throws<ArgumentException>(() => new SlabAllocator(options));
        Assert.Equal(nameof(SlabAllocatorOptions.FreeListShards), ex.ParamName);
        Assert.Contains("power of two", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Default_Shard_Count_Is_Eight()
    {
        Assert.Equal(8, new SlabAllocatorOptions().FreeListShards);
        using var allocator = new SlabAllocator();
        Assert.Equal(8, allocator.ShardCount);
    }

    [Fact]
    public void Allocator_Copies_Classes_So_Later_Mutation_Has_No_Effect()
    {
        SizeClassDefinition[] classes = [new(64, 2), new(128, 2)];
        var options = new SlabAllocatorOptions { SizeClasses = classes };
        using var allocator = new SlabAllocator(options);
        classes[0] = new(4096, 1);
        Assert.Equal(64, allocator.SizeClasses[0].BlockSize);
        Assert.Equal(2, allocator.ClassCount);
        Assert.Equal(128, allocator.MaxBlockSize);
    }
}
