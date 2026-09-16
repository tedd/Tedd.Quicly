using System.Buffers.Binary;
using Tedd.Quicly.Core.Control;

namespace Tedd.Quicly.Server.Tests;

/// <summary>
/// The server's cache of spent session tokens (PROTOCOL.md §4.1): single use while an entry lives, entries that expire with
/// the grace period, the oldest entry evicted when the cache is full — and never a refused resume.
/// </summary>
public class SessionReplayCacheTests
{
    /// <summary>A token whose last 16 bytes (its HMAC in a real token, and all the cache looks at) name it uniquely.</summary>
    private static byte[] Token(ulong name)
    {
        byte[] token = new byte[SessionTokenAuthority.TokenLength];
        Span<byte> tail = token.AsSpan(token.Length - 16);
        BinaryPrimitives.WriteUInt64LittleEndian(tail, name);
        BinaryPrimitives.WriteUInt64LittleEndian(tail.Slice(8), 0xA5A5_0000ul + name);
        return token;
    }

    [Fact]
    public void A_Spent_Token_Is_Remembered_Until_Its_Lifetime_Ends()
    {
        SessionReplayCache cache = new(8, 1_000);
        byte[] token = Token(1);
        Assert.False(cache.Contains(token, 0));

        Assert.True(cache.TryConsume(token, 0));
        Assert.True(cache.Contains(token, 0));
        Assert.False(cache.TryConsume(token, 500)); // single use while the entry lives
        Assert.Equal(1, cache.Count);
        Assert.Equal(0, cache.Evictions);

        // One grace period later the entry is gone and the session registry's epoch check is the only guard left.
        Assert.False(cache.Contains(token, 1_000));
        Assert.True(cache.TryConsume(token, 1_000));
        Assert.Equal(1, cache.Count); // the expired entry was swept, not piled on
        Assert.Equal(0, cache.Evictions);
    }

    /// <summary>The evicted entry sits behind another one in its bucket (both names hash to the same chain).</summary>
    [Fact]
    public void A_Full_Cache_Evicts_The_Oldest_Entry_From_A_Chained_Bucket()
    {
        SessionReplayCache cache = new(2, 10_000);
        Assert.Equal(2, cache.Capacity);
        Assert.True(cache.TryConsume(Token(1), 0));
        Assert.True(cache.TryConsume(Token(3), 1)); // the same bucket: the chain is 3 -> 1

        Assert.True(cache.TryConsume(Token(5), 2)); // full of unexpired entries: the oldest goes, the resume is admitted
        Assert.Equal(2, cache.Count);
        Assert.Equal(1, cache.Evictions);
        Assert.False(cache.Contains(Token(1), 2));
        Assert.True(cache.Contains(Token(3), 2));
        Assert.True(cache.Contains(Token(5), 2));
    }

    /// <summary>The evicted entry is the only member of its bucket.</summary>
    [Fact]
    public void A_Full_Cache_Evicts_The_Oldest_Entry_From_Its_Own_Bucket()
    {
        SessionReplayCache cache = new(2, 10_000);
        Assert.True(cache.TryConsume(Token(1), 0));
        Assert.True(cache.TryConsume(Token(2), 1));

        Assert.True(cache.TryConsume(Token(4), 2)); // evicts Token(1), which is alone in its bucket
        Assert.Equal(1, cache.Evictions);
        Assert.False(cache.Contains(Token(1), 2));
        Assert.True(cache.Contains(Token(2), 2));
        Assert.True(cache.Contains(Token(4), 2));
    }

    [Fact]
    public void Sweeping_Drops_The_Entries_Whose_Lifetime_Ended()
    {
        SessionReplayCache cache = new(4, 100);
        Assert.True(cache.TryConsume(Token(1), 0));
        Assert.True(cache.TryConsume(Token(2), 50));

        cache.Sweep(60);
        Assert.Equal(2, cache.Count);
        cache.Sweep(100); // the first entry's lifetime ended
        Assert.Equal(1, cache.Count);
        cache.Sweep(1_000);
        Assert.Equal(0, cache.Count);
        Assert.Equal(0, cache.Evictions); // expiry is not eviction
        cache.Sweep(2_000);               // nothing left to drop
        Assert.Equal(0, cache.Count);
    }

    [Fact]
    public void The_Whole_Ring_Is_Reused_As_Tokens_Come_And_Go()
    {
        SessionReplayCache cache = new(3, 1_000);
        for (ulong name = 0; name < 12; name++)
        {
            Assert.True(cache.TryConsume(Token(name), (long)name));
            Assert.True(cache.Contains(Token(name), (long)name));
        }

        Assert.Equal(3, cache.Count);
        Assert.Equal(9, cache.Evictions);
        Assert.True(cache.Contains(Token(11), 12));
        Assert.False(cache.Contains(Token(0), 12));
    }

    [Fact]
    public void Arguments_Are_Validated()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new SessionReplayCache(0, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new SessionReplayCache(SessionReplayCache.MaxCapacity + 1, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new SessionReplayCache(1, -1));
    }
}
