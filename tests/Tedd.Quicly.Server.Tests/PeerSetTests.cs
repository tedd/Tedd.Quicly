using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Server.Tests;

public class PeerSetTests
{
    [Fact]
    public void Add_Remove_Contains_And_Count()
    {
        PeerSet set = new(130);
        Assert.Equal(130, set.Capacity);
        Assert.True(set.IsEmpty);
        Assert.True(set.Add(0));
        Assert.True(set.Add(64));
        Assert.True(set.Add(129));
        Assert.False(set.Add(64));
        Assert.Equal(3, set.Count);
        Assert.False(set.IsEmpty);
        Assert.True(set.Contains(0));
        Assert.True(set.Contains(129));
        Assert.False(set.Contains(1));
        Assert.False(set.Contains(-1));
        Assert.False(set.Contains(130));
        Assert.True(set.Remove(64));
        Assert.False(set.Remove(64));
        Assert.False(set.Remove(-5));
        Assert.False(set.Remove(1000));
        Assert.Equal(2, set.Count);
        Assert.Equal(3, set.Words.Length);
        Assert.Equal(1UL, set.Words[0]);
        Assert.Equal(2UL, set.Words[2]);
    }

    [Fact]
    public void Out_Of_Range_Adds_And_Capacities_Throw()
    {
        PeerSet set = new(10);
        Assert.Throws<ArgumentOutOfRangeException>(() => set.Add(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => set.Add(10));
        Assert.Throws<ArgumentOutOfRangeException>(() => new PeerSet(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new PeerSet(PeerSet.MaxCapacity + 1));
        Assert.Throws<ArgumentNullException>(() => set.Add((QuiclyPeer)null!));
        Assert.Throws<ArgumentNullException>(() => set.Remove((QuiclyPeer)null!));
        Assert.Throws<ArgumentNullException>(() => set.Contains((QuiclyPeer)null!));
        PeerSet empty = new(0);
        Assert.Equal(0, empty.Words.Length);
        foreach (int _ in empty)
        {
            Assert.Fail("An empty set has no members.");
        }
    }

    [Fact]
    public void Enumerates_Ascending_Across_Words()
    {
        PeerSet set = new(300);
        int[] members = [299, 0, 63, 64, 65, 128, 200];
        foreach (int index in members)
        {
            set.Add(index);
        }

        List<int> seen = [];
        foreach (int index in set)
        {
            seen.Add(index);
        }

        Assert.Equal([0, 63, 64, 65, 128, 200, 299], seen);
        PeerSet.Enumerator enumerator = set.GetEnumerator();
        while (enumerator.MoveNext())
        {
        }

        Assert.False(enumerator.MoveNext());
        Assert.Equal(-1, enumerator.Current);
    }

    [Fact]
    public void Set_Algebra()
    {
        PeerSet a = new(200);
        PeerSet b = new(100);
        foreach (int i in new[] { 1, 2, 3, 150 })
        {
            a.Add(i);
        }

        foreach (int i in new[] { 2, 3, 4 })
        {
            b.Add(i);
        }

        PeerSet union = new(200);
        union.CopyFrom(a);
        Assert.Equal(4, union.Count);
        union.UnionWith(b);
        Assert.Equal(5, union.Count);
        Assert.True(union.Contains(4));

        PeerSet intersection = new(200);
        intersection.CopyFrom(a);
        intersection.IntersectWith(b);
        Assert.Equal(2, intersection.Count);
        Assert.True(intersection.Contains(2));
        Assert.False(intersection.Contains(150)); // beyond b's words: cleared

        PeerSet difference = new(200);
        difference.CopyFrom(a);
        difference.ExceptWith(b);
        Assert.Equal(2, difference.Count);
        Assert.True(difference.Contains(1));
        Assert.True(difference.Contains(150));

        PeerSet copy = new(200);
        copy.Add(199);
        copy.CopyFrom(b);
        Assert.Equal(3, copy.Count);
        Assert.False(copy.Contains(199));

        Assert.Throws<ArgumentException>(() => b.CopyFrom(a));
        Assert.Throws<ArgumentException>(() => b.UnionWith(a));
        Assert.Throws<ArgumentException>(() => b.IntersectWith(a));
        Assert.Throws<ArgumentException>(() => b.ExceptWith(a));
        Assert.Throws<ArgumentNullException>(() => a.UnionWith(null!));

        a.Clear();
        Assert.True(a.IsEmpty);
        a.Clear();
        Assert.True(a.IsEmpty);
    }

    [Fact]
    public async Task Server_Sets_Are_Read_Only_And_Tracked_Sets_Lose_Released_Peers()
    {
        await using ServerFixture f = new();
        QuiclyPeer one = f.ConnectAdmitted();
        QuiclyPeer two = f.ConnectAdmitted();
        QuiclyPeer serverOne = f.ServerPeerOf(one);
        QuiclyPeer serverTwo = f.ServerPeerOf(two);

        PeerSet admitted = f.Server.AdmittedPeers;
        Assert.True(admitted.IsReadOnly);
        Assert.Equal(2, admitted.Count);
        Assert.True(admitted.Contains(serverOne));
        Assert.Throws<InvalidOperationException>(() => admitted.Add(5));
        Assert.Throws<InvalidOperationException>(() => admitted.Remove(serverOne.Index));
        Assert.Throws<InvalidOperationException>(() => admitted.Clear());
        Assert.Throws<InvalidOperationException>(() => admitted.CopyFrom(new PeerSet(1)));

        PeerSet tracked = f.Server.CreateSet();
        PeerSet untracked = new(f.Server.Capacity);
        Assert.Equal(f.Server.Capacity, tracked.Capacity);
        Assert.True(tracked.Add(serverOne));
        Assert.True(tracked.Add(serverTwo));
        untracked.Add(serverOne);
        untracked.Add(serverTwo);
        int indexOne = serverOne.Index;

        one.Close();
        Assert.True(f.RunUntil(() => f.Closed.Count == 1));
        Assert.False(tracked.Contains(indexOne));
        Assert.True(tracked.Contains(serverTwo));
        Assert.Equal(1, tracked.Count);
        Assert.True(untracked.Contains(indexOne));
        Assert.False(f.Server.AdmittedPeers.Contains(indexOne));
    }

    [Fact]
    public void Operations_Do_Not_Allocate()
    {
        PeerSet set = new(1024);
        PeerSet other = new(1024);
        for (int i = 0; i < 1024; i += 3)
        {
            other.Add(i);
        }

        int sum = 0;
        AllocationAssert.NoAllocations(() =>
        {
            for (int i = 0; i < 1024; i += 7)
            {
                set.Add(i);
            }

            foreach (int index in set)
            {
                sum += index;
            }

            set.UnionWith(other);
            set.ExceptWith(other);
            if (set.Contains(7))
            {
                set.Remove(7);
            }

            set.Clear();
        }, iterations: 200);
        Assert.True(sum > 0);
    }
}
