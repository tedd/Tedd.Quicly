namespace Tedd.Quicly.Http3.Tests;

public class Http3HeaderCollectionTests
{
    [Fact]
    public void Add_Get_Enumerate()
    {
        var h = new Http3HeaderCollection(256, 4);
        Assert.Equal(0, h.Count);
        Assert.Equal(4, h.Capacity);
        Assert.Equal(256, h.BufferLength);
        Assert.True(h.TryAdd(":method"u8, "GET"u8));
        Assert.True(h.TryAdd("accept"u8, "*/*"u8));
        Assert.True(h.TryAdd("accept"u8, "text/html"u8));
        Assert.Equal(3, h.Count);
        Assert.Equal(7 + 3 + 6 + 3 + 6 + 9, h.BytesUsed);
        Assert.Equal(3 * 32 + 7 + 3 + 6 + 3 + 6 + 9, h.FieldSectionSize);

        Assert.True(h.TryGet(":method"u8, out ReadOnlySpan<byte> v));
        Assert.Equal("GET", TestUtil.AsciiString(v));
        Assert.True(h.TryGet("accept"u8, out v));
        Assert.Equal("*/*", TestUtil.AsciiString(v)); // first match
        Assert.False(h.TryGet("missing"u8, out v));
        Assert.True(v.IsEmpty);
        Assert.False(h.TryGet("accepx"u8, out _)); // same length, different bytes
        Assert.True(h.Contains("accept"u8));
        Assert.False(h.Contains("x"u8));

        Assert.Equal("accept", TestUtil.AsciiString(h.GetName(1)));
        Assert.Equal("text/html", TestUtil.AsciiString(h.GetValue(2)));
        Http3HeaderEntry e = h[2];
        Assert.Equal(6, e.NameLength);
        Assert.Equal(9, e.ValueLength);
        Assert.Equal(e.NameOffset + 6, e.ValueOffset);

        int i = 0;
        foreach (Http3Header header in h)
        {
            Assert.Equal(TestUtil.AsciiString(h.GetName(i)), TestUtil.AsciiString(header.Name));
            Assert.Equal(TestUtil.AsciiString(h.GetValue(i)), TestUtil.AsciiString(header.Value));
            i++;
        }
        Assert.Equal(3, i);
    }

    [Fact]
    public void Clear_Resets_Everything()
    {
        var h = new Http3HeaderCollection(64, 2);
        Assert.True(h.TryAdd("a"u8, "b"u8));
        h.Clear();
        Assert.Equal(0, h.Count);
        Assert.Equal(0, h.BytesUsed);
        Assert.Equal(0, h.FieldSectionSize);
        Assert.False(h.TryGet("a"u8, out _));
        Assert.True(h.TryAdd("c"u8, "d"u8));
        Assert.Equal("d", TestUtil.AsciiString(h.GetValue(0)));
    }

    [Fact]
    public void TryAdd_Fails_When_Full()
    {
        var h = new Http3HeaderCollection(8, 1);
        Assert.True(h.TryAdd("ab"u8, "cd"u8));
        Assert.False(h.TryAdd("e"u8, "f"u8)); // entries full
        Assert.Equal(1, h.Count);

        var small = new Http3HeaderCollection(new byte[5], 4);
        Assert.False(small.TryAdd("abc"u8, "def"u8)); // 6 bytes > 5
        Assert.Equal(0, small.Count);
        Assert.Equal(0, small.BytesUsed);
        Assert.True(small.TryAdd("ab"u8, "cde"u8)); // exactly 5
        Assert.True(small.TryAdd(ReadOnlySpan<byte>.Empty, ReadOnlySpan<byte>.Empty)); // zero bytes still fit
        Assert.False(small.TryAdd("x"u8, ReadOnlySpan<byte>.Empty));
    }

    [Fact]
    public void Indexers_Throw_Out_Of_Range()
    {
        var h = new Http3HeaderCollection(64, 2);
        Assert.True(h.TryAdd("a"u8, "b"u8));
        Assert.Throws<ArgumentOutOfRangeException>(() => h[1]);
        Assert.Throws<ArgumentOutOfRangeException>(() => h[-1]);
        Assert.Throws<ArgumentOutOfRangeException>(() => h.GetName(1));
        Assert.Throws<ArgumentOutOfRangeException>(() => h.GetValue(1));
        Assert.Throws<ArgumentOutOfRangeException>(() => h.GetName(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => h.GetValue(-1));
    }

    [Fact]
    public void Constructor_Validates_Arguments()
    {
        Assert.Throws<ArgumentNullException>(() => new Http3HeaderCollection(null!, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => new Http3HeaderCollection(16, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new Http3HeaderCollection(16, -1));
    }

    [Fact]
    public void Enumerator_On_Empty_Collection()
    {
        var h = new Http3HeaderCollection(16, 1);
        Http3HeaderCollection.Enumerator e = h.GetEnumerator();
        Assert.False(e.MoveNext());
    }

    [Fact]
    public void Internal_Commit_Respects_Capacity()
    {
        var h = new Http3HeaderCollection(16, 1);
        Span<byte> free = h.FreeSpace;
        Assert.Equal(16, free.Length);
        free[0] = (byte)'x';
        free[1] = (byte)'y';
        Assert.True(h.TryCommit(1, 1));
        Assert.Equal("x", TestUtil.AsciiString(h.GetName(0)));
        Assert.Equal("y", TestUtil.AsciiString(h.GetValue(0)));
        Assert.Equal(14, h.FreeSpace.Length);
        Assert.False(h.TryCommit(1, 1));
    }
}
