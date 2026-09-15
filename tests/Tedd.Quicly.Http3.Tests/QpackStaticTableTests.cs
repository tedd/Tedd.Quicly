using Tedd.Quicly.Http3.Qpack;
using Tedd.Quicly.Http3.Tests.Generation;

namespace Tedd.Quicly.Http3.Tests;

public class QpackStaticTableTests
{
    [Fact]
    public void Has_99_Entries_Matching_Reference_File()
    {
        Assert.Equal(99, QpackStaticTable.Count);
        string root = TableGenerator.FindRepositoryRoot();
        TableGenerator.ParseReference(File.ReadAllText(Path.Combine(root, "docs", "reference", "h3_tables.txt")), out var entries, out _, out _);
        for (int i = 0; i < entries.Count; i++)
        {
            Assert.True(QpackStaticTable.TryGetEntry(i, out ReadOnlySpan<byte> name, out ReadOnlySpan<byte> value));
            Assert.Equal(entries[i].Name, TestUtil.AsciiString(name));
            Assert.Equal(entries[i].Value, TestUtil.AsciiString(value));
            Assert.Equal(entries[i].Name, TestUtil.AsciiString(QpackStaticTable.GetName(i)));
            Assert.Equal(entries[i].Value, TestUtil.AsciiString(QpackStaticTable.GetValue(i)));
        }
    }

    [Fact]
    public void Well_Known_Indices()
    {
        Assert.True(QpackStaticTable.TryGetEntry(0, out var n, out var v));
        Assert.Equal(":authority", TestUtil.AsciiString(n));
        Assert.True(v.IsEmpty);
        Assert.True(QpackStaticTable.TryGetEntry(15, out n, out v));
        Assert.Equal(":method", TestUtil.AsciiString(n));
        Assert.Equal("CONNECT", TestUtil.AsciiString(v));
        Assert.True(QpackStaticTable.TryGetEntry(25, out n, out v));
        Assert.Equal(":status", TestUtil.AsciiString(n));
        Assert.Equal("200", TestUtil.AsciiString(v));
        Assert.True(QpackStaticTable.TryGetEntry(98, out n, out v));
        Assert.Equal("x-frame-options", TestUtil.AsciiString(n));
        Assert.Equal("sameorigin", TestUtil.AsciiString(v));
    }

    [Fact]
    public void Out_Of_Range_Index_Fails()
    {
        Assert.False(QpackStaticTable.TryGetEntry(99, out var n, out var v));
        Assert.True(n.IsEmpty);
        Assert.True(v.IsEmpty);
        Assert.False(QpackStaticTable.TryGetEntry(-1, out _, out _));
        Assert.False(QpackStaticTable.TryGetEntry(int.MaxValue, out _, out _));
    }

    [Fact]
    public void TryFind_Prefers_Exact_Match()
    {
        Assert.True(QpackStaticTable.TryFind(":method"u8, "GET"u8, out int index, out bool exact));
        Assert.Equal(17, index);
        Assert.True(exact);

        Assert.True(QpackStaticTable.TryFind(":status"u8, "500"u8, out index, out exact));
        Assert.Equal(71, index);
        Assert.True(exact);

        // Non-contiguous name group: :status 24-28 and 63-71; unknown value -> first name match.
        Assert.True(QpackStaticTable.TryFind(":status"u8, "418"u8, out index, out exact));
        Assert.Equal(24, index);
        Assert.False(exact);

        Assert.True(QpackStaticTable.TryFind("content-type"u8, "application/octet-stream"u8, out index, out exact));
        Assert.Equal(44, index);
        Assert.False(exact);

        // Same length as an existing value but different bytes.
        Assert.True(QpackStaticTable.TryFind(":method"u8, "GEX"u8, out index, out exact));
        Assert.Equal(15, index);
        Assert.False(exact);

        Assert.False(QpackStaticTable.TryFind("x-custom"u8, "1"u8, out index, out exact));
        Assert.Equal(-1, index);
        Assert.False(exact);

        // Same length as a table name but different bytes.
        Assert.False(QpackStaticTable.TryFind(":methoX"u8, "GET"u8, out _, out _));
    }

    [Fact]
    public void TryFindName()
    {
        Assert.True(QpackStaticTable.TryFindName("origin"u8, out int index));
        Assert.Equal(90, index);
        Assert.True(QpackStaticTable.TryFindName("cache-control"u8, out index));
        Assert.Equal(36, index);
        Assert.False(QpackStaticTable.TryFindName("nope"u8, out index));
        Assert.Equal(-1, index);
        Assert.False(QpackStaticTable.TryFindName("origix"u8, out _));
    }
}
