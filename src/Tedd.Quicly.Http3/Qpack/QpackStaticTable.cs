namespace Tedd.Quicly.Http3.Qpack;

/// <summary>The QPACK static table (RFC 9204 Appendix A): 99 (name, value) entries addressed by index.</summary>
public static class QpackStaticTable
{
    /// <summary>Number of entries (99).</summary>
    public const int Count = QpackStaticTableData.EntryCount;

    /// <summary>Returns the name and value at <paramref name="index"/>, or false when the index is out of range.</summary>
    public static bool TryGetEntry(int index, out ReadOnlySpan<byte> name, out ReadOnlySpan<byte> value)
    {
        if ((uint)index >= Count)
        {
            name = default;
            value = default;
            return false;
        }
        ReadOnlySpan<ushort> layout = QpackStaticTableData.Layout.Slice(index * 4, 4);
        ReadOnlySpan<byte> blob = QpackStaticTableData.Blob;
        name = blob.Slice(layout[0], layout[1]);
        value = blob.Slice(layout[2], layout[3]);
        return true;
    }

    /// <summary>Returns the name at <paramref name="index"/>; the index must be valid.</summary>
    public static ReadOnlySpan<byte> GetName(int index)
    {
        ReadOnlySpan<ushort> layout = QpackStaticTableData.Layout.Slice(index * 4, 2);
        return QpackStaticTableData.Blob.Slice(layout[0], layout[1]);
    }

    /// <summary>Returns the value at <paramref name="index"/>; the index must be valid.</summary>
    public static ReadOnlySpan<byte> GetValue(int index)
    {
        ReadOnlySpan<ushort> layout = QpackStaticTableData.Layout.Slice(index * 4 + 2, 2);
        return QpackStaticTableData.Blob.Slice(layout[0], layout[1]);
    }

    /// <summary>
    /// Finds the best static-table reference for a field line: an exact (name, value) match if one exists
    /// (<paramref name="exactMatch"/> = true), otherwise the first entry with a matching name
    /// (<paramref name="exactMatch"/> = false). Returns false when no entry has the name.
    /// </summary>
    public static bool TryFind(ReadOnlySpan<byte> name, ReadOnlySpan<byte> value, out int index, out bool exactMatch)
    {
        ReadOnlySpan<ushort> layout = QpackStaticTableData.Layout;
        ReadOnlySpan<byte> blob = QpackStaticTableData.Blob;
        int nameIndex = -1;
        for (int i = 0; i < Count; i++)
        {
            int l = i * 4;
            if (layout[l + 1] != name.Length) continue;
            if (!blob.Slice(layout[l], layout[l + 1]).SequenceEqual(name)) continue;
            if (layout[l + 3] == value.Length && blob.Slice(layout[l + 2], layout[l + 3]).SequenceEqual(value))
            {
                index = i;
                exactMatch = true;
                return true;
            }
            if (nameIndex < 0) nameIndex = i;
        }
        index = nameIndex;
        exactMatch = false;
        return nameIndex >= 0;
    }

    /// <summary>Finds the first entry whose name matches. Returns false when none does.</summary>
    public static bool TryFindName(ReadOnlySpan<byte> name, out int index)
    {
        ReadOnlySpan<ushort> layout = QpackStaticTableData.Layout;
        ReadOnlySpan<byte> blob = QpackStaticTableData.Blob;
        for (int i = 0; i < Count; i++)
        {
            int l = i * 4;
            if (layout[l + 1] == name.Length && blob.Slice(layout[l], layout[l + 1]).SequenceEqual(name))
            {
                index = i;
                return true;
            }
        }
        index = -1;
        return false;
    }
}
