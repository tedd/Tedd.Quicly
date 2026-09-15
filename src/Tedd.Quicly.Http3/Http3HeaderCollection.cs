namespace Tedd.Quicly.Http3;

/// <summary>Location of one field line's name and value inside an <see cref="Http3HeaderCollection"/> buffer.</summary>
public readonly struct Http3HeaderEntry
{
    /// <summary>Offset of the name in the collection buffer.</summary>
    public readonly int NameOffset;
    /// <summary>Length of the name in bytes.</summary>
    public readonly int NameLength;
    /// <summary>Offset of the value in the collection buffer.</summary>
    public readonly int ValueOffset;
    /// <summary>Length of the value in bytes.</summary>
    public readonly int ValueLength;

    /// <summary>Creates an entry.</summary>
    public Http3HeaderEntry(int nameOffset, int nameLength, int valueOffset, int valueLength)
    {
        NameOffset = nameOffset;
        NameLength = nameLength;
        ValueOffset = valueOffset;
        ValueLength = valueLength;
    }
}

/// <summary>A (name, value) pair yielded while enumerating an <see cref="Http3HeaderCollection"/>.</summary>
public readonly ref struct Http3Header
{
    /// <summary>The field name (lower-case ASCII by HTTP/3 rules).</summary>
    public readonly ReadOnlySpan<byte> Name;
    /// <summary>The field value.</summary>
    public readonly ReadOnlySpan<byte> Value;

    /// <summary>Creates a header view.</summary>
    public Http3Header(ReadOnlySpan<byte> name, ReadOnlySpan<byte> value)
    {
        Name = name;
        Value = value;
    }
}

/// <summary>
/// A field list stored in a caller-provided byte buffer: names and values are copied contiguously into the
/// buffer and a fixed-size array of <see cref="Http3HeaderEntry"/> records their positions. Adding, looking up
/// and enumerating headers never allocates. Both buffers are allocated once (by the caller or the constructor)
/// and reused across messages via <see cref="Clear"/>.
/// </summary>
public sealed class Http3HeaderCollection
{
    /// <summary>Per-field overhead used when computing the field section size (RFC 9204 §4.5.1.1 / RFC 9114 §4.2.2).</summary>
    public const int FieldOverhead = 32;

    private readonly byte[] _buffer;
    private readonly Http3HeaderEntry[] _entries;
    private int _count;
    private int _used;
    private int _sectionSize;

    /// <summary>Creates a collection over <paramref name="buffer"/> holding at most <paramref name="maxHeaderCount"/> fields.</summary>
    public Http3HeaderCollection(byte[] buffer, int maxHeaderCount)
    {
        ArgumentNullException.ThrowIfNull(buffer);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maxHeaderCount);
        _buffer = buffer;
        _entries = new Http3HeaderEntry[maxHeaderCount];
    }

    /// <summary>Creates a collection with a new <paramref name="bufferSize"/>-byte buffer holding at most <paramref name="maxHeaderCount"/> fields.</summary>
    public Http3HeaderCollection(int bufferSize, int maxHeaderCount)
        : this(new byte[bufferSize], maxHeaderCount)
    {
    }

    /// <summary>Number of fields.</summary>
    public int Count => _count;

    /// <summary>Maximum number of fields.</summary>
    public int Capacity => _entries.Length;

    /// <summary>Size of the underlying byte buffer.</summary>
    public int BufferLength => _buffer.Length;

    /// <summary>Bytes of the buffer currently holding names and values.</summary>
    public int BytesUsed => _used;

    /// <summary>Sum over all fields of name length + value length + <see cref="FieldOverhead"/>.</summary>
    public int FieldSectionSize => _sectionSize;

    /// <summary>Returns the entry record at <paramref name="index"/>.</summary>
    public Http3HeaderEntry this[int index]
    {
        get
        {
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual((uint)index, (uint)_count);
            return _entries[index];
        }
    }

    /// <summary>Removes all fields (the buffers are kept).</summary>
    public void Clear()
    {
        _count = 0;
        _used = 0;
        _sectionSize = 0;
    }

    /// <summary>Returns the name of the field at <paramref name="index"/>.</summary>
    public ReadOnlySpan<byte> GetName(int index)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual((uint)index, (uint)_count);
        ref readonly Http3HeaderEntry e = ref _entries[index];
        return _buffer.AsSpan(e.NameOffset, e.NameLength);
    }

    /// <summary>Returns the value of the field at <paramref name="index"/>.</summary>
    public ReadOnlySpan<byte> GetValue(int index)
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual((uint)index, (uint)_count);
        ref readonly Http3HeaderEntry e = ref _entries[index];
        return _buffer.AsSpan(e.ValueOffset, e.ValueLength);
    }

    /// <summary>
    /// Appends a field, copying name and value into the buffer. Returns false (and changes nothing) when the
    /// entry array or the buffer is full.
    /// </summary>
    public bool TryAdd(ReadOnlySpan<byte> name, ReadOnlySpan<byte> value)
    {
        if (_count == _entries.Length) return false;
        if (_buffer.Length - _used < name.Length + value.Length) return false;
        name.CopyTo(_buffer.AsSpan(_used));
        value.CopyTo(_buffer.AsSpan(_used + name.Length));
        return TryCommit(name.Length, value.Length);
    }

    /// <summary>Finds the first field named <paramref name="name"/> (exact byte comparison) and returns its value.</summary>
    public bool TryGet(ReadOnlySpan<byte> name, out ReadOnlySpan<byte> value)
    {
        for (int i = 0; i < _count; i++)
        {
            ref readonly Http3HeaderEntry e = ref _entries[i];
            if (e.NameLength == name.Length && _buffer.AsSpan(e.NameOffset, e.NameLength).SequenceEqual(name))
            {
                value = _buffer.AsSpan(e.ValueOffset, e.ValueLength);
                return true;
            }
        }
        value = default;
        return false;
    }

    /// <summary>True when a field named <paramref name="name"/> exists.</summary>
    public bool Contains(ReadOnlySpan<byte> name) => TryGet(name, out _);

    /// <summary>Enumerates the fields in insertion order.</summary>
    public Enumerator GetEnumerator() => new(this);

    /// <summary>The free tail of the buffer, into which a decoder may write a name followed by a value before calling <see cref="TryCommit"/>.</summary>
    internal Span<byte> FreeSpace => _buffer.AsSpan(_used);

    /// <summary>
    /// Records a field whose name (<paramref name="nameLength"/> bytes) and value (<paramref name="valueLength"/>
    /// bytes) were already written at the start of <see cref="FreeSpace"/>. Returns false when the entry array is full.
    /// </summary>
    internal bool TryCommit(int nameLength, int valueLength)
    {
        if (_count == _entries.Length) return false;
        _entries[_count++] = new Http3HeaderEntry(_used, nameLength, _used + nameLength, valueLength);
        _used += nameLength + valueLength;
        _sectionSize += nameLength + valueLength + FieldOverhead;
        return true;
    }

    /// <summary>Allocation-free enumerator over an <see cref="Http3HeaderCollection"/>.</summary>
    public struct Enumerator
    {
        private readonly Http3HeaderCollection _owner;
        private int _index;

        internal Enumerator(Http3HeaderCollection owner)
        {
            _owner = owner;
            _index = -1;
        }

        /// <summary>The current field.</summary>
        public readonly Http3Header Current
        {
            get
            {
                ref readonly Http3HeaderEntry e = ref _owner._entries[_index];
                return new Http3Header(_owner._buffer.AsSpan(e.NameOffset, e.NameLength), _owner._buffer.AsSpan(e.ValueOffset, e.ValueLength));
            }
        }

        /// <summary>Advances to the next field.</summary>
        public bool MoveNext()
        {
            int next = _index + 1;
            if (next >= _owner._count) return false;
            _index = next;
            return true;
        }
    }
}
