using System.Buffers;
using System.Collections;

namespace Tedd.Quicly.Http;

/// <summary>
/// A small, ordered, case-insensitive list of HTTP header fields. Duplicate names are allowed
/// (<see cref="TryGetValue"/> returns the first). Backed by a struct array; lookups are linear,
/// which beats hashing for the handful of headers a typical request carries.
/// </summary>
public sealed class HttpHeaderCollection : IEnumerable<HttpHeader>
{
    private static readonly SearchValues<char> InvalidNameChars = SearchValues.Create("\r\n\0: \t");
    private static readonly SearchValues<char> InvalidValueChars = SearchValues.Create("\r\n\0");

    private HttpHeader[] _items;
    private int _count;

    /// <summary>Creates an empty collection with room for <paramref name="capacity"/> headers before growing.</summary>
    public HttpHeaderCollection(int capacity = 16)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(capacity);
        _items = capacity == 0 ? [] : new HttpHeader[capacity];
    }

    /// <summary>Number of header fields.</summary>
    public int Count => _count;

    /// <summary>Gets the header at <paramref name="index"/>.</summary>
    public HttpHeader this[int index]
    {
        get
        {
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual((uint)index, (uint)_count);
            return _items[index];
        }
    }

    /// <summary>Gets the first value of <paramref name="name"/> (or <see langword="null"/>), or sets it (replacing every existing value; <see langword="null"/> removes).</summary>
    public string? this[string name]
    {
        get => TryGetValue(name, out var v) ? v : null;
        set
        {
            if (value is null)
                Remove(name);
            else
                Set(name, value);
        }
    }

    /// <summary>Returns the first value for <paramref name="name"/>.</summary>
    public bool TryGetValue(string name, out string value)
    {
        ArgumentNullException.ThrowIfNull(name);
        var items = _items;
        for (int i = 0; i < _count; i++)
        {
            if (string.Equals(items[i].Name, name, StringComparison.OrdinalIgnoreCase))
            {
                value = items[i].Value;
                return true;
            }
        }
        value = string.Empty;
        return false;
    }

    /// <summary>Whether a header named <paramref name="name"/> is present.</summary>
    public bool Contains(string name) => TryGetValue(name, out _);

    /// <summary>Appends a header (duplicates allowed).</summary>
    /// <exception cref="ArgumentException">The name is empty or the name/value contains characters that are illegal in a header line.</exception>
    public void Add(string name, string value)
    {
        Validate(name, value);
        AddUnchecked(name, value);
    }

    /// <summary>Replaces every header named <paramref name="name"/> with a single value.</summary>
    public void Set(string name, string value)
    {
        Validate(name, value);
        Remove(name);
        AddUnchecked(name, value);
    }

    /// <summary>Removes every header named <paramref name="name"/>.</summary>
    /// <returns>Whether anything was removed.</returns>
    public bool Remove(string name)
    {
        ArgumentNullException.ThrowIfNull(name);
        bool removed = false;
        int write = 0;
        var items = _items;
        for (int i = 0; i < _count; i++)
        {
            if (string.Equals(items[i].Name, name, StringComparison.OrdinalIgnoreCase))
            {
                removed = true;
                continue;
            }
            items[write++] = items[i];
        }
        for (int i = write; i < _count; i++)
            items[i] = default;
        _count = write;
        return removed;
    }

    /// <summary>Removes all headers.</summary>
    public void Clear()
    {
        Array.Clear(_items, 0, _count);
        _count = 0;
    }

    internal void AddUnchecked(string name, string value)
    {
        if (_count == _items.Length)
            Array.Resize(ref _items, Math.Max(8, _items.Length * 2));
        _items[_count++] = new HttpHeader(name, value);
    }

    private static void Validate(string name, string value)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);
        ArgumentNullException.ThrowIfNull(value);
        if (name.AsSpan().ContainsAny(InvalidNameChars))
            throw new ArgumentException("Header name contains an illegal character.", nameof(name));
        if (value.AsSpan().ContainsAny(InvalidValueChars))
            throw new ArgumentException("Header value contains CR, LF or NUL.", nameof(value));
    }

    /// <summary>Returns a non-allocating enumerator.</summary>
    public Enumerator GetEnumerator() => new(this);

    IEnumerator<HttpHeader> IEnumerable<HttpHeader>.GetEnumerator() => new Enumerator(this);

    IEnumerator IEnumerable.GetEnumerator() => new Enumerator(this);

    /// <summary>Struct enumerator over <see cref="HttpHeaderCollection"/>.</summary>
    public struct Enumerator : IEnumerator<HttpHeader>
    {
        private readonly HttpHeaderCollection _owner;
        private int _index;

        internal Enumerator(HttpHeaderCollection owner)
        {
            _owner = owner;
            _index = -1;
        }

        /// <inheritdoc/>
        public readonly HttpHeader Current => _owner._items[_index];

        readonly object IEnumerator.Current => Current;

        /// <inheritdoc/>
        public bool MoveNext() => ++_index < _owner._count;

        /// <inheritdoc/>
        public void Reset() => _index = -1;

        /// <inheritdoc/>
        public readonly void Dispose()
        {
        }
    }
}
