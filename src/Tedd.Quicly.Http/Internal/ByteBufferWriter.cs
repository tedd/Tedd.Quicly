using System.Buffers;
using System.Text;

namespace Tedd.Quicly.Http.Internal;

/// <summary>Append-only writer over a pooled byte array. Grows by renting a larger array; never allocates on the GC heap in steady state.</summary>
internal struct ByteBufferWriter : IDisposable
{
    private byte[] _buffer;
    private int _length;

    public ByteBufferWriter(int initialCapacity)
    {
        _buffer = ArrayPool<byte>.Shared.Rent(initialCapacity);
        _length = 0;
    }

    public readonly int Length => _length;

    public readonly int Capacity => _buffer.Length;

    public readonly ReadOnlyMemory<byte> WrittenMemory => _buffer.AsMemory(0, _length);

    public readonly ReadOnlySpan<byte> WrittenSpan => _buffer.AsSpan(0, _length);

    public void Clear() => _length = 0;

    public void Append(ReadOnlySpan<byte> bytes)
    {
        EnsureFree(bytes.Length);
        bytes.CopyTo(_buffer.AsSpan(_length));
        _length += bytes.Length;
    }

    public void Append(byte value)
    {
        EnsureFree(1);
        _buffer[_length++] = value;
    }

    /// <summary>Appends a string as Latin-1 (characters above U+00FF become '?').</summary>
    public void AppendLatin1(string text)
    {
        EnsureFree(text.Length);
        _length += Encoding.Latin1.GetBytes(text, _buffer.AsSpan(_length));
    }

    public void AppendInt64(long value)
    {
        EnsureFree(20);
        value.TryFormat(_buffer.AsSpan(_length), out int written, default, System.Globalization.CultureInfo.InvariantCulture);
        _length += written;
    }

    public void AppendHex(long value)
    {
        EnsureFree(16);
        value.TryFormat(_buffer.AsSpan(_length), out int written, "x", System.Globalization.CultureInfo.InvariantCulture);
        _length += written;
    }

    public void AppendCrLf()
    {
        EnsureFree(2);
        _buffer[_length] = (byte)'\r';
        _buffer[_length + 1] = (byte)'\n';
        _length += 2;
    }

    private void EnsureFree(int count)
    {
        if (_buffer.Length - _length >= count)
            return;
        int newSize = Math.Max(_buffer.Length * 2, _length + count);
        var next = ArrayPool<byte>.Shared.Rent(newSize);
        _buffer.AsSpan(0, _length).CopyTo(next);
        ArrayPool<byte>.Shared.Return(_buffer);
        _buffer = next;
    }

    public void Dispose()
    {
        var b = _buffer;
        _buffer = [];
        _length = 0;
        if (b.Length > 0)
            ArrayPool<byte>.Shared.Return(b);
    }
}
