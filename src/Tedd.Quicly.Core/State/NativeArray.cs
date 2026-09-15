using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Tedd.Quicly.Core.State;

/// <summary>
/// A fixed-length array of unmanaged values in native memory, 64-byte aligned and zero-initialised.
/// </summary>
/// <remarks>
/// <para>Native memory never moves, so pointers into the array (for example the <c>QUIC_BUFFER</c> pair inside a
/// <see cref="SendEntry"/>) stay valid for the lifetime of the array and the GC never scans it. The array is
/// allocated with <see cref="NativeMemory.AlignedAlloc(nuint, nuint)"/> at a 64-byte boundary so that an element
/// whose size is a multiple of 64 bytes occupies whole cache lines.</para>
/// <para>The indexer is bounds-checked only in debug builds (<see cref="Debug.Assert(bool)"/>); <see cref="At"/>
/// is the always-checked accessor. <see cref="Dispose"/> frees the memory and is idempotent; a finalizer frees it
/// if the owner forgot. After disposal <see cref="Length"/> is zero and <see cref="Pointer"/> is
/// <see langword="null"/>.</para>
/// <para>Thread safety: the array itself is immutable after construction; concurrent access to its elements is
/// the caller's business (see the owning table's contract).</para>
/// </remarks>
/// <typeparam name="T">Element type; must be unmanaged so the array is a flat block of values.</typeparam>
public sealed unsafe class NativeArray<T> : IDisposable where T : unmanaged
{
    /// <summary>Alignment of the first element in bytes.</summary>
    public const int Alignment = 64;

    private T* _pointer;
    private int _length;
    private bool _disposed;

    /// <summary>Allocates a zeroed array of <paramref name="length"/> elements.</summary>
    /// <param name="length">Number of elements; zero is allowed and allocates nothing.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="length"/> is negative.</exception>
    /// <exception cref="OutOfMemoryException">The native allocation failed.</exception>
    public NativeArray(int length)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(length);
        _length = length;
        if (length == 0)
            return;

        nuint bytes = (nuint)length * (nuint)sizeof(T);
        _pointer = (T*)NativeMemory.AlignedAlloc(bytes, Alignment);
        NativeMemory.Clear(_pointer, bytes);
    }

    /// <summary>Frees the native memory if <see cref="Dispose"/> was never called.</summary>
    ~NativeArray() => Free();

    /// <summary>Number of elements. Zero after <see cref="Dispose"/>.</summary>
    public int Length => _length;

    /// <summary>Size of the array in bytes.</summary>
    public long ByteLength => (long)_length * sizeof(T);

    /// <summary>Pointer to the first element (64-byte aligned), or <see langword="null"/> when empty or disposed.</summary>
    public T* Pointer => _pointer;

    /// <summary>True once <see cref="Dispose"/> has run.</summary>
    public bool IsDisposed => _disposed;

    /// <summary>
    /// Reference to the element at <paramref name="index"/>. Bounds are asserted in debug builds only; use
    /// <see cref="At"/> when the index comes from untrusted input.
    /// </summary>
    /// <param name="index">Zero-based element index.</param>
    public ref T this[int index]
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            Debug.Assert((uint)index < (uint)_length, "NativeArray index out of range");
            return ref _pointer[index];
        }
    }

    /// <summary>Reference to the element at <paramref name="index"/>, bounds-checked in every build.</summary>
    /// <param name="index">Zero-based element index.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="index"/> is outside the array.</exception>
    public ref T At(int index)
    {
        if ((uint)index >= (uint)_length)
            ThrowIndexOutOfRange(index);
        return ref _pointer[index];
    }

    /// <summary>The whole array as a span. Valid until <see cref="Dispose"/>.</summary>
    public Span<T> AsSpan() => new(_pointer, _length);

    /// <summary>A slice of the array as a span, bounds-checked.</summary>
    /// <param name="start">First element.</param>
    /// <param name="length">Number of elements.</param>
    /// <exception cref="ArgumentOutOfRangeException">The slice is outside the array.</exception>
    public Span<T> AsSpan(int start, int length)
    {
        if ((uint)start > (uint)_length || (uint)length > (uint)(_length - start))
            ThrowIndexOutOfRange(start);
        return new Span<T>(_pointer + start, length);
    }

    /// <summary>Sets every element to its default (all-zero) value.</summary>
    public void Clear()
    {
        if (_length != 0)
            NativeMemory.Clear(_pointer, (nuint)_length * (nuint)sizeof(T));
    }

    /// <summary>Sets every element to <paramref name="value"/>.</summary>
    /// <param name="value">Value to store in each element.</param>
    public void Fill(T value) => AsSpan().Fill(value);

    /// <summary>Frees the native memory. Safe to call more than once; must not race with element access.</summary>
    public void Dispose()
    {
        Free();
        GC.SuppressFinalize(this);
    }

    private void Free()
    {
        T* p = _pointer;
        _pointer = null;
        _length = 0;
        _disposed = true;
        if (p is not null)
            NativeMemory.AlignedFree(p);
    }

    [DoesNotReturn]
    private static void ThrowIndexOutOfRange(int index) =>
        throw new ArgumentOutOfRangeException(nameof(index), index, "Index is outside the native array.");
}
