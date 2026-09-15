using System.Runtime.InteropServices;

namespace Tedd.Quicly.Replication.Tests;

/// <summary>
/// Memory between two inaccessible pages (Windows): a span returned by <see cref="Tail"/> ends exactly at the trailing
/// guard page and one returned by <see cref="Head"/> starts exactly after the leading one, so a read or write one
/// byte past the end, or one byte before the start, faults instead of passing silently. On other platforms it falls
/// back to a managed array (no fault, still a correct test).
/// </summary>
internal sealed unsafe class GuardedBuffer : IDisposable
{
    private const int PageSize = 4096;
    private readonly byte* _base;
    private readonly byte* _start;
    private readonly byte* _end;
    private readonly byte[]? _managed;

    public GuardedBuffer(int capacity)
    {
        Capacity = capacity;
        if (!OperatingSystem.IsWindows())
        {
            _managed = new byte[capacity];
            return;
        }

        int pages = Math.Max(1, (capacity + PageSize - 1) / PageSize);
        _base = (byte*)VirtualAlloc(0, (nuint)((pages + 2) * PageSize), 0x3000 /* COMMIT | RESERVE */, 0x04 /* READWRITE */);
        if (_base == null)
        {
            throw new InvalidOperationException("VirtualAlloc failed.");
        }

        _start = _base + PageSize;
        _end = _start + pages * PageSize;
        if (!VirtualProtect((nint)_base, PageSize, 0x01 /* NOACCESS */, out _)
            || !VirtualProtect((nint)_end, PageSize, 0x01 /* NOACCESS */, out _))
        {
            throw new InvalidOperationException("VirtualProtect failed.");
        }
    }

    public int Capacity { get; }

    /// <summary>A span of <paramref name="length"/> bytes that ends exactly at the trailing guard page.</summary>
    public Span<byte> Tail(int length)
    {
        ValidateLength(length);
        return _managed is not null ? _managed.AsSpan(Capacity - length, length) : new Span<byte>(_end - length, length);
    }

    /// <summary>A span of <paramref name="length"/> bytes that starts exactly after the leading guard page.</summary>
    public Span<byte> Head(int length)
    {
        ValidateLength(length);
        return _managed is not null ? _managed.AsSpan(0, length) : new Span<byte>(_start, length);
    }

    public Span<byte> CopyToTail(ReadOnlySpan<byte> data)
    {
        Span<byte> tail = Tail(data.Length);
        data.CopyTo(tail);
        return tail;
    }

    public Span<byte> CopyToHead(ReadOnlySpan<byte> data)
    {
        Span<byte> head = Head(data.Length);
        data.CopyTo(head);
        return head;
    }

    /// <summary>Copies to the head or the tail, alternating guards between calls of a fuzz loop.</summary>
    public Span<byte> CopyGuarded(ReadOnlySpan<byte> data, bool atHead) => atHead ? CopyToHead(data) : CopyToTail(data);

    private void ValidateLength(int length)
    {
        if ((uint)length > (uint)Capacity)
        {
            throw new ArgumentOutOfRangeException(nameof(length));
        }
    }

    public void Dispose()
    {
        if (_base != null)
        {
            VirtualFree((nint)_base, 0, 0x8000 /* RELEASE */);
        }
    }

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern nint VirtualAlloc(nint address, nuint size, uint allocationType, uint protect);

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool VirtualProtect(nint address, nuint size, uint newProtect, out uint oldProtect);

    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool VirtualFree(nint address, nuint size, uint freeType);
}

internal static class TestData
{
    public static byte[] Random(Random random, int length)
    {
        byte[] data = new byte[length];
        random.NextBytes(data);
        return data;
    }

    /// <summary>Random bytes where each byte is 0 with probability <paramref name="zeroProbability"/>.</summary>
    public static byte[] Sparse(Random random, int length, double zeroProbability)
    {
        byte[] data = new byte[length];
        for (int i = 0; i < data.Length; i++)
        {
            data[i] = random.NextDouble() < zeroProbability ? (byte)0 : (byte)random.Next(1, 256);
        }

        return data;
    }

    /// <summary>
    /// A copy of <paramref name="source"/> resized to <paramref name="length"/> (new bytes random) with each byte changed
    /// to a different value with probability <paramref name="density"/>.
    /// </summary>
    public static byte[] Mutate(Random random, byte[] source, int length, double density)
    {
        byte[] data = new byte[length];
        for (int i = 0; i < length; i++)
        {
            data[i] = i < source.Length ? source[i] : (byte)random.Next(256);
            if (random.NextDouble() < density)
            {
                data[i] ^= (byte)random.Next(1, 256);
            }
        }

        return data;
    }
}
