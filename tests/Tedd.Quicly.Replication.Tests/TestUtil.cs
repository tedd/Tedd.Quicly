using System.Runtime.InteropServices;

namespace Tedd.Quicly.Replication.Tests;

/// <summary>
/// Memory whose end is immediately followed by an inaccessible page (Windows), so a read or write one byte past
/// the end of a span returned by <see cref="Tail"/> faults instead of passing silently. On other platforms it
/// falls back to a managed array (no fault, still a correct test).
/// </summary>
internal sealed unsafe class GuardedBuffer : IDisposable
{
    private const int PageSize = 4096;
    private readonly byte* _base;
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
        _base = (byte*)VirtualAlloc(0, (nuint)((pages + 1) * PageSize), 0x3000 /* COMMIT | RESERVE */, 0x04 /* READWRITE */);
        if (_base == null)
        {
            throw new InvalidOperationException("VirtualAlloc failed.");
        }

        _end = _base + pages * PageSize;
        if (!VirtualProtect((nint)_end, PageSize, 0x01 /* NOACCESS */, out _))
        {
            throw new InvalidOperationException("VirtualProtect failed.");
        }
    }

    public int Capacity { get; }

    /// <summary>A span of <paramref name="length"/> bytes that ends exactly at the guard page.</summary>
    public Span<byte> Tail(int length)
    {
        if ((uint)length > (uint)Capacity)
        {
            throw new ArgumentOutOfRangeException(nameof(length));
        }

        return _managed is not null ? _managed.AsSpan(Capacity - length, length) : new Span<byte>(_end - length, length);
    }

    public Span<byte> CopyToTail(ReadOnlySpan<byte> data)
    {
        Span<byte> tail = Tail(data.Length);
        data.CopyTo(tail);
        return tail;
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
