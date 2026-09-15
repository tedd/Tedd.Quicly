using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Tedd.Quicly.Testing.Simulation;

/// <summary>
/// Power-of-two buckets of pinned (pinned object heap) arrays, so segment pointers handed to sinks never move and
/// steady-state traffic does not allocate. Single-threaded (used under the network lock).
/// </summary>
internal sealed class SimBufferPool
{
    private const int MinShift = 6;
    private readonly Stack<byte[]>[] _buckets = new Stack<byte[]>[31 - MinShift];

    public SimBufferPool()
    {
        for (int i = 0; i < _buckets.Length; i++)
            _buckets[i] = new Stack<byte[]>();
    }

    private static int BucketOf(int length)
    {
        if (length <= 1 << MinShift)
            return 0;
        return 32 - BitOperations.LeadingZeroCount((uint)length - 1) - MinShift;
    }

    public byte[] Rent(int minimumLength)
    {
        int bucket = BucketOf(minimumLength);
        if (_buckets[bucket].TryPop(out byte[]? array))
            return array;
        return GC.AllocateUninitializedArray<byte>(1 << (bucket + MinShift), pinned: true);
    }

    public void Return(byte[]? array)
    {
        if (array is null)
            return;
        int bucket = BucketOf(array.Length);
        if ((1 << (bucket + MinShift)) == array.Length)
            _buckets[bucket].Push(array);
    }

    /// <summary>Address of the first element of a pinned array.</summary>
    public static unsafe byte* AddressOf(byte[] pinnedArray) => (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(pinnedArray));
}
