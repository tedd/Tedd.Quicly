using Tedd.Quicly.Core.Memory;
using Tedd.Quicly.Core.Session;

namespace Tedd.Quicly.Core.Tests.Session;

/// <summary>Counts <see cref="IPeerWorkSignal.OnWork"/> calls (and can throw, to cover the host-fault path).</summary>
internal sealed class RecordingWorkSignal : IPeerWorkSignal
{
    public int Calls;
    public QuiclyPeer? Last;
    public bool Throw;

    public void OnWork(QuiclyPeer peer)
    {
        Calls++;
        Last = peer;
        if (Throw)
        {
            throw new InvalidOperationException("work signal fault");
        }
    }

    public int Take()
    {
        int calls = Calls;
        Calls = 0;
        return calls;
    }
}

/// <summary>A host-owned pool plus its reference-count table, for the <see cref="QuiclyPeer.SendShared"/> tests.</summary>
internal sealed class SharedPool : IDisposable
{
    public SharedPool()
    {
        Allocator = new SlabAllocator(PeerCore.CreateCompactAllocatorOptions());
        Table = new SharedLeaseTable(Allocator);
    }

    public SlabAllocator Allocator { get; }

    public SharedLeaseTable Table { get; }

    /// <summary>Rents a block, fills it with <paramref name="length"/> pattern bytes and shares it with one reference.</summary>
    public SharedLease Share(int length, byte seed = 1)
    {
        Assert.True(Allocator.TryRent(length, out BufferLease lease));
        Span<byte> span = Allocator.GetSpan(in lease);
        for (int i = 0; i < length; i++)
        {
            span[i] = (byte)(seed + i);
        }

        return Table.Share(in lease, 1);
    }

    public int Count(in SharedLease lease) => Table.GetReferenceCount(in lease);

    public void Dispose() => Allocator.Dispose();
}

/// <summary>Option presets the hook tests share: no pings, no heartbeat, so an idle peer is really idle.</summary>
internal static class QuietOptions
{
    public static void Apply(PeerOptions options)
    {
        options.PingInterval = TimeSpan.FromHours(1);
        options.FastPingInterval = TimeSpan.FromHours(1);
        options.FastLockDuration = TimeSpan.Zero;
        options.HeartbeatTimeout = TimeSpan.Zero;
        options.StreamIdleTimeout = TimeSpan.Zero;
    }
}
