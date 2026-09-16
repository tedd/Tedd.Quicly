using Tedd.Quicly.Core.State;

namespace Tedd.Quicly.Core.Session;

/// <summary>
/// Per-channel FIFO queues of complete received messages waiting for <see cref="QuiclyPeer.Drain"/> (game thread only).
/// <see cref="QuiclyPeer.Poll"/> moves messages of channels without a handler here from the receive ring, so one
/// Drain-style channel never blocks the ring for the others. A fixed node pool bounds it: when the pool is full, Poll
/// stops taking from the ring and the ring's own overflow policy applies.
/// </summary>
/// <remarks>
/// Built with the peer (never lazily in <see cref="QuiclyPeer.Poll"/>: ARCHITECTURE.md §3, nothing in the hot path
/// allocates after warm-up) and backed by native memory (ADR 0008 invariant 12), so a first message on a channel
/// without a handler costs nothing on the GC heap. The pool holds <see cref="NodesFor"/> messages: the receive ring's
/// capacity, capped at <see cref="MaxNodes"/> — a Drain-style consumer that falls further behind than that leaves the
/// rest in the ring, which is where back-pressure belongs.
/// </remarks>
internal sealed class ReceiveQueues : IDisposable
{
    /// <summary>Largest node pool built for a peer (messages held across all handler-less channels at once).</summary>
    public const int MaxNodes = 1024;

    private readonly NativeArray<ReceiveEntry> _nodes;
    private readonly NativeArray<int> _next;
    private readonly NativeArray<int> _head;
    private readonly NativeArray<int> _tail;
    private readonly NativeArray<int> _count;
    private readonly int _channels;
    private int _free;

    /// <summary>Creates queues for <paramref name="channels"/> channels over a pool of <paramref name="capacity"/> nodes.</summary>
    /// <param name="capacity">Pool size (messages held at once across all channels).</param>
    /// <param name="channels">Number of channels (dense indices).</param>
    public ReceiveQueues(int capacity, int channels)
    {
        _channels = channels;
        _nodes = new NativeArray<ReceiveEntry>(capacity);
        _next = new NativeArray<int>(capacity);
        for (int i = 0; i < capacity; i++)
        {
            _next[i] = i + 1 < capacity ? i + 1 : -1;
        }

        _free = capacity > 0 ? 0 : -1;
        _head = new NativeArray<int>(Math.Max(channels, 1));
        _tail = new NativeArray<int>(Math.Max(channels, 1));
        _count = new NativeArray<int>(Math.Max(channels, 1));
        _head.Fill(-1);
        _tail.Fill(-1);
    }

    /// <summary>Node pool size for a peer whose receive ring holds <paramref name="ringCapacity"/> messages.</summary>
    /// <param name="ringCapacity">The receive ring's capacity.</param>
    /// <returns>The pool size, at most <see cref="MaxNodes"/>.</returns>
    public static int NodesFor(int ringCapacity) => Math.Clamp(ringCapacity, 1, MaxNodes);

    /// <summary>Pool size.</summary>
    public int Capacity => _nodes.Length;

    /// <summary>Messages queued across all channels.</summary>
    public int Used { get; private set; }

    /// <summary>Messages queued for <paramref name="channelIndex"/>.</summary>
    /// <param name="channelIndex">Dense channel index.</param>
    public int Count(int channelIndex) => _count[channelIndex];

    /// <summary>Appends a message to the queue of <paramref name="channelIndex"/>.</summary>
    /// <param name="channelIndex">Dense channel index.</param>
    /// <param name="entry">The message (its lease moves into the queue).</param>
    /// <returns><see langword="false"/> when the pool is full.</returns>
    public bool TryAppend(int channelIndex, in ReceiveEntry entry)
    {
        int node = _free;
        if (node < 0)
        {
            return false;
        }

        _free = _next[node];
        _nodes[node] = entry;
        _next[node] = -1;
        int tail = _tail[channelIndex];
        if (tail < 0)
        {
            _head[channelIndex] = node;
        }
        else
        {
            _next[tail] = node;
        }

        _tail[channelIndex] = node;
        _count[channelIndex]++;
        Used++;
        return true;
    }

    /// <summary>Takes the oldest message of <paramref name="channelIndex"/>.</summary>
    /// <param name="channelIndex">Dense channel index.</param>
    /// <param name="entry">The message; its lease now belongs to the caller.</param>
    /// <returns><see langword="false"/> when the queue is empty.</returns>
    public bool TryTake(int channelIndex, out ReceiveEntry entry)
    {
        int node = _head[channelIndex];
        if (node < 0)
        {
            entry = default;
            return false;
        }

        entry = _nodes[node];
        int next = _next[node];
        _head[channelIndex] = next;
        if (next < 0)
        {
            _tail[channelIndex] = -1;
        }

        _count[channelIndex]--;
        _nodes[node] = default;
        _next[node] = _free;
        _free = node;
        Used--;
        return true;
    }

    /// <summary>Returns every queued lease to <paramref name="core"/>.</summary>
    /// <param name="core">Owner of the receive budget.</param>
    public void ReleaseAll(PeerCore core)
    {
        for (int ci = 0; ci < _channels; ci++)
        {
            while (TryTake(ci, out ReceiveEntry entry))
            {
                core.ReturnReceive(entry.Lease);
            }
        }
    }

    /// <summary>Frees the native memory (after <see cref="ReleaseAll"/>).</summary>
    public void Dispose()
    {
        _nodes.Dispose();
        _next.Dispose();
        _head.Dispose();
        _tail.Dispose();
        _count.Dispose();
    }
}
