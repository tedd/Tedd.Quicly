using System.Diagnostics;
using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.State;

namespace Tedd.Quicly.Core.Session;

/// <summary>
/// How a channel's messages are queued when the channel has no handler, or when a <see cref="QuiclyPeer.Drain"/> of another
/// channel meets them (<see cref="ReceiveQueues"/>). Decided once from the channel definition.
/// </summary>
internal static class ReceiveQueueClass
{
    /// <summary>
    /// Any channel that is neither of the other two — a coalescing channel (its values wait in a mailbox, never here),
    /// ReliableLatest, Bulk — reaches the queues only through a replaced engine (<see cref="PeerOptions.EngineFactory"/>):
    /// appended while a node is free, held by the peer when none is.
    /// </summary>
    public const byte Other = 0;

    /// <summary>
    /// UnreliableUnordered and UnreliableSequenced without <see cref="ChannelDefinition.CoalesceOnReceive"/>: the backlog is
    /// bounded, and a message that does not fit evicts the oldest queued one (counted). Never held.
    /// </summary>
    public const byte Datagram = 1;

    /// <summary>
    /// ReliableOrdered and ReliableUnordered: never dropped. Appended while a node is free and held by the peer when none
    /// is, which stops the receive ring and back-pressures the streams; a share of the pool is reserved for these channels
    /// (<see cref="ReceiveQueueLayout.ReliableNodes"/>) that the <see cref="Datagram"/> class can never occupy.
    /// </summary>
    public const byte Reliable = 2;

    /// <summary>The class of <paramref name="channel"/>.</summary>
    /// <param name="channel">The channel definition.</param>
    public static byte Of(ChannelDefinition channel) => channel.Mode switch
    {
        ChannelMode.UnreliableUnordered or ChannelMode.UnreliableSequenced => channel.CoalesceOnReceive ? Other : Datagram,
        ChannelMode.ReliableOrdered or ChannelMode.ReliableUnordered => Reliable,
        _ => Other,
    };
}

/// <summary>
/// Sizes of a peer's drain queues (<see cref="ReceiveQueues"/>), computed once from the channel table, the receive ring's
/// capacity and the receive budget.
/// </summary>
/// <remarks>
/// The node pool is split in two. Each <see cref="ReceiveQueueClass.Reliable"/> channel has <see cref="ReliableNodes"/>
/// nodes reserved; the rest, <see cref="DatagramNodes"/>, is the most the <see cref="ReceiveQueueClass.Datagram"/> channels
/// may occupy together, so a flooded unreliable channel can never take the nodes a reliable channel's backlog needs. The
/// datagram channels also share a byte cap of a quarter of the receive budget (<see cref="DatagramBytes"/>, counted in
/// block sizes, which is what the budget is charged): a queue that nobody drains pins at most that, and at least three
/// quarters of the budget stay for the ring, reassembly, mailboxes and the reliable channels. Inside the datagram class
/// nothing is partitioned: a channel may use the whole sub-pool while it is alone, and <see cref="NodeFair"/> /
/// <see cref="ByteFair"/> only decide <em>who</em> loses a message once the sub-pool is full.
/// </remarks>
internal readonly struct ReceiveQueueLayout
{
    /// <summary>Creates a layout from explicit values (tests); <see cref="Compute"/> derives them for a peer.</summary>
    /// <param name="capacity">Node pool size.</param>
    /// <param name="reliableNodes">Nodes reserved per <see cref="ReceiveQueueClass.Reliable"/> channel.</param>
    /// <param name="datagramNodes">Most nodes the <see cref="ReceiveQueueClass.Datagram"/> channels occupy together.</param>
    /// <param name="datagramBytes">Most block bytes the <see cref="ReceiveQueueClass.Datagram"/> channels pin together.</param>
    /// <param name="nodeFair">A datagram channel's fair share of <paramref name="datagramNodes"/>.</param>
    /// <param name="byteFair">A datagram channel's fair share of <paramref name="datagramBytes"/>.</param>
    public ReceiveQueueLayout(int capacity, int reliableNodes, int datagramNodes, int datagramBytes, int nodeFair, int byteFair)
    {
        Capacity = capacity;
        ReliableNodes = reliableNodes;
        DatagramNodes = datagramNodes;
        DatagramBytes = datagramBytes;
        NodeFair = nodeFair;
        ByteFair = byteFair;
    }

    /// <summary>Node pool size (messages held at once across all channels).</summary>
    public int Capacity { get; }

    /// <summary>Nodes reserved for each <see cref="ReceiveQueueClass.Reliable"/> channel (0 without such a channel).</summary>
    public int ReliableNodes { get; }

    /// <summary>Most nodes the <see cref="ReceiveQueueClass.Datagram"/> channels occupy together.</summary>
    public int DatagramNodes { get; }

    /// <summary>Most block bytes the <see cref="ReceiveQueueClass.Datagram"/> channels pin together (one message always fits).</summary>
    public int DatagramBytes { get; }

    /// <summary>A datagram channel's fair share of <see cref="DatagramNodes"/> (0 without such a channel).</summary>
    public int NodeFair { get; }

    /// <summary>A datagram channel's fair share of <see cref="DatagramBytes"/> (0 without such a channel).</summary>
    public int ByteFair { get; }

    /// <summary>Computes the layout of a peer.</summary>
    /// <param name="channels">Every application channel, by dense index.</param>
    /// <param name="ringCapacity">The receive ring's capacity (<see cref="PeerOptions.ReceiveRingCapacity"/>).</param>
    /// <param name="receiveBudget">The receive budget in bytes (<see cref="PeerOptions.ReceiveBudgetBytes"/>).</param>
    /// <returns>The layout.</returns>
    public static ReceiveQueueLayout Compute(ReadOnlySpan<ChannelDefinition> channels, int ringCapacity, long receiveBudget)
    {
        int datagram = 0;
        int reliable = 0;
        for (int i = 0; i < channels.Length; i++)
        {
            byte queueClass = ReceiveQueueClass.Of(channels[i]);
            if (queueClass == ReceiveQueueClass.Datagram)
            {
                datagram++;
            }
            else if (queueClass == ReceiveQueueClass.Reliable)
            {
                reliable++;
            }
        }

        // At least two nodes per reliable channel, so that half the pool can be reserved for them (one each at the least)
        // and the datagram class still keeps the other half.
        int capacity = Math.Max(ReceiveQueues.NodesFor(ringCapacity), 2 * reliable);
        int reliableNodes = reliable == 0 ? 0 : Math.Max(1, capacity / 2 / reliable);
        int datagramNodes = capacity - (reliable * reliableNodes);
        int datagramBytes = (int)Math.Clamp(receiveBudget / 4, 1, int.MaxValue);
        int nodeFair = datagram == 0 ? 0 : Math.Max(1, datagramNodes / datagram);
        int byteFair = datagram == 0 ? 0 : Math.Max(1, datagramBytes / datagram);
        return new ReceiveQueueLayout(capacity, reliableNodes, datagramNodes, datagramBytes, nodeFair, byteFair);
    }
}

/// <summary>
/// Per-channel FIFO queues of complete received messages waiting for <see cref="QuiclyPeer.Drain"/> (game thread only).
/// <see cref="QuiclyPeer.Poll"/> moves messages of channels without a handler here from the receive ring, so a Drain-style
/// channel does not block the ring for the others. A fixed node pool bounds it, and what happens when a message does not fit
/// depends on the channel's <see cref="ReceiveQueueClass"/>:
/// <list type="bullet">
/// <item>an unreliable channel (<see cref="ReceiveQueueClass.Datagram"/>) loses its <em>oldest</em> queued message, or —
/// when the channel is under its fair share — the oldest message of the unreliable channel with the longest queue
/// (<see cref="AppendEvicting"/>; counted per channel and per peer as <c>DrainQueueDrops</c>). Such a channel never stops
/// the ring, however long nobody drains it;</item>
/// <item>a reliable channel (<see cref="ReceiveQueueClass.Reliable"/>) and a channel of a replaced engine
/// (<see cref="ReceiveQueueClass.Other"/>) lose nothing: <see cref="TryAppend"/> fails, the peer holds the message and
/// stops taking from the ring, and the ring's own rules apply (datagrams are dropped newest-first, streams are
/// back-pressured) until the application drains the channel or registers a handler.</item>
/// </list>
/// </summary>
/// <remarks>
/// Built with the peer (never lazily in <see cref="QuiclyPeer.Poll"/>: ARCHITECTURE.md §3, nothing in the hot path
/// allocates after warm-up) and backed by native memory (ADR 0008 invariant 12), so a first message on a channel
/// without a handler costs nothing on the GC heap. The pool holds <see cref="ReceiveQueueLayout.Capacity"/> messages: the
/// receive ring's capacity, capped at <see cref="MaxNodes"/> (and at least two per reliable channel). Everything here —
/// the queues, the byte totals, the drop counters and the handled-channel count — is the game thread's; nothing is shared
/// with the transport thread.
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
    // Block bytes queued per channel (the sum of the leases' lengths: what the receive budget is charged).
    private readonly NativeArray<int> _bytes;
    // Messages evicted from, or refused by, a channel's queue: lifetime totals (kept across a reconnect).
    private readonly NativeArray<long> _drops;
    private readonly NativeArray<byte> _class;
    private readonly NativeArray<byte> _handled;
    // Dense indices of the Datagram-class channels: the only ones a victim scan has to look at.
    private readonly NativeArray<int> _datagramChannels;
    private readonly int _datagramChannelCount;
    private readonly int _channels;
    private readonly int _datagramNodes;
    private readonly int _datagramBytes;
    private readonly int _nodeFair;
    private readonly int _byteFair;
    private int _free;
    private int _usedDatagram;
    private long _bytesDatagram;
    // The channel the last victim scan found largest, per pressured resource (-1: none): while it stays over its fair
    // share it is the victim without another scan.
    private int _hintNodes = -1;
    private int _hintBytes = -1;

    /// <summary>
    /// Creates queues for <paramref name="channels"/> channels over a pool of <paramref name="capacity"/> nodes, every
    /// channel of class <see cref="ReceiveQueueClass.Other"/>: no limit but the pool, nothing is ever evicted.
    /// </summary>
    /// <param name="capacity">Pool size (messages held at once across all channels).</param>
    /// <param name="channels">Number of channels (dense indices).</param>
    public ReceiveQueues(int capacity, int channels)
        : this(new ReceiveQueueLayout(capacity, 0, capacity, int.MaxValue, 0, 0), default, channels)
    {
    }

    /// <summary>Creates the queues of a peer.</summary>
    /// <param name="layout">The sizes (<see cref="ReceiveQueueLayout.Compute"/>).</param>
    /// <param name="classes">The <see cref="ReceiveQueueClass"/> of every channel, by dense index.</param>
    public ReceiveQueues(in ReceiveQueueLayout layout, ReadOnlySpan<byte> classes)
        : this(in layout, classes, classes.Length)
    {
    }

    private ReceiveQueues(in ReceiveQueueLayout layout, ReadOnlySpan<byte> classes, int channels)
    {
        int capacity = layout.Capacity;
        _channels = channels;
        _datagramNodes = layout.DatagramNodes;
        _datagramBytes = layout.DatagramBytes;
        _nodeFair = layout.NodeFair;
        _byteFair = layout.ByteFair;
        _nodes = new NativeArray<ReceiveEntry>(capacity);
        _next = new NativeArray<int>(capacity);
        for (int i = 0; i < capacity; i++)
        {
            _next[i] = i + 1 < capacity ? i + 1 : -1;
        }

        _free = capacity > 0 ? 0 : -1;
        int slots = Math.Max(channels, 1);
        _head = new NativeArray<int>(slots);
        _tail = new NativeArray<int>(slots);
        _count = new NativeArray<int>(slots);
        _bytes = new NativeArray<int>(slots);
        _drops = new NativeArray<long>(slots);
        _class = new NativeArray<byte>(slots);
        _handled = new NativeArray<byte>(slots);
        _datagramChannels = new NativeArray<int>(slots);
        _head.Fill(-1);
        _tail.Fill(-1);
        _count.Fill(0);
        _bytes.Fill(0);
        _drops.Fill(0);
        _class.Fill(ReceiveQueueClass.Other);
        _handled.Fill(0);
        for (int ci = 0; ci < classes.Length; ci++)
        {
            _class[ci] = classes[ci];
            if (classes[ci] == ReceiveQueueClass.Datagram)
            {
                _datagramChannels[_datagramChannelCount++] = ci;
            }
        }
    }

    /// <summary>Node pool size for a peer whose receive ring holds <paramref name="ringCapacity"/> messages.</summary>
    /// <param name="ringCapacity">The receive ring's capacity.</param>
    /// <returns>The pool size, at most <see cref="MaxNodes"/>.</returns>
    public static int NodesFor(int ringCapacity) => Math.Clamp(ringCapacity, 1, MaxNodes);

    /// <summary>Pool size.</summary>
    public int Capacity => _nodes.Length;

    /// <summary>Messages queued across all channels.</summary>
    public int Used { get; private set; }

    /// <summary>
    /// Messages queued for channels that have a handler (<see cref="SetHandled"/>): what the next
    /// <see cref="QuiclyPeer.Poll"/> dispatches from the queues, and part of the peer's pending-work probe. Kept here, next
    /// to the queues, because an eviction removes entries too: a count the peer kept by itself would stay positive for
    /// good after one, and a host that polls while there is work would never sleep again.
    /// </summary>
    public int QueuedHandled { get; private set; }

    /// <summary>Messages queued for <paramref name="channelIndex"/>.</summary>
    /// <param name="channelIndex">Dense channel index.</param>
    public int Count(int channelIndex) => _count[channelIndex];

    /// <summary>Block bytes queued for <paramref name="channelIndex"/> (the sum of its leases' lengths).</summary>
    /// <param name="channelIndex">Dense channel index.</param>
    public int Bytes(int channelIndex) => _bytes[channelIndex];

    /// <summary>
    /// Messages of <paramref name="channelIndex"/> that were queued and then dropped to make room, or could not be queued
    /// at all (<see cref="AppendEvicting"/>): a total since the queues were created.
    /// </summary>
    /// <param name="channelIndex">Dense channel index.</param>
    public long Drops(int channelIndex) => _drops[channelIndex];

    /// <summary>Whether <paramref name="channelIndex"/> is queued with <see cref="AppendEvicting"/> (class <see cref="ReceiveQueueClass.Datagram"/>).</summary>
    /// <param name="channelIndex">Dense channel index.</param>
    public bool IsEvicting(int channelIndex) => _class[channelIndex] == ReceiveQueueClass.Datagram;

    /// <summary>Records that <paramref name="channelIndex"/> got or lost its handler (<see cref="QueuedHandled"/>).</summary>
    /// <param name="channelIndex">Dense channel index.</param>
    /// <param name="handled">Whether the channel has a handler now.</param>
    public void SetHandled(int channelIndex, bool handled)
    {
        byte value = handled ? (byte)1 : (byte)0;
        if (_handled[channelIndex] == value)
        {
            return;
        }

        _handled[channelIndex] = value;
        QueuedHandled += handled ? _count[channelIndex] : -_count[channelIndex];
    }

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
        int length = entry.Lease.Length;
        _bytes[channelIndex] += length;
        Used++;
        QueuedHandled += _handled[channelIndex];
        if (_class[channelIndex] == ReceiveQueueClass.Datagram)
        {
            _usedDatagram++;
            _bytesDatagram += length;
        }

        return true;
    }

    /// <summary>
    /// Appends a message of an unreliable channel (class <see cref="ReceiveQueueClass.Datagram"/>), making room first when
    /// the class is at its node or byte limit, or the pool has no free node: the oldest queued message of a victim channel
    /// is returned to <paramref name="core"/> and counted (<see cref="Drops"/>, <see cref="PeerCounters.DrainQueueDrops"/>),
    /// as often as it takes. The victim is the appending channel itself when it is at or over its fair share of the
    /// resource under pressure — a flooded channel loses its own oldest messages, in O(1) — and otherwise the unreliable
    /// channel with the most queued, so a channel that is drained diligently is not evicted while another one is the hog.
    /// </summary>
    /// <remarks>
    /// Oldest first for both unreliable modes: a consumer that comes back gets the freshest messages, in order; on an
    /// UnreliableSequenced channel the transport thread has already moved the key's sequence on, so the older queued
    /// values are the stale ones. An empty class always takes one message of any size, and after an append the class pins
    /// at most the larger of <see cref="ReceiveQueueLayout.DatagramBytes"/> and that one message. The loop ends because
    /// every eviction removes one queued message.
    /// </remarks>
    /// <param name="channelIndex">Dense index of a <see cref="ReceiveQueueClass.Datagram"/> channel.</param>
    /// <param name="entry">The message (its lease moves into the queue, or back to <paramref name="core"/>).</param>
    /// <param name="core">Owner of the receive budget and the peer counters.</param>
    /// <returns>
    /// <see langword="false"/> when the message itself was dropped (returned and counted): the pool is full of messages of
    /// other classes and no unreliable channel has anything to evict.
    /// </returns>
    public bool AppendEvicting(int channelIndex, in ReceiveEntry entry, PeerCore core)
    {
        int length = entry.Lease.Length;
        while (true)
        {
            bool nodePressure = _free < 0 || _usedDatagram >= _datagramNodes;
            bool bytePressure = _bytesDatagram > 0 && _bytesDatagram + length > _datagramBytes;
            if (!nodePressure && !bytePressure)
            {
                break;
            }

            int victim = PickVictim(channelIndex, byBytes: bytePressure && !nodePressure);
            if (victim < 0)
            {
                core.ReturnReceive(in entry.Lease);
                _drops[channelIndex]++;
                core.Counters.DrainQueueDrops++;
                return false;
            }

            bool taken = TryTake(victim, out ReceiveEntry oldest);
            System.Diagnostics.Debug.Assert(taken, "a victim has a queued message");
            core.ReturnReceive(in oldest.Lease);
            _drops[victim]++;
            core.Counters.DrainQueueDrops++;
        }

        bool appended = TryAppend(channelIndex, in entry);
        System.Diagnostics.Debug.Assert(appended, "a node is free after the eviction loop");
        return appended;
    }

    /// <summary>
    /// The channel that loses its oldest message for an append on <paramref name="channelIndex"/>, or -1 when no unreliable
    /// channel has one queued.
    /// </summary>
    private int PickVictim(int channelIndex, bool byBytes)
    {
        // The appender, at or over its share: the common case of one flooded channel, without a scan.
        if (_count[channelIndex] > 0 && (byBytes ? _bytes[channelIndex] >= _byteFair : _count[channelIndex] >= _nodeFair))
        {
            return channelIndex;
        }

        // The last scan's largest, while it is still strictly over its share.
        int hint = byBytes ? _hintBytes : _hintNodes;
        if (hint >= 0 && _count[hint] > 0 && (byBytes ? _bytes[hint] > _byteFair : _count[hint] > _nodeFair))
        {
            return hint;
        }

        int victim = -1;
        int most = -1;
        for (int i = 0; i < _datagramChannelCount; i++)
        {
            int ci = _datagramChannels[i];
            if (_count[ci] == 0)
            {
                continue;
            }

            int size = byBytes ? _bytes[ci] : _count[ci];
            if (size > most)
            {
                most = size;
                victim = ci;
            }
        }

        if (byBytes)
        {
            _hintBytes = victim;
        }
        else
        {
            _hintNodes = victim;
        }

        return victim;
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
        int length = entry.Lease.Length;
        _bytes[channelIndex] -= length;
        _nodes[node] = default;
        _next[node] = _free;
        _free = node;
        Used--;
        QueuedHandled -= _handled[channelIndex];
        if (_class[channelIndex] == ReceiveQueueClass.Datagram)
        {
            _usedDatagram--;
            _bytesDatagram -= length;
        }

        return true;
    }

    /// <summary>Returns every queued lease to <paramref name="core"/>; the drop counters keep their totals.</summary>
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
        _bytes.Dispose();
        _drops.Dispose();
        _class.Dispose();
        _handled.Dispose();
        _datagramChannels.Dispose();
    }
}
