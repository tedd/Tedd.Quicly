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
    /// UnreliableUnordered and UnreliableSequenced without <see cref="ChannelDefinition.CoalesceOnReceive"/>: a backlog
    /// that survives a <see cref="QuiclyPeer.Poll"/> undrained is bounded, and evicts its oldest messages (counted) when
    /// it is full. Held only while the application is draining the channel, and once — for one Poll interval — when a
    /// burst first fills the pool (<see cref="ReceiveQueues.TryAppendDatagram"/>).
    /// </summary>
    public const byte Datagram = 1;

    /// <summary>
    /// ReliableOrdered and ReliableUnordered: never dropped. Appended while a node is free and held by the peer when none
    /// is, which stops the receive ring and back-pressures the streams; a share of the pool is reserved for these channels
    /// (<see cref="ReceiveQueueLayout.ReliableNodes"/>) that the backlog of the <see cref="Datagram"/> class can never
    /// occupy.
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
/// nodes reserved; the rest, <see cref="DatagramNodes"/>, is the most the <em>backlog</em> of the
/// <see cref="ReceiveQueueClass.Datagram"/> channels may occupy — the messages of channels that were left undrained
/// across a <see cref="QuiclyPeer.Poll"/> (<see cref="ReceiveQueues.BeginPass"/>) — so a flooded unreliable channel that
/// nobody drains can never keep the nodes a reliable channel's backlog needs. That backlog also has a byte cap of a
/// quarter of the receive budget (<see cref="DatagramBytes"/>, counted in block sizes, which is what the budget is
/// charged): queues that nobody drains pin at most that, and at least three quarters of the budget stay for the ring,
/// reassembly, mailboxes and the reliable channels. Inside the backlog nothing is partitioned: a channel may use all of it
/// while it is alone, and <see cref="NodeFair"/> / <see cref="ByteFair"/> only decide <em>who</em> loses a message once
/// it is full. Messages that left the ring in the current Poll pass for a channel whose queue was empty when the pass
/// began are outside these limits: they are on their way to the <see cref="QuiclyPeer.Drain"/> that follows the Poll, and
/// were charged to the receive budget when they arrived.
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
/// <item>an unreliable channel (<see cref="ReceiveQueueClass.Datagram"/>) is queued with
/// <see cref="TryAppendDatagram"/>. Messages it left undrained across a Poll are its <em>backlog</em>: the backlog of all
/// such channels together is bounded (<see cref="ReceiveQueueLayout.DatagramNodes"/>,
/// <see cref="ReceiveQueueLayout.DatagramBytes"/>), and once it is full the oldest message of the channel furthest over
/// its share is evicted (counted per channel and per peer as <c>DrainQueueDrops</c>). A channel the application drains
/// loses nothing: its messages are queued while a node is free, and then held by the peer until its next
/// <see cref="QuiclyPeer.Drain"/>. A channel nobody drains is held like that once, for one Poll interval: the next Poll
/// finds it undrained and makes it backlog, and only a drain (or a handler) ends a backlog;</item>
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
/// the queues, the byte totals, the backlog marks, the drop counters and the handled-channel count — is the game
/// thread's; nothing is shared with the transport thread.
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
    // 1 for a Datagram-class channel without a handler that was not drained empty during the pass before the current one
    // and had messages queued when it began, or was backlogged already (BeginPass): everything it has queued is backlog,
    // under the class limits and open to eviction. Cleared only by a take that finds or leaves its queue empty, or by a
    // handler — not by an eviction to nothing.
    private readonly NativeArray<byte> _backlogged;
    // The pass (_pass) in which a take for the application last found or left the channel's queue empty.
    private readonly NativeArray<int> _emptiedPass;
    // Dense indices of the Datagram-class channels: the only ones a pass start or a victim scan has to look at.
    private readonly NativeArray<int> _datagramChannels;
    private readonly int _datagramChannelCount;
    private readonly int _channels;
    private readonly int _datagramNodes;
    private readonly int _datagramBytes;
    private readonly int _nodeFair;
    private readonly int _byteFair;
    private int _free;
    // Messages queued for Datagram-class channels, with or without a handler (whether a pass start has anything to mark).
    private int _usedDatagram;
    // Totals over the backlogged channels: what DatagramNodes and DatagramBytes limit.
    private int _backlogNodes;
    private long _backlogBytes;
    // Pass counter: BeginPass starts the next one. Only ever compared for equality, so its wrap is harmless.
    private int _pass;
    // No pass has started since the queues were created or released (a new session): the next one is the session's first.
    private bool _sessionStart = true;
    // The backlogged channel last seen largest, per pressured resource (-1: none): while it stays strictly over its fair
    // share, and larger than the appender, it is the victim without a scan.
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
        _backlogged = new NativeArray<byte>(slots);
        _emptiedPass = new NativeArray<int>(slots);
        _datagramChannels = new NativeArray<int>(slots);
        _head.Fill(-1);
        _tail.Fill(-1);
        _count.Fill(0);
        _bytes.Fill(0);
        _drops.Fill(0);
        _class.Fill(ReceiveQueueClass.Other);
        _handled.Fill(0);
        _backlogged.Fill(0);
        _emptiedPass.Fill(0);
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
    /// to the queues, so that every way an entry leaves them keeps the count exact: a count that stayed positive over
    /// empty queues would keep a host that polls while there is work awake for good.
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
    /// at all (<see cref="TryAppendDatagram"/>, <see cref="BeginPass"/>): a total since the queues were created.
    /// </summary>
    /// <param name="channelIndex">Dense channel index.</param>
    public long Drops(int channelIndex) => _drops[channelIndex];

    /// <summary>Whether <paramref name="channelIndex"/> is queued with <see cref="TryAppendDatagram"/> (class <see cref="ReceiveQueueClass.Datagram"/>).</summary>
    /// <param name="channelIndex">Dense channel index.</param>
    public bool IsDatagram(int channelIndex) => _class[channelIndex] == ReceiveQueueClass.Datagram;

    /// <summary>
    /// Whether everything queued for <paramref name="channelIndex"/> is backlog: the channel is unreliable, has no handler
    /// and has not been drained empty since it was left undrained through a whole pass (<see cref="BeginPass"/>).
    /// </summary>
    /// <param name="channelIndex">Dense channel index.</param>
    public bool IsBacklogged(int channelIndex) => _backlogged[channelIndex] != 0;

    /// <summary>Messages queued for backlogged channels (at most <see cref="ReceiveQueueLayout.DatagramNodes"/>).</summary>
    public int BacklogNodes => _backlogNodes;

    /// <summary>Block bytes queued for backlogged channels (at most <see cref="ReceiveQueueLayout.DatagramBytes"/>, or one message).</summary>
    public long BacklogBytes => _backlogBytes;

    /// <summary>
    /// Records that <paramref name="channelIndex"/> got or lost its handler (<see cref="QueuedHandled"/>). A channel that
    /// gets a handler stops being backlog: the next <see cref="QuiclyPeer.Poll"/> dispatches what it has queued. One that
    /// loses its handler is judged at the next pass start like any other.
    /// </summary>
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
        if (handled && _backlogged[channelIndex] != 0)
        {
            _backlogged[channelIndex] = 0;
            _backlogNodes -= _count[channelIndex];
            _backlogBytes -= _bytes[channelIndex];
        }
    }

    /// <summary>
    /// Starts a pass (game thread, once at the start of every <see cref="QuiclyPeer.Poll"/> that dispatches): decides which
    /// unreliable channels are backlogged and cuts their backlog down to the class limits.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A channel of class <see cref="ReceiveQueueClass.Datagram"/> without a handler becomes <em>backlogged</em> at a
    /// pass start when its queue is not empty and no <see cref="TryTake"/> found or left it empty during the pass that
    /// ends here: the application had a whole Poll interval and did not drain what was queued for it. A host that drains
    /// its channels every frame never gets there, whatever the order of its Poll and Drain calls — every queue was empty
    /// at some point of the frame — so nothing it could have received on the receive ring and the receive budget is
    /// evicted: between the Poll and the Drain the messages wait here while a node is free, then in the held slot and
    /// the ring, as they did before the backlog was bounded.
    /// </para>
    /// <para>
    /// A backlog ends only when a <see cref="TryTake"/> finds or leaves the channel's queue empty (the application came
    /// back), or the channel gets a handler. It does <em>not</em> end when other channels evicted the queue to nothing:
    /// such a channel would otherwise count as new at the next pass start, fill the pool again by evicting the others'
    /// backlog and be held — and two channels nobody drains would take turns at closing the ring in every Poll interval.
    /// Keeping the mark, each undrained channel can be held once, and not again before it has been drained.
    /// </para>
    /// <para>
    /// The first pass of a session (the first after the queues were created or released) is the one in which the peer
    /// became Connected. No application could drain before it — <c>QuiclyClient.ConnectAsync</c> runs that Poll itself,
    /// before the caller has the peer — so every channel enters it as drained: what it queues is judged at the second
    /// pass start after it, like the burst of any frame of a host that polls and then drains.
    /// </para>
    /// <para>
    /// The backlog of all backlogged channels together is limited to <see cref="ReceiveQueueLayout.DatagramNodes"/>
    /// nodes and <see cref="ReceiveQueueLayout.DatagramBytes"/> block bytes (one message always fits). What exceeds that
    /// now — a burst that a pass queued for a channel that was then not drained — is evicted here, oldest first, from the
    /// channel with the most (<see cref="Drops"/>, <see cref="PeerCounters.DrainQueueDrops"/>). So a channel nobody
    /// drains holds more than those limits for one Poll interval at most — two after its last drain, or after the
    /// session began — and that excess was in the receive ring, already charged to the budget, when the pass that queued
    /// it began.
    /// </para>
    /// <para>
    /// Costs one compare while no unreliable channel has anything queued; otherwise one walk over the unreliable channels
    /// of the table.
    /// </para>
    /// </remarks>
    /// <param name="core">Owner of the receive budget and the peer counters.</param>
    public void BeginPass(PeerCore core)
    {
        int previous = _pass;
        int pass = unchecked(previous + 1);
        _pass = pass;
        if (_sessionStart)
        {
            // The first pass of a session: nobody could drain before it, so every channel enters it as drained.
            _sessionStart = false;
            for (int i = 0; i < _datagramChannelCount; i++)
            {
                _emptiedPass[_datagramChannels[i]] = pass;
            }

            return;
        }

        if (_usedDatagram == 0)
        {
            // Nothing queued: the totals are zero, and every mark is as the last TryTake or SetHandled left it.
            return;
        }

        int nodes = 0;
        long bytes = 0;
        for (int i = 0; i < _datagramChannelCount; i++)
        {
            int ci = _datagramChannels[i];
            if (_handled[ci] != 0 || _emptiedPass[ci] == previous)
            {
                _backlogged[ci] = 0;
            }
            else if (_count[ci] > 0 || _backlogged[ci] != 0)
            {
                // Also a backlogged channel whose queue other channels evicted to nothing: only a drain ends a backlog.
                _backlogged[ci] = 1;
                nodes += _count[ci];
                bytes += _bytes[ci];
            }
        }

        _backlogNodes = nodes;
        _backlogBytes = bytes;
        while (true)
        {
            bool nodePressure = _backlogNodes > _datagramNodes;
            bool bytePressure = _backlogBytes > _datagramBytes && _backlogNodes > 1;
            if (!nodePressure && !bytePressure)
            {
                break;
            }

            Evict(PickVictim(-1, byBytes: bytePressure && !nodePressure), core);
        }
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
            if (_backlogged[channelIndex] != 0)
            {
                _backlogNodes++;
                _backlogBytes += length;
            }
        }

        return true;
    }

    /// <summary>
    /// Queues a message of an unreliable channel (class <see cref="ReceiveQueueClass.Datagram"/>). What happens when it does
    /// not fit depends on what the channel is in this pass (<see cref="BeginPass"/>):
    /// <list type="bullet">
    /// <item><b>backlogged</b> (no handler, left undrained through a whole pass and not drained empty since): the backlog
    /// is kept at its node and byte limits by returning the oldest message of a victim channel to
    /// <paramref name="core"/>, counted (<see cref="Drops"/>, <see cref="PeerCounters.DrainQueueDrops"/>), as often as it
    /// takes. The victim is the backlogged channel furthest over its fair share of the resource under pressure — the
    /// appender itself only when no other one is larger — so one flooded channel loses its own oldest messages, in O(1).
    /// Never held;</item>
    /// <item><b>current</b> (no handler, and not backlogged — the application drained it during the last pass, or the
    /// channel is new): queued while a node is free, outside the backlog limits. With the pool full, the oldest message of a
    /// backlogged channel makes room; with no backlog to evict the answer is <see langword="false"/> and the caller holds
    /// the message, as it does for a reliable channel. That hold ends at the channel's <see cref="QuiclyPeer.Drain"/>.
    /// For a message that was already held when a pass began <paramref name="mayHold"/> is <see langword="false"/>, and
    /// what happens then depends on whether the application is draining the channel: if a take found or left its queue
    /// empty since the pass before began, the message is held again — the host polls and then drains, and the Drain that
    /// follows takes the queue, this message and the ring, in order; if not, the pass start has just made the channel
    /// backlogged when it has anything queued (the first case applies), and a channel with nothing queued drops the
    /// message, counted, because the pool is full of messages that cannot be evicted. So a hold of a channel that is
    /// not drained never outlives the pass start after the next;</item>
    /// <item><b>handled</b> (a <see cref="QuiclyPeer.Drain"/> of another channel met it; the next
    /// <see cref="QuiclyPeer.Poll"/> dispatches it): never evicted and never dropped. Queued while a node is free, making
    /// room from the backlog of channels without a handler when there is one, and otherwise held.</item>
    /// </list>
    /// </summary>
    /// <remarks>
    /// Oldest first for both unreliable modes: a consumer that comes back gets the freshest messages, in order; on an
    /// UnreliableSequenced channel the transport thread has already moved the key's sequence on, so the older queued
    /// values are the stale ones. An empty backlog always takes one message of any size, and after an append the backlog
    /// pins at most the larger of <see cref="ReceiveQueueLayout.DatagramBytes"/> and that one message. Every loop ends
    /// because each eviction removes one queued message.
    /// </remarks>
    /// <param name="channelIndex">Dense index of a <see cref="ReceiveQueueClass.Datagram"/> channel.</param>
    /// <param name="entry">The message (its lease moves into the queue, or back to <paramref name="core"/>).</param>
    /// <param name="core">Owner of the receive budget and the peer counters.</param>
    /// <param name="mayHold">
    /// <see langword="true"/> for a message that left the ring, or was first held, in this pass;
    /// <see langword="false"/> for one that was already held when the pass began, which is held again only for a channel
    /// the application is draining.
    /// </param>
    /// <returns>
    /// <see langword="false"/> when the caller has to hold the message (nothing was done with it). <see langword="true"/>
    /// when it was queued — or, for a channel without a handler, dropped and counted because nothing could be evicted for
    /// it and it may not be held.
    /// </returns>
    public bool TryAppendDatagram(int channelIndex, in ReceiveEntry entry, PeerCore core, bool mayHold)
    {
        Debug.Assert(_class[channelIndex] == ReceiveQueueClass.Datagram, "only an unreliable channel is queued here");
        if (_backlogged[channelIndex] != 0)
        {
            int length = entry.Lease.Length;
            while (true)
            {
                bool nodePressure = _free < 0 || _backlogNodes >= _datagramNodes;
                bool bytePressure = _backlogBytes > 0 && _backlogBytes + length > _datagramBytes;
                if (!nodePressure && !bytePressure)
                {
                    break;
                }

                int victim = PickVictim(channelIndex, byBytes: bytePressure && !nodePressure);
                if (victim < 0)
                {
                    // The pool is full of messages that are not backlog (reliable, handled, current) and the backlog is
                    // empty: a backlogged channel never holds the ring, so its message goes.
                    DropNew(channelIndex, in entry, core);
                    return true;
                }

                Evict(victim, core);
            }
        }
        else
        {
            while (_free < 0)
            {
                int victim = PickVictim(-1, byBytes: false);
                if (victim >= 0)
                {
                    Evict(victim, core);
                }
                else if (mayHold || _handled[channelIndex] != 0 || DrainedSinceLastPass(channelIndex))
                {
                    return false;
                }
                else
                {
                    DropNew(channelIndex, in entry, core);
                    return true;
                }
            }
        }

        bool appended = TryAppend(channelIndex, in entry);
        Debug.Assert(appended, "a node is free after the eviction loop");
        return true;
    }

    /// <summary>
    /// Whether a take for the application found or left the queue of <paramref name="channelIndex"/> empty in the current
    /// pass or in the one before it (in the session's first pass, every channel): the application is draining the channel.
    /// </summary>
    private bool DrainedSinceLastPass(int channelIndex)
    {
        int emptied = _emptiedPass[channelIndex];
        return emptied == _pass || emptied == unchecked(_pass - 1);
    }

    private void DropNew(int channelIndex, in ReceiveEntry entry, PeerCore core)
    {
        core.ReturnReceive(in entry.Lease);
        _drops[channelIndex]++;
        core.Counters.DrainQueueDrops++;
    }

    private void Evict(int victim, PeerCore core)
    {
        bool taken = Take(victim, out ReceiveEntry oldest);
        Debug.Assert(taken, "a victim has a queued message");
        core.ReturnReceive(in oldest.Lease);
        _drops[victim]++;
        core.Counters.DrainQueueDrops++;
    }

    /// <summary>
    /// The backlogged channel that loses its oldest message, or -1 when no backlogged channel has one queued.
    /// <paramref name="appender"/> is the backlogged channel the room is for (-1: a channel that is not backlog, or a
    /// pass start): it is its own victim only when it is at or over its fair share and no other channel is known to be
    /// further over — so a channel that stays within its share is not cut while another one is the hog.
    /// </summary>
    private int PickVictim(int appender, bool byBytes)
    {
        ref int hintRef = ref _hintNodes;
        if (byBytes)
        {
            hintRef = ref _hintBytes;
        }

        int fair = byBytes ? _byteFair : _nodeFair;
        int own = appender < 0 ? -1 : (byBytes ? _bytes[appender] : _count[appender]);

        // The last known largest, while it is still backlog, strictly over its share and larger than the appender.
        int hint = hintRef;
        int hintSize = -1;
        if (hint >= 0 && hint != appender && _backlogged[hint] != 0 && _count[hint] > 0)
        {
            hintSize = byBytes ? _bytes[hint] : _count[hint];
            if (hintSize > fair && hintSize > own)
            {
                return hint;
            }
        }

        // The appender, at or over its share with nothing known to be larger: the common case of one flooded channel,
        // without a scan. It becomes the hint, so that another channel's append finds it.
        if (appender >= 0 && _count[appender] > 0 && own >= fair && own >= hintSize)
        {
            hintRef = appender;
            return appender;
        }

        int victim = -1;
        int most = -1;
        for (int i = 0; i < _datagramChannelCount; i++)
        {
            int ci = _datagramChannels[i];
            if (_backlogged[ci] == 0 || _count[ci] == 0)
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

        hintRef = victim;
        return victim;
    }

    /// <summary>
    /// Takes the oldest message of <paramref name="channelIndex"/> for the application (<see cref="QuiclyPeer.Drain"/>, or
    /// the dispatch to a handler). A take that empties a backlogged channel ends its backlog: the application caught up.
    /// </summary>
    /// <param name="channelIndex">Dense channel index.</param>
    /// <param name="entry">The message; its lease now belongs to the caller.</param>
    /// <returns><see langword="false"/> when the queue is empty.</returns>
    public bool TryTake(int channelIndex, out ReceiveEntry entry)
    {
        bool taken = Take(channelIndex, out entry);
        if (_count[channelIndex] == 0)
        {
            _backlogged[channelIndex] = 0;
            _emptiedPass[channelIndex] = _pass;
        }

        return taken;
    }

    /// <summary>
    /// A copy of the oldest message of <paramref name="channelIndex"/>, which stays queued (<see cref="QuiclyPeer.Drain"/>
    /// looks at a compressed message of a reliable channel before it takes it: it needs a buffer to decode it into).
    /// </summary>
    /// <param name="channelIndex">Dense channel index.</param>
    /// <param name="entry">The message; its lease still belongs to the queue.</param>
    /// <returns><see langword="false"/> when the queue is empty.</returns>
    public bool TryPeek(int channelIndex, out ReceiveEntry entry)
    {
        int node = _head[channelIndex];
        if (node < 0)
        {
            entry = default;
            return false;
        }

        entry = _nodes[node];
        return true;
    }

    private bool Take(int channelIndex, out ReceiveEntry entry)
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
            if (_backlogged[channelIndex] != 0)
            {
                _backlogNodes--;
                _backlogBytes -= length;
            }
        }

        return true;
    }

    /// <summary>
    /// Returns every queued lease to <paramref name="core"/> and forgets the backlog marks; the drop counters keep their
    /// totals.
    /// </summary>
    /// <param name="core">Owner of the receive budget.</param>
    public void ReleaseAll(PeerCore core)
    {
        for (int ci = 0; ci < _channels; ci++)
        {
            while (Take(ci, out ReceiveEntry entry))
            {
                core.ReturnReceive(entry.Lease);
            }
        }

        _backlogged.Fill(0);
        _backlogNodes = 0;
        _backlogBytes = 0;
        _sessionStart = true;
        _hintNodes = -1;
        _hintBytes = -1;
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
        _backlogged.Dispose();
        _emptiedPass.Dispose();
        _datagramChannels.Dispose();
    }
}
