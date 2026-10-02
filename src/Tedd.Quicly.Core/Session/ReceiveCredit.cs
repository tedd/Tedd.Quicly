using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Tedd.Quicly.Core.State;
using Tedd.Quicly.Core.Threading;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Session;

/// <summary>How the application reads a reliable stream channel, which decides what may wait for it (<see cref="ReceiveCredit"/>).</summary>
internal enum CreditState : byte
{
    /// <summary>
    /// A handler takes the channel's messages at every <see cref="QuiclyPeer.Poll"/> and nothing is left in its drain queue:
    /// no limit but the receive ring's own.
    /// </summary>
    Handled = 0,

    /// <summary>
    /// Nobody reads the channel: it has no handler, and the application has not drained it empty since the Poll before
    /// last (or ever). It is held to its share: <see cref="ReceiveCredit.CountLimit"/> messages and
    /// <see cref="ReceiveCredit.ByteLimit"/> bytes of buffer blocks, and the byte share is strict — a message whose block
    /// does not fit in what is left of it, or in what the channels no handler reads leave of
    /// <see cref="ReceiveCredit.DrainedByteLimit"/>, waits in the transport, even on an empty channel, until the channel
    /// is read.
    /// A handler registered over a backlog keeps this until the backlog is dispatched.
    /// </summary>
    Unread = 1,

    /// <summary>
    /// The application drains the channel: <see cref="QuiclyPeer.Drain"/> emptied its queue since the Poll before last.
    /// Its limits are the ring's and the budget's: <see cref="ReceiveCredit.DrainedCountLimit"/> messages (the ring's
    /// capacity, so that the drain queues always have a node for what was accepted) and, once something waits,
    /// <see cref="ReceiveCredit.DrainedByteLimit"/> bytes of buffer blocks — half the receive budget, counted over every
    /// reliable channel no handler reads (this one, the other drained ones and the unread ones), because a channel keeps
    /// what it accepted when the application stops draining it: the channels drained and then abandoned, and the channels
    /// nobody reads, together keep at most that half, plus one message each, and the other half is what is left for the
    /// channels that are read. Channels with a handler do not count, and neither does a channel with nothing waiting. The
    /// limit is a threshold: a message is started while less than it waits, and one message is always accepted on an
    /// empty channel, whatever its size.
    /// </summary>
    Drained = 2,
}

/// <summary>The answer of <see cref="ReceiveCredit.TryTake"/>.</summary>
internal enum CreditTake : byte
{
    /// <summary>The channel is out of credit: the stream is held back (<see cref="Engines.StreamConsume.PendCredit"/>).</summary>
    Blocked = 0,

    /// <summary>The channel has a handler (or is not accounted): no limit, the message is staged at its wire length.</summary>
    Unlimited = 1,

    /// <summary>The channel is limited (no handler reads it) and has credit: the message is staged at its limited length.</summary>
    Limited = 2,
}

/// <summary>
/// Per-channel credit of the reliable stream channels (ReliableOrdered, ReliableUnordered): how many complete messages of
/// a channel, and how many bytes of them, may wait between the transport thread and the application.
/// </summary>
/// <remarks>
/// <para>
/// The receive ring is one FIFO for every channel, so a message the game thread cannot hand on — its channel has no
/// handler and the application does not <see cref="QuiclyPeer.Drain"/> it — has to move into the drain queues, and once
/// those are full the ring stops for every channel. A reliable message cannot be dropped to make room, so the limit has to
/// hold where the message is accepted: the engine asks <see cref="TryTake"/> before it starts a message, and a channel
/// that is out of credit answers <see cref="Engines.StreamConsume.PendCredit"/>, which leaves the bytes in the transport
/// and lets QUIC flow control hold that one stream's sender. The channel's other streams, and every other channel, keep
/// flowing.
/// </para>
/// <para>
/// Only a channel nobody reads is held to a share (<see cref="CreditState.Unread"/>), and its share is strict in bytes:
/// what the unread channels of a peer pin of the receive budget, for as long as nobody comes for it, is at most the
/// quarter <see cref="ByteLimitFor"/> divides among them. A channel with a handler has no limit: its messages leave the
/// ring at every <see cref="QuiclyPeer.Poll"/>, so the ring's own back-pressure is all it needs. A channel the
/// application drains is limited by the ring's capacity and by half the receive budget, shared with every other reliable
/// channel no handler reads (<see cref="DrainedByteLimit"/>): what it takes in a frame is what a handler would have been
/// given up to what the others leave of that half, and those channels together never keep more than that half (plus a
/// message each) if the application stops coming for them. A channel nobody reads is held to that half as well, next to
/// its strict share. Both are still counted, so that the count is right the moment the handler is removed or the
/// application stops draining.
/// </para>
/// <para>
/// Threads (ADR 0008 invariants 4 and 5). The transport thread owns what it took (<see cref="Taken"/>, with a private copy
/// of what came back) and only looks at the game thread's counters when its copies say the channel is blocked — by its
/// own limit, or by the half the channels no handler reads share (then it refreshes its copy of the peer-wide count of
/// what came back of that half). The game thread owns what it gave back and the limits; it reads the transport thread's
/// counters only to decide which held-back streams to resume. Both count in wrapping 32-bit arithmetic: the difference is
/// what matters, and it is bounded by the ring, the queues and the receive budget.
/// </para>
/// <para>
/// The shared half is counted peer-wide, on two cache lines (one per thread), so its check costs the same whatever the
/// number of channels. Which messages it counts is decided when each is taken: those of a channel that is limited then
/// (<see cref="CreditTake.Limited"/>). The message carries that tag to where its credit comes back
/// (<see cref="NoteReturned"/>, <see cref="Untake"/>), so every tagged take is paired with exactly one tagged return, or
/// cleared by <see cref="Reset"/>. Two consequences, bounded and never accumulating: a channel that gets a handler over its
/// backlog keeps that backlog counted until it is dispatched; and messages taken while a handler existed and left behind
/// by its removal are not in the half (they are bounded by the ring, that channel's own count and the receive budget).
/// </para>
/// <para>
/// A stream held back here is resumed by the game thread when its channel has credit again (or its limit is lifted), not
/// on every Poll: it waits in <see cref="_pended"/> and then in <see cref="_parked"/>. Both sides store, fence and then
/// load — the transport thread publishes the stream and re-reads the counters, the game thread publishes the counters and
/// re-reads the streams — so a stream cannot stay held back while its channel has credit.
/// </para>
/// <para>
/// The list is sized for the streams that are alive, but a stream that ends while it waits leaves its entry until the
/// game thread's next look, and its end gives the peer the stream back. A peer that resets the streams this end holds
/// back and opens new ones can therefore write more entries than it has streams. The list grows for that, by doubling,
/// until it is <see cref="ListGrowth"/> times its size (the rings it replaced keep what they held, so fewer than twice
/// that many entries fit in all). A peer that gets past that between two looks of the game thread is not one of this
/// library's senders (none resets a stream it started on these channels), and <see cref="NotePended"/> answers
/// <see langword="false"/>, on which the connection is closed.
/// </para>
/// </remarks>
internal sealed unsafe class ReceiveCredit : IDisposable
{
    /// <summary>The count limit of a channel that has a handler: never blocked, and never compared.</summary>
    public const int Unlimited = int.MaxValue;

    private readonly NativeArray<Taken> _taken;
    private readonly NativeArray<Returned> _returned;
    private readonly NativeArray<Limit> _limits;
    private readonly bool[] _enabled;
    private readonly byte[] _state;
    private readonly int _countLimit;
    private readonly int _byteLimit;
    private readonly int _drainedCountLimit;
    private readonly int _drainedByteLimit;

    // Block sizes of the receive pool, ascending: what a message of a given length takes of the receive budget.
    private readonly int[] _blockSizes;

    // Channels accounted (Enable; fixed once the engines are initialized). With fewer than two, a channel's own limit says
    // everything the shared half would.
    private int _accountedCount;

    // The half the channels no handler reads share, counted peer-wide (O(1) per message whatever the number of channels):
    // two cache lines of one native array. TransportLine is the transport thread's — the lease bytes of the messages it
    // took while their channel was limited (CreditTake.Limited, the "shared" tag a message carries to its return) and its
    // private copy of what came back of them; GameLine is the game thread's — what came back of them.
    private readonly NativeArray<SharedLine> _shared;
    private const int TransportLine = 0;
    private const int GameLine = 1;
    private NativeArray<Pended> _parked;
    private SpscRing<Pended> _pended;

    // Transport thread: the list's size for the streams that are alive; it grows to ListGrowth times that.
    private int _streamCapacity;

    // Rings SetStreamCapacity replaced: kept until Dispose and still read by the game thread, which may be reading one
    // and takes from them whatever was held back before the replacement. Published whole, never changed.
    private SpscRing<Pended>[]? _retiredPended;
    private int _parkedCount;

    // Game thread: a channel got credit back or had its limit lifted since the last Resume.
    private bool _changed;

    // Transport thread sets bits, game thread takes them: credit became available without the game thread's doing
    // (Look), or a stream ended while it was held back here (Look and Flush).
    private int _recheck;

    private const int Look = 1;
    private const int Flush = 2;

    /// <summary>
    /// How far the list of held-back streams grows beyond the streams that can be alive at once, for the entries of
    /// streams that ended while they waited and that the game thread has not collected yet.
    /// </summary>
    public const int ListGrowth = 8;

    /// <summary>Creates the credit state of a peer; every channel starts without accounting (<see cref="Enable"/>).</summary>
    /// <param name="channels">Number of channels (dense indices).</param>
    /// <param name="streams">Most streams the peer can have open at once (sizes the held-back stream lists).</param>
    /// <param name="countLimit">Messages a channel nobody reads may have waiting.</param>
    /// <param name="byteLimit">Lease bytes a channel nobody reads may have waiting before a further message is held back.</param>
    /// <param name="drainedCountLimit">Messages a channel the application drains may have waiting (the receive ring's capacity).</param>
    /// <param name="drainedByteLimit">
    /// Lease bytes the channels no handler reads may have waiting together: a channel the application drains starts no
    /// further message past it, and a channel nobody reads no message that does not fit in it.
    /// </param>
    /// <param name="blockSizes">
    /// Block sizes of the receive pool in ascending order (a message is counted with the smallest block that holds it), or
    /// <see langword="null"/> when a message is counted with its own length.
    /// </param>
    public ReceiveCredit(
        int channels,
        int streams,
        int countLimit,
        int byteLimit,
        int drainedCountLimit = Unlimited - 1,
        int drainedByteLimit = Unlimited - 1,
        ReadOnlySpan<int> blockSizes = default)
    {
        int count = Math.Max(channels, 1);

        // Whole cache lines for each of the three tables (eight entries of any of them are a multiple of 64 bytes): the
        // transport thread writes the first, the game thread the second, and the third is only read in the hot path, so
        // none of them may share its last line with whatever the allocator puts behind it.
        int slots = (count + 7) & ~7;
        _taken = new NativeArray<Taken>(slots);
        _returned = new NativeArray<Returned>(slots);
        _limits = new NativeArray<Limit>(slots);
        _limits.Fill(new Limit { Count = Unlimited, Bytes = Unlimited });
        _pended = new SpscRing<Pended>(streams);
        _streamCapacity = _pended.Capacity;
        _parked = new NativeArray<Pended>(_pended.Capacity);
        _enabled = new bool[count];
        _shared = new NativeArray<SharedLine>(2);
        _state = new byte[count];
        _countLimit = Math.Max(countLimit, 1);
        _byteLimit = Math.Max(byteLimit, 1);
        _drainedCountLimit = Math.Clamp(drainedCountLimit, _countLimit, Unlimited - 1);
        _drainedByteLimit = Math.Clamp(drainedByteLimit, _byteLimit, Unlimited - 1);
        _blockSizes = blockSizes.ToArray();
    }

    /// <summary>Messages a channel nobody reads may have waiting.</summary>
    public int CountLimit => _countLimit;

    /// <summary>Lease bytes a channel nobody reads may have waiting: a message that would take it past that is held back.</summary>
    public int ByteLimit => _byteLimit;

    /// <summary>Messages a channel the application drains may have waiting.</summary>
    public int DrainedCountLimit => _drainedCountLimit;

    /// <summary>
    /// Lease bytes the reliable channels no handler reads may have waiting together: a channel the application drains
    /// starts no further message once that much waits in them (its own waiting messages included), and a channel nobody
    /// reads no message whose block does not fit in what is left of it.
    /// </summary>
    public int DrainedByteLimit => _drainedByteLimit;

    /// <summary>Channels in <see cref="CreditState.Drained"/> (game thread): whether a pass start has any to judge.</summary>
    public int DrainedChannels { get; private set; }

    /// <summary>
    /// Lease bytes a channel nobody reads may have waiting: the reliable channels share a quarter of the receive
    /// budget, as the unreliable channels' drain queues do (<see cref="ReceiveQueueLayout.DatagramBytes"/>), which leaves
    /// half of it to the traffic that is read. The share is strict (<see cref="CreditState.Unread"/>): a message whose
    /// block is larger than the share is accepted only once the channel is read.
    /// </summary>
    /// <param name="receiveBudget">The receive budget (<see cref="PeerOptions.ReceiveBudgetBytes"/>).</param>
    /// <param name="reliableChannels">ReliableOrdered and ReliableUnordered channels in the table.</param>
    /// <returns>The limit, at least 1.</returns>
    public static int ByteLimitFor(long receiveBudget, int reliableChannels) =>
        reliableChannels == 0 ? 1 : (int)Math.Clamp(receiveBudget / 4 / reliableChannels, 1, int.MaxValue - 1);

    /// <summary>
    /// Lease bytes the reliable channels no handler reads may have waiting together (<see cref="DrainedByteLimit"/>): half
    /// of the receive budget. A channel keeps what it accepted when the application stops draining it, so the channels
    /// that were drained and then abandoned, with the channels nobody reads, pin at most that half (plus the one message
    /// each may start on top of the threshold), and the other half stays for the traffic that is read. It is shared, not
    /// divided: a channel with a handler, or with nothing waiting, leaves a drained channel all of it.
    /// </summary>
    /// <param name="receiveBudget">The receive budget (<see cref="PeerOptions.ReceiveBudgetBytes"/>).</param>
    /// <returns>The limit, at least 1.</returns>
    public static int DrainedByteLimitFor(long receiveBudget) => (int)Math.Clamp(receiveBudget / 2, 1, int.MaxValue - 1);

    // ------------------------------------------------------------------ construction (engine Initialize)

    /// <summary>
    /// Starts accounting for a channel (its engine's <see cref="Engines.ChannelEngine.Initialize"/>). It begins
    /// <see cref="CreditState.Unread"/>: nobody reads a channel until the application registers a handler or drains it.
    /// </summary>
    /// <param name="channel">Dense channel index.</param>
    public void Enable(int channel)
    {
        if (!_enabled[channel])
        {
            _accountedCount++;
        }

        _enabled[channel] = true;
        _state[channel] = (byte)CreditState.Unread;
        _limits[channel] = new Limit { Count = _countLimit, Bytes = -_byteLimit };
    }

    /// <summary>Whether a channel is accounted (any thread; fixed once the engines are initialized).</summary>
    /// <param name="channel">Dense channel index.</param>
    public bool IsEnabled(int channel) => _enabled[channel];

    /// <summary>
    /// <see cref="IsEnabled"/> by dense channel index, as the array itself: the game thread reads it for every message it
    /// hands on, and keeps the reference (never written after the engines are initialized).
    /// </summary>
    public bool[] EnabledChannels => _enabled;

    // ------------------------------------------------------------------ transport thread

    /// <summary>
    /// Whether the channel may start another message, and whether a limit applies to it (transport thread, before the ring
    /// reservation and the lease). Takes nothing: the message is counted by <see cref="NoteTaken"/> once it has its
    /// reservation and its lease.
    /// </summary>
    /// <remarks>
    /// One read of the channel's limit decides both: a channel with a handler (<see cref="CreditTake.Unlimited"/>) stages
    /// the message at <paramref name="length"/>, a limited one (<see cref="CreditTake.Limited"/>) at
    /// <paramref name="limitedLength"/> — a compressed message at its decoded size, so that its decode never waits for a
    /// second buffer (<see cref="PeerCore.LimitedStagingLength"/>) — and is judged by that length.
    /// </remarks>
    /// <param name="channel">Dense index of an enabled channel.</param>
    /// <param name="length">The message's payload length on the wire: what it takes of the receive budget when unlimited.</param>
    /// <param name="limitedLength">What it is staged in when the channel is limited: the block that holds it is what it takes.</param>
    /// <returns><see cref="CreditTake.Blocked"/> when the channel's waiting messages reached the limit of its <see cref="CreditState"/>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public CreditTake TryTake(int channel, int length, int limitedLength)
    {
        // A read-mostly line: the game thread writes it when a handler is registered or removed.
        int count = Volatile.Read(ref _limits[channel].Count);
        if (count == Unlimited)
        {
            return CreditTake.Unlimited;
        }

        return TryTakeLimited(channel, count, limitedLength) ? CreditTake.Limited : CreditTake.Blocked;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private bool TryTakeLimited(int channel, int count, int length)
    {
        ref Taken taken = ref _taken[channel];
        int bytes = Volatile.Read(ref _limits[channel].Bytes);

        // A strict byte limit (negative, see Limit) counts the message that asks: its block has to fit as well.
        int need = bytes < 0 ? BlockSizeOf(length) : 0;
        if (!IsBlocked(in taken, count, bytes, need) && !IsSharedFull(in taken, bytes, need, refresh: false))
        {
            return true;
        }

        // Only now the game thread's lines: the private copies say the channel, or the shared half, is full.
        Refresh(ref taken, channel);
        return !IsBlocked(in taken, count, bytes, need) && !IsSharedFull(in taken, bytes, need, refresh: true);
    }

    /// <summary>
    /// Whether the reliable channels no handler reads have <see cref="DrainedByteLimit"/> waiting between them (transport
    /// thread), by this thread's copy of what came back — which can only make them look fuller than they are — or, with
    /// <paramref name="refresh"/>, after refreshing the copy. A threshold (<paramref name="bytes"/> positive) lets a
    /// message start on an empty channel; a strict limit asks whether <paramref name="need"/> fits in what is left.
    /// </summary>
    /// <remarks>
    /// Constant time, whatever the number of channels: the half is counted peer-wide, by the messages that were taken
    /// while their channel was limited and have not come back (<see cref="NoteTaken"/>, <see cref="NoteReturned"/>). A
    /// message is counted there or not when it is taken, and stays so until it comes back: a channel that gets a handler
    /// over its backlog keeps that backlog counted until it is dispatched, and messages taken while a handler existed that
    /// a removed handler leaves behind are not counted (bounded by the ring, that channel's own count and the budget).
    /// </remarks>
    private bool IsSharedFull(in Taken own, int bytes, int need, bool refresh)
    {
        if (_accountedCount < 2 || (bytes > 0 && own.Count == own.SeenCount))
        {
            return false;
        }

        ref SharedLine line = ref _shared[TransportLine];
        if (refresh)
        {
            line.Seen = Volatile.Read(ref _shared[GameLine].Bytes);
        }

        uint waiting = unchecked(line.Bytes - line.Seen);
        return bytes < 0 ? (ulong)waiting + (uint)need > (uint)_drainedByteLimit : waiting >= (uint)_drainedByteLimit;
    }

    /// <summary>What a message of <paramref name="length"/> bytes takes of the receive budget: the smallest block that holds it.</summary>
    private int BlockSizeOf(int length)
    {
        if (length <= 0)
        {
            return 0; // an empty payload has no lease
        }

        int[] sizes = _blockSizes;
        for (int i = 0; i < sizes.Length; i++)
        {
            if (sizes[i] >= length)
            {
                return sizes[i];
            }
        }

        return length;
    }

    /// <summary>Counts a message that got its ring reservation and its lease (transport thread).</summary>
    /// <param name="channel">Dense index of an enabled channel.</param>
    /// <param name="leaseBytes">The lease's block size (0 for an empty payload): what the message holds of the receive budget.</param>
    /// <param name="shared">
    /// Whether the channel was limited when the message was taken (<see cref="CreditTake.Limited"/>): the message then counts
    /// in the half the channels no handler reads share until it comes back, and carries the tag to its
    /// <see cref="NoteReturned"/> or <see cref="Untake"/>.
    /// </param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void NoteTaken(int channel, int leaseBytes, bool shared)
    {
        ref Taken taken = ref _taken[channel];
        Volatile.Write(ref taken.Bytes, taken.Bytes + (uint)leaseBytes);
        Volatile.Write(ref taken.Count, taken.Count + 1);
        if (shared)
        {
            ref SharedLine line = ref _shared[TransportLine];
            Volatile.Write(ref line.Bytes, line.Bytes + (uint)leaseBytes);
        }
    }

    /// <summary>
    /// Takes back the count of a message that will not be published: its stream ended in the middle of it (transport
    /// thread; the game thread while a reconnect clears the engines).
    /// </summary>
    /// <param name="channel">Dense index of an enabled channel.</param>
    /// <param name="leaseBytes">What <see cref="NoteTaken"/> counted.</param>
    /// <param name="shared">The tag <see cref="NoteTaken"/> was given.</param>
    public void Untake(int channel, int leaseBytes, bool shared)
    {
        ref Taken taken = ref _taken[channel];
        Volatile.Write(ref taken.Count, taken.Count - 1);
        Volatile.Write(ref taken.Bytes, taken.Bytes - (uint)leaseBytes);
        if (shared)
        {
            ref SharedLine line = ref _shared[TransportLine];
            Volatile.Write(ref line.Bytes, line.Bytes - (uint)leaseBytes);
        }

        // Credit came back without the game thread: nothing it does will mark the channel, so the streams the channel
        // holds back would wait for a Drain that may have nothing to take. Ask for another look.
        Interlocked.Or(ref _recheck, Look);
    }

    /// <summary>
    /// Remembers a stream whose receive was held back by <see cref="TryTake"/> (transport thread). The game thread resumes
    /// it when its channel has credit again; if that happened while this call was under way, it is told to look again.
    /// </summary>
    /// <param name="id">The stream.</param>
    /// <param name="channel">Dense index of its channel.</param>
    /// <param name="length">
    /// Staging length of the message the stream waits with (the limited length <see cref="TryTake"/> was asked for, which the
    /// message's start asks for again when it is resumed).
    /// </param>
    /// <returns>
    /// <see langword="false"/> when the stream could not be listed: the list holds every stream the peer may have open and
    /// <see cref="ListGrowth"/> times as many that ended while they waited. Nothing will resume the stream then, so the
    /// caller closes the connection.
    /// </returns>
    public bool NotePended(TransportStreamId id, int channel, int length = 0)
    {
        ref Taken taken = ref _taken[channel];
        taken.Pends++;
        int need = BlockSizeOf(length);
        Pended pended = new() { Id = id, Channel = channel, Need = need };
        bool queued = _pended.TryEnqueue(in pended) || TryEnqueueGrowing(in pended);

        // Store, fence, load: the game thread stores its counters, fences and then loads this list (Resume), so one of the
        // two sees the other's store. Either the game thread finds the stream, or this finds the credit.
        Interlocked.MemoryBarrier();
        int count = Volatile.Read(ref _limits[channel].Count);
        if (count != Unlimited)
        {
            Refresh(ref taken, channel);
        }

        // Asked for the stream's own message: under a strict byte share a look for a message that still does not fit
        // would resume the stream only for it to be held back again, at every Poll. The shared half likewise: what the
        // other channels give back of it marks them changed (NoteReturned), and that brings the look.
        int bytes = Volatile.Read(ref _limits[channel].Bytes);
        int asked = bytes < 0 ? need : 0;
        if (count == Unlimited || (!IsBlocked(in taken, count, bytes, asked) && !IsSharedFull(in taken, bytes, asked, refresh: true)))
        {
            Interlocked.Or(ref _recheck, Look);
        }

        return queued;
    }

    /// <summary>
    /// The list is full of streams the game thread has not collected (transport thread): some of them ended while they
    /// waited, or every one of them is alive and this one is more than the peer may have open. Doubles the list while it is
    /// smaller than <see cref="ListGrowth"/> times the streams that can be alive.
    /// </summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private bool TryEnqueueGrowing(in Pended pended)
    {
        long limit = (long)_streamCapacity * ListGrowth;
        int capacity = _pended.Capacity;
        if (capacity >= limit)
        {
            return false;
        }

        ReplaceList((int)Math.Min(limit, 2L * capacity));
        return _pended.TryEnqueue(in pended);
    }

    /// <summary>
    /// A stream ended while it was held back here (transport thread). It is still listed, and <see cref="Resume"/> would
    /// spend a message of its channel's credit on it that nothing takes — with the channel's other streams left waiting for
    /// a Drain that may have nothing to return. So the game thread resumes everything it holds once: the transport ignores
    /// the streams that are gone, and the others are held back again if there is still no credit for them.
    /// </summary>
    public void NoteGone() => Interlocked.Or(ref _recheck, Look | Flush);

    /// <summary>Times a stream of the channel was held back for credit (any thread; a statistic, kept across a reconnect).</summary>
    /// <param name="channel">Dense channel index.</param>
    public long Pends(int channel) => Volatile.Read(ref _taken[channel].Pends);

    /// <summary>
    /// Makes room for every stream the peer can have open (transport thread, <see cref="PeerCore.SetTransportPeerStreams"/>,
    /// from <see cref="ITransportSink.OnConnected"/>): a transport may admit more streams by itself than the session's own
    /// limit, and each of them can be held back here. The ring that is replaced stays allocated until
    /// <see cref="Dispose"/> and stays readable: the game thread may be reading it, and <see cref="Resume"/> collects from
    /// it whatever was held back before the replacement (no in-tree transport reports its grant that late, and nothing
    /// here depends on it). The game thread's own list follows at its next <see cref="Resume"/>.
    /// </summary>
    /// <param name="streams">Streams that can be held back at once.</param>
    public void SetStreamCapacity(int streams)
    {
        int capacity = SpscRing<Pended>.RoundUpCapacity(Math.Max(streams, 1));
        if (capacity <= _streamCapacity)
        {
            return;
        }

        _streamCapacity = capacity;
        if (capacity > _pended.Capacity)
        {
            ReplaceList(capacity);
        }
    }

    /// <summary>
    /// Puts a larger ring in place of the list (transport thread). The ring that is replaced stays allocated until
    /// <see cref="Dispose"/> and stays readable, and it is published before the new one, so whoever sees the new ring sees
    /// the old one as well.
    /// </summary>
    private void ReplaceList(int capacity)
    {
        SpscRing<Pended>[] retired = _retiredPended ?? [];
        SpscRing<Pended>[] grown = new SpscRing<Pended>[retired.Length + 1];
        retired.CopyTo(grown, 0);
        grown[^1] = _pended;
        Volatile.Write(ref _retiredPended, grown);
        Volatile.Write(ref _pended, new SpscRing<Pended>(capacity));
    }

    /// <summary>Whether a replaced ring still holds a stream the game thread has not collected (any thread).</summary>
    private bool HasRetiredStreams
    {
        get
        {
            SpscRing<Pended>[]? retired = Volatile.Read(ref _retiredPended);
            if (retired is null)
            {
                return false;
            }

            foreach (SpscRing<Pended> ring in retired)
            {
                if (!ring.IsEmpty)
                {
                    return true;
                }
            }

            return false;
        }
    }

    /// <summary>
    /// Whether the channel takes nothing more, by the transport thread's own copy of what came back. A negative
    /// <paramref name="bytes"/> is a strict limit (<see cref="Limit"/>): the <paramref name="need"/> of the message that
    /// asks has to fit in it too. A positive one is a threshold for starting a message, and an empty channel takes one
    /// message of any size.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool IsBlocked(in Taken taken, int count, int bytes, int need)
    {
        uint waiting = unchecked(taken.Count - taken.SeenCount);
        if (waiting >= (uint)count)
        {
            return true;
        }

        uint waitingBytes = unchecked(taken.Bytes - taken.SeenBytes);
        if (bytes < 0)
        {
            // Nobody reads the channel: what it pins stays within its share, whatever the message's size.
            return (ulong)waitingBytes + (uint)need > (uint)-bytes;
        }

        return waiting != 0 && waitingBytes >= (uint)bytes;
    }

    private void Refresh(ref Taken taken, int channel)
    {
        // The count first, the bytes second, against the game thread's bytes first, count second: a copy taken between
        // the two stores has the older count, which can only make the channel look fuller than it is.
        ref Returned returned = ref _returned[channel];
        taken.SeenCount = Volatile.Read(ref returned.Count);
        taken.SeenBytes = Volatile.Read(ref returned.Bytes);
    }

    // ------------------------------------------------------------------ game thread

    /// <summary>
    /// Counts a message that reached the application — dispatched to a handler or taken by <see cref="QuiclyPeer.Drain"/> —
    /// or was discarded on the way (game thread).
    /// </summary>
    /// <param name="channel">Dense index of an enabled channel.</param>
    /// <param name="leaseBytes">The lease's block size as the transport thread counted it.</param>
    /// <param name="shared">The tag the message was taken with (<see cref="NoteTaken"/>).</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void NoteReturned(int channel, int leaseBytes, bool shared)
    {
        ref Returned returned = ref _returned[channel];
        Volatile.Write(ref returned.Bytes, returned.Bytes + (uint)leaseBytes);
        Volatile.Write(ref returned.Count, returned.Count + 1);
        if (shared)
        {
            ref SharedLine line = ref _shared[GameLine];
            Volatile.Write(ref line.Bytes, line.Bytes + (uint)leaseBytes);
        }

        // The shared half gained room even when the channel is handled by now: other channels' streams may go on.
        if ((shared || _state[channel] != (byte)CreditState.Handled) && !_changed)
        {
            // Written once per Drain, not once per message: the transport thread reads this object's fields for every
            // message it takes.
            _changed = true;
        }
    }

    /// <summary>Whether a limit applies to the channel (game thread): every state but <see cref="CreditState.Handled"/>.</summary>
    /// <param name="channel">Dense channel index.</param>
    public bool IsLimited(int channel) => _state[channel] != (byte)CreditState.Handled;

    /// <summary>How the channel is read (game thread).</summary>
    /// <param name="channel">Dense channel index.</param>
    public CreditState State(int channel) => (CreditState)_state[channel];

    /// <summary>
    /// <see cref="SetState"/> for the two states a handler decides: <see cref="CreditState.Unread"/> when it is removed,
    /// <see cref="CreditState.Handled"/> when it has seen the channel's backlog.
    /// </summary>
    /// <param name="channel">Dense channel index.</param>
    /// <param name="limited">Whether the limit applies.</param>
    public void SetLimited(int channel, bool limited) => SetState(channel, limited ? CreditState.Unread : CreditState.Handled);

    /// <summary>
    /// Changes how a channel is read, and with it the channel's limits (game thread). A wider limit lets
    /// <see cref="Resume"/> release the channel's streams. A narrower one takes nothing back: what was accepted stays, and
    /// the channel accepts nothing more until the application has taken it down to the new limit.
    /// </summary>
    /// <param name="channel">Dense channel index.</param>
    /// <param name="state">The new state.</param>
    public void SetState(int channel, CreditState state)
    {
        byte old = _state[channel];
        if (!_enabled[channel] || old == (byte)state)
        {
            return;
        }

        _state[channel] = (byte)state;
        DrainedChannels += (state == CreditState.Drained ? 1 : 0) - (old == (byte)CreditState.Drained ? 1 : 0);
        int count = CountLimitOf((byte)state);
        int bytes = state switch
        {
            CreditState.Unread => -_byteLimit,
            CreditState.Drained => _drainedByteLimit,
            _ => Unlimited,
        };
        ref Limit limit = ref _limits[channel];
        if (count < CountLimitOf(old))
        {
            // Narrower: the byte limit first. The transport thread reads the count first (and treats Unlimited as "no limit
            // at all"), so a pair it reads across the two stores is never wider than the state that is being left.
            Volatile.Write(ref limit.Bytes, bytes);
            Volatile.Write(ref limit.Count, count);
        }
        else
        {
            Volatile.Write(ref limit.Count, count);
            Volatile.Write(ref limit.Bytes, bytes);
            _changed = true;
        }
    }

    private int CountLimitOf(byte state) => state switch
    {
        (byte)CreditState.Handled => Unlimited,
        (byte)CreditState.Drained => _drainedCountLimit,
        _ => _countLimit,
    };

    /// <summary>
    /// Whether a <see cref="QuiclyPeer.Poll"/> has something to do here (any thread): a stream was held back since the last
    /// one, credit became available while one was being held back, a stream that was held back ended, or the game thread
    /// gave credit back and has not looked at the streams since — a handler threw, and the Poll ended before its look. A
    /// stream that is merely waiting for credit is not work.
    /// </summary>
    public bool HasWork => _changed || Volatile.Read(ref _recheck) != 0 || !Volatile.Read(ref _pended).IsEmpty || HasRetiredStreams;

    /// <summary>
    /// Collects the streams that were held back since the last call and resumes those whose channel has credit again
    /// (game thread: the end of <see cref="QuiclyPeer.Drain"/>, every <see cref="QuiclyPeer.Poll"/>, a handler
    /// registration). A channel nobody drains costs nothing here: its streams are looked at only when something changed.
    /// </summary>
    /// <remarks>
    /// A channel's streams are resumed oldest first, and only as many as the channel has messages of credit left: each of
    /// them takes at least one, so resuming more would only have them held back again, and with many streams waiting on one
    /// channel every Drain would pay for all of them. The streams that are let go either use the credit up — the next
    /// change is then a message the application takes, which comes back here — or there are none left waiting. The one
    /// stream that would take nothing is one that ended while it waited, and that is told apart (<see cref="NoteGone"/>).
    /// Under a strict byte share (<see cref="CreditState.Unread"/>) a stream is let go only when the block of the message
    /// it waits with fits in what is left of the share, and that is counted as well.
    /// </remarks>
    /// <param name="transport">The transport, or null once it is gone (the streams are then forgotten).</param>
    public void Resume(ITransport? transport)
    {
        int recheck = 0;
        bool look = _changed;
        if (look || Volatile.Read(ref _recheck) != 0)
        {
            // The fence of the store-fence-load pair (see NotePended): the counters were stored before it, the streams are
            // loaded after it.
            recheck = Interlocked.Exchange(ref _recheck, 0);
            _changed = false;
            look = true;
        }

        SpscRing<Pended> pended = Volatile.Read(ref _pended);
        if (_parked.Length < pended.Capacity)
        {
            GrowParked(pended.Capacity);
        }

        // The rings the stream capacity outgrew first: what they hold was held back before anything in the current one.
        SpscRing<Pended>[]? retired = Volatile.Read(ref _retiredPended);
        if (retired is not null)
        {
            foreach (SpscRing<Pended> old in retired)
            {
                Collect(old, transport);
            }
        }

        Collect(pended, transport);
        if (transport is null)
        {
            _parkedCount = 0;
            return;
        }

        if (!look || _parkedCount == 0)
        {
            return;
        }

        if ((recheck & Flush) != 0)
        {
            ResumeAllParked(transport);
            return;
        }

        int kept = 0;
        int channel = -1;
        int credit = 0;
        int bytes = 0;
        for (int i = 0; i < _parkedCount; i++)
        {
            Pended stream = _parked[i];
            if (stream.Channel != channel)
            {
                // Streams of several channels may alternate in the list; the count starts over with each run, which lets
                // a few more go than there is credit for.
                channel = stream.Channel;
                credit = CreditLeft(channel);
                bytes = StrictBytesLeft(channel);
            }

            if (credit == 0 || stream.Need > bytes)
            {
                _parked[kept++] = stream;
                continue;
            }

            if (credit != Unlimited)
            {
                credit--;
            }

            if (bytes != Unlimited)
            {
                bytes -= stream.Need;
            }

            transport.ResumeStreamReceive(stream.Id, 0);
        }

        _parkedCount = kept;
    }

    /// <summary>
    /// Messages of credit the channel has left (game thread): <see cref="Unlimited"/> when it is handled, 0 when a limit is
    /// reached. The transport thread's counters are read as they are: a value that is a moment old can only be lower,
    /// which lets a stream go that is then held back again, and never keeps one waiting that could go on (a count the
    /// transport thread takes back asks for another look itself, <see cref="Untake"/>).
    /// </summary>
    private int CreditLeft(int channel)
    {
        byte state = _state[channel];
        if (state == (byte)CreditState.Handled)
        {
            return Unlimited;
        }

        int limit = CountLimitOf(state);
        ref Taken taken = ref _taken[channel];
        ref Returned returned = ref _returned[channel];
        uint waiting = unchecked(Volatile.Read(ref taken.Count) - returned.Count);
        if (waiting >= (uint)limit)
        {
            return 0;
        }

        // The bytes as a threshold, for both limited states: whether the message a stream waits with fits a strict share
        // is the transport thread's to say when the stream is resumed.
        uint bytes = unchecked(Volatile.Read(ref taken.Bytes) - returned.Bytes);
        if (waiting != 0 && bytes >= (uint)(state == (byte)CreditState.Unread ? _byteLimit : _drainedByteLimit))
        {
            return 0;
        }

        // A drained channel's threshold is the half the channels no handler reads share (IsSharedFull).
        if (waiting != 0 && state == (byte)CreditState.Drained && _accountedCount > 1 && SharedWaiting() >= (uint)_drainedByteLimit)
        {
            return 0;
        }

        return limit - (int)waiting;
    }

    /// <summary>
    /// Bytes left of a strict byte share (game thread): what the block of a waiting stream's message has to fit in — the
    /// channel's own share, or what the channels no handler reads leave of <see cref="DrainedByteLimit"/> when that is less
    /// — or <see cref="Unlimited"/> for a channel whose byte limit is a threshold. Read like <see cref="CreditLeft"/>.
    /// </summary>
    private int StrictBytesLeft(int channel)
    {
        if (_state[channel] != (byte)CreditState.Unread)
        {
            return Unlimited;
        }

        uint waiting = unchecked(Volatile.Read(ref _taken[channel].Bytes) - _returned[channel].Bytes);
        int left = waiting >= (uint)_byteLimit ? 0 : _byteLimit - (int)waiting;
        if (left != 0 && _accountedCount > 1)
        {
            ulong shared = SharedWaiting();
            left = shared >= (uint)_drainedByteLimit ? 0 : Math.Min(left, _drainedByteLimit - (int)shared);
        }

        return left;
    }

    /// <summary>
    /// Lease bytes waiting in the reliable channels no handler reads — the messages taken while their channel was limited
    /// (game thread) — from the transport thread's count as it is: a moment old, it can only be lower, which lets a stream
    /// go that is then held back again (a staged message given back since is counted down a moment late, and that asks
    /// for another look itself, <see cref="Untake"/>).
    /// </summary>
    private uint SharedWaiting() => unchecked(Volatile.Read(ref _shared[TransportLine].Bytes) - _shared[GameLine].Bytes);

    /// <summary>Lease bytes counted in the shared half (tests; exact only while both threads are idle).</summary>
    internal int SharedWaitingBytes => (int)SharedWaiting();

    /// <summary>Moves the streams the transport thread held back into the game thread's own list.</summary>
    private void Collect(SpscRing<Pended> ring, ITransport? transport)
    {
        while (ring.TryDequeue(out Pended stream))
        {
            if (_parkedCount == _parked.Length)
            {
                // Streams that ended while they waited are still listed; resuming everything forgets them (a stream that
                // is still out of credit comes straight back).
                ResumeAllParked(transport);
            }

            _parked[_parkedCount++] = stream;
        }
    }

    private void ResumeAllParked(ITransport? transport)
    {
        for (int i = 0; i < _parkedCount; i++)
        {
            transport?.ResumeStreamReceive(_parked[i].Id, 0);
        }

        _parkedCount = 0;
    }

    /// <summary>The game thread's list grows to what the ring can hand it (game thread, from <see cref="Resume"/>).</summary>
    private void GrowParked(int capacity)
    {
        NativeArray<Pended> grown = new(capacity);
        _parked.AsSpan(0, _parkedCount).CopyTo(grown.AsSpan());
        _parked.Dispose();
        _parked = grown;
    }

    /// <summary>Streams waiting for credit that the game thread has collected (game thread; tests).</summary>
    public int ParkedStreams => _parkedCount;

    /// <summary>
    /// Forgets the lost connection's counts and streams (game thread, <see cref="PeerCore.ResetForReconnect"/>: no transport
    /// callback can run). The states stay: handlers survive a reconnect, and so does the way the application reads.
    /// </summary>
    public void Reset()
    {
        for (int channel = 0; channel < _enabled.Length; channel++)
        {
            // The counts start over with the connection; Pends is a statistic and keeps its total, as every counter does.
            ref Taken taken = ref _taken[channel];
            taken.Count = 0;
            taken.Bytes = 0;
            taken.SeenCount = 0;
            taken.SeenBytes = 0;
        }

        _returned.Clear();
        _shared.Clear();
        while (Volatile.Read(ref _pended).TryDequeue(out _))
        {
        }

        if (Volatile.Read(ref _retiredPended) is { } retired)
        {
            foreach (SpscRing<Pended> ring in retired)
            {
                while (ring.TryDequeue(out _))
                {
                }
            }
        }

        _parkedCount = 0;
        _changed = false;
        Volatile.Write(ref _recheck, 0);
    }

    /// <summary>Messages of a channel counted by the transport thread and not yet by the game thread (tests; exact only while both are idle).</summary>
    /// <param name="channel">Dense channel index.</param>
    internal int Waiting(int channel) => (int)unchecked(_taken[channel].Count - Volatile.Read(ref _returned[channel].Count));

    /// <summary>Lease bytes of <see cref="Waiting"/> (tests).</summary>
    /// <param name="channel">Dense channel index.</param>
    internal int WaitingBytes(int channel) => (int)unchecked(_taken[channel].Bytes - Volatile.Read(ref _returned[channel].Bytes));

    /// <summary>Moves a channel's counters to a chosen point of their 32-bit range (tests of the wrapping arithmetic; both threads idle).</summary>
    /// <param name="channel">Dense channel index.</param>
    /// <param name="count">The message counters' new value.</param>
    /// <param name="bytes">The byte counters' new value.</param>
    internal void SetCountersForTest(int channel, uint count, uint bytes)
    {
        _taken[channel] = new Taken { Count = count, Bytes = bytes, SeenCount = count, SeenBytes = bytes, Pends = _taken[channel].Pends };
        _returned[channel] = new Returned { Count = count, Bytes = bytes };
    }

    /// <summary>Moves the shared half's counters to a chosen point of their 32-bit range (tests of the wrapping arithmetic; both threads idle).</summary>
    /// <param name="bytes">The counters' new value, taken and returned alike (nothing waits).</param>
    internal void SetSharedCountersForTest(uint bytes)
    {
        _shared[TransportLine] = new SharedLine { Bytes = bytes, Seen = bytes };
        _shared[GameLine] = new SharedLine { Bytes = bytes, Seen = bytes };
    }

    /// <summary>Frees the native memory (after the transport can no longer call back).</summary>
    public void Dispose()
    {
        _taken.Dispose();
        _returned.Dispose();
        _limits.Dispose();
        _shared.Dispose();
        _parked.Dispose();
        _pended.Dispose();
        if (_retiredPended is not null)
        {
            foreach (SpscRing<Pended> retired in _retiredPended)
            {
                retired.Dispose();
            }
        }
    }

    /// <summary>What the transport thread counted for one channel, and its copy of what the game thread gave back.</summary>
    [StructLayout(LayoutKind.Sequential)]
    private struct Taken
    {
        public uint Count;
        public uint Bytes;
        public uint SeenCount;
        public uint SeenBytes;
        public long Pends;
    }

    /// <summary>What the game thread gave back for one channel.</summary>
    [StructLayout(LayoutKind.Sequential)]
    private struct Returned
    {
        public uint Count;
        public uint Bytes;
    }

    /// <summary>
    /// One thread's cache line of the shared half: <see cref="Bytes"/> are the lease bytes it counted (taken, or given back);
    /// <see cref="Seen"/> is, on the transport thread's line, its copy of the game thread's <see cref="Bytes"/>.
    /// </summary>
    [StructLayout(LayoutKind.Explicit, Size = 64)]
    private struct SharedLine
    {
        [FieldOffset(0)] public uint Bytes;
        [FieldOffset(4)] public uint Seen;
    }

    /// <summary>
    /// A channel's limits: written by the game thread, read by the transport thread for every stream message.
    /// <see cref="Bytes"/> is negative for a strict limit of that many bytes (<see cref="CreditState.Unread"/>: the block
    /// of the message that asks has to fit), and positive for a threshold (<see cref="CreditState.Drained"/>: a message is
    /// started while fewer bytes wait, and always on an empty channel).
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    private struct Limit
    {
        public int Count;
        public int Bytes;
    }

    /// <summary>A stream held back for credit.</summary>
    [StructLayout(LayoutKind.Sequential)]
    private struct Pended
    {
        public TransportStreamId Id;
        public int Channel;

        /// <summary>Block bytes of the message the stream waits with (0 for an empty payload).</summary>
        public int Need;
    }
}
