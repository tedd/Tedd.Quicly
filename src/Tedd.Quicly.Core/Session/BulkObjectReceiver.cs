using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Time;

namespace Tedd.Quicly.Core.Session;

/// <summary>
/// Reassembles bulk <em>objects</em> from the transfers they arrive in (<see cref="PeerOptions.BulkObjectRouter"/>).
/// It is the <see cref="IBulkRouter"/> the engine sees; the application sees one
/// <see cref="IBulkObjectRouter.SelectTarget"/> per object, one <see cref="IBulkObjectSink"/> written at absolute object
/// offsets, and one <see cref="IBulkObjectSink.Finish"/>.
/// </summary>
/// <remarks>
/// <para><b>Almost everything here runs on the transport thread</b>, which serialises a peer's callbacks. The exception
/// is <see cref="SweepIdle"/>, which the game thread runs from the peer's timers, so an object's <em>lifecycle</em> —
/// taken, begun, finished, released — is under a lock and the per-byte path is not. Nothing allocates: the object slots,
/// their per-range shims and their range sets are built once, at construction.</para>
/// <para><b>Nothing is buffered at the declared size.</b> A range's bytes go straight from the engine to the
/// application's sink, the same zero-copy hand-off <see cref="IBulkSink"/> gives.</para>
/// <para><b>A range is only counted once it verifies.</b> The engine checks each range against the checksum trailer its
/// sender appended (PROTOCOL.md §3.3) and fails the transfer when it does not match; this end then forgets that range so
/// the sender can send it again, and the object carries on. An object is complete when its <em>verified</em> ranges
/// exactly tile the range it was told to expect — which is what makes "every range verified" mean "the object is
/// verified", with no whole-object hash anywhere.</para>
/// <para><b>A failed range's bytes were already delivered.</b> They are written as they arrive, before the trailer that
/// condemns them, so a retry overwrites them at the same offsets. That is why <see cref="IBulkObjectSink.Write"/> is
/// contractually a random-access write: a sink that appends would corrupt the object here.</para>
/// </remarks>
internal sealed class BulkObjectReceiver : IBulkRouter
{
    /// <summary>
    /// Verified runs of one object tracked at once, however few ranges the peer may have in flight. A peer that keeps a
    /// handful of ranges going never needs this many; it is the floor below which the capacity is not worth deriving.
    /// </summary>
    private const int MinRanges = 16;

    /// <summary>
    /// How long an ended object's identity is held before it may begin again. It only has to outlast the ranges of the
    /// attempt that ended — streams the peer had already opened, or opens before it hears the object is over — which is a
    /// round trip's worth of skew, not a timeout's. It is a fixed window from the end, so an object can always be sent
    /// again under the same identity after it.
    /// </summary>
    private const long TombstoneQuietMicros = 2_000_000;

    private readonly IBulkObjectRouter _router;
    private readonly ObjectSlot[] _objects;
    private readonly RangeSink[] _shims;
    private readonly long _progressBytes;
    private readonly Action<Exception> _onFault;
    private readonly IClock _clock;

    /// <summary>
    /// Guards an object's <em>lifecycle</em> — taken, begun, finished, released — which the transport thread drives from
    /// arriving ranges and the game thread drives from <see cref="SweepIdle"/>. The per-byte path
    /// (<see cref="OnWrite"/>) never takes it: it touches only a slot that is already under way. The application's router
    /// runs under it, which its contract allows for (no blocking, no locks, no call back into the peer).
    /// </summary>
    private readonly Lock _lifecycle = new();

    /// <summary>Objects being assembled; read without the lock so an idle peer's timer pass never takes it.</summary>
    private int _live;

    /// <summary>
    /// Identities of objects that have ended, so a range still arriving for one is refused rather than taken as the start
    /// of a new object. A slot is released the moment its object ends, so without this the identity is simply forgotten
    /// and the next stray range looks like a first range.
    /// </summary>
    private readonly bool[] _deadSet;
    private readonly ushort[] _deadChannel;
    private readonly ulong[] _deadId;
    private readonly ulong[] _deadVersion;
    private readonly long[] _deadSeen;
    private int _deadNext;

    /// <param name="router">The application's object router.</param>
    /// <param name="clock">The peer's clock, for the idle sweep.</param>
    /// <param name="objects">Objects this end may assemble at once.</param>
    /// <param name="shims">Range sinks to hold, at least the transfers the engine may accept at once.</param>
    /// <param name="progressBytes">Bytes between progress reports.</param>
    /// <param name="onFault">Records an exception thrown by the application's progress callback.</param>
    public BulkObjectReceiver(
        IBulkObjectRouter router, IClock clock, int objects, int shims, long progressBytes, Action<Exception> onFault)
    {
        _router = router;
        _clock = clock;
        _progressBytes = Math.Max(1, progressBytes);
        _onFault = onFault;
        // Every range in flight can land out of order and leave a gap behind it, so the runs an honest peer can produce
        // scale with the window it is allowed, not with a constant. Sizing this off the window is what keeps a legitimate
        // wide sender from failing its own object with LimitExceeded.
        int ranges = Math.Max(MinRanges, Math.Max(1, shims) + 8);
        _objects = new ObjectSlot[Math.Max(1, objects)];
        for (int i = 0; i < _objects.Length; i++)
        {
            _objects[i] = new ObjectSlot(ranges);
        }

        _shims = new RangeSink[Math.Max(1, shims)];
        for (int i = 0; i < _shims.Length; i++)
        {
            _shims[i] = new RangeSink(this);
        }

        int dead = Math.Max(8, _objects.Length * 4);
        _deadSet = new bool[dead];
        _deadChannel = new ushort[dead];
        _deadId = new ulong[dead];
        _deadVersion = new ulong[dead];
        _deadSeen = new long[dead];
    }

    /// <summary>Objects being assembled right now (tests).</summary>
    internal int LiveObjects => Volatile.Read(ref _live);

    /// <inheritdoc/>
    public BulkReceiveDecision SelectTarget(in BulkTransferInfo info)
    {
        lock (_lifecycle)
        {
            return SelectTargetLocked(in info);
        }
    }

    /// <inheritdoc/>
    public void OnRequestRejected(in BulkRangeRequest request, QuiclyErrorCode code) => _router.OnRequestRejected(in request, code);

    private BulkReceiveDecision SelectTargetLocked(in BulkTransferInfo info)
    {
        ObjectSlot? slot = Find(in info);
        if (slot is { Finished: true })
        {
            // The object already ended — it failed, or the idle sweep gave up on it — and its slot is only still here
            // because another range was writing. Taking this one would hand the application a second object under an
            // identity it has already been told about.
            return BulkReceiveDecision.Reject(QuiclyErrorCode.BulkRejected);
        }

        if (slot is null && IsDead(in info, _clock.NowMicros))
        {
            // The object ended — completed, failed, or given up on by the idle sweep — and its slot has already gone back.
            // This range is the tail of that attempt, so beginning an object for it would hand the application a second
            // sink under an identity it has already had a Finish for.
            return BulkReceiveDecision.Reject(QuiclyErrorCode.BulkRejected);
        }

        RangeSink? shim = TakeShim();
        if (shim is null)
        {
            // More streams than shims: the engine's own BulkTransfersPerDirection normally gets there first.
            return BulkReceiveDecision.Reject(QuiclyErrorCode.LimitExceeded);
        }

        if (slot is null)
        {
            slot = TakeSlot();
            if (slot is null)
            {
                return BulkReceiveDecision.Reject(QuiclyErrorCode.LimitExceeded);
            }

            if (!TryBegin(slot, in info))
            {
                return BulkReceiveDecision.Reject(slot.RejectCode);
            }
        }

        long start = info.Offset;
        long end = info.Offset + info.Length;
        if (start < slot.Offset || end > slot.Offset + slot.Length || slot.Covered.Intersects(start, end) || IsArriving(slot, start, end))
        {
            // Outside what the object carries, or a byte it already has, or one another live range is bringing: taking it
            // would make "the verified ranges tile the object" a lie.
            return RejectRange(slot, QuiclyErrorCode.ProtocolViolation);
        }

        slot.Live++;
        slot.LastActivity = _clock.NowMicros;
        shim.Bind(slot, start, end);
        return BulkReceiveDecision.Accept(shim);
    }

    /// <summary>
    /// Gives up on objects that have gone quiet: no range of their own running and nothing new for
    /// <paramref name="idleMicros"/> (game thread, from the peer's timers). Returns the deadline of the object that will
    /// go next, so the peer can wake for it.
    /// </summary>
    /// <remarks>
    /// This is the only thing that ends an object whose sender stopped between two ranges — a cancel on a range boundary,
    /// an abandoned object, a sender that died, a sender that ran out of retries for a range that will not verify —
    /// because a sender signals abandonment by resetting a stream and in that state it has none open (PROTOCOL.md §3.4,
    /// <see cref="PeerOptions.BulkObjectIdleTimeout"/>).
    /// </remarks>
    /// <param name="now">Clock micros of this pass.</param>
    /// <param name="idleMicros">How long an object may stay quiet; 0 disables the sweep.</param>
    /// <param name="next">The peer's deadline so far.</param>
    /// <returns>The deadline, lowered to the earliest object expiry.</returns>
    internal long SweepIdle(long now, long idleMicros, long next)
    {
        // Read before the lock: this runs on every timer pass of every peer, and a peer that is assembling nothing — which
        // is nearly all of them, nearly all of the time — should not pay for the lock at all.
        if (idleMicros <= 0 || Volatile.Read(ref _live) == 0)
        {
            return next;
        }

        lock (_lifecycle)
        {
            foreach (ObjectSlot slot in _objects)
            {
                // A live range is progress by definition: only an object with nothing running can be stuck.
                if (!slot.InUse || slot.Finished || slot.Live != 0)
                {
                    continue;
                }

                long due = slot.LastActivity + idleMicros;
                if (now >= due)
                {
                    FinishObject(slot, BulkStatus.Failed, QuiclyErrorCode.Timeout);
                }
                else if (due < next)
                {
                    next = due;
                }
            }
        }

        return next;
    }

    /// <summary>
    /// Finishes every object still being assembled (game thread, peer teardown): a connection that goes away mid-object
    /// must still end the application's wait.
    /// </summary>
    /// <param name="status">Why the objects are ending.</param>
    internal void AbortAll(BulkStatus status)
    {
        lock (_lifecycle)
        {
            foreach (ObjectSlot slot in _objects)
            {
                if (slot.InUse)
                {
                    FinishObject(slot, status, QuiclyErrorCode.NoError);
                }
            }
        }
    }

    /// <summary>A range's bytes, straight to the application. The engine checks the range's checksum when it ends.</summary>
    private void OnWrite(RangeSink shim, long objectOffset, ReadOnlySpan<byte> data)
    {
        // The per-byte path takes no lock. The slot is under way with this range live, so nothing but an idle sweep could
        // be finishing it — and that only touches objects with no live range. Reading the sink once guards the rest.
        ObjectSlot? slot = shim.Slot;
        if (slot is null || slot.Finished || slot.Sink is not { } sink)
        {
            return;
        }

        sink.Write(objectOffset, data);
        shim.Written += data.Length;
        slot.BytesSeen += data.Length;
        if (slot.BytesSeen - slot.Reported >= _progressBytes)
        {
            Report(slot);
        }
    }

    /// <summary>
    /// One range ended. A range that verified is the object's for good; one that did not is forgotten, so its sender can
    /// send it again — the bytes it already wrote are overwritten by the retry.
    /// </summary>
    private void OnRangeFinish(RangeSink shim, in BulkResult result)
    {
        lock (_lifecycle)
        {
            OnRangeFinishLocked(shim, in result);
        }
    }

    private void OnRangeFinishLocked(RangeSink shim, in BulkResult result)
    {
        ObjectSlot? slot = shim.Slot;
        long start = shim.Start;
        long end = shim.End;
        long written = shim.Written;
        shim.Release();
        if (slot is null)
        {
            return;
        }

        slot.Live--;
        slot.Ranges++;
        slot.LastActivity = _clock.NowMicros;
        if (slot.Finished)
        {
            // The object ended on an earlier range (or an idle sweep) while this one was still writing; now that nothing
            // is, its slot can take the next object.
            if (slot.Live == 0 && slot.InUse)
            {
                ReleaseSlot(slot);
            }

            return;
        }

        if (result.Status == BulkStatus.Completed)
        {
            if (!slot.Covered.TryAdd(start, end))
            {
                FinishObject(slot, BulkStatus.Failed, QuiclyErrorCode.LimitExceeded);
                return;
            }

            slot.BytesDone += end - start;
            if (slot.BytesDone >= slot.Length)
            {
                FinishObject(slot, BulkStatus.Completed, QuiclyErrorCode.NoError);
            }

            return;
        }

        if (result.Code == QuiclyErrorCode.BulkChecksumFailed)
        {
            // Not the object's failure. The range is forgotten — never covered, and the bytes it wrote no longer counted —
            // and the sender's driver sends it again. If it never does, the idle sweep ends the object.
            slot.Retries++;
            slot.BytesSeen -= written;
            return;
        }

        // Cancelled, reset or disconnected: those bytes are not coming and nothing will re-send them.
        FinishObject(slot, result.Status, result.Code);
    }

    /// <summary>Asks the application where an object goes; false when it refused or described a range that cannot hold its first.</summary>
    private bool TryBegin(ObjectSlot slot, in BulkTransferInfo info)
    {
        BulkObjectInfo objectInfo = new()
        {
            Channel = info.Channel,
            ObjectId = info.ObjectId,
            ObjectVersion = info.ObjectVersion,
            TotalLength = info.TotalLength,
            Offset = info.Offset,
            HasChecksum = info.HasChecksum,
            PeerIndex = info.PeerIndex,
        };

        BulkObjectReceiveDecision decision = _router.SelectTarget(in objectInfo);
        if (!decision.Accepted || decision.Sink is null)
        {
            slot.RejectCode = decision.RejectCode == QuiclyErrorCode.NoError ? QuiclyErrorCode.BulkRejected : decision.RejectCode;
            return false;
        }

        // The object's extent is the object's, never the first-arriving range's. Ranges are carried on separate QUIC
        // streams and nothing orders their headers, so anchoring on info.Offset would reject every range below whichever
        // one won the race and call the object complete at that range's end — a truncated object reported Completed.
        long offset = decision.ExpectedOffset;
        long length = decision.ExpectedLength > 0 ? decision.ExpectedLength : info.TotalLength - offset;
        if (offset < 0 || length <= 0 || offset > info.TotalLength || offset + length > info.TotalLength
            || info.Offset < offset || info.Offset + info.Length > offset + length)
        {
            // The application named a window that does not contain the range that arrived; it has a sink out, so it gets
            // a Finish rather than silence.
            BeginSlot(slot, in info, offset, length, decision.Sink, decision.Progress);
            FinishObject(slot, BulkStatus.Failed, QuiclyErrorCode.ProtocolViolation);
            slot.RejectCode = QuiclyErrorCode.ProtocolViolation;
            return false;
        }

        BeginSlot(slot, in info, offset, length, decision.Sink, decision.Progress);
        return true;
    }

    /// <summary>Starts an object in <paramref name="slot"/> and counts it (under the lifecycle lock).</summary>
    private void BeginSlot(
        ObjectSlot slot, in BulkTransferInfo info, long offset, long length, IBulkObjectSink sink, BulkObjectProgressCallback? progress)
    {
        slot.Begin(in info, offset, length, sink, progress);
        slot.LastActivity = _clock.NowMicros;
        Volatile.Write(ref _live, _live + 1);
    }

    /// <summary>Hands <paramref name="slot"/> back for the next object (under the lifecycle lock).</summary>
    private void ReleaseSlot(ObjectSlot slot)
    {
        slot.Release();
        Volatile.Write(ref _live, _live - 1);
    }

    /// <summary>Refuses a range of an object already under way: the object fails, because its missing bytes end it anyway.</summary>
    private BulkReceiveDecision RejectRange(ObjectSlot slot, QuiclyErrorCode code)
    {
        FinishObject(slot, BulkStatus.Failed, code);
        return BulkReceiveDecision.Reject(code);
    }

    /// <summary>Ends an object exactly once: the application is told and the slot goes back.</summary>
    private void FinishObject(ObjectSlot slot, BulkStatus status, QuiclyErrorCode code)
    {
        if (slot.Finished)
        {
            return;
        }

        slot.Finished = true;
        NoteDead(slot, _clock.NowMicros);
        IBulkObjectSink? sink = slot.Sink;
        BulkObjectResult result = new(status, slot.BytesDone, slot.Length, code, slot.Ranges, slot.Retries);
        if (slot.Live == 0)
        {
            // No range is still writing into it, so the slot can take the next object at once; otherwise the last range's
            // Finish releases it.
            ReleaseSlot(slot);
        }

        sink?.Finish(in result);
    }

    /// <summary>Reports progress, swallowing what the application's callback throws.</summary>
    private void Report(ObjectSlot slot)
    {
        slot.Reported = slot.BytesSeen;
        if (slot.Progress is not { } callback)
        {
            return;
        }

        BulkObjectProgress progress = new(slot.ObjectId, slot.ObjectVersion, slot.BytesSeen, slot.Length, slot.Ranges, slot.Live);
        try
        {
            callback(in progress);
        }
        catch (Exception exception)
        {
            _onFault(exception);
        }
    }

    /// <summary>
    /// Whether <paramref name="info"/> names an object that ended too recently for this to be anything but its tail
    /// (under the lifecycle lock).
    /// </summary>
    /// <remarks>
    /// The window runs from when the object ended and a hit does not extend it. Extending it on every stray would let a
    /// sender that retries an object inside the window refresh the tombstone with its own attempts and keep that identity
    /// dead for good.
    /// </remarks>
    private bool IsDead(in BulkTransferInfo info, long now)
    {
        for (int i = 0; i < _deadSet.Length; i++)
        {
            if (!_deadSet[i] || _deadChannel[i] != info.Channel || _deadId[i] != info.ObjectId || _deadVersion[i] != info.ObjectVersion)
            {
                continue;
            }

            if (now - _deadSeen[i] > TombstoneQuietMicros)
            {
                // Long enough since it ended that its ranges cannot still be turning up: this is a new attempt.
                _deadSet[i] = false;
                return false;
            }

            return true;
        }

        return false;
    }

    /// <summary>Remembers an ended object's identity (under the lifecycle lock).</summary>
    private void NoteDead(ObjectSlot slot, long now)
    {
        for (int i = 0; i < _deadSet.Length; i++)
        {
            if (_deadSet[i] && _deadChannel[i] == slot.Channel && _deadId[i] == slot.ObjectId && _deadVersion[i] == slot.ObjectVersion)
            {
                _deadSeen[i] = now;
                return;
            }
        }

        // The oldest entry goes: an identity that has been quiet longest is the one least likely to still have ranges out.
        int at = _deadNext;
        _deadNext = at + 1 == _deadSet.Length ? 0 : at + 1;
        _deadSet[at] = true;
        _deadChannel[at] = slot.Channel;
        _deadId[at] = slot.ObjectId;
        _deadVersion[at] = slot.ObjectVersion;
        _deadSeen[at] = now;
    }

    /// <summary>Whether another range of <paramref name="slot"/> is already bringing part of <c>[start, end)</c>.</summary>
    private bool IsArriving(ObjectSlot slot, long start, long end)
    {
        foreach (RangeSink shim in _shims)
        {
            if (ReferenceEquals(shim.Slot, slot) && shim.Start < end && start < shim.End)
            {
                return true;
            }
        }

        return false;
    }

    private ObjectSlot? Find(in BulkTransferInfo info)
    {
        foreach (ObjectSlot slot in _objects)
        {
            if (slot.InUse && slot.Channel == info.Channel && slot.ObjectId == info.ObjectId && slot.ObjectVersion == info.ObjectVersion)
            {
                return slot;
            }
        }

        return null;
    }

    private ObjectSlot? TakeSlot()
    {
        foreach (ObjectSlot slot in _objects)
        {
            if (!slot.InUse)
            {
                return slot;
            }
        }

        return null;
    }

    private RangeSink? TakeShim()
    {
        foreach (RangeSink shim in _shims)
        {
            if (shim.Slot is null)
            {
                return shim;
            }
        }

        return null;
    }

    /// <summary>One object being assembled (transport thread; preallocated and reused).</summary>
    /// <param name="ranges">Disjoint verified runs this object may hold before it counts as too fragmented.</param>
    private sealed class ObjectSlot(int ranges)
    {
        /// <summary>The ranges that verified, merged; ascending ranges collapse to one entry.</summary>
        public BulkRangeSet Covered { get; } = new(ranges);

        public bool InUse { get; private set; }

        public bool Finished { get; set; }

        public ushort Channel { get; private set; }

        public ulong ObjectId { get; private set; }

        public ulong ObjectVersion { get; private set; }

        public long Offset { get; private set; }

        public long Length { get; private set; }

        public IBulkObjectSink? Sink { get; private set; }

        public BulkObjectProgressCallback? Progress { get; private set; }

        /// <summary>Bytes of ranges that verified: what decides the object is complete.</summary>
        public long BytesDone { get; set; }

        /// <summary>Bytes handed to the sink, retries and all: what progress reports, since it moves while a range runs.</summary>
        public long BytesSeen { get; set; }

        public long Reported { get; set; }

        public int Ranges { get; set; }

        /// <summary>Ranges that had to be sent again because their checksum did not match.</summary>
        public int Retries { get; set; }

        /// <summary>Ranges writing into this object right now.</summary>
        public int Live { get; set; }

        /// <summary>Why <see cref="BulkObjectReceiver.TryBegin"/> refused, so the caller can name it.</summary>
        public QuiclyErrorCode RejectCode { get; set; }

        /// <summary>Clock micros of the last range this object took or finished; what <see cref="SweepIdle"/> measures.</summary>
        public long LastActivity { get; set; }

        public void Begin(in BulkTransferInfo info, long offset, long length, IBulkObjectSink sink, BulkObjectProgressCallback? progress)
        {
            InUse = true;
            Finished = false;
            Channel = info.Channel;
            ObjectId = info.ObjectId;
            ObjectVersion = info.ObjectVersion;
            Offset = offset;
            Length = length;
            Sink = sink;
            Progress = progress;
            BytesDone = 0;
            BytesSeen = 0;
            Reported = 0;
            Ranges = 0;
            Retries = 0;
            Live = 0;
            RejectCode = QuiclyErrorCode.NoError;
            Covered.Clear();
        }

        public void Release()
        {
            InUse = false;
            Sink = null;
            Progress = null;
            Covered.Clear();
        }
    }

    /// <summary>
    /// The <see cref="IBulkSink"/> the engine sees for one transfer. It carries the object the transfer belongs to and
    /// the range it is bringing, which is what turns the engine's per-transfer view into the application's per-object one.
    /// </summary>
    private sealed class RangeSink(BulkObjectReceiver owner) : IBulkSink
    {
        public ObjectSlot? Slot { get; private set; }

        public long Start { get; private set; }

        public long End { get; private set; }

        /// <summary>Bytes handed to the application for this range; unwound when the range turns out to be corrupt.</summary>
        public long Written { get; set; }

        public void Bind(ObjectSlot slot, long start, long end)
        {
            Slot = slot;
            Start = start;
            End = end;
            Written = 0;
        }

        public void Release()
        {
            Slot = null;
            Start = 0;
            End = 0;
            Written = 0;
        }

        public void Write(long objectOffset, ReadOnlySpan<byte> data) => owner.OnWrite(this, objectOffset, data);

        public void Finish(in BulkResult result) => owner.OnRangeFinish(this, in result);
    }
}
