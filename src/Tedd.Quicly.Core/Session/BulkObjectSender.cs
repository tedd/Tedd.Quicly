using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;

namespace Tedd.Quicly.Core.Session;

/// <summary>
/// Drives a bulk <em>object</em> across the transfers it takes (<see cref="QuiclyPeer.BeginBulkObjectSend"/>). A Bulk
/// channel's <c>MaxMessageSize</c> bounds one transfer and nothing else (PROTOCOL.md §8), so this splits the object into
/// ascending ranges of at most that size, keeps a window of them in flight, re-sends any that fail their checksum, and
/// completes once — with the object's outcome, not a range's.
/// </summary>
/// <remarks>
/// <para><b>Game thread.</b> <see cref="QuiclyPeer.BeginBulkSendAsync"/> registers a transfer in the engine's own
/// bookkeeping, so it may only be called from the game thread. A range therefore finishes on the thread pool (that is
/// where <see cref="BulkTransfer.Completion"/> runs its continuations) but the <em>next</em> range starts in
/// <see cref="Pump"/>, from <see cref="QuiclyPeer.Poll"/>. The continuation does nothing but raise the peer's work
/// signal, which is what brings the host back to Poll.</para>
/// <para><b>Nothing is read twice and nothing is buffered.</b> The application's <see cref="IBulkSource"/> is handed to
/// every range unchanged and is read at absolute object offsets throughout, so a 10 GB object costs the driver a few
/// hundred bytes of bookkeeping.</para>
/// <para><b>Back-pressure is not failure.</b> <see cref="QuiclyPeer.BeginBulkSendAsync"/> answers
/// <see cref="BulkStatus.Rejected"/> when the peer's transfer slots are full — they are shared with the ranges the peer
/// asked for — so the driver simply stops issuing for this pass and tries the same range again next time. Only a range
/// that actually started and then failed ends the object.</para>
/// </remarks>
internal sealed class BulkObjectSender
{
    private readonly QuiclyPeer _peer;
    private readonly SendSlot[] _slots;
    private readonly long _progressBytes;
    private readonly int _maxRetries;
    private readonly Action<Exception> _onFault;
    private int _live;

    /// <param name="peer">The peer whose ranges these are.</param>
    /// <param name="objects">Objects this end may send at once.</param>
    /// <param name="window">Ranges of one object in flight at once.</param>
    /// <param name="progressBytes">Bytes between progress reports.</param>
    /// <param name="maxRetries">Ranges re-sent after a checksum failure before an object is given up on.</param>
    /// <param name="onFault">Records an exception thrown by the application's progress callback.</param>
    public BulkObjectSender(QuiclyPeer peer, int objects, int window, long progressBytes, int maxRetries, Action<Exception> onFault)
    {
        _peer = peer;
        _progressBytes = Math.Max(1, progressBytes);
        _maxRetries = maxRetries;
        _onFault = onFault;
        _slots = new SendSlot[Math.Max(1, objects)];
        for (int i = 0; i < _slots.Length; i++)
        {
            _slots[i] = new SendSlot(Math.Max(1, window));
        }
    }

    /// <summary>
    /// Whether a <see cref="Pump"/> has something to do (any thread; part of <see cref="QuiclyPeer.HasPendingWork"/>).
    /// </summary>
    /// <remarks>
    /// <para>An object in flight always does: a range to fold, a range to start, or a completion to publish. That is also
    /// why the driver hangs nothing off <see cref="BulkTransfer.Completion"/>. Doing so would put the object's progress
    /// behind the thread pool — a saturated pool stalling the object rather than merely delaying a wake-up, which is a
    /// failure this driver had and a full-suite run under load found.</para>
    /// <para>Nothing is lost by not watching it: a range finishes on the <em>game thread</em>, inside the pass that
    /// completed or terminated it, so the host is already awake and the next <see cref="Pump"/> folds it. That leaves no
    /// per-range allocation at all.</para>
    /// </remarks>
    public bool HasWork => Volatile.Read(ref _live) != 0;

    /// <summary>Objects being sent right now (tests).</summary>
    internal int LiveObjects => Volatile.Read(ref _live);

    /// <summary>Registers an object and issues as many of its ranges as fit right now (game thread).</summary>
    /// <param name="descriptor">The object.</param>
    /// <param name="rangeBytes">Largest range one transfer may carry.</param>
    /// <param name="source">Where the object's bytes come from.</param>
    /// <param name="progress">An optional progress callback (game thread, inside Poll).</param>
    /// <returns>The object's handle; already finished <see cref="BulkStatus.Rejected"/> when no slot was free.</returns>
    public BulkObjectTransfer Begin(
        in BulkObjectDescriptor descriptor, long rangeBytes, IBulkSource source, BulkObjectProgressCallback? progress)
    {
        long length = descriptor.EffectiveLength;
        BulkObjectTransfer transfer = new(in descriptor, length);
        SendSlot? slot = null;
        foreach (SendSlot candidate in _slots)
        {
            if (!candidate.InUse)
            {
                slot = candidate;
                break;
            }
        }

        if (slot is null)
        {
            transfer.Finish(new BulkObjectResult(BulkStatus.Rejected, 0, length, QuiclyErrorCode.BulkRejected, 0));
            return transfer;
        }

        slot.Begin(transfer, in descriptor, rangeBytes, source, progress);
        Volatile.Write(ref _live, _live + 1);
        Advance(slot);
        return transfer;
    }

    /// <summary>Folds what finished and starts what fits, for every object in flight (game thread, from Poll).</summary>
    public void Pump()
    {
        foreach (SendSlot slot in _slots)
        {
            if (slot.InUse)
            {
                Advance(slot);
            }
        }
    }

    /// <summary>
    /// Ends every object still in flight (game thread, peer teardown). The engine finishes each live range first, so this
    /// mostly settles objects whose remaining ranges were never started.
    /// </summary>
    /// <param name="status">Why the objects are ending.</param>
    public void AbortAll(BulkStatus status)
    {
        foreach (SendSlot slot in _slots)
        {
            if (slot.InUse)
            {
                Finish(slot, status, QuiclyErrorCode.NoError);
            }
        }
    }

    /// <summary>One object's step: fold finished ranges, honour a cancel, start what fits, complete when nothing is left.</summary>
    private void Advance(SendSlot slot)
    {
        BulkObjectTransfer transfer = slot.Transfer!;
        bool cancelling = transfer.CancelRequested;

        // An outcome decided on an earlier pass, while ranges were still settling, outranks anything they report now:
        // cancelling them is how they were brought down, so their own Canceled must not overwrite the real reason.
        BulkStatus terminal = slot.PendingStatus;
        QuiclyErrorCode code = slot.PendingCode;

        for (int i = 0; i < slot.InFlight.Length; i++)
        {
            BulkTransfer? range = slot.InFlight[i];
            if (range is null)
            {
                continue;
            }

            if (!range.IsFinished)
            {
                if (cancelling)
                {
                    range.Cancel();
                }

                continue;
            }

            slot.InFlight[i] = null;
            slot.Ranges++;
            BulkResult result = range.Result;
            if (result.Status == BulkStatus.Completed)
            {
                slot.BytesDone += result.BytesTransferred;
                continue;
            }

            if (result.Code == QuiclyErrorCode.BulkChecksumFailed && !cancelling && slot.Retries < _maxRetries)
            {
                // The range is the unit of recovery, which is the whole point of checksumming per range: re-read it from
                // the source and send it again rather than losing an object over one bad megabyte. The peer forgets a
                // range that failed its checksum, so the retry lands on the same offsets and overwrites what it wrote.
                slot.Retries++;
                slot.Requeue(range.Descriptor.Offset);
                continue;
            }

            // A range that started and then ended any other way takes the object with it: its bytes are gone and no later
            // range can carry them. (A range that never started is answered Rejected synchronously and never gets here.)
            if (terminal == BulkStatus.Running)
            {
                terminal = result.Status;
                code = result.Code;

                // Whatever the peer did accept still counts, so a cancelled object's byte count is not an understatement.
                slot.BytesDone += result.BytesTransferred;
            }
        }

        if (terminal == BulkStatus.Running && cancelling && slot.LiveRanges == 0)
        {
            terminal = BulkStatus.Canceled;
            code = QuiclyErrorCode.BulkCanceled;
        }

        if (terminal != BulkStatus.Running)
        {
            if (slot.LiveRanges == 0)
            {
                Finish(slot, terminal, code);
            }
            else
            {
                // Let the ranges still running settle first, so the object's byte count is final when it completes.
                slot.Cancel(terminal, code);
            }

            return;
        }

        if (!cancelling)
        {
            Issue(slot);
        }

        Report(slot);
        if (!slot.HasRangeToIssue && slot.LiveRanges == 0)
        {
            Finish(slot, BulkStatus.Completed, QuiclyErrorCode.NoError);
        }
    }

    /// <summary>Starts ranges until the window is full, the object is fully issued, or the engine has no slot left.</summary>
    private void Issue(SendSlot slot)
    {
        for (int i = 0; i < slot.InFlight.Length && slot.HasRangeToIssue; i++)
        {
            if (slot.InFlight[i] is not null)
            {
                continue;
            }

            // A range that failed its checksum goes out again before any new ground is broken, so a retry is never left
            // behind the rest of a 10 GB object.
            long offset = slot.TakeNextOffset();
            long length = Math.Min(slot.RangeBytes, slot.EndOffset - offset);
            BulkObjectDescriptor descriptor = slot.Descriptor;
            BulkDescriptor range = new(
                descriptor.Channel, descriptor.ObjectId, descriptor.ObjectVersion, descriptor.TotalLength,
                offset, length, descriptor.Compress)
            {
                // Every range carries its own checksum trailer, so a bad one is caught at its end and re-sent alone.
                Checksum = descriptor.Checksum,
            };

            // The engine answers synchronously (it registers the transfer or refuses it on the spot); AsTask is the fallback.
            ValueTask<BulkTransfer> started = _peer.BeginBulkSendAsync(range, slot.Source!);
            BulkTransfer transfer = started.IsCompletedSuccessfully ? started.Result : started.AsTask().GetAwaiter().GetResult();
            if (transfer.Status == BulkStatus.Rejected)
            {
                // The engine's per-direction slots are full (or the peer went away). Not this object's failure: put the
                // range back and try it again next pass. A peer that is gone ends the object through its live ranges.
                slot.Requeue(offset);
                if (_peer.State != PeerState.Connected && slot.LiveRanges == 0)
                {
                    Finish(slot, BulkStatus.Disconnected, QuiclyErrorCode.NoError);
                }

                // Otherwise nothing to do but come back: the object stays in flight, so HasWork keeps the peer marked and
                // the next Poll tries the same range again. No signal is raised here — that would spin.
                return;
            }

            slot.InFlight[i] = transfer;
        }
    }

    /// <summary>Publishes the object's numbers and reports them when they moved far enough.</summary>
    private void Report(SendSlot slot)
    {
        long bytes = slot.BytesDone;
        foreach (BulkTransfer? range in slot.InFlight)
        {
            if (range is { IsFinished: false })
            {
                bytes += range.BytesTransferred;
            }
        }

        slot.Transfer!.Advance(bytes, slot.Ranges);
        if (bytes - slot.Reported < _progressBytes)
        {
            return;
        }

        slot.Reported = bytes;
        if (slot.Progress is not { } callback)
        {
            return;
        }

        BulkObjectProgress progress = new(
            slot.Descriptor.ObjectId, slot.Descriptor.ObjectVersion, bytes, slot.Transfer.Length, slot.Ranges, slot.LiveRanges);
        try
        {
            callback(in progress);
        }
        catch (Exception exception)
        {
            _onFault(exception);
        }
    }

    private void Finish(SendSlot slot, BulkStatus status, QuiclyErrorCode code)
    {
        BulkObjectTransfer transfer = slot.Transfer!;
        BulkObjectResult result = new(status, slot.BytesDone, transfer.Length, code, slot.Ranges, slot.Retries);
        slot.Release();
        Volatile.Write(ref _live, _live - 1);
        transfer.Finish(in result);
    }

    /// <summary>One object being sent (game thread; preallocated and reused).</summary>
    private sealed class SendSlot(int window)
    {
        public BulkTransfer?[] InFlight { get; } = new BulkTransfer?[window];

        /// <summary>
        /// Ranges owed a second attempt: one that failed its checksum, or one the engine had no slot for. At most one per
        /// in-flight range can be owed at a time, so the window sizes this exactly.
        /// </summary>
        private readonly long[] _requeued = new long[window];

        private int _requeuedCount;

        public bool InUse { get; private set; }

        public BulkObjectTransfer? Transfer { get; private set; }

        public BulkObjectDescriptor Descriptor { get; private set; }

        public IBulkSource? Source { get; private set; }

        public BulkObjectProgressCallback? Progress { get; private set; }

        public long RangeBytes { get; private set; }

        public long NextOffset { get; set; }

        public long EndOffset { get; private set; }

        public long BytesDone { get; set; }

        public long Reported { get; set; }

        public int Ranges { get; set; }

        /// <summary>Ranges re-sent because their checksum did not match.</summary>
        public int Retries { get; set; }

        /// <summary>Whether <see cref="TakeNextOffset"/> has a range to give: one owed a retry, or new ground.</summary>
        public bool HasRangeToIssue => _requeuedCount != 0 || NextOffset < EndOffset;

        /// <summary>An outcome already decided, kept while the ranges still running are brought down.</summary>
        public BulkStatus PendingStatus { get; private set; }

        /// <summary>The code that goes with <see cref="PendingStatus"/>.</summary>
        public QuiclyErrorCode PendingCode { get; private set; }

        /// <summary>Ranges of this object still running.</summary>
        public int LiveRanges
        {
            get
            {
                int live = 0;
                foreach (BulkTransfer? range in InFlight)
                {
                    if (range is { IsFinished: false })
                    {
                        live++;
                    }
                }

                return live;
            }
        }

        public void Begin(
            BulkObjectTransfer transfer, in BulkObjectDescriptor descriptor, long rangeBytes, IBulkSource source,
            BulkObjectProgressCallback? progress)
        {
            InUse = true;
            Transfer = transfer;
            Descriptor = descriptor;
            Source = source;
            Progress = progress;
            RangeBytes = rangeBytes;
            NextOffset = descriptor.Offset;
            EndOffset = descriptor.Offset + descriptor.EffectiveLength;
            BytesDone = 0;
            Reported = 0;
            Ranges = 0;
            Retries = 0;
            _requeuedCount = 0;
            PendingStatus = BulkStatus.Running;
            PendingCode = QuiclyErrorCode.NoError;
            Array.Clear(InFlight);
        }

        /// <summary>Owes <paramref name="offset"/> another attempt, ahead of any ground not yet broken.</summary>
        public void Requeue(long offset)
        {
            if (_requeuedCount < _requeued.Length)
            {
                _requeued[_requeuedCount++] = offset;
            }
        }

        /// <summary>The next range's offset: one owed a retry first, else the next unsent one.</summary>
        public long TakeNextOffset()
        {
            if (_requeuedCount != 0)
            {
                return _requeued[--_requeuedCount];
            }

            long offset = NextOffset;
            NextOffset = Math.Min(EndOffset, offset + RangeBytes);
            return offset;
        }

        /// <summary>Stops issuing and asks every live range to stop, remembering the outcome that got here first.</summary>
        public void Cancel(BulkStatus status, QuiclyErrorCode code)
        {
            PendingStatus = status;
            PendingCode = code;
            NextOffset = EndOffset;
            _requeuedCount = 0;
            foreach (BulkTransfer? range in InFlight)
            {
                range?.Cancel();
            }
        }

        public void Release()
        {
            InUse = false;
            PendingStatus = BulkStatus.Running;
            PendingCode = QuiclyErrorCode.NoError;
            Transfer = null;
            Source = null;
            Progress = null;
            Array.Clear(InFlight);
        }
    }
}
