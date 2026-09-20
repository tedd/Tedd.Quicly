using Tedd.Quicly.Core.Control;

namespace Tedd.Quicly.Core.Session;

/// <summary>
/// Describes a whole bulk <em>object</em> — a file, a snapshot, an asset — of any size up to 2^62 − 1 bytes
/// (<see cref="QuiclyPeer.BeginBulkObjectSend"/>). A Bulk channel's <c>MaxMessageSize</c> bounds one <em>transfer</em>
/// and nothing else (PROTOCOL.md §8), so an object larger than that crosses as several transfers of ascending ranges;
/// the driver issues them, keeps a window of them in flight, resumes the object's identity on each one and reassembles
/// the whole on the far side. The application sees one begin, one completion and one hash.
/// </summary>
/// <param name="Channel">The Bulk channel.</param>
/// <param name="ObjectId">Object identity.</param>
/// <param name="ObjectVersion">Object version (identity is id + version).</param>
/// <param name="TotalLength">Total object length in bytes (at most 2^62 − 1).</param>
/// <param name="Offset">First byte of the object this send carries.</param>
/// <param name="Length">Bytes this send carries; exactly 0 means "to the end of the object". Unlike
/// <see cref="BulkDescriptor.Length"/> this is <em>not</em> bounded by the channel's <c>MaxMessageSize</c>.</param>
/// <param name="Compress">Send each range's body as independently LZ4-decodable chunks (PROTOCOL.md §3.3).</param>
public readonly record struct BulkObjectDescriptor(
    ushort Channel,
    ulong ObjectId,
    ulong ObjectVersion,
    long TotalLength,
    long Offset = 0,
    long Length = 0,
    bool Compress = false)
{
    /// <summary>
    /// Append an xxHash64 of each range after its body, so a corrupt range is caught at <em>its</em> end rather than the
    /// object.s and is re-sent on its own (PROTOCOL.md §3.3). <see langword="null"/> (the default) follows
    /// <see cref="PeerOptions.BulkChecksum"/>.
    /// </summary>
    /// <remarks>
    /// This is the whole reason the object driver carries no whole-object hash. Detecting a bad byte after 10 GB would be
    /// useless: the unit of recovery is a range, so the unit of detection has to be one too. Every range verifying, over a
    /// range set that exactly tiles the object, <em>is</em> the object verifying.
    /// </remarks>
    public bool? Checksum { get; init; }

    /// <summary>Bytes this send carries: <see cref="Length"/>, or the rest of the object when it is 0.</summary>
    public long EffectiveLength => Length > 0 ? Length : TotalLength - Offset;

    /// <summary>Whether the send covers the whole object.</summary>
    public bool IsWholeObject => Offset == 0 && EffectiveLength == TotalLength;
}

/// <summary>Outcome of a bulk object transfer (<see cref="BulkObjectTransfer.Completion"/>, <see cref="IBulkObjectSink.Finish"/>).</summary>
/// <param name="Status">How it ended.</param>
/// <param name="BytesTransferred">Bytes of the object's range that were transferred.</param>
/// <param name="Length">Bytes the object's range holds.</param>
/// <param name="Code">
/// The QUIC application error code involved, or <see cref="QuiclyErrorCode.NoError"/>. An object that kept failing its
/// range checksums ends <see cref="BulkStatus.Failed"/> with <see cref="QuiclyErrorCode.BulkChecksumFailed"/>.
/// </param>
/// <param name="Ranges">Transfers the object was carried in, retries included.</param>
/// <param name="Retries">Ranges that had to be sent again because their checksum did not match.</param>
public readonly record struct BulkObjectResult(
    BulkStatus Status,
    long BytesTransferred,
    long Length,
    QuiclyErrorCode Code,
    int Ranges,
    int Retries = 0);

/// <summary>A progress report of one bulk object (<see cref="BulkObjectProgressCallback"/>).</summary>
/// <param name="ObjectId">Object identity.</param>
/// <param name="ObjectVersion">Object version.</param>
/// <param name="BytesTransferred">Bytes of the object's range transferred so far.</param>
/// <param name="Length">Bytes the object's range holds.</param>
/// <param name="RangesCompleted">Transfers that have finished.</param>
/// <param name="RangesInFlight">Transfers running right now.</param>
public readonly record struct BulkObjectProgress(
    ulong ObjectId,
    ulong ObjectVersion,
    long BytesTransferred,
    long Length,
    int RangesCompleted,
    int RangesInFlight)
{
    /// <summary>Fraction of the object's range transferred, 0 … 1.</summary>
    public double Fraction => Length <= 0 ? 1 : Math.Min(1, (double)BytesTransferred / Length);
}

/// <summary>
/// Reports a bulk object's progress. Called on the <b>game thread</b> inside <see cref="QuiclyPeer.Poll"/> (send side) or
/// on the <b>transport thread</b> with the budget of <see cref="IBulkObjectSink"/> (receive side), only when the numbers
/// changed, and never after the object's <c>Finish</c>. It must not block or re-enter the peer; an exception it throws is
/// counted in <see cref="PeerStatistics.CallbackFaults"/> and kept in <see cref="QuiclyPeer.LastCallbackFault"/>, never
/// rethrown, so a broken progress callback cannot fail the transfer it is reporting on.
/// </summary>
/// <param name="progress">What has been transferred.</param>
public delegate void BulkObjectProgressCallback(in BulkObjectProgress progress);

/// <summary>
/// A bulk object this end is sending (<see cref="QuiclyPeer.BeginBulkObjectSend"/>): its progress, its completion and
/// its cancellation. Progress and status are readable from any thread; the completion runs its continuations on the
/// thread pool, never inside a scheduler pass.
/// </summary>
public sealed class BulkObjectTransfer
{
    private readonly TaskCompletionSource<BulkObjectResult> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private BulkObjectResult _result;
    private long _bytes;
    private int _ranges;
    private int _status;
    private int _cancelRequested;

    internal BulkObjectTransfer(in BulkObjectDescriptor descriptor, long length)
    {
        Descriptor = descriptor;
        Length = length;
    }

    /// <summary>What is being sent.</summary>
    public BulkObjectDescriptor Descriptor { get; }

    /// <summary>Bytes the object's range carries.</summary>
    public long Length { get; }

    /// <summary>Bytes the peer has accepted, across every range; any thread.</summary>
    public long BytesTransferred => Volatile.Read(ref _bytes);

    /// <summary>Transfers that have finished; any thread.</summary>
    public int RangesCompleted => Volatile.Read(ref _ranges);

    /// <summary>Current status; any thread.</summary>
    public BulkStatus Status => (BulkStatus)Volatile.Read(ref _status);

    /// <summary>Whether the object reached a terminal state.</summary>
    public bool IsFinished => Status != BulkStatus.Running;

    /// <summary>Fraction of the object's range transferred, 0 … 1.</summary>
    public double Progress => Length <= 0 ? 1 : Math.Min(1, (double)BytesTransferred / Length);

    /// <summary>The outcome once <see cref="IsFinished"/>; read <see cref="Status"/> first (it publishes this value).</summary>
    public BulkObjectResult Result => _result;

    /// <summary>Completes with the outcome; continuations run on the thread pool.</summary>
    public Task<BulkObjectResult> Completion => _completion.Task;

    /// <summary>Whether <see cref="Cancel"/> was called.</summary>
    public bool CancelRequested => Volatile.Read(ref _cancelRequested) != 0;

    /// <summary>
    /// Asks for the object to be cancelled (any thread, idempotent): no further range is started, every range in flight is
    /// cancelled as <see cref="BulkTransfer.Cancel"/> would, and the object completes <see cref="BulkStatus.Canceled"/>.
    /// </summary>
    public void Cancel() => Interlocked.Exchange(ref _cancelRequested, 1);

    /// <summary>Publishes the bytes transferred and the ranges done (game thread).</summary>
    internal void Advance(long bytes, int ranges)
    {
        Volatile.Write(ref _bytes, bytes);
        Volatile.Write(ref _ranges, ranges);
    }

    /// <summary>Finishes the object once (game thread, or any thread on teardown).</summary>
    internal void Finish(in BulkObjectResult result)
    {
        if (result.Status == BulkStatus.Running || Volatile.Read(ref _status) != (int)BulkStatus.Running)
        {
            return;
        }

        // The status word publishes the result: a reader that sees a terminal status sees the result written before it.
        _result = result;
        if (Interlocked.CompareExchange(ref _status, (int)result.Status, (int)BulkStatus.Running) != (int)BulkStatus.Running)
        {
            return;
        }

        Volatile.Write(ref _bytes, result.BytesTransferred);
        Volatile.Write(ref _ranges, result.Ranges);
        _completion.TrySetResult(result);
    }
}

/// <summary>
/// A bulk object the peer is sending to this end, as seen when its first range arrives
/// (<see cref="IBulkObjectRouter.SelectTarget"/>).
/// </summary>
public readonly struct BulkObjectInfo
{
    /// <summary>The Bulk channel.</summary>
    public ushort Channel { get; init; }

    /// <summary>Object identity.</summary>
    public ulong ObjectId { get; init; }

    /// <summary>Object version.</summary>
    public ulong ObjectVersion { get; init; }

    /// <summary>Declared size of the whole object (untrusted: nothing is allocated at this size).</summary>
    public long TotalLength { get; init; }

    /// <summary>First object byte the peer's first range carried, which is where this object's assembly starts.</summary>
    public long Offset { get; init; }

    /// <summary>Whether the sender is appending a checksum to each range, so corrupt ranges fail instead of assembling.</summary>
    public bool HasChecksum { get; init; }

    /// <summary><see cref="QuiclyPeer.Index"/> of the receiving peer.</summary>
    public int PeerIndex { get; init; }
}

/// <summary>
/// Where a received bulk object's bytes go (<see cref="IBulkObjectRouter"/>). Both members run on the
/// <b>transport thread</b>: no allocation, no locks, no blocking and no call back into the peer. Bytes are written
/// progressively and never buffered at the declared size; they arrive in object order <em>within</em> a range, and ranges
/// of one object may interleave, so <see cref="Write"/> is a random-access write and must honour the offset it is given
/// rather than appending.
/// </summary>
public interface IBulkObjectSink
{
    /// <summary>Writes decoded object bytes.</summary>
    /// <param name="objectOffset">Absolute object offset of the first byte.</param>
    /// <param name="data">The bytes; valid only during the call.</param>
    void Write(long objectOffset, ReadOnlySpan<byte> data);

    /// <summary>The object ended (exactly once per accepted object).</summary>
    /// <param name="result">The outcome, including what became of the whole-object hash.</param>
    void Finish(in BulkObjectResult result);
}

/// <summary>What the object router does with a bulk object the peer started sending.</summary>
public readonly struct BulkObjectReceiveDecision
{
    private BulkObjectReceiveDecision(IBulkObjectSink? sink, long length, BulkObjectProgressCallback? progress, QuiclyErrorCode code)
    {
        Sink = sink;
        ExpectedLength = length;
        Progress = progress;
        RejectCode = code;
    }

    /// <summary>The application's target, or <see langword="null"/> when the object is refused.</summary>
    public IBulkObjectSink? Sink { get; }

    /// <summary>Bytes the object's range holds, or 0 for "to the end of the object".</summary>
    public long ExpectedLength { get; }

    /// <summary>An optional progress callback, invoked on the transport thread.</summary>
    public BulkObjectProgressCallback? Progress { get; }

    /// <summary>Whether the object is accepted.</summary>
    public bool Accepted => Sink is not null;

    /// <summary>The RESET_STREAM / STOP_SENDING code of a refusal.</summary>
    public QuiclyErrorCode RejectCode { get; }

    /// <summary>Accepts the object and writes its bytes to <paramref name="sink"/>.</summary>
    /// <param name="sink">The target.</param>
    /// <param name="expectedLength">
    /// Bytes the object's range holds. 0 (the default) means "from <see cref="BulkObjectInfo.Offset"/> to the end of the
    /// object", which is what a whole-object send carries; pass the length explicitly when this end asked for a sub-range
    /// with <see cref="QuiclyPeer.RequestBulk"/>, so the driver knows when the object is complete.
    /// </param>
    /// <param name="progress">An optional progress callback (transport thread).</param>
    /// <exception cref="ArgumentNullException"><paramref name="sink"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="expectedLength"/> is negative.</exception>
    public static BulkObjectReceiveDecision Accept(IBulkObjectSink sink, long expectedLength = 0, BulkObjectProgressCallback? progress = null)
    {
        ArgumentNullException.ThrowIfNull(sink);
        ArgumentOutOfRangeException.ThrowIfNegative(expectedLength);
        return new BulkObjectReceiveDecision(sink, expectedLength, progress, QuiclyErrorCode.NoError);
    }

    /// <summary>Refuses the object; the range's stream is reset with <paramref name="code"/>.</summary>
    /// <param name="code">The reset code.</param>
    public static BulkObjectReceiveDecision Reject(QuiclyErrorCode code = QuiclyErrorCode.BulkRejected) => new(null, 0, null, code);
}

/// <summary>
/// Chooses where a bulk object the peer sends goes (<see cref="PeerOptions.BulkObjectRouter"/>). Called <b>once per
/// object</b> — when its first range arrives — rather than once per transfer, which is what lets an application treat a
/// 10 GB file as one object. Without a router every peer-initiated object is refused, as PROTOCOL.md §3.3 requires.
/// </summary>
public interface IBulkObjectRouter
{
    /// <summary>
    /// Accepts or refuses an object (<b>transport thread</b>, budget as <see cref="IBulkObjectSink"/>: no allocation, no
    /// locks, no blocking, no call back into the peer).
    /// </summary>
    /// <param name="info">The object, as its first range declared it.</param>
    /// <returns>The decision.</returns>
    BulkObjectReceiveDecision SelectTarget(in BulkObjectInfo info);

    /// <summary>
    /// The peer refused a range this end asked for with <see cref="QuiclyPeer.RequestBulk"/> (game thread, inside a
    /// scheduler pass). Default: ignored.
    /// </summary>
    /// <param name="request">The range that was refused.</param>
    /// <param name="code">The peer's code.</param>
    void OnRequestRejected(in BulkRangeRequest request, QuiclyErrorCode code)
    {
    }
}
