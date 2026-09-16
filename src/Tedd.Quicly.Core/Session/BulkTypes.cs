using Tedd.Quicly.Core.Control;

namespace Tedd.Quicly.Core.Session;

/// <summary>
/// Describes the object range one bulk transfer carries (<see cref="QuiclyPeer.BeginBulkSendAsync"/>, PROTOCOL.md §3.3).
/// The object's identity is <see cref="ObjectId"/> + <see cref="ObjectVersion"/>; <see cref="Sha256"/>, when present,
/// covers the <em>whole</em> object, not this range.
/// </summary>
/// <param name="Channel">The Bulk channel.</param>
/// <param name="ObjectId">Object identity.</param>
/// <param name="ObjectVersion">Object version (identity is id + version).</param>
/// <param name="TotalLength">Total object length in bytes (at most 2^62 − 1).</param>
/// <param name="Offset">First byte of the range this transfer carries.</param>
/// <param name="Length">
/// Bytes this transfer carries. Exactly 0 means "to the end of the object"; a negative value is an error, not a
/// shorthand. Bounded by the channel's <c>MaxMessageSize</c> (PROTOCOL.md §8).
/// </param>
/// <param name="Compress">Send the body as independently LZ4-decodable chunks (PROTOCOL.md §3.3 <c>Chunked</c>).</param>
/// <param name="Resumable">
/// Re-request the remaining range after an epoch change (PROTOCOL.md §4.1). It is the <em>receiving</em> end that
/// re-requests, so this flag matters on a transfer this end asked for with <see cref="QuiclyPeer.RequestBulk"/>.
/// </param>
public readonly record struct BulkDescriptor(
    ushort Channel,
    ulong ObjectId,
    ulong ObjectVersion,
    long TotalLength,
    long Offset = 0,
    long Length = 0,
    bool Compress = false,
    bool Resumable = false)
{
    /// <summary>
    /// SHA-256 of the <b>whole</b> object (32 bytes) or empty. Computed once per object version by the sender and
    /// verified by the receiver when the last byte of the object has arrived; see <see cref="BulkHashState"/> for how a
    /// transfer that carries only part of the object is handled.
    /// </summary>
    public ReadOnlyMemory<byte> Sha256 { get; init; }

    /// <summary>Bytes this transfer carries: <see cref="Length"/>, or the rest of the object when it is 0.</summary>
    public long EffectiveLength => Length > 0 ? Length : TotalLength - Offset;

    /// <summary>Whether the transfer's range is the whole object, which is what lets the receiver verify <see cref="Sha256"/>.</summary>
    public bool IsWholeObject => Offset == 0 && EffectiveLength == TotalLength;
}

/// <summary>How a bulk transfer ended (PROTOCOL.md §4.3).</summary>
public enum BulkStatus : byte
{
    /// <summary>Still transferring.</summary>
    Running = 0,

    /// <summary>Every byte of the range was accepted by the peer (its <c>BulkProgress</c> reached the range's length).</summary>
    Completed = 1,

    /// <summary>Cancelled by either side (<see cref="BulkTransfer.Cancel"/>, <see cref="QuiclyPeer.CancelBulk"/>, or a peer <c>BulkCancel</c>).</summary>
    Canceled = 2,

    /// <summary>The transfer failed: a stream error, a source that ran dry, or a hash mismatch.</summary>
    Failed = 3,

    /// <summary>Never started: the peer is not connected, the concurrency limit is reached, or the peer refused the request.</summary>
    Rejected = 4,

    /// <summary>The connection was lost while the transfer was running.</summary>
    Disconnected = 5,
}

/// <summary>
/// What became of the sender's whole-object SHA-256 (PROTOCOL.md §3.3: the hash covers the whole object and is verified
/// when its last byte has arrived).
/// </summary>
public enum BulkHashState : byte
{
    /// <summary>The sender sent no hash.</summary>
    None = 0,

    /// <summary>The transfer carried the whole object and its bytes hashed to the value the sender sent.</summary>
    Verified = 1,

    /// <summary>The transfer carried the whole object and the hash did not match: the transfer ends <see cref="BulkStatus.Failed"/>.</summary>
    Mismatch = 2,

    /// <summary>
    /// The transfer carried only part of the object (a resumed range), so this end saw neither the first nor every byte
    /// and cannot verify a whole-object hash. The hash is handed to the application, which verifies the assembled object.
    /// </summary>
    DeferredToApplication = 3,
}

/// <summary>Outcome of a bulk transfer.</summary>
/// <param name="Status">How it ended.</param>
/// <param name="BytesTransferred">Bytes of the range that were transferred (accepted on the send side, written on the receive side).</param>
/// <param name="Hash">What became of the whole-object hash.</param>
/// <param name="Code">The QUIC application error code involved, or <see cref="QuiclyErrorCode.NoError"/>.</param>
public readonly record struct BulkResult(BulkStatus Status, long BytesTransferred, BulkHashState Hash, QuiclyErrorCode Code);

/// <summary>Lets <see cref="BulkTransfer.Cancel"/> reach the engine that owns the transfer (any thread).</summary>
internal interface IBulkCancelSink
{
    /// <summary>Asks for the transfer in <paramref name="slot"/> (tagged <paramref name="serial"/>) to be cancelled.</summary>
    /// <param name="slot">The engine's transfer slot.</param>
    /// <param name="serial">The slot's serial when the transfer started.</param>
    void RequestCancel(int slot, uint serial);
}

/// <summary>
/// A bulk transfer this end is sending (<see cref="QuiclyPeer.BeginBulkSendAsync"/>): its progress, its completion and
/// its cancellation. Progress and status are readable from any thread; the completion runs its continuations on the
/// thread pool, never inside a scheduler pass.
/// </summary>
public sealed class BulkTransfer
{
    private readonly TaskCompletionSource<BulkResult> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private readonly IBulkCancelSink? _owner;
    private readonly int _slot;
    private readonly uint _serial;
    private BulkResult _result;
    private long _bytes;
    private int _status;
    private int _cancelRequested;

    internal BulkTransfer(in BulkDescriptor descriptor, ulong transferId, long length, IBulkCancelSink? owner = null, int slot = -1, uint serial = 0)
    {
        Descriptor = descriptor;
        TransferId = transferId;
        Length = length;
        _owner = owner;
        _slot = slot;
        _serial = serial;
    }

    /// <summary>What is being transferred.</summary>
    public BulkDescriptor Descriptor { get; }

    /// <summary>The transfer id on the wire (unique per peer and direction within the epoch).</summary>
    public ulong TransferId { get; }

    /// <summary>Bytes the transfer carries.</summary>
    public long Length { get; }

    /// <summary>Bytes the peer has accepted (its latest <c>BulkProgress</c>); any thread.</summary>
    public long BytesTransferred => Volatile.Read(ref _bytes);

    /// <summary>Current status; any thread.</summary>
    public BulkStatus Status => (BulkStatus)Volatile.Read(ref _status);

    /// <summary>Whether the transfer reached a terminal state.</summary>
    public bool IsFinished => Status != BulkStatus.Running;

    /// <summary>Fraction of the range transferred, 0 … 1.</summary>
    public double Progress => Length <= 0 ? 1 : Math.Min(1, (double)BytesTransferred / Length);

    /// <summary>The outcome once <see cref="IsFinished"/>; read <see cref="Status"/> first (it publishes this value).</summary>
    public BulkResult Result => _result;

    /// <summary>Completes with the outcome; continuations run on the thread pool.</summary>
    public Task<BulkResult> Completion => _completion.Task;

    /// <summary>Whether <see cref="Cancel"/> was called.</summary>
    public bool CancelRequested => Volatile.Read(ref _cancelRequested) != 0;

    /// <summary>
    /// Asks for the transfer to be cancelled (any thread, idempotent): the next scheduler pass resets the transfer's
    /// stream, sends <c>BulkCancel</c> and completes the transfer <see cref="BulkStatus.Canceled"/>.
    /// </summary>
    public void Cancel()
    {
        if (Interlocked.Exchange(ref _cancelRequested, 1) == 0)
        {
            _owner?.RequestCancel(_slot, _serial);
        }
    }

    /// <summary>Publishes the bytes the peer has accepted (game thread).</summary>
    /// <param name="bytes">Bytes accepted.</param>
    internal void Advance(long bytes) => Volatile.Write(ref _bytes, bytes);

    /// <summary>Finishes the transfer once (game thread, or any thread on teardown).</summary>
    /// <param name="result">The outcome.</param>
    internal void Finish(in BulkResult result)
    {
        if (result.Status == BulkStatus.Running)
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
        _completion.TrySetResult(result);
    }
}

/// <summary>
/// Supplies the bytes of a bulk object (<see cref="QuiclyPeer.BeginBulkSendAsync"/>). Called on the game thread inside a
/// scheduler pass, so it must not block, allocate or re-enter the peer; a random-access read is expected, because a
/// refused stream start makes the engine read the same range again.
/// </summary>
public interface IBulkSource
{
    /// <summary>Copies object bytes starting at <paramref name="offset"/> into <paramref name="destination"/>.</summary>
    /// <param name="offset">Absolute object offset.</param>
    /// <param name="destination">Receives the bytes.</param>
    /// <returns>Bytes copied; 0 means the source ran dry, which fails the transfer.</returns>
    int Read(long offset, Span<byte> destination);
}

/// <summary>
/// Where a received bulk transfer's bytes go (ARCHITECTURE.md §4.2 "Direct mode"). Both members run on the
/// <b>transport thread</b> with the budget of <c>SelectTarget</c>: no allocation, no locks, no blocking and no call back
/// into the peer. Bytes arrive in object order and are written progressively — the engine never buffers the declared
/// size (PROTOCOL.md §3.3: <c>TotalLength</c>, <c>Offset</c> and <c>Length</c> are untrusted).
/// </summary>
public interface IBulkSink
{
    /// <summary>Writes decoded object bytes.</summary>
    /// <param name="objectOffset">Absolute object offset of the first byte.</param>
    /// <param name="data">The bytes; valid only during the call.</param>
    void Write(long objectOffset, ReadOnlySpan<byte> data);

    /// <summary>The transfer ended (exactly once per accepted transfer).</summary>
    /// <param name="result">The outcome, including what became of the whole-object hash.</param>
    void Finish(in BulkResult result);
}

/// <summary>The descriptor of a bulk stream the peer opened, as validated from its header (PROTOCOL.md §3.3).</summary>
public readonly struct BulkTransferInfo
{
    /// <summary>The Bulk channel.</summary>
    public ushort Channel { get; init; }

    /// <summary>The peer's transfer id.</summary>
    public ulong TransferId { get; init; }

    /// <summary>Object identity.</summary>
    public ulong ObjectId { get; init; }

    /// <summary>Object version.</summary>
    public ulong ObjectVersion { get; init; }

    /// <summary>Declared size of the whole object (untrusted).</summary>
    public long TotalLength { get; init; }

    /// <summary>First object byte this transfer carries.</summary>
    public long Offset { get; init; }

    /// <summary>Bytes this transfer carries (already checked against the channel's <c>MaxMessageSize</c>).</summary>
    public long Length { get; init; }

    /// <summary>Whether the sender supplied a whole-object SHA-256.</summary>
    public bool HasHash { get; init; }

    /// <summary>Whether the body is chunked (each chunk independently LZ4-decodable).</summary>
    public bool Chunked { get; init; }

    /// <summary><see cref="QuiclyPeer.Index"/> of the receiving peer.</summary>
    public int PeerIndex { get; init; }

    /// <summary>Whether the range is the whole object, which is what lets this end verify the hash.</summary>
    public bool IsWholeObject => Offset == 0 && Length == TotalLength;
}

/// <summary>What the receive router does with a bulk stream the peer opened.</summary>
public readonly struct BulkReceiveDecision
{
    private BulkReceiveDecision(IBulkSink? sink, QuiclyErrorCode code)
    {
        Sink = sink;
        RejectCode = code;
    }

    /// <summary>The application's target, or <see langword="null"/> when the transfer is refused.</summary>
    public IBulkSink? Sink { get; }

    /// <summary>Whether the transfer is accepted.</summary>
    public bool Accepted => Sink is not null;

    /// <summary>The RESET_STREAM / STOP_SENDING code of a refusal.</summary>
    public QuiclyErrorCode RejectCode { get; }

    /// <summary>Accepts the transfer and writes its bytes to <paramref name="sink"/>.</summary>
    /// <param name="sink">The target.</param>
    /// <exception cref="ArgumentNullException"><paramref name="sink"/> is null.</exception>
    public static BulkReceiveDecision Accept(IBulkSink sink)
    {
        ArgumentNullException.ThrowIfNull(sink);
        return new BulkReceiveDecision(sink, QuiclyErrorCode.NoError);
    }

    /// <summary>Refuses the transfer; its stream is reset with <paramref name="code"/>.</summary>
    /// <param name="code">The reset code.</param>
    public static BulkReceiveDecision Reject(QuiclyErrorCode code = QuiclyErrorCode.BulkRejected) => new(null, code);
}

/// <summary>
/// Chooses where a peer-initiated bulk transfer's bytes go (<see cref="PeerOptions.BulkRouter"/>). Without a router
/// every peer-initiated transfer is refused, which is the PROTOCOL.md §3.3 default ("the application's receive router
/// MUST accept the descriptor (default: reject)") — including the answer to a range this end asked for, so an
/// application that calls <see cref="QuiclyPeer.RequestBulk"/> supplies a router that recognises what it requested.
/// </summary>
public interface IBulkRouter
{
    /// <summary>
    /// Accepts or refuses a bulk stream (<b>transport thread</b>, budget as <see cref="IBulkSink"/>: no allocation, no
    /// locks, no blocking, no call back into the peer).
    /// </summary>
    /// <param name="info">The validated descriptor.</param>
    /// <returns>The decision.</returns>
    BulkReceiveDecision SelectTarget(in BulkTransferInfo info);

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

/// <summary>A range of a bulk object the peer asked for (control message <c>BulkRequest</c>, PROTOCOL.md §3.4).</summary>
public readonly struct BulkRequestInfo
{
    /// <summary>The Bulk channel.</summary>
    public ushort Channel { get; init; }

    /// <summary>The peer's request id (echoed in a <c>BulkReject</c>).</summary>
    public ulong RequestId { get; init; }

    /// <summary>Requested object.</summary>
    public ulong ObjectId { get; init; }

    /// <summary>Requested object version.</summary>
    public ulong ObjectVersion { get; init; }

    /// <summary>First byte of the requested range.</summary>
    public long Offset { get; init; }

    /// <summary>Length of the requested range.</summary>
    public long Length { get; init; }

    /// <summary><see cref="QuiclyPeer.Index"/> of the peer that asked.</summary>
    public int PeerIndex { get; init; }
}

/// <summary>
/// Decides whether the peer may have a range it asked for (<see cref="PeerOptions.BulkAuthorizer"/>). Without an
/// authorizer every request is refused with <c>BulkReject</c>: bulk serving is opt-in (ADR 0009).
/// </summary>
public interface IBulkAuthorizer
{
    /// <summary>Authorises a request (game thread, inside a scheduler pass; must not block or re-enter the peer).</summary>
    /// <param name="request">What the peer asked for.</param>
    /// <returns><see langword="true"/> to serve it.</returns>
    bool Authorize(in BulkRequestInfo request);
}

/// <summary>Supplies the object an authorised <c>BulkRequest</c> asked for (<see cref="PeerOptions.BulkProvider"/>).</summary>
public interface IBulkProvider
{
    /// <summary>
    /// Produces the descriptor and source for an authorised request (game thread, inside a scheduler pass). The
    /// descriptor's channel, object identity and range normally echo the request; a provider may shorten
    /// <see cref="BulkDescriptor.Length"/>, and the requester then asks again for the rest.
    /// </summary>
    /// <param name="request">What the peer asked for.</param>
    /// <param name="descriptor">The transfer to send.</param>
    /// <param name="source">Where its bytes come from.</param>
    /// <returns><see langword="false"/> to refuse the request (answered with <c>BulkReject</c>).</returns>
    bool TryGetObject(in BulkRequestInfo request, out BulkDescriptor descriptor, out IBulkSource? source);
}

/// <summary>
/// Asks the peer to send a range of a bulk object (<see cref="QuiclyPeer.RequestBulk"/>, control message
/// <c>BulkRequest</c>). Named to avoid a clash with <see cref="Control.BulkRequest"/>, the wire message.
/// </summary>
/// <param name="Channel">The Bulk channel.</param>
/// <param name="ObjectId">Object identity.</param>
/// <param name="ObjectVersion">Object version.</param>
/// <param name="Offset">First byte of the range.</param>
/// <param name="Length">Length of the range.</param>
/// <param name="Resumable">Re-request the remaining range after an epoch change (PROTOCOL.md §4.1).</param>
public readonly record struct BulkRangeRequest(ushort Channel, ulong ObjectId, ulong ObjectVersion, long Offset, long Length, bool Resumable = false);
