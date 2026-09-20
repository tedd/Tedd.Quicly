using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session.Engines;

namespace Tedd.Quicly.Core.Session;

/// <summary>
/// The bulk surface a receiving application needs (docs/design/session-layer.md §7.7), and the object-level surface on
/// top of it. Sending one <em>transfer</em> is <see cref="BeginBulkSendAsync"/> in QuiclyPeer.Send.cs and is bounded by
/// the channel's <c>MaxMessageSize</c>; sending an <em>object</em> of any size is <see cref="BeginBulkObjectSend"/>,
/// which splits it into transfers, and a transfer this end is <em>receiving</em> has no <see cref="BulkTransfer"/> of
/// its own (its descriptor arrives on the transport thread and its bytes go straight to the application's
/// <see cref="IBulkSink"/>), so it is cancelled by transfer id instead.
/// </summary>
public sealed partial class QuiclyPeer
{
    private BulkObjectSender? _bulkObjects;

    /// <summary>
    /// Cancels a bulk transfer the peer is sending to this end (game thread): its stream is stopped with
    /// <see cref="QuiclyErrorCode.BulkCanceled"/> (STOP_SENDING), a <c>BulkCancel</c> goes to the peer so it stops
    /// reading its source, and the application's <see cref="IBulkSink.Finish"/> reports
    /// <see cref="BulkStatus.Canceled"/>.
    /// </summary>
    /// <param name="channel">The Bulk channel the transfer arrived on.</param>
    /// <param name="transferId">The peer's transfer id (<see cref="BulkTransferInfo.TransferId"/>).</param>
    /// <param name="code">The code sent to the peer; the default is <see cref="QuiclyErrorCode.BulkCanceled"/>.</param>
    /// <returns><see langword="false"/> when no such transfer is running (already finished, or never accepted).</returns>
    /// <exception cref="ArgumentException"><paramref name="channel"/> is not a <see cref="ChannelMode.Bulk"/> channel of this peer's table.</exception>
    /// <exception cref="ObjectDisposedException">The peer was disposed.</exception>
    public bool CancelBulk(ushort channel, ulong transferId, QuiclyErrorCode code = QuiclyErrorCode.BulkCanceled)
    {
        ThrowIfDisposed();
        int index = ChannelIndexOrThrow(channel);
        return _core.GetEngine(index) is BulkEngine bulk && bulk.CancelReceive(index, transferId, code);
    }

    /// <summary>
    /// Sends a whole bulk object — a file, a snapshot, an asset — of any size up to 2^62 − 1 bytes, and completes when
    /// the peer has accepted its last byte (game thread).
    /// </summary>
    /// <remarks>
    /// <para>A Bulk channel's <c>MaxMessageSize</c> bounds one <em>transfer</em> and nothing else (PROTOCOL.md §8), so an
    /// object larger than that is carried by several: this issues them in ascending ranges, keeps
    /// <see cref="PeerOptions.BulkObjectRangesInFlight"/> of them going at once, gives each one its own checksum so a
    /// corrupt range is caught at its end and re-sent alone, and hands the application one completion instead of one per
    /// range. A peer whose <see cref="PeerOptions.BulkObjectRouter"/> is set reassembles the object from them.</para>
    /// <para><paramref name="source"/> is read at absolute object offsets on the game thread inside a scheduler pass, and
    /// never twice for bytes already sent, so nothing is copied or buffered: a 10 GB object costs the driver a few
    /// hundred bytes. Its budget is <see cref="IBulkSource"/>'s — no blocking, no allocation, no re-entering the peer.</para>
    /// </remarks>
    /// <param name="descriptor">The object and the range of it to send.</param>
    /// <param name="source">Where the object's bytes come from.</param>
    /// <param name="progress">
    /// Called on the game thread inside <see cref="Poll"/> every <see cref="PeerOptions.BulkObjectProgressBytes"/>;
    /// <see langword="null"/> for none. <see cref="BulkObjectTransfer.BytesTransferred"/> carries the same numbers for a
    /// caller that would rather read than be told.
    /// </param>
    /// <returns>
    /// The object's handle. It is already finished <see cref="BulkStatus.Rejected"/> when this end is sending
    /// <see cref="PeerOptions.BulkObjectsPerDirection"/> objects already, or the peer is not connected.
    /// </returns>
    /// <exception cref="ArgumentException"><paramref name="descriptor"/>'s channel is not a Bulk channel of this peer's table.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The descriptor's range or hash is malformed.</exception>
    /// <exception cref="ArgumentNullException"><paramref name="source"/> is null.</exception>
    /// <exception cref="ObjectDisposedException">The peer was disposed.</exception>
    public BulkObjectTransfer BeginBulkObjectSend(
        in BulkObjectDescriptor descriptor, IBulkSource source, BulkObjectProgressCallback? progress = null)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(source);
        int index = ChannelIndexOrThrow(descriptor.Channel);
        ChannelDefinition channel = _core.GetChannel(index);
        if (channel.Mode != ChannelMode.Bulk)
        {
            throw new ArgumentException($"Channel {channel.Id} is not a Bulk channel of this peer.", nameof(descriptor));
        }

        long total = descriptor.TotalLength;
        long length = descriptor.EffectiveLength;
        if (total < 0 || total > (long)Primitives.VarInt.MaxValue || descriptor.Offset < 0 || descriptor.Offset > total
            || descriptor.Length < 0 || length <= 0 || length > total - descriptor.Offset)
        {
            throw new ArgumentOutOfRangeException(
                nameof(descriptor), "A bulk object needs Length >= 0 (0 = to the end) and Offset + Length <= TotalLength <= 2^62-1.");
        }

        int max = _core.EffectiveMaxMessageSize(channel);
        long rangeBytes = _core.BulkObjectRangeBytes > 0 ? Math.Min(_core.BulkObjectRangeBytes, max) : max;
        BulkObjectSender sender = _bulkObjects ??= new BulkObjectSender(
            this, _core.BulkObjectsPerDirection, _core.BulkObjectRangesInFlight, _core.BulkObjectProgressBytes, _core.BulkObjectRangeRetries, NoteObjectFault);
        return sender.Begin(in descriptor, rangeBytes, source, progress);
    }

    /// <summary>
    /// Sends a whole bulk object and awaits its outcome — <see cref="BeginBulkObjectSend"/> plus its completion, for a
    /// caller that has nothing to do while the object crosses.
    /// </summary>
    /// <param name="descriptor">The object and the range of it to send.</param>
    /// <param name="source">Where the object's bytes come from.</param>
    /// <param name="progress">An optional progress callback (game thread, inside <see cref="Poll"/>).</param>
    /// <param name="cancellationToken">Cancels the object, as <see cref="BulkObjectTransfer.Cancel"/> would.</param>
    /// <returns>The object's outcome; it does not throw on a failed transfer, it reports it.</returns>
    public async Task<BulkObjectResult> SendBulkObjectAsync(
        BulkObjectDescriptor descriptor,
        IBulkSource source,
        BulkObjectProgressCallback? progress = null,
        CancellationToken cancellationToken = default)
    {
        BulkObjectTransfer transfer = BeginBulkObjectSend(in descriptor, source, progress);
        if (!cancellationToken.CanBeCanceled)
        {
            return await transfer.Completion.ConfigureAwait(false);
        }

        using CancellationTokenRegistration registration = cancellationToken.Register(
            static state => ((BulkObjectTransfer)state!).Cancel(), transfer);
        return await transfer.Completion.ConfigureAwait(false);
    }

    /// <summary>Bulk objects this end is sending right now (tests).</summary>
    internal int LiveBulkObjectSends => _bulkObjects?.LiveObjects ?? 0;

    /// <summary>Whether the object driver has game-thread work waiting (a range finished, or one is owed a retry).</summary>
    private bool HasBulkObjectWork => _bulkObjects is { HasWork: true };

    /// <summary>Folds finished ranges and starts the next ones (game thread, from <see cref="Poll"/>).</summary>
    private void PumpBulkObjects()
    {
        if (_bulkObjects is { HasWork: true } sender && _state == PeerState.Connected)
        {
            sender.Pump();
        }
    }

    /// <summary>Ends every bulk object still in flight in either direction (game thread, peer teardown).</summary>
    /// <param name="status">Why the objects are ending.</param>
    private void AbortBulkObjects(BulkStatus status)
    {
        _bulkObjects?.AbortAll(status);
        _core.BulkObjectReceiver?.AbortAll(status);
    }

    /// <summary>
    /// Records what an application's bulk object progress callback threw: counted as
    /// <see cref="PeerStatistics.CallbackFaults"/> and kept in <see cref="LastCallbackFault"/>, never rethrown, so a
    /// broken progress callback cannot fail the transfer it is reporting on.
    /// </summary>
    /// <param name="exception">What the callback threw.</param>
    internal void NoteObjectFault(Exception exception) => NoteHandlerFault(exception);
}
