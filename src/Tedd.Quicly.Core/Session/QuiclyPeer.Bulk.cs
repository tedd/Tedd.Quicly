using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session.Engines;

namespace Tedd.Quicly.Core.Session;

/// <summary>
/// The bulk surface a receiving application needs (docs/design/session-layer.md §7.7). Sending is
/// <see cref="BeginBulkSendAsync"/> in QuiclyPeer.Send.cs and cancelled through <see cref="BulkTransfer.Cancel"/>; a
/// transfer this end is <em>receiving</em> has no <see cref="BulkTransfer"/> of its own (its descriptor arrives on the
/// transport thread and its bytes go straight to the application's <see cref="IBulkSink"/>), so it is cancelled by
/// transfer id instead.
/// </summary>
public sealed partial class QuiclyPeer
{
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
}
