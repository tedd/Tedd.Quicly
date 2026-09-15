using Tedd.Quicly.Core.Channels;
using Tedd.Quicly.Core.Control;
using Tedd.Quicly.Core.Session;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Server;

/// <summary>
/// A transport that does nothing. The server creates (and frees) one real server peer over it at construction to validate
/// the derived <see cref="PeerOptions"/> against the channel table: PeerOptions' own validation is internal to Core, and
/// without the probe a bad template would surface only as every connection being refused.
/// </summary>
internal sealed unsafe class ProbeTransport : ITransport
{
    private ITransportSink? _sink;

    /// <inheritdoc/>
    public TransportCapabilities Capabilities => default;

    /// <inheritdoc/>
    public TransportState State => TransportState.Connecting;

    /// <summary>Creates and frees a server peer with <paramref name="options"/> and <paramref name="table"/>.</summary>
    /// <exception cref="ArgumentException">An option is out of range (thrown by the peer).</exception>
    public static void Validate(PeerOptions options, ChannelTable table)
    {
        ProbeTransport transport = new();
        NewConnectionInfo info = default;
        QuiclyPeer peer = QuiclyPeer.CreateServerPeer(transport, in info, table, options, RejectAll.Instance);
        transport._sink = peer.TransportSink;
        peer.Dispose(); // closes the probe, which reports the close at once, so the peer frees its memory here
    }

    /// <inheritdoc/>
    public TransportStatus SendDatagram(TransportSegment* segments, int count, ulong context, TransportSendFlags flags) => TransportStatus.InvalidState;

    /// <inheritdoc/>
    public TransportStatus OpenStream(StreamKind kind, ulong context, ushort priority, out TransportStreamId id)
    {
        id = default;
        return TransportStatus.InvalidState;
    }

    /// <inheritdoc/>
    public TransportStatus StartStream(TransportStreamId id) => TransportStatus.InvalidState;

    /// <inheritdoc/>
    public TransportStatus SendStream(TransportStreamId id, TransportSegment* segments, int count, ulong context, TransportSendFlags flags) => TransportStatus.InvalidState;

    /// <inheritdoc/>
    public void AbortStream(TransportStreamId id, ulong errorCode, StreamAbortDirection direction)
    {
    }

    /// <inheritdoc/>
    public void SetStreamPriority(TransportStreamId id, ushort priority)
    {
    }

    /// <inheritdoc/>
    public long GetQuicStreamId(TransportStreamId id) => -1;

    /// <inheritdoc/>
    public void ResumeStreamReceive(TransportStreamId id, int bytesConsumed)
    {
    }

    /// <inheritdoc/>
    public void CloseStream(TransportStreamId id)
    {
    }

    /// <inheritdoc/>
    public void UpdatePeerStreamLimits(ushort bidirectional, ushort unidirectional)
    {
    }

    /// <inheritdoc/>
    public void Close(ulong errorCode, ReadOnlySpan<byte> reason)
    {
        ITransportSink? sink = _sink;
        _sink = null;
        sink?.OnClosed(TransportCloseReason.Local, errorCode, 0);
    }

    /// <inheritdoc/>
    public void GetStatistics(out TransportStatistics statistics) => statistics = default;

    /// <inheritdoc/>
    public void Dispose()
    {
    }

    private sealed class RejectAll : IPeerAdmission
    {
        public static readonly RejectAll Instance = new();

        public AdmissionResult Admit(in HelloInfo hello, QuiclyPeer peer) => AdmissionResult.Reject(HelloStatus.InternalError);
    }
}
