using System.Net;
using System.Runtime.CompilerServices;

namespace Tedd.Quicly.Core.Transport;

/// <summary>Result of a synchronous transport call. On any value other than <see cref="Success"/> no completion follows.</summary>
public enum TransportStatus : byte
{
    /// <summary>Accepted; a completion will follow.</summary>
    Success = 0,
    /// <summary>The transport is not in a state that allows the operation (not connected, closed, datagrams disabled).</summary>
    InvalidState,
    /// <summary>The payload exceeds the transport's current limit (for example the datagram payload).</summary>
    TooLarge,
    /// <summary>The operation is not supported by this transport (capability missing).</summary>
    NotSupported,
    /// <summary>
    /// The peer's stream limit is exhausted. The refused stream never starts: release it with
    /// <see cref="ITransport.CloseStream"/> and open a new stream after <see cref="ITransportSink.OnStreamsAvailable"/> (see
    /// <see cref="ITransport.StartStream"/>).
    /// </summary>
    StreamLimitReached,
    /// <summary>The transport could not allocate internal resources.</summary>
    OutOfMemory,
    /// <summary>The transport rejected the call for another reason; see the transport's diagnostics.</summary>
    Failed,
}

/// <summary>Kind of QUIC stream.</summary>
public enum StreamKind : byte
{
    /// <summary>Data flows only from the opener to the peer.</summary>
    Unidirectional = 0,
    /// <summary>Data flows both ways.</summary>
    Bidirectional = 1,
}

/// <summary>Which direction of a stream an abort applies to.</summary>
[Flags]
public enum StreamAbortDirection : byte
{
    /// <summary>Abort our sending side (RESET_STREAM).</summary>
    Send = 1,
    /// <summary>Abort our receiving side (STOP_SENDING).</summary>
    Receive = 2,
    /// <summary>Both directions.</summary>
    Both = Send | Receive,
}

/// <summary>Flags for <see cref="ITransport.SendStream"/> and <see cref="ITransport.SendDatagram"/>.</summary>
[Flags]
public enum TransportSendFlags : byte
{
    /// <summary>No special handling.</summary>
    None = 0,
    /// <summary>This is the last send on the stream; the transport closes its sending side after it.</summary>
    Fin = 1,
    /// <summary>More sends will follow soon; the transport may delay packetisation to coalesce them.</summary>
    DelaySend = 2,
    /// <summary>Start the stream with this send (saves an explicit start call).</summary>
    Start = 4,
    /// <summary>Datagram: prioritise over other datagrams. Stream: prioritise the work over other connection work.</summary>
    Priority = 8,
    /// <summary>Datagram: drop instead of queueing when it cannot be sent immediately (congestion).</summary>
    CancelOnBlocked = 16,
    /// <summary>Stream: cancel the stream when loss is detected (lossy stream emulation).</summary>
    CancelOnLoss = 32,
}

/// <summary>Lifecycle of a datagram send, mirroring MsQuic's <c>QUIC_DATAGRAM_SEND_STATE</c>.</summary>
public enum DatagramSendState : byte
{
    /// <summary>Not yet reported.</summary>
    Unknown = 0,
    /// <summary>Packetised and handed to the network. The payload may be released. Not final.</summary>
    Sent = 1,
    /// <summary>Suspected lost; may still be acknowledged later. Not final.</summary>
    LostSuspect = 2,
    /// <summary>Lost and no longer tracked. Final.</summary>
    LostDiscarded = 3,
    /// <summary>Acknowledged by the peer's transport. Final.</summary>
    Acknowledged = 4,
    /// <summary>Acknowledged after having been suspected lost. Final.</summary>
    AcknowledgedSpurious = 5,
    /// <summary>Never sent (blocked, oversized after an MTU change, or connection closed). Final; the payload may be released.</summary>
    Canceled = 6,
}

/// <summary>Helpers for <see cref="DatagramSendState"/>.</summary>
public static class DatagramSendStateExtensions
{
    /// <summary>True for states after which no further state change is reported for the same context.</summary>
    public static bool IsFinal(this DatagramSendState state) => state >= DatagramSendState.LostDiscarded;

    /// <summary>True when the payload buffer may be released (Sent or any final state).</summary>
    public static bool ReleasesPayload(this DatagramSendState state) => state >= DatagramSendState.Sent;
}

/// <summary>Coarse state of a transport connection.</summary>
public enum TransportState : byte
{
    /// <summary>Created, not yet connected.</summary>
    Connecting = 0,
    /// <summary>Handshake complete; data may flow.</summary>
    Connected,
    /// <summary>Closing; no new sends are accepted.</summary>
    Closing,
    /// <summary>Closed; every send has completed or been canceled.</summary>
    Closed,
}

/// <summary>Why a transport connection closed.</summary>
public enum TransportCloseReason : byte
{
    /// <summary>We closed it.</summary>
    Local = 0,
    /// <summary>The peer closed it with an application error code.</summary>
    Peer,
    /// <summary>
    /// The transport closed it (idle timeout, handshake failure, refused connection, protocol error, link loss); the error code
    /// and status reported with it are transport-specific (see <see cref="ITransportSink.OnClosed"/>).
    /// </summary>
    Transport,
}

/// <summary>Local identifier of a stream: a slot in the transport's stream table plus a generation to detect stale ids.</summary>
public readonly record struct TransportStreamId(int Slot, uint Generation)
{
    /// <summary>True when the id refers to an allocated slot.</summary>
    public bool IsValid => Generation != 0;

    /// <summary>An id that never refers to a stream.</summary>
    public static TransportStreamId None => default;
}

/// <summary>What a transport can do. Read at connect time and updated through <see cref="ITransportSink.OnDatagramCapabilityChanged"/>.</summary>
public struct TransportCapabilities
{
    /// <summary>Unreliable datagrams are available (negotiated with the peer).</summary>
    public bool Datagrams;
    /// <summary>The transport reports <see cref="DatagramSendState"/> (loss/ack) per datagram.</summary>
    public bool DatagramSendState;
    /// <summary>Largest datagram payload accepted right now. Dynamic: grows with path MTU discovery, shrinks on migration.</summary>
    public int MaxDatagramPayload;
    /// <summary>Per-stream priority is honoured.</summary>
    public bool StreamPriority;
    /// <summary>The transport can receive stream data directly into application-provided buffers.</summary>
    public bool AppOwnedReceiveBuffers;
    /// <summary>The transport reports the ideal number of bytes to keep outstanding per stream.</summary>
    public bool IdealSendBufferSize;
    /// <summary>
    /// Datagram sends honour <see cref="TransportSendFlags.CancelOnBlocked"/>; when false the flag is ignored (for example
    /// with an MsQuic library older than 2.4).
    /// </summary>
    public bool CancelOnBlocked;
}

/// <summary>Snapshot of transport-level statistics. Fixed layout, no references.</summary>
public struct TransportStatistics
{
    /// <summary>Smoothed round-trip time in microseconds.</summary>
    public uint RttMicros;
    /// <summary>Minimum observed round-trip time in microseconds.</summary>
    public uint MinRttMicros;
    /// <summary>Maximum observed round-trip time in microseconds.</summary>
    public uint MaxRttMicros;
    /// <summary>Round-trip time variance in microseconds.</summary>
    public uint RttVarianceMicros;
    /// <summary>Current congestion window in bytes.</summary>
    public uint CongestionWindowBytes;
    /// <summary>Bytes sent but not yet acknowledged.</summary>
    public ulong BytesInFlight;
    /// <summary>Current path MTU.</summary>
    public ushort PathMtu;
    /// <summary>Total bytes sent (UDP payload).</summary>
    public ulong SendTotalBytes;
    /// <summary>Total bytes received (UDP payload).</summary>
    public ulong RecvTotalBytes;
    /// <summary>Total QUIC packets sent.</summary>
    public ulong SendTotalPackets;
    /// <summary>Total QUIC packets received.</summary>
    public ulong RecvTotalPackets;
    /// <summary>Packets suspected lost.</summary>
    public ulong SendSuspectedLostPackets;
    /// <summary>Packets suspected lost that were later acknowledged.</summary>
    public ulong SendSpuriousLostPackets;
    /// <summary>Number of congestion events.</summary>
    public uint CongestionEvents;
}

/// <summary>Outcome of <see cref="ITransportSink.OnStreamReceived"/>.</summary>
/// <remarks>
/// <para><see cref="Consumed"/> with every indicated byte completes the indication; the next indication starts where it ended.</para>
/// <para><b>Partial consumption without <c>Pending</c></b> (at least one byte, fewer than indicated): the transport keeps the
/// rest and indicates it again at the offset where consumption stopped, without waiting for new data, together with whatever
/// has arrived since (MsQuic: right after the callback returns; the simulator: at the next advance step). A sink that consumes
/// only whole frames therefore sees an incomplete frame again, possibly before more of it has arrived: it should take such a
/// tail into its own parser state or return <see cref="PendingAfter"/>.</para>
/// <para><b>Consuming nothing</b> of a non-empty indication without <c>Pending</c> counts as <c>PendingAfter(0)</c>: nothing
/// more is delivered on the stream until <see cref="ITransport.ResumeStreamReceive"/>. An empty indication (it carries only
/// the FIN) is fully consumed by <c>Consumed(0)</c>.</para>
/// <para><b><see cref="PendingAfter"/></b>: the given bytes are consumed now and the rest is held back; nothing more is
/// delivered on the stream until <see cref="ITransport.ResumeStreamReceive"/> credits further bytes and has the remainder
/// indicated again. A resume issued on another thread while the receive callback is still returning takes effect once it
/// has returned. Do not call <see cref="ITransport.ResumeStreamReceive"/> for a stream from inside its own receive callback.</para>
/// <para><b>Holding the end of a stream.</b> A sink that holds an indication which carried the FIN and left no byte
/// unconsumed — <c>PendingAfter(all)</c> with the FIN set, or <c>PendingAfter(0)</c> of an indication that carried only the
/// FIN — has seen the end of the stream and must treat it as seen: nothing is indicated again after the resume, and only
/// <see cref="ITransportSink.OnStreamPeerSendShutdown"/> follows. When bytes are left, the rest is indicated again, with the
/// FIN.</para>
/// </remarks>
public readonly record struct ReceiveResult(int BytesConsumed, bool Pending)
{
    /// <summary>All given bytes consumed synchronously.</summary>
    public static ReceiveResult Consumed(int bytes) => new(bytes, false);

    /// <summary>
    /// <paramref name="bytes"/> consumed now; the rest is held back (back-pressure). The transport delivers no further
    /// data on the stream until <see cref="ITransport.ResumeStreamReceive"/> is called.
    /// </summary>
    public static ReceiveResult PendingAfter(int bytes) => new(bytes, true);
}

/// <summary>Fixed 255-byte inline buffer for an ALPN string.</summary>
[InlineArray(255)]
public struct AlpnBuffer
{
    private byte _element0;
}

/// <summary>Information available when a connection completes its handshake.</summary>
public struct TransportConnectedInfo
{
    /// <summary>The peer's address.</summary>
    public IPEndPoint? RemoteEndPoint;
    /// <summary>Our address.</summary>
    public IPEndPoint? LocalEndPoint;
    /// <summary>Negotiated ALPN, ASCII; only the first <see cref="AlpnLength"/> bytes are valid.</summary>
    public AlpnBuffer Alpn;
    /// <summary>Length of the negotiated ALPN.</summary>
    public byte AlpnLength;
    /// <summary>True when the TLS session was resumed (0-RTT capable); QUICLY rejects such sessions by default.</summary>
    public bool SessionResumed;
    /// <summary>Capabilities as known at connect time.</summary>
    public TransportCapabilities Capabilities;
}

/// <summary>Information available for a new incoming connection before any TLS work has been done.</summary>
public struct NewConnectionInfo
{
    /// <summary>The peer's address.</summary>
    public IPEndPoint? RemoteEndPoint;
    /// <summary>Server name from the ClientHello, if any.</summary>
    public string? ServerName;
    /// <summary>Negotiated ALPN, ASCII; only the first <see cref="AlpnLength"/> bytes are valid.</summary>
    public AlpnBuffer Alpn;
    /// <summary>Length of the negotiated ALPN.</summary>
    public byte AlpnLength;
}
