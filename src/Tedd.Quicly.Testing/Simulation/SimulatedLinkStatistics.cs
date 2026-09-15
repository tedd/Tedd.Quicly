namespace Tedd.Quicly.Testing.Simulation;

/// <summary>
/// Counters of one direction of a simulated link, owned by the transport that sends in that direction. Fixed layout,
/// no references; read with <see cref="SimulatedTransport.GetLinkStatistics"/>.
/// </summary>
public struct SimulatedLinkStatistics
{
    /// <summary>Datagrams accepted by <c>SendDatagram</c>.</summary>
    public long DatagramsSent;
    /// <summary>Datagrams that reached the peer's sink.</summary>
    public long DatagramsDelivered;
    /// <summary>Datagrams lost in flight (<see cref="LinkOptions.LossPercent"/>).</summary>
    public long DatagramsLost;
    /// <summary>Datagrams dropped because the serialization queue was full (<see cref="LinkOptions.MaxQueueBytes"/>).</summary>
    public long DatagramsDropped;
    /// <summary>Datagrams that completed <c>Canceled</c> (close, MTU drop, <c>CancelOnBlocked</c>).</summary>
    public long DatagramsCanceled;
    /// <summary>Datagrams given an extra reordering delay (<see cref="LinkOptions.ReorderPercent"/>).</summary>
    public long DatagramsReordered;
    /// <summary>Payload bytes of accepted datagrams.</summary>
    public long DatagramBytesSent;
    /// <summary>Payload bytes of delivered datagrams.</summary>
    public long DatagramBytesDelivered;
    /// <summary>Stream payload bytes accepted by <c>SendStream</c>.</summary>
    public long StreamBytesSent;
    /// <summary>Stream payload bytes that reached the peer's transport.</summary>
    public long StreamBytesDelivered;
    /// <summary>Stream packets (chunks of at most the datagram payload size) put on the wire.</summary>
    public long StreamPacketsSent;
    /// <summary>Simulated stream packet losses, each costing one <see cref="LinkOptions.RetransmitDelayMicros"/>.</summary>
    public long StreamRetransmissions;
    /// <summary>Highest number of bytes waiting in the serialization queue.</summary>
    public long MaxQueuedBytes;
}
