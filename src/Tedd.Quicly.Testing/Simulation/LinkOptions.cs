namespace Tedd.Quicly.Testing.Simulation;

/// <summary>A scheduled change of the path's maximum datagram payload (for example after migration or PMTU discovery).</summary>
/// <param name="AtMicros">When the change happens, in microseconds after the link was created.</param>
/// <param name="MaxDatagramPayload">The new maximum datagram payload in bytes.</param>
public readonly record struct MtuChange(long AtMicros, int MaxDatagramPayload);

/// <summary>
/// Network conditions of one simulated link. Every property applies to both directions independently (each direction
/// has its own serialization queue and its own random draws). The values are copied when a link is created; changing
/// the instance afterwards does not affect existing links.
/// </summary>
public sealed class LinkOptions
{
    /// <summary>One-way propagation delay in microseconds. Round-trip time is twice this. Default 0.</summary>
    public long DelayMicros { get; set; }

    /// <summary>Extra one-way delay drawn uniformly from [0, JitterMicros] per packet. Default 0.</summary>
    public long JitterMicros { get; set; }

    /// <summary>Percentage (0 to 100) of datagrams lost in flight. Default 0.</summary>
    public double LossPercent { get; set; }

    /// <summary>
    /// Percentage (0 to 100) of stream packets lost in flight. Streams are reliable, so a loss is modelled as a
    /// retransmission delay (see <see cref="RetransmitDelayMicros"/>), never as missing data; one packet is lost at
    /// most 32 times in a row. Default 0.
    /// </summary>
    public double StreamLossPercent { get; set; }

    /// <summary>
    /// Percentage (0 to 100) of datagrams held back by an extra random delay of up to one more one-way delay
    /// (at least 1 ms) so that they arrive after datagrams sent later. Default 0.
    /// </summary>
    public double ReorderPercent { get; set; }

    /// <summary>
    /// Serialization rate of each direction in bits per second; 0 (the default) means unlimited. With a limit every
    /// packet (datagram or stream chunk) waits in a per-direction queue served highest priority first: datagrams sent
    /// with <c>Priority</c>, then other datagrams, then stream data by stream priority (plus the send's <c>Priority</c> flag).
    /// </summary>
    public long BandwidthBitsPerSecond { get; set; }

    /// <summary>
    /// Capacity of each direction's serialization queue in bytes (only used when <see cref="BandwidthBitsPerSecond"/>
    /// is set). A datagram that would push the queue beyond it is dropped (reported <c>Sent</c> then
    /// <c>LostDiscarded</c>, or <c>Canceled</c> when sent with <c>CancelOnBlocked</c>). Stream data is never dropped.
    /// Default 256 KiB.
    /// </summary>
    public int MaxQueueBytes { get; set; } = 256 * 1024;

    /// <summary>Initial maximum datagram payload in bytes (also the stream packet size). Default 1200.</summary>
    public int MaxDatagramPayload { get; set; } = 1200;

    /// <summary>Whether datagrams were negotiated. Default true.</summary>
    public bool DatagramsEnabled { get; set; } = true;

    /// <summary>
    /// Whether the transports report acknowledgement and loss per datagram. When false each datagram reports exactly
    /// one state: <c>Sent</c> (or <c>Canceled</c>). Default true.
    /// </summary>
    public bool DatagramSendStateReporting { get; set; } = true;

    /// <summary>Unidirectional streams each endpoint initially lets its peer have open. Default 0 (MsQuic before admission).</summary>
    public ushort PeerUnidiStreams { get; set; }

    /// <summary>Bidirectional streams each endpoint initially lets its peer have open. Default 1 (MsQuic before admission).</summary>
    public ushort PeerBidiStreams { get; set; } = 1;

    /// <summary>Scheduled changes of the maximum datagram payload, relative to link creation.</summary>
    public IList<MtuChange> MtuChanges { get; private set; } = new List<MtuChange>();

    /// <summary>
    /// When set, the link dies this many microseconds after creation and both ends close with
    /// <see cref="Core.Transport.TransportCloseReason.Transport"/> and <see cref="SimulatedTransport.StatusDisconnected"/>.
    /// </summary>
    public long? DisconnectAtMicros { get; set; }

    /// <summary>
    /// Time from <see cref="SimulatedNetwork.CreatePair"/> until both ends are connected; <c>null</c> (default) means one
    /// round trip (2 × <see cref="DelayMicros"/>). Connections made through <see cref="SimulatedConnector"/> ignore it:
    /// the client connects after one round trip, the server half a round trip later.
    /// </summary>
    public long? ConnectDelayMicros { get; set; }

    /// <summary>Delay added per simulated stream packet loss: 2 × delay + 2 × jitter + 1 ms (a probe timeout).</summary>
    public long RetransmitDelayMicros => 2 * DelayMicros + 2 * JitterMicros + 1000;

    internal LinkOptions Clone()
    {
        LinkOptions copy = (LinkOptions)MemberwiseClone();
        copy.MtuChanges = new List<MtuChange>(MtuChanges);
        return copy;
    }

    internal void Validate()
    {
        ArgumentOutOfRangeException.ThrowIfNegative(DelayMicros);
        ArgumentOutOfRangeException.ThrowIfNegative(JitterMicros);
        ValidatePercent(LossPercent, nameof(LossPercent));
        ValidatePercent(StreamLossPercent, nameof(StreamLossPercent));
        ValidatePercent(ReorderPercent, nameof(ReorderPercent));
        ArgumentOutOfRangeException.ThrowIfNegative(BandwidthBitsPerSecond);
        ArgumentOutOfRangeException.ThrowIfNegative(MaxQueueBytes);
        ValidatePayload(MaxDatagramPayload);
        if (DisconnectAtMicros is < 0)
            throw new ArgumentOutOfRangeException(nameof(DisconnectAtMicros), DisconnectAtMicros, "Must be non-negative.");
        if (ConnectDelayMicros is < 0)
            throw new ArgumentOutOfRangeException(nameof(ConnectDelayMicros), ConnectDelayMicros, "Must be non-negative.");
        foreach (MtuChange change in MtuChanges)
        {
            if (change.AtMicros < 0)
                throw new ArgumentOutOfRangeException(nameof(MtuChanges), change.AtMicros, "MTU change time must be non-negative.");
            ValidatePayload(change.MaxDatagramPayload);
        }
    }

    private static void ValidatePercent(double value, string name)
    {
        if (!(value >= 0 && value <= 100))
            throw new ArgumentOutOfRangeException(name, value, "Must be between 0 and 100.");
    }

    private static void ValidatePayload(int value)
    {
        if (value is < 1 or > 65_000)
            throw new ArgumentOutOfRangeException(nameof(MaxDatagramPayload), value, "Must be between 1 and 65000.");
    }
}
