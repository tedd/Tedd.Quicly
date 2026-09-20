using System.Runtime.InteropServices;
using Tedd.Quicly.Core.Transport;

namespace Tedd.Quicly.Core.Session;

/// <summary>
/// Snapshot of a peer's statistics (<see cref="QuiclyPeer.GetStatistics"/>). Fixed layout, no references, filled
/// without allocation (ADR 0008 invariant 13). Counters are totals since the peer was created.
/// </summary>
[StructLayout(LayoutKind.Sequential)]
public struct PeerStatistics
{
    /// <summary>The transport's own statistics (QUIC RTT, congestion window, bytes and packets).</summary>
    public TransportStatistics Transport;

    /// <summary>Application-level smoothed RTT from Ping/Pong (µs); 0 before the first sample.</summary>
    public long SmoothedRttMicros;

    /// <summary>Smallest application RTT sample (µs).</summary>
    public long MinRttMicros;

    /// <summary>Largest application RTT sample (µs).</summary>
    public long MaxRttMicros;

    /// <summary>Application RTT variance estimate (µs, RFC 6298 style).</summary>
    public long RttVarianceMicros;

    /// <summary>Most recent application RTT sample (µs).</summary>
    public long LatestRttMicros;

    /// <summary>Published clock offset: remote connection-relative micros minus local connection-relative micros.</summary>
    public long ClockOffsetMicros;

    /// <summary>One-way jitter estimate from consecutive Pong timestamps (µs).</summary>
    public long JitterMicros;

    /// <summary>RTT samples taken.</summary>
    public long RttSamples;

    /// <summary>Current maximum datagram payload of the transport.</summary>
    public int MaxDatagramPayload;

    /// <summary>Receive ring capacity.</summary>
    public int ReceiveRingCapacity;

    /// <summary>Highest receive ring occupancy seen.</summary>
    public int ReceiveRingHighWater;

    /// <summary>Send entries in use now.</summary>
    public int SendEntriesInUse;

    /// <summary>Send table capacity.</summary>
    public int SendTableCapacity;

    /// <summary>Datagrams received (any channel, before validation).</summary>
    public long DatagramsReceived;

    /// <summary>Datagram payload bytes received.</summary>
    public long DatagramBytesReceived;

    /// <summary>Stream bytes received (all streams).</summary>
    public long StreamBytesReceived;

    /// <summary>Control messages received (datagram and stream).</summary>
    public long ControlMessagesReceived;

    /// <summary>Datagrams dropped as malformed (framing errors, unknown channels, bad control datagrams).</summary>
    public long MalformedDatagrams;

    /// <summary>Application datagrams and streams dropped or reset because the session was not admitted yet.</summary>
    public long DroppedBeforeAdmission;

    /// <summary>Peer streams reset by this end (unsupported channel, malformed, before admission).</summary>
    public long StreamsReset;

    /// <summary>
    /// Bulk ranges whose bytes did not match the checksum trailer their sender appended (PROTOCOL.md §3.3). Not wire
    /// corruption — QUIC.s AEAD discards anything the wire damaged — so anything above zero means a bug, a bad memory
    /// module, or a source that changed underneath a transfer. Worth alerting on.
    /// </summary>
    public long BulkChecksumFailures;

    /// <summary>Pings sent.</summary>
    public long PingsSent;

    /// <summary>Pongs received.</summary>
    public long PongsReceived;

    /// <summary>Pongs sent.</summary>
    public long PongsSent;

    /// <summary>Pings not answered because of the Pong rate limit.</summary>
    public long PingsIgnored;

    /// <summary>Pongs whose echoed time matched no outstanding Ping (late, duplicate or forged).</summary>
    public long UnmatchedPongs;

    /// <summary>Transport completions whose context no longer matched a live send (ignored, ADR 0008 invariant 2).</summary>
    public long StaleCompletions;

    /// <summary>Complete messages dropped because the receive ring was full.</summary>
    public long ReceiveRingDrops;

    /// <summary>Messages dropped because the receive budget or pool was exhausted.</summary>
    public long OutOfReceiveBuffers;

    /// <summary>Transport callbacks that threw (each one closes the connection with <c>InternalError</c>).</summary>
    public long CallbackFaults;

    /// <summary>Compressed messages dropped because decoding failed or exceeded the decode budget.</summary>
    public long DecodeFailures;

    /// <summary>
    /// Peer <c>BulkProgress</c> frames that claimed more bytes than this end handed to the transport (PROTOCOL.md §2.3, §3.4
    /// control-message bounds): a field no honest peer can produce, so each was handled as a malformed frame — dropped whole
    /// when it came as a control datagram, a <c>ProtocolViolation</c> close when it came on the control stream. A transfer
    /// completes only when this end's own send side is finished too, so an over-claim never delivers anything.
    /// </summary>
    public long BulkProgressOverClaims;

    /// <summary>
    /// Peer <c>BulkCancel</c> frames that named no transfer this end is sending (PROTOCOL.md §3.4: <c>BulkCancel</c> is
    /// receiver-to-sender, and one naming nothing is ignored and counted). A cancel that crossed its transfer's completion
    /// on the wire is counted here as well, so a small number is ordinary.
    /// </summary>
    public long BulkCancelsIgnored;

    /// <summary>Control messages (Ping, Pong, Hello, HelloAck, Close) that could not be handed to the transport.</summary>
    public long ControlSendFailures;

    /// <summary>
    /// Application datagrams handed to the transport by the scheduler: messages sent alone and packed containers (Pings
    /// and other control datagrams are not included).
    /// </summary>
    public long DatagramsSent;

    /// <summary>Bytes of <see cref="DatagramsSent"/> (whole datagram payloads, container overhead included).</summary>
    public long DatagramBytesSent;

    /// <summary>Packed containers among <see cref="DatagramsSent"/> (PROTOCOL.md §2.2).</summary>
    public long ContainersSent;

    /// <summary>Messages that travelled inside packed containers.</summary>
    public long MessagesPacked;

    /// <summary>Stream sends handed to the transport (one per gathered submission of an ordered channel, PROTOCOL.md §3.1).</summary>
    public long StreamSends;

    /// <summary>Bytes of <see cref="StreamSends"/> (preambles, frame headers and payloads).</summary>
    public long StreamBytesSent;

    /// <summary>Stream receives held back by back-pressure (receive ring full or receive budget used up) and resumed from Poll.</summary>
    public long StreamReceivePends;

    /// <summary>
    /// Peer streams reset with <c>Timeout</c> because they stopped in the middle of a message (PROTOCOL.md §7,
    /// <see cref="PeerOptions.StreamIdleTimeout"/>); their staging leases and ring reservations were released.
    /// </summary>
    public long StreamIdleTimeouts;

    /// <summary>Sends queued from other threads (<see cref="PeerOptions.ThreadSafeSend"/>).</summary>
    public long ThreadSafeSends;

    /// <summary>Sends from other threads the game thread refused when it admitted them (counted in the channel's statistics too).</summary>
    public long ThreadSafeSendDrops;

    /// <summary>Send lease bytes held now.</summary>
    public long SendBytesOutstanding;

    /// <summary>Receive lease bytes held now.</summary>
    public long ReceiveBytesOutstanding;

    /// <summary>Messages split into fragments by this end (PROTOCOL.md §2.1).</summary>
    public long FragmentedMessagesSent;

    /// <summary>
    /// Fragments produced by the sender (each goes out as its own datagram and counts as one <c>Sent</c> message of its
    /// channel, because each is a datagram the scheduler hands over separately).
    /// </summary>
    public long FragmentsSent;

    /// <summary>Fragments received (before reassembly).</summary>
    public long FragmentsReceived;

    /// <summary>Fragmented messages reassembled completely and delivered.</summary>
    public long FragmentedMessagesReceived;

    /// <summary>Fragments dropped: duplicates, inconsistent fragment fields, a limit of the reassembly table, or no receive buffer.</summary>
    public long FragmentsDropped;

    /// <summary>
    /// Partial reassemblies given up because a newer sequence of the same key arrived on an
    /// <see cref="Channels.ChannelMode.UnreliableSequenced"/> channel (on an unordered channel the sequence is only a
    /// reassembly id, so two messages of one key reassemble side by side), or because the channel's
    /// <see cref="Channels.ChannelDefinition.MaxReassemblies"/> cap evicted the oldest partial (PROTOCOL.md §7).
    /// </summary>
    public long ReassembliesAbandoned;

    /// <summary>Partial reassemblies given up because they were not completed within 2 × RTT + 100 ms (PROTOCOL.md §7).</summary>
    public long ReassembliesExpired;

    /// <summary>Requests sent with <see cref="QuiclyPeer.SendRequestAsync"/>.</summary>
    public long RequestsSent;

    /// <summary>Requests that ended without a response because their timeout elapsed.</summary>
    public long RequestsTimedOut;

    /// <summary>Responses dropped because no outstanding request matched them (PROTOCOL.md §3.1).</summary>
    public long ResponsesUnmatched;
}

/// <summary>Per-channel statistics (<see cref="QuiclyPeer.GetChannelStatistics"/>). Fixed layout, no references.</summary>
[StructLayout(LayoutKind.Sequential)]
public struct ChannelStatistics
{
    /// <summary>The channel id.</summary>
    public ushort Channel;

    /// <summary>Messages handed to the transport.</summary>
    public long Sent;

    /// <summary>Payload bytes handed to the transport.</summary>
    public long BytesSent;

    /// <summary>Pending sends replaced by a newer value of the same key.</summary>
    public long SendSuperseded;

    /// <summary>Sends dropped because their expiry elapsed before transmission.</summary>
    public long Expired;

    /// <summary>Sends rejected with <see cref="SendStatus.QueueFull"/>.</summary>
    public long QueueFull;

    /// <summary>Sends rejected with <see cref="SendStatus.TooLarge"/>.</summary>
    public long TooLarge;

    /// <summary>Retransmissions.</summary>
    public long Retries;

    /// <summary>Sends rejected with <see cref="SendStatus.KeyTableFull"/>.</summary>
    public long SendKeyTableFull;

    /// <summary>Messages accepted.</summary>
    public long Received;

    /// <summary>Payload bytes accepted.</summary>
    public long BytesReceived;

    /// <summary>Messages dropped as stale or malformed.</summary>
    public long Dropped;

    /// <summary>Received values replaced before the application saw them (coalescing).</summary>
    public long ReceiveSuperseded;

    /// <summary>Messages dropped because the receive ring was full.</summary>
    public long RingDrops;

    /// <summary>Messages dropped because the key table was full.</summary>
    public long ReceiveKeyTableFull;

    /// <summary>Messages dropped for exceeding a size limit.</summary>
    public long ReceiveTooLarge;

    /// <summary>Messages dropped because no receive buffer was available.</summary>
    public long OutOfBuffers;

    /// <summary>Messages admitted and not yet handed to the transport (now).</summary>
    public long QueuedMessages;

    /// <summary>Payload bytes of <see cref="QueuedMessages"/>.</summary>
    public long QueuedBytes;

    /// <summary>Messages handed to the transport and not yet completed (reliable channels; now).</summary>
    public long InFlightMessages;

    /// <summary>Payload bytes of <see cref="InFlightMessages"/>.</summary>
    public long InFlightBytes;
}
