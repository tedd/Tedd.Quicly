namespace Tedd.Quicly.Core.Session;

/// <summary>
/// Peer-level counters. Each field has one writing thread (ADR 0008 invariants 4 and 13): the transport-thread group
/// is incremented inside callbacks, the game-thread group inside <c>Send*</c>/<c>Flush</c>/<c>Poll</c>. Snapshots read
/// them without locks (64-bit reads are atomic on the supported platforms).
/// </summary>
internal sealed class PeerCounters
{
    // ---- transport thread

    /// <summary>Datagrams received (any channel, before validation).</summary>
    public long DatagramsReceived;

    /// <summary>Datagram payload bytes received.</summary>
    public long DatagramBytesReceived;

    /// <summary>Stream bytes received (all streams).</summary>
    public long StreamBytesReceived;

    /// <summary>Control messages received (datagram and stream).</summary>
    public long ControlMessagesReceived;

    /// <summary>Datagrams (or container members) dropped as malformed.</summary>
    public long MalformedDatagrams;

    /// <summary>Application datagrams, streams and control messages dropped or reset because the session was not admitted yet.</summary>
    public long DroppedBeforeAdmission;

    /// <summary>Peer streams reset by this end.</summary>
    public long StreamsReset;

    /// <summary>Pongs received (datagram and stream).</summary>
    public long PongsReceived;

    /// <summary>Datagram Pongs sent from the transport thread.</summary>
    public long PongsSent;

    /// <summary>Pings not answered because of the Pong rate limit.</summary>
    public long PingsIgnored;

    /// <summary>Transport completions whose context no longer matched a live send.</summary>
    public long StaleCompletions;

    /// <summary>Complete messages dropped because the receive ring was full.</summary>
    public long ReceiveRingDrops;

    /// <summary>Messages dropped because the receive budget or pool was exhausted.</summary>
    public long OutOfReceiveBuffers;

    /// <summary>Transport callbacks that threw.</summary>
    public long CallbackFaults;

    /// <summary>Datagram Pongs that could not be sent (control pool exhausted or transport refused).</summary>
    public long PongSendFailures;

    /// <summary>Pong samples dropped because the hand-off ring to the game thread was full.</summary>
    public long PongSamplesDropped;

    /// <summary>Stream receives held back (<see cref="Engines.StreamConsume.Pend"/>): ring full or receive budget used up.</summary>
    public long StreamReceivePends;

    /// <summary>Fragments of larger messages received (PROTOCOL.md §2.1).</summary>
    public long FragmentsReceived;

    /// <summary>Fragmented messages reassembled completely and delivered.</summary>
    public long FragmentedMessagesReceived;

    /// <summary>Fragments dropped: duplicates, inconsistent fragment fields, a partial's limits, or no receive buffer.</summary>
    public long FragmentsDropped;

    /// <summary>Partial reassemblies given up because a newer sequence of the same key arrived, or the channel's cap evicted them.</summary>
    public long ReassembliesAbandoned;

    /// <summary>Partial reassemblies given up because they were not completed within 2 × RTT + 100 ms.</summary>
    public long ReassembliesExpired;

    /// <summary>Highest receive ring occupancy seen (including reservations).</summary>
    public int ReceiveRingHighWater;

    // ---- any thread (Interlocked)

    /// <summary>Sends queued from other threads (<see cref="PeerOptions.ThreadSafeSend"/>).</summary>
    public long ThreadSafeSends;

    /// <summary>
    /// Sends from other threads refused when they were admitted, or dropped when the session ended (their leases went
    /// back to the pool). Incremented by the game thread and, on teardown, by the transport thread, so both use
    /// <see cref="Interlocked"/>.
    /// </summary>
    public long ThreadSafeSendDrops;

    // ---- game thread

    /// <summary>Pings sent (datagram and stream).</summary>
    public long PingsSent;

    /// <summary>Control-stream Pongs sent.</summary>
    public long StreamPongsSent;

    /// <summary>Pongs whose echoed time matched no outstanding Ping.</summary>
    public long UnmatchedPongs;

    /// <summary>Compressed messages dropped in <c>Poll</c> (decode failure or decode budget).</summary>
    public long DecodeFailures;

    /// <summary>Control messages (Ping, Pong, Hello, HelloAck, Close) the game thread could not hand to the transport.</summary>
    public long ControlSendFailures;

    /// <summary>Application datagrams the scheduler handed to the transport: loose messages and packed containers (control datagrams are not included).</summary>
    public long DatagramsSent;

    /// <summary>Bytes of <see cref="DatagramsSent"/> (whole datagram payloads).</summary>
    public long DatagramBytesSent;

    /// <summary>Packed containers among <see cref="DatagramsSent"/>.</summary>
    public long ContainersSent;

    /// <summary>Messages that travelled inside packed containers.</summary>
    public long MessagesPacked;

    /// <summary>Stream sends handed to the transport by the engines (one per gathered submission).</summary>
    public long StreamSends;

    /// <summary>Bytes of <see cref="StreamSends"/> (preambles, frame headers and payloads).</summary>
    public long StreamBytesSent;

    /// <summary>Peer streams reset because they stopped mid-message (PROTOCOL.md §7, <see cref="PeerOptions.StreamIdleTimeout"/>).</summary>
    public long StreamIdleTimeouts;

    /// <summary>Messages split into fragments by the sender (PROTOCOL.md §2.1).</summary>
    public long FragmentedMessagesSent;

    /// <summary>Fragments produced by the sender (each goes out as its own datagram).</summary>
    public long FragmentsSent;

    /// <summary>Requests sent with <see cref="QuiclyPeer.SendRequestAsync"/>.</summary>
    public long RequestsSent;

    /// <summary>Requests that ended without a response because their timeout elapsed.</summary>
    public long RequestsTimedOut;

    /// <summary>Responses dropped because no outstanding request matched them (PROTOCOL.md §3.1).</summary>
    public long ResponsesUnmatched;
}
