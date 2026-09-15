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

    /// <summary>Highest receive ring occupancy seen (including reservations).</summary>
    public int ReceiveRingHighWater;

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
}
