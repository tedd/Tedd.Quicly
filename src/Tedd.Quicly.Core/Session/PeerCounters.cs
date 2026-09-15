namespace Tedd.Quicly.Core.Session;

/// <summary>
/// Peer-level counters. Each field has one writing thread (ADR 0008 invariants 4 and 13): the transport-thread group
/// is incremented inside callbacks, the game-thread group inside <c>Send*</c>/<c>Flush</c>/<c>Poll</c>. Snapshots read
/// them without locks (64-bit reads are atomic on the supported platforms).
/// </summary>
internal sealed class PeerCounters
{
    // ---- transport thread
    public long DatagramsReceived;
    public long DatagramBytesReceived;
    public long StreamBytesReceived;
    public long ControlMessagesReceived;
    public long MalformedDatagrams;
    public long DroppedBeforeAdmission;
    public long StreamsReset;
    public long PongsReceived;
    public long PongsSent;
    public long PingsIgnored;
    public long StaleCompletions;
    public long ReceiveRingDrops;
    public long OutOfReceiveBuffers;
    public long CallbackFaults;
    public int ReceiveRingHighWater;

    // ---- game thread
    public long PingsSent;
    public long StreamPongsSent;
    public long DecodeFailures;
}
