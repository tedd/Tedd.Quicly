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

    /// <summary>
    /// Complete messages dropped on arrival because the receive ring was full: the game thread did not take messages out
    /// fast enough. A channel nobody drains does not cause this: an unreliable one has a bounded backlog, counted in
    /// <see cref="DrainQueueDrops"/>, and a reliable one has its own streams held back
    /// (<see cref="ChannelStatistics.BacklogHolds"/>; see <see cref="QuiclyPeer.Poll"/>).
    /// </summary>
    public long ReceiveRingDrops;

    /// <summary>Messages dropped because the receive budget or pool was exhausted.</summary>
    public long OutOfReceiveBuffers;

    /// <summary>Transport callbacks that threw (each one closes the connection with <c>InternalError</c>).</summary>
    public long CallbackFaults;

    /// <summary>
    /// Compressed messages dropped because decoding failed, exceeded the decode budget
    /// (<see cref="PeerOptions.DecodedBytesPerSecond"/>), or found no buffer for the decoded payload within the receive
    /// budget. The last does not apply to <see cref="QuiclyPeer.Drain"/> on a ReliableOrdered or ReliableUnordered
    /// channel: there the message waits for the next Drain.
    /// </summary>
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
    /// <remarks>
    /// On a transport that reports datagram send states, every one of them ends in exactly one place:
    /// <c>DatagramsSent = <see cref="DatagramsAcknowledged"/> + <see cref="DatagramsLost"/> + <see cref="DatagramsCanceled"/>
    /// + in flight now + ended by a close or a reconnect</c>. The last term is not counted anywhere: datagrams the transport
    /// cancelled because the connection was closing, and those whose outcome was discarded by an in-place reconnect. The
    /// three outcome counters move when <see cref="QuiclyPeer.Poll"/> or <see cref="QuiclyPeer.Flush"/> drains the
    /// transport's report, so with <see cref="CompletionMode.ThreadPool"/> a tracked send can complete a moment before its
    /// counter moves.
    /// </remarks>
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

    /// <summary>
    /// Stream receives held back by back-pressure and resumed by the game thread: the receive ring was full or the receive
    /// budget used up (resumed by the next Poll), or the stream's channel had its share waiting for the application
    /// (<see cref="ChannelStatistics.BacklogHolds"/>; resumed when the application takes messages of that channel). A
    /// value that keeps rising while no channel's <c>BacklogHolds</c> does means the ring or the budget is too small for
    /// what arrives between two Polls.
    /// </summary>
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

    /// <summary>
    /// Times the receive side of an <see cref="Channels.ChannelMode.UnreliableSequenced"/> channel resynchronised its
    /// sequence clock: a sequence arrived that reads as older than the newest one seen on the channel by more than the
    /// reorder window (1 024 on a 16-bit channel, 65 536 on a 32-bit one), more than two seconds after that newest one
    /// last advanced, and was therefore taken for a forward jump of at least half the sequence space (a long blackout,
    /// or a sender whose messages expired unsent) instead of a late message (PROTOCOL.md §8). Normally zero. A value
    /// that rises without blackouts means messages arrive more than two seconds late after more than a window of later
    /// ones overtook them, and are delivered out of order: the sender queues datagrams in its transport
    /// (<see cref="PeerOptions.DropWhenBlocked"/> off, or sequenced messages packed next to ReliableLatest values) on a
    /// link that stays congested for seconds.
    /// </summary>
    public long SequenceResyncs;

    /// <summary>
    /// Datagrams among <see cref="DatagramsSent"/> the transport reported acknowledged by the peer's transport. Stays 0 on
    /// a transport that reports no datagram send states (see <see cref="DatagramsSent"/> for how the outcome counters add up).
    /// </summary>
    public long DatagramsAcknowledged;

    /// <summary>
    /// Datagrams among <see cref="DatagramsSent"/> the transport declared lost. This is the sending transport's verdict: a
    /// datagram declared lost may still have arrived (a late acknowledgement after a stall), so the receiver can have seen
    /// more than this suggests. Stays 0 on a transport that reports no datagram send states. The peer decides nothing from
    /// it, which matters because the remote end can raise it by withholding acknowledgements.
    /// </summary>
    public long DatagramsLost;

    /// <summary>
    /// Datagrams among <see cref="DatagramsSent"/> the transport dropped before transmission while the connection was not
    /// closing: it could not send them at once and they carried <c>CancelOnBlocked</c>
    /// (<see cref="PeerOptions.DropWhenBlocked"/>), or they no longer fitted after the path's datagram limit shrank. Each
    /// message of such a datagram completes <see cref="Threading.DeliveryStatus.Expired"/> and is counted in its channel's
    /// <see cref="ChannelStatistics.TransportCanceled"/>. Datagrams the transport cancelled because the connection was
    /// closing are not counted — that includes a genuine blocked drop whose completion was still waiting when the close
    /// started.
    /// </summary>
    public long DatagramsCanceled;

    /// <summary>
    /// Received messages of unreliable channels dropped from the drain queues: the sum of
    /// <see cref="ChannelStatistics.DrainQueueDrops"/> over the channels. A message of an UnreliableUnordered or
    /// UnreliableSequenced channel (without <c>CoalesceOnReceive</c>) that has no handler waits for
    /// <see cref="QuiclyPeer.Drain"/>. What such a channel still has queued when the next <see cref="QuiclyPeer.Poll"/>
    /// begins, without having been drained empty in between, is its backlog, and the backlog of these channels together
    /// is bounded: at most the part of the queue pool that is not reserved for reliable channels (the pool is
    /// <see cref="ReceiveRingCapacity"/> messages, at most 1 024 whatever the ring's capacity; half of it is reserved when
    /// the table has a ReliableOrdered or ReliableUnordered channel) and at most a quarter of
    /// <see cref="PeerOptions.ReceiveBudgetBytes"/>, counted in buffer blocks — with every option at its default 64 KiB,
    /// which is 1 024 messages of up to 64 bytes, 256 of 65 to 256 bytes, or 42 of 257 to 1 536 bytes. Poll cuts the
    /// backlog to that, and a later message of a backlogged channel that does not fit drops the oldest queued one: of the
    /// backlogged channel furthest over its fair share, which is the same channel when no other one holds more.
    /// </summary>
    /// <remarks>
    /// Non-zero means a channel receives traffic that nobody drains, or that is not drained empty between two Polls (a
    /// host that polls several times per Drain, or drains with a span it fills and does not call again). Register a
    /// handler, or drain the channel completely once per Poll — a channel that is loses nothing here, however large the
    /// burst and however many channels are read that way. A larger <see cref="PeerOptions.ReceiveBudgetBytes"/> raises the byte limit of the backlog;
    /// <see cref="PeerOptions.ReceiveRingCapacity"/> does not raise the node limit beyond 1 024. Game thread; a total since
    /// the peer was created.
    /// </remarks>
    public long DrainQueueDrops;
}

/// <summary>Per-channel statistics (<see cref="QuiclyPeer.GetChannelStatistics"/>). Fixed layout, no references.</summary>
[StructLayout(LayoutKind.Sequential)]
public struct ChannelStatistics
{
    /// <summary>The channel id.</summary>
    public ushort Channel;

    /// <summary>
    /// Messages handed to the transport, including the ones it later dropped before transmission
    /// (<see cref="TransportCanceled"/>) or declared lost (<see cref="TransportLost"/>): the count never goes back.
    /// </summary>
    public long Sent;

    /// <summary>Payload bytes handed to the transport (of every message in <see cref="Sent"/>).</summary>
    public long BytesSent;

    /// <summary>Pending sends replaced by a newer value of the same key.</summary>
    public long SendSuperseded;

    /// <summary>
    /// Sends dropped before they were handed to the transport, because the scheduler held them back for longer than their
    /// expiry (counted from their first scheduler pass, PROTOCOL.md §4.5). A datagram the transport dropped is counted in
    /// <see cref="TransportCanceled"/> instead, although its send also completes
    /// <see cref="Threading.DeliveryStatus.Expired"/>.
    /// </summary>
    public long Expired;

    /// <summary>Sends rejected with <see cref="SendStatus.QueueFull"/>.</summary>
    public long QueueFull;

    /// <summary>Sends rejected with <see cref="SendStatus.TooLarge"/>.</summary>
    public long TooLarge;

    /// <summary>Retransmissions.</summary>
    public long Retries;

    /// <summary>Sends rejected with <see cref="SendStatus.KeyTableFull"/>.</summary>
    public long SendKeyTableFull;

    /// <summary>
    /// Messages accepted: complete, valid and handed to the game thread's side (the receive ring or a mailbox). A message
    /// counted here can still be dropped before the application sees it — <see cref="DrainQueueDrops"/> on an unreliable
    /// channel nobody drains, <see cref="ReceiveSuperseded"/> on a coalescing one — so it is not a delivery count.
    /// </summary>
    public long Received;

    /// <summary>Payload bytes accepted.</summary>
    public long BytesReceived;

    /// <summary>
    /// Messages dropped as malformed or as stale: not newer than the last accepted value of their key (or of the channel,
    /// when unkeyed), ordered on the channel's sequence clock — so a key that idled for any length of time still has its
    /// next value accepted (PROTOCOL.md §8).
    /// </summary>
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

    /// <summary>
    /// Messages among <see cref="Sent"/> whose datagram the transport dropped before transmission while the connection was
    /// not closing: it could not send the datagram at once and the datagram carried <c>CancelOnBlocked</c>
    /// (<see cref="PeerOptions.DropWhenBlocked"/>), or the datagram no longer fitted after the path's datagram limit shrank.
    /// Counted in the unit of <see cref="Sent"/>: every member of a dropped packed container counts on its own channel, a
    /// fragment counts as one (the rest of its message still travels and the receiver gives the partial up), and so does a
    /// ReliableLatest transmission (which is retransmitted). The send completes
    /// <see cref="Threading.DeliveryStatus.Expired"/>, but <see cref="Expired"/> does not move.
    /// </summary>
    /// <remarks>
    /// <c>Sent − TransportCanceled</c> is what the transport put on the wire or still holds.
    /// <c>Sent − TransportCanceled − <see cref="TransportLost"/></c> is what was acknowledged, is in flight, completed
    /// <see cref="Threading.DeliveryStatus.Sent"/> on a carrier that reports no states, or was ended by a close or a
    /// reconnect. A cancel the transport reported because the connection was closing is not counted, and neither is a blocked
    /// drop whose report was still waiting when the close started. Counted when <see cref="QuiclyPeer.Poll"/> or
    /// <see cref="QuiclyPeer.Flush"/> drains the transport's report.
    /// </remarks>
    public long TransportCanceled;

    /// <summary>
    /// Messages among <see cref="Sent"/> whose datagram the transport declared lost, in the unit of <see cref="Sent"/> (see
    /// <see cref="TransportCanceled"/>); on an unreliable channel the send completes
    /// <see cref="Threading.DeliveryStatus.Lost"/>, on a ReliableLatest channel the value is retransmitted
    /// (<see cref="Retries"/>). This is the sending transport's verdict: a datagram declared lost may still have arrived, so
    /// the receiver's <see cref="Received"/> can exceed <c>Sent − TransportCanceled − TransportLost</c>. Stays 0 on a
    /// transport that reports no datagram send states.
    /// </summary>
    public long TransportLost;

    /// <summary>
    /// Received messages of this channel dropped from its drain queue (an unreliable channel without
    /// <c>CoalesceOnReceive</c>; always 0 for the other modes, and for a channel that has always had a handler): the channel
    /// had no handler and was left undrained across a <see cref="QuiclyPeer.Poll"/>, which made what it had queued backlog,
    /// and the bounded backlog of the unreliable channels was full, so the oldest queued message made room for a newer one
    /// (see <see cref="PeerStatistics.DrainQueueDrops"/> for the limits and for which channel loses). The dropped messages
    /// are still counted in <see cref="Received"/>. Distinct from <see cref="RingDrops"/>, which counts messages dropped on
    /// arrival because the receive ring itself was full. Survives a reconnect.
    /// </summary>
    public long DrainQueueDrops;

    /// <summary>
    /// Times a stream of this channel was held back because too many of the channel's messages were waiting for the
    /// application (ReliableOrdered and ReliableUnordered; always 0 for the other modes, and for a channel that has always
    /// had a handler). A reliable channel that nobody reads — no handler, and not drained empty since the
    /// <see cref="QuiclyPeer.Poll"/> before last — may have only its share of the drain queues and of the receive budget
    /// waiting; one that is drained, as many messages as the receive ring holds and half the receive budget (see
    /// <see cref="QuiclyPeer.Poll"/>). Nothing is lost and the receive ring stays open for the other channels: QUIC flow
    /// control holds that stream's sender until the application takes messages of the channel or registers a handler for
    /// it. A number that keeps rising on a channel the other end sends on means that nobody reads the channel, or that it
    /// is read more slowly than it is written. Each hold is also counted in
    /// <see cref="PeerStatistics.StreamReceivePends"/>. Survives a reconnect.
    /// </summary>
    public long BacklogHolds;
}
