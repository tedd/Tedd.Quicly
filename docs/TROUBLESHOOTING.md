# Troubleshooting

Every way the library can drop a message is counted. This page maps a symptom to the counter that names its
cause. The counters are read with `QuiclyPeer.GetStatistics(out PeerStatistics)` and
`QuiclyPeer.GetChannelStatistics(channel, out ChannelStatistics)`; both are allocation-free, so a host can
sample them every second and log the differences. All of them are totals since the peer was created and
survive an in-place reconnect.

## Messages go missing on localhost: which counter tells you why

Loopback loses no packets, so when an unreliable channel's `Sent` on one end is larger than `Received` on the
other, one of the two ends dropped the difference on purpose and counted it. Take a snapshot on **both** ends,
for the channel and for the peer, and look the difference up here. "Sender" and "receiver" are the two ends of
the channel in question.

| Look at | What it means | What to do |
|---|---|---|
| sender `ChannelStatistics.TransportCanceled`, `PeerStatistics.DatagramsCanceled` | The sender's transport dropped the datagram before transmission. Unreliable datagrams are handed over with the transport's cancel-on-blocked flag (`PeerOptions.DropWhenBlocked`, default on), which MsQuic documents as "a frame should be dropped when it can't be sent immediately"; a datagram that no longer fits after the path's datagram limit shrank is cancelled the same way. The send completes `DeliveryStatus.Expired`, but the channel's `Expired` counter does not move. Per channel the unit is messages (every member of a dropped packed container, every fragment); per peer it is datagrams. | Look at `PeerStatistics.Transport` (`CongestionEvents`, `SendSuspectedLostPackets`, the congestion window and RTT) for what the transport thought of the path at the time. Send less per tick, or spread a burst over ticks. If late data is better than no data for you, set `PeerOptions.DropWhenBlocked = false`: blocked datagrams then wait in the transport's queue and are sent when it can send again, however old they have become. Fragmented messages are the most exposed: one of N datagrams dropped loses the whole message. |
| sender `ChannelStatistics.TransportLost`, `PeerStatistics.DatagramsLost` | The sender's transport declared the datagram lost. The send completes `DeliveryStatus.Lost` (a ReliableLatest value is retransmitted instead, see `Retries`). This is the transport's verdict, not a fact about the receiver: a datagram declared lost can still have arrived — on loopback, where nothing is really lost, suspect a stall (a debugger break, a long pause, a suspended process) that delayed the acknowledgements. | Compare with the receiver's `Received`: if the receiver got them, nothing was lost. Stays 0 on a carrier that reports no datagram states (WebTransport). |
| sender `ChannelStatistics.Expired` | The message expired before it was handed to the transport: the scheduler held it back — the send cap, datagrams unavailable, a stream that was blocked — for longer than the channel's expiry. The expiry counts from the message's first scheduler pass (PROTOCOL.md §4.5), so a long frame or a late `Flush` alone no longer expires anything. | Raise `ChannelOptions.ExpiryMicros` (0 = never) or `SendOptions.ExpiryMicros`, or look at what held the scheduler back (`PeerOptions.MaxSendBytesPerSecond`). |
| receiver `ChannelStatistics.Dropped`, `PeerStatistics.SequenceResyncs` | `UnreliableSequenced` and `ReliableLatest` deliver only values newer than the last one accepted for the key, so a value that arrives out of order, or twice, is dropped as stale and counted in `Dropped`. That is the mode working, not a loss. A key that idled for a long time while other keys of the channel stayed busy is **not** dropped any more: staleness is decided on the channel's sequence clock (PROTOCOL.md §8). `SequenceResyncs` counts the times a receiver took a sequence that read as old for a jump forward of half the sequence space or more (a long blackout, or a sender whose messages expired unsent); it is normally 0. | A steadily rising `Dropped` with no reordering on the path means the two ends disagree about the channel table or an old build is on one end. A rising `SequenceResyncs` on a slow channel means messages arrive more than two seconds late. |
| receiver `ChannelStatistics.DrainQueueDrops`, `PeerStatistics.DrainQueueDrops` | An **unreliable** channel (`UnreliableUnordered` / `UnreliableSequenced` without `CoalesceOnReceive`) has no handler — or its messages were met by a `Drain` of another channel before the next `Poll` — and nobody drained it fast enough. Its messages wait for `Drain` in a bounded backlog: the unreliable channels together may queue the part of the drain-queue pool that is not reserved for reliable channels (the pool is `ReceiveRingCapacity` messages, at most 1 024; half of it is reserved when the table has a `ReliableOrdered` or `ReliableUnordered` channel) and may pin a quarter of `ReceiveBudgetBytes` (64 KiB by default — about 42 full-size datagrams, since the budget is charged in buffer block sizes). Beyond that the **oldest** queued message is dropped for each new one: the channel's own when it is at or over its even share, otherwise that of the unreliable channel with the longest queue. The counter is on the channel that lost the message; `Received` still counts it. Only that backlog is affected: no other channel loses anything. | Register a handler for the channel, or `Drain` it every tick (after `Poll`). If a Drain-style channel legitimately receives bursts larger than the backlog between two drains, raise `ReceiveBudgetBytes` (the byte bound is a quarter of it) and, for many small messages, `ReceiveRingCapacity`. A channel you do not read at all should not be sent on. |
| receiver `ChannelStatistics.RingDrops` / `OutOfBuffers`, `PeerStatistics.ReceiveRingDrops` / `OutOfReceiveBuffers`, `ReceiveRingHighWater`, a rising `StreamReceivePends` | The receiver did not take messages out fast enough: the receive ring was full, or the receive budget (`PeerOptions.ReceiveBudgetBytes`) was used up, and complete messages were dropped on arrival. This affects every channel of the peer at once. Two causes. (1) `Poll` is not called often enough for the traffic. (2) A **reliable** channel (`ReliableOrdered`, `ReliableUnordered`) receives traffic but has no handler and is never drained: a reliable message is never dropped, so once the drain-queue pool is full the peer holds the next one and stops emptying the ring. From then on datagrams of every ring channel are dropped here (`RingDrops`), every stream channel is back-pressured (`StreamReceivePends` keeps rising; the sender's reliable sends back up behind QUIC flow control), responses on request/response channels wait so requests time out, and `HasPendingWork` stays `true`. Nothing of the reliable channel itself is lost, and coalescing and `ReliableLatest` handlers keep running. An undrained *unreliable* channel does not cause this (row above). | For (2): register a handler for every reliable channel the other end sends on, or drain it — the stall ends as soon as the channel is drained or gets a handler. To find the channel: it is a reliable channel without a handler whose `Received` stopped rising while the sender keeps sending on it (the sender's `QueuedMessages` / `InFlightMessages` for that channel stay up). For (1): call `Poll` every tick; raise `ReceiveRingCapacity` / `ReceiveBudgetBytes` if `ReceiveRingHighWater` comes close to `ReceiveRingCapacity` under normal load. |
| `ChannelStatistics.SendKeyTableFull` (sender), `ReceiveKeyTableFull` (receiver) | The channel's key table is full. A `ReliableLatest` channel never evicts a live key: the sender refuses the send with `SendStatus.KeyTableFull`, and a receiver whose table is full drops the value and tells the sender (`LatestReject`), which retries within its budget and completes the value `Failed` when the budget is used up. An `UnreliableSequenced` receiver evicts its least recently updated key instead and counts nothing here, except for a key outside a dense key space. | Raise `ChannelOptions.MaxKeys`, or retire keys that are gone (`RetireKey`). |
| receiver `PeerStatistics.DecodeFailures` | A compressed message was dropped in `Poll`: it did not decode, or decoding it would have exceeded the decode budget (`PeerOptions.DecodedBytesPerSecond`, default 8 MiB/s) or the receive budget. | Raise `DecodedBytesPerSecond` if the traffic is legitimate; otherwise compress less (`ChannelOptions.MinCompressSize`). |
| receiver `PeerStatistics.ReassembliesExpired`, `ReassembliesAbandoned`, `FragmentsDropped` | Part of a fragmented message did not arrive — its datagram was cancelled or lost on the sender (first two rows) — and the receiver gave the partial up after 2 × RTT + 100 ms, or a newer value of the key or the channel's reassembly cap replaced it. | See the first two rows; keep fragmented messages rare, or move large payloads to a reliable channel. |
| receiver `ChannelStatistics.ReceiveTooLarge`, `PeerStatistics.MalformedDatagrams`, `DroppedBeforeAdmission` | The message exceeded a size limit, the datagram did not parse against this end's channel table, or it arrived before the session was admitted. | Check that both ends run the same channel table (`ChannelTable.Hash`) and size limits. |
| none of the above, and `Sent` is still larger than `Received` | The datagrams are in flight, or the connection closed or was reconnected with them outstanding: what the transport cancels because the connection is closing completes `Disconnected` and is deliberately not counted as a transport drop. | Compare snapshots taken while the connection is up and idle. |

### How the sender's counters add up

On a transport that reports datagram send states (MsQuic; not the WebTransport carrier):

* per peer, in datagrams: `DatagramsSent = DatagramsAcknowledged + DatagramsLost + DatagramsCanceled`
  + in flight now + ended by a close or a reconnect;
* per channel, in messages: `Sent − TransportCanceled` is what the transport put on the wire or still holds, and
  `Sent − TransportCanceled − TransportLost` is what was acknowledged or is in flight (or was ended by a close or
  a reconnect).

`Sent` and `BytesSent` never go back: a message the transport later dropped was still handed to it. The outcome
counters move when `Poll` or `Flush` drains the transport's report, so they lag the event by up to one tick. A
blocked drop whose report was still waiting when `Close` started is not counted.

### Reliable channels

A `ReliableOrdered`, `ReliableUnordered` or `Bulk` message is never dropped while the connection lives; it can
only fail to be admitted (`SendStatus`, and the channel's `QueueFull` / `TooLarge` counters) or end
`Disconnected` with the connection. The price of "never dropped" is on the receiver: a reliable channel that
has no handler and is not drained eventually holds up **all** receiving on that peer — see the
`ReceiveRingDrops` row above. Every reliable channel in the table needs a handler or a `Drain` call. A `ReliableLatest` value is delivered unless a newer value of its key
replaced it (`SendSuperseded`, `ReceiveSuperseded`) — and a tracked value completes `Delivered` only when the
receiver acknowledged exactly the version that was last transmitted (PROTOCOL.md §4.4).

The exception is a `ReliableUnordered` group whose stream the **receiver** reset, which its
`PeerStatistics.StreamsReset` counts. A group's messages complete `Delivered` on the sender when the transport
has acknowledged them, so a reset that comes after that is not reported to the sender. A receiver resets a
group that is malformed, one that carries a message it could never buffer (`ReceiveTooLarge`), and one that
stops in the middle of a message (`StreamIdleTimeouts`). A receiver built before the PROTOCOL.md §7 rule
"`MaxGroups` is a sender's bound" also reset whole groups when it fell behind its sender — a late `Poll`, a full
receive ring — and more than `MaxGroups` streams of the channel were open at it. If `StreamsReset` rises on a
receiver while its peer's `ReliableUnordered` messages go missing, update the receiving end; the sender's
version does not matter.

One level below, a transport that had no table slot for a stream its peer was allowed to open could only refuse
the stream, and the session would never see it. The MsQuic transport does not get there: its stream table is
sized from the streams it grants — the role's initial grant and whatever the session asks for after admission —
next to a quarter for this end's own streams, whatever `MsQuicTransportOptions.MaxStreams` says (the option is
the size the table has when that is enough; `MsQuicTransportOptions.Diagnostic` says once when it was raised,
and `MsQuicTransport.RefusedPeerStreamCount` stays 0). What the option still limits is this end's own streams:
`OpenStream` answers `OutOfMemory` once they hold everything the table has beyond the peer's grants. The
WebTransport carrier's mirror of that table follows the slots in use and has no size of its own to get wrong.
