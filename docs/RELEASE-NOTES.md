# Release notes

What an application that upgrades has to know, newest release first. The protocol is described in
[PROTOCOL.md](PROTOCOL.md), what is merged and measured in [STATUS.md](STATUS.md), and which counter explains a
missing message in [TROUBLESHOOTING.md](TROUBLESHOOTING.md).

## Unreleased: reliable channels nobody reads, and groups of a receiver that falls behind

Fixes on the receive side of the stream channels. **There is no wire change**, and they are receiver-local:
the end that receives is the end to upgrade.

### Fixed

* **A reliable channel nobody read stopped every other channel of the peer** (the second known limit of the
  release below). A `ReliableOrdered` / `ReliableUnordered` channel without a handler that was not drained filled the
  drain-queue pool; the next message was held and nothing more left the receive ring. Reliable channels now have a
  per-channel **receive credit**: a channel nobody reads may have its share waiting for the application (its
  reserved part of the queue pool — 256 messages each with two reliable channels at default options — and the same
  part of a quarter of `ReceiveBudgetBytes`), and past that only **its own streams** are held back, by ordinary
  QUIC flow control. A channel the application drains may have half the budget waiting, shared with the other
  reliable channels no handler reads, so what channels nobody reads, channels drained and then abandoned, and
  unread datagram backlog pin together leaves a quarter of the budget (short of one message per reliable channel)
  to the channels that are read. Every other
  channel keeps working, nothing is lost, and the held streams go on when the application drains the channel or
  registers a handler for it. PROTOCOL.md §7, "Channels nobody drains".
* **A handler, or a `Drain`, called from inside another handler could deliver a `ReliableOrdered` channel out of
  order.** A `Drain` of another channel from inside a handler queued the older messages it met for "the next
  `Poll`" while the running `Poll` went on dispatching newer ones; a handler registered from inside a handler over
  a queued backlog got the ring's messages before the backlog. A message of a channel that has messages queued now
  goes behind them.
* **Compressed messages of reliable channels were dropped for want of a decode buffer.** Every compressed message
  was decoded into a second buffer of its raw size, and the compressed messages that wait for the application
  count in the receive budget too: a burst of them could fill it, and then every message that needed a buffer
  was dropped (`DecodeFailures`) after the sender had been told `Delivered` — through a handler (256 of 1 200
  messages of 20 000 bytes to a late receiver in the test that found it), and through `Drain`, which also went on
  to drop the rest of the queue once the batch did not fit. A compressed message of a `ReliableOrdered` /
  `ReliableUnordered` channel **that no handler reads** is now staged in a block of its decoded size (just
  that: the in-place decode keeps its margin on the stack, so a 64 KiB message takes a 64 KiB block), taken all or
  nothing with its ring slot and its credit when the message starts, and **decoded in place** in that block. When its
  decoded size fits the largest pool block within `ReceiveBudgetBytes` (any decoded size up to 256 KiB with default
  options), it is never dropped for want of a buffer, its decode never waits for one, and the receive budget is not
  exceeded on that path; a larger one is the edge band in "Known limits". `Drain` waits only for the decode budget
  (`DecodedBytesPerSecond`): a message it has no decode budget for stays queued, nothing newer of that channel comes
  in the same call — the channel's order holds — and the call returns what it has. Any other compressed message
  (a handler's, a response, an unreliable channel's) is decoded in place too when its block happens to fit, and is
  otherwise decoded into a second buffer that may take the receive budget past its limit by itself, tried once; see
  "Known limits". A message dropped for want of a buffer no longer uses up the decode budget of the ones behind it.
* **A `ReliableUnordered` receiver that fell behind its sender lost whole groups, and the sender reported them
  `Delivered`** (the first known limit of the release below). A sender counts a group's stream as closed when the
  transport has acknowledged its data, which can be before the receiving application has read any of it, and
  opens the next group then. A receiver that was behind — a late `Poll`, a full receive ring — therefore saw more
  than `MaxGroups` streams of a channel from a conforming sender, and reset the excess (`StreamsReset`): 80 ms
  without a `Poll` lost 128 of 320 messages in the test that found it, with nothing `Failed` on the sender.
  `MaxGroups` is now a sender's bound only. A receiver accepts every group the connection's stream limit admits,
  and its per-stream receive state is sized for that limit, including whatever an MsQuic client grants in its
  handshake (`ClientPeerUnidiStreamCount`; see "Behaviour changes" for its new default). PROTOCOL.md §7,
  "`MaxGroups` is a sender's bound".
* **A sender retried its stream opens on every pass while the peer held its streams, on a busy machine.** Over MsQuic, a
  send that starts a stream is two calls. When the peer's stream limit refused the start between them, the send failed
  with `InvalidState` — for a stream the application had just been told was refused. The engines take that for a failure
  that no stream limit caused and do not wait for stream credit, so they opened, refused and abandoned a stream on every
  pass (952 attempts in 500 ms in the test that measures it) for as long as the receiver kept its streams. The transport
  now answers `StreamLimitReached`, which every engine handles by parking on credit. It needed a thread preempted in a
  window of microseconds: about 1 run in 100 of the conformance scenario with 24 busy loops on its cores. The
  `ITransport.SendStream` contract now says that a synchronous `StreamLimitReached` is final even when the refusal
  callbacks for the stream have arrived.
* **`ReliableLatest`: a large value whose stream the peer's stream limit refused was counted as sent.** A value
  that travels on a stream is now counted when its stream has started, a refused start goes out again as the
  value's first transmission, and values that wait for a stream no longer hold up the small values behind them.
  A refused start also gives the channel's stream slot back at once: a refusal the send answered itself, whose
  shutdown is never reported, kept a slot for the rest of the connection, and two of them stopped a channel's
  large values for good. A stream open that fails because the stream table is full for the moment
  (`OutOfMemory`) is retried at the next pass instead of waiting for stream credit that nothing would bring.
* **A `Poll` resumed more held streams than the receive ring and the receive budget had room for**, so with many
  streams waiting every Poll resumed all of them only for most to be held again (73 hold-and-resume cycles per
  message in the test that measures it). It now resumes as many as the ring has slots and the budget has room
  for — the oldest always, so a stream that cannot go on moves to the back — and lets go of the entries of streams
  that were reset while they were held.
* **A peer that reset the streams this end held for the receive ring could leave a live stream held for good.**
  Such a stream's entry stayed in the list of held streams; enough of them filled it, and the next stream held was
  not listed (`CallbackFaults`) and never resumed. The list now grows for them, as the credit list does, and the
  peer is disconnected (`LimitExceeded`) past eight times its size.
* **A Bulk sender that canceled transfers while the receiver's host was late had its next transfer refused**
  (`LimitExceeded`, the transfer `Failed`) although it never had more transfers live than allowed: a canceled
  transfer's slot comes back with its stream's close, while the receiver's record of it waits for its game thread.
  The receiver keeps four records per transfer it accepts at once. (This was in 0.2.1 already.)

### Behaviour changes

* **An unread reliable channel now stalls its own streams instead of the whole receiving peer.** If an application
  relied on the old stall as back-pressure for other channels, it no longer gets it. The *sender* of the unread
  channel sees its sends queue up in its own send budget, which all its channels share: without
  `ChannelOptions.QueueLimitBytes` on that channel its sends on every channel end in `OutOfBuffers` once the budget
  is full, as before; with it, that channel alone answers `QueueFull`.
* **How a reliable channel without a handler is limited depends on whether it is read.** A channel the
  application drains — a `Drain` left its queue empty since the `Poll` before last, and an empty `Drain` counts —
  may have as many messages waiting as the receive ring holds and **half of `ReceiveBudgetBytes`, shared with the
  other reliable channels no handler reads** (128 KiB at the default when they have nothing waiting; a message is
  started while less than that waits in them, and one message is always accepted on an empty channel; channels
  with a handler take nothing of it). Before this release it shared the whole budget with every other channel. A
  host that receives more than that on its drained channels between two drains now has the rest wait at the
  sender for a frame: raise `ReceiveBudgetBytes`, or register a handler (a handler's channel is limited as
  before). A channel that is *not* read is held to the share above, strictly: a message whose buffer block is
  larger than the share (with default options: a message above 16 KiB when the table has two to four reliable
  channels, above 4 KiB with five to sixteen), or than what the channels no handler reads leave of the half, is
  not started until the application drains the channel for the first time. A host that lets a whole `Poll` interval pass without draining a
  reliable channel that has messages queued, or never empties it (a fixed-size buffer it fills without calling
  again), is treated as not reading it until a `Drain` empties the queue again. Nothing is lost either way — the
  rest waits at the sender — but to keep the throughput, drain until `Drain` returns 0, once per `Poll`.
* **`Drain` can return fewer messages than the span holds while more are queued**, on a reliable channel that
  compresses, when `DecodedBytesPerSecond` has no room for the next one: see "Fixed". Loop until it returns 0. Such
  a channel waits for `DecodedBytesPerSecond` now instead of dropping what exceeds it.
* **A compressed message of a reliable channel without a handler counts with its decoded size.** It is staged in a
  block of its decoded size from its first byte. That block is the pool's size class for the decoded size, so it
  counts in the channel's credit, the shared half and the receive budget. Back-pressure on a drained or unread
  compressed channel therefore engages sooner than before. With default options, a message of the channel's default
  `MaxMessageSize` (64 KiB) takes a 64 KiB block, a quarter of the default 256 KiB budget, however small it is on
  the wire. A 20 000-byte message that compresses to a few dozen bytes also takes a 64 KiB block. When the table has
  two or more reliable channels, that is larger than the share of a channel nobody reads, so it waits for the
  channel's first `Drain`. A peer that declares a large decoded size and stalls pins what one that declares a large
  wire length pins: the same block, under the same caps (`MaxMessageSize`, which bounds both at parse; the channel's credit; the budget; `StreamIdleTimeout`).
* **A compressed message's second decode buffer may take the receive budget past its limit**, by that one buffer
  (handlers, responses, and the cases in "Known limits"); the transport takes nothing new until it is back within
  it. `ReceiveBytesOutstanding` can read above `ReceiveBudgetBytes` for that long.
* **The half of the budget the reliable channels no handler reads share is counted by the messages taken while
  their channel had no handler.** A channel that gets a handler over its backlog keeps that backlog counted in the
  half until the handler has seen it (about one `Poll`); messages taken while a handler existed and left queued by
  `UnregisterHandler` are not counted in it (they are still bounded by the ring, the channel's own count and the
  budget). The check now costs the same whatever the number of channels.
* **`MsQuicTransportOptions.ClientPeerUnidiStreamCount` defaults to 0** (1 024 before). A QUICLY client raises it to
  its channel table's sum once it is admitted, as a server does, so a server's first streams wait one round trip
  for that credit. A larger grant is still honoured (the receiver keeps a record for every stream it admits), but
  the streams a late client holds keep their bytes in MsQuic's connection flow-control window: see "Known limits".
  A client that uses the transport without a QUICLY session grants what it sets, or calls `UpdatePeerStreamLimits`.
* **A peer that resets the streams this end holds back, over and over, is disconnected** (`LimitExceeded`) once
  the receiver cannot remember another held stream — held back for a channel's credit, or for the receive ring or
  budget. No sender of this library does that.
* **`HasPendingWork` is no longer held `true` by an unread reliable channel.** A host that polls while the probe
  is set now comes to rest. Streams that wait for the application's `Drain` are not work.
* `MsQuicTransportOptions.MaxStreams` no longer limits the streams a peer may open: the transport's stream table
  is sized from what it grants the peer (the diagnostic sink says once when that raised the table), and the
  option is the table's size when that is enough. It still limits this end's own streams: `OpenStream` answers
  `OutOfMemory` once they hold everything the table has beyond the peer's grants. While slots of closed streams
  wait for the thread pool's cleanup work item the table grows, up to twice its size; past that a peer stream is
  refused (`RefusedPeerStreamCount`, and the diagnostic sink): see "Known limits".
* `WebTransportOptions.MaxStreams` is no longer a limit (the carrier's table follows the streams in use), and the
  carrier shows the session no more peer streams than it reported it would admit.
* A large `ReliableLatest` channel and a `Bulk` channel still reset streams beyond their limit on the receiver.
  That is not silent there: the value is not acknowledged and retried, the transfer ends with a status.

### New

* `ChannelStatistics.BacklogHolds`: the times a stream of the channel was held back because the channel's share
  was waiting for the application. Rising on a channel the other end sends on: nobody reads it, or it is read more
  slowly than it is written. [TROUBLESHOOTING.md](TROUBLESHOOTING.md) has the row.
* `SpscRing<T>.TryPeek`: a copy of the oldest element, which stays queued (consumer thread).
* `Lz4Block.GetInPlaceMargin`: the bytes beyond the decoded size that an LZ4 block needs to be decoded in place
  when all of it sits in one buffer (the reference implementation's in-place margin). The receiver's own in-place
  decode keeps that margin on its stack, so it stages a compressed message in a block of its decoded size.

### Known limits

* **An unread `ReliableUnordered` channel can take the connection's stream slots.** The stream limit is one number
  for all channels, every unread group keeps its stream until it is read, and the sender keeps opening groups. The
  sender's other channels that still have to *open* a stream towards this receiver — the first message of an
  ordered channel, other groups, large `ReliableLatest` values, bulk transfers — then wait for stream credit until
  the channel is drained. Streams that are already open and all datagram channels are unaffected, and nothing is
  lost. Read every `ReliableUnordered` channel the other end sends on.
* **Unread channels that hold the transport's connection flow-control window between them stop the connection's
  streams.** The bytes of a held stream stay in the transport: up to 2 MiB per stream and 16 MiB per connection
  with the MsQuic defaults (`MsQuicSettings`). One unread ordered channel is far from that; eight of them reach it,
  and then no stream channel of the connection receives until the application reads (the receiving side's
  datagram channels are not affected). A receiver that grants its peer more unidirectional streams than its
  table's sum — an MsQuic client with `ClientPeerUnidiStreamCount` raised, as 0.2.1's default of 1 024 was — can
  reach it with held groups alone when it falls behind (514 groups of 32 KiB), and then also stall for up to
  `StreamIdleTimeout` (30 s) after it catches up: a message it has begun to receive waits for the rest, which
  needs window that only the other held streams can free, and holds its buffer — the whole receive budget for a
  message above 64 KiB with the default pool — so the other streams wait for budget. The message is then given up
  (the sender sees it `Failed`). Keep the default grant, or raise `ConnFlowControlWindow` with it.
* **The sender's queue is shared.** What a sender keeps sending into a channel the other end does not read stays
  in its send table and send budget, which all its channels share; when they are full its sends on every channel
  are refused (`OutOfBuffers`), datagrams included. `ChannelOptions.QueueLimitBytes` bounds one channel's part.
* **Channels that were drained and are then abandoned keep what they accepted**, up to half the receive budget
  together with the channels nobody reads (and one message each on top of it), until they are drained again.
* **Some compressed messages of reliable channels are still decoded into a second buffer, and dropped
  (`DecodeFailures`) when none is free**, after the sender was told `Delivered`, as in 0.2.1. Each is tried once and
  never waited for, so the channel goes on: a message whose decoded size does not fit the largest pool block within
  `ReceiveBudgetBytes` (the edge band: with the default pool, a decoded size above 256 KiB on a channel whose
  `MaxMessageSize` is raised past it, or a budget below the decoded size; no block within the budget holds it, so it
  is always dropped, as in 0.2.1); a message that arrived while its channel had a handler and is read through
  `Drain` after `UnregisterHandler`; and a handler's message whose own block cannot hold its decoded size (a handler
  never waits). A pool with a free block of the decoded size's class, within the budget, decodes the last two.
* **An MsQuic receiver whose thread pool is starved for seconds can refuse peer streams, and lose what they
  carried.** Slots of closed streams come back when the thread pool's cleanup work item has closed them, and the
  stream table grows for them only up to twice its size (so a peer's churn cannot grow it without bound). Group
  streams close all the time, so an honest sender's groups can reach that cap too; each refused stream counts in
  `MsQuicTransport.RefusedPeerStreamCount` and is said through the diagnostic sink. The default table (2 048
  slots, so 4 096 at the cap) leaves a wide margin; a larger `MsQuicTransportOptions.MaxStreams` raises the cap
  with it. Keep the thread pool from being starved for that long.
* On a `ReliableOrdered` channel everything behind an unread message waits with it (the stream is ordered),
  including a response to this end's own request on that channel.
* A compressed message dispatched to a **handler** is still dropped (`DecodeFailures`) when it exceeds
  `DecodedBytesPerSecond` (8 MiB/s by default, and a burst of the same), on reliable channels too — a burst of
  compressed messages that decode to more than that loses the rest — and, when its own block cannot hold its decoded
  size, when the application still holds decoded payloads (retained, or drained and not released) that took the
  receive budget past its limit, or the pool has no free block of the decode's size class or a larger one within
  the budget. Raise `DecodedBytesPerSecond` for a channel that receives more, or read it with `Drain`, which waits
  for the decode budget and decodes in place instead.
* **A pended message start on an undersized shared pool waits for any block holder.** A start that cannot have its
  block holds nothing while it waits, so peers on one pool never wait for each other for good; but an application
  that retains received payloads for ever starves the others, as it does for uncompressed messages.
* **A Bulk sender that cancels more than three times `BulkTransfersPerDirection` transfers (six at the default)
  while the receiver's host is late** has its next transfer refused (`LimitExceeded`, the transfer ends `Failed`)
  until the receiver polls again.
* A channel whose engine the application replaced (`PeerOptions.EngineFactory`) has no receive credit and can
  still hold the receive ring when it is not drained.

## 2026-09-30: messages dropped on localhost

Packages are numbered by the publish workflow (`0.2.<run number>`), so this section is named by its date: it
describes the first package published on or after 2026-09-30. "Earlier releases" below are the packages before it.

The subject of this release is one report: *messages go missing at random on localhost after the library has run
for a while*. An audit found four independent causes in the session layer, and the work on them a fifth in the
MsQuic transport (a held stream that was never resumed). All five are fixed, every remaining way to drop a message
is counted, and [TROUBLESHOOTING.md](TROUBLESHOOTING.md) maps a symptom to its counter. One defect that loses
reliable messages is known and **not** fixed in this release: a `ReliableUnordered` channel whose receiver falls
far behind, see "Known limits". **There is no wire change**: either end can be upgraded alone (what an old end
still does wrong is listed under "Mixed versions").

### Fixed

* **A key that idled was dropped as stale** (`UnreliableSequenced`, `ReliableLatest`). Sequences and versions are
  numbered per channel but were compared per key, in serial arithmetic. A key that was quiet while its channel
  sent more than half the sequence space (32 768 messages on a 16-bit channel, 2^31 versions) had its next values
  dropped, and a `ReliableLatest` value could be reported `Delivered` without having arrived. Receivers now order
  values on a 64-bit per-channel sequence clock; a key may idle indefinitely.
* **A message expired before it was ever offered to the transport.** Expiry was counted from the clock stamp of
  the last `Poll`/`Flush` before the send, which can be arbitrarily old (a long frame, a quiet server peer). A
  request's timeout could fire in the `Flush` that sent the request, for the same reason.
* **A channel nobody drained stopped every other channel of the peer.** Messages of a channel without a handler
  wait for `Drain`; once their queue pool was full the next one was held and nothing more left the receive ring —
  datagrams were dropped on arrival, streams back-pressured, and from the second `Poll` on even coalescing and
  `ReliableLatest` handlers stopped. For *unreliable* channels this is fixed (see "Draining" below); for
  *reliable* channels without a handler it remains, see "Known limits". Coalescing and `ReliableLatest` handlers
  now run whatever is held.
* **Over MsQuic, a reliable stream held by a late receiver could stall for good.** When the receiver cannot take
  more — its receive ring is full, `ReceiveBudgetBytes` is used up, the application is not polling — it holds the
  stream and resumes it later. A resume that had taken no bytes could be lost inside MsQuic when it raced the
  receive callback that was still returning; the stream was then never indicated again. The sender had already
  completed the stream's messages `Delivered`, they never arrived, no counter moved and nothing timed out. Every
  stream channel was exposed (`ReliableOrdered`, `ReliableUnordered`, large `ReliableLatest` values, `Bulk`), also
  under the WebTransport carrier when it runs over the MsQuic transport. A hold is now answered to MsQuic as a
  partial consumption and the resume only re-enables receiving, which MsQuic cannot drop. Nothing for the
  application to do; the fix is on the receiving end.
* **`Release` after `Dispose` leaked the block** on a peer over a shared allocator — every server peer.
* **Drops by the transport were not counted.** A datagram the transport cancelled because it could not send it at
  once, or declared lost, left no trace in the statistics. See "New".
* **A thread-safe send that raced the start of a `Poll` could wait without a wake-up.** `Poll` re-armed the work
  signal with a plain store, which on x64 could still be in the store buffer while the `Poll` looked at the
  thread-safe send queue and the completion ring. A send from another thread (or a tracked-send completion) landing
  in that window was neither taken by the `Poll` nor announced by `OnWork`; it waited for the next publication or
  the host's own timer. Seen only in Release builds, and only by a host that sleeps on the signal. The re-arm is
  now an interlocked exchange. `QuiclyServer.PollAll` was not affected.

### Behaviour changes — read these before upgrading

* **Expiry counts from the message's first scheduler pass, not from the send call.**
  `SendOptions.ExpiryMicros` / `ChannelOptions.ExpiryMicros` bound the time the *scheduler* holds a message back
  (the send cap, datagrams unavailable, a blocked stream head). The clock starts at the first `Flush` — or the
  `Immediate` pass — after the send, and that pass never expires the message. A host that sends for a long time
  without flushing therefore transmits everything it queued at the next `Flush`, however old, where earlier releases expired
  all but the last expiry's worth. How much that can be is bounded by the send table (`SendTableCapacity`, 1 024
  messages by default) and the send budget, and by `ChannelOptions.QueueLimitBytes` when you set it (its default
  is no limit). To bound a message's age from the send call, track the send and cancel it (`TryCancel`).
  A message that is held back past its expiry is dropped at the next pass after the deadline, not at the deadline:
  no timer runs for it.
* **`SendRequestAsync` timeouts count from the call**, not from the last `Poll`/`Flush`. A timeout is never
  earlier than before.
* **`ReliableLatest`: a tracked value completes `Delivered` only on the acknowledgement of exactly the version
  last transmitted.** Acknowledgements are no longer cumulative. For every case earlier releases got right the result is the
  same. A channel whose version counter runs 2^31 versions ahead with nothing arriving in between ends the
  affected values `Failed` (earlier releases: possibly a false `Delivered`).
* **`UnreliableSequenced`: resynchronisation.** A sequence that reads as old is normally dropped as stale. It is
  taken for a forward jump instead — the channel lost half its sequence space or more — when it is more than
  1 024 behind the newest sequence of the channel (65 536 on a 32-bit channel) *and* arrives more than 2 s after
  that newest sequence last advanced (`PeerStatistics.SequenceResyncs`, normally 0). Earlier releases kept dropping until
  the sender's counter came round.
* **Draining: an unreliable channel read with `Drain` must be drained completely once per `Poll`.** Do that —
  call `Drain` until it returns 0, every frame, before or after the `Poll`, for each channel you read this way —
  and nothing changes: a burst of any size the receive ring and `ReceiveBudgetBytes` take is delivered, as on
  earlier releases. That holds for any number of drained channels, and for what the server sends with its admission: the
  `Poll` in which the peer becomes Connected (`QuiclyClient.ConnectAsync` runs it for you) counts as drained, so
  the first frame of `Poll` and `Drain` after `ConnectAsync` gets everything. What is new is what happens to
  messages that are *still queued when the next `Poll` begins*: they are backlog, and the backlog of the
  unreliable channels is bounded. With every option at its default it keeps 512 messages when the channel table
  has a `ReliableOrdered`/`ReliableUnordered` channel (1 024 otherwise) and 64 KiB counted in buffer blocks — 1 024
  messages of up to 64 bytes, 256 of 65 to 256 bytes, 42 of 257 to 1 536 bytes — and drops the **oldest** beyond
  that, counted in `DrainQueueDrops`. So a host that polls several times between two drains, or drains with a
  fixed span without calling again, now loses the older part of a large burst where earlier releases kept it (and stalled
  the peer when nobody came). **This includes the common server loop that polls at network rate and drains at
  simulation rate** — `PollAll` every few milliseconds, `Drain` once per tick: only what the last `Poll` before
  the `Drain` queued is outside the bound, so with every option at its default such a host keeps, of everything
  the earlier `Poll`s of the tick queued, the newest 42 full-size datagrams (64 KiB, all its undrained channels
  together), where earlier releases kept whatever the ring and the budget held. Drain after
  every `Poll`, register a handler, or raise `ReceiveBudgetBytes` for a larger byte bound; `ReceiveRingCapacity`
  does not raise the 1 024-message bound. A backlog ends when a `Drain` finds or leaves the channel's queue
  empty. A channel with a handler never loses a message here, also when a `Drain` of another channel runs before
  the `Poll`.
* **`HasPendingWork` no longer reports a value that waits in the mailbox of a channel
  without a handler** (a coalescing channel, `ReliableLatest`). No `Poll` can consume it, and reporting it kept a
  host that polls while there is work polling for good. A host that reads such a channel with `Drain` drains on
  its own tick or on the work signal (`IPeerWorkSignal`), not on the probe.
* **`HasPendingWork` re-arms the work signal when it answers `false`.** The signal is an edge: it is raised for
  the first work published and then stays silent until it is re-armed. In earlier releases only a `Poll` re-armed it. Now a
  `HasPendingWork` that finds nothing re-arms it too, so a host that wakes on the signal, asks the probe and goes
  back to sleep without polling is woken by the next publication — without this, a value arriving for a
  `Drain`-style mailbox channel (which the probe no longer reports) would have silenced the signal until the
  host's next `Poll`. A probe that answers `true` leaves the edge alone, so a burst still costs one `OnWork`
  call. The only visible difference: a host that asks the probe while idle can get one `OnWork` per probe
  interval instead of one per `Poll`.
* **`Release` after `Dispose` is no longer a no-op on a shared allocator.** Every lease must be released exactly
  once, also after its peer is gone; a second release of the same lease corrupts the pool in a Release build (it
  throws with `ValidateLeases`), as it always did on a live peer. Release before the shared allocator itself is
  disposed.
* **`PeerStatistics` and `ChannelStatistics` grew.** The new fields are appended; no existing field moved.
* **Admission waits for the datagram capability and keeps its deadline** (from `main`, see
  [CHANGELOG.md](../CHANGELOG.md), "QUIC admission"). When the channel table needs datagrams, the client
  sends its Hello, and the server answers one, only once the transport has reported whether datagrams were
  negotiated; `CompleteAdmission`, a Hello and a HelloAck that are processed after the admission deadline close
  the session with `Timeout` instead of completing it late.

### New

* `ChannelStatistics.TransportCanceled`, `TransportLost`, `DrainQueueDrops`.
* `PeerStatistics.DatagramsAcknowledged`, `DatagramsLost`, `DatagramsCanceled`, `SequenceResyncs`,
  `DrainQueueDrops`. On a transport that reports datagram send states, `DatagramsSent` equals acknowledged + lost +
  cancelled + in flight + ended by a close or reconnect.
* `PeerOptions.DropWhenBlocked` (default `true`, the behaviour of earlier releases): `false` sends unreliable datagrams without
  the transport's cancel-on-blocked flag, so a datagram the transport cannot send at once waits in its queue
  instead of being dropped. The price is stale data and a queue that grows under overload; on an
  `UnreliableSequenced` channel, do not combine it with `Immediate` sends (a queued datagram can then be
  overtaken, see PROTOCOL.md §8).
* `SerialNumber.Extend` (16- and 32-bit), `SlabAllocator.IsDisposed`.
* [TROUBLESHOOTING.md](TROUBLESHOOTING.md).

### Mixed versions

* `UnreliableSequenced` ordering is decided by the receiver of each direction: an earlier receiver still drops the
  values of a key that idled past half the range, whatever the sender runs.
* `ReliableLatest`: a fixed receiver is correct against any sender. A fixed sender against an earlier receiver ends
  a value of a key that idled for 2^31 versions or more `Failed`, where an earlier sender reported a false
  `Delivered`; in both cases the receiver did not take the value.
* The held-stream stall over MsQuic is fixed by upgrading the *receiving* end; an earlier receiver can still
  stall a stream, whatever the sender runs.

### Known limits

* **A `ReliableUnordered` channel can still lose whole groups when the receiver falls far behind. Known, not fixed
  in this release.** The sender frees a group's stream as soon as QUIC acknowledged it, while a receiver that has
  not processed that stream yet (it is not polling, its ring or budget is full) still counts it open. The sender
  then opens more group streams than the receiver accepts — `MaxGroups` per channel, 8 by default; over MsQuic a
  client also grants 1 024 streams in its handshake, more than the receiver has records for. The receiver resets
  the streams beyond its limit, and the sender has already reported their messages `Delivered`. It shows as
  `PeerStatistics.StreamsReset` rising on the **receiver**; the sender sees nothing. Until the fix: poll every
  tick, and use `ReliableOrdered` for messages that must survive a receiver that stops polling for a while (a
  larger `MaxGroups` moves the point at which it happens, it does not remove it). A fix is in review and ships in
  the next release.
* **A `ReliableOrdered` / `ReliableUnordered` channel without a handler that is never drained still holds up all
  receiving on the peer** once the queue pool is full. Give every reliable channel the other end sends on a
  handler, or drain it. (A per-channel receive credit that confines the back-pressure to that channel is designed
  and being built.) If the undrained channel is `ReliableUnordered`, the limit above applies to it as well: its
  receiver is behind for as long as nobody drains it.
* An unreliable channel nobody drains keeps the receive ring closed for one `Poll` interval the first time a
  burst fills the queue pool; after that it only loses its own oldest messages, and it is not held again until
  it has been drained. Several such channels do this once each. A channel that was drained and then no longer is
  can keep the ring closed for two intervals, and so can a burst that arrives with the session's first `Poll`
  (which counts as drained).
* `UnreliableSequenced`: a datagram that more than 1 024 later messages of its channel overtook in the sender's
  transport queue (65 536 on a 32-bit channel), arriving after the channel stayed quiet for 2 s, is delivered out
  of order once. It needs a link congested for seconds and datagrams sent without cancel-on-blocked.
* `ReliableLatest`: 2^31 consecutive versions of a channel with no arrival in between still cost the affected
  values (they end `Failed`).
* The session benchmarks have not been re-run for these changes; the added work per message is a few loads and
  branches and the zero-allocation tests pass.
