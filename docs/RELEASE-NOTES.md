# Release notes

What an application that upgrades has to know, newest release first. The protocol is described in
[PROTOCOL.md](PROTOCOL.md), what is merged and measured in [STATUS.md](STATUS.md), and which counter explains a
missing message in [TROUBLESHOOTING.md](TROUBLESHOOTING.md).

## The release after 0.2.0 (2026-09-30): messages dropped on localhost

The subject of this release is one report: *messages go missing at random on localhost after the library has run
for a while*. An audit found four independent causes. All four are fixed, every remaining way to drop a message is
counted, and [TROUBLESHOOTING.md](TROUBLESHOOTING.md) maps a symptom to its counter. **There is no wire change**:
either end can be upgraded alone (what an old end still does wrong is listed under "Mixed versions").

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
* **`Release` after `Dispose` leaked the block** on a peer over a shared allocator — every server peer.
* **Drops by the transport were not counted.** A datagram the transport cancelled because it could not send it at
  once, or declared lost, left no trace in the statistics. See "New".

### Behaviour changes — read these before upgrading

* **Expiry counts from the message's first scheduler pass, not from the send call.**
  `SendOptions.ExpiryMicros` / `ChannelOptions.ExpiryMicros` bound the time the *scheduler* holds a message back
  (the send cap, datagrams unavailable, a blocked stream head). The clock starts at the first `Flush` — or the
  `Immediate` pass — after the send, and that pass never expires the message. A host that sends for a long time
  without flushing therefore transmits everything it queued at the next `Flush`, however old, where 0.2.0 expired
  all but the last expiry's worth. How much that can be is bounded by the send table (`SendTableCapacity`, 1 024
  messages by default) and the send budget, and by `ChannelOptions.QueueLimitBytes` when you set it (its default
  is no limit). To bound a message's age from the send call, track the send and cancel it (`TryCancel`).
  A message that is held back past its expiry is dropped at the next pass after the deadline, not at the deadline:
  no timer runs for it.
* **`SendRequestAsync` timeouts count from the call**, not from the last `Poll`/`Flush`. A timeout is never
  earlier than on 0.2.0.
* **`ReliableLatest`: a tracked value completes `Delivered` only on the acknowledgement of exactly the version
  last transmitted.** Acknowledgements are no longer cumulative. For every case 0.2.0 got right the result is the
  same. A channel whose version counter runs 2^31 versions ahead with nothing arriving in between ends the
  affected values `Failed` (0.2.0: possibly a false `Delivered`).
* **`UnreliableSequenced`: resynchronisation.** A sequence that reads as old is normally dropped as stale. It is
  taken for a forward jump instead — the channel lost half its sequence space or more — when it is more than
  1 024 behind the newest sequence of the channel (65 536 on a 32-bit channel) *and* arrives more than 2 s after
  that newest sequence last advanced (`PeerStatistics.SequenceResyncs`, normally 0). 0.2.0 kept dropping until
  the sender's counter came round.
* **Draining: an unreliable channel read with `Drain` must be drained completely once per `Poll`.** Do that —
  call `Drain` until it returns 0, every frame, before or after the `Poll`, for each channel you read this way —
  and nothing changes: a burst of any size the receive ring and `ReceiveBudgetBytes` take is delivered, as on
  0.2.0. That holds for any number of drained channels, and for what the server sends with its admission: the
  `Poll` in which the peer becomes Connected (`QuiclyClient.ConnectAsync` runs it for you) counts as drained, so
  the first frame of `Poll` and `Drain` after `ConnectAsync` gets everything. What is new is what happens to
  messages that are *still queued when the next `Poll` begins*: they are backlog, and the backlog of the
  unreliable channels is bounded. With every option at its default it keeps 512 messages when the channel table
  has a `ReliableOrdered`/`ReliableUnordered` channel (1 024 otherwise) and 64 KiB counted in buffer blocks — 1 024
  messages of up to 64 bytes, 256 of 65 to 256 bytes, 42 of 257 to 1 536 bytes — and drops the **oldest** beyond
  that, counted in `DrainQueueDrops`. So a host that polls several times between two drains, or drains with a
  fixed span without calling again, now loses the older part of a large burst where 0.2.0 kept it (and stalled
  the peer when nobody came). **This includes the common server loop that polls at network rate and drains at
  simulation rate** — `PollAll` every few milliseconds, `Drain` once per tick: only what the last `Poll` before
  the `Drain` queued is outside the bound, so with every option at its default such a host keeps, of everything
  the earlier `Poll`s of the tick queued, the newest 42 full-size datagrams (64 KiB, all its undrained channels
  together), where 0.2.0 kept whatever the ring and the budget held. Drain after
  every `Poll`, register a handler, or raise `ReceiveBudgetBytes` for a larger byte bound; `ReceiveRingCapacity`
  does not raise the 1 024-message bound. A backlog ends when a `Drain` finds or leaves the channel's queue
  empty. A channel with a handler never loses a message here, also when a `Drain` of another channel runs before
  the `Poll`.
* **`HasPendingWork` / `HasPendingPollWork` no longer report a value that waits in the mailbox of a channel
  without a handler** (a coalescing channel, `ReliableLatest`). No `Poll` can consume it, and reporting it kept a
  host that polls while there is work polling for good. A host that reads such a channel with `Drain` drains on
  its own tick or on the work signal (`IPeerWorkSignal`), not on the probe.
* **`HasPendingWork` re-arms the work signal when it answers `false`.** The signal is an edge: it is raised for
  the first work published and then stays silent until it is re-armed. On 0.2.0 only a `Poll` re-armed it. Now a
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

### New

* `ChannelStatistics.TransportCanceled`, `TransportLost`, `DrainQueueDrops`.
* `PeerStatistics.DatagramsAcknowledged`, `DatagramsLost`, `DatagramsCanceled`, `SequenceResyncs`,
  `DrainQueueDrops`. On a transport that reports datagram send states, `DatagramsSent` equals acknowledged + lost +
  cancelled + in flight + ended by a close or reconnect.
* `PeerOptions.DropWhenBlocked` (default `true`, the 0.2.0 behaviour): `false` sends unreliable datagrams without
  the transport's cancel-on-blocked flag, so a datagram the transport cannot send at once waits in its queue
  instead of being dropped. The price is stale data and a queue that grows under overload; on an
  `UnreliableSequenced` channel, do not combine it with `Immediate` sends (a queued datagram can then be
  overtaken, see PROTOCOL.md §8).
* `SerialNumber.Extend` (16- and 32-bit), `SlabAllocator.IsDisposed`.
* [TROUBLESHOOTING.md](TROUBLESHOOTING.md).

### Mixed versions

* `UnreliableSequenced` ordering is decided by the receiver of each direction: a 0.2.0 receiver still drops the
  values of a key that idled past half the range, whatever the sender runs.
* `ReliableLatest`: a fixed receiver is correct against any sender. A fixed sender against a 0.2.0 receiver ends
  a value of a key that idled for 2^31 versions or more `Failed`, where a 0.2.0 sender reported a false
  `Delivered`; in both cases the receiver did not take the value.

### Known limits

* **A `ReliableOrdered` / `ReliableUnordered` channel without a handler that is never drained still holds up all
  receiving on the peer** once the queue pool is full. Give every reliable channel the other end sends on a
  handler, or drain it. (A per-channel receive credit that confines the back-pressure to that channel is designed
  and being built.)
* An unreliable channel nobody drains keeps the receive ring closed for one `Poll` interval the first time a
  burst fills the queue pool; after that it only loses its own oldest messages, and it is not held again until
  it has been drained. Several such channels do this once each. A channel that was drained and then no longer is
  can keep the ring closed for two intervals.
* `UnreliableSequenced`: a datagram that more than 1 024 later messages of its channel overtook in the sender's
  transport queue (65 536 on a 32-bit channel), arriving after the channel stayed quiet for 2 s, is delivered out
  of order once. It needs a link congested for seconds and datagrams sent without cancel-on-blocked.
* `ReliableLatest`: 2^31 consecutive versions of a channel with no arrival in between still cost the affected
  values (they end `Failed`).
* The session benchmarks have not been re-run for these changes; the added work per message is a few loads and
  branches and the zero-allocation tests pass.
