# ADR 0009 — Security posture and resource limits

**Status:** accepted (2026-09-15)

## Decisions

* **Admission happens twice**: `IAdmissionPolicy.PreHandshake(in NewConnectionInfo)` runs in the listener's
  new-connection callback (remote address, SNI, ALPN — before any TLS work; rejecting drops the connection
  with no application state), and `Admit(in HelloInfo, peer)` runs on the first control frame. Until the
  Hello is accepted the connection has one bidirectional stream and zero unidirectional streams, application
  traffic is dropped and counted, and an `AdmissionTimeout` (5 s) closes it. Unadmitted connections are
  capped server-wide. MsQuic's stateless retry memory threshold and retry-key rotation are surfaced as server
  options.
* **Tokens** (PROTOCOL §4.1): session tokens are server-minted, HMAC-protected, rotated, single-use, expiring
  locators; resuming still requires an auth token; all token failures return the same status; comparisons
  are constant-time; failed attempts are rate-limited per address. Channel binding to the TLS session waits
  for keying-material export to leave MsQuic's preview API.
* **Every parser is bounded before it allocates** (PROTOCOL §3.4 frame bounds, §7 limits): control frame
  length, token/reason/name lengths, table size, fragment arithmetic from the first fragment, `RawLength`
  ≤ `MaxMessageSize` with exact-size decoding, bulk range arithmetic, non-minimal varints rejected.
  Decompression runs on the game thread with a per-peer decoded-bytes budget.
* **Remote-chosen identifiers are capped and have an eviction rule** (keys, groups, streams, reassemblies,
  transfers, requests); the rule is written per limit in PROTOCOL §7, and every violation is a counter.
  The cap on a `ReliableUnordered` channel's group streams is the connection's unidirectional stream limit, not
  the channel's `MaxGroups` (amended 2026-09-30). `MaxGroups` was enforced by the receiver with a reset, on the
  belief that both ends free a stream's slot at the same event. They do not: a stream is over for its sender
  when its data and FIN are acknowledged, and the receiver holds it open until it has read it, so a receiver
  that was behind reset streams of a sender that had kept the limit — after the sender had completed their
  messages `Delivered`. The bound is what the transport admits, and a slot returns only when the receiver has
  closed a stream: the limit the session asks for after admission (Σ max(`MaxGroups`, 1) over the stream
  channels, at most 4 096), or the transport's own initial grant when that is more
  (`TransportCapabilities.PeerUnidirectionalStreams`; what an MsQuic client grants in its transport parameters —
  `ClientPeerUnidiStreamCount`, 0 by default since the third review round of this amendment and 1 024 before —
  and QUIC never takes granted credit back). The session keeps its per-stream receive state for the larger of
  the two (`PeerCore.PeerStreamCapacity`): one 64-byte record of the group engine, 12 bytes of the pended-stream
  ring (the id and the block the stream waits for) and 16 + 16 bytes of the credit lists (`ReceiveCredit`'s ring
  of held-back streams and the game thread's copy) per stream, the rings rounded up to a power of two above the
  capacity. **Both numbers are this end's own configuration — its channel table and its transport options — so a
  peer cannot make the state larger than the host chose,** but for one factor: the two lists of held-back
  streams grow, up to eight times, for the entries of streams a peer resets while they are held (see the next
  item), and a peer that needs more than that is disconnected. On a server peer, and on a client at the default
  grant, the state is the table's sum: 0.6 KiB of records for one group channel and one ordered channel, and the
  lists a few hundred bytes. On a client that grants 1 024 it is 64 KiB of records, a 24 KiB pended ring and
  32 + 32 KiB of credit lists (the rings up to eight times that under churn); at the most the option allows
  (65 535) it would be 4 MiB, 1.5 MiB and 2 + 2 MiB. The transport's stream table follows the same two numbers: it is
  `MaxStreams` slots (default 2 048) or as many as the grants need next to a quarter for this end's own streams,
  so a stream the peer was allowed to open always finds a slot, and the local streams cannot take the slots of
  the granted ones; while slots of closed streams wait for their native close (the thread pool's cleanup work
  item) the table grows, but only to twice that size — a peer that churns streams faster than a starved pool
  closes them has a stream refused past that (counted) rather than growing the table without bound. The cap is
  not only a hostile peer's: an honest peer's group streams close all the time, so on a receiver whose thread pool
  is starved for seconds they can reach it too, and a refused stream's data can be lost (the closes cannot run
  inline instead, on the MsQuic worker whose callbacks they wait for); slots are
  created as streams use them, and the WebTransport carrier's mirror of the table
  grows the same way (slot records on first use, preamble storage in chunks of 256 slots). A peer that
  opens every stream it may on a single channel holds one half-received message per stream — a ring reservation
  and a staging lease, the lease inside the receive byte budget. That is more than the per-channel cap allowed
  it on that one channel, and the same total it could always hold across the group and ordered channels; the
  shares of ReliableLatest and Bulk channels, whose receive paths reserve no ring slot, are now usable for it
  too. The stream idle rule still resets the streams it leaves unfinished, the records are per connection, so
  it takes none from another peer or another channel, and what it does take is the stream slots of its own
  other channels.
* **A channel the application does not read is bounded per class** (PROTOCOL §7 "Channels nobody drains", added
  2026-09-30). A peer chooses which channels it sends on, so it must not be able to stall a receiver through a
  channel the application happens not to drain. What unreliable ring channels still have queued when a Poll begins,
  undrained since the Poll before, is their backlog, and it is capped (the part of the drain-queue pool not reserved
  for reliable channels, and a quarter of the receive byte budget) with oldest-first eviction from the backlogged
  channel furthest over its share: a flood on a channel nobody reads costs that channel its old messages and O(1)
  work per message (a backlogged channel *under* its share that needs room scans the unreliable channels for the
  longest queue, O(their number), and remembers the answer; each Poll with something queued walks them once). The
  cap is deliberately not applied to a channel the application *is* draining — that would cut bursts the ring and
  the budget had already accepted — so the first burst on an undrained channel is queued whole and may hold the
  ring until the next Poll; from that Poll on the channel is backlog and never holds again — the mark is cleared
  only by the application's own Drain (or a handler), not by other channels evicting its queue to nothing, or two
  unread channels could take turns at closing the ring. A peer can therefore close the ring through the unread
  unreliable channels of a table for one Poll interval per channel in total (two for a channel the application
  stopped draining, and at the start of a session, whose first Poll counts as drained), not longer, and pin more
  than the quarter of the budget for as long, not longer. Reliable channels cannot be evicted, so they are bounded
  where a message is accepted: a per-channel **receive credit**, checked on the transport thread before a message
  takes a ring entry and a buffer. A reliable channel nobody reads may have its reserved share of the queue pool
  and the same part of a quarter of the byte budget waiting; past that the receiver stops consuming that channel's
  streams and QUIC flow control holds their sender — the peer that floods the channel is the one that waits. The
  ring, the other channels and the host's work probe are untouched, for any number of unread reliable channels:
  together they can pin half the queue pool and a quarter of the budget — the byte share is strict, a message
  whose buffer block does not fit in it is not started — which leaves the other half of each to the traffic that
  is read. A channel with a handler is not held to the share (the ring and the budget bound it, as before). A
  channel the application drains every frame is bounded by the ring and by half the budget, counted over every
  reliable channel no handler reads, because it keeps what it accepted when the application stops draining it and
  falls back to the share (within two Poll intervals): the peer chooses the burst, the application chooses when it
  stops, and half the budget is what all such channels together, with the channels nobody reads, can then hold
  (plus a message each; the first version gave each of them half, and two abandoned channels held the whole
  budget; the second divided the half among every reliable channel of the table, handled and idle ones included,
  which cut what a drained channel took per frame for nothing). A channel nobody reads starts no message that does
  not fit in what those channels leave of that half, next to its strict share. The lists of held-back
  streams are bounded too — the credit list, and the ring of streams held for the receive ring or the budget: a
  peer that resets held streams and opens new ones leaves an entry per stream until the receiver's next Poll;
  each list grows to eight times the streams the peer may have open and then the connection is closed
  (`LimitExceeded`), and a Poll lets go of the entries beyond what the live streams account for, so a receiver
  that polls every frame keeps them from growing at all. What an attacker keeps is what QUIC gives it anyway: it
  can stall its own streams, fill the stream slots of the connection that those streams occupy, and fill the
  connection's flow-control window with data nobody reads (16 MiB with the MsQuic defaults), which stops its own
  other streams — not another connection's. A compressed message's decode buffer may take the receive budget past
  its limit by that one buffer (the budget can be full of the compressed messages that wait for it): a decode is
  refused once the budget is over its limit, so it is never exceeded by more than one decode buffer, and the
  transport accepts nothing new until it is back within the limit — a peer gains one buffer of at most the
  budget's size, not more.
* **Retransmission cannot be weaponised**: ReliableLatest has per-version and per-peer retry budgets;
  acks are coalesced per key (the highest accepted version); Pong is rate-limited; control message rate is capped.
* **Sequence numbers give the peer nothing it did not have** (PROTOCOL §8 "sequence clock"). Only the
  authenticated peer can produce datagrams (QUIC's AEAD and packet numbers exclude third-party replay and
  duplication), and it could always send any sequence it liked. A tracked key that was updated recently is
  judged exactly — nothing at or below its value is accepted — for as long as the channel's clock does not
  resynchronise. A tracked key that idled past half the range accepts its next value whatever the wire sequence —
  the position an evicted or new key is already in (§7), no wider. A sequence far ahead moves the channel's clock
  by less than half a range and only makes that peer's own later values stale. The resynchronisation of
  `UnreliableSequenced` (a sequence more than the reorder window behind the clock, after 2 s without an advance)
  moves the clock forward by up to a range, which re-opens every tracked key of that channel to lower wire
  sequences once; for the peer that is equivalent to sending a fresh higher sequence, which it always could. It is
  also the one place where the peer's *own* sender can cause a misjudgment without meaning to: a datagram that
  more than a window of later messages overtook in its transport queue, arriving after 2 s of quiet, is delivered
  out of order once (PROTOCOL §8 says when that can happen and how to avoid it). State is fixed-size, work is O(1) per message, nothing allocates. A `ReliableLatest` ack completes a
  value only when it names exactly the version last transmitted for the key, so no ack the peer can forge or
  replay reports a value `Delivered` that was not the one on the wire.
* **0-RTT is never used for QUICLY frames**; servers default to no TLS resumption.
* **Both endpoints enforce the same limits** — a client parses hostile servers too.
* **HTTP/3 co-hosting is opt-in** with a per-ALPN MsQuic configuration and HTTP-specific limits; static
  files are a further opt-in, root-jailed, MIME allow-listed, no listing, no symlinks.
* **HTTP/1.1 exposure** defaults to the ACME responder plus a redirect to HTTPS; static files and health are
  opt-in. Hard limits: request line ≤ 2 KiB, headers ≤ 8 KiB / 32 entries, header read timeout 5 s,
  keep-alive idle 15 s, ≤ 64 connections per address, ≤ 2 048 total, no request bodies unless a handler
  opts in, GET/HEAD only by default, canonicalised paths. The parser is fuzzed.
* **Certificates**: the preferred credential path is `QUIC_CREDENTIAL_TYPE_CERTIFICATE_PKCS12` (no store
  import, no persisted key container); the `CERTIFICATE_CONTEXT` path, when used on Windows, imports with
  `PersistKeySet | UserKeySet` (interactive) or `PersistKeySet | MachineKeySet` (services) — Schannel needs a
  persisted, non-ephemeral key; `Exportable` is irrelevant to that and is not used; hot-swap on renewal is a
  new MsQuic configuration selected in the new-connection callback (`ConfigurationLoadCredential` is
  one-shot per configuration), the old configuration is closed afterwards; old key containers are deleted after the configuration swap has completed and
  all connections created with the old configuration are gone (MsQuic configurations are reference-counted:
  open new, swap the pointer used by the listener, close old). On Linux key files are 0600 and replaced
  atomically. `tls-alpn-01` requires seeing the ClientHello's ALPN before choosing a certificate, which
  `SslStream` does not expose, so the HTTP server peeks the ClientHello (SNI + ALPN) and replays it into
  `SslStream`.
* **ACME** (`docs/ACME.md`): account key stored via DPAPI on Windows / 0600 file elsewhere, never logged;
  EAB secret only used on `newAccount`; nonce pool with one `badNonce` retry; orders persisted and resumed;
  exponential back-off with jitter honouring `Retry-After`; renewal at the ARI window when the CA offers it,
  otherwise at ⅓ of the lifetime remaining; challenge material removed after validation; the `http-01`
  responder answers only registered tokens matching `[A-Za-z0-9_-]{22,}` and expires them after 10 minutes;
  `ReuseKey` is explicit (required when clients pin SPKI); the CA connection is always TLS-validated with an
  injectable trust anchor for the mock CA.
* **Key logging** (`QUIC_PARAM_CONN_TLS_SECRETS`) is a debug-only, environment-variable-gated client option
  and never available in the server.
