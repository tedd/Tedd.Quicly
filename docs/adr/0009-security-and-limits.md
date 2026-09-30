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
  (`TransportCapabilities.PeerUnidirectionalStreams`; an MsQuic client grants 1 024 in its transport parameters
  by default, and QUIC never takes granted credit back). The session keeps its per-stream receive state for the
  larger of the two (`PeerCore.PeerStreamCapacity`): one 64-byte record of the group engine and 8 bytes of the
  pended-stream ring per stream. **Both numbers are this end's own configuration — its channel table and its
  transport options — so a peer cannot make the state larger than the host chose.** On a server peer the grant is
  0 and the state is the table's sum: 0.6 KiB of records for one group channel and one ordered channel. On a
  client peer at the default grant it is 64 KiB of records and a 16 KiB ring; at the most the option allows
  (65 535) it would be 4 MiB and 1 MiB, and the transport's stream table (`MaxStreams`, default 2 048, a quarter
  of it kept for local streams) has to be raised before that many streams are admitted at all. A peer that
  opens every stream it may on a single channel holds one half-received message per stream — a ring reservation
  and a staging lease, the lease inside the receive byte budget. That is more than the per-channel cap allowed
  it on that one channel, and the same total it could always hold across the group and ordered channels; the
  shares of ReliableLatest and Bulk channels, whose receive paths reserve no ring slot, are now usable for it
  too. The stream idle rule still resets the streams it leaves unfinished, the records are per connection, so
  it takes none from another peer or another channel, and what it does take is the stream slots of its own
  other channels.
* **Retransmission cannot be weaponised**: ReliableLatest has per-version and per-peer retry budgets;
  acks are coalesced per key (the highest accepted version); Pong is rate-limited; control message rate is capped.
* **Sequence numbers give the peer nothing it did not have** (PROTOCOL §8 "sequence clock"). Only the
  authenticated peer can produce datagrams (QUIC's AEAD and packet numbers exclude third-party replay and
  duplication), and it could always send any sequence it liked. A tracked key that was updated recently is
  judged exactly: nothing at or below its value is accepted. A tracked key that idled past half the range accepts
  its next value whatever the wire sequence — the position an evicted or new key is already in (§7), no wider. A
  sequence far ahead moves the channel's clock by less than half a range and only makes that peer's own later
  values stale; the time-based resynchronisation of `UnreliableSequenced` is equivalent to sending a fresh higher
  sequence. State is fixed-size, work is O(1) per message, nothing allocates. A `ReliableLatest` ack completes a
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
