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
  than the quarter of the budget for as long, not longer. Reliable channels cannot be evicted; an undrained one
  still back-pressures the whole ring (a known limit, to be confined to its own streams by per-channel receive
  credit), so a host must handle or drain every reliable channel in its table.
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
