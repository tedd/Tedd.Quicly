# QUICLY wire protocol, version 1

Normative words: MUST / MUST NOT / SHOULD / MAY as in RFC 2119.

All multi-byte **variable-length integers** ("varint") use the QUIC encoding (RFC 9000 §16: 2-bit length prefix,
big-endian, 1/2/4/8 bytes, maximum 2^62−1). Encoders MUST use the minimal length; decoders MUST reject
non-minimal encodings (protocol error). All **fixed-width** integers (sequence numbers, versions, timestamps,
error codes) are **little-endian**.

Two ALPNs can be served on one UDP port:

* `quicly/1` — raw QUIC (the default and only ALPN unless HTTP/3 is enabled). Frames map 1:1 onto QUIC
  DATAGRAM frames and QUIC streams.
* `h3` — HTTP/3 + WebTransport (opt-in, §5). Frames are carried inside a WebTransport session; the
  WebTransport prefixes are stripped before QUICLY parsing, so the two carriers are byte-identical at the
  QUICLY layer.

Every limit in this document applies to **both** endpoints: a client parses the same frames as a server and
MUST enforce the same bounds.

## 1. Channels

| Channel | Meaning |
|---|---|
| 0 | control (session handshake, ping, acks, key retirement, close) |
| 1 | packed container: several messages in one datagram |
| 2 … 63 | application channels with a one-byte channel id |
| 64 … 16383 | application channels with a two-byte channel id |

Application channels are declared once in a `ChannelTable`; both sides MUST hold the same table. Each entry:

| Field | Meaning |
|---|---|
| `Id` | 2…16383 |
| `Name` | diagnostics only; **not** part of the hash |
| `Mode` | `UnreliableUnordered` 0, `UnreliableSequenced` 1, `ReliableOrdered` 2, `ReliableUnordered` 3, `ReliableLatest` 4, `Bulk` 5 |
| `Keyed` | messages carry a `Key` |
| `SequenceBits` | 16 or 32 (ReliableLatest MUST be 32; default 32 for keyed channels, 16 for unkeyed) |
| `Fragmentation` | unreliable messages larger than one datagram are fragmented |
| `Compression` | 0 none, 1 LZ4 block (2 and 3 reserved) |
| `RequestResponse` | ReliableOrdered only: frames carry a request id and a response flag |
| `CoalesceOnReceive` | receiver delivers only the newest queued version per key (forced on for ReliableLatest, default off otherwise) |
| `Priority` | 0 (lowest) … 255 (highest); default 128 |
| `MaxMessageSize` | bytes of *raw* payload; default 1 200 for unreliable, 64 KiB for reliable, 16 MiB for Bulk |
| `QueueLimitBytes`, `ExpiryMicros`, `MaxKeys`, `MaxReassemblies`, `MaxGroups` | resource limits, see §7 |

Canonical encoding (for the hash, and sent by the server on request): `count varint`, then per channel in
ascending id order: `id varint, mode u8, flags u8 (bit0 keyed, bit1 seq32, bit2 fragmentation, bit3
request-response, bit4 coalesce, bit5-6 compression), priority u8, maxMessageSize varint`.
`TableHash` = xxHash64 (seed 0, as specified by the xxHash project) over that encoding. Names are exchanged
separately (`HelloAck.table`) and never adopted by the receiver.

Sequence counters are **per channel**, not per key (one counter serves every key of a channel), and restart
at 0 in every epoch. A key is therefore never confused with an earlier holder of the same key: the first
message for a reused key is always newer than anything the receiver remembers. Comparison is RFC 1982 serial
arithmetic in the channel's width; the maximum tolerated gap is 2^(bits−1)−1 *messages on the channel*. A
channel sending N messages per second with 16-bit sequences therefore tolerates a receiver stall of at most
32767/N seconds (≈ 5 min at 100 msg/s, ≈ 33 s at 1 000 msg/s); use 32 bits when in doubt.

## 2. Datagram frames

The first byte(s) of every datagram are a channel varint.

### 2.1 Application message (channel ≥ 2)

```
ChannelId   : varint
Sequence    : u16 LE | u32 LE   when Mode ∈ {UnreliableSequenced, ReliableLatest} or Fragmentation
Key         : varint            when Keyed
FragCount   : u8                when Fragmentation (1 = not fragmented)
FragIndex   : u8                when Fragmentation and FragCount > 1
RawLength   : varint            when Compression ≠ 0 (0 = payload is not compressed)
Payload     : remaining bytes
```

Header cost: 1 byte for an unsequenced, unkeyed channel; 3 bytes for a 16-bit sequenced unkeyed channel;
5 bytes for a 32-bit sequenced channel with a one-byte key.

Fragmentation rules: `FragCount` ∈ [1, 8] (delivery probability of a fragmented message is (1−p)^FragCount,
so large unreliable messages are a design smell; the library routes anything larger to a group stream when
`Fragmentation` is off); `FragIndex` < `FragCount`; all fragments of one
(channel, key, sequence) MUST carry the same `FragCount`; every fragment except the last MUST have the same
payload size as fragment 0 (so the total is known from the first fragment and checked against
`MaxMessageSize` before any buffer is chosen); duplicates are ignored; the reassembly scope is
(channel, key, sequence); on `UnreliableUnordered` the sequence is only a reassembly id. `RawLength`, when
present, describes the reassembled payload and appears in every fragment.

Compression rules: codec 1 is the LZ4 *block* format (no frame header). `RawLength` is the exact decoded size
and MUST be ≤ `MaxMessageSize`; the decoder MUST produce exactly `RawLength` bytes or the message is a
protocol error. Senders MUST send `RawLength = 0` (uncompressed) when compression does not shrink the
payload or the payload is shorter than `MinCompressSize` (default 64). A zero-length payload is always sent
uncompressed.

### 2.2 Packed container (channel 1)

```
0x01
Flags  : u8       bit0 = Tick present
Tick   : varint   when Flags.bit0 — the sender's flush tick for every message in this container
repeat: Length varint (≥ 1), Message (a complete §2.1 or §2.3 frame including its ChannelId)
```

The inner `ChannelId` MUST be 0 or ≥ 2 (containers do not nest); `Length` MUST not exceed the remaining
bytes. Control frames inside a container count against the same control-rate limits as loose ones. The
scheduler packs buffered datagram messages for the same peer into one container whenever at least two fit;
the cost is 2 bytes plus one length varint per message.

### 2.3 Control datagram (channel 0)

```
0x00
Type : u8
Body
```

| Type | Name | Body |
|---|---|---|
| 0x01 | Ping | `t u32 LE` — sender's monotonic micros relative to the sender's connection start, truncated |
| 0x02 | Pong | `t u32 LE` (echoed), `recv u32 LE`, `send u32 LE` — responder's relative micros at receipt and at send |
| 0x03 | LatestAck | `count varint`, repeat `channel varint, key varint, version u32 LE` — cumulative: the highest version accepted for that key |
| 0x04 | LatestReject | `count varint`, repeat `channel varint, key varint, version u32 LE, reason u8` — the receiver dropped this version locally (1 ring full, 2 too large, 3 decode error, 4 key table full); the sender MAY retry after a back-off |
| 0x05 | BulkProgress | `transferId varint, bytesAccepted varint` |
| 0x06–0x0F | reserved | (former Receipt/GroupReceipt/Applied ids; reserved for later versions) |

Control datagrams are sent with the transport's high-priority datagram flag so they never queue behind
application datagrams. Pong is sent for at most 4 Pings per second per peer; excess Pings are ignored and
counted. Acks are coalesced: at most one LatestAck/LatestReject datagram per peer per `AckDelay` (default 5 ms) or per Poll,
de-duplicated by (channel, key) keeping the highest version. BulkProgress is sent at most every 64 KiB or
100 ms per transfer, and on completion. Every control datagram type MAY also be carried as a control-stream
message (§3.4) when a datagram cannot be sent.

## 3. Stream frames

Each QUIC stream begins with a **preamble** whose first varint is the channel id. The channel's mode from the
table decides how the rest of the stream is parsed. A stream that violates the table below MUST be reset
(RESET_STREAM / STOP_SENDING with `UnsupportedChannel`, §6) and counted; a duplicate persistent stream or a
second control stream is a connection-level `ProtocolViolation`.

| Stream kind | Direction | Allowed preamble channel |
|---|---|---|
| bidirectional | client → server, exactly one | 0 (control) |
| bidirectional | server → client | none (rejected) |
| unidirectional | either | `ReliableOrdered` (§3.1, at most one per channel per direction), `ReliableUnordered` (§3.2), `ReliableLatest` (§3.2, large values only), `Bulk` (§3.3) |
| unidirectional | either | 1, unknown ids, `Unreliable*` channels: rejected |

### 3.1 Persistent ordered stream (mode `ReliableOrdered`)

One unidirectional stream per channel per direction, opened lazily on first send and kept open for the life
of the epoch.

```
Preamble : ChannelId varint
repeat:
  Length    : varint    payload length (excludes the fields below)
  Key       : varint    when Keyed
  RequestId : varint    when RequestResponse — 0 = plain message; odd = request; even ≥ 2 = response to RequestId−1
  RawLength : varint    when Compression ≠ 0 (0 = not compressed)
  Payload   : Length bytes
```

Message boundaries are never implied by QUIC write boundaries; the receiver parses incrementally and a
message may span any number of receive callbacks. A malformed frame on a persistent stream cannot be
resynchronised and is a connection-level `ProtocolViolation`.

### 3.2 Group stream (mode `ReliableUnordered`, and large `ReliableLatest` values)

One unidirectional stream per *group* (all messages of that channel admitted between two flushes, bounded by
`GroupMaxBytes`, default 64 KiB), closed with FIN after the last message. Groups are independent: loss in
one group never delays another. Groups are opened lazily; when the peer's stream credit is exhausted the
group waits (it never fails) and `GroupMinIntervalMicros` (default 1 000) bounds stream churn.

```
Preamble : ChannelId varint, GroupId varint
repeat: same message framing as §3.1 (without RequestId)
FIN
```

For `ReliableLatest` values larger than the datagram limit the group holds exactly one message with
`Key` and `Sequence` fields as in §2.1 (`GroupId` = the value's version); the sender aborts any still-open
stream for the same key when a newer version is submitted. A malformed group stream is reset with
`ProtocolViolation` and its messages are dropped; the connection survives.

### 3.3 Bulk stream (mode `Bulk`)

```
Preamble : ChannelId varint
Header   : TransferId varint, ObjectId varint, ObjectVersion varint, TotalLength varint,
           Offset varint, Length varint, Flags u8 (bit0 hash present, bit1 chunked, bits 2-3 hash algorithm: 0 = SHA-256),
           Hash 32 bytes when bit0
Body     : raw bytes                                                  when bit1 = 0
           repeat: ChunkLength varint, RawLength varint, bytes        when bit1 = 1 (each chunk independently LZ4-decodable; both lengths ≤ BulkMaxChunk, default 1 MiB)
FIN
```

Validation before any state is created: `Offset + Length ≤ TotalLength`, all three ≤ 2^62−1, `Length > 0`,
`TransferId` unique per (peer, direction), and — for a peer-initiated bulk stream — the application's
receive router MUST accept the descriptor (default: reject). `TotalLength`, `Offset` and `Length` are
untrusted: buffers grow as bytes arrive, never at the declared size. The hash covers the **whole object**
(identity = `ObjectId` + `ObjectVersion`), is computed once per object version by the sender, and is verified
by the receiver when the last byte of the object has arrived across all transfers. TLS already protects the
wire; the hash exists for cross-transfer identity and cache integrity. Resume = a new transfer for the
remaining `[Offset, Offset+Length)` range with the same `ObjectId`/`ObjectVersion`.

### 3.4 Control stream (channel 0)

A single bidirectional stream opened by the client immediately after the QUIC handshake. Preamble `0x00`,
then framed control messages:

```
Length : varint   in [1, 16384]
Type   : u8
Body   : Length − 1 bytes
```

| Type | Name | Direction | Body |
|---|---|---|---|
| 0x10 | Hello | C→S | `magic "QLCY"`, `version u16 LE (=1)`, `flags u16 LE`, `tableHash u64 LE`, `lastEpoch u32 LE (informational, 0 = fresh)`, `sessionToken (len varint + bytes, ≤ 4096)`, `authToken (len varint + bytes, ≤ 4096)`, `maxReceiveDatagram u16 LE`, `caps u16 LE` |
| 0x11 | HelloAck | S→C | `status u8 (0 = accepted)`, `sessionId u64 LE`, `epoch u32 LE`, `maxReceiveDatagram u16 LE`, `caps u16 LE`, `maxMessageSize varint`, `heartbeatMicros varint`, `graceMicros varint`, `sessionToken (len varint + bytes)`, `tableIncluded u8` [+ canonical table + names: per channel `name len varint (≤ 64) + utf8`], `reason (len varint + utf8, ≤ 512)` |

`maxReceiveDatagram` is an *application-level* receive cap (the largest datagram payload the endpoint will
process; 0 = no cap beyond the transport's). The size a datagram may actually have is dynamic: QUIC
negotiates `max_datagram_frame_size` and path MTU discovery grows and shrinks the usable payload during the
session, so senders read the transport's current maximum at pack time and never cache it from the handshake.
| 0x12 | Close | both | `code u32 LE`, `reason (len varint + utf8, ≤ 512)` |
| 0x13 | BulkRequest | both | `requestId varint, channel varint, objectId varint, objectVersion varint, offset varint, length varint` |
| 0x14 | BulkCancel | both | `transferId varint, code u32 LE` |
| 0x15 | BulkReject | both | `requestId varint, code u32 LE` |
| 0x16 | ChannelTableRequest | C→S | (empty) — server answers with a HelloAck-shaped table (status 0xFF = informational) |
| 0x17 | KeyRetired | both | `channel varint, key varint` — the sender will not use this key again in this epoch; the receiver frees its per-key state and reports `KeyRetired` to the application |
| 0x01–0x05 | as §2.3 | both | control-datagram fallbacks |

`flags` bit0 = request the channel table in the ack. `caps` bit0 = datagrams supported, bit1 = datagram
send-state (transport loss/ack) available, bit2 = LZ4. Datagrams are **required**: if either side lacks
them and the table contains any channel of mode 0, 1 or 4 the server rejects with status 7.

Handshake rules: the client MUST send Hello as the first frame within `AdmissionTimeout` (default 5 s) or the
server closes the connection; at most one Hello per connection (a second is a `ProtocolViolation`); any
application datagram or stream received before HelloAck (accepted) is dropped (datagram) or reset (stream)
and counted; the server keeps `PeerUnidiStreamCount = 0` and `PeerBidiStreamCount = 1` until it has accepted
the Hello, then raises them.

`HelloAck.status`: 0 accepted; 1 version mismatch; 2 channel table mismatch; 3 rejected (covers bad or
expired auth token, bad or expired session token, admission policy — never distinguished to the peer);
4 server full; 5 reserved; 6 internal error; 7 datagrams required. The effective maximum message size is
`min(channel.MaxMessageSize, HelloAck.maxMessageSize)`.

Control-message bounds (clarifications; they apply to both endpoints, and a violation is a malformed frame:
`ProtocolViolation` on the control stream, drop + count for a control datagram):

* The HelloAck table section (present when `tableIncluded` = 1; a `tableIncluded` other than 0 or 1 is
  malformed) is the §1 canonical table encoding followed by exactly `count` names in the same ascending-id
  order, each `len varint (≤ 64) + utf8`. It has no length prefix: the receiver finds the `reason` that follows
  by walking that structure. Channel ids in the section MUST be in [2, 16383].
* `HelloAck.sessionToken` is bounded like Hello's tokens: ≤ 4096 bytes. `HelloAck.status` values other than
  0–4, 6, 7 and 0xFF are malformed.
* Hello with `version` ≠ 1: only `magic` and `version` are interpreted (a later version may lay out the rest
  differently) and the server answers status 1. A wrong `magic` is malformed.
* Every `channel` field of a control message (LatestAck/LatestReject entries, BulkRequest, KeyRetired) MUST be
  in [2, 16383]; whether that channel exists and has the right mode is the session layer's check.
* LatestAck/LatestReject: `count` MUST NOT exceed the remaining body length divided by the smallest entry
  (6 bytes, 7 with the reason), checked before any entry is read; the whole batch is validated before any entry
  is applied, so a malformed batch is dropped as a unit. `count` = 0 is well-formed (senders never send it).
  `LatestReject.reason` values other than 1–4 are malformed.
* BulkRequest: `offset + length` MUST NOT exceed 2^62−1 (range semantics such as `length > 0` belong to the bulk
  engine). `code` fields (Close, BulkCancel, BulkReject) accept any u32.
* Types 0x10–0x17 inside a control datagram, and undefined types (0x00, 0x06–0x0F, 0x18–0xFF) anywhere, are
  malformed. Channel 0 written as a non-minimal varint (`0x40 0x00`) is malformed, not another channel.
* Before logging a peer-supplied `reason` or channel name, invalid UTF-8 sequences and every character of
  Unicode category Cc, Cf (including bidirectional overrides), Zl and Zp are replaced with U+FFFD.

## 4. Session semantics

### 4.1 Sessions, tokens, epochs

* `sessionId` is a random 64-bit value chosen by the server; it is an identifier, never a secret.
* `sessionToken` is minted only by the server. Layout (69 bytes, integers little-endian):
  `version u8 (= 1) ‖ sessionId u64 ‖ epoch u32 ‖ expiry i64 ‖ random (16 bytes) ‖ HMAC-SHA256(serverKey, all
  37 preceding bytes)`. `sessionId` and `epoch` travel in clear (neither is a secret) so the server can verify a
  token without a lookup; `expiry` is absolute microseconds on the server's clock (issue time + `graceMicros`);
  the HMAC makes every field tamper-evident. Clients treat the token as opaque bytes. *(Clarification: the
  earlier layout "16 random bytes ‖ HMAC ‖ expiry, ≥ 56 bytes" did not carry `sessionId`/`epoch`, so the server
  could not recompute the MAC without already knowing the session.)* It is rotated in every HelloAck, single-use
  (an old token is invalid once a resume succeeded or a newer token was issued), expires after `graceMicros`,
  and is a *locator*, not a credential: a resume MUST also present an `authToken` that the admission policy
  accepts. Token comparison is constant-time; failed auth attempts are rate-limited per remote address.
  Implementation policy: single use is enforced by a bounded replay cache of the random parts, each entry kept
  until its token expires; when the cache is full of unexpired entries a presented token is rejected (fail
  closed: status 3, the client starts a fresh session) rather than an entry evicted. "A newer token was issued"
  is enforced by the session registry comparing the token's `epoch` with the session's current epoch. Key
  rotation keeps accepting the previous key until a deadline (typically `graceMicros` after the rotation). The
  per-address failure limiter keys IPv4 (and IPv4-mapped IPv6) by address and IPv6 by /64 prefix; addresses its
  bounded table cannot track share one global overflow bucket.
* `epoch` is allocated by the server, strictly increasing per `sessionId`, starting at 1; the client's
  `lastEpoch` is informational. Every sequence/version/counter is scoped to the current epoch.
* A valid resume for a session that still has a live connection replaces that connection (the old one is
  closed with `SessionReplaced`). A token presented after the grace period is rejected (status 3), never
  silently turned into a fresh session.
* A resumed session (new epoch): `UnreliableSequenced` tables reset; every live `ReliableLatest` key is
  re-queued at its current version (a free full-state resync); in-flight `ReliableOrdered` /
  `ReliableUnordered` sends complete `Disconnected` and are the application's responsibility; Bulk transfers
  marked `Resumable` are re-requested by the library for the remaining range.

### 4.2 Early data

QUICLY frames are never sent in 0-RTT: clients never set `ALLOW_0_RTT`; servers run
`QUIC_SERVER_NO_RESUME` by default (at most `RESUME_ONLY`, opt-in, with documented ticket-key rotation);
any stream or datagram carrying the 0-RTT receive flag is a `ProtocolViolation`; TLS resumption application
data is never used for authentication. Reconnect = new TLS handshake + QUICLY session token.

### 4.3 Delivery guarantees and completion

| Mode | `BufferReleased` | `Delivered` | Other terminal states |
|---|---|---|---|
| Unreliable* | datagram handed to the network (transport SENT) | transport ack when the transport reports datagram send state (caps bit1); otherwise never | `Lost`, `Expired`, `Canceled` (dropped when blocked) |
| ReliableOrdered / ReliableUnordered | stream bytes acknowledged by the peer's QUIC stack | same event | `Disconnected`, `Failed` |
| ReliableLatest | as above per transmission | `LatestAck` covering the version (cumulative) | `Superseded`, `Failed`, `Disconnected` |
| Bulk | per chunk | transfer complete (`BulkProgress` = Length) | `Canceled`, `Failed` |

A QUIC acknowledgement proves delivery to the peer's transport, not that the peer application processed
the message; the only application-level acknowledgement in v1 is the response of a `RequestResponse`
channel (an empty response is an "applied" ack).

### 4.4 ReliableLatest retransmission

* Trigger: when the transport reports datagram send state, resend on `Lost` immediately (subject to the
  budget below) and treat `Acknowledged` as delivered-to-transport (the `LatestAck` still decides
  `Delivered`). A timer is the backstop: resend when `now − lastSend ≥ clamp(1.5 × RTT, MinRetry = 20 ms,
  MaxRetry = 1 s)`, doubling up to `MaxRetry`.
* Budget: at most 16 transmissions or 30 s per version, whichever first → `Failed`; an aggregate per-peer
  retry budget (default 10 % of the estimated bandwidth) scheduled *after* fresh sends.
* Receiver: a version newer than the accepted one is accepted and acked; an older or duplicate version
  triggers a re-ack of the current version (so a lost ack cannot stall completion); `LatestReject` reports
  local drops. Acks are cumulative per key: `LatestAck(v)` covers every `v' ≤ v` in serial arithmetic.

### 4.5 Scheduling

* Buffered sends are transmitted only by `Flush()` (or when a container/group fills, or together with an
  `Immediate` send for the same peer). `AutoFlushInterval` is 0 by default; hosts without a tick MAY set a
  timer, which then acts as the game thread.
* `Immediate` = eligible for transmission now, together with whatever is already buffered for that peer; it
  bypasses only the batching delay. Congestion control and pacing still apply.
* Expiry is evaluated at scheduling time, never after hand-off. Unreliable datagrams are submitted with
  cancel-on-blocked semantics (`DropWhenBlocked`, default on): a datagram that cannot be sent immediately
  because of congestion is dropped and counted as `Expired`. Default `Expiry` is 0 (none) for reliable
  channels and 2× the flush interval for `UnreliableSequenced`.
* Order: by channel `Priority` (high first), then admission order. Retries and Bulk are scheduled after fresh
  real-time traffic; Bulk is capped by `BulkMaxBytesPerSecond` / `BulkShareOfEstimatedBandwidth` (default
  50 %) and by `IdealSendBufferSize` from the transport.
* Priority mapping (MsQuic): channel priority → stream priority band (`Priority × 257`); priority ≥ 192 or
  `Immediate` datagrams → `DGRAM_PRIORITY`; Bulk streams → band 0.

### 4.6 Time synchronisation

Ping every 100 ms for the first 3 s of a session, then every `PingInterval` (default 1 s). RTT samples
feed a smoothed RTT and RTT variance; the clock offset published to the application is the sample with the
minimum RTT over the last 8, slewed (not stepped) after the initial lock. The offset is advisory (peer
supplied) and is exposed as `EstimatedRemoteMicros()`; one-way jitter is estimated from consecutive Pong
timestamps.

### 4.7 No cross-channel ordering

Ordering exists only inside one channel and direction. A spawn event on a `ReliableOrdered` channel and the
entity's first state update on an `UnreliableSequenced` channel have no relative order; applications MUST be
prepared to receive state for unknown keys (buffer for one RTT, or carry spawn data on a `ReliableLatest`
channel with the same key). The Replication package's entity generations are the documented answer.

## 5. HTTP/3 / WebTransport mapping (ALPN `h3`, opt-in)

* Enabled only by `ServerOptions.EnableHttp3`; the listener uses a separate MsQuic configuration for the
  `h3` ALPN (HTTP/3-sized stream limits) chosen from `NegotiatedAlpn` in the new-connection callback.
* Server SETTINGS: `QPACK_MAX_TABLE_CAPACITY=0`, `QPACK_BLOCKED_STREAMS=0`, `MAX_FIELD_SECTION_SIZE=16384`,
  `ENABLE_CONNECT_PROTOCOL=1`, `H3_DATAGRAM (0x33)=1`, `ENABLE_WEBTRANSPORT (0x2b603742)=1`,
  `WT_MAX_SESSIONS (0xc671706a)=1`.
* Limits: ≤ 16 concurrent request streams, capsules ≤ 32 KiB, request timeout 10 s, GOAWAY on shutdown,
  unknown frame types ignored (RFC 9114), streams/datagrams for an unknown session id are reset/dropped
  immediately (never buffered).
* Session establishment: Extended CONNECT (`:method=CONNECT`, `:protocol=webtransport`, `:scheme=https`,
  `:authority`, `:path` = `ServerOptions.WebTransportPath` (default `/quicly`), `origin` — required unless
  `AllowMissingOrigin`) → `:status 200`. The CONNECT stream stays open for the session.
* Datagrams: RFC 9297 — `quarterStreamId varint` (= CONNECT stream id / 4) prefix, then the QUICLY frame.
* Unidirectional streams: `0x54 varint`, `sessionId varint`, then the QUICLY stream. Bidirectional streams:
  `0x41 varint`, `sessionId varint`.
* Session close: `CLOSE_WEBTRANSPORT_SESSION` capsule (0x2843) on the CONNECT stream, or QUIC close.
* Plain HTTP/3 GET on the same listener is answered by the HTTP handler only when `ServerOptions.Http3Static`
  is configured (root-jailed, MIME allow-list, no listing, max file size).

## 6. Errors

Application error codes (QUIC CONNECTION_CLOSE / RESET_STREAM / STOP_SENDING and `Close.code`):

| Code | Name | Used for |
|---|---|---|
| 0x00 | NoError | orderly close |
| 0x01 | ProtocolViolation | malformed control frame, malformed message on a persistent ordered stream, second Hello, 0-RTT data, non-minimal varint |
| 0x02 | LimitExceeded | a §7 limit was hit where the rule says "close" |
| 0x03 | UnsupportedChannel | stream preamble names a channel that cannot be carried on a stream, or an unknown channel |
| 0x04 | AdmissionRejected | HelloAck status ≠ 0 |
| 0x05 | Timeout | admission timeout, heartbeat timeout |
| 0x06 | SessionReplaced | a resume replaced this connection |
| 0x07 | InternalError | implementation fault |
| 0x10 | BulkCanceled | transfer cancelled by either side |
| 0x11 | BulkRejected | bulk request not authorised / invalid range |

Rules: a malformed frame on the control stream or a persistent ordered stream → connection close
`ProtocolViolation`; a malformed group or bulk stream → RESET_STREAM/STOP_SENDING with the code, the
group/transfer is marked `Failed`, the connection survives; a datagram message rejected for size, limits or
decode failure → dropped and counted (and `LatestReject` for ReliableLatest); after sending or receiving
`Close` all further QUICLY frames are ignored and the QUIC connection is closed with the same code.
Peer-supplied `reason` strings are ≤ 512 bytes of valid UTF-8; control characters are replaced before
logging.

## 7. Receive-side limits (defaults)

| Limit | Default | On violation |
|---|---|---|
| per-peer receive byte budget (pooled leases + reassembly + stream staging) | 256 KiB | datagram channels: drop newest + count; stream channels: stop consuming (transport back-pressure), resume from Poll |
| keys per channel per peer (`MaxKeys`) | 4 096 | UnreliableSequenced: evict least-recently-updated key (evicted keys re-accept any sequence; replay window documented); ReliableLatest: reject with `LatestReject(4)`, never evict |
| concurrent peer streams per channel (`MaxGroups`) | 8 (ReliableUnordered), 4 (large ReliableLatest), 2 (Bulk) | further streams are reset `LimitExceeded` |
| stream idle mid-message | 30 s | stream reset `Timeout` |
| concurrent reassemblies per channel (`MaxReassemblies`) | 16 | evict oldest; reassembly expiry 2 × RTT + 100 ms; a newer sequence for the same key abandons the older partial |
| fragmented message size | ≤ 255 × (maxDatagram − header) and ≤ `MaxMessageSize` | drop before any buffer is chosen |
| control messages per second | 200 | connection close `LimitExceeded` |
| decoded (decompressed) bytes per second per peer | 8 MiB/s | further compressed messages dropped + counted |
| bulk transfers per direction per peer | 2 (+ 1 pending request) | `BulkReject` |
| receive ring depth per peer | 4 096 entries | see byte budget row; latest/coalescing channels use per-key mailboxes instead of ring entries |

Decompression runs on the game thread inside `Poll` (never on a transport thread); compressed messages are
staged compressed in a pooled lease. Every limit is configurable per channel or per peer, and every violation
is a counter in the peer's statistics.
