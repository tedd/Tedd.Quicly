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

There is no limit on the number of messages in a container other than the datagram's own length (about 599
two-byte messages fit 1 200 bytes): a receiver MUST accept as many messages as the datagram actually holds and
MUST NOT reject a container for its message count. A sender MAY pack fewer — the reference implementation packs
at most 64 messages per container (`PackedContainer.MaxMessages`), a send-side choice only.

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
application datagrams. Pong is sent for at most 4 Pings per second per peer, with a burst allowance of 32
(`PeerOptions.PongsPerSecond` / `PongBurst`) so that the peer's 10 Hz fast lock over the first 3 s of a session
(§4.6) is answered in full; excess Pings are ignored and counted. Acks are coalesced: at most one LatestAck/LatestReject transmission per peer per `AckDelay`, which MAY span several datagrams (at most 8) when one datagram cannot carry every pending key (default 5 ms) or per Poll,
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
| 0x14 | BulkCancel | R→S | `transferId varint, code u32 LE` — the id of a transfer the **recipient is sending** |
| 0x15 | BulkReject | both | `requestId varint, code u32 LE` |
| 0x16 | ChannelTableRequest | C→S | (empty) — server answers with a HelloAck-shaped table (status 0xFF = informational) |
| 0x17 | KeyRetired | both | `channel varint, key varint` — the sender will not use this key again in this epoch; the receiver frees its per-key state and reports `KeyRetired` to the application |
| 0x01–0x05 | as §2.3 | both | control-datagram fallbacks |

`BulkCancel` travels **receiver to sender only**. It means "stop sending the transfer you are sending to me", so it always
names a transfer of the *recipient's* own send side, and that is what the recipient resolves it against. A `TransferId` is
unique per (peer, **direction**) (§3.3), so id 1 exists in both directions of one session and the frame carries no
direction field: one sent the other way would name the peer's unrelated transfer and cancel it. A **sender** that abandons
a transfer therefore signals on the wire by resetting its stream with `BulkCanceled` (§6) — which the receiver turns into
a cancelled transfer through that stream's single close notice, and which is also what makes the receiver's own
`BulkCancel` complete: a `BulkCancel` alone would leave the peer waiting for bytes that never come. A sender that
abandons a *peer-requested* transfer before any stream exists answers `BulkReject` (0x15) instead. A `BulkCancel` naming a
transfer the recipient is not sending is ignored and counted.

A `BulkRequest` is answered either by `BulkReject` carrying its `requestId`, or by a bulk stream on the named channel whose
`ObjectId`, `ObjectVersion` and `Offset` are the request's. The bulk header carries no request id, so the requester matches
the answer on those fields; the server MAY shorten `Length`, never move `Offset`, and the requester then asks again for the
rest.

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
* The whole HelloAck, table section included, is one control message, so `Length` ≤ 16384 bounds it too. The
  table section gets what the other fields leave: 15 756 bytes with a 69-byte session token, a 512-byte reason
  and worst-case 8-byte varints (roughly 200 channels with 64-byte names, a few thousand with short ones).
  *(Clarification: version 1 has no multi-frame table response. A server MUST NOT be configured with a channel
  table whose section exceeds that budget; implementations reject such a table at configuration time.)*
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
* BulkProgress: `bytesAccepted` MUST NOT exceed the bytes of that transfer the recipient has handed to its transport — a
  peer cannot have accepted bytes that were never sent, so a larger value is malformed (and it confirms nothing: a transfer
  is `Delivered` only once the recipient's own sends of it have completed as well, §4.3). A `BulkProgress` naming a
  transfer the recipient is not sending — typically a late frame for one that already finished — is ignored.
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
  token without a lookup; `expiry` is absolute microseconds on the server's clock: the token's *maximum age*
  (issue time + the server's token lifetime, 24 h in the reference implementation), not the grace period;
  the HMAC makes every field tamper-evident. Clients treat the token as opaque bytes. *(Clarification: the
  earlier layout "16 random bytes ‖ HMAC ‖ expiry, ≥ 56 bytes" did not carry `sessionId`/`epoch`, so the server
  could not recompute the MAC without already knowing the session.)* It is rotated in every HelloAck, single-use
  (an old token is invalid once a resume succeeded or a newer token was issued), expires at its `expiry`,
  and is a *locator*, not a credential: a resume MUST also present an `authToken` that the admission policy
  accepts. Token comparison is constant-time; failed auth attempts are rate-limited per remote address.
  Implementation policy: the **session registry is the authority** on single use. It compares the token's
  `epoch` with the session's current epoch, and a resume that commits advances that epoch, so a replayed token
  can no longer match its session and is rejected (status 3) whatever any cache holds. A bounded replay cache
  of spent tokens is a secondary guard that **never fails closed**: an entry is kept for the grace period —
  the window in which a replayed token could still match a live record — not for the token's maximum age, and
  when the cache is full its **oldest entry is evicted** and the new token stored rather than a resume being
  refused, because refusing would let one authenticated client (one resume per minimum resume interval) fill
  the cache and lock every other client out. Evictions are counted and reported in the server's statistics, so
  a cache under pressure is visible and can be sized up. How long a
  session outlives its connection is the registry's decision, not the token's: a *live* session can be resumed at
  any time within its token's maximum age, and a session whose connection was lost only within `graceMicros` of
  that loss (the server starts the grace period when the connection is lost, not when the token was issued). A
  resume refused because the session resumed too often (a per-session rate with a small burst) is answered
  status 3, leaves the token usable and is not charged to the per-address failure limiter. Key rotation keeps
  accepting the previous key until a deadline (typically one token maximum age after the rotation). The
  server inspects a presented token at admission and consumes it only when the resume commits, so a resume
  refused for another reason (status 2, 4 or 7) leaves the token usable. The per-address failure limiter keys
  IPv4 (and IPv4-mapped IPv6) by address and IPv6 by /64 prefix. It only ever refuses addresses it is tracking:
  an address with no recorded failures is always admitted, so failures from other addresses never lock a client
  out. When its bounded table has no room for a newly failing address, the tracked entry closest to fully
  refilled is evicted (blocked addresses are evicted last).
* `epoch` is allocated by the server, strictly increasing per `sessionId`, starting at 1; the client's
  `lastEpoch` is informational. Every sequence/version/counter is scoped to the current epoch.
* A valid resume for a session that still has a live connection replaces that connection (the old one is
  closed with `SessionReplaced`), unless that connection is already closing deliberately (a goodbye, or the
  server kicking the player): such a session ends with that close, and a resume racing it is rejected
  (status 3). A resume after the session's grace period, counted from the loss of its connection, is rejected
  (status 3), never silently turned into a fresh session.
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
| Unreliable* | datagram handed to the network (transport SENT) | transport ack when the transport reports datagram send state (caps bit1); otherwise **never** (the send completes `Sent`) | `Sent`, `Lost`, `Expired`, `Canceled` (dropped when blocked) |
| ReliableOrdered / ReliableUnordered | stream bytes acknowledged by the peer's QUIC stack | same event | `Disconnected`, `Failed` |
| ReliableLatest | as above per transmission | `LatestAck` covering the version (cumulative) | `Superseded`, `Failed`, `Disconnected` |
| Bulk | per chunk | transfer complete (`BulkProgress` = Length) | `Canceled`, `Failed` |

A QUIC acknowledgement proves delivery to the peer's transport, not that the peer application processed
the message; the only application-level acknowledgement in v1 is the response of a `RequestResponse`
channel (an empty response is an "applied" ack).

`Sent` is the terminal outcome of an unreliable send on a carrier that reports no per-datagram send state — a
WebTransport/browser carrier, or MsQuic without the capability (§3.4 `caps` bit1). Such a carrier tells the
session only that the datagram left the host, which proves nothing about delivery, so the send completes `Sent`
and an implementation MUST NOT report `Delivered` for it. `Sent` is final: the payload is released and the send
slot is freed at the same event, because no further state will ever be reported.

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
* Buffered datagrams are **not** handed over with a delay-send hint (MsQuic's `DELAY_SEND`). Measured on
  loopback, setting it made a tick's burst 16–19 % slower per datagram, and a later re-run found no measurable
  effect in either direction; batching is achieved by packing (§2.2), so the flag is never set
  (`docs/benchmarks/msquic-transport.md`, `docs/benchmarks/session.md`).
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
| control messages per second | 2 000 (per peer, configurable; size it from the channel table, because acks scale with keyed `ReliableLatest` traffic and one ack datagram carries about 170 keys) | connection close `LimitExceeded` |
| decoded (decompressed) bytes per second per peer | 8 MiB/s | further compressed messages dropped + counted |
| bulk transfers per direction per peer | 2 transfers, and an outbound range-request table of one more than that (3), so a further range can be asked for while both transfers run | `BulkReject` |
| receive ring depth per peer | 4 096 entries | see byte budget row; latest/coalescing channels use per-key mailboxes instead of ring entries |

The **stream idle mid-message** rule is per receiving stream and applies to every stream mode: a stream that has
delivered a message's frame header but not the rest of its payload for 30 s (`PeerOptions.StreamIdleTimeout`) is
reset with `Timeout` (RESET_STREAM / STOP_SENDING), which releases that message's staging lease and its
receive-ring slot; the connection survives, and other streams of the same channel are unaffected. Without it a
peer that starts one message and stops can pin the whole per-peer receive byte budget for the life of the
connection, and the connection-level heartbeat does not notice because the peer stays live on other channels.
Progress on the stream (any accepted frame event) restarts the 30 s, and a stream between messages is never
watched.

Decompression runs on the game thread inside `Poll` (never on a transport thread); compressed messages are
staged compressed in a pooled lease. Every limit is configurable per channel or per peer, and every violation
is a counter in the peer's statistics.

## 8. Clarifications (decided by the reference implementation)

Where the sections above leave a choice open, `Tedd.Quicly.Core` (Channels/Framing) decides as follows. Every
rule below is enforced by the receiver; a violation is a malformed frame with the consequence given in §6.
Byte-exact examples are in [protocol-vectors.md](protocol-vectors.md).

* **§1 table section.** `flags` bit 7 is reserved and MUST be 0; compression codecs 2 and 3 are rejected in a v1
  table; `maxMessageSize` is 1 … 16 MiB; names are valid UTF-8 (a name of length 0 is allowed). `count` is at most
  16 382 and ids are strictly ascending. A name length is a minimal varint, so a 64-byte name takes the two-byte length
  `0x40 0x40`.
* **§1 / §3.4 table size.** The table section travels inside the HelloAck (or the ChannelTableRequest answer), whose
  body is at most 16 383 bytes. With the other HelloAck fields at their worst case (42 bytes of fixed fields and
  varints, a session token of up to 4 096 bytes, a reason of up to 512 bytes) a table section of at most
  **11 729 bytes** always fits; each channel costs 6 … 75 bytes (≈ 1 950 unnamed channels). A server whose table
  section does not fit the frame MUST send `tableIncluded = 0` instead of an oversized frame; implementations should
  check the section length (`ChannelTableCodec.GetLengthWithNames`) against this budget when the table is built.
* **§1 defaults.** A channel's local `ExpiryMicros` default is 0 (none) for every mode except
  `UnreliableSequenced` (2 × the flush interval, §4.5), including `UnreliableUnordered`. `MaxGroups` defaults to 1
  for `ReliableOrdered` and 0 for datagram-only modes.
* **§2.1 fragments.** Every fragment carries at least one payload byte, and the last fragment carries between 1
  and size(fragment 0) bytes. A receiver can therefore bound the total from any single fragment: a non-last
  fragment of size *s* implies a total ≥ *s* × (FragCount − 1) + 1, the last fragment of size *l* implies a total
  ≥ *l* × FragCount; if that bound exceeds the effective `MaxMessageSize` (uncompressed), the fragment is dropped.
* **§2.1 / §3.1 compression.** A compressed payload (`RawLength > 0`) is non-empty and strictly shorter than
  `RawLength` (for fragments: the bound above is strictly below `RawLength`); anything else is malformed.
* **§2.2 containers.** `Flags` bits 1–7 are reserved and MUST be 0; `Tick` is ≤ 2^32 − 1; a container holds at
  least one message; an inner frame whose first byte is `0x01` is a nested container. An inner frame that encodes
  channel 1 non-minimally is rejected by the inner parse (non-minimal varint). The message count is bounded only
  by the datagram's length: the parser accepts every message that fits, while the packer writes at most
  `PackedContainer.MaxMessages` = 64 per container.
* **§3 control stream.** Both directions of the control stream begin with the preamble `0x00`. A unidirectional
  stream whose preamble names channel 0 is rejected like channel 1 (`UnsupportedChannel`).
* **§3.1 request ids.** `RequestId` is ≤ 2^32 − 1.
* **§3.2 large ReliableLatest values.** The group stream's `GroupId` is the value's 32-bit version (≤ 2^32 − 1) and
  its single message frame is `Length varint, Sequence u32 LE, Key varint, RawLength varint (when Compression ≠ 0),
  Payload` — the §3.1 framing with the §2.1 `Sequence` field inserted after `Length`. `Sequence` MUST equal
  `GroupId`; a second message on the stream is malformed.
* **§3.3 bulk.** `MaxMessageSize` of a Bulk channel bounds `Length` of one transfer (the range one stream carries),
  not `TotalLength`. `Flags` bits 4–7 MUST be 0 and the hash-algorithm bits 2–3 MUST be 0 (SHA-256; the others are
  rejected, whether or not bit 0 is set). An unchunked body is exactly `Length` bytes. In a chunked body
  `ChunkLength ≥ 1`; `RawLength = 0` means the chunk is stored uncompressed, otherwise `ChunkLength < RawLength`;
  the decoded sizes of all chunks sum to exactly `Length`. Bytes after the body are malformed. The session cap
  `HelloAck.maxMessageSize` bounds message frames only (datagram messages, ordered and group stream frames); it does
  not apply to Bulk transfers, whose `Length` is bounded by the Bulk channel's own `MaxMessageSize` alone.
* **§3.3 transfer ids.** A receiver enforces the uniqueness of `TransferId` per (peer, direction) over every transfer it
  still holds state for — one that is running, and one that has finished but whose final `BulkProgress` has not gone out
  yet. Beyond that it MAY accept an id again within the epoch: remembering every id an epoch has seen would be unbounded
  state that the peer controls. A sender never reuses an id within an epoch (the reference implementation numbers its
  transfers upward from 1), so a reuse can only confuse the peer that made it.
