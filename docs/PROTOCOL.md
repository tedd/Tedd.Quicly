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
message for a reused key is always newer than anything the receiver remembers.

A receiver orders values by their **position on the channel**, not by comparing two wire sequences of one key:
it extends every arriving sequence (or `ReliableLatest` version) to 64 bits against the newest one it has seen
on the channel, *for any key*, and remembers each key's last accepted value as that extended number (§8
"sequence clock"). RFC 1982 serial arithmetic in the channel's width is used only for that one step, between an
arrival and the channel's newest, which are close on a live channel. A **key may therefore idle indefinitely**
while other keys of its channel are busy — 40 000 updates of one key between two updates of another on a 16-bit
channel are fine.

What remains is a limit on the channel as a whole: at most 2^(bits−1)−1 *consecutive messages of the channel
that do not arrive* (a blackout, or sends that expire or are canceled before transmission, which use up
numbers). Beyond it the next arrival reads as older than the newest. An `UnreliableSequenced` receiver then
resynchronises as soon as its newest sequence has not advanced for 2 s, provided the arrival is further behind
than a late message plausibly is (§8); a `ReliableLatest` receiver does
not (a retransmission is legitimately old), so the affected values end `Failed` until the counter is back
within range. A channel sending N messages per second with 16-bit sequences reaches the limit after a blackout
of 32767/N seconds (≈ 5 min at 100 msg/s, ≈ 33 s at 1 000 msg/s); use 32 bits when in doubt.

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
| 0x03 | LatestAck | `count varint`, repeat `channel varint, key varint, version u32 LE` — the highest version accepted for that key (highest on the channel's version clock, §8); the sender completes a value only on an entry naming exactly the version it last transmitted for the key (§4.4) |
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
           Offset varint, Length varint, Flags u8 (bit0 checksum trailer present, bit1 chunked, bits 2-7 reserved = 0)
Body     : raw bytes                                                  when bit1 = 0
           repeat: ChunkLength varint, RawLength varint, bytes        when bit1 = 1 (each chunk independently LZ4-decodable; both lengths ≤ BulkMaxChunk, default 1 MiB)
Trailer  : Checksum u64 LE                                            when bit0 — xxHash64 (seed 0) of this transfer's *decoded* range
FIN
```

Validation before any state is created: `Offset + Length ≤ TotalLength`, all three ≤ 2^62−1, `Length > 0`,
`TransferId` unique per (peer, direction), and — for a peer-initiated bulk stream — the application's
receive router MUST accept the descriptor (default: reject). `TotalLength`, `Offset` and `Length` are
untrusted: buffers grow as bytes arrive, never at the declared size. Resume = a new transfer for the
remaining `[Offset, Offset+Length)` range with the same `ObjectId`/`ObjectVersion`.

The **checksum trailer** covers exactly the bytes one transfer carries, decoded (before chunking and after
decompression), and follows the last body byte so that neither end has to know the digest before it has seen
the bytes: the sender hashes each piece as it reads it, the receiver as it writes it, and nothing is read
twice or buffered. A receiver whose digest does not match MUST fail **that transfer** with
`BulkChecksumFailed` (§6), MUST NOT report the range as fully accepted in `BulkProgress`, and SHOULD send
`BulkCancel` carrying that code so the sender can send the range again; the connection and the object
survive. The range is the unit of recovery (`BulkRequest` already asks for one), so it is also the unit of
detection: an object of a thousand transfers is verified by verifying each of them, and a bad byte costs one
range rather than the object.

This is **not** a wire-integrity check. TLS 1.3 is mandatory in QUIC (RFC 9001) and its AEAD tag already
discards and retransmits anything the wire corrupts. What the trailer covers is the path AEAD cannot see: the
endpoints themselves — framing and reassembly bugs, a wrong offset, an LZ4 decode that succeeds but is wrong,
memory corruption before encrypt or after decrypt, a partial object that rotted on disk between two epochs.
It is therefore a defence against bugs and hardware rather than against attackers, which is why the digest is
xxHash64 rather than a cryptographic hash — the peer supplies both the bytes and the digest, so collision
resistance would buy nothing, and 8 bytes at better than 10 GB/s costs far less than the alternative. Bit 0 is
per transfer, so a sender that omits it and a receiver that wants it still interoperate: a receiver checks
whatever arrives. Object *identity* is not a transport concern and has no field here; an application that
needs a content address carries one in its own manifest.

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
| Unreliable* | datagram handed to the network (transport SENT) | transport ack when the transport reports datagram send state (caps bit1); otherwise **never** (the send completes `Sent`) | `Sent`, `Lost`, `Expired` (expiry before hand-off, or dropped by the transport before transmission), `Canceled` (application cancel while queued), `Disconnected` |
| ReliableOrdered / ReliableUnordered | stream bytes acknowledged by the peer's QUIC stack | same event | `Disconnected`, `Failed` |
| ReliableLatest | as above per transmission | `LatestAck` naming the current version, which is the version last transmitted for the key (§4.4) | `Superseded`, `Failed`, `Disconnected` |
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
  local drops. "Newer" is measured on the channel's version clock (§1, §8), not between two versions of the
  key, so a key may idle while the channel's counter advances by 2^31 or more. The test is made when a
  datagram value arrives and, for a large value, both when its stream starts and again when it ends: a value
  overtaken by a newer version of its key while it was on its stream is dropped at its end and the newer
  version is re-acked.
* Sender: an ack names the highest version the receiver accepted for the key, which within an epoch is never
  above the version the sender last transmitted for it. The sender therefore completes a value `Delivered` only
  on a `LatestAck` entry whose version **equals** the version of its most recent transmission for the key, and
  only while that is still the key's current version. Every other entry completes nothing: the ack of a
  superseded value, of a closed epoch, a late duplicate of an earlier value's ack that arrives before the new
  value's first transmission, or the re-ack of an older version by a receiver that dropped the new one. No
  serial comparison is made, so no distance between two versions of a key can turn an old ack into a new one.

### 4.5 Scheduling

* Buffered sends are transmitted only by `Flush()` (or when a container/group fills, or together with an
  `Immediate` send for the same peer). `AutoFlushInterval` is 0 by default; hosts without a tick MAY set a
  timer, which then acts as the game thread.
* `Immediate` = eligible for transmission now, together with whatever is already buffered for that peer; it
  bypasses only the batching delay. Congestion control and pacing still apply.
* Expiry is evaluated at scheduling time, never after hand-off. A message's expiry measures **how long the
  scheduler has held it back**, not its age since the send call: the clock starts at the first scheduler pass
  after the message was admitted (the next `Flush`, or the pass an `Immediate` send runs), that pass never
  expires it, and a later pass drops it when more than its expiry has elapsed since the first one. What holds a
  message back across passes is the send cap, datagrams being unavailable, a stream that is blocked or still
  starting, or the group interval. A message that is sent in its first pass is therefore never expired, however
  long the host took between the send call and the `Flush` — a long frame, a slow server tick, or a peer that
  was not polled do not drop it. Time before the first pass is not counted; a host that needs a hard age limit
  from the send call cancels the message (`TryCancel`) instead. (Implementations that counted expiry from
  admission, against the clock stamp of their last pass, dropped every `UnreliableSequenced` message whenever
  that stamp was older than the expiry; the rule above replaces that and changes nothing on the wire.)
  Default `Expiry` is 0 (none) for reliable channels and 2× the flush interval for `UnreliableSequenced`.
* Unreliable datagrams are submitted with cancel-on-blocked semantics (`PeerOptions.DropWhenBlocked`, default
  on, and only on a transport that honours the flag): a datagram the transport cannot send immediately is
  dropped instead of queueing behind congestion. Its send completes `Expired`, and it is counted in
  `TransportCanceled` (per channel, one per message) and `DatagramsCanceled` (per peer, one per datagram) —
  **not** in the channel's `Expired` counter, which counts only messages that expired before hand-off. The
  messages stay counted in `Sent`: they were handed to the transport. A datagram the transport declares lost
  is counted the same way in `TransportLost` / `DatagramsLost` and completes `Lost`. Datagrams the transport
  cancels because the connection is closing complete `Disconnected` and are counted in neither. With
  `DropWhenBlocked` off, blocked datagrams wait in the transport's queue and are sent when it can send again;
  expiry is not evaluated after hand-off, so such a datagram goes out however old it has become. A packed
  container carries the flag only when every member is unreliable; `ReliableLatest` and control datagrams
  never carry it. A datagram without the flag can be **overtaken in the sender's transport queue** by a later
  one with the priority flag (an Immediate or high-priority member) while the link is busy, and then arrives
  after it by as long as the congestion lasts; for an `UnreliableSequenced` member that means "late", which the
  receiver's reorder window covers (§8 "Resynchronisation"). None of this is visible on the wire: the receiver cannot tell a datagram its sender's
  transport dropped from one the network lost.
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

Implemented by `Tedd.Quicly.Transport.MsQuic.WebTransport` (`WebTransportTransport`, `WebTransportConnector`,
`WebTransportListener`): the carrier is an `ITransport` over an inner raw-QUIC `ITransport`, so everything above it is
unchanged. A server is stood up with `WebTransportListener.CreateMsQuic` and a client with
`WebTransportConnector.CreateMsQuic`; `WebTransportOptions` holds the path, the origin policy and the limits below.
One listener serving both `quicly/1` and `h3` on the same UDP port (a separate MsQuic configuration chosen from
`NegotiatedAlpn`), `ServerOptions.EnableHttp3` and `ServerOptions.Http3Static` are not built yet — see STATUS.md.

* Both ends open the control stream (type `0x00`) with SETTINGS and the two QPACK streams (`0x02`, `0x03`), which
  stay empty because the dynamic table is off. A first control frame that is not SETTINGS, a second SETTINGS, or a
  request frame on the control stream closes the connection.
* Server SETTINGS: `QPACK_MAX_TABLE_CAPACITY=0`, `QPACK_BLOCKED_STREAMS=0`, `MAX_FIELD_SECTION_SIZE=16384`,
  `ENABLE_CONNECT_PROTOCOL=1`, `H3_DATAGRAM (0x33)=1`, `ENABLE_WEBTRANSPORT (0x2b603742)=1`,
  `WT_MAX_SESSIONS (0xc671706a)=1`.
* The session is usable only once the peer's SETTINGS have arrived *and* the Extended CONNECT is settled; a peer
  whose SETTINGS do not enable WebTransport and Extended CONNECT is refused with `H3_SETTINGS_ERROR`. A peer without
  `H3_DATAGRAM` leaves the session with streams only, which Core hears as the datagram capability going away.
* Limits: ≤ 16 concurrent request streams, capsules ≤ 32 KiB, GOAWAY on shutdown, unknown frame types,
  stream types and capsules ignored (RFC 9114 §9), streams/datagrams for an unknown session id are reset/dropped
  immediately (never buffered). A session whose id is known but which is not established yet counts as unknown for
  datagrams; its streams are held instead — the preamble is taken and the rest left unread on the transport until
  `OnConnected` — because a peer may legitimately open one as soon as it has sent CONNECT.
* Session establishment: Extended CONNECT (`:method=CONNECT`, `:protocol=webtransport`, `:scheme=https`,
  `:authority`, `:path` = `WebTransportOptions.Path` (default `/quicly`), `origin` — checked according to
  `WebTransportOptions.OriginPolicy`) → `:status 200`. The CONNECT stream stays open for the session and its QUIC
  stream id is the session id.
* Datagrams: RFC 9297 — `quarterStreamId varint` (= CONNECT stream id / 4) prefix, then the QUICLY frame.
* Unidirectional streams: `0x54 varint`, `sessionId varint`, then the QUICLY stream. Bidirectional streams:
  `0x41 varint`, `sessionId varint`.
* Capsules: after the header section the CONNECT stream carries the Capsule Protocol, which is the HTTP message's
  *content* — so in HTTP/3 it is the payload of `DATA` frames (RFC 9297 §3.2, §3.5), never bare frames on the stream.
  One `DATA` frame may carry several capsules and one capsule may span several. A control frame on the CONNECT stream
  is `H3_FRAME_UNEXPECTED`; unknown frames and capsules are ignored (RFC 9114 §9).
* Session close: `CLOSE_WEBTRANSPORT_SESSION` capsule (0x2843) on the CONNECT stream *and* a QUIC close whose code is
  the application code mapped into the WebTransport range of the HTTP/3 error space
  (`h3 = 0x52e4a40fa8db + n + floor(n / 0x1e)`, draft §4.3). Either signal alone tells the peer which §6 code
  ended the session, so a capsule that never made it out is not a lost close reason. `RESET_STREAM` and
  `STOP_SENDING` codes are mapped the same way.
* Framing overhead is added without copying: the datagram prefix and the stream preamble each go out as one extra
  gather segment in front of the caller's, so one Core send is one QUIC send. `MaxDatagramPayload` is reported to
  Core minus the prefix.
* Planned: plain HTTP/3 GET on the same listener answered by the HTTP handler when `ServerOptions.Http3Static` is
  configured (root-jailed, MIME allow-list, no listing, max file size).

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
| 0x12 | BulkChecksumFailed | a transfer's bytes did not match its checksum trailer (§3.3); that range fails, the connection and the object survive |

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
| per-peer receive byte budget (pooled leases + reassembly + stream staging) | 256 KiB | datagram channels: drop newest + count; stream channels: stop consuming (transport back-pressure), resume from Poll. At most a quarter of it can be pinned by unreliable messages nobody drains (next-to-last row) |
| keys per channel per peer (`MaxKeys`) | 4 096 | UnreliableSequenced: evict least-recently-updated key (evicted keys re-accept any sequence; replay window documented — a key still in the table that has idled for more than half the sequence range is in the same position for its next value, no wider: every wire value then extends above what the key holds, §8); ReliableLatest: reject with `LatestReject(4)`, never evict |
| streams a sender keeps open per channel (`MaxGroups`) | 8 (ReliableUnordered), 4 (large ReliableLatest), 2 (Bulk) | the sender's further group, value or transfer waits for one of its streams to shut down. A receiver resets further streams `LimitExceeded` on large ReliableLatest and Bulk channels; on a ReliableUnordered channel it accepts every stream the connection's stream limit admits (see below) |
| stream idle mid-message | 30 s | stream reset `Timeout` |
| concurrent reassemblies per channel (`MaxReassemblies`) | 16 | evict oldest; reassembly expiry 2 × RTT + 100 ms; on a channel whose sequence carries ordering (`UnreliableSequenced`), a newer sequence for the same key abandons the older partial |
| fragmented message size | ≤ 8 × (maxDatagram − header), the §2.1 `FragCount` cap, and ≤ `MaxMessageSize` | drop before any buffer is chosen |
| control messages per second | 2 000 (per peer, configurable; size it from the channel table, because acks scale with keyed `ReliableLatest` traffic and one ack datagram carries about 170 keys) | connection close `LimitExceeded` |
| decoded (decompressed) bytes per second per peer | 8 MiB/s | a compressed message of a reliable channel read with `Drain` waits in its queue (the channel's receive credit then holds its sender back); one dispatched to a handler, or of an unreliable channel, is dropped + counted (`DecodeFailures`) |
| bulk transfers per direction per peer | 2 transfers, and an outbound range-request table of one more than that (3), so a further range can be asked for while both transfers run | `BulkReject` |
| drain backlog of channels without a handler | a pool of min(ring depth, 1 024) messages (at least 2 per reliable channel); with a `ReliableOrdered` / `ReliableUnordered` channel in the table half of it is reserved for those, split evenly. What unreliable channels still have queued when a Poll begins, without having been drained empty since the Poll before (their *backlog*), may occupy the rest and pin ¼ of the byte budget (64 KiB, counted in buffer block sizes: 1 024 messages of ≤ 64 B, 256 of ≤ 256 B, 42 of ≤ 1 536 B) | `UnreliableUnordered` / `UnreliableSequenced` (not coalescing): the backlog is cut to the limit when a Poll begins and **drops oldest** + counts (`DrainQueueDrops`) after that — the oldest queued message of the backlogged channel furthest over its even share; a channel that is drained every frame, or has a handler, is not backlog and loses nothing. `ReliableOrdered` / `ReliableUnordered`: nothing is dropped; a channel nobody reads may have its reserved share of the pool and the same part of ¼ of the byte budget waiting, and beyond that the receiver stops consuming that channel's streams (transport back-pressure on those streams only) until the application drains the channel or registers a handler (see below) |
| receive ring depth per peer | 4 096 entries | see byte budget row (a full ring drops datagrams newest-first, where the drain backlog above drops oldest-first: nothing can be evicted from the ring); latest/coalescing channels use per-key mailboxes instead of ring entries |

**Channels nobody drains.** A message of a channel that has no handler waits for `Drain` in a per-channel
queue, and the two kinds of channel behave differently when the application never comes for it. This is
receiver-local; nothing on the wire changes and either end may run it alone.

* An *unreliable* ring channel that the application **drains every frame** — completely, once per Poll, before
  or after it — loses nothing the receive ring and the byte budget took, however many channels it reads that
  way: a burst waits in the queue pool, then in one held message and the ring, until the Drain (a message held
  when a Poll begins is held again while its channel was drained since the Poll before). The Poll in which the
  session is established counts as drained for every channel — a client library may run it before the
  application has the peer — so what arrived with the handshake is not backlog at the application's first
  Poll. The same holds for a channel **with a handler** whose messages a Drain of another channel met: they are
  never dropped, and the next Poll dispatches them.
* What an unreliable ring channel without a handler still has queued when the *next* Poll begins, without having
  been drained empty in between, is its **backlog**. The backlog of all such channels together is bounded by the
  table row above: the Poll cuts it to the limit, and from then on a message that does not fit drops the
  **oldest** queued one, counted per channel and per peer (`DrainQueueDrops`; `Received` still counts them). A
  consumer that comes back gets the freshest messages in order — on `UnreliableSequenced` the older queued values
  are the stale ones. A channel stops being backlog only when a Drain finds or leaves its queue empty, or it
  gets a handler — not when other channels evicted everything it had queued. A channel nobody drains therefore
  keeps the ring closed once, for one Poll interval (the message held for a Drain that did not come), and not
  again before it has been drained; several such channels do so once each. It pins more than ¼ of the byte
  budget for one Poll interval at most, and keeps no host busy. (A channel that was drained and then no longer
  is, and the first Poll of a session, extend both to two intervals.) Note what
  this means for a host that polls several times between two drains, or that drains with a buffer it fills
  without calling again: what survives a Poll is backlog, so such a host keeps at most the limits above of a
  burst. Drain completely after every Poll, or give the channel a handler. A larger `ReceiveBudgetBytes` raises
  the byte limit; `ReceiveRingCapacity` does not raise the node limit beyond 1 024.
* A *reliable* channel (`ReliableOrdered`, `ReliableUnordered`) is never dropped, so the limit on what may wait
  for the application has to hold where a message is accepted. The receiver keeps a **receive credit** per
  reliable channel: before it takes the ring entry and the buffer for a message, it checks how many messages of
  that channel (and how many buffer bytes) are between the transport and the application. A channel **nobody
  reads** — no handler, and not drained empty since the Poll before last — may have its share waiting: the queue
  nodes reserved for it (table row above: 256 messages with two reliable channels in the table) and the same
  part of ¼ of the byte budget, counted in buffer blocks. The byte share is strict: a message whose block does
  not fit in what is left of it is not started, even on an empty channel, so the channels nobody reads pin at
  most that quarter between them. At the limit the receiver stops consuming **that channel's streams**; the
  sender sees ordinary QUIC flow control on them, and its sends on that channel back up. Every other channel
  keeps flowing: the ring stays open, no datagram is dropped for it, responses on other channels arrive, and the
  peer reports no pending work for it. The streams go on when the application reads the channel — a Drain that
  takes it below its share or leaves it empty — or registers a handler for it. Nothing is lost in between, and
  each hold is counted per channel (`BacklogHolds`).
* A reliable channel **with a handler** has no such limit (its messages leave the ring at every Poll). A channel
  the application **drains** is not held to the share either: a Drain that leaves the channel's queue empty (an
  empty Drain counts) marks it as read, and its limits are then the ring depth in messages and **half the byte
  budget, shared with the other reliable channels no handler reads** (a message is started while less than that
  waits in them, this channel's own messages included, and always on an empty channel; a channel with a handler,
  or with nothing waiting, takes nothing of it, and a channel nobody reads starts no message that does not fit in
  what is left of it). It returns to its share at the first Poll that finds messages queued for it which no
  Drain took during the whole Poll interval before, and it keeps what it accepted until then — which is why the
  drained channels share half the budget and do not get all of it: channels drained and then abandoned, and the
  channels nobody reads, keep at most that half between them (plus one message each), and the other half is what
  is left for the channels that are read. So a host that drains a reliable channel once per frame is limited as a
  handler is, short of that half, and a host that stops draining it is confined within two Poll intervals. A message larger than the
  share of a channel that was never drained arrives after the application's first Drain of it. What the credit
  cannot change is the order inside one stream: on a `ReliableOrdered` channel everything behind an unread
  message waits with it, a response to this end's own request included.
* What the credit cannot confine is the connection's **stream limit**, which is one number for all channels.
  Every group of an unread `ReliableUnordered` channel occupies a stream until the application has read it, and
  its sender — which counts a group as closed when the transport acknowledged it — keeps opening new ones, so an
  unread group channel ends up holding every stream the receiver had left to grant. Streams that are already
  open (an ordered channel after its first message) and all datagram channels are unaffected. A channel that
  still has to open a stream in that direction — the first message of an ordered channel, a group of another
  channel, a large `ReliableLatest` value, a bulk transfer — waits at the **sender** for stream credit until the
  unread channel is drained. Nothing is lost, and the streams come back with the Drain. A `ReliableUnordered`
  channel the other end sends on SHOULD therefore still be read.
* The bytes of a stream that is held back stay in the **transport**, and count against its flow-control windows:
  the stream's own (2 MiB per unidirectional stream with the MsQuic transport's defaults) and the connection's
  (16 MiB, `MsQuicSettings.ConnFlowControlWindow`), which every stream of the connection shares. One unread
  ordered channel holds at most its stream's window. When unread channels together hold the connection's window
  — eight unread ordered channels, or held groups that add up to it (measured: 514 groups of 32 KiB, with a
  receiver that grants 1 024 streams) — no stream of the connection receives anything more until the application
  reads; datagram channels are not affected on the receiving side. A receiver that grants only its table's sum
  (Σ max(`MaxGroups`, 1); the MsQuic transport's default since this release) holds at most that many groups of at
  most `GroupMaxBytes` and a message each, far from the window at default options. With a larger grant there is a
  second effect: a message the receiver has begun to receive, whose tail is still at the sender, waits for
  window that only the other held streams can free, and it holds its buffer meanwhile — the whole receive budget
  for a message above 64 KiB with the default pool — so the held streams wait for budget, and the connection's
  streams stop until `StreamIdleTimeout` gives the message up (the sender sees it `Failed`). The **sender** has a
  limit of its own as well: the messages it keeps sending on a channel the other end does not read stay in its
  queue and in flight, and they count against its send table and send budget, which all its channels share —
  once they are used up, its sends on **every** channel are refused (`OutOfBuffers`), datagrams included, until
  the receiver reads. A sender that does not want one unread channel to use them up sets
  `ChannelOptions.QueueLimitBytes` on it; that channel then answers `QueueFull` alone.
* Two reactions to a peer that is not one of this library's senders. A peer that resets streams this end holds
  back and opens new ones, more than eight times as many as it may have open between two Polls of the receiver,
  is disconnected (`LimitExceeded`): the receiver could not remember a further held stream, and would never
  resume it. That holds for the streams held back for a channel's credit and for those held back for the receive
  ring or the byte budget; a Poll also lets go of the entries beyond what the live streams account for, so a host
  that polls every frame keeps neither list from growing. And a `Drain` that cannot get a buffer to decode a
  compressed message of a reliable channel, or decode budget for it, leaves the message queued — the channel gives
  nothing newer in that call, so its order holds — and returns what it has, instead of dropping it (§7 table,
  decoded bytes). A decode buffer may take the byte budget past its limit by itself (the budget can be full of the
  very compressed messages that wait), so the first message of a Drain that finds everything released, and a
  message dispatched to a handler, get one — unless payloads the application still holds (drained and not yet
  released, or retained) took the budget past its limit already; then a Drain waits and a handler's message is
  dropped and counted. A message that can never get one is dropped and counted by both: a raw size that needs a
  block larger than the budget, and a message whose own block is the pool's last block its decode could use (with
  the default per-peer pool, whose largest class is one block of 256 KiB, a message above 64 KiB both compressed
  and raw, on a channel whose `MaxMessageSize` was raised above the 64 KiB default).

The **stream idle mid-message** rule is per receiving stream and applies to every stream mode: a stream that has
delivered a message's frame header but not the rest of its payload for 30 s (`PeerOptions.StreamIdleTimeout`) is
reset with `Timeout` (RESET_STREAM / STOP_SENDING), which releases that message's staging lease and its
receive-ring slot; the connection survives, and other streams of the same channel are unaffected. Without it a
peer that starts one message and stops can pin the whole per-peer receive byte budget for the life of the
connection, and the connection-level heartbeat does not notice because the peer stays live on other channels.
Progress on the stream (any accepted frame event) restarts the 30 s, and a stream between messages is never
watched.

**`MaxGroups` is a sender's bound, and a ReliableUnordered receiver does not enforce it.** A sender counts a
group stream as closed when its data and FIN are acknowledged, and only then opens the next one past the limit.
The receiver can still have that stream open: acknowledgement is the transport's, and the receiving peer reads
the stream later — when its receive ring has room, its byte budget allows, or its host gets round to `Poll`. A
receiver that is behind therefore sees more than `MaxGroups` streams of a channel open although the sender kept
the limit. It MUST NOT reset them: the sender has already completed those messages `Delivered`, has released
their payloads and cannot send them again, so a reset there is a silent loss on a reliable channel. What bounds
the streams is what the transport admits: the unidirectional stream limit the session asks for after admission
(Σ max(`MaxGroups`, 1) over the stream-capable channels, at most 4 096), or the transport's own initial grant
when that is larger — a QUIC stack announces one in its transport parameters and never takes it back (an MsQuic
client grants its server `ClientPeerUnidiStreamCount` that way: 0 by default since this release, 1 024 before,
see the bullet on flow-control windows above for what a larger grant costs). A slot returns only when the receiver has closed a
stream, and the receiver keeps one receive record for every stream it can be sent, whatever channel uses it. A
sender that ignores `MaxGroups` gains nothing but the slots of its own other channels. The transport's own
stream table must have room for what it admits as well: a stream it had to refuse for want of a slot would
never reach the session and would be lost in the same way. The MsQuic transport therefore sizes its table from
the streams it grants, whatever `MaxStreams` says, and keeps a quarter of it for this end's own streams; while
slots of closed streams still wait for their native close it grows, up to twice that size. Past that a peer
stream is refused (`RefusedPeerStreamCount`) and what it carried can be lost below the session: a peer that churns
streams faster than the thread pool closes them reaches it, and so can an honest peer's group streams when the
receiving process's thread pool is starved for seconds.

This is a receiver-side rule, so it takes effect when the *receiving* end runs it, whatever the sender runs. A
receiver built before it reset the excess streams, and still loses groups when it falls behind, however new its
peer is; it counts them in `StreamsReset`. The reset remains the rule for large
ReliableLatest values and Bulk transfers. There it is not silent (the value is retried, the transfer ends with a
status), and a sender that keeps the limit reaches it through a late receiver only in one case. A value is
`Delivered` only when the receiver acknowledged it, and its receive path holds no stream open while the host is
late with its `Poll`. A Bulk stream can be held (a chunk waits when the receive budget is used up), and a
*completed* transfer's slot is given back to its sender only by the receiver's final `BulkProgress`, which its
game thread sends. A *canceled* transfer's slot, though, comes back with the reset stream's close, which needs
only the receiving transport, while the receiver's record of it waits for its game thread to report it. The
receiver keeps four records per transfer it accepts at once, so a sender that keeps the limit can cancel three
times `BulkTransfersPerDirection` transfers during one hitch of the receiver's host; the next one past that is
refused (`LimitExceeded`, the sender's transfer ends `Failed`) until the host polls again.

One consequence is not solved by it: the stream limit is shared by all channels of a connection. A
ReliableUnordered channel whose receiver never reads it while its sender keeps sending ends up holding every
slot, and from then on a group, a large value or a transfer that needs a *new* stream waits — on any channel —
until that channel is read (a large value waits for stream credit like a group does; its thirty-second version
budget still ends it, with `Failed`, if the wait lasts that long). Streams that are already open (every
ReliableOrdered channel that has sent anything) and all datagram channels are not affected.

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
* **§1 sequence clock (receiver-local; nothing here is on the wire).** Per channel and epoch a receiver keeps
  `newest`, a 64-bit number, 0 = nothing seen. Let `span` = 2^bits and `half` = span / 2.
  * *First arrival* `s`: `newest = span + s` (the bias keeps 0 free and leaves room below the seed).
  * *Later arrival* `s`: `d` = (`s` − low bits of `newest`) as a signed number of the channel's width, in
    [−half, half − 1]; its extended value is `ext = newest + d`. Exactly half the range apart is `d = −half`,
    "behind", as in RFC 1982 serial comparison. If `d > 0`, `newest = ext`.
  * *Per key*: a value is accepted iff the key has no state, or `ext` is greater than the key's last accepted
    extended value, which it then replaces. The comparison is a plain 64-bit one. An unkeyed
    `UnreliableSequenced` channel accepts iff `newest` advanced.
  * *Resynchronisation, `UnreliableSequenced` only*: an arrival that is **more than the reorder window behind**
    `newest` (`d < −W`; `W` = 1 024 on a 16-bit channel, 65 536 on a 32-bit one) **and** comes more than **2 s**
    after `newest` last advanced (on the receiver's clock) is taken for a forward jump of `span − |d|` — at least
    half the range — instead of a late message: `ext = newest + span + d`, `newest = ext`, and the value is judged
    like any other (counted in `PeerStatistics.SequenceResyncs`). A duplicate of the newest (`d = 0`) and an
    arrival inside the window never resynchronise. "Quiet" means `newest` did not advance, not that nothing
    arrived, so a clock left more than `W` ahead (a peer's mistake) heals within 2 s, and one left less than that
    ahead heals when the sender's counter passes it, at most `W` messages later.
    * *Why both tests.* The wire value cannot tell "late by x" from "ahead by span − x". Time separates them on
      the path — QUIC never retransmits a DATAGRAM frame, so the network delivers a late datagram near its
      successors — but **not in the sender**: a datagram sent without cancel-on-blocked (a packed container that
      also carries a `ReliableLatest` value, or every datagram of a sender that turned the flag off, §4.5) waits
      in the transport's queue for as long as the link is busy, and a later datagram with the priority flag (an
      Immediate or high-priority member) overtakes it. It then arrives seconds after its successor, a few numbers
      behind `newest`. Distance separates those: a late message is behind by the handful of messages that
      overtook it, whereas a jump of `J` reads as `span − J` behind, which is that small only when almost a whole
      range was lost.
    * *What the window costs.* A real jump that lands within `W` of a whole range (more than 64 512 consecutive
      messages lost on a 16-bit channel) is not resynchronised; the arrivals read as late until the sender's
      counter has passed `newest`, at most `W` messages — a small fraction of what the gap itself lost.
    * *What remains.* A message that **more than `W` later messages of its channel overtook**, arriving after
      the channel then stayed quiet for 2 s, is still read as a jump: it is delivered out of order once (for its
      key; superseded by the key's next update), and `newest` then sits at that message's low bits, so later
      genuine sequences are ahead of it and delivery goes on. A sender can produce this only on a link congested
      for more than 2 s while it sends non-cancellable datagrams with priority ones behind them; keep
      cancel-on-blocked on (the default) for sequenced channels, or do not mix Immediate and buffered sends on
      one sequenced channel, where that matters. A resynchronisation re-opens **every** tracked key of the
      channel to lower wire sequences once (they all extend above the old values).
    * A 16-bit channel faster than 16 384 msg/s can use up half its range inside the 2 s and then drops for at
      most 2 s. A jump of an exact multiple of the range reads as a duplicate and costs one message.
  * *`ReliableLatest`* uses the same clock with bits = 32 over its versions (datagram values and group-stream
    values alike; version 0 is never sent, which only makes each wrap one number shorter) and **never**
    resynchronises on time: a retransmission arrives behind the newest version after any length of quiet.
  * *Lifetime*: the clock is forgotten with the keys — at an epoch reset and, for the datagram modes, when the
    connection is replaced.
  * *Hostile peers*: only the authenticated peer can produce datagrams, and it could always send any sequence.
    A sequence far ahead moves the clock by less than half a range and makes that peer's own later values
    stale; per-channel state stays fixed-size and per-message work O(1).
  * *Interoperating with an implementation that compares per key in serial arithmetic* (the reference
    implementation before this clarification). `UnreliableSequenced` is decided by the receiver of each
    direction alone: a clock-keeping receiver is correct whatever the sender; a per-key receiver keeps dropping
    the values of a key that idled past half the range until the counter comes round, and the sender cannot help
    it. `ReliableLatest`, for a key idle for 2^31 versions or more:

    | Sender | Receiver | Outcome |
    |---|---|---|
    | exact-version ack rule | version clock | accepted, acked, `Delivered` |
    | cumulative ack rule | version clock | accepted, acked, `Delivered` (the ack names the new version) |
    | exact-version ack rule | per-key serial | dropped and the old version re-acked; the sender ignores that ack and the value ends `Failed` after its budget — never `Delivered` |
    | cumulative ack rule | per-key serial | dropped, and for distances above 2^31 the re-ack of the old version completes the new value: a `Delivered` for a value that never arrived |

    Below 2^31 all four combinations behave identically.
* **§1 defaults.** A channel's local `ExpiryMicros` default is 0 (none) for every mode except
  `UnreliableSequenced` (2 × the flush interval, §4.5), including `UnreliableUnordered`. `MaxGroups` defaults to 1
  for `ReliableOrdered` and 0 for datagram-only modes.
* **§2.1 fragments.** Every fragment carries at least one payload byte, and the last fragment carries between 1
  and size(fragment 0) bytes. A receiver can therefore bound the total from any single fragment: a non-last
  fragment of size *s* implies a total ≥ *s* × (FragCount − 1) + 1, the last fragment of size *l* implies a total
  ≥ *l* × FragCount; if that bound exceeds the effective `MaxMessageSize` (uncompressed), the fragment is dropped.
  Both are lower bounds, so once a receiver knows both sizes of one message — in whichever order they arrive — it
  checks the exact total, *s* × (FragCount − 1) + *l*, against the effective `MaxMessageSize` and drops the fragment
  with its partial when it exceeds it.
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
  not `TotalLength`. `Flags` bits 2–7 are reserved and MUST be 0. An unchunked body is exactly `Length` bytes. In a
  chunked body `ChunkLength ≥ 1`; `RawLength = 0` means the chunk is stored uncompressed, otherwise
  `ChunkLength < RawLength`; the decoded sizes of all chunks sum to exactly `Length`. The checksum trailer, when bit 0 is
  set, is the 8 bytes immediately after the last body byte; a stream that ends before them is truncated, and bytes after
  the body (or after the trailer) are malformed. The session cap
  `HelloAck.maxMessageSize` bounds message frames only (datagram messages, ordered and group stream frames); it does
  not apply to Bulk transfers, whose `Length` is bounded by the Bulk channel's own `MaxMessageSize` alone.
* **§3.3 transfer ids.** A receiver enforces the uniqueness of `TransferId` per (peer, direction) over every transfer it
  still holds state for — one that is running, and one that has finished but whose final `BulkProgress` has not gone out
  yet. Beyond that it MAY accept an id again within the epoch: remembering every id an epoch has seen would be unbounded
  state that the peer controls. A sender never reuses an id within an epoch (the reference implementation numbers its
  transfers upward from 1), so a reuse can only confuse the peer that made it.
