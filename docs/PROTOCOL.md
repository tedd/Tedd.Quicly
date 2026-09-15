# QUICLY wire protocol, version 1

All multi-byte **variable-length integers** use the QUIC varint encoding (RFC 9000 §16: 2-bit length prefix,
big-endian, 1/2/4/8 bytes, max 2^62−1). All **fixed-width** integers (sequence numbers, versions, timestamps)
are **little-endian** (native on every target we run on; loads are single instructions).

Two ALPNs are served on one UDP port:

* `quicly/1` — raw QUIC. Frames below map 1:1 onto QUIC DATAGRAM frames and QUIC streams.
* `h3` — HTTP/3 + WebTransport (draft-ietf-webtrans-http3). Frames below are carried inside the WebTransport
  session's datagrams and streams; the WebTransport layer adds its own prefixes (quarter-stream-id for
  datagrams, stream-type + session-id for streams) which are stripped before QUICLY parsing. The two
  transports are therefore byte-identical at the QUICLY layer.

## 1. Channel identifiers

| Channel | Meaning |
|---|---|
| 0 | control (session handshake, ping, acks, receipts, close) |
| 1 | packed container: several messages in one datagram |
| 2 … 16383 | application channels declared in the `ChannelTable` (default limit 256) |

A channel definition (both sides must hold the same table; the handshake compares a 64-bit hash of the
canonical encoding) contains: id, name (informational), `DeliveryMode`, `Keyed`, `SequenceBits` (16 or 32),
`Priority`, `MaxMessageSize`, `QueueLimitBytes`, `ExpiryMicros`, `Fragmentation`, `Compression`, `Receipts`,
`KeySpace` (dense or hashed).

Canonical encoding of the table (used for the hash, and sent by the server in `HelloAck` when requested):
`count varint`, then per channel: `id varint, mode u8, flags u8 (bit0 keyed, bit1 seq32, bit2 fragmentation,
bit3 receipts, bit4-5 compression), priority u8, maxMessageSize varint, name len varint + utf8`.
Hash = XXH3-64 style 64-bit hash over that encoding (implemented in Core as `StableHash64`).

## 2. Datagram frames

The first byte(s) of every datagram are a channel varint.

### 2.1 Application message (channel ≥ 2)

```
ChannelId   : varint
Sequence    : u16 LE | u32 LE   only when mode ∈ {UnreliableSequenced, ReliableLatest} or Fragmentation
Key         : varint            only when channel.Keyed
FragIndex   : u8                only when channel.Fragmentation
FragCount   : u8                only when channel.Fragmentation   (1 = not fragmented)
RawLength   : varint            only when channel.Compression != None (0 = payload not compressed)
Payload     : remaining bytes
```

Header cost: 1 byte for an unsequenced, unkeyed channel; 3 bytes for a 16-bit sequenced unkeyed channel;
4 bytes with a one-byte key. The QUIC/UDP/IP overhead is outside this document.

Sequence comparison is RFC 1982 serial arithmetic (`(short)(a - b) > 0` for 16 bits). The sequence
counter is per channel, or per key on keyed channels (each key is an independent state stream).

### 2.2 Packed container (channel 1)

```
0x01
repeat: Length varint, Message (a complete 2.1 or 2.3 frame including its ChannelId)
```

The scheduler packs buffered datagram messages for the same peer into one datagram when at least two fit;
cost is 1 byte plus one length varint per message.

### 2.3 Control datagram (channel 0)

```
0x00
Type : u8
Body
```

| Type | Name | Body |
|---|---|---|
| 0x01 | Ping | `t u64 LE` (sender monotonic micros) |
| 0x02 | Pong | `t u64 LE` (echoed), `recv u64 LE` (receiver monotonic micros when Ping was received), `send u64 LE` (when Pong was sent) |
| 0x03 | LatestAck | `count varint`, repeat: `channel varint, key varint, version u32 LE` |
| 0x04 | Receipt | `count varint`, repeat: `channel varint, cumulativeCount varint` — "accepted every ordered message up to and including #N" |
| 0x05 | GroupReceipt | `count varint`, repeat: `channel varint, groupId varint` |
| 0x06 | Applied | `channel varint, key varint, sequenceOrVersion u32 LE` |
| 0x07 | BulkProgress | `transferId varint, bytesAccepted varint` |

Every control datagram type may also be sent as a control-stream message (§3.4) when datagrams are not
available on the transport.

## 3. Stream frames

Each QUIC stream begins with a **preamble** whose first varint is the channel id. The channel's mode from the
table decides how the rest of the stream is parsed.

### 3.1 Persistent ordered stream (mode `ReliableOrdered`)

One unidirectional stream per channel per direction, opened lazily on first send, never closed while the
session lives.

```
Preamble : ChannelId varint
repeat:
  Length    : varint    payload length (excludes Key / RawLength)
  Key       : varint    only when channel.Keyed
  RawLength : varint    only when channel.Compression != None (0 = not compressed)
  Payload   : Length bytes
```

Message boundaries are never implied by QUIC write boundaries; the receiver parses incrementally and a
message may span any number of receive callbacks.

### 3.2 Group stream (mode `ReliableUnordered`, and large `ReliableLatest` values)

One unidirectional stream per *group* (all messages of that channel admitted between two flushes, bounded
by `GroupMaxBytes`), closed with FIN after the last message. Groups are independent: loss in one group
never delays another.

```
Preamble : ChannelId varint, GroupId varint
repeat: same message framing as 3.1
FIN
```

For `ReliableLatest` values larger than the datagram limit the group holds exactly one message and
`GroupId` = the value's version; the sender aborts (RESET_STREAM) any still-open stream for the same key
when a newer version is submitted.

### 3.3 Bulk stream (mode `Bulk`)

```
Preamble : ChannelId varint
Header   : TransferId varint, ObjectId varint, ObjectVersion varint, TotalLength varint,
           Offset varint, Length varint, Flags u8 (bit0 hash present, bit1 chunked/compressed),
           Hash 32 bytes (XXH3-128 low/high or SHA-256, negotiated: v1 = SHA-256) when bit0
Body     : raw bytes                                       when bit1 = 0
           repeat: ChunkLength varint, RawLength varint, bytes   when bit1 = 1 (each chunk independently decodable)
FIN
```

A transfer never requires either side to hold the whole object in memory. The receiver reports
`BulkProgress` periodically and on completion; the sender surfaces progress. Resume = a new transfer for
the remaining `[Offset, Offset+Length)` range with the same `ObjectId`/`ObjectVersion`.

### 3.4 Control stream (channel 0)

A single bidirectional stream opened by the client immediately after the QUIC handshake. Preamble `0x00`,
then framed control messages:

```
Length : varint
Type   : u8
Body   : Length-1 bytes
```

| Type | Name | Direction | Body |
|---|---|---|---|
| 0x10 | Hello | C→S | `magic "QLCY"`, `version u16 LE (=1)`, `flags u16 LE`, `tableHash u64 LE`, `epoch u32 LE (0 = fresh)`, `sessionToken (len varint + bytes)`, `authToken (len varint + bytes)`, `maxDatagram u16 LE`, `caps u16 LE` |
| 0x11 | HelloAck | S→C | `status u8 (0 = accepted)`, `sessionId u64 LE`, `epoch u32 LE`, `maxDatagram u16 LE`, `caps u16 LE`, `maxMessageSize varint`, `heartbeatMicros varint`, `sessionToken (len + bytes)`, `tableIncluded u8` [+ canonical table], `reason (len varint + utf8)` |
| 0x12 | Close | both | `code u32 LE`, `reason (len varint + utf8)` |
| 0x13 | BulkRequest | both | `requestId varint, channel varint, objectId varint, objectVersion varint, offset varint, length varint` |
| 0x14 | BulkCancel | both | `transferId varint, code u32 LE` |
| 0x15 | BulkReject | both | `requestId varint, code u32 LE` |
| 0x16 | ChannelTableRequest | C→S | (empty) — server answers with HelloAck-style table for diagnostics |
| 0x01–0x07 | as §2.3 | both | control-datagram fallbacks |

`flags` bit0 = request channel table in ack. `caps` bit0 = datagrams supported, bit1 = receipts supported,
bit2 = compression LZ4, bit3 = compression Brotli.

Handshake failure codes (HelloAck.status): 1 version mismatch, 2 channel table mismatch, 3 rejected by
admission policy, 4 server full, 5 bad session token, 6 internal.

## 4. Session semantics

* **Epoch**: every connection (fresh or reconnect) gets a new epoch; all sequence/version tables reset.
  A reconnecting client presents its `sessionToken`; the server may resume session identity (same
  `sessionId`, new epoch) within its grace period.
* **Guarantees**: `ReliableOrdered`/`ReliableUnordered` complete `RemoteAccepted` only through receipts
  (§2.3 type 4/5); without receipts they complete `BufferReleased` when QUIC acknowledged the bytes
  (MsQuic SEND_COMPLETE in non-buffered mode) — which is *not* proof that the remote application processed
  them. `ReliableLatest` completes `Delivered` on `LatestAck` for the current version, `Superseded` when a
  newer version replaced it, `Failed` on disconnect or retry budget exhaustion.
* **ReliableLatest retry**: resend the current version when `now − lastSend ≥ max(minRetry, 1.5 × RTT)`,
  exponential backoff to `maxRetry`; receivers re-ack duplicates so a lost ack cannot stall completion.
* **Packing** and **immediate** sends: `Immediate` bypasses the batching delay only; congestion control and
  pacing still apply.
* **Limits** enforced before any buffer is chosen: `MaxMessageSize`, `RawLength ≤ MaxMessageSize`,
  fragment count ≤ 255, groups per channel, keys per channel, in-flight bulk transfers, receive ring depth.

## 5. HTTP/3 / WebTransport mapping (ALPN `h3`)

* Server SETTINGS: `QPACK_MAX_TABLE_CAPACITY=0`, `QPACK_BLOCKED_STREAMS=0`, `ENABLE_CONNECT_PROTOCOL=1`,
  `H3_DATAGRAM (0x33)=1`, `ENABLE_WEBTRANSPORT (0x2b603742)=1`, `WT_MAX_SESSIONS (0xc671706a)=N`.
* Session establishment: Extended CONNECT (`:method=CONNECT`, `:protocol=webtransport`, `:scheme=https`,
  `:authority`, `:path`, `origin`) → `:status 200`. The CONNECT stream stays open for the session.
* Datagrams: RFC 9297 — `quarterStreamId varint` (= CONNECT stream id / 4) prefix, then the QUICLY frame.
* Unidirectional streams: `0x54 varint`, `sessionId varint`, then the QUICLY stream (preamble + frames).
  Bidirectional streams: `0x41 varint`, `sessionId varint`.
* Session close: `CLOSE_WEBTRANSPORT_SESSION` capsule (0x2843) on the CONNECT stream, or QUIC connection close.
* Plain HTTP/3 GET requests on the same listener are answered by the HTTP handler (static files, health);
  this is what lets the server "be an HTTP server" on its game port.
