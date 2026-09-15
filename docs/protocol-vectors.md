# QUICLY protocol test vectors

Byte-exact vectors for independent implementations of [PROTOCOL.md](PROTOCOL.md). Every vector here is asserted
by the unit tests named next to it (`tests/Tedd.Quicly.Core.Tests`); changing an encoding breaks both the tests and
interoperability. Bytes are hexadecimal; spaces only separate fields.

## 1. Channel table (PROTOCOL §1) — `ChannelTableCodecTests`

Reference table (declaration order does not matter; names are not hashed):

| Id | Name | Mode | Options | Resolved flags | Priority | MaxMessageSize |
|---:|---|---|---|---|---:|---:|
| 2 | `move` | UnreliableSequenced (1) | keyed | keyed, seq32 → `0x03` | 200 | 1 200 (default) |
| 3 | `chat` | ReliableOrdered (2) | request-response | seq16, request-response → `0x08` | 128 | 65 536 (default) |
| 5 | `fx` | UnreliableUnordered (0) | fragmentation, max 8 800 | seq16, fragmentation → `0x04` | 128 | 8 800 |
| 10 | `state` | ReliableLatest (4) | LZ4 | keyed, seq32, coalesce, LZ4 → `0x33` | 128 | 65 536 (default) |
| 64 | `world` | Bulk (5) | priority 16 | `0x00` | 16 | 16 777 216 (default) |

### Canonical encoding (38 bytes)

```
05                          count = 5
02 01 03 C8 44B0            id 2, mode 1, flags 0x03, priority 200, maxMessageSize 1200
03 02 08 80 80010000        id 3, mode 2, flags 0x08, priority 128, maxMessageSize 65536
05 00 04 80 6260            id 5, mode 0, flags 0x04, priority 128, maxMessageSize 8800
0A 04 33 80 80010000        id 10, mode 4, flags 0x33, priority 128, maxMessageSize 65536
4040 05 00 10 81000000      id 64 (two-byte varint), mode 5, flags 0x00, priority 16, maxMessageSize 16777216
```

### TableHash

XXH64, seed 0, over the 38 canonical bytes:

```
TableHash = 0xB9ACCCD14847183C   (13379293792243750972)
```

The empty table encodes as `00` and hashes to XXH64(`00`).

### HelloAck table section (canonical encoding + names)

The 38 canonical bytes followed by, per channel in id order, `name length varint + UTF-8`:

```
04 6D6F7665                 "move"
04 63686174                 "chat"
02 6678                     "fx"
05 7374617465               "state"
05 776F726C64               "world"
```

### Name of exactly 64 bytes

A name length is a minimal varint like every other length: names of 0–63 bytes cost one length byte, a 64-byte name
(the maximum) costs two (`4040`). One channel (id 2, `UnreliableUnordered`, defaults, name = 64 × `a`):

```
01 02 00 00 80 44B0         canonical: count 1; id 2, mode 0, flags 0x00, priority 128, maxMessageSize 1200
4040 6161…61                name length 64 (two-byte varint), then 64 bytes "a"
```

## 2. Datagram message headers (PROTOCOL §2.1) — `DatagramFramingTests.Header_Vectors_Write_And_Parse`

`Sequence` is little-endian; `ChannelId`, `Key`, `RawLength` are minimal QUIC varints.

| Channel shape | Header fields | Encoded header |
|---|---|---|
| id 2, UnreliableUnordered | — | `02` |
| id 3, UnreliableSequenced, 16-bit | seq 0x1234 | `03 3412` |
| id 4, UnreliableSequenced, keyed, 32-bit | seq 0x01020304, key 5 | `04 04030201 05` |
| id 5, UnreliableSequenced, keyed, 16-bit | seq 0xABCD, key 300 | `05 CDAB 412C` |
| id 6, UnreliableUnordered, fragmentation (16-bit reassembly id) | seq 7, FragCount 1 | `06 0700 01` |
| id 6, same | seq 7, FragCount 3, FragIndex 2 | `06 0700 03 02` |
| id 7, UnreliableSequenced, keyed, 32-bit, fragmentation | seq 1, key 64, FragCount 8, FragIndex 7 | `07 01000000 4040 08 07` |
| id 8, UnreliableUnordered, LZ4 | RawLength 0 (not compressed) | `08 00` |
| id 8, same | RawLength 1000 | `08 43E8` |
| id 9, UnreliableSequenced, keyed, 32-bit, fragmentation, LZ4 | seq 2, key 1, FragCount 2, FragIndex 1, RawLength 5000 | `09 02000000 01 02 01 5388` |
| id 10, ReliableLatest | seq 0xFFFFFFFF, key 16384 | `0A FFFFFFFF 80004000` |
| id 11, ReliableLatest, LZ4 | seq 9, key 2^30, RawLength 60000 | `0B 09000000 C000000040000000 8000EA60` |
| id 100, UnreliableSequenced, keyed, 32-bit | seq 3, key 2 | `4064 03000000 02` |
| id 200, UnreliableUnordered | — | `40C8` |
| id 16383, ReliableLatest, LZ4 | seq 1, key 0, RawLength 0 | `7FFF 01000000 00 00` |

## 3. Packed container header (PROTOCOL §2.2) — `PackedContainerTests.WriteHeader_Vectors`

| Tick | Encoded |
|---|---|
| absent | `01 00` |
| 5 | `01 01 05` |
| 1000 | `01 01 43E8` |
| 2^32 − 1 | `01 01 C0000000FFFFFFFF` |

A container with one message `02` and no tick: `01 00 01 02`.

## 4. Stream frames (PROTOCOL §3) — `StreamFramingTests`

Preambles: channel 0 → `00`; 63 → `3F`; 64 → `4040`; 16383 → `7FFF`. Group preamble (channel 14, group 64) →
`0E 4040`; (channel 300, group 2^30) → `412C C000000040000000`.

| Channel shape | Frame header fields | Encoded frame header |
|---|---|---|
| ReliableOrdered | Length 5 | `05` |
| ReliableOrdered | Length 64 | `4040` |
| ReliableOrdered, keyed, request-response, LZ4 | Length 10, key 7, RequestId 3, RawLength 0 | `0A 07 03 00` |
| same | Length 10, key 16384, RequestId 64, RawLength 200 | `0A 80004000 4040 40C8` |
| ReliableUnordered, keyed, LZ4 | Length 3, key 1, RawLength 9 | `03 01 09` |
| ReliableLatest (group stream) | Length 4, Sequence 0x01020304, key 9 | `04 04030201 09` |
| ReliableLatest, LZ4 (group stream) | Length 4, Sequence 1, key 9, RawLength 10 | `04 01000000 09 0A` |

Control frames (`Length = body + 1`, then `Type`): empty body, type 0xAA → `01 AA`; 63-byte body → `4040 AA`;
16 383-byte body (the maximum) → `80004000 AA`.

Bulk header (TransferId 7, ObjectId 1000, ObjectVersion 3, TotalLength 1 000 000, Offset 64, Length 100,
flags = chunked, no hash) → `07 43E8 03 800F4240 4040 4064 02`. Bulk chunk header (ChunkLength 100, RawLength 0) →
`4064 00`.
