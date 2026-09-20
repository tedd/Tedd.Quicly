# Tedd.QUICLY

Efficient game communication protocol and library for .NET, built directly on QUIC (MsQuic).

Project site: [tedd.no/Tedd.Quicly](https://tedd.no/Tedd.Quicly/)

QUICLY gives a game the transport primitives it actually needs — on one UDP port, with one API:

| Need | Channel mode | Under the hood |
|---|---|---|
| positions, aim, animation state at 20–120 Hz | `UnreliableSequenced` | QUIC DATAGRAM + serial sequence numbers, newest wins |
| bullets, hit markers, one-shot lossy events | `UnreliableUnordered` | QUIC DATAGRAM |
| RPCs, chat, inventory, matchmaking | `ReliableOrdered` | one persistent QUIC stream per channel |
| independent reliable events (no head-of-line blocking) | `ReliableUnordered` | one QUIC stream per flush group |
| "the latest value must eventually arrive" state | `ReliableLatest` | versioned datagrams + acks + retries |
| files, assets, world snapshots — any size | `Bulk` | low-priority streams, chunked, checksummed per range, resumable |

Design goals: zero allocations and no locks on the hot path, data-oriented (struct arrays, cache locality),
explicit buffer ownership (zero-copy send from your own buffers, receive straight into memory you choose),
a 1–6 byte application header, an opt-in WebTransport-over-HTTP/3 carrier so a browser can speak the same
protocol, and an HTTP/1.1 server that obtains its own certificates from any ACME CA (Let's Encrypt, ZeroSSL,
Buypass, Google Trust Services, …).

* [Architecture](docs/ARCHITECTURE.md) · [Wire protocol](docs/PROTOCOL.md) · [Decisions](docs/adr/) ·
  [Benchmarks](docs/benchmarks/) · [Development guide](docs/DEVELOPMENT.md)

## Carriers

The same QUICLY session runs over either carrier, so nothing above the transport changes:

```csharp
// Raw QUIC, ALPN quicly/1
var listener = new MsQuicTransportListener(endPoint, certificate);

// WebTransport over HTTP/3, ALPN h3 — what a browser can reach (docs/PROTOCOL.md §5)
var listener = WebTransportListener.CreateMsQuic(endPoint, certificate,
    options: new WebTransportOptions { Path = "/quicly" });
```

Both hand an `ITransport` to the accept callback and both pass the same transport conformance suite.

## Bulk objects

A Bulk channel's `MaxMessageSize` bounds one *transfer* — 16 MiB at most — and not the object it belongs to. An object
of any size up to 2^62 − 1 bytes is sent as one call: the driver splits it into ranges, keeps a window of them in
flight, re-sends any that fail their checksum, and completes once.

```csharp
// Sending: one call, one completion. The source is read at absolute object
// offsets and never buffered, so a 10 GB file costs a few hundred bytes of state.
var result = await peer.SendBulkObjectAsync(
    new BulkObjectDescriptor(Channel: 5, ObjectId: 42, ObjectVersion: 1, TotalLength: file.Length),
    source: new MyFileSource(file),
    progress: (in BulkObjectProgress p) => Console.Write($"\r{p.Fraction:P0}"));

// Receiving: asked once per object, not once per transfer.
options.BulkObjectRouter = new MyRouter();   // returns one IBulkObjectSink per object
```

Every range carries an xxHash64 trailer over its own bytes, so a corrupt range is caught at *its* end rather than the
object's and is re-sent on its own. That is an end-to-end check against bugs and bad hardware — QUIC's mandatory AEAD
already covers the wire — so it is a switch: `PeerOptions.BulkChecksum`, overridable per transfer.

## Status

Under construction; see [docs/STATUS.md](docs/STATUS.md) for what is merged, what is in progress and the test and coverage numbers. Targets `net11.0` (primary) and `net10.0`. Windows 11 / Server 2022+ (Schannel QUIC),
Linux with `libmsquic`, macOS with `libmsquic`. Browser (WebAssembly) client support is optional and experimental.

## Layout

```
src/        Tedd.Quicly.Core, Client, Server, Http3, Transport.MsQuic, Http, Acme, Testing, Replication
tests/      one xunit.v3 project per library + EndToEnd
benchmarks/ Tedd.Quicly.Benchmarks (BenchmarkDotNet) and Tedd.Quicly.Archive (superseded versions)
samples/    a runnable bulk-object file transfer over real MsQuic (server + client)
docs/       architecture, protocol, ADRs, benchmark records, reference material
```

## License

See [LICENSE](LICENSE).
