# Tedd.QUICLY

Efficient game communication protocol and library for .NET, built directly on QUIC (MsQuic).

QUICLY gives a game the transport primitives it actually needs — on one UDP port, with one API:

| Need | Channel mode | Under the hood |
|---|---|---|
| positions, aim, animation state at 20–120 Hz | `UnreliableSequenced` | QUIC DATAGRAM + serial sequence numbers, newest wins |
| bullets, hit markers, one-shot lossy events | `UnreliableUnordered` | QUIC DATAGRAM |
| RPCs, chat, inventory, matchmaking | `ReliableOrdered` | one persistent QUIC stream per channel |
| independent reliable events (no head-of-line blocking) | `ReliableUnordered` | one QUIC stream per flush group |
| "the latest value must eventually arrive" state | `ReliableLatest` | versioned datagrams + acks + retries |
| world data, assets, large snapshots | `Bulk` | dedicated low-priority streams, chunked, resumable |

Design goals: zero allocations and no locks on the hot path, data-oriented (struct arrays, cache locality),
explicit buffer ownership (zero-copy send from your own buffers, receive straight into memory you choose),
a 1–6 byte application header, and a server that also speaks HTTP/3 + WebTransport and HTTP/1.1 so it can
serve a web client and obtain its own certificates from any ACME CA (Let's Encrypt, ZeroSSL, Buypass,
Google Trust Services, …).

* [Architecture](docs/ARCHITECTURE.md) · [Wire protocol](docs/PROTOCOL.md) · [Decisions](docs/adr/) ·
  [Benchmarks](docs/benchmarks/) · [Development guide](docs/DEVELOPMENT.md)

## Status

Under construction; see [docs/STATUS.md](docs/STATUS.md) for what is merged, what is in progress and the test and coverage numbers. Targets `net11.0` (primary) and `net10.0`. Windows 11 / Server 2022+ (Schannel QUIC),
Linux with `libmsquic`, macOS with `libmsquic`. Browser (WebAssembly) client support is optional and experimental.

## Layout

```
src/        Tedd.Quicly.Core, Client, Server, Http3, Transport.MsQuic, Http, Acme, Testing, Replication
tests/      one xunit.v3 project per library + EndToEnd
benchmarks/ Tedd.Quicly.Benchmarks (BenchmarkDotNet) and Tedd.Quicly.Archive (superseded versions)
samples/    minimal server and client
docs/       architecture, protocol, ADRs, benchmark records, reference material
```

## License

See [LICENSE](LICENSE).
