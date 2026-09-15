# ADR 0001 — Bind MsQuic directly instead of using System.Net.Quic or Kestrel

**Status:** accepted (2026-09-15)

## Context
`System.Net.Quic` in .NET 10 and .NET 11 preview 7 exposes streams only; QUIC DATAGRAM frames are not
reachable (verified by reflection over the installed runtimes). Kestrel's WebTransport feature is still
preview and has no datagram support. Unreliable datagrams are the whole point of a game transport.
`msquic.dll` (2.5.x) ships inside the shared framework on Windows and is loadable with a plain
`DllImport("msquic")`; on Linux/macOS `libmsquic` is the same dependency System.Net.Quic already has.

## Decision
`Tedd.Quicly.Transport.MsQuic` carries hand-written bindings for the MsQuic v2 API table. Struct layouts and
enum values are validated by unit tests against the numbers dumped from the runtime's own internal bindings
(`docs/reference/msquic/msquic_layout_dotnet11_preview7.txt`) and against `msquic.h`.

Both raw QUIC (ALPN `quicly/1`) and WebTransport-over-HTTP/3 (ALPN `h3`) are built on this binding, so
native clients, browsers and plain HTTP/3 clients share one listener and one UDP port.

## Consequences
* Full access to datagrams, stream priorities, scatter/gather sends, non-buffered (zero-copy) sends,
  datagram send-state events, statistics, execution profiles.
* We own the safety of the interop layer: callbacks are `[UnmanagedCallersOnly]`, contexts are slot indices
  (never GC handles per message), every handle is an owner with an explicit lifetime.
* If a future .NET adds a datagram API we can add a `System.Net.Quic` adapter without touching Core.
