# ADR 0004 — Explicit buffer ownership; "zero-copy" means one copy at most, chosen by the caller

**Status:** accepted (2026-09-15)

## Context
You cannot have all three of: no retained copy, immediate buffer reuse, and later retransmission of the
original bytes. MsQuic's non-buffered mode keeps application buffers until acknowledged; its buffered mode
copies and releases early.

## Decision
* Library memory is native, pre-allocated, size-classed, lock-free (`SlabAllocator`); `BufferLease` is the
  unit of ownership; `SharedLease` adds reference counting for fan-out.
* Send: `SendCopy` (copy now, release now, for locked structures), `RentBuffer` + `SendOwned` (write into
  library memory, zero copy), `SendBorrowed` (pin caller memory, release on `BufferReleased`), `SendGather`,
  `SendShared`.
* Receive: header first, then the application chooses the destination (`Memory<byte>`, `IBufferWriter`,
  pooled lease, reject). One copy from the transport's buffer into that destination. Partial data never
  touches live state; completion is published only for complete, validated messages.
* Completion points are distinct and separately awaitable: BufferReleased, RemoteAccepted, Applied.
  Cancelling a wait never cancels the send; cancelling a send never releases the payload by itself.
* Synchronous waits exist for native hosts only and are never used to implement async paths.
* A receive lease the application holds (retained in a handler, drained, or a response) is the application's
  until it releases it, and it outlives its peer when the peer rents from a shared allocator: the library
  never returns such a block on the application's behalf (the application may still be reading it), and
  `QuiclyPeer.Release` after `Dispose` returns it to the shared pool. Over a private allocator the pool is freed
  with the peer and a later `Release` is a no-op (docs/design/session-layer.md §4 "A lease can outlive its peer").

## Consequences
Callers holding a lock over game state should use `SendCopy`; the copy is microseconds, while
`BufferReleased` for a reliable stream is at least one RTT away.
