# ADR 0005 — Threading model and data-oriented layout

**Status:** accepted (2026-09-15)

## Decision
* Receive-side state is owned by the transport thread (MsQuic serialises a connection's callbacks);
  send-side state is owned by the game thread; hand-offs go through pre-allocated lock-free rings
  (`ReceiveRing`, `CompletionRing`) drained by `Poll()`.
* All per-channel / per-key / per-message state lives in struct arrays indexed by dense ids (ECS style);
  no per-message objects, no delegates allocated per message, no LINQ, no `async` state machines on the hot
  path (`ValueTask` + pooled `IValueTaskSource` slots only where the caller opts in to tracking).
* Hot loops are written for sequential memory access; SIMD is used where it measurably helps (packing scans,
  header batch parsing, checksums) and only after a benchmark proves it.
* Default preallocation is about 16 MiB per process plus a per-peer budget (default 256 KiB); all limits are
  configurable and all exhaustion paths are explicit, counted outcomes.
