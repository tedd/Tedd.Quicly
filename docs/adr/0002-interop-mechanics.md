# ADR 0002 — Interop mechanics: LibraryImport, function pointers, no GC-transition suppression on MsQuic calls

**Status:** accepted (2026-09-15)

## Context
Modern .NET offers several ways to make native calls cheaper: source-generated `[LibraryImport]` (no runtime
IL stub, AOT/trim friendly), `[assembly: DisableRuntimeMarshalling]` (blittable-only, direct calls),
`delegate* unmanaged[Cdecl]` function pointers (a raw indirect call), `[UnmanagedCallersOnly]` callbacks
(no reverse-P/Invoke delegate thunk), and `[SuppressGCTransition]` (skips the cooperative-to-preemptive
GC mode switch around a call).

## Decision
* The interop assembly sets `DisableRuntimeMarshalling`.
* The two exports (`MsQuicOpenVersion`, `MsQuicClose`) use `[LibraryImport]`.
* Every other MsQuic function is called through the `QUIC_API_TABLE` as a `delegate* unmanaged[Cdecl]`.
* Callbacks are `[UnmanagedCallersOnly(CallConvs = [typeof(CallConvCdecl)])]` static methods.
* **`SuppressGCTransition` is not applied to MsQuic calls.** It is only sound for calls that are trivially
  short, never block, never take locks that another (possibly GC-suspended) thread might hold, and never
  call back into managed code. MsQuic's `StreamSend`, `DatagramSend`, `SetParam` and friends take internal
  locks and can dispatch callbacks inline; suppressing the transition there risks stalling a GC (the
  "GC freeze" failure mode), the opposite of the intent. The transition costs roughly 10 to 20 ns and is
  paid once per *batched* submission (one `StreamSend` carries many gathered buffers), so it is not on the
  per-message path anyway.

## Consequences
Per-message cost is dominated by memory traffic, not interop. The design keeps interop calls O(flushes),
not O(messages).
