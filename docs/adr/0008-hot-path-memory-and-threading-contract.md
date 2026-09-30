# ADR 0008 — Hot-path memory and threading contract

**Status:** accepted (2026-09-15), supersedes the informal statements in ADR 0004/0005

## Invariants

1. **Nothing the transport was given a pointer to moves or is reused before the matching completion.**
   `SendEntry` (64 bytes, native memory) holds its adjacent `QUIC_BUFFER` pair (header segment + payload
   segment), so a single-entry datagram passes `&entry.Header` as a two-element array. The header bytes live in
   a cold native array of 32-byte header blocks indexed by slot (the largest datagram header is 24 bytes).
   Entries are not adjacent `QUIC_BUFFER`s, so each stream submission copies the entries' segment pairs into a
   per-submission contiguous `QUIC_BUFFER[]` taken from a native segment arena; that array, the header blocks and
   the payloads stay reserved until `OnStreamSendCompleted`. Datagrams stay reserved until the *final* datagram
   send state (`Acknowledged`, `AcknowledgedSpurious`, `LostDiscarded`, `Canceled`), even though the payload
   block is released at `Sent`.
2. **Contexts are generation-tagged**: `(generation << 32) | slot`. Every callback validates the generation;
   a mismatch is ignored and counted.
3. **Publish before the call.** A completion can arrive on the transport thread before the send API
   returns on the game thread (loopback RTT is microseconds). The slot protocol is: allocate → fill →
   publish (`State = InFlight`, token, lease, with release semantics) → call the transport → never touch the
   slot from the game thread again until its completion has been observed through the `CompletionRing`.
   If the call returns failure, no completion follows and the caller unwinds the slot itself.
4. **One owner per field.** Send-side arrays are written by the game thread; receive-side arrays by the
   transport thread (MsQuic serialises a connection's callbacks; worker identity may change, so ownership is
   "the current callback"). `SendEntry.State` is the only dual-thread field and changes only by CAS:
   Free → Submitted → [Cancelling] → Completed → Free. The game thread reuses a slot only after observing the
   completion through the `CompletionRing`; it never polls `State`.
5. **Rings are SPSC** per peer (`ReceiveRing`, `CompletionRing`, slab return ring): cached remote index,
   `Volatile.Read/Write` acquire/release, 128-byte padding between head, tail and entries, elements in a
   `NativeArray<T>` like every other hot struct array (invariant 12). One deliberate exception (2026-09-18): the
   `CompletionTable` free list (an `MpscRing<int>`) lives in one 64-byte aligned block on the **pinned object heap**, not in
   native memory, because a slot may be returned by whichever thread consumes the last wait on it — after `Dispose` too — and
   the returning thread reaches the ring through that slot, so reachability keeps the memory valid for the return without the
   Dekker handshake the native list needed. Its capacity is therefore bounded by the largest array (2^26 slots). Genuinely multi-producer paths
   (`ThreadSafeSend`, the server's "peers with work" queue) use the Vyukov bounded MPMC algorithm so a stalled
   producer never blocks the consumer. `CompletionRing` capacity = **two per send entry** (one early `Sent`
   notice plus one final completion, so `2 × send table`; capacity is already a power of two, and asking for one
   more slot would double the ring), so it cannot overflow.
   The cached remote index is the *contract*, not an optimisation: a producer that also holds ring reservations
   checks room with `SpscRing.HasRoomFor`/`TryEnqueueReserving`, which read the consumer's index only when the
   producer's private snapshot says the ring is full, and occupancy high-water marks are refreshed only when a
   new maximum is suspected — so a received message costs no coherence traffic.
   **A ring's `out` value is undefined when `TryDequeue` returns false**: the failure path does not write it
   (`Unsafe.SkipInit`, annotated `[MaybeNullWhen(false)]`), because writing `default` there produces a
   *plausible* value — for an index-typed element a valid index — and a caller that read it anyway once
   recycled a live record.
6. **Keyed coalescing channels use mailboxes, not ring entries**: the transport thread writes into a fresh
   lease and `Interlocked.Exchange`s it into the key's mailbox; a non-negative previous value is a lease the
   game thread never saw and is freed immediately by the transport thread; the game thread claims with
   `Exchange(-1)`. A per-channel dirty bitset (scanned with `BitOperations`/`Vector256`) tells the game thread
   which keys changed. Bounded memory, no back-pressure, latest-wins.
7. **Reentrancy.** MsQuic executes API calls inline when made from the connection's own callback and can
   deliver events re-entrantly (`DatagramSend`/`StreamSend` → `Canceled`, `StreamStart` → `StartComplete`).
   Transport-thread code MUST NOT hold a ring reservation, an in-flight CAS or a partially written struct
   across any MsQuic call; re-enterable callbacks MUST be idempotent (generation tags). `INLINE` shutdown
   flags are never used. `ConnectionClose`/`StreamClose` run on a dedicated shutdown path after
   `SHUTDOWN_COMPLETE`, never inside `Poll`, and never from a callback thread. A debug-only thread-static
   "inside callback" flag asserts this.
8. **Callbacks never throw.** Every `[UnmanagedCallersOnly]` body is `try { … } catch { record; poison peer;
   return QUIC_STATUS_INTERNAL_ERROR; }`; strict mode fails fast instead.
9. **Time-driven work has one owner**: retries, expiry, pings, heartbeat and group flushes run inside
   `Flush`/`Poll` on the game thread; `now` is read once per call; there are no `System.Threading.Timer`s.
   `NextDeadline` lets a host sleep precisely. Relative time limits of queued sends (message expiry) are
   anchored at the `now` of the first scheduler pass after admission (`PeerCore.StampExpiry` /
   `ResolveExpiry`): admission reads no clock, and it never borrows the stamp of an earlier pass, which can
   be arbitrarily old. The one clock read on a send path outside a scheduler pass is `SendRequestAsync`, once
   per call, for the request's timeout (an `Immediate` send runs a pass, which reads `now` once like every
   pass).
10. **Completion delivery is a mode**: `PollOnly` (continuations inline in `Poll`) or `ThreadPool`
   (`RunContinuationsAsynchronously` from the transport thread). Awaiting from an `async` method allocates
   the caller's state machine only when it suspends; the library's own paths are 0 B.
11. **Pinning is explicit**: `SendPinned`/`SendOwned`/`SendGather(BufferLease)` never create handles.
    `SendBorrowed(ReadOnlyMemory)` stores the `MemoryHandle` as `nint` in a side table indexed by slot; it is
    the documented convenience path.
12. **Struct arrays are reference-free**, 64-byte aligned, in native memory (or the pinned object heap),
    split hot/cold and by owner; scanned fields are structure-of-arrays.
13. **Diagnostics are allocation-free**: counters are plain `long` fields incremented by the owning thread
    and snapshotted into `PeerStatistics`; any `EventSource` use takes primitive arguments only and is
    guarded by `IsEnabled()`. A snapshot reads the *other* thread's counters with a single `Volatile.Read` per
    field and is therefore allowed to cross the ownership rule of invariant 4 — it never reads two fields as one
    consistent pair and nothing in the library decides anything from what it read. The same licence covers an
    engine's diagnostic accessors for a field the transport thread owns (for example the fragment engine's
    reassembly count): one volatile read of one counter, exposed for statistics and tests only.
14. **Interop cost is O(flushes + packed datagrams)**, not O(messages): buffered datagram sends are packed
    per peer (container sized from the current max datagram payload) and stream sends are gathered per
    stream per flush.
