# ADR 0003 — Explicit delivery modes instead of orthogonal flags

**Status:** accepted (2026-09-15)

## Context
Independent `Reliable`, `Ordered`, `Latest` booleans produce ambiguous combinations ("reliable + latest +
ordered"?). Games need a small number of well-defined contracts.

## Decision
Six modes, each with one implementation (see ARCHITECTURE section 5 and PROTOCOL): `UnreliableUnordered`,
`UnreliableSequenced`, `ReliableOrdered`, `ReliableUnordered`, `ReliableLatest`, `Bulk`.
Ordering scope is channel; replacement scope is channel + key. State vs. event is the application's
choice of mode: replaceable state goes on sequenced/latest channels, events on reliable channels.
Message type is not a header field; a channel *is* a type.

## Consequences
* Header is 1 to 6 bytes; nothing is repeated per message that the channel table already fixes.
* `ReliableLatest` needs its own ack/retry protocol (control datagram type 3), an accepted cost.
* `ReliableUnordered` uses per-flush-group streams; ordering inside a group is a side effect, not a promise.
* Replacement is per key but the sequence (or version) counter is per channel, so "newer for this key" cannot be a
  serial comparison of two values of the key — a key may idle while the counter runs past half its range. Receivers
  evaluate per-key ordering on a per-channel 64-bit extended sequence (PROTOCOL section 8), and `ReliableLatest`
  completes a value only on an ack naming exactly the version last transmitted for the key, not on a cumulative one
  (added 2026-09-30).
* What a channel costs the receiver when the application neither handles nor drains it follows from the mode's
  contract (PROTOCOL section 7, added 2026-09-30): `UnreliableUnordered` / `UnreliableSequenced` keep a bounded
  backlog and drop their own oldest messages, counted, without touching another channel; coalescing channels and
  `ReliableLatest` keep one value per key; `ReliableOrdered` / `ReliableUnordered` may not drop, so an undrained
  one ends in back-pressure — which today reaches every stream channel and the receive ring, not only the
  undrained channel — until it is drained or gets a handler; `Bulk` never waits in a queue (its data goes to the
  router's target).
