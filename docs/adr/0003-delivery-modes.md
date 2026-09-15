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
