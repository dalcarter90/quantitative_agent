# 0022: The C++ order book: instrument updates in the stream, a fixed capacity, and what makes it stale

Status: Accepted (owner decisions, 2026-11-17); the `InstrumentUpdate` layout and the implementation choices await the owner's review, see "Review"
Date: 2026-11-17
Amends: 0020 (a ninth `MarketEvent` kind), 0021 (the converter emits it; its Review is confirmed here)

## Context

Phase 4a built the converter from stored data to `MarketEvent`s (ADR 0021) and
left Phase 4b, the C++ order book, with two questions for the owner:

- **How a precision change reaches the C++ book.** Kraken can change a pair's
  printed precision mid-session, and its checksum is over the printed digits:
  a mantissa divided by `10^(exp - precision)`. The reference book takes the
  metadata in force from the store for every message. `MarketEvent` carried no
  metadata, and the converter passed such a change through unseen.
- **The book's capacity.** A Coinbase BTC-USD book is about 38,500 levels (up
  to 22,639 on one side in the fixture). A book that allocates nothing after
  startup needs a capacity per side, and a rule for a message that would
  exceed it.

ADR 0021's nine Review items were also waiting for the owner.

## Decision

The owner decided on 2026-11-17:

1. **ADR 0021's Review items are confirmed**, all nine. Its decision 8 (trades
   are not converted) stands for now, with a condition recorded in the
   roadmap's Phase 7 criteria: the converter must handle trades before the
   replay engine needs them.
2. **Precision changes travel in the stream.** `MarketEvent` gains a kind for
   an instrument update. The converter emits it at the record where the change
   arrives, so the C++ book switches Kraken's checksum digits at exactly that
   point. Its field layout is to be proposed here, preferring fields the kind
   does not otherwise use over the last two reserved bytes. A test with a
   mid-session precision change must match the reference book.
3. **The book's capacity is fixed per side and configured per instrument at
   startup**: 65,536 levels per side by default, Kraken sized to its depth.
   Nothing is allocated after startup. A message that would exceed the
   capacity marks the book stale, with its own reason and a counter, and the
   book recovers at the next snapshot that fits. Tested with a deliberately
   small capacity.
4. **The borderline slow tests stay unmarked**: the eight Coinbase tests that
   ran at 5.0 to 5.8 s in Phase 4a's last run and under 4.8 s in the measured
   one.

## How it is built

### `InstrumentUpdate` (proposed layout)

A ninth kind, `EventKind::InstrumentUpdate = 8`. It carries a whole
`InstrumentMeta`, the same 24 bytes of content the book is configured with,
in fields the kind has no other use for. No reserved byte is used: 82 and 83
stay reserved, and no offset moves.

| Field | In an `InstrumentUpdate` |
|-------|--------------------------|
| `instrument` | The instrument the metadata is for |
| `price` | `tick_size`, a price mantissa at `price_exp` |
| `qty` | `lot_size`, a quantity mantissa at `qty_exp` |
| `level_index` | The four scales, one byte each from the lowest: `price_exp`, `qty_exp`, `price_precision`, `qty_precision` (the order of `InstrumentMeta`'s bytes 4 to 7) |
| `recv_ts`, `seq`, `source` | The record the version arrived in, on its log's stream source |
| `event_ts`, `flags` | The version's venue time; both venues' metadata has none, so it is the receipt time with `kEventTsFromReceipt`. `kHasMono` as for any event. No other flag |
| `venue_seq`, `trade_id`, `checksum`, `depth`, `side`, `reason`, `reserved` | Zero (`Unknown`, `NoReason`) |

- `qa::set_instrument_meta(event, meta)` writes these fields and
  `qa::instrument_meta(event)` reads them back; nothing else packs or unpacks
  the scales. `qa::scales_fit(meta)` is the rule both use: each exponent 0 to
  18, each precision 0 to its exponent, as in the store's schema.
- `MarketEvent.from_bytes` accepts kind 8 and refuses, for it, scales that do
  not fit, a side, a venue number, trade id, checksum or depth, and any flag
  but the two above. The Python binding gains `MarketEvent.instrument_update(meta)`
  and `event.instrument_meta()`.
- It is not a stream control event (`is_stream_control` is unchanged): it
  carries values in `price` and `qty`.

### The converter

`convert` yields one more kind of group, `step="instrument"`: one
`InstrumentUpdate` for **every stored `instruments` row**, at its record, on
the log's stream source (`(log_id, "ws")`), whichever channel the metadata
came on (Kraken's `ws`, Coinbase's `rest:product`): like a session event, a
metadata version belongs to the log's stream. At one record the order is
control events, then the instrument update, then levels.

Every version is emitted, not only those that differ from the one before: a
stored row is one event, as a level row and a stream event row are, and the
stream then says by itself which metadata is in force, as the reference book's
rule does (the latest version of the message's log at or before its record). A
session's first version arrives before its first book message, because the
recorder fetches metadata first and the adapters refuse market data without
it (ADR 0019).

The converter refuses, before yielding anything, an `instruments` row whose
exponents are outside 0 to 18 or differ from the others', or whose precision
does not fit its exponent.

### The book (`qa::OrderBook`, `qa/order_book.hpp`)

One book per instrument, built at startup from an `OrderBookConfig`: the
`InstrumentMeta` (exponents, the precisions in force at startup, tick and lot
sizes), the venue's integrity rule (`BookCheck::KrakenChecksum` or
`BookCheck::CoinbaseSequence`) and the capacity per side. `apply(event)` takes
one `MarketEvent` and never throws, allocates, locks or reads a clock.

**Storage.** Each side is a ring buffer of `(price, quantity)` levels kept in
order, best first, allocated once in the constructor. A level is found by
searching from the best end; inserting or removing one moves the levels on
the nearer side of it, so a change near the top of the book moves few. A
snapshot arrives best first on both venues, which is an append at the worse
end. Cutting to a depth drops the worse end.

**Capacity.** `capacity_per_side` defaults to `OrderBook::kDefaultCapacity`,
65,536. A venue that keeps a cut book is sized from its depth with
`OrderBook::capacity_for_depth(depth)`, which is **twice the depth** (50 for
Kraken's 25): an update's levels apply in order and the book is cut only when
the message ends, so a side briefly holds more than the depth. On the Kraken
fixture 4,714 of 13,781 updates hold 26 levels on a side before the cut, so a
capacity equal to the depth would go stale on a third of them. Twice the
depth holds a message that replaces a whole side before removing the old one.
A level that would be a side's `capacity + 1`th makes the book stale
(`CapacityExceeded`) there and then; the rest of its message is not applied.

**When a message takes effect** (ADR 0020). An update's levels are applied as
they arrive and the message takes effect at its `kLastInMessage` level, where
the book is cut to the message's depth (if it has one) and the venue's check
runs. A snapshot is built from its frame: nothing changes at `SnapshotBegin`;
the first level says whose snapshot it is; a stream snapshot empties the book
and applies its levels, and takes effect at `SnapshotEnd`, cut and checked the
same way; a check snapshot's frame changes nothing. The header (`venue_seq`,
`checksum`, `depth` and their flags) is read from the `kLastInMessage` level.

**What makes the book stale.** A stale book is emptied and holds nothing
until it recovers, as the reference book's is discarded.

| `StaleReason` | Made stale by |
|---------------|---------------|
| `SequenceGap` 1, `SequenceRepeat` 2, `SequenceOutOfOrder` 3, `ChecksumMismatch` 4, `MessageRejected` 5 | A `StreamStale` event with the `StreamReason` of the same number; and the book's own check: Kraken's checksum computed from the book differs from the message's (4); a Coinbase book message's number equals (2) or is below (3) the previous one's |
| `DeltaBeforeSnapshot` 64 | An update with no book: none since the start or the last reset |
| `IncompleteMessage` 65 | A message whose events did not all arrive in order: a frame whose count does not match its levels, a level out of position or outside a frame, another record's event inside a frame or an update, a `kLastInMessage` flag anywhere but on the last level |
| `CapacityExceeded` 66 | A level that does not fit the side's capacity |
| `InvalidEvent` 67 | An event the book cannot act on: a level with no side or a negative quantity; a message without the header its venue's check needs (no checksum on Kraken, no number on Coinbase); an empty snapshot frame, which says neither whose it is nor carries a header; an `InstrumentUpdate` whose scales do not fit or whose exponents are not the book's; a `StreamStale` without a stale reason; a kind the book does not know |

- The reference book has the first two rows and no others. Where it raises an
  error that is not an integrity failure (a level with no side, a negative
  quantity, a Kraken message without a checksum), the C++ book, which cannot
  throw, goes stale with `InvalidEvent`. The other three reasons are about
  things the reference cannot see: events arriving one at a time, and a
  capacity.
- The numbers 1 to 5 are `StreamReason`'s (a `static_assert` holds them
  equal); the book's own start at 64, clear of the reasons `StreamReason` will
  gain (Phase 5 adds `QueueOverflow`).
- **Recovery** is the reference's: only a complete stream snapshot that
  passes its check ends staleness. A snapshot that fails (its check, its
  frame, the capacity) leaves the book stale with that reason. Updates are not
  applied to a stale book. A `StreamReset` empties the book and neither causes
  nor ends staleness. An `InstrumentUpdate` is taken while stale, so the
  recovering snapshot is checked at the precision in force.
- **One fault is one failure.** After a frame fails, the rest of that frame's
  events are skipped without being counted again.

**What `apply` returns**: a `BookResult` of a `BookEffect` and a
`StaleReason`.

| `BookEffect` | Meaning |
|--------------|---------|
| `Pending` | Part of a message that has not taken effect yet |
| `Applied` | A message took effect and the book is fresh |
| `Recovered` | A snapshot took effect and ended a stale period |
| `Skipped` | Not applied: an update while stale, or the rest of a message that already failed |
| `WentStale` | The event made the book stale, or found a new reason while it was; the result's `reason` says why |
| `Reset` | A `StreamReset` emptied the book |
| `Instrument` | An `InstrumentUpdate` replaced the metadata in force |
| `Ignored` | Nothing for this book: a complete check snapshot, a trade, an event of another instrument |

A message, stale or reset step of the reference book is one group of events
of the converter, and the step's `reason`, `stale` and `recovered` are read
from the results of that group: that is how the tests compare the two.

**What the book reports**: `is_stale()`; `has_book()` (a snapshot has taken
effect since the start, the last reset and the last failure: the book can be
read); `stale_reason()` (what began the current stale period);
`stale_count(reason)` (how many failures of each reason since it was built:
the counter decision 3 asks for); `top`, `best_bid`, `best_ask`, `size`,
`quantity_at`, which show nothing unless `has_book()`; `checksum()` (Kraken's,
of the book as it is); `instrument()` (the metadata in force).

**Kraken's checksum** is the reference's recipe, digit for digit: the ten
best asks then the ten best bids, each level as the decimal digits of
`price // 10^(price_exp - price_precision)` then of the quantity likewise
(floor division, as Python's), CRC32 as zlib computes it. `qa/crc32.hpp` is a
table-driven CRC32; no dependency is added.

**Python** (`quantagent._core`): `OrderBook(instrument, check, capacity)`,
`apply`, the queries above with prices and quantities as integer mantissas,
`BookCheck`, `StaleReason`, `BookEffect`, `BookResult`.

## Alternatives considered

**The instrument update's layout**

- **Precisions in the two reserved bytes** (82 and 83). The smallest change,
  but it spends the last reserved bytes, and the owner asked to keep them. It
  would also carry two of the four scales, so a book could not see an update
  whose exponents contradict its own.
- **The scales in `checksum`.** Bytes 64 to 71 would then be an
  `InstrumentMeta`'s first eight bytes exactly. But `checksum` has a presence
  flag with one meaning, and a field read as a checksum by one kind and as
  scales by another invites a mix-up. `level_index` has no flag and already
  means something different per kind (a position, a count, an event index).
- **The scales in `depth`** (16 bits: the precisions only). Same objection as
  the reserved bytes, plus a flag.
- **`venue_seq` or `trade_id`** (64 bits each). Room to spare, both flagged,
  both signed or unsigned 64-bit integers a reader would have to unpack.
- **Only the precisions, not a whole `InstrumentMeta`.** The tick and lot
  sizes change with the same venue message, and pre-trade risk (Phase 9) will
  want the tick in force; `price` and `qty` are already of the right types.
- **Only versions that differ.** The converter would have to compare each
  version with the one before, across logs, and the book's metadata before
  the first difference would come from somewhere else. Every version costs
  one event per session.
- **A metadata path beside the stream** (a setter on the book). It would not
  say at which record the change takes effect, which is the point of
  decision 2.

**The book**

- **A sorted array with the best level at the end.** Simplest, and cheap near
  the top, but a snapshot that arrives best first inserts every level at the
  front: about 180 million level moves for one Coinbase snapshot.
- **A balanced tree or skip list in a node pool.** No moves, but several
  times the code, and worse locality for the top of the book.
- **A capacity equal to the depth** for a cut book: stale on a third of
  Kraken's updates (above). **Cutting after every level** instead of at the
  message's end would fit in the depth but is not the reference's book: an
  insert followed by a removal in one message would lose the level the insert
  pushed out.
- **Dropping the worst level when a side is full.** Exact only if nothing
  later in the message removes a better one; the owner's rule (stale, with a
  reason and a counter) is simpler and never wrong.
- **The book's own reasons in `StreamReason`.** That enum is what events
  carry; `from_bytes` and `reason_fits` would need a class of reasons no event
  may hold.
- **One `Invalid`-like reason per cause** (no side, no header, bad scales).
  Four more reasons for inputs no adapter or converter produces.
- **A broken check-snapshot frame leaves the book alone.** A check snapshot
  never changes the book while its frame is whole; but a frame cut short
  means events were lost, and the book cannot know that only the probe's
  were.
- **Tying the book to its snapshot's source.** The reference has no such
  rule, and a new stream is announced by a `StreamReset` (ADR 0020).

## Consequences

- `MarketEvent` has nine kinds; `from_bytes` accepts kind 8 with its own
  rules. Its last two reserved bytes are still free.
- The converter's groups include `instrument` steps, which are not steps of
  the reference book: a user comparing the two skips them, as it skips
  `check` groups.
- Phase 6's feed handlers emit an `InstrumentUpdate` for each metadata message
  of their instrument, and are compared with the converter on it too.
- The C++ book's precisions follow the stream; its exponents never change.
- A book costs 16 bytes per level per side, rounded up to a power of two:
  2 MB for the default capacity, under 2 KB for Kraken at depth 25.
- Phase 9's stale-book check reads `is_stale()`; a stale book holds nothing
  to price an order against.
- Phase 7's criteria gain the converter's trades (decision 1).

## Review

Choices made while building it, awaiting the owner:

1. **The `InstrumentUpdate` layout** above: kind 8; the tick size in `price`,
   the lot size in `qty`, the four scales in `level_index`; no reserved byte.
2. **Every stored metadata version is one event**, on the log's stream
   source, after its record's control events and before its levels.
3. **`capacity_for_depth` is twice the depth**; the measured need on the
   Kraken fixture is depth + 1.
4. **A level that does not fit makes the book stale at once**, even if a
   later level of the same message would have made room.
5. **The book's own stale reasons** and their numbers (64 to 67), in a
   `StaleReason` separate from `StreamReason`; `InvalidEvent` where the
   reference raises a non-integrity error.
6. **An empty snapshot frame makes the book stale** (`InvalidEvent`): nothing
   says whether it is the stream's, and no header verifies it.
7. **Any broken frame makes the book stale**, a check snapshot's included.
8. **The header is read from the `kLastInMessage` level**; the book does not
   compare the other levels' copies of it.
9. **An `InstrumentUpdate` with other exponents is refused** (`InvalidEvent`)
   and the metadata in force stays.
10. **Events of another instrument are ignored**, not failures.
11. **The queries show nothing unless the book can be read**, and return
    integer mantissas in Python.
12. **The constructor throws** on a configuration it cannot build (a capacity
    of 0 or above 2^24, scales that do not fit). It runs at startup, not on
    the hot path.
13. **The allocation test has its own binary** (`qa_alloc_tests`), so only it
    runs on a replaced `operator new`; Phase 5 builds the reusable guard.
14. **Existing tests changed to the new kind, none weakened**, listed in the
    roadmap's Phase 4b notes.
