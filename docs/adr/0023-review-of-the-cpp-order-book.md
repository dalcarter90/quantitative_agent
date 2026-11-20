# 0023: Review of the C++ order book: an update unread until it takes effect, one stream per book, a recorded benchmark

Status: Accepted (owner decisions, 2026-11-17); the implementation choices confirmed by the owner (2026-11-17, see "Review"), with one change (see "Owner's review": while the book is bound, another source's stale and reset events are ignored too), whose own choices were confirmed on 2026-11-19 with one change (see "Owner's second review": a session start applies from any source)
Date: 2026-11-17
Amends: 0022 (an update cannot be read until it takes effect; the book is bound to one stream; a tick or lot size below 1 is refused; its Review is confirmed here, item 11 amended), 0021 (two metadata versions at one record are emitted in the reference book's order). Data core: the Kraken adapter refuses a record with two versions of the instrument (`docs/data-core-spec.md` section 7)

## Context

The adversarial review of Phase 4b (`docs/reviews/2026-11-cpp-order-book.md`)
found six things in the C++ order book and what feeds it, four of them with
failing tests (`python/tests/data/test_review_2026_11_order_book.py`), and left
four notes for later phases:

1. Between an update's levels the book reads as fresh and shows a book no
   step of the reference has: crossed on Coinbase (57 updates of the fixture,
   790 in an hour), deeper than its depth on Kraken.
2. The book follows whatever stream it is given: two streams of one
   instrument, interleaved, merge into a book that is neither.
3. The benchmark's synthetic Coinbase stream is not the recorded one: real
   updates cost about six times the reported mean, and no figure gives a tail.
4. A stale reason `StreamReason` gains (Phase 5 plans `QueueOverflow`) would
   reach the book with no name and no counter, and nothing would fail.
5. Two metadata versions at one record are resolved three different ways (the
   reference, the converter and the C++ book, the adapter).
6. A tick or lot size the store refuses (below 1) is taken by the book.

ADR 0022's sixteen Review items were also waiting for the owner; the review
found twelve sound, three sound with a caveat (2, 6 and 14) and one, item 11,
not sound as built (finding 1).

## Decision

The owner decided on 2026-11-17:

1. **An update is not readable until its last level**, as a snapshot already
   is not until its end. Consumers act only on results that say a message
   took effect (finding 1).
2. **The book binds to the source of the snapshot that built it.** A snapshot
   from another source rebinds it; an update from any other source is ignored
   and counted, and does not make the book stale. Phase 7's criteria gain:
   the replay engine replays one stream per instrument book (finding 2).
3. **A benchmark driven by the committed Coinbase fixture's converted
   events**, reporting p50 and p99 as well as the mean; the Kraken checksum
   benchmark's comment fixed. No optimization (finding 3).
4. **A compile-time check that every `StreamReason` maps to a named stale
   reason**, so a new reason fails the build until it is mapped (finding 4).
5. **The adapter rejects a record carrying two metadata versions of the same
   instrument** (`MESSAGE_REJECTED`) (finding 5).
6. **A tick or lot size below 1 is refused** by the book's constructor, by
   `MarketEvent.from_bytes` and in an `InstrumentUpdate` (finding 6).
7. **The review's notes become later phases' criteria**: N1 Phase 5's, N2 and
   N4 Phase 7's, N3 Phase 9's.
8. **ADR 0022's Review items are confirmed**, all sixteen, item 11 amended by
   decision 1. The review's tests pass with their markers removed.

## How it is built

### Nothing is read while the book is being changed (decision 1)

- From an update's first level the book cannot be read (`has_book()` is
  false) until the update takes effect at its `kLastInMessage` level, exactly
  as from a stream snapshot's first level until its `SnapshotEnd`. Its levels
  still apply to the live sides one by one; nothing is copied. Every query
  (`top`, `best_bid`, `best_ask`, `size`, `quantity_at`) shows nothing, and
  so does `checksum()`, which is then an empty book's (0); the book's own
  check computes it from the sides as they are. A snapshot frame's begin, and
  a check snapshot's whole frame (which changes nothing), leave the book
  readable as it was.
- An update whose last level is lost stays unreadable until the next event
  fails it (`IncompleteMessage`).
- **What a consumer acts on.** A feature, pre-trade risk or a published
  snapshot reads the book only after a result that says a message took
  effect: `Applied`, or `Recovered` (a snapshot took effect and ended a stale
  period, which for a reader is the same). After `Pending` (inside an update
  or a stream snapshot), `WentStale`, `Reset` and `Skipped` the book shows
  nothing; after `Ignored` and `Instrument` it is unchanged.

### One stream per book (decision 2)

- **Bound.** The book is bound to a source (`bound_source()`) from the first
  level of a stream snapshot, when the book is emptied to be rebuilt from it,
  until a reset or a failure empties it again. A stream snapshot of any
  source is taken as before and binds the book to its own source; a check
  snapshot binds nothing.
- **An update of another source** (a `BookDelta` whose source is not the
  bound one) is `Ignored`: not applied, not checked, and it interrupts no
  message in progress, as an event of another instrument does not. It is
  counted once, at its first level, in `other_source_updates()`.
- **Unbound** (no book: at the start, after a reset or a failure) an update of
  any source is what it was: an update with no book
  (`DeltaBeforeSnapshot`), or skipped while stale. So the reference book's
  rules hold for every stream it can see.
- **Only updates are judged by their source.** A `StreamStale`, a
  `StreamReset`, an `InstrumentUpdate` or a snapshot frame of another source
  is taken as before. Phase 7's new criterion is what keeps a second stream
  out of a book. *(Changed by the owner's review, below: while the book is
  bound, a `StreamStale` or `StreamReset` of another source is ignored and
  counted too.)*
- **On converted data nothing changes.** Each log begins with its session
  start on its own stream source, which unbinds the book; the log's snapshot
  binds it to that source, and all its updates are of it. A test replays two
  logs one after the other and shows the book bound to each in turn, nothing
  ignored, and every step equal to the reference's.

### The recorded benchmark (decision 3)

- `cpp/bench/export_events.py` normalizes the committed Coinbase fixture in
  memory (the Phase 1c adapters), converts it (the Phase 4a converter) and
  writes every event, 88 bytes each, to `data/bench/coinbase-BTC-USD.events`
  (110,986 events, 9.8 MB, git-ignored). `BM_ReplayCoinbaseFixture` reads it
  from there (`QA_BENCH_DATA`, set by `cpp/bench/CMakeLists.txt`) and is
  skipped with an error saying to run the script until it exists. A Python
  test shows the file is the converter's events byte for byte and that the
  stream replays twice on one book without going stale.
- Each iteration replays the whole stream on one book; its session start
  resets the book, so every pass is the venue's. Each update message is timed
  on its own with the CPU's timestamp counter, calibrated against the steady
  clock (whose resolution on Windows is 100 ns, too coarse for a 1.6 µs
  median). The counters give, per update message, `p50_ns`, `p99_ns`,
  `max_ns` and `mean_ns`, with `levels/update`, `updates/pass` and
  `snapshot_ms`. A timer reading costs a few ns and is included.
- Baseline (AMD Ryzen 5 7600X3D, Windows, Release, three repetitions): 2,701
  updates a pass, 12.6 levels an update; per update a mean of 2.55 µs, p50
  1.63 µs, p99 11.4 µs; the maximum varies from run to run (75 µs median of
  three); the snapshot 1.70 ms. This agrees with the review's measurement over
  the hour through the binding (mean 2.74 µs, p50 1.90, p99 11.4).
- `BM_KrakenChecksum`'s comment now says what it measures: plain scales
  (every divisor 1) over the synthetic stream's digits, not Kraken BTC-USD's.
  `BM_ApplyUpdate_Coinbase38k_Top`'s comment no longer calls updates near the
  top "what most updates of a liquid book are": it is a best case.

### Every stale reason mapped, at build time (decision 4)

- `qa::stale_reason_of(StreamReason)` (`qa/order_book.hpp`) maps a
  `StreamStale`'s reason to the book's `StaleReason`: a switch with a case for
  every `StreamReason` and no default, inside a region where a missing case is
  an error (MSVC's C4062, made an error by `#pragma warning`; GCC and Clang's
  `-Wswitch`). A reason added to `StreamReason` fails the build there until it
  is mapped to a `StaleReason` or listed as one no `StreamStale` carries.
- `static_assert(detail::every_stale_reason_is_mapped())` holds the mapping to
  `reason_fits` over all 256 byte values: a reason a `StreamStale` may carry
  maps to the `StaleReason` of the same number, and no other reason maps to
  one. The book's counters (`slot_of`) are a switch of the same kind over
  `StaleReason`, and a `static_assert` requires one counter per reason, every
  mapped reason among them.
- So Phase 5's `QueueOverflow` cannot be added without a `StaleReason` of its
  number and a counter. A Python test iterates every `StreamReason` that
  `from_bytes` lets a `StreamStale` carry and requires the book to report it
  by the same name and number and count it, which also catches a reason the
  binding does not name.

### Two metadata versions at one record (decision 5)

- **The Kraken adapter** refuses an `instrument` message that lists the
  recorded pair more than once, with reason `two_metadata_versions`, before
  any of its versions is made current: like any refused record it is a
  `MESSAGE_REJECTED` event on its connection (the stream, where Kraken's
  `instrument` channel arrives), which makes both books stale there, and the
  version before it stays in force. Two identical listings are refused too.
  Coinbase's product record holds one product and cannot carry two. No
  recorded Kraken message does this: the fixture and the one-hour recording
  list BTC/USD at most once per message, so no stored data changes.
- **The converter** emits a record's versions in the reference book's order
  (its four scales ascending, then tick and lot size), so the last one, which
  the C++ book keeps, is the one the reference takes, whatever order the
  session returned the rows in (`PITSession.instruments` does not order rows
  of one record). A store written before the adapter refused such records, or
  hand-built data, can still hold them; the review's test builds them by hand.

### Tick and lot sizes (decision 6)

- `qa::meta_fits(meta)` (`qa/types.hpp`) is the store's whole rule for
  metadata: `scales_fit`, and a tick and lot size of at least 1. The
  constructor uses it (`std::invalid_argument`, a `ValueError` in Python), and
  so does the book for an `InstrumentUpdate` (`InvalidEvent`, the metadata in
  force kept). `from_bytes` refuses an `InstrumentUpdate` whose tick or lot is
  below 1.
- The `InstrumentMeta` binding and `MarketEvent.instrument_update(meta)` still
  build any tick and lot: they make values, and the three places above judge
  them.

### The notes (decision 7)

Added to the roadmap: N1 (a Coinbase snapshot is a 1.7 ms burst of core time)
to Phase 5's latency benchmark; N2 (an event that interrupts a message loses
its own effect from the result) and N4 (a book's replay starts at its log's
beginning) to Phase 7; N3 (the book records no time for freshness) to Phase 9.

## Alternatives considered

**Finding 1**

- **An `in_message()` query and a rule that nothing reads between a
  message's events** (the review's alternative). Every reader would carry the
  rule; a forgotten check is a silent error.
- **Showing the book as it was before the message** until it takes effect.
  It needs a second copy of the sides or an undo log: more memory, more code,
  and a cost on every update, where the snapshot rule already shows nothing.
- **Leaving `checksum()` as "of the book as it is".** One query would show
  what the others hide.

**Finding 2**

- **Binding at the snapshot's end** instead of its first level. An update of
  the old stream arriving inside the new snapshot's frame would interrupt it.
- **Keeping the binding through a reset or a failure.** An update of a new
  stream before its snapshot would be ignored instead of being an update with
  no book, unlike the reference.
- **An update of another source makes the book stale** (the review's first
  option). The owner chose to ignore and count it.
- **Judging control events, metadata and frames by source too.** Not decided;
  a stream's control events are what make a book stale or reset it, and
  Phase 7's criterion keeps a second stream out instead. *(Decided by the
  owner's review, below, for stale and reset events: ignored while bound.)*
- **Counting every ignored event.** A 500-level update would count 500.

**Finding 3**

- **Committing the event file** (9.8 MB, made from the committed 0.96 MB
  fixture in seconds).
- **Generating it during the build.** It needs the built Python module, which
  the same build makes.
- **Measuring through the binding** (the review's method). Each message's
  time includes the binding's, and needs a baseline subtracted.
- **The steady clock** for each message: 100 ns resolution on Windows.

**Finding 4**

- **The `static_assert` over all byte values alone.** It catches a reason
  added to `reason_fits` but not mapped; it does not catch a new enumerator
  that `reason_fits` was not told about.
- **C4062 as an error for the whole project** (`/we4062` in `qa_warnings`).
  Every enum switch without a default would have to be exhaustive. Broader
  than the decision; it can be done on its own.

**Finding 5**

- **Refusing in the converter or `messages_from_table`.** The owner chose the
  adapter, where the record is.
- **One rule in all three** (the review's alternative). For data the adapter
  did not refuse, the converter's order is that rule.
- **Keeping one of two identical listings.** A repeat is as unexplained as a
  difference.

**Finding 6**

- **Refusing in the `InstrumentMeta` binding too.** It builds values; the
  review's test builds such metadata to show the three named places refuse it.

## Consequences

- `OrderBook` gains `bound_source()` and `other_source_updates()` (Python:
  `bound_source`, `other_source_updates`), and with the owner's review
  `other_source_controls()`. `has_book()` is false while an
  update or a stream snapshot is being applied, and `checksum()` is 0 when the
  book cannot be read. `MarketEvent`'s layout is unchanged; `from_bytes`
  refuses more.
- Where a consumer reads the book (Phase 12's features, Phase 9's risk, the
  Phase 5 seqlock snapshot), it reads it at `Applied` or `Recovered` results.
- The C++ book and the reference differ only where the reference cannot look:
  interleaved streams, events one at a time, capacity.
- The data core has one more reject reason, `two_metadata_versions`
  (`docs/data-core-spec.md` section 7). No stored data changes.
- `BM_ReplayCoinbaseFixture` is the baseline Phase 5's latency benchmark and
  Phase 8's latency model start from, with its tail.
- Phase 5 cannot add `QueueOverflow` without mapping it and giving it a
  counter.
- Phase 7 replays one stream per instrument book.
- With the owner's reviews: while bound, another source's updates, stale
  events and `Connect` and `Disconnect` resets are ignored and counted; a
  `SessionStart` reset applies from any source.

## Review

Choices made while building it. **Confirmed by the owner, 2026-11-17: all
twelve, with item 5 changed** (see "Owner's review"):

1. **`Recovered` is a result a consumer acts on**, with `Applied`: both say a
   message took effect.
2. **`checksum()` is 0 when the book cannot be read**, an empty book's, as the
   other queries show nothing.
3. **The binding starts at a stream snapshot's first level**, not its end,
   and **a reset or a failure ends it**.
4. **An update of another source is counted once, at its first level**, and
   is ignored even inside a message of the bound stream, which it does not
   interrupt.
5. **Only updates are judged by their source**; control events, instrument
   updates and frames of another source are taken as before.
6. **The reject reason is `two_metadata_versions`**, and two identical
   listings are refused too.
7. **The converter orders a record's versions by the reference's rule** (the
   four scales, then tick and lot size), for data that has them.
8. **The benchmark's stream is exported to `data/bench` by a script**, not
   committed; each update is timed with the timestamp counter; an iteration is
   a whole pass of the fixture.
9. **The counters are an exhaustive switch too**, so a new `StaleReason`
   fails the build until it has one.
10. **The `InstrumentMeta` binding and `MarketEvent.instrument_update` still
    build a tick or lot below 1.**
11. **`BM_ApplyUpdate_Coinbase38k_Top`'s comment was corrected** with the
    Kraken one.
12. **Existing tests:** none changed but the review's file (markers removed,
    its docstring updated, the marker helper deleted). The C++ test helper
    `update()` gained an optional source.

## Owner's review (2026-11-17)

**Decision.** The Review above is confirmed, with one change to item 5:
**while the book is bound, a `StreamStale` or `StreamReset` of another source
is ignored and counted, as an update of another source is.** Only a snapshot
of another source rebinds the book. When the book is unbound, every event
applies as before. An `InstrumentUpdate` still applies from any source.

**Why.** In a reconnect that opens the new connection before closing the old
(make-before-break), the new connection's snapshot rebinds the book; the old
connection's disconnect then arrives, and taken as before it would empty the
new stream's book with no snapshot to follow, leaving the book empty until
the next resync.

**How it is built.**

- An event of another source than the bound one that is an update, a
  `StreamStale` or a `StreamReset` (since the second review, below, not a
  `SessionStart` one) is `Ignored`: it is not applied, does not
  interrupt a message in progress, and neither makes the book stale nor
  empties it. A stale or reset is counted as it comes in
  `other_source_controls()` (Python: `other_source_controls`); updates stay
  counted once each in `other_source_updates()`.
- The binding is unchanged: from a stream snapshot's first level until a
  reset of the bound stream or a failure empties the book, or a snapshot of
  another source rebinds it. Unbound (at the start, after a reset or failure),
  a stale or reset of any source applies as before.
- **The test of the owner's sequence**
  (`InAMakeBeforeBreakReconnectTheOldConnectionsDisconnectLeavesTheNewBook`,
  and the same through the binding in `test_order_book.py`): the old
  connection opens and its snapshot binds the book; the new connection's
  connect is ignored and the old book stands; the new connection's snapshot
  rebinds the book; the old connection's late update, a gap it reports and
  its disconnect are all ignored; the book is the new stream's, readable and
  fresh, and the new stream's next update applies. Other tests: a stale or
  reset of another source inside a message interrupts nothing; while unbound
  they apply; an `InstrumentUpdate` of another source applies.

**A consequence for logs replayed one after another** *(no longer so: the
owner's second review, below, makes a session start apply from any source)*.
The converter replays
logs in `(log_id, seq)` order, each beginning with a `SessionStart` reset on
its own source. A recorded log ends with its stream's disconnect, which
empties and unbinds the book, so the next log's session start applies and the
C++ book agrees with the reference after every step, as before. But a log that
ends *without* its disconnect (a recorder that crashed) leaves the book bound
to it, and the next log's session start is then another source's reset,
ignored: the C++ book shows the old log's book until the next log's snapshot
rebinds it, where the reference book is empty from that session start. From
the snapshot on, the two agree again. `test_a_log_with_no_closing_disconnect_keeps_its_book_until_the_next_logs_snapshot`
pins it. The golden files, the integrity cases and the fixtures are each one
log, so no comparison with the reference over them changes; Phase 7's
one-stream-per-book criterion is where a replay across logs is decided.
Because of it, `test_two_logs_replayed_one_after_the_other_bind_the_book_to_each_in_turn`
(added with this ADR) now gives its first log the closing disconnect a
recorded log has.

**Choices made for it.** **Confirmed by the owner, 2026-11-19: all three**
(see "Owner's second review"):

1. **A counter of its own** for ignored stale and reset events
   (`other_source_controls`), beside `other_source_updates`, rather than one
   counter for both.
2. **A stale or reset of another source is ignored whatever its reason**,
   even one the book would refuse from its own stream (`InvalidEvent`): it is
   not this book's stream to judge.
3. **It interrupts no message in progress**, as an update of another source
   does not.

## Owner's second review (2026-11-19)

**Decision.** The three choices above are confirmed, with one change: **a
`StreamReset` whose reason is `SessionStart` always applies, from any
source, bound or not.** A `Connect` or `Disconnect` of another source stays
ignored while the book is bound, and so does a `StreamStale`.

**Why.** A new session means the old stream is over. Ignoring the next log's
session start, as the first review's rule did when a log ended without its
disconnect, kept a crashed log's book showing as fresh into the new session:
a stale-data hazard.

**How it is built.**

- A session start is not an event of another stream (`of_another_stream`): it
  is taken as the book's own reset, whatever its source. It empties the book
  and unbinds it, neither causing nor ending staleness, and is not counted
  in `other_source_controls`. The book's next binding is the new session's
  first stream snapshot.
- Nothing else changes. In a make-before-break reconnect, which happens
  within one session, the new connection's `Connect` and the old one's
  `Disconnect` are ignored while bound, as before; that test passes
  unchanged, in C++ and through the binding.
- **Tests.** `test_the_next_logs_session_start_empties_and_unbinds_the_book_of_a_log_with_no_disconnect`
  replaces the test that pinned the first review's consequence: two logs, the
  first with no closing disconnect; the second log's session start empties
  and unbinds the book, and the C++ book matches the reference after every
  step. `ASessionStartOfAnySourceEmptiesAndUnbindsTheBook` shows the same in
  C++, with the new session's update before its snapshot being one with no
  book, and another source's `Connect` and `Disconnect` still ignored. The C++
  test `ASnapshotOfAnotherSourceRebindsTheBook`, whose log b begins with a
  session start, now expects that session start to reset and unbind the book
  before b's snapshot binds it.
- **Logs replayed one after another** now agree with the reference after
  every step whether or not a log ends with its disconnect: each log's
  session start empties and unbinds the book.

Nothing in this change was decided while building it.
