# Adversarial review: Phase 4b, the C++ order book

Date: 2026-11-17
Scope: PR #23 (`phase-4b/cpp-order-book`): `cpp/include/qa/`, `cpp/src/`,
`cpp/tests/`, `cpp/bench/bench_order_book.cpp`, the book's part of
`cpp/bindings/module.cpp`, `python/src/quantagent/marketevents.py`,
`python/tests/data/test_order_book.py` and its helper `cppbook.py`, read
against CLAUDE.md, `docs/architecture.md` and ADRs 0020 to 0022.
Reviewer: a session that did not write the code. Nothing was fixed.

## How to read this

- **Reviewed tree:** commit `1804679`, the head of PR #23. It is two commits
  ahead of `main` (`f1df44a`, ADR 0022's owner decisions, then the book).
  The C++ Release build was brought up to date first; nothing recompiled, so
  the `.pyd` the tests import is this tree's.
- **Baseline:** `scripts/check.ps1` on this review's branch (the reviewed
  tree plus this file and its tests) printed `ALL GREEN`: 77 C++ tests, 1,834
  Python tests passed and this review's 8 tests `XFAIL` (6 deselected:
  `network` and `local_data`), ruff clean, no unmarked test over 5 s. The
  PR's own count was 77 and 1,834.
- **Proof:** findings 1, 2, 5 and 6 have failing tests in
  `python/tests/data/test_review_2026_11_order_book.py` (8 tests). Each is
  marked `xfail(strict=True, raises=AssertionError)` with its finding number,
  so the suite stays green and is reported `XFAIL`. Only an `AssertionError`
  counts. Each test's setup raises `SetupError` rather than asserting, so a
  test cannot be `XFAIL` because its setup went wrong. Findings 3 and 4, and
  the T and N items, are about the benchmark, the design, the tests and the
  documents. Their evidence (a measurement, a reading of the code, or a
  quotation) is given with each.
- **Measurements** over the one-hour recordings in `data/raw` (2026-11-11
  03:09 UTC, both venues, not in git) are described in the appendix, with the
  scripts that made them.
- **Severity**, as in `2026-11-data-core.md`: *High* means research or trading
  results would be silently wrong, or the next phases cannot meet their
  criteria as written. *Medium* means wrong or missing results in a plausible
  situation, or a design choice that will cost real rework. *Low* is
  everything else worth fixing.
- Line numbers refer to commit `1804679`.

**Update, 2026-11-17 (Phase 4b's review fixes, ADR 0023).** The owner decided
all six findings, and they are fixed; the eight tests are unmarked and pass,
as the findings' regression tests. An update cannot be read until its last
level, as a snapshot cannot, and consumers act only on results that say a
message took effect (finding 1). The book is bound to the source of the
snapshot it is built from; an update of another source is ignored and
counted, and so, after the owner's reviews of ADR 0023, is another source's
stale event, connect or disconnect, while a session start of any source
applies; Phase 7 replays one stream per book (finding 2).
`BM_ReplayCoinbaseFixture` replays the committed Coinbase fixture's converted
events and reports p50, p99, the maximum and the mean per update; the Kraken
benchmark's comment is fixed (finding 3). Every `StreamReason` is mapped at
build time (finding 4). The Kraken adapter refuses a record listing the
instrument twice, and the converter emits two versions at one record in the
reference book's order (finding 5). A tick or lot size below 1 is refused
(finding 6). N1 is in Phase 5's criteria, N2 and N4 in Phase 7's, N3 in Phase
9's. ADR 0022's Review items are confirmed, item 11 amended. T1's check is
made by a new test on both fixtures; T2, T3 and the two nits are open. The
findings below are left as they were written.

```powershell
.venv\Scripts\python -m pytest python\tests\data\test_review_2026_11_order_book.py -rxX
# each failure itself:
.venv\Scripts\python -m pytest python\tests\data\test_review_2026_11_order_book.py --runxfail
```

## Summary

| # | Severity | Finding | Proof |
|---|----------|---------|-------|
| 1 | Medium | Between an update's levels the book reads as fresh and shows a book that no step of the reference has: crossed on Coinbase, deeper than its depth on Kraken | 3 tests; 57 crossed half-applied updates in the committed Coinbase fixture, 790 in the hour |
| 2 | Medium | The book follows whatever stream it is given: two streams of one instrument, interleaved (Phase 7's merge of overlapping logs, a Phase 6 reconnect), merge silently into a book that is neither | 1 test |
| 3 | Medium | The benchmark baseline does not describe the Coinbase stream it stands for: real updates cost 6× the reported mean, with a p99 25× it and a maximum 230× it | measurement |
| 4 | Low | Phase 5's planned stale reason (`QueueOverflow`) will reach the book with no name and no counter, and nothing will fail | reading |
| 5 | Low | Two metadata versions at one record: the reference, the converter (so the C++ book) and the adapter each pick by a different rule, and the converter's depends on an unspecified row order | 1 test |
| 6 | Low | A tick or lot size the store refuses (below 1) is taken by the constructor, by `from_bytes` and by an `InstrumentUpdate` | 3 test cases |
| T1–T3 | — | How finding 1 got through the tests; what the random streams never generate; one test that does not show what its docstring says | reading |
| N1–N4 | — | Notes for Phases 5, 7 and 9 | reading, measurement |

ADR 0022's sixteen Review items are assessed one by one at the end: twelve
are sound, three are sound with a caveat (2, 6 and 14), and one is not sound
as built (11, finding 1).

## What holds

I tried to break these and could not.

- **The converted path agrees with the reference over a full hour of each
  venue.** I drove the C++ book from the converter over both one-hour
  recordings and compared it with the reference book in resume mode. After
  every step I compared the step, its reason, stale, recovered, `has_book`,
  both sides' sizes and the top 20 levels. Every level of both sides was
  compared every 250 steps and at the end. All 239,451 Kraken steps agree
  (958 full comparisons), and all 62,176 Coinbase steps (249). That is about
  24 times the Kraken fixture and 23 times the Coinbase fixture. The PR's
  `local_data` tests were not run, and none of them covers the C++ book.
  Neither hour has a stale period, so recovery on recorded data rests on the
  eight integrity cases, which the PR's tests cover.
- **Frames, interruptions, resets, stale and recovery, and precision changes**
  agree with the reference for every stream the converter can produce, as far
  as I could construct one: a reset while stale and while not, a stale event
  at a message's record, a snapshot that fails its own check, a check
  snapshot while stale, a version at a message's record, versions across logs
  (the converter refuses a message with no version of its own log at or
  before it, so the stream's latest version is always the reference's), and
  Coinbase's numbering across resets and snapshots. The event-at-a-time rules
  (ADR 0022, "A message interrupted") behave as documented in every case I
  built: a lost first, middle or last level, a lost frame end, a lost begin,
  a repeated level, an event of another record, kind or source inside a
  message, and a begin inside a frame. One lost event is one counted failure.
  The exception is a `StreamStale` interrupting a message, which counts two,
  as documented.
- **No undefined behavior, overflow or out-of-bounds access found** on the
  hot path. In `BookSide`, the ring is a power of two at least the capacity,
  `open` is only reached with `size_ < capacity_`, and every slot index is
  masked. `head_ + rank` stays below 2^21. `rank_of` is a plain lower-bound
  search over ranks `[0, size_)`. Kraken's checksum buffer (800 bytes) holds
  exactly the worst case: 20 levels, two numbers each, 20 characters each
  including the sign of `INT64_MIN`. `floor_divide` only ever divides by a
  power of ten from 1 to 10^18, and `scales_fit` holds the index to 0..18
  before `set_divisors` runs, in the constructor and in `take_instrument`.
  A `kind` or `side` byte outside its enum, or a stale or reset event's
  reason outside its range, fails the event as `InvalidEvent`; no switch
  falls off its end. The one path to undefined behavior is using a moved-from
  book (nit 2).
- **`apply` allocates, throws, locks and blocks on nothing.** Every function
  it reaches is `noexcept`. Its only storage is the two rings and fixed
  members, and the checksum's text is an 800-byte stack array. `qa_alloc_tests`
  is sound: it replaces every form of `operator new`, shows the counter counts
  and the constructor allocates, and its steady-state run demonstrably goes
  stale, recovers, resets, takes an instrument update and survives a broken
  frame. The book uses no other allocator. The binding's `top` allocates a
  vector, but the binding is not the hot path. What `apply` can do is take
  long on a full Coinbase book (finding 3, N1).
- **Kraken's capacity.** Over the hour's 239,447 Kraken updates, no update
  inserted more than one new level on a side, and no side ever held more than
  depth + 1 = 26 levels before the cut. `capacity_for_depth` = 50 has wide
  margin (Review 3).

## Findings

### 1. Medium: between an update's levels the book reads as fresh and shows a book no step of the reference has

**What.** ADR 0022 says an update "takes effect at its `kLastInMessage`
level" (line 137). A stream snapshot is built out of sight: `has_book()` is
false from its first level to its end (`order_book.cpp:243`, ADR 0022 line
114). An update is not. `update_level` writes each level into the live sides
as it arrives, and `has_book()` stays true throughout. So between the first
and the last level of an update, `top`, `best_bid`, `best_ask`, `size`,
`quantity_at` and `checksum` show a half-applied, uncut and unverified book,
while `has_book()` says it can be read and `is_stale()` says it is fresh.

**Evidence.**

- Coinbase lists an update's bids before its asks: all 2,033 two-sided
  updates in the fixture do. When the market moves up a tick, the new bid
  arrives before the ask it crosses is removed, and the half-applied book is
  crossed.
  `test_finding_01_between_the_levels_of_an_update_the_book_shows_no_state_the_reference_has_not`
  shows `best_bid` 101 ≥ `best_ask` 101 with `has_book` true and `is_stale`
  false. The reference book is not crossed before the message or after it.
- On recorded data,
  `test_finding_01_on_the_coinbase_fixture_half_applied_updates_show_crossed_books`
  counts **57 of the fixture's 2,702 messages** whose half-applied book the
  C++ book shows crossed. An independent Python replay
  (`measure_crossed.py`, appendix) also finds 57. Over the hour it finds
  **790 of 51,161 multi-level updates**, about 13 a minute, none crossed
  before or after their message.
- On Kraken the half-applied book is never crossed in the hour. But on
  80,077 of its 478,894 side-updates the side holds depth + 1 levels before
  the cut, a book Kraken never has and no checksum covered.
  `test_finding_01_between_the_levels_of_a_kraken_update_the_book_is_deeper_than_its_depth`
  shows three bids at depth 2.
- If an update's last level is lost and nothing follows (a quiet market, or a
  stalled queue), the half-applied update stays readable as fresh until the
  next event arrives and fails it.

**Why it matters.** Reading the book between a message's events is exactly
what the architecture makes possible. The core pops its inputs event by
event and "merges its inputs in a fixed, deterministic order"
(`architecture.md:282`). After the book (§4.1 step 5) come incremental
features: order flow imbalance, spread and microprice "update in place"
(step 6, line 213). Pre-trade risk checks "book freshness … price collar
against the current book" (step 8, line 218). And the core publishes a
seqlock snapshot for Python readers (line 302). A feature updated per event
would see a negative spread 13 times a minute on Coinbase. That would be a
silent error in research, and it becomes High if Phase 10 builds features
that way. The tests did not catch this; T1 says why.

**Suggested fix.** Hide the update the way a snapshot is hidden: queries
show nothing (`has_book()` false) while an update is in progress, and the
book reappears at its last level. This is one flag, and it matches the
existing snapshot rule and ADR 0022's "takes effect". A reader that finds no
book treats it as no price, which pre-trade risk must already handle.
Alternatively, add an `in_message()` query and a rule in architecture §5 that
nothing reads a book between a message's events. That puts the burden on
every future reader, so I would not choose it. The tests accept either the
book before the message or nothing.

### 2. Medium: the book follows whatever stream it is given

**What.** The book remembers nothing about which stream its snapshot came
from. Any update of the instrument, from any source, is applied to whatever
book it holds, and on Coinbase it is checked only against the last number
seen. `continues()` compares sources only *within* one message.

**Evidence.**
`test_finding_02_an_update_of_another_stream_is_not_applied_to_the_book`
builds two logs of one instrument that overlap in time, interleaved by
arrival:

1. Log *a* starts, takes its snapshot and an update.
2. Log *b*'s session start resets the book, and *b*'s snapshot rebuilds it.
3. A later update of log *a* arrives. Its number (3) happens to exceed *b*'s
   last (1), so it is applied to *b*'s book.

The result is `Applied`, and the book is fresh and holds a level of each
stream. If *a*'s number had been lower, the book would instead have gone
stale, and on Coinbase it would stay stale until the next snapshot, which
comes only with a resubscribe.

**How it happens for real.**

- **Phase 7.** The replay engine merges several logs "by `recv_mono_ns`
  through `logorder.merge_logs`" (roadmap Phase 7, first criterion). Two logs
  of one instrument overlap whenever a recorder is restarted before the old
  one has stopped, or two recorders run at once. `log_id`s sort by start time
  (`rawlog.log_id_of`), so the converter and the reference, which order by
  `(log_id, seq)`, replay the logs one after the other. The book was verified
  only in that order and will be run in the other.
- **Phase 6.** A handler that opens its new connection before closing the old
  one (make-before-break, common for resilient feeds) interleaves two
  connections' messages.
- **The same mechanism, the other way round.** A check snapshot comes from
  the probe connection, which is another feed thread and so another queue
  (architecture §5). If the core takes a probe level between two levels of
  the stream's update for the same instrument, the update fails as
  `IncompleteMessage` although nothing was lost. Architecture §5 promises
  that a message crosses *its queue* whole. It does not say the core drains
  that message before taking another queue's events for the same
  instrument. The roadmap's Phase 6 note puts contiguity on the handlers
  only.

ADR 0022 rejected binding the book to its snapshot's source (line 262)
because "the reference has no such rule, and a new stream is announced by a
`StreamReset`". The reference never sees interleaved streams. And a reset
announces that a new stream has started; it does not end the old one.

**Suggested fix.** This is the owner's decision, in an ADR before Phase 6:

1. Record the source of the stream snapshot that took effect. An update or
   stream frame of another source then either makes the book stale with a
   reason of its own, or is ignored. On converted data nothing changes,
   because every log begins with a session-start reset on its own source and
   a log has one stream source.
2. Have the Phase 7 replay refuse, or split into one book per stream,
   overlapping logs of one instrument.
3. Add to Phase 5's criteria that the core merges message by message per
   instrument, across a venue's queues.

The test accepts any outcome that is not a mix of the two streams (stale, or
*b*'s book unchanged).

### 3. Medium: the benchmark baseline does not describe the Coinbase stream it stands for

**What.** The benchmark's Coinbase-sized stream (`book_events.hpp`,
`synthetic_stream`) has 1 to 3 levels a message, nine in ten within eight
ticks of the mid. The PR reports per-message times on it (447 ns, and 151 ns
"near the top only"). The bench says "Near the top only: what most updates of
a liquid book are" (`bench_order_book.cpp:90`), and `BookSide`'s design rests
on "most updates are near the best" (`book_side.hpp:7-9`). The recorded
Coinbase stream is not like that:

| | Benchmark stream | Coinbase, one hour (62,172 updates) |
|---|---|---|
| Levels per update | 1 to 3 (2.0, the bench's `levels/msg` counter) | mean 9.68; p50 5, p99 80, max 528 |
| Update levels within 8 ranks of the best | 90% (8 ticks) | 23.3% |
| Rank of an update's level (0 = best) | — | p50 39, p90 156, p99 6,006, max 22,417 |
| Ring slots moved per insert or remove | — | mean 362; p50 48, p90 160, p99 6,007, max 11,023 |
| **Time per update message** | **447 ns** (151 ns near the top) | **mean 2.74 µs; p50 1.90, p90 5.50, p99 11.4, p99.9 22.5 µs; max 104 µs** |
| Time per level | about 220 ns (447 ns over 2.0 levels) | 283 ns |
| A full snapshot (38,543 events) | 1.98 ms | 1.98 ms |

The real times were measured through the binding, each message group timed
on the real book minus the same events on a book of another instrument (which
returns at its first line), median of three passes (`measure_cost.py`,
appendix). The method reproduces the benchmark's snapshot time exactly, which
gives some confidence in it. The cost per level is close to the benchmark's.
The difference is that a real message has five times the levels, and its
levels land far from the top, where `BookSide` moves hundreds to thousands of
slots for each insert or remove.

Kraken's real stream is close to its benchmark: 1.33 levels an update, at
most 2.

Two smaller problems:

- `BM_KrakenChecksum`'s comment says "Kraken BTC-USD's scales: prices at
  exponent 8 printed at 1, quantities at 8" (`bench_order_book.cpp:155`), but
  it builds the book with `kPlain` (line 157). That means divisors of 1 and
  synthetic digits (7-digit prices, quantities of at most 4 digits, against
  Kraken's 6 and 8).
- Every figure is a mean of three repetitions; no figure gives a tail.

**Why it matters.** This is the baseline Phase 5's queue latency benchmark
and Phase 8's latency model will start from. The core is one thread: a
104 µs update, or a 2 ms snapshot during a resync, holds every other queue,
execution reports included (N1).

**Suggested fix.**

- Add a benchmark that replays a recorded stream, for example the committed
  fixture exported once as `MarketEvent` bytes, which `from_bytes` already
  reads. Report p50, p99 and the maximum per message, not only the mean.
- Fix the two comments.
- Only if Phase 5 sets a per-message budget below about 10 µs at p99, take
  the far-from-the-top moves on (for example, a price-indexed array around
  the mid with the far book kept apart). Nothing here argues for that yet.

### 4. Low: Phase 5's new stale reason will be neither named nor counted

**What.** A `StreamStale` event's reason becomes the book's reason by a cast,
`fail(static_cast<StaleReason>(event.reason))` (`order_book.cpp:124`), after
`reason_fits` accepts it (`events.hpp:100-111`). Counters exist only for
values 1 to 5 and 64 to 67 (`slot_of`, `order_book.cpp:52-61`); anything else
is silently dropped. ADR 0022 plans exactly such a reason: "Phase 5 adds
`QueueOverflow`" (line 164), the `StreamStale` ahead of a dropped message
(`architecture.md:288`).

When that reason is added and `reason_fits` accepts it, the book will accept
it too. It will report a `stale_reason()` that is no `StaleReason`
enumerator (Python's enum shows `???`), and `stale_count` will count none of
them. Queue overflow is the one reason operations most need counted. Nothing
fails at build or test time: the `static_assert`s
(`test_order_book.cpp:70-80`) and
`test_the_books_enums_have_the_documented_numbers`
(`test_order_book.py:171`, `if 1 <= int(value) <= 5`) check only today's
five reasons.

**Why no test.** No test can fail today; the defect appears with Phase 5's
change.

**Suggested fix.**

- Map with a `constexpr StaleReason stale_reason_of(StreamReason)` switch
  with no `default`.
- Add a `static_assert` over all 256 byte values that every reason
  `reason_fits(StreamStale, r)` accepts maps to a reason with a counter slot.
- Make the Python test iterate every `StreamReason` a `StreamStale` may
  carry, not 1 to 5.

### 5. Low: two metadata versions at one record are resolved three different ways

**What.** Suppose two `instruments` rows share one record, for example a
Kraken instrument message that lists the pair twice with different
precisions. Each component then picks a different one:

- **The reference** sorts each log's versions by `(seq, numbers)` and takes
  the last at or before the message (`book.py:271`, `:299`). That is the
  version with the larger `(price_exp, qty_exp, price_precision,
  qty_precision)`.
- **The converter** sorts versions by `(log_id, seq)` only, a stable sort
  that keeps the session's row order (`marketevents.py:310-312`), and emits
  both. The C++ book keeps the last. `PITSession.instruments` orders rows
  `ORDER BY "event_ts", "log_id", "seq"` (`session.py:650`), so for two rows
  of one record that order is unspecified, and the C++ book's precision can
  differ between runs.
- **The adapter** applies the rows in the message's order (`kraken.py:172-173`):
  the last listed wins.

Nothing refuses such a record.

**Evidence.**
`test_finding_05_two_metadata_versions_at_one_record_give_both_books_the_same_precision`
builds a Kraken snapshot summed at two decimals and two versions at one
record (two decimals and one), in both row orders. The reference verifies
the snapshot in both orders. In the order [two, one], the C++ book ends at one
decimal and goes stale with `checksum_mismatch`.

**Suggested fix.** Refuse two versions of one instrument at one record, in
the adapter, or in `messages_from_table` and the converter, or both. Failing
that, give all three one rule (the last in the record's order) and a column
the session can order by.

### 6. Low: a tick or lot size the store refuses is taken

**What.** The store's schema requires `tick_size` and `lot_size` of at least
1 (`schema.py:269-270`). `scales_fit` copies the store's rule for the scales
only. So the constructor, `MarketEvent.from_bytes` (`module.cpp:201` checks
only the scales) and `take_instrument` (`order_book.cpp:176`) all accept a
tick of 0 or −1 and a lot of 0 or −5. I checked `from_bytes` by hand (a lot
of −5 round-trips); the test below covers the constructor and the update.

**Evidence.**
`test_finding_06_metadata_with_a_tick_or_lot_the_store_refuses_is_refused`,
three cases: the constructor does not refuse them, and an `InstrumentUpdate`
carrying them returns `Instrument` and replaces the tick in force.

**Why it matters.** ADR 0022 gives Phase 9's pre-trade risk "the tick in
force" from the book. Rounding to a tick of 0 divides by zero, and a negative
tick inverts a collar. No adapter produces either, but a Phase 6 handler bug
could.

**Suggested fix.** A `meta_fits(meta)` that is `scales_fit` plus
`tick_size ≥ 1` and `lot_size ≥ 1`, used by the constructor,
`take_instrument` and `from_bytes`.

## Test findings

**T1. Every comparison with the reference happens at a group's end.**
`assert_same_steps`, the golden-file test and the integrity cases read the C++
book only after a group's last event. Nothing compares or bounds what the
queries show between a message's events, except the snapshot-frame tests the
breakage pass added (`top()` showing a book still being built). That is why
finding 1 passes every test. A check is cheap: after each `Pending` result,
the book shows either the state before the message or nothing.

**T2. What the random streams never generate.** The 400 streams per run are
one log with one stream source. Their metadata versions are always at records
of their own, never at a message's record. A side never holds more than 27
levels, under a capacity of 64. So cross-log ordering, interleaved sources
(finding 2), two versions at one record (finding 5), a version at a message's
record, and capacity are never randomized. The capacity case is stated in the
test; the others are not.

**T3. `test_an_instrument_update_is_taken_while_the_book_is_stale`**
(`test_order_book.py:538`) says "So the snapshot that recovers the book is
checked at the precision in force". But after the change its stream has only
updates: no snapshot recovers the book, and the test ends stale. Its C++
counterpart, `AnInstrumentUpdateIsTakenWhileStale`, does show the recovery,
so the property is tested. The Python docstring overstates its own test.

## Notes for the next phases

**N1 (Phase 5): a Coinbase snapshot is a 2 ms burst.** It is 38,543
`MarketEvent`s, 3.4 MB. Architecture §5's rule (each queue holds twice the
largest snapshot) means at least 77,086 slots, 6.8 MB, which rounds up to
2^17 slots, 11.5 MB. Applying it takes 1.98 ms of core time, during which
nothing else on the core runs. Phase 5's latency benchmark should include a
resync's snapshot among the single events, as its stress-test criterion
already does for correctness.

**N2 (Phases 5 and 7): an interrupting event's own effect is lost.** When an
event interrupts a message, `apply` returns the event's own failure or else
`WentStale(IncompleteMessage)` (`order_book.cpp:111-113`; ADR 0022 Review 14).
So a `StreamReset`, an `InstrumentUpdate` or a new frame's begin that
interrupts a message is reported as `WentStale` only. A consumer that acts on
effects never sees the reset: a feature resetting its state, or Phase 7's
journal of "every input event". It cannot happen on converted data, and it
is documented. A second effect in `BookResult`, a third byte, would carry
it.

**N3 (Phase 9): the book records no time.** Pre-trade risk checks "book
freshness" (§4.1 step 8), which needs the receipt time (or monotonic stamp)
of the last message that took effect. The core could track it from
`BookResult`. But the book is where it belongs, in the same way `is_stale()`
is the book's.

**N4 (Phase 7): replay must start at a log's beginning.** The book cannot be
copied or checkpointed (copy is deleted). In the recordings, each venue sent
its book snapshot only when the feed subscribed: each fixture and each
one-hour recording has exactly one stream snapshot. So a replay of a book must start where its log starts. That is
fine now, and worth stating in Phase 7's criteria.

## Nits

1. `OrderBook::capacity_for_depth(std::uint32_t)` returns `2 * depth` in 32
   bits. It wraps for a depth of 2^31 or more: 2^31 + 1 gives 2, a capacity
   below the depth, with no error. The binding passes a 16-bit depth, so only
   C++ callers can reach it. Take `std::uint16_t`, the type of
   `MarketEvent::depth`.
2. A moved-from `OrderBook` keeps its sides' `size_` with a null ring, so
   `apply` or a query on it dereferences null. "Don't use after move" covers
   it, but a move that also zeroes `size_` and `head_` would leave a valid
   empty side for the same cost.

## ADR 0022's Review items

| # | Item | Verdict |
|---|------|---------|
| 1 | The `InstrumentUpdate` layout | **Sound.** No reserved byte, it round-trips, and `from_bytes` checks the scales and every unused field. It does not check tick and lot sizes (finding 6). |
| 2 | Every stored version is one event, on the log's stream source, after control events and before levels | **Sound, with a caveat.** It reproduces the reference's rule (the latest version of the message's own log at or before it) on every converted stream, because a log's records are contiguous in `(log_id, seq)` order and a message with no version of its own log is refused. The exception is two versions at one record (finding 5). Under Phase 7's merge by monotonic stamp, an overlapping log's versions would interleave too (finding 2). |
| 3 | `capacity_for_depth` is twice the depth | **Sound.** The hour's 239,447 Kraken updates insert at most one new level on a side (peak 26 at depth 25), so 50 has wide margin. State the assumption it rests on: a message inserts at most `depth` new levels on a side. The failure mode is safe (stale, counted), though on Kraken recovery needs a resubscribe. Nit 1. |
| 4 | A level that does not fit makes the book stale at once | **Sound.** Simple, never wrong, counted. |
| 5 | The book's own reasons, 64 to 67, in their own enum; `InvalidEvent` where the reference raises a non-integrity error | **Sound in itself.** The mapping from `StreamReason` is a cast with no exhaustiveness check (finding 4). |
| 6 | An empty snapshot frame makes the book stale | **Sound for the book, with a caveat.** `book_events` cannot represent an empty snapshot (a message is its rows), so when a venue sends one, the reference and the converted stream both keep the old book. A Phase 6 handler that emits the empty frame would make the C++ book stale until a non-empty snapshot. That has never happened for BTC-USD, but it matters for an illiquid instrument and needs a decision before Phase 6. |
| 7 | Any broken frame makes the book stale, a check snapshot's included | **Sound and conservative**, provided the core never interleaves a probe's frame with the stream's message of one instrument (finding 2). |
| 8 | The header is read from the `kLastInMessage` level | **Sound.** `messages_from_table` refuses a message whose levels disagree on the header, so converted levels always agree. |
| 9 | An `InstrumentUpdate` with other exponents is refused, and the metadata kept | **Sound.** It should also refuse a tick or lot below 1 (finding 6). |
| 10 | Events of another instrument are ignored | **Sound, and needed.** One Kraken record can carry several symbols' books, and a feed queue carries every instrument of its connection. A C++ test shows such an event inside a message does not interrupt it. |
| 11 | The queries show nothing unless the book can be read | **Not sound as built.** True for snapshot frames, not for updates (finding 1). |
| 12 | The constructor throws on a configuration it cannot build | **Sound**: startup only. |
| 13 | `qa_alloc_tests` is its own binary | **Sound**, and the test is sound (see "What holds"). |
| 14 | An interrupted message fails as `IncompleteMessage` and the event is taken on its own; orphans are skipped while stale | **Sound for counting**: one lost event is one failure, as tested. Caveat: the interrupting event's own effect is dropped from the result (N2). |
| 15 | At most 2^20 levels a side | **Sound.** The hour ends with 15,940 bids and 22,608 asks; the largest fixture side is 22,639. |
| 16 | Existing tests changed, none weakened | **Confirmed** from the diff. `test_core_types.py`'s unknown-kind refusal tries 9, 10 and 255, since 8 is now a kind. `test_marketevents.py`'s `run` leaves out `instrument` groups unless asked, which gives the old tests the converter's pre-4b output exactly, and `converted_steps` skips them as it skips `check` groups. No assertion changed. |

## Appendix: measurements

All runs used Release, the tree at `1804679`, and the one-hour recordings
`data/raw/{kraken,coinbase}/BTC-USD/20261111T030919259143Z.qaraw.zst`
(normalized in memory by the Phase 1c adapters; nothing written), on the
machine the PR's benchmark was run on. Each script is run with the
repository's venv:
`.venv\Scripts\python <script> <venue> <raw log>` (`compare_hour.py` also
takes the repository's path).

| Script | Measures | Result |
|--------|----------|--------|
| `compare_hour.py` | The C++ book against the reference over a whole log ("What holds") | Kraken 239,451 steps, Coinbase 62,176: all agree; 0 stale |
| `measure_books.py` | Kraken: a side's peak inside an update; Coinbase: levels per update, the rank of each update level, ring slots moved (finding 3, Review 3) | Kraken: 1.33 levels per update, at most 2; peak depth + 1; at most one new level a side. Coinbase: the table in finding 3 |
| `measure_cost.py` | The C++ book's time per real Coinbase message, through the binding (finding 3) | The table in finding 3 |
| `measure_crossed.py` | Updates whose half-applied book is crossed though the book before and after is not (finding 1) | Coinbase: 790 of 51,161 in the hour, 57 of 2,415 in the fixture; Kraken: 0 |

<details>
<summary><code>measure_books.py</code></summary>

```python
import sys
from bisect import bisect_left, bisect_right, insort
from collections import Counter

from quantagent.data.adapters import normalize
from quantagent.data.rawlog import read_raw_log
from quantagent.data.schema import BookEventKind

venue, path = sys.argv[1], sys.argv[2]
rows = normalize(read_raw_log(path)).rows[("book_events", venue, "BTC-USD")]
rows.sort(key=lambda r: (r["log_id"], r["seq"], r["level_index"]))
messages, current = [], None
for r in rows:
    key = (r["log_id"], r["seq"])
    if current is None or current[0] != key:
        current = (key, r["kind"], r["depth"], [])
        messages.append(current)
    current[3].append((int(r["side"]), r["price"], r["qty"]))
stream = [m for m in messages if m[1] != BookEventKind.CHECK_SNAPSHOT]
book, qty = {1: [], -1: []}, {1: {}, -1: {}}  # ascending prices per side


def rank_of(side, price):  # 0 is the best
    prices = book[side]
    return len(prices) - bisect_right(prices, price) if side == 1 else bisect_left(prices, price)


per_message, peak_over, max_inserts, ranks, moves = Counter(), Counter(), 0, [], []
for key, kind, depth, levels in stream:
    if kind == BookEventKind.SNAPSHOT:
        book, qty = {1: [], -1: []}, {1: {}, -1: {}}
    else:
        per_message[len(levels)] += 1
    peak = {s: len(book[s]) for s in (1, -1)}
    inserts = {1: 0, -1: 0}
    for side, price, q in levels:
        prices, held, n = book[side], qty[side], len(book[side])
        if kind == BookEventKind.DELTA:
            r = rank_of(side, price)
            ranks.append(r)
            if (q == 0 and price in held) or (q != 0 and price not in held):
                moves.append(min(r, n - r) if q != 0 else min(r, n - 1 - r))  # as open/close
        if q == 0:
            if price in held:
                del held[price]
                del prices[bisect_left(prices, price)]
        else:
            if price not in held:
                insort(prices, price)
                inserts[side] += 1
            held[price] = q
        peak[side] = max(peak[side], len(prices))
    if depth is not None:
        for side in (1, -1):
            p = book[side]
            if len(p) > depth:
                cut = p[: len(p) - depth] if side == 1 else p[depth:]
                book[side] = p[-depth:] if side == 1 else p[:depth]
                for c in cut:
                    del qty[side][c]
        if kind == BookEventKind.DELTA:
            for side in (1, -1):
                peak_over[peak[side] - depth] += 1
                max_inserts = max(max_inserts, inserts[side])
# printed: per_message, peak_over, max_inserts, and percentiles of ranks and moves
```

</details>

<details>
<summary><code>measure_cost.py</code></summary>

```python
import statistics
import sys
import time

from quantagent import _core
from quantagent.data.adapters import normalize
from quantagent.data.rawlog import read_raw_log
from quantagent.data.schema import BOOK_EVENTS, INSTRUMENTS, table_from_rows
from quantagent.marketevents import convert

venue, path = sys.argv[1], sys.argv[2]
n = normalize(read_raw_log(path))
table = table_from_rows(BOOK_EVENTS, n.rows[("book_events", venue, "BTC-USD")])
events = n.rows.get(("stream_events", venue, "BTC-USD"), [])
meta = table_from_rows(INSTRUMENTS, n.rows[("instruments", venue, "BTC-USD")])
groups = [g for g in convert(table, events, meta, instrument=7) if g.step == "message"]
row = meta.to_pylist()[0]


def book_of(instrument):
    m = _core.InstrumentMeta(_core.InstrumentId(instrument), row["price_exp"], row["qty_exp"],
                             row["price_precision"], row["qty_precision"],
                             _core.Price(row["tick_size"]), _core.Qty(row["lot_size"]))
    return _core.OrderBook(m, _core.BookCheck.CoinbaseSequence)


def run(instrument):  # 8 is another instrument: every event returns at apply's first line
    apply, clock, costs = book_of(instrument).apply, time.perf_counter_ns, []
    for group in groups:
        start = clock()
        for e in group.events:
            apply(e)
        costs.append(clock() - start)
    return costs


passes = []
for _ in range(3):
    real, base = run(7), run(8)
    passes.append([r - b for r, b in zip(real, base)])
per_message = [statistics.median(v) for v in zip(*passes)]  # [0] is the snapshot
# printed: the snapshot's time, and the mean and percentiles of per_message[1:]
```

</details>

<details>
<summary><code>measure_crossed.py</code></summary>

```python
# Messages grouped as in measure_books.py; the book replayed the same way.
# For each update of more than one level: crossed (best bid >= best ask)
# after one of its levels before the last, and not crossed before the
# message or after it (after the cut to depth).
```

</details>

`compare_hour.py` is `cppbook.assert_same_steps` with three changes. It
normalizes a whole raw log in memory. At every step it compares sizes and the
top 20 levels, not every level. And it compares every level of both sides
every 250 steps and at the end.
