# Adversarial review: data core, reference book and C++ core types

Date: 2026-11-14
Scope: everything built in Phases 1a to 3d and the clock work (ADRs 0011 and
0012): `python/src/quantagent/data/`, `cpp/`, their tests, `docs/` and ADRs
0001 to 0016.
Reviewer: a session that did not write the code. Nothing was fixed.

## How to read this

- **Reviewed tree:** commit `0ad268e`, the head of PR #15 (Phase 3d). When the
  review started `main` was at `1735bdd` and **did not include PR #15**; the PR
  was open, mergeable and green. Its branch is exactly one commit ahead of
  `main`, so this is the tree `main` becomes when it merges.
- **Baseline:** `scripts/check.ps1` on that tree printed `ALL GREEN`: 17 C++
  tests, 1,255 Python tests passed (6 deselected), ruff clean.
- **Proof:** every finding numbered `R..` has at least one failing test in
  `python/tests/data/test_review_2026_11.py` (23 tests, all failing, each for
  the reason given here). Findings numbered `D..`, `T..`, `X..` and `A..` are
  design, test, documentation and ADR findings; their evidence is a
  measurement, a mutation run or a quotation, stated with each.
- **Severity:** *High* means research or trading results would be silently
  wrong, or the next phases cannot meet their criteria as written. *Medium*
  means wrong or missing results in a plausible situation, or a design choice
  that will cost real rework. *Low* is everything else worth fixing.
- Line numbers refer to commit `0ad268e`.

Run the failing tests with:

```powershell
.venv\Scripts\python -m pytest python\tests\data\test_review_2026_11.py
```

**Update, 2026-11-15.** The review and its tests reached `main` together with
PR #15. To keep `main` green, every test in that file is now marked
`xfail(strict=True)` with its finding number as the reason. Each still fails
for the reason given here and is reported as `XFAIL`. The marker is strict:
when a finding is fixed its test passes, the suite fails on the unexpected
pass, and the marker is removed in the same change.

**Update, 2026-11-15 (Phase 3e, ADR 0017).** R01, R02, R03 and R09 are fixed
and their tests are unmarked. The tests of R03 and R09 had to change shape,
not just parameters; ADR 0017's "Review" section says why. X1 and X2 are
corrected in the documents, R10 is narrower but open, and D4's first half
(a session per decision time) is answered by the replay cursor. Everything
else stands. The findings below are left as they were written: where they
say `as_of`, a session now has `available_at` (receipt time) and `version`
(what `as_of` was).

**Update, 2026-11-15 (Phase 3f, ADR 0018).** R04, R05, R10 and R11 are fixed
and their tests are unmarked. Every ingest of a raw log is a load, and a
session reads each log from its latest complete load (R04). A value the
schema refuses rejects its record (R05). A session shows each trade as its
first observation in `(log_id, seq)` order (R10). A normalizer fed records
out of order raises `InterleavedLogs` (R11). The tests of R10's conflicting
repeat and of R11 had to change shape, because the owner decided a behavior
other than the one they required; ADR 0018's "Review" section says why. X4
is corrected in data-core, the tie-break X5 describes is replaced by ADR
0018's rule, and what X6 and X7 quote holds again. R06, R07 and R08, and the
design, test and documentation findings not named here, are open.

**Update, 2026-11-16 (Phase 3g, ADR 0019).** R06, R07 and R08 are fixed and
their tests are unmarked; no test in the file is marked any more. Each
instrument has one fixed price and quantity exponent, the recorder fetches
metadata before subscribing, and a log's first metadata version is in effect
from its first row (R06). Findings are limited per severity and a truncated
report fails (R07). A session's clock is judged on every day it spans, and so
is a reconnect (R08). The three mutations of T1 and T2 are caught by new tests
(`test_recorder.py`, `test_read_paths.py`). The X list: X3 and X13 are fixed
in the code, X9 and X10 in the documents, X8, X11, X12 and X14 already held,
and the duplicate-column error now says what is wrong. The ADR chain (A) is
consolidated in `docs/data-core-spec.md`. Still open: D1 to D7, T3, and the
minor notes other than the duplicate-column one.

## Ranked findings

| # | Severity | Finding | Area (task item) |
|---|----------|---------|------------------|
| R01 | High | `as_of` bounds when a row was *loaded*, not when it was *received*. A session at a simulated time inside a recording sees nothing; a session opened later sees data that had not arrived yet | Look-ahead (1), design (3, 6) |
| R02 | Medium | Within one load, integrity events become known *after* the book rows they qualify. In between, a broken stream reads as clean | Look-ahead / correctness (1, 2) |
| R05 | Medium | A value the adapter accepts but the schema refuses (a venue time after 2262, for one) raises out of `ingest` and stops the whole load | Correctness (2) |
| R04 | Medium | A correction cannot remove a row. After an adapter fix and a reload, the errors the fix resolved are still reported and the reference book still refuses the log | Correctness (2), design (3) |
| R06 | Medium | Instrument metadata cannot be relied on to interpret a row: no version is "in effect" for the first rows of a session, and on Coinbase the exponent follows the tick size | Correctness (2), docs (5) |
| R03 | Medium | `knowledge_ts` is read from the clock before the batch is readable, so "rows with `knowledge_ts <= as_of`" is not repeatable for an `as_of` near a write | Look-ahead (1) |
| R07 | Medium | The per-check finding limit can drop an ERROR behind INFO findings, and the report then exits 0 | Correctness (2) |
| R10 | Medium | Which observation of a trade the store shows depends on how the logs were loaded and on the wall clock: three different rules for one key | Correctness (2), ADRs (6) |
| R09 | Medium | One time window selects book rows by venue time and their integrity events by receipt time; a windowed replay runs past a lost message | Correctness (2) |
| D1 | Medium | `MarketEvent` cannot carry a gap, a reset or a rejected message, and does not say which log it came from. Phase 4's Coinbase criteria cannot be met from it | Phase 4+ (3) |
| D2 | Medium | The reference book stops at the first failure and cannot resume, so there is no oracle for "recovers on the next snapshot"; and the recorder never resynchronises | Phase 4+ (3) |
| D3 | Medium | One event per level: a Coinbase snapshot is about 38,000 events, and the queue policy "full means stale and resync" would then loop | Phase 5+ (3) |
| D4 | Medium | A session per decision time is slow and gets slower with history: 374 ms for a one-second book window on a one-hour store | Phase 7+ (3) |
| T1 | Medium | No test checks that `recv_ts` is taken when a message arrives; a recorder that stamps the *previous* message's time passes all 20 recorder tests | Tests (4) |
| T2 | Medium | The two "no other read path" guards cannot see what they guard against; a new module and a direct Parquet read both pass | Tests (4) |
| R08 | Low | A session's clock error is reported only in a date range that contains the session's start | Correctness (2) |
| R11 | Low | One normalizer fed two logs in arrival order tags every row with one log id; keys collide silently | Misuse (2) |
| D5 | Low | The recorder's `recv_ts` is when a coroutine dequeued the message, and probes share its event loop and link | Phase 7+ (3) |
| D6 | Low | The `clock_skew` thresholds were calibrated on the recordings with the 1.4 s clock error and were not revisited | Correctness (2) |
| D7 | Low | C++ nits: `InstrumentId` narrows silently, `depth` is narrower than Python allows, unknown flag bits and negative exponents are accepted | C++ (2, 5) |
| T3 | Low | Six tests whose names promise more than they check | Tests (4) |
| X | Low | 14 places where documents and code disagree | Docs (5) |
| A | Low | The ADR chain: what has drifted and what to consolidate | ADRs (6) |

---

## High

### R01. `as_of` bounds when a row was loaded, not when it was received

**What.** `PITSession` keeps rows with `knowledge_ts <= as_of`, and
`knowledge_ts` is the wall clock at the moment `AppendOnlyStore.append` ran
(`store.py:81`). Data enters the store through `ingest`, which is run on a raw
log once its recording session has ended; nothing loads records as they
arrive. So for every row, `knowledge_ts` is hours or days later than the time
it was received.

The documents assume the two are close:

- Architecture 3.2 and 4.3: "Inside a backtest, the harness creates a session
  per decision time with `as_of` equal to simulated time."
- Architecture 6.2: simulated now "is the `recv_ts` of the event being
  processed plus modelled latency, so a strategy sees data when we would have
  had it, not when the venue produced it."
- Roadmap Phase 7: "the callback receives a `PITSession` whose `as_of` is
  simulated time", and "A test proves the callback cannot see data after
  simulated time."
- `quality.py:519` tells the reader that a bad recording clock means "recv_ts
  and knowledge_ts are off by about that much". The recording clock does not
  touch `knowledge_ts`.

**Evidence.** On both committed fixtures, loaded the way the command line
loads them:

| | Kraken | Coinbase |
|---|---|---|
| Trades the recorder had received by mid-recording | 139 | 496 |
| Trades a session with `as_of` = that time returns | 0 | 0 |
| Rows returned by `trades`/`book_events(0, now)` at `as_of` = everything, whose `recv_ts` is after `now` | 121 | 214 |
| Oldest resent trade (`TRADE_BACKFILL`): venue time before its receipt | 36.5 s | 23.3 s |

`test_r01_a_session_at_a_time_inside_the_recording_sees_what_had_arrived_by_then`
and `test_r01_reading_up_to_simulated_time_returns_nothing_that_arrived_later`.

**Why it matters.** There are two ways to use the session in a backtest, and
both are wrong:

1. `as_of` = simulated time, as documented. The session is empty. Phase 7's
   look-ahead test would pass with no data in it.
2. `as_of` = a time after the load, and bound the query yourself. The
   structural guarantee is gone: every row of the future is one `WHERE` away.
   The typed range methods bound `event_ts` (venue time), so the natural call
   `trades(venue, instrument, start, now)` returns rows that were still in
   flight at `now`, and resent history that arrived up to 36 seconds later.
   On data recorded with a synchronised clock the in-flight window is the
   network lag (median 95 ms, 99th percentile 476 ms on the local 30-minute
   recording), which is the same size as the horizons an order-flow signal
   works on.

The same confusion reaches the agent layer. Architecture 8.1 says of
`query_data`: "`as_of` cannot exceed now and cannot reach into the locked
holdout period". A holdout is a range of market time; `as_of` is load time.
Capping `as_of` does not keep a query out of a holdout that was loaded
earlier, and the session has no way to impose a ceiling on `event_ts` or
`recv_ts` for a SQL query.

The property test (`test_session_property.py:138`) cannot see any of this: it
draws knowledge times of a few thousand nanoseconds and event times in 2023,
so the two axes never meet.

**Suggested fix (owner decision, needs an ADR).** Separate the two questions
the session answers:

- *Which version of the store?* A knowledge-time bound, as now. It selects
  corrections. Call it what it is (for example `vintage`).
- *What had we received?* A bound on `recv_ts`, applied in the views after the
  version is chosen. This is the one a backtest sets to simulated time, and
  the kind of bound a holdout needs.

A session would take both, with the receipt bound required for research and
agent use. `recv_ts` is already the right stamp on all four datasets:
resent trades carry the time they were resent, and integrity events the time
they were noticed. Loading with `knowledge_ts` set to `recv_ts` is not an
alternative: a correction needs a later knowledge time than the row it
corrects. Until this is decided, Phases 7, 12 and 13 should not build on
"`as_of` equals simulated time".

---

## Medium

### R02. Integrity events become known after the data they qualify

**What.** `ingest` flushes each batch as four appends, in the order
instruments, trades, book events, stream events (`base.py:561`, `base.py:577`),
each with its own `knowledge_ts`. A session opened between the last two sees
the book rows and none of the events that say a message was lost.

**Evidence.** The Coinbase fixture with one book update removed, loaded with a
clock that gives each append a round number:

| `as_of` | Book rows | Stream events | `book.replay` | `stream_integrity` errors |
|---|---|---|---|---|
| 3000 (after the book append) | 110,976 | 0 | runs to the end, 2,701 messages | none |
| 4000 (after the events append) | 110,976 | 16 | `SequenceBreak` | `sequence_gap` |

`test_r02_book_rows_are_never_visible_before_the_events_that_qualify_them`.

**Why it matters.** The reconstructor's Coinbase rule depends on the stored
events ("Without them, a lost Coinbase message is invisible", roadmap Phase 2).
A point-in-time reader at the wrong moment gets a wrong book and a clean
quality report. In wall-clock terms the gap between the two appends is short.
In `as_of` terms it is permanent: any session whose `as_of` falls between the
two knowledge times sees it, now or a year from now. And it stops being short
when the load dies between the appends. R05 does exactly that: its last two
cases raise while building the `stream_events` batch, after the trades and
book rows of the same flush are published, so the flush's events (every one
of them, not just the bad one) never arrive. The same load also shows a log
half-loaded at any `as_of` between two flushes (a one-hour log takes 10 to
22 s and several flushes), with nothing to say so.

**Suggested fix.** Give one flush one knowledge time across datasets (a store
call that appends several tables as one batch). The one-line mitigation is to
append `stream_events` before `book_events`, so a reader can be missing data
but never the warning. Separately, record when a log's load is complete, so a
session can tell a partly loaded log from a short one.

### R05. A value the schema refuses stops the whole load

**What.** The adapters build rows and `ingest` validates them only at the next
flush, in `table_from_rows`. A row the schema refuses raises `SchemaError`
out of `ingest`. Batches already flushed stay in the store; the rest of the
log is never loaded and no report is returned.

**Evidence.** Five single messages, each accepted by `normalize` as data:

| Message | Fails on |
|---|---|
| Kraken trade stamped `9999-12-31T23:59:59Z` | `trades.event_ts` does not fit in int64 (the range ends in 2262) |
| Coinbase trade stamped `2300-01-01T00:00:00Z` | `trades.event_ts` |
| Coinbase book message with `sequence_num` 2^70 | `book_events.venue_seq_last` |
| Coinbase message after a jump to 2^70 | `stream_events.observed` (the gap event) |
| Recorder clock record with `offset_ns` 2^70 | `stream_events.observed` |

`test_r05_a_value_that_does_not_fit_the_schema_is_a_reject_not_a_crash`
(five cases). A fuzz run of 2,175 malformed variants of every message shape
in the fixtures found no exception escaping `Normalizer.feed`; the hole is
only between the normalizer and the schema.

**Why it matters.** Roadmap Phase 1c: "Malformed or unknown messages are
counted and reported, never silently dropped and never a crash." A sentinel
timestamp is the realistic case. ADR 0008 says an implausible venue timestamp
is "stored as given"; one after 2262 cannot be.

**Suggested fix.** Check each record's rows against the dataset's types and
ranges inside `Normalizer.feed` (the per-value check in `schema.py` already
exists), so the record is rejected with a `MESSAGE_REJECTED` event. Add a test
that every row the fuzz produces passes `table_from_rows`.

### R04. A correction cannot remove a row

**What.** The store resolves versions per key, and a later row with the same
key wins. There is no way to say "this key no longer exists". Normalizing a
log again with a fixed adapter replaces the rows it still produces and leaves
the ones it no longer produces.

**Evidence.** The Kraken fixture with one message made unparseable (standing
in for a message the adapter could not read before a fix), loaded, then the
real fixture loaded into the same store. The second load reports no rejects.
The store still reports `message_rejected` at that record, and `book.replay`
still raises `StreamFault` there.
`test_r04_loading_a_log_again_after_a_fix_clears_what_the_fix_resolved`. On
the Coinbase fixture the same experiment leaves two phantom errors
(`message_rejected` and the `sequence_gap` it caused).

**Why it matters.** ADR 0009: keying by raw record "is what makes it safe to
fix an adapter bug and re-ingest". ADR 0014 relies on it ("re-ingesting into
the same store is enough"). It holds only while a fix never produces fewer
rows or moves an event to another `event_index`. Otherwise the only remedy is
rebuilding the store, which the documents present as the exception. Roadmap
Phase 1b lists "No retraction" as a limit, with a cancelled trade as its
example; it applies to every dataset and to our own fixes.
`test_log_id.py:174` reloads identical output, so it cannot see this.

**Suggested fix (owner decision, storage semantics).** All datasets except
`trades` are keyed by raw record within a log, so the natural unit of
replacement is the log: record each load of a log (log id, knowledge time,
code version) and have the views keep, per log, only the rows of its latest
load known at `as_of`. That makes a reload a true replacement and keeps the
store append-only.

### R06. Instrument metadata cannot be relied on to interpret a row

Two defects with one consequence: a reader cannot be sure which exponents a
stored mantissa was written with.

**(a) No version is in effect for the first rows of a session.**
`instrument(at=...)` picks the latest version with `event_ts <= at`
(`session.py:268`). A metadata version's `event_ts` is its receipt time
(ADR 0008); a market row's is venue time. Trades resent on subscribe are older
than the session, and Kraken stamps its first book snapshot before its
metadata message reaches us. By `event_ts` those rows precede the version
that describes them.

| Rows with `event_ts` before their session's metadata version | Kraken | Coinbase |
|---|---|---|
| Fixtures (clock 1.4 s fast): book rows | 168 of 18,605 | 38,770 of 110,978 (the whole stream snapshot) |
| Fixtures: trades | 50 of 254 | 103 of 941 |
| Local 60-second recordings (clock within 12 ms): book rows | 50 of 11,555 (the whole stream snapshot, stamped 21.5 ms before the metadata arrived) | 0 of 57,755 |
| Local 60-second recordings: trades | 50 of 174 | 99 of 1,182 |

For those rows `session.instrument(venue, instrument, at=row.event_ts)` returns
`None` in the first session, and the *previous* session's version in every
later one.
`test_r06_a_metadata_version_is_in_effect_for_every_row_of_its_session`. The
roadmap's Phase 1c note, "a version is always in effect for the data that
follows it", is true in record order and false in the order the lookup uses.

**(b) On Coinbase the exponent follows the tick size.** `coinbase.py:265`
reads `price_exp` off `price_increment`: `"0.01"` gives 2 and `"0.1"` gives 1.
Architecture 6.1: "Tick size and lot size are validation rules ... not the
unit of representation. A venue changing a tick size therefore does not
rescale history." For Coinbase it does: a price of 73,469.60 is mantissa
7346960 before such a change and 734696 after it, with nothing on the row to
say which.
`test_r06_a_coinbase_tick_size_change_does_not_rescale_prices`.

**Why it matters.** ADR 0005: "readers rescale using the metadata in force at
each row's time". No reader does, the lookup it would use fails at session
boundaries, and the C++ `MarketEvent` carries neither an exponent nor a
metadata version.

**Suggested fix.** Look metadata up by raw-record order, not by time: the
version in force for a row is the latest `instruments` row of the same
`log_id` with `seq` at or below the row's. Offer that as a session method.
For (b) the owner should decide between pinning an instrument's exponent when
it is first seen, or accepting that it follows the increment and correcting
architecture 6.1 and ADR 0005.

### R03. `knowledge_ts` is stamped before the batch can be read

**What.** `append` reads the clock (`store.py:81`), then writes the files,
then publishes them (`store.py:105`). A session opened in between, with an
`as_of` later than the batch's `knowledge_ts`, does not see the batch. The
same `as_of` afterwards does.

**Evidence.** `test_r03_a_session_opened_after_a_batchs_knowledge_time_always_sees_it`:
a batch with `knowledge_ts` 1000; a session at `as_of` 2000 opened just before
the publish sees 0 rows, and afterwards 1.

**Why it matters.** The roadmap already says an `as_of` later than the store's
latest knowledge time is not repeatable. This is narrower and not covered by
that rule: the `as_of` is *later* than the batch's knowledge time and still
misses it. A live decision at time W and a backtest at `as_of` = W can then
read different data. The agent tool's rule "`as_of` cannot exceed now" does
not close it.

**Suggested fix.** Cap what a repeatable session may ask for at the knowledge
time of the last *published* batch, not at the clock: refuse or mark an
`as_of` beyond it. Closing the window itself needs the knowledge time to be
taken at publish, which means it cannot live inside the file (it could be the
file name alone). That is a storage change for the owner.

### R07. The finding limit can hide an ERROR behind INFO findings

**What.** `_Scope.add` drops findings once `max_findings` is reached
(`quality.py:196`), in the order the check produces them. The report's exit
status is computed from the findings that were kept (`quality.py:986`).

**Evidence.** Three check snapshots between batches (INFO) followed by one
mismatch (ERROR), with `max_findings` 3: the result holds three INFO findings,
its statistics say `mismatched: 1`, and the report prints "worst: INFO" and
exits 0.
`test_r07_information_findings_never_crowd_out_an_error` and
`test_r07_the_report_fails_when_an_error_was_cut_off_by_the_limit`.

**Why it matters.** ADR 0016 found exactly this for repeated trades and fixed
it for that kind only. With the default limit of 1,000 it takes 1,000 lesser
findings in one check and instrument: about 21 hours of Coinbase at a probe
every 60 s (23 of 29 snapshots on 2026-11-13 were between batches), or about
nine days at the default 600 s. The report is run over date ranges.

**Suggested fix.** Never drop a finding in favour of a less severe one (cap
per severity, or keep the worst), and treat a truncated result as a failure
of the report unless nothing at or above `--fail-on` could have been cut.

### R10. Three rules decide which observation of a trade is stored

**What.** `trades` is the one dataset keyed by the venue's identity rather
than by raw record. Which observation the store shows for a `trade_id`
depends on the path:

| Situation | Stored observation | Source |
|---|---|---|
| Identical repeat inside one normalizer | The first | ADR 0016 |
| Repeat in a log loaded in a separate run | The later (the load with the later `knowledge_ts`) | ADR 0015, as a known limit |
| Conflicting repeat in one batch | The one with the later `event_ts`, then the later `recv_ts` | `session.py:163` |

**Evidence.**

- The command line loads one log per run, so the second row is the normal
  case, not the exception. A trade seen live and resent as history by the
  next session is replaced by the resent copy: its `recv_ts` moves an hour
  later, no `TRADE_REPEAT` is written, and `clock_skew` now counts it as
  backfill. Both venues resend their last 50 to 100 trades on every
  subscribe.
  `test_r10_a_trade_resent_as_history_in_a_later_log_keeps_its_live_observation`.
- `docs/data-core.md` section 5 says a conflicting repeat "is stored, so the
  store shows the later value". When the later observation carries an earlier
  venue time, or the wall clock stepped back between the two, the store shows
  the first value.
  `test_r10_a_conflicting_repeat_shows_the_later_value` (two cases).

**Why it matters.** ADR 0013's rule is that no key or order depends on
`recv_ts`; this tie-break does. And once R01 is fixed by bounding `recv_ts`,
a trade whose stored `recv_ts` moved an hour later disappears from the
simulated past in which it was seen.

**Suggested fix (owner decision).** Key trades by raw record like the other
three datasets (`log_id, seq`, position in the message) and let the session's
`trades` view show one row per `trade_id`: its first observation in
`(log_id, seq)` order. Repeats and conflicts then become queries over stored
observations. They work across separate loads and need no memory in the
normalizer. ADR 0015 rejected `(log_id, trade_id)` because research would
count trades twice; a view removes that objection. At the least, order the
tie-break by `(log_id, seq)` and correct the document.

### R09. One window, two clocks

**What.** Every range method filters `event_ts`. For book rows that is venue
time. For stream events it is the receipt time of the record where the event
was noticed. The same `(start, end)` therefore selects book messages and
their integrity events over ranges that differ by the lag.

**Evidence.** The Coinbase fixture with one book update removed. A window that
ends just before the gap was noticed contains 24 book messages from after the
loss, and not the gap event. `book.replay` over that window finishes without
an error. `test_r09_the_same_window_for_book_rows_and_events_still_finds_a_lost_message`.

**Why it matters.** The roadmap's rule for `replay` is "Always pass the
instrument's `stream_events`". Nothing checks that the events cover the
messages, and the typed methods invite a common window. R08 is a related
effect in the quality checks.

**Suggested fix.** Give stream consumers a range in the order the stream has:
by `(log_id, seq)`, or by receipt time once R01 adds that bound. Have `replay`
take both tables from one call so they cannot be windowed apart.

### D1. `MarketEvent` cannot carry what the book needs to know

**What.** `EventKind` is `BookSnapshot`, `BookDelta`, `BookCheckSnapshot` and
`Trade` (`events.hpp:39`). There is no event for a sequence gap, a new
connection, a rejected message or a disconnect, and no field naming the log
or feed an event came from.

**Evidence.** The reference reconstructor's rules depend on exactly those
facts, read from `stream_events` (`book.py:303`): a stored `SEQUENCE_*`,
`CHECKSUM_MISMATCH` or `MESSAGE_REJECTED` raises, and a session start, connect
or disconnect means the next message must be a snapshot. On Coinbase a lost
book message is "unseen from book messages alone" (roadmap Phase 2), because
heartbeats and trades share the numbering. Heartbeats are not events, and the
`trades` dataset has no venue sequence number to put in a trade event's
`venue_seq`.

**Why it matters.** Phase 4: "the book is marked stale at the same event where
the reference reconstructor raises", driven "with events normalized by the
Phase 1c adapters". For a removed Coinbase message the C++ book cannot know.
In Phase 6 the feed thread detects the gap but the core thread owns the book
(architecture section 5), so the fact has to cross a queue as a message, and
no message for it exists. `seq` restarts in every log, so a journal of
`MarketEvent`s from two logs has no unique key; the Phase 3a note says golden
comparison keys rows by `(log_id, seq)`.

**Suggested fix.** Before Phase 4: add control kinds (reset, gap, rejected),
a source index in the reserved bytes with the mapping to `log_id` held outside
the event, and one sentence fixing when a multi-event message becomes visible
(only at `kLastInMessage`; truncation and the checksum already need that).
A new enum value and a use of reserved bytes do not move any offset.

### D2. The reference book cannot resume, and the recorder never resyncs

**What.** `replay` is a generator that raises at the first integrity failure
(`book.py:363`); a raised generator is finished. The recorder writes bytes
and does not parse them, so after a Coinbase gap it keeps recording a stream
whose next snapshot comes only with the next connection.

**Why it matters.** Phase 4 requires that the C++ book "recovers on the next
snapshot", and there is no reference behavior to compare the recovery with.
For recorded data, one lost Coinbase message makes the rest of that
connection's book unusable for the reconstructor (it "never repairs"), which
on a 24-hour session can be most of a day. The check snapshots taken every
probe interval could reseed a book and are not used for that.

**Suggested fix.** Add a non-raising mode to the reconstructor that marks the
book stale and resumes at the next stream snapshot, and make that the oracle
for Phase 4's recovery criterion. Decide whether the recorder should
reconnect when it sees a gap (it would have to read `sequence_num`), or
whether a check snapshot may reseed a stale book.

### D3. One event per level meets "a full queue means stale and resync"

**What.** `MarketEvent` is one per book level (88 bytes). A Coinbase snapshot
is about 38,000 levels: 38,000 events, 3.3 MB, produced in one burst. ADR 0004
gives the feed-to-core queue the policy "mark the instrument's book stale and
resync" when full.

**Why it matters.** On Coinbase a resync is a reconnect, and a reconnect
delivers another 38,000-event snapshot. Unless the queue holds a whole
snapshot (65,536 entries, 5.8 MB per feed) or the core is guaranteed to drain
during the burst, the remedy reproduces the condition. A partly delivered
snapshot is also a partly built book on the core thread. ADR 0004 does not
mention sizing or multi-event messages.

**Suggested fix.** Settle it in Phase 5's ADR work, before the queue is
built: a capacity rule tied to the largest snapshot, or a distinct path for
snapshots (the feed may wait while it delivers one), and the stale rule for a
message that is only partly in the queue.

### D4. A session per decision time is slow, and slower with more history

**What.** Opening a session lists every file, reads every footer and creates a
DuckDB instance with four views. Each range query resolves the latest version
of every record of the instrument before filtering on time
(`session.py:152` to `177`), because a correction can move a row's `event_ts`.

**Evidence** (this machine, 12 cores):

| Store | Open, one trade query, close | One-second book window |
|---|---|---|
| Both fixtures (150 s, 8 files) | 41 to 44 ms | 25 ms Kraken, 32 ms Coinbase |
| One-hour recordings (46 files; 871,546 Coinbase book rows) | 56 to 71 ms | 135 ms Kraken, 374 ms Coinbase |

**Why it matters.** Phase 7 opens a session per decision time. At one
decision a second, one replayed hour is 3,600 sessions: about four minutes to
open each, run one trade query and close it, and about 22 minutes more if each
also reads one second of the Coinbase book. The window cost grows with the
size of the store, not of the window. There are no book checkpoints either:
reading the book at hour 23 of a session means replaying 23 hours of updates,
and `messages_from_table` holds the range in memory (452 MB peak for one
Coinbase hour, roadmap Phase 2).

**Suggested fix.** Decide with R01, since both change what a session is. The
options are one engine instance per backtest whose bound only moves forward,
version resolution done once per vintage instead of per query, and a derived
top-of-book or periodic-snapshot dataset written through the store for
research that needs the book.

### T1. No test checks when `recv_ts` is taken

**Evidence (mutation).** `recorder.py:214` stamps a message after
`socket.recv()` returns. Moving the stamp to *before* the wait, so that every
record carries the time the previous message was handled, leaves all 20
recorder tests passing.
`test_recv_ts_is_taken_when_each_message_arrives` (`test_recorder.py:274`)
asserts only that the stamps are sorted and later than 2020.

**Why it matters.** `recv_ts` is the simulated "now" of every backtest
(architecture 6.2) and one side of every lag statistic. The sortedness
assertion is also the wrong property: ADR 0012 exists because wall time need
not be sorted, and the test fails if the clock steps back while it runs.

**Suggested fix.** A scripted socket that delivers after a known delay
measured on the injected clocks, asserting each record's stamp falls between
delivery and the next read.

### T2. The "no other read path" guards cannot see what they guard

**Evidence (mutations, both reverted).**

- `test_the_data_package_exports_no_other_read_path` (`test_session.py:441`)
  inspects `dir(quantagent.data)`, which lists only the submodules already
  imported. Run alone it sees 5 of the 12. A new module
  `quantagent/data/leak.py` that reads the store with
  `pyarrow.parquet.ParquetDataset` passes it. Its comment says "Adding a
  module here is a deliberate act."
- `test_the_reconstructor_reads_no_files_itself` (`test_book.py:715`) searches
  the source text for six strings. `pyarrow.parquet` is not one of them,
  because `book.py` imports it to write golden files; a function in `book.py`
  that reads store files with `pq.ParquetDataset` passes. The test's last
  line asserts that a pyarrow table is a pyarrow table.

**Why it matters.** CLAUDE.md safety rule 9 rests on these until the import
lint of Phase 12 exists.

**Suggested fix.** Enumerate modules from disk (`pkgutil.iter_modules`), and
bring the import lint forward as an AST check on imports and calls rather than
a text search. The project already holds SQL to that standard ("not by
searching the text for forbidden words").

---

## Low

### R08. A session's clock error is reported only where the session started

`check_recording_clock` (`quality.py:445`) reads the session start and clock
events in the range. They sit at the start of the log. A session that begins
at 23:59 with a clock 1.4 s off is an ERROR in that day's report and nothing
in the next day's, whose data has the same error.
`test_r08_a_bad_recording_clock_is_reported_for_every_day_the_session_covers`.
By reading, the same holds for `reconnect` in `stream_integrity`: the first
connect in a range is never counted as one. **Fix:** find the sessions by the
`log_id`s of the rows in the range, and read each one's start and clock events
wherever they are.

### R11. One normalizer fed two logs in arrival order mixes them up

`Normalizer` keeps one current log id and one adapter per venue
(`base.py:430`). `logorder.merge_logs` yields exactly the interleaving that
breaks it. Feeding 400 records of two Kraken logs alternately gives 1,102 book
rows with 551 distinct keys, all named after the second log, no reject and no
warning.
`test_r11_records_of_two_logs_fed_in_arrival_order_keep_their_own_log_ids`.
ADR 0015 presents "two logs fed to one normalizer" as the way to compare
trades across logs. **Fix:** refuse a record whose `seq` is neither 0 nor the
previous one plus one, or keep state per log.

### D5. `recv_ts` in recordings is dequeue time, and probes share the loop

The recorder is one asyncio loop. A message is stamped when its coroutine
resumes, not when its bytes arrived, and a 4 MB Coinbase probe snapshot is
received, encoded and flushed on the same loop and downloaded over the same
link. On the local 30-minute recording (median stream lag 94.7 ms), the lag
within six messages after a probe snapshot was 100 to 375 ms above the median
for 9 of 28 probes. For three of those it was already raised just before, so
loop blocking and link contention cannot be separated from this data. Not
reproducible in CI; no test. **Fix:** take probes in a separate process, and
say in the documents what the stamp measures. Phase 6's feed threads remove
the problem for live data, not for the recordings backtests will use.

### D6. The `clock_skew` band was calibrated on the wrong clock

The defaults (−0.5 s to 10 s) were "chosen from the recorded data (worst live
lag 2.3 s)", data whose clock ran 1.4 s fast. `test_quality.py:134` asserts
the check finds nothing on those fixtures. With a synchronised clock the lag
is about 95 ms, so the band now tolerates a clock that drifts by seconds
within a session, which is the case `recording_clock` cannot see because it
measures at session start only (ADR 0011). **Fix:** tighten the defaults from
the synchronised recordings, and measure the clock again at session stop, as
ADR 0011 already suggests.

### D7. C++ core types

No correctness bug was found in the arithmetic, the layouts or the bindings.
Four inconsistencies:

- `InstrumentId` is constructible from any integer and narrows with a cast
  (`types.hpp:80`): `InstrumentId(std::int64_t{1} << 32)` is id 0. The roadmap
  lists it among the types with "Construction only from integers that fit
  without loss". The header says callers keep ids in range.
- `MarketEvent::depth` is 16 bits (`events.hpp:66`); the Python schema stores
  depth as int32 and the adapters accept up to 100,000 (`base.py:53`).
- `MarketEvent.from_bytes` refuses unknown enum values and non-zero reserved
  bytes but accepts the one unassigned flag bit (0x80).
- `InstrumentMeta` accepts any int8 exponent; the Python schema allows 0 to 18.

No failing C++ test was added: a failing `ctest` step stops `check.ps1` before
pytest, which would hide the 23 Python failures in CI. **Fix:** constrain
`InstrumentId` to `uint32`-representable types as `Price` does for int64, and
make the Phase 4 converter reject what `MarketEvent` cannot hold.

### T3. Tests whose names promise more than they check

| Test | What it actually checks |
|---|---|
| `test_no_method_returns_a_row_known_after_as_of` (`test_session_property.py:138`) | Three of the four datasets. `stream_events`, added in Phase 1d, is not in it |
| `test_every_dataset_is_at_schema_version_3...` and the other "every dataset" tests (`test_schema.py:18`) | `ALL` is three datasets; `stream_events` is version 2 and absent |
| `test_no_key_depends_on_receive_time` (`test_schema.py:391`) | `... or dataset is TRADES` exempts nothing: `event_ts` is in no key |
| `test_checks_only_see_what_was_known_at_as_of` (`test_quality.py:636`) | "Before everything" against "after everything". It cannot see R02 |
| `test_normalizing_a_log_again_keeps_its_id_so_rows_are_corrections` (`test_log_id.py:174`) | Reloads identical output. It cannot see R04 |
| `SystemClockReadsTheWallClockAndAMonotonicCounter` (`cpp/tests/test_clock.cpp`) | Asserts on the wall clock, which CLAUDE.md forbids for C++ tests. Unavoidable for this class; the rule needs the exception written down |

### X. Where documents and code disagree

| # | Document says | Code does |
|---|---|---|
| X1 | Architecture 3.2 and 4.3, roadmap Phase 7: a backtest session has `as_of` equal to simulated time | Such a session is empty (R01) |
| X2 | `quality.py:519`, ADR 0011 context: a bad recording clock puts `knowledge_ts` off | `knowledge_ts` is the loading machine's clock at load time. Nothing measures that clock |
| X3 | Architecture 6.1, ADR 0005: the exponent is the venue's finest precision, and a tick change does not rescale | Coinbase's exponent is read off the tick (R06) |
| X4 | Data-core section 5: a conflicting repeat "is stored, so the store shows the later value" | Later `event_ts`, then later `recv_ts`, within a batch (R10) |
| X5 | ADR 0015: within one batch the view keeps "the later `recv_ts`" | `event_ts` is compared first (`session.py:163`) |
| X6 | ADR 0009: keys make it "safe to fix an adapter bug and re-ingest" | Only while the fix removes no row (R04) |
| X7 | Roadmap Phase 1c: malformed messages are "never a crash" | R05 |
| X8 | Data-core section 3: "a failed multi-partition append publishes nothing" | True for an exception. By reading, a process killed between two hard links leaves part of the batch published |
| X9 | `README.md`: "Status: skeleton and design only (roadmap Phase 0)" | Phases 1a to 3d are built |
| X10 | `quantagent/data/__init__.py`: "currently holds the write side only"; `PITSession` "arrives in roadmap Phase 1b" | It arrived |
| X11 | `PITSession.sql` docstring: the views `trades`, `book_events` and `instruments` | There are four; `stream_events` is missing |
| X12 | Data-core introduction: "The decisions behind it are ADRs 0005, 0006, 0008, 0009 and 0010" | Also 0011 to 0016 |
| X13 | Roadmap Phase 3: construction "only from integers that fit without loss" | Not `InstrumentId` (D7) |
| X14 | `--as-of` defaults | `quality` defaults to now; `book` defaults to the end of time, the non-repeatable kind |

Smaller: architecture 6.2 still says `event_ts_source` "arrives with Phase 1c";
`PITSession.sql` fails with a binder error naming
`json_execute_serialized_sql` for any query whose result has two columns of
the same name (`SELECT * FROM trades a, trades b`), which is a confusing way
to learn it.

### A. Whether ADRs 0001 to 0016 still form one design

They mostly do. The amendment discipline (new ADR, status line updated, old
text kept) has been followed, and each correction of an earlier ADR's facts
is recorded where it was found. Four things have drifted.

**A1. Two time axes are used as one (R01).** ADR 0006 defines `knowledge_ts`
as when a row became available in the store. Architecture 3.2 and 8.1, ADR
0007 and the roadmap then use `as_of` as "simulated time", "now" and the
boundary of the holdout. ADR 0011 groups `knowledge_ts` with `recv_ts` as a
product of the recording clock. These are consistent only if the store is
loaded live, which it is not. One ADR should state the axes (venue time,
receipt time, store version) and which one each rule is about. This is the
only contradiction with consequences.

**A2. "No key or order uses `recv_ts`" (ADR 0013) has an exception nobody
decided.** The view's tie-break and ADR 0015's description of it depend on
`event_ts` and `recv_ts`. See R10.

**A3. Trades are the special case that keeps generating ADRs.** ADR 0013 left
them keyed by `trade_id`; 0015 added conflict detection in the normalizer;
0016 added repeat dropping in the normalizer; both carry the limit "only
within one normalizer". Three resolution rules now exist (R10). Keying trades
by raw record, with a one-row-per-trade view, would fold 0015's and 0016's
mechanisms into one query and remove the special case.

**A4. The `stream_events` contract is spread over six ADRs.** The kinds and
the meaning of `expected`, `observed` and `detail` are defined in ADR 0010
(12 kinds) and added to by 0011, 0012, 0014, 0015 and 0016 (six more), with
the only complete list in `schema.py` comments and data-core section 2. A reader
(Phase 4's converter, the agent's health tool) needs one table. Make
data-core the reference and have ADRs say only "adds kind X".

Smaller points:

- ADR 0013 changed the `instruments` key that ADR 0008 defined ("is the
  version's key"), but its `Amends` line omits 0008 and 0008's status does not
  mention 0013.
- ADR 0011's assumption covers "every component that stamps wall-clock time".
  The store stamps `knowledge_ts` and is not covered.
- ADR 0004 was written before `MarketEvent` became one per level (D3).
- ADR 0014's check-snapshot grading is spread over 0014 (INFO), 0015
  (WARNING per session) and 0016 (confirmation). It reads correctly only in
  data-core section 6.

### Minor notes

By reading only; none has a test.

- `clock.sntp_query` reads the clock before `sendto`, which resolves the host
  name. The first sample of each server therefore counts the DNS lookup as
  round trip and shifts its offset by half of it. The fastest-of-three rule
  hides this when the resolver caches.
- With exactly two servers answering, `measure_offset` takes the lower of the
  two offsets, so one wrong server can decide the result. "One wrong server
  cannot move it far" holds from three.
- NTP's 32-bit seconds wrap in 2036; `_to_ntp` will raise then.
- A Coinbase connection whose first received message is not number 0 produces
  no event: numbering is checked only against the previous message.
- One writer per store root is assumed, not locked. Two `ingest` processes
  with a wall clock that steps back can publish a knowledge time below one
  already issued, which changes what a past `as_of` returns.

---

## What was checked and held

- **The SQL allow-list.** About 90 adversarial statements against a store with
  a row after `as_of` (table functions, replacement scans, catalog tables,
  time travel, sampling, lambdas, parameters, casts of `'now'`, clock and
  setting functions, positional joins, `PIVOT`, multi-statement input). None
  returned a row or value from after `as_of`, and none read the clock.
- **Adapter robustness up to the normalizer.** 2,175 malformed variants of
  every message shape in both fixtures: no exception escaped
  `Normalizer.feed` (R05 is past that point).
- **The 128-bit multiply, `to_int64`, the message layouts and the bindings'
  integer checks**, by reading and by the existing tests.
- **The Coinbase check-snapshot rule** in the adapter and the reconstructor,
  by reading both against ADR 0014. No disagreement found.
- **Raw log framing and `merge_logs`**, by reading. The recorder stamps and
  writes without an `await` in between, so monotonic stamps cannot decrease
  within a log.

## What was not verified

- Nothing was run against a venue or an NTP server. The `network` tests were
  not run.
- The `local_data` tests were not run as tests. Local recordings were used
  for measurements only: the lag figures in R01, the second half of R06's
  table, D4 and D5. Those numbers cannot be reproduced from the repository.
- The Linux preset and the `__int128` path are unbuilt, as the roadmap says.
- Venue behavior was taken from the recordings and the code's citations, not
  re-read from the venues' documentation. In particular the Coinbase trade
  side mapping was not re-derived.
- The claims in the roadmap's "breakage pass" paragraphs were not repeated,
  apart from the three mutations in T1 and T2.
- D5's cause is not established.

## Suggested order of work

1. Decide R01 (the time axes) with the owner. R03, R09, R10 and D4 depend on
   the answer.
2. Fix R05 and R02's append order. Both are small and stop a load from
   ending badly.
3. Decide R04 and R10 together: both are about what a key means.
4. Settle D1 to D3 in the Phase 4 and Phase 5 plans before code is written
   against `MarketEvent`.
5. R06, R07, R08, R11, T1, T2, then the documents (X) and ADR housekeeping (A).
