# 0020: Stream control events, a resuming reference book, and large snapshots against a bounded queue

Status: Accepted (owner decisions, 2026-11-16; implementation choices, D3's numbers and the phases for D4 to D7 and T3 confirmed by the owner 2026-11-16, see "Owner's answers" and "Review"); amended by 0022 (`MarketEvent` gains a ninth kind, `InstrumentUpdate`)
Date: 2026-11-16
Amends: 0004 (the full-queue policy, for messages of many events). Restated in `docs/data-core-spec.md` (sections 6, 11, 12) and `docs/architecture.md` (sections 4.1, 5, 6.3)

## Context

The 2026-11 review (`docs/reviews/2026-11-data-core.md`) left three design
findings open that Phase 4 and Phase 5 cannot be built without:

- **D1.** `MarketEvent` had four kinds (snapshot level, delta level, check
  snapshot level, trade) and no way to say that a stream broke, that a new
  connection began, or which stream an event came from. The reference book's
  rules depend on exactly those facts, read from stored stream events. On
  Coinbase a lost book message is invisible in the book messages (heartbeats
  and trades share the numbering), so the C++ book could not go stale "at the
  same event where the reference reconstructor raises" (Phase 4) from book
  events alone. `seq` restarts in every log, so a journal of events from two
  logs had no key.
- **D2.** The reference book raised at the first integrity failure and could
  not continue, so there was no oracle for Phase 4's "recovers on the next
  snapshot".
- **D3.** A Coinbase snapshot is about 38,000 events (3.3 MB) in one burst.
  ADR 0004's policy for a full market-data queue, "mark the book stale and
  resync", loops on Coinbase, where a resync is a reconnect and a reconnect
  sends another 38,000-event snapshot. A partly delivered snapshot is a partly
  built book.

ADR 0019 also left one consequence: the C++ `InstrumentMeta` had no
precisions, and the C++ book needs them to verify Kraken's checksum (its
digits are the mantissa divided by `10^(exp - precision)`).

## Decision

The owner decided on 2026-11-16:

1. **D1: `MarketEvent` gains control kinds**: stream stale (a gap, a checksum
   mismatch, a rejected message), connection or session reset (the next book
   message must be a snapshot), and snapshot begin and end framing. **It also
   carries a compact numeric source id**, which the harness maps from
   `log_id` and connection, so the C++ side knows which stream an event
   belongs to. The layout, its `static_assert`s, the bindings and the tests
   change with it.
2. **The C++ `InstrumentMeta` gains `price_precision` and `qty_precision`**,
   so the C++ book can verify Kraken's checksum.
3. **D2: the reference book gains a resume mode.** On an integrity failure it
   marks the book stale and continues, and recovers at the next snapshot.
   Strict mode (raise) stays the default, for `verify` too. The golden files
   gain the stale and recovered points, so Phase 4 can check that the C++
   book goes stale and recovers at exactly the same events.
4. **D3: the policy for a large snapshot against a bounded queue** is written
   down here, framed by the begin and end events of decision 1. Its
   implementation belongs to Phase 5; the Phase 5 acceptance criteria it
   implies are added to the roadmap.
5. **D4 to D7 and T3** are listed below with a recommendation and a phase
   each. None is implemented.

Asked on the pull request (2026-11-16), the owner also decided:

6. **The recommendations for D4 to D7 and T3 stand, in the phases given.**
   The `SystemClock` exception to CLAUDE.md's test rules is approved (written
   in this change; the rest of T3 is Phase 4's first commit).
7. **D3's numbers stand:** at least twice the largest snapshot, a message
   that does not fit dropped whole rather than the feed waiting, and resyncs
   gated on room with a growing backoff.
8. **A check snapshot may not reseed a stale book.**
9. **The recorder does not resubscribe on a gap.** Instead, in the recorder
   phase (with D5 and D6, before Phase 8; roadmap Phase 7a), it reconnects
   its stream on a fixed interval, configurable, between 15 and 30 minutes,
   so a gap costs at most one interval of stale book in recorded data. A
   scheduled reconnect must not be a quality WARNING, as an unscheduled one
   is.

## How it is built

### `MarketEvent` (D1)

Two of the seven reserved bytes are used; no offset moves and the size stays
88 bytes.

| Offset | Size | Field | Before | Now |
|--------|------|-------|--------|-----|
| 72 | 4 | `level_index` | Position of the level in its message | Also: the level count for `SnapshotBegin` and `SnapshotEnd`; the stored stream event's `event_index` for `StreamStale` and `StreamReset` |
| 78 | 1 | `kind` | 0 to 3 | 0 to 7 |
| 81 | 1 | `reason` | Reserved | `StreamReason` |
| 82 | 2 | `reserved` | Reserved | Reserved, zero |
| 84 | 4 | `source` | Reserved | The stream the event belongs to |

**Kinds** (`EventKind`, `uint8`; 0 to 3 unchanged and equal to the Python
schema's `BookEventKind` where they overlap):

| # | Kind | Meaning |
|---|------|---------|
| 4 | `StreamStale` | The source's book cannot be trusted until its next snapshot. `reason` says why |
| 5 | `StreamReset` | A new session or connection: the book is gone, and the next book message must be a snapshot. `reason` says which |
| 6 | `SnapshotBegin` | The levels of one snapshot message follow; `level_index` is how many |
| 7 | `SnapshotEnd` | They are complete; `level_index` is how many |

**Reasons** (`StreamReason`, `uint8`). Each is a stored stream event kind
(spec section 6); the Python reference book names them in snake case
(`book.STALE_REASONS`, `book.RESET_REASONS`), and a test checks that the two
lists agree.

| # | Reason | Kind | From the stored stream event |
|---|--------|------|------------------------------|
| 0 | `NoReason` | Every kind except the two below | |
| 1 | `SequenceGap` | `StreamStale` | `SEQUENCE_GAP` |
| 2 | `SequenceRepeat` | `StreamStale` | `SEQUENCE_REPEAT` |
| 3 | `SequenceOutOfOrder` | `StreamStale` | `SEQUENCE_OUT_OF_ORDER` |
| 4 | `ChecksumMismatch` | `StreamStale` | `CHECKSUM_MISMATCH` |
| 5 | `MessageRejected` | `StreamStale` | `MESSAGE_REJECTED` |
| 6 | `SessionStart` | `StreamReset` | `SESSION_START` |
| 7 | `Connect` | `StreamReset` | `CONNECT` |
| 8 | `Disconnect` | `StreamReset` | `DISCONNECT` |

Only stored events on the stream connection (`ws`) or of the session
(connection null) become control events, as the reference book has always
read them; events of the probe connection do not concern the stream, and
every other kind of stored event (subscriptions, backfill, clock, check
snapshot comparisons, trade repeats and conflicts) concerns no book.

**Rules:**

- **A source is one connection of one raw log:** `(log_id, "ws")` or
  `(log_id, "probe")`. The harness (in Phase 7) or a feed handler (in Phase
  6) numbers them and keeps the mapping outside the event; a session-level
  event (connection null) belongs to the log's stream source. The stream's
  reconnects share its source and are separated by `StreamReset`s. Stream
  snapshots, deltas and trades are on the stream source, check snapshots on
  the probe source.
- **`(source, seq)` names the raw record.** The events of one record are
  contiguous, its control events (in `event_index` order) before its levels.
  A control event's `level_index` is the `event_index` of the stored stream
  event it stands for, so it points back at the stored row that explains it.
- **When a book message takes effect** (the review's "one sentence"): an
  update's levels take effect together, at the level flagged
  `kLastInMessage`; a snapshot's at its `SnapshotEnd`. Nothing reads the book
  in between.
- **A snapshot is framed**, stream and check snapshots alike: `SnapshotBegin`,
  its levels (`level_index` 0 to n-1, the last also flagged
  `kLastInMessage`), then `SnapshotEnd`, all with one source and `seq`, both
  frame events carrying n, and nothing else between them.
- **Control events carry no market values.** Price, quantity, trade id,
  checksum and depth are zero and the side is `Unknown`. A gap may say which
  venue number arrived (`venue_seq`, flag `kHasVenueSeq`); every other detail
  (expected and observed values) stays in the stored stream event. Their
  `event_ts` is the receipt time with `kEventTsFromReceipt`, as for every
  stream event.
- **`from_bytes` refuses** a kind above 7, a reason above 8, a reason that
  does not fit its kind (a stale or reset event without its kind of reason,
  any other event with one), a control event carrying a market value, and
  non-zero reserved bytes. `qa::is_stream_control` and `qa::reason_fits` are
  the C++ rules it uses.
- **Not built here:** the Python converter from `BookMessage`s and stored
  stream events to `MarketEvent`s. Nothing uses it before Phase 4, which
  builds it to these rules.

### `InstrumentMeta`

`price_precision` and `qty_precision` (`int8`) follow the exponents at offsets
6 and 7. They fill what was padding: the struct stays 24 bytes and now has no
hidden bytes (`has_unique_object_representations`). The binding's constructor
takes them after the exponents, in the struct's and the schema's order, with
no default, and refuses a precision outside 0 to 18 or above its exponent, as
the schema does. The exponents themselves are still not range-checked (D7).

### The resuming reference book (D2)

- **`book.trace(venue, messages, events, *, verify=True, mode="strict")`**
  yields every step the C++ book will be given, with the book after it (None
  when it cannot be read): each stream message, each stored stream event that
  makes the book stale, and each reset. A record's stored events come before
  its message. A `Step` names its record (`log_id`, `seq`, `recv_ts`) and says
  `step` (`message`, `stale` or `reset`), `reason`, `stale` (after the step),
  `recovered`, and `error` (what strict mode raises there).
- **Strict mode** raises at the first failure, after yielding the steps
  before it: exactly what `replay` did. **Resume mode** records the failure as
  a step and continues:
  - a stored stale event is a `stale` step; a failure found at a message (its
    own Kraken checksum, Coinbase book numbers not increasing, an update with
    no book) makes that message's step stale, with the reason
    (`checksum_mismatch`, `sequence_repeat`, `sequence_out_of_order`,
    `delta_before_snapshot`);
  - a stale book is discarded, and updates are not applied to it;
  - the next stream snapshot is applied and checked; if it passes the venue's
    check the book is fresh again and the step is `recovered`; if it fails,
    the book stays stale;
  - a reset empties the book. It neither causes nor ends staleness: only a
    snapshot ends it, and an update after a reset is stale
    (`delta_before_snapshot`).
  - Errors that are not integrity failures (a Kraken message without a
    checksum or metadata, a Coinbase message without a number) still raise.
- **Resume mode goes stale exactly where strict mode raises**: on every kind
  of failure the first stale step names the record and error strict mode
  raises, and the steps before it are the same (tested).
- `replay`, `verify_check_snapshots` and `golden_table` take `mode` too. In
  resume mode `replay` yields None for the book while it is stale, and a check
  snapshot whose A finds the book stale is not compared, with the Coinbase
  adapter's reason for it, `stream_unreliable`.
- **Command line:** `verify --resume` prints each stale period, where it
  recovered and how many stream messages were not applied, and still exits 1
  if the book was ever stale; `golden --resume` writes the points into the
  table. Strict `golden` on a broken store now prints `FAILED` and exits 1.
- **Golden files** (`python/tests/fixtures/golden/`, README there): a row per
  step, with `step`, `reason`, `stale` and `recovered` after `recv_ts`, and
  every level null while the book cannot be read. On the clean fixtures the
  only new rows are each stream's three resets (session start, connect,
  disconnect); every message row is unchanged.
- **Integrity points** (`golden/integrity-points.json`, built by
  `python/tests/data/integrity.py`): eight damaged cases, each listing every
  step that is not an ordinary message. The fixtures have one stream snapshot
  each, so a damaged fixture could never show a recovery; a new test-side
  repair, `damage.resnapshot`, replaces a stream heartbeat with a snapshot of
  the true book (what resubscribing would deliver; Coinbase's keeps the
  heartbeat's `sequence_num`), applied before the damage. The cases are the
  Phase 2 gap tests' damage (removed, duplicated, swapped) on both venues,
  plus one per venue replayed with no stored events, which is how the C++
  book's own checks are tested.

| Case | Stale at (damaged log's seq) | Recovered at |
|------|------------------------------|--------------|
| `kraken-removed` | 265, `checksum_mismatch` (stored) | 12491 |
| `kraken-removed-own-check` | 265, `checksum_mismatch` (its own check) | 12491 |
| `kraken-swapped` | 265, `checksum_mismatch` (stored) | 12492 |
| `kraken-duplicated` | Never: Kraken's rule cannot see a repeat (Phase 2) | |
| `coinbase-removed` | 630, `sequence_gap` | 2487 |
| `coinbase-duplicated` | 631, `sequence_repeat` (stored) | 2489 |
| `coinbase-duplicated-own-check` | 631, `sequence_repeat` (its own check) | 2489 |
| `coinbase-swapped` | 630, `sequence_gap`, then 631 `sequence_out_of_order` | 2488 |

After the recovery the book equals the undamaged log's: its best 25 levels a
side after every later message, and the whole book at the recovery, every
200th message and the last (tested on `kraken-removed` and
`coinbase-removed`).

### Large snapshots against a bounded queue (D3, for Phase 5)

The rules, for every feed-to-core market data queue:

1. **Framed snapshots.** Every snapshot crosses the queue as `SnapshotBegin`,
   its levels and `SnapshotEnd` (decision 1).
2. **Whole messages only.** A producer pushes a book message's events only
   when the queue has room for all of them at once: n + 2 for a snapshot, n
   for an update. In a single-producer queue only the consumer frees room, so
   room the producer sees stays until it pushes. A message is therefore never
   partly in the queue, and the core never meets a frame cut short by a full
   queue.
3. **Capacity.** Each such queue is allocated at startup with room for at
   least twice the largest snapshot of the instruments its feed carries,
   rounded up to a power of two. The largest snapshot is configuration per
   instrument, starting from the recorded maxima: Coinbase BTC-USD's largest
   recorded is 38,532 levels, so with a configured 40,000 the queue holds
   131,072 events (11.5 MB). Kraken's books are its subscribed depth (25
   levels a side).
4. **A message that does not fit** is dropped whole, never in part. The
   source is stale from then on: the feed delivers a `StreamStale` with a new
   reason, `QueueOverflow` (added in Phase 5), as soon as there is room for it
   and before anything else of that source, and sends that source nothing more
   until it has resynced. Nothing is dropped silently.
5. **Resync without the loop.** The feed asks for a resync (Coinbase: a
   reconnect; Kraken: a resubscribe) only once the queue has room for a whole
   snapshot of the instrument, and at most once per backoff interval, which
   grows while resyncs keep failing. The snapshot the resync brings then fits
   by rule 2, unless other sources fill the queue in the meantime; then it is
   dropped whole again and the backoff grows. A core that stays slower than
   its feeds is an overload: its books stay stale, and stale books produce no
   orders (pre-trade risk, Phase 9).
6. **The core** builds a snapshot only from a complete frame: the levels apply
   to the book being rebuilt, and at a `SnapshotEnd` whose count matches the
   levels received the book is fresh. A frame that is not complete (a count
   that does not match, an event of another kind or source inside it) leaves
   the book stale. By rule 2 that cannot come from the queue; the book checks
   it anyway (Phase 4).

What it implies for Phase 5 (added to its criteria): an all-or-nothing push of
n elements, never leaving part of a batch in the queue; the two-thread stress
test with framed bursts of a Coinbase-sized snapshot, the consumer never
seeing a partial frame; a capacity helper for rule 3; and the burst in the
benchmark. Phase 6's handlers produce frames and drop whole messages with the
stale marker (rule 4); Phase 11 sizes the queues from configuration and gates
resyncs (rules 3 and 5).

## Recommendations for D4 to D7 and T3

Confirmed by the owner, with their phases (decision 6); none is implemented
here except the CLAUDE.md exception of T3. D5 and D6 are joined in the
recorder phase by decision 9's periodic reconnect.

| Finding | Where it stands | Recommendation | Phase |
|---------|-----------------|----------------|-------|
| **D4.** A session per decision time is slow, and slower with history | The replay cursor (ADR 0017) removed the cost of opening a session per decision. A range query still resolves record versions over everything the session can see before filtering on the window (spec 12) | The cursor's version is fixed for its life, so it can resolve versions once, when it opens, and `advance` then only filters on receipt time. Measure a one-hour replay at one decision a second, each reading a one-second book window, record it as the baseline, and set a budget from it. A derived top-of-book or checkpoint dataset only if Phase 12's features need the book in research | 7 (replay engine), where the cost first matters; the derived dataset in 12 if needed |
| **D5.** `recv_ts` in recordings is when the recorder's coroutine resumed; probes share its event loop and link | Unchanged; stated in spec 12 | Take the probe connection on its own thread with its own event loop, stamping its records there, with one writer for the log; then measure the stream's lag around probes again. Record the data that latency models will be calibrated on after that | A small recorder phase before Phase 8, Phase 7a (the simulated exchange models latency from these stamps) |
| **D6.** The `clock_skew` band was calibrated on data with a 1.4 s clock error | Unchanged. The synchronized 60-second recordings of 2026-11-17 had median lags of 0.154 s (Kraken) and 0.599 s (Coinbase), well above the ~95 ms seen before, unexplained | Measure the clock again at session stop (a second `CLOCK_OFFSET`, compared by `recording_clock`); explain the lag before tightening anything; then set the band from synchronized recordings | With D5, Phase 7a |
| **D7.** C++ nits | `InstrumentId` fixed in Phase 3g. Open: `depth` is 16 bits while the schema stores int32 and the adapters accept up to 100,000; `from_bytes` accepts the unassigned flag bit 0x80; `InstrumentMeta` accepts any int8 exponent | `from_bytes` refuses unknown flag bits; the Phase 4 converter refuses a depth above 65,535 and an exponent outside 0 to 18 (no schema change: Kraken's largest depth is 1,000); the `InstrumentMeta` binding refuses exponents outside 0 to 18 as it now does precisions | 4 |
| **T3.** Six tests whose names promised more than they checked | Two are resolved: the property test (renamed in Phase 3e) covers `stream_events`, and the quality test now checks both cutoffs at their boundaries (`test_checks_only_see_what_the_session_sees`). A reload that removes rows is now tested in `test_loads.py`. Open: `test_schema.py`'s `ALL` holds three datasets, not `stream_events`; `test_no_key_depends_on_receive_time` still exempts `TRADES`, which no longer needs it; `test_normalizing_a_log_again_keeps_its_id_so_rows_are_corrections` checks an identical reload only; the C++ `SystemClock` test asserts on the wall clock, which CLAUDE.md forbids without an exception written down | Add `STREAM_EVENTS` to `ALL` (with a row factory); drop the `TRADES` exemption; rename the reload test to what it checks; write the `SystemClock` exception into CLAUDE.md's test rules (a CLAUDE.md change, so the owner's) | Test-only: the first commit of Phase 4's session, before its own work |

## Alternatives considered

**D1, the control events**

- **One kind per failure** (`SequenceGap`, `ChecksumMismatch`, ...) instead
  of one stale kind with a reason. The book does the same for all of them; a
  reason byte keeps the kinds few and the book's switch small.
- **No reset kind: a reset as a stale event.** A reset empties the book but is
  not a failure; folding it into stale would make every session start a stale
  period and every first snapshot a "recovery".
- **The source as `uint16`** (65,536 sources), in bytes 82 and 83. Enough for
  about three and a half years of hourly logs with two sources each, but it
  would use the last reserved bytes, and a journal over a longer history, or
  over logs cut more often, would not fit. Four bytes at offset 84 were
  reserved anyway.
- **The source as a strong type** like `InstrumentId`. Safer against mixing
  the two ids, but it is a field the harness writes and the book compares; a
  plain `uint32` is enough until something does arithmetic on it.
- **Expected and observed numbers on the stale event.** They do not change
  what the book does; the stored event holds them and the control event
  points at it by `event_index`.
- **Frame stream snapshots only.** Check snapshots are as large (38,532
  levels on Coinbase) and cross the same queue in Phase 6.
- **Mark the end of a snapshot only by `kLastInMessage`.** An empty snapshot
  has no level to flag, and a frame cut short by a full queue would look like
  a short snapshot; `SnapshotBegin` carries the count to check against.

**D2, the resuming book**

- **Keep applying updates to a stale book.** The book would drift in ways the
  C++ book would have to reproduce exactly to match the golden files, for no
  use: a stale book is not read.
- **Let a reset end staleness.** A new connection is not a good book until its
  snapshot arrives; only a snapshot that passes its check proves the book.
- **Reseed a stale book from a check snapshot.** A probe snapshot is not
  aligned to the stream (ADR 0014's between-batches rule makes it inexact on
  Coinbase). Left as an open question below.
- **Commit golden tables of the damaged cases.** About 0.5 MB each for the
  same information the eight cases' points give in 8 KB; the reference
  rebuilds the full tables on demand.
- **Build recovery cases from two logs** (a damaged log and the fixture again
  as a later session). It tests a session start, not the in-session
  resubscribe the venues actually do; `resnapshot` keeps one log.

**D3, the queue**

- **Capacity only** (hold one snapshot). Gives no rule for when the queue is
  full anyway, and allows a frame to be cut short.
- **The feed waits for room at a snapshot.** No drop, but the feed stops
  reading its socket and its venue may drop the connection; feed threads
  block only on their socket (architecture section 5). It could be a
  configured wait before rule 4 applies, if Phase 11 finds drops in practice.
- **A separate snapshot path** (a preallocated book image handed over by
  index). No per-level events, but a second channel crosses threads, which
  ADR 0004 avoids, and the journal would need it too.
- **Send only the top N levels of a Coinbase snapshot.** A later update
  deeper than N, or one that removes the levels above it, would leave the
  book wrong.

## Consequences

- **The golden files were regenerated:** three reset rows each (13,785 and
  2,705 rows), four new columns, every message row unchanged in every
  previous column (compared). `integrity-points.json` is new.
- **Phase 4's criteria change** (roadmap): the book takes framed snapshots
  and control events, goes stale on `StreamStale`, its own checks and an
  update with no book, recovers at a complete snapshot that passes its check,
  and is compared with the golden files and the integrity points step by
  step. Phase 4 writes the converter.
- **ADR 0004's full-queue rule for market data** is refined by D3: whole
  messages only, and resyncs gated on room. Its other edges are unchanged.
- **`MarketEvent` has two reserved bytes left** (82 and 83).
- **Recorded data recovers within a session only at a new snapshot.** Until
  the recorder phase, the recorder never reconnects on purpose, so a lost
  Coinbase message leaves the rest of that connection stale. After it, the
  stream is reconnected every 15 to 30 minutes (decision 9), so a gap costs
  at most one interval.
- **The Python suite grows by about a minute** (`test_book_resume.py` took
  65 s alone; the eight damaged cases are replayed and compared on every run).

## Owner's answers

The questions this ADR first left open, and the owner's answers on the pull
request (2026-11-16):

1. **Should the recorder resubscribe when it sees a gap** (it would have to
   read Coinbase's `sequence_num`), so recorded data recovers within a
   session? *Recommended: not now.* **Answer: no gap-triggered resubscribe;
   the recorder reconnects its stream every 15 to 30 minutes, configurable,
   in the recorder phase** (decision 9). It stays a byte recorder: the
   reconnect is on a timer, not on anything it parses.
2. **May a check snapshot reseed a stale book?** *Recommended: no.*
   **Answer: no** (decision 8).
3. **D3's numbers:** twice the largest snapshot, drop whole rather than wait,
   resync gated on room with a growing backoff. **Answer: confirmed**
   (decision 7).
4. **D4 to D7 and T3:** the phases recommended above. **Answer: confirmed,
   with the `SystemClock` exception approved** (decision 6).

## Review

Choices made while building it. **Confirmed by the owner, 2026-11-16: all
fifteen, as written.**

1. **Kind numbers and names:** `StreamStale` 4, `StreamReset` 5,
   `SnapshotBegin` 6, `SnapshotEnd` 7; one `StreamReason` byte, 1 to 5 stale
   and 6 to 8 reset; `NoReason` for 0 (Python cannot name an enum member
   `None`).
2. **The source:** a plain `uint32` at offset 84; one per (log, connection
   name), so a stream's reconnects share it; session-level events belong to
   the stream source.
3. **Two bytes left reserved** (82 and 83).
4. **Control events:** `level_index` is the stored event's `event_index`;
   `venue_seq` is allowed on a stale event; every market field must be zero,
   and `from_bytes` refuses one that is not.
5. **Framing covers check snapshots,** and both frame events carry the level
   count; the last level keeps `kLastInMessage`.
6. **Not built:** the converter (Phase 4); the flag-bit check (D7).
7. **`InstrumentMeta`:** the precisions are required constructor arguments
   in the struct's order, checked 0 to 18 and at most the exponent.
8. **Resume semantics:** a stale book is discarded and updates are not
   applied to it; only a stream snapshot that passes its check recovers; a
   reset neither causes nor ends staleness; an update after a reset is stale
   (`delta_before_snapshot`); errors that are not integrity failures still
   raise.
9. **The API:** `trace` and `Step` are new; `replay`, `verify_check_snapshots`
   and `golden_table` gain `mode`, default `"strict"`; `Step.error` is not
   part of a step's equality.
10. **Check snapshots after a failure:** not compared, `stream_unreliable`.
11. **The golden format:** a row per step, resets included on clean data;
    every level null when the book cannot be read.
12. **Integrity points:** JSON, eight cases, a test helper to rebuild it, and
    the `resnapshot` repair; record numbers chosen as in the Phase 2 tests
    (Kraken update 250, the first of their spread whose loss is caught at
    once, and update 300 for the repeat; Coinbase update 500), the resnapshot
    at the first stream heartbeat after update 12,000 (Kraken) or 2,000
    (Coinbase).
13. **Command line:** `--resume` on `verify` and `golden`; `verify --resume`
    still exits 1 when the book was ever stale; strict `golden` fails cleanly.
14. **Existing tests changed to the new layout and format, none weakened.**
    `test_core_types.py`: the `MarketEvent` layout gains `reason` and
    `source`; the round-trip test now expects byte 81 zero, 82 and 83
    reserved and the source at 84 (it expected seven reserved bytes from 81);
    the reserved-byte refusal moved from byte 85, which is now the source, to
    82 and 83; the unknown-kind refusal tries 8, 9 and 255 instead of 9; the
    `InstrumentMeta` tests pass the precisions. `test_types.cpp`: the
    `InstrumentMeta` aggregate gains the precisions (Kraken's real values
    now). `test_book.py`: the golden-file test filters message rows, expects
    the seven leading columns, and checks the new columns on clean data; the
    documentation test asks for the new format.
15. **The phase is numbered 3h**, like the other settling phases before
    Phase 4.
