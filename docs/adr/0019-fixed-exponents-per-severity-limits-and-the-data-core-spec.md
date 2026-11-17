# 0019: Fixed exponents, metadata in effect from a session's first row, limits per severity, and the data core spec

Status: Accepted (owner decisions, 2026-11-16; implementation choices confirmed by the owner 2026-11-16, see "Review"). Restated in `docs/data-core-spec.md`
Date: 2026-11-16
Amends: 0005, 0008, 0010, 0011, 0015, 0016, 0018 (its Review item 7, and its consequence on reading an incomplete load)

## Context

After ADR 0018, three findings of the 2026-11 review
(`docs/reviews/2026-11-data-core.md`) were open, with their tests marked as
expected failures, and ADR 0018 deferred a fourth question:

- **R06.** A stored mantissa could not be interpreted with confidence. On
  Coinbase the exponent was read off the tick size (`price_increment` "0.01"
  gave 2, "0.1" would give 1), so a tick-size change would rescale every later
  price with nothing on the row to say so. And `instrument(at=...)` compared
  venue time with the receipt time of the metadata, so trades resent on
  subscribe and Kraken's first book snapshot, whose venue times are before the
  metadata reached us, had no version in effect: `None` in the first session,
  the previous session's version in every later one.
- **R07.** The quality checks dropped findings past `max_findings` in the
  order they were produced. INFO findings could crowd out an ERROR, and the
  report then exited 0.
- **R08.** `recording_clock` read a session's clock from the events in the
  range, which sit at the session's start. A session that crossed midnight
  had its clock error reported on the first day and not on the second. By
  reading, the same held for `reconnect`: the first connect in a range was
  never counted as one.
- **ADR 0018, Review item 7.** Both observations of a trade seen in two logs
  are stored, but nothing compared logs loaded in separate runs.

The review also listed three mutations that no test caught (T1, T2), and
places where documents and code disagree (X). And the data core's design was
spread over fourteen ADRs, each amending earlier ones; the review's section A
asked for one reference.

## Decision

The owner decided on 2026-11-16:

1. **R06: each instrument has one fixed price exponent,** independent of its
   tick size and generous enough for any tick size it may have. A tick-size
   change never changes the representation. **The recorder fetches instrument
   metadata before subscribing,** so a version is in effect from a session's
   first row.
2. **R07: findings are limited per severity,** an ERROR is never dropped
   behind INFO or WARNING findings, and a truncated report exits nonzero.
3. **R08: a session's clock error is reported on every day the session
   spans.**
4. **A quality check for trade conflicts between logs loaded in separate
   runs.**
5. **The three surviving mutations get tests that catch them.**
6. **Every open disagreement between documents and code is fixed.**
7. **`docs/data-core-spec.md`**: one consolidated, current description of the
   data core, superseding the chain of amendments in ADRs 0005 to 0018. Each
   ADR keeps its history and is marked superseded by the spec where its
   decisions are fully restated there.

Asked while building it (2026-11-16), the owner also decided:

8. **The venue's print precision is stored.** `instruments` gains
   `price_precision` and `qty_precision` (schema version 4). Kraken's checksum
   is over the digits Kraken prints, so the reference book needs them.
9. **BTC-USD is price exponent 8 and quantity exponent 8 on both venues,** and
   the quantity exponent is fixed the same way as the price exponent.
10. **The fixed exponents are an explicit table** (`adapters.FIXED_EXPONENTS`).
    Metadata for an instrument not in it is refused.
11. **A log read from an incomplete load is visible in the quality report**
    (added to this change on 2026-11-16). ADR 0018 reads a log with no
    complete load from its latest incomplete load; every such log gets a
    WARNING naming the log, the load, how many batches were committed and the
    last receipt time read. A load that failed (an `InterleavedLogs`, a crash)
    and one still being written are both reported.

## How it is built

### Fixed exponents (R06)

- `FIXED_EXPONENTS = {("kraken", "BTC-USD"): (8, 8), ("coinbase", "BTC-USD"): (8, 8)}`.
  Every price and quantity of an instrument is stored at its pair of
  exponents, from its first row on. Prices to 1e-8 USD, up to about 9.2e10;
  quantities to one satoshi.
- `VenueAdapter.metadata` builds every `instruments` row: the fixed
  exponents, the venue's precisions, and the tick and lot sizes as mantissas
  at the fixed exponents. Kraken's precisions are its `price_precision` and
  `qty_precision`; Coinbase states none, so they are the decimals of its
  increments.
- **Refused, as a rejected record:** metadata of an instrument not in the
  table (`no_fixed_exponent`; everything after it that needs metadata is
  `no_instrument_metadata`), and metadata needing more decimals than the
  fixed exponent (`precision_exceeds_exponent`). The representation is never
  changed to fit.
- **Schema:** `instruments` version 4, with `price_precision` and
  `qty_precision` after the exponents, each 0 to 18 and at most its exponent
  (`Dataset.at_most`, checked by `check_row` and `table_from_rows` alike).
- **Kraken's checksum** (in the adapter and the reference book) is computed
  over each mantissa divided by `10 ** (exponent - precision)`, with the
  precision in force. A precision change on Kraken changes the digits, not
  the mantissas.
- **The reference book is given the metadata.** `BookMessage` gains
  `price_exp`, `qty_exp`, `price_precision` and `qty_precision`, filled by
  `messages_from_table(table, instruments)` from the version in force for the
  message's record. Replaying Kraken with verification and no metadata raises
  a `ValueError` saying to pass it, instead of guessing.

### Metadata in effect from a session's first row (R06)

- **The recorder** (version 4) subscribes, on Kraken, to the `instrument`
  channel alone, and sends the book and trade subscriptions once the
  instrument snapshot has been received and written. A connection whose
  snapshot has not come within `metadata_timeout` (20 s) is a failure and is
  opened again. Coinbase's metadata was already fetched over REST before
  connecting.
- **By record** (exact): `PITSession.instrument_for(venue, instrument,
  log_id=, seq=)` is the latest version of the row's own log at or before its
  record. The adapters refuse market data before any metadata, so every
  stored market row has one.
- **By time:** `PITSession.instrument(venue, instrument, at=)` treats a log's
  first version as in effect from the log's first row, the earliest
  `event_ts` of any row of that log the session shows (in all four views),
  and every later version of a log from its own `event_ts`. Of the versions
  in effect at `at`, the one that took effect last wins, then the later in
  `(log_id, seq)` order. This is what makes the R06 test pass on the
  committed fixtures, which were recorded before the recorder change: their
  resent trades and Kraken's first snapshot are rows of the session, so its
  version is in effect for them.
- `InstrumentMeta` gains `log_id`, `seq`, `price_precision` and
  `qty_precision`.

### Limits per severity (R07)

- `max_findings` is per check, instrument **and severity**. A check keeps up
  to that many findings of each severity, so a lesser finding never displaces
  a worse one, and ERRORs beyond their own limit are still cut.
- `CheckResult.cut` names the severities some of whose findings were not
  kept; `truncated` is true when any were.
- Queries limited in SQL produce one severity each (the outlier check's book
  query is split into its ERROR and WARNING halves), each limited by that
  severity's room.
- **A report with any cut finding exits 1**, whatever `--fail-on` says, and
  prints a `TRUNCATED` line naming the severities.

### Every day a session spans (R08)

- `recording_clock` finds the sessions from the logs of the rows received in
  the range, in `trades`, `book_events` and `stream_events`, and reads each
  session's start, clock and override records wherever they were received.
  A session runs from a `SESSION_START` to the next in its log. Clock steps are
  reported in the range where they happened.
- `stream_integrity` knows whether each log's stream was already connected
  when the range began, so a reconnect is one whichever day the first connect
  was on.

### Conflicts between logs (ADR 0018, Review item 7)

- `PITSession.trade_observations(venue, instrument, start, end)`: every
  stored observation received in the window (latest version of each, inside
  both cutoffs, each log from its chosen load), with its trade's first
  observation as `first_log_id`, `first_seq`, `first_recv_ts`, `first_price`,
  `first_qty` and `first_side`. It is a typed method only; `sql` cannot name
  it, so research counting trades reads `trades`.
- The quality check `trade_observations` reports an observation from a log
  other than its trade's first observation's that differs in price, quantity
  or side: `trade_conflict_across_logs`, ERROR, at the later observation. An
  identical one is counted in the statistics (`repeated_across_logs`), not
  reported: both venues resend recent trades on every subscribe. Conflicts
  within one log are left to the normalizer's `TRADE_CONFLICT`.

### Logs read from an incomplete load (decision 11)

- `PITSession.loads()` returns a `LoadRead` per loaded log, in `log_id` order:
  its `log_id`, the `load_id` read, whether that load is `complete` within the
  session's version, and how many of its `batches` are committed within it. It
  depends on the version alone, like the choice of load.
- The quality check `loads` reports, for every log with rows received in the
  range whose load is incomplete, `incomplete_load` (WARNING). The message
  names the log, the load, the batches committed and the last receipt time
  read (ns and UTC); the finding points at the last record read of the log, in
  `seq` order, wherever it was received. Its statistics count the logs with
  rows in the range and the incomplete ones.
- The store cannot tell a load still being written from one whose writer
  failed or died: both have committed batches and no last one. The finding
  says it is one or the other.
- A log read from a complete load is not reported, even while a reload of it
  is in progress (that reload is not read). `ingest` writes a load as one
  batch, so a failed or crashed ingest commits nothing and nothing of it is
  read or reported; incomplete loads come from loaders that commit a log in
  chunks (a live loader, or the `Load` API used by hand).

### The mutations (T1, T2)

- **T1:** a recorder test whose scripted socket moves scripted clocks only
  when it delivers a message: each stamp must fall between its message's
  delivery and the next read. Stamping before the wait, the review's
  mutation, fails it.
- **T2:** `test_read_paths.py` lists every module of the data package from
  disk (`pkgutil`), so a new module fails until it is listed with its reason;
  and checks each module's syntax tree: only the session may use DuckDB and
  `pyarrow.parquet.read_schema`, the store and the reference book only
  `pyarrow.parquet.write_table`, nothing else any library that can read a
  store file, and the reference book and quality checks touch no file
  themselves (apart from the quality config). Both of the review's mutations
  fail it.

### Documents and code (X)

- X3 is fixed by R06. X8, X11, X12 and X14 were already true of the code
  after ADRs 0017 and 0018. X9 (`README.md`) and X10 (the package docstring)
  are corrected. X13 is fixed in the code: `InstrumentId` is constructible
  only from unsigned integers of at most 32 bits, checked by
  `static_assert`s. The review's smaller note: `PITSession.sql` now refuses a
  result with two columns of one name with a message saying so, instead of
  a binder error naming its internal call.

### The spec

`docs/data-core-spec.md` restates, as one current description, every decision
of ADRs 0005 and 0008 to 0018 and this one, and the raw-log and normalized-store
tiers of ADR 0006. Those ADRs are marked superseded by it (0006 in part: its
journal, experiment registry and audit log tiers are not the data core).
`docs/data-core.md` becomes the practical guide (commands, known data, how to
rebuild a store) and points at the spec for definitions. A later change to the
data core is a new ADR plus an edit of the spec in the same change.

## Alternatives considered

**R06, the representation**

- **Pin the exponent when an instrument is first seen.** The adapter sees one
  log; it would need to read the store to know what an earlier log used.
- **One exponent for every instrument** (ADR 0005 rejected a global scale on
  range). A table per instrument keeps the choice deliberate.
- **A venue-wide default for unlisted instruments.** Recording a new pair
  would just work, but nobody would have chosen whether its exponent is
  generous enough. The owner chose the table.
- **Derive Kraken's print precision from its tick.** No schema change, but it
  assumes the tick is always a power of ten of that precision.
- **Keep Kraken at its print precision.** Not generous: a finer Kraken tick
  would have to be refused.

**R06, which version is in effect**

- **By record only** (the review's suggestion). Exact for stored rows, and
  provided as `instrument_for`; but "the metadata at a time" is still a
  question, and the R06 test asks it.
- **The first version ever in effect for all earlier time.** Cheap, but in
  every later session resent history would get the previous session's
  version.
- **Count every stored observation of a trade toward a log's first row**
  instead of the trades the session shows. A trade first seen in one log and
  resent by the next would pull the next log's version back over the first
  log's rows.

**R07**

- **Keep the worst findings overall.** One limit for everything, but a check
  that finds many ERRORs would show no WARNING at all, and the order of the
  report would depend on severity.
- **Fail a truncated report only when something at or above `--fail-on`
  could have been cut** (the review's wording). Correct, but which severities
  a SQL-limited query would have produced is not always known; failing on
  any cut is simple and never passes an incomplete report.

**R08**

- **Read the session events of every log in the store.** Simpler, but a range
  would report sessions it holds no data of.

**Conflicts between logs**

- **A check inside `stream_integrity`.** It reads stream events; this reads
  trades, so it is a check of its own.
- **A SQL view of every observation.** Research could then count a trade
  twice by mistake. A typed method is enough for the check.
- **An INFO finding per identical cross-log repeat, or per session.** Every
  session resends history on subscribe, so every report would have one.

## Consequences

- **Every store has to be rebuilt:** `instruments` is schema version 4, and
  every price mantissa changed (Kraken's by 10^7, Coinbase's by 10^6;
  quantities did not change). A store of version 3 is refused with the
  rebuild message. `data/normalized` on the owner's machine was rebuilt; the
  comparison is in the roadmap (Phase 3g).
- **The golden files were regenerated:** the same rows and the same quantities,
  with every price at exponent 8.
- **A new instrument needs an entry in `FIXED_EXPONENTS`** before its logs
  load. Its raw logs can be recorded before then and loaded afterwards.
- **Logs recorded before recorder version 4** on Kraken can have market data
  before the instrument snapshot in record order; the adapter rejects those
  messages (`no_instrument_metadata`), as before. The committed fixtures and
  the one-hour logs do not: their metadata record precedes all their market
  data (`test_fixtures.py` checks it for the fixtures).
- **`instrument(at=)` reads all four views** to find each log's first row,
  which costs a scan of the instrument's rows. `instrument_for` reads only
  `instruments`.
- **The C++ `InstrumentMeta` has no precisions.** Phase 4's book needs them to
  verify Kraken's checksum; the roadmap says so.
- **A quality report on an hour with many findings may now fail** where it
  passed before, if any finding was cut.
- **ADR 0018's consequence "a session cannot tell such a log from one that
  ended early, until the load completes"** no longer holds for the quality
  report: `PITSession.loads()` says which logs are read from an incomplete
  load, and the `loads` check reports each. Research code that wants to know
  can ask the session too.
- ADRs 0005 and 0008 to 0018 are marked superseded by the spec, 0006 in part.

## Review

Choices made while building it. **Confirmed by the owner, 2026-11-16: all thirteen, as written**, after the network tests and a 60-second recording of each venue with recorder version 4 (roadmap, Phase 3g):

1. **The time lookup:** a log's first version is in effect from the log's
   first row, over all four views, counting trades as the session shows them
   (one row per trade). Later versions of a log from their own `event_ts`.
2. **`instrument_for`** as the exact lookup by record, and `InstrumentMeta`
   naming its record and precisions.
3. **The recorder:** Kraken's book and trade subscriptions wait for the
   instrument snapshot; 20 s without it is a reconnect; recorder version 4.
   The recorder still records an instrument with no fixed exponent (the
   adapter refuses its metadata), so raw logs can be kept until one is chosen.
4. **The reasons** `no_fixed_exponent` and `precision_exceeds_exponent`.
5. **Coinbase's precisions** are the decimals of its increments.
6. **R07:** the same `max_findings` for each severity, and exit status 1 for
   a truncated report (the same as for a failing finding).
7. **R08, extended to reconnects:** the review noted the same defect in
   `reconnect` by reading; it is fixed with a test.
8. **Conflicts between logs:** a check of its own, `trade_observations`;
   identical repeats counted, not reported; conflicts within one log left to
   `TRADE_CONFLICT`; the observations as a typed method with no SQL view.
9. **`BookMessage` carries the metadata in force,** and Kraken replay with
   verification refuses to run without it.
10. **`InstrumentId`'s constructor** now refuses signed and wider types at
    compile time (X13), rather than the documents being changed.
11. **The spec and the guide:** which ADRs are marked superseded (all of 0005
    and 0008 to 0018, 0006 in part), and `docs/data-core.md` cut down to a
    guide that points at the spec.
12. **Existing tests changed to the new representation and interfaces, none
    weakened.** Expected values that pinned the old exponents now state them
    at exponent 8: the hand-checked rows of `test_adapters.py` (with the
    precisions added), R10's review test (100.0 is now 10,000,000,000),
    `test_trade_repeats.py`, `test_check_snapshots.py`, and the fixture
    metadata in `test_fixtures.py`, whose Kraken checksum recomputation now
    divides by `10 ** (exp - precision)` and asserts the division is exact.
    `test_coinbase_exponents_come_from_the_increments` pinned the behavior R06
    removes; it now checks that the increments set the tick and precision, not
    the exponent. The case of a Kraken instrument message with precision 40 is
    now a `precision_exceeds_exponent` reject instead of `malformed_message`.
    Hand-built `instruments` rows (`_builders.py`, `test_store.py`,
    `test_schema.py`, `test_session.py`, `test_session_property.py`,
    `test_normalizer_input.py`) carry the precisions; `test_schema.py` expects
    `instruments` at version 4 and has four new refusal cases. The session's
    public surface in `test_session.py` gains `instrument_for` and
    `trade_observations`, and `test_quality.py`'s list of checks run on the
    clean fixtures gains `trade_observations`. `test_book.py`'s hand-built
    messages carry metadata with exponent equal to precision (their mantissas
    are the printed digits), and its fixture helpers pass the instruments to
    `messages_from_table`. The property test's oracle for `instrument` follows
    the new rule, and it now also checks `instrument_for`. The recorder tests'
    scripted Kraken sockets send an instrument snapshot first, and the
    recorder version they pin is 4. The golden files were regenerated (every
    price exactly the old one scaled, every other value unchanged).
13. **Incomplete loads:** WARNING, as decided; the finding points at the
    last record read (by `seq`) and names the greatest receipt time read, which
    differ only if the wall clock stepped back; it is reported on every range
    the log has rows in; `PITSession.loads()` is public. The session's public
    surface in `test_session.py` and the checks run on the clean fixtures in
    `test_quality.py` gain `loads`.
