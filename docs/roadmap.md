# Roadmap

Each phase is sized for one working session and ends with `scripts/check.ps1`
green and a merged PR. Phase 1 is too large for one session, so it is split
into 1a to 1e; each of those is one session with its own PR. Do phases in
order unless a phase says otherwise. Do not start a phase whose prerequisites
are not checked off.

Everything is built fresh in this repository. There is no earlier codebase to
import.

**Every phase, in addition to its own criteria:**

- [ ] `scripts/check.ps1` passes locally and CI is green on the PR.
- [ ] New behavior has tests; no existing test was deleted or weakened to pass.
- [ ] This file's status table is updated, and any new major decision has an ADR.

**Ask the owner** marks a decision that is hard to reverse. Stop and ask
before making it.

## Status

| Phase | Title | Status |
|-------|-------|--------|
| 0 | Skeleton and design docs | Done |
| 1a | Data core: bitemporal schema and append-only store | Done |
| 1b | Data core: point-in-time query session | Done |
| 1c | Data core: raw capture, fixtures, Kraken and Coinbase adapters | Done |
| 1d | Data core: quality checks | Done |
| 1e | Data core: recording clock (ADR 0011) | Done |
| 2 | Python reference order book reconstructor | Done |
| 2a | Replay order and monotonic receive time (ADR 0012) | Done |
| 3 | C++ core types | Done |
| 3a | Log identity in the store (ADR 0013) | Done |
| 4 | C++ order book, verified | Not started |
| 5 | SPSC queue and threading primitives | Not started |
| 6 | C++ feed handlers (offline) | Not started |
| 7 | Deterministic replay engine | Not started |
| 8 | Simulated exchange and cost model | Not started |
| 9 | Pre-trade risk checks and kill switch | Not started |
| 10 | Order state machine and paper gateway | Not started |
| 11 | Live market data connectivity | Not started |
| 12 | Features: order flow imbalance and technicals | Not started |
| 13 | Validation harness | Not started |
| 14 | Regime detection and the blended model | Not started |
| 15 | Risk/portfolio layer | Not started |
| 16 | Paper trading end to end (crypto) | Not started |
| 17 | US equities data via Alpaca | Not started |
| 18 | Alpaca paper execution | Not started |
| 19 | Agent tool server and audit log | Not started |
| 20 | Agent research loop | Not started |
| 21 | Agent monitoring | Not started |
| 22 | Live trading readiness review | Not started |

---

## Part A: Data core and reference book

### Phase 0: Skeleton and design docs (done)

- [x] `docs/architecture.md`, `docs/adr/0001` to `0007`, this roadmap, `CLAUDE.md`.
- [x] `cpp/` builds on Windows with CMake presets and the vcpkg manifest.
- [x] GoogleTest and Google Benchmark binaries build and run.
- [x] `quantagent._core` imports from Python and returns a value from C++.
- [x] `scripts/check.ps1` green; CI workflow present.

### Phase 1: Python data core

Built from scratch per `docs/architecture.md` sections 3.2, 6 and 7 and ADRs
0005 and 0006, as the subpackage `python/src/quantagent/data/`. Four
sessions, in order.

#### Phase 1a: Bitemporal schema and append-only store (done)

- [x] Schemas for trades, book events (snapshots and deltas) and instrument
      metadata. Every row carries `event_ts`, `recv_ts` and `knowledge_ts` as
      `int64` nanoseconds, and prices and quantities as `int64` mantissas
      (ADR 0005).
- [x] Parquet store partitioned by venue/instrument/date (ADR 0006), with a
      writer whose only operation is append. `knowledge_ts` is assigned by
      the writer, not supplied by the caller, and never decreases.
- [x] The writer API has no update, overwrite or delete. A test shows that
      appending never changes the bytes of an existing data file.
- [x] A correction is a new row with the same key and a later
      `knowledge_ts`; the original row is still on disk afterwards (tested).
- [x] Instrument metadata (price and quantity exponents, tick size, lot
      size) is stored the same bitemporal way.
- [x] Decimal string to mantissa conversion is exact and never goes through
      `float`; it rejects values with more precision than the instrument's
      exponent. Tested with edge cases, including very small prices and very
      large quantities.
- [x] Runtime dependencies (proposal: `pyarrow`, `duckdb`) added to
      `python/pyproject.toml`.

**What was built** (`python/src/quantagent/data/`):

- `fixedpoint.py`: `to_mantissa(text, exp)` and `to_decimal_str(mantissa, exp)`.
- `schema.py`: datasets `TRADES`, `BOOK_EVENTS`, `INSTRUMENTS`, each with a
  caller `schema`, a `stored_schema` (adds `knowledge_ts`), a `key` and a
  `version`; `table_from_rows` and `validate_table`.
- `store.py`: `AppendOnlyStore(root, clock=...)` with one public method,
  `append(dataset, venue, instrument, rows)`.

**Notes for the next phases:**

- **Layout.** `<root>/<dataset>/venue=<v>/instrument=<i>/date=<YYYY-MM-DD>/part-<knowledge_ts, 20 digits>.parquet`.
  `date` is the UTC date of `event_ts`. Venue, instrument and date exist only
  as directory names, not as columns in the files. A test confirms DuckDB
  reads this with `hive_partitioning = true` and types the timestamps and
  mantissas as `BIGINT`.
- **Knowledge time.** One append is one batch with one `knowledge_ts`, shared
  by every file it writes. It is strictly greater than any issued before,
  including by earlier processes (recovered from file names when the store
  is opened).
- **Keys (for 1b's "latest version of each record").** A row's identity is
  (venue, instrument) plus `Dataset.key`: trades `trade_id`; book events
  `(kind, venue_seq_last, side, price)`; instruments `event_ts`, which is the
  time the metadata version takes effect. A correction can change `event_ts`
  and so land in a different date partition, so 1b must pick the latest
  version across dates, not within one file or partition.
- **Instrument metadata as of a time** is two steps: the latest known row per
  version (`knowledge_ts <= as_of`), then the version with the greatest
  `event_ts` at or before the time asked about.
- **The store accepts implausible values on purpose** (zero or negative
  prices and quantities), so 1d can flag them. It rejects only structural
  errors: wrong types, nulls in required columns, unknown enum values,
  negative timestamps, invalid instrument metadata.
- **`pyarrow` silently converts `1.0` to an integer.** Always build tables
  with `table_from_rows`, which refuses floats, bools and numeric strings.
- **1c: messages without a venue timestamp (decided, ADR 0008).** Some
  venue messages carry no timestamp. The owner decided: `event_ts` stays
  required and holds `recv_ts` for such rows, and a required
  `event_ts_source` column records `"venue"` or `"receipt"`. Phase 1c
  implemented it.
- **1c: confirm the book event key** against each venue's real sequence
  semantics; it was chosen before any recorded data existed.
- **1c: batch appends.** Each append creates one file per date partition.
  Appending message by message would create a very large number of small
  files. There is no compaction yet.
- **Known limits.** Single writer per store root (not enforced by a lock).
  Publishing uses hard links, which NTFS and ext4 support but exFAT and some
  network shares do not. A crash can leave a hidden `.part-*.parquet.tmp`
  file; readers that glob `*.parquet` do not see it. Names differing only by
  case (`BTC-USD`, `btc-usd`) are one directory on Windows and two on Linux;
  adapters should use one canonical spelling per venue.
- **`.gitignore`.** The `data/` rule was anchored to `/data/`. Unanchored, it
  hid `python/src/quantagent/data/` from git and ruff. `*.parquet` and
  `*.duckdb` are still ignored everywhere, which matters if a later phase
  wants to commit such a file as a fixture.

#### Phase 1b: Point-in-time query session (done)

- [x] `PITSession(as_of=...)` is the only public read path. `as_of` is a
      required keyword with no default; constructing a session without it is
      an error (tested).
- [x] Typed query methods for trades, book events and instrument metadata.
      Every one filters on `knowledge_ts <= as_of`, and returns the latest
      known version of each record as of that time.
- [x] Look-ahead property test: rows are inserted with random
      `knowledge_ts`, sessions opened at random `as_of`, and no returned row
      ever has `knowledge_ts > as_of`, through any method.
- [x] Correction test: a corrected record is visible only to sessions with
      `as_of` at or after the correction's `knowledge_ts`; earlier sessions
      see the original.
- [x] If the session offers SQL, it accepts exactly one `SELECT` statement
      over the session's as-of-filtered views and nothing else. Each of
      these is rejected with a test per case:
  - DDL and DML (`CREATE`, `DROP`, `ALTER`, `INSERT`, `UPDATE`, `DELETE`,
    `COPY`, `ATTACH`, `PRAGMA`, `SET`, `INSTALL`, `LOAD`);
  - multi-statement input, including a second statement hidden after a
    semicolon, a comment or a string literal;
  - raw file access: table functions and replacement scans that read a path
    or URL (`read_parquet`, `read_csv`, a quoted file name used as a table,
    and similar);
  - references to the unfiltered base tables.
- [x] Rejection is by parsing the statement and allow-listing what it may
      contain, not by searching the text for forbidden words.
- [x] The session does not expose its database connection, the store's file
      paths, or any object from which they can be reached through public
      attributes.
- [x] A session's `as_of` cannot be changed after construction. (The
      backtest harness, in a later phase, creates a new session per decision
      time.)

**What was built** (`python/src/quantagent/data/`):

- `session.py`: `PITSession(root, *, as_of)` with `trades`, `book_events`,
  `instruments`, `instrument`, `sql`, `close` and the read-only `as_of`.
  Results are `pyarrow` tables (or an `InstrumentMeta`), never database
  objects. Errors: `QueryRejected` (not allowed, not run) and `QueryError`
  (allowed, failed while running).
- `sqlguard.py`: the allow-list applied to DuckDB's parse tree.

**How look-ahead is blocked, in three independent layers:**

1. Views keep rows with `knowledge_ts <= as_of`, then the latest version per
   record. Typed methods and `sql` read only these views.
2. `sql` validates DuckDB's own parse tree (`json_serialize_sql`) against an
   allow-list and executes that same tree (`json_execute_serialized_sql`).
3. Each session's DuckDB instance may open only the files written at or
   before `as_of` (`allowed_paths`), with external access off and the
   configuration locked.

Each layer was checked by breaking it on purpose and confirming a test
fails (16 breakages; 13 are caught). The other three make no difference the
tests can observe, because something else produces the same outcome: turning
Python replacement scans back on, dropping one of the two
qualified-table-name checks, and executing the query text instead of the
validated tree.

**Notes for the next phases:**

- **Views.** `trades`, `book_events` and `instruments` have the columns
  `venue`, `instrument`, `date`, then the dataset's stored columns. They show
  only the latest known version of each record. Superseded versions are not
  queryable. If 1d needs them (for example to report how often records are
  corrected), add a separate history view deliberately; it is still as-of
  filtered, so it is not a look-ahead risk.
- **SQL limits.** No `WITH` (a CTE name can stand in for a catalog object in
  one scope and not another, which defeats the table allow-list; use a
  subquery). No table functions, `VALUES`, parameters, sampling, lambdas,
  lists or structs. Functions and cast types are a short allow-list in
  `sqlguard.py`. CLAUDE.md says how to extend it.
- **Bare `current_date`, `current_user`, `localtime` and the like** parse as
  column names but run as functions. They are refused as unqualified column
  names and as aliases, so a real column could not be given one of those
  names.
- **A session is a snapshot** of the files present when it was opened. An
  `as_of` later than the store's latest knowledge time is not repeatable:
  the same `as_of` can return more rows once more data is appended. Backtests
  always use a past `as_of`. The agent tool server must cap `as_of` at the
  current time, as the architecture already requires.
- **`instrument(venue, instrument, at=...)`** defaults `at` to `as_of`. It
  compares `at` with `event_ts`, and `as_of` with `knowledge_ts`.
- **Cost.** Opening a session lists every data file in the store. A range
  query resolves the latest version of every record of that instrument
  before filtering on `event_ts`, because a correction can move a record's
  `event_ts`. Both grow with history and file count. That is fine for
  fixtures and weeks of data; it is one more reason for 1c to batch appends,
  and compaction is still unbuilt.
- **One record twice in one batch** gets the same `knowledge_ts`, so "latest"
  is decided by an arbitrary but deterministic tie-break. Adapters should not
  emit the same key twice in a batch.
- **No retraction.** A record can be corrected but not withdrawn. If a venue
  can cancel a trade, that needs a schema decision.
- **Limits of the guarantee.** The session's private connection is reachable
  by Python code that deliberately reads underscore attributes. Layer 3
  still holds then, but superseded versions and file paths become visible.
  The import-lint planned for Phase 12 should also forbid reaching into
  private attributes of the data package, and forbid opening store files
  directly.
- **Other.** Sessions are not thread-safe. A store path containing `=` is
  refused. DuckDB is pinned to 2.0.x (see CLAUDE.md for the upgrade rule).
  `hypothesis` was added as a dev dependency. The Python tests now take about
  35 seconds, mostly from building a small store for each SQL case.
- **1c: test builders.** `python/tests/data/_builders.py` and `conftest.py`
  build the rows that the session tests use. When 1c adds `event_ts_source`,
  update the builders there; the session picks up new columns from the
  schema automatically.
- **`bars`.** The architecture sketched a `bars` method on the session. It
  was not in the 1b criteria and is not built. The owner has since added it
  to the Phase 12 criteria.

#### Phase 1c: Raw capture, fixtures, Kraken and Coinbase adapters (done)

The second venue is Kraken, not Binance: Binance.com refuses API access from
US addresses (HTTP 451). Owner decision.

- [x] Raw log writer and reader per ADR 0006 (length-prefixed records with
      `recv_ts`, `seq`, venue, channel, payload bytes). Round-trip test,
      including a truncated final record.
- [x] A recorder CLI captures Kraken and Coinbase public L2 depth (the
      initial snapshot plus diffs) and trades into raw logs. Public market
      data only; no API keys.
- [x] Recorded: at least one continuous hour for one instrument per venue,
      including the snapshot needed to seed a book and at least one later
      venue snapshot for verification. Stored outside git.
- [x] A small fixture per venue (a few minutes, a few MB at most,
      compressed) committed under `python/tests/fixtures/`, with a README
      giving venue, instrument, time range and how it was captured.
- [x] Schema change per ADR 0008, done before the adapters: a required
      string column `event_ts_source` (`"venue"` or `"receipt"`, any other
      value rejected) added to `trades`, `book_events` and `instruments`,
      and each dataset's schema version raised from 1 to 2. Existing schema
      and store tests updated to carry the column.
- [x] Kraken and Coinbase adapters parse raw payloads into those schemas:
      exact integer prices and quantities, `recv_ts` from the raw record,
      and each venue's integrity fields preserved (Coinbase's sequence
      number, Kraken's checksum and subscribed depth).
- [x] `event_ts` and `event_ts_source` follow ADR 0008. A message with a
      venue timestamp stores it with source `"venue"`. A message type that
      has none (Kraken instrument metadata and Coinbase product metadata are
      the ones found while recording) stores `recv_ts` in `event_ts` with source `"receipt"`.
      The source is decided by message type, not by comparing timestamps.
      Tested with a hand-checked example of each kind per venue, and with a
      test that every `"receipt"` row has `event_ts == recv_ts`.
- [x] Adapter tests run against the committed fixtures, offline: every
      fixture message parses, row counts match message counts, and a set of
      hand-checked messages produce exactly the expected rows.
- [x] Malformed or unknown messages are counted and reported, never
      silently dropped and never a crash.
- [x] End to end: fixture raw log through adapter into the store, then read
      back through `PITSession`, gives the same rows.
- [x] Tests that need the network (the recorder against live endpoints) are
      marked and excluded from the default run.

**What was built** (`python/src/quantagent/data/`):

- `rawlog.py`: `RawLogWriter` (append-only, `seal()` compresses to
  `.qaraw.zst`) and `read_raw_log`. Each record has `recv_ts`, `seq`, venue,
  channel, payload and a CRC32. A partial final record in an unsealed log is
  dropped and reported; any other damage raises.
- `recorder.py`: `python -m quantagent.data.recorder --venue kraken
  --instrument BTC-USD --out data/raw --duration 3600`. Channels in the log:
  `ws` (the stream), `probe` (a fresh book snapshot from a second connection
  every `--probe-interval` seconds), `rest:product` (Coinbase metadata) and
  `recorder` (JSON control records: start, connect, subscribe, disconnect,
  rest, stop).
- `adapters/`: `normalize(records)`, `Normalizer` (streaming) and
  `ingest(path, store)`, with `KrakenAdapter` and `CoinbaseAdapter`. Every
  raw record ends as `data`, `ignored` or `rejected` in the returned
  `Report`, which also lists each reject with a reason and each Coinbase
  sequence anomaly.
- Fixtures: `python/tests/fixtures/raw/<venue>/BTC-USD/*.qaraw.zst` (150
  seconds each, 0.4 MB and 1.0 MB) with a README.
- `python -m quantagent.data.adapters <log>` checks a raw log without
  writing anything (exit code 1 on any reject, sequence anomaly or partial
  record); with `--store <dir>` it loads the log.
- Recordings (not in git): `data/raw/<venue>/BTC-USD/*.qaraw.zst` on the
  machine that ran this phase, with a store built from them in
  `data/normalized`.

**The one-hour recordings** (BTC-USD on both venues, 2026-11-11 03:09 to
04:10 UTC, recorded at the same time):

| | Kraken | Coinbase |
|---|---|---|
| Sealed log | 6.4 MB | 9.2 MB |
| Raw records | 245,094 | 76,715 |
| Rejected messages | 0 | 0 |
| Reconnects | 0 | 0 |
| Sequence anomalies | not applicable | 0 |
| Book update messages | 239,447 | 62,172 |
| Rows: book / trades / metadata | 319,421 / 4,569 / 2 | 871,546 / 18,372 / 1 |
| Venue checksum matches | 239,448 of 239,448 | not applicable |
| Check snapshots equal to the rebuilt book | 6 of 6 | 6 of 6 |
| Book crossed after any message | never | never |
| Time to load into a store | 10 s | 22 s |

The checksum and check-snapshot rows were produced by a one-off script using
the same small book as `test_fixtures.py`, not by code in the repository.
Phase 2 turns that into the reference reconstructor and a proper test.

**Schema changes (all datasets are now version 2):**

- `event_ts_source` added everywhere (ADR 0008). The schema also enforces
  that a `"receipt"` row has `event_ts == recv_ts`.
- `book_events` was reworked, because the Phase 1a version assumed every
  venue numbers its book messages and Kraken does not:
  - `venue_seq_first` and `venue_seq_last` are both optional;
  - new optional `checksum` (the venue's checksum after the message) and
    `depth` (levels per side the venue keeps; null means the full book);
  - new required `level_index`, the position of the level in its message.
    Order matters: Kraken can change one price twice in a message;
  - the key is now `(recv_ts, seq, level_index)`: which raw record and where
    in it. Normalizing the same raw log again therefore corrects rows
    instead of duplicating them (tested);
  - a third kind, `CHECK_SNAPSHOT`, marks the independent snapshots from the
    probe connection. They are not part of the update stream.

**What the venues actually do** (checked in the documentation and in recorded
data before the adapters were written):

- **Kraken, WebSocket v2.** The book channel has no sequence numbers. Every
  snapshot and update carries a CRC32 of the top 10 levels per side. The
  subscription has a depth (25 here) and the client must cut the book to it
  after each update. Prices and quantities are JSON numbers printed at the
  pair's full precision, so the adapter reads them as text. Trade `side` is
  the taker's. Venue symbol `BTC/USD` is stored as instrument `BTC-USD`.
- **Coinbase, Advanced Trade.** `level2`, `market_trades` and `heartbeats`
  need no authentication; no key is read or sent. `sequence_num` counts
  every message on one connection across all channels, from 0. The book
  snapshot is the full book (about 38,000 levels, 4 MB). Trade `side` is the
  maker's, so the adapter stores the opposite as the aggressor.
- **Aggressor side was checked empirically** on both fixtures: trades print
  on the aggressor's side of the mid about 80% of the time on both venues,
  which is what a correct mapping gives with this rough method (an inverted
  one gives about 20%).

**Notes for the next phases:**

- **Matching a check snapshot to the stream (Phase 2 and Phase 4).** A
  snapshot from another connection shares no sequence numbering with the
  stream. It is matched by venue time instead: rebuild the book from the
  stream up to and including the last message whose `event_ts` is at or
  before the snapshot's `event_ts`. On both fixtures the result equals the
  snapshot on every level (38,532 levels on Coinbase, 50 on Kraken). A test
  in `test_fixtures.py` does this with a 20-line book; Phase 2 should replace
  that helper with the real reconstructor.
- **Kraken's checksum is reproduced from the stored integers** for all 13,782
  book messages in the fixture: concatenate, for the 10 best asks ascending
  then the 10 best bids descending, the decimal digits of the price mantissa
  and the quantity mantissa, and take CRC32. This works because Kraken
  prints full precision, so the mantissa's digits are the venue's digits.
- **Kraken's checksum only covers the top 10 levels.** A lost update that
  touches only levels 11 to 25 is not detectable until it reaches the top
  10. Applying the same update twice changes nothing and is also invisible.
- **Ask the owner before 1d: where Coinbase stream gaps are stored.** The
  adapter detects gaps, repeats and out-of-order messages
  (`Report.sequence_anomalies`), but nothing persists them. They cannot be
  recovered from `book_events` afterwards, because heartbeats and trades
  use the same numbering and are not stored with it. The 1d check and the
  Phase 2 "raise on a gap" rule both need them through `PITSession`.
  Proposal: a small fourth dataset, `stream_events`, holding connects,
  disconnects and anomalies with `recv_ts` and `seq`. It would also record
  Kraken reconnects. This adds a dataset and a view, so it needs a decision.
- **Instrument metadata rows.** Each recording session adds a metadata
  version effective from its receipt time, even when nothing changed. The
  recorder requests metadata before market data, so a version is always in
  effect for the data that follows it. Kraken's metadata message lists
  about 1,400 pairs; only the recorded pair is stored.
- **Trade snapshots.** Both venues send recent trades on subscribe. A trade
  seen again in a later session has the same `trade_id` and is treated as a
  correction, not a duplicate.
- **`ingest` batches by rows** (200,000 pending rows per append by
  default), not by messages: one Coinbase snapshot message is about 38,000
  rows. A one-hour log becomes a handful of files per dataset, and memory
  stays bounded.
- **Reading raw logs is not point-in-time.** `read_raw_log` returns
  everything in a file. It is for capture, adapters and the replay engine.
  Signal and agent code must not use it; the Phase 12 import-lint should
  forbid `quantagent.data.rawlog` and `quantagent.data.adapters` there.
- **New dependency:** `websockets` (the recorder only). Network tests are
  marked `network` and excluded by default; run them with
  `pytest python -m network`.
- **No API keys are needed** for anything built so far. `.env.example` has
  Kraken placeholders for the later execution phases.
- **Test time** is now about 75 seconds for the Python suite.

#### Phase 1d: Quality checks (done)

Each check is a function over data read through `PITSession` that returns
structured findings (what, where, severity). Each has one test on clean
fixture data that finds nothing and one on a deliberately corrupted copy
that finds the planted defect.

- [x] **Stream integrity,** using each venue's own rule, read from the
      `stream_events` dataset (owner decision, ADR 0010; built first in this
      phase). Coinbase: missing, repeated or out-of-order `sequence_num` on a
      connection. Kraken: book checksum mismatches, which the adapter now
      verifies while normalizing. Both venues: reconnects and messages that
      could not be normalized.
- [x] **Clock skew:** `recv_ts - event_ts` outside a configured band,
      including negative values (venue timestamp later than receipt), and
      `event_ts` going backwards within a stream. Only rows with
      `event_ts_source = "venue"` are considered (ADR 0008); a test shows
      that `"receipt"` rows are neither flagged nor counted as zero skew.
- [x] **Crossed books:** best bid at or above best ask in snapshot and
      top-of-book data. (Phase 2 applies the same check to the reconstructed
      book after every event.) No top-of-book quotes are stored, so this
      checks every snapshot, the stream's own and the independent ones.
- [x] **Outliers:** trade prices far from a rolling reference, and zero or
      negative prices and quantities. Thresholds are config, and a finding
      flags the row; it never deletes or alters it.
- [x] A report command runs all checks over a date range and prints a
      summary. It exits non-zero when a finding at or above a chosen
      severity exists.
- [x] `docs/data-core.md` written: schema, store layout, `PITSession` API
      and its restrictions, adapters, and what each quality check does.

**What was built:**

- `stream_events` (ADR 0010), done first: a fourth dataset with one row per
  integrity event, written by the adapters in the same pass as the market
  data and read through `PITSession.stream_events` and the SQL view of the
  same name. Kinds: session start and stop, connect, disconnect, subscribe
  request, subscription ack, trade backfill, the three sequence breaks,
  checksum mismatch, and message rejected. Each row names its raw record.
- The Kraken adapter now keeps the top of each connection's book and
  verifies the venue's checksum after every message. After a mismatch the
  connection's book is suspect, and nothing more is reported for it until
  its next snapshot, so one fault is one event.
- `quality.py`: `check_stream_integrity`, `check_clock_skew`,
  `check_crossed_books`, `check_outliers`, `run_checks`, `QualityConfig`
  (loadable from JSON) and the report command
  `python -m quantagent.data.quality --store ... --start ... --end ...`.
- `docs/data-core.md`, describing the whole data core.
- ADR 0009 marked confirmed by the owner.

**Verified:**

- Every check finds nothing on both clean fixtures, and finds exactly the
  planted defect on a damaged copy. Defects are planted in the raw log
  (a lost Coinbase message, a corrupted Kraken checksum, an unparseable
  message) so the adapters' events are part of the test, or in the
  normalized rows (late, early and out-of-order timestamps, crossed,
  locked and one-sided snapshots, outlying and impossible prices).
- On the one-hour recordings, loaded into a fresh store, the report finds
  nothing: 1.2 million book rows, 23,000 trades, 64 stream events, 14
  snapshots.
- 35 deliberate breakages of the new code; all 35 are caught by a test.
- 899 Python tests and 2 C++ tests pass.

**Notes for the next phases:**

- **The recording clock (decided: ADR 0011, built in Phase 1e).** The machine that made the Phase
  1c recordings has its Windows Time service stopped, and its clock ran
  about 1.4 s ahead while recording (no Coinbase message arrived sooner than
  1,398 ms after Coinbase sent it) and about 1.2 s ahead a day later (four
  NTP servers agree, with 20 to 50 ms round trips). That is most of the
  ~1.48 s median lag in the quality report. It causes no look-ahead (data
  looks known later than it was), but it makes every lag and latency figure
  wrong by a drifting amount, and the clock skew check cannot see it
  because the band allows several seconds. Proposed: run recordings only on
  a machine with time sync enabled (`w32tm` running, or NTP on Linux);
  have the recorder measure its NTP offset at session start and store it
  in the `SESSION_START` detail; and add a check that flags a session whose
  offset exceeds a threshold. That changes the recorder and adds a check,
  so it needs a decision. Until then, treat absolute lags on this data as
  shifted by 1.2 to 1.4 s.
- **Phase 2 and stream events.** The reconstructor must verify Kraken's
  checksum itself and must also raise on a stored `CHECKSUM_MISMATCH` or
  `SEQUENCE_GAP`; a test should show its own verification and the
  adapter's events agree on the fixtures and on damaged copies (ADR 0010).
  The helpers in `test_stream_events.py` (`copy_log` and the damaged-log
  tests) are a starting point for the gap fixtures Phase 2 needs.
- **What the checks cannot see:** a crossed book between snapshots (needs
  the rebuilt book, Phase 2), a steadily wrong clock (above), lost Kraken
  updates the checksum misses (36% of single losses on the fixture), and
  anything during periods that were not recorded. `docs/data-core.md`
  section 6 lists these.
- **Backfilled trades.** Both venues send recent trades on subscribe.
  `TRADE_BACKFILL` marks those records. Anything that treats a trade's
  `recv_ts` as "when it became known live" (backtests, Phase 7 onward) must
  exclude or handle them, as the clock skew check does.
- **Thresholds** are defaults chosen from the recorded data (worst live
  lag 2.3 s, worst trade move 2.8 bps against the median of the previous
  50). They are untested on volatile markets; `outlier_bps` = 500 may need
  tuning for small-cap pairs.
- **Report speed.** About 2 seconds for the two one-hour recordings. The
  checks pull per-instrument rows into Python for the outlier median; a
  day of Coinbase trades is fine, months may need a per-day loop.
- **Python test time** is now about 95 seconds.

#### Phase 1e: Recording clock (done)

Added after Phase 1d by the owner, because the Phase 1c recordings were made
on a clock about 1.4 s fast (ADR 0011).

- [x] The recorder measures its NTP offset at the start of every session,
      before contacting the venue, and stores it as a `CLOCK_OFFSET` stream
      event.
- [x] It refuses to start if the offset exceeds 250 ms, or cannot be
      measured, unless run with `--allow-clock-offset`. The limit is not a
      command-line option.
- [x] Any use of the override is recorded, as a `CLOCK_OVERRIDE` stream
      event, whether or not it was needed.
- [x] A quality check, `recording_clock`, flags any session whose offset
      exceeds the limit.
- [x] The synchronized-clock assumption is recorded in the architecture
      (section 6.2) and in ADR 0011.
- [x] The existing one-hour recordings' ~1.4 s offset is documented
      (`docs/data-core.md` section 7, the fixture README, ADR 0011).
- [x] The current clock offset was confirmed before recording anything.

**What was built:**

- `clock.py`: an SNTP client (`sntp_query`) and `measure_offset`: three
  samples from each of four servers (Cloudflare, Google, Microsoft, NTP
  Pool), the fastest reply per server, the median across servers, at least
  two must answer. Replies are refused if they are not server replies, come
  from an unsynchronized server, are a kiss-of-death, or do not echo the
  request's timestamp. No new dependency.
- `recorder.py`: measures first; on refusal exits with status 2 without
  contacting the venue or writing a file. Writes a `clock` control record
  right after `start`. `RECORDER_VERSION` is now 2.
- Adapters: the `clock` control record becomes `CLOCK_OFFSET` (and
  `CLOCK_OVERRIDE` when used); malformed clock records are rejected.
- `quality.py`: `check_recording_clock` (ERROR above the limit, WARNING if
  unmeasured, INFO for sessions recorded before clocks were measured and for
  any override) and `QualityConfig.max_clock_offset_ns`.
- ADR 0011; ADR 0010's additions marked confirmed by the owner.

**Verified:**

- The SNTP client is tested against a local fake NTP server: offsets of
  0, +1.4 and -0.75 s are measured to within 20 ms, server processing time
  is excluded from the round trip, and six kinds of bad reply are refused.
- The recorder refuses a clock 251 ms ahead, 1.4 s behind, or unmeasured,
  and in each case opens no connection and writes no file.
- Before recording anything this session, the clock was measured against
  four NTP servers at 0.6 to 8.2 ms. The network tests (a real NTP
  measurement and a 6-second capture from each venue) then passed.

**Notes for the next phases:**

- **The Windows Time service on the recording machine was stopped again**
  when this phase started (start type Automatic), even though the clock was
  then within 10 ms after a sync the day before. Without the service the
  clock drifts, and the recorder will start refusing once it is 250 ms off.
- **The committed fixtures keep their 1.4 s offset.** Tests that depend on
  lags (for example the clock skew band test) are written against it. Every
  quality report on them now includes one `clock_not_recorded` INFO
  finding per session. The tests that asserted "no findings" for the
  fixtures were changed to expect exactly those findings.
- **The clock is measured at session start only.** Drift within a session is
  not caught. Measuring again at the stop record would be a small addition.
- **Phase 11** (live feed threads) stamps wall-clock time too and needs the
  same check; its criteria now say so.

---

### Phase 2: Python reference order book reconstructor (done)

A simple, obviously correct L2 book in Python
(`python/src/quantagent/data/`), used as the oracle for the C++ order book.
Correctness over speed: plain dictionaries and sorted views, no
optimization.

- [x] Builds a book from a venue snapshot and applies deltas, with integer
      prices and quantities. A level with zero quantity is removed.
- [x] Applies the levels of a message in `level_index` order, and after each
      message cuts the book to `depth` levels per side when the venue sends
      a truncated book (Kraken).
- [x] Enforces each venue's integrity rule exactly as that venue documents
      it, cited in the code with a link to the venue documentation. Kraken:
      the CRC32 checksum after every message (the Phase 1c notes give the
      exact recipe). Coinbase: `sequence_num` continuity on the connection,
      using the stored `stream_events` (ADR 0010). For both venues, a
      stored `CHECKSUM_MISMATCH` or `SEQUENCE_GAP` also raises, and a test
      shows the reconstructor's own checksum verification agrees with the
      adapter's events.
- [x] **Raises on an integrity failure.** Kraken: a checksum that does not
      match raises a specific exception carrying the expected and computed
      checksums and the message's `(recv_ts, seq)`. Coinbase: a gap raises
      one carrying the expected and received sequence numbers. It never
      skips, repairs or continues silently. Tested with fixtures that have
      a message removed, duplicated and swapped. For Kraken the removed
      message must be one that changes the top 10 levels; the tests also
      document what the checksum cannot see (a lost update below the top
      10, and a repeated update).
- [x] Applying a delta before a snapshot raises.
- [x] Verified against exchange snapshots: over the Phase 1c fixtures and
      the one-hour recordings, the reconstructed book equals every
      `CHECK_SNAPSHOT` on every level, matched by venue time (the stream
      up to the last message with `event_ts` at or before the snapshot's).
- [x] The book is never crossed after any event on the fixtures (the Phase
      1d check, run per event).
- [x] Unit tests on small hand-built sequences with hand-computed expected
      books: insert, update, delete, a delete of a missing level, and both
      sides.
- [x] Golden files exported for Phase 4: for each fixture, top-of-book and
      top-20 depth after every event, in a documented format, committed
      alongside the fixtures (or generated deterministically by a test
      helper if too large to commit).

**What was built** (`python/src/quantagent/data/book.py`):

- `ReferenceBook`: price to quantity per side in plain dictionaries, plus a
  sorted list of prices per side for the best levels. `replace` (snapshot),
  `apply` (levels in order; quantity 0 removes), `truncate(depth)`, `top`,
  `best_bid`, `best_ask`, `is_crossed`. Integers only.
- `kraken_checksum(book)`, written from Kraken's documentation and
  independent of the adapter's copy.
- `messages_from_table`: `book_events` rows (as `PITSession` returns them)
  grouped into `BookMessage`s, checked for a complete `level_index` and one
  header per message.
- `replay(venue, messages, events, verify=True)`: yields the book after every
  stream message. Raises `DeltaBeforeSnapshot`, `ChecksumMismatch` (expected,
  computed, `recv_ts`, `seq`), `SequenceBreak` (kind, expected, received) or
  `StreamFault` (a rejected message on the stream). Each says whether it came
  from its own check (`source="computed"`) or a stored stream event
  (`"stream_event"`). A new session or connection means the next message
  must be a snapshot.
- `verify_check_snapshots`: every `CHECK_SNAPSHOT` against the book rebuilt
  up to the last stream message at or before its venue time, level by level,
  cut to the snapshot's depth.
- `golden_table` and the command line: `python -m quantagent.data.book
  verify|golden --store ... --venue ... --instrument ...`.
- Golden files: `python/tests/fixtures/golden/<venue>-BTC-USD.top20.parquet`
  (476 KB and 247 KB, format in the README there); `.gitignore` now allows
  Parquet in that directory only.
- Damaged-fixture helpers: `python/tests/data/damage.py` (`removed`,
  `duplicated`, `swapped`, `write`).

**Verified:**

- Both fixtures replay with every rule satisfied and are never crossed after
  any message (Kraken 13,782 messages, every checksum verified; Coinbase
  2,702). Their check snapshots match on every level.
- The one-hour recordings (local data, `pytest -m local_data`): 6 of 6
  check snapshots match per venue (all 38,000-odd levels on Coinbase), the
  book is never crossed, and every Kraken checksum verifies.
- Damaged fixtures, per venue:

  | Damage | Kraken | Coinbase |
  |--------|--------|----------|
  | One message removed | `ChecksumMismatch` at the next book message, if the lost update touched the ten best levels; otherwise unseen (tested) | `SequenceBreak` gap, from the stored events; unseen from book messages alone (tested) |
  | One message duplicated | Unseen: quantities are absolute, so the book is unchanged (tested) | `SequenceBreak` repeat, from its own check and from the stored events |
  | Two messages swapped | `ChecksumMismatch` at the first or second of the pair | `SequenceBreak` gap at the first |

- Its own Kraken checksum and the adapter's stored `CHECKSUM_MISMATCH`
  events agree on the record and both values, over twelve removals spread
  across the fixture.
- 23 deliberate breakages of the reconstructor; all 23 are caught.

**Notes for the next phases:**

- **Replay order across clock steps (decided: ADR 0012, built in Phase 2a).**
  Within one raw log the order is `seq`; logs merge by a monotonic stamp.
- **`recv_ts` resolution is about 0.3 to 0.5 ms on this machine.** Python
  3.12's `time.time_ns()` on Windows follows the system timer, so about 70%
  of the records in the one-hour Kraken log share their `recv_ts` with the
  previous record. Ties are ordered by `seq`, so nothing is misordered, but
  latency measurements finer than 0.5 ms are not possible from this data.
  Python 3.13 uses a precise system clock on Windows.
- **For Phase 4:** drive the C++ book with `messages_from_table` output and
  compare after every message with the golden files. The stale flag must be
  set exactly where `replay` raises on the damaged fixtures from
  `damage.py`. For Kraken, use removals the tests show are caught; the table
  above lists what each venue's rule cannot see.
- **Stored events are needed for Coinbase.** Without them, a lost Coinbase
  message is invisible to the reconstructor (heartbeats and trades share the
  numbering). Always pass the instrument's `stream_events` to `replay`.
- **Check snapshots on the probe connection** are compared, not applied. A
  probe's own checksum mismatch is ignored by `replay` (it concerns the probe
  connection, not the stream).
- **Memory.** `messages_from_table` holds a whole range in memory. For the
  one-hour Coinbase recording (870,000 levels, 81 MB as Arrow) the messages
  hold 127 MB and grouping peaks at 452 MB (measured with `tracemalloc`);
  verifying all six check snapshots takes 9 seconds. Ranges much longer than
  a few hours need a per-day loop.
- **Test time** for the Python suite is now about 3 minutes, a third of it
  the damaged-fixture tests.

---

### Phase 2a: Replay order and monotonic receive time (done)

Added after Phase 2 by the owner (ADR 0012), before Phase 3.

- [x] Within one raw log, replay order is `seq` alone; `recv_ts` is a
      measurement, not an ordering key. Recorded in ADR 0012 and the
      architecture (sections 4.2 and 6.2).
- [x] Raw records gain `recv_mono_ns`, the monotonic high-resolution counter
      (`time.perf_counter_ns`), read in the same call as `recv_ts`. Logs
      from the same machine and boot are merged by it; `recv_ts` stays the
      absolute time.
- [x] The recorder records the clock identity (platform, machine id, boot
      id, counter) that says when two logs' monotonic stamps are comparable.
- [x] A quality check flags any session where the wall clock stepped against
      the monotonic counter beyond a threshold, with the size of the step.
- [x] Existing logs and fixtures are still read, are documented as lacking
      the field, and are not rewritten.

**What was built:**

- Raw log version 2 (`rawlog.py`): `recv_mono` after `recv_ts` in every
  record, -1 when absent. The reader reads versions 1 and 2 and reports
  `version`; the writer writes 2 and, unless given stamps, reads both clocks
  itself.
- `clock.py`: `stamp()` (wall clock and counter read together),
  `ClockIdentity` and `clock_identity()` (Windows `MachineGuid` and `BootId`;
  Linux `/etc/machine-id` and `boot_id`).
- `logorder.py`: `merge_logs(paths)` and `log_clock_identity(path)`. One log
  is replayed in `seq` order; several are merged by monotonic stamp, then log,
  then `seq`, and refused if not comparable or unstamped.
- Recorder version 3: stamps every record with both clocks and puts the clock
  identity in the start record.
- Adapters: `SESSION_START` detail now says whether the session has monotonic
  stamps and names its clock identity; a change in `recv_ts - recv_mono_ns`
  between consecutive records larger than 25 ms is a `CLOCK_STEP` event with
  its signed size.
- `quality.py`: `check_recording_clock` reports `clock_step` (ERROR) beyond
  `max_clock_step_ns` (default 50 ms) and `clock_steps_not_measurable` (INFO).
- `book.py`: a stored backward `CLOCK_STEP` raises `ReplayOrderUnreliable`
  (removed in Phase 3a, when stored data gained its log's identity).

**Verified:**

- Two simultaneous 60-second recordings with the new recorder (kept in
  `data/raw-adr0012/`, not in git): every record stamped, no two records
  sharing a monotonic stamp (against about 70% sharing `recv_ts` in the
  version 1 logs), `recv_ts - recv_mono_ns` moving at most 1.16 ms between
  consecutive records, drift under 0.3 ms over the minute, identities
  comparable, and the two logs merged in monotonic order. The clock was
  +11 to +12 ms from NTP. No clock steps; the quality report found nothing.
- No existing log has `recv_ts` going backwards (checked before this phase),
  so the stored data's `(recv_ts, seq)` order is arrival order for all of it.

**Notes for the next phases:**

- **Settled in Phase 3a (ADR 0013).** ~~Ask the owner: the normalized store
  does not know which log a row came from~~ (ADR 0012, "Open"). `seq` restarts in every log, so stored rows
  cannot be put in `seq`-within-log order, and book and stream event keys
  `(recv_ts, seq, ...)` could collide between two logs of the same venue and
  instrument recorded at the same time. Fixing both needs a `log_id` column
  (and probably `recv_mono_ns`) on every dataset. Until decided: one recorder
  per venue and instrument at a time, and the reconstructor refuses sessions
  with a backward clock step.
- **Ask the owner: Coinbase check snapshots are not always exact.** In the
  new 60-second Coinbase recording, its only check snapshot differs from the
  rebuilt book on 55 of 37,746 levels near the inside of the market, and no
  position in the stream reproduces it exactly (closest: 33 levels, 27 ms
  after its venue time). The stream had no gaps. Every earlier Coinbase
  check matched (1 of 1 on the fixture, 6 of 6 in the hour), so Coinbase
  stands at 7 of 8. *Corrected in Phase 3a: this note first said "one of its
  two check snapshots" and "the other snapshot matched exactly". The log has
  one check snapshot; the two that matched were Kraken's.* The likely cause is that Coinbase's
  snapshot is not taken at a message boundary of the update stream. Phase 2's
  criterion "equals every check snapshot on every level" therefore holds
  usually but not always on Coinbase, and Phase 4 inherits it. Options: keep
  the exact rule and treat a mismatch as a finding to explain, or allow a
  documented tolerance for Coinbase. Kraken is unaffected: its checksum
  verifies the book after every message.
- **Phase 7** must read raw logs through `merge_logs` (or a single reader)
  and order by `seq`; its criterion now says so.
- **The Windows Time service** was again found stopped at the start of this
  phase. The recorder checked the offset itself (11 to 12 ms).

---

## Part B: C++ hot path

### Phase 3: C++ core types (done)

- [x] `Price`, `Qty` strong types, `Timestamp`, instrument id, instrument
      metadata, `Side`. No implicit conversion to floating point (verified by
      a compile-fail check or `static_assert` on convertibility).
- [x] 128-bit notional helper that compiles on MSVC, with overflow tests.
- [x] `MarketEvent` and order message structs: fixed size, trivially copyable
      (`static_assert`), size and layout documented.
- [x] `Clock` interface with system and manual implementations.
- [x] Types exposed in `quantagent._core`; Python round-trip tests.

**What was built** (`cpp/include/qa/`, `cpp/src/clock.cpp`, bindings in
`cpp/bindings/module.cpp`):

- `types.hpp`: `Price`, `Qty`, `Timestamp`, `MonoTimestamp` (ADR 0012),
  `InstrumentId`, `Side`, `InstrumentMeta`. Construction only from integers
  that fit without loss; floating point and bool constructors are deleted.
- `notional.hpp`: `Int128`, `multiply` (`_mul128` on MSVC x64),
  `multiply_portable`, `notional(Price, Qty)`, `to_int64`.
- `events.hpp`: `MarketEvent` (88 bytes), `OrderRequest` (64),
  `ExecutionReport` (80), and their enums. Layouts in architecture
  section 6.3.
- `clock.hpp`: `Clock`, `SystemClock`, `ManualClock` (including a wall-clock
  step on its own, for testing ADR 0012 behaviour).
- Python (`quantagent._core`): all of the above, plus `to_bytes`,
  `from_bytes` (which refuses unknown enum values and non-zero reserved
  bytes), `size()` and `layout()` for each message, and `notional` returning
  an exact Python int.

**Verified:**

- 15 new GoogleTest cases plus compile-time checks: the conversion rules as
  `static_assert`s on `is_convertible`, `is_constructible` and concepts for
  `+`, `*` and `<` between the types; every layout offset; no padding
  (`has_unique_object_representations`); 128-bit products against known
  values, at the int64 boundary, and the intrinsic against the portable
  version on 200,000 random pairs.
- 92 new Python round-trip tests: every int64 extreme through every type and
  field and back, through `to_bytes`/`from_bytes`, with floats, bools,
  strings and out-of-range integers refused.
- The C++ `SystemClock`'s counter agrees with Python's `perf_counter_ns`
  within 1 ms, so stamps from C++ feed threads will be comparable with the
  recorder's (ADR 0012).
- 15 deliberate breakages (portable multiply, overflow detection, ordering,
  deleted conversions, layout, clocks, bindings): all caught, by a test or by
  the build's `static_assert`s.
- Benchmark baseline (this machine, Release, 12-core 4.1 GHz): `notional`
  1.3 ns, `multiply_portable` 2.4 ns.

**Notes for the next phases:**

- **Phase 4 needs a Python-side converter** from `BookMessage` (the
  reference reconstructor's input) to `MarketEvent`s, with `kLastInMessage`
  on each message's last level so the C++ book knows when to verify a
  checksum. It is not built yet because nothing uses it.
- **`MarketEvent` is per level.** A Coinbase snapshot is about 38,000 events.
  `level_index` is 32 bits for that reason; `depth` is 16 bits (Kraken's
  largest is 1000).
- **The order messages are a first cut** (architecture 6.3). They carry no
  venue order id: Coinbase and Kraken use strings, which do not fit a fixed
  layout. Phase 10 needs a mapping from our `client_order_id` to the venue's
  id, kept by the execution thread.
- **A recorder bug from Phase 1c was fixed here.** When the stream's
  receive timed out at the session deadline, the event loop's timer could
  fire a few milliseconds early (its clock ticks every 15.6 ms on Windows),
  and the recorder then opened a pointless new connection for the remaining
  milliseconds. It showed up as a test failing about 1 run in 25. A timeout
  within 0.1 s of the deadline now ends the session; one earlier than that
  is still a failure that reconnects. Two deterministic tests cover both.
  The one-hour recordings were not affected (their connection counts are
  exactly one stream plus six probes).
- **The Linux preset is still unbuilt.** `__int128` is the GCC/Clang path for
  `multiply`; it is only exercised once a Linux build exists, and the test
  that compares it with the portable version will cover it then.

### Phase 3a: Log identity in the store (done)

Added after Phase 3 by the owner (ADR 0013), before Phase 4. It settles ADR
0012's open point.

- [x] Every stored dataset has a `log_id` column naming the raw log each row
      came from.
- [x] Book event keys are `(log_id, seq, level_index)` and stream event keys
      `(log_id, seq, event_index)`. Every other key was reviewed for
      dependence on `recv_ts`: `instruments` was keyed by `event_ts`, which
      is the receipt time for both venues' metadata, and is now keyed by
      `(log_id, seq)`; `trades` keep `trade_id`, the venue's identity.
- [x] Stored data is ordered by `(log_id, seq)`.
- [x] The reconstructor's clock-step fail-safe (`ReplayOrderUnreliable`) is
      removed: with `(log_id, seq)` order a clock step no longer reorders
      stored data. Steps are still graded by `recording_clock`.
- [x] Stores rebuilt from the raw logs; the fixtures' derived data and golden
      files regenerated; no raw log rewritten.

**What was built:**

- `rawlog.log_id_of(first)`: `<UTC start to the ns>Z-<12 hex of SHA-256 of
  the first record>`. It depends only on the first record, so it is the same
  sealed or unsealed, cut short, copied or renamed.
- `schema.py`: `log_id` (required, form checked on write) between `recv_ts`
  and `seq`; the new keys; `trades`, `book_events`, `instruments` version 3,
  `stream_events` version 2.
- Adapters: the normalizer takes each log's id from its `seq` 0 record, so
  one normalizer can be fed several logs; a record before any log start is
  rejected (`no_log_start`).
- `PITSession`: range methods order by `(log_id, seq)`; `instruments` by
  `event_ts`, then `(log_id, seq)`.
- `book.py`: `BookMessage.log_id`; replay, errors and keys use
  `(log_id, seq)`; check snapshots are compared within their own log; golden
  files have columns `log_id, seq, recv_ts`, then the levels.
- `quality.py`: findings carry `log_id` and name `(log_id, seq)`; venue time
  is compared between consecutive messages of one log; backfilled trades are
  matched by `(log_id, seq)`; snapshots are grouped by `(log_id, seq)`.

**Verified:**

- `scripts/check.ps1`: ALL GREEN, 17 C++ tests and 1,196 Python tests.
- New `test_log_id.py`: the id's form, that every field of the first record
  contributes, that it sorts by start time, that it is the same for a sealed,
  unsealed and truncated copy, that each log fed to one normalizer gets its
  own id, that a record before any log start is rejected, and that two logs
  whose records share `recv_ts` and `seq` no longer collide in
  `book_events`, `instruments` or `stream_events`. Existing tests were
  updated to the new keys, columns, versions and order. The tests of
  `ReplayOrderUnreliable` became tests that a clock step no longer stops or
  reorders the replay. One expectation changed in substance: a trade planted
  with a late `recv_ts` but its original `seq` is still `late`, but no
  longer also `event_ts_backwards`, because venue time is now compared in
  record order. Two hand-built fixtures that gave different records the
  same `(log_id, seq)`, which no real log can do, were corrected.
- Golden files regenerated: every row, `seq`, `recv_ts` and level value is
  identical to the files they replaced; only the `log_id` column is new.
- `data/normalized` rebuilt from the two one-hour logs: the same row counts
  as Phase 1c (319,421 / 4,569 / 2 and 871,546 / 18,372 / 1), 6 of 6 check
  snapshots matching on each venue, never crossed, and the same quality
  report (two INFO `clock_not_recorded`). The old store is kept as
  `data/normalized-before-adr0013`.
- Breakage pass: 17 deliberate breakages of the new code (the id's inputs,
  its seq-0 rule, the normalizer's per-log id and `no_log_start`, the schema's
  form check, each of the three keys reverted, the session's order, the
  reconstructor's message and event order, per-log check snapshots, the
  golden `log_id` column, the per-log venue-time check, backfill matching,
  snapshot grouping, and findings' `where`). The first run caught 15. The
  two that survived (events sorted by `recv_ts`, backfill matched by `seq`
  alone) got tests, and then all 17 were caught.

**Notes for the next phases:**

- **Phase 4's golden comparison keys rows by `(log_id, seq)`.**
- **Ask the owner: stores do not check schema versions.** Every file
  records `qa.schema_version`, but nothing reads it. A store written before
  this phase fails with DuckDB's "column log_id not found", and appending new
  rows to such a store makes a mixed store that fails the same way. Neither
  returns wrong data, but the message does not say what to do. A clear
  refusal needs either a footer read per file at session open or a small
  per-store version record, which is a storage change.
- **Ask the owner: a trade seen twice is one row** (ADR 0013,
  "Consequences"). No existing log has a repeat.
- **Ask the owner: Coinbase check snapshots** (Phase 2a note). The owner
  asked for a 30-minute recording with a check snapshot every 60 s before
  deciding the rule (kept in `data/raw-probes/`, not in git). Only 6 of its
  28 snapshots are exact, and 13 differ at the best bid or ask, so "the top
  N levels are exact" fails for every N. The cause is timing. Coinbase's
  stream comes in batches about 50 ms apart, each with one venue time. A
  snapshot is exact exactly when its time equals a batch's time: all 7
  recorded on 2026-11-11, and 6 of the 29 on 2026-11-13. A snapshot timed
  between batch A and batch B equals the book after A on every level B does
  not change, in all 36 snapshots recorded. On B's levels it shows A's
  value, B's value, or a value the stream never shows. The rule and its ADR
  come in their own PR, after the owner confirms them.

### Phase 4: C++ order book, verified

- [ ] L2 order book: apply snapshot, apply delta, top-N query, best bid/ask,
      truncation to a subscribed depth, and a stale flag on an integrity
      failure (a sequence gap on Coinbase, a checksum mismatch on Kraken).
- [ ] Driven in tests from Python through the bindings, with events
      normalized by the Phase 1c adapters (C++ feed handlers come in
      Phase 6).
- [ ] Matches the Python reference reconstructor: top-of-book and top-20
      depth equal the Phase 2 golden files after every event, for both
      venues, with zero differences.
- [ ] Matches exchange snapshots: over the Phase 1c fixtures, the book
      equals every `CHECK_SNAPSHOT` on every level, matched by venue time
      as in Phase 2, and Kraken's checksum is computed and validated after
      every message.
- [ ] Both comparisons run as tests in `scripts/check.ps1` on the committed
      fixtures.
- [ ] Gap handling tested: on the Phase 2 gap fixtures (message removed,
      duplicated, swapped) the book is marked stale at the same event where
      the reference reconstructor raises, and it recovers on the next
      snapshot.
- [ ] Zero heap allocations in steady state, asserted by a test that counts
      `operator new` calls after warm-up.
- [ ] Benchmark for apply-delta and top-of-book recorded in the PR as the
      baseline (numbers and machine).

### Phase 5: SPSC queue and threading primitives

- [ ] SPSC ring buffer per ADR 0004; single-thread unit tests for wrap,
      full and empty.
- [ ] Two-thread stress test: at least 10^8 sequenced items, none lost,
      duplicated or reordered.
- [ ] Seqlock snapshot primitive with a reader/writer stress test.
- [ ] Thread pinning and naming wrapper for Windows (and Linux, compiled if
      a Linux build exists), with a test that the affinity took effect.
- [ ] Reusable allocation guard (counting `operator new`) for tests.
- [ ] Queue throughput and latency benchmark recorded as baseline.

### Phase 6: C++ feed handlers (offline)

Parsing and sequencing only, driven by recorded bytes. No sockets yet.

- [ ] **Ask the owner** to confirm the JSON parser dependency (proposal:
      simdjson via vcpkg). Record in a new ADR.
- [ ] Kraken and Coinbase handlers turn raw payloads into `MarketEvent`s
      with integer prices, never passing through `double`. Kraken sends
      prices as JSON numbers, so the handler must parse the digits itself.
- [ ] Integrity rules per venue (Coinbase sequence continuity, Kraken
      checksum), with detection and a resync state machine, tested with
      damaged fixtures.
- [ ] Feeding the Phase 1c fixtures through handler then book gives the same
      Phase 2 golden result as Phase 4, and the handlers' events equal the
      Python adapters' rows for every message.
- [ ] Malformed input is rejected and counted, never crashes (a fuzz-style
      test with truncated and corrupted payloads).
- [ ] No allocation per message after warm-up.

### Phase 7: Deterministic replay engine

- [ ] Replay engine reads raw logs and drives handlers, books and a strategy
      callback on one thread, in `seq` order within each log and merging
      several logs by `recv_mono_ns` through `logorder.merge_logs` (ADR
      0012), with time from the event stream.
- [ ] Journal writer: binary, append-only; reader in Python.
- [ ] Determinism test: two runs over the same log give byte-identical
      journals. Run in CI.
- [ ] Python binding: run a replay with a Python callback on the decision
      clock; the callback receives a `PITSession` whose `as_of` is simulated
      time.
- [ ] A test proves the callback cannot see data after simulated time.
- [ ] Replay speed recorded (events per second) as baseline.

### Phase 8: Simulated exchange and cost model

- [ ] Simulated venue in the replay engine: limit and market orders, matching
      against the recorded book, configurable order and data latency.
- [ ] Queue-position model for passive fills, documented with its
      assumptions and stated as conservative or optimistic.
- [ ] Cost model: maker/taker fees per venue from config, spread crossing,
      and slippage from walking the book. Config is versioned.
- [ ] Tests with hand-computed expected fills and costs for at least: market
      order through several levels, passive fill, partial fill, cancel
      before fill, order arriving after latency into a moved book.
- [ ] A backtest with zero latency and zero fees differs from the default
      configuration, proving the costs are applied.

### Phase 9: Pre-trade risk checks and kill switch

- [ ] Checks: kill switch, stale book, max order quantity, max position, max
      notional, price collar, order rate limit, hard loss stop.
- [ ] Limits come from a config file loaded at startup. No setter exists in
      C++ or in the bindings.
- [ ] Each check has a test that passes just inside the limit and rejects
      just outside it, and every reject is journaled with its reason.
- [ ] Structural test: the gateway's send function is only reachable through
      the risk check (enforced by type, for example a `RiskApproved` token
      only the checker can construct).
- [ ] Kill switch, once tripped, cannot be cleared from code paths reachable
      by bindings.
- [ ] Per-order check cost benchmarked.

### Phase 10: Order state machine and paper gateway

- [ ] Order lifecycle state machine (new, acked, partially filled, filled,
      cancelled, rejected) with tests for out-of-order and duplicate venue
      messages.
- [ ] Position and PnL accounting in integers, reconciled against fills in
      tests.
- [ ] Execution gateway interface with a paper implementation backed by the
      Phase 8 simulated venue.
- [ ] Target-following order management: given a `TargetSet`, works orders
      toward it through risk checks.
- [ ] No live venue adapter exists yet; the mode switch refuses `live`.

### Phase 11: Live market data connectivity

- [ ] **Ask the owner** to confirm the WebSocket/TLS dependency (proposal:
      Boost.Beast with OpenSSL via vcpkg). Record in a new ADR.
- [ ] Feed threads connect to Kraken and Coinbase public streams, with
      reconnect, resubscribe and resync.
- [ ] The feed process measures its clock offset at startup as the recorder
      does (ADR 0011), refuses to start above 250 ms unless overridden, and
      records the measurement and any override.
- [ ] Threads wired per architecture section 5: feed, core, journal, with
      pinning and wait strategy from config.
- [ ] Raw capture runs alongside; a recorded live session replays to the same
      book states the live core produced (journal comparison).
- [ ] Soak test: one hour live with zero unexplained gaps, queue high-water
      marks and latencies reported.
- [ ] Network tests are marked and excluded from the default `check.ps1`
      run; offline tests cover the logic.

---

## Part C: Signals, risk, paper trading

### Phase 12: Features: order flow imbalance and technicals

- [ ] Feature library with a registry: OFI (at least top-of-book and
      multi-level), spread, microprice, realized volatility, and a small set
      of bar technicals. Each feature declares its inputs and lookback.
- [ ] `PITSession.bars(venue, instrument, freq, start, end)`: OHLCV bars
      computed on demand from the trades visible at `as_of`, with integer
      prices and quantities. Intervals are half-open and aligned to the
      epoch. A bar whose interval has not ended by `as_of` is not returned.
      Tested on hand-computed bars, on an interval with no trades, and for
      look-ahead: a trade that arrives late changes its bar only for
      sessions opened at or after the trade's knowledge time. The bar
      technicals above read bars only through this method.
- [ ] Features read data only through `PITSession`. Import-lint test added
      that fails if signal code imports storage internals, the raw log or
      the adapters, or reaches into private attributes of the data package.
- [ ] Each feature has a test on hand-computed values.
- [ ] Look-ahead test per feature: its value at time t is unchanged when all
      data after t is deleted.
- [ ] Features needed live have a C++ incremental implementation that matches
      the Python batch implementation on the fixture to a stated tolerance.

### Phase 13: Validation harness

- [ ] Walk-forward evaluation and purged k-fold cross-validation with
      embargo; a test shows that label overlap leaks without purging and does
      not with it.
- [ ] All performance numbers are net of the Phase 8 cost model. The harness
      has no option to report gross only.
- [ ] Experiment registry: spec, code version, data range, cost-model
      version, results, and a trial counter per hypothesis family.
- [ ] Reports include deflated Sharpe (or equivalent multiple-testing
      adjustment), turnover, capacity estimate and drawdown.
- [ ] A locked final holdout period is defined in config; the harness
      refuses to evaluate on it without an explicit human flag.
- [ ] Sanity tests: a random signal has no significant net edge; a signal
      with deliberate look-ahead is caught by the harness's leak check.

### Phase 14: Regime detection and the blended model

- [ ] **Ask the owner** to confirm the model family (proposal: regularized
      linear blend first, gradient-boosted trees second). Record in an ADR.
- [ ] Regime detector (volatility/liquidity states) fitted only on past data
      within each fold.
- [ ] One blended model combining microstructure, technical and regime
      inputs, producing `Signal` objects.
- [ ] Evaluated in the Phase 13 harness. The report states plainly whether
      there is a net-of-cost edge. A negative result is an acceptable
      outcome for this phase; a hidden one is not.
- [ ] Model artifacts are versioned and reproducible from the registry entry.

### Phase 15: Risk/portfolio layer

- [ ] Sizing from signals to target positions (proposal: volatility-scaled
      with caps), producing `TargetSet`.
- [ ] Exposure limits: per instrument, per venue, gross and net, from the
      same human-approved config as the C++ limits; consistency test between
      the two.
- [ ] Drawdown guard: at threshold, pause (no new entries, cancel resting
      orders), then recall (flatten under a liquidation policy, allocation to
      zero). Tested in replay with a synthetic losing path.
- [ ] Resume is only possible through a human-invoked CLI command and is
      journaled.
- [ ] Control plane: pause, flatten and snapshot through the bindings; no
      binding raises a limit.

### Phase 16: Paper trading end to end (crypto)

- [ ] One command starts paper trading: live data, C++ core, Python decision
      clock, paper gateway. Startup prints the mode; default is paper.
- [ ] Runs unattended for 24 hours without crash, unexplained gap or journal
      loss.
- [ ] Reconciliation: replaying the captured raw log through the backtest
      reproduces the paper session's orders and fills, with differences
      explained.
- [ ] Killing the Python process leaves C++ risk checks and the hard loss
      stop working (tested).
- [ ] Daily report generated from the journal: PnL, fills, slippage versus
      model, risk events.

### Phase 17: US equities data via Alpaca

- [ ] Alpaca market data adapter into the data core (paper/free endpoints).
- [ ] Trading calendar and session handling; instrument metadata for
      equities.
- [ ] Corporate actions (splits, dividends) stored bitemporally; adjusted
      series computed point-in-time, with a test around a real split.
- [ ] Quality checks extended for equities.

### Phase 18: Alpaca paper execution

- [ ] Gateway adapter for Alpaca's paper endpoint behind the same interface
      as the crypto paper gateway.
- [ ] The base URL is checked at startup; a non-paper URL in paper mode is a
      fatal error.
- [ ] Equities paper session runs through one full market day.

---

## Part D: Agent layer

Do not start until Phase 16 is checked off: paper trading must work without
the agent.

### Phase 19: Agent tool server and audit log

No LLM in this phase.

- [ ] Tool server implementing exactly the tools in architecture section 8.
- [ ] Audit log: append-only, written before the effect; a failed write
      fails the call.
- [ ] Permission test suite: for each forbidden operation in section 8.2, a
      test shows it is unreachable from the agent process (no tool, no
      import path, no credentials in its environment).
- [ ] `request_pause` works end to end in paper mode and can only be lifted
      by the human CLI.
- [ ] Holdout period and trial budget are enforced server-side.

### Phase 20: Agent research loop

- [ ] **Ask the owner** to confirm model, budget per loop and how reports are
      delivered.
- [ ] Agent process runs propose, test, report using only the tool server.
      Model: the latest Claude model via the Anthropic API.
- [ ] Every action appears in the audit log with reasoning.
- [ ] A loop with a scripted fake LLM is tested in CI (no network).
- [ ] Prompt-injection test: data fields containing instructions do not
      change which tools are available or called outside the allow-list.
- [ ] Spend and iteration caps enforced outside the model.

### Phase 21: Agent monitoring

- [ ] Agent periodically reviews paper performance and health via read tools
      and writes a report.
- [ ] Defined triggers (for example slippage far above model, feed
      degradation) lead to `raise_flag` or `request_pause`, tested with
      injected scenarios in replay.
- [ ] Monitoring runs outside the tick loop; a test shows core latency is
      unchanged with the agent running or killed.

### Phase 22: Live trading readiness review

A human gate, not a coding phase.

- [ ] **Ask the owner.** Nothing in this phase proceeds without explicit
      instruction.
- [ ] Checklist reviewed with the owner: paper track record, reconciliation
      quality, limits file, kill switch drill, key handling, venue terms.
- [ ] Live gateway adapter and the three-part opt-in (architecture section 9)
      implemented and tested against a venue testnet or minimum size.
- [ ] Live mode remains off by default after this phase.
