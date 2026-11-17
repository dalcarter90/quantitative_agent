# Data core specification

Status: current as of ADR 0019 (2026-11-16).

This is the one description of the data core
(`python/src/quantagent/data/`) as it is: what it stores, what each time and
key means, how the store is written and read, and what the adapters, the
quality checks and the reference order book do. It supersedes the chain of
amendments in ADRs 0005 and 0008 to 0019, and the raw-log and normalized-store
tiers of ADR 0006. Those ADRs keep their history: why each decision was
made, what else was considered, and what the owner confirmed. Appendix A maps
each to the sections here.

**Changing the data core** means a new ADR for the decision and an edit of this
document in the same change. Where this document and the code disagree, one of
them is wrong; fix it. `docs/data-core.md` is the guide to the commands;
`docs/architecture.md` places the data core in the system.

Words used throughout: a **raw log** is one recording session's capture file;
a **record** is one entry in it, named by `(log_id, seq)`; a **row** is one
row of the normalized store; a **load** is one ingest of one raw log into the
store; a **session** is either a recording session (one raw log) or a
`PITSession`, and which is meant is always said.

## 1. Invariants

These hold everywhere; the rest of the document is how.

1. **Raw logs are the source of truth.** They are never rewritten. The store
   can always be rebuilt from them.
2. **The store is append-only.** No file is ever opened for writing, renamed
   or removed, except the files of a batch that failed before it was
   committed, which no reader could have seen. A correction is a new row with
   a later knowledge time; a reload is a new load. There is no update and no
   delete (CLAUDE.md safety rule 10).
3. **Research reads only through `PITSession`.** Signal and agent code never
   open store files or a database. A session cannot see a row received after
   its `available_at` or written after its `version`, through any method, and
   nothing it hands out can move either cutoff (CLAUDE.md safety rule 9).
   Only a `ReplayCursor`, which belongs to the backtest harness, moves
   `available_at`, and only forward.
4. **Prices and quantities are integers** from the venue's text to the store
   and the C++ types. Nothing converts through `float` on the way in.
5. **No key or order depends on `recv_ts`.** It is a measurement of the wall
   clock. Order within a log is `seq`; stored data is ordered by
   `(log_id, seq)`.
6. **Nothing is dropped silently.** Every raw record ends as data, ignored (a
   known kind of message with no market data) or rejected with a reason, and
   every integrity fault is a stored stream event.

## 2. Representation

### Prices and quantities

- A price or quantity is an `int64` **mantissa**: `value = mantissa ×
  10^-exp`.
- **Each instrument has one fixed price exponent and one fixed quantity
  exponent** (`price_exp`, `qty_exp`), listed in `adapters.FIXED_EXPONENTS`
  and recorded on every `instruments` row. They are independent of the tick
  and lot sizes and chosen generous enough for any tick or lot size the
  instrument may have. A tick-size change never changes the representation:
  a price has the same mantissa before and after it.

  | Venue, instrument | `price_exp` | `qty_exp` |
  |-------------------|-------------|-----------|
  | Kraken BTC-USD | 8 | 8 |
  | Coinbase BTC-USD | 8 | 8 |

  BTC-USD prices are held to 1e-8 USD, up to about 9.2 × 10^10; quantities to
  one satoshi. An instrument not in the table has no representation: its
  metadata is refused (`no_fixed_exponent`, section 7) until one is chosen
  and added. Metadata needing more decimals than the fixed exponent is
  refused too (`precision_exceeds_exponent`); the exponent is never changed
  to fit.
- **The venue's precision** (`price_precision`, `qty_precision`) is how many
  decimals the venue itself uses, per metadata version: Kraken's
  `price_precision` and `qty_precision`; for Coinbase, which states none, the
  decimals of its increments. It is never more than the exponent. It can
  change; the exponent cannot. Kraken's checksum is computed over the digits
  Kraken prints, which are the mantissa divided by `10^(exp - precision)`
  (section 7).
- **Tick size and lot size are validation rules**, stored as mantissas at the
  exponents (a valid price is a multiple of the tick). They are not the unit
  of representation.
- Text is converted by `fixedpoint.to_mantissa(text, exp)`, exactly, refusing
  more decimals than `exp`; `to_decimal_str` converts back. Exponents are 0 to
  18.
- **Notional** (`price × qty`) is exact in a 128-bit intermediate, at
  exponent `price_exp + qty_exp` (C++ `notional`, `_mul128` on MSVC,
  `__int128` on GCC and Clang).
- **In C++**, `Price` and `Qty` are distinct strong types over `int64`, with
  no implicit conversion to each other, to `bool` or to floating point, and
  constructible only from integers that fit without loss; `InstrumentId` only
  from unsigned integers of at most 32 bits (architecture section 6.3).
- Research may convert to `float64` explicitly, one way. Order prices are
  always built from integers.

### Timestamps and order

- Every timestamp is `int64` nanoseconds since the Unix epoch, UTC: years 1678
  to 2262.
- Every stored row carries three times, each with one job:

  | Time | Column | What it is | What it is used for |
  |------|--------|------------|---------------------|
  | Venue time | `event_ts` | When the venue says it happened | Lag, venue-time order, features. Never a cutoff, a window or a partition |
  | Receipt time | `recv_ts` | When our process received the bytes, by the recording machine's wall clock | **Availability**: what a session at simulated time T may see; every time window; the date partition |
  | Knowledge time | `knowledge_ts` | Which batch of the store the row was written in | **Version**: which corrections a session reads |

  A raw log is loaded after it was recorded, so a row's knowledge time is
  hours or days after its receipt time; the two are never used for each
  other.
- **`event_ts_source`** says where `event_ts` came from: `"venue"` or
  `"receipt"`. A message type that carries no venue timestamp (Kraken's and
  Coinbase's instrument metadata) stores `recv_ts` in `event_ts` with source
  `"receipt"`; the schema enforces `event_ts == recv_ts` for those rows. The
  source is decided by the kind of message, never by comparing the two times.
  A venue timestamp that is present but implausible is stored as given, if
  the schema can hold it (section 7). Anything that reads `event_ts` as venue
  time filters on `event_ts_source = 'venue'`.
- **`seq`** is the record's number in its raw log, from 0. Within one log,
  replay order is `seq` alone.
- **`log_id`** names the raw log a row came from (section 3). Stored data is
  ordered by `(log_id, seq)`: one log after another, in the order they began,
  each in the order its records arrived.
- **`recv_mono_ns`**, in raw logs only, is the machine's monotonic counter
  (`time.perf_counter_ns`) read with `recv_ts`. It merges logs recorded on one
  machine in one boot, and detects wall-clock steps (section 4).
- Hot-path code takes time from an injected `Clock`; in-process latency is
  measured on the monotonic clock and stored as a duration.

## 3. Raw logs

- A raw log is one segment file: `<name>.qaraw` while written, `<name>.qaraw.zst`
  once sealed (the same bytes, zstd-compressed). It is never reopened for
  writing.
- Layout, little-endian:

  ```
  file   := MAGIC record*                 MAGIC = "QARAW\x02\n" (version 2)
  record := length:u32 body crc:u32       crc = CRC32 of body
  body   := recv_ts:i64 recv_mono:i64 seq:u64 venue_len:u8 venue channel_len:u8 channel payload
  ```

  `recv_mono` is -1 when a record has none. **Version 1** logs (`"QARAW\x01\n"`,
  everything recorded before ADR 0012, including the committed fixtures) have
  no `recv_mono`; they are read with `recv_mono_ns` None and never rewritten.
  The writer writes version 2 only.
- `seq` starts at 0 and increases by one per record; the reader checks it. A
  partial final record in an unsealed log is dropped and reported; any other
  damage raises.
- **`log_id`** is `rawlog.log_id_of(first record)`:
  `<UTC time of the first record, to the ns>Z-<first 12 hex digits of SHA-256>`,
  for example `20261111T030919.259642800Z-2ca464c155f1`. The digest covers the
  first record as framed (`recv_ts`, `recv_mono`, `seq`, venue, channel,
  payload). It depends on nothing else, so a log keeps its id sealed or not,
  cut short, copied or renamed, and ids sort by when their logs began. It is
  derived, never written into the log.
- **Merging logs.** `logorder.merge_logs(paths)` interleaves logs by
  `recv_mono_ns`, then log, then `seq`, if their clock identities are
  comparable (same machine id, boot id and counter; an unknown machine or
  boot is never comparable). It refuses logs that are not comparable or have
  no monotonic stamps; it never merges by wall time. One log is read in `seq`
  order.
- Reading a raw log is not point-in-time. It is for capture, the adapters and
  the replay engine, never for research.

## 4. Recorder

`python -m quantagent.data.recorder --venue V --instrument I --out DIR
--duration S [--probe-interval S] [--depth N] [--allow-clock-offset]`

- Public market data only. No credential is read or sent.
- **The clock first.** Before contacting the venue or writing anything, the
  recorder measures its wall clock against four NTP servers of independent
  operators (`clock.measure_offset`): three SNTP samples per server, the
  fastest reply per server, the median across servers, at least two servers
  answering. The offset is this clock minus NTP time (positive: ahead). Above
  250 ms, or unmeasurable, it refuses to start (exit status 2, nothing
  written) unless run with `--allow-clock-offset`. The limit is a constant,
  not an option. The measurement, and any use of the override, is written into
  the log. Every component that stamps wall-clock time assumes a clock kept
  synchronized to NTP.
- **Channels in the log:** `recorder` (JSON control records: `start`, `clock`,
  `connect`, `subscribe`, `disconnect`, `rest`, `stop`), `ws` (every message
  of the stream connection, verbatim), `probe` (messages of a short second
  connection opened every probe interval for a fresh book snapshot),
  `rest:<name>` (a REST response body, verbatim).
- The `start` record names the venue, instrument, venue symbol, depth,
  recorder version and the clock identity (platform, machine id, boot id,
  counter and its resolution). The `clock` record follows it.
- **Instrument metadata comes before any market data**, so a metadata version
  is in effect from the session's first row (ADR 0019). Coinbase's product is
  fetched over REST before connecting. On Kraken the stream connection
  subscribes to the `instrument` channel first and sends the book and trade
  subscriptions only once the instrument snapshot has been received and
  written; if it has not come within 20 s, the connection is a failure and is
  opened again.
- Every record is stamped with both clocks in one call, right after its
  message arrived and before the next read; the recorder writes it with no
  `await` in between, so stamps cannot decrease within a log.
- A dropped stream connection is recorded (`disconnect` with the reason) and
  reopened after a short delay. A receive timeout within 0.1 s of the session
  deadline ends the session; an earlier one is a failure and reconnects.
- Recorder versions: 1 (Phase 1c), 2 (clock measured, ADR 0011), 3 (monotonic
  stamps and clock identity, ADR 0012), 4 (Kraken's metadata before its
  market data subscriptions, ADR 0019).

## 5. Datasets

Every row has the leading columns `event_ts`, `event_ts_source`, `recv_ts`,
`log_id`, `seq` (section 2), and in storage `knowledge_ts` after `recv_ts`.
Venue, instrument and date are not columns in the files: they are partition
directories (section 8), and columns of the session's views. A row's identity
is (venue, instrument) plus its dataset's key. Every key names the raw record
the row came from.

| Dataset | Version | One row per | Key | Other columns |
|---------|---------|-------------|-----|---------------|
| `trades` | 3 | Observation of a trade | `log_id, seq, trade_id` | `trade_id` (the venue's), `price`, `qty`, `side` (the aggressor: `BUY` 1, `SELL` -1, `UNKNOWN` 0) |
| `book_events` | 3 | Price level of a book message | `log_id, seq, level_index` | `kind`, `venue_seq_first`, `venue_seq_last`, `checksum`, `depth`, `level_index`, `side` (`BUY` or `SELL`), `price`, `qty` |
| `instruments` | 4 | Version of an instrument's metadata, as observed in one record | `log_id, seq` | `price_exp`, `qty_exp`, `price_precision`, `qty_precision`, `tick_size` (at `price_exp`), `lot_size` (at `qty_exp`) |
| `stream_events` | 2 | Integrity event (section 6) | `log_id, seq, event_index` | `kind`, `connection`, `expected`, `observed`, `detail` |

- **`trades`** stores every observation the normalizer keeps (a message can
  carry several trades; a trade twice in one message is kept once).
  `Dataset.one_row_per = ("trade_id",)`: a session shows one row per
  (venue, instrument, `trade_id`), the first observation in `(log_id, seq)`
  order (section 9).
- **`book_events`.** The levels of one message share `log_id`, `seq` and the
  integrity columns, and are numbered by `level_index` in the order the venue
  listed them; order matters, since a venue may change one price twice in a
  message. `kind` is `SNAPSHOT` (0, a level of a book image that starts or
  restarts the stream), `DELTA` (1, a level of an update; quantity 0 removes
  the level) or `CHECK_SNAPSHOT` (2, a level of an independent image from a
  probe connection, not part of the stream). The integrity columns are
  optional and each venue fills what it has: Coinbase `venue_seq_last` (its
  `sequence_num`), Kraken `checksum` (unsigned 32-bit, the venue's checksum
  after the message) and `depth` (the levels per side it keeps; null means the
  full book). Nothing is synthesized.
- **`instruments`.** `event_ts` is the receipt time for both venues
  (`"receipt"`). Which version is in effect for a row is section 9.
- **Structural rules only.** The schema refuses wrong types, nulls in
  required columns, unknown enum values, negative timestamps, a `log_id` not
  of the form above, a `"receipt"` row whose `event_ts` differs from its
  `recv_ts`, exponents and precisions outside 0 to 18, a precision above its
  exponent, and a tick or lot size below 1. Implausible market values (a zero
  or negative price) are stored as received and left to the quality checks.
- `schema.table_from_rows` builds tables and refuses floats, bools and numeric
  strings that pyarrow would coerce; `schema.check_row` applies the same rules
  to one row (a property test shows they accept the same rows).
- Every Parquet file records `qa.dataset` and `qa.schema_version`.

## 6. Stream events

One row per integrity event, written by the adapters in the same load as the
data they describe. `event_ts` is always the receipt time of the record the
event was noticed at (`"receipt"`); `connection` is `ws`, `probe` or null.

| # | Kind | Written when | `expected` | `observed` | `detail` |
|---|------|--------------|------------|------------|----------|
| 0 | `SESSION_START` | The recorder's `start` record | | | JSON: `symbol`, `depth`, `recorder_version`, `clock_identity`, `monotonic_stamps` |
| 1 | `SESSION_STOP` | The `stop` record | | | |
| 2 | `CONNECT` | A connection was opened. A second `CONNECT` on `ws` in a session is a reconnect | | | URL |
| 3 | `DISCONNECT` | A connection ended | | | The reason (`session ended` and `snapshot received` are normal) |
| 4 | `SUBSCRIBE_REQUEST` | We asked for a channel | | | The request |
| 5 | `SUBSCRIPTION_ACK` | The venue confirmed a subscription change | | | What it confirmed |
| 6 | `TRADE_BACKFILL` | The trades of this record are history sent on subscribe, not trades seen as they happened | | | |
| 7 | `CLOCK_OFFSET` | The recorder's clock measurement at session start | The limit in force, ns | The offset, ns (positive: the clock was ahead), or null if unmeasured | JSON: the per-server results |
| 8 | `CLOCK_OVERRIDE` | The recorder ran with `--allow-clock-offset` | | | Whether the offset was within the limit, above it, or not measured |
| 9 | `CLOCK_STEP` | `recv_ts - recv_mono_ns` changed by more than 25 ms between consecutive records | | The signed step, ns (negative: the wall clock went back) | |
| 10 | `SEQUENCE_GAP` | Coinbase: a `sequence_num` higher than expected on a connection | The number expected | The number received | |
| 11 | `SEQUENCE_REPEAT` | Coinbase: the previous number again | Expected | Received | |
| 12 | `SEQUENCE_OUT_OF_ORDER` | Coinbase: a number lower than the previous | Expected | Received | |
| 13 | `CHECKSUM_MISMATCH` | Kraken: the venue's checksum differs from the one computed from the book the adapter rebuilt. The connection's book is suspect, and nothing more is reported for it, until its next snapshot | The venue's | The computed | |
| 14 | `MESSAGE_REJECTED` | A record could not be normalized | | | `reason: detail`, and the events noted for it that the store could not hold (at most 200 characters) |
| 15 | `CHECK_SNAPSHOT_COMPARED` | Coinbase: a check snapshot was compared with the rebuilt book (section 7), at the record where the comparison could be made | Levels that break the rule, or null if not compared | Unverified levels that differ, or null | JSON: `snapshot_seq`, `levels`, `result` (`exact`, `explained`, `mismatch`, `not_compared`), `reason`, `on_batch`, `next_batch_levels`, `violations`, `violations_shallowest`, `violations_deepest`, `unverified`, `unverified_as_next_batch`, `unverified_shallowest`, `unverified_deepest` |
| 16 | `TRADE_CONFLICT` | A `trade_id` seen again in one normalizer with a different price, quantity or side, at the later record | | | JSON: `trade_id`, `differs` (the fields), `first` (its `log_id`, `seq`, price, qty, side) and `now` |
| 17 | `TRADE_REPEAT` | A `trade_id` seen again in one normalizer, identical; the repeat is not stored | | ns of `recv_ts` after the first observation | JSON: `trade_id`, and the first observation's `log_id` and `seq` |

Ranks in `CHECK_SNAPSHOT_COMPARED` count the snapshot's levels on that side
that are better than the price: 0 is the best bid or ask. Events noticed before
any `start` record have no instrument to be stored under and stay in the
normalization report only. A clean session produces a handful of events; a
broken one one per fault, not one per message.

## 7. Adapters

`adapters.normalize(records)`, the streaming `adapters.Normalizer` (`feed`,
`take_rows`, `report`), and `adapters.ingest(path, store)`, which loads one raw
log as one load of one batch (section 8). `python -m quantagent.data.adapters
<log>` checks a log without writing (exit 1 on any reject, sequence anomaly or
partial record); with `--store DIR` it loads it.

### Rules for every venue

- **Every record has one outcome**: `data` (it produced rows), `ignored` (a
  known kind of message without market data: heartbeats, statuses,
  subscription acks, control records) or `rejected`, with a reason and a
  detail in the report, and a `MESSAGE_REJECTED` event.
- **A message is all or nothing.** If any part is malformed, none of its rows
  are kept. Every row and every event noted for a record is checked with
  `check_row` before any is kept; a value the schema cannot hold (a venue time
  after 2262, a sequence number beyond int64) rejects the record
  (`schema_refused`) instead of failing the load. For any rejected record, the
  events noted for it that the store can hold are kept (a gap noticed before a
  parse failure, an override), and those it cannot are named in the
  `MESSAGE_REJECTED` detail. A rejected record tells the adapter (`on_reject`):
  it is a hole in the stream.
- **Reject reasons:** `invalid_json`, `malformed_message`, `unknown_message`,
  `unknown_channel`, `unknown_venue`, `venue_error`, `unexpected_symbol`,
  `no_log_start` (a record before any `seq` 0), `no_session_start`,
  `no_instrument_metadata` (market data before metadata), `no_fixed_exponent`,
  `precision_exceeds_exponent`, `schema_refused`.
- **One log at a time, in `seq` order.** A record does not say which log it is
  from; the normalizer takes the `log_id` from each `seq` 0 record. A record
  whose `seq` is neither 0 nor the previous plus one raises `InterleavedLogs`
  (a `ValueError`), and the normalizer then refuses everything, `feed` and
  `take_rows` alike, because rows it holds may name the wrong log. `ingest`
  reads one file, so it is never affected; `merge_logs` output cannot be fed
  to one normalizer.
- **Metadata** rows are built by `VenueAdapter.metadata` at the fixed
  exponents (section 2), and refused as `no_fixed_exponent` or
  `precision_exceeds_exponent`.
- **Clock steps:** a change in `recv_ts - recv_mono_ns` above 25 ms between
  consecutive records of a log is a `CLOCK_STEP` event.
- **Trades seen again**, within one normalizer: each trade is compared with
  the first observation of its (venue, instrument, `trade_id`) across every
  log fed to it. An identical repeat (same price, quantity and side) is not
  stored and is a `TRADE_REPEAT`; only the repeated trades of a message are
  dropped. A repeat that differs is stored and is a `TRADE_CONFLICT`. A trade
  in a rejected record is not remembered as seen. Other fields (the trade's
  time) are not compared. Whatever is stored, a session shows the first
  observation; logs loaded in separate runs are compared by the
  `trade_observations` quality check (section 10).
- The report (`Report.summary()`) counts records by outcome, rows per dataset,
  rejects by reason, sequence anomalies, checksum mismatches, and `trade
  conflicts: N (trades seen again: M; identical repeats dropped: K)`.

### Kraken (WebSocket v2)

- Channels `instrument`, `book` (with the subscribed depth), `trade`. Symbol
  `BTC/USD` is stored as instrument `BTC-USD` (`/` becomes `-`, since the
  name is a directory).
- Numbers arrive as JSON numbers printed at the pair's precision and are read
  as text, so no digit is lost.
- **Book:** no sequence numbers. The book is the subscribed depth per side;
  after every message the client drops the levels beyond it (the venue sends
  no removal for them). Every snapshot and update carries a CRC32 of the ten
  best asks (ascending) then the ten best bids (descending), each level as
  its price digits then quantity digits with the decimal point and leading
  zeros removed, at the precision Kraken prints. The adapter keeps the top of
  each connection's book, applies every message, computes the checksum over
  `mantissa // 10^(exp - precision)` with the precision in force, and writes
  `CHECKSUM_MISMATCH` when it differs. It covers ten levels per side: a lost
  update below them is unseen unless its level later reaches the top ten (36%
  of single removals on the fixture were never caught), and an update applied
  twice changes nothing.
- **Trades:** `side` is the taker's. `trade_id` is an integer, stored as text.
- **Metadata:** the `instrument` channel lists every pair; only the recorded
  one is stored. `price_precision` and `qty_precision` are Kraken's;
  `price_increment` and `qty_increment` are the tick and lot sizes. A pair's
  precision can change mid-session (an `update`); prices keep their mantissas
  and the checksum follows the new digits.

### Coinbase (Advanced Trade)

- Channels `level2`, `market_trades`, `heartbeats` (public, no key); product
  metadata over REST (`rest:product`).
- **Sequence:** `sequence_num` counts every message on one connection, across
  all channels, from 0. The adapter checks it per connection and writes
  `SEQUENCE_GAP`, `SEQUENCE_REPEAT` or `SEQUENCE_OUT_OF_ORDER`; it keeps the
  highest number seen, and refuses a `sequence_num` outside 0 to 2^63 - 2
  before it becomes state. Because heartbeats and trades share the numbering,
  a lost book message is visible only through these events.
- **Book:** the snapshot is the full book (about 38,000 levels); each update
  level carries its own `event_time`. `new_quantity` "0" removes a level.
- **Trades:** `side` is the maker's, so the stored aggressor is the opposite.
  The subscription starts with a snapshot of recent trades (`TRADE_BACKFILL`).
- **Metadata:** precisions are the decimals of `price_increment` and
  `base_increment`.
- **Check snapshots** (ADR 0014). The stream comes in batches about 50 ms
  apart, every update of a message carrying one `event_time`; a check
  snapshot carries one time T for all its levels. The adapter rebuilds the
  book from the stream, keeps the last 60 s of messages, and judges each check
  snapshot against A, the last stream message of its connection with venue
  time at or before T, and B, the message after A:
  - T equal to A's time: every level must equal the book after A (`exact`, or
    `mismatch`).
  - T strictly between A's and B's times: every level B does not list must
    equal the book after A; B's levels are **unverified**, counted and ranked,
    never errors (`explained` if any differ).
  - No B when the stream ends: every level must match.
  - **Not compared**, with a reason: `no_stream_book`, `before_stream_snapshot`
    (the stream's own snapshot is after T), `next_message_is_snapshot`,
    `stream_unreliable` (a message was lost or rejected since the last
    snapshot), `history_too_short` (T is more than 60 s back).

  The event is written at the snapshot's record, or at the later record where
  B arrived. A snapshot still waiting when a log ends without its stop record
  gets no event. Kraken check snapshots get no event: the reference book
  compares them (section 11), with no between-batches exemption.

## 8. Store

`AppendOnlyStore(root)`, store layout 3:

```
<root>/<dataset>/venue=<venue>/instrument=<instrument>/date=<YYYY-MM-DD>/
    part-<knowledge_ts, 20 digits>-<number, 6 digits>.parquet
<root>/_batches/batch-<knowledge_ts, 20 digits>.json        one commit marker per batch
```

- `date` is the UTC date of `recv_ts`. Venue and instrument names are 1 to 64
  of `A-Z a-z 0-9 . _ -`, starting and ending with a letter or digit; a store
  path containing `=` is refused.
- **Every write is a load** of one raw log, and `load(log_id)` is the store's
  only public operation. It returns a `Load` with `batch(*, last=False)`,
  `finish()` (an empty last batch), `log_id`, `load_id` and `finished`. Every
  row appended to a load's batch must name its log. `load_id` is the knowledge
  time of the load's first batch. A load is **complete** once its last batch
  (`last=True`, or the one `finish` commits) is committed; that marker is
  published even with no rows. A finished load takes no more batches.
  `ingest` is one load of one batch with `last=True`; an empty raw log loads
  nothing. The private `_batch` and `_append` write outside any load, for
  tests only; no production code calls them (a test checks the syntax tree).
- **A batch.** Every table appended to it, in every dataset, gets the same
  `knowledge_ts`, read from the clock when the batch opens and strictly
  greater than any issued before, including by earlier processes (recovered
  from file and marker names) and by batches never committed. Knowledge times
  start at 1. A batch's data files are written first, each created under its
  final name (the create fails if the name exists). Its **commit marker** is
  published last, by a hard link, which appears in one step and cannot
  replace a file. The marker is JSON: `layout` (3), `knowledge_ts`, `files`
  (paths within the store), `rows` (per dataset) and `load` (`null`, or
  `load_id`, `log_id` and `last`).
- **Only committed batches exist.** A data file that no marker names is not
  part of the store: that is a load still running, failed or dead. A batch
  that raises is abandoned and its files removed; nothing of it was ever
  readable. A load that dies leaves files without a marker, never deleted
  automatically. A batch refuses to commit behind a later committed one.
- **Versions.** Every committed batch is a version, named by its knowledge
  time; version 0 is the empty store. Batches commit in knowledge-time order,
  so a version, once it can be read, never gains a batch. Versions have gaps
  (abandoned and dead batches).
- **Corrections and reloads.** A correction of a record is a row with the same
  key and a later knowledge time; the batches of one load correct each other
  that way. Loading a raw log again is a new load; sessions read each log from
  one load (section 9), so rows the new load no longer produces stop
  appearing, in every dataset. The earlier load stays on disk and is what a
  session at an earlier version reads. Nothing is compacted.
- **Schema and layout.** A session checks every file it will open (its
  footer's dataset, schema version and columns) and every marker's layout, and
  the writer refuses a store of another layout. A store of another version
  (before loads, before commit markers, before `log_id`, or `instruments`
  version 3) is refused with a `StoreVersionError` saying to rebuild it from
  the raw logs into a new directory. Damaged markers (no `load`, a `load_id`
  that is not an earlier knowledge time, a malformed `log_id`, two batches of
  one load naming different logs, a batch after its load's last) are refused
  with a `ValueError`.
- **Limits.** One writer per store root, assumed and not locked. Hard links
  need NTFS or ext4. Names that differ only by case are one directory on
  Windows and two on Linux.

## 9. `PITSession`

```python
with PITSession(root, available_at=t, version=None) as s:
    s.trades(venue, instrument, start, end)          # start <= recv_ts < end
    s.book_events(venue, instrument, start, end)
    s.stream_events(venue, instrument, start, end)
    s.trade_observations(venue, instrument, start, end)
    s.loads()                                        # the load each log is read from
    s.instruments(venue, instrument)
    s.instrument(venue, instrument, at=u)            # in effect at time u (default t)
    s.instrument_for(venue, instrument, log_id=..., seq=...)   # in force for a row
    s.sql("SELECT ...")                              # one SELECT over the four views
```

### What a session reads

1. **Batches.** Only committed batches, as of when the session opened, at or
   before `version`. `version` defaults to the latest committed batch; a
   `version` later than that is refused with a `ValueError` (it would not be
   repeatable). `session.version` says which is in force.
2. **Loads.** Each log is read from one load, chosen by the version alone: its
   latest complete load (greatest `load_id` whose last batch is within the
   version), or, if it has none, its latest incomplete load (its committed
   batches within the version, so a first load is readable chunk by chunk
   while it is written). A reload in progress never hides the previous
   complete load. Rows written outside a load (tests) are read, record by
   record, for logs that have no load.
3. **Receipt.** Only rows with `recv_ts <= available_at`.
4. **Versions of records.** Of the rows left, each record's latest version
   (greatest `knowledge_ts`). Both cutoffs apply first, so a correction
   received after `available_at` does not hide the original.
5. **One row per trade.** `trades` shows, per `trade_id`, the first
   observation in `(log_id, seq)` order among those left. A window holds a
   trade if that observation was received in it.

`available_at` is required, keyword-only, with no default, and neither cutoff
can change after the session opens. `as_of` (the single knowledge-time cutoff
of ADRs 0006 to 0016) is retired: passing it raises a `TypeError` naming both
replacements.

### Methods

- **Range methods** (`trades`, `book_events`, `stream_events`) take a
  half-open window on **receipt time**, for every dataset, so a window selects
  market data and the integrity events noticed at the same records by one
  clock. They return rows in `(log_id, seq)` order, then every other column,
  as pyarrow tables with the stored columns.
- **`trade_observations`** returns every stored observation received in the
  window, with its trade's first observation (wherever it was received) as
  `first_log_id`, `first_seq`, `first_recv_ts`, `first_price`, `first_qty`
  and `first_side`. It has no SQL view, so research counting trades reads
  `trades`.
- **`loads()`** returns, per loaded log in `log_id` order, a `LoadRead`: the
  `log_id`, the `load_id` read, whether it is `complete` within the version,
  and how many of its `batches` are committed within it. A log with
  `complete` false is read from an incomplete load and may go on beyond what
  the session reads.
- **`instruments`** returns every metadata version the session can see,
  ordered by `event_ts`, then `(log_id, seq)`.
- **`instrument_for(..., log_id=, seq=)`** is the version a stored row was
  written with: the latest version of the same log at or before its record.
  The adapters refuse market data before metadata, so every stored market
  row has one.
- **`instrument(..., at=)`** is the version in effect at a time: a log's first
  version is in effect from the log's first row (the earliest `event_ts` of
  any row of that log the session shows, in any view), each later version of
  a log from its own `event_ts`; of the versions in effect at `at`, the one
  that took effect last, then the later in `(log_id, seq)` order. So the
  version a recording session fetched describes all its rows, including
  history resent on subscribe with venue times before the session began. It
  returns None if nothing was in effect. Both return an `InstrumentMeta`
  (venue, instrument, `event_ts`, `recv_ts`, `knowledge_ts`, `log_id`, `seq`,
  the exponents, the precisions, `tick_size`, `lot_size`).
- **`sql`** accepts exactly one `SELECT` over the views `trades`,
  `book_events`, `instruments` and `stream_events`, each with `venue`,
  `instrument`, `date`, then the stored columns, holding what the session
  reads. To select by venue time, filter `event_ts` there. A result with two
  columns of one name is refused (name them apart with `AS`). Errors:
  `QueryRejected` (not allowed, not run) and `QueryError` (allowed, failed).

### Three independent layers

1. **Views** hold only what the session reads (above). The typed methods and
   `sql` read nothing else.
2. **Statement validation** (`sqlguard`): DuckDB parses the query, its own
   parse tree is checked against an allow-list of node kinds, tables,
   functions and types, and the session executes that same tree, never the
   text. Refused: DDL and DML, several statements, table functions and file
   paths or URLs (replacement scans), catalog and qualified names, `WITH`,
   parameters, sampling, lambdas, lists, structs, `VALUES`, functions that
   read settings, the clock or random state, and bare `current_date` and
   similar names (as column names and aliases too). The allow-list is
   extended only with pure, deterministic functions, each with a test
   (CLAUDE.md).
3. **Engine lockdown.** Each session has its own in-memory DuckDB instance
   that may open only the files of the committed batches it reads, at or
   before its version, of the loads it reads, with a date partition on or
   before the day of `available_at` (for a cursor, the day of `end`). External
   access, extension loading and replacement scans are off, and the
   configuration is locked. Even a statement past layer 2 could not reach a
   later version, an uncommitted batch, a replaced load or a later day.

Other properties: the session hands out no connection, file path or object
leading to one, and error messages have store paths removed; it cannot be
copied, pickled or subclassed; it is not thread-safe; opening it reads every
marker and every footer it will open (about 0.3 ms per file). DuckDB is
pinned to 2.0.x, because the validator depends on its parse tree (CLAUDE.md
has the upgrade rule).

### Replay cursor

`ReplayCursor(root, *, start, end, version=None)` opens the store once for a
backtest. `cursor.session` is a `PITSession`; `cursor.advance(now)` moves its
`available_at` forward, never back and never past `end`, by replacing the
views in one transaction. After `advance(now)` the session reads exactly what
`PITSession(root, available_at=now, version=...)` would. The harness keeps the
cursor and hands research only the session, which has no method that moves
time and no reference to the cursor.

## 10. Quality checks

`python -m quantagent.data.quality --store DIR --start YYYY-MM-DD --end
YYYY-MM-DD [--venue V] [--instrument I] [--available-at NS] [--version NS]
[--fail-on error|warning|info] [--config FILE] [--max-print N]`

- Each check is `check(session, venue, instrument, start, end, config)` and
  returns a `CheckResult`: `Finding`s (check, severity, a stable code, the
  dataset, the raw record it points at as `log_id` and `seq` with its
  `recv_ts`, a message) and statistics of what it examined, so that "no
  findings" can be told from "nothing looked at". Checks read only through the
  session and change nothing.
- **Ranges are receipt time:** the report's days (UTC, both inclusive) are the
  data received on them, with the events noticed on them. The report runs
  every check on every venue and instrument with data in the range, by
  default reading everything received at the latest version; it says which
  version it read. A range with no data is a `no_data` WARNING.
- **Limits are per severity** (`max_findings`, per check, instrument and
  severity). A lesser finding never displaces a worse one; findings cut are
  named in `CheckResult.cut`. SQL-limited queries each produce one severity.
- **Exit status:** 0; 1 if any finding is at or above `--fail-on` (default
  `error`), **or if any finding was cut** (a `TRUNCATED` line names the
  severities); 2 if the store is refused (`StoreVersionError`). `--as-of` is
  refused.

| Check | Finding | Severity |
|-------|---------|----------|
| `stream_integrity` (stream events) | `sequence_gap`, `checksum_mismatch`, `message_rejected`, `trade_conflict` | ERROR |
| | `sequence_repeat`, `sequence_out_of_order`; the stream connection dropping (`disconnect`, unless the session ended) or opened again in its session (`reconnect`, judged across range boundaries) | WARNING |
| | `probe_failed` (a probe without its snapshot); `trades_repeated`, once per session with `TRADE_REPEAT`s, with their count and range of delays | INFO |
| `independent_snapshots` (`CHECK_SNAPSHOT_COMPARED`) | `check_snapshot_mismatch`, with the count and the shallowest and deepest rank | ERROR |
| | `no_check_snapshot_compared`: a session (one log) none of whose check snapshots in the range could be compared, with how many and why; `check_snapshot_unreadable` (an unknown result) | WARNING |
| | `check_snapshot_between_batches` (explained), `check_snapshot_not_compared` (with the reason). Exact snapshots are counted. A session with no check snapshot at all does not warn | INFO |
| `recording_clock` (each session with rows in the range, wherever its start and clock records are) | `clock_offset_exceeded` (beyond `max_clock_offset_ns`); `clock_step` (beyond `max_clock_step_ns`, with size and direction, in the range where it happened) | ERROR |
| | `clock_unmeasured` | WARNING |
| | `clock_not_recorded` (a session from before clocks were measured); `clock_steps_not_measurable` (measured, no monotonic stamps); `clock_override` | INFO |
| `clock_skew` (rows with venue time; backfilled trades excluded) | `early` (`recv_ts - event_ts` below `min_lag_ns`, including venue time after receipt), `late` (above `max_lag_ns`), `event_ts_backwards` (venue time stepping back by more than `max_backwards_ns` between consecutive messages of one log, in `seq` order, check snapshots excluded). One finding per message | WARNING |
| `crossed_books` (every snapshot, stream and check) | `locked`, `crossed` | ERROR |
| | `one_sided` | WARNING |
| `outliers` | Zero or negative trade price or quantity; zero or negative book price; negative book quantity | ERROR |
| | `price_outlier` (more than `outlier_bps` from the lower median of the previous `outlier_window` trades in the range, in venue-time order, once `outlier_min_trades` precede it); `zero_quantity_in_snapshot` | WARNING |
| `trade_observations` (every observation of a trade received in the range) | `trade_conflict_across_logs`: an observation from a log other than its trade's first observation's, with a different price, quantity or side, at the later observation | ERROR |
| `loads` (every log with rows received in the range) | `incomplete_load`: the log is read from an incomplete load (still being written, or failed or died before its last batch). Names the log, the load, the batches committed and the last receipt time read; points at the last record read. Reported in every range the log has rows in; not for a log read from a complete load while a reload is in progress | WARNING |

`trade_observations` counts identical observations in other logs
(`repeated_across_logs`) without reporting them: both venues resend recent
trades on every subscribe. Conflicts within one log are the normalizer's
`TRADE_CONFLICT`. `ingest` writes a load as one batch, so a failed ingest
commits nothing and is never an `incomplete_load`; that finding comes from
loaders that commit a log in chunks.

Thresholds are `QualityConfig`, loadable from a JSON object whose unknown
names are an error: `min_lag_ns` −0.5 s, `max_lag_ns` 10 s, `max_backwards_ns`
0, `outlier_window` 50, `outlier_min_trades` 20, `outlier_bps` 500,
`max_clock_offset_ns` 250 ms, `max_clock_step_ns` 50 ms, `max_findings` 1000.

**What the checks cannot see:** a crossed book between snapshots (the
reference book checks every message); a steadily wrong recording clock,
except through the offset measured at session start; lost Kraken updates the
checksum misses; anything in a period not recorded.

## 11. Reference order book

`quantagent.data.book` rebuilds an instrument's L2 book from stored book
events and stream events. It is the oracle for the C++ book (Phase 4) and
favors being obviously correct over speed.

`python -m quantagent.data.book verify|golden --store DIR --venue V
--instrument I [--available-at NS] [--version NS] [--out FILE]` (default:
everything received, latest version; exit 2 if the store is refused).

- `messages_from_table(book_events, instruments)` groups rows into
  `BookMessage`s in `(log_id, seq)` order, refusing a message whose
  `level_index` is not 0 to n-1 or whose levels disagree on the header, and
  gives each the metadata in force for its record (exponents and precisions).
- `replay(venue, messages, events, verify=True)` yields the book after every
  stream message. A snapshot replaces the book; an update applies its levels in
  `level_index` order; quantity 0 removes a level; Kraken books are cut to
  `depth` per side after every message. Check snapshots are skipped.
- **It raises; it never skips, repairs or continues.** `DeltaBeforeSnapshot`,
  `ChecksumMismatch` (expected, computed), `SequenceBreak` (kind, expected,
  received), `StreamFault` (a rejected stream message), each naming the raw
  record and whether its own check (`computed`) or a stored event
  (`stream_event`) found it. A stored `SEQUENCE_*`, `CHECKSUM_MISMATCH` or
  `MESSAGE_REJECTED` on the stream raises; a `SESSION_START`, `CONNECT` or
  `DISCONNECT` means the next message must be a snapshot. Events on the probe
  connection do not concern the stream.
- **Verification, per venue.** Kraken: the checksum after every message,
  computed independently of the adapter, over the mantissas scaled to the
  precision in force; a Kraken message without metadata cannot be verified
  and raises a `ValueError`. Coinbase: book message numbers must increase;
  gaps come from the stored events. Always pass the instrument's
  `stream_events`: without them a lost Coinbase message is invisible.
- **Check snapshots** (`verify_check_snapshots`) are compared with the book of
  the stream connection current at the snapshot's record, within the
  snapshot's depth, under the rule of section 7 (Coinbase's between-batches
  exemption; Kraken exact on every level). `CheckResult.result` is `exact`,
  `explained`, `mismatch` or `not_compared`; `summary()` gives the facts of
  the adapter's event, and tests show the two agree.
- **Windows:** book rows and stream events read with one window cover the same
  raw records, since both are receipt time.
- **Golden files** (`python/tests/fixtures/golden/`): the top 20 levels per
  side after every stream message of each fixture, at the fixed exponents;
  their README gives the format.
- **What it cannot see:** a lost Kraken update that does not reach the ten
  best levels before it is overwritten, and a repeated Kraken update.

## 12. Known limits and open questions

- **One writer per store root**, assumed, not locked; two writers whose clocks
  step back could publish a knowledge time below one already issued.
- **Nothing is compacted.** Every reload adds a load; a store with many small
  files opens slowly (about 0.3 ms per file).
- **A range query resolves versions over everything the session can see**
  before filtering on the window, so its cost grows with the store, not the
  window (review, D4). The replay cursor removes only the cost of opening.
- **A venue's cancellation of a trade** has no representation (no
  retraction). A reload withdraws what our own fixed adapter no longer
  produces.
- **A trade first seen as backfill and later live** keeps the backfilled
  observation as its first.
- **`recv_ts` in recordings is when the recorder's coroutine resumed,** and
  probes share its event loop and link (review, D5).
- **The clock is measured at session start only;** the `clock_skew` band was
  calibrated on data with a 1.4 s clock error (review, D6).
- **Open for Phase 4 and 5** (review, D1 to D3): `MarketEvent` carries no
  control kinds (gap, reset, rejected) and no source log; the reference book
  cannot resume after a failure; one event per level meets "a full queue
  means stale and resync"; the C++ `InstrumentMeta` has no precisions, which
  Kraken's checksum needs.
- The Linux preset and the `__int128` path are unbuilt.

## Appendix A: where each ADR's decisions are now

| ADR | Decided | Here | Status |
|-----|---------|------|--------|
| 0005 | Integer mantissas with per-instrument exponents; int64 ns timestamps | 2 | Superseded by this document |
| 0006 | Raw log plus Parquet/DuckDB store; journal, experiment registry, audit log | 1, 3, 8, 9 (raw log and store) | Raw-log and store tiers superseded by this document; its other tiers stand |
| 0008 | `event_ts` holds `recv_ts` without a venue timestamp; `event_ts_source` | 2, 5 | Superseded |
| 0009 | Kraken as second venue; book events keyed by raw record, optional integrity columns; check snapshots | 5, 7 | Superseded |
| 0010 | `stream_events` | 6, 7 | Superseded |
| 0011 | The recording clock is synchronized, measured, refused above 250 ms | 4, 6, 10 | Superseded |
| 0012 | Replay order is `seq`; monotonic stamps; merging; clock steps | 2, 3, 6, 10 | Superseded |
| 0013 | `log_id`; keys and order by `(log_id, seq)` | 2, 3, 5 | Superseded |
| 0014 | Coinbase check snapshots judged by stream batch | 7, 10, 11 | Superseded |
| 0015 | Schema versions checked; conflicting trades are errors; uncompared sessions warn | 7, 8, 10 | Superseded |
| 0016 | Identical repeats dropped; first observation kept | 7, 10 | Superseded |
| 0017 | Two cutoffs, committed batches, receipt-time windows, replay cursor | 2, 8, 9 | Superseded |
| 0018 | Loads; one observation per trade shown; refused input; interleaved logs | 5, 7, 8, 9, 10 | Superseded |
| 0019 | Fixed exponents; metadata from a session's first row; limits per severity; every day a session spans; conflicts between logs; incomplete loads reported; this document | 2, 4, 7, 9, 10 | Restated here; implementation choices confirmed |

ADRs 0001 to 0004 and 0007 are not about the data core.
