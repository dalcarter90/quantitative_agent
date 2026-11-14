# Data core

The Python data core (`python/src/quantagent/data/`) captures public market
data, normalizes it into an append-only bitemporal store, serves it only
through a point-in-time session, and checks its quality. This document
describes what is built. The decisions behind it are ADRs 0005, 0006, 0008,
0009 and 0010; the architecture's section 3.2 explains how it fits the rest
of the system.

```
venue ──WebSocket/REST──▶ recorder ──▶ raw log (.qaraw.zst)
                                           │
                                   adapters (normalize)
                                           │
                                           ▼
                             AppendOnlyStore (Parquet)
                                           │
                                 PITSession(as_of=...) ──▶ research, quality checks
```

| Module | Role |
|--------|------|
| `fixedpoint` | Exact decimal string ↔ int64 mantissa conversion |
| `schema` | The four datasets and their validation |
| `store` | `AppendOnlyStore`, the only writer |
| `session`, `sqlguard` | `PITSession`, the only reader, and its SQL allow-list |
| `rawlog` | The raw capture log |
| `clock` | Measures the recording clock against NTP servers |
| `recorder` | Records public market data into raw logs |
| `adapters` | Kraken and Coinbase: raw records to rows |
| `quality` | Data quality checks and the report command |
| `book` | The reference order book reconstructor (section 8) |

## 1. Representation

- **Prices and quantities** are int64 mantissas: `value = mantissa × 10^-exp`,
  with `price_exp` and `qty_exp` per instrument (ADR 0005). Nothing between
  the venue's text and the store passes through `float`; `fixedpoint` rejects
  any value with more precision than the exponent allows.
- **Timestamps** are int64 nanoseconds since the Unix epoch, UTC.
- Every row carries three times:

| Column | Meaning | Set by |
|--------|---------|--------|
| `event_ts` | When the venue says it happened. If the venue's message has no timestamp, the receipt time (ADR 0008) | The adapter |
| `event_ts_source` | `"venue"` or `"receipt"`: where `event_ts` came from, decided by the kind of message | The adapter |
| `recv_ts` | When our process received the bytes (wall clock) | The recorder |
| `recv_mono_ns` | The monotonic counter at the same moment; raw log records only, not stored rows (ADR 0012) | The recorder |
| `knowledge_ts` | When the row became available in the store | The store, at write time |

`recv_ts` and `knowledge_ts` are only as good as the recording machine's
clock. See section 7.

## 2. Datasets

All four share leading columns `event_ts`, `event_ts_source`, `recv_ts`,
`log_id` (the raw log the row came from), `seq` (the record number in that
log), and `knowledge_ts` in storage. Venue, instrument and date are not
columns in the files; they are the partition directories, and they appear as
columns in the session's views.

**`log_id`** (ADR 0013) is derived from a log's first record:
`<UTC time of that record, to the ns>Z-<12 hex digits of its SHA-256>`, for
example `20261111T030919.259642800Z-2ca464c155f1` (`rawlog.log_id_of`). It is
the same however the log is read (sealed or not, cut short, copied, renamed),
and ids sort by when their logs began. `(log_id, seq)` names one raw record.
No key uses `recv_ts`, which is a measurement (ADR 0012).

| Dataset | One row per | Key (within venue and instrument) | Other columns |
|---------|-------------|-----------------------------------|---------------|
| `trades` | Trade | `trade_id` | `price`, `qty`, `side` (the aggressor: `BUY`, `SELL` or `UNKNOWN`) |
| `book_events` | Price level of a book message | `log_id, seq, level_index` | `kind` (`SNAPSHOT`, `DELTA`, `CHECK_SNAPSHOT`), `side`, `price`, `qty`, `level_index`, and the venue's integrity fields `venue_seq_first`, `venue_seq_last`, `checksum`, `depth` (each optional; ADR 0009) |
| `instruments` | Version of an instrument's metadata, as observed in one raw record | `log_id, seq` (`event_ts` says when the version takes effect) | `price_exp`, `qty_exp`, `tick_size`, `lot_size` |
| `stream_events` | Integrity event (ADR 0010) | `log_id, seq, event_index` | `kind`, `connection`, `expected`, `observed`, `detail` |

Book rows of one message share `log_id` and `seq` and are numbered by
`level_index` in the order the venue listed them, which matters because a
venue may change one price twice in a message. A `DELTA` with quantity 0
removes the level. A `CHECK_SNAPSHOT` is an independent image of the book
taken on a second connection; it is not part of the update stream.

`stream_events` kinds: `SESSION_START`, `SESSION_STOP`, `CONNECT`,
`DISCONNECT`, `SUBSCRIBE_REQUEST`, `SUBSCRIPTION_ACK`, `TRADE_BACKFILL`,
`CLOCK_OFFSET`, `CLOCK_OVERRIDE` (ADR 0011), `CLOCK_STEP` (ADR 0012),
`SEQUENCE_GAP`, `SEQUENCE_REPEAT`, `SEQUENCE_OUT_OF_ORDER`,
`CHECKSUM_MISMATCH`, `MESSAGE_REJECTED`, `CHECK_SNAPSHOT_COMPARED` (ADR 0014),
`TRADE_CONFLICT` (ADR 0015), `TRADE_REPEAT` (ADR 0016). ADR 0010 lists what each means and what
`expected`, `observed` and `detail` hold for it; ADR 0011 the two clock kinds;
ADR 0014 the check-snapshot comparison, which the Coinbase adapter writes once
per check snapshot; ADR 0015 a trade seen again with a different price,
quantity or side; ADR 0016 one seen again unchanged.

The schema rejects structural errors only: wrong types, nulls in required
columns, unknown enum values, negative timestamps, invalid metadata, and a
`"receipt"` row whose `event_ts` differs from its `recv_ts`, and a `log_id`
not of the form above. Implausible
market values (a zero price) are stored as received and left to the quality
checks. Build tables with `schema.table_from_rows`, which refuses floats,
bools and numeric strings that pyarrow would silently convert.

## 3. Store

```
<root>/<dataset>/venue=<venue>/instrument=<instrument>/date=<YYYY-MM-DD>/
    part-<knowledge_ts, 20 digits>.parquet
```

- `date` is the UTC date of `event_ts`.
- `AppendOnlyStore(root).append(dataset, venue, instrument, table)` is the only
  write operation. One call is one batch with one `knowledge_ts`, strictly
  greater than any the store has issued, including in earlier processes.
- Existing files are never opened for writing, renamed or removed. Files are
  written under a temporary name and published by hard link, which cannot
  replace a file; a failed multi-partition append publishes nothing.
- A **correction** is a new row with the same key and a later `knowledge_ts`.
  Because book events, stream events and instrument versions are keyed by the
  raw record they came from, and a log keeps its `log_id`, normalizing a raw
  log again produces corrections, not duplicates. Two logs never collide, so
  several recorders of one instrument can run at once (ADR 0013).
- **Order.** The range methods return rows in `(log_id, seq)` order: one log
  after another, in the order they began, each in the order its records
  arrived. Logs are not interleaved in the store; merging logs by arrival is
  done on raw logs (`logorder.merge_logs`, section 5).
- **Schema versions** are written into every file (`qa.dataset`,
  `qa.schema_version`). `PITSession` checks every file it opens and refuses
  one of another dataset, version or set of columns with a
  `StoreVersionError` saying to rebuild the store from the raw logs (ADR
  0015). A store written before ADR 0013 is refused this way. To rebuild,
  load each raw log into a new directory with
  `python -m quantagent.data.adapters <log> --store <new store>`.
- Limits: one writer per store root (assumed, not locked); hard links need
  NTFS or ext4; venue and instrument names are restricted to
  `A-Z a-z 0-9 . _ -`.

## 4. `PITSession`

```python
with PITSession(store_root, as_of=t) as s:
    s.trades(venue, instrument, start, end)         # start <= event_ts < end
    s.book_events(venue, instrument, start, end)
    s.stream_events(venue, instrument, start, end)
    s.instruments(venue, instrument)                # every metadata version known at t
    s.instrument(venue, instrument, at=u)           # the version in effect at u (default t)
    s.sql("SELECT ... FROM trades JOIN ...")        # one SELECT over the four views
```

Every method returns the latest version of each record among the rows with
`knowledge_ts <= as_of`, as a pyarrow table (or an `InstrumentMeta`). Range
methods return rows in `(log_id, seq)` order (section 3); `instruments` by
`event_ts`, then `(log_id, seq)`. The views are `trades`, `book_events`,
`instruments` and `stream_events`, each with `venue`, `instrument`, `date` and
then the stored columns.

Opening a session reads the footer of every file it will open and refuses the
store if any was written with another schema (section 3); that costs about
0.3 ms per file.

Look-ahead is blocked in three independent layers:

1. **Views** filtered to `knowledge_ts <= as_of`. Nothing else is queryable.
2. **Statement validation** (`sqlguard`): `sql` accepts exactly one SELECT.
   DuckDB parses the query, the parse tree is checked against an allow-list of
   node kinds, tables, functions and types, and the session executes that same
   tree. Refused: DDL and DML, multiple statements, table functions and file
   paths, catalog and qualified names, `WITH`, parameters, sampling, functions
   that read settings, the clock or random state, and bare `current_date` and
   similar names.
3. **Engine lockdown**: each session has its own DuckDB instance that may open
   only the files written at or before `as_of`, cannot write, attach or load
   anything, and cannot be reconfigured.

Other properties: `as_of` is a required keyword and cannot be changed; a
session is a snapshot of the files present when it opened; it cannot be
copied, pickled or subclassed; errors have store paths removed; it is not
thread-safe. DuckDB is pinned to 2.0.x because the validator depends on its
parse tree (CLAUDE.md has the upgrade rule).

## 5. Capture and adapters

### Raw log

`rawlog.RawLogWriter` appends records of `recv_ts`, `recv_mono_ns`, `seq`,
venue, channel and payload bytes, each with a CRC32, to `<name>.qaraw`;
`seal()` compresses it to `<name>.qaraw.zst`. `read_raw_log` reads either. A
partial final record in an unsealed log is dropped and reported; any other
damage raises.

**Order (ADR 0012).** Within one log, records are replayed in `seq` order;
`recv_ts` is a measurement, not an ordering key. `recv_mono_ns` is the
system's monotonic high-resolution counter (`time.perf_counter_ns`) read in
the same call as `recv_ts`. `logorder.merge_logs(paths)` merges logs recorded
on the same machine in the same boot by it, and refuses logs whose counters
are not comparable or that have no monotonic stamps.

**Versions.** The writer writes version 2. Version 1 logs (everything recorded
before ADR 0012: the Phase 1c one-hour recordings and the committed fixtures)
have no monotonic stamp. They are read with `recv_mono_ns` None and are never
rewritten; a single version 1 log is still replayed in `seq` order, but it
cannot be merged with another log, and clock steps during it cannot be
detected.

### Recorder

```
python -m quantagent.data.recorder --venue kraken --instrument BTC-USD \
    --out data/raw --duration 3600 --probe-interval 600 --depth 25 \
    [--allow-clock-offset]
```

Public market data only; no API keys are read or sent. The start record
names the clock identity (platform, machine id, boot id, counter) that says
which logs' monotonic stamps are comparable. Before contacting the
venue the recorder measures its clock (section 7) and refuses to start, with
exit status 2, if the offset is above 250 ms or cannot be measured, unless
`--allow-clock-offset` is given. Raw log channels:
`ws` (the stream), `probe` (a fresh book snapshot from a short second
connection every probe interval), `rest:product` (Coinbase metadata) and
`recorder` (JSON control records: start, clock, connect, subscribe,
disconnect, rest, stop).

### Adapters

`adapters.normalize(records)`, the streaming `Normalizer`, and
`adapters.ingest(path, store)`, which appends in batches of pending rows.
`python -m quantagent.data.adapters <log>` checks a log without writing; with
`--store <dir>` it loads it. Every raw record ends as `data`, `ignored` (a
known kind of message with no market data) or `rejected` (with a reason), and
a message is all or nothing.

| | Kraken (WebSocket v2) | Coinbase (Advanced Trade) |
|---|---|---|
| Channels | `instrument`, `book` (depth from the subscription), `trade` | `level2`, `market_trades`, `heartbeats`; product metadata over REST |
| Book integrity | CRC32 of the 10 best levels per side after every message; no sequence numbers | `sequence_num` on every message of a connection, across all channels |
| Verified by the adapter | Checksum, against the top of a book it keeps per connection; a mismatch is a `CHECKSUM_MISMATCH` event | Continuity; a break is a `SEQUENCE_*` event |
| Trade `side` | The taker's | The maker's, so the adapter stores the opposite |
| Without a venue timestamp | Instrument metadata | Product metadata |
| Symbol | `BTC/USD` stored as `BTC-USD` | `BTC-USD` |

Kraken's checksum covers only ten levels per side and is checked after each
message, so it misses some lost updates: on the recorded fixture, of 250
single updates removed one at a time, 64% were caught and 36% never were.

**A trade seen again** (ADRs 0015 and 0016). A venue resends history after a
reconnect, and Coinbase sometimes sends a trade again in a later message of
its live stream (0.07% of trades in the 30-minute 2026-11-13 recording, always
identical). The normalizer compares each trade with the first observation of
its `trade_id`, across every log fed to it. The store keeps one row per
trade.
- **An identical repeat** (same price, quantity and side) is dropped, so the
  stored row is the first observation, with its `seq` and `recv_ts`. The
  normalizer writes a `TRADE_REPEAT` event at the repeat's record: `observed`
  is the delay after the first observation in ns, and `detail` names the
  `trade_id` and the first observation's `log_id` and `seq`.
- **A repeat that differs** is stored, so the store shows the later value. The
  normalizer writes a `TRADE_CONFLICT` event at the later record, naming the
  fields and both observations.

The report counts trades seen again, conflicts and identical repeats dropped.
The raw log keeps every observation. Logs loaded in separate runs are not
compared with each other; for those the store keeps the later observation.

## 6. Quality checks

```
python -m quantagent.data.quality --store data/normalized \
    --start 2026-11-11 --end 2026-11-11 [--venue V] [--instrument I] \
    [--as-of NS] [--fail-on error|warning|info] [--config quality.json]
```

The report runs every check on every venue and instrument with data in the
date range (UTC, both days inclusive), as known at `--as-of` (default: now),
and prints each check's findings with what it examined. It exits 1 if any
finding is at or above `--fail-on` (default `error`). A range with no data is
reported as a `no_data` warning, so a recorder that stopped does not pass
silently.

Each check is a function `check(session, venue, instrument, start, end,
config)` returning a `CheckResult` with `Finding`s (check, severity, a stable
code, the raw record it points at as `log_id` and `seq` with its `recv_ts`, a
message) and counts of what it examined.
Checks read only through `PITSession` and never change data. Findings per
check and instrument are capped at `max_findings`, and the result says when
the cap was hit.

| Check | Finds | Severity |
|-------|-------|----------|
| `recording_clock` | A session whose clock offset was above `max_clock_offset_ns` (`clock_offset_exceeded`) | ERROR |
| | A session whose clock could not be measured (`clock_unmeasured`) | WARNING |
| | A wall-clock step against the monotonic counter beyond `max_clock_step_ns` (`clock_step`), with its size and direction (ADR 0012) | ERROR |
| | A session recorded before clocks were measured (`clock_not_recorded`); a measured session without monotonic stamps (`clock_steps_not_measurable`); any use of `--allow-clock-offset` (`clock_override`) | INFO |
| `stream_integrity` | From `stream_events`: `sequence_gap`, `checksum_mismatch`, `message_rejected`, and `trade_conflict` (a trade seen again with a different price, quantity or side; ADR 0015) | ERROR |
| | `sequence_repeat`, `sequence_out_of_order`; the stream connection dropping (`disconnect`) or reopening (`reconnect`) | WARNING |
| | A probe connection that did not deliver its snapshot (`probe_failed`); trades sent again unchanged and dropped (`trades_repeated`, ADR 0016), one finding per session with the count and the range of delays | INFO |
| `independent_snapshots` | From `CHECK_SNAPSHOT_COMPARED` events (ADR 0014): a check snapshot that differs from the rebuilt book on a level the stream fully determines (`check_snapshot_mismatch`), with the count and the shallowest and deepest rank | ERROR |
| | A session (one raw log) none of whose check snapshots in the range could be compared (`no_check_snapshot_compared`, ADR 0015), with how many there were and why | WARNING |
| | A Coinbase check snapshot taken between two stream batches that differs only on the next batch's levels (`check_snapshot_between_batches`); a check snapshot that could not be compared (`check_snapshot_not_compared`), with the reason | INFO |
| `clock_skew` | `recv_ts - event_ts` below `min_lag_ns` (`early`, including venue time later than receipt) or above `max_lag_ns` (`late`); venue time stepping back between consecutive messages of one log (in `seq` order) by more than `max_backwards_ns` (`event_ts_backwards`). One finding per message. Only rows with `event_ts_source = "venue"`; backfilled trades (marked by `TRADE_BACKFILL`) are excluded, because their receipt time says nothing about when they happened | WARNING |
| `crossed_books` | A snapshot (stream or check) whose best bid is at (`locked`) or above (`crossed`) its best ask | ERROR |
| | A snapshot with only one side (`one_sided`) | WARNING |
| `outliers` | Zero or negative trade price or quantity, zero or negative book price, negative book quantity | ERROR |
| | A trade more than `outlier_bps` from the median of the previous `outlier_window` trades (`price_outlier`), once at least `outlier_min_trades` precede it; a snapshot level with quantity 0 | WARNING |

Thresholds (`QualityConfig`, loadable from a JSON file whose unknown names are
an error):

| Name | Default |
|------|---------|
| `min_lag_ns`, `max_lag_ns` | −0.5 s, 10 s |
| `max_backwards_ns` | 0 |
| `outlier_window`, `outlier_min_trades`, `outlier_bps` | 50, 20, 500 |
| `max_clock_offset_ns` | 250 ms |
| `max_clock_step_ns` | 50 ms |
| `max_findings` | 1000 |

What the checks cannot see:

- **A crossed book between snapshots.** No top-of-book quotes are stored, and
  an update alone cannot be judged. Checking the book after every update
  needs it rebuilt: the Phase 2 reconstructor does that.
- **A wrong recording clock, from the data alone.** A clock that is steadily
  fast or slow shifts every lag by the same amount and stays inside the
  `clock_skew` band. `recording_clock` catches it only through the offset the
  recorder measured (section 7), and only at session start.
- **Lost Kraken updates the checksum misses** (section 5), and anything in a
  period that was not recorded.

## 7. Clock accuracy

`recv_ts` and `knowledge_ts` come from the recording machine's wall clock, so
they assume that clock is synchronized to NTP (ADR 0011).

**What the recorder does.** Before contacting a venue it measures its offset
(`clock.measure_offset`): three SNTP samples from each of four servers run by
independent operators, the fastest reply per server, the median across
servers, and at least two servers must answer. The offset is this clock minus
NTP time, so positive means the clock is ahead and timestamps are late. Above
250 ms, or if it cannot be measured, the recorder refuses to start unless run
with `--allow-clock-offset`. Either way the measurement is written into the
raw log right after the session start and becomes a `CLOCK_OFFSET` stream
event, and any use of the override a `CLOCK_OVERRIDE` event. The
`recording_clock` quality check reports both.

**Known offsets in existing data.** The Phase 1c recordings (the one-hour
logs of 2026-11-11 03:09 to 04:10 UTC, and the committed 150-second fixtures
of 03:06 to 03:09 UTC) were made before this check existed, on a machine with
its Windows Time service stopped. Their clock was **about 1.4 s ahead**:

- no Coinbase message arrived sooner than 1,398 ms after Coinbase's own send
  timestamp, over routes with 20 to 50 ms round trips;
- when the time service was started, at 2026-11-12 00:13:43 UTC, the system
  event log shows the clock stepped back by 1.241 s (it had drifted in
  between).

So their `recv_ts` and `knowledge_ts` are about 1.4 s late, and the ~1.48 s
median lag the quality report shows for them is mostly this offset; the true
network and processing delay was roughly 40 to 90 ms. There is no look-ahead
(data appears known later than it was), but lag and latency figures from this
data are wrong by the offset. The timestamps are left as recorded, and these
sessions report `clock_not_recorded`.

## 8. Reference order book

`quantagent.data.book` rebuilds an instrument's L2 book from its stored book
events and stream events, and is the oracle the C++ order book is checked
against (roadmap Phase 2). It favors being obviously correct over speed.

```
python -m quantagent.data.book verify --store data/normalized --venue kraken --instrument BTC-USD
python -m quantagent.data.book golden --store ... --venue ... --instrument ... --out FILE
```

- Snapshots replace the book; updates apply level by level in `level_index`
  order, and quantity 0 removes a level. Kraken books are cut to the
  subscribed depth after every message.
- **Integrity, per venue.** Kraken: the reconstructor computes the CRC32 of
  the ten best levels per side after every message and compares it with the
  venue's. Coinbase: book message numbers must increase, and gaps come from
  the stored `SEQUENCE_*` events (heartbeats and trades share the
  numbering, so gaps are invisible in book messages alone). For both venues
  a stored `CHECKSUM_MISMATCH`, `SEQUENCE_*` or `MESSAGE_REJECTED` event on
  the stream raises, and a new session or connection means the next message
  must be a snapshot.
- **It raises; it never repairs.** `DeltaBeforeSnapshot`, `ChecksumMismatch`,
  `SequenceBreak` and `StreamFault` carry the raw record (`log_id`, `seq`,
  with its `recv_ts`) and the values involved.
- **Check snapshots** (ADR 0014) are compared with the book rebuilt from the
  stream connection that was current at the snapshot's record, level by level
  within the snapshot's depth. A is the last message of that connection whose
  venue time is at or before the snapshot's time T, and B the message after
  it. The snapshot must equal the book after A on every level, except on
  Coinbase when T falls strictly between A's and B's times: then B's levels
  are reported as `unverified`, because Coinbase sends its stream in batches
  about 50 ms apart and a snapshot taken between them already shows part of
  B. `CheckResult.result` is `exact`, `explained` (only B's levels differ),
  `mismatch` or `not_compared` (with the reason), and `summary()` gives the
  same facts as the Coinbase adapter's `CHECK_SNAPSHOT_COMPARED` event. Tests
  show the two agree. On the recordings so far, every snapshot timed on a
  batch was exact, and every one timed between batches differed only on B's
  levels.
- **Order.** Stored rows are replayed in `(log_id, seq)` order: each log in
  the order its records arrived, from its own snapshot, whatever the wall
  clock did (ADR 0013). A clock step is reported by the `recording_clock`
  check, not by the reconstructor.
- **What it cannot see:** a lost Kraken update that does not touch the ten
  best levels before it is overwritten, and a repeated Kraken update (the
  quantities are absolute, so the book does not change).

The golden files in `python/tests/fixtures/golden/` hold the top 20 levels
per side after every stream message of each fixture; their README gives the
format.
