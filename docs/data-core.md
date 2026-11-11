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
| `recorder` | Records public market data into raw logs |
| `adapters` | Kraken and Coinbase: raw records to rows |
| `quality` | Data quality checks and the report command |

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
| `knowledge_ts` | When the row became available in the store | The store, at write time |

`recv_ts` and `knowledge_ts` are only as good as the recording machine's
clock. See section 7.

## 2. Datasets

All four share leading columns `event_ts`, `event_ts_source`, `recv_ts`,
`seq` (the raw log record number), and `knowledge_ts` in storage. Venue,
instrument and date are not columns in the files; they are the partition
directories, and they appear as columns in the session's views.

| Dataset | One row per | Key (within venue and instrument) | Other columns |
|---------|-------------|-----------------------------------|---------------|
| `trades` | Trade | `trade_id` | `price`, `qty`, `side` (the aggressor: `BUY`, `SELL` or `UNKNOWN`) |
| `book_events` | Price level of a book message | `recv_ts, seq, level_index` | `kind` (`SNAPSHOT`, `DELTA`, `CHECK_SNAPSHOT`), `side`, `price`, `qty`, `level_index`, and the venue's integrity fields `venue_seq_first`, `venue_seq_last`, `checksum`, `depth` (each optional; ADR 0009) |
| `instruments` | Version of an instrument's metadata | `event_ts` (when the version takes effect) | `price_exp`, `qty_exp`, `tick_size`, `lot_size` |
| `stream_events` | Integrity event (ADR 0010) | `recv_ts, seq, event_index` | `kind`, `connection`, `expected`, `observed`, `detail` |

Book rows of one message share `recv_ts` and `seq` and are numbered by
`level_index` in the order the venue listed them, which matters because a
venue may change one price twice in a message. A `DELTA` with quantity 0
removes the level. A `CHECK_SNAPSHOT` is an independent image of the book
taken on a second connection; it is not part of the update stream.

`stream_events` kinds: `SESSION_START`, `SESSION_STOP`, `CONNECT`,
`DISCONNECT`, `SUBSCRIBE_REQUEST`, `SUBSCRIPTION_ACK`, `TRADE_BACKFILL`,
`SEQUENCE_GAP`, `SEQUENCE_REPEAT`, `SEQUENCE_OUT_OF_ORDER`,
`CHECKSUM_MISMATCH`, `MESSAGE_REJECTED`. ADR 0010 lists what each means and
what `expected`, `observed` and `detail` hold for it.

The schema rejects structural errors only: wrong types, nulls in required
columns, unknown enum values, negative timestamps, invalid metadata, and a
`"receipt"` row whose `event_ts` differs from its `recv_ts`. Implausible
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
  Because book events and stream events are keyed by the raw record they came
  from, normalizing a raw log again produces corrections, not duplicates.
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
`knowledge_ts <= as_of`, as a pyarrow table (or an `InstrumentMeta`). The
views are `trades`, `book_events`, `instruments` and `stream_events`, each
with `venue`, `instrument`, `date` and then the stored columns.

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

`rawlog.RawLogWriter` appends records of `recv_ts`, `seq`, venue, channel and
payload bytes, each with a CRC32, to `<name>.qaraw`; `seal()` compresses it to
`<name>.qaraw.zst`. `read_raw_log` reads either. A partial final record in an
unsealed log is dropped and reported; any other damage raises.

### Recorder

```
python -m quantagent.data.recorder --venue kraken --instrument BTC-USD \
    --out data/raw --duration 3600 --probe-interval 600 --depth 25
```

Public market data only; no API keys are read or sent. Raw log channels:
`ws` (the stream), `probe` (a fresh book snapshot from a short second
connection every probe interval), `rest:product` (Coinbase metadata) and
`recorder` (JSON control records: start, connect, subscribe, disconnect,
rest, stop).

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
code, the raw record it points at, a message) and counts of what it examined.
Checks read only through `PITSession` and never change data. Findings per
check and instrument are capped at `max_findings`, and the result says when
the cap was hit.

| Check | Finds | Severity |
|-------|-------|----------|
| `stream_integrity` | From `stream_events`: `sequence_gap`, `checksum_mismatch`, `message_rejected` | ERROR |
| | `sequence_repeat`, `sequence_out_of_order`; the stream connection dropping (`disconnect`) or reopening (`reconnect`) | WARNING |
| | A probe connection that did not deliver its snapshot (`probe_failed`) | INFO |
| `clock_skew` | `recv_ts - event_ts` below `min_lag_ns` (`early`, including venue time later than receipt) or above `max_lag_ns` (`late`); venue time stepping back between consecutive messages by more than `max_backwards_ns` (`event_ts_backwards`). One finding per message. Only rows with `event_ts_source = "venue"`; backfilled trades (marked by `TRADE_BACKFILL`) are excluded, because their receipt time says nothing about when they happened | WARNING |
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
| `max_findings` | 1000 |

What the checks cannot see:

- **A crossed book between snapshots.** No top-of-book quotes are stored, and
  an update alone cannot be judged. Checking the book after every update
  needs it rebuilt: the Phase 2 reconstructor does that.
- **A wrong recording clock.** A clock that is steadily fast or slow shifts
  every lag by the same amount and stays inside the band (section 7).
- **Lost Kraken updates the checksum misses** (section 5), and anything in a
  period that was not recorded.

## 7. Clock accuracy

`recv_ts` and `knowledge_ts` come from the recording machine's wall clock.
The machine that recorded the Phase 1c data had its Windows Time service
stopped, and its clock ran about 1.4 s ahead during the recordings
(measured from Coinbase's per-message send times: no message arrived sooner
than 1,398 ms after it was sent, over routes with 20 to 50 ms round trips)
and about 1.2 s ahead a day later (measured against four NTP servers). The
median lags in the quality report, about 1.48 s, are mostly this offset.

Effects on the stored data:

- No look-ahead: a fast clock makes data appear known *later* than it was.
- Latency and lag statistics are wrong by the offset, and the offset drifts.
- `recv_ts` from different machines, or from this machine at different
  times, cannot be compared to better than the offset.

Recording should happen on a machine whose clock is synchronized; the roadmap
records the open decision on how to enforce that.
