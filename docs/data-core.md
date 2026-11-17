# Data core: guide

How to record, load, read and check market data with the data core
(`python/src/quantagent/data/`). What everything means (times, keys, the
store, the session's rules, every stream event, every check) is in
**[the data core specification](data-core-spec.md)**, which is the reference;
this page is the practical side. Section numbers below are this page's;
"spec N" is a section of the specification.

```
venue ──WebSocket/REST──▶ recorder ──▶ raw log (.qaraw.zst)
                                           │
                                   adapters (normalize, load)
                                           │
                                           ▼
                             AppendOnlyStore (Parquet, loads of batches)
                                           │
                          PITSession(available_at=...) ──▶ research, quality checks, book
```

| Module | Role |
|--------|------|
| `fixedpoint` | Exact decimal text to int64 mantissas and back |
| `schema` | The four datasets and their validation |
| `store` | `AppendOnlyStore`, the only writer: loads of raw logs |
| `session`, `sqlguard` | `PITSession` and `ReplayCursor`, the only reader, and its SQL allow-list |
| `rawlog`, `logorder` | Raw capture logs, and merging logs by monotonic stamp |
| `clock` | Measures the recording clock against NTP servers |
| `recorder` | Records public market data into raw logs |
| `adapters` | Kraken and Coinbase: raw records to rows; `FIXED_EXPONENTS` |
| `quality` | Data quality checks and the report command |
| `book` | The reference order book reconstructor |

## 1. Recording

```
python -m quantagent.data.recorder --venue kraken --instrument BTC-USD \
    --out data/raw --duration 3600 --probe-interval 600 --depth 25 [--allow-clock-offset]
```

- Public data only; no keys. Writes `data/raw/<venue>/<instrument>/<start>.qaraw.zst`.
- It measures the machine's clock first and refuses to start (exit 2, nothing
  written) if it is more than 250 ms off NTP time or cannot be measured.
  On Windows, start the Windows Time service and run `w32tm /resync`; the
  service has been found stopped on the recording machine more than once
  (section 7). `--allow-clock-offset` records anyway and says so in the log.
- An instrument must have fixed exponents in `adapters.FIXED_EXPONENTS`
  (spec 2) before its logs can be loaded. BTC-USD has them on both venues.
  A raw log of another instrument can be recorded and kept, and loaded once
  its exponents are chosen and added.
- Network tests (a real capture and NTP measurement): `pytest python -m network`.

## 2. Checking a raw log

```
python -m quantagent.data.adapters data/raw/kraken/BTC-USD/<file>.qaraw.zst
```

Normalizes without writing and prints the report: records by outcome, rows per
dataset, rejects by reason, sequence anomalies, checksum mismatches and trade
repeats. Exit 1 on any reject, sequence anomaly or partial final record.

## 3. Loading, and rebuilding a store

```
python -m quantagent.data.adapters <raw log> --store data/normalized
```

Loads the log as one load (spec 8). Loading the same log again, after an
adapter fix, replaces what the earlier load said about it; the earlier load
stays on disk.

**`StoreVersionError`** means the store was written by an earlier version of
the code (another store layout, or another schema version: before loads,
before commit markers, before `log_id`, or with `instruments` older than
version 4). It cannot be read or added to. Rebuild it: load every raw log
into a **new** directory with the command above, then compare (row counts,
check snapshots, the quality report) with the old one before removing it.

## 4. Reading

```python
from quantagent.data.session import PITSession, ReplayCursor

with PITSession("data/normalized", available_at=t) as s:   # t: simulated time, ns
    trades = s.trades("kraken", "BTC-USD", start, end)      # receipt-time window
    meta = s.instrument("kraken", "BTC-USD")                # price_exp, qty_exp, ...
    s.sql("SELECT count(*) FROM book_events WHERE kind = 0")
```

Every result is a pyarrow table of integer mantissas; divide by
`10 ** price_exp` (from the metadata) explicitly to get a float. A backtest
harness opens one `ReplayCursor` and hands its `session` to the strategy
(spec 9).

## 5. Quality report

```
python -m quantagent.data.quality --store data/normalized \
    --start 2026-11-11 --end 2026-11-11 [--venue V] [--instrument I] \
    [--available-at NS] [--version NS] [--fail-on error|warning|info] [--config quality.json]
```

Exit 0 clean, 1 on a finding at or above `--fail-on` or on any finding cut by
`max_findings` (raise it in the config file), 2 if the store is refused. The
checks and their thresholds are spec 10. An `incomplete_load` WARNING means a
log is read from a load that has not completed: it is still being written, or
its loader failed or died; finish it, or load the log again with `ingest`.

## 6. The reference book

```
python -m quantagent.data.book verify --store data/normalized --venue kraken --instrument BTC-USD
python -m quantagent.data.book golden --store ... --venue ... --instrument ... --out FILE
```

`verify` replays every message under the venue's integrity rule and compares
every check snapshot (spec 11); it exits 1 on an integrity failure, a
mismatched snapshot or a crossed book. The golden files and how to regenerate
them are in `python/tests/fixtures/golden/README.md`.

## 7. Clock accuracy and known data

`recv_ts` comes from the recording machine's wall clock, so it assumes that
clock was synchronized to NTP (spec 4). `knowledge_ts` comes from the loading
machine's clock; it orders versions and is never compared with market times.

**Known offsets in existing data.** The Phase 1c recordings (the one-hour logs
of 2026-11-11 03:09 to 04:10 UTC, and the committed 150-second fixtures of
03:06 to 03:09 UTC) were made before the recorder measured its clock, on a
machine with its Windows Time service stopped. Their clock was **about 1.4 s
ahead**:

- no Coinbase message arrived sooner than 1,398 ms after Coinbase's own send
  timestamp, over routes with 20 to 50 ms round trips;
- when the time service was started, at 2026-11-12 00:13:43 UTC, the system
  event log shows the clock stepped back by 1.241 s (it had drifted in
  between).

So their `recv_ts` is about 1.4 s late, and the ~1.48 s median lag the quality
report shows for them is mostly this offset; the true network and processing
delay was roughly 40 to 90 ms. There is no look-ahead (data appears received
later than it was, so a session sees it later), but lag and latency figures
from this data are wrong by the offset. The timestamps are left as recorded,
and these sessions report `clock_not_recorded`. They are raw log version 1
(no monotonic stamps) and recorder version 1.

**Recordings on the owner's machine** (not in git): the two one-hour logs in
`data/raw/`, the 60-second ADR 0012 recordings in `data/raw-adr0012/`, the
30-minute Coinbase recording with a check snapshot every 60 s in
`data/raw-probes/`. `data/normalized` is built from the two one-hour logs; the
roadmap records each rebuild and its comparison.
