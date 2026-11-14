# 0013: Stored rows name their raw log; keys and order use `(log_id, seq)`

Status: Accepted (owner decision), with implementation choices awaiting the owner's confirmation (see "Review")
Date: 2026-11-12
Amends: 0006, 0009, 0010, 0012

## Context

ADR 0012 made `seq` the replay order within a raw log and left one point open:
no stored row said which raw log it came from, and `seq` restarts at 0 in
every log. So:

- `PITSession` returned rows ordered by `(recv_ts, seq)`, and the
  reconstructor and the clock skew check used that order. After a backward
  wall-clock step it is not arrival order. The reconstructor refused such a
  session (`ReplayOrderUnreliable`) instead of replaying it.
- Book events and stream events were keyed by `(recv_ts, seq, ...)`. Two logs
  of the same venue and instrument with the same `recv_ts` and `seq` would
  collide, and one row would silently become a "correction" of the other.
  ADR 0012 said to run at most one recorder per instrument until this was
  settled.

The owner decided to add a `log_id` column to every stored dataset, key and
order stored data by `(log_id, seq)`, review every other key for dependence
on `recv_ts`, remove the reconstructor's fail-safe if it was no longer
needed, and rebuild the stores from the raw logs without rewriting the logs.

## Decision

- **Every stored dataset has a required string column `log_id`**, placed
  between `recv_ts` and `seq`. With `seq` it names the raw record a row came
  from.
- **A log's id comes from its first record** (`seq` 0), computed by
  `rawlog.log_id_of`:

  ```
  <UTC time of the first record, to the ns>Z-<first 12 hex digits of SHA-256>
  20261111T030919.259642800Z-2ca464c155f1
  ```

  The digest covers the first record as it is framed in the log: `recv_ts`,
  `recv_mono` (-1 if none), `seq`, venue, channel and payload. The id depends
  on nothing else, so it is the same whether the log is sealed or not, cut
  short, copied, renamed or read again. Ids sort by when their logs began.
  The schema checks the form on every write (`schema.LOG_ID_PATTERN`). Raw
  logs are not changed: the id is derived, never written into a log.
- **The normalizer takes the id from each log's first record.** A log always
  begins at `seq` 0, so a `seq` 0 record starts a new log when several logs
  are fed to one normalizer. A record that comes before any `seq` 0 record
  has no log and is rejected (`no_log_start`).
- **Keys** (within venue and instrument):

  | Dataset | Before | Now |
  |---------|--------|-----|
  | `book_events` | `recv_ts, seq, level_index` | `log_id, seq, level_index` |
  | `stream_events` | `recv_ts, seq, event_index` | `log_id, seq, event_index` |
  | `instruments` | `event_ts` | `log_id, seq` |
  | `trades` | `trade_id` | `trade_id` (unchanged) |

  The `instruments` key depended on `recv_ts` too: neither venue timestamps
  its metadata, so `event_ts` is the receipt time (ADR 0008). A metadata
  version is now identified by the raw record it was observed in, like every
  other row; `event_ts` still says when it takes effect.
  `trade_id` is the venue's own identity for a trade and does not involve
  `recv_ts`.
- **Order.** `PITSession`'s range methods return rows ordered by
  `(log_id, seq)`, then the other columns. `instruments` returns versions by
  `event_ts`, then `(log_id, seq)`. Logs of one instrument come one after
  another, in the order they began. They are not interleaved; merging logs by
  arrival is done on raw logs with `logorder.merge_logs` (ADR 0012).
- **The reconstructor replays in `(log_id, seq)` order**, each log from its
  own snapshot. Its check-snapshot comparison stays within the snapshot's own
  log. **`ReplayOrderUnreliable` is removed**: a backward clock step no longer
  changes the order of stored data. Clock steps are still recorded
  (`CLOCK_STEP`) and graded by the `recording_clock` quality check, because
  around a step `recv_ts` itself is wrong.
- **Quality findings name `(log_id, seq)`**, with `recv_ts` carried as
  information. "Venue time stepped back" compares consecutive messages within
  one log in `seq` order. Backfilled trades are matched to their
  `TRADE_BACKFILL` event by `(log_id, seq)`.
- **Golden order book files** gain `log_id` as their first column:
  `log_id, seq, recv_ts`, then the levels.
- Schema versions: `trades`, `book_events` and `instruments` 2 to 3;
  `stream_events` 1 to 2.

## Alternatives considered

- **The raw log's file name** (the session start, to the microsecond). It
  describes where a file is, not what it holds: renaming, copying or
  recompressing changes it, and logs from different venues that start
  together can share it (both committed fixtures are named
  `20261111T030647970222Z`).
- **A random id written into each log at start.** A raw log format change,
  and the existing logs have none. The owner ruled out rewriting raw logs.
- **A digest of the whole file.** It changes when a log is cut short or
  sealed later, and every row would wait for the whole file to be read.
- **A number assigned by the store.** Not reproducible: normalizing a log
  again would give it a new number, so its rows would be duplicates instead
  of corrections.
- **Also store `recv_mono_ns` on every row**, as ADR 0012's open point
  suggested. It would let the store interleave logs from the same machine and
  boot by arrival. It is not needed for keys or for order within a log, and
  the replay engine reads raw logs, which have it. It can be added later.
- **Keep `instruments` keyed by `event_ts`.** That would leave one key
  depending on the wall clock, which is what this decision removes.

## Consequences

- Two recorders of the same instrument can run at once: their rows no longer
  collide. ADR 0012's "one recorder per venue and instrument at a time"
  restriction is lifted.
- Normalizing a log again gives its rows the same ids, so they are
  corrections, as before.
- A backward wall-clock step no longer stops the reconstructor. It is still
  reported as an ERROR by the `recording_clock` check when it exceeds
  `max_clock_step_ns`.
- Stores written before this change cannot be read. They lack the column, and
  DuckDB refuses to bind the views. Rebuild them from the raw logs. On the
  owner's machine `data/normalized` was rebuilt from the two one-hour logs,
  with the same row counts as before and 6 of 6 check snapshots matching on
  each venue. The old store was kept as `data/normalized-before-adr0013`.
- Not changed, and worth knowing: `trades` is keyed by `trade_id`, so a trade
  seen in two records (the history a venue resends on a stream reconnect, or
  two overlapping logs) is one row. The store keeps the later knowledge time,
  and within one batch the row with the later `recv_ts`, which is usually the
  resent copy. No existing log contains such a repeat: each has one backfill,
  at its start.

## Review

Choices made during implementation, to be confirmed by the owner:

1. The id's form: start time to the nanosecond, then 12 hex digits (48 bits)
   of SHA-256 over the first record.
2. The `instruments` key change from `event_ts` to `(log_id, seq)`.
3. Rejecting records that come before any `seq` 0 record (`no_log_start`)
   instead of guessing their log.
4. Not storing `recv_mono_ns` (see "Alternatives considered").
