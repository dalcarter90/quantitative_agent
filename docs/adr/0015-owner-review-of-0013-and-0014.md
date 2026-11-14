# 0015: Schema versions are checked, conflicting trades are errors, uncompared sessions are warnings

Status: Accepted (owner decision, 2026-11-13)
Date: 2026-11-13
Amends: 0006, 0010, 0013, 0014

## Context

ADRs 0013 and 0014 listed choices made during implementation for the owner
to confirm. Phase 3a's notes also raised two questions: stores did not check
schema versions, and a trade seen twice was silently one row. The owner
confirmed every listed choice and decided three changes, recorded here.

## Decision

1. **A session where no check snapshot could be compared is a WARNING.** A
   single check snapshot that could not be compared stays INFO (ADR 0014). But
   a session (one raw log) none of whose check snapshots could be compared is
   reported once as `no_check_snapshot_compared`, a WARNING: that session's
   book was never checked against the venue's. The finding says how many
   snapshots there were and why each failed. It is judged per session, over
   the events in the range being checked, by the `independent_snapshots`
   quality check. Its statistics gain `sessions` and
   `sessions_without_comparison`.
2. **`PITSession` checks the schema of every file it opens.** Before creating
   any view, it reads each visible file's Parquet footer and requires three
   things:
   - `qa.dataset` names the dataset whose directory the file is in;
   - `qa.schema_version` is the version this code reads;
   - the columns are that version's.

   Otherwise it raises `StoreVersionError` (a `ValueError`). The message
   names the file within the store, says what it holds and what the code
   reads, and says to rebuild the store from the raw logs. Files written after
   `as_of` are not opened, so they are not checked. `python -m
   quantagent.data.quality` and `python -m quantagent.data.book` print the
   refusal and exit with status 2.
3. **A conflicting repeat of a trade is an ERROR.** A trade seen twice stays
   one row: `trades` is keyed by the venue's `trade_id` (ADR 0013). The
   normalizer compares each trade with the first observation of its
   (venue, instrument, `trade_id`), across every log fed to it. If the price,
   quantity or side differ, it writes a `TRADE_CONFLICT` stream event (kind
   16) at the later record. Its detail holds the `trade_id`, the fields that
   differ, and both observations (the first with its `log_id` and `seq`). The
   `stream_integrity` quality check reports it as `trade_conflict`, an ERROR.
   Other fields, such as the trade's time, are not compared. The
   normalization report counts trades seen again and conflicts.

## Alternatives considered

- **Warn for every session without a compared snapshot, including sessions
  with no check snapshot at all.** Every Kraken session would warn, since
  only the Coinbase adapter writes comparison events, and so would any
  recording shorter than the probe interval. The decision concerns snapshots
  that could not be compared. A probe that delivered nothing is already
  reported as `probe_failed`.
- **Check versions only when a query fails.** Cheaper, but the failure would
  come late and only for the dataset queried.
- **One version record per store.** That is a storage format change, and a
  store whose files were mixed by appending would still pass.
- **Key trades by `(log_id, trade_id)`.** Every repeat would become a second
  row, so research reading two overlapping logs would count trades twice.
- **Compare each repeat with the previous observation instead of the first.**
  A value that changes and then changes back would be reported twice; against
  the first observation, each disagreeing repeat is reported once.

## Consequences

- The store built before ADR 0013 (`data/normalized-before-adr0013` on the
  owner's machine) is now refused with the message, naming one of its
  version 2 `trades` files.
- Opening a session reads one footer per visible file. On the owner's
  machine that is about 0.27 ms per file: 12 ms for the 46 files of
  `data/normalized`, so about 3 s at 10,000 files. If stores grow that large,
  fewer, larger files are the remedy.
- Repeats are only seen within one normalizer. `ingest` normalizes one raw
  log at a time, so a trade repeated across two logs ingested separately is
  not compared; two logs fed to one normalizer are. The normalizer keeps one
  entry per trade it has seen.
- **Coinbase resends trades within its live stream.** ADR 0013 said no
  existing log contained a repeated trade. That was checked only on the
  2026-11-11 logs, and it is wrong for the 30-minute 2026-11-13 recording
  (`data/raw-probes/`). There, 64 of 95,825 trades (0.07%) arrived a second
  time in a later `market_trades` update on the same connection: no
  reconnect, no backfill, 5.5 to 267 ms after the first (median 93 ms), in 26
  messages. All 64 were identical in price, quantity, side and time, so none
  is a conflict. The 2026-11-11 logs, the other 2026-11-13 recordings and the
  fixtures have no repeats. No recording has a session without a compared
  check snapshot.
- **For a repeat, the store keeps the later observation.** Both rows have the
  same key; the view keeps the later knowledge time and, within one batch,
  the later `recv_ts`. For those 64 trades the stored `recv_ts` is therefore
  the second arrival, up to 267 ms after the first. That cannot cause
  look-ahead, since the trade only looks known later, but it shifts their
  apparent latency. Keeping the first observation instead is a change to the
  normalizer or the store, and is left to the owner.
- The ADR 0013 and 0014 review items are confirmed; each ADR's "Review"
  section records it.
