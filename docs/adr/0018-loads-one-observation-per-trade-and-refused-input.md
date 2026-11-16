# 0018: Loads, one observation per trade, and what the normalizer refuses

Status: Accepted (owner decision, 2026-11-15); implementation choices awaiting the owner's confirmation, see "Review"
Date: 2026-11-15
Amends: 0006, 0009, 0010, 0013, 0015, 0016, 0017

## Context

The 2026-11 review (`docs/reviews/2026-11-data-core.md`) found four faults that
ADR 0017 left open:

- **R04.** The store resolved versions record by record, and a later row with
  the same key won. Nothing could say "this record no longer exists". Loading
  a raw log again after an adapter fix replaced the rows the fixed adapter
  still produced and kept the ones it no longer produced: on the Kraken
  fixture a `message_rejected` error stayed at a record that now parsed, and
  on Coinbase that error and the `sequence_gap` it caused. ADR 0009's "safe to
  fix an adapter bug and re-ingest" held only for fixes that remove nothing.
- **R05.** The adapters built rows that the schema checked only at the next
  flush. A value the adapter accepted and the schema could not hold (a venue
  time after 2262, a sequence number beyond int64) raised out of `ingest` and
  stopped the load.
- **R10.** `trades` was the one dataset keyed by the venue's identity
  (`trade_id`) rather than by raw record. Which observation of a trade the
  store showed depended on the path: the first, inside one normalizer (ADR
  0016); the later load, across separate runs (ADR 0015); within one batch,
  the later venue time and then the later receipt time.
- **R11.** A normalizer kept one current log id. Fed two logs interleaved,
  which is what `logorder.merge_logs` yields, it tagged every row with the
  second log's id; keys collided and nothing said so.

## Decision

The owner decided the following on 2026-11-15.

1. **R04: every ingest of a raw log is a load,** with its own `load_id`. A load
   can span several batches (live ingestion will add a log in chunks). For
   each `log_id`, a session reads only rows from that log's latest load whose
   batches are all within its version cutoff. Rows an older load produced but
   the latest load does not simply stop appearing. Older loads stay on disk,
   so the store remains append-only.
2. **R10: one rule for which observation of a trade is stored:** the first in
   replay order (`log_id`, then `seq`), across logs and across loads.
   Conflicts are still `TRADE_CONFLICT` errors.
3. **R11: the normalizer refuses logs fed interleaved,** with a clear error.
4. **R05: a value the adapter accepts but the schema refuses is a rejected
   message** (`MESSAGE_REJECTED`), not a crashed load.

## How it is built

### Loads (R04)

**Store layout 3.** Each commit marker gains a `load` field: `null` for a
batch written outside any load, otherwise

```json
"load": {"load_id": 1794752479134214900, "log_id": "20261111T030919.259642800Z-2ca464c155f1", "last": true}
```

- A load is of one raw log. Every row appended to one of its batches must
  name that log; a table that does not is refused (and, as for any refused
  table, the batch stays open).
- `load_id` is the knowledge time of the load's first batch, so it is unique
  in the store and loads are ordered by when they began.
- `last` marks the batch that completes the load. A last batch with no rows
  still publishes its marker, because the marker is what completes the load.

**Writer API.** `AppendOnlyStore.load(log_id)` returns a `Load` with
`batch(*, last=False)`, `finish()` (completes the load with an empty batch,
for a loader that learns only afterwards that the log has ended), and the
properties `log_id`, `load_id` and `finished`. A finished load takes no more
batches. `batch()` and `append()` are unchanged and write outside any load.

**`ingest`** loads a raw log as one load of one batch, opened with `last=True`.
An empty raw log has no first record to name it and loads nothing.

**What a session reads,** at its version V:

1. For each log, the load chosen is the one with the greatest `load_id` among
   the loads whose last batch is at or before V. Only that load's batches at
   or before V are read for the log. Loads that are not complete within V are
   not read at all.
2. A log with no complete load within V is read, as before ADR 0018, from the
   batches written outside a load: the latest version of each record.
3. Rows of a log that has a chosen load are not read from batches written
   outside a load.
4. Then both cutoffs and the choice of version per record, as in ADR 0017.

The choice depends on V alone, never on `available_at`, and is made once
when a session or replay cursor opens. Since batches commit in knowledge-time
order and a load's last batch says so when it commits, the same V always
chooses the same loads: versions stay repeatable.

**Layer 3** (the engine's file limit) covers the rule: the files of a load that
was not chosen are not in `allowed_paths`, so they cannot be opened even
through the session's private connection. For rule 3 the view filters rows;
the files of batches outside a load stay openable, since a file can hold rows
of several logs.

**Damaged markers** are refused with a `ValueError`: a layout 3 marker with no
`load` field, a load that is not an object, a `load_id` that is not an integer
at or before its batch's knowledge time, a malformed `log_id`, a `last` that is
not a boolean, two batches of one load naming different logs, and a batch of a
load after its last batch.

### One observation per trade (R10)

- `trades` is keyed by raw record like the other datasets:
  `(log_id, seq, trade_id)` (one message can carry several trades; a trade
  twice in one message is kept once, ADR 0016). Every observation the
  normalizer keeps is stored.
- `Dataset.one_row_per` is new. For `trades` it is `("trade_id",)`: the view
  shows, per venue, instrument and `trade_id`, the first observation in
  `(log_id, seq)` order. The choice is made after the load rule, both cutoffs
  and version resolution, so a session whose `available_at` is before the
  first observation arrived shows the one that had.
- The normalizer is unchanged: within one normalizer an identical repeat is
  dropped and is a `TRADE_REPEAT` event (ADR 0016), and a conflicting repeat
  is stored and is a `TRADE_CONFLICT` event (ADR 0015). Whatever is stored,
  the session shows the first observation.
- No schema version changed: the files hold the same columns. The layout
  change already forces every store to be rebuilt.

### Values the schema refuses (R05)

- `schema.check_row(dataset, row)` applies `table_from_rows`'s rules to one row
  without building a table. A property test shows the two accept exactly the
  same rows.
- `Normalizer.feed` checks every row a record produced, and every stream event
  noted for it, before keeping any. If one is refused, the record is rejected
  with reason `schema_refused` and the schema's message as detail, and none of
  its rows are kept (a message is all or nothing).
- Events noted for a rejected record that the schema can hold are stored, as
  for any rejected record (a gap noticed before a parse failure, an override
  of the recording clock). Those it cannot hold are dropped and named in the
  `MESSAGE_REJECTED` event's detail. This now applies to every rejected record,
  not only `schema_refused` ones; the fuzz test in `test_normalizer_input.py`
  found a malformed Coinbase message whose noted event could not be stored.
- The adapter is told through `on_reject`, as for any rejected record.
- A trade in a rejected record is not remembered as seen, so its next valid
  observation is stored rather than dropped as a repeat.
- **The Coinbase adapter refuses a `sequence_num` outside 0 to 2^63 - 2**
  before it changes any state. It keeps the highest number seen per
  connection, so without this one garbage number would make every later
  message on the connection look out of order against a value the store
  cannot hold, and each would be rejected.

### Interleaved logs (R11)

- Every raw log's writer numbers its records 0, 1, 2, ... A record whose `seq`
  is neither 0 (the next log) nor the previous record's plus one raises
  `InterleavedLogs` (a `ValueError`) with a message naming both numbers and
  saying to feed one log at a time, in `seq` order.
- After that the normalizer refuses everything, `feed` and `take_rows` alike:
  the rows it already holds may name the wrong log.
- The rule also refuses one log fed with a record missing or out of order,
  which no raw log can be. The test helper `damage.removed` therefore
  renumbers the records after the one it removes, as `damage.duplicated`
  already did, and the reconstructor tests that use it state their expected
  record numbers through `damage.original_seq`.
- `ingest` reads one raw log file, whose reader checks the numbering, so it
  is never fed interleaved records.

## Alternatives considered

**R04**

- **Retraction rows** (a tombstone per record a reload no longer produces).
  The loader would have to know what an earlier load produced, which means
  reading the store while writing it, and every dataset would need a deleted
  flag.
- **Delete or overwrite the earlier load.** Not append-only (CLAUDE.md rule 10).
- **A `load_id` column on every row.** A schema change of all four datasets.
  The marker is enough, because every batch belongs to at most one load.
- **Read a load as soon as any of its batches is committed.** A reload in
  progress would show part of the new load and none of the old, a log that
  looks short. And it would not be repeatable unless a load's end were
  recorded anyway.
- **Treat each batch written outside a load as a load of its own.** Every
  record-level correction written with `append` would then remove every other
  row of the log, in every dataset.
- **Order loads by when they completed** rather than when they began. The two
  differ only for two loads of one log in flight at once, which `ingest` never
  does; `load_id` is simpler to state and to read off a marker.

**R10**

- **Keep `trade_id` as the key and break ties on `(log_id, seq)`.** Versions are
  chosen by knowledge time first, so a later load would still replace an
  earlier observation.
- **Key by `(log_id, trade_id)`** and show every row (rejected in ADR 0015:
  research would count a trade twice). The one-row-per view removes the
  objection, and the raw-record key is the same shape as the other datasets'.
- **Drop repeats across logs in `ingest`** by reading what the store already
  holds. The writer would read the store, and the result would depend on load
  order.

**R05**

- **Check at flush and drop the whole flush.** Loses every other message in it.
- **Clamp the value.** Stores something the venue did not send.
- **Widen the columns.** A schema change for values that are garbage.

**R11**

- **Keep state per log** (the review's second option). A raw record does not
  say which log it is from: after the second log's first record, the first
  log's record 1 cannot be told from the second log's. No normalizer can do it.
- **Refuse only a `seq` that does not increase.** Allows records to be left
  out, but misses one log nested inside another: records 0 to 99 of A, all of
  B, then A's record 100 follows B's last record with a higher number and is
  taken as B's.
- **Reject the records instead of raising.** By the time the order breaks,
  rows of the earlier records may already name the wrong log; a count of
  rejects suggests the rest is sound.
- **Name the log in each record** (for example, `merge_logs` yielding
  `(log, record)` into a normalizer that takes it). It would make merged feeds
  work; it is a larger change to an interface nobody uses yet.

## Consequences

- **Every store has to be rebuilt.** A layout 2 store is refused by the session
  and by the writer with the rebuild message. `data/normalized` on the owner's
  machine was rebuilt; the numbers are in the roadmap (Phase 3f).
- **A reload is a replacement.** Fixing an adapter and loading the log again
  now clears what the fix resolved, including rows that disappear (ADR 0009's
  claim holds again, and ADR 0014's "re-ingesting into the same store is
  enough" for every kind of fix). A session at an earlier version still reads
  the earlier load.
- **The store grows with every reload.** Nothing is compacted.
- **A load that is not complete is not read,** including the first load of a
  log while a live loader is still adding to it. See Review item 1.
- **A trade is one row to research,** however many logs and loads saw it. A
  window holds a trade if its first observation was received in it: a trade
  resent an hour later is not a trade of that later hour.
- **A trade seen live and resent by a later log** is shown as its live
  observation, whatever order the logs were loaded in (review, R10).
- **Conflicts between logs loaded in separate runs are not detected,** as
  before: `TRADE_CONFLICT` comes from the normalizer. Both observations are now
  stored, so a query could find them. See Review item 7.
- **ADR 0016's consequence "after a conflict, the store holds the conflicting
  value" no longer holds:** the session shows the first observation.
- **A message with an unstorable value is one rejected record,** reported as a
  `message_rejected` ERROR, and the rest of the log loads.
- **Normalizing costs a schema check per row.** Measured on the one-hour logs
  in the roadmap (Phase 3f).
- **A normalizer fed anything but whole logs in order raises.** Feeding
  `merge_logs` output to one normalizer is no longer a way to compare trades
  across simultaneous logs (ADR 0015 mentioned it); feed each log whole.
- Findings R06 to R08 of the review are not changed by this ADR and their
  tests stay marked as expected failures.

## Review

These choices were made while building it and are not yet confirmed by the
owner:

1. **An incomplete load is never read.** That is the decision's "batches all
   within its version cutoff" with a load's end recorded in its last marker.
   It includes a log's first load while a future live loader is still adding
   chunks, and a multi-batch load that died before its last batch. If live
   chunks should be readable as they arrive, one option is: read the latest
   incomplete load of a log that has no complete load. A reload in progress
   would still not hide the previous complete load.
2. **"Latest" is the greatest `load_id`,** the knowledge time of the load's
   first batch.
3. **Batches outside a load remain** (`append`, `batch()`), read record by
   record for logs with no complete load. No production code writes them; the
   tests do. Their files stay openable by the engine for a loaded log (the
   view filters the rows).
4. **Store layout 3,** with layout 2 stores refused.
5. **The writer API:** `load(log_id)`, `Load.batch(last=...)`, `Load.finish()`;
   one load per raw log; `ingest` as one batch.
6. **Trades keyed by `(log_id, seq, trade_id)`,** shown one per `trade_id`
   through `Dataset.one_row_per`; no schema version bump.
7. **Conflicts across separate loads stay undetected.** An option: a view of
   every observation of a trade (still inside both cutoffs) and a quality
   check over it, which would also make `TRADE_REPEAT` across logs possible.
8. **R05's details:** reason `schema_refused`; events of a rejected record the
   schema can hold are kept and the others named in the detail (for every
   rejected record); the adapter's state is not rolled back, apart from the
   Coinbase sequence number check above.
9. **R11's rule is strict continuity,** refusing a missing record as well as
   interleaving; the normalizer refuses everything after; `damage.removed`
   renumbers.
10. **Two review tests were reshaped,** because the owner decided a behavior
    other than the one they required:
    - R10's `test_r10_a_conflicting_repeat_shows_the_later_value` required the
      *later* value of a conflicting repeat, as data-core then said. The rule
      is the first observation. It is now
      `test_r10_a_conflicting_repeat_shows_the_first_observation_whatever_the_clocks_say`,
      with the original two cases and their mirror images, where the old
      tie-break picked the later row; on the code before this ADR those two
      fail with `(4, 1010)`.
    - R11's test required every row of two interleaved logs to keep its own
      log id, which no normalizer can do (see the alternatives). It now
      requires that feeding them interleaved raises and yields no rows, and
      that the same logs fed one after the other keep their own ids with no
      key twice. On the code before this ADR it fails with `DID NOT RAISE`.

    Three other tests pinned the rule R10 replaces and were updated to it:
    `test_two_logs_with_the_same_trade_keep_one_row` (`test_log_id.py`, now
    expecting the first log's row), `test_the_version_is_chosen_among_the_rows_available`
    (`test_session.py`, whose correction now reuses its record; the resend it
    used is a new test), and the assertions of `TRADES.key` in `test_store.py`
    and `test_schema.py`. The store's public operations and the marker layout
    asserted in `test_store.py` and `test_session.py` gained `load` and
    layout 3.
