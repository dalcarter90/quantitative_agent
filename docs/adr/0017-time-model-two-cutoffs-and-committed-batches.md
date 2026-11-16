# 0017: The time model: two cutoffs, committed batches, receipt-time windows

Status: Accepted (owner decision, 2026-11-15); implementation choices to be confirmed by the owner, see "Review"
Date: 2026-11-15
Amends: 0006, 0008, 0011, 0015

## Context

The 2026-11 review (`docs/reviews/2026-11-data-core.md`) found that the data
core used one time for two different questions, and a second time for a third:

- **R01.** `PITSession` had one cutoff, `as_of`, on `knowledge_ts`: the wall
  clock when a row was loaded. The architecture said a backtest opens a
  session with `as_of` equal to simulated time. A raw log is loaded after it
  was recorded, so such a session was empty, and a session opened "as of now"
  could read every row of the future. The range methods bounded venue time,
  so even a careful caller got rows that had not been received yet.
- **R02.** One load wrote four batches (instruments, trades, book events,
  stream events), each with its own knowledge time. Between the last two a
  reader saw book rows without the integrity events that qualify them.
- **R03.** The store read the clock, then wrote, then published. A session
  opened in between with a later cutoff missed the batch; the same cutoff
  afterwards included it.
- **R09.** A time window selected book rows by venue time and stream events
  by receipt time, so one window meant two things.

## Decision

The owner decided the following on 2026-11-15.

### Three times, each with one job

| Time | Column | What it is | What it is used for |
|------|--------|------------|---------------------|
| Venue time | `event_ts` | When the venue says it happened | Lag, venue-time order, features. Never a cutoff, never a window, never a partition |
| Receipt time | `recv_ts` | When our process received the bytes | **Availability**: what a session may see. Every time window. The date partition |
| Store version | `knowledge_ts` | Which load a row belongs to | **Version**: which corrections a session reads |

### 1. A session has two cutoffs

```python
PITSession(root, *, available_at, version=None)
```

- **`available_at`**, required, no default: a simulated time T. Only rows
  with `recv_ts <= T` are visible.
- **`version`**, optional: a knowledge cutoff. Among the rows available at T,
  the version of each record that is returned is the latest with
  `knowledge_ts <= version`. Both cutoffs are applied before a version is
  chosen, so a later observation that had not arrived by T does not hide an
  earlier one that had.
- `version` defaults to the latest committed batch.
- **`as_of` is retired, not redefined.** Passing it raises a `TypeError` that
  names both replacements. An old call fails instead of silently reading
  other data. Every caller in the repository was changed by hand.
- Order is unchanged: rows come back in `(log_id, seq)` order (ADR 0013).
- `instrument(venue, instrument, at=None)` defaults `at` to `available_at`.

**A replay cursor** lets a backtest open the store once:

```python
cursor = ReplayCursor(root, *, start, end, version=None)   # the harness keeps this
strategy(cursor.session)                                   # a PITSession
cursor.advance(now)                                        # forward only, at most `end`
```

`cursor.session` reads exactly what `PITSession(root, available_at=now,
version=...)` would read. The session has no method that moves time and holds
no reference to its cursor, so research code that is handed the session
cannot look ahead.

### 2. A load is one batch, readable only once it is committed

- `AppendOnlyStore.batch()` opens a batch. Every table appended to it, in
  every dataset, gets the same `knowledge_ts`. `append` is a batch of one
  table.
- The batch's data files are written first. Its **commit marker**,
  `_batches/batch-<knowledge_ts>.json`, is published last, in one step, and
  names every file of the batch.
- **Sessions ignore uncommitted batches.** A data file that no marker names
  does not exist for a session: not in its views, and not openable by its
  engine.
- `ingest` loads a raw log as one batch.

### 3. A time window is receipt time, for every dataset

- `start` and `end` on `trades`, `book_events` and `stream_events` bound
  `recv_ts`. A book row and the integrity event noticed at the same raw
  record are in the same windows.
- The quality report's date range is receipt dates, and every check's range
  is receipt time.
- Files are partitioned by the UTC date of `recv_ts`, not `event_ts`. Every
  store is rebuilt anyway (below), so the change costs nothing more.
- Filtering by venue time stays possible on the column, through `sql`.

### 4. Stores written before this are refused

A store with data files but no commit markers is refused by the session and
by the writer with a `StoreVersionError` that says to rebuild it from the raw
logs, like a store of another schema version (ADR 0015).

## How it is built

**Store layout 2.**

```
<root>/<dataset>/venue=<v>/instrument=<i>/date=<UTC date of recv_ts>/
    part-<knowledge_ts, 20 digits>-<number, 6 digits>.parquet
<root>/_batches/batch-<knowledge_ts, 20 digits>.json
```

A marker is JSON: `layout` (2), `knowledge_ts`, `files` (paths within the
store) and `rows` (per dataset). It is written under a temporary name and
published by a hard link, which appears in one step and cannot replace a file.

**Knowledge time.** Read from the clock when the batch is opened, strictly
greater than any issued before, counting batches that were never committed.
It starts at 1, so version 0 always means "before any batch".

**Why a version is repeatable (R03).** A session reads the markers when it is
opened. Its version is the latest committed batch, or a `version` the caller
gave, which **must not be later than the latest committed batch** (a
`ValueError` otherwise). A batch cannot commit behind a later committed batch.
So every version a session can hold contains exactly the batches committed at
or before it, now and later. The knowledge time is still read before the
files are written; that no longer matters, because no session can ask for a
version at or past a batch that is not committed.

**Three layers, for both cutoffs.** The existing layers each cover the new
cutoff:

1. Views filter `knowledge_ts <= version` and `recv_ts <= available_at`.
2. `sql` is validated against the same allow-list as before.
3. The session's engine may open only the files of committed batches at or
   before `version` whose date partition is on or before the day of
   `available_at` (for a cursor, the day of `end`). A file of a later receipt
   day, a later version or an uncommitted batch cannot be opened even through
   the private connection.

**The cursor.** `advance` replaces the four views in one transaction on the
same engine, with the new time written into them as a literal. The engine's
configuration stays locked.

**A failed load.** A batch that raises is abandoned and its files are
removed; nothing of it was ever readable. A load that dies leaves files
without a marker. They are ignored by sessions and never deleted
automatically; the writer counts them so their knowledge time is not issued
again.

## Alternatives considered

- **Keep `as_of` and change what it means.** Fewer edits. Every existing call
  would keep running and read different data. The owner ruled it out.
- **Load with `knowledge_ts` set to `recv_ts`,** so the existing single
  cutoff would bound receipt. A correction needs a later knowledge time than
  the row it corrects, so the two cannot be one column.
- **Clamp a `version` later than the latest committed batch** to that batch
  instead of refusing it. Friendlier, but a caller who believes they pinned a
  version would not have. See Review item 1.
- **Stamp the knowledge time at commit.** It would then mean "readable from",
  exactly. The stamp is stored in every row and in the file names, which are
  written before the commit, so it would have to move out of the files and
  into the marker. Not needed for repeatability; see above.
- **One manifest file for the whole store** instead of a marker per batch. It
  would have to be rewritten on every commit, which the store never does.
- **Publish a batch by renaming a staging directory.** One step for the whole
  batch, but a rename can replace an existing target on POSIX, and the store's
  rule is that nothing is renamed.
- **A table format with snapshots (Delta, Iceberg, a DuckDB database).**
  Commit and versions for free, at the cost of a dependency and a file format
  the ADR 0006 archive argument rejects.
- **Keep venue-time windows and add receipt-time ones.** Two meanings for
  `start` and `end`, which is the fault R09 names.
- **Keep the partition on the date of `event_ts`.** Then no file could be
  excluded by `available_at`, and layer 3 would not cover the new cutoff.
- **Give the session an `advance` method.** Simplest, and research code could
  call it. The cursor keeps the ability to move time out of the object the
  strategy holds.
- **Hold the cursor's time in an engine variable** instead of rebuilding the
  views. No DDL per step, but the cutoff would be mutable state inside the
  engine rather than text in a view.
- **One session per decision time,** as before. Measured at 56 to 71 ms each
  on the one-hour store (review, D4).

## Consequences

- **Every store has to be rebuilt** from its raw logs. `data/normalized` on
  the owner's machine was rebuilt; the numbers are in the roadmap (Phase 3e).
- **Backtests (Phase 7)** open one `ReplayCursor` over the replay and advance
  it. Its criteria in the roadmap say so. The cursor removes the cost of
  opening a session per decision; the cost of a query, which grows with the
  store (review, D4), is unchanged.
- **The agent's `query_data`** (architecture 8.1, not built yet) takes
  `available_at`. The holdout is a ceiling on `available_at`, which is a
  ceiling on receipt time, the clock the holdout is defined on.
- **A range of days in the quality report** is the data received on those
  days. For the recordings so far that is the same days as before.
- **Venue-time queries** scan every partition the session can see: the date
  partition no longer follows `event_ts`.
- **Opening a session** reads every commit marker (one small file per batch)
  as well as the footers of the files it may open.
- **A trade seen live and resent later** (review, R10) is now seen as the
  live observation by any session whose `available_at` is before the resend.
  After it, the store still shows the later observation. R10 stays open.
- **Versions have gaps.** A batch that was abandoned or died used a knowledge
  time that no committed batch has.
- **One writer per store is still assumed.** The commit-order check turns one
  way two writers could break repeatability into an error; it is not a lock.
- **CLAUDE.md safety rule 9** forbade "a mutable `as_of`". It now forbids any
  way for research code to move `available_at`; moving it is the replay
  cursor's job, and the cursor is the harness's.
- Findings R04 to R08, R10 and R11 of the review are not changed by this ADR
  and their tests stay marked as expected failures.

## Review

The owner decided the four points under "Decision", including the names
`available_at` and `version`, refusing stores without markers, and
partitioning by the date of `recv_ts`. These choices were made while building
it and are to be confirmed:

1. **A `version` later than the latest committed batch is refused,** not
   clamped. It is what makes "the same version reads the same rows" true
   without exception.
2. **The knowledge time is still read when the batch opens,** not at commit.
3. **The commit marker:** its place (`_batches/`), its name, its JSON
   content, and publishing it by hard link.
4. **Data file names** (`part-<knowledge_ts>-<number>.parquet`, numbered per
   directory), written directly under their final names, since the marker and
   not the name makes them readable.
5. **A batch that fails removes its files; one that dies leaves them.**
6. **`ReplayCursor` requires `end` and moves forward only.** `end` is what
   lets the engine's limit (layer 3) cover the cursor.
7. **The store's write API:** `batch()`, `Batch.append`, `commit`, `abandon`,
   with `append` kept as a batch of one table.
8. **The command lines:** `--available-at` and `--version` on `quality` and
   `book`; `--as-of` is refused with a message naming them. Both default to
   everything received at the latest version.
9. **Two review tests were reshaped, not just renamed.** R03's asked two
   sessions at knowledge cutoff 2000 for the same rows while a batch at 1000
   was being published; no store can promise that without refusing the
   request, and refusing it is the fix. R09's required a window that excludes
   the gap event to hold book messages from after the loss; with one clock
   that window holds none, which is the fix. Each now checks the property its
   finding was about, and each fails on the code before this ADR.
10. **One message in the quality report was corrected:** a bad recording
    clock puts `recv_ts` off, not `knowledge_ts` (review, X2).
