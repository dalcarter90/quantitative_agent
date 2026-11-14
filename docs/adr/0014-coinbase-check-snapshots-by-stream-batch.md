# 0014: Coinbase check snapshots are judged by stream batch

Status: Accepted (owner decision, 2026-11-13), with implementation choices awaiting the owner's confirmation (see "Review")
Date: 2026-11-13
Amends: 0009, 0010

## Context

A check snapshot is an independent image of the book, taken on a second
connection (ADR 0009). Phase 2 compared each one with the book rebuilt from the
stream after the last message whose venue time is at or before the snapshot's,
and required equality on every level. That held for all 7 snapshots recorded
on 2026-11-11 (1 in the fixture, 6 in the one-hour log).

On 2026-11-13, the 60-second Coinbase recording made for ADR 0012 had one
snapshot that differed on 55 of its 37,746 levels. The owner first proposed a
tolerance ("the top N levels exact, deeper differences a warning") and asked
for a 30-minute recording, with a snapshot every 60 s, before deciding N. That
recording (28 snapshots, kept in `data/raw-probes/`, not in git) showed:

- **No N works.** 6 snapshots were exact; 13 of the other 22 differed at the
  best bid or ask (rank 0).
- **The stream comes in batches.** Coinbase's `level2` messages arrive about
  50 ms apart (median gap 49.6 ms on 2026-11-13, 52.6 ms on 2026-11-11), and
  every update in one message carries the same `event_time`. A check snapshot
  carries one time for all its levels.
- **A snapshot is exact exactly when its time equals a batch's time.** All 7
  snapshots of 2026-11-11 were timed on a batch, and all were exact. Of the 29
  of 2026-11-13, the 6 timed on a batch are exactly the 6 exact ones.
- **Between batches, only the next batch's levels differ.** For a snapshot
  timed between batch A and batch B, every level that B does not list equals
  the book after A. This held for every snapshot recorded, 36 of 36, with no
  exception. On B's levels the 23 snapshots timed between batches showed A's
  value (485 levels), B's value (503) or neither (58).

The reading that fits: the venue publishes each level's last value per batch,
and a snapshot taken partway through a batch already shows some of that
batch's changes, including values that were replaced before the batch was
sent, which the stream never shows. Why snapshots moved off the batch times
between 2026-11-11 and 2026-11-13 is not known. The recorder's probe code did
not change, so it is on the venue's side.

## Decision

The owner chose the batch rule, with explained differences graded as
information.

- **The rule.** For a check snapshot with venue time T, A is the last stream
  message of the snapshot's connection with venue time at or before T, and B
  the message after A.
  - T equals A's time: every level must equal the book after A.
  - Coinbase, T strictly between A's and B's times: every level that B does
    not list must equal the book after A. B's levels are **unverified**:
    reported with counts and ranks, never as errors.
  - No B (the stream ended first): every level must match.
  - Kraken: every level must match, always. Its stream is also verified by
    checksum after every message.
- **The Coinbase adapter keeps the book.** While normalizing, it rebuilds the
  book from the stream and keeps the last 60 s of messages, so it can recover
  the book after A. It writes one `CHECK_SNAPSHOT_COMPARED` stream event (kind
  15) per check snapshot, at the record where the comparison could be made:
  the snapshot's own, or a later one if B had not arrived. `expected` is the
  number of levels that break the rule, `observed` the number of unverified
  levels that differ, and `detail` JSON holds the snapshot's `seq`, the result
  (`exact`, `explained`, `mismatch` or `not_compared`), the reason when not
  compared, whether T was on a batch, how many levels B lists, and the
  shallowest and deepest rank of each kind of difference.
- **The reference reconstructor applies the rule independently**
  (`verify_check_snapshots`, with `CheckResult.result`, `.unverified` and
  `.summary()`). Tests show the two agree on synthetic cases, on both
  committed Coinbase fixtures, and locally on all 28 snapshots of the
  30-minute recording.
- **Rank** is the number of the snapshot's levels on that side that are
  better than the price: 0 is the best bid or ask.
- **Grading** (quality check `independent_snapshots`): a mismatch is an
  ERROR, an explained snapshot is INFO, a snapshot that could not be compared
  is INFO, and an exact one is counted.
- **Not compared**, with the reason:
  - `no_stream_book`: the snapshot's connection had no book yet.
  - `before_stream_snapshot`: the stream's own snapshot is later than T.
  - `next_message_is_snapshot`: B is a stream snapshot, not a batch.
  - `stream_unreliable` (adapter only; the reconstructor raises): the stream
    lost or rejected a message since its last snapshot.
  - `history_too_short` (adapter only): T is further back than the 60 s
    kept. Recorded snapshots arrive at most 1.7 s after T.
- **Unchanged:** the C++ order book (Phase 4) must equal the Python reference
  exactly at all depths after every message, for both venues. This rule only
  concerns the comparison with the venue's own snapshots.

## Alternatives considered

- **The top N levels exact, deeper differences a warning** (the first
  proposal). It fails for every N of 1 or more: 13 of 28 snapshots differ at
  rank 0.
- **Compare only the snapshots timed on a batch.** Simpler, but it drops the
  check entirely for 23 of the 29 snapshots of 2026-11-13. The batch rule
  still verifies every level of those outside the next batch: all but 2 to
  128 of about 36,000.
- **Keep the exact rule and report every mismatch.** 23 of 29 snapshots would
  be flagged, every one explained by timing.
- **A time tolerance per level** (a difference is accepted if the stream
  showed the snapshot's value within some window). It accepts any value the
  stream held nearby, so it is weaker on most levels. It still fails the 58
  values the stream never sent, and it has no principled window.

## Consequences

- On the 2026-11-13 recordings: 6 exact, 23 explained, 0 mismatched.
- Phase 2's criterion "equals every check snapshot on every level" holds on
  Coinbase in this rule's sense, and Phase 4's comparison with exchange
  snapshots uses this rule.
- Stores get the events by normalizing the raw logs again. The schema version
  is unchanged (a new kind value), so re-ingesting into the same store is
  enough: its rows become corrections, and the events are new rows.
- `python -m quantagent.data.book verify` reports each result. A mismatch
  fails it. A snapshot that could not be compared no longer does (before,
  one with no stream message before it failed).
- A new committed fixture, `python/tests/fixtures/between-batches/` (790 KB):
  the 60-second recording with the 55-level snapshot, now explained.
- The adapter writes no event for a snapshot still waiting for its next batch
  when a log ends without its disconnect or stop record. The reconstructor
  still compares it.
- The Kraken adapter writes no such event. Kraken's check snapshots are graded
  by `book verify` only.

## Review

Choices made during implementation, to be confirmed by the owner:

1. A snapshot that could not be compared is INFO, like `probe_failed`.
2. `book verify` no longer fails on a snapshot that could not be compared.
3. The adapter keeps 60 s of history and has the reasons listed above.
4. No comparison event for Kraken.
5. Committing the 60-second recording as a fixture.
