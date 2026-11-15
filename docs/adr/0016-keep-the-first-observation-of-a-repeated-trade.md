# 0016: An identical repeat of a trade is dropped; the store keeps the first observation

Status: Accepted (owner decision, 2026-11-13)
Date: 2026-11-13
Amends: 0010, 0015

## Context

ADR 0015 made a trade seen again with a different price, quantity or side an
ERROR (`TRADE_CONFLICT`), and left two questions for the owner:

- **Its reading of decision 1.** A session none of whose check snapshots
  could be compared is a WARNING. ADR 0015 read this as not covering a
  session with no check snapshot at all.
- **Which observation of an identical repeat to keep.** Coinbase sends some
  trades twice in its live stream: 64 of 95,825 (0.07%) in the 30-minute
  2026-11-13 recording, 5.5 to 267 ms apart, all identical. Both
  observations were stored with the same key, and the store's view kept the
  later one. Those trades' `recv_ts` was therefore the second arrival.

## Decision

1. **ADR 0015's reading of "no check snapshot compared" is confirmed.** A
   session with no check snapshot at all does not warn.
2. **An identical repeat is dropped, so the store keeps the first
   observation.** The normalizer compares each trade with the first
   observation of its (venue, instrument, `trade_id`), across every log fed
   to it (ADR 0015). If the price, quantity and side are all the same, the
   repeat's row is not passed on to the store. Instead the normalizer writes a
   `TRADE_REPEAT` stream event (kind 17) at the repeat's record:
   - `observed` is how many nanoseconds of `recv_ts` after the first
     observation the repeat arrived;
   - `detail` is JSON with the `trade_id` and the first observation's
     `log_id` and `seq`.

   Only the repeated trades of a message are dropped. The message still counts
   as data, and its other trades are stored. A trade twice in one message is
   kept once.
3. **Repeats stay visible.**
   - The normalization report's summary line reads `trade conflicts: N
     (trades seen again: M; identical repeats dropped: K)`.
   - The `stream_integrity` quality check reports `trades_repeated`, INFO,
     once per session (one raw log) that has repeats. The finding gives the
     count and the range of delays, and points at the session's first repeat.
     The statistics count every `TRADE_REPEAT` event.
4. **Conflicting repeats are unchanged.** A repeat that differs is still
   stored and is a `TRADE_CONFLICT` event, graded ERROR (ADR 0015).
5. **The raw log is unchanged.** It still holds every observation, so the
   dropped repeats can be recovered by normalizing again.

## Alternatives considered

- **Keep the first observation in the store instead of the normalizer:**
  change the view to keep the earliest `recv_ts` for a key. That is a change
  to how the store resolves corrections. The rule "a later knowledge time
  wins" is what makes corrections possible (ADR 0006), so it would have to
  become a per-dataset exception.
- **One INFO finding per repeat.** Coinbase resends about 130 trades an hour.
  The quality check stops at `max_findings` (1,000 per check and
  instrument), so about eight hours of Coinbase data would fill the limit
  with INFO findings and hide any later ERROR in the same check. One finding per session
  carries the same information. Each repeat is still its own stream event.
- **Only a count in the report.** The report is printed at load time and not
  stored, so the repeats would not be visible to the quality check or to
  research later.
- **Drop conflicting repeats too.** The owner decided they stay as they are:
  the conflict is an ERROR, and the stored row shows the later value.

## Consequences

- **On the 2026-11-13 recording all 64 repeated trades now keep the first
  arrival's `seq` and `recv_ts`.** This was checked independently of the
  adapter: the raw log's market trades were read directly and each trade's
  first arrival found. Then:
  - all 95,825 stored trades are their first arrival;
  - there are 64 `TRADE_REPEAT` events and no `TRADE_CONFLICT`;
  - the delays run from 5.5 to 267.4 ms, median 92.7 ms;
  - the quality check reports one `trades_repeated` INFO finding.
- The 2026-11-11 logs and the committed fixtures have no repeats, so their
  stores and golden files are unchanged.
- **A trade seen first in a trade-history snapshot (`TRADE_BACKFILL`) and
  later live keeps the backfilled observation.** Its receipt time says
  nothing about when it happened, and the quality checks treat it as
  backfill. A reconnect produces the other order: live, then resent in
  history. There the live observation is now kept, where
  before the backfill copy replaced it. No recording has a repeat involving
  backfill in either order.
- **After a conflict, a later repeat that matches the first observation is
  dropped.** The store then holds the conflicting value, not the one seen
  twice. The conflict is already an ERROR.
- Repeats across logs loaded in separate runs are still not compared, as
  ADR 0015 says. For those, the store keeps the later observation, as
  before.
