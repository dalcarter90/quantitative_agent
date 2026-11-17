# Architecture Decision Records

One short record per major decision. Add a new numbered file for a new
decision; do not rewrite an accepted ADR. To change a decision, write a new
ADR that supersedes it and update the old one's status line. To add to a
decision without replacing it, write a new ADR with an `Amends: NNNN` line and
add "amended by" to the old one's status line; the old text stays as it was.

Statuses: **Proposed** (not yet confirmed, do not build on it without
checking), **Accepted**, **Accepted, amended by NNNN**, **Superseded by NNNN**.
An ADR that records a choice made during implementation, rather than one the
owner made, says so in a "Review" section and in its status until the owner
confirms it.

**The data core** has one consolidated description,
[`docs/data-core-spec.md`](../data-core-spec.md) (ADR 0019). The ADRs below
marked "spec" are superseded by it: it restates their decisions as they now
stand, and they keep the reasons and the alternatives. A new decision about the
data core is a new ADR and an edit of the spec, in the same change.

| # | Decision | Status |
|---|----------|--------|
| [0001](0001-repo-layout.md) | Monorepo with `cpp/` and `python/` | Accepted |
| [0002](0002-bindings-library.md) | pybind11, installed from pip | Accepted |
| [0003](0003-build-system.md) | CMake presets + vcpkg manifest | Accepted |
| [0004](0004-queue-design.md) | In-house bounded SPSC ring buffer | Accepted, amended by 0020 |
| [0005](0005-price-and-time-representation.md) | int64 mantissa with per-instrument exponent; int64 ns timestamps | Accepted, amended by 0008, 0011, 0012 and 0019; superseded by the spec |
| [0006](0006-storage-format.md) | Raw binary log + Parquet/DuckDB normalized store | Accepted, amended by 0009, 0010, 0012, 0013, 0015, 0017 and 0018; raw-log and store tiers superseded by the spec |
| [0007](0007-agent-tool-permissions.md) | Allow-listed tool server, structurally enforced | Accepted |
| [0008](0008-event-ts-without-venue-timestamp.md) | `event_ts` holds `recv_ts` when the venue gives no timestamp; `event_ts_source` records which | Accepted, amended by 0012, 0013, 0017 and 0019; superseded by the spec |
| [0009](0009-venue-neutral-book-events.md) | Kraken as second venue; book events keyed by raw record, with optional per-venue integrity columns | Accepted, amended by 0010, 0013, 0014 and 0018; superseded by the spec |
| [0010](0010-stream-events.md) | `stream_events`: integrity events for every venue, written by the adapters, read through `PITSession` | Accepted, amended by 0011, 0013, 0014, 0015, 0016, 0018 and 0019; superseded by the spec |
| [0011](0011-synchronized-recording-clock.md) | The recording clock must be synchronized; the recorder measures it, refuses above 250 ms unless overridden, and stores the measurement | Accepted, amended by 0017 and 0019; superseded by the spec |
| [0012](0012-replay-order-and-monotonic-receive-time.md) | Replay order within a raw log is `seq`; records gain a monotonic receive stamp; logs merge by it; clock steps are recorded and checked | Accepted, amended by 0013 (its open point settled); superseded by the spec |
| [0013](0013-stored-rows-name-their-raw-log.md) | Every stored row names its raw log (`log_id`); keys and order use `(log_id, seq)`, never `recv_ts` | Accepted; implementation choices confirmed; amended by 0015 and 0018; superseded by the spec |
| [0014](0014-coinbase-check-snapshots-by-stream-batch.md) | Coinbase check snapshots are judged by stream batch: exact on a batch time; between batches, exact except on the next batch's levels | Accepted; implementation choices confirmed; amended by 0015; superseded by the spec |
| [0015](0015-owner-review-of-0013-and-0014.md) | Owner review of 0013 and 0014: `PITSession` refuses files of another schema version; a trade repeated with a different price, quantity or side is an ERROR; a session with no compared check snapshot is a WARNING | Accepted, amended by 0016, 0017, 0018 and 0019; superseded by the spec |
| [0016](0016-keep-the-first-observation-of-a-repeated-trade.md) | An identical repeat of a trade is dropped and recorded as a `TRADE_REPEAT` event, so the store keeps the first observation; confirms 0015's reading of "no check snapshot compared" | Accepted, amended by 0018 and 0019; superseded by the spec |
| [0017](0017-time-model-two-cutoffs-and-committed-batches.md) | The time model: a session has two cutoffs (`available_at` on receipt time, `version` on knowledge time) and `as_of` is retired; a load is one batch, readable only once its commit marker is written; time windows and date partitions are receipt time; a replay cursor advances one session | Accepted; implementation choices confirmed; amended by 0018; superseded by the spec |
| [0018](0018-loads-one-observation-per-trade-and-refused-input.md) | Every ingest of a raw log is a load, and a session reads each log from its latest complete load (or, if it has none, its latest incomplete load); loads are the only production write path; trades are keyed by raw record and shown once, as their first observation in `(log_id, seq)` order; a value the schema refuses is a rejected message; a normalizer fed interleaved logs raises | Accepted; implementation choices decided by the owner; amended by 0019; superseded by the spec |
| [0019](0019-fixed-exponents-per-severity-limits-and-the-data-core-spec.md) | Each instrument has one fixed price and quantity exponent (`FIXED_EXPONENTS`), with the venue's precision stored beside it; metadata is in effect from a session's first row; quality findings are limited per severity and a truncated report fails; a session's clock is judged on every day it spans; conflicts between logs and logs read from an incomplete load are quality findings; `docs/data-core-spec.md` restates the data core | Accepted; implementation choices confirmed; restated in the spec |
| [0020](0020-stream-control-events-resuming-book-and-snapshot-queue-policy.md) | `MarketEvent` gains stream control kinds (stale, reset, snapshot begin and end) with a reason and a source id; the C++ `InstrumentMeta` gains the venue's precisions; the reference book gains a resume mode, and the golden files the stale and recovered points; a large snapshot crosses a bounded queue whole or not at all, with resyncs gated on room (built in Phase 5); recommendations for D4 to D7 and T3, confirmed with their phases; the recorder reconnects its stream every 15 to 30 minutes instead of resubscribing on a gap | Accepted; implementation choices confirmed; amended by 0022; restated in the spec (sections 6, 11, 12) |
| [0021](0021-the-marketevent-converter.md) | `quantagent.marketevents.convert` turns stored book events and stream events into `MarketEvent`s by ADR 0020's rules, one group per step in the reference book's order, with sources numbered by a shared `SourceMap`; it refuses what a `MarketEvent` cannot carry; `from_bytes` refuses the unassigned flag bit and `InstrumentMeta` an exponent outside 0 to 18 (review D7) | Accepted; implementation choices confirmed; amended by 0022 and 0023 |
| [0022](0022-the-cpp-order-book.md) | The C++ order book: precision changes travel in the stream as a ninth `MarketEvent` kind, `InstrumentUpdate`, one per stored metadata version; the book's capacity is fixed per side at startup (65,536 by default, twice the depth for a cut book) and a level that does not fit makes it stale; the book has its own stale reasons beside the stream's; ADR 0021's Review confirmed | Accepted; the event's layout and the implementation choices confirmed (item 11 amended); amended by 0023 |
| [0023](0023-review-of-the-cpp-order-book.md) | The review of the C++ order book: an update cannot be read until it takes effect, and consumers act only on results that say a message did; the book is bound to its snapshot's stream and ignores and counts another stream's updates; a benchmark replays the committed Coinbase fixture and reports the tail; every stale reason is mapped at build time; the Kraken adapter refuses a record with two versions of the instrument; a tick or lot size below 1 is refused; the review's notes go to Phases 5, 7 and 9; ADR 0022's Review confirmed | Accepted (owner decisions); the implementation choices awaiting the owner's review; spec, in part (section 7) |

## Template

```markdown
# NNNN: Title

Status: Proposed | Accepted | Superseded by NNNN
Date: YYYY-MM-DD

## Context
## Decision
## Alternatives considered
## Consequences
```
