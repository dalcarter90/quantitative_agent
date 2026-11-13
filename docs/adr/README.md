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

| # | Decision | Status |
|---|----------|--------|
| [0001](0001-repo-layout.md) | Monorepo with `cpp/` and `python/` | Accepted |
| [0002](0002-bindings-library.md) | pybind11, installed from pip | Accepted |
| [0003](0003-build-system.md) | CMake presets + vcpkg manifest | Accepted |
| [0004](0004-queue-design.md) | In-house bounded SPSC ring buffer | Accepted |
| [0005](0005-price-and-time-representation.md) | int64 mantissa with per-instrument exponent; int64 ns timestamps | Accepted, amended by 0008, 0011 and 0012 |
| [0006](0006-storage-format.md) | Raw binary log + Parquet/DuckDB normalized store | Accepted, amended by 0009, 0010, 0012 and 0013 |
| [0007](0007-agent-tool-permissions.md) | Allow-listed tool server, structurally enforced | Accepted |
| [0008](0008-event-ts-without-venue-timestamp.md) | `event_ts` holds `recv_ts` when the venue gives no timestamp; `event_ts_source` records which | Accepted, amended by 0012 |
| [0009](0009-venue-neutral-book-events.md) | Kraken as second venue; book events keyed by raw record, with optional per-venue integrity columns | Accepted, amended by 0010 and 0013 |
| [0010](0010-stream-events.md) | `stream_events`: integrity events for every venue, written by the adapters, read through `PITSession` | Accepted, amended by 0011 and 0013 |
| [0011](0011-synchronized-recording-clock.md) | The recording clock must be synchronized; the recorder measures it, refuses above 250 ms unless overridden, and stores the measurement | Accepted |
| [0012](0012-replay-order-and-monotonic-receive-time.md) | Replay order within a raw log is `seq`; records gain a monotonic receive stamp; logs merge by it; clock steps are recorded and checked | Accepted, amended by 0013 (its open point settled) |
| [0013](0013-stored-rows-name-their-raw-log.md) | Every stored row names its raw log (`log_id`); keys and order use `(log_id, seq)`, never `recv_ts` | Accepted; implementation choices awaiting the owner (Review) |

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
