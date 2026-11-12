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
| [0005](0005-price-and-time-representation.md) | int64 mantissa with per-instrument exponent; int64 ns timestamps | Accepted, amended by 0008 |
| [0006](0006-storage-format.md) | Raw binary log + Parquet/DuckDB normalized store | Accepted, amended by 0009 and 0010 |
| [0007](0007-agent-tool-permissions.md) | Allow-listed tool server, structurally enforced | Accepted |
| [0008](0008-event-ts-without-venue-timestamp.md) | `event_ts` holds `recv_ts` when the venue gives no timestamp; `event_ts_source` records which | Accepted |
| [0009](0009-venue-neutral-book-events.md) | Kraken as second venue; book events keyed by raw record, with optional per-venue integrity columns | Accepted, amended by 0010 |
| [0010](0010-stream-events.md) | `stream_events`: integrity events for every venue, written by the adapters, read through `PITSession` | Accepted, additions awaiting owner review |

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
