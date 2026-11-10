# Architecture Decision Records

One short record per major decision. Add a new numbered file for a new
decision; do not rewrite an accepted ADR. To change a decision, write a new
ADR that supersedes it and update the old one's status line.

Statuses: **Proposed** (not yet confirmed, do not build on it without
checking), **Accepted**, **Superseded by NNNN**.

| # | Decision | Status |
|---|----------|--------|
| [0001](0001-repo-layout.md) | Monorepo with `cpp/` and `python/` | Accepted |
| [0002](0002-bindings-library.md) | pybind11, installed from pip | Accepted |
| [0003](0003-build-system.md) | CMake presets + vcpkg manifest | Accepted |
| [0004](0004-queue-design.md) | In-house bounded SPSC ring buffer | Accepted |
| [0005](0005-price-and-time-representation.md) | int64 mantissa with per-instrument exponent; int64 ns timestamps | Accepted |
| [0006](0006-storage-format.md) | Raw binary log + Parquet/DuckDB normalized store | Accepted |
| [0007](0007-agent-tool-permissions.md) | Allow-listed tool server, structurally enforced | Accepted |

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
