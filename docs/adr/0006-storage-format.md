# 0006: Raw binary log + Parquet/DuckDB normalized store

Status: Accepted (by the owner, as proposed), amended by 0009, 0010 and 0012
Date: 2026-11-10

## Context

Storage serves three different readers:

1. The C++ replay engine needs the exact bytes received, in order, read
   sequentially at high speed.
2. Research needs columnar, filterable, point-in-time queries over months of
   normalized data.
3. Audit needs an append-only record of what the system decided and did.

The data core is built fresh in this repository, so there is no existing data
or format to stay compatible with. Storage format is expensive to change once
data has been collected.

## Decision

| Tier | Format | Notes |
|------|--------|-------|
| Raw log | Append-only segments of length-prefixed records: `recv_ts`, `seq`, venue id, channel id, payload bytes. zstd-compressed when a segment is sealed | Source of truth. Never rewritten. Everything else can be rebuilt from it |
| Normalized store | Parquet files partitioned by `venue/instrument/date`, zstd, integer mantissas (ADR 0005), with `event_ts`, `recv_ts`, `knowledge_ts` on every row | Queried through DuckDB by `PITSession`. Corrections append rows with a later `knowledge_ts`; nothing is updated in place |
| Journal | Append-only fixed-size binary records written by the C++ journal thread | Converted to Parquet offline for analysis |
| Experiment registry | A single DuckDB or SQLite file | Small, transactional |
| Agent audit log | Append-only JSON lines | Human-readable, greppable |

Every tier is append-only. The normalized store's writer has no update or
delete operation, and readers go through `PITSession`, which exposes only
views filtered by `knowledge_ts` (architecture section 3.2).

## Alternatives considered

- **PostgreSQL/TimescaleDB.** Real transactions, mature bitemporal patterns
  with range types, concurrent writers. Needs a server, and tick-scale inserts
  and scans are slower than columnar files. A good fit for reference data and
  the experiment registry if a server is ever wanted.
- **DuckDB native database file as the store.** One file, fast, SQL. Single
  writer, and a proprietary file format is a worse long-term archive than
  Parquet. Used here as the query engine instead.
- **ClickHouse or QuestDB.** Built for this workload at scale. Operational
  overhead is not justified for a single-machine system.
- **kdb+.** Industry standard for tick data; licensing and a niche language.
- **Arrow IPC/Feather files.** Faster to read than Parquet, larger on disk,
  weaker ecosystem for partitioned datasets.
- **HDF5.** Common in older research stacks; poor concurrent access and
  fragile files.
- **Only a normalized store, no raw log.** Saves disk. Loses the ability to
  fix a parser bug by re-normalizing, and replay would no longer exercise the
  feed handlers.
- **SQLite for everything.** Simple and transactional, but row-oriented and
  slow for tick-scale analytics.

## Consequences

- Disk usage is roughly doubled by keeping raw and normalized tiers. Raw
  segments compress well and can move to cold storage.
- Parquet has no in-place update, which fits bitemporal append-only semantics
  but means compaction is a periodic batch job.
- Data directories are outside git (`data/`, `journals/` are ignored). Small
  recorded fixtures for tests are committed under a tests directory.
- DuckDB must read the Parquet files on the session's behalf while the
  session's own SQL surface is barred from reading files. Phase 1b has to
  enforce that split and test it.
