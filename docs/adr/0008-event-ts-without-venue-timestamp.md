# 0008: `event_ts` for messages without a venue timestamp

Status: Accepted (owner decision), amended by 0012
Date: 2026-11-11
Amends: 0005

## Context

ADR 0005 and the architecture give every stored row an `event_ts`, "when the
venue says it happened", and the Phase 1a schemas make it a required column.
The store also partitions by the UTC date of `event_ts`.

Some venue messages carry no venue timestamp at all. Instrument metadata is
the common case: Kraken's `instrument` channel and Coinbase's product endpoint
both list tick and lot sizes without saying when they took effect. Some
venues' REST order book snapshots are another (the question first came up for
one that returns only an update id, bids and asks). These rows still have to
be stored, partitioned and ordered.

## Decision

- `event_ts` stays required (non-null, `int64` nanoseconds) on every row.
- When the venue message has no venue timestamp, `event_ts` holds the row's
  `recv_ts`.
- Every dataset gets a required column `event_ts_source`, a string with
  exactly two values:
  - `"venue"`: `event_ts` came from the venue's message.
  - `"receipt"`: the venue gave no timestamp and `event_ts` is a copy of
    `recv_ts`.
- The adapter sets `event_ts_source` from the message type, never by
  comparing the two timestamps. A venue timestamp that happens to equal
  `recv_ts` is still `"venue"`.
- A venue timestamp that is present but implausible is stored as given with
  source `"venue"`. Substituting `recv_ts` is only for an absent timestamp;
  judging plausibility is the quality checks' job.

This is recorded now and implemented in Phase 1c, together with the adapters
that need it. It raises the schema version of `trades`, `book_events` and
`instruments` from 1 to 2. No data has been stored under version 1, so there
is nothing to migrate.

## Alternatives considered

- **Make `event_ts` nullable.** The most literal representation of "the venue
  did not say". Every consumer then has to handle null: the date partition
  needs a fallback rule, time-range queries silently drop the rows, and sorts
  need a null policy. The fallback would be `recv_ts` in practice, so it is
  better stated once in the data than re-derived by each reader.
- **A sentinel value (0 or -1).** Keeps the column non-null without a second
  column, but puts the rows in a 1970 partition and makes every range query
  and skew check special-case the sentinel.
- **Copy `recv_ts` into `event_ts` with no marker.** Simplest, but
  indistinguishable afterwards from a real venue timestamp. Latency and clock
  skew measurements would count these rows as zero skew and be biased.
- **Two columns: nullable `venue_ts` plus an always-present ordering
  timestamp.** Equivalent information to this decision, at the cost of an
  eight-byte column where a two-value marker is enough, and it renames a
  column the architecture already defines.
- **An integer enum instead of a string.** Consistent with `side` and `kind`.
  The owner chose the readable values; Parquet dictionary-encodes a two-value
  string column to almost nothing, and the C++ event struct can map it to a
  one-byte enum at its own boundary.
- **Do not store snapshots that lack a timestamp.** They are needed to seed
  and verify order books.

## Consequences

- Anything that interprets `event_ts` as venue time must filter on
  `event_ts_source = 'venue'`: the clock skew quality check (Phase 1d),
  latency statistics, and any feature that uses venue time.
- A `"receipt"` row is partitioned by the date we received it. For live
  capture that is the right day. A snapshot fetched long after the fact would
  land on the fetch date.
- `event_ts` is not a safe ordering key between a `"receipt"` snapshot and
  `"venue"` deltas around it. Book reconstruction orders by the venue's
  sequence numbers (and replay by `(recv_ts, seq)`), never by `event_ts`.
- For instrument metadata, `event_ts` is the time a version takes effect and
  is the version's key. A `"receipt"` row there means "in effect from when we
  first saw it", which is the best available statement, and the marker makes
  that visible.
- `PITSession` builds its views from each dataset's stored schema, so the new
  column appears in query results without changes to the session.
