# 0010: `stream_events`, a dataset of integrity events

Status: Accepted (owner decision; see "Review" for the parts added during implementation)
Date: 2026-11-11
Amends: 0006, 0009

## Context

Market data is only as good as the stream it came from. Phase 1c left the
facts about the stream outside the store:

- Coinbase numbers every message on a connection, across all channels. A gap
  can be seen while normalizing but cannot be recovered afterwards from
  `book_events`, because heartbeats and trades use the same numbers and are
  not stored with them.
- Kraken has no sequence numbers. Its only protection is a checksum of the top
  of the book, and nothing recorded whether it held.
- Reconnects, subscription changes and messages that could not be parsed were
  visible only in the report of one normalization run.

The quality checks (Phase 1d) and the order book reconstructor (Phase 2) both
need these facts, at a point in time, through the same read path as the data.

## Decision

Add a fourth dataset, `stream_events`, holding one row per integrity event for
every venue.

- It is an ordinary dataset: bitemporal, append-only, partitioned by
  venue/instrument/date, written through `AppendOnlyStore` and read only
  through `PITSession` (the typed method `stream_events` and the SQL view of
  the same name).
- **The adapters write the events** while normalizing, in the same pass and
  the same batches as the market data they describe.
- Each row names the raw record it refers to with `recv_ts` and `seq`.
  `event_index` orders several events from one record, and the key is
  `(recv_ts, seq, event_index)`, so normalizing a raw log again corrects the
  events instead of duplicating them (as for book events, ADR 0009).
- `event_ts` is always the receipt time with `event_ts_source = "receipt"`.
  An integrity event is something our process observed, not something the
  venue timestamped (ADR 0008: the source follows the kind of row).
- Columns: `kind`, `connection` (which connection, `ws` or `probe`),
  `expected` and `observed` (two integers whose meaning depends on the kind)
  and `detail` (text).

| Kind | Written when | `expected` / `observed` | `detail` |
|------|--------------|-------------------------|----------|
| `SESSION_START`, `SESSION_STOP` | The recorder started or stopped | | Symbol, depth, recorder version |
| `CONNECT` | A connection was opened | | URL |
| `DISCONNECT` | A connection ended | | Reason |
| `SUBSCRIBE_REQUEST` | We asked for a channel | | The request |
| `SUBSCRIPTION_ACK` | The venue confirmed a subscription change | | What it confirmed |
| `TRADE_BACKFILL` | A trade message was history sent on subscribe | | |
| `SEQUENCE_GAP`, `SEQUENCE_REPEAT`, `SEQUENCE_OUT_OF_ORDER` | Coinbase's numbering broke on a connection | Sequence numbers | |
| `CHECKSUM_MISMATCH` | Kraken's checksum differs from the one computed from the rebuilt book | Venue's / computed | |
| `MESSAGE_REJECTED` | A record could not be normalized | | Reason |

A reconnect is not a kind of its own: it is a `CONNECT` on a connection that
was already connected in the session, normally preceded by its `DISCONNECT`.

**Checksum verification moves into the Kraken adapter.** To write
`CHECKSUM_MISMATCH` the adapter has to know the book, so it keeps the top of
each connection's book while normalizing, applies every message, and compares
checksums. After a mismatch that connection's book is suspect and nothing more
is reported for it until its next snapshot, so one fault is one event.

## Alternatives considered

- **Keep integrity facts in the normalization report only.** Nothing to
  build, but they are lost when the process exits, are not point-in-time, and
  are out of reach of anything that reads through `PITSession`.
- **Add columns to `book_events`** (a "gap before this row" flag). Cannot
  express events that have no book row: a reconnect, a rejected message, a gap
  that falls on a trade or heartbeat.
- **One dataset per kind of event.** More schemas and views for a few rows
  each, and every reader wants them merged in time order anyway.
- **Store every Coinbase message's sequence number** (including heartbeats)
  and let readers find the gaps. Faithful, but pushes the venue's rule into
  every reader and stores thousands of rows to say "nothing happened".
- **Compute Kraken's checksum only in the Phase 2 reconstructor,** as the
  roadmap first planned, and have it write the events. Then the events would
  not exist until a second pass ran, the store would have a second writer, and
  the 1d checks could say nothing about Kraken. The cost of this decision is
  that the adapter now contains a small book of its own; see Consequences.
- **A JSON column for the event's fields** instead of `expected`, `observed`
  and `detail`. More flexible, but opaque to SQL, and the SQL allow-list has
  no JSON functions.

## Consequences

- The quality check for stream integrity reads this dataset for both venues.
  The roadmap's earlier plan, that Kraken's part waits for Phase 2, no longer
  applies.
- **There are now two implementations of Kraken's book rule:** the adapter's
  (about 30 lines, top of book only, there to produce events) and the Phase 2
  reference reconstructor. That is deliberate redundancy, like the C++ book
  being checked against the reference. Phase 2 must still verify the checksum
  itself and must also treat a stored `CHECKSUM_MISMATCH` as a reason to
  raise; the two should agree, and a test should say so.
- A clean stream produces a handful of rows per session. A badly broken one
  produces one row per fault, not one per message.
- `CHECKSUM_MISMATCH` inherits the limits of Kraken's checksum. It covers ten
  levels per side and is checked after the next message, so a lost update is
  caught only if its level is still wrong and in the top ten by then. On the
  recorded fixture, of 250 single updates removed one at a time, 53% were
  caught at the next message, 11% later, and 36% never, because a later
  update overwrote the level first. An absence of events is therefore not
  proof that no update was lost.
- An update that arrives before any snapshot on its connection cannot be
  checked and produces no event.
- Events noticed before a session start record have no venue and instrument
  to be stored under. They stay in the normalization report only.
- `TRADE_BACKFILL` is the only way to tell backfilled trades from live ones.
  Anything that treats a trade's `recv_ts` as "when we saw it happen" (the
  clock skew check, and later any simulation of what was known when) must
  exclude the records it marks.

## Review

The owner decided on the dataset, that it covers sequence gaps, reconnects,
subscription changes and checksum mismatches for every venue, that each event
refers to its raw record, that it is bitemporal and read through `PITSession`,
and that the adapters write it. Added during implementation, for the owner to
confirm or strike:

- the kinds `SESSION_START`, `SESSION_STOP`, `MESSAGE_REJECTED` and
  `TRADE_BACKFILL`, which the list did not name;
- the column layout (`expected`, `observed`, `detail`, `event_index`) and the
  receipt-time rule;
- verifying Kraken's checksum inside the adapter, which follows from "the
  adapters write these events" but puts book logic ahead of Phase 2.
