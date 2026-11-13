# 0009: Venue-neutral book events, with Kraken as the second venue

Status: Accepted (confirmed by the owner as written, 2026-11-11), amended by 0010 and 0013
Date: 2026-11-11
Amends: 0006

## Context

The owner replaced Binance with Kraken as the second crypto venue, because
Binance.com refuses API access from US addresses (HTTP 451).

The Phase 1a `book_events` schema had been designed around venues that number
their book messages: `venue_seq_last` was required, and a level's identity was
`(kind, venue_seq_last, side, price)`. Checking the two venues' real behavior
before writing adapters showed that this does not hold:

- **Kraken** (WebSocket v2 `book`) has no sequence numbers at all. It protects
  the book with a CRC32 checksum of the top 10 levels per side, sent with every
  snapshot and update. The book is truncated to the subscribed depth, and one
  update message can change the same price more than once.
- **Coinbase** (Advanced Trade `level2`) has `sequence_num`, but it counts
  every message on a connection across all channels, so it restarts on each
  connection and is not specific to the book.

Verifying a reconstructed book also needs an independent image of the venue's
book to compare against, and neither venue offers one that shares numbering
with the stream.

## Decision

**Book events**

- A book level's identity is where it came from: `(recv_ts, seq, level_index)`,
  the raw log record and the level's position in that message. `level_index`
  preserves the venue's order within a message.
- The venue's integrity information is carried in optional columns, and each
  venue fills the ones it has: `venue_seq_first`, `venue_seq_last` (message
  numbering), `checksum` (the venue's checksum of its book after the message)
  and `depth` (levels per side the venue keeps; null means the full book).
- Nothing is synthesized. Kraken rows have null sequence columns; Coinbase rows
  have a null checksum and depth.

**Independent snapshots**

- The recorder periodically opens a second, short-lived connection and records
  the fresh book snapshot it gets (raw log channel `probe`).
- These are stored in `book_events` with their own kind, `CHECK_SNAPSHOT`, so
  they travel with the data they verify but are never mistaken for part of the
  update stream.
- A check snapshot is matched to the stream by venue time: the stream replayed
  up to the last message whose `event_ts` is at or before the snapshot's. On
  the recorded fixtures this reproduces the snapshot exactly on both venues.

**Instrument names**

- An instrument is named `BASE-QUOTE` (`BTC-USD`) in the store on every venue.
  Kraken's `BTC/USD` is mapped by replacing `/` with `-`, since the name is
  also a directory.

## Alternatives considered

- **Keep `venue_seq_last` required and invent a number for Kraken** (a counter
  per connection, or the timestamp). The column would then mean different
  things per venue, and a made-up sequence number looks like evidence of
  continuity that the venue never gave.
- **Keep the key `(kind, venue_seq, side, price)`.** Not unique on Kraken even
  within one message, and meaningless without a sequence number.
- **Key by venue timestamp, side and price.** Venue timestamps are not
  guaranteed unique per update, and two changes to one price in one message
  share a timestamp.
- **One book table per venue.** Each could be exact for its venue, but every
  reader (session views, quality checks, the reconstructor, the C++ book
  verification) would need per-venue code paths for what is the same concept.
- **A separate dataset for check snapshots.** Cleaner separation, at the cost
  of a second dataset and view with an identical schema. A kind value is
  enough, and a reader that wants only the stream filters on it.
- **Verify against REST order book snapshots instead of a second WebSocket
  connection.** REST snapshots are depth-limited or rate-limited on both
  venues and have the same lack of shared numbering. The WebSocket snapshot is
  the full subscribed book and carries a venue timestamp.
- **Stay with Binance through a non-US endpoint or a third-party relay.** Not
  an option for a US-based operator.

## Consequences

- Normalizing the same raw log again yields the same keys, so the rows become
  corrections in the bitemporal store rather than duplicates. This is what
  makes it safe to fix an adapter bug and re-ingest.
- The key ties a row to our own capture. Book data from another source (a
  vendor file, say) needs its own stable `(recv_ts, seq)` assignment when it is
  ingested.
- Integrity checking is per venue. Kraken's checksum covers only the top 10
  levels: a lost update below them is invisible until it surfaces, and
  applying one update twice changes nothing. The reconstructor and the C++
  book must implement both rules, and the architecture's "a gap marks the
  book stale" applies to a checksum mismatch as well.
- Coinbase's numbering cannot be checked from `book_events` alone, because
  other channels use the same numbers. The adapter detects breaks while
  normalizing; persisting them is an open decision recorded in the roadmap.
- All three datasets moved to schema version 2 in the same change (with ADR
  0008's `event_ts_source`). No data had been stored under version 1.

## Review

The venue choice was the owner's. The schema rework was made while
implementing Phase 1c, on the strength of the Phase 1a note that the book
event key was provisional until checked against real venues. The owner
confirmed it as written on 2026-11-11.

The open decision named under Consequences, where Coinbase's numbering breaks
are persisted, was settled by ADR 0010 (`stream_events`).
