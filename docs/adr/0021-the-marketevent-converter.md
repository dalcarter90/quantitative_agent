# 0021: The converter from stored data to MarketEvents

Status: Accepted; implementation choices confirmed by the owner (2026-11-17, see "Review"); amended by 0022 (the converter also emits instrument updates)
Date: 2026-11-17
Amends: none (builds ADR 0020's converter rules; closes review D7 and T3)

## Context

ADR 0020 fixed what a `MarketEvent` stream looks like: book levels, framed
snapshots, and control events standing for the stored stream events a book
acts on, each naming its source. It left the converter that produces such a
stream from stored data to Phase 4, which drives the C++ order book with it
in tests. Phase 6's feed handlers must produce the same frames and control
events as this converter does, so its output is an interface, not a test
helper.

The owner split Phase 4 in two (2026-11-17): 4a is the converter, the review's
D7 (what `MarketEvent` and `InstrumentMeta` refuse) and the T3 test cleanup;
4b is the C++ book and its verification.

## Decision

1. **`quantagent.marketevents.convert(book_events, stream_events,
   instruments, *, instrument, sources=None)`** turns one instrument's stored
   data, as a `PITSession` returns it, into `MarketEvent`s by ADR 0020's
   rules. It reads nothing itself.
2. **It yields one `EventGroup(log_id, seq, step, events)` per step**, in the
   reference book's order (`book.trace`): records in `(log_id, seq)` order,
   and at one record its control events in `event_index` order before its
   levels. `step` is `message` (a stream message's levels, framed if a
   snapshot), `check` (a check snapshot, framed, on the probe source), or
   `stale` and `reset` (one control event each). The `message`, `stale` and
   `reset` groups are exactly the reference book's steps, one for one, so the
   C++ book can be compared with it after each.
3. **The input is the `book_events` table, not `BookMessage`s**, so each level
   keeps its own venue time and its `event_ts_source`. Grouping, the
   message checks and the metadata in force come from the reference's own
   `messages_from_table`, so the two cannot group rows differently.
4. **`SourceMap` numbers the sources** `(log_id, "ws")` and `(log_id,
   "probe")` in the order they are first used, from 0 (or a given first
   number), and maps each back. A harness shares one map across the
   conversions of every instrument, so events of one log name the same
   source. It refuses any other connection and a number beyond 32 bits.
5. **What a level carries:** the row's price, quantity, side, venue time,
   position and kind; the record's `recv_ts` and `seq`; the message's header
   on every level, each with its flag (`venue_seq_last` as `venue_seq`,
   `checksum`, `depth`); `kEventTsFromReceipt` if the row's venue time is the
   receipt time; `kLastInMessage` on the last. No `recv_mono` (the store keeps
   none) and no trade id.
6. **What a control event carries** (ADR 0020): the stored event's record,
   receipt time and `event_index`, the kind and reason of the same name, and,
   for all three sequence kinds (not only a gap), the venue number that
   arrived (`observed`) as `venue_seq`. Frame events are stamped like control
   events, with the level count.
7. **What it refuses, before yielding anything** (`ValueError`): a message
   with no metadata in force; an exponent outside 0 to 18 (review D7); an
   exponent different from the first message's (ADR 0019 fixes them per
   instrument, so two would make the mantissas mean two things); a depth
   above 65,535 (review D7: `MarketEvent::depth` is 16 bits, the schema's
   int32 and the adapters' limit of 100,000 are wider). A change of the
   venue's precision is not refused: the events do not depend on it.
8. **Trades are not converted.** Nothing uses `Trade` events yet.
9. **D7 in the bindings:** `MarketEvent::kKnownFlags` holds the seven
   assigned flags, and `from_bytes` refuses any other bit; the
   `InstrumentMeta` constructor refuses an exponent outside 0 to 18, as the
   schema does, before checking the precisions against it.

## Alternatives considered

- **`BookMessage`s as input.** The reference's input, so the two would share
  it exactly; but a `BookMessage` keeps only the latest venue time of its
  levels, and Coinbase stamps each update level with its own. Extending
  `BookMessage` would change the reference book for the converter's sake.
- **One flat list of events.** Simpler to produce, but every user would have
  to find the step boundaries again to compare the C++ book with the
  reference after each step.
- **Sources numbered by sorted `(log_id, connection)`.** Independent of the
  order of use, but it needs every source known before the first event, and
  a harness converting one instrument after another would renumber.
- **The converter in the data package.** It reads only what a session
  returns and imports the C++ core; the data core stays storage and its
  checks, and the spec does not grow a section for it.
- **Refuse a precision change.** The C++ book needs the precision in force to
  verify Kraken's checksum, but that is a question of how metadata reaches
  the book (Phase 4b), not of the events.
- **Venue number only on a gap**, as ADR 0020 words it ("a gap may say").
  The repeat and out-of-order events have the same field with the same
  meaning; leaving it off them would be a distinction without a use.

## Consequences

- Phase 4b drives the C++ book with these groups and compares it with the
  golden files and `integrity-points.json` after each `message`, `stale` and
  `reset` group; `check` groups feed the check-snapshot comparison.
- Phase 6's handlers are compared with this converter for frames and control
  events, event for event.
- A `MarketEvent` built from bytes now refuses the eighth flag bit, and an
  `InstrumentMeta` an exponent outside 0 to 18.
- Converting the Coinbase fixture (110,978 levels, 110,985 events) takes
  1.1 s on this machine, the Kraken fixture (18,605 levels) 0.24 s; the tests
  over whole Coinbase logs are marked `slow`.

## Review

Choices made while building it. **Confirmed by the owner, 2026-11-17: all
nine, as written** (ADR 0022). On item 7 the owner added a condition, recorded
in the roadmap's Phase 7 criteria: the converter must handle trades before
the replay engine needs them.

1. **The module and its name:** `quantagent.marketevents`, outside the data
   package; `convert`, `EventGroup`, `SourceMap`, the step names.
2. **The table as input** (decision 3), with the reference's grouping.
3. **Sources in first-use order from 0**, shared through a `SourceMap`, only
   `ws` and `probe`.
4. **The header on every level** and no `recv_mono`.
5. **The venue number on all three sequence events.**
6. **The refusals** of decision 7, all before the first group, and no refusal
   of a precision change.
7. **Trades left out.**
8. **D7's messages:** `from_bytes` names the flag byte; `InstrumentMeta`
   checks the exponent before the precision.
9. **Existing tests changed, none weakened.** `test_core_types.py`: two
   precision cases used exponent 19, which is now refused for itself; they
   use exponent 18, still testing precision 19, and the exponent bounds have
   their own test. T3 (the first commit): `test_schema.py`'s `ALL` holds all
   four datasets; in `test_a_receipt_row_must_hold_recv_ts_in_event_ts`,
   `stream_events` asserts that a venue-time row is refused instead of
   accepted, since its schema allows only receipt time; the version table
   gains `stream_events`; `test_no_key_depends_on_receive_time` loses the
   `TRADES` exemption; the identical-reload test in `test_log_id.py` is
   renamed to what it checks.
