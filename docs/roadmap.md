# Roadmap

Each phase is sized for one working session and ends with `scripts/check.ps1`
green and a merged PR. Phase 1 is too large for one session, so it is split
into 1a to 1d; each of those is one session with its own PR. Do phases in
order unless a phase says otherwise. Do not start a phase whose prerequisites
are not checked off.

Everything is built fresh in this repository. There is no earlier codebase to
import.

**Every phase, in addition to its own criteria:**

- [ ] `scripts/check.ps1` passes locally and CI is green on the PR.
- [ ] New behavior has tests; no existing test was deleted or weakened to pass.
- [ ] This file's status table is updated, and any new major decision has an ADR.

**Ask the owner** marks a decision that is hard to reverse. Stop and ask
before making it.

## Status

| Phase | Title | Status |
|-------|-------|--------|
| 0 | Skeleton and design docs | Done |
| 1a | Data core: bitemporal schema and append-only store | Not started |
| 1b | Data core: point-in-time query session | Not started |
| 1c | Data core: raw capture, fixtures, Binance and Coinbase adapters | Not started |
| 1d | Data core: quality checks | Not started |
| 2 | Python reference order book reconstructor | Not started |
| 3 | C++ core types | Not started |
| 4 | C++ order book, verified | Not started |
| 5 | SPSC queue and threading primitives | Not started |
| 6 | C++ feed handlers (offline) | Not started |
| 7 | Deterministic replay engine | Not started |
| 8 | Simulated exchange and cost model | Not started |
| 9 | Pre-trade risk checks and kill switch | Not started |
| 10 | Order state machine and paper gateway | Not started |
| 11 | Live market data connectivity | Not started |
| 12 | Features: order flow imbalance and technicals | Not started |
| 13 | Validation harness | Not started |
| 14 | Regime detection and the blended model | Not started |
| 15 | Risk/portfolio layer | Not started |
| 16 | Paper trading end to end (crypto) | Not started |
| 17 | US equities data via Alpaca | Not started |
| 18 | Alpaca paper execution | Not started |
| 19 | Agent tool server and audit log | Not started |
| 20 | Agent research loop | Not started |
| 21 | Agent monitoring | Not started |
| 22 | Live trading readiness review | Not started |

---

## Part A: Data core and reference book

### Phase 0: Skeleton and design docs (done)

- [x] `docs/architecture.md`, `docs/adr/0001` to `0007`, this roadmap, `CLAUDE.md`.
- [x] `cpp/` builds on Windows with CMake presets and the vcpkg manifest.
- [x] GoogleTest and Google Benchmark binaries build and run.
- [x] `quantagent._core` imports from Python and returns a value from C++.
- [x] `scripts/check.ps1` green; CI workflow present.

### Phase 1: Python data core

Built from scratch per `docs/architecture.md` sections 3.2, 6 and 7 and ADRs
0005 and 0006, as the subpackage `python/src/quantagent/data/`. Four
sessions, in order.

#### Phase 1a: Bitemporal schema and append-only store

- [ ] Schemas for trades, book events (snapshots and deltas) and instrument
      metadata. Every row carries `event_ts`, `recv_ts` and `knowledge_ts` as
      `int64` nanoseconds, and prices and quantities as `int64` mantissas
      (ADR 0005).
- [ ] Parquet store partitioned by venue/instrument/date (ADR 0006), with a
      writer whose only operation is append. `knowledge_ts` is assigned by
      the writer, not supplied by the caller, and never decreases.
- [ ] The writer API has no update, overwrite or delete. A test shows that
      appending never changes the bytes of an existing data file.
- [ ] A correction is a new row with the same key and a later
      `knowledge_ts`; the original row is still on disk afterwards (tested).
- [ ] Instrument metadata (price and quantity exponents, tick size, lot
      size) is stored the same bitemporal way.
- [ ] Decimal string to mantissa conversion is exact and never goes through
      `float`; it rejects values with more precision than the instrument's
      exponent. Tested with edge cases, including very small prices and very
      large quantities.
- [ ] Runtime dependencies (proposal: `pyarrow`, `duckdb`) added to
      `python/pyproject.toml`.

#### Phase 1b: Point-in-time query session

- [ ] `PITSession(as_of=...)` is the only public read path. `as_of` is a
      required keyword with no default; constructing a session without it is
      an error (tested).
- [ ] Typed query methods for trades, book events and instrument metadata.
      Every one filters on `knowledge_ts <= as_of`, and returns the latest
      known version of each record as of that time.
- [ ] Look-ahead property test: rows are inserted with random
      `knowledge_ts`, sessions opened at random `as_of`, and no returned row
      ever has `knowledge_ts > as_of`, through any method.
- [ ] Correction test: a corrected record is visible only to sessions with
      `as_of` at or after the correction's `knowledge_ts`; earlier sessions
      see the original.
- [ ] If the session offers SQL, it accepts exactly one `SELECT` statement
      over the session's as-of-filtered views and nothing else. Each of
      these is rejected with a test per case:
  - DDL and DML (`CREATE`, `DROP`, `ALTER`, `INSERT`, `UPDATE`, `DELETE`,
    `COPY`, `ATTACH`, `PRAGMA`, `SET`, `INSTALL`, `LOAD`);
  - multi-statement input, including a second statement hidden after a
    semicolon, a comment or a string literal;
  - raw file access: table functions and replacement scans that read a path
    or URL (`read_parquet`, `read_csv`, a quoted file name used as a table,
    and similar);
  - references to the unfiltered base tables.
- [ ] Rejection is by parsing the statement and allow-listing what it may
      contain, not by searching the text for forbidden words.
- [ ] The session does not expose its database connection, the store's file
      paths, or any object from which they can be reached through public
      attributes.
- [ ] A session's `as_of` cannot be changed after construction. (The
      backtest harness, in a later phase, creates a new session per decision
      time.)

#### Phase 1c: Raw capture, fixtures, Binance and Coinbase adapters

- [ ] Raw log writer and reader per ADR 0006 (length-prefixed records with
      `recv_ts`, `seq`, venue, channel, payload bytes). Round-trip test,
      including a truncated final record.
- [ ] A recorder CLI captures Binance and Coinbase public L2 depth (the
      initial snapshot plus diffs) and trades into raw logs. Public market
      data only; no API keys.
- [ ] Recorded: at least one continuous hour for one instrument per venue,
      including the snapshot needed to seed a book and at least one later
      venue snapshot for verification. Stored outside git.
- [ ] A small fixture per venue (a few minutes, a few MB at most,
      compressed) committed under `python/tests/fixtures/`, with a README
      giving venue, instrument, time range and how it was captured.
- [ ] Binance and Coinbase adapters parse raw payloads into the Phase 1a
      schemas: exact integer prices and quantities, `event_ts` from the
      venue, `recv_ts` from the raw record, and each venue's sequence fields
      preserved.
- [ ] Adapter tests run against the committed fixtures, offline: every
      fixture message parses, row counts match message counts, and a set of
      hand-checked messages produce exactly the expected rows.
- [ ] Malformed or unknown messages are counted and reported, never
      silently dropped and never a crash.
- [ ] End to end: fixture raw log through adapter into the store, then read
      back through `PITSession`, gives the same rows.
- [ ] Tests that need the network (the recorder against live endpoints) are
      marked and excluded from the default run.

#### Phase 1d: Quality checks

Each check is a function over data read through `PITSession` that returns
structured findings (what, where, severity). Each has one test on clean
fixture data that finds nothing and one on a deliberately corrupted copy
that finds the planted defect.

- [ ] **Sequence gaps:** missing, duplicated or out-of-order venue sequence
      numbers, using each venue's own continuity rule.
- [ ] **Clock skew:** `recv_ts - event_ts` outside a configured band,
      including negative values (venue timestamp later than receipt), and
      `event_ts` going backwards within a stream.
- [ ] **Crossed books:** best bid at or above best ask in snapshot and
      top-of-book data. (Phase 2 applies the same check to the reconstructed
      book after every event.)
- [ ] **Outliers:** trade prices far from a rolling reference, and zero or
      negative prices and quantities. Thresholds are config, and a finding
      flags the row; it never deletes or alters it.
- [ ] A report command runs all checks over a date range and prints a
      summary. It exits non-zero when a finding at or above a chosen
      severity exists.
- [ ] `docs/data-core.md` written: schema, store layout, `PITSession` API
      and its restrictions, adapters, and what each quality check does.

---

### Phase 2: Python reference order book reconstructor

A simple, obviously correct L2 book in Python
(`python/src/quantagent/data/`), used as the oracle for the C++ order book.
Correctness over speed: plain dictionaries and sorted views, no
optimization.

- [ ] Builds a book from a venue snapshot and applies deltas, with integer
      prices and quantities. A level with zero quantity is removed.
- [ ] Enforces each venue's sequence rules exactly as that venue documents
      them (for Binance: which buffered diffs to discard after the snapshot,
      which diff must come first, and continuity between consecutive diffs;
      for Coinbase: its sequence continuity rule). The rules are cited in the
      code with a link to the venue documentation.
- [ ] **Raises on a gap.** A missing, duplicated or out-of-order update
      raises a specific exception carrying the expected and received
      sequence numbers. It never skips, repairs or continues silently.
      Tested with fixtures that have a message removed, duplicated and
      swapped.
- [ ] Applying a delta before a snapshot raises.
- [ ] Verified against exchange snapshots: over the Phase 1c recordings, the
      reconstructed book equals each later venue snapshot in the recording
      at the matching sequence point, on every level the snapshot contains.
      Any venue-provided checksum is also validated.
- [ ] The book is never crossed after any event on the fixtures (the Phase
      1d check, run per event).
- [ ] Unit tests on small hand-built sequences with hand-computed expected
      books: insert, update, delete, a delete of a missing level, and both
      sides.
- [ ] Golden files exported for Phase 4: for each fixture, top-of-book and
      top-20 depth after every event, in a documented format, committed
      alongside the fixtures (or generated deterministically by a test
      helper if too large to commit).

---

## Part B: C++ hot path

### Phase 3: C++ core types

- [ ] `Price`, `Qty` strong types, `Timestamp`, instrument id, instrument
      metadata, `Side`. No implicit conversion to floating point (verified by
      a compile-fail check or `static_assert` on convertibility).
- [ ] 128-bit notional helper that compiles on MSVC, with overflow tests.
- [ ] `MarketEvent` and order message structs: fixed size, trivially copyable
      (`static_assert`), size and layout documented.
- [ ] `Clock` interface with system and manual implementations.
- [ ] Types exposed in `quantagent._core`; Python round-trip tests.

### Phase 4: C++ order book, verified

- [ ] L2 order book: apply snapshot, apply delta, top-N query, best bid/ask,
      stale flag on sequence gap.
- [ ] Driven in tests from Python through the bindings, with events
      normalized by the Phase 1c adapters (C++ feed handlers come in
      Phase 6).
- [ ] Matches the Python reference reconstructor: top-of-book and top-20
      depth equal the Phase 2 golden files after every event, for both
      venues, with zero differences.
- [ ] Matches exchange snapshots: over the Phase 1c recordings, the book
      equals each later venue snapshot in the recording at the matching
      sequence point, and any venue checksum is validated where the venue
      provides one.
- [ ] Both comparisons run as tests in `scripts/check.ps1` on the committed
      fixtures.
- [ ] Gap handling tested: on the Phase 2 gap fixtures (message removed,
      duplicated, swapped) the book is marked stale at the same event where
      the reference reconstructor raises, and it recovers on the next
      snapshot.
- [ ] Zero heap allocations in steady state, asserted by a test that counts
      `operator new` calls after warm-up.
- [ ] Benchmark for apply-delta and top-of-book recorded in the PR as the
      baseline (numbers and machine).

### Phase 5: SPSC queue and threading primitives

- [ ] SPSC ring buffer per ADR 0004; single-thread unit tests for wrap,
      full and empty.
- [ ] Two-thread stress test: at least 10^8 sequenced items, none lost,
      duplicated or reordered.
- [ ] Seqlock snapshot primitive with a reader/writer stress test.
- [ ] Thread pinning and naming wrapper for Windows (and Linux, compiled if
      a Linux build exists), with a test that the affinity took effect.
- [ ] Reusable allocation guard (counting `operator new`) for tests.
- [ ] Queue throughput and latency benchmark recorded as baseline.

### Phase 6: C++ feed handlers (offline)

Parsing and sequencing only, driven by recorded bytes. No sockets yet.

- [ ] **Ask the owner** to confirm the JSON parser dependency (proposal:
      simdjson via vcpkg). Record in a new ADR.
- [ ] Binance and Coinbase handlers turn raw payloads into `MarketEvent`s
      with integer prices, never passing through `double`.
- [ ] Sequence continuity rules per venue, with gap detection and resync
      state machine, tested with gapped fixtures.
- [ ] Feeding the Phase 1c fixtures through handler then book gives the same
      Phase 2 golden result as Phase 4, and the handlers' events equal the
      Python adapters' rows for every message.
- [ ] Malformed input is rejected and counted, never crashes (a fuzz-style
      test with truncated and corrupted payloads).
- [ ] No allocation per message after warm-up.

### Phase 7: Deterministic replay engine

- [ ] Replay engine reads a raw log and drives handlers, books and a strategy
      callback on one thread in `(recv_ts, seq)` order, with time from the
      event stream.
- [ ] Journal writer: binary, append-only; reader in Python.
- [ ] Determinism test: two runs over the same log give byte-identical
      journals. Run in CI.
- [ ] Python binding: run a replay with a Python callback on the decision
      clock; the callback receives a `PITSession` whose `as_of` is simulated
      time.
- [ ] A test proves the callback cannot see data after simulated time.
- [ ] Replay speed recorded (events per second) as baseline.

### Phase 8: Simulated exchange and cost model

- [ ] Simulated venue in the replay engine: limit and market orders, matching
      against the recorded book, configurable order and data latency.
- [ ] Queue-position model for passive fills, documented with its
      assumptions and stated as conservative or optimistic.
- [ ] Cost model: maker/taker fees per venue from config, spread crossing,
      and slippage from walking the book. Config is versioned.
- [ ] Tests with hand-computed expected fills and costs for at least: market
      order through several levels, passive fill, partial fill, cancel
      before fill, order arriving after latency into a moved book.
- [ ] A backtest with zero latency and zero fees differs from the default
      configuration, proving the costs are applied.

### Phase 9: Pre-trade risk checks and kill switch

- [ ] Checks: kill switch, stale book, max order quantity, max position, max
      notional, price collar, order rate limit, hard loss stop.
- [ ] Limits come from a config file loaded at startup. No setter exists in
      C++ or in the bindings.
- [ ] Each check has a test that passes just inside the limit and rejects
      just outside it, and every reject is journaled with its reason.
- [ ] Structural test: the gateway's send function is only reachable through
      the risk check (enforced by type, for example a `RiskApproved` token
      only the checker can construct).
- [ ] Kill switch, once tripped, cannot be cleared from code paths reachable
      by bindings.
- [ ] Per-order check cost benchmarked.

### Phase 10: Order state machine and paper gateway

- [ ] Order lifecycle state machine (new, acked, partially filled, filled,
      cancelled, rejected) with tests for out-of-order and duplicate venue
      messages.
- [ ] Position and PnL accounting in integers, reconciled against fills in
      tests.
- [ ] Execution gateway interface with a paper implementation backed by the
      Phase 8 simulated venue.
- [ ] Target-following order management: given a `TargetSet`, works orders
      toward it through risk checks.
- [ ] No live venue adapter exists yet; the mode switch refuses `live`.

### Phase 11: Live market data connectivity

- [ ] **Ask the owner** to confirm the WebSocket/TLS dependency (proposal:
      Boost.Beast with OpenSSL via vcpkg). Record in a new ADR.
- [ ] Feed threads connect to Binance and Coinbase public streams, with
      reconnect, resubscribe and resync.
- [ ] Threads wired per architecture section 5: feed, core, journal, with
      pinning and wait strategy from config.
- [ ] Raw capture runs alongside; a recorded live session replays to the same
      book states the live core produced (journal comparison).
- [ ] Soak test: one hour live with zero unexplained gaps, queue high-water
      marks and latencies reported.
- [ ] Network tests are marked and excluded from the default `check.ps1`
      run; offline tests cover the logic.

---

## Part C: Signals, risk, paper trading

### Phase 12: Features: order flow imbalance and technicals

- [ ] Feature library with a registry: OFI (at least top-of-book and
      multi-level), spread, microprice, realized volatility, and a small set
      of bar technicals. Each feature declares its inputs and lookback.
- [ ] Features read data only through `PITSession`. Import-lint test added
      that fails if signal code imports storage internals.
- [ ] Each feature has a test on hand-computed values.
- [ ] Look-ahead test per feature: its value at time t is unchanged when all
      data after t is deleted.
- [ ] Features needed live have a C++ incremental implementation that matches
      the Python batch implementation on the fixture to a stated tolerance.

### Phase 13: Validation harness

- [ ] Walk-forward evaluation and purged k-fold cross-validation with
      embargo; a test shows that label overlap leaks without purging and does
      not with it.
- [ ] All performance numbers are net of the Phase 8 cost model. The harness
      has no option to report gross only.
- [ ] Experiment registry: spec, code version, data range, cost-model
      version, results, and a trial counter per hypothesis family.
- [ ] Reports include deflated Sharpe (or equivalent multiple-testing
      adjustment), turnover, capacity estimate and drawdown.
- [ ] A locked final holdout period is defined in config; the harness
      refuses to evaluate on it without an explicit human flag.
- [ ] Sanity tests: a random signal has no significant net edge; a signal
      with deliberate look-ahead is caught by the harness's leak check.

### Phase 14: Regime detection and the blended model

- [ ] **Ask the owner** to confirm the model family (proposal: regularized
      linear blend first, gradient-boosted trees second). Record in an ADR.
- [ ] Regime detector (volatility/liquidity states) fitted only on past data
      within each fold.
- [ ] One blended model combining microstructure, technical and regime
      inputs, producing `Signal` objects.
- [ ] Evaluated in the Phase 13 harness. The report states plainly whether
      there is a net-of-cost edge. A negative result is an acceptable
      outcome for this phase; a hidden one is not.
- [ ] Model artifacts are versioned and reproducible from the registry entry.

### Phase 15: Risk/portfolio layer

- [ ] Sizing from signals to target positions (proposal: volatility-scaled
      with caps), producing `TargetSet`.
- [ ] Exposure limits: per instrument, per venue, gross and net, from the
      same human-approved config as the C++ limits; consistency test between
      the two.
- [ ] Drawdown guard: at threshold, pause (no new entries, cancel resting
      orders), then recall (flatten under a liquidation policy, allocation to
      zero). Tested in replay with a synthetic losing path.
- [ ] Resume is only possible through a human-invoked CLI command and is
      journaled.
- [ ] Control plane: pause, flatten and snapshot through the bindings; no
      binding raises a limit.

### Phase 16: Paper trading end to end (crypto)

- [ ] One command starts paper trading: live data, C++ core, Python decision
      clock, paper gateway. Startup prints the mode; default is paper.
- [ ] Runs unattended for 24 hours without crash, unexplained gap or journal
      loss.
- [ ] Reconciliation: replaying the captured raw log through the backtest
      reproduces the paper session's orders and fills, with differences
      explained.
- [ ] Killing the Python process leaves C++ risk checks and the hard loss
      stop working (tested).
- [ ] Daily report generated from the journal: PnL, fills, slippage versus
      model, risk events.

### Phase 17: US equities data via Alpaca

- [ ] Alpaca market data adapter into the data core (paper/free endpoints).
- [ ] Trading calendar and session handling; instrument metadata for
      equities.
- [ ] Corporate actions (splits, dividends) stored bitemporally; adjusted
      series computed point-in-time, with a test around a real split.
- [ ] Quality checks extended for equities.

### Phase 18: Alpaca paper execution

- [ ] Gateway adapter for Alpaca's paper endpoint behind the same interface
      as the crypto paper gateway.
- [ ] The base URL is checked at startup; a non-paper URL in paper mode is a
      fatal error.
- [ ] Equities paper session runs through one full market day.

---

## Part D: Agent layer

Do not start until Phase 16 is checked off: paper trading must work without
the agent.

### Phase 19: Agent tool server and audit log

No LLM in this phase.

- [ ] Tool server implementing exactly the tools in architecture section 8.
- [ ] Audit log: append-only, written before the effect; a failed write
      fails the call.
- [ ] Permission test suite: for each forbidden operation in section 8.2, a
      test shows it is unreachable from the agent process (no tool, no
      import path, no credentials in its environment).
- [ ] `request_pause` works end to end in paper mode and can only be lifted
      by the human CLI.
- [ ] Holdout period and trial budget are enforced server-side.

### Phase 20: Agent research loop

- [ ] **Ask the owner** to confirm model, budget per loop and how reports are
      delivered.
- [ ] Agent process runs propose, test, report using only the tool server.
      Model: the latest Claude model via the Anthropic API.
- [ ] Every action appears in the audit log with reasoning.
- [ ] A loop with a scripted fake LLM is tested in CI (no network).
- [ ] Prompt-injection test: data fields containing instructions do not
      change which tools are available or called outside the allow-list.
- [ ] Spend and iteration caps enforced outside the model.

### Phase 21: Agent monitoring

- [ ] Agent periodically reviews paper performance and health via read tools
      and writes a report.
- [ ] Defined triggers (for example slippage far above model, feed
      degradation) lead to `raise_flag` or `request_pause`, tested with
      injected scenarios in replay.
- [ ] Monitoring runs outside the tick loop; a test shows core latency is
      unchanged with the agent running or killed.

### Phase 22: Live trading readiness review

A human gate, not a coding phase.

- [ ] **Ask the owner.** Nothing in this phase proceeds without explicit
      instruction.
- [ ] Checklist reviewed with the owner: paper track record, reconciliation
      quality, limits file, kill switch drill, key handling, venue terms.
- [ ] Live gateway adapter and the three-part opt-in (architecture section 9)
      implemented and tested against a venue testnet or minimum size.
- [ ] Live mode remains off by default after this phase.
