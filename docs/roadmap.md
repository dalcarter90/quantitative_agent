# Roadmap

Each phase is sized for one working session and ends with `scripts/check.ps1`
green and a merged PR. Do phases in order unless a phase says otherwise. Do
not start a phase whose prerequisites are not checked off.

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
| 1 | Import `quantlab` as the data core | Blocked: needs the `quantlab` source path |
| 2 | Data core: reconcile representation and storage | Not started |
| 3 | Raw capture and recorded fixtures | Not started |
| 4 | C++ core types | Not started |
| 5 | C++ order book, verified | Not started |
| 6 | SPSC queue and threading primitives | Not started |
| 7 | C++ feed handlers (offline) | Not started |
| 8 | Deterministic replay engine | Not started |
| 9 | Simulated exchange and cost model | Not started |
| 10 | Pre-trade risk checks and kill switch | Not started |
| 11 | Order state machine and paper gateway | Not started |
| 12 | Live market data connectivity | Not started |
| 13 | Features: order flow imbalance and technicals | Not started |
| 14 | Validation harness | Not started |
| 15 | Regime detection and the blended model | Not started |
| 16 | Risk/portfolio layer | Not started |
| 17 | Paper trading end to end (crypto) | Not started |
| 18 | US equities data via Alpaca | Not started |
| 19 | Alpaca paper execution | Not started |
| 20 | Agent tool server and audit log | Not started |
| 21 | Agent research loop | Not started |
| 22 | Agent monitoring | Not started |
| 23 | Live trading readiness review | Not started |

---

## Part A: Data core

### Phase 0: Skeleton and design docs (done)

- [x] `docs/architecture.md`, `docs/adr/0001` to `0007`, this roadmap, `CLAUDE.md`.
- [x] `cpp/` builds on Windows with CMake presets and the vcpkg manifest.
- [x] GoogleTest and Google Benchmark binaries build and run.
- [x] `quantagent._core` imports from Python and returns a value from C++.
- [x] `scripts/check.ps1` green; CI workflow present.

### Phase 1: Import `quantlab` as the data core

Prerequisite: the owner supplies the path to `quantlab`. It was not on the
machine when Phase 0 was done.

Copy only. The original directory is never modified, moved or deleted.

- [ ] `quantlab` source copied to `python/src/quantlab/` and its tests to
      `python/tests/quantlab/`, keeping the package name. Its dependencies
      added to `python/pyproject.toml`.
- [ ] All of its tests (expected: 31) pass under this repo's `pytest`, with no
      edits to test logic. If the count differs from 31, say so in the PR.
- [ ] `git status` in the original `quantlab` directory is unchanged (or, if
      it is not a repo, file hashes before and after match).
- [ ] No secrets, `.env` files or data files were copied in.
- [ ] `docs/data-core.md` written: schema, how the three timestamps are stored,
      `PITSession` API, storage engine and file layout, price/quantity/time
      types, adapter coverage, what the quality checks check, what the
      reconstructor consumes and produces.
- [ ] A comparison table in that doc: `quantlab` as-is versus ADR 0005 and
      ADR 0006, with each difference listed.
- [ ] **Ask the owner** how to resolve each difference. Record the answers by
      moving ADR 0005 and 0006 to Accepted or superseding them.

### Phase 2: Data core: reconcile representation and storage

Scope depends on Phase 1's answers. Skip items already true.

- [ ] Prices, quantities and timestamps at the store boundary follow the
      accepted versions of ADR 0005/0006.
- [ ] Instrument metadata (exponents, tick size, lot size) is bitemporal and
      queryable through `PITSession`.
- [ ] Look-ahead test: a property-based test inserts rows with random
      `knowledge_ts`, queries at random `as_of`, and asserts no returned row
      has `knowledge_ts > as_of`.
- [ ] Correction test: a corrected record is visible only for `as_of` at or
      after the correction's `knowledge_ts`; the original stays visible before.
- [ ] `PITSession` cannot be constructed without `as_of`.
- [ ] The original `quantlab` tests still pass (updated only where a
      reconciled type changed, each such change listed in the PR).

### Phase 3: Raw capture and recorded fixtures

- [ ] Raw log writer/reader in Python per ADR 0006 (length-prefixed records
      with `recv_ts`, `seq`, venue, channel, payload). Round-trip test.
- [ ] A recorder CLI captures Binance and Coinbase public L2 depth (snapshot
      plus diffs) and trades to raw logs. Public data only, no keys.
- [ ] Recorded: at least one continuous hour for one instrument per venue,
      including the REST/initial snapshot needed to seed a book, stored
      outside git.
- [ ] A small fixture (a few minutes, a few MB at most, compressed) committed
      under `python/tests/fixtures/` with a README stating venue, instrument,
      time range and how it was captured.
- [ ] The Python reconstructor rebuilds the book from the fixture with no
      sequence gaps, and the quality checks pass on it.
- [ ] Expected top-of-book after every event exported from the Python
      reconstructor as a golden file for Phase 5.

---

## Part B: C++ hot path

### Phase 4: C++ core types

- [ ] `Price`, `Qty` strong types, `Timestamp`, instrument id, instrument
      metadata, `Side`. No implicit conversion to floating point (verified by
      a compile-fail check or `static_assert` on convertibility).
- [ ] 128-bit notional helper that compiles on MSVC, with overflow tests.
- [ ] `MarketEvent` and order message structs: fixed size, trivially copyable
      (`static_assert`), size and layout documented.
- [ ] `Clock` interface with system and manual implementations.
- [ ] Types exposed in `quantagent._core`; Python round-trip tests.

### Phase 5: C++ order book, verified

- [ ] L2 order book: apply snapshot, apply delta, top-N query, best bid/ask,
      stale flag on sequence gap.
- [ ] Verified against recorded exchange data: over the whole Phase 3
      fixture, the book's top 20 levels match the venue's own later snapshots
      wherever the recording contains them, and any venue checksum is
      validated where the venue provides one.
- [ ] Verified against the Python reconstructor: top-of-book and top-20
      depth equal the Phase 3 golden file after every event, for both venues.
- [ ] Gap handling tested: a fixture with a removed message marks the book
      stale and it recovers on the next snapshot.
- [ ] Zero heap allocations in steady state, asserted by a test that counts
      `operator new` calls after warm-up.
- [ ] Benchmark for apply-delta and top-of-book recorded in the PR as the
      baseline (numbers and machine).

### Phase 6: SPSC queue and threading primitives

- [ ] SPSC ring buffer per ADR 0004; single-thread unit tests for wrap,
      full and empty.
- [ ] Two-thread stress test: at least 10^8 sequenced items, none lost,
      duplicated or reordered.
- [ ] Seqlock snapshot primitive with a reader/writer stress test.
- [ ] Thread pinning and naming wrapper for Windows (and Linux, compiled if
      a Linux build exists), with a test that the affinity took effect.
- [ ] Reusable allocation guard (counting `operator new`) for tests.
- [ ] Queue throughput and latency benchmark recorded as baseline.

### Phase 7: C++ feed handlers (offline)

Parsing and sequencing only, driven by recorded bytes. No sockets yet.

- [ ] **Ask the owner** to confirm the JSON parser dependency (proposal:
      simdjson via vcpkg). Record in a new ADR.
- [ ] Binance and Coinbase handlers turn raw payloads into `MarketEvent`s
      with integer prices, never passing through `double`.
- [ ] Sequence continuity rules per venue, with gap detection and resync
      state machine, tested with gapped fixtures.
- [ ] Feeding the Phase 3 fixture through handler then book gives the same
      golden result as Phase 5.
- [ ] Malformed input is rejected and counted, never crashes (a fuzz-style
      test with truncated and corrupted payloads).
- [ ] No allocation per message after warm-up.

### Phase 8: Deterministic replay engine

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

### Phase 9: Simulated exchange and cost model

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

### Phase 10: Pre-trade risk checks and kill switch

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

### Phase 11: Order state machine and paper gateway

- [ ] Order lifecycle state machine (new, acked, partially filled, filled,
      cancelled, rejected) with tests for out-of-order and duplicate venue
      messages.
- [ ] Position and PnL accounting in integers, reconciled against fills in
      tests.
- [ ] Execution gateway interface with a paper implementation backed by the
      Phase 9 simulated venue.
- [ ] Target-following order management: given a `TargetSet`, works orders
      toward it through risk checks.
- [ ] No live venue adapter exists yet; the mode switch refuses `live`.

### Phase 12: Live market data connectivity

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

### Phase 13: Features: order flow imbalance and technicals

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

### Phase 14: Validation harness

- [ ] Walk-forward evaluation and purged k-fold cross-validation with
      embargo; a test shows that label overlap leaks without purging and does
      not with it.
- [ ] All performance numbers are net of the Phase 9 cost model. The harness
      has no option to report gross only.
- [ ] Experiment registry: spec, code version, data range, cost-model
      version, results, and a trial counter per hypothesis family.
- [ ] Reports include deflated Sharpe (or equivalent multiple-testing
      adjustment), turnover, capacity estimate and drawdown.
- [ ] A locked final holdout period is defined in config; the harness
      refuses to evaluate on it without an explicit human flag.
- [ ] Sanity tests: a random signal has no significant net edge; a signal
      with deliberate look-ahead is caught by the harness's leak check.

### Phase 15: Regime detection and the blended model

- [ ] **Ask the owner** to confirm the model family (proposal: regularized
      linear blend first, gradient-boosted trees second). Record in an ADR.
- [ ] Regime detector (volatility/liquidity states) fitted only on past data
      within each fold.
- [ ] One blended model combining microstructure, technical and regime
      inputs, producing `Signal` objects.
- [ ] Evaluated in the Phase 14 harness. The report states plainly whether
      there is a net-of-cost edge. A negative result is an acceptable
      outcome for this phase; a hidden one is not.
- [ ] Model artifacts are versioned and reproducible from the registry entry.

### Phase 16: Risk/portfolio layer

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

### Phase 17: Paper trading end to end (crypto)

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

### Phase 18: US equities data via Alpaca

- [ ] Alpaca market data adapter into the data core (paper/free endpoints).
- [ ] Trading calendar and session handling; instrument metadata for
      equities.
- [ ] Corporate actions (splits, dividends) stored bitemporally; adjusted
      series computed point-in-time, with a test around a real split.
- [ ] Quality checks extended for equities.

### Phase 19: Alpaca paper execution

- [ ] Gateway adapter for Alpaca's paper endpoint behind the same interface
      as the crypto paper gateway.
- [ ] The base URL is checked at startup; a non-paper URL in paper mode is a
      fatal error.
- [ ] Equities paper session runs through one full market day.

---

## Part D: Agent layer

Do not start until Phase 17 is checked off: paper trading must work without
the agent.

### Phase 20: Agent tool server and audit log

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

### Phase 21: Agent research loop

- [ ] **Ask the owner** to confirm model, budget per loop and how reports are
      delivered.
- [ ] Agent process runs propose, test, report using only the tool server.
      Model: the latest Claude model via the Anthropic API.
- [ ] Every action appears in the audit log with reasoning.
- [ ] A loop with a scripted fake LLM is tested in CI (no network).
- [ ] Prompt-injection test: data fields containing instructions do not
      change which tools are available or called outside the allow-list.
- [ ] Spend and iteration caps enforced outside the model.

### Phase 22: Agent monitoring

- [ ] Agent periodically reviews paper performance and health via read tools
      and writes a report.
- [ ] Defined triggers (for example slippage far above model, feed
      degradation) lead to `raise_flag` or `request_pause`, tested with
      injected scenarios in replay.
- [ ] Monitoring runs outside the tick loop; a test shows core latency is
      unchanged with the agent running or killed.

### Phase 23: Live trading readiness review

A human gate, not a coding phase.

- [ ] **Ask the owner.** Nothing in this phase proceeds without explicit
      instruction.
- [ ] Checklist reviewed with the owner: paper track record, reconciliation
      quality, limits file, kill switch drill, key handling, venue terms.
- [ ] Live gateway adapter and the three-part opt-in (architecture section 9)
      implemented and tested against a venue testnet or minimum size.
- [ ] Live mode remains off by default after this phase.
