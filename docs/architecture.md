# Architecture

Status: design. `docs/roadmap.md` records which parts are built.
Decisions referenced here are recorded in `docs/adr/`.

## 1. Principles

1. **Paper by default.** Every entry point starts in paper mode. Live trading
   needs an explicit, multi-part opt-in (section 9).
2. **Point-in-time correctness is structural.** Research code cannot read data
   that was not known at the simulated time, because the only query interface
   requires an as-of time and filters on knowledge time.
3. **One code path for backtest, paper and live.** The same C++ order book, risk
   checks and order state machine run in all three. Only the event source and
   the venue endpoint change.
4. **Determinism.** Replaying the same recorded input produces a byte-identical
   output journal. No component on the hot path reads the system clock or any
   other ambient state directly.
5. **The LLM is never in the loop that trades.** The agent researches, monitors
   and can ask for a pause. It cannot place orders or loosen risk.
6. **Risk controls only fail safe.** Anything automated may reduce risk (pause,
   cancel, flatten). Only a human may increase it (raise limits, resume, go
   live).

A note on latency, so expectations are honest: crypto market data over public
WebSocket is bounded by internet latency (milliseconds). The hot-path
discipline below is there for determinism, predictable tail latency and
backtest fidelity. It does not make this system competitive with colocated
HFT, and nothing in the design assumes it is.

## 2. The five layers

```
                          cold path (Python)                      hot path (C++)
 ┌──────────────────────────────────────────────┐   ┌─────────────────────────────────────┐
 │ 5 Agent      research loops, monitoring       │   │                                     │
 │      │ tool server (allow-list, audit log)    │   │                                     │
 │      ▼                                        │   │                                     │
 │ 4 Risk/portfolio  sizing, exposure, drawdown  │──▶│ 1 Hot path                          │
 │      ▲            guard, control plane        │   │   feed handlers → order book →      │
 │      │ targets                                │   │   features → pre-trade risk →       │
 │ 3 Signals    OFI, technicals, regime, model   │◀──│   execution gateway                 │
 │      ▲                                        │   │   replay / backtest engine          │
 │      │ PITSession(as_of)                      │   │                                     │
 │ 2 Data core  bitemporal store, adapters, QC   │◀──│   journal + raw capture             │
 └──────────────────────────────────────────────┘   └─────────────────────────────────────┘
```

| # | Layer | Language | Owns | Must never |
|---|-------|----------|------|-----------|
| 1 | Hot path | C++20 | Feed handlers, order book, incremental features, pre-trade risk, order state, execution gateway, replay engine | Allocate, lock, throw, or read the wall clock on the core thread |
| 2 | Data core | Python | Raw capture ingestion, append-only bitemporal store, `PITSession`, exchange adapters, quality checks, reference order book reconstructor | Return a row whose knowledge time is after the session's as-of time |
| 3 | Signals | Python (features mirrored in C++) | Feature definitions, regime detection, the blended model, validation harness, cost model | Read data except through `PITSession`; report a result without costs |
| 4 | Risk/portfolio | Python control plane + C++ enforcement | Position sizing, exposure limits, drawdown guard, pause/recall, the only route to the order path | Be bypassed; have limits changed at runtime by software |
| 5 | Agent | Python | Research loop, monitoring, reports, pause requests, flags | Place orders, change limits, resume trading, touch exchange credentials |

The data core is built in this repository as `python/src/quantagent/data/`
(roadmap Phase 1): bitemporal schema, append-only store, `PITSession`,
Kraken and Coinbase adapters, and quality checks. A simple Python reference
order book reconstructor (roadmap Phase 2) lives beside it and is the oracle
the C++ order book is verified against.

## 3. Interfaces between layers

All cross-layer messages are plain data. There are no shared mutable objects
across a layer boundary.

### 3.1 Hot path ⇄ Python (pybind11 module `quantagent._core`)

Bindings are a cold-path interface. Python is never called from the live core
thread.

| Direction | Interface | Notes |
|-----------|-----------|-------|
| Python → C++ | `ReplayEngine(config).run(raw_log, strategy)` | Backtests. Runs the C++ core over a recorded log. `strategy` is called on decision-clock events (section 4.3). |
| Python → C++ | `OrderBook`, `apply(event)`, `top(n)` | For verification against the Python reference reconstructor and for research. |
| Python → C++ | `Engine.submit_targets(TargetSet)` | Live/paper. Enqueues onto the control SPSC queue. Non-blocking. |
| Python → C++ | `Engine.pause(scope, reason)`, `Engine.flatten(scope, reason)` | Risk-reducing commands only. There is no binding that raises a limit or resumes. |
| C++ → Python | `Engine.snapshot()` | Positions, open orders, PnL, book tops, feature values, health counters. Read through a seqlock, never blocks the core. |
| C++ → Python | Journal files | Binary, append-only. Python reads them after the fact for analysis and reconciliation. |

### 3.2 Data core → everything above it

```python
with PITSession(store_root, as_of=t) as s:   # as_of is a required keyword, no default
    s.trades(venue, instrument, start, end)  # only rows with knowledge_ts <= t
    s.book_events(venue, instrument, start, end)
    s.stream_events(venue, instrument, start, end)  # gaps, reconnects, checksum mismatches
    s.instruments(venue, instrument)         # every metadata version known at t
    s.instrument(venue, instrument, at=u)    # the version in effect at u, as known at t
    s.sql("SELECT ... FROM trades ...")      # one SELECT over the same views
```

`start` and `end` are a half-open range on `event_ts`. Each method returns the
latest version of each record known at `as_of`. A `bars` method is planned but
not built; it belongs with the bar features in the signal layer.

`PITSession` is the only read path for layers 3 and 5, and it blocks
look-ahead structurally rather than by convention:

- `as_of` is required and fixed for the life of the session.
- Every query runs against views already filtered to `knowledge_ts <= as_of`.
  The unfiltered tables are not reachable through the session.
- The store is append-only. A correction or backfill is a new row with a later
  `knowledge_ts`, so a session at an earlier `as_of` still sees what was known
  then.
- `sql` accepts exactly one `SELECT` statement over those views. DDL, DML,
  multi-statement input, and anything that reads a file path or URL
  (`read_parquet` and similar) are rejected. The check is an allow-list applied
  to DuckDB's own parse tree, and the session then executes that tree, so there
  is no second parser to disagree with the engine. Tables, functions, types and
  expression kinds not on the list are refused, as are `WITH` clauses.
- Independently of that check, each session's database is locked so it can
  open only the data files written at or before `as_of`, cannot write, attach
  or load anything, and cannot be reconfigured. A hole in the statement check
  would still not reach later data.
- The session does not hand out its database connection or the store's file
  paths, and error messages have store paths removed.

An import-lint test (added with the signal layer) additionally fails if layer
3 or 5 code imports storage internals. Inside a backtest, the harness creates a
session per decision time with `as_of` equal to simulated time; strategy code
never constructs one.

### 3.3 Signals → Risk/portfolio

`Signal{instrument, horizon, expected_return, confidence, model_version, as_of}`.
A signal is a forecast. It carries no size and no order.

### 3.4 Risk/portfolio → Hot path

`TargetSet{as_of, targets: [{instrument, target_position, urgency, max_slippage}]}`.
Layer 4 converts signals to target positions under sizing and exposure rules.
The hot path works orders towards the targets and applies pre-trade checks to
every single order regardless of where the target came from.

### 3.5 Agent → Risk/portfolio

Tool calls only, listed exhaustively in section 8.

## 4. Data flow: market event to order

### 4.1 Live and paper

1. **Receive.** A feed thread reads bytes from the venue socket and stamps
   `recv_ts` (wall clock) and a local sequence number.
2. **Capture.** The raw bytes, `recv_ts` and sequence number are pushed to the
   capture queue. The journal thread appends them to the raw log. This log is
   the source of truth for replay and for the data core.
3. **Normalize.** The feed thread parses the message into a fixed-size
   `MarketEvent` (book snapshot/delta, trade) with integer prices and
   quantities, and checks the venue's message numbering where it has one
   (Coinbase numbers every message on a connection). A gap marks the book
   stale and triggers a resync; a stale book produces no orders.
4. **Hand off.** The event goes onto that feed's SPSC queue to the core thread.
5. **Book update.** The core thread applies the event to the instrument's
   order book. Where the venue protects the book with a checksum instead of
   message numbers (Kraken), it is verified here, after the update; a
   mismatch marks the book stale in the same way.
6. **Features.** Incremental features (order flow imbalance, spread,
   microprice, rolling volatility) update in place.
7. **Order management.** The core compares current position and working
   orders against the latest `TargetSet` and decides on new/cancel/replace.
8. **Pre-trade risk.** Every outgoing order passes all checks: kill switch,
   book freshness, max order size, max position, max notional, price collar
   against the current book, order rate limit, and the hard loss stop. A
   failed check rejects the order and journals the reason. There is no code
   path from order creation to the gateway that skips this function.
9. **Send.** The order goes onto the SPSC queue to the venue's execution
   thread, which serializes and sends it. In paper mode the "venue" is the
   simulated exchange driven by the same live market data.
10. **Feedback.** Acks, fills and rejects come back on another SPSC queue. The
    core updates order state, position and PnL.
11. **Journal.** Every input event, decision, order, risk result and fill is
    journaled by the journal thread.

Separately, on the decision clock (for example once per second, or on bar
close), the Python side reads a snapshot, layer 3 produces signals, layer 4
produces a `TargetSet`, and it is enqueued for step 7. The tick loop never
waits for Python.

### 4.2 Backtest

The replay engine reads a raw log and feeds steps 3 to 11 on a single thread,
in `(recv_ts, seq)` order. The simulated exchange replaces the venue and
models latency, fees and queue position. Time comes from the events.

### 4.3 The decision clock

Strategy logic in Python runs on a decision clock, not per tick. In replay the
engine calls the Python strategy synchronously at each decision time, with a
`PITSession` whose `as_of` equals simulated time. Live, the same function runs
in the Python process on a timer. If inference later needs to be faster, the
fitted model is exported to a C++ evaluator on the core thread; the interface
(`TargetSet`) does not change.

## 5. Threading model (C++)

| Thread | Count | Does | Blocks? |
|--------|-------|------|---------|
| Feed | 1 per venue connection | Socket read, timestamp, parse, normalize, sequence check | On the socket only |
| Core | 1 | Books, features, order management, pre-trade risk, order/position state | Never. Busy-polls its input queues |
| Execution | 1 per venue | Serialize and send orders, read acks/fills | On the socket only |
| Journal | 1 | Drain journal and capture queues to disk | On disk I/O |
| Control | Python threads | Targets, pause/flatten, snapshots | Free to block; not on the hot path |

```
 feed[kraken] ───SPSC──┐                         ┌──SPSC──▶ exec[kraken]
 feed[coinbase] ─SPSC──┼──▶  CORE (single owner) ┼──SPSC──▶ exec[coinbase]
 exec acks/fills ─SPSC─┤     books, risk, orders │
 control cmds ───SPSC──┘                         └──SPSC──▶ journal ──▶ disk
                             snapshot (seqlock) ──▶ Python readers
```

Rules:

- **Single writer.** All trading state (books, orders, positions, risk
  counters) is owned by the core thread. Nothing else reads or writes it. This
  is what makes the core lock-free and deterministic.
- **Pinned threads.** Feed, core, execution and journal threads are pinned to
  configured cores (`SetThreadAffinityMask` on Windows,
  `pthread_setaffinity_np` on Linux) through one small wrapper. Pinning and the
  wait strategy (spin, yield, or sleep) are configuration, because a
  development laptop cannot spare a spinning core per thread.
- **SPSC queues only.** Every edge in the diagram is a bounded, lock-free
  single-producer single-consumer ring buffer of fixed-size trivially copyable
  messages (ADR 0004). The core merges its inputs in a fixed, deterministic
  order.
- **Backpressure is explicit.** A full market-data queue marks the affected
  book stale and forces a resync; data is never dropped silently. A full order
  queue rejects the order. A full journal queue trips the kill switch, because
  the system does not trade without an audit trail.
- **No allocation on the hot path.** Feed (after parse buffers are set up),
  core and execution threads allocate nothing after startup. Books, order
  pools and queues are sized and allocated at startup. This is enforced by a
  test binary that replaces global `operator new` with a counting version and
  asserts zero allocations across a steady-state replay.
- **No exceptions, no locks, no system clock** on the core thread. Errors are
  return values. Time comes from a `Clock` passed in: wall time live, event
  time in replay.
- **Snapshots via seqlock.** The core publishes a fixed-size snapshot struct;
  readers retry on a torn read. The writer never waits.

## 6. Price, quantity and timestamp representation

Details and alternatives are in ADR 0005.

### 6.1 Prices and quantities

- A price or quantity is an `int64` mantissa with a per-instrument decimal
  exponent: `value = mantissa × 10^-exp`. `price_exp` and `qty_exp` live in
  instrument metadata and are set to the finest precision the venue supports
  for that instrument.
- `Price` and `Qty` are distinct strong types in C++. They cannot be mixed or
  implicitly converted to floating point.
- Tick size and lot size are validation rules in instrument metadata (a valid
  price is a multiple of the tick), not the unit of representation. A venue
  changing a tick size therefore does not rescale history.
- Notional (`price × qty`) is computed in a 128-bit intermediate through one
  helper (`_mul128` on MSVC, `__int128` elsewhere), because the product can
  exceed 64 bits.
- No `double` appears in the book, order, risk or PnL path. Research code may
  convert to `float64` for features and models; that conversion is explicit
  and one-way. Order prices are produced from integers.
- Instrument metadata is itself bitemporal, so the exponents and tick sizes in
  force at any past time are recoverable.

### 6.2 Timestamps

All timestamps are `int64` nanoseconds since the Unix epoch, UTC.

| Field | Meaning | Set by |
|-------|---------|--------|
| `event_ts` | When the venue says it happened | The venue (its precision varies; stored as given, scaled to ns). If the message has no venue timestamp, it holds `recv_ts` (ADR 0008) |
| `event_ts_source` | Where `event_ts` came from: `"venue"` or `"receipt"` | The adapter, from the message type (ADR 0008; arrives with Phase 1c) |
| `recv_ts` | When our process received the bytes | Feed thread, wall clock |
| `knowledge_ts` | When the record became available to queries in the store | Data core at write time. A correction or backfill gets a new, later `knowledge_ts`; the old row is kept |
| `seq` | Local monotonic sequence number at receipt | Feed thread. Total order for replay is `(recv_ts, seq)` |

Latency measurements inside the process use a monotonic clock and are stored
as durations, never as wall times. Simulated "now" in a backtest is the
`recv_ts` of the event being processed plus modelled latency, so a strategy
sees data when we would have had it, not when the venue produced it.

## 7. Storage

Details and alternatives are in ADR 0006.

| Tier | Content | Format | Used by |
|------|---------|--------|---------|
| Raw log | Exact bytes received, with `recv_ts` and `seq` | Append-only length-prefixed binary, zstd per segment | Replay engine, normalizer. Never rewritten |
| Normalized store | Trades, book events, instrument metadata and stream integrity events (ADR 0010), all three timestamps; bars are computed on demand (Phase 12) | Parquet, partitioned by venue/instrument/date, integer mantissas | `PITSession`, via DuckDB |
| Journal | Every hot-path decision, order, risk result and fill | Append-only fixed-record binary | Reconciliation, audit, monitoring |
| Experiment registry | Hypotheses, configs, results, trial counts | DuckDB/SQLite file | Validation harness, agent |
| Audit log | Every agent tool call with its reasoning | Append-only JSON lines | Humans |

## 8. Agent tool surface

The agent is an LLM process whose only capabilities are the tools below,
served by a tool server in layer 4. Enforcement is structural (ADR 0007), not
a matter of prompting. Every call takes a required `reasoning` string and is
written to the audit log with timestamp, model id, tool, arguments, reasoning
and result before the call's effect is applied.

### 8.1 What the agent may call

| Tool | Effect | Class |
|------|--------|-------|
| `query_data(as_of, ...)` | Read market data through `PITSession`. `as_of` cannot exceed now and cannot reach into the locked holdout period | Read |
| `get_performance(scope, window)` | Paper/live PnL, fills, slippage, turnover | Read |
| `get_risk_state()` | Current limits, exposures, drawdown, pause state | Read |
| `get_system_health()` | Feed gaps, queue depths, latency, journal lag | Read |
| `list_experiments()`, `get_experiment(id)` | Read the experiment registry | Read |
| `propose_hypothesis(spec)` | Register a hypothesis before testing it. `spec` is declarative: features from the registered library, model parameters, universe, horizon | Research |
| `run_backtest(experiment_id)` | Run a registered experiment in the harness with the fixed cost model and validation protocol. Counts against a trial budget | Research |
| `write_report(experiment_id, body)` | Attach a report to an experiment | Research |
| `raise_flag(severity, subject)` | Notify the human operator | Control |
| `request_pause(scope)` | Ask layer 4 to pause trading for an instrument, strategy or everything. Always honored | Control |
| `propose_promotion(experiment_id)` | Open a human-review request to move a strategy to paper. Does not promote anything | Control |

### 8.2 What the agent may not do

There is no tool for any of these, and the process has no other way to do them:

- Place, cancel or modify an order, or submit a `TargetSet`.
- Change any risk limit, the cost model, the validation protocol or the trial
  budget.
- Resume trading after a pause, or flatten positions directly. (A pause it
  requested is lifted only by a human.)
- Switch from paper to live, or promote a strategy.
- Read exchange credentials. They are not in the agent process's environment.
- Write to the raw log, normalized store or journal.
- Query the locked final holdout period.
- Run arbitrary code, shell commands or network requests. New feature code
  goes through a human-reviewed pull request, not through the agent.
- Be called from, or call into, the tick loop. The agent runs in its own
  process and talks only to the tool server.

### 8.3 Why pause is allowed but resume is not

Pausing can cost opportunity but cannot lose money through new exposure, so a
false positive is cheap. Resuming, raising limits and going live can, so they
need a human. The same asymmetry applies to the drawdown guard.

## 9. Risk, pause and recall

- **Limits** are loaded from a human-approved config file at startup and are
  immutable while the process runs. Changing them means editing the file and
  restarting.
- **Two enforcement points.** Layer 4 (Python) applies portfolio-level sizing
  and exposure rules when building targets. The C++ pre-trade checks enforce
  hard per-order and per-position limits and a hard loss stop, and keep
  working if the Python process dies.
- **Drawdown guard.** When drawdown from the high-water mark crosses the
  configured threshold, layer 4 (a) pauses: no new entries, resting orders
  cancelled; then (b) recalls capital: positions are flattened under a
  liquidation policy and the strategy's allocation is set to zero. Resuming
  requires a human.
- **Kill switch.** A single flag checked first in pre-trade risk. Tripped by
  the hard loss stop, journal failure, an operator, or a pause with global
  scope.
- **Live opt-in.** Live trading requires all of: `QA_TRADING_MODE=live`,
  `QA_LIVE_TRADING_ACK` set to the documented phrase, and a `--live` flag on
  the command line. If any is missing the process runs in paper mode and says
  so at startup. Live venue endpoints are not constructed in paper mode.
