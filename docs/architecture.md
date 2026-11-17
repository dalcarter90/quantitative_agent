# Architecture

Status: design. `docs/roadmap.md` records which parts are built.
Decisions referenced here are recorded in `docs/adr/`.

## 1. Principles

1. **Paper by default.** Every entry point starts in paper mode. Live trading
   needs an explicit, multi-part opt-in (section 9).
2. **Point-in-time correctness is structural.** Research code cannot read data
   that had not been received at the simulated time, because the only query
   interface requires that time and returns only rows received by it
   (ADR 0017).
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
 │      │ PITSession(available_at)               │   │                                     │
 │ 2 Data core  bitemporal store, adapters, QC   │◀──│   journal + raw capture             │
 └──────────────────────────────────────────────┘   └─────────────────────────────────────┘
```

| # | Layer | Language | Owns | Must never |
|---|-------|----------|------|-----------|
| 1 | Hot path | C++20 | Feed handlers, order book, incremental features, pre-trade risk, order state, execution gateway, replay engine | Allocate, lock, throw, or read the wall clock on the core thread |
| 2 | Data core | Python | Raw capture ingestion, append-only bitemporal store, `PITSession`, exchange adapters, quality checks, reference order book reconstructor | Return a row received after the session's `available_at`, from a later version than its `version`, or from a batch that was not committed |
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
# available_at is a required keyword, no default. version is optional.
with PITSession(store_root, available_at=t, version=None) as s:
    s.trades(venue, instrument, start, end)  # only rows with recv_ts <= t
    s.book_events(venue, instrument, start, end)
    s.stream_events(venue, instrument, start, end)  # gaps, reconnects, checksum mismatches
    s.trade_observations(venue, instrument, start, end)  # every observation of each trade
    s.instruments(venue, instrument)         # every metadata version received by t
    s.instrument(venue, instrument, at=u)    # the version in effect at u (default t)
    s.instrument_for(venue, instrument, log_id=l, seq=n)  # the version a stored row was written with
    s.sql("SELECT ... FROM trades ...")      # one SELECT over the same views
```

`docs/data-core-spec.md` is the full description of the data core; this
section is how the rest of the system uses it.

A session has two cutoffs, because it answers two questions (ADR 0017):

| Cutoff | Question | Rule |
|--------|----------|------|
| `available_at`, required | What had we received? | Only rows with `recv_ts <= available_at` are visible. A backtest sets it to simulated time |
| `version`, optional | Which version of the store? | Among the rows available, each record's latest version with `knowledge_ts <= version`. Defaults to the latest committed batch, and cannot be later than it |

`as_of`, the single knowledge-time cutoff sessions had before, is retired:
passing it is an error that names these two.

`start` and `end` are a half-open range on `recv_ts`, for every dataset: a
window selects market data and the integrity events about it by the same
clock. Venue time (`event_ts`) is a column to filter on through `sql`, never a
cutoff. A `bars` method is planned but not built; it belongs with the bar
features in the signal layer.

`PITSession` is the only read path for layers 3 and 5, and it blocks
look-ahead structurally rather than by convention:

- `available_at` is required and fixed for the life of the session. So is the
  version, once the session is open.
- Every query runs against views already filtered to
  `recv_ts <= available_at` and `knowledge_ts <= version`. The unfiltered
  tables are not reachable through the session.
- Only committed batches exist for a session. A batch's files are readable
  only once its commit marker is written, so a session never sees market data
  without the integrity events written with it.
- **Each raw log is read from one load** (ADR 0018). Every ingest of a raw log
  is a load (one or more batches); a session reads each log only from its
  latest load that is complete within its version, or, if it has none, from
  its latest incomplete one (a first load still being written). Loading a log
  again, after an adapter fix, replaces what the earlier load said about it,
  including rows it no longer produces, and a reload in progress never hides
  the previous complete load. Loads are the store's only write path.
- **One row per trade** (ADR 0018). Every observation of a trade is stored;
  the session shows its first observation in `(log_id, seq)` order, among
  those inside both cutoffs.
- The store is append-only. A correction is a new row with a later
  `knowledge_ts`, and a reload is a new load; the earlier rows stay, so a
  session at an earlier `version` still sees what the store held then. A
  version that can be asked for never changes afterwards.
- `sql` accepts exactly one `SELECT` statement over those views. DDL, DML,
  multi-statement input, and anything that reads a file path or URL
  (`read_parquet` and similar) are rejected. The check is an allow-list applied
  to DuckDB's own parse tree, and the session then executes that tree, so there
  is no second parser to disagree with the engine. Tables, functions, types and
  expression kinds not on the list are refused, as are `WITH` clauses.
- Independently of that check, each session's database is locked so it can
  open only the files of committed batches at or before its version, of the
  loads it reads, received on or before the day of `available_at`. It cannot write, attach or load
  anything, and cannot be reconfigured. A hole in the statement check would
  still not reach a later version, an uncommitted batch or a later day.
- The session does not hand out its database connection or the store's file
  paths, and error messages have store paths removed.

An import-lint test (added with the signal layer) additionally fails if layer
3 or 5 code imports storage internals.

**Backtests use a replay cursor.** The harness opens one
`ReplayCursor(store_root, start=..., end=...)` for the whole replay, hands its
`session` to the strategy, and calls `cursor.advance(now)` as simulated time
passes. The session then reads exactly what a new session with
`available_at=now` would read, without reopening the store. Time moves forward
only. The session has no method that moves time and no reference to the
cursor, so strategy code that holds the session cannot look ahead; it never
constructs a session or a cursor itself.

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
   `recv_ts` (wall clock), `recv_mono_ns` (the monotonic counter, read in the
   same call) and a local sequence number.
2. **Capture.** The raw bytes, `recv_ts` and sequence number are pushed to the
   capture queue. The journal thread appends them to the raw log. This log is
   the source of truth for replay and for the data core.
3. **Normalize.** The feed thread parses the message into fixed-size
   `MarketEvent`s (book levels, a snapshot framed by `SnapshotBegin` and
   `SnapshotEnd`, trades) with integer prices and quantities, each naming its
   source (the connection), and checks the venue's message numbering where it
   has one (Coinbase numbers every message on a connection). A gap, a
   rejected message or a new connection becomes a control event
   (`StreamStale`, `StreamReset`; ADR 0020) and triggers a resync; a stale
   book produces no orders.
4. **Hand off.** The message's events go onto that feed's SPSC queue to the
   core thread, all of them or none (section 5).
5. **Book update.** The core thread applies the events to the instrument's
   order book: an update takes effect at its last level, a snapshot at its
   `SnapshotEnd`. Where the venue protects the book with a checksum instead of
   message numbers (Kraken), it is verified here, after the update, with the
   instrument's precisions; a mismatch marks the book stale in the same way.
   A stale book recovers at the next complete snapshot that passes the check.
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
in `seq` order; several logs from the same machine and boot are merged by
their monotonic stamps (ADR 0012). The simulated exchange replaces the venue and
models latency, fees and queue position. Time comes from the events.

### 4.3 The decision clock

Strategy logic in Python runs on a decision clock, not per tick. In replay the
engine calls the Python strategy synchronously at each decision time, with the
session of a `ReplayCursor` it has advanced to simulated time (section 3.2), so
the session's `available_at` equals simulated time. Live, the same function runs
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
- **Backpressure is explicit.** A market-data message crosses its queue whole
  or not at all: the feed pushes its events only when there is room for all of
  them, and each feed's queue holds at least twice the largest snapshot it may
  carry (a Coinbase book is about 38,000 levels). A message that does not fit
  is dropped whole, the source is marked stale (a `StreamStale` ahead of
  anything else of it), and the feed resyncs only once a whole snapshot has
  room, with a growing backoff, so a resync's own snapshot cannot loop (ADR
  0020; built in Phase 5). Data is never dropped silently. A full order queue
  rejects the order. A full journal queue trips the kill switch, because the
  system does not trade without an audit trail.
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

Details are in `docs/data-core-spec.md` section 2; alternatives in ADRs 0005
and 0019.

### 6.1 Prices and quantities

- A price or quantity is an `int64` mantissa with a per-instrument decimal
  exponent: `value = mantissa × 10^-exp`. `price_exp` and `qty_exp` are fixed
  for each instrument (`adapters.FIXED_EXPONENTS`; 8 and 8 for BTC-USD),
  generous enough for any tick or lot size it may have, and recorded in its
  instrument metadata (ADR 0019). The venue's own precision, which can
  change, is recorded beside them.
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
- Instrument metadata is itself bitemporal, so the tick sizes and precisions
  in force at any past time are recoverable; the exponents never change.

### 6.2 Timestamps

All timestamps are `int64` nanoseconds since the Unix epoch, UTC.

| Field | Meaning | Set by |
|-------|---------|--------|
| `event_ts` | When the venue says it happened | The venue (its precision varies; stored as given, scaled to ns). If the message has no venue timestamp, it holds `recv_ts` (ADR 0008) |
| `event_ts_source` | Where `event_ts` came from: `"venue"` or `"receipt"` | The adapter, from the message type (ADR 0008) |
| `recv_ts` | When our process received the bytes. **The availability time**: what a session at simulated time T may see, every time window and the store's date partition are on it (ADR 0017) | Feed thread, wall clock |
| `knowledge_ts` | Which batch of the store a row was written in: its **version**. One per batch, read when the batch opens; the batch is readable from its commit. A load of a raw log is one or more batches (ADR 0018) | Data core at write time. A correction or a reload gets a new, later `knowledge_ts`; the old row is kept |
| `recv_mono_ns` | The system's monotonic high-resolution counter, read with `recv_ts` | Feed thread (recorder). Comparable only between logs from the same machine, boot and counter (ADR 0012) |
| `seq` | Local sequence number at receipt | Feed thread. **Within one raw log, replay order is `seq` alone**; logs are merged by `recv_mono_ns`. `recv_ts` is a measurement, never an ordering key (ADR 0012) |
| `log_id` | The raw log a stored row came from, derived from that log's first record (not a time field; listed because it orders stored data with `seq`) | Normalizer. Stored rows are keyed and ordered by `(log_id, seq)`, never by `recv_ts` (ADR 0013) |

**Three times, each with one job** (ADR 0017). Venue time (`event_ts`) is what
the venue says and is never a cutoff. Receipt time (`recv_ts`) decides what a
session may see. Knowledge time (`knowledge_ts`) decides which version of the
store it reads. A raw log is loaded after it was recorded, so a row's
knowledge time is hours or days after its receipt time, and the two are never
used for each other.

**Wall-clock times assume a synchronized clock** (ADR 0011). `recv_ts` is only
as accurate as the clock of the machine that recorded it, and `knowledge_ts`
as the clock of the machine that loaded it, so every component that stamps
wall-clock time must run on a clock kept synchronized to NTP. The recorder enforces this: at the start of every
session it measures its offset against several NTP servers, refuses to record
if the offset is above 250 ms unless explicitly overridden, and stores the
measurement (and any override) as stream events, which a quality check
reports. Feed and journal threads will need the same check when they start
stamping time.

Latency measurements inside the process use a monotonic clock and are stored
as durations, never as wall times. Simulated "now" in a backtest is the
`recv_ts` of the event being processed plus modelled latency, so a strategy
sees data when we would have had it, not when the venue produced it. It is the
`available_at` of the session the strategy is given.

### 6.3 C++ core types and messages

Built in roadmap Phase 3 (`cpp/include/qa/`). The layouts below are fixed by
`static_assert`s in `cpp/tests/test_events.cpp` and checked from Python in
`python/tests/test_core_types.py`; change all three together.

**Value types** (`qa/types.hpp`): `Price` and `Qty` (int64 mantissas),
`Timestamp` (wall clock, ns since the Unix epoch), `MonoTimestamp` (the
monotonic counter, ADR 0012), `InstrumentId` (uint32), `Side` (`Sell = -1`,
`Unknown = 0`, `Buy = 1`, as in the Python schema) and `InstrumentMeta` (24
bytes: `id` 0, `price_exp` 4, `qty_exp` 5, `price_precision` 6,
`qty_precision` 7, `tick_size` 8, `lot_size` 16; the precisions are the
venue's printed decimals, at most the exponents, which Kraken's checksum
needs; ADRs 0019 and 0020). Each is
constructed explicitly from an integer, never converts implicitly, cannot be
built from or turned into floating point or bool, and does not mix with the
others: a `Price` cannot be compared with, added to or multiplied by a `Qty`.

**Notional** (`qa/notional.hpp`): `notional(Price, Qty)` returns the exact
128-bit product as `Int128` (with `_mul128` on MSVC, `__int128` on GCC and
Clang, a portable fallback otherwise), at exponent `price_exp + qty_exp`;
`to_int64` says whether it fits back.

**Clock** (`qa/clock.hpp`): the interface hot-path code takes its time from.
`SystemClock` reads `std::chrono::system_clock` and `steady_clock`, which is
the same counter as Python's `time.perf_counter_ns` (tested to agree within
1 ms), so stamps from C++ and from the recorder are comparable. `ManualClock`
returns what it is set to, can advance both clocks, and can step the wall
clock alone.

**Messages** (`qa/events.hpp`): fixed size, trivially copyable, standard
layout, with no compiler padding (explicit zero `reserved` bytes), in the
machine's byte order (little-endian on every supported platform).

`MarketEvent`, 88 bytes, one per book level, trade or stream control event:

| Offset | Size | Field | Meaning |
|--------|------|-------|---------|
| 0 | 8 | `recv_ts` | Wall-clock receive time |
| 8 | 8 | `recv_mono` | Monotonic receive stamp (flag `kHasMono`) |
| 16 | 8 | `event_ts` | Venue time, or `recv_ts` if the venue gave none (flag `kEventTsFromReceipt`, ADR 0008) |
| 24 | 8 | `seq` | Raw record number: replay order within a log |
| 32 | 8 | `venue_seq` | The venue's message number (flag `kHasVenueSeq`) |
| 40 | 8 | `trade_id` | The venue's trade id, if numeric (flag `kHasTradeId`) |
| 48 | 8 | `price` | Price mantissa |
| 56 | 8 | `qty` | Quantity mantissa; 0 removes a book level |
| 64 | 4 | `instrument` | `InstrumentId` |
| 68 | 4 | `checksum` | The venue's book checksum after the message (flag `kHasChecksum`) |
| 72 | 4 | `level_index` | Position of the level in its message; the level count for `SnapshotBegin` and `SnapshotEnd`; the stored stream event's `event_index` for `StreamStale` and `StreamReset` |
| 76 | 2 | `depth` | Levels per side the venue keeps (flag `kHasDepth`) |
| 78 | 1 | `kind` | `BookSnapshot = 0`, `BookDelta = 1`, `BookCheckSnapshot = 2` (as in the Python schema), `Trade = 3`; stream control (ADR 0020): `StreamStale = 4`, `StreamReset = 5`, `SnapshotBegin = 6`, `SnapshotEnd = 7` |
| 79 | 1 | `side` | `Side` |
| 80 | 1 | `flags` | `kLastInMessage`, `kHasMono`, `kHasVenueSeq`, `kHasChecksum`, `kHasDepth`, `kHasTradeId`, `kEventTsFromReceipt` |
| 81 | 1 | `reason` | `StreamReason`: for `StreamStale` `SequenceGap = 1`, `SequenceRepeat = 2`, `SequenceOutOfOrder = 3`, `ChecksumMismatch = 4`, `MessageRejected = 5`; for `StreamReset` `SessionStart = 6`, `Connect = 7`, `Disconnect = 8`; `NoReason = 0` for everything else |
| 82 | 2 | `reserved` | Zero |
| 84 | 4 | `source` | The stream the event belongs to: one connection of one raw log, `(log_id, "ws")` or `(log_id, "probe")`, numbered by the harness or feed handler, which keeps the mapping |

Stream control (ADR 0020):

- `StreamStale`: the source's book cannot be trusted until its next snapshot.
  `StreamReset`: a new session or connection; the book is gone until a
  snapshot. Each stands for a stored stream event of the same name
  (`docs/data-core-spec.md` section 6) and carries no price, quantity, side,
  trade id, checksum or depth; a gap may carry the venue number received.
- `(source, seq)` names the raw record. A record's events are contiguous, its
  control events before its levels.
- A snapshot is `SnapshotBegin`, its levels (`level_index` 0 to n-1, the last
  with `kLastInMessage`) and `SnapshotEnd`, both frame events carrying n, with
  nothing between them; this applies to check snapshots too. An update takes
  effect at its `kLastInMessage` level, a snapshot at its `SnapshotEnd`.
- `MarketEvent.from_bytes` refuses unknown kinds and reasons, a reason that
  does not fit its kind, a control event carrying a market value, and
  non-zero reserved bytes.

`OrderRequest`, 64 bytes (core to execution): `client_order_id` 0,
`target_order_id` 8 (the order a cancel refers to), `price` 16, `qty` 24,
`created_ts` 32, `created_mono` 40, `instrument` 48 (4), `kind` 52 (`New`,
`Cancel`), `side` 53, `type` 54 (`Limit`, `Market`), `time_in_force` 55
(`GoodTillCancel`, `ImmediateOrCancel`, `PostOnly`), `reserved` 56 (8).

`ExecutionReport`, 80 bytes (execution to core): `client_order_id` 0,
`exec_id` 8, `last_price` 16, `last_qty` 24, `leaves_qty` 32, `event_ts` 40,
`recv_ts` 48, `recv_mono` 56, `instrument` 64 (4), `reject_code` 68 (2),
`kind` 70 (`Ack`, `Reject`, `PartialFill`, `Fill`, `Cancelled`), `side` 71,
`reserved` 72 (8).

The order messages are a first cut, defined now so the queue and journal
work has concrete types; Phases 9 and 10 may extend them, through a new
layout and an ADR rather than an in-place change.

## 7. Storage

Details and alternatives are in ADR 0006.

| Tier | Content | Format | Used by |
|------|---------|--------|---------|
| Raw log | Exact bytes received, with `recv_ts` and `seq` | Append-only length-prefixed binary, zstd per segment | Replay engine, normalizer. Never rewritten |
| Normalized store | Trades, book events, instrument metadata and stream integrity events (ADR 0010), all three timestamps, and the raw record each row came from (`log_id`, `seq`; ADR 0013); bars are computed on demand (Phase 12) | Parquet, partitioned by venue/instrument/receipt date, integer mantissas. Written in batches, each readable once its commit marker exists (ADR 0017); each ingest of a raw log is a load, and a log is read from its latest complete load (ADR 0018) | `PITSession`, via DuckDB |
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
| `query_data(available_at, ...)` | Read market data through `PITSession`. `available_at` cannot exceed now and cannot reach into the locked holdout period: the holdout is a ceiling on receipt time, which is what `available_at` bounds (ADR 0017) | Read |
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
