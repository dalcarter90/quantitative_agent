# CLAUDE.md

Instructions for any AI coding session working in this repository. This file
is self-contained: follow it even if you have no other context.

## What this is

A quantitative trading agent, built in phases. Five layers:

1. **C++ hot path** (`cpp/`): feed handlers, order book, pre-trade risk,
   execution gateway, deterministic replay/backtest engine. Exposed to Python
   as `quantagent._core`.
2. **Python data core** (`python/src/quantagent/data/`, built in Phase 1):
   append-only bitemporal storage (event, receipt and knowledge time) queried
   only through `PITSession(as_of=...)`, plus a reference order book
   reconstructor (Phase 2) that the C++ book is verified against.
3. **Signal layer**: order flow imbalance, technicals, regime detection, one
   blended model, validated walk-forward with purged CV and real costs.
4. **Risk/portfolio**: sizing, exposure limits, drawdown guard that pauses
   trading and recalls capital.
5. **Agent layer**: an LLM that runs research loops and monitors performance.

Most of this is not built yet. Read these before changing anything:

- `docs/roadmap.md`: the phases, their order and acceptance criteria.
- `docs/architecture.md`: layers, interfaces, threading, representations.
- `docs/adr/`: why each major decision was made.

## Safety rules (never break these)

1. **Paper trading is the default everywhere.** Any new entry point, config
   or example starts in paper mode. Live trading requires all three of
   `QA_TRADING_MODE=live`, `QA_LIVE_TRADING_ACK` set, and a `--live` flag.
   Never write code that enables live trading by default, and never run
   anything in live mode yourself.
2. **No LLM calls in the tick loop.** Nothing on the C++ core, feed or
   execution threads, and nothing they wait on, may call an LLM.
3. **The agent never places orders directly and never modifies risk limits.**
   The agent layer has no tool, import path or credential that could. Do not
   add one.
4. **Every agent action goes through layer 4 and is logged with its
   reasoning**, before the action takes effect.
5. **Automated code may only reduce risk.** Pause, cancel and flatten can be
   automatic. Raising a limit, resuming after a pause, promoting a strategy
   and enabling live trading require a human.
6. **Risk limits are read from a config file at startup.** Do not add runtime
   setters, in C++ or in bindings.
7. **Every order passes pre-trade risk.** Do not add a path to the gateway
   that skips it, including in tests or debug builds.
8. **No secrets in git.** Keys go in `.env` (ignored). `.env.example` holds
   placeholders only. Do not print, log or commit key values.
9. **Research reads data only through `PITSession`.** Signal and agent code
   must not open store files or query the database directly. Do not add a
   way around the session's restrictions (raw file access, DDL,
   multi-statement SQL, unfiltered tables, a mutable `as_of`).
10. **The data store is append-only.** Corrections are new rows with a later
    knowledge time. Do not add update or delete operations to the store.

If a task seems to require breaking one of these, stop and ask the owner.

## How to work

- **One roadmap phase per session**, in order (Phase 1 is split into 1a to
  1d; each is one session). Start by reading the phase's acceptance criteria. Do not begin a phase whose prerequisites are unmet.
- **Every phase ends with tests green.** Run `scripts/check.ps1` and confirm
  it prints `ALL GREEN` before you say a phase is done. If it fails, the
  phase is not done; report the failure with its output.
- **Never delete, skip or weaken a test to make it pass.** Fix the code, or
  stop and explain.
- **Ask before hard-to-reverse choices**: new third-party dependencies in the
  hot path, storage or schema changes, data migrations, changing a
  representation in ADR 0005, anything marked "Ask the owner" in the roadmap,
  and anything touching live trading. ADRs with status *Proposed* are not
  settled; check before building on them.
- **Record decisions.** A new major decision gets a new ADR in `docs/adr/`
  listing the alternatives considered. Do not rewrite an accepted ADR;
  supersede it.
- **Keep docs true.** If you change an interface described in
  `docs/architecture.md`, update the document in the same PR. Tick off
  roadmap criteria and update the status table as you complete them.
- **Git:** work on a branch named `phase-N/short-description`, open a PR to
  `main`, never push directly to `main`, never force-push `main`.
- **Report honestly.** State what was verified and how. If something was not
  run or not verified, say so.
- **Do not build ahead.** Implement only the current phase. No placeholder
  directories or stub classes for future phases.

## One-time setup (Windows)

Needed once per machine:

- Visual Studio 2022 Build Tools with the "Desktop development with C++"
  workload (MSVC v143, Windows SDK).
- CMake 3.25 or newer on `PATH`.
- Python 3.11 or newer (3.12 is what CI uses).
- vcpkg, cloned and bootstrapped, with the `VCPKG_ROOT` environment variable
  pointing at it.

```powershell
winget install --id Microsoft.VisualStudio.2022.BuildTools -e --override "--passive --wait --add Microsoft.VisualStudio.Workload.VCTools --includeRecommended"
winget install --id Kitware.CMake -e
git clone https://github.com/microsoft/vcpkg.git C:\vcpkg
C:\vcpkg\bootstrap-vcpkg.bat -disableMetrics
[Environment]::SetEnvironmentVariable('VCPKG_ROOT', 'C:\vcpkg', 'User')
```

Open a new shell afterwards so `PATH` and `VCPKG_ROOT` are picked up. If
`cmake` is still not found, it is at `C:\Program Files\CMake\bin\cmake.exe`.

## Build and test commands

Everything, from the repo root (this is the command that defines "green"):

```powershell
powershell -ExecutionPolicy Bypass -File scripts\check.ps1            # Release
powershell -ExecutionPolicy Bypass -File scripts\check.ps1 -Config Debug
```

It creates `.venv` if missing, installs the Python package, configures and
builds `cpp/`, then runs ctest, pytest and ruff.

Individual steps:

```powershell
# Python environment (once, or after dependency changes)
python -m venv .venv
.venv\Scripts\python -m pip install -e "python[dev]"

# C++: run from cpp\ because presets resolve from the current directory
cd cpp
cmake --preset windows
cmake --build --preset windows-release
ctest --preset windows-release
.\build\windows\bench\Release\qa_bench.exe
cd ..

# Python
.venv\Scripts\python -m pytest python
.venv\Scripts\python -m pytest python\tests\test_binding.py -k hello   # one test
.venv\Scripts\python -m ruff check python
.venv\Scripts\python -m ruff format python

# Tests that talk to the venues are excluded by default. To run them:
.venv\Scripts\python -m pytest python -m network

# Record public market data (no API keys), then load it into a store
.venv\Scripts\python -m quantagent.data.recorder --venue kraken --instrument BTC-USD --out data\raw --duration 3600
.venv\Scripts\python -c "from quantagent.data.adapters import ingest; from quantagent.data.store import AppendOnlyStore; import sys; print(ingest(sys.argv[1], AppendOnlyStore('data/normalized')).summary())" data\raw\kraken\BTC-USD\<file>.qaraw.zst
```

Crypto venues are Kraken and Coinbase. Binance.com refuses API access from US
addresses, so it is not used. Recorded data and stores live under `data/`,
which is git-ignored; small recorded fixtures are committed under
`python/tests/fixtures/raw/`.

Things that commonly go wrong:

- **`ImportError` for `quantagent._core`**: the C++ build has not run, or was
  built for a different Python. Build `cpp/` with the venv present. The
  module is written to `python/src/quantagent/_core.*.pyd`.
- **Configure fails on the toolchain file**: `VCPKG_ROOT` is not set in this
  shell.
- **pybind11 not found**: run the `pip install -e "python[dev]"` step first.
  pybind11 comes from pip, not vcpkg (ADR 0002).
- **Release and Debug share one `.pyd` path.** The last configuration built
  is the one Python imports.
- A `linux` preset exists but has not been verified. Treat Linux support as
  untested until a phase verifies it.

## Layout

```
cpp/
  CMakeLists.txt, CMakePresets.json, vcpkg.json
  include/qa/     public headers
  src/            implementation (static library qa_core)
  bindings/       pybind11 module -> quantagent._core
  tests/          GoogleTest (qa_tests)
  bench/          Google Benchmark (qa_bench)
python/
  pyproject.toml
  src/quantagent/ Python package (plus the compiled _core module);
                  the data core arrives in Phase 1 as quantagent/data/
  tests/          pytest
docs/             architecture.md, roadmap.md, adr/
scripts/check.ps1 build + all tests
.github/workflows/ci.yml   runs scripts/check.ps1 on Windows
```

Build output (`cpp/build/`), `.venv/`, compiled modules, `data/`, `journals/`
and `.env` are git-ignored.

## C++ conventions

- C++20, MSVC `/W4 /WX /permissive-`. Warnings are errors; fix them, do not
  suppress them without a comment saying why.
- Namespace `qa`. Headers are `.hpp` under `cpp/include/qa/`, included as
  `"qa/..."`, with `#pragma once`.
- Types `PascalCase`; functions and variables `snake_case`; private members
  with a trailing underscore; constants `kPascalCase`; macros `QA_UPPER`.
- New targets link `qa_warnings`. New source files are added to
  `cpp/CMakeLists.txt` explicitly (no globbing).
- Hot-path code (anything that runs on the feed, core or execution threads):
  - no heap allocation after startup, no locks, no exceptions, no blocking
    calls, no direct system clock (use the injected `Clock`);
  - prices and quantities are the integer strong types from ADR 0005, never
    `double`;
  - messages crossing threads are fixed-size and trivially copyable, and go
    through the SPSC queue (ADR 0004);
  - all trading state is owned by the single core thread.
- Mark functions `[[nodiscard]]` when ignoring the result is a bug. Prefer
  `noexcept` on hot-path functions.
- Every component has GoogleTest tests in `cpp/tests/`. Performance-sensitive
  components also get a benchmark in `cpp/bench/`; record baseline numbers in
  the PR.
- Tests must be deterministic: no sleeps for synchronization, no network, no
  wall-clock assertions.

## Python conventions

- Python 3.11+, `src` layout, type hints on public functions.
- `ruff check` must pass; format with `ruff format` (line length 100).
- Tests use pytest, live in `python/tests/`, and must not need network access
  or API keys. Tests that do need the network get a marker and are excluded
  from the default run.
- A missing C++ extension is a test failure, not a skip.
- Timestamps are `int64` nanoseconds since the Unix epoch, UTC. Prices and
  quantities at storage and C++ boundaries are integers per ADR 0005;
  conversion to float for research is explicit.
- Add runtime dependencies to `python/pyproject.toml`; do not `pip install`
  ad hoc.
- **`PITSession.sql` is an allow-list** (`python/src/quantagent/data/sqlguard.py`).
  To permit a new function, type or SQL construct, add it to the list with a
  test showing it is accepted, and only if it is pure and deterministic: it
  must not read files, settings, the catalog, the clock or random state, or
  run SQL from a string. Never loosen a check to make a query pass.
- **DuckDB is pinned to one minor series.** `sqlguard` depends on the shape of
  DuckDB's parse tree and `PITSession` on its lockdown settings. Upgrading
  means raising the bound in `pyproject.toml`, running the tests, and reading
  the release notes for new SQL syntax, table functions and settings. Tests
  under `python/tests/data/test_session_sql.py` must all still pass
  unmodified.

## Research integrity

- No result is reported without transaction costs.
- Never evaluate on the locked holdout period without the owner's
  instruction.
- Count every trial. Do not tune on the test fold. A negative result is a
  valid result; report it as such.
- Any feature must pass a look-ahead test: its value at time t does not
  change when data after t is removed.
