# 0001: Monorepo with `cpp/` and `python/`

Status: Accepted
Date: 2026-11-10

## Context

The system has a C++ hot path and four Python layers that share types (events,
prices, instrument metadata) and must be tested together: the C++ order book is
verified against the Python reference reconstructor, and backtests drive C++ from Python.

## Decision

One repository:

```
cpp/                C++20 hot path. Own CMake project, presets, vcpkg manifest
  include/qa/       public headers, namespace qa
  src/              implementation
  bindings/         pybind11 module -> quantagent._core
  tests/            GoogleTest
  bench/            Google Benchmark
python/             one installable distribution, src layout
  src/quantagent/   one package: data core (quantagent.data), signals, risk,
                    agent, and the compiled _core module
  tests/            pytest
docs/               architecture, roadmap, ADRs
scripts/            check.ps1 (build + all tests)
```

Component subdirectories (`cpp/include/qa/book/`, `python/src/quantagent/risk/`
and so on) are created by the phase that implements them, not in advance.

## Alternatives considered

- **Separate repos for C++ and Python.** Clean ownership boundaries, but a
  change to an event struct would need coordinated commits and version pins
  across repos. With one developer and tightly coupled layers this is cost
  with no benefit.
- **One Python distribution per layer.** Enforces layer boundaries through
  packaging. Too heavy now; an import-lint test gives the same guarantee more
  cheaply. Can be split later without moving files far.
- **CMake project at the repo root.** Conventional for C++-first projects, but
  mixes build files with Python packaging and makes `cpp/` harder to build in
  isolation.

## Consequences

- One PR can change a C++ type, its binding and its Python consumers together.
- CI builds everything on every change. Acceptable at this size.
- Layer boundaries inside `python/` are conventions that need a lint test
  (planned with the signal layer).
