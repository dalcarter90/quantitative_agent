# quantitative_agent

A quantitative trading agent: C++ hot path, point-in-time Python data core,
signal and risk layers, and an LLM research/monitoring agent.

Paper trading is the default everywhere. Live trading requires explicit opt-in.

**Status:** the data core (capture, the append-only store, the point-in-time
session, quality checks), the reference order book, the C++ core types and the
C++ order book, verified against the reference, are built (roadmap Part A and
Phases 3 to 4b). No trading components are implemented yet. The roadmap's
status table is the current list.

## Documentation

- [Architecture](docs/architecture.md): the five layers, interfaces, data
  flow, threading model, representations, agent tool surface.
- [Roadmap](docs/roadmap.md): phases with acceptance criteria.
- [Decision records](docs/adr/README.md).
- [CLAUDE.md](CLAUDE.md): conventions, setup, build and test commands, safety
  rules.

## Quick start (Windows)

Prerequisites and one-time setup are in [CLAUDE.md](CLAUDE.md). Then:

```powershell
powershell -ExecutionPolicy Bypass -File scripts\check.ps1
```

This builds the C++ library, tests, benchmark and Python extension, then runs
the C++ and Python test suites.
