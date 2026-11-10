# quantitative_agent

A quantitative trading agent: C++ hot path, point-in-time Python data core,
signal and risk layers, and an LLM research/monitoring agent.

Paper trading is the default everywhere. Live trading requires explicit opt-in.

**Status:** skeleton and design only (roadmap Phase 0). No trading components
are implemented yet.

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
