# 0002: pybind11, installed from pip

Status: Accepted
Date: 2026-11-10

## Context

Python needs to drive the C++ replay engine, read order books and send
targets. Bindings are a cold-path interface: Python is never called from the
live core thread, so per-call binding overhead is not on the critical path.
What matters is correctness, MSVC support, and that a future maintainer (human
or model) knows the library well.

## Decision

Use **pybind11**. Install it with pip into the repo's virtual environment
(`pybind11` is in the `dev` extra of `python/pyproject.toml`) and locate it
from CMake with `python -m pybind11 --cmakedir`. It is deliberately not in the
vcpkg manifest.

The extension module is `quantagent._core`. CMake writes it directly into
`python/src/quantagent/`, where the editable install finds it.

## Alternatives considered

- **nanobind.** Smaller binaries, faster compile and lower call overhead, by
  the same author. Its advantages matter most for binding-heavy call paths,
  which this design avoids. Less widely known and a smaller body of examples.
  A reasonable choice to revisit if binding call cost ever shows up in a
  profile; the binding layer is thin enough to port.
- **pybind11 from vcpkg.** Keeps all C++ dependencies in one manifest, but the
  vcpkg port depends on vcpkg's own `python3` port. That builds a second
  Python from source and risks CMake linking the extension against it instead
  of the venv interpreter that will import it. Using the pip package removes
  that failure mode.
- **Cython.** Good for wrapping C APIs and typed Python; awkward for C++20
  classes and templates, and adds a third language.
- **ctypes/cffi over a C API.** No build-time coupling, but every type needs a
  hand-written C shim and there is no automatic lifetime management.
- **No in-process bindings (IPC only).** Strongest isolation, but backtests
  need Python strategy callbacks inside the replay loop and zero-copy access
  to results, which IPC makes slow and complicated.

## Consequences

- The extension is built for exactly one interpreter: the one in `.venv`.
  Changing Python version means rebuilding.
- The pybind11 version is governed by the Python environment, not the vcpkg
  baseline. Pin it in `pyproject.toml` if reproducibility requires.
- Binding code must release the GIL around long C++ calls (replay runs) and
  must never be invoked from the core thread in live or paper mode.
