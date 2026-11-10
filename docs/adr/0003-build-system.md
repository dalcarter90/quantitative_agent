# 0003: CMake presets + vcpkg manifest

Status: Accepted
Date: 2026-11-10

## Context

The C++ layer must build on Windows today and on Linux later (the likely
deployment target), with pinned third-party dependencies and a single command
that a future session can run without knowing the machine.

## Decision

- **CMake** (>= 3.25), C++20, with `cpp/CMakePresets.json` as the only
  supported way to configure, build and test.
- **vcpkg in manifest mode** (`cpp/vcpkg.json`) for C++ dependencies, with a
  pinned `builtin-baseline`. vcpkg itself lives outside the repo; `VCPKG_ROOT`
  points at it.
- **Windows preset uses the "Visual Studio 17 2022" generator.** It finds MSVC
  on its own, so the build works from any shell, including non-interactive
  ones, without first entering a Visual Studio developer environment.
- **Linux preset uses Ninja Multi-Config.** It is defined but not yet verified.
- Warnings are errors for our own targets (`/W4 /WX /permissive-`, or
  `-Wall -Wextra -Wpedantic -Werror`) through the `qa_warnings` interface
  target. Third-party code is not affected.
- `scripts/check.ps1` wraps the whole sequence: venv, pip install, configure,
  build, ctest, pytest, ruff. CI runs the same script.
- The Python package uses plain setuptools. The compiled module is produced by
  the CMake build, not by `pip install`.

## Alternatives considered

- **Ninja on Windows.** Faster incremental builds and a
  `compile_commands.json`. Requires the MSVC environment to be loaded first
  (`vcvarsall` / Developer PowerShell), which is the most common way an
  automated session breaks a Windows build. Can be added as an extra preset
  without changing anything else.
- **Conan.** Capable, but vcpkg has first-class CMake toolchain integration
  and ships on GitHub's Windows runners.
- **CMake FetchContent for everything.** No external tool, but rebuilds every
  dependency in every build tree and has no binary cache.
- **Bazel or Meson.** Bazel is strong for hermetic polyglot monorepos but is a
  heavy investment and has weaker Windows/MSVC ergonomics. Meson is pleasant
  but less standard for the libraries used here.
- **scikit-build-core driving CMake from `pip install`.** One command builds
  the wheel, which is right for distribution. For development it hides the
  CMake build behind pip and makes C++ tests and benchmarks second-class. Worth
  adding when wheels are needed.

## Consequences

- Prerequisites on a new Windows machine: VS 2022 Build Tools with the C++
  workload, CMake, Python, and a bootstrapped vcpkg with `VCPKG_ROOT` set.
- The first configure builds GoogleTest and Benchmark from source (about a
  minute); vcpkg's binary cache makes later configures fast.
- The Visual Studio generator is multi-config: one configure serves both
  Release and Debug. Both write the extension to the same path in the Python
  package, so the last one built is the one Python imports.
- Upgrading dependencies means bumping `builtin-baseline` deliberately.
