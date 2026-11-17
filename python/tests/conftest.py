"""Test-run options shared by every test (CLAUDE.md, "Working efficiently").

``--skip-slow`` deselects tests marked ``slow`` (slower than about 5 s), for
day-to-day runs. It adds to the markers ``addopts`` already excludes
(``network``, ``local_data``) instead of replacing them, which a second
``-m`` would do. ``scripts/check.ps1`` runs without it, so every test runs.

A run without ``--skip-slow`` ends by listing any test that is not marked
``slow`` but whose call took longer than ``SLOW_SECONDS``, so the marks stay
true. It times the call alone: the setup of a fixture shared by a module lands
on whichever of its tests runs first, and is not that test's own cost
(marking it would only move the setup to the next test). It is a reminder,
not a failure: timings differ between machines.
"""

import pytest

SLOW_SECONDS = 5.0


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--skip-slow",
        action="store_true",
        help="leave out tests marked slow (check.ps1 runs everything)",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if not config.getoption("--skip-slow"):
        return
    slow = [item for item in items if item.get_closest_marker("slow")]
    if slow:
        config.hook.pytest_deselected(items=slow)
        items[:] = [item for item in items if not item.get_closest_marker("slow")]


_durations: dict[str, float] = {}
_marked: set[str] = set()


def pytest_runtest_setup(item: pytest.Item) -> None:
    if item.get_closest_marker("slow"):
        _marked.add(item.nodeid)


def pytest_runtest_logreport(report: pytest.TestReport) -> None:
    if report.when == "call":
        _durations[report.nodeid] = report.duration


def pytest_terminal_summary(terminalreporter, exitstatus: int, config: pytest.Config) -> None:
    if config.getoption("--skip-slow"):
        return
    unmarked = sorted(
        ((seconds, nodeid) for nodeid, seconds in _durations.items()
         if seconds > SLOW_SECONDS and nodeid not in _marked),
        reverse=True,
    )  # fmt: skip
    if unmarked:
        terminalreporter.write_sep("-", f"tests over {SLOW_SECONDS:.0f} s not marked slow")
        for seconds, nodeid in unmarked:
            terminalreporter.write_line(f"{seconds:6.1f}s {nodeid}")
