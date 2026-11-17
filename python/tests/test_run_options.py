"""The ``--skip-slow`` option and the unmarked-slow-test reminder
(``tests/conftest.py``; CLAUDE.md, "Working efficiently"), on a sample project."""

from pathlib import Path

import pytest

pytest_plugins = ["pytester"]

CONFTEST = Path(__file__).with_name("conftest.py").read_text(encoding="utf-8")
INI = """\
[pytest]
addopts = -m 'not network'
markers =
    network: needs the network
    slow: slow
"""
SAMPLE = """\
import pytest

@pytest.mark.slow
def test_slow():
    pass

@pytest.mark.network
def test_network():
    pass

def test_fast():
    pass
"""


@pytest.fixture
def project(pytester):
    # Every test counts as slow, so the reminder has something to say.
    pytester.makeconftest(CONFTEST.replace("SLOW_SECONDS = 5.0", "SLOW_SECONDS = -1.0"))
    pytester.makeini(INI)
    pytester.makepyfile(test_sample=SAMPLE)
    return pytester


def test_a_default_run_includes_slow_tests_and_keeps_the_other_exclusions(project):
    result = project.runpytest()
    result.assert_outcomes(passed=2, deselected=1)  # slow and fast; network left out


def test_skip_slow_adds_to_the_exclusions_instead_of_replacing_them(project):
    result = project.runpytest("--skip-slow")
    result.assert_outcomes(passed=1, deselected=2)  # fast only


def test_unmarked_slow_tests_are_listed_without_failing_the_run(project):
    result = project.runpytest()
    assert result.ret == 0
    result.stdout.fnmatch_lines(["*not marked slow*", "*test_sample.py::test_fast*"])
    assert "test_sample.py::test_slow" not in result.stdout.str()
    quiet = project.runpytest("--skip-slow")
    assert "not marked slow" not in quiet.stdout.str()
