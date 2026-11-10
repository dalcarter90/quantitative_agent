"""The C++ extension must be importable. A missing build is a failure, not a skip:
"tests green" has to mean the C++ -> Python path works."""

import quantagent


def test_core_is_importable_and_matches_package_version():
    from quantagent import _core

    assert _core.version() == quantagent.__version__


def test_core_hello():
    from quantagent import _core

    assert _core.hello() == f"hello from qa_core {_core.version()}"
