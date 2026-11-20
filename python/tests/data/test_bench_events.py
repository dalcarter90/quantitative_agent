"""The recorded stream the C++ order book benchmark replays
(``cpp/bench/export_events.py``, ``BM_ReplayCoinbaseFixture``; ADR 0023).

The script writes the committed Coinbase fixture's converted events as their
bytes. These tests show the file is exactly the converter's events, in order,
and that the benchmark can do what it relies on: build its book from the
stream's first ``InstrumentUpdate`` and replay the whole stream twice on one
book without it going stale.
"""

import importlib.util
from pathlib import Path

import pytest
from cppbook import EFFECT, EV, KIND
from test_fixtures import fixture_path
from test_marketevents import fixture_tables

from quantagent import _core
from quantagent.marketevents import convert

SCRIPT = Path(__file__).resolve().parents[3] / "cpp" / "bench" / "export_events.py"


def export_module():
    spec = importlib.util.spec_from_file_location("export_events", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_script_reads_the_committed_coinbase_fixture_into_the_ignored_data_directory():
    export = export_module()
    assert export.FIXTURE == fixture_path("coinbase")
    assert export.OUT.relative_to(export.REPO).as_posix() == "data/bench/coinbase-BTC-USD.events"
    # Where the benchmark looks for it (cpp/bench/CMakeLists.txt, bench_order_book.cpp).
    cmake = (SCRIPT.parent / "CMakeLists.txt").read_text()
    assert 'QA_BENCH_DATA="${PROJECT_SOURCE_DIR}/../data/bench"' in cmake
    assert '"/coinbase-BTC-USD.events"' in (SCRIPT.parent / "bench_order_book.cpp").read_text()


@pytest.mark.slow  # 5.1 s in check.ps1: two replays of 110,986 events through the binding
def test_the_benchmarks_stream_is_the_fixtures_converted_events_and_replays_cleanly(tmp_path):
    export = export_module()
    table, events, meta = fixture_tables("coinbase")
    groups = list(convert(table, events, meta, instrument=export.INSTRUMENT_ID))
    out = tmp_path / "bench" / "coinbase-BTC-USD.events"
    count = export.write_events(groups, out)
    data, size = out.read_bytes(), EV.size()
    flat = [event for group in groups for event in group.events]
    assert count == len(flat) == len(data) // size and len(data) % size == 0
    assert [data[i : i + size] for i in range(0, len(data), size)] == [e.to_bytes() for e in flat]
    assert not list(tmp_path.glob("**/*.partial"))
    back = [EV.from_bytes(data[i : i + size]) for i in range(0, len(data), size)]

    first = next(e for e in back if e.kind == KIND.InstrumentUpdate)
    book = _core.OrderBook(first.instrument_meta(), _core.BookCheck.CoinbaseSequence)
    for _ in range(2):  # the session start resets the book, so a second pass is the venue's too
        effects = [book.apply(event).effect for event in back]
        assert EFFECT.WentStale not in effects and effects[-1] != EFFECT.Pending
        assert effects.count(EFFECT.Applied) > 2_000 and not book.is_stale
