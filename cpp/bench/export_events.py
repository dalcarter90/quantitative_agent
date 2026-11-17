"""The recorded stream the order book benchmark replays (ADR 0023).

Writes the committed Coinbase fixture's MarketEvents, as the Phase 1c adapters
normalize it (in memory, nothing stored) and the Phase 4a converter converts
it, every group in order, each event as its 88 bytes (``MarketEvent.to_bytes``)
and nothing else, to ``data/bench/coinbase-BTC-USD.events``. That is where
``BM_ReplayCoinbaseFixture`` (``cpp/bench/bench_order_book.cpp``) reads it:

    .venv\\Scripts\\python cpp\\bench\\export_events.py
    cd cpp
    .\\build\\windows\\bench\\Release\\qa_bench.exe --benchmark_filter=ReplayCoinbaseFixture

Run it again when the fixture, the adapters or the converter change. The file
is git-ignored, as all of ``data/`` is; the fixture it is made from is
committed.
"""

import os
import sys
from collections.abc import Iterable, Iterator
from pathlib import Path

from quantagent.data.adapters import normalize
from quantagent.data.rawlog import read_raw_log
from quantagent.data.schema import BOOK_EVENTS, INSTRUMENTS, table_from_rows
from quantagent.marketevents import EventGroup, convert

REPO = Path(__file__).resolve().parents[2]
VENUE, INSTRUMENT = "coinbase", "BTC-USD"
(FIXTURE,) = sorted((REPO / "python/tests/fixtures/raw" / VENUE / INSTRUMENT).glob("*.qaraw.zst"))
OUT = REPO / "data" / "bench" / f"{VENUE}-{INSTRUMENT}.events"
INSTRUMENT_ID = 7  # any id: the benchmark builds its book from the stream's own metadata


def fixture_groups(path: Path = FIXTURE) -> Iterator[EventGroup]:
    """The converter's groups for the fixture, as a session over it would give them."""
    rows = normalize(read_raw_log(path)).rows
    book = table_from_rows(BOOK_EVENTS, rows[("book_events", VENUE, INSTRUMENT)])
    events = rows.get(("stream_events", VENUE, INSTRUMENT), [])
    meta = table_from_rows(INSTRUMENTS, rows[("instruments", VENUE, INSTRUMENT)])
    return convert(book, events, meta, instrument=INSTRUMENT_ID)


def write_events(groups: Iterable[EventGroup], out: Path) -> int:
    """Every event of ``groups``, in order, as its bytes; returns how many. The
    file appears whole or not at all."""
    out.parent.mkdir(parents=True, exist_ok=True)
    partial = out.with_name(out.name + ".partial")
    count = 0
    with open(partial, "wb") as file:
        for group in groups:
            for event in group.events:
                file.write(event.to_bytes())
                count += 1
    os.replace(partial, out)
    return count


def main() -> int:
    count = write_events(fixture_groups(), OUT)
    print(f"{count} MarketEvents ({OUT.stat().st_size:,} bytes) from {FIXTURE.name} to {OUT}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
