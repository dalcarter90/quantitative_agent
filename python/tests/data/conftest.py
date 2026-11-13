"""Fixtures for the data core tests.

``history`` is a small store with a known timeline. Knowledge times are fixed
by a fake clock so tests can open sessions before, at and after each batch.

    kts   batch
    1000  instruments kraken/ETH-USD   version effective T0 - 1 day, tick 1
    2000  trades      kraken/ETH-USD   a @100 (T0, seq 0), b @101 (T0 + 1s, seq 1)
    3000  trades      coinbase/BTC-USD  a @200 (T0)
    4000  trades      kraken/ETH-USD   a @150            (correction of a)
    5000  book_events kraken/ETH-USD   seq 10 bid 99 x5, seq 10 ask 102 x7
    6000  instruments kraken/ETH-USD   version effective T0 + 30 days, tick 10 (seq 1)
    7000  instruments kraken/ETH-USD   first version again, tick 5 (correction)
    8000  trades      kraken/ETH-USD   c @103 (T0 + 1 day + 2s, seq 2)
    9000  book_events kraken/ETH-USD   seq 10 bid 99 x6  (correction)
"""

from dataclasses import dataclass
from pathlib import Path

import pytest
from _builders import (
    NS_PER_DAY,
    NS_PER_SECOND,
    T0,
    V1_EFFECTIVE,
    V2_EFFECTIVE,
    FakeClock,
    book_table,
    instrument_table,
)
from _builders import trade_table as trades

from quantagent.data.schema import BOOK_EVENTS, INSTRUMENTS, TRADES, Side
from quantagent.data.store import AppendOnlyStore


@dataclass(frozen=True)
class History:
    root: Path
    store: AppendOnlyStore
    files_by_kts: dict[int, tuple[Path, ...]]

    def file(self, knowledge_ts: int) -> str:
        """Path of one data file written at ``knowledge_ts``, with forward slashes."""
        return self.files_by_kts[knowledge_ts][0].as_posix()


@pytest.fixture
def root(tmp_path):
    return tmp_path / "store"


@pytest.fixture
def history(root):
    store = AppendOnlyStore(root, clock=FakeClock(*range(1000, 10000, 1000)))
    krk = ("kraken", "ETH-USD")
    batches = [
        (INSTRUMENTS, krk, instrument_table(V1_EFFECTIVE, tick_size=1)),
        (TRADES, krk, trades(("a", T0, 100), ("b", T0 + NS_PER_SECOND, 101))),
        (TRADES, ("coinbase", "BTC-USD"), trades(("a", T0, 200))),
        (TRADES, krk, trades(("a", T0, 150))),
        (BOOK_EVENTS, krk, book_table((10, T0, Side.BUY, 99, 5), (10, T0, Side.SELL, 102, 7))),
        (INSTRUMENTS, krk, instrument_table(V2_EFFECTIVE, tick_size=10, seq=1)),
        (INSTRUMENTS, krk, instrument_table(V1_EFFECTIVE, tick_size=5)),
        (TRADES, krk, trades(("c", T0 + NS_PER_DAY + 2 * NS_PER_SECOND, 103), seq=2)),
        (BOOK_EVENTS, krk, book_table((10, T0, Side.BUY, 99, 6))),
    ]
    files_by_kts = {}
    for dataset, (venue, instrument), table in batches:
        result = store.append(dataset, venue, instrument, table)
        files_by_kts[result.knowledge_ts] = result.files
    assert sorted(files_by_kts) == list(range(1000, 10000, 1000))
    return History(root=root, store=store, files_by_kts=files_by_kts)
