"""Property test: no query path ever returns a row known after as_of.

Hypothesis generates a random history of appends (with corrections, across
venues, instruments, datasets and dates) at random knowledge times, opens a
session at a random as_of, and compares every read method against an oracle
computed in plain Python from the batches that were appended.
"""

import tempfile
from pathlib import Path

from _builders import INT64_MAX, LOG, NS_PER_DAY, T0
from hypothesis import given, settings
from hypothesis import strategies as st

from quantagent.data.schema import BOOK_EVENTS, INSTRUMENTS, TRADES, BookEventKind, Side
from quantagent.data.schema import table_from_rows as build
from quantagent.data.session import PITSession
from quantagent.data.store import AppendOnlyStore

VENUES = ["kraken", "coinbase"]
INSTRUMENT_NAMES = ["AAA", "BBB"]
EVENT_TIMES = [T0, T0 + 1, T0 + NS_PER_DAY, T0 + 2 * NS_PER_DAY + 7]
DATASETS = {"trades": TRADES, "book_events": BOOK_EVENTS, "instruments": INSTRUMENTS}


def _row(dataset_name: str, key: int, event_ts: int, value: int) -> dict:
    """A row whose identity is ``key`` and whose payload is ``value``."""
    header = {
        "event_ts": event_ts,
        "event_ts_source": "venue",
        "recv_ts": event_ts + 1,
        "log_id": LOG,
        "seq": key,
    }
    if dataset_name == "trades":
        return {**header, "trade_id": f"t{key}", "price": value, "qty": 1, "side": Side.BUY}
    if dataset_name == "book_events":
        # Book events are keyed by (log_id, seq, level_index): a later batch with
        # the same seq is a correction.
        return {
            **header,
            "kind": BookEventKind.DELTA,
            "venue_seq_first": None,
            "venue_seq_last": key,
            "checksum": None,
            "depth": None,
            "level_index": 0,
            "side": Side.BUY,
            "price": 100,
            "qty": value,
        }
    # Instrument versions are keyed by (log_id, seq); the key also picks the
    # version's effective time, so corrections keep it.
    return {
        "event_ts": EVENT_TIMES[key % len(EVENT_TIMES)],
        "event_ts_source": "venue",
        "recv_ts": 1,
        "log_id": LOG,
        "seq": key,
        "price_exp": 2,
        "qty_exp": 2,
        "tick_size": value,
        "lot_size": 1,
    }


def _identity(dataset_name: str, row: dict) -> tuple:
    return tuple(row[column] for column in DATASETS[dataset_name].key)


@st.composite
def batches(draw):
    dataset_name = draw(st.sampled_from(sorted(DATASETS)))
    keys = draw(st.lists(st.integers(0, 3), min_size=1, max_size=4, unique=True))
    rows = {}
    for key in keys:
        row = _row(
            dataset_name,
            key,
            draw(st.sampled_from(EVENT_TIMES)),
            draw(st.integers(1, 1_000_000)),
        )
        rows[_identity(dataset_name, row)] = row  # one version of a key per batch
    return {
        "dataset": dataset_name,
        "venue": draw(st.sampled_from(VENUES)),
        "instrument": draw(st.sampled_from(INSTRUMENT_NAMES)),
        "rows": list(rows.values()),
        "gap": draw(st.integers(1, 1_000)),  # knowledge time since the previous batch
    }


class SteppingClock:
    def __init__(self, gaps):
        self._times = []
        now = 0
        for gap in gaps:
            now += gap
            self._times.append(now)
        self.times = list(self._times)

    def __call__(self):
        return self._times.pop(0)


def _oracle(history, as_of):
    """Latest version of each record among batches known at ``as_of``."""
    latest = {}
    for knowledge_ts, batch in history:
        if knowledge_ts > as_of:
            continue
        for row in batch["rows"]:
            identity = (
                batch["dataset"],
                batch["venue"],
                batch["instrument"],
                _identity(batch["dataset"], row),
            )
            latest[identity] = {**row, "knowledge_ts": knowledge_ts}
    return latest


def _canonical(rows):
    return sorted(tuple(sorted(row.items(), key=lambda kv: kv[0])) for row in rows)


def _plain(rows):
    return [{k: (int(v) if isinstance(v, int) else v) for k, v in row.items()} for row in rows]


@settings(max_examples=60, deadline=None)
@given(
    history_spec=st.lists(batches(), min_size=1, max_size=8),
    as_of_fraction=st.floats(0, 1.1),
    window=st.tuples(st.sampled_from(EVENT_TIMES), st.sampled_from(EVENT_TIMES)),
)
def test_no_method_returns_a_row_known_after_as_of(history_spec, as_of_fraction, window):
    clock = SteppingClock([batch["gap"] for batch in history_spec])
    final_time = clock.times[-1]
    as_of = int(final_time * as_of_fraction)

    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
        root = Path(directory) / "store"
        store = AppendOnlyStore(root, clock=clock)
        history = []
        for batch in history_spec:
            dataset = DATASETS[batch["dataset"]]
            result = store.append(
                dataset, batch["venue"], batch["instrument"], build(dataset, batch["rows"])
            )
            history.append((result.knowledge_ts, batch))

        expected = _oracle(history, as_of)
        start, end = min(window), max(window) + 1

        with PITSession(root, as_of=as_of) as session:
            for venue in VENUES:
                for instrument in INSTRUMENT_NAMES:
                    # Typed methods over all time, and over a window.
                    for name, method in (
                        ("trades", session.trades),
                        ("book_events", session.book_events),
                    ):
                        want = [
                            row
                            for (ds, v, i, _), row in expected.items()
                            if (ds, v, i) == (name, venue, instrument)
                        ]
                        got = method(venue, instrument, 0, INT64_MAX).to_pylist()
                        assert all(r["knowledge_ts"] <= as_of for r in got)
                        assert _canonical(got) == _canonical(_plain(want))

                        windowed = method(venue, instrument, start, end).to_pylist()
                        assert all(r["knowledge_ts"] <= as_of for r in windowed)
                        assert _canonical(windowed) == _canonical(
                            _plain([r for r in want if start <= r["event_ts"] < end])
                        )

                    versions = [
                        row
                        for (ds, v, i, _), row in expected.items()
                        if (ds, v, i) == ("instruments", venue, instrument)
                    ]
                    got = session.instruments(venue, instrument).to_pylist()
                    assert all(r["knowledge_ts"] <= as_of for r in got)
                    assert _canonical(got) == _canonical(_plain(versions))

                    for at in (*EVENT_TIMES, EVENT_TIMES[0] - 1, None):
                        meta = session.instrument(venue, instrument, at=at)
                        effective = as_of if at is None else at
                        in_effect = [r for r in versions if r["event_ts"] <= effective]
                        if not in_effect:
                            assert meta is None
                            continue
                        best = max(in_effect, key=lambda r: r["event_ts"])
                        assert meta.knowledge_ts <= as_of
                        assert (meta.event_ts, meta.tick_size, meta.knowledge_ts) == (
                            best["event_ts"],
                            best["tick_size"],
                            best["knowledge_ts"],
                        )

            # SQL over every view.
            for name in DATASETS:
                want = [
                    {**row, "venue": v, "instrument": i}
                    for (ds, v, i, _), row in expected.items()
                    if ds == name
                ]
                got = session.sql(f"SELECT * EXCLUDE (date) FROM {name}").to_pylist()
                assert all(r["knowledge_ts"] <= as_of for r in got)
                assert _canonical(got) == _canonical(_plain(want))

                summary = session.sql(
                    f"SELECT count(*) AS n, max(knowledge_ts) AS latest FROM {name}"
                ).to_pylist()[0]
                assert summary["n"] == len(want)
                assert summary["latest"] is None or summary["latest"] <= as_of
