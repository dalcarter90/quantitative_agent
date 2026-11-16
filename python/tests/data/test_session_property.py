"""Property tests: no read path returns a row it should not (ADR 0017).

Hypothesis generates a random history of batches (several tables each, across
datasets, venues, instruments and receipt days, with corrections), some of
them never committed, at random knowledge times. It then opens a session at a
random availability time and version and compares every read method against
an oracle computed in plain Python from the batches.

The properties:

- no row with ``recv_ts > available_at`` is ever visible;
- no row with ``knowledge_ts > version`` is ever visible;
- no row of a batch that was not committed is ever visible;
- what is visible is exactly the latest version of each record among the rows
  that pass those three tests;
- a replay cursor advanced to a time reads what a new session at that time reads.
"""

import tempfile
from pathlib import Path

from _builders import INT64_MAX, LOG, NS_PER_DAY, T0
from hypothesis import given, settings
from hypothesis import strategies as st

from quantagent.data.schema import (
    BOOK_EVENTS,
    INSTRUMENTS,
    STREAM_EVENTS,
    TRADES,
    BookEventKind,
    Side,
    StreamEventKind,
)
from quantagent.data.schema import table_from_rows as build
from quantagent.data.session import PITSession, ReplayCursor
from quantagent.data.store import AppendOnlyStore

VENUES = ["kraken", "coinbase"]
INSTRUMENT_NAMES = ["AAA", "BBB"]
# Receipt times on three days, so the date partitions are exercised too.
RECV_TIMES = [T0, T0 + 1, T0 + NS_PER_DAY, T0 + 2 * NS_PER_DAY + 7]
EVENT_TIMES = [T0 - 5, T0, T0 + NS_PER_DAY, T0 + 2 * NS_PER_DAY]
DATASETS = {
    "trades": TRADES,
    "book_events": BOOK_EVENTS,
    "instruments": INSTRUMENTS,
    "stream_events": STREAM_EVENTS,
}
RANGE_METHODS = ("trades", "book_events", "stream_events")


def _row(dataset_name: str, key: int, recv_ts: int, event_ts: int, value: int) -> dict:
    """A row whose identity is ``key`` and whose payload is ``value``."""
    header = {
        "event_ts": event_ts,
        "event_ts_source": "venue",
        "recv_ts": recv_ts,
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
    if dataset_name == "stream_events":
        # Our own observations: event_ts is the receipt time (ADR 0008).
        return {
            **header,
            "event_ts": recv_ts,
            "event_ts_source": "receipt",
            "event_index": 0,
            "kind": StreamEventKind.SEQUENCE_GAP,
            "connection": "ws",
            "expected": key,
            "observed": value,
            "detail": None,
        }
    # Instrument versions are keyed by (log_id, seq); the key also picks the
    # version's effective time, so corrections keep it.
    return {
        **header,
        "event_ts": EVENT_TIMES[key % len(EVENT_TIMES)],
        "price_exp": 2,
        "qty_exp": 2,
        "tick_size": value,
        "lot_size": 1,
    }


def _identity(dataset_name: str, row: dict) -> tuple:
    return tuple(row[column] for column in DATASETS[dataset_name].key)


@st.composite
def tables(draw):
    dataset_name = draw(st.sampled_from(sorted(DATASETS)))
    keys = draw(st.lists(st.integers(0, 3), min_size=1, max_size=4, unique=True))
    rows = {}
    for key in keys:
        row = _row(
            dataset_name,
            key,
            draw(st.sampled_from(RECV_TIMES)),
            draw(st.sampled_from(EVENT_TIMES)),
            draw(st.integers(1, 1_000_000)),
        )
        rows[_identity(dataset_name, row)] = row  # one version of a key per table
    return {
        "dataset": dataset_name,
        "venue": draw(st.sampled_from(VENUES)),
        "instrument": draw(st.sampled_from(INSTRUMENT_NAMES)),
        "rows": list(rows.values()),
    }


@st.composite
def batches(draw):
    drawn = draw(st.lists(tables(), min_size=1, max_size=3))
    # One version of a record per batch: a batch has one knowledge time, so two
    # could not be told apart.
    unique = {}
    for table in drawn:
        unique[(table["dataset"], table["venue"], table["instrument"])] = table
    return {
        "tables": list(unique.values()),
        "committed": draw(st.sampled_from([True, True, True, False])),
        "gap": draw(st.integers(1, 1_000)),  # knowledge time since the previous batch
    }


class SteppingClock:
    def __init__(self, gaps):
        self._times = []
        now = 0
        for gap in gaps:
            now += gap
            self._times.append(now)

    def __call__(self):
        return self._times.pop(0)


def _load(root: Path, history_spec):
    """Write the history. Returns [(knowledge_ts, committed, batch)]."""
    clock = SteppingClock([batch["gap"] for batch in history_spec])
    store = AppendOnlyStore(root, clock=clock)
    history = []
    for spec in history_spec:
        batch = store.batch()
        for table in spec["tables"]:
            dataset = DATASETS[table["dataset"]]
            batch.append(
                dataset, table["venue"], table["instrument"], build(dataset, table["rows"])
            )
        if spec["committed"]:
            batch.commit()
        else:
            # The load died here: its files stay on disk with no commit marker,
            # and the next load starts from a new writer.
            store = AppendOnlyStore(root, clock=clock)
        history.append((batch.knowledge_ts, spec["committed"], spec))
    return history


def _oracle(history, available_at, version):
    """Latest version of each record among the committed rows known at
    ``version`` and received by ``available_at``."""
    latest = {}
    for knowledge_ts, committed, batch in history:
        if not committed or knowledge_ts > version:
            continue
        for table in batch["tables"]:
            for row in table["rows"]:
                if row["recv_ts"] > available_at:
                    continue
                identity = (
                    table["dataset"],
                    table["venue"],
                    table["instrument"],
                    _identity(table["dataset"], row),
                )
                latest[identity] = {**row, "knowledge_ts": knowledge_ts}
    return latest


def _canonical(rows):
    return sorted(
        tuple(sorted(row.items(), key=lambda kv: kv[0])) for row in rows
    )  # fmt: skip


def _plain(rows):
    return [{k: (int(v) if isinstance(v, int) else v) for k, v in row.items()} for row in rows]


def _check_session(session, expected, available_at, version, committed, window):
    """Every read path of ``session`` against the oracle's ``expected`` rows."""

    def allowed(rows):
        return all(
            r["recv_ts"] <= available_at
            and r["knowledge_ts"] <= version
            and r["knowledge_ts"] in committed
            for r in rows
        )

    start, end = min(window), max(window) + 1
    for venue in VENUES:
        for instrument in INSTRUMENT_NAMES:
            # Typed methods over all time, and over a window of receipt time.
            for name in RANGE_METHODS:
                want = [
                    row
                    for (ds, v, i, _), row in expected.items()
                    if (ds, v, i) == (name, venue, instrument)
                ]
                got = getattr(session, name)(venue, instrument, 0, INT64_MAX).to_pylist()
                assert allowed(got)
                assert _canonical(got) == _canonical(_plain(want))

                windowed = getattr(session, name)(venue, instrument, start, end).to_pylist()
                assert allowed(windowed)
                assert _canonical(windowed) == _canonical(
                    _plain([r for r in want if start <= r["recv_ts"] < end])
                )

            versions = [
                row
                for (ds, v, i, _), row in expected.items()
                if (ds, v, i) == ("instruments", venue, instrument)
            ]
            got = session.instruments(venue, instrument).to_pylist()
            assert allowed(got)
            assert _canonical(got) == _canonical(_plain(versions))

            for at in (*EVENT_TIMES, EVENT_TIMES[0] - 1, None):
                meta = session.instrument(venue, instrument, at=at)
                effective = available_at if at is None else at
                in_effect = [r for r in versions if r["event_ts"] <= effective]
                if not in_effect:
                    assert meta is None
                    continue
                best = max(in_effect, key=lambda r: r["event_ts"])
                assert meta.knowledge_ts <= version and meta.recv_ts <= available_at
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
        assert allowed(got)
        assert _canonical(got) == _canonical(_plain(want))

        summary = session.sql(
            f"SELECT count(*) AS n, max(knowledge_ts) AS known, max(recv_ts) AS received "
            f"FROM {name}"
        ).to_pylist()[0]
        assert summary["n"] == len(want)
        assert summary["known"] is None or summary["known"] <= version
        assert summary["received"] is None or summary["received"] <= available_at


AVAILABILITY = st.one_of(
    st.sampled_from([t + d for t in RECV_TIMES for d in (-1, 0, 1)]),
    st.sampled_from([0, T0 + NS_PER_DAY // 2, INT64_MAX]),
)


@settings(max_examples=60, deadline=None)
@given(
    history_spec=st.lists(batches(), min_size=1, max_size=6),
    available_at=AVAILABILITY,
    version_fraction=st.one_of(st.none(), st.floats(0, 1)),
    window=st.tuples(st.sampled_from(RECV_TIMES), st.sampled_from(RECV_TIMES)),
)
def test_no_method_returns_a_row_it_should_not(
    history_spec, available_at, version_fraction, window
):
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
        root = Path(directory) / "store"
        history = _load(root, history_spec)
        committed = {k for k, done, _ in history if done}
        latest = max(committed, default=0)
        version = None if version_fraction is None else int(latest * version_fraction)

        with PITSession(root, available_at=available_at, version=version) as session:
            # The default version is the latest committed batch, never an uncommitted one.
            assert session.version == (latest if version is None else version)
            expected = _oracle(history, available_at, session.version)
            _check_session(session, expected, available_at, session.version, committed, window)


@settings(max_examples=25, deadline=None)
@given(
    history_spec=st.lists(batches(), min_size=1, max_size=5),
    times=st.lists(AVAILABILITY, min_size=1, max_size=5),
    version_fraction=st.one_of(st.none(), st.floats(0, 1)),
    window=st.tuples(st.sampled_from(RECV_TIMES), st.sampled_from(RECV_TIMES)),
)
def test_a_cursor_never_shows_more_than_a_session_at_its_time(
    history_spec, times, version_fraction, window
):
    times = sorted(times)
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
        root = Path(directory) / "store"
        history = _load(root, history_spec)
        committed = {k for k, done, _ in history if done}
        latest = max(committed, default=0)
        version = None if version_fraction is None else int(latest * version_fraction)

        with ReplayCursor(root, start=times[0], end=times[-1], version=version) as cursor:
            for now in times:
                cursor.advance(now)
                session = cursor.session
                assert session.available_at == now
                expected = _oracle(history, now, session.version)
                _check_session(session, expected, now, session.version, committed, window)
