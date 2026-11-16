"""Property tests for loads and the one-row-per-trade rule (ADR 0018).

Hypothesis generates a history of batches over two raw logs: batches outside
any load, and batches of loads (some ending their load, some not, some loads
ended by an empty ``finish``), some never committed (the writer died). Trade
ids are shared between logs and records, so one trade is observed several
times. Every read path of a session, and of a replay cursor, is compared with
an oracle computed in plain Python:

- per log, the latest load (by ``load_id``) whose last batch is within the
  version; if there is none, the latest load with a committed batch within
  the version; only that load's committed batches within the version are read;
- a log with no load at all is read from the batches outside a load (which
  only tests write);
- both cutoffs, then the latest version of each record;
- then one trade per ``trade_id``: its first observation in ``(log_id, seq)``
  order.
"""

import tempfile
from pathlib import Path

from _builders import LOG
from hypothesis import given, settings
from hypothesis import strategies as st
from test_session_property import (
    AVAILABILITY,
    DATASETS,
    EVENT_TIMES,
    INSTRUMENT_NAMES,
    RECV_TIMES,
    VENUES,
    SteppingClock,
    _check_session,
    _row,
)

from quantagent.data.schema import table_from_rows as build
from quantagent.data.session import PITSession, ReplayCursor
from quantagent.data.store import AppendOnlyStore

OTHER = "20231114T130000.000000000Z-0000000000bb"
LOGS = [LOG, OTHER]
# Each dataset's record identity, written out so the oracle does not read it
# from the code it checks.
IDENTITY = {
    "trades": ("log_id", "seq", "trade_id"),
    "book_events": ("log_id", "seq", "level_index"),
    "instruments": ("log_id", "seq"),
    "stream_events": ("log_id", "seq", "event_index"),
}


def _identity(dataset_name, row):
    return tuple(row[c] for c in IDENTITY[dataset_name])


@st.composite
def tables(draw, logs):
    dataset_name = draw(st.sampled_from(sorted(DATASETS)))
    rows = {}
    for _ in range(draw(st.integers(1, 4))):
        row = _row(
            dataset_name,
            draw(st.integers(0, 3)),
            draw(st.sampled_from(RECV_TIMES)),
            draw(st.sampled_from(EVENT_TIMES)),
            draw(st.integers(1, 1_000_000)),
        )
        row["log_id"] = draw(st.sampled_from(logs))
        if dataset_name == "instruments" and row["log_id"] == OTHER:
            # Distinct effective times per log, so "the version in effect" has one answer.
            row["event_ts"] += 1
        if dataset_name == "trades":
            # Few trade ids, so the same trade is seen in several records and logs.
            row["trade_id"] = f"t{draw(st.integers(0, 2))}"
        rows[_identity(dataset_name, row)] = row
    return {
        "dataset": dataset_name,
        "venue": draw(st.sampled_from(VENUES)),
        "instrument": draw(st.sampled_from(INSTRUMENT_NAMES)),
        "rows": list(rows.values()),
    }


@st.composite
def steps(draw):
    kind = draw(st.sampled_from(["outside", "load", "load", "finish"]))
    log = draw(st.sampled_from(LOGS))
    drawn = [] if kind == "finish" else draw(st.lists(tables([log] if kind == "load" else LOGS),
                                                      min_size=1, max_size=3))  # fmt: skip
    unique = {}
    for table in drawn:
        unique[(table["dataset"], table["venue"], table["instrument"])] = table
    return {
        "kind": kind,
        "log": log,
        "slot": draw(st.integers(0, 1)),  # which of the log's loads in flight it belongs to
        "last": draw(st.booleans()),
        "tables": list(unique.values()),
        "committed": kind == "finish" or draw(st.sampled_from([True, True, True, False])),
        "gap": draw(st.integers(1, 1_000)),
    }


def _write(root, spec):
    """Write the history. Returns one entry per batch that was opened."""
    clock = SteppingClock([step["gap"] for step in spec])
    store = AppendOnlyStore(root, clock=clock)
    in_flight = {}
    history = []
    for step in spec:
        load = None
        if step["kind"] == "outside":
            batch = store._batch()
        else:
            slot = (step["log"], step["slot"])
            load = in_flight.get(slot)
            if load is None or load.finished:
                load = in_flight[slot] = store.load(step["log"])
            if step["kind"] == "finish":
                result = load.finish()
                history.append({"knowledge_ts": result.knowledge_ts, "committed": True,
                                "tables": [], "load_id": load.load_id, "log": step["log"],
                                "last": True})  # fmt: skip
                continue
            batch = load.batch(last=step["last"])
        for table in step["tables"]:
            dataset = DATASETS[table["dataset"]]
            batch.append(dataset, table["venue"], table["instrument"],
                         build(dataset, table["rows"]))  # fmt: skip
        if step["committed"]:
            batch.commit()
        else:
            # The writer died: this batch has no marker, and no load in flight
            # can be continued by the next writer.
            store = AppendOnlyStore(root, clock=clock)
            in_flight = {}
        history.append({
            "knowledge_ts": batch.knowledge_ts,
            "committed": step["committed"],
            "tables": step["tables"],
            "load_id": None if load is None else load.load_id,
            "log": step["log"],
            "last": load is not None and step["last"],
        })  # fmt: skip
    return history


def _oracle(history, available_at, version):
    known = [h for h in history if h["committed"] and h["knowledge_ts"] <= version]
    loads = {}  # load_id -> (log, complete within the version)
    for h in known:
        if h["load_id"] is not None:
            log, done = loads.get(h["load_id"], (h["log"], False))
            loads[h["load_id"]] = (log, done or h["last"])
    chosen = {}
    for log in {log for log, _ in loads.values()}:
        mine = [(done, load_id) for load_id, (other, done) in loads.items() if other == log]
        chosen[log] = max(mine)[1]  # a complete load first, then the latest

    latest = {}
    for h in sorted(known, key=lambda h: h["knowledge_ts"]):
        if h["load_id"] is not None and chosen.get(h["log"]) != h["load_id"]:
            continue
        for table in h["tables"]:
            for row in table["rows"]:
                if h["load_id"] is None and row["log_id"] in chosen:
                    continue  # the log is read from its load
                if row["recv_ts"] > available_at:
                    continue
                identity = (table["dataset"], table["venue"], table["instrument"],
                            _identity(table["dataset"], row))  # fmt: skip
                latest[identity] = {**row, "knowledge_ts": h["knowledge_ts"]}

    first = {}
    for identity, row in latest.items():
        dataset_name, venue, instrument, _ = identity
        if dataset_name != "trades":
            continue
        trade = (venue, instrument, row["trade_id"])
        if trade not in first or (row["log_id"], row["seq"]) < first[trade][0]:
            first[trade] = ((row["log_id"], row["seq"]), identity)
    shown = {identity for _, identity in first.values()}
    return {k: v for k, v in latest.items() if k[0] != "trades" or k in shown}


def _versions(history, fraction):
    committed = {h["knowledge_ts"] for h in history if h["committed"]}
    latest = max(committed, default=0)
    return committed, None if fraction is None else int(latest * fraction)


@settings(max_examples=80, deadline=None)
@given(
    spec=st.lists(steps(), min_size=1, max_size=7),
    available_at=AVAILABILITY,
    version_fraction=st.one_of(st.none(), st.floats(0, 1)),
    window=st.tuples(st.sampled_from(RECV_TIMES), st.sampled_from(RECV_TIMES)),
)
def test_a_session_reads_each_log_from_its_latest_complete_load(
    spec, available_at, version_fraction, window
):
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
        root = Path(directory) / "store"
        history = _write(root, spec)
        committed, version = _versions(history, version_fraction)
        with PITSession(root, available_at=available_at, version=version) as session:
            expected = _oracle(history, available_at, session.version)
            _check_session(session, expected, available_at, session.version, committed, window)


@settings(max_examples=30, deadline=None)
@given(
    spec=st.lists(steps(), min_size=1, max_size=6),
    times=st.lists(AVAILABILITY, min_size=1, max_size=4),
    version_fraction=st.one_of(st.none(), st.floats(0, 1)),
    window=st.tuples(st.sampled_from(RECV_TIMES), st.sampled_from(RECV_TIMES)),
)
def test_a_cursor_reads_the_same_loads_as_a_session(spec, times, version_fraction, window):
    times = sorted(times)
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as directory:
        root = Path(directory) / "store"
        history = _write(root, spec)
        committed, version = _versions(history, version_fraction)
        with ReplayCursor(root, start=times[0], end=times[-1], version=version) as cursor:
            for now in times:
                cursor.advance(now)
                expected = _oracle(history, now, cursor.session.version)
                _check_session(cursor.session, expected, now, cursor.session.version,
                               committed, window)  # fmt: skip
