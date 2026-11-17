"""Loads (ADR 0018, review finding R04).

Every ingest of a raw log is a **load** with its own ``load_id``. A load can
span several batches; its last batch says so in its commit marker. For each
``log_id``, a session reads only the rows of that log's latest load all of
whose batches are within its version: a log is read whole, from one load.
Rows an older load produced and the latest one does not stop appearing. The
older load's files stay on disk, so the store is still append-only, and a
session at an older version still reads them.

A log with no complete load is read from its latest incomplete load (the
owner's decision on Review item 1), so a log's first load is readable while
it is written; a reload in progress never hides the previous complete load.

Loads are the store's only public write path. The private ``_batch`` and
``_append`` write outside any load, for tests; such rows are read record by
record, as before ADR 0018, for logs that have no load.
"""

import json
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest
from _builders import INT64_MAX, LOG, T0, FakeClock, book_table, instrument_table, trade_table
from test_fixtures import INSTRUMENT, fixture_path
from test_review_2026_11 import records_of, stream_book_updates, write_log
from test_session_sql import _connection

from quantagent.data import book, quality
from quantagent.data.adapters import ingest
from quantagent.data.quality import QualityConfig
from quantagent.data.rawlog import RawRecord, log_id_of
from quantagent.data.schema import (
    BOOK_EVENTS,
    DATASETS,
    INSTRUMENTS,
    STREAM_EVENTS,
    TRADES,
    Side,
    StreamEventKind,
    table_from_rows,
)
from quantagent.data.session import PITSession, ReplayCursor
from quantagent.data.store import STORE_LAYOUT, AppendOnlyStore, StoreVersionError

OTHER = "20231114T130000.000000000Z-0000000000bb"
KRK = ("kraken", "ETH-USD")
ALL = (0, INT64_MAX)


def marker(root, knowledge_ts):
    path = Path(root) / "_batches" / f"batch-{knowledge_ts:020d}.json"
    return json.loads(path.read_text(encoding="utf-8"))


def markers(root):
    return sorted((Path(root) / "_batches").glob("batch-*.json"))


def prices(session, venue="kraken", instrument="ETH-USD"):
    """(trade_id, price) of every trade the session shows."""
    rows = session.trades(venue, instrument, *ALL).to_pylist()
    return sorted((r["trade_id"], r["price"]) for r in rows)


def gap_event(seq, *, log_id=LOG):
    return table_from_rows(STREAM_EVENTS, [{
        "event_ts": T0 + seq, "event_ts_source": "receipt", "recv_ts": T0 + seq, "log_id": log_id,
        "seq": seq, "event_index": 0, "kind": StreamEventKind.MESSAGE_REJECTED,
        "connection": "ws", "expected": None, "observed": None, "detail": "invalid_json",
    }])  # fmt: skip


def load_whole(store, log_id, *tables):
    """One complete load of ``log_id`` in one batch, like ``ingest``."""
    load = store.load(log_id)
    with load.batch(last=True) as batch:
        for dataset, (venue, instrument), table in tables:
            batch.append(dataset, venue, instrument, table)
    return load


# --- the writer ---------------------------------------------------------------------------


def test_a_load_is_named_by_the_knowledge_time_of_its_first_batch(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 2_000))
    load = store.load(LOG)
    assert (load.log_id, load.load_id, load.finished) == (LOG, None, False)
    with load.batch() as batch:
        batch.append(TRADES, *KRK, trade_table(("a", T0, 1)))
    assert (load.load_id, load.finished) == (1_000, False)
    with load.batch(last=True) as batch:
        batch.append(TRADES, *KRK, trade_table(("b", T0, 2), seq=1))
    assert (load.load_id, load.finished) == (1_000, True)

    first, last = marker(root, 1_000), marker(root, 2_000)
    assert first["layout"] == last["layout"] == STORE_LAYOUT == 3
    assert first["load"] == {"load_id": 1_000, "log_id": LOG, "last": False}
    assert last["load"] == {"load_id": 1_000, "log_id": LOG, "last": True}


def test_a_batch_outside_a_load_says_so_in_its_marker(root):
    AppendOnlyStore(root, clock=FakeClock(1_000))._append(TRADES, *KRK, trade_table(("a", T0, 1)))
    assert marker(root, 1_000)["load"] is None


def test_the_public_operations_of_a_load(root):
    load = AppendOnlyStore(root).load(LOG)
    public = {name for name in dir(load) if not name.startswith("_")}
    assert public == {"batch", "finish", "finished", "load_id", "log_id"}


def test_a_load_needs_a_well_formed_log_id(root):
    store = AppendOnlyStore(root)
    for bad in ("", "test-log", None, 5):
        with pytest.raises((TypeError, ValueError), match="log"):
            store.load(bad)


def test_a_load_takes_rows_of_its_own_log_only(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    with store.load(LOG).batch(last=True) as batch:
        with pytest.raises(ValueError, match=OTHER):
            batch.append(TRADES, *KRK, trade_table(("x", T0, 1), log_id=OTHER))
        mixed = table_from_rows(TRADES, [
            *trade_table(("a", T0, 1)).to_pylist(),
            *trade_table(("x", T0, 1), log_id=OTHER, seq=1).to_pylist(),
        ])  # fmt: skip
        with pytest.raises(ValueError, match="load of"):
            batch.append(TRADES, *KRK, mixed)
        # A refused table does not end the batch (as outside a load).
        batch.append(TRADES, *KRK, trade_table(("a", T0, 1)))
    assert marker(root, 1_000)["rows"] == {"trades": 1}


def test_a_finished_load_takes_no_more_batches(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 2_000))
    load = load_whole(store, LOG, (TRADES, KRK, trade_table(("a", T0, 1))))
    for use in (load.batch, lambda: load.batch(last=True), load.finish):
        with pytest.raises(RuntimeError, match="finished"):
            use()
    assert len(markers(root)) == 1


def test_finish_ends_a_load_with_a_batch_of_no_rows(root):
    """For a loader that does not know which batch is its last until the log ends."""
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 2_000))
    load = store.load(LOG)
    with load.batch() as batch:
        batch.append(TRADES, *KRK, trade_table(("a", T0, 1)))
    result = load.finish()
    assert (result.knowledge_ts, result.rows, result.files) == (2_000, 0, ())
    assert load.finished
    content = marker(root, 2_000)
    assert content["files"] == [] and content["rows"] == {}
    assert content["load"] == {"load_id": 1_000, "log_id": LOG, "last": True}


def test_a_load_with_no_rows_at_all_is_still_complete(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    load = store.load(LOG)
    with load.batch(last=True):
        pass
    assert load.finished and load.load_id == 1_000
    assert marker(root, 1_000)["load"]["last"] is True


def test_an_empty_batch_that_does_not_end_its_load_publishes_nothing(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    with store.load(LOG).batch():
        pass
    assert markers(root) == []


def test_a_last_batch_that_fails_leaves_the_load_open(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 2_000))
    load = store.load(LOG)
    with pytest.raises(RuntimeError, match="failed"):
        with load.batch(last=True) as batch:
            batch.append(TRADES, *KRK, trade_table(("a", T0, 1)))
            raise RuntimeError("the load failed")
    assert not load.finished and markers(root) == []
    with load.batch(last=True) as batch:
        batch.append(TRADES, *KRK, trade_table(("a", T0, 1)))
    assert load.finished
    assert marker(root, 2_000)["load"] == {"load_id": 1_000, "log_id": LOG, "last": True}


def test_a_store_of_the_previous_layout_is_refused(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    store._append(TRADES, *KRK, trade_table(("a", T0, 1)))
    path = markers(root)[0]
    content = json.loads(path.read_text(encoding="utf-8"))
    del content["load"]
    content["layout"] = 2  # what ADR 0017 wrote
    path.write_text(json.dumps(content), encoding="utf-8")
    with pytest.raises(StoreVersionError, match="rebuild"):
        AppendOnlyStore(root)
    with pytest.raises(StoreVersionError, match="layout 2"):
        PITSession(root, available_at=INT64_MAX)


# --- what a session reads ---------------------------------------------------------------


def test_a_reload_replaces_the_log_and_rows_it_no_longer_produces_disappear(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 2_000))
    first = load_whole(store, LOG,
                       (TRADES, KRK, trade_table(("a", T0, 1), ("b", T0, 2), ("c", T0, 3))),
                       (STREAM_EVENTS, KRK, gap_event(1)),
                       (BOOK_EVENTS, KRK, book_table((7, T0, Side.BUY, 99, 5))),
                       (INSTRUMENTS, KRK, instrument_table(T0 - 1)))  # fmt: skip
    # The log normalized again: c and the rejected message are gone, b changed.
    load_whole(store, LOG,
               (TRADES, KRK, trade_table(("a", T0, 1), ("b", T0, 20))),
               (INSTRUMENTS, KRK, instrument_table(T0 - 1, tick_size=5)))  # fmt: skip

    with PITSession(root, available_at=INT64_MAX) as session:
        assert prices(session) == [("a", 1), ("b", 20)]
        assert session.stream_events(*KRK, *ALL).num_rows == 0
        assert session.book_events(*KRK, *ALL).num_rows == 0
        assert [r["tick_size"] for r in session.instruments(*KRK).to_pylist()] == [5]
        for name in DATASETS:  # the views, too
            (count,) = session.sql(f"SELECT count(*) AS n FROM {name}").column("n").to_pylist()
            assert count == {"trades": 2, "instruments": 1}.get(name, 0), name
    # Nothing was removed: the first load is on disk and is what version 1000 reads.
    assert first.load_id == 1_000
    with PITSession(root, available_at=INT64_MAX, version=1_000) as session:
        assert prices(session) == [("a", 1), ("b", 2), ("c", 3)]
        assert session.stream_events(*KRK, *ALL).num_rows == 1
        assert session.book_events(*KRK, *ALL).num_rows == 1


def test_a_reload_leaves_other_logs_alone(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 2_000, 3_000))
    load_whole(store, LOG, (TRADES, KRK, trade_table(("a", T0, 1))))
    load_whole(store, OTHER, (TRADES, KRK, trade_table(("x", T0, 9), log_id=OTHER)))
    load_whole(store, LOG, (TRADES, KRK, trade_table(("b", T0, 2))))
    with PITSession(root, available_at=INT64_MAX) as session:
        assert prices(session) == [("b", 2), ("x", 9)]


def test_a_reload_in_progress_never_hides_the_previous_complete_load(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 2_000, 3_000, 4_000))
    load_whole(store, LOG, (TRADES, KRK, trade_table(("a", T0, 1), ("b", T0, 2))))
    reload = store.load(LOG)
    for knowledge_ts, seq, (trade, price) in ((2_000, 0, ("a", 10)), (3_000, 2, ("c", 30))):
        with reload.batch() as batch:  # the reload's chunks
            batch.append(TRADES, *KRK, trade_table((trade, T0, price), seq=seq))
        with PITSession(root, available_at=INT64_MAX) as session:
            assert session.version == knowledge_ts
            # Not half of the reload and half of the old load: the old load, whole.
            assert prices(session) == [("a", 1), ("b", 2)]
    with reload.batch(last=True) as batch:
        batch.append(TRADES, *KRK, trade_table(("b", T0, 20), seq=1))
    with PITSession(root, available_at=INT64_MAX) as session:
        assert prices(session) == [("a", 10), ("b", 20), ("c", 30)]
    # A version that was read once reads the same afterwards.
    for version in (2_000, 3_000):
        with PITSession(root, available_at=INT64_MAX, version=version) as session:
            assert prices(session) == [("a", 1), ("b", 2)]


def test_a_logs_first_load_is_read_while_it_is_being_written(root):
    """No complete load yet: the latest incomplete load is read, as far as its
    committed batches within the version go (owner decision, ADR 0018)."""
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 2_000, 3_000))
    load = store.load(LOG)
    with load.batch() as batch:
        batch.append(TRADES, *KRK, trade_table(("a", T0, 1)))
    with PITSession(root, available_at=INT64_MAX) as session:
        assert prices(session) == [("a", 1)]
    with load.batch() as batch:
        batch.append(TRADES, *KRK, trade_table(("b", T0, 2), seq=1))
    with PITSession(root, available_at=INT64_MAX) as session:
        assert prices(session) == [("a", 1), ("b", 2)]
    load.finish()
    with PITSession(root, available_at=INT64_MAX) as session:
        assert prices(session) == [("a", 1), ("b", 2)]
    # Each version reads what it read while the load was being written.
    with PITSession(root, available_at=INT64_MAX, version=1_000) as session:
        assert prices(session) == [("a", 1)]
    with PITSession(root, available_at=INT64_MAX, version=2_000) as session:
        assert prices(session) == [("a", 1), ("b", 2)]


def test_of_two_incomplete_loads_the_latest_is_read(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 2_000))
    for price in (1, 2):
        with store.load(LOG).batch() as batch:  # neither load ever ends
            batch.append(TRADES, *KRK, trade_table(("a", T0, price)))
    with PITSession(root, available_at=INT64_MAX) as session:
        assert prices(session) == [("a", 2)]
    with PITSession(root, available_at=INT64_MAX, version=1_000) as session:
        assert prices(session) == [("a", 1)]


def test_a_load_that_died_after_a_complete_one_is_not_read(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 2_000))
    load_whole(store, LOG, (TRADES, KRK, trade_table(("a", T0, 1))))
    with store.load(LOG).batch() as batch:  # committed, and then the process died
        batch.append(TRADES, *KRK, trade_table(("a", T0, 10)))
    with PITSession(root, available_at=INT64_MAX) as session:
        assert prices(session) == [("a", 1)]
    restarted = AppendOnlyStore(root, clock=FakeClock(3_000))
    load_whole(restarted, LOG, (TRADES, KRK, trade_table(("a", T0, 100))))
    with PITSession(root, available_at=INT64_MAX) as session:
        assert prices(session) == [("a", 100)]


def test_a_first_load_that_died_is_read_until_the_log_has_a_complete_load(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    with store.load(LOG).batch() as batch:  # committed, and then the process died
        batch.append(TRADES, *KRK, trade_table(("a", T0, 1), ("b", T0, 2), seq=0))
    with PITSession(root, available_at=INT64_MAX) as session:
        assert prices(session) == [("a", 1), ("b", 2)]
    restarted = AppendOnlyStore(root, clock=FakeClock(2_000))
    load_whole(restarted, LOG, (TRADES, KRK, trade_table(("a", T0, 100))))
    with PITSession(root, available_at=INT64_MAX) as session:
        assert prices(session) == [("a", 100)]


def test_within_a_load_a_later_batch_corrects_a_record(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 2_000))
    load = store.load(LOG)
    with load.batch() as batch:
        batch.append(TRADES, *KRK, trade_table(("a", T0, 1), ("b", T0, 2)))
    with load.batch(last=True) as batch:
        batch.append(TRADES, *KRK, trade_table(("a", T0, 5)))
    with PITSession(root, available_at=INT64_MAX) as session:
        assert prices(session) == [("a", 5), ("b", 2)]


def test_the_latest_load_is_the_one_begun_last(root):
    """Loads of one log are ordered by load_id, the knowledge time of their
    first batch. Two loads of one log in flight at once is not how ingest
    works, but the rule still gives one answer."""
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 2_000, 3_000))
    early, late = store.load(LOG), store.load(LOG)
    with early.batch() as batch:
        batch.append(TRADES, *KRK, trade_table(("a", T0, 1)))
    with late.batch(last=True) as batch:
        batch.append(TRADES, *KRK, trade_table(("a", T0, 2)))
    early.finish()
    assert (early.load_id, late.load_id) == (1_000, 2_000)
    with PITSession(root, available_at=INT64_MAX) as session:
        assert prices(session) == [("a", 2)]


def test_both_cutoffs_still_apply_inside_the_chosen_load(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 2_000))
    load_whole(store, LOG, (TRADES, KRK, trade_table(("a", T0, 1), ("b", T0 + 10, 2))))
    load_whole(store, LOG, (TRADES, KRK, trade_table(("a", T0, 3), ("b", T0 + 10, 4))))
    received_a = T0 + 5_000_000  # the builders' lag
    with PITSession(root, available_at=received_a) as session:
        assert prices(session) == [("a", 3)]
    with PITSession(root, available_at=received_a, version=1_000) as session:
        assert prices(session) == [("a", 1)]


def test_rows_written_outside_a_load_are_read_only_for_logs_with_no_load(root):
    """Tests write outside a load to build stores row by row (production never
    does, ADR 0018). Such rows are read for a log only while it has no load."""
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 2_000, 3_000))
    both = table_from_rows(TRADES, [
        *trade_table(("a", T0, 1), ("x", T0, 8)).to_pylist(),
        *trade_table(("y", T0, 9), log_id=OTHER, seq=0).to_pylist(),
    ])  # fmt: skip
    store._append(TRADES, *KRK, both)  # one batch, two logs, no load
    with PITSession(root, available_at=INT64_MAX) as session:
        assert prices(session) == [("a", 1), ("x", 8), ("y", 9)]
    load = store.load(LOG)
    with load.batch() as batch:
        batch.append(TRADES, *KRK, trade_table(("a", T0, 10)))
    with PITSession(root, available_at=INT64_MAX) as session:
        # LOG is read from its load, complete or not; OTHER has none, so its row stays.
        assert prices(session) == [("a", 10), ("y", 9)]
    load.finish()
    with PITSession(root, available_at=INT64_MAX) as session:
        assert prices(session) == [("a", 10), ("y", 9)]
    with PITSession(root, available_at=INT64_MAX, version=1_000) as session:
        assert prices(session) == [("a", 1), ("x", 8), ("y", 9)]


def test_a_replay_cursor_does_not_read_a_loaded_logs_rows_from_outside_its_load(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 2_000))
    store._append(TRADES, *KRK, trade_table(("x", T0, 8)))  # outside a load
    load_whole(store, LOG, (TRADES, KRK, trade_table(("a", T0, 1))))
    with ReplayCursor(root, start=0, end=INT64_MAX) as cursor:
        for now in (T0 + 5_000_000, INT64_MAX):
            cursor.advance(now)
            assert prices(cursor.session) == [("a", 1)]


def test_the_engine_cannot_open_the_files_of_a_replaced_load(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 2_000))
    load_whole(store, LOG, (TRADES, KRK, trade_table(("a", T0, 1))))
    load_whole(store, LOG, (TRADES, KRK, trade_table(("a", T0, 2))))
    old = (Path(root) / marker(root, 1_000)["files"][0]).resolve().as_posix()
    new = (Path(root) / marker(root, 2_000)["files"][0]).resolve().as_posix()
    with PITSession(root, available_at=INT64_MAX) as session:
        con = _connection(session)
        allowed = con.sql("SELECT current_setting('allowed_paths')").fetchall()[0][0]
        assert new in allowed and old not in allowed
        for query in (f"SELECT * FROM read_parquet('{old}')", f"SELECT * FROM '{old}'"):
            with pytest.raises(duckdb.PermissionException):
                con.sql(query).fetchall()
    with PITSession(root, available_at=INT64_MAX, version=1_000) as session:
        allowed = _connection(session).sql("SELECT current_setting('allowed_paths')").fetchall()
        assert old in allowed[0][0] and new not in allowed[0][0]


def test_a_replay_cursor_reads_the_loads_of_its_version(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 2_000))
    load_whole(store, LOG, (TRADES, KRK, trade_table(("a", T0, 1), ("b", T0 + 10, 2))))
    load_whole(store, LOG, (TRADES, KRK, trade_table(("b", T0 + 10, 4), seq=1)))
    for version, want in ((None, [("b", 4)]), (1_000, [("a", 1), ("b", 2)])):
        with ReplayCursor(root, start=0, end=INT64_MAX, version=version) as cursor:
            assert prices(cursor.session) == []
            cursor.advance(INT64_MAX)
            assert prices(cursor.session) == want


# --- markers that do not describe loads correctly -----------------------------------------


def _rewrite(root, knowledge_ts, change):
    path = Path(root) / "_batches" / f"batch-{knowledge_ts:020d}.json"
    content = json.loads(path.read_text(encoding="utf-8"))
    change(content)
    path.write_text(json.dumps(content), encoding="utf-8")


def _two_batch_load(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 2_000))
    load = store.load(LOG)
    with load.batch() as batch:
        batch.append(TRADES, *KRK, trade_table(("a", T0, 1)))
    with load.batch(last=True) as batch:
        batch.append(TRADES, *KRK, trade_table(("b", T0, 2), seq=1))


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda c: c.pop("load"), "load"),
        (lambda c: c.update(load="x"), "load"),
        (lambda c: c["load"].update(log_id=OTHER), "log"),
        (lambda c: c["load"].update(log_id="not-a-log"), "log"),
        (lambda c: c["load"].update(load_id=3_000), "load"),
        (lambda c: c["load"].update(load_id="1000"), "load"),
        (lambda c: c["load"].update(last="yes"), "load"),
        (lambda c: c["load"].pop("last"), "load"),
    ],
    ids=["missing", "not an object", "two logs", "malformed log id", "load id after its batch",
         "load id not an integer", "last not a boolean", "no last"],
)  # fmt: skip
def test_a_marker_whose_load_does_not_add_up_is_refused(root, change, message):
    _two_batch_load(root)
    _rewrite(root, 2_000, change)
    with pytest.raises(ValueError, match=message):
        PITSession(root, available_at=INT64_MAX)


def test_a_batch_of_a_load_after_its_last_batch_is_refused(root):
    _two_batch_load(root)
    _rewrite(root, 1_000, lambda c: c["load"].update(last=True))
    with pytest.raises(ValueError, match="after its last"):
        PITSession(root, available_at=INT64_MAX)


# --- ingest: one load per raw log --------------------------------------------------------


def test_each_ingest_of_a_raw_log_is_a_load_of_one_batch(tmp_path):
    root = tmp_path / "store"
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 2_000))
    for _ in range(2):
        ingest(fixture_path("kraken"), store)
    log_id = log_id_of(records_of("kraken")[0])
    assert [marker(root, k)["load"] for k in (1_000, 2_000)] == [
        {"load_id": 1_000, "log_id": log_id, "last": True},
        {"load_id": 2_000, "log_id": log_id, "last": True},
    ]


def _everything(root, version=None):
    """Every row of every dataset a session shows, without knowledge_ts."""
    found = {}
    with PITSession(root, available_at=INT64_MAX, version=version) as session:
        for name in DATASETS:
            table = session.sql(f"SELECT * EXCLUDE (knowledge_ts) FROM {name}")
            found[name] = sorted((tuple(sorted(r.items())) for r in table.to_pylist()), key=repr)
    return found


@pytest.mark.parametrize("venue", ["kraken", pytest.param("coinbase", marks=pytest.mark.slow)])
def test_loading_a_log_twice_reads_the_same_as_loading_it_once(tmp_path, venue):
    once, twice = tmp_path / "once", tmp_path / "twice"
    ingest(fixture_path(venue), AppendOnlyStore(once))
    store = AppendOnlyStore(twice)
    ingest(fixture_path(venue), store)
    ingest(fixture_path(venue), store)
    assert _everything(once) == _everything(twice)
    files = list(twice.rglob("*.parquet"))
    assert len(files) == 2 * len(list(once.rglob("*.parquet")))  # both loads are on disk


@pytest.mark.slow
def test_a_reload_after_a_fix_clears_the_coinbase_errors_the_fix_resolved(tmp_path):
    """The review's R04 experiment on Coinbase left two phantom errors: the
    rejected message, and the sequence gap it caused."""
    records = records_of("coinbase")
    victim = stream_book_updates(records)[len(stream_book_updates(records)) // 2]
    unparsed = [
        RawRecord(r.recv_ts, r.seq, r.venue, r.channel,
                  b'{"broken' if r.seq == victim.seq else r.payload, r.recv_mono_ns)
        for r in records
    ]  # fmt: skip
    root = tmp_path / "store"
    store = AppendOnlyStore(root)
    before = ingest(write_log(tmp_path / "before-fix.qaraw", unparsed), store)
    assert len(before.rejects) == 1
    with PITSession(root, available_at=INT64_MAX) as session:
        result = quality.check_stream_integrity(session, "coinbase", INSTRUMENT, *ALL,
                                                QualityConfig())  # fmt: skip
    assert {f.what for f in result.findings} == {"message_rejected", "sequence_gap"}

    ingest(fixture_path("coinbase"), store)  # the fix
    with PITSession(root, available_at=INT64_MAX) as session:
        result = quality.check_stream_integrity(session, "coinbase", INSTRUMENT, *ALL,
                                                QualityConfig())  # fmt: skip
        table = session.book_events("coinbase", INSTRUMENT, *ALL)
        events = session.stream_events("coinbase", INSTRUMENT, *ALL).to_pylist()
    assert result.findings == []
    for _ in book.replay("coinbase", book.messages_from_table(table), events):
        pass  # raised StreamFault at the rejected record before the reload


def test_an_empty_raw_log_loads_nothing(tmp_path):
    path = write_log(tmp_path / "empty.qaraw", [])
    root = tmp_path / "store"
    report = ingest(path, AppendOnlyStore(root))
    assert report.records == 0
    assert markers(root) == []


def test_old_loads_stay_on_disk_byte_for_byte(tmp_path):
    root = tmp_path / "store"
    store = AppendOnlyStore(root)
    ingest(fixture_path("kraken"), store)
    before = {p: p.read_bytes() for p in root.rglob("*") if p.is_file()}
    ingest(fixture_path("kraken"), store)
    for path, content in before.items():
        assert path.read_bytes() == content
    assert all(pq.read_schema(p) for p in root.rglob("*.parquet"))
