"""PITSession: the only read path into the store (architecture section 3.2).

See conftest.py for the timeline of the ``history`` store.
"""

import copy
import inspect
import pickle
from pathlib import Path, PurePath

import duckdb
import pyarrow as pa
import pytest
from _builders import (
    INT64_MAX,
    LOG,
    NS_PER_DAY,
    NS_PER_SECOND,
    T0,
    V1_EFFECTIVE,
    V2_EFFECTIVE,
    FakeClock,
    instrument_table,
    trade_table,
)

import quantagent.data as data_package
from quantagent.data.schema import BOOK_EVENTS, INSTRUMENTS, TRADES, Side
from quantagent.data.session import InstrumentMeta, PITSession, QueryError, QueryRejected
from quantagent.data.store import AppendOnlyStore

ALL_TIME = (0, INT64_MAX)
KRK = ("kraken", "ETH-USD")


def trade_view(session, venue="kraken", instrument="ETH-USD", start=0, end=INT64_MAX):
    table = session.trades(venue, instrument, start, end)
    return [(r["trade_id"], r["price"], r["knowledge_ts"]) for r in table.to_pylist()]


# --- construction ------------------------------------------------------------


def test_as_of_is_required(history):
    with pytest.raises(TypeError, match="as_of"):
        PITSession(history.root)


def test_as_of_is_keyword_only(history):
    with pytest.raises(TypeError):
        PITSession(history.root, 5000)


def test_as_of_has_no_default():
    parameter = inspect.signature(PITSession).parameters["as_of"]
    assert parameter.default is inspect.Parameter.empty
    assert parameter.kind is inspect.Parameter.KEYWORD_ONLY


@pytest.mark.parametrize("bad", [None, 5000.0, "5000", True, [5000]])
def test_as_of_must_be_an_int(history, bad):
    with pytest.raises(TypeError, match="as_of"):
        PITSession(history.root, as_of=bad)


@pytest.mark.parametrize("bad", [-1, 2**63])
def test_as_of_must_be_an_int64_nanosecond_time(history, bad):
    with pytest.raises(ValueError, match="as_of"):
        PITSession(history.root, as_of=bad)


def test_missing_store_is_an_error(tmp_path):
    with pytest.raises(FileNotFoundError):
        PITSession(tmp_path / "nowhere", as_of=1)


def test_empty_store_gives_empty_typed_results(tmp_path):
    with PITSession(tmp_path, as_of=5000) as session:
        trades = session.trades(*KRK, *ALL_TIME)
        assert trades.num_rows == 0
        assert trades.schema.equals(TRADES.stored_schema)
        assert session.book_events(*KRK, *ALL_TIME).schema.equals(BOOK_EVENTS.stored_schema)
        assert session.instruments(*KRK).schema.equals(INSTRUMENTS.stored_schema)
        assert session.instrument(*KRK) is None
        assert session.sql("SELECT count(*) AS n FROM trades").to_pylist() == [{"n": 0}]


def test_a_stray_parquet_file_in_the_store_is_an_error(history):
    stray = history.root / "trades" / "venue=kraken" / "instrument=ETH-USD" / "extra.parquet"
    stray.write_bytes(b"x")
    with pytest.raises(ValueError, match="unexpected file"):
        PITSession(history.root, as_of=5000)


def test_a_store_path_containing_an_equals_sign_is_refused(tmp_path):
    # It would be read as a hive partition key.
    odd = tmp_path / "a=b"
    odd.mkdir()
    with pytest.raises(ValueError, match="="):
        PITSession(odd, as_of=1)


def test_session_is_a_context_manager_and_unusable_after_close(history):
    with PITSession(history.root, as_of=5000) as session:
        assert session.trades(*KRK, *ALL_TIME).num_rows == 2
    for call in (
        lambda: session.trades(*KRK, *ALL_TIME),
        lambda: session.book_events(*KRK, *ALL_TIME),
        lambda: session.instruments(*KRK),
        lambda: session.instrument(*KRK),
        lambda: session.sql("SELECT 1 AS x FROM trades"),
    ):
        with pytest.raises(RuntimeError, match="closed"):
            call()
    session.close()  # closing twice is harmless


# --- typed methods: point-in-time visibility ----------------------------------


def test_nothing_is_visible_before_the_first_batch(history):
    with PITSession(history.root, as_of=999) as session:
        assert trade_view(session) == []
        assert session.instrument(*KRK) is None
        assert session.instruments(*KRK).num_rows == 0
        assert session.book_events(*KRK, *ALL_TIME).num_rows == 0


def test_as_of_is_inclusive(history):
    with PITSession(history.root, as_of=1999) as session:
        assert trade_view(session) == []
    with PITSession(history.root, as_of=2000) as session:
        assert trade_view(session) == [("a", 100, 2000), ("b", 101, 2000)]


def test_trades_grow_with_as_of(history):
    with PITSession(history.root, as_of=8000) as session:
        assert trade_view(session) == [("a", 150, 4000), ("b", 101, 2000), ("c", 103, 8000)]
    with PITSession(history.root, as_of=INT64_MAX) as session:
        assert trade_view(session) == [("a", 150, 4000), ("b", 101, 2000), ("c", 103, 8000)]


def test_a_correction_is_visible_only_from_its_knowledge_time(history):
    with PITSession(history.root, as_of=3999) as before:
        assert trade_view(before) == [("a", 100, 2000), ("b", 101, 2000)]
    with PITSession(history.root, as_of=4000) as after:
        assert trade_view(after) == [("a", 150, 4000), ("b", 101, 2000)]


def test_book_event_correction_is_visible_only_from_its_knowledge_time(history):
    def levels(as_of):
        with PITSession(history.root, as_of=as_of) as session:
            rows = session.book_events(*KRK, *ALL_TIME).to_pylist()
        return sorted((r["side"], r["price"], r["qty"], r["knowledge_ts"]) for r in rows)

    assert levels(4999) == []
    assert levels(5000) == [(Side.SELL, 102, 7, 5000), (Side.BUY, 99, 5, 5000)]
    assert levels(8999) == [(Side.SELL, 102, 7, 5000), (Side.BUY, 99, 5, 5000)]
    assert levels(9000) == [(Side.SELL, 102, 7, 5000), (Side.BUY, 99, 6, 9000)]


def test_venues_and_instruments_are_separate(history):
    with PITSession(history.root, as_of=9000) as session:
        assert trade_view(session, "coinbase", "BTC-USD") == [("a", 200, 3000)]
        assert trade_view(session, "coinbase", "ETH-USD") == []
        assert trade_view(session, "kraken", "BTC-USD") == []


def test_time_range_is_half_open_on_event_ts(history):
    with PITSession(history.root, as_of=9000) as session:
        second = T0 + NS_PER_SECOND
        assert [t[0] for t in trade_view(session, start=T0, end=second)] == ["a"]
        assert [t[0] for t in trade_view(session, start=T0, end=second + 1)] == ["a", "b"]
        assert [t[0] for t in trade_view(session, start=T0 + 1, end=INT64_MAX)] == ["b", "c"]
        assert trade_view(session, start=second, end=second) == []
        # Spans date partitions.
        assert [t[0] for t in trade_view(session, start=second, end=T0 + 2 * NS_PER_DAY)] == [
            "b",
            "c",
        ]


def test_a_correction_that_moves_event_ts_leaves_the_old_time_range(root):
    store = AppendOnlyStore(root, clock=FakeClock(1000, 2000))
    store.append(TRADES, *KRK, trade_table(("a", T0, 100)))
    store.append(TRADES, *KRK, trade_table(("a", T0 + NS_PER_DAY, 100)))  # other partition

    with PITSession(root, as_of=1000) as before:
        assert [t[0] for t in trade_view(before, start=T0, end=T0 + 1)] == ["a"]
    with PITSession(root, as_of=2000) as after:
        assert trade_view(after, start=T0, end=T0 + 1) == []
        assert trade_view(after, start=T0 + NS_PER_DAY, end=INT64_MAX) == [("a", 100, 2000)]
        assert trade_view(after) == [("a", 100, 2000)]


def test_typed_results_use_the_stored_schema_and_replay_order(history):
    with PITSession(history.root, as_of=9000) as session:
        trades = session.trades(*KRK, *ALL_TIME)
        assert isinstance(trades, pa.Table)
        assert trades.schema.equals(TRADES.stored_schema)
        order = [(r["log_id"], r["seq"]) for r in trades.to_pylist()]
        assert order == [(LOG, 0), (LOG, 1), (LOG, 2)]  # replay order (ADR 0013)
        assert session.book_events(*KRK, *ALL_TIME).schema.equals(BOOK_EVENTS.stored_schema)
        assert session.instruments(*KRK).schema.equals(INSTRUMENTS.stored_schema)


@pytest.mark.parametrize("method", ["trades", "book_events"])
@pytest.mark.parametrize(
    "arguments",
    [
        ("../x", "ETH-USD", 0, 1),
        ("kraken", "a/b", 0, 1),
        ("kraken", "x' OR '1'='1", 0, 1),
        ("kraken", "ETH-USD", 0.0, 1),
        ("kraken", "ETH-USD", 0, "1"),
        ("kraken", "ETH-USD", None, 1),
        ("kraken", "ETH-USD", True, 1),
        ("kraken", "ETH-USD", -1, 1),
        ("kraken", "ETH-USD", 0, 2**63),
    ],
)
def test_typed_methods_validate_their_arguments(history, method, arguments):
    with PITSession(history.root, as_of=9000) as session:
        with pytest.raises((TypeError, ValueError)):
            getattr(session, method)(*arguments)


# --- instrument metadata -------------------------------------------------------


def test_instrument_versions_as_known(history):
    def versions(as_of):
        with PITSession(history.root, as_of=as_of) as session:
            rows = session.instruments(*KRK).to_pylist()
        return [(r["event_ts"], r["tick_size"], r["knowledge_ts"]) for r in rows]

    assert versions(999) == []
    assert versions(1000) == [(V1_EFFECTIVE, 1, 1000)]
    assert versions(6000) == [(V1_EFFECTIVE, 1, 1000), (V2_EFFECTIVE, 10, 6000)]
    assert versions(7000) == [(V1_EFFECTIVE, 5, 7000), (V2_EFFECTIVE, 10, 6000)]


def test_instrument_in_effect_is_the_latest_known_version_at_or_before_the_time(history):
    with PITSession(history.root, as_of=5999) as session:
        meta = session.instrument(*KRK, at=T0)
        assert isinstance(meta, InstrumentMeta)
        assert (meta.venue, meta.instrument) == KRK
        assert (meta.event_ts, meta.tick_size, meta.knowledge_ts) == (V1_EFFECTIVE, 1, 1000)
        assert (meta.price_exp, meta.qty_exp, meta.lot_size) == (2, 5, 1)
        # The later version is not known yet, whatever time is asked about.
        assert session.instrument(*KRK, at=V2_EFFECTIVE + 1).tick_size == 1
        # Nothing was in effect before the first version.
        assert session.instrument(*KRK, at=V1_EFFECTIVE - 1) is None
        assert session.instrument(*KRK, at=V1_EFFECTIVE).tick_size == 1

    with PITSession(history.root, as_of=6000) as session:
        # Known now, but it only takes effect in the future.
        assert session.instrument(*KRK, at=T0).tick_size == 1
        assert session.instrument(*KRK, at=V2_EFFECTIVE - 1).tick_size == 1
        assert session.instrument(*KRK, at=V2_EFFECTIVE).tick_size == 10

    with PITSession(history.root, as_of=7000) as session:
        assert session.instrument(*KRK, at=T0).tick_size == 5
        assert session.instrument("coinbase", "BTC-USD") is None


def test_instrument_defaults_to_the_version_in_effect_at_as_of(root):
    store = AppendOnlyStore(root, clock=FakeClock(100, 200))
    store.append(INSTRUMENTS, *KRK, instrument_table(1_000, tick_size=1))
    store.append(INSTRUMENTS, *KRK, instrument_table(5_000, tick_size=2, seq=1))
    with PITSession(root, as_of=4_999) as session:
        assert session.instrument(*KRK).tick_size == 1
    with PITSession(root, as_of=5_000) as session:
        assert session.instrument(*KRK).tick_size == 2


def test_instrument_meta_is_immutable(history):
    with PITSession(history.root, as_of=9000) as session:
        meta = session.instrument(*KRK, at=T0)
    assert meta is not None
    with pytest.raises(AttributeError):
        meta.tick_size = 99


# --- a session is a fixed snapshot ---------------------------------------------


def test_later_appends_are_invisible_to_an_open_session(history):
    with PITSession(history.root, as_of=8000) as session:
        before = trade_view(session)
        history.store.append(TRADES, *KRK, trade_table(("a", T0, 999), ("z", T0, 1)))
        assert trade_view(session) == before
        assert session.sql("SELECT max(knowledge_ts) AS k FROM trades").to_pylist() == [{"k": 8000}]


def test_as_of_cannot_be_changed(history):
    with PITSession(history.root, as_of=3999) as session:
        assert session.as_of == 3999
        with pytest.raises(AttributeError):
            session.as_of = 9000
        with pytest.raises(AttributeError):
            del session.as_of
        with pytest.raises(AttributeError):
            session.anything_else = 1
        # Ordinary assignment to the private fields is refused as well.
        for name in type(session).__slots__:
            with pytest.raises(AttributeError):
                setattr(session, name, 9000)
            with pytest.raises(AttributeError):
                delattr(session, name)
        assert session.as_of == 3999
        assert trade_view(session) == [("a", 100, 2000), ("b", 101, 2000)]


def test_an_engine_error_that_quotes_a_file_path_is_redacted(history):
    with PITSession(history.root, as_of=9000) as session:
        # Test-only tampering, to make the engine fail on a file it may read.
        victim = history.files_by_kts[2000][0]
        victim.write_bytes(b"this is not a parquet file")
        with pytest.raises(QueryError) as caught:
            session.sql("SELECT * FROM trades")
    message = str(caught.value)
    assert "<store>" in message
    assert history.root.as_posix() not in message.replace("\\", "/")
    for link in (caught.value.__cause__, caught.value.__context__):
        assert link is None


def test_forcing_the_private_as_of_does_not_change_what_is_visible(history):
    # Deliberately breaking encapsulation: the views were fixed at construction,
    # so even this cannot widen what the session reads.
    with PITSession(history.root, as_of=3999) as session:
        for name in list(getattr(type(session), "__slots__", ())) + list(
            getattr(session, "__dict__", {})
        ):
            if isinstance(getattr(session, name, None), int):
                object.__setattr__(session, name, 9000)
        assert trade_view(session) == [("a", 100, 2000), ("b", 101, 2000)]
        assert session.sql("SELECT max(knowledge_ts) AS k FROM trades").to_pylist() == [
            {"k": 3000}  # the coinbase trade; still nothing after 3999
        ]


def test_sessions_cannot_be_copied_pickled_or_subclassed(history):
    with PITSession(history.root, as_of=3999) as session:
        with pytest.raises(TypeError):
            pickle.dumps(session)
        with pytest.raises(TypeError):
            copy.copy(session)
        with pytest.raises(TypeError):
            copy.deepcopy(session)
    with pytest.raises(TypeError):

        class Wider(PITSession):
            pass


# --- nothing leaks through the public surface -----------------------------------


def test_public_surface_is_exactly_the_query_methods(history):
    public = {name for name in dir(PITSession) if not name.startswith("_")}
    assert public == {
        "as_of",
        "trades",
        "book_events",
        "stream_events",
        "instruments",
        "instrument",
        "sql",
        "close",
    }


def _holds_connection_or_path(value, root: Path, depth=0) -> bool:
    if isinstance(value, duckdb.DuckDBPyConnection | duckdb.DuckDBPyRelation | PurePath):
        return True
    if isinstance(value, str | bytes):
        text = value if isinstance(value, str) else value.decode("utf-8", "replace")
        return str(root) in text or root.as_posix() in text
    if depth > 3:
        return False
    if isinstance(value, dict):
        return any(_holds_connection_or_path(v, root, depth + 1) for v in value.values())
    if isinstance(value, list | tuple | set | frozenset):
        return any(_holds_connection_or_path(v, root, depth + 1) for v in value)
    return False


def test_public_attributes_and_results_hold_no_connection_and_no_path(history):
    with PITSession(history.root, as_of=9000) as session:
        values = {
            "as_of": session.as_of,
            "repr": repr(session),
            "str": str(session),
            "trades": session.trades(*KRK, *ALL_TIME),
            "book_events": session.book_events(*KRK, *ALL_TIME),
            "instruments": session.instruments(*KRK),
            "instrument": session.instrument(*KRK, at=T0),
            "sql": session.sql("SELECT * FROM trades"),
        }
        for name, value in values.items():
            assert not _holds_connection_or_path(value, history.root), name
            if isinstance(value, pa.Table):
                assert not _holds_connection_or_path(value.to_pylist(), history.root), name
                assert not _holds_connection_or_path(value.schema.metadata, history.root), name
                assert not _holds_connection_or_path(value.schema.names, history.root), name
        for name in dir(session):
            if name.startswith("_"):
                continue
            attribute = getattr(session, name)
            assert not _holds_connection_or_path(attribute, history.root), name
            # Bound methods must not carry state in defaults or closures.
            function = getattr(attribute, "__func__", None)
            if function is not None:
                assert function.__closure__ is None, name
                assert not _holds_connection_or_path(function.__defaults__, history.root), name


def test_error_messages_do_not_reveal_store_paths(history):
    with PITSession(history.root, as_of=9000) as session:
        failures = []
        for query in (
            "SELECT no_such_column FROM trades",
            f"SELECT * FROM read_parquet('{history.file(9000)}')",
            f"SELECT * FROM '{history.file(9000)}'",
            "SELECT CAST(trade_id AS BIGINT) FROM trades",
            "SELECT * FROM trades WHERE",
        ):
            with pytest.raises((QueryError, QueryRejected)) as caught:
                session.sql(query)
            failures.append(caught.value)
        assert {type(error) for error in failures} == {QueryError, QueryRejected}
        for error in failures:
            # Not even on the exception chain.
            for link in (error, error.__cause__, error.__context__):
                if link is not None:
                    assert history.root.as_posix() not in str(link).replace("\\", "/")


def test_the_data_package_exports_no_other_read_path():
    # The store is write-only and nothing else in the package reads files back.
    public_store = {n for n in dir(AppendOnlyStore) if not n.startswith("_")}
    assert public_store == {"append"}
    modules = {
        name for name in dir(data_package) if not name.startswith("_") and name != "annotations"
    }
    # rawlog, recorder and adapters are the capture side: they feed the store.
    # The raw log is readable (the replay engine needs it), but it is not a
    # research read path; the import-lint planned for the signal layer must
    # keep layers 3 and 5 away from it. Adding a module here is a deliberate act.
    assert modules <= {
        "fixedpoint",
        "schema",
        "store",
        "session",
        "sqlguard",
        "rawlog",
        "recorder",
        "adapters",
        "quality",  # reads only through PITSession (tested in test_quality.py)
        "clock",  # measures the recording clock; reads no data
        "book",  # reads only through PITSession (tested in test_book.py)
        "logorder",  # orders raw log records for replay; reads no store data
    }


# --- order: (log_id, seq), never recv_ts (ADR 0012, ADR 0013) ---------------------------------


def test_rows_come_back_in_log_and_seq_order_even_if_recv_ts_goes_backwards(root):
    from quantagent.data.schema import table_from_rows

    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    later_log = "20231114T130000.000000000Z-000000000001"
    rows = []
    # In the first log the wall clock stepped back after seq 1.
    for log_id, seq, recv_ts in [(LOG, 0, T0 + 500), (LOG, 1, T0 + 900), (LOG, 2, T0 + 100),
                                 (later_log, 0, T0 + 50), (later_log, 1, T0 + 60)]:  # fmt: skip
        rows.append({"event_ts": T0, "event_ts_source": "venue", "recv_ts": recv_ts,
                     "log_id": log_id, "seq": seq, "trade_id": f"{log_id[-1]}{seq}",
                     "price": 1, "qty": 1, "side": 1})  # fmt: skip
    store.append(TRADES, *KRK, table_from_rows(TRADES, rows))
    with PITSession(root, as_of=INT64_MAX) as session:
        got = session.trades(*KRK, 0, INT64_MAX).to_pylist()
    assert [(r["log_id"], r["seq"]) for r in got] == [
        (LOG, 0), (LOG, 1), (LOG, 2), (later_log, 0), (later_log, 1)
    ]  # fmt: skip
