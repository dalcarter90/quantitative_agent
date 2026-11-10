"""Append-only Parquet store (ADR 0006).

These tests read the files directly with pyarrow and DuckDB. That is allowed
here because they test the store itself; everything above the data core reads
through PITSession (Phase 1b).
"""

import hashlib
import inspect
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from quantagent.data.schema import (
    BOOK_EVENTS,
    INSTRUMENTS,
    TRADES,
    BookEventKind,
    SchemaError,
    Side,
    table_from_rows,
)
from quantagent.data.store import AppendOnlyStore, AppendResult

NS_PER_DAY = 86_400 * 1_000_000_000
DAY_19675 = 19675 * NS_PER_DAY  # 2023-11-14T00:00:00Z
T0 = DAY_19675 + 12 * 3600 * 1_000_000_000  # 2023-11-14T12:00:00Z


class FakeClock:
    """Returns the given values in order, then keeps returning the last one."""

    def __init__(self, *values):
        self._values = list(values)

    def __call__(self):
        if len(self._values) > 1:
            return self._values.pop(0)
        return self._values[0]


def trades(*specs):
    """Each spec is (trade_id, event_ts, price)."""
    rows = [
        {
            "event_ts": event_ts,
            "recv_ts": event_ts + 5_000_000,
            "seq": i,
            "trade_id": trade_id,
            "price": price,
            "qty": 100,
            "side": Side.BUY,
        }
        for i, (trade_id, event_ts, price) in enumerate(specs)
    ]
    return table_from_rows(TRADES, rows)


def instrument(event_ts, *, price_exp=2, qty_exp=5, tick_size=1, lot_size=1):
    row = {
        "event_ts": event_ts,
        "recv_ts": event_ts + 5_000_000,
        "seq": 0,
        "price_exp": price_exp,
        "qty_exp": qty_exp,
        "tick_size": tick_size,
        "lot_size": lot_size,
    }
    return table_from_rows(INSTRUMENTS, [row])


def data_files(root: Path) -> list[Path]:
    return sorted(root.rglob("*.parquet"))


def all_files(root: Path) -> list[Path]:
    return sorted(p for p in root.rglob("*") if p.is_file())


def digests(root: Path) -> dict[Path, str]:
    return {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in all_files(root)}


def read_all(root: Path, dataset) -> list[dict]:
    rows = []
    for path in data_files(root / dataset.name):
        rows.extend(pq.read_table(path).to_pylist())
    return sorted(rows, key=lambda r: (r["knowledge_ts"], r["seq"]))


@pytest.fixture
def root(tmp_path):
    return tmp_path / "store"


# --- layout and content ------------------------------------------------------


def test_append_writes_one_parquet_file_in_the_hive_partition(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    result = store.append(TRADES, "binance", "BTCUSDT", trades(("a", T0, 9712310)))

    assert isinstance(result, AppendResult)
    assert result.knowledge_ts == 1_000
    assert result.rows == 1
    expected = (
        root
        / "trades"
        / "venue=binance"
        / "instrument=BTCUSDT"
        / "date=2023-11-14"
        / f"part-{1_000:020d}.parquet"
    )
    assert list(result.files) == [expected]
    assert data_files(root) == [expected]


def test_stored_rows_equal_input_plus_knowledge_ts(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    table = trades(("a", T0, 9712310), ("b", T0 + 1, 9712320))
    result = store.append(TRADES, "binance", "BTCUSDT", table)

    stored = pq.read_table(result.files[0])
    assert stored.schema.equals(TRADES.stored_schema)
    assert stored.column("knowledge_ts").to_pylist() == [1_000, 1_000]
    assert stored.drop_columns(["knowledge_ts"]).to_pylist() == table.to_pylist()
    for name in ("event_ts", "recv_ts", "knowledge_ts", "price", "qty"):
        assert stored.schema.field(name).type == pa.int64()


def test_files_record_dataset_and_schema_version(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    result = store.append(TRADES, "binance", "BTCUSDT", trades(("a", T0, 1)))
    metadata = pq.read_schema(result.files[0]).metadata
    assert metadata[b"qa.dataset"] == b"trades"
    assert metadata[b"qa.schema_version"] == str(TRADES.version).encode()


def test_rows_are_partitioned_by_utc_date_of_event_ts(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    last_ns_of_day = DAY_19675 + NS_PER_DAY - 1
    table = trades(
        ("a", DAY_19675, 1),
        ("b", last_ns_of_day, 2),
        ("c", last_ns_of_day + 1, 3),
    )
    result = store.append(TRADES, "binance", "BTCUSDT", table)

    by_date = {p.parent.name: pq.read_table(p).column("trade_id").to_pylist() for p in result.files}
    assert by_date == {"date=2023-11-14": ["a", "b"], "date=2023-11-15": ["c"]}
    # One append is one batch: every file of it shares one knowledge_ts.
    assert {p.name for p in result.files} == {f"part-{1_000:020d}.parquet"}
    assert result.rows == 3


def test_venues_instruments_and_datasets_are_separate_partitions(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    store.append(TRADES, "binance", "BTCUSDT", trades(("a", T0, 1)))
    store.append(TRADES, "coinbase", "BTC-USD", trades(("a", T0, 1)))
    store.append(INSTRUMENTS, "binance", "BTCUSDT", instrument(T0))

    relative = {p.relative_to(root).parts[:3] for p in data_files(root)}
    assert relative == {
        ("trades", "venue=binance", "instrument=BTCUSDT"),
        ("trades", "venue=coinbase", "instrument=BTC-USD"),
        ("instruments", "venue=binance", "instrument=BTCUSDT"),
    }


def test_book_events_snapshot_and_delta_are_stored(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    rows = [
        {
            "event_ts": T0,
            "recv_ts": T0 + 1,
            "seq": 0,
            "kind": BookEventKind.SNAPSHOT,
            "venue_seq_first": None,
            "venue_seq_last": 100,
            "side": Side.BUY,
            "price": 9712300,
            "qty": 50,
        },
        {
            "event_ts": T0 + 10,
            "recv_ts": T0 + 11,
            "seq": 1,
            "kind": BookEventKind.DELTA,
            "venue_seq_first": 101,
            "venue_seq_last": 103,
            "side": Side.BUY,
            "price": 9712300,
            "qty": 0,
        },
    ]
    store.append(BOOK_EVENTS, "binance", "BTCUSDT", table_from_rows(BOOK_EVENTS, rows))
    stored = read_all(root, BOOK_EVENTS)
    assert [r["kind"] for r in stored] == [BookEventKind.SNAPSHOT, BookEventKind.DELTA]
    assert [r["venue_seq_first"] for r in stored] == [None, 101]


def test_duckdb_reads_the_store_with_hive_partitioning(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 2_000))
    store.append(TRADES, "binance", "BTCUSDT", trades(("a", T0, 9712310)))
    store.append(TRADES, "coinbase", "BTC-USD", trades(("b", T0 + NS_PER_DAY, 9712320)))

    glob = (root / "trades" / "**" / "*.parquet").as_posix()
    rows = duckdb.sql(
        "SELECT venue, instrument, CAST(date AS VARCHAR), trade_id, price, knowledge_ts, "
        "typeof(price), typeof(knowledge_ts), typeof(event_ts) "
        f"FROM read_parquet('{glob}', hive_partitioning = true) ORDER BY knowledge_ts"
    ).fetchall()
    assert rows == [
        ("binance", "BTCUSDT", "2023-11-14", "a", 9712310, 1_000, "BIGINT", "BIGINT", "BIGINT"),
        ("coinbase", "BTC-USD", "2023-11-15", "b", 9712320, 2_000, "BIGINT", "BIGINT", "BIGINT"),
    ]


# --- knowledge time ----------------------------------------------------------


def test_append_takes_no_knowledge_ts_argument():
    parameters = inspect.signature(AppendOnlyStore.append).parameters
    assert list(parameters) == ["self", "dataset", "venue", "instrument", "rows"]


def test_caller_supplied_knowledge_ts_column_is_rejected(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    table = trades(("a", T0, 1))
    with_kts = table.append_column("knowledge_ts", pa.array([5], type=pa.int64()))
    with pytest.raises(SchemaError, match="knowledge_ts"):
        store.append(TRADES, "binance", "BTCUSDT", with_kts)
    # A table already in stored layout is rejected too.
    stored_layout = with_kts.select(TRADES.stored_schema.names)
    with pytest.raises(SchemaError, match="knowledge_ts"):
        store.append(TRADES, "binance", "BTCUSDT", stored_layout)
    assert all_files(root) == []


def test_knowledge_ts_strictly_increases_even_if_the_clock_goes_backwards(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 500, 500, 1_001, 5_000))
    seen = [
        store.append(TRADES, "binance", "BTCUSDT", trades((f"t{i}", T0, i))).knowledge_ts
        for i in range(5)
    ]
    assert seen == [1_000, 1_001, 1_002, 1_003, 5_000]


def test_knowledge_ts_keeps_increasing_across_datasets_and_partitions(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    a = store.append(TRADES, "binance", "BTCUSDT", trades(("a", T0, 1))).knowledge_ts
    b = store.append(TRADES, "coinbase", "BTC-USD", trades(("a", T0, 1))).knowledge_ts
    c = store.append(INSTRUMENTS, "binance", "BTCUSDT", instrument(T0)).knowledge_ts
    assert a < b < c


def test_knowledge_ts_never_decreases_after_reopening_with_an_earlier_clock(root):
    first = AppendOnlyStore(root, clock=FakeClock(9_000))
    first.append(INSTRUMENTS, "binance", "BTCUSDT", instrument(T0))
    first.append(TRADES, "binance", "BTCUSDT", trades(("a", T0, 1)))  # knowledge_ts 9_001

    reopened = AppendOnlyStore(root, clock=FakeClock(10))
    result = reopened.append(TRADES, "coinbase", "BTC-USD", trades(("b", T0, 2)))
    assert result.knowledge_ts == 9_002


def test_default_clock_is_wall_time_in_nanoseconds(root):
    import time

    before = time.time_ns()
    result = AppendOnlyStore(root).append(TRADES, "binance", "BTCUSDT", trades(("a", T0, 1)))
    after = time.time_ns()
    assert before <= result.knowledge_ts <= after


@pytest.mark.parametrize("bad", [1.5, "1000", None, True, -1, 2**63])
def test_a_clock_that_does_not_return_int64_nanoseconds_is_an_error(root, bad):
    store = AppendOnlyStore(root, clock=lambda: bad)
    with pytest.raises((TypeError, ValueError)):
        store.append(TRADES, "binance", "BTCUSDT", trades(("a", T0, 1)))
    assert all_files(root) == []


# --- append-only -------------------------------------------------------------


def test_the_only_public_operation_is_append():
    public = {name for name in dir(AppendOnlyStore) if not name.startswith("_")}
    assert public == {"append"}


def test_appending_never_changes_the_bytes_of_existing_files(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    store.append(TRADES, "binance", "BTCUSDT", trades(("a", T0, 1), ("b", T0 + 1, 2)))
    store.append(INSTRUMENTS, "binance", "BTCUSDT", instrument(T0))
    before = digests(root)
    assert len(before) == 2

    # Same partition, same key, other partitions, other datasets, a reopen.
    store.append(TRADES, "binance", "BTCUSDT", trades(("a", T0, 999)))
    store.append(TRADES, "binance", "BTCUSDT", trades(("c", T0 + 2, 3)))
    store.append(TRADES, "binance", "BTCUSDT", trades(("d", T0 + NS_PER_DAY, 4)))
    store.append(INSTRUMENTS, "binance", "BTCUSDT", instrument(T0, tick_size=5))
    AppendOnlyStore(root, clock=FakeClock(1)).append(
        TRADES, "binance", "BTCUSDT", trades(("e", T0, 5))
    )

    after = digests(root)
    assert len(after) == 7
    for path, digest in before.items():
        assert after[path] == digest, path


def test_a_correction_is_a_new_row_and_the_original_stays_on_disk(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 2_000))
    original = store.append(TRADES, "binance", "BTCUSDT", trades(("a", T0, 9712310)))
    original_bytes = original.files[0].read_bytes()

    correction = store.append(TRADES, "binance", "BTCUSDT", trades(("a", T0, 9712399)))

    assert correction.files[0] != original.files[0]
    assert original.files[0].read_bytes() == original_bytes
    rows = read_all(root, TRADES)
    assert [(r["trade_id"], r["price"], r["knowledge_ts"]) for r in rows] == [
        ("a", 9712310, 1_000),
        ("a", 9712399, 2_000),
    ]
    assert TRADES.key == ("trade_id",)
    assert correction.knowledge_ts > original.knowledge_ts


def test_an_existing_file_is_never_overwritten(root):
    # Something else already occupies the path the next append would use.
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    first = store.append(TRADES, "binance", "BTCUSDT", trades(("a", T0, 1)))
    squatter = first.files[0].with_name(f"part-{1_001:020d}.parquet")
    squatter.write_bytes(b"not ours")

    with pytest.raises(FileExistsError):
        store.append(TRADES, "binance", "BTCUSDT", trades(("b", T0, 2)))
    assert squatter.read_bytes() == b"not ours"


def test_a_failed_multi_partition_append_publishes_nothing(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    first = store.append(TRADES, "binance", "BTCUSDT", trades(("a", T0 + NS_PER_DAY, 1)))
    squatter = first.files[0].with_name(f"part-{1_001:020d}.parquet")
    squatter.write_bytes(b"not ours")
    before = digests(root)

    two_days = trades(("b", T0, 2), ("c", T0 + NS_PER_DAY, 3))
    with pytest.raises(FileExistsError):
        store.append(TRADES, "binance", "BTCUSDT", two_days)
    assert digests(root) == before


def test_no_temporary_files_are_left_behind(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    store.append(TRADES, "binance", "BTCUSDT", trades(("a", T0, 1), ("b", T0 + NS_PER_DAY, 2)))
    assert all_files(root) == data_files(root)
    assert len(data_files(root)) == 2


# --- instrument metadata -----------------------------------------------------


def test_instrument_metadata_is_stored_bitemporally(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 2_000, 3_000))
    later = T0 + 30 * NS_PER_DAY
    store.append(INSTRUMENTS, "binance", "BTCUSDT", instrument(T0, tick_size=1))
    # The venue changes the tick size: a new version, effective later.
    store.append(INSTRUMENTS, "binance", "BTCUSDT", instrument(later, tick_size=10))
    # We learn the first version was recorded wrongly: same key, later knowledge.
    store.append(INSTRUMENTS, "binance", "BTCUSDT", instrument(T0, tick_size=5))

    rows = read_all(root, INSTRUMENTS)
    assert INSTRUMENTS.key == ("event_ts",)
    assert [(r["event_ts"], r["tick_size"], r["knowledge_ts"]) for r in rows] == [
        (T0, 1, 1_000),
        (later, 10, 2_000),
        (T0, 5, 3_000),
    ]
    for row in rows:
        assert {"price_exp", "qty_exp", "tick_size", "lot_size", "recv_ts"} <= set(row)


# --- input validation --------------------------------------------------------


@pytest.mark.parametrize(
    "name",
    ["", ".", "..", "../x", "a/b", "a\\b", "a b", "a=b", "C:x", "x\n", "é", ".hidden", "a" * 65],
)
def test_venue_and_instrument_names_cannot_escape_or_break_the_layout(root, name):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    with pytest.raises(ValueError):
        store.append(TRADES, name, "BTCUSDT", trades(("a", T0, 1)))
    with pytest.raises(ValueError):
        store.append(TRADES, "binance", name, trades(("a", T0, 1)))
    assert all_files(root) == []


@pytest.mark.parametrize("name", ["binance", "BTCUSDT", "BTC-USD", "BTC_USD", "1000SHIB.P", "x"])
def test_ordinary_venue_and_instrument_names_are_accepted(root, name):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    store.append(TRADES, name, name, trades(("a", T0, 1)))


def test_wrong_schema_is_rejected_and_writes_nothing(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    with pytest.raises(SchemaError):
        store.append(BOOK_EVENTS, "binance", "BTCUSDT", trades(("a", T0, 1)))
    with pytest.raises(TypeError):
        store.append(TRADES, "binance", "BTCUSDT", [{"trade_id": "a"}])
    with pytest.raises(TypeError):
        store.append("trades", "binance", "BTCUSDT", trades(("a", T0, 1)))
    assert all_files(root) == []


def test_empty_append_is_rejected(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    with pytest.raises(ValueError, match="empty"):
        store.append(TRADES, "binance", "BTCUSDT", trades())
    assert all_files(root) == []


def test_a_rejected_append_does_not_consume_a_knowledge_ts(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    with pytest.raises(ValueError):
        store.append(TRADES, "binance", "BTCUSDT", trades())
    result = store.append(TRADES, "binance", "BTCUSDT", trades(("a", T0, 1)))
    assert result.knowledge_ts == 1_000
