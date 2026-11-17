"""Append-only Parquet store (ADR 0006, ADR 0017).

These tests read the files directly with pyarrow and DuckDB. That is allowed
here because they test the store itself; everything above the data core reads
through PITSession (Phase 1b).

Since ADR 0017 a write is a batch: one knowledge time for every table in it,
files partitioned by the UTC date of ``recv_ts``, and a commit marker written
last. ``append`` is a batch of one table.
"""

import hashlib
import inspect
import json
import os
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
from quantagent.data.store import AppendOnlyStore, AppendResult, StoreVersionError

LOG = "20231114T120000.000000000Z-000000000000"

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


LAG = 5_000_000  # the builders' recv_ts is this long after event_ts


def trades(*specs):
    """Each spec is (trade_id, event_ts, price). recv_ts is event_ts + LAG."""
    rows = [
        {
            "event_ts": event_ts,
            "event_ts_source": "venue",
            "recv_ts": event_ts + LAG,
            "log_id": LOG,
            "seq": i,
            "trade_id": trade_id,
            "price": price,
            "qty": 100,
            "side": Side.BUY,
        }
        for i, (trade_id, event_ts, price) in enumerate(specs)
    ]
    return table_from_rows(TRADES, rows)


def instrument(event_ts, *, price_exp=2, qty_exp=5, tick_size=1, lot_size=1, seq=0):
    row = {
        "event_ts": event_ts,
        "event_ts_source": "venue",
        "recv_ts": event_ts + 5_000_000,
        "log_id": LOG,
        "seq": seq,
        "price_exp": price_exp,
        "qty_exp": qty_exp,
        "price_precision": price_exp,
        "qty_precision": qty_exp,
        "tick_size": tick_size,
        "lot_size": lot_size,
    }
    return table_from_rows(INSTRUMENTS, [row])


def data_files(root: Path) -> list[Path]:
    return sorted(root.rglob("*.parquet"))


def all_files(root: Path) -> list[Path]:
    return sorted(p for p in root.rglob("*") if p.is_file())


def markers(root: Path) -> list[Path]:
    """The commit markers, one per committed batch (ADR 0017)."""
    return sorted((root / "_batches").glob("batch-*.json"))


def part(knowledge_ts: int, index: int = 0) -> str:
    """A data file's name: its batch's knowledge time, then its number in the directory."""
    return f"part-{knowledge_ts:020d}-{index:06d}.parquet"


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
    result = store._append(TRADES, "kraken", "ETH-USD", trades(("a", T0, 9712310)))

    assert isinstance(result, AppendResult)
    assert result.knowledge_ts == 1_000
    assert result.rows == 1
    expected = (
        root / "trades" / "venue=kraken" / "instrument=ETH-USD" / "date=2023-11-14" / part(1_000)
    )
    assert list(result.files) == [expected]
    assert data_files(root) == [expected]


def test_stored_rows_equal_input_plus_knowledge_ts(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    table = trades(("a", T0, 9712310), ("b", T0 + 1, 9712320))
    result = store._append(TRADES, "kraken", "ETH-USD", table)

    stored = pq.read_table(result.files[0])
    assert stored.schema.equals(TRADES.stored_schema)
    assert stored.column("knowledge_ts").to_pylist() == [1_000, 1_000]
    assert stored.drop_columns(["knowledge_ts"]).to_pylist() == table.to_pylist()
    for name in ("event_ts", "recv_ts", "knowledge_ts", "price", "qty"):
        assert stored.schema.field(name).type == pa.int64()


def test_files_record_dataset_and_schema_version(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    result = store._append(TRADES, "kraken", "ETH-USD", trades(("a", T0, 1)))
    metadata = pq.read_schema(result.files[0]).metadata
    assert metadata[b"qa.dataset"] == b"trades"
    assert metadata[b"qa.schema_version"] == str(TRADES.version).encode()


def test_rows_are_partitioned_by_utc_date_of_recv_ts(root):
    """ADR 0017: the partition is the day we received a row, not the venue's day."""
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    last_ns_of_day = DAY_19675 + NS_PER_DAY - 1
    table = trades(  # received at midnight, at the day's last ns, and one ns later
        ("a", DAY_19675 - LAG, 1),
        ("b", last_ns_of_day - LAG, 2),
        ("c", last_ns_of_day + 1 - LAG, 3),
    )
    assert table.column("event_ts")[0].as_py() < DAY_19675  # the venue's day is the 13th
    result = store._append(TRADES, "kraken", "ETH-USD", table)

    by_date = {p.parent.name: pq.read_table(p).column("trade_id").to_pylist() for p in result.files}
    assert by_date == {"date=2023-11-14": ["a", "b"], "date=2023-11-15": ["c"]}
    # One append is one batch: every file of it shares one knowledge_ts.
    assert {p.name for p in result.files} == {part(1_000)}
    assert result.rows == 3


def test_venues_instruments_and_datasets_are_separate_partitions(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    store._append(TRADES, "kraken", "ETH-USD", trades(("a", T0, 1)))
    store._append(TRADES, "coinbase", "BTC-USD", trades(("a", T0, 1)))
    store._append(INSTRUMENTS, "kraken", "ETH-USD", instrument(T0))

    relative = {p.relative_to(root).parts[:3] for p in data_files(root)}
    assert relative == {
        ("trades", "venue=kraken", "instrument=ETH-USD"),
        ("trades", "venue=coinbase", "instrument=BTC-USD"),
        ("instruments", "venue=kraken", "instrument=ETH-USD"),
    }


def test_book_events_snapshot_and_delta_are_stored(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    rows = [
        {
            "event_ts": T0,
            "event_ts_source": "venue",
            "recv_ts": T0 + 1,
            "log_id": LOG,
            "seq": 0,
            "kind": BookEventKind.SNAPSHOT,
            "venue_seq_first": None,
            "venue_seq_last": 100,
            "checksum": None,
            "depth": None,
            "level_index": 0,
            "side": Side.BUY,
            "price": 9712300,
            "qty": 50,
        },
        {
            "event_ts": T0 + 10,
            "event_ts_source": "venue",
            "recv_ts": T0 + 11,
            "log_id": LOG,
            "seq": 1,
            "kind": BookEventKind.DELTA,
            "venue_seq_first": 101,
            "venue_seq_last": 103,
            "checksum": None,
            "depth": None,
            "level_index": 0,
            "side": Side.BUY,
            "price": 9712300,
            "qty": 0,
        },
    ]
    store._append(BOOK_EVENTS, "kraken", "ETH-USD", table_from_rows(BOOK_EVENTS, rows))
    stored = read_all(root, BOOK_EVENTS)
    assert [r["kind"] for r in stored] == [BookEventKind.SNAPSHOT, BookEventKind.DELTA]
    assert [r["venue_seq_first"] for r in stored] == [None, 101]


def test_duckdb_reads_the_store_with_hive_partitioning(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 2_000))
    store._append(TRADES, "kraken", "ETH-USD", trades(("a", T0, 9712310)))
    store._append(TRADES, "coinbase", "BTC-USD", trades(("b", T0 + NS_PER_DAY, 9712320)))

    glob = (root / "trades" / "**" / "*.parquet").as_posix()
    rows = duckdb.sql(
        "SELECT venue, instrument, CAST(date AS VARCHAR), trade_id, price, knowledge_ts, "
        "typeof(price), typeof(knowledge_ts), typeof(event_ts) "
        f"FROM read_parquet('{glob}', hive_partitioning = true) ORDER BY knowledge_ts"
    ).fetchall()
    assert rows == [
        ("kraken", "ETH-USD", "2023-11-14", "a", 9712310, 1_000, "BIGINT", "BIGINT", "BIGINT"),
        ("coinbase", "BTC-USD", "2023-11-15", "b", 9712320, 2_000, "BIGINT", "BIGINT", "BIGINT"),
    ]


# --- knowledge time ----------------------------------------------------------


def test_append_takes_no_knowledge_ts_argument():
    parameters = inspect.signature(AppendOnlyStore._append).parameters
    assert list(parameters) == ["self", "dataset", "venue", "instrument", "rows"]


def test_caller_supplied_knowledge_ts_column_is_rejected(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    table = trades(("a", T0, 1))
    with_kts = table.append_column("knowledge_ts", pa.array([5], type=pa.int64()))
    with pytest.raises(SchemaError, match="knowledge_ts"):
        store._append(TRADES, "kraken", "ETH-USD", with_kts)
    # A table already in stored layout is rejected too.
    stored_layout = with_kts.select(TRADES.stored_schema.names)
    with pytest.raises(SchemaError, match="knowledge_ts"):
        store._append(TRADES, "kraken", "ETH-USD", stored_layout)
    assert all_files(root) == []


def test_knowledge_ts_strictly_increases_even_if_the_clock_goes_backwards(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 500, 500, 1_001, 5_000))
    seen = [
        store._append(TRADES, "kraken", "ETH-USD", trades((f"t{i}", T0, i))).knowledge_ts
        for i in range(5)
    ]
    assert seen == [1_000, 1_001, 1_002, 1_003, 5_000]


def test_knowledge_ts_keeps_increasing_across_datasets_and_partitions(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    a = store._append(TRADES, "kraken", "ETH-USD", trades(("a", T0, 1))).knowledge_ts
    b = store._append(TRADES, "coinbase", "BTC-USD", trades(("a", T0, 1))).knowledge_ts
    c = store._append(INSTRUMENTS, "kraken", "ETH-USD", instrument(T0)).knowledge_ts
    assert a < b < c


def test_knowledge_ts_never_decreases_after_reopening_with_an_earlier_clock(root):
    first = AppendOnlyStore(root, clock=FakeClock(9_000))
    first._append(INSTRUMENTS, "kraken", "ETH-USD", instrument(T0))
    first._append(TRADES, "kraken", "ETH-USD", trades(("a", T0, 1)))  # knowledge_ts 9_001

    reopened = AppendOnlyStore(root, clock=FakeClock(10))
    result = reopened._append(TRADES, "coinbase", "BTC-USD", trades(("b", T0, 2)))
    assert result.knowledge_ts == 9_002


def test_default_clock_is_wall_time_in_nanoseconds(root):
    import time

    before = time.time_ns()
    result = AppendOnlyStore(root)._append(TRADES, "kraken", "ETH-USD", trades(("a", T0, 1)))
    after = time.time_ns()
    assert before <= result.knowledge_ts <= after


@pytest.mark.parametrize("bad", [1.5, "1000", None, True, -1, 2**63])
def test_a_clock_that_does_not_return_int64_nanoseconds_is_an_error(root, bad):
    store = AppendOnlyStore(root, clock=lambda: bad)
    with pytest.raises((TypeError, ValueError)):
        store._append(TRADES, "kraken", "ETH-USD", trades(("a", T0, 1)))
    assert all_files(root) == []


# --- append-only -------------------------------------------------------------


def test_the_stores_public_surface_is_the_load_api_only():
    """Loads are the only production write path (ADR 0018, Review item 3)."""
    public = {name for name in dir(AppendOnlyStore) if not name.startswith("_")}
    assert public == {"load"}
    load = AppendOnlyStore(Path("unused")).load(LOG)
    assert {name for name in dir(load) if not name.startswith("_")} == {
        "batch", "finish", "finished", "load_id", "log_id"}  # fmt: skip
    assert inspect.signature(AppendOnlyStore.load).parameters.keys() == {"self", "log_id"}


def test_no_production_code_writes_outside_a_load():
    """``_batch`` and ``_append`` are for tests. No module of the package may
    call them (checked on the syntax tree, not the text)."""
    import ast

    import quantagent

    package = Path(quantagent.__file__).parent
    calls = []
    for path in sorted(package.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and node.attr in ("_batch", "_append"):
                calls.append(f"{path.relative_to(package).as_posix()}:{node.lineno}")
    # store.py's own _append opens its batch with _batch; nothing else may.
    assert calls == ["data/store.py:" + str(_own_batch_call_line())], calls


def _own_batch_call_line():
    import quantagent.data.store as store_module

    source = Path(store_module.__file__).read_text(encoding="utf-8").split("\n")
    (line,) = [i + 1 for i, text in enumerate(source) if "self._batch()" in text]
    return line


def test_appending_never_changes_the_bytes_of_existing_files(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    store._append(TRADES, "kraken", "ETH-USD", trades(("a", T0, 1), ("b", T0 + 1, 2)))
    store._append(INSTRUMENTS, "kraken", "ETH-USD", instrument(T0))
    before = digests(root)
    assert len(before) == 4  # two data files and their two commit markers

    # Same partition, same key, other partitions, other datasets, a reopen.
    store._append(TRADES, "kraken", "ETH-USD", trades(("a", T0, 999)))
    store._append(TRADES, "kraken", "ETH-USD", trades(("c", T0 + 2, 3)))
    store._append(TRADES, "kraken", "ETH-USD", trades(("d", T0 + NS_PER_DAY, 4)))
    store._append(INSTRUMENTS, "kraken", "ETH-USD", instrument(T0, tick_size=5))
    AppendOnlyStore(root, clock=FakeClock(1))._append(
        TRADES, "kraken", "ETH-USD", trades(("e", T0, 5))
    )

    after = digests(root)
    assert len(after) == 14 and len(data_files(root)) == 7
    for path, digest in before.items():
        assert after[path] == digest, path


def test_a_correction_is_a_new_row_and_the_original_stays_on_disk(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 2_000))
    original = store._append(TRADES, "kraken", "ETH-USD", trades(("a", T0, 9712310)))
    original_bytes = original.files[0].read_bytes()

    correction = store._append(TRADES, "kraken", "ETH-USD", trades(("a", T0, 9712399)))

    assert correction.files[0] != original.files[0]
    assert original.files[0].read_bytes() == original_bytes
    rows = read_all(root, TRADES)
    assert [(r["trade_id"], r["price"], r["knowledge_ts"]) for r in rows] == [
        ("a", 9712310, 1_000),
        ("a", 9712399, 2_000),
    ]
    # Trades are keyed by raw record (ADR 0018); a correction reuses the record.
    assert TRADES.key == ("log_id", "seq", "trade_id")
    assert correction.knowledge_ts > original.knowledge_ts


def test_an_existing_file_is_never_overwritten(root):
    # Something else already occupies the path the next append would use.
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    first = store._append(TRADES, "kraken", "ETH-USD", trades(("a", T0, 1)))
    squatter = first.files[0].with_name(part(1_001))
    squatter.write_bytes(b"not ours")

    with pytest.raises(FileExistsError):
        store._append(TRADES, "kraken", "ETH-USD", trades(("b", T0, 2)))
    assert squatter.read_bytes() == b"not ours"
    assert len(markers(root)) == 1  # the refused batch was not committed


def test_a_failed_multi_partition_append_publishes_nothing(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    first = store._append(TRADES, "kraken", "ETH-USD", trades(("a", T0 + NS_PER_DAY, 1)))
    squatter = first.files[0].with_name(part(1_001))
    squatter.write_bytes(b"not ours")
    before = digests(root)

    two_days = trades(("b", T0, 2), ("c", T0 + NS_PER_DAY, 3))
    with pytest.raises(FileExistsError):
        store._append(TRADES, "kraken", "ETH-USD", two_days)
    assert digests(root) == before


def test_no_temporary_files_are_left_behind(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    store._append(TRADES, "kraken", "ETH-USD", trades(("a", T0, 1), ("b", T0 + NS_PER_DAY, 2)))
    assert all_files(root) == sorted(data_files(root) + markers(root))
    assert len(data_files(root)) == 2 and len(markers(root)) == 1


# --- instrument metadata -----------------------------------------------------


def test_instrument_metadata_is_stored_bitemporally(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 2_000, 3_000))
    later = T0 + 30 * NS_PER_DAY
    store._append(INSTRUMENTS, "kraken", "ETH-USD", instrument(T0, tick_size=1))
    # The venue changes the tick size: a new version, effective later.
    store._append(INSTRUMENTS, "kraken", "ETH-USD", instrument(later, tick_size=10, seq=1))
    # We learn the first version was recorded wrongly: same key, later knowledge.
    store._append(INSTRUMENTS, "kraken", "ETH-USD", instrument(T0, tick_size=5))

    rows = read_all(root, INSTRUMENTS)
    assert INSTRUMENTS.key == ("log_id", "seq")
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
        store._append(TRADES, name, "ETH-USD", trades(("a", T0, 1)))
    with pytest.raises(ValueError):
        store._append(TRADES, "kraken", name, trades(("a", T0, 1)))
    assert all_files(root) == []


@pytest.mark.parametrize("name", ["kraken", "ETH-USD", "BTC-USD", "BTC_USD", "1000SHIB.P", "x"])
def test_ordinary_venue_and_instrument_names_are_accepted(root, name):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    store._append(TRADES, name, name, trades(("a", T0, 1)))


def test_wrong_schema_is_rejected_and_writes_nothing(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    with pytest.raises(SchemaError):
        store._append(BOOK_EVENTS, "kraken", "ETH-USD", trades(("a", T0, 1)))
    with pytest.raises(TypeError):
        store._append(TRADES, "kraken", "ETH-USD", [{"trade_id": "a"}])
    with pytest.raises(TypeError):
        store._append("trades", "kraken", "ETH-USD", trades(("a", T0, 1)))
    assert all_files(root) == []


def test_empty_append_is_rejected(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    with pytest.raises(ValueError, match="empty"):
        store._append(TRADES, "kraken", "ETH-USD", trades())
    assert all_files(root) == []


def test_a_rejected_append_does_not_consume_a_knowledge_ts(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    with pytest.raises(ValueError):
        store._append(TRADES, "kraken", "ETH-USD", trades())
    result = store._append(TRADES, "kraken", "ETH-USD", trades(("a", T0, 1)))
    assert result.knowledge_ts == 1_000


# --- batches and commit markers (ADR 0017) -----------------------------------------------


def test_a_batch_gives_every_table_in_it_one_knowledge_ts(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 2_000))
    with store._batch() as batch:
        assert batch.knowledge_ts == 1_000
        batch.append(INSTRUMENTS, "kraken", "ETH-USD", instrument(T0))
        batch.append(TRADES, "kraken", "ETH-USD", trades(("a", T0, 1)))
        batch.append(TRADES, "coinbase", "BTC-USD", trades(("b", T0 + NS_PER_DAY, 2)))
    stamps = {pq.read_table(p).column("knowledge_ts")[0].as_py() for p in data_files(root)}
    assert stamps == {1_000}
    assert {p.name for p in data_files(root)} == {part(1_000)}
    assert [p.name for p in markers(root)] == [f"batch-{1_000:020d}.json"]
    # The next batch is a later version.
    assert store._append(TRADES, "kraken", "ETH-USD", trades(("c", T0, 3))).knowledge_ts == 2_000


def test_several_appends_to_one_partition_are_numbered_files_of_the_same_batch(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    with store._batch() as batch:
        batch.append(TRADES, "kraken", "ETH-USD", trades(("a", T0, 1)))
        batch.append(TRADES, "kraken", "ETH-USD", trades(("b", T0 + 1, 2)))
    assert [p.name for p in data_files(root)] == [part(1_000, 0), part(1_000, 1)]
    assert len(markers(root)) == 1


def test_the_commit_marker_is_written_last_and_names_every_file(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    batch = store._batch()
    batch.append(TRADES, "kraken", "ETH-USD", trades(("a", T0, 1), ("b", T0 + NS_PER_DAY, 2)))
    batch.append(INSTRUMENTS, "kraken", "ETH-USD", instrument(T0))
    assert len(data_files(root)) == 3 and markers(root) == []  # written, not yet committed

    result = batch.commit()
    assert isinstance(result, AppendResult)
    assert (result.knowledge_ts, result.rows) == (1_000, 3)
    assert sorted(result.files) == data_files(root)
    (marker,) = markers(root)
    content = json.loads(marker.read_text(encoding="utf-8"))
    assert content["knowledge_ts"] == 1_000
    assert content["layout"] == 3
    assert content["load"] is None  # written outside a load (ADR 0018)
    assert sorted(content["files"]) == sorted(
        p.relative_to(root).as_posix() for p in data_files(root)
    )
    assert content["rows"] == {"instruments": 1, "trades": 2}


def test_a_batch_that_fails_is_abandoned_and_leaves_nothing(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 2_000))
    with pytest.raises(RuntimeError, match="the load failed"):
        with store._batch() as batch:
            batch.append(TRADES, "kraken", "ETH-USD", trades(("a", T0, 1)))
            raise RuntimeError("the load failed")
    assert all_files(root) == []
    # The store is usable again, and the failed batch's knowledge time is not reused.
    assert store._append(TRADES, "kraken", "ETH-USD", trades(("b", T0, 2))).knowledge_ts == 2_000


def test_a_rejected_table_does_not_end_the_batch_by_itself(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    with store._batch() as batch:
        with pytest.raises(SchemaError):
            batch.append(BOOK_EVENTS, "kraken", "ETH-USD", trades(("a", T0, 1)))
        with pytest.raises(ValueError, match="empty"):
            batch.append(TRADES, "kraken", "ETH-USD", trades())
        batch.append(TRADES, "kraken", "ETH-USD", trades(("a", T0, 1)))
    assert len(data_files(root)) == 1 and len(markers(root)) == 1


def test_a_batch_with_no_rows_commits_nothing(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 2_000))
    with store._batch():
        pass
    assert all_files(root) == []
    result = store._batch().commit()
    assert (result.rows, result.files) == (0, ())
    assert all_files(root) == []


def test_only_one_batch_can_be_open_and_a_finished_batch_cannot_be_used(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 2_000, 3_000))
    first = store._batch()
    with pytest.raises(RuntimeError, match="already open"):
        store._batch()
    with pytest.raises(RuntimeError, match="already open"):
        store._append(TRADES, "kraken", "ETH-USD", trades(("a", T0, 1)))
    first.append(TRADES, "kraken", "ETH-USD", trades(("a", T0, 1)))
    first.commit()
    for use in (
        lambda: first.append(TRADES, "kraken", "ETH-USD", trades(("b", T0, 2))),
        first.commit,
    ):
        with pytest.raises(RuntimeError, match="finished"):
            use()
    second = store._batch()
    second.abandon()
    with pytest.raises(RuntimeError, match="finished"):
        second.append(TRADES, "kraken", "ETH-USD", trades(("b", T0, 2)))
    assert len(markers(root)) == 1


def test_a_commit_marker_is_never_overwritten(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    (root / "_batches").mkdir(parents=True)
    squatter = root / "_batches" / f"batch-{1_000:020d}.json"
    squatter.write_text("not ours", encoding="utf-8")
    with pytest.raises(FileExistsError):
        store._append(TRADES, "kraken", "ETH-USD", trades(("a", T0, 1)))
    assert squatter.read_text(encoding="utf-8") == "not ours"
    assert data_files(root) == []  # the batch was abandoned


def test_knowledge_ts_starts_at_one_so_version_zero_means_an_empty_store(root):
    store = AppendOnlyStore(root, clock=FakeClock(0))
    assert store._append(TRADES, "kraken", "ETH-USD", trades(("a", T0, 1))).knowledge_ts == 1


def test_a_reopened_store_continues_after_an_uncommitted_batch(root):
    """A load that died: its files are on disk without a marker. The next
    batch must still get a later knowledge time."""
    crashed = AppendOnlyStore(root, clock=FakeClock(5_000))
    crashed._batch().append(TRADES, "kraken", "ETH-USD", trades(("a", T0, 1)))  # never committed
    assert len(data_files(root)) == 1 and markers(root) == []

    reopened = AppendOnlyStore(root, clock=FakeClock(10))
    result = reopened._append(TRADES, "kraken", "ETH-USD", trades(("b", T0, 2)))
    assert result.knowledge_ts == 5_001
    assert [p.name for p in markers(root)] == [f"batch-{5_001:020d}.json"]


def test_a_batch_cannot_commit_behind_a_later_committed_batch(root):
    """Batches must become readable in knowledge-time order, or a version that
    was readable would change afterwards. Two writers on one root are not
    supported; this is what stops them from breaking that."""
    slow = AppendOnlyStore(root, clock=FakeClock(1_000))
    batch = slow._batch()
    batch.append(TRADES, "kraken", "ETH-USD", trades(("a", T0, 1)))
    other = AppendOnlyStore(root, clock=FakeClock(2_000))
    other._append(TRADES, "kraken", "ETH-USD", trades(("b", T0, 2)))  # committed at 2_000
    with pytest.raises(RuntimeError, match="later batch"):
        batch.commit()
    assert [p.name for p in markers(root)] == [f"batch-{2_000:020d}.json"]


def test_the_marker_is_published_in_one_step_after_every_data_file(root, monkeypatch):
    """The marker appears by one hard link, and by then every file it names exists."""
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    seen = []
    real_link = os.link

    def watch(source, target):
        seen.append((Path(target).name, len(data_files(root)), len(markers(root))))
        real_link(source, target)

    monkeypatch.setattr(os, "link", watch)
    store._append(TRADES, "kraken", "ETH-USD", trades(("a", T0, 1), ("b", T0 + NS_PER_DAY, 2)))
    assert seen == [(f"batch-{1_000:020d}.json", 2, 0)]


def test_a_store_written_before_commit_markers_is_refused_by_the_writer(root):
    directory = root / "trades" / "venue=kraken" / "instrument=ETH-USD" / "date=2023-11-14"
    directory.mkdir(parents=True)
    pq.write_table(trades(("a", T0, 1)), directory / f"part-{500:020d}.parquet")
    with pytest.raises(StoreVersionError, match="rebuild") as raised:
        AppendOnlyStore(root, clock=FakeClock(1_000))
    assert "raw logs" in str(raised.value)


def test_the_marker_directory_exists_before_the_first_data_file(root):
    """A first load that dies leaves data files and no marker. The empty marker
    directory is what tells a reader this is a store of the current layout with
    nothing committed, not one written before commit markers."""
    store = AppendOnlyStore(root, clock=FakeClock(1_000))
    store._batch().append(TRADES, "kraken", "ETH-USD", trades(("a", T0, 1)))  # never committed
    assert len(data_files(root)) == 1
    assert (root / "_batches").is_dir() and markers(root) == []
