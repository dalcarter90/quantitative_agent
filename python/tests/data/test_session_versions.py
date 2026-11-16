"""PITSession refuses stores written in another layout or schema (ADRs 0015, 0017).

Every file the store writes records its dataset and schema version in its
Parquet metadata. A session checks every file it opens, before any query, and
refuses one that does not match the current code with a message saying the
store must be rebuilt from the raw logs.

Since ADR 0017 a store also has a commit marker per batch. A store written
before that has none and is refused the same way.
"""

import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from _builders import INT64_MAX, T0, FakeClock, trade_table

import quantagent.data.book as book_module
import quantagent.data.quality as quality
from quantagent.data.schema import TRADES
from quantagent.data.session import PITSession, StoreVersionError
from quantagent.data.store import AppendOnlyStore

PARTITION = "trades/venue=kraken/instrument=ETH-USD/date=2023-11-14"


def old_trades_schema():
    """The trades schema before ADR 0013 (version 2): no log_id."""
    fields = [f for f in TRADES.stored_schema if f.name != "log_id"]
    return pa.schema(fields, metadata={"qa.dataset": "trades", "qa.schema_version": "2"})


def old_trades_table():
    schema = old_trades_schema()
    values = {"event_ts": [T0], "event_ts_source": ["venue"], "recv_ts": [T0 + 5_000_000],
              "knowledge_ts": [500], "seq": [0], "trade_id": ["old"], "price": [100],
              "qty": [1], "side": [1]}  # fmt: skip
    return pa.table({f.name: pa.array(values[f.name], f.type) for f in schema}, schema=schema)


def write_file(root, table, knowledge_ts=500, partition=PARTITION):
    """A committed batch holding one file, written by hand so its content can
    be whatever the test needs."""
    directory = root / partition
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"part-{knowledge_ts:020d}-000000.parquet"
    pq.write_table(table, path)
    marker = {"layout": 3, "knowledge_ts": knowledge_ts,
              "files": [path.relative_to(root).as_posix()], "rows": {}, "load": None}  # fmt: skip
    (root / "_batches").mkdir(exist_ok=True)
    (root / "_batches" / f"batch-{knowledge_ts:020d}.json").write_text(json.dumps(marker))
    return path


def write_legacy_file(root, table, knowledge_ts=500, partition=PARTITION):
    """A data file as stores had them before ADR 0017: no commit marker."""
    directory = root / partition
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"part-{knowledge_ts:020d}.parquet"
    pq.write_table(table, path)
    return path


def current_store(root):
    store = AppendOnlyStore(root, clock=FakeClock(1_000, 2_000))
    store._append(TRADES, "kraken", "ETH-USD", trade_table(("a", T0, 100)))
    return root


def test_a_store_written_with_the_old_schema_is_refused_with_a_clear_message(tmp_path):
    root = tmp_path / "store"
    write_file(root, old_trades_table())
    with pytest.raises(StoreVersionError) as raised:
        PITSession(root, available_at=INT64_MAX)
    message = str(raised.value)
    assert f"{PARTITION}/part-00000000000000000500-000000.parquet" in message
    assert "trades schema version 2" in message and "version 3" in message
    assert "rebuild" in message and "raw logs" in message
    assert str(tmp_path) not in message  # names the file within the store, not the machine


def test_one_old_file_among_current_ones_is_refused(tmp_path):
    root = current_store(tmp_path / "store")
    with PITSession(root, available_at=INT64_MAX) as session:  # current files alone are fine
        assert session.trades("kraken", "ETH-USD", 0, INT64_MAX).num_rows == 1
    write_file(root, old_trades_table(), knowledge_ts=3_000)
    with pytest.raises(StoreVersionError, match="rebuild"):
        PITSession(root, available_at=INT64_MAX)


def test_a_file_without_a_schema_version_is_refused(tmp_path):
    root = tmp_path / "store"
    write_file(root, old_trades_table().replace_schema_metadata(None))
    with pytest.raises(StoreVersionError, match="no schema version"):
        PITSession(root, available_at=INT64_MAX)


def test_a_file_holding_another_datasets_rows_is_refused(tmp_path):
    root = current_store(tmp_path / "store")
    (good,) = sorted(root.rglob("*.parquet"))
    table = pq.read_table(good)
    metadata = {**table.schema.metadata, b"qa.dataset": b"book_events"}
    write_file(root, table.replace_schema_metadata(metadata), knowledge_ts=3_000)
    with pytest.raises(StoreVersionError, match="book_events"):
        PITSession(root, available_at=INT64_MAX)


def test_a_file_whose_columns_do_not_match_its_version_is_refused(tmp_path):
    root = tmp_path / "store"
    table = old_trades_table().replace_schema_metadata(
        {"qa.dataset": "trades", "qa.schema_version": str(TRADES.version)}
    )
    write_file(root, table)
    with pytest.raises(StoreVersionError, match="columns"):
        PITSession(root, available_at=INT64_MAX)


def test_a_file_the_session_does_not_open_is_not_checked(tmp_path):
    """Only files known at the session's version are opened, so only they are checked."""
    root = current_store(tmp_path / "store")
    write_file(root, old_trades_table(), knowledge_ts=9_000)
    with PITSession(root, available_at=INT64_MAX, version=8_999) as session:
        assert session.trades("kraken", "ETH-USD", 0, INT64_MAX).num_rows == 1
    with pytest.raises(StoreVersionError):
        PITSession(root, available_at=INT64_MAX, version=9_000)


def test_the_command_lines_print_the_refusal_and_exit_2(tmp_path, capsys):
    root = tmp_path / "store"
    write_file(root, old_trades_table())
    days = ["--start", "2023-11-14", "--end", "2023-11-14"]
    assert quality.main(["--store", str(root), *days]) == 2
    assert "rebuild" in capsys.readouterr().err
    verify = ["verify", "--store", str(root), "--venue", "kraken", "--instrument", "ETH-USD"]
    assert book_module.main(verify) == 2
    assert "rebuild" in capsys.readouterr().err


# --- stores written before commit markers (ADR 0017) -------------------------------------


def test_a_store_written_before_commit_markers_is_refused_with_a_clear_message(tmp_path):
    root = tmp_path / "store"
    write_legacy_file(root, old_trades_table())
    with pytest.raises(StoreVersionError) as raised:
        PITSession(root, available_at=INT64_MAX)
    message = str(raised.value)
    assert "commit marker" in message and "ADR 0017" in message
    assert "rebuild" in message and "raw logs" in message
    assert str(tmp_path) not in message


def test_one_file_from_before_commit_markers_among_current_ones_is_refused(tmp_path):
    root = current_store(tmp_path / "store")
    write_legacy_file(root, pq.read_table(next(iter(sorted(root.rglob("*.parquet"))))))
    with pytest.raises(StoreVersionError, match="rebuild"):
        PITSession(root, available_at=INT64_MAX)


def test_data_files_with_no_marker_directory_at_all_are_refused(tmp_path):
    """Files named the new way but with nothing to say they were committed and
    no sign a writer of this layout ever ran: not a store this code wrote."""
    root = current_store(tmp_path / "store")
    for marker in (root / "_batches").iterdir():
        marker.unlink()
    (root / "_batches").rmdir()
    with pytest.raises(StoreVersionError, match="rebuild"):
        PITSession(root, available_at=INT64_MAX)


@pytest.mark.parametrize(
    ("change", "problem"),
    [
        ({"layout": 1}, "layout"),
        ({"layout": 2}, "layout"),  # ADR 0017's, before loads (ADR 0018)
        ({"layout": 4}, "layout"),
        ({"knowledge_ts": 1_001}, "knowledge"),
        ({"files": ["trades/venue=kraken/instrument=ETH-USD/../../../../outside.parquet"]}, "file"),
        ({"files": "not a list"}, "file"),
    ],
)
def test_a_commit_marker_that_does_not_describe_its_batch_is_refused(tmp_path, change, problem):
    root = current_store(tmp_path / "store")
    (marker,) = (root / "_batches").glob("batch-*.json")
    marker.write_text(json.dumps({**json.loads(marker.read_text()), **change}))
    with pytest.raises(ValueError, match=problem) as raised:
        PITSession(root, available_at=INT64_MAX)
    assert str(tmp_path) not in str(raised.value)


def test_a_commit_marker_that_is_not_json_is_refused(tmp_path):
    root = current_store(tmp_path / "store")
    (marker,) = (root / "_batches").glob("batch-*.json")
    marker.write_text("{ not json")
    with pytest.raises(ValueError, match="marker"):
        PITSession(root, available_at=INT64_MAX)
