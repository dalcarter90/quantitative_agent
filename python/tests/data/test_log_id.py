"""log_id: which raw log a stored row came from (ADR 0013).

A log's id comes from its first record, so it is the same however the log is
read: sealed or not, complete or cut short, read once or normalized again.
"""

import re

import pytest
from _builders import LOG, T0, FakeClock, book_table, instrument_table, trade_table
from test_fixtures import fixture_path

from quantagent.data.adapters import ingest, normalize
from quantagent.data.rawlog import RawLogWriter, RawRecord, log_id_of, read_raw_log
from quantagent.data.schema import (
    BOOK_EVENTS,
    INSTRUMENTS,
    STREAM_EVENTS,
    TRADES,
    Side,
    StreamEventKind,
    table_from_rows,
)
from quantagent.data.session import PITSession
from quantagent.data.store import AppendOnlyStore

FORMAT = re.compile(r"[0-9]{8}T[0-9]{6}\.[0-9]{9}Z-[0-9a-f]{12}")
INT64_MAX = 2**63 - 1


def first(**overrides):
    values = {"recv_ts": 1_794_366_407_970_222_600, "seq": 0, "venue": "kraken",
              "channel": "recorder", "payload": b'{"event":"start"}',
              "recv_mono_ns": None}  # fmt: skip
    values.update(overrides)
    return RawRecord(**values)


def test_the_id_is_the_start_time_and_a_digest_of_the_first_record():
    log_id = log_id_of(first())
    assert FORMAT.fullmatch(log_id)
    assert log_id.startswith("20261111T030647.970222600Z-")
    assert log_id == log_id_of(first())  # deterministic


@pytest.mark.parametrize(
    "change",
    [{"recv_ts": 1_794_366_407_970_222_601}, {"recv_mono_ns": 5}, {"venue": "coinbase"},
     {"channel": "ws"}, {"payload": b'{"event":"start","x":1}'}],
)  # fmt: skip
def test_every_field_of_the_first_record_contributes(change):
    assert log_id_of(first(**change)) != log_id_of(first())


def test_ids_sort_by_start_time():
    earlier = log_id_of(first(recv_ts=1_794_366_407_000_000_000, payload=b"zzz"))
    later = log_id_of(first(recv_ts=1_794_366_407_000_000_001, payload=b"aaa"))
    assert earlier < later


def test_only_a_first_record_has_an_id():
    with pytest.raises(ValueError, match="seq 0"):
        log_id_of(first(seq=1))


def test_the_test_builders_id_is_well_formed():
    assert FORMAT.fullmatch(LOG)


def test_a_log_has_the_same_id_sealed_unsealed_and_cut_short(tmp_path):
    path = tmp_path / "a.qaraw"
    with RawLogWriter(path) as writer:
        for i in range(5):
            writer.write("kraken", "ws", f"record {i}".encode())
    unsealed = log_id_of(next(iter(read_raw_log(path))))
    cut = tmp_path / "cut.qaraw"
    cut.write_bytes(path.read_bytes()[:-9])
    truncated = log_id_of(next(iter(read_raw_log(cut))))
    writer = RawLogWriter(tmp_path / "b.qaraw")
    copy = list(read_raw_log(path))
    for record in copy:
        writer.write(record.venue, record.channel, record.payload, recv_ts=record.recv_ts,
                     recv_mono_ns=record.recv_mono_ns)  # fmt: skip
    sealed = log_id_of(next(iter(read_raw_log(writer.seal()))))
    assert unsealed == truncated == sealed


@pytest.mark.parametrize("venue", ["kraken", "coinbase"])
def test_every_row_of_a_normalized_log_carries_its_id(venue):
    records = list(read_raw_log(fixture_path(venue)))
    expected = log_id_of(records[0])
    normalized = normalize(records)
    for rows in normalized.rows.values():
        assert {row["log_id"] for row in rows} == {expected}


def test_each_log_fed_to_one_normalizer_gets_its_own_id():
    from test_adapters import KRAKEN_INSTRUMENT, KRAKEN_START, KRAKEN_TRADES, log

    one = log("kraken", KRAKEN_START, KRAKEN_INSTRUMENT, KRAKEN_TRADES)
    two = [RawRecord(r.recv_ts + 10**12, r.seq, r.venue, r.channel, r.payload) for r in one]
    normalized = normalize(one + two)
    first, second = log_id_of(one[0]), log_id_of(two[0])
    assert first != second
    instruments = normalized.rows[("instruments", "kraken", "BTC-USD")]
    assert [row["log_id"] for row in instruments] == [first, second]
    events = normalized.rows[("stream_events", "kraken", "BTC-USD")]
    starts = [row["log_id"] for row in events if row["kind"] == StreamEventKind.SESSION_START]
    assert starts == [first, second]
    # The second log resends the same trades unchanged: the rows kept are the
    # first log's, and the repeats are events in the second (ADR 0016).
    trades = normalized.rows[("trades", "kraken", "BTC-USD")]
    assert [row["log_id"] for row in trades] == [first] * 2
    repeats = [row["log_id"] for row in events if row["kind"] == StreamEventKind.TRADE_REPEAT]
    assert repeats == [second] * 2


def test_records_before_any_log_start_are_rejected():
    from test_adapters import KRAKEN_START, KRAKEN_TRADES, log

    records = log("kraken", KRAKEN_START, KRAKEN_TRADES)
    normalized = normalize(records[1:])  # the log's first record is missing
    assert [r.reason for r in normalized.report.rejects] == ["no_log_start"]
    assert normalized.rows == {}


def stream_event_table(log_id):
    row = {"event_ts": T0, "event_ts_source": "receipt", "recv_ts": T0, "log_id": log_id,
           "seq": 0, "event_index": 0, "kind": StreamEventKind.SESSION_START,
           "connection": None, "expected": None, "observed": None, "detail": log_id}  # fmt: skip
    return table_from_rows(STREAM_EVENTS, [row])


TABLES = {
    "book_events": (BOOK_EVENTS, lambda log: book_table((1, T0, Side.BUY, 99, 5), log_id=log)),
    "instruments": (INSTRUMENTS, lambda log: instrument_table(T0, log_id=log)),
    "stream_events": (STREAM_EVENTS, stream_event_table),
}


@pytest.mark.parametrize("name", sorted(TABLES))
def test_two_logs_with_the_same_receive_times_and_numbers_do_not_collide(tmp_path, name):
    """The collision ADR 0012 left open: two recorders of one instrument whose
    records have the same recv_ts and seq. Under the old keys, which used
    recv_ts and seq, the second row replaced the first as a "correction"."""
    dataset, build = TABLES[name]
    store = AppendOnlyStore(tmp_path / "store", clock=FakeClock(1_000, 2_000))
    a = "20231114T120000.000000000Z-aaaaaaaaaaaa"
    b = "20231114T120000.000000000Z-bbbbbbbbbbbb"
    first, second = build(a), build(b)
    assert first.column("recv_ts").equals(second.column("recv_ts"))
    store.append(dataset, "kraken", "ETH-USD", first)
    store.append(dataset, "kraken", "ETH-USD", second)
    recv_ts = first.column("recv_ts")[0].as_py()
    with PITSession(tmp_path / "store", as_of=INT64_MAX) as session:
        rows = session.sql(f"SELECT log_id, seq, recv_ts FROM {name} ORDER BY log_id").to_pylist()
    assert rows == [{"log_id": a, "seq": 0, "recv_ts": recv_ts},
                    {"log_id": b, "seq": 0, "recv_ts": recv_ts}]  # fmt: skip


def test_two_logs_with_the_same_trade_keep_one_row(tmp_path):
    """Trades keep the venue's identity (ADR 0013, "not changed"): the same
    trade_id from another log is a later observation of the same trade."""
    store = AppendOnlyStore(tmp_path / "store", clock=FakeClock(1_000, 2_000))
    a = "20231114T120000.000000000Z-aaaaaaaaaaaa"
    b = "20231114T130000.000000000Z-bbbbbbbbbbbb"
    store.append(TRADES, "kraken", "ETH-USD", trade_table(("t1", T0, 100), log_id=a))
    store.append(TRADES, "kraken", "ETH-USD", trade_table(("t1", T0, 100), log_id=b, seq=7))
    with PITSession(tmp_path / "store", as_of=INT64_MAX) as session:
        rows = session.trades("kraken", "ETH-USD", 0, INT64_MAX).to_pylist()
    assert [(r["log_id"], r["seq"], r["trade_id"]) for r in rows] == [(b, 7, "t1")]


def test_normalizing_a_log_again_keeps_its_id_so_rows_are_corrections(tmp_path):
    path = fixture_path("kraken")
    store = AppendOnlyStore(tmp_path / "store")
    ingest(path, store)
    ingest(path, store)
    with PITSession(tmp_path / "store", as_of=INT64_MAX) as session:
        (row,) = session.sql(
            "SELECT count(*) AS n, count(DISTINCT log_id) AS logs FROM book_events"
        ).to_pylist()
    assert row == {"n": 18605, "logs": 1}
