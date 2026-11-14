"""stream_events: integrity events for every venue (ADR 0010).

The adapters write one row per event, each tied to the raw record it refers
to. Quality checks and the order book reconstructor read them through
PITSession.
"""

import json
import zlib
from collections import Counter

import pyarrow as pa
import pytest
from _builders import INT64_MAX
from test_adapters import (
    COINBASE_CONNECT,
    COINBASE_SNAPSHOT,
    COINBASE_TRADES,
    COINBASE_UPDATE,
    KRAKEN_BOOK_ACK,
    KRAKEN_INSTRUMENT,
    KRAKEN_SNAPSHOT,
    KRAKEN_START,
    KRAKEN_TRADES,
    coinbase_session,
    kraken_session,
    log,
)
from test_fixtures import INSTRUMENT, VENUES, fixture_path

from quantagent.data.adapters import ingest, normalize
from quantagent.data.rawlog import RawLogWriter, log_id_of, read_raw_log
from quantagent.data.schema import (
    DATASETS,
    STREAM_EVENTS,
    SchemaError,
    StreamEventKind,
    table_from_rows,
)
from quantagent.data.session import PITSession
from quantagent.data.store import AppendOnlyStore

K = StreamEventKind


def events_of(normalized, venue, instrument="BTC-USD"):
    return normalized.rows.get(("stream_events", venue, instrument), [])


def brief(rows):
    return [
        (r["seq"], r["event_index"], K(r["kind"]), r["connection"], r["expected"], r["observed"])
        for r in rows
    ]


def event_row(**overrides):
    row = {
        "event_ts": 77,
        "event_ts_source": "receipt",
        "recv_ts": 77,
        "log_id": "20231114T120000.000000000Z-000000000000",
        "seq": 3,
        "event_index": 0,
        "kind": K.SEQUENCE_GAP,
        "connection": "ws",
        "expected": 3,
        "observed": 5,
        "detail": None,
    }
    row.update(overrides)
    return row


# --- schema ----------------------------------------------------------------------


def test_stream_events_is_a_registered_bitemporal_dataset():
    assert DATASETS["stream_events"] is STREAM_EVENTS
    assert STREAM_EVENTS.key == ("log_id", "seq", "event_index")
    assert STREAM_EVENTS.stored_schema.names == [
        "event_ts",
        "event_ts_source",
        "recv_ts",
        "knowledge_ts",
        "log_id",
        "seq",
        "event_index",
        "kind",
        "connection",
        "expected",
        "observed",
        "detail",
    ]
    schema = STREAM_EVENTS.schema
    assert schema.field("kind").type == pa.int8()
    assert schema.field("event_index").type == pa.int32()
    assert schema.field("expected").type == pa.int64()
    assert schema.field("observed").type == pa.int64()
    for name in ("connection", "expected", "observed", "detail"):
        assert schema.field(name).nullable, name
    for name in ("event_ts", "recv_ts", "log_id", "seq", "event_index", "kind"):
        assert not schema.field(name).nullable, name


def test_stream_event_kinds_cover_every_class_of_integrity_event():
    assert {k.name for k in K} == {
        "SESSION_START",
        "SESSION_STOP",
        "CONNECT",
        "DISCONNECT",
        "SUBSCRIBE_REQUEST",
        "SUBSCRIPTION_ACK",
        "TRADE_BACKFILL",
        "CLOCK_OFFSET",
        "CLOCK_OVERRIDE",
        "CLOCK_STEP",
        "SEQUENCE_GAP",
        "SEQUENCE_REPEAT",
        "SEQUENCE_OUT_OF_ORDER",
        "CHECKSUM_MISMATCH",
        "MESSAGE_REJECTED",
        "CHECK_SNAPSHOT_COMPARED",
        "TRADE_CONFLICT",
    }
    assert len({int(k) for k in K}) == len(K)


def test_a_stream_event_row_builds_and_validates():
    table = table_from_rows(STREAM_EVENTS, [event_row(), event_row(event_index=1, kind=K.CONNECT)])
    assert table.num_rows == 2


@pytest.mark.parametrize(
    "overrides",
    [
        {"kind": 99},
        {"kind": -1},
        {"event_index": -1},
        {"event_index": None},
        {"kind": None},
        {"expected": 1.5},
        {"detail": 5},
        {"connection": 7},
    ],
)
def test_stream_event_columns_must_be_structurally_valid(overrides):
    (column,) = overrides
    with pytest.raises(SchemaError, match=column):
        table_from_rows(STREAM_EVENTS, [event_row(**overrides)])


def test_a_stream_event_is_our_own_observation_so_its_time_is_the_receipt_time():
    # ADR 0008: the source is decided by the kind of row. An integrity event is
    # something our process noticed, at the moment it received a record.
    with pytest.raises(SchemaError, match="event_ts_source"):
        table_from_rows(STREAM_EVENTS, [event_row(event_ts_source="venue")])
    with pytest.raises(SchemaError, match="receipt"):
        table_from_rows(STREAM_EVENTS, [event_row(event_ts=76)])


# --- what the adapters write: session, connections, subscriptions --------------------


def test_kraken_session_events_in_order():
    probe_ack = ("probe", KRAKEN_BOOK_ACK[1])
    normalized = kraken_session(
        ("recorder", {"event": "connect", "connection": "ws", "url": "wss://ws.kraken.com/v2"}),
        (
            "recorder",
            {"event": "subscribe", "connection": "ws", "request": {"method": "subscribe"}},
        ),
        KRAKEN_SNAPSHOT,
        ("recorder", {"event": "connect", "connection": "probe", "url": "wss://ws.kraken.com/v2"}),
        probe_ack,
        ("recorder", {"event": "disconnect", "connection": "probe", "reason": "snapshot received"}),
        ("recorder", {"event": "disconnect", "connection": "ws", "reason": "session ended"}),
        ("recorder", {"event": "stop"}),
    )
    rows = events_of(normalized, "kraken")
    assert brief(rows) == [
        (0, 0, K.SESSION_START, None, None, None),
        (2, 0, K.SUBSCRIPTION_ACK, "ws", None, None),
        (3, 0, K.CONNECT, "ws", None, None),
        (4, 0, K.SUBSCRIBE_REQUEST, "ws", None, None),
        (6, 0, K.CONNECT, "probe", None, None),
        (7, 0, K.SUBSCRIPTION_ACK, "probe", None, None),
        (8, 0, K.DISCONNECT, "probe", None, None),
        (9, 0, K.DISCONNECT, "ws", None, None),
        (10, 0, K.SESSION_STOP, None, None, None),
    ]
    details = {(r["seq"]): r["detail"] for r in rows}
    assert json.loads(details[0]) == {
        "symbol": "BTC/USD",
        "depth": 10,
        "recorder_version": 1,
        "clock_identity": None,  # recorded before ADR 0012
        "monotonic_stamps": False,
    }
    assert json.loads(details[2]) == {
        "method": "subscribe",
        "channel": "book",
        "depth": 10,
        "snapshot": True,
        "symbol": "BTC/USD",
    }
    assert details[3] == "wss://ws.kraken.com/v2"
    assert json.loads(details[4]) == {"method": "subscribe"}
    assert details[8] == "snapshot received"
    assert details[9] == "session ended"
    assert details[10] is None


def test_every_stream_event_refers_to_its_raw_record_and_takes_the_receipt_time():
    records = log("kraken", KRAKEN_START, KRAKEN_INSTRUMENT, KRAKEN_BOOK_ACK, KRAKEN_TRADES)
    rows = events_of(normalize(records), "kraken")
    assert rows
    by_seq = {r.seq: r for r in records}
    for row in rows:
        assert row["log_id"] == log_id_of(records[0])
        assert row["recv_ts"] == by_seq[row["seq"]].recv_ts
        assert (row["event_ts"], row["event_ts_source"]) == (row["recv_ts"], "receipt")
    table_from_rows(STREAM_EVENTS, rows)


def test_coinbase_subscription_messages_are_acks():
    rows = events_of(coinbase_session(), "coinbase")
    assert [(r["seq"], K(r["kind"]), r["connection"]) for r in rows] == [
        (0, K.SESSION_START, None),
        (2, K.CONNECT, "ws"),
        (3, K.SUBSCRIPTION_ACK, "ws"),
    ]
    assert json.loads(rows[2]["detail"]) == {"heartbeats": ["heartbeats"]}


def test_stream_events_do_not_change_the_outcome_of_a_record():
    # Outcomes describe market data. A control record is still "ignored".
    report = coinbase_session().report
    assert report.outcomes == {"data": 1, "ignored": 4}
    assert report.rows == {"instruments": 1, "stream_events": 3}
    assert report.events == {"SESSION_START": 1, "CONNECT": 1, "SUBSCRIPTION_ACK": 1}


# --- trade history delivered on subscribe ----------------------------------------------


def test_a_trade_snapshot_is_marked_as_backfill_and_an_update_is_not():
    kraken_snapshot = ("ws", KRAKEN_TRADES[1].replace('"type":"update"', '"type":"snapshot"'))
    normalized = kraken_session(kraken_snapshot, KRAKEN_TRADES)
    backfill = [r for r in events_of(normalized, "kraken") if r["kind"] == K.TRADE_BACKFILL]
    assert brief(backfill) == [(3, 0, K.TRADE_BACKFILL, "ws", None, None)]
    assert len(normalized.rows[("trades", "kraken", "BTC-USD")]) == 4  # the trades are still stored

    coinbase_snapshot = ("ws", COINBASE_TRADES[1].replace('"type":"update"', '"type":"snapshot"'))
    normalized = coinbase_session(renumbered(coinbase_snapshot, 2))
    backfill = [r for r in events_of(normalized, "coinbase") if r["kind"] == K.TRADE_BACKFILL]
    assert brief(backfill) == [(5, 0, K.TRADE_BACKFILL, "ws", None, None)]
    assert not [
        r
        for r in events_of(coinbase_session(renumbered(COINBASE_TRADES, 2)), "coinbase")
        if r["kind"] == K.TRADE_BACKFILL
    ]


# --- sequence numbers (Coinbase) ---------------------------------------------------------


def renumbered(entry, number):
    channel, text = entry
    original = json.loads(text)["sequence_num"]
    return channel, text.replace(f'"sequence_num":{original}', f'"sequence_num":{number}')


def test_coinbase_sequence_breaks_become_events_with_expected_and_observed():
    normalized = coinbase_session(
        COINBASE_SNAPSHOT,
        renumbered(COINBASE_UPDATE, 4),
        renumbered(COINBASE_TRADES, 4),
        renumbered(COINBASE_UPDATE, 3),
    )
    breaks = [r for r in events_of(normalized, "coinbase") if K(r["kind"]).name.startswith("SEQ")]
    assert brief(breaks) == [
        (6, 0, K.SEQUENCE_GAP, "ws", 3, 4),
        (7, 0, K.SEQUENCE_REPEAT, "ws", 5, 4),
        (8, 0, K.SEQUENCE_OUT_OF_ORDER, "ws", 5, 3),
    ]
    assert normalized.report.events["SEQUENCE_GAP"] == 1


def test_a_new_connection_restarts_numbering_without_an_event():
    normalized = coinbase_session(
        COINBASE_SNAPSHOT, COINBASE_UPDATE, COINBASE_CONNECT, renumbered(COINBASE_SNAPSHOT, 0)
    )
    kinds = Counter(K(r["kind"]) for r in events_of(normalized, "coinbase"))
    assert kinds[K.CONNECT] == 2  # the reconnect itself is recorded
    assert not any(k.name.startswith("SEQUENCE") for k in kinds)


# --- checksums (Kraken) ------------------------------------------------------------------


def kraken_book(kind, bids, asks, checksum, channel="ws"):
    def levels(side):
        return ",".join(f'{{"price":{p},"qty":{q}}}' for p, q in side)

    return (
        channel,
        f'{{"channel":"book","type":"{kind}","data":[{{"symbol":"BTC/USD",'
        f'"bids":[{levels(bids)}],"asks":[{levels(asks)}],'
        f'"checksum":{checksum},"timestamp":"2026-11-11T03:06:47.100000Z"}}]}}',
    )


def crc(asks, bids):
    """Kraken's rule, written out by hand: asks ascending then bids descending,
    price digits then quantity digits, no decimal point or leading zeros."""
    return zlib.crc32("".join(asks + bids).encode())


SNAP_BIDS = [("100.0", "1.00000000"), ("99.9", "2.00000000")]
SNAP_ASKS = [("100.1", "0.50000000"), ("100.2", "0.00000300")]
SNAP_CRC = crc(["100150000000", "1002300"], ["1000100000000", "999200000000"])
# After: best bid 100.0 changes to 3.5, and ask 100.2 is removed.
UPDATE_BIDS = [("100.0", "3.50000000")]
UPDATE_ASKS = [("100.2", "0.00000000")]
UPDATE_CRC = crc(["100150000000"], ["1000350000000", "999200000000"])


def mismatches(normalized):
    rows = events_of(normalized, "kraken")
    return brief([r for r in rows if r["kind"] == K.CHECKSUM_MISMATCH])


def test_kraken_checksums_that_match_produce_no_event():
    normalized = kraken_session(
        kraken_book("snapshot", SNAP_BIDS, SNAP_ASKS, SNAP_CRC),
        kraken_book("update", UPDATE_BIDS, UPDATE_ASKS, UPDATE_CRC),
    )
    assert mismatches(normalized) == []
    assert normalized.report.events["CHECKSUM_MISMATCH"] == 0
    assert len(normalized.rows[("book_events", "kraken", "BTC-USD")]) == 6


def test_a_kraken_checksum_mismatch_is_one_event_until_the_next_snapshot():
    wrong = UPDATE_CRC ^ 1
    normalized = kraken_session(
        kraken_book("snapshot", SNAP_BIDS, SNAP_ASKS, SNAP_CRC),  # seq 3
        kraken_book("update", UPDATE_BIDS, UPDATE_ASKS, wrong),  # seq 4: mismatch
        kraken_book("update", [("99.9", "2.50000000")], [], 12345),  # seq 5: book already suspect
        kraken_book("snapshot", SNAP_BIDS, SNAP_ASKS, SNAP_CRC),  # seq 6: trusted again
        kraken_book("update", UPDATE_BIDS, UPDATE_ASKS, UPDATE_CRC),  # seq 7: fine
        kraken_book("update", [("99.9", "2.50000000")], [], 12345),  # seq 8: mismatch
    )
    after_seq_8 = crc(["100150000000"], ["1000350000000", "999250000000"])
    assert mismatches(normalized) == [
        (4, 0, K.CHECKSUM_MISMATCH, "ws", wrong, UPDATE_CRC),
        (8, 0, K.CHECKSUM_MISMATCH, "ws", 12345, after_seq_8),
    ]
    # Flagging is not dropping: every level is still stored.
    assert len(normalized.rows[("book_events", "kraken", "BTC-USD")]) == 4 + 2 + 1 + 4 + 2 + 1
    assert normalized.report.rejects == []


def test_a_bad_snapshot_checksum_is_an_event_and_probes_are_checked_independently():
    wrong = UPDATE_CRC ^ 1
    normalized = kraken_session(
        kraken_book("snapshot", SNAP_BIDS, SNAP_ASKS, SNAP_CRC),
        kraken_book("snapshot", SNAP_BIDS, SNAP_ASKS, 1, channel="probe"),
        # A bad probe says nothing about the stream: its book is still trusted,
        # so a fault on the stream right afterwards is still found.
        kraken_book("update", UPDATE_BIDS, UPDATE_ASKS, wrong),
        kraken_book("snapshot", SNAP_BIDS, SNAP_ASKS, 2),
    )
    assert mismatches(normalized) == [
        (4, 0, K.CHECKSUM_MISMATCH, "probe", 1, SNAP_CRC),
        (5, 0, K.CHECKSUM_MISMATCH, "ws", wrong, UPDATE_CRC),
        (6, 0, K.CHECKSUM_MISMATCH, "ws", 2, SNAP_CRC),
    ]


def test_kraken_checksum_respects_the_subscribed_depth_and_the_top_ten():
    # Depth 10 (from the subscription ack). Twelve bids arrive; the two worst
    # are outside the book and must not be kept, or a later removal inside the
    # top ten would bring them into the checksum.
    bids = [(f"{100 - i}.0", "1.00000000") for i in range(12)]
    asks = [("101.0", "1.00000000")]
    top_ten = [f"{100 - i}0100000000" for i in range(10)]
    snapshot_crc = crc(["1010100000000"], top_ten)
    # Remove the best bid. With the book cut to ten, nine bids remain.
    after = crc(["1010100000000"], top_ten[1:])
    normalized = kraken_session(
        kraken_book("snapshot", bids, asks, snapshot_crc),
        kraken_book("update", [("100.0", "0.00000000")], [], after),
    )
    assert mismatches(normalized) == []


def test_an_update_with_no_snapshot_before_it_cannot_be_checked_and_is_not_flagged():
    normalized = kraken_session(kraken_book("update", UPDATE_BIDS, UPDATE_ASKS, 999))
    assert mismatches(normalized) == []


# --- rejected messages -------------------------------------------------------------------


def test_a_rejected_message_in_a_session_is_an_integrity_event():
    normalized = kraken_session(KRAKEN_SNAPSHOT, ("ws", b'{"channel":"book","type":"update"'))
    rejected = [r for r in events_of(normalized, "kraken") if r["kind"] == K.MESSAGE_REJECTED]
    assert brief(rejected) == [(4, 0, K.MESSAGE_REJECTED, "ws", None, None)]
    assert rejected[0]["detail"].startswith("invalid_json")
    assert len(rejected[0]["detail"]) <= 200
    assert normalized.report.rejected == {"invalid_json": 1}


def test_one_record_can_carry_two_events():
    # Sequence number 9 after 2 is a gap, and the message itself is malformed.
    bad = (
        "ws",
        '{"channel":"l2_data","timestamp":"2026-11-11T03:06:47Z","sequence_num":9,"events":['
        '{"type":"update","product_id":"BTC-USD","updates":[{"side":"north",'
        '"event_time":"2026-11-11T03:06:47Z","price_level":"1.00","new_quantity":"1"}]}]}',
    )
    normalized = coinbase_session(COINBASE_SNAPSHOT, bad)
    rows = [r for r in events_of(normalized, "coinbase") if r["seq"] == 6]
    assert brief(rows) == [
        (6, 0, K.SEQUENCE_GAP, "ws", 3, 9),
        (6, 1, K.MESSAGE_REJECTED, "ws", None, None),
    ]


def test_a_reject_before_any_session_has_nowhere_to_be_stored():
    normalized = normalize(log("kraken", KRAKEN_SNAPSHOT))
    assert normalized.rows == {}
    assert [r.reason for r in normalized.report.rejects] == ["no_session_start"]


# --- recorded fixtures --------------------------------------------------------------------


@pytest.fixture(scope="module", params=VENUES)
def capture(request):
    venue = request.param
    records = list(read_raw_log(fixture_path(venue)))
    return venue, records, normalize(records)


def test_clean_fixture_has_session_events_and_no_integrity_failures(capture):
    venue, records, normalized = capture
    kinds = Counter(K(r["kind"]).name for r in events_of(normalized, venue))
    controls = Counter(json.loads(r.payload)["event"] for r in records if r.channel == "recorder")
    assert kinds["SESSION_START"] == controls["start"] == 1
    assert kinds["SESSION_STOP"] == controls["stop"] == 1
    assert kinds["CONNECT"] == controls["connect"] == 2
    assert kinds["DISCONNECT"] == controls["disconnect"] == 2
    assert kinds["SUBSCRIBE_REQUEST"] == controls["subscribe"] == 4
    assert kinds["SUBSCRIPTION_ACK"] == (4 if venue == "kraken" else 3)
    assert kinds["TRADE_BACKFILL"] == 1
    for failure in (
        "SEQUENCE_GAP",
        "SEQUENCE_REPEAT",
        "SEQUENCE_OUT_OF_ORDER",
        "CHECKSUM_MISMATCH",
        "MESSAGE_REJECTED",
        "TRADE_CONFLICT",
    ):
        assert kinds[failure] == 0, failure
    assert normalized.report.events == kinds
    assert normalized.report.rows["stream_events"] == sum(kinds.values())


def test_fixture_events_point_at_real_raw_records(capture):
    venue, records, normalized = capture
    by_seq = {r.seq: r for r in records}
    rows = events_of(normalized, venue)
    keys = [(r["log_id"], r["seq"], r["event_index"]) for r in rows]
    assert len(set(keys)) == len(keys)
    for row in rows:
        record = by_seq[row["seq"]]
        assert record.recv_ts == row["recv_ts"]
        kind = K(row["kind"])
        if kind in (K.SUBSCRIPTION_ACK, K.TRADE_BACKFILL):
            assert record.channel == row["connection"]
        elif kind == K.CHECK_SNAPSHOT_COMPARED:
            # Decided at the check snapshot's own record: the next batch had come.
            assert record.channel == row["connection"] == "probe"
            assert json.loads(row["detail"])["snapshot_seq"] == record.seq
        else:
            assert record.channel == "recorder"
    table_from_rows(STREAM_EVENTS, rows)


def copy_log(records, path, *, drop=(), replace=None):
    replace = replace or {}
    with RawLogWriter(path) as writer:
        for record in records:
            if record.seq in drop:
                continue
            payload = replace.get(record.seq, record.payload)
            writer.write(record.venue, record.channel, payload, recv_ts=record.recv_ts,
                         recv_mono_ns=record.recv_mono_ns)  # fmt: skip
    return path


def test_a_lost_coinbase_message_is_found_as_one_gap_at_the_next_message(tmp_path):
    records = list(read_raw_log(fixture_path("coinbase")))
    stream = [r for r in records if r.channel == "ws"]
    lost, following = stream[500], stream[501]
    lost_number = json.loads(lost.payload)["sequence_num"]
    path = copy_log(records, tmp_path / "gap.qaraw", drop={lost.seq})

    normalized = normalize(read_raw_log(path))
    gaps = [r for r in events_of(normalized, "coinbase") if r["kind"] == K.SEQUENCE_GAP]
    assert len(gaps) == 1
    assert gaps[0]["recv_ts"] == following.recv_ts
    assert (gaps[0]["expected"], gaps[0]["observed"]) == (lost_number, lost_number + 1)
    assert gaps[0]["connection"] == "ws"


def test_a_lost_kraken_book_update_is_found_by_the_checksum(tmp_path):
    records = list(read_raw_log(fixture_path("kraken")))
    updates = [
        r
        for r in records
        if r.channel == "ws" and b'"channel":"book"' in r.payload and b'"update"' in r.payload
    ]
    # The checksum covers the ten best levels per side of a 25-level book, and
    # it is checked after the next message is applied. So a lost update is
    # caught only if its level is still wrong, and in the top ten, by then. One
    # that is overwritten first is never seen. This test shows both outcomes
    # on real data, for twelve updates spread across the recording.
    caught, caught_at_once, missed = 0, 0, 0
    for lost in updates[200::1100][:12]:
        path = copy_log(records, tmp_path / f"lost-{lost.seq}.qaraw", drop={lost.seq})
        normalized = normalize(read_raw_log(path))
        found = [r for r in events_of(normalized, "kraken") if r["kind"] == K.CHECKSUM_MISMATCH]
        assert len(found) <= 1, lost.seq  # one event, then the book is suspect
        if not found:
            missed += 1
            continue
        caught += 1
        assert found[0]["recv_ts"] >= lost.recv_ts  # two records can share a receive time
        assert found[0]["expected"] != found[0]["observed"]
        following = next(r for r in updates if r.seq > lost.seq)
        caught_at_once += found[0]["recv_ts"] == following.recv_ts
    assert caught + missed == 12
    assert (caught, caught_at_once, missed) == EXPECTED_LOSS_OUTCOMES


# Measured on the committed fixture: of the twelve lost updates, how many the
# checksum catches, how many of those at the very next message, and how many
# it never sees. (Over 250 samples the split is 64% caught, 36% never.)
EXPECTED_LOSS_OUTCOMES = (9, 9, 3)


def test_a_corrupted_kraken_checksum_is_reported_at_that_exact_record(tmp_path):
    records = list(read_raw_log(fixture_path("kraken")))
    target = [r for r in records if r.channel == "ws" and b'"type":"update"' in r.payload
              and b'"channel":"book"' in r.payload][300]  # fmt: skip
    message = json.loads(target.payload)
    real = message["data"][0]["checksum"]
    altered = target.payload.replace(
        f'"checksum":{real}'.encode(), f'"checksum":{real ^ 7}'.encode()
    )
    assert altered != target.payload
    path = copy_log(records, tmp_path / "bad.qaraw", replace={target.seq: altered})

    normalized = normalize(read_raw_log(path))
    found = [r for r in events_of(normalized, "kraken") if r["kind"] == K.CHECKSUM_MISMATCH]
    assert [(r["recv_ts"], r["expected"], r["observed"]) for r in found] == [
        (target.recv_ts, real ^ 7, real)
    ]


# --- stored and read like every other dataset -----------------------------------------------


@pytest.mark.parametrize("venue", VENUES)
def test_stream_events_go_through_the_store_and_come_back_through_pitsession(venue, tmp_path):
    expected = events_of(normalize(read_raw_log(fixture_path(venue))), venue)
    store = AppendOnlyStore(tmp_path / "store")
    report = ingest(fixture_path(venue), store, batch_rows=20_000)
    assert report.rows["stream_events"] == len(expected)
    assert (tmp_path / "store" / "stream_events" / f"venue={venue}").is_dir()

    with PITSession(tmp_path / "store", as_of=INT64_MAX) as session:
        table = session.stream_events(venue, INSTRUMENT, 0, INT64_MAX)
        assert table.schema.equals(STREAM_EVENTS.stored_schema)
        got = [{k: v for k, v in r.items() if k != "knowledge_ts"} for r in table.to_pylist()]
        assert got == [dict(r, kind=int(r["kind"])) for r in expected]
        # Time range is on event_ts, which is the receipt time here.
        first = expected[0]["recv_ts"]
        assert session.stream_events(venue, INSTRUMENT, first, first + 1).num_rows == 1
        counted = session.sql(
            "SELECT kind, count(*) AS n FROM stream_events GROUP BY kind ORDER BY kind"
        ).to_pylist()
        assert {K(r["kind"]).name: r["n"] for r in counted} == report.events

    knowledge_times = {int(p.name[5:25]) for p in (tmp_path / "store").rglob("*.parquet")}
    with PITSession(tmp_path / "store", as_of=min(knowledge_times) - 1) as before:
        assert before.stream_events(venue, INSTRUMENT, 0, INT64_MAX).num_rows == 0


# --- the recording clock (ADR 0011) -------------------------------------------------------


def clock_record(offset_ns, *, override=False, limit_ns=250_000_000):
    servers = [{"host": "time.example", "offset_ns": offset_ns, "round_trip_ns": 20_000_000}]
    if offset_ns is None:
        servers = [{"host": "time.example", "error": "OSError: timed out"}]
    return (
        "recorder",
        {
            "event": "clock",
            "offset_ns": offset_ns,
            "limit_ns": limit_ns,
            "within_limit": offset_ns is not None and abs(offset_ns) <= limit_ns,
            "override": override,
            "servers": servers,
        },
    )


def clock_events(normalized):
    rows = events_of(normalized, "kraken")
    wanted = (K.CLOCK_OFFSET, K.CLOCK_OVERRIDE)
    return [(K(r["kind"]), r["expected"], r["observed"], r["detail"]) for r in rows
            if r["kind"] in wanted]  # fmt: skip


def test_a_clock_record_becomes_a_clock_offset_event():
    normalized = kraken_session(clock_record(3_000_000))
    ((kind, expected, observed, detail),) = clock_events(normalized)
    assert (kind, expected, observed) == (K.CLOCK_OFFSET, 250_000_000, 3_000_000)
    assert json.loads(detail) == {
        "servers": [{"host": "time.example", "offset_ns": 3_000_000, "round_trip_ns": 20_000_000}]
    }


@pytest.mark.parametrize(
    ("offset", "reason"),
    [(3_000_000, "offset within limit"), (-1_400_000_000, "offset above limit"),
     (None, "offset not measured")],
)  # fmt: skip
def test_an_override_is_its_own_event(offset, reason):
    normalized = kraken_session(clock_record(offset, override=True))
    assert clock_events(normalized)[1:] == [(K.CLOCK_OVERRIDE, None, None, reason)]
    assert clock_events(normalized)[0][2] == offset


@pytest.mark.parametrize(
    "change",
    [
        {"offset_ns": 1.5},
        {"offset_ns": "3"},
        {"limit_ns": 0},
        {"limit_ns": None},
        {"override": "yes"},
        {"servers": "none"},
    ],
)
def test_a_malformed_clock_record_is_rejected(change):
    channel, message = clock_record(3_000_000)
    normalized = kraken_session((channel, {**message, **change}))
    assert [r.reason for r in normalized.report.rejects] == ["malformed_message"]
    assert clock_events(normalized) == []


# --- wall clock steps against the monotonic counter (ADR 0012) ------------------------------

from quantagent.data.adapters.base import CLOCK_STEP_FLOOR_NS  # noqa: E402
from quantagent.data.rawlog import RawRecord  # noqa: E402

MONO_OFFSET = 1_700_000_000_000_000_000  # wall time minus counter, while the clock is steady


def stamped(records, steps=None, jitter=None):
    """Give records monotonic stamps. ``steps`` maps a record seq to a wall-clock
    jump applied from that record on; ``jitter`` adds a small per-record wobble."""
    out, shift = [], 0
    for record in records:
        shift += (steps or {}).get(record.seq, 0)
        wobble = (jitter or {}).get(record.seq, 0)
        mono = record.recv_ts - MONO_OFFSET
        out.append(RawRecord(record.recv_ts + shift + wobble, record.seq, record.venue,
                             record.channel, record.payload, mono))  # fmt: skip
    return out


def kraken_records_with(steps=None, jitter=None, start_extra=None):
    from test_adapters import KRAKEN_START, KRAKEN_TRADES

    start = KRAKEN_START if start_extra is None else (
        "recorder", {**KRAKEN_START[1], **start_extra})  # fmt: skip
    base = log("kraken", start, KRAKEN_INSTRUMENT, KRAKEN_BOOK_ACK, KRAKEN_SNAPSHOT,
               KRAKEN_TRADES, KRAKEN_TRADES, KRAKEN_TRADES)  # fmt: skip
    return stamped(base, steps, jitter)


def steps_of(normalized):
    rows = events_of(normalized, "kraken")
    return [(r["seq"], r["connection"], r["observed"]) for r in rows if r["kind"] == K.CLOCK_STEP]


def test_a_session_with_monotonic_stamps_says_so_in_its_start_event():
    identity = {"platform": "win32", "machine_id": "m", "boot_id": "BootId:58",
                "counter": "QueryPerformanceCounter()", "counter_resolution_ns": 100}  # fmt: skip
    normalized = normalize(kraken_records_with(start_extra={"clock_identity": identity}))
    start = events_of(normalized, "kraken")[0]
    assert K(start["kind"]) == K.SESSION_START
    detail = json.loads(start["detail"])
    assert (detail["monotonic_stamps"], detail["clock_identity"]) == (True, identity)


@pytest.mark.parametrize("step", [-1_241_000_000, -60_000_000, 40_000_000, 3_000_000_000])
def test_a_wall_clock_step_is_an_event_with_its_signed_size(step):
    normalized = normalize(kraken_records_with(steps={5: step}))
    assert steps_of(normalized) == [(5, "ws", step)]
    assert normalized.report.events["CLOCK_STEP"] == 1


def test_movement_below_the_floor_is_not_a_step():
    assert CLOCK_STEP_FLOOR_NS == 25_000_000
    # Each record differs from the one before by less than the floor.
    wobble = {3: 10_000_000, 4: -8_000_000, 5: 12_000_000, 6: 0}
    assert steps_of(normalize(kraken_records_with(jitter=wobble))) == []


def test_each_step_is_reported_once_against_the_offset_before_it():
    normalized = normalize(kraken_records_with(steps={3: 100_000_000, 6: -300_000_000}))
    assert steps_of(normalized) == [(3, "ws", 100_000_000), (6, "ws", -300_000_000)]


def test_a_new_session_starts_a_new_baseline():
    first = kraken_records_with()
    second = [RawRecord(r.recv_ts + 10**12, r.seq, r.venue, r.channel, r.payload,
                        r.recv_mono_ns + 5 * 10**11) for r in kraken_records_with()]  # fmt: skip
    assert steps_of(normalize(first + second)) == []


def test_records_without_monotonic_stamps_produce_no_step_events():
    from test_fixtures import fixture_path

    normalized = normalize(read_raw_log(fixture_path("kraken")))
    assert normalized.report.events["CLOCK_STEP"] == 0
