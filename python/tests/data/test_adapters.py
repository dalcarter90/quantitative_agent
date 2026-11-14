"""Kraken and Coinbase adapters: raw records to normalized rows.

The hand-built messages below are trimmed copies of real messages from each
venue. The expected rows were worked out by hand from the venue documentation:

- Kraken v2 book and trade channels
  https://docs.kraken.com/api/docs/websocket-v2/book
  https://docs.kraken.com/api/docs/websocket-v2/trade  ("side": the taker's)
- Coinbase Advanced Trade level2 and market_trades channels
  https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-channels
  (a market trade's "side" is the maker's, so the aggressor is the opposite)
"""

import json
import random

import pytest

from quantagent.data.adapters import Normalizer, normalize, parse_rfc3339_ns
from quantagent.data.rawlog import RawRecord
from quantagent.data.schema import (
    BOOK_EVENTS,
    DATASETS,
    INSTRUMENTS,
    TRADES,
    BookEventKind,
    Side,
    StreamEventKind,
    table_from_rows,
)

NS = 1_000_000_000
# 2026-11-11T03:06:47Z
BASE = 1_794_366_407 * NS


def log(venue, *entries):
    """Build raw records. Each entry is (channel, payload) with str, bytes or dict payload."""
    records = []
    for seq, (channel, payload) in enumerate(entries):
        if isinstance(payload, dict):
            payload = json.dumps(payload, separators=(",", ":"))
        if isinstance(payload, str):
            payload = payload.encode()
        records.append(RawRecord(BASE + seq * 1000, seq, venue, channel, payload))
    return records


def rows_of(normalized, dataset, venue, instrument="BTC-USD"):
    """The rows of one dataset, without log_id, after checking that every row
    names the one log they came from (log ids have their own tests)."""
    rows = normalized.rows.get((dataset.name, venue, instrument), [])
    assert len({row["log_id"] for row in rows}) <= 1
    return [{k: v for k, v in row.items() if k != "log_id"} for row in rows]


# --- timestamps ------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("1970-01-01T00:00:00Z", 0),
        ("1970-01-01T00:00:00.000000001Z", 1),
        ("2026-11-11T03:06:47Z", BASE),
        ("2026-11-11T03:06:47.646791889Z", BASE + 646_791_889),
        ("2026-11-11T03:06:47.646791Z", BASE + 646_791_000),
        ("2026-11-11T03:06:47.6Z", BASE + 600_000_000),
        ("2026-11-11T03:06:47.08228108Z", BASE + 82_281_080),  # 8 digits, as Coinbase sends
        ("2024-02-29T23:59:59.999999999Z", 1_709_251_199_999_999_999),
    ],
)
def test_rfc3339_timestamps_convert_exactly_to_nanoseconds(text, expected):
    result = parse_rfc3339_ns(text)
    assert result == expected
    assert type(result) is int


@pytest.mark.parametrize(
    "text",
    [
        "",
        "2026-11-11",
        "2026-11-11T03:06:47",  # no zone
        "2026-11-11T03:06:47+00:00",
        "2026-11-11 03:06:47Z",
        "2026-11-11T03:06:47.Z",
        "2026-11-11T03:06:47.1234567890Z",  # 10 fraction digits
        "2026-13-01T00:00:00Z",
        "2026-02-30T00:00:00Z",
        "2026-11-11T24:00:00Z",
        "2026-11-11T03:60:00Z",
        "2026-11-11T03:06:60Z",
        "1969-12-31T23:59:59Z",
        "2026-11-11T03:06:47Z ",
        "not a time",
    ],
)
def test_rfc3339_parser_rejects_anything_else(text):
    with pytest.raises(ValueError):
        parse_rfc3339_ns(text)


@pytest.mark.parametrize("value", [None, 1_794_366_407, 1.5, b"2026-11-11T03:06:47Z"])
def test_rfc3339_parser_only_accepts_str(value):
    with pytest.raises(TypeError):
        parse_rfc3339_ns(value)


# --- Kraken ----------------------------------------------------------------------

KRAKEN_START = (
    "recorder",
    {"event": "start", "venue": "kraken", "instrument": "BTC-USD", "symbol": "BTC/USD",
     "depth": 10, "recorder_version": 1},
)  # fmt: skip
KRAKEN_INSTRUMENT = (
    "ws",
    '{"channel":"instrument","type":"snapshot","data":{"assets":[{"id":"USD","precision":4}],'
    '"pairs":[{"symbol":"ETH/USD","base":"ETH","quote":"USD","status":"online","qty_precision":8,'
    '"qty_increment":0.00000001,"price_precision":2,"price_increment":0.01},'
    '{"symbol":"BTC/USD","base":"BTC","quote":"USD","status":"online","qty_precision":8,'
    '"qty_increment":0.00000001,"price_precision":1,"cost_precision":5,"marginable":true,'
    '"tick_size":0.1,"price_increment":0.1,"qty_min":0.00006000}]}}',
)
KRAKEN_BOOK_ACK = (
    "ws",
    '{"method":"subscribe","result":{"channel":"book","depth":10,"snapshot":true,'
    '"symbol":"BTC/USD"},"success":true,"time_in":"2026-11-11T00:56:24.861486Z",'
    '"time_out":"2026-11-11T00:56:24.861508Z"}',
)
KRAKEN_SNAPSHOT = (
    "ws",
    '{"channel":"book","type":"snapshot","data":[{"symbol":"BTC/USD",'
    '"bids":[{"price":73468.5,"qty":0.20416909},{"price":73466.9,"qty":3.40289101}],'
    '"asks":[{"price":73468.6,"qty":0.89472056},{"price":73470.0,"qty":0.00208204}],'
    '"checksum":2937112725,"timestamp":"2026-11-11T03:06:47.006069Z"}]}',
)
KRAKEN_UPDATE = (
    "ws",
    '{"channel":"book","type":"update","data":[{"symbol":"BTC/USD",'
    '"bids":[{"price":73466.9,"qty":0.00000000},{"price":73468.5,"qty":0.20467952},'
    '{"price":73468.5,"qty":0.00006120}],"asks":[],'
    '"checksum":1401313711,"timestamp":"2026-11-11T03:06:47.064192Z"}]}',
)
KRAKEN_TRADES = (
    "ws",
    '{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"sell",'
    '"price":73469.7,"qty":0.00051042,"ord_type":"limit","trade_id":115202912,'
    '"timestamp":"2026-11-11T03:06:47.999916Z"},{"symbol":"BTC/USD","side":"buy",'
    '"price":73470.0,"qty":1.00000000,"ord_type":"market","trade_id":115202913,'
    '"timestamp":"2026-11-11T03:06:48.000001Z"}]}',
)
KRAKEN_PROBE_SNAPSHOT = ("probe", KRAKEN_SNAPSHOT[1])
KRAKEN_NOISE = [
    ("ws", '{"channel":"heartbeat"}'),
    ("ws", '{"channel":"status","type":"update","data":[{"system":"online","api_version":"v2"}]}'),
    ("ws", '{"method":"pong","time_in":"2026-11-11T00:56:24.8Z",'
           '"time_out":"2026-11-11T00:56:24.8Z"}'),
    ("recorder", {"event": "connect", "connection": "ws", "url": "wss://ws.kraken.com/v2"}),
    ("recorder", {"event": "subscribe", "connection": "ws", "request": {"method": "subscribe"}}),
    ("recorder", {"event": "disconnect", "connection": "probe", "reason": "snapshot received"}),
    ("recorder", {"event": "stop"}),
]  # fmt: skip


def kraken_session(*entries):
    return normalize(log("kraken", KRAKEN_START, KRAKEN_INSTRUMENT, KRAKEN_BOOK_ACK, *entries))


def test_kraken_instrument_metadata_has_no_venue_timestamp_so_it_is_a_receipt_row():
    normalized = kraken_session()
    assert rows_of(normalized, INSTRUMENTS, "kraken") == [
        {
            "event_ts": BASE + 1000,
            "event_ts_source": "receipt",
            "recv_ts": BASE + 1000,
            "seq": 1,
            "price_exp": 1,
            "qty_exp": 8,
            "tick_size": 1,  # 0.1 at one decimal place
            "lot_size": 1,  # 0.00000001 at eight decimal places
        }
    ]
    # Pairs that are not being recorded are not stored.
    assert set(normalized.rows) == {
        ("instruments", "kraken", "BTC-USD"),
        ("stream_events", "kraken", "BTC-USD"),  # the session start and the subscription ack
    }


def test_kraken_book_snapshot_rows_are_exact():
    normalized = kraken_session(KRAKEN_SNAPSHOT)
    event_ts = BASE + 6_069_000
    common = {
        "event_ts": event_ts,
        "event_ts_source": "venue",
        "recv_ts": BASE + 3000,
        "seq": 3,
        "kind": BookEventKind.SNAPSHOT,
        "venue_seq_first": None,
        "venue_seq_last": None,
        "checksum": 2937112725,
        "depth": 10,
    }
    assert rows_of(normalized, BOOK_EVENTS, "kraken") == [
        {**common, "level_index": 0, "side": Side.BUY, "price": 734685, "qty": 20416909},
        {**common, "level_index": 1, "side": Side.BUY, "price": 734669, "qty": 340289101},
        {**common, "level_index": 2, "side": Side.SELL, "price": 734686, "qty": 89472056},
        {**common, "level_index": 3, "side": Side.SELL, "price": 734700, "qty": 208204},
    ]


def test_kraken_book_update_keeps_message_order_removals_and_repeated_prices():
    normalized = kraken_session(KRAKEN_SNAPSHOT, KRAKEN_UPDATE)
    update = [r for r in rows_of(normalized, BOOK_EVENTS, "kraken") if r["seq"] == 4]
    assert [(r["level_index"], r["side"], r["price"], r["qty"]) for r in update] == [
        (0, Side.BUY, 734669, 0),  # qty 0 removes the level
        (1, Side.BUY, 734685, 20467952),
        (2, Side.BUY, 734685, 6120),  # the same price again, later in the message
    ]
    assert {r["kind"] for r in update} == {BookEventKind.DELTA}
    assert {r["checksum"] for r in update} == {1401313711}
    assert {r["event_ts"] for r in update} == {BASE + 64_192_000}
    assert {r["event_ts_source"] for r in update} == {"venue"}
    assert {r["venue_seq_last"] for r in update} == {None}


def test_kraken_trades_use_the_taker_side_as_given():
    normalized = kraken_session(KRAKEN_TRADES)
    assert rows_of(normalized, TRADES, "kraken") == [
        {
            "event_ts": BASE + 999_916_000,
            "event_ts_source": "venue",
            "recv_ts": BASE + 3000,
            "seq": 3,
            "trade_id": "115202912",
            "price": 734697,
            "qty": 51042,
            "side": Side.SELL,
        },
        {
            "event_ts": BASE + NS + 1000,
            "event_ts_source": "venue",
            "recv_ts": BASE + 3000,
            "seq": 3,
            "trade_id": "115202913",
            "price": 734700,
            "qty": 100000000,
            "side": Side.BUY,
        },
    ]


def test_kraken_probe_snapshot_is_a_check_snapshot_not_part_of_the_stream():
    normalized = kraken_session(KRAKEN_SNAPSHOT, KRAKEN_PROBE_SNAPSHOT, ("probe", KRAKEN_UPDATE[1]))
    rows = rows_of(normalized, BOOK_EVENTS, "kraken")
    assert [r["kind"] for r in rows if r["seq"] == 3] == [BookEventKind.SNAPSHOT] * 4
    assert [r["kind"] for r in rows if r["seq"] == 4] == [BookEventKind.CHECK_SNAPSHOT] * 4
    assert not [r for r in rows if r["seq"] == 5]  # updates on a probe are not stored
    assert normalized.report.ignored["probe.book.update"] == 1


def test_kraken_noise_is_counted_as_ignored_not_rejected():
    normalized = kraken_session(*KRAKEN_NOISE)
    report = normalized.report
    assert report.rejects == []
    assert report.ignored["heartbeat"] == 1
    assert report.ignored["status.update"] == 1
    assert report.ignored["pong"] == 1
    assert report.ignored["subscribe.ack"] == 1
    assert report.ignored["recorder.start"] == 1
    assert report.ignored["recorder.stop"] == 1
    assert report.records == 3 + len(KRAKEN_NOISE)
    assert report.outcomes == {"data": 1, "ignored": 2 + len(KRAKEN_NOISE)}


def test_kraken_depth_comes_from_the_subscription_on_each_connection():
    probe_ack = ("probe", KRAKEN_BOOK_ACK[1].replace('"depth":10', '"depth":25'))
    normalized = kraken_session(KRAKEN_SNAPSHOT, probe_ack, KRAKEN_PROBE_SNAPSHOT)
    rows = rows_of(normalized, BOOK_EVENTS, "kraken")
    assert {r["depth"] for r in rows if r["kind"] == BookEventKind.SNAPSHOT} == {10}
    assert {r["depth"] for r in rows if r["kind"] == BookEventKind.CHECK_SNAPSHOT} == {25}


# --- Coinbase --------------------------------------------------------------------

COINBASE_START = (
    "recorder",
    {"event": "start", "venue": "coinbase", "instrument": "BTC-USD", "symbol": "BTC-USD",
     "depth": 25, "recorder_version": 1},
)  # fmt: skip
COINBASE_PRODUCT = (
    "rest:product",
    '{"product_id":"BTC-USD", "price":"73462.72", "base_increment":"0.00000001", '
    '"quote_increment":"0.01", "price_increment":"0.01", "status":"online", "product_type":"SPOT"}',
)
COINBASE_CONNECT = (
    "recorder",
    {"event": "connect", "connection": "ws", "url": "wss://advanced-trade-ws.coinbase.com"},
)
COINBASE_SNAPSHOT = (
    "ws",
    '{"channel":"l2_data","timestamp":"2026-11-11T03:06:47.624143697Z","sequence_num":2,'
    '"events":[{"type":"snapshot","product_id":"BTC-USD","updates":['
    '{"side":"bid","event_time":"2026-11-11T03:06:47.490859Z","price_level":"73469.65",'
    '"new_quantity":"0.02208558"},'
    '{"side":"offer","event_time":"2026-11-11T03:06:47.490859Z","price_level":"73469.66",'
    '"new_quantity":"0.75654522"}]}]}',
)
COINBASE_UPDATE = (
    "ws",
    '{"channel":"l2_data","timestamp":"2026-11-11T03:06:47.646791889Z","sequence_num":3,'
    '"events":[{"type":"update","product_id":"BTC-USD","updates":['
    '{"side":"bid","event_time":"2026-11-11T03:06:47.572686Z","price_level":"73464.78",'
    '"new_quantity":"0"},'
    '{"side":"offer","event_time":"2026-11-11T03:06:47.572686Z","price_level":"73470",'
    '"new_quantity":"0.0007"}]}]}',
)
COINBASE_TRADES = (
    "ws",
    '{"channel":"market_trades","timestamp":"2026-11-11T03:06:48.052121501Z","sequence_num":4,'
    '"events":[{"type":"update","trades":['
    '{"product_id":"BTC-USD","trade_id":"1118502354","price":"73469.65","size":"0.02129674",'
    '"time":"2026-11-11T03:06:47.947537Z","side":"BUY"},'
    '{"product_id":"BTC-USD","trade_id":"1118502355","price":"73466.84","size":"0.00000007",'
    '"time":"2026-11-11T03:06:47.950000Z","side":"SELL"}]}]}',
)
COINBASE_NOISE = [
    ("ws", '{"channel":"subscriptions","timestamp":"2026-11-11T03:06:47.149684966Z",'
           '"sequence_num":0,"events":[{"subscriptions":{"heartbeats":["heartbeats"]}}]}'),
    ("ws", '{"channel":"heartbeats","timestamp":"2026-11-11T03:06:47.162283607Z",'
           '"sequence_num":1,"events":[{"current_time":"2026-11-11 03:06:47.158 +0000 UTC",'
           '"heartbeat_counter":39528}]}'),
]  # fmt: skip


def coinbase_session(*entries):
    head = [COINBASE_START, COINBASE_PRODUCT, COINBASE_CONNECT, *COINBASE_NOISE]
    return normalize(log("coinbase", *head, *entries))


def test_coinbase_product_metadata_has_no_venue_timestamp_so_it_is_a_receipt_row():
    normalized = coinbase_session()
    assert rows_of(normalized, INSTRUMENTS, "coinbase") == [
        {
            "event_ts": BASE + 1000,
            "event_ts_source": "receipt",
            "recv_ts": BASE + 1000,
            "seq": 1,
            "price_exp": 2,  # price_increment 0.01
            "qty_exp": 8,  # base_increment 0.00000001
            "tick_size": 1,
            "lot_size": 1,
        }
    ]


def test_coinbase_exponents_come_from_the_increments():
    product = COINBASE_PRODUCT[1].replace('"price_increment":"0.01"', '"price_increment":"0.050"')
    product = product.replace('"base_increment":"0.00000001"', '"base_increment":"1"')
    normalized = normalize(log("coinbase", COINBASE_START, ("rest:product", product)))
    (row,) = rows_of(normalized, INSTRUMENTS, "coinbase")
    assert (row["price_exp"], row["tick_size"]) == (2, 5)
    assert (row["qty_exp"], row["lot_size"]) == (0, 1)


def test_coinbase_book_snapshot_and_update_rows_are_exact():
    normalized = coinbase_session(COINBASE_SNAPSHOT, COINBASE_UPDATE)
    snapshot_time = BASE + 490_859_000
    update_time = BASE + 572_686_000
    base = {
        "event_ts_source": "venue",
        "venue_seq_first": None,
        "checksum": None,
        "depth": None,
    }
    snapshot = {**base, "event_ts": snapshot_time, "recv_ts": BASE + 5000, "seq": 5,
                "kind": BookEventKind.SNAPSHOT, "venue_seq_last": 2}  # fmt: skip
    update = {**base, "event_ts": update_time, "recv_ts": BASE + 6000, "seq": 6,
              "kind": BookEventKind.DELTA, "venue_seq_last": 3}  # fmt: skip
    assert rows_of(normalized, BOOK_EVENTS, "coinbase") == [
        {**snapshot, "level_index": 0, "side": Side.BUY, "price": 7346965, "qty": 2208558},
        {**snapshot, "level_index": 1, "side": Side.SELL, "price": 7346966, "qty": 75654522},
        {**update, "level_index": 0, "side": Side.BUY, "price": 7346478, "qty": 0},
        {**update, "level_index": 1, "side": Side.SELL, "price": 7347000, "qty": 70000},
    ]


def test_coinbase_trade_side_is_the_makers_so_the_aggressor_is_the_opposite():
    normalized = coinbase_session(COINBASE_TRADES)
    rows = rows_of(normalized, TRADES, "coinbase")
    assert rows == [
        {
            "event_ts": BASE + 947_537_000,
            "event_ts_source": "venue",
            "recv_ts": BASE + 5000,
            "seq": 5,
            "trade_id": "1118502354",
            "price": 7346965,
            "qty": 2129674,
            "side": Side.SELL,  # maker BUY
        },
        {
            "event_ts": BASE + 950_000_000,
            "event_ts_source": "venue",
            "recv_ts": BASE + 5000,
            "seq": 5,
            "trade_id": "1118502355",
            "price": 7346684,
            "qty": 7,
            "side": Side.BUY,  # maker SELL
        },
    ]


def test_coinbase_probe_snapshot_is_a_check_snapshot():
    probe_connect = ("recorder", {"event": "connect", "connection": "probe", "url": "wss://x"})
    probe_snapshot = ("probe", COINBASE_SNAPSHOT[1].replace('"sequence_num":2', '"sequence_num":0'))
    probe_update = ("probe", COINBASE_UPDATE[1].replace('"sequence_num":3', '"sequence_num":1'))
    normalized = coinbase_session(COINBASE_SNAPSHOT, probe_connect, probe_snapshot, probe_update)
    rows = rows_of(normalized, BOOK_EVENTS, "coinbase")
    assert [r["kind"] for r in rows] == [BookEventKind.SNAPSHOT] * 2 + [
        BookEventKind.CHECK_SNAPSHOT
    ] * 2
    assert normalized.report.ignored["probe.l2_data.update"] == 1
    assert normalized.report.sequence_anomalies == []  # each connection has its own numbering


def test_coinbase_noise_is_counted_as_ignored_not_rejected():
    report = coinbase_session().report
    assert report.rejects == []
    assert report.ignored["subscriptions"] == 1
    assert report.ignored["heartbeats"] == 1
    assert report.outcomes == {"data": 1, "ignored": 4}


def test_coinbase_sequence_numbers_are_checked_per_connection_across_channels():
    def at(entry, number):
        channel, text = entry
        original = json.loads(text)["sequence_num"]
        return channel, text.replace(f'"sequence_num":{original}', f'"sequence_num":{number}')

    clean = coinbase_session(COINBASE_SNAPSHOT, COINBASE_UPDATE, COINBASE_TRADES)
    assert clean.report.sequence_anomalies == []

    # 0, 1 (noise), 2, then 4: message 3 was lost. Then 4 again, then 3 arrives late.
    broken = coinbase_session(
        COINBASE_SNAPSHOT, at(COINBASE_UPDATE, 4), at(COINBASE_TRADES, 4), at(COINBASE_UPDATE, 3)
    )
    anomalies = [(a.kind, a.connection, a.expected, a.received, a.seq)
                 for a in broken.report.sequence_anomalies]  # fmt: skip
    assert anomalies == [
        ("gap", "ws", 3, 4, 6),
        ("repeat", "ws", 5, 4, 7),
        ("out_of_order", "ws", 5, 3, 8),
    ]
    # The messages are still stored; flagging is not dropping.
    assert len(rows_of(broken, BOOK_EVENTS, "coinbase")) == 6
    assert broken.report.rejects == []

    # A new connection starts numbering again without that being an anomaly.
    reconnected = coinbase_session(
        COINBASE_SNAPSHOT, COINBASE_UPDATE, COINBASE_CONNECT, *COINBASE_NOISE, COINBASE_SNAPSHOT
    )
    assert reconnected.report.sequence_anomalies == []


# --- ADR 0008 ---------------------------------------------------------------------


def test_event_ts_source_is_decided_by_message_type_not_by_comparing_timestamps():
    # A book snapshot whose venue timestamp happens to equal our receipt time
    # is still a venue timestamp.
    records = log("kraken", KRAKEN_START, KRAKEN_INSTRUMENT, KRAKEN_BOOK_ACK, KRAKEN_SNAPSHOT)
    snapshot = records[3]
    same_time = RawRecord(
        BASE + 6_069_000, snapshot.seq, snapshot.venue, snapshot.channel, snapshot.payload
    )
    normalized = normalize([*records[:3], same_time])
    rows = rows_of(normalized, BOOK_EVENTS, "kraken")
    assert {(r["event_ts"] == r["recv_ts"], r["event_ts_source"]) for r in rows} == {
        (True, "venue")
    }


@pytest.mark.parametrize(
    "normalized",
    [
        pytest.param(
            lambda: kraken_session(KRAKEN_SNAPSHOT, KRAKEN_UPDATE, KRAKEN_TRADES), id="kraken"
        ),
        pytest.param(lambda: coinbase_session(COINBASE_SNAPSHOT, COINBASE_TRADES), id="coinbase"),
    ],
)  # fmt: skip
def test_every_receipt_row_holds_recv_ts_and_rows_pass_the_schema(normalized):
    normalized = normalized()
    sources = set()
    for (dataset_name, _, _), rows in normalized.rows.items():
        dataset = DATASETS[dataset_name]
        table_from_rows(dataset, rows)  # raises if a row breaks the schema
        for row in rows:
            sources.add((dataset_name, row["event_ts_source"]))
            if row["event_ts_source"] == "receipt":
                assert row["event_ts"] == row["recv_ts"]
    assert ("instruments", "receipt") in sources
    assert ("book_events", "venue") in sources and ("trades", "venue") in sources
    assert ("book_events", "receipt") not in sources and ("trades", "receipt") not in sources


# --- malformed and unknown input ------------------------------------------------------

KRAKEN_BAD = {
    "invalid_json": [b"", b"{", b'{"channel":"book"', b"\xff\xfe\x00", b"not json", b"[1,2"],
    "malformed_message": [
        b"[]",
        b"42",
        b'"text"',
        b"null",
        b'{"channel":"book","type":"snapshot"}',
        b'{"channel":"book","type":"snapshot","data":{}}',
        b'{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[{"price":1.0}],'
        b'"asks":[],"checksum":1,"timestamp":"2026-11-11T03:06:47Z"}]}',
        b'{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[],'
        b'"asks":[{"price":"abc","qty":1.0}],"checksum":1,"timestamp":"2026-11-11T03:06:47Z"}]}',
        b'{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[],'
        b'"asks":[{"price":1.23,"qty":1.0}],"checksum":1,"timestamp":"2026-11-11T03:06:47Z"}]}',
        b'{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[],'
        b'"asks":[{"price":1e5,"qty":1.0}],"checksum":1,"timestamp":"2026-11-11T03:06:47Z"}]}',
        b'{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[],'
        b'"asks":[{"price":1.0,"qty":1.0}],"checksum":1,"timestamp":"yesterday"}]}',
        b'{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[],'
        b'"asks":[{"price":1.0,"qty":1.0}],"checksum":-5,"timestamp":"2026-11-11T03:06:47Z"}]}',
        b'{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":null,'
        b'"asks":[],"checksum":1,"timestamp":"2026-11-11T03:06:47Z"}]}',
        b'{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"up","price":1.0,'
        b'"qty":1.0,"trade_id":1,"timestamp":"2026-11-11T03:06:47Z"}]}',
        b'{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"buy","price":1.0,'
        b'"qty":NaN,"trade_id":1,"timestamp":"2026-11-11T03:06:47Z"}]}',
        b'{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"buy","price":1.0,'
        b'"qty":99999999999999.0,"trade_id":1,"timestamp":"2026-11-11T03:06:47Z"}]}',
        b'{"channel":"instrument","type":"snapshot","data":{"pairs":[{"symbol":"BTC/USD",'
        b'"price_precision":40,"qty_precision":8,"price_increment":0.1,"qty_increment":0.1}]}}',
    ],
    "unexpected_symbol": [
        b'{"channel":"book","type":"update","data":[{"symbol":"DOGE/USD","bids":[],'
        b'"asks":[{"price":1.0,"qty":1.0}],"checksum":1,"timestamp":"2026-11-11T03:06:47Z"}]}',
    ],
    "unknown_message": [
        b"{}",
        b'{"channel":"ohlc","type":"update","data":[]}',
        b'{"channel":"book","type":"replace","data":[]}',
        b'{"method":"explode","success":true}',
    ],
    "venue_error": [
        b'{"error":"Currency pair not supported XYZ/USD","method":"subscribe","success":false}',
    ],
}

COINBASE_BAD = {
    "invalid_json": [b"", b"{", b"\xff", b'{"channel":"l2_data",'],
    "malformed_message": [
        b"[]",
        b"7",
        b'{"channel":"l2_data","timestamp":"2026-11-11T03:06:47Z","sequence_num":9}',
        b'{"channel":"l2_data","timestamp":"2026-11-11T03:06:47Z","sequence_num":"x","events":[]}',
        b'{"channel":"l2_data","timestamp":"2026-11-11T03:06:47Z","sequence_num":9,"events":['
        b'{"type":"update","product_id":"BTC-USD","updates":[{"side":"north",'
        b'"event_time":"2026-11-11T03:06:47Z","price_level":"1.00","new_quantity":"1"}]}]}',
        b'{"channel":"l2_data","timestamp":"2026-11-11T03:06:47Z","sequence_num":9,"events":['
        b'{"type":"update","product_id":"BTC-USD","updates":[{"side":"bid",'
        b'"event_time":"2026-11-11T03:06:47Z","price_level":"1.001","new_quantity":"1"}]}]}',
        b'{"channel":"l2_data","timestamp":"2026-11-11T03:06:47Z","sequence_num":9,"events":['
        b'{"type":"update","product_id":"BTC-USD","updates":[{"side":"bid",'
        b'"event_time":"2026-11-11T03:06:47Z","price_level":1.0,"new_quantity":"1"}]}]}',
        b'{"channel":"l2_data","timestamp":"2026-11-11T03:06:47Z","sequence_num":9,"events":['
        b'{"type":"update","product_id":"BTC-USD","updates":[{"side":"bid",'
        b'"event_time":"soon","price_level":"1.00","new_quantity":"1"}]}]}',
        b'{"channel":"market_trades","timestamp":"2026-11-11T03:06:47Z","sequence_num":9,"events":['
        b'{"type":"update","trades":[{"product_id":"BTC-USD","trade_id":"1","price":"1.00",'
        b'"size":"1","time":"2026-11-11T03:06:47Z","side":"HOLD"}]}]}',
        b'{"channel":"market_trades","timestamp":"2026-11-11T03:06:47Z","sequence_num":9,"events":['
        b'{"type":"update","trades":[{"product_id":"BTC-USD","price":"1.00",'
        b'"size":"1","time":"2026-11-11T03:06:47Z","side":"BUY"}]}]}',
    ],
    "unexpected_symbol": [
        b'{"channel":"l2_data","timestamp":"2026-11-11T03:06:47Z","sequence_num":9,"events":['
        b'{"type":"update","product_id":"DOGE-USD","updates":[{"side":"bid",'
        b'"event_time":"2026-11-11T03:06:47Z","price_level":"1.00","new_quantity":"1"}]}]}',
    ],
    "unknown_message": [
        b"{}",
        b'{"channel":"candles","timestamp":"2026-11-11T03:06:47Z","sequence_num":9,"events":[]}',
        b'{"channel":"l2_data","timestamp":"2026-11-11T03:06:47Z","sequence_num":9,"events":['
        b'{"type":"rewind","product_id":"BTC-USD","updates":[]}]}',
    ],
    "venue_error": [
        b'{"type":"error","message":"authentication failure"}',
        b'{"channel":"error","timestamp":"2026-11-11T03:06:47Z","sequence_num":9,'
        b'"events":[{"message":"rate limit exceeded"}]}',
    ],
}

BAD = [
    pytest.param(venue, reason, payload, id=f"{venue}-{reason}-{index}")
    for venue, table in (("kraken", KRAKEN_BAD), ("coinbase", COINBASE_BAD))
    for reason, payloads in table.items()
    for index, payload in enumerate(payloads)
]


@pytest.mark.parametrize(("venue", "reason", "payload"), BAD)
def test_a_bad_message_is_rejected_with_a_reason_and_nothing_else_is_lost(venue, reason, payload):
    if venue == "kraken":
        head = [KRAKEN_START, KRAKEN_INSTRUMENT, KRAKEN_BOOK_ACK]
        good_before, good_after, dataset = KRAKEN_SNAPSHOT, KRAKEN_TRADES, TRADES
    else:
        head = [COINBASE_START, COINBASE_PRODUCT, COINBASE_CONNECT]
        good_before, good_after, dataset = COINBASE_SNAPSHOT, COINBASE_TRADES, TRADES
    records = log(venue, *head, good_before, ("ws", payload), good_after)
    normalized = normalize(records)

    (reject,) = normalized.report.rejects
    assert reject.reason == reason
    assert reject.seq == len(head) + 1
    assert reject.recv_ts == records[reject.seq].recv_ts
    assert reject.channel == "ws"
    assert isinstance(reject.detail, str) and reject.detail
    # The messages on either side are intact, and no row came from the bad one.
    assert len(rows_of(normalized, dataset, venue)) == 2
    for (dataset_name, _, _), rows in normalized.rows.items():
        from_bad_record = [row for row in rows if row["seq"] == reject.seq]
        if dataset_name == "stream_events":
            # The hole in the stream is itself recorded (ADR 0010).
            kinds = [StreamEventKind(row["kind"]) for row in from_bad_record]
            assert kinds.count(StreamEventKind.MESSAGE_REJECTED) == 1
            assert from_bad_record[-1]["detail"].startswith(reason)
        else:
            assert from_bad_record == []
    assert normalized.report.outcomes["rejected"] == 1
    assert sum(normalized.report.outcomes.values()) == normalized.report.records == len(records)
    assert normalized.report.rejected == {reason: 1}


def test_a_partly_bad_message_produces_no_rows_at_all():
    # The second level is malformed. The first, valid, level must not be stored.
    payload = (
        b'{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":['
        b'{"price":73468.5,"qty":0.5},{"price":"oops","qty":0.5}],"asks":[],'
        b'"checksum":1,"timestamp":"2026-11-11T03:06:47Z"}]}'
    )
    normalized = kraken_session(("ws", payload))
    assert rows_of(normalized, BOOK_EVENTS, "kraken") == []
    assert normalized.report.rejected == {"malformed_message": 1}


@pytest.mark.parametrize(
    ("venue", "entries", "reason"),
    [
        ("kraken", [KRAKEN_SNAPSHOT], "no_session_start"),
        ("kraken", [KRAKEN_START, KRAKEN_SNAPSHOT], "no_instrument_metadata"),
        ("kraken", [KRAKEN_START, KRAKEN_TRADES], "no_instrument_metadata"),
        ("coinbase", [COINBASE_SNAPSHOT], "no_session_start"),
        ("coinbase", [COINBASE_START, COINBASE_SNAPSHOT], "no_instrument_metadata"),
        ("coinbase", [COINBASE_START, ("rest:product", b"<html>503</html>")], "invalid_json"),
        ("coinbase", [COINBASE_START, ("rest:other", b"{}")], "unknown_channel"),
        ("kraken", [KRAKEN_START, ("carrier-pigeon", b"{}")], "unknown_channel"),
        ("kraken", [("recorder", b"not json")], "invalid_json"),
        ("kraken", [("recorder", b'{"event":"teleport"}')], "unknown_message"),
        ("binance", [("ws", b"{}")], "unknown_venue"),
    ],
)
def test_data_without_its_prerequisites_is_rejected_not_guessed(venue, entries, reason):
    normalized = normalize(log(venue, *entries))
    assert [r.reason for r in normalized.report.rejects] == [reason]
    # Nothing but stream events (the session start, the reject) may be produced.
    assert {key[0] for key in normalized.rows} <= {"stream_events"}


@pytest.mark.parametrize("venue", ["kraken", "coinbase"])
def test_random_damage_never_crashes_and_every_record_is_accounted_for(venue):
    rng = random.Random(1234)
    if venue == "kraken":
        head = [KRAKEN_START, KRAKEN_INSTRUMENT, KRAKEN_BOOK_ACK]
        good = [KRAKEN_SNAPSHOT, KRAKEN_UPDATE, KRAKEN_TRADES, *KRAKEN_NOISE[:3]]
    else:
        head = [COINBASE_START, COINBASE_PRODUCT, COINBASE_CONNECT]
        good = [COINBASE_SNAPSHOT, COINBASE_UPDATE, COINBASE_TRADES, *COINBASE_NOISE]
    entries = list(head)
    for _ in range(1500):
        channel, text = rng.choice(good)
        data = bytearray(text.encode())
        action = rng.randrange(5)
        if action == 0:
            data = data[: rng.randrange(len(data))]  # truncated
        elif action == 1:
            data[rng.randrange(len(data))] = rng.randrange(256)  # one byte changed
        elif action == 2:
            position = rng.randrange(len(data))
            del data[position : position + rng.randrange(1, 12)]  # a piece removed
        elif action == 3:
            position = rng.randrange(len(data))
            data[position:position] = bytes(
                rng.randrange(32, 127) for _ in range(rng.randrange(1, 6))
            )
        entries.append((channel, bytes(data)))  # action 4: left intact
    normalized = normalize(log(venue, *entries))  # must not raise

    report = normalized.report
    assert report.records == len(entries)
    assert sum(report.outcomes.values()) == report.records
    assert report.outcomes["rejected"] == len(report.rejects) > 100
    assert report.outcomes["data"] > 100
    # Whatever survived is still valid against the schema.
    for (dataset_name, _, _), rows in normalized.rows.items():
        table_from_rows(DATASETS[dataset_name], rows)


def test_the_normalizer_can_be_fed_and_drained_in_pieces():
    records = log("kraken", KRAKEN_START, KRAKEN_INSTRUMENT, KRAKEN_BOOK_ACK, KRAKEN_SNAPSHOT,
                  KRAKEN_UPDATE, KRAKEN_TRADES)  # fmt: skip
    whole = normalize(records)

    normalizer = Normalizer()
    pieces = {}
    for record in records:
        normalizer.feed(record)
        for key, rows in normalizer.take_rows().items():
            pieces.setdefault(key, []).extend(rows)
    assert normalizer.take_rows() == {}
    assert pieces == whole.rows
    assert normalizer.report.outcomes == whole.report.outcomes
