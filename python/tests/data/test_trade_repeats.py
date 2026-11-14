"""A trade seen twice (ADR 0015).

A venue can send the same trade twice: the history it resends after a stream
reconnect, or two logs fed to one normalizer that overlap. The store keeps one
row per trade_id. If the two observations differ in price, quantity or side,
the adapters write a TRADE_CONFLICT stream event and the quality check
reports it as an ERROR.
"""

import json

import pytest
from _builders import INT64_MAX
from test_adapters import (
    COINBASE_CONNECT,
    COINBASE_PRODUCT,
    COINBASE_START,
    KRAKEN_BOOK_ACK,
    KRAKEN_INSTRUMENT,
    KRAKEN_START,
    log,
)
from test_quality import run, store_from_rows

from quantagent.data.adapters import ingest, normalize
from quantagent.data.quality import Severity, check_stream_integrity
from quantagent.data.rawlog import RawLogWriter, RawRecord
from quantagent.data.schema import Side, StreamEventKind
from quantagent.data.session import PITSession
from quantagent.data.store import AppendOnlyStore

CONFLICT = StreamEventKind.TRADE_CONFLICT
INSTRUMENT = "BTC-USD"


def kraken_trade(trade_id=115202912, price="73469.7", qty="0.00051042", side="sell",
                 kind="update"):  # fmt: skip
    """Kraken sends prices and quantities as JSON numbers, so they go in unquoted."""
    trade = (f'{{"symbol":"BTC/USD","side":"{side}","price":{price},"qty":{qty},'
             f'"ord_type":"limit","trade_id":{trade_id},'
             '"timestamp":"2026-11-11T03:06:47.999916Z"}')  # fmt: skip
    return ("ws", f'{{"channel":"trade","type":"{kind}","data":[{trade}]}}')


def coinbase_trade(number, trade_id="1118502354", price="73469.65", size="0.02129674",
                   side="BUY"):  # fmt: skip
    trade = {"product_id": "BTC-USD", "trade_id": trade_id, "price": price, "size": size,
             "time": "2026-11-11T03:06:47.947537Z", "side": side}  # fmt: skip
    event = {"type": "update", "trades": [trade]}
    message = {"channel": "market_trades", "timestamp": "2026-11-11T03:06:48.052121501Z",
               "sequence_num": number, "events": [event]}  # fmt: skip
    return ("ws", json.dumps(message))


def kraken(*trades):
    return log("kraken", KRAKEN_START, KRAKEN_INSTRUMENT, KRAKEN_BOOK_ACK, *trades)


def coinbase(*trades):
    return log("coinbase", COINBASE_START, COINBASE_PRODUCT, COINBASE_CONNECT, *trades)


def conflicts(normalized, venue):
    rows = normalized.rows.get(("stream_events", venue, INSTRUMENT), [])
    return [r for r in rows if r["kind"] == CONFLICT]


def test_a_trade_seen_again_unchanged_is_one_row_and_no_event(tmp_path):
    records = kraken(kraken_trade(), kraken_trade())
    normalized = normalize(records)
    assert conflicts(normalized, "kraken") == []
    assert normalized.report.trade_repeats == 1
    path = tmp_path / "a.qaraw"
    with RawLogWriter(path) as writer:
        for r in records:
            writer.write(r.venue, r.channel, r.payload, recv_ts=r.recv_ts)
    ingest(path, AppendOnlyStore(tmp_path / "store"))
    with PITSession(tmp_path / "store", as_of=INT64_MAX) as session:
        trades = session.trades("kraken", INSTRUMENT, 0, INT64_MAX).to_pylist()
    assert [t["trade_id"] for t in trades] == ["115202912"]
    # The row kept is the later observation (ADR 0015, "Consequences").
    assert (trades[0]["seq"], trades[0]["recv_ts"]) == (records[-1].seq, records[-1].recv_ts)


@pytest.mark.parametrize(
    ("change", "field", "now"),
    [({"price": "73469.8"}, "price", 734698), ({"qty": "0.00051043"}, "qty", 51043),
     ({"side": "buy"}, "side", int(Side.BUY))],
)  # fmt: skip
def test_a_trade_seen_again_with_a_different_value_is_a_conflict(change, field, now):
    records = kraken(kraken_trade(), kraken_trade(**change))
    normalized = normalize(records)
    (event,) = conflicts(normalized, "kraken")
    assert (event["seq"], event["connection"]) == (records[-1].seq, "ws")
    detail = json.loads(event["detail"])
    assert detail["trade_id"] == "115202912" and detail["differs"] == [field]
    first = {"log_id": event["log_id"], "seq": records[-2].seq, "price": 734697, "qty": 51042,
             "side": int(Side.SELL)}  # fmt: skip
    assert detail["first"] == first
    assert detail["now"][field] == now
    assert normalized.report.trade_repeats == 1


def test_every_differing_field_is_named_in_order():
    normalized = normalize(kraken(kraken_trade(), kraken_trade(price="1.0", qty="2", side="buy")))
    (event,) = conflicts(normalized, "kraken")
    assert json.loads(event["detail"])["differs"] == ["price", "qty", "side"]


def test_each_disagreeing_repeat_is_compared_with_the_first_observation():
    normalized = normalize(kraken(kraken_trade(), kraken_trade(price="1.0"), kraken_trade()))
    (event,) = conflicts(normalized, "kraken")  # the third matches the first
    assert json.loads(event["detail"])["now"]["price"] == 10


def test_coinbase_trades_are_compared_too():
    records = coinbase(coinbase_trade(0), coinbase_trade(1, side="SELL"))
    (event,) = conflicts(normalize(records), "coinbase")
    assert json.loads(event["detail"])["differs"] == ["side"]


def test_different_trades_are_never_compared():
    normalized = normalize(kraken(kraken_trade(), kraken_trade(trade_id=115202913, price="1.0")))
    assert conflicts(normalized, "kraken") == [] and normalized.report.trade_repeats == 0


def test_two_logs_fed_to_one_normalizer_are_compared_with_each_other():
    first = kraken(kraken_trade())
    second = [RawRecord(r.recv_ts + 10**12, r.seq, r.venue, r.channel, r.payload)
              for r in kraken(kraken_trade(qty="9"))]  # fmt: skip
    normalized = normalize(first + second)
    (event,) = conflicts(normalized, "kraken")
    detail = json.loads(event["detail"])
    assert detail["first"]["log_id"] != event["log_id"]  # seen first in the other log
    assert detail["differs"] == ["qty"]


def test_the_report_counts_conflicts():
    normalized = normalize(kraken(kraken_trade(), kraken_trade(price="1.0")))
    assert "trade conflicts: 1" in normalized.report.summary()
    assert normalized.report.events["TRADE_CONFLICT"] == 1


def test_a_conflict_is_an_error_in_the_quality_report_and_the_store_keeps_one_row(tmp_path):
    normalized = normalize(kraken(kraken_trade(), kraken_trade(price="73469.8")))
    root = store_from_rows(tmp_path / "store", normalized.rows)
    result = run(check_stream_integrity, root)
    (finding,) = [f for f in result.findings if f.what == "trade_conflict"]
    assert finding.severity == Severity.ERROR and finding.dataset == "stream_events"
    assert "115202912" in finding.message and "price" in finding.message
    with PITSession(root, as_of=INT64_MAX) as session:
        assert session.trades("kraken", INSTRUMENT, 0, INT64_MAX).num_rows == 1
