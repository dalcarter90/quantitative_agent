"""A trade seen twice (ADRs 0015, 0016 and 0018).

A venue can send the same trade twice: Coinbase resends some trades in its
live stream, a stream reconnect resends history, the next session resends
the last trades of the previous one, and two logs fed to one normalizer can
overlap. A session shows one row per trade_id: its first observation in
(log_id, seq) order (ADR 0018), across logs and loads.

Within one normalizer:

- An identical repeat (same price, quantity and side) is dropped: the first
  observation is the one stored, and the repeat is a TRADE_REPEAT stream
  event, reported as INFO (ADR 0016).
- A repeat that differs in price, quantity or side is stored as before and is
  a TRADE_CONFLICT stream event, reported as an ERROR (ADR 0015).
"""

import json

import pytest
from _builders import INT64_MAX, trade_table
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
from quantagent.data.quality import QualityConfig, Severity, check_stream_integrity
from quantagent.data.rawlog import RawLogWriter, RawRecord, log_id_of
from quantagent.data.schema import TRADES, Side, StreamEventKind
from quantagent.data.session import PITSession
from quantagent.data.store import AppendOnlyStore

CONFLICT = StreamEventKind.TRADE_CONFLICT
REPEAT = StreamEventKind.TRADE_REPEAT
INSTRUMENT = "BTC-USD"


def kraken_trade(trade_id=115202912, price="73469.7", qty="0.00051042", side="sell",
                 kind="update"):  # fmt: skip
    item = kraken_item(trade_id, price, qty, side)
    return ("ws", f'{{"channel":"trade","type":"{kind}","data":[{item}]}}')


def kraken_item(trade_id=115202912, price="73469.7", qty="0.00051042", side="sell"):
    """Kraken sends prices and quantities as JSON numbers, so they go in unquoted."""
    return (f'{{"symbol":"BTC/USD","side":"{side}","price":{price},"qty":{qty},'
            f'"ord_type":"limit","trade_id":{trade_id},'
            '"timestamp":"2026-11-11T03:06:47.999916Z"}')  # fmt: skip


def kraken_trades(*items):
    """One Kraken message carrying several trades."""
    return ("ws", f'{{"channel":"trade","type":"update","data":[{",".join(items)}]}}')


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


def events(normalized, venue, kind):
    rows = normalized.rows.get(("stream_events", venue, INSTRUMENT), [])
    return [r for r in rows if r["kind"] == kind]


def conflicts(normalized, venue):
    return events(normalized, venue, CONFLICT)


def repeats(normalized, venue):
    return events(normalized, venue, REPEAT)


def trade_rows(normalized, venue):
    return normalized.rows.get(("trades", venue, INSTRUMENT), [])


def write_log(path, records):
    with RawLogWriter(path) as writer:
        for r in records:
            writer.write(r.venue, r.channel, r.payload, recv_ts=r.recv_ts)
    return path


# --- identical repeats: the first observation is kept (ADR 0016) ----------------------


def test_an_identical_repeat_is_dropped_and_the_store_keeps_the_first_observation(tmp_path):
    records = kraken(kraken_trade(), kraken_trade())
    normalized = normalize(records)
    assert conflicts(normalized, "kraken") == []
    assert [r["seq"] for r in trade_rows(normalized, "kraken")] == [records[-2].seq]
    assert normalized.report.rows["trades"] == 1
    assert normalized.report.trade_repeats == 1
    ingest(write_log(tmp_path / "a.qaraw", records), AppendOnlyStore(tmp_path / "store"))
    with PITSession(tmp_path / "store", available_at=INT64_MAX) as session:
        trades = session.trades("kraken", INSTRUMENT, 0, INT64_MAX).to_pylist()
    assert [t["trade_id"] for t in trades] == ["115202912"]
    assert (trades[0]["seq"], trades[0]["recv_ts"]) == (records[-2].seq, records[-2].recv_ts)


def test_an_identical_repeat_is_a_trade_repeat_event_naming_the_first_observation():
    records = kraken(kraken_trade(), kraken_trade())
    normalized = normalize(records)
    (event,) = repeats(normalized, "kraken")
    assert (event["seq"], event["recv_ts"], event["connection"]) == (
        records[-1].seq, records[-1].recv_ts, "ws")  # fmt: skip
    # observed is how long after the first observation the repeat arrived.
    assert event["observed"] == records[-1].recv_ts - records[-2].recv_ts
    assert event["expected"] is None
    detail = json.loads(event["detail"])
    assert detail == {"trade_id": "115202912",
                      "first": {"log_id": event["log_id"], "seq": records[-2].seq}}  # fmt: skip
    assert normalized.report.events["TRADE_REPEAT"] == 1


def test_a_message_of_repeats_only_still_counts_as_data():
    records = kraken(kraken_trade(), kraken_trade())
    normalized = normalize(records)
    assert normalized.report.messages["trade.update"] == 2
    assert normalized.report.outcomes["rejected"] == 0


def test_only_the_repeated_trades_of_a_message_are_dropped():
    records = kraken(kraken_trade(),
                     kraken_trades(kraken_item(trade_id=115202913), kraken_item()))  # fmt: skip
    normalized = normalize(records)
    rows = trade_rows(normalized, "kraken")
    assert [(r["trade_id"], r["seq"]) for r in rows] == [
        ("115202912", records[-2].seq), ("115202913", records[-1].seq)]  # fmt: skip
    (event,) = repeats(normalized, "kraken")
    assert json.loads(event["detail"])["trade_id"] == "115202912"


def test_a_trade_twice_in_one_message_is_kept_once():
    records = kraken(kraken_trades(kraken_item(), kraken_item()))
    normalized = normalize(records)
    assert len(trade_rows(normalized, "kraken")) == 1
    (event,) = repeats(normalized, "kraken")
    assert event["observed"] == 0
    assert json.loads(event["detail"])["first"]["seq"] == records[-1].seq


def test_every_identical_repeat_is_its_own_event():
    records = kraken(kraken_trade(), kraken_trade(), kraken_trade())
    normalized = normalize(records)
    first = records[-3].recv_ts
    # Each delay is measured from the first observation, not the previous repeat.
    assert [e["observed"] for e in repeats(normalized, "kraken")] == [
        records[-2].recv_ts - first, records[-1].recv_ts - first]  # fmt: skip
    assert len(trade_rows(normalized, "kraken")) == 1
    assert normalized.report.trade_repeats == 2


def test_coinbase_identical_repeats_are_dropped_too():
    records = coinbase(coinbase_trade(0), coinbase_trade(1))
    normalized = normalize(records)
    assert [r["seq"] for r in trade_rows(normalized, "coinbase")] == [records[-2].seq]
    assert len(repeats(normalized, "coinbase")) == 1


def test_an_identical_repeat_in_a_second_log_fed_to_the_same_normalizer_is_dropped():
    first = kraken(kraken_trade())
    second = [RawRecord(r.recv_ts + 10**12, r.seq, r.venue, r.channel, r.payload)
              for r in kraken(kraken_trade())]  # fmt: skip
    normalized = normalize(first + second)
    (row,) = trade_rows(normalized, "kraken")
    (event,) = repeats(normalized, "kraken")
    assert row["log_id"] != event["log_id"]  # kept from the first log
    assert json.loads(event["detail"])["first"]["log_id"] == row["log_id"]


# --- conflicting repeats: unchanged (ADR 0015) -----------------------------------------


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
    # A conflicting repeat is not dropped and is not a TRADE_REPEAT.
    assert len(trade_rows(normalized, "kraken")) == 2
    assert repeats(normalized, "kraken") == []


def test_every_differing_field_is_named_in_order():
    normalized = normalize(kraken(kraken_trade(), kraken_trade(price="1.0", qty="2", side="buy")))
    (event,) = conflicts(normalized, "kraken")
    assert json.loads(event["detail"])["differs"] == ["price", "qty", "side"]


def test_each_repeat_is_compared_with_the_first_observation():
    records = kraken(kraken_trade(), kraken_trade(price="1.0"), kraken_trade())
    normalized = normalize(records)
    (event,) = conflicts(normalized, "kraken")
    assert json.loads(event["detail"])["now"]["price"] == 10
    (repeat,) = repeats(normalized, "kraken")  # the third matches the first
    assert repeat["seq"] == records[-1].seq
    assert [r["seq"] for r in trade_rows(normalized, "kraken")] == [r.seq for r in records[-3:-1]]


def test_coinbase_trades_are_compared_too():
    records = coinbase(coinbase_trade(0), coinbase_trade(1, side="SELL"))
    (event,) = conflicts(normalize(records), "coinbase")
    assert json.loads(event["detail"])["differs"] == ["side"]


def test_different_trades_are_never_compared():
    normalized = normalize(kraken(kraken_trade(), kraken_trade(trade_id=115202913, price="1.0")))
    assert conflicts(normalized, "kraken") == [] and repeats(normalized, "kraken") == []
    assert normalized.report.trade_repeats == 0
    assert len(trade_rows(normalized, "kraken")) == 2


def test_two_logs_fed_to_one_normalizer_are_compared_with_each_other():
    first = kraken(kraken_trade())
    second = [RawRecord(r.recv_ts + 10**12, r.seq, r.venue, r.channel, r.payload)
              for r in kraken(kraken_trade(qty="9"))]  # fmt: skip
    normalized = normalize(first + second)
    (event,) = conflicts(normalized, "kraken")
    detail = json.loads(event["detail"])
    assert detail["first"]["log_id"] != event["log_id"]  # seen first in the other log
    assert detail["differs"] == ["qty"]


# --- the report and the quality check ----------------------------------------------------


def test_the_report_counts_conflicts_and_dropped_repeats():
    normalized = normalize(kraken(kraken_trade(), kraken_trade(price="1.0"), kraken_trade()))
    summary = normalized.report.summary()
    assert "trade conflicts: 1" in summary
    assert "identical repeats dropped: 1" in summary
    assert "trades seen again: 2" in summary
    assert normalized.report.events["TRADE_CONFLICT"] == 1


def test_a_conflict_is_an_error_in_the_quality_report_and_the_store_keeps_one_row(tmp_path):
    normalized = normalize(kraken(kraken_trade(), kraken_trade(price="73469.8")))
    root = store_from_rows(tmp_path / "store", normalized.rows)
    result = run(check_stream_integrity, root)
    (finding,) = [f for f in result.findings if f.what == "trade_conflict"]
    assert finding.severity == Severity.ERROR and finding.dataset == "stream_events"
    assert "115202912" in finding.message and "price" in finding.message
    with PITSession(root, available_at=INT64_MAX) as session:
        assert session.trades("kraken", INSTRUMENT, 0, INT64_MAX).num_rows == 1


def test_identical_repeats_are_one_information_finding_per_session(tmp_path):
    records = kraken(kraken_trade(), kraken_trade(), kraken_trade(trade_id=115202913),
                     kraken_trade(trade_id=115202913), kraken_trade())  # fmt: skip
    normalized = normalize(records)
    root = store_from_rows(tmp_path / "store", normalized.rows)
    result = run(check_stream_integrity, root)
    (finding,) = [f for f in result.findings if f.what == "trades_repeated"]
    assert finding.severity == Severity.INFO and finding.dataset == "stream_events"
    first_repeat = repeats(normalized, "kraken")[0]
    assert (finding.log_id, finding.seq) == (first_repeat["log_id"], first_repeat["seq"])
    assert "3 trades" in finding.message and "first observation" in finding.message
    assert result.stats["TRADE_REPEAT"] == 3
    assert [f.what for f in result.findings].count("trades_repeated") == 1


def test_each_session_with_repeats_gets_its_own_finding(tmp_path):
    first = kraken(kraken_trade(), kraken_trade())
    second = [RawRecord(r.recv_ts + 10**12, r.seq, r.venue, r.channel, r.payload)
              for r in kraken(kraken_trade(trade_id=7), kraken_trade(trade_id=7))]  # fmt: skip
    root = store_from_rows(tmp_path / "store", normalize(first + second).rows)
    findings = [f for f in run(check_stream_integrity, root).findings
                if f.what == "trades_repeated"]  # fmt: skip
    assert len(findings) == 2 and len({f.log_id for f in findings}) == 2
    assert all("1 trade " in f.message for f in findings)


def test_two_logs_fed_together_report_a_conflict_and_show_the_first_observation(tmp_path):
    """TRADE_CONFLICT comes from the normalizer, so it needs both observations
    in one normalizer. The session shows the first observation (ADR 0018)."""
    first = kraken(kraken_trade())
    second = [RawRecord(r.recv_ts + 10**12, r.seq, r.venue, r.channel, r.payload)
              for r in kraken(kraken_trade(qty="9"))]  # fmt: skip
    normalized = normalize(first + second)
    assert len(conflicts(normalized, "kraken")) == 1
    root = store_from_rows(tmp_path / "store", normalized.rows)
    with PITSession(root, available_at=INT64_MAX) as session:
        (shown,) = session.trades("kraken", INSTRUMENT, 0, INT64_MAX).to_pylist()
    assert (shown["log_id"], shown["qty"]) == (trade_rows(normalized, "kraken")[0]["log_id"], 51042)


# --- which observation a session shows: the first in replay order (ADR 0018) -------------
#
# Trades are keyed by the raw record they were observed in, like the other
# datasets, and every observation the normalizer keeps is stored. The session
# shows one row per trade_id: its first observation in (log_id, seq) order,
# among the observations inside both cutoffs. Across logs, across loads, and
# whatever the venue's or our clock say.


def later_log(records, hours=1):
    return [RawRecord(r.recv_ts + hours * 3600 * 10**9, r.seq, r.venue, r.channel, r.payload)
            for r in records]  # fmt: skip


def shown(root, available_at=INT64_MAX, version=None):
    with PITSession(root, available_at=available_at, version=version) as session:
        trades = session.trades("kraken", INSTRUMENT, 0, INT64_MAX).to_pylist()
    return [(t["log_id"], t["seq"], t["recv_ts"], t["price"]) for t in trades]


def test_trades_are_keyed_by_raw_record_and_shown_one_per_trade_id():
    assert TRADES.key == ("log_id", "seq", "trade_id")
    assert TRADES.one_row_per == ("trade_id",)


@pytest.mark.parametrize("order", ["in recording order", "later log loaded first"])
def test_a_trade_in_two_logs_loaded_separately_shows_the_earlier_logs_observation(tmp_path, order):
    live = kraken(kraken_trade())
    resent = later_log(kraken(kraken_trade(kind="snapshot")))
    paths = [write_log(tmp_path / "live.qaraw", live), write_log(tmp_path / "resent.qaraw", resent)]
    if order != "in recording order":
        paths.reverse()
    root = tmp_path / "store"
    store = AppendOnlyStore(root)
    for path in paths:
        ingest(path, store)
    ((log_id, seq, recv_ts, _),) = shown(root)
    assert (log_id, seq, recv_ts) == (log_id_of(live[0]), live[3].seq, live[3].recv_ts)


def test_reloading_the_earlier_log_does_not_change_which_observation_is_shown(tmp_path):
    live = write_log(tmp_path / "live.qaraw", kraken(kraken_trade()))
    resent = write_log(tmp_path / "resent.qaraw", later_log(kraken(kraken_trade(kind="snapshot"))))
    root = tmp_path / "store"
    store = AppendOnlyStore(root)
    for path in (live, resent, live, resent):  # the latest load of each log is read
        ingest(path, store)
    ((_, seq, recv_ts, _),) = shown(root)
    first = kraken(kraken_trade())[3]
    assert (seq, recv_ts) == (first.seq, first.recv_ts)


def test_a_conflicting_repeat_in_one_log_shows_the_first_observation(tmp_path):
    records = kraken(kraken_trade(price="100.0"), kraken_trade(price="101.0"))
    root = tmp_path / "store"
    report = ingest(write_log(tmp_path / "a.qaraw", records), AppendOnlyStore(root))
    assert report.events["TRADE_CONFLICT"] == 1  # still an error
    with PITSession(root, available_at=INT64_MAX) as session:
        stored = session.sql("SELECT count(*) AS n FROM trades").column("n").to_pylist()
    assert stored == [1]
    assert [(seq, price) for _, seq, _, price in shown(root)] == [(3, 1000)]


def _trade(log_id, seq, recv_ts, price):
    """One observation of trade "t", received at ``recv_ts`` (the builders' lag is 5 ms)."""
    return trade_table(("t", recv_ts - 5_000_000, price), log_id=log_id, seq=seq)


def test_before_the_first_observation_arrived_a_session_shows_the_one_that_had(tmp_path):
    """Both cutoffs apply before the choice. Two recorders ran at once; the log
    that began later received the trade 30 ms sooner."""
    early_log = "20261111T030000.000000000Z-00000000000a"
    late_log = "20261111T030001.000000000Z-00000000000b"
    t = 1_794_366_407 * 10**9
    root = tmp_path / "store"
    store = AppendOnlyStore(root)
    store._append(TRADES, "kraken", INSTRUMENT, _trade(early_log, 9, t + 30_000_000, 1))
    store._append(TRADES, "kraken", INSTRUMENT, _trade(late_log, 4, t, 1))
    assert shown(root, available_at=t - 1) == []
    assert shown(root, available_at=t + 1) == [(late_log, 4, t, 1)]
    assert shown(root) == [(early_log, 9, t + 30_000_000, 1)]


def test_a_window_holds_a_trade_when_its_shown_observation_was_received_in_it(tmp_path):
    log_a = "20261111T030000.000000000Z-00000000000a"
    log_b = "20261111T040000.000000000Z-00000000000b"
    t = 1_794_366_407 * 10**9
    root = tmp_path / "store"
    store = AppendOnlyStore(root)
    store._append(TRADES, "kraken", INSTRUMENT, _trade(log_a, 3, t, 1))
    store._append(TRADES, "kraken", INSTRUMENT, _trade(log_b, 3, t + 3600 * 10**9, 1))
    with PITSession(root, available_at=INT64_MAX) as session:
        assert session.trades("kraken", INSTRUMENT, t, t + 1).num_rows == 1
        # The resend an hour later is not a trade of that hour.
        assert session.trades("kraken", INSTRUMENT, t + 1, INT64_MAX).num_rows == 0


def test_repeats_never_crowd_out_errors(tmp_path):
    # Many repeats before a conflict: the conflict is still reported, because
    # repeats are summarized per session rather than one finding each.
    records = kraken(*[kraken_trade() for _ in range(5)], kraken_trade(price="1.0"))
    root = store_from_rows(tmp_path / "store", normalize(records).rows)
    result = run(check_stream_integrity, root, config=QualityConfig(max_findings=2))
    assert [f.what for f in result.findings] == ["trade_conflict", "trades_repeated"]
    assert not result.truncated
