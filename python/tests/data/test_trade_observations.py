"""Trades seen in more than one log, loaded in separate runs (ADR 0019).

Since ADR 0018 every observation of a trade is stored and a session shows the
first, in ``(log_id, seq)`` order. The normalizer compares observations only
within one run (``TRADE_CONFLICT``, ``TRADE_REPEAT``), and ``ingest`` loads
one log per run, so two logs that disagree about a trade went unreported. The
session now offers every observation with its trade's first
(``PITSession.trade_observations``), and the ``trade_observations`` quality
check reports an observation that disagrees with the first from another log.
"""

import pytest
from _builders import INT64_MAX
from test_adapters import KRAKEN_BOOK_ACK, KRAKEN_INSTRUMENT, KRAKEN_START, log
from test_fixtures import INSTRUMENT

from quantagent.data import quality
from quantagent.data.adapters import ingest
from quantagent.data.quality import QualityConfig, Severity, check_trade_observations
from quantagent.data.rawlog import RawLogWriter, RawRecord, log_id_of
from quantagent.data.session import PITSession, QueryRejected
from quantagent.data.store import AppendOnlyStore

NS = 1_000_000_000
HOUR = 3600 * NS
ALL = (0, INT64_MAX)


def trade(trade_id, price="73469.7", qty="0.50000000", side="sell", kind="update",
          timestamp="2026-11-11T03:06:47.999916Z"):  # fmt: skip
    return ("ws", f'{{"channel":"trade","type":"{kind}","data":[{{"symbol":"BTC/USD",'
                  f'"side":"{side}","price":{price},"qty":{qty},"ord_type":"limit",'
                  f'"trade_id":{trade_id},"timestamp":"{timestamp}"}}]}}')  # fmt: skip


def session_log(*messages, later_by=0):
    """A Kraken session; its records are received ``later_by`` ns after the fixture's."""
    records = log("kraken", KRAKEN_START, KRAKEN_INSTRUMENT, KRAKEN_BOOK_ACK, *messages)
    return [RawRecord(r.recv_ts + later_by, r.seq, r.venue, r.channel, r.payload) for r in records]


def write(path, records):
    with RawLogWriter(path) as writer:
        for r in records:
            writer.write(r.venue, r.channel, r.payload, recv_ts=r.recv_ts,
                         recv_mono_ns=r.recv_mono_ns)  # fmt: skip
    return path


def store_of(tmp_path, *logs):
    """Each log loaded in its own run, in the order given."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    root = tmp_path / "store"
    store = AppendOnlyStore(root)
    for number, records in enumerate(logs):
        ingest(write(tmp_path / f"log-{number}.qaraw", records), store)
    return root


def check(root, window=ALL, available_at=INT64_MAX, config=None):
    with PITSession(root, available_at=available_at) as session:
        return check_trade_observations(session, "kraken", INSTRUMENT, *window,
                                        config or QualityConfig())  # fmt: skip


FIRST = session_log(trade(7, price="100.0"), trade(8, price="200.0"))
# The next session, an hour later, resends both as history: one changed.
RESENT = session_log(trade(7, price="101.0", kind="snapshot"),
                     trade(8, price="200.0", kind="snapshot"), later_by=HOUR)  # fmt: skip


def test_a_trade_that_a_later_log_saw_differently_is_an_error(tmp_path):
    root = store_of(tmp_path, FIRST, RESENT)
    result = check(root)
    (finding,) = result.findings
    assert (finding.check, finding.what, finding.severity) == (
        "trade_observations", "trade_conflict_across_logs", Severity.ERROR)  # fmt: skip
    # It points at the later observation, like TRADE_CONFLICT, and names the first.
    assert (finding.dataset, finding.log_id, finding.seq) == ("trades", log_id_of(RESENT[0]), 3)
    assert finding.recv_ts == RESENT[3].recv_ts
    assert "trade 7" in finding.message and "price" in finding.message
    assert log_id_of(FIRST[0]) in finding.message
    assert result.stats == {"observations": 4, "trades_seen_in_several_logs": 2,
                            "repeated_across_logs": 1, "conflicts_across_logs": 1}  # fmt: skip
    # The session still shows each trade once, as its first observation.
    with PITSession(root, available_at=INT64_MAX) as session:
        shown = session.trades("kraken", INSTRUMENT, *ALL).to_pylist()
    assert sorted((t["trade_id"], t["price"]) for t in shown) == [
        ("7", 10_000_000_000), ("8", 20_000_000_000)]  # fmt: skip


@pytest.mark.parametrize(
    ("field", "first", "later"),
    [
        ("qty", trade(7, qty="0.50000000"), trade(7, qty="0.60000000", kind="snapshot")),
        ("side", trade(7, side="sell"), trade(7, side="buy", kind="snapshot")),
    ],
)
def test_each_field_that_must_agree_is_compared(tmp_path, field, first, later):
    root = store_of(tmp_path, session_log(first), session_log(later, later_by=HOUR))
    (finding,) = check(root).findings
    assert finding.what == "trade_conflict_across_logs" and field in finding.message


def test_identical_observations_in_several_logs_are_counted_not_reported(tmp_path):
    root = store_of(tmp_path, FIRST, session_log(trade(8, price="200.0", kind="snapshot"),
                                                 later_by=HOUR))  # fmt: skip
    result = check(root)
    assert result.findings == []
    assert result.stats["repeated_across_logs"] == 1


def test_the_order_the_logs_were_loaded_in_does_not_matter(tmp_path):
    forward = check(store_of(tmp_path / "a", FIRST, RESENT))
    backward = check(store_of(tmp_path / "b", RESENT, FIRST))
    assert [(f.what, f.log_id, f.seq) for f in backward.findings] == [
        (f.what, f.log_id, f.seq) for f in forward.findings
    ]
    assert backward.stats == forward.stats


def test_the_conflict_is_reported_where_the_later_observation_was_received(tmp_path):
    root = store_of(tmp_path, FIRST, RESENT)
    later = RESENT[3].recv_ts
    assert len(check(root, window=(later, later + 1)).findings) == 1
    assert check(root, window=(0, later)).findings == []  # the first session's range
    assert check(root, window=(later + 1, INT64_MAX)).findings == []


def test_a_session_before_the_later_observation_arrived_sees_no_conflict(tmp_path):
    root = store_of(tmp_path, FIRST, RESENT)
    later = RESENT[3].recv_ts
    assert check(root, available_at=later - 1).findings == []
    assert len(check(root, available_at=later).findings) == 1


def test_a_conflict_within_one_log_is_left_to_its_trade_conflict_event(tmp_path):
    root = store_of(tmp_path, session_log(trade(7, price="100.0"), trade(7, price="101.0")))
    with PITSession(root, available_at=INT64_MAX) as session:
        integrity = quality.check_stream_integrity(session, "kraken", INSTRUMENT, *ALL,
                                                   QualityConfig())  # fmt: skip
    assert [f.what for f in integrity.findings] == ["trade_conflict"]
    result = check(root)
    assert result.findings == []
    assert result.stats["conflicts_across_logs"] == 0


def test_a_reload_is_not_a_second_log(tmp_path):
    root = store_of(tmp_path, FIRST, FIRST)
    result = check(root)
    assert result.findings == [] and result.stats["trades_seen_in_several_logs"] == 0


def test_the_session_offers_every_observation_with_its_trades_first(tmp_path):
    root = store_of(tmp_path, FIRST, RESENT)
    first, second = log_id_of(FIRST[0]), log_id_of(RESENT[0])
    with PITSession(root, available_at=INT64_MAX) as session:
        rows = session.trade_observations("kraken", INSTRUMENT, *ALL).to_pylist()
        later_only = session.trade_observations("kraken", INSTRUMENT, RESENT[0].recv_ts,
                                                INT64_MAX).to_pylist()  # fmt: skip
        with pytest.raises(QueryRejected):
            session.sql("SELECT * FROM trade_observations")
    summary = [(r["log_id"], r["seq"], r["trade_id"], r["price"], r["first_log_id"],
                r["first_seq"], r["first_price"]) for r in rows]  # fmt: skip
    assert summary == [
        (first, 3, "7", 10_000_000_000, first, 3, 10_000_000_000),
        (first, 4, "8", 20_000_000_000, first, 4, 20_000_000_000),
        (second, 3, "7", 10_100_000_000, first, 3, 10_000_000_000),
        (second, 4, "8", 20_000_000_000, first, 4, 20_000_000_000),
    ]
    assert {r["first_recv_ts"] for r in rows if r["trade_id"] == "7"} == {FIRST[3].recv_ts}
    # A window selects observations by their own receipt time; the first
    # observation is found wherever it is.
    assert [(r["log_id"], r["first_log_id"]) for r in later_only] == [(second, first)] * 2


def test_the_report_runs_the_check_and_fails_on_a_conflict(tmp_path, capsys):
    root = store_of(tmp_path, FIRST, RESENT)
    status = quality.main(["--store", str(root), "--start", "2026-11-11", "--end", "2026-11-11"])
    out = capsys.readouterr().out
    assert status == 1
    assert "trade_observations: 1 finding: 1 ERROR" in out
    assert "trade_conflict_across_logs" in out
