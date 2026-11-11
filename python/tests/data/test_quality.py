"""Data quality checks (roadmap Phase 1d).

Each check is run on the clean recorded fixtures, where it must find nothing,
and on a copy with one defect planted, where it must find exactly that.
Defects are planted either in the raw log (so the adapter and the stream
events it writes are part of the test) or in the normalized rows.
"""

import hashlib
import json
from pathlib import Path

import pytest
from _builders import INT64_MAX
from test_fixtures import INSTRUMENT, fixture_path
from test_stream_events import copy_log

import quantagent.data.quality as quality
from quantagent.data.adapters import ingest, normalize
from quantagent.data.quality import (
    CheckResult,
    Finding,
    QualityConfig,
    Severity,
    check_clock_skew,
    check_crossed_books,
    check_outliers,
    check_stream_integrity,
    run_checks,
)
from quantagent.data.rawlog import RawLogWriter, read_raw_log
from quantagent.data.schema import (
    DATASETS,
    BookEventKind,
    Side,
    StreamEventKind,
    table_from_rows,
)
from quantagent.data.session import PITSession
from quantagent.data.store import AppendOnlyStore

NS = 1_000_000_000
ALL = (0, INT64_MAX)
CHECKS = [check_stream_integrity, check_clock_skew, check_crossed_books, check_outliers]
TRADES = ("trades", "kraken", INSTRUMENT)
BOOK = ("book_events", "kraken", INSTRUMENT)
EVENTS = ("stream_events", "kraken", INSTRUMENT)
_ORDER = {"instruments": 0, "trades": 1, "book_events": 2, "stream_events": 3}


# --- building stores -------------------------------------------------------------------


@pytest.fixture(scope="module")
def clean_store(tmp_path_factory):
    root = tmp_path_factory.mktemp("clean") / "store"
    store = AppendOnlyStore(root)
    for venue in ("kraken", "coinbase"):
        ingest(fixture_path(venue), store)
    return root


@pytest.fixture(scope="module")
def kraken_records():
    return list(read_raw_log(fixture_path("kraken")))


@pytest.fixture(scope="module")
def kraken_rows(kraken_records):
    return normalize(kraken_records).rows


def store_from_rows(root: Path, rows_by_key) -> Path:
    store = AppendOnlyStore(root)
    for key in sorted(rows_by_key, key=lambda k: (_ORDER[k[0]], k)):
        dataset = DATASETS[key[0]]
        store.append(dataset, key[1], key[2], table_from_rows(dataset, rows_by_key[key]))
    return root


def planted(tmp_path, base_rows, edit) -> Path:
    """A store built from the Kraken fixture rows after ``edit`` changed them.

    ``edit`` receives a dict of fresh lists; it must replace rows, not mutate them.
    """
    rows = {key: list(value) for key, value in base_rows.items()}
    edit(rows)
    return store_from_rows(tmp_path / "store", rows)


def run(check, root, venue="kraken", config=None, window=ALL) -> CheckResult:
    with PITSession(root, as_of=INT64_MAX) as session:
        return check(session, venue, INSTRUMENT, *window, config or QualityConfig())


def brief(result):
    return [(f.what, f.severity, f.dataset, f.seq) for f in result.findings]


def backfill_seqs(rows):
    return {r["seq"] for r in rows[EVENTS] if r["kind"] == StreamEventKind.TRADE_BACKFILL}


def live_trades(rows):
    skip = backfill_seqs(rows)
    return [r for r in rows[TRADES] if r["seq"] not in skip]


def messages(book_rows, kind):
    found = {}
    for row in book_rows:
        if row["kind"] == kind:
            found.setdefault(row["seq"], []).append(row)
    return found


def replace_message(rows, key, seq, change):
    rows[key] = [change(r) if r["seq"] == seq else r for r in rows[key]]


# --- clean data ------------------------------------------------------------------------


@pytest.mark.parametrize("venue", ["kraken", "coinbase"])
@pytest.mark.parametrize("check", CHECKS, ids=lambda c: c.__name__)
def test_every_check_finds_nothing_on_the_clean_fixtures(clean_store, venue, check):
    result = run(check, clean_store, venue)
    assert isinstance(result, CheckResult)
    assert result.findings == []
    assert result.truncated is False
    assert result.check == check.__name__.removeprefix("check_")
    assert result.stats  # every check says how much it looked at


def test_clean_fixture_stats_show_the_checks_actually_looked(clean_store):
    integrity = run(check_stream_integrity, clean_store, "coinbase").stats
    assert integrity["events"] > 10 and integrity["sessions"] == 1

    skew = run(check_clock_skew, clean_store, "kraken").stats
    assert skew["rows_considered"] > 18_000
    assert skew["backfill_trades_skipped"] == 50  # Kraken sends the last 50 trades
    assert 0 < skew["min_lag_ns"] <= skew["median_lag_ns"] <= skew["max_lag_ns"] < 10 * NS

    crossed = run(check_crossed_books, clean_store, "coinbase").stats
    assert crossed["snapshots_checked"] == 2  # the stream's own and the independent one

    outliers = run(check_outliers, clean_store, "coinbase").stats
    assert outliers["trades_checked"] == 941
    assert outliers["book_rows_checked"] > 100_000
    assert outliers["trades_with_reference"] > 800


def test_run_checks_covers_every_venue_and_instrument_in_the_store(clean_store):
    with PITSession(clean_store, as_of=INT64_MAX) as session:
        results = run_checks(session, *ALL)
    assert sorted({(r.venue, r.instrument) for r in results}) == [
        ("coinbase", INSTRUMENT),
        ("kraken", INSTRUMENT),
    ]
    assert len(results) == 2 * len(CHECKS)
    assert [f for r in results for f in r.findings] == []
    with PITSession(clean_store, as_of=INT64_MAX) as session:
        only = run_checks(session, *ALL, venue="kraken")
        assert {r.venue for r in only} == {"kraken"}
        assert run_checks(session, 0, 1) == []  # nothing that early


def test_findings_are_structured():
    finding = Finding(
        check="outliers",
        severity=Severity.ERROR,
        what="nonpositive_price",
        venue="kraken",
        instrument=INSTRUMENT,
        dataset="trades",
        recv_ts=5,
        seq=7,
        message="price 0",
    )
    assert finding.where == "kraken/BTC-USD trades record (recv_ts 5, seq 7)"
    assert Severity.INFO < Severity.WARNING < Severity.ERROR
    with pytest.raises(AttributeError):
        finding.severity = Severity.INFO


# --- stream integrity ------------------------------------------------------------------


def test_a_lost_coinbase_message_is_an_error(tmp_path):
    records = list(read_raw_log(fixture_path("coinbase")))
    stream = [r for r in records if r.channel == "ws"]
    lost = stream[700]
    number = json.loads(lost.payload)["sequence_num"]
    log = copy_log(records, tmp_path / "gap.qaraw", drop={lost.seq})
    ingest(log, AppendOnlyStore(tmp_path / "store"))

    result = run(check_stream_integrity, tmp_path / "store", "coinbase")
    (finding,) = result.findings
    assert (finding.what, finding.severity, finding.dataset) == (
        "sequence_gap",
        Severity.ERROR,
        "stream_events",
    )
    assert finding.recv_ts == stream[701].recv_ts
    assert f"expected {number}" in finding.message and f"received {number + 1}" in finding.message
    assert "1 message" in finding.message
    assert result.stats["SEQUENCE_GAP"] == 1


def test_a_kraken_checksum_mismatch_is_an_error(tmp_path, kraken_records):
    updates = [r for r in kraken_records if r.channel == "ws" and b'"update"' in r.payload
               and b'"channel":"book"' in r.payload]  # fmt: skip
    target = updates[400]
    real = json.loads(target.payload)["data"][0]["checksum"]
    altered = target.payload.replace(
        f'"checksum":{real}'.encode(), f'"checksum":{real ^ 5}'.encode()
    )
    log = copy_log(kraken_records, tmp_path / "bad.qaraw", replace={target.seq: altered})
    ingest(log, AppendOnlyStore(tmp_path / "store"))

    (finding,) = run(check_stream_integrity, tmp_path / "store").findings
    assert (finding.what, finding.severity) == ("checksum_mismatch", Severity.ERROR)
    assert finding.recv_ts == target.recv_ts
    assert str(real ^ 5) in finding.message and str(real) in finding.message


def test_a_message_that_could_not_be_normalized_is_an_error(tmp_path, kraken_records):
    log = tmp_path / "bad.qaraw"
    with RawLogWriter(log) as writer:
        for record in kraken_records[:100]:
            writer.write(record.venue, record.channel, record.payload, recv_ts=record.recv_ts)
        bad = writer.write("kraken", "ws", b'{"channel":"book","type":"update","data":7}')
    ingest(log, AppendOnlyStore(tmp_path / "store"))

    (finding,) = run(check_stream_integrity, tmp_path / "store").findings
    assert (finding.what, finding.severity, finding.seq) == (
        "message_rejected",
        Severity.ERROR,
        bad.seq,
    )
    assert "malformed_message" in finding.message


def event(template, seq, kind, connection, detail=None, expected=None, observed=None):
    return {
        **template,
        "recv_ts": template["recv_ts"] + seq,
        "event_ts": template["recv_ts"] + seq,
        "seq": 900_000 + seq,
        "event_index": 0,
        "kind": kind,
        "connection": connection,
        "expected": expected,
        "observed": observed,
        "detail": detail,
    }


def test_a_dropped_and_reopened_stream_connection_is_a_warning(tmp_path, kraken_rows):
    K = StreamEventKind

    def edit(rows):
        last = rows[EVENTS][-2]  # just before the session stop
        rows[EVENTS] = [
            *rows[EVENTS][:-1],
            event(last, 1, K.DISCONNECT, "ws", "ConnectionClosedError: no close frame"),
            event(last, 2, K.CONNECT, "ws", "wss://ws.kraken.com/v2"),
            rows[EVENTS][-1],
        ]

    result = run(check_stream_integrity, planted(tmp_path, kraken_rows, edit))
    assert brief(result) == [
        ("disconnect", Severity.WARNING, "stream_events", 900_001),
        ("reconnect", Severity.WARNING, "stream_events", 900_002),
    ]
    assert "no close frame" in result.findings[0].message
    # The fixture's own probe connection and normal shutdown are not findings.
    assert result.stats["CONNECT"] == 3 and result.stats["DISCONNECT"] == 3


def test_repeats_out_of_order_and_failed_probes_are_graded(tmp_path, kraken_rows):
    K = StreamEventKind

    def edit(rows):
        last = rows[EVENTS][-2]
        rows[EVENTS] = [
            *rows[EVENTS][:-1],
            event(last, 1, K.SEQUENCE_REPEAT, "ws", expected=8, observed=7),
            event(last, 2, K.SEQUENCE_OUT_OF_ORDER, "ws", expected=8, observed=5),
            event(last, 3, K.DISCONNECT, "probe", "TimeoutError: no snapshot"),
            event(last, 4, K.SEQUENCE_GAP, "probe", expected=2, observed=9),
            rows[EVENTS][-1],
        ]

    result = run(check_stream_integrity, planted(tmp_path, kraken_rows, edit))
    assert brief(result) == [
        ("sequence_repeat", Severity.WARNING, "stream_events", 900_001),
        ("sequence_out_of_order", Severity.WARNING, "stream_events", 900_002),
        ("probe_failed", Severity.INFO, "stream_events", 900_003),
        ("sequence_gap", Severity.ERROR, "stream_events", 900_004),
    ]
    assert "7 message" in result.findings[3].message


# --- clock skew -------------------------------------------------------------------------


def test_a_message_received_long_after_its_venue_time_is_flagged(tmp_path, kraken_rows):
    victim = live_trades(kraken_rows)[40]

    def edit(rows):
        replace_message(
            rows, TRADES, victim["seq"], lambda r: {**r, "recv_ts": r["event_ts"] + 25 * NS}
        )

    result = run(check_clock_skew, planted(tmp_path, kraken_rows, edit))
    # Arriving 25 seconds late also puts it after messages with later venue times.
    assert brief(result) == [
        ("late", Severity.WARNING, "trades", victim["seq"]),
        ("event_ts_backwards", Severity.WARNING, "trades", victim["seq"]),
    ]
    finding = result.findings[0]
    assert finding.recv_ts == victim["event_ts"] + 25 * NS
    assert "25.000 s" in finding.message
    assert result.stats["max_lag_ns"] == 25 * NS


def test_a_venue_timestamp_later_than_receipt_is_flagged(tmp_path, kraken_rows):
    seq = sorted(messages(kraken_rows[BOOK], BookEventKind.DELTA))[500]

    def edit(rows):
        replace_message(rows, BOOK, seq, lambda r: {**r, "event_ts": r["recv_ts"] + 2 * NS})

    result = run(check_clock_skew, planted(tmp_path, kraken_rows, edit))
    whats = [f.what for f in result.findings]
    assert whats[0] == "early"
    first = result.findings[0]
    assert (first.severity, first.dataset, first.seq) == (Severity.WARNING, "book_events", seq)
    assert "-2.000 s" in first.message and "later than receipt" in first.message
    # The message after it now looks as if venue time ran backwards.
    assert whats[1:] == ["event_ts_backwards"]
    assert result.stats["min_lag_ns"] == -2 * NS


def test_venue_time_running_backwards_within_a_stream_is_flagged(tmp_path, kraken_rows):
    deltas = messages(kraken_rows[BOOK], BookEventKind.DELTA)
    ordered = sorted(deltas)
    seq, previous = ordered[800], ordered[799]
    step_back = deltas[previous][0]["event_ts"] - (deltas[seq][0]["event_ts"] - NS)
    assert 0 < step_back <= NS

    def edit(rows):
        # One second earlier: still inside the lag band, but out of order.
        replace_message(rows, BOOK, seq, lambda r: {**r, "event_ts": r["event_ts"] - NS})

    result = run(check_clock_skew, planted(tmp_path, kraken_rows, edit))
    (finding,) = result.findings
    assert (finding.what, finding.severity, finding.dataset, finding.seq) == (
        "event_ts_backwards",
        Severity.WARNING,
        "book_events",
        seq,
    )
    assert f"{step_back / NS:.3f} s before the previous message's" in finding.message


def test_receipt_rows_are_not_flagged_and_do_not_count_as_zero_skew(tmp_path, kraken_rows):
    clean = run(check_clock_skew, store_from_rows(tmp_path / "clean", kraken_rows)).stats
    seqs = sorted(messages(kraken_rows[BOOK], BookEventKind.DELTA))[100:160]
    converted = sum(1 for r in kraken_rows[BOOK] if r["seq"] in seqs)

    def edit(rows):
        # As if these messages had carried no venue timestamp (ADR 0008).
        rows[BOOK] = [
            {**r, "event_ts": r["recv_ts"], "event_ts_source": "receipt"} if r["seq"] in seqs else r
            for r in rows[BOOK]
        ]

    result = run(check_clock_skew, planted(tmp_path, kraken_rows, edit))
    assert result.findings == []
    assert result.stats["receipt_rows_skipped"] == converted > 0
    assert result.stats["rows_considered"] == clean["rows_considered"] - converted
    # Had they been counted, the minimum lag would have become zero.
    assert result.stats["min_lag_ns"] == clean["min_lag_ns"] > 0
    assert clean["receipt_rows_skipped"] == 0


def test_backfilled_trades_are_excluded_because_their_receipt_time_means_nothing(
    tmp_path, kraken_rows
):
    def edit(rows):
        rows[EVENTS] = [r for r in rows[EVENTS] if r["kind"] != StreamEventKind.TRADE_BACKFILL]

    result = run(check_clock_skew, planted(tmp_path, kraken_rows, edit))
    # Without the marker, the history Kraken sent on subscribe looks 36 seconds late.
    assert {f.what for f in result.findings} == {"late"}
    assert {f.dataset for f in result.findings} == {"trades"}
    assert result.stats["backfill_trades_skipped"] == 0


def test_the_lag_band_is_configurable(clean_store):
    # The fixture was recorded on a machine whose clock ran about 1.4 s ahead,
    # so a 1.5 s ceiling catches its slower messages (docs/data-core.md, section 7).
    tight = QualityConfig(max_lag_ns=1_500_000_000)
    result = run(check_clock_skew, clean_store, config=tight)
    assert result.findings and {f.what for f in result.findings} == {"late"}
    shifted = QualityConfig(min_lag_ns=2 * NS, max_lag_ns=60 * NS)
    assert {f.what for f in run(check_clock_skew, clean_store, config=shifted).findings} == {
        "early"
    }


# --- crossed books ----------------------------------------------------------------------


def best(rows, seq):
    levels = [r for r in rows[BOOK] if r["seq"] == seq]
    bid = max(r["price"] for r in levels if r["side"] == Side.BUY)
    ask = min(r["price"] for r in levels if r["side"] == Side.SELL)
    return bid, ask


@pytest.mark.parametrize(
    ("kind", "offset", "what"),
    [
        (BookEventKind.CHECK_SNAPSHOT, 3, "crossed"),
        (BookEventKind.SNAPSHOT, 0, "locked"),
        (BookEventKind.SNAPSHOT, 1, "crossed"),
    ],
)
def test_a_snapshot_with_bid_at_or_above_ask_is_an_error(tmp_path, kraken_rows, kind, offset, what):
    (seq,) = messages(kraken_rows[BOOK], kind)
    bid, ask = best(kraken_rows, seq)
    assert bid < ask

    def edit(rows):
        replace_message(
            rows,
            BOOK,
            seq,
            lambda r: (
                {**r, "price": ask + offset} if (r["side"], r["price"]) == (Side.BUY, bid) else r
            ),
        )

    result = run(check_crossed_books, planted(tmp_path, kraken_rows, edit))
    (finding,) = result.findings
    assert (finding.what, finding.severity, finding.dataset, finding.seq) == (
        what,
        Severity.ERROR,
        "book_events",
        seq,
    )
    assert str(ask + offset) in finding.message and str(ask) in finding.message
    assert result.stats["snapshots_checked"] == 2


def test_a_one_sided_snapshot_is_a_warning(tmp_path, kraken_rows):
    (seq,) = messages(kraken_rows[BOOK], BookEventKind.CHECK_SNAPSHOT)

    def edit(rows):
        rows[BOOK] = [r for r in rows[BOOK] if not (r["seq"] == seq and r["side"] == Side.SELL)]

    result = run(check_crossed_books, planted(tmp_path, kraken_rows, edit))
    assert brief(result) == [("one_sided", Severity.WARNING, "book_events", seq)]


def test_crossed_check_looks_at_snapshots_only(tmp_path, kraken_rows):
    # A delta alone cannot be judged: that needs the rebuilt book (Phase 2).
    seq = sorted(messages(kraken_rows[BOOK], BookEventKind.DELTA))[10]

    def edit(rows):
        replace_message(rows, BOOK, seq, lambda r: {**r, "side": Side.BUY, "price": 10**9})
        rows[BOOK].append({**rows[BOOK][-1], "level_index": 99, "side": Side.SELL, "price": 5})

    assert run(check_crossed_books, planted(tmp_path, kraken_rows, edit)).findings == []


# --- outliers ---------------------------------------------------------------------------


def test_a_trade_far_from_recent_prices_is_flagged(tmp_path, kraken_rows):
    victim = live_trades(kraken_rows)[100]

    def edit(rows):
        rows[TRADES] = [
            {**r, "price": r["price"] * 2} if r["trade_id"] == victim["trade_id"] else r
            for r in rows[TRADES]
        ]

    result = run(check_outliers, planted(tmp_path, kraken_rows, edit))
    (finding,) = result.findings
    assert (finding.what, finding.severity, finding.dataset, finding.seq) == (
        "price_outlier",
        Severity.WARNING,
        "trades",
        victim["seq"],
    )
    assert victim["trade_id"] in finding.message
    assert str(victim["price"] * 2) in finding.message


@pytest.mark.parametrize(
    ("column", "value", "what"),
    [
        ("price", 0, "nonpositive_price"),
        ("price", -5, "nonpositive_price"),
        ("qty", 0, "nonpositive_quantity"),
        ("qty", -1, "nonpositive_quantity"),
    ],
)
def test_zero_or_negative_trade_values_are_errors(tmp_path, kraken_rows, column, value, what):
    victim = live_trades(kraken_rows)[60]

    def edit(rows):
        rows[TRADES] = [
            {**r, column: value} if r["trade_id"] == victim["trade_id"] else r for r in rows[TRADES]
        ]

    result = run(check_outliers, planted(tmp_path, kraken_rows, edit))
    assert brief(result) == [(what, Severity.ERROR, "trades", victim["seq"])]
    # A bad price is not used as a reference for the trades after it.
    assert result.stats["trades_checked"] == len(kraken_rows[TRADES])


def test_bad_book_levels_are_errors(tmp_path, kraken_rows):
    deltas = sorted(messages(kraken_rows[BOOK], BookEventKind.DELTA))
    (snapshot,) = messages(kraken_rows[BOOK], BookEventKind.SNAPSHOT)

    changes = {deltas[20]: {"price": 0}, deltas[30]: {"qty": -7}, snapshot: {"qty": 0}}

    def edit(rows):
        # Change the first level of each of the three messages.
        rows[BOOK] = [
            {**r, **changes[r["seq"]]} if r["seq"] in changes and r["level_index"] == 0 else r
            for r in rows[BOOK]
        ]

    result = run(check_outliers, planted(tmp_path, kraken_rows, edit))
    assert sorted(brief(result)) == sorted(
        [
            ("zero_quantity_in_snapshot", Severity.WARNING, "book_events", snapshot),
            ("nonpositive_price", Severity.ERROR, "book_events", deltas[20]),
            ("negative_quantity", Severity.ERROR, "book_events", deltas[30]),
        ]
    )


def synthetic_trades(tmp_path, prices):
    rows = [
        {
            "event_ts": 1_000 * NS + i * NS,
            "event_ts_source": "venue",
            "recv_ts": 1_000 * NS + i * NS + 1,
            "seq": i,
            "trade_id": f"t{i:04d}",
            "price": price,
            "qty": 1,
            "side": Side.BUY,
        }
        for i, price in enumerate(prices)
    ]
    return store_from_rows(tmp_path / "store", {TRADES: rows})


def test_outlier_reference_is_the_median_of_the_previous_trades(tmp_path):
    # Thirty trades at 10000, one at 10600 (6% away), then back.
    prices = [10_000] * 30 + [10_600] + [10_000] * 5
    result = run(check_outliers, synthetic_trades(tmp_path, prices))
    assert brief(result) == [("price_outlier", Severity.WARNING, "trades", 30)]
    assert "10600" in result.findings[0].message and "10000" in result.findings[0].message
    assert "600" in result.findings[0].message  # basis points away


def test_outlier_threshold_is_exclusive_and_configurable(tmp_path):
    prices = [10_000] * 30 + [10_500, 10_501]
    root = synthetic_trades(tmp_path, prices)
    assert [f.seq for f in run(check_outliers, root).findings] == [31]  # 500 bps is allowed
    loose = QualityConfig(outlier_bps=600)
    assert run(check_outliers, root, config=loose).findings == []
    tight = QualityConfig(outlier_bps=100)
    assert [f.seq for f in run(check_outliers, root, config=tight).findings] == [30, 31]


def test_the_first_trades_have_no_reference_and_are_not_judged(tmp_path):
    prices = [10_000, 20_000, 10_000, 30_000] + [10_000] * 30
    result = run(check_outliers, synthetic_trades(tmp_path, prices))
    assert result.findings == []
    assert result.stats["trades_with_reference"] == len(prices) - 20


def test_a_lasting_price_move_stops_being_flagged_once_it_is_the_norm(tmp_path):
    prices = [10_000] * 60 + [12_000] * 60
    flagged = [f.seq for f in run(check_outliers, synthetic_trades(tmp_path, prices)).findings]
    # Flagged while the old level is still at least half of the 50-trade window.
    assert flagged == list(range(60, 86))


# --- shared behavior ----------------------------------------------------------------------


def test_findings_are_limited_to_the_requested_time_range(tmp_path, kraken_rows):
    victim = live_trades(kraken_rows)[60]

    def edit(rows):
        rows[TRADES] = [
            {**r, "qty": 0} if r["trade_id"] == victim["trade_id"] else r for r in rows[TRADES]
        ]

    root = planted(tmp_path, kraken_rows, edit)
    at = victim["event_ts"]
    assert len(run(check_outliers, root, window=(at, at + 1)).findings) == 1
    assert run(check_outliers, root, window=(at + 1, INT64_MAX)).findings == []
    assert run(check_outliers, root, window=(0, at)).findings == []


def test_findings_are_capped_and_the_result_says_so(clean_store):
    config = QualityConfig(outlier_bps=0, max_findings=7)
    result = run(check_outliers, clean_store, "coinbase", config=config)
    assert len(result.findings) == 7
    assert result.truncated is True


def test_checks_only_see_what_was_known_at_as_of(tmp_path, kraken_rows):
    def edit(rows):
        victim = live_trades(rows)[60]
        rows[TRADES] = [
            {**r, "qty": 0} if r["trade_id"] == victim["trade_id"] else r for r in rows[TRADES]
        ]

    root = planted(tmp_path, kraken_rows, edit)
    first_file = min(int(p.name[5:25]) for p in root.rglob("*.parquet"))
    with PITSession(root, as_of=first_file - 1) as before:
        assert check_outliers(before, "kraken", INSTRUMENT, *ALL, QualityConfig()).findings == []
    with PITSession(root, as_of=INT64_MAX) as after:
        assert len(check_outliers(after, "kraken", INSTRUMENT, *ALL, QualityConfig()).findings) == 1


def test_running_every_check_changes_nothing_in_the_store(tmp_path, kraken_rows):
    def edit(rows):
        victim = live_trades(rows)[60]
        rows[TRADES] = [
            {**r, "price": -1} if r["trade_id"] == victim["trade_id"] else r for r in rows[TRADES]
        ]

    root = planted(tmp_path, kraken_rows, edit)

    def digests():
        return {
            p.relative_to(root).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in sorted(root.rglob("*"))
            if p.is_file()
        }

    before = digests()
    with PITSession(root, as_of=INT64_MAX) as session:
        results = run_checks(session, *ALL)
        assert sum(len(r.findings) for r in results) == 1
        # The flagged row is still there, exactly as stored.
        assert session.sql("SELECT count(*) AS n FROM trades WHERE price = -1").to_pylist() == [
            {"n": 1}
        ]
    assert quality.main(["--store", str(root), "--start", "2026-11-11", "--end", "2026-11-11"]) == 1
    assert digests() == before


def test_the_quality_module_reads_only_through_pitsession():
    source = Path(quality.__file__).read_text(encoding="utf-8")
    for forbidden in ("duckdb", "pyarrow.parquet", "rawlog", "read_raw_log", "AppendOnlyStore",
                      "open(", ".parquet", "glob("):  # fmt: skip
        assert forbidden not in source, forbidden
    assert "PITSession" in source


@pytest.mark.parametrize("name", ["venue", "instrument"])
def test_names_are_validated_before_they_reach_sql(clean_store, name):
    arguments = {"venue": "kraken", "instrument": INSTRUMENT, name: "x' OR '1'='1"}
    with PITSession(clean_store, as_of=INT64_MAX) as session:
        for check in CHECKS:
            with pytest.raises(ValueError):
                check(session, arguments["venue"], arguments["instrument"], *ALL, QualityConfig())


# --- configuration ------------------------------------------------------------------------


def test_config_defaults_and_loading_from_a_file(tmp_path):
    default = QualityConfig()
    assert (default.min_lag_ns, default.max_lag_ns) == (-500_000_000, 10 * NS)
    assert (default.outlier_window, default.outlier_min_trades, default.outlier_bps) == (
        50,
        20,
        500,
    )
    assert default.max_backwards_ns == 0 and default.max_findings == 1000

    path = tmp_path / "quality.json"
    path.write_text(json.dumps({"outlier_bps": 250, "max_lag_ns": 3 * NS}), encoding="utf-8")
    loaded = QualityConfig.from_file(path)
    assert (loaded.outlier_bps, loaded.max_lag_ns, loaded.outlier_window) == (250, 3 * NS, 50)
    with pytest.raises(AttributeError):
        loaded.outlier_bps = 1


@pytest.mark.parametrize(
    "content",
    [
        '{"outlier_bp": 5}',  # misspelled
        '{"outlier_bps": "5"}',
        '{"outlier_bps": 5.5}',
        '{"outlier_bps": -1}',
        '{"outlier_window": 0}',
        '{"outlier_min_trades": 60}',  # more than the window can hold
        '{"min_lag_ns": 5, "max_lag_ns": 4}',
        '{"max_findings": 0}',
        "[1, 2]",
        "not json",
    ],
)
def test_a_bad_config_file_is_refused(tmp_path, content):
    path = tmp_path / "quality.json"
    path.write_text(content, encoding="utf-8")
    with pytest.raises(ValueError):
        QualityConfig.from_file(path)


# --- the report command ---------------------------------------------------------------------

DAY = ["--start", "2026-11-11", "--end", "2026-11-11"]


def test_report_on_clean_data_exits_zero_and_summarizes(clean_store, capsys):
    assert quality.main(["--store", str(clean_store), *DAY]) == 0
    output = capsys.readouterr().out
    assert "2026-11-11 to 2026-11-11" in output
    for text in ("kraken/BTC-USD", "coinbase/BTC-USD", "stream_integrity", "clock_skew",
                 "crossed_books", "outliers", "0 findings", "median lag"):  # fmt: skip
        assert text in output, text
    assert "Result: no findings" in output


def test_report_exits_nonzero_only_at_or_above_the_chosen_severity(tmp_path, kraken_rows, capsys):
    victim = live_trades(kraken_rows)[100]

    def edit(rows):  # a warning: a price outlier
        rows[TRADES] = [
            {**r, "price": r["price"] * 2} if r["trade_id"] == victim["trade_id"] else r
            for r in rows[TRADES]
        ]

    root = str(planted(tmp_path, kraken_rows, edit))
    assert quality.main(["--store", root, *DAY]) == 0  # default: fail on errors only
    output = capsys.readouterr().out
    assert "WARNING" in output and "price_outlier" in output
    assert "Result: 1 finding (worst: WARNING); failing at ERROR" in output
    assert quality.main(["--store", root, *DAY, "--fail-on", "warning"]) == 1
    assert quality.main(["--store", root, *DAY, "--fail-on", "info"]) == 1
    assert quality.main(["--store", root, *DAY, "--fail-on", "error"]) == 0


def test_report_exits_nonzero_on_an_error(tmp_path, kraken_rows, capsys):
    victim = live_trades(kraken_rows)[100]

    def edit(rows):
        rows[TRADES] = [
            {**r, "qty": 0} if r["trade_id"] == victim["trade_id"] else r for r in rows[TRADES]
        ]

    root = str(planted(tmp_path, kraken_rows, edit))
    assert quality.main(["--store", root, *DAY]) == 1
    output = capsys.readouterr().out
    assert "ERROR" in output and "nonpositive_quantity" in output
    assert f"seq {victim['seq']}" in output


def test_report_date_range_and_filters(clean_store, capsys):
    store = ["--store", str(clean_store)]
    # No data on that day: reported, and a warning rather than a silent pass.
    assert quality.main([*store, "--start", "2026-11-12", "--end", "2026-11-13"]) == 0
    assert "no data" in capsys.readouterr().out
    empty_day = [*store, "--start", "2026-11-12", "--end", "2026-11-13", "--fail-on", "warning"]
    assert quality.main(empty_day) == 1
    capsys.readouterr()

    assert quality.main([*store, *DAY, "--venue", "kraken"]) == 0
    output = capsys.readouterr().out
    assert "kraken/BTC-USD" in output and "coinbase" not in output

    # A range of several days includes the day with data.
    assert quality.main([*store, "--start", "2026-11-09", "--end", "2026-11-12"]) == 0
    assert "coinbase/BTC-USD" in capsys.readouterr().out


def test_report_as_of_shows_what_was_known_then(clean_store, capsys):
    first_file = min(int(p.name[5:25]) for p in clean_store.rglob("*.parquet"))
    early = ["--store", str(clean_store), *DAY, "--as-of", str(first_file - 1)]
    assert quality.main(early) == 0
    assert "no data" in capsys.readouterr().out


def test_report_uses_a_config_file(clean_store, tmp_path, capsys):
    path = tmp_path / "quality.json"
    path.write_text('{"max_lag_ns": 1500000000}', encoding="utf-8")
    arguments = ["--store", str(clean_store), *DAY, "--config", str(path), "--fail-on", "warning"]
    assert quality.main(arguments) == 1
    assert "late" in capsys.readouterr().out


@pytest.mark.parametrize(
    "arguments",
    [
        ["--start", "2026-11-11", "--end", "2026-11-11"],  # no store
        ["--store", "X", "--start", "2026-11-12", "--end", "2026-11-11"],  # end before start
        ["--store", "X", "--start", "11/11/2026", "--end", "2026-11-11"],
        ["--store", "X", "--start", "2026-11-11", "--end", "2026-11-11", "--fail-on", "maybe"],
    ],
)
def test_report_rejects_bad_arguments(arguments, clean_store):
    arguments = [str(clean_store) if a == "X" else a for a in arguments]
    with pytest.raises(SystemExit) as stopped:
        quality.main(arguments)
    assert stopped.value.code == 2
