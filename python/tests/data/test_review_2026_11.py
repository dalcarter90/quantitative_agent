"""Failing tests for the 2026-11 adversarial review (docs/reviews/2026-11-data-core.md).

EVERY TEST IN THIS FILE FAILS ON PURPOSE. Each one proves a finding of the
review, and is named after it (R01, R02, ...). Nothing here is fixed: the
tests state what the documents promise, or what the review argues the code
should do, and show that it does not.

When a finding is fixed its test passes and stays as the regression test. If
the owner decides a behavior is intended, the test is replaced by one that
pins the decision, in the same change as the ADR that records it. Do not
delete, skip or weaken one to get to green (CLAUDE.md).

The data is the committed fixtures and small hand-built logs: no network and
no local recordings.
"""

import json
import os
from pathlib import Path

import pytest
from _builders import INT64_MAX, LOG, T0, FakeClock, trade_table
from test_adapters import (
    COINBASE_CONNECT,
    COINBASE_START,
    KRAKEN_BOOK_ACK,
    KRAKEN_INSTRUMENT,
    KRAKEN_START,
    log,
)
from test_fixtures import INSTRUMENT, fixture_path

from quantagent.data import book, quality
from quantagent.data.adapters import Normalizer, ingest, normalize
from quantagent.data.quality import QualityConfig, Severity
from quantagent.data.rawlog import RawLogWriter, RawRecord, log_id_of, read_raw_log
from quantagent.data.schema import (
    STREAM_EVENTS,
    TRADES,
    BookEventKind,
    StreamEventKind,
    table_from_rows,
)
from quantagent.data.session import PITSession
from quantagent.data.store import AppendOnlyStore

NS = 1_000_000_000
DAY = 86_400 * NS
ALL = (0, INT64_MAX)


# --- helpers ---------------------------------------------------------------------------


def records_of(venue):
    return list(read_raw_log(fixture_path(venue)))


def write_log(path, records):
    with RawLogWriter(path) as writer:
        for r in records:
            writer.write(r.venue, r.channel, r.payload, recv_ts=r.recv_ts,
                         recv_mono_ns=r.recv_mono_ns)  # fmt: skip
    return path


def stream_book_updates(records):
    marker = (b'"channel":"book"', b'"channel":"l2_data"')
    return [
        r
        for r in records
        if r.channel == "ws"
        and any(m in r.payload[:60] for m in marker)
        and b'"update"' in r.payload[:200]
    ]


class SteppingClock:
    """1000, 2000, 3000, ...: one knowledge time per append, easy to open sessions at."""

    def __init__(self):
        self.now = 0

    def __call__(self):
        self.now += 1000
        return self.now


def knowledge_times(root):
    return sorted({int(p.name[5:25]) for p in Path(root).rglob("*.parquet")})


@pytest.fixture(scope="module")
def loaded(tmp_path_factory):
    """Both fixtures loaded the way the command line loads them: now."""
    root = tmp_path_factory.mktemp("review") / "store"
    store = AppendOnlyStore(root)
    for venue in ("kraken", "coinbase"):
        ingest(fixture_path(venue), store)
    return root


def kraken_trade(trade_id=115202912, price="73469.7", kind="update",
                 timestamp="2026-11-11T03:06:47.999916Z"):  # fmt: skip
    return (
        "ws",
        f'{{"channel":"trade","type":"{kind}","data":[{{"symbol":"BTC/USD","side":"sell",'
        f'"price":{price},"qty":0.00051042,"ord_type":"limit","trade_id":{trade_id},'
        f'"timestamp":"{timestamp}"}}]}}',
    )


def kraken(*messages):
    return log("kraken", KRAKEN_START, KRAKEN_INSTRUMENT, KRAKEN_BOOK_ACK, *messages)


def coinbase_product(price_increment="0.01"):
    return (
        "rest:product",
        json.dumps(
            {
                "product_id": "BTC-USD",
                "base_increment": "0.00000001",
                "price_increment": price_increment,
            }
        ),  # fmt: skip
    )


def coinbase(*messages, price_increment="0.01"):
    return log("coinbase", COINBASE_START, coinbase_product(price_increment), COINBASE_CONNECT,
               *messages)  # fmt: skip


def coinbase_trade(sequence_num, time="2026-11-11T03:06:47.947537Z"):
    trade = {"product_id": "BTC-USD", "trade_id": "1118502354", "price": "73469.65",
             "size": "0.02129674", "time": time, "side": "BUY"}  # fmt: skip
    return ("ws", json.dumps({"channel": "market_trades", "sequence_num": sequence_num,
                              "timestamp": "2026-11-11T03:06:48.052121501Z",
                              "events": [{"type": "update", "trades": [trade]}]}))  # fmt: skip


def coinbase_book_snapshot(sequence_num):
    level = {"side": "bid", "event_time": "2026-11-11T03:06:47.490859Z",
             "price_level": "73469.65", "new_quantity": "0.02208558"}  # fmt: skip
    event = {"type": "snapshot", "product_id": "BTC-USD", "updates": [level]}
    return ("ws", json.dumps({"channel": "l2_data", "sequence_num": sequence_num,
                              "timestamp": "2026-11-11T03:06:47.624143697Z",
                              "events": [event]}))  # fmt: skip


def stream_event(seq, kind, ts, *, index=0, connection=None, expected=None, observed=None,
                 detail=None):  # fmt: skip
    return {"event_ts": ts, "event_ts_source": "receipt", "recv_ts": ts, "log_id": LOG,
            "seq": seq, "event_index": index, "kind": kind, "connection": connection,
            "expected": expected, "observed": observed, "detail": detail}  # fmt: skip


def comparison(seq, ts, result, *, violations=0, unverified=0):
    """A CHECK_SNAPSHOT_COMPARED event as the Coinbase adapter writes it (ADR 0014)."""
    detail = {"snapshot_seq": seq, "levels": 10, "result": result, "reason": None,
              "on_batch": False, "next_batch_levels": 3, "violations": violations,
              "violations_shallowest": 0 if violations else None,
              "violations_deepest": 0 if violations else None, "unverified": unverified,
              "unverified_as_next_batch": unverified,
              "unverified_shallowest": 0 if unverified else None,
              "unverified_deepest": 0 if unverified else None}  # fmt: skip
    text = json.dumps(detail, separators=(",", ":"), sort_keys=True)
    return stream_event(seq, StreamEventKind.CHECK_SNAPSHOT_COMPARED, ts, connection="probe",
                        expected=violations, observed=unverified, detail=text)  # fmt: skip


def events_store(root, rows):
    AppendOnlyStore(root).append(STREAM_EVENTS, "coinbase", INSTRUMENT,
                                 table_from_rows(STREAM_EVENTS, rows))  # fmt: skip
    return root


# --- R01: as_of bounds when a row was stored, not when it was received ------------------
#
# Architecture 3.2 and 4.3: "Inside a backtest, the harness creates a session
# per decision time with as_of equal to simulated time." Section 6.2: simulated
# now "is the recv_ts of the event being processed plus modelled latency, so a
# strategy sees data when we would have had it".
#
# knowledge_ts is the wall clock when ``ingest`` ran. A raw log is loaded after
# it was recorded, so every row's knowledge_ts is later than every simulated
# time inside the recording.


@pytest.mark.parametrize("venue", ["kraken", "coinbase"])
def test_r01_a_session_at_a_time_inside_the_recording_sees_what_had_arrived_by_then(loaded, venue):
    records = records_of(venue)
    now = (records[0].recv_ts + records[-1].recv_ts) // 2  # simulated time, mid recording
    arrived = [
        row
        for row in normalize(records).rows[("trades", venue, INSTRUMENT)]
        if row["recv_ts"] <= now
    ]
    assert len(arrived) > 50  # the recorder had received these by then
    with PITSession(loaded, as_of=now) as session:
        visible = session.trades(venue, INSTRUMENT, *ALL)
    # Fails with 0 rows: the store learned of all of them days later, at load time.
    assert visible.num_rows == len(arrived)


@pytest.mark.parametrize("venue", ["kraken", "coinbase"])
def test_r01_reading_up_to_simulated_time_returns_nothing_that_arrived_later(loaded, venue):
    """The call a harness would make once R01 forces it to open the session
    "as of now" instead: the range bounds venue time, so rows the recorder had
    not received yet come back (in flight, or history resent at session start)."""
    records = records_of(venue)
    now = (records[0].recv_ts + records[-1].recv_ts) // 2
    with PITSession(loaded, as_of=INT64_MAX) as session:
        trades = session.trades(venue, INSTRUMENT, 0, now).to_pylist()
        levels = session.book_events(venue, INSTRUMENT, 0, now).to_pylist()
    not_yet_received = [r for r in (*trades, *levels) if r["recv_ts"] > now]
    assert not_yet_received == []


# --- R02: integrity events become known after the data they qualify ----------------------
#
# ``ingest`` appends each flush as instruments, trades, book_events, then
# stream_events, each with its own knowledge time. Between the last two, a
# point-in-time reader sees a stream with a lost message and no sign of it.


def test_r02_book_rows_are_never_visible_before_the_events_that_qualify_them(tmp_path):
    records = records_of("coinbase")
    updates = stream_book_updates(records)
    lost = updates[len(updates) // 2]
    damaged = write_log(tmp_path / "lost.qaraw", [r for r in records if r.seq != lost.seq])
    root = tmp_path / "store"
    ingest(damaged, AppendOnlyStore(root, clock=SteppingClock()))

    unqualified = []
    for as_of in knowledge_times(root):
        with PITSession(root, as_of=as_of) as session:
            table = session.book_events("coinbase", INSTRUMENT, *ALL)
            events = session.stream_events("coinbase", INSTRUMENT, *ALL).to_pylist()
        if table.num_rows == 0:
            continue
        try:
            for _ in book.replay("coinbase", book.messages_from_table(table), events):
                pass
        except book.SequenceBreak:
            continue  # the reader was told
        unqualified.append(as_of)
    # Fails: at the book batch's knowledge time the stream replays as if intact.
    assert unqualified == []


# --- R03: knowledge_ts is stamped before the batch can be read ---------------------------
#
# "Every method returns ... the rows with knowledge_ts <= as_of" is only
# repeatable if a batch is readable from its knowledge time on. The store reads
# the clock first and publishes the files afterwards.


def test_r03_a_session_opened_after_a_batchs_knowledge_time_always_sees_it(root, monkeypatch):
    root.mkdir()
    seen = {}
    real_link = os.link

    def look_then_publish(source, target):
        # Another process opening a session while the writer is between the two steps.
        with PITSession(root, as_of=2_000) as session:
            seen["while_writing"] = session.trades("kraken", "ETH-USD", *ALL).num_rows
        real_link(source, target)

    monkeypatch.setattr(os, "link", look_then_publish)
    result = AppendOnlyStore(root, clock=FakeClock(1_000)).append(
        TRADES, "kraken", "ETH-USD", trade_table(("a", T0, 100))
    )
    monkeypatch.undo()
    assert result.knowledge_ts == 1_000
    with PITSession(root, as_of=2_000) as session:
        seen["afterwards"] = session.trades("kraken", "ETH-USD", *ALL).num_rows
    # Fails: the same as_of, later than the batch's knowledge time, gave 0 rows then 1.
    assert seen["while_writing"] == seen["afterwards"]


# --- R04: a correction cannot take a row away -------------------------------------------
#
# ADR 0009: keying rows by their raw record "is what makes it safe to fix an
# adapter bug and re-ingest". True for rows the fixed adapter still produces.
# Rows it no longer produces stay, and here one of them is an integrity error.


def test_r04_loading_a_log_again_after_a_fix_clears_what_the_fix_resolved(tmp_path):
    records = records_of("kraken")
    updates = stream_book_updates(records)
    victim = updates[len(updates) // 2]
    # Stand-in for "a message the adapter could not parse until it was fixed".
    unparsed = [
        RawRecord(r.recv_ts, r.seq, r.venue, r.channel,
                  b'{"broken' if r.seq == victim.seq else r.payload, r.recv_mono_ns)
        for r in records
    ]  # fmt: skip
    assert log_id_of(unparsed[0]) == log_id_of(records[0])  # the same log

    root = tmp_path / "store"
    store = AppendOnlyStore(root)
    first = ingest(write_log(tmp_path / "before-fix.qaraw", unparsed), store)
    assert len(first.rejects) == 1
    second = ingest(fixture_path("kraken"), store)  # the fix: every message parses
    assert second.rejects == [] and second.events["CHECKSUM_MISMATCH"] == 0

    with PITSession(root, as_of=INT64_MAX) as session:
        result = quality.check_stream_integrity(session, "kraken", INSTRUMENT, *ALL,
                                                QualityConfig())  # fmt: skip
    # Fails: message_rejected is still reported, at the record that now parses.
    assert [(f.what, f.seq) for f in result.findings] == []


# --- R05: a message the adapter accepts but the schema refuses stops the whole load -----
#
# Roadmap Phase 1c: "Malformed or unknown messages are counted and reported,
# never silently dropped and never a crash."

OUT_OF_RANGE = {
    "kraken venue time after year 2262": lambda: kraken(
        kraken_trade(timestamp="9999-12-31T23:59:59.000000Z")
    ),
    "coinbase venue time after year 2262": lambda: coinbase(
        coinbase_trade(0, time="2300-01-01T00:00:00Z")
    ),
    "coinbase sequence number beyond int64": lambda: coinbase(coinbase_book_snapshot(2**70)),
    "coinbase sequence jump beyond int64": lambda: coinbase(
        coinbase_trade(0), coinbase_trade(2**70)
    ),
    "recorder clock offset beyond int64": lambda: log(
        "kraken",
        KRAKEN_START,
        (
            "recorder",
            {
                "event": "clock",
                "offset_ns": 2**70,
                "limit_ns": 250_000_000,
                "within_limit": False,
                "override": True,
                "servers": [],
            },
        ),
    ),  # fmt: skip
}


@pytest.mark.parametrize("case", sorted(OUT_OF_RANGE))
def test_r05_a_value_that_does_not_fit_the_schema_is_a_reject_not_a_crash(tmp_path, case):
    path = write_log(tmp_path / "odd.qaraw", OUT_OF_RANGE[case]())
    # Fails: SchemaError out of ingest at the next flush; the rest of the log is not loaded.
    report = ingest(path, AppendOnlyStore(tmp_path / "store"))
    assert len(report.rejects) == 1


# --- R06: instrument metadata cannot be relied on to interpret a row --------------------
#
# ADR 0005: "readers rescale using the metadata in force at each row's time",
# and "exp is the finest decimal precision the venue supports for the
# instrument, not the current tick size".


@pytest.mark.parametrize("venue", ["kraken", "coinbase"])
def test_r06_a_metadata_version_is_in_effect_for_every_row_of_its_session(loaded, venue):
    with PITSession(loaded, as_of=INT64_MAX) as session:
        levels = session.book_events(venue, INSTRUMENT, *ALL).to_pylist()
        stream = [r for r in levels if r["kind"] != BookEventKind.CHECK_SNAPSHOT]
        trades = session.trades(venue, INSTRUMENT, *ALL).to_pylist()
        earliest = min((*stream, *trades), key=lambda r: r["event_ts"])
        meta = session.instrument(venue, INSTRUMENT, at=earliest["event_ts"])
    # Fails with None. A version's event_ts is the time we received it; a row's
    # is venue time. Trades resent on subscribe are older than the session, and
    # Kraken stamps its first book snapshot before its metadata reaches us.
    assert meta is not None


def test_r06_a_coinbase_tick_size_change_does_not_rescale_prices():
    def price_exp(price_increment):
        rows = normalize(coinbase(price_increment=price_increment)).rows
        (row,) = rows[("instruments", "coinbase", INSTRUMENT)]
        return row["price_exp"]

    # Fails with 2 != 1: the exponent is read off the tick size, so 73469.60 is
    # mantissa 7346960 before the change and 734696 after it.
    assert price_exp("0.01") == price_exp("0.1")


# --- R07: the finding limit can hide an ERROR behind INFO findings -----------------------
#
# ADR 0016 fixed this for repeated trades only ("would fill the limit with
# INFO findings and hide any later ERROR in the same check").


def test_r07_information_findings_never_crowd_out_an_error(tmp_path):
    rows = [stream_event(0, StreamEventKind.SESSION_START, T0)]
    rows += [comparison(seq, T0 + seq, "explained", unverified=1) for seq in (1, 2, 3)]
    rows.append(comparison(4, T0 + 4, "mismatch", violations=7))
    root = events_store(tmp_path / "store", rows)
    with PITSession(root, as_of=INT64_MAX) as session:
        result = quality.check_independent_snapshots(
            session, "coinbase", INSTRUMENT, *ALL, QualityConfig(max_findings=3)
        )
    assert result.stats["mismatched"] == 1
    # Fails: three INFO findings, and the mismatch is dropped.
    assert Severity.ERROR in {f.severity for f in result.findings}


def test_r07_the_report_fails_when_an_error_was_cut_off_by_the_limit(tmp_path, capsys):
    rows = [stream_event(0, StreamEventKind.SESSION_START, T0)]
    rows += [comparison(seq, T0 + seq, "explained", unverified=1) for seq in (1, 2, 3)]
    rows.append(comparison(4, T0 + 4, "mismatch", violations=7))
    root = events_store(tmp_path / "store", rows)
    config = tmp_path / "quality.json"
    config.write_text('{"max_findings": 3}', encoding="utf-8")
    status = quality.main(["--store", str(root), "--start", "2023-11-14", "--end", "2023-11-14",
                           "--config", str(config)])  # fmt: skip
    capsys.readouterr()
    # Fails with 0: "worst: INFO", although a check snapshot mismatched.
    assert status == 1


# --- R08: a session's clock error is reported only where the session started -------------


def test_r08_a_bad_recording_clock_is_reported_for_every_day_the_session_covers(tmp_path):
    midnight = (T0 // DAY + 1) * DAY
    start = midnight - 60 * NS  # the session begins at 23:59 and records into the next day
    rows = [
        stream_event(0, StreamEventKind.SESSION_START, start,
                     detail='{"monotonic_stamps":true}'),
        stream_event(1, StreamEventKind.CLOCK_OFFSET, start + 1, expected=250_000_000,
                     observed=1_400_000_000, detail='{"servers":[]}'),
        stream_event(2, StreamEventKind.CONNECT, start + 2, connection="ws"),
        stream_event(900, StreamEventKind.SUBSCRIPTION_ACK, midnight + 3600 * NS,
                     connection="ws"),
    ]  # fmt: skip
    root = events_store(tmp_path / "store", rows)

    def clock_errors(day_start):
        with PITSession(root, as_of=INT64_MAX) as session:
            result = quality.check_recording_clock(
                session, "coinbase", INSTRUMENT, day_start, day_start + DAY, QualityConfig()
            )
        return [f.what for f in result.findings if f.severity == Severity.ERROR]

    assert clock_errors(midnight - DAY) == ["clock_offset_exceeded"]
    # Fails with []: the second day's data has the same 1.4 s error and no finding.
    assert clock_errors(midnight) == ["clock_offset_exceeded"]


# --- R09: one time window selects book rows and their events on different clocks --------
#
# Every range method filters event_ts. For book rows that is venue time; for
# stream events it is the receipt time, which is later by the lag.


def test_r09_the_same_window_for_book_rows_and_events_still_finds_a_lost_message(tmp_path):
    records = records_of("coinbase")
    updates = stream_book_updates(records)
    lost = updates[len(updates) // 2]
    damaged = write_log(tmp_path / "lost.qaraw", [r for r in records if r.seq != lost.seq])
    root = tmp_path / "store"
    ingest(damaged, AppendOnlyStore(root))

    with PITSession(root, as_of=INT64_MAX) as session:
        everything = session.stream_events("coinbase", INSTRUMENT, *ALL).to_pylist()
        (gap,) = [e for e in everything if e["kind"] == StreamEventKind.SEQUENCE_GAP]
        end = gap["event_ts"]  # a window that ends just before the gap was noticed
        table = session.book_events("coinbase", INSTRUMENT, 0, end)
        events = session.stream_events("coinbase", INSTRUMENT, 0, end).to_pylist()
    after_the_loss = {r["seq"] for r in table.to_pylist() if r["seq"] >= gap["seq"]}
    assert len(after_the_loss) > 10  # the window holds book messages from after the loss
    # Fails: no error; those messages are applied to a book that is missing an update.
    with pytest.raises(book.SequenceBreak):
        for _ in book.replay("coinbase", book.messages_from_table(table), events):
            pass


# --- R10: which observation of a trade the store shows depends on how it was loaded ------
#
# ADR 0016: "the store keeps the first observation". docs/data-core.md section
# 5: a repeat that differs "is stored, so the store shows the later value".


def test_r10_a_trade_resent_as_history_in_a_later_log_keeps_its_live_observation(tmp_path):
    live = kraken(kraken_trade())  # seen as it happened
    resent = [  # the next session, an hour later: the venue's history on subscribe
        RawRecord(r.recv_ts + 3600 * NS, r.seq, r.venue, r.channel, r.payload)
        for r in kraken(kraken_trade(kind="snapshot"))
    ]
    root = tmp_path / "store"
    store = AppendOnlyStore(root)
    # One log per run, which is the only way the command line loads them.
    ingest(write_log(tmp_path / "first.qaraw", live), store)
    ingest(write_log(tmp_path / "second.qaraw", resent), store)

    with PITSession(root, as_of=INT64_MAX) as session:
        (trade,) = session.trades("kraken", INSTRUMENT, *ALL).to_pylist()
        skew = quality.check_clock_skew(session, "kraken", INSTRUMENT, *ALL, QualityConfig())
    # Fails: the stored row is the resent copy, received an hour later, and the
    # trade now counts as backfill (0 live rows considered).
    assert (trade["log_id"], trade["recv_ts"]) == (log_id_of(live[0]), live[3].recv_ts)
    assert skew.stats["backfill_trades_skipped"] == 0


@pytest.mark.parametrize(
    ("first_time", "later_time", "clock_step"),
    [
        # The venue's timestamp on each observation, and how far the wall clock
        # stepped between receiving the first and the later one.
        pytest.param(
            "2026-11-11T03:00:00.500000Z",
            "2026-11-11T03:00:00.000000Z",
            0,
            id="the later observation carries an earlier venue time",
        ),
        pytest.param(
            "2026-11-11T03:00:00.000000Z",
            "2026-11-11T03:00:00.000000Z",
            -NS,
            id="the wall clock stepped back between the two",
        ),
    ],  # fmt: skip
)
def test_r10_a_conflicting_repeat_shows_the_later_value(
    tmp_path, first_time, later_time, clock_step
):
    records = kraken(
        kraken_trade(price="100.0", timestamp=first_time),  # seq 3
        kraken_trade(price="101.0", timestamp=later_time),  # seq 4: the later observation
    )
    records = [
        RawRecord(r.recv_ts + (clock_step if r.seq == 4 else 0), r.seq, r.venue, r.channel,
                  r.payload)
        for r in records
    ]  # fmt: skip
    root = tmp_path / "store"
    report = ingest(write_log(tmp_path / "conflict.qaraw", records), AppendOnlyStore(root))
    assert report.events["TRADE_CONFLICT"] == 1
    with PITSession(root, as_of=INT64_MAX) as session:
        (trade,) = session.trades("kraken", INSTRUMENT, *ALL).to_pylist()
    # Fails with (3, 1000): the two rows share a batch, and the view breaks the
    # tie on event_ts and then recv_ts, not on the order the records arrived.
    assert (trade["seq"], trade["price"]) == (4, 1010)


# --- R11: one normalizer fed two logs in arrival order mixes them up ---------------------
#
# ``logorder.merge_logs`` yields the records of several logs interleaved, in
# arrival order. Feeding that to a Normalizer is the obvious way to compare
# trades across simultaneous logs (ADR 0015), and nothing refuses it.


def test_r11_records_of_two_logs_fed_in_arrival_order_keep_their_own_log_ids():
    one = records_of("kraken")[:400]
    two = [RawRecord(r.recv_ts + 1, r.seq, r.venue, r.channel, r.payload, r.recv_mono_ns)
           for r in one]  # fmt: skip
    assert log_id_of(one[0]) != log_id_of(two[0])
    normalizer = Normalizer()
    for a, b in zip(one, two, strict=True):
        normalizer.feed(a)
        normalizer.feed(b)
    levels = normalizer.take_rows()[("book_events", "kraken", INSTRUMENT)]
    keys = [(r["log_id"], r["seq"], r["level_index"]) for r in levels]
    # Fails: every row names the second log, so each key appears twice and the
    # store would take one log's rows as corrections of the other's.
    assert {r["log_id"] for r in levels} == {log_id_of(one[0]), log_id_of(two[0])}
    assert len(set(keys)) == len(keys)
