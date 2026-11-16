"""Tests for the 2026-11 adversarial review (docs/reviews/2026-11-data-core.md).

Each test proves a finding of the review and is named after it (R01, R02,
...). The tests state what the documents promise, or what the review argues
the code should do.

A TEST WHOSE FINDING IS STILL OPEN IS MARKED ``xfail(strict=True)`` WITH ITS
FINDING NUMBER AS THE REASON. The marker keeps ``scripts/check.ps1`` green
while the finding is open, and it is strict: once a finding is fixed its test
passes, the suite fails on the unexpected pass, and the marker has to be
removed in the same change. From then on the test is the finding's regression
test.

Every finding with a test here is fixed, and no test is marked: R01, R02, R03
and R09 (the time model, ADR 0017), R04, R05, R10 and R11 (loads and the
normalizer's input, ADR 0018), and R06, R07 and R08 (ADR 0019). The tests of
R01 to R03 and R09 use the session's two cutoffs. Four tests could not keep
their original shape: R03's and R09's because the fix makes the situation
they set up impossible, R10's conflict test and R11's because the owner
decided a behavior other than the one they required. Each says so where it
stands. R10's also states its price at the fixed exponent of ADR 0019.

If the owner decides a behavior is intended, the test is replaced by one that
pins the decision, in the same change as the ADR that records it. Do not
delete or weaken a test here, and do not leave a marker on a test that passes
(CLAUDE.md).

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
from quantagent.data.adapters import InterleavedLogs, Normalizer, ingest, normalize
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
    AppendOnlyStore(root)._append(STREAM_EVENTS, "coinbase", INSTRUMENT,
                                 table_from_rows(STREAM_EVENTS, rows))  # fmt: skip
    return root


# --- R01: a session is bounded by when a row was received (fixed, ADR 0017) --------------
#
# Architecture 4.3: a backtest's session shows what had been received by
# simulated time. Section 6.2: simulated now "is the recv_ts of the event being
# processed plus modelled latency, so a strategy sees data when we would have
# had it".
#
# Before ADR 0017 the session's one cutoff, as_of, bounded knowledge_ts: the
# wall clock when ``ingest`` ran, days after the recording. A session at a time
# inside the recording was empty. The cutoff is now ``available_at``, on
# recv_ts, and ranges are receipt time too.


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
    with PITSession(loaded, available_at=now) as session:
        visible = session.trades(venue, INSTRUMENT, *ALL)
    # Failed with 0 rows: the store learned of all of them days later, at load time.
    assert visible.num_rows == len(arrived)


@pytest.mark.parametrize("venue", ["kraken", "coinbase"])
def test_r01_reading_up_to_simulated_time_returns_nothing_that_arrived_later(loaded, venue):
    """The other call a harness could make: read everything and bound the range
    at simulated time. The range used to bound venue time, so rows the recorder
    had not received yet came back (in flight, or history resent at session
    start). It bounds receipt time now."""
    records = records_of(venue)
    now = (records[0].recv_ts + records[-1].recv_ts) // 2
    with PITSession(loaded, available_at=INT64_MAX) as session:
        trades = session.trades(venue, INSTRUMENT, 0, now).to_pylist()
        levels = session.book_events(venue, INSTRUMENT, 0, now).to_pylist()
    assert len(trades) > 50 and len(levels) > 1000
    not_yet_received = [r for r in (*trades, *levels) if r["recv_ts"] > now]
    assert not_yet_received == []


# --- R02: integrity events are known with the data they qualify (fixed, ADR 0017) -------
#
# ``ingest`` used to append each flush as instruments, trades, book_events,
# then stream_events, each with its own knowledge time. Between the last two, a
# point-in-time reader saw a stream with a lost message and no sign of it. A
# load is one batch now: one knowledge time, readable only when all of it is.


def test_r02_book_rows_are_never_visible_before_the_events_that_qualify_them(tmp_path):
    records = records_of("coinbase")
    updates = stream_book_updates(records)
    lost = updates[len(updates) // 2]
    damaged = write_log(tmp_path / "lost.qaraw", [r for r in records if r.seq != lost.seq])
    root = tmp_path / "store"
    ingest(damaged, AppendOnlyStore(root, clock=SteppingClock()))

    unqualified, told = [], []
    for version in knowledge_times(root):
        with PITSession(root, available_at=INT64_MAX, version=version) as session:
            table = session.book_events("coinbase", INSTRUMENT, *ALL)
            events = session.stream_events("coinbase", INSTRUMENT, *ALL).to_pylist()
        if table.num_rows == 0:
            continue
        try:
            for _ in book.replay("coinbase", book.messages_from_table(table), events):
                pass
        except book.SequenceBreak:
            told.append(version)  # the reader was told
            continue
        unqualified.append(version)
    # Failed: at the book batch's knowledge time the stream replayed as if intact.
    assert unqualified == []
    assert told == [1000]  # the whole load is one version, and it holds the gap


# --- R03: a version that can be read never changes afterwards (fixed, ADR 0017) ---------
#
# The store reads the clock when a batch is opened and publishes it later. A
# session opened in between, with a cutoff later than the batch's knowledge
# time, used to miss the batch; the same cutoff afterwards included it.
#
# This test could not keep its original form. It opened both sessions at
# knowledge cutoff 2000 and required the same rows from both. No store can
# promise that for a cutoff ahead of a batch that is still being published
# without refusing the request, and refusing it is the fix: a session's
# version cannot be later than the latest committed batch. So the test now
# checks the two things that make versions repeatable: the cutoff of 2000 is
# refused while the batch is unpublished, and the version a session *was*
# given returns the same rows afterwards.


def test_r03_a_session_opened_after_a_batchs_knowledge_time_always_sees_it(root, monkeypatch):
    root.mkdir()
    seen = {}
    real_link = os.link

    def look_then_publish(source, target):
        # Another process opening a session while the writer is between the two
        # steps: the batch's knowledge time (1000) is issued, its marker is not out.
        with pytest.raises(ValueError, match="latest committed"):
            PITSession(root, available_at=INT64_MAX, version=2_000)
        with PITSession(root, available_at=INT64_MAX) as session:
            seen["version"] = session.version
            seen["while_writing"] = session.trades("kraken", "ETH-USD", *ALL).num_rows
        real_link(source, target)

    monkeypatch.setattr(os, "link", look_then_publish)
    result = AppendOnlyStore(root, clock=FakeClock(1_000))._append(
        TRADES, "kraken", "ETH-USD", trade_table(("a", T0, 100))
    )
    monkeypatch.undo()
    assert result.knowledge_ts == 1_000
    assert seen["version"] < 1_000  # the unpublished batch was not part of any version
    with PITSession(root, available_at=INT64_MAX, version=seen["version"]) as session:
        seen["afterwards"] = session.trades("kraken", "ETH-USD", *ALL).num_rows
    # Failed: the same cutoff, later than the batch's knowledge time, gave 0 rows then 1.
    assert seen["while_writing"] == seen["afterwards"]
    # From its knowledge time on, the batch is in every version that can be asked for.
    for version in (1_000, None):
        with PITSession(root, available_at=INT64_MAX, version=version) as session:
            assert session.trades("kraken", "ETH-USD", *ALL).num_rows == 1


# --- R04: a reload takes away the rows it no longer produces (fixed, ADR 0018) ----------
#
# ADR 0009: keying rows by their raw record "is what makes it safe to fix an
# adapter bug and re-ingest". That was true for rows the fixed adapter still
# produced; the rows it no longer produced stayed, and here one of them is an
# integrity error. Every ingest is now a load, and a session reads each log
# from its latest complete load only.


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

    with PITSession(root, available_at=INT64_MAX) as session:
        result = quality.check_stream_integrity(session, "kraken", INSTRUMENT, *ALL,
                                                QualityConfig())  # fmt: skip
    # Failed: message_rejected was still reported, at the record that now parses.
    assert [(f.what, f.seq) for f in result.findings] == []


# --- R05: a value the schema refuses is a rejected message (fixed, ADR 0018) ------------
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
    # Failed: SchemaError out of ingest at the next flush; the rest of the log was not loaded.
    report = ingest(path, AppendOnlyStore(tmp_path / "store"))
    assert len(report.rejects) == 1


# --- R06: instrument metadata can be relied on to interpret a row (fixed, ADR 0019) ------
#
# ADR 0005: "readers rescale using the metadata in force at each row's time",
# and "exp is the finest decimal precision the venue supports for the
# instrument, not the current tick size". The owner decided (ADR 0019) that
# each instrument has one fixed exponent, independent of its tick size, and
# that a log's metadata version is in effect from the session's first row.


@pytest.mark.parametrize("venue", ["kraken", "coinbase"])
def test_r06_a_metadata_version_is_in_effect_for_every_row_of_its_session(loaded, venue):
    with PITSession(loaded, available_at=INT64_MAX) as session:
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


# --- R07: the finding limit never hides an ERROR (fixed, ADR 0019) --------------------------
#
# ADR 0016 fixed this for repeated trades only ("would fill the limit with
# INFO findings and hide any later ERROR in the same check").


def test_r07_information_findings_never_crowd_out_an_error(tmp_path):
    rows = [stream_event(0, StreamEventKind.SESSION_START, T0)]
    rows += [comparison(seq, T0 + seq, "explained", unverified=1) for seq in (1, 2, 3)]
    rows.append(comparison(4, T0 + 4, "mismatch", violations=7))
    root = events_store(tmp_path / "store", rows)
    with PITSession(root, available_at=INT64_MAX) as session:
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


# --- R08: a session's clock error is reported on every day it spans (fixed, ADR 0019) ----


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
        with PITSession(root, available_at=INT64_MAX) as session:
            result = quality.check_recording_clock(
                session, "coinbase", INSTRUMENT, day_start, day_start + DAY, QualityConfig()
            )
        return [f.what for f in result.findings if f.severity == Severity.ERROR]

    assert clock_errors(midnight - DAY) == ["clock_offset_exceeded"]
    # Fails with []: the second day's data has the same 1.4 s error and no finding.
    assert clock_errors(midnight) == ["clock_offset_exceeded"]


# --- R09: one window selects book rows and their events by one clock (fixed, ADR 0017) ---
#
# Every range method used to filter event_ts: venue time for book rows, receipt
# time for stream events, later by the lag. A window could hold book messages
# from after a lost message and not the event that reports the loss. Ranges
# are receipt time for every dataset now.
#
# This test could not keep its original form either. It took the one window
# that ends just before the gap was noticed and required more than ten book
# messages from after the loss inside it. With one clock that window holds
# none, which is the fix. So the test now sweeps window ends across the loss,
# by the venue's clock and by ours, and requires of every one of them what the
# original required of one: if the window holds a book message from after the
# loss, replaying it raises.


def test_r09_the_same_window_for_book_rows_and_events_still_finds_a_lost_message(tmp_path):
    records = records_of("coinbase")
    updates = stream_book_updates(records)
    lost = updates[len(updates) // 2]
    damaged = write_log(tmp_path / "lost.qaraw", [r for r in records if r.seq != lost.seq])
    root = tmp_path / "store"
    ingest(damaged, AppendOnlyStore(root))

    def stream_rows_after_the_loss(table):
        return {
            r["seq"]
            for r in table.to_pylist()
            if r["seq"] >= gap["seq"] and r["kind"] != BookEventKind.CHECK_SNAPSHOT
        }

    with PITSession(root, available_at=INT64_MAX) as session:
        everything = session.stream_events("coinbase", INSTRUMENT, *ALL).to_pylist()
        (gap,) = [e for e in everything if e["kind"] == StreamEventKind.SEQUENCE_GAP]
        noticed = gap["recv_ts"]  # when the record that revealed the loss arrived
        levels = session.book_events("coinbase", INSTRUMENT, *ALL)
        venue_times = sorted(
            {
                r["event_ts"]
                for r in levels.to_pylist()
                if r["seq"] >= gap["seq"] and r["kind"] != BookEventKind.CHECK_SNAPSHOT
            }
        )[:3]
        assert venue_times[-1] < noticed  # the venue stamped them before we saw the gap
        ends = [t + 1 for t in venue_times] + [noticed - 1, noticed, noticed + 1, noticed + NS]

        held, raised = [], []
        for end in ends:
            table = session.book_events("coinbase", INSTRUMENT, 0, end)
            events = session.stream_events("coinbase", INSTRUMENT, 0, end).to_pylist()
            after_the_loss = stream_rows_after_the_loss(table)
            try:
                for _ in book.replay("coinbase", book.messages_from_table(table), events):
                    pass
            except book.SequenceBreak:
                raised.append(end)
            else:
                # Failed for the windows that end at the venue times: no error, and
                # those messages were applied to a book that is missing an update.
                assert not after_the_loss, end
            if after_the_loss:
                held.append(end)
    # The sweep really did cross the loss: windows before it, and windows holding
    # book messages from after it (more than ten in the widest).
    assert held == [noticed + 1, noticed + NS] or held == [noticed + NS]
    assert set(held) <= set(raised) and ends[0] not in raised
    with PITSession(root, available_at=INT64_MAX) as session:
        widest = session.book_events("coinbase", INSTRUMENT, 0, noticed + NS)
    assert len(stream_rows_after_the_loss(widest)) > 10


# --- R10: one rule for which observation of a trade is shown (fixed, ADR 0018) ---------
#
# ADR 0016: "the store keeps the first observation". docs/data-core.md section
# 5 said a repeat that differs "is stored, so the store shows the later value".
# Which observation the store showed depended on how the logs were loaded and
# on the clocks. The owner decided one rule (ADR 0018): the first observation
# in replay order, by log_id and then seq, across logs and across loads.
#
# The second test could not keep its original form. It required the *later*
# value, as data-core said, in two cases where the view's tie-break (venue
# time, then receipt time) happened to pick the first. The decision is the
# opposite of what it required, so it now requires the first observation, and
# it has two more cases: the mirror images, where that tie-break picked the
# later one. Before ADR 0018 those two failed with (4, 1010).


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

    with PITSession(root, available_at=INT64_MAX) as session:
        (trade,) = session.trades("kraken", INSTRUMENT, *ALL).to_pylist()
        skew = quality.check_clock_skew(session, "kraken", INSTRUMENT, *ALL, QualityConfig())
    # Failed: the stored row was the resent copy, received an hour later, and the
    # trade counted as backfill (0 live rows considered).
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
        pytest.param(
            "2026-11-11T03:00:00.000000Z",
            "2026-11-11T03:00:00.500000Z",
            0,
            id="the later observation carries a later venue time",
        ),
        pytest.param(
            "2026-11-11T03:00:00.000000Z",
            "2026-11-11T03:00:00.000000Z",
            0,
            id="the same venue time, received in order",
        ),
    ],  # fmt: skip
)
def test_r10_a_conflicting_repeat_shows_the_first_observation_whatever_the_clocks_say(
    tmp_path, first_time, later_time, clock_step
):
    records = kraken(
        kraken_trade(price="100.0", timestamp=first_time),  # seq 3: the first observation
        kraken_trade(price="101.0", timestamp=later_time),  # seq 4: the later observation
    )
    records = [
        RawRecord(r.recv_ts + (clock_step if r.seq == 4 else 0), r.seq, r.venue, r.channel,
                  r.payload)
        for r in records
    ]  # fmt: skip
    root = tmp_path / "store"
    report = ingest(write_log(tmp_path / "conflict.qaraw", records), AppendOnlyStore(root))
    assert report.events["TRADE_CONFLICT"] == 1  # a conflict is still an error
    with PITSession(root, available_at=INT64_MAX) as session:
        (trade,) = session.trades("kraken", INSTRUMENT, *ALL).to_pylist()
    # The last two cases failed with (4, 1010): the two rows shared a batch, and
    # the view broke the tie on event_ts and then recv_ts, not on replay order.
    # (Prices were then at Kraken's one decimal; since ADR 0019 every BTC-USD
    # price is at eight, so 100.0 is 10_000_000_000.)
    assert (trade["seq"], trade["price"]) == (3, 10_000_000_000)


# --- R11: one normalizer fed two logs in arrival order mixed them up (fixed, ADR 0018) ---
#
# ``logorder.merge_logs`` yields the records of several logs interleaved, in
# arrival order. Feeding that to a Normalizer is the obvious way to compare
# trades across simultaneous logs (ADR 0015), and nothing refused it.
#
# The owner decided the normalizer refuses it, with a clear error. This test
# could not keep its original form. It required every row to keep its own
# log's id. A raw record does not say which log it is from: once the second
# log's first record is in, the first log's record 1 cannot be told from the
# second log's record 1, so no normalizer can do that. The test now requires
# what the finding was about, that keys never collide silently: fed
# interleaved, the normalizer raises and nothing comes out of it; fed one log
# after the other, each row keeps its log's id and no key appears twice.


def test_r11_records_of_two_logs_fed_in_arrival_order_are_refused_not_mixed_up():
    one = records_of("kraken")[:400]
    two = [RawRecord(r.recv_ts + 1, r.seq, r.venue, r.channel, r.payload, r.recv_mono_ns)
           for r in one]  # fmt: skip
    assert log_id_of(one[0]) != log_id_of(two[0])
    normalizer = Normalizer()
    # Failed with DID NOT RAISE: every row named the second log, so each key
    # appeared twice and the store would take one log's rows as corrections
    # of the other's.
    with pytest.raises(InterleavedLogs, match="interleaved"):
        for a, b in zip(one, two, strict=True):
            normalizer.feed(a)
            normalizer.feed(b)
    with pytest.raises(InterleavedLogs):
        normalizer.take_rows()

    levels = normalize(one + two).rows[("book_events", "kraken", INSTRUMENT)]
    keys = [(r["log_id"], r["seq"], r["level_index"]) for r in levels]
    assert {r["log_id"] for r in levels} == {log_id_of(one[0]), log_id_of(two[0])}
    assert len(set(keys)) == len(keys)
