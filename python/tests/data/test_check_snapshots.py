"""Coinbase check snapshots: the batch rule (ADR 0014).

Coinbase sends its book stream in batches about 50 ms apart, each with one
venue time. A check snapshot whose time T equals a batch's time must equal the
rebuilt book exactly. One whose time falls between batch A (time <= T) and
batch B (time > T) must equal the book after A on every level B does not
change; B's own levels cannot be verified from the stream.

The adapter applies the rule while normalizing and writes one
CHECK_SNAPSHOT_COMPARED stream event per check snapshot. The reconstructor
applies it again from the stored rows. These tests check each, and that the
two agree.

Synthetic book used below (prices with 2 decimals, quantities in whole units):

    after the stream snapshot (0 ms)   bids 100 x1, 99 x2, 98 x3    asks 101 x1, 102 x2, 103 x3
    batch at 50 ms                     bid 100 -> 5                  ask 104 -> 1 (new)
    batch at 100 ms                    bid 99 removed                ask 101 -> 2
"""

import json
from pathlib import Path

import pytest
from _builders import INT64_MAX
from test_adapters import COINBASE_CONNECT, COINBASE_PRODUCT, COINBASE_START, log
from test_fixtures import INSTRUMENT, fixture_path
from test_quality import run, store_from_rows

from quantagent.data.adapters import ingest, normalize
from quantagent.data.adapters.coinbase import CHECK_HISTORY_NS
from quantagent.data.book import messages_from_table, verify_check_snapshots
from quantagent.data.quality import Severity, check_independent_snapshots
from quantagent.data.rawlog import read_raw_log
from quantagent.data.schema import BOOK_EVENTS, Side, StreamEventKind, table_from_rows
from quantagent.data.session import PITSession
from quantagent.data.store import AppendOnlyStore

COMPARED = StreamEventKind.CHECK_SNAPSHOT_COMPARED
BETWEEN_BATCHES = (
    Path(__file__).parent.parent / "fixtures" / "between-batches" / "coinbase" / INSTRUMENT
)
PROBES = Path(__file__).parents[3] / "data" / "raw-probes" / "coinbase" / INSTRUMENT


def at(ms: int) -> str:
    """A venue time, ``ms`` milliseconds after 2026-11-13T03:00:01Z."""
    seconds, millis = divmod(1000 + ms, 1000)
    minutes, seconds = divmod(seconds, 60)
    return f"2026-11-13T03:{minutes:02d}:{seconds:02d}.{millis:03d}000Z"


class Session:
    """Builds a Coinbase raw log record by record, numbering each connection's
    messages as Coinbase does."""

    def __init__(self):
        self.entries = [COINBASE_START, COINBASE_PRODUCT, COINBASE_CONNECT]
        self.numbers = {"ws": 0, "probe": 0}

    def _l2(self, channel, kind, ms, levels):
        updates = [{"side": side, "event_time": at(ms), "price_level": f"{price}.00",
                    "new_quantity": str(qty)} for side, price, qty in levels]  # fmt: skip
        event = {"type": kind, "product_id": "BTC-USD", "updates": updates}
        message = {"channel": "l2_data", "timestamp": at(ms),
                   "sequence_num": self.numbers[channel], "events": [event]}  # fmt: skip
        self.numbers[channel] += 1
        self.entries.append((channel, json.dumps(message)))
        return len(self.entries) - 1  # the record's seq

    def snapshot(self, ms, *levels):
        return self._l2("ws", "snapshot", ms, levels)

    def update(self, ms, *levels):
        return self._l2("ws", "update", ms, levels)

    def check(self, ms, *levels):
        if self.numbers["probe"] == 0:
            self.control(event="connect", connection="probe", url="wss://probe")
        return self._l2("probe", "snapshot", ms, levels)

    def control(self, **fields):
        self.entries.append(("recorder", fields))
        return len(self.entries) - 1

    def end(self):
        disconnect = self.control(event="disconnect", connection="ws", reason="session ended")
        self.control(event="stop")
        return disconnect

    def normalize(self):
        return normalize(log("coinbase", *self.entries))


START = (("bid", 100, 1), ("bid", 99, 2), ("bid", 98, 3),
         ("offer", 101, 1), ("offer", 102, 2), ("offer", 103, 3))  # fmt: skip
BATCH_50 = (("bid", 100, 5), ("offer", 104, 1))
BATCH_100 = (("bid", 99, 0), ("offer", 101, 2))
AFTER_50 = (("bid", 100, 5), ("bid", 99, 2), ("bid", 98, 3),
            ("offer", 101, 1), ("offer", 102, 2), ("offer", 103, 3), ("offer", 104, 1))  # fmt: skip


def book(*changes, base=AFTER_50):
    """``base`` with some levels changed (quantity 0 removes one)."""
    levels = {(side, price): qty for side, price, qty in base}
    for side, price, qty in changes:
        levels[(side, price)] = qty
    return tuple((side, price, qty) for (side, price), qty in levels.items() if qty)


def standard(probe_ms, probe_levels, *, probe_after=50):
    """Stream snapshot, the two batches, and a check snapshot received after
    the batch at ``probe_after`` ms (or after both)."""
    session = Session()
    session.snapshot(0, *START)
    session.update(50, *BATCH_50)
    seqs = {}
    if probe_after == 50:
        seqs["check"] = session.check(probe_ms, *probe_levels)
    seqs["b"] = session.update(100, *BATCH_100)
    if probe_after == 100:
        seqs["check"] = session.check(probe_ms, *probe_levels)
    seqs["disconnect"] = session.end()
    return session, seqs


def events_of(normalized):
    rows = normalized.rows.get(("stream_events", "coinbase", INSTRUMENT), [])
    return [r for r in rows if r["kind"] == COMPARED]


def details(normalized):
    return [json.loads(e["detail"]) for e in events_of(normalized)]


def reconstructed(normalized):
    rows = normalized.rows[("book_events", "coinbase", INSTRUMENT)]
    messages = list(messages_from_table(table_from_rows(BOOK_EVENTS, rows)))
    events = normalized.rows.get(("stream_events", "coinbase", INSTRUMENT), [])
    return verify_check_snapshots("coinbase", messages, events)


def assert_agree(normalized):
    """The adapter's events and the reconstructor's results say the same."""
    adapter = sorted(details(normalized), key=lambda d: d["snapshot_seq"])
    reconstructor = sorted((r.summary() for r in reconstructed(normalized)),
                           key=lambda d: d["snapshot_seq"])  # fmt: skip
    assert adapter == reconstructor
    return adapter


# --- on a batch time: exact on every level ------------------------------------------------


def test_a_snapshot_on_a_batch_time_that_equals_the_book_is_exact():
    session, seqs = standard(50, AFTER_50)
    (detail,) = assert_agree(session.normalize())
    assert detail == {
        "snapshot_seq": seqs["check"], "result": "exact", "reason": None, "levels": 7,
        "on_batch": True, "next_batch_levels": 2,
        "violations": 0, "violations_shallowest": None, "violations_deepest": None,
        "unverified": 0, "unverified_as_next_batch": 0,
        "unverified_shallowest": None, "unverified_deepest": None,
    }  # fmt: skip


def test_on_a_batch_time_even_the_next_batchs_levels_must_match():
    # The snapshot already shows the 100 ms batch's ask 101 -> 2: on a batch
    # time nothing may differ, so this is a mismatch at the best ask (rank 0).
    session, _ = standard(50, book(("offer", 101, 2)))
    (detail,) = assert_agree(session.normalize())
    assert (detail["result"], detail["violations"]) == ("mismatch", 1)
    assert (detail["violations_shallowest"], detail["violations_deepest"]) == (0, 0)
    (event,) = events_of(session.normalize())
    assert (event["expected"], event["observed"], event["connection"]) == (1, 0, "probe")


# --- between batches: exact except on the next batch's levels ------------------------------


def test_between_batches_the_next_batchs_levels_are_unverified_not_wrong():
    # At 75 ms the snapshot has the 100 ms batch's bid 99 removal already, and
    # ask 101 at a value the stream never shows (conflated within the batch).
    session, seqs = standard(75, book(("bid", 99, 0), ("offer", 101, 7)))
    (detail,) = assert_agree(session.normalize())
    assert detail["result"] == "explained"
    assert (detail["on_batch"], detail["next_batch_levels"], detail["violations"]) == (False, 2, 0)
    assert detail["unverified"] == 2 and detail["unverified_as_next_batch"] == 1
    # bid 99 ranks 1 (only bid 100 is better); ask 101 is the best ask.
    assert (detail["unverified_shallowest"], detail["unverified_deepest"]) == (0, 1)
    (result,) = reconstructed(session.normalize())
    assert result.matched and not result.exact
    # (side, price, after A, snapshot, after B)
    assert set(result.unverified) == {
        (Side.BUY, 9900, 200_000_000, None, None),
        (Side.SELL, 10100, 100_000_000, 700_000_000, 200_000_000),
    }


def test_between_batches_a_difference_outside_the_next_batch_is_a_mismatch():
    session, _ = standard(75, book(("bid", 98, 4)))
    (detail,) = assert_agree(session.normalize())
    assert (detail["result"], detail["violations"], detail["unverified"]) == ("mismatch", 1, 0)
    assert (detail["violations_shallowest"], detail["violations_deepest"]) == (2, 2)
    (result,) = reconstructed(session.normalize())
    assert result.differences == [(Side.BUY, 9800, 300_000_000, 400_000_000)]


def test_between_batches_equal_on_every_level_is_exact():
    session, _ = standard(75, AFTER_50)
    (detail,) = assert_agree(session.normalize())
    assert (detail["result"], detail["on_batch"]) == ("exact", False)


# --- when the adapter can decide -------------------------------------------------------------


def test_a_snapshot_received_after_the_next_batch_is_decided_at_its_own_record():
    session, seqs = standard(75, book(("offer", 101, 2)), probe_after=100)
    normalized = session.normalize()
    (event,) = events_of(normalized)
    assert event["seq"] == seqs["check"]
    assert assert_agree(normalized)[0]["result"] == "explained"


def test_a_snapshot_received_before_the_next_batch_is_decided_when_it_arrives():
    session, seqs = standard(75, book(("offer", 101, 2)), probe_after=50)
    normalized = session.normalize()
    (event,) = events_of(normalized)
    assert event["seq"] == seqs["b"]  # noticed at the batch that settled it
    (detail,) = assert_agree(normalized)
    assert (detail["snapshot_seq"], detail["result"]) == (seqs["check"], "explained")


def test_with_no_later_batch_before_the_stream_ends_every_level_must_match():
    session = Session()
    session.snapshot(0, *START)
    session.update(50, *BATCH_50)
    session.check(75, *book(("offer", 101, 2)))
    disconnect = session.end()
    normalized = session.normalize()
    (event,) = events_of(normalized)
    assert event["seq"] == disconnect
    (detail,) = assert_agree(normalized)
    assert detail["result"] == "mismatch"
    assert (detail["next_batch_levels"], detail["violations"]) == (0, 1)


def test_a_snapshot_older_than_the_streams_own_snapshot_is_not_compared():
    session = Session()
    session.snapshot(0, *START)
    session.check(-10, *START)
    session.update(50, *BATCH_50)
    session.end()
    (detail,) = assert_agree(session.normalize())
    assert (detail["result"], detail["reason"]) == ("not_compared", "before_stream_snapshot")
    assert detail["violations"] is None and detail["unverified"] is None


def test_a_snapshot_with_no_stream_book_is_not_compared():
    session = Session()
    session.check(0, *START)
    session.snapshot(10, *START)
    session.end()
    (detail,) = assert_agree(session.normalize())
    assert (detail["result"], detail["reason"]) == ("not_compared", "no_stream_book")


def test_after_a_stream_gap_snapshots_are_not_compared_until_the_next_stream_snapshot():
    session = Session()
    session.snapshot(0, *START)
    session.numbers["ws"] += 1  # a lost message
    session.update(50, *BATCH_50)
    session.check(75, *AFTER_50)
    session.update(100, *BATCH_100)
    session.end()
    (detail,) = details(session.normalize())
    assert (detail["result"], detail["reason"]) == ("not_compared", "stream_unreliable")


def test_a_snapshot_further_back_than_the_kept_history_is_not_compared():
    session = Session()
    session.snapshot(0, *START)
    seconds = CHECK_HISTORY_NS // 1_000_000_000 + 2
    for second in range(1, seconds + 1):
        session.update(1000 * second, ("bid", 97, second))
    session.check(500, *START)
    session.end()
    (detail,) = details(session.normalize())
    assert (detail["result"], detail["reason"]) == ("not_compared", "history_too_short")


def test_after_a_rejected_stream_message_snapshots_are_not_compared():
    session = Session()
    session.snapshot(0, *START)
    session.update(50, ("north", 100, 5))  # an unknown side: the record is rejected
    session.check(75, *AFTER_50)
    session.end()
    normalized = session.normalize()
    assert [r.reason for r in normalized.report.rejects] == ["malformed_message"]
    (detail,) = details(normalized)
    assert (detail["result"], detail["reason"]) == ("not_compared", "stream_unreliable")


@pytest.mark.parametrize("check_first", [True, False])
def test_a_stream_snapshot_as_the_next_message_leaves_nothing_to_compare(check_first):
    session = Session()
    session.snapshot(0, *START)
    session.update(50, *BATCH_50)
    if check_first:
        session.check(75, *AFTER_50)
    session.snapshot(100, *START)  # the venue resends its book on the same connection
    if not check_first:
        session.check(75, *AFTER_50)
    session.end()
    (detail,) = assert_agree(session.normalize())
    assert (detail["result"], detail["reason"]) == ("not_compared", "next_message_is_snapshot")


def test_a_snapshot_waiting_when_its_log_ends_unsealed_gets_no_event_in_the_next_log():
    """A log cut short (no disconnect or stop record) leaves a check snapshot
    waiting for its next batch. The next log fed to the same normalizer must
    not receive an event for it."""
    from quantagent.data.rawlog import RawRecord

    cut = Session()
    cut.snapshot(0, *START)
    cut.update(50, *BATCH_50)
    cut.check(75, *AFTER_50)  # no batch, disconnect or stop follows
    later, seqs = standard(50, AFTER_50)
    second = [RawRecord(r.recv_ts + 10**12, r.seq, r.venue, r.channel, r.payload)
              for r in log("coinbase", *later.entries)]  # fmt: skip
    normalized = normalize(log("coinbase", *cut.entries) + second)
    (event,) = events_of(normalized)  # the second log's own check, nothing for the first's
    assert json.loads(event["detail"])["snapshot_seq"] == seqs["check"]
    book_rows = normalized.rows[("book_events", "coinbase", INSTRUMENT)]
    assert event["log_id"] == book_rows[-1]["log_id"] != book_rows[0]["log_id"]


def test_a_reconnect_ends_the_stream_a_pending_snapshot_waited_for():
    session = Session()
    session.snapshot(0, *START)
    session.update(50, *BATCH_50)
    session.check(75, *AFTER_50)
    reconnect = session.control(event="connect", connection="ws", url="wss://again")
    session.numbers["ws"] = 0
    session.snapshot(200, *START)
    session.end()
    normalized = session.normalize()
    (event,) = events_of(normalized)
    assert event["seq"] == reconnect
    (detail,) = assert_agree(normalized)
    assert (detail["result"], detail["next_batch_levels"]) == ("exact", 0)


# --- recorded data ---------------------------------------------------------------------------


def test_the_coinbase_fixtures_snapshot_is_on_a_batch_time_and_exact():
    normalized = normalize(read_raw_log(fixture_path("coinbase")))
    (detail,) = assert_agree(normalized)
    assert (detail["result"], detail["on_batch"], detail["levels"]) == ("exact", True, 38_532)


def test_the_between_batches_fixture_differs_only_on_the_next_batch():
    """The 60-second recording of 2026-11-13 whose check snapshot differed on
    55 levels (roadmap Phase 2a): every one of them is in the next batch."""
    (path,) = sorted(BETWEEN_BATCHES.glob("*.qaraw.zst"))
    normalized = normalize(read_raw_log(path))
    (detail,) = assert_agree(normalized)
    assert detail["result"] == "explained" and detail["on_batch"] is False
    assert (detail["levels"], detail["next_batch_levels"], detail["violations"]) == (37_746, 93, 0)
    assert (detail["unverified"], detail["unverified_as_next_batch"]) == (55, 50)


def test_the_between_batches_fixture_is_documented():
    readme = (BETWEEN_BATCHES.parents[1] / "README.md").read_text(encoding="utf-8")
    (path,) = sorted(BETWEEN_BATCHES.glob("*.qaraw.zst"))
    assert path.stat().st_size < 1_500_000
    for word in (path.name, "public", "ADR 0014"):
        assert word in readme, word


def test_the_events_reach_the_store_and_read_back_through_pitsession(tmp_path):
    (path,) = sorted(BETWEEN_BATCHES.glob("*.qaraw.zst"))
    ingest(path, AppendOnlyStore(tmp_path / "store"))
    with PITSession(tmp_path / "store", as_of=INT64_MAX) as session:
        events = session.stream_events("coinbase", INSTRUMENT, 0, INT64_MAX).to_pylist()
        messages = list(messages_from_table(session.book_events("coinbase", INSTRUMENT, 0,
                                                                INT64_MAX)))  # fmt: skip
    stored = [json.loads(e["detail"]) for e in events if e["kind"] == COMPARED]
    assert stored == [r.summary() for r in verify_check_snapshots("coinbase", messages, events)]


# --- the quality check grades the events -------------------------------------------------------


def graded(tmp_path, normalized, edit=None):
    rows = {key: list(value) for key, value in normalized.rows.items()}
    if edit:
        key = ("stream_events", "coinbase", INSTRUMENT)
        rows[key] = [edit(r) if r["kind"] == COMPARED else r for r in rows[key]]
    root = store_from_rows(tmp_path / "store", rows)
    return run(check_independent_snapshots, root, venue="coinbase")


def test_an_exact_snapshot_is_counted_and_not_reported(tmp_path):
    result = graded(tmp_path, normalize(read_raw_log(fixture_path("coinbase"))))
    assert result.findings == []
    assert result.stats == {"snapshots": 1, "exact": 1, "explained": 0, "mismatched": 0,
                            "not_compared": 0, "sessions": 1,
                            "sessions_without_comparison": 0}  # fmt: skip


def test_a_snapshot_between_batches_is_information(tmp_path):
    (path,) = sorted(BETWEEN_BATCHES.glob("*.qaraw.zst"))
    result = graded(tmp_path, normalize(read_raw_log(path)))
    (finding,) = result.findings
    assert (finding.what, finding.severity, finding.dataset) == (
        "check_snapshot_between_batches",
        Severity.INFO,
        "stream_events",
    )
    assert "55 of the next batch's 93 levels" in finding.message
    assert result.stats["explained"] == 1


def test_a_mismatch_is_an_error_with_its_count_and_depth(tmp_path):
    def edit(row):
        changed = {"result": "mismatch", "violations": 3, "violations_shallowest": 4,
                   "violations_deepest": 90}  # fmt: skip
        detail = json.loads(row["detail"]) | changed
        return {**row, "expected": 3, "detail": json.dumps(detail)}

    result = graded(tmp_path, normalize(read_raw_log(fixture_path("coinbase"))), edit)
    (finding,) = result.findings
    assert (finding.what, finding.severity) == ("check_snapshot_mismatch", Severity.ERROR)
    assert "3 levels" in finding.message and "ranks 4 to 90" in finding.message
    assert result.stats["mismatched"] == 1


def test_a_snapshot_that_could_not_be_compared_is_information(tmp_path):
    def edit(row):
        detail = json.loads(row["detail"]) | {"result": "not_compared",
                                              "reason": "stream_unreliable"}  # fmt: skip
        return {**row, "expected": None, "observed": None, "detail": json.dumps(detail)}

    result = graded(tmp_path, normalize(read_raw_log(fixture_path("coinbase"))), edit)
    # The fixture's session has only this snapshot, so the session is a warning too.
    snapshot, session = result.findings
    assert (snapshot.what, snapshot.severity) == ("check_snapshot_not_compared", Severity.INFO)
    assert "stream_unreliable" in snapshot.message
    assert (session.what, session.severity) == ("no_check_snapshot_compared", Severity.WARNING)


# --- a session where no check snapshot could be compared (ADR 0015) ------------------------


def unreliable_session():
    """Two check snapshots, neither comparable: the stream lost a message."""
    session = Session()
    session.snapshot(0, *START)
    session.numbers["ws"] += 1  # a lost message: the stream book is unreliable from here
    session.update(50, *BATCH_50)
    session.check(75, *AFTER_50)
    session.update(100, *BATCH_100)
    session.check(150, *AFTER_50)
    session.end()
    return session


def test_a_session_where_no_check_snapshot_could_be_compared_is_a_warning(tmp_path):
    result = graded(tmp_path, unreliable_session().normalize())
    assert sorted((f.what, f.severity) for f in result.findings) == [
        ("check_snapshot_not_compared", Severity.INFO),
        ("check_snapshot_not_compared", Severity.INFO),
        ("no_check_snapshot_compared", Severity.WARNING),
    ]
    (warning,) = [f for f in result.findings if f.severity == Severity.WARNING]
    assert "none of this session's 2 check snapshots" in warning.message
    assert "stream_unreliable" in warning.message
    assert (result.stats["sessions"], result.stats["sessions_without_comparison"]) == (1, 1)


def test_one_compared_snapshot_is_enough_for_its_session(tmp_path):
    session = Session()
    session.snapshot(0, *START)
    session.check(-10, *START)  # before the stream's snapshot: not compared
    session.update(50, *BATCH_50)
    session.check(50, *AFTER_50)  # on a batch time: exact
    session.update(100, *BATCH_100)
    session.end()
    result = graded(tmp_path, session.normalize())
    assert [(f.what, f.severity) for f in result.findings] == [
        ("check_snapshot_not_compared", Severity.INFO)
    ]
    assert result.stats["sessions_without_comparison"] == 0


def test_each_session_is_judged_on_its_own(tmp_path):
    from quantagent.data.rawlog import RawRecord

    bad = log("coinbase", *unreliable_session().entries)
    good, _ = standard(50, AFTER_50)
    later = [RawRecord(r.recv_ts + 10**12, r.seq, r.venue, r.channel, r.payload)
             for r in log("coinbase", *good.entries)]  # fmt: skip
    normalized = normalize(bad + later)
    result = graded(tmp_path, normalized)
    (warning,) = [f for f in result.findings if f.severity == Severity.WARNING]
    first_log = normalized.rows[("book_events", "coinbase", INSTRUMENT)][0]["log_id"]
    assert warning.log_id == first_log
    assert (result.stats["sessions"], result.stats["sessions_without_comparison"]) == (2, 1)


@pytest.mark.local_data
def test_the_thirty_minute_probe_recording_agrees_and_has_no_mismatch():
    """data/raw-probes (not in git): 28 check snapshots, 60 s apart."""
    (path,) = sorted(PROBES.glob("*.qaraw.zst"))
    normalized = normalize(read_raw_log(path))
    results = [d["result"] for d in assert_agree(normalized)]
    assert len(results) == 28
    assert results.count("exact") == 6 and results.count("explained") == 22
