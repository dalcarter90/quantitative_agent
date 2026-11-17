"""The reference book's resume mode (ADR 0020, review D2).

On an integrity failure the book goes stale and replay continues; updates are
not applied while it is stale, and the next stream snapshot that passes the
venue's check recovers it. Strict mode, the default, raises at exactly the
step where resume mode goes stale. ``trace`` yields every step the C++ book
will be given (stream messages, stale points from stored events, resets), so
Phase 4 can compare the two step by step.
"""

import dataclasses

import pyarrow.parquet as pq
import pytest
from _builders import INT64_MAX, LOG
from damage import book_updates, is_heartbeat, original_seq, removed, write
from integrity import (
    CASES,
    committed,
    damaged,
    from_records,
    is_point,
    normalized,
    points,
    resnapshotted,
    resume_steps,
)
from test_book import (
    BUY,
    CHECK,
    DELTA,
    SELL,
    SNAP,
    K,
    coinbase,
    coinbase_records,
    event,
    kraken_records,
    msg,
    sides,
)
from test_fixtures import INSTRUMENT, fixture_path

from quantagent.data import book as book_module
from quantagent.data.adapters import ingest
from quantagent.data.book import (
    RESET_REASONS,
    STALE_REASONS,
    BookIntegrityError,
    ChecksumMismatch,
    DeltaBeforeSnapshot,
    ReferenceBook,
    SequenceBreak,
    StreamFault,
    golden_table,
    kraken_checksum,
    replay,
    trace,
    verify_check_snapshots,
)
from quantagent.data.schema import StreamEventKind
from quantagent.data.store import AppendOnlyStore


def resumed(venue, messages, events=(), **options):
    """(step, seq, reason, stale, recovered, book) for every step, resume mode."""
    return [
        (s.step, s.seq, s.reason, s.stale, s.recovered, None if b is None else sides(b))
        for s, b in trace(venue, messages, events, mode="resume", **options)
    ]


def checksum_of(*levels):
    book = ReferenceBook()
    book.replace(levels)
    return kraken_checksum(book)


# --- the rules, on hand-built streams ----------------------------------------------------


def test_a_stored_gap_makes_the_book_stale_until_the_next_snapshot():
    messages = [
        msg(0, SNAP, (BUY, 100, 5), (SELL, 101, 3)),
        msg(1, DELTA, (BUY, 99, 1)),
        msg(2, DELTA, (BUY, 98, 1)),  # the gap was noticed at this record
        msg(3, DELTA, (BUY, 97, 1)),
        msg(4, SNAP, (BUY, 50, 1), (SELL, 60, 2)),
        msg(5, DELTA, (BUY, 49, 1)),
    ]
    gap = event(2, K.SEQUENCE_GAP, expected=3, observed=5)
    assert resumed("coinbase", messages, [gap]) == [
        ("message", 0, None, False, False, ({100: 5}, {101: 3})),
        ("message", 1, None, False, False, ({100: 5, 99: 1}, {101: 3})),
        ("stale", 2, "sequence_gap", True, False, None),  # before the record's own message
        ("message", 2, None, True, False, None),  # not applied
        ("message", 3, None, True, False, None),
        ("message", 4, None, False, True, ({50: 1}, {60: 2})),  # recovered
        ("message", 5, None, False, False, ({50: 1, 49: 1}, {60: 2})),
    ]


def _good_kraken():
    return checksum_of((BUY, 100, 5), (SELL, 101, 3))


# Every way the stream can be found broken. Each case: venue, messages, stored
# events, verify, the reason resume mode records, and the error strict raises.
FAILURES = {
    "stored_gap": ("coinbase", coinbase(2, 9, 10),
                   [event(1, K.SEQUENCE_GAP, expected=3, observed=9)], True,
                   "sequence_gap", SequenceBreak),
    "stored_repeat": ("coinbase", coinbase(2, 3, 3),
                      [event(2, K.SEQUENCE_REPEAT, expected=4, observed=3)], True,
                      "sequence_repeat", SequenceBreak),
    "stored_out_of_order": ("coinbase", coinbase(2, 5, 4),
                            [event(2, K.SEQUENCE_OUT_OF_ORDER, expected=6, observed=4)], True,
                            "sequence_out_of_order", SequenceBreak),
    "stored_checksum": ("kraken",
                        [msg(0, SNAP, (BUY, 100, 5), (SELL, 101, 3), checksum=_good_kraken(),
                             depth=10),
                         msg(1, DELTA, (BUY, 100, 6), checksum=12345, depth=10)],
                        [event(1, K.CHECKSUM_MISMATCH, expected=12345, observed=999)], False,
                        "checksum_mismatch", ChecksumMismatch),
    "stored_rejected": ("coinbase", coinbase(2, 3, 4),
                        [event(2, K.MESSAGE_REJECTED, detail="invalid_json: Expecting value")],
                        True, "message_rejected", StreamFault),
    "stored_after_the_last_message": ("coinbase", coinbase(2, 3),
                                      [event(7, K.SEQUENCE_GAP, expected=5, observed=7)], True,
                                      "sequence_gap", SequenceBreak),
    "own_checksum": ("kraken",
                     [msg(0, SNAP, (BUY, 100, 5), (SELL, 101, 3), checksum=_good_kraken(),
                          depth=10),
                      msg(1, DELTA, (BUY, 100, 6), checksum=_good_kraken(), depth=10)],
                     [], True, "checksum_mismatch", ChecksumMismatch),
    "own_repeat": ("coinbase", coinbase(2, 3, 3), [], True, "sequence_repeat", SequenceBreak),
    "own_out_of_order": ("coinbase", coinbase(2, 5, 4), [], True, "sequence_out_of_order",
                         SequenceBreak),
    "delta_before_snapshot": ("coinbase", [msg(0, DELTA, (BUY, 100, 5))], [], True,
                              "delta_before_snapshot", DeltaBeforeSnapshot),
    "delta_after_a_reset": ("coinbase", coinbase(2, 3, 4), [event(2, K.CONNECT)], True,
                            "delta_before_snapshot", DeltaBeforeSnapshot),
}  # fmt: skip


@pytest.mark.parametrize("case", FAILURES, ids=str)
def test_resume_mode_goes_stale_exactly_where_strict_mode_raises(case):
    venue, messages, events, verify, reason, error_type = FAILURES[case]
    strict_books = []
    with pytest.raises(error_type) as raised:
        for message, state in replay(venue, messages, events, verify=verify):
            strict_books.append((message.seq, sides(state)))
    error = raised.value

    # The yielded book is live: copy its levels at each step.
    steps = [(s, None if b is None else sides(b))
             for s, b in trace(venue, messages, events, verify=verify, mode="resume")]  # fmt: skip
    first = next(i for i, (s, _) in enumerate(steps) if s.stale)
    stale = steps[first][0]
    assert (stale.log_id, stale.seq, stale.recv_ts) == (error.log_id, error.seq, error.recv_ts)
    assert stale.reason == reason
    assert type(stale.error) is type(error) and str(stale.error) == str(error)
    # A stored event is its own step; a failure found at a message is that message's.
    assert stale.step == ("stale" if case.startswith("stored") else "message")
    # Up to there, resume mode yields exactly what strict mode did.
    before = [(s.seq, b) for s, b in steps[:first] if s.step == "message"]
    assert before == strict_books
    assert steps[first][1] is None  # a stale book cannot be read


def test_every_step_after_a_failure_is_stale_until_a_snapshot_recovers():
    venue, messages, events, verify, *_ = FAILURES["stored_gap"]
    steps = list(trace(venue, messages, events, verify=verify, mode="resume"))
    after = steps[next(i for i, (s, _) in enumerate(steps) if s.stale) :]
    assert all(s.stale and book is None for s, book in after)
    assert not any(s.recovered for s, _ in steps)  # no snapshot came


def test_a_reset_empties_the_book_without_making_it_stale():
    messages = coinbase(2, 3) + [msg(2, SNAP, (BUY, 1, 1), (SELL, 2, 1), venue_seq=0)]
    assert resumed("coinbase", messages, [event(2, K.CONNECT)])[2:] == [
        ("reset", 2, "connect", False, False, None),
        ("message", 2, None, False, False, ({1: 1}, {2: 1})),  # a snapshot, not a recovery
    ]


def test_a_reset_neither_causes_nor_ends_staleness_only_a_snapshot_ends_it():
    messages = coinbase(2, 3, 4) + [msg(3, SNAP, (BUY, 1, 1), venue_seq=0)]
    stored = [event(1, K.SEQUENCE_GAP, expected=3, observed=5), event(3, K.DISCONNECT),
              {**event(3, K.CONNECT), "event_index": 1}]  # fmt: skip
    assert resumed("coinbase", messages, stored) == [
        ("message", 0, None, False, False, ({100: 5}, {101: 3})),
        ("stale", 1, "sequence_gap", True, False, None),
        ("message", 1, None, True, False, None),
        ("message", 2, None, True, False, None),
        ("reset", 3, "disconnect", True, False, None),
        ("reset", 3, "connect", True, False, None),
        ("message", 3, None, False, True, ({1: 1}, {})),
    ]


def test_a_snapshot_that_fails_its_own_check_does_not_recover():
    good = _good_kraken()
    messages = [
        msg(0, SNAP, (BUY, 100, 5), (SELL, 101, 3), checksum=good, depth=10),
        msg(1, DELTA, (BUY, 100, 6), checksum=good, depth=10),  # the venue's book differs
        msg(2, SNAP, (BUY, 90, 1), (SELL, 91, 1), checksum=good, depth=10),  # wrong for itself
        msg(3, DELTA, (BUY, 89, 1), checksum=good, depth=10),
        msg(4, SNAP, (BUY, 90, 1), (SELL, 91, 1), checksum=checksum_of((BUY, 90, 1), (SELL, 91, 1)),
            depth=10),
    ]  # fmt: skip
    assert [step[:5] for step in resumed("kraken", messages)] == [
        ("message", 0, None, False, False),
        ("message", 1, "checksum_mismatch", True, False),
        ("message", 2, "checksum_mismatch", True, False),  # applied, checked, still stale
        ("message", 3, None, True, False),  # an update is not applied to a stale book
        ("message", 4, None, False, True),
    ]


def test_only_stream_resets_and_failures_are_steps():
    harmless = [event(0, K.SESSION_START, connection=None), event(1, K.SUBSCRIPTION_ACK),
                event(1, K.TRADE_BACKFILL), event(2, K.CLOCK_OFFSET, connection=None),
                event(2, K.SEQUENCE_GAP, connection="probe", expected=1, observed=3),
                event(2, K.CONNECT, connection="probe"), event(2, K.SESSION_STOP, connection=None),
                event(2, K.CHECK_SNAPSHOT_COMPARED, connection="probe")]  # fmt: skip
    steps = resumed("coinbase", coinbase(2, 3, 4), harmless)
    assert [s[:3] for s in steps] == [("reset", 0, "session_start"), ("message", 0, None),
                                      ("message", 1, None), ("message", 2, None)]  # fmt: skip


def test_the_reasons_cover_exactly_the_events_the_book_acts_on():
    assert STALE_REASONS == {
        StreamEventKind.SEQUENCE_GAP: "sequence_gap",
        StreamEventKind.SEQUENCE_REPEAT: "sequence_repeat",
        StreamEventKind.SEQUENCE_OUT_OF_ORDER: "sequence_out_of_order",
        StreamEventKind.CHECKSUM_MISMATCH: "checksum_mismatch",
        StreamEventKind.MESSAGE_REJECTED: "message_rejected",
    }
    assert RESET_REASONS == {
        StreamEventKind.SESSION_START: "session_start",
        StreamEventKind.CONNECT: "connect",
        StreamEventKind.DISCONNECT: "disconnect",
    }


def test_strict_mode_is_the_default_and_unknown_modes_are_refused():
    venue, messages, events, *_ = FAILURES["stored_gap"]
    with pytest.raises(SequenceBreak):
        list(trace(venue, messages, events))
    with pytest.raises(SequenceBreak):
        list(replay(venue, messages, events))
    with pytest.raises(ValueError, match="mode"):
        list(trace(venue, messages, events, mode="lenient"))


def test_errors_that_are_not_integrity_failures_still_raise_in_resume_mode():
    with pytest.raises(ValueError, match="checksum"):  # nothing to verify against
        list(trace("kraken", [msg(0, SNAP, (BUY, 100, 5), depth=10)], mode="resume"))
    no_metadata = dataclasses.replace(msg(0, SNAP, (BUY, 100, 5), checksum=1, depth=10),
                                      price_precision=None)  # fmt: skip
    with pytest.raises(ValueError, match="metadata"):
        list(trace("kraken", [no_metadata], mode="resume"))
    with pytest.raises(ValueError, match="sequence"):
        list(trace("coinbase", [msg(0, SNAP, (BUY, 100, 5), venue_seq=None)], mode="resume"))


def test_check_snapshots_are_not_steps():
    messages = [msg(0, SNAP, (BUY, 100, 5), (SELL, 101, 3)), msg(1, CHECK, (BUY, 1, 1)),
                msg(2, DELTA, (BUY, 99, 1))]  # fmt: skip
    assert [s[:2] for s in resumed("coinbase", messages)] == [("message", 0), ("message", 2)]


def test_replay_in_resume_mode_yields_every_stream_message():
    venue, messages, events, *_ = FAILURES["stored_gap"]
    shown = [(m.seq, None if b is None else sides(b))
             for m, b in replay(venue, messages, events, mode="resume")]  # fmt: skip
    assert shown == [(0, ({100: 5}, {101: 3})), (1, None), (2, None)]


# --- the recorded fixtures --------------------------------------------------------------------


@pytest.fixture(scope="module", params=["kraken", "coinbase"])
def clean(request):
    venue = request.param
    records = list(read_fixture(venue))
    messages, events = from_records(records, venue)
    return venue, records, messages, events


def read_fixture(venue):
    from quantagent.data.rawlog import read_raw_log

    return read_raw_log(fixture_path(venue))


def top(book, levels=25):
    """The book's best levels, cheap enough to take after every step."""
    return None if book is None else (book.top(BUY, levels), book.top(SELL, levels))


def test_resume_mode_changes_nothing_on_a_clean_fixture(clean):
    venue, _, messages, events = clean
    strict = [(s, top(b)) for s, b in trace(venue, messages, events)]
    resume = [(s, top(b)) for s, b in trace(venue, messages, events, mode="resume")]
    assert strict == resume
    assert not any(s.stale or s.recovered or s.step == "stale" for s, _ in resume)
    resets = [(s.seq, s.reason) for s, _ in resume if s.step == "reset"]
    stored = [
        (e["seq"], RESET_REASONS[StreamEventKind(e["kind"])])
        for e in events
        if StreamEventKind(e["kind"]) in RESET_REASONS and e["connection"] in ("ws", None)
    ]
    assert resets == stored and len(resets) == 3  # session start, connect, disconnect


def test_a_resnapshot_alone_changes_no_book(clean):
    """The repair the integrity cases rely on: the inserted snapshot equals the
    book it replaces, so every later book is the same as without it."""
    venue, records, messages, events = clean
    repaired = resnapshotted(venue)
    (seq,) = [r.seq for r, o in zip(repaired, records, strict=True) if r != o]
    assert is_heartbeat(records[seq]) and not is_heartbeat(repaired[seq])
    repaired_messages, repaired_events = from_records(list(repaired), venue)
    inserted = next(m for m in repaired_messages if m.seq == seq)
    assert inserted.kind == SNAP
    # In lockstep: the repaired log has one more message, the inserted snapshot.
    clean_run = iter(replay(venue, messages, events))
    truth = None
    for message, state in replay(venue, repaired_messages, repaired_events):
        if message.seq == seq:
            # The clean replay is still just after the message before it: in full.
            assert sides(state) == sides(truth)
            continue
        original, truth = next(clean_run)
        assert original.key == message.key
        assert top(state) == top(truth), message.seq
        if message.seq % 500 == 0:
            assert sides(state) == sides(truth), message.seq
    assert next(clean_run, None) is None
    assert sides(state) == sides(truth)
    # No stored event about it: the venue's own checks pass on the new snapshot.
    assert [e for e in repaired_events if e["seq"] == seq] == []


# --- the integrity cases (golden/integrity-points.json) ----------------------------------------


@pytest.fixture(scope="module", params=CASES, ids=lambda c: c.name)
def case(request):
    return request.param


def test_the_committed_integrity_points_match_a_fresh_resume_replay(case):
    (stored,) = [c for c in committed()["cases"] if c["name"] == case.name]
    assert points(case) == stored


def test_the_committed_file_lists_every_case_once():
    data = committed()
    assert data["format"] == 1
    assert [c["name"] for c in data["cases"]] == [c.name for c in CASES]


def _strict_error(case):
    messages, events = normalized(case)
    try:
        for _ in replay(case.venue, messages, events):
            pass
    except BookIntegrityError as error:
        return error
    return None


def test_each_case_goes_stale_where_strict_mode_raises_and_recovers_at_the_resnapshot(case):
    steps = resume_steps(case)
    error = _strict_error(case)
    if case.name == "kraken-duplicated":  # a limit of Kraken's rule (Phase 2): never seen
        assert error is None and not any(s.stale for s in steps)
        return
    first = next(s for s in steps if s.stale)
    assert (first.log_id, first.seq) == (error.log_id, error.seq)
    assert type(first.error) is type(error)
    (recovered,) = [s for s in steps if s.recovered]
    resnap = dict(case.damage)["resnapshot"]
    shift = {"removed": -1, "duplicated": 1, "swapped": 0}
    ((name, damaged_seq),) = [d for d in case.damage if d[0] != "resnapshot"]
    assert recovered.seq == resnap + shift[name] * (resnap > damaged_seq)
    assert recovered.message.kind == SNAP
    # Stale from the first failure to the recovery, and never again after it.
    index = steps.index(first), steps.index(recovered)
    assert all(s.stale for s in steps[index[0] : index[1]])
    assert not any(s.stale for s in steps[index[1] :])


@pytest.mark.parametrize("name", ["kraken-removed", "coinbase-removed"])
def test_after_recovery_the_book_is_the_true_book_again(name):
    """After the recovering snapshot, the damaged log's book equals the
    undamaged log's at every message: nothing of the stale period remains."""
    case = next(c for c in CASES if c.name == name)
    clean_messages, clean_events = from_records(list(resnapshotted(case.venue)), case.venue)
    lost = dict(case.damage)["removed"]
    clean_run = replay(case.venue, clean_messages, clean_events)
    recovered, compared = False, 0
    messages, events = normalized(case)
    for step, state in trace(case.venue, messages, events, mode="resume"):
        recovered = recovered or step.recovered
        if not (recovered and step.step == "message"):
            continue
        original, truth = next(clean_run)
        while original.seq != original_seq(step.seq, lost):
            original, truth = next(clean_run)
        assert top(state) == top(truth), step.seq
        if step.recovered or compared % 200 == 0:
            assert sides(state) == sides(truth), step.seq
        compared += 1
        last = state, truth  # neither book changes after the final reset
    assert compared > 500
    assert sides(last[0]) == sides(last[1])  # and in full after the last message


def test_check_snapshots_after_a_failure_are_not_compared_in_resume_mode():
    """Agrees with the Coinbase adapter, which judges the same snapshot as not
    comparable (ADR 0014's ``stream_unreliable``) on the damaged log."""
    records = coinbase_records()
    lost = book_updates(records)[500].seq
    messages, events = from_records(removed(records, lost), "coinbase")
    with pytest.raises(SequenceBreak):
        verify_check_snapshots("coinbase", messages, events)
    (result,) = verify_check_snapshots("coinbase", messages, events, mode="resume")
    assert result.result == "not_compared" and result.not_compared == "stream_unreliable"
    (compared,) = [e for e in events if e["kind"] == StreamEventKind.CHECK_SNAPSHOT_COMPARED]
    assert '"reason":"stream_unreliable"' in compared["detail"]


def test_kraken_check_snapshots_after_a_failure_are_not_compared_in_resume_mode():
    records = kraken_records()
    messages, events = from_records(removed(records, 265), "kraken")
    (result,) = verify_check_snapshots("kraken", messages, events, mode="resume")
    assert result.not_compared == "stream_unreliable"


# --- golden tables with stale and recovered points --------------------------------------------


@pytest.mark.parametrize("name", ["kraken-removed", "coinbase-removed"])
def test_a_resume_golden_table_holds_the_stale_and_recovered_points(name):
    case = next(c for c in CASES if c.name == name)
    messages, stored = normalized(case)
    with pytest.raises(BookIntegrityError):
        golden_table(case.venue, messages, stored)
    table = golden_table(case.venue, messages, stored, mode="resume").to_pylist()
    # The yielded book is live: read its top at each step.
    steps = [(s, None if b is None else (b.best_bid(), b.best_ask()))
             for s, b in trace(case.venue, messages, stored, mode="resume")]  # fmt: skip
    assert len(table) == len(steps)
    for row, (step, top) in zip(table, steps, strict=True):
        assert (row["log_id"], row["seq"], row["recv_ts"]) == (step.log_id, step.seq, step.recv_ts)
        assert (row["step"], row["reason"]) == (step.step, step.reason)
        assert (row["stale"], row["recovered"]) == (step.stale, step.recovered)
        if top is None:
            assert row["bid_price_00"] is None and row["ask_price_00"] is None
        else:
            assert ((row["bid_price_00"], row["bid_qty_00"]),
                    (row["ask_price_00"], row["ask_qty_00"])) == top  # fmt: skip
    shown = [{k: r[k] for k in ("seq", "step", "reason", "stale", "recovered")}
             for r, (s, _) in zip(table, steps, strict=True) if is_point(s)]  # fmt: skip
    (stored_points,) = [c["points"] for c in committed()["cases"] if c["name"] == case.name]
    assert shown == stored_points


# --- the command line ---------------------------------------------------------------------


@pytest.fixture(scope="module")
def damaged_store(tmp_path_factory):
    case = next(c for c in CASES if c.name == "coinbase-removed")
    root = tmp_path_factory.mktemp("damaged")
    ingest(write(list(damaged(case)), root / "damaged.qaraw"), AppendOnlyStore(root / "store"))
    return root / "store"


def _arguments(command, store, *extra):
    return [command, "--store", str(store), "--venue", "coinbase", "--instrument", INSTRUMENT,
            *extra]  # fmt: skip


def test_verify_stays_strict_by_default(damaged_store, capsys):
    assert book_module.main(_arguments("verify", damaged_store)) == 1
    output = capsys.readouterr().out
    assert "FAILED" in output and "gap" in output and "recovered" not in output


def test_verify_resume_reports_every_stale_period_and_its_recovery(damaged_store, capsys):
    assert book_module.main(_arguments("verify", damaged_store, "--resume")) == 1
    output = capsys.readouterr().out
    case = committed()["cases"][[c.name for c in CASES].index("coinbase-removed")]
    stale, recovered = (next(p for p in case["points"] if p[k]) for k in ("stale", "recovered"))
    assert f"stale from seq {stale['seq']}: sequence_gap" in output
    assert f"recovered at seq {recovered['seq']}" in output
    assert f"1 stale period, 1 recovered; {case['stale_messages']} stream messages not applied" in (
        output
    )
    assert "not compared (stream_unreliable)" in output
    assert "never crossed" in output and output.rstrip().endswith("FAILED")


def test_golden_is_strict_unless_asked_to_resume(damaged_store, tmp_path, capsys):
    out = tmp_path / "golden.parquet"
    assert book_module.main(_arguments("golden", damaged_store, "--out", str(out))) == 1
    assert "FAILED" in capsys.readouterr().out and not out.exists()
    assert book_module.main(_arguments("golden", damaged_store, "--out", str(out), "--resume")) == 0
    table = pq.read_table(out).to_pylist()
    assert any(r["step"] == "stale" for r in table) and sum(r["recovered"] for r in table) == 1


def test_verify_resume_on_a_clean_store_is_ok(tmp_path, capsys):
    ingest(fixture_path("kraken"), AppendOnlyStore(tmp_path / "store"))
    arguments = ["verify", "--store", str(tmp_path / "store"), "--venue", "kraken",
                 "--instrument", INSTRUMENT, "--resume"]  # fmt: skip
    assert book_module.main(arguments) == 0
    output = capsys.readouterr().out
    assert "0 stale periods" in output and output.rstrip().endswith("OK")


def test_the_store_read_and_direct_normalizing_give_the_same_trace(damaged_store):
    from quantagent.data.book import messages_from_table
    from quantagent.data.session import PITSession

    case = next(c for c in CASES if c.name == "coinbase-removed")
    with PITSession(damaged_store, available_at=INT64_MAX) as session:
        table = session.book_events("coinbase", INSTRUMENT, 0, INT64_MAX)
        messages = list(messages_from_table(table, session.instruments("coinbase", INSTRUMENT)))
        events = session.stream_events("coinbase", INSTRUMENT, 0, INT64_MAX).to_pylist()
    from_store = [s for s, _ in trace("coinbase", messages, events, mode="resume")]
    assert from_store == resume_steps(case)
    assert LOG not in {s.log_id for s in from_store}
