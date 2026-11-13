"""The reference order book reconstructor (roadmap Phase 2).

Hand-built sequences with hand-computed books first, then the venues'
integrity rules, then the recorded fixtures and damaged copies of them.
"""

import dataclasses
import zlib
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from _builders import INT64_MAX
from damage import book_updates, duplicated, removed, sequence_number, swapped, write
from test_fixtures import FIXTURES, INSTRUMENT, fixture_path

from quantagent.data import book as book_module
from quantagent.data.adapters import ingest, normalize
from quantagent.data.book import (
    GOLDEN_LEVELS,
    BookMessage,
    ChecksumMismatch,
    DeltaBeforeSnapshot,
    ReferenceBook,
    SequenceBreak,
    StreamFault,
    golden_table,
    kraken_checksum,
    messages_from_table,
    replay,
    verify_check_snapshots,
)
from quantagent.data.rawlog import read_raw_log
from quantagent.data.schema import BOOK_EVENTS, BookEventKind, Side, StreamEventKind
from quantagent.data.schema import table_from_rows as build
from quantagent.data.session import PITSession
from quantagent.data.store import AppendOnlyStore

BUY, SELL = Side.BUY, Side.SELL
SNAP, DELTA, CHECK = BookEventKind.SNAPSHOT, BookEventKind.DELTA, BookEventKind.CHECK_SNAPSHOT
K = StreamEventKind


FOLLOW = object()  # default venue sequence number: increasing with seq


def msg(seq, kind, *levels, venue_seq=FOLLOW, checksum=None, depth=None, event_ts=None):
    """A book message. Coinbase-style sequence numbers follow ``seq`` unless given."""
    return BookMessage(
        recv_ts=1_000 + seq,
        seq=seq,
        event_ts=event_ts if event_ts is not None else 500 + seq,
        kind=kind,
        venue_seq_last=seq + 1 if venue_seq is FOLLOW else venue_seq,
        checksum=checksum,
        depth=depth,
        levels=tuple(levels),
    )


def run(venue, messages, events=(), **options):
    """Replay and return the book after the last message."""
    book = None
    for _, book in replay(venue, messages, events, **options):  # noqa: B007
        pass
    return book


def sides(book):
    return book.levels(BUY), book.levels(SELL)


# --- the book itself ---------------------------------------------------------------


def test_a_snapshot_builds_the_book_and_deltas_insert_update_and_delete_on_both_sides():
    messages = [
        msg(0, SNAP, (BUY, 100, 5), (BUY, 99, 7), (SELL, 101, 3), (SELL, 103, 9)),
        msg(1, DELTA, (BUY, 98, 2)),  # insert a bid
        msg(2, DELTA, (SELL, 102, 4)),  # insert an ask
        msg(3, DELTA, (BUY, 100, 6)),  # update a bid
        msg(4, DELTA, (SELL, 103, 1)),  # update an ask
        msg(5, DELTA, (BUY, 99, 0)),  # delete a bid
        msg(6, DELTA, (SELL, 101, 0)),  # delete an ask
    ]
    states = [sides(book) for _, book in replay("coinbase", messages)]
    assert states[0] == ({100: 5, 99: 7}, {101: 3, 103: 9})
    assert states[1] == ({100: 5, 99: 7, 98: 2}, {101: 3, 103: 9})
    assert states[2] == ({100: 5, 99: 7, 98: 2}, {101: 3, 102: 4, 103: 9})
    assert states[3] == ({100: 6, 99: 7, 98: 2}, {101: 3, 102: 4, 103: 9})
    assert states[4] == ({100: 6, 99: 7, 98: 2}, {101: 3, 102: 4, 103: 1})
    assert states[5] == ({100: 6, 98: 2}, {101: 3, 102: 4, 103: 1})
    assert states[6] == ({100: 6, 98: 2}, {102: 4, 103: 1})


def test_deleting_a_level_that_is_not_there_changes_nothing():
    book = run("coinbase", [msg(0, SNAP, (BUY, 100, 5), (SELL, 101, 3)),
                            msg(1, DELTA, (BUY, 97, 0), (SELL, 150, 0))])  # fmt: skip
    assert sides(book) == ({100: 5}, {101: 3})


def test_levels_apply_in_message_order_so_the_last_change_to_a_price_wins():
    book = run(
        "coinbase",
        [
            msg(0, SNAP, (BUY, 100, 5), (SELL, 101, 3)),
            msg(1, DELTA, (BUY, 100, 0), (BUY, 100, 8), (SELL, 101, 4), (SELL, 101, 0)),
        ],
    )
    assert sides(book) == ({100: 8}, {})


def test_a_new_snapshot_replaces_the_whole_book():
    book = run(
        "coinbase",
        [msg(0, SNAP, (BUY, 100, 5), (SELL, 101, 3)), msg(1, DELTA, (BUY, 90, 1)),
         msg(2, SNAP, (BUY, 50, 1), (SELL, 60, 2))],
    )  # fmt: skip
    assert sides(book) == ({50: 1}, {60: 2})


def test_best_prices_top_levels_and_crossing():
    book = ReferenceBook()
    book.replace([(BUY, 100, 5), (BUY, 99, 7), (BUY, 98, 1), (SELL, 101, 3), (SELL, 103, 9)])
    assert book.best_bid() == (100, 5) and book.best_ask() == (101, 3)
    assert book.top(BUY, 2) == [(100, 5), (99, 7)]
    assert book.top(SELL, 5) == [(101, 3), (103, 9)]
    assert book.is_crossed() is False
    book.apply([(BUY, 101, 1)])
    assert book.is_crossed() is True  # locked counts: bid at the ask
    empty = ReferenceBook()
    assert empty.best_bid() is None and empty.best_ask() is None and empty.is_crossed() is False


def test_the_book_only_holds_integers_and_known_sides():
    book = ReferenceBook()
    with pytest.raises(TypeError):
        book.apply([(BUY, 100.0, 5)])
    with pytest.raises(TypeError):
        book.apply([(BUY, 100, True)])
    with pytest.raises(ValueError):
        book.apply([(Side.UNKNOWN, 100, 5)])
    with pytest.raises(ValueError):
        book.apply([(BUY, 100, -1)])


def test_a_delta_before_any_snapshot_raises():
    with pytest.raises(DeltaBeforeSnapshot) as raised:
        run("coinbase", [msg(0, DELTA, (BUY, 100, 5))])
    assert (raised.value.recv_ts, raised.value.seq) == (1_000, 0)


def test_check_snapshots_are_not_part_of_the_stream():
    messages = [msg(0, SNAP, (BUY, 100, 5), (SELL, 101, 3)),
                msg(1, CHECK, (BUY, 1, 1), (SELL, 2, 2)), msg(2, DELTA, (BUY, 99, 1))]  # fmt: skip
    applied = [m.seq for m, _ in replay("coinbase", messages)]
    assert applied == [0, 2]
    assert sides(run("coinbase", messages)) == ({100: 5, 99: 1}, {101: 3})
    with pytest.raises(DeltaBeforeSnapshot):
        run("coinbase", [msg(0, CHECK, (BUY, 1, 1)), msg(1, DELTA, (BUY, 9, 1))])


# --- Kraken: depth and checksum -------------------------------------------------------


def crc(asks, bids):
    """Kraken's rule written out by hand: asks ascending then bids descending,
    each as price digits then quantity digits."""
    return zlib.crc32("".join(f"{p}{q}" for p, q in [*asks, *bids]).encode())


def test_kraken_checksum_follows_the_documented_recipe():
    book = ReferenceBook()
    bids = [(BUY, 1000 - i, 10 + i) for i in range(12)]
    asks = [(SELL, 1001 + i, 20 + i) for i in range(12)]
    book.replace(bids + asks)
    expected = crc([(1001 + i, 20 + i) for i in range(10)], [(1000 - i, 10 + i) for i in range(10)])
    assert kraken_checksum(book) == expected


def test_kraken_book_is_cut_to_the_subscribed_depth_after_every_message():
    bids = [(BUY, 100 - i, 1) for i in range(12)]
    snapshot = ReferenceBook()
    snapshot.replace([*bids[:10], (SELL, 101, 1)])
    after_snapshot = kraken_checksum(snapshot)
    snapshot.apply([(BUY, 100, 0)])
    after_delete = kraken_checksum(snapshot)  # nine bids: the two below were cut

    messages = [
        msg(0, SNAP, *bids, (SELL, 101, 1), checksum=after_snapshot, depth=10),
        msg(1, DELTA, (BUY, 100, 0), checksum=after_delete, depth=10),
    ]
    book = run("kraken", messages)
    assert sorted(book.levels(BUY)) == [91, 92, 93, 94, 95, 96, 97, 98, 99]


def test_a_kraken_checksum_mismatch_raises_with_both_values_and_the_message():
    book = ReferenceBook()
    book.replace([(BUY, 100, 5), (SELL, 101, 3)])
    good = kraken_checksum(book)
    messages = [
        msg(0, SNAP, (BUY, 100, 5), (SELL, 101, 3), checksum=good, depth=10),
        msg(1, DELTA, (BUY, 100, 6), checksum=good, depth=10),  # the venue's book differs
    ]
    with pytest.raises(ChecksumMismatch) as raised:
        run("kraken", messages)
    error = raised.value
    book.apply([(BUY, 100, 6)])
    assert (error.recv_ts, error.seq) == (1_001, 1)
    assert (error.expected, error.computed) == (good, kraken_checksum(book))
    assert error.source == "computed"
    assert str(good) in str(error) and "seq 1" in str(error)


def test_a_kraken_message_without_a_checksum_cannot_be_verified_and_raises():
    with pytest.raises(ValueError, match="checksum"):
        run("kraken", [msg(0, SNAP, (BUY, 100, 5), depth=10)])


# --- Coinbase: sequence numbers ---------------------------------------------------------


def coinbase(*venue_seqs):
    first, *rest = venue_seqs
    return [msg(0, SNAP, (BUY, 100, 5), (SELL, 101, 3), venue_seq=first)] + [
        msg(i + 1, DELTA, (BUY, 99, i + 1), venue_seq=n) for i, n in enumerate(rest)
    ]


def test_coinbase_book_sequence_numbers_may_skip_because_other_channels_share_them():
    run("coinbase", coinbase(2, 3, 7, 8, 20))


@pytest.mark.parametrize(
    ("numbers", "kind", "expected", "received"),
    [((2, 3, 3), "repeat", 4, 3), ((2, 5, 4), "out_of_order", 6, 4)],
)
def test_coinbase_book_sequence_numbers_must_increase(numbers, kind, expected, received):
    with pytest.raises(SequenceBreak) as raised:
        run("coinbase", coinbase(*numbers))
    error = raised.value
    assert (error.kind, error.expected, error.received, error.source) == (
        kind,
        expected,
        received,
        "computed",
    )
    assert (error.recv_ts, error.seq) == (1_002, 2)


def test_a_snapshot_restarts_the_numbering():
    run("coinbase", coinbase(500, 501) + [msg(5, SNAP, (BUY, 1, 1), venue_seq=2)])


def test_a_coinbase_message_without_a_sequence_number_cannot_be_verified_and_raises():
    with pytest.raises(ValueError, match="sequence"):
        run("coinbase", [msg(0, SNAP, (BUY, 100, 5), venue_seq=None)])


# --- stored stream events ---------------------------------------------------------------


def event(seq, kind, connection="ws", expected=None, observed=None, detail=None):
    return {
        "recv_ts": 1_000 + seq,
        "seq": seq,
        "event_index": 0,
        "kind": int(kind),
        "connection": connection,
        "expected": expected,
        "observed": observed,
        "detail": detail,
    }


@pytest.mark.parametrize(
    ("kind", "name"),
    [(K.SEQUENCE_GAP, "gap"), (K.SEQUENCE_REPEAT, "repeat"),
     (K.SEQUENCE_OUT_OF_ORDER, "out_of_order")],
)  # fmt: skip
def test_a_stored_sequence_event_raises(kind, name):
    with pytest.raises(SequenceBreak) as raised:
        run("coinbase", coinbase(2, 9, 10), [event(1, kind, expected=3, observed=9)])
    error = raised.value
    assert (error.kind, error.expected, error.received, error.source) == (
        name,
        3,
        9,
        "stream_event",
    )
    assert (error.recv_ts, error.seq) == (1_001, 1)


def test_a_stored_checksum_mismatch_raises_even_without_own_verification():
    book = ReferenceBook()
    book.replace([(BUY, 100, 5), (SELL, 101, 3)])
    good = kraken_checksum(book)
    messages = [msg(0, SNAP, (BUY, 100, 5), (SELL, 101, 3), checksum=good, depth=10),
                msg(1, DELTA, (BUY, 100, 6), checksum=12345, depth=10)]  # fmt: skip
    stored = [event(1, K.CHECKSUM_MISMATCH, expected=12345, observed=999)]
    with pytest.raises(ChecksumMismatch) as raised:
        run("kraken", messages, stored, verify=False)
    assert (raised.value.expected, raised.value.computed, raised.value.source) == (
        12345,
        999,
        "stream_event",
    )
    run("kraken", messages, (), verify=False)  # nothing to go on: no error


def test_a_rejected_message_on_the_stream_raises():
    with pytest.raises(StreamFault, match="invalid_json"):
        run("coinbase", coinbase(2, 3, 4),
            [event(2, K.MESSAGE_REJECTED, detail="invalid_json: Expecting value")])  # fmt: skip


def test_events_on_the_probe_connection_do_not_concern_the_stream():
    stored = [
        event(1, K.SEQUENCE_GAP, connection="probe", expected=1, observed=3),
        event(1, K.CHECKSUM_MISMATCH, connection="probe", expected=1, observed=2),
        event(1, K.MESSAGE_REJECTED, connection="probe", detail="x"),
    ]
    run("coinbase", coinbase(2, 3, 4), stored)


def test_a_reconnect_means_the_next_message_must_be_a_snapshot():
    with pytest.raises(DeltaBeforeSnapshot) as raised:
        run("coinbase", coinbase(2, 3, 4), [event(2, K.CONNECT)])
    assert raised.value.seq == 2
    after = coinbase(2, 3) + [msg(3, SNAP, (BUY, 1, 1), venue_seq=0)]
    assert sides(run("coinbase", after, [event(2, K.DISCONNECT), event(3, K.CONNECT)])) == (
        {1: 1},
        {},
    )


def test_harmless_events_are_ignored():
    stored = [event(0, K.SESSION_START, connection=None), event(1, K.SUBSCRIPTION_ACK),
              event(1, K.TRADE_BACKFILL), event(2, K.CLOCK_OFFSET, connection=None)]  # fmt: skip
    run("coinbase", coinbase(2, 3, 4), stored)


# --- grouping stored rows into messages ------------------------------------------------------


def row(seq, level_index, **overrides):
    values = {
        "event_ts": 500 + seq, "event_ts_source": "venue", "recv_ts": 1_000 + seq, "seq": seq,
        "kind": DELTA, "venue_seq_first": None, "venue_seq_last": 10 + seq, "checksum": None,
        "depth": None, "level_index": level_index, "side": BUY, "price": 100 + level_index,
        "qty": 1,
    }  # fmt: skip
    values.update(overrides)
    return values


def test_rows_are_grouped_into_messages_in_level_order():
    rows = [row(0, 1, kind=SNAP), row(0, 0, kind=SNAP, side=SELL), row(1, 0)]
    (first, second) = messages_from_table(build(BOOK_EVENTS, rows))
    assert first == BookMessage(
        recv_ts=1_000, seq=0, event_ts=500, kind=SNAP, venue_seq_last=10, checksum=None,
        depth=None, levels=((SELL, 100, 1), (BUY, 101, 1)),
    )  # fmt: skip
    assert second.levels == ((BUY, 100, 1),)


@pytest.mark.parametrize(
    "rows",
    [
        [row(0, 0), row(0, 2)],  # a level is missing
        [row(0, 0), row(0, 0)],  # a level appears twice
        [row(0, 0), row(0, 1, kind=SNAP)],  # one message, two kinds
        [row(0, 0), row(0, 1, checksum=5)],  # one message, two checksums
    ],
)
def test_inconsistent_messages_are_refused(rows):
    with pytest.raises(ValueError):
        list(messages_from_table(build(BOOK_EVENTS, rows)))


# --- the recorded fixtures -----------------------------------------------------------------------


def from_records(records, venue):
    normalized = normalize(records)
    rows = normalized.rows[("book_events", venue, INSTRUMENT)]
    events = normalized.rows.get(("stream_events", venue, INSTRUMENT), [])
    return list(messages_from_table(build(BOOK_EVENTS, rows))), events


@pytest.fixture(scope="module", params=["kraken", "coinbase"])
def recorded(request):
    venue = request.param
    records = list(read_raw_log(fixture_path(venue)))
    messages, events = from_records(records, venue)
    return venue, records, messages, events


def test_the_fixture_replays_with_every_integrity_rule_satisfied(recorded):
    venue, _, messages, events = recorded
    stream = [m for m in messages if m.kind != CHECK]
    applied = 0
    for message, state in replay(venue, messages, events):
        applied += 1
        # Never crossed after any event: the Phase 1d crossed-book rule, per event.
        assert not state.is_crossed(), message.seq
    assert applied == len(stream) > 2_000


def test_every_check_snapshot_equals_the_rebuilt_book_on_every_level(recorded):
    venue, _, messages, events = recorded
    results = verify_check_snapshots(venue, messages, events)
    assert len(results) == sum(m.kind == CHECK for m in messages) >= 1
    for result in results:
        assert result.matched, result
        assert result.levels_compared == len(result.snapshot.levels)
        assert result.differences == []
        assert result.stream_messages_applied > 0


def test_a_check_snapshot_that_differs_is_reported_level_by_level(recorded):
    venue, _, messages, events = recorded
    altered = []
    for message in messages:
        if message.kind == CHECK:
            (side, price, qty), *rest = message.levels
            message = dataclasses.replace(message, levels=((side, price, qty + 1), *rest))
        altered.append(message)
    (result, *_) = verify_check_snapshots(venue, altered, events)
    assert result.matched is False
    assert result.differences == [(side, price, qty, qty + 1)]


# --- damaged fixtures: what each venue's rule catches, and what it cannot ----------------------


# Update indexes spread across the Kraken fixture. Whether a lost update is
# caught depends on whether it touched the ten best levels, which varies a lot
# along the recording: the first few hundred updates are mostly deeper.
SPREAD = list(range(250, 13_500, 1_117))


def kraken_records():
    return list(read_raw_log(fixture_path("kraken")))


def coinbase_records():
    return list(read_raw_log(fixture_path("coinbase")))


def outcome(records, venue, *, use_events=True, verify=True):
    """The integrity error replaying ``records`` raises, or None.

    ``use_events=False`` replays without the adapter's stored stream events, so
    only the reconstructor's own verification applies; ``verify=False`` turns
    its own verification off, so only the stored events apply.
    """
    messages, events = from_records(records, venue)
    try:
        run(venue, messages, events if use_events else (), verify=verify)
    except (ChecksumMismatch, SequenceBreak, DeltaBeforeSnapshot, StreamFault) as error:
        return error
    return None


def books_by_message(records, venue):
    """The book after every stream message, keyed by (recv_ts, seq)."""
    messages, events = from_records(records, venue)
    return {(m.recv_ts, m.seq): sides(book) for m, book in replay(venue, messages, events)}


def final_book(records, venue):
    messages, events = from_records(records, venue)
    return sides(run(venue, messages, events))


def first(candidates, predicate):
    for candidate in candidates:
        if predicate(candidate):
            return candidate
    raise AssertionError("no candidate found")


def test_kraken_a_lost_top_of_book_update_raises_at_the_next_message():
    records = kraken_records()
    updates = book_updates(records)

    def caught_at_once(i):
        error = outcome(removed(records, updates[i].seq), "kraken")
        return isinstance(error, ChecksumMismatch) and error.seq == updates[i + 1].seq

    i = first(SPREAD, caught_at_once)
    error = outcome(removed(records, updates[i].seq), "kraken")
    assert error.recv_ts == updates[i + 1].recv_ts
    assert error.expected != error.computed


def test_kraken_own_checksum_and_the_adapters_events_agree():
    records = kraken_records()
    updates = book_updates(records)
    agreed = 0
    for i in SPREAD[:12]:
        damaged = removed(records, updates[i].seq)
        own = outcome(damaged, "kraken", use_events=False)
        stored = outcome(damaged, "kraken", verify=False)
        assert type(own) is type(stored), i
        if own is None:
            continue  # neither saw it (see the test below)
        assert (own.recv_ts, own.seq, own.expected, own.computed) == (
            stored.recv_ts,
            stored.seq,
            stored.expected,
            stored.computed,
        )
        assert (own.source, stored.source) == ("computed", "stream_event")
        agreed += 1
    assert agreed >= 6


def test_kraken_cannot_see_a_lost_update_below_the_top_ten_levels():
    """Documents a limit of the venue's rule: its checksum covers ten levels per
    side of a 25-level book. A lost update that never reaches the top ten
    leaves a wrong book and no error."""
    records = kraken_records()
    updates = book_updates(records)
    clean = books_by_message(records, "kraken")

    def unseen(i):
        try:
            damaged = books_by_message(removed(records, updates[i].seq), "kraken")
        except ChecksumMismatch:
            return False
        return any(damaged[key] != clean[key] for key in damaged)

    i = first(SPREAD, unseen)
    assert outcome(removed(records, updates[i].seq), "kraken") is None
    # The wrong book later heals: a newer update overwrites the level.
    damaged = books_by_message(removed(records, updates[i].seq), "kraken")
    wrong = [key for key in damaged if damaged[key] != clean[key]]
    assert wrong and wrong[-1] != max(damaged)


def test_kraken_cannot_see_a_repeated_update():
    """Documents a limit: Kraken sends absolute quantities, so applying an update
    twice leaves the same book, and nothing reveals the repeat."""
    records = kraken_records()
    target = book_updates(records)[300]
    damaged = duplicated(records, target.seq)
    assert outcome(damaged, "kraken") is None
    assert final_book(damaged, "kraken") == final_book(records, "kraken")


def test_kraken_two_updates_in_the_wrong_order_raise():
    records = kraken_records()
    updates = book_updates(records)
    target = first(
        [updates[i] for i in SPREAD],
        lambda u: isinstance(outcome(swapped(records, u.seq), "kraken"), ChecksumMismatch),
    )
    error = outcome(swapped(records, target.seq), "kraken")
    # Caught where the first of the pair now carries the other's checksum, or
    # one message later if that one happened to agree.
    partner = next(r for r in records if r.seq > target.seq and r.channel == "ws")
    assert error.seq in (target.seq, partner.seq)


def test_coinbase_a_lost_message_raises_a_gap_with_the_sequence_numbers():
    records = coinbase_records()
    updates = book_updates(records)
    lost, following = updates[500], updates[501]
    error = outcome(removed(records, lost.seq), "coinbase")
    assert isinstance(error, SequenceBreak)
    assert (error.kind, error.source) == ("gap", "stream_event")
    assert (error.expected, error.received) == (sequence_number(lost), sequence_number(lost) + 1)
    assert error.seq == next(r.seq for r in records if r.seq > lost.seq and r.channel == "ws")
    assert following.seq >= error.seq


def test_coinbase_a_gap_is_invisible_from_the_book_messages_alone():
    """Documents why the stored events are needed: heartbeats and trades share
    the numbering, so the book's own numbers skip all the time."""
    records = coinbase_records()
    lost = book_updates(records)[500]
    assert outcome(removed(records, lost.seq), "coinbase", use_events=False) is None


def test_coinbase_a_repeated_message_raises():
    records = coinbase_records()
    target = book_updates(records)[500]
    error = outcome(duplicated(records, target.seq), "coinbase")
    assert isinstance(error, SequenceBreak) and error.kind == "repeat"
    assert (error.expected, error.received) == (
        sequence_number(target) + 1,
        sequence_number(target),
    )
    own = outcome(duplicated(records, target.seq), "coinbase", use_events=False)
    assert isinstance(own, SequenceBreak) and own.kind == "repeat" and own.source == "computed"


def test_coinbase_two_messages_in_the_wrong_order_raise():
    records = coinbase_records()
    target = book_updates(records)[500]
    error = outcome(swapped(records, target.seq), "coinbase")
    assert isinstance(error, SequenceBreak)
    assert error.kind == "gap"  # the later message arrives first
    assert error.seq == target.seq


# --- golden files for Phase 4 --------------------------------------------------------------------


GOLDEN = FIXTURES.parent / "golden"


@pytest.mark.parametrize("venue", ["kraken", "coinbase"])
def test_golden_file_matches_a_fresh_replay(venue):
    messages, events = from_records(list(read_raw_log(fixture_path(venue))), venue)
    fresh = golden_table(venue, messages, events)
    committed = pq.read_table(GOLDEN / f"{venue}-{INSTRUMENT}.top{GOLDEN_LEVELS}.parquet")
    assert committed.schema.equals(fresh.schema)
    assert committed.equals(fresh)


@pytest.mark.parametrize("venue", ["kraken", "coinbase"])
def test_golden_file_has_one_row_per_stream_message_with_the_top_of_book(venue):
    messages, events = from_records(list(read_raw_log(fixture_path(venue))), venue)
    table = golden_table(venue, messages, events)
    states = [(m, book.best_bid(), book.best_ask(), book.top(BUY, 20), book.top(SELL, 20))
              for m, book in replay(venue, messages, events)]  # fmt: skip
    assert table.num_rows == len(states)
    assert GOLDEN_LEVELS == 20
    names = table.schema.names
    assert names[:2] == ["recv_ts", "seq"]
    assert names[2:6] == ["bid_price_00", "bid_qty_00", "ask_price_00", "ask_qty_00"]
    assert len(names) == 2 + 4 * GOLDEN_LEVELS
    columns = table.to_pydict()
    for index in (0, 1, len(states) // 2, len(states) - 1):
        message, bid, ask, bids, asks = states[index]
        assert (columns["recv_ts"][index], columns["seq"][index]) == (message.recv_ts, message.seq)
        assert (columns["bid_price_00"][index], columns["bid_qty_00"][index]) == bid
        assert (columns["ask_price_00"][index], columns["ask_qty_00"][index]) == ask
        for rank in range(GOLDEN_LEVELS):
            want_bid = bids[rank] if rank < len(bids) else (None, None)
            want_ask = asks[rank] if rank < len(asks) else (None, None)
            got_bid = (
                columns[f"bid_price_{rank:02d}"][index],
                columns[f"bid_qty_{rank:02d}"][index],
            )
            got_ask = (
                columns[f"ask_price_{rank:02d}"][index],
                columns[f"ask_qty_{rank:02d}"][index],
            )
            assert (got_bid, got_ask) == (want_bid, want_ask)


def test_golden_files_are_small_and_documented():
    readme = (GOLDEN / "README.md").read_text(encoding="utf-8")
    for venue in ("kraken", "coinbase"):
        path = GOLDEN / f"{venue}-{INSTRUMENT}.top{GOLDEN_LEVELS}.parquet"
        assert path.stat().st_size < 3_000_000
        assert path.name in readme
    for text in ("recv_ts", "bid_price_00", "price mantissa", "python -m quantagent.data.book"):
        assert text in readme, text


# --- reading through PITSession, and the command line ----------------------------------------


@pytest.mark.parametrize("venue", ["kraken", "coinbase"])
def test_the_command_line_verifies_a_store_read_through_pitsession(venue, tmp_path, capsys):
    ingest(fixture_path(venue), AppendOnlyStore(tmp_path / "store"))
    arguments = ["verify", "--store", str(tmp_path / "store"), "--venue", venue,
                 "--instrument", INSTRUMENT]  # fmt: skip
    assert book_module.main(arguments) == 0
    output = capsys.readouterr().out
    assert "1 of 1 check snapshots match" in output
    assert "never crossed" in output
    assert "OK" in output


def test_the_command_line_reports_an_integrity_failure(tmp_path, capsys):
    records = coinbase_records()
    damaged = write(removed(records, book_updates(records)[500].seq), tmp_path / "gap.qaraw")
    ingest(damaged, AppendOnlyStore(tmp_path / "store"))
    arguments = ["verify", "--store", str(tmp_path / "store"), "--venue", "coinbase",
                 "--instrument", INSTRUMENT]  # fmt: skip
    assert book_module.main(arguments) == 1
    output = capsys.readouterr().out
    assert "FAILED" in output and "gap" in output


def test_the_command_line_writes_golden_files(tmp_path):
    ingest(fixture_path("kraken"), AppendOnlyStore(tmp_path / "store"))
    out = tmp_path / "golden.parquet"
    arguments = ["golden", "--store", str(tmp_path / "store"), "--venue", "kraken",
                 "--instrument", INSTRUMENT, "--out", str(out)]  # fmt: skip
    assert book_module.main(arguments) == 0
    committed = pq.read_table(GOLDEN / f"kraken-{INSTRUMENT}.top{GOLDEN_LEVELS}.parquet")
    assert pq.read_table(out).equals(committed)


def test_the_store_read_gives_the_same_messages_as_normalizing_directly(tmp_path):
    ingest(fixture_path("kraken"), AppendOnlyStore(tmp_path / "store"))
    direct, _ = from_records(kraken_records(), "kraken")
    with PITSession(tmp_path / "store", as_of=INT64_MAX) as session:
        stored = list(messages_from_table(session.book_events("kraken", INSTRUMENT, 0, INT64_MAX)))
    assert stored == direct


def test_the_reconstructor_reads_no_files_itself():
    source = Path(book_module.__file__).read_text(encoding="utf-8")
    for forbidden in ("duckdb", "read_raw_log", "rawlog", "AppendOnlyStore", "glob(", "open("):
        assert forbidden not in source, forbidden
    assert "PITSession" in source
    assert isinstance(pa.table({"a": [1]}), pa.Table)


# --- the one-hour recordings (local data, not in git) ---------------------------------------------


RECORDINGS = Path(__file__).parents[3] / "data" / "raw"


@pytest.mark.local_data
@pytest.mark.parametrize("venue", ["kraken", "coinbase"])
def test_the_one_hour_recordings_match_every_check_snapshot(venue, tmp_path):
    (path,) = sorted((RECORDINGS / venue / INSTRUMENT).glob("*.qaraw.zst"))
    ingest(path, AppendOnlyStore(tmp_path / "store"))
    with PITSession(tmp_path / "store", as_of=INT64_MAX) as session:
        messages = list(messages_from_table(session.book_events(venue, INSTRUMENT, 0, INT64_MAX)))
        events = session.stream_events(venue, INSTRUMENT, 0, INT64_MAX).to_pylist()
    results = verify_check_snapshots(venue, messages, events)
    assert len(results) == 6
    assert all(result.matched for result in results)
    crossed = sum(state.is_crossed() for _, state in replay(venue, messages, events))
    assert crossed == 0


def test_a_shallower_check_snapshot_is_compared_only_to_its_depth():
    # The stream keeps 12 levels; the independent snapshot was taken at depth 10.
    bids = [(BUY, 100 - i, 1) for i in range(12)]
    messages = [
        msg(0, SNAP, *bids, (SELL, 101, 1), depth=12),
        msg(1, CHECK, *bids[:10], (SELL, 101, 1), depth=10, event_ts=10_000),
    ]
    (result,) = verify_check_snapshots("coinbase", messages)
    assert result.matched and result.levels_compared == 11
    deeper = [messages[0], dataclasses.replace(messages[1], levels=(*bids[:11], (SELL, 101, 1)))]
    (result,) = verify_check_snapshots("coinbase", deeper)
    assert result.differences == [(BUY, 90, None, 1)]  # level 11 is outside depth 10


# --- replay order and wall-clock steps (ADR 0012) ------------------------------------------------


def test_a_backward_wall_clock_step_stops_the_replay():
    """Stored rows are ordered by (recv_ts, seq). That is arrival order only
    while the wall clock never goes back, so a backward step raises."""
    stored = [event(2, K.CLOCK_STEP, expected=None, observed=-1_241_000_000)]
    with pytest.raises(book_module.ReplayOrderUnreliable) as raised:
        run("coinbase", coinbase(2, 3, 4), stored)
    assert (raised.value.recv_ts, raised.value.seq, raised.value.step_ns) == (
        1_002,
        2,
        -1_241_000_000,
    )
    assert "1241.0 ms" in str(raised.value) and "ADR 0012" in str(raised.value)


def test_a_step_on_any_connection_counts_because_the_clock_is_shared():
    stored = [event(1, K.CLOCK_STEP, connection="probe", observed=-30_000_000)]
    with pytest.raises(book_module.ReplayOrderUnreliable):
        run("coinbase", coinbase(2, 3, 4), stored)


def test_a_forward_step_does_not_reorder_and_does_not_stop_the_replay():
    stored = [event(2, K.CLOCK_STEP, observed=300_000_000)]
    assert sides(run("coinbase", coinbase(2, 3, 4), stored)) == ({100: 5, 99: 2}, {101: 3})
