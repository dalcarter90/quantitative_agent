"""The C++ order book through the bindings (roadmap Phase 4b, ADR 0022).

The book is driven by the Phase 4a converter's events and compared with the
Python reference book after every step: on hand-built streams, on random
ones, on the recorded fixtures (against the committed golden files and
against the reference at every depth), on the eight integrity cases of
``golden/integrity-points.json``, and against the venues' own snapshots under
ADR 0014. What the reference cannot see (a capacity, events arriving one at a
time, an instrument update in the stream) is tested on its own.
"""

import functools
import itertools

import pyarrow.parquet as pq
import pytest
from _builders import LOG
from cppbook import (
    ALL,
    BUY,
    CHECKS,
    EFFECT,
    EV,
    INSTRUMENT_ID,
    KIND,
    NOT_STEPS,
    SELL,
    STALE,
    apply_all,
    apply_group,
    assert_same_steps,
    control_event,
    core_meta,
    depth_of,
    level_event,
    new_book,
    reference_sides,
    sides,
    snapshot_events,
    update_events,
)
from hypothesis import given, settings
from hypothesis import strategies as st
from integrity import CASES, KRAKEN_RESNAPSHOT, committed, is_point, resnapshotted
from test_adapters import log
from test_book import CHECK, DELTA, SNAP, K, coinbase, event, msg
from test_book_resume import FAILURES
from test_check_snapshots import BETWEEN_BATCHES
from test_fixtures import FIXTURES, INSTRUMENT
from test_instruments import PRECISION_CHANGE_LOG, kraken_book
from test_marketevents import case_tables, fixture_tables, instruments, stored, tables

from quantagent import _core
from quantagent.data.book import (
    GOLDEN_LEVELS,
    ReferenceBook,
    kraken_checksum,
    messages_from_table,
    trace,
    verify_check_snapshots,
)
from quantagent.data.rawlog import read_raw_log
from quantagent.data.schema import BOOK_EVENTS, INSTRUMENTS, Side, table_from_rows
from quantagent.marketevents import convert

SLOW = pytest.mark.slow  # over about 5 s (CLAUDE.md, "Working efficiently")
REASON = _core.StreamReason
GOLDEN = FIXTURES.parent / "golden"
# Plain digits: exponents and precisions 0, as test_book.msg's messages carry.
PLAIN = {"price_exp": 0, "qty_exp": 0, "price_precision": 0, "qty_precision": 0}


def camel(reason: str) -> str:
    return "".join(part.capitalize() for part in reason.split("_"))


def rows_of(messages):
    """The stored ``book_events`` rows of hand-built messages (``test_book.msg``),
    whose sides may be the C++ ``Side`` or the schema's: both are 1 and -1."""
    return [
        {"event_ts": m.event_ts, "event_ts_source": "venue", "recv_ts": m.recv_ts,
         "log_id": m.log_id, "seq": m.seq, "kind": m.kind, "venue_seq_first": None,
         "venue_seq_last": m.venue_seq_last, "checksum": m.checksum, "depth": m.depth,
         "level_index": index, "side": int(side), "price": price, "qty": qty}
        for m in messages
        for index, (side, price, qty) in enumerate(m.levels)
    ]  # fmt: skip


def hand_built(messages):
    """What a session would return for hand-built messages: book rows and one
    metadata version in force from the log's first record."""
    rows = rows_of(messages)
    table = table_from_rows(BOOK_EVENTS, rows) if rows else BOOK_EVENTS.schema.empty_table()
    return table, instruments(LOG, **PLAIN)


def same(venue, messages, events=(), **options):
    """The reference book and the C++ book agree after every step."""
    table, meta = hand_built(messages)
    return assert_same_steps(venue, table, list(events), meta, **options)


def drive(venue, messages, events=(), *, capacity=None):
    """The C++ book alone: its outcome for every step."""
    table, meta = hand_built(messages)
    book = new_book(venue, meta, depth=depth_of(table), capacity=capacity)
    groups = convert(table, list(events), meta, instrument=INSTRUMENT_ID)
    outcomes = [apply_group(book, group) for group in groups]
    return book, [o for o in outcomes if o.step not in NOT_STEPS]


def plain_meta(**changes):
    row = {**PLAIN, "tick_size": 1, "lot_size": 1, **changes}
    return core_meta(row)


def coinbase_book(capacity=None):
    capacity = _core.OrderBook.DEFAULT_CAPACITY if capacity is None else capacity
    return _core.OrderBook(plain_meta(), _core.BookCheck.CoinbaseSequence, capacity)


def kraken_crc(bids, asks, divisors=(1, 1)):
    """The reference's checksum of a book given as (price, quantity) lists."""
    book = ReferenceBook()
    book.replace([(Side.BUY, p, q) for p, q in bids] + [(Side.SELL, p, q) for p, q in asks])
    return kraken_checksum(book, divisors)


# --- the binding ---------------------------------------------------------------------------


def test_a_book_is_built_with_its_instrument_its_venues_rule_and_a_capacity():
    meta = core_meta({"price_exp": 8, "qty_exp": 8, "price_precision": 1, "qty_precision": 8,
                      "tick_size": 10_000_000, "lot_size": 1})  # fmt: skip
    book = _core.OrderBook(meta, _core.BookCheck.KrakenChecksum, 50)
    assert (book.capacity, book.check) == (50, _core.BookCheck.KrakenChecksum)
    assert (book.is_stale, book.has_book, book.stale_reason) == (False, False, STALE.NotStale)
    held = book.instrument
    assert (held.id.value, held.price_exp, held.qty_exp) == (INSTRUMENT_ID, 8, 8)
    assert (held.price_precision, held.qty_precision) == (1, 8)
    assert (held.tick_size.value, held.lot_size.value) == (10_000_000, 1)
    # The default is 65,536 levels a side; a cut book is sized from its depth (ADR 0022).
    default = _core.OrderBook(meta, _core.BookCheck.CoinbaseSequence)
    assert default.capacity == _core.OrderBook.DEFAULT_CAPACITY == 65_536
    assert _core.OrderBook.capacity_for_depth(25) == 50
    assert _core.OrderBook.capacity_for_depth(1_000) == 2_000


def test_a_capacity_no_book_can_have_is_refused():
    for bad in (0, _core.OrderBook.MAX_CAPACITY + 1):
        with pytest.raises(ValueError, match="capacity"):
            coinbase_book(bad)
    for bad in (-1, 2**32):
        with pytest.raises(OverflowError):
            coinbase_book(bad)
    with pytest.raises(TypeError):
        coinbase_book(50.0)
    assert coinbase_book(_core.OrderBook.MAX_CAPACITY).capacity == 2**20
    assert coinbase_book(1).capacity == 1


def test_the_books_enums_have_the_documented_numbers():
    assert {name: int(value) for name, value in STALE.__members__.items()} == {
        "NotStale": 0, "SequenceGap": 1, "SequenceRepeat": 2, "SequenceOutOfOrder": 3,
        "ChecksumMismatch": 4, "MessageRejected": 5, "DeltaBeforeSnapshot": 64,
        "IncompleteMessage": 65, "CapacityExceeded": 66, "InvalidEvent": 67,
    }  # fmt: skip
    # A StreamStale's reason is the book's reason of the same name and number.
    for name, value in REASON.__members__.items():
        if 1 <= int(value) <= 5:
            assert int(getattr(STALE, name)) == int(value)
    assert {name: int(value) for name, value in EFFECT.__members__.items()} == {
        "Pending": 0, "Applied": 1, "Recovered": 2, "Skipped": 3, "WentStale": 4, "Reset": 5,
        "Instrument": 6, "Ignored": 7,
    }  # fmt: skip
    assert {name: int(v) for name, v in _core.BookCheck.__members__.items()} == {
        "KrakenChecksum": 0, "CoinbaseSequence": 1}  # fmt: skip


def test_queries_return_integer_mantissas_best_first():
    book = coinbase_book()
    levels = [(BUY, 99, 7), (SELL, 103, 9), (BUY, 100, 5), (SELL, 101, 3)]
    effects = apply_all(book, snapshot_events(1, levels, venue_seq=4))
    assert effects == [EFFECT.Pending] * 5 + [EFFECT.Applied]
    assert book.top(BUY, 1) == [(100, 5)]
    assert book.top(BUY, 10) == [(100, 5), (99, 7)]
    assert book.top(SELL, ALL) == [(101, 3), (103, 9)]
    assert (book.best_bid(), book.best_ask()) == ((100, 5), (101, 3))
    assert (book.size(BUY), book.size(SELL)) == (2, 2)
    assert book.quantity_at(BUY, 99) == 7
    assert book.quantity_at(BUY, 98) is None and book.quantity_at(SELL, 99) is None
    assert all(type(v) is int for level in book.top(BUY, 2) for v in level)
    assert book.top(_core.Side.Unknown, 5) == [] and book.size(_core.Side.Unknown) == 0
    assert book.top(BUY, 0) == []
    result = book.apply(level_event(KIND.BookDelta, 2, 0, BUY, 98, 1, last=True, venue_seq=5))
    assert (result.effect, result.reason) == (EFFECT.Applied, STALE.NotStale)
    assert "Applied" in repr(result)
    for bad in (None, b"", 5):
        with pytest.raises(TypeError):
            book.apply(bad)
    with pytest.raises(TypeError):
        book.top(BUY, 1.0)
    with pytest.raises(TypeError):
        book.top(1, 5)
    with pytest.raises(TypeError):
        book.quantity_at(BUY, _core.Price(99))  # a mantissa, as the queries return


def test_a_book_with_no_snapshot_shows_nothing():
    book = coinbase_book()
    assert (book.has_book, book.is_stale) == (False, False)
    assert sides(book) == ([], []) and book.best_bid() is None and book.best_ask() is None
    assert book.size(BUY) == 0 and book.quantity_at(BUY, 100) is None


# --- the same as the reference book, on hand-built streams ----------------------------------


def test_a_snapshot_builds_the_book_and_updates_insert_change_and_remove_levels():
    messages = [
        msg(0, SNAP, (BUY, 100, 5), (BUY, 99, 7), (SELL, 101, 3), (SELL, 103, 9)),
        msg(1, DELTA, (BUY, 98, 2)),  # insert a bid
        msg(2, DELTA, (SELL, 102, 4)),  # insert an ask
        msg(3, DELTA, (BUY, 99, 1)),  # change a quantity
        msg(4, DELTA, (SELL, 103, 0)),  # remove an ask
        msg(5, DELTA, (BUY, 100, 0)),  # remove the best bid
        msg(6, DELTA, (BUY, 50, 0)),  # remove a level that is not there
        # Levels apply in the message's order: the last change to a price wins.
        msg(7, DELTA, (BUY, 97, 1), (BUY, 97, 3), (SELL, 104, 2), (SELL, 104, 0)),
    ]
    book, outcomes = same("coinbase", messages)
    assert sides(book) == ([(99, 1), (98, 2), (97, 3)], [(101, 3), (102, 4)])  # by hand
    assert (book.best_bid(), book.best_ask()) == ((99, 1), (101, 3))
    assert [o.effects[-1] for o in outcomes] == [EFFECT.Applied] * 8
    assert outcomes[7].effects == [EFFECT.Pending] * 3 + [EFFECT.Applied]


def test_a_new_snapshot_replaces_the_whole_book():
    messages = [msg(0, SNAP, (BUY, 100, 5), (SELL, 101, 3)), msg(1, DELTA, (BUY, 99, 1)),
                msg(2, SNAP, (BUY, 50, 1), (SELL, 60, 2), venue_seq=0)]  # fmt: skip
    book, outcomes = same("coinbase", messages)
    assert sides(book) == ([(50, 1)], [(60, 2)])
    assert not outcomes[-1].recovered  # a snapshot, not a recovery: nothing was stale


@pytest.mark.parametrize("case", FAILURES, ids=str)
def test_the_book_goes_stale_exactly_where_the_reference_does(case):
    """Every way the reference book finds the stream broken (test_book_resume),
    stored or by its own check: the same step, the same reason."""
    venue, messages, events, verify, reason, _ = FAILURES[case]
    book, outcomes = same(venue, messages, events, verify=verify)
    first = next(o for o in outcomes if o.stale)
    assert first.reason == reason
    assert first.step == ("stale" if case.startswith("stored") else "message")
    assert book.stale_reason == getattr(STALE, camel(reason))
    assert book.stale_count(getattr(STALE, camel(reason))) == 1
    assert (book.is_stale, book.has_book) == (True, False) and sides(book) == ([], [])


def test_a_stale_book_takes_no_updates_and_recovers_at_the_next_snapshot():
    messages = [
        msg(0, SNAP, (BUY, 100, 5), (SELL, 101, 3)),
        msg(1, DELTA, (BUY, 99, 1)),
        msg(2, DELTA, (BUY, 98, 1)),  # the gap was noticed at this record
        msg(3, DELTA, (BUY, 97, 1), (SELL, 102, 1)),
        msg(4, SNAP, (BUY, 50, 1), (SELL, 60, 2)),
        msg(5, DELTA, (BUY, 49, 1)),
    ]
    book, outcomes = same("coinbase", messages, [event(2, K.SEQUENCE_GAP, expected=3, observed=5)])
    assert [(o.step, o.seq, o.reason, o.stale, o.recovered) for o in outcomes] == [
        ("message", 0, None, False, False),
        ("message", 1, None, False, False),
        ("stale", 2, "sequence_gap", True, False),
        ("message", 2, None, True, False),
        ("message", 3, None, True, False),
        ("message", 4, None, False, True),
        ("message", 5, None, False, False),
    ]
    assert outcomes[2].effects == [EFFECT.WentStale]
    assert outcomes[4].effects == [EFFECT.Skipped, EFFECT.Skipped]
    assert outcomes[5].effects[-1] == EFFECT.Recovered
    assert (book.is_stale, book.stale_reason) == (False, STALE.NotStale)
    assert sides(book) == ([(50, 1), (49, 1)], [(60, 2)])


def test_a_reset_empties_the_book_and_neither_causes_nor_ends_staleness():
    messages = coinbase(2, 3, 4) + [
        msg(3, SNAP, (BUY, 1, 1), venue_seq=0),
        msg(4, DELTA, (BUY, 2, 2)),
    ]
    stored_events = [event(1, K.SEQUENCE_GAP, expected=3, observed=5), event(3, K.DISCONNECT),
                     {**event(3, K.CONNECT), "event_index": 1}, event(4, K.CONNECT)]  # fmt: skip
    book, outcomes = same("coinbase", messages, stored_events)
    assert [(o.step, o.seq, o.reason, o.stale, o.recovered) for o in outcomes] == [
        ("message", 0, None, False, False),
        ("stale", 1, "sequence_gap", True, False),
        ("message", 1, None, True, False),
        ("message", 2, None, True, False),
        ("reset", 3, "disconnect", True, False),  # still stale
        ("reset", 3, "connect", True, False),
        ("message", 3, None, False, True),  # only a snapshot ends it
        ("reset", 4, "connect", False, False),  # not stale, but no book
        ("message", 4, "delta_before_snapshot", True, False),
    ]
    assert outcomes[4].effects == [EFFECT.Reset] and outcomes[7].effects == [EFFECT.Reset]
    assert book.stale_count(STALE.DeltaBeforeSnapshot) == 1


def test_a_kraken_book_is_cut_to_its_depth_and_checked_after_every_message():
    bids = [(BUY, 100 - i, 1) for i in range(12)]
    after_snapshot = kraken_crc([(100 - i, 1) for i in range(10)], [(101, 1)])
    after_delete = kraken_crc([(99 - i, 1) for i in range(9)], [(101, 1)])  # the two below were cut
    messages = [msg(0, SNAP, *bids, (SELL, 101, 1), checksum=after_snapshot, depth=10),
                msg(1, DELTA, (BUY, 100, 0), checksum=after_delete, depth=10)]  # fmt: skip
    book, _ = same("kraken", messages)
    assert [price for price, _ in book.top(BUY, ALL)] == [99, 98, 97, 96, 95, 94, 93, 92, 91]
    assert book.checksum() == after_delete
    assert book.capacity == 20  # twice the depth


def test_a_snapshot_that_fails_its_own_check_does_not_recover_the_book():
    good = kraken_crc([(100, 5)], [(101, 3)])
    other = kraken_crc([(90, 1)], [(91, 1)])
    messages = [
        msg(0, SNAP, (BUY, 100, 5), (SELL, 101, 3), checksum=good, depth=10),
        msg(1, DELTA, (BUY, 100, 6), checksum=good, depth=10),  # the venue's book differs
        msg(2, SNAP, (BUY, 90, 1), (SELL, 91, 1), checksum=good, depth=10),  # wrong for itself
        msg(3, DELTA, (BUY, 89, 1), checksum=good, depth=10),
        msg(4, SNAP, (BUY, 90, 1), (SELL, 91, 1), checksum=other, depth=10),
    ]
    book, outcomes = same("kraken", messages)
    assert [(o.reason, o.stale, o.recovered) for o in outcomes] == [
        (None, False, False),
        ("checksum_mismatch", True, False),
        ("checksum_mismatch", True, False),  # applied, checked, still stale
        (None, True, False),
        (None, False, True),
    ]
    assert book.stale_count(STALE.ChecksumMismatch) == 2


def test_coinbase_numbers_may_skip_and_a_snapshot_restarts_them():
    same("coinbase", coinbase(2, 3, 7, 8, 20))  # other channels share the numbering
    restarted = [msg(5, SNAP, (BUY, 1, 1), venue_seq=2), msg(6, DELTA, (BUY, 2, 1), venue_seq=3)]
    book, outcomes = same("coinbase", coinbase(500, 501) + restarted)
    assert not any(o.stale for o in outcomes)


def test_events_no_book_acts_on_and_check_snapshots_change_nothing():
    harmless = [event(0, K.SESSION_START, connection=None), event(1, K.SUBSCRIPTION_ACK),
                event(2, K.SEQUENCE_GAP, connection="probe", expected=1, observed=3),
                event(2, K.CONNECT, connection="probe")]  # fmt: skip
    messages = [msg(0, SNAP, (BUY, 100, 5), (SELL, 101, 3)), msg(1, CHECK, (BUY, 1, 1)),
                msg(2, DELTA, (BUY, 99, 1))]  # fmt: skip
    book, outcomes = same("coinbase", messages, harmless)
    assert [(o.step, o.seq) for o in outcomes] == [("reset", 0), ("message", 0), ("message", 2)]
    assert sides(book) == ([(100, 5), (99, 1)], [(101, 3)])


# --- the same as the reference book, on random streams --------------------------------------

STALE_KINDS = [K.SEQUENCE_GAP, K.SEQUENCE_REPEAT, K.SEQUENCE_OUT_OF_ORDER, K.CHECKSUM_MISMATCH,
               K.MESSAGE_REJECTED]  # fmt: skip
RESET_KINDS = [K.SESSION_START, K.CONNECT, K.DISCONNECT]
OPS = ["update"] * 8 + ["snapshot"] * 3 + ["stale", "reset", "harmless", "check", "instrument"]


def _book_row(seq, index, kind, level, *, venue_seq, checksum, depth):
    side, price, qty = level
    return {"event_ts": 500 + seq, "event_ts_source": "venue", "recv_ts": 1_000 + seq,
            "log_id": LOG, "seq": seq, "kind": kind, "venue_seq_first": None,
            "venue_seq_last": venue_seq, "checksum": checksum, "depth": depth,
            "level_index": index, "side": side, "price": price, "qty": qty}  # fmt: skip


def _meta_row(seq, exps, precisions):
    return {"event_ts": 1_000 + seq, "event_ts_source": "receipt", "recv_ts": 1_000 + seq,
            "log_id": LOG, "seq": seq, "price_exp": exps[0], "qty_exp": exps[1],
            "price_precision": precisions[0], "qty_precision": precisions[1], "tick_size": 1,
            "lot_size": 1}  # fmt: skip


@st.composite
def streams(draw):
    """A venue's stored data for one log: snapshots, updates and check snapshots
    of a few levels at colliding prices, stored events the book acts on and
    ones it does not, and, on Kraken, precision changes. Checksums and message
    numbers are the venue's true ones, except where a fault is planted."""
    venue = draw(st.sampled_from(["kraken", "coinbase"]))
    exps = draw(st.sampled_from([(0, 0), (2, 1), (8, 8)]))
    precision = st.tuples(st.integers(0, exps[0]), st.integers(0, exps[1]))
    precisions = draw(precision)
    # Prices and quantities are small numbers times a unit, so that dividing by
    # 10^(exp - precision) gives varied digits; prices include zero and below.
    price_unit = draw(st.sampled_from([1, 7, 10 ** exps[0]]))
    qty_unit = draw(st.sampled_from([1, 3, 10 ** exps[1]]))
    depth = draw(st.sampled_from([1, 2, 3, 5, 10])) if venue == "kraken" else None
    level = st.tuples(st.sampled_from([Side.BUY, Side.SELL]),
                      st.integers(-2, 24).map(lambda p: p * price_unit),
                      st.integers(0, 4).map(lambda q: q * qty_unit))  # fmt: skip
    rows, events, versions = [], [], [_meta_row(0, exps, precisions)]
    venue_book = ReferenceBook()  # what the venue's book truly is, stale or not
    number = draw(st.integers(0, 3))
    seq = 0
    for _ in range(draw(st.integers(1, 30))):
        seq += 1
        op = draw(st.sampled_from(OPS))
        if op == "stale":
            events.append(stored(seq, draw(st.sampled_from(STALE_KINDS)), expected=5, observed=7))
            if draw(st.booleans()):
                continue  # on a record of its own; otherwise at the next message's record
            op = "update"
        elif op == "reset":
            kind = draw(st.sampled_from(RESET_KINDS))
            events.append(stored(seq, kind, connection=None if kind == K.SESSION_START else "ws"))
            if draw(st.booleans()):
                continue
            op = draw(st.sampled_from(["snapshot", "update"]))
        elif op == "harmless":
            events.append(stored(seq, K.SEQUENCE_GAP, connection="probe", expected=1, observed=3))
            events.append(stored(seq, K.SUBSCRIPTION_ACK, event_index=1))
            continue
        elif op == "instrument":
            if venue == "kraken":
                precisions = draw(precision)
                versions.append(_meta_row(seq, exps, precisions))
            continue
        levels = draw(st.lists(level, min_size=1, max_size=6))
        if op == "check":
            rows += [_book_row(seq, i, CHECK, lv, venue_seq=None, checksum=None, depth=depth)
                     for i, lv in enumerate(levels)]  # fmt: skip
            continue
        kind = SNAP if op == "snapshot" else DELTA
        if kind == SNAP:
            venue_book.replace(levels)
        else:
            venue_book.apply(levels)
        venue_seq = checksum = None
        if venue == "kraken":
            venue_book.truncate(depth)
            divisors = (10 ** (exps[0] - precisions[0]), 10 ** (exps[1] - precisions[1]))
            checksum = kraken_checksum(venue_book, divisors)
            if draw(st.integers(0, 9)) == 0:
                checksum = (checksum + 1) % 2**32  # a planted mismatch
        else:
            number += draw(st.sampled_from([1, 1, 1, 2, 5, 0, -1]))  # 0 repeats, -1 goes back
            venue_seq = max(number, 0)
        rows += [_book_row(seq, i, kind, lv, venue_seq=venue_seq, checksum=checksum, depth=depth)
                 for i, lv in enumerate(levels)]  # fmt: skip
    table = table_from_rows(BOOK_EVENTS, rows) if rows else BOOK_EVENTS.schema.empty_table()
    return venue, table, events, table_from_rows(INSTRUMENTS, versions)


@settings(max_examples=400, deadline=None)
@given(stream=streams())
def test_on_any_stream_the_book_and_the_reference_agree_after_every_step(stream):
    venue, table, events, meta = stream
    # Room for every level a stream here can hold: the reference has no capacity,
    # and these snapshots list more levels than their depth, which Kraken's do not.
    assert_same_steps(venue, table, events, meta, capacity=64)


# --- instrument updates in the stream (ADR 0022) ---------------------------------------------

# test_instruments' session: Kraken moves BTC/USD from one decimal to two
# between its snapshot and its first update. One more update follows here,
# printed at two decimals like the first, to change the best levels again.
AFTER_2 = ([("73468.50", "0.30000000"), ("73468.45", "1.50000000")],
           [("73468.55", "0.10000000"), ("73468.60", "0.89472056")])  # fmt: skip
UPDATE_2 = kraken_book("update", [("73468.45", "1.50000000")], [], after=AFTER_2,
                       timestamp="2026-11-11T03:06:49.000000Z")  # fmt: skip
# records: 0 start, 1 instrument, 2 ack, 3 snapshot, 4 the change, 5 and 6 updates
CHANGE_SEQ = 4


def precision_tables(*entries):
    return tables(log("kraken", *entries), "kraken")


def test_a_mid_session_precision_change_reaches_the_book_at_its_record():
    table, events, meta = precision_tables(*PRECISION_CHANGE_LOG, UPDATE_2)
    groups = list(convert(table, events, meta, instrument=INSTRUMENT_ID))
    assert [(g.seq, g.step) for g in groups if g.step in ("instrument", "message")] == [
        (1, "instrument"), (3, "message"), (CHANGE_SEQ, "instrument"), (5, "message"),
        (6, "message")]  # fmt: skip
    # The reference takes each message's precision from the store; the C++ book
    # is told by the stream. They agree after every step, with every checksum
    # verified at the digits Kraken printed at the time.
    book, outcomes = assert_same_steps("kraken", table, events, meta)
    assert not any(o.stale for o in outcomes)
    assert all(book.stale_count(reason) == 0 for reason in STALE.__members__.values())
    assert (book.instrument.price_precision, book.instrument.qty_precision) == (2, 8)
    assert (book.instrument.tick_size.value, book.instrument.price_exp) == (1_000_000, 8)
    assert sides(book) == (
        [(7_346_850_000_000, 30_000_000), (7_346_845_000_000, 150_000_000)],
        [(7_346_855_000_000, 10_000_000), (7_346_860_000_000, 89_472_056)],
    )


def test_an_update_checked_at_the_old_precision_is_a_mismatch_in_both_books():
    # The same update with its checksum over the book printed at one decimal.
    at_one_decimal = ([("73468.5", "0.30000000")],
                      [("73468.55", "0.10000000"), ("73468.6", "0.89472056")])  # fmt: skip
    wrong = kraken_book("update", [("73468.50", "0.30000000")], [("73468.55", "0.10000000")],
                        after=at_one_decimal)  # fmt: skip
    table, events, meta = precision_tables(*PRECISION_CHANGE_LOG[:-1], wrong)
    # With the adapter's stored mismatch, then by the book's own check alone.
    for stored_events, step in ((events, "stale"), ([], "message")):
        book, outcomes = assert_same_steps("kraken", table, stored_events, meta)
        (failed,) = [o for o in outcomes if o.reason == "checksum_mismatch"]
        assert (failed.seq, failed.step) == (5, step)
        assert book.is_stale and book.stale_count(STALE.ChecksumMismatch) == 1


def test_the_instrument_update_is_what_switches_the_digits_and_exactly_where_it_arrives():
    table, events, meta = precision_tables(*PRECISION_CHANGE_LOG, UPDATE_2)
    groups = list(convert(table, events, meta, instrument=INSTRUMENT_ID))
    (change,) = [g for g in groups if g.step == "instrument" and g.seq == CHANGE_SEQ]
    others = [g for g in groups if g is not change]

    # Withheld: the update after it is checked at one decimal and fails.
    book = new_book("kraken", meta, depth=depth_of(table))
    outcomes = [apply_group(book, g) for g in others]
    failed = next(o for o in outcomes if o.step == "message" and o.reason is not None)
    assert (failed.seq, failed.reason) == (5, "checksum_mismatch")
    assert book.instrument.price_precision == 1

    # One message early: the snapshot before it, printed at one decimal, fails.
    book = new_book("kraken", meta, depth=depth_of(table))
    snapshot = next(i for i, g in enumerate(others) if g.step == "message")
    early = [*others[:snapshot], change, *others[snapshot:]]
    outcomes = [apply_group(book, g) for g in early]
    failed = next(o for o in outcomes if o.step == "message" and o.reason is not None)
    assert (failed.seq, failed.reason) == (3, "checksum_mismatch")


def test_an_instrument_update_is_taken_while_the_book_is_stale():
    """So the snapshot that recovers the book is checked at the precision in force."""
    table, events, meta = precision_tables(*PRECISION_CHANGE_LOG, UPDATE_2)
    groups = list(convert(table, events, meta, instrument=INSTRUMENT_ID))
    book = new_book("kraken", meta, depth=depth_of(table))
    for group in groups:
        if group.step == "instrument" and group.seq == CHANGE_SEQ:
            gap = control_event(KIND.StreamStale, CHANGE_SEQ, reason=REASON.SequenceGap)
            assert book.apply(gap).effect == EFFECT.WentStale
            (result,) = apply_group(book, group).results
            assert (result.effect, book.is_stale) == (EFFECT.Instrument, True)
        else:
            apply_group(book, group)
    assert book.instrument.price_precision == 2 and book.is_stale


def test_the_sessions_first_instrument_update_sets_the_precisions_whatever_the_book_began_with():
    table, events, meta = fixture_tables("kraken")
    (version,) = meta.to_pylist()
    began = core_meta({**version, "price_precision": 8, "qty_precision": 0})
    book = _core.OrderBook(began, CHECKS["kraken"], _core.OrderBook.capacity_for_depth(25))
    groups = convert(table, events, meta, instrument=INSTRUMENT_ID)
    outcomes = [apply_group(book, group) for group in itertools.islice(groups, 60)]
    assert [o.step for o in outcomes[:4]] == ["reset", "reset", "instrument", "message"]
    assert (book.instrument.price_precision, book.instrument.qty_precision) == (1, 8)
    assert book.has_book and not book.is_stale  # every checksum since has verified


def test_an_instrument_update_the_book_cannot_follow_is_refused():
    book = _core.OrderBook(plain_meta(price_exp=8, qty_exp=8, price_precision=1, qty_precision=8),
                           _core.BookCheck.KrakenChecksum, 20)  # fmt: skip
    crc = kraken_crc([(1_000_000_000, 5)], [(1_010_000_000, 3)], (10**7, 1))
    apply_all(book, snapshot_events(1, [(BUY, 1_000_000_000, 5), (SELL, 1_010_000_000, 3)],
                                    checksum=crc, depth=10))  # fmt: skip
    assert book.has_book

    def update(**changes):
        row = {"price_exp": 8, "qty_exp": 8, "price_precision": 1, "qty_precision": 8,
               "tick_size": 10_000_000, "lot_size": 1, **changes}  # fmt: skip
        made = EV.instrument_update(core_meta(row))
        made.seq = 2
        return made

    # Other exponents would give every mantissa in the book another meaning (ADR 0019).
    result = book.apply(update(price_exp=7))
    assert (result.effect, result.reason) == (EFFECT.WentStale, STALE.InvalidEvent)
    assert (book.is_stale, book.has_book) == (True, False)
    assert (book.instrument.price_exp, book.instrument.price_precision) == (8, 1)  # as it was
    # One it can follow is taken, stale or not.
    result = book.apply(update(price_precision=2, tick_size=1_000_000))
    assert (result.effect, result.reason) == (EFFECT.Instrument, STALE.NotStale)
    assert (book.instrument.price_precision, book.instrument.tick_size.value) == (2, 1_000_000)
    assert book.stale_count(STALE.InvalidEvent) == 1


# --- capacity (ADR 0022, decision 3) -----------------------------------------------------------


def test_a_level_that_does_not_fit_makes_the_book_stale_until_a_snapshot_fits():
    three_bids = [(BUY, 100, 1), (BUY, 99, 1), (BUY, 98, 1)]
    asks = [(SELL, 101, 1), (SELL, 102, 1), (SELL, 103, 1)]
    messages = [
        msg(0, SNAP, *three_bids, (SELL, 101, 1)),  # three bids: the side is full
        msg(1, DELTA, (BUY, 99, 0), (BUY, 97, 1)),  # one out, then one in: it fits
        msg(2, DELTA, (BUY, 96, 1), (SELL, 102, 1)),  # a fourth bid
        msg(3, DELTA, (SELL, 103, 1)),
        msg(4, SNAP, *asks, (SELL, 104, 1), (BUY, 100, 1)),  # four asks: it does not fit either
        msg(5, SNAP, *three_bids, *asks),  # this one does
        msg(6, DELTA, (BUY, 98, 0)),
    ]
    book, outcomes = drive("coinbase", messages, capacity=3)
    assert [(o.reason, o.stale, o.recovered) for o in outcomes] == [
        (None, False, False),
        (None, False, False),
        ("capacity_exceeded", True, False),
        (None, True, False),  # not applied
        ("capacity_exceeded", True, False),
        (None, False, True),  # recovered at the snapshot that fits
        (None, False, False),
    ]
    # Stale at the level that did not fit; the rest of its message is not applied.
    assert outcomes[2].effects == [EFFECT.WentStale, EFFECT.Skipped]
    assert outcomes[4].effects == [EFFECT.Pending] * 4 + [EFFECT.WentStale, EFFECT.Skipped,
                                                          EFFECT.Skipped]  # fmt: skip
    assert book.stale_count(STALE.CapacityExceeded) == 2  # its own reason, and a counter
    assert sides(book) == ([(100, 1), (99, 1)], [(101, 1), (102, 1), (103, 1)])

    stale_book, _ = drive("coinbase", messages[:4], capacity=3)
    assert (stale_book.is_stale, stale_book.has_book) == (True, False)
    assert stale_book.stale_reason == STALE.CapacityExceeded and sides(stale_book) == ([], [])
    # With room for one more level a side, nothing goes stale at all.
    roomy, outcomes = same("coinbase", messages, capacity=4)
    assert roomy.stale_count(STALE.CapacityExceeded) == 0


def test_a_level_that_does_not_fit_is_refused_even_if_a_later_one_would_make_room():
    messages = [
        msg(0, SNAP, (BUY, 100, 1), (BUY, 99, 1), (BUY, 98, 1), (SELL, 101, 1)),
        msg(1, DELTA, (BUY, 97, 1), (BUY, 99, 0)),
    ]  # in before out
    book, outcomes = drive("coinbase", messages, capacity=3)
    assert outcomes[-1].reason == "capacity_exceeded" and book.is_stale
    # Changing a level the side already holds needs no room.
    messages[1] = msg(1, DELTA, (BUY, 99, 7), (BUY, 100, 0), (BUY, 97, 1))
    book, outcomes = drive("coinbase", messages, capacity=3)
    assert not book.is_stale and book.top(BUY, ALL) == [(99, 7), (98, 1), (97, 1)]


@functools.cache
def resnapshot_tables(venue):
    """The fixture with a second stream snapshot late in it (integrity.py)."""
    return tables(list(resnapshotted(venue)), venue)


def test_with_no_room_above_its_depth_the_kraken_book_goes_stale_and_recovers_at_a_snapshot():
    """Kraken's updates apply before the book is cut to its depth, so a side
    briefly holds 26 levels. With a capacity of exactly 25 the book goes stale
    at the first such update and recovers at the next snapshot; while it is
    not stale it is the reference's book."""
    table, events, meta = resnapshot_tables("kraken")
    messages = list(messages_from_table(table, meta))
    reference = trace("kraken", messages, events, mode="resume")
    book = new_book("kraken", meta, capacity=25)
    failures, recoveries, compared = [], [], {False: 0, True: 0}  # before, after the resnapshot
    for group in convert(table, events, meta, instrument=INSTRUMENT_ID):
        outcome = apply_group(book, group)
        if group.step in NOT_STEPS:
            continue
        step, state = next(reference)
        assert not step.stale  # the reference has no capacity: it never is, on this log
        if outcome.reason is not None and outcome.step == "message":
            failures.append(outcome)
        if outcome.recovered:
            recoveries.append(outcome.seq)
        if outcome.step == "message" and not outcome.stale:
            assert sides(book) == reference_sides(state), outcome.seq
            compared[outcome.seq >= KRAKEN_RESNAPSHOT] += 1
    assert {o.reason for o in failures} == {"capacity_exceeded"}
    # Once before the second snapshot and once after it; one recovery, at it.
    assert len(failures) == 2 and recoveries == [KRAKEN_RESNAPSHOT]
    assert failures[0].seq < KRAKEN_RESNAPSHOT < failures[1].seq
    assert book.stale_count(STALE.CapacityExceeded) == 2 and book.is_stale
    # The fresh book was compared, in full, both before the first failure and after the recovery.
    assert compared[False] >= 1 and compared[True] >= 1, compared


@pytest.mark.parametrize("capacity", [26, 50])
def test_with_room_for_one_level_above_its_depth_the_kraken_fixture_never_overflows(capacity):
    """The measured need is depth + 1; ``capacity_for_depth`` gives twice the depth."""
    table, events, meta = fixture_tables("kraken")
    book, outcomes = assert_same_steps("kraken", table, events, meta, capacity=capacity)
    assert not any(o.stale for o in outcomes)
    assert book.stale_count(STALE.CapacityExceeded) == 0


# --- messages arrive one event at a time: frames, and what cannot be acted on -------------------

GOOD = [(BUY, 100, 5), (BUY, 99, 1), (SELL, 101, 3)]


def fresh_book():
    book = coinbase_book()
    assert apply_all(book, snapshot_events(1, GOOD, venue_seq=1))[-1] == EFFECT.Applied
    return book


def test_a_snapshot_takes_effect_at_its_end_and_an_update_at_its_last_level():
    book = coinbase_book()
    begin, *levels, end = snapshot_events(1, GOOD, venue_seq=1)
    assert book.apply(begin).effect == EFFECT.Pending
    for level in levels:
        assert book.apply(level).effect == EFFECT.Pending
        assert not book.has_book and sides(book) == ([], [])  # nothing to read yet
    assert book.apply(end).effect == EFFECT.Applied
    assert book.has_book and sides(book) == ([(100, 5), (99, 1)], [(101, 3)])
    first, last = update_events(2, [(BUY, 98, 2), (SELL, 101, 0)], venue_seq=2)
    assert book.apply(first).effect == EFFECT.Pending
    assert book.apply(last).effect == EFFECT.Applied
    assert sides(book) == ([(100, 5), (99, 1), (98, 2)], [])


def test_a_check_snapshot_changes_nothing_and_cannot_recover_a_stale_book():
    book = fresh_book()
    check = snapshot_events(2, [(BUY, 1, 1), (SELL, 2, 2)], kind=KIND.BookCheckSnapshot, source=1)
    assert apply_all(book, check) == [EFFECT.Pending] * 3 + [EFFECT.Ignored]
    assert sides(book) == ([(100, 5), (99, 1)], [(101, 3)]) and not book.is_stale
    # ADR 0020, decision 8: a check snapshot may not reseed a stale book.
    book.apply(control_event(KIND.StreamStale, 3, reason=REASON.MessageRejected))
    check = snapshot_events(4, [(BUY, 1, 1), (SELL, 2, 2)], kind=KIND.BookCheckSnapshot, source=1)
    assert apply_all(book, check) == [EFFECT.Pending] * 3 + [EFFECT.Ignored]
    assert (book.is_stale, book.has_book, book.stale_reason) == (True, False, STALE.MessageRejected)


def _without(events, index):
    return events[:index] + events[index + 1 :]


def _recount(events, count):
    events[-1].level_index = count
    return events


BROKEN_FRAMES = {
    "a level missing": lambda f: _without(f, 2),
    "the last level missing": lambda f: _without(f, 3),
    "no end before the next message": lambda f: f[:-1] + update_events(3, [(BUY, 1, 1)],
                                                                      venue_seq=9),
    "no begin": lambda f: f[1:],
    "levels out of position": lambda f: [f[0], f[2], f[1], f[3], f[4]],
    "an end that counts differently": lambda f: _recount(f, 2),
    "another kind of event inside it": lambda f: [
        *f[:2], control_event(KIND.StreamReset, 2, reason=REASON.Connect), *f[2:]],
    "a level of another record inside it": lambda f: [
        *f[:2], level_event(KIND.BookSnapshot, 9, 1, BUY, 5, 5), *f[2:]],
    "a begin inside it": lambda f: [*f[:3], *f],
    "a check level inside a stream snapshot": lambda f: [
        *f[:2], level_event(KIND.BookCheckSnapshot, 2, 1, BUY, 99, 1), *f[3:]],
    "the last-level flag too early": lambda f: [
        f[0], level_event(KIND.BookSnapshot, 2, 0, BUY, 100, 5, last=True, venue_seq=5), *f[2:]],
}  # fmt: skip


@pytest.mark.parametrize("damage", BROKEN_FRAMES, ids=str)
def test_a_snapshot_frame_that_is_not_complete_leaves_the_book_stale(damage):
    book = fresh_book()
    frame = snapshot_events(2, GOOD, venue_seq=5)
    assert len(frame) == 5
    effects = apply_all(book, BROKEN_FRAMES[damage](frame))
    assert effects.count(EFFECT.WentStale) == 1 and EFFECT.Applied not in effects
    # One fault is one failure, however many of the frame's events follow it.
    assert book.stale_count(STALE.IncompleteMessage) == 1
    assert sum(book.stale_count(reason) for reason in STALE.__members__.values()) == 1
    if damage == "a begin inside it":  # the second frame is whole: it recovers the book
        assert effects[-1] == EFFECT.Recovered and not book.is_stale
        return
    assert EFFECT.Recovered not in effects
    assert (book.is_stale, book.stale_reason) == (True, STALE.IncompleteMessage)
    assert not book.has_book and sides(book) == ([], [])
    # A whole snapshot recovers it.
    assert apply_all(book, snapshot_events(7, GOOD, venue_seq=6))[-1] == EFFECT.Recovered
    assert sides(book) == ([(100, 5), (99, 1)], [(101, 3)])


def test_a_stale_reason_is_kept_until_the_book_recovers():
    book = fresh_book()
    frame = snapshot_events(2, GOOD, venue_seq=5)
    apply_all(book, _without(frame, 2))
    assert book.stale_reason == STALE.IncompleteMessage
    result = book.apply(control_event(KIND.StreamStale, 3, reason=REASON.SequenceGap))
    assert (result.effect, result.reason) == (EFFECT.WentStale, STALE.SequenceGap)
    assert book.stale_reason == STALE.IncompleteMessage  # what began the stale period
    assert book.stale_count(STALE.SequenceGap) == 1
    apply_all(book, snapshot_events(4, GOOD, venue_seq=6))
    assert (book.is_stale, book.stale_reason) == (False, STALE.NotStale)
    assert book.stale_count(STALE.IncompleteMessage) == 1  # counts are since the book was built


def test_an_update_cut_short_by_another_message_leaves_the_book_stale():
    book = fresh_book()
    first, _ = update_events(2, [(BUY, 98, 2), (SELL, 102, 1)], venue_seq=2)
    assert book.apply(first).effect == EFFECT.Pending
    result = book.apply(update_events(3, [(BUY, 97, 1)], venue_seq=3)[0])
    assert (result.effect, result.reason) == (EFFECT.WentStale, STALE.IncompleteMessage)
    assert (book.is_stale, book.has_book) == (True, False)
    # A level that is not the next of its message is the same fault.
    book = fresh_book()
    result = book.apply(level_event(KIND.BookDelta, 2, 1, BUY, 98, 2, last=True, venue_seq=2))
    assert (result.effect, result.reason) == (EFFECT.WentStale, STALE.IncompleteMessage)


def test_an_empty_snapshot_frame_makes_the_book_stale():
    """Nothing in it says whether it is the stream's or a check snapshot, and
    no header verifies it (ADR 0022)."""
    book = fresh_book()
    effects = apply_all(book, snapshot_events(2, []))
    assert effects == [EFFECT.Pending, EFFECT.WentStale]
    assert (book.is_stale, book.stale_reason) == (True, STALE.InvalidEvent)


@pytest.mark.parametrize(
    ("levels", "header"),
    [
        ([(_core.Side.Unknown, 100, 1)], {"venue_seq": 2}),  # no side
        ([(BUY, 100, -1)], {"venue_seq": 2}),  # a negative quantity
        ([(BUY, 98, 1)], {}),  # no message number: nothing to check Coinbase's rule by
        ([(BUY, 98, 1)], {"checksum": 1, "depth": 10}),  # Kraken's header, not Coinbase's
    ],
)
def test_an_update_the_book_cannot_act_on_makes_it_stale(levels, header):
    book = fresh_book()
    effects = apply_all(book, update_events(2, levels, **header))
    assert effects == [EFFECT.WentStale]
    assert (book.is_stale, book.has_book, book.stale_reason) == (True, False, STALE.InvalidEvent)
    assert book.stale_count(STALE.InvalidEvent) == 1


def test_a_kraken_message_without_a_checksum_cannot_be_verified_and_makes_the_book_stale():
    book = _core.OrderBook(plain_meta(), _core.BookCheck.KrakenChecksum, 20)
    effects = apply_all(book, snapshot_events(1, GOOD, depth=10))
    assert effects[-1] == EFFECT.WentStale and book.stale_reason == STALE.InvalidEvent
    good = snapshot_events(2, GOOD, depth=10, checksum=kraken_crc([(100, 5), (99, 1)], [(101, 3)]))
    assert apply_all(book, good)[-1] == EFFECT.Recovered


def test_events_of_another_instrument_and_trades_are_ignored():
    book = fresh_book()
    other = update_events(2, [(BUY, 98, 2)], venue_seq=2)[0]
    other.instrument = _core.InstrumentId(INSTRUMENT_ID + 1)
    assert book.apply(other).effect == EFFECT.Ignored
    stale = control_event(KIND.StreamStale, 2, reason=REASON.SequenceGap,
                          instrument=INSTRUMENT_ID + 1)  # fmt: skip
    assert book.apply(stale).effect == EFFECT.Ignored
    trade = level_event(KIND.Trade, 3, 0, BUY, 100, 1)
    assert book.apply(trade).effect == EFFECT.Ignored
    assert sides(book) == ([(100, 5), (99, 1)], [(101, 3)]) and not book.is_stale
    assert all(book.stale_count(reason) == 0 for reason in STALE.__members__.values())


# --- the recorded fixtures: the golden files, every depth, the venues' own snapshots ------------

VENUES = ["kraken", pytest.param("coinbase", marks=SLOW)]


@pytest.mark.parametrize("venue", VENUES)
def test_the_book_equals_the_golden_file_after_every_step(venue):
    """Roadmap Phase 4b: after every step of the golden files (every message,
    stale and reset row), the C++ book's stale flag, top of book and top 20
    levels equal the row."""
    table, events, meta = fixture_tables(venue)
    golden = pq.read_table(GOLDEN / f"{venue}-{INSTRUMENT}.top{GOLDEN_LEVELS}.parquet").to_pydict()
    rows = len(golden["seq"])
    book = new_book(venue, meta, depth=depth_of(table))
    row = 0
    for group in convert(table, events, meta, instrument=INSTRUMENT_ID):
        before = (book.is_stale, book.has_book, book.top(BUY, GOLDEN_LEVELS),
                  book.top(SELL, GOLDEN_LEVELS))  # fmt: skip
        outcome = apply_group(book, group)
        if group.step in NOT_STEPS:
            # A check snapshot and an unchanged metadata version change nothing.
            assert outcome.effects[-1] in (EFFECT.Ignored, EFFECT.Instrument)
            assert before == (book.is_stale, book.has_book, book.top(BUY, GOLDEN_LEVELS),
                              book.top(SELL, GOLDEN_LEVELS))  # fmt: skip
            continue
        where = (group.log_id, group.seq, group.step)
        assert (golden["log_id"][row], golden["seq"][row], golden["step"][row]) == where
        assert (golden["reason"][row], golden["stale"][row], golden["recovered"][row]) == (
            outcome.reason, outcome.stale, outcome.recovered), where  # fmt: skip
        bids, asks = book.top(BUY, GOLDEN_LEVELS), book.top(SELL, GOLDEN_LEVELS)
        for rank in range(GOLDEN_LEVELS):
            want = tuple(
                (golden[f"{name}_price_{rank:02d}"][row], golden[f"{name}_qty_{rank:02d}"][row])
                for name in ("bid", "ask")
            )
            got = (bids[rank] if rank < len(bids) else (None, None),
                   asks[rank] if rank < len(asks) else (None, None))  # fmt: skip
            assert got == want, (where, rank)
        assert (book.best_bid(), book.best_ask()) == (bids[0] if bids else None,
                                                      asks[0] if asks else None)  # fmt: skip
        if venue == "kraken" and group.step == "message":
            # Kraken's checksum, computed from the book, is the venue's after every message.
            framed = group.events[-1].kind == KIND.SnapshotEnd
            last_level = group.events[-2 if framed else -1]
            assert last_level.has(EV.LAST_IN_MESSAGE) and last_level.has(EV.HAS_CHECKSUM)
            assert book.checksum() == last_level.checksum, where
        row += 1
    assert row == rows > 2_000
    assert all(book.stale_count(reason) == 0 for reason in STALE.__members__.values())


@pytest.mark.parametrize("venue", VENUES)
def test_the_book_equals_the_reference_at_every_depth_after_every_step(venue):
    """Not only the 20 levels the golden files hold: every level of both sides,
    after every step (38,000 on Coinbase)."""
    table, events, meta = fixture_tables(venue)
    book, outcomes = assert_same_steps(venue, table, events, meta)
    assert not any(o.stale for o in outcomes)
    assert sum(o.step == "message" for o in outcomes) > 2_000
    assert book.size(BUY) + book.size(SELL) == 0  # the session's disconnect emptied it


def check_snapshot_differences(venue, table, events, meta):
    """Every check snapshot against the C++ book, under ADR 0014's rule, written
    here from the rule and not from the reference's code. A is the last stream
    message whose venue time is at or before the snapshot's, B the one after
    it; the snapshot is compared with the book after A, within the snapshot's
    depth. On Coinbase, between two batches, B's own levels are unverified.
    Returns, per snapshot record, (violations, unverified), each a list of
    (side, price, the book's quantity, the snapshot's)."""
    messages = list(messages_from_table(table, meta))
    stream = [m for m in messages if m.kind != CHECK]
    connects = [e for e in events if e["kind"] == K.CONNECT and e["connection"] == "ws"]
    assert len(connects) == 1  # one stream connection: every stream message is on it
    plan = {}
    for check in (m for m in messages if m.kind == CHECK):
        later = [i for i, m in enumerate(stream) if m.event_ts > check.event_ts]
        a = stream[(later[0] if later else len(stream)) - 1]
        b = stream[later[0]] if later else None
        assert later[:1] != [0] and a.key < check.key
        plan.setdefault(a.key, []).append((check, a, b))
    book = new_book(venue, meta, depth=depth_of(table))
    found = {}
    for group in convert(table, events, meta, instrument=INSTRUMENT_ID):
        apply_group(book, group)
        if group.step != "message":
            continue
        for check, a, b in plan.pop((group.log_id, group.seq), ()):
            between = venue == "coinbase" and a.event_ts != check.event_ts
            exempt = {(s, p) for s, p, _ in b.levels} if between and b is not None else set()
            violations, unverified = [], []
            for side, schema_side in ((BUY, Side.BUY), (SELL, Side.SELL)):
                ours = dict(book.top(side, ALL if check.depth is None else check.depth))
                theirs = {p: q for s, p, q in check.levels if s == schema_side}
                for price in sorted(set(ours) | set(theirs)):
                    if ours.get(price) != theirs.get(price):
                        which = unverified if (schema_side, price) in exempt else violations
                        which.append((schema_side, price, ours.get(price), theirs.get(price)))
            found[check.key] = (violations, unverified)
    assert not plan
    return found


def assert_agrees_with_the_reference(venue, table, events, meta, found):
    messages = list(messages_from_table(table, meta))
    results = verify_check_snapshots(venue, messages, events)
    assert len(results) == len(found) >= 1
    for result in results:
        violations, unverified = found[result.snapshot.key]
        assert result.differences == violations
        assert [u[:4] for u in result.unverified] == unverified
    return results


@pytest.mark.parametrize("venue", VENUES)
def test_the_book_equals_the_fixtures_check_snapshot_on_every_level(venue):
    """Both fixtures' snapshots are timed on a stream batch: exact, every level
    (Kraken's 50 at depth 25, Coinbase's 38,532)."""
    table, events, meta = fixture_tables(venue)
    found = check_snapshot_differences(venue, table, events, meta)
    assert list(found.values()) == [([], [])]
    (result,) = assert_agrees_with_the_reference(venue, table, events, meta, found)
    assert result.exact and result.levels_compared == {"kraken": 50, "coinbase": 38_532}[venue]


@SLOW
def test_between_batches_the_book_differs_from_the_snapshot_only_on_the_next_batchs_levels():
    """ADR 0014, on the 2026-11-13 recording whose check snapshot falls between
    two of Coinbase's batches: every difference is a level of the next batch."""
    (path,) = sorted(BETWEEN_BATCHES.glob("*.qaraw.zst"))
    table, events, meta = tables(list(read_raw_log(path)), "coinbase")
    found = check_snapshot_differences("coinbase", table, events, meta)
    ((violations, unverified),) = found.values()
    assert violations == [] and len(unverified) == 55
    (result,) = assert_agrees_with_the_reference("coinbase", table, events, meta, found)
    assert (result.result, result.on_batch, result.next_batch_levels) == ("explained", False, 93)


# --- gap handling: the integrity cases of golden/integrity-points.json --------------------------


@pytest.fixture(
    params=[pytest.param(c, marks=SLOW) if c.venue == "coinbase" else c for c in CASES],
    ids=lambda c: c.name,
)
def case(request):
    return request.param


def test_on_an_integrity_case_the_book_goes_stale_and_recovers_at_the_listed_points(case):
    """Roadmap Phase 4b: stale and recovered at exactly the committed points,
    and between them the top 20 levels equal the reference's resume-mode book
    after every message. The two cases with no stored events test the book's
    own Kraken checksum and Coinbase numbering."""
    table, events, meta = case_tables(case)
    (listed,) = [c for c in committed()["cases"] if c["name"] == case.name]
    assert listed["stored_events"] == bool(events) == case.stored_events
    messages = list(messages_from_table(table, meta))
    reference = trace(case.venue, messages, events, mode="resume")
    book = new_book(case.venue, meta, depth=depth_of(table))
    points, steps, stream_messages, stale_messages = [], 0, 0, 0
    for group in convert(table, events, meta, instrument=INSTRUMENT_ID):
        outcome = apply_group(book, group)
        if group.step in NOT_STEPS:
            continue
        step, state = next(reference)
        where = (group.log_id, group.seq, group.step)
        assert where == (step.log_id, step.seq, step.step)
        assert (outcome.reason, outcome.stale, outcome.recovered) == (
            step.reason, step.stale, step.recovered), where  # fmt: skip
        want = ([], []) if state is None else (state.top(Side.BUY, GOLDEN_LEVELS),
                                               state.top(Side.SELL, GOLDEN_LEVELS))  # fmt: skip
        assert (book.top(BUY, GOLDEN_LEVELS), book.top(SELL, GOLDEN_LEVELS)) == want, where
        steps += 1
        stream_messages += group.step == "message"
        stale_messages += group.step == "message" and outcome.stale
        if is_point(step):
            points.append({"seq": outcome.seq, "step": outcome.step, "reason": outcome.reason,
                           "stale": outcome.stale, "recovered": outcome.recovered})  # fmt: skip
    assert next(reference, None) is None
    assert points == listed["points"]
    assert (steps, stream_messages, stale_messages) == (
        listed["steps"], listed["messages"], listed["stale_messages"])  # fmt: skip
    # What the book counted: one failure for each stale point, each under its reason.
    failures = [p for p in points if p["step"] != "reset" and p["reason"] is not None]
    for reason in {p["reason"] for p in failures}:
        expected = sum(p["reason"] == reason for p in failures)
        assert book.stale_count(getattr(STALE, camel(reason))) == expected
    assert sum(book.stale_count(r) for r in STALE.__members__.values()) == len(failures)
    assert (case.name == "kraken-duplicated") == (not failures)  # Kraken cannot see a repeat
