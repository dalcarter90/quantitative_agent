"""Tests for the adversarial review of Phase 4b, the C++ order book
(docs/reviews/2026-11-cpp-order-book.md).

Each test proves one finding of the review and is named after its number
(``test_finding_01_...``). It states what the ADRs and the architecture
promise, or what the review argues the book should do.

A TEST WHOSE FINDING IS STILL OPEN IS MARKED
``xfail(strict=True, raises=AssertionError)`` WITH ITS FINDING NUMBER AS THE
REASON, so ``scripts/check.ps1`` stays green while the finding is open. The
marker is strict: once a finding is fixed its test passes, the suite fails on
the unexpected pass, and the marker has to be removed in the same change. From
then on the test is the finding's regression test. The setup inside each test
raises ``SetupError`` instead of asserting, so a test cannot be XFAIL because
its setup went wrong.

Every finding with a test here is fixed, and no test is marked (ADR 0023):
finding 1 (an update cannot be read until its last level, as a snapshot
cannot), finding 2 (the book is bound to its snapshot's stream), finding 5 (the
Kraken adapter refuses a record with two versions of the instrument; the
converter emits two versions at one record in the reference book's order, so
the C++ book takes the same one) and finding 6 (a tick or lot size below 1 is
refused). Each test kept its shape: finding 1's accepts "nothing", which the
fix chose, and finding 5's still builds the two versions by hand, as a store
written before the adapters refused them could hold them.

If the owner decides a behavior is intended, the test is replaced by one that
pins the decision, in the same change as the ADR that records it. Do not
delete or weaken a test here, and do not leave a marker on a test that passes
(CLAUDE.md).

The data is hand-built events and the committed fixtures: no network and no
local recordings.
"""

import pytest
from _builders import LOG
from cppbook import (
    BUY,
    EFFECT,
    EV,
    INSTRUMENT_ID,
    KIND,
    SELL,
    STALE,
    apply_all,
    apply_group,
    control_event,
    new_book,
    sides,
    snapshot_events,
    update_events,
)
from test_book import SNAP, msg
from test_marketevents import fixture_tables
from test_order_book import coinbase_book, kraken_crc, plain_meta, rows_of

from quantagent import _core
from quantagent.data.book import ReferenceBook, messages_from_table, trace
from quantagent.data.schema import BOOK_EVENTS, INSTRUMENTS, Side, table_from_rows
from quantagent.marketevents import convert

REASON = _core.StreamReason
NOTHING = ([], [])  # what the queries show when the book cannot be read


class SetupError(Exception):
    """A test's setup did not do what the test relies on: not the finding."""


def require(condition: bool, what: str) -> None:
    if not condition:
        raise SetupError(what)


def crossed(book) -> bool:
    bid, ask = book.best_bid(), book.best_ask()
    return bid is not None and ask is not None and bid[0] >= ask[0]


# --- finding 1: a half-applied update reads as a fresh book (fixed, ADR 0023) ------------------


def test_finding_01_between_the_levels_of_an_update_the_book_shows_no_state_the_reference_has_not():
    """ADR 0022: an update "takes effect at its kLastInMessage level". Coinbase
    lists an update's bids before its asks, so when the market moves up a tick
    the bid arrives before the ask it crosses is removed. Neither the venue
    nor the reference ever has the crossed book in between, yet the C++ book
    shows it, with ``has_book`` true and ``is_stale`` false."""
    book = coinbase_book()
    effects = apply_all(book, snapshot_events(1, [(BUY, 100, 5), (SELL, 101, 3), (SELL, 102, 4)],
                                              venue_seq=1))  # fmt: skip
    require(effects[-1] == EFFECT.Applied, "the snapshot takes effect")
    before = sides(book)
    first, last = update_events(2, [(BUY, 101, 2), (SELL, 101, 0)], venue_seq=2)

    reference = ReferenceBook()
    reference.replace([(Side.BUY, 100, 5), (Side.SELL, 101, 3), (Side.SELL, 102, 4)])
    require(not reference.is_crossed(), "the book before the message is not crossed")
    reference.apply([(Side.BUY, 101, 2), (Side.SELL, 101, 0)])
    require(not reference.is_crossed(), "nor the book after it")

    require(book.apply(first).effect == EFFECT.Pending, "the message has not taken effect")
    # What anything reading the book now (a feature, pre-trade risk's collar, the
    # seqlock snapshot) may see: the book before the message, or nothing.
    shown = (book.has_book, book.is_stale, crossed(book), sides(book))
    assert shown in ((True, False, False, before), (False, False, False, NOTHING)), shown
    require(book.apply(last).effect == EFFECT.Applied, "the message then takes effect")


def test_finding_01_between_the_levels_of_a_kraken_update_the_book_is_deeper_than_its_depth():
    """Kraken's book is cut to its depth when a message ends. In between, the
    side holds a level more than the depth (80,077 of the one-hour recording's
    478,894 side-updates), a book Kraken never has and the checksum never
    covered."""
    book = _core.OrderBook(plain_meta(), _core.BookCheck.KrakenChecksum,
                           _core.OrderBook.capacity_for_depth(2))  # fmt: skip
    crc = kraken_crc([(100, 1), (99, 1)], [(102, 1)])
    effects = apply_all(book, snapshot_events(1, [(BUY, 100, 1), (BUY, 99, 1), (SELL, 102, 1)],
                                              checksum=crc, depth=2))  # fmt: skip
    require(effects[-1] == EFFECT.Applied, "the snapshot takes effect")
    before = sides(book)
    after = kraken_crc([(101, 1), (100, 1)], [(102, 1), (103, 1)])
    first, last = update_events(2, [(BUY, 101, 1), (SELL, 103, 1)], checksum=after, depth=2)
    require(book.apply(first).effect == EFFECT.Pending, "the message has not taken effect")
    shown = (book.has_book, book.size(BUY), sides(book))
    assert shown in ((True, 2, before), (False, 0, NOTHING)), shown
    require(book.apply(last).effect == EFFECT.Applied, "the message then takes effect")


# 4.4 s alone, most of it the fixture's conversion (cached when run with others)
def test_finding_01_on_the_coinbase_fixture_half_applied_updates_show_crossed_books():
    """The same on recorded data: updates of the committed Coinbase fixture
    whose half-applied book is crossed while the book says it can be read (57
    by the review's count; 790 in the one-hour recording)."""
    table, events, meta = fixture_tables("coinbase")
    book = new_book("coinbase", meta)
    messages = shown_crossed = 0
    for group in convert(table, events, meta, instrument=INSTRUMENT_ID):
        if group.step != "message":
            apply_group(book, group)
            continue
        messages += 1
        seen = False
        for event in group.events:
            pending = book.apply(event).effect == EFFECT.Pending
            seen = seen or (pending and book.has_book and crossed(book))
        shown_crossed += seen
        require(not book.is_stale and not crossed(book), f"step boundary at seq {group.seq}")
    require(messages > 2_000, "the fixture's messages were applied")
    assert shown_crossed == 0, f"{shown_crossed} of {messages} messages showed a crossed book"


# --- finding 2: the book follows whatever stream it is given (fixed, ADR 0023) ----------------


def test_finding_02_an_update_of_another_stream_is_not_applied_to_the_book():
    """Two logs of one instrument that overlap in time (a recorder restarted
    before the old one stopped), merged by monotonic stamp as Phase 7's replay
    engine will merge them (roadmap Phase 7, ADR 0012); or a Phase 6 handler
    that opens its new connection before the old one closes. Log b's session
    start resets the book and b's snapshot rebuilds it; then an update of log
    a, whose Coinbase number (3) happens to exceed b's last (1), is applied to
    b's book. The book is fresh and is neither stream's book. The reference
    never sees this order: it replays logs one after the other."""
    a, b = 0, 2  # the stream sources of the two logs
    book = coinbase_book()
    apply_all(book, [control_event(KIND.StreamReset, 0, reason=REASON.SessionStart, source=a)])
    apply_all(book, snapshot_events(1, [(BUY, 100, 5), (SELL, 101, 3)], venue_seq=1, source=a))
    apply_all(book, update_events(2, [(BUY, 99, 1)], venue_seq=2, source=a))
    apply_all(book, [control_event(KIND.StreamReset, 0, reason=REASON.SessionStart, source=b)])
    effects = apply_all(book, snapshot_events(1, [(BUY, 200, 1), (SELL, 201, 1)], venue_seq=1,
                                              source=b))  # fmt: skip
    require(effects[-1] == EFFECT.Applied and not book.is_stale, "b's snapshot is the book")
    b_book = sides(book)
    (late,) = update_events(3, [(BUY, 98, 7)], venue_seq=3, source=a)
    result = book.apply(late)
    # Stale, or b's book unchanged: never a mix of the two streams.
    assert book.is_stale or sides(book) == b_book, (result, sides(book))


# --- finding 5: two metadata versions at one record (fixed, ADR 0023) ------------------------


def _version(price_precision: int) -> dict:
    return {"event_ts": 1_000, "event_ts_source": "receipt", "recv_ts": 1_000, "log_id": LOG,
            "seq": 0, "price_exp": 8, "qty_exp": 8, "price_precision": price_precision,
            "qty_precision": 8, "tick_size": 10 ** (8 - price_precision),
            "lot_size": 1}  # fmt: skip


def test_finding_05_two_metadata_versions_at_one_record_give_both_books_the_same_precision():
    """Two ``instruments`` rows at one record (a Kraken instrument message
    listing the pair twice). The reference takes the version with the larger
    (price_exp, qty_exp, price_precision, qty_precision); the converter emits
    both in the session's row order and the C++ book keeps the last, an order
    ``PITSession.instruments`` (ORDER BY event_ts, log_id, seq) leaves
    unspecified for rows of one record. The snapshot here is summed at two
    decimals: in one of the two orders the C++ book checks it at one."""
    bids, asks = [(7_346_855_000_000, 10_000_000)], [(7_346_860_000_000, 20_000_000)]
    crc = kraken_crc(bids, asks, (10**6, 1))  # printed at two decimals
    message = msg(1, SNAP, *[(BUY, p, q) for p, q in bids], *[(SELL, p, q) for p, q in asks],
                  venue_seq=None, checksum=crc, depth=10)  # fmt: skip
    table = table_from_rows(BOOK_EVENTS, rows_of([message]))
    for order in ([_version(2), _version(1)], [_version(1), _version(2)]):
        meta = table_from_rows(INSTRUMENTS, order)
        messages = list(messages_from_table(table, meta))
        require(messages[0].price_precision == 2, "the reference takes the larger tuple")
        ((step, _),) = list(trace("kraken", messages, [], mode="resume"))
        require((step.reason, step.stale) == (None, False), "the reference verifies it")
        book = new_book("kraken", meta, depth=10)
        groups = convert(table, [], meta, instrument=INSTRUMENT_ID)
        outcomes = [apply_group(book, g) for g in groups]
        (got,) = [o for o in outcomes if o.step == "message"]
        updates = [o for o in outcomes if o.step == "instrument"]
        require(len(updates) == 2, "both versions reach the C++ book")
        where = ([v["price_precision"] for v in order], book.instrument.price_precision)
        assert (got.reason, got.stale) == (step.reason, step.stale), where


# --- finding 6: tick and lot sizes the store refuses (fixed, ADR 0023) -----------------------


@pytest.mark.parametrize("changes", [{"tick_size": 0}, {"tick_size": -1}, {"lot_size": 0}],
                         ids=str)  # fmt: skip
def test_finding_06_metadata_with_a_tick_or_lot_the_store_refuses_is_refused(changes):
    """The store's schema requires tick_size and lot_size of at least 1
    (``schema.INSTRUMENTS``); ``scales_fit`` copies only its rule for the
    scales. Phase 9's pre-trade risk will read the tick in force from the book
    (ADR 0022)."""
    meta = plain_meta(**changes)
    try:
        _core.OrderBook(meta, _core.BookCheck.CoinbaseSequence, 8)
        refused_at_startup = False
    except ValueError:
        refused_at_startup = True
    book = coinbase_book(8)
    effects = apply_all(book, snapshot_events(1, [(BUY, 100, 5), (SELL, 101, 3)], venue_seq=1))
    require(effects[-1] == EFFECT.Applied, "the snapshot takes effect")
    update = EV.instrument_update(meta)
    update.seq = 2
    result = book.apply(update)
    assert (refused_at_startup, result.effect, result.reason) == (
        True, EFFECT.WentStale, STALE.InvalidEvent), book.instrument.tick_size.value  # fmt: skip
