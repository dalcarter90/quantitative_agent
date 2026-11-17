"""Driving the C++ order book (``quantagent._core.OrderBook``) in tests
(roadmap Phase 4b, ADR 0022).

The book is given the Phase 4a converter's groups, one event at a time. A
``message``, ``stale`` or ``reset`` group is one step of the reference book
(``book.trace``), and ``apply_group`` reads that step's ``reason``, ``stale``
and ``recovered`` from what the book returned for the group's events, so the
two can be compared step by step. ``check`` and ``instrument`` groups are not
steps of the reference book.
"""

from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from quantagent import _core
from quantagent.data.book import messages_from_table, trace
from quantagent.data.schema import Side
from quantagent.marketevents import EventGroup, convert

INSTRUMENT_ID = 7
BUY, SELL = _core.Side.Buy, _core.Side.Sell
SIDES = ((BUY, Side.BUY), (SELL, Side.SELL))  # the C++ side and the schema's
EFFECT = _core.BookEffect
STALE = _core.StaleReason
KIND = _core.EventKind
EV = _core.MarketEvent
CHECKS = {"kraken": _core.BookCheck.KrakenChecksum, "coinbase": _core.BookCheck.CoinbaseSequence}
ALL = 2**31 - 1  # more levels than any book has: top(side, ALL) is the whole side
NOT_STEPS = ("check", "instrument")  # groups the reference book has no step for


def snake(name: str) -> str:
    """``SequenceGap`` as the reference book and the golden files spell it."""
    return "".join(f"_{c.lower()}" if c.isupper() else c for c in name).lstrip("_")


def core_meta(row: dict[str, Any], instrument: int = INSTRUMENT_ID) -> _core.InstrumentMeta:
    """A stored ``instruments`` row as the C++ ``InstrumentMeta``."""
    return _core.InstrumentMeta(
        _core.InstrumentId(instrument),
        row["price_exp"],
        row["qty_exp"],
        row["price_precision"],
        row["qty_precision"],
        _core.Price(row["tick_size"]),
        _core.Qty(row["lot_size"]),
    )


def depth_of(table: pa.Table) -> int | None:
    """The deepest cut the stored messages ask for, or None for a full book."""
    depths = {d for d in table.column("depth").to_pylist() if d is not None}
    return max(depths) if depths else None


def new_book(venue: str, instruments: pa.Table, *, depth: int | None = None,
             capacity: int | None = None) -> _core.OrderBook:  # fmt: skip
    """The book as it is configured at startup (ADR 0022): the venue's rule, the
    instrument's first metadata version, and a capacity per side: the default,
    or twice the depth for a venue that keeps a cut book."""
    first = min(instruments.to_pylist(), key=lambda r: (r["log_id"], r["seq"]))
    if capacity is None:
        capacity = (_core.OrderBook.DEFAULT_CAPACITY if depth is None
                    else _core.OrderBook.capacity_for_depth(depth))  # fmt: skip
    return _core.OrderBook(core_meta(first), CHECKS[venue], capacity)


@dataclass(frozen=True)
class Outcome:
    """What one group of events did to the book. For a ``message``, ``stale``
    or ``reset`` group these are the fields of the reference book's ``Step``."""

    log_id: str
    seq: int
    step: str
    reason: str | None
    stale: bool
    recovered: bool
    results: tuple[Any, ...]  # every event's BookResult

    @property
    def effects(self) -> list[Any]:
        return [r.effect for r in self.results]


def apply_group(book: _core.OrderBook, group: EventGroup) -> Outcome:
    results = tuple(book.apply(event) for event in group.events)
    failures = [r for r in results if r.effect == EFFECT.WentStale]
    if group.step == "reset":  # the reference gives a reset its stored reason
        reason = snake(group.events[0].reason.name)
    else:  # why a stale step happened, or why a message made the book stale
        reason = snake(failures[0].reason.name) if failures else None
    recovered = any(r.effect == EFFECT.Recovered for r in results)
    return Outcome(group.log_id, group.seq, group.step, reason, book.is_stale, recovered, results)


def sides(book: _core.OrderBook) -> tuple[list[tuple[int, int]], list[tuple[int, int]]]:
    """Every level of both sides, best first."""
    return book.top(BUY, ALL), book.top(SELL, ALL)


def reference_sides(state) -> tuple[list[tuple[int, int]], list[tuple[int, int]]]:
    """The reference book's levels in the same shape; nothing if it has no book."""
    if state is None:
        return [], []
    return state.top(Side.BUY, ALL), state.top(Side.SELL, ALL)


def assert_same_steps(venue: str, table: pa.Table, events, instruments: pa.Table, *,
                      verify: bool = True, capacity: int | None = None,
                      levels: int = ALL):  # fmt: skip
    """Give the reference book (resume mode) and the C++ book the same stored
    data and require the same outcome after every step: the step itself, its
    reason, the stale flag, the recovery, whether there is a book, and the best
    ``levels`` of both sides (every level by default). Returns the book and its
    outcomes for the steps."""
    events = list(events)
    messages = list(messages_from_table(table, instruments))
    reference = trace(venue, messages, events, verify=verify, mode="resume")
    book = new_book(venue, instruments, depth=depth_of(table), capacity=capacity)
    outcomes = []
    for group in convert(table, events, instruments, instrument=INSTRUMENT_ID):
        outcome = apply_group(book, group)
        if group.step in NOT_STEPS:
            continue
        step, state = next(reference)
        where = (group.log_id, group.seq, group.step)
        assert where == (step.log_id, step.seq, step.step)
        assert (outcome.reason, outcome.stale, outcome.recovered) == (
            step.reason, step.stale, step.recovered), where  # fmt: skip
        assert book.has_book == (state is not None), where
        want = ([], []) if state is None else (state.top(Side.BUY, levels),
                                               state.top(Side.SELL, levels))  # fmt: skip
        assert (book.top(BUY, levels), book.top(SELL, levels)) == want, where
        outcomes.append(outcome)
    assert next(reference, None) is None
    return book, outcomes


# --- events built by hand, for what no stored data can say ------------------------------------


def level_event(kind, seq: int, index: int, side, price: int, qty: int, *, last: bool = False,
                venue_seq: int | None = None, checksum: int | None = None,
                depth: int | None = None, source: int = 0,
                instrument: int = INSTRUMENT_ID) -> _core.MarketEvent:  # fmt: skip
    """One book level, as the converter makes them."""
    event = EV()
    event.kind, event.seq, event.source, event.level_index = kind, seq, source, index
    event.instrument = _core.InstrumentId(instrument)
    event.side, event.price, event.qty = side, _core.Price(price), _core.Qty(qty)
    flags = EV.LAST_IN_MESSAGE if last else 0
    if venue_seq is not None:
        event.venue_seq, flags = venue_seq, flags | EV.HAS_VENUE_SEQ
    if checksum is not None:
        event.checksum, flags = checksum, flags | EV.HAS_CHECKSUM
    if depth is not None:
        event.depth, flags = depth, flags | EV.HAS_DEPTH
    event.flags = flags
    return event


def control_event(kind, seq: int, *, count: int = 0, reason=_core.StreamReason.NoReason,
                  source: int = 0, instrument: int = INSTRUMENT_ID):  # fmt: skip
    """A frame event (``count`` levels) or a stale or reset event."""
    event = EV()
    event.kind, event.seq, event.source, event.level_index = kind, seq, source, count
    event.instrument = _core.InstrumentId(instrument)
    event.reason = reason
    event.flags = EV.EVENT_TS_FROM_RECEIPT
    return event


def snapshot_events(seq: int, levels, *, kind=None, source: int = 0, **header):
    """A framed snapshot: begin, its levels, end. ``levels`` are (side, price, qty)."""
    kind = KIND.BookSnapshot if kind is None else kind
    count = len(levels)
    body = [level_event(kind, seq, i, side, price, qty, last=i == count - 1, source=source,
                        **header)
            for i, (side, price, qty) in enumerate(levels)]  # fmt: skip
    return [control_event(KIND.SnapshotBegin, seq, count=count, source=source), *body,
            control_event(KIND.SnapshotEnd, seq, count=count, source=source)]  # fmt: skip


def update_events(seq: int, levels, *, source: int = 0, **header):
    """An update's levels, the last flagged."""
    count = len(levels)
    return [level_event(KIND.BookDelta, seq, i, side, price, qty, last=i == count - 1,
                        source=source, **header)
            for i, (side, price, qty) in enumerate(levels)]  # fmt: skip


def apply_all(book: _core.OrderBook, events) -> list[Any]:
    """Every event's effect, in order."""
    return [book.apply(event).effect for event in events]
