"""Stored book events and stream events, converted to the C++ core's MarketEvents.

The rules are ADR 0020's (architecture section 6.3); this module's own choices
are ADR 0021's. It is how the C++ order book is driven from recorded data in
tests (roadmap Phase 4), and what the C++ feed handlers' events are compared
with (Phase 6). It reads nothing itself: its input is what a ``PITSession``
returns.

    with PITSession(store, available_at=t) as session:
        book = session.book_events(venue, instrument, start, end)
        events = session.stream_events(venue, instrument, start, end).to_pylist()
        meta = session.instruments(venue, instrument)
    for group in convert(book, events, meta, instrument=1):
        for event in group.events:
            ...

``convert`` yields one ``EventGroup`` per step, in the reference book's order
(``book.trace``): records in ``(log_id, seq)`` order, and at one record its
control events (in ``event_index`` order), then its instrument update, then
its levels.

- A stream message (``step="message"``) is its levels, each a ``MarketEvent``
  with the row's price, quantity, side, venue time and position, and the
  message's header (``venue_seq``, ``checksum``, ``depth``, each with its
  flag), the last level flagged ``kLastInMessage``. A snapshot is framed:
  ``SnapshotBegin``, its levels, ``SnapshotEnd``, both frame events carrying
  the level count.
- A check snapshot (``step="check"``, not a step of the reference book) is
  framed the same way, on the log's probe source.
- A stored event the book acts on (``book.STALE_REASONS``,
  ``book.RESET_REASONS``), on the stream connection or of the session, is one
  control event (``step="stale"`` or ``"reset"``): ``StreamStale`` or
  ``StreamReset`` with the ``StreamReason`` of the same name, its
  ``level_index`` the stored ``event_index``. A sequence event also says which
  venue number arrived. Every other stored event is left out.
- A stored metadata version (an ``instruments`` row, ADR 0022) is one
  ``InstrumentUpdate`` (``step="instrument"``, not a step of the reference
  book) at its record, on its log's stream source: the C++ book takes the
  precisions Kraken's checksum is computed with from the stream, at exactly
  the record where they changed. Every version is one event, changed or not.

Frames and control events are stamped with the record's receipt time
(``kEventTsFromReceipt``) and carry no market values. Trades are not
converted yet; the replay engine needs them first (roadmap Phase 7).
"""

import heapq
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any

import pyarrow as pa

from quantagent import _core
from quantagent.data.book import RESET_REASONS, STALE_REASONS, BookMessage, messages_from_table
from quantagent.data.schema import EVENT_TS_RECEIPT, BookEventKind, StreamEventKind

# What MarketEvent can hold (review D7). depth is 16 bits; an exponent is 0 to
# 18 in the schema, and anything else would make a mantissa meaningless.
MAX_DEPTH = 2**16 - 1
MAX_EXPONENT = 18
MAX_SOURCE = 2**32 - 1

STREAM, PROBE = "ws", "probe"  # the two connections a source can be (ADR 0020)
STEPS = ("message", "check", "stale", "reset", "instrument")
_INSTRUMENT_COLUMNS = (
    "log_id", "seq", "recv_ts", "event_ts", "event_ts_source", "price_exp", "qty_exp",
    "price_precision", "qty_precision", "tick_size", "lot_size",
)  # fmt: skip

_EV = _core.MarketEvent
_LEVEL_KINDS = {
    BookEventKind.SNAPSHOT: _core.EventKind.BookSnapshot,
    BookEventKind.DELTA: _core.EventKind.BookDelta,
    BookEventKind.CHECK_SNAPSHOT: _core.EventKind.BookCheckSnapshot,
}
_SIDES = {1: _core.Side.Buy, -1: _core.Side.Sell}
_SEQUENCE_KINDS = frozenset(
    {StreamEventKind.SEQUENCE_GAP, StreamEventKind.SEQUENCE_REPEAT,
     StreamEventKind.SEQUENCE_OUT_OF_ORDER}
)  # fmt: skip


def _camel(snake: str) -> str:
    return "".join(part.capitalize() for part in snake.split("_"))


# The StreamReason of each stored kind the book acts on, by name, so the
# converter and the reference book cannot disagree about which they are.
_CONTROL: dict[StreamEventKind, tuple[str, Any, Any]] = {
    **{k: ("stale", _core.EventKind.StreamStale, getattr(_core.StreamReason, _camel(name)))
       for k, name in STALE_REASONS.items()},
    **{k: ("reset", _core.EventKind.StreamReset, getattr(_core.StreamReason, _camel(name)))
       for k, name in RESET_REASONS.items()},
}  # fmt: skip


class SourceMap:
    """Numbers the sources, one connection of one raw log each, ``(log_id,
    "ws")`` or ``(log_id, "probe")``, in the order they are first asked for,
    from ``first`` (0 by default). The harness keeps it and shares it across
    conversions, so every instrument's events of one log name the same
    source."""

    def __init__(self, first: int = 0) -> None:
        self._next = first
        self._ids: dict[tuple[str, str], int] = {}
        self._connections: dict[int, tuple[str, str]] = {}

    def source(self, log_id: str, connection: str) -> int:
        if connection not in (STREAM, PROBE):
            raise ValueError(f"a source's connection is {STREAM!r} or {PROBE!r}, not "
                             f"{connection!r}")  # fmt: skip
        key = (log_id, connection)
        found = self._ids.get(key)
        if found is not None:
            return found
        if self._next > MAX_SOURCE:
            raise OverflowError(f"no source number left for {key}: MarketEvent.source is 32 bits")
        self._ids[key], self._connections[self._next] = self._next, key
        self._next += 1
        return self._ids[key]

    def connection(self, source: int) -> tuple[str, str]:
        """The (log_id, connection) a source number stands for."""
        return self._connections[source]

    def items(self) -> Iterator[tuple[int, tuple[str, str]]]:
        return iter(self._connections.items())

    def __len__(self) -> int:
        return len(self._ids)


@dataclass(frozen=True, slots=True)
class EventGroup:
    """The events of one step: one record's message, check snapshot, or one of
    its control events (``step`` is one of ``STEPS``)."""

    log_id: str
    seq: int
    step: str
    events: tuple[_core.MarketEvent, ...]


def _receipt_stamped(kind: Any, recv_ts: int, seq: int, source: int,
                     instrument: Any) -> _core.MarketEvent:  # fmt: skip
    event = _EV()
    event.recv_ts = _core.Timestamp(recv_ts)
    event.event_ts = _core.Timestamp(recv_ts)
    event.seq = seq
    event.source = source
    event.instrument = instrument
    event.kind = kind
    event.flags = _EV.EVENT_TS_FROM_RECEIPT
    return event


def _check_message(message: BookMessage, exponents: list[tuple[int, int]]) -> None:
    where = f"message (log {message.log_id}, seq {message.seq})"
    if message.price_exp is None or message.qty_exp is None:
        raise ValueError(f"{where} has no instrument metadata in force; pass the instrument's "
                         "versions (PITSession.instruments)")  # fmt: skip
    for name, value in (("price_exp", message.price_exp), ("qty_exp", message.qty_exp)):
        if not 0 <= value <= MAX_EXPONENT:
            raise ValueError(f"{where}: {name} {value} is outside 0 to {MAX_EXPONENT}")
    numbers = (message.price_exp, message.qty_exp)
    if not exponents:
        exponents.append(numbers)
    elif numbers != exponents[0]:
        raise ValueError(
            f"{where}: exponents {numbers} differ from {exponents[0]} earlier in "
            "the same conversion; an instrument's exponents are fixed (ADR 0019)"
        )
    if message.depth is not None and not 0 <= message.depth <= MAX_DEPTH:
        raise ValueError(f"{where}: depth {message.depth} does not fit MarketEvent's 16-bit "
                         f"depth (at most {MAX_DEPTH})")  # fmt: skip


def _check_version(version: dict[str, Any], exponents: list[tuple[int, int]]) -> None:
    """What an InstrumentUpdate cannot carry, or an instrument cannot have."""
    where = f"instruments row (log {version['log_id']}, seq {version['seq']})"
    for name in ("price_exp", "qty_exp"):
        if not 0 <= version[name] <= MAX_EXPONENT:
            raise ValueError(f"{where}: {name} {version[name]} is outside 0 to {MAX_EXPONENT}")
    for name, exponent in (("price_precision", "price_exp"), ("qty_precision", "qty_exp")):
        if not 0 <= version[name] <= version[exponent]:
            raise ValueError(f"{where}: {name} {version[name]} is outside 0 to its exponent "
                             f"{version[exponent]}")  # fmt: skip
    numbers = (version["price_exp"], version["qty_exp"])
    if not exponents:
        exponents.append(numbers)
    elif numbers != exponents[0]:
        raise ValueError(
            f"{where}: exponents {numbers} differ from {exponents[0]} earlier in the same "
            "conversion; an instrument's exponents are fixed (ADR 0019)"
        )


def _instrument_group(version: dict[str, Any], sources: SourceMap, instrument: Any) -> EventGroup:
    meta = _core.InstrumentMeta(instrument, version["price_exp"], version["qty_exp"],
                                version["price_precision"], version["qty_precision"],
                                _core.Price(version["tick_size"]),
                                _core.Qty(version["lot_size"]))  # fmt: skip
    event = _EV.instrument_update(meta)
    event.recv_ts = _core.Timestamp(version["recv_ts"])
    event.event_ts = _core.Timestamp(version["event_ts"])
    event.seq = version["seq"]
    # Whatever channel it came on, a metadata version belongs to its log's stream.
    event.source = sources.source(version["log_id"], STREAM)
    if version["event_ts_source"] == EVENT_TS_RECEIPT:
        event.flags = _EV.EVENT_TS_FROM_RECEIPT
    return EventGroup(version["log_id"], version["seq"], "instrument", (event,))


def _message_group(message: BookMessage, times: dict[tuple[str, int, int], tuple[int, str]],
                   sources: SourceMap, instrument: Any) -> EventGroup:  # fmt: skip
    check = message.kind == BookEventKind.CHECK_SNAPSHOT
    source = sources.source(message.log_id, PROBE if check else STREAM)
    header = 0
    if message.venue_seq_last is not None:
        header |= _EV.HAS_VENUE_SEQ
    if message.checksum is not None:
        header |= _EV.HAS_CHECKSUM
    if message.depth is not None:
        header |= _EV.HAS_DEPTH
    kind = _LEVEL_KINDS[message.kind]
    count = len(message.levels)
    levels = []
    for index, (side, price, qty) in enumerate(message.levels):
        event_ts, event_ts_source = times[(message.log_id, message.seq, index)]
        event = _EV()
        event.recv_ts = _core.Timestamp(message.recv_ts)
        event.event_ts = _core.Timestamp(event_ts)
        event.seq = message.seq
        event.source = source
        event.instrument = instrument
        event.kind = kind
        event.side = _SIDES[int(side)]
        event.price = _core.Price(price)
        event.qty = _core.Qty(qty)
        event.level_index = index
        if message.venue_seq_last is not None:
            event.venue_seq = message.venue_seq_last
        if message.checksum is not None:
            event.checksum = message.checksum
        if message.depth is not None:
            event.depth = message.depth
        flags = header
        if event_ts_source == EVENT_TS_RECEIPT:
            flags |= _EV.EVENT_TS_FROM_RECEIPT
        if index == count - 1:
            flags |= _EV.LAST_IN_MESSAGE
        event.flags = flags
        levels.append(event)
    if message.kind == BookEventKind.DELTA:
        events = tuple(levels)
    else:  # a snapshot, of the stream or a check, is framed (ADR 0020)
        frames = []
        for frame_kind in (_core.EventKind.SnapshotBegin, _core.EventKind.SnapshotEnd):
            frame = _receipt_stamped(frame_kind, message.recv_ts, message.seq, source, instrument)
            frame.level_index = count
            frames.append(frame)
        events = (frames[0], *levels, frames[1])
    return EventGroup(message.log_id, message.seq, "check" if check else "message", events)


def _control_group(row: dict[str, Any], sources: SourceMap, instrument: Any) -> EventGroup:
    step, kind, reason = _CONTROL[StreamEventKind(row["kind"])]
    source = sources.source(row["log_id"], STREAM)  # a session event belongs to the stream
    event = _receipt_stamped(kind, row["recv_ts"], row["seq"], source, instrument)
    event.reason = reason
    event.level_index = row["event_index"]
    if StreamEventKind(row["kind"]) in _SEQUENCE_KINDS and row["observed"] is not None:
        event.venue_seq = row["observed"]
        event.flags = event.flags | _EV.HAS_VENUE_SEQ
    return EventGroup(row["log_id"], row["seq"], step, (event,))


def _acts_on_book(row: dict[str, Any]) -> bool:
    return row["connection"] in (STREAM, None) and StreamEventKind(row["kind"]) in _CONTROL


def convert(
    book_events: pa.Table,
    stream_events: Iterable[dict[str, Any]],
    instruments: pa.Table,
    *,
    instrument: int,
    sources: SourceMap | None = None,
) -> Iterator[EventGroup]:
    """The MarketEvents of one instrument's stored data, one group per step.

    ``book_events`` and ``instruments`` are the tables ``PITSession`` returns,
    ``stream_events`` its rows (``.to_pylist()``); ``instrument`` is the
    ``InstrumentId`` the events carry; ``sources`` numbers the sources (a new
    map unless one is shared). Raises ``ValueError``, before yielding
    anything, for what a MarketEvent cannot carry: a message with no metadata
    in force, an exponent outside 0 to 18 or different from another's, a
    precision outside 0 to its exponent, a depth above 65,535.
    """
    sources = SourceMap() if sources is None else sources
    instrument_id = _core.InstrumentId(instrument)
    names = ("log_id", "seq", "level_index", "event_ts", "event_ts_source")
    columns = [book_events.column(name).to_pylist() for name in names]
    times = {(log, seq, index): (event_ts, source)
             for log, seq, index, event_ts, source in zip(*columns, strict=True)}  # fmt: skip
    messages = list(messages_from_table(book_events, instruments))  # (log_id, seq) order
    columns = [instruments.column(name).to_pylist() for name in _INSTRUMENT_COLUMNS]
    versions = sorted((dict(zip(_INSTRUMENT_COLUMNS, values, strict=True))
                       for values in zip(*columns, strict=True)),
                      key=lambda v: (v["log_id"], v["seq"]))  # fmt: skip
    exponents: list[tuple[int, int]] = []
    for version in versions:
        _check_version(version, exponents)
    for message in messages:
        _check_message(message, exponents)
    controls = sorted((row for row in stream_events if _acts_on_book(row)),
                      key=lambda r: (r["log_id"], r["seq"], r["event_index"]))  # fmt: skip
    # At one record: its control events (0), its instrument update (1), its message (2).
    keyed = heapq.merge(
        (((r["log_id"], r["seq"], 0, r["event_index"]), "control", r) for r in controls),
        (((v["log_id"], v["seq"], 1, 0), "instrument", v) for v in versions),
        (((m.log_id, m.seq, 2, 0), "message", m) for m in messages),
        key=lambda item: item[0],
    )
    for _, what, item in keyed:
        if what == "message":
            yield _message_group(item, times, sources, instrument_id)
        elif what == "instrument":
            yield _instrument_group(item, sources, instrument_id)
        else:
            yield _control_group(item, sources, instrument_id)
