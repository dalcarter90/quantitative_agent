"""Coinbase Advanced Trade, public WebSocket and REST market data.

Rules checked against the venue's documentation and against recorded data
before this was written
(https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-channels):

- ``level2``, ``market_trades`` and ``heartbeats`` are public. No key is used.
- Every WebSocket message carries ``sequence_num``. Recorded data shows it
  counts every message **on one connection, across all channels**, starting at
  0: heartbeats, book and trades share one numbering. A jump means a message
  was lost. Because the numbering is per connection, a book snapshot taken on
  another connection cannot be lined up with the stream by sequence number,
  only by time.
- ``level2`` sends the full book as a snapshot on subscribe, then updates.
  ``new_quantity`` is the new size at the level; ``"0"`` removes it. Each
  level carries its own ``event_time``.
- ``market_trades``: ``side`` is the **maker's** side, so the aggressor is the
  opposite. The subscription starts with a snapshot of recent trades.
- Product metadata from REST has no timestamp, so those rows take the receipt
  time (ADR 0008).
- Prices and sizes are decimal strings.

**Check snapshots (ADR 0014).** Recorded data shows the ``level2`` stream comes
in batches about 50 ms apart, every update in a message carrying the same
``event_time``, and an independent snapshot (from a probe connection) carrying
one time T for all its levels. When T is a batch's time the snapshot equals the
book after that batch. When T falls between batch A and batch B, it equals the
book after A on every level B does not list; on B's levels it may show A's
value, B's value, or a value the stream never sent (the venue reports only each
level's last value per batch). This adapter keeps the book rebuilt from the
stream, with the last ``CHECK_HISTORY_NS`` of messages so it can see the book
as it was at T, and writes one CHECK_SNAPSHOT_COMPARED stream event per check
snapshot. The reference reconstructor applies the same rule independently.
"""

import json
from bisect import bisect_left, bisect_right
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from quantagent.data.adapters.base import (
    PROBE_CHANNEL,
    STREAM_CHANNEL,
    Handled,
    Rejected,
    Row,
    SequenceAnomaly,
    VenueAdapter,
    _compact,
    as_list,
    as_object,
    data,
    ignored,
    increment,
    load_json,
    receipt_time,
    venue_time,
)
from quantagent.data.fixedpoint import to_mantissa
from quantagent.data.rawlog import RawRecord
from quantagent.data.schema import BookEventKind, Side, StreamEventKind

PRODUCT_CHANNEL = "rest:product"
_BOOK_KINDS = {
    (STREAM_CHANNEL, "snapshot"): BookEventKind.SNAPSHOT,
    (STREAM_CHANNEL, "update"): BookEventKind.DELTA,
    (PROBE_CHANNEL, "snapshot"): BookEventKind.CHECK_SNAPSHOT,
}
_BOOK_SIDES = {"bid": Side.BUY, "offer": Side.SELL}
_AGGRESSOR_FOR_MAKER = {"BUY": Side.SELL, "SELL": Side.BUY}
_SEQUENCE_EVENTS = {
    "gap": StreamEventKind.SEQUENCE_GAP,
    "repeat": StreamEventKind.SEQUENCE_REPEAT,
    "out_of_order": StreamEventKind.SEQUENCE_OUT_OF_ORDER,
}
# How much of the stream is kept to see the book as it was at a check
# snapshot's time. Recorded snapshots arrive at most 1.7 s after their time.
CHECK_HISTORY_NS = 60 * 1_000_000_000

Book = dict[Side, dict[int, int]]
Level = tuple[Side, int, int]


@dataclass
class _Applied:
    """A stream message as applied to the book, with what it replaced."""

    time: int
    kind: BookEventKind
    levels: list[Level]
    undo: list[tuple[Side, int, int | None]] = field(default_factory=list)  # deltas
    previous: Book | None = None  # snapshots: the book it replaced


@dataclass
class _Check:
    seq: int
    time: int
    levels: list[Level]


class _StreamBook:
    """The book rebuilt from the stream connection, and its recent past."""

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self.book: Book | None = None
        self.history: deque[_Applied] = deque()
        self.trimmed = False  # older messages were dropped from the history
        self.unreliable = False  # a message was lost; wait for the next snapshot

    def lost(self) -> None:
        self.reset()
        self.unreliable = True

    def apply(self, time: int, kind: BookEventKind, levels: list[Level]) -> None:
        if kind == BookEventKind.SNAPSHOT:
            previous, book = self.book, {Side.BUY: {}, Side.SELL: {}}
            for side, price, qty in levels:
                if qty:
                    book[side][price] = qty
                else:
                    book[side].pop(price, None)
            if previous is None:
                self.history.clear()
                self.trimmed = False
            self.book, self.unreliable = book, False
            self.history.append(_Applied(time, kind, levels, previous=previous))
        else:
            if self.book is None:
                return  # nothing to apply it to (the reconstructor raises here)
            undo = []
            for side, price, qty in levels:
                undo.append((side, price, self.book[side].get(price)))
                if qty:
                    self.book[side][price] = qty
                else:
                    self.book[side].pop(price, None)
            self.history.append(_Applied(time, kind, levels, undo=undo))
        while self.history and self.history[0].time < time - CHECK_HISTORY_NS:
            self.history.popleft()
            self.trimmed = True

    def as_before(self, index: int) -> Book:
        """The book before the history's message ``index``."""
        assert self.book is not None
        state = {side: dict(levels) for side, levels in self.book.items()}
        for applied in reversed(list(self.history)[index:]):
            if applied.kind == BookEventKind.SNAPSHOT:
                assert applied.previous is not None
                state = {side: dict(levels) for side, levels in applied.previous.items()}
                continue
            for side, price, qty in reversed(applied.undo):
                if qty is None:
                    state[side].pop(price, None)
                else:
                    state[side][price] = qty
        return state


class CoinbaseAdapter(VenueAdapter):
    venue = "coinbase"

    def __init__(self) -> None:
        super().__init__()
        self._last_sequence: dict[str, int] = {}  # highest sequence_num seen per connection
        self._stream = _StreamBook()
        self._waiting: list[_Check] = []  # check snapshots whose next batch has not come yet

    def on_connect(self, connection: str) -> None:
        self._last_sequence.pop(connection, None)
        if connection == STREAM_CHANNEL:
            self._stream_ended()

    def on_disconnect(self, connection: str) -> None:
        if connection == STREAM_CHANNEL:
            self._stream_ended()

    def on_session_stop(self) -> None:
        self._stream_ended()

    def on_session_start(self) -> None:
        # Snapshots still waiting belong to a log that ended without its stop
        # record. Their event would name the wrong log, so none is written.
        self._waiting.clear()
        self._stream.reset()

    def on_reject(self, record: RawRecord) -> None:
        if record.channel == STREAM_CHANNEL:
            self._stream_lost()

    def _stream_ended(self) -> None:
        """No batch follows: waiting snapshots are judged on every level."""
        for check in self._waiting:
            self._compare(check, self._stream.book, self._stream.history[-1].time, None)
        self._waiting.clear()
        self._stream.reset()

    def _stream_lost(self) -> None:
        for check in self._waiting:
            self._not_compared(check, "stream_unreliable")
        self._waiting.clear()
        self._stream.lost()

    def handle_venue(self, record: RawRecord) -> Handled:
        if record.channel == PRODUCT_CHANNEL:
            return self._product(record)
        if record.channel not in (STREAM_CHANNEL, PROBE_CHANNEL):
            raise Rejected("unknown_channel", record.channel)
        message = as_object(load_json(record.payload))

        if message.get("type") == "error":
            raise Rejected("venue_error", str(message.get("message")))
        channel = message.get("channel")
        if channel is None:
            raise Rejected("unknown_message", "no channel")
        if "sequence_num" in message:
            self._track_sequence(record, message["sequence_num"])
        if channel == "error":
            raise Rejected("venue_error", str(message.get("events")))
        if channel == "subscriptions":
            confirmed = as_object(as_list(message["events"], "events")[0], "event")
            self.note(
                StreamEventKind.SUBSCRIPTION_ACK,
                connection=record.channel,
                detail=json.dumps(confirmed.get("subscriptions"), separators=(",", ":")),
            )
            return ignored(channel)
        if channel == "heartbeats":
            return ignored(channel)
        if channel == "l2_data":
            return self._book(record, message)
        if channel == "market_trades":
            return self._trades(record, message)
        raise Rejected("unknown_message", f"channel {channel!r}")

    def _track_sequence(self, record: RawRecord, received: Any) -> None:
        if type(received) is not int:
            raise TypeError(f"sequence_num is not an integer: {received!r}")
        last = self._last_sequence.get(record.channel)
        if last is not None and received != last + 1:
            if record.channel == STREAM_CHANNEL:
                self._stream_lost()
            kind = "gap" if received > last else "repeat" if received == last else "out_of_order"
            self.anomalies.append(
                SequenceAnomaly(
                    kind, record.channel, last + 1, received, record.seq, record.recv_ts
                )
            )
            self.note(
                _SEQUENCE_EVENTS[kind],
                connection=record.channel,
                expected=last + 1,
                observed=received,
            )
        if last is None or received > last:
            self._last_sequence[record.channel] = received

    def _product(self, record: RawRecord) -> Handled:
        product = as_object(load_json(record.payload), "product")
        self.require_symbol(product["product_id"])
        price_exp, tick_size = increment(product["price_increment"], "price_increment")
        qty_exp, lot_size = increment(product["base_increment"], "base_increment")
        row = {
            **receipt_time(record),
            "recv_ts": record.recv_ts,
            "seq": record.seq,
            "price_exp": price_exp,
            "qty_exp": qty_exp,
            "tick_size": tick_size,
            "lot_size": lot_size,
        }
        self.exponents = (price_exp, qty_exp)
        return "data", "product", [("instruments", row)]

    def _book(self, record: RawRecord, message: dict) -> Handled:
        events = as_list(message["events"], "events")
        sequence = message["sequence_num"]
        self.require_session()
        rows: list[tuple[str, Row]] = []
        kinds = set()
        levels: list[Level] = []
        index = 0
        for event in events:
            kind = as_object(event, "event").get("type")
            if kind not in ("snapshot", "update"):
                raise Rejected("unknown_message", f"l2_data event type {kind!r}")
            self.require_symbol(event["product_id"])
            updates = as_list(event["updates"], "updates")
            book_kind = _BOOK_KINDS.get((record.channel, kind))
            if book_kind is None:
                continue  # an update on a probe connection
            kinds.add(kind)
            price_exp, qty_exp = self.require_exponents()
            header = {
                "recv_ts": record.recv_ts,
                "seq": record.seq,
                "kind": book_kind,
                "venue_seq_first": None,
                "venue_seq_last": sequence,
                "checksum": None,
                "depth": None,  # the full book
            }
            for update in updates:
                side = _BOOK_SIDES.get(as_object(update, "level")["side"])
                if side is None:
                    raise ValueError(f"unknown book side {update['side']!r}")
                row = {
                    **venue_time(update["event_time"]),
                    **header,
                    "level_index": index,
                    "side": side,
                    "price": to_mantissa(update["price_level"], price_exp),
                    "qty": to_mantissa(update["new_quantity"], qty_exp),
                }
                rows.append(("book_events", row))
                levels.append((side, row["price"], row["qty"]))
                index += 1
        if not kinds:
            return ignored(f"{record.channel}.l2_data.update" if events else "l2_data.empty")
        name = "snapshot" if "snapshot" in kinds else "update"
        if rows:  # the whole message parsed: now it may change the book
            time = max(row["event_ts"] for _, row in rows)
            if record.channel == PROBE_CHANNEL:
                self._check(_Check(record.seq, time, levels))
            else:
                kind = BookEventKind.SNAPSHOT if name == "snapshot" else BookEventKind.DELTA
                self._stream_message(time, kind, levels)
        return data(f"l2_data.{name}", rows, empty=f"l2_data.{name}.empty")

    # --- check snapshots (ADR 0014) -----------------------------------------------------

    def _stream_message(self, time: int, kind: BookEventKind, levels: list[Level]) -> None:
        stream = self._stream
        for check in [c for c in self._waiting if time > c.time]:
            # This message is the batch after the snapshot's time.
            self._waiting.remove(check)
            if kind == BookEventKind.SNAPSHOT:
                self._not_compared(check, "next_message_is_snapshot")
            else:
                self._compare(check, stream.book, stream.history[-1].time, levels)
        stream.apply(time, kind, levels)

    def _check(self, check: _Check) -> None:
        stream = self._stream
        if stream.unreliable:
            return self._not_compared(check, "stream_unreliable")
        if stream.book is None:
            return self._not_compared(check, "no_stream_book")
        history = stream.history
        later = next((i for i, applied in enumerate(history) if applied.time > check.time), None)
        if later is None:
            self._waiting.append(check)  # decided when the next batch comes
        elif later == 0:
            self._not_compared(
                check, "history_too_short" if stream.trimmed else "before_stream_snapshot"
            )
        elif history[later].kind == BookEventKind.SNAPSHOT:
            self._not_compared(check, "next_message_is_snapshot")
        else:
            book = stream.as_before(later)
            self._compare(check, book, history[later - 1].time, history[later].levels)
        return None

    def _not_compared(self, check: _Check, reason: str) -> None:
        counts = ("violations", "violations_shallowest", "violations_deepest",
                  "unverified", "unverified_as_next_batch", "unverified_shallowest",
                  "unverified_deepest")  # fmt: skip
        facts = {"result": "not_compared", "reason": reason, "on_batch": None,
                 "next_batch_levels": 0, **dict.fromkeys(counts)}  # fmt: skip
        self._note_check(check, facts)

    def _compare(
        self, check: _Check, book: Book | None, a_time: int, next_batch: list[Level] | None
    ) -> None:
        """``book`` is the book after A, the last message at or before the
        snapshot's time; ``next_batch`` the levels of B, the message after it."""
        assert book is not None
        theirs: dict[tuple[Side, int], int] = {(s, p): q for s, p, q in check.levels}
        after_b: dict[tuple[Side, int], int | None] = {}
        for side, price, qty in next_batch or ():
            after_b[(side, price)] = qty or None
        on_batch = a_time == check.time
        exempt = {} if on_batch else after_b
        bids = sorted(p for s, p, _ in check.levels if s == Side.BUY)
        asks = sorted(p for s, p, _ in check.levels if s == Side.SELL)

        def rank(side: Side, price: int) -> int:
            if side == Side.BUY:
                return len(bids) - bisect_right(bids, price)
            return bisect_left(asks, price)

        violations, unverified, as_next_batch = [], [], 0
        ours = {(side, price): qty for side in book for price, qty in book[side].items()}
        for level in ours.keys() | theirs.keys():
            if ours.get(level) == theirs.get(level):
                continue
            if level in exempt:
                unverified.append(rank(*level))
                as_next_batch += theirs.get(level) == exempt[level]
            else:
                violations.append(rank(*level))
        if violations:
            result = "mismatch"
        else:
            result = "explained" if unverified else "exact"
        self._note_check(check, {
            "result": result, "reason": None, "on_batch": on_batch,
            "next_batch_levels": len(after_b),
            "violations": len(violations),
            "violations_shallowest": min(violations, default=None),
            "violations_deepest": max(violations, default=None),
            "unverified": len(unverified), "unverified_as_next_batch": as_next_batch,
            "unverified_shallowest": min(unverified, default=None),
            "unverified_deepest": max(unverified, default=None),
        })  # fmt: skip

    def _note_check(self, check: _Check, facts: dict[str, Any]) -> None:
        detail = {"snapshot_seq": check.seq, "levels": len(check.levels), **facts}
        self.note(StreamEventKind.CHECK_SNAPSHOT_COMPARED, connection=PROBE_CHANNEL,
                  expected=facts["violations"], observed=facts["unverified"],
                  detail=_compact(detail))  # fmt: skip

    def _trades(self, record: RawRecord, message: dict) -> Handled:
        events = as_list(message["events"], "events")
        self.require_session()
        rows: list[tuple[str, Row]] = []
        name = "update"
        for event in events:
            kind = as_object(event, "event").get("type")
            if kind not in ("snapshot", "update"):
                raise Rejected("unknown_message", f"market_trades event type {kind!r}")
            if kind == "snapshot" and name != "snapshot":
                name = "snapshot"
                self.note(StreamEventKind.TRADE_BACKFILL, connection=record.channel)
            for trade in as_list(event["trades"], "trades"):
                self.require_symbol(as_object(trade, "trade")["product_id"])
                price_exp, qty_exp = self.require_exponents()
                trade_id, maker_side = trade["trade_id"], trade["side"]
                if not isinstance(trade_id, str) or not trade_id:
                    raise TypeError("trade_id is not a non-empty string")
                if maker_side not in _AGGRESSOR_FOR_MAKER:
                    raise ValueError(f"unknown trade side {maker_side!r}")
                row = {
                    **venue_time(trade["time"]),
                    "recv_ts": record.recv_ts,
                    "seq": record.seq,
                    "trade_id": trade_id,
                    "price": to_mantissa(trade["price"], price_exp),
                    "qty": to_mantissa(trade["size"], qty_exp),
                    "side": _AGGRESSOR_FOR_MAKER[maker_side],
                }
                rows.append(("trades", row))
        return data(f"market_trades.{name}", rows, empty=f"market_trades.{name}.empty")
