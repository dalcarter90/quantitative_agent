"""Kraken spot, WebSocket API v2.

Rules checked against the venue's documentation and against recorded data
before this was written:

- Book (https://docs.kraken.com/api/docs/websocket-v2/book and
  https://docs.kraken.com/api/docs/guides/spot-ws-book-v2): there are **no
  sequence numbers**. Each snapshot and update carries a CRC32 ``checksum`` of
  the top 10 levels per side after the message is applied. The subscription
  has a depth; the client must drop levels that fall outside it, because the
  venue sends no removal for them. One update can change the same price more
  than once, so the order of levels within a message matters. Snapshots and
  updates both carry a venue ``timestamp``.
- Trades (https://docs.kraken.com/api/docs/websocket-v2/trade): ``side`` is the
  taker's side. ``trade_id`` is a per-book sequence number.
- Instrument metadata (the ``instrument`` channel) has no venue timestamp, so
  those rows take the receipt time (ADR 0008).
- Prices and quantities arrive as JSON numbers, always printed with the pair's
  full precision. They are read as text so no digit is lost; the checksum is
  defined over those digits.

Because the checksum is Kraken's only protection for the book, this adapter
verifies it while normalizing, the way it checks Coinbase's sequence numbers:
it keeps the top of each connection's book, applies every message, and writes
a CHECKSUM_MISMATCH stream event when the venue's checksum and its own differ
(ADR 0010). After a mismatch the book is suspect, so nothing more is reported
for that connection until its next snapshot. The checksum covers only the ten
best levels per side: a lost update below them goes unnoticed unless its level
later reaches the top ten.
"""

import json
import zlib
from typing import Any

from quantagent.data.adapters.base import (
    PROBE_CHANNEL,
    STREAM_CHANNEL,
    Handled,
    Rejected,
    Row,
    VenueAdapter,
    as_list,
    as_object,
    checked_depth,
    data,
    ignored,
    load_json,
    positive,
    receipt_time,
    text_int,
    venue_time,
)
from quantagent.data.fixedpoint import to_mantissa
from quantagent.data.rawlog import RawRecord
from quantagent.data.schema import BookEventKind, Side, StreamEventKind

_BOOK_KINDS = {
    (STREAM_CHANNEL, "snapshot"): BookEventKind.SNAPSHOT,
    (STREAM_CHANNEL, "update"): BookEventKind.DELTA,
    (PROBE_CHANNEL, "snapshot"): BookEventKind.CHECK_SNAPSHOT,
}
_TAKER_SIDES = {"buy": Side.BUY, "sell": Side.SELL}
_MAX_CHECKSUM = 2**32 - 1
_CHECKSUM_LEVELS = 10

Book = dict[Side, dict[int, int]]  # side -> price mantissa -> quantity mantissa


def _apply(book: Book, levels: list[tuple[Side, int, int]], depth: int | None) -> None:
    """Apply levels in message order, then keep ``depth`` levels per side."""
    for side, price, qty in levels:
        if qty == 0:
            book[side].pop(price, None)
        else:
            book[side][price] = qty
    if depth is not None:
        # The venue sends no removal for a level pushed out of the subscribed depth.
        for side, best_first in ((Side.BUY, True), (Side.SELL, False)):
            if len(book[side]) > depth:
                kept = sorted(book[side], reverse=best_first)[:depth]
                book[side] = {price: book[side][price] for price in kept}


def book_checksum(book: Book) -> int:
    """Kraken's CRC32 over the ten best asks (ascending) then bids (descending).

    Each level contributes its price digits then its quantity digits, with the
    decimal point and leading zeros removed. Kraken prints both at the pair's
    full precision, so those digits are exactly the stored mantissas.
    https://docs.kraken.com/api/docs/guides/spot-ws-book-v2
    """
    asks = sorted(book[Side.SELL].items())[:_CHECKSUM_LEVELS]
    bids = sorted(book[Side.BUY].items(), reverse=True)[:_CHECKSUM_LEVELS]
    return zlib.crc32("".join(f"{price}{qty}" for price, qty in (*asks, *bids)).encode())


class KrakenAdapter(VenueAdapter):
    venue = "kraken"

    def __init__(self) -> None:
        super().__init__()
        self._depth: dict[str, int] = {}  # subscribed book depth per connection
        self._books: dict[str, Book] = {}  # the book rebuilt from each connection's stream
        self._suspect: set[str] = set()  # connections whose book failed its checksum

    def on_connect(self, connection: str) -> None:
        self._depth.pop(connection, None)
        self._books.pop(connection, None)
        self._suspect.discard(connection)

    def handle_venue(self, record: RawRecord) -> Handled:
        if record.channel not in (STREAM_CHANNEL, PROBE_CHANNEL):
            raise Rejected("unknown_channel", record.channel)
        message = as_object(load_json(record.payload, numbers_as_text=True))

        if message.get("success") is False or "error" in message:
            raise Rejected("venue_error", str(message.get("error")))
        if "method" in message:
            return self._response(record, message)

        channel, kind = message.get("channel"), message.get("type")
        if channel == "heartbeat":
            return ignored("heartbeat")
        if channel == "status":
            return ignored(f"status.{kind}")
        if kind in ("snapshot", "update"):
            if channel == "instrument":
                return self._instrument(record, message, kind)
            if channel == "book":
                return self._book(record, message, kind)
            if channel == "trade":
                return self._trade(record, message, kind)
        raise Rejected("unknown_message", f"channel {channel!r}, type {kind!r}")

    def _response(self, record: RawRecord, message: dict) -> Handled:
        method = message["method"]
        if method == "pong":
            return ignored("pong")
        if method not in ("subscribe", "unsubscribe"):
            raise Rejected("unknown_message", f"method {method!r}")
        result = as_object(message.get("result", {}), "result")
        if method == "subscribe" and result.get("channel") == "book":
            self._depth[record.channel] = checked_depth(text_int(result["depth"], "depth"))
        # Parsed again without number-as-text, so the detail holds ordinary JSON.
        confirmed = {"method": method, **load_json(record.payload).get("result", {})}
        self.note(
            StreamEventKind.SUBSCRIPTION_ACK,
            connection=record.channel,
            detail=json.dumps(confirmed, separators=(",", ":"), sort_keys=True),
        )
        return ignored(f"{method}.ack")

    def _instrument(self, record: RawRecord, message: dict, kind: str) -> Handled:
        self.require_session()
        rows: list[tuple[str, Row]] = []
        for pair in as_list(as_object(message["data"], "data")["pairs"], "pairs"):
            if as_object(pair, "pair").get("symbol") != self.symbol:
                continue  # the channel lists every pair; only the recorded one is kept
            price_exp = text_int(pair["price_precision"], "price_precision")
            qty_exp = text_int(pair["qty_precision"], "qty_precision")
            row = {
                **receipt_time(record),
                "recv_ts": record.recv_ts,
                "seq": record.seq,
                "price_exp": price_exp,
                "qty_exp": qty_exp,
                "tick_size": positive(
                    to_mantissa(pair["price_increment"], price_exp), "price_increment"
                ),
                "lot_size": positive(to_mantissa(pair["qty_increment"], qty_exp), "qty_increment"),
            }
            rows.append(("instruments", row))
            self.exponents = (price_exp, qty_exp)
        return data(f"instrument.{kind}", rows, empty=f"instrument.{kind}.other_pairs")

    def _book(self, record: RawRecord, message: dict, kind: str) -> Handled:
        entries = as_list(message["data"], "data")
        self.require_session()
        book_kind = _BOOK_KINDS.get((record.channel, kind))
        if book_kind is None:
            return ignored(f"{record.channel}.book.{kind}")  # an update on a probe connection
        depth = self._depth.get(record.channel, self.start_depth)

        rows: list[tuple[str, Row]] = []
        parsed: list[tuple[int, list[tuple[Side, int, int]]]] = []  # (checksum, levels) per entry
        index = 0
        for entry in entries:
            self.require_symbol(as_object(entry, "book entry")["symbol"])
            price_exp, qty_exp = self.require_exponents()
            checksum = text_int(entry["checksum"], "checksum")
            if not 0 <= checksum <= _MAX_CHECKSUM:
                raise ValueError(f"checksum {checksum} is not an unsigned 32-bit value")
            parsed.append((checksum, []))
            header = {
                **venue_time(entry["timestamp"]),
                "recv_ts": record.recv_ts,
                "seq": record.seq,
                "kind": book_kind,
                "venue_seq_first": None,
                "venue_seq_last": None,
                "checksum": checksum,
                "depth": depth,
            }
            for side, levels in ((Side.BUY, entry["bids"]), (Side.SELL, entry["asks"])):
                for level in as_list(levels, "levels"):
                    row = {
                        **header,
                        "level_index": index,
                        "side": side,
                        "price": to_mantissa(level["price"], price_exp),
                        "qty": to_mantissa(level["qty"], qty_exp),
                    }
                    rows.append(("book_events", row))
                    parsed[-1][1].append((side, row["price"], row["qty"]))
                    index += 1
        # Only now, with the whole message known to be well formed.
        for checksum, levels in parsed:
            self._verify(record.channel, book_kind, levels, checksum, depth)
        return data(f"book.{kind}", rows, empty=f"book.{kind}.empty")

    def _verify(
        self,
        connection: str,
        book_kind: BookEventKind,
        levels: list[tuple[Side, int, int]],
        checksum: int,
        depth: int | None,
    ) -> None:
        if book_kind == BookEventKind.DELTA:
            book = self._books.get(connection)
            if book is None or connection in self._suspect:
                return  # no trusted book to check against until the next snapshot
        else:
            book = {Side.BUY: {}, Side.SELL: {}}
            if book_kind == BookEventKind.SNAPSHOT:
                self._books[connection] = book
                self._suspect.discard(connection)
        _apply(book, levels, depth)
        computed = book_checksum(book)
        if computed != checksum:
            if book_kind != BookEventKind.CHECK_SNAPSHOT:
                self._suspect.add(connection)
            self.note(
                StreamEventKind.CHECKSUM_MISMATCH,
                connection=connection,
                expected=checksum,
                observed=computed,
            )

    def _trade(self, record: RawRecord, message: dict, kind: str) -> Handled:
        entries = as_list(message["data"], "data")
        self.require_session()
        rows: list[tuple[str, Row]] = []
        if kind == "snapshot":
            self.note(StreamEventKind.TRADE_BACKFILL, connection=record.channel)
        for trade in entries:
            self.require_symbol(as_object(trade, "trade")["symbol"])
            price_exp, qty_exp = self.require_exponents()
            row = {
                **venue_time(trade["timestamp"]),
                "recv_ts": record.recv_ts,
                "seq": record.seq,
                "trade_id": str(text_int(trade["trade_id"], "trade_id")),
                "price": to_mantissa(trade["price"], price_exp),
                "qty": to_mantissa(trade["qty"], qty_exp),
                "side": _side(trade["side"]),
            }
            rows.append(("trades", row))
        return data(f"trade.{kind}", rows, empty=f"trade.{kind}.empty")


def _side(value: Any) -> Side:
    if value not in _TAKER_SIDES:
        raise ValueError(f"unknown trade side {value!r}")
    return _TAKER_SIDES[value]
