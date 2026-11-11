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
"""

from typing import Any

from quantagent.data.adapters.base import (
    PROBE_CHANNEL,
    STREAM_CHANNEL,
    Handled,
    Rejected,
    Row,
    SequenceAnomaly,
    VenueAdapter,
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
from quantagent.data.schema import BookEventKind, Side

PRODUCT_CHANNEL = "rest:product"
_BOOK_KINDS = {
    (STREAM_CHANNEL, "snapshot"): BookEventKind.SNAPSHOT,
    (STREAM_CHANNEL, "update"): BookEventKind.DELTA,
    (PROBE_CHANNEL, "snapshot"): BookEventKind.CHECK_SNAPSHOT,
}
_BOOK_SIDES = {"bid": Side.BUY, "offer": Side.SELL}
_AGGRESSOR_FOR_MAKER = {"BUY": Side.SELL, "SELL": Side.BUY}


class CoinbaseAdapter(VenueAdapter):
    venue = "coinbase"

    def __init__(self) -> None:
        super().__init__()
        self._last_sequence: dict[str, int] = {}  # highest sequence_num seen per connection

    def on_connect(self, connection: str) -> None:
        self._last_sequence.pop(connection, None)

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
        if channel in ("subscriptions", "heartbeats"):
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
            kind = "gap" if received > last else "repeat" if received == last else "out_of_order"
            self.anomalies.append(
                SequenceAnomaly(
                    kind, record.channel, last + 1, received, record.seq, record.recv_ts
                )
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
                index += 1
        if not kinds:
            return ignored(f"{record.channel}.l2_data.update" if events else "l2_data.empty")
        name = "snapshot" if "snapshot" in kinds else "update"
        return data(f"l2_data.{name}", rows, empty=f"l2_data.{name}.empty")

    def _trades(self, record: RawRecord, message: dict) -> Handled:
        events = as_list(message["events"], "events")
        self.require_session()
        rows: list[tuple[str, Row]] = []
        name = "update"
        for event in events:
            kind = as_object(event, "event").get("type")
            if kind not in ("snapshot", "update"):
                raise Rejected("unknown_message", f"market_trades event type {kind!r}")
            if kind == "snapshot":
                name = "snapshot"
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
