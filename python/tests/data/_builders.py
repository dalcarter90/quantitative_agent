"""Shared builders for the data core tests."""

from quantagent.data.schema import (
    BOOK_EVENTS,
    INSTRUMENTS,
    TRADES,
    BookEventKind,
    Side,
    table_from_rows,
)

NS_PER_SECOND = 1_000_000_000
NS_PER_DAY = 86_400 * NS_PER_SECOND
T0 = 19675 * NS_PER_DAY + 12 * 3600 * NS_PER_SECOND  # 2023-11-14T12:00:00Z
INT64_MAX = 2**63 - 1

# Effective times of the two instrument metadata versions in conftest.history.
V1_EFFECTIVE = T0 - NS_PER_DAY
V2_EFFECTIVE = T0 + 30 * NS_PER_DAY


class FakeClock:
    """Returns the given values in order, then keeps returning the last one."""

    def __init__(self, *values):
        self._values = list(values)

    def __call__(self):
        if len(self._values) > 1:
            return self._values.pop(0)
        return self._values[0]


def trade_table(*specs):
    """Each spec is (trade_id, event_ts, price)."""
    rows = [
        {
            "event_ts": event_ts,
            "recv_ts": event_ts + 5_000_000,
            "seq": i,
            "trade_id": trade_id,
            "price": price,
            "qty": 100,
            "side": Side.BUY,
        }
        for i, (trade_id, event_ts, price) in enumerate(specs)
    ]
    return table_from_rows(TRADES, rows)


def book_table(*specs):
    """Each spec is (venue_seq_last, event_ts, side, price, qty); all are deltas."""
    rows = [
        {
            "event_ts": event_ts,
            "recv_ts": event_ts + 5_000_000,
            "seq": i,
            "kind": BookEventKind.DELTA,
            "venue_seq_first": None,
            "venue_seq_last": venue_seq_last,
            "side": side,
            "price": price,
            "qty": qty,
        }
        for i, (venue_seq_last, event_ts, side, price, qty) in enumerate(specs)
    ]
    return table_from_rows(BOOK_EVENTS, rows)


def instrument_table(event_ts, *, price_exp=2, qty_exp=5, tick_size=1, lot_size=1):
    row = {
        "event_ts": event_ts,
        "recv_ts": event_ts + 5_000_000,
        "seq": 0,
        "price_exp": price_exp,
        "qty_exp": qty_exp,
        "tick_size": tick_size,
        "lot_size": lot_size,
    }
    return table_from_rows(INSTRUMENTS, [row])
