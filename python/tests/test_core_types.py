"""The C++ core types through the bindings (roadmap Phase 3): every value
survives Python -> C++ -> Python unchanged, and the types refuse what the
C++ side refuses."""

import time

import pytest

from quantagent import _core
from quantagent.data.schema import BookEventKind
from quantagent.data.schema import Side as SchemaSide

INT64_MIN, INT64_MAX = -(2**63), 2**63 - 1
UINT64_MAX = 2**64 - 1

STRONG = [_core.Price, _core.Qty, _core.Timestamp, _core.MonoTimestamp]


@pytest.mark.parametrize("kind", STRONG, ids=lambda k: k.__name__)
@pytest.mark.parametrize("value", [INT64_MIN, -1, 0, 734685, INT64_MAX])
def test_strong_types_round_trip_every_int64(kind, value):
    assert kind(value).value == value
    assert kind(value) == kind(value)
    assert hash(kind(value)) == hash(kind(value))
    assert repr(kind(value)) == f"{kind.__name__}({value})"


@pytest.mark.parametrize("kind", STRONG, ids=lambda k: k.__name__)
@pytest.mark.parametrize("bad", [1.0, 1.5, "1", None, True, b"1"])
def test_strong_types_accept_only_integers(kind, bad):
    with pytest.raises(TypeError):
        kind(bad)


@pytest.mark.parametrize("kind", STRONG, ids=lambda k: k.__name__)
@pytest.mark.parametrize("value", [INT64_MAX + 1, INT64_MIN - 1, 10**30])
def test_strong_types_refuse_values_outside_int64(kind, value):
    with pytest.raises(OverflowError):
        kind(value)


@pytest.mark.parametrize("kind", STRONG, ids=lambda k: k.__name__)
def test_strong_types_have_no_float_or_int_conversion(kind):
    value = kind(5)
    with pytest.raises(TypeError):
        float(value)
    with pytest.raises(TypeError):
        int(value)
    with pytest.raises(TypeError):
        range(value)  # no __index__ either


def test_strong_types_compare_within_a_type_only():
    assert _core.Price(1) < _core.Price(2) <= _core.Price(2)
    assert _core.Qty(3) > _core.Qty(2)
    assert _core.Price(5) != _core.Qty(5)  # different types are never equal
    with pytest.raises(TypeError):
        _core.Price(1) < _core.Qty(2)  # noqa: B015
    with pytest.raises(TypeError):
        _core.Timestamp(1) < _core.MonoTimestamp(2)  # noqa: B015


def meta(price_exp=8, qty_exp=8, price_precision=1, qty_precision=8, tick=10_000_000, lot=1):
    """Kraken BTC-USD's metadata by default (ADR 0019)."""
    return _core.InstrumentMeta(_core.InstrumentId(7), price_exp, qty_exp, price_precision,
                                qty_precision, _core.Price(tick), _core.Qty(lot))  # fmt: skip


def test_instrument_id_and_metadata():
    assert _core.InstrumentId(7).value == 7
    assert _core.InstrumentId(2**32 - 1).value == 2**32 - 1
    for bad in (-1, 2**32):
        with pytest.raises(OverflowError):
            _core.InstrumentId(bad)
    kraken = meta()
    assert (kraken.id.value, kraken.price_exp, kraken.qty_exp) == (7, 8, 8)
    assert (kraken.price_precision, kraken.qty_precision) == (1, 8)
    assert (kraken.tick_size.value, kraken.lot_size.value) == (10_000_000, 1)
    with pytest.raises(TypeError):  # raw ints are not Price/Qty
        _core.InstrumentMeta(_core.InstrumentId(7), 8, 8, 1, 8, 1, 1)
    with pytest.raises(TypeError):  # the precisions are required: no default guesses them
        _core.InstrumentMeta(_core.InstrumentId(7), 8, 8, _core.Price(1), _core.Qty(1))
    with pytest.raises(OverflowError):
        meta(price_exp=200)
    with pytest.raises(TypeError):
        meta(price_precision=1.0)


@pytest.mark.parametrize(
    "fields",
    # Exponent 18 is the largest there is (D7, below), so precision 19 is above both.
    [{"price_precision": 9}, {"qty_precision": 9}, {"price_precision": -1}, {"qty_precision": -1},
     {"price_exp": 18, "price_precision": 19}, {"qty_exp": 18, "qty_precision": 19}],
)  # fmt: skip
def test_instrument_metadata_refuses_a_precision_the_store_would_refuse(fields):
    """ADR 0019: a precision is 0 to 18 and never above its exponent, as in the
    schema; the C++ book divides by 10^(exp - precision) to get Kraken's digits."""
    with pytest.raises(ValueError, match="precision"):
        meta(**fields)
    meta(price_precision=8, qty_precision=0)  # the bounds themselves are fine


@pytest.mark.parametrize(
    "fields",
    [{"price_exp": 19}, {"qty_exp": 19}, {"price_exp": -1, "price_precision": 0},
     {"qty_exp": -1, "qty_precision": 0}, {"price_exp": 127}, {"qty_exp": -128}],
)  # fmt: skip
def test_instrument_metadata_refuses_an_exponent_the_store_would_refuse(fields):
    """Review D7 (ADR 0020): an exponent is 0 to 18, as in the schema; any other
    int8 used to be accepted."""
    with pytest.raises(ValueError, match="_exp"):
        meta(**fields)
    for exp in (0, 18):  # the bounds themselves are fine
        meta(price_exp=exp, qty_exp=exp, price_precision=0, qty_precision=0)


def test_enums_use_the_schemas_numbers():
    assert int(_core.Side.Buy) == SchemaSide.BUY == 1
    assert int(_core.Side.Sell) == SchemaSide.SELL == -1
    assert int(_core.Side.Unknown) == SchemaSide.UNKNOWN == 0
    assert int(_core.EventKind.BookSnapshot) == BookEventKind.SNAPSHOT
    assert int(_core.EventKind.BookDelta) == BookEventKind.DELTA
    assert int(_core.EventKind.BookCheckSnapshot) == BookEventKind.CHECK_SNAPSHOT
    assert int(_core.EventKind.Trade) == 3
    # Stream control and snapshot framing (ADR 0020).
    assert int(_core.EventKind.StreamStale) == 4
    assert int(_core.EventKind.StreamReset) == 5
    assert int(_core.EventKind.SnapshotBegin) == 6
    assert int(_core.EventKind.SnapshotEnd) == 7
    assert {name: int(value) for name, value in _core.StreamReason.__members__.items()} == {
        "NoReason": 0, "SequenceGap": 1, "SequenceRepeat": 2, "SequenceOutOfOrder": 3,
        "ChecksumMismatch": 4, "MessageRejected": 5, "SessionStart": 6, "Connect": 7,
        "Disconnect": 8,
    }  # fmt: skip


def _snake(name):
    return "".join(f"_{c.lower()}" if c.isupper() else c for c in name).lstrip("_")


def test_the_reference_books_reasons_are_the_cpp_stream_reasons():
    """ADR 0020: every stored stream event the reference book treats as a stale
    or reset point has a StreamReason of the same name, so the Phase 4
    converter maps one to the other by name and the golden files' reasons are
    the C++ book's."""
    from quantagent.data import book

    members = _core.StreamReason.__members__
    stale = {_snake(name) for name, value in members.items() if 1 <= int(value) <= 5}
    reset = {_snake(name) for name, value in members.items() if int(value) >= 6}
    assert set(book.STALE_REASONS.values()) == stale
    assert set(book.RESET_REASONS.values()) == reset
    assert len(book.STALE_REASONS) == len(stale) and len(book.RESET_REASONS) == len(reset)


@pytest.mark.parametrize(
    ("price", "qty"),
    [(734685, 20416909), (INT64_MAX, INT64_MAX), (INT64_MIN, INT64_MIN), (INT64_MIN, INT64_MAX),
     (-1, 1), (0, INT64_MIN), (3037000500, 3037000500)],
)  # fmt: skip
def test_notional_is_exact_beyond_64_bits(price, qty):
    assert _core.notional(_core.Price(price), _core.Qty(qty)) == price * qty


def test_notional_takes_only_a_price_and_a_quantity():
    with pytest.raises(TypeError):
        _core.notional(_core.Qty(1), _core.Price(2))
    with pytest.raises(TypeError):
        _core.notional(1, 2)


# --- messages ----------------------------------------------------------------------------

MARKET_EVENT_LAYOUT = {
    "recv_ts": (0, 8), "recv_mono": (8, 8), "event_ts": (16, 8), "seq": (24, 8),
    "venue_seq": (32, 8), "trade_id": (40, 8), "price": (48, 8), "qty": (56, 8),
    "instrument": (64, 4), "checksum": (68, 4), "level_index": (72, 4), "depth": (76, 2),
    "kind": (78, 1), "side": (79, 1), "flags": (80, 1), "reason": (81, 1), "source": (84, 4),
}  # fmt: skip
ORDER_REQUEST_LAYOUT = {
    "client_order_id": (0, 8), "target_order_id": (8, 8), "price": (16, 8), "qty": (24, 8),
    "created_ts": (32, 8), "created_mono": (40, 8), "instrument": (48, 4), "kind": (52, 1),
    "side": (53, 1), "type": (54, 1), "time_in_force": (55, 1),
}  # fmt: skip
EXECUTION_REPORT_LAYOUT = {
    "client_order_id": (0, 8), "exec_id": (8, 8), "last_price": (16, 8), "last_qty": (24, 8),
    "leaves_qty": (32, 8), "event_ts": (40, 8), "recv_ts": (48, 8), "recv_mono": (56, 8),
    "instrument": (64, 4), "reject_code": (68, 2), "kind": (70, 1), "side": (71, 1),
}  # fmt: skip


@pytest.mark.parametrize(
    ("kind", "size", "layout"),
    [(_core.MarketEvent, 88, MARKET_EVENT_LAYOUT), (_core.OrderRequest, 64, ORDER_REQUEST_LAYOUT),
     (_core.ExecutionReport, 80, EXECUTION_REPORT_LAYOUT)],
    ids=["MarketEvent", "OrderRequest", "ExecutionReport"],
)  # fmt: skip
def test_message_layouts_match_the_documentation(kind, size, layout):
    assert kind.size() == size
    assert kind.layout() == layout


def full_market_event():
    event = _core.MarketEvent()
    event.recv_ts = _core.Timestamp(1_794_366_407_006_069_000)
    event.recv_mono = _core.MonoTimestamp(142_629_801_982_300)
    event.event_ts = _core.Timestamp(1_794_366_405_550_000_000)
    event.seq = UINT64_MAX
    event.venue_seq = INT64_MIN
    event.trade_id = UINT64_MAX - 1
    event.price = _core.Price(INT64_MAX)
    event.qty = _core.Qty(INT64_MIN)
    event.instrument = _core.InstrumentId(2**32 - 1)
    event.checksum = 2**32 - 1
    event.level_index = 38_531
    event.depth = 1000
    event.kind = _core.EventKind.BookDelta
    event.side = _core.Side.Sell
    event.flags = _core.MarketEvent.LAST_IN_MESSAGE | _core.MarketEvent.HAS_CHECKSUM
    event.source = 2**32 - 1
    return event


def test_a_market_event_round_trips_every_field_at_its_extremes():
    event = full_market_event()
    assert event.recv_ts.value == 1_794_366_407_006_069_000
    assert event.recv_mono.value == 142_629_801_982_300
    assert event.event_ts.value == 1_794_366_405_550_000_000
    assert (event.seq, event.venue_seq, event.trade_id) == (UINT64_MAX, INT64_MIN, UINT64_MAX - 1)
    assert (event.price.value, event.qty.value) == (INT64_MAX, INT64_MIN)
    assert (event.instrument.value, event.checksum) == (2**32 - 1, 2**32 - 1)
    assert (event.level_index, event.depth) == (38_531, 1000)
    assert (event.kind, event.side) == (_core.EventKind.BookDelta, _core.Side.Sell)
    assert event.has(_core.MarketEvent.HAS_CHECKSUM)
    assert not event.has(_core.MarketEvent.HAS_VENUE_SEQ)
    assert (event.reason, event.source) == (_core.StreamReason.NoReason, 2**32 - 1)


def test_a_market_event_round_trips_through_its_bytes():
    event = full_market_event()
    raw = event.to_bytes()
    assert isinstance(raw, bytes) and len(raw) == 88
    copy = _core.MarketEvent.from_bytes(raw)
    assert copy.to_bytes() == raw
    assert copy.price.value == INT64_MAX and copy.seq == UINT64_MAX
    assert copy.source == 2**32 - 1
    # Little-endian fields at their documented offsets.
    assert int.from_bytes(raw[48:56], "little", signed=True) == INT64_MAX
    assert int.from_bytes(raw[24:32], "little") == UINT64_MAX
    assert raw[81] == 0  # no reason: this is a book level
    assert raw[82:84] == bytes(2)  # the reserved bytes stay zero
    assert int.from_bytes(raw[84:88], "little") == 2**32 - 1
    with pytest.raises(ValueError):
        _core.MarketEvent.from_bytes(raw[:-1])


def control(kind, reason=_core.StreamReason.NoReason, **fields):
    """A stream control or framing event (ADR 0020)."""
    event = _core.MarketEvent()
    event.recv_ts = _core.Timestamp(1_794_366_407_006_069_000)
    event.event_ts = event.recv_ts
    event.seq = 1_790
    event.source = 3
    event.kind = kind
    event.reason = reason
    event.flags = _core.MarketEvent.EVENT_TS_FROM_RECEIPT
    for name, value in fields.items():
        setattr(event, name, value)
    return event


@pytest.mark.parametrize(
    ("kind", "reason", "fields"),
    [
        (_core.EventKind.StreamStale, _core.StreamReason.SequenceGap, {}),
        (_core.EventKind.StreamStale, _core.StreamReason.SequenceRepeat, {}),
        (_core.EventKind.StreamStale, _core.StreamReason.SequenceOutOfOrder, {}),
        (_core.EventKind.StreamStale, _core.StreamReason.ChecksumMismatch, {}),
        (_core.EventKind.StreamStale, _core.StreamReason.MessageRejected, {"level_index": 1}),
        (_core.EventKind.StreamReset, _core.StreamReason.SessionStart, {}),
        (_core.EventKind.StreamReset, _core.StreamReason.Connect, {}),
        (_core.EventKind.StreamReset, _core.StreamReason.Disconnect, {}),
        (_core.EventKind.SnapshotBegin, _core.StreamReason.NoReason, {"level_index": 38_526}),
        (_core.EventKind.SnapshotEnd, _core.StreamReason.NoReason, {"level_index": 38_526}),
        # A gap event may say which venue number arrived (ADR 0020).
        (_core.EventKind.StreamStale, _core.StreamReason.SequenceGap,
         {"venue_seq": 1_791, "flags": _core.MarketEvent.HAS_VENUE_SEQ}),
    ],
)  # fmt: skip
def test_control_events_round_trip_through_their_bytes(kind, reason, fields):
    event = control(kind, reason, **fields)
    raw = event.to_bytes()
    copy = _core.MarketEvent.from_bytes(raw)
    assert copy.to_bytes() == raw
    assert (copy.kind, copy.reason, copy.source, copy.seq) == (kind, reason, 3, 1_790)
    assert raw[81] == int(reason)


@pytest.mark.parametrize(
    ("kind", "reason"),
    [
        (_core.EventKind.StreamStale, _core.StreamReason.NoReason),  # a control event says why
        (_core.EventKind.StreamReset, _core.StreamReason.NoReason),
        (_core.EventKind.StreamStale, _core.StreamReason.Connect),  # a reset's reason
        (_core.EventKind.StreamReset, _core.StreamReason.SequenceGap),  # a stale reason
        (_core.EventKind.BookDelta, _core.StreamReason.SequenceGap),  # data has no reason
        (_core.EventKind.Trade, _core.StreamReason.Disconnect),
        (_core.EventKind.SnapshotBegin, _core.StreamReason.SessionStart),
    ],
)
def test_from_bytes_refuses_a_reason_that_does_not_fit_the_kind(kind, reason):
    raw = control(kind, reason).to_bytes()
    with pytest.raises(ValueError, match="reason"):
        _core.MarketEvent.from_bytes(raw)


@pytest.mark.parametrize(
    "fields",
    [{"price": _core.Price(1)}, {"qty": _core.Qty(1)}, {"side": _core.Side.Buy},
     {"trade_id": 1}, {"checksum": 1}, {"depth": 25}],
)  # fmt: skip
@pytest.mark.parametrize(
    ("kind", "reason"),
    [(_core.EventKind.StreamStale, _core.StreamReason.ChecksumMismatch),
     (_core.EventKind.StreamReset, _core.StreamReason.Connect),
     (_core.EventKind.SnapshotBegin, _core.StreamReason.NoReason),
     (_core.EventKind.SnapshotEnd, _core.StreamReason.NoReason)],
)  # fmt: skip
def test_from_bytes_refuses_a_control_event_carrying_market_values(kind, reason, fields):
    """A control event moves no level and is no trade: a price, quantity, side,
    trade id, checksum or depth on one is a converter bug (ADR 0020)."""
    with pytest.raises(ValueError, match="control event"):
        _core.MarketEvent.from_bytes(control(kind, reason, **fields).to_bytes())


def test_a_default_market_event_is_all_zero():
    assert _core.MarketEvent().to_bytes() == bytes(88)
    assert _core.OrderRequest().to_bytes() == bytes(64)
    assert _core.ExecutionReport().to_bytes() == bytes(80)


@pytest.mark.parametrize(
    ("field", "bad", "error"),
    [
        ("seq", -1, OverflowError),
        ("seq", 2**64, OverflowError),
        ("seq", 1.0, TypeError),
        ("venue_seq", 2**63, OverflowError),
        ("checksum", 2**32, OverflowError),
        ("level_index", -1, OverflowError),
        ("depth", 2**16, OverflowError),
        ("flags", 256, OverflowError),
        ("source", -1, OverflowError),
        ("source", 2**32, OverflowError),
        ("source", 1.0, TypeError),
        ("reason", 1, TypeError),  # a StreamReason, not an int
        ("kind", 4, TypeError),
        ("price", 5, TypeError),  # a Price, not an int
        ("recv_ts", _core.MonoTimestamp(5), TypeError),  # the other clock
        ("seq", True, TypeError),
    ],
)
def test_market_event_fields_refuse_out_of_range_or_wrong_types(field, bad, error):
    event = _core.MarketEvent()
    with pytest.raises(error):
        setattr(event, field, bad)
    assert event.to_bytes() == bytes(88)  # nothing was half-written


def test_order_request_and_execution_report_round_trip():
    request = _core.OrderRequest()
    request.client_order_id = UINT64_MAX
    request.target_order_id = 42
    request.price = _core.Price(734685)
    request.qty = _core.Qty(100)
    request.created_ts = _core.Timestamp(INT64_MAX)
    request.created_mono = _core.MonoTimestamp(1)
    request.instrument = _core.InstrumentId(3)
    request.kind = _core.RequestKind.Cancel
    request.side = _core.Side.Buy
    request.type = _core.OrderType.Limit
    request.time_in_force = _core.TimeInForce.PostOnly
    copy = _core.OrderRequest.from_bytes(request.to_bytes())
    assert (copy.client_order_id, copy.target_order_id) == (UINT64_MAX, 42)
    assert (copy.price.value, copy.qty.value, copy.created_ts.value) == (734685, 100, INT64_MAX)
    assert (copy.kind, copy.side, copy.type, copy.time_in_force) == (
        _core.RequestKind.Cancel,
        _core.Side.Buy,
        _core.OrderType.Limit,
        _core.TimeInForce.PostOnly,
    )

    report = _core.ExecutionReport()
    report.client_order_id = 7
    report.exec_id = UINT64_MAX
    report.last_price = _core.Price(-5)
    report.last_qty = _core.Qty(3)
    report.leaves_qty = _core.Qty(0)
    report.event_ts = _core.Timestamp(1)
    report.recv_ts = _core.Timestamp(2)
    report.recv_mono = _core.MonoTimestamp(3)
    report.instrument = _core.InstrumentId(4)
    report.reject_code = 2**16 - 1
    report.kind = _core.ReportKind.PartialFill
    report.side = _core.Side.Sell
    copy = _core.ExecutionReport.from_bytes(report.to_bytes())
    assert (copy.exec_id, copy.last_price.value, copy.reject_code) == (UINT64_MAX, -5, 2**16 - 1)
    assert (copy.kind, copy.side) == (_core.ReportKind.PartialFill, _core.Side.Sell)


def test_from_bytes_refuses_unknown_enum_values():
    for kind in (9, 10, 255):  # InstrumentUpdate (8) is the last kind (ADR 0022)
        raw = bytearray(_core.MarketEvent().to_bytes())
        raw[78] = kind
        with pytest.raises(ValueError, match="kind"):
            _core.MarketEvent.from_bytes(bytes(raw))
    raw = bytearray(_core.MarketEvent().to_bytes())
    raw[79] = 5  # side
    with pytest.raises(ValueError, match="side"):
        _core.MarketEvent.from_bytes(bytes(raw))
    for reason in (9, 255):  # Disconnect (8) is the last reason
        raw = bytearray(control(_core.EventKind.StreamStale,
                                _core.StreamReason.SequenceGap).to_bytes())  # fmt: skip
        raw[81] = reason
        with pytest.raises(ValueError, match="reason"):
            _core.MarketEvent.from_bytes(bytes(raw))
    for offset in (82, 83):  # the reserved bytes left after ADR 0020
        raw = bytearray(_core.MarketEvent().to_bytes())
        raw[offset] = 1
        with pytest.raises(ValueError, match="reserved"):
            _core.MarketEvent.from_bytes(bytes(raw))
    raw = bytearray(_core.MarketEvent().to_bytes())
    raw[85] = 1  # was reserved; now the source
    assert _core.MarketEvent.from_bytes(bytes(raw)).source == 256


def test_from_bytes_refuses_the_unassigned_flag_bit():
    """Review D7 (ADR 0020): seven flags are defined; the eighth bit was accepted."""
    known = (_core.MarketEvent.LAST_IN_MESSAGE | _core.MarketEvent.HAS_MONO
             | _core.MarketEvent.HAS_VENUE_SEQ | _core.MarketEvent.HAS_CHECKSUM
             | _core.MarketEvent.HAS_DEPTH | _core.MarketEvent.HAS_TRADE_ID
             | _core.MarketEvent.EVENT_TS_FROM_RECEIPT)  # fmt: skip
    assert known == _core.MarketEvent.KNOWN_FLAGS == 0x7F
    event = _core.MarketEvent()
    event.flags = known
    assert _core.MarketEvent.from_bytes(event.to_bytes()).flags == known
    raw = bytearray(_core.MarketEvent().to_bytes())
    raw[80] = 0x80
    with pytest.raises(ValueError, match="flag"):
        _core.MarketEvent.from_bytes(bytes(raw))
    raw[80] = 0xFF
    with pytest.raises(ValueError, match="flag"):
        _core.MarketEvent.from_bytes(bytes(raw))


# --- instrument updates (ADR 0022) ---------------------------------------------------------


def instrument_update(**fields):
    """An InstrumentUpdate for Kraken BTC-USD, stamped like any event of a record."""
    event = _core.MarketEvent.instrument_update(meta())
    event.recv_ts = _core.Timestamp(1_794_366_407_006_069_000)
    event.event_ts = event.recv_ts
    event.seq = 7
    event.source = 3
    event.flags = _core.MarketEvent.EVENT_TS_FROM_RECEIPT
    for name, value in fields.items():
        setattr(event, name, value)
    return event


def test_an_instrument_update_is_the_ninth_kind_and_carries_a_whole_instrument_meta():
    assert int(_core.EventKind.InstrumentUpdate) == 8
    event = instrument_update()
    assert event.kind == _core.EventKind.InstrumentUpdate
    assert event.instrument.value == 7
    # The tick size is a price and the lot size a quantity, at the instrument's exponents.
    assert (event.price.value, event.qty.value) == (10_000_000, 1)
    # The four scales in level_index, a byte each from the lowest, in the order
    # of InstrumentMeta's bytes 4 to 7: price_exp, qty_exp, price_precision, qty_precision.
    assert event.level_index == 8 | 8 << 8 | 1 << 16 | 8 << 24
    raw = event.to_bytes()
    assert raw[72:76] == bytes([8, 8, 1, 8])
    assert raw[78] == 8 and raw[81] == 0
    assert raw[82:84] == bytes(2)  # no reserved byte is used
    assert (event.side, event.reason) == (_core.Side.Unknown, _core.StreamReason.NoReason)
    assert (event.venue_seq, event.trade_id, event.checksum, event.depth) == (0, 0, 0, 0)


@pytest.mark.parametrize(
    "fields",
    [{}, {"price_exp": 0, "qty_exp": 0, "price_precision": 0, "qty_precision": 0},
     {"price_exp": 18, "qty_exp": 18, "price_precision": 18, "qty_precision": 0},
     {"price_exp": 2, "qty_exp": 5, "price_precision": 1, "qty_precision": 3,
      "tick": 2**63 - 1, "lot": 1}],
)  # fmt: skip
def test_an_instrument_update_round_trips_its_metadata_through_its_bytes(fields):
    sent = meta(**fields)
    event = instrument_update()
    made = _core.MarketEvent.instrument_update(sent)
    event.price, event.qty, event.level_index = made.price, made.qty, made.level_index
    copy = _core.MarketEvent.from_bytes(event.to_bytes())
    assert copy.to_bytes() == event.to_bytes()
    assert (copy.seq, copy.source) == (7, 3)
    got = copy.instrument_meta()
    assert (got.id.value, got.price_exp, got.qty_exp, got.price_precision, got.qty_precision,
            got.tick_size.value, got.lot_size.value) == (
        sent.id.value, sent.price_exp, sent.qty_exp, sent.price_precision, sent.qty_precision,
        sent.tick_size.value, sent.lot_size.value)  # fmt: skip


def test_only_an_instrument_update_has_instrument_metadata():
    for kind in (_core.EventKind.BookDelta, _core.EventKind.SnapshotBegin, _core.EventKind.Trade):
        event = full_market_event()
        event.kind = kind
        with pytest.raises(ValueError, match="InstrumentUpdate"):
            event.instrument_meta()
    with pytest.raises(TypeError):
        _core.MarketEvent.instrument_update(7)


def _scales(price_exp, qty_exp, price_precision, qty_precision):
    return bytes(v & 0xFF for v in (price_exp, qty_exp, price_precision, qty_precision))


@pytest.mark.parametrize(
    "scales",
    [_scales(19, 8, 1, 8), _scales(8, 19, 1, 8), _scales(8, 8, 9, 8), _scales(8, 8, 1, 9),
     _scales(-1, 8, 0, 8), _scales(8, 8, -1, 8), _scales(8, 8, 1, -1), _scales(127, 8, 1, 8)],
)  # fmt: skip
def test_from_bytes_refuses_an_instrument_update_whose_scales_do_not_fit(scales):
    """As the InstrumentMeta binding and the store's schema do: an exponent is 0
    to 18 and a precision 0 to its exponent."""
    raw = bytearray(instrument_update().to_bytes())
    raw[72:76] = scales
    with pytest.raises(ValueError, match="exponent|precision"):
        _core.MarketEvent.from_bytes(bytes(raw))
    with pytest.raises(ValueError, match="exponent|precision"):
        event = instrument_update()
        event.level_index = int.from_bytes(scales, "little")
        event.instrument_meta()


@pytest.mark.parametrize(("tick", "lot"), [(0, 1), (-1, 1), (1, 0), (1, -5), (-(2**63), 1)])
def test_from_bytes_refuses_an_instrument_update_whose_tick_or_lot_is_below_one(tick, lot):
    """As the store's schema does (ADR 0023): a tick of 0 would divide by zero
    when a price is rounded to it, and a negative one would invert a collar."""
    event = instrument_update(price=_core.Price(tick), qty=_core.Qty(lot))
    with pytest.raises(ValueError, match="tick size .* lot size .* at least 1"):
        _core.MarketEvent.from_bytes(event.to_bytes())
    ones = instrument_update(price=_core.Price(1), qty=_core.Qty(1))
    assert _core.MarketEvent.from_bytes(ones.to_bytes()).instrument_meta().tick_size.value == 1


@pytest.mark.parametrize(
    "fields",
    [{"side": _core.Side.Buy}, {"venue_seq": 1}, {"trade_id": 1}, {"checksum": 1}, {"depth": 25},
     {"flags": _core.MarketEvent.LAST_IN_MESSAGE}, {"flags": _core.MarketEvent.HAS_VENUE_SEQ},
     {"flags": _core.MarketEvent.HAS_CHECKSUM}, {"flags": _core.MarketEvent.HAS_DEPTH},
     {"flags": _core.MarketEvent.HAS_TRADE_ID}],
)  # fmt: skip
def test_from_bytes_refuses_an_instrument_update_carrying_anything_else(fields):
    with pytest.raises(ValueError, match="instrument update"):
        _core.MarketEvent.from_bytes(instrument_update(**fields).to_bytes())


def test_from_bytes_accepts_an_instrument_updates_own_flags_and_refuses_a_reason():
    flags = _core.MarketEvent.EVENT_TS_FROM_RECEIPT | _core.MarketEvent.HAS_MONO
    for value in (0, _core.MarketEvent.HAS_MONO, flags):
        event = instrument_update(flags=value, recv_mono=_core.MonoTimestamp(5))
        assert _core.MarketEvent.from_bytes(event.to_bytes()).flags == value
    with pytest.raises(ValueError, match="reason"):
        _core.MarketEvent.from_bytes(
            instrument_update(reason=_core.StreamReason.Connect).to_bytes())  # fmt: skip
    # It carries values, so it is not one of the control events that may carry none.
    assert instrument_update().price.value != 0


# --- clocks ----------------------------------------------------------------------------------


def test_manual_clock():
    clock = _core.ManualClock(_core.Timestamp(1_000), _core.MonoTimestamp(50))
    assert (clock.wall().value, clock.mono().value) == (1_000, 50)
    clock.advance(25)
    assert (clock.wall().value, clock.mono().value) == (1_025, 75)
    clock.step_wall(-2_000)
    assert (clock.wall().value, clock.mono().value) == (-975, 75)
    clock.set(_core.Timestamp(7), _core.MonoTimestamp(9))
    assert (clock.wall().value, clock.mono().value) == (7, 9)


def test_the_system_clocks_counter_is_the_one_the_recorder_stamps_with():
    """ADR 0012: C++ and Python must read the same monotonic counter, or
    their stamps could not be compared."""
    clock = _core.SystemClock()
    before_wall, before_mono = time.time_ns(), time.perf_counter_ns()
    wall, mono = clock.wall().value, clock.mono().value
    after_wall, after_mono = time.time_ns(), time.perf_counter_ns()
    # The wall clock agrees to within its tick; the counter to within a millisecond.
    assert before_wall - 20_000_000 <= wall <= after_wall + 20_000_000
    assert before_mono - 1_000_000 <= mono <= after_mono + 1_000_000
