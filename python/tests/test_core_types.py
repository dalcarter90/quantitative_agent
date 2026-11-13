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


def test_instrument_id_and_metadata():
    assert _core.InstrumentId(7).value == 7
    assert _core.InstrumentId(2**32 - 1).value == 2**32 - 1
    for bad in (-1, 2**32):
        with pytest.raises(OverflowError):
            _core.InstrumentId(bad)
    meta = _core.InstrumentMeta(_core.InstrumentId(7), 1, 8, _core.Price(1), _core.Qty(1))
    assert (meta.id.value, meta.price_exp, meta.qty_exp) == (7, 1, 8)
    assert (meta.tick_size.value, meta.lot_size.value) == (1, 1)
    with pytest.raises(TypeError):
        _core.InstrumentMeta(_core.InstrumentId(7), 1, 8, 1, 1)  # raw ints are not Price/Qty
    with pytest.raises(OverflowError):
        _core.InstrumentMeta(_core.InstrumentId(7), 200, 8, _core.Price(1), _core.Qty(1))


def test_enums_use_the_schemas_numbers():
    assert int(_core.Side.Buy) == SchemaSide.BUY == 1
    assert int(_core.Side.Sell) == SchemaSide.SELL == -1
    assert int(_core.Side.Unknown) == SchemaSide.UNKNOWN == 0
    assert int(_core.EventKind.BookSnapshot) == BookEventKind.SNAPSHOT
    assert int(_core.EventKind.BookDelta) == BookEventKind.DELTA
    assert int(_core.EventKind.BookCheckSnapshot) == BookEventKind.CHECK_SNAPSHOT
    assert int(_core.EventKind.Trade) == 3


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
    "kind": (78, 1), "side": (79, 1), "flags": (80, 1),
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


def test_a_market_event_round_trips_through_its_bytes():
    event = full_market_event()
    raw = event.to_bytes()
    assert isinstance(raw, bytes) and len(raw) == 88
    copy = _core.MarketEvent.from_bytes(raw)
    assert copy.to_bytes() == raw
    assert copy.price.value == INT64_MAX and copy.seq == UINT64_MAX
    # Little-endian fields at their documented offsets.
    assert int.from_bytes(raw[48:56], "little", signed=True) == INT64_MAX
    assert int.from_bytes(raw[24:32], "little") == UINT64_MAX
    assert raw[81:] == bytes(7)  # the reserved bytes stay zero
    with pytest.raises(ValueError):
        _core.MarketEvent.from_bytes(raw[:-1])


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
    raw = bytearray(_core.MarketEvent().to_bytes())
    raw[78] = 9  # kind
    with pytest.raises(ValueError, match="kind"):
        _core.MarketEvent.from_bytes(bytes(raw))
    raw = bytearray(_core.MarketEvent().to_bytes())
    raw[79] = 5  # side
    with pytest.raises(ValueError, match="side"):
        _core.MarketEvent.from_bytes(bytes(raw))
    raw = bytearray(_core.MarketEvent().to_bytes())
    raw[85] = 1  # a reserved byte
    with pytest.raises(ValueError, match="reserved"):
        _core.MarketEvent.from_bytes(bytes(raw))


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
