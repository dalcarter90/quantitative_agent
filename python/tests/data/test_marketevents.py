"""The converter from stored book events and stream events to the C++ core's
MarketEvents (roadmap Phase 4a; ADR 0020's rules, ADR 0021).

Hand-built tables first, one rule at a time; then the recorded fixtures and the
eight integrity cases, where the converter's steps must be the reference book's
steps, one for one, and every event must be one ``MarketEvent.from_bytes``
accepts.
"""

import functools

import pyarrow as pa
import pytest
from _builders import LOG
from integrity import CASES, damaged
from test_fixtures import INSTRUMENT, fixture_path

from quantagent import _core
from quantagent.data.adapters import normalize
from quantagent.data.book import RESET_REASONS, STALE_REASONS, messages_from_table, trace
from quantagent.data.rawlog import read_raw_log
from quantagent.data.schema import (
    BOOK_EVENTS,
    INSTRUMENTS,
    BookEventKind,
    Side,
    StreamEventKind,
    table_from_rows,
)
from quantagent.marketevents import MAX_DEPTH, STEPS, EventGroup, SourceMap, convert

K = StreamEventKind
SNAP, DELTA, CHECK = BookEventKind.SNAPSHOT, BookEventKind.DELTA, BookEventKind.CHECK_SNAPSHOT
BUY, SELL = Side.BUY, Side.SELL
EV = _core.MarketEvent
KIND = _core.EventKind
REASON = _core.StreamReason
OTHER_LOG = "20231114T130000.000000000Z-000000000001"
INSTRUMENT_ID = 7


def level(seq, index, side, price, qty, *, kind=DELTA, log_id=LOG, event_ts=None,
          event_ts_source="venue", venue_seq=None, checksum=None, depth=None):  # fmt: skip
    """One stored book_events row: a level of the message at record ``seq``."""
    return {
        "event_ts": event_ts if event_ts is not None else 500 + seq * 10 + index,
        "event_ts_source": event_ts_source,
        "recv_ts": 1_000 + seq,
        "log_id": log_id,
        "seq": seq,
        "kind": kind,
        "venue_seq_first": None,
        "venue_seq_last": venue_seq,
        "checksum": checksum,
        "depth": depth,
        "level_index": index,
        "side": side,
        "price": price,
        "qty": qty,
    }


def instruments(*logs, price_exp=8, qty_exp=8, price_precision=1, qty_precision=8):
    """One metadata version at record 0 of each log."""
    rows = [{"event_ts": 900, "event_ts_source": "receipt", "recv_ts": 900, "log_id": log_id,
             "seq": 0, "price_exp": price_exp, "qty_exp": qty_exp,
             "price_precision": price_precision, "qty_precision": qty_precision,
             "tick_size": 1, "lot_size": 1} for log_id in (logs or (LOG,))]  # fmt: skip
    return table_from_rows(INSTRUMENTS, rows)


def stored(seq, kind, *, connection="ws", event_index=0, log_id=LOG, expected=None,
           observed=None, detail=None):  # fmt: skip
    """One stored stream_events row, as PITSession.stream_events returns it."""
    return {"event_ts": 1_000 + seq, "event_ts_source": "receipt", "recv_ts": 1_000 + seq,
            "log_id": log_id, "seq": seq, "event_index": event_index, "kind": int(kind),
            "connection": connection, "expected": expected, "observed": observed,
            "detail": detail}  # fmt: skip


def run(rows, events=(), *, metadata=None, sources=None, with_instruments=False):
    """The converter's groups for hand-built rows. Every metadata version is a
    group of its own (ADR 0022); they have their own tests below and are left
    out here unless asked for, so a test of messages sees its messages."""
    table = table_from_rows(BOOK_EVENTS, rows) if rows else BOOK_EVENTS.schema.empty_table()
    meta = metadata if metadata is not None else instruments(*sorted({r["log_id"] for r in rows}))
    groups = list(convert(table, events, meta, instrument=INSTRUMENT_ID, sources=sources))
    return [g for g in groups if with_instruments or g.step != "instrument"]


def flags(*names):
    value = 0
    for name in names:
        value |= getattr(EV, name)
    return value


# --- book messages -----------------------------------------------------------------------


def test_a_snapshot_is_framed_by_its_level_count():
    rows = [level(5, 0, BUY, 100, 3, kind=SNAP, checksum=77, depth=25),
            level(5, 1, SELL, 101, 4, kind=SNAP, checksum=77, depth=25),
            level(5, 2, BUY, 99, 1, kind=SNAP, checksum=77, depth=25)]  # fmt: skip
    (group,) = run(rows)
    assert (group.log_id, group.seq, group.step) == (LOG, 5, "message")
    kinds = [e.kind for e in group.events]
    assert kinds == [KIND.SnapshotBegin, *[KIND.BookSnapshot] * 3, KIND.SnapshotEnd]
    begin, *levels, end = group.events
    assert begin.level_index == end.level_index == 3
    assert [e.level_index for e in levels] == [0, 1, 2]
    assert [e.has(EV.LAST_IN_MESSAGE) for e in levels] == [False, False, True]
    assert {e.seq for e in group.events} == {5}
    assert len({e.source for e in group.events}) == 1


def test_an_update_is_its_levels_with_the_last_one_flagged():
    rows = [level(6, 0, BUY, 100, 0, venue_seq=41), level(6, 1, SELL, 102, 9, venue_seq=41)]
    (group,) = run(rows)
    assert [e.kind for e in group.events] == [KIND.BookDelta, KIND.BookDelta]
    assert [e.has(EV.LAST_IN_MESSAGE) for e in group.events] == [False, True]


def test_a_level_carries_its_row_and_its_messages_header():
    rows = [level(6, 0, BUY, 7_365_600_000_000, 20_333_696, event_ts=111, checksum=4_294_967_295,
                  depth=25),
            level(6, 1, SELL, 7_365_610_000_000, 0, event_ts=222, checksum=4_294_967_295,
                  depth=25)]  # fmt: skip
    (group,) = run(rows)
    first, second = group.events
    assert (first.price.value, first.qty.value, first.side) == (7_365_600_000_000, 20_333_696,
                                                                _core.Side.Buy)  # fmt: skip
    assert (second.price.value, second.qty.value, second.side) == (7_365_610_000_000, 0,
                                                                   _core.Side.Sell)  # fmt: skip
    # Each level keeps its own venue time; the message's record gives the rest.
    assert [e.event_ts.value for e in group.events] == [111, 222]
    assert {e.recv_ts.value for e in group.events} == {1_006}
    assert {e.instrument.value for e in group.events} == {INSTRUMENT_ID}
    assert {e.checksum for e in group.events} == {4_294_967_295}
    assert {e.depth for e in group.events} == {25}
    assert first.flags == flags("HAS_CHECKSUM", "HAS_DEPTH")
    assert second.flags == flags("HAS_CHECKSUM", "HAS_DEPTH", "LAST_IN_MESSAGE")
    assert {(e.venue_seq, e.trade_id, e.reason) for e in group.events} == {(0, 0, REASON.NoReason)}
    assert {e.recv_mono.value for e in group.events} == {0}  # the store keeps no monotonic stamp


def test_a_coinbase_level_carries_its_message_number():
    (group,) = run([level(6, 0, BUY, 100, 1, venue_seq=2**62)])
    (event,) = group.events
    assert event.venue_seq == 2**62
    assert event.flags == flags("HAS_VENUE_SEQ", "LAST_IN_MESSAGE")


def test_a_receipt_stamped_level_says_so():
    (group,) = run([level(6, 0, BUY, 100, 1, event_ts=1_006, event_ts_source="receipt")])
    assert group.events[0].has(EV.EVENT_TS_FROM_RECEIPT)


def test_a_check_snapshot_is_framed_on_the_probe_source():
    rows = [level(2, 0, BUY, 100, 3, kind=SNAP), level(9, 0, BUY, 100, 3, kind=CHECK),
            level(9, 1, SELL, 101, 1, kind=CHECK)]  # fmt: skip
    sources = SourceMap()
    stream, check = run(rows, sources=sources)
    assert check.step == "check"
    assert [e.kind for e in check.events] == [KIND.SnapshotBegin, KIND.BookCheckSnapshot,
                                              KIND.BookCheckSnapshot, KIND.SnapshotEnd]  # fmt: skip
    assert {e.source for e in check.events} == {sources.source(LOG, "probe")}
    assert {e.source for e in stream.events} == {sources.source(LOG, "ws")}
    assert sources.source(LOG, "ws") != sources.source(LOG, "probe")


def test_frame_events_are_receipt_stamped_and_carry_no_market_values():
    rows = [level(5, 0, BUY, 100, 3, kind=SNAP, checksum=77, depth=25)]
    (group,) = run(rows)
    for frame in (group.events[0], group.events[-1]):
        assert frame.flags == EV.EVENT_TS_FROM_RECEIPT
        assert frame.event_ts.value == frame.recv_ts.value == 1_005
        assert (frame.price.value, frame.qty.value, frame.side) == (0, 0, _core.Side.Unknown)
        assert (frame.checksum, frame.depth, frame.trade_id, frame.venue_seq) == (0, 0, 0, 0)
        assert frame.reason == REASON.NoReason


# --- stored stream events ------------------------------------------------------------------


@pytest.mark.parametrize(
    ("kind", "event_kind", "reason", "step"),
    [
        (K.SEQUENCE_GAP, KIND.StreamStale, REASON.SequenceGap, "stale"),
        (K.SEQUENCE_REPEAT, KIND.StreamStale, REASON.SequenceRepeat, "stale"),
        (K.SEQUENCE_OUT_OF_ORDER, KIND.StreamStale, REASON.SequenceOutOfOrder, "stale"),
        (K.CHECKSUM_MISMATCH, KIND.StreamStale, REASON.ChecksumMismatch, "stale"),
        (K.MESSAGE_REJECTED, KIND.StreamStale, REASON.MessageRejected, "stale"),
        (K.SESSION_START, KIND.StreamReset, REASON.SessionStart, "reset"),
        (K.CONNECT, KIND.StreamReset, REASON.Connect, "reset"),
        (K.DISCONNECT, KIND.StreamReset, REASON.Disconnect, "reset"),
    ],
)
def test_a_stored_event_the_book_acts_on_becomes_a_control_event(kind, event_kind, reason, step):
    sources = SourceMap()
    (group,) = run([], [stored(4, kind, event_index=3, expected=10, observed=12)], sources=sources)
    (event,) = group.events
    assert (group.log_id, group.seq, group.step) == (LOG, 4, step)
    assert (event.kind, event.reason) == (event_kind, reason)
    assert event.level_index == 3  # points back at the stored row
    assert (event.seq, event.source) == (4, sources.source(LOG, "ws"))
    assert event.recv_ts.value == event.event_ts.value == 1_004
    assert event.instrument.value == INSTRUMENT_ID
    sequence = kind in (K.SEQUENCE_GAP, K.SEQUENCE_REPEAT, K.SEQUENCE_OUT_OF_ORDER)
    # A sequence event says which venue number arrived; nothing else does.
    assert event.flags == flags("EVENT_TS_FROM_RECEIPT", *(["HAS_VENUE_SEQ"] if sequence else []))
    assert event.venue_seq == (12 if sequence else 0)
    assert (event.price.value, event.qty.value, event.checksum, event.depth) == (0, 0, 0, 0)


def test_session_events_belong_to_the_stream_source():
    sources = SourceMap()
    groups = run([], [stored(0, K.SESSION_START, connection=None),
                      stored(1, K.CONNECT)], sources=sources)  # fmt: skip
    assert {g.events[0].source for g in groups} == {sources.source(LOG, "ws")}
    assert len(sources) == 1


def test_events_of_the_probe_and_events_no_book_acts_on_are_not_converted():
    events = [stored(1, K.SEQUENCE_GAP, connection="probe"),
              stored(1, K.CONNECT, connection="probe"),
              stored(1, K.DISCONNECT, connection="probe", event_index=1),
              *[stored(2, kind, event_index=i) for i, kind in enumerate(K)
                if kind not in STALE_REASONS and kind not in RESET_REASONS]]  # fmt: skip
    assert len(events) > 3 + 8
    assert run([], events) == []


def test_a_records_control_events_come_first_in_event_index_order():
    rows = [level(2, 0, BUY, 100, 3, kind=SNAP)]
    events = [stored(2, K.CONNECT, event_index=1), stored(2, K.DISCONNECT, event_index=0),
              stored(1, K.SEQUENCE_GAP), stored(3, K.SESSION_STOP, connection=None)]  # fmt: skip
    groups = run(rows, events)
    assert [(g.seq, g.step) for g in groups] == [(1, "stale"), (2, "reset"), (2, "reset"),
                                                 (2, "message")]  # fmt: skip
    assert [g.events[0].reason for g in groups[1:3]] == [REASON.Disconnect, REASON.Connect]


def test_logs_come_one_after_another_each_with_its_own_sources():
    rows = [level(3, 0, BUY, 1, 1, kind=SNAP, log_id=OTHER_LOG),
            level(4, 0, BUY, 2, 2, kind=CHECK, log_id=OTHER_LOG),
            level(1, 0, BUY, 3, 3, kind=SNAP), level(2, 0, BUY, 4, 4)]  # fmt: skip
    events = [stored(0, K.SESSION_START, connection=None, log_id=OTHER_LOG),
              stored(0, K.SESSION_START, connection=None)]  # fmt: skip
    sources = SourceMap()
    groups = run(rows, events, sources=sources)
    assert [(g.log_id, g.seq, g.step) for g in groups] == [
        (LOG, 0, "reset"), (LOG, 1, "message"), (LOG, 2, "message"),
        (OTHER_LOG, 0, "reset"), (OTHER_LOG, 3, "message"), (OTHER_LOG, 4, "check"),
    ]  # fmt: skip
    # Numbered in the order first used, from 0; the map gives each back.
    assert [sources.connection(i) for i in range(len(sources))] == [
        (LOG, "ws"), (OTHER_LOG, "ws"), (OTHER_LOG, "probe")]  # fmt: skip


def test_group_events_are_a_tuple_of_market_events():
    (group,) = run([level(6, 0, BUY, 100, 1)])
    assert isinstance(group, EventGroup)
    assert isinstance(group.events, tuple) and isinstance(group.events[0], EV)


# --- the source map ------------------------------------------------------------------------


def test_a_source_map_numbers_sources_in_the_order_first_used_and_can_be_shared():
    sources = SourceMap()
    assert sources.source(LOG, "ws") == 0
    assert sources.source(LOG, "probe") == 1
    assert sources.source(LOG, "ws") == 0
    assert sources.connection(1) == (LOG, "probe")
    # One harness, two instruments: the second conversion reuses the map.
    run([level(1, 0, BUY, 1, 1, kind=SNAP, log_id=OTHER_LOG)], sources=sources)
    assert sources.source(OTHER_LOG, "ws") == 2 and len(sources) == 3
    assert dict(sources.items()) == {0: (LOG, "ws"), 1: (LOG, "probe"), 2: (OTHER_LOG, "ws")}


def test_a_source_map_refuses_what_a_source_cannot_be():
    sources = SourceMap()
    for connection in ("rest:product", "", None, "WS"):
        with pytest.raises(ValueError, match="connection"):
            sources.source(LOG, connection)
    with pytest.raises(KeyError):
        sources.connection(5)
    full = SourceMap(first=2**32 - 1)
    assert full.source(LOG, "ws") == 2**32 - 1
    with pytest.raises(OverflowError, match="source"):  # MarketEvent.source is 32 bits
        full.source(LOG, "probe")


# --- what MarketEvent cannot hold (review D7) ----------------------------------------------


def test_a_depth_above_what_market_event_holds_is_refused():
    (group,) = run([level(1, 0, BUY, 1, 1, kind=SNAP, depth=MAX_DEPTH)])
    assert group.events[1].depth == MAX_DEPTH == 65_535
    with pytest.raises(ValueError, match="depth 65536"):
        run([level(1, 0, BUY, 1, 1, kind=SNAP, depth=MAX_DEPTH + 1)])


@pytest.mark.parametrize("field", ["price_exp", "qty_exp"])
@pytest.mark.parametrize("value", [-1, 19])
def test_an_exponent_outside_0_to_18_is_refused(field, value):
    # The schema refuses such a version; build the table around it, as a damaged
    # or hand-made input could.
    rows = instruments().to_pylist()
    rows[0][field] = value
    rows[0][field.replace("exp", "precision")] = min(value, 0)
    table = pa.Table.from_pylist(rows, schema=INSTRUMENTS.schema)
    with pytest.raises(ValueError, match=f"{field} {value}"):
        run([level(1, 0, BUY, 1, 1, kind=SNAP)], metadata=table)


def test_a_message_with_no_metadata_in_force_is_refused():
    with pytest.raises(ValueError, match="metadata"):
        run([level(1, 0, BUY, 1, 1, kind=SNAP, log_id=OTHER_LOG)], metadata=instruments(LOG))


def test_exponents_must_be_the_same_for_every_message():
    """ADR 0019 fixes an instrument's exponents; mantissas at two exponents in
    one book would mean nothing."""
    both = pa.concat_tables([instruments(LOG), instruments(OTHER_LOG, price_exp=7)])
    rows = [level(1, 0, BUY, 1, 1, kind=SNAP), level(1, 0, BUY, 1, 1, kind=SNAP, log_id=OTHER_LOG)]
    with pytest.raises(ValueError, match="exponent"):
        run(rows, metadata=both)
    # A precision may change (Kraken does); the mantissas do not.
    same = pa.concat_tables([instruments(LOG), instruments(OTHER_LOG, price_precision=2)])
    assert len(run(rows, metadata=same)) == 2


# --- instrument updates (ADR 0022) -----------------------------------------------------------


def version(seq, *, log_id=LOG, price_exp=8, qty_exp=8, price_precision=1, qty_precision=8,
            tick_size=10_000_000, lot_size=1):  # fmt: skip
    """One stored instruments row: the metadata version observed in record ``seq``."""
    return {"event_ts": 1_000 + seq, "event_ts_source": "receipt", "recv_ts": 1_000 + seq,
            "log_id": log_id, "seq": seq, "price_exp": price_exp, "qty_exp": qty_exp,
            "price_precision": price_precision, "qty_precision": qty_precision,
            "tick_size": tick_size, "lot_size": lot_size}  # fmt: skip


def versions(*rows):
    return table_from_rows(INSTRUMENTS, list(rows))


def test_every_stored_metadata_version_is_one_instrument_update_at_its_record():
    sources = SourceMap()
    meta = versions(version(1), version(4, price_precision=2, tick_size=1_000_000))
    rows = [level(3, 0, BUY, 100, 3, kind=SNAP), level(5, 0, BUY, 100, 0)]
    groups = run(rows, metadata=meta, sources=sources, with_instruments=True)
    assert [(g.log_id, g.seq, g.step) for g in groups] == [
        (LOG, 1, "instrument"), (LOG, 3, "message"), (LOG, 4, "instrument"),
        (LOG, 5, "message")]  # fmt: skip
    first, change = groups[0], groups[2]
    for group, precision, tick in ((first, 1, 10_000_000), (change, 2, 1_000_000)):
        (event,) = group.events
        assert event.kind == KIND.InstrumentUpdate
        got = event.instrument_meta()
        assert (got.id.value, got.price_exp, got.qty_exp) == (INSTRUMENT_ID, 8, 8)
        assert (got.price_precision, got.qty_precision) == (precision, 8)
        assert (got.tick_size.value, got.lot_size.value) == (tick, 1)
        # Stamped with its record; the venues give metadata no time of its own.
        assert (event.seq, event.recv_ts.value, event.event_ts.value) == (
            group.seq, 1_000 + group.seq, 1_000 + group.seq)  # fmt: skip
        assert event.flags == EV.EVENT_TS_FROM_RECEIPT
        # On the log's stream source, like a session event.
        assert event.source == sources.source(LOG, "ws")
        assert (event.reason, event.side) == (REASON.NoReason, _core.Side.Unknown)
        assert EV.from_bytes(event.to_bytes()).to_bytes() == event.to_bytes()
    assert len(sources) == 1


def test_a_version_with_a_venue_time_keeps_it():
    timed = {**version(1), "event_ts": 777, "event_ts_source": "venue"}
    (group,) = run([], metadata=versions(timed), with_instruments=True)
    (event,) = group.events
    assert (event.event_ts.value, event.recv_ts.value, event.flags) == (777, 1_001, 0)


def test_at_one_record_control_events_come_first_then_the_instrument_update_then_levels():
    rows = [level(2, 0, BUY, 100, 3, kind=SNAP)]
    events = [stored(2, K.CONNECT, event_index=1), stored(2, K.DISCONNECT, event_index=0)]
    groups = run(rows, events, metadata=versions(version(2)), with_instruments=True)
    assert [(g.seq, g.step) for g in groups] == [(2, "reset"), (2, "reset"), (2, "instrument"),
                                                 (2, "message")]  # fmt: skip


def test_each_logs_versions_are_on_its_own_stream_source_whatever_has_no_messages():
    sources = SourceMap()
    meta = versions(version(0), version(0, log_id=OTHER_LOG), version(9, log_id=OTHER_LOG))
    groups = run([level(1, 0, BUY, 1, 1, kind=SNAP)], metadata=meta, sources=sources,
                 with_instruments=True)  # fmt: skip
    assert [(g.log_id, g.seq, g.step) for g in groups] == [
        (LOG, 0, "instrument"), (LOG, 1, "message"), (OTHER_LOG, 0, "instrument"),
        (OTHER_LOG, 9, "instrument")]  # fmt: skip
    assert [g.events[0].source for g in groups] == [
        sources.source(LOG, "ws"), sources.source(LOG, "ws"), sources.source(OTHER_LOG, "ws"),
        sources.source(OTHER_LOG, "ws")]  # fmt: skip


@pytest.mark.parametrize(
    ("rows", "message"),
    [
        # Exponents are fixed per instrument (ADR 0019): two versions cannot differ.
        ([version(0), version(4, price_exp=7)], "exponent"),
        ([version(0), version(4, qty_exp=9, qty_precision=8)], "exponent"),
        ([version(0, price_exp=19, price_precision=0)], "price_exp 19"),
        ([version(0, price_precision=9)], "precision"),
        ([version(0, qty_precision=-1)], "precision"),
    ],
)
def test_a_metadata_version_no_event_can_carry_is_refused_before_anything_is_yielded(rows, message):
    # The schema refuses most of these; build the table around it, as a damaged
    # or hand-made input could.
    table = pa.Table.from_pylist(rows, schema=INSTRUMENTS.schema)
    groups = convert(BOOK_EVENTS.schema.empty_table(), [], table, instrument=INSTRUMENT_ID)
    with pytest.raises(ValueError, match=message):
        next(groups)


@pytest.mark.parametrize("venue", ["kraken", pytest.param("coinbase", marks=pytest.mark.slow)])
def test_on_a_fixture_the_sessions_metadata_is_one_update_before_its_first_snapshot(venue):
    book, events, meta = fixture_tables(venue)
    sources = SourceMap()
    groups = list(convert(book, events, meta, instrument=INSTRUMENT_ID, sources=sources))
    (index,) = [i for i, g in enumerate(groups) if g.step == "instrument"]
    (row,) = meta.to_pylist()
    group = groups[index]
    # Kraken's arrives on the stream, Coinbase's over REST: both on the stream source.
    assert (group.log_id, group.seq) == (row["log_id"], row["seq"])
    assert group.events[0].source == sources.source(row["log_id"], "ws")
    got = group.events[0].instrument_meta()
    assert (got.price_exp, got.qty_exp, got.price_precision, got.qty_precision) == (
        8, 8, {"kraken": 1, "coinbase": 2}[venue], 8)  # fmt: skip
    assert (got.tick_size.value, got.lot_size.value) == (row["tick_size"], row["lot_size"])
    first_message = next(i for i, g in enumerate(groups) if g.step == "message")
    assert index < first_message


def test_the_step_names_are_the_groups_a_user_may_meet():
    assert STEPS == ("message", "check", "stale", "reset", "instrument")


# --- the recorded fixtures and the integrity cases -------------------------------------------


def tables(records, venue):
    """What a session would return: book_events, stream_events rows, instruments."""
    normalized = normalize(records)
    book = table_from_rows(BOOK_EVENTS, normalized.rows[("book_events", venue, INSTRUMENT)])
    events = normalized.rows.get(("stream_events", venue, INSTRUMENT), [])
    meta = table_from_rows(INSTRUMENTS, normalized.rows[("instruments", venue, INSTRUMENT)])
    return book, events, meta


@functools.cache
def fixture_tables(venue):
    return tables(list(read_raw_log(fixture_path(venue))), venue)


@functools.cache
def case_tables(case):
    book, events, meta = tables(list(damaged(case)), case.venue)
    return book, events if case.stored_events else [], meta


def reference_steps(venue, book, events, meta):
    messages = list(messages_from_table(book, meta))
    return [(s.log_id, s.seq, s.step, s.reason if s.step != "message" else None)
            for s, _ in trace(venue, messages, events, mode="resume")]  # fmt: skip


def snake(name):
    return "".join(f"_{c.lower()}" if c.isupper() else c for c in name).lstrip("_")


def converted_steps(groups):
    steps = []
    for g in groups:
        if g.step in ("check", "instrument"):  # not steps of the reference book
            continue
        reason = snake(g.events[0].reason.name) if g.step in ("stale", "reset") else None
        steps.append((g.log_id, g.seq, g.step, reason))
    return steps


@pytest.mark.parametrize("venue", ["kraken", pytest.param("coinbase", marks=pytest.mark.slow)])
def test_on_a_fixture_every_event_is_well_formed_and_every_row_is_one_level(venue):
    book, events, meta = fixture_tables(venue)
    groups = list(convert(book, events, meta, instrument=INSTRUMENT_ID))
    all_events = [e for g in groups for e in g.events]
    for event in all_events:
        assert EV.from_bytes(event.to_bytes()).to_bytes() == event.to_bytes()
    levels = [e for e in all_events if e.kind in (KIND.BookSnapshot, KIND.BookDelta,
                                                  KIND.BookCheckSnapshot)]  # fmt: skip
    assert len(levels) == book.num_rows
    snapshots = len({(r["log_id"], r["seq"]) for r in book.to_pylist() if r["kind"] != DELTA})
    frames = [e for e in all_events if e.kind in (KIND.SnapshotBegin, KIND.SnapshotEnd)]
    assert len(frames) == 2 * snapshots == 4  # the stream's and the probe's
    assert sum(e.has(EV.LAST_IN_MESSAGE) for e in levels) == len(
        {(r["log_id"], r["seq"]) for r in book.to_pylist()})  # fmt: skip


@pytest.mark.parametrize("venue", ["kraken", pytest.param("coinbase", marks=pytest.mark.slow)])
def test_on_a_fixture_the_steps_are_the_reference_books_steps(venue):
    book, events, meta = fixture_tables(venue)
    groups = list(convert(book, events, meta, instrument=INSTRUMENT_ID))
    assert converted_steps(groups) == reference_steps(venue, book, events, meta)
    assert sum(g.step == "check" for g in groups) == 1


@pytest.fixture(
    params=[pytest.param(c, marks=pytest.mark.slow) if c.venue == "coinbase" else c for c in CASES],
    ids=lambda c: c.name,
)
def case(request):
    return request.param


def test_on_an_integrity_case_the_steps_are_the_reference_books_steps(case):
    """Each stale and reset step the reference takes is one control event here,
    with the reason of the same name, at the same record; each stream message
    is one group. The C++ book (Phase 4b) is compared with the reference at
    exactly these steps."""
    book, events, meta = case_tables(case)
    groups = list(convert(book, events, meta, instrument=INSTRUMENT_ID))
    steps = converted_steps(groups)
    assert steps == reference_steps(case.venue, book, events, meta)
    # Stale steps come from stored events; Kraken's rule never sees a repeat (Phase 2).
    has_stale_steps = any(step == "stale" for _, _, step, _ in steps)
    assert has_stale_steps == (case.stored_events and case.name != "kraken-duplicated")
