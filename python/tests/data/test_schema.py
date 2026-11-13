"""Bitemporal schemas for the normalized store (ADR 0005, ADR 0006)."""

import pyarrow as pa
import pytest

from quantagent.data.schema import (
    BOOK_EVENTS,
    DATASETS,
    INSTRUMENTS,
    TRADES,
    BookEventKind,
    SchemaError,
    Side,
    table_from_rows,
    validate_table,
)

ALL = [TRADES, BOOK_EVENTS, INSTRUMENTS]


def trade_row(**overrides):
    row = {
        "event_ts": 1_700_000_000_000_000_000,
        "event_ts_source": "venue",
        "recv_ts": 1_700_000_000_050_000_000,
        "log_id": "20231114T120000.000000000Z-000000000000",
        "seq": 1,
        "trade_id": "t-1",
        "price": 9712310,
        "qty": 150000,
        "side": Side.BUY,
    }
    row.update(overrides)
    return row


def book_row(**overrides):
    row = {
        "event_ts": 1_700_000_000_000_000_000,
        "event_ts_source": "venue",
        "recv_ts": 1_700_000_000_050_000_000,
        "log_id": "20231114T120000.000000000Z-000000000000",
        "seq": 1,
        "kind": BookEventKind.DELTA,
        "venue_seq_first": 100,
        "venue_seq_last": 105,
        "checksum": None,
        "depth": None,
        "level_index": 0,
        "side": Side.SELL,
        "price": 9712400,
        "qty": 0,
    }
    row.update(overrides)
    return row


def instrument_row(**overrides):
    row = {
        "event_ts": 1_700_000_000_000_000_000,
        "event_ts_source": "venue",
        "recv_ts": 1_700_000_000_050_000_000,
        "log_id": "20231114T120000.000000000Z-000000000000",
        "seq": 1,
        "price_exp": 2,
        "qty_exp": 5,
        "tick_size": 1,
        "lot_size": 1,
    }
    row.update(overrides)
    return row


ROW_FACTORIES = {"trades": trade_row, "book_events": book_row, "instruments": instrument_row}


def test_the_datasets_are_registered():
    # stream_events (ADR 0010) has its own tests in test_stream_events.py.
    assert set(DATASETS) == {"trades", "book_events", "instruments", "stream_events"}
    assert DATASETS["trades"] is TRADES
    assert DATASETS["book_events"] is BOOK_EVENTS
    assert DATASETS["instruments"] is INSTRUMENTS


@pytest.mark.parametrize("dataset", ALL, ids=lambda d: d.name)
def test_every_stored_row_carries_three_int64_timestamps(dataset):
    stored = dataset.stored_schema
    for name in ("event_ts", "recv_ts", "knowledge_ts"):
        field = stored.field(name)
        assert field.type == pa.int64(), name
        assert not field.nullable, name


@pytest.mark.parametrize("dataset", ALL, ids=lambda d: d.name)
def test_knowledge_ts_is_not_a_caller_column(dataset):
    assert "knowledge_ts" not in dataset.schema.names
    assert "knowledge_ts" in dataset.stored_schema.names


@pytest.mark.parametrize("dataset", ALL, ids=lambda d: d.name)
def test_no_floating_point_or_decimal_columns(dataset):
    for field in dataset.stored_schema:
        assert not pa.types.is_floating(field.type), field.name
        assert not pa.types.is_decimal(field.type), field.name


@pytest.mark.parametrize(
    ("dataset", "columns"),
    [
        (TRADES, ["price", "qty"]),
        (BOOK_EVENTS, ["price", "qty"]),
        (INSTRUMENTS, ["tick_size", "lot_size"]),
    ],
    ids=lambda v: getattr(v, "name", None),
)
def test_prices_and_quantities_are_int64_mantissas(dataset, columns):
    for name in columns:
        assert dataset.schema.field(name).type == pa.int64()


@pytest.mark.parametrize("dataset", ALL, ids=lambda d: d.name)
def test_key_columns_exist_and_schema_is_versioned(dataset):
    assert dataset.key
    for name in dataset.key:
        assert name in dataset.schema.names
    assert dataset.version >= 1


@pytest.mark.parametrize("dataset", ALL, ids=lambda d: d.name)
def test_partition_columns_are_not_stored_in_the_file(dataset):
    for name in ("venue", "instrument", "date"):
        assert name not in dataset.stored_schema.names


def test_instrument_metadata_has_exponents_tick_and_lot():
    for name in ("price_exp", "qty_exp", "tick_size", "lot_size"):
        assert name in INSTRUMENTS.schema.names


@pytest.mark.parametrize("dataset", ALL, ids=lambda d: d.name)
def test_table_from_rows_builds_a_table_with_the_exact_schema(dataset):
    rows = [ROW_FACTORIES[dataset.name](seq=i) for i in range(3)]
    table = table_from_rows(dataset, rows)
    assert table.num_rows == 3
    assert table.schema.equals(dataset.schema)
    validate_table(dataset, table)


def test_book_events_cover_snapshots_and_deltas_and_allow_missing_first_seq():
    rows = [
        book_row(kind=BookEventKind.SNAPSHOT, venue_seq_first=None, side=Side.BUY, qty=5),
        book_row(kind=BookEventKind.DELTA),
    ]
    table = table_from_rows(BOOK_EVENTS, rows)
    assert table.column("kind").to_pylist() == [BookEventKind.SNAPSHOT, BookEventKind.DELTA]
    assert table.column("venue_seq_first").to_pylist() == [None, 100]


@pytest.mark.parametrize("bad", [9712310.0, 9712310.5, "9712310", True, None])
def test_price_must_be_a_real_int(bad):
    with pytest.raises(SchemaError, match="price"):
        table_from_rows(TRADES, [trade_row(price=bad)])


@pytest.mark.parametrize("column", ["event_ts", "recv_ts", "qty", "seq"])
def test_integer_columns_reject_floats(column):
    with pytest.raises(SchemaError, match=column):
        table_from_rows(TRADES, [trade_row(**{column: 1.0})])


@pytest.mark.parametrize("value", [2**63, -(2**63) - 1])
def test_int64_overflow_is_rejected(value):
    with pytest.raises(SchemaError, match="price"):
        table_from_rows(TRADES, [trade_row(price=value)])


def test_negative_seq_is_rejected():
    with pytest.raises(SchemaError, match="seq"):
        table_from_rows(TRADES, [trade_row(seq=-1)])


def test_trade_id_must_be_str():
    with pytest.raises(SchemaError, match="trade_id"):
        table_from_rows(TRADES, [trade_row(trade_id=123)])


def test_missing_column_is_rejected():
    row = trade_row()
    del row["qty"]
    with pytest.raises(SchemaError, match="qty"):
        table_from_rows(TRADES, [row])


def test_unknown_column_is_rejected():
    with pytest.raises(SchemaError, match="surprise"):
        table_from_rows(TRADES, [trade_row(surprise=1)])


@pytest.mark.parametrize("dataset", ALL, ids=lambda d: d.name)
def test_caller_cannot_supply_knowledge_ts(dataset):
    row = ROW_FACTORIES[dataset.name](knowledge_ts=1)
    with pytest.raises(SchemaError, match="knowledge_ts"):
        table_from_rows(dataset, [row])


def test_negative_and_zero_prices_are_storable():
    # Flagging bad values is the quality checks' job (Phase 1d). The store must
    # be able to hold what the venue actually sent.
    table = table_from_rows(TRADES, [trade_row(price=0), trade_row(price=-5, qty=-1)])
    validate_table(TRADES, table)


@pytest.mark.parametrize("side", [2, -2, 7])
def test_trade_side_must_be_a_known_value(side):
    with pytest.raises(SchemaError, match="side"):
        table_from_rows(TRADES, [trade_row(side=side)])


def test_trade_side_may_be_unknown():
    table_from_rows(TRADES, [trade_row(side=Side.UNKNOWN)])


def test_book_side_must_be_bid_or_ask():
    with pytest.raises(SchemaError, match="side"):
        table_from_rows(BOOK_EVENTS, [book_row(side=Side.UNKNOWN)])


def test_book_kind_must_be_snapshot_or_delta():
    with pytest.raises(SchemaError, match="kind"):
        table_from_rows(BOOK_EVENTS, [book_row(kind=5)])


@pytest.mark.parametrize(
    "overrides",
    [
        {"price_exp": -1},
        {"price_exp": 19},
        {"qty_exp": -1},
        {"qty_exp": 19},
        {"tick_size": 0},
        {"tick_size": -1},
        {"lot_size": 0},
    ],
)
def test_instrument_metadata_must_be_structurally_valid(overrides):
    (column,) = overrides
    with pytest.raises(SchemaError, match=column):
        table_from_rows(INSTRUMENTS, [instrument_row(**overrides)])


def test_validate_table_rejects_a_table_with_another_schema():
    table = table_from_rows(TRADES, [trade_row()])
    with pytest.raises(SchemaError):
        validate_table(BOOK_EVENTS, table)
    with pytest.raises(SchemaError):
        validate_table(TRADES, table.drop_columns(["qty"]))
    as_float = table.set_column(
        table.schema.get_field_index("price"), "price", pa.array([97123.10], type=pa.float64())
    )
    with pytest.raises(SchemaError, match="price"):
        validate_table(TRADES, as_float)


def test_validate_table_rejects_nulls_in_required_columns():
    table = table_from_rows(TRADES, [trade_row()])
    with_null = table.set_column(
        table.schema.get_field_index("price"), "price", pa.array([None], type=pa.int64())
    )
    with pytest.raises(SchemaError, match="price"):
        validate_table(TRADES, with_null)


def test_validate_table_rejects_negative_timestamps():
    table = table_from_rows(TRADES, [trade_row()])
    for column in ("event_ts", "recv_ts"):
        negative = table.set_column(
            table.schema.get_field_index(column), column, pa.array([-1], type=pa.int64())
        )
        with pytest.raises(SchemaError, match=column):
            validate_table(TRADES, negative)
        with pytest.raises(SchemaError, match=column):
            table_from_rows(TRADES, [trade_row(**{column: -1})])


def test_validate_table_rejects_non_tables():
    with pytest.raises(TypeError):
        validate_table(TRADES, [trade_row()])


# --- schema version 2: ADR 0008 and venue-neutral book events ------------------


@pytest.mark.parametrize("dataset", ALL, ids=lambda d: d.name)
def test_every_dataset_is_at_schema_version_3_with_event_ts_source(dataset):
    assert dataset.version == 3
    field = dataset.schema.field("event_ts_source")
    assert field.type == pa.string()
    assert not field.nullable
    assert dataset.stored_schema.names[:4] == [
        "event_ts",
        "event_ts_source",
        "recv_ts",
        "knowledge_ts",
    ]


@pytest.mark.parametrize("dataset", ALL, ids=lambda d: d.name)
@pytest.mark.parametrize("bad", ["", "Venue", "RECEIPT", "exchange", "unknown", None, 1])
def test_event_ts_source_must_be_venue_or_receipt(dataset, bad):
    with pytest.raises(SchemaError, match="event_ts_source"):
        table_from_rows(dataset, [ROW_FACTORIES[dataset.name](event_ts_source=bad)])


@pytest.mark.parametrize("dataset", ALL, ids=lambda d: d.name)
def test_a_receipt_row_must_hold_recv_ts_in_event_ts(dataset):
    factory = ROW_FACTORIES[dataset.name]
    table_from_rows(dataset, [factory(event_ts_source="receipt", event_ts=77, recv_ts=77)])
    with pytest.raises(SchemaError, match="receipt"):
        table_from_rows(dataset, [factory(event_ts_source="receipt", event_ts=76, recv_ts=77)])
    # A venue timestamp may equal, precede or follow the receipt time.
    for event_ts in (76, 77, 78):
        table_from_rows(dataset, [factory(event_ts_source="venue", event_ts=event_ts, recv_ts=77)])
    mixed = [
        factory(event_ts_source="venue", event_ts=1, recv_ts=77, seq=0),
        factory(event_ts_source="receipt", event_ts=76, recv_ts=77, seq=1),
    ]
    with pytest.raises(SchemaError, match="receipt"):
        table_from_rows(dataset, mixed)


def test_book_events_are_keyed_by_raw_record_and_position():
    assert BOOK_EVENTS.key == ("log_id", "seq", "level_index")
    assert TRADES.key == ("trade_id",)
    assert INSTRUMENTS.key == ("log_id", "seq")


def test_book_events_work_for_a_venue_with_a_checksum_and_no_sequence_numbers():
    row = book_row(venue_seq_first=None, venue_seq_last=None, checksum=3310070434, depth=25)
    table = table_from_rows(BOOK_EVENTS, [row])
    assert table.column("checksum").to_pylist() == [3310070434]
    assert table.column("depth").to_pylist() == [25]
    assert table.column("venue_seq_last").to_pylist() == [None]
    assert BOOK_EVENTS.schema.field("checksum").type == pa.int64()
    assert BOOK_EVENTS.schema.field("depth").type == pa.int32()
    assert BOOK_EVENTS.schema.field("level_index").type == pa.int32()
    for name in ("venue_seq_first", "venue_seq_last", "checksum", "depth"):
        assert BOOK_EVENTS.schema.field(name).nullable, name
    assert not BOOK_EVENTS.schema.field("level_index").nullable


def test_one_message_can_update_the_same_price_level_twice():
    rows = [book_row(level_index=0, qty=5), book_row(level_index=1, qty=0)]
    table = table_from_rows(BOOK_EVENTS, rows)
    assert table.column("level_index").to_pylist() == [0, 1]


def test_book_events_have_a_kind_for_independent_check_snapshots():
    assert {k.name for k in BookEventKind} == {"SNAPSHOT", "DELTA", "CHECK_SNAPSHOT"}
    table_from_rows(BOOK_EVENTS, [book_row(kind=BookEventKind.CHECK_SNAPSHOT)])


@pytest.mark.parametrize(
    "overrides",
    [
        {"level_index": -1},
        {"level_index": None},
        {"checksum": -1},
        {"checksum": 2**32},
        {"depth": 0},
        {"depth": -5},
    ],
)
def test_book_event_columns_must_be_structurally_valid(overrides):
    (column,) = overrides
    with pytest.raises(SchemaError, match=column):
        table_from_rows(BOOK_EVENTS, [book_row(**overrides)])


# --- log_id (ADR 0013) ---------------------------------------------------------------------


@pytest.mark.parametrize("dataset", ALL, ids=lambda d: d.name)
def test_every_row_names_the_raw_log_it_came_from(dataset):
    field = dataset.schema.field("log_id")
    assert field.type == pa.string() and not field.nullable
    names = dataset.stored_schema.names
    assert names[names.index("knowledge_ts") + 1 : names.index("seq") + 1] == ["log_id", "seq"]


@pytest.mark.parametrize("dataset", ALL, ids=lambda d: d.name)
def test_no_key_depends_on_receive_time(dataset):
    assert "recv_ts" not in dataset.key
    assert "event_ts" not in dataset.key or dataset is TRADES


@pytest.mark.parametrize("dataset", ALL, ids=lambda d: d.name)
@pytest.mark.parametrize(
    "bad",
    ["", "test-log", "20231114T120000Z-000000000000", "20231114T120000.000000000Z-00000000000G",
     "20231114T120000.000000000Z-0000000000000", " 20231114T120000.000000000Z-000000000000",
     None, 5],
)  # fmt: skip
def test_log_id_must_be_well_formed(dataset, bad):
    with pytest.raises(SchemaError, match="log_id"):
        table_from_rows(dataset, [ROW_FACTORIES[dataset.name](log_id=bad)])
