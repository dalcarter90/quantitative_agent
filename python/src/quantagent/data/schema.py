"""Bitemporal schemas for the normalized store (ADR 0005, ADR 0006).

Every stored row carries three timestamps, all int64 nanoseconds since the
Unix epoch, UTC:

- ``event_ts``: when the venue says it happened. If the venue's message has
  no timestamp, it holds ``recv_ts`` and ``event_ts_source`` is ``"receipt"``
  (ADR 0008).
- ``recv_ts``: when our process received it.
- ``knowledge_ts``: when the row became available in the store. Assigned by
  the store's writer; callers never supply it.

Prices and quantities are int64 mantissas at the instrument's ``price_exp``
and ``qty_exp`` (see ``fixedpoint``). There are no floating-point columns.

Venue, instrument and date are not columns. They are the store's partition
directories, so a row's identity is (venue, instrument) plus the dataset's
``key`` columns. A correction is a new row with the same key and a later
``knowledge_ts``.

``log_id`` names the raw log a row came from (ADR 0013), and ``seq`` is the
row's raw record in that log. ``(log_id, seq)`` is the order of stored data;
no key depends on ``recv_ts``, which is a measurement (ADR 0012).
"""

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc

from quantagent.data.fixedpoint import MAX_EXP

KNOWLEDGE_TS = "knowledge_ts"


class SchemaError(ValueError):
    """Rows do not match a dataset's schema."""


class Side(IntEnum):
    """Bid/ask for book levels; aggressor side for trades."""

    SELL = -1
    UNKNOWN = 0  # trades only, when the venue does not report the aggressor
    BUY = 1


class BookEventKind(IntEnum):
    SNAPSHOT = 0  # one level of a book image that starts or restarts the stream
    DELTA = 1  # one level of an incremental update; qty 0 removes the level
    # One level of an independent book image taken on another connection. It
    # is not part of the update stream; it exists to check a reconstruction.
    CHECK_SNAPSHOT = 2


class StreamEventKind(IntEnum):
    """What happened to a capture stream (ADR 0010)."""

    SESSION_START = 0  # a recording session began
    SESSION_STOP = 1
    CONNECT = 2  # a connection was opened; a second one on "ws" is a reconnect
    DISCONNECT = 3  # detail holds the reason
    SUBSCRIBE_REQUEST = 4  # we asked for a channel; detail holds the request
    SUBSCRIPTION_ACK = 5  # the venue confirmed a subscription change
    # The trades in this record are history the venue sent on subscribe, not
    # trades observed as they happened. Their receipt time says nothing about
    # when they occurred.
    TRADE_BACKFILL = 6
    # The recording clock's offset from NTP time, measured at session start
    # (ADR 0011). observed is the offset in ns (positive: the clock was ahead),
    # or null if it could not be measured; expected is the limit in force.
    CLOCK_OFFSET = 7
    # The recorder was run with --allow-clock-offset; detail says whether the
    # offset was within the limit, above it, or not measured.
    CLOCK_OVERRIDE = 8
    # The wall clock moved against the monotonic counter between two records
    # by more than the adapter's floor (ADR 0012). observed is the signed size
    # in ns: negative means the wall clock went back.
    CLOCK_STEP = 9
    SEQUENCE_GAP = 10  # expected and observed hold the venue's sequence numbers
    SEQUENCE_REPEAT = 11
    SEQUENCE_OUT_OF_ORDER = 12
    # expected is the venue's checksum, observed the one computed from the
    # book rebuilt from the stream. The book is suspect until the next snapshot.
    CHECKSUM_MISMATCH = 13
    MESSAGE_REJECTED = 14  # the record could not be normalized; detail says why
    # A check snapshot compared with the book rebuilt from the stream under the
    # venue's rule (ADR 0014). Noticed at the record where the comparison could
    # be made: the snapshot's own, or a later one. expected is the number of
    # levels that break the rule (0 when it holds), observed the number of
    # levels the rule cannot verify that differ; both null if it could not be
    # compared. detail is JSON naming the snapshot's record and the result.
    CHECK_SNAPSHOT_COMPARED = 15


EVENT_TS_VENUE = "venue"
EVENT_TS_RECEIPT = "receipt"


@dataclass(frozen=True, eq=False)
class Dataset:
    """A table in the normalized store.

    ``schema`` is what callers supply. ``stored_schema`` adds ``knowledge_ts``.
    ``allowed`` and ``bounds`` are structural rules (enum membership, inclusive
    ranges). They are not data quality checks: implausible market values are
    stored as received and flagged later by the quality checks.
    """

    name: str
    version: int
    schema: pa.Schema
    key: tuple[str, ...]
    allowed: Mapping[str, frozenset[int] | frozenset[str]] = field(default_factory=dict)
    bounds: Mapping[str, tuple[int | None, int | None]] = field(default_factory=dict)

    @property
    def stored_schema(self) -> pa.Schema:
        fields = list(self.schema)
        position = self.schema.get_field_index("recv_ts") + 1
        fields.insert(position, pa.field(KNOWLEDGE_TS, pa.int64(), nullable=False))
        metadata = {"qa.dataset": self.name, "qa.schema_version": str(self.version)}
        return pa.schema(fields, metadata=metadata)


def _required(name: str, type_: pa.DataType) -> pa.Field:
    return pa.field(name, type_, nullable=False)


# Common leading columns. ``log_id`` is the raw log the row came from and
# ``seq`` its record number there; (log_id, seq) is the order of stored data
# (ADR 0012, ADR 0013).
_HEADER = [
    _required("event_ts", pa.int64()),
    _required("event_ts_source", pa.string()),  # "venue" or "receipt", ADR 0008
    _required("recv_ts", pa.int64()),
    _required("log_id", pa.string()),
    _required("seq", pa.uint64()),
]
# The form rawlog.log_id_of produces.
LOG_ID_PATTERN = r"[0-9]{8}T[0-9]{6}\.[0-9]{9}Z-[0-9a-f]{12}"
_LOG_ID = re.compile(LOG_ID_PATTERN)
_TIMESTAMP_BOUNDS = {"event_ts": (0, None), "recv_ts": (0, None)}
_EVENT_TS_SOURCES = {"event_ts_source": frozenset({EVENT_TS_VENUE, EVENT_TS_RECEIPT})}

TRADES = Dataset(
    name="trades",
    version=3,
    schema=pa.schema(
        [
            *_HEADER,
            _required("trade_id", pa.string()),  # the venue's trade identifier
            _required("price", pa.int64()),
            _required("qty", pa.int64()),
            _required("side", pa.int8()),  # aggressor, a Side value
        ]
    ),
    key=("trade_id",),
    allowed={**_EVENT_TS_SOURCES, "side": frozenset(int(s) for s in Side)},
    bounds=_TIMESTAMP_BOUNDS,
)

# One row per price level. A venue message with many levels becomes many rows
# that share log_id, seq and the venue's integrity columns, and are numbered
# by level_index in the order the venue listed them. That order matters: a
# venue may update the same price twice in one message.
#
# Venues protect the stream in different ways, so the integrity columns are
# all optional: Coinbase numbers its messages (venue_seq_last), Kraken sends a
# checksum of the top of the book and no sequence number at all.
#
# A level's identity is which raw record it came from and where in it, so
# normalizing the same raw log again produces corrections, not duplicates.
BOOK_EVENTS = Dataset(
    name="book_events",
    version=3,
    schema=pa.schema(
        [
            *_HEADER,
            _required("kind", pa.int8()),  # a BookEventKind value
            # The venue's own sequence range for the message this level came
            # from. Venues with a single sequence number leave "first" null.
            pa.field("venue_seq_first", pa.int64(), nullable=True),
            pa.field("venue_seq_last", pa.int64(), nullable=True),
            # The venue's checksum of its book after this message (unsigned 32-bit).
            pa.field("checksum", pa.int64(), nullable=True),
            # Levels per side the venue keeps for this subscription, when it
            # sends a truncated book. Null means the full book.
            pa.field("depth", pa.int32(), nullable=True),
            _required("level_index", pa.int32()),
            _required("side", pa.int8()),  # Side.BUY (bid) or Side.SELL (ask)
            _required("price", pa.int64()),
            _required("qty", pa.int64()),
        ]
    ),
    key=("log_id", "seq", "level_index"),
    allowed={
        **_EVENT_TS_SOURCES,
        "kind": frozenset(int(k) for k in BookEventKind),
        "side": frozenset({int(Side.BUY), int(Side.SELL)}),
    },
    bounds={
        **_TIMESTAMP_BOUNDS,
        "level_index": (0, None),
        "checksum": (0, 2**32 - 1),
        "depth": (1, None),
    },
)

# One row per version of an instrument's metadata, observed in one raw record.
# ``event_ts`` is when the version takes effect. The key is the observation, not
# event_ts: for metadata without a venue timestamp event_ts is the receipt
# time, and no key depends on recv_ts (ADR 0013).
INSTRUMENTS = Dataset(
    name="instruments",
    version=3,
    schema=pa.schema(
        [
            *_HEADER,
            _required("price_exp", pa.int8()),
            _required("qty_exp", pa.int8()),
            _required("tick_size", pa.int64()),  # mantissa at price_exp
            _required("lot_size", pa.int64()),  # mantissa at qty_exp
        ]
    ),
    key=("log_id", "seq"),
    allowed=_EVENT_TS_SOURCES,
    bounds={
        **_TIMESTAMP_BOUNDS,
        "price_exp": (0, MAX_EXP),
        "qty_exp": (0, MAX_EXP),
        "tick_size": (1, None),
        "lot_size": (1, None),
    },
)

# Integrity events for every venue, one row per event. Each refers to the raw
# record it was noticed at (log_id, seq); event_index orders several events
# from one record. These are our own observations, so event_ts is always the
# receipt time (ADR 0008).
STREAM_EVENTS = Dataset(
    name="stream_events",
    version=2,
    schema=pa.schema(
        [
            *_HEADER,
            _required("event_index", pa.int32()),
            _required("kind", pa.int8()),  # a StreamEventKind value
            pa.field("connection", pa.string(), nullable=True),  # "ws", "probe", ...
            pa.field("expected", pa.int64(), nullable=True),
            pa.field("observed", pa.int64(), nullable=True),
            pa.field("detail", pa.string(), nullable=True),
        ]
    ),
    key=("log_id", "seq", "event_index"),
    allowed={
        "event_ts_source": frozenset({EVENT_TS_RECEIPT}),
        "kind": frozenset(int(k) for k in StreamEventKind),
    },
    bounds={**_TIMESTAMP_BOUNDS, "event_index": (0, None)},
)

DATASETS: Mapping[str, Dataset] = {
    d.name: d for d in (TRADES, BOOK_EVENTS, INSTRUMENTS, STREAM_EVENTS)
}


def _integer_range(type_: pa.DataType) -> tuple[int, int]:
    bits = type_.bit_width
    if pa.types.is_unsigned_integer(type_):
        return 0, 2**bits - 1
    return -(2 ** (bits - 1)), 2 ** (bits - 1) - 1


def _check_value(dataset: Dataset, column: pa.Field, value: Any, row: int) -> Any:
    where = f"{dataset.name}.{column.name} (row {row})"
    if value is None:
        if not column.nullable:
            raise SchemaError(f"{where}: null in a required column")
        return None
    if pa.types.is_integer(column.type):
        # bool is an int subclass, and pyarrow would silently accept 1.0.
        if not isinstance(value, int) or isinstance(value, bool):
            raise SchemaError(f"{where}: expected int, got {type(value).__name__} {value!r}")
        low, high = _integer_range(column.type)
        if not low <= value <= high:
            raise SchemaError(f"{where}: {value} does not fit in {column.type}")
        return int(value)
    if pa.types.is_string(column.type):
        if not isinstance(value, str):
            raise SchemaError(f"{where}: expected str, got {type(value).__name__} {value!r}")
        return value
    raise SchemaError(f"{where}: unsupported column type {column.type}")


def table_from_rows(dataset: Dataset, rows: Sequence[Mapping[str, Any]]) -> pa.Table:
    """Build a table for ``dataset`` from row dicts, with strict type checks.

    Unlike pyarrow's own conversion, this never coerces: a float, bool or
    numeric string in an integer column is an error.
    """
    names = dataset.schema.names
    expected = set(names)
    columns: dict[str, list[Any]] = {name: [] for name in names}
    for index, row in enumerate(rows):
        given = set(row)
        if KNOWLEDGE_TS in given:
            raise SchemaError(
                f"{dataset.name} (row {index}): {KNOWLEDGE_TS} is assigned by the store "
                "and cannot be supplied"
            )
        if given != expected:
            missing = sorted(expected - given)
            unknown = sorted(given - expected)
            raise SchemaError(
                f"{dataset.name} (row {index}): missing columns {missing}, "
                f"unknown columns {unknown}"
            )
        for column in dataset.schema:
            columns[column.name].append(_check_value(dataset, column, row[column.name], index))

    arrays = [pa.array(columns[column.name], type=column.type) for column in dataset.schema]
    table = pa.Table.from_arrays(arrays, schema=dataset.schema)
    validate_table(dataset, table)
    return table


def validate_table(dataset: Dataset, table: pa.Table) -> None:
    """Raise SchemaError unless ``table`` has exactly ``dataset``'s caller columns
    and types, no nulls in required columns, and satisfies the structural rules."""
    if not isinstance(table, pa.Table):
        raise TypeError(f"expected a pyarrow.Table, got {type(table).__name__}")

    given = set(table.schema.names)
    if KNOWLEDGE_TS in given:
        raise SchemaError(
            f"{dataset.name}: {KNOWLEDGE_TS} is assigned by the store and cannot be supplied"
        )
    expected = set(dataset.schema.names)
    if given != expected or len(table.schema.names) != len(expected):
        missing = sorted(expected - given)
        unknown = sorted(given - expected)
        raise SchemaError(f"{dataset.name}: missing columns {missing}, unknown columns {unknown}")

    for column in dataset.schema:
        actual = table.schema.field(column.name).type
        if actual != column.type:
            raise SchemaError(f"{dataset.name}.{column.name}: expected {column.type}, got {actual}")
        if not column.nullable and table.column(column.name).null_count:
            raise SchemaError(f"{dataset.name}.{column.name}: null in a required column")

    if table.num_rows == 0:
        return

    for name, values in dataset.allowed.items():
        value_set = pa.array(sorted(values), type=dataset.schema.field(name).type)
        if not pc.all(pc.is_in(table.column(name), value_set=value_set)).as_py():
            raise SchemaError(f"{dataset.name}.{name}: value not in {sorted(values)}")

    for name, (low, high) in dataset.bounds.items():
        extremes = pc.min_max(table.column(name)).as_py()
        if extremes["min"] is None:
            continue  # the column is entirely null
        if low is not None and extremes["min"] < low:
            raise SchemaError(f"{dataset.name}.{name}: {extremes['min']} is below {low}")
        if high is not None and extremes["max"] > high:
            raise SchemaError(f"{dataset.name}.{name}: {extremes['max']} is above {high}")

    # ADR 0013: every row names a well-formed raw log.
    log_ids = table.column("log_id")
    if not pc.all(pc.match_substring_regex(log_ids, f"^{LOG_ID_PATTERN}$")).as_py():
        bad = next(v for v in log_ids.to_pylist() if not _LOG_ID.fullmatch(v))
        raise SchemaError(f"{dataset.name}.log_id: {bad!r} is not a log id")

    # ADR 0008: a row without a venue timestamp carries its receipt time.
    receipt = pc.equal(table.column("event_ts_source"), EVENT_TS_RECEIPT)
    mismatch = pc.and_(receipt, pc.not_equal(table.column("event_ts"), table.column("recv_ts")))
    if pc.any(mismatch).as_py():
        raise SchemaError(
            f"{dataset.name}: a row with event_ts_source 'receipt' must have "
            "event_ts equal to recv_ts"
        )
