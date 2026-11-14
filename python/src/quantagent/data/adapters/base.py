"""Shared machinery for venue adapters: raw records in, normalized rows out.

An adapter turns the raw messages of one venue into rows for the normalized
store. The rules that hold for every venue live here:

- Every raw record ends in exactly one outcome: ``data`` (it produced rows),
  ``ignored`` (a known kind of message that carries no market data, such as a
  heartbeat) or ``rejected`` (malformed or unknown). Nothing is dropped
  without being counted, and a bad message never stops the run.
- A message is all or nothing. If any part of it is malformed, none of its
  rows are kept.
- Prices and quantities become int64 mantissas through ``fixedpoint`` and never
  pass through ``float``.
- ``event_ts`` and ``event_ts_source`` follow ADR 0008: the source is decided
  by the kind of message, never by comparing timestamps.
- A trade seen again (same venue, instrument and ``trade_id``) is compared
  with its first observation in this normalizer, across every log fed to it.
  The store keeps one row per trade; an observation that differs in price,
  quantity or side is a TRADE_CONFLICT stream event (ADR 0015).
"""

import calendar
import json
import re
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from functools import lru_cache
from os import PathLike
from typing import Any

from quantagent.data.fixedpoint import to_mantissa
from quantagent.data.rawlog import RawRecord, log_id_of, read_raw_log
from quantagent.data.schema import (
    DATASETS,
    EVENT_TS_RECEIPT,
    EVENT_TS_VENUE,
    StreamEventKind,
    table_from_rows,
)
from quantagent.data.store import AppendOnlyStore, _check_name

Row = dict[str, Any]
RowKey = tuple[str, str, str]  # (dataset name, venue, instrument)

CONTROL_CHANNEL = "recorder"
STREAM_CHANNEL = "ws"
PROBE_CHANNEL = "probe"
_CONTROL_EVENTS = {"start", "clock", "connect", "subscribe", "disconnect", "rest", "stop"}
_MAX_DEPTH = 100_000
_MAX_EVENT_DETAIL = 200
# A change in (recv_ts - recv_mono_ns) between consecutive records larger than
# this is a wall-clock step (ADR 0012). Above the wall clock's tick on Windows
# (up to 15.6 ms) plus the time between the two reads; far below any step
# that matters.
CLOCK_STEP_FLOOR_NS = 25_000_000
# What must agree between two observations of one trade (ADR 0015).
_TRADE_FIELDS = ("price", "qty", "side")


def _compact(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


_RFC3339 = re.compile(
    r"([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})(?:\.([0-9]{1,9}))?Z"
)
_PLAIN_DECIMAL = re.compile(r"[0-9]+(?:\.([0-9]+))?")
_INTEGER = re.compile(r"-?[0-9]+")


@lru_cache(maxsize=8192)
def parse_rfc3339_ns(text: str) -> int:
    """Convert a UTC RFC 3339 timestamp ("...Z") to nanoseconds since the epoch, exactly."""
    if not isinstance(text, str):
        raise TypeError(f"timestamp must be str, got {type(text).__name__}")
    match = _RFC3339.fullmatch(text)
    if match is None:
        raise ValueError(f"not a UTC RFC 3339 timestamp: {text!r}")
    year, month, day, hour, minute, second = (int(g) for g in match.groups()[:6])
    # datetime() validates the calendar and clock fields.
    seconds = calendar.timegm(datetime(year, month, day, hour, minute, second).timetuple())
    if seconds < 0:
        raise ValueError(f"timestamp before 1970: {text!r}")
    return seconds * 1_000_000_000 + int((match.group(7) or "").ljust(9, "0"))


class Rejected(Exception):
    """Raised by an adapter to refuse a message for a stated reason."""

    def __init__(self, reason: str, detail: str):
        super().__init__(f"{reason}: {detail}")
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True, slots=True)
class Reject:
    seq: int
    recv_ts: int
    channel: str
    reason: str
    detail: str


@dataclass(frozen=True, slots=True)
class SequenceAnomaly:
    """A break in a venue's per-connection message numbering."""

    kind: str  # "gap", "repeat" or "out_of_order"
    connection: str
    expected: int
    received: int
    seq: int
    recv_ts: int


@dataclass
class Report:
    records: int = 0
    outcomes: Counter = field(default_factory=Counter)  # data / ignored / rejected
    messages: Counter = field(default_factory=Counter)  # kinds that produced rows
    ignored: Counter = field(default_factory=Counter)  # kinds that carry no data
    rows: Counter = field(default_factory=Counter)  # rows per dataset
    rejects: list[Reject] = field(default_factory=list)
    sequence_anomalies: list[SequenceAnomaly] = field(default_factory=list)
    events: Counter = field(default_factory=Counter)  # stream events by kind name
    trade_repeats: int = 0  # trades seen again, conflicting or not (ADR 0015)
    truncated: bool = False  # the raw log ended in a partial record

    @property
    def rejected(self) -> Counter:
        return Counter(reject.reason for reject in self.rejects)

    def summary(self) -> str:
        def listed(counts: Counter) -> str:
            return ", ".join(f"{name} {count}" for name, count in sorted(counts.items())) or "none"

        lines = [
            f"records {self.records}: {listed(self.outcomes)}",
            f"rows: {listed(self.rows)}",
            f"rejected: {listed(self.rejected)}",
            f"sequence anomalies: {len(self.sequence_anomalies)}",
            f"checksum mismatches: {self.events['CHECKSUM_MISMATCH']}",
            f"trade conflicts: {self.events['TRADE_CONFLICT']} "
            f"(trades seen again: {self.trade_repeats})",
        ]
        if self.truncated:
            lines.append("the raw log ended in a partial record")
        return "\n".join(lines)


@dataclass
class Normalized:
    rows: dict[RowKey, list[Row]]
    report: Report


# --- helpers for adapters ---------------------------------------------------------


def _refuse_constant(name: str) -> None:
    raise Rejected("malformed_message", f"{name} is not a number")


def load_json(payload: bytes, *, numbers_as_text: bool = False) -> Any:
    """Decode a JSON payload.

    With ``numbers_as_text`` every JSON number is returned as the exact text
    the venue sent, so prices written as numbers keep every digit.
    """
    try:
        text = payload.decode("utf-8")
        if numbers_as_text:
            return json.loads(text, parse_float=str, parse_int=str, parse_constant=_refuse_constant)
        return json.loads(text, parse_constant=_refuse_constant)
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as error:
        raise Rejected("invalid_json", f"{type(error).__name__}: {error}") from None


def as_object(value: Any, what: str = "message") -> dict:
    if not isinstance(value, dict):
        raise TypeError(f"{what} is not a JSON object")
    return value


def as_list(value: Any, what: str) -> list:
    if not isinstance(value, list):
        raise TypeError(f"{what} is not a JSON array")
    return value


def text_int(value: Any, what: str) -> int:
    """An integer that arrived as number text (see ``load_json``)."""
    if not isinstance(value, str) or _INTEGER.fullmatch(value) is None:
        raise ValueError(f"{what} is not an integer: {value!r}")
    return int(value)


def checked_depth(value: int) -> int:
    if not 1 <= value <= _MAX_DEPTH:
        raise ValueError(f"book depth {value} is out of range")
    return value


def increment(text: Any, what: str) -> tuple[int, int]:
    """Decimal places and mantissa of a tick or lot size given as a decimal string.

    "0.01" is two places with mantissa 1; "0.050" is two places with mantissa 5.
    """
    if not isinstance(text, str):
        raise TypeError(f"{what} must be a decimal string, got {type(text).__name__}")
    match = _PLAIN_DECIMAL.fullmatch(text)
    if match is None:
        raise ValueError(f"{what} is not a plain decimal: {text!r}")
    exponent = len((match.group(1) or "").rstrip("0"))
    return exponent, positive(to_mantissa(text, exponent), what)


def positive(value: int, what: str) -> int:
    if value <= 0:
        raise ValueError(f"{what} must be positive, got {value}")
    return value


def venue_time(text: Any) -> dict[str, Any]:
    """Timestamp columns for a message that carries the venue's own timestamp."""
    return {"event_ts": parse_rfc3339_ns(text), "event_ts_source": EVENT_TS_VENUE}


def receipt_time(record: RawRecord) -> dict[str, Any]:
    """Timestamp columns for a kind of message that has no venue timestamp (ADR 0008)."""
    return {"event_ts": record.recv_ts, "event_ts_source": EVENT_TS_RECEIPT}


Handled = tuple[str, str, list[tuple[str, Row]]]  # (outcome, message kind, (dataset, row) pairs)


def ignored(kind: str) -> Handled:
    return "ignored", kind, []


def data(kind: str, rows: list[tuple[str, Row]], *, empty: str) -> Handled:
    """``kind`` with its rows, or ignored as ``empty`` if the message had none."""
    return ("data", kind, rows) if rows else ignored(empty)


class VenueAdapter:
    """State and control-record handling common to every venue."""

    venue: str

    def __init__(self) -> None:
        self.instrument: str | None = None  # our name, used for the store partition
        self.symbol: str | None = None  # the venue's name for it
        self.start_depth: int | None = None
        self.exponents: tuple[int, int] | None = None  # (price_exp, qty_exp) in force
        self.anomalies: list[SequenceAnomaly] = []
        self.events: list[Row] = []  # stream events noticed while handling a record
        self._clock_offset: int | None = None  # recv_ts - recv_mono_ns of the previous record

    def note(
        self,
        kind: StreamEventKind,
        *,
        connection: str | None = None,
        expected: int | None = None,
        observed: int | None = None,
        detail: str | None = None,
    ) -> None:
        """Record a stream event for the record being handled (ADR 0010)."""
        self.events.append(
            {
                "kind": kind,
                "connection": connection,
                "expected": expected,
                "observed": observed,
                "detail": detail,
            }
        )

    def handle(self, record: RawRecord) -> Handled:
        try:
            if record.channel == CONTROL_CHANNEL:
                return self._control(record)
            return self.handle_venue(record)
        finally:
            self._track_clock(record)

    def _track_clock(self, record: RawRecord) -> None:
        """Note a wall-clock step against the monotonic counter (ADR 0012)."""
        if record.recv_mono_ns is None:
            return
        offset = record.recv_ts - record.recv_mono_ns
        previous, self._clock_offset = self._clock_offset, offset
        if previous is not None and abs(offset - previous) > CLOCK_STEP_FLOOR_NS:
            self.note(StreamEventKind.CLOCK_STEP, connection=record.channel,
                      observed=offset - previous)  # fmt: skip

    def handle_venue(self, record: RawRecord) -> Handled:
        raise NotImplementedError

    def on_connect(self, connection: str) -> None:
        """A new connection was opened; per-connection state starts again."""

    def on_disconnect(self, connection: str) -> None:
        """A connection was closed."""

    def on_session_start(self) -> None:
        """A recording session (a new raw log) began."""

    def on_session_stop(self) -> None:
        """The recording session ended."""

    def on_reject(self, record: RawRecord) -> None:
        """``record`` could not be normalized: whatever it carried was lost."""

    def _control(self, record: RawRecord) -> Handled:
        message = as_object(load_json(record.payload), "control record")
        event = message.get("event")
        if event not in _CONTROL_EVENTS:
            raise Rejected("unknown_message", f"control event {event!r}")
        if event == "start":
            if message["venue"] != self.venue:
                raise ValueError(f"session is for venue {message['venue']!r}")
            _check_name("instrument", message["instrument"])
            symbol, depth = message["symbol"], message["depth"]
            if not isinstance(symbol, str) or type(depth) is not int:
                raise TypeError("start record has a malformed symbol or depth")
            self.instrument, self.symbol = message["instrument"], symbol
            self.start_depth = checked_depth(depth)
            self.exponents = None
            self.on_session_start()
            self._clock_offset = None  # a new log: its counter reading starts afresh
            about = {
                k: message.get(k) for k in ("symbol", "depth", "recorder_version", "clock_identity")
            }
            about["monotonic_stamps"] = record.recv_mono_ns is not None
            self.note(StreamEventKind.SESSION_START, detail=_compact(about))
        elif event == "stop":
            self.on_session_stop()
            self.note(StreamEventKind.SESSION_STOP)
        elif event == "clock":
            self._clock(message)
        elif event == "connect":
            connection = str(message["connection"])
            self.on_connect(connection)
            self.note(StreamEventKind.CONNECT, connection=connection, detail=message.get("url"))
        elif event == "subscribe":
            self.note(
                StreamEventKind.SUBSCRIBE_REQUEST,
                connection=str(message["connection"]),
                detail=_compact(message.get("request")),
            )
        elif event == "disconnect":
            connection = str(message["connection"])
            self.on_disconnect(connection)
            self.note(
                StreamEventKind.DISCONNECT, connection=connection, detail=str(message.get("reason"))
            )
        return ignored(f"recorder.{event}")

    def _clock(self, message: dict) -> None:
        """The clock offset the recorder measured at session start (ADR 0011)."""
        offset, limit = message["offset_ns"], message["limit_ns"]
        override, servers = message["override"], message["servers"]
        if offset is not None and type(offset) is not int:
            raise TypeError(f"clock offset_ns is not an integer: {offset!r}")
        if type(limit) is not int or limit <= 0:
            raise ValueError(f"clock limit_ns is not a positive integer: {limit!r}")
        if type(override) is not bool:
            raise TypeError(f"clock override is not a boolean: {override!r}")
        servers = as_list(servers, "clock servers")
        self.note(
            StreamEventKind.CLOCK_OFFSET,
            expected=limit,
            observed=offset,
            detail=_compact({"servers": servers}),
        )
        if override:
            if offset is None:
                reason = "offset not measured"
            elif abs(offset) <= limit:
                reason = "offset within limit"
            else:
                reason = "offset above limit"
            self.note(StreamEventKind.CLOCK_OVERRIDE, detail=reason)

    def require_session(self) -> None:
        if self.symbol is None:
            raise Rejected("no_session_start", "no recorder start record before this message")

    def require_symbol(self, symbol: Any) -> None:
        self.require_session()
        if symbol != self.symbol:
            raise Rejected("unexpected_symbol", f"expected {self.symbol!r}")

    def require_exponents(self) -> tuple[int, int]:
        if self.exponents is None:
            raise Rejected("no_instrument_metadata", "no instrument metadata before this message")
        return self.exponents


# --- driver ------------------------------------------------------------------------


class Normalizer:
    """Feed raw records in order; take the rows out whenever convenient."""

    def __init__(self) -> None:
        from quantagent.data.adapters import ADAPTERS  # the registry imports this module

        self._registry = ADAPTERS
        self._adapters: dict[str, VenueAdapter] = {}
        self._rows: dict[RowKey, list[Row]] = {}
        self.pending_rows = 0  # rows produced and not yet taken
        self.report = Report()
        self._log_id: str | None = None  # the raw log being fed (ADR 0013)
        # The first observation of every trade fed so far (ADR 0015).
        self._trades: dict[tuple[str, str | None, str], dict[str, Any]] = {}

    def feed(self, record: RawRecord) -> None:
        report = self.report
        report.records += 1
        if record.seq == 0:
            self._log_id = log_id_of(record)  # every raw log begins at seq 0
        adapter = self._adapters.get(record.venue)
        try:
            if self._log_id is None:
                raise Rejected("no_log_start", "no record with seq 0 before this one")
            if adapter is None:
                if record.venue not in self._registry:
                    raise Rejected("unknown_venue", record.venue)
                adapter = self._adapters[record.venue] = self._registry[record.venue]()
            try:
                outcome, kind, rows = adapter.handle(record)
            finally:
                if adapter.anomalies:
                    report.sequence_anomalies.extend(adapter.anomalies)
                    adapter.anomalies.clear()
        except Rejected as refused:
            self._reject(record, adapter, refused.reason, refused.detail)
            return
        except (KeyError, TypeError, ValueError, IndexError, AttributeError) as error:
            self._reject(record, adapter, "malformed_message", f"{type(error).__name__}: {error}")
            return
        self._compare_trades(record, adapter, rows)
        self._store_events(record, adapter)

        report.outcomes[outcome] += 1
        if outcome == "ignored":
            report.ignored[kind] += 1
            return
        report.messages[kind] += 1
        instrument = adapter.instrument
        for dataset_name, row in rows:
            row["log_id"] = self._log_id
            self._rows.setdefault((dataset_name, record.venue, instrument), []).append(row)
            report.rows[dataset_name] += 1
        self.pending_rows += len(rows)

    def _reject(
        self, record: RawRecord, adapter: VenueAdapter | None, reason: str, detail: str
    ) -> None:
        self.report.outcomes["rejected"] += 1
        self.report.rejects.append(
            Reject(record.seq, record.recv_ts, record.channel, reason, detail or reason)
        )
        if adapter is not None and self._log_id is not None:
            # A record that could not be normalized is a hole in the stream.
            adapter.on_reject(record)
            adapter.note(
                StreamEventKind.MESSAGE_REJECTED,
                connection=record.channel,
                detail=f"{reason}: {detail}"[:_MAX_EVENT_DETAIL],
            )
            self._store_events(record, adapter)

    def _compare_trades(
        self, record: RawRecord, adapter: VenueAdapter, rows: list[tuple[str, Row]]
    ) -> None:
        """Compare each trade with the first observation of its trade_id."""
        for dataset_name, row in rows:
            if dataset_name != "trades":
                continue
            key = (record.venue, adapter.instrument, row["trade_id"])
            now = {"price": row["price"], "qty": row["qty"], "side": int(row["side"])}
            first = self._trades.get(key)
            if first is None:
                self._trades[key] = {"log_id": self._log_id, "seq": record.seq, **now}
                continue
            self.report.trade_repeats += 1
            differs = [name for name in _TRADE_FIELDS if first[name] != now[name]]
            if differs:
                detail = {"trade_id": row["trade_id"], "differs": differs, "first": first,
                          "now": now}  # fmt: skip
                adapter.note(StreamEventKind.TRADE_CONFLICT, connection=record.channel,
                             detail=_compact(detail))  # fmt: skip

    def _store_events(self, record: RawRecord, adapter: VenueAdapter) -> None:
        """Turn the events the adapter noted for ``record`` into stream_events rows."""
        if not adapter.events:
            return
        noted, adapter.events = adapter.events, []
        if adapter.instrument is None:
            return  # before any session start there is no partition to put them in
        key = ("stream_events", record.venue, adapter.instrument)
        rows = self._rows.setdefault(key, [])
        for index, event in enumerate(noted):
            rows.append(
                {
                    "event_ts": record.recv_ts,
                    "event_ts_source": EVENT_TS_RECEIPT,
                    "recv_ts": record.recv_ts,
                    "log_id": self._log_id,
                    "seq": record.seq,
                    "event_index": index,
                    **event,
                }
            )
            self.report.events[event["kind"].name] += 1
        self.report.rows["stream_events"] += len(noted)
        self.pending_rows += len(noted)

    def take_rows(self) -> dict[RowKey, list[Row]]:
        """Return the rows produced since the last call and forget them."""
        rows, self._rows = self._rows, {}
        self.pending_rows = 0
        return rows


def normalize(records: Iterable[RawRecord]) -> Normalized:
    """Normalize a whole sequence of raw records in memory."""
    normalizer = Normalizer()
    for record in records:
        normalizer.feed(record)
    return Normalized(rows=normalizer.take_rows(), report=normalizer.report)


_APPEND_ORDER = {"instruments": 0, "trades": 1, "book_events": 2, "stream_events": 3}


def ingest(
    path: str | PathLike[str], store: AppendOnlyStore, *, batch_rows: int = 200_000
) -> Report:
    """Normalize a raw log into ``store`` and return what happened to every record.

    Rows are appended whenever ``batch_rows`` of them are pending, one append
    per dataset and instrument. That bounds memory (a single book snapshot can
    be tens of thousands of rows, so the limit is on rows, not messages) and
    keeps the store from filling with one small file per message.
    """
    reader = read_raw_log(path)
    normalizer = Normalizer()

    def flush() -> None:
        pending = normalizer.take_rows()
        for key in sorted(pending, key=lambda k: (_APPEND_ORDER[k[0]], k)):
            dataset_name, venue, instrument = key
            dataset = DATASETS[dataset_name]
            store.append(dataset, venue, instrument, table_from_rows(dataset, pending[key]))

    for record in reader:
        normalizer.feed(record)
        if normalizer.pending_rows >= batch_rows:
            flush()
    flush()
    normalizer.report.truncated = reader.truncated
    return normalizer.report
