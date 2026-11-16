"""Data quality checks over the normalized store.

    python -m quantagent.data.quality --store data/normalized \
        --start 2026-11-11 --end 2026-11-11

Each check is a function of a ``PITSession`` and a time range. It reads only
through the session, so it sees what the session sees (the rows received by
its ``available_at``, at its store ``version``), and it cannot change
anything: a finding points at a row, it never removes or alters it.

The time range is receipt time (``recv_ts``) for every dataset, like the
session's own ranges (ADR 0017): a range of days means the data received on
those days, together with the integrity events noticed on them.

The checks:

- ``check_stream_integrity``: what the adapters recorded about the stream
  itself (ADR 0010): lost, repeated or reordered messages, checksum
  mismatches, messages that could not be normalized, dropped connections.
- ``check_recording_clock``: whether each recording session's clock was
  measured, and whether it was within the limit (ADR 0011).
- ``check_clock_skew``: receive time minus venue time outside a band, and
  venue time running backwards within a stream.
- ``check_crossed_books``: a book snapshot whose best bid is at or above its
  best ask, or that has only one side.
- ``check_outliers``: trade prices far from recent trades, and prices or
  quantities that cannot be real.
- ``check_trade_observations``: a trade another log saw with a different
  price, quantity or side than its first observation (ADR 0019).

A check returns its findings and some counts of what it examined, so that
"no findings" can be told apart from "nothing was looked at". Findings are
limited per severity (``max_findings`` each), so a lesser finding never
displaces a worse one, and a report with any finding cut fails (ADR 0019).
docs/data-core-spec.md section 10 lists every finding.
"""

import argparse
import json
import sys
from bisect import bisect_left, insort
from collections import Counter, deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass, fields
from datetime import UTC, date, datetime, timedelta
from enum import IntEnum
from pathlib import Path
from typing import Any

from quantagent.data.schema import BookEventKind, Side, StreamEventKind
from quantagent.data.session import PITSession, StoreVersionError
from quantagent.data.store import _check_name

_INT64_MAX = 2**63 - 1
_NS = 1_000_000_000
_DATA_VIEWS = ("trades", "book_events", "stream_events")
_SESSION_ENDED = "session ended"  # the recorder's reason for a normal shutdown
_PROBE_DONE = "snapshot received"
_STREAM = "ws"
_PROBE = "probe"


class Severity(IntEnum):
    INFO = 1  # worth knowing; the data is usable
    WARNING = 2  # the data may be affected
    ERROR = 3  # the data is wrong or incomplete here


@dataclass(frozen=True, slots=True)
class Finding:
    check: str
    severity: Severity
    what: str  # a short stable code, for example "sequence_gap"
    venue: str
    instrument: str
    dataset: str
    log_id: str  # with seq, the raw record the finding points at (ADR 0013)
    recv_ts: int  # when that record arrived
    seq: int
    message: str

    @property
    def where(self) -> str:
        return (
            f"{self.venue}/{self.instrument} {self.dataset} "
            f"record (log {self.log_id}, seq {self.seq})"
        )


@dataclass(frozen=True)
class CheckResult:
    check: str
    venue: str
    instrument: str
    findings: list[Finding]
    stats: dict[str, int]
    truncated: bool = False  # more findings existed than max_findings allowed
    # The severities some of whose findings were not kept, least severe first.
    # The limit is per severity, so a lesser finding never displaces a worse one.
    cut: tuple[Severity, ...] = ()


@dataclass(frozen=True)
class QualityConfig:
    """Thresholds. Times are nanoseconds; ``outlier_bps`` is basis points."""

    # Allowed range of recv_ts - event_ts. Slightly negative is tolerated:
    # two clocks never agree exactly.
    min_lag_ns: int = -500_000_000
    max_lag_ns: int = 10 * _NS
    # How far venue time may step back between consecutive messages.
    max_backwards_ns: int = 0
    # A trade is an outlier if it is more than outlier_bps away from the
    # median of the previous outlier_window trades. Trades with fewer than
    # outlier_min_trades before them are not judged.
    outlier_window: int = 50
    outlier_min_trades: int = 20
    outlier_bps: int = 500
    # A recording session whose clock was further than this from NTP time.
    max_clock_offset_ns: int = 250_000_000
    # A wall-clock step against the monotonic counter larger than this (ADR 0012).
    max_clock_step_ns: int = 50_000_000
    max_findings: int = 1000  # per check, instrument and severity

    def __post_init__(self) -> None:
        for item in fields(self):
            value = getattr(self, item.name)
            if type(value) is not int:
                raise ValueError(f"{item.name} must be an integer, got {value!r}")
        if self.min_lag_ns > self.max_lag_ns:
            raise ValueError("min_lag_ns must not exceed max_lag_ns")
        for name in ("max_backwards_ns", "outlier_bps", "max_clock_offset_ns", "max_clock_step_ns"):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must not be negative")
        for name in ("outlier_window", "outlier_min_trades", "max_findings"):
            if getattr(self, name) < 1:
                raise ValueError(f"{name} must be at least 1")
        if self.outlier_min_trades > self.outlier_window:
            raise ValueError("outlier_min_trades must not exceed outlier_window")

    @classmethod
    def from_file(cls, path: str | Path) -> "QualityConfig":
        """Load thresholds from a JSON object. Unknown names are an error."""
        try:
            values = json.loads(Path(path).read_text(encoding="utf-8"))
        except json.JSONDecodeError as error:
            raise ValueError(f"config file is not valid JSON: {error}") from None
        if not isinstance(values, dict):
            raise ValueError("config file must hold a JSON object")
        unknown = set(values) - {item.name for item in fields(cls)}
        if unknown:
            raise ValueError(f"unknown config names: {sorted(unknown)}")
        return cls(**values)


Check = Callable[[PITSession, str, str, int, int, QualityConfig], CheckResult]


# --- helpers ----------------------------------------------------------------------------


def _seconds(nanoseconds: int) -> str:
    return f"{nanoseconds / _NS:.3f} s"


class _Scope:
    """One check on one instrument over one time range."""

    def __init__(
        self, check: str, session: PITSession, venue: str, instrument: str, start: int, end: int,
        config: QualityConfig,
    ):  # fmt: skip
        _check_name("venue", venue)
        _check_name("instrument", instrument)
        for name, value in (("start", start), ("end", end)):
            if type(value) is not int or not 0 <= value <= _INT64_MAX:
                raise ValueError(f"{name} must be int nanoseconds, got {value!r}")
        if not isinstance(config, QualityConfig):
            raise TypeError("config must be a QualityConfig")
        self.check, self.session, self.config = check, session, config
        self.venue, self.instrument, self.start, self.end = venue, instrument, start, end
        self.findings: list[Finding] = []
        self.kept: Counter = Counter()  # findings kept, per severity
        self.cut: set[Severity] = set()
        self.stats: dict[str, int] = {}

    def where(self, alias: str = "") -> str:
        """SQL condition selecting this instrument and time range. The names
        were validated above and the times are integers."""
        p = f"{alias}." if alias else ""
        return (
            f"{p}venue = '{self.venue}' AND {p}instrument = '{self.instrument}' "
            f"AND {p}recv_ts >= {self.start} AND {p}recv_ts < {self.end}"
        )

    def rows(self, sql: str) -> list[dict[str, Any]]:
        return self.session.sql(sql).to_pylist()

    def room(self, severity: Severity) -> int:
        """How many more findings of ``severity`` fit, plus one so that a cut
        can be noticed: the LIMIT for a query that produces only that severity."""
        return max(self.config.max_findings - self.kept[severity], 0) + 1

    def add(
        self, severity: Severity, what: str, dataset: str, at: dict[str, Any], message: str
    ) -> None:
        """``at`` is the raw record: a row or event with log_id, seq and recv_ts.

        Each severity has its own limit of ``max_findings`` (review, R07): an
        ERROR is never dropped to make room for, or because of, lesser ones.
        """
        if self.kept[severity] >= self.config.max_findings:
            self.cut.add(severity)
            return
        self.kept[severity] += 1
        self.findings.append(
            Finding(
                check=self.check,
                severity=severity,
                what=what,
                venue=self.venue,
                instrument=self.instrument,
                dataset=dataset,
                log_id=at["log_id"],
                recv_ts=at["recv_ts"],
                seq=at["seq"],
                message=message,
            )
        )

    def result(self) -> CheckResult:
        return CheckResult(
            check=self.check,
            venue=self.venue,
            instrument=self.instrument,
            findings=self.findings,
            stats=self.stats,
            truncated=bool(self.cut),
            cut=tuple(sorted(self.cut)),
        )


# --- stream integrity -------------------------------------------------------------------

_SEQUENCE_FINDINGS = {
    StreamEventKind.SEQUENCE_GAP: ("sequence_gap", Severity.ERROR),
    StreamEventKind.SEQUENCE_REPEAT: ("sequence_repeat", Severity.WARNING),
    StreamEventKind.SEQUENCE_OUT_OF_ORDER: ("sequence_out_of_order", Severity.WARNING),
}


def check_stream_integrity(
    session: PITSession, venue: str, instrument: str, start: int, end: int, config: QualityConfig
) -> CheckResult:
    """Findings from the stream events the adapters recorded (ADR 0010).

    Errors mean market data is missing or cannot be trusted from that point:
    a sequence gap (Coinbase), a checksum mismatch (Kraken), a message that
    could not be normalized, or a trade the venue sent twice with a different
    price, quantity or side (ADR 0015). A dropped stream connection and its
    reconnect are warnings, because whatever happened in between was not seen.
    Trades the venue sent again unchanged, which the adapters dropped, are one
    INFO finding per session (ADR 0016).

    An absence of findings is not proof of a complete stream. Kraken's
    checksum covers only the ten best levels per side, and events are only
    known for the sessions that were recorded and normalized.
    """
    scope = _Scope("stream_integrity", session, venue, instrument, start, end, config)
    events = session.stream_events(venue, instrument, start, end).to_pylist()
    kinds = Counter(StreamEventKind(e["kind"]).name for e in events)
    scope.stats = {"events": len(events), "sessions": kinds["SESSION_START"], **kinds}

    # Whether each log's stream was already connected when the range began, so
    # that the first connect in a range is a reconnect if it is one (review, R08).
    stream_connected = _connected_before(scope, sorted({e["log_id"] for e in events}))
    repeats: dict[str, list[dict]] = {}  # TRADE_REPEAT events by session (log_id)
    for event in events:
        kind = StreamEventKind(event["kind"])
        connection, detail = event["connection"], event["detail"]

        def add(severity: Severity, what: str, message: str, event: dict = event) -> None:
            scope.add(severity, what, "stream_events", event, message)

        if kind == StreamEventKind.SESSION_START:
            stream_connected[event["log_id"]] = False
        elif kind == StreamEventKind.CONNECT and connection == _STREAM:
            if stream_connected.get(event["log_id"], False):
                add(
                    Severity.WARNING,
                    "reconnect",
                    "the stream connection was opened again; data in between was not seen",
                )
            stream_connected[event["log_id"]] = True
        elif kind == StreamEventKind.DISCONNECT:
            if connection == _STREAM and detail != _SESSION_ENDED:
                add(Severity.WARNING, "disconnect", f"the stream connection dropped: {detail}")
            elif connection == _PROBE and detail != _PROBE_DONE:
                add(Severity.INFO, "probe_failed", f"a check snapshot was not obtained: {detail}")
        elif kind in _SEQUENCE_FINDINGS:
            what, severity = _SEQUENCE_FINDINGS[kind]
            expected, observed = event["expected"], event["observed"]
            message = f"on {connection}: expected {expected}, received {observed}"
            if kind == StreamEventKind.SEQUENCE_GAP:
                missing = observed - expected
                message += f": {missing} message{'s' if missing != 1 else ''} missing"
            add(severity, what, message)
        elif kind == StreamEventKind.CHECKSUM_MISMATCH:
            add(
                Severity.ERROR,
                "checksum_mismatch",
                f"on {connection}: the venue's book checksum is {event['expected']}, the "
                f"rebuilt book's is {event['observed']}; the book is suspect until its "
                "next snapshot",
            )
        elif kind == StreamEventKind.MESSAGE_REJECTED:
            add(Severity.ERROR, "message_rejected", f"on {connection}: {detail}")
        elif kind == StreamEventKind.TRADE_CONFLICT:
            facts = _start_detail(event)
            first, now = facts.get("first") or {}, facts.get("now") or {}
            differs = facts.get("differs") or []
            changes = ", ".join(f"{n} {first.get(n)} then {now.get(n)}" for n in differs)
            add(
                Severity.ERROR,
                "trade_conflict",
                f"trade {facts.get('trade_id')} was seen again with a different "
                f"{' and '.join(differs)} ({changes}; first at log {first.get('log_id')}, "
                f"seq {first.get('seq')}); the store keeps one row for it (ADR 0015)",
            )
        elif kind == StreamEventKind.TRADE_REPEAT:
            repeats.setdefault(event["log_id"], []).append(event)
    # Identical repeats are summarized once per session, after the loop, so
    # that a venue that resends trades all day cannot fill the finding limit
    # ahead of an error (ADR 0016).
    for session_repeats in repeats.values():
        n = len(session_repeats)
        delays = [e["observed"] for e in session_repeats if e["observed"] is not None]
        when = (f", {_ms(min(delays))} to {_ms(max(delays))} after the first observation"
                if delays else "")  # fmt: skip
        scope.add(
            Severity.INFO,
            "trades_repeated",
            "stream_events",
            session_repeats[0],
            f"{n} trade{'s' if n != 1 else ''} {'were' if n != 1 else 'was'} sent again "
            f"unchanged in this session{when}; the repeats were dropped and the store keeps "
            "each trade's first observation (ADR 0016)",
        )
    return scope.result()


def _connected_before(scope: _Scope, logs: Sequence[str]) -> dict[str, bool]:
    """For each log, whether its stream connection was open when the range
    began: a stream connect since the log's last session start before it."""
    if not logs:
        return {}
    kinds = f"{int(StreamEventKind.SESSION_START)}, {int(StreamEventKind.CONNECT)}"
    connected: dict[str, bool] = {}
    for event in scope.rows(
        "SELECT log_id, seq, event_index, kind, connection FROM stream_events "
        f"WHERE venue = '{scope.venue}' AND instrument = '{scope.instrument}' "
        f"AND recv_ts < {scope.start} AND kind IN ({kinds}) AND log_id IN ({_quoted(logs)}) "
        "ORDER BY log_id, seq, event_index"
    ):
        if event["kind"] == StreamEventKind.SESSION_START:
            connected[event["log_id"]] = False
        elif event["connection"] == _STREAM:
            connected[event["log_id"]] = True
    return connected


def _ms(nanoseconds: int) -> str:
    return f"{nanoseconds / 1_000_000:.3f} ms"


# --- check snapshots --------------------------------------------------------------------


def _ranks(shallowest: Any, deepest: Any) -> str:
    return f"rank {shallowest}" if shallowest == deepest else f"ranks {shallowest} to {deepest}"


def check_independent_snapshots(
    session: PITSession, venue: str, instrument: str, start: int, end: int, config: QualityConfig
) -> CheckResult:
    """Independent check snapshots against the book rebuilt from the stream,
    from the adapters' CHECK_SNAPSHOT_COMPARED events (ADR 0014).

    A level that breaks the rule is an ERROR: the rebuilt book or the stream
    is wrong. A Coinbase snapshot taken between two stream batches that
    differs only on the next batch's levels is INFO: the stream cannot show
    the book between batches, and every other level matched. A snapshot that
    could not be compared is INFO, but a session (one raw log) none of whose
    snapshots in the range could be compared is a WARNING: its book was never
    checked against the venue's (ADR 0015). Exact snapshots are counted, not
    reported.

    Only the Coinbase adapter writes these events. Kraken's check snapshots are
    compared by the reference reconstructor (``book verify``), and its stream
    is verified by checksum after every message.
    """
    scope = _Scope("independent_snapshots", session, venue, instrument, start, end, config)
    kind = int(StreamEventKind.CHECK_SNAPSHOT_COMPARED)
    events = [e for e in session.stream_events(venue, instrument, start, end).to_pylist()
              if e["kind"] == kind]  # fmt: skip
    counts: Counter = Counter()
    for event in events:
        detail = _start_detail(event)
        result, seq = detail.get("result"), detail.get("snapshot_seq")
        counts[result] += 1
        if result == "mismatch":
            scope.add(
                Severity.ERROR,
                "check_snapshot_mismatch",
                "stream_events",
                event,
                f"the check snapshot at seq {seq} differs from the rebuilt book on "
                f"{detail.get('violations')} levels "
                f"({_ranks(detail.get('violations_shallowest'), detail.get('violations_deepest'))})"
                " that the stream fully determines (ADR 0014)",
            )
        elif result == "explained":
            ranks = _ranks(detail.get("unverified_shallowest"), detail.get("unverified_deepest"))
            scope.add(
                Severity.INFO,
                "check_snapshot_between_batches",
                "stream_events",
                event,
                f"the check snapshot at seq {seq} was taken between two stream batches: it "
                f"differs on {detail.get('unverified')} of the next batch's "
                f"{detail.get('next_batch_levels')} levels ({ranks}; "
                f"{detail.get('unverified_as_next_batch')} as that batch has them) and matches "
                "on every other level (ADR 0014)",
            )
        elif result == "not_compared":
            scope.add(
                Severity.INFO,
                "check_snapshot_not_compared",
                "stream_events",
                event,
                f"the check snapshot at seq {seq} could not be compared with the rebuilt book: "
                f"{detail.get('reason')}",
            )
        elif result != "exact":
            scope.add(Severity.WARNING, "check_snapshot_unreadable", "stream_events", event,
                      f"the comparison event has no known result: {event['detail']!r}")  # fmt: skip

    # A session (one raw log) none of whose check snapshots could be compared
    # was never checked against the venue's own book (ADR 0015).
    sessions: dict[str, list[dict[str, Any]]] = {}
    for event in events:
        sessions.setdefault(event["log_id"], []).append(event)
    uncompared = 0
    for session_events in sessions.values():
        details = [_start_detail(e) for e in session_events]
        if any(d.get("result") != "not_compared" for d in details):
            continue
        uncompared += 1
        reasons = Counter(str(d.get("reason")) for d in details)
        listed = ", ".join(f"{reason} {count}" for reason, count in sorted(reasons.items()))
        scope.add(
            Severity.WARNING,
            "no_check_snapshot_compared",
            "stream_events",
            session_events[0],
            f"none of this session's {len(details)} check snapshots in this range could be "
            f"compared with the rebuilt book ({listed}), so its book was never checked "
            "against the venue's (ADR 0015)",
        )
    scope.stats = {
        "snapshots": len(events),
        "exact": counts["exact"],
        "explained": counts["explained"],
        "mismatched": counts["mismatch"],
        "not_compared": counts["not_compared"],
        "sessions": len(sessions),
        "sessions_without_comparison": uncompared,
    }
    return scope.result()


# --- recording clock --------------------------------------------------------------------


def check_recording_clock(
    session: PITSession, venue: str, instrument: str, start: int, end: int, config: QualityConfig
) -> CheckResult:
    """Each recording session's clock, as the recorder measured it (ADR 0011).

    An offset beyond ``max_clock_offset_ns`` is an error: every ``recv_ts`` of
    that session is off by about that much. A session whose clock could not be
    measured is a warning; one recorded before clocks were measured at all is
    reported as information. Use of the recorder's override is always reported.

    A session is judged on every range it has data in, not only the one that
    holds its start (review, R08): the sessions are found by the logs of the
    rows received in the range, in any dataset, and each one's start, clock
    and override records are read wherever they are. A clock step is reported
    in the range where it happened.
    """
    scope = _Scope("recording_clock", session, venue, instrument, start, end, config)
    sessions: list[dict[str, Any]] = []
    for found in _sessions_with_data(scope):
        steps = [
            e
            for e in found["events"]
            if e["kind"] == StreamEventKind.CLOCK_STEP and start <= e["recv_ts"] < end
        ]
        clock = next((e for e in found["events"] if e["kind"] == StreamEventKind.CLOCK_OFFSET),
                     None)  # fmt: skip
        override = next(
            (e for e in found["events"] if e["kind"] == StreamEventKind.CLOCK_OVERRIDE), None
        )
        sessions.append({"start": found["start"], "clock": clock, "override": override,
                         "steps": steps})  # fmt: skip

    measured_offsets: list[int] = []
    steps: list[int] = []
    for found in sessions:
        clock, override = found["clock"], found["override"]
        if clock is None:
            if found["start"] is not None:
                begin = found["start"]
                scope.add(
                    Severity.INFO,
                    "clock_not_recorded",
                    "stream_events",
                    begin,
                    "this session was recorded before the clock was measured and before "
                    "records had monotonic stamps, so its recv_ts may be off by an unknown "
                    "amount and clock steps cannot be detected (docs/data-core.md, section 7)",
                )
        elif clock["observed"] is None:
            scope.add(
                Severity.WARNING,
                "clock_unmeasured",
                "stream_events",
                clock,
                "the recording clock could not be measured at session start",
            )
        else:
            offset = clock["observed"]
            measured_offsets.append(abs(offset))
            if abs(offset) > config.max_clock_offset_ns:
                note = "; the session was recorded with the override" if override else ""
                scope.add(
                    Severity.ERROR,
                    "clock_offset_exceeded",
                    "stream_events",
                    clock,
                    f"the recording clock was {offset / 1e6:+.1f} ms from NTP time (positive: "
                    f"ahead), beyond the {config.max_clock_offset_ns / 1e6:.1f} ms limit; "
                    f"every recv_ts of the session is off by about that much{note}",
                )
        begin = found["start"]
        monotonic = begin is not None and _start_detail(begin).get("monotonic_stamps") is True
        if clock is not None and begin is not None and not monotonic:
            scope.add(
                Severity.INFO,
                "clock_steps_not_measurable",
                "stream_events",
                begin,
                "this session's records have no monotonic stamps, so wall-clock steps "
                "during it cannot be detected (ADR 0012)",
            )
        for step in found["steps"]:
            size = step["observed"]
            steps.append(abs(size))
            if abs(size) > config.max_clock_step_ns:
                direction = "back" if size < 0 else "forward"
                scope.add(
                    Severity.ERROR,
                    "clock_step",
                    "stream_events",
                    step,
                    f"the wall clock stepped {abs(size) / 1e6:.1f} ms {direction} against the "
                    f"monotonic counter here, beyond the {config.max_clock_step_ns / 1e6:.1f} "
                    "ms limit; recv_ts before and after this record are not comparable"
                    + " (ADR 0012)",
                )
        if override is not None:
            scope.add(
                Severity.INFO,
                "clock_override",
                "stream_events",
                override,
                f"recorded with --allow-clock-offset ({override['detail']})",
            )

    scope.stats = {
        "sessions": sum(found["start"] is not None for found in sessions),
        "measured": len(measured_offsets),
        "overridden": sum(found["override"] is not None for found in sessions),
        "clock_steps": len(steps),
    }
    if measured_offsets:
        scope.stats["max_abs_offset_ns"] = max(measured_offsets)
    if steps:
        scope.stats["max_abs_step_ns"] = max(steps)
    return scope.result()


_SESSION_KINDS = (
    StreamEventKind.SESSION_START,
    StreamEventKind.CLOCK_OFFSET,
    StreamEventKind.CLOCK_OVERRIDE,
    StreamEventKind.CLOCK_STEP,
)


def _quoted(values: Sequence[str]) -> str:
    """SQL string literals. Only for log ids, whose form the schema checks."""
    return ", ".join("'" + value.replace("'", "''") + "'" for value in values)


def _sessions_with_data(scope: _Scope) -> list[dict[str, Any]]:
    """The recording sessions with rows received in the scope's range.

    A session runs from a ``SESSION_START`` to the next one in its log (one
    per raw log, as the recorder writes them). Each comes with every
    session-level event of it (start, clock, override, steps) the session can
    see, whatever its receipt time; ``start`` is None for rows before any
    start in their log.
    """
    union = " UNION ALL ".join(f"SELECT log_id, seq FROM {view} WHERE {scope.where()}"
                               for view in _DATA_VIEWS)  # fmt: skip
    spans = scope.rows(
        f"SELECT log_id, min(seq) AS first, max(seq) AS last FROM ({union}) x "
        "GROUP BY log_id ORDER BY log_id"
    )
    if not spans:
        return []
    kinds = ", ".join(str(int(kind)) for kind in _SESSION_KINDS)
    events = scope.rows(
        "SELECT log_id, seq, event_index, recv_ts, kind, expected, observed, detail "
        f"FROM stream_events WHERE venue = '{scope.venue}' AND instrument = '{scope.instrument}' "
        f"AND kind IN ({kinds}) AND log_id IN ({_quoted([s['log_id'] for s in spans])}) "
        "ORDER BY log_id, seq, event_index"
    )
    found: list[dict[str, Any]] = []
    for span in spans:
        of_log = [e for e in events if e["log_id"] == span["log_id"]]
        starts = [e["seq"] for e in of_log if e["kind"] == StreamEventKind.SESSION_START]
        # Session boundaries in this log: before its first start, then each start.
        bounds = [None, *starts] if not starts or starts[0] > 0 else starts
        for index, begin in enumerate(bounds):
            until = bounds[index + 1] if index + 1 < len(bounds) else None
            low = -1 if begin is None else begin
            high = _INT64_MAX if until is None else until - 1
            if span["last"] < low or span["first"] > high:
                continue  # no row of this session in the range
            members = [e for e in of_log if low <= e["seq"] <= high]
            start = next((e for e in members if e["kind"] == StreamEventKind.SESSION_START
                          and e["seq"] == begin), None)  # fmt: skip
            if begin is None and not members:
                continue
            found.append({"start": start, "events": members})
    return found


def _start_detail(event: dict[str, Any]) -> dict[str, Any]:
    try:
        detail = json.loads(event["detail"] or "{}")
    except json.JSONDecodeError:
        return {}
    return detail if isinstance(detail, dict) else {}


# --- clock skew -------------------------------------------------------------------------

_VENUE_TIME = "event_ts_source = 'venue'"
# True for a trade whose record the adapter marked as history sent on subscribe.
_IS_BACKFILL = (
    "EXISTS (SELECT 1 FROM stream_events e WHERE e.venue = t.venue "
    f"AND e.instrument = t.instrument AND e.kind = {int(StreamEventKind.TRADE_BACKFILL)} "
    "AND e.log_id = t.log_id AND e.seq = t.seq)"
)


def check_clock_skew(
    session: PITSession, venue: str, instrument: str, start: int, end: int, config: QualityConfig
) -> CheckResult:
    """Receive time against venue time, for book events and live trades.

    Only rows whose ``event_ts`` came from the venue are considered. A row
    with ``event_ts_source = "receipt"`` has no venue time to compare (ADR
    0008); counting it would look like a perfect clock. Backfilled trades are
    left out too: they were received long after they happened, by design.

    One finding is reported per message, not per row.
    """
    scope = _Scope("clock_skew", session, venue, instrument, start, end, config)
    book = f"FROM book_events WHERE {scope.where()}"
    trades = f"FROM trades t WHERE {scope.where('t')}"
    live_trades = f"{trades} AND t.{_VENUE_TIME} AND NOT {_IS_BACKFILL}"

    (lags,) = scope.rows(
        "SELECT count(*) AS n, min(d) AS lo, max(d) AS hi, median(d) AS mid FROM ("
        f"SELECT recv_ts - event_ts AS d {book} AND {_VENUE_TIME} "
        f"UNION ALL SELECT t.recv_ts - t.event_ts AS d {live_trades}) x"
    )
    (receipt,) = scope.rows(
        f"SELECT count(*) AS n FROM (SELECT seq {book} AND NOT {_VENUE_TIME} "
        f"UNION ALL SELECT t.seq {trades} AND NOT t.{_VENUE_TIME}) x"
    )
    (backfill,) = scope.rows(f"SELECT count(*) AS n {trades} AND {_IS_BACKFILL}")
    scope.stats = {
        "rows_considered": lags["n"],
        "receipt_rows_skipped": receipt["n"],
        "backfill_trades_skipped": backfill["n"],
    }
    if lags["n"]:
        scope.stats |= {
            "min_lag_ns": lags["lo"],
            "median_lag_ns": int(lags["mid"]),
            "max_lag_ns": lags["hi"],
        }

    low, high = config.min_lag_ns, config.max_lag_ns
    sources = (
        ("book_events", f"{book} AND {_VENUE_TIME}", ""),
        ("trades", live_trades, "t."),
    )
    for dataset, source, p in sources:
        lag = f"{p}recv_ts - {p}event_ts"
        for row in scope.rows(
            f"SELECT {p}log_id AS log_id, {p}seq AS seq, max({p}recv_ts) AS recv_ts, "
            f"min({lag}) AS lo, max({lag}) AS hi "
            f"{source} GROUP BY {p}log_id, {p}seq "
            f"HAVING min({lag}) < {low} OR max({lag}) > {high} "
            f"ORDER BY 1, 2 LIMIT {scope.room(Severity.WARNING)}"
        ):
            if row["lo"] < low:
                note = " (the venue's timestamp is later than receipt)" if row["lo"] < 0 else ""
                scope.add(
                    Severity.WARNING,
                    "early",
                    dataset,
                    row,
                    f"received {_seconds(row['lo'])} after venue time, below the allowed "
                    f"{_seconds(low)}{note}",
                )
            if row["hi"] > high:
                scope.add(
                    Severity.WARNING,
                    "late",
                    dataset,
                    row,
                    f"received {_seconds(row['hi'])} after venue time, above the allowed "
                    f"{_seconds(high)}",
                )

    # Venue time stepping back between consecutive messages of one stream: one
    # log, in record order. Check snapshots come from another connection and are
    # not part of it.
    in_stream = f"kind <> {int(BookEventKind.CHECK_SNAPSHOT)}"
    streams = (
        ("book_events", f"{book} AND {_VENUE_TIME} AND {in_stream}", ""),
        ("trades", live_trades, "t."),
    )
    for dataset, source, p in streams:
        for row in scope.rows(
            "SELECT log_id, seq, recv_ts, step FROM (SELECT log_id, seq, recv_ts, "
            "ts - lag(ts) OVER (PARTITION BY log_id ORDER BY seq) AS step FROM ("
            f"SELECT {p}log_id AS log_id, {p}seq AS seq, max({p}recv_ts) AS recv_ts, "
            f"max({p}event_ts) AS ts {source} GROUP BY {p}log_id, {p}seq) m) d "
            f"WHERE step < {-config.max_backwards_ns} ORDER BY 1, 2 "
            f"LIMIT {scope.room(Severity.WARNING)}"
        ):
            scope.add(
                Severity.WARNING,
                "event_ts_backwards",
                dataset,
                row,
                f"venue time is {_seconds(-row['step'])} before the previous message's",
            )
    return scope.result()


# --- crossed books ----------------------------------------------------------------------


def check_crossed_books(
    session: PITSession, venue: str, instrument: str, start: int, end: int, config: QualityConfig
) -> CheckResult:
    """Book snapshots whose best bid is at or above their best ask.

    This looks at snapshots as the venue sent them (the stream's own and the
    independent check snapshots). An update message alone cannot be judged;
    checking the book after every update needs it rebuilt, which is the
    reference reconstructor's job (roadmap Phase 2). No top-of-book quotes are
    stored, so there is nothing else to check here.
    """
    scope = _Scope("crossed_books", session, venue, instrument, start, end, config)
    bid, ask = int(Side.BUY), int(Side.SELL)
    snapshots = scope.rows(
        "SELECT log_id, seq, max(recv_ts) AS recv_ts, "
        f"max(CASE WHEN side = {bid} THEN price END) AS bid, "
        f"min(CASE WHEN side = {ask} THEN price END) AS ask "
        f"FROM book_events WHERE {scope.where()} AND kind <> {int(BookEventKind.DELTA)} "
        "GROUP BY log_id, seq ORDER BY log_id, seq"
    )
    scope.stats = {"snapshots_checked": len(snapshots)}
    for row in snapshots:
        best_bid, best_ask = row["bid"], row["ask"]
        if best_bid is None or best_ask is None:
            side = "bids" if best_bid is None else "asks"
            scope.add(
                Severity.WARNING,
                "one_sided",
                "book_events",
                row,
                f"the snapshot has no {side}",
            )
        elif best_bid >= best_ask:
            what = "locked" if best_bid == best_ask else "crossed"
            scope.add(
                Severity.ERROR,
                what,
                "book_events",
                row,
                f"best bid {best_bid} is {'equal to' if what == 'locked' else 'above'} "
                f"best ask {best_ask} (price mantissas)",
            )
    return scope.result()


# --- outliers ---------------------------------------------------------------------------


class _RollingMedian:
    """The lower median of the last ``size`` values added."""

    def __init__(self, size: int):
        self._size = size
        self._order: deque[int] = deque()
        self._sorted: list[int] = []

    def __len__(self) -> int:
        return len(self._order)

    def add(self, value: int) -> None:
        self._order.append(value)
        insort(self._sorted, value)
        if len(self._order) > self._size:
            del self._sorted[bisect_left(self._sorted, self._order.popleft())]

    def median(self) -> int:
        return self._sorted[(len(self._sorted) - 1) // 2]


def check_outliers(
    session: PITSession, venue: str, instrument: str, start: int, end: int, config: QualityConfig
) -> CheckResult:
    """Values that cannot be real, and trade prices far from recent trades.

    A trade's reference is the median of the trades before it among those
    received in the time range, in venue-time order. A lasting move stops being flagged once the
    new level is the majority of the window. Zero and negative prices are
    errors and are never used as a reference.
    """
    scope = _Scope("outliers", session, venue, instrument, start, end, config)
    trades = session.sql(
        "SELECT log_id, seq, recv_ts, trade_id, price, qty FROM trades "
        f"WHERE {scope.where()} ORDER BY event_ts, log_id, seq, trade_id"
    ).to_pylist()
    reference = _RollingMedian(config.outlier_window)
    judged = 0
    for trade in trades:
        trade_id, price, qty = trade["trade_id"], trade["price"], trade["qty"]
        if qty <= 0:
            scope.add(
                Severity.ERROR,
                "nonpositive_quantity",
                "trades",
                trade,
                f"trade {trade_id}: quantity mantissa {qty}",
            )
        if price <= 0:
            scope.add(
                Severity.ERROR,
                "nonpositive_price",
                "trades",
                trade,
                f"trade {trade_id}: price mantissa {price}",
            )
            continue
        if len(reference) >= config.outlier_min_trades:
            judged += 1
            middle = reference.median()
            if abs(price - middle) * 10_000 > config.outlier_bps * middle:
                away = abs(price - middle) * 10_000 // middle
                scope.add(
                    Severity.WARNING,
                    "price_outlier",
                    "trades",
                    trade,
                    f"trade {trade_id}: price {price} is {away} bps from {middle}, the "
                    f"median of the previous {len(reference)} trades (price mantissas)",
                )
        reference.add(price)

    snapshot_kinds = f"kind <> {int(BookEventKind.DELTA)}"
    (book,) = scope.rows(f"SELECT count(*) AS n FROM book_events WHERE {scope.where()}")
    scope.stats = {
        "trades_checked": len(trades),
        "trades_with_reference": judged,
        "book_rows_checked": book["n"],
    }
    # One query per severity, each limited by that severity's room, so that
    # warnings early in the range cannot keep a later error out (review, R07).
    levels = "SELECT log_id, seq, recv_ts, level_index, kind, price, qty FROM book_events"
    order = "ORDER BY log_id, seq, level_index"
    for row in scope.rows(
        f"{levels} WHERE {scope.where()} AND (price <= 0 OR qty < 0) "
        f"{order} LIMIT {scope.room(Severity.ERROR)}"
    ):
        level = f"level {row['level_index']}"
        if row["price"] <= 0:
            message = f"{level}: price mantissa {row['price']}"
            scope.add(Severity.ERROR, "nonpositive_price", "book_events", row, message)
        if row["qty"] < 0:
            message = f"{level}: quantity mantissa {row['qty']}"
            scope.add(Severity.ERROR, "negative_quantity", "book_events", row, message)
    for row in scope.rows(
        f"{levels} WHERE {scope.where()} AND qty = 0 AND {snapshot_kinds} "
        f"{order} LIMIT {scope.room(Severity.WARNING)}"
    ):
        # Zero means "remove this level", which only makes sense in an update.
        message = f"level {row['level_index']}: a snapshot lists a level with zero quantity"
        scope.add(Severity.WARNING, "zero_quantity_in_snapshot", "book_events", row, message)
    return scope.result()


# --- trades seen in several logs ------------------------------------------------------

_TRADE_FIELDS = ("price", "qty", "side")


def check_trade_observations(
    session: PITSession, venue: str, instrument: str, start: int, end: int, config: QualityConfig
) -> CheckResult:
    """Observations of a trade in another log that disagree with its first (ADR 0019).

    Every observation of a trade is stored, and a session shows the first in
    ``(log_id, seq)`` order (ADR 0018). Within one log the normalizer
    compares them and writes ``TRADE_CONFLICT``, which ``stream_integrity``
    reports; but each log is loaded in its own run, so nothing compares two
    logs. This does: for every observation received in the range from a log
    other than its trade's first observation's, a different price, quantity
    or side is an ERROR (``trade_conflict_across_logs``), reported at the
    later observation wherever the first was received. An identical one is
    counted (``repeated_across_logs``): venues resend their recent trades on
    every subscribe, so it is expected and not reported.
    """
    scope = _Scope("trade_observations", session, venue, instrument, start, end, config)
    observations = session.trade_observations(venue, instrument, start, end).to_pylist()
    several: set[str] = set()
    repeated = conflicts = 0
    for seen in observations:
        if seen["log_id"] == seen["first_log_id"]:
            continue  # the first observation, or a repeat within its log (TRADE_CONFLICT)
        several.add(seen["trade_id"])
        differs = [name for name in _TRADE_FIELDS if seen[name] != seen[f"first_{name}"]]
        if not differs:
            repeated += 1
            continue
        conflicts += 1
        changes = ", ".join(f"{name} {seen[f'first_{name}']} then {seen[name]}" for name in differs)
        scope.add(
            Severity.ERROR,
            "trade_conflict_across_logs",
            "trades",
            seen,
            f"trade {seen['trade_id']} was seen in this log with a different "
            f"{' and '.join(differs)} ({changes}) from its first observation (log "
            f"{seen['first_log_id']}, seq {seen['first_seq']}); the session shows the first "
            "(ADR 0018)",
        )
    scope.stats = {
        "observations": len(observations),
        "trades_seen_in_several_logs": len(several),
        "repeated_across_logs": repeated,
        "conflicts_across_logs": conflicts,
    }
    return scope.result()


CHECKS: tuple[Check, ...] = (
    check_stream_integrity,
    check_independent_snapshots,
    check_recording_clock,
    check_clock_skew,
    check_crossed_books,
    check_outliers,
    check_trade_observations,
)


def run_checks(
    session: PITSession,
    start: int,
    end: int,
    config: QualityConfig | None = None,
    *,
    venue: str | None = None,
    instrument: str | None = None,
) -> list[CheckResult]:
    """Run every check on every venue and instrument with data in the range."""
    config = config or QualityConfig()
    for name, value in (("start", start), ("end", end)):
        if type(value) is not int or not 0 <= value <= _INT64_MAX:
            raise ValueError(f"{name} must be int nanoseconds, got {value!r}")
    found: set[tuple[str, str]] = set()
    for view in _DATA_VIEWS:
        pairs = session.sql(
            f"SELECT DISTINCT venue, instrument FROM {view} "
            f"WHERE recv_ts >= {start} AND recv_ts < {end}"
        )
        found.update((row["venue"], row["instrument"]) for row in pairs.to_pylist())
    results = []
    for found_venue, found_instrument in sorted(found):
        if venue not in (None, found_venue) or instrument not in (None, found_instrument):
            continue
        for check in CHECKS:
            results.append(check(session, found_venue, found_instrument, start, end, config))
    return results


# --- report command -----------------------------------------------------------------------


def _day(text: str) -> date:
    try:
        return date.fromisoformat(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a date (YYYY-MM-DD): {text!r}") from None


def _midnight_ns(day: date) -> int:
    return int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp()) * _NS


def _stats_line(result: CheckResult) -> str:
    stats = result.stats
    if result.check == "clock_skew" and "median_lag_ns" in stats:
        return (
            f"{stats['rows_considered']} rows, lag min {_seconds(stats['min_lag_ns'])}, "
            f"median lag {_seconds(stats['median_lag_ns'])}, max {_seconds(stats['max_lag_ns'])}; "
            f"skipped {stats['receipt_rows_skipped']} without venue time, "
            f"{stats['backfill_trades_skipped']} backfilled trades"
        )
    return ", ".join(f"{name} {value}" for name, value in stats.items())


def _report(results: Sequence[CheckResult], max_print: int, out: Callable[[str], None]) -> None:
    current = None
    for result in results:
        if (result.venue, result.instrument) != current:
            current = (result.venue, result.instrument)
            out(f"\n{result.venue}/{result.instrument}")
        counts = Counter(f.severity for f in result.findings)
        graded = ", ".join(f"{counts[s]} {s.name}" for s in sorted(counts, reverse=True))
        more = ""
        if result.cut:
            names = " and ".join(s.name for s in result.cut)
            more = f" (more {names} findings were cut off by max_findings; raise it)"
        total = len(result.findings)
        summary = f"{total} finding{'s' if total != 1 else ''}" + (f": {graded}" if graded else "")
        out(f"  {result.check}: {summary}{more}")
        out(f"    looked at: {_stats_line(result)}")
        for finding in result.findings[:max_print]:
            out(f"    {finding.severity.name} {finding.what}: {finding.message}")
            out(f"      at {finding.where}")
        if total > max_print:
            out(f"    ... and {total - max_print} more")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m quantagent.data.quality",
        description="Run the data quality checks over a date range and print a summary.",
    )
    parser.add_argument("--store", required=True, help="directory of the normalized store")
    parser.add_argument(
        "--start", required=True, type=_day, help="first day received, UTC (YYYY-MM-DD)"
    )
    parser.add_argument("--end", required=True, type=_day, help="last day received, UTC, inclusive")
    parser.add_argument("--venue")
    parser.add_argument("--instrument")
    parser.add_argument(
        "--available-at",
        type=int,
        help="simulated time in ns since the epoch: check only what had been received by "
        "then (default: everything received)",
    )
    parser.add_argument(
        "--version",
        type=int,
        help="knowledge time in ns since the epoch: check the store as of that committed "
        "batch (default: the latest committed batch)",
    )
    parser.add_argument("--as-of", help=argparse.SUPPRESS)  # retired; see below
    parser.add_argument(
        "--fail-on",
        choices=[s.name.lower() for s in Severity],
        default="error",
        help="exit with status 1 if any finding has at least this severity (default: error)",
    )
    parser.add_argument("--config", help="JSON file of thresholds (see QualityConfig)")
    parser.add_argument("--max-print", type=int, default=20, help="findings shown per check")
    args = parser.parse_args(argv)
    if args.end < args.start:
        parser.error("--end is before --start")
    if args.as_of is not None:
        parser.error(
            "--as-of was retired (ADR 0017): use --version for a knowledge cutoff and "
            "--available-at for a receipt-time cutoff"
        )
    try:
        config = QualityConfig.from_file(args.config) if args.config else QualityConfig()
    except (OSError, ValueError) as error:
        parser.error(f"--config: {error}")

    start = _midnight_ns(args.start)
    end = _midnight_ns(args.end + timedelta(days=1))
    available_at = _INT64_MAX if args.available_at is None else args.available_at
    fail_on = Severity[args.fail_on.upper()]

    try:
        with PITSession(args.store, available_at=available_at, version=args.version) as session:
            version = session.version
            results = run_checks(
                session, start, end, config, venue=args.venue, instrument=args.instrument
            )
    except StoreVersionError as error:
        print(f"refused: {error}", file=sys.stderr)
        return 2
    except ValueError as error:  # a version later than the latest committed batch
        parser.error(str(error))

    received = "" if args.available_at is None else f", received by {available_at}"
    print(
        f"Data quality, received {args.start} to {args.end} (UTC), "
        f"store version {version}{received}"
    )
    severities = [finding.severity for result in results for finding in result.findings]
    if not results:
        # Passing silently on an empty range would hide a recorder that stopped.
        print("WARNING no_data: no data in this range for the requested venue and instrument")
        severities.append(Severity.WARNING)
    _report(results, args.max_print, print)

    # A report that could not hold every finding is incomplete, whatever it
    # holds: it fails, so that no limit can turn a failing range into a
    # passing one (review, R07).
    cut = sorted({severity for result in results for severity in result.cut})
    if not severities:
        print("\nResult: no findings")
        return 0
    worst = max(severities)
    count = len(severities)
    print(
        f"\nResult: {count} finding{'s' if count != 1 else ''} (worst: {worst.name}); "
        f"failing at {fail_on.name}"
    )
    if cut:
        names = " and ".join(severity.name for severity in cut)
        print(f"TRUNCATED: {names} findings were cut off by max_findings, so this report is "
              "incomplete and fails; raise max_findings to see them all")  # fmt: skip
        return 1
    return 1 if worst >= fail_on else 0


if __name__ == "__main__":
    sys.exit(main())
