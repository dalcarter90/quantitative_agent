"""Replay order across raw logs (ADR 0012).

Within one raw log, records are replayed in ``seq`` order: the order the
recorder wrote them, which is the order they arrived. ``recv_ts`` is the wall
clock, which a time service can step, so it is a measurement and never an
ordering key.

Several logs recorded on the same machine in the same boot are merged by each
record's ``recv_mono_ns``, the system's monotonic counter read with its
``recv_ts``. Each log's start record names its counter (``clock_identity``);
logs whose counters are not comparable, or that have no monotonic stamps
(version 1 logs, recorded before ADR 0012), are refused rather than merged by
wall time.
"""

import heapq
import json
import os
from collections.abc import Iterator, Sequence
from pathlib import Path

from quantagent.data.clock import ClockIdentity
from quantagent.data.rawlog import RawRecord, read_raw_log


class NotComparable(ValueError):
    """These logs' monotonic stamps cannot be compared, so they cannot be merged."""


def log_clock_identity(path: str | os.PathLike[str]) -> ClockIdentity | None:
    """The clock identity named by a log's start record, or None if it has none."""
    for record in read_raw_log(path):
        if record.channel != "recorder":
            return None
        try:
            start = json.loads(record.payload)
        except json.JSONDecodeError:
            return None
        if not isinstance(start, dict) or start.get("event") != "start":
            return None
        identity = start.get("clock_identity")
        return None if identity is None else ClockIdentity.from_json(identity)
    return None


def _stamped(index: int, path: Path) -> Iterator[tuple[int, int, int, RawRecord]]:
    previous = -1
    for record in read_raw_log(path):
        mono = record.recv_mono_ns
        if mono is None:
            raise NotComparable(f"{path.name}: record seq {record.seq} has no monotonic stamp")
        if mono < previous:
            raise ValueError(
                f"{path.name}: monotonic stamps decrease at seq {record.seq} "
                f"({mono} after {previous}); the log is damaged"
            )
        previous = mono
        yield mono, index, record.seq, record


def merge_logs(paths: Sequence[str | os.PathLike[str]]) -> Iterator[tuple[int, RawRecord]]:
    """Yield ``(log index, record)`` for every record of ``paths``, in arrival order.

    One log is replayed in ``seq`` order, whatever version it is. Several are
    merged by monotonic stamp, then log index, then ``seq``.
    """
    paths = [Path(path) for path in paths]
    if not paths:
        raise ValueError("no logs to merge")
    if len(paths) == 1:
        for record in read_raw_log(paths[0]):
            yield 0, record
        return

    identities = []
    for path in paths:
        identity = log_clock_identity(path)
        if identity is None:
            raise NotComparable(
                f"{path.name} has no clock identity (recorded before ADR 0012), so its "
                "records cannot be ordered against another log's"
            )
        identities.append(identity)
    for path, identity in zip(paths[1:], identities[1:], strict=True):
        reason = identities[0].difference(identity)
        if reason is not None:
            raise NotComparable(f"{paths[0].name} and {path.name}: {reason}")

    streams = [_stamped(index, path) for index, path in enumerate(paths)]
    for _, index, _, record in heapq.merge(*streams):
        yield index, record
