"""Damaged copies of recorded raw logs, for integrity tests.

These are the "gap fixtures" of roadmap Phase 2: each is the committed
fixture with one message removed, duplicated, or swapped with the next. They
are generated deterministically instead of committed, because they differ
from the fixture by one record. Phase 4 uses the same functions.

Every function takes the fixture's records and returns a new list in which
each record keeps its receive time, so the stream is replayed in the same
order apart from the damage. ``write`` turns a list into a raw log.
"""

import json
from pathlib import Path

from quantagent.data.rawlog import RawLogWriter, RawRecord


def write(records: list[RawRecord], path: Path) -> Path:
    with RawLogWriter(path) as writer:
        for record in records:
            writer.write(record.venue, record.channel, record.payload, recv_ts=record.recv_ts)
    return path


def removed(records: list[RawRecord], seq: int) -> list[RawRecord]:
    """The log without the record ``seq``: a message lost in transit."""
    assert any(r.seq == seq for r in records)
    return [r for r in records if r.seq != seq]


def duplicated(records: list[RawRecord], seq: int) -> list[RawRecord]:
    """The record ``seq`` delivered twice in a row. Raw record numbers from the
    copy on shift up by one, exactly as they would when the log is written."""
    out = []
    shift = 0
    for record in records:
        out.append(RawRecord(record.recv_ts, record.seq + shift, record.venue, record.channel,
                             record.payload))  # fmt: skip
        if record.seq == seq:
            shift = 1
            out.append(RawRecord(record.recv_ts, record.seq + 1, record.venue, record.channel,
                                 record.payload))  # fmt: skip
    assert len(out) == len(records) + 1 and shift == 1
    return out


def swapped(records: list[RawRecord], seq: int) -> list[RawRecord]:
    """The payloads of record ``seq`` and the next stream record exchanged:
    two messages delivered in the wrong order. Receive times stay in order."""
    index = next(i for i, r in enumerate(records) if r.seq == seq)
    following = next(i for i in range(index + 1, len(records)) if records[i].channel == "ws")
    out = list(records)
    a, b = records[index], records[following]
    out[index] = RawRecord(a.recv_ts, a.seq, a.venue, a.channel, b.payload)
    out[following] = RawRecord(b.recv_ts, b.seq, b.venue, b.channel, a.payload)
    return out


def book_updates(records: list[RawRecord]) -> list[RawRecord]:
    """The stream's book update records, in order."""
    marker = (b'"channel":"book"', b'"channel":"l2_data"')
    return [
        r
        for r in records
        if r.channel == "ws"
        and any(m in r.payload[:60] for m in marker)
        and b'"update"' in r.payload[:200]
    ]


def sequence_number(record: RawRecord) -> int:
    """Coinbase's per-connection message number."""
    return json.loads(record.payload)["sequence_num"]
