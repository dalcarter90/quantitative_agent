"""Damaged copies of recorded raw logs, for integrity tests.

These are the "gap fixtures" of roadmap Phase 2: each is the committed
fixture with one message removed, duplicated, or swapped with the next. They
are generated deterministically instead of committed, because they differ
from the fixture by one record. Phase 4 uses the same functions.

Every function takes the fixture's records and returns a new list in which
each record keeps its receive time and its order, so the stream is replayed
in the same order apart from the damage. Record numbers stay consecutive from
0, as in any raw log (its writer numbers what it receives), so a normalizer
accepts the list as one log (ADR 0018). ``write`` turns a list into a raw log.

``resnapshot`` is the one repair: a heartbeat replaced by a fresh snapshot of
the stream's book, as resubscribing would deliver. The fixtures have one
stream snapshot each, so without it a damaged fixture never recovers (ADR
0020). Apply it to the undamaged records first, then the damage.
"""

import json
from pathlib import Path

from quantagent.data.adapters import normalize
from quantagent.data.book import kraken_checksum, messages_from_table, replay
from quantagent.data.fixedpoint import to_decimal_str
from quantagent.data.rawlog import RawLogWriter, RawRecord
from quantagent.data.schema import BOOK_EVENTS, INSTRUMENTS, Side, table_from_rows


def write(records: list[RawRecord], path: Path) -> Path:
    with RawLogWriter(path) as writer:
        for record in records:
            writer.write(record.venue, record.channel, record.payload, recv_ts=record.recv_ts,
                         recv_mono_ns=record.recv_mono_ns)  # fmt: skip
    return path


def removed(records: list[RawRecord], seq: int) -> list[RawRecord]:
    """The log without the record ``seq``: a message lost in transit. The
    recorder never saw it, so record numbers after it shift down by one,
    exactly as they would when the log is written."""
    assert any(r.seq == seq for r in records)
    return [
        RawRecord(r.recv_ts, r.seq - (r.seq > seq), r.venue, r.channel, r.payload, r.recv_mono_ns)
        for r in records
        if r.seq != seq
    ]


def original_seq(damaged_seq: int, removed_seq: int) -> int:
    """The record number in the undamaged log of record ``damaged_seq`` of
    ``removed(records, removed_seq)``."""
    return damaged_seq + (damaged_seq >= removed_seq)


def duplicated(records: list[RawRecord], seq: int) -> list[RawRecord]:
    """The record ``seq`` delivered twice in a row. Raw record numbers from the
    copy on shift up by one, exactly as they would when the log is written."""
    out = []
    shift = 0
    for record in records:
        out.append(RawRecord(record.recv_ts, record.seq + shift, record.venue, record.channel,
                             record.payload, record.recv_mono_ns))  # fmt: skip
        if record.seq == seq:
            shift = 1
            out.append(RawRecord(record.recv_ts, record.seq + 1, record.venue, record.channel,
                                 record.payload, record.recv_mono_ns))  # fmt: skip
    assert len(out) == len(records) + 1 and shift == 1
    return out


def swapped(records: list[RawRecord], seq: int) -> list[RawRecord]:
    """The payloads of record ``seq`` and the next stream record exchanged:
    two messages delivered in the wrong order. Receive times stay in order."""
    index = next(i for i, r in enumerate(records) if r.seq == seq)
    following = next(i for i in range(index + 1, len(records)) if records[i].channel == "ws")
    out = list(records)
    a, b = records[index], records[following]
    out[index] = RawRecord(a.recv_ts, a.seq, a.venue, a.channel, b.payload, a.recv_mono_ns)
    out[following] = RawRecord(b.recv_ts, b.seq, b.venue, b.channel, a.payload, b.recv_mono_ns)
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


def is_heartbeat(record: RawRecord) -> bool:
    return record.channel == "ws" and (
        record.payload == b'{"channel":"heartbeat"}'  # Kraken
        or record.payload.startswith(b'{"channel":"heartbeats"')  # Coinbase
    )


def _digits(mantissa: int, exp: int, precision: int) -> str:
    """A stored mantissa as the venue prints it: ``precision`` decimals."""
    divisor = 10 ** (exp - precision)
    assert mantissa % divisor == 0, (mantissa, exp, precision)
    return to_decimal_str(mantissa // divisor, precision)


def resnapshot(records: list[RawRecord], seq: int) -> list[RawRecord]:
    """The stream's heartbeat at record ``seq`` replaced by a book snapshot equal
    to the book rebuilt from every record before it: what the venue sends when
    the stream is resubscribed. Same record number, receive time and channel;
    Coinbase's snapshot keeps the heartbeat's ``sequence_num``, so the
    numbering stays continuous. The book is taken from the reference
    reconstructor, which the venue's own checks have verified on the fixtures
    (replaying the records before ``seq`` must raise nothing)."""
    index = next(i for i, r in enumerate(records) if r.seq == seq)
    target = records[index]
    assert is_heartbeat(target), target
    venue = target.venue
    normalized = normalize(records[:index])
    ((_, _, instrument),) = [k for k in normalized.rows if k[0] == "book_events"]
    versions = normalized.rows[("instruments", venue, instrument)]
    book_rows = table_from_rows(BOOK_EVENTS, normalized.rows[("book_events", venue, instrument)])
    messages = list(messages_from_table(book_rows, table_from_rows(INSTRUMENTS, versions)))
    events = normalized.rows.get(("stream_events", venue, instrument), [])
    book = None
    for _, book in replay(venue, messages, events):  # noqa: B007
        pass
    meta = versions[-1]
    exps = (meta["price_exp"], meta["qty_exp"])
    precisions = (meta["price_precision"], meta["qty_precision"])
    stream = [r for r in records[:index] if r.channel == "ws"]

    def levels(side: Side) -> list[tuple[str, str]]:
        top = book.top(side, len(book.levels(side)))  # best first: bids descending, asks ascending
        return [(_digits(p, exps[0], precisions[0]), _digits(q, exps[1], precisions[1]))
                for p, q in top]  # fmt: skip

    if venue == "kraken":
        last_book = next(json.loads(r.payload) for r in reversed(stream)
                         if r.payload.startswith(b'{"channel":"book"'))  # fmt: skip
        symbol, timestamp = last_book["data"][0]["symbol"], last_book["data"][0]["timestamp"]
        divisors = (10 ** (exps[0] - precisions[0]), 10 ** (exps[1] - precisions[1]))

        def side_json(side: Side) -> str:
            return ",".join(f'{{"price":{p},"qty":{q}}}' for p, q in levels(side))

        payload = (
            f'{{"channel":"book","type":"snapshot","data":[{{"symbol":"{symbol}",'
            f'"bids":[{side_json(Side.BUY)}],"asks":[{side_json(Side.SELL)}],'
            f'"checksum":{kraken_checksum(book, divisors)},"timestamp":"{timestamp}"}}]}}'
        ).encode()
    else:
        heartbeat = json.loads(target.payload)
        last_update = next(json.loads(r.payload) for r in reversed(stream)
                           if r.payload.startswith(b'{"channel":"l2_data"'))  # fmt: skip
        event = last_update["events"][0]
        event_time = event["updates"][0]["event_time"]
        updates = [{"side": name, "event_time": event_time, "price_level": p, "new_quantity": q}
                   for side, name in ((Side.BUY, "bid"), (Side.SELL, "offer"))
                   for p, q in levels(side)]  # fmt: skip
        payload = json.dumps(
            {"channel": "l2_data", "timestamp": heartbeat["timestamp"],
             "sequence_num": heartbeat["sequence_num"],
             "events": [{"type": "snapshot", "product_id": event["product_id"],
                         "updates": updates}]},
            separators=(",", ":"),
        ).encode()  # fmt: skip
    out = list(records)
    out[index] = RawRecord(target.recv_ts, target.seq, target.venue, target.channel, payload,
                           target.recv_mono_ns)  # fmt: skip
    return out
