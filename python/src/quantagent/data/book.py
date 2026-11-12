"""The reference order book reconstructor (roadmap Phase 2).

A simple, deliberately unoptimized L2 book that serves as the oracle for the
C++ order book (Phase 4). It rebuilds each venue's book from the stored book
events, enforces that venue's integrity rule, and raises on any failure: it
never skips, repairs or continues past one.

    python -m quantagent.data.book verify --store data/normalized \
        --venue kraken --instrument BTC-USD
    python -m quantagent.data.book golden --store ... --venue ... --instrument ... --out FILE

Venue rules, as the venues document them:

- **Kraken** (https://docs.kraken.com/api/docs/websocket-v2/book and
  https://docs.kraken.com/api/docs/guides/spot-ws-book-v2): the book is the
  subscribed depth per side; after every message the client drops levels
  beyond it, because the venue sends no removal for them. Every snapshot and
  update carries a CRC32 of the ten best asks (ascending) then the ten best
  bids (descending), each level as its price digits then quantity digits with
  the decimal point and leading zeros removed. Kraken prints prices and
  quantities at full precision, so those digits are the stored mantissas.
  There are no sequence numbers.
- **Coinbase** (https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-channels):
  every message on a connection carries ``sequence_num``, one higher than the
  previous message on that connection, across all channels. A jump means a
  message was lost. Heartbeats and trades take numbers too, so book messages
  alone show only that the numbers must increase; gaps are known from the
  adapter's stored stream events (ADR 0010).

Both venues' book messages give absolute quantities per level; a quantity of
zero removes the level. Levels within a message are applied in the order the
venue listed them (``level_index``).

The reconstructor verifies Kraken's checksum itself, independently of the
adapter, and also raises on the integrity events the adapter stored. The two
are deliberate redundancy; tests show they agree.
"""

import argparse
import sys
import zlib
from bisect import bisect_left, insort
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from quantagent.data.schema import BookEventKind, Side, StreamEventKind
from quantagent.data.session import PITSession

GOLDEN_LEVELS = 20
KRAKEN_CHECKSUM_LEVELS = 10
_STREAM = "ws"
_INT64_MAX = 2**63 - 1

Level = tuple[int, int, int]  # (side, price mantissa, quantity mantissa)


# --- errors -------------------------------------------------------------------------


class BookIntegrityError(Exception):
    """The stream cannot be trusted from this message on."""

    def __init__(self, recv_ts: int, seq: int, message: str):
        super().__init__(f"{message} (raw record recv_ts {recv_ts}, seq {seq})")
        self.recv_ts = recv_ts
        self.seq = seq


class DeltaBeforeSnapshot(BookIntegrityError):
    """An update arrived when there was no snapshot to apply it to."""


class ChecksumMismatch(BookIntegrityError):
    def __init__(self, recv_ts: int, seq: int, expected: int, computed: int, source: str):
        super().__init__(
            recv_ts,
            seq,
            f"book checksum mismatch: the venue's is {expected}, the rebuilt book's is "
            f"{computed} ({source})",
        )
        self.expected, self.computed, self.source = expected, computed, source


class SequenceBreak(BookIntegrityError):
    def __init__(
        self, recv_ts: int, seq: int, kind: str, expected: int, received: int, source: str
    ):
        super().__init__(
            recv_ts,
            seq,
            f"sequence {kind}: expected {expected}, received {received} ({source})",
        )
        self.kind, self.expected, self.received, self.source = kind, expected, received, source


class StreamFault(BookIntegrityError):
    """A message on the stream could not be normalized, so the stream has a hole."""


# --- the book ---------------------------------------------------------------------------


def _side(value: Any) -> Side:
    if value == Side.BUY:
        return Side.BUY
    if value == Side.SELL:
        return Side.SELL
    raise ValueError(f"a book level must be a bid or an ask, got side {value!r}")


def _integer(value: Any, what: str) -> int:
    if type(value) is not int:
        raise TypeError(f"{what} must be an int mantissa, got {type(value).__name__} {value!r}")
    return value


class ReferenceBook:
    """One instrument's price levels. Plain dictionaries, plus a sorted list
    of prices per side so the best levels are easy to read."""

    def __init__(self) -> None:
        self._qty: dict[Side, dict[int, int]] = {Side.BUY: {}, Side.SELL: {}}
        self._prices: dict[Side, list[int]] = {Side.BUY: [], Side.SELL: []}  # ascending

    def replace(self, levels: Iterable[Level]) -> None:
        """Make the book exactly these levels (a snapshot)."""
        self._qty = {Side.BUY: {}, Side.SELL: {}}
        self.apply(levels)
        self._prices = {side: sorted(self._qty[side]) for side in self._qty}

    def apply(self, levels: Iterable[Level]) -> None:
        """Apply levels in order. Quantity 0 removes the level, if present."""
        for side_value, price, qty in levels:
            side = _side(side_value)
            _integer(price, "price")
            if _integer(qty, "quantity") < 0:
                raise ValueError(f"negative quantity {qty} at price {price}")
            book, prices = self._qty[side], self._prices[side]
            if qty == 0:
                if book.pop(price, None) is not None:
                    del prices[bisect_left(prices, price)]
            else:
                if price not in book:
                    insort(prices, price)
                book[price] = qty

    def truncate(self, depth: int) -> None:
        """Keep the ``depth`` best levels per side."""
        for side in (Side.BUY, Side.SELL):
            prices = self._prices[side]
            if len(prices) <= depth:
                continue
            if side == Side.BUY:  # best bids are the highest: drop from the low end
                dropped, self._prices[side] = prices[: len(prices) - depth], prices[-depth:]
            else:
                dropped, self._prices[side] = prices[depth:], prices[:depth]
            for price in dropped:
                del self._qty[side][price]

    def levels(self, side: Side) -> dict[int, int]:
        """A copy of one side, price to quantity."""
        return dict(self._qty[_side(side)])

    def top(self, side: Side, count: int) -> list[tuple[int, int]]:
        """The ``count`` best levels of one side, best first."""
        side = _side(side)
        prices = self._prices[side]
        best = reversed(prices[-count:]) if side == Side.BUY else prices[:count]
        return [(price, self._qty[side][price]) for price in best]

    def best_bid(self) -> tuple[int, int] | None:
        top = self.top(Side.BUY, 1)
        return top[0] if top else None

    def best_ask(self) -> tuple[int, int] | None:
        top = self.top(Side.SELL, 1)
        return top[0] if top else None

    def is_crossed(self) -> bool:
        """True if the best bid is at or above the best ask."""
        bid, ask = self.best_bid(), self.best_ask()
        return bid is not None and ask is not None and bid[0] >= ask[0]


def kraken_checksum(book: ReferenceBook) -> int:
    """Kraken's book checksum; see the module docstring for the recipe."""
    asks = book.top(Side.SELL, KRAKEN_CHECKSUM_LEVELS)
    bids = book.top(Side.BUY, KRAKEN_CHECKSUM_LEVELS)
    return zlib.crc32("".join(f"{price}{qty}" for price, qty in (*asks, *bids)).encode())


# --- messages ---------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class BookMessage:
    """The book levels of one raw record, in the venue's order."""

    recv_ts: int
    seq: int
    event_ts: int  # the latest venue time among its levels
    kind: BookEventKind
    venue_seq_last: int | None
    checksum: int | None
    depth: int | None
    levels: tuple[Level, ...]

    @property
    def key(self) -> tuple[int, int]:
        return (self.recv_ts, self.seq)


_HEADER = ("kind", "venue_seq_last", "checksum", "depth")


def messages_from_table(table: pa.Table) -> Iterator[BookMessage]:
    """Group ``book_events`` rows (from ``PITSession.book_events``) into
    messages, ordered by (recv_ts, seq), each with its levels in order."""
    names = ("recv_ts", "seq", "event_ts", *_HEADER, "level_index", "side", "price", "qty")
    columns = [table.column(name).to_pylist() for name in names]
    grouped: dict[tuple[int, int], list[tuple]] = {}
    for values in zip(*columns, strict=True):
        grouped.setdefault((values[0], values[1]), []).append(values)

    for (recv_ts, seq), rows in sorted(grouped.items()):
        rows.sort(key=lambda r: r[7])
        if [r[7] for r in rows] != list(range(len(rows))):
            raise ValueError(f"message (recv_ts {recv_ts}, seq {seq}): level_index is not 0..n-1")
        header = {r[3:7] for r in rows}
        if len(header) != 1:
            raise ValueError(
                f"message (recv_ts {recv_ts}, seq {seq}): levels disagree on {_HEADER}"
            )
        kind, venue_seq_last, checksum, depth = header.pop()
        yield BookMessage(
            recv_ts=recv_ts,
            seq=seq,
            event_ts=max(r[2] for r in rows),
            kind=BookEventKind(kind),
            venue_seq_last=venue_seq_last,
            checksum=checksum,
            depth=depth,
            levels=tuple((Side(r[8]), r[9], r[10]) for r in rows),
        )


# --- replay -----------------------------------------------------------------------------------

_SEQUENCE_KINDS = {
    StreamEventKind.SEQUENCE_GAP: "gap",
    StreamEventKind.SEQUENCE_REPEAT: "repeat",
    StreamEventKind.SEQUENCE_OUT_OF_ORDER: "out_of_order",
}
_RESETS = {StreamEventKind.SESSION_START, StreamEventKind.CONNECT, StreamEventKind.DISCONNECT}
_VENUES = ("kraken", "coinbase")


class _Replayer:
    """Applies stream messages and stored events in (recv_ts, seq) order."""

    def __init__(self, venue: str, events: Iterable[dict[str, Any]], verify: bool):
        if venue not in _VENUES:
            raise ValueError(f"no book rule for venue {venue!r}")
        self.venue, self.verify = venue, verify
        relevant = [e for e in events if e["connection"] in (_STREAM, None)]
        self._events = sorted(relevant, key=lambda e: (e["recv_ts"], e["seq"], e["event_index"]))
        self._next_event = 0
        self.book: ReferenceBook | None = None
        self._last_venue_seq: int | None = None

    def _events_until(self, key: tuple[int, int]) -> None:
        """Handle every stored event at or before ``key``. Events at a message's
        own record come first: a reconnect or gap there concerns that message."""
        while self._next_event < len(self._events):
            event = self._events[self._next_event]
            if (event["recv_ts"], event["seq"]) > key:
                return
            self._next_event += 1
            self._event(event)

    def _event(self, event: dict[str, Any]) -> None:
        kind = StreamEventKind(event["kind"])
        recv_ts, seq = event["recv_ts"], event["seq"]
        if kind in _RESETS:
            # A new session or connection starts a new stream: a snapshot must come first.
            self.book, self._last_venue_seq = None, None
        elif kind in _SEQUENCE_KINDS:
            raise SequenceBreak(
                recv_ts, seq, _SEQUENCE_KINDS[kind], event["expected"], event["observed"],
                "stream_event",
            )  # fmt: skip
        elif kind == StreamEventKind.CHECKSUM_MISMATCH:
            raise ChecksumMismatch(
                recv_ts, seq, event["expected"], event["observed"], "stream_event"
            )
        elif kind == StreamEventKind.MESSAGE_REJECTED:
            raise StreamFault(recv_ts, seq, f"a stream message was rejected: {event['detail']}")

    def message(self, message: BookMessage) -> ReferenceBook:
        self._events_until(message.key)
        if message.kind == BookEventKind.SNAPSHOT:
            self.book = ReferenceBook()
            self.book.replace(message.levels)
            self._last_venue_seq = None
        elif message.kind == BookEventKind.DELTA:
            if self.book is None:
                raise DeltaBeforeSnapshot(
                    message.recv_ts, message.seq, "a book update arrived before any snapshot"
                )
            self.book.apply(message.levels)
        else:
            raise ValueError("check snapshots are not part of the stream")
        if message.depth is not None:
            self.book.truncate(message.depth)
        if self.verify:
            self._verify(message)
        return self.book

    def _verify(self, message: BookMessage) -> None:
        if self.venue == "kraken":
            if message.checksum is None:
                raise ValueError(f"Kraken message seq {message.seq} has no checksum to verify")
            computed = kraken_checksum(self.book)
            if computed != message.checksum:
                raise ChecksumMismatch(
                    message.recv_ts, message.seq, message.checksum, computed, "computed"
                )
        else:
            number = message.venue_seq_last
            if number is None:
                raise ValueError(f"Coinbase message seq {message.seq} has no sequence number")
            last = self._last_venue_seq
            if last is not None and number <= last:
                kind = "repeat" if number == last else "out_of_order"
                raise SequenceBreak(
                    message.recv_ts, message.seq, kind, last + 1, number, "computed"
                )
            self._last_venue_seq = number

    def finish(self) -> None:
        self._events_until((_INT64_MAX, _INT64_MAX))


def _stream(messages: Iterable[BookMessage]) -> list[BookMessage]:
    return sorted(
        (m for m in messages if m.kind != BookEventKind.CHECK_SNAPSHOT), key=lambda m: m.key
    )


def replay(
    venue: str,
    messages: Iterable[BookMessage],
    events: Iterable[dict[str, Any]] = (),
    *,
    verify: bool = True,
) -> Iterator[tuple[BookMessage, ReferenceBook]]:
    """Rebuild the book message by message; yield it after each stream message.

    ``events`` are the instrument's ``stream_events`` rows. A stored sequence
    break, checksum mismatch or rejected message on the stream raises; a new
    session or connection means the next message must be a snapshot. With
    ``verify`` the venue's rule is also checked from the book itself.
    Check snapshots are skipped: they are not part of the stream.

    The yielded book is the live object: it changes as replay continues.
    """
    replayer = _Replayer(venue, events, verify)
    for message in _stream(messages):
        yield message, replayer.message(message)
    replayer.finish()


@dataclass(frozen=True)
class CheckResult:
    """One independent snapshot compared with the book rebuilt from the stream."""

    snapshot: BookMessage
    stream_messages_applied: int  # stream messages with venue time at or before the snapshot's
    levels_compared: int
    # (side, price, rebuilt quantity, snapshot quantity); None where a level is missing
    differences: list[tuple[int, int, int | None, int | None]] = field(default_factory=list)

    @property
    def matched(self) -> bool:
        return self.stream_messages_applied > 0 and not self.differences


def _compare(snapshot: BookMessage, book: ReferenceBook | None, applied: int) -> CheckResult:
    expected: dict[int, dict[int, int]] = {Side.BUY: {}, Side.SELL: {}}
    for side, price, qty in snapshot.levels:
        expected[side][price] = qty
    differences = []
    for side in (Side.BUY, Side.SELL):
        if book is None:
            rebuilt = {}
        elif snapshot.depth is not None:
            rebuilt = dict(book.top(side, snapshot.depth))
        else:
            rebuilt = book.levels(side)
        for price in sorted(set(rebuilt) | set(expected[side])):
            if rebuilt.get(price) != expected[side].get(price):
                differences.append((side, price, rebuilt.get(price), expected[side].get(price)))
    return CheckResult(snapshot, applied, len(snapshot.levels), differences)


def verify_check_snapshots(
    venue: str, messages: Sequence[BookMessage], events: Iterable[dict[str, Any]] = ()
) -> list[CheckResult]:
    """Compare every check snapshot with the book rebuilt from the stream up to
    and including the last message whose venue time is at or before the
    snapshot's. Raises on any integrity failure, like ``replay``."""
    checks = sorted(
        (m for m in messages if m.kind == BookEventKind.CHECK_SNAPSHOT),
        key=lambda m: (m.event_ts, m.recv_ts, m.seq),
    )
    replayer = _Replayer(venue, events, verify=True)
    results, pending, applied = [], list(checks), 0
    for message in _stream(messages):
        while pending and message.event_ts > pending[0].event_ts:
            results.append(_compare(pending.pop(0), replayer.book, applied))
        replayer.message(message)
        applied += 1
    replayer.finish()
    results.extend(_compare(check, replayer.book, applied) for check in pending)
    return results


# --- golden files ------------------------------------------------------------------------


def golden_schema(levels: int = GOLDEN_LEVELS) -> pa.Schema:
    fields = [pa.field("recv_ts", pa.int64(), nullable=False),
              pa.field("seq", pa.uint64(), nullable=False)]  # fmt: skip
    for rank in range(levels):
        for side in ("bid", "ask"):
            fields.append(pa.field(f"{side}_price_{rank:02d}", pa.int64()))
            fields.append(pa.field(f"{side}_qty_{rank:02d}", pa.int64()))
    return pa.schema(fields)


def golden_table(
    venue: str,
    messages: Iterable[BookMessage],
    events: Iterable[dict[str, Any]] = (),
    *,
    levels: int = GOLDEN_LEVELS,
) -> pa.Table:
    """The book's best ``levels`` per side after every stream message.

    One row per stream message: ``recv_ts`` and ``seq`` name the raw record,
    then for each rank, best first, the bid's price and quantity and the ask's
    price and quantity (mantissas; null where the book has fewer levels).
    """
    schema = golden_schema(levels)
    columns: dict[str, list] = {name: [] for name in schema.names}
    for message, book in replay(venue, messages, events):
        columns["recv_ts"].append(message.recv_ts)
        columns["seq"].append(message.seq)
        for side, name in ((Side.BUY, "bid"), (Side.SELL, "ask")):
            top = book.top(side, levels)
            for rank in range(levels):
                price, qty = top[rank] if rank < len(top) else (None, None)
                columns[f"{name}_price_{rank:02d}"].append(price)
                columns[f"{name}_qty_{rank:02d}"].append(qty)
    return pa.table(columns, schema=schema)


# --- command line ---------------------------------------------------------------------------------


def _load(args: argparse.Namespace) -> tuple[list[BookMessage], list[dict[str, Any]]]:
    as_of = _INT64_MAX if args.as_of is None else args.as_of
    with PITSession(args.store, as_of=as_of) as session:
        table = session.book_events(args.venue, args.instrument, 0, _INT64_MAX)
        messages = list(messages_from_table(table))
        events = session.stream_events(args.venue, args.instrument, 0, _INT64_MAX).to_pylist()
    return messages, events


def _verify_command(args: argparse.Namespace) -> int:
    messages, events = _load(args)
    stream = len(_stream(messages))
    print(f"{args.venue}/{args.instrument}: {stream} stream messages, {len(events)} stream events")
    try:
        results = verify_check_snapshots(args.venue, messages, events)
        crossed = sum(book.is_crossed() for _, book in replay(args.venue, messages, events))
    except BookIntegrityError as error:
        print(f"FAILED: {error}")
        return 1
    matched = sum(result.matched for result in results)
    print(f"{matched} of {len(results)} check snapshots match the rebuilt book")
    for result in results:
        if not result.matched:
            print(
                f"  snapshot seq {result.snapshot.seq}: {len(result.differences)} of "
                f"{result.levels_compared} levels differ"
            )
    print("never crossed" if crossed == 0 else f"crossed after {crossed} messages")
    ok = matched == len(results) and crossed == 0
    print("OK" if ok else "FAILED")
    return 0 if ok else 1


def _golden_command(args: argparse.Namespace) -> int:
    messages, events = _load(args)
    table = golden_table(args.venue, messages, events)
    pq.write_table(table, args.out, compression="zstd", compression_level=19)
    print(f"wrote {args.out}: {table.num_rows} rows")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m quantagent.data.book",
        description="Rebuild an instrument's order book from the store and check it.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    for name, purpose in (
        ("verify", "replay with every integrity rule and compare every check snapshot"),
        ("golden", "write the top-of-book after every message (Phase 4 golden file)"),
    ):
        command = commands.add_parser(name, help=purpose)
        command.add_argument("--store", required=True)
        command.add_argument("--venue", required=True, choices=_VENUES)
        command.add_argument("--instrument", required=True)
        command.add_argument("--as-of", type=int, help="knowledge time, ns (default: everything)")
        if name == "golden":
            command.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    if args.command == "verify":
        return _verify_command(args)
    return _golden_command(args)


if __name__ == "__main__":
    sys.exit(main())
