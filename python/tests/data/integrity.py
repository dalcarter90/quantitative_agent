"""The integrity cases behind ``golden/integrity-points.json`` (ADR 0020).

Each case is a committed fixture damaged the way the Phase 2 gap tests damage
it (a book update removed, duplicated, or swapped with the next stream
message), after a heartbeat further on was replaced by a snapshot of the true
book (``damage.resnapshot``), so the book has something to recover at. The
points are where the reference book, replayed in resume mode, goes stale,
resets and recovers. Phase 4 checks that the C++ book does the same at
exactly the same records, and that between those points its book equals the
reference's after every message.

``stored_events`` false replays with no stored stream events at all, so only
the book's own checks apply (Kraken's checksum, Coinbase's book numbers): that
is how the C++ book's own verification is tested.

To rewrite the committed file, on purpose, from the repository root:

    .venv\\Scripts\\python python\\tests\\data\\integrity.py
"""

import functools
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from damage import duplicated, removed, resnapshot, swapped

from quantagent.data.adapters import normalize
from quantagent.data.book import BookMessage, Step, messages_from_table, trace
from quantagent.data.rawlog import RawRecord, read_raw_log
from quantagent.data.schema import BOOK_EVENTS, INSTRUMENTS, table_from_rows

FIXTURES = Path(__file__).parents[1] / "fixtures"
RAW = FIXTURES / "raw"
POINTS = FIXTURES / "golden" / "integrity-points.json"
INSTRUMENT = "BTC-USD"
FORMAT = 1

DAMAGES = {"resnapshot": resnapshot, "removed": removed, "duplicated": duplicated,
           "swapped": swapped}  # fmt: skip

# Record numbers in the undamaged fixtures. Kraken 265 is book update 250, the
# first of the gap tests' spread whose loss the next update's checksum
# catches; 315 is update 300. Coinbase 630 is update 500. 12492 and 2488 are
# the first stream heartbeats after updates 12000 and 2000, well after each
# fixture's check snapshot.
KRAKEN_RESNAPSHOT, COINBASE_RESNAPSHOT = 12_492, 2_488


@dataclass(frozen=True)
class Case:
    name: str
    venue: str
    damage: tuple[tuple[str, int], ...]  # applied in order to the fixture's records
    stored_events: bool = True


CASES = (
    Case("kraken-removed", "kraken", (("resnapshot", KRAKEN_RESNAPSHOT), ("removed", 265))),
    Case("kraken-removed-own-check", "kraken",
         (("resnapshot", KRAKEN_RESNAPSHOT), ("removed", 265)), stored_events=False),
    Case("kraken-swapped", "kraken", (("resnapshot", KRAKEN_RESNAPSHOT), ("swapped", 265))),
    Case("kraken-duplicated", "kraken", (("resnapshot", KRAKEN_RESNAPSHOT), ("duplicated", 315))),
    Case("coinbase-removed", "coinbase", (("resnapshot", COINBASE_RESNAPSHOT), ("removed", 630))),
    Case("coinbase-duplicated", "coinbase",
         (("resnapshot", COINBASE_RESNAPSHOT), ("duplicated", 630))),
    Case("coinbase-duplicated-own-check", "coinbase",
         (("resnapshot", COINBASE_RESNAPSHOT), ("duplicated", 630)), stored_events=False),
    Case("coinbase-swapped", "coinbase", (("resnapshot", COINBASE_RESNAPSHOT), ("swapped", 630))),
)  # fmt: skip


def fixture(venue: str) -> Path:
    (path,) = sorted((RAW / venue / INSTRUMENT).glob("*.qaraw.zst"))
    return path


@functools.cache
def _undamaged(venue: str, repairs: tuple[tuple[str, int], ...]) -> tuple[RawRecord, ...]:
    records = list(read_raw_log(fixture(venue)))
    for name, seq in repairs:
        records = DAMAGES[name](records, seq)
    return tuple(records)


@functools.cache
def damaged(case: Case) -> tuple[RawRecord, ...]:
    """The case's records (cached: tests share them and must not change them).
    The resnapshot each case starts with is shared by the cases of a venue."""
    repairs = tuple(d for d in case.damage if d[0] == "resnapshot")
    assert case.damage[: len(repairs)] == repairs, "resnapshot first, then the damage"
    records = list(_undamaged(case.venue, repairs))
    for name, seq in case.damage[len(repairs) :]:
        records = DAMAGES[name](records, seq)
    return tuple(records)


def resnapshotted(venue: str) -> tuple[RawRecord, ...]:
    """The undamaged fixture with the integrity cases' resnapshot only."""
    seq = KRAKEN_RESNAPSHOT if venue == "kraken" else COINBASE_RESNAPSHOT
    return _undamaged(venue, (("resnapshot", seq),))


@functools.cache
def normalized(case: Case) -> tuple[tuple[BookMessage, ...], tuple[dict[str, Any], ...]]:
    """The case's book messages and the stored events it replays with."""
    messages, events = from_records(list(damaged(case)), case.venue)
    return tuple(messages), tuple(events) if case.stored_events else ()


def from_records(
    records: list[RawRecord], venue: str
) -> tuple[list[BookMessage], list[dict[str, Any]]]:
    normalized = normalize(records)
    rows = normalized.rows[("book_events", venue, INSTRUMENT)]
    events = normalized.rows.get(("stream_events", venue, INSTRUMENT), [])
    instruments = table_from_rows(INSTRUMENTS, normalized.rows[("instruments", venue, INSTRUMENT)])
    return list(messages_from_table(table_from_rows(BOOK_EVENTS, rows), instruments)), events


def resume_steps(case: Case) -> list[Step]:
    messages, events = normalized(case)
    return [step for step, _ in trace(case.venue, messages, events, mode="resume")]


def is_point(step: Step) -> bool:
    """A step where something other than an ordinary update happens."""
    return step.step != "message" or step.reason is not None or step.recovered


def points(case: Case) -> dict[str, Any]:
    steps = resume_steps(case)
    messages = [s for s in steps if s.step == "message"]
    (log_id,) = {s.log_id for s in steps}
    return {
        "name": case.name,
        "venue": case.venue,
        "fixture": fixture(case.venue).relative_to(FIXTURES).as_posix(),
        "log_id": log_id,
        "damage": [list(d) for d in case.damage],
        "stored_events": case.stored_events,
        "steps": len(steps),
        "messages": len(messages),
        "stale_messages": sum(s.stale for s in messages),
        "points": [
            {
                "seq": s.seq,
                "step": s.step,
                "reason": s.reason,
                "stale": s.stale,
                "recovered": s.recovered,
            }
            for s in steps
            if is_point(s)
        ],  # fmt: skip
    }


def build() -> dict[str, Any]:
    return {"format": FORMAT, "cases": [points(case) for case in CASES]}


def committed() -> dict[str, Any]:
    return json.loads(POINTS.read_text(encoding="utf-8"))


def main() -> int:
    POINTS.write_text(json.dumps(build(), indent=1) + "\n", encoding="utf-8")
    print(f"wrote {POINTS}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
