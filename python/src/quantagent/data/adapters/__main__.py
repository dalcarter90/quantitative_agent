"""Check a raw log, and optionally load it into a store.

    python -m quantagent.data.adapters data/raw/kraken/BTC-USD/<file>.qaraw.zst
    python -m quantagent.data.adapters <file> --store data/normalized

Without ``--store`` nothing is written: the log is normalized and the rows are
discarded, which is enough to see whether every message parses. The exit code
is 1 if any message was rejected, a sequence anomaly was found, or the log
ends in a partial record.
"""

import argparse
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

from quantagent.data.adapters.base import Normalizer, Report, ingest
from quantagent.data.rawlog import read_raw_log
from quantagent.data.schema import BookEventKind
from quantagent.data.store import AppendOnlyStore


def _utc(nanoseconds: int) -> str:
    return datetime.fromtimestamp(nanoseconds // 1_000_000_000, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def inspect(path: Path) -> tuple[Report, list[str]]:
    """Normalize ``path`` without storing anything."""
    reader = read_raw_log(path)
    normalizer = Normalizer()
    book_messages: Counter = Counter()
    first = last = None
    for record in reader:
        first = record.recv_ts if first is None else first
        last = record.recv_ts
        normalizer.feed(record)
        for (dataset_name, _, _), rows in normalizer.take_rows().items():
            if dataset_name == "book_events":
                book_messages[BookEventKind(rows[0]["kind"])] += 1
    report = normalizer.report
    report.truncated = reader.truncated
    lines = []
    if first is not None:
        lines.append(f"recorded: {(last - first) / 1e9:.0f} s, {_utc(first)} to {_utc(last)}")
    lines.append(
        f"book messages: {book_messages[BookEventKind.SNAPSHOT]} stream snapshot, "
        f"{book_messages[BookEventKind.DELTA]} update, "
        f"{book_messages[BookEventKind.CHECK_SNAPSHOT]} check snapshot"
    )
    return report, lines


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check a raw log; optionally load it.")
    parser.add_argument("path", type=Path)
    parser.add_argument("--store", type=Path, help="normalized store to append the rows to")
    args = parser.parse_args(argv)

    print(args.path)
    if args.store is None:
        report, extra = inspect(args.path)
    else:
        report, extra = ingest(args.path, AppendOnlyStore(args.store)), []
    print(report.summary())
    for line in extra:
        print(line)
    for reject in report.rejects[:20]:
        where = f"seq {reject.seq} ({reject.channel})"
        print(f"  rejected {where}: {reject.reason}: {reject.detail[:120]}")
    for anomaly in report.sequence_anomalies[:20]:
        print(
            f"  sequence {anomaly.kind} on {anomaly.connection} at seq {anomaly.seq}: "
            f"expected {anomaly.expected}, received {anomaly.received}"
        )
    clean = not (report.rejects or report.sequence_anomalies or report.truncated)
    return 0 if clean else 1


if __name__ == "__main__":
    sys.exit(main())
