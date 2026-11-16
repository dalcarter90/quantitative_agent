"""Append-only Parquet store for normalized market data (ADR 0006, ADR 0017).

Layout, hive-style so DuckDB and pyarrow can discover the partitions::

    <root>/<dataset>/venue=<venue>/instrument=<instrument>/date=<YYYY-MM-DD>/
        part-<knowledge_ts, 20 digits>-<number, 6 digits>.parquet
    <root>/_batches/batch-<knowledge_ts, 20 digits>.json

``date`` is the UTC date of ``recv_ts``: the day we received the row.

A write is a **batch**. Every table appended to one batch gets the same
``knowledge_ts``, read from the clock when the batch is opened and strictly
greater than any issued before. The batch's files are written first; its
**commit marker** (``_batches/batch-*.json``, naming every file) is published
last, in one step. A batch without a marker is not part of the store: readers
ignore its files, whether the load is still running, failed, or died.

Existing files are never opened for writing, renamed or removed, and there is
no update or delete operation: a correction is another batch. The only files
ever removed are those of a batch that failed before it was committed, which
no reader could have seen.

The store assumes a single writer process per root.

This is the write side only. Reading goes through PITSession.
"""

import json
import os
import re
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from quantagent.data.fixedpoint import INT64_MAX
from quantagent.data.schema import KNOWLEDGE_TS, Dataset, validate_table

_NS_PER_DAY = 86_400 * 1_000_000_000
_EPOCH = date(1970, 1, 1)

# The layout described above. Written into every commit marker.
STORE_LAYOUT = 2
BATCH_DIRECTORY = "_batches"

# Venue and instrument names become directory names, so they are restricted to
# characters that are safe on every filesystem and cannot form a path.
_NAME = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9._-]{0,62}[A-Za-z0-9])?")
_PART = re.compile(r"part-([0-9]{20})-([0-9]{6})\.parquet")
_MARKER = re.compile(r"batch-([0-9]{20})\.json")
# Data files as they were named before ADR 0017: no commit markers, and
# partitioned by the date of event_ts.
_LEGACY_PART = re.compile(r"part-[0-9]{20}\.parquet")

REBUILD = (
    "rebuild it from the raw logs into a new directory "
    "(python -m quantagent.data.adapters <raw log> --store <new store>; "
    "docs/data-core.md, section 3)"
)


class StoreVersionError(ValueError):
    """The store was not written in the layout and schema this code reads
    (ADR 0015, ADR 0017). It has to be rebuilt from the raw logs."""


@dataclass(frozen=True)
class AppendResult:
    knowledge_ts: int
    rows: int
    files: tuple[Path, ...]


def _check_name(kind: str, name: str) -> None:
    if not isinstance(name, str) or _NAME.fullmatch(name) is None:
        raise ValueError(
            f"invalid {kind} name {name!r}: use 1-64 of A-Z a-z 0-9 . _ - "
            "starting and ending with a letter or digit"
        )


def _legacy(path: Path) -> StoreVersionError:
    return StoreVersionError(
        f"store file {path.name} was written before commit markers (ADR 0017), so this "
        f"store cannot be read or added to; {REBUILD}"
    )


class Batch:
    """One load into the store. Get one from ``AppendOnlyStore.batch()``.

    Every table appended shares ``knowledge_ts``. Nothing is readable until
    ``commit``; used as a context manager, the batch is committed when the
    block ends normally and abandoned if it raises.
    """

    def __init__(self, store: "AppendOnlyStore", knowledge_ts: int):
        self._store = store
        self._knowledge_ts = knowledge_ts
        self._files: list[Path] = []
        self._rows: Counter = Counter()
        self._numbers: Counter = Counter()  # files written so far, per directory
        self._open = True

    @property
    def knowledge_ts(self) -> int:
        return self._knowledge_ts

    def append(self, dataset: Dataset, venue: str, instrument: str, rows: pa.Table) -> None:
        """Write ``rows`` as part of this batch.

        ``rows`` must match ``dataset.schema`` exactly (build it with
        ``schema.table_from_rows``). A table that is refused is not written
        and does not end the batch.
        """
        self._require_open()
        _validate(dataset, venue, instrument, rows)
        stored = _with_knowledge_ts(dataset, rows, self._knowledge_ts)
        partition = self._store._root / dataset.name / f"venue={venue}" / f"instrument={instrument}"
        days = pc.divide(stored.column("recv_ts"), _NS_PER_DAY)
        # The marker directory exists before any data file does. A reader tells
        # a store of this layout whose first load never committed (data files,
        # no markers yet) from one written before commit markers by it.
        (self._store._root / BATCH_DIRECTORY).mkdir(parents=True, exist_ok=True)
        written: list[Path] = []
        numbers = Counter()
        try:
            for day in sorted(pc.unique(days).to_pylist()):
                directory = partition / f"date={(_EPOCH + timedelta(days=day)).isoformat()}"
                directory.mkdir(parents=True, exist_ok=True)
                number = self._numbers[directory] + numbers[directory]
                path = directory / f"part-{self._knowledge_ts:020d}-{number:06d}.parquet"
                # "x": fail rather than write over a file that is already there.
                with open(path, "xb") as handle:
                    written.append(path)
                    pq.write_table(stored.filter(pc.equal(days, day)), handle, compression="zstd")
                    handle.flush()
                    os.fsync(handle.fileno())
                numbers[directory] += 1
        except BaseException:
            # All of a table or none of it: a later commit must not hold part of one.
            for path in written:
                path.unlink(missing_ok=True)
            raise
        self._files.extend(written)
        self._numbers.update(numbers)
        self._rows[dataset.name] += rows.num_rows

    def commit(self) -> AppendResult:
        """Publish the commit marker: from here on the batch is readable.

        A batch with no rows publishes nothing.
        """
        self._require_open()
        try:
            if self._files:
                self._publish_marker()
        except BaseException:
            self.abandon()
            raise
        self._finish()
        return AppendResult(self._knowledge_ts, sum(self._rows.values()), tuple(self._files))

    def abandon(self) -> None:
        """Give the batch up. Its files, which no reader could see, are removed."""
        if not self._open:
            return
        for path in self._files:
            path.unlink(missing_ok=True)
        self._files = []
        self._finish()

    def _publish_marker(self) -> None:
        root = self._store._root
        directory = root / BATCH_DIRECTORY
        directory.mkdir(parents=True, exist_ok=True)
        # Batches must become readable in knowledge-time order: a version a
        # session was given must never gain a batch afterwards.
        later = [k for k in _committed(directory) if k > self._knowledge_ts]
        if later:
            raise RuntimeError(
                f"cannot commit batch {self._knowledge_ts}: a later batch ({max(later)}) is "
                "already committed. Is another writer using this store?"
            )
        content = {
            "layout": STORE_LAYOUT,
            "knowledge_ts": self._knowledge_ts,
            "files": [path.relative_to(root).as_posix() for path in self._files],
            "rows": dict(sorted(self._rows.items())),
        }
        final = directory / f"batch-{self._knowledge_ts:020d}.json"
        temporary = directory / f".{final.name}.tmp"
        try:
            with open(temporary, "xb") as handle:
                handle.write(json.dumps(content, indent=1, sort_keys=True).encode("utf-8"))
                handle.flush()
                os.fsync(handle.fileno())
            # A hard link appears in one step and fails if the target exists,
            # on every platform, so a marker is never partial and never replaced.
            os.link(temporary, final)
        finally:
            temporary.unlink(missing_ok=True)

    def _require_open(self) -> None:
        if not self._open:
            raise RuntimeError("this batch is finished (committed or abandoned)")

    def _finish(self) -> None:
        self._open = False
        self._store._batch_finished(self)

    def __enter__(self) -> "Batch":
        return self

    def __exit__(self, exc_type: object, *exc_info: object) -> None:
        if not self._open:
            return
        if exc_type is None:
            self.commit()
        else:
            self.abandon()


class AppendOnlyStore:
    """Writer for the normalized store. Its operations are ``batch`` and
    ``append``, which is a batch of one table."""

    def __init__(self, root: str | os.PathLike[str], *, clock: Callable[[], int] = time.time_ns):
        self._root = Path(root)
        self._clock = clock
        self._last_knowledge_ts = self._recover_last_knowledge_ts()
        self._open_batch: Batch | None = None

    def batch(self) -> Batch:
        """Open a batch: one knowledge time for everything appended to it,
        readable only once it is committed (ADR 0017)."""
        if self._open_batch is not None:
            raise RuntimeError("a batch is already open on this store; commit or abandon it first")
        knowledge_ts = self._next_knowledge_ts()
        self._last_knowledge_ts = knowledge_ts
        self._open_batch = Batch(self, knowledge_ts)
        return self._open_batch

    def append(self, dataset: Dataset, venue: str, instrument: str, rows: pa.Table) -> AppendResult:
        """Append ``rows`` as a batch of its own and return the knowledge time it was given.

        ``rows`` must match ``dataset.schema`` exactly (build it with
        ``schema.table_from_rows``). A table that is refused consumes no
        knowledge time.
        """
        if self._open_batch is not None:
            raise RuntimeError("a batch is already open on this store; append to that batch")
        _validate(dataset, venue, instrument, rows)
        with self.batch() as batch:
            batch.append(dataset, venue, instrument, rows)
            return batch.commit()

    def _batch_finished(self, batch: Batch) -> None:
        if self._open_batch is batch:
            self._open_batch = None

    def _next_knowledge_ts(self) -> int:
        now = self._clock()
        if type(now) is not int:
            raise TypeError(f"clock must return int nanoseconds, got {type(now).__name__}")
        if not 0 <= now <= INT64_MAX:
            raise ValueError(f"clock returned {now}, outside the int64 nanosecond range")
        knowledge_ts = max(now, self._last_knowledge_ts + 1)
        if knowledge_ts > INT64_MAX:
            raise ValueError("knowledge_ts would overflow int64")
        return knowledge_ts

    def _recover_last_knowledge_ts(self) -> int:
        # Knowledge times start at 1, so version 0 always means "before any batch".
        last = 0
        if self._root.is_dir():
            # Files of batches that were never committed count too: their
            # knowledge time must not be issued again.
            for path in self._root.rglob("*.parquet"):
                match = _PART.fullmatch(path.name)
                if match is None:
                    if _LEGACY_PART.fullmatch(path.name):
                        raise _legacy(path)
                    raise ValueError(f"unexpected file in store: {path}")
                last = max(last, int(match.group(1)))
            last = max([last, *_committed(self._root / BATCH_DIRECTORY)])
        return last


def _committed(directory: Path) -> list[int]:
    """Knowledge times of the committed batches, from the marker names."""
    if not directory.is_dir():
        return []
    found = []
    for path in directory.glob("batch-*.json"):
        match = _MARKER.fullmatch(path.name)
        if match is None:
            raise ValueError(f"unexpected file in store: {path}")
        found.append(int(match.group(1)))
    return found


def _validate(dataset: Dataset, venue: str, instrument: str, rows: pa.Table) -> None:
    if not isinstance(dataset, Dataset):
        raise TypeError(f"dataset must be a Dataset, got {type(dataset).__name__}")
    _check_name("venue", venue)
    _check_name("instrument", instrument)
    validate_table(dataset, rows)
    if rows.num_rows == 0:
        raise ValueError("cannot append an empty batch")


def _with_knowledge_ts(dataset: Dataset, rows: pa.Table, knowledge_ts: int) -> pa.Table:
    stamp = pa.array([knowledge_ts] * rows.num_rows, type=pa.int64())
    stored_schema = dataset.stored_schema
    columns = [stamp if name == KNOWLEDGE_TS else rows.column(name) for name in stored_schema.names]
    return pa.Table.from_arrays(columns, schema=stored_schema)
