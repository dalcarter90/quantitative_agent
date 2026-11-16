"""Append-only Parquet store for normalized market data (ADR 0006, ADR 0017, ADR 0018).

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

A **load** is one ingest of one raw log (ADR 0018): one or more batches, all
of whose rows name that log. Its ``load_id`` is the knowledge time of its
first batch, and its last batch says so in its marker. A reader reads each
log from its latest complete load, so loading a log again replaces what an
earlier load said about it, including rows the new load no longer produces,
while the earlier load stays on disk. Batches written outside a load
(``append``, ``batch()``) are read record by record until their log has a
complete load.

Existing files are never opened for writing, renamed or removed, and there is
no update or delete operation: a correction is another batch, or another
load. The only files ever removed are those of a batch that failed before it
was committed, which no reader could have seen.

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
from quantagent.data.schema import KNOWLEDGE_TS, LOG_ID_PATTERN, Dataset, validate_table

_NS_PER_DAY = 86_400 * 1_000_000_000
_EPOCH = date(1970, 1, 1)

# The layout described above. Written into every commit marker. Layout 2
# (ADR 0017) had no loads; layout 3 (ADR 0018) names each batch's load.
STORE_LAYOUT = 3
BATCH_DIRECTORY = "_batches"

# Venue and instrument names become directory names, so they are restricted to
# characters that are safe on every filesystem and cannot form a path.
_NAME = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9._-]{0,62}[A-Za-z0-9])?")
_PART = re.compile(r"part-([0-9]{20})-([0-9]{6})\.parquet")
_MARKER = re.compile(r"batch-([0-9]{20})\.json")
# Data files as they were named before ADR 0017: no commit markers, and
# partitioned by the date of event_ts.
_LEGACY_PART = re.compile(r"part-[0-9]{20}\.parquet")
_LOG_ID = re.compile(LOG_ID_PATTERN)

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

    def __init__(
        self,
        store: "AppendOnlyStore",
        knowledge_ts: int,
        load: "Load | None" = None,
        last: bool = False,
    ):
        self._store = store
        self._knowledge_ts = knowledge_ts
        self._load = load
        self._last = last
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
        if self._load is not None:
            self._load._check_rows(rows)
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

        A batch with no rows publishes nothing, unless it is the last batch of
        a load: its marker is what makes the load complete.
        """
        self._require_open()
        try:
            if self._files or self._last:
                self._publish_marker()
        except BaseException:
            self.abandon()
            raise
        self._finish()
        if self._load is not None and self._last:
            self._load._finished = True
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
        load = self._load
        content = {
            "layout": STORE_LAYOUT,
            "knowledge_ts": self._knowledge_ts,
            "files": [path.relative_to(root).as_posix() for path in self._files],
            "rows": dict(sorted(self._rows.items())),
            "load": None
            if load is None
            else {"load_id": load.load_id, "log_id": load.log_id, "last": self._last},
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


class Load:
    """One load of a raw log into the store (ADR 0018). Get one from
    ``AppendOnlyStore.load(log_id)``.

    A load is one or more batches, every row of which names ``log_id``. It is
    complete once its last batch is committed: a batch opened with
    ``last=True``, or the empty one ``finish`` commits. Until then readers do
    not read it at all, and read the log from its previous complete load.
    ``load_id`` is the knowledge time of the load's first batch.
    """

    def __init__(self, store: "AppendOnlyStore", log_id: str):
        self._store = store
        self._log_id = log_id
        self._load_id: int | None = None
        self._finished = False

    @property
    def log_id(self) -> str:
        return self._log_id

    @property
    def load_id(self) -> int | None:
        """The knowledge time of the load's first batch; None before it has one."""
        return self._load_id

    @property
    def finished(self) -> bool:
        """True once the load's last batch is committed."""
        return self._finished

    def batch(self, *, last: bool = False) -> Batch:
        """Open the load's next batch. With ``last=True``, committing it
        completes the load."""
        if self._finished:
            raise RuntimeError(
                f"this load of {self._log_id} is finished: its last batch is committed"
            )
        if type(last) is not bool:
            raise TypeError(f"last must be a bool, got {type(last).__name__}")
        batch = self._store._open(self, last)
        if self._load_id is None:
            self._load_id = batch.knowledge_ts
        return batch

    def finish(self) -> AppendResult:
        """Complete the load with a batch of no rows, for a loader that learns
        only after its last data batch that the log has ended."""
        return self.batch(last=True).commit()

    def _check_rows(self, rows: pa.Table) -> None:
        column = rows.column("log_id")
        others = pc.unique(pc.filter(column, pc.not_equal(column, self._log_id)))
        if len(others):
            raise ValueError(
                f"a load of {self._log_id} takes rows of that log only; this table has rows "
                f"of {sorted(others.to_pylist())}"
            )


class AppendOnlyStore:
    """Writer for the normalized store. Its operations are ``load`` (a raw
    log's batches, ADR 0018), ``batch`` and ``append``, which is a batch of
    one table."""

    def __init__(self, root: str | os.PathLike[str], *, clock: Callable[[], int] = time.time_ns):
        self._root = Path(root)
        self._clock = clock
        self._last_knowledge_ts = self._recover_last_knowledge_ts()
        self._open_batch: Batch | None = None

    def load(self, log_id: str) -> Load:
        """Begin a load of the raw log ``log_id`` (ADR 0018). Every ingest of a
        raw log is one."""
        if not isinstance(log_id, str) or _LOG_ID.fullmatch(log_id) is None:
            raise ValueError(f"a load is of one raw log, named by its log id; got {log_id!r}")
        return Load(self, log_id)

    def batch(self) -> Batch:
        """Open a batch outside any load: one knowledge time for everything
        appended to it, readable only once it is committed (ADR 0017)."""
        return self._open(None, False)

    def _open(self, load: Load | None, last: bool) -> Batch:
        if self._open_batch is not None:
            raise RuntimeError("a batch is already open on this store; commit or abandon it first")
        knowledge_ts = self._next_knowledge_ts()
        self._last_knowledge_ts = knowledge_ts
        self._open_batch = Batch(self, knowledge_ts, load, last)
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
            directory = self._root / BATCH_DIRECTORY
            last = max([last, *_committed(directory)])
            _check_layout(directory)
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


def _check_layout(directory: Path) -> None:
    """Refuse to add to a store whose committed batches are of another layout."""
    if not directory.is_dir():
        return
    for path in sorted(directory.glob("batch-*.json")):
        try:
            layout = json.loads(path.read_text(encoding="utf-8")).get("layout")
        except (OSError, ValueError, AttributeError):
            raise ValueError(f"commit marker {path.name} cannot be read as JSON") from None
        if layout != STORE_LAYOUT:
            raise StoreVersionError(
                f"commit marker {path.name} is for store layout {layout!r}, but this code writes "
                f"layout {STORE_LAYOUT} (ADR 0018), so this store cannot be added to; {REBUILD}"
            )


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
