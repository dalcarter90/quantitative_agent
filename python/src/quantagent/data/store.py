"""Append-only Parquet store for normalized market data (ADR 0006).

Layout, hive-style so DuckDB and pyarrow can discover the partitions::

    <root>/<dataset>/venue=<venue>/instrument=<instrument>/date=<YYYY-MM-DD>/
        part-<knowledge_ts, 20 digits>.parquet

``date`` is the UTC date of ``event_ts``. Each append is one batch with one
``knowledge_ts``, written as one new file per partition it touches. Existing
files are never opened for writing, renamed or removed, and there is no update
or delete operation: a correction is another append.

The store assumes a single writer process per root.

This is the write side only. Reading goes through PITSession (Phase 1b).
"""

import os
import re
import time
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

# Venue and instrument names become directory names, so they are restricted to
# characters that are safe on every filesystem and cannot form a path.
_NAME = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9._-]{0,62}[A-Za-z0-9])?")
_PART = re.compile(r"part-([0-9]{20})\.parquet")


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


class AppendOnlyStore:
    """Writer for the normalized store. Its only operation is ``append``."""

    def __init__(self, root: str | os.PathLike[str], *, clock: Callable[[], int] = time.time_ns):
        self._root = Path(root)
        self._clock = clock
        self._last_knowledge_ts = self._recover_last_knowledge_ts()

    def append(self, dataset: Dataset, venue: str, instrument: str, rows: pa.Table) -> AppendResult:
        """Append ``rows`` as one batch and return the knowledge time it was given.

        ``rows`` must match ``dataset.schema`` exactly (build it with
        ``schema.table_from_rows``). The store stamps every row of the batch
        with one ``knowledge_ts``, strictly greater than any it has issued
        before, including in earlier processes.
        """
        if not isinstance(dataset, Dataset):
            raise TypeError(f"dataset must be a Dataset, got {type(dataset).__name__}")
        _check_name("venue", venue)
        _check_name("instrument", instrument)
        validate_table(dataset, rows)
        if rows.num_rows == 0:
            raise ValueError("cannot append an empty batch")

        knowledge_ts = self._next_knowledge_ts()
        stored = self._with_knowledge_ts(dataset, rows, knowledge_ts)
        partition = self._root / dataset.name / f"venue={venue}" / f"instrument={instrument}"

        # Write every file of the batch under a temporary name first, then
        # publish them, so a failure part-way through publishes nothing.
        staged: list[tuple[Path, Path]] = []
        published: list[Path] = []
        try:
            days = pc.divide(stored.column("event_ts"), _NS_PER_DAY)
            for day in sorted(pc.unique(days).to_pylist()):
                directory = partition / f"date={(_EPOCH + timedelta(days=day)).isoformat()}"
                final = directory / f"part-{knowledge_ts:020d}.parquet"
                temporary = directory / f".{final.name}.tmp"
                directory.mkdir(parents=True, exist_ok=True)
                staged.append((temporary, final))
                self._write_file(stored.filter(pc.equal(days, day)), temporary)

            for _, final in staged:
                if final.exists():
                    raise FileExistsError(f"refusing to overwrite {final}")
            for temporary, final in staged:
                # A hard link fails if the target exists, on every platform,
                # so this can never replace a file.
                os.link(temporary, final)
                published.append(final)
        except BaseException:
            for final in published:
                final.unlink(missing_ok=True)
            raise
        finally:
            for temporary, _ in staged:
                temporary.unlink(missing_ok=True)

        self._last_knowledge_ts = knowledge_ts
        return AppendResult(knowledge_ts=knowledge_ts, rows=rows.num_rows, files=tuple(published))

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
        last = -1
        if self._root.is_dir():
            for path in self._root.rglob("*.parquet"):
                match = _PART.fullmatch(path.name)
                if match is None:
                    raise ValueError(f"unexpected file in store: {path}")
                last = max(last, int(match.group(1)))
        return last

    @staticmethod
    def _with_knowledge_ts(dataset: Dataset, rows: pa.Table, knowledge_ts: int) -> pa.Table:
        stamp = pa.array([knowledge_ts] * rows.num_rows, type=pa.int64())
        stored_schema = dataset.stored_schema
        columns = [
            stamp if name == KNOWLEDGE_TS else rows.column(name) for name in stored_schema.names
        ]
        return pa.Table.from_arrays(columns, schema=stored_schema)

    @staticmethod
    def _write_file(table: pa.Table, path: Path) -> None:
        with open(path, "wb") as handle:
            pq.write_table(table, handle, compression="zstd")
            handle.flush()
            os.fsync(handle.fileno())
