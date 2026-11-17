"""PITSession: the point-in-time read path into the normalized store.

A session answers two different questions with two cutoffs (ADR 0017):

- **What had we received?** ``available_at``, required: a simulated time. Only
  rows with ``recv_ts <= available_at`` are visible. This is the cutoff a
  backtest sets to simulated time.
- **Which version of the store?** ``version``, optional: a knowledge cutoff.
  Among the rows available, each record's latest version with
  ``knowledge_ts <= version`` is returned. It defaults to the latest committed
  batch and cannot be later than it, so a version that was read once reads
  the same forever.

Only committed batches exist for a session: the files of a load that is still
running, failed or died have no commit marker and are never opened.

**Loads (ADR 0018).** Every ingest of a raw log is a load. For each log, a
session reads only the rows of one load: the log's latest complete load (by
``load_id``, its last batch within the version), or, if it has none, its
latest incomplete load (its committed batches within the version). A log
loaded again is therefore read from the new load alone, once it is complete:
rows the earlier load produced and the new one does not are gone from the
session, though not from the store, and a reload in progress never hides the
previous complete load. Batches written outside a load (tests only) are read
record by record, for logs that have no load.

**One row per trade.** Trades are stored once per observation. The session
shows each trade once: its first observation in ``(log_id, seq)`` order among
the observations inside both cutoffs. ``trade_observations`` shows them all,
for the quality check that compares logs (ADR 0019); ``sql`` cannot.

**Instrument metadata.** ``instrument_for`` gives the version a stored row was
written with; ``instrument`` the version in effect at a time, a log's first
version being in effect from the log's first row (ADR 0019).
docs/data-core-spec.md section 9 is the full description.

That is enforced in three independent ways (architecture section 3.2):

1. **Views.** Every query runs against views that keep the rows inside both
   cutoffs and then the latest version of each record. The typed methods and
   ``sql`` both read these views and nothing else.
2. **Statement validation.** ``sql`` accepts exactly one SELECT over those
   views, checked on DuckDB's own syntax tree (see ``sqlguard``).
3. **Engine lockdown.** The session's private DuckDB instance is configured
   so it can open only the files of committed batches at or before the
   session's version that it reads (not those of a load another load
   replaced), received on or before the day of ``available_at``. It
   cannot write, attach or load anything, and that configuration cannot be
   changed. Even a statement that slipped past the validator could not reach
   a later version, an uncommitted batch or a later day.

A session is a snapshot: batches committed after it was opened are not
visible to it. It is not safe to share between threads.

``ReplayCursor`` is for backtests: it opens the store once and hands out one
session whose ``available_at`` the cursor's owner moves forward. The session
itself has no way to move it.

Before anything else, a session checks every file it will open: the dataset,
schema version and columns its Parquet metadata records must be what this code
reads. A store written by another version of the code is refused with a
``StoreVersionError`` saying to rebuild it from the raw logs (ADR 0015).
"""

import inspect
import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from quantagent.data.fixedpoint import INT64_MAX
from quantagent.data.schema import (
    BOOK_EVENTS,
    DATASETS,
    INSTRUMENTS,
    STREAM_EVENTS,
    TRADES,
    Dataset,
)
from quantagent.data.sqlguard import MAX_QUERY_LENGTH, QueryRejected, validate
from quantagent.data.store import (
    _LEGACY_PART,
    _LOG_ID,
    _MARKER,
    _PART,
    BATCH_DIRECTORY,
    REBUILD,
    STORE_LAYOUT,
    StoreVersionError,
    _check_name,
    _legacy,
)

__all__ = [
    "InstrumentMeta",
    "LoadRead",
    "PITSession",
    "QueryError",
    "QueryRejected",
    "ReplayCursor",
    "StoreVersionError",
]

_NS_PER_DAY = 86_400 * 1_000_000_000
_EPOCH = date(1970, 1, 1)
_PARTITION_COLUMNS = {"venue": "VARCHAR", "instrument": "VARCHAR", "date": "DATE"}
_VIEW_NAMES = frozenset(DATASETS)
_DUCKDB_TYPES = {pa.int8(): "TINYINT", pa.int32(): "INTEGER", pa.int64(): "BIGINT",
                 pa.uint64(): "UBIGINT", pa.string(): "VARCHAR"}  # fmt: skip
_PARTITION_DIRECTORY = re.compile(r"(venue|instrument|date)=[^=]+")
_DATE_DIRECTORY = re.compile(r"date=([0-9]{4}-[0-9]{2}-[0-9]{2})")
_DUPLICATE_COLUMN = re.compile(r'has duplicate column name "([^"]*)"')
# Every stored observation of a trade, before the cut to one per trade_id
# (ADR 0019). Not a dataset name, so sqlguard refuses it in ``sql``.
_TRADE_OBSERVATIONS = "trade_observations"
# The columns of a trade's first observation that trade_observations adds, in
# the stored schema's order.
_FIRST_OBSERVATION = tuple(
    f.name for f in TRADES.stored_schema
    if f.name in {"recv_ts", "log_id", "seq", "price", "qty", "side"}
)  # fmt: skip
_TRADE_OBSERVATION_SCHEMA = pa.schema(
    [*TRADES.stored_schema.remove_metadata(),
     *(pa.field(f"first_{name}", TRADES.stored_schema.field(name).type, nullable=False)
       for name in _FIRST_OBSERVATION)]
)  # fmt: skip

_AS_OF_RETIRED = (
    "as_of was retired (ADR 0017): it was a knowledge cutoff, and a session now has two "
    "cutoffs. Pass available_at=<simulated time, ns> to see the rows received at or before "
    "it; to pin a store version as well, pass version=<knowledge time, ns> (as_of's old "
    "meaning). version defaults to the latest committed batch."
)


class QueryError(RuntimeError):
    """An allowed statement failed while running (for example, an unknown column)."""


_REBUILD = f"the store was written by another version of the code; {REBUILD}"


@dataclass(frozen=True, slots=True)
class _DataFile:
    """One file of a committed batch."""

    dataset: str
    day: int  # its date partition, in days since the epoch: the UTC day its rows were received
    knowledge_ts: int
    path: str  # absolute, with forward slashes


def _well_placed(parts: tuple[str, ...]) -> bool:
    return (
        len(parts) == 5
        and parts[0] in DATASETS
        and [p.split("=")[0] for p in parts[1:4]] == list(_PARTITION_COLUMNS)
        and all(_PARTITION_DIRECTORY.fullmatch(p) for p in parts[1:4])
        and _DATE_DIRECTORY.fullmatch(parts[3]) is not None
    )


def _data_file(root: Path, parts: tuple[str, ...]) -> _DataFile | None:
    """The data file at ``parts`` under ``root``, or None if the path is not one."""
    match = _PART.fullmatch(parts[-1]) if parts else None
    if match is None or not _well_placed(parts):
        return None
    try:
        day = (date.fromisoformat(parts[3][len("date=") :]) - _EPOCH).days
    except ValueError:
        return None
    return _DataFile(parts[0], day, int(match.group(1)), root.joinpath(*parts).as_posix())


@dataclass(frozen=True, slots=True)
class _LoadOf:
    """What a commit marker says about the load its batch belongs to (ADR 0018)."""

    load_id: int
    log_id: str
    last: bool


@dataclass(frozen=True, slots=True)
class _CommittedBatch:
    knowledge_ts: int
    files: tuple[_DataFile, ...]
    load: _LoadOf | None  # None: written outside a load


@dataclass(frozen=True, slots=True)
class LoadRead:
    """The load a session reads one log from (ADR 0018).

    ``complete`` is false when the log has no complete load within the
    session's version, so it is read from its latest incomplete load: one
    still being written, or one that failed or died before its last batch.
    The log may then go on beyond what the session reads. ``batches`` is how
    many of the load's batches are committed within the version.
    """

    log_id: str
    load_id: int
    complete: bool
    batches: int


@dataclass(frozen=True, slots=True)
class _Selection:
    """What a session at one version reads."""

    files: tuple[_DataFile, ...]
    # Rows of these logs in batches written outside a load are not read: each
    # of these logs is read from its load.
    outside_loads: tuple[int, ...]  # knowledge times of the batches outside a load
    loaded_logs: tuple[str, ...]
    loads: tuple[LoadRead, ...] = ()  # one per loaded log, in log order


def _load_of(where: str, knowledge_ts: int, content: dict) -> _LoadOf | None:
    if "load" not in content:
        raise ValueError(f"{where} does not say which load its batch belongs to")
    load = content["load"]
    if load is None:
        return None
    if not isinstance(load, dict):
        raise ValueError(f"{where} has a load that is not a JSON object")
    load_id, log_id, last = load.get("load_id"), load.get("log_id"), load.get("last")
    if type(load_id) is not int or not 1 <= load_id <= knowledge_ts:
        raise ValueError(
            f"{where} has a load_id that is not the knowledge time of an earlier batch"
        )
    if not isinstance(log_id, str) or _LOG_ID.fullmatch(log_id) is None:
        raise ValueError(f"{where} names a load of something that is not a log id")
    if type(last) is not bool:
        raise ValueError(f"{where} does not say whether its batch is its load's last")
    return _LoadOf(load_id, log_id, last)


def _check_loads(batches: list[_CommittedBatch]) -> None:
    """A load is of one log, and has no batch after its last."""
    seen: dict[int, tuple[str, int | None]] = {}  # load_id -> (log_id, knowledge time of its last)
    for batch in sorted(batches, key=lambda b: b.knowledge_ts):
        load = batch.load
        if load is None:
            continue
        where = f"commit marker batch-{batch.knowledge_ts:020d}.json"
        log_id, last = seen.get(load.load_id, (load.log_id, None))
        if log_id != load.log_id:
            raise ValueError(f"{where}: load {load.load_id} is of log {log_id}, not {load.log_id}")
        if last is not None:
            raise ValueError(f"{where}: a batch of load {load.load_id} after its last batch")
        seen[load.load_id] = (log_id, batch.knowledge_ts if load.last else None)


def _select(batches: list[_CommittedBatch], version: int, horizon_day: int) -> _Selection:
    """Each log from one load (ADR 0018): its latest load complete within
    ``version``, or, if it has none, its latest incomplete load, read as far as
    its batches within ``version`` go. Batches outside a load (tests only) are
    read for the logs that have no load."""
    known = [b for b in batches if b.knowledge_ts <= version]
    complete: dict[str, int] = {}  # log_id -> load_id of its latest complete load
    started: dict[str, int] = {}  # log_id -> load_id of its latest load of any kind
    for batch in known:
        load = batch.load
        if load is None:
            continue
        started[load.log_id] = max(started.get(load.log_id, 0), load.load_id)
        if load.last:
            complete[load.log_id] = max(complete.get(load.log_id, 0), load.load_id)
    # A reload in progress never hides the previous complete load.
    chosen = {log_id: complete.get(log_id, latest) for log_id, latest in started.items()}
    read = set(chosen.values())
    kept = [b for b in known if b.load is None or b.load.load_id in read]
    batches = Counter(b.load.load_id for b in kept if b.load is not None)
    return _Selection(
        files=tuple(f for b in kept for f in b.files if f.day <= horizon_day),
        outside_loads=tuple(sorted(b.knowledge_ts for b in kept if b.load is None)),
        loaded_logs=tuple(sorted(chosen)),
        loads=tuple(
            LoadRead(log_id, load_id, complete.get(log_id) == load_id, batches[load_id])
            for log_id, load_id in sorted(chosen.items())
        ),
    )


def _committed_batches(root: Path) -> tuple[list[_CommittedBatch], int]:
    """Every committed batch, and the latest committed knowledge time (0 if no
    batch was ever committed).

    A data file that no commit marker names belongs to a batch that was not
    committed, and is left out. Anything that is not the layout this code
    writes is refused.
    """
    on_disk: dict[str, _DataFile] = {}
    for path in sorted(root.rglob("*.parquet")):
        parts = path.relative_to(root).parts
        found = _data_file(root, parts)
        if found is None:
            if _LEGACY_PART.fullmatch(parts[-1]):
                raise _legacy(path)
            raise ValueError(f"unexpected file in store: {'/'.join(parts)}")
        on_disk["/".join(parts)] = found

    directory = root / BATCH_DIRECTORY
    if not directory.is_dir():
        if on_disk:
            raise StoreVersionError(
                "the store has data files but no commit markers (ADR 0017), so it was not "
                f"written by this version of the code; {REBUILD}"
            )
        return [], 0

    committed: list[_CommittedBatch] = []
    latest = 0
    for marker in sorted(directory.iterdir()):
        if marker.name.startswith("."):
            continue  # a marker still being written
        match = _MARKER.fullmatch(marker.name)
        if match is None:
            raise ValueError(f"unexpected file in store: {BATCH_DIRECTORY}/{marker.name}")
        knowledge_ts = int(match.group(1))
        where = f"commit marker {marker.name}"
        try:
            content = json.loads(marker.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            raise ValueError(f"{where} cannot be read as JSON") from None
        if not isinstance(content, dict):
            raise ValueError(f"{where} is not a JSON object")
        if content.get("layout") != STORE_LAYOUT:
            raise StoreVersionError(
                f"{where} is for store layout {content.get('layout')!r}, but this code reads "
                f"layout {STORE_LAYOUT}; {REBUILD}"
            )
        if content.get("knowledge_ts") != knowledge_ts:
            raise ValueError(f"{where} does not hold the knowledge time its name gives")
        names = content.get("files")
        if not isinstance(names, list) or not all(isinstance(n, str) for n in names):
            raise ValueError(f"{where} has no list of files")
        files = []
        for name in names:
            listed = _data_file(root, tuple(name.split("/")))
            if listed is None or listed.knowledge_ts != knowledge_ts:
                raise ValueError(f"{where} names a file that is not one of its batch's data files")
            if name not in on_disk:
                raise ValueError(f"committed store file {name} is missing; the store is damaged")
            files.append(on_disk[name])
        load = _load_of(where, knowledge_ts, content)
        committed.append(_CommittedBatch(knowledge_ts, tuple(files), load))
        latest = max(latest, knowledge_ts)
    _check_loads(committed)
    return committed, latest


def _check_versions(root: Path, files: list[_DataFile]) -> None:
    """Refuse any file whose recorded dataset, schema version or columns are not
    what this code reads. Only the footer of each file is read."""
    for file in files:
        name, path = file.dataset, file.path
        dataset = DATASETS[name]
        expected = dataset.stored_schema.remove_metadata()
        where = f"store file {Path(path).relative_to(root).as_posix()}"
        try:
            schema = pq.read_schema(path)
        except (OSError, pa.ArrowException) as error:
            problem = f"cannot be read as Parquet ({type(error).__name__})"
            raise StoreVersionError(f"{where} {problem}; {_REBUILD}") from None
        metadata = schema.metadata or {}
        found = metadata.get(b"qa.dataset", b"").decode("utf-8", "replace")
        version = metadata.get(b"qa.schema_version", b"").decode("utf-8", "replace")
        if not found or not version:
            raise StoreVersionError(f"{where} has no schema version; {_REBUILD}")
        if found != name:
            raise StoreVersionError(
                f"{where} is in the {name} directory but holds {found} rows; {_REBUILD}"
            )
        if version != str(dataset.version):
            raise StoreVersionError(
                f"{where} holds {name} schema version {version}, but this code reads "
                f"version {dataset.version}: {_REBUILD}"
            )
        if not schema.remove_metadata().equals(expected):
            raise StoreVersionError(
                f"{where} says {name} schema version {version}, but its columns are not "
                f"that version's; {_REBUILD}"
            )


@dataclass(frozen=True, slots=True)
class InstrumentMeta:
    """One version of an instrument's metadata, as known to a session.

    ``price_exp`` and ``qty_exp`` are the instrument's fixed representation
    (ADR 0019); ``price_precision`` and ``qty_precision`` the decimals the venue
    used when this version was observed. ``log_id`` and ``seq`` name the raw
    record it was observed in.
    """

    venue: str
    instrument: str
    event_ts: int  # the version's own time: the receipt time, for both venues
    recv_ts: int
    knowledge_ts: int
    log_id: str
    seq: int
    price_exp: int
    qty_exp: int
    price_precision: int
    qty_precision: int
    tick_size: int  # mantissa at price_exp
    lot_size: int  # mantissa at qty_exp


def _meta(venue: str, instrument: str, row: dict[str, Any]) -> InstrumentMeta:
    return InstrumentMeta(
        venue=venue,
        instrument=instrument,
        **{name: row[name] for name in ("event_ts", "recv_ts", "knowledge_ts", "log_id", "seq",
                                        "price_exp", "qty_exp", "price_precision",
                                        "qty_precision", "tick_size", "lot_size")},
    )  # fmt: skip


def _check_time(name: str, value: Any) -> None:
    if type(value) is not int:
        raise TypeError(f"{name} must be int nanoseconds, got {type(value).__name__}")
    if not 0 <= value <= INT64_MAX:
        raise ValueError(f"{name}={value} is outside the int64 nanosecond range")


def _quote(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _view_sql(
    dataset: Dataset,
    files: list[str],
    version: int,
    available_at: int,
    selection: _Selection,
    *,
    every_observation: bool = False,
) -> str:
    """The view of ``dataset`` inside both cutoffs. With ``every_observation``
    it is not cut to ``one_row_per`` (the trade observations view)."""
    stored = dataset.stored_schema
    view = _TRADE_OBSERVATIONS if every_observation else dataset.name
    columns = ", ".join(f'"{name}"' for name in (*_PARTITION_COLUMNS, *stored.names))
    if not files:
        typed = [f'CAST(NULL AS {t}) AS "{n}"' for n, t in _PARTITION_COLUMNS.items()]
        typed += [f'CAST(NULL AS {_DUCKDB_TYPES[f.type]}) AS "{f.name}"' for f in stored]
        return f'CREATE OR REPLACE VIEW "{view}" AS SELECT {", ".join(typed)} WHERE false'

    identity = ", ".join(f'"{name}"' for name in ("venue", "instrument", *dataset.key))
    # Latest knowledge first. The remaining columns only make the choice
    # deterministic if one batch holds the same record twice.
    tie_break = ", ".join(f'"{name}" DESC' for name in stored.names if name != "knowledge_ts")
    hive_types = ", ".join(f"'{n}': {t}" for n, t in _PARTITION_COLUMNS.items())
    # Both cutoffs apply before a version is chosen: the version of a record a
    # session sees is the latest among the rows it had received (ADR 0017).
    where = f'"knowledge_ts" <= {version} AND "recv_ts" <= {available_at}'
    if selection.outside_loads and selection.loaded_logs:
        # A log read from its load is not read from batches outside a load (ADR 0018).
        batches = ", ".join(str(k) for k in selection.outside_loads)
        logs = ", ".join(_quote(log) for log in selection.loaded_logs)
        where += f' AND NOT ("knowledge_ts" IN ({batches}) AND "log_id" IN ({logs}))'
    versions = f"""
        SELECT {columns} FROM (
            SELECT {columns}, row_number() OVER (
                PARTITION BY {identity} ORDER BY "knowledge_ts" DESC, {tie_break}
            ) AS _version_rank
            FROM read_parquet(
                [{", ".join(_quote(f) for f in files)}],
                hive_partitioning = true, hive_types = {{{hive_types}}}
            )
            WHERE {where}
        ) WHERE _version_rank = 1
    """
    if dataset.one_row_per and not every_observation:
        # One row per trade: the first observation in replay order among those
        # inside both cutoffs (ADR 0018). (log_id, seq) is unique within one
        # partition, because both are in the key.
        per = ", ".join(f'"{name}"' for name in ("venue", "instrument", *dataset.one_row_per))
        versions = f"""
            SELECT {columns} FROM (
                SELECT {columns}, row_number() OVER (
                    PARTITION BY {per} ORDER BY "log_id", "seq"
                ) AS _first
                FROM ({versions})
            ) WHERE _first = 1
        """
    return f'CREATE OR REPLACE VIEW "{view}" AS {versions}'


def _create_views(
    con: duckdb.DuckDBPyConnection, selection: _Selection, version: int, available_at: int
) -> None:
    """(Re)define the views for ``available_at``, all of them or none: one per
    dataset, and the trade observations view (not reachable from ``sql``)."""
    day = available_at // _NS_PER_DAY
    con.execute("BEGIN")
    try:
        for dataset in DATASETS.values():
            # A file of a later receipt day cannot hold a row received by now.
            paths = [f.path for f in selection.files if f.dataset == dataset.name and f.day <= day]
            con.execute(_view_sql(dataset, paths, version, available_at, selection))
            if dataset.one_row_per:
                con.execute(_view_sql(dataset, paths, version, available_at, selection,
                                      every_observation=True))  # fmt: skip
        con.execute("COMMIT")
    except BaseException:
        con.execute("ROLLBACK")
        raise


def _open(
    session: "PITSession", root: Any, *, available_at: int, version: int | None, horizon: int
) -> _Selection:
    """Set ``session`` up to read ``root`` at the two cutoffs.

    ``horizon`` is the latest ``available_at`` the session's engine may ever
    serve: ``available_at`` itself for a plain session, the cursor's end for a
    replay cursor. Returns what the session reads, including the files its
    engine may open.
    """
    _check_time("available_at", available_at)
    if version is not None:
        if type(version) is not int:
            raise TypeError(
                f"version must be int nanoseconds or None, got {type(version).__name__}"
            )
        if not 0 <= version <= INT64_MAX:
            raise ValueError(f"version={version} is outside the int64 nanosecond range")

    root_path = Path(root).resolve()
    if not root_path.is_dir():
        raise FileNotFoundError(f"store directory does not exist: {root_path}")
    if any("=" in part for part in root_path.parts):
        raise ValueError(
            f"store path must not contain '=' (it would be read as a partition): {root_path}"
        )

    committed, latest = _committed_batches(root_path)
    if version is None:
        version = latest
    elif version > latest:
        raise ValueError(
            f"version={version} is later than the latest committed batch ({latest}). Such a "
            "version would not be repeatable: a batch committed later could fall inside it. "
            f"Omit version to read the latest committed batch, or pass version={latest}."
        )
    selection = _select(committed, version, horizon // _NS_PER_DAY)
    files = selection.files
    _check_versions(root_path, list(files))

    con = duckdb.connect(":memory:", config={"python_enable_replacements": False})
    try:
        _create_views(con, selection, version, available_at)
        # From here on this database can open only the files listed, and
        # none of these settings can be changed again.
        con.execute(f"SET allowed_paths = [{', '.join(_quote(f.path) for f in files)}]")
        con.execute("SET enable_external_access = false")
        con.execute("SET autoinstall_known_extensions = false")
        con.execute("SET autoload_known_extensions = false")
        con.execute("SET lock_configuration = true")
    except BaseException:
        con.close()
        raise

    object.__setattr__(session, "_available_at", available_at)
    object.__setattr__(session, "_version", version)
    object.__setattr__(session, "_loads", selection.loads)
    object.__setattr__(session, "_con", con)
    object.__setattr__(session, "_redactions", (root_path.as_posix(), str(root_path)))
    return selection


class PITSession:
    """Read the store as it was received by ``available_at``, at one version.

    ``available_at`` is a required keyword: there is no default and no way to
    change it afterwards. ``version`` defaults to the latest committed batch.
    To read at another time or version, open another session; a backtest that
    moves through time uses a ``ReplayCursor``.
    """

    __slots__ = ("_available_at", "_version", "_loads", "_con", "_redactions")

    def __new__(cls, *args: Any, **kwargs: Any) -> "PITSession":
        if "as_of" in kwargs:
            raise TypeError(_AS_OF_RETIRED)
        return super().__new__(cls)

    def __init__(self, root: Any, *, available_at: int, version: int | None = None):
        _open(self, root, available_at=available_at, version=version, horizon=available_at)

    # --- public surface -----------------------------------------------------

    @property
    def available_at(self) -> int:
        """Simulated time: only rows with ``recv_ts`` at or before it are visible."""
        return self._available_at

    @property
    def version(self) -> int:
        """The knowledge cutoff in force: the committed batch this session reads up to."""
        return self._version

    def loads(self) -> tuple[LoadRead, ...]:
        """The load each loaded log is read from, at this session's version, in
        ``log_id`` order (ADR 0018). A ``LoadRead`` with ``complete`` false is a
        log read from an incomplete load, which the ``loads`` quality check
        reports (ADR 0019). It depends on the version alone, not on
        ``available_at``."""
        return self._loads

    def trades(self, venue: str, instrument: str, start: int, end: int) -> pa.Table:
        """Trades received in ``start <= recv_ts < end``, latest known version of each.

        One row per trade: its first observation in ``(log_id, seq)`` order
        among those the session can see (ADR 0018). A trade is in the window
        if that observation was received in it.
        """
        return self._range(TRADES, venue, instrument, start, end)

    def book_events(self, venue: str, instrument: str, start: int, end: int) -> pa.Table:
        """Book levels received in ``start <= recv_ts < end``, latest known version of each."""
        return self._range(BOOK_EVENTS, venue, instrument, start, end)

    def stream_events(self, venue: str, instrument: str, start: int, end: int) -> pa.Table:
        """Integrity events (gaps, reconnects, checksum mismatches, ...) noticed at
        records received in ``start <= recv_ts < end``: the same window, on the
        same clock, as the market data they qualify."""
        return self._range(STREAM_EVENTS, venue, instrument, start, end)

    def instruments(self, venue: str, instrument: str) -> pa.Table:
        """Every known metadata version for an instrument, oldest first."""
        _check_name("venue", venue)
        _check_name("instrument", instrument)
        columns = ", ".join(f'"{name}"' for name in INSTRUMENTS.stored_schema.names)
        table = self._run(
            f'SELECT {columns} FROM "instruments" WHERE "venue" = ? AND "instrument" = ? '
            'ORDER BY "event_ts", "log_id", "seq"',
            [venue, instrument],
        )
        return table.cast(INSTRUMENTS.stored_schema)

    def instrument(
        self, venue: str, instrument: str, *, at: int | None = None
    ) -> InstrumentMeta | None:
        """The metadata version in effect at time ``at`` (default: ``available_at``).

        A version takes effect at its ``event_ts``, except a log's first
        version, which is in effect from the log's first row: the earliest
        ``event_ts`` of any row of that log the session shows (ADR 0019). The
        recorder fetches metadata before it subscribes to market data, so
        that version describes every row of its session, including history
        the venue resent on subscribe with venue times before the session
        began. Of two versions in effect, the one that took effect later wins,
        then the later in ``(log_id, seq)`` order.

        Only versions and rows the session can see are considered, whatever
        ``at`` is. Returns None if no version was in effect at that time. To
        interpret a stored row, ``instrument_for`` is exact; this is for a
        point in time.
        """
        effective = self._available_at if at is None else at
        _check_time("at", effective)
        versions = self.instruments(venue, instrument).to_pylist()
        if not versions:
            return None
        first_rows = self._first_row_times(venue, instrument)
        first_of_log: dict[str, int] = {}
        for row in versions:
            first_of_log[row["log_id"]] = min(first_of_log.get(row["log_id"], row["seq"]),
                                              row["seq"])  # fmt: skip

        def takes_effect(row: dict[str, Any]) -> int:
            if row["seq"] != first_of_log[row["log_id"]]:
                return row["event_ts"]
            return min(row["event_ts"], first_rows.get(row["log_id"], row["event_ts"]))

        in_effect = [(takes_effect(r), r["log_id"], r["seq"], r) for r in versions]
        in_effect = [entry for entry in in_effect if entry[0] <= effective]
        if not in_effect:
            return None
        return _meta(venue, instrument, max(in_effect, key=lambda entry: entry[:3])[3])

    def instrument_for(
        self, venue: str, instrument: str, *, log_id: str, seq: int
    ) -> InstrumentMeta | None:
        """The metadata version a stored row was written with: the latest
        version of the row's own log at or before its record (ADR 0019).

        Every market row the adapters store follows a metadata version of its
        log (they refuse market data before one), so for such a row this
        never returns None. Only versions the session can see are considered.
        """
        if not isinstance(log_id, str) or _LOG_ID.fullmatch(log_id) is None:
            raise ValueError(f"log_id must be a log id, got {log_id!r}")
        if type(seq) is not int:
            raise TypeError(f"seq must be an int, got {type(seq).__name__}")
        if seq < 0:
            raise ValueError(f"seq must not be negative, got {seq}")
        of_log = [
            row
            for row in self.instruments(venue, instrument).to_pylist()
            if row["log_id"] == log_id and row["seq"] <= seq
        ]
        if not of_log:
            return None
        return _meta(venue, instrument, max(of_log, key=lambda row: row["seq"]))

    def trade_observations(self, venue: str, instrument: str, start: int, end: int) -> pa.Table:
        """Every stored observation of a trade received in ``start <= recv_ts < end``,
        each with its trade's first observation (ADR 0019).

        ``trades`` shows one row per trade, its first observation in
        ``(log_id, seq)`` order (ADR 0018). This shows every observation the
        session can see, latest known version of each, in ``(log_id, seq)``
        order, with the first observation's ``log_id``, ``seq``, ``recv_ts``,
        ``price``, ``qty`` and ``side`` as ``first_...`` columns, wherever and
        whenever it was received. It is for finding trades that two logs saw
        differently; research counting trades reads ``trades``. There is no
        SQL view of it.
        """
        _check_name("venue", venue)
        _check_name("instrument", instrument)
        _check_time("start", start)
        _check_time("end", end)
        names = TRADES.stored_schema.names
        columns = ", ".join(f'o."{name}"' for name in names)
        firsts = ", ".join(f'f."{name}" AS "first_{name}"' for name in _FIRST_OBSERVATION)
        table = self._run(
            f'SELECT {columns}, {firsts} FROM "{_TRADE_OBSERVATIONS}" o JOIN "trades" f '
            'ON f."venue" = o."venue" AND f."instrument" = o."instrument" '
            'AND f."trade_id" = o."trade_id" '
            'WHERE o."venue" = ? AND o."instrument" = ? AND o."recv_ts" >= ? AND o."recv_ts" < ? '
            'ORDER BY o."log_id", o."seq", o."trade_id"',
            [venue, instrument, start, end],
        )
        return table.cast(_TRADE_OBSERVATION_SCHEMA)

    def sql(self, query: str) -> pa.Table:
        """Run one SELECT over the views ``trades``, ``book_events``, ``instruments``
        and ``stream_events``.

        Each view has the columns venue, instrument, date, then the dataset's
        stored columns, and holds the latest version of each record inside the
        session's two cutoffs, each log read from its latest complete load;
        ``trades`` holds one row per trade (ADR 0018). Anything other than a single allowed SELECT
        raises QueryRejected without being run; see ``sqlguard`` for what is
        allowed.
        """
        if not isinstance(query, str):
            raise TypeError(f"query must be str, got {type(query).__name__}")
        if len(query) > MAX_QUERY_LENGTH:
            raise QueryRejected(f"query is too long ({len(query)} > {MAX_QUERY_LENGTH})")
        con = self._connection()
        try:
            serialized = con.sql("SELECT json_serialize_sql(?)", params=[query]).fetchall()[0][0]
            tree = json.loads(serialized)
        except (duckdb.Error, RecursionError):
            tree = None  # refused below; never executed
        validate(tree, _VIEW_NAMES)
        # Execute the tree that was validated, not the text it came from.
        try:
            return self._run("SELECT * FROM json_execute_serialized_sql(?)", [serialized])
        except QueryError as error:
            # The result becomes a table, and a table cannot have two columns
            # of one name; the engine's message names the internal call.
            duplicate = _DUPLICATE_COLUMN.search(str(error))
            if duplicate is None:
                raise
        raise QueryError(
            f'the result would have two columns named "{duplicate.group(1)}"; give each '
            'result column its own name, for example "b.price AS other_price"'
        )

    def close(self) -> None:
        if self._con is not None:
            self._con.close()
            object.__setattr__(self, "_con", None)

    # --- internals -----------------------------------------------------------

    def _connection(self) -> duckdb.DuckDBPyConnection:
        if self._con is None:
            raise RuntimeError("session is closed")
        return self._con

    def _first_row_times(self, venue: str, instrument: str) -> dict[str, int]:
        """Per log, the earliest ``event_ts`` of any row the session shows."""
        each = " UNION ALL ".join(
            f'SELECT "log_id", "event_ts" FROM "{name}" WHERE "venue" = ? AND "instrument" = ?'
            for name in DATASETS
        )
        table = self._run(
            f'SELECT "log_id", min("event_ts") AS "first" FROM ({each}) GROUP BY "log_id"',
            [venue, instrument] * len(DATASETS),
        )
        return dict(zip(table.column("log_id").to_pylist(), table.column("first").to_pylist(),
                        strict=True))  # fmt: skip

    def _range(
        self, dataset: Dataset, venue: str, instrument: str, start: int, end: int
    ) -> pa.Table:
        _check_name("venue", venue)
        _check_name("instrument", instrument)
        _check_time("start", start)
        _check_time("end", end)
        names = dataset.stored_schema.names
        columns = ", ".join(f'"{name}"' for name in names)
        # Replay order first, then every other column so the order is total.
        replay_order = ("log_id", "seq")  # ADR 0012, ADR 0013
        rest = [name for name in names if name not in replay_order]
        order = ", ".join(f'"{name}"' for name in (*replay_order, *rest))
        # The window is receipt time for every dataset (ADR 0017).
        table = self._run(
            f'SELECT {columns} FROM "{dataset.name}" WHERE "venue" = ? AND "instrument" = ? '
            f'AND "recv_ts" >= ? AND "recv_ts" < ? ORDER BY {order}',
            [venue, instrument, start, end],
        )
        return table.cast(dataset.stored_schema)

    def _run(self, sql: str, params: list[Any]) -> pa.Table:
        con = self._connection()
        try:
            return con.sql(sql, params=params).to_arrow_table()
        except duckdb.Error as error:
            message = str(error)
        # Raised outside the handler so the original error, which can quote
        # file paths, is not kept on the exception chain.
        for text in self._redactions:
            message = message.replace(text, "<store>")
        raise QueryError(message)

    def __enter__(self) -> "PITSession":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"PITSession(available_at={self._available_at}, version={self._version})"

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError(
            "PITSession is immutable; open a new session for another available_at or version"
        )

    def __delattr__(self, name: str) -> None:
        raise AttributeError("PITSession is immutable")

    def __reduce_ex__(self, protocol: object) -> None:
        raise TypeError("PITSession cannot be copied or pickled")

    def __init_subclass__(cls, **kwargs: object) -> None:
        raise TypeError("PITSession cannot be subclassed")


# __new__ takes anything so that it can refuse as_of with a useful message; the
# constructor's real signature is __init__'s.
PITSession.__signature__ = inspect.Signature(
    list(inspect.signature(PITSession.__init__).parameters.values())[1:]
)


class ReplayCursor:
    """One session for a whole backtest, and the only thing that can move its time.

    The harness opens a cursor over the replay's span, hands ``cursor.session``
    to the strategy, and calls ``advance`` as simulated time passes. The
    session reads exactly what a new ``PITSession(available_at=now)`` at the
    same version would read, without reopening the store. Time only moves
    forward, and never past ``end``.

    The strategy must be given the session, never the cursor: the session has
    no method that moves time and holds no reference to its cursor.
    """

    __slots__ = ("_session", "_selection", "_end")

    def __init__(self, root: Any, *, start: int, end: int, version: int | None = None):
        _check_time("start", start)
        _check_time("end", end)
        if end < start:
            raise ValueError(f"end={end} is before start={start}")
        session = object.__new__(PITSession)
        selection = _open(session, root, available_at=start, version=version, horizon=end)
        object.__setattr__(self, "_session", session)
        object.__setattr__(self, "_selection", selection)
        object.__setattr__(self, "_end", end)

    @property
    def session(self) -> PITSession:
        """The session to hand to research code. It follows this cursor's time."""
        return self._session

    @property
    def available_at(self) -> int:
        return self._session.available_at

    @property
    def end(self) -> int:
        """The latest time this cursor can be advanced to."""
        return self._end

    def advance(self, available_at: int) -> None:
        """Move simulated time forward to ``available_at``."""
        _check_time("available_at", available_at)
        session = self._session
        con = session._connection()
        if available_at < session.available_at:
            raise ValueError(
                f"a replay cursor cannot move backwards: it is at {session.available_at}, "
                f"asked for {available_at}. Open a new cursor to replay from an earlier time."
            )
        if available_at > self._end:
            raise ValueError(
                f"available_at={available_at} is past this cursor's end ({self._end}); "
                "open the cursor with a later end"
            )
        if available_at == session.available_at:
            return
        try:
            _create_views(con, self._selection, session.version, available_at)
        except duckdb.Error as error:
            message = str(error)
        else:
            object.__setattr__(session, "_available_at", available_at)
            return
        for text in session._redactions:
            message = message.replace(text, "<store>")
        raise QueryError(message)

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> "ReplayCursor":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    def __repr__(self) -> str:
        return (
            f"ReplayCursor(available_at={self.available_at}, end={self._end}, "
            f"version={self._session.version})"
        )

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError("ReplayCursor is immutable; move time with advance()")

    def __delattr__(self, name: str) -> None:
        raise AttributeError("ReplayCursor is immutable")

    def __reduce_ex__(self, protocol: object) -> None:
        raise TypeError("ReplayCursor cannot be copied or pickled")

    def __init_subclass__(cls, **kwargs: object) -> None:
        raise TypeError("ReplayCursor cannot be subclassed")
