"""PITSession: the point-in-time read path into the normalized store.

A session is opened at a fixed ``as_of`` knowledge time and can only ever
return rows that were in the store at that time. That is enforced in three
independent ways (architecture section 3.2):

1. **Views.** Every query runs against views that keep rows with
   ``knowledge_ts <= as_of`` and then the latest known version of each record.
   The typed methods and ``sql`` both read these views and nothing else.
2. **Statement validation.** ``sql`` accepts exactly one SELECT over those
   views, checked on DuckDB's own syntax tree (see ``sqlguard``).
3. **Engine lockdown.** The session's private DuckDB instance is configured
   so it can open only the data files whose knowledge time is at or before
   ``as_of``, cannot write, attach or load anything, and cannot have that
   configuration changed. Even a statement that slipped past the validator
   could not reach later data.

A session is a snapshot: files appended after it was opened are not visible
to it. It is not safe to share between threads.
"""

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa

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
from quantagent.data.store import _PART, _check_name

__all__ = ["InstrumentMeta", "PITSession", "QueryError", "QueryRejected"]

_PARTITION_COLUMNS = {"venue": "VARCHAR", "instrument": "VARCHAR", "date": "DATE"}
_VIEW_NAMES = frozenset(DATASETS)
_DUCKDB_TYPES = {pa.int8(): "TINYINT", pa.int32(): "INTEGER", pa.int64(): "BIGINT",
                 pa.uint64(): "UBIGINT", pa.string(): "VARCHAR"}  # fmt: skip
_PARTITION_DIRECTORY = re.compile(r"(venue|instrument|date)=[^=]+")


class QueryError(RuntimeError):
    """An allowed statement failed while running (for example, an unknown column)."""


@dataclass(frozen=True, slots=True)
class InstrumentMeta:
    """One version of an instrument's metadata, as known to a session."""

    venue: str
    instrument: str
    event_ts: int  # when this version takes effect
    recv_ts: int
    knowledge_ts: int
    price_exp: int
    qty_exp: int
    tick_size: int  # mantissa at price_exp
    lot_size: int  # mantissa at qty_exp


def _check_time(name: str, value: Any) -> None:
    if type(value) is not int:
        raise TypeError(f"{name} must be int nanoseconds, got {type(value).__name__}")
    if not 0 <= value <= INT64_MAX:
        raise ValueError(f"{name}={value} is outside the int64 nanosecond range")


def _quote(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _visible_files(root: Path, as_of: int) -> dict[str, list[str]]:
    """Data files whose knowledge time is at or before ``as_of``, per dataset."""
    files: dict[str, list[str]] = {name: [] for name in DATASETS}
    for path in sorted(root.rglob("*.parquet")):
        parts = path.relative_to(root).parts
        match = _PART.fullmatch(parts[-1])
        well_placed = (
            len(parts) == 5
            and parts[0] in DATASETS
            and [p.split("=")[0] for p in parts[1:4]] == list(_PARTITION_COLUMNS)
            and all(_PARTITION_DIRECTORY.fullmatch(p) for p in parts[1:4])
        )
        if match is None or not well_placed:
            raise ValueError(f"unexpected file in store: {'/'.join(parts)}")
        if int(match.group(1)) <= as_of:
            files[parts[0]].append(path.as_posix())
    return files


def _view_sql(dataset: Dataset, files: list[str], as_of: int) -> str:
    stored = dataset.stored_schema
    columns = ", ".join(f'"{name}"' for name in (*_PARTITION_COLUMNS, *stored.names))
    if not files:
        typed = [f'CAST(NULL AS {t}) AS "{n}"' for n, t in _PARTITION_COLUMNS.items()]
        typed += [f'CAST(NULL AS {_DUCKDB_TYPES[f.type]}) AS "{f.name}"' for f in stored]
        return f'CREATE VIEW "{dataset.name}" AS SELECT {", ".join(typed)} WHERE false'

    identity = ", ".join(f'"{name}"' for name in ("venue", "instrument", *dataset.key))
    # Latest knowledge first. The remaining columns only make the choice
    # deterministic if one batch holds the same record twice.
    tie_break = ", ".join(f'"{name}" DESC' for name in stored.names if name != "knowledge_ts")
    hive_types = ", ".join(f"'{n}': {t}" for n, t in _PARTITION_COLUMNS.items())
    return f"""
        CREATE VIEW "{dataset.name}" AS
        SELECT {columns} FROM (
            SELECT {columns}, row_number() OVER (
                PARTITION BY {identity} ORDER BY "knowledge_ts" DESC, {tie_break}
            ) AS _version_rank
            FROM read_parquet(
                [{", ".join(_quote(f) for f in files)}],
                hive_partitioning = true, hive_types = {{{hive_types}}}
            )
            WHERE "knowledge_ts" <= {as_of}
        ) WHERE _version_rank = 1
    """


class PITSession:
    """Read the store as it was known at ``as_of``.

    ``as_of`` is a required keyword: there is no default and no way to change
    it afterwards. To read as of another time, open another session.
    """

    __slots__ = ("_as_of", "_con", "_redactions")

    def __init__(self, root: Any, *, as_of: int):
        if type(as_of) is not int:
            raise TypeError(f"as_of must be int nanoseconds, got {type(as_of).__name__}")
        if not 0 <= as_of <= INT64_MAX:
            raise ValueError(f"as_of={as_of} is outside the int64 nanosecond range")

        root_path = Path(root).resolve()
        if not root_path.is_dir():
            raise FileNotFoundError(f"store directory does not exist: {root_path}")
        if any("=" in part for part in root_path.parts):
            raise ValueError(
                f"store path must not contain '=' (it would be read as a partition): {root_path}"
            )

        files = _visible_files(root_path, as_of)
        readable = [path for paths in files.values() for path in paths]

        con = duckdb.connect(":memory:", config={"python_enable_replacements": False})
        try:
            for dataset in DATASETS.values():
                con.execute(_view_sql(dataset, files[dataset.name], as_of))
            # From here on this database can open only the files listed, and
            # none of these settings can be changed again.
            con.execute(f"SET allowed_paths = [{', '.join(_quote(p) for p in readable)}]")
            con.execute("SET enable_external_access = false")
            con.execute("SET autoinstall_known_extensions = false")
            con.execute("SET autoload_known_extensions = false")
            con.execute("SET lock_configuration = true")
        except BaseException:
            con.close()
            raise

        object.__setattr__(self, "_as_of", as_of)
        object.__setattr__(self, "_con", con)
        object.__setattr__(self, "_redactions", (root_path.as_posix(), str(root_path)))

    # --- public surface -----------------------------------------------------

    @property
    def as_of(self) -> int:
        return self._as_of

    def trades(self, venue: str, instrument: str, start: int, end: int) -> pa.Table:
        """Trades with ``start <= event_ts < end``, latest known version of each."""
        return self._range(TRADES, venue, instrument, start, end)

    def book_events(self, venue: str, instrument: str, start: int, end: int) -> pa.Table:
        """Book levels with ``start <= event_ts < end``, latest known version of each."""
        return self._range(BOOK_EVENTS, venue, instrument, start, end)

    def stream_events(self, venue: str, instrument: str, start: int, end: int) -> pa.Table:
        """Integrity events (gaps, reconnects, checksum mismatches, ...) with
        ``start <= event_ts < end``. Their ``event_ts`` is the receipt time."""
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
        """The metadata version in effect at time ``at`` (default: ``as_of``).

        Only versions known at ``as_of`` are considered, whatever ``at`` is.
        Returns None if no known version was in effect at that time.
        """
        effective = self._as_of if at is None else at
        _check_time("at", effective)
        versions = self.instruments(venue, instrument).to_pylist()
        in_effect = [row for row in versions if row["event_ts"] <= effective]
        if not in_effect:
            return None
        row = in_effect[-1]
        return InstrumentMeta(
            venue=venue,
            instrument=instrument,
            event_ts=row["event_ts"],
            recv_ts=row["recv_ts"],
            knowledge_ts=row["knowledge_ts"],
            price_exp=row["price_exp"],
            qty_exp=row["qty_exp"],
            tick_size=row["tick_size"],
            lot_size=row["lot_size"],
        )

    def sql(self, query: str) -> pa.Table:
        """Run one SELECT over the views ``trades``, ``book_events`` and ``instruments``.

        Each view has the columns venue, instrument, date, then the dataset's
        stored columns, and holds the latest version of each record known at
        ``as_of``. Anything other than a single allowed SELECT raises
        QueryRejected without being run; see ``sqlguard`` for what is allowed.
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
        return self._run("SELECT * FROM json_execute_serialized_sql(?)", [serialized])

    def close(self) -> None:
        if self._con is not None:
            self._con.close()
            object.__setattr__(self, "_con", None)

    # --- internals -----------------------------------------------------------

    def _connection(self) -> duckdb.DuckDBPyConnection:
        if self._con is None:
            raise RuntimeError("session is closed")
        return self._con

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
        table = self._run(
            f'SELECT {columns} FROM "{dataset.name}" WHERE "venue" = ? AND "instrument" = ? '
            f'AND "event_ts" >= ? AND "event_ts" < ? ORDER BY {order}',
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
        return f"PITSession(as_of={self._as_of})"

    def __setattr__(self, name: str, value: object) -> None:
        raise AttributeError("PITSession is immutable; open a new session for another as_of")

    def __delattr__(self, name: str) -> None:
        raise AttributeError("PITSession is immutable")

    def __reduce_ex__(self, protocol: object) -> None:
        raise TypeError("PITSession cannot be copied or pickled")

    def __init_subclass__(cls, **kwargs: object) -> None:
        raise TypeError("PITSession cannot be subclassed")
