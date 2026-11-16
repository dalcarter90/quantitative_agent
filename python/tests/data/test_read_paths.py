"""No module of the data package reads the store except through PITSession.

CLAUDE.md safety rule 9 rests on this until the signal layer's import lint
exists. The 2026-11 review (T2) showed that the earlier guards could not see
what they guarded against: one listed only the submodules already imported,
the other searched the source text for six words. These tests read the
package from disk and check its syntax trees.

- Every module of ``quantagent.data`` is listed here with what it is for. A
  new module fails the first test until it is added, which is a deliberate
  act: say why it needs no read path, or route it through the session.
- Only the modules allowed to may import a library that can open a store file
  (DuckDB, pyarrow's Parquet, dataset and file readers, pandas, polars, ...),
  and they may use only the names listed for them.
- The modules that read through a session (the reconstructor and the quality
  checks) do not walk directories or open files themselves.
"""

import ast
import pkgutil
from pathlib import Path

import pytest

import quantagent.data as data_package

PACKAGE = Path(data_package.__file__).parent

# Every module, and why it is here. rawlog, recorder, logorder and adapters
# are the capture side: they read and write raw logs, never store files.
MODULES = {
    "quantagent.data": "the package",
    "quantagent.data.fixedpoint": "decimal text to mantissas; no I/O",
    "quantagent.data.schema": "dataset definitions and row checks; no I/O",
    "quantagent.data.store": "the only writer",
    "quantagent.data.session": "the only reader",
    "quantagent.data.sqlguard": "the session's SQL allow-list; no I/O",
    "quantagent.data.rawlog": "raw capture logs",
    "quantagent.data.recorder": "writes raw logs",
    "quantagent.data.clock": "measures the recording clock",
    "quantagent.data.logorder": "orders raw log records",
    "quantagent.data.adapters": "raw records to rows",
    "quantagent.data.adapters.__main__": "the adapters' command line",
    "quantagent.data.adapters.base": "the normalizer and ingest",
    "quantagent.data.adapters.kraken": "Kraken's messages",
    "quantagent.data.adapters.coinbase": "Coinbase's messages",
    "quantagent.data.quality": "reads only through PITSession",
    "quantagent.data.book": "reads only through PITSession",
}

# Libraries that can open a Parquet file, or any file as a table. A module may
# import one only if it is listed for it below, and then use only the names
# listed.
READERS = (
    "duckdb",
    "pyarrow.parquet",
    "pyarrow.dataset",
    "pyarrow.csv",
    "pyarrow.json",
    "pyarrow.feather",
    "pyarrow.orc",
    "pyarrow.ipc",
    "pyarrow.fs",
    "pandas",
    "polars",
    "fastparquet",
    "sqlite3",
)
ALLOWED_READER_NAMES = {
    "quantagent.data.session": {
        "duckdb": {"connect", "Error", "DuckDBPyConnection"},
        "pyarrow.parquet": {"read_schema"},  # the footer check (ADR 0015)
    },
    "quantagent.data.store": {"pyarrow.parquet": {"write_table"}},
    "quantagent.data.book": {"pyarrow.parquet": {"write_table"}},  # golden files, written
}
# Attributes of pyarrow itself that reach a reader without importing it.
PYARROW_READER_ATTRIBUTES = {"parquet", "dataset", "csv", "json", "feather", "orc", "ipc", "fs",
                             "memory_map", "OSFile", "input_stream",
                             "MemoryMappedFile"}  # fmt: skip
# Modules that may not touch the file system, other than as listed.
SESSION_READERS = {
    "quantagent.data.book": set(),
    "quantagent.data.quality": {"read_text"},  # QualityConfig.from_file reads its config
}
FILE_SYSTEM_CALLS = {"open", "glob", "rglob", "iterdir", "walk", "listdir", "scandir",
                     "read_bytes", "read_text", "open_input_file", "open_input_stream",
                     "__import__", "import_module", "exec", "eval"}  # fmt: skip


def module_files() -> dict[str, Path]:
    found = {"quantagent.data": PACKAGE / "__init__.py"}
    for info in pkgutil.walk_packages([str(PACKAGE)], prefix="quantagent.data."):
        path = PACKAGE.joinpath(*info.name.split(".")[2:])
        found[info.name] = path / "__init__.py" if info.ispkg else path.with_suffix(".py")
    return found


def _reader(name: str) -> str | None:
    return next((r for r in READERS if name == r or name.startswith(r + ".")), None)


def reader_uses(module: ast.Module) -> dict[str, set[str]]:
    """For each reader library the module imports, the names it uses from it.

    ``import duckdb`` then ``duckdb.connect`` uses ``connect``; ``from
    pyarrow.parquet import read_table`` uses ``read_table``; ``import
    pyarrow.parquet as pq`` then ``pq.ParquetDataset`` uses ``ParquetDataset``.
    Using the module object itself (passing it around, or importing a dotted
    name without ``as``) is ``*``, which no module is allowed.
    """
    aliases: dict[str, str] = {}  # local name -> reader library
    used: dict[str, set[str]] = {}
    for node in ast.walk(module):
        if isinstance(node, ast.Import):
            for alias in node.names:
                library = _reader(alias.name)
                if library is None:
                    continue
                if alias.asname:
                    aliases[alias.asname] = library
                    used.setdefault(library, set())
                elif "." in alias.name:
                    used.setdefault(library, set()).add("*")  # binds the top-level package
                else:
                    aliases[alias.name] = library
                    used.setdefault(library, set())
        elif isinstance(node, ast.ImportFrom) and node.module:
            library = _reader(node.module)
            if library is not None:
                used.setdefault(library, set()).update(a.name for a in node.names)
            elif node.module == "pyarrow":
                for alias in node.names:
                    if alias.name in PYARROW_READER_ATTRIBUTES:
                        used.setdefault(f"pyarrow.{alias.name}", set()).add("*")
    bases = {
        id(node.value)
        for node in ast.walk(module)
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name)
    }
    for node in ast.walk(module):
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            if node.value.id in aliases:
                used[aliases[node.value.id]].add(node.attr)
        elif isinstance(node, ast.Name) and node.id in aliases and id(node) not in bases:
            used[aliases[node.id]].add("*")
    return used


def pyarrow_reader_attributes(module: ast.Module) -> set[str]:
    """``pa.parquet``, ``pyarrow.dataset`` and the like, through any pyarrow alias."""
    names = {"pyarrow"}
    for node in ast.walk(module):
        if isinstance(node, ast.Import):
            names |= {a.asname for a in node.names if a.name == "pyarrow" and a.asname}
    return {
        node.attr
        for node in ast.walk(module)
        if isinstance(node, ast.Attribute)
        and isinstance(node.value, ast.Name)
        and node.value.id in names
        and node.attr in PYARROW_READER_ATTRIBUTES
    }


def called_names(module: ast.Module) -> set[str]:
    called = set()
    for node in ast.walk(module):
        if isinstance(node, ast.Call):
            function = node.func
            if isinstance(function, ast.Name):
                called.add(function.id)
            elif isinstance(function, ast.Attribute):
                called.add(function.attr)
    return called


def lint(source: str, name: str) -> list[str]:
    """Every way ``source``, as module ``name``, reads around the session."""
    module = ast.parse(source)
    problems = []
    allowed = ALLOWED_READER_NAMES.get(name, {})
    for library, names in sorted(reader_uses(module).items()):
        if library not in allowed:
            problems.append(f"imports {library}, which can read store files")
        else:
            problems += [f"uses {n} from {library}" for n in sorted(names - allowed[library])]
    problems += [f"reaches pyarrow.{a}" for a in sorted(pyarrow_reader_attributes(module))]
    if name in SESSION_READERS:
        calls = (called_names(module) & FILE_SYSTEM_CALLS) - SESSION_READERS[name]
        problems += [f"calls {c}" for c in sorted(calls)]
    return problems


# --- the tests -----------------------------------------------------------------------------------


def test_the_data_package_has_exactly_the_modules_listed():
    # Read from disk, not from what happens to be imported (review, T2).
    found = set(module_files())
    assert found - set(MODULES) == set(), "a new module: list it in MODULES, with why"
    assert set(MODULES) - found == set(), "a listed module no longer exists"


@pytest.mark.parametrize("name", sorted(MODULES))
def test_no_module_reads_around_the_session(name):
    assert lint(module_files()[name].read_text(encoding="utf-8"), name) == []


BOOK = (PACKAGE / "book.py").read_text(encoding="utf-8")


@pytest.mark.parametrize(
    ("name", "source"),
    [
        # The review's first mutation: a new module that reads the store directly.
        ("quantagent.data.leak", "import pyarrow.parquet as pq\n"
                                 "def rows(root):\n    return pq.ParquetDataset(root).read()\n"),
        # The review's second: the reconstructor reading store files with the
        # pyarrow.parquet it already imports to write golden files.
        ("quantagent.data.book", BOOK + "\ndef sneak(root):\n    return pq.ParquetDataset(root)\n"),
        ("quantagent.data.book", BOOK + "\ndef sneak(path):\n    return pq.read_table(path)\n"),
        ("quantagent.data.book", BOOK + "\nfrom pyarrow.parquet import read_table\n"),
        ("quantagent.data.book", BOOK + "\nx = pa.parquet\n"),
        ("quantagent.data.book", BOOK + "\nimport pyarrow.parquet\n"),
        ("quantagent.data.book", BOOK + "\nreader = pq\n"),
        ("quantagent.data.book", BOOK + "\nimport duckdb\n"),
        ("quantagent.data.book", BOOK + "\nimport pyarrow.dataset as ds\n"),
        ("quantagent.data.book", BOOK + "\nfrom pyarrow import dataset\n"),
        ("quantagent.data.book", BOOK + "\ndef sneak(p):\n    return open(p, 'rb').read()\n"),
        ("quantagent.data.book", BOOK + "\ndef sneak(p):\n    return list(p.rglob('*'))\n"),
        ("quantagent.data.book", BOOK + "\ndef sneak(n):\n    return __import__(n)\n"),
        ("quantagent.data.quality", "import pandas\n"),
        ("quantagent.data.quality", "def f(p):\n    return p.read_bytes()\n"),
        ("quantagent.data.schema", "import duckdb\nduckdb.sql('SELECT 1')\n"),
        ("quantagent.data.session", "import duckdb\nduckdb.read_parquet('x')\n"),
    ],
)  # fmt: skip
def test_the_lint_catches_a_read_path_around_the_session(name, source):
    assert lint(source, name) != []


def test_a_new_module_on_disk_is_found(tmp_path):
    # pkgutil reads the directory: a module nobody imported is still listed.
    (tmp_path / "leak.py").write_text("x = 1\n", encoding="utf-8")
    found = {info.name for info in pkgutil.walk_packages([str(tmp_path)], prefix="p.")}
    assert found == {"p.leak"}
