"""PITSession.sql: one SELECT over the as-of views, and nothing else.

Every rejection below must come from the statement validator (QueryRejected),
not from the query happening to fail later. A second group of tests then
shows that the database engine itself refuses the same things, so a validator
bug alone could not expose future data.

See conftest.py for the timeline of the ``history`` store. Sessions here are
opened at as_of=4000, so the files written at 5000 and later are "the future".
"""

import duckdb
import pyarrow as pa
import pytest
from _builders import T0

from quantagent.data.session import PITSession, QueryError, QueryRejected

AS_OF = 4000


@pytest.fixture
def session(history):
    with PITSession(history.root, as_of=AS_OF) as s:
        yield s


@pytest.fixture
def paths(history):
    return {
        "future": history.file(9000),  # knowledge_ts after AS_OF
        "past": history.file(2000),  # knowledge_ts before AS_OF
        "root": history.root.as_posix(),
    }


def rows(table: pa.Table):
    return [tuple(r.values()) for r in table.to_pylist()]


# --- what is allowed -----------------------------------------------------------


def test_views_have_partition_columns_and_stored_columns(session):
    table = session.sql("SELECT * FROM trades ORDER BY venue, trade_id")
    assert table.schema.names == [
        "venue",
        "instrument",
        "date",
        "event_ts",
        "event_ts_source",
        "recv_ts",
        "knowledge_ts",
        "seq",
        "trade_id",
        "price",
        "qty",
        "side",
    ]
    assert [(r["venue"], r["instrument"], str(r["date"]), r["trade_id"], r["price"]) for r in
            table.to_pylist()] == [
        ("coinbase", "BTC-USD", "2023-11-14", "a", 200),
        ("kraken", "ETH-USD", "2023-11-14", "a", 150),
        ("kraken", "ETH-USD", "2023-11-14", "b", 101),
    ]  # fmt: skip
    assert table.schema.field("price").type == pa.int64()
    assert table.schema.field("knowledge_ts").type == pa.int64()


def test_views_show_the_latest_known_version_only(history):
    with PITSession(history.root, as_of=3999) as before:
        assert rows(before.sql("SELECT price FROM trades WHERE trade_id = 'a' ORDER BY 1")) == [
            (100,),
            (200,),
        ]
    with PITSession(history.root, as_of=4000) as after:
        assert rows(after.sql("SELECT price FROM trades WHERE trade_id = 'a' ORDER BY 1")) == [
            (150,),
            (200,),
        ]


def test_all_three_views_exist_and_future_rows_are_absent(session):
    assert rows(session.sql("SELECT count(*) FROM trades")) == [(3,)]
    assert rows(session.sql("SELECT count(*) FROM book_events")) == [(0,)]  # written at 5000
    assert rows(session.sql("SELECT tick_size FROM instruments")) == [(1,)]
    for view in ("trades", "book_events", "instruments"):
        (latest,) = rows(session.sql(f"SELECT max(knowledge_ts) FROM {view}"))[0]
        assert latest is None or latest <= AS_OF


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("SELECT price FROM trades WHERE venue = 'kraken' ORDER BY price DESC LIMIT 1", [(150,)]),
        ("SELECT venue, count(*) FROM trades GROUP BY venue ORDER BY venue",
         [("coinbase", 1), ("kraken", 2)]),
        ("SELECT venue, sum(price * qty) FROM trades GROUP BY ALL HAVING count(*) > 1",
         [("kraken", 25100)]),
        ("SELECT t.trade_id, i.tick_size FROM trades t JOIN instruments i "
         "ON t.venue = i.venue AND t.instrument = i.instrument ORDER BY 1",
         [("a", 1), ("b", 1)]),
        ("SELECT trade_id FROM (SELECT * FROM trades WHERE price > 100) s ORDER BY trade_id",
         [("a",), ("a",), ("b",)]),
        ("SELECT trade_id FROM trades WHERE price = (SELECT max(price) FROM trades)", [("a",)]),
        ("SELECT count(*) FROM trades WHERE EXISTS (SELECT 1 FROM instruments)", [(3,)]),
        ("SELECT trade_id, row_number() OVER (PARTITION BY venue ORDER BY price) FROM trades "
         "WHERE venue = 'kraken' ORDER BY 2", [("b", 1), ("a", 2)]),
        ("SELECT price FROM trades WHERE venue = 'coinbase' UNION ALL "
         "SELECT tick_size FROM instruments ORDER BY 1", [(1,), (200,)]),
        ("SELECT CASE WHEN price > 120 THEN 'hi' ELSE 'lo' END, CAST(price AS DOUBLE) / 100 "
         "FROM trades WHERE venue = 'coinbase'", [("hi", 2.0)]),
        ("SELECT price FROM trades WHERE price BETWEEN 101 AND 150 AND trade_id IN ('a', 'b') "
         "AND venue IS NOT NULL AND NOT (qty <> 100) ORDER BY price", [(101,), (150,)]),
        ("SELECT DISTINCT venue FROM trades ORDER BY venue", [("coinbase",), ("kraken",)]),
        ("FROM trades SELECT trade_id WHERE venue = 'coinbase'", [("a",)]),
        ("SELECT trade_id FROM TRADES WHERE venue = 'coinbase'", [("a",)]),
        ('SELECT trade_id FROM "trades" WHERE venue = \'coinbase\'', [("a",)]),
        ("SELECT trade_id FROM trades WHERE venue = 'coinbase';", [("a",)]),
        ("  \n SELECT trade_id FROM trades WHERE venue = 'coinbase' \n ", [("a",)]),
        ("SELECT coalesce(max(price), 0), abs(min(price) - 500), round(avg(qty)) FROM trades",
         [(200, 399, 100.0)]),
        ("SELECT 1 + 1", [(2,)]),
    ],
)  # fmt: skip
def test_ordinary_selects_work(session, query, expected):
    assert rows(session.sql(query)) == expected


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        # Forbidden words inside strings, comments and aliases are just text. A
        # validator that searched the query for keywords would refuse these.
        ("SELECT 'DROP TABLE trades; ATTACH read_parquet(''x'')' AS note FROM instruments",
         [("DROP TABLE trades; ATTACH read_parquet('x')",)]),
        ("SELECT ';' AS semi, '--' AS dashes, '/*' AS opener FROM instruments",
         [(";", "--", "/*")]),
        ("SELECT count(*) FROM trades -- ; DROP VIEW trades; SELECT * FROM read_parquet('x')",
         [(3,)]),
        ("SELECT /* ; DELETE FROM trades; */ count(*) FROM trades", [(3,)]),
        ('SELECT price AS "delete", qty AS "read_parquet" FROM trades WHERE venue = \'coinbase\'',
         [(200, 100)]),
        ("SELECT count(*) FROM trades WHERE trade_id = 'x''; DROP VIEW trades; --'", [(0,)]),
        ("SELECT count(*) FROM trades WHERE trade_id = $$a; COPY trades TO 'f'$$", [(0,)]),
        ("SELECT t.price AS insert_update_pragma FROM trades AS copy_attach "
         "JOIN trades t ON t.seq = copy_attach.seq AND t.venue = copy_attach.venue "
         "WHERE t.venue = 'coinbase'", [(200,)]),
    ],
)  # fmt: skip
def test_validation_is_by_parsing_not_by_searching_for_words(session, query, expected):
    assert rows(session.sql(query)) == expected


def test_the_views_survive_everything_above(session):
    # None of the accepted or rejected statements may alter the session.
    assert rows(session.sql("SELECT count(*) FROM trades")) == [(3,)]


def test_result_column_names_and_aliases_are_preserved(session):
    table = session.sql('SELECT venue AS v, price * 2 AS "Double Price" FROM trades LIMIT 1')
    assert table.schema.names == ["v", "Double Price"]
    assert isinstance(table, pa.Table)


def test_an_invalid_but_allowed_query_is_a_query_error(session):
    with pytest.raises(QueryError):
        session.sql("SELECT no_such_column FROM trades")
    with pytest.raises(QueryError):
        session.sql("SELECT CAST(trade_id AS BIGINT) FROM trades")


# --- what is rejected ----------------------------------------------------------

DDL_AND_DML = [
    "CREATE TABLE t AS SELECT * FROM trades",
    "CREATE TABLE t (a INTEGER)",
    "CREATE TEMP TABLE t AS SELECT 1",
    "CREATE VIEW v AS SELECT * FROM trades",
    "CREATE OR REPLACE VIEW trades AS SELECT * FROM read_parquet('{future}')",
    "CREATE MACRO m(a) AS a + 1",
    "CREATE MACRO m() AS TABLE SELECT * FROM read_parquet('{future}')",
    "CREATE SCHEMA s",
    "CREATE SEQUENCE s",
    "CREATE TYPE mood AS ENUM ('a')",
    "CREATE SECRET (TYPE S3, KEY_ID 'a', SECRET 'b')",
    "CREATE INDEX i ON trades (price)",
    "DROP VIEW trades",
    "DROP TABLE trades",
    "DROP VIEW IF EXISTS trades CASCADE",
    "ALTER VIEW trades RENAME TO t",
    "ALTER TABLE trades ADD COLUMN x INTEGER",
    "INSERT INTO trades SELECT * FROM trades",
    "INSERT INTO trades VALUES (1)",
    "UPDATE trades SET price = 0",
    "DELETE FROM trades",
    "TRUNCATE trades",
    "MERGE INTO trades USING instruments ON true WHEN MATCHED THEN DELETE",
    "COPY trades TO 'out.csv'",
    "COPY (SELECT * FROM trades) TO 'out.parquet'",
    "COPY trades FROM '{future}'",
    "ATTACH 'x.db'",
    "ATTACH '{future}' AS f",
    "ATTACH ':memory:' AS m",
    "DETACH memory",
    "USE memory",
    "PRAGMA database_list",
    "PRAGMA enable_external_access",
    "PRAGMA table_info('trades')",
    "SET enable_external_access = true",
    "SET allowed_paths = ['{future}']",
    "SET GLOBAL lock_configuration = false",
    "SET VARIABLE x = 1",
    "RESET enable_external_access",
    "INSTALL httpfs",
    "LOAD httpfs",
    "FORCE INSTALL httpfs",
    "UPDATE EXTENSIONS",
    "EXPORT DATABASE 'dir'",
    "IMPORT DATABASE 'dir'",
    "CALL pragma_version()",
    "CHECKPOINT",
    "VACUUM",
    "ANALYZE",
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "PREPARE p AS SELECT * FROM read_parquet('{future}')",
    "EXECUTE p",
    "DEALLOCATE p",
    "EXPLAIN SELECT * FROM trades",
    "EXPLAIN ANALYZE SELECT * FROM trades",
    "DESCRIBE trades",
    "DESCRIBE SELECT * FROM trades",
    "SHOW TABLES",
    "SHOW ALL TABLES",
    "SHOW trades",
    "SUMMARIZE trades",
    "SUMMARIZE SELECT * FROM read_parquet('{future}')",
    "PIVOT trades ON venue USING sum(price)",
    "COMMENT ON VIEW trades IS 'x'",
]

MULTIPLE_STATEMENTS = [
    "SELECT 1; SELECT 2",
    "SELECT * FROM trades; SELECT * FROM trades",
    "SELECT 1; DROP VIEW trades",
    "SELECT 1; SELECT * FROM read_parquet('{future}')",
    "SELECT 1;\n\nDELETE FROM trades",
    # Hidden after a comment.
    "SELECT 1 -- harmless\n; DELETE FROM trades",
    "SELECT 1 /* harmless */; DELETE FROM trades",
    "SELECT 1 /* ; */ ; ATTACH 'x.db'",
    "SELECT 1; -- trailing\nSELECT * FROM read_parquet('{future}')",
    # Hidden after a string literal that looks like it swallows the rest.
    "SELECT ';' ; ATTACH 'x.db'",
    "SELECT 'a''b'; DROP VIEW trades --'",
    "SELECT $$a$$; DROP VIEW trades",
    "SELECT 'x' AS \"a;b\"; SET enable_external_access = true",
    "SELECT 1; ; SELECT 2",
    "; SELECT 1; SELECT 2",
    "SELECT * FROM trades;SELECT * FROM '{future}'",
]

NOT_A_STATEMENT = ["", "   ", "\n", ";", ";;", "-- only a comment", "/* only a comment */"]

FILE_ACCESS = [
    "SELECT * FROM read_parquet('{future}')",
    "SELECT * FROM read_parquet(['{future}'])",
    "SELECT * FROM read_parquet('{past}')",
    "SELECT * FROM read_parquet('{root}/**/*.parquet')",
    "SELECT * FROM read_parquet('{root}/**/*.parquet', hive_partitioning = true)",
    "SELECT * FROM READ_PARQUET('{future}')",
    "SELECT * FROM \"read_parquet\"('{future}')",
    "SELECT * FROM main.read_parquet('{future}')",
    "SELECT * FROM system.main.read_parquet('{future}')",
    "SELECT * FROM parquet_scan('{future}')",
    "SELECT * FROM parquet_metadata('{future}')",
    "SELECT * FROM parquet_schema('{future}')",
    "SELECT * FROM parquet_file_metadata('{future}')",
    "SELECT * FROM parquet_kv_metadata('{future}')",
    "SELECT * FROM read_csv('{future}')",
    "SELECT * FROM read_csv_auto('{future}')",
    "SELECT * FROM sniff_csv('{future}')",
    "SELECT * FROM read_json('{future}')",
    "SELECT * FROM read_json_auto('{future}')",
    "SELECT * FROM read_ndjson_objects('{future}')",
    "SELECT * FROM read_text('{future}')",
    "SELECT * FROM read_blob('{future}')",
    "SELECT * FROM glob('{root}/**')",
    "SELECT * FROM delta_scan('{root}')",
    "SELECT * FROM iceberg_scan('{root}')",
    "SELECT * FROM '{future}'",
    'SELECT * FROM "{future}"',
    "SELECT * FROM '{root}/trades/*/*/*/*.parquet'",
    "SELECT * FROM 'https://example.com/data.parquet'",
    "SELECT * FROM 's3://bucket/data.parquet'",
    "SELECT * FROM 'data.csv'",
    "FROM '{future}'",
    "FROM read_parquet('{future}') SELECT *",
    # Nested where a shallow check would miss it.
    "SELECT * FROM (SELECT * FROM read_parquet('{future}')) s",
    "SELECT * FROM (SELECT * FROM (SELECT * FROM '{future}') a) b",
    "SELECT (SELECT max(knowledge_ts) FROM read_parquet('{future}'))",
    "SELECT * FROM trades WHERE EXISTS (SELECT 1 FROM read_parquet('{future}'))",
    "SELECT * FROM trades WHERE price IN (SELECT price FROM '{future}')",
    "SELECT * FROM trades WHERE price > ANY (SELECT price FROM read_parquet('{future}'))",
    "SELECT * FROM trades t JOIN read_parquet('{future}') f ON t.seq = f.seq",
    "SELECT * FROM trades t LEFT JOIN '{future}' f USING (seq)",
    "SELECT * FROM trades, read_parquet('{future}')",
    "SELECT * FROM trades t, LATERAL (SELECT * FROM read_parquet('{future}')) l",
    "SELECT price FROM trades UNION ALL SELECT price FROM read_parquet('{future}')",
    "SELECT price FROM trades EXCEPT SELECT price FROM '{future}'",
    "SELECT CASE WHEN price > 0 THEN (SELECT max(price) FROM '{future}') END FROM trades",
    "SELECT price FROM trades ORDER BY (SELECT max(price) FROM read_parquet('{future}'))",
    "SELECT venue FROM trades GROUP BY venue HAVING count(*) > (SELECT count(*) FROM '{future}')",
    "SELECT sum(price) FILTER (WHERE price > (SELECT max(price) FROM '{future}')) FROM trades",
    "SELECT sum(price) OVER (ORDER BY (SELECT max(seq) FROM '{future}')) FROM trades",
    "SELECT * FROM trades LIMIT (SELECT count(*) FROM read_parquet('{future}'))",
    "SELECT * FROM trades QUALIFY row_number() OVER () > (SELECT count(*) FROM '{future}')",
    "SELECT coalesce((SELECT max(price) FROM read_parquet('{future}')), 0)",
    "SELECT CAST((SELECT max(price) FROM '{future}') AS VARCHAR)",
    # Running SQL from a string.
    "SELECT * FROM query('SELECT * FROM read_parquet(''{future}'')')",
    "SELECT * FROM query_table('trades')",
    # Other table functions, harmless or not, are not on the allow-list.
    "SELECT * FROM range(10)",
    "SELECT * FROM generate_series(1, 10)",
    "SELECT * FROM unnest([1, 2])",
    "SELECT * FROM (VALUES (1), (2)) t(a)",
]

UNFILTERED_AND_CATALOG = [
    "SELECT * FROM main.trades",
    "SELECT * FROM memory.main.trades",
    "SELECT * FROM temp.trades",
    "SELECT * FROM system.main.trades",
    "SELECT * FROM information_schema.tables",
    "SELECT * FROM information_schema.columns",
    "SELECT * FROM information_schema.views",
    "SELECT * FROM pg_catalog.pg_views",
    "SELECT * FROM pg_catalog.pg_tables",
    "SELECT * FROM pg_views",
    "SELECT * FROM sqlite_master",
    "SELECT * FROM duckdb_views",
    "SELECT sql FROM duckdb_views()",
    "SELECT * FROM duckdb_tables()",
    "SELECT * FROM duckdb_settings()",
    "SELECT * FROM duckdb_functions()",
    "SELECT * FROM duckdb_extensions()",
    "SELECT * FROM duckdb_secrets()",
    "SELECT * FROM duckdb_databases()",
    "SELECT * FROM duckdb_dependencies()",
    "SELECT * FROM pragma_database_list",
    "SELECT * FROM pragma_table_info('trades')",
    "SELECT * FROM pragma_storage_info('trades')",
    "SELECT * FROM no_such_table",
    "SELECT * FROM _trades",
    "SELECT * FROM trades_all",
    "SELECT * FROM raw_trades",
    "SELECT * FROM trades t JOIN duckdb_views v ON true",
    "SELECT (SELECT sql FROM duckdb_views LIMIT 1) FROM trades",
    "SELECT * FROM trades AT (VERSION => 1)",
    "SELECT * FROM trades AT (TIMESTAMP => TIMESTAMP '2030-01-01')",
]

FUNCTIONS = [
    "SELECT current_setting('allowed_paths')",
    "SELECT current_setting('enable_external_access') FROM trades",
    "SELECT system.main.current_setting('allowed_paths')",
    "SELECT main.count(*) FROM trades",
    "SELECT getenv('PATH')",
    "SELECT getvariable('x')",
    "SELECT nextval('s')",
    "SELECT currval('s')",
    "SELECT version()",
    "SELECT now()",
    "SELECT get_current_timestamp()",
    "SELECT today()",
    "SELECT random()",
    "SELECT uuid()",
    "SELECT gen_random_uuid()",
    "SELECT current_database()",
    "SELECT current_schema()",
    "SELECT current_schemas(true)",
    "SELECT txid_current()",
    "SELECT error('boom')",
    "SELECT md5(trade_id) FROM trades",
    "SELECT list_transform([1, 2], x -> x + 1)",
    "SELECT [price] FROM trades",
    "SELECT {{'a': price}} FROM trades",
    "SELECT struct_pack(a := price) FROM trades",
    "SELECT count(*) FROM trades WHERE price = (SELECT max(len(current_setting('x'))))",
    "SELECT sum(price) OVER (ORDER BY random()) FROM trades",
    "SELECT price FROM trades ORDER BY random()",
    "SELECT max(price) FILTER (WHERE current_setting('threads') IS NOT NULL) FROM trades",
    "SELECT query('SELECT 1')",
    "SELECT read_text('{future}')",
    "SELECT parse_path('{future}')",
    "SELECT in_search_path('memory', 'main')",
    "SELECT pg_typeof(price) FROM trades",
    "SELECT typeof(price) FROM trades",
    "SELECT hash(price) FROM trades",
    "SELECT export_state_fn(price) FROM trades",
]

# Bare identifiers that SQL binds to functions when no column has that name.
VALUE_FUNCTIONS = [
    f"SELECT {name}{suffix}"
    for name in (
        "current_date",
        "current_time",
        "current_timestamp",
        "current_user",
        "current_role",
        "current_catalog",
        "current_schema",
        "session_user",
        "user",
        "localtime",
        "localtimestamp",
        "CURRENT_DATE",
        '"current_date"',
    )
    for suffix in ("", " FROM trades")
] + [
    "SELECT price FROM trades WHERE event_ts < epoch_ns(current_timestamp)",
    "SELECT price FROM trades ORDER BY current_time",
    "SELECT 1 AS current_date",
    "SELECT * FROM (SELECT 1 AS current_user) s",
    'SELECT s.user FROM (SELECT 1) s("user")',
    "SELECT * FROM trades AS current_date",
]

COMMON_TABLE_EXPRESSIONS = [
    "WITH c AS (SELECT * FROM trades) SELECT * FROM c",
    "WITH c AS (SELECT 1 AS a) SELECT * FROM c",
    "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT * FROM r",
    # A CTE name can make a catalog object look like an allowed reference.
    "WITH duckdb_views AS (SELECT * FROM duckdb_views) SELECT * FROM duckdb_views",
    "WITH a AS (SELECT * FROM duckdb_views), duckdb_views AS (SELECT 1) SELECT * FROM a",
    "WITH trades AS (SELECT * FROM read_parquet('{future}')) SELECT * FROM trades",
    "SELECT * FROM (WITH trades AS (SELECT * FROM '{future}') SELECT * FROM trades) s",
    "SELECT (WITH c AS (SELECT 1 AS a) SELECT a FROM c)",
    "WITH c AS MATERIALIZED (SELECT * FROM trades) SELECT * FROM c",
]

OTHER = [
    "SELECT * FROM trades WHERE price = ?",
    "SELECT * FROM trades WHERE price = $1",
    "SELECT * FROM trades WHERE price = $price",
    "SELECT * FROM trades USING SAMPLE 10%",
    "SELECT * FROM trades USING SAMPLE 1 ROWS",
    "SELECT * FROM trades TABLESAMPLE reservoir(1)",
    "SELECT * FROM (SELECT * FROM trades USING SAMPLE 50%) s",
    "SELECT #1 FROM trades",
    "SELECT * FROM trades ORDER BY ALL LIMIT 10%",
    "SELECT venue COLLATE NOCASE FROM trades",
    "SELECT price::mood FROM trades",
    "SELECT CAST(price AS main.mood) FROM trades",
    "SELECT * FROM trades POSITIONAL JOIN instruments",
]

CASES = {
    "ddl_dml": DDL_AND_DML,
    "multi": MULTIPLE_STATEMENTS,
    "empty": NOT_A_STATEMENT,
    "file": FILE_ACCESS,
    "catalog": UNFILTERED_AND_CATALOG,
    "function": FUNCTIONS,
    "value_function": VALUE_FUNCTIONS,
    "cte": COMMON_TABLE_EXPRESSIONS,
    "other": OTHER,
}
REJECTED = [
    pytest.param(query, id=f"{group}-{index:02d}")
    for group, queries in CASES.items()
    for index, query in enumerate(queries)
]


@pytest.mark.parametrize("template", REJECTED)
def test_everything_else_is_rejected_by_the_validator(session, paths, template):
    query = template.format(**paths)
    with pytest.raises(QueryRejected):
        session.sql(query)
    # And the session is unharmed.
    assert rows(session.sql("SELECT count(*), max(knowledge_ts) FROM trades")) == [(3, 4000)]


def test_rejection_cases_cover_every_statement_kind_the_roadmap_lists():
    listed = ["CREATE", "DROP", "ALTER", "INSERT", "UPDATE", "DELETE", "COPY", "ATTACH", "PRAGMA",
              "SET", "INSTALL", "LOAD"]  # fmt: skip
    for keyword in listed:
        assert any(q.startswith(keyword) for q in DDL_AND_DML), keyword


@pytest.mark.parametrize("bad", [None, 5, b"SELECT 1", ["SELECT 1"], ("SELECT 1",)])
def test_query_must_be_a_string(session, bad):
    with pytest.raises(TypeError):
        session.sql(bad)


def test_sql_takes_no_parameters_or_options(session):
    with pytest.raises(TypeError):
        session.sql("SELECT * FROM trades WHERE price = ?", [100])
    with pytest.raises(TypeError):
        session.sql("SELECT 1", params=[1])


def test_oversized_queries_are_rejected(session):
    with pytest.raises(QueryRejected, match="too long"):
        session.sql("SELECT 1 " + " " * 200_000)


@pytest.mark.parametrize(
    "column",
    ["filename", "file_row_number", "file_index", "_version_rank", "rowid"],
)
def test_views_expose_no_file_or_internal_columns(session, column):
    # Parquet scans can offer the source file name as a virtual column. The
    # views list their columns explicitly, so none of that comes through.
    with pytest.raises(QueryError):
        session.sql(f"SELECT {column} FROM trades")
    assert column not in session.sql("SELECT * FROM trades").schema.names


@pytest.mark.parametrize("depth", [50, 5_000])
def test_deep_nesting_is_handled_without_crashing(session, depth):
    parentheses = "SELECT " + "(" * depth + "1" + ")" * depth
    subqueries = "SELECT * FROM " + "(SELECT * FROM " * depth + "trades" + ") s" * depth
    for query in (parentheses, subqueries):
        try:
            result = session.sql(query)
        except (QueryRejected, QueryError):
            continue
        assert result.num_rows in (1, 3)


def test_python_objects_in_scope_are_not_queryable(session):
    leak = pa.table({"knowledge_ts": [9_999_999]})  # noqa: F841
    with pytest.raises(QueryRejected):
        session.sql("SELECT * FROM leak")


def test_rejection_messages_name_the_reason(session, paths):
    cases = {
        "DROP VIEW trades": "SELECT",
        "SELECT 1; SELECT 2": "one statement",
        f"SELECT * FROM read_parquet('{paths['future']}')": "table function",
        "SELECT * FROM information_schema.tables": "qualified",
        "SELECT * FROM no_such_table": "no_such_table",
        "SELECT version()": "version",
        "WITH c AS (SELECT 1) SELECT * FROM c": "WITH",
        "SELECT current_date": "current_date",
        "SELECT * FROM trades WHERE price = ?": "PARAMETER",
    }
    for query, fragment in cases.items():
        with pytest.raises(QueryRejected, match=fragment):
            session.sql(query)


# --- second layer: the engine refuses too ----------------------------------------
#
# These tests deliberately reach into the session's private connection, which
# no caller is allowed to do, to show what would happen if the validator had a
# hole: the connection can read only files whose knowledge time is at or before
# as_of, and its configuration cannot be changed.


def _connection(session) -> duckdb.DuckDBPyConnection:
    found = [
        getattr(session, name)
        for name in type(session).__slots__
        if isinstance(getattr(session, name, None), duckdb.DuckDBPyConnection)
    ]
    assert len(found) == 1
    return found[0]


def test_engine_cannot_read_a_future_file(session, paths):
    con = _connection(session)
    for query in (
        f"SELECT * FROM read_parquet('{paths['future']}')",
        f"SELECT * FROM '{paths['future']}'",
        f"SELECT * FROM read_parquet('{paths['root']}/**/*.parquet')",
        f"SELECT * FROM glob('{paths['root']}/**')",
        f"SELECT * FROM read_text('{paths['future']}')",
        f"SELECT * FROM read_blob('{paths['future']}')",
        f"SELECT * FROM parquet_metadata('{paths['future']}')",
    ):
        with pytest.raises(duckdb.PermissionException):
            con.sql(query).fetchall()


def test_engine_cannot_write_attach_or_load(session, paths, tmp_path):
    con = _connection(session)
    out = (tmp_path / "out.csv").as_posix()
    for statement in (
        f"COPY (SELECT * FROM trades) TO '{out}'",
        f"ATTACH '{(tmp_path / 'x.db').as_posix()}' AS x",
        f"EXPORT DATABASE '{(tmp_path / 'dump').as_posix()}'",
        "INSTALL httpfs",
    ):
        with pytest.raises(duckdb.Error):
            con.execute(statement)
    assert not (tmp_path / "out.csv").exists()
    assert not (tmp_path / "x.db").exists()
    assert not (tmp_path / "dump").exists()


def test_engine_configuration_is_locked(session, paths):
    con = _connection(session)
    for statement in (
        "SET enable_external_access = true",
        f"SET allowed_paths = ['{paths['future']}']",
        f"SET allowed_directories = ['{paths['root']}']",
        "SET lock_configuration = false",
        "RESET enable_external_access",
        "SET python_enable_replacements = true",
    ):
        with pytest.raises(duckdb.Error):
            con.execute(statement)
    with pytest.raises(duckdb.PermissionException):
        con.sql(f"SELECT * FROM read_parquet('{paths['future']}')").fetchall()


def test_engine_does_not_resolve_python_variables(session):
    con = _connection(session)
    leak = pa.table({"knowledge_ts": [9_999_999]})  # noqa: F841
    with pytest.raises(duckdb.CatalogException):
        con.sql("SELECT * FROM leak").fetchall()


def test_engine_holds_no_row_after_as_of_even_in_the_files_it_may_read(session, history):
    con = _connection(session)
    allowed = con.sql("SELECT current_setting('allowed_paths')").fetchall()[0][0]
    assert allowed, "expected an explicit list of readable files"
    assert history.file(9000) not in allowed
    for path in allowed:
        (latest,) = con.sql(f"SELECT max(knowledge_ts) FROM read_parquet('{path}')").fetchall()[0]
        assert latest <= AS_OF


def test_a_file_whose_name_understates_its_knowledge_time_is_still_filtered(root):
    # Defence against a mislabelled file: the views filter on the knowledge_ts
    # stored in each row, not only on the file name.
    from _builders import FakeClock, trade_table

    from quantagent.data.schema import TRADES
    from quantagent.data.store import AppendOnlyStore

    store = AppendOnlyStore(root, clock=FakeClock(9000))
    result = store.append(TRADES, "kraken", "ETH-USD", trade_table(("late", T0, 1)))
    disguised = result.files[0].with_name(f"part-{1000:020d}.parquet")
    result.files[0].rename(disguised)  # test-only tampering

    with PITSession(root, as_of=5000) as s:
        assert rows(s.sql("SELECT count(*) FROM trades")) == [(0,)]
        assert s.trades("kraken", "ETH-USD", 0, 2**63 - 1).num_rows == 0
    with PITSession(root, as_of=9000) as s:
        assert rows(s.sql("SELECT trade_id FROM trades")) == [("late",)]
